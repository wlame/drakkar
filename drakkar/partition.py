"""Per-partition processor for Drakkar framework."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import structlog

from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler
from drakkar.recorder import EventRecorder
from drakkar.metrics import (
    batch_duration,
    executor_duration,
    executor_pool_active,
    executor_tasks,
    executor_timeouts,
    handler_duration,
    messages_consumed,
    offset_lag,
    partition_queue_size,
    task_retries,
)
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorResult,
    ExecutorTask,
    PendingContext,
    SourceMessage,
)
from drakkar.offsets import OffsetTracker

logger = structlog.get_logger()

CollectCallback = Callable[[CollectResult, int], Awaitable[None]]
CommitCallback = Callable[[int, int], Awaitable[None]]

MAX_RETRIES = 3


@dataclass
class Window:
    """Tracks the state of one arrange() window within a partition."""

    window_id: int
    source_messages: list[SourceMessage]
    tasks: list[ExecutorTask] = field(default_factory=list)
    results: list[ExecutorResult] = field(default_factory=list)
    completed_count: int = 0
    total_tasks: int = 0
    start_time: float = 0.0

    @property
    def is_complete(self) -> bool:
        return self.completed_count >= self.total_tasks and self.total_tasks > 0


class PartitionProcessor:
    """Processes messages for a single partition.

    Takes windows of messages from its queue, calls the arrange hook,
    submits tasks to the shared executor pool, and tracks offsets.
    Windows are processed concurrently — the processor doesn't wait
    for one window to complete before starting the next.
    """

    def __init__(
        self,
        partition_id: int,
        handler: BaseDrakkarHandler,
        executor_pool: ExecutorPool,
        window_size: int,
        on_collect: CollectCallback | None = None,
        on_commit: CommitCallback | None = None,
        recorder: EventRecorder | None = None,
    ):
        self._partition_id = partition_id
        self._handler = handler
        self._executor_pool = executor_pool
        self._window_size = window_size
        self._on_collect = on_collect
        self._on_commit = on_commit
        self._recorder = recorder

        self._queue: asyncio.Queue[SourceMessage] = asyncio.Queue()
        self._offset_tracker = OffsetTracker()
        self._pending_tasks: dict[str, ExecutorTask] = {}
        self._window_counter = 0
        self._running = False
        self._task: asyncio.Task | None = None
        self._inflight_count = 0

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def offset_tracker(self) -> OffsetTracker:
        return self._offset_tracker

    @property
    def inflight_count(self) -> int:
        return self._inflight_count

    def enqueue(self, message: SourceMessage) -> None:
        """Add a message to this partition's processing queue."""
        self._queue.put_nowait(message)
        if self._recorder:
            self._recorder.record_consumed(message)
        messages_consumed.labels(partition=str(self._partition_id)).inc()
        partition_queue_size.labels(partition=str(self._partition_id)).set(
            self._queue.qsize()
        )

    def start(self) -> None:
        """Start the partition processing loop."""
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the partition processor and wait for completion."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def drain(self) -> None:
        """Wait for all in-flight work to complete."""
        while self._offset_tracker.has_pending() or self._inflight_count > 0:
            await asyncio.sleep(0.05)

    async def _run(self) -> None:
        log = logger.bind(partition=self._partition_id, category="partition")
        await log.ainfo("partition_processor_started")

        try:
            while self._running:
                messages = await self._collect_window()
                if not messages:
                    continue

                await self._process_window(messages)
        except asyncio.CancelledError:
            await log.ainfo("partition_processor_cancelled")
            raise
        except Exception as e:
            await log.aerror("partition_processor_error", error=str(e))
            raise

    async def _collect_window(self) -> list[SourceMessage]:
        """Collect up to window_size messages from the queue."""
        messages: list[SourceMessage] = []

        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            messages.append(first)
        except asyncio.TimeoutError:
            return []

        while len(messages) < self._window_size:
            try:
                msg = self._queue.get_nowait()
                messages.append(msg)
            except asyncio.QueueEmpty:
                break

        partition_queue_size.labels(partition=str(self._partition_id)).set(
            self._queue.qsize()
        )
        return messages

    async def _process_window(self, messages: list[SourceMessage]) -> None:
        self._window_counter += 1
        window = Window(
            window_id=self._window_counter,
            source_messages=messages,
            start_time=time.monotonic(),
        )

        for msg in messages:
            self._offset_tracker.register(msg.offset)

        pending_ctx = PendingContext(
            pending_tasks=list(self._pending_tasks.values()),
            pending_task_ids=set(self._pending_tasks.keys()),
        )

        arrange_start = time.monotonic()
        tasks = await self._handler.arrange(messages, pending_ctx)
        handler_duration.labels(hook="arrange").observe(time.monotonic() - arrange_start)
        if self._recorder:
            self._recorder.record_arranged(self._partition_id, messages, tasks)
        window.tasks = tasks
        window.total_tasks = len(tasks)

        if not tasks:
            for msg in messages:
                self._offset_tracker.complete(msg.offset)
            await self._try_commit()
            return

        for task in tasks:
            self._pending_tasks[task.task_id] = task

        for task in tasks:
            self._inflight_count += 1
            asyncio.create_task(self._execute_and_track(task, window))

    async def _execute_and_track(self, task: ExecutorTask, window: Window, retry_count: int = 0) -> None:
        log = logger.bind(
            category="executor",
            partition=self._partition_id,
            task_id=task.task_id,
            window_id=window.window_id,
        )
        executor_tasks.labels(status="started").inc()
        executor_pool_active.set(self._executor_pool.active_count)
        if self._recorder:
            self._recorder.record_task_started(task, self._partition_id)

        try:
            result = await self._executor_pool.execute(task)
            executor_tasks.labels(status="completed").inc()
            executor_duration.observe(result.duration_seconds)
            if self._recorder:
                self._recorder.record_task_completed(result, self._partition_id)

            collect_start = time.monotonic()
            collect_result = await self._handler.collect(result)
            handler_duration.labels(hook="collect").observe(time.monotonic() - collect_start)
            if collect_result and self._on_collect:
                await self._on_collect(collect_result, self._partition_id)

            window.results.append(result)

        except ExecutorTaskError as e:
            executor_tasks.labels(status="failed").inc()
            if e.error.exception and "Timeout" in (e.error.exception or ""):
                executor_timeouts.inc()
            if self._recorder:
                self._recorder.record_task_failed(task, e.error, self._partition_id)
            await log.awarning("executor_task_failed", error=str(e))

            on_error_start = time.monotonic()
            action = await self._handler.on_error(task, e.error)
            handler_duration.labels(hook="on_error").observe(time.monotonic() - on_error_start)
            if isinstance(action, list):
                for new_task in action:
                    self._pending_tasks[new_task.task_id] = new_task
                    window.tasks.append(new_task)
                    window.total_tasks += 1
                    self._inflight_count += 1
                    asyncio.create_task(self._execute_and_track(new_task, window))
            elif action == ErrorAction.RETRY and retry_count < MAX_RETRIES:
                task_retries.inc()
                # don't decrement inflight or increment completed — the retry
                # reuses this slot, so we just re-enter with same task
                asyncio.create_task(self._execute_and_track(task, window, retry_count + 1))
                return
            else:
                if action == ErrorAction.RETRY:
                    await log.awarning(
                        "max_retries_exceeded",
                        task_id=task.task_id,
                        retries=retry_count,
                    )
                window.results.append(e.result)

        except Exception as e:
            await log.aerror("unexpected_error_in_task", error=str(e), exc_info=True)
            # still count as completed so the window can progress
            window.results.append(ExecutorResult(
                task_id=task.task_id,
                exit_code=-1,
                stdout="",
                stderr=str(e),
                duration_seconds=0,
                task=task,
            ))

        finally:
            self._pending_tasks.pop(task.task_id, None)
            self._inflight_count -= 1
            executor_pool_active.set(self._executor_pool.active_count)

        window.completed_count += 1

        if window.is_complete:
            duration = time.monotonic() - window.start_time
            batch_duration.observe(duration)

            wc_start = time.monotonic()
            on_complete_result = await self._handler.on_window_complete(
                window.results, window.source_messages
            )
            handler_duration.labels(hook="on_window_complete").observe(time.monotonic() - wc_start)
            if on_complete_result and self._on_collect:
                await self._on_collect(on_complete_result, self._partition_id)

            for msg in window.source_messages:
                self._offset_tracker.complete(msg.offset)

            offset_lag.labels(partition=str(self._partition_id)).set(
                self._offset_tracker.pending_count
            )

            await self._try_commit()

    async def _try_commit(self) -> None:
        """Commit offsets if the watermark has advanced."""
        committable = self._offset_tracker.committable()
        if committable is not None:
            if self._on_commit:
                try:
                    await self._on_commit(self._partition_id, committable)
                except Exception as e:
                    logger.warning(
                        "commit_failed", category="kafka",
                        partition=self._partition_id, offset=committable, error=str(e),
                    )
                    return
            self._offset_tracker.acknowledge_commit(committable)
