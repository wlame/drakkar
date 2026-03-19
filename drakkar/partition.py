"""Per-partition processor for Drakkar framework."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import structlog

from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler
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
from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

CollectCallback = Callable[[CollectResult, int], Awaitable[None]]
CommitCallback = Callable[[int, int], Awaitable[None]]

MAX_RETRIES = 3  # default, overridden by config.executor.max_retries


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
        max_retries: int = 3,
        on_collect: CollectCallback | None = None,
        on_commit: CommitCallback | None = None,
        recorder: EventRecorder | None = None,
    ) -> None:
        self._partition_id = partition_id
        self._handler = handler
        self._executor_pool = executor_pool
        self._window_size = window_size
        self._max_retries = max_retries
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
        self._active_tasks: set[asyncio.Task] = set()
        self._arranging = False
        self._arrange_start: float = 0.0
        self._arrange_labels: list[str] = []

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
        partition_queue_size.labels(partition=str(self._partition_id)).set(self._queue.qsize())

    def start(self) -> None:
        """Start the partition processing loop."""
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the partition processor and wait for completion.

        Sets _running=False so _run() exits its main loop and drains
        remaining queued messages. Waits up to 10s for natural exit
        before force-cancelling.
        """
        self._running = False
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass
            self._task = None

    async def drain(self) -> None:
        """Wait for all in-flight work and queued messages to complete."""
        while (
            self._queue.qsize() > 0
            or self._offset_tracker.has_pending()
            or self._inflight_count > 0
        ):
            await asyncio.sleep(0.05)

    async def _run(self) -> None:
        log = logger.bind(partition=self._partition_id, category='partition')
        await log.ainfo('partition_processor_started')

        try:
            while self._running:
                messages = await self._collect_window()
                if not messages:
                    # retry any uncommitted offsets on idle iterations
                    await self._try_commit()
                    continue

                await self._process_window(messages)

            # drain remaining queued messages after _running becomes False
            while self._queue.qsize() > 0:
                messages = []
                while not self._queue.empty():
                    try:
                        messages.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if messages:
                    await self._process_window(messages)

            # wait for any in-flight tasks to complete
            while self._inflight_count > 0 or self._offset_tracker.has_pending():
                await asyncio.sleep(0.05)

            # final commit
            await self._try_commit()
        except asyncio.CancelledError:
            await log.ainfo('partition_processor_cancelled')
        except Exception as e:
            await log.aerror('partition_processor_error', error=str(e), exc_info=True)

    async def _collect_window(self) -> list[SourceMessage]:
        """Collect up to window_size messages from the queue."""
        messages: list[SourceMessage] = []

        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            messages.append(first)
        except TimeoutError:
            return []

        while len(messages) < self._window_size:
            try:
                msg = self._queue.get_nowait()
                messages.append(msg)
            except asyncio.QueueEmpty:
                break

        partition_queue_size.labels(partition=str(self._partition_id)).set(self._queue.qsize())
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
            self._handler.deserialize_message(msg)

        pending_ctx = PendingContext(
            pending_tasks=list(self._pending_tasks.values()),
            pending_task_ids=set(self._pending_tasks.keys()),
        )

        self._arranging = True
        self._arrange_start = time.monotonic()
        self._arrange_labels = [self._handler.message_label(msg) for msg in messages]
        try:
            tasks = await self._handler.arrange(messages, pending_ctx)
        finally:
            self._arranging = False
            arrange_labels = self._arrange_labels
            self._arrange_labels = []
        arrange_duration = time.monotonic() - self._arrange_start
        handler_duration.labels(hook='arrange').observe(arrange_duration)
        if self._recorder:
            self._recorder.record_arranged(
                self._partition_id, messages, tasks,
                duration=arrange_duration,
                message_labels=arrange_labels,
            )
        window.tasks = tasks
        window.total_tasks = len(tasks)

        if not tasks:
            for msg in messages:
                self._offset_tracker.complete(msg.offset)
            await self._try_commit()
            return

        for task in tasks:
            if task.task_id in self._pending_tasks:
                logger.warning(
                    'duplicate_task_id_in_pending',
                    category='partition',
                    partition=self._partition_id,
                    task_id=task.task_id,
                )
            self._pending_tasks[task.task_id] = task

        for task in tasks:
            self._inflight_count += 1
            t = asyncio.create_task(self._execute_and_track(task, window))
            self._active_tasks.add(t)
            t.add_done_callback(self._active_tasks.discard)

    async def _execute_and_track(
        self, task: ExecutorTask, window: Window, retry_count: int = 0
    ) -> None:
        log = logger.bind(
            category='executor',
            partition=self._partition_id,
            task_id=task.task_id,
            window_id=window.window_id,
        )
        executor_tasks.labels(status='started').inc()
        executor_pool_active.set(self._executor_pool.active_count)

        try:
            result = await self._executor_pool.execute(task, self._recorder, self._partition_id)
            executor_tasks.labels(status='completed').inc()
            executor_duration.observe(result.duration_seconds)
            if self._recorder:
                self._recorder.record_task_completed(
                    result, self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                )

            collect_start = time.monotonic()
            collect_result = await self._handler.collect(result)
            collect_duration = time.monotonic() - collect_start
            handler_duration.labels(hook='collect').observe(collect_duration)
            if self._recorder:
                self._recorder.record_collect_completed(
                    task_id=task.task_id,
                    partition=self._partition_id,
                    duration=collect_duration,
                    output_message_count=len(collect_result.output_messages) if collect_result else 0,
                )
            if collect_result and self._on_collect:
                await self._on_collect(collect_result, self._partition_id)

            window.results.append(result)

        except ExecutorTaskError as e:
            executor_tasks.labels(status='failed').inc()
            if e.error.exception and 'Timeout' in (e.error.exception or ''):
                executor_timeouts.inc()
            if self._recorder:
                self._recorder.record_task_failed(
                    task, e.error, self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                )
            await log.awarning('executor_task_failed', error=str(e))

            on_error_start = time.monotonic()
            action = await self._handler.on_error(task, e.error)
            handler_duration.labels(hook='on_error').observe(time.monotonic() - on_error_start)
            if isinstance(action, list):
                for new_task in action:
                    self._pending_tasks[new_task.task_id] = new_task
                    window.tasks.append(new_task)
                    window.total_tasks += 1
                    self._inflight_count += 1
                    t = asyncio.create_task(self._execute_and_track(new_task, window))
                    self._active_tasks.add(t)
                    t.add_done_callback(self._active_tasks.discard)
            elif action == ErrorAction.RETRY and retry_count < self._max_retries:
                task_retries.inc()
                # don't decrement inflight or increment completed — the retry
                # reuses this slot, so we just re-enter with same task
                t = asyncio.create_task(self._execute_and_track(task, window, retry_count + 1))
                self._active_tasks.add(t)
                t.add_done_callback(self._active_tasks.discard)
                return
            else:
                if action == ErrorAction.RETRY:
                    await log.awarning(
                        'max_retries_exceeded',
                        task_id=task.task_id,
                        retries=retry_count,
                    )
                window.results.append(e.result)

        except Exception as e:
            await log.aerror('unexpected_error_in_task', error=str(e), exc_info=True)
            # still count as completed so the window can progress
            window.results.append(
                ExecutorResult(
                    task_id=task.task_id,
                    exit_code=-1,
                    stdout='',
                    stderr=str(e),
                    duration_seconds=0,
                    task=task,
                )
            )

        finally:
            removed = self._pending_tasks.pop(task.task_id, None)
            self._inflight_count -= 1
            executor_pool_active.set(self._executor_pool.active_count)
            if removed is None:
                await log.awarning(
                    'task_not_in_pending_on_cleanup',
                    task_id=task.task_id,
                    retry_count=retry_count,
                    pending_keys=list(self._pending_tasks.keys())[:5],
                )

        window.completed_count += 1

        if window.is_complete:
            duration = time.monotonic() - window.start_time
            batch_duration.observe(duration)

            wc_start = time.monotonic()
            on_complete_result = await self._handler.on_window_complete(
                window.results, window.source_messages
            )
            handler_duration.labels(hook='on_window_complete').observe(time.monotonic() - wc_start)
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
                        'commit_failed',
                        category='kafka',
                        partition=self._partition_id,
                        offset=committable,
                        error=str(e),
                    )
                    return
            self._offset_tracker.acknowledge_commit(committable)
