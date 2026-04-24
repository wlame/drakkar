"""Per-partition processor for Drakkar framework."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

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
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    PendingContext,
    SourceMessage,
)
from drakkar.offsets import OffsetTracker
from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

CollectCallback = Callable[[CollectResult, int], Awaitable[None]]
CommitCallback = Callable[[int, int], Awaitable[None]]

MAX_RETRIES = 3  # default, overridden by config.executor.max_retries
DRAIN_POLL_INTERVAL = 0.05  # seconds between checks when draining in-flight work


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


@dataclass
class _MessageTracker:
    """Internal tracker for the task fan-out derived from a single source message.

    Keyed in ``PartitionProcessor._message_trackers`` by source offset
    (unique per partition). Used to:
      - decide when ``on_message_complete`` should fire
      - collect results/errors into the user-facing ``MessageGroup``
      - trigger per-message offset completion on the ``OffsetTracker``

    Lifecycle:
      - Created in ``_process_window`` for every source message before
        arrange() runs (so the handler can rely on the tracker existing
        in a crash-free manner).
      - Populated with tasks after arrange() returns, stamped against
        each task's ``source_offsets``.
      - Per-task outcomes decrement ``remaining`` and append to ``results``
        or ``errors`` — replaced tasks decrement without appending.
      - When ``remaining == 0`` AND all scheduled tasks are accounted for,
        the tracker fires ``on_message_complete`` and the offset completes.
    """

    source_message: SourceMessage
    tasks: list[ExecutorTask] = field(default_factory=list)
    results: list[ExecutorResult] = field(default_factory=list)
    errors: list[ExecutorError] = field(default_factory=list)
    # Tasks still awaiting a terminal outcome. Incremented on schedule /
    # replacement, decremented on success / SKIP / retries-exhausted /
    # replaced-by-list.
    remaining: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    # Guard against double-firing on_message_complete if bookkeeping hits
    # zero more than once due to e.g. a cancellation race.
    completion_fired: bool = False


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
        # Per-source-message trackers, keyed by offset (unique per partition).
        # Entries are added in _process_window before arrange() runs and
        # removed in _finalize_message_tracker when on_message_complete fires.
        self._message_trackers: dict[int, _MessageTracker] = {}

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

    def signal_stop(self) -> None:
        """Signal the run loop to exit without awaiting task completion.

        Sync counterpart to :meth:`stop`. Setting ``_running = False`` is
        enough to make ``_run()`` break out of its main loop on the next
        iteration, but the caller must still ``await stop()`` (or the
        processor's ``_task``) to guarantee in-flight work has drained.

        Use cases:
          - Multi-processor fan-out where each processor needs an
            early shutdown signal before waiting on them collectively.
          - Shutdown hot-paths where spawning ``await`` points per
            processor would serialise the signal.

        Pair with :meth:`stop` for full shutdown.
        """
        self._running = False

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
        while self._queue.qsize() > 0 or self._offset_tracker.has_pending() or self._inflight_count > 0:
            await asyncio.sleep(DRAIN_POLL_INTERVAL)

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
                await asyncio.sleep(DRAIN_POLL_INTERVAL)

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

        # Create a per-message tracker BEFORE arrange() so any outcome
        # we observe afterwards (even an immediate failure) has a place
        # to land. Also register the offset with the watermark tracker.
        arrange_started_at = time.monotonic()
        for msg in messages:
            self._offset_tracker.register(msg.offset)
            self._handler.deserialize_message(msg)
            self._message_trackers[msg.offset] = _MessageTracker(
                source_message=msg,
                started_at=arrange_started_at,
            )

        pending_ctx = PendingContext(
            pending_tasks=list(self._pending_tasks.values()),
            pending_task_ids=set(self._pending_tasks.keys()),
        )

        offsets = [m.offset for m in messages]
        bind_contextvars(
            partition=self._partition_id,
            hook='arrange',
            window_id=self._window_counter,
            offsets=offsets,
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
            unbind_contextvars('hook', 'window_id', 'offsets')
        arrange_duration = time.monotonic() - self._arrange_start
        handler_duration.labels(hook='arrange').observe(arrange_duration)
        if self._recorder:
            self._recorder.record_arranged(
                self._partition_id,
                messages,
                tasks,
                duration=arrange_duration,
                message_labels=arrange_labels,
            )
        window.tasks = tasks
        window.total_tasks = len(tasks)

        # Register each task with every message tracker it belongs to
        # (by source_offsets). A task with multiple source_offsets
        # participates in multiple groups — its terminal outcome is
        # reported to all of them.
        for task in tasks:
            for offset in task.source_offsets:
                tracker = self._message_trackers.get(offset)
                if tracker is not None:
                    tracker.tasks.append(task)
                    tracker.remaining += 1

        # Fire on_message_complete immediately for any message whose
        # arrange() produced zero tasks. The hook still runs — empty
        # MessageGroup is a legitimate outcome ("message skipped by
        # arrange") and the user may want to emit an audit record.
        for msg in messages:
            tracker = self._message_trackers.get(msg.offset)
            if tracker is not None and tracker.remaining == 0:
                await self._finalize_message_tracker(tracker)

        if not tasks:
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

    async def _execute_and_track(self, task: ExecutorTask, window: Window, retry_count: int = 0) -> None:
        # Bind partition context for this async task — inherited by all user hooks called within
        bind_contextvars(partition=self._partition_id, window_id=window.window_id)
        log = logger.bind(
            category='executor',
            partition=self._partition_id,
            task_id=task.task_id,
            window_id=window.window_id,
        )
        executor_tasks.labels(status='started').inc()
        executor_pool_active.set(self._executor_pool.active_count)

        # When on_error returns RETRY, this invocation hands the task to a newly
        # scheduled retry coroutine that owns the in-flight slot and pending_tasks
        # entry. Must NOT decrement the inflight counter or pop from pending here,
        # or the retry would see inflight go negative and pending_tasks mutated
        # under it — causing drain()/stop() to exit while work is still in flight.
        handed_off_to_retry = False

        # Collected per-task outcome: exactly one of result / error.
        # When neither is set after the main try/finally, this task was
        # replaced via on_error list-return — the tracker decrement still
        # happens but neither results nor errors is appended.
        task_result: ExecutorResult | None = None
        task_error: ExecutorError | None = None

        try:
            result = await self._executor_pool.execute(task, self._recorder, self._partition_id)
            executor_tasks.labels(status='completed').inc()
            executor_duration.observe(result.duration_seconds)
            if self._recorder:
                self._recorder.record_task_completed(
                    result,
                    self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                )

            bind_contextvars(hook='on_task_complete', task_id=task.task_id)
            collect_start = time.monotonic()
            collect_result = await self._handler.on_task_complete(result)
            collect_duration = time.monotonic() - collect_start
            handler_duration.labels(hook='on_task_complete').observe(collect_duration)
            unbind_contextvars('hook', 'task_id')
            if self._recorder:
                self._recorder.record_task_complete(
                    task_id=task.task_id,
                    partition=self._partition_id,
                    duration=collect_duration,
                    output_message_count=len(collect_result.kafka) if collect_result else 0,
                )
            if collect_result and self._on_collect:
                await self._on_collect(collect_result, self._partition_id)

            window.results.append(result)
            task_result = result

        except ExecutorTaskError as e:
            executor_tasks.labels(status='failed').inc()
            if e.error.exception and 'Timeout' in (e.error.exception or ''):
                executor_timeouts.inc()
            if self._recorder:
                self._recorder.record_task_failed(
                    task,
                    e.error,
                    self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                    duration_seconds=e.result.duration_seconds,
                )
            await log.awarning('executor_task_failed', error=str(e))

            bind_contextvars(hook='on_error', task_id=task.task_id)
            on_error_start = time.monotonic()
            action = await self._handler.on_error(task, e.error)
            handler_duration.labels(hook='on_error').observe(time.monotonic() - on_error_start)
            unbind_contextvars('hook', 'task_id')
            if isinstance(action, list):
                # Replacement: the original task is "replaced" (not a
                # terminal failure of the group). Decrement its contribution
                # to every message tracker; add the replacements.
                for new_task in action:
                    # Auto-link the replacement back to its parent unless
                    # the handler explicitly set a different parent_task_id.
                    # Lets on_message_complete walk the replacement chain.
                    if new_task.parent_task_id is None:
                        new_task.parent_task_id = task.task_id
                    self._pending_tasks[new_task.task_id] = new_task
                    window.tasks.append(new_task)
                    window.total_tasks += 1
                    self._inflight_count += 1
                    # Register the replacement with every message tracker
                    # listed in its source_offsets (usually inherited from
                    # the original failing task — it's the handler's
                    # responsibility to set source_offsets on replacements).
                    for offset in new_task.source_offsets:
                        tracker = self._message_trackers.get(offset)
                        if tracker is not None:
                            tracker.tasks.append(new_task)
                            tracker.remaining += 1
                    t = asyncio.create_task(self._execute_and_track(new_task, window))
                    self._active_tasks.add(t)
                    t.add_done_callback(self._active_tasks.discard)
            elif action == ErrorAction.RETRY and retry_count < self._max_retries:
                task_retries.inc()
                # The retry coroutine reuses this invocation's inflight slot,
                # pending_tasks entry, AND message-tracker slot — the finally
                # below must skip cleanup and the retry, not this coroutine,
                # is what eventually updates the tracker(s).
                handed_off_to_retry = True
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
                task_error = e.error

        except Exception as e:
            await log.aerror('unexpected_error_in_task', error=str(e), exc_info=True)
            # still count as completed so the window can progress
            window.results.append(
                ExecutorResult(
                    exit_code=-1,
                    stdout='',
                    stderr=str(e),
                    duration_seconds=0,
                    task=task,
                )
            )
            # Synthesize an ExecutorError so the message tracker sees a
            # terminal failure for this task (the group treats unexpected
            # exceptions like retries-exhausted failures).
            task_error = ExecutorError(
                task=task,
                exception=str(e),
                stderr=str(e),
            )

        finally:
            if not handed_off_to_retry:
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

        if handed_off_to_retry:
            # The retry coroutine owns this slot now — it will update the
            # message trackers and window.completed_count when it finishes.
            return

        # Update per-message trackers with the terminal outcome of THIS task.
        # task_result / task_error / replaced_by are mutually exclusive.
        for offset in task.source_offsets:
            tracker = self._message_trackers.get(offset)
            if tracker is None:
                continue
            # Every non-retry path for THIS task-invocation reduces the
            # tracker's remaining count by 1 — whether success, terminal
            # error, or replaced.
            tracker.remaining -= 1
            if task_result is not None:
                tracker.results.append(task_result)
            elif task_error is not None:
                tracker.errors.append(task_error)
            # replaced_by path: neither results nor errors is appended.
            # The replacements will eventually report their own outcomes
            # through this same loop and settle the tracker.
            if tracker.remaining == 0 and not tracker.completion_fired:
                await self._finalize_message_tracker(tracker)

        window.completed_count += 1

        if window.is_complete:
            duration = time.monotonic() - window.start_time
            batch_duration.observe(duration)

            bind_contextvars(hook='on_window_complete', window_id=window.window_id)
            wc_start = time.monotonic()
            on_complete_result = await self._handler.on_window_complete(window.results, window.source_messages)
            wc_duration = time.monotonic() - wc_start
            handler_duration.labels(hook='on_window_complete').observe(wc_duration)
            unbind_contextvars('hook', 'window_id')
            if self._recorder:
                self._recorder.record_window_complete(
                    partition=self._partition_id,
                    window_id=window.window_id,
                    duration=wc_duration,
                    task_count=window.total_tasks,
                    output_message_count=len(on_complete_result.kafka) if on_complete_result else 0,
                )
            if on_complete_result and self._on_collect:
                await self._on_collect(on_complete_result, self._partition_id)

            # Per-message offsets were already marked complete individually
            # by _finalize_message_tracker as each message's tasks finished.
            # Just make sure any outstanding commit attempt goes through in
            # case the last commit was blocked.
            offset_lag.labels(partition=str(self._partition_id)).set(self._offset_tracker.pending_count)
            await self._try_commit()

    async def _finalize_message_tracker(self, tracker: _MessageTracker) -> None:
        """Fire ``on_message_complete`` for a fully-terminal message and
        complete its offset on the watermark tracker.

        Called whenever a tracker's ``remaining`` counter hits zero —
        either by arrange() returning no tasks, or by the last outstanding
        task reaching a terminal state. Idempotent via
        ``tracker.completion_fired`` to guard against pathological
        double-fires.
        """
        if tracker.completion_fired:
            return
        tracker.completion_fired = True
        tracker.finished_at = time.monotonic()

        group = MessageGroup(
            source_message=tracker.source_message,
            tasks=list(tracker.tasks),
            results=list(tracker.results),
            errors=list(tracker.errors),
            started_at=tracker.started_at,
            finished_at=tracker.finished_at,
        )

        bind_contextvars(
            hook='on_message_complete',
            offset=tracker.source_message.offset,
        )
        mc_start = time.monotonic()
        try:
            on_complete_result = await self._handler.on_message_complete(group)
        except Exception as e:
            # Log and move on — raising here would block the offset from
            # committing, stalling the partition behind a handler bug.
            await logger.aerror(
                'on_message_complete_failed',
                category='handler',
                partition=self._partition_id,
                offset=tracker.source_message.offset,
                error=str(e),
                exc_info=True,
            )
            on_complete_result = None
        mc_duration = time.monotonic() - mc_start
        handler_duration.labels(hook='on_message_complete').observe(mc_duration)
        unbind_contextvars('hook', 'offset')

        if self._recorder:
            self._recorder.record_message_complete(
                partition=self._partition_id,
                offset=tracker.source_message.offset,
                duration=mc_duration,
                task_count=group.total,
                succeeded=group.succeeded,
                failed=group.failed,
                replaced=group.replaced,
                output_message_count=len(on_complete_result.kafka) if on_complete_result else 0,
            )

        if on_complete_result and self._on_collect:
            try:
                await self._on_collect(on_complete_result, self._partition_id)
            except Exception as e:
                # Same reasoning as above — a sink failure on the aggregate
                # payload should not stall the message's offset. The sink
                # manager already runs its own on_delivery_error hook.
                logger.warning(
                    'on_message_complete_sink_delivery_failed',
                    category='handler',
                    partition=self._partition_id,
                    offset=tracker.source_message.offset,
                    error=str(e),
                )

        # Mark the offset complete on the watermark tracker and try to
        # advance the commit. Per-message commit granularity — a slow task
        # in a later message does not pin offsets of already-finished ones.
        self._offset_tracker.complete(tracker.source_message.offset)
        offset_lag.labels(partition=str(self._partition_id)).set(self._offset_tracker.pending_count)
        await self._try_commit()

        # Release tracker memory once its offset is committable.
        self._message_trackers.pop(tracker.source_message.offset, None)

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
