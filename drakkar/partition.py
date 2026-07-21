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
    delivery_stalled_offsets,
    dlq_dropped_payloads,
    executor_duration,
    executor_pool_active,
    executor_tasks,
    executor_timeouts,
    handler_duration,
    handler_hook_errors,
    messages_consumed,
    offset_lag,
    partition_queue_size,
    suppressed_zombie_deliveries,
    task_retries,
)
from drakkar.models import (
    CollectResult,
    DeliveryError,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    MessageParseError,
    ParseFailurePayload,
    PendingContext,
    SinkDeliveryFailedError,
    SourceMessage,
)
from drakkar.offsets import OffsetTracker
from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

CollectCallback = Callable[[CollectResult, int], Awaitable[None]]
CommitCallback = Callable[[int, int], Awaitable[None]]
# Matches DLQSink.send(error, partition_id) -> bool (True = confirmed write).
DLQSendCallback = Callable[[DeliveryError, int], Awaitable[bool]]
# Invoked once (per processor lifetime) when the first offset stalls under
# dlq.on_send_failure=stall — the lifecycle pauses the partition in Kafka.
StallCallback = Callable[[int], Awaitable[None]]

MAX_RETRIES = 3  # default, overridden by config.executor.max_retries
DRAIN_POLL_INTERVAL = 0.05  # seconds between checks when draining in-flight work


@dataclass
class Window:
    """Tracks the state of one arrange() window within a partition.

    Accounting invariants (see also ``docs/fan-out.md`` "Replacement
    accounting — Window vs MessageGroup"):

    - ``total_tasks`` counts EVERY task ever scheduled in this window —
      the tasks returned by ``arrange()`` PLUS any replacements added by
      ``on_error`` list-return. It is incremented at schedule time and
      never decremented. Retries do NOT bump this counter (they reuse
      the original invocation's slot).
    - ``completed_count`` is a per-task-invocation settlement counter.
      One tick fires for every terminal outcome (success / SKIP /
      retries-exhausted) AND for every replacement handoff (the
      replaced original's invocation). Retries hand off to a fresh
      coroutine and defer the increment; the final retry is the one
      that ticks.
    - ``tasks`` mirrors ``total_tasks`` in length — the full scheduled
      history, including replaced originals. Used for debugging /
      tracing the replacement chain via ``parent_task_id``. Retries
      are NOT appended here (the same ``ExecutorTask`` instance is
      re-run).
    - ``results`` contains the ``ExecutorResult`` of every task
      invocation that reached a terminal outcome — success OR
      subprocess-level failure (exit_code != 0, SKIP'd or retries
      exhausted). Replaced originals do NOT contribute — their slot
      in ``tasks`` has no corresponding entry in ``results``. So
      ``len(results)`` can be less than ``total_tasks`` whenever any
      task was replaced; the gap equals the number of replaced tasks.

    This is by design: ``window.results`` is what gets passed to
    ``on_window_complete`` and it represents "outcomes of task runs
    that actually happened end-to-end," not "one slot per scheduled
    task." Replacements express "the original didn't count; its
    successors' outcomes are what matter," so the original's entry
    is omitted.

    ``is_complete`` compares ``completed_count >= total_tasks`` — so
    even though ``len(results) < total_tasks`` is possible, the window
    still closes correctly because each replaced original ticked the
    counter without appending a result.
    """

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
class MessageTracker:
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
    # Set when a sink delivery for this message's payloads could not be
    # confirmed (including the DLQ fallback). A True value makes
    # _finalize_message_tracker skip offset completion — the watermark
    # stalls and the message is redelivered after restart/rebalance.
    delivery_failed: bool = False


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
        on_parse_error: str = 'skip',
        dlq_send: DLQSendCallback | None = None,
        on_dlq_failure: str = 'drop',
        on_stall: StallCallback | None = None,
    ) -> None:
        self._partition_id = partition_id
        self._handler = handler
        self._executor_pool = executor_pool
        self._window_size = window_size
        self._max_retries = max_retries
        self._on_collect = on_collect
        self._on_commit = on_commit
        self._recorder = recorder
        self._on_parse_error = on_parse_error
        self._dlq_send = dlq_send
        # dlq.on_send_failure strategy: 'drop' commits past lost payloads,
        # 'stall' leaves their offsets uncommitted and pauses the partition.
        self._on_dlq_failure = on_dlq_failure
        self._on_stall = on_stall
        self._stall_signaled = False

        self._queue: asyncio.Queue[SourceMessage] = asyncio.Queue()
        self._offset_tracker = OffsetTracker()
        # Serializes the read → commit → acknowledge sequence in
        # _try_commit, which is called concurrently from every task
        # completion as well as the run loop. See _try_commit's docstring.
        self._commit_lock = asyncio.Lock()
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
        self._message_trackers: dict[int, MessageTracker] = {}
        # Offsets deliberately left pending because their sink delivery
        # (including the DLQ fallback) could not be confirmed. They block
        # the watermark by design but must NOT block drain/stop — they will
        # never complete in this process; redelivery happens after restart.
        self._stalled_offsets: set[int] = set()
        # Set when the partition was revoked (or shutdown began) and the
        # drain timed out: tasks still running are zombies — the new
        # partition owner re-processes their messages, so delivering their
        # results or committing their offsets here would double-write /
        # clobber the new owner's progress.
        self._deliveries_suppressed = False

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

        Signals _run() to exit its main loop via ``signal_stop`` (the
        single documented way to clear ``_running``), then waits up to 10s
        for natural exit before force-cancelling.
        """
        self.signal_stop()
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

    def suppress_deliveries(self) -> None:
        """Mark in-flight tasks as zombies: no further sink deliveries or
        offset commits from this processor.

        Called by the lifecycle when a revoke/shutdown drain times out.
        Tasks that complete afterwards belong to a partition this worker
        no longer owns — the new owner replays from the last committed
        offset, so any delivery here would be a guaranteed double-write
        and any commit could clobber the new owner's progress.
        """
        self._deliveries_suppressed = True

    def _note_suppressed_delivery(self, hook: str) -> None:
        suppressed_zombie_deliveries.labels(partition=str(self._partition_id)).inc()
        logger.warning(
            'zombie_delivery_suppressed',
            category='partition',
            partition=self._partition_id,
            hook=hook,
            hint='task finished after the revoke/shutdown drain timed out; '
            'the new partition owner re-processes the message — raise '
            'executor.drain_timeout_seconds if this happens routinely',
        )

    def _has_drainable_pending(self) -> bool:
        """Pending offsets that can still complete in this process.

        Stalled offsets (delivery unconfirmed) are excluded: they never
        complete here by design, and waiting on them would wedge drain
        until the timeout on every shutdown/rebalance.
        """
        return self._offset_tracker.pending_count > len(self._stalled_offsets)

    async def drain(self) -> None:
        """Wait for all in-flight work and queued messages to complete."""
        while self._queue.qsize() > 0 or self._has_drainable_pending() or self._inflight_count > 0:
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

            # wait for any in-flight tasks to complete (stalled offsets are
            # excluded — they never complete in this process by design)
            while self._inflight_count > 0 or self._has_drainable_pending():
                await asyncio.sleep(DRAIN_POLL_INTERVAL)

            # final commit
            await self._try_commit()
        except asyncio.CancelledError:
            # Re-raise so the awaiting caller (stop()) sees the true
            # termination cause instead of a clean return.
            await log.ainfo('partition_processor_cancelled')
            raise
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

        # Register offsets and deserialize BEFORE building the window so
        # the parse-error policy can drop unparseable messages from the
        # window while their offsets are already watermark-tracked.
        # Policy ('skip' keeps the message in the window with payload=None):
        arrange_started_at = time.monotonic()
        accepted: list[SourceMessage] = []
        for msg in messages:
            self._offset_tracker.register(msg.offset)
            self._handler.deserialize_message(msg)
            if msg.parse_error is not None and self._on_parse_error != 'skip':
                await self._handle_parse_failure(msg)
                continue
            accepted.append(msg)
        messages = accepted
        if not messages:
            await self._try_commit()
            return

        window = Window(
            window_id=self._window_counter,
            source_messages=messages,
            start_time=time.monotonic(),
        )

        # Create a per-message tracker BEFORE arrange() so any outcome
        # we observe afterwards (even an immediate failure) has a place
        # to land.
        for msg in messages:
            self._message_trackers[msg.offset] = MessageTracker(
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

    async def _signal_stall(self) -> None:
        """Notify the lifecycle (once) that this partition has stalled.

        The lifecycle reacts by pausing the partition in Kafka so no new
        messages are fetched — without the pause, processing would continue
        past the stall and everything after it would be re-processed (and
        re-delivered to sinks) on restart, while the offset tracker
        accumulated unacknowledged state without bound.
        """
        if self._stall_signaled or self._on_stall is None:
            return
        self._stall_signaled = True
        try:
            await self._on_stall(self._partition_id)
        except Exception as e:
            await logger.aerror(
                'partition_stall_signal_failed',
                category='partition',
                partition=self._partition_id,
                error=str(e),
                exc_info=True,
            )

    async def _stall_offset(self, offset: int) -> None:
        """Leave ``offset`` permanently pending: the watermark stalls at it
        and the message is redelivered after a restart or rebalance."""
        self._stalled_offsets.add(offset)
        delivery_stalled_offsets.labels(partition=str(self._partition_id)).inc()
        await logger.aerror(
            'offset_stalled_on_delivery_failure',
            category='partition',
            partition=self._partition_id,
            offset=offset,
            hint='offset will not commit; message redelivered after restart/rebalance',
        )
        await self._signal_stall()

    def _mark_offsets_delivery_failed(self, source_offsets: list[int]) -> None:
        """Flag every tracker owning ``source_offsets`` as undelivered.

        ``_finalize_message_tracker`` then stalls those offsets rather than
        committing past data whose delivery could not be confirmed. A task
        may fan in from several source messages, so this marks all of them.
        """
        for src_offset in source_offsets:
            affected = self._message_trackers.get(src_offset)
            if affected is not None:
                affected.delivery_failed = True

    async def _handle_parse_failure(self, msg: SourceMessage) -> None:
        """Apply the 'dlq' / 'raise' parse-error policy to one message.

        ('skip' never reaches this method — the message stays in the
        window with payload=None and the handler decides what to do.)

        'raise' propagates a MessageParseError, stopping the partition
        processor with the offset uncommitted (fail-fast, redelivery on
        restart). 'dlq' writes a ParseFailurePayload to the DLQ topic and
        completes the offset when the write is confirmed; on a failed
        write the ``dlq.on_send_failure`` strategy applies — 'drop'
        commits anyway (payload lost, loud log + metric), 'stall' leaves
        the offset uncommitted and pauses the partition.
        """
        if self._on_parse_error == 'raise':
            raise MessageParseError(
                partition=msg.partition,
                offset=msg.offset,
                error=msg.parse_error or 'unknown parse error',
            )

        payload = ParseFailurePayload(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            raw_value=msg.value.decode('utf-8', errors='replace') if msg.value else '',
            parse_error=msg.parse_error or 'unknown parse error',
        )
        error = DeliveryError(
            sink_name='parse',
            sink_type='parse_error',
            error=msg.parse_error or 'unknown parse error',
            payloads=[payload],
        )
        sent = False
        if self._dlq_send is not None:
            sent = await self._dlq_send(error, self._partition_id)
        if sent:
            self._offset_tracker.complete(msg.offset)
            await self._try_commit()
        elif self._on_dlq_failure == 'stall':
            await self._stall_offset(msg.offset)
        else:
            # 'drop': the unparseable message is lost (it had no parsed
            # payload to begin with); commit past it and keep moving.
            dlq_dropped_payloads.labels(partition=str(self._partition_id)).inc()
            await logger.acritical(
                'dlq_failure_payloads_dropped',
                category='partition',
                partition=self._partition_id,
                offset=msg.offset,
                reason='on_parse_error=dlq but the DLQ write was not confirmed',
                action='ALERT: unparseable message dropped (dlq.on_send_failure=drop)',
            )
            self._offset_tracker.complete(msg.offset)
            await self._try_commit()

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
            if collect_result and self._on_collect and self._deliveries_suppressed:
                self._note_suppressed_delivery('on_task_complete')
            elif collect_result and self._on_collect:
                try:
                    await self._on_collect(collect_result, self._partition_id)
                except SinkDeliveryFailedError as e:
                    # Delivery (including the DLQ fallback) could not be
                    # confirmed. Mark every affected message tracker so
                    # _finalize_message_tracker stalls the offset instead
                    # of committing past undelivered data. The task itself
                    # succeeded — group/window accounting proceeds normally.
                    self._mark_offsets_delivery_failed(task.source_offsets)
                    await log.aerror(
                        'task_sink_delivery_failed',
                        error=str(e),
                        source_offsets=task.source_offsets,
                        hint='affected offsets will not commit (redelivery on restart)',
                    )
                except Exception as e:
                    # Unexpected error — delivery state is unknown, so the
                    # dlq.on_send_failure strategy decides whether the
                    # watermark may advance past a payload that may never
                    # have been written: 'stall' treats unknown as
                    # undelivered (replay on restart), 'drop' commits so a
                    # deterministic handler bug cannot wedge the partition.
                    # Mirrors _finalize_message_tracker and the window path,
                    # which have always applied the strategy here.
                    #
                    # Reachable whenever validate_collect rejects a payload
                    # (a sink name that is unconfigured or ambiguous) and
                    # whenever a user's on_delivery_error re-raises. Before
                    # this branch existed such payloads were discarded —
                    # never delivered, never DLQ'd — while the offset
                    # committed anyway, silently defeating 'stall'.
                    #
                    # Re-raised so the outer handler still synthesizes a
                    # terminal task failure: that accounting is unchanged and
                    # identical on the Go backend (settleSuccess →
                    # synthesizeFailure). Only the offset decision is new.
                    if self._on_dlq_failure == 'stall':
                        self._mark_offsets_delivery_failed(task.source_offsets)
                    await log.aerror(
                        'task_sink_delivery_error',
                        category='handler',
                        error=str(e),
                        error_type=type(e).__name__,
                        source_offsets=task.source_offsets,
                        offset_action='stall' if self._on_dlq_failure == 'stall' else 'commit',
                    )
                    raise

            window.results.append(result)
            task_result = result

        except ExecutorTaskError as e:
            executor_tasks.labels(status='failed').inc()
            if e.error.kind == 'timeout':
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
            try:
                action = await self._handler.on_error(task, e.error)
            except Exception as hook_exc:
                # A broken on_error must never wedge the partition. This
                # call sits inside an ``except`` clause, so a raise here is
                # NOT caught by the sibling ``except Exception`` below —
                # Python only matches sibling handlers against exceptions
                # from the ``try`` body. The escaping exception would skip
                # the tracker-settling loop after this try/finally, leaving
                # the offset PENDING forever; because ``committable()``
                # stops at the first incomplete offset, that freezes the
                # whole partition's commit watermark and every later
                # message replays on restart.
                #
                # Degrade to SKIP: the task settles as a terminal failure,
                # the tracker completes, the watermark keeps advancing.
                # At-least-once holds either way. Matches the Go backend,
                # which contains the equivalent hook error the same way.
                action = ErrorAction.SKIP
                handler_hook_errors.labels(hook='on_error').inc()
                await log.aerror(
                    'on_error_hook_failed',
                    error=str(hook_exc),
                    error_type=type(hook_exc).__name__,
                    action='treating as skip (terminal failure)',
                    exc_info=True,
                )
            finally:
                # In a ``finally`` so a raising hook still records its
                # latency and still releases the bound contextvars —
                # otherwise ``hook``/``task_id`` leak into every later log
                # line emitted by this coroutine.
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
                kind='internal',
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
            if on_complete_result and self._on_collect and self._deliveries_suppressed:
                self._note_suppressed_delivery('on_window_complete')
            elif on_complete_result and self._on_collect:
                try:
                    await self._on_collect(on_complete_result, self._partition_id)
                except SinkDeliveryFailedError as e:
                    # The window's per-message offsets were already completed
                    # individually as each message finished, so there is
                    # nothing left to stall — the aggregate payload is the
                    # only loss. Loudest possible signal for the operator.
                    await logger.acritical(
                        'on_window_complete_delivery_failed',
                        category='sink',
                        partition=self._partition_id,
                        window_id=window.window_id,
                        error=str(e),
                        action='ALERT: window aggregate payload lost — offsets already committed',
                    )
                except Exception as e:
                    # Previously this propagated out of the fire-and-forget
                    # task and surfaced only as an unobserved-exception
                    # warning at GC time. Log it properly instead.
                    await logger.aerror(
                        'on_window_complete_sink_delivery_error',
                        category='sink',
                        partition=self._partition_id,
                        window_id=window.window_id,
                        error=str(e),
                        exc_info=True,
                    )

            # Per-message offsets were already marked complete individually
            # by _finalize_message_tracker as each message's tasks finished.
            # Just make sure any outstanding commit attempt goes through in
            # case the last commit was blocked.
            offset_lag.labels(partition=str(self._partition_id)).set(self._offset_tracker.pending_count)
            await self._try_commit()

    async def _finalize_message_tracker(self, tracker: MessageTracker) -> None:
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

        delivery_failed = tracker.delivery_failed
        if on_complete_result and self._on_collect and self._deliveries_suppressed:
            self._note_suppressed_delivery('on_message_complete')
        elif on_complete_result and self._on_collect:
            try:
                await self._on_collect(on_complete_result, self._partition_id)
            except SinkDeliveryFailedError as e:
                # Delivery (including the DLQ fallback) could not be
                # confirmed for the per-message aggregate payload. Only
                # raised under dlq.on_send_failure=stall — under 'drop'
                # the DLQ-failure handler already logged + counted the
                # loss and returned normally.
                delivery_failed = True
                await logger.aerror(
                    'on_message_complete_delivery_failed',
                    category='sink',
                    partition=self._partition_id,
                    offset=tracker.source_message.offset,
                    error=str(e),
                )
            except Exception as e:
                # Unexpected error — delivery state is unknown. The
                # dlq.on_send_failure strategy decides: 'stall' treats
                # unknown as undelivered (replay on restart), 'drop'
                # logs loudly and commits so a deterministic handler bug
                # cannot wedge the partition.
                if self._on_dlq_failure == 'stall':
                    delivery_failed = True
                await logger.aerror(
                    'on_message_complete_sink_delivery_failed',
                    category='handler',
                    partition=self._partition_id,
                    offset=tracker.source_message.offset,
                    error=str(e),
                    offset_action='stall' if self._on_dlq_failure == 'stall' else 'commit',
                    exc_info=True,
                )

        if delivery_failed:
            # Do NOT complete the offset: the watermark stalls at this
            # message and it is redelivered after a restart or rebalance.
            # The tracker stays registered so the stall is inspectable.
            await self._stall_offset(tracker.source_message.offset)
            return

        # Mark the offset complete on the watermark tracker and try to
        # advance the commit. Per-message commit granularity — a slow task
        # in a later message does not pin offsets of already-finished ones.
        self._offset_tracker.complete(tracker.source_message.offset)
        offset_lag.labels(partition=str(self._partition_id)).set(self._offset_tracker.pending_count)
        await self._try_commit()

        # Release tracker memory once its offset is committable.
        self._message_trackers.pop(tracker.source_message.offset, None)

    async def _try_commit(self) -> None:
        """Commit offsets if the watermark has advanced.

        The whole read → commit → acknowledge sequence runs under
        ``_commit_lock``. Without it, two task completions can interleave
        across the ``await`` on the commit RPC: A reads watermark 6 and
        starts its round-trip, B reads 8 and starts its own, B's lands
        first, then A's ``acknowledge_commit(6)`` overwrites the tracker's
        ``_last_committed`` with the smaller value — and because the two
        RPCs are in flight concurrently, the broker itself can apply them
        out of order and move the group's committed offset backwards.
        Offsets 6-7 would then be reprocessed if the worker died in that
        window. Holding the lock also means each waiter re-reads a fresh
        watermark, so a queued commit never re-sends a stale one.
        """
        if self._deliveries_suppressed:
            # Zombie path: the partition belongs to another worker now.
            # Committing here could clobber the new owner's progress.
            return
        async with self._commit_lock:
            committable = self._offset_tracker.committable()
            if committable is None:
                return
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
