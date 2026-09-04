"""Per-partition processor for Drakkar framework."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import taskflow
from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler, overridden_completion_hooks
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.metrics import (
    batch_duration,
    delivery_stalled_offsets,
    dlq_dropped_payloads,
    executor_duration,
    executor_pool_active,
    executor_spawn_duration,
    executor_tasks,
    executor_timeouts,
    handler_duration,
    handler_hook_errors,
    messages_consumed,
    observe_task_labels,
    offset_lag,
    partition_processor_deaths,
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
from drakkar.throughput import ThroughputTracker

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

# How long the loop blocks on an empty queue before waking to retry an
# outstanding commit. It bounds two things: how long a quiet partition holds
# committable progress, and how long ``stop()`` waits for a loop that is
# sitting on the queue. Named rather than inline so the test suite can shorten
# it — otherwise every test that stops a running processor pays up to a
# second of real waiting for nothing.
IDLE_POLL_TIMEOUT = 1.0

# How long ``stop()`` waits for the processing loop to leave on its own before
# force-cancelling it. Callers with their own deadline pass a smaller value.
DEFAULT_STOP_TIMEOUT = 10.0

# Tasks created per loop turn when fanning out a window. The loop is handed
# back between chunks so a large window cannot monopolise it; the chunk is the
# unit of stall, so it must stay small enough that one turn is unnoticeable
# (~2 ms at the measured per-task cost) and large enough that the yields do not
# dominate. Not configurable: it trades one fixed cost against another and has
# no workload-dependent right answer.
FANOUT_CHUNK_TASKS = 256

# Offset-commit coalescing. A commit is a broker round trip, and it used to
# happen once per finished message: with a low-fan-out handler that makes the
# commit rate the bottleneck, and completions queue behind the partition's
# commit lock waiting for it. Deferring is always safe for at-least-once —
# it can only make the worker redo work after a crash, never skip it — so
# the watermark, the tracker and the ordering guarantees are untouched;
# only the frequency drops.
#
# Whichever trigger fires first wins. The count bounds how much work a crash
# can cost; the delay bounds how long a quiet partition holds its progress.
# Note that a commit carries ONE offset per partition (the watermark), so
# the count is how far the watermark moved, not how big the request gets.
COMMIT_BATCH_MAX_OFFSETS = 300
COMMIT_BATCH_MAX_DELAY_SECONDS = 0.5

# How many times a partition's processing loop is restarted after an
# unexpected error before the partition is declared dead. One restart
# absorbs a transient fault; a loop that dies twice is failing
# deterministically, and restarting it forever would bury the fault under
# an endless error stream instead of surfacing it on /readyz.
PARTITION_RESTART_LIMIT = 1


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
    - ``task_ids`` mirrors ``total_tasks`` in length — the full
      scheduled history, including replaced originals. Used for
      debugging / tracing the replacement chain via ``parent_task_id``.
      Retries are NOT appended here (the same ``ExecutorTask`` instance
      is re-run). Ids rather than the tasks themselves: a task holds its
      whole stdin, and a window that outlives its tasks would pin every
      byte of it until the last task of the window settles.
    - ``results`` contains the ``ExecutorResult`` of every task
      invocation that reached a terminal outcome — success OR
      subprocess-level failure (exit_code != 0, SKIP'd or retries
      exhausted). Replaced originals do NOT contribute — their slot
      in ``task_ids`` has no corresponding entry in ``results``. So
      ``len(results)`` can be less than ``total_tasks`` whenever any
      task was replaced; the gap equals the number of replaced tasks.

      **Populated only when the handler implements
      ``on_window_complete``** — that hook is the only reader. A result
      holds the task's stdout, stderr and the task itself (stdin
      included), and the window holds them until its last task settles,
      so accumulating them for a hook that does nothing costs the whole
      window's payload in resident memory for no gain. When the hook is
      not implemented the list stays empty and only the counters move;
      ``completed_count`` and ``total_tasks`` are unaffected either way.

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
    task_ids: list[str] = field(default_factory=list)
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

    ``tasks``, ``results`` and ``errors`` exist for the ``MessageGroup``
    handed to ``on_message_complete``, so they are populated only when the
    handler implements that hook — one message's fan-out can be a thousand
    tasks, each holding its stdin and its captured output. The counters
    below are kept either way: the recorder reports them for every message
    and they are three integers, not the payloads.
    """

    source_message: SourceMessage
    tasks: list[ExecutorTask] = field(default_factory=list)
    results: list[ExecutorResult] = field(default_factory=list)
    errors: list[ExecutorError] = field(default_factory=list)
    # Outcome counts, always maintained — the same numbers the lists above
    # would give through ``MessageGroup.total`` / ``.succeeded`` / ``.failed``
    # when the lists are being kept.
    scheduled_count: int = 0
    succeeded_count: int = 0
    failed_count: int = 0
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
        throughput: ThroughputTracker | None = None,
    ) -> None:
        self._partition_id = partition_id
        self._handler = handler
        self._executor_pool = executor_pool
        self._window_size = window_size
        self._max_retries = max_retries
        self._on_collect = on_collect
        self._on_commit = on_commit
        self._recorder = recorder
        self._throughput = throughput
        self._on_parse_error = on_parse_error
        self._dlq_send = dlq_send
        # dlq.on_send_failure strategy: 'drop' commits past lost payloads,
        # 'stall' leaves their offsets uncommitted and pauses the partition.
        self._on_dlq_failure = on_dlq_failure
        self._on_stall = on_stall
        self._stall_signaled = False

        # Whether anything will ever read the outcomes the window and the
        # message trackers would collect. Both lists cost the payload of
        # every task in them — stdin on the task, stdout and stderr on the
        # result — held until the window's last task settles, which at a
        # large fan-out is the dominant memory the worker uses. Asked once
        # here rather than per task: the answer is a property of the
        # handler's class and cannot change while the processor runs.
        implemented = overridden_completion_hooks(handler)
        self._keep_window_results = implemented['window_complete']
        self._keep_message_details = implemented['message_complete']

        self._queue: asyncio.Queue[SourceMessage] = asyncio.Queue()
        self._offset_tracker = OffsetTracker()
        # Serializes the read → commit → acknowledge sequence in
        # _commit_now, which is called concurrently from every task
        # completion as well as the run loop. See _commit_now's docstring.
        self._commit_lock = asyncio.Lock()
        # Pending coalesced-commit timer (see COMMIT_BATCH_MAX_OFFSETS). At
        # most one is alive at a time; ``stop`` cancels it after the forced
        # final commit has already flushed whatever it was waiting to send.
        self._commit_flush_task: asyncio.Task | None = None
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
        # Set when the run loop has exhausted its restart budget and this
        # partition is permanently stalled in this process. Surfaced on
        # ``/readyz`` so the pod is taken out of rotation — see ``is_dead``.
        self._dead = False
        self._death_reason = ''

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def is_dead(self) -> bool:
        """True when the processing loop gave up and this partition is stalled.

        The loop is restarted once after an unexpected error (see
        :meth:`_supervise`); a second death sets this flag. Nothing drains
        the queue afterwards, so the worker reports itself unready rather
        than holding an assignment it cannot serve.
        """
        return self._dead

    @property
    def death_reason(self) -> str:
        """Message from the error that killed the loop, or '' while alive."""
        return self._death_reason

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
        """Start the partition processing loop under its supervisor."""
        self._running = True
        self._task = asyncio.create_task(self._supervise())

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

    async def stop(self, timeout: float = DEFAULT_STOP_TIMEOUT) -> None:
        """Stop the partition processor and wait for completion.

        Signals _run() to exit its main loop via ``signal_stop`` (the
        single documented way to clear ``_running``), then waits up to
        ``timeout`` for natural exit before force-cancelling.

        ``timeout`` exists so a caller working against its own deadline —
        the rebalance callback, which blocks librdkafka's thread — can hand
        down what is left of its budget instead of always paying the
        default.
        """
        self.signal_stop()
        if self._deliveries_suppressed:
            # Zombie path. These tasks already had the whole drain budget and
            # their results are discarded on arrival, so waiting on them below
            # would burn the full ``timeout`` for nothing — and ``_run`` cannot
            # leave its drain loop while they are counted in flight, so that
            # wait always expires. Cancelling frees their executor slots and
            # kills their subprocesses now.
            await self.cancel_active_tasks()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass
            self._task = None
        # Cancelling ``_run`` above does not reach the task coroutines it
        # spawned — they are separate tasks. Anything still running here
        # would hold an executor slot and a subprocess with no owner left to
        # read its result.
        await self.cancel_active_tasks()
        # ``_run`` ends with a forced commit, so by here there is nothing a
        # pending coalescing timer still needs to send — but the loop may
        # have been cancelled before reaching it, and a processor that was
        # never started has no ``_run`` at all. Flush, then retire the timer
        # so it cannot outlive the processor.
        await self._commit_now()
        await self._cancel_commit_flush()

    async def _cancel_commit_flush(self) -> None:
        """Stop the pending coalescing timer and wait for it to unwind."""
        task, self._commit_flush_task = self._commit_flush_task, None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

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

    async def cancel_active_tasks(self) -> int:
        """Cancel every in-flight task coroutine and wait for it to unwind.

        Returns how many were cancelled. Idempotent, and a no-op on a
        processor that drained cleanly.

        Cancellation propagates into ``ExecutorPool.execute``, whose
        ``finally`` kills the subprocess' process group and releases the
        priority-gate slot — which is the point. Without it a zombie holds
        its slot until ``task_timeout_seconds`` (two minutes by default)
        while the partitions this worker still owns queue behind it.

        ``_execute_and_track``'s ``finally`` decrements the in-flight count
        and pops ``_pending_tasks`` on the way out, but ``CancelledError``
        is a ``BaseException``: it propagates past the tracker settlement
        that follows, so a cancelled task's offsets stay pending and are
        never committed. That is deliberate — committing past work this
        worker abandoned would lose it.
        """
        tasks = [t for t in self._active_tasks if not t.done()]
        if not tasks:
            return 0
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await logger.awarning(
            'zombie_tasks_cancelled',
            category='partition',
            partition=self._partition_id,
            cancelled=len(tasks),
            hint='tasks outlived the drain timeout; their executor slots and '
            'subprocesses are released and their offsets stay uncommitted — '
            'raise executor.drain_timeout_seconds if this happens routinely',
        )
        return len(tasks)

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
        """Wait for all in-flight work and queued messages to complete,
        then commit the progress that work earned.

        The commit is forced rather than coalesced. A caller drains because
        it is about to give the partition up — a rebalance revoke, a
        shutdown — and every offset left sitting in the coalescing window at
        that moment is redelivered to the next owner for nothing. Draining
        without committing would also make the wait pointless: the work is
        done but no record of it survives.

        A dead processor returns at once. Its loop gave up, so nothing will
        ever empty its queue or settle its pending offsets; waiting would
        burn the caller's whole drain budget and then suppress a commit that
        was in fact safe to make.
        """
        if self._dead or self._deliveries_suppressed:
            await self._commit_now()
            return
        while self._queue.qsize() > 0 or self._has_drainable_pending() or self._inflight_count > 0:
            await asyncio.sleep(DRAIN_POLL_INTERVAL)
        await self._commit_now()

    async def _run(self) -> None:
        log = logger.bind(partition=self._partition_id, category='partition')
        await log.ainfo('partition_processor_started')

        try:
            while self._running:
                messages = await self._collect_window()
                if not messages:
                    # retry any uncommitted offsets on idle iterations
                    await self._note_commit_due()
                    continue

                await self._process_window(messages)

            # Drain remaining queued messages after _running becomes False,
            # in window_size chunks. One unbounded window here would hand
            # arrange() the whole backlog at every shutdown/rebalance —
            # a memory spike, and quadratic pain for O(n²) arrange hooks.
            while self._queue.qsize() > 0 and not self._deliveries_suppressed:
                messages = []
                while len(messages) < self._window_size:
                    try:
                        messages.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if messages:
                    await self._process_window(messages)

            # wait for any in-flight tasks to complete (stalled offsets are
            # excluded — they never complete in this process by design).
            #
            # A suppressed processor exits at once. Its drain already timed
            # out and its tasks were cancelled, so their offsets stay pending
            # by design (nothing settles a cancelled task's tracker) and this
            # condition would never clear — the loop would spin until the
            # caller force-cancelled it, which is exactly the ten seconds
            # every ``stop()`` used to pay after a revoke.
            while not self._deliveries_suppressed and (self._inflight_count > 0 or self._has_drainable_pending()):
                await asyncio.sleep(DRAIN_POLL_INTERVAL)

            # Final commit — forced, not coalesced: this is the last chance
            # to record progress before the partition is released, and any
            # offset left uncommitted here is redelivered for nothing.
            await self._commit_now()
        except asyncio.CancelledError:
            # Re-raise so the awaiting caller (stop()) sees the true
            # termination cause instead of a clean return.
            await log.ainfo('partition_processor_cancelled')
            raise
        # No generic ``except Exception`` here: _supervise owns crash
        # handling. Swallowing it at this level is what used to make a
        # dead loop invisible — the task completed successfully, so
        # nothing restarted it and nothing reported it.

    async def _supervise(self) -> None:
        """Run the processing loop, restarting it once if it dies unexpectedly.

        A partition loop that exits on an unexpected error takes its
        partition out of the pipeline *silently*: Kafka still assigns it,
        ``enqueue`` still accepts messages, and the queue grows with
        nothing draining it. Offsets stop committing, so the lag climbs
        until the consumer is evicted for a poll timeout — with no signal
        that names the actual cause.

        The policy is restart-once, then give up:

        * **First death** — log an error, count it as ``restarted``, and
          start the loop again. The queue, offset tracker, and pending
          state are all preserved, so buffered messages are processed and
          uncommitted offsets are retried. This covers the transient case
          (a blip in a sink, a momentary resource failure).
        * **Second death** — mark the partition dead, log CRITICAL, count
          it as ``dead``, and stop. A loop that dies twice is failing
          deterministically; restarting it forever would hide the fault
          behind an endless error stream. ``/readyz`` then fails, naming
          this partition, so orchestration replaces the pod and the
          partition is reassigned to a healthy worker.

        A crash *after* ``_running`` goes false (i.e. during the shutdown
        drain) is never restarted — the loop was on its way out anyway,
        and a restart would re-enter a drain that has already been
        accounted for.

        **What a restart does not fix.** ``_process_window`` registers
        each offset *before* arrange runs, so a crash there leaves those
        offsets PENDING for the life of the process, and ``committable()``
        stops at the first incomplete offset. The restarted loop keeps
        processing, but its commit watermark never advances past the
        window that died — lag climbs until a rebalance or restart hands
        those offsets to an owner that redelivers them. That is the
        correct at-least-once outcome (the messages were never processed,
        so committing past them would lose them), not an oversight: a
        restart buys continued processing, not recovery of the lost
        window.
        """
        log = logger.bind(partition=self._partition_id, category='partition')
        label = str(self._partition_id)
        restarts = 0
        while True:
            try:
                await self._run()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if restarts >= PARTITION_RESTART_LIMIT or not self._running:
                    self._dead = True
                    self._death_reason = str(e)
                    partition_processor_deaths.labels(partition=label, outcome='dead').inc()
                    await log.acritical(
                        'partition_processor_died',
                        error=str(e),
                        restarts=restarts,
                        impact='partition is no longer processed; queued messages are not drained '
                        'and offsets are not committed. The worker now fails /readyz so it can be '
                        'replaced and the partition reassigned.',
                        exc_info=True,
                    )
                    return
                restarts += 1
                partition_processor_deaths.labels(partition=label, outcome='restarted').inc()
                await log.aerror(
                    'partition_processor_restarting',
                    error=str(e),
                    restarts=restarts,
                    exc_info=True,
                )

    async def _collect_window(self) -> list[SourceMessage]:
        """Collect up to window_size messages from the queue."""
        messages: list[SourceMessage] = []

        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=IDLE_POLL_TIMEOUT)
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
            await self._note_commit_due()
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
        hook_token = bind_hook_context(
            hook='arrange',
            partition=self._partition_id,
            window_id=self._window_counter,
            offsets=tuple(offsets),
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
            clear_hook_context(hook_token)
        arrange_duration = time.monotonic() - self._arrange_start
        handler_duration.labels(hook='arrange').observe(arrange_duration)
        if self._recorder:
            self._recorder.record_arranged(
                self._partition_id,
                messages,
                tasks,
                duration=arrange_duration,
                message_labels=arrange_labels,
                window_id=window.window_id,
            )
        window.task_ids = [task.task_id for task in tasks]
        window.total_tasks = len(tasks)

        # Register each task with every message tracker it belongs to
        # (by source_offsets). A task with multiple source_offsets
        # participates in multiple groups — its terminal outcome is
        # reported to all of them.
        for task in tasks:
            for offset in task.source_offsets:
                tracker = self._message_trackers.get(offset)
                if tracker is not None:
                    if self._keep_message_details:
                        tracker.tasks.append(task)
                    tracker.scheduled_count += 1
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
            await self._note_commit_due()
            return

        # Fan out in chunks, handing the loop back between them.
        #
        # There used to be no await in this loop, and asyncio runs every ready
        # handle before it polls I/O again — so creating N tasks also ran the
        # first step of all N coroutines back to back: the context binds, the
        # metric lookups, the priority computation and the gate's heap push.
        # Measured at ~9 µs per task, which is a tenth of a second for a
        # 10 000-task window and close to a second for 100 000, during which
        # the worker does nothing else: no Kafka poll, no sink delivery, no UI
        # frame, no health sample. A window's task count is bounded only by
        # what the handler returns, so the stall grew with the message shape.
        #
        # Registration rides in the same chunk: a dict insert is cheap, but
        # 100 000 of them in one turn is not.
        #
        # Yielding mid-fan-out means a task from an earlier chunk can complete
        # while later chunks are still being created. That is safe because
        # ``window.total_tasks`` and every tracker's ``remaining`` were set for
        # the whole list above, so neither the window nor a message group can
        # reach a completed state early.
        for start in range(0, len(tasks), FANOUT_CHUNK_TASKS):
            for task in tasks[start : start + FANOUT_CHUNK_TASKS]:
                if task.task_id in self._pending_tasks:
                    logger.warning(
                        'duplicate_task_id_in_pending',
                        category='partition',
                        partition=self._partition_id,
                        task_id=task.task_id,
                    )
                self._pending_tasks[task.task_id] = task
                self._inflight_count += 1
                t = asyncio.create_task(self._execute_and_track(task, window))
                self._active_tasks.add(t)
                t.add_done_callback(self._active_tasks.discard)
            if start + FANOUT_CHUNK_TASKS < len(tasks):
                await asyncio.sleep(0)

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
        logger.error(
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
            await self._note_commit_due()
        elif self._on_dlq_failure == 'stall':
            await self._stall_offset(msg.offset)
        else:
            # 'drop': the unparseable message is lost (it had no parsed
            # payload to begin with); commit past it and keep moving.
            dlq_dropped_payloads.labels(partition=str(self._partition_id)).inc()
            logger.critical(
                'dlq_failure_payloads_dropped',
                category='partition',
                partition=self._partition_id,
                offset=msg.offset,
                reason='on_parse_error=dlq but the DLQ write was not confirmed',
                action='ALERT: unparseable message dropped (dlq.on_send_failure=drop)',
            )
            self._offset_tracker.complete(msg.offset)
            await self._note_commit_due()

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
            if result.spawn_seconds is not None:
                executor_spawn_duration.observe(result.spawn_seconds)
            observe_task_labels(task.labels)
            # Contract v1.16: counted successful completions carry cost and
            # speed. The tracker owns the label-side counting rules; the
            # failed and precomputed paths never reach this call.
            cost_speed = (
                self._throughput.observe_completion(task.labels, result.duration_seconds) if self._throughput else None
            )
            if self._recorder:
                self._recorder.record_task_completed(
                    result,
                    self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                    cost=cost_speed[0] if cost_speed else None,
                    speed=cost_speed[1] if cost_speed else None,
                )

            bind_contextvars(hook='on_task_complete', task_id=task.task_id)
            hook_token = bind_hook_context(
                hook='on_task_complete',
                partition=self._partition_id,
                window_id=window.window_id,
                task_id=task.task_id,
            )
            collect_start = time.monotonic()
            try:
                collect_result = await self._handler.on_task_complete(result)
            finally:
                # Both releases belong in the ``finally``: this hook's
                # exception propagates (the caller synthesizes a failure from
                # it), so releasing after the ``try`` would leak ``hook`` and
                # ``task_id`` into every later log line this coroutine emits.
                unbind_contextvars('hook', 'task_id')
                clear_hook_context(hook_token)
            collect_duration = time.monotonic() - collect_start
            handler_duration.labels(hook='on_task_complete').observe(collect_duration)
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
                    # terminal task failure: that accounting is unchanged.
                    # Only the offset decision is new.
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

            if self._keep_window_results:
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
            # Sync, not ``await log.awarning``: structlog's async variants
            # copy the context and hop through the default thread pool, which
            # measured ~8.5x the cost of the sync call (83us vs 10us) and adds
            # a scheduling point. This line fires once per FAILED task, so the
            # cost peaks exactly when the worker is already struggling — the
            # same reasoning applies to the other per-task/per-batch log
            # statements in the delivery path.
            log.warning('executor_task_failed', error=str(e))

            bind_contextvars(hook='on_error', task_id=task.task_id)
            try:
                call = await taskflow.call_on_error(
                    self._handler,
                    task,
                    e.error,
                    partition=self._partition_id,
                    window_id=window.window_id,
                )
            finally:
                # ``hook``/``task_id`` would otherwise leak into every later
                # log line emitted by this coroutine. (``call_on_error``
                # owns the hook context and the timing; these are structlog's
                # separate store — see drakkar/hookctx.py on why both exist.)
                unbind_contextvars('hook', 'task_id')
            handler_duration.labels(hook='on_error').observe(call.duration_seconds)
            if call.failed:
                # A broken on_error must never wedge the partition. The hook
                # is invoked from inside this ``except`` clause, so an
                # escaping exception would skip the tracker-settling loop
                # below, leaving the offset PENDING forever; because
                # ``committable()`` stops at the first incomplete offset,
                # that freezes the whole partition's commit watermark and
                # every later message replays on restart. ``call_on_error``
                # therefore returns the exception instead of raising it.
                #
                # Degrade to SKIP: the task settles as a terminal failure,
                # the tracker completes, the watermark keeps advancing.
                # At-least-once holds either way.
                action: Any = ErrorAction.SKIP
                handler_hook_errors.labels(hook='on_error').inc()
                await log.aerror(
                    'on_error_hook_failed',
                    error=str(call.exception),
                    error_type=type(call.exception).__name__,
                    action='treating as skip (terminal failure)',
                    exc_info=call.exception,
                )
            else:
                action = call.action
            decision = taskflow.decide_after_on_error(
                action,
                retry_count=retry_count,
                max_retries=self._max_retries,
            )
            if decision.outcome is taskflow.TaskOutcome.REPLACE:
                # Replacement: the original task is "replaced" (not a
                # terminal failure of the group). Decrement its contribution
                # to every message tracker; add the replacements.
                for new_task in taskflow.link_replacements(task, list(decision.replacements)):
                    self._pending_tasks[new_task.task_id] = new_task
                    window.task_ids.append(new_task.task_id)
                    window.total_tasks += 1
                    self._inflight_count += 1
                    # Register the replacement with every message tracker
                    # listed in its source_offsets (usually inherited from
                    # the original failing task — it's the handler's
                    # responsibility to set source_offsets on replacements).
                    for offset in new_task.source_offsets:
                        tracker = self._message_trackers.get(offset)
                        if tracker is not None:
                            if self._keep_message_details:
                                tracker.tasks.append(new_task)
                            tracker.scheduled_count += 1
                            tracker.remaining += 1
                    t = asyncio.create_task(self._execute_and_track(new_task, window))
                    self._active_tasks.add(t)
                    t.add_done_callback(self._active_tasks.discard)
            elif decision.outcome is taskflow.TaskOutcome.RETRY:
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
                if decision.retries_exhausted:
                    await log.awarning(
                        'max_retries_exceeded',
                        task_id=task.task_id,
                        retries=retry_count,
                    )
                if self._keep_window_results:
                    window.results.append(e.result)
                task_error = e.error

        except Exception as e:
            await log.aerror('unexpected_error_in_task', error=str(e), exc_info=True)
            # Still count as completed so the window can progress, and
            # synthesize an ExecutorError so the message tracker sees a
            # terminal failure for this task (the group treats unexpected
            # exceptions like retries-exhausted failures).
            synthesized_result, task_error = taskflow.synthesize_internal_failure(task, e)
            if self._keep_window_results:
                window.results.append(synthesized_result)

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
                tracker.succeeded_count += 1
                if self._keep_message_details:
                    tracker.results.append(task_result)
            elif task_error is not None:
                tracker.failed_count += 1
                if self._keep_message_details:
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
            hook_token = bind_hook_context(
                hook='on_window_complete',
                partition=self._partition_id,
                window_id=window.window_id,
                offsets=tuple(m.offset for m in window.source_messages),
            )
            wc_start = time.monotonic()
            try:
                on_complete_result = await self._handler.on_window_complete(window.results, window.source_messages)
            except Exception as e:
                # Log and move on — mirrors on_message_complete below. This
                # runs inside a fire-and-forget task, so a raising hook would
                # otherwise skip the window's recorder event, delivery, and
                # the final _try_commit, surfacing only as an
                # unretrieved-task-exception warning at GC time.
                await logger.aerror(
                    'on_window_complete_failed',
                    category='handler',
                    partition=self._partition_id,
                    window_id=window.window_id,
                    error=str(e),
                    exc_info=True,
                )
                on_complete_result = None
            finally:
                # Released together in the ``finally`` — see on_task_complete
                # above for why unbinding after the ``try`` leaks.
                unbind_contextvars('hook', 'window_id')
                clear_hook_context(hook_token)
            wc_duration = time.monotonic() - wc_start
            handler_duration.labels(hook='on_window_complete').observe(wc_duration)
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
            await self._note_commit_due()

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
        # No window_id: a message tracker outlives its arrange() window (the
        # last task of a fan-out can settle long after the window closed), so
        # there is no single window this hook belongs to.
        hook_token = bind_hook_context(
            hook='on_message_complete',
            partition=self._partition_id,
            offset=tracker.source_message.offset,
            offsets=(tracker.source_message.offset,),
        )
        mc_start = time.monotonic()
        try:
            on_complete_result = await self._handler.on_message_complete(group)
        except Exception as e:
            # Log and move on — raising here would block the offset from
            # committing, stalling the partition behind a handler bug.
            logger.error(
                'on_message_complete_failed',
                category='handler',
                partition=self._partition_id,
                offset=tracker.source_message.offset,
                error=str(e),
                exc_info=True,
            )
            on_complete_result = None
        finally:
            # The ``except`` above catches Exception, but NOT BaseException —
            # a cancellation during the hook would skip an unbind placed after
            # the block and leak ``hook``/``offset`` into every later log line
            # this coroutine emits.
            unbind_contextvars('hook', 'offset')
            clear_hook_context(hook_token)
        mc_duration = time.monotonic() - mc_start
        handler_duration.labels(hook='on_message_complete').observe(mc_duration)

        if self._recorder:
            # From the tracker's counters, not the group's list lengths: the
            # lists are populated only for a handler that implements
            # on_message_complete, but this event is recorded for every
            # message. The arithmetic is MessageGroup's own — replaced tasks
            # are the ones that were scheduled and never reached a terminal
            # outcome of their own.
            replaced = max(0, tracker.scheduled_count - tracker.succeeded_count - tracker.failed_count)
            self._recorder.record_message_complete(
                partition=self._partition_id,
                offset=tracker.source_message.offset,
                duration=mc_duration,
                task_count=tracker.scheduled_count,
                succeeded=tracker.succeeded_count,
                failed=tracker.failed_count,
                replaced=replaced,
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
                logger.error(
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
                logger.error(
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
        await self._note_commit_due()

        # Release tracker memory once its offset is committable.
        self._message_trackers.pop(tracker.source_message.offset, None)

    async def _note_commit_due(self) -> None:
        """A watermark advance happened; commit now or shortly.

        The coalescing entry point. Every caller that merely *finished
        something* goes through here; only the paths that must not leave
        progress unsent — the drain's final commit — call ``_commit_now``
        directly.

        Deferring is safe in the direction that matters: a commit that
        arrives late can only cause reprocessing after a crash, which
        at-least-once already allows. Committing early is what would lose
        messages, and nothing here does that.
        """
        if self._deliveries_suppressed:
            # Zombie path: the partition belongs to another worker now.
            return
        advance = self._offset_tracker.uncommitted_advance()
        if advance <= 0:
            return
        if advance >= COMMIT_BATCH_MAX_OFFSETS:
            await self._commit_now()
            return
        self._schedule_commit_flush()

    def _schedule_commit_flush(self) -> None:
        """Ensure a timer is running that will flush the deferred commit.

        One timer at a time: the first deferral after a commit starts it,
        later ones ride along, so the delay is measured from the OLDEST
        uncommitted advance rather than being pushed back by every new one.
        """
        if self._commit_flush_task is not None and not self._commit_flush_task.done():
            return
        self._commit_flush_task = asyncio.create_task(self._commit_after_delay())

    async def _commit_after_delay(self) -> None:
        """Sleep out the coalescing window, then commit whatever accumulated."""
        try:
            await asyncio.sleep(COMMIT_BATCH_MAX_DELAY_SECONDS)
            await self._commit_now()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A failed deferred commit is not fatal: the offsets stay
            # pending and the next completion (or the drain) retries. It
            # must not, however, take the timer down silently.
            logger.warning(
                'commit_flush_failed',
                category='kafka',
                partition=self._partition_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    async def _commit_now(self) -> None:
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
