"""Per-partition processor for Drakkar framework."""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import taskflow
from drakkar.budget import TASKS_LIMIT, BudgetAccount, MessageShape, PendingBudget, output_bytes, text_bytes
from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler, overridden_completion_hooks, task_run_seconds_hook
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
    queued_messages_dropped_at_stop,
    suppressed_zombie_deliveries,
    task_retries,
    window_admission_wait,
    window_messages,
)
from drakkar.models import (
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    MessageParseError,
    PendingContext,
    SinkDeliveryFailedError,
    SourceMessage,
)
from drakkar.offsets import OffsetTracker
from drakkar.recorder import EventRecorder
from drakkar.reported_run import reported_run_seconds
from drakkar.request_ids import RequestIdResolver, reset_request_id
from drakkar.sinks.dlq_record import DLQRecord
from drakkar.sinks.manager import CollectCallback, MessageContext, MessageDLQState

logger = structlog.get_logger()

CommitCallback = Callable[[int, int], Awaitable[None]]
# Matches DLQSink.send(record, partition_id) -> bool (True = confirmed write).
DLQSendCallback = Callable[[DLQRecord, int], Awaitable[bool]]
# Invoked once (per processor lifetime) when the first offset stalls under
# dlq.on_send_failure=stall — the lifecycle pauses the partition in Kafka.
StallCallback = Callable[[int], Awaitable[None]]


class ExecuteDLQCallback(Protocol):
    """Write the ``execute`` record of a task's source messages (``SinkManager.send_execute_dlq``)."""

    def __call__(
        self,
        message_context: MessageContext | None,
        *,
        task: ExecutorTask,
        error: ExecutorError,
        attempts: int,
        partition_id: int,
    ) -> Awaitable[None]: ...


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

# Offset-commit coalescing. A commit is a broker round trip; committing once
# per finished message turns the commit rate into the bottleneck under a
# low-fan-out handler, with completions queuing behind the partition's
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

# Windows that created tasks a partition must measure itself before its window
# size adapts. Until then each window takes one message, so a jump of the
# message shape after the first sample passes the cap by one message, not by a
# whole window sized on that sample.
WARM_UP_WINDOWS = 3


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
    # Bytes of the stdout and stderr in ``results``, charged to the pending
    # budget until on_window_complete has run.
    kept_bytes: int = 0

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
    # Shared by every task result and the message result of this message:
    # once its DLQ record is written, no later delivery of it starts.
    dlq: MessageDLQState = field(default_factory=MessageDLQState)
    # Bytes of the stdout and stderr in ``results`` and ``errors``, charged to
    # the pending budget until on_message_complete has run.
    kept_bytes: int = 0


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
        execute_dlq: ExecuteDLQCallback | None = None,
        executor_on_failure: str = 'skip',
        budget: PendingBudget | None = None,
        request_ids: RequestIdResolver | None = None,
    ) -> None:
        self._partition_id = partition_id
        # Gives each message its request id and binds ids into the log
        # context of every scope.
        self._request_ids = request_ids if request_ids is not None else RequestIdResolver(handler)
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
        # Writes the record of a task that on_error sent to the DLQ.
        self._execute_dlq = execute_dlq
        # executor.on_failure: what a spent retry budget and a raising
        # on_error become (skip, or dlq through execute_dlq).
        self._executor_on_failure = ErrorAction(executor_on_failure)
        # The worker's pending budget. Without one (a processor built on its
        # own) windows take up to window_size messages and nothing is counted.
        self._account = BudgetAccount(budget) if budget is not None else None
        # The waiter this loop holds in the budget's FIFO list while it waits
        # for room; kept across the one-second wake-ups so the loop keeps its
        # place in the list.
        self._room_waiter: asyncio.Future[bool] | None = None
        # Per pending task object (``id``, not task_id: two pending tasks may
        # share a task_id): [pending runs, stdin bytes charged]. A task object
        # can be scheduled more than once (arrange returns it twice, on_error
        # returns it as its own replacement); its stdin and its charge are
        # freed when its LAST run settles, so no run starts without its input.
        self._task_holds: dict[int, list[int]] = {}
        # The first message of the next window, taken off the queue before the
        # loop waits for room. It still counts as queued.
        self._held_message: SourceMessage | None = None
        # Moving averages per message, from the windows this partition took.
        # Until the first window is measured, the worker-wide averages of the
        # budget size the window.
        self._shape = MessageShape()
        # Windows this partition processed, with tasks or without. Below
        # WARM_UP_WINDOWS each window takes one message. A window with no
        # tasks counts here but not in the averages, so a partition whose
        # arrange skips its messages still leaves the warm-up.
        self._windows_seen = 0
        # Whether the window being arranged holds the budget's one slot for a
        # window with no estimate.
        self._holds_unmeasured_window = False

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
        # The optional run-time hook, or None when the handler keeps the
        # default — asked once for the same reason, so a handler without it
        # pays nothing per task.
        self._task_run_seconds = task_run_seconds_hook(handler)

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
        """Messages polled and not yet taken into a window, the held first message included."""
        return self._queue.qsize() + (self._held_message is not None)

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
        # A loop waiting for room in the pending budget leaves at once.
        if self._room_waiter is not None and not self._room_waiter.done():
            self._room_waiter.set_result(False)

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
        # Give back what cancelled tasks and unfinished windows still hold.
        if self._account is not None:
            self._account.close()

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
        its slot until ``executor.task_timeout`` (two minutes by default)
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
            'raise executor.drain_timeout if this happens routinely',
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
            'executor.drain_timeout if this happens routinely',
        )

    def _has_drainable_pending(self) -> bool:
        """Pending offsets that can still complete in this process.

        Stalled offsets (delivery unconfirmed) are excluded: they never
        complete here by design, and waiting on them would wedge drain
        until the timeout on every shutdown/rebalance.
        """
        return self._offset_tracker.pending_count > len(self._stalled_offsets)

    async def drain(self) -> None:
        """Wait for in-flight work to complete and for the queue to empty,
        then commit the progress that work earned.

        The queue empties because the loop takes the messages into a window,
        or, once a stop is signalled, drops them unarranged: their offsets
        were never registered, so the commit stays below them and the next
        owner of the partition reads them again.

        The commit is forced rather than coalesced. A caller drains because
        it is about to give the partition up — a rebalance revoke, a
        shutdown — and every offset left sitting in the coalescing window at
        that moment is redelivered to the next owner for nothing. Draining
        without committing would also make the wait pointless: the work is
        done but no record of it survives.

        A dead processor returns at once. Its loop gave up, so nothing will
        ever empty its queue or settle its pending offsets; waiting would
        burn the caller's whole drain budget and then suppress a commit that
        was in fact safe to make. A processor whose loop has ended after a
        stop returns too, for the same reason: no loop reads its queue.
        """
        if self._dead or self._deliveries_suppressed:
            await self._commit_now()
            return
        while (
            self.queue_size > 0 or self._has_drainable_pending() or self._inflight_count > 0
        ) and not self._loop_ended:
            await asyncio.sleep(DRAIN_POLL_INTERVAL)
        await self._commit_now()

    @property
    def _loop_ended(self) -> bool:
        """Whether the partition loop task has returned, so nothing reads the queue any more."""
        return self._task is not None and self._task.done()

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

                try:
                    await self._process_window(messages)
                finally:
                    # The window has fanned out (or failed): what it reserved
                    # and did not charge goes back, and the next waiting loop
                    # may take the room.
                    self._end_window_reservation()

            # Queued messages that are not in a window are dropped, not
            # arranged: only in-flight work drains. Their offsets were never
            # registered, so the final commit stays below them and the next
            # owner of the partition reads them again.
            self._drop_queued_messages()

            # wait for any in-flight tasks to complete (stalled offsets are
            # excluded — they never complete in this process by design).
            #
            # A suppressed processor exits at once. Its drain already timed
            # out and its tasks were cancelled, so their offsets stay pending
            # by design (nothing settles a cancelled task's tracker) and this
            # condition would never clear — the loop would spin until the
            # caller force-cancelled it, costing the full ``stop()`` timeout
            # on every revoke.
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
        # handling. Swallowing it at this level would make a dead loop
        # invisible — the task would complete successfully, so nothing
        # would restart it and nothing would report it.

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

    def _drop_queued_messages(self) -> None:
        """Empty the queue at stop, counting the messages that were never arranged."""
        dropped: list[SourceMessage] = []
        if self._held_message is not None:
            dropped.append(self._held_message)
            self._held_message = None
        while True:
            try:
                dropped.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        partition_queue_size.labels(partition=str(self._partition_id)).set(0)
        if not dropped:
            return
        queued_messages_dropped_at_stop.inc(len(dropped))
        logger.info(
            'queued_messages_dropped_at_stop',
            category='partition',
            partition=self._partition_id,
            count=len(dropped),
            first_offset=min(msg.offset for msg in dropped),
            hint='never arranged; the next owner of the partition reads them again',
        )

    async def _collect_window(self) -> list[SourceMessage]:
        """Take the next window: up to the admitted number of messages, at most window_size.

        The first message is taken off the queue and held while the loop
        waits for room in the pending budget, so only loops that have work
        wait. Every wait is bounded by ``IDLE_POLL_TIMEOUT``: an empty list
        lets ``_run`` retry a commit and see a stop.
        """
        if self._held_message is None:
            try:
                self._held_message = await asyncio.wait_for(self._queue.get(), timeout=IDLE_POLL_TIMEOUT)
            except TimeoutError:
                return []
        if self._account is not None and not await self._wait_for_room():
            return []

        # From the room check to the reservation there is no await, so no
        # other loop can pass on the same room.
        limit = self._admitted_window_size()
        messages = [self._held_message]
        self._held_message = None
        while len(messages) < limit:
            try:
                messages.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        self._reserve_window(len(messages))

        partition_queue_size.labels(partition=str(self._partition_id)).set(self._queue.qsize())
        window_messages.observe(len(messages))
        return messages

    def _estimate(self) -> MessageShape | None:
        """The averages that size this partition's next window, or ``None`` before any window was measured."""
        assert self._account is not None
        if self._shape.measured:
            return self._shape
        worker_shape = self._account.budget.shape
        return worker_shape if worker_shape.measured else None

    def _blocking_limit(self) -> str | None:
        """Why this loop may not take a window now, or ``None`` when it may.

        The budget must have room. A loop with no estimate also needs the
        budget's one slot for an unmeasured window; while another loop holds
        it, the wait is counted under ``tasks`` (the counter has no third
        label, and it lasts only until the first window of the worker is
        measured).
        """
        assert self._account is not None
        budget = self._account.budget
        limit = budget.full_limit()
        if limit is None and self._estimate() is None and budget.unmeasured_window_open:
            return TASKS_LIMIT
        return limit

    async def _wait_for_room(self) -> bool:
        """Wait, at most ``IDLE_POLL_TIMEOUT``, until this loop may take a window.

        True when it may. False after the time bound or a stop; the loop's
        place in the budget's FIFO list is kept across the time bound and
        given up on a stop. A loop that is not waiting yet queues behind the
        waiters even when there is room, and a woken loop checks the room
        again: when another loop took it first, it queues again at the front.
        """
        assert self._account is not None
        budget = self._account.budget
        if not self._running:
            return False
        limit = self._blocking_limit()
        if self._room_waiter is None:
            if limit is None and not budget.has_waiters():
                return True
            self._room_waiter = budget.add_waiter()
        if limit is None:
            # Room exists, but older waiters come first; the oldest is woken now.
            budget.wake_next()
        waiter = self._room_waiter
        started = time.monotonic()
        # ``asyncio.wait`` does not cancel the waiter on the time bound, so the
        # same waiter keeps its place for the next call.
        done, _ = await asyncio.wait({waiter}, timeout=IDLE_POLL_TIMEOUT)
        # A loop that queued behind other waiters while there was room counts
        # its wait under the limit that last blocked a loop.
        window_admission_wait.labels(limit=limit or budget.last_reached_limit).inc(time.monotonic() - started)
        if not done:
            return False
        self._room_waiter = None
        if not waiter.result():
            return False
        if not self._running:
            # The wake-up was meant for a loop that takes a window: pass it on.
            budget.wake_next()
            return False
        if self._blocking_limit() is not None:
            self._room_waiter = budget.add_waiter(front=True)
            return False
        return True

    def _in_warm_up(self) -> bool:
        """Whether this partition has processed fewer than ``WARM_UP_WINDOWS`` windows of its own."""
        return self._windows_seen < WARM_UP_WINDOWS

    def _per_message_estimate(self, shape: MessageShape) -> tuple[float, float]:
        """Tasks and bytes to reserve per message: the averages, never below the floor.

        The floor is ``max_pending_tasks / window_size`` tasks and
        ``max_pending_bytes / window_size`` bytes, so a full window always
        reserves the whole cap. Without it, an average that is low for every
        partition (a small first message, a long run of small messages) lets
        many loops each take a full window on the same room, and the excess of
        all of them adds up.
        """
        assert self._account is not None and shape.tasks is not None
        budget = self._account.budget
        floor_tasks = math.ceil(budget.max_tasks / self._window_size)
        floor_bytes = budget.max_bytes / self._window_size
        return max(shape.tasks, floor_tasks), max(shape.held_bytes, floor_bytes)

    def _admitted_window_size(self) -> int:
        """How many messages the next window may take.

        During the warm-up (fewer than ``WARM_UP_WINDOWS`` windows of its own,
        counted whether they created tasks or not) a partition takes one
        message per window. After it,
        ``min(free tasks / tasks per message, free bytes / bytes per
        message)``, clamped to 1 .. window_size, with the per-message
        estimate of ``_per_message_estimate`` and the free room already
        excluding what other windows reserved. A window takes at least one
        message, so a loop never waits for ever; the cost is that the limits
        are passed by the error of the estimate: normally one message, more
        when the tasks per message jump after the warm-up.
        """
        if self._account is None:
            return self._window_size
        shape = self._estimate()
        if shape is None or self._in_warm_up():
            return 1
        budget = self._account.budget
        tasks_per_message, bytes_per_message = self._per_message_estimate(shape)
        fit = min(
            float(self._window_size),
            budget.free_tasks / tasks_per_message,
            budget.free_bytes / bytes_per_message,
        )
        return max(1, min(self._window_size, int(fit)))

    def _reserve_window(self, message_count: int) -> None:
        """Reserve the estimated tasks and bytes of a window just taken, or the one unmeasured slot.

        A warm-up window reserves at least all the free room: its estimate
        rests on too few windows of this partition, so no other window is
        admitted until its fan-out has landed, and the cap is passed by at most
        this one message. The warm-up lasts ``WARM_UP_WINDOWS`` windows, so a
        partition whose windows create no tasks reserves the whole room only
        for those first windows.
        """
        if self._account is None:
            return
        shape = self._estimate()
        if shape is None:
            self._account.budget.open_unmeasured_window()
            self._holds_unmeasured_window = True
            return
        tasks_per_message, bytes_per_message = self._per_message_estimate(shape)
        tasks = message_count * tasks_per_message
        held_bytes = message_count * bytes_per_message
        if self._in_warm_up():
            budget = self._account.budget
            tasks = max(tasks, budget.free_tasks)
            held_bytes = max(held_bytes, budget.free_bytes)
        self._account.reserve(tasks, held_bytes)

    def _end_window_reservation(self) -> None:
        """Give back what the window reserved and did not charge, and its unmeasured slot."""
        if self._account is None:
            return
        self._account.end_reservation()
        if self._holds_unmeasured_window:
            self._holds_unmeasured_window = False
            self._account.budget.close_unmeasured_window()
        else:
            self._account.budget.wake_next()

    def _hold_task(self, task: ExecutorTask, held_bytes: int) -> None:
        """Count one more pending run of ``task`` and the stdin bytes charged for it."""
        hold = self._task_holds.get(id(task))
        if hold is None:
            self._task_holds[id(task)] = [1, held_bytes]
        else:
            hold[0] += 1
            hold[1] += held_bytes

    def _settle_task(self, task: ExecutorTask, output_bytes: int = 0) -> None:
        """One run of ``task`` settled; at its last pending run, free its stdin and give back the charge.

        ``output_bytes`` is the output charge of this run's outcome, given back
        in the same release so a settled task wakes a waiting loop once.
        """
        hold = self._task_holds.get(id(task))
        if hold is None:
            # A run that was never held (a task scheduled outside the fan-out
            # paths, as some tests do): free the stdin, nothing to give back.
            task.stdin = None
            if self._account is not None:
                self._account.release(1, output_bytes)
            return
        hold[0] -= 1
        if hold[0] > 0:
            if self._account is not None:
                self._account.release(1, output_bytes)
            return
        del self._task_holds[id(task)]
        task.stdin = None
        if self._account is not None:
            self._account.release(1, hold[1] + output_bytes)

    def _note_window_shape(self, message_count: int, task_count: int, stdin_bytes: int) -> None:
        """Fold one window's fan-out into this partition's averages and the worker's."""
        self._shape.note_window(message_count, task_count, stdin_bytes)
        if self._account is not None:
            self._account.budget.shape.note_window(message_count, task_count, stdin_bytes)

    def _keep_bytes(self, holder: Window | MessageTracker, outcome: Any) -> None:
        """Charge the output of a kept result or error to the budget, on the holder that keeps it."""
        if self._account is None:
            return
        held = output_bytes(outcome)
        holder.kept_bytes += held
        self._account.charge(0, held)

    def _charge_output(self, outcome: Any) -> int:
        """Charge the stdout and stderr of a task's outcome while its hooks and delivery hold it; return the bytes."""
        if self._account is None:
            return 0
        held = output_bytes(outcome)
        self._account.charge(0, held)
        return held

    def _move_output_charge(self, holder: Window | MessageTracker, held: int) -> None:
        """Hand a charged outcome to the holder that keeps it, so its hook releases the bytes, not the task."""
        holder.kept_bytes += held

    def _release_kept_bytes(self, holder: Window | MessageTracker) -> None:
        """Give back the output bytes a window or tracker kept, once its hook has run.

        The amount also feeds the averages of kept output per message, so the
        next windows are sized for the output the hooks will keep.
        """
        if self._account is None or not holder.kept_bytes:
            return
        worker_shape = self._account.budget.shape
        if isinstance(holder, Window):
            message_count = max(1, len(holder.source_messages))
            self._shape.note_window_kept(message_count, holder.kept_bytes)
            worker_shape.note_window_kept(message_count, holder.kept_bytes)
        else:
            self._shape.note_message_kept(holder.kept_bytes)
            worker_shape.note_message_kept(holder.kept_bytes)
        self._account.release(0, holder.kept_bytes)
        holder.kept_bytes = 0

    async def _process_window(self, messages: list[SourceMessage]) -> None:
        self._window_counter += 1
        # Counted before any early return: a window whose messages all fail to
        # parse, or whose arrange creates no tasks, still ends a warm-up step.
        self._windows_seen += 1

        # Register offsets and deserialize BEFORE building the window so
        # the parse-error policy can drop unparseable messages from the
        # window while their offsets are already watermark-tracked.
        # Policy ('skip' keeps the message in the window with payload=None):
        arrange_started_at = time.monotonic()
        accepted: list[SourceMessage] = []
        for msg in messages:
            self._offset_tracker.register(msg.offset)
            self._handler.deserialize_message(msg)
            # After the parse, so the handler hook can read msg.payload.
            self._request_ids.assign(msg)
            if msg.parse_error is not None and self._on_parse_error != 'skip':
                with self._request_ids.scope([msg.request_id]):
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
        request_id_tokens = self._request_ids.bind(msg.request_id for msg in messages)
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
            reset_request_id(request_id_tokens)
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
        self._stamp_request_ids(tasks)
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
            # A window with no tasks measures nothing: its messages leave the
            # averages. Counted as zeros they would drive the estimate to 0,
            # and a zero estimate reserves no room.
            await self._note_commit_due()
            return

        # Fan out in chunks, handing the loop back between them.
        #
        # Without a periodic yield here, asyncio runs every ready handle
        # before it polls I/O again — so creating N tasks back to back also
        # runs the first step of all N coroutines together: the context
        # binds, the metric lookups, the priority computation and the gate's
        # heap push. Measured at ~9 µs per task, that is a tenth of a second
        # for a 10 000-task window and close to a second for 100 000, during
        # which the worker does nothing else: no Kafka poll, no sink
        # delivery, no UI frame, no health sample. A window's task count is
        # bounded only by what the handler returns, so the stall scales with
        # the message shape.
        #
        # Registration rides in the same chunk: a dict insert is cheap, but
        # 100 000 of them in one turn is not.
        #
        # Yielding mid-fan-out means a task from an earlier chunk can complete
        # while later chunks are still being created. That is safe because
        # ``window.total_tasks`` and every tracker's ``remaining`` were set for
        # the whole list above, so neither the window nor a message group can
        # reach a completed state early.
        #
        # Each task is charged to the pending budget here, by the size of its
        # stdin at this moment, out of the window's reservation. The size is
        # kept per task object until the last outcome, so the release gives
        # back exactly what was charged even if on_error rewrites
        # ``task.stdin`` before a retry.
        window_stdin_bytes = 0
        for start in range(0, len(tasks), FANOUT_CHUNK_TASKS):
            chunk = tasks[start : start + FANOUT_CHUNK_TASKS]
            chunk_stdin_bytes = 0
            for task in chunk:
                held = text_bytes(task.stdin)
                chunk_stdin_bytes += held
                if task.task_id in self._pending_tasks:
                    logger.warning(
                        'duplicate_task_id_in_pending',
                        category='partition',
                        partition=self._partition_id,
                        task_id=task.task_id,
                    )
                self._pending_tasks[task.task_id] = task
                self._inflight_count += 1
                self._hold_task(task, held)
                t = asyncio.create_task(self._execute_and_track(task, window))
                self._active_tasks.add(t)
                t.add_done_callback(self._active_tasks.discard)
            if self._account is not None:
                self._account.charge_reserved(len(chunk), chunk_stdin_bytes)
            window_stdin_bytes += chunk_stdin_bytes
            if start + FANOUT_CHUNK_TASKS < len(tasks):
                await asyncio.sleep(0)
        self._note_window_shape(len(messages), len(tasks), window_stdin_bytes)

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

    def _message_context(self, source_offsets: Iterable[int]) -> MessageContext | None:
        """The source messages of a task result with the DLQ state of each, from their trackers.

        ``None`` when none of the offsets has a tracker: an empty context
        would take a ``dlq`` outcome for a written record, while no context
        makes it follow ``dlq.on_send_failure``.
        """
        trackers = [
            tracker
            for offset in dict.fromkeys(source_offsets)
            if (tracker := self._message_trackers.get(offset)) is not None
        ]
        if not trackers:
            return None
        return MessageContext(
            source_messages=[tracker.source_message for tracker in trackers],
            states=[tracker.dlq for tracker in trackers],
        )

    async def _send_execute_dlq(self, task: ExecutorTask, error: ExecutorError, *, attempts: int) -> None:
        """Write the ``execute`` record of each source message of ``task``.

        Runs inside the task's failure handling, before the task settles, so
        nothing may escape: a raise would skip the tracker settle and leave
        the offset pending for ever. A record that could not be written
        follows ``dlq.on_send_failure``: ``stall`` holds the offsets.
        """
        if self._execute_dlq is None:
            self._lose_execute_records(task)
            return
        try:
            await self._execute_dlq(
                self._message_context(task.source_offsets),
                task=task,
                error=error,
                attempts=attempts,
                partition_id=self._partition_id,
            )
        except Exception as exc:
            # SinkDeliveryFailedError is the counted ``stall`` answer. Any
            # other error leaves the write unknown: ``stall`` treats it as
            # lost and holds the offsets, ``drop`` counts the messages as
            # lost and lets the offsets commit.
            is_stall = isinstance(exc, SinkDeliveryFailedError) or self._on_dlq_failure == 'stall'
            logger.error(
                'execute_dlq_failed',
                category='partition',
                partition=self._partition_id,
                task_id=task.task_id,
                source_offsets=task.source_offsets,
                error=str(exc),
                error_type=type(exc).__name__,
                offset_action='stall' if is_stall else 'commit',
                exc_info=exc,
            )
            if is_stall:
                self._mark_offsets_delivery_failed(task.source_offsets)
            else:
                self._drop_execute_records(task, reason='the DLQ write raised')

    def _lose_execute_records(self, task: ExecutorTask) -> None:
        """Apply ``dlq.on_send_failure`` to a task sent to the DLQ with no writer wired."""
        if self._on_dlq_failure == 'stall':
            self._mark_offsets_delivery_failed(task.source_offsets)
            return
        self._drop_execute_records(task, reason='on_error answered dlq but no DLQ writer is wired')

    def _drop_execute_records(self, task: ExecutorTask, *, reason: str) -> None:
        """Count and log, under ``drop``, the source messages of ``task`` whose record is lost.

        A message already on the DLQ, or whose loss is already counted, is
        left out, and the rest are marked lost, so a later failure of the
        message counts nothing again (the manager's ``dlq_lost`` rule).
        Without a tracker the task counts as one message, as a missing
        context does in the manager.
        """
        message_context = self._message_context(task.source_offsets)
        if message_context is None:
            message_count = 1
        else:
            open_states = [state for state in message_context.states if not (state.dlq_written or state.dlq_lost)]
            for state in open_states:
                state.dlq_lost = True
            message_count = len(open_states)
        if message_count == 0:
            return
        dlq_dropped_payloads.labels(partition=str(self._partition_id)).inc(message_count)
        logger.critical(
            'dlq_failure_payloads_dropped',
            category='partition',
            partition=self._partition_id,
            task_id=task.task_id,
            message_count=message_count,
            reason=reason,
            action='ALERT: source messages lost (dlq.on_send_failure=drop)',
        )

    async def _handle_parse_failure(self, msg: SourceMessage) -> None:
        """Apply the 'dlq' / 'raise' parse-error policy to one message.

        ('skip' never reaches this method — the message stays in the
        window with payload=None and the handler decides what to do.)

        'raise' propagates a MessageParseError, stopping the partition
        processor with the offset uncommitted (fail-fast, redelivery on
        restart). 'dlq' writes the message itself to the DLQ topic as a
        ``parse`` record and completes the offset when the write is confirmed; on a failed
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

        record = DLQRecord(
            key=msg.key,
            value=msg.value,
            source_headers=msg.headers,
            stage='parse',
            error=msg.parse_error or 'unknown parse error',
            source_topic=msg.topic,
            source_partition=msg.partition,
            source_offset=msg.offset,
            request_id=msg.request_id,
            source_timestamp_ms=msg.timestamp,
        )
        sent = False
        if self._dlq_send is not None:
            sent = await self._dlq_send(record, self._partition_id)
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
                reason='on_parse_error=dlq but the message was not written to the DLQ',
                action='ALERT: unparseable message dropped (dlq.on_send_failure=drop)',
            )
            self._offset_tracker.complete(msg.offset)
            await self._note_commit_due()

    async def _execute_and_track(self, task: ExecutorTask, window: Window, retry_count: int = 0) -> None:
        """Run one task to its last outcome: hooks, delivery, trackers, window.

        The last outcome releases what the fan-out charged to the pending
        budget for this task's stdin, and sets ``task.stdin`` to None so the
        string is freed even while a message or window hook keeps the task.

        The stdout and stderr of the outcome are charged as soon as it exists
        and released when ``on_task_complete`` or ``on_error`` and the sink
        delivery end, on every path (a retry, a replacement and a cancellation
        included). A result kept for ``on_window_complete`` takes the charge
        with it to the window.
        """
        # Bind partition context for this async task — inherited by all user hooks called within
        bind_contextvars(partition=self._partition_id, window_id=window.window_id)
        # This coroutine's own context: no reset needed when it ends.
        self._request_ids.bind(self._request_ids_of(task.source_offsets))
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
        # Output bytes of this run's outcome still charged to the task; 0 once
        # released or moved to the window that keeps the outcome.
        charged_output = 0

        try:
            result = await self._executor_pool.execute(task, self._recorder, self._partition_id)
            charged_output = self._charge_output(result)
            executor_tasks.labels(status='completed').inc()
            executor_duration.observe(result.duration_seconds)
            if result.spawn_seconds is not None:
                executor_spawn_duration.observe(result.spawn_seconds)
            observe_task_labels(task.labels)
            if self._recorder:
                self._recorder.record_task_completed(
                    result,
                    self._partition_id,
                    pool_active=self._executor_pool.active_count,
                    pool_waiting=self._executor_pool.waiting_count,
                    reported_run_seconds=reported_run_seconds(self._task_run_seconds, result),
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
                    await self._on_collect(
                        collect_result,
                        self._partition_id,
                        message_context=self._message_context(task.source_offsets),
                    )
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
                self._move_output_charge(window, charged_output)
                charged_output = 0
            task_result = result

        except ExecutorTaskError as e:
            charged_output += self._charge_output(e.result)
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
                    result=e.result,
                    reported_run_seconds=reported_run_seconds(self._task_run_seconds, e.result),
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
                # Take executor.on_failure, as a raising on_delivery_error
                # takes the sink's declared policy: the task settles as a
                # terminal failure (with its record under dlq), the tracker
                # completes, the watermark keeps advancing. The error line
                # comes first, then the policy.
                action: Any = self._executor_on_failure
                handler_hook_errors.labels(hook='on_error').inc()
                await log.aerror(
                    'on_error_hook_failed',
                    error=str(call.exception),
                    error_type=type(call.exception).__name__,
                    action=self._executor_on_failure.value,
                    exc_info=call.exception,
                )
            else:
                action = call.action
            decision = taskflow.decide_after_on_error(
                action,
                retry_count=retry_count,
                max_retries=self._max_retries,
                on_failure=self._executor_on_failure,
            )
            if decision.outcome is taskflow.TaskOutcome.REPLACE:
                # Replacement: the original task is "replaced" (not a
                # terminal failure of the group). Decrement its contribution
                # to every message tracker; add the replacements.
                for new_task in taskflow.link_replacements(task, list(decision.replacements)):
                    new_held = text_bytes(new_task.stdin)
                    if self._account is not None:
                        self._account.charge(1, new_held)
                    self._hold_task(new_task, new_held)
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
                        action=self._executor_on_failure.value,
                    )
                if decision.sends_to_dlq:
                    # Written now, before the task settles: from here on no
                    # delivery of its messages starts, and the message
                    # result of on_message_complete is not delivered.
                    await self._send_execute_dlq(task, e.error, attempts=retry_count + 1)
                if self._keep_window_results:
                    window.results.append(e.result)
                    self._move_output_charge(window, charged_output)
                    charged_output = 0
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
                self._keep_bytes(window, synthesized_result)

        finally:
            # Released on every path; a retry charges its own outcome when it ends.
            if handed_off_to_retry and charged_output and self._account is not None:
                self._account.release(0, charged_output)
            if not handed_off_to_retry:
                removed = self._pending_tasks.pop(task.task_id, None)
                self._inflight_count -= 1
                executor_pool_active.set(self._executor_pool.active_count)
                # This run's last outcome: no retry runs it again, and on_error
                # already built any replacement from it. Message and window
                # hooks see the task without its stdin once no other run of
                # the same object is pending.
                self._settle_task(task, charged_output)
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
                    self._keep_bytes(tracker, task_result)
            elif task_error is not None:
                tracker.failed_count += 1
                if self._keep_message_details:
                    tracker.errors.append(task_error)
                    self._keep_bytes(tracker, task_error)
            # replaced_by path: neither results nor errors is appended.
            # The replacements will eventually report their own outcomes
            # through this same loop and settle the tracker.
            if tracker.remaining == 0 and not tracker.completion_fired:
                await self._finalize_message_tracker(tracker)

        window.completed_count += 1

        if window.is_complete:
            with self._request_ids.scope(msg.request_id for msg in window.source_messages):
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
                        # The explicit window context: this result has no source
                        # message left, so a ``dlq`` outcome acts as ``skip``.
                        await self._on_collect(
                            on_complete_result,
                            self._partition_id,
                            message_context=MessageContext.window(),
                        )
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
                self._release_kept_bytes(window)

                # Per-message offsets were already marked complete individually
                # by _finalize_message_tracker as each message's tasks finished.
                # Just make sure any outstanding commit attempt goes through in
                # case the last commit was blocked.
                offset_lag.labels(partition=str(self._partition_id)).set(self._offset_tracker.pending_count)
                await self._note_commit_due()

    async def _finalize_message_tracker(self, tracker: MessageTracker) -> None:
        """Finish a message (see :meth:`_finish_message`) with its request id in the log context."""
        with self._request_ids.scope([tracker.source_message.request_id]):
            await self._finish_message(tracker)

    def _request_ids_of(self, offsets: list[int]) -> list[str | None]:
        """The request ids of the tracked messages at ``offsets``."""
        trackers = self._message_trackers
        return [trackers[offset].source_message.request_id if offset in trackers else None for offset in offsets]

    def _stamp_request_ids(self, tasks: list[ExecutorTask]) -> None:
        """Give each task the request ids of its source messages; a value the handler set is kept."""
        for task in tasks:
            RequestIdResolver.stamp_task(task, self._request_ids_of(task.source_offsets))

    async def _finish_message(self, tracker: MessageTracker) -> None:
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
                await self._on_collect(
                    on_complete_result,
                    self._partition_id,
                    message_context=MessageContext(
                        source_messages=[tracker.source_message],
                        states=[tracker.dlq],
                    ),
                )
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

        # The hook has run, so the kept outcomes have no reader left. A stalled
        # tracker stays registered below; dropping its outcomes too keeps a
        # stall from holding pending bytes for the life of the process.
        self._release_kept_bytes(tracker)
        tracker.results.clear()
        tracker.errors.clear()

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
