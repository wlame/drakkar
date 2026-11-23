"""The Kafka consumer as an input source: poll loop, partition processors, backpressure, commits and the rebalance-safe drain."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import math
import time
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.consumer import KafkaConsumer
from drakkar.handler import uses_arrange_stream
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    consumer_idle,
    consumer_lag,
    drain_timeout_hit,
    executor_idle_waste,
    messages_unassigned_dropped,
    total_queued,
)
from drakkar.partition import PartitionProcessor
from drakkar.providers.resilience import RateLimitedLog, describe_error
from drakkar.split.partmode import PartMode, no_report, part_collect
from drakkar.timefmt import format_rfc3339_micro
from drakkar.utils import wait_for_aligned_startup

if TYPE_CHECKING:
    from drakkar.consume_pause import ConsumePauseController
    from drakkar.models import SourceMessage
    from drakkar.sources.base import SourceContext
    from drakkar.split.runtime import SplitRuntime

logger = structlog.get_logger()

# Seconds to sleep when Kafka poll returns no messages. Defined once here,
# next to the poll loop, so the source module is self-contained.
POLL_IDLE_SLEEP = 0.05

# Floor for the shares of a teardown deadline handed to a commit RPC or to
# ``PartitionProcessor.stop``. A deadline that has already passed still has to
# let a step that is only waiting on an already-settled future finish, so it
# never gets zero.
MIN_TEARDOWN_STEP_SECONDS = 0.5

# How often the broker-lag gauge is refreshed from the cached watermarks.
LAG_REFRESH_INTERVAL_SECONDS = 10.0

# How long a revoke waits at most for the job tracker to drop the revoked partitions, and for
# cancelled restores to end. The drop only unassigns a partition of a consumer
# that reads no group; the bound keeps a client that hangs from holding the
# rebalance callback.
SPLIT_RELEASE_TIMEOUT_SECONDS = 2.0

# The waits between two tries to resume a restored partition whose resume failed.
RESTORE_RESUME_FIRST_WAIT_SECONDS = 1.0
RESTORE_RESUME_MAX_WAIT_SECONDS = 30.0

# Seconds a group leave may take beyond ``executor.drain_timeout``: the revoke
# drain has that bound, and the poll that runs the revoke needs a little more.
LEAVE_MARGIN_SECONDS = 10.0


class LeaveOutcome(StrEnum):
    """How a leave of the consumer group ended."""

    LEFT = 'left'
    TIMED_OUT = 'timed_out'
    STOPPING = 'stopping'


def _split_step_bound(deadline: float) -> float:
    """The time limit of one split step of a revoke: what is left of ``deadline``, within its floor and its cap."""
    return min(_remaining(deadline), SPLIT_RELEASE_TIMEOUT_SECONDS)


def _remaining(deadline: float) -> float:
    """Return what is left of ``deadline``, never below the step floor."""
    return max(deadline - time.monotonic(), MIN_TEARDOWN_STEP_SECONDS)


class SourceGauges:
    """The worker-level metrics a Kafka source writes about its topic; none of them has a partition label."""

    def assigned(self, count: int) -> None:
        """The number of partitions assigned to this worker."""
        assigned_partitions.set(count)

    def backpressure(self, active: bool) -> None:
        """Whether the fetch is paused by backpressure."""
        backpressure_active.set(1 if active else 0)

    def queued(self, total: int) -> None:
        """Messages in the partition queues plus tasks in flight."""
        total_queued.set(total)

    def idle(self, seconds: float) -> None:
        """Time with nothing to consume and nothing queued."""
        consumer_idle.inc(seconds)

    def idle_waste(self, slot_seconds: float) -> None:
        """Executor slots that were free while messages waited."""
        executor_idle_waste.inc(slot_seconds)


class GroupSource(Protocol):
    """What the detach controller needs of the consumer group membership it leaves and joins."""

    @property
    def is_ready(self) -> bool: ...

    @property
    def is_group_member(self) -> bool: ...

    def _poll_waits_end(self) -> bool: ...

    async def leave_group(self) -> LeaveOutcome: ...

    async def join_group(self) -> bool: ...


class KafkaSource:
    """The Kafka consumer as an input source.

    Owns everything that only the Kafka path needs: the subscribed
    consumer, one :class:`PartitionProcessor` per assigned partition, the
    backpressure pause/resume cycle, the rebalance callbacks, and the
    drain that has to commit before a revoke may return. Collaborators
    outside that set arrive through :class:`SourceContext`.

    Every config read goes through that context too, never a copy captured
    in ``__init__``: construction happens before ``on_startup`` runs, so a
    handler that returns a replaced config must still be the one in effect.
    """

    name = 'kafka'
    # Added to a partition number wherever this source shows it: a metric
    # label, a log field, a recorder row, its snapshot. Calls to the Kafka
    # client and the keys of ``processors`` keep the real number. A source of
    # another topic sets a base, so its partitions are told apart.
    partition_label_base = 0
    _gauges = SourceGauges()

    def __init__(self, consume_pause: ConsumePauseController) -> None:
        # Operator-driven debug pause. It shares ``consumer.pause``/``resume``
        # with backpressure, so the poll loop has to consult it before it
        # resumes anything (see drakkar.consume_pause).
        self._consume_pause = consume_pause
        self.processors: dict[int, PartitionProcessor] = {}
        self.paused = False
        # Partitions paused because an offset stalled under
        # dlq.on_send_failure=stall. Excluded from backpressure resume;
        # cleared on revoke so a reassignment starts fresh.
        self.stalled_partitions: set[int] = set()
        # Partitions paused because their processing loop died. Nothing reads
        # their queue any more, so fetching more for them only grows it; they
        # stay out of every resume. Cleared on revoke.
        self.dead_partitions_paused: set[int] = set()
        # Partitions paused while the job tracker restores their split jobs:
        # their processor exists and has not started. No resume touches them,
        # whoever asks for it; the restore resumes each one when it ends.
        # Always empty on a worker that does not split.
        self.restoring_partitions: set[int] = set()
        # The restore of each such partition; a revoke or a stop cancels it.
        self._restore_tasks: dict[int, asyncio.Task[None]] = {}
        self._restore_failure_log = RateLimitedLog(time.monotonic)
        self.consumer: KafkaConsumer | None = None
        self.background_tasks: set[asyncio.Task] = set()
        self._ready = False
        self._stop_requested = False
        # Set by ``drain``; ``stop`` commits final offsets only when it is True.
        self._drained_cleanly = False
        self._ctx: SourceContext | None = None
        self._lag_refresh_task: asyncio.Task | None = None
        # One failure line per 60 s, however many refreshes or queries fail.
        self._lag_failure_log = RateLimitedLog(time.monotonic)
        # Revoke drains running now. The lag refresh sends nothing while one
        # runs (or after a stop request): its drain and final commit share one
        # deadline and the consumer pool with every broker read.
        self._revokes_in_progress = 0
        # Group membership. False from the call to ``unsubscribe()`` until a
        # join has seen one poll after its ``subscribe()``.
        self._group_member = True
        # Polls of the poll loop: started and completed, and the event set at
        # the end of each poll (and on a stop) and then replaced. A leave or a
        # join waits on it for a poll that started after its call.
        self._polls_started = 0
        self._polls_completed = 0
        self._poll_progress = asyncio.Event()
        # Set when ``run()`` has ended, by a stop or by an error: no poll
        # comes any more, so a leave or a join must not wait for one.
        self._poll_loop_ended = False

    def bind(self, ctx: SourceContext) -> None:
        """Receive the worker context. Called once, before ``start``."""
        self._ctx = ctx

    def _context(self) -> SourceContext:
        """The bound context. A method reaching here before ``bind`` is a wiring bug."""
        assert self._ctx is not None
        return self._ctx

    def _shown(self, partition_id: int) -> int:
        """The number this source shows for one of its partitions."""
        base = self.partition_label_base
        return partition_id + base if base else partition_id

    async def start(self) -> None:
        """Align the fleet, construct the consumer, and subscribe."""
        ctx = self._context()
        config = ctx.config
        log = logger.bind(worker_id=ctx.worker_id)

        # Stagger startup: sleep until the next wall-clock alignment
        # boundary so a fleet of workers in a rolling deploy converges
        # on a single Kafka consumer-group rebalance instead of N. See
        # StartupAlignConfig for tuning and rationale.
        align = config.sources.kafka.startup_align
        if align.enabled:
            min_wait = align.min_wait
            interval = align.interval
            target_wall = math.ceil((time.time() + min_wait) / interval) * interval
            await log.ainfo(
                'startup_align_waiting',
                category='lifecycle',
                min_wait_seconds=min_wait,
                align_interval_seconds=interval,
                target_wall_unix=target_wall,
                target_wall_iso=format_rfc3339_micro(datetime.fromtimestamp(target_wall, tz=UTC)),
            )
            slept = await wait_for_aligned_startup(min_wait, interval)
            await log.ainfo('startup_align_done', category='lifecycle', slept_seconds=round(slept, 3))

        if uses_arrange_stream(ctx.handler):
            await log.ainfo(
                'arrange_stream_enabled',
                category='lifecycle',
                hint='executor.window_size has no effect: a stream handler takes one message at a time',
            )

        await self._subscribe()

    def _consumer_settings(self) -> dict[str, Any]:
        """The connection and the topic, group and poll settings of this source's consumer."""
        config = self._context().config
        return {
            'connection': config.declared_connection(
                'sources.kafka.connection', 'kafka', config.sources.kafka.connection
            ),
            'source': config.sources.kafka,
        }

    async def _subscribe(self) -> None:
        """Construct the consumer, subscribe it, and start the refresh of the lag gauge."""
        self.consumer = KafkaConsumer(
            **self._consumer_settings(),
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
        )
        await self.consumer.subscribe()
        self._lag_refresh_task = asyncio.create_task(self._refresh_lag_forever(), name=f'{self.name}:lag-refresh')

    async def _refresh_lag_forever(self) -> None:
        """Refresh the broker-lag gauge every ``LAG_REFRESH_INTERVAL_SECONDS`` until cancelled."""
        while True:
            await asyncio.sleep(LAG_REFRESH_INTERVAL_SECONDS)
            await self._refresh_lag_once()

    async def _refresh_lag_once(self) -> None:
        """Set ``drakkar_consumer_lag`` for the assigned partitions; a failure is logged and never raised."""
        consumer = self.consumer
        if consumer is None or not self._lag_refresh_allowed():
            return
        try:
            lag = await consumer.cached_broker_lag(
                sorted(self.processors),
                self._paused_partitions(),
                may_query=self._lag_refresh_allowed,
                on_query_failure=self._log_lag_query_failure,
            )
        except Exception as exc:
            self._log_lag_refresh_failure(exc)
            return
        for pid, value in lag.items():
            # A revoke during the read removed the label; do not bring it back.
            if pid in self.processors:
                consumer_lag.labels(partition=str(self._shown(pid))).set(value)

    def _lag_refresh_allowed(self) -> bool:
        """Whether the lag refresh may send a broker read now: no revoke drain and no stop in progress."""
        return not self._stop_requested and self._revokes_in_progress == 0

    def _log_lag_query_failure(self, partition_id: int, exc: Exception) -> None:
        """Log one failed watermark query of the refresh, under the refresh's one-line-per-60-s limit."""
        self._log_lag_refresh_failure(exc, partition=self._shown(partition_id))

    def _log_lag_refresh_failure(self, exc: Exception, **context: int) -> None:
        """Log a failure of the lag refresh at most once per 60 s; the gauge keeps its last values."""
        suppressed = self._lag_failure_log.admit('lag_refresh')
        if suppressed is not None:
            logger.warning(
                'consumer_lag_refresh_failed',
                category='kafka',
                error=str(exc),
                error_type=type(exc).__name__,
                suppressed=suppressed,
                **context,
            )

    def _paused_partitions(self) -> frozenset[int]:
        """Assigned partitions the consumer does not fetch from right now."""
        if self.paused or self._consume_pause.active:
            return frozenset(self.processors)
        held = self.stalled_partitions | self.dead_partitions_paused | self.restoring_partitions
        return frozenset(held) & frozenset(self.processors)

    async def _apply_backpressure(self, consumer: Any, queued: int, low_watermark: int, high_watermark: int) -> None:
        """Resume the fetch at the low watermark and pause it at the high one."""
        # An active operator debug pause (ui.consume_pause) outranks the
        # backpressure resume: queues draining below the low watermark
        # must not restart fetching while the operator asked for quiet.
        # The debug resume hands control back here — if backpressure is
        # still holding, this branch resumes once queues drain.
        if self.paused and queued <= low_watermark and not self._consume_pause.active:
            # Never resume partitions paused by a delivery stall or a dead
            # loop — they stay paused until restart/revoke regardless of
            # backpressure. A partition whose split jobs are being restored
            # is resumed by its restore alone.
            held = self.stalled_partitions | self.dead_partitions_paused | self.restoring_partitions
            partition_ids = [p for p in self.processors if p not in held]
            if partition_ids:
                await consumer.resume(partition_ids)
                self.paused = False
                self._gauges.backpressure(False)

        if not self.paused and queued >= high_watermark:
            partition_ids = list(self.processors.keys())
            if partition_ids:
                await consumer.pause(partition_ids)
                self.paused = True
                self._gauges.backpressure(True)

    def _route_messages(self, messages: list[SourceMessage]) -> None:
        """Hand each polled message to the loop of its partition; drop and count one with no loop."""
        for msg in messages:
            processor = self.processors.get(msg.partition)
            if processor:
                processor.enqueue(msg)
            else:
                # Revoke raced the poll: the processor was popped but
                # the broker delivered a few more messages before
                # acknowledging the revoke. The new partition owner
                # redelivers from the last committed offset, so
                # dropping here is safe — but it must be visible.
                messages_unassigned_dropped.labels(partition=str(self._shown(msg.partition))).inc()
                logger.warning(
                    'message_for_unassigned_partition_dropped',
                    category='kafka',
                    partition=self._shown(msg.partition),
                    offset=msg.offset,
                )

    async def run(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume.

        Fetching pauses when the messages waiting in the queues of live
        partitions reach ``sources.kafka.max_queued_messages`` and resumes at
        half of it. The queues fill because partition loops take new windows
        only while the pending budget has room, so a full budget pauses the
        fetch by itself. ``poll_batch`` runs on every turn, paused or not:
        a consumer that stops polling for ``max_poll_interval`` is evicted.
        """
        ctx = self._context()
        assert self.consumer is not None
        consumer = self.consumer
        max_executors = ctx.config.executor.max_executors
        high_watermark = ctx.config.sources.kafka.max_queued_messages
        low_watermark = high_watermark // 2
        try:
            await self._poll_until_stopped(consumer, max_executors, low_watermark, high_watermark)
        finally:
            self._poll_loop_ended = True
            self._wake_poll_waiters()

    async def _poll_until_stopped(
        self,
        consumer: KafkaConsumer,
        max_executors: int,
        low_watermark: int,
        high_watermark: int,
    ) -> None:
        """The poll loop of ``run``: one poll per turn until a stop is requested."""
        ctx = self._context()
        last_tick = time.monotonic()

        while not self._stop_requested:
            now = time.monotonic()
            dt = now - last_tick
            last_tick = now

            total = self.total_queued()
            self._gauges.queued(total)
            await self._pause_dead_partitions(consumer)
            queued = self.queued_messages()

            # Executor idle waste: slots sitting free while messages wait in queues.
            # Uses queue_size only (not inflight) — inflight tasks ARE using slots.
            waiting = self.total_waiting()
            if waiting > 0:
                idle_slots = max_executors - ctx.executor_pool.active_count
                if idle_slots > 0:
                    self._gauges.idle_waste(idle_slots * dt)

            await self._apply_backpressure(consumer, queued, low_watermark, high_watermark)

            self._polls_started += 1
            try:
                messages = await consumer.poll_batch()
            finally:
                self._polls_completed += 1
                self._wake_poll_waiters()
            if self._stop_requested:
                # A stop arrived while the poll was in flight. The partition
                # loops are leaving, and a loop that has already ended never
                # reads its queue again. The messages were never registered,
                # so the final commit stays below them and they are read
                # again by the next owner of the partition.
                if messages:
                    logger.info(
                        'messages_polled_at_stop_dropped',
                        category='kafka',
                        count=len(messages),
                        partitions=sorted({self._shown(msg.partition) for msg in messages}),
                    )
                break
            self._route_messages(messages)

            # After the first poll completes successfully we consider this
            # source ready to serve traffic — the consumer is subscribed,
            # sinks were connected before the loop started, and at least
            # one poll round-trip has finished. Kubernetes readiness probes
            # can now flip us into the service endpoints. Idempotent: the
            # assignment on subsequent iterations is a no-op.
            self._ready = True

            if not messages:
                # Consumer idle: no messages from Kafka, nothing queued, not paused.
                # Measures time with genuinely nothing to do (consumer lag is zero).
                # Time out of the group is not idle: the worker chose to take no work.
                if total == 0 and not self.paused and self._group_member:
                    self._gauges.idle(dt)
                await asyncio.sleep(POLL_IDLE_SLEEP)

    @property
    def is_ready(self) -> bool:
        """Whether the first poll round-trip has completed."""
        return self._ready

    def signal_stop(self) -> None:
        """Leave the poll loop and stop every processor from accepting new work."""
        self._stop_requested = True
        # First the restores: a partition whose restore is cancelled never starts its processor.
        for restore in self._restore_tasks.values():
            restore.cancel()
        for pid, processor in list(self.processors.items()):
            processor.signal_stop()
            if pid in self.restoring_partitions:
                # Not started: no loop will read what was fetched for it; the drain must not wait for that.
                processor.drop_queue()
        # The poll loop runs no further poll, so a leave or a join must not
        # wait for one.
        self._wake_poll_waiters()

    @property
    def is_group_member(self) -> bool:
        """Whether the consumer is known to be a member of its group.

        False from the call to ``unsubscribe()`` until a join completes. False
        means "not known to be a member": after a cancelled leave or join the
        consumer may be subscribed or not, and the caller must join again.
        """
        return self._group_member

    def _poll_waits_end(self) -> bool:
        """Whether no further poll can come: a stop was requested or the poll loop has ended."""
        return self._stop_requested or self._poll_loop_ended

    def _wake_poll_waiters(self) -> None:
        """Wake every coroutine waiting for a poll, and arm a fresh event for the next one."""
        self._poll_progress.set()
        self._poll_progress = asyncio.Event()

    async def _wait_for_poll(self, after: int, until: Callable[[], bool]) -> bool:
        """Wait for a completed poll numbered above ``after`` after which ``until()`` holds.

        Polls run one at a time, so poll ``after + 1`` started after the
        caller read ``after``. Returns False when a stop was requested or the
        poll loop ended first.
        """
        while not self._poll_waits_end():
            if self._polls_completed > after and until():
                return True
            await self._poll_progress.wait()
        return False

    def _nothing_left_to_revoke(self) -> bool:
        """Whether no partition and no revoke drain is left on this worker."""
        return not self.processors and self._revokes_in_progress == 0

    async def leave_group(self) -> LeaveOutcome:
        """Leave the consumer group and wait until no partition is left on this worker.

        ``unsubscribe()`` changes nothing by itself: the client runs the
        revoke inside its next poll, which waits for ``on_revoke`` and so for
        the drain. So the wait ends on a poll that started after the call,
        with no processor and no revoke left. A member that holds no
        partition gets no revoke at all; that poll alone ends its leave. The
        wait is bounded by ``executor.drain_timeout`` plus
        ``LEAVE_MARGIN_SECONDS``; on ``TIMED_OUT`` the source stays out of
        the group until a join. A stop request, or the end of the poll loop,
        ends the wait at once with ``STOPPING``.

        ``unsubscribe()`` returns at once also while a poll is in flight; the
        client may then run the revoke inside that poll, and the leave ends on
        the next one.
        """
        if self._poll_waits_end():
            return LeaveOutcome.STOPPING
        assert self.consumer is not None
        bound = self._context().config.executor.drain_timeout + LEAVE_MARGIN_SECONDS
        self._group_member = False
        try:
            await self.consumer.unsubscribe()
        except Exception:
            # The client refused the call, so the consumer is still a member.
            # A cancellation is not caught: the client call runs on a worker
            # thread and may still unsubscribe, so membership stays unknown.
            self._group_member = True
            raise
        after = self._polls_started
        try:
            async with asyncio.timeout(bound):
                left = await self._wait_for_poll(after, self._nothing_left_to_revoke)
        except TimeoutError:
            return LeaveOutcome.TIMED_OUT
        return LeaveOutcome.LEFT if left else LeaveOutcome.STOPPING

    async def join_group(self) -> bool:
        """Join the consumer group again and wait for one poll that started after ``subscribe()``.

        The assignment comes in a later poll and can be empty, so the join
        does not wait for one. Returns False when a stop was requested, or
        the poll loop ended, before the call or during the wait. A join
        cancelled after ``subscribe()`` leaves ``is_group_member`` False while
        the consumer is subscribed; calling the join again is harmless.
        """
        if self._poll_waits_end():
            return False
        assert self.consumer is not None
        await self.consumer.subscribe()
        after = self._polls_started
        joined = await self._wait_for_poll(after, lambda: True)
        if joined:
            self._group_member = True
        return joined

    async def drain(self, deadline: float) -> bool:
        """Wait for queued and in-flight work, bounded by ``deadline``.

        Returns True when everything finished; ``stop`` reads the same
        answer off ``self._drained_cleanly`` to decide whether the final
        commit is safe.
        """
        ctx = self._context()
        log = logger.bind(worker_id=ctx.worker_id)

        # Snapshot the processors BEFORE draining: a rebalance firing
        # concurrently with shutdown pops processors from ``processors``
        # (handing them to ``_stop_processor`` background tasks). Draining
        # from the live dict would skip those, and ``drained_cleanly=True``
        # could fire with their work still in flight.
        processors_snapshot = list(self.processors.values())

        budget = _remaining(deadline)
        await log.ainfo('draining_executors', category='lifecycle', timeout=budget)
        self._drained_cleanly = False
        try:
            await asyncio.wait_for(self._drain_all_processors(processors_snapshot), timeout=budget)
            self._drained_cleanly = True
            await log.ainfo('executors_drained', category='lifecycle')
        except TimeoutError:
            # Surface the drain-timeout event as a Prometheus counter so
            # operators can alert on ``rate(...[5m]) > 0`` instead of
            # parsing logs for the ``drain_timeout`` warning.
            drain_timeout_hit.inc()
            # In-flight tasks are zombies now: after this worker exits,
            # another consumer-group member replays their messages from
            # the last committed offset. Suppress their late sink
            # deliveries and commits to avoid double-writes during the
            # remaining teardown window.
            zombies = list(self.processors.values())
            for processor in zombies:
                processor.suppress_deliveries()
            # Cancel them as well: an uncancelled zombie keeps its
            # executor slot and subprocess for up to
            # ``executor.task_timeout`` and keeps its processing loop from
            # exiting, so every ``processor.stop()`` below would wait out
            # its full timeout inside the pod's grace period.
            await asyncio.gather(*(p.cancel_active_tasks() for p in zombies))
            await log.awarning(
                'drain_timeout',
                category='lifecycle',
                msg=f'some executors did not finish in {round(budget, 3)}s; skipping final commit',
            )
        except Exception as exc:
            # Any non-TimeoutError raised during drain is logged as a
            # distinct event so it does not get conflated with the
            # benign "some tasks took too long" timeout case. We do
            # NOT increment ``drain_timeout_hit`` here — this is a
            # different failure mode (drain bug, processor invariant
            # violation, OS error mid-drain) and should be alerted
            # on its own metric in the future. ``_drained_cleanly``
            # stays False so the post-drain final-commit phase is
            # skipped (preferring at-least-once duplication over
            # silent loss, same rationale as the timeout branch).
            await log.aerror(
                'drain_exception',
                category='lifecycle',
                error=str(exc),
                exc_type=type(exc).__name__,
                exc_info=True,
            )
        return self._drained_cleanly

    async def stop(self, deadline: float) -> None:
        """Commit what drained cleanly, stop the processors, close the consumer.

        ``deadline`` is advisory here: every step below already carries its own
        ``executor.drain_timeout`` bound, and the background-task wait
        needs that full budget rather than whatever the drain left of a shared
        deadline.
        """
        ctx = self._context()
        log = logger.bind(worker_id=ctx.worker_id)
        # Its own time limit, as every step of a stop has: the cap of one split step.
        await self._cancel_restores(list(self._restore_tasks), time.monotonic() + SPLIT_RELEASE_TIMEOUT_SECONDS)

        # Only commit final offsets if drain succeeded cleanly. After
        # a timeout / drain-exception we cannot be sure tasks have
        # stopped running, so committing here would silently skip
        # in-flight work on restart — preferring at-least-once
        # duplication over silent loss.
        if self._drained_cleanly:
            for processor in list(self.processors.values()):
                committable = processor.offset_tracker.committable()
                if committable is not None and self.consumer:
                    try:
                        await self.consumer.commit({processor.partition_id: committable})
                        processor.offset_tracker.acknowledge_commit(committable)
                    except Exception as e:
                        await log.awarning(
                            'final_commit_failed',
                            category='kafka',
                            partition=self._shown(processor.partition_id),
                            error=str(e),
                            exc_info=True,
                        )

        # Stop every partition processor regardless of drain outcome.
        # ``processor.stop()`` is idempotent on already-drained
        # processors, and skipping it on a drain failure would leak
        # the processor's worker tasks.
        # Concurrently, as the revoke path does. Sequentially, a worker
        # with N partitions pays N stop timeouts back to back and the
        # pod's grace period expires mid-teardown — after the watchdog
        # was already marked clean, so the SIGKILL is recorded as a
        # clean exit and the sink/DLQ/recorder/consumer closes below
        # never run.
        async def _stop_one(processor: PartitionProcessor) -> None:
            try:
                await processor.stop()
            except Exception as exc:
                await log.awarning(
                    'processor_stop_failed',
                    category='lifecycle',
                    partition=self._shown(processor.partition_id),
                    error=str(exc),
                    exc_info=True,
                )

        stopping = list(self.processors.values())
        if stopping:
            await asyncio.gather(*(_stop_one(p) for p in stopping))
        self.processors.clear()

        # Wait for background tasks scheduled by rebalance callbacks
        # (_stop_processor from revoke, on_assign/revoke handler hooks,
        # backpressure pauses) to complete BEFORE we close the
        # consumer. These tasks hold references to the consumer;
        # closing it while they run would cause use-after-close
        # errors and skip their final commits.
        #
        # This step gets its own full budget instead of a share of
        # ``deadline``. A drain that timed out has already spent that
        # deadline, so the shared budget would collapse to the step floor
        # and close the consumer out from under a revoke's
        # ``_stop_processor`` — the use-after-close hazard above. Nothing
        # holds a rebalance callback open here, so the extra wait is
        # affordable.
        if self.background_tasks:
            bg_snapshot = list(self.background_tasks)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*bg_snapshot, return_exceptions=True),
                    timeout=ctx.config.executor.drain_timeout,
                )
            except TimeoutError:
                await log.awarning(
                    'background_task_drain_timeout',
                    category='lifecycle',
                    count=len(bg_snapshot),
                )

        if self._lag_refresh_task is not None:
            self._lag_refresh_task.cancel()
            await asyncio.gather(self._lag_refresh_task, return_exceptions=True)
            self._lag_refresh_task = None

        if self.consumer:
            try:
                await self.consumer.close()
            except Exception as exc:
                await log.awarning(
                    'consumer_close_failed',
                    category='lifecycle',
                    error=str(exc),
                    exc_info=True,
                )

    def snapshot(self) -> dict[str, Any]:
        """Source state for worker_state rows and the UI."""
        return {
            'assigned_partitions': sorted(self._shown(pid) for pid in self.processors),
            'partition_count': len(self.processors),
            'paused': self.paused,
            'total_queued': self.total_queued(),
        }

    def on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        ctx = self._context()
        if ctx.recorder:
            ctx.recorder.record_assigned([self._shown(pid) for pid in partition_ids])
        runtime = self._owner_runtime()
        newly_added: list[int] = []
        for pid in partition_ids:
            if pid not in self.processors:
                if runtime is None:
                    processor = PartitionProcessor(**self._processor_settings(pid))
                    self.processors[pid] = processor
                    processor.start()
                else:
                    # Created, not started: the processor starts when the
                    # split jobs of the partition are restored.
                    settings = {**self._processor_settings(pid), **self._owner_processor_settings(runtime)}
                    processor = PartitionProcessor(**settings)
                    self.processors[pid] = processor
                    self._start_restore(pid, processor, runtime)
                newly_added.append(pid)

        self._gauges.assigned(len(self.processors))

        # If backpressure or an operator debug pause is active, the
        # previously-assigned partitions are already paused. Newly-assigned
        # partitions were not in that pause set, so Kafka would deliver
        # messages from them until the next poll tick (or the pause's end).
        # Pause them now so neither gate is bypassed between assignment and
        # the next ``run`` iteration.
        if (self.paused or self._consume_pause.active) and newly_added and self.consumer is not None:
            consumer = self.consumer

            async def _pause_newly_assigned() -> None:
                await consumer.pause(newly_added)

            pt = asyncio.ensure_future(self._safe_call(_pause_newly_assigned()))
            self.background_tasks.add(pt)
            pt.add_done_callback(self.background_tasks.discard)

        self._notify_handler_of_assign(partition_ids)

    def _processor_settings(self, partition_id: int) -> dict[str, Any]:
        """The arguments of the processor of one assigned partition."""
        ctx = self._context()
        config = ctx.config
        return {
            'partition_id': partition_id,
            'handler': ctx.handler,
            'executor_pool': ctx.executor_pool,
            'window_size': config.executor.window_size,
            'max_retries': config.executor.max_retries,
            'on_collect': ctx.on_collect,
            'on_commit': self._handle_commit,
            'recorder': ctx.recorder,
            'on_parse_error': config.sources.kafka.on_parse_error,
            'dlq_send': ctx.sink_manager.send_parse_dlq if ctx.dlq_sink else None,
            'on_dlq_failure': config.dlq.on_send_failure,
            'on_stall': self.pause_stalled_partition,
            'execute_dlq': ctx.sink_manager.send_execute_dlq,
            'executor_on_failure': config.executor.on_failure,
            'budget': ctx.pending_budget,
            'request_ids': ctx.request_ids,
        }

    def _owner_runtime(self) -> SplitRuntime | None:
        """The split feature of the worker when this source owns split jobs: the source of the source topic does."""
        return self._context().split

    def _owner_processor_settings(self, runtime: SplitRuntime) -> dict[str, Any]:
        """What the processor of a source partition needs on a worker that splits.

        A message whose split yields nothing runs on this worker in part
        mode: its DLQ outcomes are kept for its one result, and a result of
        its hooks that names an aggregate sink is refused. That result goes
        to the owner of the partition, not to a topic.
        """
        ctx = self._context()
        part_mode = PartMode(
            worker=runtime.worker,
            max_part_data=ctx.config.split.max_part_data,
            part_data_model=getattr(type(ctx.handler), 'part_data_model', None),
            report=no_report,
            source_group=runtime.source_group,
        )
        return {
            'on_collect': part_collect(ctx.sink_manager, ctx.on_collect),
            'part_mode': part_mode,
            'http_jobs': runtime.http is not None,
        }

    def _start_restore(self, partition_id: int, processor: PartitionProcessor, runtime: SplitRuntime) -> None:
        """Mark the partition as restoring and start the task that restores its split jobs."""
        self.restoring_partitions.add(partition_id)
        restore = asyncio.ensure_future(self._restore_partition(partition_id, processor, runtime))
        self._restore_tasks[partition_id] = restore
        restore.add_done_callback(functools.partial(self._forget_restore, partition_id))

    def _forget_restore(self, partition_id: int, restore: asyncio.Task[None]) -> None:
        if self._restore_tasks.get(partition_id) is restore:
            del self._restore_tasks[partition_id]

    async def _restore_partition(self, partition_id: int, processor: PartitionProcessor, runtime: SplitRuntime) -> None:
        """Pause the partition, restore its split jobs, then start its processor and resume it.

        The job tracker tries a failed restore again by itself, for as long
        as it takes; the partition stays paused meanwhile. A revoke of the
        partition or a stop cancels this task. From the end of the restore to
        the start of the processor there is no await, so a cancellation finds
        the processor either not started or started with its owner.
        """
        shown = self._shown(partition_id)
        try:
            await self._pause_for_restore(partition_id)
            committed = await self._committed_source_offset(partition_id)
            table = await runtime.follow(partition_id, committed_source_offset=committed)
            ctx = self._context()
            owner = runtime.owner_for(
                partition_id,
                table=table,
                host=processor,
                handler=ctx.handler,
                sink_manager=ctx.sink_manager,
                on_collect=ctx.on_collect,
                on_dlq_failure=ctx.config.dlq.on_send_failure,
                partition_label=shown,
            )
            processor.attach_split_owner(owner)
            self.restoring_partitions.discard(partition_id)
            processor.start()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # Nothing would ever start the processor: say so and take the worker out of rotation.
            self.restoring_partitions.discard(partition_id)
            processor.mark_dead(f'the restore of its split jobs failed: {describe_error(error)}')
            logger.critical(
                'split_restore_task_failed',
                category='split',
                partition=shown,
                error=describe_error(error),
                impact='the partition is not processed; the worker now fails /readyz',
                exc_info=error,
            )
            return
        await self._resume_after_restore(partition_id)

    async def _resume_after_restore(self, partition_id: int) -> None:
        """Resume the fetch of a restored partition; a resume that fails is tried again until it works.

        A partition left paused with a running processor would stop with no
        sign of it. The waits are 1, 2, 4 ... 30 seconds; a revoke or a stop
        cancels them. When another pause applies to the partition, whoever
        holds that pause resumes it.
        """
        wait = RESTORE_RESUME_FIRST_WAIT_SECONDS
        while True:
            held = self.stalled_partitions | self.dead_partitions_paused
            other_pause = self.paused or self._consume_pause.active or partition_id in held
            if self.consumer is None or other_pause or partition_id not in self.processors:
                return
            try:
                await self.consumer.resume([partition_id])
                return
            except Exception as error:
                suppressed = self._restore_failure_log.admit(f'resume:{partition_id}')
                if suppressed is not None:
                    logger.error(
                        'split_restore_resume_failed',
                        category='split',
                        partition=self._shown(partition_id),
                        error=describe_error(error),
                        retry_seconds=wait,
                        suppressed=suppressed,
                    )
            await asyncio.sleep(wait)
            wait = min(wait * 2, RESTORE_RESUME_MAX_WAIT_SECONDS)

    async def _pause_for_restore(self, partition_id: int) -> None:
        """Pause the fetch of a restoring partition; a failure is logged, and what is fetched waits in its queue."""
        if self.consumer is None:
            return
        try:
            await self.consumer.pause([partition_id])
        except Exception as error:
            logger.error(
                'split_restore_pause_failed',
                category='split',
                partition=self._shown(partition_id),
                error=describe_error(error),
                exc_info=error,
            )

    async def _committed_source_offset(self, partition_id: int) -> int | None:
        """The committed offset of a source partition, or None when the group has none or the read failed.

        The restore retires every job below it. With None nothing retires
        at the restore; a job that the commit has passed is then closed as
        orphaned when the next message of the partition arrives.
        """
        if self.consumer is None:
            return None
        try:
            return await self.consumer.committed_offset(partition_id)
        except Exception as error:
            logger.warning(
                'split_committed_offset_unknown',
                category='split',
                partition=self._shown(partition_id),
                error=describe_error(error),
            )
            return None

    async def _cancel_restores(self, partition_ids: list[int], deadline: float) -> None:
        """Cancel the restores of these partitions and wait a bounded time for them to end.

        A revoke calls this inside the rebalance callback, so the wait has a
        time limit, taken from the ``deadline`` of the whole revoke; a
        restore that does not end in it is logged and left to end by itself.
        """
        restores = [restore for pid in partition_ids if (restore := self._restore_tasks.pop(pid, None)) is not None]
        self.restoring_partitions.difference_update(partition_ids)
        for restore in restores:
            restore.cancel()
        if not restores:
            return
        bound = _split_step_bound(deadline)
        _, late = await asyncio.wait(restores, timeout=bound)
        if late:
            logger.warning(
                'split_restore_cancel_slow',
                category='split',
                partitions=[self._shown(pid) for pid in partition_ids],
                waited_seconds=round(bound, 3),
            )

    async def _release_split_partitions(self, partition_ids: list[int], deadline: float) -> None:
        """Tell the job tracker to drop the revoked partitions and their job tables; a failure is logged.

        All partitions are released at once, under one time limit taken from
        the ``deadline`` of the whole revoke: the rebalance callback waits
        for this, and one limit for each partition would add up to far more
        than the drain timeout when the client of the job tracker hangs.
        """
        runtime = self._owner_runtime()
        if runtime is None or not partition_ids:
            return
        releases = {asyncio.ensure_future(runtime.release(pid)): pid for pid in partition_ids}
        _, late = await asyncio.wait(releases, timeout=_split_step_bound(deadline))
        for release, pid in releases.items():
            if release in late:
                release.cancel()
                self._log_release_failure(pid, 'the release did not end in time')
            elif not release.cancelled() and (error := release.exception()) is not None:
                self._log_release_failure(pid, describe_error(error))
        if late:
            # A cancelled release ends at once unless it ignores its cancel; then it is left behind.
            await asyncio.wait(late, timeout=MIN_TEARDOWN_STEP_SECONDS)

    def _log_release_failure(self, partition_id: int, error: str) -> None:
        logger.warning('split_release_failed', category='split', partition=self._shown(partition_id), error=error)

    def _notify_handler_of_assign(self, partition_ids: list[int]) -> None:
        """Call the handler's ``on_assign`` hook on a background task."""
        ctx = self._context()

        async def _on_assign_with_ctx() -> None:
            bind_contextvars(hook='on_assign', partitions=partition_ids)
            try:
                await ctx.handler.on_assign(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_assign_with_ctx()))
        self.background_tasks.add(t)
        t.add_done_callback(self.background_tasks.discard)

    async def on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation, blocking until the drain commits.

        This coroutine does NOT return until every revoked partition has
        drained and committed. ``AIOConsumer`` runs rebalance callbacks via
        ``run_coroutine_threadsafe(...).result()``, so librdkafka's
        rebalance thread waits here — which is what holds the rebalance
        open until our offsets are committed.

        Returning early and finishing the drain on a detached background
        task would let the rebalance complete while this worker is still
        draining: the new owner would begin consuming from the last
        committed offset while in-flight work here is still producing sink
        deliveries for the same messages, so every message between the
        last commit and the drain end would be delivered twice.

        The wait is bounded, not open-ended: every ``_stop_processor`` runs
        against one ``executor.drain_timeout`` deadline that covers
        the drain, the final commit RPC and the processor stop, so this
        always returns within roughly that budget. That bound matters —
        ``run_coroutine_threadsafe(...).result()`` has no timeout of its
        own, so an unbounded wait here would wedge the consumer thread
        permanently. Teardown runs concurrently across partitions, so N
        revoked partitions cost one drain timeout, not N.

        A drain that expires cancels the tasks it was waiting for rather
        than leaving them running: their results are discarded anyway, and
        an uncancelled zombie holds an executor slot until
        ``executor.task_timeout`` while it also keeps the processing loop
        from exiting, adding a full stop timeout on top of the drain.

        The handler's ``on_revoke`` hook stays on a background task — it is
        a user notification, not part of the commit contract, and a slow
        hook must not eat into the rebalance budget.
        """
        ctx = self._context()
        if ctx.recorder:
            ctx.recorder.record_revoked([self._shown(pid) for pid in partition_ids])
        # One deadline for the whole revoke: the cancel of the restores, the stop of the processors
        # and the release of the split partitions all come out of the same budget.
        deadline = time.monotonic() + ctx.config.executor.drain_timeout
        # First: a restore that ends now would start a processor of a partition that is gone.
        was_restoring = self.restoring_partitions.intersection(partition_ids)
        waited_for_restores = bool(was_restoring) or not self._restore_tasks.keys().isdisjoint(partition_ids)
        if waited_for_restores:
            await self._cancel_restores(partition_ids, deadline)
        to_stop: list[PartitionProcessor] = []
        for pid in partition_ids:
            # A revoked partition is no longer ours — clear any stall-pause
            # bookkeeping so a future reassignment starts fresh.
            self.stalled_partitions.discard(pid)
            self.dead_partitions_paused.discard(pid)
            processor = self.processors.pop(pid, None)
            with contextlib.suppress(KeyError):
                consumer_lag.remove(str(self._shown(pid)))
            if processor:
                if pid in was_restoring:
                    # Not started, and its restore was cancelled: no loop will read what was fetched for it.
                    processor.drop_queue()
                to_stop.append(processor)

        self._gauges.assigned(len(self.processors))
        if not self.processors and self.paused:
            # Nothing is left to pause, so the next assignment (after a join,
            # say) must not start under an old backpressure pause.
            self.paused = False
            self._gauges.backpressure(False)

        if to_stop:
            self._revokes_in_progress += 1
            try:
                # A revoke that spent no time on restores starts the deadline of each stop now, as it
                # always did; one that did gives the stops what is left of its own deadline.
                stops = (
                    (self._stop_processor(p, deadline) for p in to_stop)
                    if waited_for_restores
                    else (self._stop_processor(p) for p in to_stop)
                )
                await asyncio.gather(*stops)
            finally:
                self._revokes_in_progress -= 1

        # After the processors stopped: an end of a job in progress read the table until then.
        await self._release_split_partitions(partition_ids, deadline)
        self._notify_handler_of_revoke(partition_ids)

    def _notify_handler_of_revoke(self, partition_ids: list[int]) -> None:
        """Call the handler's ``on_revoke`` hook on a background task."""
        ctx = self._context()

        async def _on_revoke_with_ctx() -> None:
            bind_contextvars(hook='on_revoke', partitions=partition_ids)
            try:
                await ctx.handler.on_revoke(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_revoke_with_ctx()))
        self.background_tasks.add(t)
        t.add_done_callback(self.background_tasks.discard)

    async def _safe_call(self, coro: Coroutine) -> None:
        """Run a coroutine and log any exception instead of leaving it unretrieved.

        For best-effort user hooks (on_assign/on_revoke) and auxiliary
        framework work only. Critical cleanup paths like _stop_processor
        must NOT go through this wrapper — they carry their own
        error handling with forced teardown.
        """
        try:
            await coro
        except Exception as e:
            logger.warning(
                'async_callback_failed',
                category='lifecycle',
                error=str(e),
                error_type=type(e).__name__,
                exc_info=True,
            )

    async def pause_stalled_partition(self, partition_id: int) -> None:
        """Pause a partition whose watermark stalled (dlq.on_send_failure=stall).

        Called (once per processor lifetime) by ``PartitionProcessor`` when
        the first offset stalls. Pausing stops Kafka from delivering new
        messages so the stall doesn't snowball: without it, every message
        processed past the stall point would be re-processed (and
        re-delivered to sinks) after restart, and the offset tracker would
        grow without bound. The partition stays paused until the worker
        restarts or the partition is revoked — ``stalled_partitions``
        keeps the backpressure resume cycle from silently un-pausing it.
        """
        ctx = self._context()
        self.stalled_partitions.add(partition_id)
        if ctx.recorder:
            ctx.recorder.record_partition_stalled(self._shown(partition_id))
        if self.consumer is not None:
            try:
                await self.consumer.pause([partition_id])
                logger.error(
                    'partition_paused_on_stall',
                    category='lifecycle',
                    partition=self._shown(partition_id),
                    hint='delivery (incl. DLQ) unconfirmed and dlq.on_send_failure=stall; '
                    'partition paused — fix the downstream and restart the worker to resume',
                )
            except Exception as e:
                logger.error(
                    'partition_stall_pause_failed',
                    category='lifecycle',
                    partition=self._shown(partition_id),
                    error=str(e),
                    exc_info=True,
                )

    async def _pause_dead_partitions(self, consumer: KafkaConsumer) -> None:
        """Pause, once, each partition whose processing loop died.

        A dead loop never empties its queue, so its messages are left out of
        the pause rule and the partition stays paused, like a stalled one.
        """
        newly_dead = [pid for pid, p in self.processors.items() if p.is_dead and pid not in self.dead_partitions_paused]
        if not newly_dead:
            return
        self.dead_partitions_paused.update(newly_dead)
        try:
            await consumer.pause(newly_dead)
            logger.error(
                'partition_paused_on_death',
                category='lifecycle',
                partitions=[self._shown(pid) for pid in newly_dead],
                hint='the partition loop died; the partition stays paused until restart or revoke',
            )
        except Exception as e:
            # Not marked as paused, so the next poll turn tries again: a dead
            # partition left fetching fills a queue that nothing reads.
            self.dead_partitions_paused.difference_update(newly_dead)
            logger.error(
                'partition_death_pause_failed',
                category='lifecycle',
                partitions=[self._shown(pid) for pid in newly_dead],
                error=str(e),
                exc_info=True,
            )

    async def _stop_processor(self, processor: PartitionProcessor, deadline: float | None = None) -> None:
        """Drain in-flight tasks, commit final offsets, then stop.

        ``deadline`` (monotonic) is the deadline of a teardown that already
        spent time on an earlier step; None starts one of
        ``executor.drain_timeout`` now.

        Only commits the watermark when drain completed cleanly. If drain
        timed out, tasks may still be in flight — committing their offsets
        now would silently skip them on partition reassign and lose data.
        Preferring at-least-once duplication over silent loss.
        """
        drain_timeout = self._context().config.executor.drain_timeout
        # One deadline for the whole teardown. librdkafka's rebalance thread
        # is blocked on this coroutine, so every step below has to come out
        # of the same budget — a step that takes its own would let the
        # callback overrun ``max.poll.interval.ms`` and get the member
        # evicted, which triggers the next rebalance.
        if deadline is None:
            deadline = time.monotonic() + drain_timeout
        try:
            processor.signal_stop()
            drained_cleanly = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=_remaining(deadline))
                drained_cleanly = True
            except TimeoutError:
                # Tasks still running are zombies now — the new partition
                # owner replays their messages, so their late results must
                # not reach sinks (double-write) or commit offsets
                # (clobbering the new owner's progress).
                processor.suppress_deliveries()
                cancelled = await processor.cancel_active_tasks()
                logger.warning(
                    'stop_processor_drain_timeout',
                    category='lifecycle',
                    partition=self._shown(processor.partition_id),
                    inflight=processor.inflight_count,
                    queue_size=processor.queue_size,
                    cancelled_tasks=cancelled,
                )
            if drained_cleanly:
                committable = processor.offset_tracker.committable()
                if committable is not None and self.consumer:
                    try:
                        # Bounded: this is a synchronous librdkafka commit
                        # dispatched to the consumer's thread pool, and a
                        # coordinator that stopped answering would otherwise
                        # hold the rebalance callback open with no deadline.
                        await asyncio.wait_for(
                            self.consumer.commit({processor.partition_id: committable}),
                            timeout=_remaining(deadline),
                        )
                        processor.offset_tracker.acknowledge_commit(committable)
                    except Exception as e:
                        logger.warning(
                            'stop_processor_commit_failed',
                            category='kafka',
                            partition=self._shown(processor.partition_id),
                            error=str(e),
                        )
            await processor.stop(timeout=_remaining(deadline))
        except Exception as e:
            # Critical cleanup path: a failure here must not leave the
            # processor running (it would keep consuming executor slots
            # with no owner). Log loudly with the full traceback, then
            # force-stop as a last resort.
            logger.error(
                'stop_processor_failed',
                category='lifecycle',
                partition=self._shown(processor.partition_id),
                error=str(e),
                error_type=type(e).__name__,
                exc_info=True,
            )
            try:
                await processor.stop()
            except Exception as stop_exc:
                logger.error(
                    'stop_processor_force_stop_failed',
                    category='lifecycle',
                    partition=self._shown(processor.partition_id),
                    error=str(stop_exc),
                    exc_info=True,
                )

    async def _drain_all_processors(self, processors: list[PartitionProcessor]) -> None:
        """Wait for the given partition processors to finish queued + in-flight work.

        Takes an explicit snapshot instead of reading ``self.processors``
        so a rebalance that pops processors mid-shutdown cannot shrink the
        drain set under us. Processors whose only pending offsets are
        stalled (delivery unconfirmed) drain promptly — ``drain()`` itself
        excludes stalled offsets from its wait condition.
        """
        drain_tasks = [
            processor.drain()
            for processor in processors
            # A dead processor has no loop left to empty its queue or settle
            # its pending offsets, so including it guarantees the whole drain
            # hits the timeout and every healthy partition's commit is
            # suppressed with it. ``drain()`` returns at once for one anyway;
            # skipping it here keeps the intent visible at the call site.
            if not processor.is_dead
            and (processor.queue_size > 0 or processor.offset_tracker.has_pending() or processor.inflight_count > 0)
        ]
        if drain_tasks:
            await asyncio.gather(*drain_tasks)

    def total_queued(self) -> int:
        """Total messages buffered across all partition queues + in-flight tasks."""
        return sum(p.queue_size + p.inflight_count for p in self.processors.values())

    def queued_messages(self) -> int:
        """Messages waiting in the queues of live partitions: the input of the pause rule."""
        restoring = self.restoring_partitions
        return sum(p.queue_size for pid, p in self.processors.items() if not p.is_dead and pid not in restoring)

    def total_waiting(self) -> int:
        """Messages waiting in partition queues, not yet dispatched to executors."""
        return sum(p.queue_size for p in self.processors.values())

    def dead_partitions(self) -> list[int]:
        """Partitions whose processing loop gave up; ``/readyz`` fails while any exist."""
        return sorted(self._shown(pid) for pid, p in self.processors.items() if p.is_dead)

    def uncommitted_offsets(self) -> int:
        """Pending (uncommitted) offsets across every assigned partition."""
        return sum(p.offset_tracker.pending_count for p in self.processors.values())

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        if self.consumer:
            await self.consumer.commit({partition_id: offset})
        runtime = self._owner_runtime()
        if runtime is not None:
            # The committed value is the next offset to read: every job below it is done.
            runtime.note_source_commit(partition_id, offset)
        recorder = self._context().recorder
        if recorder:
            recorder.record_committed(self._shown(partition_id), offset)
