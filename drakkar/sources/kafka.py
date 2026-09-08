"""The Kafka consumer as an input source: poll loop, partition processors, backpressure, commits and the rebalance-safe drain."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Coroutine
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.consumer import KafkaConsumer
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    consumer_idle,
    drain_timeout_hit,
    executor_idle_waste,
    messages_unassigned_dropped,
    total_queued,
)
from drakkar.partition import PartitionProcessor
from drakkar.timefmt import format_rfc3339_micro
from drakkar.utils import wait_for_aligned_startup

if TYPE_CHECKING:
    from drakkar.consume_pause import ConsumePauseController
    from drakkar.sources.base import SourceContext

logger = structlog.get_logger()

# Seconds to sleep when Kafka poll returns no messages. Defined once here,
# next to the poll loop, so the source module is self-contained.
POLL_IDLE_SLEEP = 0.05

# Floor for the shares of a teardown deadline handed to a commit RPC or to
# ``PartitionProcessor.stop``. A deadline that has already passed still has to
# let a step that is only waiting on an already-settled future finish, so it
# never gets zero.
MIN_TEARDOWN_STEP_SECONDS = 0.5


def _remaining(deadline: float) -> float:
    """Return what is left of ``deadline``, never below the step floor."""
    return max(deadline - time.monotonic(), MIN_TEARDOWN_STEP_SECONDS)


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
        self.consumer: KafkaConsumer | None = None
        self.background_tasks: set[asyncio.Task] = set()
        self._ready = False
        self._stop_requested = False
        # Set by ``drain``; ``stop`` commits final offsets only when it is True.
        self._drained_cleanly = False
        self._ctx: SourceContext | None = None

    def bind(self, ctx: SourceContext) -> None:
        """Receive the worker context. Called once, before ``start``."""
        self._ctx = ctx

    def _context(self) -> SourceContext:
        """The bound context. A method reaching here before ``bind`` is a wiring bug."""
        assert self._ctx is not None
        return self._ctx

    async def start(self) -> None:
        """Align the fleet, construct the consumer, and subscribe."""
        ctx = self._context()
        config = ctx.config
        log = logger.bind(worker_id=ctx.worker_id)

        # Stagger startup: sleep until the next wall-clock alignment
        # boundary so a fleet of workers in a rolling deploy converges
        # on a single Kafka consumer-group rebalance instead of N. See
        # KafkaSourceConfig.startup_align_* for tuning and rationale.
        if config.sources.kafka.startup_align_enabled:
            min_wait = config.sources.kafka.startup_min_wait_seconds
            interval = config.sources.kafka.startup_align_interval_seconds
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

        self.consumer = KafkaConsumer(
            connection=config.kafka,
            source=config.sources.kafka,
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
        )
        await self.consumer.subscribe()

    async def run(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume."""
        ctx = self._context()
        assert self.consumer is not None
        consumer = self.consumer
        executor = ctx.config.executor
        max_executors = executor.max_executors
        high_watermark = max_executors * executor.backpressure_high_multiplier
        low_watermark = max(1, max_executors * executor.backpressure_low_multiplier)
        last_tick = time.monotonic()

        while not self._stop_requested:
            now = time.monotonic()
            dt = now - last_tick
            last_tick = now

            total = self.total_queued()
            total_queued.set(total)

            # Executor idle waste: slots sitting free while messages wait in queues.
            # Uses queue_size only (not inflight) — inflight tasks ARE using slots.
            waiting = self.total_waiting()
            if waiting > 0:
                idle_slots = max_executors - ctx.executor_pool.active_count
                if idle_slots > 0:
                    executor_idle_waste.inc(idle_slots * dt)

            # An active operator debug pause (ui.consume_pause) outranks the
            # backpressure resume: queues draining below the low watermark
            # must not restart fetching while the operator asked for quiet.
            # The debug resume hands control back here — if backpressure is
            # still holding, this branch resumes once queues drain.
            if self.paused and total <= low_watermark and not self._consume_pause.active:
                # Never resume partitions paused by a delivery stall — they
                # stay paused until restart/revoke regardless of backpressure.
                partition_ids = [p for p in self.processors if p not in self.stalled_partitions]
                if partition_ids:
                    await consumer.resume(partition_ids)
                    self.paused = False
                    backpressure_active.set(0)

            if not self.paused and total >= high_watermark:
                partition_ids = list(self.processors.keys())
                if partition_ids:
                    await consumer.pause(partition_ids)
                    self.paused = True
                    backpressure_active.set(1)

            messages = await consumer.poll_batch()
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
                    messages_unassigned_dropped.labels(partition=str(msg.partition)).inc()
                    logger.warning(
                        'message_for_unassigned_partition_dropped',
                        category='kafka',
                        partition=msg.partition,
                        offset=msg.offset,
                    )

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
                if total == 0 and not self.paused:
                    consumer_idle.inc(dt)
                await asyncio.sleep(POLL_IDLE_SLEEP)

    @property
    def is_ready(self) -> bool:
        """Whether the first poll round-trip has completed."""
        return self._ready

    def signal_stop(self) -> None:
        """Leave the poll loop and stop every processor from accepting new work."""
        self._stop_requested = True
        for processor in list(self.processors.values()):
            processor.signal_stop()

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
            # ``task_timeout_seconds`` and keeps its processing loop from
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
        ``executor.drain_timeout_seconds`` bound, and the background-task wait
        needs that full budget rather than whatever the drain left of a shared
        deadline.
        """
        ctx = self._context()
        log = logger.bind(worker_id=ctx.worker_id)

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
                            partition=processor.partition_id,
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
                    partition=processor.partition_id,
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
                    timeout=ctx.config.executor.drain_timeout_seconds,
                )
            except TimeoutError:
                await log.awarning(
                    'background_task_drain_timeout',
                    category='lifecycle',
                    count=len(bg_snapshot),
                )

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
            'assigned_partitions': sorted(self.processors),
            'partition_count': len(self.processors),
            'paused': self.paused,
            'total_queued': self.total_queued(),
        }

    def on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        ctx = self._context()
        config = ctx.config
        if ctx.recorder:
            ctx.recorder.record_assigned(partition_ids)
        newly_added: list[int] = []
        for pid in partition_ids:
            if pid not in self.processors:
                processor = PartitionProcessor(
                    partition_id=pid,
                    handler=ctx.handler,
                    executor_pool=ctx.executor_pool,
                    window_size=config.executor.window_size,
                    max_retries=config.executor.max_retries,
                    on_collect=ctx.on_collect,
                    on_commit=self._handle_commit,
                    recorder=ctx.recorder,
                    on_parse_error=config.sources.kafka.on_parse_error,
                    dlq_send=ctx.dlq_sink.send if ctx.dlq_sink else None,
                    on_dlq_failure=config.dlq.on_send_failure,
                    on_stall=self.pause_stalled_partition,
                    throughput=ctx.throughput,
                )
                self.processors[pid] = processor
                processor.start()
                newly_added.append(pid)

        assigned_partitions.set(len(self.processors))

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
        against one ``executor.drain_timeout_seconds`` deadline that covers
        the drain, the final commit RPC and the processor stop, so this
        always returns within roughly that budget. That bound matters —
        ``run_coroutine_threadsafe(...).result()`` has no timeout of its
        own, so an unbounded wait here would wedge the consumer thread
        permanently. Teardown runs concurrently across partitions, so N
        revoked partitions cost one drain timeout, not N.

        A drain that expires cancels the tasks it was waiting for rather
        than leaving them running: their results are discarded anyway, and
        an uncancelled zombie holds an executor slot until
        ``task_timeout_seconds`` while it also keeps the processing loop
        from exiting, adding a full stop timeout on top of the drain.

        The handler's ``on_revoke`` hook stays on a background task — it is
        a user notification, not part of the commit contract, and a slow
        hook must not eat into the rebalance budget.
        """
        ctx = self._context()
        if ctx.recorder:
            ctx.recorder.record_revoked(partition_ids)
        to_stop: list[PartitionProcessor] = []
        for pid in partition_ids:
            # A revoked partition is no longer ours — clear any stall-pause
            # bookkeeping so a future reassignment starts fresh.
            self.stalled_partitions.discard(pid)
            processor = self.processors.pop(pid, None)
            if processor:
                to_stop.append(processor)

        assigned_partitions.set(len(self.processors))

        if to_stop:
            await asyncio.gather(*(self._stop_processor(p) for p in to_stop))

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
            ctx.recorder.record_partition_stalled(partition_id)
        if self.consumer is not None:
            try:
                await self.consumer.pause([partition_id])
                logger.error(
                    'partition_paused_on_stall',
                    category='lifecycle',
                    partition=partition_id,
                    hint='delivery (incl. DLQ) unconfirmed and dlq.on_send_failure=stall; '
                    'partition paused — fix the downstream and restart the worker to resume',
                )
            except Exception as e:
                logger.error(
                    'partition_stall_pause_failed',
                    category='lifecycle',
                    partition=partition_id,
                    error=str(e),
                    exc_info=True,
                )

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        """Drain in-flight tasks, commit final offsets, then stop.

        Only commits the watermark when drain completed cleanly. If drain
        timed out, tasks may still be in flight — committing their offsets
        now would silently skip them on partition reassign and lose data.
        Preferring at-least-once duplication over silent loss.
        """
        drain_timeout = self._context().config.executor.drain_timeout_seconds
        # One deadline for the whole teardown. librdkafka's rebalance thread
        # is blocked on this coroutine, so every step below has to come out
        # of the same budget — a step that takes its own would let the
        # callback overrun ``max.poll.interval.ms`` and get the member
        # evicted, which triggers the next rebalance.
        deadline = time.monotonic() + drain_timeout
        try:
            processor.signal_stop()
            drained_cleanly = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=drain_timeout)
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
                    partition=processor.partition_id,
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
                            partition=processor.partition_id,
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
                partition=processor.partition_id,
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
                    partition=processor.partition_id,
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

    def total_waiting(self) -> int:
        """Messages waiting in partition queues, not yet dispatched to executors."""
        return sum(p.queue_size for p in self.processors.values())

    def dead_partitions(self) -> list[int]:
        """Partitions whose processing loop gave up; ``/readyz`` fails while any exist."""
        return sorted(pid for pid, p in self.processors.items() if p.is_dead)

    def uncommitted_offsets(self) -> int:
        """Pending (uncommitted) offsets across every assigned partition."""
        return sum(p.offset_tracker.pending_count for p in self.processors.values())

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        if self.consumer:
            await self.consumer.commit({partition_id: offset})
        recorder = self._context().recorder
        if recorder:
            recorder.record_committed(partition_id, offset)
