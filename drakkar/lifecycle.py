"""Application lifecycle for :class:`drakkar.app.DrakkarApp`.

The lifecycle is the slice of ``DrakkarApp`` that drives the running
worker — startup orchestration, the Kafka poll loop, partition assign /
revoke callbacks, and graceful shutdown / drain. It was extracted from
``DrakkarApp`` so the public app class stays focused on wiring (config,
handler, sink manager, recorder) while this class owns the event-loop-bound
machinery.

Design notes
============

- ``AppLifecycle`` is **internal-use** — it is not exported from
  :mod:`drakkar` and not part of the public API. Third-party code must
  keep calling :meth:`DrakkarApp.run`, not poke at ``app._lifecycle``
  directly. The class name itself is plain PascalCase per project style:
  the public surface is controlled by :mod:`drakkar.__init__` exports,
  not by underscore-prefixing class names.
- The class holds a single back-reference, ``self._app``. All extracted
  methods read and write app state via ``self._app.<attr>`` so the move
  is a pure relocation — no semantic change to which object owns
  ``_running``, ``_paused``, ``_processors``, etc. The app remains the
  single source of truth for that state because the debug server and
  user-facing properties continue to read it from ``DrakkarApp``.
- ``DrakkarApp`` instantiates an ``AppLifecycle`` eagerly in ``__init__``
  so tests that exercise the extracted methods directly can do so via
  ``app._lifecycle._on_assign(...)`` without first running the full
  startup sequence.
"""

from __future__ import annotations

import asyncio
import math
import signal
import time
from collections.abc import Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.app_security import warn_if_ui_unauthenticated
from drakkar.cache import Cache, CacheEngine
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.logging import close_logging
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    consumer_idle,
    discover_handler_metrics,
    drain_timeout_hit,
    executor_idle_waste,
    inflight_at_stop,
    messages_unassigned_dropped,
    start_metrics_server,
    total_queued,
    uncommitted_offsets_at_stop,
    worker_info,
)
from drakkar.partition import PartitionProcessor
from drakkar.periodic import discover_periodic_tasks, run_periodic_task
from drakkar.recorder import EventRecorder
from drakkar.sinks.manager import SinkNotConfiguredError
from drakkar.utils import wait_for_aligned_startup
from drakkar.watchdog import WatchdogFile

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

# Seconds to sleep when Kafka poll returns no messages. Mirrors the
# constant previously defined in ``drakkar.app`` and re-exported here
# from a single location to keep the lifecycle self-contained.
POLL_IDLE_SLEEP = 0.05


class AppLifecycle:
    """Lifecycle driver for :class:`DrakkarApp` (internal use).

    All methods read and mutate state on the back-referenced ``DrakkarApp``;
    none of the underlying state was duplicated during the extraction.
    See module docstring for the design rationale.

    Although this class is not part of the public API (not exported from
    :mod:`drakkar`), its name carries no leading underscore — project
    style uses plain PascalCase for all classes and controls the public
    surface via package-level ``__init__`` exports.
    """

    def __init__(self, drakkar_app: DrakkarApp) -> None:
        # Back-reference. We use a single attribute to keep the boundary
        # explicit: every read or write goes through ``self._app.X``.
        self._app = drakkar_app
        # Watchdog file for OOM/SIGKILL detection across restarts.
        # Constructed in ``_async_run`` once we know the worker_id and
        # the resolved data directory; held here so tests can reach it
        # via ``app._lifecycle._watchdog``.
        self._watchdog: WatchdogFile | None = None

    async def _async_run(self) -> None:
        """Full async startup → poll-loop → shutdown sequence.

        Mirrors the previous ``DrakkarApp._async_run`` byte-for-byte,
        with each ``self.X`` rewritten to ``self._app.X`` so the app
        remains the single source of truth for instance state.
        """
        app = self._app

        # Capture the running loop so the debug server (separate thread)
        # can dispatch probes back here for ExecutorPool access.
        app._loop = asyncio.get_running_loop()

        log = logger.bind(worker_id=app._worker_id)

        # Watchdog file for OOM / SIGKILL detection. Resolves the durable
        # directory from ``config.ui.recorder.db_dir`` (the canonical location
        # for per-worker durable files in this codebase — already used by
        # the recorder and the cache engine). When ``db_dir`` is empty —
        # fully disk-less deployment where the operator deliberately
        # disabled every on-disk file — we skip the watchdog entirely
        # rather than fall back to the worker's CWD: the CWD is often a
        # read-only volume in containers, and falling back there would
        # either crash or break the "no on-disk state" promise. Operators
        # opting into disk-less mode forfeit the OOM signal; that tradeoff
        # is documented in ``docs/observability.md``.
        #
        # We CONSTRUCT the WatchdogFile here (so ``check_previous`` runs
        # before any startup work — order matters: a startup that crashes
        # before subscribe must still have read the previous run's
        # watchdog), but the actual ``write()`` (which truncates the file
        # to empty body, the SIGKILL signature) is deferred until we are
        # committed to running. Otherwise an exception during sink
        # connect / consumer.subscribe / on_startup would leave the empty
        # body and falsely flag the next startup as OOM-killed.
        if app._config.ui.recorder.db_dir:
            watchdog_dir = Path(app._config.ui.recorder.db_dir)
            self._watchdog = WatchdogFile(data_dir=watchdog_dir, worker_id=app._worker_id)
            # Detect a possible SIGKILL from the prior run BEFORE we
            # claim the slot for this run. ``check_previous`` returns
            # True when no suspect-OOM signature was found; logging that
            # at info level lets operators confirm the watchdog ran
            # without grepping the warn-only suspect path.
            previous_run_clean = self._watchdog.check_previous()
            if previous_run_clean:
                await log.ainfo(
                    'watchdog_previous_run_clean',
                    category='watchdog',
                    worker_id=app._worker_id,
                )
        else:
            self._watchdog = None
            await log.ainfo(
                'watchdog_disabled_no_db_dir',
                category='watchdog',
                reason='ui.recorder.db_dir is empty — OOM/SIGKILL detection disabled for this run',
            )

        bind_contextvars(hook='on_startup')
        app._config = await app._handler.on_startup(app._config)
        unbind_contextvars('hook')

        app._config_summary = app._config.config_summary(
            worker_id=app._worker_id,
            cluster_name=app._cluster_name,
        )
        await log.ainfo('drakkar_starting', category='lifecycle', config=app._config_summary)

        # validate at least one sink is configured
        if app._config.sinks.is_empty:
            raise SinkNotConfiguredError('No sinks configured. Add at least one sink to the sinks: section in config.')

        app._executor_pool = ExecutorPool(
            binary_path=app._config.executor.binary_path,
            max_executors=app._config.executor.max_executors,
            task_timeout_seconds=app._config.executor.task_timeout_seconds,
            env=app._config.executor.env,
            inherit_parent_env=app._config.executor.env_inherit_parent,
            inherit_deny_patterns=app._config.executor.env_inherit_deny,
            # Wire the handler's priority hook into the pool's wait queue.
            # Handlers that don't override ``task_priority`` get the
            # framework default (smallest source offset → older messages
            # drain first). See ``BaseDrakkarHandler.task_priority`` for
            # the override contract.
            priority_fn=app._handler.task_priority,
        )

        start_metrics_server(app._config.metrics)
        worker_info.info(
            {
                'worker_id': app._worker_id,
                'version': __version__,
                'consumer_group': app._config.kafka.consumer_group,
            }
        )

        user_metrics = discover_handler_metrics(app._handler)
        if user_metrics:
            await log.ainfo(
                'user_metrics_discovered',
                category='lifecycle',
                metrics=[f'{m._name} ({m._type})' for m in user_metrics.values()],
            )

        if app._config.ui.enabled:
            # Auth is opt-in. Emit a startup warning naming how to set a
            # token when none is configured — the UI is read-only by design
            # and meant for private-network deployment, so this is
            # informational rather than fail-fast. See README §"Security &
            # trust model" for the rationale.
            warn_if_ui_unauthenticated(app._config)

            app._recorder = EventRecorder(
                app._config.ui,
                worker_name=app._worker_id,
                cluster_name=app._cluster_name,
            )
            app._recorder.set_state_provider(app._get_worker_state)
            await app._recorder.start()
            await app._recorder.write_config(app._config)

            from drakkar.uiserver.server import UIServer

            app._ui_server = UIServer(
                config=app._config.ui,
                recorder=app._recorder,
                app=app,
            )
            await app._ui_server.start()

        # Framework cache. Constructed after the recorder so the cache
        # engine can pass it as the sink for its periodic_run events. If
        # cache.enabled=false, we leave the handler's default NoOpCache stub
        # in place — user code can call self.cache.<method>(...) unconditionally.
        if app._config.cache.enabled:
            app._cache_engine = CacheEngine(
                config=app._config.cache,
                ui_config=app._config.ui,
                worker_id=app._worker_id,
                cluster_name=app._cluster_name,
                recorder=app._recorder,
            )
            # The handler-facing Cache: origin_worker_id is this worker's
            # id so LWW tiebreaks during peer-sync can identify our writes.
            handler_cache = Cache(
                origin_worker_id=app._worker_id,
                max_memory_entries=app._config.cache.max_memory_entries,
            )
            # Wire the Cache to the engine BEFORE start() so the engine's
            # reader connection is attached atomically as part of start().
            app._cache_engine.attach_cache(handler_cache)
            await app._cache_engine.start()
            # Replace the handler's default NoOpCache with the real one.
            # Users access via self.cache regardless of which variant is
            # installed — signatures are identical.
            app._handler.cache = handler_cache

        # build and connect sinks
        app._build_sinks()
        await app._sink_manager.connect_all()

        # build and connect DLQ
        app._build_dlq()
        assert app._dlq_sink is not None
        await app._dlq_sink.connect()

        # Wire recorder + DLQ into the sink manager now that both are ready.
        # SinkManager was constructed in ``__init__`` (required by tests and
        # by ``_build_sinks`` which registers sinks before we get here) with
        # ``recorder=None`` / ``dlq_sink=None`` placeholders. ``attach_runtime``
        # is the named one-time wiring step at the boundary between
        # construction and runtime — same pattern as ``BaseSink.mark_connected``.
        app._sink_manager.attach_runtime(
            recorder=app._recorder,
            dlq_sink=app._dlq_sink,
            dlq_on_send_failure=app._config.dlq.on_send_failure,
        )

        # log sink topology
        await log.ainfo(
            'sinks_configured',
            category='lifecycle',
            sinks=app._config.sinks.summary(),
            dlq_topic=app._dlq_sink.topic,
        )

        # Webapp HTTP server. Constructed AFTER sinks connect (so the
        # readyz/health gate the route uses is meaningful) and BEFORE
        # consumer subscribe (so the route is reachable even during the
        # short window before the first poll completes — at which point
        # the not_ready 503 gate keeps requests from racing the pipeline).
        # Wrapped in try/except so a webapp construction failure does
        # NOT abort startup — sink and consumer setup is the critical
        # path; the webapp is optional infrastructure that an operator
        # can disable on the next config reload.
        if app._config.webapp.enabled:
            try:
                from drakkar.webapp import WebApp

                app._webapp = WebApp(app, app._config.webapp)
                app._webapp.start_in_thread()
                # 5s is generous for a clean uvicorn bind on a free port;
                # tests use the same default. A startup-time TimeoutError
                # is logged and the worker proceeds without the webapp.
                app._webapp.wait_until_ready(timeout=5.0)
            except Exception as exc:
                await log.aerror(
                    'webapp_start_failed',
                    category='webapp',
                    error=str(exc),
                    exc_type=type(exc).__name__,
                )
                app._webapp = None

        app._consumer = KafkaConsumer(
            config=app._config.kafka,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # expose postgres pool for on_ready if available
        pg_pool = None
        for (sink_type, _), sink in app._sink_manager.sinks.items():
            if sink_type == 'postgres' and hasattr(sink, 'pool'):
                pg_pool = sink.pool
                break

        bind_contextvars(hook='on_ready')
        await app._handler.on_ready(app._config, pg_pool)
        unbind_contextvars('hook')

        # start periodic tasks declared on the handler
        for name, method, meta in discover_periodic_tasks(app._handler):
            task = asyncio.create_task(
                run_periodic_task(
                    name=name,
                    coro_fn=method,
                    seconds=meta.seconds,
                    on_error=meta.on_error,
                    recorder=app._recorder,
                ),
                name=f'periodic:{name}',
            )
            app._periodic_tasks.append(task)

        # Stagger startup: sleep until the next wall-clock alignment
        # boundary so a fleet of workers in a rolling deploy converges
        # on a single Kafka consumer-group rebalance instead of N. See
        # KafkaConfig.startup_align_* for tuning and rationale.
        if app._config.kafka.startup_align_enabled:
            min_wait = app._config.kafka.startup_min_wait_seconds
            interval = app._config.kafka.startup_align_interval_seconds
            target_wall = math.ceil((time.time() + min_wait) / interval) * interval
            await log.ainfo(
                'startup_align_waiting',
                category='lifecycle',
                min_wait_seconds=min_wait,
                align_interval_seconds=interval,
                target_wall_unix=target_wall,
                target_wall_iso=datetime.fromtimestamp(target_wall, tz=UTC).isoformat(),
            )
            slept = await wait_for_aligned_startup(min_wait, interval)
            await log.ainfo('startup_align_done', category='lifecycle', slept_seconds=round(slept, 3))

        await app._consumer.subscribe()

        # Claim the watchdog slot for this run NOW — only once we're
        # committed to running. See ``_claim_watchdog_slot`` for the
        # OSError-tolerance contract; deferring the call to this point
        # ensures a startup-stage exception (above) leaves any previous
        # watchdog state untouched and never falsely flags the next
        # startup as OOM-killed.
        await self._claim_watchdog_slot()

        app._running = True

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal)

        try:
            await self._poll_loop()
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _poll_loop(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume."""
        app = self._app
        assert app._consumer is not None
        assert app._executor_pool is not None
        max_executors = app._config.executor.max_executors
        high_watermark = max_executors * app._config.executor.backpressure_high_multiplier
        low_watermark = max(1, max_executors * app._config.executor.backpressure_low_multiplier)
        last_tick = time.monotonic()

        while app._running:
            now = time.monotonic()
            dt = now - last_tick
            last_tick = now

            total = app._total_queued()
            total_queued.set(total)

            # Executor idle waste: slots sitting free while messages wait in queues.
            # Uses queue_size only (not inflight) — inflight tasks ARE using slots.
            waiting = app._total_waiting()
            if waiting > 0:
                idle_slots = max_executors - app._executor_pool.active_count
                if idle_slots > 0:
                    executor_idle_waste.inc(idle_slots * dt)

            if app._paused and total <= low_watermark:
                # Never resume partitions paused by a delivery stall — they
                # stay paused until restart/revoke regardless of backpressure.
                partition_ids = [p for p in app._processors if p not in app._stalled_partitions]
                if partition_ids:
                    await app._consumer.resume(partition_ids)
                    app._paused = False
                    backpressure_active.set(0)

            if not app._paused and total >= high_watermark:
                partition_ids = list(app._processors.keys())
                if partition_ids:
                    await app._consumer.pause(partition_ids)
                    app._paused = True
                    backpressure_active.set(1)

            messages = await app._consumer.poll_batch()
            for msg in messages:
                processor = app._processors.get(msg.partition)
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

            # After the first poll completes successfully we consider the
            # worker ready to serve traffic — the consumer is subscribed,
            # sinks were connected before the loop started, and at least
            # one poll round-trip has finished. Kubernetes readiness probes
            # can now flip us into the service endpoints. Idempotent: the
            # assignment on subsequent iterations is a no-op.
            app.is_ready = True

            if not messages:
                # Consumer idle: no messages from Kafka, nothing queued, not paused.
                # Measures time with genuinely nothing to do (consumer lag is zero).
                if total == 0 and not app._paused:
                    consumer_idle.inc(dt)
                await asyncio.sleep(POLL_IDLE_SLEEP)

    def _on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        app = self._app
        assert app._executor_pool is not None
        if app._recorder:
            app._recorder.record_assigned(partition_ids)
        newly_added: list[int] = []
        for pid in partition_ids:
            if pid not in app._processors:
                processor = PartitionProcessor(
                    partition_id=pid,
                    handler=app._handler,
                    executor_pool=app._executor_pool,
                    window_size=app._config.executor.window_size,
                    max_retries=app._config.executor.max_retries,
                    on_collect=app._handle_collect,
                    on_commit=app._handle_commit,
                    recorder=app._recorder,
                    on_parse_error=app._config.kafka.on_parse_error,
                    dlq_send=app._dlq_sink.send if app._dlq_sink else None,
                    on_dlq_failure=app._config.dlq.on_send_failure,
                    on_stall=self._pause_stalled_partition,
                )
                app._processors[pid] = processor
                processor.start()
                newly_added.append(pid)

        assigned_partitions.set(len(app._processors))

        # If backpressure is active, the poll loop has already paused the
        # previously-assigned partitions. Newly-assigned partitions were not
        # in that pause set, so Kafka would deliver messages from them until
        # the next poll tick re-evaluated the watermark. Pause them now so
        # the backpressure gate is not bypassed between assignment and the
        # next _poll_loop iteration.
        if app._paused and newly_added and app._consumer is not None:
            consumer = app._consumer

            async def _pause_newly_assigned() -> None:
                await consumer.pause(newly_added)

            pt = asyncio.ensure_future(self._safe_call(_pause_newly_assigned()))
            app._background_tasks.add(pt)
            pt.add_done_callback(app._background_tasks.discard)

        async def _on_assign_with_ctx() -> None:
            bind_contextvars(hook='on_assign', partitions=partition_ids)
            try:
                await app._handler.on_assign(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_assign_with_ctx()))
        app._background_tasks.add(t)
        t.add_done_callback(app._background_tasks.discard)

    def _on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation."""
        app = self._app
        if app._recorder:
            app._recorder.record_revoked(partition_ids)
        for pid in partition_ids:
            # A revoked partition is no longer ours — clear any stall-pause
            # bookkeeping so a future reassignment starts fresh.
            app._stalled_partitions.discard(pid)
            processor = app._processors.pop(pid, None)
            if processor:
                t = asyncio.ensure_future(self._stop_processor(processor))
                app._background_tasks.add(t)
                t.add_done_callback(app._background_tasks.discard)

        assigned_partitions.set(len(app._processors))

        async def _on_revoke_with_ctx() -> None:
            bind_contextvars(hook='on_revoke', partitions=partition_ids)
            try:
                await app._handler.on_revoke(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_revoke_with_ctx()))
        app._background_tasks.add(t)
        t.add_done_callback(app._background_tasks.discard)

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

    async def _pause_stalled_partition(self, partition_id: int) -> None:
        """Pause a partition whose watermark stalled (dlq.on_send_failure=stall).

        Called (once per processor lifetime) by ``PartitionProcessor`` when
        the first offset stalls. Pausing stops Kafka from delivering new
        messages so the stall doesn't snowball: without it, every message
        processed past the stall point would be re-processed (and
        re-delivered to sinks) after restart, and the offset tracker would
        grow without bound. The partition stays paused until the worker
        restarts or the partition is revoked — ``_stalled_partitions``
        keeps the backpressure resume cycle from silently un-pausing it.
        """
        app = self._app
        app._stalled_partitions.add(partition_id)
        if app._recorder:
            app._recorder.record_partition_stalled(partition_id)
        if app._consumer is not None:
            try:
                await app._consumer.pause([partition_id])
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

    async def _claim_watchdog_slot(self) -> None:
        """Write the per-worker watchdog file, tolerating ``OSError``.

        ``WatchdogFile.write`` lazily creates the data directory and
        writes an empty body to the file (the SIGKILL signature). On a
        read-only mount, missing volume, no space, or insufficient
        permissions either step can raise ``OSError`` — the watchdog
        is observability-only and the rest of the worker is fully
        functional without it, so we catch the exception, log a
        structured ``watchdog_write_failed`` warning, and disable the
        watchdog for this run by setting ``self._watchdog = None``.
        ``mark_clean`` later short-circuits when the field is None.

        Idempotent on re-entry: if the watchdog is already disabled
        the method is a no-op.
        """
        if self._watchdog is None:
            return
        try:
            self._watchdog.write()
        except OSError as exc:
            await logger.awarning(
                'watchdog_write_failed',
                category='watchdog',
                path=str(self._watchdog.path),
                error=str(exc),
            )
            self._watchdog = None

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        """Drain in-flight tasks, commit final offsets, then stop.

        Only commits the watermark when drain completed cleanly. If drain
        timed out, tasks may still be in flight — committing their offsets
        now would silently skip them on partition reassign and lose data.
        Preferring at-least-once duplication over silent loss.
        """
        app = self._app
        try:
            processor.signal_stop()
            drained_cleanly = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=app._config.executor.drain_timeout_seconds)
                drained_cleanly = True
            except TimeoutError:
                # Tasks still running are zombies now — the new partition
                # owner replays their messages, so their late results must
                # not reach sinks (double-write) or commit offsets
                # (clobbering the new owner's progress).
                processor.suppress_deliveries()
                logger.warning(
                    'stop_processor_drain_timeout',
                    category='lifecycle',
                    partition=processor.partition_id,
                    inflight=processor.inflight_count,
                    queue_size=processor.queue_size,
                )
            if drained_cleanly:
                committable = processor.offset_tracker.committable()
                if committable is not None and app._consumer:
                    try:
                        await app._consumer.commit({processor.partition_id: committable})
                        processor.offset_tracker.acknowledge_commit(committable)
                    except Exception as e:
                        logger.warning(
                            'stop_processor_commit_failed',
                            category='kafka',
                            partition=processor.partition_id,
                            error=str(e),
                        )
            await processor.stop()
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

    def _handle_signal(self) -> None:
        """Handle shutdown signals."""
        logger.info('shutdown_signal_received', category='lifecycle')
        self._app._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown: cancel periodic tasks, drain executors, commit offsets, close sinks."""
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        await log.ainfo('drakkar_shutting_down', category='lifecycle')

        # Flip the webapp shutdown gate FIRST — ahead of any drain work.
        # New HTTP requests immediately receive a 503 with
        # ``status='shutdown'`` while in-flight requests continue draining
        # naturally. Wrapped in try/except so a webapp-internal hiccup
        # never aborts the wider teardown sequence (sinks, recorder,
        # consumer.close all still need to run).
        if app._webapp is not None:
            try:
                app._webapp.shutdown_event.set()
                await log.ainfo('webapp_shutdown_starting', category='webapp')
            except Exception as exc:
                await log.awarning(
                    'webapp_shutdown_event_set_failed',
                    category='webapp',
                    error=str(exc),
                )

        # Snapshot the drain-phase observability gauges BEFORE doing any
        # drain work. We always call ``.set()`` (even with ``0``) so the
        # gauge reads as "this is the value at the moment shutdown began"
        # rather than "stale value from earlier in the run". See
        # ``drakkar.metrics`` for the metric docstrings.
        #
        # Uncommitted offsets: sum the per-partition offset-tracker pending
        # counts across every assigned partition. ``pending_count`` is the
        # canonical accessor used elsewhere in the framework (e.g. the
        # ``drakkar_offset_lag`` per-partition gauge updated on every
        # message complete in ``PartitionProcessor``).
        uncommitted_total = sum(processor.offset_tracker.pending_count for processor in app._processors.values())
        uncommitted_offsets_at_stop.set(uncommitted_total)

        # In-flight executor tasks: read the pool's running ``active_count``,
        # which is the same accessor used by ``ExecutorPool`` to drive the
        # ``drakkar_executor_pool_active`` gauge during normal operation.
        # The pool may be ``None`` if shutdown is invoked before startup
        # completed (defensive programming for tests / aborted boot).
        inflight_total = app._executor_pool.active_count if app._executor_pool is not None else 0
        inflight_at_stop.set(inflight_total)

        # Flip readiness off IMMEDIATELY so a Kubernetes readiness probe
        # that fires between now and ``close_all`` fails — the pod is
        # taken out of the service endpoints before we start tearing down
        # sinks. Liveness (``/healthz``) stays responsive until the process
        # actually exits.
        app.is_ready = False

        # cancel periodic tasks
        for task in app._periodic_tasks:
            task.cancel()
        if app._periodic_tasks:
            await asyncio.gather(*app._periodic_tasks, return_exceptions=True)
            app._periodic_tasks.clear()

        # Snapshot the processors BEFORE draining: a rebalance firing
        # concurrently with shutdown pops processors from ``_processors``
        # (handing them to ``_stop_processor`` background tasks). Draining
        # from the live dict would skip those, and ``drained_cleanly=True``
        # could fire with their work still in flight.
        processors_snapshot = list(app._processors.values())
        for processor in processors_snapshot:
            processor.signal_stop()

        drain_timeout = app._config.executor.drain_timeout_seconds
        await log.ainfo('draining_executors', category='lifecycle', timeout=drain_timeout)
        drained_cleanly = False
        # Drain-and-teardown wrapped in try/finally so the teardown phase
        # (mark_clean + final commits + processor.stop + cache/recorder/
        # debug-server/sinks/DLQ/consumer.close) ALWAYS runs even when
        # the drain itself raises an unexpected exception. Without this
        # structure, a non-TimeoutError out of ``_drain_all_processors``
        # (e.g. RuntimeError, OSError, an upstream CancelledError) would
        # escape ``_shutdown`` and skip every cleanup step below — leaking
        # connections, partial writes, and a stale (empty-body) watchdog
        # file that the next startup would mis-classify as a SIGKILL.
        try:
            try:
                await asyncio.wait_for(self._drain_all_processors(processors_snapshot), timeout=drain_timeout)
                drained_cleanly = True
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
                for processor in list(app._processors.values()):
                    processor.suppress_deliveries()
                await log.awarning(
                    'drain_timeout',
                    category='lifecycle',
                    msg=f'some executors did not finish in {drain_timeout}s; skipping final commit',
                )
            except Exception as exc:
                # Any non-TimeoutError raised during drain is logged as a
                # distinct event so it does not get conflated with the
                # benign "some tasks took too long" timeout case. We do
                # NOT increment ``drain_timeout_hit`` here — this is a
                # different failure mode (drain bug, processor invariant
                # violation, OS error mid-drain) and should be alerted
                # on its own metric in the future. ``drained_cleanly``
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
        finally:
            # Mark the watchdog clean as soon as the drain phase has been
            # accounted for — drain-timeout / drain-exception are both
            # captured by their own observability and are NOT OOM kills,
            # so we should not leave the watchdog body empty in either
            # case. Conflating the two muddles dashboards: a slow / buggy
            # shutdown looks identical to a SIGKILL. Marking clean here
            # means the OOM counter only ticks for the genuinely-empty-
            # body case (process killed before reaching this line).
            # Wrapped in try/except so a filesystem hiccup at the very
            # end does not mask the drain outcome — observability over
            # availability would be the wrong tradeoff at this layer.
            if self._watchdog is not None:
                try:
                    self._watchdog.mark_clean()
                except OSError as exc:
                    await log.awarning(
                        'watchdog_mark_clean_failed',
                        category='watchdog',
                        error=str(exc),
                        exc_info=True,
                    )

            # Only commit final offsets if drain succeeded cleanly. After
            # a timeout / drain-exception we cannot be sure tasks have
            # stopped running, so committing here would silently skip
            # in-flight work on restart — preferring at-least-once
            # duplication over silent loss.
            if drained_cleanly:
                for processor in list(app._processors.values()):
                    committable = processor.offset_tracker.committable()
                    if committable is not None and app._consumer:
                        try:
                            await app._consumer.commit({processor.partition_id: committable})
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
            for processor in list(app._processors.values()):
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
            app._processors.clear()

            # Wait for background tasks scheduled by rebalance callbacks
            # (_stop_processor from revoke, on_assign/revoke handler hooks,
            # backpressure pauses) to complete BEFORE we close the
            # consumer. These tasks hold references to self._consumer;
            # closing it while they run would cause use-after-close
            # errors and skip their final commits.
            if app._background_tasks:
                bg_snapshot = list(app._background_tasks)
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*bg_snapshot, return_exceptions=True),
                        timeout=drain_timeout,
                    )
                except TimeoutError:
                    await log.awarning(
                        'background_task_drain_timeout',
                        category='lifecycle',
                        count=len(bg_snapshot),
                    )

            # Stop the cache engine BEFORE the recorder so the engine's
            # final flush (``_flush_once`` called inside
            # ``CacheEngine.stop()``) can still record its
            # ``periodic_run`` event through the recorder. If we stopped
            # the recorder first, that last event would be dropped —
            # users lose observability on the most critical flush of the
            # lifecycle (the one that persists whatever was in memory
            # when shutdown signalled). Each subsystem stop is wrapped
            # individually so a failure in one does not skip the others.
            if app._cache_engine is not None:
                try:
                    await app._cache_engine.stop()
                except Exception as exc:
                    await log.awarning(
                        'cache_engine_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )
                app._cache_engine = None

            if app._recorder:
                try:
                    await app._recorder.stop()
                except Exception as exc:
                    await log.awarning(
                        'recorder_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )

            if app._ui_server:
                try:
                    await app._ui_server.stop()
                except Exception as exc:
                    await log.awarning(
                        'debug_server_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )

            # Stop the webapp uvicorn thread. ``stop`` is a sync method
            # (the webapp owns its own thread/loop, not an asyncio task)
            # and bounds its join on ``drain_timeout`` so a stuck request
            # cannot prevent worker shutdown. Wrapped in try/except so a
            # filesystem / thread hiccup at the very end does not skip
            # the consumer close on the next line.
            if app._webapp is not None:
                try:
                    app._webapp.stop(drain_timeout=app._config.executor.drain_timeout_seconds)
                except Exception as exc:
                    await log.awarning(
                        'webapp_stop_failed',
                        category='webapp',
                        error=str(exc),
                        exc_info=True,
                    )
                app._webapp = None

            # close all sinks and DLQ. ``close_all`` already swallows
            # per-sink errors internally; only an unexpected framework
            # bug in close_all itself can raise here, but we still wrap
            # it so the consumer close on the next line still runs.
            try:
                await app._sink_manager.close_all()
            except Exception as exc:
                await log.awarning(
                    'sink_manager_close_failed',
                    category='lifecycle',
                    error=str(exc),
                    exc_info=True,
                )
            if app._dlq_sink:
                try:
                    await app._dlq_sink.close()
                except Exception as exc:
                    await log.awarning(
                        'dlq_sink_close_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )

            if app._consumer:
                try:
                    await app._consumer.close()
                except Exception as exc:
                    await log.awarning(
                        'consumer_close_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )

            await log.ainfo('drakkar_stopped', category='lifecycle')
            close_logging()

    async def _drain_all_processors(self, processors: list[PartitionProcessor]) -> None:
        """Wait for the given partition processors to finish queued + in-flight work.

        Takes an explicit snapshot instead of reading ``app._processors``
        so a rebalance that pops processors mid-shutdown cannot shrink the
        drain set under us. Processors whose only pending offsets are
        stalled (delivery unconfirmed) drain promptly — ``drain()`` itself
        excludes stalled offsets from its wait condition.
        """
        drain_tasks = [
            processor.drain()
            for processor in processors
            if processor.queue_size > 0 or processor.offset_tracker.has_pending() or processor.inflight_count > 0
        ]
        if drain_tasks:
            await asyncio.gather(*drain_tasks)
