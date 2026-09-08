"""Application lifecycle for :class:`drakkar.app.DrakkarApp`.

The lifecycle is the slice of ``DrakkarApp`` that drives the running
worker: it builds the subsystems every input source needs (executor pool,
recorder, cache, sinks), then binds, starts, runs, drains and stops the
enabled sources. It was extracted from ``DrakkarApp`` so the public app
class stays focused on wiring (config, handler, sink manager, recorder)
while this class owns the event-loop-bound machinery.

Design notes
============

- ``AppLifecycle`` is **internal-use** — it is not exported from
  :mod:`drakkar` and not part of the public API. Third-party code must
  keep calling :meth:`DrakkarApp.run`, not poke at ``app._lifecycle``
  directly. The class name itself is plain PascalCase per project style:
  the public surface is controlled by :mod:`drakkar.__init__` exports,
  not by underscore-prefixing class names.
- The class holds a single back-reference, ``self._app``. All methods
  read and write app state via ``self._app.<attr>``; the app remains the
  single source of truth for that state because the debug server and
  user-facing properties continue to read it from ``DrakkarApp``.
- The lifecycle knows nothing about Kafka or HTTP specifics. Everything a
  source needs arrives through one :class:`drakkar.sources.base.SourceContext`,
  and the shutdown order below is the same for every source — see
  :mod:`drakkar.sources.base` for the contract.
- ``DrakkarApp`` instantiates an ``AppLifecycle`` eagerly in ``__init__``
  so tests that exercise the startup steps directly can do so via
  ``app._lifecycle._start_sources()`` without running the full sequence.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.annotations import Annotator
from drakkar.app import (
    changed_consumed_settings,
    reasons_for_settings,
    snapshot_consumed_settings,
)
from drakkar.app_security import warn_if_ui_unauthenticated
from drakkar.cache import Cache, CacheEngine
from drakkar.executor import ExecutorPool
from drakkar.hostinfo import effective_cpu_count
from drakkar.kafka_security import KafkaSecurityConfig, describe_mixed_security
from drakkar.logging import close_logging
from drakkar.metrics import (
    discover_handler_metrics,
    executor_pool_max,
    host_effective_cpus,
    inflight_at_stop,
    start_metrics_server,
    uncommitted_offsets_at_stop,
    worker_info,
)
from drakkar.periodic import discover_periodic_tasks, run_periodic_task
from drakkar.recorder import EventRecorder
from drakkar.recorder.archive import warn_if_archives_unbounded
from drakkar.sinks.manager import SinkNotConfiguredError
from drakkar.sources.base import Source, SourceContext
from drakkar.timeline_events import TimelineEventEmitter
from drakkar.watchdog import WatchdogFile

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()


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

    async def _run_on_startup(self) -> None:
        """Run the handler's ``on_startup`` and adopt the config it returned.

        Most of the config is read after this point, so a change the hook
        makes lands. A handful of settings were already consumed while
        ``DrakkarApp`` was constructed — the sink manager, the handler's app
        config, the worker and cluster names — and changing those here
        changes nothing. Rather than silently dropping such a change, or
        moving construction out of ``__init__``, the ignored settings are
        named in one table (``app.SETTINGS_CONSUMED_BEFORE_ON_STARTUP``) and
        reported at warning level with what to do instead. ``docs/handler.md``
        documents the same boundary.
        """
        app = self._app
        before = snapshot_consumed_settings(app._config)

        bind_contextvars(hook='on_startup')
        try:
            app._config = await app._handler.on_startup(app._config)
        finally:
            unbind_contextvars('hook')

        ignored = changed_consumed_settings(before, app._config)
        if ignored:
            await logger.awarning(
                'on_startup_config_change_ignored',
                category='lifecycle',
                settings=ignored,
                reasons=reasons_for_settings(ignored),
                hint='these settings are read before on_startup runs; set them in the config file or environment',
            )

    async def _async_run(self) -> None:
        """Full async startup → run-the-sources → shutdown sequence.

        Every ``self.X`` is written ``self._app.X`` so the app remains the
        single source of truth for instance state. Startup and the run
        phase are both guarded: whichever one fails, ``_shutdown`` runs
        before the exception leaves this coroutine.
        """
        app = self._app

        # Capture the running loop so the debug server (separate thread)
        # can dispatch probes back here for ExecutorPool access.
        app._loop = asyncio.get_running_loop()

        log = logger.bind(worker_id=app._worker_id)

        # Every startup step below allocates something that must be released:
        # the recorder and the cache open aiosqlite connections whose worker
        # threads are non-daemon, so an exception escaping startup would leave
        # the interpreter blocked in ``threading._shutdown`` — the process stays
        # alive with liveness green, the atexit last-breath flush never runs, and
        # no orchestrator restarts it. Any startup failure therefore runs the
        # same ``_shutdown`` the run phase uses, then re-raises so the worker
        # exits non-zero. ``_shutdown`` is written to tolerate partial state.
        try:
            await self._setup_watchdog()

            await self._run_on_startup()

            app._config_summary = app._config.config_summary(
                worker_id=app._worker_id,
                cluster_name=app._cluster_name,
            )
            await log.ainfo('drakkar_starting', category='lifecycle', config=app._config_summary)
            await self._report_kafka_security()

            # validate at least one sink is configured
            if app._config.sinks.is_empty:
                raise SinkNotConfiguredError(
                    'No sinks configured. Add at least one sink to the sinks: section in config.'
                )

            await self._build_executor_pool()
            await self._start_observability()
            await self._start_ui_and_recorder()
            self._start_runtime_health()
            self._start_throughput()
            self._wire_annotator()
            self._wire_io_executor()
            self._wire_offload_pool()
            await self._start_cache()
            await self._connect_sinks()
            await self._warn_ignored_source_config()
            self._bind_sources()
            await self._run_on_ready_and_periodics()
            await self._start_sources()

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
        except BaseException:
            # A teardown failure must never mask the startup failure that
            # caused it — the latter is what the operator has to fix.
            try:
                await self._shutdown()
            except Exception as teardown_exc:
                await log.aerror(
                    'startup_teardown_failed',
                    category='lifecycle',
                    error=str(teardown_exc),
                    exc_type=type(teardown_exc).__name__,
                    exc_info=True,
                )
            raise

        try:
            await self._run_sources()
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _report_kafka_security(self) -> None:
        """Log how the worker will authenticate to Kafka, and flag mismatches.

        The one-line ``config_summary`` deliberately does not carry security
        (its exact bytes are contractual), so this is where an operator
        confirms the worker negotiated what they configured. Protocol and mechanism only —
        never a username, password, or key path.

        The warning covers the one combination that is almost always a
        mistake: the consumer authenticates, but a sink or DLQ pointing at
        its own brokers carries no security block and would connect in
        plaintext.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        consumer_security = app._config.kafka.security

        await log.ainfo(
            'kafka_security',
            category='lifecycle',
            protocol=consumer_security.protocol,
            mechanism=consumer_security.sasl_mechanism or '',
            summary=consumer_security.describe(),
        )

        clients: list[tuple[str, str, KafkaSecurityConfig]] = [
            ('dlq', app._config.dlq.brokers, app._config.dlq.security),
            *((f'sinks.kafka.{name}', cfg.brokers, cfg.security) for name, cfg in app._config.sinks.kafka.items()),
        ]
        for label, brokers, security in clients:
            # Only a client with its own brokers can diverge; an empty
            # brokers field inherits the consumer's settings wholesale.
            if not brokers:
                continue
            message = describe_mixed_security(security, consumer_security)
            if message:
                await log.awarning(
                    'kafka_security_mismatch',
                    category='lifecycle',
                    client=label,
                    message=message,
                )

    async def _setup_watchdog(self) -> None:
        """Construct the watchdog file and check the previous run.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged. Must run first so ``check_previous()``
        reads the prior run's file, and must NOT call ``write()`` -- that stays
        in ``_claim_watchdog_slot()``, which runs after ``subscribe()``.
        """
        app = self._app
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
            # File I/O on db_dir (often NFS) — keep it off the event loop.
            previous_run_clean = await asyncio.to_thread(self._watchdog.check_previous)
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

    async def _build_executor_pool(self) -> None:
        """Construct the subprocess pool from executor config.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged.
        """
        app = self._app
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
            max_stdout_bytes=app._config.executor.max_stdout_bytes,
            max_stderr_bytes=app._config.executor.max_stderr_bytes,
        )

    async def _start_observability(self) -> None:
        """Start the metrics server and publish worker + handler metrics.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        start_metrics_server(app._config.metrics)
        worker_info.info(
            {
                'worker_id': app._worker_id,
                'version': __version__,
                # Empty when no Kafka source is enabled — see the property.
                'consumer_group': app._config.resolved_consumer_group,
            }
        )

        # Host-capacity check: a pool larger than the CPUs the process can
        # really use makes every subprocess time-share — task wall times
        # stretch and converge with no visible cause. Say so once, loudly, at
        # the moment the operator can still change the config.
        pool_size = app._config.executor.max_executors
        effective_cpus = effective_cpu_count()
        executor_pool_max.set(pool_size)
        host_effective_cpus.set(effective_cpus)
        if pool_size > effective_cpus:
            await log.awarning(
                'executor_pool_exceeds_cpus',
                category='executor',
                pool_size=pool_size,
                effective_cpus=effective_cpus,
                hint=(
                    'executor.max_executors exceeds the CPUs this process can use '
                    '(affinity mask capped by any cgroup quota); concurrent tasks '
                    'will time-share cores and their wall times will stretch'
                ),
            )

        user_metrics = discover_handler_metrics(app._handler)
        if user_metrics:
            await log.ainfo(
                'user_metrics_discovered',
                category='lifecycle',
                metrics=[f'{m._name} ({m._type})' for m in user_metrics.values()],
            )

    async def _start_ui_and_recorder(self) -> None:
        """Start the recorder and UI server, or warn when the UI is disabled.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        if app._config.ui.enabled:
            # Auth is opt-in. Emit a startup warning naming how to set a
            # token when none is configured — no endpoint touches the
            # pipeline and the UI is meant for private-network deployment,
            # so this is informational rather than fail-fast. The warning
            # also names any side-effecting endpoint (probe, merge) left
            # enabled. See docs/faq.md §"Security and trust model".
            warn_if_ui_unauthenticated(app._config)
            # Archives are the one recorder artifact nothing else reclaims,
            # so "keep forever" is stated at startup rather than discovered
            # when a volume fills.
            warn_if_archives_unbounded(app._config.ui.recorder)

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
        else:
            # The UI server is this worker's ONLY probe surface — without
            # it nothing serves /healthz or /readyz and Kubernetes probes
            # simply fail. Warn loudly, because that consequence is easy
            # to miss.
            await log.awarning(
                'ui_disabled_no_probes',
                category='lifecycle',
                reason='ui.enabled=false — /healthz and /readyz are not served; Kubernetes probes need the UI server',
            )

    def _start_runtime_health(self) -> None:
        """Start the event-loop health monitor when enabled.

        Runs after the recorder so transitions/stalls persist as events;
        the recorder handle may still be ``None`` (ui.enabled=false), in
        which case the monitor feeds only Prometheus and the in-memory
        history — deliberately still worth running.
        """
        app = self._app
        if not app._config.runtime_health.enabled:
            return
        from drakkar.runtimehealth import RuntimeHealthMonitor

        app._runtime_health = RuntimeHealthMonitor(
            app._config.runtime_health,
            recorder=app._recorder,
        )
        app._runtime_health.start()

    def _start_throughput(self) -> None:
        """Start the task cost/throughput tracker when configured.

        Runs after the recorder for the same reason as the runtime-health
        monitor: the per-second WS frame broadcasts through it. A None
        recorder (ui.enabled=false) still leaves Prometheus and the
        worker_state snapshots working. Must run before partitions are
        assigned — processors capture the tracker handle at construction.
        """
        app = self._app
        if not app._config.throughput.cost_label:
            return
        from drakkar.throughput import ThroughputTracker

        app._throughput = ThroughputTracker(
            app._config.throughput,
            recorder=app._recorder,
        )
        app._throughput.start()

    def _wire_annotator(self) -> None:
        """Replace the handler's NoOpAnnotator stub with a live one, and wire timeline events on top of it.

        Called right after the recorder starts, since annotations are stored
        as recorder events and there is nowhere to put them otherwise. When
        the recorder is absent, annotations are switched off, or the events
        table is not being written, the class-level no-op stub stays in place
        and ``self.annotate(...)`` remains a cheap call the handler can make
        unconditionally.

        ``store_events`` is part of the condition on purpose: with it false
        the recorder never starts a flush loop, so annotations would fill the
        bounded buffer and be evicted, spending memory and inflating
        ``drakkar_recorder_dropped_events_total`` to no benefit.

        Custom timeline events (``self.timeline_event(...)``) ride the same
        annotator instance, so they share its byte budgets and its
        no-context handling. They are wired only when at least one type is
        declared under ``ui.timeline.events`` — with none declared, every
        emission would be dropped as unknown, so the class-level
        ``NoOpTimelineEventEmitter`` stub stays in place instead.
        """
        app = self._app
        recorder_config = app._config.ui.recorder
        if app._recorder is None or not recorder_config.annotations_enabled or not recorder_config.store_events:
            return
        annotator = Annotator(
            app._recorder,
            max_bytes=recorder_config.annotation_max_bytes,
            max_bytes_per_call=recorder_config.annotation_max_bytes_per_call,
            log_max_bytes=recorder_config.annotation_log_max_bytes,
        )
        app._handler._annotator = annotator

        event_types = app._config.ui.timeline.events
        if event_types:
            app._handler._timeline_events = TimelineEventEmitter(annotator, {t.name: t for t in event_types})

    def _wire_io_executor(self) -> None:
        """Resize asyncio's default to_thread executor when io.max_threads is set.

        Every ``asyncio.to_thread`` call in the process — handler blocking
        I/O plus the framework's own background file work — shares this one
        pool, which Python caps at ``min(32, cpu_count + 4)``. Installing a
        replacement via ``loop.set_default_executor`` lifts that cap;
        ``asyncio.run``'s cleanup (``shutdown_default_executor``) then owns
        its shutdown exactly as it owns the stock pool's, so lifecycle
        teardown needs no extra step. 0 (the default) leaves Python's own
        sizing untouched.
        """
        app = self._app
        max_threads = app._config.io.max_threads
        if max_threads <= 0:
            return
        from concurrent.futures import ThreadPoolExecutor

        executor = ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix='drakkar-io')
        asyncio.get_running_loop().set_default_executor(executor)
        logger.info(
            'io_executor_configured',
            category='lifecycle',
            max_threads=max_threads,
            replaces_default=f'min(32, cpu_count + 4) = {min(32, (os.cpu_count() or 1) + 4)}',
        )

    def _wire_offload_pool(self) -> None:
        """Replace the handler's InlineOffloader stub with the shared pool.

        Runs after the recorder starts so per-call ``offload`` events have
        somewhere to land; with the recorder absent the pool still runs
        and feeds Prometheus only. Unlike the annotator there is no
        enabled/disabled split — the pool is a few idle threads and
        ``handler.offload()`` must behave identically whether or not the
        UI is on.
        """
        app = self._app
        from drakkar.offload import OffloadPool

        app._offload_pool = OffloadPool(
            app._config.offload,
            recorder=app._recorder,
            executor_pool_max=app._config.executor.max_executors,
        )
        app._handler._offloader = app._offload_pool

    async def _start_cache(self) -> None:
        """Construct the cache engine and wire the handler-facing Cache.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged.
        """
        app = self._app
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

    async def _connect_sinks(self) -> None:
        """Build and connect sinks and the DLQ, then wire the sink manager.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        # build and connect sinks
        app._build_sinks()
        await app._sink_manager.connect_all()

        # Build and connect the DLQ. ``_build_dlq`` leaves the sink None
        # when the config disables the DLQ — there is no producer to
        # connect and nothing to name in the topology log.
        app._build_dlq()
        if app._dlq_sink is not None:
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
            dlq_topic=app._dlq_sink.topic if app._dlq_sink is not None else '',
        )

    async def _run_on_ready_and_periodics(self) -> None:
        """Run the handler's ``on_ready`` hook and start its periodic tasks.

        Runs after the sinks connect and before any source starts, so the
        hook sees a fully wired worker that is not yet taking input.
        """
        app = self._app
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

    async def _warn_ignored_source_config(self) -> None:
        """A disabled source block with invalid values is ignored, not fatal — say so once.

        Pydantic validates a source block only when it is enabled, so a typo
        in a block someone switched off passes startup silently and then
        surprises whoever switches it back on. Naming it here costs nothing
        and keeps ``enabled: false`` a safe edit.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        for name in ('kafka', 'http'):
            block = getattr(app._config.sources, name)
            if block.enabled:
                continue
            errors = block.validation_errors()
            if errors:
                await log.awarning(
                    'source_config_ignored',
                    category='lifecycle',
                    source=name,
                    errors=errors,
                )

    def _bind_sources(self) -> None:
        """Hand every source the one context it may read the worker through.

        Built once, after the sinks connect, so every collaborator a source
        needs is already live. The context is the only channel the lifecycle
        opens: a source reads the worker through it rather than reaching for
        app attributes. ``HttpSource`` is the exception — it holds the app so
        it can pass it to the webapp server, which the request path needs.
        """
        app = self._app
        assert app._executor_pool is not None
        ctx = SourceContext(
            config=app._config,
            handler=app._handler,
            executor_pool=app._executor_pool,
            sink_manager=app._sink_manager,
            dlq_sink=app._dlq_sink,
            recorder=app._recorder,
            throughput=app._throughput,
            worker_id=app._worker_id,
            cluster_name=app._cluster_name,
            on_collect=app._handle_collect,
            # A late-bound lambda, not the current value: readiness is
            # composed across every source and flips during shutdown.
            is_worker_ready=lambda: app.is_ready,
        )
        for source in app.sources:
            source.bind(ctx)
        app._sources_bound = True

    async def _start_sources(self) -> None:
        """Start every enabled source in table order. A failure is fatal.

        A source that cannot acquire its input — no consumer group, no
        bound socket — leaves the worker unable to do the job it was
        deployed for, so the exception propagates and ``_async_run``
        tears the worker down rather than running half a pipeline.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        await log.ainfo('sources_starting', category='lifecycle', sources=[s.name for s in app.sources])
        for source in app.sources:
            try:
                await source.start()
            except Exception as exc:
                await log.aerror(
                    'source_start_failed',
                    category='lifecycle',
                    source=source.name,
                    error=str(exc),
                    exc_type=type(exc).__name__,
                )
                raise
            await log.ainfo('source_started', category='lifecycle', source=source.name)

    async def _run_sources(self) -> None:
        """Run all sources until a signal or a fatal source error.

        ``FIRST_EXCEPTION`` returns as soon as one source's ``run`` raises,
        or when every source has returned because ``signal_stop`` was
        called. A raised error cancels the other sources and is re-raised
        so ``_async_run`` runs ``_shutdown`` and the worker exits non-zero
        — a source that died silently would leave the worker up, ready and
        processing nothing.
        """
        app = self._app
        tasks = [asyncio.create_task(source.run(), name=f'source:{source.name}') for source in app.sources]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            error = None if task.cancelled() else task.exception()
            if error is None:
                continue
            for other in pending:
                other.cancel()
            if pending:
                # Let the cancellations settle here rather than leaving
                # pending tasks for the loop to complain about at exit.
                await asyncio.gather(*pending, return_exceptions=True)
            raise error

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
            # File I/O on db_dir (often NFS) — keep it off the event loop.
            await asyncio.to_thread(self._watchdog.write)
        except OSError as exc:
            await logger.awarning(
                'watchdog_write_failed',
                category='watchdog',
                path=str(self._watchdog.path),
                error=str(exc),
            )
            self._watchdog = None

    def _handle_signal(self) -> None:
        """Handle shutdown signals.

        Signalling every source is what makes ``_run_sources`` return: each
        ``run`` loop watches its own stop flag, so without this the worker
        would sit in the poll loop until the orchestrator escalated to
        SIGKILL. In-flight work is untouched — the drain settles it.
        """
        logger.info('shutdown_signal_received', category='lifecycle')
        self._app._running = False
        for source in self._app.sources:
            source.signal_stop()

    async def _shutdown(self) -> None:
        """Graceful shutdown: stop taking input, drain and stop the sources, close everything else.

        Tolerates partial state: startup failures land here too, so every
        step guards against the subsystem it releases never having started.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        await log.ainfo('drakkar_shutting_down', category='lifecycle')

        # Flip readiness off IMMEDIATELY so a Kubernetes readiness probe
        # that fires between now and ``close_all`` fails — the pod is
        # taken out of the service endpoints before we start tearing down
        # sinks. Liveness (``/healthz``) stays responsive until the process
        # actually exits. ``DrakkarApp.is_ready`` reads this flag, and so
        # does the webapp's per-request gate.
        app._stopping = True

        # Then tell every source to stop accepting input. Idempotent: the
        # signal handler already did this when a signal started the
        # shutdown, but a startup failure or a dead source gets here
        # without one.
        for source in app.sources:
            source.signal_stop()

        # Snapshot the drain-phase observability gauges BEFORE doing any
        # drain work. We always call ``.set()`` (even with ``0``) so the
        # gauge reads as "this is the value at the moment shutdown began"
        # rather than "stale value from earlier in the run". See
        # ``drakkar.metrics`` for the metric docstrings.
        #
        # Uncommitted offsets exist only with a Kafka source; a worker
        # without one reports 0 rather than leaving the gauge stale.
        kafka = app.kafka_source
        uncommitted_offsets_at_stop.set(kafka.uncommitted_offsets() if kafka is not None else 0)

        # In-flight executor tasks: read the pool's running ``active_count``,
        # which is the same accessor used by ``ExecutorPool`` to drive the
        # ``drakkar_executor_pool_active`` gauge during normal operation.
        # The pool may be ``None`` if shutdown is invoked before startup
        # completed (defensive programming for tests / aborted boot).
        inflight_at_stop.set(app._executor_pool.active_count if app._executor_pool is not None else 0)

        # cancel periodic tasks
        for task in app._periodic_tasks:
            task.cancel()
        if app._periodic_tasks:
            await asyncio.gather(*app._periodic_tasks, return_exceptions=True)
            app._periodic_tasks.clear()

        # A startup failure before ``_bind_sources`` leaves the sources with
        # no context and nothing acquired — no consumer, no bound socket —
        # so there is nothing to drain or stop.
        sources = app.sources if app._sources_bound else []

        drain_timeout = app._config.executor.drain_timeout_seconds
        deadline = time.monotonic() + drain_timeout

        # Everything from the watchdog mark to ``drakkar_stopped`` is teardown
        # that has to run whatever the drain phase does, so it sits in a
        # ``finally``. Two things can take that phase out: this ``_shutdown``
        # task being cancelled — an orchestrator whose grace period expired
        # mid-drain — and anything that is not an ``Exception`` escaping it.
        # Skipping the block below would leave the recorder, the cache, the
        # sinks and the consumer open, and a stale empty-body watchdog file
        # that the next startup reads as a SIGKILL. The error still propagates
        # once the teardown has run.
        try:
            await self._drain_sources(sources, deadline)
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
                    # File I/O on db_dir (often NFS) — keep it off the loop.
                    await asyncio.to_thread(self._watchdog.mark_clean)
                except OSError as exc:
                    await log.awarning(
                        'watchdog_mark_clean_failed',
                        category='watchdog',
                        error=str(exc),
                        exc_info=True,
                    )

            # The sources stop before the sinks and the DLQ. ``stop()``
            # finishes the final commits, stops the partition processors and
            # awaits the background rebalance work before it closes the
            # consumer, and the sinks only have to outlive the in-flight
            # deliveries the drain above settled.
            await self._stop_sources(deadline, sources)

            # Shut the offload pool down first among the observability-
            # adjacent subsystems: hooks have finished draining by now, so
            # anything still queued belongs to abandoned work — drop it
            # (cancel_futures) rather than crunch through it. A running
            # computation finishes in the background; shutdown(wait=False)
            # never blocks worker stop behind it.
            if app._offload_pool is not None:
                try:
                    app._offload_pool.shutdown()
                except Exception as exc:
                    await log.awarning(
                        'offload_pool_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                    )
                app._offload_pool = None

            # Stop the runtime-health monitor before the recorder for the
            # same reason as the cache engine below: its final transition
            # or stall event still needs a live recorder to land in.
            if app._throughput is not None:
                try:
                    await app._throughput.stop()
                except Exception as exc:
                    await log.awarning(
                        'throughput_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
            if app._runtime_health is not None:
                try:
                    await app._runtime_health.stop()
                except Exception as exc:
                    await log.awarning(
                        'runtime_health_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )
                app._runtime_health = None

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

            await log.ainfo('drakkar_stopped', category='lifecycle')
            close_logging()

    async def _drain_sources(self, sources: list[Source], deadline: float) -> None:
        """Wait for every source's in-flight work, all against one ``deadline``.

        The sources share the executor pool, so giving each its own budget
        would multiply the worst case by their number and overrun the pod's
        grace period. A source that fails its own drain is logged and does
        not stop the others from finishing theirs.
        """
        log = logger.bind(worker_id=self._app._worker_id)
        if not sources:
            return
        await log.ainfo(
            'sources_draining',
            category='lifecycle',
            timeout=round(max(deadline - time.monotonic(), 0.0), 3),
            sources=[source.name for source in sources],
        )
        results = await asyncio.gather(
            *(source.drain(deadline) for source in sources),
            return_exceptions=True,
        )
        for source, result in zip(sources, results, strict=True):
            if isinstance(result, BaseException):
                await log.aerror(
                    'drain_exception',
                    category='lifecycle',
                    source=source.name,
                    error=str(result),
                    exc_type=type(result).__name__,
                )
        if all(result is True for result in results):
            await log.ainfo('sources_drained', category='lifecycle')

    async def _stop_sources(self, deadline: float, sources: list[Source]) -> None:
        """Release every source's resources against the remaining deadline.

        One failing source must not keep the others (or the subsystems that
        still have to close after them) from being released, so each stop
        is wrapped on its own.
        """
        log = logger.bind(worker_id=self._app._worker_id)
        for source in sources:
            try:
                await source.stop(deadline)
            except Exception as exc:
                await log.awarning(
                    'source_stop_failed',
                    category='lifecycle',
                    source=source.name,
                    error=str(exc),
                    exc_info=True,
                )
