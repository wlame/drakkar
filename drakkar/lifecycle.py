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
import inspect
import os
import signal
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.agent.notes import AGENT_NOTES_ASYNC_MESSAGE, AgentNotes, handler_metrics_of, prepare_notes
from drakkar.agent.registry import load_registry
from drakkar.annotations import Annotator
from drakkar.app import (
    changed_consumed_settings,
    reasons_for_settings,
    snapshot_consumed_settings,
)
from drakkar.app_security import warn_if_agent_unauthenticated, warn_if_ui_unauthenticated
from drakkar.budget import PendingBudget
from drakkar.cache import Cache, CacheEngine
from drakkar.cluster_tasks import (
    ARCHIVE_SCOPE_CLUSTER,
    ARCHIVE_SCOPE_WORKER,
    ClusterElection,
    ClusterHashChecker,
    ClusterTask,
    SlotContext,
    archive_scope,
    run_cluster_task,
)
from drakkar.config.autodiscover import FILESYSTEM_PROVIDER
from drakkar.config.recorder import PERSISTENCE_POSTGRES
from drakkar.configmeta import dump_by_yaml_key
from drakkar.executor import ExecutorPool
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.fleet.service import FleetRegistry
from drakkar.hostinfo import effective_cpu_count
from drakkar.kafka_security import describe_mixed_security
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
from drakkar.ownerlock import OwnerLock, open_own_folder, unsafe_folder_levels
from drakkar.periodic import STARTUP_NO_AUTODISCOVER, SYSTEM_CLOCK, Clock, run_own_system_task, run_periodic_task
from drakkar.periodic_roster import RosterTask, resolve_periodic_roster
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import ResilientCaller, describe_error
from drakkar.recorder import EventRecorder
from drakkar.recorder.archive import warn_if_archives_unbounded
from drakkar.recorder.election import (
    BUILTIN_ELECTIONS,
    ELECTION_FEATURE,
    ElectionContext,
)
from drakkar.recorder.history import history_dir
from drakkar.recorder.postgres import RecorderPostgresSettings
from drakkar.schedule import ARCHIVE_TASK_NAME, DEFAULT_CLUSTER
from drakkar.sinks.manager import SinkNotConfiguredError
from drakkar.sources.base import Source, SourceContext
from drakkar.timeline_events import TimelineEventEmitter
from drakkar.watchdog import WatchdogFile

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp
    from drakkar.config import AutodiscoverConfig, DrakkarConfig

logger = structlog.get_logger()


# Startup refuses a worker whose results go to sinks when it has none.
NO_SINKS_ERROR = (
    'No sinks configured, but results are delivered to sinks (the Kafka source is on, '
    'or sources.http.deliver_to_sinks is true). Add at least one sink to the sinks: section in config.'
)


def _connection_and_prune(autodiscover: AutodiscoverConfig) -> dict[str, Any]:
    """The registry options of a provider that keeps its cards in a shared database: connection and prune age."""
    return {'connection': autodiscover.connection, 'prune_after_seconds': autodiscover.prune_after}


def registry_provider_options(config: DrakkarConfig) -> dict[str, Any]:
    """The options mapping the registry side of the configured provider receives.

    Each provider takes its settings from its own config keys.
    """
    autodiscover = config.autodiscover
    builders = {
        FILESYSTEM_PROVIDER: lambda: {
            'dir': config.resolved_registry_dir,
            'prune_after_seconds': autodiscover.prune_after,
        },
        'postgres': lambda: _connection_and_prune(autodiscover),
        'redis': lambda: _connection_and_prune(autodiscover),
        # A consul card lives as long as its session, whose TTL comes from the offline limit; it is never pruned.
        'consul': lambda: {'connection': autodiscover.connection, 'offline_after_seconds': autodiscover.offline_after},
    }
    return builders[autodiscover.provider]()


def election_provider_options(config: DrakkarConfig) -> dict[str, Any]:
    """The options mapping the election side of the configured provider receives.

    The filesystem election finds its folder in the start context; the others
    keep their lock in the service of the named connection.
    """
    autodiscover = config.autodiscover
    return {} if autodiscover.provider == FILESYSTEM_PROVIDER else {'connection': autodiscover.connection}


def recorder_postgres_settings(config: DrakkarConfig) -> RecorderPostgresSettings | None:
    """The table settings of the ``postgres`` recorder mode, or ``None`` when the effective mode is another.

    The DSN and the schema come from the connection entry that
    ``recorder.postgres.connection`` names; an undeclared one fails with the
    message of the config load.
    """
    if config.resolved_persistence != PERSISTENCE_POSTGRES:
        return None
    section = config.recorder.postgres
    return RecorderPostgresSettings(
        connection_name=section.connection,
        connection=config.declared_connection('recorder.postgres.connection', PERSISTENCE_POSTGRES, section.connection),
        table_prefix=section.table_prefix,
        retention_hours=section.initial_retention,
        store_completed_output=section.store_completed_output,
        max_output=section.max_output,
        max_size=section.max_size,
        batch_rows=section.batch_rows,
        queue_max_bytes=section.max_queue,
        resilience=section.resilience,
    )


def user_cluster_task(roster_task: RosterTask) -> ClusterTask:
    """The slot loop's view of a handler's cluster task: a won slot calls the bound method.

    The schedule and the lease come from the roster's definition of the task.

    Raises:
        ValueError: ``roster_task`` is a worker task (it has no definition).
    """
    method = roster_task.method
    definition = roster_task.definition
    if definition is None:
        raise ValueError(f"periodic task '{roster_task.name}' is not a cluster task")

    async def run(_ctx: SlotContext) -> None:
        await method()

    return ClusterTask(
        name=roster_task.name,
        run=run,
        interval=definition.interval,
        cron=definition.cron,
        lease_seconds=definition.lease_seconds,
        on_error=roster_task.meta.on_error,
    )


def watch_periodic_loop(task: asyncio.Task, name: str) -> None:
    """Log ``periodic_loop_ended`` when the loop of the periodic task ``name`` ends on an exception.

    Each run is guarded, so reaching this means something outside the run
    (the slot math, a skip, the lease setup) ended the loop. Nothing awaits
    the loop until shutdown, so without this line the task would stop
    running with no trace. A cancelled loop and a loop that returned (a
    failed run under ``on_error='stop'``, already logged) stay silent.
    """

    def on_done(finished: asyncio.Task) -> None:
        if finished.cancelled():
            return
        exc = finished.exception()
        if exc is None:
            return
        # Sync logger: done callbacks run on the loop and cannot await.
        logger.error('periodic_loop_ended', category='periodic', name=name, error=str(exc), exc_info=exc)

    task.add_done_callback(on_done)


# The hint of ``storage_folder_unsafe``: the own files hold task args and subprocess output.
STORAGE_FOLDER_UNSAFE_HINT = (
    'the own files hold task arguments and subprocess output, and another local user can replace '
    'what is in this folder; give storage.dir and the folders below it to the worker user only '
    '(for example /var/lib/drakkar)'
)


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
        # The owner lock of the own folder, taken by ``_prepare_storage`` and
        # dropped last in the shutdown; None in memory-only mode.
        self._owner_lock: OwnerLock | None = None
        # The clock of the cluster periodic tasks' slot loops; tests replace it
        # so no test waits for a real slot.
        self._clock: Clock = SYSTEM_CLOCK

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
        # The whole config before the hook, so the agent config route can name
        # the keys the hook changed. A dump, so a hook that changes the config
        # in place cannot change it too.
        app._config_before_on_startup = dump_by_yaml_key(app._config, mode='json')

        bind_contextvars(hook='on_startup')
        try:
            app._config = await app._handler.on_startup(app._config)
        finally:
            unbind_contextvars('hook')
        app._wire_executor_on_failure()

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
            await self._prepare_storage()
            await self._setup_watchdog()

            await self._run_on_startup()

            app._config_summary = app._config.config_summary(
                worker_id=app._worker_id,
                cluster_name=app._cluster_name,
            )
            await log.ainfo('drakkar_starting', category='lifecycle', config=app._config_summary)
            await self._report_kafka_security()

            # A source that delivers results to sinks needs at least one.
            if app._config.sinks.is_empty and app._config.sources.delivers_to_sinks:
                raise SinkNotConfiguredError(NO_SINKS_ERROR)
            self._check_config_after_startup()

            await self._build_executor_pool()
            await self._start_observability()
            self._create_connections()
            self._build_periodic_roster()
            await self._collect_agent_notes()
            await self._start_recorder_and_api()
            await self._start_fleet()
            self._start_runtime_health()
            self._wire_annotator()
            self._wire_io_executor()
            self._wire_offload_pool()
            await self._start_cache()
            await self._connect_sinks()
            await self._warn_ignored_source_config()
            self._bind_sources()
            await self._run_on_ready_and_periodics()
            await self._start_sources()
            self._set_lifecycle_state('running')
            # Files an earlier process with this worker name left behind
            # are sealed in the background; startup never waits on them.
            if app._recorder is not None:
                app._recorder.start_leftover_sealing()

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
        mistake: the consumer authenticates, but a sink or the DLQ names
        another connection with no security block and would connect in
        plaintext. A worker with no Kafka source has no consumer to report.
        """
        app = self._app
        config = app._config
        log = logger.bind(worker_id=app._worker_id)
        if config.kafka_source_connection is None:
            return
        consumer_security = config.kafka_source_connection.security

        await log.ainfo(
            'kafka_security',
            category='lifecycle',
            protocol=consumer_security.protocol,
            mechanism=consumer_security.sasl_mechanism or '',
            summary=consumer_security.describe(),
        )

        clients: list[tuple[str, str]] = [
            *((('dlq', config.dlq.connection),) if config.dlq_enabled else ()),
            *((f'sinks.kafka.{name}', cfg.connection) for name, cfg in config.sinks.kafka.items()),
        ]
        for label, connection_name in clients:
            # Only a client on another connection can diverge.
            if connection_name == config.sources.kafka.connection:
                continue
            message = describe_mixed_security(
                config.declared_connection(f'{label}.connection', 'kafka', connection_name).security, consumer_security
            )
            if message:
                await log.awarning(
                    'kafka_security_mismatch',
                    category='lifecycle',
                    client=label,
                    message=message,
                )

    async def _prepare_storage(self) -> None:
        """Create this worker's own folder and take its owner lock before anything writes into it.

        The own folder ``<storage.dir>/<sanitized cluster>/<worker>/`` holds
        this worker's own files (recorder, cache, crash marker), so each
        folder created here is owner-only (0700). An existing folder,
        typically a mounted volume, keeps the mode the operator gave it.
        Memory-only mode (``storage.dir: ""``) creates nothing and takes no lock.

        The owner lock is held until the shutdown ends, so a second live
        process with this worker name on this host cannot start. Download
        temps an earlier process left in the folder are deleted once the
        lock is held.

        Raises:
            FolderInUseError: another live process holds the folder.
            OSError: a folder cannot be created: every own file lives there,
                so the worker could not record or cache anything.
        """
        app = self._app
        storage_dir = app._config.storage.dir
        app._own_storage_dir = app.own_storage_dir
        if not storage_dir:
            return
        # File I/O — keep it off the loop.
        self._owner_lock = await asyncio.to_thread(
            open_own_folder,
            storage_dir=storage_dir,
            own_dir=app.own_storage_dir,
            worker=app._worker_id,
        )
        for folder, reason in await asyncio.to_thread(unsafe_folder_levels, storage_dir, app.own_storage_dir):
            await logger.awarning(
                'storage_folder_unsafe',
                category='lifecycle',
                folder=folder,
                reason=reason,
                hint=STORAGE_FOLDER_UNSAFE_HINT,
            )

    async def _release_owner_lock(self) -> None:
        """Drop the owner lock of the own folder, last in the shutdown; nothing to do when none was taken."""
        lock, self._owner_lock = self._owner_lock, None
        if lock is not None:
            await asyncio.to_thread(lock.release)

    async def _setup_watchdog(self) -> None:
        """Construct the watchdog file and check the previous run.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step; the body is unchanged. Must run first so ``check_previous()``
        reads the prior run's file, and must NOT call ``write()`` -- that stays
        in ``_claim_watchdog_slot()``, which runs after ``subscribe()``.
        """
        app = self._app
        log = logger.bind(worker_id=app._worker_id)
        # Watchdog file for OOM / SIGKILL detection. Lives in
        # ``storage.dir``, the folder for this worker's own files (the
        # recorder and the cache engine write there too). When it is empty —
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
        if app.own_storage_dir:
            watchdog_dir = Path(app.own_storage_dir)
            self._watchdog = WatchdogFile(data_dir=watchdog_dir, worker_id=app._worker_id)
            # Detect a possible SIGKILL from the prior run BEFORE we
            # claim the slot for this run. ``check_previous`` returns
            # True when no suspect-OOM signature was found; logging that
            # at info level lets operators confirm the watchdog ran
            # without grepping the warn-only suspect path.
            # File I/O in the own folder — keep it off the event loop.
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
                'watchdog_disabled_no_storage_dir',
                category='watchdog',
                reason='storage.dir is empty — OOM/SIGKILL detection disabled for this run',
            )

    async def _build_executor_pool(self) -> None:
        """Construct the subprocess pool and the pending budget from executor config.

        Extracted from ``_async_run`` so the boot sequence is testable step by
        step. The budget is built here because every source charges it: the
        partition loops wait on it before a window, the HTTP source only counts.
        """
        app = self._app
        app._executor_pool = ExecutorPool(
            binary_path=app._config.executor.binary_path,
            max_executors=app._config.executor.max_executors,
            task_timeout_seconds=app._config.executor.task_timeout,
            env=app._config.executor.env,
            inherit_parent_env=app._config.executor.env_inherit_parent,
            inherit_deny_patterns=app._config.executor.env_inherit_deny,
            # Wire the handler's priority hook into the pool's wait queue.
            # Handlers that don't override ``task_priority`` get the
            # framework default (smallest source offset → older messages
            # drain first). See ``BaseDrakkarHandler.task_priority`` for
            # the override contract.
            priority_fn=app._handler.task_priority,
            max_stdout_bytes=app._config.executor.max_stdout,
            max_stderr_bytes=app._config.executor.max_stderr,
        )
        app._pending_budget = PendingBudget(
            max_tasks=app._config.executor.max_pending_tasks,
            max_bytes=app._config.executor.max_pending_bytes,
        )
        # Start the exit watcher now rather than at the first task, so a host
        # that cannot watch says so at startup (a backend the kernel refused
        # has also logged its reason). Without a watcher, task run times are
        # measured on the event loop.
        exit_watch_backend = app._executor_pool.start_exit_watcher()
        logger.info(
            'exit_watcher_started' if exit_watch_backend else 'exit_watcher_off',
            category='executor',
            backend=exit_watch_backend,
            platform=sys.platform,
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

    async def _collect_agent_notes(self) -> None:
        """Ask the handler for its agent notes and check them, when the agent surface is on.

        The hook runs on the loop like the other handler hooks. The registry the
        notes are checked against is read on a worker thread, and the agent
        routes reuse it. A hook that raises leaves the worker without notes; a
        note that breaks a rule is dropped, the others stay.
        """
        app = self._app
        if not app._config.agent.enabled:
            return
        handler = app._handler
        try:
            notes = handler.agent_notes()
            if inspect.iscoroutine(notes):
                # Close it, so no "never awaited" warning follows the error line.
                notes.close()
                raise TypeError(AGENT_NOTES_ASYNC_MESSAGE)
            if notes is not None and not isinstance(notes, AgentNotes):
                raise TypeError(f'agent_notes returned {type(notes).__name__}, not AgentNotes or None')
        except Exception as exc:
            await logger.aerror(
                'agent_notes_failed',
                category='handler',
                error=str(exc),
                exc_type=type(exc).__name__,
                exc_info=True,
            )
            return
        if notes is None:
            return
        registry = await asyncio.to_thread(load_registry)
        prepared, reasons = prepare_notes(
            notes,
            registry=registry,
            handler_metrics=handler_metrics_of(discover_handler_metrics(handler).values()),
        )
        if reasons:
            await logger.awarning('agent_notes_invalid', category='agent', reasons=reasons)
        app._agent_registry = registry
        app._agent_notes = prepared

    async def _start_recorder_and_api(self) -> None:
        """Start the recorder and the API server, each when its config asks for it.

        The recorder runs with the UI or with durable recording; a memory store
        exists only to serve the UI. With the API off, the worker warns about its
        missing probes.
        """
        app = self._app
        config = app._config
        log = logger.bind(worker_id=app._worker_id)
        if config.ui.enabled:
            # Auth is opt-in. Emit a startup warning naming how to set a
            # token when none is configured — no endpoint touches the
            # pipeline and the UI is meant for private-network deployment,
            # so this is informational rather than fail-fast. The warning
            # also names any side-effecting endpoint (probe, merge) left
            # enabled. See docs/faq.md §"Security and trust model".
            warn_if_ui_unauthenticated(config)
        await self._start_recorder()
        if not config.api.enabled:
            # The API server is this worker's ONLY probe surface — without
            # it nothing serves /healthz or /readyz and Kubernetes probes
            # simply fail. Warn loudly, because that consequence is easy
            # to miss.
            await log.awarning(
                'ui_disabled_no_probes',
                category='lifecycle',
                reason='api.enabled=false — /healthz and /readyz are not served; Kubernetes probes need the API server',
            )
            return

        warn_if_agent_unauthenticated(config)

        from drakkar.uiserver.server import APIServer

        app._api_server = APIServer(
            config=config.api,
            app=app,
            ui=config.ui if config.ui.enabled else None,
            recorder=app._recorder,
        )
        await app._api_server.start()

    async def _start_recorder(self) -> None:
        """Build and start the recorder, unless the worker has neither a UI nor durable recording."""
        app = self._app
        config = app._config
        if not config.ui.enabled and not config.recorder.enabled:
            return
        # Archives are the one recorder artifact nothing else reclaims,
        # so "keep forever" is stated at startup rather than discovered
        # when a volume fills. Only a worker that runs archive passes
        # produces them.
        scope = archive_scope(config)
        if scope is not None:
            warn_if_archives_unbounded(
                config.recorder.archive,
                history_dir(config.resolved_shared_dir, app._cluster_name),
            )
        # The own scope finds its files by the worker name, so the files of an
        # earlier name are never archived or expired.
        if scope == ARCHIVE_SCOPE_WORKER and app._worker_id_is_generated:
            logger.warning('archive_own_scope_unstable_worker_name', category='recorder', worker=app._worker_id)
        memory = config.recorder.memory
        app._recorder = EventRecorder(
            config.recorder,
            worker_name=app._worker_id,
            cluster_name=app._cluster_name,
            persistence=config.resolved_persistence,
            memory_enabled=config.ui.enabled,
            api_port=config.api.port,
            api_public_url=config.api.public_url,
            # With the UI off no /ws route exists, so nothing can subscribe: a
            # zero threshold defers no ``task_started`` for a stream nobody reads.
            ws_min_duration_ms=round(config.ui.ws_min_duration * 1000) if config.ui.enabled else 0,
            log_min_duration_ms=round(config.logging.min_task_duration * 1000),
            storage_dir=app.own_storage_dir,
            shared_dir=config.resolved_shared_dir,
            instance=app._instance,
            memory_window_seconds=memory.window,
            memory_max_rows=config.memory_max_rows,
            memory_max_bytes=memory.max_size,
            postgres=recorder_postgres_settings(config),
            sibling_dir=os.path.dirname(app.own_storage_dir) if app.own_storage_dir else '',
            # The registry starts after the recorder, so it is looked up at each call.
            worker_online=self._worker_name_online if config.autodiscover.enabled else None,
        )
        app._recorder.set_state_provider(app._get_worker_state)
        await app._recorder.start()
        await app._recorder.write_config(config)

    def _worker_name_online(self, worker: str) -> bool | None:
        """Whether the registry lists ``worker`` as online; ``None`` while the registry has not started."""
        fleet = self._app._fleet
        return None if fleet is None else fleet.name_online(worker)

    def _check_config_after_startup(self) -> None:
        """Check the ``api``, recorder and ``autodiscover`` rules again, and say once when the cluster name is empty.

        ``on_startup`` may have changed the keys, so this gives the messages of
        the config load.

        Raises:
            ValueError: the UI, the recorder or ``autodiscover`` cannot run as configured.
        """
        app = self._app
        app._config.check_what_needs_the_api()
        app._config.check_recorder_provider()
        app._config.check_autodiscover()
        if app._config.autodiscover.enabled and not app._cluster_name:
            logger.warning(
                'cluster_name_empty',
                category='lifecycle',
                scope=DEFAULT_CLUSTER,
            )

    def _build_periodic_roster(self) -> None:
        """Resolve the handler's periodic tasks and make the election of its cluster tasks.

        Runs after ``on_startup`` and before any connection opens, the
        registry starts or a sink connects, so the registry card carries the
        definition hash from its first publish. The election serves every
        cluster task, ``history.archive`` included when the worker runs it as one.

        Raises:
            ValueError: a cluster task is declared while ``autodiscover`` is off.
        """
        app = self._app
        app._periodic_roster = resolve_periodic_roster(app._handler, app._config)
        app._cluster_election = self._build_cluster_election(app._periodic_roster.cluster_task_names)

    def _build_cluster_election(self, cluster_task_names: Sequence[str]) -> ClusterElection | None:
        """The election service of ``cluster_task_names``; ``None`` when the worker runs no cluster task.

        The provider is made here and starts at the first slot that needs it.

        Raises:
            ValueError: a cluster task with ``autodiscover`` off, naming the
                first task in sorted order.
        """
        if not cluster_task_names:
            return None
        app = self._app
        config = app._config
        autodiscover = config.autodiscover
        if not autodiscover.enabled:
            raise ValueError(STARTUP_NO_AUTODISCOVER.format(name=sorted(cluster_task_names)[0]))
        options = election_provider_options(config)
        context = ElectionContext(
            worker=app._worker_id,
            instance=app._instance,
            cluster=app._cluster_name,
            shared_dir=config.resolved_shared_dir,
            logger=logger,
            options=options,
            connections=app._connections,
        )
        caller = ResilientCaller(
            autodiscover.resilience,
            feature=ELECTION_FEATURE,
            provider=autodiscover.provider,
            category='periodic',
        )
        return ClusterElection(
            BUILTIN_ELECTIONS[autodiscover.provider](options),
            caller,
            provider_name=autodiscover.provider,
            context=context,
            clock=self._clock,
        )

    def _create_connections(self) -> None:
        """Make the resolver of the named shared connections; it opens nothing until a provider asks.

        Made after ``on_startup``, so a handler that changes ``connections``
        there is honoured, and before the recorder and the registry, whose
        providers use it.
        """
        app = self._app
        app._connections = Connections(app._config.connections)

    async def _start_fleet(self) -> None:
        """Start the worker registry when ``autodiscover`` is on, or leave ``app._fleet`` unset.

        Runs right after the API server listens: peers reach this worker
        through the URL its card publishes. Before the first publish the
        same-name check refuses the start when a live worker of another
        process publishes this worker's name; it waits at most two
        heartbeats plus 5 s, and a registry that is down only skips it.
        Then the service starts its loops in the background and returns at
        once; nothing later in startup, and nothing in the pipeline, waits on
        a registry call.

        Raises:
            WorkerNameInUseError: a live worker publishes this worker's name.
        """
        app = self._app
        config = app._config
        autodiscover = config.autodiscover
        if not autodiscover.enabled:
            return
        options = registry_provider_options(config)
        app._fleet = FleetRegistry(
            BUILTIN_PROVIDERS[autodiscover.provider](options),
            provider_name=autodiscover.provider,
            config=autodiscover,
            identity=app.worker_identity,
            provider_options=options,
            features=self._card_features,
            connections=app._connections,
            on_refresh=ClusterHashChecker(app.cluster_tasks_hash, app._cluster_name).check,
        )
        await app._fleet.check_name_free()
        await app._fleet.start()

    def _card_features(self) -> dict[str, Any]:
        """The card's ``features``: the cluster task hash, the UI switch, and the cache marks while the cache serves.

        Read at each publish: the cache engine starts after the registry.
        """
        app = self._app
        cache = app._cache_engine.features() if app._cache_engine is not None else {}
        return {'periodic': {'cluster_hash': app.cluster_tasks_hash}, 'ui': app._config.ui.enabled, **cache}

    def _set_lifecycle_state(self, state: str) -> None:
        """Record the worker's state and publish it in the registry card at once."""
        app = self._app
        app._lifecycle_state = state
        if app._fleet is not None:
            app._fleet.set_state(state)

    def _start_runtime_health(self) -> None:
        """Start the event-loop health monitor when enabled.

        Runs after the recorder so transitions/stalls persist as events;
        the recorder handle may still be ``None`` (no UI and no durable
        recording), in which case the monitor feeds only Prometheus and the in-memory
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

    def _wire_annotator(self) -> None:
        """Replace the handler's NoOpAnnotator stub with a live one, and wire timeline events on top of it.

        Called right after the recorder starts, since annotations are stored
        as recorder events and there is nowhere to put them otherwise. When
        the recorder is absent, annotations are switched off, or the events
        table is not being written, the class-level no-op stub stays in place
        and ``self.annotate(...)`` remains a cheap call the handler can make
        unconditionally.

        ``capture.events`` is part of the condition on purpose: with it false
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
        recorder_config = app._config.recorder
        annotations = recorder_config.annotations
        if app._recorder is None or not annotations.enabled or not recorder_config.capture.events:
            return
        annotator = Annotator(
            app._recorder,
            max_bytes=annotations.max_size,
            max_bytes_per_call=annotations.max_size_per_call,
            log_max_bytes=annotations.max_log_size,
        )
        app._handler._annotator = annotator

        event_types = app._config.ui.timeline.events
        if event_types:
            app._handler._timeline_events = TimelineEventEmitter(annotator, {t.name: t for t in event_types})

    def _wire_io_executor(self) -> None:
        """Resize asyncio's default to_thread executor when threads.io is set.

        Every ``asyncio.to_thread`` call on the main loop — handler blocking
        I/O plus the framework's own short file work — shares this one pool,
        which Python caps at ``min(32, cpu_count + 4)``. The API server's and
        the webapp's loops keep pools of their own. Installing a
        replacement via ``loop.set_default_executor`` lifts that cap;
        ``asyncio.run``'s cleanup (``shutdown_default_executor``) then owns
        its shutdown exactly as it owns the stock pool's, so lifecycle
        teardown needs no extra step. Null (the default) leaves Python's own
        sizing untouched.
        """
        app = self._app
        max_threads = app._config.threads.io
        if max_threads is None:
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
            app._config.threads.offload,
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
                storage_dir=app.own_storage_dir,
                worker_id=app._worker_id,
                cluster_name=app._cluster_name,
                recorder=app._recorder,
                peer_views=app._fleet.peer_views if app._fleet is not None else None,
            )
            # The handler-facing Cache: origin_worker_id is this worker's
            # id so LWW tiebreaks during peer-sync can identify our writes.
            # The config's 0 (no cap) is the cache's None.
            handler_cache = Cache(
                origin_worker_id=app._worker_id,
                max_memory_entries=app._config.cache.max_memory_entries or None,
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
            dlq_unconfigured=app._config.dlq_unconfigured,
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

        roster = app._periodic_roster
        if roster is not None:
            for roster_task in roster.worker_tasks:
                self._start_worker_task(roster_task)
            for roster_task in roster.cluster_tasks:
                self._start_cluster_task(user_cluster_task(roster_task))
        self._start_archive_task()

    def _start_worker_task(self, roster_task: RosterTask) -> None:
        """Start the loop of one worker-scope periodic task of the handler."""
        app = self._app
        meta = roster_task.meta
        task = asyncio.create_task(
            run_periodic_task(
                name=roster_task.name,
                coro_fn=roster_task.method,
                interval=meta.interval,
                cron=meta.cron,
                on_error=meta.on_error,
                recorder=app._recorder,
                clock=self._clock,
            ),
            name=f'periodic:{roster_task.name}',
        )
        watch_periodic_loop(task, roster_task.name)
        app._periodic_tasks.append(task)

    def _start_cluster_task(self, cluster_task: ClusterTask) -> None:
        """Start the slot loop of one cluster task; the roster made an election for every one."""
        app = self._app
        if app._cluster_election is None:
            return
        task = asyncio.create_task(
            run_cluster_task(
                cluster_task,
                app._cluster_election,
                cluster=app._cluster_name,
                recorder=app._recorder,
                clock=self._clock,
            ),
            name=f'periodic:{cluster_task.name}',
        )
        watch_periodic_loop(task, cluster_task.name)
        app._periodic_tasks.append(task)

    def _start_archive_task(self) -> None:
        """Start the slot loop of ``history.archive`` in the scope the roster chose, if it chose one."""
        app = self._app
        roster = app._periodic_roster
        if roster is None or app._recorder is None:
            return
        if roster.archive_scope == ARCHIVE_SCOPE_CLUSTER:
            self._start_cluster_task(app._recorder.archive_cluster_task())
        elif roster.archive_scope == ARCHIVE_SCOPE_WORKER:
            self._start_own_archive_task(app._recorder)

    def _start_own_archive_task(self, recorder: EventRecorder) -> None:
        """Start the slot loop of ``history.archive`` over this worker's own files, with no election."""
        app = self._app
        archive = app._config.recorder.archive
        task = asyncio.create_task(
            run_own_system_task(
                ARCHIVE_TASK_NAME,
                recorder.run_own_archive_pass,
                interval=archive.effective_interval,
                cron=archive.cron,
                recorder=recorder,
                clock=self._clock,
            ),
            name=f'periodic:{ARCHIVE_TASK_NAME}',
        )
        watch_periodic_loop(task, ARCHIVE_TASK_NAME)
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
            worker_id=app._worker_id,
            cluster_name=app._cluster_name,
            on_collect=app._handle_collect,
            # A late-bound lambda, not the current value: readiness is
            # composed across every source and flips during shutdown.
            is_worker_ready=lambda: app.is_ready,
            pending_budget=app._pending_budget,
            request_ids=app._request_ids,
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
            # File I/O in the own folder — keep it off the event loop.
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
        self._set_lifecycle_state('draining')

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

        drain_timeout = app._config.executor.drain_timeout
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
                    # File I/O in the own folder — keep it off the loop.
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

            # Every task has settled once the sources stopped, so the exit
            # watcher has nothing left to time. Its thread wakes and ends at
            # once; a task that still runs is timed on the loop. The stop
            # joins the thread (up to 1 s), so it runs off the loop.
            if app._executor_pool is not None:
                await asyncio.to_thread(app._executor_pool.stop_exit_watcher)

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
                    # The seal always runs; moving the file into the history
                    # folder gets only the shutdown time that is left. The
                    # wait for an archive pass in progress keeps the time of
                    # one registry call for the card withdraw below.
                    await app._recorder.stop(
                        timeout=max(deadline - time.monotonic(), 0.0),
                        reserve_seconds=self._registry_stop_reserve_seconds(),
                    )
                except Exception as exc:
                    await log.awarning(
                        'recorder_stop_failed',
                        category='lifecycle',
                        error=str(exc),
                        exc_info=True,
                    )

            # The election stops once the cluster task loops are cancelled (at
            # the top of the shutdown) and the archive pass that still checked
            # its lease has ended or been left: one attempt in the time left.
            # Its caller counts and logs a failed call; anything else it
            # raises is logged here, so the card withdraw and the closes below still run.
            if app._cluster_election is not None:
                try:
                    await app._cluster_election.stop(max(deadline - time.monotonic(), 0.0))
                except Exception as exc:
                    await log.awarning(
                        'periodic_election_stop_failed',
                        category='periodic',
                        error=str(exc),
                        exc_info=True,
                    )

            # Withdraw the registry card after the recorder's last write and
            # before the API server goes: until the card is gone, peers may
            # still reach this worker through it.
            if app._fleet is not None:
                try:
                    await app._fleet.stop(deadline_seconds=max(deadline - time.monotonic(), 0.0))
                except Exception as exc:
                    await log.awarning(
                        'fleet_registry_stop_failed',
                        category='fleet',
                        error=str(exc),
                        exc_info=True,
                    )

            # The shared connections close once every provider that uses them
            # has stopped, inside the shutdown time that is left. A closer
            # still running when that runs out is cut off, and its client
            # goes with the process. ``close_all`` logs a closer that raises;
            # an error of its own is logged here, so the closers below still run.
            if app._connections is not None:
                close_seconds = max(deadline - time.monotonic(), 0.0)
                try:
                    async with asyncio.timeout(close_seconds):
                        await app._connections.close_all()
                except TimeoutError:
                    await log.awarning(
                        'connections_close_timed_out',
                        category='fleet',
                        timeout_seconds=round(close_seconds, 3),
                    )
                except Exception as exc:
                    await log.awarning(
                        'connections_close_failed',
                        category='fleet',
                        error=describe_error(exc),
                        exc_info=True,
                    )

            if app._api_server:
                try:
                    await app._api_server.stop()
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

            # Last: the own folder stays locked until no file in it is written any more.
            await self._release_owner_lock()

            await log.ainfo('drakkar_stopped', category='lifecycle')
            close_logging()

    def _registry_stop_reserve_seconds(self) -> float:
        """The stop time the recorder keeps for the registry: one call's time limit while a registry runs, else 0."""
        app = self._app
        if app._fleet is None:
            return 0.0
        return app._config.autodiscover.resilience.timeout

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
