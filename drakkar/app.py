"""Main Drakkar application — wires all components together.

Orchestrates Kafka consumption, subprocess execution, sink delivery,
and offset management. Uses the pluggable sink system for output.

The bulk of the runtime — startup orchestration, the poll loop,
partition rebalance callbacks, and graceful shutdown — lives in
:class:`drakkar.lifecycle.AppLifecycle`. ``DrakkarApp`` holds the
instance state (config, handler, processors, sinks, recorder…) and
exposes the public API; the lifecycle reads and mutates that state via
a back-reference. See :mod:`drakkar.lifecycle` for the rationale.
"""

import asyncio
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import structlog
from pydantic import BaseModel
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.appconfig import load_app_config
from drakkar.cache import CacheEngine
from drakkar.config import DrakkarConfig, load_config
from drakkar.consume_pause import ConsumePauseController
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.kafka_security import resolve_client
from drakkar.logging import setup_logging
from drakkar.metrics import dlq_dropped_payloads
from drakkar.models import CollectResult, DeliveryAction, DeliveryError, SinkDeliveryFailedError
from drakkar.partition import PartitionProcessor
from drakkar.probe import build_layout, referenced_bases
from drakkar.recorder import EventRecorder
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq import DLQSink
from drakkar.sinks.filesystem import FileSink
from drakkar.sinks.http import HttpSink
from drakkar.sinks.kafka import KafkaSink
from drakkar.sinks.manager import SinkManager
from drakkar.sinks.mongo import MongoSink
from drakkar.sinks.postgres import PostgresSink
from drakkar.sinks.redis import RedisSink
from drakkar.uipages import UIPage, build_pages, pages_referenced_bases

logger = structlog.get_logger()


# Re-exported for backward compatibility — moved into
# :mod:`drakkar.app_security` so the warning helper can be exercised in
# isolation without importing the full ``DrakkarApp`` machinery. Test
# imports of the form ``from drakkar.app import warn_if_ui_unauthenticated``
# continue to work via this re-export.
from drakkar.app_security import warn_if_ui_unauthenticated as warn_if_ui_unauthenticated  # noqa: E402


@dataclass(frozen=True)
class ConsumedSetting:
    """One config setting that ``DrakkarApp.__init__`` reads for good.

    ``path`` is dotted from the config root; ``reason`` says what already
    consumed it, so the warning tells an operator where to set the value
    instead of just that their change was dropped.
    """

    path: str
    reason: str


# Settings ``DrakkarApp.__init__`` consumes into a component or a derived
# value. ``on_startup`` runs later (``AppLifecycle._async_run``), so a hook
# that changes one of these changes nothing — the object built from the old
# value is already there. Every other section of the config is read after
# the hook and a change to it lands.
#
# A table rather than checks scattered through ``__init__``: it is the
# documented contract (``docs/handler.md``), it is what the warning below
# iterates, and a test asserts every path still resolves — so a setting
# that stops being consumed early cannot quietly keep warning, and one that
# starts being consumed early is a single row.
SETTINGS_CONSUMED_BEFORE_ON_STARTUP: tuple[ConsumedSetting, ...] = (
    ConsumedSetting(
        'app',
        'the handler app-config model is built from it in DrakkarApp.__init__ '
        '(set it in YAML or through the handler app_env_prefix env vars)',
    ),
    ConsumedSetting(
        'sinks.circuit_breaker',
        'the sink manager is constructed with it in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'sinks.delivery_timeout_seconds',
        'the sink manager is constructed with it in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'cluster_name',
        'the worker resolves its cluster name in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'cluster_name_env',
        'the worker resolves its cluster name in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'worker_name_env',
        'the worker id is resolved in DrakkarApp.__init__',
    ),
)


def _read_path(config: DrakkarConfig, path: str) -> Any:
    """Read a dotted config path (``sinks.circuit_breaker``)."""
    value: Any = config
    for part in path.split('.'):
        value = getattr(value, part)
    return value


def snapshot_consumed_settings(config: DrakkarConfig) -> dict[str, Any]:
    """Copy the values of every early-consumed setting, for later comparison.

    Pydantic models are deep-copied: a handler that mutates the config in
    place (the documented style) would otherwise leave the "before" value
    pointing at the object it just changed, and every comparison would say
    "unchanged".
    """
    snapshot: dict[str, Any] = {}
    for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP:
        value = _read_path(config, setting.path)
        snapshot[setting.path] = value.model_copy(deep=True) if isinstance(value, BaseModel) else value
    return snapshot


def changed_consumed_settings(before: dict[str, Any], config: DrakkarConfig) -> list[str]:
    """Paths whose value differs from ``before`` — the ignored changes.

    Compared by value, so a handler that returns a freshly built
    ``DrakkarConfig`` rather than mutating the one it was given is judged
    on content, not identity.
    """
    return [
        setting.path
        for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP
        if _read_path(config, setting.path) != before[setting.path]
    ]


def reasons_for_settings(paths: list[str]) -> list[str]:
    """The ``path — reason`` lines the warning shows for ``paths``."""
    by_path = {setting.path: setting.reason for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP}
    return [f'{path} — {by_path[path]}' for path in paths]


class DrakkarApp:
    """Main application that orchestrates Kafka consumption, subprocess
    execution, and result delivery to configured sinks.
    """

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        config_path: str | Path | None = None,
        config: DrakkarConfig | None = None,
        worker_id: str = '',
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = load_config(config_path)

        self._handler = handler
        # Fail fast on a webapp/handler mismatch, mirroring the Go
        # backend's app.New check: a webapp without the HTTP hooks (or
        # the typed models they need) can never serve a request, so
        # surface it at construction rather than at the first POST. The
        # import is local because the webapp stack (FastAPI/uvicorn) is
        # deliberately loaded only when the webapp is enabled.
        if self._config.webapp.enabled:
            from drakkar.webapp.server import validate_webapp_handler

            validate_webapp_handler(handler)
        # Fail fast on an invalid probe-details model — same philosophy as
        # the webapp handler check above: code-owned mistakes surface at
        # boot, not at first probe. Read into a local so the type checker
        # narrows `type[BaseModel] | None` to `type[BaseModel]` below.
        probe_details_model = handler.probe_details_model
        layout = build_layout(probe_details_model) if probe_details_model is not None else None
        # Fail fast on invalid declared UI pages too — same philosophy,
        # and unconditional (not gated on a probe model being
        # registered): a page/widget declaration mistake is a deploy-time
        # bug regardless of whether the User-defined probe tab is in use.
        self._ui_pages: list[UIPage] = build_pages(getattr(handler, 'ui_pages', None))
        # Fail fast when a configured custom-renderers module does not
        # exist — same philosophy as the checks above (a deploy-time typo
        # should break at boot, not silently 404 the first cell that tries
        # to use it) and unconditional on ui.enabled for the same reason
        # build_layout/build_pages are. The router factory reads the
        # file's CONTENT later; this only confirms the path resolves.
        if self._config.ui.custom_renderers_path:
            renderers_path = Path(self._config.ui.custom_renderers_path)
            if not renderers_path.is_file():
                raise ValueError(f'ui.custom_renderers_path does not exist or is not a file: {renderers_path}')
        # Templates — both the probe-details layout and declared pages —
        # may reference a base (e.g. {jira}) that the deployment never
        # configured in ui.link_bases — not a startup error, since the UI
        # degrades gracefully to plain text, but worth flagging in one
        # warning so the gap doesn't go unnoticed until someone clicks a
        # dead link. Layout/page validation itself stays unconditional
        # (fail-fast even with the UI off); the warning is skipped when
        # ui.enabled is False, since no link is ever rendered for anyone
        # to click — same guard warn_if_ui_unauthenticated uses in
        # drakkar/app_security.py.
        if self._config.ui.enabled:
            referenced = pages_referenced_bases(self._ui_pages)
            if layout is not None:
                referenced = referenced | referenced_bases(layout)
            missing_bases = sorted(referenced - set(self._config.ui.link_bases))
            if missing_bases:
                logger.warning(
                    'probe_details_link_bases_missing',
                    category='probe',
                    missing_bases=missing_bases,
                    message=(
                        'templates reference ui.link_bases entries that are not '
                        f'configured: {", ".join(missing_bases)}; affected links render as plain text'
                    ),
                )
        # Load the handler-declared application config (docs/app-config.md)
        # at construction — after load_config, before the lifecycle ever
        # calls on_startup — so a bad app: section or env override is a
        # fail-fast startup error and ``self.app_config`` is usable inside
        # every hook, on_startup included.
        self._wire_app_config()
        self._worker_id = worker_id or os.environ.get(self._config.worker_name_env, '') or f'drakkar-{id(self):x}'
        self._cluster_name = ''
        if self._config.cluster_name_env:
            self._cluster_name = os.environ.get(self._config.cluster_name_env, '')
        if not self._cluster_name:
            self._cluster_name = self._config.cluster_name
        self._start_time = time.monotonic()

        self._executor_pool: ExecutorPool | None = None
        self._consumer: KafkaConsumer | None = None
        # SinkManager receives the circuit breaker default so the breaker
        # installed on each registered sink honors operator thresholds.
        self._sink_manager: SinkManager = SinkManager(
            circuit_breaker_config=self._config.sinks.circuit_breaker,
            delivery_timeout_seconds=self._config.sinks.delivery_timeout_seconds,
        )
        self._dlq_sink: DLQSink | None = None
        self._recorder: EventRecorder | None = None
        self._ui_server = None
        # Runtime health monitor (event-loop lag + stall introspection) —
        # constructed in lifecycle._async_run when runtime_health.enabled.
        # Forward-declared as ``Any`` for the same import-cost reason as
        # ``_webapp`` below; the uiserver routes read it for the
        # /runtime/health snapshot.
        self._runtime_health: Any = None
        # Throughput tracker (contract v1.16) — constructed by the
        # lifecycle when throughput.cost_label is set; None otherwise.
        # Partition processors feed it, worker_state snapshots read it.
        self._throughput: Any = None
        # Webapp HTTP server — constructed in lifecycle._async_run when
        # webapp.enabled=true. ``None`` otherwise (the lifecycle never
        # touches the field on the disabled path). Held here so the
        # shutdown sequence can signal/stop it alongside the debug
        # server. Forward-declared as ``Any`` to avoid pulling
        # ``drakkar.webapp.server`` into the import graph at app
        # construction time (the webapp depends on FastAPI; users who
        # never enable it shouldn't pay the import cost).
        self._webapp: Any = None
        # Framework cache — constructed in lifecycle._async_run when
        # cache.enabled=true, else the handler keeps its default NoOpCache
        # stub. Held here so _shutdown can stop the engine in the correct
        # order (before recorder, so the final flush's periodic_run event
        # still records).
        self._cache_engine: CacheEngine | None = None
        # Offload thread pool backing handler.offload() — constructed in
        # lifecycle._async_run (always; it is cheap and has no external
        # dependencies), shut down in _shutdown. Forward-declared as
        # ``Any`` to keep drakkar.offload out of the import graph at app
        # construction time, mirroring ``_runtime_health``.
        self._offload_pool: Any = None

        self._processors: dict[int, PartitionProcessor] = {}
        self._running = False
        self._paused = False
        # Partitions paused because an offset stalled under
        # dlq.on_send_failure=stall. Excluded from backpressure resume;
        # cleared on revoke so a reassignment starts fresh.
        self._stalled_partitions: set[int] = set()
        # Operator-driven timed pause (Live page control; opt-in via
        # ui.consume_pause.enabled). Constructed unconditionally — it is a
        # tiny state holder; the routes gate on the config flag.
        self._consume_pause = ConsumePauseController(self)
        # Readiness signal for the ``/readyz`` Kubernetes probe (exposed
        # via the debug server). Flipped to ``True`` after the worker has
        # cleared its full startup sequence — consumer subscribed, sinks
        # connected, first poll cycle completed — and back to ``False``
        # during ``_shutdown`` so a draining pod fails its readiness
        # probe and is taken out of rotation immediately.
        self.is_ready: bool = False
        self._background_tasks: set[asyncio.Task] = set()
        self._periodic_tasks: list[asyncio.Task] = []
        self._config_summary: str = ''
        # Main event loop — captured at the top of lifecycle._async_run.
        # The debug FastAPI server runs in a separate thread with its own
        # event loop, but the ExecutorPool's asyncio.Semaphore is bound to
        # this loop. The Message Probe endpoint uses this ref to dispatch
        # runner.run() back here via asyncio.run_coroutine_threadsafe so
        # acquires don't fail with "bound to a different event loop" on a
        # contended pool.
        self._loop: asyncio.AbstractEventLoop | None = None

        # Internal lifecycle driver. Created eagerly so tests that exercise
        # the rebalance / shutdown helpers (``_on_assign``, ``_shutdown``,
        # …) can reach them via ``app._lifecycle._on_assign(...)`` without
        # first running the full startup sequence. The lifecycle is a thin
        # back-reference holder — see :mod:`drakkar.lifecycle`.
        # Imported lazily here to avoid a circular import (lifecycle
        # imports ``DrakkarApp`` only under ``TYPE_CHECKING``).
        from drakkar.lifecycle import AppLifecycle

        self._lifecycle = AppLifecycle(self)

    @property
    def config(self) -> DrakkarConfig:
        return self._config

    @property
    def consume_pause(self) -> ConsumePauseController:
        """The operator-driven timed pause controller (Live page control)."""
        return self._consume_pause

    @property
    def handler(self) -> BaseDrakkarHandler:
        """Return the user-supplied handler instance.

        Exposed so the debug server can introspect the handler (e.g. to
        detect which completion hooks are implemented) without reaching
        into private state. Read-only.
        """
        return self._handler

    @property
    def ui_pages(self) -> list[UIPage]:
        """Return the validated declared UI pages, wire form.

        Empty list when the handler declares none. Read by the debug/API
        server for ``GET /api/v1/pages``.
        """
        return self._ui_pages

    @property
    def processors(self) -> dict[int, PartitionProcessor]:
        return self._processors

    @property
    def recorder(self) -> EventRecorder | None:
        return self._recorder

    @property
    def sink_manager(self) -> SinkManager:
        return self._sink_manager

    @property
    def cache_engine(self) -> CacheEngine | None:
        """Return the framework-managed CacheEngine when cache.enabled=True.

        The debug UI reads this to decide whether to render the Cache nav
        link and the /debug/cache page. None when the cache is disabled —
        debug server routes should gracefully 404 in that state rather
        than attempting to query a non-existent reader connection.
        """
        return self._cache_engine

    @property
    def config_summary(self) -> str:
        return self._config_summary

    @property
    def main_loop(self) -> asyncio.AbstractEventLoop | None:
        """Return the event loop the pipeline runs on, or None before start.

        Exposed so the debug server (which runs on a separate thread + loop)
        can dispatch the Message Probe back to this loop — the ExecutorPool's
        semaphore is bound here and cannot be acquired from another loop
        once it has contention.
        """
        return self._loop

    def run(self) -> None:
        """Start the application. Blocks until shutdown.

        Constructs the lifecycle driver and hands control to it via
        :meth:`asyncio.run`. The lifecycle owns the event-loop-bound
        machinery so this method stays focused on logging setup and the
        public entry point.
        """
        setup_logging(
            self._config.logging,
            worker_id=self._worker_id,
            consumer_group=self._config.kafka.consumer_group,
            version=__version__,
            cluster_name=self._cluster_name,
        )
        asyncio.run(self._lifecycle._async_run())

    def _wire_app_config(self) -> None:
        """Load the handler-declared application config and attach it.

        When the handler declares ``app_config_model``, build the instance
        from the already-parsed ``config.app`` section plus the handler's
        own env prefix (``app_env_prefix``) and attach it so
        ``self.app_config`` is set before any hook runs. A validation
        failure propagates and is startup-fatal — ``load_app_config``'s
        error names the section and the model.

        When the handler declares NO model but the YAML carries a
        non-empty ``app:`` section, warn loudly: a typo'd setup (section
        present, declaration forgotten) must not be silent.
        """
        model = getattr(self._handler, 'app_config_model', None)
        if model is None:
            if self._config.app:
                logger.warning(
                    'app_config_ignored',
                    category='lifecycle',
                    keys=sorted(self._config.app),
                    message=(
                        'drakkar.yaml carries a non-empty app: section but the handler '
                        'declares no app_config_model — the section is ignored'
                    ),
                )
            return
        env_prefix = getattr(self._handler, 'app_env_prefix', 'APP_')
        # Wrap the section content back under its key: load_app_config
        # takes a whole-file-shaped dict (mirroring load_config), and
        # ``config.app`` is the section body, not the file.
        app_config = load_app_config(model, yaml_data={'app': self._config.app}, env_prefix=env_prefix)
        self._handler._app_config = app_config
        # Field COUNT only — values may include credentials and never
        # belong in a log line.
        logger.info(
            'app_config_loaded',
            category='lifecycle',
            model=model.__name__,
            env_prefix=env_prefix,
            field_count=len(model.model_fields),
        )

    def _build_sinks(self) -> None:
        """Create sink instances from config and register with SinkManager.

        Built-in sinks (kafka, postgres, mongo, http, redis, filesystem)
        are constructed directly — their config models are typed and
        well-known. Plugin-discovered sinks declared under
        ``sinks.custom.<type>.<instance>`` go through
        ``SinkRegistry.get(type)`` which is populated by
        ``SinkManager.discover()`` during the manager's construction
        (see :class:`drakkar.sinks.registry.SinkRegistry`). Unknown type
        names raise loudly so a typo or a missing plugin install
        surfaces at startup rather than silently dropping data.
        """
        from drakkar.sinks.registry import SinkRegistry

        kafka_brokers = self._config.kafka.brokers

        for name, cfg in self._config.sinks.kafka.items():
            self._sink_manager.register(
                KafkaSink(
                    name,
                    cfg,
                    brokers_fallback=kafka_brokers,
                    security_fallback=self._config.kafka.security,
                    client_config_fallback=self._config.kafka.client_config,
                )
            )

        for name, cfg in self._config.sinks.postgres.items():
            self._sink_manager.register(PostgresSink(name, cfg))

        for name, cfg in self._config.sinks.mongo.items():
            self._sink_manager.register(MongoSink(name, cfg))

        for name, cfg in self._config.sinks.http.items():
            self._sink_manager.register(HttpSink(name, cfg))

        for name, cfg in self._config.sinks.redis.items():
            self._sink_manager.register(RedisSink(name, cfg))

        for name, cfg in self._config.sinks.filesystem.items():
            self._sink_manager.register(FileSink(name, cfg))

        # Plugin-discovered sinks. Each top-level key in ``sinks.custom``
        # is a sink type name (matches the entry-point key in the plugin's
        # pyproject.toml); the registry resolves it to the plugin class.
        # Instances are constructed via ``cls(name, instance_cfg)`` —
        # plugin authors are expected to accept the same ``(name, config)``
        # signature the built-in sinks use.
        for sink_type, instances in self._config.sinks.custom.items():
            sink_cls = SinkRegistry.get(sink_type)
            if sink_cls is None:
                raise ValueError(
                    f'Unknown sink type {sink_type!r} declared under sinks.custom — '
                    f'no class registered under that name. '
                    f'Known types: {SinkRegistry.all_names()!r}. '
                    f'Make sure the plugin is installed and exposes the right '
                    f'[project.entry-points."drakkar.sinks"] entry.'
                )
            # ``BaseSink.__init__`` declares ``(name, ui_url='')`` for the
            # built-in sinks, but plugin authors widen the second parameter
            # to a config dict / Pydantic model. ty cannot see across the
            # entry-point boundary, so we cast ``sink_cls`` to a callable
            # taking two ``Any`` positionals before invoking it. The plugin
            # contract documented in docs/sinks.md is the source of truth
            # for the constructor signature, not the local type-check view.
            sink_factory = cast(Callable[[str, Any], BaseSink[Any]], sink_cls)
            for instance_name, instance_cfg in instances.items():
                self._sink_manager.register(sink_factory(instance_name, instance_cfg))

    def _build_dlq(self) -> None:
        """Create the DLQ sink from config."""
        dlq_topic = self._config.dlq.topic or f'{self._config.kafka.source_topic}_dlq'
        resolved = resolve_client(
            self._config.dlq.brokers,
            self._config.dlq.security,
            self._config.dlq.client_config,
            fallback_brokers=self._config.kafka.brokers,
            fallback_security=self._config.kafka.security,
            fallback_client_config=self._config.kafka.client_config,
        )
        self._dlq_sink = DLQSink(
            topic=dlq_topic,
            brokers=resolved.brokers,
            security=resolved.security,
            client_config=resolved.client_config,
            flush_timeout_seconds=self._config.dlq.flush_timeout_seconds,
        )

    def _total_queued(self) -> int:
        """Total messages buffered across all partition queues + in-flight tasks."""
        return sum(p.queue_size + p.inflight_count for p in self._processors.values())

    def _total_waiting(self) -> int:
        """Messages waiting in partition queues, not yet dispatched to executors."""
        return sum(p.queue_size for p in self._processors.values())

    def _get_worker_state(self) -> dict:
        """Return current worker state for the recorder's state sync."""
        state = {
            'uptime_seconds': time.monotonic() - self._start_time,
            'assigned_partitions': sorted(self._processors.keys()),
            'partition_count': len(self._processors),
            'pool_active': self._executor_pool.active_count if self._executor_pool else 0,
            'pool_max': self._executor_pool.max_executors if self._executor_pool else 0,
            'total_queued': self._total_queued(),
            'paused': self._paused,
        }
        if self._runtime_health is not None:
            # Contract v1.15: merged fleet databases can answer "which
            # worker was degraded when" without replaying events. NULL
            # (absent here) means the monitor is off.
            health_state, loop_lag_ms = self._runtime_health.brief_state()
            state['health_state'] = health_state
            state['loop_lag_ms'] = loop_lag_ms
        if self._throughput is not None:
            # Contract v1.16: the five-window aggregate as JSON — the
            # worker_state time series then replays throughput history.
            state['throughput'] = self._throughput.window_stats()
        return state

    async def _handle_dlq_failure(self, error: 'DeliveryError', partition_id: int, reason: str) -> None:
        """Apply the ``dlq.on_send_failure`` strategy when the DLQ fallback failed.

        'drop' (default): log CRITICAL, tick ``dlq_dropped_payloads``, and
        return — the delivery counts as handled, the offset commits, and the
        payloads are lost. 'stall': raise :class:`SinkDeliveryFailedError`
        so the partition pipeline leaves the affected offsets uncommitted
        and pauses the partition (redelivery after restart/rebalance).
        """
        if self._config.dlq.on_send_failure == 'stall':
            raise SinkDeliveryFailedError(
                sink_name=error.sink_name,
                sink_type=error.sink_type,
                reason=reason,
            )
        dlq_dropped_payloads.labels(partition=str(partition_id)).inc()
        await logger.acritical(
            'dlq_failure_payloads_dropped',
            category='sink',
            sink_name=error.sink_name,
            sink_type=error.sink_type,
            partition=partition_id,
            payload_count=len(error.payloads),
            reason=reason,
            action='ALERT: payloads lost (dlq.on_send_failure=drop) — '
            'set dlq.on_send_failure=stall to prefer replay over loss',
        )

    async def _handle_collect(self, result: CollectResult, partition_id: int) -> None:
        """Deliver CollectResult payloads to configured sinks.

        Validates that all payload sink types are configured, then delivers
        via SinkManager. On delivery error, calls the handler's
        on_delivery_error hook and handles DLQ/RETRY/SKIP.
        """
        if not result.has_outputs:
            return

        self._sink_manager.validate_collect(result)

        async def _on_delivery_error(error: 'DeliveryError') -> DeliveryAction:
            bind_contextvars(hook='on_delivery_error', sink_type=error.sink_type, sink_name=error.sink_name)
            try:
                action = await self._handler.on_delivery_error(error)
            finally:
                unbind_contextvars('hook', 'sink_type', 'sink_name')
            if action == DeliveryAction.DLQ:
                # DLQ is the last resort. If it is missing or the write
                # fails, the payloads have nowhere safe to go — apply the
                # dlq.on_send_failure strategy.
                if self._dlq_sink is None:
                    await self._handle_dlq_failure(
                        error,
                        partition_id=partition_id,
                        reason='handler returned DLQ but no DLQ sink is configured',
                    )
                elif not await self._dlq_sink.send(error, partition_id=partition_id):
                    await self._handle_dlq_failure(
                        error,
                        partition_id=partition_id,
                        reason='DLQ send failed',
                    )
            return action

        await self._sink_manager.deliver_all(
            result,
            on_delivery_error=_on_delivery_error,
            partition_id=partition_id,
        )

        if self._recorder:
            for payload in result.kafka:
                self._recorder.record_produced(payload, source_partition=partition_id)

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        if self._consumer:
            await self._consumer.commit({partition_id: offset})
        if self._recorder:
            self._recorder.record_committed(partition_id, offset)
