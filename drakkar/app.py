"""Main Drakkar application — wires all components together.

Orchestrates Kafka consumption, subprocess execution, sink delivery,
and offset management. Delivers results to the configured sinks.

The bulk of the runtime — startup orchestration, the poll loop,
partition rebalance callbacks, and graceful shutdown — lives in
:class:`drakkar.lifecycle.AppLifecycle`. ``DrakkarApp`` holds the
instance state (config, handler, processors, sinks, recorder…) and
exposes the public API; the lifecycle reads and mutates that state via
a back-reference. See :mod:`drakkar.lifecycle` for the rationale.
"""

import asyncio
import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from pydantic import BaseModel
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.appconfig import load_app_config
from drakkar.budget import PendingBudget
from drakkar.cache import CacheEngine
from drakkar.cluster_tasks import ClusterElection
from drakkar.config import BUILTIN_LINK_BASES, DrakkarConfig, SinkPolicy, load_config, own_storage_dir
from drakkar.config.identity import WORKER_NAME_FROM_ARGUMENT, check_worker_name, generated_worker_name
from drakkar.config.sinks import POLICY_KEYS, SINK_TYPES
from drakkar.consume_pause import ConsumePauseController
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.fleet.card import WorkerIdentity
from drakkar.fleet.service import FleetRegistry
from drakkar.handler import BaseDrakkarHandler
from drakkar.logging import setup_logging
from drakkar.models import CollectResult, DeliveryAction, DeliveryError, ErrorAction
from drakkar.partition import PartitionProcessor
from drakkar.periodic_roster import PeriodicRoster
from drakkar.probe import build_layout, referenced_bases
from drakkar.providers.connections import Connections
from drakkar.recorder import EventRecorder
from drakkar.recorder.helpers import detect_worker_ip
from drakkar.schedule import cluster_tasks_hash
from drakkar.sinks.dlq import DLQSink
from drakkar.sinks.filesystem import FileSink
from drakkar.sinks.http import HttpSink
from drakkar.sinks.kafka import KafkaSink
from drakkar.sinks.manager import DeliveryOutcome, MessageContext, SinkManager
from drakkar.sinks.mongo import MongoSink
from drakkar.sinks.postgres import PostgresSink
from drakkar.sinks.redis import RedisSink
from drakkar.sources.base import Source
from drakkar.sources.http import HttpSource
from drakkar.sources.kafka import KafkaSource
from drakkar.sources.registry import build_sources
from drakkar.timeline_events import referenced_link_bases
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


SINK_POLICY_REASON = "each sink's policy is resolved in DrakkarApp.__init__"

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
# starts being consumed early is a single row. The policy keys of each sink
# instance have names no table can list, so ``_sink_policy_settings`` adds them.
SETTINGS_CONSUMED_BEFORE_ON_STARTUP: tuple[ConsumedSetting, ...] = (
    ConsumedSetting(
        'app',
        'the handler app-config model is built from it in DrakkarApp.__init__ '
        '(set it in YAML or through the handler app_env_prefix env vars)',
    ),
    ConsumedSetting(
        'cluster.name',
        'the worker resolves its cluster name in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'cluster.name_env',
        'the worker resolves its cluster name in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'worker.name',
        'the worker id is resolved in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'worker.name_env',
        'the worker id is resolved in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'sources.kafka.enabled',
        'handler validation and source construction happen in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'sources.http.enabled',
        'handler validation and source construction happen in DrakkarApp.__init__',
    ),
    ConsumedSetting(
        'storage.dir',
        'the worker creates its own-files folder before on_startup runs',
    ),
    ConsumedSetting('sinks.defaults', SINK_POLICY_REASON),
)


def _read_path(config: DrakkarConfig, path: str) -> Any:
    """Read a dotted config path (``sinks.defaults``)."""
    value: Any = config
    for part in path.split('.'):
        value = getattr(value, part)
    return value


def _sink_policy_settings(config: DrakkarConfig) -> dict[str, Any]:
    """The policy keys of every sink instance, by config path. A list is copied, so it can be compared later."""
    settings: dict[str, Any] = {}
    for sink_type in SINK_TYPES:
        for name, instance in getattr(config.sinks, sink_type).items():
            for key in POLICY_KEYS:
                value = getattr(instance, key)
                settings[f'sinks.{sink_type}.{name}.{key}'] = tuple(value) if isinstance(value, list) else value
    return settings


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
    return {**snapshot, **_sink_policy_settings(config)}


def changed_consumed_settings(before: dict[str, Any], config: DrakkarConfig) -> list[str]:
    """Paths whose value differs from ``before`` — the ignored changes.

    Compared by value, so a handler that returns a freshly built
    ``DrakkarConfig`` rather than mutating the one it was given is judged
    on content, not identity. A sink the hook added or removed is no ignored
    change: a sink added there is built, and its policy comes from the config
    it was added to.
    """
    changed = [
        setting.path
        for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP
        if _read_path(config, setting.path) != before[setting.path]
    ]
    policy_settings = _sink_policy_settings(config)
    changed.extend(path for path, value in policy_settings.items() if path in before and before[path] != value)
    return changed


def reasons_for_settings(paths: list[str]) -> list[str]:
    """The ``path — reason`` lines the warning shows for ``paths``."""
    by_path = {setting.path: setting.reason for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP}
    return [f'{path} — {by_path.get(path, SINK_POLICY_REASON)}' for path in paths]


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
        # Fail fast when the handler cannot serve an enabled source: a
        # source without its hooks can never process input, so surface it
        # at construction rather than at the first message or request.
        from drakkar.sources.validation import validate_handler_for_sources

        validate_handler_for_sources(handler, self._config.sources)
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
            # BUILTIN_LINK_BASES is subtracted everywhere a template's bases
            # are checked: {docs} is resolved by the framework itself, so it
            # is never something ui.link_bases has to configure.
            missing_bases = sorted(referenced - set(self._config.ui.link_bases) - BUILTIN_LINK_BASES)
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
            # Same warn-don't-fail treatment for custom timeline event link
            # templates. Checked per type (unlike the probe-details case
            # above) because each type's link template is independent, so
            # naming which type is affected is more useful than one pooled
            # list.
            for event_type in self._config.ui.timeline.events:
                missing_event_bases = sorted(
                    referenced_link_bases(event_type.link) - set(self._config.ui.link_bases) - BUILTIN_LINK_BASES
                )
                if missing_event_bases:
                    logger.warning(
                        'timeline_event_link_bases_missing',
                        category='timeline',
                        event_type=event_type.name,
                        missing_bases=missing_event_bases,
                        message=(
                            f"timeline event '{event_type.name}' link template references ui.link_bases "
                            f'entries that are not configured: {", ".join(missing_event_bases)}; '
                            'link renders as plain text'
                        ),
                    )
            # Docs anchors name surfaces declared elsewhere in the
            # deployment; an anchor naming nothing renders nothing, so each
            # one is cross-checked here (warn, don't fail — same treatment
            # as the missing link bases above).
            self._warn_on_unresolved_docs_anchors()
        # Load the handler-declared application config (docs/app-config.md)
        # at construction — after load_config, before the lifecycle ever
        # calls on_startup — so a bad app: section or env override is a
        # fail-fast startup error and ``self.app_config`` is usable inside
        # every hook, on_startup included.
        self._wire_app_config()
        self._wire_executor_on_failure()
        if worker_id:
            configured_worker_id, name_source = worker_id, WORKER_NAME_FROM_ARGUMENT
        else:
            configured_worker_id, name_source = self._config.worker.resolved_name_and_source()
        if configured_worker_id:
            # Here and not at the config load: the name may come from a
            # variable or an argument that the load never reads.
            check_worker_name(configured_worker_id, source=name_source)
        self._worker_id = configured_worker_id or generated_worker_name()
        # A generated name can change with the host (a recreated pod); the own archive scope warns about it.
        self._worker_id_is_generated = not configured_worker_id
        self._cluster_name = self._config.cluster.resolved_name()
        self._start_time = time.monotonic()
        # Identity of this process in the worker registry: a new random
        # instance at every start, so two processes that share a worker name
        # (or a restart) are told apart.
        self._instance = secrets.token_hex(8)
        self._started_at = datetime.now(UTC)
        # starting -> running (sources started) -> draining (shutdown began);
        # published in the registry card and shown in the workers list.
        self._lifecycle_state = 'starting'
        # Registry service, started by the lifecycle after the API server
        # listens; None while the registry is off (autodiscover off).
        self._fleet: FleetRegistry | None = None
        # Named shared connections, made by the lifecycle before the first
        # provider starts and closed after the last one stops.
        self._connections: Connections | None = None
        # The election of the cluster periodic tasks, made by the lifecycle
        # when the worker runs at least one; it starts at the first slot and
        # stops after the recorder.
        self._cluster_election: ClusterElection | None = None
        # The handler's periodic tasks after the config overrides, built by
        # the lifecycle before the registry starts; None until then.
        self._periodic_roster: PeriodicRoster | None = None
        self._api_url: str | None = None

        self._executor_pool: ExecutorPool | None = None
        # Pending tasks and bytes of every source; built with the pool.
        self._pending_budget: PendingBudget | None = None
        self._sink_manager: SinkManager = SinkManager()
        # Each sink's policy, resolved once, here. The sinks are built later
        # (``_build_sinks``, after ``on_startup``); a change of a policy key in
        # the hook is reported as ignored (SETTINGS_CONSUMED_BEFORE_ON_STARTUP).
        self._sink_policies: dict[str, SinkPolicy] = self._config.sinks.policies()
        self._dlq_sink: DLQSink | None = None
        self._recorder: EventRecorder | None = None
        self._api_server = None
        # Runtime health monitor (event-loop lag + stall introspection) —
        # constructed in lifecycle._async_run when runtime_health.enabled.
        # Forward-declared as ``Any`` to keep drakkar.runtimehealth out of
        # the import graph at app construction time; the uiserver routes
        # read it for the /runtime/health snapshot.
        self._runtime_health: Any = None
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

        self._running = False
        # Operator-driven timed pause (Live page control; opt-in via
        # ui.consume_pause.enabled). Constructed unconditionally — it is a
        # tiny state holder; the routes gate on the config flag.
        self._consume_pause = ConsumePauseController(self)
        # Input sources, built now (not at startup) so their state — the
        # partition map, pause flags — exists from construction; the
        # lifecycle binds their context and starts them. See drakkar/sources.
        self.sources: list[Source] = build_sources(self)
        # Flipped in _shutdown so ``is_ready`` (and with it /readyz and the
        # webapp request gate) goes false the moment a drain starts, before
        # any source has been told to stop.
        self._stopping = False
        # Set once the lifecycle hands the sources their context. A startup
        # that failed earlier leaves them holding nothing, so shutdown skips
        # their drain and stop instead of driving an unbound source.
        self._sources_bound = False
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
        # the startup and shutdown helpers can reach them via
        # ``app._lifecycle._bind_sources()`` / ``_shutdown()`` without first
        # running the full startup sequence; the rebalance helpers moved to
        # ``KafkaSource`` (``app.kafka_source.on_assign(...)``). The
        # lifecycle is a thin back-reference holder — see
        # :mod:`drakkar.lifecycle`.
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
    def kafka_source(self) -> KafkaSource | None:
        """The Kafka input source, or None when ``sources.kafka`` is disabled."""
        return next((s for s in self.sources if isinstance(s, KafkaSource)), None)

    @property
    def http_source(self) -> HttpSource | None:
        """The HTTP input source, or None when ``sources.http`` is disabled."""
        return next((s for s in self.sources if isinstance(s, HttpSource)), None)

    # Delegating views for the UI routes, the consume-pause controller and
    # tests. Empty or None when the Kafka source is not configured.
    @property
    def _processors(self) -> dict[int, PartitionProcessor]:
        src = self.kafka_source
        return src.processors if src is not None else {}

    @property
    def _paused(self) -> bool:
        src = self.kafka_source
        return src.paused if src is not None else False

    @_paused.setter
    def _paused(self, value: bool) -> None:
        src = self.kafka_source
        if src is not None:
            src.paused = value

    @property
    def _stalled_partitions(self) -> set[int]:
        src = self.kafka_source
        return src.stalled_partitions if src is not None else set()

    @property
    def _consumer(self) -> KafkaConsumer | None:
        src = self.kafka_source
        return src.consumer if src is not None else None

    @property
    def _background_tasks(self) -> set[asyncio.Task]:
        src = self.kafka_source
        return src.background_tasks if src is not None else set()

    @property
    def _webapp(self) -> Any:
        src = self.http_source
        return src.webapp if src is not None else None

    @property
    def is_ready(self) -> bool:
        """Readiness for /readyz: not stopping, and every source is ready."""
        return not self._stopping and all(s.is_ready for s in self.sources)

    @property
    def recorder(self) -> EventRecorder | None:
        return self._recorder

    @property
    def fleet(self) -> FleetRegistry | None:
        """The worker registry service, or None while the registry is off."""
        return self._fleet

    @property
    def lifecycle_state(self) -> str:
        """``starting``, ``running`` or ``draining``."""
        return self._lifecycle_state

    @property
    def api_url(self) -> str:
        """Where peers and browsers reach this worker's HTTP server.

        ``api.public_url`` when set, else ``http://<outbound ip>:<api.port>/``.
        Detected once and kept: the outbound address does not change while
        the process runs.
        """
        if self._api_url is None:
            api = self._config.api
            self._api_url = api.public_url or f'http://{detect_worker_ip()}:{api.port}/'
        return self._api_url

    @property
    def own_storage_dir(self) -> str:
        """The folder of this worker's own files, ``<storage.dir>/<sanitized cluster>/<worker>``; ``''`` without one."""
        return own_storage_dir(self._config.storage.dir, self._cluster_name, self._worker_id)

    @property
    def worker_identity(self) -> WorkerIdentity:
        """The fields of this worker's registry card that do not change while it runs."""
        from drakkar.uiserver.server_helpers import BACKEND_NAME, backend_version

        return WorkerIdentity(
            worker=self._worker_id,
            instance=self._instance,
            cluster=self._cluster_name,
            url=self.api_url,
            backend=BACKEND_NAME,
            version=backend_version(),
            started_at=self._started_at,
        )

    @property
    def cluster_tasks_hash(self) -> str:
        """The hash of this worker's cluster task definitions; the hash of none before the roster is built."""
        if self._periodic_roster is None:
            return cluster_tasks_hash(())
        return self._periodic_roster.cluster_hash

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
            consumer_group=self._config.resolved_consumer_group,
            version=__version__,
            cluster_name=self._cluster_name,
        )
        asyncio.run(self._lifecycle._async_run())

    def _warn_on_unresolved_docs_anchors(self) -> None:
        """Warn once per docs anchor whose sink/event/page selector names nothing.

        An anchor attaches to a surface declared elsewhere in the deployment
        — a sink instance, a timeline event type, a declared UI page — and a
        selector matching none of them simply renders no anchor, which stays
        invisible until an operator goes looking for the link. Label anchors
        are deliberately unchecked: label keys come from runtime task labels,
        not from config, so there is no declared set to check them against.
        """
        anchors = self._config.ui.docs.anchors
        if not anchors:
            return
        sink_names = {name for section in SINK_TYPES for name in getattr(self._config.sinks, section)}
        # One row per checkable selector: the match field, the warning it
        # raises, the declared names it must be one of, what such a name is,
        # and where the deployment declares one.
        anchor_checks = (
            ('sink', 'docs_anchor_unknown_sink', sink_names, 'configured sink instance', 'sinks.*'),
            (
                'event',
                'docs_anchor_unknown_event',
                {event_type.name for event_type in self._config.ui.timeline.events},
                'declared timeline event type',
                'ui.timeline.events',
            ),
            (
                'page',
                'docs_anchor_unknown_page',
                {page.slug for page in self._ui_pages},
                'declared UI page slug',
                "the handler's ui_pages",
            ),
        )
        for anchor in anchors:
            for selector, warning_event, declared_names, noun, source in anchor_checks:
                target = getattr(anchor.match, selector)
                if not target or target in declared_names:
                    continue
                logger.warning(
                    warning_event,
                    category='docs',
                    selector=selector,
                    target=target,
                    message=(
                        f"docs anchor for {selector} '{target}' names no {noun}; the anchor never "
                        f'renders — fix the name in ui.docs.anchors or declare it in {source}'
                    ),
                )

    def _wire_executor_on_failure(self) -> None:
        """Give the handler's default ``on_error`` the answer ``executor.on_failure`` names.

        Called at construction and again after ``on_startup``, which may
        return a config with another value.
        """
        self._handler._executor_on_failure = ErrorAction(self._config.executor.on_failure)

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

        Sinks (kafka, postgres, mongo, http, redis, filesystem) are
        constructed directly — their config models are typed and well-known.
        Each sink gets the connection it names, and owns its client or pool.
        A delivery reads the manager's copy of the policy, never the config
        section.
        """
        sinks = self._config.sinks
        for name, cfg in sinks.kafka.items():
            self._sink_manager.register(
                KafkaSink(
                    name,
                    cfg,
                    self._config.declared_connection(f'sinks.kafka.{name}.connection', 'kafka', cfg.connection),
                ),
                policy=self._sink_policy('kafka', name),
            )

        for name, cfg in sinks.postgres.items():
            self._sink_manager.register(
                PostgresSink(
                    name,
                    cfg,
                    self._config.declared_connection(f'sinks.postgres.{name}.connection', 'postgres', cfg.connection),
                ),
                policy=self._sink_policy('postgres', name),
            )

        for name, cfg in sinks.mongo.items():
            self._sink_manager.register(
                MongoSink(
                    name,
                    cfg,
                    self._config.declared_connection(f'sinks.mongo.{name}.connection', 'mongo', cfg.connection),
                ),
                policy=self._sink_policy('mongo', name),
            )

        for name, cfg in sinks.http.items():
            self._sink_manager.register(HttpSink(name, cfg), policy=self._sink_policy('http', name))

        for name, cfg in sinks.redis.items():
            self._sink_manager.register(
                RedisSink(
                    name,
                    cfg,
                    self._config.declared_connection(f'sinks.redis.{name}.connection', 'redis', cfg.connection),
                ),
                policy=self._sink_policy('redis', name),
            )

        for name, cfg in sinks.filesystem.items():
            self._sink_manager.register(FileSink(name, cfg), policy=self._sink_policy('filesystem', name))

    def _sink_policy(self, sink_type: str, name: str) -> SinkPolicy:
        """The policy resolved at construction; a sink ``on_startup`` added resolves against the config it was added to."""
        policy = self._sink_policies.get(f'{sink_type}/{name}')
        return policy if policy is not None else self._config.sinks.policy_of(sink_type, name)

    def _build_dlq(self) -> None:
        """Create the DLQ sink, or leave it None when the config disables the DLQ (see DrakkarConfig.dlq_enabled)."""
        connection = self._config.dlq_connection
        if connection is None:
            self._dlq_sink = None
            return
        self._dlq_sink = DLQSink(
            topic=self._config.resolved_dlq_topic,
            brokers=connection.brokers,
            security=connection.security,
            client_config=connection.client_config_with(self._config.dlq.client_config),
            flush_timeout_seconds=self._config.dlq.flush_timeout,
            worker=self._worker_id,
            instance=self._instance,
        )

    def _get_worker_state(self) -> dict:
        """Return current worker state for the recorder's state sync."""
        src = self.kafka_source
        # A worker with no Kafka source has no partitions, nothing queued
        # and nothing to pause — the recorder still gets the full row shape.
        kafka_state: dict[str, Any] = (
            src.snapshot()
            if src is not None
            else {'assigned_partitions': [], 'partition_count': 0, 'paused': False, 'total_queued': 0}
        )
        state = {
            'uptime_seconds': time.monotonic() - self._start_time,
            'pool_active': self._executor_pool.active_count if self._executor_pool else 0,
            'pool_max': self._executor_pool.max_executors if self._executor_pool else 0,
            **kafka_state,
        }
        if self._runtime_health is not None:
            # Contract v1.15: merged fleet databases can answer "which
            # worker was degraded when" without replaying events. NULL
            # (absent here) means the monitor is off.
            health_state, loop_lag_ms = self._runtime_health.brief_state()
            state['health_state'] = health_state
            state['loop_lag_ms'] = loop_lag_ms
        return state

    async def _handle_collect(
        self,
        result: CollectResult,
        partition_id: int,
        message_context: MessageContext | None = None,
    ) -> None:
        """Deliver CollectResult payloads to configured sinks.

        Validates that all payload sink types are configured, then delivers
        via SinkManager, which asks the handler's on_delivery_error hook
        after a failed attempt and writes the DLQ record when a delivery
        ends in ``dlq``. ``message_context`` names the source messages of a
        task or message result, or is ``MessageContext.window()`` for a
        window result. ``None`` means no context (fail closed): a ``dlq``
        outcome then follows ``dlq.on_send_failure``.
        """
        if not result.has_outputs:
            return

        self._sink_manager.validate_collect(result)

        async def _on_delivery_error(error: DeliveryError) -> DeliveryAction:
            bind_contextvars(hook='on_delivery_error', sink_type=error.sink_type, sink_name=error.sink_name)
            try:
                return await self._handler.on_delivery_error(error)
            finally:
                unbind_contextvars('hook', 'sink_type', 'sink_name')

        outcomes = await self._sink_manager.deliver_all(
            result,
            on_delivery_error=_on_delivery_error,
            partition_id=partition_id,
            message_context=message_context,
        )

        if self._recorder:
            for payload in result.kafka:
                # Only a group that was delivered produced anything; a group
                # that failed, was skipped or never started did not.
                kafka_sink = self._sink_manager.resolve_sink('kafka', payload.sink)
                if outcomes.get((kafka_sink.sink_type, kafka_sink.name)) == DeliveryOutcome.DELIVERED:
                    self._recorder.record_produced(payload, source_partition=partition_id)
