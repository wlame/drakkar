"""The agent routes: ``/api/v1/agent`` and the routes under it.

They explain the running worker to an AI agent and only read. They live on the
API app, so they need only ``api.enabled`` and answer with ``ui.enabled: false``
too. Every route checks, in this order: ``agent.enabled`` (404), the agent's own
bearer token (401), then its parameters (400). Every answer is compact UTF-8
JSON written by :func:`drakkar.agent.registry.dumps` (the Markdown manual
excepted) and carries ``Cache-Control: no-store``.

None of these routes reads the pipeline's live state. The registry files and
the config metadata are read on a worker thread at the first request, and so is
the metric collection; the answers are built on the API server's own loop.
"""

from __future__ import annotations

import asyncio
import hmac
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from starlette.responses import Response

from drakkar.agent.config_view import (
    APP_SECTION,
    AppConfigView,
    ConfigLeaf,
    ConfigSources,
    config_answer,
    entries_by_part,
)
from drakkar.agent.metrics_view import MetricNotes, MetricsQuery, metrics_answer
from drakkar.agent.params import (
    AGENT_OFF_MESSAGE,
    CONFIG_PARTS,
    MANUAL_PARTS,
    NO_SUCH_TOPIC_MESSAGE,
    START_HERE,
    STATE_PARTS,
    TOKEN_REQUIRED_MESSAGE,
    ParameterError,
    parse_detail,
    parse_format,
    parse_parts,
)
from drakkar.agent.registry import (
    Registry,
    TopicNotFoundError,
    approx_tokens,
    dumps,
    load_registry,
    read_config_metadata,
    select_manual,
    selection_json,
    selection_markdown,
)
from drakkar.app import SETTINGS_CONSUMED_BEFORE_ON_STARTUP
from drakkar.config import _parse_env_overrides
from drakkar.config.origins import leaf_paths
from drakkar.config.sinks import POLICY_KEYS, SINK_TYPES
from drakkar.configmeta import _to_jsonable, dump_by_yaml_key, walk_model_fields
from drakkar.metrics import discover_handler_metrics
from drakkar.uiserver.server_helpers import (
    BACKEND_NAME,
    backend_version,
    capabilities_of,
    prometheus_label_filters,
)

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

AGENT_INDEX_PATH = '/api/v1/agent'
AGENT_MANUAL_PATH = '/api/v1/agent/manual'
AGENT_CONFIG_PATH = '/api/v1/agent/config'
AGENT_METRICS_PATH = '/api/v1/agent/metrics'
AGENT_STATE_PATH = '/api/v1/agent/state'
AGENT_DIAGNOSE_PATH = '/api/v1/agent/diagnose'

INDEX_SCHEMA = 1
# The approx_tokens key of a route that has no parts.
WHOLE_ANSWER_KEY = 'all'

JSON_MEDIA_TYPE = 'application/json'
MARKDOWN_MEDIA_TYPE = 'text/markdown; charset=utf-8'
NO_STORE_HEADERS = {'Cache-Control': 'no-store'}
BEARER_SCHEME = 'bearer'

# The index rows, in the contract order: path and the parts the route takes.
INDEX_ROUTES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (AGENT_MANUAL_PATH, MANUAL_PARTS),
    (AGENT_CONFIG_PATH, CONFIG_PARTS),
    (AGENT_METRICS_PATH, ()),
    (AGENT_STATE_PATH, STATE_PARTS),
    (AGENT_DIAGNOSE_PATH, ()),
)


def json_answer(value: Any, status_code: int = 200) -> Response:
    """A JSON answer with the bytes of :func:`dumps` and ``no-store``."""
    return Response(
        content=dumps(value).encode('utf-8'),
        status_code=status_code,
        media_type=JSON_MEDIA_TYPE,
        headers=NO_STORE_HEADERS,
    )


def error_answer(message: str, status_code: int) -> Response:
    """An error in the ``{"error": ...}`` envelope."""
    return json_answer({'error': message}, status_code=status_code)


def token_matches(header: str, token: str) -> bool:
    """Compare the bearer token of an ``Authorization`` header with ``token``, in constant time.

    The scheme name ``Bearer`` is matched in any letter case.
    """
    scheme, _, given = header.partition(' ')
    if scheme.lower() != BEARER_SCHEME:
        return False
    return hmac.compare_digest(given.strip().encode('utf-8'), token.encode('utf-8'))


def consumed_paths(before_on_startup: Mapping[str, Any] | None) -> frozenset[str]:
    """The dotted config paths the worker read before ``on_startup`` ran; empty before the hook ran."""
    if before_on_startup is None:
        return frozenset()
    sinks = before_on_startup.get('sinks', {})
    policy = {
        f'sinks.{sink_type}.{name}.{key}'
        for sink_type in SINK_TYPES
        for name in sinks.get(sink_type, {})
        for key in POLICY_KEYS
    }
    return frozenset({setting.path for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP} | policy)


class AgentSurface:
    """What the agent routes of one API app share: the registry and the size estimates.

    The registry, the config metadata and the manual sizes are loaded on a worker
    thread at the first request (:meth:`ensure_loaded`), so a worker with the
    surface off never reads them and the API server's loop never waits on the
    files. ``live_tokens`` holds the latest size estimate of each live part, by
    route path. The manual sizes are computed again after the handler's manual
    entries change.
    """

    def __init__(self, drakkar_app: DrakkarApp) -> None:
        self._app = drakkar_app
        self._app_entries: tuple[dict[str, Any], ...] = ()
        self.app_config_meanings: dict[str, str] = {}
        self.metric_notes = MetricNotes()
        self.live_tokens: dict[str, dict[str, int]] = {
            AGENT_METRICS_PATH: {WHOLE_ANSWER_KEY: 0},
            AGENT_STATE_PATH: dict.fromkeys(STATE_PARTS, 0),
            AGENT_DIAGNOSE_PATH: {WHOLE_ANSWER_KEY: 0},
        }
        self._registry: Registry | None = None
        self._config_leaves: tuple[ConfigLeaf, ...] | None = None
        self._manual_tokens: dict[str, int] | None = None
        self._load_lock: asyncio.Lock | None = None

    @property
    def app_entries(self) -> tuple[dict[str, Any], ...]:
        """The handler's manual entries (the ``app`` part)."""
        return self._app_entries

    @app_entries.setter
    def app_entries(self, entries: Sequence[dict[str, Any]]) -> None:
        self._app_entries = tuple(entries)
        self._manual_tokens = None

    @property
    def is_loaded(self) -> bool:
        """True when the registry, the metadata and the manual sizes are ready."""
        return self._registry is not None and self._config_leaves is not None and self._manual_tokens is not None

    def load(self) -> None:
        """Read the registry and the config metadata and compute the manual sizes; blocking, for a worker thread."""
        if self._registry is None:
            self._registry = load_registry()
        if self._config_leaves is None:
            self._config_leaves = tuple(
                ConfigLeaf(
                    path=row['path'],
                    description=row['description'],
                    type=row['type'],
                    default=row['default'],
                    secret=row['secret'],
                )
                for row in read_config_metadata()
            )
        if self._manual_tokens is None:
            entries = self._app_entries
            tokens = self._compute_manual_tokens(self._registry, entries)
            # Entries that changed meanwhile leave the sizes to the next load.
            if entries is self._app_entries:
                self._manual_tokens = tokens

    async def ensure_loaded(self) -> None:
        """Run :meth:`load` on a worker thread unless everything is ready; one load at a time."""
        if self.is_loaded:
            return
        if self._load_lock is None:
            self._load_lock = asyncio.Lock()
        async with self._load_lock:
            if not self.is_loaded:
                await asyncio.to_thread(self.load)

    @property
    def registry(self) -> Registry:
        """The shipped registry; :meth:`ensure_loaded` must have run."""
        assert self._registry is not None, 'AgentSurface used before ensure_loaded'
        return self._registry

    @property
    def config_leaves(self) -> tuple[ConfigLeaf, ...]:
        """The leaves of the shipped config metadata; :meth:`ensure_loaded` must have run."""
        assert self._config_leaves is not None, 'AgentSurface used before ensure_loaded'
        return self._config_leaves

    @property
    def manual_tokens(self) -> dict[str, int]:
        """The size estimate of each manual part's brief answer; :meth:`ensure_loaded` must have run."""
        assert self._manual_tokens is not None, 'AgentSurface used before ensure_loaded'
        return self._manual_tokens

    def _compute_manual_tokens(self, registry: Registry, entries: Sequence[dict[str, Any]]) -> dict[str, int]:
        filters = self.promql_filters()
        return {
            part: approx_tokens(
                dumps(selection_json(select_manual(registry, parts=[part], app_entries=entries), filters))
            )
            for part in MANUAL_PARTS
        }

    def promql_filters(self) -> dict[str, str]:
        """The label filters that fill the ``{{worker}}`` and ``{{cluster}}`` placeholders."""
        app = self._app
        config = app._config
        return prometheus_label_filters(
            config.ui.prometheus,
            metrics_port=config.metrics.port,
            api_port=config.api.port,
            worker_id=app._worker_id,
            cluster_name=app._cluster_name or '',
        )

    # --- the answers -------------------------------------------------------

    def index(self) -> dict[str, Any]:
        """The index answer."""
        app = self._app
        config = app._config
        use = {entry['key']: entry['use'] for entry in self.registry.parts['routes']}
        sizes = {
            AGENT_MANUAL_PATH: self.manual_tokens,
            AGENT_CONFIG_PATH: self.config_tokens(),
            **self.live_tokens,
        }
        return {
            'schema': INDEX_SCHEMA,
            'framework': {
                'backend': BACKEND_NAME,
                'version': backend_version(),
                'registry_version': self.registry.version,
            },
            'worker': {
                'name': app._worker_id,
                'cluster': app._cluster_name or None,
                'instance': app._instance,
            },
            'capabilities': capabilities_of(config, app._recorder),
            'include_payloads': config.agent.include_payloads,
            'app_notes': bool(self.app_entries),
            'routes': [
                {
                    'path': path,
                    'parts': list(parts),
                    'approx_tokens': dict(sizes[path]),
                    'use': use[f'GET {path}'],
                }
                for path, parts in INDEX_ROUTES
            ],
            'start_here': START_HERE,
        }

    def manual(self, *, parts: Sequence[str], detail: str, topic: str | None, markdown: bool) -> str:
        """The manual answer text; raises ``TopicNotFoundError`` for a topic that names nothing."""
        selection = select_manual(
            self.registry,
            parts=parts,
            detail=detail,
            topic=topic,
            app_entries=self.app_entries,
        )
        filters = self.promql_filters()
        if markdown:
            return selection_markdown(selection, filters)
        return dumps(selection_json(selection, filters))

    def config(self, *, parts: Sequence[str], full: bool) -> dict[str, Any]:
        """The config answer for ``parts``."""
        app = self._app
        before = getattr(app, '_config_before_on_startup', None)
        return config_answer(
            leaves=self.config_leaves,
            config_dump=dump_by_yaml_key(app._config, mode='json'),
            sources=ConfigSources(
                origins=getattr(app, '_config_origins', None),
                before_on_startup=before,
                consumed=consumed_paths(before),
            ),
            app=self.app_config_view() if APP_SECTION in parts else AppConfigView(),
            parts=parts,
            registry=self.registry,
            full=full,
        )

    def config_tokens(self) -> dict[str, int]:
        """The size estimate of each config part's brief answer."""
        grouped = entries_by_part(self.config(parts=CONFIG_PARTS, full=False)['entries'])
        return {part: approx_tokens(dumps({'entries': grouped.get(part, [])})) for part in CONFIG_PARTS}

    def app_config_view(self) -> AppConfigView:
        """The ``app`` section: the handler's app config model when it has one, else the raw section."""
        app = self._app
        handler = getattr(app, 'handler', None)
        instance = getattr(handler, 'app_config', None)
        yaml_section = app._config.app
        if not isinstance(instance, BaseModel):
            return AppConfigView(values=yaml_section, yaml_section=yaml_section, meanings=self.app_config_meanings)
        prefix = getattr(handler, 'app_env_prefix', 'APP_')
        leaves = tuple(
            ConfigLeaf(
                path=meta.path,
                description=meta.description,
                type=meta.type,
                default=meta.default,
                secret=meta.secret,
            )
            for meta in walk_model_fields(type(instance), path_prefix=[APP_SECTION], group=APP_SECTION)
        )
        return AppConfigView(
            leaves=leaves,
            values=_to_jsonable(dump_by_yaml_key(instance, mode='python')),
            yaml_section=yaml_section,
            env_paths=leaf_paths(_parse_env_overrides(prefix, '__', skip_config_key=False)),
            meanings=self.app_config_meanings,
        )

    def handler_metric_families(self) -> frozenset[str]:
        """The family names of the metrics the handler declares."""
        handler = getattr(self._app, 'handler', None)
        if handler is None:
            return frozenset()
        return frozenset(metric._name for metric in discover_handler_metrics(handler).values())

    async def metrics(self, *, query: MetricsQuery, full: bool) -> dict[str, Any]:
        """The metrics answer; the registry is collected on a worker thread."""
        from prometheus_client.registry import REGISTRY

        families = await asyncio.to_thread(lambda: list(REGISTRY.collect()))
        return metrics_answer(
            families=families,
            registry=self.registry,
            handler_families=self.handler_metric_families(),
            query=query,
            full=full,
            promql_filters=self.promql_filters(),
            notes=self.metric_notes,
        )


def _gate(drakkar_app: DrakkarApp, request: Request) -> Response | None:
    """The answer of the switch and token checks, or ``None`` when the request may go on."""
    agent = drakkar_app._config.agent
    if not agent.enabled:
        return error_answer(AGENT_OFF_MESSAGE, 404)
    if agent.auth_token and not token_matches(request.headers.get('authorization', ''), agent.auth_token):
        return error_answer(TOKEN_REQUIRED_MESSAGE, 401)
    return None


def _split_names(raw: str | None) -> list[str] | None:
    return None if raw is None else raw.split(',')


def create_agent_router(drakkar_app: DrakkarApp, surface: AgentSurface | None = None) -> APIRouter:
    """The router of the agent routes; every worker with an API mounts it, UI on or off."""
    surface = surface if surface is not None else AgentSurface(drakkar_app)
    router = APIRouter()

    def guarded(build: Callable[[Request], Any]) -> Callable[[Request], Any]:
        """Run the gate, then ``build``; a ``ParameterError`` answers 400."""

        async def endpoint(request: Request) -> Response:
            refused = _gate(drakkar_app, request)
            if refused is not None:
                return refused
            await surface.ensure_loaded()
            try:
                return await build(request)
            except ParameterError as exc:
                return error_answer(str(exc), 400)

        endpoint.__doc__ = build.__doc__
        return endpoint

    async def index(request: Request) -> Response:
        """The index: the routes, their parts and the size of each part."""
        return json_answer(surface.index())

    async def manual(request: Request) -> Response:
        """The knowledge registry, by part or by topic, as JSON or Markdown."""
        params = request.query_params
        parts = parse_parts(params.get('parts'), MANUAL_PARTS)
        detail = parse_detail(params.get('detail'))
        markdown = parse_format(params.get('format')) == 'markdown'
        topic = params.get('topic')
        try:
            text = surface.manual(parts=parts, detail=detail, topic=topic, markdown=markdown)
        except TopicNotFoundError:
            return error_answer(NO_SUCH_TOPIC_MESSAGE.format(ref=topic), 404)
        media_type = MARKDOWN_MEDIA_TYPE if markdown else JSON_MEDIA_TYPE
        return Response(content=text.encode('utf-8'), media_type=media_type, headers=NO_STORE_HEADERS)

    async def config(request: Request) -> Response:
        """The effective config, with origins and masked secrets."""
        params = request.query_params
        parts = parse_parts(params.get('parts'), CONFIG_PARTS)
        full = parse_detail(params.get('detail')) == 'full'
        return json_answer(surface.config(parts=parts, full=full))

    async def metrics(request: Request) -> Response:
        """The current samples of the framework and handler metrics."""
        params = request.query_params
        full = parse_detail(params.get('detail')) == 'full'
        query = MetricsQuery(names=_split_names(params.get('names')), prefix=params.get('prefix'))
        text = dumps(await surface.metrics(query=query, full=full))
        if not query.is_filtered and not full:
            surface.live_tokens[AGENT_METRICS_PATH] = {WHOLE_ANSWER_KEY: approx_tokens(text)}
        return Response(content=text.encode('utf-8'), media_type=JSON_MEDIA_TYPE, headers=NO_STORE_HEADERS)

    router.add_api_route(AGENT_INDEX_PATH, guarded(index), methods=['GET'])
    router.add_api_route(AGENT_MANUAL_PATH, guarded(manual), methods=['GET'])
    router.add_api_route(AGENT_CONFIG_PATH, guarded(config), methods=['GET'])
    router.add_api_route(AGENT_METRICS_PATH, guarded(metrics), methods=['GET'])
    return router
