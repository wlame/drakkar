"""Debug web UI for Drakkar workers — FastAPI + Jinja2 templates.

The route handlers are split across four sibling modules to keep this
factory thin:

  * ``routes_pages``  — HTML pages, health probes, WebSocket, top-level
    JSON APIs (dashboard, sinks, workers, processors).
  * ``routes_live``   — ``/live`` page + ``/api/live/*`` + ``/api/recent-tasks``
    + ``/api/events``.
  * ``routes_debug``  — ``/debug`` page + ``/api/debug/*`` (databases,
    trace, metrics, periodic, probe, download).
  * ``routes_cache``  — ``/api/debug/cache/*`` endpoints.
  * ``routes_config_reference`` — ``/api/v1/config-reference`` (v1-only).

Each routes module defines a ``create_*_router(deps)`` factory that
returns an ``APIRouter`` and is mounted into the app via
``include_router``. The shared helpers (auth/origin checks, cross-thread
dispatch, prometheus-link builder, etc.) hang off ``UIDeps`` so the
routers can reach them through one parameter.

Helpers that are pure functions (no closure over ``drakkar_app`` or
``config``) live in :mod:`drakkar.uiserver.server_helpers`.
"""

from __future__ import annotations

import asyncio
import secrets
import threading
import time
from collections.abc import Coroutine, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import structlog
import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.routing import APIRoute
from fastapi.templating import Jinja2Templates

from drakkar.concurrency import dispatch_to_loop
from drakkar.config import UIConfig
from drakkar.recorder import EventRecorder
from drakkar.uihost import ResolvedBundle

# Re-import the helpers that used to live here so test imports and
# patches like ``drakkar.uiserver.server.hook_flags`` keep working
# without changes; ``noqa: F401`` keeps the re-exports visible without
# tripping ruff's unused-import check.
from drakkar.uiserver.server_helpers import (
    format_ts,
    format_ts_full,
    format_ts_ms,
    format_uptime,
    hook_flags,  # noqa: F401  (re-exported for tests)
    origin_allowed,  # noqa: F401  (re-exported for tests)
    worker_group,  # noqa: F401  (re-exported for tests)
)

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

# Cap on how long a UI request waits for the main loop to answer a
# live-state read. Bare ``dispatch_to_loop`` waits forever, so a wedged
# pipeline would hang every operator page that touches worker state —
# exactly the pages someone opens when the worker is misbehaving. Generous
# enough that a merely busy loop still answers, short enough that a browser
# tab never appears frozen.
MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS = 5.0

TEMPLATES_DIR = Path(__file__).parent.parent / 'templates'

# A degraded recorder read repeats on every poll of every open page, so the
# cause is logged at most once per interval per cause rather than per
# request — enough for an operator to see WHY the UI is empty without the
# log turning into the flood that made the previous silence tempting.
DEGRADED_READ_LOG_INTERVAL_SECONDS = 60.0
_degraded_read_last_logged: dict[str, float] = {}

# Distinguishes "the dispatch never produced a value" from a legitimate
# ``None`` returned by the read itself, which the reader-absent branch uses.
_DISPATCH_FAILED = object()


def _warn_degraded_read(event: str, hint: str) -> None:
    """Log a degraded-read cause, at most once per interval per cause."""
    now = time.monotonic()
    last = _degraded_read_last_logged.get(event)
    if last is not None and now - last < DEGRADED_READ_LOG_INTERVAL_SECONDS:
        return
    _degraded_read_last_logged[event] = now
    logger.warning(event, category='debug', hint=hint)


class UIDeps:
    """Shared dependencies + helpers passed to each route-module factory.

    Routes don't capture ``drakkar_app`` / ``recorder`` / ``config`` /
    ``templates`` directly; they reach them through this object. Pure
    helpers that need access to the live app/config (auth, kafka-UI URL
    builder, prometheus-link builder, cross-thread dispatch) are methods
    here so the router factories don't have to plumb them individually.
    """

    def __init__(
        self,
        config: UIConfig,
        recorder: EventRecorder,
        drakkar_app: DrakkarApp,
        templates: Jinja2Templates,
        ui_version: str | None = None,
        ui_source: str = 'builtin',
    ) -> None:
        self.config = config
        self.recorder = recorder
        self.drakkar_app = drakkar_app
        self.templates = templates
        # Identity v1.2: which drakkar-ui bundle this server actually
        # serves — 'release' + tag in SPA mode, 'builtin' + None when the
        # server-rendered fallback pages are active.
        self.ui_version = ui_version
        self.ui_source = ui_source

    # --- Sink-UI / Kafka-UI helpers (also wired into Jinja globals) ---

    def get_sink_ui_links(self) -> list[dict[str, str]]:
        """Return deduplicated sink UI links for the nav header."""
        mgr = self.drakkar_app.sink_manager
        if not mgr:
            return []
        seen: set[str] = set()
        links: list[dict[str, str]] = []
        for info in mgr.get_sink_info():
            url = info.get('ui_url', '')
            if not url or url in seen:
                continue
            seen.add(url)
            links.append(
                {
                    'sink_type': info['sink_type'],
                    'name': info['name'],
                    'ui_url': url,
                }
            )
        return links

    def is_cache_enabled(self) -> bool:
        """Template helper — returns True when the cache page should be visible.

        Used by ``base.html`` to conditionally render the Cache nav link.
        Keeping this as a lambda-style getter (not a captured boolean) so
        that a cache engine that's swapped in/out at runtime (unit tests
        frequently do this) is reflected immediately without a page reload.
        """
        return self.drakkar_app.cache_engine is not None

    def kafka_ui_message_url(self, topic: str, partition: int, offset: int) -> str:
        """Build a Kafka-UI deep-link URL for a single message.

        Returns '' when ``kafka.ui_url`` or ``kafka.ui_cluster_name`` is
        absent, so callers (Jinja templates and JS) can treat it as a
        feature toggle. The ``%3A%3A`` literal is the URL-encoded form of
        ``::`` that Kafka-UI expects in the seekTo parameter.
        """
        kcfg = self.drakkar_app._config.kafka
        if not kcfg.ui_url or not kcfg.ui_cluster_name or not topic:
            return ''
        base = kcfg.ui_url.rstrip('/')
        cluster = quote(kcfg.ui_cluster_name, safe='')
        topic_q = quote(str(topic), safe='')
        seek = f'{int(partition)}%3A%3A{int(offset)}'
        return f'{base}/ui/clusters/{cluster}/all-topics/{topic_q}/messages?seekType=OFFSET&seekTo={seek}&limit=1'

    # --- Auth ---

    def token_matches(self, provided: str | None) -> bool:
        """Timing-safe comparison of a provided token against the configured token.

        ``secrets.compare_digest`` raises ``TypeError`` on non-str/bytes inputs
        and leaks no information about the point of divergence when operands
        have the same length. Empty/None provided values short-circuit to
        ``False`` so ``compare_digest`` is never called with an empty operand.

        Non-ASCII str operands also raise ``TypeError`` ("comparing strings
        with non-ASCII characters is not supported"). We catch that and
        treat it as an auth failure so an attacker sending a bearer token
        like ``Bearer tôken`` can't surface 500s (or crash the WS handshake
        before the ``4401`` close code is sent), and so an operator who
        mistakenly configures a non-ASCII ``auth_token`` in YAML simply
        locks everyone out with a clean 401 instead of 500s.
        """
        if not provided or not self.config.auth_token:
            return False
        try:
            return secrets.compare_digest(provided, self.config.auth_token)
        except TypeError:
            # Non-ASCII operands raise TypeError; treat as auth failure.
            return False

    async def require_auth(
        self,
        request: Request,
        token: str | None = Query(default=None),
    ) -> None:
        """Check bearer token for protected endpoints (download, merge, probe).

        Skipped when ``auth_token`` is empty (no auth configured). Accepts
        token via ``Authorization: Bearer`` header or ``?token=`` query
        parameter (browsers can't set headers on file downloads).
        """
        if not self.config.auth_token:
            return
        auth_header = request.headers.get('authorization', '')
        header_token = auth_header.removeprefix('Bearer ').strip() if auth_header.startswith('Bearer ') else ''
        if self.token_matches(header_token) or self.token_matches(token):
            return
        raise HTTPException(status_code=401, detail='Invalid or missing auth token')

    async def flush_and_select(
        self,
        query: str,
        params: Sequence[Any] = (),
    ) -> tuple[list[str], list] | None:
        """Flush the recorder then run a SELECT against the reader connection.

        Consolidates the "flush → check DB → SELECT → (columns, rows)"
        pattern shared by 11 UI-server endpoints. Reads go through
        ``recorder.reader_db`` (the dedicated reader aiosqlite connection
        opened alongside the writer) so UI queries don't queue behind
        writer flushes. Dispatches via ``dispatch_to_loop`` so the
        connection stays on its owning loop when the UI server runs
        in a separate thread.

        Returns ``(columns, rows)`` on success or ``None`` when the reader
        connection is absent (recorder not started, or started without
        event storage). No automatic fallback to the writer — that would
        defeat the dedicated-reader-pool design. Callers map ``None`` onto
        whatever empty response shape their endpoint uses
        (``JSONResponse([])``, ``{}``, etc.).

        Both ``None`` causes — reader absent and dispatch timeout — are
        logged (rate-limited), because they look identical from the browser
        and an operator staring at a degraded page needs to know which one
        they have.
        """
        recorder = self.recorder

        async def _inner() -> tuple[list[str], list] | None:
            await recorder.flush()
            db = recorder.reader_db
            if not db:
                _warn_degraded_read(
                    'ui_read_reader_db_absent',
                    hint='recorder has no reader connection — not started yet, '
                    'or started without event storage (ui.recorder.db_dir / store_events)',
                )
                return None
            async with db.execute(query, params) as cur:
                columns: list[str] = [desc[0] for desc in cur.description or []]
                rows: list = list(await cur.fetchall())
            return columns, rows

        # Bounded: a bare dispatch waits forever, so a wedged main loop would
        # hang every one of the endpoints sharing this helper — including the
        # pages an operator opens precisely because the worker is misbehaving.
        # On timeout the caller sees the same ``None`` it already handles for
        # "no reader connection", so no endpoint needs a new branch.
        result = await self.dispatch_bounded(_inner(), default=_DISPATCH_FAILED)
        if result is _DISPATCH_FAILED:
            # ``dispatch_bounded`` already logged the timeout itself; this
            # line names the consequence, so the degraded UI response and its
            # cause sit together in the log.
            _warn_degraded_read(
                'ui_read_dispatch_unavailable',
                hint='main loop did not answer the recorder read in time — '
                'the pipeline may be stalled; UI data is degraded',
            )
            return None
        return result

    async def dispatch_bounded(self, coro: Coroutine[Any, Any, Any], default: Any = None) -> Any:
        """Run ``coro`` on the main loop, giving up after a bounded wait.

        Bare ``dispatch_to_loop`` waits forever. When the main loop is wedged
        (a stuck sink flush, an executor deadlock) every UI request that
        touches live worker state hangs with it — including the pages an
        operator opens *because* something is wrong. Bounding the wait turns
        that into a fast degraded response instead of a dead browser tab.

        Returns ``default`` on timeout or error; the caller renders its own
        empty shape. Errors are swallowed deliberately here — these are
        best-effort display reads, and the underlying operations already log
        and count their own failures.
        """
        try:
            return await asyncio.wait_for(
                dispatch_to_loop(coro, self.drakkar_app.main_loop),
                timeout=MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning(
                'ui_main_loop_dispatch_timeout',
                category='debug',
                timeout_seconds=MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS,
                hint='main loop did not answer in time — the pipeline may be stalled',
            )
            return default
        except Exception as exc:
            logger.warning(
                'ui_main_loop_dispatch_failed',
                category='debug',
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return default

    async def get_lag(self) -> dict[int, dict]:
        """Return per-partition lag info for currently assigned partitions.

        Hops to the main loop per the live-state ownership rule, and is
        bounded twice over: the Kafka metadata RPCs carry their own timeout
        (``consumer.LAG_QUERY_TIMEOUT_SECONDS``) and the dispatch itself is
        capped, so a broker outage degrades the lag panel instead of the page.
        """
        consumer = self.drakkar_app._consumer
        if not consumer or not self.drakkar_app.processors:
            return {}
        partition_ids = list(self.drakkar_app.processors.keys())
        return await self.dispatch_bounded(consumer.get_partition_lag(partition_ids), default={})

    async def get_total_lag(self, partition_ids: list[int]) -> int:
        """Return summed lag across ``partition_ids`` (0 when unavailable).

        Same bounding rationale as :meth:`get_lag`. Callers previously awaited
        the consumer directly from the UI loop with no cap at all.
        """
        consumer = self.drakkar_app._consumer
        if not consumer or not partition_ids:
            return 0
        return await self.dispatch_bounded(consumer.get_total_lag(partition_ids), default=0)

    # --- Prometheus link builder ---

    def build_prometheus_links(self) -> dict:
        """Build Prometheus graph URLs for dashboard cards and metrics panel.

        Returns empty dicts/lists when ``prometheus_url`` is not configured.
        """
        config = self.config
        drakkar_app = self.drakkar_app
        prom_url = config.prometheus_url.rstrip('/')
        if not prom_url:
            return {'card_links': {}, 'worker_links': [], 'cluster_links': []}

        rate = config.prometheus_rate_interval
        metrics_port = str(drakkar_app._config.metrics.port)
        tpl_vars = {
            'worker_id': drakkar_app._worker_id,
            'cluster_name': drakkar_app._cluster_name or '',
            'metrics_port': metrics_port,
            'debug_port': str(config.port),
        }

        def _expand(template: str) -> str:
            result = template
            for key, val in tpl_vars.items():
                result = result.replace('{' + key + '}', val)
            return result

        # Worker-scoped label filter
        if config.prometheus_worker_label:
            wf = _expand(config.prometheus_worker_label)
        else:
            import socket

            hostname = socket.gethostname()
            wf = f'instance="{hostname}:{metrics_port}"'

        # Cluster-scoped label filter
        cf = _expand(config.prometheus_cluster_label) if config.prometheus_cluster_label else ''

        def _graph_url(expr: str, range_input: str = '1h') -> str:
            return f'{prom_url}/graph?g0.expr={quote(expr)}&g0.tab=0&g0.range_input={range_input}'

        # Links for dashboard stat cards (worker-filtered)
        card_links = {
            'lag': _graph_url(f'drakkar_offset_lag{{{wf}}}'),
            'consumed': _graph_url(f'rate(drakkar_messages_consumed_total{{{wf}}}[{rate}])'),
            'completed': _graph_url(f'rate(drakkar_executor_tasks_total{{{wf},status="completed"}}[{rate}])'),
            'failed': _graph_url(f'rate(drakkar_executor_tasks_total{{{wf},status="failed"}}[{rate}])'),
            'produced': _graph_url(f'rate(drakkar_sink_payloads_delivered_total{{{wf}}}[{rate}])'),
        }

        # Worker-scoped panel links (grouped by category)
        worker_links = [
            {
                'category': 'Throughput',
                'links': [
                    ('Consume rate', _graph_url(f'rate(drakkar_messages_consumed_total{{{wf}}}[{rate}])')),
                    (
                        'Task completion rate',
                        _graph_url(f'rate(drakkar_executor_tasks_total{{{wf},status="completed"}}[{rate}])'),
                    ),
                    ('Sink delivery rate', _graph_url(f'rate(drakkar_sink_payloads_delivered_total{{{wf}}}[{rate}])')),
                    ('Commit rate', _graph_url(f'rate(drakkar_offsets_committed_total{{{wf}}}[{rate}])')),
                ],
            },
            {
                'category': 'Latency',
                'links': [
                    (
                        'Executor p95',
                        _graph_url(
                            f'histogram_quantile(0.95, rate(drakkar_executor_duration_seconds_bucket{{{wf}}}[{rate}]))'
                        ),
                    ),
                    (
                        'Batch p95',
                        _graph_url(
                            f'histogram_quantile(0.95, rate(drakkar_batch_duration_seconds_bucket{{{wf}}}[{rate}]))'
                        ),
                    ),
                    (
                        'Sink delivery p95',
                        _graph_url(
                            f'histogram_quantile(0.95, rate(drakkar_sink_deliver_duration_seconds_bucket{{{wf}}}[{rate}]))'
                        ),
                    ),
                    (
                        'Handler hooks p95',
                        _graph_url(
                            f'histogram_quantile(0.95, rate(drakkar_handler_duration_seconds_bucket{{{wf}}}[{rate}]))'
                        ),
                    ),
                ],
            },
            {
                'category': 'Health',
                'links': [
                    ('Consumer lag', _graph_url(f'drakkar_offset_lag{{{wf}}}')),
                    ('Queue sizes', _graph_url(f'drakkar_partition_queue_size{{{wf}}}')),
                    ('Backpressure', _graph_url(f'drakkar_backpressure_active{{{wf}}}')),
                    ('Pool active', _graph_url(f'drakkar_executor_pool_active{{{wf}}}')),
                ],
            },
            {
                'category': 'Errors',
                'links': [
                    (
                        'Task failures',
                        _graph_url(f'rate(drakkar_executor_tasks_total{{{wf},status="failed"}}[{rate}])'),
                    ),
                    ('Task timeouts', _graph_url(f'rate(drakkar_executor_timeouts_total{{{wf}}}[{rate}])')),
                    ('Task retries', _graph_url(f'rate(drakkar_task_retries_total{{{wf}}}[{rate}])')),
                    ('Sink errors', _graph_url(f'rate(drakkar_sink_deliver_errors_total{{{wf}}}[{rate}])')),
                    ('Sink retries', _graph_url(f'rate(drakkar_sink_delivery_retries_total{{{wf}}}[{rate}])')),
                    ('Consumer errors', _graph_url(f'rate(drakkar_consumer_errors_total{{{wf}}}[{rate}])')),
                    ('DLQ messages', _graph_url(f'rate(drakkar_sink_dlq_messages_total{{{wf}}}[{rate}])')),
                ],
            },
        ]

        # Cluster-wide links (only when cluster label is configured)
        cluster_links = []
        if cf:
            cluster_links = [
                ('Consume rate (cluster)', _graph_url(f'sum(rate(drakkar_messages_consumed_total{{{cf}}}[{rate}]))')),
                ('Total lag (cluster)', _graph_url(f'sum(drakkar_offset_lag{{{cf}}})')),
                (
                    'Task failures (cluster)',
                    _graph_url(f'sum(rate(drakkar_executor_tasks_total{{{cf},status="failed"}}[{rate}]))'),
                ),
                ('Sink errors (cluster)', _graph_url(f'sum(rate(drakkar_sink_deliver_errors_total{{{cf}}}[{rate}]))')),
                ('Pool active (cluster)', _graph_url(f'sum(drakkar_executor_pool_active{{{cf}}})')),
                ('Backpressure (cluster)', _graph_url(f'sum(drakkar_backpressure_active{{{cf}}})')),
            ]

        return {'card_links': card_links, 'worker_links': worker_links, 'cluster_links': cluster_links}


# Legacy JSON API paths that live outside the ``/api/`` namespace but still
# get a versioned alias (contract v1). Everything under ``/api/`` maps by the
# strip-``/api``-prefix rule in ``_v1_alias_path``; this table carries the
# irregular ones.
_V1_EXTRA_ALIASES: dict[str, str] = {
    '/debug/download/{filename}': '/api/v1/debug/download/{filename}',
}


def _v1_alias_path(path: str) -> str | None:
    """Map a legacy route path to its ``/api/v1`` alias, or ``None`` for no alias.

    HTML pages, the probes (``/healthz``/``/readyz``), the WebSocket, and
    routes already under ``/api/v1`` (the v1-only contract endpoints) get no
    alias.
    """
    if path.startswith('/api/v1/'):
        return None
    if path.startswith('/api/'):
        return '/api/v1/' + path.removeprefix('/api/')
    return _V1_EXTRA_ALIASES.get(path)


def register_v1_aliases(app: FastAPI, routers: Sequence[APIRouter]) -> None:
    """Register every legacy JSON API route under ``/api/v1/...`` as well.

    The UI contract (v1) versions the JSON surface behind an ``/api/v1``
    prefix while the legacy unprefixed paths keep working during the
    transition. Rather than duplicating handler registrations per module,
    this walks the given routers and re-registers each qualifying route on
    the app under its computed alias — same endpoint function, same
    dependency list (router-level ``Depends`` are merged into each route at
    registration time, so auth gating is identical on both prefixes).

    The walk deliberately reads ``router.routes`` (where ``@router.get``
    registrations live as ``APIRoute`` objects on every FastAPI version)
    rather than ``app.routes``: newer FastAPI includes routers lazily, so
    the app-level table no longer exposes ``APIRoute`` entries.
    """
    registered = 0
    for router in routers:
        for route in router.routes:
            if not isinstance(route, APIRoute):
                continue
            alias = _v1_alias_path(route.path)
            if alias is None:
                continue
            app.add_api_route(
                alias,
                route.endpoint,
                methods=sorted(route.methods or []),
                dependencies=route.dependencies,
                name=f'{route.name}_v1',
                response_class=route.response_class,
            )
            registered += 1
    if registered == 0:
        # Serving without the /api/v1 surface would silently break the UI
        # contract — fail at startup instead.
        raise RuntimeError('v1 alias registration found no legacy API routes; FastAPI routing internals changed')


def create_ui_app(
    config: UIConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
    ui_root: Path | None = None,
    ui_version: str | None = None,
    ui_source: str = 'release',
) -> FastAPI:
    """Create the FastAPI UI application.

    Wires up Jinja templates with the format helpers + sink-link / kafka-UI
    helpers, then mounts the four route modules onto the app. Each route
    module receives a single ``UIDeps`` parameter that exposes shared
    state and helpers.

    ``ui_root`` is the resolved drakkar-ui bundle directory (SPA mode, from
    ``ui.release.enabled``): the Jinja page routes are dropped and a catch-all
    registered LAST serves the SPA — bundle files as-is, ``index.html`` with
    a 200 for every unknown path — auth-gated exactly like the HTML pages.
    The probes, ``/ws``, all ``/api*`` JSON routes (legacy and ``/api/v1``),
    and ``/debug/download/{filename}`` keep precedence over the catch-all.
    ``None`` (default) keeps the built-in server-rendered HTML pages.

    ``ui_version`` is the release tag ``ui_root`` holds (identity v1.2's
    ``ui_version`` field); it is meaningful only in SPA mode.
    """
    # Local imports break a routes_* → server module cycle: each routes
    # module imports ``UIDeps`` from this module, and this module
    # imports the route factories. Pulling them in at call time keeps the
    # module-import graph acyclic.
    from drakkar.uiserver.routes_cache import create_cache_router
    from drakkar.uiserver.routes_config_reference import create_config_reference_router
    from drakkar.uiserver.routes_consume_pause import create_consume_pause_router
    from drakkar.uiserver.routes_debug import create_debug_router
    from drakkar.uiserver.routes_kafka_read import create_kafka_read_router
    from drakkar.uiserver.routes_live import create_live_router
    from drakkar.uiserver.routes_openapi import create_openapi_router
    from drakkar.uiserver.routes_pages import create_pages_router
    from drakkar.uiserver.routes_renderers import create_renderers_router
    from drakkar.uiserver.routes_runtime import create_runtime_router
    from drakkar.uiserver.routes_spa import create_spa_router
    from drakkar.uiserver.routes_uipages import create_uipages_router

    # ``openapi_url=None`` disables FastAPI's auto-generated schema route.
    # Disabling docs_url/redoc_url alone still leaves ``GET /openapi.json``
    # served, and that route is created by FastAPI itself so it carries none
    # of our auth dependencies — it answered 200 with the full route table
    # (including every /api/debug/* path) even with ui.auth_token set. The
    # contract surface is served instead by routes_openapi.py, which vendors
    # the spec and is auth-gated like the rest of its route class.
    app = FastAPI(title='Drakkar UI', docs_url=None, redoc_url=None, openapi_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.autoescape = True
    templates.env.globals['format_ts'] = format_ts  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_ms'] = format_ts_ms  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_full'] = format_ts_full  # ty: ignore[invalid-assignment]
    templates.env.globals['format_uptime'] = format_uptime  # ty: ignore[invalid-assignment]

    deps = UIDeps(
        config=config,
        recorder=recorder,
        drakkar_app=drakkar_app,
        templates=templates,
        ui_version=ui_version if ui_root is not None else None,
        # Identity's ui_source contract value: 'release'/'embedded' in SPA
        # mode (whatever the caller resolved), 'builtin' when the Jinja
        # pages serve.
        ui_source=ui_source if ui_root is not None else 'builtin',
    )

    # Jinja globals that need access to the live app/config — bound
    # methods on ``deps`` so config changes (e.g. cache engine swapped
    # in/out at runtime in tests) are reflected on the next render.
    templates.env.globals['get_sink_ui_links'] = deps.get_sink_ui_links  # ty: ignore[invalid-assignment]
    templates.env.globals['is_cache_enabled'] = deps.is_cache_enabled  # ty: ignore[invalid-assignment]
    templates.env.globals['kafka_ui_message_url'] = deps.kafka_ui_message_url  # ty: ignore[invalid-assignment]
    templates.env.globals['kafka_source_topic'] = drakkar_app._config.kafka.source_topic  # ty: ignore[invalid-assignment]
    # The JS-rendered pages (history, live) need to build these URLs too.
    # Expose the raw bits so the templates can inject them into a JS
    # constants block once and let the renderers compose URLs per-row.
    templates.env.globals['kafka_ui_base'] = drakkar_app._config.kafka.ui_url.rstrip('/')  # ty: ignore[invalid-assignment]
    templates.env.globals['kafka_ui_cluster'] = drakkar_app._config.kafka.ui_cluster_name  # ty: ignore[invalid-assignment]

    # Mount the four route modules. Order doesn't matter for correctness
    # among them (FastAPI matches by path), but grouping reads naturally as
    # pages → live → debug → cache. Only leaf routers land here — the
    # /api/v1 alias walk below cannot see through nested include_router
    # calls on newer FastAPI. In SPA mode (``ui_root``) the Jinja page
    # routes are dropped so the catch-all below owns the page surface.
    include_html = ui_root is None
    pages_public, pages_gated = create_pages_router(deps, include_html=include_html)
    routers = (
        pages_public,
        pages_gated,
        create_live_router(deps, include_html=include_html),
        create_debug_router(deps, include_html=include_html),
        create_cache_router(deps),
        create_consume_pause_router(deps),
        create_kafka_read_router(deps),
        create_runtime_router(deps),
        create_config_reference_router(deps),
        create_uipages_router(deps),
        create_renderers_router(deps),
        create_openapi_router(deps),
    )
    for router in routers:
        app.include_router(router)

    # Contract v1: every legacy JSON route is also served under /api/v1.
    register_v1_aliases(app, routers)

    # SPA catch-all LAST: Starlette matches in registration order, so every
    # route above (probes, /ws, /api*, downloads) keeps precedence.
    if ui_root is not None:
        app.include_router(create_spa_router(deps, ui_root))

    return app


class UIServer:
    """Manages the UI FastAPI server in a separate thread.

    Runs Uvicorn in its own thread with a dedicated event loop so that
    CPU-intensive executor tasks on the main loop don't block the UI.
    """

    def __init__(
        self,
        config: UIConfig,
        recorder: EventRecorder,
        app: DrakkarApp,
    ) -> None:
        self._config = config
        self._recorder = recorder
        self._drakkar_app = app
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        bundle = await self._resolve_ui_bundle()
        fastapi_app = create_ui_app(
            self._config,
            self._recorder,
            self._drakkar_app,
            ui_root=bundle.root if bundle else None,
            ui_version=bundle.version if bundle else None,
            # Contract v1.2 label: cache/fetched bundles are a 'release';
            # the package-baked copy reports 'embedded'.
            ui_source='embedded' if bundle and bundle.source == 'embedded' else 'release',
        )
        uvi_config = uvicorn.Config(
            app=fastapi_app,
            host=self._config.host,
            port=self._config.port,
            log_level='warning',
        )
        self._server = uvicorn.Server(uvi_config)
        self._thread = threading.Thread(
            target=self._server.run,
            name='drakkar-ui-server',
            daemon=True,
        )
        self._thread.start()
        # Verify the bind before declaring success. Without this a
        # port-in-use error dies silently inside the daemon thread while
        # the worker keeps running with no probe surface (/healthz,
        # /readyz). The Go backend treats a debug-server bind failure as
        # fatal — mirror that by raising so startup fails visibly. The
        # wait runs off-loop (to_thread) because it polls with sleeps.
        try:
            await asyncio.to_thread(self._wait_until_serving, timeout=5.0)
        except Exception as exc:
            await logger.aerror(
                'ui_server_start_failed',
                category='ui',
                host=self._config.host,
                port=self._config.port,
                error=str(exc),
            )
            raise
        await logger.ainfo('ui_server_started', category='ui', port=self._config.port)

    def _wait_until_serving(self, timeout: float) -> None:
        """Block (in a worker thread) until uvicorn is serving or fail loudly.

        ``uvicorn.Server.started`` flips True once the socket is bound
        and accept() is running; on a bind failure uvicorn logs and
        ``sys.exit(1)``s, killing the daemon thread — so a dead thread
        before ``started`` means the bind failed.
        """
        assert self._server is not None and self._thread is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if getattr(self._server, 'started', False):
                return
            if not self._thread.is_alive():
                raise RuntimeError(
                    f'UI server thread exited before serving — '
                    f'is {self._config.host}:{self._config.port} already in use?'
                )
            time.sleep(0.05)
        raise TimeoutError(f'UI server did not start serving within {timeout}s')

    async def _resolve_ui_bundle(self) -> ResolvedBundle | None:
        """Resolve the drakkar-ui bundle when ``ui.release.enabled``, else ``None``.

        Returns the full :class:`drakkar.uihost.ResolvedBundle` (directory +
        provenance + version) so the identity endpoint can report which UI
        release this worker serves.

        The resolution (cache → GitHub fetch → embedded fallback) runs in a
        worker thread so a slow network fetch never blocks the main loop; it
        is bounded internally by ``UI_RESOLVE_TIMEOUT_SECONDS`` and is never
        fatal. The ladder always lands somewhere: an offline host with an
        empty cache serves the release embedded in the package. Only a
        resolution *error* (or ``ui.release.enabled=false``) leaves the
        server on its built-in HTML pages.
        """
        from drakkar.config import UIConfig
        from drakkar.uihost import resolve

        ui_cfg = getattr(self._drakkar_app._config, 'ui', None)
        # The isinstance check keeps MagicMock-configured test apps (whose
        # attribute chain is truthy) from triggering a real resolution.
        if not isinstance(ui_cfg, UIConfig) or not ui_cfg.release.enabled:
            return None
        try:
            bundle = await asyncio.to_thread(resolve, ui_cfg.release)
        except Exception as exc:
            await logger.awarning('ui_resolve_failed', category='ui', error=str(exc))
            return None
        if bundle is None:
            return None
        # 'embedded' is the real drakkar-ui release baked into the package
        # (``just embed-ui``) — a first-class rung of the ladder, served
        # like any other bundle. The built-in Jinja pages remain only for
        # ``ui.release.enabled=false`` and resolution errors.
        await logger.ainfo(
            'ui_bundle_resolved', category='ui', source=bundle.source, version=bundle.version, dir=str(bundle.root)
        )
        return bundle

    async def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            # join() blocks; run it off-loop so a slow uvicorn drain
            # cannot stall the main event loop during shutdown.
            await asyncio.to_thread(self._thread.join, timeout=5.0)
        await logger.ainfo('ui_server_stopped', category='ui')
