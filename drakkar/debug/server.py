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

Each routes module defines a ``create_*_router(deps)`` factory that
returns an ``APIRouter`` and is mounted into the app via
``include_router``. The shared helpers (auth/origin checks, cross-thread
dispatch, prometheus-link builder, etc.) hang off ``DebugDeps`` so the
routers can reach them through one parameter.

Helpers that are pure functions (no closure over ``drakkar_app`` or
``config``) live in :mod:`drakkar.debug.server_helpers`.
"""

from __future__ import annotations

import secrets
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates

from drakkar.concurrency import dispatch_to_loop
from drakkar.config import DebugConfig

# Re-import the helpers that used to live here so test patches like
# ``drakkar.debug.server.hook_flags`` keep working without changes.
# ``_DEFAULT_PORTS`` / ``normalize_hostport`` / ``parse_host_header`` are
# not referenced inside this module after the helper extraction but ARE
# imported by tests via the original ``drakkar.debug.server.<name>``
# path; ``noqa: F401`` keeps the re-export visible without tripping
# ruff's unused-import check.
from drakkar.debug.server_helpers import (
    _DEFAULT_PORTS,  # noqa: F401  (re-exported for tests)
    format_ts,
    format_ts_full,
    format_ts_ms,
    format_uptime,
    hook_flags,  # noqa: F401  (re-exported for tests)
    normalize_hostport,  # noqa: F401  (re-exported for tests)
    origin_allowed,  # noqa: F401  (re-exported for tests)
    parse_host_header,  # noqa: F401  (re-exported for tests)
    worker_group,  # noqa: F401  (re-exported for tests)
)
from drakkar.recorder import EventRecorder

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

TEMPLATES_DIR = Path(__file__).parent.parent / 'templates'


class DebugDeps:
    """Shared dependencies + helpers passed to each route-module factory.

    Routes don't capture ``drakkar_app`` / ``recorder`` / ``config`` /
    ``templates`` directly; they reach them through this object. Pure
    helpers that need access to the live app/config (auth, kafka-UI URL
    builder, prometheus-link builder, cross-thread dispatch) are methods
    here so the router factories don't have to plumb them individually.
    """

    def __init__(
        self,
        config: DebugConfig,
        recorder: EventRecorder,
        drakkar_app: DrakkarApp,
        templates: Jinja2Templates,
    ) -> None:
        self.config = config
        self.recorder = recorder
        self.drakkar_app = drakkar_app
        self.templates = templates

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
        pattern shared by 11 debug-server endpoints. Reads go through
        ``recorder.reader_db`` (the dedicated reader aiosqlite connection
        opened alongside the writer) so UI queries don't queue behind
        writer flushes. Dispatches via ``dispatch_to_loop`` so the
        connection stays on its owning loop when the debug server runs
        in a separate thread.

        Returns ``(columns, rows)`` on success or ``None`` when the reader
        connection is absent (recorder not started, or started without
        event storage). No automatic fallback to the writer — that would
        defeat the dedicated-reader-pool design. Callers map ``None`` onto
        whatever empty response shape their endpoint uses
        (``JSONResponse([])``, ``{}``, etc.).
        """
        recorder = self.recorder

        async def _inner() -> tuple[list[str], list] | None:
            await recorder.flush()
            db = recorder.reader_db
            if not db:
                return None
            async with db.execute(query, params) as cur:
                columns: list[str] = [desc[0] for desc in cur.description or []]
                rows: list = list(await cur.fetchall())
            return columns, rows

        return await dispatch_to_loop(_inner(), self.drakkar_app.main_loop)

    async def get_lag(self) -> dict[int, dict]:
        """Return per-partition lag info for currently assigned partitions."""
        consumer = self.drakkar_app._consumer
        if not consumer or not self.drakkar_app.processors:
            return {}
        try:
            return await consumer.get_partition_lag(
                list(self.drakkar_app.processors.keys()),
            )
        except Exception:
            return {}

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


def create_debug_app(
    config: DebugConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
) -> FastAPI:
    """Create the FastAPI debug application.

    Wires up Jinja templates with the format helpers + sink-link / kafka-UI
    helpers, then mounts the four route modules onto the app. Each route
    module receives a single ``DebugDeps`` parameter that exposes shared
    state and helpers.
    """
    # Local imports break a routes_* → server module cycle: each routes
    # module imports ``DebugDeps`` from this module, and this module
    # imports the route factories. Pulling them in at call time keeps the
    # module-import graph acyclic.
    from drakkar.debug.routes_cache import create_cache_router
    from drakkar.debug.routes_debug import create_debug_router
    from drakkar.debug.routes_live import create_live_router
    from drakkar.debug.routes_pages import create_pages_router

    app = FastAPI(title='Drakkar Debug', docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.autoescape = True
    templates.env.globals['format_ts'] = format_ts  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_ms'] = format_ts_ms  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_full'] = format_ts_full  # ty: ignore[invalid-assignment]
    templates.env.globals['format_uptime'] = format_uptime  # ty: ignore[invalid-assignment]

    deps = DebugDeps(
        config=config,
        recorder=recorder,
        drakkar_app=drakkar_app,
        templates=templates,
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
    # (FastAPI matches by path), but grouping reads naturally as
    # pages → live → debug → cache.
    app.include_router(create_pages_router(deps))
    app.include_router(create_live_router(deps))
    app.include_router(create_debug_router(deps))
    app.include_router(create_cache_router(deps))

    return app


class DebugServer:
    """Manages the debug FastAPI server in a separate thread.

    Runs Uvicorn in its own thread with a dedicated event loop so that
    CPU-intensive executor tasks on the main loop don't block the UI.
    """

    def __init__(
        self,
        config: DebugConfig,
        recorder: EventRecorder,
        app: DrakkarApp,
    ) -> None:
        self._config = config
        self._recorder = recorder
        self._drakkar_app = app
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        fastapi_app = create_debug_app(
            self._config,
            self._recorder,
            self._drakkar_app,
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
            name='drakkar-debug-ui',
            daemon=True,
        )
        self._thread.start()
        await logger.ainfo('debug_server_started', category='debug', port=self._config.port)

    async def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5.0)
        await logger.ainfo('debug_server_stopped', category='debug')


# ---------------------------------------------------------------------------
# Re-exports for tests that still import request-body models from
# ``drakkar.debug.server``. Kept at the bottom because importing the routes
# modules above triggers their module-level Pydantic class definitions,
# which we then surface here for backwards compatibility.
# ---------------------------------------------------------------------------

# Tests + the historical public surface expect these names on the server
# module. Import lazily-after-module-load: routes_live and routes_debug
# define their request-body models at module scope.
from drakkar.debug.routes_debug import _ProbeRequest  # noqa: E402, F401
from drakkar.debug.routes_live import (  # noqa: E402, F401
    _ArrangeTaskLookupRequest,
    _SinkBreakdownRequest,
)
