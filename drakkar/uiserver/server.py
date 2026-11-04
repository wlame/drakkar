"""The worker's one HTTP server: the always-on routes, plus the UI when it is on.

:func:`create_api_app` builds what every worker with an API serves.
:func:`create_ui_app` adds the UI to it: a FastAPI JSON API plus the SPA.

The route handlers are split across sibling modules to keep these factories
thin. Every JSON endpoint lives under ``/api/v1``; the pages are served by
the drakkar-ui bundle through ``routes_spa``, not rendered here.

  * ``routes_health`` — ``/healthz`` and ``/readyz``.
  * ``routes_peer``   — ``/api/v1/peer/*``, the routes workers call on each other.
  * ``routes_pages``  — WebSocket, and the top-level JSON
    APIs (dashboard, partitions, task, identity, sinks, workers,
    processors).
  * ``routes_live``   — ``/api/v1/live/*``, ``/api/v1/recent-tasks``,
    ``/api/v1/events``.
  * ``routes_debug``  — ``/api/v1/debug/*`` (databases, trace, metrics,
    periodic, probe, download, archives).
  * ``routes_cache``  — ``/api/v1/debug/cache/*``.
  * ``routes_kafka_read``    — ``/api/v1/debug/kafka/*``.
  * ``routes_consume_pause`` — ``/api/v1/debug/consume-pause``.
  * ``routes_runtime``       — ``/api/v1/debug/runtime/*``.
  * ``routes_renderers``     — ``/api/v1/ui/renderers.js``.
  * ``routes_config_reference`` — ``/api/v1/config-reference``.
  * ``routes_openapi``       — the vendored OpenAPI artifact + Swagger UI.
  * ``routes_spa``           — the drakkar-ui bundle, with an
    ``index.html`` fallback for client-side routes.

Each UI routes module defines a ``create_*_router(deps)`` factory that
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
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from drakkar.concurrency import dispatch_to_loop
from drakkar.config import APIConfig, UIConfig
from drakkar.loopserver import LoopLoggingServer
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder.queries import (
    SOURCE_FILE,
    SOURCE_MEMORY,
    ReadBound,
    ReadClass,
    Source,
    pick_source,
    read_memory,
    still_covered,
)
from drakkar.uihost import ResolvedBundle

# Re-export the helpers here so test imports and patches like
# ``drakkar.uiserver.server.hook_flags`` keep working; ``noqa: F401`` keeps
# the re-exports visible without tripping ruff's unused-import check.
from drakkar.uiserver.cors import install_fanout_cors
from drakkar.uiserver.server_helpers import (
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

# HTTP transport limits. Uvicorn sets none of these by default, so a
# client that dribbled bytes — or simply opened connections and left them —
# could pin connections and memory in the process that also runs the
# pipeline AND answers the Kubernetes probes. The same values are used on
# every worker of a fleet, so all of them behave alike.
#
# Note that ``/ws`` is exempt from the keep-alive reaper: an upgraded
# WebSocket is not a keep-alive HTTP connection.

# Reaps keep-alive connections idle between requests, so an operator
# leaving a dashboard tab open does not pin connections forever.
KEEP_ALIVE_TIMEOUT_SECONDS = 120

# Caps the request header section.
MAX_HEADER_BYTES = 64 * 1024

# The ``/ws`` live stream uses the sans-I/O websockets backend. Uvicorn's
# ``auto`` picked the deprecated ``websockets.legacy`` backend before
# uvicorn 0.50, whose close path could leave a keepalive-timeout error
# unretrieved on the loop; naming the backend keeps that out of any install.
WS_PROTOCOL = 'websockets-sansio'

# Keepalive for ``/ws``: a client that misses a pong within the timeout
# (sleeping laptop, throttled background tab) is disconnected, so a dead
# dashboard does not keep a live-event subscriber queue alive. These are
# uvicorn's own defaults, pinned so they cannot drift with an upgrade.
WS_PING_INTERVAL_SECONDS = 20.0
WS_PING_TIMEOUT_SECONDS = 20.0

# Caps a request body before it is buffered. The server takes a body on
# exactly two routes (the message probe and the merge request), and the
# probe's own 10 MB limit is a pydantic field constraint — it fires only
# AFTER the whole body has been read into memory, which is the wrong end of
# the problem. Rejecting at this size is what keeps an oversized POST from
# being buffered at all.
MAX_BODY_BYTES = 10 * 1024 * 1024

# A degraded recorder read repeats on every poll of every open page, so the
# cause is logged at most once per interval per cause rather than per
# request — enough for an operator to see WHY the UI is empty without
# flooding the log on a busy page.
DEGRADED_READ_LOG_INTERVAL_SECONDS = 60.0
_degraded_read_last_logged: dict[str, float] = {}

# Distinguishes "the dispatch never produced a value" from a legitimate
# ``None`` returned by the read itself, which the reader-absent branch uses.
_DISPATCH_FAILED = object()


@dataclass(frozen=True)
class RecorderRows:
    """The answer of one recorder read: column names, rows, and the source that gave them."""

    columns: list[str]
    rows: list
    source: Source


async def dispatch_bounded(coro: Coroutine[Any, Any, Any], main_loop: Any, default: Any = None) -> Any:
    """Run ``coro`` on the main loop, giving up after a bounded wait.

    Bare ``dispatch_to_loop`` waits forever. When the main loop is wedged
    (a stuck sink flush, an executor deadlock) every request that
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
            dispatch_to_loop(coro, main_loop),
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

    Routes don't capture ``drakkar_app`` / ``recorder`` / ``config``
    directly; they reach them through this object. Pure
    helpers that need access to the live app/config (auth, kafka-UI URL
    builder, prometheus-link builder, cross-thread dispatch) are methods
    here so the router factories don't have to plumb them individually.
    """

    def __init__(
        self,
        config: UIConfig,
        recorder: EventRecorder,
        drakkar_app: DrakkarApp,
        ui_version: str | None = None,
        ui_source: str = 'release',
    ) -> None:
        self.config = config
        self.recorder = recorder
        self.drakkar_app = drakkar_app
        # Identity v1.2: which drakkar-ui bundle this server serves — the
        # release tag, or ``None`` when no bundle could be resolved and the
        # server serves no pages. ``ui_source`` is ``'release'`` for a
        # fetched or cached bundle and ``''`` when none is served.
        self.ui_version = ui_version
        self.ui_source = ui_source
        # A failing Postgres history search repeats on every search of every
        # open page, so its line goes out at most once per minute.
        self.postgres_read_log = RateLimitedLog(time.monotonic)

    # --- Sink-UI / Kafka-UI helpers ---

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

    async def recorder_select(
        self,
        query: str,
        params: Sequence[Any],
        *,
        read_class: ReadClass,
        bound: ReadBound,
        endpoint: str,
    ) -> RecorderRows | None:
        """Run one recorder SELECT on the source the read class picks; ``None`` when there is no answer.

        Runs on the main loop: the source rule (:func:`pick_source`) reads
        the coverage there, a memory read runs the memory stage first and a
        file read both stages. A memory answer is checked again against
        the coverage read after its SELECT (:func:`still_covered`): when an
        eviction in between removed rows the read asked for, the read runs
        again on the file with ``sqlite``, and keeps the shorter answer
        without a recorder file (``none`` and ``postgres``).

        ``None`` means no answer, and the caller answers its endpoint's
        empty shape. A missing file reader and a main loop that did not
        answer in time are logged (rate-limited), and so is a memory read
        stopped by the read limit (``ui_read_memory_timed_out``). A memory
        store that is not open, or closed under the read while the
        recorder stops, answers ``None`` without a log line.
        """
        recorder = self.recorder

        async def _inner() -> RecorderRows | None:
            source = pick_source(recorder.persistence, read_class, bound, recorder.memory_coverage())
            if source == SOURCE_MEMORY:
                await recorder.flush_memory()
                result = await read_memory(recorder.memory_store, query, params, endpoint=endpoint)
                if result is None:
                    return None
                columns, rows = result
                if not recorder.has_file:
                    return RecorderRows(columns=columns, rows=rows, source=SOURCE_MEMORY)
                # The coverage now, read on the main loop after the SELECT.
                coverage = recorder.memory_coverage()
                if coverage is not None and still_covered(read_class, bound, coverage, len(rows)):
                    return RecorderRows(columns=columns, rows=rows, source=SOURCE_MEMORY)
            return await self._select_file(query, params)

        return await self._dispatch_read(_inner())

    async def recorder_select_on(
        self,
        source: Source,
        query: str,
        params: Sequence[Any],
        *,
        endpoint: str,
    ) -> RecorderRows | None:
        """Run one recorder SELECT on ``source``, with no source rule; ``None`` when there is no answer.

        For the aux lookups of a composite endpoint, which read the source
        of its primary query, and for reads that always use memory.
        """
        recorder = self.recorder

        async def _inner() -> RecorderRows | None:
            if source == SOURCE_FILE:
                return await self._select_file(query, params)
            await recorder.flush_memory()
            result = await read_memory(recorder.memory_store, query, params, endpoint=endpoint)
            if result is None:
                return None
            columns, rows = result
            return RecorderRows(columns=columns, rows=rows, source=SOURCE_MEMORY)

        return await self._dispatch_read(_inner())

    async def _select_file(self, query: str, params: Sequence[Any]) -> RecorderRows | None:
        """Both stages, then the SELECT on the live file; on the main loop."""
        recorder = self.recorder
        await recorder.flush()
        db = recorder.file_reader_db
        if db is None:
            _warn_degraded_read(
                'ui_read_reader_db_absent',
                hint='recorder has no reader connection — not started yet, '
                'or started without event storage (recorder.capture.events)',
            )
            return None
        async with db.execute(query, params) as cur:
            columns: list[str] = [desc[0] for desc in cur.description or []]
            rows: list = list(await cur.fetchall())
        return RecorderRows(columns=columns, rows=rows, source=SOURCE_FILE)

    async def _dispatch_read(self, read: Coroutine[Any, Any, RecorderRows | None]) -> RecorderRows | None:
        """Run a recorder read on the main loop, bounded; a loop that does not answer gives ``None``."""
        # Bounded: a bare dispatch waits forever, so a wedged main loop would
        # hang every endpoint that reads the recorder — including the pages
        # an operator opens precisely because the worker is misbehaving. On
        # timeout the caller sees the same ``None`` it already handles for
        # "no reader connection", so no endpoint needs a new branch.
        result = await self.dispatch_bounded(read, default=_DISPATCH_FAILED)
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
        """Run ``coro`` on this worker's main loop; see :func:`dispatch_bounded`."""
        return await dispatch_bounded(coro, self.drakkar_app.main_loop, default=default)

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

        Same bounding rationale as :meth:`get_lag`: without a bound, awaiting
        the consumer directly from the UI loop would have no cap on the
        wait at all.
        """
        consumer = self.drakkar_app._consumer
        if not consumer or not partition_ids:
            return 0
        return await self.dispatch_bounded(consumer.get_total_lag(partition_ids), default=0)

    # --- Prometheus link builder ---

    def build_prometheus_links(self) -> dict:
        """Build Prometheus graph URLs for dashboard cards and metrics panel.

        Returns empty dicts/lists when ``prometheus.url`` is not configured.
        """
        prometheus = self.config.prometheus
        drakkar_app = self.drakkar_app
        prom_url = prometheus.url.rstrip('/')
        if not prom_url:
            return {'card_links': {}, 'worker_links': [], 'cluster_links': []}

        rate = prometheus.rate_interval
        metrics_port = str(drakkar_app._config.metrics.port)
        tpl_vars = {
            'worker_id': drakkar_app._worker_id,
            'cluster_name': drakkar_app._cluster_name or '',
            'metrics_port': metrics_port,
            'debug_port': str(drakkar_app._config.api.port),
        }

        def _expand(template: str) -> str:
            result = template
            for key, val in tpl_vars.items():
                result = result.replace('{' + key + '}', val)
            return result

        # Worker-scoped label filter
        if prometheus.worker_label:
            wf = _expand(prometheus.worker_label)
        else:
            import socket

            hostname = socket.gethostname()
            wf = f'instance="{hostname}:{metrics_port}"'

        # Cluster-scoped label filter
        cf = _expand(prometheus.cluster_label) if prometheus.cluster_label else ''

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
                    ('Pending tasks', _graph_url(f'drakkar_pending_tasks{{{wf}}}')),
                    ('Pending bytes', _graph_url(f'drakkar_pending_bytes{{{wf}}}')),
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


def create_api_app(drakkar_app: DrakkarApp) -> FastAPI:
    """Create the FastAPI application every worker with an API serves.

    It holds ``/healthz``, ``/readyz`` and the peer routes, which read only
    the worker itself: no recorder, no ``ui`` section, no bundle. Any other
    path answers 404. :func:`create_ui_app` adds the UI to this app.
    """
    # Local imports for the same reason as in ``create_ui_app``.
    from drakkar.uiserver.routes_health import create_health_router
    from drakkar.uiserver.routes_peer import create_peer_router

    # ``openapi_url=None`` disables FastAPI's auto-generated schema route.
    # Disabling docs_url/redoc_url alone still leaves ``GET /openapi.json``
    # served, and that route is created by FastAPI itself so it carries none
    # of our auth dependencies: it would answer with the full route table
    # (including every /api/v1/debug/* path) whatever ui.auth_token says. The
    # contract surface is served instead by routes_openapi.py, which vendors
    # the spec and is auth-gated like the rest of its route class.
    app = FastAPI(title='Drakkar', docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware('http')
    async def _reject_oversized_bodies(request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Reject an over-long request body before anything buffers it.

        Declared size only — a body arriving without ``Content-Length``
        (chunked) is not counted here, because reading it to find out is
        exactly the buffering this exists to avoid. The two routes that
        take a body validate their own content afterwards; this is the
        cheap outer bound, and ``MAX_BODY_BYTES`` explains why the probe's
        pydantic-level limit was not enough on its own.
        """
        declared = request.headers.get('content-length')
        if declared is not None:
            try:
                length = int(declared)
            except ValueError:
                return JSONResponse({'error': 'Invalid Content-Length'}, status_code=400)
            if length > MAX_BODY_BYTES:
                return JSONResponse(
                    {'error': 'Request body too large', 'max_bytes': MAX_BODY_BYTES},
                    status_code=413,
                )
        return await call_next(request)

    app.include_router(create_health_router(drakkar_app))
    app.include_router(create_peer_router(drakkar_app))
    return app


def create_ui_app(
    config: UIConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
    ui_root: Path | None = None,
    ui_version: str | None = None,
    ui_source: str | None = None,
) -> FastAPI:
    """Create the FastAPI application of a worker with the UI on.

    Mounts the route modules onto the API app; each receives a single
    ``UIDeps`` parameter exposing the shared state and helpers.

    ``ui_root`` is the resolved drakkar-ui bundle directory. A catch-all
    registered LAST serves it — bundle files as-is, ``index.html`` with a
    200 for every unknown path — auth-gated exactly like the API. The
    probes, ``/ws`` and every ``/api/v1/*`` route keep precedence over it.

    ``None`` means no bundle could be resolved (or ``ui.release.enabled``
    is off): the worker then serves its API, probes and WebSocket as
    normal, and the catch-all answers every page request with a 503 saying
    how to supply a bundle. There is no built-in HTML fallback.

    ``ui_version`` is the release tag ``ui_root`` holds (identity v1.2's
    ``ui_version`` field), or ``None`` when there is no bundle.
    """
    # Local imports break a routes_* → server module cycle: each routes
    # module imports ``UIDeps`` from this module, and this module
    # imports the route factories. Pulling them in at call time keeps the
    # module-import graph acyclic.
    from drakkar.uiserver.routes_cache import create_cache_router
    from drakkar.uiserver.routes_config_reference import create_config_reference_router
    from drakkar.uiserver.routes_consume_pause import create_consume_pause_router
    from drakkar.uiserver.routes_databases import create_databases_router
    from drakkar.uiserver.routes_debug import create_debug_router
    from drakkar.uiserver.routes_docs import create_docs_router
    from drakkar.uiserver.routes_kafka_read import create_kafka_read_router
    from drakkar.uiserver.routes_live import create_live_router
    from drakkar.uiserver.routes_openapi import create_openapi_router
    from drakkar.uiserver.routes_pages import create_pages_router
    from drakkar.uiserver.routes_renderers import create_renderers_router
    from drakkar.uiserver.routes_runtime import create_runtime_router
    from drakkar.uiserver.routes_spa import create_spa_router
    from drakkar.uiserver.routes_uipages import create_uipages_router

    app = create_api_app(drakkar_app)

    # The browser fan-out reads three debug paths cross-origin; their
    # preflight must answer before auth (it carries no token).
    install_fanout_cors(app, config)

    if ui_source is None:
        # Derived, not defaulted: identity must never report a served bundle
        # ('release') next to a null ui_version. Deriving both from ui_root
        # keeps the pair consistent for every caller, tests included. An
        # explicit value is still accepted for a future source label.
        ui_source = 'release' if ui_root else ''

    deps = UIDeps(
        config=config,
        recorder=recorder,
        drakkar_app=drakkar_app,
        ui_version=ui_version,
        ui_source=ui_source,
    )

    # Mount the route modules. Order doesn't matter for correctness among
    # them (FastAPI matches by path), but grouping reads naturally as
    # pages → live → debug → cache. Only leaf routers land here: newer
    # FastAPI includes routers lazily, so a nesting ``include_router``
    # would hide its routes from the app-level table.
    pages_websocket, pages_gated = create_pages_router(deps)
    routers = (
        pages_websocket,
        pages_gated,
        create_live_router(deps),
        create_debug_router(deps),
        create_databases_router(deps),
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

    # The operator docs site sits between the API routers and the SPA: it
    # must outrank the catch-all (otherwise /docs/ renders the UI shell),
    # and it is registered unconditionally so an unconfigured or missing
    # site answers its own hint-404 rather than falling through.
    app.include_router(create_docs_router(deps))

    # SPA catch-all LAST: Starlette matches in registration order, so every
    # route above (probes, /ws, /api/v1/*) keeps precedence. With no bundle
    # resolved there is no page surface at all — see ``create_spa_router``,
    # which then answers every non-API path with a 503 explaining how to
    # supply one. The JSON API, the probes and the WebSocket are unaffected.
    app.include_router(create_spa_router(deps, ui_root))

    return app


class APIServer:
    """Manages the worker's HTTP server in a separate thread.

    Runs Uvicorn in its own thread with a dedicated event loop so that
    CPU-intensive executor tasks on the main loop don't block it. ``ui`` is
    the UI section when the worker serves the UI, which also needs ``recorder``;
    with ``ui=None`` the server holds only the routes of :func:`create_api_app`.
    """

    def __init__(
        self,
        config: APIConfig,
        app: DrakkarApp,
        ui: UIConfig | None = None,
        recorder: EventRecorder | None = None,
    ) -> None:
        self._config = config
        self._drakkar_app = app
        self._ui = ui
        self._recorder = recorder
        self._server: LoopLoggingServer | None = None
        self._thread: threading.Thread | None = None

    async def _build_app(self) -> FastAPI:
        """The application to serve; only a worker with the UI on resolves a bundle.

        Raises:
            ValueError: ``ui`` is given without the ``recorder`` its routes read.
        """
        if self._ui is None or not self._ui.enabled:
            return create_api_app(self._drakkar_app)
        if self._recorder is None:
            raise ValueError('APIServer: the UI routes need a recorder')
        bundle = await self._resolve_ui_bundle(self._ui)
        return create_ui_app(
            self._ui,
            self._recorder,
            self._drakkar_app,
            ui_root=bundle.root if bundle else None,
            ui_version=bundle.version if bundle else None,
        )

    async def start(self) -> None:
        fastapi_app = await self._build_app()
        uvi_config = uvicorn.Config(
            app=fastapi_app,
            host=self._config.host,
            port=self._config.port,
            log_level='warning',
            # See the module constants for why each of these is set.
            timeout_keep_alive=KEEP_ALIVE_TIMEOUT_SECONDS,
            h11_max_incomplete_event_size=MAX_HEADER_BYTES,
            ws=WS_PROTOCOL,
            ws_ping_interval=WS_PING_INTERVAL_SECONDS,
            ws_ping_timeout=WS_PING_TIMEOUT_SECONDS,
        )
        self._server = LoopLoggingServer(uvi_config, server_name='ui')
        self._thread = threading.Thread(
            target=self._server.run,
            name='drakkar-ui-server',
            daemon=True,
        )
        self._thread.start()
        # Verify the bind before declaring success. Without this a
        # port-in-use error dies silently inside the daemon thread while
        # the worker keeps running with no probe surface (/healthz,
        # /readyz). A bind failure is treated as fatal: raise so startup
        # fails visibly rather than half-succeeding. The
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
                    f'API server thread exited before serving — '
                    f'is {self._config.host}:{self._config.port} already in use?'
                )
            time.sleep(0.05)
        raise TimeoutError(f'API server did not start serving within {timeout}s')

    async def _resolve_ui_bundle(self, ui: UIConfig) -> ResolvedBundle | None:
        """Resolve the drakkar-ui bundle when ``ui.release.enabled``, else ``None``.

        Returns the full :class:`drakkar.uihost.ResolvedBundle` (directory +
        provenance + version) so the identity endpoint can report which UI
        release this worker serves.

        The resolution (cache → GitHub fetch) runs in a worker thread so a
        slow network fetch never blocks the main loop; it is bounded
        internally by ``UI_RESOLVE_TIMEOUT_SECONDS`` and is never fatal.

        Returns ``None`` when ``ui.release.enabled`` is off, when resolution
        errored, or when nothing could be resolved at all — an offline host
        whose cache is still empty. The worker then serves no pages; see
        :mod:`drakkar.uiserver.routes_spa`.
        """
        from drakkar.uihost import resolve

        if not ui.release.enabled:
            return None
        try:
            bundle = await asyncio.to_thread(resolve, ui.release)
        except Exception as exc:
            await logger.awarning('ui_resolve_failed', category='ui', error=str(exc))
            return None
        if bundle is None:
            return None
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
