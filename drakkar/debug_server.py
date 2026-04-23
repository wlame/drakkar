"""Debug web UI for Drakkar workers — FastAPI + Jinja2 templates."""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from drakkar.config import DebugConfig
from drakkar.debug_runner import DebugRunner, ProbeInput
from drakkar.metrics import cache_gauge_snapshot
from drakkar.recorder import EventRecorder


# ``/api/live/arrange-tasks`` request body — kept at module scope so
# FastAPI's automatic "single Pydantic param = request body" detection
# fires. Nested class definitions inside the ``create_debug_app`` factory
# don't trigger the same heuristic and end up being treated as query
# parameters (surfaces as 422 "Field required" responses).
class _ArrangeTaskLookupRequest(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=5000)


# ``/api/live/sink-breakdown`` request body — also at module scope for the
# same reason. Aggregates ``produced`` events by ``output_topic`` (sink
# name) for a given (partition, offsets[]) tuple. Called from the
# completion-hook sidebars (Task Results, Message Results, Window
# Results) to show per-sink-type output counts on demand.
class _SinkBreakdownRequest(BaseModel):
    partition: int
    offsets: list[int] = Field(default_factory=list, max_length=5000)


# ``/api/debug/probe`` request body — module-scope per the same FastAPI
# heuristic as the two classes above. Mirrors ``ProbeInput`` from
# ``drakkar.debug_runner``. FastAPI requires the body model to be
# DEFINED in the same module as the endpoint function for its "single
# pydantic param = body" heuristic to fire; an imported model ends up
# being treated as a query parameter and surfaces as 422
# (``feedback/fastapi_body_model_scope``). The two-model layout is
# ugly but forced on us by FastAPI.
class _ProbeRequest(BaseModel):
    value: str = Field(max_length=10_000_000)
    key: str | None = Field(default=None, max_length=65_536)
    partition: int = Field(default=0, ge=0)
    offset: int = Field(default=0, ge=0)
    topic: str = Field(default='', max_length=65_536)
    timestamp: int | None = None
    use_cache: bool = False


# Extra headroom (in seconds) on top of ``2 * task_timeout_seconds`` for
# the probe's wall-clock timeout. Covers arrange + two round-trips of
# hook work + serialization overhead. Exposed at module scope so tests
# can monkeypatch it to a small value without plumbing a timeout arg
# through the endpoint signature.
PROBE_TIMEOUT_HEADROOM_SECONDS: float = 30.0


def _hook_flags(handler: object) -> dict[str, bool]:
    """Detect which completion hooks the user's handler overrides.

    The Live view uses this to render only the tabs that actually have
    semantic meaning for this handler — an empty "Window Results" tab on
    a handler that never implements ``on_window_complete`` is just noise.

    We compare bound-method identity against the base class's no-op
    implementation. A subclass that overrides the hook has a different
    function object reachable via ``type(handler).on_*``; one that
    inherits the default shares the same object. This works for:

      * standard subclasses of BaseDrakkarHandler (common case)
      * handlers that implement DrakkarHandler directly without
        subclassing — their on_* methods aren't the base class's, so
        they also register as "implemented"

    Handlers that use composition or decorators that wrap the method
    will still register as "implemented" — we err on the side of showing
    the tab rather than hiding it.
    """
    # Import here to avoid a circular import; handler imports debug_server
    # only via app.py's wiring, but the other direction is live.
    from drakkar.handler import BaseDrakkarHandler

    cls = type(handler)
    # ``getattr`` on the class (not the instance) so we compare unbound
    # function objects — bound methods would wrap with a different id
    # per-instance and break the identity check. A handler that somehow
    # lacks one of these attributes (shouldn't happen given Protocol
    # conformance) is treated as "not implemented".
    return {
        'task_complete': getattr(cls, 'on_task_complete', None) is not BaseDrakkarHandler.on_task_complete,
        'message_complete': getattr(cls, 'on_message_complete', None) is not BaseDrakkarHandler.on_message_complete,
        'window_complete': getattr(cls, 'on_window_complete', None) is not BaseDrakkarHandler.on_window_complete,
    }


if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

WS_DRAIN_SLEEP = 0.02  # seconds to sleep when WebSocket event queue is empty

TEMPLATES_DIR = Path(__file__).parent / 'templates'


def _format_ts(ts: float | None) -> str:
    if ts is None:
        return ''
    from datetime import datetime

    return datetime.fromtimestamp(ts, tz=UTC).strftime('%H:%M:%S')


def _format_ts_ms(ts: float | None) -> str:
    if ts is None:
        return ''
    from datetime import datetime

    return datetime.fromtimestamp(ts, tz=UTC).strftime('%H:%M:%S.%f')[:-3]


def _format_ts_full(ts: float | None) -> str:
    if ts is None:
        return ''
    from datetime import datetime

    return datetime.fromtimestamp(ts, tz=UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _format_uptime(seconds: float) -> str:
    """Format uptime as a human-readable string, scaling to the largest unit."""
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    if s < 3600:
        return f'{s // 60}m {s % 60}s'
    if s < 86400:
        h = s // 3600
        m = (s % 3600) // 60
        return f'{h}h {m}m'
    if s < 86400 * 30:
        d = s // 86400
        h = (s % 86400) // 3600
        return f'{d}d {h}h'
    if s < 86400 * 365:
        d = s // 86400
        mo = d // 30
        d_rem = d % 30
        return f'{mo}mo {d_rem}d'
    d = s // 86400
    y = d // 365
    d_rem = d % 365
    return f'{y}y {d_rem // 30}mo'


def _worker_group(name: str) -> str:
    """Derive a group key by stripping trailing numbers and separator.

    ``worker-1``  → ``worker``
    ``worker-vip-2`` → ``worker-vip``
    ``slow-worker-05`` → ``slow-worker``
    ``worker15`` → ``worker``
    """
    return re.sub(r'[-_]?\d+$', '', name) or name


def create_debug_app(
    config: DebugConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
) -> FastAPI:
    """Create the FastAPI debug application."""
    app = FastAPI(title='Drakkar Debug', docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.autoescape = True
    templates.env.globals['format_ts'] = _format_ts  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_ms'] = _format_ts_ms  # ty: ignore[invalid-assignment]
    templates.env.globals['format_ts_full'] = _format_ts_full  # ty: ignore[invalid-assignment]
    templates.env.globals['format_uptime'] = _format_uptime  # ty: ignore[invalid-assignment]

    def _get_sink_ui_links() -> list[dict[str, str]]:
        """Return deduplicated sink UI links for the nav header."""
        mgr = drakkar_app.sink_manager
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

    templates.env.globals['get_sink_ui_links'] = _get_sink_ui_links  # ty: ignore[invalid-assignment]

    def _is_cache_enabled() -> bool:
        """Template helper — returns True when the cache page should be visible.

        Used by ``base.html`` to conditionally render the Cache nav link.
        Keeping this as a lambda-style getter (not a captured boolean) so
        that a cache engine that's swapped in/out at runtime (unit tests
        frequently do this) is reflected immediately without a page reload.
        """
        return drakkar_app.cache_engine is not None

    templates.env.globals['is_cache_enabled'] = _is_cache_enabled  # ty: ignore[invalid-assignment]

    # --- Kafka-UI deep-link helper ---
    #
    # Builds a URL that opens Kafka-UI (the provectus tool) filtered to a
    # single Kafka message. Requires both ``kafka.ui_url`` and
    # ``kafka.ui_cluster_name`` in config — returns '' when either is
    # missing so callers (Jinja templates and JS) can treat it as a feature
    # toggle. The ``%3A%3A`` literal is the URL-encoded form of ``::``
    # that Kafka-UI expects in the seekTo parameter.

    def _kafka_ui_message_url(topic: str, partition: int, offset: int) -> str:
        kcfg = drakkar_app._config.kafka
        if not kcfg.ui_url or not kcfg.ui_cluster_name or not topic:
            return ''
        base = kcfg.ui_url.rstrip('/')
        cluster = quote(kcfg.ui_cluster_name, safe='')
        topic_q = quote(str(topic), safe='')
        seek = f'{int(partition)}%3A%3A{int(offset)}'
        return f'{base}/ui/clusters/{cluster}/all-topics/{topic_q}/messages?seekType=OFFSET&seekTo={seek}&limit=1'

    templates.env.globals['kafka_ui_message_url'] = _kafka_ui_message_url  # ty: ignore[invalid-assignment]
    templates.env.globals['kafka_source_topic'] = drakkar_app._config.kafka.source_topic  # ty: ignore[invalid-assignment]
    # The JS-rendered pages (history, live) need to build these URLs too.
    # Expose the raw bits so the templates can inject them into a JS
    # constants block once and let the renderers compose URLs per-row.
    templates.env.globals['kafka_ui_base'] = drakkar_app._config.kafka.ui_url.rstrip('/')  # ty: ignore[invalid-assignment]
    templates.env.globals['kafka_ui_cluster'] = drakkar_app._config.kafka.ui_cluster_name  # ty: ignore[invalid-assignment]

    # --- Auth dependency for sensitive endpoints ---

    async def _require_auth(
        request: Request,
        token: str | None = Query(default=None),
    ) -> None:
        """Check bearer token for protected endpoints (download, merge).

        Skipped when auth_token is empty (no auth configured).
        Accepts token via Authorization header or ?token= query parameter.
        """
        if not config.auth_token:
            return
        auth_header = request.headers.get('authorization', '')
        header_token = auth_header.removeprefix('Bearer ').strip() if auth_header.startswith('Bearer ') else ''
        if header_token == config.auth_token or token == config.auth_token:
            return
        raise HTTPException(status_code=401, detail='Invalid or missing auth token')

    async def _get_lag() -> dict[int, dict]:
        consumer = drakkar_app._consumer
        if not consumer or not drakkar_app.processors:
            return {}
        try:
            return await consumer.get_partition_lag(
                list(drakkar_app.processors.keys()),
            )
        except Exception:
            return {}

    # --- Prometheus link builder ---

    def _build_prometheus_links() -> dict:
        """Build Prometheus graph URLs for dashboard cards and metrics panel.

        Returns empty dicts/lists when prometheus_url is not configured.
        """
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

    @app.get('/', response_class=HTMLResponse)
    async def dashboard(request: Request):
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        consumer = drakkar_app._consumer
        partition_ids = sorted(processors.keys())
        total_lag = 0
        if consumer and partition_ids:
            try:
                total_lag = await consumer.get_total_lag(partition_ids)
            except Exception:
                pass
        # Expand custom link URL templates
        custom_links = []
        if config.custom_links:
            tpl_vars = {
                'worker_id': drakkar_app._worker_id,
                'cluster_name': drakkar_app._cluster_name or '',
                'metrics_port': str(drakkar_app._config.metrics.port),
                'debug_port': str(config.port),
            }
            for link in config.custom_links:
                url = link.get('url', '')
                for key, val in tpl_vars.items():
                    url = url.replace('{' + key + '}', val)
                custom_links.append({'name': link.get('name', url), 'url': url})

        return templates.TemplateResponse(
            request,
            'dashboard.html',
            {
                'worker_id': drakkar_app._worker_id,
                'uptime': time.monotonic() - drakkar_app._start_time,
                'stats': stats,
                'partition_count': len(processors),
                'partitions': partition_ids,
                'pool_active': pool.active_count if pool else 0,
                'pool_max': pool.max_executors if pool else 0,
                'total_lag': total_lag,
                'prom': _build_prometheus_links(),
                'custom_links': custom_links,
            },
        )

    @app.get('/partitions', response_class=HTMLResponse)
    async def partitions(request: Request):
        summary = await recorder.get_partition_summary()
        processors = drakkar_app.processors
        lag_data = await _get_lag()
        for s in summary:
            pid = s['partition']
            proc = processors.get(pid)
            s['queue_size'] = proc.queue_size if proc else 0
            s['pending_offsets'] = proc.offset_tracker.pending_count if proc else 0
            s['is_live'] = pid in processors
            lag = lag_data.get(pid, {})
            s['committed_offset'] = lag.get('committed', s.get('last_committed_offset'))
            s['high_watermark'] = lag.get('high_watermark')
            s['lag'] = lag.get('lag', 0)
        return templates.TemplateResponse(
            request,
            'partitions.html',
            {
                'worker_id': drakkar_app._worker_id,
                'summary': summary,
            },
        )

    @app.get('/partitions/{partition_id}', response_class=HTMLResponse)
    async def partition_detail(
        request: Request,
        partition_id: int,
        page: int = Query(default=0, ge=0),
    ):
        limit = 50
        events = await recorder.get_events(
            partition=partition_id,
            limit=limit,
            offset=page * limit,
        )
        return templates.TemplateResponse(
            request,
            'partition_detail.html',
            {
                'worker_id': drakkar_app._worker_id,
                'partition_id': partition_id,
                'events': events,
                'page': page,
                'has_next': len(events) == limit,
            },
        )

    @app.get('/live', response_class=HTMLResponse)
    async def live(request: Request):
        active = await recorder.get_active_tasks()
        now = time.time()
        for task in active:
            task['elapsed'] = now - task['ts'] if task.get('ts') else 0
        # split tasks: running (have task_started in DB) vs pending (no task_started yet)
        processors = drakkar_app.processors
        active_task_ids = {t['task_id'] for t in active}
        running_tasks = {}
        pending_tasks = {}
        for proc in processors.values():
            for tid, t in proc._pending_tasks.items():
                entry = {
                    'task_id': tid,
                    'args': t.args,
                    'partition': proc.partition_id,
                    'source_offsets': t.source_offsets,
                }
                if tid in active_task_ids:
                    running_tasks[tid] = entry
                else:
                    pending_tasks[tid] = entry

        # recently finished tasks
        finished = await recorder.get_events(
            event_type='task_completed',
            limit=config.max_ui_rows,
        )
        failed = await recorder.get_events(
            event_type='task_failed',
            limit=1000,
        )
        recent_finished = sorted(finished + failed, key=lambda e: e.get('ts', 0), reverse=True)[: config.max_ui_rows]

        # active arrange() calls
        arranging = []
        for proc in processors.values():
            if proc._arranging:
                arranging.append(
                    {
                        'partition': proc.partition_id,
                        'duration': round(now - proc._arrange_start, 2),
                        'message_count': len(proc._arrange_labels),
                        'labels': proc._arrange_labels[:10],
                    }
                )

        # ``partition_count`` powers the Arrange tab's "last N batches" cap
        # (3 x partition_count) so the live list stays stable-sized regardless
        # of how many partitions the broker has assigned to this worker.
        # ``hook_flags`` hides completion-hook tabs (Task/Message/Window
        # Results) for hooks the handler doesn't implement.
        return templates.TemplateResponse(
            request,
            'live.html',
            {
                'worker_id': drakkar_app._worker_id,
                'running_tasks': running_tasks,
                'pending_tasks': pending_tasks,
                'recent_finished': recent_finished,
                'arranging': arranging,
                'pool_active': drakkar_app._executor_pool.active_count if drakkar_app._executor_pool else 0,
                'pool_waiting': drakkar_app._executor_pool.waiting_count if drakkar_app._executor_pool else 0,
                'pool_max': drakkar_app._executor_pool.max_executors if drakkar_app._executor_pool else 0,
                'partition_count': len(drakkar_app.processors),
                'max_ui_rows': config.max_ui_rows,
                'ws_min_duration_ms': config.ws_min_duration_ms,
                'hook_flags': _hook_flags(drakkar_app.handler)
                if drakkar_app.handler
                else {
                    'task_complete': False,
                    'message_complete': False,
                    'window_complete': False,
                },
            },
        )

    @app.get('/task/{task_id}', response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str):
        # Strip retry composite key suffix (e.g. "task-abc:r1234567.89" → "task-abc")
        base_id = task_id.split(':r')[0] if ':r' in task_id else task_id
        events = await recorder.get_task_events(base_id)
        started = next((e for e in events if e['event'] == 'task_started'), None)
        completed = next((e for e in events if e['event'] == 'task_completed'), None)
        failed = next((e for e in events if e['event'] == 'task_failed'), None)
        finished = completed or failed
        duration = finished['duration'] if finished and finished.get('duration') else None
        if not duration and started and finished:
            duration = finished['ts'] - started['ts']
        import json

        source_offsets = None
        task_env = None
        if started and started.get('metadata'):
            try:
                meta = json.loads(started['metadata'])
                source_offsets = meta.get('source_offsets')
                task_env = meta.get('env')
            except (json.JSONDecodeError, TypeError):
                pass
        args = None
        if started and started.get('args'):
            try:
                args = json.loads(started['args'])
            except (json.JSONDecodeError, TypeError):
                args = started['args']
        labels = None
        if started and started.get('labels'):
            try:
                labels = json.loads(started['labels'])
            except (json.JSONDecodeError, TypeError):
                pass
        pid = (completed or failed or {}).get('pid') or (started or {}).get('pid')
        return templates.TemplateResponse(
            request,
            'task_detail.html',
            {
                'worker_id': drakkar_app._worker_id,
                'task_id': task_id,
                'events': events,
                'started': started,
                'completed': completed,
                'failed': failed,
                'duration': duration,
                'source_offsets': source_offsets,
                'args': args,
                'labels': labels,
                'task_env': task_env,
                'partition': started['partition'] if started else None,
                'pid': pid,
                'binary_path': drakkar_app._config.executor.binary_path,
            },
        )

    @app.get('/history', response_class=HTMLResponse)
    async def history(
        request: Request,
        partition: str | None = Query(default=None),
        event_type: str | None = Query(default=None),
        page: int = Query(default=0, ge=0),
    ):
        part_int = int(partition) if partition and partition.strip() else None
        evt_type = event_type if event_type and event_type.strip() else None
        limit = 100
        events = await recorder.get_events(
            partition=part_int,
            event_type=evt_type,
            limit=limit,
            offset=page * limit,
        )
        return templates.TemplateResponse(
            request,
            'history.html',
            {
                'worker_id': drakkar_app._worker_id,
                'events': events,
                'page': page,
                'has_next': len(events) == limit,
                'filter_partition': part_int,
                'filter_event_type': evt_type,
                'partitions': sorted(drakkar_app.processors.keys()),
                'max_ui_rows': config.max_ui_rows,
            },
        )

    @app.get('/sinks', response_class=HTMLResponse)
    async def sinks_page(request: Request):
        mgr = drakkar_app.sink_manager
        info = mgr.get_sink_info()
        all_stats = mgr.get_all_stats()
        sinks_data = []
        for item in info:
            key = (item['sink_type'], item['name'])
            stats = all_stats.get(key)
            sinks_data.append(
                {
                    **item,
                    'delivered_count': stats.delivered_count if stats else 0,
                    'delivered_payloads': stats.delivered_payloads if stats else 0,
                    'error_count': stats.error_count if stats else 0,
                    'retry_count': stats.retry_count if stats else 0,
                    'last_delivery_ts': stats.last_delivery_ts if stats else None,
                    'last_delivery_duration': stats.last_delivery_duration if stats else None,
                    'last_error': stats.last_error if stats else None,
                    'last_error_ts': stats.last_error_ts if stats else None,
                }
            )
        return templates.TemplateResponse(
            request,
            'sinks.html',
            {
                'worker_id': drakkar_app._worker_id,
                'sinks': sinks_data,
            },
        )

    # --- Debug databases page ---

    @app.get('/debug', response_class=HTMLResponse)
    async def debug_databases(request: Request):
        return templates.TemplateResponse(
            request,
            'debug.html',
            {
                'worker_id': drakkar_app._worker_id,
                'db_dir': config.db_dir,
                'config_summary': drakkar_app.config_summary,
            },
        )

    # --- Cache page + JSON API ---
    #
    # All cache routes 404 when the cache is disabled (``cache_engine`` is
    # None). This keeps stale bookmarks / open browser tabs from rendering a
    # half-broken page after a config change. The reader connection on the
    # engine is shared with ``Cache.get`` fallback — no additional thread is
    # spun up for UI queries; SELECTs run on the same aiosqlite worker thread.

    def _cache_reader_or_404():
        """Fetch the shared reader connection or raise 404.

        All cache routes (HTML + JSON) funnel through this so we have a
        single source of truth for "cache not active". Returns the aiosqlite
        connection; the caller uses it like any other reader DB.

        Uses the public ``reader_db`` property on ``CacheEngine`` rather
        than reaching into the underscore-prefixed attribute so the
        encapsulation of the engine stays intact.
        """
        engine = drakkar_app.cache_engine
        if engine is None or engine.reader_db is None:
            raise HTTPException(status_code=404, detail='Cache is disabled')
        return engine.reader_db

    @app.get('/api/debug/cache/entries')
    async def api_debug_cache_entries(
        # ``ge=0, le=1000`` enforces the bounds at the FastAPI layer —
        # requests outside the range get a 422 response instead of reaching
        # the handler. Default 200 mirrors the UI page size.
        limit: int = Query(default=200, ge=0, le=1000),
        offset: int = Query(default=0, ge=0),
        scope: str | None = Query(default=None),
        search: str | None = Query(default=None),
        expired_only: bool = Query(default=False),
    ):
        """Paginated listing of cache rows with optional filters.

        Query params:
          limit         — rows per page (default 200, enforced [0, 1000])
          offset        — pagination offset
          scope         — exact scope match (``local``/``cluster``/``global``)
          search        — substring match against key (case-sensitive)
          expired_only  — show only expired rows (``expires_at_ms <= now_ms``)

        Returns ``{entries, total, limit, offset}``; ``total`` is the count
        matching the filters (not the clamped-page length), so the UI can
        render "N of M" pagination without a second round-trip.
        """
        reader = _cache_reader_or_404()

        conditions: list[str] = []
        params: list = []
        if scope is not None:
            conditions.append('scope = ?')
            params.append(scope)
        if search:
            # SQL LIKE with a substring pattern (``%search%``). User-typed
            # input can contain literal ``%`` or ``_`` which LIKE would
            # otherwise interpret as wildcards. We pick ``|`` as the ESCAPE
            # char (not ``\`` — SQLite + Python string-escaping gets
            # brittle with backslashes) and prefix each wildcard char
            # with it.
            conditions.append("key LIKE ? ESCAPE '|'")
            safe_search = search.replace('|', '||').replace('%', '|%').replace('_', '|_')
            params.append('%' + safe_search + '%')
        if expired_only:
            now_ms = int(time.time() * 1000)
            # ``<= ?`` matches the inclusive cleanup convention in
            # ``drakkar.cache`` — an entry whose ``expires_at_ms`` equals
            # ``now_ms`` is expired and should surface in the expired_only
            # filter too.
            conditions.append('expires_at_ms IS NOT NULL AND expires_at_ms <= ?')
            params.append(now_ms)

        where = f'WHERE {" AND ".join(conditions)}' if conditions else ''

        # total count for pagination. A DB corruption or schema drift would
        # otherwise surface as "empty cache" in the UI — log at warning
        # so operators see the signal even when the UI masks the failure.
        try:
            async with reader.execute(f'SELECT COUNT(*) FROM cache_entries {where}', params) as cursor:
                row = await cursor.fetchone()
                total = row[0] if row else 0
        except Exception as exc:
            await logger.awarning(
                'debug_cache_entries_count_failed',
                category='debug',
                error=str(exc),
                where=where,
            )
            total = 0

        entries: list[dict] = []
        if limit > 0:
            query = (
                'SELECT key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                'expires_at_ms, origin_worker_id FROM cache_entries '
                f'{where} ORDER BY updated_at_ms DESC LIMIT ? OFFSET ?'
            )
            try:
                async with reader.execute(query, [*params, limit, offset]) as cursor:
                    columns = [d[0] for d in cursor.description]
                    rows = await cursor.fetchall()
                for r in rows:
                    entries.append(dict(zip(columns, r, strict=False)))
            except Exception as exc:
                await logger.awarning(
                    'debug_cache_entries_query_failed',
                    category='debug',
                    error=str(exc),
                    where=where,
                    limit=limit,
                    offset=offset,
                )
                entries = []

        return JSONResponse(
            {
                'entries': entries,
                'total': total,
                'limit': limit,
                'offset': offset,
            }
        )

    @app.get('/api/debug/cache/entry/{key:path}')
    async def api_debug_cache_entry(key: str):
        """Return a single entry by exact key, with the value decoded from JSON.

        Uses ``{key:path}`` so colons (a common separator in cache keys) and
        other URL-special chars pass through unchanged. 404 when the key
        doesn't exist. On JSON decode failure (corruption / legacy data),
        the ``raw_value`` field carries the original string for the UI to
        display as-is.
        """
        reader = _cache_reader_or_404()
        try:
            async with reader.execute(
                'SELECT key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                'expires_at_ms, origin_worker_id FROM cache_entries WHERE key = ?',
                (key,),
            ) as cursor:
                columns = [d[0] for d in cursor.description]
                row = await cursor.fetchone()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f'Failed to read cache entry: {exc}') from exc

        if row is None:
            raise HTTPException(status_code=404, detail='Cache entry not found')

        entry = dict(zip(columns, row, strict=False))
        # Try to decode the JSON value; on failure carry the raw string so
        # the UI can show something rather than 500ing the request.
        raw_value = entry.pop('value')
        try:
            entry['value'] = json.loads(raw_value)
            entry['raw_value'] = raw_value
        except (json.JSONDecodeError, TypeError):
            entry['value'] = None
            entry['raw_value'] = raw_value

        return JSONResponse(entry)

    @app.get('/api/debug/cache/stats')
    async def api_debug_cache_stats():
        """Return a snapshot of the four cache gauges.

        Values come from the live Prometheus gauges — same numbers you'd
        see in the /metrics scrape, just wrapped in a JSON envelope for
        the UI's stat cards. Reading a gauge is O(1); we never walk the
        DB or memory dict here.

        Delegates to ``metrics.cache_gauge_snapshot`` so the endpoint
        doesn't depend on prometheus_client internals (``_value.get()``
        was a private attribute and could break silently on a library
        upgrade).
        """
        if drakkar_app.cache_engine is None:
            raise HTTPException(status_code=404, detail='Cache is disabled')

        return JSONResponse(cache_gauge_snapshot())

    @app.get('/api/debug/databases', dependencies=[Depends(_require_auth)])
    async def api_debug_databases():
        """List all debug database files in db_dir with stats."""
        from drakkar.merge import scan_directory

        databases = scan_directory(config.db_dir)
        return JSONResponse(
            [
                {
                    'filename': db.filename,
                    'path': db.path,
                    'worker_name': db.worker_name,
                    'cluster_name': db.cluster_name,
                    'event_count': db.event_count,
                    'event_counts': db.event_counts,
                    'first_event_ts': db.first_event_ts,
                    'last_event_ts': db.last_event_ts,
                    'has_events': db.has_events,
                    'has_config': db.has_config,
                    'has_state': db.has_state,
                    'size_bytes': db.size_bytes,
                }
                for db in databases
            ]
        )

    @app.post('/api/debug/merge', dependencies=[Depends(_require_auth)])
    async def api_debug_merge(request: Request):
        """Merge selected database files into one."""
        import asyncio
        from datetime import datetime

        from drakkar.merge import merge_databases

        body = await request.json()
        filenames = body.get('filenames', [])
        if len(filenames) < 2:
            return JSONResponse({'error': 'Select at least 2 databases'}, status_code=400)

        # resolve to full paths, validate they exist in db_dir
        db_paths = []
        for fn in filenames:
            # prevent directory traversal
            if '/' in fn or '\\' in fn or fn.startswith('.'):
                return JSONResponse({'error': f'Invalid filename: {fn}'}, status_code=400)
            full = os.path.join(config.db_dir, fn)
            if not os.path.realpath(full).startswith(os.path.realpath(config.db_dir) + os.sep):
                return JSONResponse({'error': f'Invalid path: {fn}'}, status_code=400)
            if not os.path.isfile(full):
                return JSONResponse({'error': f'File not found: {fn}'}, status_code=404)
            db_paths.append(full)

        ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
        output_name = f'merged-{ts}.db'
        output_path = os.path.join(config.db_dir, output_name)

        result = await asyncio.to_thread(merge_databases, db_paths, output_path)

        return JSONResponse(
            {
                'filename': output_name,
                'worker_count': result.worker_count,
                'event_count': result.event_count,
                'state_count': result.state_count,
                'cluster_name': result.cluster_name,
                'source_files': result.source_files,
            }
        )

    @app.get('/api/debug/trace')
    async def api_debug_trace(
        partition: int = Query(),
        offset: int = Query(),
    ):
        """Trace a message across all workers in the same cluster."""
        events = await recorder.cross_trace(partition, offset)
        return JSONResponse(events)

    @app.get('/api/debug/label-keys')
    async def api_debug_label_keys():
        """Return distinct label keys found in events."""
        await recorder._flush()
        if not recorder._db:
            return JSONResponse([])
        try:
            query = """
                SELECT DISTINCT labels FROM events
                WHERE labels IS NOT NULL
                LIMIT 100
            """
            async with recorder._db.execute(query) as cursor:
                rows = await cursor.fetchall()
            keys: set[str] = set()
            for (labels_json,) in rows:
                try:
                    parsed = json.loads(labels_json)
                    keys.update(parsed.keys())
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
            return JSONResponse(sorted(keys))
        except Exception:
            return JSONResponse([])

    @app.get('/api/debug/trace-by-label')
    async def api_debug_trace_by_label(
        key: str = Query(),
        value: str = Query(),
    ):
        """Trace tasks by label value across all workers in the cluster."""
        events = await recorder.cross_trace_by_label(key, value)
        return JSONResponse(events)

    @app.get('/api/debug/metrics')
    async def api_debug_metrics():
        """Return all registered Prometheus metrics with current values."""
        from drakkar.metrics import collect_all_metrics

        return JSONResponse(collect_all_metrics())

    @app.get('/api/debug/periodic')
    async def api_debug_periodic():
        """Return periodic task run history from the flight recorder.

        Groups events by task name and returns the latest run, total counts,
        and recent history for each task.
        """
        await recorder._flush()
        if not recorder._db:
            return JSONResponse([])

        query = """
            SELECT ts, task_id, duration, exit_code, metadata
            FROM events
            WHERE event = 'periodic_run'
            ORDER BY ts DESC
            LIMIT 500
        """
        try:
            async with recorder._db.execute(query) as cursor:
                columns = [d[0] for d in cursor.description]
                rows = await cursor.fetchall()
        except Exception:
            return JSONResponse([])

        # group by task name. We also surface a per-task ``system: bool``
        # derived from the event's ``metadata.system``. Framework-internal
        # loops (cache.flush / cache.sync / cache.cleanup, etc.) set this to
        # True so the debug UI can render a [system] pill and operators can
        # distinguish them from user-defined ``@periodic`` handler methods.
        # When the key is absent (older rows, user tasks) we default to False
        # — the field is always present in the response for UI simplicity.
        tasks: dict[str, dict] = {}
        for row in rows:
            entry = dict(zip(columns, row, strict=False))
            name = entry['task_id']
            meta = {}
            if entry.get('metadata'):
                try:
                    meta = json.loads(entry['metadata'])
                except (json.JSONDecodeError, TypeError):
                    pass
            status = meta.get('status', 'ok')
            error = meta.get('error', '')
            # system flag: latest value wins if events disagree (shouldn't
            # happen under normal use, but we iterate ts-DESC so first seen
            # == latest event for the task)
            is_system = bool(meta.get('system', False))

            if name not in tasks:
                tasks[name] = {
                    'name': name,
                    'last_run_ts': entry['ts'],
                    'last_duration': entry['duration'],
                    'last_status': status,
                    'last_error': error,
                    'system': is_system,
                    'total_ok': 0,
                    'total_error': 0,
                    'recent': [],
                }
            t = tasks[name]
            if status == 'ok':
                t['total_ok'] += 1
            else:
                t['total_error'] += 1
            if len(t['recent']) < 20:
                t['recent'].append(
                    {
                        'ts': entry['ts'],
                        'duration': entry['duration'],
                        'status': status,
                        'error': error,
                    }
                )

        result = sorted(tasks.values(), key=lambda t: t['name'])
        return JSONResponse(result)

    @app.get('/debug/download/{filename}', dependencies=[Depends(_require_auth)])
    async def debug_download(filename: str):
        """Download a database file from db_dir."""
        # prevent directory traversal
        if '/' in filename or '\\' in filename or filename.startswith('.'):
            return JSONResponse({'error': 'Invalid filename'}, status_code=400)
        full = os.path.join(config.db_dir, filename)
        if not os.path.realpath(full).startswith(os.path.realpath(config.db_dir) + os.sep):
            return JSONResponse({'error': 'Invalid path'}, status_code=400)
        if not os.path.isfile(full):
            return JSONResponse({'error': 'File not found'}, status_code=404)
        return FileResponse(
            path=full,
            filename=filename,
            media_type='application/x-sqlite3',
        )

    # --- JSON API endpoints for JS-driven pages ---

    @app.get('/api/events')
    async def api_events(
        partitions: str | None = Query(default=None),
        event_types: str | None = Query(default=None),
        after_id: int = Query(default=0),
        limit: int = Query(default=200, le=10000),
    ):
        """Get events as JSON. Supports multiple partitions/types as comma-separated."""
        await recorder._flush()
        part_list = [int(p) for p in partitions.split(',') if p.strip()] if partitions else None
        type_list = [t.strip() for t in event_types.split(',') if t.strip()] if event_types else None

        if not recorder._db:
            return JSONResponse([])

        conditions = []
        params: list = []
        if part_list:
            placeholders = ','.join(['?'] * len(part_list))
            conditions.append(f'partition IN ({placeholders})')
            params.extend(part_list)
        if type_list:
            placeholders = ','.join(['?'] * len(type_list))
            conditions.append(f'event IN ({placeholders})')
            params.extend(type_list)
        if after_id > 0:
            conditions.append('id > ?')
            params.append(after_id)

        where = f'WHERE {" AND ".join(conditions)}' if conditions else ''
        query = f'SELECT * FROM events {where} ORDER BY id DESC LIMIT ?'
        params.append(limit)

        async with recorder._db.execute(query, params) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return JSONResponse([dict(zip(columns, row, strict=False)) for row in rows])

    @app.get('/api/recent-tasks')
    async def api_recent_tasks(minutes: int = Query(default=2)):
        """Get tasks from the last N minutes for timeline visualization."""
        await recorder._flush()
        if not recorder._db:
            return JSONResponse([])

        since = time.time() - (minutes * 60)
        query = """
            SELECT * FROM events
            WHERE event IN ('task_started', 'task_completed', 'task_failed')
            AND ts >= ?
            ORDER BY ts ASC
        """
        async with recorder._db.execute(query, [since]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            events = [dict(zip(columns, row, strict=False)) for row in rows]

        # group events into timeline entries — one entry per execution attempt.
        # retries (same task_id with multiple task_started) produce separate entries:
        # previous attempts get composite keys (task_id:r{ts}), the latest keeps
        # the original task_id so WS events can match it.
        tasks: dict[str, dict] = {}
        for e in events:
            tid = e.get('task_id')
            if not tid:
                continue

            if e['event'] == 'task_started':
                # if this task_id already has a current entry, archive it as a retry
                if tid in tasks:
                    old = tasks[tid]
                    archive_key = tid + ':r' + str(old['start_ts'])
                    tasks[archive_key] = old
                    old['task_id'] = archive_key
                    if old['end_ts'] is None:
                        old['end_ts'] = e['ts']
                        old['status'] = 'failed'

                slot = None
                meta = None
                if e.get('metadata'):
                    try:
                        meta = json.loads(e['metadata'])
                        slot = meta.get('slot')
                    except (json.JSONDecodeError, TypeError):
                        pass
                labels = None
                if e.get('labels'):
                    try:
                        labels = json.loads(e['labels'])
                    except (json.JSONDecodeError, TypeError):
                        pass
                task_env = meta.get('env') if meta else None
                tasks[tid] = {
                    'task_id': tid,
                    'partition': e.get('partition'),
                    'start_ts': e['ts'],
                    'end_ts': None,
                    'duration': None,
                    'status': 'running',
                    'args': e.get('args'),
                    'pid': e.get('pid'),
                    'slot': slot,
                    'labels': labels,
                    'env': task_env,
                }

            elif e['event'] in ('task_completed', 'task_failed'):
                if tid in tasks:
                    t = tasks[tid]
                    t['end_ts'] = e['ts']
                    t['status'] = 'completed' if e['event'] == 'task_completed' else 'failed'
                    t['duration'] = e.get('duration')
                    if e.get('pid'):
                        t['pid'] = e['pid']

        pool = drakkar_app._executor_pool
        max_lanes = pool.max_executors if pool else 8

        # Apply ws_min_duration_ms filtering: hide fast completed tasks
        # from the live UI, same as the WebSocket path. Running tasks
        # (duration unknown) and failed tasks (always visible) are kept.
        ws_threshold_s = recorder._config.ws_min_duration_ms / 1000.0
        result = []
        for t in tasks.values():
            if not t['start_ts']:
                continue
            if t['status'] == 'completed' and t['duration'] is not None and t['duration'] < ws_threshold_s:
                continue
            result.append(t)
        return JSONResponse({'tasks': result, 'lane_count': max_lanes})

    # Lookup-by-task-ID endpoint for the Arrange tab. Unlike /api/recent-tasks
    # this does NOT filter by ``minutes`` and does NOT apply the
    # ``ws_min_duration_ms`` threshold — callers pass exactly the task_ids
    # they want state for, and we return whatever the recorder has within
    # its retention window (default 24h). This fills the gap where batches
    # in the Arrange tab are older than the 10-min timeline window but
    # their task state is still authoritative in the DB.
    @app.post('/api/live/arrange-tasks')
    async def api_live_arrange_tasks(req: _ArrangeTaskLookupRequest):
        """Return the current state of specific task_ids as a map.

        Used by the Arrange tab's sidebar + list row progress. Payload:
        ``{"task_ids": ["rg-...", "rg-..."]}``. Response: ``{"<task_id>":
        {status, start_ts, end_ts, duration, partition, source_offsets,
        pid, labels, exit_code}}``. Unknown IDs are simply absent from
        the response map — callers treat missing keys as "not in DB yet".
        """
        task_ids = [t for t in req.task_ids if t]
        if not task_ids:
            return JSONResponse({})
        await recorder._flush()
        if not recorder._db or not recorder._config.store_events:
            return JSONResponse({})

        placeholders = ','.join(['?'] * len(task_ids))
        query = f"""
            SELECT task_id, event, ts, duration, partition, metadata,
                   exit_code, pid, args, labels
            FROM events
            WHERE task_id IN ({placeholders})
              AND event IN ('task_started', 'task_completed', 'task_failed')
            ORDER BY task_id, id ASC
        """
        async with recorder._db.execute(query, task_ids) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            events = [dict(zip(columns, row, strict=False)) for row in rows]

        result: dict[str, dict] = {}
        for e in events:
            tid = e['task_id']
            t = result.setdefault(
                tid,
                {
                    'task_id': tid,
                    'status': 'unknown',
                    'start_ts': None,
                    'end_ts': None,
                    'duration': None,
                    'partition': None,
                    'source_offsets': None,
                    'pid': None,
                    'args': None,
                    'labels': None,
                    'exit_code': None,
                },
            )
            if e['event'] == 'task_started':
                t['start_ts'] = e['ts']
                # ``running`` is provisional — overwritten on the next row
                # if a completion event exists for the same task_id.
                if t['status'] == 'unknown':
                    t['status'] = 'running'
                t['partition'] = e.get('partition')
                t['pid'] = e.get('pid')
                t['args'] = e.get('args')
                if e.get('metadata'):
                    try:
                        meta = json.loads(e['metadata'])
                        t['source_offsets'] = meta.get('source_offsets')
                    except (json.JSONDecodeError, TypeError):
                        pass
                if e.get('labels'):
                    try:
                        t['labels'] = json.loads(e['labels'])
                    except (json.JSONDecodeError, TypeError):
                        pass
            elif e['event'] in ('task_completed', 'task_failed'):
                t['end_ts'] = e['ts']
                t['status'] = 'completed' if e['event'] == 'task_completed' else 'failed'
                t['duration'] = e.get('duration')
                t['exit_code'] = e.get('exit_code')
                if e.get('pid'):
                    t['pid'] = e['pid']

        return JSONResponse(result)

    # ------------------------------------------------------------------
    # Completion-hook result feeds for the Live view's three new tabs:
    #   * Task Results     — one row per on_task_complete()  call
    #   * Message Results  — one row per on_message_complete() call
    #   * Window Results   — one row per on_window_complete() call
    # Each endpoint returns the most recent N rows ordered by ts DESC.
    # No joins: all the user-visible columns are already in the event's
    # metadata JSON (see recorder.record_task_complete etc.). Sink-type
    # breakdown is fetched lazily by the sidebar via /api/live/sink-breakdown.
    # ------------------------------------------------------------------

    async def _fetch_events(event_name: str, limit: int) -> list[dict]:
        """Common helper for the three completion-hook endpoints.

        Returns raw events (ts DESC, limited) as list of dicts, or empty
        list when recorder storage is disabled. Callers parse metadata
        themselves because each event type has different metadata shape.
        """
        await recorder._flush()
        if not recorder._db or not recorder._config.store_events:
            return []
        query = (
            'SELECT ts, task_id, partition, offset, duration, metadata '
            'FROM events WHERE event = ? ORDER BY id DESC LIMIT ?'
        )
        async with recorder._db.execute(query, (event_name, limit)) as cur:
            columns = [d[0] for d in cur.description]
            rows = await cur.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    def _parse_meta(raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @app.get('/api/live/task-results')
    async def api_live_task_results(limit: int = Query(default=200, ge=0, le=5000)):
        """Latest N ``task_complete`` events with their matching exec state.

        For each task_complete row we pair the subprocess-level outcome
        (task_completed or task_failed) by task_id so the UI can surface
        both the exec duration (how long the subprocess ran) and the hook
        duration (how long on_task_complete took). Exec state comes from
        a single extra SELECT with ``task_id IN (...)`` — one round-trip
        per page, not per row.
        """
        events = await _fetch_events('task_complete', limit)
        task_ids = [e['task_id'] for e in events if e.get('task_id')]
        # Batch lookup across three related event types in one query —
        # task_started carries ``source_offsets`` in metadata (essential
        # for rendering the message source in the UI), while
        # task_completed/task_failed carry the subprocess exit status.
        aux_by_id: dict[str, dict] = {}
        if task_ids and recorder._db:
            placeholders = ','.join(['?'] * len(task_ids))
            q = (
                f'SELECT task_id, event, duration, exit_code, metadata '
                f'FROM events WHERE task_id IN ({placeholders}) '
                f"AND event IN ('task_started', 'task_completed', 'task_failed')"
            )
            async with recorder._db.execute(q, task_ids) as cur:
                cols = [d[0] for d in cur.description]
                for row in await cur.fetchall():
                    ex = dict(zip(cols, row, strict=False))
                    entry = aux_by_id.setdefault(
                        ex['task_id'],
                        {'exec_duration': None, 'status': None, 'exit_code': None, 'source_offsets': None},
                    )
                    if ex['event'] == 'task_started':
                        started_meta = _parse_meta(ex.get('metadata'))
                        so = started_meta.get('source_offsets')
                        if isinstance(so, list):
                            entry['source_offsets'] = so
                    else:
                        # Last-write-wins on retries within the batch.
                        entry['exec_duration'] = ex.get('duration')
                        entry['status'] = 'completed' if ex['event'] == 'task_completed' else 'failed'
                        entry['exit_code'] = ex.get('exit_code')
        result = []
        for e in events:
            meta = _parse_meta(e.get('metadata'))
            aux = aux_by_id.get(e.get('task_id') or '', {})
            result.append(
                {
                    'ts': e['ts'],
                    'task_id': e.get('task_id'),
                    'partition': e.get('partition'),
                    'source_offsets': aux.get('source_offsets'),
                    'hook_duration': e.get('duration'),
                    'exec_duration': aux.get('exec_duration'),
                    'status': aux.get('status'),
                    'exit_code': aux.get('exit_code'),
                    'output_message_count': meta.get('output_message_count', 0),
                }
            )
        return JSONResponse(result)

    @app.get('/api/live/message-results')
    async def api_live_message_results(limit: int = Query(default=200, ge=0, le=5000)):
        """Latest N ``message_complete`` events.

        All summary data lives in metadata (task_count / succeeded /
        failed / replaced / output_message_count). In addition we pair
        each row with its matching ``consumed`` event (by partition +
        offset) so the response carries ``end_to_end_duration`` — the
        wall-clock time from poll to on_message_complete finish, which
        includes arrange time, task scheduling, subprocess execution,
        and hook runtime. For a message that was consumed multiple
        times (replay after restart), we pick the most recent consumed
        event whose ts is <= message_complete.ts.
        """
        events = await _fetch_events('message_complete', limit)

        # Batch lookup of consumed events for the exact (partition, offset)
        # set that appears in this batch of message_complete rows. Filters
        # by partition IN (...) AND offset IN (...) to let SQLite use the
        # column indexes; the Python-side tuple match handles the final
        # (partition, offset) pairing — cheap given the small row count.
        consumed_by_key: dict[tuple, list[float]] = {}
        if events and recorder._db:
            pairs = {
                (e['partition'], e['offset'])
                for e in events
                if e.get('partition') is not None and e.get('offset') is not None
            }
            if pairs:
                partitions = sorted({p for p, _ in pairs})
                offsets = sorted({o for _, o in pairs})
                pp = ','.join(['?'] * len(partitions))
                oo = ','.join(['?'] * len(offsets))
                q = (
                    f'SELECT partition, offset, ts FROM events '
                    f"WHERE event = 'consumed' "
                    f'AND partition IN ({pp}) AND offset IN ({oo})'
                )
                params: list = [*partitions, *offsets]
                async with recorder._db.execute(q, params) as cur:
                    for row in await cur.fetchall():
                        consumed_by_key.setdefault((row[0], row[1]), []).append(row[2])

        result = []
        for e in events:
            meta = _parse_meta(e.get('metadata'))
            end_to_end = None
            candidates = consumed_by_key.get((e.get('partition'), e.get('offset')))
            if candidates:
                mc_ts = e['ts']
                # Most recent consumed_ts that's <= message_complete ts.
                # A later consumed would be a subsequent re-poll; this
                # message_complete corresponds to the most-recent-prior
                # poll of the same (partition, offset).
                best = max((c for c in candidates if c <= mc_ts), default=None)
                if best is not None:
                    end_to_end = round(mc_ts - best, 4)
            result.append(
                {
                    'ts': e['ts'],
                    'partition': e.get('partition'),
                    'offset': e.get('offset'),
                    'duration': e.get('duration'),
                    'end_to_end_duration': end_to_end,
                    'task_count': meta.get('task_count', 0),
                    'succeeded': meta.get('succeeded', 0),
                    'failed': meta.get('failed', 0),
                    'replaced': meta.get('replaced', 0),
                    'output_message_count': meta.get('output_message_count', 0),
                }
            )
        return JSONResponse(result)

    @app.get('/api/live/window-results')
    async def api_live_window_results(limit: int = Query(default=200, ge=0, le=5000)):
        """Latest N ``window_complete`` events. Metadata carries
        window_id, task_count, output_message_count."""
        events = await _fetch_events('window_complete', limit)
        result = []
        for e in events:
            meta = _parse_meta(e.get('metadata'))
            result.append(
                {
                    'ts': e['ts'],
                    'partition': e.get('partition'),
                    'window_id': meta.get('window_id'),
                    'duration': e.get('duration'),
                    'task_count': meta.get('task_count', 0),
                    'output_message_count': meta.get('output_message_count', 0),
                }
            )
        return JSONResponse(result)

    # Shared DebugRunner instance. The runner holds an ``asyncio.Lock``
    # that serializes overlapping probes; keeping a single instance per
    # FastAPI app means that lock is actually shared across requests.
    # Built lazily on first use so tests that don't exercise the probe
    # endpoint don't pay the wiring cost (and so tests can freely swap
    # ``mock_app.handler`` / ``_executor_pool`` before the first call).
    # NOTE: The runner is built lazily on first call and then cached for
    # the life of the app. Tests that swap the handler or executor pool
    # AFTER the first probe request won't see their changes take effect.
    # Test fixtures swap these before touching the endpoint, so this is
    # fine in practice — but callers should be aware.
    _probe_runner: DebugRunner | None = None

    def _get_probe_runner() -> DebugRunner:
        # ``nonlocal`` lets this closure mutate the outer binding without
        # the list-of-one trick. The outer name stays a plain
        # ``DebugRunner | None`` so ty narrows cleanly after the check.
        nonlocal _probe_runner
        if _probe_runner is None:
            # The executor pool is created during ``DrakkarApp.run`` and
            # lives for the whole process; by the time the probe endpoint
            # is reachable it's always non-None. Guard here so ty's
            # ``ExecutorPool | None`` narrowing is happy.
            pool = drakkar_app._executor_pool
            if pool is None:
                raise HTTPException(status_code=503, detail='executor pool not ready')
            _probe_runner = DebugRunner(
                handler=drakkar_app.handler,
                executor_pool=pool,
                app_config=drakkar_app._config,
            )
        return _probe_runner

    @app.post('/api/debug/probe', dependencies=[Depends(_require_auth)])
    async def api_debug_probe(req: _ProbeRequest) -> JSONResponse:
        """Run a single-message probe through the live handler pipeline.

        The probe executes arrange → executor → on_task_complete →
        on_message_complete → on_window_complete exactly like the
        production path, but with zero side-effects (no sinks, no
        recorder rows, no cache writes, no offset commits). Concurrent
        requests serialize on the runner's internal ``asyncio.Lock``.

        Returns 200 with a ``DebugReport``. If the wall-clock timeout
        fires (``2 * task_timeout_seconds + PROBE_TIMEOUT_HEADROOM_SECONDS``),
        also returns 200 but with ``truncated=true`` and whatever partial
        state the runner had captured up to the cancellation point.
        """
        runner = _get_probe_runner()
        # Default empty topic to the configured source topic so handlers
        # that key on ``msg.topic`` see a realistic value. The model
        # itself accepts an empty topic to support callers that
        # deliberately want to probe with no topic set.
        topic = req.topic or drakkar_app._config.kafka.source_topic
        probe_input = ProbeInput(
            value=req.value,
            key=req.key,
            partition=req.partition,
            offset=req.offset,
            topic=topic,
            timestamp=req.timestamp,
            use_cache=req.use_cache,
        )
        # Timeout = 2x the per-task timeout + headroom. ``config`` here is
        # ``DebugConfig``; the executor timeout lives on the full
        # ``DrakkarConfig`` reachable via ``drakkar_app._config``.
        timeout = 2 * drakkar_app._config.executor.task_timeout_seconds + PROBE_TIMEOUT_HEADROOM_SECONDS
        # ``start_probe`` creates an asyncio.Task with the per-run
        # _RunState attached as an attribute. This makes the partial
        # report strictly scoped to THIS request — a concurrent probe
        # whose timeout fires can't accidentally read our in-flight
        # state, and vice versa. See the runner for details.
        run_task = runner.start_probe(probe_input)
        try:
            report = await asyncio.wait_for(run_task, timeout=timeout)
        except TimeoutError:
            # The runner's finally block has already restored handler.cache
            # by the time we get here (wait_for cancels run_task and awaits
            # its completion before re-raising). partial_report_for reads
            # the state attached to OUR task — not any other probe's.
            report = runner.partial_report_for(run_task)
        return JSONResponse(report.model_dump(mode='json'))

    @app.post('/api/live/sink-breakdown')
    async def api_live_sink_breakdown(req: _SinkBreakdownRequest):
        """Group ``produced`` events by ``output_topic`` (sink name) for
        a given (partition, offsets) filter.

        Called from the completion-hook sidebars. Task Results sidebar
        passes the task's source_offsets; Message Results passes
        ``[offset]``; Window Results passes the list of offsets covered.
        Response: ``{"<sink_name>": <count>}``. Empty map when the filter
        matches nothing (no fabrication of zero-count entries).
        """
        if not req.offsets:
            return JSONResponse({})
        await recorder._flush()
        if not recorder._db or not recorder._config.store_events:
            return JSONResponse({})
        placeholders = ','.join(['?'] * len(req.offsets))
        q = (
            f'SELECT output_topic, COUNT(*) as n FROM events '
            f"WHERE event = 'produced' AND partition = ? "
            f'AND offset IN ({placeholders}) GROUP BY output_topic'
        )
        params: list = [req.partition, *req.offsets]
        async with recorder._db.execute(q, params) as cur:
            rows = await cur.fetchall()
        out: dict[str, int] = {}
        for row in rows:
            topic = row[0] or '(unknown)'
            out[topic] = int(row[1])
        return JSONResponse(out)

    @app.get('/api/dashboard')
    async def api_dashboard():
        """Dashboard data as JSON for JS refresh."""
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        partition_ids = sorted(processors.keys())
        consumer = drakkar_app._consumer
        total_lag = 0
        if consumer and partition_ids:
            try:
                total_lag = await consumer.get_total_lag(partition_ids)
            except Exception:
                pass
        return JSONResponse(
            {
                'uptime': time.monotonic() - drakkar_app._start_time,
                'stats': stats,
                'partition_count': len(processors),
                'partitions': partition_ids,
                'pool_active': pool.active_count if pool else 0,
                'pool_max': pool.max_executors if pool else 0,
                'total_lag': total_lag,
            }
        )

    @app.get('/api/sinks')
    async def api_sinks():
        """Sink configuration and live delivery stats."""
        mgr = drakkar_app.sink_manager
        info = mgr.get_sink_info()
        all_stats = mgr.get_all_stats()
        result = []
        for item in info:
            key = (item['sink_type'], item['name'])
            stats = all_stats.get(key)
            result.append(
                {
                    **item,
                    'delivered_count': stats.delivered_count if stats else 0,
                    'delivered_payloads': stats.delivered_payloads if stats else 0,
                    'error_count': stats.error_count if stats else 0,
                    'retry_count': stats.retry_count if stats else 0,
                    'last_delivery_ts': stats.last_delivery_ts if stats else None,
                    'last_delivery_duration': stats.last_delivery_duration if stats else None,
                    'last_error': stats.last_error if stats else None,
                    'last_error_ts': stats.last_error_ts if stats else None,
                }
            )
        return JSONResponse(result)

    @app.get('/api/debug/processors')
    async def api_debug_processors():
        """Dump internal state of all partition processors for diagnostics."""
        result = {}
        for pid, proc in sorted(drakkar_app.processors.items()):
            tracker = proc.offset_tracker
            sorted_offsets = list(tracker._sorted_offsets[:20])
            offset_states = {o: str(tracker._offsets.get(o, '?')) for o in sorted_offsets}
            arrange_info = None
            if proc._arranging:
                arrange_info = {
                    'duration': round(time.time() - proc._arrange_start, 2),
                    'message_count': len(proc._arrange_labels),
                    'labels': proc._arrange_labels[:20],
                }
            entry: dict = {
                'queue_size': proc.queue_size,
                'inflight_count': proc.inflight_count,
                'arranging': proc._arranging,
                'arrange': arrange_info,
                'pending_count': tracker.pending_count,
                'completed_count': tracker.completed_count,
                'total_tracked': tracker.total_tracked,
                'last_committed': tracker.last_committed,
                'committable': tracker.committable(),
                'first_offsets': sorted_offsets,
                'offset_states': offset_states,
                'active_task_count': len(proc._active_tasks),
            }
            # show stuck task details
            stuck = []
            for task in proc._active_tasks:
                if not task.done():
                    frames = task.get_stack(limit=5)
                    stack_lines = []
                    for frame in frames:
                        stack_lines.append(f'{frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}')
                    stuck.append(
                        {
                            'name': task.get_name(),
                            'stack': stack_lines,
                        }
                    )
            if stuck:
                entry['stuck_tasks'] = stuck
            result[pid] = entry
        pool = drakkar_app._executor_pool
        return JSONResponse(
            {
                'processors': result,
                'pool_active': pool.active_count if pool else 0,
                'pool_waiting': pool.waiting_count if pool else 0,
                'pool_max': pool.max_executors if pool else 0,
            }
        )

    # --- Workers autodiscovery API ---

    @app.get('/api/workers')
    async def api_workers():
        """Discover live workers sharing the same db_dir, including self.

        Each worker gets a ``url`` field (debug_url if set, else http://ip:port),
        a ``cluster`` field from the stored cluster_name (falls back to
        auto-derived group from worker name), and ``is_current`` for self.

        Workers are sorted: clustered first (by cluster then name),
        unclustered at the end (sorted by name).
        """
        workers = await recorder.discover_workers()

        # add the current worker to the list
        current_entry = {
            'worker_name': drakkar_app._worker_id,
            'cluster_name': drakkar_app._cluster_name or None,
            'ip_address': None,
            'debug_port': config.port,
            'debug_url': config.debug_url or None,
        }
        workers.append(current_entry)

        for w in workers:
            w['url'] = w.get('debug_url') or f'http://{w.get("ip_address", "127.0.0.1")}:{w.get("debug_port", 8080)}/'
            w['cluster'] = w.get('cluster_name') or ''
            w['is_current'] = w.get('worker_name') == drakkar_app._worker_id

        # sort: clustered workers first (by cluster name, then worker name),
        # unclustered at the end sorted by worker name
        workers.sort(
            key=lambda w: (
                0 if w['cluster'] else 1,
                w['cluster'],
                w.get('worker_name', ''),
            )
        )
        return JSONResponse(workers)

    # --- WebSocket endpoint for live event streaming ---

    @app.websocket('/ws')
    async def ws_events(ws: WebSocket):
        """Stream recorder events to connected clients in real-time.

        Uses a thread-safe queue (stdlib queue.Queue) since the recorder
        writes from the main thread and Uvicorn runs in a separate thread.
        """
        import queue as queue_mod

        await ws.accept()
        q = recorder.subscribe()
        try:
            while True:
                # drain all available events from queue in one batch
                batch = []
                try:
                    batch.append(q.get(timeout=0.1))
                    # grab more without blocking
                    while len(batch) < 100:
                        batch.append(q.get_nowait())
                except queue_mod.Empty:
                    pass
                if not batch:
                    await asyncio.sleep(WS_DRAIN_SLEEP)
                    continue
                try:
                    for event in batch:
                        await ws.send_text(json.dumps(event, default=str))
                except Exception:
                    break
        except WebSocketDisconnect:
            pass
        finally:
            recorder.unsubscribe(q)

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
