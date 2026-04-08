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

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from drakkar.config import DebugConfig
from drakkar.recorder import EventRecorder

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
            from urllib.parse import quote

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
                'max_ui_rows': config.max_ui_rows,
                'ws_min_duration_ms': config.ws_min_duration_ms,
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
        if started and started.get('metadata'):
            try:
                meta = json.loads(started['metadata'])
                source_offsets = meta.get('source_offsets')
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

        # group by task name
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

            if name not in tasks:
                tasks[name] = {
                    'name': name,
                    'last_run_ts': entry['ts'],
                    'last_duration': entry['duration'],
                    'last_status': status,
                    'last_error': error,
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
