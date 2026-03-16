"""Debug web UI for Drakkar workers — FastAPI + Jinja2 templates."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from drakkar.config import DebugConfig
from drakkar.recorder import EventRecorder

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

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


def create_debug_app(
    config: DebugConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
) -> FastAPI:
    """Create the FastAPI debug application."""
    app = FastAPI(title='Drakkar Debug', docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.autoescape = True
    templates.env.globals['format_ts'] = _format_ts
    templates.env.globals['format_ts_ms'] = _format_ts_ms
    templates.env.globals['format_ts_full'] = _format_ts_full

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

    @app.get('/', response_class=HTMLResponse)
    async def dashboard(request: Request):
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        lag_data = await _get_lag()
        total_lag = sum(v['lag'] for v in lag_data.values())
        return templates.TemplateResponse(
            request,
            'dashboard.html',
            {
                'worker_id': drakkar_app._worker_id,
                'uptime': time.monotonic() - drakkar_app._start_time,
                'stats': stats,
                'partition_count': len(processors),
                'partitions': sorted(processors.keys()),
                'pool_active': pool.active_count if pool else 0,
                'pool_max': pool.max_workers if pool else 0,
                'total_lag': total_lag,
                'lag_data': lag_data,
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

    @app.get('/executors', response_class=HTMLResponse)
    async def executors(request: Request):
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

        # recently finished tasks (last 5 minutes)
        finished = await recorder.get_events(
            event_type='task_completed',
            limit=50,
        )
        failed = await recorder.get_events(
            event_type='task_failed',
            limit=20,
        )
        recent_finished = sorted(finished + failed, key=lambda e: e.get('ts', 0), reverse=True)[:50]

        return templates.TemplateResponse(
            request,
            'executors.html',
            {
                'worker_id': drakkar_app._worker_id,
                'running_tasks': running_tasks,
                'pending_tasks': pending_tasks,
                'recent_finished': recent_finished,
                'pool_active': drakkar_app._executor_pool.active_count
                if drakkar_app._executor_pool
                else 0,
                'pool_max': drakkar_app._executor_pool.max_workers
                if drakkar_app._executor_pool
                else 0,
            },
        )

    @app.get('/trace/{partition_id}/{offset}', response_class=HTMLResponse)
    async def trace(request: Request, partition_id: int, offset: int):
        events = await recorder.get_trace(partition_id, offset)
        return templates.TemplateResponse(
            request,
            'trace.html',
            {
                'worker_id': drakkar_app._worker_id,
                'partition_id': partition_id,
                'offset': offset,
                'events': events,
            },
        )

    @app.get('/task/{task_id}', response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str):
        events = await recorder.get_task_events(task_id)
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
            },
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
        type_list = (
            [t.strip() for t in event_types.split(',') if t.strip()] if event_types else None
        )

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

        # group by task_id into timeline entries
        tasks: dict[str, dict] = {}
        now = time.time()
        for e in events:
            tid = e.get('task_id')
            if not tid:
                continue
            if tid not in tasks:
                tasks[tid] = {
                    'task_id': tid,
                    'partition': e.get('partition'),
                    'start_ts': None,
                    'end_ts': None,
                    'duration': None,
                    'status': 'running',
                    'args': e.get('args'),
                    'pid': e.get('pid'),
                }
            t = tasks[tid]
            if e['event'] == 'task_started':
                t['start_ts'] = e['ts']
            elif e['event'] in ('task_completed', 'task_failed'):
                t['end_ts'] = e['ts']
                t['status'] = 'completed' if e['event'] == 'task_completed' else 'failed'
                t['duration'] = e.get('duration')
                if e.get('pid'):
                    t['pid'] = e['pid']

        # compute relative positions for timeline
        timeline_start = now - (minutes * 60)
        timeline_span = minutes * 60

        # sort by start time for lane assignment
        sorted_tasks = sorted(
            [t for t in tasks.values() if t['start_ts']],
            key=lambda t: t['start_ts'],
        )

        # assign tasks to executor lanes (slots), capped at max_workers
        pool = drakkar_app._executor_pool
        max_lanes = pool.max_workers if pool else 8
        lane_end_times: list[float] = [0.0] * max_lanes
        result = []
        for t in sorted_tasks:
            start_pct = max(0, (t['start_ts'] - timeline_start) / timeline_span * 100)
            end_ts = t['end_ts'] or now
            end_pct = min(100, (end_ts - timeline_start) / timeline_span * 100)
            width_pct = max(0.15, end_pct - start_pct)

            # find the lane with the earliest end time (most idle slot)
            lane = min(range(max_lanes), key=lambda i: lane_end_times[i])
            lane_end_times[lane] = end_ts

            t['lane'] = lane
            t['start_pct'] = round(start_pct, 2)
            t['width_pct'] = round(width_pct, 2)
            result.append(t)

        return JSONResponse({'tasks': result, 'lane_count': max_lanes})

    @app.get('/api/dashboard')
    async def api_dashboard():
        """Dashboard data as JSON for JS refresh."""
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        lag_data = await _get_lag()
        total_lag = sum(v['lag'] for v in lag_data.values())
        return JSONResponse(
            {
                'uptime': time.monotonic() - drakkar_app._start_time,
                'stats': stats,
                'partition_count': len(processors),
                'partitions': sorted(processors.keys()),
                'pool_active': pool.active_count if pool else 0,
                'pool_max': pool.max_workers if pool else 0,
                'total_lag': total_lag,
                'lag_data': {str(k): v for k, v in lag_data.items()},
            }
        )

    return app


class DebugServer:
    """Manages the lifecycle of the debug FastAPI server."""

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
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        fastapi_app = create_debug_app(
            self._config,
            self._recorder,
            self._drakkar_app,
        )
        uvi_config = uvicorn.Config(
            app=fastapi_app,
            host='0.0.0.0',
            port=self._config.port,
            log_level='warning',
        )
        self._server = uvicorn.Server(uvi_config)
        self._task = asyncio.create_task(self._server.serve())
        await logger.ainfo('debug_server_started', category='debug', port=self._config.port)

    async def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
        await logger.ainfo('debug_server_stopped', category='debug')
