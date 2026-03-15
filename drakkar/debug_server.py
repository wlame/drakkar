"""Debug web UI for Drakkar workers — FastAPI + Jinja2 templates."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from drakkar.config import DebugConfig
from drakkar.recorder import EventRecorder

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _relative_time(ts: float | None) -> str:
    if ts is None:
        return "never"
    delta = time.time() - ts
    if delta < 60:
        return f"{delta:.0f}s ago"
    if delta < 3600:
        return f"{delta / 60:.0f}m ago"
    if delta < 86400:
        return f"{delta / 3600:.1f}h ago"
    return f"{delta / 86400:.1f}d ago"


def _format_ts(ts: float | None) -> str:
    if ts is None:
        return ""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def create_debug_app(
    config: DebugConfig,
    recorder: EventRecorder,
    drakkar_app: DrakkarApp,
) -> FastAPI:
    """Create the FastAPI debug application."""
    app = FastAPI(title="Drakkar Debug", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals['relative_time'] = _relative_time
    templates.env.globals['format_ts'] = _format_ts

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        return templates.TemplateResponse(request, "dashboard.html", {
            'worker_id': drakkar_app._worker_id,
            'uptime': time.monotonic() - drakkar_app._start_time,
            'stats': stats,
            'partition_count': len(processors),
            'partitions': sorted(processors.keys()),
            'pool_active': pool.active_count if pool else 0,
            'pool_max': pool.max_workers if pool else 0,
        })

    @app.get("/partitions", response_class=HTMLResponse)
    async def partitions(request: Request):
        summary = await recorder.get_partition_summary()
        processors = drakkar_app.processors
        for s in summary:
            pid = s['partition']
            proc = processors.get(pid)
            s['queue_size'] = proc.queue_size if proc else 0
            s['pending_offsets'] = proc.offset_tracker.pending_count if proc else 0
            s['is_live'] = pid in processors
        return templates.TemplateResponse(request, "partitions.html", {
            'worker_id': drakkar_app._worker_id,
            'summary': summary,
        })

    @app.get("/partitions/{partition_id}", response_class=HTMLResponse)
    async def partition_detail(
        request: Request,
        partition_id: int,
        page: int = Query(default=0, ge=0),
    ):
        limit = 50
        events = await recorder.get_events(
            partition=partition_id, limit=limit, offset=page * limit,
        )
        return templates.TemplateResponse(request, "partition_detail.html", {
            'worker_id': drakkar_app._worker_id,
            'partition_id': partition_id,
            'events': events,
            'page': page,
            'has_next': len(events) == limit,
        })

    @app.get("/executors", response_class=HTMLResponse)
    async def executors(request: Request):
        active = await recorder.get_active_tasks()
        now = time.time()
        for task in active:
            task['elapsed'] = now - task['ts'] if task.get('ts') else 0
        processors = drakkar_app.processors
        live_tasks = {}
        for proc in processors.values():
            for tid, t in proc._pending_tasks.items():
                live_tasks[tid] = {
                    'task_id': tid,
                    'args': t.args,
                    'partition': proc.partition_id,
                    'source_offsets': t.source_offsets,
                }
        return templates.TemplateResponse(request, "executors.html", {
            'worker_id': drakkar_app._worker_id,
            'active_from_db': active,
            'live_tasks': live_tasks,
            'pool_active': drakkar_app._executor_pool.active_count if drakkar_app._executor_pool else 0,
            'pool_max': drakkar_app._executor_pool.max_workers if drakkar_app._executor_pool else 0,
        })

    @app.get("/trace/{partition_id}/{offset}", response_class=HTMLResponse)
    async def trace(request: Request, partition_id: int, offset: int):
        events = await recorder.get_trace(partition_id, offset)
        return templates.TemplateResponse(request, "trace.html", {
            'worker_id': drakkar_app._worker_id,
            'partition_id': partition_id,
            'offset': offset,
            'events': events,
        })

    @app.get("/history", response_class=HTMLResponse)
    async def history(
        request: Request,
        partition: int | None = Query(default=None),
        event_type: str | None = Query(default=None),
        page: int = Query(default=0, ge=0),
    ):
        limit = 100
        events = await recorder.get_events(
            partition=partition,
            event_type=event_type,
            limit=limit,
            offset=page * limit,
        )
        return templates.TemplateResponse(request, "history.html", {
            'worker_id': drakkar_app._worker_id,
            'events': events,
            'page': page,
            'has_next': len(events) == limit,
            'filter_partition': partition,
            'filter_event_type': event_type,
        })

    return app


class DebugServer:
    """Manages the lifecycle of the debug FastAPI server."""

    def __init__(
        self,
        config: DebugConfig,
        recorder: EventRecorder,
        app: DrakkarApp,
    ):
        self._config = config
        self._recorder = recorder
        self._drakkar_app = app
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        fastapi_app = create_debug_app(
            self._config, self._recorder, self._drakkar_app,
        )
        uvi_config = uvicorn.Config(
            app=fastapi_app,
            host="0.0.0.0",
            port=self._config.port,
            log_level="warning",
        )
        self._server = uvicorn.Server(uvi_config)
        self._task = asyncio.create_task(self._server.serve())
        await logger.ainfo("debug_server_started", port=self._config.port)

    async def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        await logger.ainfo("debug_server_stopped")
