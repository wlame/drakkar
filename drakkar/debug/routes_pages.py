"""HTML pages, health probes, top-level JSON APIs, and WebSocket endpoint.

Routes:
  * ``/healthz``                 — Kubernetes liveness probe.
  * ``/readyz``                  — Kubernetes readiness probe.
  * ``/``                        — dashboard page.
  * ``/partitions``              — partition list page.
  * ``/partitions/{partition_id}`` — partition detail page.
  * ``/task/{task_id}``          — task detail page.
  * ``/history``                 — events history page.
  * ``/sinks``                   — sinks summary page.
  * ``/api/dashboard``           — dashboard JSON for JS refresh.
  * ``/api/sinks``               — sink config + delivery stats JSON.
  * ``/api/workers``             — peer-worker discovery JSON.
  * ``/api/debug/processors``    — partition-processor diagnostics JSON.
  * ``/ws``                      — recorder-event WebSocket stream.

The factory ``create_pages_router(deps)`` returns a single ``APIRouter``
that the server module mounts onto the FastAPI app via
``app.include_router(...)``. Routes reach shared state through ``deps``.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from drakkar.concurrency import dispatch_to_loop
from drakkar.debug.server_helpers import origin_allowed

if TYPE_CHECKING:
    from drakkar.debug.server import DebugDeps

WS_DRAIN_SLEEP = 0.02  # seconds to sleep when WebSocket event queue is empty


def create_pages_router(deps: DebugDeps) -> APIRouter:
    """Build the router that owns HTML pages, top-level APIs, and the WS endpoint."""
    router = APIRouter()
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app
    templates = deps.templates

    async def _build_webapp_tile() -> dict | None:
        """Compose the dashboard "WebApp" tile, or ``None`` when webapp is off.

        Reads:
        * ``drakkar_webapp_inflight`` — current in-flight request count from
          the prometheus Gauge (via the private ``_value.get()`` accessor that
          the recorder tests already use).
        * ``webapp.clients`` config — name + per-client rpm cap. The webapp's
          rate-limit dependency persists no live counter, so the tile only
          surfaces the configured cap (operators alert on the rpm metric for
          actual rates).
        * Recorder events table — counts ``webapp_request_completed`` (success)
          and ``webapp_request_timeout`` / ``webapp_request_dropped_after_timeout``
          (error) rows in the last 60 seconds. The DB query goes through the
          shared ``flush_and_select`` helper so the reader connection stays
          on the main loop.
        """
        webapp_cfg = drakkar_app._config.webapp
        if not webapp_cfg.enabled:
            return None
        # Read the gauge directly. ``Gauge._value.get()`` is the single-process
        # accessor used by drakkar's existing recorder tests; we keep the same
        # convention here so debug-UI access stays consistent with metric tests.
        from drakkar.metrics import webapp_inflight

        try:
            inflight_count = int(webapp_inflight._value.get())
        except Exception:
            inflight_count = 0

        success_60s = 0
        error_60s = 0
        since = time.time() - 60.0
        success_query = "SELECT COUNT(*) FROM events WHERE event = 'webapp_request_completed' AND ts >= ?"
        error_query = (
            'SELECT COUNT(*) FROM events '
            "WHERE event IN ('webapp_request_timeout', 'webapp_request_dropped_after_timeout') "
            'AND ts >= ?'
        )
        try:
            success_result = await deps.flush_and_select(success_query, [since])
            if success_result is not None:
                _cols, rows = success_result
                if rows:
                    success_60s = int(rows[0][0] or 0)
            error_result = await deps.flush_and_select(error_query, [since])
            if error_result is not None:
                _cols, rows = error_result
                if rows:
                    error_60s = int(rows[0][0] or 0)
        except Exception:
            # Recorder may not be ready in startup-edge windows; surface
            # the tile with zeros rather than 500ing the dashboard.
            pass

        return {
            'inflight_count': inflight_count,
            'clients': [{'name': c.name, 'rpm_limit': c.rpm} for c in webapp_cfg.clients],
            'success_60s': success_60s,
            'error_60s': error_60s,
            'host': webapp_cfg.host,
            'port': webapp_cfg.port,
            'path': webapp_cfg.path,
        }

    # --- Kubernetes probes (unauthenticated by design) ---
    #
    # ``/healthz`` and ``/readyz`` intentionally skip the auth dependency:
    # Kubernetes probes have no facility for bearer tokens, and the
    # endpoints expose only liveness / readiness signals — nothing that
    # leaks message content, partition state, or operator credentials.
    # They are fast-path (no I/O, no recorder access) so the kubelet's
    # sub-second probe budget is respected even under load.
    #
    # Contract:
    #   - ``/healthz`` — liveness: always 200 while the process is running
    #     and the FastAPI loop is responsive. Kubernetes restarts the pod
    #     on failure.
    #   - ``/readyz`` — readiness: 200 only when ``DrakkarApp.is_ready`` is
    #     True AND every registered sink reports ``is_connected``. 503
    #     otherwise with a ``reasons`` list pointing at what's missing.
    #     Kubernetes removes the pod from service endpoints on failure
    #     (without restarting it).

    @router.get('/healthz')
    async def healthz() -> JSONResponse:
        """Liveness probe — returns 200 as long as the event loop is alive."""
        return JSONResponse({'status': 'ok'})

    @router.get('/readyz')
    async def readyz() -> JSONResponse:
        """Readiness probe — 200 iff the worker is ready AND all sinks are connected."""
        reasons: list[str] = []
        if not drakkar_app.is_ready:
            reasons.append('not_started')
        # ``sink_manager`` is always constructed in ``DrakkarApp.__init__``
        # and ``all_connected`` returns True for an empty manager, so we
        # can consult it directly. Startup validation rejects a zero-sink
        # config before the debug server is mounted.
        mgr = drakkar_app.sink_manager
        if not mgr.all_connected():
            for sink_id in mgr.disconnected_sink_names():
                reasons.append(f'sink_{sink_id}_not_connected')
        if reasons:
            return JSONResponse({'status': 'not_ready', 'reasons': reasons}, status_code=503)
        return JSONResponse({'status': 'ready'})

    # --- HTML pages ---

    @router.get('/', response_class=HTMLResponse)
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

        webapp_tile = await _build_webapp_tile()
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
                'prom': deps.build_prometheus_links(),
                'custom_links': custom_links,
                'webapp_tile': webapp_tile,
            },
        )

    @router.get('/partitions', response_class=HTMLResponse)
    async def partitions(request: Request):
        # ``get_partition_summary`` internally does ``self._db.execute(...)``;
        # dispatch to the main loop so the aiosqlite cursor stays there.
        summary = await dispatch_to_loop(recorder.get_partition_summary(), deps.drakkar_app.main_loop)
        processors = drakkar_app.processors
        lag_data = await deps.get_lag()
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
        webapp_tile = await _build_webapp_tile()
        return templates.TemplateResponse(
            request,
            'partitions.html',
            {
                'worker_id': drakkar_app._worker_id,
                'summary': summary,
                'webapp_tile': webapp_tile,
            },
        )

    @router.get('/partitions/{partition_id}', response_class=HTMLResponse)
    async def partition_detail(
        request: Request,
        partition_id: int,
        page: int = Query(default=0, ge=0),
    ):
        limit = 50
        events = await dispatch_to_loop(
            recorder.get_events(
                partition=partition_id,
                limit=limit,
                offset=page * limit,
            ),
            deps.drakkar_app.main_loop,
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

    @router.get('/task/{task_id}', response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str):
        # Strip retry composite key suffix (e.g. "task-abc:r1234567.89" → "task-abc")
        base_id = task_id.split(':r')[0] if ':r' in task_id else task_id
        events = await dispatch_to_loop(recorder.get_task_events(base_id), deps.drakkar_app.main_loop)
        started = next((e for e in events if e['event'] == 'task_started'), None)
        completed = next((e for e in events if e['event'] == 'task_completed'), None)
        failed = next((e for e in events if e['event'] == 'task_failed'), None)
        finished = completed or failed
        duration = finished['duration'] if finished and finished.get('duration') else None
        if not duration and started and finished:
            duration = finished['ts'] - started['ts']

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

        # ``origin`` is part of every event row (column added in Task 8 of
        # the webapp pipeline plan); HTTP-origin tasks carry the
        # ``client_name`` / ``request_id`` columns too. The template uses
        # these to swap the Partition/Offset header for Client/Request ID.
        origin_value = 'kafka'
        client_name = None
        request_id = None
        for ev in events:
            ev_origin = ev.get('origin')
            if ev_origin:
                origin_value = ev_origin
            if ev.get('client_name'):
                client_name = ev['client_name']
            if ev.get('request_id'):
                request_id = ev['request_id']
        # Pull the truncated webapp-request body (≤ 4KB) and the final
        # response, when the recorder captured them. Both come from the
        # ``webapp_request_received`` / ``webapp_request_completed`` rows'
        # metadata. ``body_bytes`` is the original size; when the recorder
        # only captured the size (current behavior — see
        # ``EventRecorder._compute_body_bytes``) we surface the size and a
        # "request body not recorded" notice via a ``None`` payload.
        webapp_request_body = None
        webapp_response_body = None
        if origin_value == 'http':
            for ev in events:
                if ev['event'] == 'webapp_request_received' and ev.get('metadata'):
                    try:
                        body_meta = json.loads(ev['metadata'])
                        webapp_request_body = body_meta.get('body')
                        if webapp_request_body is None and body_meta.get('body_bytes') is not None:
                            # Recorder logged the size but not the payload.
                            webapp_request_body = {
                                'body_bytes': body_meta['body_bytes'],
                                'recorded': False,
                            }
                    except (json.JSONDecodeError, TypeError):
                        pass
                elif ev['event'] == 'webapp_request_completed' and ev.get('metadata'):
                    try:
                        resp_meta = json.loads(ev['metadata'])
                        webapp_response_body = resp_meta.get('response')
                    except (json.JSONDecodeError, TypeError):
                        pass

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
                'origin': origin_value,
                'client_name': client_name,
                'request_id': request_id,
                'webapp_request_body': webapp_request_body,
                'webapp_response_body': webapp_response_body,
            },
        )

    @router.get('/history', response_class=HTMLResponse)
    async def history(
        request: Request,
        partition: str | None = Query(default=None),
        event_type: str | None = Query(default=None),
        origin: str = Query(default='all'),
        page: int = Query(default=0, ge=0),
    ):
        part_int = int(partition) if partition and partition.strip() else None
        evt_type = event_type if event_type and event_type.strip() else None
        # ``origin`` is a small enum (kafka/http/all). Default ``all`` keeps
        # historical behavior; any other value collapses to ``all`` so a
        # malformed query string can't 500 the page.
        origin_filter = origin if origin in ('kafka', 'http') else 'all'
        limit = 100
        events = await dispatch_to_loop(
            recorder.get_events(
                partition=part_int,
                event_type=evt_type,
                origin=origin_filter if origin_filter != 'all' else None,
                limit=limit,
                offset=page * limit,
            ),
            deps.drakkar_app.main_loop,
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
                'filter_origin': origin_filter,
                'partitions': sorted(drakkar_app.processors.keys()),
                'max_ui_rows': config.max_ui_rows,
            },
        )

    @router.get('/sinks', response_class=HTMLResponse)
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

    # --- Top-level JSON APIs ---

    @router.get('/api/dashboard')
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
        webapp_tile = await _build_webapp_tile()
        payload = {
            'uptime': time.monotonic() - drakkar_app._start_time,
            'stats': stats,
            'partition_count': len(processors),
            'partitions': partition_ids,
            'pool_active': pool.active_count if pool else 0,
            'pool_max': pool.max_executors if pool else 0,
            'total_lag': total_lag,
        }
        # Webapp tile is only included when ``webapp.enabled``. Keeping the
        # key absent (rather than ``None``) lets the JS dashboard treat its
        # presence as the feature flag without a separate boolean.
        if webapp_tile is not None:
            payload['webapp_tile'] = webapp_tile
        return JSONResponse(payload)

    @router.get('/api/sinks')
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

    @router.get('/api/debug/processors')
    async def api_debug_processors():
        """Dump internal state of all partition processors for diagnostics."""

        # Build the full per-processor snapshot on the main loop. These
        # containers (``_sorted_offsets``, ``_offsets``, ``_arrange_labels``,
        # ``_active_tasks``) are mutated exclusively by the main loop; a
        # list slice from another thread can shear while the main loop
        # rebalances. Collecting everything inside one coroutine pinned
        # to the main loop keeps the snapshot internally consistent.
        async def _snapshot():
            snap: dict = {}
            for pid, proc in sorted(drakkar_app.processors.items()):
                tracker = proc.offset_tracker
                sorted_offsets = list(tracker._sorted_offsets[:20])
                offset_states = {o: str(tracker._offsets.get(o, '?')) for o in sorted_offsets}
                arrange_info = None
                if proc._arranging:
                    arrange_info = {
                        'duration': round(time.time() - proc._arrange_start, 2),
                        'message_count': len(proc._arrange_labels),
                        'labels': list(proc._arrange_labels[:20]),
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
                # show stuck task details — ``task.get_stack()`` reads the
                # frame state of the coroutine as of now, so it must run
                # on the main loop where the task lives to get an
                # accurate snapshot.
                stuck = []
                for task in list(proc._active_tasks):
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
                snap[pid] = entry
            return snap

        result = await dispatch_to_loop(_snapshot(), deps.drakkar_app.main_loop)
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

    @router.get('/api/workers')
    async def api_workers():
        """Discover live workers sharing the same db_dir, including self.

        Each worker gets a ``url`` field (debug_url if set, else http://ip:port),
        a ``cluster`` field from the stored cluster_name (falls back to
        auto-derived group from worker name), and ``is_current`` for self.

        Workers are sorted: clustered first (by cluster then name),
        unclustered at the end (sorted by name).
        """
        # ``discover_workers`` only opens transient aiosqlite connections
        # against peer live-DB symlinks — it never touches ``recorder._db``
        # or any primitive bound to the main loop. No dispatch needed;
        # the connections created inside the coroutine are bound to
        # whichever loop invokes it, which is fine for a one-shot read.
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

    @router.websocket('/ws')
    async def ws_events(ws: WebSocket):
        """Stream recorder events to connected clients in real-time.

        Uses a thread-safe queue (stdlib queue.Queue) since the recorder
        writes from the main thread and Uvicorn runs in a separate thread.

        Authentication: when ``config.auth_token`` is set, the client must
        provide a matching token either via the ``Authorization: Bearer``
        header (non-browser clients) or the ``?token=`` query parameter
        (browsers, which cannot set custom headers on WS handshakes).

        Origin validation: when ``auth_token`` is set, the ``Origin``
        header (if present) must match the configured allowlist. With an
        empty allowlist we fall back to same-origin: the origin's host
        must equal the request's ``Host`` header. Absent ``Origin`` is
        treated as same-origin (non-browser clients typically don't send
        it). When ``auth_token`` is empty we skip both checks to preserve
        the dev workflow.
        """
        import queue as queue_mod

        # --- Auth gate (WebSocket) ---
        # FastAPI's Depends() works on websocket endpoints, but keeping the
        # auth check inline lets us call ws.close() with a specific 4xxx
        # code that the browser can surface — HTTPException during the
        # handshake drops the connection without a useful reason.
        if config.auth_token:
            auth_header = ws.headers.get('authorization', '')
            header_token = auth_header.removeprefix('Bearer ').strip() if auth_header.startswith('Bearer ') else ''
            query_token = ws.query_params.get('token')
            if not (deps.token_matches(header_token) or deps.token_matches(query_token)):
                # 4401: application-specific unauthorized (RFC 6455 reserves
                # 4000-4999 for app use). Browsers expose this code via the
                # WebSocket close event.
                await ws.close(code=4401, reason='unauthorized')
                return

            # --- Origin validation ---
            # Delegate to ``origin_allowed`` (module scope) so the
            # decision logic is directly unit-testable and the four
            # branches (absent origin / allowlist hit / allowlist miss /
            # same-origin fallback) are spelled out in one place.
            origin = ws.headers.get('origin')
            request_host = ws.headers.get('host', '')
            if not origin_allowed(origin, request_host, config):
                await ws.close(code=4403, reason='forbidden origin')
                return

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

    return router
