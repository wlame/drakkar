"""Health probes, top-level JSON APIs, and the WebSocket endpoint.

Routes:
  * ``/healthz``                    — Kubernetes liveness probe (public).
  * ``/readyz``                     — Kubernetes readiness probe (public).
  * ``/ws``                         — recorder-event WebSocket stream (public).
  * ``/api/v1/dashboard``           — dashboard JSON.
  * ``/api/v1/partitions``          — partition list JSON.
  * ``/api/v1/task/{task_id}``      — task detail JSON.
  * ``/api/v1/identity``            — worker identity + config summary.
  * ``/api/v1/sinks``               — sink config + delivery stats JSON.
  * ``/api/v1/workers``             — peer-worker discovery JSON.
  * ``/api/v1/debug/processors``    — partition-processor diagnostics JSON.

The server-rendered pages this module used to hold (``/``, ``/partitions``,
``/history``, ``/sinks``) and the unprefixed ``/api/*`` aliases they called
were removed in v1.19: the UI is one versioned drakkar-ui bundle, served by
``routes_spa``, and every JSON endpoint lives under ``/api/v1``.

The factory ``create_pages_router(deps)`` returns two leaf ``APIRouter``s
(public probes/WS + auth-gated everything else) that the server module
mounts onto the FastAPI app via ``app.include_router(...)``. Routes reach
shared state through ``deps``.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from drakkar.concurrency import dispatch_to_loop
from drakkar.recorder import queries
from drakkar.uiserver.server_helpers import backend_version, origin_allowed

if TYPE_CHECKING:
    from drakkar.config import UIDocsConfig, UITimelineConfig
    from drakkar.uiserver.server import UIDeps

# Idle backoff bounds for the WebSocket drain loop. The loop polls a
# thread-safe queue (the recorder writes from the main loop, uvicorn runs on
# its own thread), so it cannot simply await the queue.
#
# The sleep is adaptive rather than fixed. A fixed 20ms cost 50 wakeups per
# second per connection even on a completely idle worker — ten open tabs
# meant 500 pointless wakeups/s on the UI thread. Backing off to
# WS_DRAIN_SLEEP_MAX while nothing arrives makes idle dashboards nearly free,
# and resetting to WS_DRAIN_SLEEP_MIN on the first event keeps live latency
# unchanged while events are actually flowing (under load the queue is never
# empty and the loop never sleeps at all).
#
# The alternative — signalling the UI loop from the recorder with
# ``call_soon_threadsafe`` — was rejected: it makes the cost scale with the
# EVENT rate rather than the idle rate, which is backwards for a worker
# emitting thousands of events per second.
WS_DRAIN_SLEEP_MIN = 0.02
WS_DRAIN_SLEEP_MAX = 0.25

# Events drained into a single frame. The batch is sent as ONE JSON frame
# rather than one frame per event: a fan-out-heavy worker produces events in
# bursts, and per-event framing cost a WebSocket header, a syscall and a
# separate browser-side parse + reactive update for each one.
WS_BATCH_MAX = 100


def _timeline_wire(cfg: UITimelineConfig) -> dict[str, object]:
    """Serialize the timeline config for /api/v1/identity (when always a list, only bound roles)."""
    rules = []
    for rule in cfg.color_rules:
        conditions = []
        for c in rule.when:
            entry: dict[str, object] = {'op': c.op}
            if c.label:
                entry['label'] = c.label
            else:
                entry['field'] = c.field
            if c.value is not None:
                entry['value'] = c.value
            conditions.append(entry)
        rules.append({'name': rule.name, 'when': conditions, 'color': rule.color})
    labels = {role: key for role, key in cfg.labels.model_dump().items() if key}
    out: dict[str, object] = {
        'history_factor': cfg.history_factor,
        'max_age_minutes': cfg.max_age_minutes,
        'color_rules': rules,
        'labels': labels,
    }
    events = []
    for e in cfg.events:
        if not e.enabled:
            continue
        entry: dict[str, object] = {'name': e.name, 'kind': e.kind, 'color': e.color}
        if e.kind == 'marker':
            entry['line'] = e.line
        if e.label:
            entry['label'] = e.label
        entry['show'] = e.show
        if e.link:
            entry['link'] = e.link
        entry['action'] = e.action
        events.append(entry)
    if events:
        out['events'] = events
    return out


# Where the built-in ``{docs}`` link base resolves to. Same-origin path (the
# static mount this worker serves), unlike the absolute URLs user-configured
# bases carry — see the contract's "Built-in docs link base".
DOCS_LINK_BASE = '/docs'


def _docs_wire(cfg: UIDocsConfig) -> dict[str, object] | None:
    """Serialize the operator docs site for /api/v1/identity, or None when it is off.

    ``None`` keeps the ``docs`` key out of the payload entirely: the SPA
    reads its presence as the feature flag, the same presence-as-flag
    convention the dashboard tiles and ``_timeline_wire``'s ``events`` use.
    """
    if not cfg.site_dir:
        return None
    anchors: list[dict[str, object]] = []
    for anchor in cfg.anchors:
        # Only the selector fields the operator actually set — the model
        # defaults the other four to '' and sending those would make every
        # anchor look like it matched five contexts.
        match = {field: value for field, value in anchor.match.model_dump().items() if value}
        entry: dict[str, object] = {'match': match, 'path': anchor.path}
        if anchor.title:
            entry['title'] = anchor.title
        anchors.append(entry)
    return {'title': cfg.title, 'anchors': anchors}


def _link_bases_wire(cfg: UIDocsConfig, link_bases: dict[str, str]) -> dict[str, str]:
    """User link bases plus the built-in ``docs`` entry when the docs site is served.

    Config load rejects a user base named ``docs``, so the injection can
    never shadow one.
    """
    if not cfg.site_dir:
        return link_bases
    return {**link_bases, 'docs': DOCS_LINK_BASE}


def create_pages_router(deps: UIDeps) -> tuple[APIRouter, APIRouter]:
    """Build the routers that own the top-level APIs and the WS endpoint.

    Returns two leaf routers — ``(public, gated)`` — for the app to include
    directly. Every gated route goes through ``require_auth`` (a no-op when
    ``auth_token`` is empty). The probes must stay public for the kubelet;
    the WebSocket runs its own auth handshake inside the endpoint so it can
    reply with a proper 4401 close code.

    The two routers are deliberately NOT combined behind a wrapper
    ``include_router``: newer FastAPI includes routers lazily, so a nesting
    router would hide its routes from the app-level table.
    """
    # Public router: probes + WS (WS authenticates internally).
    public = APIRouter()
    # Everything else requires auth when a token is configured.
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app

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
        * Recorder events table — three buckets in the last 60 seconds:
          ``ok`` (``webapp_request_completed``),
          ``err`` (``webapp_request_timeout`` /
          ``webapp_request_dropped_after_timeout``), and
          ``rejected`` (``webapp_request_rate_limited`` /
          ``webapp_request_auth_failed``). The third bucket exists because
          a worker that's saturating a client's rpm cap (or refusing
          unknown tokens) is still doing useful work — surfacing those
          counts as zero in ``ok`` AND zero in ``err`` made the tile look
          dead during normal high-rate-limit traffic. The DB query goes
          through the shared ``flush_and_select`` helper so the reader
          connection stays on the main loop.
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

        since = time.time() - 60.0
        tiles = dict.fromkeys((key for key, _ in queries.WEBAPP_RATE_TILES), 0)
        try:
            for key, event_names in queries.WEBAPP_RATE_TILES:
                query, params = queries.event_count_query(event_names=event_names, since=since)
                result = await deps.flush_and_select(query, params)
                if result is not None:
                    _cols, rows = result
                    if rows:
                        tiles[key] = int(rows[0][0] or 0)
        except Exception:
            # Recorder may not be ready in startup-edge windows; surface
            # the tile with zeros rather than 500ing the dashboard.
            pass

        return {
            'inflight_count': inflight_count,
            'clients': [{'name': c.name, 'rpm_limit': c.rpm} for c in webapp_cfg.clients],
            **tiles,
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

    @public.get('/healthz')
    async def healthz() -> JSONResponse:
        """Liveness probe — returns 200 as long as the event loop is alive."""
        return JSONResponse({'status': 'ok'})

    @public.get('/readyz')
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
        # A partition whose loop gave up is stalled for the life of this
        # process: its queue is never drained and its offsets are never
        # committed. Failing readiness is what gets the pod replaced and
        # the partition reassigned to a worker that can serve it — without
        # this the worker looks healthy while silently processing nothing.
        for pid, processor in sorted(drakkar_app.processors.items()):
            if processor.is_dead:
                reasons.append(f'partition_{pid}_processor_died')
        if reasons:
            return JSONResponse({'status': 'not_ready', 'reasons': reasons}, status_code=503)
        return JSONResponse({'status': 'ready'})

    async def _partitions_data() -> list[dict]:
        """Per-partition summary rows enriched with live processor state and lag.

        Shared by the ``/partitions`` HTML page and ``GET /api/v1/partitions``
        so the page and the API never drift. Rows are recorder aggregates
        (``get_partition_summary`` columns) plus ``is_live`` / ``queue_size`` /
        ``pending_offsets`` / ``committed_offset`` / ``high_watermark`` /
        ``lag``, sorted by partition.
        """
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
        summary.sort(key=lambda s: s['partition'])
        return summary

    # v1-only contract endpoint (no legacy alias): the partitions table as
    # JSON for the static SPA. ``[]`` when nothing has been recorded.
    @router.get('/api/v1/partitions')
    async def api_partitions():
        """Per-partition summary rows as JSON, sorted by partition."""
        return JSONResponse(await _partitions_data())

    async def _task_detail_data(task_id: str) -> dict:
        """One task's reconstructed lifecycle from its recorded events.

        Shared by the ``/task/{task_id}`` HTML page and
        ``GET /api/v1/task/{task_id}`` so the page and the API never drift.
        ``task_id`` is echoed back as requested; the recorder lookup strips
        the ``:r…`` retry composite-key suffix to the base id.
        """
        events = await dispatch_to_loop(
            recorder.get_task_events(queries.base_task_id(task_id)),
            deps.drakkar_app.main_loop,
        )
        detail = queries.build_task_detail(task_id, events)
        # The configured binary is app state, not recorder state, so it is
        # added here rather than inside the reconstruction.
        detail['binary_path'] = drakkar_app._config.executor.binary_path
        return detail

    # v1-only contract endpoint (no legacy alias): one task's full lifecycle
    # as JSON for the static SPA. stdout/stderr live inside the event rows.
    @router.get('/api/v1/task/{task_id}')
    async def api_task_detail(task_id: str):
        """Single-task detail as JSON; ``:r…`` retry suffixes resolve to the base task."""
        return JSONResponse(await _task_detail_data(task_id))

    # --- Top-level JSON APIs ---

    def _build_dashboard_links() -> dict | None:
        """The dashboard ``links`` payload, or ``None`` when nothing is configured.

        Key presence is the feature flag (like ``webapp_tile``): the key
        appears only when ``prometheus_url`` is configured or ``custom_links``
        is non-empty. ``card_links`` / ``worker_links`` / ``cluster_links``
        come from ``build_prometheus_links`` verbatim (empty containers when
        only custom links are configured); ``custom_links`` passes the
        configured dicts through unchanged.
        """
        if not config.prometheus_url and not config.custom_links:
            return None
        return {**deps.build_prometheus_links(), 'custom_links': config.custom_links}

    @router.get('/api/v1/dashboard')
    async def api_dashboard():
        """Dashboard data as JSON for JS refresh."""
        stats = await recorder.get_stats()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool
        partition_ids = sorted(processors.keys())
        total_lag = await deps.get_total_lag(partition_ids)
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
        # Same presence-as-flag convention for the Prometheus/custom links.
        links = _build_dashboard_links()
        if links is not None:
            payload['links'] = links
        return JSONResponse(payload)

    # v1-only contract endpoint (no legacy alias): worker identity + the
    # one-line config summary for the SPA's debug-page banner. v1.2 adds
    # the backend flavor/version and the served drakkar-ui bundle so the
    # SPA header popover can show the full version picture.
    @router.get('/api/v1/identity')
    async def api_identity():
        """Worker identity: id, cluster, config summary, and versions."""
        payload: dict[str, object] = {
            'worker_id': drakkar_app._worker_id,
            'cluster': drakkar_app._cluster_name or None,
            'config_summary': drakkar_app.config_summary,
            'backend': 'python',
            'backend_version': backend_version(),
            'ui_version': deps.ui_version,
            'ui_source': deps.ui_source,
            'link_bases': _link_bases_wire(config.docs, config.link_bases),
            'custom_renderers': bool(config.custom_renderers_path),
            'timeline': _timeline_wire(config.timeline),
        }
        # v1.22: absent rather than null when no docs site is configured.
        docs = _docs_wire(config.docs)
        if docs is not None:
            payload['docs'] = docs
        return JSONResponse(payload)

    @router.get('/api/v1/sinks')
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

    @router.get('/api/v1/debug/processors')
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

    @router.get('/api/v1/workers')
    async def api_workers():
        """Discover live workers sharing the same db_dir, including self.

        Each worker gets a ``url`` field (debug_url if set, else http://ip:port),
        a ``cluster`` field from the stored cluster_name (falls back to
        auto-derived group from worker name), and ``is_current`` for self.
        Discovered peers carry ``last_seen_ts``/``online`` liveness fields
        from the recorder; the current worker is always online (it is
        answering this request) with ``last_seen_ts`` = now.

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
            'debug_url': config.public_url or None,
            # The current worker is serving this very response, so it is
            # online by definition — no heartbeat lookup needed.
            'last_seen_ts': time.time(),
            'online': True,
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

    @public.websocket('/ws')
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

        # Optional subscription filter: ``?events=task_started,task_completed``.
        # A page that names the events it renders stops paying for the rest at
        # the fan-out, before the event is ever encoded or queued. Omitting the
        # parameter streams everything, which is what non-browser clients and
        # older UI bundles expect.
        events_param = ws.query_params.get('events')
        event_types = [e.strip() for e in events_param.split(',') if e.strip()] if events_param else None
        sub = recorder.subscribe(event_types)

        # Starlette only surfaces a client disconnect through receive(), and
        # this handler is send-only — so WebSocketDisconnect could never fire
        # and a closed browser tab's coroutine (plus its 10k-slot queue, which
        # can pin whole stdout/stderr payloads the recorder has already
        # flushed) survived until the next send happened to fail. On a quiet
        # worker that could be minutes, or never. This watcher makes the
        # disconnect observable immediately.
        disconnected = asyncio.Event()

        async def _watch_disconnect() -> None:
            try:
                while True:
                    message = await ws.receive()
                    if message.get('type') == 'websocket.disconnect':
                        break
            except Exception:
                pass
            finally:
                disconnected.set()

        watcher = asyncio.create_task(_watch_disconnect())
        idle_sleep = WS_DRAIN_SLEEP_MIN
        try:
            while not disconnected.is_set():
                # Drain what is already queued, without ever blocking. The
                # previous q.get(timeout=0.1) was a stdlib queue.Queue call —
                # a real OS-level block inside an async handler. The UI server
                # runs uvicorn on ONE thread with ONE loop, so during that
                # wait nothing else on this server could run: not other
                # WebSocket clients, not /healthz, not /readyz. An idle
                # dashboard tab was enough to trigger it, and the Kubernetes
                # probes share the loop.
                #
                # ``drain_encoded`` yields JSON text, and the encoding is
                # memoized on an event object shared by every subscriber —
                # so a tenth open tab does not cost a tenth serialization
                # pass over the same events.
                batch = sub.drain_encoded(WS_BATCH_MAX)
                if not batch:
                    await asyncio.sleep(idle_sleep)
                    idle_sleep = min(idle_sleep * 2, WS_DRAIN_SLEEP_MAX)
                    continue
                idle_sleep = WS_DRAIN_SLEEP_MIN
                # ``dropped`` tells the client it lost events, so it can
                # resync deliberately instead of drifting. Reading it AFTER
                # the drain means a drop is never reported before the events
                # that preceded it.
                frame = f'{{"dropped":{sub.take_dropped()},"events":[{",".join(batch)}]}}'
                try:
                    await ws.send_text(frame)
                except Exception:
                    break
        except WebSocketDisconnect:
            pass
        finally:
            watcher.cancel()
            recorder.unsubscribe(sub)

    return public, router
