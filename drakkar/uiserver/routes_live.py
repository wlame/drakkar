"""Live view page + ``/api/live/*`` endpoints + ``/api/recent-tasks`` + ``/api/events``.

Routes:
  * ``/live``                          — live view HTML page.
  * ``/api/events``                    — paginated event JSON feed.
  * ``/api/recent-tasks``              — last-N-minutes timeline view.
  * ``/api/live/arrange-tasks``        — per-task lookup by task_id list.
  * ``/api/live/task-results``         — completion-hook task feed.
  * ``/api/live/message-results``      — completion-hook message feed.
  * ``/api/live/window-results``       — completion-hook window feed.
  * ``/api/live/sink-breakdown``       — per-sink output count for a task.

The request-body Pydantic models (``_ArrangeTaskLookupRequest``,
``_SinkBreakdownRequest``) MUST be at module scope for FastAPI's
"single Pydantic param = request body" heuristic to fire — an imported
model is treated as a query parameter and surfaces as 422 errors.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from drakkar.concurrency import dispatch_to_loop
from drakkar.uiserver.server_helpers import hook_flags

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps


# ``/api/live/arrange-tasks`` request body — kept at module scope so
# FastAPI's automatic "single Pydantic param = request body" detection
# fires. Nested class definitions inside a route factory would not
# trigger the same heuristic and would end up being treated as query
# parameters (surfaces as 422 "Field required" responses).
class _ArrangeTaskLookupRequest(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=5000)


# ``/api/live/sink-breakdown`` request body — also at module scope for
# the same reason. Aggregates ``produced`` events by ``output_topic``
# (sink name) for a given (partition, offsets[]) tuple.
class _SinkBreakdownRequest(BaseModel):
    partition: int
    offsets: list[int] = Field(default_factory=list, max_length=5000)


# Ceiling on the ``/api/recent-tasks?minutes=`` query parameter — kept in
# sync with ``UITimelineConfig.max_age_minutes``'s own ceiling (24h) so a
# caller sees FastAPI's validation error rather than a silent downstream
# clamp. The handler clamps further down to ``config.timeline.max_age_minutes``
# (a per-worker override, tighter by default at 60 minutes), and the row
# and event caps derived from ``config.timeline.history_factor`` keep the
# query viable on a high-fan-out worker even at the full window.
RECENT_TASKS_MAX_MINUTES = 1440

# Ceiling on ``/api/recent-tasks?limit=`` — shared with the *derived*
# default (``history_factor x max_executors``), which has no ceiling of its
# own at the config layer (both factors are operator-set positive ints with
# no upper bound). Without this shared cap a misconfigured
# ``history_factor`` flows straight into ``event_limit`` and a main-loop
# SQLite query, defeating the whole point of bounding the query.
RECENT_TASKS_MAX_LIMIT = 100_000

# Lane count (and history-factor multiplier) fallback when no executor pool
# is attached — a contract value the SPA/Go backend also assume, not just a
# local default.
DEFAULT_LANE_COUNT = 8


def create_live_router(deps: UIDeps, include_html: bool = True) -> APIRouter:
    """Build the router that owns the live view + completion-hook feeds.

    ``include_html=False`` (SPA mode) drops the ``/live`` Jinja page so the
    SPA catch-all owns it; the JSON feeds are unaffected.
    """
    # All live-data routes expose task args/output and partition state —
    # gate the whole router behind require_auth (no-op without a token).
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    # HTML page routes register on ``html``: the real router normally, or a
    # throwaway router (never mounted) when the SPA owns the page surface.
    html = router if include_html else APIRouter()
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app
    templates = deps.templates

    async def _live_overview_data() -> dict:
        """The live view's server-side snapshot.

        Shared by the ``/live`` HTML page and ``GET /api/v1/live/overview``
        so the page and the API never drift: running/pending task maps,
        in-flight Arrange calls, pool occupancy, UI tuning knobs, handler
        hook flags, and the Kafka-UI deep-link config.
        """
        # Bounded dispatch: when the main loop is wedged the overview must
        # still answer (degraded) — it carries pool_max, which the UI header
        # can get nowhere else, and a hung fetch left it rendering "0 slots".
        active = await deps.dispatch_bounded(recorder.get_active_tasks(), default=[])
        now = time.time()
        # split tasks: running (have task_started in DB) vs pending (no task_started yet)
        processors = drakkar_app.processors
        active_task_ids = {t['task_id'] for t in active}
        running_tasks: dict = {}
        pending_tasks: dict = {}

        # ``proc._pending_tasks``, ``_arranging``, ``_arrange_start``, and
        # ``_arrange_labels`` are mutated exclusively on the main loop.
        # Snapshot them via a small coroutine dispatched there so a list
        # slice doesn't shear while the main loop mutates the underlying
        # container.
        async def _snapshot_processors():
            snapshot: dict = {}
            arranging_data: list[dict] = []
            for proc in processors.values():
                pending_items = list(proc._pending_tasks.items())
                pid_entries: list[tuple[str, object, int, object]] = []
                for tid, t in pending_items:
                    pid_entries.append((tid, t.args, proc.partition_id, t.source_offsets))
                snapshot[proc.partition_id] = pid_entries
                if proc._arranging:
                    arranging_data.append(
                        {
                            'partition': proc.partition_id,
                            'duration': round(now - proc._arrange_start, 2),
                            'message_count': len(proc._arrange_labels),
                            'labels': list(proc._arrange_labels[:10]),
                        }
                    )
            return snapshot, arranging_data

        pending_snapshot, arranging = await deps.dispatch_bounded(_snapshot_processors(), default=({}, []))
        for _pid, entries in pending_snapshot.items():
            for tid, args, partition_id, source_offsets in entries:
                entry = {
                    'task_id': tid,
                    'args': args,
                    'partition': partition_id,
                    'source_offsets': source_offsets,
                }
                if tid in active_task_ids:
                    running_tasks[tid] = entry
                else:
                    pending_tasks[tid] = entry

        # ``partition_count`` powers the Arrange tab's "last N batches" cap
        # (3 x partition_count) so the live list stays stable-sized regardless
        # of how many partitions the broker has assigned to this worker.
        # ``hook_flags`` hides completion-hook tabs (Task/Message/Window
        # Results) for hooks the handler doesn't implement.
        kafka_cfg = drakkar_app._config.kafka
        overview = {
            'worker_id': drakkar_app._worker_id,
            'running_tasks': running_tasks,
            'pending_tasks': pending_tasks,
            'arranging': arranging,
            'pool_active': drakkar_app._executor_pool.active_count if drakkar_app._executor_pool else 0,
            'pool_waiting': drakkar_app._executor_pool.waiting_count if drakkar_app._executor_pool else 0,
            'pool_max': drakkar_app._executor_pool.max_executors if drakkar_app._executor_pool else 0,
            'partition_count': len(drakkar_app.processors),
            'max_ui_rows': config.max_rows,
            'ws_min_duration_ms': config.ws_min_duration_ms,
            'hook_flags': hook_flags(drakkar_app.handler)
            if drakkar_app.handler
            else {
                'task_complete': False,
                'message_complete': False,
                'window_complete': False,
            },
            # Kafka-UI deep-link config for the SPA (Jinja globals on the
            # HTML pages). Always strings; empty when unconfigured.
            'kafka_ui_base': kafka_cfg.ui_url.rstrip('/'),
            'kafka_ui_cluster': kafka_cfg.ui_cluster_name,
            'kafka_source_topic': kafka_cfg.source_topic,
        }
        # Key-presence-as-flag (same idiom as ``webapp_tile`` on the
        # dashboard): backends without an offload pool — the Go worker,
        # or this worker before lifecycle wiring — omit the key entirely
        # and the UI readout stays hidden. ``snapshot()`` is thread-safe,
        # so no main-loop dispatch is needed.
        if drakkar_app._offload_pool is not None:
            overview['offload'] = drakkar_app._offload_pool.snapshot()
        return overview

    @html.get('/live', response_class=HTMLResponse)
    async def live(request: Request):
        overview = await _live_overview_data()

        # Recent-finished rows are page-only (the SPA reads live feeds over
        # /api/events and the WS instead); both queries batch into a single
        # cross-thread dispatch.
        async def _read_finished():
            finished_rows = await recorder.get_events(
                event_type='task_completed',
                limit=config.max_rows,
            )
            failed_rows = await recorder.get_events(
                event_type='task_failed',
                limit=1000,
            )
            return finished_rows, failed_rows

        finished, failed = await dispatch_to_loop(_read_finished(), deps.drakkar_app.main_loop)
        recent_finished = sorted(finished + failed, key=lambda e: e.get('ts', 0), reverse=True)[: config.max_rows]

        return templates.TemplateResponse(
            request,
            'live.html',
            {**overview, 'recent_finished': recent_finished},
        )

    # v1-only contract endpoint (no legacy alias): the live page's
    # server-side snapshot as JSON for the static SPA.
    @router.get('/api/v1/live/overview')
    async def api_live_overview():
        """Live overview snapshot: tasks, arranging, pool, UI knobs, hook flags."""
        return JSONResponse(await _live_overview_data())

    @router.get('/api/events')
    async def api_events(
        partitions: str | None = Query(default=None),
        event_types: str | None = Query(default=None),
        after_id: int = Query(default=0),
        limit: int = Query(default=200, le=10000),
    ):
        """Get events as JSON. Supports multiple partitions/types as comma-separated."""
        # A malformed partitions CSV is a caller error, not a server bug —
        # reply 422 with the minimal FastAPI-style envelope (contract v1).
        try:
            part_list = [int(p) for p in partitions.split(',') if p.strip()] if partitions else None
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=[{'loc': ['query', 'partitions'], 'msg': 'Input should be a valid integer'}],
            ) from None
        type_list = [t.strip() for t in event_types.split(',') if t.strip()] if event_types else None

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

        result = await deps.flush_and_select(query, params)
        if result is None:
            return JSONResponse([])
        columns, rows = result
        return JSONResponse([dict(zip(columns, row, strict=False)) for row in rows])

    @router.get('/api/recent-tasks')
    async def api_recent_tasks(
        minutes: int = Query(default=2, ge=1, le=RECENT_TASKS_MAX_MINUTES),
        limit: int | None = Query(
            default=None,
            ge=1,
            le=RECENT_TASKS_MAX_LIMIT,
            description=(
                'Max tasks returned, newest by start time. Defaults to '
                'ui.timeline.history_factor x the executor pool max_executors '
                f'(x{DEFAULT_LANE_COUNT} when no pool is attached), capped at '
                f'{RECENT_TASKS_MAX_LIMIT}.'
            ),
        ),
    ):
        """Get tasks from the last N minutes for timeline visualization.

        Bounds keep this endpoint viable on a high-fan-out worker, where one
        source message can produce a thousand tasks and a ten-minute window
        can hold hundreds of thousands of rows:

        * an explicit column list instead of ``SELECT *`` — the ``stdout``
          and ``stderr`` columns hold captured subprocess output that the
          timeline never displays, and pulling them made the response size
          track total task output rather than task count;
        * a row cap derived from ``limit`` (or ``ui.timeline.history_factor``
          when ``limit`` is unset), taking the **most recent** events (hence
          the descending inner query) and re-sorting ascending for the retry
          grouping below, which depends on chronological order;
        * a ceiling on ``minutes``, clamped further down to
          ``ui.timeline.max_age_minutes``.

        Without these the query could not finish inside the main-loop
        dispatch timeout, and the endpoint returned an empty timeline with
        no indication that anything had gone wrong.
        """
        timeline_cfg = config.timeline
        minutes = min(minutes, timeline_cfg.max_age_minutes)
        pool = drakkar_app._executor_pool
        max_lanes = pool.max_executors if pool else DEFAULT_LANE_COUNT
        if limit is None:
            # ``history_factor`` and ``max_executors`` are both operator-set
            # positive ints with no config-layer ceiling; clamp their
            # product so a misconfigured value can't blow up the query below.
            limit = min(timeline_cfg.history_factor * max_lanes, RECENT_TASKS_MAX_LIMIT)
        # Two events per task in the common case; the margin covers retries,
        # which add a start event per attempt.
        event_limit = limit * 3

        since = time.time() - (minutes * 60)
        query = """
            SELECT * FROM (
                SELECT ts, event, partition, task_id, args, duration, metadata,
                       pid, labels, origin, client_name, request_id, stdout_size
                FROM events
                WHERE event IN ('task_started', 'task_completed', 'task_failed')
                AND ts >= ?
                ORDER BY ts DESC
                LIMIT ?
            ) ORDER BY ts ASC
        """
        result = await deps.flush_and_select(query, [since, event_limit])
        if result is None:
            # Degraded read (no reader connection, or the bounded main-loop
            # dispatch timed out). Keep the documented object shape — a bare
            # ``[]`` here made every client's ``payload.tasks`` iteration
            # throw — and flag it so the UI can hold the last good timeline
            # instead of drawing an empty one.
            return JSONResponse({'tasks': [], 'lane_count': max_lanes, 'truncated': False, 'unavailable': True})
        columns, rows = result
        events = [dict(zip(columns, row, strict=False)) for row in rows]
        # Hitting the cap means older tasks in the window were dropped. Say
        # so rather than letting the UI present a partial window as complete.
        truncated = len(events) >= event_limit

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
                    # Populated from the task_completed event below; stays
                    # null for running/failed tasks.
                    'stdout_size': None,
                    'slot': slot,
                    'labels': labels,
                    'env': task_env,
                    # Webapp-pipeline columns: ``origin`` defaults to
                    # ``'kafka'`` at the schema level, so the absence of
                    # the column on older recorder rows still yields a
                    # sensible value here. ``client_name`` /
                    # ``request_id`` are NULL for Kafka tasks.
                    'origin': e.get('origin') or 'kafka',
                    'client_name': e.get('client_name'),
                    'request_id': e.get('request_id'),
                }

            elif e['event'] in ('task_completed', 'task_failed'):
                if tid in tasks:
                    t = tasks[tid]
                    t['end_ts'] = e['ts']
                    t['status'] = 'completed' if e['event'] == 'task_completed' else 'failed'
                    t['duration'] = e.get('duration')
                    if e.get('pid'):
                        t['pid'] = e['pid']
                    if e['event'] == 'task_completed':
                        t['stdout_size'] = e.get('stdout_size')
                        # Contract v1.16: throughput-counted completions
                        # carry cost/speed in their metadata; surface them
                        # on the row so the timeline shows per-task speed
                        # without re-deriving. Absent for excluded tasks.
                        if e.get('metadata'):
                            try:
                                completed_meta = json.loads(e['metadata'])
                                if 'speed' in completed_meta:
                                    t['cost'] = completed_meta.get('cost')
                                    t['speed'] = completed_meta.get('speed')
                            except (json.JSONDecodeError, TypeError):
                                pass

        # Apply ws_min_duration_ms filtering: hide fast completed tasks
        # from the live UI, same as the WebSocket path. Running tasks
        # (duration unknown) and failed tasks (always visible) are kept.
        ws_threshold_s = recorder.config.ws_min_duration_ms / 1000.0
        tasks_result = []
        for t in tasks.values():
            if not t['start_ts']:
                continue
            if t['status'] == 'completed' and t['duration'] is not None and t['duration'] < ws_threshold_s:
                continue
            tasks_result.append(t)

        # ``limit`` bounds the response to the newest tasks by start time,
        # independent of the row-count-based ``event_limit`` cap above (that
        # one can drop an OLDER task's matching event out of the query
        # window entirely; this one trims the grouped, filtered result).
        trimmed = len(tasks_result) > limit
        if trimmed:
            tasks_result.sort(key=lambda t: t['start_ts'])
            tasks_result = tasks_result[-limit:]

        return JSONResponse(
            {
                'tasks': tasks_result,
                'lane_count': max_lanes,
                'truncated': truncated or trimmed,
            }
        )

    # Lookup-by-task-ID endpoint for the Arrange tab. Unlike /api/recent-tasks
    # this does NOT filter by ``minutes`` and does NOT apply the
    # ``ws_min_duration_ms`` threshold — callers pass exactly the task_ids
    # they want state for, and we return whatever the recorder has within
    # its retention window (default 24h). This fills the gap where batches
    # in the Arrange tab are older than the 10-min timeline window but
    # their task state is still authoritative in the DB.
    @router.post('/api/live/arrange-tasks')
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

        placeholders = ','.join(['?'] * len(task_ids))
        query = f"""
            SELECT task_id, event, ts, duration, partition, metadata,
                   exit_code, pid, args, labels,
                   origin, client_name, request_id
            FROM events
            WHERE task_id IN ({placeholders})
              AND event IN ('task_started', 'task_completed', 'task_failed')
            ORDER BY task_id, id ASC
        """

        # Short-circuit when recorder event storage is disabled — no point
        # flushing + SELECTing against an empty table. ``flush_and_select``
        # would still dispatch to the main loop and return rows, but they'd
        # always be empty.
        if not recorder.config.recorder.store_events:
            return JSONResponse({})
        query_result = await deps.flush_and_select(query, task_ids)
        if query_result is None:
            return JSONResponse({})
        columns, rows = query_result
        events = [dict(zip(columns, row, strict=False)) for row in rows]

        # ``by_id`` aggregates event rows per task_id — one entry per task.
        by_id: dict[str, dict] = {}
        for e in events:
            tid = e['task_id']
            t = by_id.setdefault(
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
                    # Webapp-pipeline columns. The first event row to
                    # populate them wins (origin is NOT NULL with default
                    # 'kafka', so it's always set; client_name /
                    # request_id are NULL for Kafka tasks).
                    'origin': 'kafka',
                    'client_name': None,
                    'request_id': None,
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
                # Origin / client_name / request_id propagate from the
                # task_started row (every recorder write site populates
                # them). Last write wins on retries within the batch,
                # matching the existing convention for ``pid`` etc.
                if e.get('origin'):
                    t['origin'] = e['origin']
                if e.get('client_name'):
                    t['client_name'] = e['client_name']
                if e.get('request_id'):
                    t['request_id'] = e['request_id']
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

        return JSONResponse(by_id)

    # ------------------------------------------------------------------
    # Completion-hook result feeds for the Live view's three tabs:
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
        if not recorder.config.recorder.store_events:
            return []
        query = (
            'SELECT ts, task_id, partition, offset, duration, metadata '
            'FROM events WHERE event = ? ORDER BY id DESC LIMIT ?'
        )
        result = await deps.flush_and_select(query, (event_name, limit))
        if result is None:
            return []
        columns, rows = result
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def _parse_meta(raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @router.get('/api/live/task-results')
    async def api_live_task_results(limit: int = Query(default=200, ge=0, le=5000)):
        """Latest N ``task_complete`` events with their matching exec state."""
        events = await _fetch_events('task_complete', limit)
        task_ids = [e['task_id'] for e in events if e.get('task_id')]
        # Batch lookup across three related event types in one query —
        # task_started carries ``source_offsets`` in metadata (essential
        # for rendering the message source in the UI), while
        # task_completed/task_failed carry the subprocess exit status.
        aux_by_id: dict[str, dict] = {}
        if task_ids and recorder.reader_db:
            placeholders = ','.join(['?'] * len(task_ids))
            q = (
                f'SELECT task_id, event, duration, exit_code, metadata '
                f'FROM events WHERE task_id IN ({placeholders}) '
                f"AND event IN ('task_started', 'task_completed', 'task_failed')"
            )

            # The aiosqlite connection lives on the main loop, so run
            # the SELECT + fetchall there and return plain Python data.
            # Routes through the reader connection so UI lookups don't
            # queue behind writer flushes.
            async def _read_aux():
                reader = recorder.reader_db
                if not reader:
                    return [], []
                async with reader.execute(q, task_ids) as cur:
                    cols = [d[0] for d in cur.description]
                    aux_rows = await cur.fetchall()
                return cols, aux_rows

            cols, aux_rows = await dispatch_to_loop(_read_aux(), deps.drakkar_app.main_loop)
            for row in aux_rows:
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

    @router.get('/api/live/message-results')
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
        # set that appears in this batch of message_complete rows.
        consumed_by_key: dict[tuple, list[float]] = {}
        if events and recorder.reader_db:
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

                async def _read_consumed():
                    reader = recorder.reader_db
                    if not reader:
                        return []
                    async with reader.execute(q, params) as cur:
                        return await cur.fetchall()

                for row in await dispatch_to_loop(_read_consumed(), deps.drakkar_app.main_loop):
                    consumed_by_key.setdefault((row[0], row[1]), []).append(row[2])

        result = []
        for e in events:
            meta = _parse_meta(e.get('metadata'))
            end_to_end = None
            candidates = consumed_by_key.get((e.get('partition'), e.get('offset')))
            if candidates:
                mc_ts = e['ts']
                # Most recent consumed_ts that's <= message_complete ts.
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

    @router.get('/api/live/window-results')
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

    @router.post('/api/live/sink-breakdown')
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
        if not recorder.config.recorder.store_events:
            return JSONResponse({})
        placeholders = ','.join(['?'] * len(req.offsets))
        q = (
            f'SELECT output_topic, COUNT(*) as n FROM events '
            f"WHERE event = 'produced' AND partition = ? "
            f'AND offset IN ({placeholders}) GROUP BY output_topic'
        )
        params: list = [req.partition, *req.offsets]

        result = await deps.flush_and_select(q, params)
        if result is None:
            return JSONResponse({})
        _columns, rows = result
        out: dict[str, int] = {}
        for row in rows:
            topic = row[0] or '(unknown)'
            out[topic] = int(row[1])
        return JSONResponse(out)

    return router
