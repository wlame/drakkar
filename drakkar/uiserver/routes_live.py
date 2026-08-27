"""Live-view JSON: ``/api/v1/live/*``, ``/api/v1/recent-tasks``, ``/api/v1/events``.

These back the drakkar-ui Live page; the page itself is served by
``routes_spa`` from the bundle, not rendered here.

Routes:
  * ``/api/v1/events``                    — paginated event JSON feed.
  * ``/api/v1/recent-tasks``              — last-N-minutes timeline view.
  * ``/api/v1/live/arrange-tasks``        — per-task lookup by task_id list.
  * ``/api/v1/live/task-results``         — completion-hook task feed.
  * ``/api/v1/live/message-results``      — completion-hook message feed.
  * ``/api/v1/live/window-results``       — completion-hook window feed.
  * ``/api/v1/live/sink-breakdown``       — per-sink output count for a task.

The request-body Pydantic models (``_ArrangeTaskLookupRequest``,
``_SinkBreakdownRequest``) MUST be at module scope for FastAPI's
"single Pydantic param = request body" heuristic to fire — an imported
model is treated as a query parameter and surfaces as 422 errors.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from drakkar.concurrency import dispatch_to_loop
from drakkar.recorder import queries
from drakkar.uiserver.server_helpers import hook_flags

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps


# ``/api/v1/live/arrange-tasks`` request body — kept at module scope so
# FastAPI's automatic "single Pydantic param = request body" detection
# fires. Nested class definitions inside a route factory would not
# trigger the same heuristic and would end up being treated as query
# parameters (surfaces as 422 "Field required" responses).
class _ArrangeTaskLookupRequest(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=5000)


# ``/api/v1/live/sink-breakdown`` request body — also at module scope for
# the same reason. Aggregates ``produced`` events by ``output_topic``
# (sink name) for a given (partition, offsets[]) tuple.
class _SinkBreakdownRequest(BaseModel):
    partition: int
    offsets: list[int] = Field(default_factory=list, max_length=5000)


# Ceiling on the ``/api/v1/recent-tasks?minutes=`` query parameter — kept in
# sync with ``UITimelineConfig.max_age_minutes``'s own ceiling (24h) so a
# caller sees FastAPI's validation error rather than a silent downstream
# clamp. The handler clamps further down to ``config.timeline.max_age_minutes``
# (a per-worker override, tighter by default at 60 minutes), and the row
# and event caps derived from ``config.timeline.history_factor`` keep the
# query viable on a high-fan-out worker even at the full window.
RECENT_TASKS_MAX_MINUTES = 1440

# Ceiling on ``/api/v1/recent-tasks?limit=`` — shared with the *derived*
# default (``history_factor x max_executors``), which has no ceiling of its
# own at the config layer (both factors are operator-set positive ints with
# no upper bound). Without this shared cap a misconfigured
# ``history_factor`` flows straight into ``event_limit`` and a main-loop
# SQLite query, defeating the whole point of bounding the query.
RECENT_TASKS_MAX_LIMIT = 100_000

# Lane count (and history-factor multiplier) fallback when no executor pool
# is attached — a contract value the SPA assumes too, not just a local
# default.
DEFAULT_LANE_COUNT = 8


def create_live_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns the live view + completion-hook feeds."""
    # All live-data routes expose task args/output and partition state —
    # gate the whole router behind require_auth (no-op without a token).
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app

    async def _live_overview_data() -> dict:
        """The live view's server-side snapshot.

        Backs ``GET /api/v1/live/overview``: running/pending task counts,
        in-flight Arrange calls, pool occupancy, UI tuning knobs, handler
        hook flags, and the Kafka-UI deep-link config.
        """
        now = time.time()
        processors = drakkar_app.processors
        pool = drakkar_app._executor_pool

        # ``proc._pending_tasks``, ``_arranging``, ``_arrange_start``, and
        # ``_arrange_labels`` are mutated exclusively on the main loop.
        # Snapshot them via a small coroutine dispatched there so a read
        # doesn't shear while the main loop mutates the underlying container.
        #
        # Cost matters here: one source message can fan out to a thousand
        # tasks, so anything per-in-flight-task runs up to six figures per
        # poll. The running/pending split therefore iterates the POOL's
        # running ids (bounded by ``max_executors``) and probes each
        # processor's pending dict, rather than walking every in-flight task
        # — and it counts instead of building a dict per task. Contract
        # v1.20 turned both fields into integers for exactly this reason.
        async def _snapshot_processors():
            arranging_data: list[dict] = []
            in_flight = 0
            for proc in processors.values():
                in_flight += len(proc._pending_tasks)
                if proc._arranging:
                    arranging_data.append(
                        {
                            'partition': proc.partition_id,
                            'duration': round(now - proc._arrange_start, 2),
                            'message_count': len(proc._arrange_labels),
                            'labels': list(proc._arrange_labels[:10]),
                        }
                    )
            # The webapp ingress shares this pool, so a running id is only
            # partition work when some processor is tracking it. Counting
            # every running id would push ``pending`` negative.
            running = 0
            if pool is not None:
                for task_id in pool.running_task_ids:
                    if any(task_id in proc._pending_tasks for proc in processors.values()):
                        running += 1
            return running, max(in_flight - running, 0), arranging_data

        # Bounded dispatch: when the main loop is wedged the overview must
        # still answer (degraded) — it carries pool_max, which the UI header
        # can get nowhere else, and a hung fetch left it rendering "0 slots".
        running_tasks, pending_tasks, arranging = await deps.dispatch_bounded(
            _snapshot_processors(), default=(0, 0, [])
        )

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
        # dashboard): a worker with no offload pool — including this one
        # before lifecycle wiring — omits the key entirely and the UI
        # readout stays hidden. ``snapshot()`` is thread-safe,
        # so no main-loop dispatch is needed.
        if drakkar_app._offload_pool is not None:
            overview['offload'] = drakkar_app._offload_pool.snapshot()
        return overview

    # v1-only contract endpoint: the live page's
    # server-side snapshot as JSON for the static SPA.
    @router.get('/api/v1/live/overview')
    async def api_live_overview():
        """Live overview snapshot: tasks, arranging, pool, UI knobs, hook flags."""
        return JSONResponse(await _live_overview_data())

    @router.get('/api/v1/events')
    async def api_events(
        partitions: str | None = Query(default=None),
        event_types: str | None = Query(default=None),
        origin: str | None = Query(default=None),
        after_id: int = Query(default=0),
        limit: int = Query(default=200, le=10000),
    ):
        """Get events as JSON. Supports multiple partitions/types as comma-separated.

        ``origin`` (``kafka`` / ``http``) splits Kafka-origin tasks from
        webapp requests — the History page's origin radio.
        """
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

        query, params = queries.events_query(
            partitions=part_list,
            event_types=type_list,
            origin=origin,
            after_id=after_id,
            limit=limit,
        )
        result = await deps.flush_and_select(query, params)
        if result is None:
            return JSONResponse([])
        columns, rows = result
        return JSONResponse([dict(zip(columns, row, strict=False)) for row in rows])

    @router.get('/api/v1/recent-tasks')
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
        query, params = queries.recent_tasks_query(since=since, event_limit=event_limit)
        result = await deps.flush_and_select(query, params)
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

        tasks_result, trimmed = queries.group_timeline_tasks(
            events,
            ws_min_duration_seconds=recorder.config.ws_min_duration_ms / 1000.0,
            limit=limit,
        )

        return JSONResponse(
            {
                'tasks': tasks_result,
                'lane_count': max_lanes,
                'truncated': truncated or trimmed,
            }
        )

    # Lookup-by-task-ID endpoint for the Arrange tab. Unlike /api/v1/recent-tasks
    # this does NOT filter by ``minutes`` and does NOT apply the
    # ``ws_min_duration_ms`` threshold — callers pass exactly the task_ids
    # they want state for, and we return whatever the recorder has within
    # its retention window (default 24h). This fills the gap where batches
    # in the Arrange tab are older than the 10-min timeline window but
    # their task state is still authoritative in the DB.
    @router.post('/api/v1/live/arrange-tasks')
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

        # Short-circuit when recorder event storage is disabled — no point
        # flushing + SELECTing against an empty table. ``flush_and_select``
        # would still dispatch to the main loop and return rows, but they'd
        # always be empty.
        if not recorder.config.recorder.store_events:
            return JSONResponse({})

        query, params = queries.task_state_query(task_ids)
        query_result = await deps.flush_and_select(query, params)
        if query_result is None:
            return JSONResponse({})
        columns, rows = query_result
        events = [dict(zip(columns, row, strict=False)) for row in rows]
        by_id = queries.group_task_states(events)
        return JSONResponse(by_id)

    # ------------------------------------------------------------------
    # Completion-hook result feeds for the Live view's three tabs:
    #   * Task Results     — one row per on_task_complete()  call
    #   * Message Results  — one row per on_message_complete() call
    #   * Window Results   — one row per on_window_complete() call
    # Each endpoint returns the most recent N rows ordered by ts DESC.
    # No joins: all the user-visible columns are already in the event's
    # metadata JSON (see recorder.record_task_complete etc.). Sink-type
    # breakdown is fetched lazily by the sidebar via /api/v1/live/sink-breakdown.
    # ------------------------------------------------------------------

    async def _fetch_events(event_name: str, limit: int) -> list[dict]:
        """Common helper for the three completion-hook endpoints.

        Returns raw events (ts DESC, limited) as list of dicts, or empty
        list when recorder storage is disabled. Callers parse metadata
        themselves because each event type has different metadata shape.
        """
        if not recorder.config.recorder.store_events:
            return []
        query, params = queries.hook_events_query(event_name=event_name, limit=limit)
        result = await deps.flush_and_select(query, params)
        if result is None:
            return []
        columns, rows = result
        return [dict(zip(columns, row, strict=False)) for row in rows]

    _parse_meta = queries.parse_json_object

    @router.get('/api/v1/live/task-results')
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
            query, params = queries.task_exec_state_query(task_ids)

            # The aiosqlite connection lives on the main loop, so run the
            # SELECT + fetchall there and return plain Python data. Routes
            # through the reader connection so UI lookups don't queue
            # behind writer flushes.
            async def _read_aux():
                reader = recorder.reader_db
                if not reader:
                    return [], []
                async with reader.execute(query, params) as cur:
                    cols = [d[0] for d in cur.description]
                    aux_rows = await cur.fetchall()
                return cols, aux_rows

            cols, aux_rows = await dispatch_to_loop(_read_aux(), deps.drakkar_app.main_loop)
            aux_by_id = queries.group_task_exec_state([dict(zip(cols, row, strict=False)) for row in aux_rows])
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

    @router.get('/api/v1/live/message-results')
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
                query, params = queries.consumed_timestamps_query(sorted(pairs))

                async def _read_consumed():
                    reader = recorder.reader_db
                    if not reader:
                        return []
                    async with reader.execute(query, params) as cur:
                        return await cur.fetchall()

                consumed_by_key = queries.index_consumed_timestamps(
                    await dispatch_to_loop(_read_consumed(), deps.drakkar_app.main_loop)
                )

        result = []
        for e in events:
            meta = _parse_meta(e.get('metadata'))
            elapsed = queries.end_to_end_seconds(
                consumed_by_key.get((e.get('partition'), e.get('offset'))),
                e['ts'],
            )
            end_to_end = None if elapsed is None else round(elapsed, 4)
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

    @router.get('/api/v1/live/window-results')
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

    @router.post('/api/v1/live/sink-breakdown')
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
        query, params = queries.sink_breakdown_query(partition=req.partition, offsets=req.offsets)
        result = await deps.flush_and_select(query, params)
        if result is None:
            return JSONResponse({})
        _columns, rows = result
        return JSONResponse(queries.count_by_topic(rows))

    return router
