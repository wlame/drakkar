"""Debug page + ``/api/debug/*`` (databases, trace, metrics, periodic, probe, download).

Routes:
  * ``/debug``                       — debug HTML page.
  * ``/api/debug/databases``         — list of debug DB files.
  * ``/api/debug/merge``             — merge multiple DB files.
  * ``/debug/download/{filename}``   — download a single DB file.
  * ``/api/debug/archives``          — list of compressed archive files.
  * ``/api/debug/archives/{name}``   — download a single archive file.
  * ``/api/debug/trace``             — cross-worker trace for one (partition, offset).
  * ``/api/debug/label-keys``        — distinct label keys across events.
  * ``/api/debug/trace-by-label``    — cross-worker trace by (key, value).
  * ``/api/debug/metrics``           — Prometheus metric snapshot as JSON.
  * ``/api/debug/periodic``          — periodic-task run history.
  * ``/api/debug/probe``             — single-message probe through the live handler.

The request-body Pydantic model ``_ProbeRequest`` MUST be at module
scope for FastAPI's "single Pydantic param = request body" heuristic to
fire — an imported model is treated as a query parameter and surfaces
as 422 errors at runtime.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from drakkar.concurrency import dispatch_to_loop
from drakkar.uiserver.runner import DebugRunner, ProbeBusyError, ProbeInput

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

# Extra headroom (in seconds) on top of ``2 * task_timeout_seconds`` for
# the probe's wall-clock timeout. Covers arrange + two round-trips of
# hook work + serialization overhead. Exposed at module scope so tests
# can monkeypatch it to a small value without plumbing a timeout arg
# through the endpoint signature.
PROBE_TIMEOUT_HEADROOM_SECONDS: float = 30.0


# Kafka partitions are non-negative int32; ``/api/debug/trace`` rejects
# values that would silently truncate when narrowed to the recorder's
# int32 partition column (contract v1).
_INT32_MAX = 2**31 - 1


def _has_unsafe_filename_char(filename: str) -> bool:
    """True when ``filename`` carries a char that must not reach the download headers.

    The double-quote ends the ``Content-Disposition: filename="..."`` token,
    the semicolon starts a new header parameter, and control characters
    (incl. CR/LF, plus DEL) enable header injection. Path-separator and
    dot-prefix traversal are checked separately by the callers.
    """
    return any(c in '";' or ord(c) < 0x20 or ord(c) == 0x7F for c in filename)


# Names the database download and merge endpoints will act on.
#
# They exist to hand over recorder artifacts: the timestamped DBs
# ``make_db_path`` writes, the ``-live.db`` / ``-cache.db`` symlinks and the
# ``merged-*.db`` output. ``db_dir`` defaults to ``/tmp`` — shared with
# every other program on the host — and the debug UI is unauthenticated by
# design, so checking only for separators and header-injection characters
# let any readable file there be fetched by exact name.
#
# Archives are NOT matched here: they are ``.db.gz`` and have their own
# route guarded by ``ARCHIVE_NAME_RE``.
#
# Note the character class: a worker whose name contains something outside
# ``[\w.-]`` (a space, say) would not be able to serve its own database
# through this endpoint. Worker names are not validated anywhere, so that is
# possible — but such a name already makes for awkward filenames, and
# widening this pattern to accommodate it would defeat its purpose.
DB_DOWNLOAD_NAME_RE = re.compile(r'[\w.-]+\.db')


def _probe_busy_response(exc: ProbeBusyError) -> JSONResponse:
    """429 for a probe shed because too many are already queued.

    ``Retry-After: 1`` is a hint, not a promise — probes serialize on one
    lock and the queue in front of the caller is short by construction
    (see ``MAX_PROBE_WAITERS``).
    """
    return JSONResponse(
        {'error': str(exc), 'max_waiters': exc.max_waiters},
        status_code=429,
        headers={'Retry-After': '1'},
    )


def _db_root(db_dir: str) -> Path | None:
    """The recorder directory as an absolute path, or None when there is none.

    ``ui.recorder.db_dir: ""`` is the documented memory-only mode. It used
    to be an arbitrary-file-read hole in every endpoint that joined a name
    onto it: ``os.path.join('', name)`` is just ``name``, and
    ``os.path.realpath('')`` is the worker's current directory, so a
    prefix-string containment check passed for any regular file sitting
    next to the process — its own ``drakkar.yaml``, with SASL passwords and
    sink DSNs, included. Merge had the same hole in the write direction.

    There is no directory to serve from in that mode, so callers answer 404
    rather than trying to contain a path that has no root.
    """
    if not db_dir:
        return None
    return Path(db_dir).resolve()


def _contained_path(root: Path, filename: str) -> Path | None:
    """``filename`` resolved inside ``root``, or None when it is not contained.

    Rejects path separators, dot-prefixed names and the characters that
    would break out of the ``Content-Disposition`` header, then resolves
    and checks containment against the real root — so a symlink inside
    ``db_dir`` pointing outside it is refused too.

    Containment only. Callers decide WHICH names they are willing to act
    on: ``_contained_db_path`` for database files, ``ARCHIVE_NAME_RE`` for
    the ``.db.gz`` archives.
    """
    if '/' in filename or '\\' in filename or filename.startswith('.') or _has_unsafe_filename_char(filename):
        return None
    candidate = (root / filename).resolve()
    if candidate == root or not candidate.is_relative_to(root):
        return None
    return candidate


def _contained_db_path(root: Path, filename: str) -> Path | None:
    """A contained path that is also a recorder DATABASE name.

    See ``DB_DOWNLOAD_NAME_RE`` for why the name policy exists at all.
    """
    if not DB_DOWNLOAD_NAME_RE.fullmatch(filename):
        return None
    return _contained_path(root, filename)


# ``/api/debug/probe`` request body — module-scope per the FastAPI
# "single Pydantic param = body" heuristic: an imported model is
# treated as a query parameter and surfaces as 422 errors. Mirrors
# ``ProbeInput`` from ``drakkar.uiserver.runner``.
class _ProbeRequest(BaseModel):
    value: str = Field(max_length=10_000_000)
    key: str | None = Field(default=None, max_length=65_536)
    partition: int = Field(default=0, ge=0)
    offset: int = Field(default=0, ge=0)
    topic: str = Field(default='', max_length=65_536)
    timestamp: int | None = None
    use_cache: bool = False


def _disabled_response(endpoint: str, config_key: str) -> JSONResponse:
    """403 for an endpoint an operator has switched off.

    403 rather than 404: the route exists and the caller may well be
    authenticated — it is policy, not absence, that refuses. Naming the
    config key in the body turns "the button stopped working" into a
    one-line fix without a trip through the logs. Uses the same
    ``{"error": ...}`` envelope as the rest of the debug API (contract v1).
    """
    return JSONResponse(
        {'error': f'The {endpoint} endpoint is disabled by configuration (set {config_key}=true to enable it)'},
        status_code=403,
    )


def create_debug_router(deps: UIDeps, include_html: bool = True) -> APIRouter:
    """Build the router that owns the debug page + ``/api/debug/*`` endpoints (excluding cache).

    ``include_html=False`` (SPA mode) drops the ``/debug`` Jinja page so the
    SPA catch-all owns it; the JSON endpoints and the file download at
    ``/debug/download/{filename}`` are unaffected.
    """
    # Every debug route (config summary, traces, metrics, merge, probe,
    # downloads) is sensitive — gate the whole router behind require_auth
    # (no-op without a token). The per-route Depends below predate the
    # router-level gate and are kept as explicit markers.
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    # HTML page routes register on ``html``: the real router normally, or a
    # throwaway router (never mounted) when the SPA owns the page surface.
    html = router if include_html else APIRouter()
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app
    templates = deps.templates

    # --- Debug databases page ---

    @html.get('/debug', response_class=HTMLResponse)
    async def debug_databases(request: Request):
        return templates.TemplateResponse(
            request,
            'debug.html',
            {
                'worker_id': drakkar_app._worker_id,
                'db_dir': config.recorder.db_dir,
                'config_summary': drakkar_app.config_summary,
            },
        )

    @router.get('/api/debug/databases', dependencies=[Depends(deps.require_auth)])
    async def api_debug_databases():
        """List all debug database files in db_dir with stats.

        Backed by the ``.dbstats.db`` cache (see :mod:`drakkar.dbstats`):
        the file LIST always comes from a live directory scan — externally
        deleted files disappear on the next request — while statistics for
        immutable rotated files come from the cache. At most
        ``ui.recorder.dbstats_inline_scan_limit`` cold files are scanned
        inline; the rest return ``stats_pending`` and fill in as the
        recorder's warmer loop catches up. Contract v1.12 adds ``kind``,
        ``live_for`` (the worker currently writing the file — the in-use
        highlight), ``stats_pending``, and ``cache_entry_count``.
        """
        from drakkar.dbstats import DbStatsCache, collect

        # Offloaded: sync sqlite3 work. Even with the cache, a cold file
        # or a live-DB delta scan does real reads — /healthz and /readyz
        # live on this same loop.
        cache = DbStatsCache(config.recorder.db_dir)
        rows = await asyncio.to_thread(
            collect,
            config.recorder.db_dir,
            cache,
            inline_scan_limit=config.recorder.dbstats_inline_scan_limit,
        )
        return JSONResponse(
            [
                {
                    'filename': row.stats.filename,
                    'path': row.stats.path,
                    'worker_name': row.stats.worker_name,
                    'cluster_name': row.stats.cluster_name,
                    'event_count': row.stats.event_count,
                    'event_counts': row.stats.event_counts,
                    'first_event_ts': row.stats.first_event_ts,
                    'last_event_ts': row.stats.last_event_ts,
                    'has_events': row.stats.has_events,
                    'has_config': row.stats.has_config,
                    'has_state': row.stats.has_state,
                    'size_bytes': row.stats.size_bytes,
                    'kind': row.stats.kind,
                    'live_for': row.live_for,
                    'stats_pending': row.stats_pending,
                    'cache_entry_count': row.stats.cache_entry_count,
                }
                for row in rows
            ]
        )

    @router.post('/api/debug/merge', dependencies=[Depends(deps.require_auth)])
    async def api_debug_merge(request: Request):
        """Merge selected database files into one."""

        from drakkar.merge import merge_databases

        # Policy gate, checked before the body is even read: merge is the
        # one UI endpoint that writes to disk, and nothing reclaims what it
        # writes. Independent of auth_token by design — see
        # ``UIConfig.merge_enabled``.
        if not config.merge_enabled:
            return _disabled_response('merge', 'ui.merge_enabled')

        # A malformed body is a caller error, not a server bug — 400 with
        # the legacy {"error": ...} envelope (contract v1), never a 500.
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse({'error': 'Invalid JSON body'}, status_code=400)
        filenames = body.get('filenames', []) if isinstance(body, dict) else None
        if not isinstance(filenames, list) or not all(isinstance(fn, str) for fn in filenames):
            return JSONResponse({'error': 'Invalid JSON body'}, status_code=400)
        if len(filenames) < 2:
            return JSONResponse({'error': 'Select at least 2 databases'}, status_code=400)

        # Memory-only recorder: no directory to read sources from, and —
        # the part that mattered — none to write the output into either.
        root = _db_root(config.recorder.db_dir)
        if root is None:
            return JSONResponse({'error': 'No database directory configured'}, status_code=404)

        # resolve to full paths, validate they exist in db_dir
        db_paths = []
        for fn in filenames:
            full = _contained_db_path(root, fn)
            if full is None:
                return JSONResponse({'error': f'Invalid filename: {fn}'}, status_code=400)
            if not full.is_file():
                return JSONResponse({'error': f'File not found: {fn}'}, status_code=404)
            db_paths.append(str(full))

        ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
        output_name = f'merged-{ts}.db'
        output_path = str(root / output_name)

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

    @router.get('/api/debug/trace')
    async def api_debug_trace(
        partition: int = Query(ge=0, le=_INT32_MAX),
        offset: int = Query(),
    ):
        """Trace a message across all workers in the same cluster."""
        events = await dispatch_to_loop(recorder.cross_trace(partition, offset), deps.drakkar_app.main_loop)
        return JSONResponse(events)

    @router.get('/api/debug/label-keys')
    async def api_debug_label_keys():
        """Return distinct label keys found in events."""
        query = """
            SELECT DISTINCT labels FROM events
            WHERE labels IS NOT NULL
            LIMIT 100
        """
        # No try/except: ``flush_and_select`` already maps DB-absent to
        # ``None`` (handled below) and any real exception here is a bug
        # (malformed SQL, recorder internal state corruption) that should
        # surface loudly rather than be silently hidden behind an empty
        # response. Fail-loud beats a silently-empty label dropdown.
        result = await deps.flush_and_select(query)
        if result is None:
            return JSONResponse([])
        _columns, rows = result
        keys: set[str] = set()
        for (labels_json,) in rows:
            try:
                parsed = json.loads(labels_json)
                keys.update(parsed.keys())
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
        return JSONResponse(sorted(keys))

    @router.get('/api/debug/trace-by-label')
    async def api_debug_trace_by_label(
        key: str = Query(min_length=1),
        value: str = Query(min_length=1),
    ):
        """Trace tasks by label value across all workers in the cluster."""
        events = await dispatch_to_loop(recorder.cross_trace_by_label(key, value), deps.drakkar_app.main_loop)
        return JSONResponse(events)

    @router.get('/api/debug/metrics')
    async def api_debug_metrics():
        """Return all registered Prometheus metrics with current values."""
        from drakkar.metrics import collect_all_metrics

        return JSONResponse(collect_all_metrics())

    @router.get('/api/debug/periodic')
    async def api_debug_periodic():
        """Return periodic task run history from the flight recorder.

        Groups events by task name and returns the latest run, total counts,
        and recent history for each task.
        """
        query = """
            SELECT ts, task_id, duration, exit_code, metadata
            FROM events
            WHERE event = 'periodic_run'
            ORDER BY ts DESC
            LIMIT 500
        """
        # No try/except — see ``api_debug_label_keys`` for rationale.
        # ``flush_and_select`` returns ``None`` on DB-absent; any raised
        # exception is a real bug and should surface.
        result = await deps.flush_and_select(query)
        if result is None:
            return JSONResponse([])
        columns, rows = result

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

        return JSONResponse(sorted(tasks.values(), key=lambda t: t['name']))

    @router.get('/debug/download/{filename}', dependencies=[Depends(deps.require_auth)])
    async def debug_download(filename: str):
        """Download a database file from db_dir."""
        root = _db_root(config.recorder.db_dir)
        if root is None:
            return JSONResponse({'error': 'No database directory configured'}, status_code=404)
        full = _contained_db_path(root, filename)
        if full is None:
            return JSONResponse({'error': 'Invalid filename'}, status_code=400)
        if not full.is_file():
            return JSONResponse({'error': 'File not found'}, status_code=404)
        return FileResponse(
            path=str(full),
            filename=filename,
            media_type='application/x-sqlite3',
            # The download URL may carry ?token= (browsers can't set headers
            # on <a> navigations); no-store keeps token-bearing responses
            # out of shared proxy/CDN caches.
            headers={'Cache-Control': 'no-store, private'},
        )

    @router.get('/api/debug/archives', dependencies=[Depends(deps.require_auth)])
    async def api_debug_archives():
        """List compressed archive files in db_dir."""
        from drakkar.recorder.archive import list_archives

        # Offloaded for the same reason as api_debug_databases above: this
        # is one listdir + one stat per archive, cheap today, but db_dir is
        # shared and unbounded, so it must never run inline on the loop
        # that also serves /healthz and /readyz.
        archives = await asyncio.to_thread(list_archives, config.recorder.db_dir)
        return JSONResponse(
            {
                'archives': [
                    {
                        'name': archive.name,
                        'cluster': archive.cluster,
                        'from_ts': archive.from_ts,
                        'to_ts': archive.to_ts,
                        'size_bytes': archive.size_bytes,
                    }
                    for archive in archives
                ]
            }
        )

    @router.get('/api/debug/archives/{name}', dependencies=[Depends(deps.require_auth)])
    async def debug_download_archive(name: str):
        """Download one compressed archive file from db_dir."""
        from drakkar.recorder.archive import ARCHIVE_NAME_RE

        # The naming pattern alone already rules out path separators and a
        # leading dot, but the realpath check stays as defense-in-depth —
        # same belt-and-braces shape as debug_download above.
        if not ARCHIVE_NAME_RE.fullmatch(name):
            return JSONResponse({'error': 'Invalid archive name'}, status_code=404)
        root = _db_root(config.recorder.db_dir)
        if root is None:
            return JSONResponse({'error': 'No database directory configured'}, status_code=404)
        # ARCHIVE_NAME_RE above is this route's name policy (archives are
        # ``.db.gz``, not ``.db``), so only containment is left to check.
        full = _contained_path(root, name)
        if full is None or not full.is_file():
            return JSONResponse({'error': 'File not found'}, status_code=404)
        return FileResponse(
            path=str(full),
            filename=name,
            media_type='application/gzip',
            headers={'Cache-Control': 'no-store, private'},
        )

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
    probe_state: dict[str, DebugRunner | None] = {'runner': None}

    def _get_probe_runner() -> DebugRunner:
        # Single-key dict so the closure can mutate the slot without a
        # ``nonlocal`` declaration. ``ty`` narrows the after-check value
        # cleanly.
        existing = probe_state['runner']
        if existing is None:
            # The executor pool is created during ``DrakkarApp.run`` and
            # lives for the whole process; by the time the probe endpoint
            # is reachable it's always non-None. Guard here so ty's
            # ``ExecutorPool | None`` narrowing is happy.
            pool = drakkar_app._executor_pool
            if pool is None:
                raise HTTPException(status_code=503, detail='executor pool not ready')
            existing = DebugRunner(
                handler=drakkar_app.handler,
                executor_pool=pool,
                app_config=drakkar_app._config,
            )
            probe_state['runner'] = existing
        return existing

    @router.post('/api/debug/probe', dependencies=[Depends(deps.require_auth)])
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

        Returns 403 when ``ui.probe_enabled`` is false. That gate is
        independent of ``auth_token``: the probe runs caller-supplied bytes
        through the live handler and competes with production traffic for
        executor slots, so an operator may want it closed regardless of
        whether auth is configured.
        """
        if not config.probe_enabled:
            return _disabled_response('probe', 'ui.probe_enabled')
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
        # Read the constant via this module's globals so tests can
        # monkeypatch ``PROBE_TIMEOUT_HEADROOM_SECONDS`` and have the
        # patched value observed at request time without a lazy import.
        timeout = 2 * drakkar_app._config.executor.task_timeout_seconds + PROBE_TIMEOUT_HEADROOM_SECONDS
        # Build the per-run state here so we own a reference for the
        # truncated-partial-report path even when the actual probe
        # coroutine runs on a different event loop (and thus an
        # asyncio.Task we can't see).
        state = runner._make_run_state(probe_input)
        current_loop = asyncio.get_running_loop()
        # CRITICAL: the ExecutorPool.semaphore is an ``asyncio.Semaphore``
        # bound to the loop where the pool was constructed (the main
        # DrakkarApp loop). The debug FastAPI server typically runs in a
        # separate thread + loop, so we must dispatch the probe back to
        # that main loop — otherwise ``semaphore.acquire()`` raises
        # "bound to a different event loop" as soon as the pool has
        # contention. When ``drakkar_app.main_loop`` is a real loop and
        # is NOT our running loop, use the cross-thread path. Otherwise
        # (same-loop in tests, or loop unavailable) we run inline.
        candidate_loop = drakkar_app.main_loop
        if isinstance(candidate_loop, asyncio.AbstractEventLoop) and candidate_loop is not current_loop:
            # Cross-thread: dispatch to the main loop. On timeout,
            # cancel the future and sleep briefly so the main-loop
            # task can run its ``finally`` and restore handler.cache
            # before we return the partial report.
            run_future = asyncio.run_coroutine_threadsafe(runner._run_with_state(state), candidate_loop)
            try:
                report = await asyncio.wait_for(asyncio.wrap_future(run_future), timeout=timeout)
            except TimeoutError:
                run_future.cancel()
                await asyncio.sleep(0.1)
                report = state.to_report(truncated=True)
            except ProbeBusyError as exc:
                return _probe_busy_response(exc)
        else:
            # Same-loop: plain asyncio. ``wait_for`` already awaits the
            # cancelled task's ``finally`` before raising, so the cache
            # is restored by the time we reach the except branch.
            run_task = asyncio.create_task(runner._run_with_state(state))
            try:
                report = await asyncio.wait_for(run_task, timeout=timeout)
            except TimeoutError:
                report = state.to_report(truncated=True)
            except ProbeBusyError as exc:
                return _probe_busy_response(exc)
        return JSONResponse(report.model_dump(mode='json'))

    return router
