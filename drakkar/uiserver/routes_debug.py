"""Debug JSON: ``/api/v1/debug/*`` (databases, trace, metrics, periodic, probe, download).

These back the drakkar-ui Debug page; the page itself is served by
``routes_spa`` from the bundle, not rendered here.

Routes:
  * ``/api/v1/debug/databases``         — own files, then the shared history folders.
  * ``/api/v1/debug/merge``             — merge history files into ``merged/``.
  * ``/api/v1/debug/download/{path}``   — download one file of the shared folder.
  * ``/api/v1/debug/archives``          — compressed archives of every cluster folder.
  * ``/api/v1/debug/trace``             — trace one (partition, offset): own files, or sealed history.
  * ``/api/v1/debug/label-keys``        — distinct label keys across events.
  * ``/api/v1/debug/trace-by-label``    — trace by (key, value) in the live file.
  * ``/api/v1/debug/metrics``           — Prometheus metric snapshot as JSON.
  * ``/api/v1/debug/periodic``          — periodic-task run history.
  * ``/api/v1/debug/probe``             — single-message probe through the live handler.

The request-body Pydantic model ``_ProbeRequest`` MUST be at module
scope for FastAPI's "single Pydantic param = request body" heuristic to
fire — an imported model is treated as a query parameter and surfaces
as 422 errors at runtime.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import re
import secrets
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from drakkar.concurrency import dispatch_to_loop
from drakkar.dbstats import OwnFileStats
from drakkar.merge import DbStats, MergeResult, merge_databases_atomic
from drakkar.recorder.archive import ARCHIVE_NAME_RE, list_archives
from drakkar.recorder.history import HISTORY_FOLDER, MERGED_FOLDER, meta_path, read_meta
from drakkar.recorder.queries import trace_history
from drakkar.recorder.schema import EventType
from drakkar.recorder.sealing import own_db_files
from drakkar.timefmt import format_epoch_ns
from drakkar.uiserver.runner import DebugRunner, ProbeBusyError, ProbeInput

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()

# Extra headroom (in seconds) on top of ``2 * task_timeout_seconds`` for
# the probe's wall-clock timeout. Covers arrange + two round-trips of
# hook work + serialization overhead. Exposed at module scope so tests
# can monkeypatch it to a small value without plumbing a timeout arg
# through the endpoint signature.
PROBE_TIMEOUT_HEADROOM_SECONDS: float = 30.0


# Kafka partitions are non-negative int32; ``/api/v1/debug/trace`` rejects
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


# Names of database files the download and merge endpoints act on: the
# sealed recorder files (``<worker>-<time>.db``) and the merge results
# (``merged-*.db``). ``shared.dir`` is shared with other workers and the
# debug UI is unauthenticated by design, so the endpoints serve only names
# the framework writes, never any readable file a path happens to reach.
#
# A worker whose name contains something outside ``[\w.-]`` (a space, say)
# cannot have its files served; widening the pattern for that would defeat
# its purpose.
DB_DOWNLOAD_NAME_RE = re.compile(r'[\w.-]+\.db')

# A sanitized cluster folder name (``drakkar.recorder.history.sanitize_cluster``).
_CLUSTER_FOLDER_RE = re.compile(r'[a-zA-Z0-9_-]+')

# The paths, relative to ``shared.dir``, that the download and merge
# endpoints act on. The first segment picks the folder; the folder fixes
# how many segments the path has and which names its last segment may
# carry: ``history/<cluster>/<sealed>.db`` or ``history/<cluster>/<archive>``,
# and ``merged/<result>.db``.
_SHARED_PATH_SHAPES: dict[str, tuple[int, tuple[re.Pattern[str], ...]]] = {
    HISTORY_FOLDER: (3, (DB_DOWNLOAD_NAME_RE, ARCHIVE_NAME_RE)),
    MERGED_FOLDER: (2, (DB_DOWNLOAD_NAME_RE,)),
}


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

    ``storage.dir: ""`` is the documented memory-only mode. It used
    to be an arbitrary-file-read hole in every endpoint that joined a name
    onto it: ``os.path.join('', name)`` is just ``name``, and
    ``os.path.realpath('')`` is the worker's current directory, so a
    prefix-string containment check passed for any regular file sitting
    next to the process — its own ``drakkar.yaml``, with SASL passwords and
    sink DSNs, included. Merge had the same hole in the write direction.

    There is no directory to serve from in that mode, so callers answer 404
    rather than trying to contain a path that has no root.

    Blocking: ``resolve`` is ``realpath`` (an lstat per path component), and
    ``shared.dir`` is a network mount by design, so callers run it in a
    thread, never on the loop that answers /healthz and /readyz.
    """
    if not db_dir:
        return None
    return Path(db_dir).resolve()


def _is_safe_segment(segment: str) -> bool:
    """One path segment: not empty, no leading dot, no separator, no header-breaking character."""
    return (
        bool(segment) and not segment.startswith('.') and '\\' not in segment and not _has_unsafe_filename_char(segment)
    )


def _shared_file_path(root: Path, relative: str) -> Path | None:
    """``relative`` (a path inside ``shared.dir``) as a file path, or None when it is not one these endpoints serve.

    The path must have one of the :data:`_SHARED_PATH_SHAPES` and every
    segment must be safe. The file itself must not be a symlink, and the
    resolved path must stay inside ``root`` — so a symlinked folder that
    leads elsewhere is refused too. Blocking (stat calls).
    """
    segments = relative.split('/')
    shape = _SHARED_PATH_SHAPES.get(segments[0])
    if shape is None:
        return None
    segment_count, name_patterns = shape
    if len(segments) != segment_count or not all(_is_safe_segment(segment) for segment in segments):
        return None
    if segment_count == 3 and not _CLUSTER_FOLDER_RE.fullmatch(segments[1]):
        return None
    if not any(pattern.fullmatch(segments[-1]) for pattern in name_patterns):
        return None
    candidate = root.joinpath(*segments)
    if candidate.is_symlink():
        return None
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root):
        return None
    return resolved


def _existing_shared_file(shared_dir: str, relative: str) -> Path | None:
    """:func:`_shared_file_path` for a file that exists; None in memory-only mode. Blocking."""
    root = _db_root(shared_dir)
    if root is None:
        return None
    path = _shared_file_path(root, relative)
    return path if path is not None and path.is_file() else None


def _own_db_row(stats: DbStats, kind: str) -> dict[str, Any]:
    return {
        'filename': stats.filename,
        'worker_name': stats.worker_name,
        'cluster_name': stats.cluster_name or None,
        'kind': kind,
        'event_count': stats.event_count,
        'event_counts': stats.event_counts,
        'first_event_ts': stats.first_event_ts,
        'last_event_ts': stats.last_event_ts,
        'size_bytes': stats.size_bytes,
    }


def _file_entry(folder: str, name: str, info: os.stat_result, kind: str, meta: dict | None) -> dict[str, Any]:
    return {
        'path': f'{folder}/{name}',
        'name': name,
        'kind': kind,
        'size_bytes': info.st_size,
        'modified_at': format_epoch_ns(info.st_mtime_ns),
        'meta': meta,
    }


def _regular_entries(folder: Path) -> list[tuple[str, os.stat_result]]:
    """(name, lstat) of the plain files in ``folder``, sorted by name; no dot names, no links."""
    try:
        names = os.listdir(folder)
    except FileNotFoundError:
        return []
    entries = []
    for name in sorted(names):
        if name.startswith('.'):
            continue
        try:
            info = os.lstat(folder / name)
        except FileNotFoundError:
            continue
        if stat.S_ISREG(info.st_mode):
            entries.append((name, info))
    return entries


def _history_files(shared: Path, cluster: str) -> list[dict[str, Any]]:
    """The sealed files and archives of one cluster folder; a sealed file's stats come from its meta file."""
    folder = shared / HISTORY_FOLDER / cluster
    relative = f'{HISTORY_FOLDER}/{cluster}'
    files = []
    for name, info in _regular_entries(folder):
        if ARCHIVE_NAME_RE.fullmatch(name):
            files.append(_file_entry(relative, name, info, 'archive', None))
        elif name.endswith('.db'):
            files.append(_file_entry(relative, name, info, 'sealed', read_meta(meta_path(str(folder / name)))))
    return files


def _cluster_folders(shared: Path) -> list[str]:
    """Names of the cluster folders under ``history/``, sorted; no dot names, no links."""
    history = shared / HISTORY_FOLDER
    try:
        names = os.listdir(history)
    except FileNotFoundError:
        return []
    folders = []
    for name in sorted(names):
        if name.startswith('.'):
            continue
        try:
            if stat.S_ISDIR(os.lstat(history / name).st_mode):
                folders.append(name)
        except FileNotFoundError:
            continue
    return folders


def _history_answer(shared: Path) -> list[dict[str, Any]]:
    folders: list[dict[str, Any]] = [
        {
            'folder': f'{HISTORY_FOLDER}/{cluster}',
            'kind': 'history',
            'cluster': cluster,
            'files': _history_files(shared, cluster),
        }
        for cluster in _cluster_folders(shared)
    ]
    merged = shared / MERGED_FOLDER
    if merged.is_dir() and not merged.is_symlink():
        files = [
            _file_entry(MERGED_FOLDER, name, info, 'merged', None)
            for name, info in _regular_entries(merged)
            if name.endswith('.db')
        ]
        folders.append({'folder': MERGED_FOLDER, 'kind': 'merged', 'cluster': None, 'files': files})
    return folders


def _databases_answer(recorder: EventRecorder, own_stats: OwnFileStats) -> dict[str, Any]:
    """The ``/debug/databases`` answer: own files measured now, then the shared history. Blocking."""
    if not recorder.storage_dir:
        return {'own': [], 'history': {'folders': []}}
    live = os.path.normpath(recorder.db_path) if recorder.db_path else ''
    own = [
        _own_db_row(stats, 'live' if stats.path == live else 'backlog')
        for stats in own_stats.measure(own_db_files(recorder.storage_dir, recorder.worker_name))
    ]
    return {'own': own, 'history': {'folders': _history_answer(Path(recorder.shared_dir))}}


def _all_archives(shared_dir: str) -> list[dict[str, Any]]:
    """Archives of every cluster folder, newest window end first; none in memory-only mode. Blocking."""
    shared = _db_root(shared_dir)
    if shared is None:
        return []
    entries = []
    for cluster in _cluster_folders(shared):
        folder = f'{HISTORY_FOLDER}/{cluster}'
        for archive in list_archives(str(shared / HISTORY_FOLDER / cluster)):
            entries.append(
                {
                    'name': archive.name,
                    'cluster': archive.cluster,
                    'from_ts': archive.from_ts,
                    'to_ts': archive.to_ts,
                    'size_bytes': archive.size_bytes,
                    'folder': folder,
                    'path': f'{folder}/{archive.name}',
                }
            )
    entries.sort(key=lambda entry: entry['to_ts'], reverse=True)
    return entries


def _merge_sources(root: Path, paths: list[str]) -> tuple[list[str], JSONResponse | None]:
    """Check merge source paths; an error answer instead when one is not a database file we serve. Blocking."""
    sources = []
    for relative in paths:
        full = _shared_file_path(root, relative)
        if full is None or not relative.endswith('.db'):
            return [], JSONResponse({'error': f'Invalid filename: {relative}'}, status_code=400)
        if not full.is_file():
            return [], JSONResponse({'error': f'File not found: {relative}'}, status_code=404)
        sources.append(str(full))
    return sources, None


def _merge_into_shared(shared_dir: str, paths: list[str], output_name: str) -> MergeResult | JSONResponse:
    """Check the sources, then merge them into ``<shared>/merged/<output_name>``. Blocking.

    Answers with the error response when a source is refused. The output
    folder is created owner-only when missing; a merge failure raises.
    """
    root = _db_root(shared_dir)
    if root is None:
        return JSONResponse({'error': 'No database directory configured'}, status_code=404)
    sources, error = _merge_sources(root, paths)
    if error is not None:
        return error
    folder = root / MERGED_FOLDER
    folder.mkdir(mode=0o700, exist_ok=True)
    return merge_databases_atomic(sources, str(folder / output_name))


# ``/api/v1/debug/probe`` request body — module-scope per the FastAPI
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


def create_debug_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns the debug page + ``/api/v1/debug/*`` endpoints (excluding cache).

    The file download at ``/debug/download/{filename}`` lives here too.
    """
    # Every debug route (config summary, traces, metrics, merge, probe,
    # downloads) is sensitive — gate the whole router behind require_auth
    # (no-op without a token). The per-route Depends below predate the
    # router-level gate and are kept as explicit markers.
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    config = deps.config
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app

    # --- Debug databases ---

    # Statistics of this worker's own files, refreshed incrementally per
    # request (sealed files carry theirs in meta files).
    own_stats = OwnFileStats()

    @router.get('/api/v1/debug/databases', dependencies=[Depends(deps.require_auth)])
    async def api_debug_databases():
        """This worker's own files with measured stats, then every shared history folder.

        Offloaded: it lists folders on a possibly network-mounted
        ``shared.dir`` and reads the own files with sqlite3 — /healthz and
        /readyz live on this same loop.
        """
        return JSONResponse(await asyncio.to_thread(_databases_answer, recorder, own_stats))

    @router.post('/api/v1/debug/merge', dependencies=[Depends(deps.require_auth)])
    async def api_debug_merge(request: Request):
        """Merge selected database files into one."""

        # Policy gate, checked before the body is even read: merge is the
        # one UI endpoint that writes to disk (the archive pass deletes its
        # results after ``history.merged_retention_days``). Independent of
        # auth_token by design — see ``UIConfig.merge_enabled``.
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

        ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
        # The random suffix keeps two merges in the same second apart.
        output_name = f'merged-{ts}-{secrets.token_hex(8)}.db'

        # Memory-only recorder: no folder to read sources from, and — the
        # part that mattered — none to write the output into either. The
        # whole check-and-merge runs in a thread: it resolves paths on the
        # shared mount.
        try:
            result = await asyncio.to_thread(_merge_into_shared, recorder.shared_dir, filenames, output_name)
        except Exception as exc:
            await logger.awarning(
                'merge_failed',
                category='debug',
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return JSONResponse({'error': f'Merge failed: {exc}'}, status_code=500)

        if isinstance(result, JSONResponse):
            return result
        return JSONResponse(
            {
                'filename': output_name,
                'path': f'{MERGED_FOLDER}/{output_name}',
                'worker_count': result.worker_count,
                'event_count': result.event_count,
                'state_count': result.state_count,
                'cluster_name': result.cluster_name,
                'source_files': result.source_files,
            }
        )

    @router.get('/api/v1/debug/trace')
    async def api_debug_trace(
        partition: int = Query(ge=0, le=_INT32_MAX),
        offset: int = Query(),
        history: bool = Query(False),
    ):
        """Trace a message: this worker's own files, or with ``history`` its cluster's sealed files.

        The history search reads only sealed files, so it runs in a thread
        here and never touches the worker's main loop.
        """
        if history:
            events = await asyncio.to_thread(
                functools.partial(
                    trace_history,
                    recorder.history_dir,
                    partition,
                    offset,
                    max_files=config.trace.history_max_files,
                    max_seconds=config.trace.history_max_seconds,
                )
            )
            return JSONResponse(events)
        events = await dispatch_to_loop(recorder.cross_trace(partition, offset), deps.drakkar_app.main_loop)
        return JSONResponse(events)

    @router.get('/api/v1/debug/label-keys')
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

    @router.get('/api/v1/debug/trace-by-label')
    async def api_debug_trace_by_label(
        key: str = Query(min_length=1),
        value: str = Query(min_length=1),
    ):
        """Trace tasks by label value through this worker's live file."""
        events = await dispatch_to_loop(recorder.cross_trace_by_label(key, value), deps.drakkar_app.main_loop)
        return JSONResponse(events)

    @router.get('/api/v1/debug/metrics')
    async def api_debug_metrics():
        """Return all registered Prometheus metrics with current values."""
        from drakkar.metrics import collect_all_metrics

        return JSONResponse(collect_all_metrics())

    @router.get('/api/v1/debug/periodic')
    async def api_debug_periodic():
        """Return periodic task run history from the flight recorder.

        Groups events by task name and returns the latest run, total counts,
        and recent history for each task.
        """
        query = f"""
            SELECT ts, task_id, duration, exit_code, metadata
            FROM events
            WHERE event = '{EventType.PERIODIC_RUN}'
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

    @router.get('/api/v1/debug/download/{path:path}', dependencies=[Depends(deps.require_auth)])
    async def debug_download(path: str):
        """Download one sealed file, archive or merge result; ``path`` is relative to ``shared.dir``.

        Every refusal — a path of another shape, an unsafe segment, a link
        leading out of the folder, a missing file — answers 404.
        """
        full = await asyncio.to_thread(_existing_shared_file, recorder.shared_dir, path)
        if full is None:
            return JSONResponse({'error': 'File not found'}, status_code=404)
        name = path.rsplit('/', 1)[-1]
        return FileResponse(
            path=str(full),
            filename=name,
            media_type='application/gzip' if ARCHIVE_NAME_RE.fullmatch(name) else 'application/x-sqlite3',
            # The download URL may carry ?token= (browsers can't set headers
            # on <a> navigations); no-store keeps token-bearing responses
            # out of shared proxy/CDN caches.
            headers={'Cache-Control': 'no-store, private'},
        )

    @router.get('/api/v1/debug/archives', dependencies=[Depends(deps.require_auth)])
    async def api_debug_archives():
        """Compressed archives of every cluster history folder, newest first.

        Offloaded: one listdir per folder and one stat per archive, on a
        shared folder that may be network-mounted and unbounded.
        """
        return JSONResponse({'archives': await asyncio.to_thread(_all_archives, recorder.shared_dir)})

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

    @router.post('/api/v1/debug/probe', dependencies=[Depends(deps.require_auth)])
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
        # that key on ``msg.topic`` see a realistic value. A worker with
        # the Kafka source disabled has no configured topic to fall back
        # to, so an empty request topic is then a caller error, not a
        # value to paper over with a default that would not exist in
        # production.
        kafka_src = drakkar_app._config.sources.kafka
        topic = req.topic or (kafka_src.topic if kafka_src.enabled else '')
        if not topic:
            return JSONResponse(
                status_code=400,
                content={'error': 'topic is required: the Kafka source is disabled on this worker'},
            )
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
