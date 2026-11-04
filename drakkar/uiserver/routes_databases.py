"""Databases view routes: ``/api/v1/debug/slice``.

Routes:
  * ``/api/v1/debug/slice`` — build a slice of the recorder events of one time
    range into a private temp file (or in RAM without a folder), unlink it and
    stream it as an attachment.

The answer is a navigation target (a plain link, also to a peer), so the
route takes only query parameters and answers no CORS. The check order is
fixed: every 400 rule first, then 409 (no shared source for the whole
cluster), then 429 (a build already runs on this worker), then the build
(503 when the Postgres table cannot be read, 500 when the file cannot be
written). A limit never answers an error: it marks the file truncated.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from drakkar.concurrency import dispatch_to_loop
from drakkar.recorder.postgres_history import PostgresHistoryError
from drakkar.slice_build import BuiltSlice, SliceWorker, build_slice
from drakkar.slices import (
    BUSY_ERROR,
    CLUSTER_UNAVAILABLE_ERROR,
    COMPRESS_GZIP,
    FAILED_ERROR_PREFIX,
    HEADER_ROWS,
    HEADER_TRUNCATED,
    NOTE_FLUSH_FAILED,
    NOTE_FLUSH_TIMED_OUT,
    POSTGRES_FAILED_ERROR,
    SCOPE_CLUSTER,
    SLICE_FLUSH_WAIT_SECONDS,
    SLICE_RETRY_AFTER_SECONDS,
    SliceRequestError,
    SliceSources,
    available_sources,
    parse_slice_request,
    plan_slice,
)
from drakkar.uiserver.routes_debug import _gzip_chunks, _read_chunks

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()

SLICE_ENDPOINT = '/api/v1/debug/slice'


def slice_sources_of(recorder: EventRecorder) -> SliceSources:
    """The sources this worker can read for a slice, per scope."""
    store = recorder.memory_store
    return available_sources(
        provider=recorder.persistence,
        has_memory=store is not None and store.is_open,
        has_history=bool(recorder.history_dir),
        has_postgres=recorder.postgres_settings is not None,
    )


def slice_worker_of(recorder: EventRecorder) -> SliceWorker:
    """What a build needs to know of this worker, read from the recorder on the API server's loop."""
    return SliceWorker(
        worker=recorder.worker_name,
        instance=recorder.instance,
        cluster=recorder.cluster_name,
        provider=recorder.persistence,
        storage_dir=recorder.storage_dir,
        history_dir=recorder.history_dir,
        live_path=recorder.db_path if recorder.has_file else '',
        max_files=recorder.config.search.max_files,
        open_memory=recorder.open_memory_reader,
        coverage=recorder.memory_coverage,
        postgres=recorder.postgres_settings,
    )


def _error(status: int, message: str, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse({'error': message}, status_code=status, headers=headers)


def _slice_response(built: BuiltSlice, compress: str | None) -> StreamingResponse:
    """Stream a built slice as an attachment; the stream closes the handle after the last chunk."""
    headers = {
        'Content-Disposition': f'attachment; filename="{built.name}"',
        # The link may carry ?token=: keep the answer out of shared caches.
        'Cache-Control': 'no-store, private',
        HEADER_ROWS: str(built.rows),
    }
    if built.truncated is not None:
        headers[HEADER_TRUNCATED] = built.truncated
    if compress == COMPRESS_GZIP:
        return StreamingResponse(_gzip_chunks(built.handle), media_type='application/gzip', headers=headers)
    headers['Content-Length'] = str(built.size_bytes)
    return StreamingResponse(_read_chunks(built.handle), media_type='application/x-sqlite3', headers=headers)


def _close_unsent(build: asyncio.Future[BuiltSlice]) -> None:
    """Close the file of a slice whose request went away before the build ended."""
    if not build.cancelled() and build.exception() is None:
        build.result().handle.close()


def create_databases_router(deps: UIDeps) -> APIRouter:
    """Build the router of the Databases view routes (``/api/v1/debug/slice``)."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app
    # One build at a time on this worker, taken with no wait: a second
    # request gets 429. An asyncio lock, so the check and the take happen
    # with no await in between.
    build_slot = asyncio.Lock()

    def _release(build: asyncio.Future[BuiltSlice]) -> None:
        """Free the build slot when the build ends (not when its request ends); mark its error as seen."""
        build_slot.release()
        if not build.cancelled():
            build.exception()

    async def _flush_on_main_loop(flush: Coroutine[Any, Any, None]) -> str | None:
        """Run one recorder flush on the main loop, at most :data:`SLICE_FLUSH_WAIT_SECONDS`; a note when it did not end."""
        try:
            async with asyncio.timeout(SLICE_FLUSH_WAIT_SECONDS):
                await dispatch_to_loop(flush, drakkar_app.main_loop)
        except TimeoutError:
            return NOTE_FLUSH_TIMED_OUT
        except Exception as exc:
            await logger.awarning('debug_slice_flush_failed', category='debug', error=str(exc))
            return NOTE_FLUSH_FAILED
        return None

    async def _flush_recorder() -> str | None:
        """Write the waiting events to the memory store and the file."""
        return await _flush_on_main_loop(recorder.flush())

    async def _flush_memory() -> str | None:
        """Write the waiting events to the memory store."""
        return await _flush_on_main_loop(recorder.flush_memory())

    async def _log_postgres_failed(reason: str, error: str) -> None:
        """``ui_read_postgres_failed`` of the slice route, at most one line per minute."""
        await deps.postgres_read_log.emit(
            SLICE_ENDPOINT,
            logger.awarning,
            'ui_read_postgres_failed',
            category='debug',
            endpoint=SLICE_ENDPOINT,
            reason=reason,
            error=error,
        )

    @router.get(SLICE_ENDPOINT)
    async def api_debug_slice(request: Request):
        """A slice of the recorder events of one time range, as a SQLite file attachment.

        The query parameters are read as text and checked in a fixed order
        (``drakkar.slices.parse_slice_request``); see the module docstring for
        the answers.
        """
        sources = slice_sources_of(recorder)
        try:
            slice_request = parse_slice_request(request.query_params, sources)
        except SliceRequestError as exc:
            return _error(400, str(exc))
        if slice_request.scope == SCOPE_CLUSTER and not sources.cluster:
            return _error(409, CLUSTER_UNAVAILABLE_ERROR)
        if build_slot.locked():
            return _error(429, BUSY_ERROR, {'Retry-After': str(SLICE_RETRY_AFTER_SECONDS)})
        await build_slot.acquire()
        build = asyncio.ensure_future(
            build_slice(
                slice_request,
                slice_worker_of(recorder),
                plan_slice(slice_request, recorder.persistence),
                flush=_flush_recorder,
                flush_memory=_flush_memory,
                on_postgres_failed=_log_postgres_failed,
            )
        )
        build.add_done_callback(_release)
        try:
            # Shielded: a request that goes away leaves the build running to
            # its end (at most the build time limit), then its file is closed.
            built = await asyncio.shield(build)
        except asyncio.CancelledError:
            build.add_done_callback(_close_unsent)
            raise
        except PostgresHistoryError as exc:
            await _log_postgres_failed(exc.reason, exc.error)
            return _error(503, POSTGRES_FAILED_ERROR)
        except (OSError, sqlite3.Error) as exc:
            await logger.awarning('debug_slice_failed', category='debug', error=str(exc), error_type=type(exc).__name__)
            return _error(500, f'{FAILED_ERROR_PREFIX}{exc}')
        return _slice_response(built, slice_request.compress)

    return router
