"""Databases view routes: the slice download, the Postgres partitions and the autodiscover state.

Routes:
  * ``/api/v1/debug/slice`` — build a slice of the recorder events of one time
    range into a private temp file (or in RAM without a folder), unlink it and
    stream it as an attachment.
  * ``/api/v1/debug/databases/postgres`` — the hour partitions of the
    recorder's Postgres table and what the next upkeep drops.
  * ``/api/v1/debug/autodiscover`` — the registry cards of the last refresh,
    from memory.
  * ``/api/v1/debug/autodiscover/leases`` — who holds each cluster task's
    lease, read through the election's own provider caller.

The answer is a navigation target (a plain link, also to a peer), so the
route takes only query parameters and answers no CORS. The check order is
fixed: every 400 rule first, then 409 (no shared source for the whole
cluster), then 429 (a build already runs on this worker, or finished slices
still being sent hold :data:`SLICE_OPEN_MAX_BYTES` or number
:data:`SLICE_OPEN_MAX_COUNT`), then the build (503
when the Postgres table cannot be read, 500 when the file cannot be
written). A limit never answers an error: it marks the file truncated. A
download whose client takes no chunk for :data:`SLICE_SEND_IDLE_SECONDS`
is ended.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from drakkar.cluster_tasks import HOLDER_OP, HOLDER_READS_BUSY, ClusterElection
from drakkar.concurrency import dispatch_to_loop
from drakkar.providers.resilience import CallResult, describe_error
from drakkar.recorder.postgres_history import PostgresHistoryError, read_table_partitions
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
    NOTE_POSTGRES_QUEUE_WAITING,
    OPEN_BYTES_ERROR,
    OPEN_COUNT_ERROR,
    POSTGRES_FAILED_ERROR,
    SCOPE_CLUSTER,
    SLICE_FLUSH_WAIT_SECONDS,
    SLICE_OPEN_MAX_BYTES,
    SLICE_OPEN_MAX_COUNT,
    SLICE_POSTGRES_FLUSH_SECONDS,
    SLICE_RETRY_AFTER_SECONDS,
    SLICE_SEND_IDLE_SECONDS,
    SliceRequestError,
    SliceSources,
    available_sources,
    parse_slice_request,
    plan_slice,
)
from drakkar.uiserver.autodiscover_status import (
    AUTODISCOVER_OFF_ERROR,
    LEASE_BUSY,
    LEASE_FAILED,
    card_rows,
    display_key,
    lease_row,
    lease_skeletons,
    lease_status,
    registry_connection,
    registry_header,
)
from drakkar.uiserver.server import dispatch_bounded
from drakkar.uiserver.streaming import (
    OPEN_LIMIT_BYTES,
    IdleLimitedStreamingResponse,
    OpenDownload,
    OpenDownloads,
    stream_file,
)

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()

SLICE_ENDPOINT = '/api/v1/debug/slice'
POSTGRES_PARTITIONS_ENDPOINT = '/api/v1/debug/databases/postgres'
AUTODISCOVER_ENDPOINT = '/api/v1/debug/autodiscover'
LEASES_ENDPOINT = '/api/v1/debug/autodiscover/leases'
NOT_POSTGRES_ERROR = 'the recorder provider is not postgres'


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
        postgres=recorder.postgres_settings,
    )


def _error(status: int, message: str, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse({'error': message}, status_code=status, headers=headers)


def _slice_response(built: BuiltSlice, compress: str | None, reservation: OpenDownload) -> StreamingResponse:
    """Stream a built slice as an attachment.

    ``reservation`` (taken when the request was admitted) gets the size of
    the file. The file closes, and the reservation is released, when the
    stream ends, when the client goes away, or when the client takes no
    chunk for :data:`SLICE_SEND_IDLE_SECONDS`.
    """
    reservation.set_size(built.size_bytes)
    headers = {
        'Content-Disposition': f'attachment; filename="{built.name}"',
        # The link may carry ?token=: keep the answer out of shared caches.
        'Cache-Control': 'no-store, private',
        HEADER_ROWS: str(built.rows),
    }
    if built.truncated is not None:
        headers[HEADER_TRUNCATED] = built.truncated
    gzip = compress == COMPRESS_GZIP
    if not gzip:
        headers['Content-Length'] = str(built.size_bytes)
    return IdleLimitedStreamingResponse(
        stream_file(built.handle, gzip=gzip),
        idle_seconds=SLICE_SEND_IDLE_SECONDS,
        on_close=reservation.release,
        media_type='application/gzip' if gzip else 'application/x-sqlite3',
        headers=headers,
    )


def _close_unsent(build: asyncio.Future[BuiltSlice]) -> None:
    """Close the file of a slice whose request went away before the build ended."""
    if not build.cancelled() and build.exception() is None:
        build.result().handle.close()


def create_databases_router(deps: UIDeps) -> APIRouter:
    """Build the router of the Databases view routes (see the module docstring)."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    recorder = deps.recorder
    drakkar_app = deps.drakkar_app
    # One build at a time on this worker, taken with no wait: a second
    # request gets 429. An asyncio lock, so the check and the take happen
    # with no await in between.
    build_slot = asyncio.Lock()
    open_slices = OpenDownloads(max_bytes=SLICE_OPEN_MAX_BYTES, max_count=SLICE_OPEN_MAX_COUNT)

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

    async def _flush_postgres() -> str | None:
        """Write the waiting events to the memory store, then run the Postgres write stage once.

        A note when it did not end, or when rows of this worker still wait in
        the stage's queue (a row that also left the memory store is then in
        neither source).
        """
        waiting: list[int | None] = []

        async def flush() -> None:
            waiting.append(await recorder.flush_postgres(SLICE_POSTGRES_FLUSH_SECONDS))

        note = await _flush_on_main_loop(flush())
        if note is None and waiting and waiting[0]:
            return NOTE_POSTGRES_QUEUE_WAITING
        return note

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
        # Read before the checks: nothing between the reservation and the
        # handlers below that release it, nor between the slot take and its
        # release callback, may raise.
        worker = slice_worker_of(recorder)
        plan = plan_slice(slice_request, recorder.persistence)
        retry = {'Retry-After': str(SLICE_RETRY_AFTER_SECONDS)}
        if build_slot.locked():
            return _error(429, BUSY_ERROR, retry)
        if (open_limit := open_slices.full_limit()) is not None:
            return _error(429, OPEN_BYTES_ERROR if open_limit == OPEN_LIMIT_BYTES else OPEN_COUNT_ERROR, retry)
        # Taken with no await after the check, so concurrent requests cannot
        # all pass it; counted from now, while the slice is built.
        reservation = open_slices.reserve()
        await build_slot.acquire()
        build = asyncio.ensure_future(
            build_slice(
                slice_request,
                worker,
                plan,
                flush=_flush_recorder,
                flush_postgres=_flush_postgres,
                flush_memory=_flush_memory,
                on_postgres_failed=_log_postgres_failed,
            )
        )
        build.add_done_callback(_release)
        try:
            # Shielded: a request that goes away leaves the build running to
            # its end (at most the build time limit), then its file is closed
            # and the reservation released.
            built = await asyncio.shield(build)
        except asyncio.CancelledError:
            build.add_done_callback(_close_unsent)
            build.add_done_callback(lambda _done: reservation.release())
            raise
        except PostgresHistoryError as exc:
            reservation.release()
            await _log_postgres_failed(exc.reason, exc.error)
            return _error(503, POSTGRES_FAILED_ERROR)
        except (OSError, sqlite3.Error) as exc:
            reservation.release()
            await logger.awarning('debug_slice_failed', category='debug', error=str(exc), error_type=type(exc).__name__)
            return _error(500, f'{FAILED_ERROR_PREFIX}{exc}')
        except BaseException:
            reservation.release()
            raise
        return _slice_response(built, slice_request.compress, reservation)

    # --- the Postgres partitions ---

    async def _log_partitions_failed(reason: str, error: str) -> None:
        """``ui_read_postgres_failed`` of the partitions read, at most one line per minute."""
        await deps.postgres_read_log.emit(
            POSTGRES_PARTITIONS_ENDPOINT,
            logger.awarning,
            'ui_read_postgres_failed',
            category='debug',
            endpoint=POSTGRES_PARTITIONS_ENDPOINT,
            reason=reason,
            error=error,
        )

    async def _log_partitions_close_failed(error: str) -> None:
        await _log_partitions_failed('close', error)

    @router.get(POSTGRES_PARTITIONS_ENDPOINT)
    async def api_debug_databases_postgres():
        """The hour partitions of the recorder table and the next drops; 404 unless the provider is postgres.

        One connection of its own and one of the shared UI read slots, on the
        API server's loop (asyncpg does not block it); 503 when the read
        fails, runs out of time or finds no free slot.
        """
        settings = recorder.postgres_settings
        if settings is None:
            return _error(404, NOT_POSTGRES_ERROR)
        try:
            table = await read_table_partitions(
                settings,
                max_seconds=recorder.config.search.timeout,
                on_close_failed=_log_partitions_close_failed,
            )
        except PostgresHistoryError as exc:
            await _log_partitions_failed(exc.reason, exc.error)
            return _error(503, POSTGRES_FAILED_ERROR)
        return JSONResponse(
            {
                'server_now': table.server_now,
                'stored_retention_hours': table.stored_retention_hours,
                'partitions': [
                    {'name': row.name, 'from_ts': row.from_ts, 'to_ts': row.to_ts, 'size_bytes': row.size_bytes}
                    for row in table.partitions
                ],
                'total_bytes': table.total_bytes,
                'next_drops': {'retention': table.retention_drops, 'size': table.size_drops},
            }
        )

    # --- autodiscover ---

    @router.get(AUTODISCOVER_ENDPOINT)
    async def api_debug_autodiscover():
        """The registry and its cards from the last refresh; no store is read. 404 with autodiscover off."""
        config = drakkar_app._config
        fleet = drakkar_app.fleet
        if not config.autodiscover.enabled or fleet is None:
            return _error(404, AUTODISCOVER_OFF_ERROR)
        shared_dir = config.resolved_shared_dir

        def key_of(worker: str) -> str | None:
            return display_key(fleet.card_key(worker), shared_dir)

        identity = fleet.identity
        rows, truncated = card_rows(fleet.peer_views(), own_cluster=identity.cluster, card_key=key_of)
        return JSONResponse(
            {
                'registry': registry_header(
                    fleet.status(),
                    connection=registry_connection(fleet.provider_name, config.autodiscover.connection),
                    offline_after_seconds=config.autodiscover.offline_after,
                ),
                'worker': {
                    'name': identity.worker,
                    'cluster': identity.cluster or None,
                    'instance': identity.instance,
                    'key': key_of(identity.worker),
                },
                'workers': rows,
                'truncated': truncated,
            }
        )

    async def _log_provider_read_failed(provider: str, error: str) -> None:
        """``ui_read_provider_failed`` of the leases read, at most one line per minute."""
        await deps.postgres_read_log.emit(
            LEASES_ENDPOINT,
            logger.awarning,
            'ui_read_provider_failed',
            category='debug',
            endpoint=LEASES_ENDPOINT,
            provider=provider,
            op=HOLDER_OP,
            error=error,
        )

    @router.get(LEASES_ENDPOINT)
    async def api_debug_autodiscover_leases():
        """Each cluster task of this worker and who holds its lease; the read never starts the election.

        The rows are built here from the roster and memory. Only the holder
        reads run on the main loop, where the provider lives, through the
        election's UI caller (op ``holder``, a breaker of its own), one batch
        per worker at a time and within the main-loop dispatch limit. A row
        whose read did not answer is ``failed``; every row is ``busy`` while
        another request's batch runs.
        """
        election = drakkar_app.cluster_election
        roster = drakkar_app.periodic_roster
        if election is None or roster is None:
            return JSONResponse({'election': None, 'leases': []})
        skeletons = lease_skeletons(
            roster.definitions,
            cluster=recorder.cluster_name,
            now=time.time(),
            last_slots=election.last_slots,
        )
        started = election.started and not election.stopped
        results: dict[str, Any] = {}
        busy = False
        if started and skeletons:
            answered = await dispatch_bounded(
                election.read_holders([skeleton.scope for skeleton in skeletons]), drakkar_app.main_loop
            )
            busy = answered == HOLDER_READS_BUSY
            results = answered if isinstance(answered, dict) else {}
        shared_dir = drakkar_app._config.resolved_shared_dir
        rows = []
        failures: list[str] = []
        for skeleton in skeletons:
            result = results.get(skeleton.scope)
            status, holder = (LEASE_BUSY, None) if busy else lease_status(started, result)
            if status == LEASE_FAILED:
                failures.append(_failure_text(result))
            key = _lease_key(election, skeleton.scope, shared_dir) if election.started else None
            rows.append(lease_row(skeleton, status=status, holder=holder, key=key))
        if failures:
            await _log_provider_read_failed(election.provider_name, failures[0])
        return JSONResponse(
            {
                'election': {
                    'provider': election.provider_name,
                    'started': election.started,
                    'breaker_open': election.caller.breaker_open,
                },
                'leases': rows,
            }
        )

    return router


def _failure_text(result: Any) -> str:
    """The error text of a failed holder read, for the log line."""
    if isinstance(result, CallResult):
        return f'holder {result.outcome}'
    if isinstance(result, BaseException):
        return describe_error(result)
    return 'the main loop did not answer in time'


def _lease_key(election: ClusterElection, scope: str, shared_dir: str) -> str | None:
    """Where the provider keeps the lease of ``scope``, as the UI shows it; ``None`` when it cannot tell."""
    try:
        return display_key(election.provider.lease_key(scope), shared_dir)
    except Exception:
        # A display value only: a provider that cannot name the key shows none.
        return None
