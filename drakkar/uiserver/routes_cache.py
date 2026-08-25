"""Cache JSON API: ``/api/debug/cache/*``.

Routes:
  * ``/api/debug/cache/entries``      — paginated cache entry listing.
  * ``/api/debug/cache/entry/{key}``  — single entry by exact key.
  * ``/api/debug/cache/stats``        — gauge snapshot for the four cache metrics.

All routes 404 when the cache is disabled (``cache_engine`` is None).
This keeps stale bookmarks / open browser tabs from rendering a
half-broken page after a config change. The reader connection on the
engine is shared with ``Cache.get`` fallback — no additional thread is
spun up for UI queries; SELECTs run on the same aiosqlite worker thread.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from drakkar.concurrency import dispatch_to_loop
from drakkar.metrics import cache_gauge_snapshot

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()


def create_cache_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns ``/api/debug/cache/*`` endpoints."""
    # Cache routes expose key/value contents — gate the whole router
    # behind require_auth (no-op without a token).
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    drakkar_app = deps.drakkar_app

    def _cache_reader_or_404():
        """Fetch the shared reader connection or raise 404.

        All cache routes (HTML + JSON) funnel through this so we have a
        single source of truth for "cache not active". Returns the
        aiosqlite connection; the caller uses it like any other reader DB.

        Uses the public ``reader_db`` property on ``CacheEngine`` rather
        than reaching into the underscore-prefixed attribute so the
        encapsulation of the engine stays intact.
        """
        engine = drakkar_app.cache_engine
        if engine is None or engine.reader_db is None:
            raise HTTPException(status_code=404, detail='Cache is disabled')
        return engine.reader_db

    @router.get('/api/v1/debug/cache/entries')
    async def api_debug_cache_entries(
        # ``ge=0, le=1000`` enforces the bounds at the FastAPI layer —
        # requests outside the range get a 422 response instead of reaching
        # the handler. Default 200 mirrors the UI page size.
        limit: int = Query(default=200, ge=0, le=1000),
        offset: int = Query(default=0, ge=0),
        scope: str | None = Query(default=None),
        search: str | None = Query(default=None),
        expired_only: bool = Query(default=False),
    ):
        """Paginated listing of cache rows with optional filters.

        Query params:
          limit         — rows per page (default 200, enforced [0, 1000])
          offset        — pagination offset
          scope         — exact scope match (``local``/``cluster``/``global``;
                          ``memory``-scoped entries never reach the DB, so
                          this browser never lists them)
          search        — substring match against key (case-sensitive)
          expired_only  — show only expired rows (``expires_at_ms <= now_ms``)

        Returns ``{entries, total, limit, offset}``; ``total`` is the count
        matching the filters (not the clamped-page length), so the UI can
        render "N of M" pagination without a second round-trip.
        """
        reader = _cache_reader_or_404()

        conditions: list[str] = []
        params: list = []
        if scope is not None:
            conditions.append('scope = ?')
            params.append(scope)
        if search:
            # SQL LIKE with a substring pattern (``%search%``). User-typed
            # input can contain literal ``%`` or ``_`` which LIKE would
            # otherwise interpret as wildcards. We pick ``|`` as the ESCAPE
            # char (not ``\\`` — SQLite + Python string-escaping gets
            # brittle with backslashes) and prefix each wildcard char
            # with it.
            conditions.append("key LIKE ? ESCAPE '|'")
            safe_search = search.replace('|', '||').replace('%', '|%').replace('_', '|_')
            params.append('%' + safe_search + '%')
        if expired_only:
            now_ms = int(time.time() * 1000)
            # ``<= ?`` matches the inclusive cleanup convention in
            # ``drakkar.cache`` — an entry whose ``expires_at_ms`` equals
            # ``now_ms`` is expired and should surface in the expired_only
            # filter too.
            conditions.append('expires_at_ms IS NOT NULL AND expires_at_ms <= ?')
            params.append(now_ms)

        where = f'WHERE {" AND ".join(conditions)}' if conditions else ''

        # total count for pagination. A DB corruption or schema drift would
        # otherwise surface as "empty cache" in the UI — log at warning
        # so operators see the signal even when the UI masks the failure.
        # The cache reader aiosqlite connection is bound to the main
        # loop (opened inside ``CacheEngine.start()``), so dispatch the
        # COUNT there too.
        async def _read_count():
            async with reader.execute(f'SELECT COUNT(*) FROM cache_entries {where}', params) as cursor:
                return await cursor.fetchone()

        try:
            row = await dispatch_to_loop(_read_count(), deps.drakkar_app.main_loop)
            total = row[0] if row else 0
        except Exception as exc:
            await logger.awarning(
                'debug_cache_entries_count_failed',
                category='debug',
                error=str(exc),
                where=where,
            )
            total = 0

        entries: list[dict] = []
        if limit > 0:
            query = (
                'SELECT key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                'expires_at_ms, origin_worker_id FROM cache_entries '
                f'{where} ORDER BY updated_at_ms DESC LIMIT ? OFFSET ?'
            )

            async def _read_rows():
                async with reader.execute(query, [*params, limit, offset]) as cursor:
                    columns = [d[0] for d in cursor.description]
                    rows = await cursor.fetchall()
                return columns, rows

            try:
                columns, rows = await dispatch_to_loop(_read_rows(), deps.drakkar_app.main_loop)
                for r in rows:
                    entries.append(dict(zip(columns, r, strict=False)))
            except Exception as exc:
                await logger.awarning(
                    'debug_cache_entries_query_failed',
                    category='debug',
                    error=str(exc),
                    where=where,
                    limit=limit,
                    offset=offset,
                )
                entries = []

        return JSONResponse(
            {
                'entries': entries,
                'total': total,
                'limit': limit,
                'offset': offset,
            }
        )

    @router.get('/api/v1/debug/cache/entry/{key:path}')
    async def api_debug_cache_entry(key: str):
        """Return a single entry by exact key, with the value decoded from JSON.

        Uses ``{key:path}`` so colons (a common separator in cache keys) and
        other URL-special chars pass through unchanged. 404 when the key
        doesn't exist. On JSON decode failure (corruption / legacy data),
        the ``raw_value`` field carries the original string for the UI to
        display as-is.
        """
        reader = _cache_reader_or_404()

        # Reads via the cache engine's reader connection rather than the
        # recorder's ``flush_and_select`` helper: this endpoint queries
        # the cache_entries table, not the events table, and there's
        # nothing to flush on the recorder side.
        async def _read():
            async with reader.execute(
                'SELECT key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                'expires_at_ms, origin_worker_id FROM cache_entries WHERE key = ?',
                (key,),
            ) as cursor:
                columns = [d[0] for d in cursor.description]
                row = await cursor.fetchone()
            return columns, row

        try:
            columns, row = await dispatch_to_loop(_read(), deps.drakkar_app.main_loop)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f'Failed to read cache entry: {exc}') from exc

        if row is None:
            raise HTTPException(status_code=404, detail='Cache entry not found')

        entry = dict(zip(columns, row, strict=False))
        # Try to decode the JSON value; on failure carry the raw string so
        # the UI can show something rather than 500ing the request.
        raw_value = entry.pop('value')
        try:
            entry['value'] = json.loads(raw_value)
            entry['raw_value'] = raw_value
        except (json.JSONDecodeError, TypeError):
            entry['value'] = None
            entry['raw_value'] = raw_value

        return JSONResponse(entry)

    @router.get('/api/v1/debug/cache/stats')
    async def api_debug_cache_stats():
        """Return a snapshot of the four cache gauges.

        Values come from the live Prometheus gauges — same numbers you'd
        see in the /metrics scrape, just wrapped in a JSON envelope for
        the UI's stat cards. Reading a gauge is O(1); we never walk the
        DB or memory dict here.

        Delegates to ``metrics.cache_gauge_snapshot`` so the endpoint
        doesn't depend on prometheus_client internals (``_value.get()``
        was a private attribute and could break silently on a library
        upgrade).
        """
        if drakkar_app.cache_engine is None:
            raise HTTPException(status_code=404, detail='Cache is disabled')

        return JSONResponse(cache_gauge_snapshot())

    return router
