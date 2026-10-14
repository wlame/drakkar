"""SQLite schema + LWW UPSERT statement + runtime tunables for the cache.

Kept separate from the cache classes so the DDL/DML can be inspected
without pulling in the engine's runtime imports (asyncio, structlog,
httpx, etc.). Tests that only need the schema text — for example, to
materialise a peer's cache file in a fixture — depend on this module
alone.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

# `cache_entries` is the single table backing every worker's cache DB file.
# The schema mirrors `CacheEntry` one-to-one so the flush path can bind
# dataclass fields directly to SQL parameters without translation. The
# CHECK on `scope` keeps `CacheScope` values enforced at the DB layer too —
# a corrupt peer that tried to write a bogus scope would be rejected at
# UPSERT time rather than silently corrupting our store. `'memory'` is
# deliberately NOT in the CHECK: memory-scoped entries never flush (set()
# records a DELETE tombstone instead), so a `'memory'` row reaching this
# table is a bug and the constraint makes it fail loudly.
#
# Two indexes:
#
# - ``idx_cache_expires`` is partial (only rows with a TTL), to speed up the
#   cleanup loop's "DELETE WHERE expires_at_ms <= now" without widening the
#   index to cover NULL rows that never expire.
# - ``idx_cache_scope_updated`` is composite; the changes query a peer
#   pulls through searches one scope at a time and orders the rows by
#   ``updated_at_ms`` for cursor-based pagination. Having both columns in
#   one index means the planner walks the rows of each scope in the cursor
#   order and stops at the page size.
SCHEMA_CACHE_ENTRIES = """
CREATE TABLE IF NOT EXISTS cache_entries (
    key               TEXT    NOT NULL PRIMARY KEY,
    scope             TEXT    NOT NULL CHECK(scope IN ('local','cluster','global')),
    value             TEXT    NOT NULL,
    size_bytes        INTEGER NOT NULL,
    created_at_ms     INTEGER NOT NULL,
    updated_at_ms     INTEGER NOT NULL,
    expires_at_ms     INTEGER,
    origin_worker_id  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cache_expires
    ON cache_entries(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cache_scope_updated
    ON cache_entries(scope, updated_at_ms);
"""


# LWW UPSERT — Last-Write-Wins conflict resolution for the cache_entries
# table. Single source of truth: this same SQL runs from the local flush
# path and the peer-sync apply path, so a local write and a peer-supplied
# write cannot take different routes through the resolution logic — they
# resolve identically.
#
# Conflict rules (the WHERE clause on the UPDATE branch):
#
# 1. If the incoming row's ``updated_at_ms`` is strictly greater than the
#    stored row's, accept the incoming row (it is "newer").
# 2. If the two ``updated_at_ms`` values are equal, the row whose
#    ``origin_worker_id`` is lexicographically smaller wins — a stable,
#    deterministic tiebreak so two workers observing the same conflict
#    independently converge on the same survivor. This matters during
#    peer-sync where both sides apply the same UPSERT.
# 3. Otherwise (older, or equal-ts with larger origin), the stored row
#    is kept — the UPDATE branch's WHERE clause evaluates to false, so
#    the ON CONFLICT effectively becomes a no-op for that row. This is
#    the expected behavior: SQLite does not raise when the WHERE excludes
#    the row; the INSERT simply does not take effect.
#
# Why not a pure INSERT OR REPLACE? REPLACE drops existing rows
# unconditionally, losing the LWW guarantee: a peer with a stale row
# would clobber our fresh write as soon as it synced. The guarded UPSERT
# above is the minimum SQL needed for eventual consistency under LWW.
LWW_UPSERT_SQL = """
INSERT INTO cache_entries
    (key, scope, value, size_bytes, created_at_ms, updated_at_ms, expires_at_ms, origin_worker_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
    scope = excluded.scope,
    value = excluded.value,
    size_bytes = excluded.size_bytes,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms,
    expires_at_ms = excluded.expires_at_ms,
    origin_worker_id = excluded.origin_worker_id
WHERE
    excluded.updated_at_ms > cache_entries.updated_at_ms
    OR (
        excluded.updated_at_ms = cache_entries.updated_at_ms
        AND excluded.origin_worker_id < cache_entries.origin_worker_id
    )
"""


# ---- cache exchange: the serving side ---------------------------------------

# Column order of a served row; the answer keys use the same names.
CHANGES_FIELDS: tuple[str, ...] = (
    'key',
    'scope',
    'value',
    'size_bytes',
    'created_at_ms',
    'updated_at_ms',
    'expires_at_ms',
    'origin_worker_id',
)


def changes_query(
    *, origin: str, scopes: Sequence[str], now_ms: int, after: tuple[int, str] | None, limit: int
) -> tuple[str, list[Any]]:
    """The SQL and parameters of one page of ``GET /api/v1/peer/cache/changes``.

    Own-origin rows of the requested scopes that are not expired at the
    server's ``now``, after the ``(updated_at_ms, key)`` cursor, in cursor
    order, at most ``limit`` rows. The row-value comparison keeps rows that
    share one ``updated_at_ms`` apart by key, so a page boundary inside such
    a group neither loses nor repeats a row.

    One sub-select per scope, each walking ``idx_cache_scope_updated`` and
    stopping at ``limit`` rows, joined with ``UNION ALL`` under an outer
    order and limit. A single ``scope IN (...)`` query sorted every row of
    both scopes after the cursor on every page; this one reads at most
    ``limit`` rows per scope.
    """
    cursor = 'AND (updated_at_ms, key) > (?, ?) ' if after is not None else ''
    one_scope = (
        f'SELECT {", ".join(CHANGES_FIELDS)} FROM cache_entries '
        'WHERE origin_worker_id = ? AND scope = ? '
        'AND (expires_at_ms IS NULL OR expires_at_ms > ?) '
        f'{cursor}ORDER BY updated_at_ms, key LIMIT ?'
    )
    params: list[Any] = []
    for scope in scopes:
        params += [origin, scope, now_ms, *(after or ()), limit]
    if len(scopes) == 1:
        return one_scope, params
    parts = ' UNION ALL '.join(f'SELECT * FROM ({one_scope})' for _scope in scopes)
    return f'{parts} ORDER BY updated_at_ms, key LIMIT ?', [*params, limit]


# The newest own-origin row of one scope: the high-water mark the worker
# card carries, read once at start. Parameters: origin, scope.
OWN_MARK_SQL = (
    'SELECT updated_at_ms, key FROM cache_entries '
    'WHERE origin_worker_id = ? AND scope = ? '
    'ORDER BY updated_at_ms DESC, key DESC LIMIT 1'
)


# ---- runtime tunables (non-SQL) --------------------------------------------

# Explicit busy timeout of the engine's own writer and reader, in
# milliseconds: the local engine rides out a checkpoint rather than failing.
BUSY_TIMEOUT_MS = 10000
