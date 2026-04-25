"""SQLite schema + LWW UPSERT statement + runtime tunables for the cache.

Kept separate from the cache classes so the DDL/DML can be inspected
without pulling in the engine's runtime imports (asyncio, structlog,
peer-discovery, etc.). Tests that only need the schema text — for
example, to materialise a peer DB in a fixture — depend on this module
alone.
"""

from __future__ import annotations

# `cache_entries` is the single table backing every worker's cache DB file.
# The schema mirrors `CacheEntry` one-to-one so the flush path can bind
# dataclass fields directly to SQL parameters without translation. The
# CHECK on `scope` keeps `CacheScope` values enforced at the DB layer too —
# a corrupt peer that tried to write a bogus scope would be rejected at
# UPSERT time rather than silently corrupting our store.
#
# Two indexes:
#
# - ``idx_cache_expires`` is partial (only rows with a TTL), to speed up the
#   cleanup loop's "DELETE WHERE expires_at_ms <= now" without widening the
#   index to cover NULL rows that never expire.
# - ``idx_cache_scope_updated`` is composite; the peer-sync loop filters
#   rows by ``scope IN (...)`` and orders them by ``updated_at_ms`` for
#   cursor-based pagination. Having both columns in one index means the
#   planner can walk rows in exactly the cursor order.
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


# ---- runtime tunables (non-SQL) --------------------------------------------

# Hardcoded TTL (seconds) for the in-process peer-cluster_name lookup cache.
# When peer-sync encounters a new peer, it reads the peer's ``-live.db``
# ``worker_config`` row to learn the peer's ``cluster_name``. That read is
# cached for this long, after which the next sync cycle re-reads. Operators
# rarely change cluster_name at runtime — exposing the TTL as a config knob
# would add surface without real benefit. YAGNI; bump if someone actually
# needs it.
#
# Kept separate from the SQL constants above so "which of these knobs are
# runtime tunables vs DDL/DML?" is obvious at a glance.
PEER_CLUSTER_CACHE_TTL_SECONDS = 300.0
