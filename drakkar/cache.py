"""Handler-accessible key/value cache: models, scope enum, and JSON codec.

This module holds the building blocks used by the cache engine (added in
later tasks) and the handler-facing Cache API. Keeping the models and codec
isolated lets tests exercise the serialization contract without spinning up
SQLite, and makes the encode/decode semantics easy to reason about in
isolation — every `set` flows through `_encode`, every `get` flows through
`_decode`.

Scope model
-----------
`CacheScope` names the visibility contract a cache entry has in peer-sync:

- ``LOCAL``  — this worker only; never pulled by peers
- ``CLUSTER`` — workers with the same ``cluster_name``; pulled by same-cluster peers
- ``GLOBAL`` — visible to all peers regardless of cluster

The enum uses Python's ``StrEnum`` so members double as strings for SQL
parameter binding and JSON round-tripping with zero coercion.

Serialization (JSON-only in v1)
-------------------------------
We deliberately restrict cached values to JSON-serializable shapes:
primitives, lists, dicts, and Pydantic models (which serialize via
``model_dump_json``). The rationale is three-fold:

1. **Cross-worker portability.** Peer-sync moves entries across workers that
   may run different Python minor versions or different code revisions.
   JSON is the lowest-common-denominator format — a row written by
   worker-A on Python 3.13 will parse cleanly on worker-B running 3.14.

2. **Inspectability.** The debug UI shows cached values directly from the
   DB. JSON is human-readable; opaque binary blobs would force us to
   embed a decoder into the UI server per type.

3. **Safety.** Binary object serializers permit arbitrary code execution
   on load. Since peers can write to each other's DBs via sync, a
   compromised peer with such a codec would become an RCE vector. JSON
   parsing is bounded to data values — no code paths to execute.

For Pydantic-typed values, callers can pass ``as_type=SomeModel`` to
``Cache.get()`` (added in Task 9) and ``_decode`` will revive the value
through ``model_validate`` rather than returning a plain dict.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite
import structlog
from pydantic import BaseModel

from drakkar import metrics

# Imported at module scope (not inside ``start()``) so that tests can
# ``monkeypatch.setattr(cache_module, 'run_periodic_task', ...)`` — a local
# import would bind to the concrete function each call and defeat the spy.
from drakkar.periodic import run_periodic_task

if TYPE_CHECKING:
    from drakkar.config import CacheConfig, DebugConfig
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# ---- scope enum -------------------------------------------------------------


class CacheScope(StrEnum):
    """Visibility discriminator for a cache entry.

    The string values match the SQL schema's ``CHECK(scope IN (...))``
    constraint and the peer-sync scope-filter WHERE clauses. Keep the
    values in lockstep with ``cache_entries.scope`` column constraints.

    StrEnum inheritance means ``CacheScope.LOCAL`` is both a distinct
    enum member and an ordinary string equal to ``'local'`` — useful
    when binding SQL parameters or serializing to JSON.
    """

    LOCAL = 'local'
    CLUSTER = 'cluster'
    GLOBAL = 'global'


# ---- cache entry dataclass --------------------------------------------------


@dataclass(slots=True)
class CacheEntry:
    """In-memory representation of one cached row, mirroring the SQL schema.

    Fields:
        key: primary key, unique per worker DB.
        scope: visibility (see ``CacheScope``).
        value: serialized JSON text — opaque to the engine, typed by the
            caller via ``_encode`` / ``_decode``.
        size_bytes: UTF-8 byte length of ``value``. Caller populates this
            at construction time from ``_encode``'s second return value;
            we store it rather than recomputing so the Prometheus
            ``bytes_in_memory`` gauge can maintain a running sum without
            walking the dict on every scrape.
        created_at_ms: wall-clock ms when this key was first stored.
        updated_at_ms: wall-clock ms of the most recent write. Drives LWW
            conflict resolution during peer sync.
        expires_at_ms: wall-clock ms after which the entry is dead;
            ``None`` means "never expires".
        origin_worker_id: the worker that performed the write. Used as
            the lexicographic tiebreaker when two entries share
            ``updated_at_ms`` during LWW resolution.

    Using ``slots=True`` cuts the per-entry memory footprint since the
    in-memory dict can hold tens of thousands of entries; a regular
    dataclass would carry the full ``__dict__`` per instance.
    """

    key: str
    scope: CacheScope
    value: str
    size_bytes: int
    created_at_ms: int
    updated_at_ms: int
    expires_at_ms: int | None
    origin_worker_id: str


# ---- JSON codec -------------------------------------------------------------


def _encode(value: Any) -> tuple[str, int]:
    """Serialize a value to JSON text and report its UTF-8 byte length.

    The byte length is what SQLite will store and what Prometheus gauges
    count — we compute it once here so callers never have to re-encode
    just to learn the size.

    Pydantic models go through ``model_dump_json()`` rather than being
    handed to ``json.dumps`` — the latter would hit ``__dict__`` or fail
    outright, missing field aliases, ``Enum``/``datetime`` coercion, and
    the strict serialization mode the user declared on the model.

    All other inputs (primitives, lists, dicts) go through ``json.dumps``.
    Anything that isn't JSON-serializable raises ``TypeError`` — that's
    intentional: we want a clear error at ``set()`` time rather than a
    silent failure at read time on a peer that can't parse the value.

    Args:
        value: any JSON-serializable object, or a Pydantic ``BaseModel``.

    Returns:
        ``(json_text, byte_length)`` where ``byte_length`` is the number
        of UTF-8 bytes in ``json_text``.
    """
    # By the way: we use `isinstance` rather than duck-typing on
    # `model_dump_json` — a plain dict with a `model_dump_json` method
    # is not a Pydantic model and should take the json.dumps path.
    text = value.model_dump_json() if isinstance(value, BaseModel) else json.dumps(value)
    return text, len(text.encode('utf-8'))


def _decode[T: BaseModel](json_text: str, *, as_type: type[T] | None = None) -> Any:
    """Parse JSON text back to a Python value.

    Without ``as_type``, returns the raw ``json.loads`` result — primitives
    stay primitives, lists stay lists, dicts stay plain dicts. No magic
    revival: a dict-shaped payload will not be silently upcast to a
    Pydantic model, even if the caller "meant" one. Explicit is better
    than implicit.

    With ``as_type=SomeModel``, the parsed value is handed to
    ``SomeModel.model_validate(...)`` — Pydantic's standard entry point
    for external-data validation. Use this when the caller knows the
    value was stored from a specific Pydantic model and wants a typed
    instance back.

    Raises:
        json.JSONDecodeError: if ``json_text`` is not valid JSON (including
            an empty string). Surfacing the error rather than returning
            ``None`` makes data-corruption bugs visible immediately.
        pydantic.ValidationError: if ``as_type`` is provided and the
            parsed value doesn't match the model's declared shape.
    """
    parsed = json.loads(json_text)
    if as_type is not None:
        return as_type.model_validate(parsed)
    return parsed


# ---- dirty-op tracking ------------------------------------------------------
#
# The Cache accumulates pending writes in a ``_dirty`` map keyed by cache
# key. Each entry records what the next flush should do: either UPSERT a
# value (``Op.SET`` with the current ``CacheEntry`` payload) or DELETE a
# row (``Op.DELETE``, no payload needed — the key is enough for SQL).
#
# Flush (added in Task 7) swaps the ``_dirty`` map atomically under the
# GIL:
#
#     snapshot, self._dirty = self._dirty, {}
#
# Any ``set`` or ``delete`` that lands **during** the flush writes into the
# fresh empty dict and gets picked up by the next cycle — no locking needed
# for in-memory coherence because Python dict ops are single-operation atomic
# under CPython's GIL. (If we ever run on a free-threaded interpreter, we'd
# wrap mutation in a lock; that's out of scope for v1.)


class Op(Enum):
    """What the flush worker should do with a dirty key.

    ``SET`` → upsert the attached ``CacheEntry`` into ``cache_entries``
    via the ``LWW_UPSERT_SQL`` (Task 7).

    ``DELETE`` → remove the row from ``cache_entries`` by key. Local-only:
    deletes do not propagate to peers (documented sharp edge — see plan
    "Delete is deliberately local-only").
    """

    SET = 'set'
    DELETE = 'delete'


@dataclass(slots=True)
class DirtyOp:
    """One pending mutation awaiting flush to the local SQLite DB.

    Fields:
        op: the operation kind — SET or DELETE.
        entry: the ``CacheEntry`` to upsert for SET ops; ``None`` for DELETE
            (the row removal only needs the key, which is the dict key of
            the dirty map itself).

    The entry reference is kept so flush can serialize the current value
    without another ``_memory`` lookup — necessary because the entry may
    have been LRU-evicted from ``_memory`` between the mutation and the
    flush. The DB is the source of truth, but the dirty map is the pipeline
    that gets writes there; losing a dirty op because the entry was evicted
    from memory would silently drop the write.
    """

    op: Op
    entry: CacheEntry | None


# ---- handler-facing cache ---------------------------------------------------


def _now_ms() -> int:
    """Wall-clock milliseconds since epoch.

    Used for ``created_at_ms`` / ``updated_at_ms`` / ``expires_at_ms``.
    Wall-clock is deliberate (not ``time.monotonic``): these timestamps
    participate in LWW resolution across workers, where each worker's
    monotonic clock has a different zero. Comparing wall-clock across
    workers assumes NTP-sync'd hosts — good enough for eventual consistency,
    and the usual baseline for LWW-based designs.
    """
    return int(time.time() * 1000)


class Cache:
    """In-memory key/value cache surfaced to handlers as ``self.cache``.

    This class covers the **synchronous**, memory-only operations of the
    cache contract: ``set``, ``peek``, ``delete``, ``__contains__``. All of
    these are GIL-safe dict manipulations; no I/O happens on these calls.

    The async DB-backed ``get()`` (with memory-miss → DB fallback) lands in
    Task 9 together with the reader connection. Flush to SQLite (Task 7)
    and peer sync (Tasks 11-13) read from the ``_dirty`` map this class
    populates.

    Why ``OrderedDict``?
    --------------------
    LRU eviction (when ``max_memory_entries`` is set) relies on access
    order. ``OrderedDict.move_to_end(key)`` is O(1) and the class preserves
    insertion order by contract — regular ``dict`` makes insertion order
    guarantees too, but lacks the ``move_to_end`` primitive, so we'd
    otherwise have to pop-and-reinsert. OrderedDict is the canonical fit.

    Eviction policy
    ---------------
    When ``max_memory_entries`` is set and the dict would grow past the
    cap, we pop the LRU key (the first item in iteration order). The
    evicted key's dirty-op is **preserved** — the DB is the source of
    truth, and any pending SET still needs to reach it via flush. Losing
    the dirty entry would silently drop the write. Evicted entries fall
    through to the DB on next read; the DB will re-warm ``_memory`` in
    ``get()`` (Task 9).
    """

    def __init__(
        self,
        *,
        origin_worker_id: str,
        max_memory_entries: int | None = None,
    ) -> None:
        """Construct a Cache for a single handler.

        Args:
            origin_worker_id: the stable identifier for this worker, stored
                on every written entry for LWW tiebreaks during peer sync.
            max_memory_entries: optional LRU cap. ``None`` (default) means
                unbounded — the dict grows at the caller's discretion.
        """
        # insertion-ordered; ``move_to_end`` bumps a key to MRU position
        self._memory: OrderedDict[str, CacheEntry] = OrderedDict()
        # pending mutations for the next flush (Task 7 consumes this)
        self._dirty: dict[str, DirtyOp] = {}
        self._origin_worker_id = origin_worker_id
        self._max_memory_entries = max_memory_entries
        # Reader-only aiosqlite connection. ``CacheEngine.start()`` wires
        # this in via ``attach_reader_db`` so ``get()`` can fall through to
        # SQLite on a memory miss without touching the writer connection —
        # the writer's flush/cleanup/sync commits and the reader's SELECTs
        # run on separate aiosqlite worker threads, and WAL mode lets the
        # reader see consistent snapshots concurrent with writes.
        self._reader_db: aiosqlite.Connection | None = None

    # -- sync API -------------------------------------------------------------

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = CacheScope.LOCAL,
    ) -> None:
        """Store ``value`` under ``key`` in memory and mark it dirty for flush.

        Args:
            key: unique key within the worker's cache namespace.
            value: any JSON-serializable object, or a Pydantic model
                (serialized via ``model_dump_json``). See ``_encode``.
            ttl: optional time-to-live in **seconds**; ``None`` means
                "never expires". Internally we convert to a wall-clock
                ``expires_at_ms`` so downstream comparisons are trivial.
            scope: visibility for peer sync (default LOCAL — safe choice;
                LOCAL entries never leave this worker).

        Side effects:
            - Writes to ``_memory`` (creates or overwrites).
            - Writes to ``_dirty`` with an ``Op.SET`` referencing the entry.
            - Bumps the key to MRU position in the OrderedDict.
            - May trigger LRU eviction if ``max_memory_entries`` is set
              and the cap is now exceeded.
        """
        encoded, size_bytes = _encode(value)
        now_ms = _now_ms()
        expires_at_ms = now_ms + int(ttl * 1000) if ttl is not None else None

        # Preserve ``created_at_ms`` across overwrites — the column is
        # "when did this key first appear", not "when was this value
        # written". ``updated_at_ms`` is the one that changes every set.
        existing = self._memory.get(key)
        created_at_ms = existing.created_at_ms if existing is not None else now_ms

        entry = CacheEntry(
            key=key,
            scope=scope,
            value=encoded,
            size_bytes=size_bytes,
            created_at_ms=created_at_ms,
            updated_at_ms=now_ms,
            expires_at_ms=expires_at_ms,
            origin_worker_id=self._origin_worker_id,
        )

        # Place in memory and bump to MRU. If the key already existed,
        # OrderedDict's insertion replaces the value but keeps its slot —
        # we then explicitly move_to_end so overwrites also count as a
        # recency bump (consistent with typical LRU intuition).
        self._memory[key] = entry
        self._memory.move_to_end(key)

        # Track dirty op. A SET overrides any prior DELETE for the same
        # key — that's the expected last-write-wins at the dirty-map level,
        # matching the SQL LWW at the DB level.
        self._dirty[key] = DirtyOp(op=Op.SET, entry=entry)

        self._maybe_evict()

    def peek(self, key: str) -> Any | None:
        """Return the decoded value if present **and** unexpired, else None.

        Pure memory lookup — no DB access. If the entry is expired, we
        opportunistically evict it so stale data doesn't linger until the
        cleanup cycle runs. ``peek`` also bumps the key to MRU position
        (the call counts as an access for LRU bookkeeping).

        Returns ``None`` for missing or expired keys. To distinguish the
        two cases, use ``__contains__`` — but usually callers don't need
        to.
        """
        entry = self._memory.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            # Opportunistic eviction. The dirty map is untouched: if there
            # was a pending SET, letting it flush and then be cleaned up
            # by the expiration cleanup cycle is cheaper than trying to
            # untangle it here. A pending DELETE stays as-is.
            self._memory.pop(key, None)
            return None
        # access → MRU bump
        self._memory.move_to_end(key)
        return _decode(entry.value)

    def attach_reader_db(self, reader_db: aiosqlite.Connection | None) -> None:
        """Wire the engine's reader aiosqlite connection into this Cache.

        Called by ``CacheEngine.start()`` after the reader connection is
        opened, and by ``CacheEngine.stop()`` (with ``None``) so follow-up
        ``get()`` calls short-circuit instead of hitting a closed connection.
        Keeping the reference on the Cache rather than routing every ``get``
        through the engine avoids a method-dispatch hop on the hot path.
        """
        self._reader_db = reader_db

    async def get[T: BaseModel](self, key: str, *, as_type: type[T] | None = None) -> Any | None:
        """Return the value for ``key``, falling through to SQLite on a memory miss.

        The lookup order:

        1. **Memory hit** — if the key exists in ``_memory`` and has not
           expired, return it without any DB access. LRU position bumps
           to MRU.
        2. **DB fallback** — on a memory miss (including after LRU eviction
           or a worker restart), run a SELECT on the reader connection.
           The SQL filter ``expires_at_ms IS NULL OR expires_at_ms > now``
           excludes expired rows from the DB layer too.
        3. **Warm memory** — on a DB hit, rebuild the ``CacheEntry`` from
           the row and insert it into ``_memory`` with MRU position. If
           ``max_memory_entries`` is set, this may trigger an LRU eviction.

        Args:
            key: the lookup key.
            as_type: optional Pydantic model class. When provided, the
                decoded JSON value is revived via ``model_validate`` for
                a typed return value; otherwise ``get`` returns the raw
                ``json.loads`` result (primitives / lists / dicts).

        Returns:
            The decoded (and optionally typed) value, or ``None`` if the
            key is absent or expired.

        Notes:
            - No DB access on a memory hit — the writer's flush/cleanup/
              sync traffic and the reader's SELECTs do not cross paths.
            - The reader connection is opened in ``CacheEngine.start()``;
              if the engine is disabled, ``_reader_db`` stays None and
              the DB-fallback branch is skipped.
        """
        # --- memory fast path ---
        entry = self._memory.get(key)
        if entry is not None:
            if self._is_expired(entry):
                # Mirror peek()'s opportunistic eviction on stale reads.
                self._memory.pop(key, None)
                # Do NOT consult the DB — if the memory entry is expired,
                # the DB row (if any) will also have expires_at_ms <= now.
                return None
            # access → MRU bump
            self._memory.move_to_end(key)
            return _decode(entry.value, as_type=as_type) if as_type is not None else _decode(entry.value)

        # --- DB fallback ---
        if self._reader_db is None:
            # Engine disabled or not yet started — treat as a miss.
            return None

        now_ms = _now_ms()
        cursor = await self._reader_db.execute(
            # SELECT only the columns we need to reconstruct a CacheEntry;
            # the WHERE clause filters out expired rows at the DB layer so
            # we don't have to post-filter in Python.
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries '
            'WHERE key = ? AND (expires_at_ms IS NULL OR expires_at_ms > ?)',
            (key, now_ms),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None

        # Rebuild the CacheEntry from the row and warm memory with it — a
        # subsequent get() for the same key will hit the memory fast path.
        db_key, db_scope, db_value, db_size, db_created, db_updated, db_expires, db_origin = row
        warmed = CacheEntry(
            key=db_key,
            scope=CacheScope(db_scope),
            value=db_value,
            size_bytes=db_size,
            created_at_ms=db_created,
            updated_at_ms=db_updated,
            expires_at_ms=db_expires,
            origin_worker_id=db_origin,
        )
        # Warm-on-read puts the key at MRU in the OrderedDict; the
        # _maybe_evict call below respects the cap (may drop an older key,
        # but preserves its dirty op if any — same invariant as set()).
        self._memory[db_key] = warmed
        self._memory.move_to_end(db_key)
        self._maybe_evict()

        return _decode(warmed.value, as_type=as_type) if as_type is not None else _decode(warmed.value)

    def delete(self, key: str) -> bool:
        """Remove ``key`` from memory and schedule a DB row deletion on next flush.

        Returns True if the key was present in memory, False otherwise.
        The dirty-op is always recorded (``Op.DELETE``) regardless of
        presence in memory — because the DB may still hold a row for this
        key that we can't see from memory alone.

        **Local-only**: this does NOT propagate to peers. A peer who synced
        our value before the delete still has its own copy and may re-push
        it back via LWW if its ``updated_at_ms`` is newer. See the "delete
        is deliberately local-only" note in the plan for the full edge
        discussion — use TTL for cross-worker invalidation.
        """
        present = key in self._memory
        self._memory.pop(key, None)
        # A DELETE op overwrites any prior SET; flush will see the final
        # state only. Entry is None since the row is going away.
        self._dirty[key] = DirtyOp(op=Op.DELETE, entry=None)
        return present

    def __contains__(self, key: str) -> bool:
        """Membership test including TTL check.

        Returns True if the key is in memory and unexpired; False otherwise.
        An expired entry is not a member. We do **not** bump LRU here —
        membership is a probe, not an access.
        """
        entry = self._memory.get(key)
        if entry is None:
            return False
        return not self._is_expired(entry)

    # -- internals ------------------------------------------------------------

    @staticmethod
    def _is_expired(entry: CacheEntry) -> bool:
        """True iff the entry has a TTL that has already elapsed."""
        if entry.expires_at_ms is None:
            return False
        return _now_ms() >= entry.expires_at_ms

    def _maybe_evict(self) -> None:
        """Drop the LRU key if the memory dict exceeds ``max_memory_entries``.

        Called after every ``set``. Evicts at most one entry per call —
        since we never bulk-insert without calls to ``set``, that matches
        the growth rate. The evicted key's dirty-op is preserved: the DB
        is the source of truth, and losing a pending SET before flush
        would silently drop the user's write.

        Note on ``popitem(last=False)``: OrderedDict's popitem pops the
        first inserted (i.e. least-recently-touched after our move_to_end
        calls), which is exactly LRU.
        """
        cap = self._max_memory_entries
        if cap is None:
            return
        while len(self._memory) > cap:
            # pop oldest by access order (move_to_end in set/peek keeps
            # recently-touched keys at the tail)
            self._memory.popitem(last=False)
            metrics.cache_evictions.inc()


# ---- SQLite schema + SQL constants -----------------------------------------

# `cache_entries` is the single table backing every worker's cache DB file.
# The schema mirrors `CacheEntry` one-to-one so the flush path (Task 7) can
# bind dataclass fields directly to SQL parameters without translation. The
# CHECK on `scope` keeps `CacheScope` values enforced at the DB layer too —
# a corrupt peer that tried to write a bogus scope would be rejected at
# UPSERT time rather than silently corrupting our store.
#
# Two indexes:
#
# - ``idx_cache_expires`` is partial (only rows with a TTL), to speed up the
#   cleanup loop's "DELETE WHERE expires_at_ms < now" without widening the
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
# path (Task 7) and the peer-sync apply path (Task 12), so a local write
# and a peer-supplied write cannot take different routes through the
# resolution logic — they resolve identically.
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


# ---- engine lifecycle -------------------------------------------------------


def _cache_link_path(db_dir: str, worker_id: str) -> str:
    """Return the peer-discovery symlink path: ``<db_dir>/<worker>-cache.db``.

    This is the *symlink* name, scanned by ``discover_peer_dbs(...,
    suffix='-cache.db')``. Peers find us by reading this symlink and
    following it to the underlying DB file (see ``_cache_db_file_path``).
    """
    return str(Path(db_dir) / f'{worker_id}-cache.db')


def _cache_db_file_path(db_dir: str, worker_id: str) -> str:
    """Return the actual DB file path: ``<db_dir>/<worker>-cache.db.actual``.

    The cache DB file lives under a ``.actual`` suffix so the stable
    ``<worker>-cache.db`` name can be reserved as a symlink target — the
    same convention peer discovery uses for recorder live links. Since
    the cache does not rotate files in v1, the ``.actual`` file is
    effectively permanent; the indirection exists purely so peer
    discovery's ``os.path.islink`` filter works identically for recorder
    and cache.
    """
    return str(Path(db_dir) / f'{worker_id}-cache.db.actual')


class CacheEngine:
    """Owns the SQLite backing store and periodic loops for a single worker's cache.

    Two connections (added in later tasks): a **writer** for flush/cleanup/
    sync-UPSERT, and a **reader** for ``Cache.get()`` DB fallback. aiosqlite
    spawns a dedicated OS thread per connection, so SQLite operations run
    off the event loop; WAL mode allows the reader to see consistent
    snapshots while the writer commits.

    Task 6 (current scope): writer connection lifecycle, schema init,
    symlink creation, graceful stop. The flush loop lands in Task 7/8, the
    reader + async ``get`` in Task 9, cleanup in Task 10, peer-sync in
    Tasks 11-13.

    The engine is constructed by the app during startup; ``start()`` opens
    the DB and prepares the periodic loops. If neither ``cache.db_dir`` nor
    ``debug.db_dir`` is set, ``start()`` logs a warning and puts the engine
    into an *effectively disabled* state — no connection is opened, no file
    is created, and subsequent flush/sync/cleanup calls become no-ops. The
    handler still sees a ``Cache`` instance; it just never persists.
    """

    def __init__(
        self,
        config: CacheConfig,
        debug_config: DebugConfig,
        worker_id: str,
        cluster_name: str,
        recorder: EventRecorder | None = None,
    ) -> None:
        """Wire up an engine without opening any resources.

        Args:
            config: cache-specific settings (enabled flag, intervals,
                peer-sync, memory cap, optional dedicated db_dir).
            debug_config: referenced only for its ``db_dir`` fallback when
                ``config.db_dir`` is empty. The engine does NOT otherwise
                reach into debug settings.
            worker_id: stable identifier for this worker — used for the
                DB filename, the symlink name, peer-discovery self-filter,
                and the ``origin_worker_id`` column on every write.
            cluster_name: used later by peer-sync to decide whether to
                pull ``cluster``-scoped rows from a given peer. Stored
                here on construction so the info is available without
                re-reading config.
            recorder: optional event recorder the periodic loops feed
                ``periodic_run`` events into. Kept None in tests that
                only care about lifecycle / schema, so the flush path
                (Task 7+) can avoid requiring the recorder's presence.
        """
        self._config = config
        self._debug_config = debug_config
        self._worker_id = worker_id
        self._cluster_name = cluster_name
        self._recorder = recorder
        # Writer connection. Opened in ``start()``, closed in ``stop()``.
        # Nil when the engine is disabled (config.enabled=False or no
        # db_dir resolution) or has not been started yet.
        self._writer_db: aiosqlite.Connection | None = None
        # Reader connection. Second aiosqlite connection to the same DB
        # file, opened in read-only mode. Used exclusively by ``Cache.get``
        # for DB fallback. aiosqlite spawns one worker thread per
        # connection, so SELECTs run off the event loop and never queue
        # behind a flush/cleanup/sync commit on the writer. WAL mode lets
        # the reader see a consistent snapshot while the writer commits.
        self._reader_db: aiosqlite.Connection | None = None
        # Resolved DB file path — stored so ``stop()`` can remove the
        # symlink without re-resolving the dir.
        self._db_path: str = ''
        # The handler-facing Cache that feeds this engine's flush/sync
        # pipelines. Wired in via ``attach_cache`` — the app does this
        # at startup before scheduling the periodic loops. We keep the
        # relation one-to-one because a single worker has a single
        # cache-DB file, and splitting caches across engines would make
        # the ``_dirty`` swap (see ``_flush_once``) ambiguous.
        self._cache: Cache | None = None
        # Task reference for the periodic flush loop. Set in ``start()``
        # after ``run_periodic_task`` is scheduled; cleared in ``stop()``
        # once the task has been cancelled and awaited. None when the
        # engine is effectively disabled (no writer connection) since
        # there's nothing to flush.
        self._flush_task: asyncio.Task | None = None
        # Task reference for the periodic cleanup loop — same lifecycle as
        # the flush task. Runs on its own (typically slower) cadence and
        # reclaims space by deleting rows whose ``expires_at_ms`` is past.
        self._cleanup_task: asyncio.Task | None = None

    def attach_cache(self, cache: Cache) -> None:
        """Associate the handler-facing Cache instance with this engine.

        The engine owns the SQLite side; the Cache owns the in-memory
        side. ``_flush_once`` reads from ``cache._dirty`` (atomic swap)
        and writes to ``_writer_db``. App startup wires these together
        before scheduling the flush periodic (Task 8).

        Idempotent in the sense that passing the same cache twice is
        a no-op; passing a different cache replaces the binding — tests
        sometimes re-attach a fresh cache after reconfiguring.

        If the engine is already started (reader connection open), we
        also wire the reader through so a late-attached Cache can still
        fall through to the DB on ``get()`` misses.
        """
        self._cache = cache
        if self._reader_db is not None:
            cache.attach_reader_db(self._reader_db)

    def _resolve_db_dir(self) -> str:
        """Return the directory the cache DB should live in, or ''.

        Precedence:
        1. ``cache.db_dir`` if set (operator explicitly isolated the
           cache DB from the event recorder DB).
        2. ``debug.db_dir`` otherwise (default operational setup — cache
           piggybacks on the debug directory).
        3. Empty string → engine runs in disabled mode.
        """
        return self._config.db_dir or self._debug_config.db_dir

    def _update_live_link(self) -> None:
        """Create/refresh the ``<worker>-cache.db`` symlink pointing at the DB file.

        Uses the atomic-rename pattern (write ``.tmp`` symlink, os.replace
        into place) mirrored from the recorder's ``_update_live_link`` —
        this avoids windows where a concurrent peer discovery scan sees
        a half-created link.

        Silently swallows ``OSError`` — on filesystems that don't support
        symlinks (rare on Linux, possible on exotic mounts) the missing
        link just means peers can't discover us, not a fatal error.
        """
        if not self._db_path:
            return
        db_dir = os.path.dirname(self._db_path)
        link = _cache_link_path(db_dir, self._worker_id)
        target = os.path.basename(self._db_path)
        try:
            tmp = link + '.tmp'
            # clean up stale tmp leftover from a crashed prior run
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            os.symlink(target, tmp)
            os.replace(tmp, link)
        except OSError:
            pass

    def _remove_live_link(self) -> None:
        """Remove the peer-discovery symlink on graceful shutdown.

        Mirrors the recorder's shutdown hygiene so peers don't keep trying
        to pull from a shut-down worker's cache DB. Safe to call when the
        link doesn't exist or the filesystem doesn't support symlinks.
        """
        if not self._db_path:
            return
        db_dir = os.path.dirname(self._db_path)
        link = _cache_link_path(db_dir, self._worker_id)
        try:
            if os.path.islink(link):
                os.remove(link)
        except OSError:
            pass

    async def _create_schema(self) -> None:
        """Apply the cache schema to the writer connection.

        Uses ``executescript`` so the full DDL block — table + both
        indexes — lands in one round trip. ``CREATE TABLE IF NOT EXISTS``
        makes the operation a no-op against an existing cache DB, so a
        restarted worker picks up the previous rows on the next call to
        ``get()`` (reader connection, Task 9) or flush (Task 7).
        """
        assert self._writer_db is not None, 'writer DB not open'
        await self._writer_db.executescript(SCHEMA_CACHE_ENTRIES)
        # WAL mode must be set per-connection; applied here so the writer
        # is in WAL before any writes. Subsequent reader connections
        # (Task 9) and peer-opened ephemeral connections will see WAL
        # files already on disk.
        await self._writer_db.execute('PRAGMA journal_mode = WAL')
        await self._writer_db.commit()

    async def start(self) -> None:
        """Open the writer connection and apply the schema.

        Idempotent: calling ``start()`` on an already-started engine is
        a no-op (the second call would otherwise re-run schema DDL, which
        is safe but wasteful). Returns without opening resources when the
        cache is disabled or no db_dir can be resolved — the engine stays
        in effectively-disabled mode with ``_writer_db = None``.
        """
        # already started — keep the existing connection
        if self._writer_db is not None:
            return
        # disabled by config — nothing to do
        if not self._config.enabled:
            return

        db_dir = self._resolve_db_dir()
        if not db_dir:
            # Warn-and-continue (not fail-at-startup) per the plan. The
            # handler will still get a Cache instance; it just won't
            # persist. Peer-sync is automatically disabled because it
            # needs the writer connection to apply UPSERTs.
            await logger.awarning(
                'cache_engine_disabled_no_db_dir',
                category='cache',
                worker_id=self._worker_id,
                reason='cache.db_dir empty and debug.db_dir empty',
            )
            return

        os.makedirs(db_dir, exist_ok=True)
        self._db_path = _cache_db_file_path(db_dir, self._worker_id)
        self._writer_db = await aiosqlite.connect(self._db_path)
        await self._create_schema()

        # Open the reader connection on a read-only URI. ``file:…?mode=ro``
        # tells SQLite to reject any write attempt, and aiosqlite spawns a
        # fresh worker thread for this connection — so DB-fallback SELECTs
        # from ``Cache.get`` never queue behind a flush/cleanup/sync commit
        # on the writer's thread. WAL mode (set on the writer above) lets
        # the reader see consistent snapshots while the writer commits.
        reader_uri = f'file:{self._db_path}?mode=ro'
        self._reader_db = await aiosqlite.connect(reader_uri, uri=True)
        # If a Cache has already been attached (typical — engine.attach_cache
        # happens before start()), wire the reader through now; if it gets
        # attached later, the caller is responsible for calling
        # ``attach_reader_db`` themselves.
        if self._cache is not None:
            self._cache.attach_reader_db(self._reader_db)

        self._update_live_link()

        # Schedule the periodic flush loop. It runs as a framework-system
        # task (``system=True``) so the debug UI can render a [system] badge
        # alongside user-defined ``@periodic`` handlers, and errors use the
        # "continue" policy — a bad flush cycle must not kill the worker, it
        # just logs and retries next tick. The recorder (if present) logs
        # each run to the event timeline so operators see cache health.
        self._flush_task = asyncio.create_task(
            run_periodic_task(
                name='cache.flush',
                coro_fn=self._flush_once,
                seconds=self._config.flush_interval_seconds,
                on_error='continue',
                recorder=self._recorder,
                system=True,
            ),
            name='cache.flush',
        )

        # Schedule the periodic cleanup loop. Same system/error policy as
        # flush — a bad cleanup cycle logs and retries; it never kills the
        # worker. Runs on its own (typically slower) cadence since row
        # expiration is a background reclaim job, not on the write path.
        self._cleanup_task = asyncio.create_task(
            run_periodic_task(
                name='cache.cleanup',
                coro_fn=self._cleanup_once,
                seconds=self._config.cleanup_interval_seconds,
                on_error='continue',
                recorder=self._recorder,
                system=True,
            ),
            name='cache.cleanup',
        )

        await logger.ainfo(
            'cache_engine_started',
            category='cache',
            worker_id=self._worker_id,
            db_path=self._db_path,
        )

    async def stop(self) -> None:
        """Cancel the periodic flush, drain any remaining dirty ops,
        close both DB connections, and remove the live symlink.

        Ordering inside this method is deliberate:

        1. **Cancel the flush task** so it doesn't race with the final
           drain — otherwise the periodic loop might hit the writer DB
           concurrently with our explicit ``_flush_once`` and interleave.
        2. **Await the cancelled task** — ``run_periodic_task`` re-raises
           ``CancelledError``; ``return_exceptions=True`` on a single task
           would be overkill, so we just ``except CancelledError``.
        3. **Final drain** via ``_flush_once`` — this is what prevents the
           "writes lost on shutdown" window. Any entry sitting in
           ``cache._dirty`` between the last scheduled flush and stop()
           reaches the DB here.
        4. **Close the writer connection** only after the drain commits,
           so the drain's UPSERT/DELETE sees an open connection.
        5. **Detach the reader from the Cache then close it** — future
           ``Cache.get`` calls short-circuit rather than hitting a closed
           connection.
        6. **Remove the live symlink** last — the row-level data is safe,
           and peers can stop pulling from us once the symlink is gone.

        Safe to call when ``start()`` never ran or when the engine was in
        disabled mode (no connection opened): every step short-circuits on
        None checks. Matches the recorder's stop() contract so app
        shutdown can call both unconditionally.
        """
        # Step 1 + 2: stop the periodic flush + cleanup tasks cleanly.
        # Cancel cleanup first — it typically runs on a slower cadence and
        # is side-effect-free with respect to the dirty map, so we can
        # drain ordering around flush: flush's final drain needs the writer
        # connection, cleanup does not add writes we care about preserving.
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                # Expected — run_periodic_task re-raises on cancel.
                pass
            self._flush_task = None

        # Step 3: final drain — catches any dirty ops that accumulated
        # after the last scheduled flush fired, so shutdown isn't a
        # write-loss window.
        if self._writer_db is not None:
            try:
                await self._flush_once()
            except Exception:
                # A final drain that fails is logged but must not prevent
                # stop() from proceeding — otherwise a faulty DB would
                # deadlock shutdown. The loop's continue-on-error policy
                # has already written any error we could report.
                await logger.aexception(
                    'cache_final_drain_failed',
                    category='cache',
                    worker_id=self._worker_id,
                )

        # Step 4: close writer.
        if self._writer_db is not None:
            await self._writer_db.close()
            self._writer_db = None

        # Step 5: close the reader. Detach from the Cache first so a
        # post-stop Cache.get() call returns None instead of crashing on
        # a closed connection.
        if self._reader_db is not None:
            if self._cache is not None:
                self._cache.attach_reader_db(None)
            await self._reader_db.close()
            self._reader_db = None

        # Step 6: tell peers we're gone.
        self._remove_live_link()
        await logger.ainfo(
            'cache_engine_stopped',
            category='cache',
            worker_id=self._worker_id,
        )

    # ---- flush loop ---------------------------------------------------------

    async def _flush_once(self) -> None:
        """Drain one batch of pending mutations from the Cache's dirty map
        to the local SQLite DB.

        The method is the worker body that Task 8's ``run_periodic_task``
        wraps. A single flush cycle does the following:

        1. **Atomic swap** — snapshot the current ``_dirty`` map and
           reset it to an empty dict in one tuple-assign operation. Under
           CPython's GIL this is atomic: any ``Cache.set`` /
           ``Cache.delete`` landing during the swap either wrote to the
           now-snapshotted dict (and will be flushed this cycle) or to
           the fresh empty dict (picked up next cycle). No writes lost.

        2. **Split by op type** — ``Op.SET`` entries go through
           ``LWW_UPSERT_SQL`` as an ``executemany``; ``Op.DELETE`` keys
           go through ``DELETE FROM cache_entries WHERE key = ?`` as a
           separate ``executemany``. Splitting reduces SQLite round trips
           to at most two regardless of batch size — one per op type.

        3. **Single transaction** — all ops land between two ``commit()``
           boundaries so a crash mid-flush leaves the DB in a consistent
           prior-to-this-cycle state; we never half-apply a batch.

        4. **Metric ticks** — ``drakkar_cache_flush_entries_total{op=...}``
           counter advances by the number of op intents drained, not by
           the number of rows actually modified. A SET that loses LWW to
           a newer existing row still ticks the counter (the flush did
           real work; the rejection is a legitimate outcome, not a skip).

        No-op fast paths:

        - Engine not started / disabled (``_writer_db is None``) → return
          immediately; leave ``_dirty`` untouched (next start will pick
          it up if the engine is re-enabled, which is not currently a
          supported transition but is also harmless).
        - No cache attached (``_cache is None``) → return; the engine was
          constructed but never wired to a Cache (tests of pure lifecycle).
        - Empty snapshot after swap → return before any SQL is issued.
          This keeps the idle-engine cost at O(1).
        """
        if self._writer_db is None or self._cache is None:
            return

        # Atomic swap: tuple assignment is single-opcode under CPython's
        # GIL, so any ``Cache.set`` landing after this line sees the fresh
        # empty dict and will be drained on the next flush cycle —
        # nothing lost, no locking needed for in-memory coherence.
        snapshot: dict[str, DirtyOp] = self._cache._dirty
        self._cache._dirty = {}

        if not snapshot:
            return

        # Partition ops. We keep the split simple: two lists, one for
        # UPSERTs, one for DELETEs. Order inside each list follows the
        # snapshot's dict-iteration order, which is insertion order under
        # CPython — so a SET followed by a DELETE on the same key would
        # collapse (DELETE overrides in the dirty map itself), and
        # overwrites are already LWW-resolved at the SQL level below.
        upsert_rows: list[tuple] = []
        delete_keys: list[tuple[str]] = []
        for key, op in snapshot.items():
            if op.op is Op.SET:
                entry = op.entry
                assert entry is not None, 'Op.SET must carry a CacheEntry'
                upsert_rows.append(
                    (
                        entry.key,
                        str(entry.scope),  # StrEnum → raw 'local'/'cluster'/'global'
                        entry.value,
                        entry.size_bytes,
                        entry.created_at_ms,
                        entry.updated_at_ms,
                        entry.expires_at_ms,
                        entry.origin_worker_id,
                    )
                )
            else:  # Op.DELETE
                delete_keys.append((key,))

        # Execute under a single transaction. aiosqlite's default isolation
        # starts an implicit transaction on the first DML statement, and
        # ``commit()`` closes it — so the two ``executemany`` calls land
        # together.
        if upsert_rows:
            await self._writer_db.executemany(LWW_UPSERT_SQL, upsert_rows)
        if delete_keys:
            await self._writer_db.executemany(
                'DELETE FROM cache_entries WHERE key = ?',
                delete_keys,
            )
        await self._writer_db.commit()

        # Metric ticks — one label per op type. Incremented even for
        # LWW-rejected UPSERTs (see docstring point 4): the flush did
        # real work.
        if upsert_rows:
            metrics.cache_flush_entries.labels(op='set').inc(len(upsert_rows))
        if delete_keys:
            metrics.cache_flush_entries.labels(op='delete').inc(len(delete_keys))

    # ---- cleanup loop -------------------------------------------------------

    async def _cleanup_once(self) -> None:
        """Reclaim space by removing expired entries from the DB, memory,
        and the dirty map — and refresh DB-size gauges.

        Expiration is wall-clock relative: a row's ``expires_at_ms`` is
        compared to the current wall-clock milliseconds. Rows with
        ``expires_at_ms IS NULL`` (no TTL) are never cleaned up; rows with
        a future TTL are preserved; rows with a past TTL are removed.

        Four jobs per cycle:

        1. **DB purge.** ``DELETE FROM cache_entries WHERE expires_at_ms IS
           NOT NULL AND expires_at_ms < ?`` — one round trip, one commit.
           The partial index ``idx_cache_expires`` (only indexes non-null
           ``expires_at_ms``) makes this a focused scan rather than a full
           table sweep.

        2. **Memory sweep.** Walk ``Cache._memory`` for entries whose
           ``expires_at_ms`` is past, and pop them. Without this, expired
           keys would linger in memory until ``peek``/``get``/``__contains__``
           opportunistically evicts them — cleanup gives us a deterministic
           upper bound on staleness.

        3. **Dirty-map pruning** for expired keys with a pending ``Op.SET``.
           Leaving a pending SET would resurrect a row we just deleted on
           the next flush cycle. We explicitly drop ``Op.SET`` entries for
           expired keys — but leave ``Op.DELETE`` entries alone, since a
           pending DELETE on an expired key is still semantically correct
           (the row is going away either way) and simpler to leave through.

        4. **DB-gauge refresh.** ``drakkar_cache_entries_in_db`` and
           ``drakkar_cache_bytes_in_db`` are updated from a single SELECT.
           Counting DB rows on every ``set``/``get`` would defeat the
           running-sum design used for the in-memory gauges; refreshing at
           cleanup cadence (default 60s) gives operators an approximate but
           bounded-staleness DB view.

        No-op fast paths:

        - Engine not started / disabled (``_writer_db is None``) → return
          immediately without touching anything.
        - No cache attached (``_cache is None``) → DB purge still runs, but
          the memory/dirty-map steps short-circuit. This supports
          lifecycle tests that only want to exercise DB behavior.
        """
        if self._writer_db is None:
            return

        now_ms = _now_ms()

        # Step 1: DB purge. Count the rows first via ``rowcount`` on the
        # cursor so we can increment the ``cleanup_removed`` counter by
        # the real number of rows affected.
        cursor = await self._writer_db.execute(
            'DELETE FROM cache_entries WHERE expires_at_ms IS NOT NULL AND expires_at_ms < ?',
            (now_ms,),
        )
        removed = cursor.rowcount if cursor.rowcount is not None else 0
        await cursor.close()
        await self._writer_db.commit()

        # Step 2 + 3: memory sweep + dirty-map pruning for expired SETs.
        # If an expired entry had a pending set, drop it — otherwise the
        # next flush would revive a row we just deleted.
        if self._cache is not None:
            # Snapshot keys first to avoid "dict changed during iteration".
            expired_keys: list[str] = []
            for key, entry in self._cache._memory.items():  # type: ignore[reportPrivateUsage]
                if entry.expires_at_ms is not None and entry.expires_at_ms < now_ms:
                    expired_keys.append(key)
            for key in expired_keys:
                self._cache._memory.pop(key, None)  # type: ignore[reportPrivateUsage]
                # Drop only pending SET ops — DELETEs are still the correct
                # action (row is going away either way) and simpler to leave.
                pending = self._cache._dirty.get(key)  # type: ignore[reportPrivateUsage]
                if pending is not None and pending.op is Op.SET:
                    self._cache._dirty.pop(key, None)  # type: ignore[reportPrivateUsage]

        # Step 4: refresh DB-size gauges with a single aggregate query. We
        # use ``coalesce(sum(size_bytes), 0)`` so an empty table yields 0
        # rather than NULL.
        cursor = await self._writer_db.execute('SELECT count(*), coalesce(sum(size_bytes), 0) FROM cache_entries')
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            db_count, db_bytes = row
            metrics.cache_entries_in_db.set(db_count)
            metrics.cache_bytes_in_db.set(db_bytes)

        if removed > 0:
            metrics.cache_cleanup_removed.inc(removed)
