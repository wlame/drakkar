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

# Imported at module scope for the same monkeypatch reason — tests replace
# this helper with a fake that yields canned ``(worker_name, target_path)``
# tuples so peer sync can be exercised without a real filesystem.
from drakkar.peer_discovery import discover_peer_dbs

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
        # Running sum for the ``drakkar_cache_bytes_in_memory`` gauge.
        # Adjusted on mutation — Prometheus scrape reads a single int,
        # never walks the dict. Without this, a scrape on a large cache
        # would turn observability into an O(N) cost.
        self._bytes_sum: int = 0

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

        # Running-sum bookkeeping for ``bytes_in_memory``. If this is an
        # overwrite, first subtract the old entry's size (we're about to
        # replace it). Then add the new size. Keeping the sum in sync per
        # mutation is cheaper than walking the dict on scrape.
        if existing is not None:
            self._bytes_sum -= existing.size_bytes
        self._bytes_sum += size_bytes

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

        # Write-path metrics. ``writes{scope}`` ticks once per user intent
        # — we count the call, not row transitions (matches the "throughput
        # not state" semantics used by flush/sync counters). Memory gauges
        # reflect the post-set state; ``_maybe_evict`` may tick them back
        # down if the cap is exceeded.
        metrics.cache_writes.labels(scope=str(scope)).inc()
        self._refresh_memory_gauges()

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

        Note on metrics: ``peek`` is a pure memory probe with zero DB
        fallback, so we deliberately do NOT increment the hit/miss
        counters here. Hit/miss semantics are scoped to ``get()`` — where
        the memory-vs-db distinction matters. Counting peek as a hit
        would double-count if a caller does peek-then-get, and counting
        it as a miss would overcount memory pressure. Keep peek out of
        the cache-hit-rate math.
        """
        entry = self._memory.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            # Opportunistic eviction. The dirty map is untouched: if there
            # was a pending SET, letting it flush and then be cleaned up
            # by the expiration cleanup cycle is cheaper than trying to
            # untangle it here. A pending DELETE stays as-is. The pop + bytes
            # accounting goes through the shared helper so the invariant
            # "running sum tracks the dict" holds without duplicating the
            # bookkeeping in five places.
            self._pop_memory_entry(key)
            self._refresh_memory_gauges()
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
                # Mirror peek()'s opportunistic eviction on stale reads —
                # same shared helpers keep the bookkeeping coherent.
                self._pop_memory_entry(key)
                self._refresh_memory_gauges()
                # Do NOT consult the DB — if the memory entry is expired,
                # the DB row (if any) will also have expires_at_ms <= now.
                metrics.cache_misses.inc()
                return None
            # access → MRU bump
            self._memory.move_to_end(key)
            metrics.cache_hits.labels(source='memory').inc()
            # ``_decode`` accepts ``as_type=None`` natively — no ternary
            # needed to special-case the "no revival" branch.
            return _decode(entry.value, as_type=as_type)

        # --- DB fallback ---
        if self._reader_db is None:
            # Engine disabled or not yet started — treat as a miss.
            metrics.cache_misses.inc()
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
            metrics.cache_misses.inc()
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
        #
        # ``_bytes_sum`` accounting: if the key is already in memory (race
        # with a concurrent ``set`` between the earlier miss check and this
        # warm, or a double-warm from a future refactor), subtract the
        # existing entry's bytes before overwriting — otherwise the sum
        # leaks by the old entry's size every warm. Mirrors the pattern in
        # ``set()`` which also handles the overwrite case.
        existing = self._memory.get(db_key)
        if existing is not None:
            self._bytes_sum -= existing.size_bytes
        self._memory[db_key] = warmed
        self._memory.move_to_end(db_key)
        self._bytes_sum += warmed.size_bytes
        self._refresh_memory_gauges()
        self._maybe_evict()

        metrics.cache_hits.labels(source='db').inc()
        # ``_decode`` accepts ``as_type=None`` — no ternary needed.
        return _decode(warmed.value, as_type=as_type)

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
        # Adjust running sum + gauges for the removed memory entry (if any)
        # via the shared helpers. The dirty-map DELETE still records — the
        # DB may have a row we can't see from memory alone, and flush needs
        # to drop it.
        existing = self._pop_memory_entry(key)
        present = existing is not None
        if present:
            self._refresh_memory_gauges()
        # A DELETE op overwrites any prior SET; flush will see the final
        # state only. Entry is None since the row is going away.
        self._dirty[key] = DirtyOp(op=Op.DELETE, entry=None)
        # User intent counter — ticks once per delete call regardless of
        # whether the key was actually in memory (mirrors the "throughput
        # not state" semantics used by flush/sync).
        metrics.cache_deletes.inc()
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
    #
    # Expiration boundary convention (see TTL discussion in ``set``): an entry
    # is expired iff ``now_ms >= expires_at_ms``. The corresponding inclusive
    # cleanup uses ``expires_at_ms <= now_ms``; DB read-gating uses
    # ``expires_at_ms > now_ms`` (exclusive of now). Keeping one convention
    # across memory, cleanup, DB purge, and DB SELECT avoids the "off by one
    # millisecond at the boundary" class of bugs.

    @staticmethod
    def _is_expired(entry: CacheEntry) -> bool:
        """True iff the entry has a TTL that has already elapsed.

        Treats the boundary moment as expired: ``now_ms >= expires_at_ms``
        matches the SQL ``expires_at_ms <= now_ms`` used by cleanup and the
        ``expires_at_ms > now_ms`` used by read-gating.
        """
        if entry.expires_at_ms is None:
            return False
        return _now_ms() >= entry.expires_at_ms

    def _pop_memory_entry(self, key: str) -> CacheEntry | None:
        """Remove ``key`` from memory and deduct its bytes from the running sum.

        Returns the popped entry for callers that need the original metadata
        (e.g. size), or ``None`` when the key wasn't in memory. Does NOT
        refresh the Prometheus gauges — callers that pop multiple keys in
        one sweep should call ``_refresh_memory_gauges()`` once at the end
        to amortize the set() calls.

        This helper is the single source of truth for the "pop + bytes-sum
        deduct" pair; five paths in this class used to inline the sequence,
        diverging subtly over time (missing size tracking, missing gauge
        refresh, etc.). Funneling through one helper keeps the invariant
        that ``_bytes_sum == sum(entry.size_bytes for entry in _memory)``
        after every mutation.
        """
        entry = self._memory.pop(key, None)
        if entry is not None:
            self._bytes_sum -= entry.size_bytes
        return entry

    def _refresh_memory_gauges(self) -> None:
        """Push the current memory dict size + bytes to Prometheus gauges.

        Prometheus gauges maintain a single int value; ``set`` is O(1).
        Refreshing after a batch of ``_pop_memory_entry`` calls is cheaper
        than refreshing per-pop when many entries are removed together
        (peer-sync invalidation, expire-purge).
        """
        metrics.cache_entries_in_memory.set(len(self._memory))
        metrics.cache_bytes_in_memory.set(self._bytes_sum)

    def _invalidate_memory_keys(self, keys: list[str]) -> None:
        """Drop the listed keys from memory, adjust ``_bytes_sum`` + gauges.

        Exposed as a semi-private helper so ``CacheEngine`` doesn't have to
        reach into ``_memory`` / ``_bytes_sum`` directly from peer-sync's
        apply-step. Keeps the memory / running-sum / gauge triad coherent —
        callers get the invariant "entries_in_memory and bytes_in_memory
        reflect the current dict" for free.

        The ``_dirty`` map is left alone: peer-sync's invalidation does not
        cancel a pending local write — the next flush will pick up whatever
        SET/DELETE op the user queued, which is independent of whether the
        memory entry was just invalidated.

        Args:
            keys: keys to pop from memory. Missing keys are silently skipped.
        """
        if not keys:
            return
        popped_any = False
        for key in keys:
            if self._pop_memory_entry(key) is not None:
                popped_any = True
        if popped_any:
            self._refresh_memory_gauges()

    def swap_dirty(self) -> dict[str, DirtyOp]:
        """Atomically swap ``_dirty`` for a fresh empty dict and return the
        old contents.

        Tuple assignment is a single opcode under CPython's GIL, so any
        concurrent ``set`` / ``delete`` landing after the swap writes into
        the new empty dict and is picked up by the next flush cycle — no
        locking needed for in-memory coherence.

        Symmetric with ``restore_dirty``: callers (currently just
        ``CacheEngine._flush_once``) use this to take a snapshot, perform
        I/O, and — on failure — put the unsaved ops back via
        ``restore_dirty``. Keeping the swap + restore pair inside ``Cache``
        means the engine never has to reach past the class boundary into
        ``_dirty`` directly, preserving encapsulation.
        """
        snapshot = self._dirty
        self._dirty = {}
        return snapshot

    def restore_dirty(self, snapshot: dict[str, DirtyOp]) -> None:
        """Merge an un-flushed snapshot back into ``_dirty`` after a failure.

        Iterates the snapshot in insertion order; only restores a key when
        the live ``_dirty`` has no newer op for that key — the newer op
        represents a racing ``set`` / ``delete`` landing post-swap that
        supersedes the snapshot's view. The snapshot's insertion order is
        preserved for fully-restored keys; racing keys keep their current
        position in ``_dirty``.

        Paired with ``swap_dirty`` — used by ``_flush_once`` to maintain
        the "nothing lost" invariant under cancellation or commit failure.
        """
        for key, op in snapshot.items():
            if key not in self._dirty:
                self._dirty[key] = op

    def _expire_purge(self, now_ms: int) -> list[str]:
        """Remove expired entries from memory and drop any pending SET dirty ops.

        Used by ``CacheEngine._cleanup_once`` as the memory-side counterpart
        to the SQL ``DELETE FROM cache_entries WHERE expires_at_ms <= ?``.
        Combines three things the cleanup loop needs in one atomic sweep:

        1. Pop expired entries from ``_memory`` (tracks ``_bytes_sum``).
        2. Drop pending ``Op.SET`` entries in ``_dirty`` for those keys —
           otherwise the next flush would revive a row we just deleted.
        3. Preserve pending ``Op.DELETE`` entries — a user-scheduled delete
           is still the correct action for the row.

        After the sweep, refreshes ``entries_in_memory`` / ``bytes_in_memory``
        gauges once (cheaper than per-evict updates when many entries expire
        together).

        Args:
            now_ms: wall-clock milliseconds used for the TTL comparison.

        Returns:
            The list of expired keys that were removed. The caller (cleanup
            loop) does not use this today, but returning it keeps the method
            testable in isolation and matches the helper's "expire and
            report" contract.
        """
        expired_keys: list[str] = []
        for key, entry in self._memory.items():
            if entry.expires_at_ms is not None and entry.expires_at_ms <= now_ms:
                expired_keys.append(key)
        for key in expired_keys:
            self._pop_memory_entry(key)
            # Drop pending SET ops only. DELETE ops on an expired key are
            # still correct — the DB row may linger until the next DB purge
            # and the pending DELETE is the mechanism that removes it
            # (same path as a user-scheduled delete of any key).
            pending = self._dirty.get(key)
            if pending is not None and pending.op is Op.SET:
                self._dirty.pop(key, None)
        if expired_keys:
            self._refresh_memory_gauges()
        return expired_keys

    def _maybe_evict(self) -> None:
        """Drop the LRU key if the memory dict exceeds ``max_memory_entries``.

        Called after every ``set`` (and after warm-on-read in ``get()``).
        Evicts at most one entry per call in the common case — since
        ``set``/``get`` add at most one entry, a single evict per call
        matches the growth rate. The evicted key's dirty-op is preserved:
        the DB is the source of truth, and losing a pending SET before
        flush would silently drop the user's write.

        Note on ``popitem(last=False)``: OrderedDict's popitem pops the
        first inserted (i.e. least-recently-touched after our move_to_end
        calls), which is exactly LRU.

        Gauge bookkeeping: each popped entry's ``size_bytes`` is deducted
        from the running sum so ``bytes_in_memory`` stays accurate.
        """
        cap = self._max_memory_entries
        if cap is None:
            return
        evicted_any = False
        while len(self._memory) > cap:
            # pop oldest by access order (move_to_end in set/peek keeps
            # recently-touched keys at the tail). We use popitem directly
            # here rather than _pop_memory_entry because we need the pair
            # (key, entry) atomically — popitem returns both in one call.
            _, evicted_entry = self._memory.popitem(last=False)
            self._bytes_sum -= evicted_entry.size_bytes
            metrics.cache_evictions.inc()
            evicted_any = True
        if evicted_any:
            self._refresh_memory_gauges()


# ---- no-op cache stub -------------------------------------------------------


class NoOpCache:
    """Cache-shaped stub used when ``cache.enabled=false``.

    The stub lets handler code call ``self.cache.set(...)`` / ``self.cache.get(...)``
    unconditionally — no ``if self.cache is not None`` guards in user code.
    Every method mirrors the real ``Cache`` signature but does nothing:

    - ``peek`` / ``get`` always return ``None``
    - ``set`` / ``delete`` silently discard
    - ``__contains__`` always returns ``False``

    ``get`` remains a coroutine (not a plain function) for API parity with
    the real ``Cache.get`` — a user who ``await``s the result on either flavour
    sees identical behaviour aside from the value. If it weren't async, a
    caller switching from disabled to enabled would have to add ``await``.

    The stub does not touch any metrics or counters — a disabled cache is
    invisible in observability, which matches the "pay for what you use"
    contract.
    """

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = CacheScope.LOCAL,
    ) -> None:
        """No-op: the real Cache would store the value; we discard it."""
        # Deliberate no-op. Signature mirrors Cache.set so the handler API is
        # identical across enabled/disabled states.
        return None

    def peek(self, key: str) -> Any | None:
        """No-op: always reports "not present"."""
        return None

    def delete(self, key: str) -> bool:
        """No-op: always reports "nothing to delete"."""
        return False

    def __contains__(self, key: str) -> bool:
        """No-op: always reports "not a member"."""
        return False

    async def get[T: BaseModel](self, key: str, *, as_type: type[T] | None = None) -> Any | None:
        """No-op: always reports a miss.

        Kept async to mirror ``Cache.get`` — switching a handler between
        enabled and disabled cache must not require code changes at call sites.
        """
        return None


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


# ---- engine lifecycle -------------------------------------------------------


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
        # Task reference for the periodic peer-sync loop. Same lifecycle
        # as flush/cleanup; only scheduled when peer sync is effectively
        # enabled (both config flag on AND debug.store_config=True). None
        # when disabled so there's no wakeup cost.
        self._sync_task: asyncio.Task | None = None
        # Whether peer-sync is *effectively* enabled — i.e. both the config
        # flag is on and the startup gating rules passed. Set during
        # ``start()``: peer-sync needs ``debug.store_config`` (to read peer
        # cluster_names from their ``worker_config`` tables) and a writable
        # local DB, so startup may silently downgrade to disabled even if
        # ``config.peer_sync.enabled=True``. ``_sync_once`` short-circuits
        # on this flag to keep the idle-engine cost at O(1).
        self._peer_sync_enabled: bool = False
        # In-process cache of peer ``cluster_name`` values keyed by peer
        # worker_name. Populated lazily from each peer's ``-live.db``
        # ``worker_config`` row and refreshed when the entry's age exceeds
        # ``PEER_CLUSTER_CACHE_TTL_SECONDS``. A cache miss is cheap (one
        # small SELECT), but on a large fleet the N^2 read pattern adds up
        # without this short-lived cache.
        self._peer_cluster_cache: dict[str, tuple[str, float]] = {}
        # Per-peer sync cursors. Maps peer worker_name → max ``updated_at_ms``
        # pulled so far. The next cycle's pull uses ``WHERE updated_at_ms > ?``
        # with this value so we never re-read rows we've already processed.
        #
        # Advancement rules (see ``_advance_cursor`` / ``_sync_once``):
        #  - zero rows     → cursor unchanged (no progress, retry next cycle)
        #  - partial batch → cursor jumps to max(last_row_ts, cursor_ms) to
        #                    stay anchored on the peer's clock (clock-skew
        #                    safe) and never regress on a stale insert
        #  - full batch    → cursor advances to last row's ``updated_at_ms``
        #                    so the next cycle picks up the tail
        #
        # Same-ms edge case: if a peer has more rows sharing ``rows[-1]``'s
        # ``updated_at_ms`` than fit in the ``LIMIT`` (either a pure
        # burst or a mixed-ms batch with spillover at the last ms), the
        # strict ``> cursor_ms`` filter would skip the tail (SQLite's
        # deterministic ordering would keep returning the same
        # LIMIT-bounded prefix even if we stepped the cursor back).
        # ``_sync_once`` runs a follow-up "drain" pull after ANY full
        # batch, keyed on ``updated_at_ms = last_ms AND key > last_key``,
        # that applies all remaining same-ms rows before the cursor
        # advances. See ``_drain_same_ms_tail``.
        #
        # Cursor state is in-memory only. A worker restart resets cursors to
        # zero and the next cycle re-pulls everything from peers — bounded by
        # TTL cleanup and LWW rejection, so no runaway work. Persisting
        # cursors across restarts is a follow-up (marked as such in the plan).
        self._peer_cursors: dict[str, int] = {}

    @property
    def reader_db(self) -> aiosqlite.Connection | None:
        """Public accessor for the reader aiosqlite connection.

        Exposes the read-only connection so external code (notably the
        debug server's cache endpoints) doesn't need to reach into
        ``_reader_db`` directly. Returns ``None`` when the engine is
        disabled or has not been started (no DB is present to read).

        The connection is shared with ``Cache.get``'s DB fallback — we
        don't open a third connection for the UI, since adding a
        fourth aiosqlite worker thread per worker would cost more than
        it saves on read-only queries.
        """
        return self._reader_db

    @staticmethod
    def _cache_link_path(db_dir: str, worker_id: str) -> str:
        """Return the peer-discovery symlink path: ``<db_dir>/<worker>-cache.db``.

        This is the *symlink* name, scanned by ``discover_peer_dbs(...,
        suffix='-cache.db')``. Peers find us by reading this symlink and
        following it to the underlying DB file (see ``_cache_db_file_path``).
        Scoped to ``CacheEngine`` because nothing outside the engine
        builds this path.
        """
        return str(Path(db_dir) / f'{worker_id}-cache.db')

    @staticmethod
    def _cache_db_file_path(db_dir: str, worker_id: str) -> str:
        """Return the actual DB file path: ``<db_dir>/<worker>-cache.db.actual``.

        The cache DB file lives under a ``.actual`` suffix so the stable
        ``<worker>-cache.db`` name can be reserved as a symlink target — the
        same convention peer discovery uses for recorder live links. Since
        the cache does not rotate files in v1, the ``.actual`` file is
        effectively permanent; the indirection exists purely so peer
        discovery's ``os.path.islink`` filter works identically for
        recorder and cache.
        """
        return str(Path(db_dir) / f'{worker_id}-cache.db.actual')

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
        link = self._cache_link_path(db_dir, self._worker_id)
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
        link = self._cache_link_path(db_dir, self._worker_id)
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
        self._db_path = self._cache_db_file_path(db_dir, self._worker_id)
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

        # Gate peer-sync on both config flags. ``peer_sync.enabled=False``
        # (the explicit off-switch) always wins. Otherwise peer-sync also
        # needs ``debug.store_config=True`` — we pull each peer's
        # ``cluster_name`` from their ``worker_config`` table, which only
        # exists when the peer's recorder has ``store_config=True``.
        # Logging once at startup (per the plan) keeps operators aware of
        # the effective state without spamming the sync loop.
        #
        # We use two log levels intentionally so operators can tell the
        # cases apart at a glance:
        #   - explicit off-switch (config says disabled) → INFO ("I turned
        #     it off"), no action needed
        #   - silent downgrade (peer_sync asked for, but store_config off)
        #     → WARNING ("you wanted this but it got disabled"), operator
        #     should decide whether to flip store_config or accept the
        #     degraded mode
        if not self._config.peer_sync.enabled:
            self._peer_sync_enabled = False
            await logger.ainfo(
                'cache_peer_sync_disabled_by_config',
                category='cache',
                worker_id=self._worker_id,
                reason='peer_sync.enabled=False in config',
            )
        elif not self._debug_config.store_config:
            self._peer_sync_enabled = False
            await logger.awarning(
                'cache_peer_sync_disabled_no_store_config',
                category='cache',
                worker_id=self._worker_id,
                reason='peer_sync needs debug.store_config=True to read peer cluster_names',
            )
        else:
            self._peer_sync_enabled = True

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

        # Schedule the periodic peer-sync loop — but only when peer sync
        # is effectively enabled. When disabled we skip registration
        # entirely so there's zero wakeup cost on workers that don't
        # participate in cross-worker cache sharing (including workers
        # where ``debug.store_config=False`` silently downgraded sync).
        if self._peer_sync_enabled:
            self._sync_task = asyncio.create_task(
                run_periodic_task(
                    name='cache.sync',
                    coro_fn=self._sync_once,
                    seconds=self._config.peer_sync.interval_seconds,
                    on_error='continue',
                    recorder=self._recorder,
                    system=True,
                ),
                name='cache.sync',
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
        # Step 1 + 2: stop the periodic sync + cleanup + flush tasks cleanly.
        # Cancel sync and cleanup first — they're side-effect-free with
        # respect to the dirty map. Flush's final drain below needs the
        # writer connection open, so we save flush for last.
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
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

        # Atomic swap via the Cache's own helper. Keeps the "reach into
        # ``_dirty`` directly" pattern encapsulated on ``Cache`` — the
        # engine doesn't own that mutation detail.
        snapshot = self._cache.swap_dirty()

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
                if entry is None:
                    # Structural invariant: ``Op.SET`` DirtyOps are constructed
                    # with the freshly-encoded ``CacheEntry`` alongside — an
                    # ``entry is None`` here would indicate a construction bug
                    # upstream, not user input. We convert what used to be an
                    # ``assert`` (strippable under ``python -O``) to a hard
                    # runtime check so production builds still fail loudly.
                    raise RuntimeError(f"Op.SET DirtyOp for key '{key}' missing CacheEntry payload")
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
        #
        # Cancel-safety: if this coroutine is cancelled (or raises) between
        # the atomic swap above and a successful ``commit()``, the snapshot
        # local would otherwise unwind with the frame and its ops would be
        # lost. The stop() path's final-drain ``_flush_once()`` call would
        # see an already-empty ``_dirty`` and persist nothing.
        #
        # To keep the "nothing lost" guarantee under cancellation, we track
        # whether the commit completed. On any failure path (including
        # CancelledError) we merge the un-committed snapshot back into
        # ``_cache._dirty`` so the next flush — scheduled or the shutdown
        # drain — picks them up. When merging, any key that has a newer op
        # in the current ``_dirty`` (a racing ``set`` landed post-swap) wins
        # over the snapshot entry; otherwise we restore the snapshot op.
        committed = False
        try:
            if upsert_rows:
                await self._writer_db.executemany(LWW_UPSERT_SQL, upsert_rows)
            if delete_keys:
                await self._writer_db.executemany(
                    'DELETE FROM cache_entries WHERE key = ?',
                    delete_keys,
                )
            await self._writer_db.commit()
            committed = True
        finally:
            if not committed:
                # Restore un-committed ops through Cache's helper — the
                # merge logic (skip keys with newer live ops) lives there
                # too so the engine doesn't have to know about race
                # semantics on ``_dirty``.
                self._cache.restore_dirty(snapshot)

        # Metric ticks — one label per op type. Incremented even for
        # LWW-rejected UPSERTs (see docstring point 4): the flush did
        # real work. Only ticked on a successful commit — a rolled-back
        # flush did not "do work" from the DB's perspective.
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
           NOT NULL AND expires_at_ms <= ?`` — one round trip, one commit.
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
        # the real number of rows affected. Uses ``<= ?`` (inclusive of the
        # boundary ms) for consistency with ``Cache._is_expired`` and
        # ``_expire_purge`` — an entry whose ``expires_at_ms`` equals
        # ``now_ms`` is expired and gets cleaned up here too.
        cursor = await self._writer_db.execute(
            'DELETE FROM cache_entries WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?',
            (now_ms,),
        )
        removed = cursor.rowcount if cursor.rowcount is not None else 0
        await cursor.close()
        await self._writer_db.commit()

        # Step 2 + 3: memory sweep + dirty-map pruning for expired SETs.
        # Delegate to ``Cache._expire_purge`` — the helper handles both
        # steps atomically and refreshes the memory gauges once after
        # the sweep. Moving this logic onto Cache keeps the memory /
        # bytes-sum / dirty-map triad encapsulated behind the Cache class
        # rather than reaching across the layer from CacheEngine.
        if self._cache is not None:
            self._cache._expire_purge(now_ms)

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

    # ---- peer sync ----------------------------------------------------------

    async def _resolve_peer_cluster(self, peer_worker_name: str, db_dir: str) -> str | None:
        """Return the peer's ``cluster_name``, or ``None`` if it cannot be read.

        Reads ``<db_dir>/<peer>-live.db``'s ``worker_config`` table. Results
        are cached in process for ``PEER_CLUSTER_CACHE_TTL_SECONDS`` keyed
        by peer worker name, so we don't re-open the recorder DB on every
        sync cycle. On a miss (missing symlink, missing table, read error)
        we return ``None`` and the caller treats the peer as "unknown cluster"
        — conservative, since we only want to pull ``scope='global'`` rows
        in that case.

        Args:
            peer_worker_name: the peer's stable identifier from the cache
                symlink basename.
            db_dir: shared directory holding the peer's recorder DBs.

        Returns:
            The peer's ``cluster_name`` string (possibly empty — some
            deployments run without a cluster label) or ``None`` if the
            information is not currently readable. The empty-string case
            is cached like any other value.
        """
        # Per-peer in-process cache with TTL. A missing entry or a stale
        # one falls through to the recorder-DB read below.
        now = time.time()
        cached = self._peer_cluster_cache.get(peer_worker_name)
        if cached is not None:
            cluster_name, stored_at = cached
            if now - stored_at < PEER_CLUSTER_CACHE_TTL_SECONDS:
                return cluster_name

        # The cache DB lives at ``<peer>-cache.db``; the recorder DB lives
        # at ``<peer>-live.db`` alongside it. We open the recorder DB
        # read-only to avoid any chance of writes to a peer's file.
        live_link = str(Path(db_dir) / f'{peer_worker_name}-live.db')
        if not os.path.exists(live_link):
            # Peer's recorder may be down or not configured with
            # ``store_config=True``. Don't populate the cache so next
            # cycle can retry.
            return None

        # Each peer DB query is wrapped in ``asyncio.wait_for`` with the
        # configured timeout so a stuck peer cannot block the sync cycle.
        # ``asyncio.TimeoutError`` propagates into the outer ``except`` below.
        timeout = self._config.peer_sync.timeout_seconds
        try:
            async with aiosqlite.connect(f'file:{live_link}?mode=ro', uri=True) as db:
                async with db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='worker_config'"
                ) as cur:
                    existing_row = await asyncio.wait_for(cur.fetchone(), timeout=timeout)
                    if not existing_row:
                        # Peer exists but didn't enable store_config. Treat
                        # as "no cluster info available" — pull only
                        # ``scope='global'`` rows.
                        await logger.awarning(
                            'cache_peer_missing_worker_config',
                            category='cache',
                            peer=peer_worker_name,
                            db=live_link,
                        )
                        return None
                async with db.execute('SELECT cluster_name FROM worker_config WHERE id = 1') as cur:
                    row = await asyncio.wait_for(cur.fetchone(), timeout=timeout)
                    # ``row`` is None when the table is empty; ``row[0]`` is
                    # None when the column was never populated. Both cases
                    # collapse to an empty string (unclustered peer) — the
                    # ``or ''`` handles both None-row (via the ternary) and
                    # None-column in a single expression.
                    cluster_name = (row[0] if row is not None else None) or ''
        except Exception:
            # Corrupt / inaccessible peer DB — log and move on. Don't
            # cache the failure; next cycle retries.
            await logger.aexception(
                'cache_peer_cluster_resolve_failed',
                category='cache',
                peer=peer_worker_name,
                db=live_link,
            )
            return None

        self._peer_cluster_cache[peer_worker_name] = (cluster_name, now)
        return cluster_name

    def _peer_scope_filter(self, peer_cluster_name: str | None) -> tuple[str, tuple]:
        """Build the ``scope`` predicate + params used when pulling from a peer.

        Rules (per plan):
        - Peer in the same cluster as us → pull ``scope IN ('cluster','global')``.
        - Peer in a different cluster → pull ``scope = 'global'`` only.
        - Peer with unknown cluster (read failure) → pull ``scope = 'global'``
          only (conservative — we won't assume a same-cluster match without
          evidence).
        - ``scope='local'`` is never pulled; LOCAL entries stay on the
          originating worker by contract.

        Returns a ``(sql_fragment, params)`` pair ready to splice into the
        larger SELECT in ``_sync_once`` via parameterized binding.
        """
        if peer_cluster_name is not None and peer_cluster_name == self._cluster_name:
            return ('scope IN (?, ?)', (CacheScope.CLUSTER.value, CacheScope.GLOBAL.value))
        return ('scope = ?', (CacheScope.GLOBAL.value,))

    async def _pull_peer_rows(
        self,
        *,
        peer_db_path: str,
        scope_sql: str,
        scope_params: tuple,
        batch_size: int,
        cursor_ms: int,
    ) -> list[tuple]:
        """Open an ephemeral read-only connection to the peer DB and pull rows.

        Each peer sync gets its own short-lived aiosqlite connection. This
        is deliberate: aiosqlite spawns a fresh OS worker thread per
        connection, so peer reads never queue behind our writer's
        flush/cleanup/sync-UPSERT commits. We use a ``file:...?mode=ro`` URI
        so even a misconfigured peer DB can never be written to through our
        connection.

        Filters out expired rows at the DB layer — no point pulling
        already-dead entries across the network boundary just to reject
        them during UPSERT (LWW would let them in if they happened to have
        a newer ``updated_at_ms``, only for cleanup to reap them soon
        after).

        Also filters by the per-peer cursor (``updated_at_ms > cursor_ms``)
        so we skip rows we've already read. Callers pass the current cursor
        from ``_peer_cursors``; on the first sync cycle the cursor is 0 and
        nothing is filtered out.

        Args:
            peer_db_path: resolved path to the peer's cache DB (``.actual``
                file, not the symlink — caller already resolved).
            scope_sql: the scope predicate, e.g. ``'scope = ?'`` or
                ``'scope IN (?, ?)'``.
            scope_params: parameters for ``scope_sql``.
            batch_size: maximum rows to fetch this cycle.
            cursor_ms: only rows with ``updated_at_ms > cursor_ms`` are
                returned; pass 0 on the first cycle.

        Returns:
            A list of row tuples in the same column order as the SELECT.
            Empty list on any error reading the peer (logged by the caller
            via the surrounding try/except — see Task 13's isolation).
        """
        now_ms = _now_ms()
        # ORDER BY includes ``key ASC`` as an explicit secondary sort. This
        # is a correctness requirement for the same-ms drain in
        # ``_sync_once``: the drain uses ``rows[-1][0]`` (the last row's
        # key) as a lex-max cursor and queries ``WHERE updated_at_ms = ms
        # AND key > last_key``. Without an explicit ``key ASC`` tiebreaker,
        # SQLite breaks ``updated_at_ms`` ties by rowid (insertion order),
        # which equals the peer's flush order — derived from arbitrary
        # business-logic ``Cache.set`` call order, not lex order. In that
        # case ``rows[-1][0]`` might NOT be the lex-max among the same-ms
        # batch, and ``key > last_key`` would silently skip keys that
        # happen to sort lexicographically below ``last_key`` but were
        # inserted after it. Explicit secondary key sort guarantees
        # ``rows[-1][0]`` IS the lex-max key among the same-ms tail, making
        # the drain's ``key > last_key`` walk correct.
        query = (
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries '
            f'WHERE {scope_sql} '
            'AND (expires_at_ms IS NULL OR expires_at_ms > ?) '
            'AND updated_at_ms > ? '
            'ORDER BY updated_at_ms ASC, key ASC '
            'LIMIT ?'
        )
        params = (*scope_params, now_ms, cursor_ms, batch_size)
        # ``asyncio.wait_for`` wraps the fetch with the configured peer-sync
        # timeout so a slow or wedged peer cannot stall the whole sync
        # cycle. Timeout raises ``asyncio.TimeoutError`` which the caller's
        # per-peer try/except catches and isolates.
        timeout = self._config.peer_sync.timeout_seconds
        async with (
            aiosqlite.connect(f'file:{peer_db_path}?mode=ro', uri=True) as db,
            db.execute(query, params) as cur,
        ):
            # aiosqlite's ``fetchall`` returns ``Iterable[Row]``; we materialize
            # to a concrete tuple-list so callers can index by column position
            # (keys come back at row[0], scope at row[1], etc.) without worrying
            # about cursor lifetime after the ``async with`` closes.
            fetched = await asyncio.wait_for(cur.fetchall(), timeout=timeout)
            return [tuple(row) for row in fetched]

    async def _drain_same_ms_tail(
        self,
        *,
        peer_db_path: str,
        scope_sql: str,
        scope_params: tuple,
        batch_size: int,
        ms: int,
        last_key: str,
    ) -> list[tuple]:
        """Drain any remaining rows at a single peer timestamp ``ms`` that
        spilled past the main pull's ``LIMIT``.

        The main ``_pull_peer_rows`` ordering is ``ORDER BY updated_at_ms
        ASC, key ASC`` and its filter is ``WHERE updated_at_ms > cursor_ms``.
        Whenever the main pull returns a FULL batch, rows sharing the
        last row's ``updated_at_ms`` may exist past the ``LIMIT`` — in
        two flavors:

        1. **All-same-ms batch**: every row in the batch shares one ``ms``
           (a burst of cache sets committed on the peer in one
           millisecond). The cursor would advance to that ``ms`` and the
           strict ``updated_at_ms > cursor_ms`` filter on the next cycle
           would permanently hide the tail.
        2. **Mixed-ms batch whose last ms has spillover**: e.g. batch
           ``[ts=1000, ts=1001, ts=1002]`` where the peer has more rows
           at ts=1002 past the limit. Same permanent-loss mechanic.

        Both flavors are handled identically by this helper: walk
        forward by ``key`` at the single ``ms``. The main pull's
        explicit ``key ASC`` tiebreaker guarantees that ``rows[-1][0]``
        (passed here as ``last_key``) is the LEX-MAX key among the
        main pull's rows that share ``ms`` in the batch. Without the
        tiebreaker, SQLite would break ``updated_at_ms`` ties by rowid
        (insertion order), which has no relation to lex key order,
        and ``key > last_key`` below would silently skip keys that sort
        lexicographically below ``last_key`` but were inserted after it
        on the peer. The follow-up query targets exactly
        ``updated_at_ms = ms AND key > last_key`` with
        ``ORDER BY key ASC LIMIT batch_size`` — a deterministic key-based
        walk that makes progress even when every row shares one
        timestamp.

        We loop until a partial batch comes back or the pull returns no
        rows — same-ms bursts are rare and bounded (a single millisecond
        can't realistically hold millions of rows), so an inner loop is
        safe. Each iteration opens its own short-lived read connection
        for the same reason as the main pull (no queueing behind the
        writer).

        Args:
            peer_db_path: same resolved path as the main pull.
            scope_sql/scope_params: same scope predicate the main pull uses.
            batch_size: page size for the drain.
            ms: the shared ``updated_at_ms`` value we're draining.
            last_key: the ``key`` of the last row already pulled at ``ms``
                — the drain starts strictly after it.

        Returns:
            All additional same-ms rows beyond ``last_key`` (empty if the
            main pull already saw everything at this timestamp — common
            on mixed-ms batches that happen to have no spillover tail).
        """
        now_ms = _now_ms()
        # Query shape mirrors ``_pull_peer_rows`` for the SELECT list (so
        # the tuple order is identical and the caller can hand the rows
        # straight to ``_apply_peer_rows``). The WHERE is tighter: we're
        # only interested in one millisecond, and we walk forward by key
        # for a deterministic same-ms tiebreaker. ``ORDER BY key ASC``
        # guarantees ``key > cursor_key`` makes progress.
        query = (
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries '
            f'WHERE {scope_sql} '
            'AND (expires_at_ms IS NULL OR expires_at_ms > ?) '
            'AND updated_at_ms = ? '
            'AND key > ? '
            'ORDER BY key ASC '
            'LIMIT ?'
        )
        drained: list[tuple] = []
        cursor_key = last_key
        # One connection for the whole drain — each iteration used to open
        # and close its own ``aiosqlite.connect`` with the aiosqlite-spawned
        # worker-thread overhead that implies. Hoisting the connection keeps
        # the drain on a single worker thread end-to-end; the ``key > ?``
        # cursor still guarantees forward progress and distinct pages per
        # iteration.
        timeout = self._config.peer_sync.timeout_seconds
        async with aiosqlite.connect(f'file:{peer_db_path}?mode=ro', uri=True) as db:
            while True:
                params = (*scope_params, now_ms, ms, cursor_key, batch_size)
                async with db.execute(query, params) as cur:
                    # ``wait_for`` applies the configured peer-sync timeout to
                    # each page fetch. A peer whose DB is stuck (corrupt WAL,
                    # slow I/O) raises ``asyncio.TimeoutError`` rather than
                    # stalling the whole sync cycle; the per-peer try/except
                    # in ``_sync_once`` catches and isolates the failure.
                    page = await asyncio.wait_for(
                        cur.fetchall(),
                        timeout=timeout,
                    )
                page_rows = [tuple(row) for row in page]
                if not page_rows:
                    break
                drained.extend(page_rows)
                # Last page — no need for another round trip.
                if len(page_rows) < batch_size:
                    break
                # Advance the key cursor and loop; the ``key > ?`` filter
                # guarantees forward progress on the next iteration.
                cursor_key = str(page_rows[-1][0])
        return drained

    def _advance_cursor(
        self,
        *,
        rows_count: int,
        batch_size: int,
        rows: list[tuple],
        cursor_ms: int,
    ) -> int | None:
        """Compute the new cursor value for a peer after a pull, or ``None``
        if it should stay where it is.

        Rules (see ``_peer_cursors`` docstring in ``__init__``):

        * Zero rows returned → return ``None`` (caller leaves cursor
          unchanged). This prevents a single empty cycle from skipping
          over late-arriving rows whose ``updated_at_ms`` is between
          the old cursor and "now".
        * Full batch (``rows_count == batch_size``) → return the last
          row's ``updated_at_ms``. The peer has more we haven't read;
          next cycle picks up from here.

          **Same-millisecond edge case.** A full batch may hit a peer
          timestamp shared by MORE rows than fit in the LIMIT — either
          a pure burst (every row in the batch at one ``ms``) or a
          mixed-ms batch whose trailing ``ms`` has spillover past the
          ``LIMIT``. In both cases the extra rows at ``rows[-1].ms``
          would be permanently skipped because advancing the cursor to
          ``last_ts`` combined with the strict ``WHERE updated_at_ms >
          cursor_ms`` excludes them, and SQLite's deterministic ordering
          would keep returning the same prefix on any retry at an
          earlier cursor. The drain for this case happens in
          ``_sync_once`` whenever the main pull returns a full batch,
          via a separate follow-up query keyed on
          ``(updated_at_ms = rows[-1].ms AND key > rows[-1].key)``;
          see ``_drain_same_ms_tail``. This helper doesn't need to
          special-case it — the follow-up drain is an additive apply
          before the cursor settles here.

        * Partial batch (``0 < rows_count < batch_size``) → return
          ``max(rows[-1].updated_at_ms, cursor_ms)``. We've caught up on
          everything the peer had below the max observed timestamp.
          Anchoring to the observed peer timestamp (rather than our
          local wall clock) is the critical property here: peer rows
          are written with the peer's clock, which can be skewed
          relative to ours by seconds or minutes on NTP-uncorrected
          hosts. If we advanced to ``_now_ms()`` and the peer's clock
          was ahead of ours, rows with ``updated_at_ms`` between our
          previous cursor and our local now would be silently skipped
          on subsequent pulls (the ``WHERE updated_at_ms > cursor`` clause
          would exclude them). The ``max(..., cursor_ms)`` guard keeps
          the cursor monotonic — partial batches never regress — without
          relying on our local clock.
        """
        if rows_count == 0:
            return None
        if rows_count >= batch_size:
            # Rows are ordered by updated_at_ms ASC, so row[-1] has the
            # largest timestamp. Column index 5 is updated_at_ms (see the
            # SELECT list in ``_pull_peer_rows``).
            return int(rows[-1][5])
        # Partial batch: track the peer's observed timestamps, not our
        # wall clock. ``max(..., cursor_ms)`` enforces monotonicity.
        return max(int(rows[-1][5]), cursor_ms)

    async def _sync_once(self) -> dict[str, list[tuple]]:
        """Pull one batch of entries from every discovered peer and apply
        them to the local DB via LWW UPSERT.

        Task 11 scope: discovery + per-peer pull with scope-aware filter.
        Task 12: apply the pulled rows to the local DB via the shared
        ``LWW_UPSERT_SQL`` constant, invalidate any cached in-memory
        entries for the upserted keys, and tick the
        ``sync_entries_fetched`` / ``sync_entries_upserted`` counters.
        Task 13 (this change): cursor-based incremental pulls and
        per-peer error isolation.

        Apply step details:

        - UPSERTs go through the **same** ``LWW_UPSERT_SQL`` used by the
          local flush path (Task 7). Single source of truth for conflict
          resolution — a local write and a peer-supplied write cannot
          diverge in their LWW rules.
        - Memory is invalidated unconditionally for every key we attempted
          to UPSERT. We don't know per-row whether LWW accepted, so the
          simpler invariant is: drop from memory, let the next ``get``
          fall through to the DB. Cheaper than a SELECT-back-and-compare.
        - The ``sync_entries_upserted{peer}`` counter advances by the number
          of UPSERT *attempts*, not the rows actually modified. A peer row
          whose LWW lost to a newer local row still ticks the counter —
          matching the "count throughput, not applied rows" semantics of
          ``cache_flush_entries``. That way operators see sync cost even
          on idle clusters where LWW rejects most rows.

        Per-peer error isolation:

        One bad peer (corrupt DB, missing file, read timeout) must not
        break the whole cycle. Each peer's pull + apply is wrapped in a
        try/except; on failure we log a warning with the peer name and
        error, increment ``drakkar_cache_sync_errors_total{peer}``, and
        continue to the next peer. The failing peer's cursor is left
        untouched so the next cycle retries the same range.

        Fast paths:
        - Engine not started / disabled (``_writer_db is None``) → return
          an empty dict.
        - Peer sync disabled (``_peer_sync_enabled=False``) → return an
          empty dict with zero filesystem reads. This makes the
          ``peer_sync.enabled=False`` path essentially free at runtime.

        Returns:
            ``{peer_name: list_of_row_tuples}`` — one entry per peer we
            attempted to sync from this cycle, mapping to the rows actually
            pulled (main pull + same-ms drain combined where applicable).
            Empty dict when the engine is disabled or peer sync is gated
            off. The periodic-task wrapper discards the return value
            (``run_periodic_task`` doesn't inspect the callable's result);
            unit tests capture it directly from ``await engine._sync_once()``
            to assert per-peer scope filtering + pagination without mocking
            aiosqlite internals.
        """
        pulled: dict[str, list[tuple]] = {}

        if self._writer_db is None or not self._peer_sync_enabled:
            return pulled

        # Resolve the dir once per cycle — both peer-discovery and cluster-
        # name resolution (below) use it, so avoid recomputing.
        db_dir = self._resolve_db_dir()
        if not db_dir:
            return pulled

        batch_size = self._config.peer_sync.batch_size
        # Iterate peers. One bad peer must not break the whole sync cycle —
        # isolation is part of the peer-sync contract. Any exception inside
        # the per-peer block: log, tick the error counter, leave the
        # cursor untouched, continue to the next peer.
        async for peer_name, peer_db_path in discover_peer_dbs(
            db_dir,
            '-cache.db',
            self._worker_id,
        ):
            # Cursor: 0 on first encounter, otherwise the last successful
            # advance. A failing peer keeps its previous cursor so next
            # cycle retries the same range.
            cursor_ms = self._peer_cursors.get(peer_name, 0)
            try:
                peer_cluster = await self._resolve_peer_cluster(peer_name, db_dir)
                scope_sql, scope_params = self._peer_scope_filter(peer_cluster)
                rows = await self._pull_peer_rows(
                    peer_db_path=peer_db_path,
                    scope_sql=scope_sql,
                    scope_params=scope_params,
                    batch_size=batch_size,
                    cursor_ms=cursor_ms,
                )
                pulled[peer_name] = rows
                # Apply step — UPSERT pulled rows into our local DB.
                # _apply_peer_rows handles the zero-row fast path internally
                # so we don't branch here.
                await self._apply_peer_rows(peer_name=peer_name, rows=rows)
                # Same-ms tail drain after ANY full batch. If the peer
                # returned a full batch, there may be more rows sharing
                # the LAST row's ``updated_at_ms`` past our LIMIT — even
                # when earlier rows in the batch had smaller timestamps.
                # Example with batch_size=3 and peer rows
                # [ts=1000, ts=1001, ts=1002, ts=1002, ts=1002]: the main
                # pull returns the first three, the cursor advances to
                # 1002, and the strictly-greater ``WHERE updated_at_ms >
                # 1002`` clause on the next cycle would permanently skip
                # the two remaining ts=1002 rows.
                #
                # Trigger is simply "full batch" — the old narrower check
                # (``rows[0] ts == rows[-1] ts``) missed mixed-ms batches
                # whose trailing ms had spillover tail rows. Given the
                # main pull's ``ORDER BY updated_at_ms ASC, key ASC``
                # (see ``_pull_peer_rows``), ``rows[-1]`` is guaranteed
                # to be the lex-max key among rows sharing its
                # ``updated_at_ms`` in the batch, so the drain's
                # ``WHERE updated_at_ms = last_ms AND key > last_key``
                # walk pages through only the unseen tail. The drain may
                # return empty if no more same-ms rows exist on the
                # peer — that's the common case for mixed batches and
                # it's cheap (one small indexed query).
                if len(rows) >= batch_size:
                    last_ms = int(rows[-1][5])
                    last_key = str(rows[-1][0])
                    drained = await self._drain_same_ms_tail(
                        peer_db_path=peer_db_path,
                        scope_sql=scope_sql,
                        scope_params=scope_params,
                        batch_size=batch_size,
                        ms=last_ms,
                        last_key=last_key,
                    )
                    if drained:
                        await self._apply_peer_rows(peer_name=peer_name, rows=drained)
                        # Include the drained rows in the returned dict
                        # alongside the main pull so callers (tests) see
                        # the full per-peer sync result of this cycle.
                        pulled[peer_name] = rows + drained
                # Advance cursor on success. Zero rows → no change; rules
                # live in ``_advance_cursor``. ``cursor_ms`` is passed so
                # partial-batch advancement can stay monotonic without
                # relying on our local wall clock (peer clocks may drift).
                next_cursor = self._advance_cursor(
                    rows_count=len(rows),
                    batch_size=batch_size,
                    rows=rows,
                    cursor_ms=cursor_ms,
                )
                if next_cursor is not None:
                    self._peer_cursors[peer_name] = next_cursor
            except Exception as exc:
                # Per-peer isolation: log warning, tick error metric,
                # leave cursor untouched. The sync loop itself continues
                # to the next peer — one bad peer cannot break the whole
                # cycle, and a failing peer can recover on next cycle.
                metrics.cache_sync_errors.labels(peer=peer_name).inc()
                await logger.awarning(
                    'cache_peer_sync_failed',
                    category='cache',
                    peer=peer_name,
                    db=peer_db_path,
                    error=str(exc),
                )

        return pulled

    async def _apply_peer_rows(self, *, peer_name: str, rows: list[tuple]) -> None:
        """UPSERT a batch of peer-supplied rows into the local DB via the
        shared LWW SQL, then invalidate memory for the affected keys.

        The incoming tuple order mirrors the SELECT in ``_pull_peer_rows``:
        ``(key, scope, value, size_bytes, created_at_ms, updated_at_ms,
        expires_at_ms, origin_worker_id)`` — which happens to be the exact
        binding order ``LWW_UPSERT_SQL`` expects, so no translation is
        needed.

        Metric ticks:
        - ``cache_sync_entries_fetched{peer}`` += len(rows) — every row that
          crossed the wire, regardless of LWW outcome.
        - ``cache_sync_entries_upserted{peer}`` += len(rows) — every row we
          *attempted* to UPSERT, regardless of whether LWW accepted. See
          the ``_sync_once`` docstring for why we count attempts, not
          applied rows.

        Memory invalidation: every key we attempted gets popped from
        ``Cache._memory``. We don't know per-row whether LWW accepted, so
        the simpler invariant is: drop from memory, let the next ``get``
        fall through to the DB. This keeps memory coherent with the DB
        without a SELECT-back-and-compare per row.

        No-op fast paths:
        - Empty ``rows`` → return after counting fetched==0 (no counter
          increment, no writer round-trip).
        - Writer connection gone (engine stopped mid-cycle) → return.
        """
        if not rows:
            return
        if self._writer_db is None:
            return

        # executemany over LWW_UPSERT_SQL — one SQLite round trip for the
        # whole batch. aiosqlite's default isolation opens an implicit
        # transaction on the first DML; the commit() below closes it.
        await self._writer_db.executemany(LWW_UPSERT_SQL, rows)
        await self._writer_db.commit()

        # Both counters tick AFTER commit so they stay consistent under
        # commit-failure: a crash between executemany and commit leaves the
        # DB untouched and also leaves both counters at their pre-call
        # values. Mirrors the flush path's post-commit metric-tick pattern —
        # counters track work that actually landed, not work attempted.
        metrics.cache_sync_entries_fetched.labels(peer=peer_name).inc(len(rows))
        metrics.cache_sync_entries_upserted.labels(peer=peer_name).inc(len(rows))

        # Memory invalidation: unconditional pop for every key we
        # attempted. Simpler than per-row LWW-result bookkeeping, and any
        # caller that needs the freshest value will hit the DB on the next
        # ``get`` which already includes the merged LWW result.
        #
        # Delegate to ``Cache._invalidate_memory_keys`` — the helper
        # handles bytes_sum bookkeeping and gauge refresh atomically,
        # keeping the memory-side invariants encapsulated on Cache.
        if self._cache is not None:
            self._cache._invalidate_memory_keys([row[0] for row in rows])
