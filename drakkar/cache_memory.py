"""Handler-facing in-memory cache: ``Cache`` (real LRU) and ``NoOpCache`` stub.

The handler API surface — ``set`` / ``peek`` / ``delete`` / ``__contains__`` /
async ``get`` — lives here. Persistence (write-behind to SQLite, peer-sync
across workers) is owned by ``CacheEngine`` in ``drakkar.cache_engine``. This
split is intentional: a handler that calls ``self.cache.set(...)`` only
needs to know about this module's contract, not the engine's lifecycle.

Why ``OrderedDict``?
--------------------
LRU eviction (when ``max_memory_entries`` is set) relies on access order.
``OrderedDict.move_to_end(key)`` is O(1) and the class preserves insertion
order by contract — regular ``dict`` makes insertion-order guarantees too
but lacks the ``move_to_end`` primitive, so we'd otherwise pop-and-reinsert.

Eviction policy
---------------
When ``max_memory_entries`` is set and the dict would grow past the cap,
we pop the LRU key (the first item in iteration order). The evicted key's
dirty-op is **preserved** — the DB is the source of truth, and any pending
SET still needs to reach it via flush. Losing the dirty entry would silently
drop the write. Evicted entries fall through to the DB on next read; the DB
will re-warm ``_memory`` in ``get()``.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

import aiosqlite
from pydantic import BaseModel

from drakkar import metrics
from drakkar.cache_models import (
    CacheEntry,
    CacheScope,
    DirtyOp,
    Op,
    _now_ms,
    decode_value,
    encode_value,
)


class Cache:
    """In-memory key/value cache surfaced to handlers as ``self.cache``.

    This class covers the **synchronous**, memory-only operations of the
    cache contract: ``set``, ``peek``, ``delete``, ``__contains__``. All of
    these are GIL-safe dict manipulations; no I/O happens on these calls.

    The async DB-backed ``get()`` (memory-miss → DB fallback) lives here too
    — the engine wires its reader connection in via ``attach_reader_db`` so
    a memory miss can fall through to SQLite without touching the writer.
    Flush to SQLite and peer sync read from the ``_dirty`` map this class
    populates.
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
        # pending mutations for the next flush
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
                (serialized via ``model_dump_json``). See ``encode_value``.
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
        encoded, size_bytes = encode_value(value)
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
        return decode_value(entry.value)

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
            # ``decode_value`` accepts ``as_type=None`` natively — no ternary
            # needed to special-case the "no revival" branch.
            return decode_value(entry.value, as_type=as_type)

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
        # ``decode_value`` accepts ``as_type=None`` — no ternary needed.
        return decode_value(warmed.value, as_type=as_type)

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
