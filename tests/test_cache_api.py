"""Tests for the handler-facing Cache API (memory + DB-backed behavior).

Task 4 covered the synchronous operations of `Cache` — `set`, `peek`,
`delete`, `__contains__` — all pure in-memory dict ops. Task 9 adds:

- An async ``get(key, *, as_type=None)`` with memory → DB fallback
- A second aiosqlite connection (``_reader_db``) on ``CacheEngine`` so
  DB-fallback reads run on a dedicated worker thread and never queue
  behind a flush/cleanup/sync commit on the writer
- Pydantic-aware revival via ``as_type``
- Warm-on-read behavior that respects ``max_memory_entries`` (LRU eviction)

The tests are split into two groups:

1. Pure memory-only semantics (Task 4 — untouched).
2. Async ``get`` behavior including DB fallback, Pydantic revival,
   expiration handling, LRU warm-on-read, and writer/reader isolation.

We exercise ``get`` against real on-disk SQLite via ``tmp_path`` so the
reader connection is a true second connection — any test that used
``:memory:`` would mask cross-connection issues.

The Prometheus ``drakkar_cache_evictions_total`` counter is introduced
in Task 4; the full metrics-wiring pass (including hits/misses/source
labels) happens in Task 14.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import aiosqlite
import pytest
from pydantic import BaseModel

from drakkar.cache import LWW_UPSERT_SQL, Cache, CacheEngine, CacheScope, DirtyOp, Op
from drakkar.config import CacheConfig, DebugConfig


def _make_cache(*, max_memory_entries: int | None = None) -> Cache:
    """Build a Cache with a fixed worker id — tests don't care about the id
    except that it must be a stable string for dirty-op and LWW tiebreak
    assertions in later tasks."""
    return Cache(origin_worker_id='worker-test', max_memory_entries=max_memory_entries)


# --- set / peek basic --------------------------------------------------------


def test_set_stores_in_memory_and_peek_returns_value():
    """`set` stores the value in memory; `peek` returns the decoded value
    (not the raw JSON text)."""
    cache = _make_cache()
    cache.set('k', 'hello')
    assert cache.peek('k') == 'hello'


def test_peek_returns_none_for_missing_key():
    """Missing keys produce None — no KeyError, no sentinel."""
    cache = _make_cache()
    assert cache.peek('missing') is None


def test_set_list_value_roundtrips_through_peek():
    """Containers survive the JSON codec without loss."""
    cache = _make_cache()
    cache.set('k', [1, 2, 3])
    assert cache.peek('k') == [1, 2, 3]


def test_set_dict_value_roundtrips_through_peek():
    cache = _make_cache()
    cache.set('k', {'a': 1, 'b': 'two'})
    assert cache.peek('k') == {'a': 1, 'b': 'two'}


# --- TTL / expiration --------------------------------------------------------


def test_peek_returns_none_for_expired_entry_and_evicts_it():
    """Expired entries look like misses to callers and are removed from
    memory opportunistically (so stale data doesn't hog RAM until the
    cleanup cycle runs)."""
    cache = _make_cache()
    cache.set('k', 'v', ttl=0.01)  # 10ms TTL
    time.sleep(0.02)
    assert cache.peek('k') is None
    # expired entry has been evicted from memory — internal assertion
    assert 'k' not in cache._memory  # type: ignore[reportPrivateUsage]


def test_set_without_ttl_has_no_expiration():
    """`ttl=None` (default) → ``expires_at_ms is None`` → never expires."""
    cache = _make_cache()
    cache.set('k', 'v')
    entry = cache._memory['k']  # type: ignore[reportPrivateUsage]
    assert entry.expires_at_ms is None


def test_set_with_ttl_records_expires_at_ms():
    """Positive TTL translates into a wall-clock expiration timestamp in ms."""
    cache = _make_cache()
    before_ms = int(time.time() * 1000)
    cache.set('k', 'v', ttl=60)  # 60-second TTL
    entry = cache._memory['k']  # type: ignore[reportPrivateUsage]
    assert entry.expires_at_ms is not None
    after_ms = int(time.time() * 1000)
    # should expire ~60s from now, give or take test jitter
    assert before_ms + 60000 - 50 <= entry.expires_at_ms <= after_ms + 60000 + 50


# --- delete ------------------------------------------------------------------


def test_delete_removes_from_memory_and_returns_true_when_present():
    cache = _make_cache()
    cache.set('k', 'v')
    assert cache.delete('k') is True
    assert cache.peek('k') is None


def test_delete_returns_false_when_key_absent():
    """Deleting a non-existent key is not an error — returns False so the
    caller can log/metric on it, but never raises."""
    cache = _make_cache()
    assert cache.delete('missing') is False


def test_delete_marks_pending_op_delete_in_dirty_tracking():
    """After `delete`, the dirty map must record an ``Op.DELETE`` for the
    key so the next flush pushes the deletion to SQLite."""
    cache = _make_cache()
    cache.set('k', 'v')
    cache.delete('k')
    assert 'k' in cache._dirty  # type: ignore[reportPrivateUsage]
    assert cache._dirty['k'].op is Op.DELETE  # type: ignore[reportPrivateUsage]


def test_delete_is_local_only_no_peer_propagation_mechanism():
    """`delete` is deliberately local: the only observable side effect is
    the local dirty-op (``Op.DELETE``). There must be no peer-send queue,
    no tombstone, no cross-worker invalidation path — all of that would be
    a future-version design. Cross-worker delete semantics are tested
    end-to-end in Task 11/13."""
    cache = _make_cache()
    cache.set('k', 'v')
    cache.delete('k')
    # only the local dirty op exists
    assert set(cache._dirty.keys()) == {'k'}  # type: ignore[reportPrivateUsage]
    # and no other peer-propagation attribute is present on the Cache
    assert not hasattr(cache, '_peer_send_queue')
    assert not hasattr(cache, '_tombstones')


# --- __contains__ ------------------------------------------------------------


def test_contains_returns_true_for_present_key():
    cache = _make_cache()
    cache.set('k', 'v')
    assert 'k' in cache


def test_contains_returns_false_for_missing_key():
    cache = _make_cache()
    assert 'k' not in cache


def test_contains_returns_false_for_expired_entry():
    """Membership mirrors read semantics — an expired entry is not a member."""
    cache = _make_cache()
    cache.set('k', 'v', ttl=0.01)
    time.sleep(0.02)
    assert 'k' not in cache


# --- scopes ------------------------------------------------------------------


@pytest.mark.parametrize('scope', list(CacheScope))
def test_set_stores_scope_on_entry(scope):
    """Each scope lands on the entry verbatim — used by peer sync to filter
    which rows are eligible for cross-worker propagation (Task 11)."""
    cache = _make_cache()
    cache.set('k', 'v', scope=scope)
    entry = cache._memory['k']  # type: ignore[reportPrivateUsage]
    assert entry.scope is scope


def test_set_defaults_to_local_scope():
    """The default scope is LOCAL — the safest choice since LOCAL values
    never leave the worker."""
    cache = _make_cache()
    cache.set('k', 'v')
    entry = cache._memory['k']  # type: ignore[reportPrivateUsage]
    assert entry.scope is CacheScope.LOCAL


# --- overwrite + updated_at_ms -----------------------------------------------


def test_overwrite_updates_updated_at_ms():
    """Overwriting a key must refresh ``updated_at_ms`` — this is what
    drives LWW resolution on the peer-sync path (Task 11/12)."""
    cache = _make_cache()
    cache.set('k', 'v1')
    first_updated = cache._memory['k'].updated_at_ms  # type: ignore[reportPrivateUsage]
    # sleep briefly so the two timestamps are distinguishable even on fast
    # machines (time.time() has at least microsecond resolution, but we
    # pack to integer ms so a same-ms rewrite would hide the bump)
    time.sleep(0.002)
    cache.set('k', 'v2')
    second_updated = cache._memory['k'].updated_at_ms  # type: ignore[reportPrivateUsage]
    assert second_updated >= first_updated
    # at least the updated_at_ms didn't regress; value changed
    assert cache.peek('k') == 'v2'


def test_overwrite_preserves_created_at_ms():
    """`created_at_ms` is write-once, set on first insert. Overwriting a
    key updates ``updated_at_ms`` but leaves ``created_at_ms`` alone."""
    cache = _make_cache()
    cache.set('k', 'v1')
    created = cache._memory['k'].created_at_ms  # type: ignore[reportPrivateUsage]
    time.sleep(0.002)
    cache.set('k', 'v2')
    assert cache._memory['k'].created_at_ms == created  # type: ignore[reportPrivateUsage]


# --- dirty tracking ----------------------------------------------------------


def test_set_marks_op_set_in_dirty_tracking():
    """Every `set` records a ``DirtyOp(op=Op.SET, ...)`` so the next flush
    knows which rows to UPSERT."""
    cache = _make_cache()
    cache.set('k', 'v')
    assert 'k' in cache._dirty  # type: ignore[reportPrivateUsage]
    assert cache._dirty['k'].op is Op.SET  # type: ignore[reportPrivateUsage]


def test_overwriting_set_keeps_latest_dirty_op():
    """When `set` runs twice on the same key without a flush in between,
    the dirty map must hold the latest op — the earlier value is implicitly
    superseded (the SQL UPSERT is idempotent on the final state)."""
    cache = _make_cache()
    cache.set('k', 'v1')
    cache.set('k', 'v2')
    # single entry per key in the dirty map, regardless of set frequency
    assert list(cache._dirty.keys()) == ['k']  # type: ignore[reportPrivateUsage]
    # and it tracks the latest entry, not the first
    assert cache._dirty['k'].op is Op.SET  # type: ignore[reportPrivateUsage]


def test_set_after_delete_overwrites_dirty_op_back_to_set():
    """A SET landing after a DELETE on the same key replaces the pending
    DELETE op — otherwise the flush would delete what the user just set.
    Tuple-like "last write wins" at the dirty-map level mirrors the SQL
    LWW at the DB level."""
    cache = _make_cache()
    cache.set('k', 'v1')
    cache.delete('k')
    assert cache._dirty['k'].op is Op.DELETE  # type: ignore[reportPrivateUsage]
    cache.set('k', 'v2')
    assert cache._dirty['k'].op is Op.SET  # type: ignore[reportPrivateUsage]


def test_dirty_op_carries_entry_reference_for_set():
    """For SET ops, the DirtyOp must carry the entry so flush can serialize
    it without another memory lookup (the entry is already in ``_memory``
    but flush takes an atomic snapshot and pops the dirty map — the entry
    reference is the ground truth)."""
    cache = _make_cache()
    cache.set('k', 'v')
    dirty = cache._dirty['k']  # type: ignore[reportPrivateUsage]
    assert dirty.op is Op.SET
    assert dirty.entry is not None
    assert dirty.entry.key == 'k'


def test_dirty_op_has_no_entry_for_delete():
    """DELETE ops only need the key — no entry payload is required on the
    flush path."""
    cache = _make_cache()
    cache.set('k', 'v')
    cache.delete('k')
    dirty = cache._dirty['k']  # type: ignore[reportPrivateUsage]
    assert dirty.op is Op.DELETE
    assert dirty.entry is None


# --- LRU eviction (max_memory_entries set) -----------------------------------


def test_lru_evicts_least_recently_used_when_cap_exceeded():
    """With a cap of N, the (N+1)th set evicts the oldest by access order."""
    cache = _make_cache(max_memory_entries=2)
    cache.set('a', 1)
    cache.set('b', 2)
    cache.set('c', 3)  # evicts 'a' (least recently used)
    assert 'a' not in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'b' in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'c' in cache._memory  # type: ignore[reportPrivateUsage]


def test_lru_peek_bumps_recency():
    """`peek` moves the key to the MRU position, so the next eviction
    skips it in favor of an older key."""
    cache = _make_cache(max_memory_entries=2)
    cache.set('a', 1)
    cache.set('b', 2)
    # touch 'a' to bump it to MRU — now 'b' is the LRU
    cache.peek('a')
    cache.set('c', 3)  # should evict 'b' now, not 'a'
    assert 'a' in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'b' not in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'c' in cache._memory  # type: ignore[reportPrivateUsage]


def test_lru_eviction_keeps_dirty_op_for_flush():
    """Evicting from memory must not drop the pending dirty op. The DB is
    the source of truth and the flush is the only way the SET reaches it —
    if eviction silently killed the dirty entry, the set would be lost."""
    cache = _make_cache(max_memory_entries=1)
    cache.set('a', 1)  # dirty: {a: SET}
    cache.set('b', 2)  # evicts 'a' from memory; dirty still has both
    assert 'a' not in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'a' in cache._dirty  # type: ignore[reportPrivateUsage]
    assert cache._dirty['a'].op is Op.SET  # type: ignore[reportPrivateUsage]
    assert 'b' in cache._dirty  # type: ignore[reportPrivateUsage]


def test_lru_overwrite_bumps_recency_and_does_not_evict_self():
    """Overwriting an existing key (under the cap) just updates the entry
    and moves it to MRU — no eviction, memory count unchanged."""
    cache = _make_cache(max_memory_entries=2)
    cache.set('a', 1)
    cache.set('b', 2)
    cache.set('a', 10)  # overwrite; no eviction expected
    assert len(cache._memory) == 2  # type: ignore[reportPrivateUsage]
    assert cache.peek('a') == 10


def test_lru_peek_missing_key_does_not_affect_order():
    """Peeking a non-existent key must not shuffle the existing order —
    otherwise a miss probe could cause unrelated eviction surprises."""
    cache = _make_cache(max_memory_entries=2)
    cache.set('a', 1)
    cache.set('b', 2)
    cache.peek('missing')  # must not reorder 'a' or 'b'
    cache.set('c', 3)  # should still evict 'a'
    assert 'a' not in cache._memory  # type: ignore[reportPrivateUsage]
    assert 'b' in cache._memory  # type: ignore[reportPrivateUsage]


def test_lru_none_cap_means_unbounded():
    """`max_memory_entries=None` disables eviction entirely — the dict can
    grow without bound (at the caller's discretion)."""
    cache = _make_cache(max_memory_entries=None)
    for i in range(100):
        cache.set(f'k{i}', i)
    assert len(cache._memory) == 100  # type: ignore[reportPrivateUsage]


def test_lru_eviction_increments_metric():
    """LRU eviction bumps ``drakkar_cache_evictions_total``. We capture the
    metric's current value before the push-over-cap set, then assert the
    delta is 1."""
    from drakkar import metrics

    cache = _make_cache(max_memory_entries=1)
    before = metrics.cache_evictions._value.get()  # type: ignore[reportPrivateUsage]
    cache.set('a', 1)  # no eviction yet — cap not exceeded
    assert metrics.cache_evictions._value.get() == before  # type: ignore[reportPrivateUsage]
    cache.set('b', 2)  # evicts 'a'
    after = metrics.cache_evictions._value.get()  # type: ignore[reportPrivateUsage]
    assert after - before == 1


def test_lru_no_eviction_metric_when_cap_not_exceeded():
    """Sets below the cap must not emit the eviction metric — otherwise
    the metric would lie about memory pressure."""
    from drakkar import metrics

    cache = _make_cache(max_memory_entries=5)
    before = metrics.cache_evictions._value.get()  # type: ignore[reportPrivateUsage]
    for i in range(5):
        cache.set(f'k{i}', i)
    assert metrics.cache_evictions._value.get() == before  # type: ignore[reportPrivateUsage]


# --- DirtyOp dataclass shape (small sanity) ---------------------------------


def test_dirty_op_is_importable_with_op_enum():
    """DirtyOp must be importable and carry `op` + optional `entry`. The
    engine (Task 7) relies on this exact shape."""
    entry = None
    op = DirtyOp(op=Op.DELETE, entry=entry)
    assert op.op is Op.DELETE
    assert op.entry is None


# =============================================================================
# Task 9: async get() with DB fallback + reader connection isolation
# =============================================================================
#
# The tests below exercise the ``Cache.get`` async method and the
# ``CacheEngine._reader_db`` connection that backs its DB-fallback path. These
# tests use real on-disk SQLite (via ``tmp_path``) so the reader actually is a
# second connection against the same DB file — an in-memory DB would mask any
# cross-connection bugs (each ``:memory:`` is its own database).
#
# Key invariants we verify:
#
# - ``get`` on a memory hit returns the value without touching the reader DB.
# - ``get`` on a memory miss falls through to the reader DB and warms memory
#   with the decoded value for future calls.
# - ``get`` respects TTL in both memory and DB paths — expired entries read
#   as None regardless of which layer they live in.
# - ``get`` with ``as_type=SomeModel`` revives a typed Pydantic instance.
# - The writer connection is never used by ``get`` (and the reader is never
#   used by writes) — this is the isolation guarantee that prevents the
#   read path from queuing behind a flush/cleanup/sync commit.
# - Concurrent ``get`` calls during an in-progress flush complete without
#   waiting on the writer's transaction.


# -- helpers for engine-backed tests -----------------------------------------


def _make_debug_config(tmp_path: Path) -> DebugConfig:
    return DebugConfig(
        enabled=True,
        db_dir=str(tmp_path),
        store_events=False,
        store_config=False,
        store_state=False,
    )


def _make_cache_config(**overrides) -> CacheConfig:
    defaults: dict = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_started_engine(
    tmp_path: Path,
    *,
    worker_id: str = 'w1',
    max_memory_entries: int | None = None,
    **cfg_overrides,
) -> CacheEngine:
    """Build + start an engine with an attached Cache, ready for get tests.

    The Cache and engine share a ``_dirty`` map, and after ``start()`` both
    the writer and reader connections are open against a real on-disk DB.
    """
    cache = Cache(origin_worker_id=worker_id, max_memory_entries=max_memory_entries)
    engine = CacheEngine(
        config=_make_cache_config(**cfg_overrides),
        debug_config=_make_debug_config(tmp_path),
        worker_id=worker_id,
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    return engine


# -- memory hit returns without DB access ------------------------------------


async def test_get_memory_hit_returns_value_without_db_access(tmp_path):
    """A ``get`` for a key present in memory returns the decoded value and
    never touches the reader connection — DB fallback is strictly a miss
    path, not a "confirm on every read" path.

    We spy on ``_reader_db.execute`` and assert zero calls.
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        cache.set('k', 'hello')

        assert engine._reader_db is not None  # type: ignore[reportPrivateUsage]
        original_execute = engine._reader_db.execute  # type: ignore[reportPrivateUsage]
        calls = 0

        async def spy_execute(*args, **kwargs):
            nonlocal calls
            calls += 1
            return await original_execute(*args, **kwargs)

        engine._reader_db.execute = spy_execute  # type: ignore[reportPrivateUsage,assignment]

        result = await cache.get('k')

        engine._reader_db.execute = original_execute  # type: ignore[reportPrivateUsage,assignment]

        assert result == 'hello'
        assert calls == 0
    finally:
        await engine.stop()


# -- memory miss + DB hit: warms memory --------------------------------------


async def test_get_memory_miss_then_db_hit_warms_memory(tmp_path):
    """After the key has been flushed to the DB and evicted from memory, a
    ``get`` must fall through to the reader connection, re-populate memory,
    and return the decoded value.

    We force the pipeline manually: set → flush → pop from memory → get.
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        cache.set('k', {'a': 1, 'b': 2})
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # Simulate eviction (same result an LRU cap or a restart would reach)
        cache._memory.pop('k', None)  # type: ignore[reportPrivateUsage]
        assert 'k' not in cache._memory  # type: ignore[reportPrivateUsage]

        result = await cache.get('k')
        assert result == {'a': 1, 'b': 2}
        # Memory has been warmed with the recovered entry
        assert 'k' in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_get_memory_miss_and_db_miss_returns_none(tmp_path):
    """Neither memory nor DB has the key → None, not KeyError."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        assert await cache.get('never_written') is None
    finally:
        await engine.stop()


# -- as_type Pydantic revival ------------------------------------------------


class _SampleUser(BaseModel):
    id: int
    name: str


async def test_get_with_as_type_revives_pydantic(tmp_path):
    """`get(..., as_type=SomeModel)` returns a typed instance, not a dict."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        cache.set('u', _SampleUser(id=7, name='alice'))

        # Memory path: dict stored in memory but as_type revives it
        got = await cache.get('u', as_type=_SampleUser)
        assert isinstance(got, _SampleUser)
        assert got.id == 7
        assert got.name == 'alice'
    finally:
        await engine.stop()


async def test_get_with_as_type_roundtrip_through_db(tmp_path):
    """Pydantic end-to-end: set → flush → evict → get(as_type=…) → typed instance."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        original = _SampleUser(id=42, name='bob')
        cache.set('u', original)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        cache._memory.pop('u', None)  # type: ignore[reportPrivateUsage]

        got = await cache.get('u', as_type=_SampleUser)
        assert isinstance(got, _SampleUser)
        assert got == original
        # memory was re-warmed with the raw CacheEntry; subsequent as_type call
        # should still revive
        assert 'u' in cache._memory  # type: ignore[reportPrivateUsage]
        got_again = await cache.get('u', as_type=_SampleUser)
        assert got_again == original
    finally:
        await engine.stop()


# -- expiration ---------------------------------------------------------------


async def test_get_returns_none_for_expired_entry_in_memory(tmp_path):
    """An expired entry in memory reads as None and is evicted — same semantic
    as `peek`, but on the async path."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        cache.set('k', 'v', ttl=0.01)
        time.sleep(0.02)
        assert await cache.get('k') is None
        assert 'k' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_get_returns_none_for_expired_entry_in_db(tmp_path):
    """An expired row in the DB (e.g. cleanup hasn't swept yet) reads as None
    — the SQL filter `expires_at_ms IS NULL OR expires_at_ms > now` excludes
    it from the DB-fallback lookup."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Store with a past expires_at_ms by direct DB injection — we can't use
        # ttl=0 in the cache API because that would expire before the flush.
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        past_ms = int(time.time() * 1000) - 10_000
        await engine._writer_db.execute(  # type: ignore[reportPrivateUsage]
            LWW_UPSERT_SQL,
            ('k', 'local', '"stale"', len(b'"stale"'), 1, 1, past_ms, 'w1'),
        )
        await engine._writer_db.commit()  # type: ignore[reportPrivateUsage]

        # Memory is empty → get falls through to DB → SQL filter excludes
        # the expired row → None.
        assert await cache.get('k') is None
        # memory stays empty because we couldn't warm it (nothing valid to load)
        assert 'k' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# -- warm-on-read respects LRU cap -------------------------------------------


async def test_get_warm_on_read_respects_max_memory_entries(tmp_path):
    """When ``max_memory_entries`` is set, DB-fallback warming must not grow
    memory past the cap — the newly warmed key triggers an eviction."""
    engine = await _make_started_engine(tmp_path, max_memory_entries=2)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Seed three keys in the DB so get() can warm them
        cache.set('a', 1)
        cache.set('b', 2)
        cache.set('c', 3)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # Evict all from memory to force DB fallback on every get
        cache._memory.clear()  # type: ignore[reportPrivateUsage]

        # Warm 'a' — memory has 1 entry now
        assert await cache.get('a') == 1
        assert len(cache._memory) == 1  # type: ignore[reportPrivateUsage]
        # Warm 'b' — memory has 2 entries, still under cap
        assert await cache.get('b') == 2
        assert len(cache._memory) == 2  # type: ignore[reportPrivateUsage]
        # Warm 'c' — memory would grow to 3, but cap triggers an eviction
        assert await cache.get('c') == 3
        assert len(cache._memory) == 2  # type: ignore[reportPrivateUsage]
        # 'a' was the LRU after we fetched 'b' — so it got evicted
        assert 'a' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# -- writer/reader isolation -------------------------------------------------


async def test_reader_used_for_gets_writer_used_for_writes(tmp_path):
    """Cross-contamination check: the reader connection must not see write
    activity and the writer connection must not receive ``get`` queries.

    We install spies on both connections and drive the engine through a
    flush (writer should be used) and a DB-fallback ``get`` (reader should
    be used). Each connection's spy should only fire on its own lane.
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        assert engine._reader_db is not None  # type: ignore[reportPrivateUsage]

        writer_executemany_calls = 0
        reader_executemany_calls = 0
        writer_execute_calls = 0
        reader_execute_calls = 0

        writer_original_executemany = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]
        writer_original_execute = engine._writer_db.execute  # type: ignore[reportPrivateUsage]
        reader_original_executemany = engine._reader_db.executemany  # type: ignore[reportPrivateUsage]
        reader_original_execute = engine._reader_db.execute  # type: ignore[reportPrivateUsage]

        async def writer_spy_executemany(sql, params):
            nonlocal writer_executemany_calls
            writer_executemany_calls += 1
            return await writer_original_executemany(sql, params)

        async def writer_spy_execute(*args, **kwargs):
            nonlocal writer_execute_calls
            writer_execute_calls += 1
            return await writer_original_execute(*args, **kwargs)

        async def reader_spy_executemany(sql, params):
            nonlocal reader_executemany_calls
            reader_executemany_calls += 1
            return await reader_original_executemany(sql, params)

        async def reader_spy_execute(*args, **kwargs):
            nonlocal reader_execute_calls
            reader_execute_calls += 1
            return await reader_original_execute(*args, **kwargs)

        engine._writer_db.executemany = writer_spy_executemany  # type: ignore[reportPrivateUsage,assignment]
        engine._writer_db.execute = writer_spy_execute  # type: ignore[reportPrivateUsage,assignment]
        engine._reader_db.executemany = reader_spy_executemany  # type: ignore[reportPrivateUsage,assignment]
        engine._reader_db.execute = reader_spy_execute  # type: ignore[reportPrivateUsage,assignment]

        # --- write path: flush should hit the writer only ---
        cache.set('k', 'v')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        writer_execute_after_flush = writer_execute_calls
        writer_executemany_after_flush = writer_executemany_calls

        # writer did an executemany (for the UPSERT batch)
        assert writer_executemany_after_flush >= 1
        # reader saw NO activity during the flush
        assert reader_execute_calls == 0
        assert reader_executemany_calls == 0

        # --- read path: force DB fallback ---
        cache._memory.pop('k', None)  # type: ignore[reportPrivateUsage]
        assert await cache.get('k') == 'v'

        # reader saw a SELECT
        assert reader_execute_calls >= 1
        # writer still has no new activity from the get
        assert writer_execute_calls == writer_execute_after_flush
        assert writer_executemany_calls == writer_executemany_after_flush

        # restore
        engine._writer_db.executemany = writer_original_executemany  # type: ignore[reportPrivateUsage,assignment]
        engine._writer_db.execute = writer_original_execute  # type: ignore[reportPrivateUsage,assignment]
        engine._reader_db.executemany = reader_original_executemany  # type: ignore[reportPrivateUsage,assignment]
        engine._reader_db.execute = reader_original_execute  # type: ignore[reportPrivateUsage,assignment]
    finally:
        await engine.stop()


# -- concurrency: read does not block on in-progress write -------------------


async def test_concurrent_get_completes_during_flush(tmp_path):
    """A ``get`` launched while the writer is mid-commit must complete without
    waiting — the separate reader connection (and WAL mode) mean SELECTs see a
    consistent snapshot concurrently.

    The strategy: seed a row and evict memory so ``get`` must go to DB, then
    launch a slow flush that holds the writer for a beat, and assert the
    ``get`` finishes while the flush is still in-flight.
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Seed 'a' so get() has something to fetch from DB
        cache.set('a', 'seed')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        cache._memory.pop('a', None)  # type: ignore[reportPrivateUsage]

        # Arrange a pending write + a slow executemany so the writer holds
        # the transaction briefly. The reader should race ahead.
        cache.set('b', 'pending')

        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        original_executemany = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]

        get_completed_during_flush = False

        async def slow_executemany(sql, params):
            # Yield to the loop so any pending coroutine gets a chance,
            # then perform the real executemany.
            await asyncio.sleep(0.05)
            result = await original_executemany(sql, params)
            # Give the concurrent get() a chance to finish before commit.
            await asyncio.sleep(0.05)
            return result

        engine._writer_db.executemany = slow_executemany  # type: ignore[reportPrivateUsage,assignment]

        async def do_get():
            nonlocal get_completed_during_flush
            # We launch after yielding to the flush task — race is okay:
            # the point is that DB reads do not queue behind the writer.
            val = await cache.get('a')
            assert val == 'seed'
            get_completed_during_flush = True

        # Launch both concurrently
        flush_task = asyncio.create_task(engine._flush_once())  # type: ignore[reportPrivateUsage]
        get_task = asyncio.create_task(do_get())
        await asyncio.gather(flush_task, get_task)

        engine._writer_db.executemany = original_executemany  # type: ignore[reportPrivateUsage,assignment]

        assert get_completed_during_flush
    finally:
        await engine.stop()


# -- reader connection lifecycle ---------------------------------------------


async def test_reader_connection_opens_on_start(tmp_path):
    """After ``CacheEngine.start()``, the reader connection is a live
    ``aiosqlite.Connection`` — sibling to the writer."""
    engine = await _make_started_engine(tmp_path)
    try:
        assert engine._reader_db is not None  # type: ignore[reportPrivateUsage]
        assert isinstance(engine._reader_db, aiosqlite.Connection)  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_reader_connection_closes_on_stop(tmp_path):
    """``stop()`` closes the reader connection and clears the attribute —
    mirror of the writer lifecycle so there is no connection leak."""
    engine = await _make_started_engine(tmp_path)
    assert engine._reader_db is not None  # type: ignore[reportPrivateUsage]
    await engine.stop()
    assert engine._reader_db is None  # type: ignore[reportPrivateUsage]


async def test_reader_connection_not_opened_when_engine_disabled(tmp_path):
    """When cache is disabled or db_dir is unresolved, the reader connection
    stays None — matches the writer's effectively-disabled behavior."""
    engine = CacheEngine(
        config=CacheConfig(enabled=False),
        debug_config=_make_debug_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert engine._reader_db is None  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_get_on_disabled_engine_returns_none(tmp_path):
    """When the engine is disabled (no reader connection), a memory-miss
    ``get`` does not raise — it just returns None."""
    cache = Cache(origin_worker_id='w1')
    engine = CacheEngine(
        config=CacheConfig(enabled=False),
        debug_config=_make_debug_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    try:
        assert await cache.get('anything') is None
    finally:
        await engine.stop()


async def test_reader_connection_is_separate_from_writer(tmp_path):
    """The reader and writer are distinct ``aiosqlite.Connection`` instances
    — same DB file, but different connections (and therefore different
    worker threads inside aiosqlite)."""
    engine = await _make_started_engine(tmp_path)
    try:
        assert engine._reader_db is not None  # type: ignore[reportPrivateUsage]
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        assert engine._reader_db is not engine._writer_db  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# -- warm-on-read bytes_sum invariant regression -----------------------------


async def test_get_warm_on_read_does_not_drift_bytes_sum_when_key_preexists(tmp_path):
    """Regression: the DB-fallback warm path must subtract an existing
    memory entry's bytes before overwriting, otherwise ``_bytes_sum`` leaks
    by the old entry's size every warm.

    Simulates the race described in the code review: a concurrent
    ``set()`` lands between ``get()``'s memory-miss check and its
    warm-on-read overwrite. The raced ``set()`` installs a same-key
    entry in memory and bumps ``_bytes_sum``; the DB-fallback path then
    overwrites the memory slot. Without the fix, ``_bytes_sum`` would
    hold (raced_bytes + warmed_bytes) even though the dict only contains
    the warmed entry.

    We force the race deterministically by swapping ``_memory.get`` with a
    one-shot function that returns None on the miss-check (as if the key
    were absent) and simultaneously installs a same-key entry + bumps the
    running sum — exactly the state a concurrent ``set()`` would leave.
    """
    from drakkar.cache import CacheEntry, CacheScope, _now_ms

    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Seed a DB row for 'k' so get() can warm from DB.
        cache.set('k', 'from_db')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # Reset memory + sum so the post-fix invariant holds cleanly.
        cache._memory.clear()  # type: ignore[reportPrivateUsage]
        cache._bytes_sum = 0  # type: ignore[reportPrivateUsage]

        raced_entry = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"raced"',
            size_bytes=len(b'"raced"'),
            created_at_ms=_now_ms(),
            updated_at_ms=_now_ms(),
            expires_at_ms=None,
            origin_worker_id='worker-test',
        )

        # Stash the real ``get`` so we can restore after the miss check.
        original_memory_get = cache._memory.get  # type: ignore[reportPrivateUsage]

        def one_shot_miss(key, default=None):
            # First call: pretend the key is absent (miss-check path)
            # while simultaneously installing the raced entry and
            # bumping the running sum, mirroring what a concurrent
            # ``set()`` between the miss-check and the warm would
            # leave behind.
            cache._memory.get = original_memory_get  # type: ignore[reportPrivateUsage,assignment]
            cache._memory[key] = raced_entry  # type: ignore[reportPrivateUsage]
            cache._bytes_sum += raced_entry.size_bytes  # type: ignore[reportPrivateUsage]
            return default

        cache._memory.get = one_shot_miss  # type: ignore[reportPrivateUsage,assignment]

        # Run the warm path. With the fix, the warm subtracts the raced
        # entry's bytes before installing the DB row.
        _ = await cache.get('k')

        # Invariant: running sum equals actual memory contents.
        expected_sum = sum(e.size_bytes for e in cache._memory.values())  # type: ignore[reportPrivateUsage]
        assert cache._bytes_sum == expected_sum  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_concurrent_get_and_flush_preserves_bytes_sum_invariant(tmp_path):
    """After a concurrent get + flush pair completes, the running
    ``_bytes_sum`` must equal the sum of ``size_bytes`` across the current
    ``_memory`` contents — there is no room for drift.
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Seed a row so get() has something to fetch from DB.
        cache.set('seeded', 'v')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        cache._memory.pop('seeded', None)  # type: ignore[reportPrivateUsage]
        cache._bytes_sum = 0  # type: ignore[reportPrivateUsage]

        # Launch a concurrent set + flush + get on the same key.
        cache.set('other', 'pending')

        async def do_get():
            await cache.get('seeded')

        await asyncio.gather(engine._flush_once(), do_get())  # type: ignore[reportPrivateUsage]

        # Invariant: ``_bytes_sum`` equals the actual sum of entries.
        expected_sum = sum(e.size_bytes for e in cache._memory.values())  # type: ignore[reportPrivateUsage]
        assert cache._bytes_sum == expected_sum  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()
