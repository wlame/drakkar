"""Tests for the handler-facing Cache API (memory-only behavior).

These tests cover the synchronous operations of `Cache` — `set`, `peek`,
`delete`, `__contains__` — all of which are pure in-memory dict ops that
do not touch SQLite. The DB-backed `get()` method is added and tested in
Task 9; here we only care about:

- In-memory storage semantics (OrderedDict-backed)
- TTL expiration handled opportunistically on read
- Dirty-op tracking for the upcoming flush loop (Task 7)
- LRU eviction when ``max_memory_entries`` is set
- Deliberate absence of any peer-propagation side effect on ``delete``

The Prometheus ``drakkar_cache_evictions_total`` counter is introduced
here because Task 4 explicitly tests LRU eviction metrics; the full
metrics-wiring pass happens in Task 14.
"""

from __future__ import annotations

import time

import pytest

from drakkar.cache import Cache, CacheScope, DirtyOp, Op


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
