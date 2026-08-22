"""Tests for the Prometheus metrics wiring across the cache subsystem.

Individual counters are asserted alongside the code paths that drive
them (``cache_evictions``, ``cache_flush_entries``,
``cache_cleanup_removed`` + DB gauges, and the ``cache_sync_*``
counters) in their own test files. This file covers the remaining
metrics:

- Read-path: ``cache_hits{source}`` / ``cache_misses``
- Write-path: ``cache_writes{scope}`` / ``cache_deletes``
- Memory gauges (running sum): ``cache_entries_in_memory`` /
  ``cache_bytes_in_memory``

and wires them into ``Cache.set`` / ``Cache.get`` / ``Cache.delete`` plus
the cleanup/sync paths that need to keep the running sum coherent when
memory entries disappear.

This file tests every metric against the concrete call paths that drive
it, so any regression in the wiring is caught close to the source. It
also asserts the invariants the running-sum design depends on — e.g. the
byte gauge equals the UTF-8 length of the stored JSON, and the gauges
reflect live state after each mutation without walking the dict.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from prometheus_client.metrics import MetricWrapperBase

from drakkar import metrics
from drakkar.cache import (
    Cache,
    CacheEngine,
    CacheScope,
)
from drakkar.config import CacheConfig, UIConfig
from tests.conftest import make_ui_config

# --- helpers ----------------------------------------------------------------


def _counter_value(metric: MetricWrapperBase, **labels: str) -> float:
    """Read the current value of a prometheus counter (labelled or not).

    prometheus_client exposes ``._value.get()`` on each labelled child; for
    unlabelled counters the top-level counter itself has ``_value``. We try
    the labelled form first, falling back to the unlabelled access.
    """
    if labels:
        return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]
    return metric._value.get()  # type: ignore[attr-defined]


def _gauge_value(metric: MetricWrapperBase) -> float:
    """Read the current value of an unlabelled prometheus gauge."""
    return metric._value.get()  # type: ignore[attr-defined]


def _make_cache(*, max_memory_entries: int | None = None, worker_id: str = 'w1') -> Cache:
    """Build a Cache with a fixed worker id — tests don't care about the id."""
    return Cache(origin_worker_id=worker_id, max_memory_entries=max_memory_entries)


def make_debug_config(tmp_path: Path, **overrides) -> UIConfig:
    defaults: dict = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': False,
        'store_config': False,
        'store_state': False,
    }
    defaults.update(overrides)
    return make_ui_config(**defaults)


def make_cache_config(**overrides) -> CacheConfig:
    defaults: dict = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(tmp_path: Path, *, worker_id: str = 'w1', **cfg_overrides) -> CacheEngine:
    cache = Cache(origin_worker_id=worker_id)
    engine = CacheEngine(
        config=make_cache_config(**cfg_overrides),
        ui_config=make_debug_config(tmp_path),
        worker_id=worker_id,
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(cache)
    return engine


# --- hits / misses ----------------------------------------------------------


async def test_get_memory_hit_increments_hits_memory_counter(tmp_path):
    """``get`` served from memory ticks ``hits{source="memory"}`` by 1."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        cache.set('k', 'v')

        before = _counter_value(metrics.cache_hits, source='memory')
        result = await cache.get('k')
        after = _counter_value(metrics.cache_hits, source='memory')

        assert result == 'v'
        assert after - before == 1
    finally:
        await engine.stop()


async def test_get_db_hit_increments_hits_db_counter(tmp_path):
    """``get`` served from the DB fallback ticks ``hits{source="db"}`` by 1."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        # Populate the DB then drop from memory to force a DB fallback on get.
        cache.set('k', 'hello')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        cache._memory.clear()  # type: ignore[reportPrivateUsage]
        cache._bytes_sum = 0  # type: ignore[reportPrivateUsage]

        before = _counter_value(metrics.cache_hits, source='db')
        result = await cache.get('k')
        after = _counter_value(metrics.cache_hits, source='db')

        assert result == 'hello'
        assert after - before == 1
    finally:
        await engine.stop()


async def test_get_miss_increments_misses_counter(tmp_path):
    """``get`` on a missing key ticks ``misses`` by 1."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        before = _counter_value(metrics.cache_misses)
        result = await cache.get('missing')
        after = _counter_value(metrics.cache_misses)

        assert result is None
        assert after - before == 1
    finally:
        await engine.stop()


async def test_get_memory_hit_does_not_increment_db_or_miss():
    """A memory hit must not tick the db-hit or miss counters — otherwise
    operators can't distinguish memory vs db pressure."""
    cache = _make_cache()
    cache.set('k', 'v')

    db_before = _counter_value(metrics.cache_hits, source='db')
    miss_before = _counter_value(metrics.cache_misses)
    await cache.get('k')
    db_after = _counter_value(metrics.cache_hits, source='db')
    miss_after = _counter_value(metrics.cache_misses)

    assert db_after == db_before
    assert miss_after == miss_before


async def test_get_expired_memory_entry_increments_miss():
    """An expired memory entry falls through as a miss — the counter
    reflects the caller's intent (wanted a live value, got None)."""
    cache = _make_cache()
    cache.set('k', 'v', ttl=0.001)
    await asyncio.sleep(0.01)

    before = _counter_value(metrics.cache_misses)
    result = await cache.get('k')
    after = _counter_value(metrics.cache_misses)

    assert result is None
    assert after - before == 1


# --- writes / deletes -------------------------------------------------------


def test_set_with_local_scope_increments_writes_local():
    """Writes are labelled by scope — ``set(scope=LOCAL)`` ticks
    ``writes{scope="local"}``."""
    cache = _make_cache()
    before = _counter_value(metrics.cache_writes, scope='local')
    cache.set('k', 'v', scope=CacheScope.LOCAL)
    after = _counter_value(metrics.cache_writes, scope='local')
    assert after - before == 1


def test_set_with_cluster_scope_increments_writes_cluster():
    cache = _make_cache()
    before = _counter_value(metrics.cache_writes, scope='cluster')
    cache.set('k', 'v', scope=CacheScope.CLUSTER)
    after = _counter_value(metrics.cache_writes, scope='cluster')
    assert after - before == 1


def test_set_with_global_scope_increments_writes_global():
    cache = _make_cache()
    before = _counter_value(metrics.cache_writes, scope='global')
    cache.set('k', 'v', scope=CacheScope.GLOBAL)
    after = _counter_value(metrics.cache_writes, scope='global')
    assert after - before == 1


def test_set_with_each_scope_only_ticks_its_own_label():
    """A ``set(scope=LOCAL)`` must not increment cluster or global counters
    — otherwise the label split is useless."""
    cache = _make_cache()
    cluster_before = _counter_value(metrics.cache_writes, scope='cluster')
    global_before = _counter_value(metrics.cache_writes, scope='global')
    cache.set('k', 'v', scope=CacheScope.LOCAL)
    assert _counter_value(metrics.cache_writes, scope='cluster') == cluster_before
    assert _counter_value(metrics.cache_writes, scope='global') == global_before


def test_delete_increments_deletes_counter():
    """Every ``delete`` call ticks the deletes counter regardless of
    presence — matches the "user intent, not row state" semantics."""
    cache = _make_cache()
    cache.set('k', 'v')

    before = _counter_value(metrics.cache_deletes)
    cache.delete('k')
    after = _counter_value(metrics.cache_deletes)
    assert after - before == 1


def test_delete_on_missing_key_still_increments_deletes():
    """A delete against an absent key still ticks the counter — because the
    DB may still have a row we can't see from memory, so the call is still
    a legitimate operation."""
    cache = _make_cache()
    before = _counter_value(metrics.cache_deletes)
    cache.delete('never-existed')
    after = _counter_value(metrics.cache_deletes)
    assert after - before == 1


# --- memory gauges (running sum) --------------------------------------------


def test_set_updates_entries_in_memory_gauge():
    """``entries_in_memory`` reflects live memory dict size after each set."""
    cache = _make_cache()
    cache.set('a', 1)
    assert _gauge_value(metrics.cache_entries_in_memory) == 1
    cache.set('b', 2)
    assert _gauge_value(metrics.cache_entries_in_memory) == 2
    cache.set('c', 3)
    assert _gauge_value(metrics.cache_entries_in_memory) == 3


def test_set_updates_bytes_in_memory_gauge():
    """``bytes_in_memory`` reflects the running sum of ``size_bytes``."""
    cache = _make_cache()
    cache.set('a', 'hello')  # size = len(b'"hello"') = 7
    # size_bytes on the entry should match what the gauge reports
    entry = cache._memory['a']  # type: ignore[reportPrivateUsage]
    assert _gauge_value(metrics.cache_bytes_in_memory) == entry.size_bytes

    # Adding a second entry: gauge increases by the new entry's size_bytes
    cache.set('b', 'world')
    entry_b = cache._memory['b']  # type: ignore[reportPrivateUsage]
    assert _gauge_value(metrics.cache_bytes_in_memory) == entry.size_bytes + entry_b.size_bytes


def test_size_bytes_equals_serialized_utf8_length():
    """The stored ``size_bytes`` equals ``len(serialized.encode('utf-8'))``.

    This invariant is what lets the running-sum gauge reflect actual byte
    weight — any drift here breaks the ``bytes_in_memory`` semantics.
    """
    cache = _make_cache()
    cache.set('k', {'name': 'alice', 'items': [1, 2, 3], 'nested': {'a': 'b'}})
    entry = cache._memory['k']  # type: ignore[reportPrivateUsage]
    assert entry.size_bytes == len(entry.value.encode('utf-8'))
    assert entry.size_bytes > 0


def test_delete_decrements_gauges():
    """Deleting a present key drops ``entries_in_memory`` by 1 and
    ``bytes_in_memory`` by the entry's ``size_bytes``."""
    cache = _make_cache()
    cache.set('a', 'hello')
    cache.set('b', 'world')
    entry_b = cache._memory['b']  # type: ignore[reportPrivateUsage]
    before_bytes = _gauge_value(metrics.cache_bytes_in_memory)
    before_entries = _gauge_value(metrics.cache_entries_in_memory)

    cache.delete('b')
    assert _gauge_value(metrics.cache_entries_in_memory) == before_entries - 1
    assert _gauge_value(metrics.cache_bytes_in_memory) == before_bytes - entry_b.size_bytes


def test_delete_missing_key_does_not_change_gauges():
    """Deleting an absent key leaves gauges untouched — nothing was in
    memory to subtract."""
    cache = _make_cache()
    cache.set('a', 'v')
    entries_before = _gauge_value(metrics.cache_entries_in_memory)
    bytes_before = _gauge_value(metrics.cache_bytes_in_memory)

    cache.delete('never-existed')
    assert _gauge_value(metrics.cache_entries_in_memory) == entries_before
    assert _gauge_value(metrics.cache_bytes_in_memory) == bytes_before


def test_overwrite_set_adjusts_bytes_gauge_by_delta():
    """Overwriting a key subtracts the old size and adds the new one —
    the gauge reflects the post-overwrite total, not old+new."""
    cache = _make_cache()
    cache.set('k', 'a')  # short value
    first = cache._memory['k']  # type: ignore[reportPrivateUsage]
    cache.set('k', 'longer-string-value')  # longer value
    second = cache._memory['k']  # type: ignore[reportPrivateUsage]

    # After overwrite, gauge = only the new size, not first+second
    assert _gauge_value(metrics.cache_bytes_in_memory) == second.size_bytes
    # Entries stays at 1 since same key
    assert _gauge_value(metrics.cache_entries_in_memory) == 1
    # Sanity: second and first have different sizes
    assert second.size_bytes != first.size_bytes


def test_lru_eviction_decrements_gauges():
    """When LRU evicts a key, ``entries_in_memory`` drops by 1 and
    ``bytes_in_memory`` drops by the evicted entry's ``size_bytes``."""
    cache = _make_cache(max_memory_entries=1)
    # Use strings of distinctly different lengths so the size delta
    # proves the running sum is doing real accounting — not just
    # coincidentally equal.
    cache.set('a', 'short')
    entry_a = cache._memory['a']  # type: ignore[reportPrivateUsage]

    # Add 'b' — evicts 'a'. Gauge should now reflect only 'b's size.
    cache.set('b', 'much-longer-value-string')
    entry_b = cache._memory['b']  # type: ignore[reportPrivateUsage]
    assert _gauge_value(metrics.cache_entries_in_memory) == 1
    assert _gauge_value(metrics.cache_bytes_in_memory) == entry_b.size_bytes

    # Sanity: different sizes so the assertion is meaningful
    assert entry_a.size_bytes != entry_b.size_bytes


async def test_expired_peek_decrements_gauges():
    """Expired entry removed during peek → gauges track the shrinkage."""
    cache = _make_cache()
    cache.set('k', 'v', ttl=0.001)
    assert _gauge_value(metrics.cache_entries_in_memory) == 1
    await asyncio.sleep(0.01)
    cache.peek('k')  # triggers expire-pop
    assert _gauge_value(metrics.cache_entries_in_memory) == 0
    assert _gauge_value(metrics.cache_bytes_in_memory) == 0


async def test_get_expired_entry_decrements_gauges():
    """Expired entry evicted during get → gauges track the shrinkage."""
    cache = _make_cache()
    cache.set('k', 'v', ttl=0.001)
    await asyncio.sleep(0.01)
    await cache.get('k')
    assert _gauge_value(metrics.cache_entries_in_memory) == 0
    assert _gauge_value(metrics.cache_bytes_in_memory) == 0


# --- warm-on-read respects gauge + cap --------------------------------------


async def test_db_warm_on_read_updates_gauges(tmp_path):
    """A DB-fallback warm-back into memory increments the gauges.

    We bypass the Cache ``set`` path for the initial seeding (raw memory
    clear) so ``cache._bytes_sum`` starts at 0 and the gauge reset is
    visible to the assertion. After get, the gauges must equal the
    warmed-back entry's contribution exactly.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        cache.set('k', 'hello')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # clear memory so get() falls through to DB — then manually set
        # gauges to mirror the cleared state (the production cache keeps
        # gauges consistent via its own mutation paths).
        cache._memory.clear()  # type: ignore[reportPrivateUsage]
        cache._bytes_sum = 0  # type: ignore[reportPrivateUsage]
        metrics.cache_entries_in_memory.set(0)
        metrics.cache_bytes_in_memory.set(0)

        await cache.get('k')
        # After warm-on-read, gauges track the one warmed-back entry.
        warmed = cache._memory['k']  # type: ignore[reportPrivateUsage]
        assert _gauge_value(metrics.cache_entries_in_memory) == 1
        assert _gauge_value(metrics.cache_bytes_in_memory) == warmed.size_bytes
    finally:
        await engine.stop()


# --- running-sum invariant: no full-dict walk on gauge read ----------------


def test_bytes_gauge_does_not_walk_dict_on_read():
    """The ``bytes_in_memory`` gauge must report the running sum without
    walking the dict.

    We can't easily spy on the Prometheus client internals from Python,
    but we can verify the invariant: the gauge's value is equal to the
    Cache's ``_bytes_sum`` attribute at any point. If the implementation
    ever switched to a full-dict walk, ``_bytes_sum`` would go stale
    while the gauge still produced the correct value — catching that
    drift via this equality.
    """
    cache = _make_cache()
    cache.set('a', 'hello')
    cache.set('b', 'world')
    cache.set('c', 'again')

    # The gauge read must equal _bytes_sum exactly — no re-computation.
    assert _gauge_value(metrics.cache_bytes_in_memory) == cache._bytes_sum  # type: ignore[reportPrivateUsage]

    # Even after delete, the invariant holds.
    cache.delete('b')
    assert _gauge_value(metrics.cache_bytes_in_memory) == cache._bytes_sum  # type: ignore[reportPrivateUsage]


def test_bytes_sum_matches_sum_of_entries_after_many_mutations():
    """After a mix of sets / deletes / overwrites, ``_bytes_sum`` still
    equals the sum of size_bytes across the memory dict."""
    cache = _make_cache()
    cache.set('a', 'hello')
    cache.set('b', 'world')
    cache.set('a', 'much-longer-overwrite')  # overwrite
    cache.delete('b')
    cache.set('c', 'new-entry')

    expected = sum(e.size_bytes for e in cache._memory.values())  # type: ignore[reportPrivateUsage]
    assert cache._bytes_sum == expected  # type: ignore[reportPrivateUsage]
    assert _gauge_value(metrics.cache_bytes_in_memory) == expected


# --- isolation: delete's counter separate from flush's counter --------------


def test_delete_and_flush_delete_counters_are_independent():
    """``cache_deletes_total`` counts user ``delete()`` calls.
    ``cache_flush_entries_total{op="delete"}`` counts drained DELETE ops.
    They measure different things and must not be conflated.
    """
    cache = _make_cache()
    cache.set('k', 'v')
    user_before = _counter_value(metrics.cache_deletes)
    flush_before = _counter_value(metrics.cache_flush_entries, op='delete')
    cache.delete('k')
    user_after = _counter_value(metrics.cache_deletes)
    flush_after = _counter_value(metrics.cache_flush_entries, op='delete')

    assert user_after - user_before == 1  # user delete ticked
    # Flush counter did NOT move — flush hasn't run yet
    assert flush_after - flush_before == 0
