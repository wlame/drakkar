"""Cross-thread safety of the Cache sync ops (the handler.offload() contract).

``handler.offload()`` lets user code call ``cache.set`` / ``peek`` /
``delete`` / ``in`` from a pool thread while the event loop keeps using
the same Cache (including the engine's ``swap_dirty`` flush cycle and the
LRU eviction path). These tests hammer those paths concurrently and then
assert the invariants the class promises:

- ``_bytes_sum`` equals the sum of the sizes actually in ``_memory``
  (the running-sum bookkeeping never drifts);
- the LRU cap is respected;
- every write is represented in the dirty stream collected across
  concurrent ``swap_dirty`` calls (no write silently lost).

Deliberately no real DB, no engine, no event loop — pure thread hammer
with deterministic post-join assertions. A failure here is a race, so the
hammer sizes are chosen to finish in well under a second while still
giving the scheduler thousands of interleaving opportunities.
"""

from __future__ import annotations

import threading

from drakkar.cache import Cache, Op


def _make_cache(*, max_memory_entries: int | None = None) -> Cache:
    return Cache(origin_worker_id='worker-test', max_memory_entries=max_memory_entries)


def _bytes_invariant_holds(cache: Cache) -> bool:
    with cache._lock:
        return cache._bytes_sum == sum(entry.size_bytes for entry in cache._memory.values())


def test_concurrent_set_peek_delete_keeps_bytes_sum_coherent():
    """Four writer/reader threads on overlapping keys; running sum must not drift."""
    cache = _make_cache()
    barrier = threading.Barrier(4)

    def hammer(worker: int) -> None:
        barrier.wait()
        for i in range(2000):
            key = f'k{i % 50}'
            # Values of different sizes so a lost size adjustment shows up.
            cache.set(key, 'x' * ((worker + 1) * (i % 7 + 1)))
            cache.peek(key)
            if i % 5 == worker % 5:
                cache.delete(key)
            _ = key in cache

    threads = [threading.Thread(target=hammer, args=(w,)) for w in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert _bytes_invariant_holds(cache)


def test_concurrent_set_with_lru_eviction_keeps_invariants():
    """Eviction under contention: cap respected, byte sum coherent."""
    cache = _make_cache(max_memory_entries=10)
    barrier = threading.Barrier(3)

    def hammer(worker: int) -> None:
        barrier.wait()
        for i in range(3000):
            cache.set(f'w{worker}-k{i % 40}', 'v' * (i % 11 + 1))
            cache.peek(f'w{worker}-k{(i * 7) % 40}')

    threads = [threading.Thread(target=hammer, args=(w,)) for w in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with cache._lock:
        assert len(cache._memory) <= 10
    assert _bytes_invariant_holds(cache)


def test_swap_dirty_racing_writers_loses_no_write():
    """A flush-style swap loop racing thread writers sees every key at least once.

    Models the engine's ``_flush_once`` swap running on the loop thread
    while offloaded code writes: the union of all swapped snapshots plus
    the final dirty map must cover every key any writer set — a torn swap
    would drop some.
    """
    cache = _make_cache()
    collected: dict[str, object] = {}
    stop = threading.Event()

    def swapper() -> None:
        while not stop.is_set():
            collected.update(cache.swap_dirty())
        collected.update(cache.swap_dirty())

    def writer(worker: int) -> None:
        for i in range(1500):
            cache.set(f'w{worker}-k{i}', i)

    swap_thread = threading.Thread(target=swapper)
    writer_threads = [threading.Thread(target=writer, args=(w,)) for w in range(3)]
    swap_thread.start()
    for t in writer_threads:
        t.start()
    for t in writer_threads:
        t.join()
    stop.set()
    swap_thread.join()

    expected = {f'w{w}-k{i}' for w in range(3) for i in range(1500)}
    assert set(collected) == expected
    assert all(op.op is Op.SET for op in collected.values())
    assert _bytes_invariant_holds(cache)


def test_restore_dirty_racing_writers_prefers_newer_ops():
    """restore_dirty under a racing writer never overwrites a newer op."""
    cache = _make_cache()
    cache.set('shared', 'old')
    snapshot = cache.swap_dirty()

    done = threading.Event()

    def racing_writer() -> None:
        cache.set('shared', 'new')
        done.set()

    t = threading.Thread(target=racing_writer)
    t.start()
    done.wait(timeout=5)
    t.join()

    cache.restore_dirty(snapshot)
    with cache._lock:
        entry = cache._dirty['shared'].entry
    assert entry is not None
    # The racing set landed after the swap, so restore must not clobber it.
    assert cache.peek('shared') == 'new'
