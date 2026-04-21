"""Tests for the CacheEngine cleanup loop (Task 10).

The cleanup loop reclaims space from expired entries. It runs on its own
periodic cadence (``cache.cleanup_interval_seconds``, default 60s) and
performs three jobs:

1. **DB-side purge** — ``DELETE FROM cache_entries WHERE expires_at_ms <
   now_ms`` drops every row that has a TTL and is past it. Rows with
   ``expires_at_ms IS NULL`` (no TTL) and rows still within their TTL are
   preserved by the WHERE clause.

2. **Memory-side purge** — any entries still sitting in ``Cache._memory``
   whose ``expires_at_ms`` is past are popped. Without this, ``peek()``
   and ``__contains__`` would still eventually evict them opportunistically
   but cleanup gives us a deterministic upper bound on staleness.

3. **Dirty-map cleanup** — any pending SET in ``Cache._dirty`` for a key
   we're cleaning up must be dropped too. Otherwise the next flush would
   re-insert a row we just deleted from the DB. This is the subtle
   correctness bit documented in the plan.

4. **DB-gauge refresh** — ``drakkar_cache_entries_in_db`` and
   ``drakkar_cache_bytes_in_db`` are refreshed here (not on the hot path)
   because counting DB rows on every ``set``/``get`` would defeat the
   running-sum design for the in-memory gauges. Operators get an
   approximate DB view that updates every cleanup cycle.

We test on real on-disk SQLite via ``tmp_path`` + ``aiosqlite`` and inspect
the resulting rows directly. The memory-dict and dirty-map assertions go
through the same Cache/CacheEngine handles the production code uses.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite

from drakkar import metrics
from drakkar.cache import (
    Cache,
    CacheEngine,
    CacheEntry,
    CacheScope,
    DirtyOp,
    Op,
)
from drakkar.config import CacheConfig, DebugConfig

# --- helpers ----------------------------------------------------------------


def make_debug_config(tmp_path: Path, **overrides) -> DebugConfig:
    defaults: dict = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': False,
        'store_config': False,
        'store_state': False,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


def make_cache_config(**overrides) -> CacheConfig:
    defaults: dict = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(tmp_path: Path, *, worker_id: str = 'w1', **cfg_overrides) -> CacheEngine:
    cache = Cache(origin_worker_id=worker_id)
    engine = CacheEngine(
        config=make_cache_config(**cfg_overrides),
        debug_config=make_debug_config(tmp_path),
        worker_id=worker_id,
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(cache)
    return engine


async def _count_rows(db_path: Path) -> int:
    async with aiosqlite.connect(str(db_path)) as db:
        cur = await db.execute('SELECT count(*) FROM cache_entries')
        row = await cur.fetchone()
        await cur.close()
        assert row is not None
        return row[0]


async def _fetch_keys(db_path: Path) -> set[str]:
    async with aiosqlite.connect(str(db_path)) as db:
        cur = await db.execute('SELECT key FROM cache_entries')
        rows = await cur.fetchall()
        await cur.close()
        return {r[0] for r in rows}


def _make_entry(
    *,
    key: str,
    value: str = '"v"',
    expires_at_ms: int | None,
    updated_at_ms: int = 1000,
    origin: str = 'w1',
    scope: CacheScope = CacheScope.LOCAL,
) -> CacheEntry:
    """Build a CacheEntry with explicit timestamps — bypasses ``_now_ms`` so
    we can deterministically set rows to "already expired"."""
    return CacheEntry(
        key=key,
        scope=scope,
        value=value,
        size_bytes=len(value.encode('utf-8')),
        created_at_ms=500,
        updated_at_ms=updated_at_ms,
        expires_at_ms=expires_at_ms,
        origin_worker_id=origin,
    )


async def _stage_raw_set(engine: CacheEngine, entry: CacheEntry) -> None:
    """Write a CacheEntry directly into the dirty map + memory, bypassing
    ``Cache.set`` so we can use absolute ``expires_at_ms`` values rather
    than TTL offsets from "now"."""
    cache = engine._cache  # type: ignore[reportPrivateUsage]
    assert cache is not None
    cache._memory[entry.key] = entry  # type: ignore[reportPrivateUsage]
    cache._dirty[entry.key] = DirtyOp(op=Op.SET, entry=entry)  # type: ignore[reportPrivateUsage]


# --- DB-side purge --------------------------------------------------------


async def test_cleanup_deletes_rows_with_expired_ttl(tmp_path):
    """Rows whose ``expires_at_ms`` is less than current wall-clock ms are
    dropped from the cache_entries table."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # Stage an already-expired entry via raw dirty-map write so we can
        # pick the absolute expires_at_ms rather than a TTL offset.
        expired = _make_entry(key='expired', expires_at_ms=1)  # 1 ms past epoch — long gone
        await _stage_raw_set(engine, expired)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert 'expired' in await _fetch_keys(db_path)

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        assert 'expired' not in await _fetch_keys(db_path)
    finally:
        await engine.stop()


async def test_cleanup_preserves_rows_with_no_ttl(tmp_path):
    """Rows with ``expires_at_ms IS NULL`` are never cleaned up — a ``set``
    without a TTL should live until explicitly deleted."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        no_ttl = _make_entry(key='forever', expires_at_ms=None)
        await _stage_raw_set(engine, no_ttl)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert 'forever' in await _fetch_keys(db_path)
    finally:
        await engine.stop()


async def test_cleanup_preserves_rows_with_future_ttl(tmp_path):
    """Rows with a TTL in the future are kept — cleanup only touches past-
    due rows."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # expires far in the future (year 2286+)
        future = _make_entry(key='fresh', expires_at_ms=10_000_000_000_000)
        await _stage_raw_set(engine, future)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert 'fresh' in await _fetch_keys(db_path)
    finally:
        await engine.stop()


async def test_cleanup_mixed_ttls(tmp_path):
    """In a mixed DB, only the expired rows disappear — null-TTL and future-
    TTL rows survive cleanup."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        await _stage_raw_set(engine, _make_entry(key='expired_a', expires_at_ms=1))
        await _stage_raw_set(engine, _make_entry(key='expired_b', expires_at_ms=50))
        await _stage_raw_set(engine, _make_entry(key='no_ttl', expires_at_ms=None))
        await _stage_raw_set(engine, _make_entry(key='future', expires_at_ms=10_000_000_000_000))
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        remaining = await _fetch_keys(db_path)
        assert remaining == {'no_ttl', 'future'}
    finally:
        await engine.stop()


# --- memory-side purge ----------------------------------------------------


async def test_cleanup_pops_expired_entries_from_memory(tmp_path):
    """After cleanup, any expired entries still sitting in ``_memory``
    should be gone. This guarantees an upper bound on staleness — without
    the memory sweep, expired keys would linger until the next peek or
    get that probes them."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        expired = _make_entry(key='stale', expires_at_ms=1)
        cache._memory['stale'] = expired  # type: ignore[reportPrivateUsage]
        # Also populate the DB row so cleanup has work to do; otherwise the
        # memory-only purge is still required to run
        await _stage_raw_set(engine, expired)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        assert 'stale' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_cleanup_preserves_unexpired_memory_entries(tmp_path):
    """Entries with no TTL or future TTL stay in memory after cleanup."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        no_ttl = _make_entry(key='keep_null', expires_at_ms=None)
        future = _make_entry(key='keep_future', expires_at_ms=10_000_000_000_000)
        cache._memory['keep_null'] = no_ttl  # type: ignore[reportPrivateUsage]
        cache._memory['keep_future'] = future  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        assert 'keep_null' in cache._memory  # type: ignore[reportPrivateUsage]
        assert 'keep_future' in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# --- dirty-map cleanup ----------------------------------------------------


async def test_cleanup_drops_pending_sets_for_expired_keys(tmp_path):
    """If an expired key has a pending SET in ``_dirty``, the cleanup must
    drop it — otherwise the next flush would re-insert the row we just
    deleted.

    We stage an expired entry, run cleanup (which removes the DB row and
    the memory entry), and verify the dirty map no longer carries the SET.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        expired = _make_entry(key='zombie', expires_at_ms=1)
        # Put both the memory entry and the pending set in place — without
        # flushing first, so the DB has no row but the dirty map does.
        cache._memory['zombie'] = expired  # type: ignore[reportPrivateUsage]
        cache._dirty['zombie'] = DirtyOp(op=Op.SET, entry=expired)  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        # Dirty SET must be dropped so next flush doesn't revive the row
        assert 'zombie' not in cache._dirty  # type: ignore[reportPrivateUsage]
        assert 'zombie' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_cleanup_preserves_pending_delete_for_expired_key(tmp_path):
    """A pending DELETE for an expired key stays — the delete is still the
    right operation to reach the DB. Cleanup dropping it would be a no-op
    (the row is already expired and will be cleaned up), but leaving it
    through is also harmless and simpler.

    The plan's "drop pending-flush ops" comment specifically targets the
    dangerous case: a pending SET that would re-insert a just-cleaned row.
    A pending DELETE has no such risk.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # stage a DB row first so the DELETE has something to delete
        expired = _make_entry(key='delete_me', expires_at_ms=1)
        await _stage_raw_set(engine, expired)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # then queue a DELETE op for that key
        cache._dirty['delete_me'] = DirtyOp(op=Op.DELETE, entry=None)  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        # The dirty DELETE is still there — cleanup only drops dangerous
        # pending SETs, not pending DELETEs.
        assert cache._dirty.get('delete_me') is not None  # type: ignore[reportPrivateUsage]
        assert cache._dirty['delete_me'].op is Op.DELETE  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_cleanup_preserves_pending_sets_for_unexpired_keys(tmp_path):
    """Unexpired pending SETs stay put — cleanup only drops ops for keys
    whose row was actually cleaned up.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # populate a fresh set that should survive cleanup
        fresh = _make_entry(key='keeper', expires_at_ms=10_000_000_000_000)
        cache._memory['keeper'] = fresh  # type: ignore[reportPrivateUsage]
        cache._dirty['keeper'] = DirtyOp(op=Op.SET, entry=fresh)  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        assert 'keeper' in cache._dirty  # type: ignore[reportPrivateUsage]
        assert cache._dirty['keeper'].op is Op.SET  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# --- metrics --------------------------------------------------------------


def _counter_val(metric, **labels) -> float:
    if labels:
        return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]
    return metric._value.get()  # type: ignore[attr-defined]


def _gauge_val(metric, **labels) -> float:
    if labels:
        return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]
    return metric._value.get()  # type: ignore[attr-defined]


async def test_cleanup_removed_counter_increments(tmp_path):
    """``drakkar_cache_cleanup_removed_total`` counter advances by exactly
    the number of DB rows removed in a cleanup cycle."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        before = _counter_val(metrics.cache_cleanup_removed)

        # three expired rows
        for i in range(3):
            await _stage_raw_set(engine, _make_entry(key=f'exp_{i}', expires_at_ms=1))
        # one fresh row
        await _stage_raw_set(engine, _make_entry(key='fresh', expires_at_ms=10_000_000_000_000))
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        after = _counter_val(metrics.cache_cleanup_removed)
        assert after - before == 3
    finally:
        await engine.stop()


async def test_cleanup_refreshes_db_entries_gauge(tmp_path):
    """``drakkar_cache_entries_in_db`` gauge reflects the DB row count
    after cleanup. This is where the gauge gets refreshed — not on the hot
    path.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # Two live rows, one expired
        await _stage_raw_set(engine, _make_entry(key='a', expires_at_ms=None))
        await _stage_raw_set(engine, _make_entry(key='b', expires_at_ms=10_000_000_000_000))
        await _stage_raw_set(engine, _make_entry(key='c', expires_at_ms=1))
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        assert _gauge_val(metrics.cache_entries_in_db) == 2
    finally:
        await engine.stop()


async def test_cleanup_refreshes_db_bytes_gauge(tmp_path):
    """``drakkar_cache_bytes_in_db`` gauge reflects the total
    ``size_bytes`` of remaining rows after cleanup."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # value lengths chosen so the bytes sum is non-trivial
        kept1 = _make_entry(key='a', value='"aaaa"', expires_at_ms=None)
        kept2 = _make_entry(key='b', value='"bbbbbb"', expires_at_ms=10_000_000_000_000)
        expired = _make_entry(key='c', value='"ccccccc"', expires_at_ms=1)
        await _stage_raw_set(engine, kept1)
        await _stage_raw_set(engine, kept2)
        await _stage_raw_set(engine, expired)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await engine._cleanup_once()  # type: ignore[reportPrivateUsage]

        expected_bytes = kept1.size_bytes + kept2.size_bytes
        assert _gauge_val(metrics.cache_bytes_in_db) == expected_bytes
    finally:
        await engine.stop()


# --- periodic task registration ------------------------------------------


async def test_start_registers_cleanup_task_as_system_periodic(tmp_path, monkeypatch):
    """``start()`` schedules the cleanup loop via ``run_periodic_task`` with
    ``name='cache.cleanup'``, ``system=True``, and
    ``on_error='continue'``.

    As with the flush task in Task 8, we patch ``run_periodic_task`` on
    the ``drakkar.cache`` module and capture the call kwargs for each
    task. We allow multiple invocations (flush + cleanup) and pick the one
    targeting the cleanup name.
    """
    from drakkar import cache as cache_module

    captured: list[dict] = []

    async def spy_run_periodic_task(**kwargs):
        captured.append(kwargs)
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise

    monkeypatch.setattr(cache_module, 'run_periodic_task', spy_run_periodic_task)

    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        # give every scheduled task a chance to run and populate captured
        await asyncio.sleep(0)
        cleanup_calls = [c for c in captured if c.get('name') == 'cache.cleanup']
        assert cleanup_calls, 'cleanup task not registered'
        kwargs = cleanup_calls[0]
        assert kwargs.get('system') is True
        assert kwargs.get('on_error') == 'continue'
        assert kwargs.get('seconds') == engine._config.cleanup_interval_seconds  # type: ignore[reportPrivateUsage]
        assert kwargs.get('coro_fn') == engine._cleanup_once  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_start_creates_pending_cleanup_task(tmp_path):
    """After ``start()`` the cleanup task is a real pending asyncio.Task."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        task = engine._cleanup_task  # type: ignore[reportPrivateUsage]
        assert task is not None, 'engine did not record the cleanup asyncio.Task'
        assert isinstance(task, asyncio.Task)
        assert not task.done()
    finally:
        await engine.stop()


async def test_stop_cancels_cleanup_task(tmp_path):
    """``stop()`` cancels the cleanup task so the engine does not leak a
    running loop."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    task = engine._cleanup_task  # type: ignore[reportPrivateUsage]
    assert task is not None
    await engine.stop()
    assert task.done()
    assert engine._cleanup_task is None  # type: ignore[reportPrivateUsage]


async def test_cleanup_on_disabled_engine_is_noop(tmp_path):
    """When the engine has no writer connection (disabled), cleanup must
    be a no-op — not raise, not touch anything."""
    engine = CacheEngine(
        config=CacheConfig(enabled=False),
        debug_config=make_debug_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    cache = Cache(origin_worker_id='w1')
    engine.attach_cache(cache)
    await engine.start()
    await engine._cleanup_once()  # type: ignore[reportPrivateUsage]  # must not raise
    await engine.stop()
