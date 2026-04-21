"""Tests for the CacheEngine flush loop (Task 7).

The flush loop is the pipeline that moves pending mutations from the
Cache's in-memory ``_dirty`` map to the worker's SQLite cache DB. The
contract we verify here:

1. An ``Op.SET`` dirty op lands as a row via the LWW UPSERT.
2. An ``Op.DELETE`` dirty op removes the row (if any) from the DB.
3. Many mutations in one call are batched in a single transaction (one
   ``executemany`` per op type).
4. The atomic-swap pattern (``snapshot, self._dirty = self._dirty, {}``)
   means a ``set`` landing mid-flush goes to the fresh dict and is
   picked up next cycle — no writes lost.
5. The LWW UPSERT itself respects the ``updated_at_ms`` → ``origin_worker_id``
   priority order — older writes cannot clobber newer ones, identical
   timestamps resolved by lexicographic worker id.
6. An empty dirty map produces a no-op (no SQL executed, no metric ticks).
7. The Prometheus ``drakkar_cache_flush_entries_total{op=...}`` counter
   reflects exactly the rows touched per op.
8. Rows written by flush preserve ``size_bytes`` matching the encoded
   value's UTF-8 length — the running Prometheus ``bytes_in_*`` gauges
   rely on this invariant.

We exercise everything against real on-disk SQLite via ``tmp_path`` +
``aiosqlite`` rather than mocks. The lifecycle tests (Task 6) already
verified the schema lands correctly; here we drive it through the flush
code path and inspect the resulting rows.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from drakkar import metrics
from drakkar.cache import (
    LWW_UPSERT_SQL,
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
    """Build a CacheEngine + an attached Cache, pre-wired for flush tests.

    The engine and the cache share a ``_dirty`` map so populating the
    Cache via its public API (``set``/``delete``) or shoving a ``DirtyOp``
    directly onto ``_dirty`` both drive the same flush code path.
    """
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
    """Return the total number of rows in cache_entries."""
    async with aiosqlite.connect(str(db_path)) as db:
        cur = await db.execute('SELECT count(*) FROM cache_entries')
        row = await cur.fetchone()
        await cur.close()
        assert row is not None
        return row[0]


async def _fetch_row(db_path: Path, key: str) -> tuple | None:
    """Return the cache_entries row matching ``key`` or None."""
    async with aiosqlite.connect(str(db_path)) as db:
        cur = await db.execute(
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries WHERE key = ?',
            (key,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row


# --- SET path --------------------------------------------------------------


async def test_flush_once_inserts_row_for_single_set(tmp_path):
    """A single ``Op.SET`` in the dirty map lands as exactly one row."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('k', 'hello')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        key, scope, value, size_bytes, _created, _updated, expires, origin = row
        assert key == 'k'
        assert scope == 'local'
        assert value == '"hello"'
        assert size_bytes == len(b'"hello"')
        assert expires is None
        assert origin == 'w1'
    finally:
        await engine.stop()


async def test_flush_once_clears_dirty_map(tmp_path):
    """After flush, the dirty map is empty (ops were moved to DB)."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('k', 'v')  # type: ignore[reportPrivateUsage]
        assert engine._cache._dirty  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        assert engine._cache._dirty == {}  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_flush_once_stores_size_bytes_matching_utf8_length(tmp_path):
    """The ``size_bytes`` column equals the UTF-8 byte length of the
    serialized value — this is the invariant the Prometheus running-sum
    byte gauges depend on.

    We exercise a multi-character string + nested structure so the stored
    byte count is nontrivial (i.e. not something the DB could round-trip
    by coincidence).
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('k', {'name': 'alice', 'age': 30, 'items': [1, 2, 3]})  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        _key, _scope, value, size_bytes, *_ = row
        # size_bytes MUST equal the UTF-8 byte length of the stored JSON
        # — any drift here breaks the Prometheus running-sum gauges
        assert size_bytes == len(value.encode('utf-8'))
        assert size_bytes > 0
    finally:
        await engine.stop()


async def test_flush_once_preserves_scope(tmp_path):
    """Each ``CacheScope`` value is stored verbatim on flush."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('local_k', 'v', scope=CacheScope.LOCAL)  # type: ignore[reportPrivateUsage]
        engine._cache.set('cluster_k', 'v', scope=CacheScope.CLUSTER)  # type: ignore[reportPrivateUsage]
        engine._cache.set('global_k', 'v', scope=CacheScope.GLOBAL)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert (await _fetch_row(db_path, 'local_k'))[1] == 'local'
        assert (await _fetch_row(db_path, 'cluster_k'))[1] == 'cluster'
        assert (await _fetch_row(db_path, 'global_k'))[1] == 'global'
    finally:
        await engine.stop()


async def test_flush_once_preserves_ttl(tmp_path):
    """TTL-bearing entries write a non-null ``expires_at_ms``; ttl=None
    writes NULL."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('ttl_k', 'v', ttl=60.0)  # type: ignore[reportPrivateUsage]
        engine._cache.set('no_ttl_k', 'v')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        ttl_row = await _fetch_row(db_path, 'ttl_k')
        no_ttl_row = await _fetch_row(db_path, 'no_ttl_k')
        assert ttl_row is not None
        assert no_ttl_row is not None
        assert ttl_row[6] is not None  # expires_at_ms
        assert no_ttl_row[6] is None
    finally:
        await engine.stop()


# --- DELETE path -----------------------------------------------------------


async def test_flush_once_deletes_existing_row(tmp_path):
    """A ``Op.DELETE`` dirty op removes a previously flushed row."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.set('k', 'v')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert (await _fetch_row(db_path, 'k')) is not None

        engine._cache.delete('k')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        assert (await _fetch_row(db_path, 'k')) is None
    finally:
        await engine.stop()


async def test_flush_once_delete_nonexistent_is_harmless(tmp_path):
    """Deleting a key that never made it to the DB is a no-op at the row
    level — ``DELETE FROM ... WHERE key=?`` is safely 0-row."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        engine._cache.delete('never_set')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # no crash, no row added
        db_path = tmp_path / 'w1-cache.db.actual'
        assert await _count_rows(db_path) == 0
    finally:
        await engine.stop()


# --- batching + atomic-swap ------------------------------------------------


async def test_flush_once_batches_mixed_ops_in_one_call(tmp_path):
    """N sets + M deletes produce exactly N rows added and M rows removed
    in a single flush, and the dirty map is empty afterward."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # Pre-populate a row we can delete
        engine._cache.set('delete_me', 'bye')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Mix: 3 sets + 1 delete
        engine._cache.set('a', 1)  # type: ignore[reportPrivateUsage]
        engine._cache.set('b', 2)  # type: ignore[reportPrivateUsage]
        engine._cache.set('c', 3)  # type: ignore[reportPrivateUsage]
        engine._cache.delete('delete_me')  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        # 3 new rows present, delete_me gone
        assert await _count_rows(db_path) == 3
        assert (await _fetch_row(db_path, 'delete_me')) is None
        assert (await _fetch_row(db_path, 'a')) is not None
        assert (await _fetch_row(db_path, 'b')) is not None
        assert (await _fetch_row(db_path, 'c')) is not None
        assert engine._cache._dirty == {}  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_atomic_dirty_swap_isolates_mid_flush_writes(tmp_path):
    """Writes landing on ``_dirty`` during a flush go to a fresh dict and
    are picked up on the next cycle — they are not dropped.

    We simulate "write during flush" by patching the writer's ``executemany``
    so that when it runs, we synchronously mutate ``_cache._dirty`` (just
    like a racing ``set`` would under the GIL). Without the atomic swap
    pattern, that mutation would be observed inside the flush loop and
    either double-flushed or swallowed.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        # Initial set — this will be the swapped snapshot when flush runs
        cache.set('first', 'v1')

        # Spy wraps executemany; during its execution we inject a fresh
        # dirty entry to mimic a racing `set`.
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        original_executemany = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]
        injected: list[str] = []

        async def spy_executemany(sql, params):
            # After the atomic swap, _dirty is the fresh empty dict — any
            # mutation here should land there, not in the snapshot.
            if 'first' not in injected:
                injected.append('done')
                cache.set('during_flush', 'injected')
            return await original_executemany(sql, params)

        engine._writer_db.executemany = spy_executemany  # type: ignore[reportPrivateUsage,assignment]

        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # restore to avoid leaking patched method
        engine._writer_db.executemany = original_executemany  # type: ignore[reportPrivateUsage,assignment]

        db_path = tmp_path / 'w1-cache.db.actual'
        # 'first' is in DB (was in the snapshot)
        assert (await _fetch_row(db_path, 'first')) is not None
        # 'during_flush' is NOT yet in DB — it went into the new dirty map
        assert (await _fetch_row(db_path, 'during_flush')) is None
        # but it IS in the cache's current dirty map
        assert 'during_flush' in cache._dirty  # type: ignore[reportPrivateUsage]

        # next flush picks it up
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        assert (await _fetch_row(db_path, 'during_flush')) is not None
    finally:
        await engine.stop()


# --- LWW semantics ---------------------------------------------------------


async def test_lww_rejects_older_updated_at_ms(tmp_path):
    """A flush whose ``CacheEntry.updated_at_ms`` is older than the DB's
    row is rejected by the LWW UPSERT — existing row is unchanged."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # Pre-populate with a newer timestamp by bypassing the cache's
        # _now_ms and going direct. We inject a DirtyOp with a specific
        # updated_at_ms so the flush path carries that exact value.
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        newer = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"newer"',
            size_bytes=len('"newer"'),
            created_at_ms=1000,
            updated_at_ms=2000,
            expires_at_ms=None,
            origin_worker_id='w1',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=newer)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Now stage an older update (smaller updated_at_ms) and flush again
        older = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"older"',
            size_bytes=len('"older"'),
            created_at_ms=500,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w1',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=older)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        # newer value still in DB — LWW rejected the older one
        assert row[2] == '"newer"'
        assert row[5] == 2000
    finally:
        await engine.stop()


async def test_lww_tiebreak_prefers_smaller_origin_worker_id(tmp_path):
    """When two entries share ``updated_at_ms``, the one with the
    lexicographically smaller ``origin_worker_id`` wins."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        # First insert: origin 'w2' at ts=1000
        first = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w2"',
            size_bytes=len('"from_w2"'),
            created_at_ms=1000,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w2',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=first)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Second: same ts=1000, origin 'w1' (lex smaller) — should win
        second = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w1"',
            size_bytes=len('"from_w1"'),
            created_at_ms=1000,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w1',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=second)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        # 'w1' < 'w2' lexicographically — w1's value wins
        assert row[2] == '"from_w1"'
        assert row[7] == 'w1'
    finally:
        await engine.stop()


async def test_lww_tiebreak_larger_origin_rejected(tmp_path):
    """Reverse-case of the tiebreak: equal ts, larger origin_worker_id
    loses — the DB row stays unchanged."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        first = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w1"',
            size_bytes=len('"from_w1"'),
            created_at_ms=1000,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w1',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=first)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Higher lex origin — should be rejected
        second = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w2"',
            size_bytes=len('"from_w2"'),
            created_at_ms=1000,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w2',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=second)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        assert row[2] == '"from_w1"'  # unchanged
        assert row[7] == 'w1'
    finally:
        await engine.stop()


async def test_lww_newer_updated_at_wins_regardless_of_origin(tmp_path):
    """Newer ``updated_at_ms`` wins even if the incoming origin_worker_id
    is lexicographically larger — the origin tiebreak only applies when
    timestamps are equal."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        first = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w1_older"',
            size_bytes=len('"from_w1_older"'),
            created_at_ms=1000,
            updated_at_ms=1000,
            expires_at_ms=None,
            origin_worker_id='w1',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=first)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # newer timestamp, larger origin id — still wins
        newer = CacheEntry(
            key='k',
            scope=CacheScope.LOCAL,
            value='"from_w2_newer"',
            size_bytes=len('"from_w2_newer"'),
            created_at_ms=1000,
            updated_at_ms=3000,
            expires_at_ms=None,
            origin_worker_id='w2',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=newer)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        db_path = tmp_path / 'w1-cache.db.actual'
        row = await _fetch_row(db_path, 'k')
        assert row is not None
        assert row[2] == '"from_w2_newer"'
        assert row[5] == 3000
    finally:
        await engine.stop()


# --- empty dirty no-op -----------------------------------------------------


async def test_flush_once_empty_dirty_is_noop(tmp_path):
    """An empty dirty map must not execute SQL — no count, no metric tick.

    We spy on executemany; a call count of zero proves the flush short-
    circuited before touching the writer connection.
    """
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        assert engine._cache._dirty == {}  # type: ignore[reportPrivateUsage]
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]

        original = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]
        call_count = 0

        async def spy(sql, params):
            nonlocal call_count
            call_count += 1
            return await original(sql, params)

        engine._writer_db.executemany = spy  # type: ignore[reportPrivateUsage,assignment]

        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        engine._writer_db.executemany = original  # type: ignore[reportPrivateUsage,assignment]

        assert call_count == 0
    finally:
        await engine.stop()


# --- disabled engine -------------------------------------------------------


async def test_flush_once_on_disabled_engine_is_noop(tmp_path):
    """When the engine has no writer connection (disabled), flush must be
    a no-op — not raise, not touch anything."""
    engine = CacheEngine(
        config=CacheConfig(enabled=False),
        debug_config=make_debug_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    cache = Cache(origin_worker_id='w1')
    cache.set('k', 'v')
    engine.attach_cache(cache)
    # start() is a no-op because config.enabled is False
    await engine.start()
    # flush must not raise — dirty map is preserved because nothing was done
    await engine._flush_once()  # type: ignore[reportPrivateUsage]
    # dirty unchanged
    assert 'k' in cache._dirty  # type: ignore[reportPrivateUsage]
    await engine.stop()


# --- metrics ---------------------------------------------------------------


def _get_counter_value(metric, **labels) -> float:
    """Read the current value from a prometheus counter.

    prometheus_client exposes ``._value.get()`` on each labelled child;
    we fetch it via labels(...) so the test reads the exact sample we
    want.
    """
    return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]


async def test_flush_counter_increments_per_op_type(tmp_path):
    """``drakkar_cache_flush_entries_total{op="set"|"delete"}`` is bumped by
    the number of rows of each op type in the flush."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        # baseline snapshots
        set_before = _get_counter_value(metrics.cache_flush_entries, op='set')
        delete_before = _get_counter_value(metrics.cache_flush_entries, op='delete')

        cache = engine._cache  # type: ignore[reportPrivateUsage]
        # 2 sets + 1 delete (delete is against a not-yet-existing row,
        # but the counter measures op intent, not row-affected count)
        cache.set('a', 1)
        cache.set('b', 2)
        cache.delete('c')
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        set_after = _get_counter_value(metrics.cache_flush_entries, op='set')
        delete_after = _get_counter_value(metrics.cache_flush_entries, op='delete')
        assert set_after - set_before == pytest.approx(2)
        assert delete_after - delete_before == pytest.approx(1)
    finally:
        await engine.stop()


async def test_flush_counter_not_incremented_on_empty_dirty(tmp_path):
    """Empty dirty → no counter ticks."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        set_before = _get_counter_value(metrics.cache_flush_entries, op='set')
        delete_before = _get_counter_value(metrics.cache_flush_entries, op='delete')

        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        set_after = _get_counter_value(metrics.cache_flush_entries, op='set')
        delete_after = _get_counter_value(metrics.cache_flush_entries, op='delete')
        assert set_after == set_before
        assert delete_after == delete_before
    finally:
        await engine.stop()


# --- SQL constant sanity ---------------------------------------------------


def test_lww_upsert_sql_has_conflict_guard():
    """``LWW_UPSERT_SQL`` must contain the LWW conflict guard — the flush
    code uses the constant verbatim, so any regression in the SQL would
    silently break the cross-worker sync path (Task 12) that reuses it."""
    sql = LWW_UPSERT_SQL.lower()
    assert 'on conflict(key) do update' in sql
    # the WHERE clause with the (newer ts) OR (equal ts AND smaller origin) guard
    assert 'excluded.updated_at_ms > cache_entries.updated_at_ms' in sql
    assert 'excluded.origin_worker_id < cache_entries.origin_worker_id' in sql
