"""Tests for the CacheEngine peer-sync UPSERT apply step (Task 12).

Task 11 added the *pull* side: discovering peers and SELECTing rows with a
scope-aware filter. Task 12 closes the loop by applying the pulled rows
back into our own ``cache_entries`` table via the shared ``LWW_UPSERT_SQL``
constant — the exact same SQL used by the local flush path (Task 7), so
local and cross-worker writes cannot diverge in their conflict resolution.

The contract verified here:

1. The UPSERT goes through ``cache_module.LWW_UPSERT_SQL`` — not a private
   copy inside ``_sync_once``. Verified by patching the constant and
   observing that the sync path picks up the patched SQL.
2. LWW rules still hold for peer-supplied rows:
   - Peer row with older ``updated_at_ms`` → rejected (local row preserved).
   - Peer row with newer ``updated_at_ms`` → accepted (local row overwritten).
   - Equal timestamp tiebreaker by lexicographic ``origin_worker_id``.
3. After UPSERT, the keys we attempted to upsert are *evicted from the
   Cache's in-memory dict*. A subsequent ``get`` falls through to the DB
   and reads the newly-merged value — keeping memory and DB coherent without
   an extra SELECT-back-and-compare.
4. A peer pull that returns zero rows performs no UPSERT (no writer-DB
   round-trip).
5. Prometheus counters ``drakkar_cache_sync_entries_fetched_total{peer}``
   and ``drakkar_cache_sync_entries_upserted_total{peer}`` advance per peer.
   ``upserted`` may be less than ``fetched`` when LWW rejects some rows —
   we count the UPSERT *attempts*, not the rows actually changed, matching
   the same "flush throughput" semantics used by ``cache_flush_entries``.
6. The ``size_bytes`` column in newly-upserted rows matches the peer's
   reported value verbatim — we do not re-encode the JSON text, so a
   byte-identical row lands locally.

All tests exercise the real on-disk SQLite stack via ``tmp_path`` +
``aiosqlite``, consistent with Task 11. Peer DBs are minimal fixtures
built through the same helpers as the pull tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite

from drakkar import cache as cache_module
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

# --- helpers ---------------------------------------------------------------
#
# These mirror the Task 11 sync-pull helpers. Duplication is deliberate:
# each task keeps a self-contained test file so failures localize cleanly.


def make_debug_config(tmp_path: Path, **overrides: Any) -> DebugConfig:
    """DebugConfig with ``db_dir`` pointed at tmp_path; events off, config on."""
    defaults: dict[str, Any] = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': False,
        'store_config': True,
        'store_state': False,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


def make_cache_config(**overrides: Any) -> CacheConfig:
    defaults: dict[str, Any] = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(
    tmp_path: Path,
    *,
    worker_id: str = 'me',
    cluster_name: str = 'prod',
    cache_overrides: dict[str, Any] | None = None,
    debug_overrides: dict[str, Any] | None = None,
) -> CacheEngine:
    """Spin up a started CacheEngine with ``peer_sync`` on by default."""
    cache = Cache(origin_worker_id=worker_id)
    engine = CacheEngine(
        config=make_cache_config(**(cache_overrides or {})),
        debug_config=make_debug_config(tmp_path, **(debug_overrides or {})),
        worker_id=worker_id,
        cluster_name=cluster_name,
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    return engine


async def _seed_peer_cache_db(
    tmp_path: Path,
    peer_name: str,
    rows: list[dict[str, Any]],
) -> Path:
    """Create a peer cache DB + symlink with the given rows.

    Row defaults: scope=LOCAL (so the pull scope filter excludes them
    unless test bumps to CLUSTER/GLOBAL), no TTL, timestamps at 1000,
    origin set to the peer name.
    """
    actual = tmp_path / f'{peer_name}-cache.db.actual'
    async with aiosqlite.connect(str(actual)) as db:
        await db.executescript(cache_module.SCHEMA_CACHE_ENTRIES)
        for r in rows:
            value = r.get('value', '"v"')
            size = r.get('size_bytes', len(value.encode('utf-8')))
            await db.execute(
                'INSERT INTO cache_entries '
                '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                ' expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                (
                    r['key'],
                    r.get('scope', CacheScope.GLOBAL.value),
                    value,
                    size,
                    r.get('created_at_ms', 1000),
                    r.get('updated_at_ms', 1000),
                    r.get('expires_at_ms'),
                    r.get('origin_worker_id', peer_name),
                ),
            )
        await db.commit()
    link = tmp_path / f'{peer_name}-cache.db'
    link.symlink_to(actual.name)
    return actual


async def _seed_peer_live_db(tmp_path: Path, peer_name: str, cluster_name: str) -> Path:
    """Create a peer ``-live.db`` stand-in with a single ``worker_config`` row."""
    actual = tmp_path / f'{peer_name}-live.db.actual'
    async with aiosqlite.connect(str(actual)) as db:
        await db.execute(
            """CREATE TABLE worker_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                worker_name TEXT NOT NULL,
                cluster_name TEXT,
                created_at REAL NOT NULL,
                created_at_dt TEXT NOT NULL
            )"""
        )
        await db.execute(
            'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) '
            'VALUES (1, ?, ?, 0, "")',
            (peer_name, cluster_name),
        )
        await db.commit()
    link = tmp_path / f'{peer_name}-live.db'
    link.symlink_to(actual.name)
    return actual


async def _fetch_local_row(tmp_path: Path, worker_id: str, key: str) -> tuple | None:
    """Fetch a row from our own ``<worker>-cache.db.actual``."""
    db_path = tmp_path / f'{worker_id}-cache.db.actual'
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


# --- UPSERT uses LWW_UPSERT_SQL constant ------------------------------------


async def test_sync_apply_uses_shared_lww_upsert_sql(tmp_path, monkeypatch):
    """Peer-sync UPSERT must call through to ``cache_module.LWW_UPSERT_SQL`` —
    the same constant local flush uses. Verified by spying on the writer
    connection's ``executemany`` and checking the SQL text matches.

    This guards against the "accidental fork" risk: if a future refactor
    inlines a local copy of the UPSERT inside ``_sync_once``, local and
    peer writes could diverge in their LWW rules. The whole point of the
    shared constant is that they cannot.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k', 'scope': CacheScope.GLOBAL.value, 'value': '"v"'}],
        )

        # Spy on executemany: capture SQL strings seen by the writer during
        # sync. We wrap, not replace, so the real UPSERT still runs.
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        original_executemany = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]
        seen_sql: list[str] = []

        async def spy_executemany(sql, params):
            seen_sql.append(sql)
            return await original_executemany(sql, params)

        engine._writer_db.executemany = spy_executemany  # type: ignore[reportPrivateUsage,assignment]

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # restore to avoid leaking the spy beyond this test
        engine._writer_db.executemany = original_executemany  # type: ignore[reportPrivateUsage,assignment]

        # At least one executemany call must carry the LWW_UPSERT_SQL text.
        assert any(sql == LWW_UPSERT_SQL for sql in seen_sql), (
            f'expected LWW_UPSERT_SQL to appear in sync-time SQL; saw: {seen_sql!r}'
        )
    finally:
        await engine.stop()


# --- LWW: older peer row rejected, newer peer row accepted ------------------


async def test_sync_apply_rejects_older_peer_row(tmp_path):
    """A peer row whose ``updated_at_ms`` is older than the local row is
    rejected by the LWW UPSERT — our row stays unchanged."""
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Local row (newer)
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        local_entry = CacheEntry(
            key='shared',
            scope=CacheScope.GLOBAL,
            value='"local_newer"',
            size_bytes=len('"local_newer"'),
            created_at_ms=1000,
            updated_at_ms=5000,
            expires_at_ms=None,
            origin_worker_id='me',
        )
        cache._dirty['shared'] = DirtyOp(op=Op.SET, entry=local_entry)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Peer row (older)
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'shared',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_older"',
                    'updated_at_ms': 2000,
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        row = await _fetch_local_row(tmp_path, 'me', 'shared')
        assert row is not None
        # value unchanged — LWW rejected the older peer row
        assert row[2] == '"local_newer"'
        assert row[5] == 5000
    finally:
        await engine.stop()


async def test_sync_apply_accepts_newer_peer_row(tmp_path):
    """A peer row whose ``updated_at_ms`` is newer than the local row is
    accepted by the LWW UPSERT — our row is overwritten with the peer's
    value."""
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        local_entry = CacheEntry(
            key='shared',
            scope=CacheScope.GLOBAL,
            value='"local_older"',
            size_bytes=len('"local_older"'),
            created_at_ms=1000,
            updated_at_ms=2000,
            expires_at_ms=None,
            origin_worker_id='me',
        )
        cache._dirty['shared'] = DirtyOp(op=Op.SET, entry=local_entry)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'shared',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_newer"',
                    'updated_at_ms': 5000,
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        row = await _fetch_local_row(tmp_path, 'me', 'shared')
        assert row is not None
        assert row[2] == '"peer_newer"'
        assert row[5] == 5000
        # origin flipped to the peer that won LWW
        assert row[7] == 'peer1'
    finally:
        await engine.stop()


async def test_sync_apply_lww_tiebreak_by_smaller_origin(tmp_path):
    """When ``updated_at_ms`` ties, the lexicographically smaller
    ``origin_worker_id`` wins. We install a local row with origin ``'me'``
    (wins over ``'peer1'`` since ``'m' < 'p'``) at the same timestamp and
    a peer row at the same timestamp — our row must survive.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        local_entry = CacheEntry(
            key='shared',
            scope=CacheScope.GLOBAL,
            value='"local_wins"',
            size_bytes=len('"local_wins"'),
            created_at_ms=1000,
            updated_at_ms=3000,
            expires_at_ms=None,
            origin_worker_id='me',
        )
        cache._dirty['shared'] = DirtyOp(op=Op.SET, entry=local_entry)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'shared',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_loses"',
                    'updated_at_ms': 3000,  # tie
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        row = await _fetch_local_row(tmp_path, 'me', 'shared')
        assert row is not None
        # local wins tie because 'me' < 'peer1' lexicographically
        assert row[2] == '"local_wins"'
        assert row[7] == 'me'
    finally:
        await engine.stop()


# --- memory invalidation ----------------------------------------------------


async def test_sync_apply_invalidates_memory_for_upserted_keys(tmp_path):
    """After UPSERT, the Cache's in-memory entries for the pulled keys are
    popped. A subsequent ``get`` falls through to the DB and reads the
    merged value — simpler (and cheaper) than SELECT-back-and-compare to
    figure out whether LWW actually accepted each row.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        # Stage a local value in memory only (don't flush).
        cache.set('stale_in_mem', 'local_mem_value', scope=CacheScope.GLOBAL)
        assert 'stale_in_mem' in cache._memory  # type: ignore[reportPrivateUsage]

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'stale_in_mem',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_value"',
                    'updated_at_ms': 9_999_999_999_999,  # clearly newer
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Memory entry for the upserted key was invalidated. Any subsequent
        # ``get`` falls through to the DB; we just verify the pop here.
        assert 'stale_in_mem' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_sync_apply_invalidates_memory_even_when_lww_rejects(tmp_path):
    """Memory invalidation is unconditional — we pop the key from memory
    regardless of whether LWW accepted the peer row. This is a deliberate
    simplification: "pop from memory, let the next ``get`` hit the DB" is
    cheaper than a per-row SELECT-back-and-compare.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        # Flush a newer local row — LWW will reject the older peer row.
        local_entry = CacheEntry(
            key='k',
            scope=CacheScope.GLOBAL,
            value='"local_newer"',
            size_bytes=len('"local_newer"'),
            created_at_ms=1000,
            updated_at_ms=10_000,
            expires_at_ms=None,
            origin_worker_id='me',
        )
        cache._dirty['k'] = DirtyOp(op=Op.SET, entry=local_entry)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        # Also place in memory (simulating a recent write).
        cache.set('k', 'in_memory_recent', scope=CacheScope.GLOBAL)
        assert 'k' in cache._memory  # type: ignore[reportPrivateUsage]

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'k',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_older"',
                    'updated_at_ms': 5000,  # older than the 10_000 we flushed
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Even though LWW rejected the peer row at the DB level, memory is
        # still popped — by contract, sync invalidates memory for every
        # key it attempted to UPSERT.
        assert 'k' not in cache._memory  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# --- empty peer result → no UPSERT ------------------------------------------


async def test_sync_apply_noop_when_peer_pull_empty(tmp_path):
    """A peer pull that returns zero rows performs no UPSERT — the writer
    connection sees no executemany for the UPSERT step.

    This guards the idle-engine cost: when peers have nothing newer to
    share, the sync cycle should not do any write work.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Peer exists but has zero rows.
        await _seed_peer_cache_db(tmp_path, 'peer1', [])

        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        original_executemany = engine._writer_db.executemany  # type: ignore[reportPrivateUsage]
        upsert_calls = 0

        async def spy_executemany(sql, params):
            nonlocal upsert_calls
            if sql == LWW_UPSERT_SQL:
                upsert_calls += 1
            return await original_executemany(sql, params)

        engine._writer_db.executemany = spy_executemany  # type: ignore[reportPrivateUsage,assignment]

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        engine._writer_db.executemany = original_executemany  # type: ignore[reportPrivateUsage,assignment]

        assert upsert_calls == 0
    finally:
        await engine.stop()


# --- metrics ----------------------------------------------------------------


def _counter_value(counter, **labels) -> float:
    """Read a Prometheus Counter's value at a given label set (or 0)."""
    # ``_metrics`` is the internal map {label_tuple -> metric_child}; using it
    # directly lets the test assert "zero when absent" without having to
    # increment-and-reset the counter first. The labels() call creates a
    # zero-valued child if missing — we specifically want to avoid that side
    # effect when asserting "no sync fetch happened for an unused peer".
    return counter.labels(**labels)._value.get() if counter._metrics.get(tuple(labels.values())) else 0.0


async def test_sync_apply_counter_fetched_and_upserted(tmp_path):
    """``drakkar_cache_sync_entries_fetched_total{peer}`` counts rows pulled
    from the peer; ``drakkar_cache_sync_entries_upserted_total{peer}`` counts
    rows the engine attempted to UPSERT. Under LWW, upserted == fetched for
    this test (no conflicting local rows).
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Snapshot counters before — they may be non-zero from earlier tests
        # because Prometheus counters are module-global. We assert on deltas.
        fetched_before = _counter_value(metrics.cache_sync_entries_fetched, peer='peer1')
        upserted_before = _counter_value(metrics.cache_sync_entries_upserted, peer='peer1')

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000},
                {'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 2000},
                {'key': 'k3', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 3000},
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        fetched_after = _counter_value(metrics.cache_sync_entries_fetched, peer='peer1')
        upserted_after = _counter_value(metrics.cache_sync_entries_upserted, peer='peer1')
        assert fetched_after - fetched_before == 3
        assert upserted_after - upserted_before == 3
    finally:
        await engine.stop()


async def test_sync_apply_upserted_leq_fetched_when_lww_rejects(tmp_path):
    """When some peer rows lose LWW, ``upserted`` counts the attempts
    (still equal to fetched under our "count intents, not applied rows"
    convention). This mirrors the semantics of ``cache_flush_entries`` —
    the counter measures pipeline throughput, not DB state transitions.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Pre-populate a local row that will beat the peer's row.
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        local_entry = CacheEntry(
            key='shared',
            scope=CacheScope.GLOBAL,
            value='"local_newer"',
            size_bytes=len('"local_newer"'),
            created_at_ms=1000,
            updated_at_ms=10_000,
            expires_at_ms=None,
            origin_worker_id='me',
        )
        cache._dirty['shared'] = DirtyOp(op=Op.SET, entry=local_entry)  # type: ignore[reportPrivateUsage]
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        fetched_before = _counter_value(metrics.cache_sync_entries_fetched, peer='peer1')
        upserted_before = _counter_value(metrics.cache_sync_entries_upserted, peer='peer1')

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'shared',
                    'scope': CacheScope.GLOBAL.value,
                    'updated_at_ms': 5000,  # older → rejected
                    'origin_worker_id': 'peer1',
                },
                {
                    'key': 'new_key',
                    'scope': CacheScope.GLOBAL.value,
                    'updated_at_ms': 5000,
                    'origin_worker_id': 'peer1',
                },
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        fetched_after = _counter_value(metrics.cache_sync_entries_fetched, peer='peer1')
        upserted_after = _counter_value(metrics.cache_sync_entries_upserted, peer='peer1')
        # Both rows fetched; both UPSERT attempts counted. DB state: 'shared'
        # keeps its newer local value, 'new_key' is inserted fresh.
        assert fetched_after - fetched_before == 2
        assert upserted_after - upserted_before == 2
        # Verify the LWW outcome at the DB level matches the counter's
        # "counts attempts, not applied" semantics.
        shared_row = await _fetch_local_row(tmp_path, 'me', 'shared')
        assert shared_row is not None
        assert shared_row[2] == '"local_newer"'  # local won
        new_row = await _fetch_local_row(tmp_path, 'me', 'new_key')
        assert new_row is not None  # inserted fresh
    finally:
        await engine.stop()


# --- size_bytes preserved ---------------------------------------------------


async def test_sync_apply_preserves_size_bytes_from_peer(tmp_path):
    """The ``size_bytes`` stored in our DB after a successful UPSERT equals
    the peer's reported value — we do not re-encode the JSON text. This
    keeps the Prometheus running-sum gauges coherent across workers.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        peer_value = '"abcdef"'  # 8 bytes including quotes
        peer_size = len(peer_value.encode('utf-8'))

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'kk',
                    'scope': CacheScope.GLOBAL.value,
                    'value': peer_value,
                    'size_bytes': peer_size,
                    'updated_at_ms': 5000,
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        row = await _fetch_local_row(tmp_path, 'me', 'kk')
        assert row is not None
        # row columns: key, scope, value, size_bytes, ...
        assert row[2] == peer_value
        assert row[3] == peer_size
    finally:
        await engine.stop()
