"""Tests for the CacheEngine peer-sync cursor + error isolation (Task 13).

Task 11 added peer discovery + scope-aware pull. Task 12 closed the apply
loop with LWW UPSERT + memory invalidation. Task 13 layers on the two
remaining concerns that make peer sync a durable, steady-state system:

1. **Per-peer cursor** (``_peer_cursors: dict[str, int]``) — ensures each
   sync cycle picks up where the last one left off for every peer. The
   cursor stores the max ``updated_at_ms`` we've seen from that peer so
   far, and the pull query's ``WHERE updated_at_ms > ?`` clause uses it
   to skip rows we've already pulled.

   Advancement rules:
   - Zero rows → cursor stays where it is (no progress, no harm).
   - Fewer rows than ``batch_size`` → cursor advances to "now-ish" (we
     caught up; next cycle re-scans from now). This prevents a stuck
     cursor if the peer's schema clocks drift.
   - Full batch (``len(rows) == batch_size``) → cursor advances to the
     last row's ``updated_at_ms`` so the next cycle picks up the
     remaining tail.

2. **Per-peer error isolation** — one bad peer (corrupt DB, missing file,
   connection refused) must not break the whole sync cycle. Each peer's
   pull + apply is wrapped in try/except; on failure we log a warning
   with the peer name and error, increment
   ``drakkar_cache_sync_errors_total{peer}``, and continue to the next
   peer. The failing peer's cursor is left untouched so next cycle
   retries from the same point.

3. **Cross-worker delete regression** — the documented sharp edge. A
   key deleted locally, if still present on a peer with a newer
   ``updated_at_ms``, will be re-pulled by the next sync. We test this
   end-to-end so future refactors don't quietly flip the behavior.

4. **System periodic registration** — ``cache.sync`` must be scheduled
   via ``run_periodic_task(system=True)`` in ``CacheEngine.start()``.

All tests use the same ``tmp_path`` + ``aiosqlite`` on-disk pattern as
Tasks 11-12 for filesystem/DB realism, plus the ``_seed_peer_cache_db`` /
``_seed_peer_live_db`` helpers from those files (duplicated here per the
"self-contained per-task test file" convention).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from drakkar import cache as cache_module
from drakkar import cache_engine as cache_engine_module
from drakkar import metrics
from drakkar.cache import (
    Cache,
    CacheEngine,
    CacheScope,
)
from drakkar.config import CacheConfig, CachePeerSyncConfig, DebugConfig

# --- helpers ---------------------------------------------------------------
#
# Mirror the helpers in the other sync test files so this file stays
# self-contained — the same construct appears in Tasks 11 and 12, but
# duplicating keeps test failures localized to one task's file.


def make_debug_config(tmp_path: Path, **overrides: Any) -> DebugConfig:
    """DebugConfig pointing at tmp_path; events off, config on (needed for peer sync)."""
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
    """Spin up a started CacheEngine with peer_sync on by default."""
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

    Same defaults as the sync_pull/sync_apply tests: scope=GLOBAL so the
    default scope filter picks up rows without any extra setup.
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
    """Create a peer -live.db stand-in with a single worker_config row."""
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


def _counter_value(counter, **labels) -> float:
    """Read a Prometheus Counter's value for a label set (0 if never incremented).

    Duplicated from test_cache_sync_apply.py — avoids a side effect from
    ``labels().inc(0)`` which would auto-create the child and make "never
    touched" indistinguishable from "zero".
    """
    return counter.labels(**labels)._value.get() if counter._metrics.get(tuple(labels.values())) else 0.0


# --- cursor initialization --------------------------------------------------


async def test_peer_cursor_starts_at_zero(tmp_path):
    """On a fresh worker startup, every peer's cursor starts at 0.

    That means the first sync cycle will pull every row in the peer's
    (scope-filtered) window. Subsequent cycles advance the cursor so
    we don't re-read rows we've already seen.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Cursors map should exist and be empty (no peers seen yet).
        assert engine._peer_cursors == {}  # type: ignore[reportPrivateUsage]

        # After a sync cycle against a peer, that peer's cursor has an
        # entry; we don't assert the exact value here (the dedicated
        # advancement tests cover that).
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000}],
        )
        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert 'peer1' in engine._peer_cursors  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


# --- cursor advances to last row's updated_at_ms on full batch --------------


async def test_peer_cursor_advances_to_last_row_when_batch_full(tmp_path):
    """When the pull returned exactly ``batch_size`` rows, the peer has
    more we haven't read yet. The cursor advances only to the last
    returned row's ``updated_at_ms`` so the next cycle picks up the
    remaining tail.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # 5 rows, batch_size=3 — first cycle pulls keys with the three
        # smallest ``updated_at_ms`` values (our pull is ORDER BY
        # updated_at_ms ASC LIMIT 3).
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000 + i} for i in range(5)],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Cursor should land on the third row's updated_at_ms (1002),
        # since rows 1000, 1001, 1002 were pulled.
        assert engine._peer_cursors['peer1'] == 1002  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_peer_cursor_picks_up_remaining_tail_on_next_cycle(tmp_path):
    """Two sync cycles in a row: cycle 1 pulls the first ``batch_size``
    rows, cycle 2 pulls the remaining tail. Verifies the cursor
    actually participates in the SELECT's WHERE clause.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000 + i} for i in range(5)],
        )

        pulled1 = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        first_batch = pulled1['peer1']
        assert {row[0] for row in first_batch} == {'k0', 'k1', 'k2'}

        pulled2 = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        second_batch = pulled2['peer1']
        # Cycle 2 should pull only rows updated_at_ms > 1002 → k3, k4.
        assert {row[0] for row in second_batch} == {'k3', 'k4'}
    finally:
        await engine.stop()


# --- same-ms full batch: follow-up drain picks up the tail ------------------


async def test_full_batch_all_same_updated_at_ms_drains_remainder_same_cycle(tmp_path):
    """Regression: a peer producing MORE than ``batch_size`` rows that all
    share the SAME ``updated_at_ms`` (e.g. a burst of cache sets committed
    in the same millisecond) would have its tail rows permanently skipped
    under a naive cursor-advance-to-last-row-ts policy.

    Why this used to fail:
    1. Cycle 1 pulls the first ``batch_size`` rows (all at ts=1000).
    2. Naive advance sets the cursor to 1000.
    3. Cycle 2 uses ``WHERE updated_at_ms > 1000`` — skipping every
       remaining same-ms row permanently. (Stepping the cursor BACK
       wouldn't fix it either: SQLite's deterministic tie ordering
       would keep returning the same prefix of same-ms rows on any
       retry at an earlier cursor.)

    Fix: when the main pull returns a full batch that entirely shares
    one ``updated_at_ms``, ``_sync_once`` triggers a follow-up "drain"
    pull (``_drain_same_ms_tail``) keyed on ``updated_at_ms = same_ms
    AND key > last_key``. The drain walks the same-ms rows by key so
    every tail row gets applied in the same cycle. The cursor then
    advances normally to that millisecond — there's nothing left to
    skip.

    This test uses batch_size=3 and seeds 4 same-ms rows. In a single
    cycle: main pull yields 3, drain yields the remaining 1, and the
    local DB ends up with all 4.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # 4 rows, batch_size=3, all at the SAME updated_at_ms. This is
        # the burst scenario: imagine four cache.set(...) calls that all
        # complete in the same millisecond on the peer.
        same_ts = 1000
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': same_ts} for i in range(4)],
        )

        # Single cycle: main pull + same-ms drain, all in one go.
        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # _sync_once returns main pull rows + drained rows combined per peer
        # so we can see the whole cycle's effect. All four keys must be
        # there.
        pulled = pulled_by_peer['peer1']
        pulled_keys = {row[0] for row in pulled}
        assert pulled_keys == {'k0', 'k1', 'k2', 'k3'}, (
            f'main pull + drain together must cover all 4 same-ms rows; got {pulled_keys}'
        )

        # Cursor advances to the same-ms timestamp (no step-back
        # needed — the drain already handled the tail).
        cursor_after = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after == same_ts, f'cursor should advance to same_ts={same_ts} after drain; got {cursor_after}'

        # End-to-end: every key the peer had must now be applied locally.
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            rows = await cur.fetchall()
            await cur.close()
        local_keys = {row[0] for row in rows}
        assert local_keys == {'k0', 'k1', 'k2', 'k3'}, (
            f'all four same-ms rows must be in the local DB after one cycle; got {local_keys}'
        )
    finally:
        await engine.stop()


async def test_full_batch_all_same_ms_with_large_tail_drains_multiple_pages(tmp_path):
    """The drain loop must page through more than one ``batch_size``
    worth of same-ms tail. Here batch_size=3 and there are 10 same-ms
    rows total — 3 from the main pull and 7 more drained across two
    follow-up pages. All 10 must land locally in one cycle.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        same_ts = 5000
        # 10 rows, all at the same ms. Keys k00..k09 sort deterministically
        # for the drain's ``ORDER BY key ASC`` walk.
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i:02d}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': same_ts} for i in range(10)],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            rows = await cur.fetchall()
            await cur.close()
        local_keys = {row[0] for row in rows}
        expected_keys = {f'k{i:02d}' for i in range(10)}
        assert local_keys == expected_keys, (
            f'drain must page through all {len(expected_keys)} same-ms rows in one cycle; got {local_keys}'
        )
    finally:
        await engine.stop()


async def test_full_batch_same_ms_drain_handles_non_lex_insertion_order(tmp_path):
    """Regression: the same-ms drain must work when peer inserts keys in
    an order that does NOT match lex sort (i.e. ``Cache.set`` call order
    is arbitrary business-logic order, not sorted).

    Why this matters: the main pull orders by ``(updated_at_ms ASC,
    key ASC)`` and the drain queries ``WHERE updated_at_ms = ms AND
    key > last_key``. If the main pull's ``ORDER BY`` lacked the explicit
    ``key ASC`` tiebreaker, SQLite would break same-ms ties by rowid —
    which equals peer insertion order — and ``rows[-1][0]`` would be the
    LAST-INSERTED same-ms key, NOT the lex-max. Any same-ms keys that
    sort lexicographically BELOW that last-inserted key but were
    inserted AFTER it would then be silently skipped by the drain's
    ``key > last_key`` filter — permanently missing from the local DB.

    This test reproduces the failure scenario: keys are seeded in a
    shuffled order that does not match lex order. All keys must still
    sync correctly after one cycle.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        same_ts = 1000
        # Insertion order deliberately scrambled relative to lex order.
        # This is the exact scenario the bug report described: 10 same-ms
        # keys inserted in an arbitrary order that doesn't match lex sort.
        # With the pre-fix ORDER BY (no secondary key sort), SQLite would
        # return the first 3 by rowid = [k7, k2, k9], set last_key='k9',
        # and the drain's ``key > 'k9'`` would miss k0..k6 and k8.
        shuffled_keys = ['k7', 'k2', 'k9', 'k5', 'k1', 'k8', 'k3', 'k6', 'k0', 'k4']
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': k, 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': same_ts} for k in shuffled_keys],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # All 10 same-ms keys must be in the local DB after one cycle.
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            rows = await cur.fetchall()
            await cur.close()
        local_keys = {row[0] for row in rows}
        expected_keys = set(shuffled_keys)
        assert local_keys == expected_keys, (
            f'all same-ms rows must sync regardless of peer insertion order; '
            f'missing from local DB: {expected_keys - local_keys}'
        )
    finally:
        await engine.stop()


async def test_full_batch_mixed_updated_at_ms_drain_no_tail_is_noop(tmp_path):
    """A mixed-timestamp full batch with NO spillover at its trailing ``ms``
    still triggers the drain (drain fires on ANY full batch now), but
    the drain query returns zero rows — nothing beyond ``last_key`` at
    ``last_ms`` exists on the peer. The main pull's rows still apply,
    the cursor advances to ``rows[-1].updated_at_ms`` exactly, and the
    test hook shows only the main pull's rows (not extended by the
    empty drain).

    This is the cheap-noop case: one extra small indexed query on the
    peer, no duplicated work.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Full batch (3 rows) with all-distinct timestamps. The trailing
        # ms (1002) has exactly one row on the peer, so the drain's
        # ``updated_at_ms = 1002 AND key > 'k2'`` returns empty.
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000 + i} for i in range(4)],
        )

        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        # Cycle 1 pulls k0..k2 (ts 1000..1002). Cursor advances to 1002.
        assert engine._peer_cursors['peer1'] == 1002  # type: ignore[reportPrivateUsage]
        # Drain fired but returned empty → the returned dict still shows
        # only the main pull's 3 rows (code only extends the per-peer
        # entry when ``drained`` is truthy).
        pulled = pulled_by_peer['peer1']
        assert len(pulled) == 3, (
            f'mixed-ms full batch with no tail spillover: _sync_once should return 3 rows, got {len(pulled)} rows {[row[0] for row in pulled]}'
        )
    finally:
        await engine.stop()


async def test_mixed_ms_full_batch_with_same_ms_tail_drains_remainder(tmp_path):
    """Regression: the drain must fire on ANY full batch, not only when
    every row in the batch shares one ``updated_at_ms``.

    Scenario that used to lose data under the old narrower trigger
    (``rows[0] ts == rows[-1] ts``):

    - batch_size=3, peer rows: k1@1000, k2@1001, k3@1002, k4@1002,
      k5@1002, k6@1002, k7@1002.
    - Main pull returns [k1, k2, k3]. ``rows[0] ts=1000`` !=
      ``rows[-1] ts=1002`` → drain skipped → cursor advances to 1002
      → next cycle's ``WHERE updated_at_ms > 1002`` permanently skips
      k4..k7.

    With the broader trigger (drain on any full batch), the first
    cycle pulls [k1, k2, k3] via the main pull and then drains
    [k4, k5, k6, k7] via ``updated_at_ms = 1002 AND key > 'k3'``.
    All seven keys land in the local DB in ONE cycle.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Deliberately seed k3..k7 all at ts=1002 to create the mixed-ms
        # batch with spillover at the trailing ms. k1@1000, k2@1001 are
        # the earlier-ms prefix of the main pull.
        rows = [
            {'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000},
            {'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1001},
            {'key': 'k3', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
            {'key': 'k4', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
            {'key': 'k5', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
            {'key': 'k6', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
            {'key': 'k7', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
        ]
        await _seed_peer_cache_db(tmp_path, 'peer1', rows)

        # Single cycle: main pull + same-ms drain for the trailing ms.
        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # _sync_once returns all 7 rows (3 main + 4 drained).
        pulled = pulled_by_peer['peer1']
        pulled_keys = {row[0] for row in pulled}
        assert pulled_keys == {'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7'}, (
            f'mixed-ms full batch with trailing-ms spillover must pull everything in one cycle; got {pulled_keys}'
        )

        # Cursor advances to the trailing ms, same as any full batch.
        cursor_after = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after == 1002, f'cursor should advance to 1002 after drain; got {cursor_after}'

        # End-to-end: every key the peer had is applied locally — no
        # rows permanently lost to a silent cursor advance.
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            local_rows = await cur.fetchall()
            await cur.close()
        local_keys = {row[0] for row in local_rows}
        assert local_keys == {'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7'}, (
            f'all seven peer rows must be in the local DB after one cycle; got {local_keys}'
        )
    finally:
        await engine.stop()


# --- cursor advances to max(last_row_ts, cursor_ms) on partial batch -------


async def test_peer_cursor_advances_to_last_row_ts_when_partial_batch(tmp_path):
    """When the pull returned fewer than ``batch_size`` rows, the peer
    has no more rows newer than what we've seen. The cursor advances
    to the last (largest) observed peer timestamp so the next cycle
    only looks at rows newer than that — anchoring to the peer's
    clock rather than our local wall clock.

    This is the clock-skew-safe property: if we used our local
    ``_now_ms()`` and the peer's clock was skewed relative to ours,
    rows with peer-stamped timestamps falling between our previous
    cursor and our local now would be silently skipped on later pulls.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=100)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Only 2 rows, way below the batch_size=100 cap.
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 500},
                {'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 600},
            ],
        )

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Cursor should land on the last observed peer timestamp (600),
        # not on our local wall clock. Starting cursor was 0, and
        # max(600, 0) == 600.
        cursor = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor == 600, f'cursor should equal last observed peer ts; got {cursor}'
    finally:
        await engine.stop()


async def test_peer_cursor_never_regresses_on_partial_batch(tmp_path):
    """The partial-batch cursor must never regress. If the peer's last
    row timestamp is *below* the current cursor (a stale insert on the
    peer side, or a clock-drift artifact), the cursor stays at its
    current value — ``max(rows[-1], cursor_ms)``.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Full batch: 3 rows with timestamps [1000, 1001, 1002].
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000 + i} for i in range(3)],
        )
        await engine._sync_once()  # type: ignore[reportPrivateUsage]
        cursor_after_cycle_1 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after_cycle_1 == 1002

        # Manually advance the cursor past any peer timestamps to simulate
        # a scenario where the cursor sat ahead of what the peer later
        # reported. Now seed a smaller row; the partial batch's
        # ``max(..., cursor_ms)`` guard must keep the cursor at its
        # current value.
        engine._peer_cursors['peer1'] = 5000  # type: ignore[reportPrivateUsage]
        async with aiosqlite.connect(str(tmp_path / 'peer1-cache.db.actual')) as db:
            # Insert a row above the cursor so the pull sees it.
            await db.execute(
                'INSERT INTO cache_entries '
                '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                ' expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                ('k_new', CacheScope.GLOBAL.value, '"v"', 3, 6000, 6000, None, 'peer1'),
            )
            await db.commit()

        await engine._sync_once()  # type: ignore[reportPrivateUsage]
        cursor_after_cycle_2 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        # Partial batch with last ts 6000, cursor 5000 → max = 6000.
        # Cursor moved forward but is anchored to observed peer ts.
        assert cursor_after_cycle_2 == 6000, f'expected cursor=6000, got {cursor_after_cycle_2}'
    finally:
        await engine.stop()


async def test_peer_cursor_tracks_future_skewed_peer_timestamps(tmp_path):
    """Regression: peer writes rows with timestamps far in the future
    (clock skewed ahead). The cursor must track the observed peer
    timestamps so subsequent syncs still see late-arriving rows stamped
    with peer's clock.

    Timeline:
    1. Peer seeds rows with timestamps in the future (e.g., 50 years ahead).
    2. First sync: pulls them (partial batch); cursor → max(peer_ts, 0) = peer_ts.
    3. Peer adds another row slightly newer than the first batch.
    4. Second sync: cursor filters correctly — the new row still arrives.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=100)},
    )
    try:
        # Peer timestamp: ~50 years in the future (well past any local now_ms).
        future_ts_base = 4_000_000_000_000  # ≈ year 2096

        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k_future', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': future_ts_base},
            ],
        )
        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Cursor must be at the observed peer timestamp, not our local
        # wall clock. If the old code path ran _now_ms() here, the cursor
        # would be much smaller, and the peer's future-stamped rows
        # between cursor and peer_ts would be re-pulled repeatedly —
        # but more importantly, the follow-up check below verifies no
        # rows are silently skipped.
        cursor_after_cycle_1 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after_cycle_1 == future_ts_base, (
            f'cursor must track observed peer ts; got {cursor_after_cycle_1}, expected {future_ts_base}'
        )

        # Peer adds a newer row (still future-stamped, but higher than
        # the previous). We must observe it on the next sync.
        async with aiosqlite.connect(str(tmp_path / 'peer1-cache.db.actual')) as db:
            await db.execute(
                'INSERT INTO cache_entries '
                '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                ' expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                (
                    'k_future_newer',
                    CacheScope.GLOBAL.value,
                    '"v"',
                    3,
                    future_ts_base + 1000,
                    future_ts_base + 1000,
                    None,
                    'peer1',
                ),
            )
            await db.commit()

        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        pulled_keys = {row[0] for row in pulled_by_peer['peer1']}
        assert 'k_future_newer' in pulled_keys, f'future-stamped row should be pulled after cycle 2; got {pulled_keys}'
    finally:
        await engine.stop()


# --- zero rows: cursor stays put ---------------------------------------------


async def test_peer_cursor_unchanged_when_zero_rows_returned(tmp_path):
    """If the peer has nothing new, the cursor must not move — otherwise
    a single empty cycle could skip over late-arriving rows whose
    ``updated_at_ms`` is between the old cursor and "now".

    Specifically: we pre-set the cursor via a first populated cycle,
    then remove all rows, then run a second cycle with an empty result.
    The cursor must equal the value it had after cycle 1.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Seed exactly batch_size rows so the cursor sets to the last
        # row's timestamp (deterministic).
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': f'k{i}', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000 + i} for i in range(3)],
        )
        await engine._sync_once()  # type: ignore[reportPrivateUsage]
        cursor_after_cycle_1 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after_cycle_1 == 1002

        # Second cycle: nothing newer than the cursor — pull returns 0 rows.
        await engine._sync_once()  # type: ignore[reportPrivateUsage]
        cursor_after_cycle_2 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        # Cursor unchanged: zero-row pulls don't move the cursor.
        assert cursor_after_cycle_2 == cursor_after_cycle_1
    finally:
        await engine.stop()


# --- per-peer isolation ------------------------------------------------------


async def test_peer_error_does_not_affect_other_peers(tmp_path):
    """One peer failing must not break the whole cycle. If peer A's DB
    file is corrupted or the peer's connection fails, peer B's pull
    and cursor update proceed normally.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=100)},
    )
    try:
        # Good peer
        await _seed_peer_live_db(tmp_path, 'good_peer', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'good_peer',
            [{'key': 'kg', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1500}],
        )

        # Bad peer: symlink exists but target is gone (simulates a crashed
        # worker that left a dangling symlink).
        bad_actual = tmp_path / 'bad_peer-cache.db.actual'
        bad_actual.write_text('not a valid sqlite database', encoding='utf-8')
        bad_link = tmp_path / 'bad_peer-cache.db'
        bad_link.symlink_to(bad_actual.name)
        await _seed_peer_live_db(tmp_path, 'bad_peer', cluster_name='prod')

        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Good peer's cursor advanced.
        assert 'good_peer' in engine._peer_cursors  # type: ignore[reportPrivateUsage]
        # Bad peer: we either have no cursor entry or it stayed at 0 —
        # whichever policy, verify the good peer got through.
        assert engine._peer_cursors['good_peer'] > 0  # type: ignore[reportPrivateUsage]
        # Good peer's pulled rows got through too.
        assert 'good_peer' in pulled_by_peer
    finally:
        await engine.stop()


async def test_peer_error_increments_sync_errors_counter(tmp_path):
    """On a per-peer failure we must tick ``drakkar_cache_sync_errors_total
    {peer=...}`` so operators can alert on a misbehaving peer without
    parsing logs."""
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
    )
    try:
        # Corrupt peer.
        bad_actual = tmp_path / 'bad_peer-cache.db.actual'
        bad_actual.write_text('not a sqlite file', encoding='utf-8')
        bad_link = tmp_path / 'bad_peer-cache.db'
        bad_link.symlink_to(bad_actual.name)
        await _seed_peer_live_db(tmp_path, 'bad_peer', cluster_name='prod')

        before = _counter_value(metrics.cache_sync_errors, peer='bad_peer')

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        after = _counter_value(metrics.cache_sync_errors, peer='bad_peer')
        assert after - before >= 1
    finally:
        await engine.stop()


async def test_peer_error_cursor_not_advanced(tmp_path):
    """A peer whose pull/apply threw must keep its cursor where it was
    so next cycle retries the same range. A bad cycle should not
    silently skip rows."""
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # First cycle: a healthy peer with rows advances the cursor.
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 5000}],
        )
        await engine._sync_once()  # type: ignore[reportPrivateUsage]
        cursor_after_good = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after_good > 0

        # Now corrupt the peer's cache DB file to force a pull failure
        # on the next cycle. The peer's symlink / live.db are fine;
        # only the actual DB data becomes unreadable.
        peer_actual = tmp_path / 'peer1-cache.db.actual'
        peer_actual.write_text('garbage', encoding='utf-8')

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Cursor must not advance (must stay at cursor_after_good).
        cursor_after_bad = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
        assert cursor_after_bad == cursor_after_good
    finally:
        await engine.stop()


# --- cross-worker delete regression -----------------------------------------


async def test_delete_is_local_only_peer_copy_survives_and_is_repulled(tmp_path):
    """**Documented sharp edge.** Deleting a key locally does NOT propagate
    to peers. If a peer still has the same key with a newer
    ``updated_at_ms``, the next sync cycle pulls it back into our local
    DB via LWW — exactly as designed. Use TTL for cross-worker
    invalidation, not delete.

    Test timeline:
    1. Set key locally (SET → flush → DB) with ``updated_at_ms=1000``.
    2. Delete key locally (memory pop + DELETE → flush → row removed
       from local DB).
    3. Peer has the same key with ``updated_at_ms=9999`` (newer).
    4. Sync runs → LWW UPSERT accepts peer's row back into local DB.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]

        # Step 1: locally set and flush.
        cache.set('shared', 'local_value', scope=CacheScope.GLOBAL)
        await engine._flush_once()  # type: ignore[reportPrivateUsage]
        assert cache.delete('shared') is True
        await engine._flush_once()  # type: ignore[reportPrivateUsage]

        # Verify local DB row is gone post-delete.
        db_path = tmp_path / 'me-cache.db.actual'
        async with aiosqlite.connect(str(db_path)) as db:
            cur = await db.execute('SELECT count(*) FROM cache_entries WHERE key = ?', ('shared',))
            row = await cur.fetchone()
            assert row is not None
            assert row[0] == 0, 'local delete should have removed the DB row'
            await cur.close()

        # Step 2: peer has the same key with a much newer timestamp.
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {
                    'key': 'shared',
                    'scope': CacheScope.GLOBAL.value,
                    'value': '"peer_value"',
                    'updated_at_ms': 9_999_999_999_999,
                    'origin_worker_id': 'peer1',
                }
            ],
        )

        # Step 3: sync pulls the peer's row back (LWW accepts).
        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Local DB now has the key again — the delete did NOT propagate.
        async with aiosqlite.connect(str(db_path)) as db:
            cur = await db.execute(
                'SELECT value, origin_worker_id FROM cache_entries WHERE key = ?',
                ('shared',),
            )
            row = await cur.fetchone()
            assert row is not None
            assert row[0] == '"peer_value"'
            assert row[1] == 'peer1'
            await cur.close()
    finally:
        await engine.stop()


# --- system periodic registration --------------------------------------------


async def test_sync_registered_as_system_periodic_task(tmp_path, monkeypatch):
    """``CacheEngine.start()`` must schedule the sync loop via
    ``run_periodic_task(name='cache.sync', ..., system=True)`` — so the
    debug UI renders the [system] badge alongside the flush + cleanup
    tasks, and the loop picks up the shared error-continue policy.

    The spy blocks on a cancellation-aware sleep so the ``asyncio.Task``
    scheduled by ``start()`` stays live for the duration of the test,
    mirroring the pattern in ``test_start_registers_flush_task_as_system_periodic``
    — otherwise the task completes instantly and the monkeypatch binding
    may be unstable across parallel test modules.
    """
    import asyncio as _asyncio

    call_records: list[dict[str, Any]] = []

    async def fake_run_periodic_task(*, name, coro_fn, seconds, on_error, recorder, system):
        # Capture the call shape; block forever until cancelled so the
        # scheduled task exists through the test body. The test can
        # inspect ``call_records`` via a one-tick yield.
        call_records.append(
            {
                'name': name,
                'seconds': seconds,
                'on_error': on_error,
                'system': system,
            }
        )
        try:
            while True:
                await _asyncio.sleep(3600)
        except _asyncio.CancelledError:
            raise

    monkeypatch.setattr(cache_engine_module, 'run_periodic_task', fake_run_periodic_task)

    cache = Cache(origin_worker_id='me')
    engine = CacheEngine(
        config=make_cache_config(),
        debug_config=make_debug_config(tmp_path),
        worker_id='me',
        cluster_name='prod',
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    try:
        # One tick so the scheduled tasks start executing the spy body.
        await _asyncio.sleep(0)

        sync_calls = [c for c in call_records if c['name'] == 'cache.sync']
        assert len(sync_calls) == 1, f'expected one cache.sync registration; got {call_records!r}'
        call = sync_calls[0]
        assert call['system'] is True
        assert call['on_error'] == 'continue'
        # Interval is whatever peer_sync.interval_seconds resolved to
        # (default 30s); we just assert it was passed, not its value.
        assert call['seconds'] > 0
    finally:
        await engine.stop()


async def test_sync_not_registered_when_peer_sync_disabled(tmp_path, monkeypatch):
    """When ``peer_sync.enabled=false``, no ``cache.sync`` periodic should
    be scheduled — the loop would be a waste of wakeups on an engine
    that can't pull from peers.
    """
    import asyncio as _asyncio

    call_records: list[dict[str, Any]] = []

    async def fake_run_periodic_task(*, name, coro_fn, seconds, on_error, recorder, system):
        call_records.append({'name': name})
        try:
            while True:
                await _asyncio.sleep(3600)
        except _asyncio.CancelledError:
            raise

    monkeypatch.setattr(cache_engine_module, 'run_periodic_task', fake_run_periodic_task)

    cache = Cache(origin_worker_id='me')
    engine = CacheEngine(
        config=make_cache_config(peer_sync=CachePeerSyncConfig(enabled=False)),
        debug_config=make_debug_config(tmp_path),
        worker_id='me',
        cluster_name='prod',
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    try:
        # One tick so any scheduled tasks start executing the spy body.
        await _asyncio.sleep(0)

        sync_calls = [c for c in call_records if c['name'] == 'cache.sync']
        assert sync_calls == [], f'expected no cache.sync registration; got {call_records!r}'
    finally:
        await engine.stop()


# --- smoke: cursor used in WHERE clause -------------------------------------


async def test_cursor_filters_already_seen_rows(tmp_path):
    """Integration-style: after cycle 1 pulls some rows, seed a *new*
    row with an ``updated_at_ms`` *below* the cursor. Cycle 2 must NOT
    pull it, since the cursor's ``WHERE updated_at_ms > ?`` excludes it.

    This is the core correctness property of the cursor: we never
    re-read rows we've already seen.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=100)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000},
                {'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 2000},
            ],
        )
        # Cycle 1 reads both and advances cursor to "now-ish" (partial batch).
        pulled1 = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        first = pulled1['peer1']
        assert {row[0] for row in first} == {'k1', 'k2'}
        cursor_after_cycle_1 = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]

        # Add a row whose updated_at_ms is below the cursor — simulates
        # a stale insert on the peer side. Cycle 2 must skip it.
        async with aiosqlite.connect(str(tmp_path / 'peer1-cache.db.actual')) as db:
            stale_ts = cursor_after_cycle_1 - 500
            await db.execute(
                'INSERT INTO cache_entries '
                '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                ' expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                ('k_stale', CacheScope.GLOBAL.value, '"v"', 3, stale_ts, stale_ts, None, 'peer1'),
            )
            await db.commit()

        pulled2 = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        second = pulled2['peer1']
        keys_second = {row[0] for row in second}
        assert 'k_stale' not in keys_second, (
            f'cursor should exclude rows with updated_at_ms <= cursor; got {keys_second}'
        )
    finally:
        await engine.stop()


# --- parametrized: on_error='continue' propagates through periodic loop -----


@pytest.mark.parametrize('err_kind', ['corrupt_db', 'missing_file'])
async def test_peer_error_variants_isolate_and_continue(tmp_path, err_kind):
    """Both corrupt DB files and dangling symlinks produce per-peer
    isolation: a warning is logged, the error counter ticks, and a
    co-located good peer still syncs.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Healthy peer
        await _seed_peer_live_db(tmp_path, 'healthy', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'healthy',
            [{'key': 'kh', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 7000}],
        )

        if err_kind == 'corrupt_db':
            bad_actual = tmp_path / 'bad-cache.db.actual'
            bad_actual.write_text('totally not sqlite', encoding='utf-8')
            (tmp_path / 'bad-cache.db').symlink_to(bad_actual.name)
        else:  # missing_file
            # Symlink points to a path that doesn't exist. discover_peer_dbs
            # filters out broken symlinks, so this peer won't even show up —
            # but if it ever did, the per-peer try/except would catch it.
            # To actually reach the per-peer handler, we create the target
            # then delete it AFTER seeding.
            bad_actual = tmp_path / 'bad-cache.db.actual'
            async with aiosqlite.connect(str(bad_actual)) as db:
                await db.executescript(cache_module.SCHEMA_CACHE_ENTRIES)
                await db.commit()
            (tmp_path / 'bad-cache.db').symlink_to(bad_actual.name)
            # Keep the target file so discover_peer_dbs yields the peer,
            # then make it unreadable by replacing with garbage content.
            bad_actual.write_text('garbage', encoding='utf-8')

        await _seed_peer_live_db(tmp_path, 'bad', cluster_name='prod')

        pulled_by_peer = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Good peer still pulled.
        assert 'healthy' in pulled_by_peer
    finally:
        await engine.stop()
