"""Tests for the CacheEngine peer-sync per-cycle deadline (Phase 2 Task 2).

The engine's ``_sync_once`` wraps ``_sync_inner`` with ``asyncio.wait_for``
to bound a single cycle's wall-clock time. The cap prevents a single slow
peer (NFS lag, remote hang, disk contention) from starving the periodic
task and stacking up overlapping cycles.

Behaviors verified:

1. A slow cycle that exceeds the deadline causes the wrapper to:
   - Return within ~deadline (not block forever).
   - Increment ``cache_peer_sync_timeouts`` by 1.
   - Not raise into the periodic-task wrapper.
2. A normal sync (well under the deadline) leaves the counter untouched.
3. After a timeout, a subsequent ``_sync_once`` runs normally — the
   deadline is per-cycle state, not accumulated state.

To simulate a slow peer without booting a real stalled filesystem we
patch ``_sync_inner`` on the specific engine instance to sleep. That's the
simplest reliable way to exercise ``asyncio.wait_for`` without monkey-
patching aiosqlite's connection lifecycle (which has dual
awaitable/context-manager semantics that are hard to wrap faithfully).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from drakkar import cache as cache_module
from drakkar import metrics
from drakkar.cache import (
    Cache,
    CacheEngine,
    CacheScope,
)
from drakkar.config import CacheConfig, CachePeerSyncConfig, DebugConfig

# --- helpers (mirrors test_cache_sync_pull helpers) -------------------------


def make_debug_config(tmp_path: Path, **overrides: Any) -> DebugConfig:
    """DebugConfig with ``db_dir`` pointed at the temp path and events off."""
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
    """CacheConfig enabled by default, with peer_sync overrides honoured."""
    defaults: dict[str, Any] = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(
    tmp_path: Path,
    *,
    worker_id: str = 'w1',
    cluster_name: str = '',
    cache_overrides: dict[str, Any] | None = None,
    debug_overrides: dict[str, Any] | None = None,
) -> CacheEngine:
    """Spin up a started CacheEngine for peer-sync deadline testing."""
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
    """Create a peer ``-cache.db.actual`` + symlink with the given rows.

    Mirrors the helper in ``test_cache_sync_pull`` so the two test files
    stay drop-in compatible — no production behavior is shared between
    them other than the engine itself.
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


async def _seed_peer_live_db(tmp_path: Path, peer_name: str, cluster_name: str | None) -> Path:
    """Create a peer ``-live.db.actual`` with a single ``worker_config`` row."""
    actual = tmp_path / f'{peer_name}-live.db.actual'
    async with aiosqlite.connect(str(actual)) as db:
        if cluster_name is not None:
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
                (peer_name, cluster_name or None),
            )
        await db.commit()
    link = tmp_path / f'{peer_name}-live.db'
    link.symlink_to(actual.name)
    return actual


# --- deadline-firing behavior -----------------------------------------------


async def test_peer_sync_cycle_deadline_fires_and_counter_increments(tmp_path):
    """A slow ``_sync_inner`` exceeds the configured deadline; ``_sync_once``
    returns within ~deadline and the timeout counter ticks.

    Strategy: replace ``_sync_inner`` on the engine instance with a coro
    that sleeps well past the deadline. This exercises the wrapper's
    ``asyncio.wait_for`` path without depending on aiosqlite's dual
    awaitable/context-manager lifecycle, which is brittle to patch.
    """
    # Two peers seeded — matches the production pattern where ``_sync_inner``
    # would iterate multiple peers. The replacement below doesn't actually
    # touch the peer DBs, but having them on disk documents the "2 peer DBs"
    # aspect of the scenario.
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=30.0,
                cycle_deadline_seconds=0.5,
            ),
        },
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000}],
        )
        await _seed_peer_live_db(tmp_path, 'peer2', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer2',
            [{'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 2000}],
        )

        # Simulate one slow peer: ``_sync_inner`` sleeps for 5s. The 0.5s
        # deadline trips well before that completes.
        async def slow_sync_inner() -> dict[str, list[tuple]]:
            await asyncio.sleep(5.0)
            return {}

        engine._sync_inner = slow_sync_inner  # type: ignore[reportPrivateUsage, method-assign]

        before = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # MED-1: ``_sync_once`` re-raises ``TimeoutError`` on deadline so the
        # periodic-task wrapper logs the run as ``status=error``. Dashboards
        # keyed on ``periodic_task_runs{status='error'}`` already alert on
        # sync failures, so a silent-swallow here would hide peer-sync
        # deadline breaches. The wrapper's default behavior is to keep the
        # loop alive (re-raising does NOT disable peer-sync).
        start_time = asyncio.get_event_loop().time()
        with pytest.raises(TimeoutError):
            await engine._sync_once()  # type: ignore[reportPrivateUsage]
        elapsed = asyncio.get_event_loop().time() - start_time

        after = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # Deadline is 0.5s — must return well before the 5s sleep would
        # have finished. Slack of 1.0s keeps the assertion stable on slow
        # CI runners.
        assert elapsed < 1.5, f'expected <1.5s (deadline 0.5s), got {elapsed:.3f}s'
        # Counter ticked exactly once.
        assert after - before == 1
    finally:
        await engine.stop()


# --- normal cycle leaves counter alone -------------------------------------


async def test_peer_sync_cycle_normal_does_not_increment_counter(tmp_path):
    """A normal peer sync (well under the deadline) must not tick the
    timeout counter. This guards against a spurious increment on the
    success path.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=30.0,
                # Generous deadline — a real on-disk sync should finish
                # in tens of milliseconds.
                cycle_deadline_seconds=5.0,
            ),
        },
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

        before = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        result = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        after = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # No timeout tick.
        assert after == before
        # Normal sync returned rows from the peer.
        assert 'peer1' in result
        keys = {row[0] for row in result['peer1']}
        assert keys == {'k1', 'k2'}
    finally:
        await engine.stop()


# --- subsequent cycle after timeout ----------------------------------------


async def test_peer_sync_continues_after_timeout(tmp_path):
    """After a cycle hits the deadline, a subsequent ``_sync_once`` must
    run normally. No accumulated state should leak from the timed-out
    cycle into the next one.

    Simulates the production case where a transient NFS stall causes one
    cycle to time out but the next cycle (after the stall clears) completes
    normally.

    Strategy: swap ``_sync_inner`` for a slow version, run the first
    ``_sync_once`` (times out), then restore the real ``_sync_inner`` and
    run a second cycle (completes normally).
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=30.0,
                cycle_deadline_seconds=0.5,
            ),
        },
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000}],
        )

        # Keep a reference to the real ``_sync_inner`` bound method so we
        # can restore it after the slow cycle.
        real_sync_inner = engine._sync_inner  # type: ignore[reportPrivateUsage]

        async def slow_sync_inner() -> dict[str, list[tuple]]:
            await asyncio.sleep(5.0)
            return {}

        # First cycle: slow — should time out.
        engine._sync_inner = slow_sync_inner  # type: ignore[reportPrivateUsage, method-assign]

        before_first = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]
        # MED-1: the deadline path re-raises ``TimeoutError`` (see
        # ``test_peer_sync_cycle_deadline_fires_and_counter_increments``
        # for the full rationale).
        with pytest.raises(TimeoutError):
            await engine._sync_once()  # type: ignore[reportPrivateUsage]
        after_first = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]
        assert after_first - before_first == 1

        # Second cycle: restore the real impl. The engine must not carry
        # over any "I hit a deadline last time" state — it should work
        # exactly like a fresh call.
        engine._sync_inner = real_sync_inner  # type: ignore[reportPrivateUsage, method-assign]

        before_second = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]
        result = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        after_second = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # No new timeout — cycle ran normally.
        assert after_second == before_second
        # And we got the peer's row.
        assert 'peer1' in result
        keys = {row[0] for row in result['peer1']}
        assert keys == {'k1'}
    finally:
        await engine.stop()


# --- default-derived deadline ----------------------------------------------


async def test_peer_sync_default_deadline_derives_from_interval(tmp_path):
    """When ``cycle_deadline_seconds`` is ``None``, the wrapper must derive
    the deadline from ``interval_seconds * 0.9``. Sanity check that the
    default path exercises the timeout wiring (not just that it compiles).
    """
    # Interval 0.5s → default deadline 0.45s. A 5s slow sync still trips
    # well inside that cap.
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=0.5,
                # cycle_deadline_seconds left as None — must fall back to
                # interval_seconds * 0.9 = 0.45s.
            ),
        },
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [{'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000}],
        )

        async def slow_sync_inner() -> dict[str, list[tuple]]:
            await asyncio.sleep(5.0)
            return {}

        engine._sync_inner = slow_sync_inner  # type: ignore[reportPrivateUsage, method-assign]

        before = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]
        start_time = asyncio.get_event_loop().time()
        # MED-1: re-raises on deadline (see the earlier deadline test).
        with pytest.raises(TimeoutError):
            await engine._sync_once()  # type: ignore[reportPrivateUsage]
        elapsed = asyncio.get_event_loop().time() - start_time
        after = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # Default deadline is 0.45s — comfortably under 1.5s.
        assert elapsed < 1.5, f'expected <1.5s (default deadline ~0.45s), got {elapsed:.3f}s'
        assert after - before == 1
    finally:
        await engine.stop()


# --- concurrent peer-sync regression (Phase 2 Task 10) ---------------------
#
# Two dimensions of concurrency to pin down:
#
# 1. **Intra-cycle**: two ``_sync_once()`` calls fire on the same event loop
#    at the same time (overlapping cycles). Real cause: a late-running
#    periodic tick overlaps the next scheduled tick under load.
# 2. **Cross-operation**: ``_sync_once()`` runs alongside ``_flush_once()``
#    on the same writer connection. Real cause: a periodic flush tick
#    overlaps a periodic sync tick — both acquire the writer through the
#    same aiosqlite thread.
#
# Neither scenario holds an explicit lock in production — concurrency is
# implicit through CPython's GIL (dict writes are atomic), aiosqlite's
# single-thread serialization of SQL calls, and the per-peer error
# isolation in ``_sync_inner``. These tests exercise the path to catch
# regressions (e.g. someone refactors ``_peer_cursors`` into a class that
# breaks atomic dict writes, or a future ``_sync_inner`` rewrite loses its
# per-peer try/except).


async def test_concurrent_sync_once_smoke(tmp_path):
    """SMOKE: two overlapping ``_sync_once`` calls plus a concurrent
    ``_flush_once`` on the same engine complete without aiosqlite errors,
    and the final local-DB state is consistent.

    NOTE: This is a smoke test, NOT a race-regression test. aiosqlite
    serializes all SQL calls on a single background thread, so two
    concurrent ``_sync_once`` coroutines on the event loop can never
    actually interleave reads/writes to the same connection. A genuine
    race-regression test would need to inject a deterministic yield via
    monkey-patch (e.g. ``await asyncio.sleep(0)`` between the cursor
    read and write in ``_advance_cursor``) to force the interleaving
    the scheduler won't provide.

    Why the smoke check is still worth running:
     - Catches coarse-grained regressions (e.g. a refactor that removes
       per-peer try/except would surface here as a raised exception
       even without interleaving).
     - ``_peer_cursors`` is a plain dict — reads and writes to individual
       keys are atomic under CPython's GIL.
     - Per-peer errors inside ``_sync_inner`` are isolated, so even if one
       concurrent call happens to observe a transient state inconsistency
       from the other, it would be caught and logged, not raised.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=30.0,
                # Generous deadline — we don't want the deadline wrapper
                # to mask any actual race by preempting a slow cycle.
                cycle_deadline_seconds=10.0,
            ),
        },
    )
    try:
        # Seed 2 peer DBs — matches the plan's "2 peer DBs" scenario.
        # Different rows per peer so we can assert both peers' rows
        # end up in the local DB.
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'p1_k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000},
                {'key': 'p1_k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1001},
            ],
        )
        await _seed_peer_live_db(tmp_path, 'peer2', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer2',
            [
                {'key': 'p2_k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 2000},
                {'key': 'p2_k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 2001},
            ],
        )

        # Seed the local cache with a local row so ``_flush_once`` has
        # something to write alongside the two syncs.
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None
        cache.set('local_k1', 'v1', scope=CacheScope.LOCAL)

        # Two ``_sync_once`` calls and one ``_flush_once`` in the SAME
        # gather. Use ``return_exceptions=True`` so one coroutine's error
        # doesn't cancel the others — we want to see every exception.
        results = await asyncio.gather(
            engine._sync_once(),  # type: ignore[reportPrivateUsage]
            engine._sync_once(),  # type: ignore[reportPrivateUsage]
            engine._flush_once(),  # type: ignore[reportPrivateUsage]
            return_exceptions=True,
        )

        # No coroutine raised. The engine's contract for both methods is
        # to return silently on transient errors (per-peer isolation,
        # writer guard on None); an exception here means a real race.
        for i, result in enumerate(results):
            assert not isinstance(result, BaseException), f'coroutine {i} raised {type(result).__name__}: {result}'

        # Final DB state: every peer row + the one local row. No
        # duplicates (UPSERT via LWW guarantees one row per key even
        # when both sync cycles UPSERT the same peer rows).
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            rows = await cur.fetchall()
            await cur.close()
        local_keys = [row[0] for row in rows]

        # Set equality — every expected key present.
        assert set(local_keys) == {'local_k1', 'p1_k1', 'p1_k2', 'p2_k1', 'p2_k2'}
        # List length equality — no duplicates. (Redundant with the set
        # check because ``cache_entries.key`` has a PRIMARY KEY
        # constraint, but the assertion documents the invariant.)
        assert len(local_keys) == len(set(local_keys)), f'duplicate keys in local DB: {local_keys}'
    finally:
        await engine.stop()


async def test_concurrent_sync_across_cycles(tmp_path):
    """Three concurrent ``_sync_once`` cycles run back-to-back via
    ``asyncio.gather``. The peer cursor must be monotonic — it never
    regresses from one cycle to the next, even when two cycles interleave
    reads/writes of the cursor map.

    The strict monotonicity contract (``_advance_cursor``: full-batch →
    advance to last row's ``updated_at_ms``, partial-batch →
    ``max(last_row_ts, cursor_ms)``) must survive concurrent invocation.
    A regression where two overlapping cycles both read the same old
    cursor value and one writes back a smaller value would skip rows on
    the next cycle — hard to spot in production but trivially caught here.
    """
    # Use batch_size=2 so a peer with 4 rows takes multiple cycles to
    # fully drain. The early cycles return full batches (cursor advances
    # to the last row's ts); later cycles return partial batches (cursor
    # stays at or above its last value).
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                batch_size=2,
                interval_seconds=30.0,
                cycle_deadline_seconds=10.0,
            ),
        },
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # 4 rows with strictly increasing timestamps — cursor advances
        # deterministically cycle by cycle.
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k1', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1000},
                {'key': 'k2', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1001},
                {'key': 'k3', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1002},
                {'key': 'k4', 'scope': CacheScope.GLOBAL.value, 'updated_at_ms': 1003},
            ],
        )

        # Snapshot the cursor map AFTER each gather call to assert
        # monotonicity. Before the first cycle: no cursor at all.
        assert 'peer1' not in engine._peer_cursors  # type: ignore[reportPrivateUsage]

        cursor_history: list[int] = []

        # Run 3 concurrent cycles. Each cycle calls ``_sync_once`` and
        # independently advances the cursor. After gather returns,
        # capture the final cursor value; repeat for 3 gather rounds.
        for _ in range(3):
            results = await asyncio.gather(
                engine._sync_once(),  # type: ignore[reportPrivateUsage]
                engine._sync_once(),  # type: ignore[reportPrivateUsage]
                engine._sync_once(),  # type: ignore[reportPrivateUsage]
                return_exceptions=True,
            )
            for i, result in enumerate(results):
                assert not isinstance(result, BaseException), f'cycle {i} raised {type(result).__name__}: {result}'
            # Cursor MUST exist after the first gather and MUST NOT
            # regress across gathers.
            current = engine._peer_cursors['peer1']  # type: ignore[reportPrivateUsage]
            if cursor_history:
                assert current >= cursor_history[-1], (
                    f'cursor regressed: {cursor_history[-1]} -> {current}. Full history: {[*cursor_history, current]}'
                )
            cursor_history.append(current)

        # After 3 rounds of 3 concurrent cycles each, every peer row
        # must be in the local DB. 4 rows, batch_size=2 — even a single
        # round would sync at least 2 rows; three rounds drain the peer
        # completely (main pull of 2 rows on cycle 1, main pull of 2
        # more on cycle 2, partial/zero pull on cycle 3). Concurrent
        # cycles within a round may all race to the same cursor value,
        # but the UPSERT path is idempotent so the outcome is the same.
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT key FROM cache_entries ORDER BY key')
            rows = await cur.fetchall()
            await cur.close()
        local_keys = {row[0] for row in rows}
        assert local_keys == {'k1', 'k2', 'k3', 'k4'}, (
            f'all peer rows must be synced after 3 concurrent cycles; '
            f'cursor history: {cursor_history}; local keys: {local_keys}'
        )

        # Final cursor must be at 1003 (last row's timestamp) once the
        # peer is fully drained. If a race had advanced the cursor past
        # 1003 (impossible given strict monotonicity in the SQL) or
        # rolled it back below, this catches it.
        assert cursor_history[-1] == 1003, f'cursor must settle at 1003 after full drain; history: {cursor_history}'
    finally:
        await engine.stop()


# --- deadline-vs-memory-invalidation regression ----------------------------
#
# Review finding: ``_apply_peer_rows`` previously called
# ``asyncio.shield(self._commit_peer_rows(rows))`` and then ran the counter
# ticks and ``_invalidate_memory_keys`` AFTER the shielded block. When the
# outer ``_sync_once`` deadline fired, ``CancelledError`` would still raise
# at the shield call site (the shield only protects the inner coro from
# being cancelled, not the caller from seeing the cancellation). The code
# after the shield never ran, so the DB had new peer rows but memory kept
# the stale pre-sync values — ``Cache.get()`` kept returning stale until
# TTL or overwrite.
#
# Fix: move the counter ticks and ``_invalidate_memory_keys`` INSIDE
# ``_commit_peer_rows`` (which the shield fully protects). The deadline
# still fires, but the shielded region completes the full trio
# (commit + counters + memory invalidation) before cancellation surfaces.
#
# The test below drives the same race deterministically by calling
# ``_apply_peer_rows`` under a short ``asyncio.wait_for`` deadline, with a
# patched ``_commit_peer_rows`` that sleeps LONG ENOUGH for the deadline to
# fire mid-apply. With the fix, memory is invalidated despite the
# cancellation; without the fix, memory would still hold the stale entry.


async def test_peer_sync_deadline_does_not_leave_stale_memory(tmp_path):
    """When the peer-sync deadline fires during ``_apply_peer_rows``, memory
    invalidation MUST still run — otherwise ``Cache.get()`` serves stale.

    Pre-fix shape (the regression):
        ``_apply_peer_rows`` called ``asyncio.shield(_commit_peer_rows(rows))``
        for the commit, then ran counter ticks + ``_invalidate_memory_keys``
        AFTER the shield. A ``wait_for`` cancellation would raise
        ``CancelledError`` at the shield boundary as soon as the inner coro
        completed, skipping the invalidation. The DB had new rows but
        memory kept the stale pre-sync values.

    Fix shape:
        Counters + invalidation moved INSIDE ``_commit_peer_rows`` so the
        shield covers the full trio. The deadline still fires, but the
        shielded region runs to completion before cancellation surfaces.

    Test strategy: seed a memory entry, then call ``_apply_peer_rows``
    under a short ``wait_for`` while a patched writer ``commit()``
    sleeps long enough to trip the deadline. Assert the ``TimeoutError``
    is raised (deadline fired) AND memory is invalidated anyway.

    The writer's ``commit`` is patched rather than ``_commit_peer_rows``
    itself so the test doesn't couple to the internal helper's exact
    signature. Any implementation that keeps invalidation inside the
    shielded region passes; any regression that moves it back outside
    fails.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={
            'peer_sync': CachePeerSyncConfig(
                enabled=True,
                interval_seconds=30.0,
                # Generous deadline — this test does not go through
                # ``_sync_once``; it calls ``_apply_peer_rows`` directly
                # with its own ``wait_for`` to pin down the cancellation
                # timing deterministically.
                cycle_deadline_seconds=10.0,
            ),
        },
    )
    try:
        cache = engine._cache  # type: ignore[reportPrivateUsage]
        assert cache is not None

        # Seed a stale memory entry. Without invalidation, ``Cache.get``
        # would return 'stale' from the memory fast path after the sync.
        cache.set('shared', 'stale', scope=CacheScope.GLOBAL)
        assert 'shared' in cache._memory  # type: ignore[reportPrivateUsage]

        # Patch the writer's ``commit`` to sleep after performing the
        # real commit. That extends the shielded region long enough for
        # the outer ``wait_for`` deadline to fire WHILE
        # ``_commit_peer_rows`` is still running. The shield keeps us
        # inside ``_commit_peer_rows`` until the sleep finishes; the
        # fix's post-commit invalidation runs during that window. Pre-fix,
        # the post-shield invalidation would be skipped as soon as the
        # shielded call returned and ``CancelledError`` propagated.
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        original_commit = engine._writer_db.commit  # type: ignore[reportPrivateUsage]

        async def slow_commit(*args, **kwargs):
            # Run the real commit first so the DB transaction actually
            # closes — otherwise the sleep below would hold an open
            # transaction against the writer.
            await original_commit(*args, **kwargs)
            # Sleep LONG ENOUGH that the outer ``wait_for`` (50ms) fires
            # while we're still inside the shielded region.
            await asyncio.sleep(0.5)

        engine._writer_db.commit = slow_commit  # type: ignore[reportPrivateUsage,assignment]

        # Build a peer row that targets 'shared' with a newer timestamp so
        # the LWW UPSERT accepts it. Binding order mirrors LWW_UPSERT_SQL:
        # (key, scope, value, size_bytes, created_at_ms, updated_at_ms,
        #  expires_at_ms, origin_worker_id).
        new_value = '"fresh"'
        peer_rows = [
            (
                'shared',
                CacheScope.GLOBAL.value,
                new_value,
                len(new_value.encode('utf-8')),
                9_999_999_000,  # created_at_ms
                9_999_999_999,  # updated_at_ms — newer than local
                None,  # no TTL
                'peer1',
            )
        ]

        # Call ``_apply_peer_rows`` under a short ``wait_for`` that fires
        # well before the slow commit returns. The TimeoutError surfaces
        # at the ``await asyncio.shield(...)`` boundary.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                engine._apply_peer_rows(peer_name='peer1', rows=peer_rows),  # type: ignore[reportPrivateUsage]
                timeout=0.05,
            )

        # ``asyncio.shield`` keeps the inner coroutine running even after
        # the caller's ``wait_for`` raises ``TimeoutError``. The post-
        # commit invalidation runs INSIDE that shielded region, so it
        # executes eventually — but not before ``wait_for`` returns.
        # Wait for the shielded commit to finish its patched 0.5s sleep
        # so we can assert on the final memory state (what subsequent
        # ``Cache.get()`` calls would see once the shield completes).
        #
        # Pre-fix behavior: the post-shield invalidation would be SKIPPED
        # entirely when ``CancelledError`` propagated. Even after the
        # shielded task finished, 'shared' would remain in memory forever
        # — until TTL or a local overwrite. No amount of sleeping would
        # invalidate it.
        #
        # Post-fix behavior: sleeping for longer than the slow-commit sleep
        # gives the shielded coro time to complete; its internal
        # ``_invalidate_memory_keys`` call runs and 'shared' is evicted.
        await asyncio.sleep(0.6)

        # Restore the original commit BEFORE any further engine I/O —
        # otherwise ``engine.stop()`` in the finally block would wait
        # for the 0.5s sleep on its own shutdown commit.
        engine._writer_db.commit = original_commit  # type: ignore[reportPrivateUsage,assignment]

        # The core regression assertion: memory was invalidated inside
        # the shielded region, so the stale 'shared' entry is GONE from
        # memory after the shielded task completes. Without the fix,
        # the invalidation call would be AFTER the shield and never run
        # under cancellation — 'shared' would still be present.
        assert 'shared' not in cache._memory, (  # type: ignore[reportPrivateUsage]
            'memory invalidation must run inside the shielded region — '
            'deadline-triggered cancellation must not leave stale memory entries'
        )

        # Also assert the DB got the new value (commit ran before sleep,
        # so this should always hold — sanity check that the shield
        # protected the commit itself).
        async with aiosqlite.connect(str(tmp_path / 'me-cache.db.actual')) as db:
            cur = await db.execute('SELECT value FROM cache_entries WHERE key = ?', ('shared',))
            row = await cur.fetchone()
            await cur.close()
        assert row is not None, 'peer row must have been committed inside the shield'
        assert row[0] == new_value, f'expected committed peer value, got {row[0]!r}'
    finally:
        await engine.stop()


# --- pytest configuration ---------------------------------------------------
# All tests are async and use the project's auto asyncio_mode; no explicit
# ``@pytest.mark.asyncio`` needed. ``pytest_plugins`` intentionally omitted.
_ = pytest  # keep the import anchored even when no marks are used
