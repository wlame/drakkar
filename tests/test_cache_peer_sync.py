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

        start_time = asyncio.get_event_loop().time()
        result = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        elapsed = asyncio.get_event_loop().time() - start_time

        after = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # Deadline is 0.5s — must return well before the 5s sleep would
        # have finished. Slack of 1.0s keeps the assertion stable on slow
        # CI runners.
        assert elapsed < 1.5, f'expected <1.5s (deadline 0.5s), got {elapsed:.3f}s'
        # Counter ticked exactly once.
        assert after - before == 1
        # Empty dict on timeout — the periodic task continues.
        assert result == {}
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
        result = await engine._sync_once()  # type: ignore[reportPrivateUsage]
        elapsed = asyncio.get_event_loop().time() - start_time
        after = metrics.cache_peer_sync_timeouts._value.get()  # type: ignore[reportPrivateUsage]

        # Default deadline is 0.45s — comfortably under 1.5s.
        assert elapsed < 1.5, f'expected <1.5s (default deadline ~0.45s), got {elapsed:.3f}s'
        assert after - before == 1
        assert result == {}
    finally:
        await engine.stop()


# --- pytest configuration ---------------------------------------------------
# All tests are async and use the project's auto asyncio_mode; no explicit
# ``@pytest.mark.asyncio`` needed. ``pytest_plugins`` intentionally omitted.
_ = pytest  # keep the import anchored even when no marks are used
