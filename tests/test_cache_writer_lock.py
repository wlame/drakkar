"""Tests for writer-connection serialization and flush-failure rollback.

The engine's writer connection is shared by three paths — the flush cycle,
the cleanup DELETE+commit, and peer-sync's UPSERT+commit. aiosqlite
serializes individual statements, not transactions, so all three must hold
``_writer_lock`` for their transaction; otherwise an interleaved commit
from one path could persist another path's half-applied batch.

The second contract verified here: a failed flush explicitly rolls back
the open transaction. Without the rollback, a partial batch (say, a
successful UPSERT ``executemany`` followed by a failed DELETE
``executemany``) would sit in an open transaction and be silently
committed by the next unrelated commit on the shared connection.

Everything runs against real on-disk SQLite via ``tmp_path`` (the
established pattern for cache tests) — no mocks beyond targeted
monkeypatching of the writer's ``executemany``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from drakkar.cache import Cache, CacheEngine, CacheScope
from drakkar.config import CacheConfig, UIConfig
from tests.conftest import make_ui_config

# --- helpers ----------------------------------------------------------------


def make_debug_config(tmp_path: Path, **overrides: Any) -> UIConfig:
    defaults: dict[str, Any] = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': False,
        'store_config': False,
        'store_state': False,
    }
    defaults.update(overrides)
    return make_ui_config(**defaults)


async def _make_started_engine(tmp_path: Path, *, worker_id: str = 'w1') -> CacheEngine:
    """Build and start a CacheEngine with an attached Cache."""
    cache = Cache(origin_worker_id=worker_id)
    engine = CacheEngine(
        config=CacheConfig(enabled=True),
        ui_config=make_debug_config(tmp_path),
        worker_id=worker_id,
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    return engine


async def _db_keys(db_path: Path) -> set[str]:
    """Return the set of keys currently in cache_entries."""
    async with aiosqlite.connect(str(db_path)) as db, db.execute('SELECT key FROM cache_entries') as cur:
        rows = await cur.fetchall()
    return {row[0] for row in rows}


# --- flush failure → rollback ------------------------------------------------


async def test_flush_failure_rolls_back_partial_batch(tmp_path):
    """A flush that fails mid-transaction must roll back — a later
    unrelated commit on the shared writer must not persist the partial batch.

    Failure shape: the SET ``executemany`` succeeds, then the DELETE
    ``executemany`` raises. Without an explicit ``rollback()``, the SET
    rows would sit in an open transaction and be committed by the next
    ``commit()`` on the connection (here: a cleanup cycle).
    """
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache
        assert cache is not None
        assert engine._writer_db is not None

        # Two SETs + one DELETE so the flush issues both executemany calls.
        cache.set('alpha', 'value-a', scope=CacheScope.LOCAL)
        cache.set('beta', 'value-b', scope=CacheScope.LOCAL)
        cache.delete('gone')

        original_executemany = engine._writer_db.executemany

        async def failing_delete_executemany(sql: str, params):
            if sql.lstrip().upper().startswith('DELETE'):
                raise RuntimeError('simulated disk error on DELETE')
            return await original_executemany(sql, params)

        engine._writer_db.executemany = failing_delete_executemany  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match='simulated disk error'):
            await engine._flush_once()

        # An unrelated commit on the same connection — pre-fix this would
        # silently persist the half-applied SET batch left in the open
        # transaction. Cleanup is a natural "next unrelated commit".
        await engine._cleanup_once()

        db_path = tmp_path / 'w1-cache.db.actual'
        assert await _db_keys(db_path) == set(), 'rolled-back flush batch must not reach the DB'

        # The failed ops were restored to the dirty map — a healthy flush
        # persists them.
        engine._writer_db.executemany = original_executemany  # type: ignore[method-assign]
        await engine._flush_once()
        assert await _db_keys(db_path) == {'alpha', 'beta'}
    finally:
        await engine.stop()


# --- writer-lock serialization ----------------------------------------------


async def test_cleanup_waits_for_writer_lock(tmp_path):
    """``_cleanup_once`` must not touch the writer while another path holds
    ``_writer_lock`` — its DELETE+commit is serialized behind the lock."""
    engine = await _make_started_engine(tmp_path)
    try:
        async with engine._writer_lock:
            cleanup = asyncio.create_task(engine._cleanup_once())
            # Give the task a real chance to (wrongly) run its SQL.
            await asyncio.sleep(0.05)
            assert not cleanup.done(), 'cleanup must block until the writer lock is free'
        await asyncio.wait_for(cleanup, timeout=2.0)
    finally:
        await engine.stop()


async def test_peer_commit_waits_for_writer_lock(tmp_path):
    """``_commit_peer_rows`` must serialize behind ``_writer_lock`` like
    every other writer user — and still land its rows once the lock frees."""
    engine = await _make_started_engine(tmp_path)
    try:
        value = '"from-peer"'
        rows = [
            (
                'peer-key',
                CacheScope.GLOBAL.value,
                value,
                len(value.encode('utf-8')),
                1000,  # created_at_ms
                2000,  # updated_at_ms
                None,  # no TTL
                'peer1',
            )
        ]
        async with engine._writer_lock:
            commit = asyncio.create_task(engine._commit_peer_rows(peer_name='peer1', rows=rows))
            await asyncio.sleep(0.05)
            assert not commit.done(), 'peer commit must block until the writer lock is free'
        await asyncio.wait_for(commit, timeout=2.0)

        db_path = tmp_path / 'w1-cache.db.actual'
        assert await _db_keys(db_path) == {'peer-key'}
    finally:
        await engine.stop()


async def test_flush_holds_writer_lock_against_concurrent_cleanup(tmp_path):
    """A slow flush commit must exclude cleanup's commit for its whole
    transaction — the interleaving the shared lock exists to prevent."""
    engine = await _make_started_engine(tmp_path)
    try:
        cache = engine._cache
        assert cache is not None
        assert engine._writer_db is not None
        cache.set('slow-key', 'v', scope=CacheScope.LOCAL)

        in_commit = asyncio.Event()
        release_commit = asyncio.Event()
        original_commit = engine._writer_db.commit

        async def pausing_commit(*args, **kwargs):
            in_commit.set()
            await release_commit.wait()
            return await original_commit(*args, **kwargs)

        engine._writer_db.commit = pausing_commit  # type: ignore[method-assign,assignment]
        flush = asyncio.create_task(engine._flush_once())
        await asyncio.wait_for(in_commit.wait(), timeout=2.0)

        # Flush is mid-transaction (paused before commit). Cleanup must
        # queue on the lock rather than committing into the transaction.
        cleanup = asyncio.create_task(engine._cleanup_once())
        await asyncio.sleep(0.05)
        assert not cleanup.done(), 'cleanup must wait for the in-flight flush transaction'

        release_commit.set()
        await asyncio.wait_for(flush, timeout=2.0)
        await asyncio.wait_for(cleanup, timeout=2.0)
        engine._writer_db.commit = original_commit  # type: ignore[method-assign,assignment]

        db_path = tmp_path / 'w1-cache.db.actual'
        assert await _db_keys(db_path) == {'slow-key'}
    finally:
        await engine.stop()


# --- pytest configuration ---------------------------------------------------
# All tests are async and use the project's auto asyncio_mode.
_ = pytest
