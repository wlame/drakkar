"""Tests for ``Cache.get`` warm-on-read racing concurrent ``set``/``delete``.

``get()`` releases the cache lock during its DB fallback SELECT. A
concurrent ``set`` or ``delete`` can therefore land between the memory
miss check and the post-SELECT warm. The warm must not clobber the newer
state:

- set-during-get: the racing set's newer value stays in memory (and is
  returned); the stale DB row must not overwrite it — otherwise memory
  and DB permanently disagree once the dirty op flushes.
- delete-during-get: the warm must not resurrect the deleted key in
  memory; the get reports a miss.

The races are made deterministic by wrapping the real read-only aiosqlite
connection in a pausing proxy: the test parks ``get()`` inside its DB
read on an event, mutates the cache, then releases the read.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import aiosqlite
import pytest

from drakkar.cache import SCHEMA_CACHE_ENTRIES, Cache, CacheScope, Op, _now_ms

# --- helpers ----------------------------------------------------------------


class _PausingReader:
    """Proxy for the reader connection whose ``execute`` blocks on an event.

    ``entered`` is set once ``get()`` reaches the DB read; the read then
    waits for ``release`` before delegating to the real connection. This
    pins down the exact interleaving the warm-on-read race needs.
    """

    def __init__(self, inner: aiosqlite.Connection) -> None:
        self._inner = inner
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def execute(self, sql: str, params):
        @contextlib.asynccontextmanager
        async def paused():
            self.entered.set()
            await self.release.wait()
            async with self._inner.execute(sql, params) as cursor:
                yield cursor

        return paused()


async def _seed_db(tmp_path: Path, *, key: str, value_json: str, updated_at_ms: int) -> Path:
    """Create a cache DB holding one unexpired row for ``key``."""
    db_path = tmp_path / 'cache.db'
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(SCHEMA_CACHE_ENTRIES)
        await db.execute(
            'INSERT INTO cache_entries '
            '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, expires_at_ms, origin_worker_id) '
            'VALUES (?,?,?,?,?,?,?,?)',
            (
                key,
                CacheScope.LOCAL.value,
                value_json,
                len(value_json.encode('utf-8')),
                updated_at_ms,
                updated_at_ms,
                None,
                'other-worker',
            ),
        )
        await db.commit()
    return db_path


async def _make_paused_cache(tmp_path: Path, *, key: str) -> tuple[Cache, _PausingReader, aiosqlite.Connection]:
    """Cache wired to a pausing read-only connection over a seeded DB.

    The seeded row is deliberately old (60s in the past) so a racing
    ``set()`` during the paused read is unambiguously newer.
    """
    db_path = await _seed_db(
        tmp_path,
        key=key,
        value_json='"old-db-value"',
        updated_at_ms=_now_ms() - 60_000,
    )
    reader = await aiosqlite.connect(f'file:{db_path}?mode=ro', uri=True)
    cache = Cache(origin_worker_id='w1')
    pausing = _PausingReader(reader)
    cache._reader_db = pausing  # type: ignore[assignment]
    return cache, pausing, reader


# --- set-during-get ----------------------------------------------------------


async def test_get_racing_set_keeps_newer_value_in_memory(tmp_path):
    """A ``set`` landing during the DB read wins: the warm must not
    overwrite the newer memory value with the stale DB row."""
    cache, pausing, reader = await _make_paused_cache(tmp_path, key='k')
    try:
        get_task = asyncio.create_task(cache.get('k'))
        await asyncio.wait_for(pausing.entered.wait(), timeout=2.0)

        # The race: a newer value lands while the DB read is in flight.
        cache.set('k', 'new-value', scope=CacheScope.LOCAL)

        pausing.release.set()
        result = await asyncio.wait_for(get_task, timeout=2.0)

        # The get returns the freshest value, and memory keeps it.
        assert result == 'new-value'
        assert cache.peek('k') == 'new-value'
        # The pending SET still flushes the new value — the dirty op must
        # not have been disturbed by the warm.
        pending = cache._dirty['k']
        assert pending.op is Op.SET
        assert pending.entry is not None
        assert pending.entry.value == '"new-value"'
    finally:
        await reader.close()


async def test_get_racing_set_on_missing_db_row_still_serves_set(tmp_path):
    """Same race, but the DB has no row at all: get() must serve the value
    the racing set installed rather than reporting a miss."""
    db_path = tmp_path / 'cache.db'
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(SCHEMA_CACHE_ENTRIES)
        await db.commit()
    reader = await aiosqlite.connect(f'file:{db_path}?mode=ro', uri=True)
    cache = Cache(origin_worker_id='w1')
    pausing = _PausingReader(reader)
    cache._reader_db = pausing  # type: ignore[assignment]
    try:
        get_task = asyncio.create_task(cache.get('k'))
        await asyncio.wait_for(pausing.entered.wait(), timeout=2.0)
        cache.set('k', 'set-mid-read', scope=CacheScope.LOCAL)
        pausing.release.set()
        # The DB row is absent so this get reports a miss (it read before
        # the set), but memory must retain the racing set's value.
        await asyncio.wait_for(get_task, timeout=2.0)
        assert cache.peek('k') == 'set-mid-read'
    finally:
        await reader.close()


# --- delete-during-get --------------------------------------------------------


async def test_get_racing_delete_does_not_resurrect_key(tmp_path):
    """A ``delete`` landing during the DB read wins: the warm must not
    resurrect the deleted key in memory from the stale DB row."""
    cache, pausing, reader = await _make_paused_cache(tmp_path, key='k')
    try:
        get_task = asyncio.create_task(cache.get('k'))
        await asyncio.wait_for(pausing.entered.wait(), timeout=2.0)

        # The race: the key is deleted while the DB read is in flight.
        cache.delete('k')

        pausing.release.set()
        result = await asyncio.wait_for(get_task, timeout=2.0)

        assert result is None
        assert 'k' not in cache._memory, 'warm must not resurrect a deleted key in memory'
        assert cache.peek('k') is None
        # The pending DELETE still removes the DB row on the next flush.
        assert cache._dirty['k'].op is Op.DELETE
    finally:
        await reader.close()


# --- no-race baseline ---------------------------------------------------------


async def test_get_without_race_still_warms_memory(tmp_path):
    """Sanity check: with no concurrent mutation the DB fallback warms
    memory exactly as before the race guards were added."""
    cache, pausing, reader = await _make_paused_cache(tmp_path, key='k')
    try:
        pausing.release.set()  # no pause — plain read
        result = await asyncio.wait_for(cache.get('k'), timeout=2.0)
        assert result == 'old-db-value'
        assert cache.peek('k') == 'old-db-value'
        assert 'k' in cache._memory
    finally:
        await reader.close()


# --- pytest configuration ---------------------------------------------------
# All tests are async and use the project's auto asyncio_mode.
_ = pytest
