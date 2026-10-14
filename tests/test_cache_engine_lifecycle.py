"""Tests for CacheEngine start/stop lifecycle and SQLite schema init.

This task focuses on the engine's **lifecycle contract** — opening the
writer connection, applying the schema, putting the DB in WAL mode,
creating the `<worker>-cache.db` file, and closing
cleanly on stop. The flush loop, reader connection, peer sync, and
cleanup loop are wired up in later tasks (7/8/9/10/11+); here we only
verify that the engine's bootstrap work is correct in isolation.

The tests use `tmp_path` for on-disk scenarios so concurrent runs do not
collide, and `aiosqlite` directly to inspect the resulting DB file (WAL
mode, table presence, index presence). We don't mock SQLite — the whole
point of this task is that the schema actually lands on disk.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
from structlog.testing import capture_logs

from drakkar.cache import CacheEngine
from drakkar.config import CacheConfig, CacheExchangeConfig
from tests.conftest import split_cache_overrides


def make_cache_config(**overrides) -> CacheConfig:
    """Build a CacheConfig with cache enabled by default."""
    defaults: dict = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(tmp_path: Path, *, worker_id: str = 'w1', **cfg_overrides) -> CacheEngine:
    """Construct an engine with sane test defaults.

    No recorder wired in — lifecycle tests don't need one. The recorder
    hook path is exercised in later tasks.
    """
    cache_fields, exchange = split_cache_overrides(cfg_overrides)
    return CacheEngine(
        config=make_cache_config(**cache_fields),
        exchange=exchange,
        storage_dir=str(tmp_path),
        worker_id=worker_id,
        cluster_name='',
        recorder=None,
    )


# --- happy-path start/stop --------------------------------------------------


async def test_start_opens_writer_connection(tmp_path):
    """start() must open an aiosqlite writer connection."""
    engine = await _make_engine(tmp_path)
    await engine.start()
    try:
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        assert isinstance(engine._writer_db, aiosqlite.Connection)  # type: ignore[reportPrivateUsage]
    finally:
        await engine.stop()


async def test_start_creates_cache_db_file(tmp_path):
    """The cache file ``<worker>-cache.db`` is a plain file on disk after start()."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        path = tmp_path / 'w1-cache.db'
        assert path.is_file()
        assert not path.is_symlink()
    finally:
        await engine.stop()


async def test_stop_keeps_the_cache_file(tmp_path):
    """The cache file stays on disk after a graceful stop (persistence across restarts)."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    await engine.stop()

    assert (tmp_path / 'w1-cache.db').is_file()


async def test_start_applies_schema_table(tmp_path):
    """cache_entries table exists after schema init with the correct columns."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        db_path = tmp_path / 'w1-cache.db'
        async with aiosqlite.connect(str(db_path)) as db:
            cursor = await db.execute('PRAGMA table_info(cache_entries)')
            rows = await cursor.fetchall()
            await cursor.close()
            # PRAGMA table_info returns (cid, name, type, notnull, dflt, pk)
            column_names = {row[1] for row in rows}
            assert column_names == {
                'key',
                'scope',
                'value',
                'size_bytes',
                'created_at_ms',
                'updated_at_ms',
                'expires_at_ms',
                'origin_worker_id',
            }
    finally:
        await engine.stop()


async def test_start_applies_both_indexes(tmp_path):
    """Both cache_entries indexes exist after schema init."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        db_path = tmp_path / 'w1-cache.db'
        async with aiosqlite.connect(str(db_path)) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='cache_entries'")
            rows = await cursor.fetchall()
            await cursor.close()
            index_names = {row[0] for row in rows}
            # implicit autoindex for PRIMARY KEY may also appear; filter to our named ones
            assert 'idx_cache_expires' in index_names
            assert 'idx_cache_scope_updated' in index_names
    finally:
        await engine.stop()


async def test_start_sets_wal_mode(tmp_path):
    """PRAGMA journal_mode = WAL is applied to the writer connection."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        # read the pragma back from the live writer connection
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        cursor = await engine._writer_db.execute('PRAGMA journal_mode')  # type: ignore[reportPrivateUsage]
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        # SQLite reports 'wal' in lower case
        assert row[0].lower() == 'wal'
    finally:
        await engine.stop()


# --- idempotency -------------------------------------------------------------


async def test_start_is_idempotent(tmp_path):
    """Calling start() twice does not re-create the table or error."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    try:
        # Second start is a no-op — same connection, no schema re-apply.
        await engine.start()
        # insert a marker row via the writer, reconnect to verify schema
        # still intact — no "table already exists" crash happened above.
        assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
        cursor = await engine._writer_db.execute(  # type: ignore[reportPrivateUsage]
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='cache_entries'"
        )
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        assert row[0] == 1
    finally:
        await engine.stop()


async def test_schema_init_noop_against_existing_db(tmp_path):
    """Starting twice against the same on-disk DB preserves existing data.

    The CREATE TABLE IF NOT EXISTS clause means existing rows survive
    worker restarts. We insert a row, stop, restart with a fresh engine,
    and verify the row is still there.
    """
    engine1 = await _make_engine(tmp_path, worker_id='w1')
    await engine1.start()
    assert engine1._writer_db is not None  # type: ignore[reportPrivateUsage]
    await engine1._writer_db.execute(  # type: ignore[reportPrivateUsage]
        """INSERT INTO cache_entries
           (key, scope, value, size_bytes, created_at_ms, updated_at_ms, expires_at_ms, origin_worker_id)
           VALUES ('pre', 'local', '"x"', 3, 1, 1, NULL, 'w1')"""
    )
    await engine1._writer_db.commit()  # type: ignore[reportPrivateUsage]
    await engine1.stop()

    # second engine instance against the same file — row should still be there
    engine2 = await _make_engine(tmp_path, worker_id='w1')
    await engine2.start()
    try:
        assert engine2._writer_db is not None  # type: ignore[reportPrivateUsage]
        cursor = await engine2._writer_db.execute(  # type: ignore[reportPrivateUsage]
            "SELECT value FROM cache_entries WHERE key='pre'"
        )
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        assert row[0] == '"x"'
    finally:
        await engine2.stop()


# --- stop ------------------------------------------------------------------


async def test_stop_closes_writer_connection(tmp_path):
    """stop() closes the writer connection and nulls it out for the
    `started` predicate."""
    engine = await _make_engine(tmp_path, worker_id='w1')
    await engine.start()
    assert engine._writer_db is not None  # type: ignore[reportPrivateUsage]
    await engine.stop()
    assert engine._writer_db is None  # type: ignore[reportPrivateUsage]


async def test_stop_without_start_is_safe(tmp_path):
    """Calling stop() on an unstarted engine does not error.

    Matters because app-shutdown paths may call stop() defensively in
    error branches; we must not raise when the engine never ran.
    """
    engine = await _make_engine(tmp_path, worker_id='w1')
    # no start() — just stop()
    await engine.stop()  # must not raise
    assert engine._writer_db is None  # type: ignore[reportPrivateUsage]


# --- disabled / memory-only storage -------------------------------------------


async def test_start_with_empty_storage_dir_disables_engine(tmp_path):
    """With ``storage.dir: ""`` (memory-only), start() logs a warning and
    marks the engine as effectively disabled (no file, no connection)."""
    engine = CacheEngine(
        config=CacheConfig(enabled=True),
        exchange=CacheExchangeConfig(),
        storage_dir='',
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    with capture_logs() as cap:
        await engine.start()
    try:
        # no writer connection opened
        assert engine._writer_db is None  # type: ignore[reportPrivateUsage]
        # no file created anywhere under tmp_path
        assert not (tmp_path / 'w1-cache.db').exists()
        assert any(e['event'] == 'cache_engine_disabled_no_storage_dir' for e in cap)
    finally:
        await engine.stop()


async def test_start_puts_the_cache_file_in_storage_dir(tmp_path):
    """The cache file lives in the worker's own folder, storage.dir."""
    storage = tmp_path / 'own'
    storage.mkdir()
    engine = CacheEngine(
        config=CacheConfig(enabled=True),
        exchange=CacheExchangeConfig(),
        storage_dir=str(storage),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert (storage / 'w1-cache.db').exists()
    finally:
        await engine.stop()


async def test_start_disabled_cache_does_not_open_connection(tmp_path):
    """When `cache.enabled=false`, start() is a no-op even with a storage folder."""
    engine = CacheEngine(
        config=CacheConfig(enabled=False),
        exchange=CacheExchangeConfig(),
        storage_dir=str(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert engine._writer_db is None  # type: ignore[reportPrivateUsage]
        assert not (tmp_path / 'w1-cache.db').exists()
    finally:
        await engine.stop()


# --- max_memory_entries=None warning at config load ------------------------


def test_cache_config_with_max_memory_entries_none_emits_warning():
    """When the operator explicitly opts into an unbounded memory dict
    (``max_memory_entries=None``), the config validator must emit a warning
    at load time so the intentional choice is visible in logs. The warning
    lives in the config layer (not the engine) so it fires once per process
    even if ``start()`` runs multiple times (tests, rotation, reinit)."""
    import structlog.testing

    with structlog.testing.capture_logs() as captured:
        # Construct the config directly — validators run on instantiation.
        CacheConfig(enabled=True, max_memory_entries=None)
    unbounded_events = [ev for ev in captured if ev.get('event') == 'cache_max_memory_entries_unbounded']
    assert len(unbounded_events) == 1
    assert unbounded_events[0]['log_level'] == 'warning'
    # ``worker_id`` should NOT appear — the warning fires at config load,
    # which is worker-agnostic; including a worker id would be misleading.
    assert 'worker_id' not in unbounded_events[0]


def test_cache_config_with_default_max_memory_entries_does_not_warn():
    """With the default cap of 10_000, the unbounded-memory warning must
    NOT fire — that warning is reserved for the explicit opt-in path."""
    import structlog.testing

    with structlog.testing.capture_logs() as captured:
        CacheConfig(enabled=True)  # default max_memory_entries=10_000
    unbounded_events = [ev for ev in captured if ev.get('event') == 'cache_max_memory_entries_unbounded']
    assert unbounded_events == []


def test_cache_config_disabled_with_none_does_not_warn():
    """When the cache is disabled the setting has no effect, so the warning
    should not fire — it would mislead operators into thinking the cache is
    using unbounded memory when in fact no cache exists."""
    import structlog.testing

    with structlog.testing.capture_logs() as captured:
        CacheConfig(enabled=False, max_memory_entries=None)
    unbounded_events = [ev for ev in captured if ev.get('event') == 'cache_max_memory_entries_unbounded']
    assert unbounded_events == []
