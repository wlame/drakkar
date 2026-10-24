"""The recorder memory store: its name, tables, open guard, read limit and recorder wiring."""

import asyncio
import sqlite3
import threading
import time

import pytest
from structlog.testing import capture_logs

from drakkar.recorder import EventRecorder
from drakkar.recorder import memstore as memstore_mod
from drakkar.recorder.memstore import (
    EVICT_BATCH_ROWS,
    MemoryCoverage,
    MemoryStore,
    MemoryStoreUnavailableError,
    arm_read_limit,
    delete_id_prefix,
    is_read_interrupted,
    is_transient_memory_error,
    memory_store_uri,
    next_store_seq,
)
from drakkar.recorder.schema import EVENT_COLUMNS
from tests.conftest import make_ui_config

INSTANCE = '0123456789abcdef'
ENDLESS_QUERY = 'WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c'
THREAD_JOIN_SECONDS = 5


@pytest.fixture
async def store():
    opened = MemoryStore(INSTANCE, store_config=True)
    await opened.open()
    try:
        yield opened
    finally:
        await opened.close()


async def _insert_event(store: MemoryStore, event: str = 'task_started') -> None:
    await store.writer.execute(
        'INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', (time.time(), '2026-11-01 00:00:00', event)
    )
    await store.writer.commit()


def _tables_seen_by_a_raw_open(uri: str) -> set[str]:
    """Tables of the database a plain by-name open finds (a new, empty one once the store is gone)."""
    conn = sqlite3.connect(uri, uri=True)
    try:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    finally:
        conn.close()


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def test_memory_store_uri_has_instance_seq_and_vfs():
    assert memory_store_uri(INSTANCE, 7) == 'file:/drakkar-recorder-0123456789abcdef-7?vfs=memdb'
    assert memory_store_uri(INSTANCE, 7, read_only=True) == (
        'file:/drakkar-recorder-0123456789abcdef-7?vfs=memdb&mode=ro'
    )


async def test_next_store_seq_increases_and_two_recorders_never_share_a_store():
    first = next_store_seq()
    assert next_store_seq() == first + 1

    one = MemoryStore(INSTANCE, store_config=False)
    two = MemoryStore(INSTANCE, store_config=False)
    assert one.uri != two.uri
    await one.open()
    await two.open()
    try:
        await _insert_event(one)
        _, rows = await two.read('SELECT count(*) FROM events')
        assert rows[0][0] == 0
    finally:
        await one.close()
        await two.close()


async def test_memory_store_journal_mode_is_memory_and_wal_is_refused(store):
    async with store.writer.execute('PRAGMA journal_mode=WAL') as cursor:
        row = await cursor.fetchone()
    assert row[0] == 'memory'


async def test_memory_store_has_events_and_worker_config_but_no_worker_state():
    with_config = MemoryStore(INSTANCE, store_config=True)
    without_config = MemoryStore(INSTANCE, store_config=False)
    await with_config.open()
    await without_config.open()
    try:
        _, rows = await with_config.read("SELECT name FROM sqlite_master WHERE type = 'table'")
        assert {'events', 'worker_config'} <= {row[0] for row in rows}
        assert 'worker_state' not in {row[0] for row in rows}
        _, rows = await without_config.read("SELECT name FROM sqlite_master WHERE type = 'table'")
        names = {row[0] for row in rows}
        assert 'events' in names
        assert 'worker_config' not in names
        assert 'worker_state' not in names
    finally:
        await with_config.close()
        await without_config.close()


async def test_memory_store_events_columns_match_the_pin(store):
    _, rows = await store.read('PRAGMA table_info(events)')
    assert tuple(row[1] for row in rows) == EVENT_COLUMNS
    assert len(EVENT_COLUMNS) == 20


async def test_memory_store_read_returns_column_names_and_rows(store):
    await _insert_event(store, 'task_completed')
    columns, rows = await store.read('SELECT id, event FROM events WHERE event = ?', ('task_completed',))
    assert columns == ['id', 'event']
    assert [tuple(row) for row in rows] == [(1, 'task_completed')]


async def test_memory_store_byte_numbers_follow_the_pages(store):
    used = await store.used_bytes()
    allocated = await store.allocated_bytes()
    assert 0 < used <= allocated
    for _ in range(50):
        await _insert_event(store)
    assert await store.allocated_bytes() >= allocated


async def test_store_survives_the_reader_close_and_is_gone_after_close():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    await _insert_event(store)
    await store.reader.close()

    def count_rows() -> int:
        with store.open_by_name() as conn:
            assert conn is not None
            return conn.execute('SELECT count(*) FROM events').fetchone()[0]

    assert await asyncio.to_thread(count_rows) == 1
    await store.close()
    assert 'events' not in _tables_seen_by_a_raw_open(store.uri)


async def test_open_by_name_after_close_returns_none():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    await store.close()
    with store.open_by_name() as conn:
        assert conn is None
    assert not store.is_open


def test_open_by_name_before_open_returns_none():
    store = MemoryStore(INSTANCE, store_config=False)
    with store.open_by_name() as conn:
        assert conn is None


async def test_open_by_name_connection_reads_and_refuses_writes(store):
    await _insert_event(store)

    def use() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            assert conn.execute('PRAGMA busy_timeout').fetchone()[0] == 5000
            assert conn.execute('SELECT count(*) FROM events').fetchone()[0] == 1
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO events (ts, dt, event) VALUES (1, 'x', 'y')")

    await asyncio.to_thread(use)


async def test_close_waits_for_a_counted_reader():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    entered = threading.Event()

    def hold() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            entered.set()
            time.sleep(0.2)

    holder = threading.Thread(target=hold)
    holder.start()
    assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
    started = time.monotonic()
    await store.close()
    waited = time.monotonic() - started
    holder.join(THREAD_JOIN_SECONDS)
    assert waited >= 0.1
    assert not holder.is_alive()


async def test_close_gives_up_after_the_wait_limit(monkeypatch):
    monkeypatch.setattr(memstore_mod, 'MEMORY_CLOSE_WAIT_SECONDS', 0.1)
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            entered.set()
            release.wait(THREAD_JOIN_SECONDS)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
        started = time.monotonic()
        await store.close()
        assert time.monotonic() - started < 1.0
    finally:
        release.set()
        holder.join(THREAD_JOIN_SECONDS)
    assert 'events' not in _tables_seen_by_a_raw_open(store.uri)


async def test_counted_reader_in_flight_at_close_finishes_and_store_is_not_recreated():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    await _insert_event(store)
    entered = threading.Event()
    closing = threading.Event()
    seen: dict = {}

    def read_during_close() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            entered.set()
            closing.wait(THREAD_JOIN_SECONDS)
            seen['rows'] = conn.execute('SELECT count(*) FROM events').fetchone()[0]

    reader = threading.Thread(target=read_during_close)
    reader.start()
    assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
    close = asyncio.create_task(store.close())
    await asyncio.sleep(0.05)
    assert not close.done()
    closing.set()
    await close
    reader.join(THREAD_JOIN_SECONDS)

    assert seen['rows'] == 1
    with store.open_by_name() as conn:
        assert conn is None
    assert 'events' not in _tables_seen_by_a_raw_open(store.uri)


async def test_missing_memdb_vfs_fails_startup_with_the_text(monkeypatch):
    async def no_memdb(database, **kwargs):
        raise sqlite3.OperationalError('no such vfs: memdb')

    monkeypatch.setattr(memstore_mod, 'open_connection', no_memdb)
    store = MemoryStore(INSTANCE, store_config=False)
    with pytest.raises(MemoryStoreUnavailableError) as raised:
        await store.open()
    assert str(raised.value) == (
        'recorder memory store: this SQLite library has no memdb VFS; '
        'SQLite 3.36 or later built with deserialize support is required'
    )
    await store.close()


async def test_other_open_errors_pass_through(monkeypatch):
    async def broken(database, **kwargs):
        raise sqlite3.OperationalError('unable to open database file')

    monkeypatch.setattr(memstore_mod, 'open_connection', broken)
    with pytest.raises(sqlite3.OperationalError, match='unable to open'):
        await MemoryStore(INSTANCE, store_config=False).open()


async def test_a_failed_schema_closes_the_writer(monkeypatch):
    monkeypatch.setattr(memstore_mod, 'SCHEMA_EVENTS', 'CREATE TABLE broken (')
    store = MemoryStore(INSTANCE, store_config=False)
    with pytest.raises(sqlite3.OperationalError):
        await store.open()
    assert not store.is_open
    with pytest.raises(RuntimeError, match='not open'):
        _ = store.writer
    with pytest.raises(RuntimeError, match='not open'):
        _ = store.reader


async def test_slow_memory_read_stops_at_the_limit_and_a_write_then_commits(monkeypatch, store):
    monkeypatch.setattr(memstore_mod, 'MEMORY_READ_LIMIT_SECONDS', 0.05)

    started = time.monotonic()
    with pytest.raises(sqlite3.OperationalError) as raised:
        await store.read(ENDLESS_QUERY)
    assert is_read_interrupted(raised.value)
    assert time.monotonic() - started < 1.0

    def slow_by_name_read() -> sqlite3.Error:
        with store.open_by_name() as conn:
            assert conn is not None
            arm_read_limit(conn)
            try:
                conn.execute(ENDLESS_QUERY).fetchone()
            except sqlite3.OperationalError as exc:
                return exc
        raise AssertionError('the read was not stopped')

    assert is_read_interrupted(await asyncio.to_thread(slow_by_name_read))

    await _insert_event(store)
    _, rows = await store.read('SELECT count(*) FROM events')
    assert rows[0][0] == 1


@pytest.mark.parametrize(
    ('code', 'transient'), [(5, True), (6, True), (517, True), (262, True), (13, False), (1, False)]
)
def test_is_transient_memory_error_classes(code, transient):
    exc = sqlite3.OperationalError('x')
    exc.sqlite_errorcode = code
    assert is_transient_memory_error(exc) is transient


def test_errors_without_a_code_are_neither_transient_nor_interrupted():
    exc = sqlite3.OperationalError('x')
    assert not is_transient_memory_error(exc)
    assert not is_read_interrupted(exc)


@pytest.mark.parametrize(
    ('coverage', 'rows'),
    [
        (MemoryCoverage(1, 0, False, 0.0), 0),
        (MemoryCoverage(11, 10, True, 5.0), 0),
        (MemoryCoverage(4, 10, True, 5.0), 7),
    ],
)
def test_memory_coverage_rows(coverage, rows):
    assert coverage.rows == rows


async def test_no_memory_store_with_store_events_false(tmp_path):
    rec = EventRecorder(
        make_ui_config(store_events=False), worker_name='w-1', persistence='sqlite', storage_dir=str(tmp_path)
    )
    await rec.start()
    try:
        assert rec.memory_reader_db is None
        with rec.open_memory_reader() as conn:
            assert conn is None
    finally:
        await rec.stop()


async def test_recorder_opens_the_memory_store_and_closes_it_at_stop():
    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='none')
    assert rec.memory_reader_db is None
    await rec.start()
    stopped = False
    try:
        assert rec.memory_reader_db is not None

        def tables() -> set[str]:
            with rec.open_memory_reader() as conn:
                assert conn is not None
                return _tables(conn)

        assert {'events', 'worker_config'} <= await asyncio.to_thread(tables)
        store = rec._memory_store
        await rec.stop()
        stopped = True
        assert rec.memory_reader_db is None
        with rec.open_memory_reader() as conn:
            assert conn is None
        assert 'events' not in _tables_seen_by_a_raw_open(store.uri)
    finally:
        if not stopped:
            await rec.stop()


async def test_a_failed_file_open_closes_the_memory_store(tmp_path, monkeypatch):
    def refuse(path):
        raise PermissionError('storage folder is read-only')

    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='sqlite', storage_dir=str(tmp_path))
    monkeypatch.setattr('drakkar.recorder.core.secure_db_file', refuse)
    with pytest.raises(PermissionError):
        await rec.start()
    assert rec._memory_store is None
    assert rec.memory_reader_db is None


async def test_a_missing_memdb_fails_the_recorder_start(monkeypatch):
    async def no_memdb(database, **kwargs):
        raise sqlite3.OperationalError('no such vfs: memdb')

    monkeypatch.setattr(memstore_mod, 'open_connection', no_memdb)
    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='none')
    with pytest.raises(MemoryStoreUnavailableError):
        await rec.start()
    assert rec._memory_store is None


async def _started_line(rec: EventRecorder) -> dict:
    with capture_logs() as captured:
        await rec.start()
    try:
        return next(line for line in captured if line['event'] == 'recorder_started')
    finally:
        await rec.stop()


async def test_recorder_started_logs_the_memory_fields(tmp_path):
    sqlite_line = await _started_line(
        EventRecorder(make_ui_config(), worker_name='w-1', persistence='sqlite', storage_dir=str(tmp_path))
    )
    assert sqlite_line['persistence'] == 'sqlite'
    assert sqlite_line['memory_window_seconds'] == 120
    assert sqlite_line['memory_max_rows'] == 3000
    assert sqlite_line['memory_max_bytes'] == 268435456
    assert 'ignored_keys' not in sqlite_line

    none_line = await _started_line(
        EventRecorder(
            make_ui_config(store_state=True),
            worker_name='w-1',
            persistence='none',
            memory_window_seconds=60,
            memory_max_rows=800,
            memory_max_bytes=16777216,
        )
    )
    assert none_line['db_path'] == '(memory only)'
    assert none_line['persistence'] == 'none'
    assert none_line['memory_window_seconds'] == 60
    assert none_line['memory_max_rows'] == 800
    assert none_line['memory_max_bytes'] == 16777216
    assert none_line['ignored_keys'] == ['rotation_interval_hours', 'store_state']

    plain_none_line = await _started_line(
        EventRecorder(make_ui_config(store_state=False), worker_name='w-1', persistence='none')
    )
    assert plain_none_line['ignored_keys'] == ['rotation_interval_hours']

    no_events_line = await _started_line(
        EventRecorder(make_ui_config(store_events=False), worker_name='w-1', persistence='none')
    )
    assert no_events_line['persistence'] == 'none'
    for key in ('memory_window_seconds', 'memory_max_rows', 'memory_max_bytes'):
        assert key not in no_events_line


async def test_cancelled_close_still_closes_both_connections():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    reader, writer = store.reader, store.writer
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            entered.set()
            release.wait(THREAD_JOIN_SECONDS)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
        close = asyncio.create_task(store.close())
        await asyncio.sleep(0.05)
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        assert reader._connection is None
        assert writer._connection is None
        assert not store.is_open
        with store.open_by_name() as conn:
            assert conn is None
    finally:
        release.set()
        holder.join(THREAD_JOIN_SECONDS)


async def test_close_with_a_zero_wait_does_not_wait_for_a_counted_reader():
    store = MemoryStore(INSTANCE, store_config=False)
    await store.open()
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with store.open_by_name() as conn:
            assert conn is not None
            entered.set()
            release.wait(THREAD_JOIN_SECONDS)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
        started = time.monotonic()
        await store.close(wait_seconds=-1.0)
        assert time.monotonic() - started < 0.5
    finally:
        release.set()
        holder.join(THREAD_JOIN_SECONDS)


async def test_stop_with_almost_no_time_left_does_not_wait_for_a_counted_reader():
    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='none')
    await rec.start()
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with rec.open_memory_reader() as conn:
            assert conn is not None
            entered.set()
            release.wait(THREAD_JOIN_SECONDS)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert await asyncio.to_thread(entered.wait, THREAD_JOIN_SECONDS)
        started = time.monotonic()
        await rec.stop(timeout=0.05)
        assert time.monotonic() - started < 1.0
        assert rec.memory_store is None
    finally:
        release.set()
        holder.join(THREAD_JOIN_SECONDS)


async def test_memory_store_property_follows_the_recorder_lifetime():
    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='none')
    assert rec.memory_store is None
    await rec.start()
    try:
        assert isinstance(rec.memory_store, MemoryStore)
        assert rec.memory_store.is_open
    finally:
        await rec.stop()
    assert rec.memory_store is None


async def test_closing_the_memory_store_waits_for_a_memory_stage_in_flight():
    rec = EventRecorder(make_ui_config(), worker_name='w-1', persistence='none')
    await rec.start()
    store = rec.memory_store
    # A memory stage in flight holds the memory lock.
    await rec._memory_lock.acquire()
    holds_lock = True
    try:
        closing = asyncio.create_task(rec._close_memory_store())
        await asyncio.sleep(0.05)
        assert rec.memory_store is store
        assert store.is_open
        rec._memory_lock.release()
        holds_lock = False
        await asyncio.wait_for(closing, timeout=5)
        assert rec.memory_store is None
        assert not store.is_open
    finally:
        if holds_lock:
            rec._memory_lock.release()
        await rec.stop()


async def _fill_events(store: MemoryStore, count: int) -> None:
    await store.writer.executemany(
        'INSERT INTO events (id, ts, dt, event) VALUES (?, ?, ?, ?)',
        [(row_id, float(row_id), '2026-11-01 00:00:00', 'consumed') for row_id in range(1, count + 1)],
    )
    await store.writer.commit()


async def test_delete_id_prefix_deletes_in_batches_and_reports_the_largest_ts(store):
    await _fill_events(store, 12_000)

    result = await delete_id_prefix(store.writer, 11_001)

    assert (result.deleted, result.max_ts, result.error) == (11_000, 11_000.0, None)
    _, rows = await store.read('SELECT MIN(id), count(*) FROM events')
    assert rows == [(11_001, 1000)]


async def test_delete_id_prefix_keeps_committed_batches_when_a_later_one_fails(store, monkeypatch):
    await _fill_events(store, 12_000)
    writer = store.writer
    real_execute = writer.execute
    deletes = []

    def execute(sql, params=()):
        if sql.startswith('DELETE'):
            deletes.append(params)
            if len(deletes) == 2:
                raise sqlite3.OperationalError('database or disk is full')
        return real_execute(sql, params)

    monkeypatch.setattr(writer, 'execute', execute)

    result = await delete_id_prefix(writer, 12_001)

    assert result.deleted == EVICT_BATCH_ROWS
    assert result.max_ts == float(EVICT_BATCH_ROWS)
    assert isinstance(result.error, sqlite3.OperationalError)
    _, rows = await store.read('SELECT MIN(id) FROM events')
    assert rows == [(EVICT_BATCH_ROWS + 1,)]


async def test_delete_id_prefix_reports_a_failed_rollback_and_counts_no_failed_batch(store, monkeypatch):
    await _fill_events(store, 10)
    writer = store.writer
    real_execute = writer.execute

    def execute(sql, params=()):
        if sql.startswith('DELETE'):
            raise sqlite3.OperationalError('database or disk is full')
        return real_execute(sql, params)

    async def failing_rollback():
        raise sqlite3.OperationalError('cannot roll back')

    monkeypatch.setattr(writer, 'execute', execute)
    monkeypatch.setattr(writer, 'rollback', failing_rollback)

    result = await delete_id_prefix(writer, 11)

    assert result.deleted == 0
    assert str(result.error) == 'database or disk is full'
    assert str(result.rollback_error) == 'cannot roll back'
