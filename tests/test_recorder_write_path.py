"""The recorder write path: row ids, the intake, the file queue, the two stages and their failure rules."""

import asyncio
import os
import sqlite3
import threading
import time
from unittest.mock import AsyncMock

import pytest
from structlog.testing import capture_logs

from drakkar.metrics import (
    recorder_buffer_size,
    recorder_dropped_events,
    recorder_memory_evicted_rows,
    recorder_memory_write_failures,
)
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder.memstore import EVICT_MIN_INTERVAL_SECONDS, MemoryCoverage, PrefixDeletion
from tests.conftest import kafka_connections, make_ui_config

WORKER = 'w-1'
SQLITE_BUSY = 5
SQLITE_FULL = 13
SMALL_CHUNK_ROWS = 4
WAIT_SECONDS = 5.0


@pytest.fixture
def unique_db_paths(monkeypatch):
    """Give every recorder file its own name; the real name has a one-second stamp."""
    real_make_db_path = core_mod.make_db_path
    counter = iter(range(1, 100))
    monkeypatch.setattr(
        core_mod,
        'make_db_path',
        lambda db_dir, worker_name: real_make_db_path(db_dir, f'{worker_name}-r{next(counter)}'),
    )


def _recorder(persistence: str, *, storage_dir: str = '', **ui) -> EventRecorder:
    settings = {'enabled': True, 'store_output': False, 'flush_interval_seconds': 60}
    settings.update(ui)
    return EventRecorder(
        make_ui_config(**settings),
        worker_name=WORKER,
        cluster_name='c',
        persistence=persistence,
        storage_dir=storage_dir,
    )


@pytest.fixture
async def none_recorder():
    rec = _recorder('none')
    await rec.start()
    yield rec
    await rec.stop()


@pytest.fixture
async def sqlite_recorder(tmp_path, unique_db_paths):
    rec = _recorder('sqlite', storage_dir=str(tmp_path))
    await rec.start()
    yield rec
    await rec.stop()


def _event(offset: int, ts: float | None = None) -> dict:
    return {'ts': time.time() if ts is None else ts, 'event': 'consumed', 'partition': 0, 'offset': offset}


def _record_many(rec: EventRecorder, count: int, *, first_offset: int = 0) -> None:
    for offset in range(first_offset, first_offset + count):
        rec._record(_event(offset))


async def _memory_rows(rec: EventRecorder) -> list[tuple[int, int]]:
    _, rows = await rec.memory_store.read('SELECT id, offset FROM events ORDER BY id')
    return [(row[0], row[1]) for row in rows]


async def _memory_ids(rec: EventRecorder) -> list[int]:
    return [row_id for row_id, _ in await _memory_rows(rec)]


def _file_rows(path: str) -> list[tuple[int, int]]:
    conn = sqlite3.connect(path)
    try:
        return [(row[0], row[1]) for row in conn.execute('SELECT id, offset FROM events ORDER BY id')]
    finally:
        conn.close()


def _file_ids(path: str) -> list[int]:
    return [row_id for row_id, _ in _file_rows(path)]


def _sqlite_error(code: int) -> sqlite3.OperationalError:
    exc = sqlite3.OperationalError('database is locked' if code == SQLITE_BUSY else 'database or disk is full')
    exc.sqlite_errorcode = code
    return exc


def _fail_memory_writes(monkeypatch, rec: EventRecorder, failures: dict[int, int]) -> list[int]:
    """Make the memory writer's ``executemany`` raise on the given call numbers (1-based) with the given codes."""
    writer = rec.memory_store.writer
    real_executemany = writer.executemany
    calls: list[int] = []

    async def executemany(sql, rows):
        calls.append(len(calls) + 1)
        code = failures.get(len(calls))
        if code is not None:
            raise _sqlite_error(code)
        return await real_executemany(sql, rows)

    monkeypatch.setattr(writer, 'executemany', executemany)
    return calls


def _block_writes(monkeypatch, connection, gate: asyncio.Event) -> asyncio.Event:
    """Make ``connection.executemany`` wait for ``gate``; the returned event is set once a write waits."""
    real_executemany = connection.executemany
    waiting = asyncio.Event()

    async def executemany(sql, rows):
        waiting.set()
        await gate.wait()
        return await real_executemany(sql, rows)

    monkeypatch.setattr(connection, 'executemany', executemany)
    return waiting


def _fake_clock_log(rec: EventRecorder) -> list[float]:
    clock = [1000.0]
    rec._memory_log = RateLimitedLog(lambda: clock[0])
    return clock


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


# --- ids ---


async def test_ids_are_assigned_at_pop_in_pop_order_once(none_recorder):
    rec = none_recorder

    def record_from_thread(first_offset: int) -> None:
        _record_many(rec, 50, first_offset=first_offset)

    threads = [threading.Thread(target=record_from_thread, args=(first,)) for first in (0, 1000)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    pop_order = [event['offset'] for event in rec._intake]

    await rec.flush_memory()
    await rec.flush_memory()

    rows = await _memory_rows(rec)
    assert [row_id for row_id, _ in rows] == list(range(1, 101))
    assert [offset for _, offset in rows] == pop_order
    assert rec.memory_coverage().last_id == 100


async def test_ids_continue_across_a_rotation(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush()
    first_path = rec.db_path

    await rec._rotate()
    _record_many(rec, 2, first_offset=3)
    await rec.flush()

    assert _file_ids(first_path) == [1, 2, 3]
    assert _file_ids(rec.db_path) == [4, 5]


async def test_memory_and_file_hold_the_same_ids_for_the_same_rows(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 5)

    await rec.flush()

    assert await _memory_rows(rec) == _file_rows(rec.db_path)
    assert len(_file_rows(rec.db_path)) == 5


async def test_event_dict_has_no_id_after_a_flush(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    broadcast: list[dict] = []
    monkeypatch.setattr(rec._fanout, 'broadcast', broadcast.append)
    event = _event(7)

    rec._record(event)
    await rec.flush()

    assert broadcast[0] is event
    assert 'id' not in event
    assert await _memory_ids(rec) == [1]


async def test_rows_recorded_with_skip_db_get_no_id(none_recorder):
    rec = none_recorder
    rec._record(_event(1), skip_db=True)
    rec._record(_event(2))

    await rec.flush_memory()

    assert await _memory_rows(rec) == [(1, 2)]
    assert rec.memory_coverage().last_id == 1


async def test_requeued_pair_keeps_its_id(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush_memory()
    real_executemany = rec._db.executemany
    monkeypatch.setattr(rec._db, 'executemany', AsyncMock(side_effect=sqlite3.OperationalError('disk I/O error')))

    await rec._stage_file()

    assert [row_id for row_id, _ in rec._file_queue] == [1, 2, 3]
    monkeypatch.setattr(rec._db, 'executemany', real_executemany)
    await rec._stage_file()
    assert _file_ids(rec.db_path) == [1, 2, 3]


# --- stages and loops ---


async def test_hanging_file_stage_does_not_stop_the_memory_stage(tmp_path, unique_db_paths, monkeypatch):
    rec = _recorder('sqlite', storage_dir=str(tmp_path), flush_interval_seconds=1, max_buffer=1000)
    await rec.start()
    gate = asyncio.Event()
    try:
        file_waiting = _block_writes(monkeypatch, rec._db, gate)
        _record_many(rec, 600)
        await asyncio.wait_for(file_waiting.wait(), WAIT_SECONDS)

        _record_many(rec, 600, first_offset=600)
        deadline = time.monotonic() + WAIT_SECONDS
        while rec.memory_coverage().last_id < 1200 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

        assert await _memory_ids(rec) == list(range(1, 1201))
        assert _file_ids(rec.db_path) == []
    finally:
        gate.set()
        await rec.stop()


async def test_flush_memory_runs_stage_m_only(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 3)

    await rec.flush_memory()

    assert await _memory_ids(rec) == [1, 2, 3]
    assert [row_id for row_id, _ in rec._file_queue] == [1, 2, 3]
    assert _file_ids(rec.db_path) == []


async def test_flush_runs_stage_m_then_stage_f(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 3)

    await rec.flush()

    assert await _memory_ids(rec) == [1, 2, 3]
    assert len(rec._file_queue) == 0
    assert _file_ids(rec.db_path) == [1, 2, 3]


async def test_cancelled_ui_read_does_not_cancel_a_stage(none_recorder, monkeypatch):
    rec = none_recorder
    gate = asyncio.Event()
    waiting = _block_writes(monkeypatch, rec.memory_store.writer, gate)
    _record_many(rec, 3)

    read = asyncio.create_task(rec.flush_memory())
    await asyncio.wait_for(waiting.wait(), WAIT_SECONDS)
    read.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read
    gate.set()
    async with rec._memory_lock:
        pass

    assert await _memory_ids(rec) == [1, 2, 3]
    assert len(rec._intake) == 0


async def test_lock_order_memory_lock_never_inside_flush_lock(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush_memory()
    gate = asyncio.Event()
    file_waiting = _block_writes(monkeypatch, rec._db, gate)
    stalled_file_stage = asyncio.create_task(rec._stage_file())
    await asyncio.wait_for(file_waiting.wait(), WAIT_SECONDS)

    _record_many(rec, 2, first_offset=3)
    flush = asyncio.create_task(rec.flush())
    deadline = time.monotonic() + WAIT_SECONDS
    while rec.memory_coverage().last_id < 5 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)

    assert await _memory_ids(rec) == [1, 2, 3, 4, 5]
    assert not flush.done()
    gate.set()
    await asyncio.wait_for(asyncio.gather(stalled_file_stage, flush), WAIT_SECONDS)
    assert _file_ids(rec.db_path) == [1, 2, 3, 4, 5]


async def test_file_queue_overflow_counts_dropped_events(tmp_path, unique_db_paths):
    rec = _recorder('sqlite', storage_dir=str(tmp_path), max_buffer=1000)
    await rec.start()
    try:
        before = recorder_dropped_events._value.get()
        _record_many(rec, 600)
        await rec.flush_memory()
        _record_many(rec, 600, first_offset=600)
        await rec.flush_memory()

        assert len(rec._file_queue) == 1000
        assert rec._file_queue[0][0] == 201
        assert recorder_dropped_events._value.get() - before == 200
    finally:
        await rec.stop()


async def test_buffer_size_gauge_reports_intake_plus_file_queue(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 5)
    await rec.flush_memory()

    _record_many(rec, 2, first_offset=5)

    assert recorder_buffer_size._value.get() == 7


# --- failure matrix ---


async def test_file_failure_keeps_rows_in_memory_and_retries_without_duplicates(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush_memory()
    real_commit = rec._db.commit
    monkeypatch.setattr(rec._db, 'commit', AsyncMock(side_effect=sqlite3.OperationalError('disk I/O error')))

    await rec._stage_file()

    assert await _memory_ids(rec) == [1, 2, 3]
    assert len(rec._file_queue) == 3
    monkeypatch.setattr(rec._db, 'commit', real_commit)
    await rec._stage_file()
    assert _file_ids(rec.db_path) == [1, 2, 3]


async def test_busy_memory_write_retries_on_next_tick_and_drops_nothing(none_recorder, monkeypatch):
    rec = none_recorder
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_BUSY})
    failures_before = recorder_memory_write_failures._value.get()
    _record_many(rec, 3)

    with capture_logs() as captured:
        await rec.flush_memory()

    assert [event['offset'] for event in rec._intake] == [0, 1, 2]
    assert rec._next_id == 1
    retries = _logs(captured, 'recorder_memory_write_retry')
    assert len(retries) == 1
    assert retries[0]['batch_size'] == 3
    assert retries[0]['suppressed'] == 0
    await rec.flush_memory()
    assert await _memory_ids(rec) == [1, 2, 3]
    assert recorder_memory_write_failures._value.get() == failures_before


async def test_transient_failure_on_second_chunk_gives_back_only_its_ids(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', SMALL_CHUNK_ROWS)
    _fail_memory_writes(monkeypatch, rec, {2: SQLITE_BUSY})
    _record_many(rec, 10)

    await rec.flush_memory()

    assert await _memory_ids(rec) == [1, 2, 3, 4]
    assert [row_id for row_id, _ in rec._file_queue] == [1, 2, 3, 4]
    assert len(rec._intake) == 6
    assert rec._next_id == 5
    await rec.flush_memory()
    assert await _memory_ids(rec) == list(range(1, 11))
    assert [row_id for row_id, _ in rec._file_queue] == list(range(1, 11))


async def test_transient_failure_skips_the_eviction_pass(none_recorder, monkeypatch):
    rec = none_recorder
    evict = AsyncMock()
    monkeypatch.setattr(rec, '_evict', evict)
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_BUSY})
    _record_many(rec, 3)

    await rec.flush_memory()
    evict.assert_not_awaited()

    await rec.flush_memory()
    evict.assert_awaited_once_with(EVICT_MIN_INTERVAL_SECONDS)


async def test_final_memory_error_with_sqlite_writes_file_and_deletes_memory_up_to_the_chunk(
    sqlite_recorder, monkeypatch
):
    rec = sqlite_recorder
    clock = _fake_clock_log(rec)
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', SMALL_CHUNK_ROWS)
    _fail_memory_writes(monkeypatch, rec, {2: SQLITE_FULL, 4: SQLITE_FULL, 5: SQLITE_FULL})
    base_ts = time.time() - 30
    for offset in range(10):
        rec._record(_event(offset, ts=base_ts + offset))
    gap_before = recorder_memory_evicted_rows.labels(reason='gap')._value.get()
    failures_before = recorder_memory_write_failures._value.get()

    with capture_logs() as captured:
        await rec.flush_memory()

    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == [9, 10]
    assert coverage.rows == len(await _memory_ids(rec))
    assert (coverage.floor_id, coverage.last_id, coverage.evicted) == (9, 10, True)
    assert coverage.evicted_max_ts == base_ts + 7
    assert recorder_memory_evicted_rows.labels(reason='gap')._value.get() - gap_before == 4
    assert recorder_memory_write_failures._value.get() - failures_before == 1
    assert [row_id for row_id, _ in rec._file_queue] == list(range(1, 11))
    lines = _logs(captured, 'recorder_memory_write_failed')
    assert len(lines) == 1
    assert lines[0]['batch_size'] == SMALL_CHUNK_ROWS
    assert lines[0]['suppressed'] == 0

    _record_many(rec, 1, first_offset=10)
    with capture_logs() as captured:
        await rec.flush_memory()
    assert _logs(captured, 'recorder_memory_write_failed') == []

    clock[0] += 61
    _record_many(rec, 1, first_offset=11)
    with capture_logs() as captured:
        await rec.flush_memory()
    lines = _logs(captured, 'recorder_memory_write_failed')
    assert len(lines) == 1
    assert lines[0]['suppressed'] == 1
    assert recorder_memory_write_failures._value.get() - failures_before == 3

    await rec._stage_file()
    assert _file_ids(rec.db_path) == list(range(1, 13))


async def test_final_memory_error_with_none_loses_the_chunk_and_moves_the_floor(none_recorder, monkeypatch):
    rec = none_recorder
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', SMALL_CHUNK_ROWS)
    _fail_memory_writes(monkeypatch, rec, {2: SQLITE_FULL})
    _record_many(rec, 10)

    await rec.flush_memory()

    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == [9, 10]
    assert (coverage.floor_id, coverage.last_id, coverage.evicted) == (9, 10, True)
    assert len(rec._file_queue) == 0
    assert len(rec._intake) == 0


async def test_empty_table_after_a_gap_has_floor_last_plus_one(none_recorder, monkeypatch):
    rec = none_recorder
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_FULL})
    _record_many(rec, 3)

    await rec.flush_memory()

    coverage = rec.memory_coverage()
    assert (coverage.floor_id, coverage.last_id, coverage.rows, coverage.evicted) == (4, 3, 0, True)
    assert await _memory_ids(rec) == []


async def test_failed_gap_delete_stays_pending_and_the_next_stage_deletes_it(none_recorder, monkeypatch):
    rec = none_recorder
    _record_many(rec, 3)
    await rec.flush_memory()
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_FULL})
    real_delete = core_mod.delete_id_prefix
    attempts: list[int] = []

    async def failing_once(conn, below_id, *, on_batch=None):
        attempts.append(below_id)
        if len(attempts) == 1:
            return PrefixDeletion(0, None, _sqlite_error(SQLITE_FULL))
        return await real_delete(conn, below_id, on_batch=on_batch)

    monkeypatch.setattr(core_mod, 'delete_id_prefix', failing_once)
    gap_before = recorder_memory_evicted_rows.labels(reason='gap')._value.get()
    _record_many(rec, 2, first_offset=3)

    with capture_logs() as captured:
        await rec.flush_memory()

    lines = _logs(captured, 'recorder_memory_eviction_failed')
    assert [(line['reason'], line['suppressed']) for line in lines] == [('gap', 0)]
    assert await _memory_ids(rec) == [1, 2, 3]
    coverage = rec.memory_coverage()
    assert (coverage.floor_id, coverage.last_id) == (6, 5)

    await rec.flush_memory()

    assert attempts == [6, 6]
    assert await _memory_ids(rec) == []
    assert recorder_memory_evicted_rows.labels(reason='gap')._value.get() - gap_before == 3


async def test_failed_rollback_is_logged_and_retried_by_the_next_stage(none_recorder, monkeypatch):
    rec = none_recorder
    writer = rec.memory_store.writer
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_BUSY})
    real_rollback = writer.rollback
    rollbacks: list[int] = []

    async def rollback():
        rollbacks.append(1)
        if len(rollbacks) == 1:
            raise sqlite3.OperationalError('cannot rollback - no transaction is active')
        return await real_rollback()

    monkeypatch.setattr(writer, 'rollback', rollback)
    _record_many(rec, 2)

    with capture_logs() as captured:
        await rec.flush_memory()
    await rec.flush_memory()

    assert len(_logs(captured, 'recorder_memory_rollback_failed')) == 1
    assert len(rollbacks) == 2
    assert await _memory_ids(rec) == [1, 2]


async def test_final_error_with_a_failed_gap_delete_stops_the_stage_and_keeps_one_range(none_recorder, monkeypatch):
    rec = none_recorder
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', SMALL_CHUNK_ROWS)
    _fail_memory_writes(monkeypatch, rec, {2: SQLITE_FULL})
    real_delete = core_mod.delete_id_prefix
    attempts: list[int] = []

    async def failing_once(conn, below_id, *, on_batch=None):
        attempts.append(below_id)
        if len(attempts) == 1:
            return PrefixDeletion(0, None, _sqlite_error(SQLITE_FULL))
        return await real_delete(conn, below_id, on_batch=on_batch)

    monkeypatch.setattr(core_mod, 'delete_id_prefix', failing_once)
    _record_many(rec, 10)

    await rec.flush_memory()

    assert await _memory_ids(rec) == [1, 2, 3, 4]
    assert [event['offset'] for event in rec._intake] == [8, 9]
    assert rec._next_id == 9

    await rec.flush_memory()

    assert attempts == [9, 9]
    assert await _memory_ids(rec) == [9, 10]
    coverage = rec.memory_coverage()
    assert (coverage.floor_id, coverage.last_id) == (9, 10)


async def test_gap_delete_with_a_failed_rollback_logs_it_and_the_next_stage_rolls_back_first(
    none_recorder, monkeypatch
):
    rec = none_recorder
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_FULL})
    real_delete = core_mod.delete_id_prefix
    calls: list[str] = []
    deletes: list[int] = []

    async def failing_once(conn, below_id, *, on_batch=None):
        calls.append('delete')
        deletes.append(below_id)
        if len(deletes) == 1:
            return PrefixDeletion(0, None, _sqlite_error(SQLITE_FULL), sqlite3.OperationalError('rollback failed'))
        return await real_delete(conn, below_id, on_batch=on_batch)

    writer = rec.memory_store.writer
    real_rollback = writer.rollback

    async def rollback():
        calls.append('rollback')
        return await real_rollback()

    monkeypatch.setattr(core_mod, 'delete_id_prefix', failing_once)
    monkeypatch.setattr(writer, 'rollback', rollback)
    _record_many(rec, 2)

    with capture_logs() as captured:
        await rec.flush_memory()

    lines = _logs(captured, 'recorder_memory_eviction_failed')
    assert [(line['reason'], line['rollback_error']) for line in lines] == [('gap', 'rollback failed')]
    assert rec._memory_dirty
    calls.clear()

    _record_many(rec, 1, first_offset=2)
    await rec.flush_memory()

    assert calls == ['rollback', 'delete']
    assert await _memory_ids(rec) == [3]


async def test_repeated_rollback_failures_log_once_per_minute_with_the_suppressed_count(none_recorder, monkeypatch):
    rec = none_recorder
    clock = _fake_clock_log(rec)
    writer = rec.memory_store.writer
    _fail_memory_writes(monkeypatch, rec, {1: SQLITE_BUSY})
    real_rollback = writer.rollback
    monkeypatch.setattr(writer, 'rollback', AsyncMock(side_effect=sqlite3.OperationalError('cannot roll back')))
    _record_many(rec, 2)

    with capture_logs() as captured:
        for _ in range(3):
            await rec.flush_memory()
        clock[0] += 61
        await rec.flush_memory()

    lines = _logs(captured, 'recorder_memory_rollback_failed')
    assert [(line['log_level'], line['suppressed']) for line in lines] == [('warning', 0), ('warning', 2)]
    monkeypatch.setattr(writer, 'rollback', real_rollback)
    await rec.flush_memory()
    assert await _memory_ids(rec) == [1, 2]


async def test_stage_m_chunk_cancelled_by_stop_with_none_is_written_by_the_final_stage(monkeypatch):
    rec = _recorder('none', flush_interval_seconds=1, max_buffer=1000)
    await rec.start()
    gate = asyncio.Event()
    waiting = _block_writes(monkeypatch, rec.memory_store.writer, gate)
    _record_many(rec, 600)
    await asyncio.wait_for(waiting.wait(), WAIT_SECONDS)
    gate.set()
    seen_at_close: list[list[int]] = []
    real_close = rec._close_memory_store

    async def close_after_reading(wait_seconds=None):
        seen_at_close.append(await _memory_ids(rec))
        await real_close(wait_seconds)

    monkeypatch.setattr(rec, '_close_memory_store', close_after_reading)
    # The loop's write is still waiting for the event loop to run it; stop()
    # cancels the loop first, so the final stage writes the chunk.
    await rec.stop()

    assert seen_at_close == [list(range(1, 601))]


async def test_stage_m_chunk_cancelled_by_stop_with_sqlite_goes_to_the_file_and_moves_the_floor(
    tmp_path, unique_db_paths, monkeypatch
):
    rec = _recorder('sqlite', storage_dir=str(tmp_path), flush_interval_seconds=1, max_buffer=1000)
    await rec.start()
    path = rec.db_path
    gate = asyncio.Event()
    waiting = _block_writes(monkeypatch, rec.memory_store.writer, gate)
    _record_many(rec, 600)
    await asyncio.wait_for(waiting.wait(), WAIT_SECONDS)
    gate.set()
    seen_at_close: list[tuple[list[int], MemoryCoverage]] = []
    real_close = rec._close_memory_store

    async def close_after_reading(wait_seconds=None):
        seen_at_close.append((await _memory_ids(rec), rec.memory_coverage()))
        await real_close(wait_seconds)

    monkeypatch.setattr(rec, '_close_memory_store', close_after_reading)
    await rec.stop()

    ids, coverage = seen_at_close[0]
    assert ids == []
    assert (coverage.floor_id, coverage.last_id, coverage.evicted) == (601, 600, True)
    sealed = os.path.join(rec.history_dir, os.path.basename(path))
    assert _file_ids(sealed if os.path.exists(sealed) else path) == list(range(1, 601))


# --- last breath ---


async def test_last_breath_with_sqlite_writes_file_queue_then_intake_with_ids(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush_memory()
    _record_many(rec, 2, first_offset=3)

    rec._last_breath_flush()

    assert _file_rows(rec.db_path) == [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)]
    assert len(rec._file_queue) == 0
    assert len(rec._intake) == 0


async def test_last_breath_with_only_file_queue_rows_still_writes(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 3)
    await rec.flush_memory()

    with capture_logs() as captured:
        rec._last_breath_flush()

    assert _file_ids(rec.db_path) == [1, 2, 3]
    assert _logs(captured, 'recorder_last_breath_flush')[0]['events'] == 3


async def test_last_breath_with_none_writes_nothing(tmp_path):
    rec = _recorder('none', storage_dir=str(tmp_path))
    await rec.start()
    try:
        _record_many(rec, 3)

        rec._last_breath_flush()

        assert os.listdir(tmp_path) == []
        assert len(rec._intake) == 3
    finally:
        await rec.stop()


# --- worker config ---


async def test_write_config_writes_memory_and_file_and_rotation_rewrites_the_new_file(sqlite_recorder):
    from drakkar.config import DrakkarConfig, ExecutorConfig

    rec = sqlite_recorder
    config = DrakkarConfig(
        connections=kafka_connections(),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sources={'kafka': {'enabled': True}},
    )

    await rec.write_config(config)
    _, memory_rows = await rec.memory_store.read('SELECT worker_name FROM worker_config')
    await rec._rotate()

    assert memory_rows == [(WORKER,)]
    conn = sqlite3.connect(rec.db_path)
    try:
        assert conn.execute('SELECT worker_name FROM worker_config').fetchall() == [(WORKER,)]
    finally:
        conn.close()


def _drakkar_config():
    from drakkar.config import DrakkarConfig, ExecutorConfig

    return DrakkarConfig(
        connections=kafka_connections(),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sources={'kafka': {'enabled': True}},
    )


async def test_write_config_rolls_back_an_open_chunk_before_its_memory_commit(none_recorder):
    rec = none_recorder
    writer = rec.memory_store.writer
    await writer.execute("INSERT INTO events (id, ts, dt, event) VALUES (1, 1.0, 'x', 'consumed')")
    rec._memory_dirty = True

    await rec.write_config(_drakkar_config())

    _, events = await rec.memory_store.read('SELECT count(*) FROM events')
    _, config_rows = await rec.memory_store.read('SELECT worker_name FROM worker_config')
    assert events == [(0,)]
    assert config_rows == [(WORKER,)]
    assert not rec._memory_dirty


async def test_write_config_writes_the_file_when_the_memory_write_fails(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    memory_writer = rec.memory_store.writer
    real_insert = rec._insert_config_row

    async def insert(db, row):
        if db is memory_writer:
            raise _sqlite_error(SQLITE_FULL)
        await real_insert(db, row)

    monkeypatch.setattr(rec, '_insert_config_row', insert)

    with capture_logs() as captured:
        await rec.write_config(_drakkar_config())

    lines = _logs(captured, 'recorder_memory_config_write_failed')
    assert [(line['log_level'], line['suppressed']) for line in lines] == [('warning', 0)]
    conn = sqlite3.connect(rec.db_path)
    try:
        assert conn.execute('SELECT worker_name FROM worker_config').fetchall() == [(WORKER,)]
    finally:
        conn.close()


async def test_cancelled_caller_does_not_cancel_the_file_stage_of_a_flush(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    gate = asyncio.Event()
    waiting = _block_writes(monkeypatch, rec._db, gate)
    _record_many(rec, 3)

    read = asyncio.create_task(rec.flush())
    await asyncio.wait_for(waiting.wait(), WAIT_SECONDS)
    read.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read
    gate.set()
    async with rec._flush_lock:
        pass

    assert _file_ids(rec.db_path) == [1, 2, 3]
    assert len(rec._file_queue) == 0


async def test_sqlite_sequence_of_the_file_is_the_largest_explicit_id(sqlite_recorder):
    rec = sqlite_recorder
    _record_many(rec, 7)
    await rec.flush()
    _record_many(rec, 5, first_offset=7)
    await rec.flush()

    conn = sqlite3.connect(rec.db_path)
    try:
        sequence = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'events'").fetchone()
        largest = conn.execute('SELECT MAX(id) FROM events').fetchone()
    finally:
        conn.close()
    assert sequence == largest == (12,)


async def test_write_config_skips_memory_when_an_open_chunk_cannot_be_rolled_back(sqlite_recorder, monkeypatch):
    rec = sqlite_recorder
    writer = rec.memory_store.writer
    rec._memory_dirty = True
    monkeypatch.setattr(writer, 'rollback', AsyncMock(side_effect=sqlite3.OperationalError('cannot roll back')))

    with capture_logs() as captured:
        await rec.write_config(_drakkar_config())

    assert len(_logs(captured, 'recorder_memory_config_write_failed')) == 1
    _, config_rows = await rec.memory_store.read('SELECT count(*) FROM worker_config')
    assert config_rows == [(0,)]
    conn = sqlite3.connect(rec.db_path)
    try:
        assert conn.execute('SELECT worker_name FROM worker_config').fetchall() == [(WORKER,)]
    finally:
        conn.close()
