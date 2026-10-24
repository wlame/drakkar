"""Eviction of the recorder memory store: the window, the row cap, the byte budget, and the memory gauges."""

import asyncio
import sqlite3
import time

import pytest
from structlog.testing import capture_logs

from drakkar.metrics import (
    recorder_memory_allocated_bytes,
    recorder_memory_bytes,
    recorder_memory_evicted_rows,
    recorder_memory_oldest_age_seconds,
    recorder_memory_rows,
)
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder.memstore import (
    EVICT_MAX_BATCHES,
    MIB,
    MemoryCoverage,
    MemoryStore,
    read_page_numbers,
    run_eviction_pass,
)
from tests.conftest import make_ui_config

WORKER = 'w-1'
LARGE_WINDOW_SECONDS = 86_400
LARGE_MAX_ROWS = 1_000_000
LARGE_MAX_BYTES = 512 * MIB
OLD_SECONDS = 600
WAIT_SECONDS = 5.0


def _recorder(
    *,
    window_seconds: int = LARGE_WINDOW_SECONDS,
    max_rows: int = LARGE_MAX_ROWS,
    max_bytes: int = LARGE_MAX_BYTES,
    **ui,
) -> EventRecorder:
    settings = {'enabled': True, 'store_output': False, 'flush_interval_seconds': 60}
    settings.update(ui)
    return EventRecorder(
        make_ui_config(**settings),
        worker_name=WORKER,
        cluster_name='c',
        persistence='none',
        memory_window_seconds=window_seconds,
        memory_max_rows=max_rows,
        memory_max_bytes=max_bytes,
    )


@pytest.fixture
async def started():
    """Start recorders built in the test and stop them at the end."""
    recorders: list[EventRecorder] = []

    async def start(**kwargs) -> EventRecorder:
        rec = _recorder(**kwargs)
        await rec.start()
        recorders.append(rec)
        return rec

    yield start
    for rec in recorders:
        await rec.stop()


@pytest.fixture
async def store():
    """A memory store on its own, for direct eviction passes."""
    memory_store = MemoryStore('eviction-test', store_config=False)
    await memory_store.open()
    yield memory_store
    await memory_store.close()


def _event(offset: int, ts: float, metadata: str | None = None) -> dict:
    event = {'ts': ts, 'event': 'consumed', 'partition': 0, 'offset': offset}
    if metadata is not None:
        event['metadata'] = metadata
    return event


def _record(rec: EventRecorder, timestamps: list[float], *, metadata: str | None = None) -> None:
    for offset, ts in enumerate(timestamps):
        rec._record(_event(offset, ts, metadata))


async def _memory_ids(rec: EventRecorder) -> list[int]:
    _, rows = await rec.memory_store.read('SELECT id FROM events ORDER BY id')
    return [row[0] for row in rows]


async def _insert_rows(memory_store: MemoryStore, timestamps: list[float], *, first_id: int = 1) -> None:
    rows = [(first_id + index, ts, 'x', 'consumed') for index, ts in enumerate(timestamps)]
    await memory_store.writer.executemany('INSERT INTO events (id, ts, dt, event) VALUES (?, ?, ?, ?)', rows)
    await memory_store.writer.commit()


async def _trace_statements(memory_store: MemoryStore) -> list[str]:
    statements: list[str] = []
    await memory_store.writer.set_trace_callback(statements.append)
    return statements


def _evicted(reason: str) -> float:
    return recorder_memory_evicted_rows.labels(reason=reason)._value.get()


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


async def test_window_deletes_the_prefix_before_the_first_kept_row(started):
    rec = await started(window_seconds=60)
    now = time.time()
    old = [now - 300, now - 100, now - 200]
    _record(rec, [*old, now, now])

    await rec.flush_memory()

    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == [4, 5]
    assert (coverage.floor_id, coverage.last_id, coverage.evicted) == (4, 5, True)
    assert coverage.evicted_max_ts == now - 100


async def test_late_ts_after_the_boundary_stays_until_the_prefix_passes_it(started):
    rec = await started(window_seconds=60)
    now = time.time()
    _record(rec, [now - OLD_SECONDS] * 5 + [now, now - OLD_SECONDS])

    await rec._stage_memory(0.0)
    assert await _memory_ids(rec) == [6, 7]

    rec._memory_max_rows = 1
    await rec._stage_memory(0.0)
    assert await _memory_ids(rec) == [7]

    await rec._stage_memory(0.0)
    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == []
    assert (coverage.floor_id, coverage.last_id, coverage.rows) == (8, 7, 0)


async def test_row_cap_deletes_the_oldest_ids(started):
    rec = await started(max_rows=100)
    _record(rec, [time.time()] * 150)

    await rec._stage_memory(0.0)

    assert await _memory_ids(rec) == list(range(51, 151))
    assert rec.memory_coverage().floor_id == 51


async def test_byte_budget_uses_pages_in_use_not_page_count(started):
    budget = 256 * 1024
    rec = await started(max_bytes=budget)
    bytes_before = _evicted('bytes')
    # 1 row per batch at this size: the budget is met well inside the batch cap.
    _record(rec, [time.time()] * 100, metadata='m' * 4000)

    await rec._stage_memory(0.0)

    page_count, freelist_count, page_size = await read_page_numbers(rec.memory_store.writer)
    assert (page_count - freelist_count) * page_size <= budget
    assert page_count * page_size > budget
    assert _evicted('bytes') > bytes_before
    rows_after_first_pass = len(await _memory_ids(rec))
    assert rows_after_first_pass > 0

    _record(rec, [time.time()] * 20, metadata='m' * 4000)
    await rec._stage_memory(0.0)

    page_count, freelist_count, page_size = await read_page_numbers(rec.memory_store.writer)
    assert page_count * page_size > budget
    assert len(await _memory_ids(rec)) > rows_after_first_pass // 2


async def test_order_is_window_then_rows_then_bytes(started):
    rec = await started(window_seconds=60, max_rows=10, max_bytes=64 * 1024)
    before = {reason: _evicted(reason) for reason in ('window', 'rows', 'bytes')}
    now = time.time()
    _record(rec, [now - OLD_SECONDS] * 5 + [now] * 30, metadata='m' * 8000)

    await rec._stage_memory(0.0)

    deltas = {reason: _evicted(reason) - before[reason] for reason in before}
    assert deltas['window'] == 5
    assert deltas['rows'] == 20
    assert deltas['bytes'] > 0


async def test_batches_are_at_most_5000_rows_with_a_commit_each(store):
    now = time.time()
    await _insert_rows(store, [now - OLD_SECONDS] * 12_000 + [now] * 10)
    statements = await _trace_statements(store)

    result = await run_eviction_pass(
        store.writer,
        MemoryCoverage(floor_id=1, last_id=12_010, evicted=False, evicted_max_ts=0.0),
        now=now,
        window_seconds=60,
        max_rows=LARGE_MAX_ROWS,
        max_bytes=LARGE_MAX_BYTES,
    )

    assert result.evicted_by_reason == {'window': 12_000, 'rows': 0, 'bytes': 0}
    assert result.coverage.floor_id == 12_001
    assert len([s for s in statements if s.startswith('DELETE FROM events')]) == 3
    assert len([s for s in statements if s.upper().startswith('COMMIT')]) == 3


async def test_byte_step_stops_after_64_batches(store):
    now = time.time()
    await _insert_rows(store, [now] * 10_000)
    statements = await _trace_statements(store)
    coverage = MemoryCoverage(floor_id=1, last_id=10_000, evicted=False, evicted_max_ts=0.0)

    result = await run_eviction_pass(
        store.writer,
        coverage,
        now=now,
        window_seconds=LARGE_WINDOW_SECONDS,
        max_rows=LARGE_MAX_ROWS,
        max_bytes=1,
    )

    assert len([s for s in statements if s.startswith('DELETE FROM events')]) == EVICT_MAX_BATCHES
    assert result.failed_reason is None
    assert result.coverage.rows > 0
    assert result.evicted_by_reason['bytes'] == result.coverage.floor_id - 1


async def test_evicted_max_ts_is_the_largest_evicted_ts_over_the_process(started):
    rec = await started()
    now = time.time()
    _record(rec, [now - 10, now - 50, now])

    rec._memory_max_rows = 2
    await rec._stage_memory(0.0)
    assert rec.memory_coverage().evicted_max_ts == now - 10

    rec._memory_max_rows = 1
    await rec._stage_memory(0.0)
    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == [3]
    assert coverage.evicted_max_ts == now - 10


async def test_idle_memory_loop_tick_evicts_by_the_window(started):
    rec = await started(window_seconds=1, flush_interval_seconds=1)
    _record(rec, [time.time()] * 3)
    await rec.flush_memory()
    assert await _memory_ids(rec) == [1, 2, 3]

    deadline = time.monotonic() + WAIT_SECONDS
    while await _memory_ids(rec) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)

    coverage = rec.memory_coverage()
    assert await _memory_ids(rec) == []
    assert (coverage.floor_id, coverage.last_id, coverage.evicted) == (4, 3, True)


async def test_ui_read_stage_evicts_at_most_once_per_second(started, monkeypatch):
    rec = await started()
    clock = [100.0]
    rec._evict_clock = lambda: clock[0]
    passes = []
    real_pass = core_mod.run_eviction_pass

    async def counting_pass(*args, **kwargs):
        passes.append(clock[0])
        return await real_pass(*args, **kwargs)

    monkeypatch.setattr(core_mod, 'run_eviction_pass', counting_pass)

    await rec.flush_memory()
    clock[0] += 0.5
    await rec.flush_memory()
    assert passes == [100.0]

    clock[0] += 0.5
    await rec.flush_memory()
    await rec._stage_memory(0.0)
    assert passes == [100.0, 101.0, 101.0]


async def test_worker_config_row_is_never_evicted(started):
    from drakkar.config import DrakkarConfig, ExecutorConfig

    rec = await started(max_rows=0, store_config=True)
    await rec.write_config(
        DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'), sources={'kafka': {'enabled': True}})
    )
    _record(rec, [time.time()] * 5)

    await rec._stage_memory(0.0)

    _, config_rows = await rec.memory_store.read('SELECT worker_name FROM worker_config')
    assert await _memory_ids(rec) == []
    assert config_rows == [(WORKER,)]


async def test_failed_statement_logs_once_and_the_next_pass_recovers(started, monkeypatch):
    rec = await started(window_seconds=60)
    clock = [1000.0]
    rec._memory_log = RateLimitedLog(lambda: clock[0])
    writer = rec.memory_store.writer
    real_execute = writer.execute
    failures = [2]

    def execute(sql, parameters=None):
        if sql.startswith('SELECT id FROM events NOT INDEXED') and failures[0] > 0:
            failures[0] -= 1
            exc = sqlite3.OperationalError('database is locked')
            exc.sqlite_errorcode = 5
            raise exc
        return real_execute(sql, parameters)

    monkeypatch.setattr(writer, 'execute', execute)
    _record(rec, [time.time() - OLD_SECONDS] * 3)

    with capture_logs() as captured:
        await rec._stage_memory(0.0)
        await rec._stage_memory(0.0)

    lines = _logs(captured, 'recorder_memory_eviction_failed')
    assert [(line['log_level'], line['reason'], line['suppressed']) for line in lines] == [('warning', 'window', 0)]
    assert lines[0]['error'] == 'database is locked'
    assert await _memory_ids(rec) == [1, 2, 3]
    assert rec.memory_coverage().evicted is False

    await rec._stage_memory(0.0)
    assert await _memory_ids(rec) == []
    assert rec.memory_coverage().floor_id == 4


async def test_gauges_after_a_pass(started):
    rec = await started()
    now = time.time()
    _record(rec, [now - 30] * 4)

    await rec._stage_memory(0.0)

    assert recorder_memory_rows._value.get() == 4
    assert recorder_memory_bytes._value.get() > 0
    assert recorder_memory_allocated_bytes._value.get() >= recorder_memory_bytes._value.get()
    assert recorder_memory_oldest_age_seconds._value.get() >= 30

    rec._memory_max_rows = 0
    await rec._stage_memory(0.0)

    assert recorder_memory_rows._value.get() == 0
    assert recorder_memory_oldest_age_seconds._value.get() == 0
    assert recorder_memory_allocated_bytes._value.get() >= recorder_memory_bytes._value.get()


def _locked_error() -> sqlite3.OperationalError:
    exc = sqlite3.OperationalError('database is locked')
    exc.sqlite_errorcode = 5
    return exc


def _fail_statements(monkeypatch, conn, prefix: str) -> None:
    real_execute = conn.execute

    def execute(sql, parameters=None):
        if sql.startswith(prefix):
            raise _locked_error()
        return real_execute(sql, parameters)

    monkeypatch.setattr(conn, 'execute', execute)


@pytest.mark.parametrize(
    ('statement', 'reason', 'deleted_rows'),
    [
        ('SELECT id FROM events NOT INDEXED', 'window', 0),
        ('PRAGMA page_count', 'bytes', 5),
        ('SELECT MIN(ts)', 'floor', 10),
    ],
)
async def test_failed_read_ends_the_pass_with_its_reason(store, monkeypatch, statement, reason, deleted_rows):
    now = time.time()
    await _insert_rows(store, [now - OLD_SECONDS] * 5 + [now] * 5)
    _fail_statements(monkeypatch, store.writer, statement)

    result = await run_eviction_pass(
        store.writer,
        MemoryCoverage(floor_id=1, last_id=10, evicted=False, evicted_max_ts=0.0),
        now=now,
        window_seconds=60,
        max_rows=LARGE_MAX_ROWS,
        max_bytes=1,
    )

    assert result.failed_reason == reason
    assert str(result.error) == 'database is locked'
    assert result.rollback_error is None
    assert result.coverage.floor_id == 1 + deleted_rows
    assert (result.used_bytes, result.allocated_bytes, result.oldest_ts) == (0, 0, None)


async def test_failed_delete_keeps_the_committed_batches_and_names_its_reason(store, monkeypatch):
    from drakkar.recorder import memstore as memstore_mod

    now = time.time()
    await _insert_rows(store, [now] * 12_000)

    async def failing_after_one_batch(conn, below_id):
        return memstore_mod.PrefixDeletion(5000, now, _locked_error(), batches=1)

    monkeypatch.setattr(memstore_mod, 'delete_id_prefix', failing_after_one_batch)

    result = await run_eviction_pass(
        store.writer,
        MemoryCoverage(floor_id=1, last_id=12_000, evicted=False, evicted_max_ts=0.0),
        now=now,
        window_seconds=LARGE_WINDOW_SECONDS,
        max_rows=100,
        max_bytes=LARGE_MAX_BYTES,
    )

    assert result.failed_reason == 'rows'
    assert result.evicted_by_reason == {'window': 0, 'rows': 5000, 'bytes': 0}
    assert (result.coverage.floor_id, result.coverage.evicted, result.coverage.evicted_max_ts) == (5001, True, now)


async def test_failed_rollback_is_reported_with_the_error(store, monkeypatch):
    _fail_statements(monkeypatch, store.writer, 'SELECT id FROM events NOT INDEXED')

    async def rollback():
        raise sqlite3.OperationalError('cannot roll back')

    monkeypatch.setattr(store.writer, 'rollback', rollback)

    result = await run_eviction_pass(
        store.writer,
        MemoryCoverage(floor_id=1, last_id=0, evicted=False, evicted_max_ts=0.0),
        now=time.time(),
        window_seconds=60,
        max_rows=LARGE_MAX_ROWS,
        max_bytes=LARGE_MAX_BYTES,
    )

    assert result.failed_reason == 'window'
    assert str(result.rollback_error) == 'cannot roll back'


async def test_byte_budget_stops_when_a_batch_removes_nothing(store):
    now = time.time()
    await _insert_rows(store, [now] * 100, first_id=500)

    result = await run_eviction_pass(
        store.writer,
        MemoryCoverage(floor_id=1, last_id=599, evicted=False, evicted_max_ts=0.0),
        now=now,
        window_seconds=LARGE_WINDOW_SECONDS,
        max_rows=LARGE_MAX_ROWS,
        max_bytes=1,
    )

    assert result.evicted_by_reason == {'window': 0, 'rows': 0, 'bytes': 0}
    assert result.failed_reason is None
    assert result.coverage.floor_id == 500


async def test_eviction_failure_with_a_failed_rollback_marks_the_writer_dirty(started, monkeypatch):
    from drakkar.recorder import memstore as memstore_mod

    rec = await started(max_rows=1)
    rec._memory_log = RateLimitedLog(lambda: 1000.0)

    async def failing_delete(conn, below_id):
        return memstore_mod.PrefixDeletion(0, None, _locked_error(), sqlite3.OperationalError('cannot roll back'))

    monkeypatch.setattr(memstore_mod, 'delete_id_prefix', failing_delete)
    _record(rec, [time.time()] * 3)

    with capture_logs() as captured:
        await rec._stage_memory(0.0)

    lines = _logs(captured, 'recorder_memory_eviction_failed')
    assert [(line['reason'], line['rollback_error']) for line in lines] == [('rows', 'cannot roll back')]
    assert rec._memory_dirty is True
    assert recorder_memory_rows._value.get() == 3
