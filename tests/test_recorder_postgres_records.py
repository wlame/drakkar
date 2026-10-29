"""Recorder rows on their way to Postgres: records, the P queue, its bounds and stage M's hand-over."""

import asyncio
import inspect
import json
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.config import PostgresConnectionConfig, RecorderConfig, ResilienceConfig
from drakkar.metrics import (
    recorder_postgres_dropped_rows,
    recorder_postgres_queue_rows,
    recorder_postgres_rows_written,
)
from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask, SourceMessage
from drakkar.providers.postgres import COPY_COLUMNS, POSTGRES_CHUNK_MAX_BYTES
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder.memstore import MIB
from drakkar.recorder.postgres import (
    PostgresQueue,
    RecorderPostgresSettings,
    build_record,
    replace_nul,
    row_text_bytes,
    trace_offsets_from_metadata,
)
from drakkar.recorder.schema import EVENT_COLUMNS, EventType
from drakkar.recorder.writer import EventWriter
from drakkar.webapp.models import WebRequestContext
from tests.conftest import wait_for
from tests.recorder_pg_fake import RecorderFakePostgres

WORKER = 'w-1'
CLUSTER = 'c'
INSTANCE = 'inst-1'
DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
SQLITE_BUSY = 5
SQLITE_FULL = 13
SMALL_CHUNK_ROWS = 4
WAIT_SECONDS = 5.0
RECORD_TYPES = (int, float, str, type(None))

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['recorder']


def _settings(*, store_completed_output: bool = False, queue_max_bytes: int = 128 * MIB) -> RecorderPostgresSettings:
    return RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN),
        table_prefix='',
        retention_hours=6,
        store_completed_output=store_completed_output,
        max_output=0,
        max_size=0,
        batch_rows=5000,
        queue_max_bytes=queue_max_bytes,
        resilience=ResilienceConfig(),
    )


def _recorder(
    *, queue_max_bytes: int = 128 * MIB, fake: RecorderFakePostgres | None = None, **recorder
) -> EventRecorder:
    settings = {'flush_interval': '60s', 'capture': {'output': False}, **recorder}
    return EventRecorder(
        RecorderConfig(**settings),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='postgres',
        instance=INSTANCE,
        postgres=_settings(queue_max_bytes=queue_max_bytes),
        postgres_open_pool=(fake if fake is not None else _unreachable_postgres()).open_pool,
    )


def _unreachable_postgres() -> RecorderFakePostgres:
    """A write pool that never opens, so stage P never starts and the rows wait in the P queue."""
    fake = RecorderFakePostgres()
    fake.open_error = OSError('connection refused')
    return fake


@pytest.fixture
async def postgres_recorder():
    rec = _recorder()
    await rec.start()
    yield rec
    await rec.stop()


def _event(offset: int, ts: float | None = None, **fields) -> dict:
    return {'ts': time.time() if ts is None else ts, 'event': 'consumed', 'partition': 0, 'offset': offset, **fields}


def _record_many(rec: EventRecorder, count: int, *, first_offset: int = 0) -> None:
    for offset in range(first_offset, first_offset + count):
        rec._record(_event(offset))


def _row(**fields) -> tuple:
    values = {'id': 1, 'ts': 1.5, 'dt': 'dt', 'event': 'consumed', 'origin': 'kafka', **fields}
    return tuple(values.get(column) for column in EVENT_COLUMNS)


def _pair(row_id: int, text: str = '') -> tuple[int, tuple]:
    return row_id, _row(id=row_id, args=text)


def _queue_ids(queue: PostgresQueue) -> list[int]:
    return [row_id for row_id, _ in queue.take_chunk(len(queue) + 1, 2**62)]


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


def _dropped(reason: str) -> float:
    return recorder_postgres_dropped_rows.labels(reason=reason)._value.get()


async def _memory_ids(rec: EventRecorder) -> list[int]:
    _, rows = await rec.memory_store.read('SELECT id FROM events ORDER BY id')
    return [row[0] for row in rows]


def _sqlite_error(code: int) -> sqlite3.OperationalError:
    exc = sqlite3.OperationalError('database is locked' if code == SQLITE_BUSY else 'database or disk is full')
    exc.sqlite_errorcode = code
    return exc


def _fail_memory_writes(monkeypatch, rec: EventRecorder, failures: dict[int, int]) -> None:
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


# --- records ---


def test_record_has_24_values_in_copy_column_order():
    row = _row(id=7, event='task_started', task_id='t1', metadata='{"source_offsets": [3]}', stdout_size=4)

    record = build_record(row, cluster='', worker=WORKER, instance=INSTANCE, store_completed_output=True, max_output=0)

    assert len(record) == len(COPY_COLUMNS) == 24
    by_column = dict(zip(COPY_COLUMNS, record, strict=True))
    assert (by_column['cluster'], by_column['worker'], by_column['instance']) == ('', WORKER, INSTANCE)
    assert record[3:23] == row
    assert COPY_COLUMNS[-1] == 'trace_offsets'
    assert by_column['trace_offsets'] == [3]


class _CollectingRecorder(EventWriter):
    """Collects every event a helper records, with the ``dt`` the recorder adds."""

    def __init__(self) -> None:
        super().__init__(
            RecorderConfig(capture={'output': True, 'stdin': True}),
            ws_min_duration_ms=500,
            log_min_duration_ms=500,
        )
        self.events: list[dict] = []

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        event['dt'] = core_mod.format_dt(event['ts'])
        self.events.append(event)


class _Payload(BaseModel):
    value: str = 'x'


def _record_calls() -> dict:
    task = ExecutorTask(task_id='t1', args=['-a'], source_offsets=[10], labels={'k': 'v'}, stdin='in')
    message = SourceMessage(topic='orders', partition=0, offset=10, value=b'{}', timestamp=0)
    ctx = WebRequestContext(
        request_id='req-1',
        client_name='client',
        request=_Payload(),
        started_at=datetime.now(UTC),
        headers={},
        raw_body=b'',
    )
    result = ExecutorResult(exit_code=0, stdout='out', stderr='err', duration_seconds=0.1, task=task, pid=42)
    return {
        'record_consumed': lambda w: w.record_consumed(message),
        'record_arranged': lambda w: w.record_arranged(0, [message], [task], 0.1, ['label'], 3),
        'record_annotation': lambda w: w.record_annotation(
            kind='note', partition=0, metadata_json='{"offsets": [10]}', offset=10, task_id='t1', labels={'k': 'v'}
        ),
        'record_task_started': lambda w: w.record_task_started(
            task, 0, origin='http', client_name='client', request_id='req-1', queue_wait_ms=1.5
        ),
        'record_task_completed': lambda w: w.record_task_completed(result, 0),
        'record_task_failed': lambda w: w.record_task_failed(
            task, ExecutorError(task=task, exit_code=1, stderr='boom'), 0, duration_seconds=0.2
        ),
        'record_task_complete': lambda w: w.record_task_complete('t1', 0, 0.1, 1),
        'record_message_complete': lambda w: w.record_message_complete(0, 10, 0.1, 1, 1, 0, 0, 1),
        'record_window_complete': lambda w: w.record_window_complete(0, 3, 0.1, 1, 1),
        'record_runtime_health': lambda w: w.record_runtime_health('loop', 'ok', 1.0, 1),
        'record_runtime_stall': lambda w: w.record_runtime_stall(10.0, [], 0, 1),
        'record_runtime_lag_episode': lambda w: w.record_runtime_lag_episode(
            duration_ms=1.0,
            peak_lag_ms=1.0,
            lag_sum_ms=1.0,
            verdict='host',
            stall_count=0,
            sample_count=1,
            stacks=[],
            dropped_stacks=0,
            unit_count=1,
        ),
        'record_runtime_probe': lambda w: w.record_runtime_probe(lag_ms=1.0, unit_count=1, stacks=[]),
        'record_produced': lambda w: w.record_produced(_Payload(), 0, 10),
        'record_sink_delivery': lambda w: w.record_sink_delivery('kafka', 'out', 1, 0.1),
        'record_sink_error': lambda w: w.record_sink_error('kafka', 'out', 'boom', 1, 'dlq'),
        'record_message_dlq': lambda w: w.record_message_dlq(
            partition=0, offset=10, stage='sink', sink='kafka/out', error='boom', delivered_sinks=[], attempts=1
        ),
        'record_sink_not_started': lambda w: w.record_sink_not_started(
            sink='kafka/out', reason='message_dlq', partition=0
        ),
        'record_committed': lambda w: w.record_committed(0, 10),
        'record_assigned': lambda w: w.record_assigned([0]),
        'record_revoked': lambda w: w.record_revoked([0]),
        'record_partition_stalled': lambda w: w.record_partition_stalled(0),
        'record_offload': lambda w: w.record_offload(
            hook='arrange', partition=0, function='f', duration=0.1, queued=0.0, status='ok', offsets=(10,), offset=10
        ),
        'record_periodic_run': lambda w: w.record_periodic_run('tick', 0.1, 'ok', slot=60),
        'record_webapp_request_received': lambda w: w.record_webapp_request_received(ctx),
        'record_webapp_request_completed': lambda w: w.record_webapp_request_completed(ctx, 'ok', 1.0),
        'record_webapp_request_timeout': lambda w: w.record_webapp_request_timeout(ctx, 1.0),
        'record_webapp_request_rate_limited': lambda w: w.record_webapp_request_rate_limited('client', 10, 11),
        'record_webapp_request_auth_failed': lambda w: w.record_webapp_request_auth_failed('abcd'),
        'record_webapp_request_dropped_after_timeout': lambda w: w.record_webapp_request_dropped_after_timeout(ctx),
    }


async def test_record_column_types_per_record_helper():
    calls = _record_calls()
    helpers = {name for name, _ in inspect.getmembers(EventWriter, inspect.isfunction) if name.startswith('record_')}
    assert set(calls) == helpers

    for name, call in calls.items():
        writer = _CollectingRecorder()
        call(writer)
        assert writer.events, name
        for event in writer.events:
            record = build_record(
                core_mod._row_tuple(1, event),
                cluster=CLUSTER,
                worker=WORKER,
                instance=INSTANCE,
                store_completed_output=True,
                max_output=0,
            )
            wrong = {
                column: type(value).__name__
                for column, value in zip(COPY_COLUMNS[:-1], record[:-1], strict=True)
                if type(value) not in RECORD_TYPES
            }
            assert wrong == {}, name
            assert record[-1] is None or all(type(item) is int for item in record[-1]), name


@pytest.mark.parametrize(('text', 'expected'), [(case['text'], case['expected']) for case in FIXTURE['text_cases']])
def test_nul_becomes_replacement_character(text, expected):
    assert replace_nul(text) == expected
    record = build_record(
        _row(args=text), cluster=text, worker=WORKER, instance=INSTANCE, store_completed_output=True, max_output=0
    )
    assert record[0] == expected
    assert record[COPY_COLUMNS.index('args')] == expected


def _output_row(event: str = EventType.TASK_COMPLETED, **fields) -> tuple:
    values = {
        'event': event,
        'stdout': 'o\x00ut',
        'stderr': 'err',
        'stdout_size': 4,
        'metadata': '{"stdin": "x", "duration": 1}',
        **fields,
    }
    return _row(**values)


def _output_record(row: tuple, *, store_completed_output: bool, max_output: int = 0) -> dict:
    record = build_record(
        row,
        cluster=CLUSTER,
        worker=WORKER,
        instance=INSTANCE,
        store_completed_output=store_completed_output,
        max_output=max_output,
    )
    return dict(zip(COPY_COLUMNS, record, strict=True))


def test_completed_row_drops_output_without_the_flag():
    row = _output_row()

    by_column = _output_record(row, store_completed_output=False)

    assert (by_column['stdout'], by_column['stderr']) == (None, None)
    assert by_column['stdout_size'] == 4
    assert by_column['metadata'] == row[EVENT_COLUMNS.index('metadata')]
    kept = [column for column in EVENT_COLUMNS if column not in ('stdout', 'stderr')]
    assert [by_column[column] for column in kept] == [row[EVENT_COLUMNS.index(column)] for column in kept]


def test_completed_row_keeps_output_with_the_flag():
    by_column = _output_record(_output_row(), store_completed_output=True)

    assert (by_column['stdout'], by_column['stderr']) == ('o\ufffdut', 'err')


def test_task_failed_row_keeps_stderr_without_completed_output():
    by_column = _output_record(
        _output_row(EventType.TASK_FAILED, stderr='Traceback\x00: boom'), store_completed_output=False
    )

    assert (by_column['stdout'], by_column['stderr']) == (None, 'Traceback\ufffd: boom')
    assert by_column['stdout_size'] == 4


def test_task_failed_stderr_is_cut_to_max_output_as_valid_utf8_with_the_exact_count():
    stderr = 'é' * 1000
    by_column = _output_record(
        _output_row(EventType.TASK_FAILED, stderr=stderr), store_completed_output=False, max_output=1025
    )

    # 2000 bytes: a head of 512 bytes (256 characters) and a tail of 513
    # bytes, of which only 512 hold whole characters.
    assert by_column['stderr'] == 'é' * 256 + '\n...[cut 976 bytes]...\n' + 'é' * 256
    assert by_column['stderr'].encode('utf-8').decode('utf-8') == by_column['stderr']


@pytest.mark.parametrize(
    'case', FIXTURE['output_cut_cases'], ids=[case['name'] for case in FIXTURE['output_cut_cases']]
)
@pytest.mark.parametrize('column', ['stdout', 'stderr'])
def test_output_cut_cases(case, column):
    by_column = _output_record(
        _output_row(**{column: case['text']}), store_completed_output=True, max_output=case['max_output']
    )

    assert by_column[column] == case['expected']


def test_output_of_one_kibibyte_keeps_half_from_each_end():
    stdout = 'a' * 3000 + 'z' * 3000

    by_column = _output_record(_output_row(stdout=stdout), store_completed_output=True, max_output=1024)

    assert by_column['stdout'] == 'a' * 512 + '\n...[cut 4976 bytes]...\n' + 'z' * 512


def test_metadata_is_never_cut():
    metadata = json.dumps({'stdin': 'x' * 10_000, 'duration': 1})
    args = 'y' * 10_000
    row = _output_row(metadata=metadata, args=args)

    by_column = _output_record(row, store_completed_output=True, max_output=1024)

    assert (by_column['metadata'], by_column['args']) == (metadata, args)


@pytest.mark.parametrize(
    ('event', 'metadata', 'expected'),
    [
        pytest.param(case['event'], case['metadata'], case['expected'], id=case['name'])
        for case in FIXTURE['trace_offsets_cases']
    ],
)
def test_trace_offsets_from_metadata(event, metadata, expected):
    assert trace_offsets_from_metadata(event, metadata) == expected


# --- the P queue and chunks ---


def test_queue_row_bound_drops_oldest_and_counts_queue_full():
    queue = PostgresQueue(max_rows=3, max_bytes=2**40)

    assert queue.append([_pair(1), _pair(2)]) == 0
    assert queue.append([_pair(3), _pair(4), _pair(5)]) == 2

    assert len(queue) == 3
    assert _queue_ids(queue) == [3, 4, 5]


def test_queue_byte_bound_drops_oldest_and_counts_queue_full():
    one_row_bytes = row_text_bytes(_pair(1, 'x' * 100)[1])
    queue = PostgresQueue(max_rows=1000, max_bytes=2 * one_row_bytes)

    assert queue.append([_pair(1, 'x' * 100), _pair(2, 'x' * 100)]) == 0
    assert queue.bytes == 2 * one_row_bytes
    assert queue.append([_pair(3, 'x' * 100)]) == 1

    assert queue.bytes == 2 * one_row_bytes
    assert _queue_ids(queue) == [2, 3]
    assert queue.bytes == 0


def test_oversized_row_is_dropped_alone_and_the_queued_rows_stay():
    small = row_text_bytes(_pair(1, 'x' * 10)[1])
    queue = PostgresQueue(max_rows=1000, max_bytes=4 * small)

    dropped = queue.append([_pair(1, 'x' * 10), _pair(2, 'x' * (5 * small)), _pair(3, 'x' * 10)])

    assert dropped == 1
    assert queue.bytes == 2 * small
    assert _queue_ids(queue) == [1, 3]


async def test_oversized_row_counts_one_queue_full_drop():
    rec = _recorder(max_buffer_rows=1000, queue_max_bytes=1000)
    await rec.start()
    try:
        before = _dropped('queue_full')
        rec._record(_event(0, args='x' * 10))
        rec._record(_event(1, args='x' * 2000))
        rec._record(_event(2, args='x' * 10))
        await rec.flush_memory()

        assert _dropped('queue_full') - before == 1
        assert _queue_ids(rec._pg_queue) == [1, 3]
    finally:
        await rec.stop()


async def test_queue_full_on_append_counts_and_keeps_the_newest_pairs(monkeypatch):
    rec = _recorder(max_buffer_rows=1000)
    await rec.start()
    try:
        before = _dropped('queue_full')
        _record_many(rec, 800)
        await rec.flush_memory()
        _record_many(rec, 500, first_offset=800)
        await rec.flush_memory()

        assert _dropped('queue_full') - before == 300
        assert _queue_ids(rec._pg_queue) == list(range(301, 1301))
    finally:
        await rec.stop()


def test_chunk_respects_rows_and_bytes_and_holds_at_least_one_pair():
    queue = PostgresQueue(max_rows=100, max_bytes=2**40)
    queue.append([_pair(row_id, 'x' * 10) for row_id in range(1, 11)])
    row_bytes = row_text_bytes(_pair(1, 'x' * 10)[1])

    assert [row_id for row_id, _ in queue.take_chunk(3, 2**40)] == [1, 2, 3]
    assert [row_id for row_id, _ in queue.take_chunk(100, 2 * row_bytes)] == [4, 5]
    assert [row_id for row_id, _ in queue.take_chunk(100, 1)] == [6]
    assert len(queue) == 4
    assert queue.bytes == 4 * row_bytes

    big = PostgresQueue(max_rows=100, max_bytes=2**40)
    big.append([_pair(1, 'x' * (5 * MIB)), _pair(2)])
    chunk = big.take_chunk(5000, POSTGRES_CHUNK_MAX_BYTES)
    assert [row_id for row_id, _ in chunk] == [1]
    assert len(big) == 1
    assert PostgresQueue(max_rows=1, max_bytes=1).take_chunk(5000, POSTGRES_CHUNK_MAX_BYTES) == []


# --- stage M hands rows to the P queue ---


async def test_stage_m_success_path_appends_pairs_to_the_p_queue(postgres_recorder):
    rec = postgres_recorder
    _record_many(rec, 5)

    await rec.flush_memory()

    pairs = rec._pg_queue.take_chunk(100, 2**40)
    assert [row_id for row_id, _ in pairs] == await _memory_ids(rec) == [1, 2, 3, 4, 5]
    assert all(row[0] == row_id for row_id, row in pairs)
    assert [row[EVENT_COLUMNS.index('offset')] for _, row in pairs] == [0, 1, 2, 3, 4]
    assert len(rec._file_queue) == 0


async def test_memory_gap_rows_reach_the_p_queue_with_their_ids(postgres_recorder, monkeypatch):
    rec = postgres_recorder
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', SMALL_CHUNK_ROWS)
    _fail_memory_writes(monkeypatch, rec, {2: SQLITE_FULL})
    _record_many(rec, 10)

    await rec.flush_memory()

    assert await _memory_ids(rec) == [9, 10]
    assert rec.memory_coverage().evicted is True
    assert _queue_ids(rec._pg_queue) == list(range(1, 11))


async def test_cancelled_memory_write_with_postgres_goes_back_to_the_intake(postgres_recorder, monkeypatch):
    rec = postgres_recorder
    gate = asyncio.Event()
    waiting = asyncio.Event()
    writer = rec.memory_store.writer
    real_executemany = writer.executemany

    async def executemany(sql, rows):
        waiting.set()
        await gate.wait()
        return await real_executemany(sql, rows)

    monkeypatch.setattr(writer, 'executemany', executemany)
    _record_many(rec, 3)
    offsets = [event['offset'] for event in rec._intake]
    stage = asyncio.create_task(rec._stage_memory(0.0))
    await asyncio.wait_for(waiting.wait(), WAIT_SECONDS)

    stage.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stage

    assert len(rec._pg_queue) == 0
    assert [event['offset'] for event in rec._intake] == offsets
    assert all('id' not in event for event in rec._intake)
    assert rec._next_id == 1
    monkeypatch.setattr(writer, 'executemany', real_executemany)
    await rec.flush_memory()
    assert _queue_ids(rec._pg_queue) == [1, 2, 3]


async def test_p_queue_past_half_wakes_stage_p():
    fake = RecorderFakePostgres()
    rec = _recorder(max_buffer_rows=1000, fake=fake)
    await rec.start()
    try:
        _record_many(rec, 499)
        await rec.flush_memory()
        await asyncio.sleep(0.1)
        assert fake.copy_calls() == []

        _record_many(rec, 1, first_offset=499)
        await rec.flush_memory()
        await wait_for(lambda: len(fake.copied) == 500)
    finally:
        await rec.stop()


async def test_p_queue_past_half_of_the_byte_bound_wakes_stage_p():
    fake = RecorderFakePostgres()
    rec = _recorder(max_buffer_rows=1000, queue_max_bytes=1000, fake=fake)
    await rec.start()
    try:
        rec._record(_event(0, args='x' * 600))
        await rec.flush_memory()

        await wait_for(lambda: fake.copied_ids() == [1])
    finally:
        await rec.stop()


@pytest.mark.parametrize(
    ('prefix', 'table'), [('', 'recorder_events'), ('busy', 'busy_recorder_events')], ids=['no_prefix', 'prefix']
)
def test_settings_name_the_schema_and_the_table(prefix, table):
    connection = PostgresConnectionConfig(dsn=DSN, schema='history')
    settings = RecorderPostgresSettings(
        connection_name='main',
        connection=connection,
        table_prefix=prefix,
        retention_hours=6,
        store_completed_output=False,
        max_output=0,
        max_size=0,
        batch_rows=5000,
        queue_max_bytes=128 * MIB,
        resilience=ResilienceConfig(),
    )

    assert (settings.schema, settings.table, settings.qualified_table) == ('history', table, f'history.{table}')


def test_persistence_postgres_without_settings_raises():
    with pytest.raises(ValueError, match=r'^recorder: persistence postgres needs postgres settings$'):
        EventRecorder(RecorderConfig(), persistence='postgres')


@pytest.mark.parametrize('persistence', ['none', 'sqlite'])
def test_other_modes_build_no_p_queue(tmp_path, persistence):
    rec = EventRecorder(RecorderConfig(), persistence=persistence, storage_dir=str(tmp_path), postgres=_settings())

    assert rec._pg_queue is None


def test_recorder_postgres_metrics_names_and_help_texts():
    rows = [
        (recorder_postgres_queue_rows, 'gauge', 'drakkar_recorder_postgres_queue_rows', ()),
        (recorder_postgres_rows_written, 'counter', 'drakkar_recorder_postgres_rows_written', ()),
        (recorder_postgres_dropped_rows, 'counter', 'drakkar_recorder_postgres_dropped_rows', ('reason',)),
    ]
    helps = {
        'drakkar_recorder_postgres_queue_rows': 'Rows waiting to be written to the recorder Postgres table',
        'drakkar_recorder_postgres_rows_written': 'Rows written to the recorder Postgres table',
        'drakkar_recorder_postgres_dropped_rows': 'Rows that never reached the recorder Postgres table, by reason',
    }
    for metric, kind, name, labels in rows:
        assert metric._type == kind
        assert metric._name == name
        assert metric._labelnames == labels
        assert metric._documentation == helps[name]


async def test_queue_full_drop_logs_once_per_minute_with_suppressed():
    rec = _recorder(max_buffer_rows=1000)
    await rec.start()
    clock = [1000.0]
    rec._pg_drop_log = RateLimitedLog(lambda: clock[0])
    try:
        _record_many(rec, 800)
        await rec.flush_memory()
        _record_many(rec, 300, first_offset=800)
        with capture_logs() as captured:
            await rec.flush_memory()
        lines = _logs(captured, 'recorder_postgres_rows_dropped')
        assert [
            (line['log_level'], line['category'], line['reason'], line['rows'], line['error']) for line in lines
        ] == [('warning', 'recorder', 'queue_full', 100, '')]
        assert lines[0]['suppressed'] == 0

        _record_many(rec, 30, first_offset=1100)
        with capture_logs() as captured:
            await rec.flush_memory()
        assert _logs(captured, 'recorder_postgres_rows_dropped') == []

        clock[0] += 61
        _record_many(rec, 10, first_offset=1130)
        with capture_logs() as captured:
            await rec.flush_memory()
        lines = _logs(captured, 'recorder_postgres_rows_dropped')
        assert [(line['rows'], line['suppressed']) for line in lines] == [(40, 1)]
        assert rec._pg_unlogged_drops == 0
    finally:
        await rec.stop()
