"""Stage P: the start, the COPY outcomes, the Postgres loop and the stop of the ``postgres`` recorder mode.

No database: the write pool is the recording fake of ``tests/recorder_pg_fake.py``.
Most outcomes drive :class:`PostgresWriter` directly on the test's loop with a
fake clock; the tests of the loop and the stop run a whole recorder, whose
stage P runs on its own thread.
"""

import asyncio
import os
import threading
import time

import asyncpg
import pytest
from structlog.testing import capture_logs

from drakkar.config import PostgresConnectionConfig, RecorderConfig, ResilienceConfig
from drakkar.metrics import (
    provider_calls,
    recorder_postgres_dropped_rows,
    recorder_postgres_queue_rows,
    recorder_postgres_rows_written,
)
from drakkar.providers.postgres import (
    COPY_COLUMNS,
    RECORDER_STATEMENT_TEMPLATES,
    STATEMENT_TEMPLATES,
    recorder_missing_table_message,
)
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder.memstore import MIB
from drakkar.recorder.postgres import PostgresQueue, RecorderPostgresSettings, row_text_bytes
from drakkar.recorder.postgres_writer import CopyOutcome, PostgresLoopThread, PostgresWriter, UpkeepStatus, WriterStatus
from drakkar.recorder.schema import EVENT_COLUMNS
from tests.conftest import wait_for
from tests.recorder_pg_fake import RecorderFakePostgres

WORKER = 'w-1'
CLUSTER = 'c'
INSTANCE = 'inst-1'
DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
BATCH_ROWS = 100
# What a stop may take beyond the time left minus its reserve: the thread's answer and the joins.
STOP_MARGIN_SECONDS = 0.25
DROP_REASONS = ('queue_full', 'rejected', 'no_partition', 'shutdown')


class FakeClock:
    """A monotonic clock the test moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _no_sleep(_seconds: float) -> None:
    return None


def _settings(
    *,
    manage_schema: bool = True,
    table_prefix: str = '',
    pool_max: int = 4,
    store_completed_output: bool = False,
    max_output: int = 0,
) -> RecorderPostgresSettings:
    return RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN, manage_schema=manage_schema, pool_min=2, pool_max=pool_max),
        table_prefix=table_prefix,
        retention_hours=6,
        store_completed_output=store_completed_output,
        max_output=max_output,
        max_size=0,
        batch_rows=BATCH_ROWS,
        queue_max_bytes=128 * MIB,
        resilience=ResilienceConfig(),
    )


def _resilience(**values) -> ResilienceConfig:
    return ResilienceConfig.model_validate(values)


def _writer(
    fake: RecorderFakePostgres,
    *,
    clock: FakeClock | None = None,
    queue: PostgresQueue | None = None,
    resilience: ResilienceConfig | None = None,
    now_wall=time.time,
    **settings,
) -> PostgresWriter:
    clock = clock if clock is not None else FakeClock()
    caller = ResilientCaller(
        resilience if resilience is not None else _resilience(max_retries=0),
        feature='recorder',
        provider='postgres',
        category='recorder',
        now_monotonic=clock,
        sleep=_no_sleep,
    )
    return PostgresWriter(
        _settings(**settings),
        cluster=CLUSTER,
        worker=WORKER,
        instance=INSTANCE,
        queue=queue if queue is not None else PostgresQueue(max_rows=10_000, max_bytes=128 * MIB),
        caller=caller,
        open_pool=fake.open_pool,
        now_monotonic=clock,
        now_wall=now_wall,
    )


def _row(row_id: int) -> tuple:
    values = {'id': row_id, 'ts': 1.5, 'dt': 'dt', 'event': 'consumed', 'offset': row_id, 'origin': 'kafka'}
    return tuple(values.get(column) for column in EVENT_COLUMNS)


def _pairs(first: int, count: int) -> list[tuple[int, tuple]]:
    return [(row_id, _row(row_id)) for row_id in range(first, first + count)]


def _counter(metric) -> float:
    return metric._value.get()


def _dropped() -> dict[str, float]:
    return {reason: _counter(recorder_postgres_dropped_rows.labels(reason=reason)) for reason in DROP_REASONS}


def _calls(op: str, outcome: str) -> float:
    return _counter(provider_calls.labels(feature='recorder', provider='postgres', op=op, outcome=outcome))


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


def _final_error(sqlstate_class: type[asyncpg.PostgresError], text: str = 'bad value') -> asyncpg.PostgresError:
    return sqlstate_class(text)


# --- start ---


async def test_start_opens_a_one_connection_pool_then_ensures_the_table():
    fake = RecorderFakePostgres()
    writer = _writer(fake, pool_max=8)

    with capture_logs() as captured:
        await writer.run_once()

    assert [(entry.pool_min, entry.pool_max, entry.dsn) for entry in fake.opened] == [(1, 1, DSN)]
    assert fake.calls[0] == ('fetchval', STATEMENT_TEMPLATES['check_table'], ('drakkar.recorder_events',))
    started = _logs(captured, 'provider_started')
    assert [(line['category'], line['feature'], line['provider']) for line in started] == [
        ('recorder', 'recorder', 'postgres')
    ]


async def test_start_with_an_unmanaged_missing_table_is_a_final_start_error_retried_next_tick():
    fake = RecorderFakePostgres(table_exists=False)
    writer = _writer(fake, manage_schema=False)
    writer._queue.append(_pairs(1, 3))
    errors_before = _calls('start', 'error')

    with capture_logs() as captured:
        await writer.run_once()

    expected = recorder_missing_table_message('drakkar', 'recorder_events', 'main', 6, '')
    assert [line['error'] for line in _logs(captured, 'provider_call_failed')] == [expected]
    assert _calls('start', 'error') == errors_before + 1
    assert fake.copy_calls() == []

    fake.table_exists = True
    await writer.run_once()

    assert len(fake.opened) == 1
    assert fake.copied_ids() == [1, 2, 3]


async def test_postgres_down_at_start_retries_per_tick_and_drops_only_queue_full_then_drains():
    fake = RecorderFakePostgres()
    fake.open_error = OSError('connection refused')
    clock = FakeClock()
    queue = PostgresQueue(max_rows=1000, max_bytes=128 * MIB)
    writer = _writer(fake, clock=clock, queue=queue, resilience=_resilience(max_retries=0, breaker_failures=3))
    dropped_before = _dropped()
    skipped_before = _calls('start', 'skipped_open')

    for tick in range(5):
        assert queue.append(_pairs(1 + tick * 240, 240)) == (200 if tick == 4 else 0)
        await writer.run_once()
        clock.now += 5

    assert len(fake.opened) == 3
    assert writer._caller.breaker_open
    assert _calls('start', 'skipped_open') == skipped_before + 2
    assert _dropped() == dropped_before
    assert len(queue) == 1000

    fake.open_error = None
    clock.now += 30
    await writer.run_once()

    assert fake.copied_ids() == list(range(201, 1201))
    assert len(fake.copy_calls()) == 10
    assert len(queue) == 0
    assert _dropped() == dropped_before


# --- one COPY ---


async def test_copy_runs_after_set_local_synchronous_commit_off_and_statement_timeout_zero():
    fake = RecorderFakePostgres()
    writer = _writer(fake, table_prefix='fast')
    await writer.run_once()
    writer._queue.append(_pairs(1, 2))
    fake.calls.clear()

    await writer.run_once()

    assert [call[0] for call in fake.calls] == ['BEGIN', 'execute', 'execute', 'copy', 'COMMIT']
    assert fake.calls[1] == ('execute', RECORDER_STATEMENT_TEMPLATES['set_local'], ('synchronous_commit', 'off'))
    assert fake.calls[2] == ('execute', RECORDER_STATEMENT_TEMPLATES['set_local'], ('statement_timeout', '0'))
    _, schema, table, columns, records = fake.calls[3]
    assert (schema, table, columns) == ('drakkar', 'fast_recorder_events', COPY_COLUMNS)
    assert [record[:4] for record in records] == [(CLUSTER, WORKER, INSTANCE, 1), (CLUSTER, WORKER, INSTANCE, 2)]


async def test_written_chunk_counts_rows_and_a_backlog_drains_in_one_run():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 250))
    written_before = _counter(recorder_postgres_rows_written)

    await writer.run_once()

    assert [len(call[4]) for call in fake.copy_calls()] == [100, 100, 50]
    assert fake.copied_ids() == list(range(1, 251))
    assert _counter(recorder_postgres_rows_written) == written_before + 250
    assert _counter(recorder_postgres_queue_rows) == 0


@pytest.mark.parametrize(
    ('store_completed_output', 'expected_stdout', 'expected_stderr'),
    [
        (True, 'a' * 512 + '\n...[cut 1488 bytes]...\n' + 'z' * 512, 'boom'),
        (False, None, 'boom'),
    ],
    ids=['with_completed_output', 'without_completed_output'],
)
async def test_written_records_follow_the_output_settings(store_completed_output, expected_stdout, expected_stderr):
    fake = RecorderFakePostgres()
    writer = _writer(fake, store_completed_output=store_completed_output, max_output=1024)
    values = {
        'id': 1,
        'ts': 1.5,
        'dt': 'dt',
        'event': 'task_failed',
        'origin': 'kafka',
        'stdout': 'a' * 1000 + 'z' * 1512,
        'stderr': 'boom',
    }
    writer._queue.append([(1, tuple(values.get(column) for column in EVENT_COLUMNS))])

    await writer.run_once()

    (record,) = fake.copied
    by_column = dict(zip(COPY_COLUMNS, record, strict=True))
    assert (by_column['stdout'], by_column['stderr']) == (expected_stdout, expected_stderr)


async def test_unique_violation_counts_as_written():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 5))
    fake.fail_copies(_final_error(asyncpg.exceptions.UniqueViolationError, 'duplicate key'))
    written_before = _counter(recorder_postgres_rows_written)
    dropped_before = _dropped()

    await writer.run_once()

    assert _counter(recorder_postgres_rows_written) == written_before + 5
    assert writer._slot is None
    assert _dropped() == dropped_before


async def test_retried_chunk_is_the_same_record_set_after_the_queue_head_changed():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 3))
    fake.fail_copies(asyncpg.exceptions.ConnectionDoesNotExistError('connection lost'))
    await writer.run_once()

    writer._queue.append(_pairs(4, 3))
    await writer.run_once()

    first, second, third = fake.copy_calls()
    assert second[4] == first[4]
    assert [record[3] for record in third[4]] == [4, 5, 6]
    assert fake.copied_ids() == [1, 2, 3, 4, 5, 6]


@pytest.mark.parametrize('failure', ['transient_after_retries', 'connection_lost', 'skipped_open', 'not_started'])
async def test_kept_chunk_is_never_dropped_or_counted(failure):
    fake = RecorderFakePostgres()
    retries = 1 if failure == 'transient_after_retries' else 0
    writer = _writer(fake, resilience=_resilience(max_retries=retries, breaker_failures=1))
    writer._queue.append(_pairs(1, 4))
    if failure == 'not_started':
        fake.open_error = OSError('connection refused')
    elif failure == 'transient_after_retries':
        # An OS error is transient for the caller: one attempt and one retry, both failing.
        fake.fail_copies(OSError('connection reset'), OSError('connection reset'))
    else:
        # A Postgres connection error (SQLSTATE 08003) leaves the pool helpers as a transient error.
        fake.fail_copies(asyncpg.exceptions.ConnectionDoesNotExistError('connection lost'))
    if failure == 'skipped_open':
        await writer.run_once()
    written_before = _counter(recorder_postgres_rows_written)
    dropped_before = _dropped()
    copies_before = len(fake.copy_calls())
    errors_before = _calls('copy', 'error')

    await writer.run_once()

    assert _counter(recorder_postgres_rows_written) == written_before
    assert _dropped() == dropped_before
    if failure == 'not_started':
        assert writer._slot is None
        assert len(writer._queue) == 4
        assert fake.copy_calls() == []
        return
    assert [pair[0] for pair in writer._slot.pairs] == [1, 2, 3, 4]
    assert writer._slot.final_failures == 0
    expected_attempts = {'transient_after_retries': 2, 'connection_lost': 1, 'skipped_open': 0}[failure]
    assert len(fake.copy_calls()) - copies_before == expected_attempts
    assert _calls('copy', 'error') == errors_before + (0 if failure == 'skipped_open' else 1)


async def test_a_value_the_client_cannot_encode_is_a_final_error_and_drops_with_rejected():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 2))
    # What the client raises while it encodes an int above 2**31 - 1 for an integer column: its own
    # data error (an interface error and a ValueError, not the server's SQLSTATE class 22), then an overflow.
    fake.fail_copies(
        asyncpg.exceptions._base.DataError(
            'invalid input for query argument $5: 4294967296 (value out of int32 range)'
        ),
        OverflowError('int too big to convert'),
    )
    dropped_before = _dropped()
    errors_before = _calls('copy', 'error')

    with capture_logs() as captured:
        await writer.run_once()
    assert writer._slot.final_failures == 1
    await writer.run_once()

    rejected = _logs(captured, 'recorder_postgres_copy_rejected')
    assert [(line['sqlstate'], line['final_failures'], line['rows']) for line in rejected] == [(None, 1, 2)]
    assert _dropped()['rejected'] == dropped_before['rejected'] + 2
    assert _calls('copy', 'error') == errors_before
    assert not writer._caller.breaker_open
    assert writer._slot is None


@pytest.mark.parametrize(
    'error_class', [asyncpg.exceptions.InvalidTextRepresentationError, asyncpg.exceptions.NotNullViolationError]
)
async def test_two_row_errors_drop_with_rejected_and_log(error_class):
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 4))
    fake.fail_copies(_final_error(error_class), _final_error(error_class, 'still bad'))
    dropped_before = _dropped()

    await writer.run_once()
    assert writer._slot.final_failures == 1
    with capture_logs() as captured:
        await writer.run_once()

    assert writer._slot is None
    assert _dropped()['rejected'] == dropped_before['rejected'] + 4
    lines = _logs(captured, 'recorder_postgres_rows_dropped')
    assert [(line['log_level'], line['category'], line['reason'], line['rows'], line['error']) for line in lines] == [
        ('warning', 'recorder', 'rejected', 4, 'still bad')
    ]
    assert lines[0]['suppressed'] == 0


async def test_two_check_violations_drop_with_no_partition():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 2))
    fake.fail_copies(
        _final_error(asyncpg.exceptions.CheckViolationError, 'no partition of relation found for row'),
        _final_error(asyncpg.exceptions.CheckViolationError, 'no partition of relation found for row'),
    )
    dropped_before = _dropped()

    await writer.run_once()
    await writer.run_once()

    assert _dropped()['no_partition'] == dropped_before['no_partition'] + 2
    assert _dropped()['rejected'] == dropped_before['rejected']


async def test_check_violation_keeps_the_chunk_and_the_next_run_upkeeps_then_writes_the_same_records():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    order: list[str] = []
    real_copy_calls = fake.copy_calls

    async def upkeep_spy() -> None:
        order.append(f'upkeep after {len(real_copy_calls())} copies')
        fake.copy_failures.clear()

    writer.upkeep = upkeep_spy
    await writer.run_once()
    writer._queue.append(_pairs(1, 3))
    fake.fail_copies(_final_error(asyncpg.exceptions.CheckViolationError, 'no partition of relation found for row'))
    dropped_before = _dropped()

    with capture_logs() as captured:
        await writer.run_once()

    assert writer._slot.final_failures == 1
    assert len(_logs(captured, 'recorder_postgres_copy_rejected')) == 1
    assert writer.upkeep_due()

    fake.fail_copies(_final_error(asyncpg.exceptions.CheckViolationError, 'cured by the upkeep'))
    await writer.run_once()

    assert order == ['upkeep after 0 copies', 'upkeep after 1 copies']
    first, second = fake.copy_calls()
    assert second[4] == first[4]
    assert fake.copied_ids() == [1, 2, 3]
    assert _dropped() == dropped_before


@pytest.mark.parametrize(
    'error_class',
    [
        asyncpg.exceptions.ReadOnlySQLTransactionError,
        asyncpg.exceptions.InsufficientPrivilegeError,
        asyncpg.exceptions.InternalServerError,
        asyncpg.exceptions.LockNotAvailableError,
    ],
)
async def test_a_server_error_the_rows_did_not_cause_keeps_the_chunk_and_opens_the_breaker(error_class):
    fake = RecorderFakePostgres()
    clock = FakeClock()
    writer = _writer(fake, clock=clock, resilience=_resilience(max_retries=0, breaker_failures=3))
    await writer.run_once()
    writer._queue.append(_pairs(1, 4))
    fake.fail_copies(*(_final_error(error_class, 'not the rows') for _ in range(20)))
    dropped_before = _dropped()
    errors_before = _calls('copy', 'error')
    skipped_before = _calls('copy', 'skipped_open')

    with capture_logs() as captured:
        for _ in range(8):
            await writer.run_once()
            clock.now += 1

    assert [pair[0] for pair in writer._slot.pairs] == [1, 2, 3, 4]
    assert writer._slot.final_failures == 0
    assert _dropped() == dropped_before
    assert writer._caller.breaker_open
    assert len(fake.copy_calls()) == 3
    assert _calls('copy', 'error') == errors_before + 3
    assert _calls('copy', 'skipped_open') == skipped_before + 5
    assert _logs(captured, 'recorder_postgres_copy_rejected') == []
    assert [line['op'] for line in _logs(captured, 'provider_call_failed')] == ['copy']


async def test_a_missing_table_on_copy_starts_the_writer_again_and_writes_the_kept_chunk():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    await writer.run_once()
    writer._queue.append(_pairs(1, 3))
    fake.fail_copies(asyncpg.exceptions.UndefinedTableError('relation "drakkar.recorder_events" does not exist'))
    dropped_before = _dropped()

    await writer.run_once()

    assert [pair[0] for pair in writer._slot.pairs] == [1, 2, 3]
    assert not writer._started
    (first,) = fake.copy_calls()
    fake.table_exists = False
    fake.calls.clear()

    await writer.run_once()

    checks = [call for call in fake.calls if call[1:2] == (STATEMENT_TEMPLATES['check_table'],)]
    assert len(checks) == 2
    assert ('execute', STATEMENT_TEMPLATES['ddl_lock'], ('drakkar',)) in fake.calls
    assert writer._started
    assert fake.copied_ids() == [1, 2, 3]
    (second,) = fake.copy_calls()
    assert second[4] == first[4]
    assert _dropped() == dropped_before


async def test_a_missing_table_on_copy_with_an_unmanaged_schema_names_the_schema_command():
    fake = RecorderFakePostgres()
    writer = _writer(fake, manage_schema=False)
    await writer.run_once()
    writer._queue.append(_pairs(1, 2))
    fake.fail_copies(asyncpg.exceptions.UndefinedTableError('relation "drakkar.recorder_events" does not exist'))
    await writer.run_once()
    fake.table_exists = False

    with capture_logs() as captured:
        await writer.run_once()

    expected = recorder_missing_table_message('drakkar', 'recorder_events', 'main', 6, '')
    assert [line['error'] for line in _logs(captured, 'provider_call_failed')] == [expected]
    assert [pair[0] for pair in writer._slot.pairs] == [1, 2]


async def test_a_missing_table_in_the_upkeep_starts_the_writer_again():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    fake.fail_partition_ddl(asyncpg.exceptions.UndefinedTableError('relation "drakkar.recorder_events" does not exist'))

    await writer.run_once()

    assert not writer._started
    fake.calls.clear()

    await writer.run_once()

    assert writer._started
    assert fake.calls[0] == ('fetchval', STATEMENT_TEMPLATES['check_table'], ('drakkar.recorder_events',))


async def test_final_copy_error_is_not_a_breaker_failure():
    fake = RecorderFakePostgres()
    writer = _writer(fake, resilience=_resilience(max_retries=0, breaker_failures=2))
    writer._queue.append(_pairs(1, 1000))
    fake.fail_copies(*(_final_error(asyncpg.exceptions.InvalidTextRepresentationError) for _ in range(20)))
    dropped_before = _dropped()

    for _ in range(20):
        await writer.run_once()

    assert not writer._caller.breaker_open
    assert _dropped()['rejected'] == dropped_before['rejected'] + 10 * BATCH_ROWS
    assert len(writer._queue) == 0


async def test_first_final_error_logs_copy_rejected():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 7))
    fake.fail_copies(_final_error(asyncpg.exceptions.InvalidTextRepresentationError, 'invalid input syntax'))

    with capture_logs() as captured:
        await writer.run_once()

    lines = _logs(captured, 'recorder_postgres_copy_rejected')
    assert [
        (line['log_level'], line['category'], line['sqlstate'], line['error'], line['rows'], line['final_failures'])
        for line in lines
    ] == [('warning', 'recorder', '22P02', 'invalid input syntax', 7, 1)]
    assert lines[0]['suppressed'] == 0
    assert _logs(captured, 'provider_call_failed') == []


# --- the final stage P, on the writer ---


async def test_final_of_a_writer_that_never_started_skips_and_drops_with_shutdown():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 5))
    dropped_before = _dropped()
    skipped_before = _calls('copy', 'not_started')

    with capture_logs() as captured:
        await writer.final(5.0)

    assert fake.opened == []
    assert _calls('copy', 'not_started') == skipped_before + 1
    assert _dropped()['shutdown'] == dropped_before['shutdown'] + 5
    lines = _logs(captured, 'recorder_postgres_rows_dropped')
    assert [(line['reason'], line['rows'], line['error']) for line in lines] == [('shutdown', 5, '')]


async def test_stop_with_the_breaker_open_makes_no_attempt():
    fake = RecorderFakePostgres()
    writer = _writer(fake, resilience=_resilience(max_retries=0, breaker_failures=1))
    writer._queue.append(_pairs(1, 4))
    fake.fail_copies(asyncpg.exceptions.ConnectionDoesNotExistError('connection lost'))
    await writer.run_once()
    writer._queue.append(_pairs(5, 2))
    copies_before = len(fake.copy_calls())
    skipped_before = _calls('copy', 'skipped_open')
    dropped_before = _dropped()

    await writer.final(5.0)

    assert len(fake.copy_calls()) == copies_before
    assert _calls('copy', 'skipped_open') == skipped_before + 1
    assert _dropped()['shutdown'] == dropped_before['shutdown'] + 6
    assert writer._slot is None
    assert len(writer._queue) == 0


# --- the status the writer publishes ---


async def test_status_of_a_new_writer_is_empty():
    writer = _writer(RecorderFakePostgres())

    assert writer.status == WriterStatus()


async def test_status_after_a_run_holds_the_start_the_written_rows_and_the_upkeep():
    fake = RecorderFakePostgres()
    writer = _writer(fake, now_wall=lambda: 1_760_000_000.0)
    writer._queue.append(_pairs(1, 250))

    await writer.run_once()

    status = writer.status
    assert (status.started, status.slot_rows, status.rows_written, status.breaker_open) == (True, 0, 250, False)
    upkeep = status.last_upkeep
    assert upkeep is not None
    assert (upkeep.at, upkeep.held_lock, upkeep.dropped, upkeep.dropped_for_size) == (1_760_000_000.0, True, (), ())
    assert len(upkeep.created) == 5


async def test_status_of_an_upkeep_that_lost_the_try_lock_says_so_with_no_changes():
    fake = RecorderFakePostgres()
    fake.try_lock_answer = False
    writer = _writer(fake, now_wall=lambda: 1_760_000_000.0)

    await writer.run_once()

    assert writer.status.last_upkeep == UpkeepStatus(at=1_760_000_000.0, held_lock=False)


async def test_status_counts_a_kept_chunk_and_its_rejected_drop():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 4))
    error = asyncpg.exceptions.InvalidTextRepresentationError
    fake.fail_copies(_final_error(error), _final_error(error, 'still bad'))

    await writer.run_once()
    assert (writer.status.slot_rows, writer.status.dropped_rejected) == (4, 0)
    assert writer.status.slot_bytes == sum(row_text_bytes(row) for _, row in _pairs(1, 4))
    await writer.run_once()

    assert (writer.status.slot_rows, writer.status.slot_bytes) == (0, 0)
    assert (writer.status.dropped_rejected, writer.status.rows_written) == (4, 0)


async def test_status_counts_no_partition_and_shutdown_drops():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    writer._queue.append(_pairs(1, 2))
    violation = asyncpg.exceptions.CheckViolationError
    fake.fail_copies(_final_error(violation), _final_error(violation))
    await writer.run_once()
    await writer.run_once()
    writer._queue.append(_pairs(3, 3))
    fake.fail_copies(asyncpg.exceptions.ConnectionDoesNotExistError('connection lost'))

    await writer.final(5.0)

    assert (writer.status.dropped_no_partition, writer.status.dropped_shutdown) == (2, 3)


async def test_status_shows_the_open_breaker():
    fake = RecorderFakePostgres()
    writer = _writer(fake, resilience=_resilience(max_retries=0, breaker_failures=1))
    writer._queue.append(_pairs(1, 4))
    fake.fail_copies(asyncpg.exceptions.ConnectionDoesNotExistError('connection lost'))

    await writer.run_once()

    assert (writer.status.breaker_open, writer.status.slot_rows) == (True, 4)


async def test_recorder_stage_status_counts_queue_full_drops_and_the_queue():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, max_buffer_rows=1000)
    assert rec.postgres_stage_status() is None
    rec._pg_writer = _writer(fake, queue=rec._pg_queue)

    rec._append_downstream(_pairs(1, 1020))

    stage = rec.postgres_stage_status()
    assert stage is not None
    assert (stage.queue_rows, stage.dropped_queue_full) == (1000, 20)
    assert stage.queue_bytes == rec._pg_queue.bytes
    assert stage.writer == WriterStatus()


# --- the loop thread ---


async def test_loop_thread_runs_coroutines_on_its_own_thread_and_stops():
    worker_thread = PostgresLoopThread('drakkar-test-stage-p')

    async def where() -> str:
        return threading.current_thread().name

    assert await worker_thread.run(where, None) == 'drakkar-test-stage-p'
    assert await worker_thread.stop(2.0)
    assert not worker_thread.thread.is_alive()


# --- the recorder ---


def _recorder(
    fake: RecorderFakePostgres, *, storage_dir: str = '', table_prefix: str = '', **recorder
) -> EventRecorder:
    values = {'flush_interval': '60s', 'capture': {'output': False}, **recorder}
    return EventRecorder(
        RecorderConfig(**values),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='postgres',
        storage_dir=storage_dir,
        instance=INSTANCE,
        postgres=_settings(table_prefix=table_prefix),
        postgres_open_pool=fake.open_pool,
    )


async def _start_stage_p(rec: EventRecorder) -> None:
    """Run one stage P on the recorder's thread, so the writer has started."""
    await rec._pg_thread.run(rec._pg_writer.run_once, None)


def _record(rec: EventRecorder, count: int, first: int = 0) -> None:
    for offset in range(first, first + count):
        rec._record({'ts': time.time(), 'event': 'consumed', 'partition': 0, 'offset': offset})


async def test_the_memory_tick_wakes_stage_p_and_the_rows_land():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, flush_interval='1s')
    await rec.start()
    try:
        _record(rec, 3)
        await wait_for(lambda: fake.copied_ids() == [1, 2, 3])
    finally:
        await rec.stop()


async def test_hanging_copy_does_not_stop_stage_m_or_eviction():
    fake = RecorderFakePostgres()
    fake.hang_copies()
    rec = _recorder(fake, flush_interval='1s')
    rec._memory_max_rows = 50
    await rec.start()
    try:
        _record(rec, 10)
        await wait_for(lambda: fake.copies_started == 1)

        _record(rec, 200, first=10)
        # The memory loop's own stage M, with its eviction pass, while the COPY hangs.
        await asyncio.wait_for(rec._stage_memory(0.0), 2.0)

        _, rows = await rec.memory_store.read('SELECT count(*), max(id) FROM events')
        assert rows[0] == (50, 210)
    finally:
        await rec.stop(timeout=1.0)


async def test_stop_with_time_spent_drops_with_shutdown():
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    await rec.start()
    await _start_stage_p(rec)
    _record(rec, 6)
    dropped_before = _dropped()
    timeouts_before = _calls('copy', 'timeout')

    with capture_logs() as captured:
        await rec.stop(timeout=0.0)

    assert fake.copy_calls() == []
    assert _calls('copy', 'timeout') == timeouts_before + 1
    assert _dropped()['shutdown'] == dropped_before['shutdown'] + 6
    lines = _logs(captured, 'recorder_postgres_rows_dropped')
    assert [(line['reason'], line['rows'], line['error']) for line in lines] == [('shutdown', 6, '')]


async def test_stop_with_time_spent_gives_the_thread_its_grace_and_logs_no_thread_failure():
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    await rec.start()
    await _start_stage_p(rec)
    stage_p_thread = rec._pg_thread.thread

    with capture_logs() as captured:
        await rec.stop(timeout=0.0)

    assert not stage_p_thread.is_alive()
    errors = [line['error'] for line in _logs(captured, 'recorder_final_flush_failed')]
    assert 'the stage P thread did not end in the shutdown time left' not in errors


async def test_stop_while_a_copy_attempt_hangs_returns_within_the_time_left():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, flush_interval='1s')
    await rec.start()
    await _start_stage_p(rec)
    fake.hang_copies()
    _record(rec, 4)
    await wait_for(lambda: fake.copies_started == 1)
    dropped_before = _dropped()
    stage_p_thread = rec._pg_thread.thread

    started = time.monotonic()
    await rec.stop(timeout=1.0, reserve_seconds=0.5)
    elapsed = time.monotonic() - started

    # The COPY gets the time left minus the reserve; the stop keeps the rest of the reserve.
    assert elapsed < 0.5 + STOP_MARGIN_SECONDS
    assert fake.calls.count(('terminate',)) == 2
    assert _dropped()['shutdown'] == dropped_before['shutdown'] + 4
    assert not stage_p_thread.is_alive()


async def test_stop_while_the_pool_close_hangs_returns_within_the_time_left():
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    await rec.start()
    await _start_stage_p(rec)
    fake.hang_close = True
    _record(rec, 3)

    started = time.monotonic()
    with capture_logs() as captured:
        await rec.stop(timeout=1.0, reserve_seconds=0.5)
    elapsed = time.monotonic() - started

    assert elapsed < 0.5 + STOP_MARGIN_SECONDS
    assert fake.copied_ids() == [1, 2, 3]
    assert fake.calls[-1] == ('pool_terminate',)
    lines = _logs(captured, 'recorder_final_flush_failed')
    assert [(line['stage'], line['error']) for line in lines] == [('postgres', 'timed out')]


async def test_stop_without_a_deadline_bounds_a_hanging_pool_close(monkeypatch):
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    monkeypatch.setattr(core_mod, 'POSTGRES_STOP_STEP_SECONDS', 0.2)
    await rec.start()
    await _start_stage_p(rec)
    fake.hang_close = True

    started = time.monotonic()
    await rec.stop()

    assert time.monotonic() - started < 0.2 + STOP_MARGIN_SECONDS
    assert fake.calls[-1] == ('pool_terminate',)


async def test_stop_runs_no_upkeep_and_closes_the_pool_last():
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    upkeeps: list[float] = []

    async def upkeep_spy() -> None:
        upkeeps.append(time.monotonic())

    await rec.start()
    rec._pg_writer.upkeep = upkeep_spy
    await _start_stage_p(rec)
    rec._pg_writer._upkeep_requested = True
    _record(rec, 3)

    await rec.stop(timeout=5.0)

    assert len(upkeeps) == 1
    assert fake.copied_ids() == [1, 2, 3]
    assert fake.calls[-1] == ('close',)


async def test_postgres_mode_writes_no_file_in_storage_dir_and_no_last_breath(tmp_path):
    fake = RecorderFakePostgres()
    rec = _recorder(fake, storage_dir=str(tmp_path))
    await rec.start()
    try:
        _record(rec, 3)
        await rec.flush_memory()
        _record(rec, 2, first=3)
        rec._last_breath_flush()

        assert os.listdir(tmp_path) == []
        assert len(rec._intake) == 2
        assert len(rec._pg_queue) == 3
    finally:
        await rec.stop()
    assert os.listdir(tmp_path) == []


async def test_capture_events_off_starts_no_stage_p():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, capture={'events': False}, flush_interval='1s')
    await rec.start()
    try:
        await asyncio.sleep(0.05)
        assert rec._pg_thread is None
        assert rec._pg_writer is None
    finally:
        await rec.stop()
    assert fake.opened == []


@pytest.mark.parametrize('capture_events', [True, False], ids=['capture_events', 'no_capture_events'])
async def test_recorder_started_logs_the_postgres_fields(capture_events):
    fake = RecorderFakePostgres()
    rec = _recorder(fake, table_prefix='fast', capture={'events': capture_events})

    with capture_logs() as captured:
        await rec.start()
    await rec.stop()

    (line,) = _logs(captured, 'recorder_started')
    assert line['persistence'] == 'postgres'
    assert line['postgres_connection'] == 'main'
    assert line['postgres_table'] == 'drakkar.fast_recorder_events'


def test_postgres_settings_property():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, table_prefix='fast')

    assert rec.postgres_settings is not None
    assert rec.postgres_settings.qualified_table == 'drakkar.fast_recorder_events'


def test_copy_outcome_values():
    assert [outcome.value for outcome in CopyOutcome] == ['written', 'already_written', 'kept', 'rejected']


async def test_loop_thread_refuses_a_run_after_it_stopped():
    worker_thread = PostgresLoopThread('drakkar-test-stage-p')
    assert await worker_thread.stop(2.0)

    async def never() -> None:
        raise AssertionError('ran on a stopped loop')

    with pytest.raises(RuntimeError):
        await worker_thread.run(never, None)
    assert await worker_thread.stop(1.0)


async def test_loop_thread_cancels_the_coroutines_left_on_it_when_it_stops():
    worker_thread = PostgresLoopThread('drakkar-test-stage-p')
    left_running = worker_thread.submit(lambda: asyncio.sleep(3600))

    assert await worker_thread.stop(2.0)
    assert left_running.cancelled()


async def test_close_that_is_cancelled_terminates_the_pool(monkeypatch):
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    await writer.run_once()

    async def hanging_close(_pool) -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr('drakkar.providers.postgres.close_pool', hanging_close)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(writer.close(), 0.05)

    assert fake.calls[-1] == ('pool_terminate',)


async def test_final_that_cannot_take_the_writer_in_time_drops_with_shutdown():
    fake = RecorderFakePostgres()
    writer = _writer(fake)
    await writer.run_once()
    writer._queue.append(_pairs(1, 3))
    dropped_before = _dropped()
    await writer._lock.acquire()

    await writer.final(0.05)

    assert fake.copy_calls() == []
    assert _dropped()['shutdown'] == dropped_before['shutdown'] + 3


async def test_a_failed_stage_p_run_is_logged_and_the_loop_goes_on():
    fake = RecorderFakePostgres()
    rec = _recorder(fake, flush_interval='1s')
    await rec.start()
    runs: list[int] = []

    async def failing_run() -> None:
        runs.append(1)
        raise RuntimeError('broken run')

    rec._pg_writer.run_once = failing_run
    try:
        with capture_logs() as captured:
            await wait_for(lambda: len(runs) >= 2, timeout=5.0)
        lines = _logs(captured, 'recorder_flush_loop_iteration_failed')
        assert [(line['stage'], line['error']) for line in lines][:1] == [('postgres', 'broken run')]
    finally:
        await rec.stop()


async def test_a_failed_final_step_is_logged_and_the_stop_goes_on():
    fake = RecorderFakePostgres()
    rec = _recorder(fake)
    await rec.start()

    async def failing_close() -> None:
        raise RuntimeError('close failed')

    rec._pg_writer.close = failing_close
    with capture_logs() as captured:
        await rec.stop(timeout=5.0)

    lines = _logs(captured, 'recorder_final_flush_failed')
    assert [(line['stage'], line['error']) for line in lines] == [('postgres', 'close failed')]
    assert _logs(captured, 'recorder_stopped') != []
