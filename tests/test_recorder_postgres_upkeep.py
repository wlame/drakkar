"""The partition upkeep of stage P: which partitions it creates and drops, and how one upkeep runs.

No database: the write pool is the recording fake of ``tests/recorder_pg_fake.py``,
which answers the try-lock, the server clock, the table comment and the
partition list, and keeps the partitions that committed creates and drops left.
"""

import json
import time
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from structlog.testing import capture_logs

from drakkar.config import PostgresConnectionConfig, ResilienceConfig
from drakkar.metrics import provider_calls, recorder_postgres_dropped_rows, recorder_postgres_partitions_dropped
from drakkar.providers.postgres import (
    PARTITION_DDL_TEMPLATES,
    PARTITION_MAINTENANCE_SECONDS,
    RECORDER_STATEMENT_TEMPLATES,
    SECONDS_PER_HOUR,
    partition_bounds,
    partition_name,
    render_recorder,
)
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder.memstore import MIB
from drakkar.recorder.postgres import PostgresQueue, RecorderPostgresSettings
from drakkar.recorder.postgres_writer import (
    PostgresWriter,
    UpkeepActions,
    measured_partitions,
    size_drops,
    upkeep_actions,
)
from drakkar.recorder.schema import EVENT_COLUMNS
from tests.recorder_pg_fake import RecorderFakePostgres


def _refuse_constant(name: str) -> Any:
    """Refuse ``NaN`` and ``Infinity`` so the fixture reads the same with every JSON parser."""
    raise ValueError(f'the fixture holds the non-JSON constant {name}')


FIXTURE = json.loads(
    (Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text(), parse_constant=_refuse_constant
)
UPKEEP_CASES = FIXTURE['recorder']['upkeep_cases']
SIZE_UPKEEP_CASES = FIXTURE['recorder']['size_upkeep_cases']

DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
TABLE = 'recorder_events'
QUALIFIED_TABLE = 'drakkar.recorder_events'
CONFIGURED_RETENTION_HOURS = 6
# 2026-11-03 14:30:00.25 UTC: the partitions ahead are the hours 13 to 17 of that day.
SERVER_NOW = 1793716200.25
SERVER_HOUR = int(SERVER_NOW // SECONDS_PER_HOUR)
DROP_REASONS = ('queue_full', 'rejected', 'no_partition', 'shutdown')


class FakeClock:
    """A monotonic clock the test moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _no_sleep(_seconds: float) -> None:
    return None


def _writer(
    fake: RecorderFakePostgres, clock: FakeClock, queue: PostgresQueue | None = None, *, max_size: int = 0
) -> PostgresWriter:
    settings = RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN),
        table_prefix='',
        retention_hours=CONFIGURED_RETENTION_HOURS,
        store_completed_output=False,
        max_output=0,
        max_size=max_size,
        batch_rows=100,
        queue_max_bytes=128 * MIB,
        resilience=ResilienceConfig(),
    )
    caller = ResilientCaller(
        ResilienceConfig.model_validate({'max_retries': 0}),
        feature='recorder',
        provider='postgres',
        category='recorder',
        now_monotonic=clock,
        sleep=_no_sleep,
    )
    return PostgresWriter(
        settings,
        cluster='c',
        worker='w-1',
        instance='inst-1',
        queue=queue if queue is not None else PostgresQueue(max_rows=10_000, max_bytes=128 * MIB),
        caller=caller,
        open_pool=fake.open_pool,
        now_monotonic=clock,
    )


def _fake(**values: Any) -> RecorderFakePostgres:
    fake = RecorderFakePostgres()
    fake.server_now = SERVER_NOW
    for name, value in values.items():
        setattr(fake, name, value)
    return fake


def _names(*hours: int) -> list[str]:
    return [partition_name(TABLE, hour) for hour in hours]


def _pairs(first: int, count: int, ts: float) -> list[tuple[int, tuple]]:
    pairs = []
    for row_id in range(first, first + count):
        values = {'id': row_id, 'ts': ts, 'dt': 'dt', 'event': 'consumed', 'offset': row_id, 'origin': 'kafka'}
        pairs.append((row_id, tuple(values.get(column) for column in EVENT_COLUMNS)))
    return pairs


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


def _calls(outcome: str) -> float:
    return provider_calls.labels(feature='recorder', provider='postgres', op='partitions', outcome=outcome)._value.get()


def _dropped() -> dict[str, float]:
    return {reason: recorder_postgres_dropped_rows.labels(reason=reason)._value.get() for reason in DROP_REASONS}


def _partitions_dropped() -> dict[str, float]:
    return {
        reason: recorder_postgres_partitions_dropped.labels(reason=reason)._value.get()
        for reason in ('retention', 'size')
    }


# --- which partitions ---


@pytest.mark.parametrize('case', UPKEEP_CASES, ids=[case['name'] for case in UPKEEP_CASES])
def test_upkeep_actions_cases(case):
    actions = upkeep_actions(case['table'], case['now'], case['stored_retention_hours'], case['listed'])

    assert actions == UpkeepActions(
        create=[tuple(entry) for entry in case['created']],
        drop=list(case['dropped']),
    )


@pytest.mark.parametrize('case', SIZE_UPKEEP_CASES, ids=[case['name'] for case in SIZE_UPKEEP_CASES])
def test_size_upkeep_cases(case):
    actions = upkeep_actions(case['table'], case['now'], case['retention'], case['listed'])
    measured = measured_partitions(case['table'], case['listed'], actions.drop)
    answered = {name: size for name, size in case['sizes'].items() if name in measured}

    dropped_for_size = size_drops(case['table'], case['now'], measured, answered, case['max_size'])

    assert (actions.create, actions.drop, dropped_for_size) == (
        [tuple(entry) for entry in case['create']],
        case['drop'],
        case['dropped_for_size'],
    )
    current_hour = int(case['now'] // SECONDS_PER_HOUR)
    assert partition_name(case['table'], current_hour) not in dropped_for_size


def test_measured_partitions_are_the_kept_partitions_of_the_table_ascending():
    listed = [
        *_names(SERVER_HOUR, SERVER_HOUR - 9, SERVER_HOUR - 2),
        'recorder_events_old',
        'fast_recorder_events_p2026110310',
    ]

    measured = measured_partitions(TABLE, listed, _names(SERVER_HOUR - 9))

    assert measured == _names(SERVER_HOUR - 2, SERVER_HOUR)


def test_upkeep_actions_creates_hours_h_minus_1_to_h_plus_3():
    actions = upkeep_actions(TABLE, SERVER_NOW, 1, [])

    hours = range(SERVER_HOUR - 1, SERVER_HOUR + 4)
    assert actions.create == [(partition_name(TABLE, hour), *partition_bounds(hour)) for hour in hours]
    assert actions.drop == []


def test_upkeep_actions_drops_nothing_without_a_stored_retention():
    listed = _names(SERVER_HOUR - 1000, SERVER_HOUR - 24, SERVER_HOUR - 2)

    actions = upkeep_actions(TABLE, SERVER_NOW, None, listed)

    assert actions.drop == []


def test_upkeep_actions_leaves_names_of_other_shapes_alone():
    old_hour = SERVER_HOUR - 48
    listed = [
        'recorder_events_old',
        'recorder_events_p20261103',
        'recorder_events_p2026023012',
        'recorder_events_p2026110300x',
        'fast_recorder_events_p2026110100',
        partition_name('fast_recorder_events', old_hour),
        partition_name(TABLE, old_hour),
    ]

    actions = upkeep_actions(TABLE, SERVER_NOW, 1, listed)

    assert actions.drop == [partition_name(TABLE, old_hour)]


# --- one upkeep ---


async def test_upkeep_lost_try_lock_counts_as_done_and_is_due_again_after_300_seconds():
    fake = _fake(try_lock_answer=False)
    clock = FakeClock()
    writer = _writer(fake, clock)
    ok_before = _calls('ok')

    with capture_logs() as captured:
        await writer.run_once()

    upkeep_calls = fake.calls[fake.calls.index(('BEGIN',)) :]
    assert upkeep_calls == [
        ('BEGIN',),
        ('fetchval', RECORDER_STATEMENT_TEMPLATES['try_lock'], (QUALIFIED_TABLE,)),
        ('COMMIT',),
    ]
    assert fake.partitions == set()
    assert _calls('ok') == ok_before + 1
    assert _logs(captured, 'recorder_postgres_partitions_changed') == []
    assert _logs(captured, 'recorder_postgres_retention_mismatch') == []
    assert not writer.upkeep_due()

    clock.now += PARTITION_MAINTENANCE_SECONDS - 1
    await writer.run_once()
    assert len(fake.statements('try_lock')) == 1

    clock.now += 1
    await writer.run_once()
    assert len(fake.statements('try_lock')) == 2


async def test_upkeep_runs_its_statements_in_order_in_one_transaction():
    fake = _fake()
    writer = _writer(fake, FakeClock())

    await writer.run_once()

    upkeep_calls = fake.calls[fake.calls.index(('BEGIN',)) :]
    creates = [
        (
            'execute',
            render_recorder(
                PARTITION_DDL_TEMPLATES['create'],
                schema='drakkar',
                table=TABLE,
                partition=partition_name(TABLE, hour),
                from_=partition_bounds(hour)[0],
                to=partition_bounds(hour)[1],
            ),
            (),
        )
        for hour in range(SERVER_HOUR - 1, SERVER_HOUR + 4)
    ]
    assert upkeep_calls == [
        ('BEGIN',),
        ('fetchval', RECORDER_STATEMENT_TEMPLATES['try_lock'], (QUALIFIED_TABLE,)),
        ('execute', RECORDER_STATEMENT_TEMPLATES['set_local'], ('lock_timeout', '3000')),
        ('fetchval', RECORDER_STATEMENT_TEMPLATES['server_now'], ()),
        ('fetchval', RECORDER_STATEMENT_TEMPLATES['retention_read'], (QUALIFIED_TABLE,)),
        ('fetch', RECORDER_STATEMENT_TEMPLATES['partitions_list'], (QUALIFIED_TABLE,)),
        *creates,
        ('COMMIT',),
    ]


async def test_upkeep_creates_before_it_drops_each_in_ascending_order():
    old_hours = (SERVER_HOUR - 9, SERVER_HOUR - 8)
    fake = _fake(partitions=set(_names(*old_hours)))
    writer = _writer(fake, FakeClock())

    await writer.run_once()

    drop_texts = [
        render_recorder(PARTITION_DDL_TEMPLATES['drop'], schema='drakkar', table=TABLE, partition=name)
        for name in _names(*old_hours)
    ]
    ddl = fake.partition_ddl()
    assert ddl[-2:] == drop_texts
    assert all(text.startswith('CREATE') for text in ddl[:-2])
    assert fake.partitions == set(_names(*range(SERVER_HOUR - 1, SERVER_HOUR + 4)))


async def test_upkeep_uses_the_server_clock_not_the_worker_clock():
    server_now = time.time() + 3 * SECONDS_PER_HOUR
    fake = _fake(server_now=server_now)
    writer = _writer(fake, FakeClock())

    await writer.run_once()

    server_hour = int(server_now // SECONDS_PER_HOUR)
    assert fake.partitions == set(_names(*range(server_hour - 1, server_hour + 4)))


@pytest.mark.parametrize(
    'comment',
    [None, 'retention_hours=0', 'retention_hours=169', 'retention_hours=6 ', 'history of the payments team'],
    ids=['null', 'zero', 'above_the_cap', 'trailing_space', 'free_text'],
)
async def test_upkeep_without_a_valid_retention_comment_creates_drops_nothing_and_warns_once(comment):
    old = set(_names(SERVER_HOUR - 100, SERVER_HOUR - 30))
    fake = _fake(retention_comment=comment, partitions=set(old))
    clock = FakeClock()
    writer = _writer(fake, clock)

    with capture_logs() as captured:
        await writer.run_once()
        clock.now += PARTITION_MAINTENANCE_SECONDS
        await writer.run_once()

    assert len(fake.statements('try_lock')) == 2
    assert fake.partitions == old | set(_names(*range(SERVER_HOUR - 1, SERVER_HOUR + 4)))
    mismatches = _logs(captured, 'recorder_postgres_retention_mismatch')
    assert [
        (
            line['log_level'],
            line['category'],
            line['table'],
            line['stored_retention_hours'],
            line['configured_retention_hours'],
        )
        for line in mismatches
    ] == [('warning', 'recorder', QUALIFIED_TABLE, None, CONFIGURED_RETENTION_HOURS)]


async def test_upkeep_with_a_differing_stored_retention_drops_by_the_stored_value_and_warns_once():
    # Hours 00 to 13 of the day. With 12 hours stored only 00 and 01 have
    # ended before 02:30; the configured 6 hours would also drop 02 to 07.
    listed = set(_names(*range(SERVER_HOUR - 14, SERVER_HOUR)))
    fake = _fake(retention_comment='retention_hours=12', partitions=set(listed))
    clock = FakeClock()
    writer = _writer(fake, clock)

    with capture_logs() as captured:
        await writer.run_once()
        clock.now += PARTITION_MAINTENANCE_SECONDS
        await writer.run_once()

    assert listed - fake.partitions == set(_names(SERVER_HOUR - 14, SERVER_HOUR - 13))
    mismatches = _logs(captured, 'recorder_postgres_retention_mismatch')
    assert [(line['stored_retention_hours'], line['configured_retention_hours']) for line in mismatches] == [
        (12, CONFIGURED_RETENTION_HOURS)
    ]


async def test_upkeep_logs_partitions_changed_only_on_change():
    old_hour = SERVER_HOUR - 7
    fake = _fake(partitions=set(_names(old_hour)))
    clock = FakeClock()
    writer = _writer(fake, clock)

    with capture_logs() as captured:
        await writer.run_once()
        clock.now += PARTITION_MAINTENANCE_SECONDS
        await writer.run_once()

    assert len(fake.statements('try_lock')) == 2
    changed = _logs(captured, 'recorder_postgres_partitions_changed')
    assert [
        (line['log_level'], line['category'], line['table'], line['created'], line['dropped'], line['dropped_for_size'])
        for line in changed
    ] == [
        (
            'info',
            'recorder',
            QUALIFIED_TABLE,
            _names(*range(SERVER_HOUR - 1, SERVER_HOUR + 4)),
            _names(old_hour),
            [],
        )
    ]
    assert _logs(captured, 'recorder_postgres_retention_mismatch') == []


async def test_failed_upkeep_does_not_stop_the_run():
    fake = _fake()
    fake.fail_partition_ddl(asyncpg.exceptions.LockNotAvailableError('canceling statement due to lock timeout'))
    clock = FakeClock()
    writer = _writer(fake, clock)
    writer._queue.append(_pairs(1, 3, SERVER_NOW))
    errors_before = _calls('error')

    with capture_logs() as captured:
        await writer.run_once()

    failed = _logs(captured, 'provider_call_failed')
    assert [(line['category'], line['feature'], line['op']) for line in failed] == [
        ('recorder', 'recorder', 'partitions')
    ]
    assert 'lock timeout' in failed[0]['error']
    assert _calls('error') == errors_before + 1
    assert ('ROLLBACK',) in fake.calls
    assert fake.partitions == set()
    assert _logs(captured, 'recorder_postgres_partitions_changed') == []
    assert fake.copied_ids() == [1, 2, 3]
    assert not writer.upkeep_due()

    clock.now += PARTITION_MAINTENANCE_SECONDS
    assert writer.upkeep_due()


async def test_upkeep_runs_first_after_start_then_every_300_seconds():
    fake = _fake()
    clock = FakeClock()
    writer = _writer(fake, clock)

    await writer.run_once()
    assert len(fake.statements('try_lock')) == 1

    counts = []
    for step in (PARTITION_MAINTENANCE_SECONDS - 0.5, 0.5, PARTITION_MAINTENANCE_SECONDS - 0.5, 0.5):
        clock.now += step
        await writer.run_once()
        counts.append(len(fake.statements('try_lock')))

    assert counts == [1, 2, 2, 3]


async def test_check_violation_is_cured_by_the_next_upkeep_and_the_same_records_land():
    fake = _fake(require_partition=True)
    clock = FakeClock()
    writer = _writer(fake, clock)
    await writer.run_once()
    assert fake.partitions == set(_names(*range(SERVER_HOUR - 1, SERVER_HOUR + 4)))

    # The server clock moved two hours and the next upkeep is not due yet: a
    # row of the hour after the last partition has none.
    fake.server_now = SERVER_NOW + 2 * SECONDS_PER_HOUR
    writer._queue.append(_pairs(1, 3, (SERVER_HOUR + 4) * SECONDS_PER_HOUR + 1.5))
    dropped_before = _dropped()

    with capture_logs() as captured:
        await writer.run_once()

    assert writer._slot is not None and writer._slot.final_failures == 1
    assert len(_logs(captured, 'recorder_postgres_copy_rejected')) == 1
    assert fake.copied_ids() == []
    assert writer.upkeep_due()

    await writer.run_once()

    assert partition_name(TABLE, SERVER_HOUR + 4) in fake.partitions
    first, second = fake.copy_calls()
    assert second[4] == first[4]
    assert fake.copied_ids() == [1, 2, 3]
    assert _dropped() == dropped_before


# --- the size cap ---

MIB_BYTES = 1024 * 1024
CAP = 64 * MIB_BYTES


@pytest.mark.parametrize('max_size', [0, CAP], ids=['no_cap', 'cap'])
async def test_size_statement_runs_only_with_max_size(max_size):
    fake = _fake(partitions=set(_names(*range(SERVER_HOUR - 3, SERVER_HOUR + 4))))
    writer = _writer(fake, FakeClock(), max_size=max_size)

    await writer.run_once()

    expected = (
        []
        if max_size == 0
        else [
            (
                'fetch',
                RECORDER_STATEMENT_TEMPLATES['partition_sizes'],
                ('drakkar', _names(*range(SERVER_HOUR - 3, SERVER_HOUR + 4))),
            )
        ]
    )
    assert fake.statements('partition_sizes') == expected


async def test_size_cap_drops_the_oldest_hours_after_the_retention_drops_and_logs_them():
    # Hours 06 and 07 ended more than the 6 stored hours ago; 08 to 17 are
    # measured, and the oldest before the current hour go one by one.
    listed = _names(*range(SERVER_HOUR - 8, SERVER_HOUR + 4))
    sizes = {name: 30 * MIB_BYTES for name in listed}
    fake = _fake(partitions=set(listed), sizes=sizes)
    writer = _writer(fake, FakeClock(), max_size=CAP)

    with capture_logs() as captured:
        await writer.run_once()

    # Ten partitions of 30 MiB stay after the retention: still above the cap
    # when every hour before the current one is gone.
    for_size = _names(*range(SERVER_HOUR - 6, SERVER_HOUR))
    retention = _names(SERVER_HOUR - 8, SERVER_HOUR - 7)
    drops = [
        render_recorder(PARTITION_DDL_TEMPLATES['drop'], schema='drakkar', table=TABLE, partition=name)
        for name in [*retention, *for_size]
    ]
    upkeep_calls = fake.calls[fake.calls.index(('BEGIN',)) :]
    sizes_call = ('fetch', RECORDER_STATEMENT_TEMPLATES['partition_sizes'], ('drakkar', listed[2:]))
    assert upkeep_calls[-len(drops) - 2 :] == [
        *[('execute', text, ()) for text in drops[:2]],
        sizes_call,
        *[('execute', text, ()) for text in drops[2:]],
        ('COMMIT',),
    ]
    assert fake.partitions == set(_names(*range(SERVER_HOUR, SERVER_HOUR + 4)))
    (changed,) = _logs(captured, 'recorder_postgres_partitions_changed')
    assert (changed['created'], changed['dropped'], changed['dropped_for_size']) == ([], retention, for_size)


async def test_size_cap_never_drops_the_current_hour_or_later():
    listed = _names(*range(SERVER_HOUR - 2, SERVER_HOUR + 4))
    fake = _fake(partitions=set(listed), sizes={name: 100 * MIB_BYTES for name in listed})
    writer = _writer(fake, FakeClock(), max_size=CAP)

    await writer.run_once()

    assert fake.partitions == set(_names(*range(SERVER_HOUR, SERVER_HOUR + 4)))


async def test_partitions_dropped_metric_counts_by_reason():
    listed = _names(*range(SERVER_HOUR - 8, SERVER_HOUR + 4))
    fake = _fake(partitions=set(listed), sizes={name: 30 * MIB_BYTES for name in listed})
    clock = FakeClock()
    writer = _writer(fake, clock, max_size=CAP)
    before = _partitions_dropped()

    await writer.run_once()

    after = _partitions_dropped()
    assert (after['retention'] - before['retention'], after['size'] - before['size']) == (2, 6)


async def test_a_failed_size_drop_rolls_back_and_counts_no_partition():
    listed = _names(*range(SERVER_HOUR - 3, SERVER_HOUR + 4))
    fake = _fake(partitions=set(listed), sizes={name: 30 * MIB_BYTES for name in listed})
    fake.fail_partition_ddl(asyncpg.exceptions.LockNotAvailableError('canceling statement due to lock timeout'))
    writer = _writer(fake, FakeClock(), max_size=CAP)
    before = _partitions_dropped()

    with capture_logs() as captured:
        await writer.run_once()

    assert ('ROLLBACK',) in fake.calls
    assert fake.partitions == set(listed)
    assert _partitions_dropped() == before
    assert _logs(captured, 'recorder_postgres_partitions_changed') == []


async def test_lost_try_lock_measures_nothing():
    fake = _fake(try_lock_answer=False, partitions=set(_names(SERVER_HOUR - 3)), sizes={})
    writer = _writer(fake, FakeClock(), max_size=CAP)

    await writer.run_once()

    assert fake.statements('partition_sizes') == []
