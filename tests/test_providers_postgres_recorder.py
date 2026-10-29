"""Unit tests for the recorder events table corpus and ``ensure_recorder_table``.

No database: the pool is a recording fake that knows only the statements of
the recorder corpus, so a text that is not in the corpus fails the test. Every
text and value is checked against the ``recorder`` section of the shared
fixture.
"""

import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from drakkar.config.recorder import TABLE_PREFIX_RE
from drakkar.providers import postgres
from drakkar.providers.postgres import (
    COPY_COLUMNS,
    COPY_STATEMENT_TIMEOUT,
    LOCK_TIMEOUT_MS,
    PARTITION_DDL_TEMPLATES,
    PARTITION_MAINTENANCE_SECONDS,
    PARTITIONS_AHEAD_HOURS,
    POSTGRES_CHUNK_FINAL_FAILURES,
    POSTGRES_CHUNK_MAX_BYTES,
    RECORDER_DDL_TEMPLATES,
    RECORDER_FINAL_SQLSTATE_CLASSES,
    RECORDER_MISSING_TABLE_COMMAND,
    RECORDER_MISSING_TABLE_MESSAGE,
    RECORDER_MISSING_TABLE_PREFIX_FLAG,
    RECORDER_PLACEHOLDERS,
    RECORDER_STATEMENT_TEMPLATES,
    SET_LOCAL_VALUES,
    STATEMENT_TEMPLATES,
    STATEMENT_TIMEOUT_SETTING,
    ensure_recorder_table,
    is_recorder_final_sqlstate,
    parse_partition_hour,
    parse_retention_comment,
    partition_bounds,
    partition_name,
    recorder_ddl_for,
    recorder_missing_table_message,
    recorder_table_name,
    render_recorder,
    render_recorder_schema_sql,
)
from drakkar.recorder.schema import EVENT_COLUMNS


def _refuse_constant(name: str) -> Any:
    """Refuse ``NaN`` and ``Infinity`` so the fixture reads the same with every JSON parser."""
    raise ValueError(f'the fixture holds the non-JSON constant {name}')


FIXTURE = json.loads(
    (Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text(), parse_constant=_refuse_constant
)
RECORDER = FIXTURE['recorder']
IDENTIFIER_LIMIT_BYTES = 63
LONGEST_INDEX_SUFFIX = '_trace_offsets_idx'


def test_recorder_templates_match_the_fixture():
    assert list(RECORDER_PLACEHOLDERS) == RECORDER['placeholders']
    assert list(RECORDER_DDL_TEMPLATES) == RECORDER['ddl']
    assert RECORDER['partition_ddl'] == PARTITION_DDL_TEMPLATES
    assert {name: entry['sql'] for name, entry in RECORDER['statements'].items()} == RECORDER_STATEMENT_TEMPLATES
    assert list(COPY_COLUMNS) == RECORDER['copy_columns']
    expected_messages = {
        'missing_table': RECORDER_MISSING_TABLE_MESSAGE,
        'missing_table_command': RECORDER_MISSING_TABLE_COMMAND,
        'missing_table_prefix_flag': RECORDER_MISSING_TABLE_PREFIX_FLAG,
    }
    assert expected_messages == RECORDER['messages']
    constants = RECORDER['constants']
    assert constants == {
        'partitions_ahead_hours': PARTITIONS_AHEAD_HOURS,
        'partition_maintenance_seconds': PARTITION_MAINTENANCE_SECONDS,
        'lock_timeout_ms': LOCK_TIMEOUT_MS,
        'chunk_max_bytes': POSTGRES_CHUNK_MAX_BYTES,
        'chunk_final_failures': POSTGRES_CHUNK_FINAL_FAILURES,
        'set_local_values': SET_LOCAL_VALUES,
        'statement_timeout_setting': STATEMENT_TIMEOUT_SETTING,
        'copy_statement_timeout': COPY_STATEMENT_TIMEOUT,
    }
    assert list(RECORDER_FINAL_SQLSTATE_CLASSES) == RECORDER['final_sqlstate_classes']


@pytest.mark.parametrize(
    ('sqlstate', 'expected'),
    [
        ('22P02', True),
        ('22003', True),
        ('23502', True),
        ('23505', True),
        ('23514', True),
        ('25006', False),
        ('42501', False),
        ('42P01', False),
        ('58030', False),
        ('XX000', False),
        ('55P03', False),
        ('2', False),
        (None, False),
    ],
)
def test_is_recorder_final_sqlstate_follows_the_pinned_classes(sqlstate, expected):
    assert is_recorder_final_sqlstate(sqlstate) is expected


def test_recorder_statements_do_not_repeat_the_provider_statements():
    # check_table and ddl_lock are shared with the providers and live only there.
    assert not RECORDER_STATEMENT_TEMPLATES.keys() & STATEMENT_TEMPLATES.keys()


def test_copy_columns_are_the_writer_the_event_columns_and_trace_offsets():
    assert COPY_COLUMNS[:3] == ('cluster', 'worker', 'instance')
    assert COPY_COLUMNS[3:23] == EVENT_COLUMNS
    assert COPY_COLUMNS[23:] == ('trace_offsets',)


def test_recorder_ddl_renders_schema_table_and_retention():
    example = RECORDER['ddl_example']

    rendered = recorder_ddl_for(example['schema'], example['table'], example['retention_hours'])

    assert rendered == example['ddl']
    assert len(rendered) == 6
    assert "COMMENT ON TABLE drakkar.recorder_events IS 'retention_hours=6'" in rendered
    assert all('{' not in statement for statement in rendered)


def test_recorder_ddl_keeps_the_gin_index_without_a_pending_list():
    assert 'USING gin (trace_offsets) WITH (fastupdate = off)' in RECORDER_DDL_TEMPLATES[-1]


def test_render_recorder_fills_the_partition_templates():
    sql = render_recorder(
        PARTITION_DDL_TEMPLATES['create'],
        schema='drakkar',
        table='recorder_events',
        partition='recorder_events_p2026110314',
        from_=1793714400,
        to=1793718000,
    )

    assert sql == (
        'CREATE TABLE IF NOT EXISTS drakkar.recorder_events_p2026110314 PARTITION OF drakkar.recorder_events '
        'FOR VALUES FROM (1793714400) TO (1793718000)'
    )


def test_render_recorder_schema_sql_has_the_providers_script_format():
    script = render_recorder_schema_sql('drakkar', 'recorder_events', 6)

    assert script == '\n'.join(f'{statement};\n' for statement in RECORDER['ddl_example']['ddl'])


@pytest.mark.parametrize('case', RECORDER['table_names'], ids=lambda case: case['prefix'] or 'empty')
def test_recorder_table_name(case):
    assert recorder_table_name(case['prefix']) == case['table']


@pytest.mark.parametrize('case', RECORDER['prefix_cases'], ids=lambda case: repr(case['prefix']))
def test_prefix_cases_follow_the_prefix_rule(case):
    prefix = case['prefix']

    assert (prefix == '' or re.fullmatch(TABLE_PREFIX_RE, prefix) is not None) is case['valid']


def test_longest_index_name_fits_63_bytes():
    longest_index = recorder_table_name('a' * 24) + LONGEST_INDEX_SUFFIX

    assert len(longest_index.encode()) == 58
    assert len(longest_index.encode()) <= IDENTIFIER_LIMIT_BYTES


@pytest.mark.parametrize('case', RECORDER['partition_names'], ids=lambda case: case['name'])
def test_partition_name_and_bounds(case):
    assert partition_name(case['table'], case['hour']) == case['name']
    assert partition_bounds(case['hour']) == (case['from'], case['to'])
    assert parse_partition_hour(case['table'], case['name']) == case['hour']


@pytest.mark.parametrize('case', RECORDER['partition_parse_cases'], ids=lambda case: repr(case['name']))
def test_parse_partition_hour_refuses_other_shapes(case):
    assert parse_partition_hour(case['table'], case['name']) == case['hour']


@pytest.mark.parametrize('case', RECORDER['retention_comment_cases'], ids=lambda case: repr(case['text']))
def test_parse_retention_comment(case):
    assert parse_retention_comment(case['text']) == case['expected']


@pytest.mark.parametrize('case', RECORDER['missing_table_cases'], ids=lambda case: case['prefix'] or 'no-prefix')
def test_recorder_missing_table_message_names_the_command(case):
    message = recorder_missing_table_message(
        case['schema'], case['table'], case['connection'], case['retention_hours'], case['prefix']
    )

    assert message == case['message']
    assert message.endswith(' --table-prefix=heavy') is bool(case['prefix'])


class RecordingConnection:
    """A connection that records ``(sql, args)`` and knows the recorder table as a set of names.

    ``check_table`` answers from ``tables``; the ``CREATE TABLE`` of the recorder
    DDL adds the table. ``appears_after_lock`` names a table another worker
    creates while this one waits for the advisory lock. Any text outside the
    recorder corpus fails the test.
    """

    def __init__(self, schema: str, table: str, retention_hours: int, *, appears_after_lock: str | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.tables: set[str] = set()
        self._appears_after_lock = appears_after_lock
        self._qualified = f'{schema}.{table}'
        self._ddl = recorder_ddl_for(schema, table, retention_hours)
        self._known = {
            STATEMENT_TEMPLATES['check_table'],
            STATEMENT_TEMPLATES['ddl_lock'],
            RECORDER_STATEMENT_TEMPLATES['set_local'],
            *self._ddl,
        }

    def _record(self, sql: str, args: tuple[Any, ...]) -> None:
        if sql not in self._known:
            raise AssertionError(f'statement not in the recorder corpus: {sql!r}')
        self.calls.append((sql, args))

    async def execute(self, sql: str, *args: Any) -> str:
        self._record(sql, args)
        if sql == STATEMENT_TEMPLATES['ddl_lock'] and self._appears_after_lock is not None:
            self.tables.add(self._appears_after_lock)
        if sql == self._ddl[1]:
            self.tables.add(self._qualified)
        return 'OK'

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self._record(sql, args)
        return args[0] in self.tables

    def transaction(self) -> 'RecordingTransaction':
        return RecordingTransaction(self.calls)

    def terminate(self) -> None:
        raise AssertionError('no call is cancelled in these tests')


class RecordingTransaction:
    """Records the transaction boundaries among the calls of its connection."""

    def __init__(self, calls: list[tuple[str, tuple[Any, ...]]]) -> None:
        self._calls = calls

    async def start(self) -> None:
        self._calls.append(('BEGIN', ()))

    async def commit(self) -> None:
        self._calls.append(('COMMIT', ()))

    async def rollback(self) -> None:
        self._calls.append(('ROLLBACK', ()))


class RecordingPool:
    """A pool whose ``acquire()`` hands out one shared recording connection."""

    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


CHECK = STATEMENT_TEMPLATES['check_table']
LOCK = STATEMENT_TEMPLATES['ddl_lock']
SET_LOCAL = RECORDER_STATEMENT_TEMPLATES['set_local']


async def _ensure(conn: RecordingConnection, *, manage_schema: bool, prefix: str = '') -> None:
    await ensure_recorder_table(
        RecordingPool(conn),
        schema='drakkar',
        table=recorder_table_name(prefix),
        retention_hours=6,
        manage_schema=manage_schema,
        connection='main',
        prefix=prefix,
    )


@pytest.mark.parametrize('manage_schema', [True, False])
async def test_ensure_recorder_table_present_runs_only_check_table(manage_schema):
    conn = RecordingConnection('drakkar', 'recorder_events', 6)
    conn.tables.add('drakkar.recorder_events')

    await _ensure(conn, manage_schema=manage_schema)

    assert conn.calls == [(CHECK, ('drakkar.recorder_events',))]


async def test_ensure_recorder_table_missing_and_managed_runs_lock_timeout_recheck_and_ddl():
    conn = RecordingConnection('drakkar', 'recorder_events', 6)

    await _ensure(conn, manage_schema=True)

    ddl = recorder_ddl_for('drakkar', 'recorder_events', 6)
    assert conn.calls == [
        (CHECK, ('drakkar.recorder_events',)),
        ('BEGIN', ()),
        (LOCK, ('drakkar',)),
        (SET_LOCAL, ('lock_timeout', '3000')),
        (CHECK, ('drakkar.recorder_events',)),
        *[(statement, ()) for statement in ddl],
        ('COMMIT', ()),
    ]
    assert "COMMENT ON TABLE drakkar.recorder_events IS 'retention_hours=6'" in ddl
    assert conn.tables == {'drakkar.recorder_events'}


async def test_ensure_recorder_table_missing_and_unmanaged_raises_the_pinned_message():
    conn = RecordingConnection('drakkar', 'heavy_recorder_events', 6)

    with pytest.raises(RuntimeError) as caught:
        await _ensure(conn, manage_schema=False, prefix='heavy')

    assert str(caught.value) == (
        'table drakkar.heavy_recorder_events does not exist and connections.postgres.main.manage_schema is false; '
        'create it with: drakkar-schema postgres --schema=drakkar --recorder --retention-hours=6 --table-prefix=heavy'
    )
    assert conn.calls == [(CHECK, ('drakkar.heavy_recorder_events',))]


async def test_ensure_recorder_table_recheck_finds_the_table_and_runs_no_ddl():
    conn = RecordingConnection('drakkar', 'recorder_events', 6, appears_after_lock='drakkar.recorder_events')

    await _ensure(conn, manage_schema=True)

    assert conn.calls == [
        (CHECK, ('drakkar.recorder_events',)),
        ('BEGIN', ()),
        (LOCK, ('drakkar',)),
        (SET_LOCAL, ('lock_timeout', '3000')),
        (CHECK, ('drakkar.recorder_events',)),
        ('COMMIT', ()),
    ]


async def test_ensure_recorder_table_never_creates_the_provider_tables():
    conn = RecordingConnection('drakkar', 'recorder_events', 6)

    await _ensure(conn, manage_schema=True)

    executed = ' '.join(sql for sql, _ in conn.calls)
    assert postgres.FLEET_WORKERS_TABLE not in executed
    assert postgres.TASK_LOCKS_TABLE not in executed
