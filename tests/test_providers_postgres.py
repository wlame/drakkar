"""Unit tests for the Postgres statement corpus, rendering and pool helpers.

No database: the pool is a small recording fake with the ``acquire()`` shape
asyncpg's pool has. The statement texts are checked against the shared fixture.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.providers import postgres
from drakkar.providers.postgres import (
    DDL_TEMPLATES,
    MISSING_TABLE_MESSAGE,
    SCHEMA_PLACEHOLDER,
    STATEMENT_TEMPLATES,
    TRANSIENT_SQLSTATE_PREFIXES,
    borrow,
    borrow_transaction,
    classify,
    close_pool,
    ddl_for,
    ensure_tables,
    is_transient_sqlstate,
    missing_table_message,
    open_pool,
    render,
    render_schema_sql,
    statements_for,
)
from drakkar.providers.resilience import TransientError

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())


class FakeTransaction:
    """Records the transaction boundaries among the calls of its connection."""

    def __init__(self, calls: list[tuple[str, tuple[Any, ...]]], rollback_error: Exception | None = None) -> None:
        self._calls = calls
        self._rollback_error = rollback_error

    async def start(self) -> None:
        self._calls.append(('BEGIN', ()))

    async def commit(self) -> None:
        self._calls.append(('COMMIT', ()))

    async def rollback(self) -> None:
        self._calls.append(('ROLLBACK', ()))
        if self._rollback_error is not None:
            raise self._rollback_error


class FakeConnection:
    """Records every call as ``(sql, args)`` and answers ``fetchval`` with a fixed value."""

    def __init__(
        self,
        fetchval_answer: Any = True,
        fail_on_execute: Exception | None = None,
        terminate_error: Exception | None = None,
        rollback_error: Exception | None = None,
    ) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.terminated = False
        self._fail_on_execute = fail_on_execute
        self._fetchval_answer = fetchval_answer
        self._terminate_error = terminate_error
        self._rollback_error = rollback_error

    def terminate(self) -> None:
        self.terminated = True
        if self._terminate_error is not None:
            raise self._terminate_error

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        if self._fail_on_execute is not None:
            raise self._fail_on_execute
        return 'OK'

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self.calls, self._rollback_error)

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        self.calls.append((sql, args))
        return []

    async def fetchrow(self, sql: str, *args: Any) -> None:
        self.calls.append((sql, args))

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.calls.append((sql, args))
        return self._fetchval_answer


class FakePool:
    """A pool whose ``acquire()`` hands out one shared fake connection."""

    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.acquired = 0

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield self.conn


def test_statement_templates_match_the_fixture():
    assert {name: entry['sql'] for name, entry in FIXTURE['statements'].items()} == STATEMENT_TEMPLATES


def test_ddl_templates_match_the_fixture():
    assert list(DDL_TEMPLATES) == FIXTURE['ddl']


def test_schema_placeholder_matches_the_fixture():
    assert FIXTURE['schema_placeholder'] == SCHEMA_PLACEHOLDER


def test_transient_prefixes_match_the_fixture():
    assert list(TRANSIENT_SQLSTATE_PREFIXES) == FIXTURE['transient_sqlstate_prefixes']


def test_missing_table_message_matches_the_fixture():
    assert FIXTURE['messages']['missing_table'] == MISSING_TABLE_MESSAGE
    assert (
        missing_table_message('ops_fleet', 'archive_locks', 'main')
        == 'table ops_fleet.archive_locks does not exist and connections.postgres.main.manage_schema is false; '
        'create it with: drakkar-schema postgres --schema=ops_fleet'
    )


@pytest.mark.parametrize('case', FIXTURE['schema_command'], ids=lambda c: c['args'][1])
def test_render_schema_sql_matches_the_fixture(case):
    assert render_schema_sql(case['args'][1].removeprefix('--schema=')) == case['stdout']


def test_render_substitutes_every_placeholder():
    rendered = render(STATEMENT_TEMPLATES['acquire'], 'ops_fleet')
    assert SCHEMA_PLACEHOLDER not in rendered
    assert 'ops_fleet.archive_locks AS held' in rendered


def test_statements_for_renders_every_template():
    rendered = statements_for('ops_fleet')
    assert rendered.keys() == STATEMENT_TEMPLATES.keys()
    assert all(SCHEMA_PLACEHOLDER not in sql for sql in rendered.values())


def test_ddl_for_renders_the_templates_in_order():
    rendered = ddl_for('ops_fleet')
    assert len(rendered) == len(DDL_TEMPLATES)
    assert rendered[0] == 'CREATE SCHEMA IF NOT EXISTS ops_fleet'
    assert all(SCHEMA_PLACEHOLDER not in sql for sql in rendered)


@pytest.mark.parametrize(
    ('sqlstate', 'expected'),
    [
        ('08006', True),
        ('53300', True),
        ('57P01', True),
        ('40001', True),
        ('40P01', True),
        ('42P01', False),
        ('28P01', False),
        ('23505', False),
        (None, False),
    ],
)
def test_is_transient_sqlstate_follows_the_pinned_classes(sqlstate, expected):
    assert is_transient_sqlstate(sqlstate) is expected


def test_classify_wraps_a_connection_error_as_transient():
    original = asyncpg.exceptions.ConnectionDoesNotExistError('closed')

    result = classify(original)

    assert isinstance(result, TransientError)
    assert str(result) == str(original)
    assert result.__cause__ is original


def test_classify_leaves_an_undefined_table_error_alone():
    original = asyncpg.exceptions.UndefinedTableError('no such table')

    assert classify(original) is original


def test_classify_leaves_a_non_postgres_error_alone():
    original = ValueError('not a database error')

    assert classify(original) is original


async def test_borrow_yields_the_acquired_connection():
    pool = FakePool(FakeConnection())

    async with borrow(pool) as conn:
        assert conn is pool.conn

    assert pool.acquired == 1


async def test_borrow_converts_a_transient_postgres_error():
    pool = FakePool(FakeConnection())
    original = asyncpg.exceptions.ConnectionDoesNotExistError('closed')

    with pytest.raises(TransientError) as caught:
        async with borrow(pool):
            raise original

    assert caught.value.__cause__ is original


async def test_borrow_lets_a_permanent_postgres_error_through():
    pool = FakePool(FakeConnection())

    with pytest.raises(asyncpg.exceptions.UndefinedTableError):
        async with borrow(pool):
            raise asyncpg.exceptions.UndefinedTableError('no such table')


async def test_borrow_lets_other_errors_through():
    pool = FakePool(FakeConnection())

    with pytest.raises(ValueError, match='boom'):
        async with borrow(pool):
            raise ValueError('boom')

    assert pool.conn.terminated is False


async def test_borrow_terminates_the_connection_when_the_block_is_cancelled():
    pool = FakePool(FakeConnection())

    with pytest.raises(asyncio.CancelledError):
        async with borrow(pool):
            raise asyncio.CancelledError

    assert pool.conn.terminated is True


async def test_borrow_keeps_the_cancel_when_the_connection_is_already_detached():
    # asyncpg's pool proxy raises InterfaceError on a detached connection.
    pool = FakePool(FakeConnection(terminate_error=asyncpg.exceptions.InterfaceError('detached')))

    with pytest.raises(asyncio.CancelledError):
        async with borrow(pool):
            raise asyncio.CancelledError

    assert pool.conn.terminated is True


class ReleaseWaitsForServerPool(FakePool):
    """A pool whose release of a cancelled connection waits for a server that never answers, unless it was closed."""

    def __init__(self, conn: FakeConnection) -> None:
        super().__init__(conn)
        self.server_answers = asyncio.Event()

    @asynccontextmanager
    async def acquire(self):
        try:
            yield self.conn
        finally:
            if not self.conn.terminated:
                await asyncio.shield(self.server_answers.wait())


async def test_a_time_limit_around_borrow_holds_when_the_server_does_not_answer():
    pool = ReleaseWaitsForServerPool(FakeConnection())
    query_never_returns = asyncio.Event()

    async def call() -> None:
        async with borrow(pool):
            await query_never_returns.wait()

    limited = asyncio.ensure_future(asyncio.wait_for(call(), timeout=0.05))
    # An outer bound, so a regression fails here instead of hanging the suite.
    done, _ = await asyncio.wait({limited}, timeout=1)
    pool.server_answers.set()

    assert limited in done
    assert isinstance(limited.exception(), TimeoutError)
    assert pool.conn.terminated is True


async def test_borrow_transaction_commits_after_the_block():
    conn = FakeConnection()

    async with borrow_transaction(FakePool(conn)) as borrowed:
        await borrowed.execute('SELECT 1')

    assert conn.calls == [('BEGIN', ()), ('SELECT 1', ()), ('COMMIT', ())]


async def test_borrow_transaction_rolls_back_on_an_error():
    conn = FakeConnection()

    with pytest.raises(ValueError, match='boom'):
        async with borrow_transaction(FakePool(conn)):
            raise ValueError('boom')

    assert conn.calls == [('BEGIN', ()), ('ROLLBACK', ())]
    assert conn.terminated is False


async def test_borrow_transaction_reports_the_block_error_when_the_rollback_fails():
    lost = asyncpg.exceptions.ConnectionDoesNotExistError('lost in the block')
    conn = FakeConnection(rollback_error=asyncpg.exceptions.InterfaceError('connection is closed'))

    with pytest.raises(TransientError) as caught:
        async with borrow_transaction(FakePool(conn)):
            raise lost

    assert caught.value.__cause__ is lost
    assert conn.calls == [('BEGIN', ()), ('ROLLBACK', ())]


async def test_borrow_transaction_sends_no_rollback_when_cancelled():
    conn = FakeConnection()

    with pytest.raises(asyncio.CancelledError):
        async with borrow_transaction(FakePool(conn)):
            raise asyncio.CancelledError

    assert conn.calls == [('BEGIN', ())]
    assert conn.terminated is True


async def test_ensure_tables_runs_the_lock_and_the_ddl_in_one_transaction_when_managed():
    conn = FakeConnection()

    await ensure_tables(
        FakePool(conn),
        schema='drakkar',
        manage_schema=True,
        connection='main',
        tables=[postgres.FLEET_WORKERS_TABLE],
    )

    lock_sql = statements_for('drakkar')['ddl_lock']
    expected = [('BEGIN', ()), (lock_sql, ('drakkar',))]
    expected += [(sql, ()) for sql in ddl_for('drakkar')]
    expected.append(('COMMIT', ()))
    assert conn.calls == expected
    assert len(conn.calls) == 8


async def test_ensure_tables_converts_a_transient_error_from_the_ddl():
    conn = FakeConnection(fail_on_execute=asyncpg.exceptions.ConnectionDoesNotExistError('closed'))

    with pytest.raises(TransientError):
        await ensure_tables(FakePool(conn), schema='drakkar', manage_schema=True, connection='main', tables=[])


async def test_ensure_tables_checks_each_table_when_not_managed():
    conn = FakeConnection(fetchval_answer=True)
    tables = [postgres.FLEET_WORKERS_TABLE, postgres.ARCHIVE_LOCKS_TABLE]

    await ensure_tables(FakePool(conn), schema='drakkar', manage_schema=False, connection='main', tables=tables)

    check_sql = statements_for('drakkar')['check_table']
    # Only the two probes: no transaction and no advisory lock.
    assert conn.calls == [(check_sql, ('drakkar.fleet_workers',)), (check_sql, ('drakkar.archive_locks',))]


async def test_ensure_tables_raises_the_pinned_message_for_a_missing_table():
    conn = FakeConnection(fetchval_answer=False)

    with pytest.raises(RuntimeError) as caught:
        await ensure_tables(
            FakePool(conn),
            schema='drakkar',
            manage_schema=False,
            connection='main',
            tables=[postgres.ARCHIVE_LOCKS_TABLE, postgres.FLEET_WORKERS_TABLE],
        )

    assert str(caught.value) == missing_table_message('drakkar', 'archive_locks', 'main')
    assert len(conn.calls) == 1  # stops at the first missing table


async def test_open_pool_passes_the_pool_sizes_and_no_command_timeout(monkeypatch):
    create_pool = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr(postgres.asyncpg, 'create_pool', create_pool)
    settings = PostgresConnectionConfig(dsn='postgresql://u:p@db/x', pool_min=2, pool_max=6)

    pool = await open_pool(settings)

    assert pool is create_pool.return_value
    create_pool.assert_awaited_once_with(dsn='postgresql://u:p@db/x', min_size=2, max_size=6)


async def test_close_pool_closes_the_pool():
    pool = MagicMock()
    pool.close = AsyncMock()

    await close_pool(pool)

    pool.close.assert_awaited_once_with()
