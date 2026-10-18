"""The ``postgres`` election provider, against an in-memory Postgres.

No database: ``tests.pg_fake.FakePostgres`` answers only the pinned statement
texts and models the ``archive_locks`` table on a clock the test moves. The
provider runs the whole election test kit, then the tests of what it sends:
the statements and their arguments, the takeover log line, the table check
at start, the shared pool and the error mapping.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.config.connections import ConnectionsConfig, PostgresConnectionConfig
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers_postgres import PostgresRegistry
from drakkar.providers import postgres
from drakkar.providers.connections import Connections
from drakkar.providers.postgres import FLEET_WORKERS_TABLE, missing_table_message, statements_for
from drakkar.providers.resilience import TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from drakkar.recorder import electiontest
from drakkar.recorder.election import BUILTIN_ELECTIONS, ElectionContext
from drakkar.recorder.election_postgres import PostgresElection, postgres_election_factory
from drakkar.recorder.electiontest import INSTANCE_A, INSTANCE_B, TOKEN_A, TOKEN_B
from tests.pg_fake import FakePostgres

LEASE_SECONDS = 600.0
SCOPE = 'search-fleet'
DSN = 'postgresql://user:secret@db/x'


@pytest.fixture
def fake() -> FakePostgres:
    return FakePostgres()


def _provider(fake: FakePostgres, **overrides: Any) -> PostgresElection:
    """A provider on the fake as an injected pool."""
    return PostgresElection(connection='main', pool=fake, **overrides)


def _context(
    connections: Connections | None = None, worker: str = 'w-1', instance: str = INSTANCE_A
) -> ElectionContext:
    return ElectionContext(
        worker=worker,
        instance=instance,
        cluster='c',
        shared_dir='',
        logger=structlog.get_logger(),
        options={'connection': 'main'},
        connections=connections,
    )


async def _started(fake: FakePostgres, worker: str = 'w-1', instance: str = INSTANCE_A, **overrides: Any):
    provider = _provider(fake, **overrides)
    await provider.start(_context(worker=worker, instance=instance))
    return provider


def make_connections(**settings: Any) -> Connections:
    """The worker connections with ``connections.postgres.main`` declared."""
    return Connections(ConnectionsConfig(postgres={'main': PostgresConnectionConfig(dsn=DSN, **settings)}))


def _takeover_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'recorder_archive_lock_taken_over']


# --- The kit ---


@pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS, ids=lambda check: check.__name__)
async def test_kit_checks_pass_for_postgres(check, fake):
    await check(lambda: _provider(fake), fake.advance, LEASE_SECONDS)


# --- What the provider sends ---


async def test_acquire_runs_the_select_and_upsert_in_one_transaction(fake):
    provider = await _started(fake)
    fake.calls.clear()

    took = await provider.acquire(SCOPE, TOKEN_A, 600)

    statements = statements_for('drakkar')
    assert took is True
    assert fake.calls == [
        ('BEGIN', ()),
        (statements['acquire_select'], (SCOPE,)),
        (statements['acquire'], (SCOPE, TOKEN_A, 'w-1', INSTANCE_A, 600.0)),
        ('COMMIT', ()),
    ]
    assert type(fake.calls[2][1][4]) is float


async def test_acquire_of_a_held_scope_answers_false(fake):
    holder = await _started(fake, 'w-1', INSTANCE_A)
    other = await _started(fake, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    took = await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert took is False
    assert fake.locks[SCOPE].token == TOKEN_A


async def test_acquire_takeover_logs_the_previous_holder(fake):
    stalled = await _started(fake, 'w-stalled', INSTANCE_A)
    taker = await _started(fake, 'w-taker', INSTANCE_B)
    await stalled.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.advance(LEASE_SECONDS + 1.23456)

    with capture_logs() as logs:
        took = await taker.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert took is True
    assert _takeover_logs(logs) == [
        {
            'event': 'recorder_archive_lock_taken_over',
            'category': 'recorder',
            'provider': 'postgres',
            'scope': SCOPE,
            'previous_worker': 'w-stalled',
            'previous_instance': INSTANCE_A,
            'age_seconds': round(LEASE_SECONDS + 1.23456, 3),
            'log_level': 'warning',
        }
    ]
    assert fake.locks[SCOPE].worker == 'w-taker'


async def test_acquire_of_a_free_scope_logs_nothing(fake):
    provider = await _started(fake)

    with capture_logs() as logs:
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert _takeover_logs(logs) == []


async def test_acquire_by_the_holding_token_logs_nothing(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.advance(LEASE_SECONDS + 1)

    with capture_logs() as logs:
        took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    assert _takeover_logs(logs) == []


async def test_acquire_refused_logs_nothing(fake):
    holder = await _started(fake, 'w-1', INSTANCE_A)
    other = await _started(fake, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    with capture_logs() as logs:
        await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert _takeover_logs(logs) == []


@pytest.mark.parametrize(('tag', 'expected'), [('UPDATE 1', True), ('UPDATE 0', False), ('UPDATE 2', False)])
async def test_refresh_reads_the_rows_affected(tag, expected):
    connection = AsyncMock()
    connection.execute.return_value = tag
    pool = _OneConnectionPool(connection)
    provider = PostgresElection(connection='main', pool=pool, manage_schema=False)
    connection.fetchval.return_value = True
    await provider.start(_context())

    refreshed = await provider.refresh(SCOPE, TOKEN_A)

    assert refreshed is expected
    connection.execute.assert_awaited_with(statements_for('drakkar')['refresh'], SCOPE, TOKEN_A)


async def test_refresh_is_held_and_release_send_the_pinned_statements(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.calls.clear()

    refreshed = await provider.refresh(SCOPE, TOKEN_A)
    held = await provider.is_held(SCOPE, TOKEN_A)
    await provider.release(SCOPE, TOKEN_A)

    statements = statements_for('drakkar')
    assert (refreshed, held) == (True, True)
    assert fake.calls == [
        (statements['refresh'], (SCOPE, TOKEN_A)),
        (statements['is_held'], (SCOPE, TOKEN_A)),
        (statements['release'], (SCOPE, TOKEN_A)),
    ]
    assert fake.locks == {}


class _OneConnectionPool:
    """A pool that lends one mocked connection, for tests that set a command tag by hand."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def acquire(self) -> Any:
        pool = self

        class _Borrowed:
            async def __aenter__(self) -> Any:
                return pool._connection

            async def __aexit__(self, *exc_info: Any) -> None:
                return None

        return _Borrowed()


# --- Start ---


async def test_start_runs_the_ddl_when_manage_schema_is_true(fake):
    await _started(fake)

    assert fake.calls[0] == ('BEGIN', ())
    assert fake.calls[1] == (statements_for('drakkar')['ddl_lock'], ('drakkar',))
    assert fake.calls[-1] == ('COMMIT', ())


async def test_start_checks_only_the_locks_table_when_not_managed(fake):
    await _started(fake, manage_schema=False)

    assert fake.calls == [(statements_for('drakkar')['check_table'], ('drakkar.archive_locks',))]


async def test_start_with_a_missing_locks_table_raises_the_pinned_message():
    fake = FakePostgres()
    fake.tables = {FLEET_WORKERS_TABLE}

    with pytest.raises(RuntimeError) as raised:
        await _started(fake, manage_schema=False)

    assert str(raised.value) == missing_table_message('drakkar', 'archive_locks', 'main')


@pytest.mark.parametrize('schema', ['Bad-Name', '', '1st', 'ops fleet'])
def test_a_preset_schema_that_is_not_a_lower_case_identifier_is_refused(fake, schema):
    with pytest.raises(ValueError, match=r'must be a lower-case identifier \(\[a-z_\]\[a-z0-9_\]\*\)'):
        _provider(fake, schema=schema)


async def test_start_without_connections_raises():
    provider = PostgresElection(connection='main')

    with pytest.raises(RuntimeError, match=r'^the postgres election needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_takes_the_schema_and_manage_schema_of_the_entry():
    fake = FakePostgres('ops_fleet')
    connections = make_connections(schema='ops_fleet', manage_schema=False)
    provider = PostgresElection(connection='main', open_pool=AsyncMock(return_value=fake))

    await provider.start(_context(connections))
    await provider.is_held(SCOPE, TOKEN_A)

    statements = statements_for('ops_fleet')
    assert fake.calls == [
        (statements['check_table'], ('ops_fleet.archive_locks',)),
        (statements['is_held'], (SCOPE, TOKEN_A)),
    ]


async def test_registry_and_election_on_one_connection_share_one_pool(fake):
    connections = make_connections()
    opener = AsyncMock(return_value=fake)
    registry = PostgresRegistry(connection='main', prune_after_seconds=600, open_pool=opener)
    election = PostgresElection(connection='main', open_pool=opener)
    registry_context = RegistryContext(
        worker='w-1',
        instance=INSTANCE_A,
        cluster='c',
        logger=structlog.get_logger(),
        options={},
        connections=connections,
    )

    await registry.start(registry_context)
    await election.start(_context(connections))
    took = await election.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    opener.assert_awaited_once()
    assert opener.await_args.args[0].dsn == DSN


async def test_a_call_before_start_raises(fake):
    provider = _provider(fake)

    with pytest.raises(RuntimeError, match='before start'):
        await provider.is_held(SCOPE, TOKEN_A)


# --- Stop ---


async def test_start_after_stop_raises_provider_stopped(fake):
    provider = await _started(fake)
    await provider.stop()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await provider.start(_context())


@pytest.mark.parametrize(
    'call',
    [
        lambda provider: provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS),
        lambda provider: provider.refresh(SCOPE, TOKEN_A),
        lambda provider: provider.is_held(SCOPE, TOKEN_A),
        lambda provider: provider.release(SCOPE, TOKEN_A),
    ],
    ids=['acquire', 'refresh', 'is_held', 'release'],
)
async def test_calls_after_stop_raise_provider_stopped(call, fake):
    provider = await _started(fake)
    await provider.stop()
    fake.calls.clear()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    assert fake.calls == []


async def test_stop_leaves_the_shared_pool_to_the_connections(fake):
    connections = make_connections()
    closer = AsyncMock()
    provider = PostgresElection(connection='main', open_pool=AsyncMock(return_value=fake), close_pool=closer)
    await provider.start(_context(connections))

    await provider.stop()
    closed_by_stop = closer.await_count
    await connections.close_all()

    assert closed_by_stop == 0
    closer.assert_awaited_once_with(fake)


# --- Errors ---


async def test_a_connection_error_is_raised_transient(fake):
    provider = await _started(fake)
    fake.fail_next = asyncpg.exceptions.ConnectionDoesNotExistError('x')

    with pytest.raises(TransientError):
        await provider.is_held(SCOPE, TOKEN_A)


async def test_a_failed_upsert_rolls_the_acquire_back(fake):
    provider = await _started(fake)
    fake.calls.clear()
    real_run = fake.run

    def fail_on_upsert(sql: str, args: tuple[Any, ...]) -> Any:
        if sql == statements_for('drakkar')['acquire']:
            fake.fail_next = asyncpg.exceptions.SerializationError('retry')
        return real_run(sql, args)

    fake.run = fail_on_upsert  # type: ignore[method-assign]

    with pytest.raises(TransientError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert fake.calls[-1] == ('ROLLBACK', ())
    assert fake.locks == {}


async def test_a_cancelled_acquire_closes_the_connection_instead_of_rolling_back(fake):
    provider = await _started(fake)
    fake.calls.clear()
    real_run = fake.run

    def cancel_after_upsert(sql: str, args: tuple[Any, ...]) -> Any:
        answer = real_run(sql, args)
        if sql == statements_for('drakkar')['acquire']:
            # The time limit ends the call after the server wrote the row.
            raise asyncio.CancelledError
        return answer

    fake.run = cancel_after_upsert  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    # A ROLLBACK would wait for a server that may not answer; closing the
    # connection ends the transaction on the server instead.
    assert ('ROLLBACK', ()) not in fake.calls
    assert fake.calls[-1] == ('TERMINATE', ())
    assert fake.locks == {}


# --- Registration ---


def test_builtin_elections_include_postgres():
    assert BUILTIN_ELECTIONS['postgres'] is postgres_election_factory


async def test_postgres_election_factory_builds_a_provider_from_the_options(monkeypatch, fake):
    create_pool = AsyncMock(return_value=fake)
    monkeypatch.setattr(postgres.asyncpg, 'create_pool', create_pool)
    provider = postgres_election_factory({'connection': 'main'})

    await provider.start(_context(make_connections()))
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    create_pool.assert_awaited_once_with(dsn=DSN, min_size=1, max_size=4)
