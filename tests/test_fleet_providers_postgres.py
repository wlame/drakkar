"""The ``postgres`` registry provider, against an in-memory Postgres.

No database: ``tests.pg_fake.FakePostgres`` answers only the pinned statement
texts and models the ``fleet_workers`` table on a clock the test moves. The
provider runs the whole registry test kit, then the tests of what it sends:
the statements and their arguments, the DDL at start, the error mapping, and
the breaker of the registry service on a server that dies and comes back.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.config.connections import ConnectionsConfig, PostgresConnectionConfig
from drakkar.config.fleet import FleetRegistryConfig
from drakkar.config.resilience import BreakerConfig, ResilienceConfig
from drakkar.fleet import testkit
from drakkar.fleet.card import WorkerIdentity, serialize_card
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.fleet.providers_postgres import PostgresRegistry, postgres_registry_factory
from drakkar.fleet.service import FleetRegistry
from drakkar.providers import postgres
from drakkar.providers.connections import Connections
from drakkar.providers.postgres import (
    FLEET_WORKERS_TABLE,
    TASK_LOCKS_TABLE,
    ddl_for,
    missing_table_message,
    statements_for,
)
from drakkar.providers.resilience import ResilientCaller, TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from tests.pg_fake import FakePostgres, WorkerRow

PRUNE_AFTER = 600.0
OFFLINE_AFTER = 60.0
INSTANCE = 'a' * 16
DSN = 'postgresql://user:secret@db/x'

IDENTITY = WorkerIdentity(
    worker='w-1',
    instance=INSTANCE,
    cluster='c',
    url='http://10.0.0.1:8080/',
    backend='python',
    version='9.9.9',
    started_at=datetime(2026, 10, 24, 9, 0, tzinfo=UTC),
)


@pytest.fixture
def fake() -> FakePostgres:
    return FakePostgres()


def _card(worker: str = 'w-1', instance: str = INSTANCE, **extra: Any) -> dict[str, Any]:
    return {
        'schema': 1,
        'worker': worker,
        'cluster': 'c',
        'url': 'http://10.0.0.5:8080/',
        'instance': instance,
        **extra,
    }


def _provider(fake: FakePostgres, **overrides: Any) -> PostgresRegistry:
    """A provider on the fake as an injected pool."""
    return PostgresRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, pool=fake, **overrides)


def make_connections(**settings: Any) -> Connections:
    """The worker connections with ``connections.postgres.main`` declared."""
    return Connections(ConnectionsConfig(postgres={'main': PostgresConnectionConfig(dsn=DSN, **settings)}))


def _context(connections: Connections | None = None, worker: str = 'w-1') -> RegistryContext:
    return RegistryContext(
        worker=worker,
        instance=INSTANCE,
        cluster='c',
        logger=structlog.get_logger(),
        options={},
        connections=connections,
    )


async def _started(fake: FakePostgres, **overrides: Any) -> PostgresRegistry:
    provider = _provider(fake, **overrides)
    await provider.start(_context())
    return provider


def _opener(fake: FakePostgres) -> AsyncMock:
    """An ``open_pool`` that hands out the fake and records its calls."""
    return AsyncMock(return_value=fake)


# --- The kit ---


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS, ids=lambda check: check.__name__)
async def test_kit_publishing_checks_pass_for_postgres(check, fake):
    await check(lambda: _provider(fake), fake.advance, PRUNE_AFTER)


@pytest.mark.parametrize('check', testkit.LIVENESS_CHECKS, ids=lambda check: check.__name__)
async def test_kit_liveness_checks_pass_for_postgres(check, fake):
    await check(lambda: _provider(fake), fake.advance, OFFLINE_AFTER)


async def test_kit_shape_and_age_checks_pass_for_postgres(fake):
    fake.workers['w-2'] = WorkerRow(
        instance=INSTANCE, cluster='c', card=serialize_card(_card('w-2')).decode(), heartbeat_at=fake.now
    )

    await testkit.check_list_shape(_provider(fake), testkit.make_context('w-1'))
    await testkit.check_age_is_none_or_non_negative(_provider(fake), testkit.make_context('w-1'))


# --- What the provider sends ---


async def test_publish_sends_the_pinned_upsert_with_the_canonical_card(fake):
    provider = await _started(fake)
    card = _card(state='running', version='9.9.9')

    await provider.publish(card)

    assert fake.calls[-1] == (
        statements_for('drakkar')['publish'],
        ('w-1', INSTANCE, 'c', serialize_card(card).decode()),
    )


async def test_list_prunes_then_selects(fake):
    provider = await _started(fake)
    fake.calls.clear()

    await provider.list()

    statements = statements_for('drakkar')
    assert fake.calls == [(statements['list_prune'], (PRUNE_AFTER,)), (statements['list_select'], ())]
    assert type(fake.calls[0][1][0]) is float


async def test_list_decodes_a_non_ascii_worker_name_byte_for_byte(fake):
    provider = await _started(fake)
    card = _card('wörker-ü')

    await provider.publish(card)
    listed = await provider.list()

    assert [entry.card for entry in listed] == [card]
    assert fake.workers['wörker-ü'].card == serialize_card(card).decode('utf-8')


def _bad_row(fake: FakePostgres, card: str = 'not json') -> WorkerRow:
    return WorkerRow(instance=INSTANCE, cluster='c', card=card, heartbeat_at=fake.now)


def _invalid_card_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'fleet_card_invalid']


async def test_list_skips_an_undecodable_card_and_logs_once(fake):
    provider = await _started(fake)
    await provider.publish(_card('w-1'))
    fake.workers['w-bad'] = _bad_row(fake)

    with capture_logs() as logs:
        first = await provider.list()
        fake.workers['w-bad'] = _bad_row(fake, card='[]')
        second = await provider.list()

    assert [entry.card['worker'] for entry in first] == ['w-1']
    assert [entry.card['worker'] for entry in second] == ['w-1']
    assert _invalid_card_logs(logs) == [
        {
            'event': 'fleet_card_invalid',
            'category': 'fleet',
            'worker': 'w-bad',
            'reason': 'not_json',
            'log_level': 'warning',
        }
    ]


async def test_list_logs_a_bad_card_again_after_its_worker_was_pruned(fake):
    provider = await _started(fake)
    fake.workers['w-bad'] = _bad_row(fake)

    with capture_logs() as logs:
        await provider.list()
        fake.advance(PRUNE_AFTER + 1)
        await provider.list()
        fake.workers['w-bad'] = _bad_row(fake)
        fake.calls.clear()
        await provider.list()

    assert [entry['worker'] for entry in _invalid_card_logs(logs)] == ['w-bad', 'w-bad']
    assert fake.calls[-1] == (statements_for('drakkar')['list_select'], ())


async def test_list_age_comes_from_the_server_clock(fake):
    provider = await _started(fake)
    await provider.publish(_card())

    fake.advance(30)
    (entry,) = await provider.list()

    assert entry.age_seconds == pytest.approx(30.0)
    assert entry.online is None


async def test_list_clamps_an_age_below_zero(fake):
    provider = await _started(fake)
    fake.workers['w-1'] = WorkerRow(
        instance=INSTANCE, cluster='c', card=serialize_card(_card()).decode(), heartbeat_at=fake.now + 5
    )

    (entry,) = await provider.list()

    assert entry.age_seconds == 0.0


async def test_withdraw_sends_the_pinned_delete(fake):
    provider = await _started(fake)

    await provider.withdraw('w-1', INSTANCE)

    assert fake.calls[-1] == (statements_for('drakkar')['withdraw'], ('w-1', INSTANCE))


# --- Start ---


def _managed_start_calls() -> list[tuple[str, tuple[Any, ...]]]:
    """What a start with ``manage_schema`` sends: the lock and the DDL in one transaction."""
    calls: list[tuple[str, tuple[Any, ...]]] = [('BEGIN', ()), (statements_for('drakkar')['ddl_lock'], ('drakkar',))]
    calls += [(sql, ()) for sql in ddl_for('drakkar')]
    calls.append(('COMMIT', ()))
    return calls


async def test_start_runs_the_ddl_when_manage_schema_is_true(fake):
    await _started(fake)

    assert fake.calls == _managed_start_calls()


async def test_start_checks_the_table_when_manage_schema_is_false(fake):
    await _started(fake, manage_schema=False)

    assert fake.calls == [(statements_for('drakkar')['check_table'], ('drakkar.fleet_workers',))]


async def test_start_with_a_missing_table_raises_the_pinned_message():
    fake = FakePostgres(tables_exist=False)

    with pytest.raises(RuntimeError) as raised:
        await _started(fake, manage_schema=False)

    assert str(raised.value) == missing_table_message('drakkar', 'fleet_workers', 'main')


async def test_start_with_only_the_lock_table_raises_for_the_workers_table():
    fake = FakePostgres()
    fake.tables = {TASK_LOCKS_TABLE}

    with pytest.raises(RuntimeError) as raised:
        await _started(fake, manage_schema=False)

    assert str(raised.value) == missing_table_message('drakkar', 'fleet_workers', 'main')


async def test_start_with_a_missing_table_creates_it_when_manage_schema_is_true():
    fake = FakePostgres(tables_exist=False)
    provider = await _started(fake)

    await provider.publish(_card())

    assert fake.tables == {FLEET_WORKERS_TABLE, TASK_LOCKS_TABLE}
    assert list(fake.workers) == ['w-1']


@pytest.mark.parametrize('schema', ['Bad-Name', '', '1st', 'ops fleet'])
def test_a_preset_schema_that_is_not_a_lower_case_identifier_is_refused(fake, schema):
    with pytest.raises(ValueError, match=r'must be a lower-case identifier \(\[a-z_\]\[a-z0-9_\]\*\)'):
        _provider(fake, schema=schema)


async def test_a_preset_pool_defaults_to_the_schema_of_a_connection_entry(fake):
    await _started(fake)

    assert PostgresConnectionConfig(dsn=DSN).schema_name == 'drakkar'
    assert fake.calls[1] == (statements_for('drakkar')['ddl_lock'], ('drakkar',))


async def test_start_without_connections_raises():
    provider = PostgresRegistry(connection='main', prune_after_seconds=PRUNE_AFTER)

    with pytest.raises(RuntimeError, match=r'^the postgres registry needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_opens_one_shared_pool(fake):
    connections = make_connections()
    opener = _opener(fake)
    first = PostgresRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, open_pool=opener)
    second = PostgresRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, open_pool=opener)

    await first.start(_context(connections, worker='w-1'))
    await second.start(_context(connections, worker='w-2'))
    await first.publish(_card('w-1'))
    await second.publish(_card('w-2'))

    opener.assert_awaited_once()
    assert opener.await_args.args[0].dsn == DSN
    assert sorted(fake.workers) == ['w-1', 'w-2']


async def test_start_through_the_connections_takes_the_schema_and_manage_schema_of_the_entry():
    fake = FakePostgres('ops_fleet')
    connections = make_connections(schema='ops_fleet', manage_schema=False)
    provider = PostgresRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, open_pool=_opener(fake))

    await provider.start(_context(connections))
    await provider.publish(_card())

    statements = statements_for('ops_fleet')
    assert fake.calls[0] == (statements['check_table'], ('ops_fleet.fleet_workers',))
    assert fake.calls[-1][0] == statements['publish']


async def test_start_retried_after_a_failed_ddl_runs_the_ddl_again(fake):
    provider = _provider(fake)
    fake.fail_next = asyncpg.exceptions.ConnectionDoesNotExistError('closed')

    with pytest.raises(TransientError):
        await provider.start(_context())
    await provider.start(_context())
    await provider.publish(_card())

    rolled_back_at = fake.calls.index(('ROLLBACK', ()))
    assert fake.calls[: rolled_back_at + 1] == [_managed_start_calls()[0], _managed_start_calls()[1], ('ROLLBACK', ())]
    assert fake.calls[rolled_back_at + 1 : -1] == _managed_start_calls()
    assert fake.calls[-1][0] == statements_for('drakkar')['publish']
    assert list(fake.workers) == ['w-1']


async def test_publish_before_start_raises(fake):
    provider = _provider(fake)

    with pytest.raises(RuntimeError, match='before start'):
        await provider.publish(_card())


# --- Stop ---


async def test_start_after_stop_raises_provider_stopped(fake):
    provider = await _started(fake)
    await provider.stop()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await provider.start(_context())


@pytest.mark.parametrize(
    'call',
    [
        lambda provider: provider.publish(_card()),
        lambda provider: provider.list(),
        lambda provider: provider.withdraw('w-1', INSTANCE),
    ],
    ids=['publish', 'list', 'withdraw'],
)
async def test_calls_after_stop_raise(call, fake):
    provider = await _started(fake)
    await provider.stop()
    fake.calls.clear()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    assert fake.calls == []


async def test_stop_leaves_the_shared_pool_to_the_connections(fake):
    connections = make_connections()
    closer = AsyncMock()
    provider = PostgresRegistry(
        connection='main', prune_after_seconds=PRUNE_AFTER, open_pool=_opener(fake), close_pool=closer
    )
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
        await provider.publish(_card())


async def test_an_undefined_table_error_is_raised_as_is(fake):
    provider = await _started(fake)
    fake.tables = set()

    with pytest.raises(asyncpg.exceptions.UndefinedTableError):
        await provider.list()


async def test_a_dead_server_opens_the_breaker_and_a_live_one_closes_it(fake):
    now = [100.0]
    resilience = ResilienceConfig(
        timeout_seconds=1.0, retries=0, breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=0.0)
    )
    service = FleetRegistry(
        _provider(fake),
        provider_name='postgres',
        config=FleetRegistryConfig(provider='postgres', resilience=resilience),
        identity=IDENTITY,
        now_monotonic=lambda: now[0],
        now_epoch=lambda: 1_760_000_000.0,
        caller=ResilientCaller(resilience, feature='registry', provider='postgres', now_monotonic=lambda: now[0]),
    )

    with capture_logs() as logs:
        await service._publish_tick('running')
        fake.fail_every = asyncpg.exceptions.ConnectionDoesNotExistError('x')
        await service._publish_tick('running')
        await service._publish_tick('running')
        opened = service._caller.breaker_open
        fake.fail_every = None
        await service._publish_tick('running')

    assert opened
    assert not service._caller.breaker_open
    events = [entry['event'] for entry in logs]
    assert events.index('provider_breaker_opened') < events.index('provider_breaker_closed')
    assert list(fake.workers) == ['w-1']


# --- Registration ---


def test_builtin_providers_include_postgres():
    assert BUILTIN_PROVIDERS['postgres'] is postgres_registry_factory


async def test_postgres_registry_factory_builds_a_provider_from_the_options(monkeypatch, fake):
    create_pool = AsyncMock(return_value=fake)
    monkeypatch.setattr(postgres.asyncpg, 'create_pool', create_pool)
    provider = postgres_registry_factory({'connection': 'main', 'prune_after_seconds': 120})

    await provider.start(_context(make_connections()))
    fake.calls.clear()
    await provider.list()

    assert fake.calls[0] == (statements_for('drakkar')['list_prune'], (120.0,))
    create_pool.assert_awaited_once_with(dsn=DSN, min_size=1, max_size=4)
