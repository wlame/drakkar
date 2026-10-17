"""The connection resolver: one open per name, unknown names, close order, close failures."""

import asyncio
import re

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.config import ConnectionsConfig, ConsulConnectionConfig, PostgresConnectionConfig, RedisConnectionConfig
from drakkar.providers.connections import Connections, UnknownConnectionError


class Client:
    """A stand-in for an opened service client."""

    def __init__(self, label: str) -> None:
        self.label = label


class Factory:
    """An opener and a closer that record every call; the fakes stand in for real service clients.

    ``open_gate`` holds every open until it is set, so a test can overlap acquires.
    ``open_errors`` are raised by the next opens, one each; ``close_errors`` map a client label to the error its close raises.
    """

    def __init__(self) -> None:
        self.opened_with: list[BaseModel] = []
        self.closed: list[Client] = []
        self.open_gate: asyncio.Event | None = None
        self.open_errors: list[Exception] = []
        self.close_errors: dict[str, Exception] = {}

    async def open(self, settings: BaseModel) -> Client:
        self.opened_with.append(settings)
        if self.open_gate is not None:
            await self.open_gate.wait()
        if self.open_errors:
            raise self.open_errors.pop(0)
        return Client(label=f'client-{len(self.opened_with)}')

    async def close(self, client: Client) -> None:
        self.closed.append(client)
        if client.label in self.close_errors:
            raise self.close_errors[client.label]


@pytest.fixture
def config() -> ConnectionsConfig:
    return ConnectionsConfig(
        consul={'main': ConsulConnectionConfig()},
        redis={'main': RedisConnectionConfig(), 'cache': RedisConnectionConfig(key_prefix='cache:')},
        postgres={'main': PostgresConnectionConfig(dsn='postgresql://h/db')},
    )


@pytest.fixture
def connections(config: ConnectionsConfig) -> Connections:
    return Connections(config)


@pytest.fixture
def factory() -> Factory:
    return Factory()


async def test_acquire_opens_once_and_shares_the_client(connections, factory):
    factory.open_gate = asyncio.Event()
    first = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    second = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    for _ in range(3):
        await asyncio.sleep(0)
    assert len(factory.opened_with) == 1

    factory.open_gate.set()
    client_a, client_b = await asyncio.gather(first, second)

    assert client_a is client_b
    assert len(factory.opened_with) == 1


async def test_a_later_acquire_returns_the_same_client_without_opening(connections, factory):
    first = await connections.acquire('redis', 'main', factory.open, factory.close)
    second = await connections.acquire('redis', 'main', factory.open, factory.close)

    assert first is second
    assert len(factory.opened_with) == 1


async def test_each_kind_and_name_gets_its_own_client_opened_with_its_own_settings(connections, config, factory):
    redis_main = await connections.acquire('redis', 'main', factory.open, factory.close)
    redis_cache = await connections.acquire('redis', 'cache', factory.open, factory.close)
    postgres_main = await connections.acquire('postgres', 'main', factory.open, factory.close)

    assert len({id(redis_main), id(redis_cache), id(postgres_main)}) == 3
    assert factory.opened_with == [config.redis['main'], config.redis['cache'], config.postgres['main']]
    assert factory.opened_with[1] is config.redis['cache']


def test_settings_returns_the_declared_model(connections, config):
    assert connections.settings('redis', 'cache') is config.redis['cache']
    assert connections.settings('consul', 'main') is config.consul['main']


@pytest.mark.parametrize(
    'kind, name',
    [('redis', 'other'), ('kafka', 'main'), ('model_fields', 'main'), ('consul', 'cache')],
    ids=['unknown name', 'unknown kind', 'attribute that is not a kind', 'name of another kind'],
)
async def test_an_unknown_name_raises_with_the_path(connections, factory, kind, name):
    expected = re.escape(f'connections.{kind}.{name} is not defined')

    with pytest.raises(UnknownConnectionError, match=expected):
        connections.settings(kind, name)
    with pytest.raises(UnknownConnectionError, match=expected):
        await connections.acquire(kind, name, factory.open, factory.close)

    assert factory.opened_with == []


def test_the_unknown_connection_error_is_a_value_error():
    assert issubclass(UnknownConnectionError, ValueError)


async def test_a_failed_open_is_not_cached_and_the_next_acquire_tries_again(connections, factory):
    factory.open_errors = [ConnectionError('refused')]

    with pytest.raises(ConnectionError, match='refused'):
        await connections.acquire('redis', 'main', factory.open, factory.close)
    client = await connections.acquire('redis', 'main', factory.open, factory.close)

    assert isinstance(client, Client)
    assert len(factory.opened_with) == 2


async def test_a_failed_open_leaves_nothing_to_close(connections, factory):
    factory.open_errors = [ConnectionError('refused')]
    with pytest.raises(ConnectionError):
        await connections.acquire('redis', 'main', factory.open, factory.close)

    await connections.close_all()

    assert factory.closed == []


async def test_a_waiter_of_a_failed_open_opens_again_itself(connections, factory):
    factory.open_errors = [ConnectionError('refused')]
    first = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    second = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))

    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert isinstance(outcomes[0], ConnectionError)
    assert isinstance(outcomes[1], Client)
    assert len(factory.opened_with) == 2


async def test_a_cancelled_open_does_not_block_the_next_acquire(connections, factory):
    factory.open_gate = asyncio.Event()
    first = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    second = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    for _ in range(3):
        await asyncio.sleep(0)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    factory.open_gate.set()
    client = await second

    assert isinstance(client, Client)
    assert len(factory.opened_with) == 2


async def test_close_all_closes_each_client_once_in_reverse_order_of_opening(connections, factory):
    redis_main = await connections.acquire('redis', 'main', factory.open, factory.close)
    postgres_main = await connections.acquire('postgres', 'main', factory.open, factory.close)
    redis_cache = await connections.acquire('redis', 'cache', factory.open, factory.close)

    await connections.close_all()
    await connections.close_all()

    assert factory.closed == [redis_cache, postgres_main, redis_main]


async def test_close_all_closes_each_client_with_the_closer_given_when_it_was_opened(connections):
    closed_by: list[str] = []

    async def open_client(settings: BaseModel) -> Client:
        return Client(label='opened')

    def closer_for(tag: str):
        async def close(client: Client) -> None:
            closed_by.append(f'{tag}:{client.label}')

        return close

    await connections.acquire('redis', 'main', open_client, closer_for('redis'))
    await connections.acquire('postgres', 'main', open_client, closer_for('postgres'))

    await connections.close_all()

    assert closed_by == ['postgres:opened', 'redis:opened']


@pytest.mark.parametrize(
    'error, logged_text',
    [
        (RuntimeError('boom'), 'boom'),
        (TimeoutError(), 'TimeoutError'),
        (
            ConnectionError('could not connect to postgresql://user:hunter2@host/db: refused'),
            'could not connect to postgresql://***:***@host/db: refused',
        ),
        (
            ConnectionError('dial postgresql://host/db?password=hunter2&sslmode=require failed'),
            'dial postgresql://host/db?password=***&sslmode=require failed',
        ),
    ],
    ids=['error with a message', 'error without a message', 'password in the authority', 'password in the query'],
)
async def test_close_all_closes_each_client_once_and_logs_a_failing_closer(connections, factory, error, logged_text):
    redis_main = await connections.acquire('redis', 'main', factory.open, factory.close)
    postgres_main = await connections.acquire('postgres', 'main', factory.open, factory.close)
    factory.close_errors[postgres_main.label] = error

    with capture_logs() as logs:
        await connections.close_all()

    assert factory.closed == [postgres_main, redis_main]
    failures = [entry for entry in logs if entry['event'] == 'connection_close_failed']
    assert failures == [
        {
            'event': 'connection_close_failed',
            'log_level': 'warning',
            'category': 'fleet',
            'kind': 'postgres',
            'name': 'main',
            'error': logged_text,
        }
    ]
    assert 'hunter2' not in str(logs)


async def test_close_all_with_nothing_open_does_nothing(connections):
    await connections.close_all()


async def test_acquire_after_close_all_raises_and_opens_nothing(connections, factory):
    await connections.close_all()

    with pytest.raises(RuntimeError, match='connections are closed'):
        await connections.acquire('redis', 'main', factory.open, factory.close)

    assert factory.opened_with == []


async def test_an_open_that_finishes_after_close_all_is_closed_at_once(connections, factory):
    factory.open_gate = asyncio.Event()
    acquiring = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    for _ in range(3):
        await asyncio.sleep(0)

    await connections.close_all()
    factory.open_gate.set()
    with pytest.raises(RuntimeError, match='connections are closed'):
        await acquiring

    assert [client.label for client in factory.closed] == ['client-1']
    await connections.close_all()
    assert len(factory.closed) == 1


async def test_a_waiter_behind_an_open_does_not_open_after_close_all(connections, factory):
    factory.open_gate = asyncio.Event()
    first = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    second = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    for _ in range(3):
        await asyncio.sleep(0)

    await connections.close_all()
    factory.open_gate.set()
    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    assert len(factory.opened_with) == 1
    assert len(factory.closed) == 1


async def test_a_failing_closer_of_a_late_open_is_logged_and_the_acquire_still_raises(connections, factory):
    factory.open_gate = asyncio.Event()
    factory.close_errors['client-1'] = RuntimeError('boom')
    acquiring = asyncio.create_task(connections.acquire('redis', 'main', factory.open, factory.close))
    for _ in range(3):
        await asyncio.sleep(0)
    await connections.close_all()

    with capture_logs() as logs:
        factory.open_gate.set()
        with pytest.raises(RuntimeError, match='connections are closed'):
            await acquiring

    failures = [entry for entry in logs if entry['event'] == 'connection_close_failed']
    assert [(entry['kind'], entry['name'], entry['error']) for entry in failures] == [('redis', 'main', 'boom')]


async def test_a_cancelled_close_leaves_the_clients_not_yet_closed_for_a_second_call(connections, factory):
    redis_main = await connections.acquire('redis', 'main', factory.open, factory.close)
    postgres_main = await connections.acquire('postgres', 'main', factory.open, factory.close)
    consul_main = await connections.acquire('consul', 'main', factory.open, factory.close)
    factory.close_errors[postgres_main.label] = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await connections.close_all()
    assert factory.closed == [consul_main, postgres_main]

    await connections.close_all()
    assert factory.closed == [consul_main, postgres_main, redis_main]
