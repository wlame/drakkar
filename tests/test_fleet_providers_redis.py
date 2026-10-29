"""The ``redis`` registry provider, against an in-process Redis.

``fakeredis`` runs the real command set and the real Lua scripts, so the
provider sends exactly what it would send to a server. The provider runs the
whole registry test kit, then the tests of what it sends (the argv of every
command, pinned by the shared fixture), the ages on the server clock, the keys
of odd worker names, the index, the errors, and the breaker of the registry
service on a server that dies and comes back.
"""

import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import fakeredis
import pytest
import structlog
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from structlog.testing import capture_logs

from drakkar.config.connections import ConnectionsConfig, RedisConnectionConfig
from drakkar.config.fleet import FleetRegistryConfig
from drakkar.config.resilience import ResilienceConfig
from drakkar.fleet.card import WorkerIdentity, sanitize_worker_name, serialize_card
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.fleet.providers_redis import RedisRegistry, redis_registry_factory
from drakkar.fleet.service import FleetRegistry
from drakkar.providers.connections import Connections
from drakkar.providers.redis import DEFAULT_STATE_PREFIX, SCRIPTS
from drakkar.providers.resilience import ResilientCaller, TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from tests import fleet_testkit as testkit
from tests.redis_fake import FakeClock, RecordingClient, fake_client

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['redis']
EXAMPLE = FIXTURE['example']

PRUNE_AFTER = 600.0
OFFLINE_AFTER = 60.0
INSTANCE = 'a' * 16
OTHER_INSTANCE = 'b' * 16
URL = 'redis://user:secret@cache:6379/0'
INDEX_KEY = 'drakkar:fleet:workers'
CARD_PREFIX = 'drakkar:fleet:workers:'

IDENTITY = WorkerIdentity(
    worker='w-1',
    instance=INSTANCE,
    cluster='c',
    url='http://10.0.0.1:8080/',
    backend='python',
    version='9.9.9',
    started_at=datetime(2026, 10, 25, 9, 0, tzinfo=UTC),
)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    return FakeClock(monkeypatch)


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def expand_scripts(command: list[str]) -> list[str]:
    """Replace an ``@name`` argument with the script of that name, as the fixture's commands mean it."""
    return [SCRIPTS[argument[1:]] if argument.startswith('@') else argument for argument in command]


def _card(worker: str = 'w-1', instance: str = INSTANCE, **extra: Any) -> dict[str, Any]:
    return {
        'schema': 1,
        'worker': worker,
        'cluster': 'c',
        'url': 'http://10.0.0.5:8080/',
        'instance': instance,
        **extra,
    }


def _context(connections: Connections | None = None, worker: str = 'w-1') -> RegistryContext:
    return RegistryContext(
        worker=worker,
        instance=INSTANCE,
        cluster='c',
        logger=structlog.get_logger(),
        options={},
        connections=connections,
    )


def make_connections(**settings: Any) -> Connections:
    """The worker connections with ``connections.redis.main`` declared."""
    return Connections(ConnectionsConfig(redis={'main': RedisConnectionConfig(url=URL, **settings)}))


def _provider(
    server: fakeredis.FakeServer, prune_after: float = PRUNE_AFTER, client: Any | None = None, **overrides: Any
) -> RedisRegistry:
    """A provider on a client of ``server`` (or on ``client``), which stands for the shared client."""
    return RedisRegistry(
        connection='main',
        prune_after_seconds=prune_after,
        client=fake_client(server) if client is None else client,
        **overrides,
    )


def make_provider_factory(server: fakeredis.FakeServer):
    """What the kit calls to get another provider on the same storage."""
    return lambda: _provider(server)


async def _started(server: fakeredis.FakeServer, **overrides: Any) -> RedisRegistry:
    provider = _provider(server, **overrides)
    await provider.start(_context())
    return provider


def _invalid_card_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'fleet_card_invalid']


# --- The kit ---


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS, ids=lambda check: check.__name__)
async def test_kit_publishing_checks_pass_for_redis(check, clock, server):
    await check(make_provider_factory(server), clock.advance, PRUNE_AFTER)


@pytest.mark.parametrize('check', testkit.LIVENESS_CHECKS, ids=lambda check: check.__name__)
async def test_kit_liveness_checks_pass_for_redis(check, clock, server):
    await check(make_provider_factory(server), clock.advance, OFFLINE_AFTER)


async def test_kit_shape_and_age_checks_pass_for_redis(clock, server):
    seed = await _started(server)
    await seed.publish(_card('w-2'))

    await testkit.check_list_shape(_provider(server), testkit.make_context('w-1'))
    await testkit.check_age_is_none_or_non_negative(_provider(server), testkit.make_context('w-1'))


# --- What the provider sends ---


async def test_commands_match_the_fixture_example(clock, server):
    inputs = EXAMPLE['inputs']
    commands = EXAMPLE['commands']
    recorder = RecordingClient(fake_client(server))
    provider = RedisRegistry(
        connection='main',
        prune_after_seconds=inputs['prune_after_seconds'],
        client=recorder,
        state_prefix=inputs['prefix'],
    )
    card = inputs['card']

    await provider.start(_context())
    await provider.publish(card)
    await provider.list()
    await provider.withdraw(card['worker'], card['instance'])

    assert [list(argv) for argv in recorder.calls] == [
        expand_scripts(commands[name]) for name in ('start', 'publish', 'list', 'withdraw')
    ]
    assert commands['publish'][5] == serialize_card(card).decode('utf-8')


async def test_a_preset_prefix_goes_into_every_key(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder, state_prefix='ops.fleet:')
    await provider.start(_context())

    await provider.publish(_card())
    await provider.list()

    publish, listing = recorder.calls[1], recorder.calls[2]
    assert publish[3:5] == ('ops.fleet:fleet:workers:w-1', 'ops.fleet:fleet:workers')
    assert listing[3:5] == ('ops.fleet:fleet:workers', 'ops.fleet:fleet:workers:')


async def test_the_default_prefix_is_the_one_of_a_connection_entry(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())

    await provider.publish(_card())

    assert DEFAULT_STATE_PREFIX == 'drakkar:'
    assert recorder.calls[1][3:5] == (f'{CARD_PREFIX}w-1', INDEX_KEY)


async def test_publish_stores_the_canonical_card_with_the_prune_time_to_live(clock, server):
    raw = fake_client(server)
    provider = await _started(server, prune_after=120)
    card = _card(state='running')

    await provider.publish(card)

    assert await raw.get(f'{CARD_PREFIX}w-1') == serialize_card(card)
    assert await raw.pttl(f'{CARD_PREFIX}w-1') == 120_000
    assert await raw.pttl(INDEX_KEY) == 120_000
    assert await raw.zrange(INDEX_KEY, 0, -1) == [b'w-1']


async def test_list_age_comes_from_the_server_clock(clock, server):
    provider = await _started(server)
    await provider.publish(_card())

    clock.advance(30)
    (entry,) = await provider.list()

    assert entry.age_seconds == pytest.approx(30.0, abs=0.001)
    assert entry.online is None


async def test_list_age_keeps_the_microseconds_of_the_server_clock(clock, server):
    provider = await _started(server)
    clock.set(1_800_000_000.25)
    await provider.publish(_card())

    clock.set(1_800_000_002.0)
    (entry,) = await provider.list()

    assert entry.age_seconds == pytest.approx(1.75, abs=0.001)


async def test_list_clamps_an_age_below_zero(clock, server):
    provider = await _started(server)
    clock.set(1_800_000_010.0)
    await provider.publish(_card())

    clock.set(1_800_000_005.0)
    (entry,) = await provider.list()

    assert entry.age_seconds == 0.0


async def test_ages_ignore_the_readers_prune_value(clock, server):
    writer = await _started(server, prune_after=86_400)
    reader = await _started(server, prune_after=3_600)
    await writer.publish(_card())

    clock.advance(20)
    (entry,) = await reader.list()

    # An age taken from the TTL (the reader's prune_after_seconds minus the writer's remaining TTL)
    # would read 0 or about 82800 here.
    assert entry.age_seconds == pytest.approx(20.0, abs=0.001)


async def test_list_sends_one_command_whatever_the_keyspace(clock, server):
    raw = fake_client(server)
    await raw.mset({f'cache:{number}': 'x' for number in range(5000)})
    await raw.set('drakkar:fleet:workers:orphan', 'not an indexed card')
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    await provider.publish(_card('w-1'))
    await provider.publish(_card('w-2'))
    sent_before = len(recorder.calls)

    listed = await provider.list()

    assert len(recorder.calls) - sent_before == 1
    assert sorted(entry.card['worker'] for entry in listed) == ['w-1', 'w-2']


async def test_a_non_ascii_worker_name_round_trips_byte_for_byte(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    card = _card('wörker 1')

    await provider.publish(card)
    listed = await provider.list()

    assert sanitize_worker_name('wörker 1') == 'w_rker_1'
    assert await raw.get(f'{CARD_PREFIX}w_rker_1') == serialize_card(card)
    assert [entry.card for entry in listed] == [card]


async def test_two_names_with_one_stem_share_one_card(clock, server):
    provider = await _started(server)

    await provider.publish(_card('a.b', state='first'))
    await provider.publish(_card('a_b', state='second'))
    listed = await provider.list()

    assert [(entry.card['worker'], entry.card['state']) for entry in listed] == [('a_b', 'second')]


async def test_list_skips_an_undecodable_card_and_logs_once(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card('w-1'))
    await provider.publish(_card('w-bad'))
    await raw.set(f'{CARD_PREFIX}w-bad', 'not json')

    with capture_logs() as logs:
        first = await provider.list()
        await raw.set(f'{CARD_PREFIX}w-bad', '[]')
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


async def test_list_logs_a_bad_card_again_after_its_worker_left_the_listing(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card('w-bad'))
    await raw.set(f'{CARD_PREFIX}w-bad', 'not json')

    with capture_logs() as logs:
        await provider.list()
        clock.advance(PRUNE_AFTER + 1)
        await provider.list()
        await provider.publish(_card('w-bad'))
        await raw.set(f'{CARD_PREFIX}w-bad', 'not json')
        await provider.list()

    assert [entry['worker'] for entry in _invalid_card_logs(logs)] == ['w-bad', 'w-bad']


@pytest.mark.parametrize(
    'reply',
    [[], [b'1800000000'], [b'1800000000', b'0', b'w-1'], [b'1800000000', b'0', b'w-1', b'1800000000.000000']],
    ids=['empty', 'only_seconds', 'member_alone', 'card_missing'],
)
async def test_list_with_a_reply_of_an_unexpected_length_raises(reply):
    stub = AsyncMock()
    stub.execute_command.side_effect = [b'PONG', reply]
    provider = RedisRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, client=stub)
    await provider.start(_context())

    with pytest.raises(ValueError, match='unexpected length'):
        await provider.list()


async def test_an_expired_card_leaves_the_index(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card('w-old'))
    clock.advance(400)
    await provider.publish(_card('w-new'))
    # w-old is now past its time to live; the index lives on because w-new renewed it.
    clock.advance(PRUNE_AFTER - 400 + 1)

    listed = await provider.list()

    assert [entry.card['worker'] for entry in listed] == ['w-new']
    assert await raw.zrange(INDEX_KEY, 0, -1) == [b'w-new']


async def test_when_every_card_expired_the_listing_and_the_index_are_empty(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card())

    clock.advance(PRUNE_AFTER + 1)

    assert await provider.list() == []
    assert await raw.zrange(INDEX_KEY, 0, -1) == []


async def test_withdraw_deletes_the_card_and_its_index_entry(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card())

    await provider.withdraw('w-1', INSTANCE)

    assert await provider.list() == []
    assert await raw.exists(f'{CARD_PREFIX}w-1') == 0
    assert await raw.zrange(INDEX_KEY, 0, -1) == []


async def test_withdraw_leaves_a_card_of_another_instance(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.publish(_card(instance=OTHER_INSTANCE))

    await provider.withdraw('w-1', INSTANCE)
    listed = await provider.list()

    assert [entry.card['instance'] for entry in listed] == [OTHER_INSTANCE]
    assert await raw.zrange(INDEX_KEY, 0, -1) == [b'w-1']


async def test_withdraw_uses_the_sanitized_name_of_the_worker(clock, server):
    provider = await _started(server)
    await provider.publish(_card('wörker 1'))

    await provider.withdraw('wörker 1', INSTANCE)

    assert await provider.list() == []


# --- The preset seam ---


@pytest.mark.parametrize('prefix', ['has space', 'a/b', 'star*', 'brace{', 'line\n'])
def test_a_preset_prefix_outside_the_allowed_characters_is_refused(prefix, server):
    with pytest.raises(ValueError, match='state_prefix'):
        _provider(server, state_prefix=prefix)


@pytest.mark.parametrize('prefix', ['', 'a', 'ops.fleet:', 'A_b-c:d.e'])
def test_a_preset_prefix_inside_the_allowed_characters_is_accepted(prefix, server):
    assert _provider(server, state_prefix=prefix) is not None


# --- Start ---


async def test_start_sends_ping_and_fails_when_the_server_does_not_answer(server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    recorder.fail_next = RedisConnectionError('connection refused')

    with pytest.raises(TransientError):
        await provider.start(_context())
    with pytest.raises(RuntimeError, match='before start'):
        await provider.publish(_card())
    await provider.start(_context())
    await provider.publish(_card())

    assert [argv[0] for argv in recorder.calls] == ['PING', 'PING', 'EVAL']


async def test_start_without_connections_raises():
    provider = RedisRegistry(connection='main', prune_after_seconds=PRUNE_AFTER)

    with pytest.raises(RuntimeError, match=r'^the redis registry needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_opens_one_shared_client_with_the_prefix_of_the_entry(clock, server):
    connections = make_connections(state_prefix='ops.fleet:')
    opener = AsyncMock(return_value=RecordingClient(fake_client(server)))
    first = RedisRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, open_client=opener)
    second = RedisRegistry(connection='main', prune_after_seconds=PRUNE_AFTER, open_client=opener)

    await first.start(_context(connections, worker='w-1'))
    await second.start(_context(connections, worker='w-2'))
    await first.publish(_card('w-1'))
    await second.publish(_card('w-2'))
    listed = await first.list()

    opener.assert_awaited_once()
    assert opener.await_args.args[0].url == URL
    assert sorted(entry.card['worker'] for entry in listed) == ['w-1', 'w-2']
    assert await fake_client(server).zrange('ops.fleet:fleet:workers', 0, -1) == [b'w-1', b'w-2']


async def test_start_after_stop_raises_provider_stopped(server):
    provider = await _started(server)
    await provider.stop()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await provider.start(_context())


async def test_calls_before_start_raise(server):
    provider = _provider(server)

    for call in (provider.publish(_card()), provider.list(), provider.withdraw('w-1', INSTANCE)):
        with pytest.raises(RuntimeError, match=r'^RedisRegistry used before start\(\)$'):
            await call


# --- Stop ---


@pytest.mark.parametrize(
    'call',
    [
        lambda provider: provider.publish(_card()),
        lambda provider: provider.list(),
        lambda provider: provider.withdraw('w-1', INSTANCE),
    ],
    ids=['publish', 'list', 'withdraw'],
)
async def test_calls_after_stop_raise(call, clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    await provider.stop()
    recorder.calls.clear()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    assert recorder.calls == []


async def test_stop_leaves_the_shared_client_to_the_connections(server):
    connections = make_connections()
    client = RecordingClient(fake_client(server))
    closer = AsyncMock()
    provider = RedisRegistry(
        connection='main',
        prune_after_seconds=PRUNE_AFTER,
        open_client=AsyncMock(return_value=client),
        close_client=closer,
    )
    await provider.start(_context(connections))

    await provider.stop()
    closed_by_stop = closer.await_count
    await connections.close_all()

    assert closed_by_stop == 0
    closer.assert_awaited_once_with(client)


# --- Errors ---


async def test_a_connection_error_is_raised_transient(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    recorder.fail_next = RedisConnectionError('connection reset')

    with pytest.raises(TransientError):
        await provider.publish(_card())


async def test_a_hung_call_ends_at_its_limit(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    recorder.hang = True
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(provider.publish(_card()), 0.05)
    elapsed = time.monotonic() - started
    await provider.publish(_card())
    listed = await provider.list()

    assert elapsed < 1.0
    assert [entry.card['worker'] for entry in listed] == ['w-1']


async def test_a_dead_server_opens_the_breaker_and_a_live_one_closes_it(clock, server):
    now = [100.0]
    recorder = RecordingClient(fake_client(server))
    resilience = ResilienceConfig(timeout='1s', max_retries=0, breaker_failures=2, breaker_cooldown=0)
    service = FleetRegistry(
        _provider(server, client=recorder),
        provider_name='redis',
        config=FleetRegistryConfig(provider='redis', resilience=resilience),
        identity=IDENTITY,
        now_monotonic=lambda: now[0],
        now_epoch=lambda: 1_760_000_000.0,
        caller=ResilientCaller(resilience, feature='registry', provider='redis', now_monotonic=lambda: now[0]),
    )

    with capture_logs() as logs:
        await service._publish_tick('running')
        recorder.fail_every = RedisConnectionError('connection refused')
        await service._publish_tick('running')
        await service._publish_tick('running')
        opened = service._caller.breaker_open
        recorder.fail_every = None
        await service._publish_tick('running')

    assert opened
    assert not service._caller.breaker_open
    events = [entry['event'] for entry in logs]
    assert events.index('provider_breaker_opened') < events.index('provider_breaker_closed')
    assert await fake_client(server).zrange(INDEX_KEY, 0, -1) == [b'w-1']


# --- Registration ---


def test_builtin_providers_include_redis():
    assert BUILTIN_PROVIDERS['redis'] is redis_registry_factory


async def test_redis_registry_factory_builds_a_provider_from_the_options(clock, server, monkeypatch):
    opened_urls: list[str] = []

    def open_recording_client(url: str, **kwargs: Any) -> RecordingClient:
        opened_urls.append(url)
        return RecordingClient(fake_client(server))

    monkeypatch.setattr(Redis, 'from_url', open_recording_client)
    provider = redis_registry_factory({'connection': 'main', 'prune_after_seconds': 120})
    connections = make_connections()

    await provider.start(_context(connections))
    await provider.publish(_card())

    assert opened_urls == [URL]
    assert await fake_client(server).pttl(f'{CARD_PREFIX}w-1') == 120_000
    await connections.close_all()
