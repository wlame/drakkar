"""The ``redis`` election provider, against an in-process Redis.

``fakeredis`` runs the real command set and the real Lua scripts, so the
provider sends exactly what it would send to a server. The provider runs the
whole election test kit, then the tests of what it sends (the argv of every
command, pinned by the shared fixture), the lease kept in the key's time to
live, the lease length remembered per scope, the shared client and the errors.
"""

import json
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
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers_redis import RedisRegistry
from drakkar.providers.connections import Connections
from drakkar.providers.redis import DEFAULT_KEY_PREFIX, SCRIPTS
from drakkar.providers.resilience import TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from drakkar.recorder import electiontest
from drakkar.recorder.election import BUILTIN_ELECTIONS, ElectionContext
from drakkar.recorder.election_redis import REFRESH_BEFORE_ACQUIRE, RedisElection, redis_election_factory
from drakkar.recorder.electiontest import INSTANCE_A, INSTANCE_B, TOKEN_A, TOKEN_B
from tests.redis_fake import FakeClock, RecordingClient, fake_client

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['redis']
EXAMPLE = FIXTURE['example']

LEASE_SECONDS = 600.0
LEASE_MILLISECONDS = 600_000
SCOPE = 'search-fleet'
LOCK_KEY = f'drakkar:archive-locks:{SCOPE}'
URL = 'redis://user:secret@cache:6379/0'
CANONICAL_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    return FakeClock(monkeypatch)


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def expand_scripts(command: list[str]) -> list[str]:
    """Replace an ``@name`` argument with the script of that name, as the fixture's commands mean it."""
    return [SCRIPTS[argument[1:]] if argument.startswith('@') else argument for argument in command]


def _epoch_of(text: str) -> float:
    """The epoch seconds of a canonical timestamp text such as ``2026-10-14T02:00:00.125000Z``."""
    return datetime.strptime(text, CANONICAL_TIME_FORMAT).replace(tzinfo=UTC).timestamp()


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


def make_connections(**settings: Any) -> Connections:
    """The worker connections with ``connections.redis.main`` declared."""
    return Connections(ConnectionsConfig(redis={'main': RedisConnectionConfig(url=URL, **settings)}))


def _provider(server: fakeredis.FakeServer, client: Any | None = None, **overrides: Any) -> RedisElection:
    """A provider on a client of ``server`` (or on ``client``), which stands for the shared client."""
    return RedisElection(connection='main', client=fake_client(server) if client is None else client, **overrides)


async def _started(
    server: fakeredis.FakeServer, worker: str = 'w-1', instance: str = INSTANCE_A, **overrides: Any
) -> RedisElection:
    provider = _provider(server, **overrides)
    await provider.start(_context(worker=worker, instance=instance))
    return provider


def _takeover_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'recorder_archive_lock_taken_over']


# --- The kit ---


@pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS, ids=lambda check: check.__name__)
async def test_kit_checks_pass_for_redis(check, clock, server):
    await check(lambda: _provider(server), clock.advance, LEASE_SECONDS)


# --- What the provider sends ---


async def test_commands_match_the_fixture_example(clock, server):
    inputs = EXAMPLE['inputs']
    commands = EXAMPLE['commands']
    clock.set(_epoch_of(inputs['acquired_at']))
    recorder = RecordingClient(fake_client(server))
    provider = RedisElection(connection='main', client=recorder, key_prefix=inputs['prefix'])
    card = inputs['card']
    scope, token = inputs['scope'], inputs['token']

    await provider.start(_context(worker=card['worker'], instance=card['instance']))
    await provider.acquire(scope, token, inputs['lease_seconds'])
    await provider.refresh(scope, token)
    await provider.is_held(scope, token)
    await provider.release(scope, token)

    assert [list(argv) for argv in recorder.calls] == [
        expand_scripts(commands[name]) for name in ('start', 'acquire', 'refresh', 'is_held', 'release')
    ]


async def test_a_preset_prefix_goes_into_the_lock_key(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder, key_prefix='ops.fleet:')
    await provider.start(_context())

    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert recorder.calls[1][3] == f'ops.fleet:archive-locks:{SCOPE}'


async def test_the_default_prefix_is_the_one_of_a_connection_entry(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())

    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert DEFAULT_KEY_PREFIX == 'drakkar:'
    assert recorder.calls[1][3] == LOCK_KEY


async def test_acquire_stores_the_lock_body_with_the_lease_as_the_time_to_live(clock, server):
    raw = fake_client(server)
    provider = await _started(server, 'search-worker-3', INSTANCE_A)
    clock.set(_epoch_of('2026-10-14T02:00:00.125000Z'))

    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    assert json.loads(await raw.get(LOCK_KEY)) == {
        'acquired_at': '2026-10-14T02:00:00.125000Z',
        'instance': INSTANCE_A,
        'provider': 'redis',
        'token': TOKEN_A,
        'worker': 'search-worker-3',
    }
    assert await raw.pttl(LOCK_KEY) == LEASE_MILLISECONDS


async def test_acquire_by_the_holder_keeps_the_body_and_moves_the_ttl(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    body = await raw.get(LOCK_KEY)

    clock.advance(100)
    took_again = await provider.acquire(SCOPE, TOKEN_A, 900)

    assert took_again is True
    assert await raw.get(LOCK_KEY) == body
    assert await raw.pttl(LOCK_KEY) == 900_000


async def test_acquire_of_a_held_scope_answers_false_and_changes_nothing(clock, server):
    raw = fake_client(server)
    holder = await _started(server, 'w-1', INSTANCE_A)
    other = await _started(server, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    body = await raw.get(LOCK_KEY)
    clock.advance(10)

    took = await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert took is False
    assert await raw.get(LOCK_KEY) == body
    assert await raw.pttl(LOCK_KEY) == LEASE_MILLISECONDS - 10_000


async def test_an_expired_lease_is_taken_without_a_takeover_line(clock, server):
    stalled = await _started(server, 'w-stalled', INSTANCE_A)
    taker = await _started(server, 'w-taker', INSTANCE_B)
    await stalled.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    clock.advance(LEASE_SECONDS + 1)

    with capture_logs() as logs:
        took = await taker.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert took is True
    assert await taker.is_held(SCOPE, TOKEN_B) is True
    assert await stalled.is_held(SCOPE, TOKEN_A) is False
    assert _takeover_logs(logs) == []


async def test_refresh_extends_the_lease_of_the_last_acquire_of_that_scope(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.acquire('scope-short', TOKEN_A, 100)
    await provider.acquire('scope-long', TOKEN_A, 200)
    clock.advance(50)

    refreshed_short = await provider.refresh('scope-short', TOKEN_A)
    refreshed_long = await provider.refresh('scope-long', TOKEN_A)

    assert (refreshed_short, refreshed_long) == (True, True)
    assert await raw.pttl('drakkar:archive-locks:scope-short') == 100_000
    assert await raw.pttl('drakkar:archive-locks:scope-long') == 200_000


async def test_refresh_uses_the_lease_of_the_last_successful_acquire(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await provider.acquire(SCOPE, TOKEN_A, 900)
    clock.advance(100)

    refreshed = await provider.refresh(SCOPE, TOKEN_A)

    assert refreshed is True
    assert await raw.pttl(LOCK_KEY) == 900_000


async def test_refresh_after_a_takeover_answers_false_and_keeps_the_new_holders_ttl(clock, server):
    raw = fake_client(server)
    stalled = await _started(server, 'w-stalled', INSTANCE_A)
    taker = await _started(server, 'w-taker', INSTANCE_B)
    await stalled.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    clock.advance(LEASE_SECONDS + 1)
    await taker.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)
    clock.advance(10)

    refreshed = await stalled.refresh(SCOPE, TOKEN_A)

    assert refreshed is False
    assert await raw.pttl(LOCK_KEY) == LEASE_MILLISECONDS - 10_000


async def test_a_refused_acquire_remembers_no_lease(clock, server):
    holder = await _started(server, 'w-1', INSTANCE_A)
    other = await _started(server, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    with pytest.raises(RuntimeError) as raised:
        await other.refresh(SCOPE, TOKEN_B)

    assert str(raised.value) == REFRESH_BEFORE_ACQUIRE


async def test_refresh_before_acquire_raises(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    recorder.calls.clear()

    with pytest.raises(RuntimeError) as raised:
        await provider.refresh(SCOPE, TOKEN_A)

    assert str(raised.value) == REFRESH_BEFORE_ACQUIRE
    assert recorder.calls == []


async def test_release_forgets_the_lease(clock, server):
    provider = await _started(server)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await provider.release(SCOPE, TOKEN_A)

    with pytest.raises(RuntimeError) as raised:
        await provider.refresh(SCOPE, TOKEN_A)

    assert str(raised.value) == REFRESH_BEFORE_ACQUIRE


async def test_release_deletes_the_key_of_the_holder_and_leaves_another_holders(clock, server):
    raw = fake_client(server)
    holder = await _started(server, 'w-1', INSTANCE_A)
    other = await _started(server, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    await other.release(SCOPE, TOKEN_B)
    kept = await raw.exists(LOCK_KEY)
    await holder.release(SCOPE, TOKEN_A)

    assert kept == 1
    assert await raw.exists(LOCK_KEY) == 0


async def test_is_held_is_false_for_a_body_that_is_not_a_lock(clock, server):
    raw = fake_client(server)
    provider = await _started(server)
    await raw.set(LOCK_KEY, 'garbage')

    held = await provider.is_held(SCOPE, TOKEN_A)
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await provider.release(SCOPE, TOKEN_A)

    assert held is False
    assert took is False
    assert await raw.get(LOCK_KEY) == b'garbage'


async def test_is_held_is_false_for_a_free_scope(clock, server):
    provider = await _started(server)

    assert await provider.is_held(SCOPE, TOKEN_A) is False


async def test_is_held_matches_a_token_whole_not_a_part_of_one(clock, server):
    provider = await _started(server)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert await provider.is_held(SCOPE, TOKEN_A[:-1]) is False


# --- The preset seam ---


@pytest.mark.parametrize('prefix', ['has space', 'a/b', 'star*', 'brace{', 'line\n'])
def test_a_preset_prefix_outside_the_allowed_characters_is_refused(prefix, server):
    with pytest.raises(ValueError, match='key_prefix'):
        _provider(server, key_prefix=prefix)


@pytest.mark.parametrize('prefix', ['', 'a', 'ops.fleet:', 'A_b-c:d.e'])
def test_a_preset_prefix_inside_the_allowed_characters_is_accepted(prefix, server):
    assert _provider(server, key_prefix=prefix) is not None


# --- Start ---


async def test_start_sends_ping_and_fails_when_the_server_does_not_answer(server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    recorder.fail_next = RedisConnectionError('connection refused')

    with pytest.raises(TransientError):
        await provider.start(_context())
    with pytest.raises(RuntimeError, match='before start'):
        await provider.is_held(SCOPE, TOKEN_A)
    await provider.start(_context())
    await provider.is_held(SCOPE, TOKEN_A)

    assert [argv[0] for argv in recorder.calls] == ['PING', 'PING', 'GET']


async def test_start_without_connections_raises():
    provider = RedisElection(connection='main')

    with pytest.raises(RuntimeError, match=r'^the redis election needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_takes_the_prefix_of_the_entry(clock, server):
    connections = make_connections(key_prefix='ops.fleet:')
    opener = AsyncMock(return_value=RecordingClient(fake_client(server)))
    provider = RedisElection(connection='main', open_client=opener)

    await provider.start(_context(connections))
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    opener.assert_awaited_once()
    assert await fake_client(server).exists(f'ops.fleet:archive-locks:{SCOPE}') == 1


async def test_registry_and_election_on_one_connection_share_one_client(clock, server):
    connections = make_connections()
    opener = AsyncMock(return_value=RecordingClient(fake_client(server)))
    registry = RedisRegistry(connection='main', prune_after_seconds=600, open_client=opener)
    election = RedisElection(connection='main', open_client=opener)
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
    assert opener.await_args.args[0].url == URL


async def test_a_call_before_start_raises(server):
    provider = _provider(server)

    with pytest.raises(RuntimeError, match=r'^RedisElection used before start\(\)$'):
        await provider.is_held(SCOPE, TOKEN_A)


# --- Stop ---


async def test_start_after_stop_raises_provider_stopped(server):
    provider = await _started(server)
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
async def test_calls_after_stop_raise_provider_stopped(call, clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await provider.stop()
    recorder.calls.clear()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    assert recorder.calls == []


async def test_stop_leaves_the_shared_client_to_the_connections(server):
    connections = make_connections()
    client = RecordingClient(fake_client(server))
    closer = AsyncMock()
    provider = RedisElection(connection='main', open_client=AsyncMock(return_value=client), close_client=closer)
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
        await provider.is_held(SCOPE, TOKEN_A)


async def test_a_failed_acquire_remembers_no_lease(clock, server):
    recorder = RecordingClient(fake_client(server))
    provider = _provider(server, client=recorder)
    await provider.start(_context())
    recorder.fail_next = RedisConnectionError('connection reset')

    with pytest.raises(TransientError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    with pytest.raises(RuntimeError) as raised:
        await provider.refresh(SCOPE, TOKEN_A)

    assert str(raised.value) == REFRESH_BEFORE_ACQUIRE


# --- Registration ---


def test_builtin_elections_include_redis():
    assert BUILTIN_ELECTIONS['redis'] is redis_election_factory


async def test_redis_election_factory_builds_a_provider_from_the_options(clock, server, monkeypatch):
    opened_urls: list[str] = []

    def open_recording_client(url: str, **kwargs: Any) -> RecordingClient:
        opened_urls.append(url)
        return RecordingClient(fake_client(server))

    monkeypatch.setattr(Redis, 'from_url', open_recording_client)
    provider = redis_election_factory({'connection': 'main'})

    await provider.start(_context(make_connections()))
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    assert opened_urls == [URL]
    assert await fake_client(server).pttl(LOCK_KEY) == LEASE_MILLISECONDS
