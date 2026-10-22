"""The ``consul`` election provider, against an in-process Consul agent.

``tests/consul_fake.py`` answers the agent requests through ``httpx.MockTransport``
with the answers a real agent gave to the same requests, so no socket is opened.
The provider runs the whole election test kit, then the tests of what it sends
(the request sequences of the shared fixture), the session it keeps per scope
and token (recorded before the acquire request, renewed by the holder, destroyed
when refused or released), the shared client and the errors.
"""

import asyncio
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.config.connections import ConnectionsConfig, ConsulConnectionConfig
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers_consul import ConsulRegistry
from drakkar.providers.connections import Connections
from drakkar.providers.consul import (
    DEFAULT_KEY_PREFIX,
    NO_LEADER_MESSAGE,
    PATHS,
    REFRESH_BEFORE_ACQUIRE_MESSAGE,
    path,
    read_key,
    session_name,
)
from drakkar.providers.resilience import TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from drakkar.recorder import electiontest
from drakkar.recorder.election import BUILTIN_ELECTIONS, ElectionContext
from drakkar.recorder.election_consul import REFRESH_BEFORE_ACQUIRE, ConsulElection, consul_election_factory
from drakkar.recorder.electiontest import INSTANCE_A, INSTANCE_B, TOKEN_A, TOKEN_B
from drakkar.schedule import SCOPE_INVALID
from tests.consul_fake import LEADER, FakeConsul, Seen, client_holding_the_first_renew, fake_client

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['consul']
EXAMPLE = FIXTURE['example']
INPUTS = EXAMPLE['inputs']
REQUESTS = EXAMPLE['requests']
FIRST, SECOND = INPUTS['sessions']['first'], INPUTS['sessions']['second']

# The kit's lease argument is the configured lease (lease_seconds): the session's TTL is half of it,
# and the fake ends a session twice its TTL after the last renew, so a lease ends exactly at this age.
LEASE_SECONDS = 600.0
SESSION_TTL_SECONDS = 300
SCOPE = 'search-fleet/history.archive'
LOCK_KEY = f'{DEFAULT_KEY_PREFIX}/task-locks/{SCOPE}'
LOCK_PATH = path('election_lock', prefix=DEFAULT_KEY_PREFIX, scope=SCOPE)
FOREIGN_SESSION = '00000000-0000-4000-8000-0000000000f0'
CANONICAL_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


@pytest.fixture
def fake() -> FakeConsul:
    return FakeConsul()


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


def _connections(**settings: Any) -> Connections:
    """The worker connections with ``connections.consul.main`` declared."""
    return Connections(ConnectionsConfig(consul={'main': ConsulConnectionConfig(**settings)}))


def _provider(fake: FakeConsul, **overrides: Any) -> ConsulElection:
    """A provider on a new client of ``fake``, which stands for the shared client."""
    return ConsulElection(connection='main', client=fake_client(fake), **overrides)


def make_provider_factory(fake: FakeConsul) -> Callable[[], ConsulElection]:
    """What the kit calls to get another provider on the same agent."""
    return lambda: _provider(fake)


async def _started(fake: FakeConsul, worker: str = 'w-1', instance: str = INSTANCE_A) -> ConsulElection:
    provider = _provider(fake)
    await provider.start(_context(worker=worker, instance=instance))
    return provider


def _requests(fake: FakeConsul, start: int = 0) -> list[list[Any]]:
    """The requests the fake received from ``start`` on, written as the fixture writes them."""
    return [seen.as_request() for seen in fake.seen[start:]]


def _routes(fake: FakeConsul, start: int = 0) -> list[tuple[str, str]]:
    """The method and path of each request the fake received from ``start`` on."""
    return [(seen.method, seen.path) for seen in fake.seen[start:]]


def _creates(fake: FakeConsul) -> int:
    """How many sessions the fake was asked to create."""
    return sum(1 for seen in fake.seen if seen.path == PATHS['session_create'])


def _destroys(fake: FakeConsul) -> list[str]:
    """The paths of the session destroys the fake received."""
    return [seen.path for seen in fake.seen if seen.path.startswith('/v1/session/destroy/')]


def _is_acquire(seen: Seen) -> bool:
    return seen.method == 'PUT' and 'acquire' in dict(seen.query)


def _lose_the_answer_of_the_first_acquire(fake: FakeConsul) -> None:
    """Make the first acquire PUT take the key and then lose its answer on the way back."""
    lost: list[Seen] = []

    def lose_it(seen: Seen) -> None:
        if lost or not _is_acquire(seen):
            return
        lost.append(seen)
        fake.fail_after_answer = httpx.ReadError('lost')

    fake.before_answer = lose_it


def _takeover_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'periodic_lock_taken_over']


# --- The kit ---


@pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS, ids=lambda check: check.__name__)
async def test_kit_checks_pass_for_consul(check, fake):
    await check(make_provider_factory(fake), fake.advance, LEASE_SECONDS)


@pytest.mark.parametrize('scope', FIXTURE['refused_scopes'])
@pytest.mark.parametrize('call', ['acquire', 'refresh', 'is_held', 'release'])
async def test_consul_election_checks_the_scope_before_any_request(fake, scope, call):
    provider = await _started(fake)
    started = len(fake.seen)
    args = (scope, TOKEN_A, LEASE_SECONDS) if call == 'acquire' else (scope, TOKEN_A)

    with pytest.raises(ValueError) as raised:
        await getattr(provider, call)(*args)

    assert str(raised.value) == SCOPE_INVALID.format(scope=scope)
    assert _requests(fake, started) == []


# --- What the provider sends ---


async def test_requests_match_the_fixture_example(fake, monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: _epoch_of(INPUTS['acquired_at']))
    provider = ConsulElection(
        connection='main',
        client=fake_client(fake, datacenter=INPUTS['datacenter']),
        key_prefix=INPUTS['prefix'],
    )
    card = INPUTS['card']
    scope, token = INPUTS['scope'], INPUTS['token']
    sent: dict[str, list[list[Any]]] = {}

    async def step(name: str, call: Any) -> None:
        mark = len(fake.seen)
        await call
        sent[name] = _requests(fake, mark)

    await step('start', provider.start(_context(worker=card['worker'], instance=card['instance'])))
    await step('acquire', provider.acquire(scope, token, INPUTS['lease_seconds']))
    await step('acquire_by_the_holder', provider.acquire(scope, token, INPUTS['lease_seconds']))
    await step('refresh', provider.refresh(scope, token))
    await step('is_held', provider.is_held(scope, token))
    await step('release', provider.release(scope, token))

    names = ('start', 'acquire', 'acquire_by_the_holder', 'refresh', 'is_held', 'release')
    assert sent == {name: REQUESTS[name] for name in names}


async def test_the_session_name_and_ttl_follow_the_scope_the_token_and_the_lease(fake):
    provider = await _started(fake)

    await provider.acquire(SCOPE, TOKEN_A, 15)

    create = next(seen for seen in fake.seen if seen.path == PATHS['session_create'])
    assert create.body is not None
    assert json.loads(create.body) == {
        'Behavior': 'delete',
        'LockDelay': '0s',
        'Name': f'drakkar/task-lock/{SCOPE}/{TOKEN_A}',
        'TTL': '10s',
    }


async def test_a_preset_prefix_goes_into_the_lock_path(fake):
    provider = _provider(fake, key_prefix='ops/fleet')
    await provider.start(_context())

    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert ('PUT', path('election_lock', prefix='ops/fleet', scope=SCOPE)) in _routes(fake)


# --- Acquire ---


async def test_acquire_by_the_holder_keeps_the_body_and_renews_the_session(fake, monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: _epoch_of('2026-10-14T02:00:00.125000Z'))
    provider = await _started(fake, 'search-worker-3', INSTANCE_A)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    stored = await read_key(fake_client(fake), LOCK_PATH)
    monkeypatch.setattr(time, 'time', lambda: _epoch_of('2026-10-14T02:01:40.125000Z'))
    fake.advance(100)
    mark = len(fake.seen)

    # Another lease length keeps the session's TTL: it is set when the session is created.
    took_again = await provider.acquire(SCOPE, TOKEN_A, 7200)
    sent = _routes(fake, mark)
    stored_again = await read_key(fake_client(fake), LOCK_PATH)
    # The renew moved the session's end from 600 to 700; at 650 the lease is still held.
    fake.advance(2 * SESSION_TTL_SECONDS - 50)
    held = await provider.is_held(SCOPE, TOKEN_A)

    assert took_again is True
    assert sent == [('PUT', f'/v1/session/renew/{FIRST}'), ('PUT', LOCK_PATH)]
    assert stored is not None
    assert stored_again is not None
    assert json.loads(stored['Value'])['acquired_at'] == '2026-10-14T02:00:00.125000Z'
    assert stored_again['Value'] == stored['Value']
    assert held is True
    assert _creates(fake) == 1


async def test_acquire_by_the_holder_after_its_session_ended_takes_a_new_session(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.end_session(FIRST)
    mark = len(fake.seen)

    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    sent = [(seen.method, seen.path, dict(seen.query).get('acquire')) for seen in fake.seen[mark:]]
    stored = await read_key(fake_client(fake), LOCK_PATH)

    assert took is True
    assert sent == [
        ('PUT', f'/v1/session/renew/{FIRST}', None),
        ('PUT', PATHS['session_create'], None),
        ('PUT', LOCK_PATH, SECOND),
    ]
    assert stored is not None
    assert stored['Session'] == SECOND


async def test_a_refused_acquire_destroys_its_new_session(fake):
    holder = await _started(fake, 'w-1', INSTANCE_A)
    other = await _started(fake, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    took = await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)
    mark = len(fake.seen)
    other_holds = await other.is_held(SCOPE, TOKEN_B)
    sent_by_the_check = _routes(fake, mark)

    assert took is False
    assert _destroys(fake) == [f'/v1/session/destroy/{SECOND}']
    assert await session_name(fake_client(fake), SECOND) is None
    # The entry went with the session: no check is sent and no refresh is possible.
    assert other_holds is False
    assert sent_by_the_check == []
    with pytest.raises(RuntimeError, match=f'^{REFRESH_BEFORE_ACQUIRE}$'):
        await other.refresh(SCOPE, TOKEN_B)
    assert await holder.is_held(SCOPE, TOKEN_A) is True


async def test_an_acquire_by_the_holder_that_lost_its_key_destroys_the_session_and_answers_false(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    # The key was deleted and another session took it while this provider's session lived on.
    fake.plant_session(FOREIGN_SESSION, 'someone-else', SESSION_TTL_SECONDS)
    fake.put_raw(LOCK_KEY, b'other', session=FOREIGN_SESSION)

    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    stored = await read_key(fake_client(fake), LOCK_PATH)

    assert took is False
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}']
    assert stored is not None
    assert (stored['Value'], stored['Session']) == (b'other', FOREIGN_SESSION)
    with pytest.raises(RuntimeError, match=f'^{REFRESH_BEFORE_ACQUIRE}$'):
        await provider.refresh(SCOPE, TOKEN_A)


async def test_an_acquire_retried_after_a_lost_answer_finds_its_session(fake):
    provider = await _started(fake)
    _lose_the_answer_of_the_first_acquire(fake)

    with pytest.raises(TransientError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    stored = await read_key(fake_client(fake), LOCK_PATH)

    assert took is True
    assert _creates(fake) == 1
    assert stored is not None
    assert stored['Session'] == FIRST


async def test_release_after_an_unclear_acquire_destroys_the_recorded_session(fake):
    provider = await _started(fake)
    _lose_the_answer_of_the_first_acquire(fake)

    with pytest.raises(TransientError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    taken = await read_key(fake_client(fake), LOCK_PATH)
    await provider.release(SCOPE, TOKEN_A)

    assert taken is not None
    assert taken['Session'] == FIRST
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}']
    assert await read_key(fake_client(fake), LOCK_PATH) is None


async def test_an_expired_lease_is_taken_without_a_takeover_line(fake):
    stalled = await _started(fake, 'w-stalled', INSTANCE_A)
    taker = await _started(fake, 'w-taker', INSTANCE_B)
    await stalled.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.advance(LEASE_SECONDS)

    with capture_logs() as logs:
        took = await taker.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    assert took is True
    assert await taker.is_held(SCOPE, TOKEN_B) is True
    assert await stalled.is_held(SCOPE, TOKEN_A) is False
    assert _takeover_logs(logs) == []


# --- Refresh ---


async def test_refresh_before_acquire_raises(fake):
    provider = await _started(fake)
    mark = len(fake.seen)

    with pytest.raises(RuntimeError) as raised:
        await provider.refresh(SCOPE, TOKEN_A)

    assert str(raised.value) == REFRESH_BEFORE_ACQUIRE == REFRESH_BEFORE_ACQUIRE_MESSAGE
    assert fake.seen[mark:] == []


async def test_refresh_of_an_ended_session_is_false_and_forgets_it(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.advance(LEASE_SECONDS)

    refreshed = await provider.refresh(SCOPE, TOKEN_A)

    assert refreshed is False
    with pytest.raises(RuntimeError, match=f'^{REFRESH_BEFORE_ACQUIRE}$'):
        await provider.refresh(SCOPE, TOKEN_A)


async def test_a_release_during_a_refresh_is_not_undone(fake):
    reached, release = asyncio.Event(), asyncio.Event()
    provider = ConsulElection(connection='main', client=client_holding_the_first_renew(fake, reached, release))
    await provider.start(_context())
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    refreshing = asyncio.create_task(provider.refresh(SCOPE, TOKEN_A))
    await reached.wait()
    await provider.release(SCOPE, TOKEN_A)
    release.set()
    refreshed = await refreshing

    # The release destroyed the session while the renew waited, so the renew found it gone.
    assert refreshed is False
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}']
    assert await read_key(fake_client(fake), LOCK_PATH) is None
    assert await provider.is_held(SCOPE, TOKEN_A) is False


async def test_a_refresh_that_ends_after_a_new_acquire_keeps_the_new_entry(fake):
    reached, release = asyncio.Event(), asyncio.Event()
    provider = ConsulElection(connection='main', client=client_holding_the_first_renew(fake, reached, release))
    await provider.start(_context())
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.end_session(FIRST)

    refreshing = asyncio.create_task(provider.refresh(SCOPE, TOKEN_A))
    await reached.wait()
    # While the refresh's renew is pending, an acquire of the same scope and token finds the session
    # ended and records a new entry with a new session.
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    release.set()
    refreshed = await refreshing

    # The refresh's renew of the old session answered 404; it forgot nothing, since the entry is a new one.
    assert took is True
    assert refreshed is False
    assert await provider.is_held(SCOPE, TOKEN_A) is True
    assert await provider.refresh(SCOPE, TOKEN_A) is True
    stored = await read_key(fake_client(fake), LOCK_PATH)
    assert stored is not None
    assert stored['Session'] == SECOND


# --- Is held ---


async def test_is_held_without_an_acquire_sends_nothing(fake):
    provider = await _started(fake)
    mark = len(fake.seen)

    held = await provider.is_held(SCOPE, TOKEN_A)

    assert held is False
    assert fake.seen[mark:] == []


async def test_is_held_is_false_once_the_lock_key_is_gone(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.end_session(FIRST)
    mark = len(fake.seen)

    held = await provider.is_held(SCOPE, TOKEN_A)

    assert held is False
    assert _routes(fake, mark) == [('GET', LOCK_PATH)]


# --- Release ---


async def test_release_without_an_acquire_sends_nothing(fake):
    provider = await _started(fake)
    mark = len(fake.seen)

    await provider.release(SCOPE, TOKEN_A)

    assert fake.seen[mark:] == []


async def test_a_release_whose_destroy_failed_sends_the_destroy_again(fake):
    provider = await _started(fake)
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.fail_next = 503

    with pytest.raises(TransientError):
        await provider.release(SCOPE, TOKEN_A)
    # The resilience caller retries a transient failure: the retry must destroy the session, not find nothing.
    await provider.release(SCOPE, TOKEN_A)
    mark = len(fake.seen)
    await provider.release(SCOPE, TOKEN_A)
    sent_by_the_third_release = fake.seen[mark:]

    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}', f'/v1/session/destroy/{FIRST}']
    assert await read_key(fake_client(fake), LOCK_PATH) is None
    assert await session_name(fake_client(fake), FIRST) is None
    # A release that succeeded forgot the entry, so the next one sends nothing.
    assert sent_by_the_third_release == []


async def test_a_refused_acquire_whose_destroy_failed_leaves_the_session_to_the_release(fake):
    holder = await _started(fake, 'w-1', INSTANCE_A)
    other = await _started(fake, 'w-2', INSTANCE_B)
    await holder.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    def fail_the_first_destroy(seen: Seen) -> None:
        if seen.path.startswith('/v1/session/destroy/') and len(_destroys(fake)) == 1:
            fake.fail_next = 503

    fake.before_answer = fail_the_first_destroy

    with pytest.raises(TransientError):
        await other.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)
    # The entry was kept, so the release that follows an unclear acquire destroys the refused session.
    await other.release(SCOPE, TOKEN_B)

    assert _destroys(fake) == [f'/v1/session/destroy/{SECOND}', f'/v1/session/destroy/{SECOND}']
    assert await session_name(fake_client(fake), SECOND) is None
    assert await holder.is_held(SCOPE, TOKEN_A) is True


async def test_release_by_a_token_that_was_taken_over_leaves_the_new_holder_alone(fake):
    stalled = await _started(fake, 'w-stalled', INSTANCE_A)
    taker = await _started(fake, 'w-taker', INSTANCE_B)
    await stalled.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.advance(LEASE_SECONDS)
    await taker.acquire(SCOPE, TOKEN_B, LEASE_SECONDS)

    await stalled.release(SCOPE, TOKEN_A)
    stored = await read_key(fake_client(fake), LOCK_PATH)

    # The stalled session had already ended; destroying it again deletes nothing it no longer holds.
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}']
    assert stored is not None
    assert stored['Session'] == SECOND


# --- The preset seam ---


@pytest.mark.parametrize(
    'prefix', ['', '/drakkar', 'drakkar/', 'a//b', 'has space', 'a:b', 'brace{', 'line\n', '.', '..', 'a/../b', 'a/./b']
)
def test_a_preset_prefix_outside_the_allowed_shape_is_refused(prefix, fake):
    with pytest.raises(ValueError, match='key_prefix'):
        _provider(fake, key_prefix=prefix)


@pytest.mark.parametrize('prefix', ['drakkar', 'ops/fleet', 'A_b-c.d/e', '..x', 'v1.2'])
def test_a_preset_prefix_inside_the_allowed_shape_is_accepted(prefix, fake):
    assert _provider(fake, key_prefix=prefix) is not None


# --- Start ---


async def test_start_checks_the_leader_and_fails_without_one(fake):
    provider = _provider(fake)
    fake.leader = ''

    with pytest.raises(TransientError, match=f'^{NO_LEADER_MESSAGE}$'):
        await provider.start(_context())
    with pytest.raises(RuntimeError, match=r'^ConsulElection used before start\(\)$'):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    fake.leader = LEADER
    await provider.start(_context())
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    assert took is True
    assert _routes(fake) == [
        ('GET', PATHS['leader']),
        ('GET', PATHS['leader']),
        ('PUT', PATHS['session_create']),
        ('PUT', LOCK_PATH),
    ]


async def test_start_without_connections_raises():
    provider = ConsulElection(connection='main')

    with pytest.raises(RuntimeError, match=r'^the consul election needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_takes_the_prefix_of_the_entry(fake):
    opener = AsyncMock(return_value=fake_client(fake))
    provider = ConsulElection(connection='main', open_client=opener)

    await provider.start(_context(_connections(key_prefix='ops/fleet')))
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    opener.assert_awaited_once()
    assert ('PUT', path('election_lock', prefix='ops/fleet', scope=SCOPE)) in _routes(fake)


async def test_registry_and_election_on_one_connection_share_one_client(fake):
    connections = _connections()
    opener = AsyncMock(return_value=fake_client(fake))
    registry = ConsulRegistry(connection='main', offline_after_seconds=60, open_client=opener)
    election = ConsulElection(connection='main', open_client=opener)
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


async def test_calls_before_start_raise(fake):
    provider = _provider(fake)

    for call in (
        provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS),
        provider.refresh(SCOPE, TOKEN_A),
        provider.is_held(SCOPE, TOKEN_A),
        provider.release(SCOPE, TOKEN_A),
    ):
        with pytest.raises(RuntimeError, match=r'^ConsulElection used before start\(\)$'):
            await call
    assert fake.seen == []


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
    await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    mark = len(fake.seen)

    await provider.stop()
    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    # Neither the stop nor the refused call sends a request: the lease is left to the release or its TTL.
    assert fake.seen[mark:] == []


async def test_stop_leaves_the_shared_client_to_the_connections(fake):
    connections = _connections()
    client = fake_client(fake)
    closer = AsyncMock()
    provider = ConsulElection(connection='main', open_client=AsyncMock(return_value=client), close_client=closer)
    await provider.start(_context(connections))

    await provider.stop()
    closed_by_stop = closer.await_count
    await connections.close_all()

    assert closed_by_stop == 0
    closer.assert_awaited_once_with(client)


# --- Errors ---


async def test_a_connection_error_is_raised_transient(fake):
    provider = await _started(fake)
    fake.fail_next = httpx.ConnectError('connection refused')

    with pytest.raises(TransientError):
        await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)

    # The create failed, so nothing was recorded.
    with pytest.raises(RuntimeError, match=f'^{REFRESH_BEFORE_ACQUIRE}$'):
        await provider.refresh(SCOPE, TOKEN_A)


# --- Registration ---


def test_builtin_elections_include_consul():
    assert BUILTIN_ELECTIONS['consul'] is consul_election_factory


async def test_consul_election_factory_builds_a_provider_from_the_options(fake, monkeypatch):
    # The real client of the connection entry is opened; its transport answers from the fake, so nothing connects.
    async def answer_from_the_fake(transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        return await fake.transport().handle_async_request(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, 'handle_async_request', answer_from_the_fake)
    connections = Connections(ConnectionsConfig(consul={'locks': ConsulConnectionConfig(key_prefix='ops')}))
    provider = consul_election_factory({'connection': 'locks'})

    await provider.start(_context(connections))
    took = await provider.acquire(SCOPE, TOKEN_A, LEASE_SECONDS)
    await connections.close_all()

    assert took is True
    assert _routes(fake)[-1] == ('PUT', path('election_lock', prefix='ops', scope=SCOPE))
