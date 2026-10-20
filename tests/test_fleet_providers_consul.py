"""The ``consul`` registry provider, against an in-process Consul agent.

``tests/consul_fake.py`` answers the agent requests through ``httpx.MockTransport``
with the answers a real agent gave to the same requests, so no socket is opened.
The provider runs the registry test kit (all but the age check: a Consul card
has no age), then the tests of what it sends (the request sequences of the
shared fixture), the takeover of a predecessor's card, the names, the listing,
the errors, and the breaker of the registry service on an agent that dies and
comes back.
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
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config.connections import ConnectionsConfig, ConsulConnectionConfig
from drakkar.config.fleet import FleetRegistryConfig
from drakkar.config.resilience import BreakerConfig, ResilienceConfig
from drakkar.fleet import testkit
from drakkar.fleet.card import WorkerIdentity, sanitize_worker_name, serialize_card
from drakkar.fleet.provider import RegistryContext
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.fleet.providers_consul import ConsulRegistry, consul_registry_factory
from drakkar.fleet.service import FleetRegistry
from drakkar.providers.connections import Connections
from drakkar.providers.consul import (
    CARD_HELD_MESSAGE,
    DEFAULT_KEY_PREFIX,
    INSTANCE_RE,
    NO_LEADER_MESSAGE,
    PATHS,
    ConsulClient,
    ConsulError,
    is_own_session_name,
    path,
    read_key,
    registry_session_name,
)
from drakkar.providers.resilience import ResilientCaller, TransientError
from drakkar.providers.thread import PROVIDER_STOPPED
from tests.consul_fake import FAKE_BASE, LEADER, FakeConsul, Seen, client_holding_the_first_renew, fake_client
from tests.test_recorder_wiring import _app as recorder_wiring_app

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['consul']
EXAMPLE = FIXTURE['example']
INPUTS = EXAMPLE['inputs']
REQUESTS = EXAMPLE['requests']
FIRST, SECOND, PREDECESSOR = (INPUTS['sessions'][name] for name in ('first', 'second', 'predecessor'))
EXAMPLE_CARD: dict[str, Any] = INPUTS['card']
EXAMPLE_MEMBER = sanitize_worker_name(EXAMPLE_CARD['worker'])
# The instance that ends the fixture's predecessor_name.
PREDECESSOR_INSTANCE = INPUTS['predecessor_name'][-16:]

OFFLINE_AFTER = 60
# The session TTL that OFFLINE_AFTER gives; the fake ends a session twice this long after its last renew.
SESSION_TTL_SECONDS = 30
INSTANCE = 'a' * 16
OTHER_INSTANCE = 'b' * 16
FOREIGN_SESSION = '00000000-0000-4000-8000-0000000000f0'

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
def fake() -> FakeConsul:
    return FakeConsul()


def card_key(member: str, prefix: str = DEFAULT_KEY_PREFIX) -> str:
    """The KV key of the card of ``member``, as ``FakeConsul.put_raw`` takes it."""
    return f'{prefix}/fleet/workers/{member}'


def card_path(member: str, prefix: str = DEFAULT_KEY_PREFIX) -> str:
    """The request path of the card of ``member``."""
    return path('registry_card', prefix=prefix, worker=member)


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


def _provider(fake: FakeConsul, offline_after: float = OFFLINE_AFTER, **overrides: Any) -> ConsulRegistry:
    """A provider on a new client of ``fake``, which stands for the shared client."""
    return ConsulRegistry(
        connection='main',
        offline_after_seconds=offline_after,
        client=fake_client(fake),
        **overrides,
    )


def make_provider_factory(fake: FakeConsul) -> Callable[[], ConsulRegistry]:
    """What the kit calls to get another provider on the same agent."""
    return lambda: _provider(fake)


async def _started(fake: FakeConsul, **overrides: Any) -> ConsulRegistry:
    provider = _provider(fake, **overrides)
    await provider.start(_context())
    return provider


def _example_provider(fake: FakeConsul) -> ConsulRegistry:
    """A provider with the inputs of the fixture's example: its datacenter, prefix and offline limit."""
    return ConsulRegistry(
        connection='main',
        offline_after_seconds=INPUTS['offline_after_seconds'],
        client=fake_client(fake, datacenter=INPUTS['datacenter']),
        key_prefix=INPUTS['prefix'],
    )


def _requests(fake: FakeConsul, start: int = 0) -> list[list[Any]]:
    """The requests the fake received from ``start`` on, written as the fixture writes them."""
    return [seen.as_request() for seen in fake.seen[start:]]


def _routes(fake: FakeConsul, start: int = 0) -> list[tuple[str, str]]:
    """The method and path of each request the fake received from ``start`` on."""
    return [(seen.method, seen.path) for seen in fake.seen[start:]]


def _destroys(fake: FakeConsul) -> list[str]:
    """The paths of the session destroys the fake received."""
    return [seen.path for seen in fake.seen if seen.path.startswith('/v1/session/destroy/')]


def _is_acquire(seen: Seen) -> bool:
    return seen.method == 'PUT' and 'acquire' in dict(seen.query)


def _invalid_card_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry['event'] == 'fleet_card_invalid']


def _calls(op: str, outcome: str) -> float:
    """``drakkar_provider_calls_total`` of the consul registry for one operation and outcome."""
    labels = {'feature': 'registry', 'provider': 'consul', 'op': op, 'outcome': outcome}
    return REGISTRY.get_sample_value('drakkar_provider_calls_total', labels) or 0.0


def _service(fake: FakeConsul, resilience: ResilienceConfig, now: list[float]) -> FleetRegistry:
    """The registry service on a consul provider, on a monotonic clock that only the test moves."""
    return FleetRegistry(
        _provider(fake),
        provider_name='consul',
        config=FleetRegistryConfig(provider='consul', resilience=resilience),
        identity=IDENTITY,
        now_monotonic=lambda: now[0],
        now_epoch=lambda: 1_760_000_000.0,
        caller=ResilientCaller(resilience, feature='registry', provider='consul', now_monotonic=lambda: now[0]),
    )


# --- The kit ---

# check_age_grows_with_time needs an age; a consul card has none, and online replaces it.
AGE_FREE_CHECKS = [check for check in testkit.PUBLISHING_CHECKS if check is not testkit.check_age_grows_with_time]


@pytest.mark.parametrize('check', AGE_FREE_CHECKS, ids=lambda check: check.__name__)
async def test_kit_publishing_checks_pass_for_consul(check, fake):
    # A card lives until its session ends, at most offline_after_seconds after the last publish,
    # so the offline limit is also the prune age.
    await check(make_provider_factory(fake), fake.advance, OFFLINE_AFTER)


@pytest.mark.parametrize('check', testkit.LIVENESS_CHECKS, ids=lambda check: check.__name__)
async def test_kit_liveness_checks_pass_for_consul(check, fake):
    await check(make_provider_factory(fake), fake.advance, OFFLINE_AFTER)


async def test_kit_shape_and_age_checks_pass_for_consul(fake):
    seed = await _started(fake)
    await seed.publish(_card('w-2'))

    await testkit.check_list_shape(_provider(fake), testkit.make_context('w-1'))
    await testkit.check_age_is_none_or_non_negative(_provider(fake), testkit.make_context('w-1'))


# --- What the provider sends ---


async def test_requests_match_the_fixture_example(fake):
    provider = _example_provider(fake)
    sent: dict[str, list[list[Any]]] = {}

    async def step(name: str, call: Any) -> None:
        mark = len(fake.seen)
        await call
        sent[name] = _requests(fake, mark)

    await step('start', provider.start(_context()))
    await step('first_publish', provider.publish(EXAMPLE_CARD))
    await step('next_publish', provider.publish(EXAMPLE_CARD))
    await step('list', provider.list())
    await step('withdraw', provider.withdraw(EXAMPLE_CARD['worker'], EXAMPLE_CARD['instance']))

    names = ('start', 'first_publish', 'next_publish', 'list', 'withdraw')
    assert sent == {name: REQUESTS[name] for name in names}


async def test_publish_recreates_an_ended_session(fake):
    provider = _example_provider(fake)
    await provider.start(_context())
    await provider.publish(EXAMPLE_CARD)
    fake.advance(2 * SESSION_TTL_SECONDS)
    mark = len(fake.seen)

    await provider.publish(EXAMPLE_CARD)
    sent = _requests(fake, mark)
    listed = await provider.list()

    assert sent == REQUESTS['publish_after_the_session_ended']
    assert [(entry.card, entry.online) for entry in listed] == [(EXAMPLE_CARD, True)]


async def test_a_next_instance_takes_over_its_predecessors_card_at_once(fake):
    old_card = {**EXAMPLE_CARD, 'instance': PREDECESSOR_INSTANCE, 'state': 'draining'}
    fake.plant_session(PREDECESSOR, INPUTS['predecessor_name'], SESSION_TTL_SECONDS)
    fake.put_raw(card_key(EXAMPLE_MEMBER, INPUTS['prefix']), serialize_card(old_card), session=PREDECESSOR)
    provider = _example_provider(fake)
    await provider.start(_context())
    mark = len(fake.seen)

    await provider.publish(EXAMPLE_CARD)
    sent = _requests(fake, mark)
    listed = await provider.list()

    assert sent == REQUESTS['publish_over_the_predecessor']
    assert [(entry.card, entry.online) for entry in listed] == [(EXAMPLE_CARD, True)]


# --- The takeover of a card key ---


async def test_a_worker_never_destroys_the_session_of_a_longer_name(fake):
    # Both cards here have their own key; the real pin of the own-session rule is the longer_worker_name case below.
    longer = await _started(fake)
    shorter = await _started(fake)

    await longer.publish(_card('w-1', INSTANCE))
    await shorter.publish(_card('w', OTHER_INSTANCE))
    listed = await shorter.list()

    assert _destroys(fake) == []
    assert sorted((entry.card['worker'], entry.online) for entry in listed) == [('w', True), ('w-1', True)]


@pytest.mark.parametrize(
    'holder_name',
    [
        'someone-else',
        'drakkar-w-1-0123456789abcdef',
        'drakkar-w-0123456789ABCDEF',
        'drakkar/archive-lock/main/5f0c1d2e3a4b6978',
    ],
    ids=['foreign', 'longer_worker_name', 'upper_case_instance', 'election'],
)
async def test_a_foreign_session_on_the_card_key_fails_the_publish(holder_name, fake):
    fake.plant_session(FOREIGN_SESSION, holder_name, SESSION_TTL_SECONDS)
    fake.put_raw(card_key('w'), b'x', session=FOREIGN_SESSION)
    provider = await _started(fake)

    with pytest.raises(ConsulError) as info:
        await provider.publish(_card('w'))
    entry = await read_key(fake_client(fake), card_path('w'))

    assert str(info.value) == CARD_HELD_MESSAGE.format(worker='w')
    assert not isinstance(info.value, TransientError)
    assert _destroys(fake) == []
    assert entry is not None
    assert (entry['Value'], entry['Session']) == (b'x', FOREIGN_SESSION)


async def test_a_card_key_taken_again_after_the_takeover_fails_the_publish(fake):
    fake.plant_session(PREDECESSOR, f'drakkar-w-1-{PREDECESSOR_INSTANCE}', SESSION_TTL_SECONDS)
    fake.put_raw(card_key('w-1'), b'old', session=PREDECESSOR)
    provider = await _started(fake)
    acquires: list[Seen] = []

    def take_the_key_before_the_second_acquire(seen: Seen) -> None:
        if not _is_acquire(seen):
            return
        acquires.append(seen)
        if len(acquires) == 2:
            fake.plant_session(FOREIGN_SESSION, 'drakkar-w-1-cccccccccccccccc', SESSION_TTL_SECONDS)
            fake.put_raw(card_key('w-1'), b'newer', session=FOREIGN_SESSION)

    fake.before_answer = take_the_key_before_the_second_acquire

    with pytest.raises(ConsulError) as info:
        await provider.publish(_card('w-1'))

    assert str(info.value) == CARD_HELD_MESSAGE.format(worker='w-1')
    assert len(acquires) == 2
    assert _destroys(fake) == [f'/v1/session/destroy/{PREDECESSOR}']


def _end_the_predecessor(fake: FakeConsul) -> None:
    fake.end_session(PREDECESSOR)


def _free_the_key_of_its_session(fake: FakeConsul) -> None:
    fake.put_raw(card_key('w-1'), b'unheld')


@pytest.mark.parametrize(
    ('trigger_path', 'action', 'routes_before_the_second_acquire'),
    [
        (card_path('w-1'), _end_the_predecessor, [('GET', card_path('w-1'))]),
        (card_path('w-1'), _free_the_key_of_its_session, [('GET', card_path('w-1'))]),
        (
            f'/v1/session/info/{PREDECESSOR}',
            _end_the_predecessor,
            [('GET', card_path('w-1')), ('GET', f'/v1/session/info/{PREDECESSOR}')],
        ),
    ],
    ids=['key_gone', 'key_without_a_session', 'holder_session_ended'],
)
async def test_a_holder_that_left_before_the_takeover_lets_the_next_acquire_through(
    trigger_path, action, routes_before_the_second_acquire, fake
):
    fake.plant_session(PREDECESSOR, f'drakkar-w-1-{PREDECESSOR_INSTANCE}', SESSION_TTL_SECONDS)
    fake.put_raw(card_key('w-1'), b'old', session=PREDECESSOR)
    provider = await _started(fake)
    card = _card('w-1')

    def leave_at_the_trigger(seen: Seen) -> None:
        if seen.method == 'GET' and seen.path == trigger_path:
            action(fake)

    fake.before_answer = leave_at_the_trigger
    mark = len(fake.seen)

    await provider.publish(card)
    sent = _routes(fake, mark)
    listed = await provider.list()

    assert sent == [
        ('PUT', PATHS['session_create']),
        ('PUT', card_path('w-1')),
        *routes_before_the_second_acquire,
        ('PUT', card_path('w-1')),
    ]
    assert _destroys(fake) == []
    assert [(entry.card, entry.online) for entry in listed] == [(card, True)]


@pytest.mark.parametrize('shown', [None, ''], ids=['null', 'empty'])
async def test_a_card_key_with_a_null_or_empty_session_is_taken_as_unheld(shown, fake):
    fake.plant_session(PREDECESSOR, f'drakkar-w-1-{PREDECESSOR_INSTANCE}', SESSION_TTL_SECONDS)
    fake.put_raw(card_key('w-1'), b'old', session=PREDECESSOR)
    provider = ConsulRegistry(
        connection='main', offline_after_seconds=OFFLINE_AFTER, client=_client_showing_session(fake, shown)
    )
    await provider.start(_context())
    card = _card('w-1')

    def free_the_key_at_the_read(seen: Seen) -> None:
        if seen.method == 'GET' and seen.path == card_path('w-1'):
            fake.put_raw(card_key('w-1'), b'unheld')

    fake.before_answer = free_the_key_at_the_read
    mark = len(fake.seen)

    await provider.publish(card)
    sent = _routes(fake, mark)

    assert sent == [
        ('PUT', PATHS['session_create']),
        ('PUT', card_path('w-1')),
        ('GET', card_path('w-1')),
        ('PUT', card_path('w-1')),
    ]
    assert _destroys(fake) == []


async def test_two_names_with_one_stem_share_one_card(fake):
    first = await _started(fake)
    second = await _started(fake)

    await first.publish(_card('a.b', INSTANCE, state='first'))
    await second.publish(_card('a_b', OTHER_INSTANCE, state='second'))
    listed = await first.list()

    assert [(entry.card['worker'], entry.card['state']) for entry in listed] == [('a_b', 'second')]


async def test_a_non_ascii_worker_name_round_trips_byte_for_byte(fake):
    provider = await _started(fake)
    card = _card('wörker 1')

    await provider.publish(card)
    acquire = next(seen for seen in fake.seen if _is_acquire(seen))
    stored = await read_key(fake_client(fake), card_path('w_rker_1'))
    listed = await provider.list()

    assert sanitize_worker_name('wörker 1') == 'w_rker_1'
    assert acquire.path == card_path('w_rker_1')
    assert acquire.body == serialize_card(card).decode('utf-8')
    assert stored is not None
    assert stored['Value'] == serialize_card(card)
    assert [entry.card for entry in listed] == [card]


async def test_the_session_name_and_ttl_follow_the_member_the_instance_and_the_offline_limit(fake):
    provider = await _started(fake, offline_after=7200)

    await provider.publish(_card('wörker 1', '0123456789abcdef'))

    create = next(seen for seen in fake.seen if seen.path == PATHS['session_create'])
    assert create.body is not None
    assert json.loads(create.body) == {
        'Behavior': 'delete',
        'LockDelay': '0s',
        'Name': 'drakkar-w_rker_1-0123456789abcdef',
        'TTL': '3600s',
    }


def test_the_worker_instance_is_a_tail_that_the_own_session_name_rule_recognises(tmp_path):
    # publish names the session with the card's instance unchecked; a successor takes the card over only
    # when is_own_session_name recognises that tail, which reads 16 lowercase hex characters.
    instance = recorder_wiring_app(tmp_path)._instance

    assert INSTANCE_RE.fullmatch(instance) is not None
    assert is_own_session_name(registry_session_name('w-1', instance), 'w-1')
    assert not is_own_session_name(registry_session_name('w-1', 'ABCDEF0123456789'), 'w-1')


# --- The listing ---


async def test_list_reads_an_empty_fleet_from_a_404(fake):
    provider = await _started(fake)

    assert await provider.list() == []
    assert _routes(fake)[-1] == ('GET', path('registry_list', prefix=DEFAULT_KEY_PREFIX))


async def test_a_key_without_a_session_is_listed_offline(fake):
    card = _card('w-2')
    fake.put_raw(card_key('w-2'), serialize_card(card))
    provider = await _started(fake)

    listed = await provider.list()

    assert [(entry.card, entry.age_seconds, entry.online) for entry in listed] == [(card, None, False)]


@pytest.mark.parametrize('shown', [None, ''], ids=['null', 'empty'])
async def test_a_key_with_a_null_or_empty_session_is_listed_offline(shown, fake):
    card = _card('w-2')
    fake.put_raw(card_key('w-2'), serialize_card(card))
    provider = ConsulRegistry(
        connection='main', offline_after_seconds=OFFLINE_AFTER, client=_client_showing_session(fake, shown)
    )
    await provider.start(_context())

    listed = await provider.list()

    assert [(entry.card, entry.online) for entry in listed] == [(card, False)]


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        (f'{DEFAULT_KEY_PREFIX}/fleet/workers/', b''),
        (f'{DEFAULT_KEY_PREFIX}/fleet/workers/x/y', serialize_card(_card('x'))),
    ],
    ids=['folder_key', 'nested_key'],
)
async def test_list_skips_a_key_that_cannot_be_a_card_key_without_a_log_line(key, value, fake):
    fake.put_raw(key, value)
    provider = await _started(fake)
    await provider.publish(_card('w-1'))

    with capture_logs() as logs:
        listed = await provider.list()

    assert [entry.card['worker'] for entry in listed] == ['w-1']
    assert _invalid_card_logs(logs) == []


async def test_list_skips_a_key_outside_the_card_prefix(fake):
    def add_a_key_outside_the_prefix(entries: list[dict[str, Any]]) -> None:
        entries.append({'Key': 'w-9', 'Value': None, 'Session': FIRST})

    provider = ConsulRegistry(
        connection='main',
        offline_after_seconds=OFFLINE_AFTER,
        client=_client_rewriting_reads(fake, add_a_key_outside_the_prefix),
    )
    await provider.start(_context())
    await provider.publish(_card('w-1'))

    with capture_logs() as logs:
        listed = await provider.list()

    assert [entry.card['worker'] for entry in listed] == ['w-1']
    assert _invalid_card_logs(logs) == []


async def test_list_skips_an_undecodable_card_and_logs_once(fake):
    fake.plant_session(FOREIGN_SESSION, f'drakkar-bad-{PREDECESSOR_INSTANCE}', SESSION_TTL_SECONDS)
    fake.put_raw(card_key('bad'), b'not json', session=FOREIGN_SESSION)
    provider = await _started(fake)
    await provider.publish(_card('w-1'))

    with capture_logs() as logs:
        first = await provider.list()
        second = await provider.list()

    assert [entry.card['worker'] for entry in first] == ['w-1']
    assert [entry.card['worker'] for entry in second] == ['w-1']
    assert _invalid_card_logs(logs) == [
        {
            'event': 'fleet_card_invalid',
            'category': 'fleet',
            'worker': 'bad',
            'reason': 'not_json',
            'log_level': 'warning',
        }
    ]


# --- Withdraw ---


async def test_withdraw_without_a_session_sends_nothing(fake):
    provider = await _started(fake)
    mark = len(fake.seen)

    await provider.withdraw('w-1', INSTANCE)

    assert fake.seen[mark:] == []


async def test_withdraw_destroys_the_session_and_forgets_it(fake):
    provider = await _started(fake)
    await provider.publish(_card())

    await provider.withdraw('w-1', INSTANCE)
    listed = await provider.list()
    mark = len(fake.seen)
    await provider.withdraw('w-1', INSTANCE)
    withdrawn_again = _routes(fake, mark)
    await provider.publish(_card())

    assert listed == []
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}']
    assert withdrawn_again == []
    # A publish after the withdraw starts a new session instead of renewing the destroyed one.
    assert _routes(fake, mark) == [('PUT', PATHS['session_create']), ('PUT', card_path('w-1'))]


def _client_rewriting_reads(fake: FakeConsul, rewrite: Callable[[list[dict[str, Any]]], None]) -> ConsulClient:
    """A client of ``fake`` whose key reads that answer ``200`` have their entries changed by ``rewrite`` first."""

    async def handle(request: httpx.Request) -> httpx.Response:
        response = await fake.transport().handle_async_request(request)
        if request.method != 'GET' or not request.url.path.startswith('/v1/kv/') or response.status_code != 200:
            return response
        entries = json.loads(response.content)
        rewrite(entries)
        return httpx.Response(200, json=entries)

    return ConsulClient(
        http=httpx.AsyncClient(transport=httpx.MockTransport(handle)), base=FAKE_BASE, token='', datacenter=''
    )


def _client_showing_session(fake: FakeConsul, shown: str | None) -> ConsulClient:
    """A client of ``fake`` whose key reads give every entry no session holds the field ``"Session": shown``.

    The agent leaves the field out of such an entry; another writer of the
    answer could send ``null`` or an empty string, which must read the same.
    """

    def show_the_session(entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            entry.setdefault('Session', shown)

    return _client_rewriting_reads(fake, show_the_session)


async def test_a_withdraw_during_a_publish_leaves_no_session_the_provider_forgot(fake):
    reached, release = asyncio.Event(), asyncio.Event()
    provider = ConsulRegistry(
        connection='main',
        offline_after_seconds=OFFLINE_AFTER,
        client=client_holding_the_first_renew(fake, reached, release),
    )
    await provider.start(_context())
    await provider.publish(_card())

    publishing = asyncio.create_task(provider.publish(_card()))
    await reached.wait()
    await provider.withdraw('w-1', INSTANCE)
    release.set()
    await publishing
    listed_after_the_publish = await provider.list()
    await provider.withdraw('w-1', INSTANCE)

    # The publish found its session destroyed (renew 404) and made a new one; the provider kept that one,
    # so the next withdraw destroyed it and no card is left.
    assert [entry.card['worker'] for entry in listed_after_the_publish] == ['w-1']
    assert _destroys(fake) == [f'/v1/session/destroy/{FIRST}', f'/v1/session/destroy/{SECOND}']
    assert await provider.list() == []


async def test_withdraw_after_a_takeover_is_harmless(fake):
    first = await _started(fake)
    second = await _started(fake)
    await first.publish(_card(instance=INSTANCE))
    await second.publish(_card(instance=OTHER_INSTANCE))

    await first.withdraw('w-1', INSTANCE)
    listed = await second.list()

    assert [(entry.card['instance'], entry.online) for entry in listed] == [(OTHER_INSTANCE, True)]


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
    with pytest.raises(RuntimeError, match=r'^ConsulRegistry used before start\(\)$'):
        await provider.publish(_card())
    fake.leader = LEADER
    await provider.start(_context())
    await provider.publish(_card())

    assert _routes(fake) == [
        ('GET', PATHS['leader']),
        ('GET', PATHS['leader']),
        ('PUT', PATHS['session_create']),
        ('PUT', card_path('w-1')),
    ]


async def test_start_without_connections_raises():
    provider = ConsulRegistry(connection='main', offline_after_seconds=OFFLINE_AFTER)

    with pytest.raises(RuntimeError, match=r'^the consul registry needs the worker connections$'):
        await provider.start(_context(connections=None))


async def test_start_through_the_connections_shares_one_client_with_the_prefix_of_the_entry(fake):
    connections = Connections(ConnectionsConfig(consul={'main': ConsulConnectionConfig(key_prefix='ops/fleet')}))
    opener = AsyncMock(return_value=fake_client(fake))
    first = ConsulRegistry(connection='main', offline_after_seconds=OFFLINE_AFTER, open_client=opener)
    second = ConsulRegistry(connection='main', offline_after_seconds=OFFLINE_AFTER, open_client=opener)

    await first.start(_context(connections, worker='w-1'))
    await second.start(_context(connections, worker='w-2'))
    await first.publish(_card('w-1'))
    await second.publish(_card('w-2', OTHER_INSTANCE))
    listed = await first.list()

    opener.assert_awaited_once()
    assert opener.await_args is not None
    assert opener.await_args.args[0].key_prefix == 'ops/fleet'
    assert sorted(entry.card['worker'] for entry in listed) == ['w-1', 'w-2']
    assert ('PUT', card_path('w-2', 'ops/fleet')) in _routes(fake)


async def test_start_after_stop_raises_provider_stopped(fake):
    provider = await _started(fake)
    await provider.stop()

    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await provider.start(_context())


async def test_calls_before_start_raise(fake):
    provider = _provider(fake)

    for call in (provider.publish(_card()), provider.list(), provider.withdraw('w-1', INSTANCE)):
        with pytest.raises(RuntimeError, match=r'^ConsulRegistry used before start\(\)$'):
            await call
    assert fake.seen == []


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
async def test_calls_after_stop_raise(call, fake):
    provider = await _started(fake)
    await provider.publish(_card())
    mark = len(fake.seen)

    await provider.stop()
    with pytest.raises(RuntimeError, match=f'^{PROVIDER_STOPPED}$'):
        await call(provider)

    # Neither the stop nor the refused call sends a request: the session is left to the withdraw.
    assert fake.seen[mark:] == []


async def test_stop_leaves_the_shared_client_to_the_connections(fake):
    connections = Connections(ConnectionsConfig(consul={'main': ConsulConnectionConfig()}))
    client = fake_client(fake)
    closer = AsyncMock()
    provider = ConsulRegistry(
        connection='main',
        offline_after_seconds=OFFLINE_AFTER,
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


async def test_an_acquire_with_a_vanished_session_is_retried_with_a_new_one(fake):
    resilience = ResilienceConfig(timeout_seconds=1.0, retries=1, backoff_seconds=0.0)
    service = _service(fake, resilience, [100.0])
    ended: list[str] = []

    def end_the_session_at_the_first_acquire(seen: Seen) -> None:
        if ended or not _is_acquire(seen):
            return
        ended.append(dict(seen.query)['acquire'])
        fake.end_session(ended[0])

    fake.before_answer = end_the_session_at_the_first_acquire
    ok_before, error_before = _calls('publish', 'ok'), _calls('publish', 'error')

    await service._publish_tick('running')
    sent = [(seen.method, seen.path, dict(seen.query).get('acquire')) for seen in fake.seen]
    listed = await (await _started(fake)).list()

    assert ended == [FIRST]
    assert sent == [
        ('GET', PATHS['leader'], None),
        ('PUT', PATHS['session_create'], None),
        ('PUT', card_path('w-1'), FIRST),
        ('PUT', f'/v1/session/renew/{FIRST}', None),
        ('PUT', PATHS['session_create'], None),
        ('PUT', card_path('w-1'), SECOND),
    ]
    assert _calls('publish', 'ok') - ok_before == 1
    assert _calls('publish', 'error') - error_before == 0
    assert [(entry.card['worker'], entry.online) for entry in listed] == [('w-1', True)]


async def test_a_hung_call_ends_at_its_limit(fake):
    provider = await _started(fake)
    fake.hang = True
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(provider.publish(_card()), 0.05)
    elapsed = time.monotonic() - started
    await provider.publish(_card())
    listed = await provider.list()

    assert elapsed < 1.0
    assert [(entry.card['worker'], entry.online) for entry in listed] == [('w-1', True)]


async def test_a_dead_agent_opens_the_breaker_and_a_live_one_closes_it(fake):
    resilience = ResilienceConfig(
        timeout_seconds=1.0, retries=0, breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=0.0)
    )
    service = _service(fake, resilience, [100.0])

    with capture_logs() as logs:
        await service._publish_tick('running')
        fake.fail_every = httpx.ConnectError('down')
        await service._publish_tick('running')
        await service._publish_tick('running')
        opened = service._caller.breaker_open
        fake.fail_every = None
        await service._publish_tick('running')

    listed = await (await _started(fake)).list()
    assert opened
    assert not service._caller.breaker_open
    events = [entry['event'] for entry in logs]
    assert events.index('provider_breaker_opened') < events.index('provider_breaker_closed')
    assert [(entry.card['worker'], entry.online) for entry in listed] == [('w-1', True)]


# --- Registration ---


def test_builtin_providers_include_consul():
    assert BUILTIN_PROVIDERS['consul'] is consul_registry_factory


async def test_consul_registry_factory_builds_a_provider_from_the_options(fake, monkeypatch):
    # The real client of the connection entry is opened; its transport answers from the fake, so nothing connects.
    async def answer_from_the_fake(transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        return await fake.transport().handle_async_request(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, 'handle_async_request', answer_from_the_fake)
    connections = Connections(ConnectionsConfig(consul={'locks': ConsulConnectionConfig(key_prefix='ops')}))
    provider = consul_registry_factory({'connection': 'locks', 'offline_after_seconds': 120})

    await provider.start(_context(connections))
    await provider.publish(_card())
    await connections.close_all()

    create = next(seen for seen in fake.seen if seen.path == PATHS['session_create'])
    assert create.body is not None
    assert json.loads(create.body)['TTL'] == '60s'
    assert _routes(fake)[-1] == ('PUT', card_path('w-1', 'ops'))
