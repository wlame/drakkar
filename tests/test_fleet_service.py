"""The registry service: publish and refresh loops, snapshot, liveness, failures.

Providers here are in-process fakes. Loop intervals come from configs built
with ``model_construct`` so a test can use sub-second timings that the
config validation would reject. The provider caller of most tests reads the
test's fake clock and skips the backoff waits, and makes one attempt per
operation unless a test asks for retries.
"""

import asyncio
import time
from datetime import UTC, datetime

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config import AutodiscoverConfig, ResilienceConfig
from drakkar.config.connections import ConnectionsConfig
from drakkar.fleet.card import WorkerIdentity
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.fleet.service import FleetRegistry
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import ResilientCaller
from tests.conftest import wait_for
from tests.fleet_fakes import MemoryRegistry, MemoryStore

OWN = WorkerIdentity(
    worker='w-1',
    instance='1111111111111111',
    cluster='search',
    url='http://10.0.0.1:8080/',
    backend='python',
    version='9.9.9',
    started_at=datetime(2026, 10, 14, 9, 0, tzinfo=UTC),
)


def _config(timeout_seconds: float = 1.0, retries: int = 0, **overrides) -> AutodiscoverConfig:
    values = {
        'enabled': True,
        'provider': 'memory',
        'heartbeat': 3600,
        'refresh': 3600,
        'offline_after': 30,
        'prune_after': 86_400,
        'resilience': ResilienceConfig(timeout=f'{timeout_seconds}s', max_retries=retries),
    }
    values.update(overrides)
    return AutodiscoverConfig.model_construct(**values)


async def _no_backoff(_seconds: float) -> None:
    await asyncio.sleep(0)


def _never_opening_breaker() -> ResilienceConfig:
    """One attempt per operation and a breaker that stays closed, for tests that fail many operations in a row.

    An open breaker skips ``start`` too, and the fake clock never ends its cooldown.
    """
    return ResilienceConfig(timeout='1s', max_retries=0, breaker_failures=1000)


def _peer(worker: str, instance: str = '2222222222222222', cluster: str = 'search', **extra) -> dict:
    return {
        'schema': 1,
        'worker': worker,
        'instance': instance,
        'cluster': cluster,
        'url': f'http://{worker}:8080/',
        **extra,
    }


class _Clock:
    """A monotonic clock and a wall clock the test moves together."""

    def __init__(self) -> None:
        self.monotonic = 100.0
        self.epoch = 1_760_000_000.0

    def advance(self, seconds: float) -> None:
        self.monotonic += seconds
        self.epoch += seconds


class _Recording(MemoryRegistry):
    """A memory provider that records every call in order."""

    def __init__(self, store: MemoryStore) -> None:
        super().__init__(store)
        self.calls: list[tuple] = []

    async def publish(self, card: dict) -> None:
        self.calls.append(('publish', card['state']))
        await super().publish(card)

    async def list(self) -> PeerCardList:
        self.calls.append(('list',))
        return await super().list()

    async def withdraw(self, worker: str, instance: str) -> None:
        self.calls.append(('withdraw', worker, instance))
        await super().withdraw(worker, instance)


class _Listing:
    """A provider whose ``list`` returns whatever the test put in ``cards``."""

    def __init__(self, cards: PeerCardList | None = None) -> None:
        self.cards: PeerCardList = cards or []
        self.withdrawn = 0

    async def start(self, ctx: RegistryContext) -> None:
        return None

    async def publish(self, card: dict) -> None:
        return None

    async def list(self) -> PeerCardList:
        return list(self.cards)

    async def withdraw(self, worker: str, instance: str) -> None:
        self.withdrawn += 1

    async def stop(self) -> None:
        return None


class _Failing(_Listing):
    """Every call raises, or (``hang=True``) never returns."""

    def __init__(self, hang: bool = False) -> None:
        super().__init__()
        self._hang = hang
        self.stopped = False

    async def _fail(self) -> None:
        if self._hang:
            await asyncio.Event().wait()
        raise OSError('registry storage unavailable')

    async def publish(self, card: dict) -> None:
        await self._fail()

    async def list(self) -> PeerCardList:
        await self._fail()
        return []

    async def withdraw(self, worker: str, instance: str) -> None:
        self.withdrawn += 1
        await self._fail()

    async def stop(self) -> None:
        self.stopped = True


class _StartsWhenAllowed(_Recording):
    """A memory provider whose ``start`` raises ``ConnectionError`` until the test sets ``allowed``."""

    def __init__(self, store: MemoryStore) -> None:
        super().__init__(store)
        self.allowed = False

    async def start(self, ctx: RegistryContext) -> None:
        if not self.allowed:
            raise ConnectionError('registry service unreachable')
        await super().start(ctx)


def _service(provider, clock: _Clock | None = None, features=None, on_refresh=None, **config) -> FleetRegistry:
    clock = clock or _Clock()
    registry_config = _config(**config)
    return FleetRegistry(
        provider,
        provider_name='memory',
        config=registry_config,
        identity=OWN,
        features=features,
        on_refresh=on_refresh,
        now_monotonic=lambda: clock.monotonic,
        now_epoch=lambda: clock.epoch,
        caller=ResilientCaller(
            registry_config.resilience,
            feature='registry',
            provider='memory',
            now_monotonic=lambda: clock.monotonic,
            sleep=_no_backoff,
        ),
    )


def _sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def _calls(op: str, outcome: str) -> float:
    """``drakkar_provider_calls_total`` of this file's provider for one operation and outcome."""
    return _sample(
        'drakkar_provider_calls_total', {'feature': 'registry', 'provider': 'memory', 'op': op, 'outcome': outcome}
    )


# --- publishing ---


async def test_states_are_published_in_order_and_the_card_is_withdrawn_at_stop():
    provider = _Recording(MemoryStore())
    service = _service(provider)

    await service.start()
    await wait_for(lambda: ('publish', 'starting') in provider.calls)
    service.set_state('running')
    await wait_for(lambda: ('publish', 'running') in provider.calls)
    service.set_state('draining')
    await wait_for(lambda: ('publish', 'draining') in provider.calls)
    await service.stop()

    publishes = [call[1] for call in provider.calls if call[0] == 'publish']
    assert publishes == ['starting', 'running', 'draining']
    assert provider.calls[-1] == ('withdraw', 'w-1', OWN.instance)


async def test_the_card_published_at_start_has_the_contract_fields():
    store = MemoryStore()
    service = _service(MemoryRegistry(store))

    await service.start()
    await wait_for(lambda: 'w-1' in store.cards)
    card = store.cards['w-1'][0]
    await service.stop()

    assert card['schema'] == 1
    assert (card['worker'], card['instance'], card['cluster'], card['url']) == (
        'w-1',
        OWN.instance,
        'search',
        'http://10.0.0.1:8080/',
    )
    assert (card['backend'], card['version'], card['state']) == ('python', '9.9.9', 'starting')
    assert card['started_at'] == '2026-10-14T09:00:00.000000Z'
    assert card['features'] == {}


async def test_every_publish_reads_the_features_supplier_at_that_moment():
    # The cache engine can start after the registry: the supplier is read
    # at each publish, never once at construction.
    store = MemoryStore()
    features: dict = {}
    service = FleetRegistry(
        MemoryRegistry(store),
        provider_name='memory',
        config=_config(heartbeat=0.02),
        identity=OWN,
        features=lambda: dict(features),
    )

    await service.start()
    await wait_for(lambda: 'w-1' in store.cards)
    features['cache'] = {'cluster': {'ms': 5, 'key': 'k'}}
    await wait_for(lambda: store.cards['w-1'][0]['features'] == {'cache': {'cluster': {'ms': 5, 'key': 'k'}}})
    await service.stop()


async def test_heartbeats_republish_the_card():
    provider = _Recording(MemoryStore())
    service = _service(provider, heartbeat=0.02)

    await service.start()
    await wait_for(lambda: len([call for call in provider.calls if call[0] == 'publish']) >= 3)
    await service.stop()


async def test_the_first_refresh_waits_for_the_first_publish():
    """A file folder learns the server clock from its own write; listing first would give unknown ages."""

    class _SlowPublish(_Recording):
        async def publish(self, card: dict) -> None:
            await asyncio.sleep(0.05)
            await super().publish(card)
            self.calls.append(('published',))

    provider = _SlowPublish(MemoryStore())
    service = _service(provider)

    await service.start()
    await wait_for(lambda: ('list',) in provider.calls)
    await service.stop()

    assert provider.calls.index(('published',)) < provider.calls.index(('list',))


async def test_the_first_refresh_still_runs_when_the_first_publish_fails():
    provider = _Failing()
    service = _service(provider)
    before = _sample('drakkar_fleet_registry_errors_total', {'op': 'list'})

    await service.start()
    await wait_for(lambda: _sample('drakkar_fleet_registry_errors_total', {'op': 'list'}) > before)
    await service.stop()


# --- the snapshot ---


async def test_the_snapshot_leaves_out_the_own_card_and_keeps_the_peers():
    provider = _Listing(
        [
            PeerCard(card=_peer('w-1', instance=OWN.instance), age_seconds=1.0),
            PeerCard(card=_peer('w-2'), age_seconds=2.0),
            PeerCard(card=_peer('w-3', cluster=''), age_seconds=3.0),
        ]
    )
    service = _service(provider)

    await service._refresh_once()

    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2', 'w-3']


async def test_peer_views_compute_liveness_at_read_time():
    """An age is the age at list time plus the time since; a slow refresh never makes a card look younger."""
    clock = _Clock()
    provider = _Listing(
        [
            PeerCard(card=_peer('fresh'), age_seconds=5.0),
            PeerCard(card=_peer('ageing'), age_seconds=20.0),
            PeerCard(card=_peer('static-peer'), age_seconds=None),
        ]
    )
    service = _service(provider, clock)
    await service._refresh_once()

    clock.advance(15)
    views = {view.card['worker']: view for view in service.peer_views()}

    assert views['fresh'].online is True
    assert views['fresh'].age_seconds == pytest.approx(20.0)
    assert views['fresh'].last_seen_ts == pytest.approx(clock.epoch - 20.0)
    assert views['ageing'].online is False
    assert views['static-peer'].online is None
    assert views['static-peer'].last_seen_ts is None


async def test_the_peers_gauge_counts_by_liveness():
    provider = _Listing(
        [
            PeerCard(card=_peer('a'), age_seconds=1.0),
            PeerCard(card=_peer('b'), age_seconds=2.0),
            PeerCard(card=_peer('c'), age_seconds=99.0),
            PeerCard(card=_peer('d'), age_seconds=None),
            PeerCard(card=_peer('w-1', instance=OWN.instance), age_seconds=0.0),
        ]
    )
    service = _service(provider)

    await service._refresh_once()

    assert _sample('drakkar_fleet_peers', {'state': 'online'}) == 2
    assert _sample('drakkar_fleet_peers', {'state': 'offline'}) == 1
    assert _sample('drakkar_fleet_peers', {'state': 'unknown'}) == 1
    assert _sample('drakkar_fleet_registry_snapshot_age_seconds') == 0


@pytest.mark.parametrize(
    ('age', 'online', 'expected'),
    [
        (1000.0, True, True),
        (1.0, False, False),
        (None, True, True),
        (None, False, False),
        (1.0, None, True),
        (1000.0, None, False),
        (None, None, None),
        (1000.0, 'yes', False),
    ],
    ids=[
        'online-overrides-old-age',
        'offline-overrides-young-age',
        'online-without-age',
        'offline-without-age',
        'age-rule-young',
        'age-rule-old',
        'unknown',
        'non-bool-verdict-ignored',
    ],
)
async def test_online_from_the_provider_overrides_the_age_rule(age, online, expected):
    provider = _Listing([PeerCard(card=_peer('w-2'), age_seconds=age, online=online)])
    service = _service(provider)
    await service._refresh_once()

    (view,) = service.peer_views()

    assert view.online is expected
    assert (view.last_seen_ts is None) == (age is None)


async def test_the_peers_gauge_follows_the_providers_online_verdict():
    provider = _Listing(
        [
            PeerCard(card=_peer('old-but-online'), age_seconds=1000.0, online=True),
            PeerCard(card=_peer('young-but-offline'), age_seconds=1.0, online=False),
            PeerCard(card=_peer('online-without-age'), age_seconds=None, online=True),
            PeerCard(card=_peer('age-rule'), age_seconds=1.0),
        ]
    )
    service = _service(provider)

    await service._refresh_once()

    assert _sample('drakkar_fleet_peers', {'state': 'online'}) == 3
    assert _sample('drakkar_fleet_peers', {'state': 'offline'}) == 1
    assert _sample('drakkar_fleet_peers', {'state': 'unknown'}) == 0


# --- invalid cards ---


async def test_invalid_and_unsupported_cards_are_skipped_and_logged_once_per_worker():
    provider = _Listing(
        [
            PeerCard(card={'schema': 2, 'worker': 'future'}, age_seconds=1.0),
            PeerCard(card={'schema': 1, 'worker': 'broken', 'cluster': 7, 'url': 'u'}, age_seconds=1.0),
            PeerCard(card={'schema': 1, 'cluster': '', 'url': 'u'}, age_seconds=1.0),
            PeerCard(card=_peer('w-2'), age_seconds=1.0),
        ]
    )
    service = _service(provider)

    with capture_logs() as first:
        await service._refresh_once()
    with capture_logs() as second:
        await service._refresh_once()

    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2']
    unsupported = [entry for entry in first if entry['event'] == 'fleet_card_schema_unsupported']
    assert [(entry['worker'], entry['schema']) for entry in unsupported] == [('future', 2)]
    invalid = sorted((entry['worker'], entry['reason']) for entry in first if entry['event'] == 'fleet_card_invalid')
    assert invalid == [('', 'missing_field:worker'), ('broken', 'bad_type:cluster')]
    assert [entry for entry in second if entry['event'].startswith('fleet_card_')] == []


# --- failures ---


async def test_a_hanging_list_times_out_keeps_the_snapshot_and_counts_the_error():
    clock = _Clock()
    provider = _Listing([PeerCard(card=_peer('w-2'), age_seconds=1.0)])
    service = _service(provider, clock, timeout_seconds=0.05)
    await service._refresh_once()
    hanging = _Failing(hang=True)
    service._provider = hanging
    before = _sample('drakkar_fleet_registry_errors_total', {'op': 'list'})

    clock.advance(40)
    await service._refresh_once()

    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2']
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'list'}) == before + 1
    assert _sample('drakkar_fleet_registry_snapshot_age_seconds') == pytest.approx(40)


async def test_failure_logs_are_limited_to_one_a_minute_per_operation():
    clock = _Clock()
    service = _service(_Failing(), clock)

    with capture_logs() as logs:
        await service._refresh_once()
        clock.advance(30)
        await service._refresh_once()
        await service._publish_once('running')
        clock.advance(31)
        await service._refresh_once()

    failures = [entry for entry in logs if entry['event'] == 'provider_call_failed']
    assert [entry['op'] for entry in failures] == ['list', 'publish', 'list']
    assert failures[2]['suppressed'] == 1
    assert (failures[0]['feature'], failures[0]['provider']) == ('registry', 'memory')


async def test_call_durations_are_observed_per_operation():
    before = _sample('drakkar_fleet_registry_duration_seconds_count', {'op': 'list'})
    service = _service(_Listing())

    await service._refresh_once()

    assert _sample('drakkar_fleet_registry_duration_seconds_count', {'op': 'list'}) == before + 1


async def test_stop_withdraws_once_and_survives_a_failing_withdraw():
    provider = _Failing()
    service = _service(provider)
    await service.start()
    await wait_for(service._provider_started.is_set)
    before = _sample('drakkar_fleet_registry_errors_total', {'op': 'withdraw'})

    with capture_logs() as logs:
        await service.stop()
        await service.stop()

    assert provider.withdrawn == 1
    assert provider.stopped
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'withdraw'}) == before + 1
    withdraw_failures = [
        entry for entry in logs if entry['event'] == 'provider_call_failed' and entry['op'] == 'withdraw'
    ]
    assert len(withdraw_failures) == 1


async def test_stop_without_a_deadline_makes_one_withdraw_attempt_under_the_call_timeout():
    provider = _Failing(hang=True)
    service = _service(provider, timeout_seconds=0.05, retries=2)
    await service.start()
    await wait_for(service._provider_started.is_set)

    await asyncio.wait_for(service.stop(), timeout=2)

    assert provider.withdrawn == 1
    assert provider.stopped


# --- starting the provider ---


async def test_a_failing_start_is_retried_each_heartbeat_and_publishes_right_after_success():
    provider = _StartsWhenAllowed(MemoryStore())
    clock = _Clock()
    service = _service(provider, clock, retries=2)
    skipped_before = _calls('publish', 'not_started')

    with capture_logs() as logs:
        await service._publish_tick('starting')
        clock.advance(1)
        await service._publish_tick('starting')
        clock.advance(1)
        provider.allowed = True
        await service._publish_tick('starting')

    failures = [entry for entry in logs if entry['event'] == 'provider_call_failed']
    assert [(entry['op'], entry['attempts']) for entry in failures] == [('start', 3)]
    assert 'registry service unreachable' in failures[0]['error']
    assert _calls('publish', 'not_started') == skipped_before + 2
    (started,) = [entry for entry in logs if entry['event'] == 'provider_started']
    assert (started['category'], started['feature'], started['provider']) == ('fleet', 'registry', 'memory')
    assert service._provider_started.is_set()
    assert provider.calls == [('publish', 'starting')]


async def test_the_provider_starts_with_the_identity_options_and_connections():
    provider = MemoryRegistry(MemoryStore())
    connections = Connections(ConnectionsConfig())
    service = FleetRegistry(
        provider,
        provider_name='memory',
        config=_config(),
        identity=OWN,
        provider_options={'region': 'north'},
        connections=connections,
    )

    await service._publish_tick('starting')

    ctx = provider.started_with
    assert ctx is not None
    assert (ctx.worker, ctx.instance, ctx.cluster) == ('w-1', OWN.instance, 'search')
    assert ctx.options == {'region': 'north'}
    assert ctx.connections is connections


async def test_start_failures_log_once_a_minute():
    clock = _Clock()
    service = _service(_StartsWhenAllowed(MemoryStore()), clock, resilience=_never_opening_breaker())

    with capture_logs() as within_a_minute:
        for _tick in range(10):
            await service._publish_tick('starting')
            clock.advance(5)
    clock.advance(15)
    with capture_logs() as after_a_minute:
        await service._publish_tick('starting')

    start_failures = [entry for entry in within_a_minute if entry['event'] == 'provider_call_failed']
    assert [entry['op'] for entry in start_failures] == ['start']
    (next_line,) = [entry for entry in after_a_minute if entry['event'] == 'provider_call_failed']
    assert next_line['suppressed'] == 9


async def test_an_open_breaker_holds_start_back_until_the_cooldown_then_the_probe_starts_and_publishes():
    provider = _StartsWhenAllowed(MemoryStore())
    clock = _Clock()
    resilience = ResilienceConfig(timeout='1s', max_retries=0, breaker_failures=1, breaker_cooldown='30s')
    service = _service(provider, clock, resilience=resilience)
    skipped_before = _calls('start', 'skipped_open')

    await service._publish_tick('running')
    provider.allowed = True
    clock.advance(10)
    await service._publish_tick('running')
    held_back = (_calls('start', 'skipped_open') - skipped_before, list(provider.calls))
    clock.advance(20)
    await service._publish_tick('running')

    assert held_back == (1, [])
    assert service._provider_started.is_set()
    assert provider.calls == [('publish', 'running')]


async def test_the_snapshot_age_grows_from_the_service_start_while_the_provider_has_not_started():
    clock = _Clock()
    service = _service(_StartsWhenAllowed(MemoryStore()), clock)
    clock.advance(42)

    await service._publish_tick('starting')

    assert _sample('drakkar_fleet_registry_snapshot_age_seconds') == pytest.approx(42)


async def test_nothing_is_listed_before_the_provider_starts():
    provider = _StartsWhenAllowed(MemoryStore())
    service = _service(provider, heartbeat=0.01, refresh=0.01, resilience=_never_opening_breaker())
    skipped_before = _calls('publish', 'not_started')

    await service.start()
    await wait_for(lambda: _calls('publish', 'not_started') >= skipped_before + 3)
    listed_before_start = [call for call in provider.calls if call[0] == 'list']
    provider.allowed = True
    await wait_for(lambda: ('list',) in provider.calls)
    await service.stop()

    assert listed_before_start == []


async def test_stop_skips_withdraw_and_provider_stop_when_the_provider_never_started():
    provider = _StartsWhenAllowed(MemoryStore())
    service = _service(provider)
    skipped_before = (_calls('withdraw', 'not_started'), _calls('stop', 'not_started'))
    publish_skips_before = _calls('publish', 'not_started')
    await service.start()
    await wait_for(lambda: _calls('publish', 'not_started') > publish_skips_before)

    await service.stop()

    assert ('withdraw', 'w-1', OWN.instance) not in provider.calls
    assert not provider.stopped
    assert (_calls('withdraw', 'not_started'), _calls('stop', 'not_started')) == (
        skipped_before[0] + 1,
        skipped_before[1] + 1,
    )


# --- the loop guard ---


async def test_an_exception_in_the_features_callback_does_not_stop_the_loops():
    store = MemoryStore()
    raised = []

    def features() -> dict:
        if not raised:
            raised.append(True)
            raise ValueError('features supplier broke')
        return {}

    service = _service(MemoryRegistry(store), features=features, heartbeat=0.01)
    errors_before = _sample('drakkar_fleet_registry_errors_total', {'op': 'publish'})

    with capture_logs() as logs:
        await service.start()
        await wait_for(lambda: 'w-1' in store.cards)
        await service.stop()

    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_iteration_failed']
    assert (failure['category'], failure['loop']) == ('fleet', 'publish')
    assert failure['error'] == 'features supplier broke'
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'publish'}) == errors_before + 1


async def test_an_exception_while_building_the_snapshot_does_not_stop_the_refresh_loop(monkeypatch):
    provider = _Listing([PeerCard(card=_peer('w-2'), age_seconds=1.0)])
    service = _service(provider, refresh=0.01)
    original_peers_from = service._peers_from
    raised = []

    async def peers_from_once_broken(listed, listed_at):
        if not raised:
            raised.append(True)
            raise KeyError('url')
        return await original_peers_from(listed, listed_at)

    monkeypatch.setattr(service, '_peers_from', peers_from_once_broken)
    errors_before = _sample('drakkar_fleet_registry_errors_total', {'op': 'list'})

    with capture_logs() as logs:
        await service.start()
        await wait_for(lambda: service.snapshot().refreshed_at is not None)
        await service.stop()

    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_iteration_failed']
    assert failure['loop'] == 'refresh'
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'list'}) == errors_before + 1
    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2']


async def test_loop_iteration_failures_log_once_a_minute_per_loop():
    def features() -> dict:
        raise ValueError('features supplier broke')

    clock = _Clock()
    service = _service(MemoryRegistry(MemoryStore()), clock, features=features)
    errors_before = _sample('drakkar_fleet_registry_errors_total', {'op': 'publish'})

    with capture_logs() as logs:
        await service._publish_tick('running')
        clock.advance(30)
        await service._publish_tick('running')
        clock.advance(31)
        await service._publish_tick('running')

    failures = [entry for entry in logs if entry['event'] == 'fleet_registry_iteration_failed']
    assert [entry['suppressed'] for entry in failures] == [0, 1]
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'publish'}) == errors_before + 3


async def test_a_publish_iteration_that_keeps_raising_does_not_stop_the_refresh_loop():
    def features() -> dict:
        raise ValueError('features supplier broke')

    provider = _Recording(MemoryStore())
    service = _service(provider, features=features, heartbeat=0.01, refresh=0.01)

    await service.start()
    await wait_for(lambda: provider.calls.count(('list',)) >= 3)
    await service.stop()

    assert [call for call in provider.calls if call[0] == 'publish'] == []


# --- stopping inside a deadline ---


async def test_stop_skips_withdraw_at_once_while_the_breaker_is_open():
    provider = _Failing()
    resilience = ResilienceConfig(timeout='1s', max_retries=0, breaker_failures=2, breaker_cooldown='30s')
    service = _service(provider, resilience=resilience)
    await service.start()
    await wait_for(lambda: service._caller.breaker_open)
    skipped_before = (_calls('withdraw', 'skipped_open'), _calls('stop', 'skipped_open'))

    started = time.monotonic()
    await service.stop(deadline_seconds=5)

    assert time.monotonic() - started < 0.5
    assert (_calls('withdraw', 'skipped_open'), _calls('stop', 'skipped_open')) == (
        skipped_before[0] + 1,
        skipped_before[1] + 1,
    )
    assert provider.withdrawn == 0
    assert provider.stopped is False


async def test_stop_makes_one_withdraw_attempt_inside_the_deadline():
    class _HangingWithdraw(_Listing):
        async def withdraw(self, worker: str, instance: str) -> None:
            self.withdrawn += 1
            await asyncio.Event().wait()

    provider = _HangingWithdraw()
    # The real clock: the deadline has to pass while the withdraw hangs.
    service = FleetRegistry(
        provider,
        provider_name='memory',
        config=_config(timeout_seconds=5.0, retries=2),
        identity=OWN,
    )
    await service.start()
    await wait_for(service._provider_started.is_set)
    timeouts_before = _calls('withdraw', 'timeout')

    started = time.monotonic()
    await service.stop(deadline_seconds=0.2)

    assert time.monotonic() - started < 0.5
    assert provider.withdrawn == 1
    assert _calls('withdraw', 'timeout') == timeouts_before + 1


# --- duplicate worker names ---


async def test_a_live_foreign_instance_with_this_name_is_reported_once_per_instance():
    provider = _Listing([PeerCard(card=_peer('w-1', instance='3333333333333333'), age_seconds=5.0)])
    service = _service(provider)
    before = _sample('drakkar_fleet_duplicate_worker_name_total')

    with capture_logs() as logs:
        await service._refresh_once()
        await service._refresh_once()
        provider.cards = [PeerCard(card=_peer('w-1', instance='4444444444444444'), age_seconds=5.0)]
        await service._refresh_once()

    assert _sample('drakkar_fleet_duplicate_worker_name_total') == before + 2
    warned = [entry['other_instance'] for entry in logs if entry['event'] == 'fleet_duplicate_worker_name']
    assert warned == ['3333333333333333', '4444444444444444']
    assert service.snapshot().peers == ()


@pytest.mark.parametrize(
    ('age', 'online'),
    [(31.0, None), (None, None), (5.0, False)],
    ids=['offline', 'unknown-age', 'provider-says-offline'],
)
async def test_a_stale_or_unknown_foreign_card_with_this_name_is_not_a_duplicate(age, online):
    provider = _Listing([PeerCard(card=_peer('w-1', instance='3333333333333333'), age_seconds=age, online=online)])
    service = _service(provider)
    before = _sample('drakkar_fleet_duplicate_worker_name_total')

    await service._refresh_once()

    assert _sample('drakkar_fleet_duplicate_worker_name_total') == before


async def test_a_foreign_card_the_provider_calls_online_is_a_duplicate_without_an_age():
    provider = _Listing([PeerCard(card=_peer('w-1', instance='5555555555555555'), age_seconds=None, online=True)])
    service = _service(provider)
    before = _sample('drakkar_fleet_duplicate_worker_name_total')

    await service._refresh_once()

    assert _sample('drakkar_fleet_duplicate_worker_name_total') == before + 1


# --- start log ---


async def test_start_logs_the_provider_and_identity():
    service = _service(_Listing())

    with capture_logs() as logs:
        await service.start()
        await service.stop()

    (started,) = [entry for entry in logs if entry['event'] == 'fleet_registry_started']
    assert started['provider'] == 'memory'
    assert started['instance'] == OWN.instance
    assert started['url'] == 'http://10.0.0.1:8080/'


async def test_a_provider_stop_that_fails_is_logged():
    class _BadStop(_Listing):
        async def stop(self) -> None:
            raise OSError('lost the session')

    service = _service(_BadStop())
    await service.start()
    await wait_for(service._provider_started.is_set)

    with capture_logs() as logs:
        await service.stop()

    (failure,) = [entry for entry in logs if entry['event'] == 'provider_call_failed']
    assert (failure['op'], failure['error'], failure['attempts']) == ('stop', 'lost the session', 1)


async def test_start_twice_runs_one_set_of_loops():
    provider = _Recording(MemoryStore())
    service = _service(provider)

    await service.start()
    first_task = service._task
    await service.start()

    assert service._task is first_task
    await service.stop()


async def test_every_state_is_published_even_when_the_next_arrives_before_the_first_publish():
    release = asyncio.Event()

    class _SlowStart(_Recording):
        async def start(self, ctx: RegistryContext) -> None:
            await release.wait()

    provider = _SlowStart(MemoryStore())
    service = _service(provider)

    await service.start()
    service.set_state('running')
    release.set()
    await wait_for(lambda: ('publish', 'running') in provider.calls)
    await service.stop()

    publishes = [call[1] for call in provider.calls if call[0] == 'publish']
    assert publishes[:2] == ['starting', 'running']


# --- malformed provider results ---


class _BadListing(_Recording):
    """A provider whose ``list`` returns something that is not a list of PeerCard."""

    def __init__(self, store: MemoryStore, result) -> None:
        super().__init__(store)
        self.result = result

    async def list(self):
        self.calls.append(('list',))
        return self.result


async def test_a_listing_that_is_not_a_list_counts_as_a_failed_list():
    service = _service(_BadListing(MemoryStore(), None))
    before = _sample('drakkar_fleet_registry_errors_total', {'op': 'list'})

    with capture_logs() as logs:
        await service._refresh_once()

    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'list'}) == before + 1
    assert [(entry['event'], entry['op']) for entry in logs] == [('provider_call_failed', 'list')]
    assert service.snapshot().refreshed_at is None


async def test_listed_entries_that_are_not_cards_are_skipped_as_invalid():
    provider = _BadListing(
        MemoryStore(),
        [PeerCard(card=None, age_seconds=1.0), 'garbage', PeerCard(card=_peer('w-2'), age_seconds=1.0)],
    )
    service = _service(provider)

    with capture_logs() as logs:
        await service._refresh_once()

    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2']
    assert [entry['reason'] for entry in logs if entry['event'] == 'fleet_card_invalid'] == ['not_object']


async def test_the_loops_keep_running_and_stop_withdraws_after_a_malformed_listing():
    provider = _BadListing(MemoryStore(), None)
    service = _service(provider, heartbeat=0.02, refresh=0.02)

    await service.start()
    await wait_for(lambda: len([call for call in provider.calls if call[0] == 'publish']) >= 3)
    await service.stop()

    assert provider.calls[-1] == ('withdraw', 'w-1', OWN.instance)


async def test_stop_still_withdraws_after_a_loop_died(monkeypatch):
    provider = _Recording(MemoryStore())
    service = _service(provider)

    async def dying_loop(first_state):
        await service._ensure_started()
        raise RuntimeError('loop bug')

    monkeypatch.setattr(service, '_publish_loop', dying_loop)
    await service.start()
    await wait_for(lambda: service._task.done())

    with capture_logs() as logs:
        await service.stop()

    assert provider.calls[-1] == ('withdraw', 'w-1', OWN.instance)
    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_loop_failed']
    assert 'loop bug' in failure['error']


# --- the refresh callback ---


async def test_on_refresh_gets_the_peer_views_after_each_good_refresh_only():
    clock = _Clock()
    provider = _Listing([PeerCard(card=_peer('w-2'), age_seconds=1.0)])
    seen: list[list] = []

    async def on_refresh(views) -> None:
        seen.append(views)

    service = _service(provider, clock, on_refresh=on_refresh)
    service._provider_started.set()

    await service._refresh_tick()
    provider.list = _Failing().list  # type: ignore[method-assign]
    await service._refresh_tick()

    (views,) = seen
    assert [(view.card['worker'], view.online) for view in views] == [('w-2', True)]


async def test_an_on_refresh_error_is_a_failed_refresh_iteration_and_keeps_the_snapshot():
    provider = _Listing([PeerCard(card=_peer('w-2'), age_seconds=1.0)])

    async def on_refresh(views) -> None:
        raise RuntimeError('hash check broke')

    service = _service(provider, on_refresh=on_refresh)
    service._provider_started.set()
    errors_before = _sample('drakkar_fleet_registry_errors_total', {'op': 'list'})

    with capture_logs() as logs:
        await service._refresh_tick()

    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_iteration_failed']
    assert (failure['loop'], failure['error']) == ('refresh', 'hash check broke')
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'list'}) == errors_before + 1
    assert [entry.card['worker'] for entry in service.snapshot().peers] == ['w-2']
