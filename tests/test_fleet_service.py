"""The registry service: publish and refresh loops, snapshot, liveness, failures.

Providers here are in-process fakes. Loop intervals come from configs built
with ``model_construct`` so a test can use sub-second timings that the
config validation would reject.
"""

import asyncio
from datetime import UTC, datetime

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config import FleetRegistryConfig
from drakkar.fleet.card import WorkerIdentity
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.fleet.service import FleetRegistry
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


def _config(**overrides) -> FleetRegistryConfig:
    values = {
        'provider': 'memory',
        'heartbeat_seconds': 3600,
        'refresh_seconds': 3600,
        'offline_after_seconds': 30,
        'prune_after_seconds': 86_400,
        'timeout_seconds': 1.0,
    }
    values.update(overrides)
    return FleetRegistryConfig.model_construct(**values)


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


def _service(provider, clock: _Clock | None = None, **config) -> FleetRegistry:
    clock = clock or _Clock()
    return FleetRegistry(
        provider,
        provider_name='memory',
        config=_config(**config),
        identity=OWN,
        now_monotonic=lambda: clock.monotonic,
        now_epoch=lambda: clock.epoch,
    )


def _sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


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
        config=_config(heartbeat_seconds=0.02),
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
    service = _service(provider, heartbeat_seconds=0.02)

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

    events = [entry['event'] for entry in logs]
    assert events.count('fleet_registry_list_failed') == 2
    assert events.count('fleet_registry_publish_failed') == 1
    second_list_log = [entry for entry in logs if entry['event'] == 'fleet_registry_list_failed'][1]
    assert second_list_log['suppressed'] == 1


async def test_call_durations_are_observed_per_operation():
    before = _sample('drakkar_fleet_registry_duration_seconds_count', {'op': 'list'})
    service = _service(_Listing())

    await service._refresh_once()

    assert _sample('drakkar_fleet_registry_duration_seconds_count', {'op': 'list'}) == before + 1


async def test_a_provider_that_fails_to_start_is_replaced_by_none():
    class _NoStart(_Listing):
        async def start(self, ctx: RegistryContext) -> None:
            raise ConnectionError('consul unreachable')

    provider = _NoStart([PeerCard(card=_peer('w-2'), age_seconds=1.0)])
    service = _service(provider)

    with capture_logs() as logs:
        await service.start()
        await wait_for(lambda: service.snapshot().refreshed_at is not None)
        await service.stop()

    assert service.snapshot().peers == ()
    failures = [entry for entry in logs if entry['event'] == 'fleet_registry_start_failed']
    assert len(failures) == 1
    assert 'consul unreachable' in failures[0]['error']


async def test_stop_withdraws_once_and_survives_a_failing_withdraw():
    provider = _Failing()
    service = _service(provider)
    await service.start()
    before = _sample('drakkar_fleet_registry_errors_total', {'op': 'withdraw'})

    with capture_logs() as logs:
        await service.stop()
        await service.stop()

    assert provider.withdrawn == 1
    assert provider.stopped
    assert _sample('drakkar_fleet_registry_errors_total', {'op': 'withdraw'}) == before + 1
    assert [entry['event'] for entry in logs].count('fleet_registry_withdraw_failed') == 1


async def test_stop_gives_up_on_a_hanging_withdraw_after_the_timeout():
    service = _service(_Failing(hang=True), timeout_seconds=0.05)
    await service.start()

    await asyncio.wait_for(service.stop(), timeout=2)


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


@pytest.mark.parametrize('age', [31.0, None], ids=['offline', 'unknown-age'])
async def test_a_stale_or_unknown_foreign_card_with_this_name_is_not_a_duplicate(age):
    provider = _Listing([PeerCard(card=_peer('w-1', instance='3333333333333333'), age_seconds=age)])
    service = _service(provider)
    before = _sample('drakkar_fleet_duplicate_worker_name_total')

    await service._refresh_once()

    assert _sample('drakkar_fleet_duplicate_worker_name_total') == before


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

    with capture_logs() as logs:
        await service.stop()

    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_stop_failed']
    assert failure['error'] == 'lost the session'


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
    assert [entry['event'] for entry in logs] == ['fleet_registry_list_failed']
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
    service = _service(provider, heartbeat_seconds=0.02, refresh_seconds=0.02)

    await service.start()
    await wait_for(lambda: len([call for call in provider.calls if call[0] == 'publish']) >= 3)
    await service.stop()

    assert provider.calls[-1] == ('withdraw', 'w-1', OWN.instance)


async def test_stop_still_withdraws_after_a_loop_died(monkeypatch):
    provider = _Recording(MemoryStore())
    service = _service(provider)

    async def dying_loop(first_state):
        raise RuntimeError('loop bug')

    monkeypatch.setattr(service, '_publish_loop', dying_loop)
    await service.start()
    await wait_for(lambda: service._task.done())

    with capture_logs() as logs:
        await service.stop()

    assert provider.calls[-1] == ('withdraw', 'w-1', OWN.instance)
    (failure,) = [entry for entry in logs if entry['event'] == 'fleet_registry_loop_failed']
    assert 'loop bug' in failure['error']
