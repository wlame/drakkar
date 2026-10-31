"""The same-name check at startup: a live worker of another process with this worker's name refuses the start."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from structlog.testing import capture_logs

from drakkar.fleet.card import WorkerIdentity, build_card
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.fleet.service import FleetRegistry, WorkerNameInUseError
from drakkar.providers.resilience import ResilientCaller
from tests.fleet_fakes import MemoryRegistry, MemoryStore
from tests.test_fleet_service import OWN, _Clock, _config, _never_opening_breaker, _no_backoff
from tests.test_fleet_wiring import _app

HEARTBEAT = 10
TWIN = WorkerIdentity(
    worker=OWN.worker,
    instance='9999999999999999',
    cluster=OWN.cluster,
    url='http://10.0.0.9:8080/',
    backend='python',
    version='9.9.9',
    started_at=datetime(2026, 11, 8, 9, 0, tzinfo=UTC),
)


def _twin_card(second: int) -> dict:
    return build_card(TWIN, state='running', heartbeat_at=datetime(2026, 11, 8, 10, 0, second, tzinfo=UTC))


class _Pauses:
    """The probe's sleep: records each pause, moves the clocks, and runs what the test asks for meanwhile."""

    def __init__(self, clock: _Clock, store: MemoryStore | None = None, meanwhile=None) -> None:
        self.clock = clock
        self.store = store
        self.meanwhile = meanwhile
        self.seconds: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.seconds.append(seconds)
        self.clock.advance(seconds)
        if self.store is not None:
            self.store.advance(seconds)
        if self.meanwhile is not None:
            self.meanwhile()


class _Recording(MemoryRegistry):
    def __init__(self, store: MemoryStore, **kwargs) -> None:
        super().__init__(store, **kwargs)
        self.calls: list[str] = []

    async def start(self, ctx: RegistryContext) -> None:
        self.calls.append('start')
        await super().start(ctx)

    async def publish(self, card: dict) -> None:
        self.calls.append('publish')
        await super().publish(card)

    async def list(self) -> PeerCardList:
        self.calls.append('list')
        return await super().list()

    async def withdraw(self, worker: str, instance: str) -> None:
        self.calls.append('withdraw')
        await super().withdraw(worker, instance)

    async def stop(self) -> None:
        self.calls.append('stop')
        await super().stop()


class _FailingOn(_Recording):
    """Raises on ``start``, or on the n-th ``list`` (counted from 1)."""

    def __init__(self, store: MemoryStore, *, fail_start: bool = False, fail_list: int = 0) -> None:
        super().__init__(store)
        self._fail_start = fail_start
        self._fail_list = fail_list
        self._lists = 0

    async def start(self, ctx: RegistryContext) -> None:
        if self._fail_start:
            raise ConnectionError('registry service unreachable')
        await super().start(ctx)

    async def list(self) -> PeerCardList:
        self._lists += 1
        if self._lists == self._fail_list:
            raise OSError('registry storage unavailable')
        return await super().list()


class _Fixed:
    """A provider whose ``list`` answers the cards the test gives, in turn."""

    def __init__(self, *listings: PeerCardList) -> None:
        self._listings = list(listings)

    async def start(self, ctx: RegistryContext) -> None:
        return None

    async def publish(self, card: dict) -> None:
        return None

    async def list(self) -> PeerCardList:
        return self._listings.pop(0)

    async def withdraw(self, worker: str, instance: str) -> None:
        return None

    async def stop(self) -> None:
        return None


def _service(provider, clock: _Clock, pauses: _Pauses, resilience=None) -> FleetRegistry:
    config = _config(heartbeat=HEARTBEAT, **({'resilience': resilience} if resilience else {}))
    return FleetRegistry(
        provider,
        provider_name='memory',
        config=config,
        identity=OWN,
        now_monotonic=lambda: clock.monotonic,
        now_epoch=lambda: clock.epoch,
        caller=ResilientCaller(
            config.resilience,
            feature='registry',
            provider='memory',
            now_monotonic=lambda: clock.monotonic,
            sleep=_no_backoff,
        ),
        sleep=pauses,
    )


def _events(logs: list[dict], name: str) -> list[dict]:
    return [entry for entry in logs if entry['event'] == name]


async def test_check_name_free_no_card_of_the_name_continues_without_waiting():
    clock, store = _Clock(), MemoryStore()
    provider = _Recording(store)
    pauses = _Pauses(clock, store)
    service = _service(provider, clock, pauses)

    with capture_logs() as logs:
        await service.check_name_free()

    assert pauses.seconds == []
    assert provider.calls == ['start', 'list']
    assert not _events(logs, 'fleet_worker_name_probe_waiting')


async def test_check_name_free_card_older_than_two_heartbeats_continues_without_waiting():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    store.advance(2 * HEARTBEAT)
    pauses = _Pauses(clock, store)
    service = _service(_Recording(store), clock, pauses)

    await service.check_name_free()

    assert pauses.seconds == []


async def test_check_name_free_card_offline_by_its_provider_continues_without_waiting():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    store.advance(5)
    pauses = _Pauses(clock, store)
    service = _service(_Recording(store, offline_after_seconds=1), clock, pauses)

    await service.check_name_free()

    assert pauses.seconds == []


async def test_check_name_free_card_that_moves_on_refuses_the_start_before_any_publish():
    clock, store = _Clock(), MemoryStore()
    twin = MemoryRegistry(store)
    await twin.publish(_twin_card(0))
    provider = _Recording(store)

    def twin_publishes_again() -> None:
        store.cards[OWN.worker] = (_twin_card(10), store.now)

    pauses = _Pauses(clock, store, meanwhile=twin_publishes_again)
    service = _service(provider, clock, pauses)

    with capture_logs() as logs, pytest.raises(WorkerNameInUseError) as refused:
        await service.check_name_free()

    assert str(refused.value) == (
        f"worker name 'w-1' is used by a live worker (instance {TWIN.instance}, {TWIN.url}); "
        'give each worker a unique name'
    )
    assert pauses.seconds == [HEARTBEAT + 2.0]
    assert 'publish' not in provider.calls
    (waiting,) = _events(logs, 'fleet_worker_name_probe_waiting')
    assert waiting['log_level'] == 'info'
    assert waiting['other_instances'] == [TWIN.instance]
    assert waiting['wait_seconds'] == HEARTBEAT + 2.0
    (in_use,) = _events(logs, 'fleet_worker_name_in_use')
    assert in_use['log_level'] == 'error'
    assert in_use['category'] == 'fleet'
    assert (in_use['worker'], in_use['other_instance'], in_use['other_url']) == ('w-1', TWIN.instance, TWIN.url)


async def test_check_name_free_card_that_stays_the_same_continues_after_one_wait():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    pauses = _Pauses(clock, store)
    service = _service(_Recording(store), clock, pauses)

    with capture_logs() as logs:
        await service.check_name_free()

    assert pauses.seconds == [HEARTBEAT + 2.0]
    assert not _events(logs, 'fleet_worker_name_in_use')


async def test_check_name_free_card_withdrawn_during_the_wait_continues():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    pauses = _Pauses(clock, store, meanwhile=lambda: store.cards.clear())
    service = _service(_Recording(store), clock, pauses)

    await service.check_name_free()

    assert pauses.seconds == [HEARTBEAT + 2.0]


async def test_check_name_free_card_with_no_age_waits_then_judges_by_its_heartbeat():
    clock = _Clock()
    pauses = _Pauses(clock)
    provider = _Fixed(
        [PeerCard(card=_twin_card(0), age_seconds=None)], [PeerCard(card=_twin_card(10), age_seconds=None)]
    )
    service = _service(provider, clock, pauses)

    with pytest.raises(WorkerNameInUseError):
        await service.check_name_free()

    assert pauses.seconds == [HEARTBEAT + 2.0]


async def test_check_name_free_ignores_invalid_cards_and_cards_of_other_names():
    clock = _Clock()
    pauses = _Pauses(clock)
    other = build_card(
        WorkerIdentity(**{**TWIN.__dict__, 'worker': 'w-2'}),
        state='running',
        heartbeat_at=datetime(2026, 11, 8, tzinfo=UTC),
    )
    listing = [
        PeerCard(card={'worker': OWN.worker, 'instance': TWIN.instance}, age_seconds=0.0),
        PeerCard(card='not a card', age_seconds=0.0),
        PeerCard(card=other, age_seconds=0.0),
        PeerCard(card={**_twin_card(0), 'instance': OWN.instance}, age_seconds=0.0),
    ]
    service = _service(_Fixed(listing), clock, pauses)

    await service.check_name_free()

    assert pauses.seconds == []


@pytest.mark.parametrize(
    ('provider_kwargs', 'error'),
    [({'fail_start': True}, 'start did not succeed'), ({'fail_list': 1}, 'list error')],
    ids=['start-fails', 'first-list-fails'],
)
async def test_check_name_free_provider_failure_skips_the_check(provider_kwargs, error):
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    pauses = _Pauses(clock, store)
    service = _service(_FailingOn(store, **provider_kwargs), clock, pauses, resilience=_never_opening_breaker())

    with capture_logs() as logs:
        await service.check_name_free()

    (skipped,) = _events(logs, 'fleet_worker_name_probe_skipped')
    assert skipped['log_level'] == 'warning'
    assert skipped['error'] == error
    assert skipped['provider'] == 'memory'
    assert pauses.seconds == []


async def test_check_name_free_second_list_failure_skips_the_check():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    pauses = _Pauses(clock, store)
    service = _service(_FailingOn(store, fail_list=2), clock, pauses, resilience=_never_opening_breaker())

    with capture_logs() as logs:
        await service.check_name_free()

    (skipped,) = _events(logs, 'fleet_worker_name_probe_skipped')
    assert skipped['error'] == 'list error'


async def test_check_name_free_spent_bound_skips_the_second_list():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    pauses = _Pauses(clock, store, meanwhile=lambda: clock.advance(3 * HEARTBEAT))
    service = _service(_Recording(store), clock, pauses)

    with capture_logs() as logs:
        await service.check_name_free()

    (skipped,) = _events(logs, 'fleet_worker_name_probe_skipped')
    assert skipped['error'] == 'list timeout'


async def test_stop_after_a_refused_start_stops_the_provider_without_a_withdraw():
    clock, store = _Clock(), MemoryStore()
    await MemoryRegistry(store).publish(_twin_card(0))
    provider = _Recording(store)
    pauses = _Pauses(clock, store, meanwhile=lambda: store.cards.update({OWN.worker: (_twin_card(10), store.now)}))
    service = _service(provider, clock, pauses)
    with pytest.raises(WorkerNameInUseError):
        await service.check_name_free()

    await service.stop()
    await service.stop()

    assert provider.calls[-1] == 'stop'
    assert 'withdraw' not in provider.calls
    assert provider.calls.count('stop') == 1


async def test_start_fleet_runs_the_name_check_before_the_loops(tmp_path, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(FleetRegistry, 'check_name_free', AsyncMock(side_effect=lambda: calls.append('check')))
    monkeypatch.setattr(FleetRegistry, 'start', AsyncMock(side_effect=lambda: calls.append('start')))
    app = _app(tmp_path)

    await app._lifecycle._start_fleet()

    assert calls == ['check', 'start']
