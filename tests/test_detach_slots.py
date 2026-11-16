"""The budget of detached workers: lease slots in the election store, taken through a caller of their own.

Time is a virtual clock: a sleep wakes only when nothing else on the event
loop can run, so every time in an assertion is exact. The log lines of the
module go to an in-memory logger for the same reason.
"""

import asyncio
import contextlib
import hashlib
from typing import Any

import pytest

from drakkar import cluster_tasks, detach_slots
from drakkar.cluster_tasks import ClusterElection
from drakkar.config.resilience import ResilienceConfig
from drakkar.detach_slots import (
    DETACH_FEATURE,
    LOSS_EXPIRED,
    LOSS_TAKEN_OVER,
    SLOT_ACQUIRE_SECONDS,
    DetachSlot,
    SlotAcquire,
    group_key,
    slot_scope,
)
from drakkar.recorder.election import ELECTION_FEATURE, ElectionContext
from drakkar.recorder.lease import LeaseHolder
from drakkar.schedule import parse_task_scope
from tests.clocks import VirtualClock
from tests.fleet_fakes import RecordingLogger, ScriptedElection, SpyCaller

CLUSTER = 'search-fleet'
GROUP = 'my-group'
PROVIDER = 'scripted'
NOW = 1_800_000_000.0
LEASE = 60
# A third of the lease: the refresh interval of a 60 s lease.
REFRESH = 20.0
CONTEXT = ElectionContext(
    worker='w-1', instance='0123456789abcdef', cluster=CLUSTER, shared_dir='', logger=None, options={}
)
SCOPES = [slot_scope(CLUSTER, GROUP, index) for index in range(2)]


async def _no_sleep(seconds: float) -> None:
    return None


@pytest.fixture
def logs(monkeypatch) -> RecordingLogger:
    recording = RecordingLogger()
    monkeypatch.setattr(detach_slots, 'logger', recording)
    monkeypatch.setattr(cluster_tasks, 'logger', recording)
    return recording


def make_caller(feature: str, **resilience: Any) -> SpyCaller:
    """A caller with no retries by default, so one provider error fails one whole operation."""
    return SpyCaller(
        ResilienceConfig(**{'max_retries': 0, **resilience}),
        feature=feature,
        provider=PROVIDER,
        category='kafka' if feature == DETACH_FEATURE else 'periodic',
        logger=RecordingLogger(),
        sleep=_no_sleep,
    )


def make_election(provider: Any, clock: VirtualClock) -> ClusterElection:
    return ClusterElection(
        provider,
        make_caller(ELECTION_FEATURE),
        provider_name=PROVIDER,
        context=CONTEXT,
        clock=clock,
    )


def make_slot(
    provider: Any,
    clock: VirtualClock,
    *,
    caller: SpyCaller | None = None,
    scopes: list[str] = SCOPES,
    election: ClusterElection | None = None,
) -> DetachSlot:
    return DetachSlot(
        election if election is not None else make_election(provider, clock),
        caller if caller is not None else make_caller(DETACH_FEATURE),
        scopes=scopes,
        lease_seconds=LEASE,
        clock=clock,
    )


def hold_elsewhere(provider: ScriptedElection, scope: str) -> None:
    """Let another worker's token hold ``scope`` for longer than any test runs."""
    provider.store.leases[scope] = ('f' * 16, provider.store.now + 86_400, 86_400)


async def run_clock_until(clock: VirtualClock, until: float) -> None:
    """Let the event loop and the clock run until the clock would pass ``until``."""
    clock.parked.clear()
    clock.limit = until
    # A sleeper keeps the clock moving to ``until`` even when nothing else sleeps.
    keeper = asyncio.ensure_future(clock.sleep(until - clock.time()))
    await asyncio.wait_for(clock.parked.wait(), timeout=5)
    keeper.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await keeper


class LossRecorder:
    """The ``on_lost`` callback: keeps each reason with the clock time it came."""

    def __init__(self, clock: VirtualClock) -> None:
        self._clock = clock
        self.reports: list[tuple[str, float]] = []

    async def __call__(self, reason: str) -> None:
        self.reports.append((reason, self._clock.time()))


# --- scopes ---


def test_slot_scope_fits_the_scope_pattern():
    scope = slot_scope('main', 'my-group', 0)

    assert scope == 'main/detach.' + 'g' + hashlib.sha256(b'my-group').hexdigest()[:12] + '_0'
    assert parse_task_scope(scope) == ('main', scope.split('/', 1)[1])


def test_group_key_is_the_same_for_the_same_group_and_differs_for_another():
    assert group_key('my-group') == group_key('my-group')
    assert group_key('my-group') != group_key('other-group')
    assert len(group_key('my-group')) == 13


# --- acquire ---


async def test_acquire_takes_the_first_free_slot(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    hold_elsewhere(provider, SCOPES[0])
    slot = make_slot(provider, clock)

    assert await slot.acquire() is SlotAcquire.WON

    assert slot.index == 1
    assert provider.store.holder(SCOPES[1]) is not None
    assert provider.store.holder(SCOPES[1]) != provider.store.holder(SCOPES[0])
    assert slot.seconds_left == LEASE


async def test_acquire_answers_budget_full_when_every_slot_is_held(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    for scope in SCOPES:
        hold_elsewhere(provider, scope)
    slot = make_slot(provider, clock)

    assert await slot.acquire() is SlotAcquire.BUDGET_FULL

    assert slot.index is None
    assert slot.seconds_left == 0.0
    assert provider.calls['release'] == 0


async def test_acquire_answers_store_unavailable_when_an_acquire_fails_and_none_wins(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(OSError('store down'),))
    hold_elsewhere(provider, SCOPES[1])
    slot = make_slot(provider, clock)

    assert await slot.acquire() is SlotAcquire.STORE_UNAVAILABLE

    assert slot.index is None
    assert provider.calls['acquire'] == 2


async def test_acquire_answers_store_unavailable_when_the_provider_cannot_start(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, start_error=OSError('no store'))
    slot = make_slot(provider, clock)

    assert await slot.acquire() is SlotAcquire.STORE_UNAVAILABLE

    assert provider.calls['acquire'] == 0


async def test_acquire_gives_each_call_its_bound(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    hold_elsewhere(provider, SCOPES[0])
    caller = make_caller(DETACH_FEATURE)
    slot = make_slot(provider, clock, caller=caller)

    await slot.acquire()

    assert caller.deadlines == [
        ('start', SLOT_ACQUIRE_SECONDS),
        ('acquire', SLOT_ACQUIRE_SECONDS),
        ('acquire', SLOT_ACQUIRE_SECONDS),
    ]


async def test_acquire_while_a_slot_is_held_raises(logs):
    clock = VirtualClock(NOW)
    slot = make_slot(ScriptedElection(clock), clock)
    await slot.acquire()

    with pytest.raises(RuntimeError, match='already holds'):
        await slot.acquire()


class WritesThenFails(ScriptedElection):
    """An acquire that takes the lease at the store and then fails, as a lost answer does."""

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        await super().acquire(scope, token, lease_seconds)
        raise OSError('the answer was lost')


async def test_a_failed_acquire_after_an_attempt_is_released(logs):
    clock = VirtualClock(NOW)
    provider = WritesThenFails(clock)
    slot = make_slot(provider, clock, scopes=SCOPES[:1])

    assert await slot.acquire() is SlotAcquire.STORE_UNAVAILABLE

    assert provider.calls['release'] == 1
    assert provider.store.holder(SCOPES[0]) is None


async def test_an_acquire_refused_by_an_open_breaker_sends_no_release(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(OSError('store down'),))
    caller = make_caller(DETACH_FEATURE, breaker_failures=1)
    slot = make_slot(provider, clock, caller=caller)

    assert await slot.acquire() is SlotAcquire.STORE_UNAVAILABLE

    # The first acquire made an attempt and is released; the second made none.
    assert [op for op, _ in caller.deadlines] == ['start', 'acquire', 'release', 'acquire']
    assert provider.calls['acquire'] == 1


class HangingAcquire(ScriptedElection):
    """An acquire that takes the lease at the store and then does not answer until it is cancelled."""

    def __init__(self, clock: VirtualClock) -> None:
        super().__init__(clock)
        self.taken = asyncio.Event()

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        await super().acquire(scope, token, lease_seconds)
        self.taken.set()
        await asyncio.Event().wait()
        return True


async def test_a_cancelled_acquire_leaves_no_slot_held(logs):
    clock = VirtualClock(NOW)
    provider = HangingAcquire(clock)
    slot = make_slot(provider, clock)
    acquiring = asyncio.ensure_future(slot.acquire())
    await asyncio.wait_for(provider.taken.wait(), timeout=5)

    acquiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquiring
    for _ in range(20):
        await asyncio.sleep(0)

    assert slot.index is None
    assert provider.calls['release'] == 1
    assert provider.store.holder(SCOPES[0]) is None


# --- held_count ---


async def test_held_count_counts_own_and_other_slots(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    scopes = [slot_scope(CLUSTER, GROUP, index) for index in range(3)]
    hold_elsewhere(provider, scopes[0])
    slot = make_slot(provider, clock, scopes=scopes)
    await slot.acquire()

    assert await slot.held_count() == 2
    assert provider.calls['holder'] == 3


class FailingHolder(ScriptedElection):
    """A provider whose holder read of one scope fails."""

    def __init__(self, clock: VirtualClock, failing_scope: str) -> None:
        super().__init__(clock)
        self._failing_scope = failing_scope

    async def holder(self, scope: str) -> LeaseHolder | None:
        if scope == self._failing_scope:
            raise OSError('read failed')
        return await super().holder(scope)


async def test_held_count_counts_a_failed_read_as_held(logs):
    clock = VirtualClock(NOW)
    provider = FailingHolder(clock, failing_scope=SCOPES[1])
    caller = make_caller(DETACH_FEATURE)
    slot = make_slot(provider, clock, caller=caller)
    await slot.acquire()

    assert await slot.held_count() == 2
    assert ('holder', SLOT_ACQUIRE_SECONDS) in caller.deadlines


# --- refresh ---


async def test_refresh_keeps_the_lease(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    slot = make_slot(provider, clock)
    await slot.acquire()
    token = provider.store.holder(SCOPES[0])
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 5 * REFRESH + 1)

    assert provider.at['refresh'] == [NOW + REFRESH * step for step in range(1, 6)]
    assert provider.store.holder(SCOPES[0]) == token
    assert lost.reports == []
    assert slot.seconds_left == LEASE - 1
    slot.stop_refreshing()


async def test_one_failed_refresh_does_not_report_a_loss(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=(OSError('store down'),))
    slot = make_slot(provider, clock)
    await slot.acquire()
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 2 * REFRESH)

    assert provider.at['refresh'] == [NOW + REFRESH, NOW + 2 * REFRESH]
    assert lost.reports == []
    assert slot.seconds_left == LEASE
    unconfirmed = logs.named('detach_lease_unconfirmed')
    assert unconfirmed == [
        {
            'event': 'detach_lease_unconfirmed',
            'log_level': 'warning',
            'category': 'kafka',
            'scope': SCOPES[0],
            'seconds_left': LEASE - REFRESH,
        }
    ]
    slot.stop_refreshing()


async def test_unconfirmed_refreshes_report_expired_at_the_lease_end(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=tuple(OSError('store down') for _ in range(10)))
    slot = make_slot(provider, clock)
    await slot.acquire()
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 3 * LEASE)

    assert lost.reports == [(LOSS_EXPIRED, NOW + LEASE)]
    # Tries at each refresh time; the lease end itself makes no call.
    assert provider.at['refresh'] == [NOW + REFRESH, NOW + 2 * REFRESH]
    assert slot.seconds_left == 0.0
    assert len(logs.named('detach_lease_unconfirmed')) == 2


async def test_an_open_breaker_does_not_make_the_refresher_spin(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=(OSError('store down'),))
    caller = make_caller(DETACH_FEATURE, breaker_failures=1)
    slot = make_slot(provider, clock, caller=caller)
    await slot.acquire()
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 3 * LEASE)

    assert [op for op, _ in caller.deadlines].count('refresh') == 2
    assert provider.calls['refresh'] == 1
    assert lost.reports == [(LOSS_EXPIRED, NOW + LEASE)]


async def test_taken_over_reports_at_once_and_sends_no_release(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    slot = make_slot(provider, clock)
    await slot.acquire()
    hold_elsewhere(provider, SCOPES[0])
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 3 * LEASE)
    await slot.release()

    assert lost.reports == [(LOSS_TAKEN_OVER, NOW + REFRESH)]
    assert provider.calls['refresh'] == 1
    assert provider.calls['release'] == 0
    assert provider.store.holder(SCOPES[0]) == 'f' * 16
    assert slot.index is None


async def test_release_frees_the_slot_and_stops_the_refresher(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    slot = make_slot(provider, clock)
    await slot.acquire()
    slot.start_refreshing(LossRecorder(clock))

    await slot.release()
    await run_clock_until(clock, NOW + 3 * LEASE)

    assert provider.store.holder(SCOPES[0]) is None
    assert provider.calls['release'] == 1
    assert provider.calls['refresh'] == 0
    assert slot.index is None
    assert slot.seconds_left == 0.0


async def test_release_after_an_expired_lease_still_sends_a_release(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=tuple(OSError('store down') for _ in range(10)))
    slot = make_slot(provider, clock)
    await slot.acquire()
    slot.start_refreshing(LossRecorder(clock))
    await run_clock_until(clock, NOW + 2 * LEASE)

    await slot.release()

    assert provider.calls['release'] == 1


async def test_release_with_no_slot_does_nothing(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    slot = make_slot(provider, clock)

    await slot.release()
    slot.stop_refreshing()

    assert provider.calls['release'] == 0


async def test_release_from_inside_on_lost_does_not_wait_for_itself(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=tuple(OSError('store down') for _ in range(10)))
    slot = make_slot(provider, clock)
    await slot.acquire()
    released: list[float] = []

    async def attach_on_loss(reason: str) -> None:
        await slot.release()
        released.append(clock.time())

    slot.start_refreshing(attach_on_loss)
    await run_clock_until(clock, NOW + 2 * LEASE)

    assert released == [NOW + LEASE]
    assert provider.calls['release'] == 1


class RefreshRaises(SpyCaller):
    """A caller whose refresh raises, as an unexpected error inside the refresher would."""

    async def call(self, op: str, make_call: Any, **kwargs: Any) -> Any:
        if op == 'refresh':
            raise RuntimeError('broken refresher')
        return await super().call(op, make_call, **kwargs)


async def test_a_refresher_that_breaks_logs_and_reports_expired(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    caller = RefreshRaises(
        ResilienceConfig(max_retries=0),
        feature=DETACH_FEATURE,
        provider=PROVIDER,
        category='kafka',
        logger=RecordingLogger(),
        sleep=_no_sleep,
    )
    slot = make_slot(provider, clock, caller=caller)
    await slot.acquire()
    lost = LossRecorder(clock)

    slot.start_refreshing(lost)
    await run_clock_until(clock, NOW + 2 * LEASE)

    assert lost.reports == [(LOSS_EXPIRED, NOW + REFRESH)]
    [failed] = logs.named('detach_lease_refresh_failed')
    assert (failed['log_level'], failed['category'], failed['scope']) == ('error', 'kafka', SCOPES[0])


async def test_an_on_lost_that_raises_is_logged(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    slot = make_slot(provider, clock)
    await slot.acquire()
    hold_elsewhere(provider, SCOPES[0])

    async def broken(reason: str) -> None:
        raise RuntimeError('broken callback')

    slot.start_refreshing(broken)
    await run_clock_until(clock, NOW + 2 * LEASE)

    [failed] = logs.named('detach_lease_loss_report_failed')
    assert (failed['log_level'], failed['reason']) == ('error', LOSS_TAKEN_OVER)


# --- the callers ---


async def test_detach_calls_do_not_count_on_the_cluster_tasks_breaker(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(OSError('down'), OSError('down')))
    election = make_election(provider, clock)
    caller = make_caller(DETACH_FEATURE, breaker_failures=1)
    slot = make_slot(provider, clock, caller=caller, election=election)

    assert await slot.acquire() is SlotAcquire.STORE_UNAVAILABLE

    assert caller.breaker_open
    assert not election.caller.breaker_open
    assert election.caller.deadlines == []


async def test_a_failed_start_through_the_detach_caller_does_not_feed_the_cluster_tasks_breaker(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, start_error=OSError('no store'))
    election = make_election(provider, clock)
    caller = make_caller(DETACH_FEATURE, breaker_failures=1)
    slot = make_slot(provider, clock, caller=caller, election=election)

    await slot.acquire()

    assert caller.breaker_open
    assert not election.caller.breaker_open
    assert not election.started


async def test_start_refreshing_with_no_slot_raises(logs):
    clock = VirtualClock(NOW)
    slot = make_slot(ScriptedElection(clock), clock)

    with pytest.raises(RuntimeError, match='no detach slot'):
        slot.start_refreshing(LossRecorder(clock))
