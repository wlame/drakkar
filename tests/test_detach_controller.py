"""The detach controller: refusals, the detach and attach steps, the origin rules and the safety rules.

Time is a virtual clock that moves only when a test lets it (``run_clock_until``),
so deadlines and lease ends are exact. The Kafka source is a subclass whose
leave and join are scripted; the registry is a small fake that serves peer
views and counts card publishes; the slots live in an in-memory election store.
"""

import asyncio
import contextlib
import time
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar import cluster_tasks, detach_slots
from drakkar import detach as detach_module
from drakkar.app import DrakkarApp
from drakkar.cluster_tasks import ClusterElection
from drakkar.config import DrakkarConfig
from drakkar.detach import (
    MIN_DURATION_SECONDS,
    REASON_MAX_CHARS,
    AttachResult,
    DetachController,
    DetachOrigin,
    DetachResult,
    check_duration,
)
from drakkar.detach_slots import DetachSlot, slot_scope
from drakkar.fleet.service import PeerView
from drakkar.metrics import attach_total, detach_requests, detached
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder.election import ElectionContext
from drakkar.sources.kafka import KafkaSource, LeaveOutcome
from drakkar.timefmt import format_rfc3339_micro
from tests.clocks import VirtualClock
from tests.conftest import kafka_connections, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection, RecordingLogger, ScriptedElection, SpyCaller
from tests.test_sources_validation import KafkaOnlyHandler

NOW = 1_800_000_000.0
CLUSTER = 'search'
GROUP = 'matcher'
LEASE = 60
HANG = 'hang'


class FakeSource(KafkaSource):
    """A Kafka source whose leave and join are scripted; ``calls`` records each one."""

    def __init__(self, app: DrakkarApp, calls: list[str]) -> None:
        super().__init__(app.consume_pause)
        self._ready = True
        self.calls = calls
        self.leave_outcomes: list[LeaveOutcome] = []
        self.join_answers: list[Any] = []
        self.leave_gate: asyncio.Event | None = None

    async def leave_group(self) -> LeaveOutcome:
        self.calls.append('leave_group')
        self._group_member = False
        if self.leave_gate is not None:
            await self.leave_gate.wait()
        return self.leave_outcomes.pop(0) if self.leave_outcomes else LeaveOutcome.LEFT

    async def join_group(self) -> bool:
        self.calls.append('join_group')
        answer = self.join_answers.pop(0) if self.join_answers else True
        if answer == HANG:
            await asyncio.Event().wait()
        if isinstance(answer, Exception):
            raise answer
        if answer:
            self._group_member = True
        return answer


class FakeFleet:
    """Serves the peer views a test sets and records each read and each card publish."""

    def __init__(self, calls: list[str]) -> None:
        self.views: list[PeerView] = []
        self.calls = calls

    def peer_views(self) -> list[PeerView]:
        self.calls.append('size_check')
        return list(self.views)

    def publish_soon(self) -> None:
        self.calls.append('publish')


class ListedElection(MemoryElection):
    """A memory election that records its acquire, holder and release calls in a shared list."""

    def __init__(self, store: ElectionStore, calls: list[str]) -> None:
        super().__init__(store)
        self.listed = calls

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.listed.append('acquire')
        return await super().acquire(scope, token, lease_seconds)

    async def holder(self, scope: str):
        self.listed.append('holder')
        return await super().holder(scope)

    async def release(self, scope: str, token: str) -> None:
        self.listed.append('release')
        await super().release(scope, token)


class BarrierElection(MemoryElection):
    """Holds each won acquire until ``parties`` acquires were won on the shared barrier."""

    def __init__(self, store: ElectionStore, barrier: dict[str, Any]) -> None:
        super().__init__(store)
        self._barrier = barrier

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        won = await super().acquire(scope, token, lease_seconds)
        if won:
            self._barrier['won'] += 1
            if self._barrier['won'] >= self._barrier['parties']:
                self._barrier['event'].set()
            await self._barrier['event'].wait()
        return won


def peer(worker: str = 'w-2', *, detached_flag: Any = False, **extra: Any) -> PeerView:
    card = {
        'schema': 1,
        'worker': worker,
        'cluster': CLUSTER,
        'url': f'http://{worker}:8080/',
        'features': {'kafka': {'group': GROUP, 'detached': detached_flag, **extra}},
    }
    return PeerView(card=card, age_seconds=1.0, online=True, last_seen_ts=None)


async def run_clock_until(clock: VirtualClock, until: float) -> None:
    """Let the event loop and the clock run until the clock would pass ``until``."""
    clock.parked.clear()
    clock.limit = until
    keeper = asyncio.ensure_future(clock.sleep(until - clock.time()))
    await asyncio.wait_for(clock.parked.wait(), timeout=5)
    keeper.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await keeper


def counted(origin: str, outcome: str) -> float:
    return detach_requests.labels(origin=origin, outcome=outcome)._value.get()


def attached_by(cause: str) -> float:
    return attach_total.labels(cause=cause)._value.get()


class Harness:
    """One worker: its app, the controller, the fake source, registry and election, and the recorded calls."""

    def __init__(self, tmp_path, clock: VirtualClock, *, worker: str = 'w-1', provider: Any = None, **detach: Any):
        self.calls: list[str] = []
        values = {
            'cluster': {'name': CLUSTER},
            'autodiscover': {'enabled': True, 'provider': 'filesystem', 'resilience': {'max_retries': 0}},
            'storage': {'dir': str(tmp_path / 'storage'), 'shared_dir': str(tmp_path / 'shared')},
            'ui': {'enabled': True, 'release': {'enabled': False}},
            'metrics': {'enabled': False},
            'runtime_health': {'enabled': False},
            'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout': '1s'},
            'sources': {
                'kafka': {
                    'enabled': True,
                    'consumer_group': GROUP,
                    'startup_align': {'enabled': False},
                    'detach': {'enabled': True, 'lease': '1m', 'cooldown': '5m', **detach},
                }
            },
            'connections': kafka_connections(),
        }
        self.app = DrakkarApp(handler=KafkaOnlyHandler(), config=DrakkarConfig(**values), worker_id=worker)
        self.source = FakeSource(self.app, self.calls)
        self.app.sources = [self.source]
        self.fleet = FakeFleet(self.calls)
        self.app._fleet = self.fleet
        self.store = ElectionStore()
        self.provider = provider if provider is not None else ListedElection(self.store, self.calls)
        context = ElectionContext(
            worker=worker, instance='0123456789abcdef', cluster=CLUSTER, shared_dir='', logger=None, options={}
        )
        self.app._cluster_election = ClusterElection(
            self.provider,
            SpyCaller(
                self.app.config.autodiscover.resilience,
                feature='election',
                provider='memory',
                logger=RecordingLogger(),
            ),
            provider_name='memory',
            context=context,
            clock=clock,
        )
        self.recorder = MagicMock()
        self.app._recorder = self.recorder
        self.controller = DetachController(self.app, clock=clock)
        # The caller logs to memory: a real async log line waits on a thread, and the clock moves meanwhile.
        self.controller._caller = SpyCaller(
            self.app.config.autodiscover.resilience,
            feature='detach',
            provider='memory',
            category='kafka',
            logger=RecordingLogger(),
        )
        self.app._detach = self.controller

    @property
    def scopes(self) -> list[str]:
        return [slot_scope(CLUSTER, GROUP, index) for index in range(self.app.config.sources.kafka.detach.max_detached)]

    def held_scopes(self) -> list[str]:
        store = self.provider.store if isinstance(self.provider, ScriptedElection) else self.store
        return [scope for scope in self.scopes if store.holder(scope) is not None]

    async def detach(self, origin: DetachOrigin = DetachOrigin.HANDLER, **kwargs: Any) -> DetachResult:
        return await self.controller.detach(
            origin=origin, duration=kwargs.pop('duration', 3600), reason=kwargs.pop('reason', 'slow disk'), **kwargs
        )


@pytest.fixture
def clock() -> VirtualClock:
    clock = VirtualClock(NOW)
    # Time moves only when a test runs the clock.
    clock.limit = NOW
    return clock


@pytest.fixture(autouse=True)
def short_join_error_pause(monkeypatch) -> None:
    # The pause after an error in the join retry is real time, not clock time.
    monkeypatch.setattr(detach_module, 'JOIN_ERROR_PAUSE_SECONDS', 0.01, raising=False)


@pytest.fixture
def logs(monkeypatch) -> RecordingLogger:
    recording = RecordingLogger()
    monkeypatch.setattr(detach_module, 'logger', recording)
    monkeypatch.setattr(detach_slots, 'logger', recording)
    monkeypatch.setattr(cluster_tasks, 'logger', recording)
    return recording


@pytest.fixture
async def make(tmp_path, clock, logs):
    made: list[Harness] = []

    def build(**kwargs: Any) -> Harness:
        harness = Harness(tmp_path / f'w{len(made)}', clock, worker=f'w-{len(made) + 1}', **kwargs)
        made.append(harness)
        return harness

    yield build
    for harness in made:
        await harness.controller.release_at_stop(1.0)
    detached.set(0)


@pytest.fixture
def worker(make) -> Harness:
    harness = make()
    harness.fleet.views = [peer()]
    return harness


# --- refusals ------------------------------------------------------------------


async def test_detach_before_the_first_poll_answers_not_running(worker):
    worker.source._ready = False

    result = await worker.detach()

    assert result == DetachResult(detached=False, reason='not_running', until=None)


async def test_detach_after_a_stop_request_answers_stopping(worker, logs):
    before = counted('handler', 'stopping')
    worker.source._stop_requested = True

    result = await worker.detach()

    assert result.reason == 'stopping'
    assert counted('handler', 'stopping') == before + 1
    assert logs.named('detach_refused')[0]['reason'] == 'stopping'


@pytest.mark.parametrize('in_pipeline_hook', [False, True])
async def test_detach_with_the_switch_off_answers_disabled(worker, in_pipeline_hook):
    worker.app.config.sources.kafka.detach.enabled = False

    result = await worker.detach(in_pipeline_hook=in_pipeline_hook)

    assert result.reason == 'disabled'


async def test_detach_with_no_kafka_source_answers_disabled(worker):
    worker.app.sources = []

    result = await worker.detach(in_pipeline_hook=True)

    assert result.reason == 'disabled'


async def test_detach_inside_a_pipeline_hook_is_refused_with_no_store_call(worker):
    before = counted('handler', 'inside_pipeline_hook')

    result = await worker.detach(in_pipeline_hook=True)

    assert result.reason == 'inside_pipeline_hook'
    assert sum(worker.provider.calls.values()) == 0
    assert counted('handler', 'inside_pipeline_hook') == before + 1


async def test_a_handler_detach_inside_the_cooldown_is_refused_and_an_operator_detach_is_not(worker, clock):
    assert (await worker.detach()).detached
    assert (await worker.controller.attach(cause='handler')).attached
    await run_clock_until(clock, NOW + 60)

    refused = await worker.detach()
    operator = await worker.detach(origin=DetachOrigin.OPERATOR)

    assert refused.reason == 'cooldown'
    assert operator.detached and operator.reason == ''


async def test_too_few_attached_peers_refuse_before_any_store_call(worker, logs):
    worker.fleet.views = [peer(detached_flag=True)]

    result = await worker.detach()

    assert result.reason == 'too_few_workers'
    assert sum(worker.provider.calls.values()) == 0
    assert logs.named('detach_refused')[0] == {
        'event': 'detach_refused',
        'log_level': 'info',
        'category': 'kafka',
        'origin': 'handler',
        'reason': 'too_few_workers',
    }


async def test_no_registry_counts_the_group_as_this_worker_alone(worker):
    worker.app._fleet = None

    assert (await worker.detach()).reason == 'too_few_workers'


async def test_every_slot_held_elsewhere_answers_budget_full(worker):
    worker.store.leases[worker.scopes[0]] = ('f' * 16, worker.store.now + 86_400, 86_400)

    result = await worker.detach()

    assert result.reason == 'budget_full'
    assert worker.source.calls.count('leave_group') == 0


async def test_a_failed_acquire_answers_store_unavailable(make, clock):
    harness = make(provider=ScriptedElection(clock, acquire=(RuntimeError('store down'),)))
    harness.fleet.views = [peer()]
    before = counted('operator', 'store_unavailable')

    result = await harness.detach(origin=DetachOrigin.OPERATOR)

    assert result.reason == 'store_unavailable'
    assert counted('operator', 'store_unavailable') == before + 1


async def test_a_worker_with_no_election_answers_store_unavailable(worker):
    worker.app._cluster_election = None

    assert (await worker.detach()).reason == 'store_unavailable'


# --- the detach --------------------------------------------------------------------


async def test_detach_runs_the_steps_in_order(worker, logs):
    before = counted('handler', 'detached')

    result = await worker.detach(duration=600, reason='slow disk')

    steps = [call for call in worker.calls if call in {'size_check', 'acquire', 'holder', 'leave_group', 'publish'}]
    assert steps[:3] == ['size_check', 'acquire', 'holder']
    assert steps[-2:] == ['leave_group', 'publish']
    until = datetime.fromtimestamp(NOW + 600, UTC)
    assert result == DetachResult(detached=True, reason='', until=until)
    state = worker.controller.state
    assert (state.state, state.origin, state.reason, state.until) == (
        'detached',
        DetachOrigin.HANDLER,
        'slow disk',
        until,
    )
    assert worker.controller.is_detached
    assert detached._value.get() == 1
    assert counted('handler', 'detached') == before + 1
    worker.recorder.record_worker_detached.assert_called_once_with(
        origin='handler', reason='slow disk', until=NOW + 600, slot=0
    )
    assert worker.held_scopes() == [worker.scopes[0]]
    assert logs.named('worker_detached')[0]['slot'] == 0


async def test_second_size_check_gives_the_slot_back(make):
    harness = make(max_detached=2, min_attached=1)
    harness.fleet.views = [peer()]
    harness.store.leases[harness.scopes[0]] = ('f' * 16, harness.store.now + 86_400, 86_400)

    result = await harness.detach()

    assert result.reason == 'too_few_workers'
    assert harness.store.holder(harness.scopes[1]) is None
    assert 'leave_group' not in harness.calls


async def test_two_workers_detaching_together_never_both_detach(make):
    barrier = {'won': 0, 'parties': 2, 'event': asyncio.Event()}
    store = ElectionStore()
    first = make(provider=BarrierElection(store, barrier), max_detached=2)
    second = make(provider=BarrierElection(store, barrier), max_detached=2)
    first.fleet.views = [peer('w-2')]
    second.fleet.views = [peer('w-1')]

    results = await asyncio.gather(first.detach(), second.detach())

    assert not all(result.detached for result in results)
    assert 'too_few_workers' in {result.reason for result in results}


async def test_leave_timeout_joins_again_and_releases(worker):
    worker.source.leave_outcomes = [LeaveOutcome.TIMED_OUT]

    result = await worker.detach()

    assert result.reason == 'leave_failed'
    assert worker.source.calls.count('join_group') == 1
    assert worker.held_scopes() == []
    assert worker.controller.state.state == 'attached'


async def test_stop_during_the_leave_answers_stopping_and_does_not_join(worker):
    worker.source.leave_outcomes = [LeaveOutcome.STOPPING]

    result = await worker.detach()

    assert result.reason == 'stopping'
    assert 'join_group' not in worker.calls
    assert worker.held_scopes() == []


async def test_a_leave_that_raises_joins_again_and_releases(worker, logs):
    async def broken_leave() -> LeaveOutcome:
        raise RuntimeError('unsubscribe refused')

    worker.source.leave_group = broken_leave

    result = await worker.detach()

    assert result.reason == 'leave_failed'
    assert worker.held_scopes() == []
    assert worker.controller.state.state == 'attached'
    assert logs.named('detach_transition_failed')


async def test_a_cancelled_detach_leaves_no_slot_and_no_detached_state(worker):
    worker.source.leave_gate = asyncio.Event()
    caller = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    caller.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await caller

    await wait_for(lambda: worker.controller.state.state == 'attached' and worker.held_scopes() == [])
    assert worker.calls.count('join_group') == 1


async def test_a_stop_after_the_size_check_releases_the_slot(worker):
    original = worker.provider.holder

    async def holder_then_stop(scope: str):
        worker.source._stop_requested = True
        return await original(scope)

    worker.provider.holder = holder_then_stop

    result = await worker.detach()

    assert result.reason == 'stopping'
    assert worker.held_scopes() == []


# --- the attach ----------------------------------------------------------------------


async def test_attach_order_is_join_then_release(worker, clock, logs):
    await worker.detach()
    worker.calls.clear()
    before = attached_by('handler')
    await run_clock_until(clock, NOW + 12.5)

    result = await worker.controller.attach(cause='handler', reason='better now')

    assert result == AttachResult(attached=True, reason='')
    assert worker.calls.index('join_group') < worker.calls.index('release')
    assert worker.controller.state.state == 'attached'
    assert detached._value.get() == 0
    assert attached_by('handler') == before + 1
    worker.recorder.record_worker_attached.assert_called_once_with(cause='handler', detached_seconds=12.5)
    assert logs.named('worker_attached')[0]['detached_seconds'] == 12.5
    assert worker.held_scopes() == []


async def test_attach_of_an_attached_worker_is_a_success(worker):
    assert await worker.controller.attach(cause='operator') == AttachResult(attached=True, reason='')


@pytest.mark.parametrize(
    ('ready', 'stop', 'reason'), [(False, False, 'not_running'), (True, True, 'stopping')], ids=['not_running', 'stop']
)
async def test_attach_before_the_run_or_at_a_stop_is_refused(worker, ready, stop, reason):
    worker.source._ready = ready
    worker.source._stop_requested = stop

    assert await worker.controller.attach(cause='handler') == AttachResult(attached=False, reason=reason)


async def test_a_join_that_fails_answers_join_failed_and_is_tried_again(worker, clock, logs):
    await worker.detach()
    worker.source.join_answers = [False, RuntimeError('broker gone'), HANG, True]

    result = await worker.controller.attach(cause='operator')

    assert result == AttachResult(attached=False, reason='join_failed')
    assert worker.controller.wire_state()['last_request']['outcome'] == 'join_failed'
    assert worker.controller.state.state == 'attaching'
    assert worker.held_scopes() == [worker.scopes[0]]
    await run_clock_until(clock, NOW + 40)

    assert worker.controller.state.state == 'attached'
    assert worker.calls.count('join_group') == 4
    assert len(logs.named('detach_join_failed')) == 3
    assert worker.held_scopes() == []


async def test_a_join_retry_that_succeeds_writes_the_attach_once_with_its_cause(worker, clock, logs):
    before = attached_by('operator')
    await worker.detach()
    worker.source.join_answers = [False]

    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    await run_clock_until(clock, NOW + 6)

    assert worker.controller.state.state == 'attached'
    assert attached_by('operator') == before + 1
    worker.recorder.record_worker_attached.assert_called_once_with(cause='operator', detached_seconds=5.0)
    assert len(logs.named('worker_attached')) == 1


async def test_an_attach_while_attaching_answers_at_once_and_starts_no_second_retry(worker, clock):
    await worker.detach()
    worker.source.join_answers = [False, False, True]
    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    retry = worker.controller._join_retry

    second = await worker.controller.attach(cause='handler')

    assert second == AttachResult(attached=False, reason='join_failed')
    assert worker.controller._join_retry is retry
    assert worker.calls.count('join_group') == 1
    await run_clock_until(clock, NOW + 11)
    assert worker.controller.state.state == 'attached'
    assert worker.calls.count('join_group') == 3


async def test_a_detach_while_attaching_answers_join_failed(worker):
    await worker.detach()
    worker.source.join_answers = [False]
    await worker.controller.attach(cause='operator')
    before = counted('handler', 'join_failed')

    result = await worker.detach()

    assert result == DetachResult(detached=False, reason='join_failed', until=None)
    assert counted('handler', 'join_failed') == before + 1
    assert worker.controller.wire_state()['last_request']['outcome'] == 'join_failed'


async def test_a_stop_during_the_join_retries_releases_the_slot(worker, clock):
    await worker.detach()
    worker.source.join_answers = [RuntimeError('broker gone')]

    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    worker.source._stop_requested = True
    await run_clock_until(clock, NOW + 10)

    assert worker.held_scopes() == []


async def test_until_ms_is_set_only_while_detached(worker, clock):
    seen: dict[str, Any] = {}
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)
    seen['detaching'] = worker.controller.wire_state()
    worker.source.leave_gate.set()
    await task
    seen['detached'] = worker.controller.wire_state()
    worker.source.join_answers = [False]
    await worker.controller.attach(cause='operator')
    seen['attaching'] = worker.controller.wire_state()
    await run_clock_until(clock, NOW + 6)
    seen['attached'] = worker.controller.wire_state()

    assert {name: (state['state'], state['until_ms'] is not None) for name, state in seen.items()} == {
        'detaching': ('detaching', False),
        'detached': ('detached', True),
        'attaching': ('attaching', False),
        'attached': ('attached', False),
    }


async def test_a_cancelled_attach_caller_does_not_stop_the_attach(worker, clock):
    await worker.detach()
    worker.source.join_answers = [False]
    caller = asyncio.create_task(worker.controller.attach(cause='handler'))
    await wait_for(lambda: 'join_group' in worker.calls)

    caller.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await caller
    await run_clock_until(clock, NOW + 10)

    assert worker.controller.state.state == 'attached'


async def test_deadline_attaches_with_cause_deadline(worker, clock):
    before = attached_by('deadline')
    await worker.detach(duration=30)

    await run_clock_until(clock, NOW + 29)
    assert worker.controller.state.state == 'detached'
    await run_clock_until(clock, NOW + 31)

    assert worker.controller.state.state == 'attached'
    assert attached_by('deadline') == before + 1
    worker.recorder.record_worker_attached.assert_called_once_with(cause='deadline', detached_seconds=30.0)


async def test_a_crashed_deadline_timer_attaches(worker, logs):
    async def broken_wait(until: float) -> None:
        raise RuntimeError('timer broke')

    worker.controller._wait_for_deadline = broken_wait

    await worker.detach()
    await wait_for(lambda: worker.controller.state.state == 'attached')

    assert len(logs.named('detach_deadline_timer_failed')) == 1


async def test_a_timer_cancelled_from_outside_attaches(worker):
    await worker.detach()

    worker.controller._timer.cancel()
    await wait_for(lambda: worker.controller.state.state == 'attached')


# --- origin rules ----------------------------------------------------------------------


async def test_a_handler_attach_does_not_end_an_operator_detach(worker):
    await worker.detach(origin=DetachOrigin.OPERATOR)
    state = worker.controller.state

    result = await worker.controller.attach(cause='handler')

    assert result == AttachResult(attached=False, reason='held_by_operator')
    assert worker.controller.state == state


async def test_an_operator_detach_of_a_handler_detach_takes_the_origin_and_the_deadline(worker):
    await worker.detach(duration=3600)

    result = await worker.detach(origin=DetachOrigin.OPERATOR, duration=100, reason='look at it')

    until = datetime.fromtimestamp(NOW + 100, UTC)
    assert result == DetachResult(detached=True, reason='', until=until)
    state = worker.controller.state
    assert (state.origin, state.until, state.reason) == (DetachOrigin.OPERATOR, until, 'look at it')
    assert worker.calls.count('leave_group') == 1


async def test_the_same_origin_replaces_the_deadline(worker, clock):
    await worker.detach(duration=3600)

    await worker.detach(duration=20)
    await run_clock_until(clock, NOW + 21)

    assert worker.controller.state.state == 'attached'


async def test_a_handler_detach_while_operator_detached_answers_already_detached(worker):
    first = await worker.detach(origin=DetachOrigin.OPERATOR, duration=600)

    result = await worker.detach(duration=60)

    assert result == DetachResult(detached=True, reason='already_detached', until=first.until)
    assert worker.controller.state.until == first.until


# --- the lease and the watchdog -----------------------------------------------------------


async def test_taken_over_attaches_at_once_with_cause_lease_lost(make, clock):
    harness = make(provider=ScriptedElection(clock, refresh=(False,)))
    harness.fleet.views = [peer()]
    before = attached_by('lease_lost')
    await harness.detach()

    await run_clock_until(clock, NOW + 21)

    assert harness.controller.state.state == 'attached'
    assert attached_by('lease_lost') == before + 1
    assert harness.provider.calls['release'] == 0


async def test_store_outage_attaches_at_the_lease_end(make, clock):
    harness = make(provider=ScriptedElection(clock, refresh=(RuntimeError('down'),) * 5))
    harness.fleet.views = [peer()]
    await harness.detach()

    await run_clock_until(clock, NOW + 59)
    assert harness.controller.state.state == 'detached'
    await run_clock_until(clock, NOW + 61)

    assert harness.controller.state.state == 'attached'
    harness.recorder.record_worker_attached.assert_called_once_with(cause='lease_lost', detached_seconds=60.0)


async def test_one_failed_refresh_keeps_the_worker_detached(make, clock):
    harness = make(provider=ScriptedElection(clock, refresh=(RuntimeError('down'),)))
    harness.fleet.views = [peer()]
    await harness.detach()

    await run_clock_until(clock, NOW + 200)

    assert harness.controller.state.state == 'detached'


async def test_a_lease_lost_during_the_leave_undoes_the_detach(make, clock):
    harness = make(provider=ScriptedElection(clock, refresh=(False,)))
    harness.fleet.views = [peer()]
    harness.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(harness.detach())

    await run_clock_until(clock, NOW + 21)

    assert (await task).reason == 'store_unavailable'
    assert harness.controller.state.state == 'attached'
    assert harness.calls.count('join_group') == 1


async def test_watchdog_attaches_when_too_few_peers_are_attached(worker):
    before = attached_by('too_few_workers')
    await worker.detach()

    await worker.controller.on_registry_refresh([peer()])
    assert worker.controller.state.state == 'detached'
    await worker.controller.on_registry_refresh([peer(detached_flag=True)])
    await wait_for(lambda: worker.controller.state.state == 'attached')

    assert attached_by('too_few_workers') == before + 1


async def test_watchdog_aborts_a_detach_in_progress(worker):
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    await worker.controller.on_registry_refresh([])

    assert (await task).reason == 'too_few_workers'
    assert worker.controller.state.state == 'attached'


async def test_watchdog_does_nothing_with_the_feature_off(worker):
    worker.app.config.sources.kafka.detach.enabled = False

    await worker.controller.on_registry_refresh([])

    assert worker.controller.state.state == 'attached'


async def test_mismatch_is_logged_once_per_peer_and_values(worker, logs):
    await worker.controller.on_registry_refresh([peer(max_detached=2, min_attached=1)])
    await worker.controller.on_registry_refresh([peer(max_detached=2, min_attached=1)])
    assert len(logs.named('detach_config_mismatch')) == 1

    await worker.controller.on_registry_refresh([peer(max_detached=3, min_attached=1)])

    lines = logs.named('detach_config_mismatch')
    assert len(lines) == 2
    assert lines[1] == {
        'event': 'detach_config_mismatch',
        'log_level': 'warning',
        'category': 'kafka',
        'peer': 'w-2',
        'peer_max_detached': 3,
        'peer_min_attached': 1,
        'max_detached': 1,
        'min_attached': 1,
    }


# --- concurrency, stop and the wire state ---------------------------------------------------


async def test_concurrent_calls_run_one_transition_at_a_time(worker):
    first, second = await asyncio.gather(worker.detach(duration=600), worker.detach(duration=100))

    assert first.detached and second.detached
    assert worker.calls.count('leave_group') == 1
    assert second.until == datetime.fromtimestamp(NOW + 100, UTC)


async def test_release_at_stop_makes_one_release_attempt(worker):
    await worker.detach()

    await worker.controller.release_at_stop(5.0)

    assert worker.provider.calls['release'] == 1
    assert worker.held_scopes() == []


async def test_release_at_stop_ends_a_transition_in_progress(worker):
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    await worker.controller.release_at_stop(5.0)

    assert worker.held_scopes() == []
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_wire_state_has_the_contract_keys(worker):
    state = worker.controller.wire_state()
    assert set(state) == {
        'enabled',
        'ui_enabled',
        'state',
        'origin',
        'reason',
        'since_ms',
        'until_ms',
        'max_duration_seconds',
        'default_duration_seconds',
        'last_request',
    }
    assert state['last_request'] is None
    assert (state['enabled'], state['state'], state['until_ms']) == (True, 'attached', None)

    await worker.detach(duration=600)
    state = worker.controller.wire_state()
    assert (state['state'], state['origin'], state['until_ms']) == ('detached', 'handler', int((NOW + 600) * 1000))
    assert state['last_request'] == {
        'origin': 'handler',
        'action': 'detach',
        'outcome': 'detached',
        'at_ms': NOW * 1000,
    }

    worker.controller.request(action='attach')
    await wait_for(lambda: worker.controller.wire_state()['state'] == 'attached')
    assert worker.controller.wire_state()['last_request']['outcome'] == 'attached'
    assert worker.controller.wire_state()['last_request']['origin'] == 'operator'


async def test_an_operator_request_shows_its_refusal(worker):
    worker.fleet.views = []

    worker.controller.request(action='detach', duration=60, reason='look')
    await wait_for(lambda: worker.controller.wire_state()['last_request'] is not None)

    assert worker.controller.wire_state()['last_request']['outcome'] == 'too_few_workers'


@pytest.mark.parametrize(
    ('action', 'duration'), [('pause', None), ('detach', 5.0)], ids=['unknown_action', 'short_duration']
)
async def test_request_refuses_a_bad_action_or_duration(worker, action, duration):
    with pytest.raises(ValueError):
        worker.controller.request(action=action, duration=duration)


async def test_reason_is_cut_at_200_characters(worker):
    await worker.detach(reason='x' * 300)

    assert worker.controller.state.reason == 'x' * REASON_MAX_CHARS


async def test_check_duration_bounds(make):
    config = make().app.config.sources.kafka.detach

    assert check_duration(None, config) == 3600.0
    assert check_duration(MIN_DURATION_SECONDS, config) == 10.0
    assert check_duration(config.max_duration, config) == config.max_duration
    for bad in (9.9, config.max_duration + 1):
        with pytest.raises(ValueError, match='from 10 to 86400 seconds'):
            check_duration(bad, config)


# --- the wiring ----------------------------------------------------------------------------


async def test_the_card_reads_the_detach_state(worker):
    await worker.detach()

    assert worker.app._lifecycle._card_features()['kafka']['detached'] is True


async def test_the_app_builds_a_controller_whose_state_the_card_reads(make):
    app = make().app
    fresh = DrakkarApp(handler=KafkaOnlyHandler(), config=app.config, worker_id='w-9')

    assert isinstance(fresh.detach, DetachController)
    assert fresh._lifecycle._is_detached() is False


async def test_the_registry_callback_runs_the_watchdog_when_the_hash_check_fails(worker, monkeypatch):
    await worker.detach()

    async def broken_check(self, views):
        raise RuntimeError('hash check broke')

    monkeypatch.setattr(cluster_tasks.ClusterHashChecker, 'check', broken_check)
    on_refresh = worker.app._lifecycle._registry_refresh_callback()

    with capture_logs() as logs:
        await on_refresh([])
    await wait_for(lambda: worker.controller.state.state == 'attached')

    failures = [entry for entry in logs if entry['event'] == 'registry_refresh_check_failed']
    assert [entry['check'] for entry in failures] == ['cluster_hash']


async def test_a_failing_watchdog_is_logged_by_the_registry_callback(worker, monkeypatch):
    async def broken_watchdog(views):
        raise RuntimeError('watchdog broke')

    monkeypatch.setattr(worker.controller, 'on_registry_refresh', broken_watchdog)
    on_refresh = worker.app._lifecycle._registry_refresh_callback()

    with capture_logs() as logs:
        await on_refresh([])

    failures = [entry for entry in logs if entry['event'] == 'registry_refresh_check_failed']
    assert [(entry['check'], entry['category']) for entry in failures] == [('detach', 'kafka')]


async def test_the_slot_release_step_runs_before_the_election_stops(worker):
    await worker.detach()
    steps = worker.app._lifecycle._stop_steps(time.monotonic() + 5)
    events = [step.event for step in steps]

    assert events.index('detach_slot_release_failed') < events.index('periodic_election_stop_failed')
    step = steps[events.index('detach_slot_release_failed')]
    assert step.category == 'kafka'
    await step.stop()
    assert worker.held_scopes() == []


async def test_the_detach_caller_is_built_from_the_autodiscover_settings(worker):
    worker.controller._caller = None

    slot = worker.controller._new_slot()

    caller = worker.controller._caller
    assert slot is not None and isinstance(caller, ResilientCaller)
    assert (caller._feature, caller._category) == ('detach', 'kafka')


async def test_a_stop_of_the_app_answers_stopping(worker):
    worker.app._stopping = True

    assert (await worker.detach()).reason == 'stopping'


async def test_a_worker_with_no_recorder_detaches_and_attaches(worker):
    worker.app._recorder = None

    assert (await worker.detach()).detached
    assert (await worker.controller.attach(cause='handler')).attached


async def test_a_failed_slot_release_is_logged_and_the_worker_attaches(worker, logs, monkeypatch):
    await worker.detach()

    async def broken_release(self, **kwargs):
        raise RuntimeError('release broke')

    monkeypatch.setattr(DetachSlot, 'release', broken_release)

    assert (await worker.controller.attach(cause='handler')).attached
    assert logs.named('detach_slot_release_failed')
    worker.controller._slot = None


async def test_an_attach_that_raises_answers_join_failed_and_is_tried_again(worker, logs, clock, monkeypatch):
    await worker.detach()
    original = worker.controller._join_and_finish
    tries: list[int] = []

    async def join_raising_once(**kwargs):
        tries.append(1)
        if len(tries) == 1:
            raise RuntimeError('join log broke')
        return await original(**kwargs)

    monkeypatch.setattr(worker.controller, '_join_and_finish', join_raising_once)

    assert await worker.controller.attach(cause='handler') == AttachResult(attached=False, reason='join_failed')
    assert worker.controller.state.state == 'attaching'
    await run_clock_until(clock, NOW + 10)

    assert worker.controller.state.state == 'attached'
    assert worker.held_scopes() == []
    assert len(logs.named('attach_transition_failed')) == 1


async def test_a_lease_lost_while_the_detach_is_logged_attaches(worker, logs, monkeypatch):
    class LossInsideTheLog(RecordingLogger):
        async def awarning(self, event, **fields):
            if event == 'worker_detached':
                await worker.controller._on_slot_lost('taken_over')
            await super().awarning(event, **fields)

    monkeypatch.setattr(detach_module, 'logger', LossInsideTheLog(logs.entries))

    assert (await worker.detach()).detached
    await wait_for(lambda: worker.controller.state.state == 'attached')

    assert worker.held_scopes() == []


async def test_an_undo_that_raises_still_releases_the_slot_and_attaches(worker, logs, clock, monkeypatch):
    worker.source.leave_outcomes = [LeaveOutcome.TIMED_OUT]
    original = worker.controller._join_attempt
    tries: list[int] = []

    async def join_raising_once(source):
        tries.append(1)
        if len(tries) <= 2:
            raise RuntimeError('join log broke')
        return await original(source)

    monkeypatch.setattr(worker.controller, '_join_attempt', join_raising_once)

    result = await worker.detach()

    assert result.reason == 'leave_failed'
    assert worker.held_scopes() == []
    await run_clock_until(clock, NOW + 10)
    assert worker.controller.state.state == 'attached'


async def test_a_failed_publish_clears_the_gauge_the_timer_and_the_card(worker, monkeypatch):
    def broken_record(**kwargs):
        raise RuntimeError('recorder broke')

    worker.recorder.record_worker_detached.side_effect = broken_record

    result = await worker.detach()

    assert result.reason == 'leave_failed'
    assert worker.controller.state.state == 'attached'
    assert detached._value.get() == 0
    assert worker.controller._timer is None
    # At detaching, at detached, and at the attach of the undo.
    assert worker.calls.count('publish') == 3


async def test_the_leave_task_is_owned_by_the_controller(worker):
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    names = {owned.get_name() for owned in worker.controller._tasks}

    assert 'drakkar-detach-leave' in names
    await worker.controller.release_at_stop(1.0)
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert all(owned.done() for owned in worker.controller._tasks)


async def test_an_error_in_the_tail_of_a_detach_job_is_logged(worker, logs, monkeypatch):
    async def broken_account(*args, **kwargs):
        raise RuntimeError('account broke')

    monkeypatch.setattr(worker.controller, '_account_detach', broken_account)

    result = await worker.detach()

    assert result.detached
    assert logs.named('detach_transition_failed')


async def test_a_detach_cancelled_by_the_stop_is_counted_as_stopping(worker):
    before = counted('handler', 'stopping')
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    await worker.controller.release_at_stop(1.0)
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert counted('handler', 'stopping') == before + 1
    assert worker.controller.wire_state()['last_request']['outcome'] == 'stopping'


@pytest.mark.parametrize('cause', ['deadline', 'lease_lost', 'too_few_workers', 'anything'])
async def test_attach_refuses_a_cause_a_caller_may_not_give(worker, cause):
    with pytest.raises(ValueError, match='cause must be handler or operator'):
        await worker.controller.attach(cause=cause)


async def test_an_operator_request_that_raises_is_logged(worker, logs, monkeypatch):
    async def broken_attach(**kwargs):
        raise RuntimeError('attach broke')

    monkeypatch.setattr(worker.controller, 'attach', broken_attach)

    worker.controller.request(action='attach')
    await wait_for(lambda: logs.named('detach_request_failed'))


async def test_with_the_feature_off_a_hash_check_error_reaches_the_registry(worker, monkeypatch):
    worker.app.config.sources.kafka.detach.enabled = False

    async def broken_check(self, views):
        raise RuntimeError('hash check broke')

    monkeypatch.setattr(cluster_tasks.ClusterHashChecker, 'check', broken_check)
    on_refresh = worker.app._lifecycle._registry_refresh_callback()

    with pytest.raises(RuntimeError, match='hash check broke'):
        await on_refresh([])


# --- the undo and the join retry -------------------------------------------------------------


async def test_an_attach_waiting_behind_a_failed_undo_answers_join_failed(worker):
    worker.source.leave_gate = asyncio.Event()
    worker.source.leave_outcomes = [LeaveOutcome.TIMED_OUT]
    worker.source.join_answers = [False] * 10
    detach = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)
    attach = asyncio.create_task(worker.controller.attach(cause='operator'))
    await asyncio.sleep(0)

    worker.source.leave_gate.set()

    assert (await asyncio.wait_for(detach, timeout=2)).reason == 'leave_failed'
    assert await asyncio.wait_for(attach, timeout=2) == AttachResult(attached=False, reason='join_failed')
    assert worker.controller.state.state == 'attaching'
    assert worker.held_scopes() == [worker.scopes[0]]


async def test_a_detach_during_the_undo_join_answers_join_failed_at_once(worker):
    worker.source.leave_outcomes = [LeaveOutcome.TIMED_OUT]
    worker.source.join_answers = [HANG]
    first = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'join_group' in worker.calls)

    second = await asyncio.wait_for(worker.detach(origin=DetachOrigin.OPERATOR), timeout=2)

    assert second == DetachResult(detached=False, reason='join_failed', until=None)
    assert worker.controller.wire_state()['last_request']['outcome'] == 'join_failed'
    await worker.controller.release_at_stop(1.0)
    with contextlib.suppress(asyncio.CancelledError):
        await first


async def test_a_join_retry_cancelled_from_outside_starts_again(worker, clock, logs):
    await worker.detach()
    worker.source.join_answers = [False, True]
    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'

    worker.controller._join_retry.cancel()
    await asyncio.sleep(0)
    await run_clock_until(clock, NOW + 6)

    assert worker.controller.state.state == 'attached'
    assert len(logs.named('detach_join_retry_restarted')) == 1


async def test_a_join_retry_whose_sleep_raises_goes_on(worker, clock, logs, monkeypatch):
    await worker.detach()
    worker.source.join_answers = [False, True]
    real_sleep = clock.sleep
    raised: list[int] = []

    async def sleep_raising_once(seconds):
        if seconds == 5.0 and not raised:
            raised.append(1)
            raise RuntimeError('sleep broke')
        await real_sleep(seconds)

    monkeypatch.setattr(clock, 'sleep', sleep_raising_once)
    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    await run_clock_until(clock, NOW + 6)
    # After the error the retry pauses on the event loop, not on the clock.
    await wait_for(lambda: worker.controller.state.state == 'attached')

    assert worker.controller.state.state == 'attached'
    assert raised


async def test_a_new_attaching_period_gets_its_own_retry_and_cause(worker, clock):
    await worker.detach(origin=DetachOrigin.OPERATOR)
    worker.source.join_answers = [False]
    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    old_retry = worker.controller._join_retry
    # The first period ends without its retry: the worker is attached again by other means.
    await worker.controller._release_slot(worker.controller._slot)
    worker.controller._state = detach_module.ATTACHED_STATE
    worker.source._group_member = True
    assert (await worker.detach(origin=DetachOrigin.OPERATOR)).detached
    worker.source.join_answers = [False, True]

    assert (await worker.controller.attach(cause='handler')).reason == 'held_by_operator'
    assert (await worker.controller.attach(cause='operator', reason='second')).reason == 'join_failed'

    assert worker.controller._join_retry is not old_retry
    await run_clock_until(clock, NOW + 6)
    assert worker.controller.state.state == 'attached'
    worker.recorder.record_worker_attached.assert_called_once()
    assert old_retry.done()


async def test_an_undone_detach_writes_no_attach_row_and_logs_detach_undone(worker, clock, logs):
    before = attached_by('handler')
    worker.source.leave_outcomes = [LeaveOutcome.TIMED_OUT]
    worker.source.join_answers = [False, True]

    assert (await worker.detach()).reason == 'leave_failed'
    await run_clock_until(clock, NOW + 6)

    assert worker.controller.state.state == 'attached'
    assert worker.held_scopes() == []
    worker.recorder.record_worker_attached.assert_not_called()
    assert attached_by('handler') == before
    assert not logs.named('worker_attached')
    (undone,) = logs.named('detach_undone')
    assert (undone['log_level'], undone['category'], undone['origin'], undone['outside_seconds']) == (
        'info',
        'kafka',
        'handler',
        5.0,
    )


# --- the stop, cancelled callers and the card ---------------------------------------------------


async def test_a_detach_caller_whose_job_the_stop_cancels_answers_stopping(worker):
    worker.source.leave_gate = asyncio.Event()
    caller = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    await worker.controller.release_at_stop(1.0)

    assert await caller == DetachResult(detached=False, reason='stopping', until=None)


async def test_an_attach_caller_whose_job_the_stop_cancels_answers_stopping(worker):
    await worker.detach()
    worker.source.join_answers = [HANG]
    caller = asyncio.create_task(worker.controller.attach(cause='operator'))
    await wait_for(lambda: 'join_group' in worker.calls)

    await worker.controller.release_at_stop(1.0)

    assert await caller == AttachResult(attached=False, reason='stopping')


async def test_a_caller_cancelled_while_the_worker_stops_is_counted_as_stopping(worker):
    stopping_before = counted('handler', 'stopping')
    failed_before = counted('handler', 'leave_failed')
    worker.source.leave_gate = asyncio.Event()
    caller = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    worker.app._stopping = True
    caller.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await caller
    worker.source.leave_gate.set()
    await wait_for(lambda: all(task.done() for task in worker.controller._tasks))

    assert counted('handler', 'stopping') == stopping_before + 1
    assert counted('handler', 'leave_failed') == failed_before
    assert 'join_group' not in worker.calls


async def test_a_caller_cancelled_before_its_job_took_the_lock_is_not_counted(worker, logs):
    failed_before = counted('handler', 'leave_failed')
    worker.source.leave_gate = asyncio.Event()
    first = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)
    second = asyncio.create_task(worker.detach())
    for _ in range(5):
        await asyncio.sleep(0)

    second.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await second
    worker.source.leave_gate.set()
    assert (await first).detached
    await wait_for(lambda: not any(task.get_name() == 'drakkar-detach' for task in worker.controller._tasks))

    assert counted('handler', 'leave_failed') == failed_before
    assert not logs.named('detach_refused')
    assert worker.controller.state.state == 'detached'
    assert worker.calls.count('leave_group') == 1


async def test_the_card_is_published_when_the_detach_starts(worker):
    worker.source.leave_gate = asyncio.Event()
    task = asyncio.create_task(worker.detach())
    await wait_for(lambda: 'leave_group' in worker.calls)

    assert worker.calls.index('publish') < worker.calls.index('leave_group')
    assert worker.app._lifecycle._card_features()['kafka']['detached'] is True
    worker.source.leave_gate.set()
    assert (await task).detached


async def test_a_replaced_deadline_or_origin_is_logged_and_published(worker, logs):
    await worker.detach(duration=3600)
    worker.calls.clear()

    result = await worker.detach(origin=DetachOrigin.OPERATOR, duration=100, reason='look at it')

    assert logs.named('detach_extended') == [
        {
            'event': 'detach_extended',
            'log_level': 'info',
            'category': 'kafka',
            'origin': 'operator',
            'until': format_rfc3339_micro(result.until),
            'reason': 'look at it',
        }
    ]
    assert 'publish' in worker.calls
    worker.recorder.record_worker_detached.assert_called_once()


async def test_an_already_detached_answer_logs_no_extension(worker, logs):
    await worker.detach(origin=DetachOrigin.OPERATOR, duration=600)

    assert (await worker.detach(duration=60)).reason == 'already_detached'

    assert not logs.named('detach_extended')


async def test_a_size_read_that_raises_releases_the_slot_without_a_join(worker, logs, monkeypatch):
    async def broken_held_count(self):
        raise RuntimeError('holder read broke')

    monkeypatch.setattr(DetachSlot, 'held_count', broken_held_count)
    states: list[str] = []
    original_enter = worker.controller._enter_attaching
    monkeypatch.setattr(
        worker.controller, '_enter_attaching', lambda state: (states.append('attaching'), original_enter(state))
    )

    result = await worker.detach()

    assert result.reason == 'leave_failed'
    assert states == []
    assert 'join_group' not in worker.calls
    assert not logs.named('detach_undone')
    assert worker.held_scopes() == []
    assert worker.controller.state.state == 'attached'


async def test_the_slot_release_runs_before_the_recorder_stops(worker):
    await worker.detach()
    events = [step.event for step in worker.app._lifecycle._stop_steps(time.monotonic() + 5)]

    assert events[0] == 'detach_slot_release_failed'
    assert events.index('detach_slot_release_failed') < events.index('recorder_stop_failed')


async def test_a_join_retry_whose_sleep_always_raises_waits_on_the_loop(worker, clock, monkeypatch):
    monkeypatch.setattr(detach_module, 'JOIN_ERROR_PAUSE_SECONDS', 0.05, raising=False)
    await worker.detach()
    worker.source.join_answers = [False] * 1000
    real_sleep = clock.sleep
    sleeps: list[int] = []

    async def retry_sleep_raises(seconds):
        if seconds != detach_module.JOIN_RETRY_SECONDS:
            await real_sleep(seconds)
            return
        sleeps.append(1)
        if len(sleeps) >= 1000:
            # Ends the period, so a loop that spins still stops.
            worker.controller._attaching_period += 1
        raise RuntimeError('sleep broke')

    monkeypatch.setattr(clock, 'sleep', retry_sleep_raises)
    assert (await worker.controller.attach(cause='operator')).reason == 'join_failed'
    await asyncio.sleep(0.3)

    assert 1 <= len(sleeps) < 50
    worker.controller._attaching_period += 1
