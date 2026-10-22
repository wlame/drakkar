"""Cluster periodic tasks: the slot loop, the async slot lease, the election service and the thread fence.

The loop tests run on a virtual clock: a sleep wakes only when nothing else
on the event loop can run, and the clock then jumps to the earliest wake
time. No test waits for a real slot, and every time in an assertion is
exact. The loop's log lines go to an in-memory logger, because structlog's
async methods write through a thread, and the virtual clock would move on
while a line is still being written.
"""

import asyncio
import contextlib
import heapq
import importlib
import itertools
import math
import threading
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from prometheus_client import REGISTRY

from drakkar import cluster_tasks
from drakkar.cluster_tasks import (
    SLOT_ELECTION_FAILED,
    SLOT_HELD_ELSEWHERE,
    SLOT_LATE,
    SLOT_WON,
    ClusterElection,
    ClusterHashChecker,
    ClusterTask,
    SlotContext,
    SlotLease,
    card_cluster_hash,
    run_cluster_task,
)
from drakkar.config.resilience import ResilienceConfig
from drakkar.fleet.service import PeerView
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import election as election_mod
from drakkar.recorder.election import LOSS_TAKEN_OVER, LOSS_UNCONFIRMED, ElectionContext, ThreadFence
from drakkar.schedule import ClusterTaskDefinition, task_scope
from tests.fleet_fakes import ElectionStore, MemoryElection

CLUSTER = 'search-fleet'
PROVIDER = 'scripted'
# A whole multiple of 30: the first slot of a 30 s task after NOW is SLOT.
SLOT = 1_800_000_030
NOW = SLOT - 25.0
CONTEXT = ElectionContext(
    worker='w-1', instance='0123456789abcdef', cluster=CLUSTER, shared_dir='', logger=None, options={}
)


# --- test doubles ---


class VirtualClock:
    """A clock whose time moves only to the next sleeper's wake time, once the event loop has nothing else to run.

    ``drift`` lists, for each of the first sleeps, how many seconds it wakes
    before it was asked to (negative: after), like ``asyncio.sleep`` rounding
    or a clock that runs behind. The clock does not move past ``limit``;
    when the next wake time is past it, ``parked`` is set and the sleepers
    stay asleep.
    """

    def __init__(self, now: float, drift: tuple[float, ...] = ()) -> None:
        self.now = now
        self.drift = list(drift)
        self.sleeps: list[float] = []
        self.limit = math.inf
        self.parked = asyncio.Event()
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._order = itertools.count()
        self._check_scheduled = False

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        wake = self.now + seconds - (self.drift.pop(0) if self.drift else 0)
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (wake, next(self._order), future))
        self._schedule_check()
        await future

    def _schedule_check(self) -> None:
        if not self._check_scheduled:
            self._check_scheduled = True
            asyncio.get_running_loop().call_soon(self._wake_next_when_idle)

    def _wake_next_when_idle(self) -> None:
        loop = asyncio.get_running_loop()
        # asyncio has no public "idle" signal: the ready queue is empty only
        # when every task waits, here on a sleeper's future.
        if loop._ready:  # type: ignore[attr-defined]
            loop.call_soon(self._wake_next_when_idle)
            return
        self._check_scheduled = False
        while self._sleepers and self._sleepers[0][2].done():
            heapq.heappop(self._sleepers)  # a sleeper whose task was cancelled
        if not self._sleepers or self._sleepers[0][0] > self.limit:
            self.parked.set()
            return
        wake = self._sleepers[0][0]
        self.now = max(self.now, wake)
        # Every sleeper due at this time wakes at once, so loops due
        # in the same second really run side by side.
        while self._sleepers and self._sleepers[0][0] == wake:
            future = heapq.heappop(self._sleepers)[2]
            if not future.done():
                future.set_result(None)
        self._schedule_check()


class RecordingLogger:
    """An async logger that keeps every line in memory, written on the event loop."""

    def __init__(self, entries: list[dict] | None = None, context: dict | None = None) -> None:
        self.entries: list[dict] = [] if entries is None else entries
        self._context = context or {}

    def bind(self, **fields: Any) -> 'RecordingLogger':
        return RecordingLogger(self.entries, {**self._context, **fields})

    def _write(self, level: str, event: str, fields: dict) -> None:
        self.entries.append({'event': event, 'log_level': level, **self._context, **fields})

    def debug(self, event: str, **fields: Any) -> None:
        self._write('debug', event, fields)

    async def ainfo(self, event: str, **fields: Any) -> None:
        self._write('info', event, fields)

    async def awarning(self, event: str, **fields: Any) -> None:
        self._write('warning', event, fields)

    async def aexception(self, event: str, **fields: Any) -> None:
        self._write('error', event, fields)

    def named(self, event: str) -> list[dict]:
        return [entry for entry in self.entries if entry['event'] == event]


class ScriptedElection(MemoryElection):
    """A memory election whose answers can be scripted per call, recording the clock time of every call.

    ``start_error`` makes every start raise. ``acquire`` and ``refresh`` list
    the answers of the first calls (a bool, or an exception to raise); later
    calls answer as the memory election does. ``start_yields``,
    ``refresh_yields`` and ``is_held_yields`` make a call give the event loop
    that many turns before it answers; ``start_seconds`` makes a start take
    that long on the clock.
    """

    def __init__(
        self,
        clock: VirtualClock | None = None,
        *,
        start_error: Exception | None = None,
        acquire: tuple[bool | Exception, ...] = (),
        refresh: tuple[bool | Exception, ...] = (),
        start_yields: int = 0,
        refresh_yields: int = 0,
        is_held_yields: int = 0,
        start_seconds: float = 0.0,
    ) -> None:
        super().__init__(ElectionStore())
        self._clock = clock
        self._start_error = start_error
        self._acquire = list(acquire)
        self._refresh = list(refresh)
        self._start_yields = start_yields
        self._refresh_yields = refresh_yields
        self._is_held_yields = is_held_yields
        self._start_seconds = start_seconds
        self.at: defaultdict[str, list[float]] = defaultdict(list)

    @property
    def store(self) -> ElectionStore:
        return self._store

    def _note(self, op: str) -> None:
        self.at[op].append(self._clock.time() if self._clock is not None else 0.0)

    async def start(self, ctx: ElectionContext) -> None:
        self._note('start')
        for _ in range(self._start_yields):
            await asyncio.sleep(0)
        if self._start_seconds and self._clock is not None:
            await self._clock.sleep(self._start_seconds)
        if self._start_error is not None:
            self.calls['start'] += 1
            raise self._start_error
        await super().start(ctx)

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self._note('acquire')
        if self._acquire:
            return self._scripted('acquire', self._acquire.pop(0))
        return await super().acquire(scope, token, lease_seconds)

    async def refresh(self, scope: str, token: str) -> bool:
        self._note('refresh')
        for _ in range(self._refresh_yields):
            await asyncio.sleep(0)
        if self._refresh:
            return self._scripted('refresh', self._refresh.pop(0))
        return await super().refresh(scope, token)

    async def is_held(self, scope: str, token: str) -> bool:
        for _ in range(self._is_held_yields):
            await asyncio.sleep(0)
        return await super().is_held(scope, token)

    async def release(self, scope: str, token: str) -> None:
        self._note('release')
        await super().release(scope, token)

    def _scripted(self, op: str, answer: bool | Exception) -> bool:
        self.calls[op] += 1
        if isinstance(answer, Exception):
            raise answer
        return answer


class SpyCaller(ResilientCaller):
    """A resilient caller that notes the deadline each operation was given."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.deadlines: list[tuple[str, float | None]] = []

    async def call(self, op: str, make_call: Callable[[], Awaitable[Any]], **kwargs: Any) -> Any:
        self.deadlines.append((op, kwargs.get('deadline_seconds')))
        return await super().call(op, make_call, **kwargs)


class FakeRecorder:
    """Keeps the keyword arguments of every periodic run row."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def record_periodic_run(self, **row: Any) -> None:
        self.rows.append(row)


async def _no_sleep(seconds: float) -> None:
    return None


@pytest.fixture
def logs(monkeypatch) -> RecordingLogger:
    recording = RecordingLogger()
    monkeypatch.setattr(cluster_tasks, 'logger', recording)
    # The package re-exports the decorator under the module's name.
    monkeypatch.setattr(importlib.import_module('drakkar.periodic'), 'logger', recording)
    return recording


def make_caller(logger: Any = None, provider: str = PROVIDER, **resilience: Any) -> SpyCaller:
    return SpyCaller(
        ResilienceConfig(**{'retries': 0, **resilience}),
        feature='election',
        provider=provider,
        category='periodic',
        logger=logger if logger is not None else RecordingLogger(),
        sleep=_no_sleep,
    )


def make_election(
    provider: Any, logger: Any = None, provider_name: str = PROVIDER, clock: Any = None
) -> ClusterElection:
    return ClusterElection(
        provider,
        make_caller(logger, provider=provider_name),
        provider_name=provider_name,
        context=CONTEXT,
        **({} if clock is None else {'clock': clock}),
    )


def make_task(
    name: str,
    run: Callable[[SlotContext], Awaitable[None]],
    *,
    interval: int | None = 30,
    cron: str | None = None,
    lease_seconds: int = 60,
    **kwargs: Any,
) -> ClusterTask:
    return ClusterTask(
        name=name,
        run=run,
        interval=interval,
        cron=cron,
        lease_seconds=lease_seconds,
        on_error=kwargs.pop('on_error', 'continue'),
        **kwargs,
    )


def sleeping_body(clock: VirtualClock, seconds: float, starts: list[tuple[float, int]]):
    async def body(ctx: SlotContext) -> None:
        starts.append((clock.time(), ctx.slot))
        await clock.sleep(seconds)

    return body


async def run_until(clock: VirtualClock, coro: Awaitable[None], until: float) -> None:
    """Run ``coro`` until it ends or the clock would pass ``until``; then cancel it."""
    clock.limit = until
    loop_task = asyncio.ensure_future(coro)
    parked = asyncio.ensure_future(clock.parked.wait())
    await asyncio.wait({loop_task, parked}, return_when=asyncio.FIRST_COMPLETED)
    parked.cancel()
    loop_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await loop_task


def slots(name: str, outcome: str) -> float:
    return REGISTRY.get_sample_value('drakkar_periodic_cluster_slots_total', {'name': name, 'outcome': outcome}) or 0.0


def sample(metric: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(metric, labels) or 0.0


def at(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp()


# --- the slot loop ---


async def test_cluster_slot_won_runs_once_holds_until_s_plus_h_then_releases(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('won_once', sleeping_body(clock, 3, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 25
    )

    assert starts == [(SLOT, SLOT)]
    assert provider.at['acquire'] == [SLOT]
    assert provider.at['release'] == [SLOT + 20]
    assert provider.store.holder(task_scope(CLUSTER, 'won_once')) is None
    assert slots('won_once', SLOT_WON) == 1
    assert sample('drakkar_periodic_task_runs_total', name='won_once', status='ok') == 1
    assert sample('drakkar_periodic_task_duration_seconds_count', name='won_once') == 1
    assert logs.named('periodic_task_started') == [
        {
            'event': 'periodic_task_started',
            'log_level': 'info',
            'periodic_task': 'won_once',
            'interval_seconds': 30,
            'category': 'periodic',
            'scope': 'cluster',
        }
    ]
    assert logs.named('periodic_slot_skipped') == []


async def test_cluster_slot_held_elsewhere_skips_and_logs_debug(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(False,))
    starts: list[tuple[float, int]] = []
    task = make_task('held_elsewhere', sleeping_body(clock, 1, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 5
    )

    assert starts == []
    assert provider.calls['release'] == 0
    assert slots('held_elsewhere', SLOT_HELD_ELSEWHERE) == 1
    assert logs.named('periodic_slot_skipped') == [
        {
            'event': 'periodic_slot_skipped',
            'log_level': 'debug',
            'category': 'periodic',
            'name': 'held_elsewhere',
            'slot': SLOT,
            'outcome': SLOT_HELD_ELSEWHERE,
        }
    ]


async def test_cluster_slot_late_when_woken_after_the_acquire_window(logs):
    clock = VirtualClock(NOW, drift=(-10,))
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('late_wake', sleeping_body(clock, 1, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 15
    )

    assert starts == []
    assert provider.calls == {}
    assert slots('late_wake', SLOT_LATE) == 1
    assert logs.named('periodic_slot_skipped') == [
        {
            'event': 'periodic_slot_skipped',
            'log_level': 'warning',
            'category': 'periodic',
            'name': 'late_wake',
            'slot': SLOT,
            'outcome': SLOT_LATE,
            'late_seconds': 10.0,
            'suppressed': 0,
        }
    ]


async def test_cluster_slot_early_wake_sleeps_again_and_never_runs_before_slot(logs):
    clock = VirtualClock(NOW, drift=(0.5,))
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('early_wake', sleeping_body(clock, 1, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 5
    )

    assert starts == [(SLOT, SLOT)]
    assert provider.at['acquire'] == [SLOT]
    assert clock.sleeps[:2] == [25.0, 0.5]


async def test_cluster_slot_after_a_clock_step_back_never_runs_the_same_slot_twice(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    starts: list[int] = []

    async def body(ctx: SlotContext) -> None:
        starts.append(ctx.slot)
        clock.now -= 40  # the clock steps back during the run

    task = make_task('step_back', body)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 35
    )

    assert starts == [SLOT, SLOT + 30]


async def test_cluster_slot_acquire_deadline_is_s_plus_w_minus_now(logs):
    clock = VirtualClock(NOW, drift=(-3.25,))
    provider = ScriptedElection(clock)
    election = make_election(provider, logs, clock=clock)
    task = make_task('deadline', sleeping_body(clock, 1, []))

    await run_until(clock, run_cluster_task(task, election, cluster=CLUSTER, clock=clock), SLOT + 5)

    deadlines = dict(election.caller.deadlines)
    assert deadlines['start'] == 6.75
    assert deadlines['acquire'] == 6.75


async def test_cluster_election_start_after_waiting_on_the_lock_gets_the_time_left(logs):
    clock = VirtualClock(NOW)
    # Each start takes 4 s on the clock and fails; the second task waits for the first one's.
    provider = ScriptedElection(clock, start_error=ConnectionError('election service unreachable'), start_seconds=4)
    election = make_election(provider, logs, clock=clock)
    tasks = [make_task(name, sleeping_body(clock, 1, [])) for name in ('waits_a', 'waits_b')]

    async def both() -> None:
        await asyncio.gather(*(run_cluster_task(task, election, cluster=CLUSTER, clock=clock) for task in tasks))

    await run_until(clock, both(), SLOT + 9)

    assert [deadline for op, deadline in election.caller.deadlines if op == 'start'] == [10.0, 6.0]


async def test_cluster_slot_unclear_acquire_releases_and_counts_election_failed(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(ConnectionError('election service unreachable'),))
    starts: list[tuple[float, int]] = []
    task = make_task('unclear_acquire', sleeping_body(clock, 1, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 5
    )

    assert starts == []
    # The provider may have taken the lease before its answer was lost: it is given back at once.
    assert provider.at['release'] == [SLOT]
    assert slots('unclear_acquire', SLOT_ELECTION_FAILED) == 1
    [skipped] = logs.named('periodic_slot_skipped')
    assert skipped['outcome'] == SLOT_ELECTION_FAILED
    assert skipped['error'] == 'acquire error'
    assert skipped['log_level'] == 'warning'
    assert 'late_seconds' not in skipped


async def test_cluster_slot_start_failure_counts_election_failed_with_error_text(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, start_error=ConnectionError('election service unreachable'))
    starts: list[tuple[float, int]] = []
    task = make_task('start_fails', sleeping_body(clock, 1, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 5
    )

    assert starts == []
    assert provider.calls['acquire'] == 0
    assert slots('start_fails', SLOT_ELECTION_FAILED) == 1
    assert logs.named('periodic_slot_skipped') == [
        {
            'event': 'periodic_slot_skipped',
            'log_level': 'warning',
            'category': 'periodic',
            'name': 'start_fails',
            'slot': SLOT,
            'outcome': SLOT_ELECTION_FAILED,
            'error': 'start error',
            'suppressed': 0,
        }
    ]
    assert logs.named('provider_started') == []


async def test_cluster_slot_warnings_are_rate_limited_per_task_with_suppressed_count(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, start_error=ConnectionError('election service unreachable'))
    election = make_election(provider, logs)
    first = make_task('limited_first', sleeping_body(clock, 1, []))
    second = make_task('limited_second', sleeping_body(clock, 1, []))

    async def both() -> None:
        await asyncio.gather(
            run_cluster_task(first, election, cluster=CLUSTER, clock=clock),
            run_cluster_task(second, election, cluster=CLUSTER, clock=clock),
        )

    # Slots at SLOT, SLOT + 30 and SLOT + 60 fail; the warning of the middle one is held back.
    await run_until(clock, both(), SLOT + 65)

    assert slots('limited_first', SLOT_ELECTION_FAILED) == 3
    for name in ('limited_first', 'limited_second'):
        lines = [entry for entry in logs.named('periodic_slot_skipped') if entry['name'] == name]
        assert [(line['slot'], line['suppressed']) for line in lines] == [(SLOT, 0), (SLOT + 60, 1)]


async def test_cluster_election_concurrent_first_slots_start_the_provider_once(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, start_yields=3)
    election = make_election(provider, logs)
    starts: list[tuple[float, int]] = []
    tasks = [make_task(name, sleeping_body(clock, 1, starts)) for name in ('first_slot_a', 'first_slot_b')]

    async def both() -> None:
        await asyncio.gather(*(run_cluster_task(task, election, cluster=CLUSTER, clock=clock) for task in tasks))

    await run_until(clock, both(), SLOT + 5)

    assert provider.calls['start'] == 1
    assert provider.calls['acquire'] == 2
    assert starts == [(SLOT, SLOT), (SLOT, SLOT)]
    assert logs.named('provider_started') == [
        {
            'event': 'provider_started',
            'log_level': 'info',
            'category': 'periodic',
            'feature': 'election',
            'provider': PROVIDER,
        }
    ]


async def test_slot_lease_refresh_false_logs_lease_lost_once_and_run_goes_on(logs):
    clock = VirtualClock(NOW)
    # A lease of 60 is refreshed every 20 s; the first refresh finds another holder.
    provider = ScriptedElection(clock, refresh=(False,))
    finished: list[float] = []
    checks: list[bool] = []

    async def body(ctx: SlotContext) -> None:
        await clock.sleep(50)
        finished.append(clock.time())
        checks.append(await ctx.lease.is_held())

    task = make_task('lease_taken_over', body)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 55
    )

    assert finished == [SLOT + 50]
    assert provider.at['refresh'] == [SLOT + 20]
    # A lost lease reads as not held without asking the provider again.
    assert checks == [False]
    assert provider.calls['is_held'] == 0
    assert provider.calls['release'] == 0
    assert logs.named('periodic_lease_lost') == [
        {
            'event': 'periodic_lease_lost',
            'log_level': 'warning',
            'category': 'periodic',
            'name': 'lease_taken_over',
            'slot': SLOT,
            'reason': LOSS_TAKEN_OVER,
        }
    ]
    assert sample('drakkar_periodic_task_runs_total', name='lease_taken_over', status='ok') == 1


async def test_slot_lease_taken_over_found_by_a_check_is_final(logs):
    clock = VirtualClock(NOW)
    # A refresh would fail; the takeover the check found must stay the reason.
    provider = ScriptedElection(clock, refresh=(ConnectionError('election service unreachable'),))
    leases: list[SlotLease] = []
    checks: list[bool] = []

    async def body(ctx: SlotContext) -> None:
        leases.append(ctx.lease)
        provider.store.leases[ctx.lease.scope] = ('another-token', math.inf, 60)
        checks.append(await ctx.lease.is_held())
        await clock.sleep(50)
        checks.append(await ctx.lease.is_held())

    task = make_task('check_taken_over', body)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 55
    )

    assert checks == [False, False]
    assert leases[0].loss_reason == LOSS_TAKEN_OVER
    # The refresher ended with the check: no refresh, no lost line with another reason, no release.
    assert provider.calls['refresh'] == 0
    assert provider.calls['is_held'] == 1
    assert logs.named('periodic_lease_lost') == []
    assert provider.calls['release'] == 0


async def test_slot_lease_failed_refresh_never_replaces_a_takeover_found_meanwhile(logs):
    clock = VirtualClock(NOW)
    # At SLOT + 20 the check and the refresh run side by side; the check answers first.
    provider = ScriptedElection(
        clock, refresh=(ConnectionError('election service unreachable'),), refresh_yields=5, is_held_yields=1
    )

    async def body(ctx: SlotContext) -> None:
        await clock.sleep(20)
        provider.store.leases[ctx.lease.scope] = ('another-token', math.inf, 60)
        await ctx.lease.is_held()
        await clock.sleep(10)

    task = make_task('refresh_after_check', body)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 35
    )

    assert provider.at['refresh'] == [SLOT + 20]
    assert [line['reason'] for line in logs.named('periodic_lease_lost')] == [LOSS_TAKEN_OVER]
    assert provider.calls['release'] == 0


async def test_slot_lease_release_waits_for_a_refresh_in_progress_and_keeps_its_answer(logs):
    clock = VirtualClock(NOW)
    # The refresh and the end of the hold both fall on SLOT + 20; the refresh is still in its call.
    provider = ScriptedElection(clock, refresh=(False,), refresh_yields=3)
    task = make_task('release_waits', sleeping_body(clock, 1, []))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 25
    )

    assert provider.at['refresh'] == [SLOT + 20]
    assert [line['reason'] for line in logs.named('periodic_lease_lost')] == [LOSS_TAKEN_OVER]
    assert provider.calls['release'] == 0


async def test_slot_lease_release_after_a_successful_refresh_in_progress_still_releases(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh_yields=3)
    task = make_task('release_after_refresh', sleeping_body(clock, 1, []))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 25
    )

    assert provider.at['refresh'] == [SLOT + 20]
    assert provider.at['release'] == [SLOT + 20]
    assert logs.named('periodic_lease_lost') == []


class _UncheckableElection(ScriptedElection):
    """A provider whose checks fail; every other call answers."""

    async def is_held(self, scope: str, token: str) -> bool:
        raise ConnectionError('election service unreachable')


async def test_slot_lease_check_that_fails_is_unconfirmed_not_final():
    provider = _UncheckableElection()
    lease = SlotLease(make_election(provider), scope=task_scope(CLUSTER, 'check_fails'), lease_seconds=60)
    assert await lease.acquire(10) == (SLOT_WON, None)

    assert await lease.is_held() is False
    assert lease.loss_reason == LOSS_UNCONFIRMED
    # Not final: the next check asks again, and the lease may still be held, so it is released.
    assert await lease.is_held() is False
    await lease.release()
    assert provider.calls['release'] == 1


def test_slot_lease_stop_refreshing_without_a_refresher_does_nothing():
    lease = SlotLease(make_election(ScriptedElection()), scope=task_scope(CLUSTER, 'never_refreshed'), lease_seconds=60)

    lease.stop_refreshing()

    assert lease.loss_reason is None


async def test_slot_lease_refresher_error_is_logged_not_left_on_the_task(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=(False,))
    lease = SlotLease(
        make_election(provider, logs), scope=task_scope(CLUSTER, 'refresher_error'), lease_seconds=60, clock=clock
    )
    assert await lease.acquire(10) == (SLOT_WON, None)
    refreshers: list[asyncio.Task] = []

    async def failing_report(reason: str) -> None:
        raise RuntimeError('log sink is gone')

    async def scenario() -> None:
        lease.start_refreshing(failing_report)
        refreshers.append(lease._refresher)  # type: ignore[arg-type]
        await clock.sleep(30)
        await lease.release()

    await run_until(clock, scenario(), NOW + 35)

    [failed] = logs.named('periodic_lease_refresh_failed')
    assert failed['scope'] == task_scope(CLUSTER, 'refresher_error')
    assert failed['log_level'] == 'error'
    assert refreshers[0].done() and refreshers[0].exception() is None
    assert provider.calls['release'] == 0


async def test_slot_lease_refresh_failure_reason_unconfirmed_still_releases(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, refresh=(ConnectionError('election service unreachable'),))
    task = make_task('lease_unconfirmed', sleeping_body(clock, 50, []))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 55
    )

    [lost] = logs.named('periodic_lease_lost')
    assert lost['reason'] == LOSS_UNCONFIRMED
    assert provider.at['refresh'] == [SLOT + 20]
    assert provider.at['release'] == [SLOT + 50]


async def test_slot_lease_refreshes_every_third_of_the_lease_while_the_run_goes_on(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    task = make_task('lease_refreshed', sleeping_body(clock, 100, []), lease_seconds=90)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 105
    )

    assert provider.at['refresh'] == [SLOT + 30, SLOT + 60, SLOT + 90]
    assert provider.at['release'] == [SLOT + 100]
    assert logs.named('periodic_lease_lost') == []


async def test_cluster_task_cancelled_while_holding_does_not_release(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('cancelled_holder', sleeping_body(clock, 100, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 10
    )
    await asyncio.sleep(0)

    assert starts == [(SLOT, SLOT)]
    assert provider.calls['release'] == 0
    assert provider.store.holder(task_scope(CLUSTER, 'cancelled_holder')) is not None
    # The refresher stopped with the loop: no task of the lease is left.
    assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []


async def test_long_run_skips_the_slots_it_overlaps(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('long_run', sleeping_body(clock, 70, starts))

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 95
    )

    # The run ends at SLOT + 70; SLOT + 30 and SLOT + 60 passed during it and are not run.
    assert starts == [(SLOT, SLOT), (SLOT + 90, SLOT + 90)]
    assert provider.at['release'] == [SLOT + 70]
    assert slots('long_run', SLOT_LATE) == 0


async def test_cluster_cron_task_uses_the_next_fire_time_as_the_slot(logs):
    clock = VirtualClock(at('2026-11-01T00:00:10'))
    provider = ScriptedElection(clock)
    starts: list[tuple[float, int]] = []
    task = make_task('cron_slot', sleeping_body(clock, 1, starts), interval=None, cron='*/5 * * * *')

    await run_until(
        clock,
        run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock),
        at('2026-11-01T00:05:30'),
    )

    fire = int(at('2026-11-01T00:05:00'))
    assert starts == [(fire, fire)]
    assert logs.named('periodic_task_started')[0]['cron'] == '*/5 * * * *'


@pytest.mark.parametrize('system', [False, True])
async def test_cluster_run_records_scope_and_slot_in_the_periodic_run_row(logs, system):
    clock = VirtualClock(NOW)
    recorder = FakeRecorder()
    name = f'recorded_row_{system}'.lower()
    task = make_task(name, sleeping_body(clock, 1, []), system=system)
    election = make_election(ScriptedElection(clock), logs)

    await run_until(clock, run_cluster_task(task, election, cluster=CLUSTER, recorder=recorder, clock=clock), SLOT + 5)

    [row] = recorder.rows
    assert row['name'] == name
    assert row['status'] == 'ok'
    assert row['slot'] == SLOT
    assert row['system'] is system


@pytest.mark.parametrize(('on_error', 'runs'), [('continue', 2), ('stop', 1)])
async def test_cluster_run_error_follows_on_error_after_the_release(logs, on_error, runs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock)
    recorder = FakeRecorder()
    name = f'failing_run_{on_error}'

    async def body(ctx: SlotContext) -> None:
        raise ValueError('rates source answered 500')

    task = make_task(name, body, on_error=on_error)

    await run_until(
        clock,
        run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, recorder=recorder, clock=clock),
        SLOT + 35,
    )

    assert [row['status'] for row in recorder.rows] == ['error'] * runs
    assert recorder.rows[0]['error'] == 'rates source answered 500'
    assert provider.at['release'][0] == SLOT + 20
    assert len(logs.named('periodic_task_failed')) == runs
    assert len(logs.named('periodic_task_stopped')) == (1 if on_error == 'stop' else 0)


async def test_on_skip_hook_gets_every_non_won_outcome(logs):
    clock = VirtualClock(NOW, drift=(-10,))
    provider = ScriptedElection(clock, acquire=(False, ConnectionError('election service unreachable')))
    seen: list[str] = []
    task = make_task('skip_hook', sleeping_body(clock, 1, []), on_skip=seen.append)

    # SLOT is late, SLOT + 30 is held elsewhere, SLOT + 60 fails its acquire.
    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 65
    )

    assert seen == [SLOT_LATE, SLOT_HELD_ELSEWHERE, SLOT_ELECTION_FAILED]


async def test_failing_on_skip_hook_is_logged_and_the_loop_goes_on(logs):
    clock = VirtualClock(NOW)
    provider = ScriptedElection(clock, acquire=(False, False))

    def failing_hook(outcome: str) -> None:
        raise ValueError('footprint executor is gone')

    task = make_task('failing_hook', sleeping_body(clock, 1, []), on_skip=failing_hook)

    await run_until(
        clock, run_cluster_task(task, make_election(provider, logs), cluster=CLUSTER, clock=clock), SLOT + 35
    )

    assert slots('failing_hook', SLOT_HELD_ELSEWHERE) == 2
    lines = logs.named('periodic_skip_hook_failed')
    assert [(line['slot'], line['outcome'], line['log_level']) for line in lines] == [
        (SLOT, SLOT_HELD_ELSEWHERE, 'error'),
        (SLOT + 30, SLOT_HELD_ELSEWHERE, 'error'),
    ]
    assert len(logs.named('periodic_slot_skipped')) == 2


def test_cluster_task_definition_carries_name_schedule_and_lease():
    async def body(ctx: SlotContext) -> None:
        return None

    task = make_task('refresh_rates', body, interval=None, cron='0 * * * *', lease_seconds=120)

    assert task.definition() == ClusterTaskDefinition(
        name='refresh_rates', interval=None, cron='0 * * * *', lease_seconds=120
    )


# --- the slot lease and the election service ---


async def test_slot_lease_token_is_16_hex_unless_given():
    election = make_election(ScriptedElection())

    lease = SlotLease(election, scope=task_scope(CLUSTER, 'tokens'), lease_seconds=60)

    assert len(lease.token) == 16
    assert int(lease.token, 16) >= 0
    assert SlotLease(election, scope='a/b', lease_seconds=60, token='f' * 16).token == 'f' * 16
    assert lease.election is election


async def test_slot_lease_acquire_with_a_spent_deadline_makes_no_call_and_releases_nothing():
    provider = ScriptedElection()
    lease = SlotLease(make_election(provider), scope=task_scope(CLUSTER, 'spent'), lease_seconds=60)

    assert await lease.acquire(0) == (SLOT_ELECTION_FAILED, 'acquire timeout')

    assert provider.calls == {}


async def test_slot_lease_is_held_follows_the_provider_and_ends_after_a_loss():
    provider = ScriptedElection()
    scope = task_scope(CLUSTER, 'checked')
    lease = SlotLease(make_election(provider), scope=scope, lease_seconds=60)
    assert await lease.acquire(10) == (SLOT_WON, None)

    assert await lease.is_held() is True
    provider.store.leases[scope] = ('another-token', math.inf, 60)
    assert await lease.is_held() is False
    assert lease.loss_reason == LOSS_TAKEN_OVER

    await lease.release()
    assert provider.calls['release'] == 0


async def test_cluster_election_stop_counts_not_started_when_never_started():
    provider = ScriptedElection()
    election = make_election(provider, provider_name='never_started')
    before = sample(
        'drakkar_provider_calls_total', feature='election', provider='never_started', op='stop', outcome='not_started'
    )

    await election.stop(5.0)
    await election.stop(5.0)

    after = sample(
        'drakkar_provider_calls_total', feature='election', provider='never_started', op='stop', outcome='not_started'
    )
    assert after == before + 1
    assert provider.calls['stop'] == 0


async def test_cluster_election_stop_after_a_start_makes_one_attempt_once():
    provider = ScriptedElection()
    election = make_election(provider)
    assert (await election.ensure_started(5.0)).ok

    await election.stop(5.0)
    await election.stop(5.0)

    assert provider.calls['stop'] == 1
    assert ('stop', 5.0) in election.caller.deadlines


async def test_cluster_election_after_stop_never_starts_the_provider():
    provider = ScriptedElection()
    election = make_election(provider)
    await election.stop(None)

    result = await election.ensure_started(5.0)

    assert result.ok is False
    assert result.outcome == 'not_started'
    assert provider.calls['start'] == 0


# --- the thread fence ---


async def _held_lease(provider: ScriptedElection, name: str) -> SlotLease:
    lease = SlotLease(make_election(provider), scope=task_scope(CLUSTER, name), lease_seconds=60)
    assert await lease.acquire(10) == (SLOT_WON, None)
    return lease


def _fence(lease: SlotLease, loop: asyncio.AbstractEventLoop) -> ThreadFence:
    return ThreadFence(lease, lease.election.caller, loop, provider_name=lease.election.provider_name)


async def test_thread_fence_refuses_calls_on_its_loop():
    provider = ScriptedElection()
    fence = _fence(await _held_lease(provider, 'fence_on_loop'), asyncio.get_running_loop())

    with pytest.raises(RuntimeError, match='never on its event loop'):
        fence.is_held()

    assert provider.calls['is_held'] == 0


async def test_thread_fence_answers_from_a_worker_thread():
    provider = ScriptedElection()
    lease = await _held_lease(provider, 'fence_in_thread')
    fence = _fence(lease, asyncio.get_running_loop())

    assert fence.token == lease.token
    assert fence.provider_name == PROVIDER
    assert await asyncio.to_thread(fence.is_held) is True
    provider.store.leases[lease.scope] = ('another-token', math.inf, 60)
    assert await asyncio.to_thread(fence.is_held) is False
    assert fence.loss_reason == LOSS_TAKEN_OVER


async def test_thread_fence_on_a_closed_loop_reads_as_unconfirmed():
    lease = await _held_lease(ScriptedElection(), 'fence_closed_loop')
    loop = asyncio.new_event_loop()
    loop.close()
    fence = _fence(lease, loop)

    assert fence.is_held() is False
    assert fence.loss_reason == LOSS_UNCONFIRMED


@contextlib.contextmanager
def _loop_in_thread() -> Iterator[asyncio.AbstractEventLoop]:
    """An event loop running in its own thread, so the test's thread can call the fence directly."""
    loop = asyncio.new_event_loop()
    runner = threading.Thread(target=loop.run_forever, name='test-fence-loop')
    runner.start()
    try:
        yield loop
    finally:
        # Like asyncio.run: cancel and await what is left, so no abandoned check is destroyed unrun.
        asyncio.run_coroutine_threadsafe(_cancel_the_other_tasks(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        runner.join()
        loop.close()


async def _cancel_the_other_tasks() -> None:
    others = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in others:
        task.cancel()
    await asyncio.gather(*others, return_exceptions=True)


def test_thread_fence_that_gets_no_answer_in_time_reads_as_unconfirmed(monkeypatch):
    monkeypatch.setattr(election_mod, 'BRIDGE_SLACK_SECONDS', 0.0)
    provider = ScriptedElection()
    with _loop_in_thread() as loop:
        caller = make_caller(timeout_seconds=0.01)
        election = ClusterElection(provider, caller, provider_name=PROVIDER, context=CONTEXT)
        lease = SlotLease(election, scope=task_scope(CLUSTER, 'fence_slow'), lease_seconds=60)
        fence = _fence(lease, loop)
        # The loop is busy for longer than the check may take.
        loop.call_soon_threadsafe(time.sleep, 0.2)

        assert fence.is_held() is False
        assert fence.loss_reason == LOSS_UNCONFIRMED

        # The loop is free again; give the next check time to answer. The lease
        # was never acquired, so the provider answers that another token holds it.
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(timeout=5)
        fence._wait_seconds = 5.0
        assert fence.is_held() is False
        assert fence.loss_reason == LOSS_TAKEN_OVER

        # A later check with no answer does not hide the confirmed takeover.
        fence._wait_seconds = 0.01
        loop.call_soon_threadsafe(time.sleep, 0.2)
        assert fence.is_held() is False
        assert fence.loss_reason == LOSS_TAKEN_OVER


# --- an open breaker recovers without a restart ---


class _FailsFirstStarts(ScriptedElection):
    """A memory election whose first ``failures`` starts raise, like a service that comes back."""

    def __init__(self, clock: VirtualClock, failures: int) -> None:
        super().__init__(clock)
        self.failures = failures

    async def start(self, ctx: ElectionContext) -> None:
        if self.failures:
            self.failures -= 1
            self._note('start')
            raise ConnectionError('election service unreachable')
        await super().start(ctx)


async def test_cluster_slots_recover_after_the_breaker_cooldown_without_a_restart(logs):
    clock = VirtualClock(NOW)
    provider = _FailsFirstStarts(clock, failures=2)
    caller = ResilientCaller(
        ResilienceConfig(retries=0, breaker={'failure_threshold': 2, 'cooldown_seconds': 40}),
        feature='election',
        provider='recovering',
        category='periodic',
        logger=RecordingLogger(),
        now_monotonic=clock.time,
        sleep=_no_sleep,
    )
    election = ClusterElection(provider, caller, provider_name='recovering', context=CONTEXT, clock=clock)
    starts: list[tuple[float, int]] = []
    skipped: list[str] = []
    task = make_task('recovers', sleeping_body(clock, 1, starts), on_skip=skipped.append)

    # Two failed starts open the breaker at SLOT + 30; SLOT + 60 is inside its
    # cooldown; the probe start at SLOT + 90 succeeds and the slot is won.
    await run_until(clock, run_cluster_task(task, election, cluster=CLUSTER, clock=clock), SLOT + 115)

    assert provider.at['start'] == [SLOT, SLOT + 30, SLOT + 90]
    assert skipped == [SLOT_ELECTION_FAILED] * 3
    errors = [(line['slot'], line['error']) for line in logs.named('periodic_slot_skipped')]
    assert errors == [(SLOT, 'start error'), (SLOT + 60, 'start skipped_open')]
    assert starts == [(SLOT + 90, SLOT + 90)]
    assert slots('recovers', SLOT_WON) == 1


# --- the definition hash of peers ---

OWN_HASH = 'b5557e3108585ea3'
OTHER_HASH = '0123456789abcdef'


def peer(worker: str, *, online: bool | None = True, cluster: str = CLUSTER, features: Any = None) -> PeerView:
    card: dict[str, Any] = {'schema': 1, 'worker': worker, 'cluster': cluster, 'url': f'http://{worker}:8080/'}
    if features is not None:
        card['features'] = features
    return PeerView(card=card, age_seconds=1.0, online=online, last_seen_ts=None)


def hashed(value: Any) -> dict:
    return {'cache': {}, 'periodic': {'cluster_hash': value}}


MALFORMED_FEATURES = {
    'no_features': None,
    'features_not_an_object': ['periodic'],
    'periodic_not_an_object': {'periodic': 'b5557e3108585ea3'},
    'hash_too_short': hashed('b5557e3108585ea'),
    'hash_upper_case': hashed('B5557E3108585EA3'),
    'hash_not_a_string': hashed(1234567890123456),
}


@pytest.mark.parametrize('features', MALFORMED_FEATURES.values(), ids=MALFORMED_FEATURES.keys())
def test_card_cluster_hash_reads_a_malformed_card_as_unknown(features):
    assert card_cluster_hash(peer('w-2', features=features).card) is None


def test_card_cluster_hash_reads_the_contract_form():
    assert card_cluster_hash(peer('w-2', features=hashed(OTHER_HASH)).card) == OTHER_HASH


def mismatch_gauge() -> float:
    return REGISTRY.get_sample_value('drakkar_periodic_cluster_mismatch_peers') or 0.0


async def test_mismatch_logged_once_per_peer_and_hash_and_gauge_counts_online_peers():
    log = RecordingLogger()
    checker = ClusterHashChecker(OWN_HASH, CLUSTER, log)
    views = [
        peer('w-2', features=hashed(OTHER_HASH)),
        peer('w-3', features=hashed(OTHER_HASH)),
        peer('w-4', features=hashed(OWN_HASH)),
    ]

    await checker.check(views)
    await checker.check(views)
    assert mismatch_gauge() == 2
    newer = 'fedcba9876543210'
    await checker.check([peer('w-2', features=hashed(newer)), *views[1:]])

    lines = [(line['peer'], line['peer_hash']) for line in log.named('periodic_cluster_mismatch')]
    assert lines == [('w-2', OTHER_HASH), ('w-3', OTHER_HASH), ('w-2', newer)]
    assert log.named('periodic_cluster_mismatch')[0] == {
        'event': 'periodic_cluster_mismatch',
        'log_level': 'warning',
        'category': 'periodic',
        'peer': 'w-2',
        'own_hash': OWN_HASH,
        'peer_hash': OTHER_HASH,
    }
    await checker.check([peer('w-4', features=hashed(OWN_HASH))])
    assert mismatch_gauge() == 0


async def test_mismatch_ignores_offline_peers_other_clusters_and_malformed_features():
    log = RecordingLogger()
    checker = ClusterHashChecker(OWN_HASH, CLUSTER, log)
    views = [
        peer('w-offline', online=False, features=hashed(OTHER_HASH)),
        peer('w-unknown', online=None, features=hashed(OTHER_HASH)),
        peer('w-elsewhere', cluster='other-fleet', features=hashed(OTHER_HASH)),
        *(peer(f'w-{name}', features=features) for name, features in MALFORMED_FEATURES.items()),
    ]

    await checker.check(views)

    assert log.named('periodic_cluster_mismatch') == []
    assert mismatch_gauge() == 0


async def test_mismatch_logs_again_for_a_peer_that_left_the_listing_and_came_back():
    log = RecordingLogger()
    checker = ClusterHashChecker(OWN_HASH, CLUSTER, log)
    returning = peer('w-2', features=hashed(OTHER_HASH))

    await checker.check([returning])
    await checker.check([])
    await checker.check([returning])

    assert len(log.named('periodic_cluster_mismatch')) == 2
