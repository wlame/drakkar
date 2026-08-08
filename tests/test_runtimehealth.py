"""Tests for the runtime health monitor (drakkar/runtimehealth.py).

Everything time-related is driven synthetically — lag values are passed
straight into the state machine and the sampler check reads a heartbeat
we set by hand — so no test depends on real wall-clock stalls.
"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from drakkar.config import RuntimeHealthConfig
from drakkar.runtimehealth import (
    RECOVERY_TICKS,
    RuntimeHealthMonitor,
    _LagWindow,
    _StallSamples,
    task_census,
)


class RecorderSpy:
    """Captures record_runtime_* calls; mocked because the real recorder needs a DB."""

    def __init__(self) -> None:
        self.health_events: list[dict] = []
        self.stall_events: list[dict] = []

    def record_runtime_health(self, **kwargs) -> None:
        self.health_events.append(kwargs)

    def record_runtime_stall(self, **kwargs) -> None:
        self.stall_events.append(kwargs)


@pytest.fixture
def monitor():
    config = RuntimeHealthConfig(tick_seconds=0.25, warn_lag_seconds=0.1, stall_seconds=1.0)
    spy = RecorderSpy()
    mon = RuntimeHealthMonitor(config, recorder=spy)  # type: ignore[arg-type]
    return mon, spy


# ---- _LagWindow -------------------------------------------------------------


def test_lag_window_aggregates_within_one_second():
    window = _LagWindow(window_seconds=60)
    window.add(100.1, 0.010)
    window.add(100.5, 0.030)
    window.add(100.9, 0.020)
    (bucket,) = window.snapshot()
    assert bucket['t'] == 100
    assert bucket['max_lag_ms'] == 30.0
    assert bucket['avg_lag_ms'] == 20.0


def test_lag_window_evicts_beyond_window():
    window = _LagWindow(window_seconds=3)
    for second in range(5):
        window.add(100.0 + second, 0.001)
    assert [b['t'] for b in window.snapshot()] == [102, 103, 104]


# ---- _StallSamples ----------------------------------------------------------


def test_stall_samples_collapse_same_location_and_cap_distinct():
    samples = _StallSamples(max_stacks=2)

    def grab():
        # Both frames report the same (file, line) — this return line —
        # like a loop stuck at one blocking call sampled twice.
        return sys._getframe()

    samples.add(grab())
    samples.add(grab())
    stacks, dropped = samples.drain()
    assert len(stacks) == 1
    assert stacks[0]['count'] == 2
    assert stacks[0]['location'].split(':')[0].endswith('test_runtimehealth.py')
    assert 'test_stall_samples_collapse' in stacks[0]['stack']
    assert dropped == 0


def test_stall_samples_drop_past_cap_and_reset_on_drain():
    samples = _StallSamples(max_stacks=1)

    def frame_at_distinct_line(n):
        if n == 0:
            return sys._getframe()
        return sys._getframe()

    samples.add(frame_at_distinct_line(0))
    samples.add(frame_at_distinct_line(1))
    stacks, dropped = samples.drain()
    assert len(stacks) == 1 and dropped == 1
    assert samples.drain() == ([], 0)


# ---- state machine ----------------------------------------------------------


def test_clean_ticks_stay_healthy_and_emit_nothing(monitor):
    mon, spy = monitor
    for _ in range(20):
        mon._advance_state(0.001, 100.0)
    assert mon.state == 'healthy'
    assert spy.health_events == []
    assert spy.stall_events == []


async def test_warn_lag_degrades_then_recovers_with_hysteresis(monitor):
    mon, spy = monitor
    mon._advance_state(0.2, 100.0)
    assert mon.state == 'degraded'
    assert spy.health_events[-1]['kind'] == 'transition'
    assert spy.health_events[-1]['state'] == 'degraded'

    # One clean tick is not recovery.
    mon._advance_state(0.001, 101.0)
    assert mon.state == 'degraded'

    for i in range(RECOVERY_TICKS):
        mon._advance_state(0.001, 102.0 + i)
    assert mon.state == 'healthy'
    assert spy.health_events[-1]['state'] == 'healthy'
    # Exactly two transitions total — no flapping spam.
    assert len([e for e in spy.health_events if e['kind'] == 'transition']) == 2


async def test_stall_tick_emits_stall_event_with_drained_stacks(monitor):
    mon, spy = monitor
    mon._samples.add(sys._getframe())
    mon._advance_state(2.5, 100.0)
    assert mon.state == 'stalled'
    (stall,) = spy.stall_events
    assert stall['duration_ms'] == 2500.0
    assert len(stall['stacks']) == 1
    assert stall['dropped_stacks'] == 0
    assert mon._recent_stalls[-1]['stack_count'] == 1


async def test_degraded_tick_between_stalls_does_not_reemit_stall(monitor):
    mon, spy = monitor
    mon._advance_state(2.5, 100.0)
    mon._advance_state(0.2, 101.0)
    assert mon.state == 'degraded'
    assert len(spy.stall_events) == 1


async def test_sample_events_respect_interval(monitor):
    mon, spy = monitor
    mon._maybe_emit_sample(100.0, 0.001)
    mon._maybe_emit_sample(105.0, 0.001)  # < sample_interval_seconds (10) later
    mon._maybe_emit_sample(111.0, 0.001)
    samples = [e for e in spy.health_events if e['kind'] == 'sample']
    assert len(samples) == 2


# ---- sampler-thread check ---------------------------------------------------


def test_sample_once_captures_nothing_while_heartbeat_fresh(monitor):
    mon, _ = monitor
    mon._loop_thread_id = -1  # never matches a real thread
    mon._heartbeat = time.monotonic()
    mon._sample_once()
    assert mon._samples.drain() == ([], 0)


def test_sample_once_captures_own_thread_stack_when_stalled(monitor):
    mon, _ = monitor
    import threading

    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic() - 10.0  # heartbeat long silent
    mon._sample_once()
    stacks, _ = mon._samples.drain()
    assert len(stacks) == 1
    assert 'test_sample_once_captures_own_thread_stack' in stacks[0]['stack']


# ---- snapshot ---------------------------------------------------------------


def test_snapshot_reports_stalled_from_heartbeat_age_alone(monitor):
    mon, _ = monitor
    mon._heartbeat = time.monotonic() - 10.0
    snap = mon.snapshot()
    assert snap['state'] == 'stalled'
    assert snap['heartbeat_age_ms'] >= 10_000
    assert snap['unit_label'] == 'tasks'
    assert snap['window'] == []
    assert snap['recent_stalls'] == []


def test_snapshot_uses_state_machine_state_when_heartbeat_fresh(monitor):
    mon, _ = monitor
    mon._heartbeat = time.monotonic()
    mon.state = 'degraded'
    assert mon.snapshot()['state'] == 'degraded'


# ---- lifecycle + census -----------------------------------------------------


async def test_start_and_stop_clean():
    config = RuntimeHealthConfig(tick_seconds=0.05)
    mon = RuntimeHealthMonitor(config, recorder=None)
    mon.start()
    assert mon._task is not None and mon._thread is not None
    await mon.stop()
    assert mon._task is None and mon._thread is None


async def test_task_census_groups_by_coroutine_and_location():
    started = asyncio.Event()

    async def parked_worker():
        started.set()
        await asyncio.sleep(3600)

    tasks = [asyncio.create_task(parked_worker(), name=f'worker-{i}') for i in range(3)]
    await started.wait()
    await asyncio.sleep(0)  # let every worker reach its suspension point
    try:
        rows = task_census()
        parked = next(r for r in rows if 'parked_worker' in r['name'])
        assert parked['count'] == 3
        assert parked['location'].split(':')[0].endswith('test_runtimehealth.py')
        assert parked['example'].startswith('worker-')
    finally:
        for task in tasks:
            task.cancel()
