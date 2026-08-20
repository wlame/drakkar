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
    classify_episode,
    task_census,
)


class RecorderSpy:
    """Captures record_runtime_* calls; mocked because the real recorder needs a DB."""

    def __init__(self) -> None:
        self.health_events: list[dict] = []
        self.stall_events: list[dict] = []
        self.episode_events: list[dict] = []
        self.probe_events: list[dict] = []

    def record_runtime_health(self, **kwargs) -> None:
        self.health_events.append(kwargs)

    def record_runtime_stall(self, **kwargs) -> None:
        self.stall_events.append(kwargs)

    def record_runtime_lag_episode(self, **kwargs) -> None:
        self.episode_events.append(kwargs)

    def record_runtime_probe(self, **kwargs) -> None:
        self.probe_events.append(kwargs)


@pytest.fixture(autouse=True)
def isolated_host_readers(monkeypatch):
    """Episode close reads /proc via hostinfo; stub it all out for isolation."""
    from drakkar import runtimehealth as rh

    monkeypatch.setattr(rh, 'read_thread_cpu_ms', lambda _tid: None)
    monkeypatch.setattr(rh, 'read_cpu_throttle', lambda: None)
    monkeypatch.setattr(rh, 'read_pressure', lambda: None)
    monkeypatch.setattr(rh, 'read_loadavg', lambda: None)
    return rh


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


# ---- verdict classification ---------------------------------------------------


@pytest.mark.parametrize(
    ('cpu_ratio', 'dominant_share', 'evidence', 'expected'),
    [
        # The loop itself burned the wall time — regardless of stacks.
        (0.95, 0.9, False, 'cpu_bound'),
        (0.7, 0.0, False, 'cpu_bound'),
        # Little CPU + one dominant call site = a blocking call.
        (0.05, 0.8, False, 'blocked'),
        (0.0, 0.6, True, 'blocked'),
        # Little CPU + no single culprit = the process was starved.
        (0.05, 0.2, False, 'starved'),
        (0.1, 0.0, True, 'starved'),
        # Thread clock unreadable: a dominant site is enough alone...
        (None, 0.9, False, 'blocked'),
        # ...otherwise only corroborated pressure justifies "starved".
        (None, 0.1, True, 'starved'),
        (None, 0.1, False, 'inconclusive'),
        # Mid CPU band matches nothing specific.
        (0.5, 0.9, True, 'inconclusive'),
    ],
)
def test_classify_episode_decision_table(cpu_ratio, dominant_share, evidence, expected):
    assert classify_episode(cpu_ratio, dominant_share, evidence) == expected


# ---- lag episodes -------------------------------------------------------------


async def test_episode_opens_on_degraded_and_closes_on_recovery(monitor):
    mon, spy = monitor
    mon._advance_state(0.2, 100.0)
    assert mon._episode is not None
    assert mon.snapshot()['current_episode'] is not None

    for i in range(RECOVERY_TICKS):
        mon._advance_state(0.001, 101.0 + i)

    assert mon.state == 'healthy'
    assert mon._episode is None
    assert mon.snapshot()['current_episode'] is None
    (episode,) = spy.episode_events
    assert episode['peak_lag_ms'] == 200.0
    assert episode['lag_sum_ms'] == 200.0
    assert episode['stall_count'] == 0
    assert episode['verdict'] == 'inconclusive'  # no CPU signal, no stacks, no evidence
    assert mon.snapshot()['recent_episodes'][-1]['verdict'] == 'inconclusive'


async def test_episode_counts_stalls_and_accumulates_lag(monitor):
    mon, spy = monitor
    mon._advance_state(2.0, 100.0)  # stall tick opens the episode
    mon._advance_state(0.3, 102.0)  # degraded tick continues it
    mon._advance_state(1.5, 104.0)  # second stall
    for i in range(RECOVERY_TICKS):
        mon._advance_state(0.001, 105.0 + i)

    (episode,) = spy.episode_events
    assert episode['stall_count'] == 2
    assert episode['peak_lag_ms'] == 2000.0
    assert episode['lag_sum_ms'] == pytest.approx(3800.0)
    assert len(spy.stall_events) == 2


async def test_episode_start_is_backdated_by_the_opening_lag(monitor):
    mon, _ = monitor
    mon._advance_state(2.0, 100.0)
    episode = mon._episode
    assert episode is not None
    assert episode.started_wall == pytest.approx(98.0)


async def test_episode_max_seconds_flushes_and_reopens(monitor):
    mon, spy = monitor
    mon._advance_state(0.2, 100.0)
    first = mon._episode
    assert first is not None
    # Simulate an episode that has already outlived the cap.
    first.started_monotonic -= mon._config.episode_max_seconds + 1
    mon._advance_state(0.2, 101.0)

    assert len(spy.episode_events) == 1
    assert mon._episode is not None
    assert mon._episode is not first
    assert spy.episode_events[0]['duration_ms'] >= mon._config.episode_max_seconds * 1000


async def test_episode_verdict_blocked_from_dominant_stack_and_low_cpu(monitor, isolated_host_readers, monkeypatch):
    mon, spy = monitor
    # Loop-thread CPU: 0 ms at open, 1 ms at close — negligible vs wall.
    cpu_values = iter([0.0, 1.0])
    monkeypatch.setattr(isolated_host_readers, 'read_thread_cpu_ms', lambda _tid: next(cpu_values))
    mon._loop_native_tid = 1234

    mon._advance_state(0.2, 100.0)
    episode = mon._episode
    assert episode is not None

    def grab():
        # Same (file, line) for both samples — one dominant blocking site.
        return sys._getframe()

    episode.samples.add(grab())
    episode.samples.add(grab())
    for i in range(RECOVERY_TICKS):
        mon._advance_state(0.001, 101.0 + i)

    (event,) = spy.episode_events
    assert event['verdict'] == 'blocked'
    assert event['cpu_ms'] == 1.0
    assert event['sample_count'] == 2
    assert len(event['stacks']) == 1
    assert event['stacks'][0]['count'] == 2


async def test_episode_verdict_starved_from_throttle_evidence(monitor, isolated_host_readers, monkeypatch):
    mon, spy = monitor
    # Thread clock unreadable, no stacks captured — but the cgroup says the
    # process spent 3 seconds throttled during the episode.
    throttle_values = iter([(10, 1_000_000), (14, 4_000_000)])
    monkeypatch.setattr(isolated_host_readers, 'read_cpu_throttle', lambda: next(throttle_values))

    mon._advance_state(0.2, 100.0)
    for i in range(RECOVERY_TICKS):
        mon._advance_state(0.001, 101.0 + i)

    (event,) = spy.episode_events
    assert event['verdict'] == 'starved'
    # The recorder merges evidence flat into the metadata JSON; the spy
    # sees it as the keyword argument it was passed as.
    assert event['evidence']['cpu_throttled_ms'] == 3000.0


async def test_snapshot_reports_running_episode_with_verdict(monitor, isolated_host_readers, monkeypatch):
    mon, _ = monitor
    monkeypatch.setattr(isolated_host_readers, 'read_thread_cpu_ms', lambda _tid: 0.0)
    mon._loop_native_tid = 1234
    mon._advance_state(0.5, 100.0)
    episode = mon._episode
    assert episode is not None
    episode.samples.add(sys._getframe())

    view = mon.snapshot()['current_episode']
    assert view is not None
    assert view['peak_lag_ms'] == 500.0
    assert view['sample_count'] == 1
    assert view['cpu_ms'] == 0.0
    assert view['verdict'] == 'blocked'  # low CPU, one dominant site
    # The live view must not steal the samples the close will aggregate.
    stacks, _, _ = episode.samples.peek()
    assert len(stacks) == 1


# ---- sampler during episodes ---------------------------------------------------


def test_sampler_captures_into_episode_while_heartbeat_fresh(monitor):
    import threading

    mon, _ = monitor
    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic()  # loop looks alive — diffuse degradation
    mon._episode = mon._open_episode(0.2, 100.0)

    mon._sample_once()

    stacks, _, total = mon._episode.samples.peek()
    assert total == 1
    assert 'test_sampler_captures_into_episode' in stacks[0]['stack']
    assert mon._samples.drain() == ([], 0)  # the hard-stall aggregate stays empty


def test_sampler_feeds_both_aggregates_during_stalled_episode(monitor):
    import threading

    mon, _ = monitor
    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic() - 10.0  # hard stall in progress
    mon._episode = mon._open_episode(2.0, 100.0)

    mon._sample_once()

    stall_stacks, _ = mon._samples.drain()
    episode_stacks, _, _ = mon._episode.samples.peek()
    assert len(stall_stacks) == 1
    assert len(episode_stacks) == 1


# ---- probes ---------------------------------------------------------------------


def test_probes_disabled_by_default(monitor):
    import threading

    mon, spy = monitor
    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic()

    mon._sample_once()
    mon._drain_probes()

    assert spy.probe_events == []


def test_probe_captures_and_heartbeat_records_it():
    import threading

    config = RuntimeHealthConfig(probe_interval_seconds=0.0001)
    spy = RecorderSpy()
    mon = RuntimeHealthMonitor(config, recorder=spy)  # type: ignore[arg-type]
    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic()

    mon._sample_once()
    assert spy.probe_events == []  # captured on the sampler thread, not yet recorded
    mon._drain_probes()

    (probe,) = spy.probe_events
    assert probe['stacks'][0]['count'] == 1
    assert 'test_probe_captures_and_heartbeat_records_it' in probe['stacks'][0]['stack']
    assert probe['unit_count'] == -1  # heartbeat never computed one yet


def test_probe_respects_interval():
    import threading

    config = RuntimeHealthConfig(probe_interval_seconds=3600.0)
    mon = RuntimeHealthMonitor(config, recorder=RecorderSpy())  # type: ignore[arg-type]
    mon._loop_thread_id = threading.get_ident()
    mon._heartbeat = time.monotonic()

    mon._sample_once()  # first probe fires immediately
    mon._sample_once()  # second is 3600 s away

    with mon._probe_lock:
        assert len(mon._pending_probes) == 1


def test_probe_first_capture_ignores_host_uptime(monkeypatch):
    # time.monotonic() counts from BOOT on Linux: on a freshly started host
    # (every CI runner) it can be smaller than the probe interval, and the
    # old 0.0 sentinel silently delayed the first probe until host uptime
    # exceeded it. The first probe must fire regardless of the clock value.
    import threading

    from drakkar import runtimehealth as rh

    config = RuntimeHealthConfig(probe_interval_seconds=3600.0)
    mon = RuntimeHealthMonitor(config, recorder=RecorderSpy())  # type: ignore[arg-type]
    mon._loop_thread_id = threading.get_ident()
    monkeypatch.setattr(rh.time, 'monotonic', lambda: 100.0)  # "booted 100 s ago"
    mon._heartbeat = 100.0

    mon._sample_once()

    with mon._probe_lock:
        assert len(mon._pending_probes) == 1
