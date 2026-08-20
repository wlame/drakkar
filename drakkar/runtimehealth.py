"""Runtime health monitor: event-loop lag tracking and stall introspection.

Two cooperating parts, because a blocked event loop cannot observe itself:

- A **heartbeat task** (coroutine) sleeps ``tick_seconds`` and measures how
  late it wakes — that lateness IS the event-loop lag. Each tick it folds
  the lag into a per-second ring buffer, updates Prometheus metrics, and
  advances a monotonic heartbeat timestamp.
- A **sampler thread** wakes at the same interval and does exactly one
  comparison: heartbeat age vs ``stall_seconds``. Only while the loop is
  actually stalled does it call ``sys._current_frames()`` and capture the
  traceback of the code blocking the loop — the introspection payload.

When the loop resumes, the heartbeat task drains the captured stacks into
one ``runtime_stall`` flight-recorder event. State transitions and
low-frequency samples become ``runtime_health`` events. The healthy-path
cost per tick is one clock read, one comparison, and one ring-buffer
write under an uncontended lock.

On top of single stalls sits the **lag episode**: the whole span the
runtime is degraded or stalled. A single long blocking call produces a
clean stall report, but diffuse degradation — a thousand small delays
from CPU starvation — never trips the stall threshold. During an episode
the sampler thread therefore captures stacks on every wakeup (not only
while the heartbeat is stale) and the monitor tracks the loop *thread's*
CPU time next to wall time. On close, one ``runtime_lag_episode`` event
carries the aggregated stacks and a **verdict**: ``blocked`` (one call
site dominated, little CPU), ``cpu_bound`` (the loop itself burned the
time), ``starved`` (the loop wanted CPU and did not get it — host-level
contention), or ``inconclusive``.

Wire naming is deliberately backend-neutral (``lag_ms``, ``unit_count``,
``unit_label``): the Go backend can serve the same contract from its
scheduler-latency and goroutine metrics without a spec change.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import traceback
from collections import deque
from typing import TYPE_CHECKING, Any, Literal

import structlog

from drakkar.hostinfo import read_cpu_throttle, read_loadavg, read_pressure, read_thread_cpu_ms
from drakkar.metrics import loop_lag_seconds, runtime_health_state, runtime_stalls

if TYPE_CHECKING:
    from drakkar.config import RuntimeHealthConfig
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

HealthState = Literal['healthy', 'degraded', 'stalled']

# Gauge encoding of the state, shared with the Go backend's contract.
STATE_VALUES: dict[HealthState, int] = {'healthy': 0, 'degraded': 1, 'stalled': 2}

# Consecutive clean ticks required to leave 'degraded' — hysteresis so a
# loop hovering around warn_lag_seconds emits one transition, not dozens.
RECOVERY_TICKS = 5

# What this backend counts as its concurrency unit; the Go backend sends
# "goroutines". The UI takes wording from this field, never hardcodes it.
UNIT_LABEL = 'tasks'

Verdict = Literal['blocked', 'cpu_bound', 'starved', 'inconclusive']

# Verdict thresholds. cpu_ratio is loop-thread CPU over wall time of the
# episode: near 1.0 means the loop itself burned the wall clock, near 0
# means it mostly waited (on a syscall, or on the scheduler).
CPU_BOUND_RATIO = 0.7
LOW_CPU_RATIO = 0.3
# A single non-idle call site must account for this share of the non-idle
# samples to count as "dominant" — the signature of one blocking call.
DOMINANT_SITE_SHARE = 0.6

# Sampled frames whose innermost location matches one of these are the
# loop's own idle wait (epoll), not a blocking site — they carry no blame.
IDLE_STACK_MARKERS = ('selectors.py', 'selector_events.py')

# The verdict decision table, first match wins. Row shape:
# (cpu_band, dominant_site, pressure_evidence) -> verdict, None = wildcard.
# cpu bands: 'high' >= CPU_BOUND_RATIO, 'low' <= LOW_CPU_RATIO,
# 'mid' in between, 'unknown' when the thread clock was unreadable.
_VERDICT_TABLE: tuple[tuple[str | None, bool | None, bool | None, Verdict], ...] = (
    ('high', None, None, 'cpu_bound'),  # the loop itself consumed the time
    ('low', True, None, 'blocked'),  # little CPU, one site dominates
    ('low', False, None, 'starved'),  # little CPU, no single culprit
    ('unknown', True, None, 'blocked'),  # a dominant site is enough alone
    ('unknown', False, True, 'starved'),  # no CPU signal, but the host says pressure
    (None, None, None, 'inconclusive'),
)


def classify_episode(
    cpu_ratio: float | None,
    dominant_share: float,
    has_pressure_evidence: bool,
) -> Verdict:
    """Classify one lag episode from its three discriminating signals.

    - ``cpu_ratio``: loop-thread CPU over episode wall time (None when the
      per-thread clock was unreadable).
    - ``dominant_share``: share of non-idle stack samples landing on the
      single most-sampled call site (0.0 when nothing non-idle was seen).
    - ``has_pressure_evidence``: corroborating host signals — cgroup
      throttling during the episode, or CPU pressure (PSI).
    """
    if cpu_ratio is None:
        cpu_band = 'unknown'
    elif cpu_ratio >= CPU_BOUND_RATIO:
        cpu_band = 'high'
    elif cpu_ratio <= LOW_CPU_RATIO:
        cpu_band = 'low'
    else:
        cpu_band = 'mid'
    dominant = dominant_share >= DOMINANT_SITE_SHARE

    for band, dom, evidence, verdict in _VERDICT_TABLE:
        if band is not None and band != cpu_band:
            continue
        if dom is not None and dom != dominant:
            continue
        if evidence is not None and evidence != has_pressure_evidence:
            continue
        return verdict
    return 'inconclusive'  # unreachable: the table ends with a catch-all row


class _LagWindow:
    """Per-second {max, avg} lag aggregates in a bounded ring buffer.

    Appends happen once per tick from the heartbeat task; reads come from
    the UI server thread — everything is guarded by one uncontended lock
    (a few ns per tick when nobody is reading). Deliberately NOT guarded
    by the event loop: the /runtime/health endpoint must stay readable
    from another thread while the loop itself is stalled.
    """

    def __init__(self, window_seconds: int) -> None:
        self._lock = threading.Lock()
        # Each bucket: [epoch_second, max_lag, lag_sum, tick_count]
        self._buckets: deque[list[float]] = deque(maxlen=window_seconds)

    def add(self, wall_now: float, lag: float) -> None:
        second = int(wall_now)
        with self._lock:
            if self._buckets and self._buckets[-1][0] == second:
                bucket = self._buckets[-1]
                bucket[1] = max(bucket[1], lag)
                bucket[2] += lag
                bucket[3] += 1
            else:
                self._buckets.append([second, lag, lag, 1])

    def snapshot(self) -> list[dict[str, float]]:
        with self._lock:
            return [
                {'t': b[0], 'max_lag_ms': round(b[1] * 1000, 3), 'avg_lag_ms': round(b[2] / b[3] * 1000, 3)}
                for b in self._buckets
            ]


class _StallSamples:
    """Stacks the sampler thread captured during the current stall.

    Distinct stacks are keyed by their innermost frame (file, line) and
    collapse into a count; at most ``max_stacks`` distinct entries are
    kept. Written by the sampler thread, drained by the heartbeat task —
    lock-guarded on both sides.
    """

    def __init__(self, max_stacks: int) -> None:
        self._lock = threading.Lock()
        self._max = max_stacks
        self._stacks: dict[tuple[str, int], dict[str, Any]] = {}
        self._dropped = 0
        # Every add() counts here, including deduplicated and dropped ones —
        # the episode verdict needs "how many samples were taken", not just
        # "how many distinct sites survived".
        self._total = 0

    def add(self, frame: Any) -> None:
        key = (frame.f_code.co_filename, frame.f_lineno)
        with self._lock:
            self._total += 1
            entry = self._stacks.get(key)
            if entry is not None:
                entry['count'] += 1
                return
            if len(self._stacks) >= self._max:
                self._dropped += 1
                return
            self._stacks[key] = {
                'stack': ''.join(traceback.format_stack(frame)),
                'location': f'{frame.f_code.co_filename}:{frame.f_lineno}',
                'count': 1,
            }

    def drain(self) -> tuple[list[dict[str, Any]], int]:
        """Return (captured stacks, dropped-distinct-stack count) and reset."""
        with self._lock:
            stacks = list(self._stacks.values())
            dropped = self._dropped
            self._stacks = {}
            self._dropped = 0
            self._total = 0
        return stacks, dropped

    def peek(self) -> tuple[list[dict[str, Any]], int, int]:
        """Non-destructive view: (stack copies, dropped count, total samples).

        Used for the live current-episode snapshot, which must not steal
        the samples the episode close will aggregate.
        """
        with self._lock:
            return [dict(entry) for entry in self._stacks.values()], self._dropped, self._total


def _dominant_site_share(stacks: list[dict[str, Any]]) -> tuple[float, str | None]:
    """Share of non-idle samples on the most-sampled site, and that site.

    Samples landing in the loop's own idle wait (epoll) are excluded: they
    mean "the loop was waiting for work", which blames nobody. Returns
    ``(0.0, None)`` when nothing non-idle was sampled.
    """
    non_idle = [s for s in stacks if not any(marker in s['location'] for marker in IDLE_STACK_MARKERS)]
    total = sum(s['count'] for s in non_idle)
    if total == 0:
        return 0.0, None
    top = max(non_idle, key=lambda s: s['count'])
    return top['count'] / total, top['location']


class _Episode:
    """Running totals of one lag episode; owned by the heartbeat task.

    The sampler thread only touches ``samples`` (internally locked) and
    ``sample_count`` (via ``samples``); everything else is written by the
    heartbeat task and read by ``snapshot()`` from other threads — plain
    attributes are safe there because each is independently consistent.
    """

    def __init__(self, started_monotonic: float, started_wall: float, max_stacks: int) -> None:
        self.started_monotonic = started_monotonic
        self.started_wall = started_wall
        self.peak_lag_ms = 0.0
        self.lag_sum_ms = 0.0
        self.stall_count = 0
        self.samples = _StallSamples(max_stacks)
        # Loop-thread CPU (ms) and cgroup throttle counters at open; both
        # None when the platform cannot answer — the verdict degrades.
        self.cpu_ms_start: float | None = None
        self.throttle_start: tuple[int, int] | None = None


class RuntimeHealthMonitor:
    """Owns the heartbeat task and the sampler thread for one worker.

    Construct on the event loop, then ``start()`` / ``await stop()``
    around the worker's run. The recorder is optional: without it (UI
    disabled) the monitor still feeds Prometheus and the in-memory
    history, it just has nowhere to persist events.
    """

    def __init__(self, config: RuntimeHealthConfig, recorder: EventRecorder | None = None) -> None:
        self._config = config
        self._recorder = recorder
        self._window = _LagWindow(config.history_window_seconds)
        self._samples = _StallSamples(config.max_stall_stacks)
        self.state: HealthState = 'healthy'
        # Monotonic timestamp of the last completed heartbeat tick. Plain
        # float attribute: writes/reads are GIL-atomic, and the sampler
        # thread only ever compares it against "now" — no lock needed.
        self._heartbeat = time.monotonic()
        self._loop_thread_id: int | None = None
        self._task: asyncio.Task | None = None
        self._thread: threading.Thread | None = None
        self._thread_stop = threading.Event()
        self._clean_ticks = 0
        self._last_sample_at = 0.0
        # Recent stall summaries (without full stacks) for the snapshot
        # endpoint; bounded so a pathological day cannot grow memory.
        self._recent_stalls: deque[dict[str, Any]] = deque(maxlen=50)
        self._current_lag = 0.0
        # The open lag episode, or None while healthy. Written only by the
        # heartbeat task; the sampler thread and snapshot() read the
        # reference (GIL-atomic) and use its internally-locked pieces.
        self._episode: _Episode | None = None
        self._recent_episodes: deque[dict[str, Any]] = deque(maxlen=50)
        # Kernel thread id of the loop thread, for /proc/self/task/<tid>/stat
        # CPU reads from the sampler/UI threads. Set in start().
        self._loop_native_tid: int | None = None
        # Opt-in probe bookkeeping: capture happens on the sampler thread,
        # but recording must run on the loop — captured probes queue here
        # and the next heartbeat tick drains them.
        self._last_probe_at = 0.0
        self._pending_probes: list[dict[str, Any]] = []
        self._probe_lock = threading.Lock()
        # Last unit count the heartbeat computed; probes reuse it because
        # counting tasks from another thread is not possible.
        self._last_unit_count = -1

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        """Start the heartbeat task and sampler thread. Call on the loop."""
        self._loop_thread_id = threading.get_ident()
        self._loop_native_tid = threading.get_native_id()
        self._heartbeat = time.monotonic()
        self._task = asyncio.get_running_loop().create_task(self._run(), name='runtime-health-heartbeat')
        self._thread = threading.Thread(target=self._sampler, name='runtime-health-sampler', daemon=True)
        self._thread.start()
        runtime_health_state.set(STATE_VALUES['healthy'])
        logger.info(
            'runtime_health_started',
            category='runtime_health',
            tick_seconds=self._config.tick_seconds,
            warn_lag_seconds=self._config.warn_lag_seconds,
            stall_seconds=self._config.stall_seconds,
        )

    async def stop(self) -> None:
        """Stop the sampler thread and cancel the heartbeat task."""
        self._thread_stop.set()
        if self._thread is not None:
            # join() from the loop is acceptable: the thread's loop body is
            # one Event.wait(tick) away from observing the stop flag.
            self._thread.join(timeout=self._config.tick_seconds * 4)
            self._thread = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- heartbeat task --------------------------------------------------------

    async def _run(self) -> None:
        tick = self._config.tick_seconds
        while True:
            expected = time.monotonic() + tick
            await asyncio.sleep(tick)
            now = time.monotonic()
            lag = max(0.0, now - expected)
            self._heartbeat = now
            self._current_lag = lag
            loop_lag_seconds.observe(lag)
            wall_now = time.time()
            self._window.add(wall_now, lag)
            self._advance_state(lag, wall_now)
            self._maybe_emit_sample(wall_now, lag)
            self._drain_probes()

    def _advance_state(self, lag: float, wall_now: float) -> None:
        """One state-machine step; emits transition events and stall reports."""
        cfg = self._config
        if lag >= cfg.stall_seconds:
            # The loop just came back from a stall long enough that the
            # sampler thread saw it. Emit the stall report regardless of
            # the previous state — the stacks are already captured.
            self._emit_stall(lag, wall_now)
            self._transition('stalled', lag, wall_now)
            # A stall tick is not a clean tick.
            self._clean_ticks = 0
        elif lag >= cfg.warn_lag_seconds:
            self._clean_ticks = 0
            self._transition('degraded', lag, wall_now)
        else:
            self._clean_ticks += 1
            if self.state != 'healthy' and self._clean_ticks >= RECOVERY_TICKS:
                self._transition('healthy', lag, wall_now)
        self._advance_episode(lag, wall_now)

    # -- lag episodes ------------------------------------------------------------

    def _advance_episode(self, lag: float, wall_now: float) -> None:
        """Open, feed, cap, or close the lag episode after a state step."""
        episode = self._episode
        if self.state == 'healthy':
            if episode is not None:
                self._close_episode(episode, wall_now)
                self._episode = None
            return
        if episode is None:
            episode = self._open_episode(lag, wall_now)
            self._episode = episode
        # Clean ticks inside the recovery hysteresis keep the episode open
        # (wall time counts) but their sub-warn lags are noise, not signal.
        if lag >= self._config.warn_lag_seconds:
            episode.lag_sum_ms += lag * 1000
            episode.peak_lag_ms = max(episode.peak_lag_ms, lag * 1000)
        if lag >= self._config.stall_seconds:
            episode.stall_count += 1
        if time.monotonic() - episode.started_monotonic >= self._config.episode_max_seconds:
            # The incident outlives the cap: flush what we have (so the
            # evidence exists even if the process dies mid-incident) and
            # keep tracking in a fresh episode.
            self._close_episode(episode, wall_now)
            self._episode = self._open_episode(lag, wall_now)

    def _open_episode(self, lag: float, wall_now: float) -> _Episode:
        # The tick that OPENS the episode already carries `lag` of lateness
        # — the degradation began that long ago, so backdate the start.
        episode = _Episode(
            started_monotonic=time.monotonic() - lag,
            started_wall=wall_now - lag,
            max_stacks=self._config.max_stall_stacks,
        )
        if self._loop_native_tid is not None:
            episode.cpu_ms_start = read_thread_cpu_ms(self._loop_native_tid)
        episode.throttle_start = read_cpu_throttle()
        return episode

    def _episode_cpu(self, episode: _Episode, wall_ms: float) -> tuple[float | None, float | None]:
        """Loop-thread CPU (ms) since the episode opened, and its wall ratio."""
        if episode.cpu_ms_start is None or self._loop_native_tid is None:
            return None, None
        cpu_now = read_thread_cpu_ms(self._loop_native_tid)
        if cpu_now is None:
            return None, None
        cpu_ms = max(0.0, cpu_now - episode.cpu_ms_start)
        ratio = round(cpu_ms / wall_ms, 3) if wall_ms > 0 else None
        return round(cpu_ms, 1), ratio

    def _episode_evidence(self, episode: _Episode) -> tuple[dict[str, Any], bool]:
        """Best-effort host-pressure corroboration at episode close."""
        evidence: dict[str, Any] = {}
        throttled = False
        throttle_now = read_cpu_throttle()
        if throttle_now is not None and episode.throttle_start is not None:
            usec_delta = throttle_now[1] - episode.throttle_start[1]
            if usec_delta >= 0:
                evidence['cpu_throttled_ms'] = round(usec_delta / 1000, 1)
                throttled = usec_delta > 0
        pressure = read_pressure()
        psi_cpu = (pressure or {}).get('cpu_some_avg10')
        if psi_cpu is not None:
            evidence['psi_cpu_some_avg10'] = psi_cpu
        loadavg = read_loadavg()
        if loadavg is not None:
            evidence['load1'] = round(loadavg[0], 2)
        # "The host says pressure": we were throttled, or CPU PSI shows
        # meaningful stalling (>1% of the last 10 s).
        has_pressure = throttled or (psi_cpu is not None and psi_cpu > 1.0)
        return evidence, has_pressure

    def _close_episode(self, episode: _Episode, wall_now: float) -> None:
        duration_ms = max(0.0, (time.monotonic() - episode.started_monotonic) * 1000)
        cpu_ms, cpu_ratio = self._episode_cpu(episode, duration_ms)
        stacks, dropped = episode.samples.drain()
        sample_count = sum(s['count'] for s in stacks)
        dominant_share, top_location = _dominant_site_share(stacks)
        evidence, has_pressure = self._episode_evidence(episode)
        verdict = classify_episode(cpu_ratio, dominant_share, has_pressure)
        summary = {
            't': wall_now,
            'duration_ms': round(duration_ms, 1),
            'verdict': verdict,
            'peak_lag_ms': round(episode.peak_lag_ms, 3),
            'top_location': top_location,
        }
        self._recent_episodes.append(summary)
        logger.warning(
            'runtime_lag_episode',
            category='runtime_health',
            duration_ms=summary['duration_ms'],
            peak_lag_ms=summary['peak_lag_ms'],
            verdict=verdict,
            stall_count=episode.stall_count,
            top_location=top_location,
        )
        if self._recorder is not None:
            self._recorder.record_runtime_lag_episode(
                duration_ms=round(duration_ms, 1),
                peak_lag_ms=round(episode.peak_lag_ms, 3),
                lag_sum_ms=round(episode.lag_sum_ms, 1),
                verdict=verdict,
                stall_count=episode.stall_count,
                sample_count=sample_count,
                stacks=stacks,
                dropped_stacks=dropped,
                unit_count=len(asyncio.all_tasks()),
                cpu_ms=cpu_ms,
                cpu_ratio=cpu_ratio,
                evidence=evidence,
            )

    def _transition(self, new_state: HealthState, lag: float, wall_now: float) -> None:
        if new_state == self.state:
            return
        old_state = self.state
        self.state = new_state
        runtime_health_state.set(STATE_VALUES[new_state])
        log = logger.warning if new_state != 'healthy' else logger.info
        log(
            'runtime_health_transition',
            category='runtime_health',
            old_state=old_state,
            new_state=new_state,
            lag_ms=round(lag * 1000, 3),
        )
        if self._recorder is not None:
            self._recorder.record_runtime_health(
                kind='transition',
                state=new_state,
                lag_ms=round(lag * 1000, 3),
                unit_count=len(asyncio.all_tasks()),
            )

    def _maybe_emit_sample(self, wall_now: float, lag: float) -> None:
        if wall_now - self._last_sample_at < self._config.sample_interval_seconds:
            return
        self._last_sample_at = wall_now
        self._last_unit_count = len(asyncio.all_tasks())
        if self._recorder is not None:
            self._recorder.record_runtime_health(
                kind='sample',
                state=self.state,
                lag_ms=round(lag * 1000, 3),
                unit_count=self._last_unit_count,
            )

    def _emit_stall(self, lag: float, wall_now: float) -> None:
        stacks, dropped = self._samples.drain()
        runtime_stalls.inc()
        summary = {
            't': wall_now,
            'duration_ms': round(lag * 1000, 3),
            'stack_count': len(stacks),
            'top_location': stacks[0]['location'] if stacks else None,
        }
        self._recent_stalls.append(summary)
        logger.warning(
            'runtime_stall',
            category='runtime_health',
            duration_ms=summary['duration_ms'],
            stack_count=len(stacks),
            top_location=summary['top_location'],
        )
        if self._recorder is not None:
            self._recorder.record_runtime_stall(
                duration_ms=round(lag * 1000, 3),
                stacks=stacks,
                dropped_stacks=dropped,
                unit_count=len(asyncio.all_tasks()),
            )

    # -- sampler thread --------------------------------------------------------

    def _sampler(self) -> None:
        """Thread body: watch heartbeat age; capture stacks only while stalled."""
        while not self._thread_stop.wait(self._config.tick_seconds):
            self._sample_once()

    def _sample_once(self) -> None:
        """One sampler-thread iteration, split out so tests can drive it
        with a synthetic heartbeat instead of real wall-clock stalls."""
        now = time.monotonic()
        stalled_now = now - self._heartbeat >= self._config.stall_seconds
        episode = self._episode
        probe_due = (
            self._config.probe_interval_seconds > 0 and now - self._last_probe_at >= self._config.probe_interval_seconds
        )
        if not stalled_now and episode is None and not probe_due:
            return
        frame = sys._current_frames().get(self._loop_thread_id or -1)
        if frame is None:
            return
        if stalled_now:
            # The loop is stalled RIGHT NOW — this thread is the only place
            # that can see what is blocking it.
            self._samples.add(frame)
        if episode is not None:
            # During an episode every wakeup samples, stale heartbeat or
            # not: diffuse degradation never trips the stall threshold,
            # and these aggregated samples are its only stack evidence.
            episode.samples.add(frame)
        if probe_due:
            self._last_probe_at = now
            self._capture_probe(frame)

    def _capture_probe(self, frame: Any) -> None:
        """Format one opt-in probe; the heartbeat task records it later.

        Recording must not happen on this thread — the recorder's buffers
        belong to the event loop — so probes queue under a lock and
        ``_drain_probes`` writes them from the next heartbeat tick. During
        a hard stall the probe therefore lands when the loop resumes,
        which is also when the recorder can accept it.
        """
        probe = {
            'lag_ms': round(max(0.0, time.monotonic() - self._heartbeat) * 1000, 3),
            'unit_count': self._last_unit_count,
            'stacks': [
                {
                    'stack': ''.join(traceback.format_stack(frame)),
                    'location': f'{frame.f_code.co_filename}:{frame.f_lineno}',
                    'count': 1,
                }
            ],
        }
        with self._probe_lock:
            self._pending_probes.append(probe)

    def _drain_probes(self) -> None:
        """Record queued probes; runs on the heartbeat task (the loop)."""
        with self._probe_lock:
            probes, self._pending_probes = self._pending_probes, []
        if self._recorder is None:
            return
        for probe in probes:
            self._recorder.record_runtime_probe(
                lag_ms=probe['lag_ms'],
                unit_count=probe['unit_count'],
                stacks=probe['stacks'],
            )

    # -- read surface ----------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Current state + lag history for the /runtime/health endpoint.

        Thread-safe and loop-free on purpose: it must answer from the UI
        server thread even while the event loop is stalled. ``unit_count``
        is therefore -1 here (counting tasks needs the loop); the census
        endpoint reports exact numbers when the loop is responsive.
        """
        heartbeat_age = time.monotonic() - self._heartbeat
        return {
            'state': ('stalled' if heartbeat_age >= self._config.stall_seconds else self.state),
            'unit_label': UNIT_LABEL,
            'current_lag_ms': round(self._current_lag * 1000, 3),
            'heartbeat_age_ms': round(heartbeat_age * 1000, 3),
            'window': self._window.snapshot(),
            'recent_stalls': list(self._recent_stalls),
            'current_episode': self._current_episode_view(),
            'recent_episodes': list(self._recent_episodes),
        }

    def brief_state(self) -> tuple[HealthState, float]:
        """``(state, current_lag_ms)`` without copying the lag window.

        For the recorder's worker_state sync, which runs every tick and
        must stay cheap; the heartbeat-age override matches snapshot().
        """
        heartbeat_age = time.monotonic() - self._heartbeat
        state: HealthState = 'stalled' if heartbeat_age >= self._config.stall_seconds else self.state
        return state, round(self._current_lag * 1000, 3)

    def _current_episode_view(self) -> dict[str, Any] | None:
        """The open episode with a running verdict, or None while healthy.

        This is what lets the UI say "episode in progress — starved" DURING
        the incident: everything here reads monitor memory and /proc, never
        the loop or the recorder database.
        """
        episode = self._episode
        if episode is None:
            return None
        wall_ms = max(0.0, (time.monotonic() - episode.started_monotonic) * 1000)
        cpu_ms, cpu_ratio = self._episode_cpu(episode, wall_ms)
        stacks, _dropped, _total = episode.samples.peek()
        sample_count = sum(s['count'] for s in stacks)
        dominant_share, _top = _dominant_site_share(stacks)
        # Evidence probes at snapshot time would cost /proc reads per UI
        # poll; the running verdict settles for the cheap signals and the
        # close-time verdict adds the pressure corroboration.
        verdict = classify_episode(cpu_ratio, dominant_share, False)
        return {
            'started_t': episode.started_wall,
            'wall_ms': round(wall_ms, 1),
            'peak_lag_ms': round(episode.peak_lag_ms, 3),
            'cpu_ms': cpu_ms,
            'sample_count': sample_count,
            'verdict': verdict,
        }


def task_census() -> list[dict[str, Any]]:
    """Group live asyncio tasks by (coroutine, suspension point).

    Must run on the event loop (``asyncio.all_tasks`` requires it) — the
    debug endpoint dispatches here and turns a dispatch timeout into an
    HTTP 503, which is itself a signal: a census that cannot run means the
    loop is not serving coroutines at all.
    """
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for task in asyncio.all_tasks():
        coro = task.get_coro()
        name = getattr(coro, '__qualname__', None) or type(coro).__name__
        frames = task.get_stack(limit=1)
        location = ''
        if frames:
            frame = frames[0]
            location = f'{frame.f_code.co_filename}:{frame.f_lineno}'
        key = (name, location)
        entry = groups.get(key)
        if entry is None:
            groups[key] = {'name': name, 'location': location, 'count': 1, 'example': task.get_name()}
        else:
            entry['count'] += 1
    return sorted(groups.values(), key=lambda g: -g['count'])
