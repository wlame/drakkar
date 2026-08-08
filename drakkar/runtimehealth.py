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

    def add(self, frame: Any) -> None:
        key = (frame.f_code.co_filename, frame.f_lineno)
        with self._lock:
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
        return stacks, dropped


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

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        """Start the heartbeat task and sampler thread. Call on the loop."""
        self._loop_thread_id = threading.get_ident()
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
            return
        if lag >= cfg.warn_lag_seconds:
            self._clean_ticks = 0
            self._transition('degraded', lag, wall_now)
            return
        self._clean_ticks += 1
        if self.state != 'healthy' and self._clean_ticks >= RECOVERY_TICKS:
            self._transition('healthy', lag, wall_now)

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
        if self._recorder is not None:
            self._recorder.record_runtime_health(
                kind='sample',
                state=self.state,
                lag_ms=round(lag * 1000, 3),
                unit_count=len(asyncio.all_tasks()),
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
        if time.monotonic() - self._heartbeat < self._config.stall_seconds:
            return
        # The loop is stalled RIGHT NOW — this thread is the only place
        # that can see what is blocking it.
        frame = sys._current_frames().get(self._loop_thread_id or -1)
        if frame is not None:
            self._samples.add(frame)

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
