"""Task cost, speed, and windowed throughput (contract v1.16).

The operator names a numeric task label (set in ``arrange()``) whose value
correlates with the task's computational hardness — bytes parsed, a
computed score, any unit. From it the framework derives:

- per-task **speed** = cost / duration (cost-units per second), attached to
  the ``task_completed`` event and the recent-tasks API;
- sliding-window **throughput** = sum(cost completed in the last N seconds)
  / N, plus **task_rate** (count / N) and **tasks** (count), for the fixed
  window set N in {1, 5, 30} — pushed once per second as a
  broadcast-only ``throughput`` WS frame, exported as Prometheus gauges,
  and snapshotted into worker_state rows.

Counting rules (the contract's normative list): the feature is configured,
the task completed successfully, it is not a precomputed fast-track task,
the label parses as a finite number, ``cost >= min_cost``, and
``duration > 0``. Excluded tasks carry no cost/speed anywhere — absent,
never zeroed.

Everything here runs on the event loop (completions are processed there and
the emit loop is an asyncio task), so no locking is needed.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.metrics import task_rate_gauge, task_speed, throughput_gauge

if TYPE_CHECKING:
    from drakkar.config import ThroughputConfig
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# The fixed window set (seconds). Every WS frame and worker_state snapshot
# carries all three, so the UI can switch windows client-side with no
# reconfiguration; the contract pins the keys.
WINDOW_SECONDS = (1, 5, 30)
_MAX_WINDOW = WINDOW_SECONDS[-1]

# The emit loop's cadence: the 1 s window needs 1 Hz to mean anything.
EMIT_INTERVAL_SECONDS = 1.0


def parse_cost(labels: dict[str, str] | None, config: ThroughputConfig) -> float | None:
    """The task's cost per config, or None when it must not be counted.

    None covers every label-side exclusion: feature off, label absent,
    value unparseable or non-finite, or below ``min_cost``. The caller owns
    the task-side exclusions (success, not precomputed, duration > 0).
    """
    if not config.cost_label:
        return None
    raw = (labels or {}).get(config.cost_label)
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < config.min_cost:
        return None
    return value


class ThroughputTracker:
    """Sliding-window throughput over counted task completions.

    Construct when ``throughput.cost_label`` is configured; feed it from
    the task-completion path via :meth:`observe_completion`; ``start()`` /
    ``await stop()`` bracket the 1 s emit loop. The recorder is optional —
    without it (UI disabled) Prometheus and worker_state still work, the
    WS frame simply has nowhere to go.
    """

    def __init__(self, config: ThroughputConfig, recorder: EventRecorder | None = None) -> None:
        self._config = config
        self._recorder = recorder
        # (monotonic completion time, cost) pairs, oldest first, trimmed to
        # the largest window on every append and every stats read.
        self._completions: deque[tuple[float, float]] = deque()
        self._task: asyncio.Task | None = None

    def observe_completion(
        self,
        labels: dict[str, str] | None,
        duration_seconds: float,
        now: float | None = None,
    ) -> tuple[float, float] | None:
        """Count one successful, non-precomputed completion.

        Returns ``(cost, speed)`` for the recorder to attach to the
        ``task_completed`` metadata, or None when the task is excluded
        (no cost label match, below min_cost, or zero duration).
        """
        cost = parse_cost(labels, self._config)
        if cost is None or duration_seconds <= 0:
            return None
        speed = cost / duration_seconds
        if now is None:
            now = time.monotonic()
        self._completions.append((now, cost))
        self._evict(now)
        task_speed.observe(speed)
        return cost, speed

    def _evict(self, now: float) -> None:
        while self._completions and self._completions[0][0] < now - _MAX_WINDOW:
            self._completions.popleft()

    def window_stats(self, now: float | None = None) -> dict[str, dict[str, Any]]:
        """The three-window aggregate, keyed by the window's seconds as a string.

        Quiet windows report zeros rather than disappearing: an idle or
        stalled worker must draw as a real dip on the UI track, not a gap.
        """
        if now is None:
            now = time.monotonic()
        self._evict(now)
        stats: dict[str, dict[str, Any]] = {}
        # One pass per window over an already-bounded deque; at the target
        # rate (~1k tasks/s x 30 s) this is a few tens of thousands of
        # float compares per second in C — negligible next to the pipeline.
        for window in WINDOW_SECONDS:
            cutoff = now - window
            cost_sum = 0.0
            count = 0
            for completed_at, cost in reversed(self._completions):
                if completed_at < cutoff:
                    break
                cost_sum += cost
                count += 1
            stats[str(window)] = {
                'throughput': round(cost_sum / window, 3),
                'task_rate': round(count / window, 3),
                'tasks': count,
            }
        return stats

    # -- emit loop -------------------------------------------------------------

    def start(self, recorder: EventRecorder | None = None) -> None:
        """Start the 1 s emit loop. Call on the event loop."""
        if recorder is not None:
            self._recorder = recorder
        self._task = asyncio.get_running_loop().create_task(self._emit_loop(), name='throughput-emit')
        logger.info(
            'throughput_started',
            category='throughput',
            cost_label=self._config.cost_label,
            min_cost=self._config.min_cost,
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _emit_loop(self) -> None:
        while True:
            await asyncio.sleep(EMIT_INTERVAL_SECONDS)
            # Same per-iteration guard as the recorder loops: one bad tick
            # must not end the feed for the rest of the run.
            try:
                self.emit_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    'throughput_emit_failed',
                    category='throughput',
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    def emit_once(self) -> None:
        """One tick: refresh the gauges and broadcast the WS frame."""
        stats = self.window_stats()
        for window, values in stats.items():
            throughput_gauge.labels(window=window).set(values['throughput'])
            task_rate_gauge.labels(window=window).set(values['task_rate'])
        if self._recorder is not None:
            self._recorder.broadcast_throughput(stats)
