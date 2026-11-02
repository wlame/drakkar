"""The handler's optional ``task_run_seconds`` hook: call it and check its answer.

The framework measures every task's run time itself (spawn to exit, off the
event loop). A handler may report a better number for the timeline bar —
see :meth:`drakkar.handler.BaseDrakkarHandler.task_run_seconds`. Every
answer that is not a valid run time falls back to the framework measure;
nothing here can fail a task.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable

import structlog

from drakkar.metrics import handler_hook_errors
from drakkar.models import ExecutorResult
from drakkar.providers.resilience import RateLimitedLog

logger = structlog.get_logger()

HOOK_NAME = 'task_run_seconds'

# A reported time may exceed the framework measure by this much: the
# binary's own clock and the framework's are read at different moments, and
# the framework rounds to milliseconds. More than that cannot be a run time
# of this process.
REPORTED_RUN_TOLERANCE_SECONDS = 0.05

# One line per 60 s for the whole process, however many partitions call the hook.
_failure_log = RateLimitedLog(time.monotonic)

RunSecondsHook = Callable[[ExecutorResult], float | None]


def reported_run_seconds(hook: RunSecondsHook | None, result: ExecutorResult) -> float | None:
    """The hook's run time for ``result``, or None to use the framework measure.

    ``hook`` is None when the handler keeps the default, and the call is then
    skipped. A result with no process (a precomputed result, a launch
    failure) is never passed to the hook.
    """
    if hook is None or result.pid is None:
        return None
    try:
        value = hook(result)
    except Exception as exc:
        _hook_failed(result, f'{type(exc).__name__}: {exc}')
        return None
    if value is None:
        return None
    if not _is_run_time(value, result.duration_seconds):
        _hook_failed(result, f'value out of range: {value!r}')
        return None
    return float(value)


def _is_run_time(value: object, duration_seconds: float) -> bool:
    """A finite number from 0 to the framework measure plus the tolerance (a bool is not a number here)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    return math.isfinite(value) and 0 <= value <= duration_seconds + REPORTED_RUN_TOLERANCE_SECONDS


def _hook_failed(result: ExecutorResult, error: str) -> None:
    """Count the failure and log it, at most one line per 60 s."""
    handler_hook_errors.labels(hook=HOOK_NAME).inc()
    suppressed = _failure_log.admit(HOOK_NAME)
    if suppressed is not None:
        logger.warning(
            'task_run_seconds_hook_failed',
            category='handler',
            task_id=result.task.task_id,
            error=error,
            suppressed=suppressed,
        )
