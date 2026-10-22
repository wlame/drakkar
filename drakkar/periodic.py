"""Periodic task decorator and scheduler for Drakkar handlers.

A periodic task has one schedule: an ``interval`` in seconds or a five-field
UTC ``cron`` expression (the subset :mod:`drakkar.cronexpr` accepts). Its
``scope`` says where it runs: ``worker`` tasks run on every worker, ``cluster``
tasks on one worker of the cluster per slot (see :mod:`drakkar.schedule`).
"""

from __future__ import annotations

import asyncio
import inspect
import math
import re
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, get_args

import structlog

from drakkar.cronexpr import CRON_FIELDS_MESSAGE, next_cron_fire, validate_cron
from drakkar.metrics import periodic_task_duration, periodic_task_runs
from drakkar.schedule import MAX_LEASE_SECONDS, MIN_CLUSTER_INTERVAL_SECONDS, MIN_LEASE_SECONDS, TASK_NAME_PATTERN

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

PERIODIC_ATTR = '_periodic_meta'

PeriodicScope = Literal['worker', 'cluster']
OnErrorPolicy = Literal['continue', 'stop']
PERIODIC_SCOPES: tuple[str, ...] = get_args(PeriodicScope)

PREFIX_DECORATOR = "periodic task '{name}': "
RULE_NAME = 'the name must be 1 to 64 characters of a-z, 0-9 and _, and must not start with a digit'
RULE_SCOPE = 'scope must be worker or cluster'
RULE_ONE_SCHEDULE = 'give exactly one of interval and cron'
RULE_WORKER_INTERVAL = 'interval must be greater than 0'
RULE_CLUSTER_INTERVAL = "a cluster task's interval must be a whole number of seconds and at least 30"
RULE_LEASE_SCOPE = 'lease_seconds is only for cluster tasks'
RULE_LEASE_BOUNDS = 'lease_seconds must be a whole number from 40 to 86400'

# The texts of the config checks: the task overrides under ``periodic.tasks`` and ``history.archive``.
PREFIX_OVERRIDE = 'periodic.tasks.{name}: '
PREFIX_ARCHIVE = 'history.archive: '
RULE_AT_MOST_ONE = 'give at most one of interval and cron'
RULE_ARCHIVE_INTERVAL = 'interval must be a whole number of seconds and at least 30'
RULE_SWEEP_AFTER = 'sweep_after_seconds must be at least 60'
RULE_DOT_NAME = 'a name with a dot is a system task; configure it in its own section (cache.*, history.archive)'
# Filled with ``str.replace``, not ``format``: the text carries a literal ``{}``.
OVERRIDE_NULL = 'periodic.tasks.{name} has no value; write {} to keep the declared settings'

_TASK_NAME = re.compile(TASK_NAME_PATTERN)


class PeriodicRuleError(ValueError):
    """A periodic task declaration breaks one of the periodic rules."""


@dataclass(frozen=True, slots=True)
class PeriodicMeta:
    """Metadata attached to a handler method by the @periodic decorator."""

    interval: float | None
    cron: str | None
    scope: PeriodicScope
    lease_seconds: int | None
    on_error: OnErrorPolicy


class Clock(Protocol):
    """The wall clock and the sleep a periodic loop uses; tests pass a fake one."""

    def time(self) -> float:
        """Seconds since the epoch."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait ``seconds``."""
        ...


class _SystemClock:
    """The real clock: ``time.time`` and ``asyncio.sleep``."""

    def time(self) -> float:
        """Seconds since the epoch."""
        return time.time()

    async def sleep(self, seconds: float) -> None:
        """Wait ``seconds`` on the event loop."""
        await asyncio.sleep(seconds)


SYSTEM_CLOCK: Clock = _SystemClock()


def _as_number(value: object) -> float | None:
    """``value`` when it is an int or a float, else ``None`` (a bool is not a number here)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return value


def _as_whole_number(value: object) -> float | None:
    """``value`` when it is a number without a fractional part (``30`` and ``30.0``, not ``30.5``), else ``None``."""
    number = _as_number(value)
    return number if number is not None and float(number).is_integer() else None


def _check_interval(interval: object, scope: str) -> str | None:
    """The interval rule of ``scope``: positive for a worker task, whole and at least 30 for a cluster task."""
    if scope == 'worker':
        number = _as_number(interval)
        return None if number is not None and number > 0 else RULE_WORKER_INTERVAL
    whole = _as_whole_number(interval)
    return None if whole is not None and whole >= MIN_CLUSTER_INTERVAL_SECONDS else RULE_CLUSTER_INTERVAL


def _check_lease(lease_seconds: object, scope: str) -> str | None:
    """The lease rules: only a cluster task takes one, as a whole number from 40 to 86400."""
    if scope != 'cluster':
        return RULE_LEASE_SCOPE
    whole = _as_whole_number(lease_seconds)
    return None if whole is not None and MIN_LEASE_SECONDS <= whole <= MAX_LEASE_SECONDS else RULE_LEASE_BOUNDS


def check_periodic_rules(
    *,
    name: str,
    interval: float | None,
    cron: str | None,
    scope: str,
    lease_seconds: int | None,
    now_epoch: int,
) -> str | None:
    """Return the first rule a periodic task declaration breaks, without prefix, or ``None``.

    Order: the name, the scope, exactly one schedule, then the interval rule
    of the scope or the cron check (one fire-time lookup from ``now_epoch``),
    then the lease rules. A given ``0`` counts as given: ``interval=0`` breaks
    the interval rule and ``lease_seconds=0`` the lease rules. A given empty
    ``cron`` is checked like any other text, and a ``cron`` that is not text
    gets the field-count message.
    """
    if not isinstance(name, str) or _TASK_NAME.fullmatch(name) is None:
        return RULE_NAME
    if scope not in PERIODIC_SCOPES:
        return RULE_SCOPE
    if (interval is None) == (cron is None):
        return RULE_ONE_SCHEDULE
    if cron is None:
        message = _check_interval(interval, scope)
    elif not isinstance(cron, str):
        message = CRON_FIELDS_MESSAGE.format(expr=cron)
    else:
        message = validate_cron(cron, now_epoch=now_epoch)
    if message is not None:
        return message
    if lease_seconds is not None:
        return _check_lease(lease_seconds, scope)
    return None


def periodic(
    interval: float | None = None,
    *,
    cron: str | None = None,
    scope: PeriodicScope = 'worker',
    lease_seconds: int | None = None,
    on_error: OnErrorPolicy = 'continue',
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
    """Mark a handler coroutine as a periodic task.

    The framework discovers decorated methods at startup and schedules them.
    Runs of one task never overlap.

    Args:
        interval: Seconds between runs. A worker task sleeps this long after
            each run ends. A cluster task runs at the slots that are whole
            multiples of the interval since the epoch; its interval is a
            whole number of seconds, at least 30.
        cron: A five-field UTC cron expression, instead of ``interval``. The
            task runs at its fire times; a fire time that passes during a run
            is skipped.
        scope: ``"worker"`` (default) runs the task on every worker;
            ``"cluster"`` runs each slot on one worker of the cluster.
        lease_seconds: Cluster tasks only: the length of the election lease,
            a whole number from 40 to 86400 (the default lease applies when
            it is not given).
        on_error: What to do when the coroutine raises an exception.
            ``"continue"`` (default) — log the error, keep scheduling.
            ``"stop"`` — log the error, cancel this periodic task.

    Raises:
        PeriodicRuleError: when the decorated method breaks a rule; the
            message names the method.
        TypeError: when the decorated function is not a coroutine function.
    """

    def decorator(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f'@periodic can only decorate async functions, got {fn!r}')
        # Discovery finds the task under its attribute name, which is the function's name.
        name = getattr(fn, '__name__', repr(fn))
        rule = check_periodic_rules(
            name=name,
            interval=interval,
            cron=cron,
            scope=scope,
            lease_seconds=lease_seconds,
            now_epoch=int(time.time()),
        )
        if rule is not None:
            raise PeriodicRuleError(PREFIX_DECORATOR.format(name=name) + rule)
        meta = PeriodicMeta(interval=interval, cron=cron, scope=scope, lease_seconds=lease_seconds, on_error=on_error)
        setattr(fn, PERIODIC_ATTR, meta)
        return fn

    return decorator


def discover_periodic_tasks(handler: object) -> list[tuple[str, Callable[..., Coroutine[Any, Any, Any]], PeriodicMeta]]:
    """Inspect a handler instance and return all @periodic-decorated methods."""
    tasks: list[tuple[str, Callable[..., Coroutine[Any, Any, Any]], PeriodicMeta]] = []
    for name in dir(handler):
        try:
            attr = getattr(handler, name, None)
        except Exception:
            continue
        if attr is None:
            continue
        meta = getattr(attr, PERIODIC_ATTR, None)
        if isinstance(meta, PeriodicMeta):
            tasks.append((name, attr, meta))
    return tasks


async def _run_counted(
    name: str,
    coro_fn: Callable[[], Awaitable[Any]],
    recorder: EventRecorder | None,
    system: bool,
    slot: int | None,
) -> Exception | None:
    """Run the task once, count it in the metrics and the recorder; return the error it raised.

    The ok-path metrics and recorder row are inside the ``try``: an error
    while counting a run is counted as a failed run, like an error of the task.
    """
    start = time.monotonic()
    try:
        await coro_fn()
        duration = time.monotonic() - start
        periodic_task_runs.labels(name=name, status='ok').inc()
        periodic_task_duration.labels(name=name).observe(duration)
        if recorder:
            recorder.record_periodic_run(name=name, duration=round(duration, 3), status='ok', system=system, slot=slot)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        duration = time.monotonic() - start
        periodic_task_runs.labels(name=name, status='error').inc()
        periodic_task_duration.labels(name=name).observe(duration)
        if recorder:
            recorder.record_periodic_run(
                name=name,
                duration=round(duration, 3),
                status='error',
                error=str(exc),
                system=system,
                slot=slot,
            )
        return exc
    return None


async def run_task_once(
    name: str,
    coro_fn: Callable[[], Awaitable[Any]],
    *,
    recorder: EventRecorder | None,
    system: bool,
    on_error: OnErrorPolicy,
    log: Any,
    slot: int | None = None,
) -> bool:
    """Run one periodic task once and apply its error policy; True when its loop must end.

    Every periodic loop runs its task through this: the run metrics, the
    recorder row (``slot`` given for a cluster task's run) and the
    ``on_error`` policy. A failed run logs ``periodic_task_failed`` on
    ``log``; with ``on_error='stop'`` it also logs ``periodic_task_stopped``
    and returns True.
    """
    error = await _run_counted(name, coro_fn, recorder, system, slot)
    if error is None:
        return False
    await log.aexception('periodic_task_failed', category='periodic', exc_info=error)
    if on_error != 'stop':
        return False
    await log.awarning('periodic_task_stopped', category='periodic')
    return True


async def sleep_until(clock: Clock, moment: float) -> None:
    """Sleep until the clock reaches ``moment``; a sleep that wakes early sleeps again."""
    while (now := clock.time()) < moment:
        await clock.sleep(moment - now)


async def run_periodic_task(
    name: str,
    coro_fn: Callable[[], Coroutine[Any, Any, Any]],
    *,
    interval: float | None = None,
    cron: str | None = None,
    on_error: OnErrorPolicy,
    recorder: EventRecorder | None = None,
    system: bool = False,
    clock: Clock = SYSTEM_CLOCK,
) -> None:
    """Run a single worker-scope periodic task in a loop until cancelled.

    With ``interval`` the loop sleeps that long, runs the task, and sleeps
    again from the end of the run. With ``cron`` it sleeps until the next fire
    time after the current second, runs the task, and computes the next fire
    time from the end of the run, so fire times that pass during a run are
    skipped. A fire time never runs twice, even when the clock steps back.

    The ``system`` flag marks the task as framework-internal (e.g. cache
    flush/sync/cleanup loops) rather than a user-defined ``@periodic`` handler
    method. It propagates to ``EventRecorder.record_periodic_run`` so the
    debug UI can render a "[system]" badge next to these rows.

    Raises:
        ValueError: when not exactly one of ``interval`` and ``cron`` is given.
    """
    if (interval is None) == (cron is None):
        raise ValueError(f"periodic task '{name}' needs exactly one of interval and cron")
    schedule: dict[str, float | str] = {'interval_seconds': interval} if interval is not None else {'cron': cron or ''}
    log = logger.bind(periodic_task=name, **schedule)
    await log.ainfo('periodic_task_started', category='periodic', scope='worker')

    last_fire = 0
    while True:
        if interval is not None:
            await clock.sleep(interval)
        else:
            # max() keeps a clock that stepped back from returning the fire time just run.
            last_fire = next_cron_fire(cron or '', max(math.floor(clock.time()), last_fire))
            await sleep_until(clock, last_fire)
        if await run_task_once(name, coro_fn, recorder=recorder, system=system, on_error=on_error, log=log):
            return
