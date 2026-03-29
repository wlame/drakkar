"""Periodic task decorator and scheduler for Drakkar handlers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal

import structlog

logger = structlog.get_logger()

PERIODIC_ATTR = '_periodic_meta'


@dataclass(frozen=True, slots=True)
class PeriodicMeta:
    """Metadata attached to a handler method by the @periodic decorator."""

    seconds: float
    on_error: Literal['continue', 'stop']


def periodic(
    seconds: float,
    *,
    on_error: Literal['continue', 'stop'] = 'continue',
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
    """Mark a handler coroutine as a periodic task.

    The framework discovers decorated methods at startup and schedules
    them to run every ``seconds`` in the event loop. Overlapping runs
    are prevented — the next interval starts only after the current
    invocation finishes.

    Args:
        seconds: Interval between invocations. Must be positive.
        on_error: What to do when the coroutine raises an exception.
            ``"continue"`` (default) — log the error, keep scheduling.
            ``"stop"`` — log the error, cancel this periodic task.
    """
    if seconds <= 0:
        raise ValueError(f"periodic seconds must be positive, got {seconds}")

    def decorator(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
        if not asyncio.iscoroutinefunction(fn):
            raise TypeError(f"@periodic can only decorate async functions, got {fn!r}")
        setattr(fn, PERIODIC_ATTR, PeriodicMeta(seconds=seconds, on_error=on_error))
        return fn

    return decorator


def discover_periodic_tasks(handler: object) -> list[tuple[str, Callable[..., Coroutine[Any, Any, Any]], PeriodicMeta]]:
    """Inspect a handler instance and return all @periodic-decorated methods."""
    tasks: list[tuple[str, Callable[..., Coroutine[Any, Any, Any]], PeriodicMeta]] = []
    for name in dir(handler):
        attr = getattr(handler, name, None)
        if attr is None:
            continue
        meta = getattr(attr, PERIODIC_ATTR, None)
        if isinstance(meta, PeriodicMeta):
            tasks.append((name, attr, meta))
    return tasks


async def run_periodic_task(
    name: str,
    coro_fn: Callable[[], Coroutine[Any, Any, Any]],
    seconds: float,
    on_error: Literal['continue', 'stop'],
) -> None:
    """Run a single periodic task in a loop until cancelled."""
    log = logger.bind(periodic_task=name, interval_seconds=seconds)
    await log.ainfo("periodic_task_started", category='periodic')

    while True:
        await asyncio.sleep(seconds)
        try:
            await coro_fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            await log.aexception("periodic_task_failed", category='periodic')
            if on_error == 'stop':
                await log.awarning("periodic_task_stopped", category='periodic')
                return
