"""Shared utility functions for Drakkar framework."""

import asyncio
import math
import re
import time
from collections.abc import Awaitable, Callable


def redact_url(url: str) -> str:
    """Redact credentials from URIs. Replaces user:pass@ with ***:***@."""
    return re.sub(r'://[^@/]+@', '://***:***@', url)


async def wait_for_aligned_startup(
    min_wait_seconds: float,
    align_interval_seconds: int,
    *,
    _clock: Callable[[], float] | None = None,
    _sleep: Callable[[float], Awaitable[None]] | None = None,
) -> float:
    """Sleep until a wall-clock boundary so a fleet of workers aligns on one moment.

    Sequence:
      1. Sleep ``min_wait_seconds`` — buffer for slow init (DB connects,
         schema migrations, cache warm-up, etc.) before we're ready to
         subscribe to Kafka.
      2. Sleep until the next Unix-epoch second that is a multiple of
         ``align_interval_seconds``. For the default interval of 10,
         that's :00/:10/:20/:30/:40/:50 in wall-clock (timezone offsets
         are always whole minutes so second-of-minute alignment is
         identical in UTC and local time).

    Rationale: during a rolling deploy, workers come up one at a time.
    Each fresh subscribe triggers a Kafka consumer-group rebalance that
    stalls consumption on all other workers. Converging on a shared
    boundary collapses N rebalances into 1.

    Returns the total seconds actually slept.

    ``_clock`` and ``_sleep`` are injection hooks for tests; production
    callers should not set them. ``_clock()`` must return current unix
    seconds (like ``time.time()``); ``_sleep(seconds)`` must be an async
    sleep (like ``asyncio.sleep``).
    """
    # time.time and asyncio.sleep are the real-world defaults; tests
    # inject fakes to avoid sitting through 10-second waits.
    clock = _clock if _clock is not None else time.time
    sleep = _sleep if _sleep is not None else asyncio.sleep

    start = clock()
    if min_wait_seconds > 0:
        await sleep(min_wait_seconds)

    # ceil(now / interval) * interval — the earliest boundary that is
    # >= now. If we land EXACTLY on a boundary, stay there (zero extra
    # wait) so an on-time worker doesn't skip a whole interval.
    now = clock()
    target = math.ceil(now / align_interval_seconds) * align_interval_seconds
    remaining = target - now
    if remaining > 0:
        await sleep(remaining)
    return clock() - start
