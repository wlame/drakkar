"""Cross-thread asyncio dispatch helper.

This module owns ``dispatch_to_loop(coro, target_loop)`` — the single
shared mechanism the framework uses to bridge work between two
``asyncio`` event loops running on different threads.

Why this helper exists
----------------------

Drakkar's main pipeline (``DrakkarApp`` + ``AppLifecycle``) runs on the
main thread's event loop. Several auxiliary servers (the debug FastAPI
UI; soon the webapp HTTP endpoint) run on their own daemon threads, each
with its own loop, so heavy UI requests or sync HTTP calls don't block
Kafka polling, the executor pool, or sink flushes.

That split has well-known ``asyncio`` pitfalls:

1. **Loop binding for asyncio primitives.** ``asyncio.Lock``,
   ``asyncio.Event``, ``aiosqlite`` connections, etc., are bound at
   construction time to whichever loop is running. Awaiting them on a
   different loop raises ``RuntimeError: <primitive> is bound to a
   different event loop`` under contention. Anything that wraps such a
   primitive (the recorder's writer connection, ``ExecutorPool._gate``,
   the cache reader) MUST execute on its owning loop.

2. **Cancellation propagation.** ``concurrent.futures.Future.cancel()``
   does NOT propagate into the running coroutine on the target loop the
   way ``asyncio.Task.cancel()`` does. ``asyncio.wrap_future`` bridges
   the two so that cancelling the awaitable on the caller side
   propagates a ``CancelledError`` into the coroutine running on the
   target loop.

3. **Exception forwarding.** Exceptions raised inside the coroutine on
   the target loop must surface to the caller. Both
   ``run_coroutine_threadsafe`` and ``wrap_future`` re-raise on the
   awaiting side, but only when used together — the caller awaiting the
   wrapped future sees the original exception type and traceback.

The function is intentionally minimal: it does NOT manage the target
loop's lifecycle (the caller owns the thread + loop), it does NOT spawn
threads, and it does NOT log. It's a thin, named helper around the
``run_coroutine_threadsafe`` + ``wrap_future`` pair plus a same-loop
short-circuit so unit tests that share one loop don't pay the
cross-thread tax.

Usage
-----

>>> from drakkar.concurrency import dispatch_to_loop
>>> result = await dispatch_to_loop(some_coro(), target_loop=app.main_loop)

Same-loop short-circuit: when ``target_loop`` is the loop currently
running (typical in unit tests that skip the background thread, and the
tight inner loop where producer and consumer share a single loop), the
coroutine is awaited inline — no future, no cross-thread hop. This also
covers the case where ``target_loop`` is a ``MagicMock`` in tests; the
``isinstance`` check filters mocks out and falls back to inline await.

The parameter name is ``target_loop`` — symmetric, no fixed direction.
The same helper is used for "endpoint loop -> main loop" (debug UI
reading ``aiosqlite``) AND for "endpoint loop -> webapp loop" (rare;
documented for advanced users), and the call site spells out which one
via the variable name passed in.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any


async def dispatch_to_loop(coro: Coroutine[Any, Any, Any], target_loop: Any) -> Any:
    """Run ``coro`` on ``target_loop`` and await the result on the current loop.

    Parameters
    ----------
    coro:
        The coroutine to schedule. It will be awaited exactly once.
    target_loop:
        The event loop on which to execute ``coro``. When this loop is
        the same as the currently running loop, the coroutine is awaited
        inline (no cross-thread machinery). When it's a real
        ``asyncio.AbstractEventLoop`` distinct from the current loop,
        ``run_coroutine_threadsafe`` schedules it there and
        ``wrap_future`` makes the resulting ``concurrent.futures.Future``
        awaitable on the caller's loop. Anything else (e.g. a
        ``MagicMock`` in unit tests) also falls back to inline await.

    Returns
    -------
    Whatever ``coro`` returns. Exceptions raised inside the coroutine
    propagate to the caller. Cancelling the awaiting task on the caller
    side propagates ``CancelledError`` into the dispatched coroutine via
    ``asyncio.wrap_future``.
    """
    current_loop = asyncio.get_running_loop()
    # Only use the cross-thread path when the target is a real, distinct
    # event loop. ``isinstance`` filters out test doubles (MagicMock) and
    # ``is`` filters out the same-loop case where dispatching would be a
    # waste (and would deadlock if the target loop were single-threaded
    # and currently driving us).
    if isinstance(target_loop, asyncio.AbstractEventLoop) and target_loop is not current_loop:
        # ``run_coroutine_threadsafe`` returns a ``concurrent.futures.Future``;
        # ``asyncio.wrap_future`` adapts it into an ``asyncio.Future`` that
        # is awaitable on the current loop. The pair forwards exceptions
        # AND propagates cancellation in both directions.
        fut = asyncio.run_coroutine_threadsafe(coro, target_loop)
        return await asyncio.wrap_future(fut)
    # Same-loop (or no real loop available in tests): run inline. The
    # coroutine still runs to completion; this branch just skips the
    # ``run_coroutine_threadsafe`` round-trip.
    return await coro
