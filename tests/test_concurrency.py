"""Tests for ``drakkar.concurrency.dispatch_to_loop``.

The helper bridges two asyncio event loops running on different threads.
Four scenarios matter:

1. Same-loop short-circuit — when ``target_loop`` is the loop already
   running, dispatch must skip the cross-thread machinery and just
   ``await coro``. We assert the coroutine ran on the same thread (no
   thread hop) and that ``run_coroutine_threadsafe`` was NOT called via
   monkeypatching.

2. Cross-thread happy path — spin up a real secondary loop in a
   daemon thread, dispatch a coroutine there, get the result back on
   the calling loop.

3. Exception propagation — exceptions raised inside the dispatched
   coroutine must surface unchanged on the awaiting side.

4. Cancellation propagation — cancelling the awaiting task must
   propagate ``CancelledError`` into the dispatched coroutine on the
   target loop.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from drakkar.concurrency import dispatch_to_loop
from tests.conftest import wait_for


@pytest.fixture
def secondary_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Spin up a secondary asyncio loop in a daemon thread.

    Mirrors what ``UIServer.start()`` does in production: the loop
    runs on a daemon thread and stays alive for the test, then is
    stopped + joined on teardown. We expose the loop reference so tests
    can pass it as ``target_loop=`` to ``dispatch_to_loop``.
    """
    loop_box: list[asyncio.AbstractEventLoop | None] = [None]
    ready = threading.Event()

    def thread_body() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_box[0] = loop
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=thread_body, name='test-secondary-loop', daemon=True)
    thread.start()
    ready.wait(timeout=5)
    loop = loop_box[0]
    assert loop is not None

    yield loop

    # Stop the loop and join the thread so the next test starts clean.
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


async def test_same_loop_short_circuits_without_run_coroutine_threadsafe(monkeypatch):
    """When target_loop is the running loop, dispatch must NOT call
    ``run_coroutine_threadsafe`` — it should just await the coroutine
    inline. We monkeypatch the function to a sentinel that fails the
    test if invoked, and we also assert the coroutine ran on the same
    thread (no cross-thread hop).
    """
    calling_thread = threading.get_ident()
    observed_thread: list[int] = []

    def fail_if_called(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail('run_coroutine_threadsafe was called on the same-loop short-circuit path')

    monkeypatch.setattr(asyncio, 'run_coroutine_threadsafe', fail_if_called)

    async def inner() -> int:
        observed_thread.append(threading.get_ident())
        return 42

    current_loop = asyncio.get_running_loop()
    result = await dispatch_to_loop(inner(), target_loop=current_loop)

    assert result == 42
    assert observed_thread == [calling_thread]


async def test_dispatch_falls_back_to_inline_when_target_is_not_a_real_loop(monkeypatch):
    """When ``target_loop`` is something other than an
    ``AbstractEventLoop`` instance (e.g. ``None`` or a ``MagicMock`` in
    unit tests), dispatch must inline the coroutine instead of trying
    to schedule it on the non-loop. Regression for the
    ``isinstance`` guard.
    """
    from unittest.mock import MagicMock

    def fail_if_called(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail('run_coroutine_threadsafe was called for a non-loop target')

    monkeypatch.setattr(asyncio, 'run_coroutine_threadsafe', fail_if_called)

    async def inner() -> str:
        return 'inlined'

    result = await dispatch_to_loop(inner(), target_loop=MagicMock())
    assert result == 'inlined'


async def test_cross_thread_happy_path_returns_result_on_calling_loop(secondary_loop):
    """Dispatch on the secondary loop and confirm:
    - the coroutine actually ran on the secondary loop (different
      thread, different running-loop reference);
    - the result comes back to the awaiting coroutine on the test loop.
    """
    test_loop = asyncio.get_running_loop()
    captured: dict[str, Any] = {}

    async def inner() -> int:
        captured['ran_on_loop'] = asyncio.get_running_loop()
        captured['ran_on_thread'] = threading.get_ident()
        return 7

    result = await dispatch_to_loop(inner(), target_loop=secondary_loop)

    assert result == 7
    # The coroutine ran on the SECONDARY loop, not the test loop.
    assert captured['ran_on_loop'] is secondary_loop
    assert captured['ran_on_loop'] is not test_loop
    # And on a DIFFERENT thread than the calling test thread.
    assert captured['ran_on_thread'] != threading.get_ident()


async def test_cross_thread_exception_propagates(secondary_loop):
    """An exception raised inside the dispatched coroutine on the
    secondary loop must propagate unchanged to the awaiting caller on
    the primary loop. The exception type and message MUST be preserved
    so callers can pattern-match on them as if the coroutine had run
    locally.
    """

    class _CustomError(RuntimeError):
        pass

    async def inner() -> int:
        raise _CustomError('boom from the other loop')

    with pytest.raises(_CustomError, match='boom from the other loop'):
        await dispatch_to_loop(inner(), target_loop=secondary_loop)


async def test_cross_thread_cancellation_propagates(secondary_loop):
    """Cancelling the awaiting task on the calling side must propagate
    ``CancelledError`` into the coroutine running on the secondary
    loop. The dispatched coroutine should observe the cancel as a
    raised ``CancelledError`` while it's at an ``await`` point.
    """
    cancel_observed = threading.Event()
    started = threading.Event()

    async def slow_inner() -> None:
        # Signal that we've reached the await point on the secondary
        # loop so the test can cancel the awaiting task at the right
        # moment. ``threading.Event`` works across loops because it's
        # not bound to any particular loop.
        started.set()
        try:
            # A long sleep gives the test loop time to issue the cancel
            # while the secondary loop is parked here.
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            cancel_observed.set()
            raise

    async def awaiter() -> None:
        await dispatch_to_loop(slow_inner(), target_loop=secondary_loop)

    task = asyncio.create_task(awaiter())

    # Wait until the dispatched coroutine has actually started and is
    # parked at the ``asyncio.sleep`` await on the secondary loop.
    # Polling ``started`` on the test side rather than a blocking
    # ``started.wait`` because we want the awaiter task to progress past
    # the ``run_coroutine_threadsafe`` call first.
    await wait_for(lambda: started.is_set())

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Give the secondary loop a moment to process the cancel and exit
    # the ``except CancelledError`` block. The Event is set inside that
    # block — if propagation worked, this returns True quickly.
    assert cancel_observed.wait(timeout=2.0), 'CancelledError did not propagate to the dispatched coroutine'
