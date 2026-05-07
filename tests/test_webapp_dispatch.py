"""Tests for the webapp's T2->T1 dispatch wiring (Task 6b).

Covers the behaviours added in Task 6b:

* Future round-trip happy path — the route handler calls
  ``dispatch_to_loop`` on a real cross-thread setup and gets the
  runner's value back.
* Timeout: ``asyncio.wait_for`` trips → cancellation signal lands on
  T1's loop (``ctx.cancelled.is_set()`` becomes True), the route
  handler returns 504, and the runner sees the flag at its next
  cancellation gate.
* Subprocesses finishing after cancellation have their results
  discarded — ``on_http_request_complete`` is NOT called when
  ``ctx.cancelled`` is set before the gate.
* Concurrency cap (``max_concurrent``): N+1 simultaneous requests on
  a pool of size N → the (N+1)th gets 503 ``capacity`` immediately.
* Shutdown gate: ``shutdown_event.set()`` returns 503 ahead of dispatch.

The cross-thread tests construct two real event loops on two threads
and drive the route handler against them; the in-process tests use
TestClient with a stubbed ``main_loop`` that runs the runner inline.
"""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import ExecutorResult, ExecutorTask, make_task_id
from drakkar.webapp import WebApp
from drakkar.webapp.models import WebRequestContext

# ---------------------------------------------------------------------------
# Test fixtures (intentionally similar to test_webapp_runner.py)
# ---------------------------------------------------------------------------


class _Input(BaseModel):
    a: int = 0


class _Output(BaseModel):
    b: int = 0


class _HttpReq(BaseModel):
    pattern: str = ''


class _HttpResp(BaseModel):
    matches: int = 0


class _ConfigurableHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Handler whose HTTP hooks are pluggable per test."""

    def __init__(self) -> None:
        super().__init__()
        self.arrange_impl: Any = self._default_arrange
        self.complete_impl: Any = self._default_complete
        # Counters tests use to assert that hooks did/didn't run.
        self.arrange_calls = 0
        self.complete_calls = 0

    async def arrange(self, messages, pending):
        return []

    async def _default_arrange(self, req, pending):
        return []

    async def _default_complete(self, group):
        return _HttpResp(matches=group.succeeded)

    async def arrange_http_request(self, req, pending):
        self.arrange_calls += 1
        return await self.arrange_impl(req, pending)

    async def on_http_request_complete(self, group):
        self.complete_calls += 1
        return await self.complete_impl(group)


def _make_stub_app(
    handler: BaseDrakkarHandler,
    *,
    pool: Any,
    main_loop: Any = None,
    is_ready: bool = True,
) -> Any:
    """Construct a minimal ``DrakkarApp`` stand-in for dispatch tests."""
    app = MagicMock()
    app._handler = handler
    app._executor_pool = pool
    app._recorder = None
    app.is_ready = is_ready
    app.main_loop = main_loop
    return app


def _make_config(*, max_concurrent: int = 64, request_timeout_seconds: float = 30.0) -> WebAppConfig:
    return WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=10000)],
        max_concurrent=max_concurrent,
        request_timeout_seconds=request_timeout_seconds,
    )


def _make_pool(execute_coro: Any) -> MagicMock:
    """Build a stub pool whose ``execute`` is the provided coroutine factory."""
    pool = MagicMock()
    pool.execute = execute_coro
    return pool


def _canned_result(task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(
        exit_code=0,
        stdout='',
        stderr='',
        duration_seconds=0.001,
        task=task,
        pid=12345,
    )


# ---------------------------------------------------------------------------
# Cross-thread loop helper. Spins up a second event loop on its own
# thread so tests can exercise the real ``run_coroutine_threadsafe`` +
# ``wrap_future`` path that the route handler hits in production.
# ---------------------------------------------------------------------------


class _BackgroundLoop:
    """Run an asyncio event loop on a daemon thread for cross-thread tests.

    Mimics the production layout: T1 (this fixture) hosts the runner's
    coroutine; T2 (the FastAPI TestClient's request thread) dispatches
    onto T1 via ``dispatch_to_loop``.
    """

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        def _runner() -> None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self._ready.set()
            self.loop.run_forever()

        self._thread = threading.Thread(target=_runner, name='t1-loop', daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2.0)

    def stop(self) -> None:
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=2.0)


@pytest.fixture
def background_loop():
    bg = _BackgroundLoop()
    bg.start()
    try:
        yield bg
    finally:
        bg.stop()


# ---------------------------------------------------------------------------
# Future round-trip happy path
# ---------------------------------------------------------------------------


def test_dispatch_round_trip_returns_runner_value(background_loop):
    """Cross-thread dispatch: T2 receives the runner's WebReport from T1."""
    handler = _ConfigurableHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    async def execute(task_arg, recorder, partition_id):
        return _canned_result(task_arg)

    handler.arrange_impl = arrange_impl

    app = _make_stub_app(handler, pool=_make_pool(execute), main_loop=background_loop.loop)
    config = _make_config()
    webapp = WebApp(app, config)

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'hi'})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body['status'] == 'ok'
        assert body['task_summary']['total'] == 1
        # The hooks ran end-to-end.
        assert handler.arrange_calls == 1
        assert handler.complete_calls == 1


# ---------------------------------------------------------------------------
# Timeout → 504 + cancellation flag set on T1
# ---------------------------------------------------------------------------


def test_timeout_triggers_504_and_sets_cancellation_on_t1(background_loop):
    """Slow runner + tight timeout → 504, ctx.cancelled set on T1."""
    handler = _ConfigurableHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    captured_ctx: list[WebRequestContext] = []

    async def arrange_impl(req, pending):
        # Stash the ctx so we can assert ctx.cancelled after the route
        # returns its 504. ``arrange_http_request`` runs on T1 so the
        # ctx we capture here is the same instance the route handler
        # built on T2.
        return [task]

    async def slow_execute(task_arg, recorder, partition_id):
        # Sleep long enough that the route's wait_for fires first.
        # ``asyncio.wait_for(...)`` will cancel us via wrap_future, but
        # we also want to verify the post-execute gate; sleep beyond
        # the timeout to guarantee that order.
        try:
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            # Re-raise so the gather sees us as a failed task; this is
            # the realistic shape of cancellation propagation through
            # the executor pool.
            raise
        return _canned_result(task_arg)

    handler.arrange_impl = arrange_impl

    app = _make_stub_app(handler, pool=_make_pool(slow_execute), main_loop=background_loop.loop)
    # 0.1s timeout — slow_execute sleeps 2s, so the route will time out.
    config = _make_config(request_timeout_seconds=0.1)
    webapp = WebApp(app, config)

    # Patch the runner.run to capture the ctx before delegating.
    original_run = webapp._runner.run

    async def capturing_run(ctx):
        captured_ctx.append(ctx)
        return await original_run(ctx)

    webapp._runner.run = capturing_run  # type: ignore[method-assign]

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'hi'})
        assert response.status_code == 504
        body = response.json()
        assert body['status'] == 'timeout'
        assert 'request_id' in body
        assert 'duration_ms' in body
        assert 'kafka' in body['hint'].lower()

    # Wait briefly for the cross-thread ``call_soon_threadsafe`` to land.
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if captured_ctx and captured_ctx[0].cancelled is not None and captured_ctx[0].cancelled.is_set():
            break
        time.sleep(0.05)

    assert captured_ctx, 'runner.run should have been called and captured ctx'
    cancelled = captured_ctx[0].cancelled
    assert cancelled is not None
    assert cancelled.is_set() is True
    # The user response hook must NOT have been called — its work was
    # dropped at the post-execute / pre-on_http_request_complete gate.
    assert handler.complete_calls == 0


# ---------------------------------------------------------------------------
# Cancellation skips on_http_request_complete (in-process / same-loop)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancellation_set_pre_complete_skips_response_hook():
    """ctx.cancelled set before on_http_request_complete → hook NOT called.

    Same-loop variant: drives the runner directly without the
    cross-thread machinery so the test runs fast and deterministically.
    """
    from drakkar.webapp.runner import WebappRunner

    handler = _ConfigurableHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_impl = arrange_impl

    # Pool's execute returns a canned result and on its way out flips
    # ctx.cancelled — simulating "subprocess finished but T2 has
    # already 504'd". Reach the ctx via closure.
    canned = _canned_result(task)

    captured: dict[str, WebRequestContext] = {}

    async def execute(task_arg, recorder, partition_id):
        # Flip cancellation just before returning. The runner's post-
        # execute gate will trip because ctx.cancelled.is_set() is True.
        # We have to wait until ctx.cancelled is allocated by the runner
        # (it's the first line of run()).
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            ctx = captured.get('ctx')
            if ctx is not None and ctx.cancelled is not None:
                ctx.cancelled.set()
                break
            await asyncio.sleep(0)
        return canned

    app = _make_stub_app(handler, pool=_make_pool(execute), main_loop=None)
    config = _make_config()
    runner = WebappRunner(app, config)

    ctx = WebRequestContext(
        request_id='req_test_0099',
        client_name='anonymous',
        request=_HttpReq(pattern='hi'),
        started_at=datetime.now(UTC),
        headers={},
    )
    captured['ctx'] = ctx

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    # User response hook never ran — runner short-circuited at the
    # post-execute gate.
    assert handler.complete_calls == 0


# ---------------------------------------------------------------------------
# Subprocess result discarded after cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subprocess_completion_after_cancellation_does_not_invoke_response_hook():
    """Subprocess finishes after T2 timed out → its output is dropped.

    This formalises the v1 caveat: in-flight subprocesses cannot be
    SIGTERM'd, so they finish naturally — but the runner's gate
    ensures ``on_http_request_complete`` is skipped, so the work is
    not surfaced into a webapp response.
    """
    from drakkar.webapp.runner import WebappRunner

    handler = _ConfigurableHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    seen_in_complete: list[Any] = []

    async def arrange_impl(req, pending):
        return [task]

    async def complete_impl(group):
        seen_in_complete.append(group)
        return _HttpResp(matches=group.succeeded)

    handler.arrange_impl = arrange_impl
    handler.complete_impl = complete_impl

    async def execute(task_arg, recorder, partition_id):
        # Subprocess completes normally — but ctx.cancelled is already
        # set, so the runner's gate trips on the way back.
        return _canned_result(task_arg)

    app = _make_stub_app(handler, pool=_make_pool(execute), main_loop=None)
    runner = WebappRunner(app, _make_config())

    ctx = WebRequestContext(
        request_id='req_test_0100',
        client_name='anonymous',
        request=_HttpReq(pattern='hi'),
        started_at=datetime.now(UTC),
        headers={},
    )
    # Pre-allocate the cancelled flag on this loop and set it. The
    # runner's "if ctx.cancelled is None" branch leaves ours alone.
    ctx.cancelled = asyncio.Event()
    ctx.cancelled.set()

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    # The response hook never ran — its output was correctly discarded.
    assert seen_in_complete == []


# ---------------------------------------------------------------------------
# max_concurrent gate
# ---------------------------------------------------------------------------


def test_max_concurrent_returns_503_capacity_when_pool_full(background_loop):
    """max_concurrent=2 + 3 in-flight → 3rd request gets 503 capacity."""
    handler = _ConfigurableHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    # Block requests inside the runner so the semaphore stays held.
    release = threading.Event()

    async def arrange_impl(req, pending):
        return [task]

    async def execute(task_arg, recorder, partition_id):
        # Sleep on T1 until ``release`` flips, then return.
        while not release.is_set():
            await asyncio.sleep(0.01)
        return _canned_result(task_arg)

    handler.arrange_impl = arrange_impl

    app = _make_stub_app(handler, pool=_make_pool(execute), main_loop=background_loop.loop)
    config = _make_config(max_concurrent=2, request_timeout_seconds=10.0)
    webapp = WebApp(app, config)

    # Fire 3 simultaneous requests against the wired webapp. The first
    # two grab the semaphore; the third should bounce off with 503
    # ``capacity`` immediately.
    results: list[httpx.Response] = []
    threads: list[threading.Thread] = []

    def _send(client: TestClient) -> None:
        results.append(client.post('/process', json={'pattern': 'hi'}))

    with TestClient(webapp._fastapi_app) as client:
        # Two slow requests in flight.
        for _ in range(2):
            t = threading.Thread(target=_send, args=(client,), daemon=True)
            t.start()
            threads.append(t)
        # Give them time to acquire the semaphore.
        time.sleep(0.1)

        # Third request: must get 503 capacity quickly.
        third = client.post('/process', json={'pattern': 'hi'})
        assert third.status_code == 503, third.text
        body = third.json()
        assert body['status'] == 'capacity'
        assert body['max_concurrent'] == 2
        assert 'kafka' in body['hint'].lower()
        assert 'request_id' in body

        # Now release the slow ones so the test can clean up.
        release.set()
        for t in threads:
            t.join(timeout=5.0)

    # Both slow requests eventually completed cleanly.
    assert len(results) == 2
    for r in results:
        assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# shutdown_event ahead of dispatch
# ---------------------------------------------------------------------------


def test_shutdown_event_returns_503_ahead_of_dispatch(background_loop):
    """shutdown_event set BEFORE the request lands → 503 status='shutdown'.

    Confirms the gate fires in the route handler ahead of any runner
    dispatch (so in-flight requests can drain while new ones bounce).
    """
    handler = _ConfigurableHandler()

    async def execute(task_arg, recorder, partition_id):
        return _canned_result(task_arg)  # never reached

    app = _make_stub_app(handler, pool=_make_pool(execute), main_loop=background_loop.loop)
    config = _make_config()
    webapp = WebApp(app, config)

    # Pre-flip the shutdown gate before any traffic.
    webapp.shutdown_event.set()

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'hi'})
        assert response.status_code == 503
        body = response.json()
        assert body['status'] == 'shutdown'
        assert 'kafka' in body['hint'].lower()
        # Runner was never reached — the user hook count proves it.
        assert handler.arrange_calls == 0
        assert handler.complete_calls == 0
