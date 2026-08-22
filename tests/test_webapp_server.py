"""Tests for :class:`drakkar.webapp.WebApp` — bootstrap, gates, lifecycle.

Focus areas:

* The uvicorn-on-thread bootstrap binds the configured port,
  ``wait_until_ready`` resolves once serving, ``stop`` exits cleanly.
* Construction-time validation: webapp.enabled but no HTTP types →
  :class:`ConfigurationError`.
* Per-request 503 gates ahead of the runner:
  - request before main loop ready → 503 ``not_ready`` with the
    Kafka-routing hint.
  - request during shutdown → 503 ``status='shutdown'``.
* Request once both gates pass exercises the runner. The
  default ``arrange_http_request`` raises ``NotImplementedError`` so
  the route handler returns a flat 500 — happy-path runner tests live
  in ``tests/test_webapp_runner.py``.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.webapp import ConfigurationError, WebApp


def _free_port() -> int:
    """Reserve a free TCP port for the test webapp to bind to.

    Binds to port 0 so the kernel picks an unused port, immediately
    closes the socket, and returns the chosen port number. The window
    between close() and uvicorn binding is short enough that races are
    rare in practice; if a CI run gets unlucky the test simply re-runs.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


# ---------------------------------------------------------------------------
# Test handlers and config builders
# ---------------------------------------------------------------------------


class _Input(BaseModel):
    """Stand-in Kafka-input model — required slot for BaseDrakkarHandler."""

    a: int = 0


class _Output(BaseModel):
    """Stand-in Kafka-output model."""

    b: int = 0


class _HttpReq(BaseModel):
    """Stand-in HTTP request model — fills the 3rd Generic slot."""

    pattern: str = ''


class _HttpResp(BaseModel):
    """Stand-in HTTP response model — fills the 4th Generic slot."""

    matches: int = 0


class _WebHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Handler with all four Generic slots populated.

    Overrides both HTTP hooks — construction-time validation (mirroring
    the Go backend) rejects a webapp-enabled handler that leaves them at
    the raising Base defaults.
    """

    async def arrange(self, messages, pending):
        return []

    async def arrange_http_request(self, req, pending):
        return []

    async def on_http_request_complete(self, group):
        return _HttpResp()


class _RaisingWebHandler(_WebHandler):
    """Hooks present (passes construction validation) but raising.

    For tests that assert the route's flat-500 mapping of handler
    errors — the pre-validation era relied on the raising Base default
    for this; now the raise must be an explicit override.
    """

    async def arrange_http_request(self, req, pending):
        raise NotImplementedError('arrange_http_request')


class _NoHttpHandler(BaseDrakkarHandler[_Input, _Output]):
    """Handler with the legacy 2-param form — HTTP slots resolve to None."""

    async def arrange(self, messages, pending):
        return []


def _make_app_stub(handler: Any, *, is_ready: bool = False) -> Any:
    """Build a minimal ``DrakkarApp`` stand-in for unit tests.

    The webapp only reaches into ``app._handler`` (for type validation)
    and ``app.is_ready`` (for the 503 gate); a ``MagicMock`` with those
    attributes set is enough to drive the bootstrap.
    """
    app = MagicMock()
    app._handler = handler
    app.is_ready = is_ready
    return app


def _make_config(*, port: int) -> WebAppConfig:
    """Build a ``WebAppConfig`` bound to ``port`` for the test webapp."""
    return WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=port,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
    )


# ---------------------------------------------------------------------------
# Construction-time validation
# ---------------------------------------------------------------------------


def test_construction_raises_when_handler_missing_http_types():
    """webapp.enabled=true + 2-param handler → ConfigurationError."""
    handler = _NoHttpHandler()
    app = _make_app_stub(handler)
    config = _make_config(port=_free_port())

    with pytest.raises(ConfigurationError) as exc_info:
        WebApp(app, config)

    # Message should name the offending handler class so operators can
    # find the bad subclass quickly.
    assert '_NoHttpHandler' in str(exc_info.value)
    assert 'HttpRequestT' in str(exc_info.value) or 'HttpResponseT' in str(exc_info.value)


def test_construction_succeeds_with_full_4_param_handler():
    """4-param handler → WebApp constructs without raising."""
    handler = _WebHandler()
    app = _make_app_stub(handler)
    config = _make_config(port=_free_port())

    webapp = WebApp(app, config)

    # Sanity — internal fields the lifecycle reads are initialised.
    assert webapp._fastapi_app is not None
    assert webapp._loop is None
    assert webapp._uvicorn_server is None
    assert webapp._thread is None
    assert webapp.shutdown_event.is_set() is False


# ---------------------------------------------------------------------------
# uvicorn lifecycle: start, wait_until_ready, stop
# ---------------------------------------------------------------------------


def test_start_in_thread_and_wait_until_ready_returns_once_serving():
    """Server binds the port and ``wait_until_ready`` resolves cleanly."""
    handler = _RaisingWebHandler()
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        # After wait_until_ready resolves, the inner loop is captured
        # AND uvicorn flipped started=True.
        assert webapp._loop is not None
        assert webapp._uvicorn_server is not None
        assert webapp._uvicorn_server.started is True

        # Sanity-check the server is actually accepting TCP connections.
        # If wait_until_ready returned without binding we'd see a
        # ConnectionRefusedError here.
        with httpx.Client() as client:
            # Hit the configured route — body shape matters now
            # because the route parses the body before dispatching to
            # the runner. Empty JSON validates against the test's
            # ``_HttpReq`` model (all defaults). The fixture's raising
            # ``arrange_http_request`` maps to a flat 500 from the route.
            response = client.post(f'http://127.0.0.1:{port}/process', json={})
            assert response.status_code == 500
            body = response.json()
            assert body['status'] == 'error'
            assert body['error'] == 'internal error'
            assert 'request_id' in body
    finally:
        webapp.stop(drain_timeout=2.0)


def test_stop_exits_cleanly_when_started():
    """``stop`` signals uvicorn and joins the thread within drain_timeout."""
    handler = _WebHandler()
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    webapp.start_in_thread()
    webapp.wait_until_ready(timeout=5.0)

    assert webapp._thread is not None
    assert webapp._thread.is_alive()

    webapp.stop(drain_timeout=5.0)

    # uvicorn flagged for exit and the thread has joined.
    assert webapp._uvicorn_server is not None
    assert webapp._uvicorn_server.should_exit is True
    # Give the daemon thread a moment to actually exit after join returns.
    deadline = time.time() + 2.0
    while webapp._thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    assert webapp._thread.is_alive() is False


def test_stop_when_never_started_does_not_raise():
    """``stop`` on a webapp that never called ``start_in_thread`` is a no-op."""
    handler = _WebHandler()
    app = _make_app_stub(handler)
    config = _make_config(port=_free_port())
    webapp = WebApp(app, config)

    # No exception — stop is defensive against early-shutdown paths.
    webapp.stop(drain_timeout=1.0)


# ---------------------------------------------------------------------------
# Per-request 503 gates (checked before dispatching to the runner)
# ---------------------------------------------------------------------------


def test_request_before_main_loop_ready_returns_503_with_hint():
    """is_ready=False → 503 with status='not_ready' and the Kafka hint."""
    handler = _WebHandler()
    app = _make_app_stub(handler, is_ready=False)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        with httpx.Client() as client:
            response = client.post(f'http://127.0.0.1:{port}/process', json={})
            assert response.status_code == 503
            body = response.json()
            assert body['status'] == 'not_ready'
            assert 'error' in body
            assert 'request_id' in body
            # Hint must point at the Kafka source topic — clients use
            # this to switch over to the durable-queue path.
            assert 'hint' in body
            assert 'kafka' in body['hint'].lower()
    finally:
        webapp.stop(drain_timeout=2.0)


def test_request_during_shutdown_returns_503_with_status_shutdown():
    """shutdown_event set → 503 with status='shutdown' (gate ahead of dispatch)."""
    handler = _WebHandler()
    # ``is_ready=True`` so the not_ready gate doesn't pre-empt shutdown.
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        # Flip the shutdown gate — new requests should now bounce off
        # immediately with status='shutdown'.
        webapp.shutdown_event.set()

        with httpx.Client() as client:
            response = client.post(f'http://127.0.0.1:{port}/process', json={})
            assert response.status_code == 503
            body = response.json()
            assert body['status'] == 'shutdown'
            assert 'request_id' in body
            assert 'hint' in body
            assert 'kafka' in body['hint'].lower()
    finally:
        webapp.stop(drain_timeout=2.0)


def test_request_when_ready_and_not_shutting_down_dispatches_to_runner():
    """Both gates pass → request flows into the runner.

    ``_RaisingWebHandler.arrange_http_request`` raises
    ``NotImplementedError``; that failure surfaces as a flat 500 from the
    route handler — confirming body parse + runner dispatch happened. The
    happy-path runner tests live in ``tests/test_webapp_runner.py``.
    """
    handler = _RaisingWebHandler()
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        with httpx.Client() as client:
            response = client.post(f'http://127.0.0.1:{port}/process', json={})
            assert response.status_code == 500
            body = response.json()
            assert body['status'] == 'error'
            assert body['error'] == 'internal error'
            assert 'request_id' in body
    finally:
        webapp.stop(drain_timeout=2.0)


# ---------------------------------------------------------------------------
# Body-size cap (parity with the Go backend's max_body_bytes / 413 path)
# ---------------------------------------------------------------------------


def test_oversized_body_returns_413_with_go_parity_envelope():
    """A body beyond ``max_body_bytes`` trips 413 BEFORE parsing.

    The envelope (keys and details text) matches the Go backend's
    MaxBytesReader path byte-for-byte, and the cap fires ahead of the
    422 parse gate — the payload is never read, so "invalid" would
    mislead clients into retrying the same oversized body.
    """
    handler = _WebHandler()
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=port,
        path='/process',
        max_body_bytes=64,
        clients=[WebClientConfig(name='anonymous', token='', rpm=100)],
    )
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        with httpx.Client() as client:
            # 100 bytes of non-JSON: the cap (64) must answer before the
            # parse gate ever sees the payload.
            response = client.post(
                f'http://127.0.0.1:{port}/process',
                content=b'x' * 100,
                headers={'content-type': 'application/json'},
            )
            assert response.status_code == 413
            body = response.json()
            assert body['error'] == 'request_too_large'
            assert body['details'] == 'request body exceeds webapp.max_body_bytes (64 bytes)'
            assert 'request_id' in body

            # Exactly at the limit is still accepted by the cap — this
            # invalid JSON reaches the parse gate and 422s instead.
            response = client.post(
                f'http://127.0.0.1:{port}/process',
                content=b'y' * 64,
                headers={'content-type': 'application/json'},
            )
            assert response.status_code == 422
    finally:
        webapp.stop(drain_timeout=2.0)


# ---------------------------------------------------------------------------
# Construction-time validation: hooks not overridden
# ---------------------------------------------------------------------------


class _TypedButHookless(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """HTTP types declared, but both HTTP hooks left at the Base defaults."""

    async def arrange(self, messages, pending):
        return []


def test_construction_raises_when_http_hooks_not_overridden():
    """Typed handler that never overrides the HTTP hooks → ConfigurationError."""
    handler = _TypedButHookless()
    app = _make_app_stub(handler)
    config = _make_config(port=_free_port())

    with pytest.raises(ConfigurationError, match='arrange_http_request'):
        WebApp(app, config)


# ---------------------------------------------------------------------------
# wait_until_ready timeout stages
# ---------------------------------------------------------------------------


def test_wait_until_ready_times_out_when_loop_never_captured():
    """Stage-1 timeout: the inner loop was never signalled ready."""
    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))

    with pytest.raises(TimeoutError, match='inner loop'):
        webapp.wait_until_ready(timeout=0.05)


def test_wait_until_ready_times_out_when_uvicorn_never_starts():
    """Stage-2 timeout: loop captured but uvicorn never flips ``started``."""
    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))
    webapp._loop_ready.set()
    webapp._uvicorn_server = SimpleNamespace(started=False)

    with pytest.raises(TimeoutError, match='started state'):
        webapp.wait_until_ready(timeout=0.05)


# ---------------------------------------------------------------------------
# stop(): drain-timeout warning when the thread refuses to join
# ---------------------------------------------------------------------------


def test_stop_warns_when_thread_join_exceeds_drain_timeout():
    """A stuck worker thread must not block shutdown — stop() warns and returns."""
    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))
    hang = threading.Event()
    stuck = threading.Thread(target=hang.wait, daemon=True)
    stuck.start()
    webapp._thread = stuck
    webapp._uvicorn_server = SimpleNamespace(should_exit=False)

    with capture_logs() as cap:
        webapp.stop(drain_timeout=0.05)

    assert webapp._uvicorn_server.should_exit is True
    assert any(e['event'] == 'webapp_stop_thread_join_timeout' for e in cap)
    hang.set()
    stuck.join(timeout=2.0)


# ---------------------------------------------------------------------------
# _observe_outcome: early-error paths carry no start time
# ---------------------------------------------------------------------------


def test_observe_outcome_without_start_time_counts_but_skips_histogram():
    """When request.state has no start-time the counter still increments
    but no duration sample is observed (there is nothing to measure)."""
    from drakkar.metrics import webapp_request_duration, webapp_requests

    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))
    request = SimpleNamespace(state=SimpleNamespace())  # no webapp_start_monotonic

    counter = webapp_requests.labels(client='obs-test', status='error')
    histogram = webapp_request_duration.labels(client='obs-test', status='error')
    count_before = counter._value.get()
    hist_before = histogram._sum.get()

    webapp._observe_outcome(request=request, client='obs-test', status='error')

    assert counter._value.get() == count_before + 1
    assert histogram._sum.get() == hist_before  # untouched — no start time


# ---------------------------------------------------------------------------
# _signal_cancel: main-loop scheduling and its late-shutdown fallback
# ---------------------------------------------------------------------------


def test_signal_cancel_schedules_on_main_loop_when_available():
    """With a healthy main loop the set() runs via call_soon_threadsafe."""
    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))

    class _ImmediateLoop:
        """Fake loop that runs the callback synchronously — proves the
        cancel flag is set through the threadsafe-scheduling path."""

        def call_soon_threadsafe(self, cb):
            cb()

    webapp._app.main_loop = _ImmediateLoop()
    ctx = SimpleNamespace(cancelled=asyncio.Event())

    webapp._signal_cancel(ctx)

    assert ctx.cancelled.is_set()


def test_signal_cancel_falls_back_to_direct_set_when_loop_closed():
    """A closed main loop (late shutdown) raises RuntimeError from
    call_soon_threadsafe; the flag must still be set directly."""
    webapp = WebApp(_make_app_stub(_WebHandler()), _make_config(port=_free_port()))

    class _ClosedLoop:
        def call_soon_threadsafe(self, cb):
            raise RuntimeError('Event loop is closed')

    webapp._app.main_loop = _ClosedLoop()
    ctx = SimpleNamespace(cancelled=asyncio.Event())

    webapp._signal_cancel(ctx)

    assert ctx.cancelled.is_set()


# ---------------------------------------------------------------------------
# request_id resolution failure → flat 500
# ---------------------------------------------------------------------------


class _BadRequestIdHandler(_WebHandler):
    """Handler whose http_request_id override raises — the route must
    answer with the flat 500 envelope instead of propagating."""

    def http_request_id(self, req, headers):
        raise RuntimeError('id resolution exploded')


def test_request_id_resolution_failure_returns_flat_500():
    handler = _BadRequestIdHandler()
    app = _make_app_stub(handler, is_ready=True)
    port = _free_port()
    config = _make_config(port=port)
    webapp = WebApp(app, config)

    try:
        webapp.start_in_thread()
        webapp.wait_until_ready(timeout=5.0)

        with httpx.Client() as client, capture_logs() as cap:
            response = client.post(f'http://127.0.0.1:{port}/process', json={})

        assert response.status_code == 500
        body = response.json()
        assert body['status'] == 'error'
        assert body['error'] == 'internal error'
        assert 'request_id' in body
        assert any(e['event'] == 'webapp_request_id_resolution_failed' for e in cap)
    finally:
        webapp.stop(drain_timeout=2.0)
