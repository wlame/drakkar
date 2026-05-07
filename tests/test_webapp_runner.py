"""Tests for :class:`drakkar.webapp.runner.WebappRunner` — happy path + errors.

Coverage focus for Task 6a:

* End-to-end roundtrip on a synthetic ``DrakkarApp`` stub: the runner
  builds a ``WebReport`` whose ``request_id``/``client``/``status='ok'``
  match the request context, ``result`` carries the user's response,
  ``tasks``/``task_summary`` reflect the canned executor outcomes, and
  the ``timeline`` populates with stage durations.

* User-hook failures: ``arrange_http_request`` raising surfaces as
  :class:`WebappHandlerError` from the runner; the same applies to
  ``on_http_request_complete``. The wrapped error never leaks the
  traceback into the response body.

* Server-level integration via ``TestClient``: a malformed JSON body
  produces a flat 422 envelope; a user-hook failure produces a flat
  500 with no traceback in the response.

The tests construct ``WebappRunner`` directly with a stub ``DrakkarApp``
that exposes only the fields the runner reads (``_handler``,
``_executor_pool``, ``_recorder``). No FastAPI machinery is exercised
in the runner-only tests — that keeps them fast and focused.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    ExecutorResult,
    ExecutorTask,
    PendingContext,
    make_task_id,
)
from drakkar.webapp import WebApp
from drakkar.webapp.models import WebRequestContext
from drakkar.webapp.runner import WebappHandlerError, WebappRunner

# ---------------------------------------------------------------------------
# Test fixtures: tiny Pydantic models + a configurable handler
# ---------------------------------------------------------------------------


class _Input(BaseModel):
    """Stand-in Kafka-input model — fills the 1st Generic slot."""

    a: int = 0


class _Output(BaseModel):
    """Stand-in Kafka-output model — fills the 2nd Generic slot."""

    b: int = 0


class _HttpReq(BaseModel):
    """Synthetic HTTP request body."""

    pattern: str = ''


class _HttpResp(BaseModel):
    """Synthetic HTTP response body."""

    matches: int = 0
    echoed_pattern: str = ''


class _RecordingHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Test handler with overridable HTTP hooks.

    Each hook is wired to a lambda set on the instance so individual
    tests can inject custom behaviour (return canned tasks, raise, etc.)
    without subclassing per case.
    """

    def __init__(self) -> None:
        super().__init__()
        # Default: no tasks, simple response. Tests override these
        # callables to drive arrange_http_request / on_http_request_complete.
        self.arrange_http_request_impl: Any = self._default_arrange
        self.on_http_request_complete_impl: Any = self._default_complete

    async def arrange(self, messages, pending):
        """Unused on the webapp path; keeps the abstract base happy."""
        return []

    async def _default_arrange(
        self,
        req: _HttpReq,
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        return []

    async def _default_complete(self, group) -> _HttpResp:
        return _HttpResp(matches=group.succeeded, echoed_pattern='')

    async def arrange_http_request(self, req, pending):
        return await self.arrange_http_request_impl(req, pending)

    async def on_http_request_complete(self, group):
        return await self.on_http_request_complete_impl(group)


def _make_stub_app(
    handler: BaseDrakkarHandler,
    *,
    pool: Any,
    recorder: Any = None,
    is_ready: bool = True,
) -> Any:
    """Build a minimal ``DrakkarApp`` substitute for runner unit tests.

    The runner reaches into ``app._handler``, ``app._executor_pool``,
    ``app._recorder``, ``app.is_ready``, and ``app.main_loop``. A
    MagicMock with those attributes set is enough.
    """
    app = MagicMock()
    app._handler = handler
    app._executor_pool = pool
    app._recorder = recorder
    app.is_ready = is_ready
    # ``main_loop`` is read by the server's route handler, not the runner.
    # The runner-only tests await ``run()`` directly so the loop attr is
    # unused; setting it to None keeps the mock honest.
    app.main_loop = None
    return app


def _make_config() -> WebAppConfig:
    """Build a default ``WebAppConfig`` for runner tests."""
    return WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
    )


def _make_ctx(client_name: str = 'anonymous', request_id: str = 'req_test_0001') -> WebRequestContext:
    """Build a ``WebRequestContext`` for direct runner.run() invocation."""
    return WebRequestContext(
        request_id=request_id,
        client_name=client_name,
        request=_HttpReq(pattern='hello'),
        started_at=datetime.now(UTC),
        headers={},
    )


def _make_pool_returning(results: list[Any]) -> MagicMock:
    """Build a stub executor pool whose ``execute`` returns canned results.

    ``results`` is consumed in order — one entry per call. Items can be
    :class:`ExecutorResult` for success or any ``Exception`` instance to
    simulate a terminal failure (the runner treats both via
    ``return_exceptions=True`` and aggregates accordingly).
    """
    pool = MagicMock()
    iterator = iter(results)

    async def _execute(task: ExecutorTask, recorder, partition_id: int) -> ExecutorResult:
        outcome = next(iterator)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    pool.execute = _execute
    return pool


def _make_canned_result(task: ExecutorTask, *, exit_code: int = 0, duration: float = 0.05) -> ExecutorResult:
    """Build an ``ExecutorResult`` matching ``task`` for the stub pool."""
    return ExecutorResult(
        exit_code=exit_code,
        stdout='',
        stderr='',
        duration_seconds=duration,
        task=task,
        pid=12345,
    )


# ---------------------------------------------------------------------------
# Happy-path roundtrip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runner_happy_path_returns_assembled_web_report():
    """One request, two tasks, both succeed → WebReport carries all fields."""
    handler = _RecordingHandler()

    # Two synthetic tasks. We capture the task ids so we can assert the
    # framework auto-stamping (origin/client_name/request_id) below.
    task_a = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_b = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        # Arrange returns the two tasks; framework stamps them after.
        return [task_a, task_b]

    async def complete_impl(group):
        # Mirror the matched count back so assertions can pin down the
        # value — the handler sees the FULL synthetic group.
        return _HttpResp(matches=group.succeeded, echoed_pattern=group.source_message.value.decode())

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl

    canned_a = _make_canned_result(task_a, duration=0.01)
    canned_b = _make_canned_result(task_b, duration=0.02)
    pool = _make_pool_returning([canned_a, canned_b])

    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())
    ctx = _make_ctx()

    report = await runner.run(ctx)

    # Top-level identity fields propagate through.
    assert report.request_id == ctx.request_id
    assert report.client == ctx.client_name
    assert report.status == 'ok'
    assert report.started_at == ctx.started_at
    assert report.finished_at >= ctx.started_at
    assert report.duration_ms >= 0.0

    # Result is the user's HttpResponseT model — runner does not dump it.
    assert isinstance(report.result, _HttpResp)
    assert report.result.matches == 2

    # Tasks list reflects the executor outcomes; stdout/stderr never
    # appear here per the documented "no subprocess output in body" rule.
    assert len(report.tasks) == 2
    task_ids = {t.task_id for t in report.tasks}
    assert task_ids == {task_a.task_id, task_b.task_id}
    for entry in report.tasks:
        assert entry.exit_code == 0
        assert entry.duration_ms > 0.0
        assert entry.retries == 0

    # Summary aggregates count.
    assert report.task_summary.total == 2
    assert report.task_summary.success == 2
    assert report.task_summary.failed == 0

    # Sinks stays None on the sinks_enabled=False path (Task 6a default).
    assert report.sinks is None

    # Timeline records arrange / execute / on_http_request_complete.
    stages = [s.stage for s in report.timeline]
    assert stages == ['arrange', 'execute', 'on_http_request_complete']
    for stage in report.timeline:
        assert stage.duration_ms >= 0.0


@pytest.mark.asyncio
async def test_runner_stamps_origin_client_request_id_on_tasks():
    """Tasks returned from arrange_http_request get origin/client/request_id."""
    handler = _RecordingHandler()

    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        # Sanity: the handler sees a "fresh" task — pre-stamp markers None.
        assert task.origin == 'kafka'  # default
        assert task.client_name is None
        assert task.request_id is None
        return [task]

    handler.arrange_http_request_impl = arrange_impl
    pool = _make_pool_returning([_make_canned_result(task)])

    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())
    ctx = _make_ctx(client_name='tenant-A', request_id='req_test_0002')

    await runner.run(ctx)

    # After the run the framework has stamped the markers in-place.
    assert task.origin == 'http'
    assert task.client_name == 'tenant-A'
    assert task.request_id == 'req_test_0002'


@pytest.mark.asyncio
async def test_runner_synthetic_message_group_carries_origin_and_client():
    """The MessageGroup the runner builds carries http/client/request_id explicitly."""
    handler = _RecordingHandler()
    captured_group: dict[str, Any] = {}

    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    async def complete_impl(group):
        # Stash the group so the test can assert on its fields.
        captured_group['group'] = group
        return _HttpResp(matches=group.succeeded)

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl

    pool = _make_pool_returning([_make_canned_result(task)])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())
    ctx = _make_ctx(client_name='tenant-A', request_id='req_test_0003')

    await runner.run(ctx)

    group = captured_group['group']
    assert group.origin == 'http'
    assert group.client_name == 'tenant-A'
    assert group.request_id == 'req_test_0003'
    # The synthetic source message uses partition=-1 (HTTP marker).
    assert group.source_message.partition == -1
    # Offset is the runner's monotone seq — first request → 1.
    assert group.source_message.offset == 1
    # Source message key is the client name encoded; tests can rely on it.
    assert group.source_message.key == b'tenant-A'


@pytest.mark.asyncio
async def test_runner_increments_request_seq_per_call():
    """Two consecutive runs produce monotone synthetic offsets 1, 2."""
    handler = _RecordingHandler()
    captured_offsets: list[int] = []

    async def complete_impl(group):
        captured_offsets.append(group.source_message.offset)
        return _HttpResp(matches=0)

    handler.on_http_request_complete_impl = complete_impl
    pool = _make_pool_returning([])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    await runner.run(_make_ctx(request_id='req_test_0010'))
    await runner.run(_make_ctx(request_id='req_test_0011'))

    assert captured_offsets == [1, 2]


@pytest.mark.asyncio
async def test_runner_handles_failed_task_in_summary():
    """A subprocess raising surfaces as failed=1 in task_summary."""
    handler = _RecordingHandler()
    task_ok = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_bad = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_ok, task_bad]

    handler.arrange_http_request_impl = arrange_impl
    pool = _make_pool_returning([_make_canned_result(task_ok), RuntimeError('boom')])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    report = await runner.run(_make_ctx())

    assert report.task_summary.total == 2
    assert report.task_summary.success == 1
    assert report.task_summary.failed == 1
    # The failed task is intentionally NOT in ``report.tasks`` — Task 6a
    # only reports successful executions there. Task 6b/6c may revisit.
    assert len(report.tasks) == 1


# ---------------------------------------------------------------------------
# User-hook error paths (runner-level)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runner_wraps_arrange_http_request_exceptions():
    """arrange_http_request raising → WebappHandlerError(where='arrange_http_request')."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        raise RuntimeError('arrange exploded')

    handler.arrange_http_request_impl = arrange_impl

    pool = _make_pool_returning([])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    with pytest.raises(WebappHandlerError) as exc_info:
        await runner.run(_make_ctx())

    err = exc_info.value
    assert err.where == 'arrange_http_request'
    assert isinstance(err.original_exc, RuntimeError)
    assert 'arrange exploded' in str(err.original_exc)
    # Traceback is captured server-side but never leaks into the body.
    assert err.traceback_str
    assert 'RuntimeError' in err.traceback_str


@pytest.mark.asyncio
async def test_runner_wraps_on_http_request_complete_exceptions():
    """on_http_request_complete raising → WebappHandlerError with that ``where``."""
    handler = _RecordingHandler()

    async def complete_impl(group):
        raise ValueError('response build failed')

    handler.on_http_request_complete_impl = complete_impl

    pool = _make_pool_returning([])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    with pytest.raises(WebappHandlerError) as exc_info:
        await runner.run(_make_ctx())

    err = exc_info.value
    assert err.where == 'on_http_request_complete'
    assert isinstance(err.original_exc, ValueError)


@pytest.mark.asyncio
async def test_runner_allocates_cancelled_event_lazily():
    """ctx.cancelled is allocated by the runner on the awaiting loop."""
    handler = _RecordingHandler()
    pool = _make_pool_returning([])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    ctx = _make_ctx()
    assert ctx.cancelled is None  # not allocated by ctor

    await runner.run(ctx)
    # After run completes the event is bound to a real loop and hasn't
    # been set (Task 6a never trips cancellation; Task 6b adds the
    # set/check pair).
    assert isinstance(ctx.cancelled, asyncio.Event)
    assert ctx.cancelled.is_set() is False


# ---------------------------------------------------------------------------
# End-to-end via FastAPI TestClient (server.py + runner integrated)
# ---------------------------------------------------------------------------


def _build_test_webapp(
    *,
    arrange_impl: Any,
    complete_impl: Any,
    pool_results: list[Any],
) -> tuple[WebApp, _RecordingHandler]:
    """Build a fully-wired ``WebApp`` for TestClient assertions."""
    handler = _RecordingHandler()
    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl

    pool = _make_pool_returning(pool_results)
    app = _make_stub_app(handler, pool=pool, is_ready=True)
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=1000)],
    )
    return WebApp(app, config), handler


def test_route_returns_422_with_flat_envelope_for_malformed_body():
    """Malformed JSON → 422 with ``{'error': 'invalid_request', 'details': ...}``."""

    async def arrange_impl(req, pending):
        return []

    async def complete_impl(group):
        return _HttpResp()

    webapp, _ = _build_test_webapp(
        arrange_impl=arrange_impl,
        complete_impl=complete_impl,
        pool_results=[],
    )

    with TestClient(webapp._fastapi_app) as client:
        # Bytes body is not valid JSON → ValidationError-or-ValueError.
        response = client.post('/process', content=b'{not json')
        assert response.status_code == 422
        body = response.json()
        assert body['error'] == 'invalid_request'
        assert 'request_id' in body
        assert 'details' in body
        # FastAPI's default 422 envelope is ``{'detail': [...]}``;
        # our flat envelope must NOT include ``detail``.
        assert 'detail' not in body


def test_route_returns_500_when_arrange_http_request_raises():
    """User hook raising → 500 with ``{'status':'error', 'error':'internal error'}``."""

    async def arrange_impl(req, pending):
        raise RuntimeError('user bug')

    async def complete_impl(group):
        return _HttpResp()

    webapp, _ = _build_test_webapp(
        arrange_impl=arrange_impl,
        complete_impl=complete_impl,
        pool_results=[],
    )

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})
        assert response.status_code == 500
        body = response.json()
        assert body['status'] == 'error'
        assert body['error'] == 'internal error'
        assert 'request_id' in body
        # The traceback must NEVER appear in the response body.
        assert 'Traceback' not in str(body)
        assert 'user bug' not in str(body)


def test_route_returns_500_when_on_http_request_complete_raises():
    """User response hook raising → flat 500 (same envelope as arrange failures)."""

    async def arrange_impl(req, pending):
        return []

    async def complete_impl(group):
        raise ValueError('response build broke')

    webapp, _ = _build_test_webapp(
        arrange_impl=arrange_impl,
        complete_impl=complete_impl,
        pool_results=[],
    )

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})
        assert response.status_code == 500
        body = response.json()
        assert body['status'] == 'error'
        assert body['error'] == 'internal error'
        assert 'response build broke' not in str(body)


def test_route_returns_200_with_full_web_report_on_happy_path():
    """End-to-end: TestClient POST → 200 with a complete WebReport JSON."""
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    async def complete_impl(group):
        return _HttpResp(matches=group.succeeded, echoed_pattern='ok')

    webapp, _ = _build_test_webapp(
        arrange_impl=arrange_impl,
        complete_impl=complete_impl,
        pool_results=[_make_canned_result(task)],
    )

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'hi'})
        assert response.status_code == 200
        body = response.json()
        assert body['status'] == 'ok'
        assert body['client'] == 'anonymous'
        assert 'request_id' in body
        assert body['result'] == {'matches': 1, 'echoed_pattern': 'ok'}
        assert body['task_summary'] == {'total': 1, 'success': 1, 'failed': 0}
        assert body['sinks'] is None
        assert {s['stage'] for s in body['timeline']} == {
            'arrange',
            'execute',
            'on_http_request_complete',
        }
