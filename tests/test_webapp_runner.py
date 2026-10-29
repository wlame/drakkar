"""Tests for :class:`drakkar.webapp.runner.WebappRunner` — happy path + errors.

Coverage focus:

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
from drakkar.executor import ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    PendingContext,
    PrecomputedResult,
    make_task_id,
)
from drakkar.sinks.manager import SinkManager
from drakkar.webapp import WebApp
from drakkar.webapp.models import WebRequestContext
from drakkar.webapp.runner import WebappHandlerError, WebappRunner
from tests.test_sink_delivery_policy import FakeSink, policy

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
        raw_body=b'',
    )


def _make_pool_returning(results: list[Any]) -> MagicMock:
    """Build a stub executor pool whose ``execute`` returns canned results.

    ``results`` is consumed in order — one entry per call. Items can be
    :class:`ExecutorResult` for success or any ``BaseException`` instance to
    simulate a terminal failure (the runner treats both via
    ``return_exceptions=True`` and aggregates accordingly).
    """
    pool = MagicMock()
    iterator = iter(results)

    async def _execute(task: ExecutorTask, recorder, partition_id: int) -> ExecutorResult:
        outcome = next(iterator)
        if isinstance(outcome, BaseException):
            # BaseException (not just Exception) so CancelledError — which
            # asyncio raises for real on shutdown — is raised, not returned.
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

    # Sinks stays None on the deliver_to_sinks=False path (the default).
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
async def test_runner_synthetic_message_payload_is_parsed_request():
    """source_message.payload carries the SAME parsed model that
    arrange_http_request received — the webapp path upholds the Kafka-path
    invariant (deserialize_message sets payload before hooks fire)."""
    handler = _RecordingHandler()
    captured: dict[str, Any] = {}

    async def complete_impl(group):
        captured['group'] = group
        return _HttpResp()

    handler.on_http_request_complete_impl = complete_impl
    pool = _make_pool_returning([])
    runner = WebappRunner(_make_stub_app(handler, pool=pool), _make_config())
    ctx = _make_ctx()

    await runner.run(ctx)

    group = captured['group']
    assert group.source_message.payload is ctx.request
    assert isinstance(group.source_message.payload, _HttpReq)


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
    # The failed task is intentionally NOT in ``report.tasks`` — only
    # successful executions are reported there.
    assert len(report.tasks) == 1


@pytest.mark.asyncio
async def test_runner_surfaces_terminal_failures_in_group_errors():
    """An ExecutorTaskError outcome lands in group.errors — the SAME
    ExecutorError object the pool built, matching the Kafka path."""
    handler = _RecordingHandler()
    task_ok = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_bad = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_ok, task_bad]

    captured: dict[str, Any] = {}

    async def complete_impl(group):
        captured['group'] = group
        return _HttpResp()

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl

    task_error = ExecutorError(task=task_bad, exit_code=2, stderr='exploded')
    pool = _make_pool_returning(
        [
            _make_canned_result(task_ok),
            ExecutorTaskError(error=task_error, result=_make_canned_result(task_bad, exit_code=2)),
        ]
    )
    runner = WebappRunner(_make_stub_app(handler, pool=pool), _make_config())

    report = await runner.run(_make_ctx())

    group = captured['group']
    assert group.errors == [task_error]
    assert group.errors[0] is task_error
    assert len(group.results) == 1
    assert group.failed == 1
    assert report.task_summary.success == 1
    assert report.task_summary.failed == 1


@pytest.mark.asyncio
async def test_runner_synthesizes_executor_error_for_unexpected_exception():
    """A non-ExecutorTaskError exception becomes a synthesized ExecutorError
    bound to the right task — framework surprises stay visible."""
    handler = _RecordingHandler()
    task_ok = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_bad = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_ok, task_bad]

    captured: dict[str, Any] = {}

    async def complete_impl(group):
        captured['group'] = group
        return _HttpResp()

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl
    pool = _make_pool_returning([_make_canned_result(task_ok), RuntimeError('boom')])
    runner = WebappRunner(_make_stub_app(handler, pool=pool), _make_config())

    await runner.run(_make_ctx())

    group = captured['group']
    assert len(group.errors) == 1
    assert group.errors[0].task.task_id == task_bad.task_id
    assert group.errors[0].exception == 'RuntimeError: boom'
    assert group.errors[0].exit_code is None
    assert group.errors[0].kind == 'internal'


@pytest.mark.asyncio
async def test_runner_records_terminal_event_for_every_executed_task():
    """Each started task gets its task_completed / task_failed event —
    the pool records only task_started, so without these the live
    Timeline shows HTTP-origin tasks as running forever."""
    handler = _RecordingHandler()
    task_ok = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_exit = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    task_boom = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_ok, task_exit, task_boom]

    handler.arrange_http_request_impl = arrange_impl
    ok_result = _make_canned_result(task_ok)
    exit_error = ExecutorError(task=task_exit, exit_code=2, stderr='exploded')
    pool = _make_pool_returning(
        [
            ok_result,
            ExecutorTaskError(error=exit_error, result=_make_canned_result(task_exit, exit_code=2, duration=0.3)),
            RuntimeError('boom'),
        ]
    )
    recorder = MagicMock()
    runner = WebappRunner(_make_stub_app(handler, pool=pool, recorder=recorder), _make_config())

    await runner.run(_make_ctx())

    recorder.record_task_completed.assert_called_once()
    completed_args = recorder.record_task_completed.call_args
    assert completed_args.args == (ok_result, -1)

    failed_by_task = {c.args[0].task_id: c for c in recorder.record_task_failed.call_args_list}
    assert set(failed_by_task) == {task_exit.task_id, task_boom.task_id}
    exit_call = failed_by_task[task_exit.task_id]
    assert exit_call.args[1] is exit_error
    assert exit_call.args[2] == -1
    assert exit_call.kwargs['duration_seconds'] == 0.3
    boom_call = failed_by_task[task_boom.task_id]
    assert boom_call.args[1].kind == 'internal'
    assert boom_call.args[1].exception == 'RuntimeError: boom'


@pytest.mark.asyncio
async def test_runner_does_not_rerecord_precomputed_task_outcome():
    """The pool records both events for a precomputed task itself; the
    runner must not add a second completion row for it."""
    handler = _RecordingHandler()
    task = ExecutorTask(
        task_id=make_task_id('t'),
        source_offsets=[1],
        precomputed=PrecomputedResult(stdout='cached'),
    )

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_http_request_impl = arrange_impl
    pool = _make_pool_returning([_make_canned_result(task)])
    recorder = MagicMock()
    runner = WebappRunner(_make_stub_app(handler, pool=pool, recorder=recorder), _make_config())

    await runner.run(_make_ctx())

    recorder.record_task_completed.assert_not_called()
    recorder.record_task_failed.assert_not_called()


@pytest.mark.asyncio
async def test_runner_reraises_cancelled_error_from_pool():
    """A CancelledError outcome aborts the request — shutdown must never
    masquerade as a task failure, and the response hook must not run."""
    handler = _RecordingHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    hook_calls: list[Any] = []

    async def complete_impl(group):
        hook_calls.append(group)
        return _HttpResp()

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl
    pool = _make_pool_returning([asyncio.CancelledError()])
    runner = WebappRunner(_make_stub_app(handler, pool=pool), _make_config())

    with pytest.raises(asyncio.CancelledError):
        await runner.run(_make_ctx())

    assert hook_calls == []


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
async def test_runner_skips_on_http_request_complete_when_cancelled_pre_hook():
    """ctx.cancelled set before on_http_request_complete → hook NOT called.

    Drives the runner with a successful execute, then flips
    ctx.cancelled while the runner is between gather() and the
    response hook. The runner's pre-hook gate trips and raises
    CancelledError; the user hook never runs.
    """
    handler = _RecordingHandler()
    task_a = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_a]

    complete_called: list[Any] = []

    async def complete_impl(group):
        complete_called.append(group)
        return _HttpResp()

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl

    canned = _make_canned_result(task_a)
    pool = _make_pool_returning([canned])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    ctx = _make_ctx()
    # Pre-allocate ctx.cancelled on this loop AND set it. The runner's
    # "if ctx.cancelled is None" branch leaves our pre-set Event alone.
    ctx.cancelled = asyncio.Event()
    ctx.cancelled.set()

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    # The user response hook never ran — runner short-circuited at the
    # post-execute / pre-hook gate.
    assert complete_called == []


@pytest.mark.asyncio
async def test_runner_logs_dropped_after_timeout_event(monkeypatch):
    """Cancellation after tasks complete logs ``webapp_request_dropped_after_timeout``.

    Uses ``structlog.testing.capture_logs`` to assert the structured
    log event fires with the expected category + request_id when the
    runner trips its post-execute gate.
    """
    import structlog
    from structlog.testing import capture_logs

    handler = _RecordingHandler()
    task_a = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_a]

    handler.arrange_http_request_impl = arrange_impl

    pool = _make_pool_returning([_make_canned_result(task_a)])
    app = _make_stub_app(handler, pool=pool)
    runner = WebappRunner(app, _make_config())

    ctx = _make_ctx(client_name='tenant-A', request_id='req_test_drop_1')
    ctx.cancelled = asyncio.Event()
    ctx.cancelled.set()

    # ``capture_logs`` requires structlog's contextvars binding; ensure
    # the logger we use (drakkar.webapp.runner) routes through it.
    structlog.configure()  # idempotent — restores defaults if previously cleared

    with capture_logs() as cap, pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    events = [e for e in cap if e.get('event') == 'webapp_request_dropped_after_timeout']
    assert events, f'expected webapp_request_dropped_after_timeout in {cap}'
    event = events[0]
    assert event['category'] == 'webapp'
    assert event['request_id'] == 'req_test_drop_1'
    assert event['client'] == 'tenant-A'


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
    # been set (the runner itself never trips cancellation; the
    # dispatch wiring owns the set/check pair).
    assert isinstance(ctx.cancelled, asyncio.Event)
    assert ctx.cancelled.is_set() is False


# ---------------------------------------------------------------------------
# Sinks-enabled delivery path
#
# These tests focus on the runner's optional sinks block: it triggers
# only when ``WebAppConfig.deliver_to_sinks=True``, calls
# ``handler.on_message_complete(group)`` for the synthetic group, and
# delivers any returned ``CollectResult`` through the SinkManager. The
# runner accumulates per-sink-type outcomes into a ``SinkDeliverySummary``
# that lands on ``WebReport.sinks``. The tests here exercise:
#
#   * happy path → summary populated with attempted/delivered counts;
#   * a validation failure or a raising ``deliver_all`` → recorded in the summary;
#   * ``on_message_complete`` returning ``None`` → ``WebReport.sinks=None``;
#   * ``deliver_to_sinks=False`` → ``on_message_complete`` not called;
#   * ``on_message_complete`` raising → wrapped in ``WebappHandlerError``.
# ---------------------------------------------------------------------------


class _SinkOut(BaseModel):
    """Tiny payload model used by the sinks tests (any BaseModel works)."""

    value: int = 0


def _make_collect_result_kafka_postgres(
    *,
    kafka_count: int = 1,
    postgres_count: int = 1,
):
    """Build a ``CollectResult`` carrying the requested kafka+postgres payloads."""
    from drakkar.models import CollectResult, KafkaPayload, PostgresPayload

    return CollectResult(
        kafka=[KafkaPayload(data=_SinkOut(value=i)) for i in range(kafka_count)],
        postgres=[PostgresPayload(table='out', data=_SinkOut(value=i)) for i in range(postgres_count)],
    )


def _make_deliver_to_sinks_config() -> WebAppConfig:
    """Build a ``WebAppConfig`` with sinks enabled — used by the sinks tests."""
    return WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        deliver_to_sinks=True,
        clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
    )


class _RecordingSinkManager(SinkManager):
    """A real SinkManager over one in-memory kafka and one postgres sink, recording each deliver_all call."""

    def __init__(self) -> None:
        super().__init__()
        self.kafka = FakeSink('default', 'kafka')
        self.postgres = FakeSink('default', 'postgres')
        self.register(self.kafka, policy=policy())
        self.register(self.postgres, policy=policy())
        self.attach_runtime(recorder=None, dlq_sink=None, dlq_on_send_failure='drop', dlq_unconfigured=True)
        self.deliver_calls: list[CollectResult] = []

    async def deliver_all(self, result, on_delivery_error, partition_id, **kwargs):
        self.deliver_calls.append(result)
        return await super().deliver_all(result, on_delivery_error, partition_id, **kwargs)


def _make_stub_app_with_sinks(
    handler: BaseDrakkarHandler,
    *,
    pool: Any,
    sink_manager: SinkManager,
) -> Any:
    """Build a stub ``DrakkarApp`` that exposes ``_sink_manager``."""
    app = MagicMock()
    app._handler = handler
    app._executor_pool = pool
    app._recorder = None
    app._sink_manager = sink_manager
    app.is_ready = True
    app.main_loop = None
    return app


@pytest.mark.asyncio
async def test_runner_deliver_to_sinks_happy_path_populates_summary():
    """deliver_to_sinks + on_message_complete returns CollectResult → summary populated."""
    handler = _RecordingHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_http_request_impl = arrange_impl

    # The default on_message_complete on BaseDrakkarHandler returns
    # None; override it to ship a CollectResult with kafka+postgres.
    captured_collect = _make_collect_result_kafka_postgres(kafka_count=2, postgres_count=3)

    async def on_message_complete_impl(group):
        return captured_collect

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([_make_canned_result(task)])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    report = await runner.run(_make_ctx())

    # Summary must populate with both sink types.
    assert report.sinks is not None
    assert set(report.sinks.by_type.keys()) == {'kafka', 'postgres'}

    kafka_summary = report.sinks.by_type['kafka']
    assert kafka_summary.attempted == 2
    assert kafka_summary.delivered == 2
    assert kafka_summary.dlq == 0
    assert kafka_summary.errors == []

    pg_summary = report.sinks.by_type['postgres']
    assert pg_summary.attempted == 3
    assert pg_summary.delivered == 3
    assert pg_summary.dlq == 0
    assert pg_summary.errors == []

    # One delivery pass for the whole request.
    assert len(sink_manager.deliver_calls) == 1
    assert [len(batch) for batch in sink_manager.kafka.delivered] == [2]
    assert [len(batch) for batch in sink_manager.postgres.delivered] == [3]

    # Timeline records the new ``sinks`` stage.
    stages = [s.stage for s in report.timeline]
    assert 'sinks' in stages


class _RaisingSinkManager(_RecordingSinkManager):
    """A sink manager whose deliver_all raises directly — simulates a
    catastrophic SinkManager-internal failure rather than the normal
    per-sink outcome."""

    async def deliver_all(self, result, on_delivery_error, partition_id, **kwargs):
        raise RuntimeError('sink manager internal failure')


@pytest.mark.asyncio
async def test_runner_deliver_all_raising_records_catastrophic_failure():
    """A deliver_all that raises directly (rather than routing through
    on_delivery_error) is a rare, catastrophic SinkManager-internal
    failure — it must still be captured in the summary rather than
    crashing the whole request, since the user-facing response was
    already built by on_http_request_complete."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        return []

    handler.arrange_http_request_impl = arrange_impl

    async def on_message_complete_impl(group):
        return _make_collect_result_kafka_postgres(kafka_count=1, postgres_count=0)

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([])
    sink_manager = _RaisingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    report = await runner.run(_make_ctx())

    assert report.status == 'ok'
    assert report.sinks is not None
    kafka_summary = report.sinks.by_type['kafka']
    assert kafka_summary.delivered == 0
    assert any('deliver_all_raised' in e for e in kafka_summary.errors)


@pytest.mark.asyncio
async def test_runner_deliver_to_sinks_but_on_message_complete_returns_none():
    """deliver_to_sinks=True + on_message_complete returns None → WebReport.sinks=None."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        return []

    handler.arrange_http_request_impl = arrange_impl

    on_message_complete_calls: list[Any] = []

    async def on_message_complete_impl(group):
        on_message_complete_calls.append(group)
        return None

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    report = await runner.run(_make_ctx())

    # Hook fired; sinks block stayed off the response.
    assert len(on_message_complete_calls) == 1
    assert report.sinks is None
    # The deliver path was not exercised — no dispatches.
    assert sink_manager.deliver_calls == []


@pytest.mark.asyncio
async def test_runner_sink_validation_failure_records_error_and_skips_that_sink_only():
    """A validate_collect failure for one sink type must not abort the
    request — the user-facing response was already built by
    on_http_request_complete. The summary records the validation error
    for that sink type and delivery still proceeds for the others."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        return []

    handler.arrange_http_request_impl = arrange_impl

    from drakkar.models import KafkaPayload, PostgresPayload

    collected = CollectResult(
        kafka=[KafkaPayload(sink='nope', data=_SinkOut())],
        postgres=[PostgresPayload(table='out', data=_SinkOut())],
    )

    async def on_message_complete_impl(group):
        return collected

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    report = await runner.run(_make_ctx())

    kafka_summary = report.sinks.by_type['kafka']
    assert kafka_summary.delivered == 0
    assert any('validation_error' in e for e in kafka_summary.errors)

    # The other sink type was unaffected — validation failure is scoped
    # to the sink type that raised, not the whole request.
    postgres_summary = report.sinks.by_type['postgres']
    assert postgres_summary.delivered == 1
    assert postgres_summary.errors == []
    # Only postgres reached deliver_all — kafka was left out after its
    # validation error.
    [delivered] = sink_manager.deliver_calls
    assert (delivered.kafka, len(delivered.postgres)) == ([], 1)


@pytest.mark.asyncio
async def test_runner_drops_request_when_timeout_detected_before_response_hook():
    """A timeout detected after the sinks stage finishes (but before the
    user's response hook runs) must still skip on_http_request_complete.

    This exercises the ``pre_on_http_request_complete`` gate, distinct
    from the post-execute gate: T2 may
    504 the caller while ``_deliver_sinks`` is still on the call stack,
    and the user's response hook must not run afterward on a request the
    caller already gave up on. This is NOT the ``during_sinks`` gate
    between sink groups — ``on_message_complete`` returns ``None`` below,
    so ``_deliver_sinks`` short-circuits before any delivery is reached.
    The ``during_sinks`` gate itself, with its recorder row, is covered by
    ``test_http_cancellation_between_groups_keeps_its_log_event`` in
    ``test_webapp_delivery_policy.py``.
    """
    handler = _RecordingHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_http_request_impl = arrange_impl

    ctx = _make_ctx(request_id='req_test_drop_2')
    ctx.cancelled = asyncio.Event()

    # Flip the cancellation flag from inside on_message_complete — this
    # simulates T2's timeout landing after the post-execute gate already
    # passed (ctx.cancelled was clear then) but before the
    # pre_on_http_request_complete gate runs. on_message_complete
    # returning None means _deliver_sinks short-circuits immediately
    # (see its own "no payloads -> no sink writes" branch), so the
    # in-loop during_sinks gate is never reached here.
    async def on_message_complete_impl(group):
        ctx.cancelled.set()
        return None

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    response_called = False

    async def complete_impl(group):
        nonlocal response_called
        response_called = True
        return _HttpResp()

    handler.on_http_request_complete_impl = complete_impl

    pool = _make_pool_returning([_make_canned_result(task)])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    # The response hook must never run once the second gate has tripped.
    assert response_called is False


@pytest.mark.asyncio
async def test_runner_sinks_disabled_does_not_call_on_message_complete():
    """deliver_to_sinks=False → on_message_complete is NOT called and sinks=None."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        return []

    handler.arrange_http_request_impl = arrange_impl

    on_message_complete_calls: list[Any] = []

    async def on_message_complete_impl(group):
        on_message_complete_calls.append(group)
        return _make_collect_result_kafka_postgres()

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    # Default config has deliver_to_sinks=False — verify the default.
    config = _make_config()
    assert config.deliver_to_sinks is False
    runner = WebappRunner(app, config)

    report = await runner.run(_make_ctx())

    assert on_message_complete_calls == []
    assert report.sinks is None
    assert sink_manager.deliver_calls == []


@pytest.mark.asyncio
async def test_runner_sinks_on_message_complete_raising_wraps_handler_error():
    """on_message_complete raising → WebappHandlerError(where='on_message_complete')."""
    handler = _RecordingHandler()

    async def arrange_impl(req, pending):
        return []

    handler.arrange_http_request_impl = arrange_impl

    async def on_message_complete_impl(group):
        raise RuntimeError('on_message_complete exploded')

    handler.on_message_complete = on_message_complete_impl  # type: ignore[method-assign]

    pool = _make_pool_returning([])
    sink_manager = _RecordingSinkManager()
    app = _make_stub_app_with_sinks(handler, pool=pool, sink_manager=sink_manager)
    runner = WebappRunner(app, _make_deliver_to_sinks_config())

    with pytest.raises(WebappHandlerError) as exc_info:
        await runner.run(_make_ctx())

    err = exc_info.value
    assert err.where == 'on_message_complete'
    assert isinstance(err.original_exc, RuntimeError)
    assert 'on_message_complete exploded' in str(err.original_exc)
    # Sink delivery never started.
    assert sink_manager.deliver_calls == []


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


# ---------------------------------------------------------------------------
# Request body serialization
# ---------------------------------------------------------------------------


def test_serialize_request_body_passes_bytes_through_unchanged():
    """Raw bytes/bytearray bodies (the synthetic-context test path, and any
    caller that already has an encoded body) must pass through untouched —
    re-encoding them would corrupt binary payloads."""
    from drakkar.webapp.runner import WebappRunner

    assert WebappRunner._serialize_request_body(b'raw-bytes') == b'raw-bytes'
    assert WebappRunner._serialize_request_body(bytearray(b'raw-bytearray')) == b'raw-bytearray'


def test_serialize_request_body_falls_back_to_str_encoding_for_non_pydantic_input():
    """A non-BaseModel, non-bytes body (plain values used by synthetic-context
    tests that don't wrap fixtures in a model) falls back to str().encode()
    rather than raising — production always sees a BaseModel, so this is a
    deliberate best-effort path, not a silent data-corruption risk."""
    from drakkar.webapp.runner import WebappRunner

    assert WebappRunner._serialize_request_body(42) == b'42'
    assert WebappRunner._serialize_request_body({'a': 1}) == str({'a': 1}).encode('utf-8')


def test_split_collect_by_type_covers_every_sink_type():
    """The per-sink-type split must route every CollectResult field to its
    own batch — missing one here would silently drop that sink type's
    payloads from webapp delivery whenever deliver_to_sinks=True. kafka and
    postgres are exercised elsewhere; this pins down the remaining four."""
    from drakkar.models import CollectResult, FilePayload, HttpPayload, MongoPayload, RedisPayload

    result = CollectResult(
        mongo=[MongoPayload(collection='out', data=_SinkOut(value=1))],
        http=[HttpPayload(data=_SinkOut(value=2))],
        redis=[RedisPayload(key='k', data=_SinkOut(value=3))],
        files=[FilePayload(path='out.jsonl', data=_SinkOut(value=4))],
    )

    groups = WebappRunner._split_collect_by_type(result)

    # Deterministic order, one entry per populated field, each sub-result
    # carrying only that field's own payload.
    assert [sink_type for sink_type, _ in groups] == ['mongo', 'http', 'redis', 'filesystem']
    by_type = dict(groups)
    assert len(by_type['mongo'].mongo) == 1
    assert len(by_type['http'].http) == 1
    assert len(by_type['redis'].redis) == 1
    assert len(by_type['filesystem'].files) == 1


@pytest.mark.asyncio
async def test_runner_records_dropped_request_when_recorder_configured():
    """When a recorder is configured, the post-execute cancellation gate
    must also persist a row via record_webapp_request_dropped_after_timeout
    — not just log — so the debug UI surfaces dropped requests too."""
    handler = _RecordingHandler()
    task_a = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task_a]

    handler.arrange_http_request_impl = arrange_impl

    pool = _make_pool_returning([_make_canned_result(task_a)])
    recorder = MagicMock()
    app = _make_stub_app(handler, pool=pool, recorder=recorder)
    runner = WebappRunner(app, _make_config())

    ctx = _make_ctx(request_id='req_test_drop_recorder')
    ctx.cancelled = asyncio.Event()
    ctx.cancelled.set()

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    recorder.record_webapp_request_dropped_after_timeout.assert_called_once_with(ctx)


@pytest.mark.asyncio
async def test_runner_submit_tasks_raises_when_pool_not_initialised():
    """A webapp request arriving before AppLifecycle finishes constructing
    the executor pool must fail loudly with a clear message, not with a
    confusing AttributeError deep inside the submission path."""
    handler = _RecordingHandler()
    app = _make_stub_app(handler, pool=None)
    runner = WebappRunner(app, _make_config())

    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])
    with pytest.raises(RuntimeError, match='executor pool is not initialised'):
        await runner._submit_tasks([task])
