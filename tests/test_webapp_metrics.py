"""Tests for webapp metrics (Task 7).

Covers the five metrics added to :mod:`drakkar.metrics`:

* ``drakkar_webapp_requests_total`` — Counter labelled by client/status
* ``drakkar_webapp_request_duration_seconds`` — Histogram (same labels)
* ``drakkar_webapp_inflight`` — Gauge (no labels)
* ``drakkar_webapp_dropped_after_timeout_total`` — Counter labelled by client
* ``drakkar_webapp_rpm_limit`` — Gauge labelled by client (informational)

Each test exercises one outcome path end-to-end (status='ok',
'auth_failed', 'rate_limited', 'timeout', 'capacity', 'shutdown',
'not_ready', 'error') and asserts that the matching counter ticked by 1
with the right labels. The auth-failed path always uses the fixed
``client='unauthenticated'`` sentinel — verified explicitly so a
regression that derives the label from the failed token (cardinality
explosion) is caught.

The inflight gauge tests exercise three concurrent in-flight requests
to verify entry/exit accounting holds under contention; the
``dropped_after_timeout`` counter test drives the runner directly with
``ctx.cancelled`` pre-set so the post-execute gate fires.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from prometheus_client.metrics import MetricWrapperBase
from pydantic import BaseModel

from drakkar import metrics
from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import ExecutorResult, ExecutorTask, make_task_id
from drakkar.webapp import WebApp
from drakkar.webapp.models import WebRequestContext
from drakkar.webapp.runner import WebappRunner

# ---------------------------------------------------------------------------
# Tiny Pydantic models + handler with overridable HTTP hooks
# ---------------------------------------------------------------------------


class _Input(BaseModel):
    a: int = 0


class _Output(BaseModel):
    b: int = 0


class _HttpReq(BaseModel):
    pattern: str = ''


class _HttpResp(BaseModel):
    matches: int = 0


class _MetricsHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Handler with overridable HTTP hooks for metric tests."""

    def __init__(self) -> None:
        super().__init__()
        self.arrange_http_request_impl: Any = self._default_arrange
        self.on_http_request_complete_impl: Any = self._default_complete

    async def arrange(self, messages, pending):
        return []

    async def _default_arrange(self, req, pending) -> list[ExecutorTask]:
        return []

    async def _default_complete(self, group) -> _HttpResp:
        return _HttpResp(matches=0)

    async def arrange_http_request(self, req, pending):
        return await self.arrange_http_request_impl(req, pending)

    async def on_http_request_complete(self, group):
        return await self.on_http_request_complete_impl(group)


# ---------------------------------------------------------------------------
# Metric reading helpers
# ---------------------------------------------------------------------------


def _counter_value(metric: MetricWrapperBase, **labels: str) -> float:
    """Read the current value of a (possibly labelled) prometheus counter."""
    if labels:
        return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]
    return metric._value.get()  # type: ignore[attr-defined]


def _gauge_value(metric: MetricWrapperBase, **labels: str) -> float:
    """Read the current value of a (possibly labelled) prometheus gauge."""
    if labels:
        return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]
    return metric._value.get()  # type: ignore[attr-defined]


def _histogram_sample_count(metric: MetricWrapperBase, labels: dict[str, str]) -> float:
    """Walk the histogram's collected samples to find the ``_count`` series.

    prometheus_client exposes a synthetic ``<name>_count`` sample under
    each labelled child via ``collect()``. We iterate the family's
    samples rather than reading private state on the labelled child so
    a prometheus_client upgrade that changes the private representation
    does not break the test helper.
    """
    for family in metric.collect():
        for sample in family.samples:
            if sample.name.endswith('_count') and sample.labels == labels:
                return sample.value
    return 0.0


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _make_app_stub(handler: BaseDrakkarHandler, *, is_ready: bool = True, pool: Any = None) -> Any:
    """Build a minimal DrakkarApp substitute for runner / route tests."""
    app = MagicMock()
    app._handler = handler
    app._executor_pool = pool
    app._recorder = None
    app.is_ready = is_ready
    app.main_loop = None
    return app


def _make_pool_returning(results: list[Any]) -> MagicMock:
    """Build a stub executor pool whose ``execute`` returns canned results."""
    pool = MagicMock()
    iterator = iter(results)

    async def _execute(task: ExecutorTask, recorder, partition_id: int) -> ExecutorResult:
        outcome = next(iterator)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    pool.execute = _execute
    return pool


def _make_canned_result(task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(
        exit_code=0,
        stdout='',
        stderr='',
        duration_seconds=0.01,
        task=task,
        pid=12345,
    )


def _make_webapp(*, rpm: int = 4, request_timeout_seconds: float = 30.0, max_concurrent: int = 64) -> WebApp:
    """Build a fully-wired WebApp for TestClient / metric assertions."""
    handler = _MetricsHandler()
    app = _make_app_stub(handler, is_ready=True, pool=_make_pool_returning([]))
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        request_timeout_seconds=request_timeout_seconds,
        max_concurrent=max_concurrent,
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=rpm),
            WebClientConfig(name='tenant-A', token='token-a-secret', rpm=rpm),
        ],
    )
    return WebApp(app, config)


# ---------------------------------------------------------------------------
# Counter / histogram for each outcome path (via TestClient)
# ---------------------------------------------------------------------------


def test_happy_path_increments_requests_ok_and_duration():
    """200 OK → webapp_requests_total{status='ok',client=name}+1; histogram observes."""
    webapp = _make_webapp(rpm=10)
    before_count = _counter_value(metrics.webapp_requests, client='tenant-A', status='ok')
    before_hist = _histogram_sample_count(
        metrics.webapp_request_duration,
        {'client': 'tenant-A', 'status': 'ok'},
    )

    with TestClient(webapp._fastapi_app) as client:
        response = client.post(
            '/process',
            json={'pattern': 'x'},
            headers={'Authorization': 'Bearer token-a-secret'},
        )

    assert response.status_code == 200
    after_count = _counter_value(metrics.webapp_requests, client='tenant-A', status='ok')
    after_hist = _histogram_sample_count(
        metrics.webapp_request_duration,
        {'client': 'tenant-A', 'status': 'ok'},
    )
    assert after_count == before_count + 1
    assert after_hist == before_hist + 1


def test_auth_failed_increments_unauthenticated_sentinel_only():
    """401 → webapp_requests_total{status='auth_failed',client='unauthenticated'}+1.

    Cardinality safety: even though the client sent ``Bearer not-a-real-token``,
    the metric label MUST stay at the fixed ``unauthenticated`` sentinel —
    deriving it from the unknown token would let a hostile client explode
    the time-series count.
    """
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='tenant-A', token='token-a-secret', rpm=4)],
    )
    handler = _MetricsHandler()
    webapp = WebApp(_make_app_stub(handler, is_ready=True), config)
    before = _counter_value(metrics.webapp_requests, client='unauthenticated', status='auth_failed')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post(
            '/process',
            json={'pattern': 'x'},
            headers={'Authorization': 'Bearer not-a-real-token'},
        )

    assert response.status_code == 401
    after = _counter_value(metrics.webapp_requests, client='unauthenticated', status='auth_failed')
    assert after == before + 1


def test_rate_limited_increments_with_real_client_label():
    """429 → webapp_requests_total{status='rate_limited',client=<matched>}+1."""
    webapp = _make_webapp(rpm=2)
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='rate_limited')

    with TestClient(webapp._fastapi_app) as client:
        # Burn the cap as anonymous (first two admit, third rate-limits).
        for _ in range(2):
            assert client.post('/process', json={'pattern': 'x'}).status_code == 200
        response = client.post('/process', json={'pattern': 'x'})

    assert response.status_code == 429
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='rate_limited')
    assert after == before + 1


def test_internal_error_increments_status_error_with_real_client():
    """500 (handler raised) → webapp_requests_total{status='error',client=<matched>}+1."""
    handler = _MetricsHandler()

    async def _raise(req, pending):
        raise RuntimeError('handler bug')

    handler.arrange_http_request_impl = _raise
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=10)],
    )
    app_stub = _make_app_stub(handler, is_ready=True, pool=_make_pool_returning([]))
    webapp = WebApp(app_stub, config)
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='error')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})

    assert response.status_code == 500
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='error')
    assert after == before + 1


def test_invalid_request_body_increments_status_error():
    """422 (malformed JSON) → webapp_requests_total{status='error',client=<matched>}+1."""
    webapp = _make_webapp(rpm=10)
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='error')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', content=b'{not json')

    assert response.status_code == 422
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='error')
    assert after == before + 1


def test_shutdown_gate_increments_status_shutdown():
    """503 (shutdown_event set) → webapp_requests_total{status='shutdown'}+1."""
    webapp = _make_webapp(rpm=10)
    webapp.shutdown_event.set()
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='shutdown')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})

    assert response.status_code == 503
    assert response.json()['status'] == 'shutdown'
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='shutdown')
    assert after == before + 1


def test_not_ready_gate_increments_status_not_ready():
    """503 (app not ready) → webapp_requests_total{status='not_ready'}+1."""
    handler = _MetricsHandler()
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=10)],
    )
    # is_ready=False trips the not-ready gate.
    app_stub = _make_app_stub(handler, is_ready=False, pool=_make_pool_returning([]))
    webapp = WebApp(app_stub, config)
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='not_ready')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})

    assert response.status_code == 503
    assert response.json()['status'] == 'not_ready'
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='not_ready')
    assert after == before + 1


def test_capacity_gate_increments_status_capacity():
    """503 (max_concurrent=0) → webapp_requests_total{status='capacity'}+1.

    Building the webapp with max_concurrent=0 leaves the semaphore with
    no permits, so every request fails the acquire-probe and falls
    through the capacity branch. (The config validator forbids
    max_concurrent<=0 at config load, so we patch directly post-construct.)
    """
    webapp = _make_webapp(rpm=10, max_concurrent=1)
    # Drain the semaphore manually so any acquire-probe times out —
    # simulates a fully-loaded webapp without the indirection of
    # spinning up multiple concurrent slow requests.
    webapp._semaphore = asyncio.Semaphore(0)
    before = _counter_value(metrics.webapp_requests, client='anonymous', status='capacity')

    with TestClient(webapp._fastapi_app) as client:
        response = client.post('/process', json={'pattern': 'x'})

    assert response.status_code == 503
    assert response.json()['status'] == 'capacity'
    after = _counter_value(metrics.webapp_requests, client='anonymous', status='capacity')
    assert after == before + 1


# ---------------------------------------------------------------------------
# Inflight gauge: tracks runner entry/exit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inflight_gauge_increments_on_entry_and_decrements_in_finally():
    """Inflight gauge: +1 on runner entry, -1 in finally (verified mid-run)."""
    handler = _MetricsHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_http_request_impl = arrange_impl

    # Capture the inflight gauge mid-flight by reading it from inside
    # ``on_http_request_complete``. The gauge MUST read 1 there because
    # the runner is mid-run; before/after the run it MUST be back at the
    # baseline.
    captured: dict[str, float] = {}

    async def complete_impl(group):
        captured['mid'] = _gauge_value(metrics.webapp_inflight)
        return _HttpResp(matches=0)

    handler.on_http_request_complete_impl = complete_impl

    pool = _make_pool_returning([_make_canned_result(task)])
    app_stub = _make_app_stub(handler, pool=pool)
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=10)],
    )
    runner = WebappRunner(app_stub, config)

    before = _gauge_value(metrics.webapp_inflight)
    ctx = WebRequestContext(
        request_id='req_test_inflight_1',
        client_name='anonymous',
        request=_HttpReq(pattern='x'),
        started_at=datetime.now(UTC),
        headers={},
    )
    await runner.run(ctx)
    after = _gauge_value(metrics.webapp_inflight)

    assert captured['mid'] == before + 1.0
    assert after == before


@pytest.mark.asyncio
async def test_inflight_gauge_decrements_on_handler_error():
    """Inflight gauge releases its increment even when the runner raises."""
    handler = _MetricsHandler()

    async def arrange_impl(req, pending):
        raise RuntimeError('arrange exploded')

    handler.arrange_http_request_impl = arrange_impl

    app_stub = _make_app_stub(handler, pool=_make_pool_returning([]))
    runner = WebappRunner(app_stub, _make_runner_config())

    before = _gauge_value(metrics.webapp_inflight)
    ctx = WebRequestContext(
        request_id='req_test_inflight_2',
        client_name='anonymous',
        request=_HttpReq(pattern='x'),
        started_at=datetime.now(UTC),
        headers={},
    )

    from drakkar.webapp.runner import WebappHandlerError

    with pytest.raises(WebappHandlerError):
        await runner.run(ctx)

    after = _gauge_value(metrics.webapp_inflight)
    assert after == before


def _make_runner_config() -> WebAppConfig:
    return WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
    )


@pytest.mark.asyncio
async def test_inflight_gauge_tracks_three_concurrent_requests():
    """Three concurrent slow requests: gauge reads 3 mid-flight, 0 after.

    Builds three runner.run() coroutines whose handlers block on a
    barrier until the test inspects the gauge, then releases them.
    Verifies the gauge accumulates entries from concurrent runs.
    """
    handler = _MetricsHandler()

    # Each invocation of on_http_request_complete waits on this event so
    # we can observe the gauge while three runs are mid-flight.
    release = asyncio.Event()
    arrived = asyncio.Event()
    arrival_count = {'n': 0}

    async def complete_impl(group):
        arrival_count['n'] += 1
        if arrival_count['n'] == 3:
            arrived.set()
        await release.wait()
        return _HttpResp(matches=0)

    handler.on_http_request_complete_impl = complete_impl

    app_stub = _make_app_stub(handler, pool=_make_pool_returning([]))
    runner = WebappRunner(app_stub, _make_runner_config())

    before = _gauge_value(metrics.webapp_inflight)

    async def _one():
        ctx = WebRequestContext(
            request_id=f'req_test_concurrent_{id(asyncio.current_task())}',
            client_name='anonymous',
            request=_HttpReq(pattern='x'),
            started_at=datetime.now(UTC),
            headers={},
        )
        await runner.run(ctx)

    tasks = [asyncio.create_task(_one()) for _ in range(3)]
    # Wait until all three are waiting inside on_http_request_complete.
    await asyncio.wait_for(arrived.wait(), timeout=2.0)

    mid = _gauge_value(metrics.webapp_inflight)
    assert mid == before + 3.0

    # Release and wait for completion; the gauge must return to baseline.
    release.set()
    await asyncio.gather(*tasks)
    after = _gauge_value(metrics.webapp_inflight)
    assert after == before


# ---------------------------------------------------------------------------
# webapp_dropped_after_timeout counter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dropped_after_timeout_increments_when_cancellation_set():
    """ctx.cancelled set after task execution → counter ticks for that client."""
    handler = _MetricsHandler()
    task = ExecutorTask(task_id=make_task_id('t'), source_offsets=[1])

    async def arrange_impl(req, pending):
        return [task]

    handler.arrange_http_request_impl = arrange_impl

    pool = _make_pool_returning([_make_canned_result(task)])
    app_stub = _make_app_stub(handler, pool=pool)
    runner = WebappRunner(app_stub, _make_runner_config())

    before = _counter_value(metrics.webapp_dropped_after_timeout, client='tenant-A')

    ctx = WebRequestContext(
        request_id='req_test_drop_metric',
        client_name='tenant-A',
        request=_HttpReq(pattern='x'),
        started_at=datetime.now(UTC),
        headers={},
    )
    # Pre-allocate cancelled and trip it so the post-execute gate fires.
    ctx.cancelled = asyncio.Event()
    ctx.cancelled.set()

    with pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    after = _counter_value(metrics.webapp_dropped_after_timeout, client='tenant-A')
    assert after == before + 1


# ---------------------------------------------------------------------------
# webapp_rpm_limit gauge: set once at startup
# ---------------------------------------------------------------------------


def test_rpm_limit_gauge_set_for_each_client_at_startup():
    """WebApp construction sets webapp_rpm_limit{client=name} for every client."""
    handler = _MetricsHandler()
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=7),
            WebClientConfig(name='tenant-A', token='token-a', rpm=42),
            WebClientConfig(name='tenant-B', token='token-b', rpm=100),
        ],
    )
    WebApp(_make_app_stub(handler, is_ready=True), config)

    assert _gauge_value(metrics.webapp_rpm_limit, client='anonymous') == 7.0
    assert _gauge_value(metrics.webapp_rpm_limit, client='tenant-A') == 42.0
    assert _gauge_value(metrics.webapp_rpm_limit, client='tenant-B') == 100.0
