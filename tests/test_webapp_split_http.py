"""The HTTP source in job mode: with ``split.http.enabled`` a POST stores a split job and a GET reads its status.

The gateway is a fake that runs only on the main loop (a real loop on a
helper thread here), records every call and the thread it ran on, and
answers what the test sets.
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

import drakkar.webapp.server as webapp_server
from drakkar import metrics
from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.providers.resilience import RateLimitedLog
from drakkar.request_ids import RequestIdResolver
from drakkar.split.http_jobs import HttpAnswer
from drakkar.webapp import WebApp

PATH = '/process'
TOKEN_A = 'token-a-secret'
TOKEN_B = 'token-b-secret'


class _Body(BaseModel):
    value: int


class _ModelSplitHandler(BaseDrakkarHandler[_Body, _Body]):
    """A splitting handler with an input model and no HTTP generics or hooks."""

    async def arrange(self, messages, pending):
        return []

    async def split(self, message):
        yield message.payload


class _RawSplitHandler(BaseDrakkarHandler):
    """A splitting handler with no input model."""

    async def arrange(self, messages, pending):
        return []

    async def split(self, message):
        yield message.value


class _HeaderIdHandler(_ModelSplitHandler):
    """Takes the request id from the ``x-request-id`` header; answers an invalid id for ``bad``."""

    def request_id(self, msg):
        for name, value in msg.headers or []:
            if name == 'x-request-id':
                text = value.decode('utf-8')
                return 'has space' if text == 'bad' else text
        return 'no-header'


class _MainLoop:
    """A real event loop on a helper thread: the worker's main loop."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread_id: int | None = None
        ready = threading.Event()

        def _run() -> None:
            asyncio.set_event_loop(self.loop)
            self.thread_id = threading.get_ident()
            ready.set()
            self.loop.run_forever()

        self._thread = threading.Thread(target=_run, name='main-loop', daemon=True)
        self._thread.start()
        ready.wait(timeout=2.0)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=2.0)
        self.loop.close()


class _FakeGateway:
    """Records calls; refuses any loop but the main loop, like the real gateway."""

    def __init__(self, main: _MainLoop) -> None:
        self._main = main
        self.is_ready = True
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.threads: list[int] = []
        self.hang = False
        self.cancelled = threading.Event()
        self.accept_answer = HttpAnswer(202, b'{"request_id":"r-1","status":"accepted"}', 'accepted')
        self.read_answer = HttpAnswer(200, b'{"request_id":"r-1","status":"running"}', 'status_read')

    async def _enter(self, name: str, kwargs: dict[str, Any]) -> None:
        if asyncio.get_running_loop() is not self._main.loop:
            raise RuntimeError('the HTTP job gateway runs only on the main loop')
        self.threads.append(threading.get_ident())
        self.calls.append((name, kwargs))
        if self.hang:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

    async def accept(self, **kwargs: Any) -> HttpAnswer:
        await self._enter('accept', kwargs)
        return self.accept_answer

    async def read(self, **kwargs: Any) -> HttpAnswer:
        await self._enter('read', kwargs)
        return self.read_answer


@pytest.fixture
def main_loop():
    main = _MainLoop()
    try:
        yield main
    finally:
        main.stop()


def _config(**overrides: Any) -> WebAppConfig:
    values: dict[str, Any] = {
        'enabled': True,
        'host': '127.0.0.1',
        'port': 0,
        'path': PATH,
        'clients': [
            WebClientConfig(name='tenant-a', token=TOKEN_A, rpm=1000),
            WebClientConfig(name='tenant-b', token=TOKEN_B, rpm=1000),
        ],
        'deliver_to_sinks': True,
        'request_timeout': '2s',
    }
    values.update(overrides)
    return WebAppConfig(**values)


def _webapp(main: _MainLoop | None, handler: Any = None, config: WebAppConfig | None = None, *, is_ready=True):
    """A job-mode ``WebApp`` over a stub app; returns the webapp and its fake gateway."""
    handler = handler if handler is not None else _ModelSplitHandler()
    app = MagicMock()
    app._handler = handler
    app._request_ids = RequestIdResolver(handler)
    app._recorder = None
    app.is_ready = is_ready
    app.main_loop = main.loop if main is not None else None
    app._split_http_enabled = True
    gateway = _FakeGateway(main) if main is not None else MagicMock()
    app._split.http.gateway = gateway
    return WebApp(app, config or _config()), gateway


def _auth(token: str = TOKEN_A) -> dict[str, str]:
    return {'Authorization': f'Bearer {token}'}


async def _post(webapp: WebApp, body: Any = None, *, content: Any = None, headers: dict | None = None):
    transport = httpx.ASGITransport(app=webapp._fastapi_app)
    async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
        kwargs: dict[str, Any] = {'headers': {**_auth(), **(headers or {})}}
        if content is not None:
            kwargs['content'] = content
        else:
            kwargs['json'] = {'value': 1} if body is None else body
        return await client.post(PATH, **kwargs)


async def _get(webapp: WebApp, params: dict | None = None, *, headers: dict | None = None, times: int = 1):
    transport = httpx.ASGITransport(app=webapp._fastapi_app)
    async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
        answers = []
        for _ in range(times):
            answers.append(await client.get(PATH, params=params, headers=headers if headers is not None else _auth()))
        return answers[-1] if times == 1 else answers


def _count(client: str, status: str) -> float:
    return metrics.webapp_requests.labels(client=client, status=status)._value.get()


# ---------------------------------------------------------------------------
# POST
# ---------------------------------------------------------------------------


async def test_post_answers_gateway_202_and_counts_accepted(main_loop):
    webapp, gateway = _webapp(main_loop)
    before = _count('tenant-a', 'accepted')

    response = await _post(webapp, {'value': 7})

    assert response.status_code == 202
    assert response.content == b'{"request_id":"r-1","status":"accepted"}'
    assert response.headers['content-type'] == 'application/json'
    assert _count('tenant-a', 'accepted') == before + 1
    [(name, kwargs)] = gateway.calls
    assert name == 'accept'
    assert kwargs['client'] == 'tenant-a'
    assert kwargs['body'] == b'{"value":7}'
    assert kwargs['deliver_sinks'] is True
    assert kwargs['timeout'] == 2.0
    assert kwargs['request_id']


async def test_post_passes_deliver_to_sinks_false(main_loop):
    webapp, gateway = _webapp(main_loop, config=_config(deliver_to_sinks=False))
    await _post(webapp)
    assert gateway.calls[0][1]['deliver_sinks'] is False


async def test_post_gates_in_order(main_loop):
    # 401: an unknown token.
    webapp, gateway = _webapp(main_loop)
    response = await _post(webapp, headers={'Authorization': 'Bearer nope'})
    assert (response.status_code, response.json()) == (401, {'error': 'unauthorized'})

    # 429: the rpm window of the client is full, ahead of every later gate.
    webapp, gateway = _webapp(
        main_loop, config=_config(clients=[WebClientConfig(name='tenant-a', token=TOKEN_A, rpm=1)])
    )
    webapp.shutdown_event.set()
    assert (await _post(webapp)).status_code == 503
    response = await _post(webapp)
    assert response.status_code == 429
    assert response.json()['error'] == 'rate_limited'

    # 503 shutdown before not_ready.
    webapp, gateway = _webapp(main_loop, is_ready=False)
    webapp.shutdown_event.set()
    body = (await _post(webapp)).json()
    assert body['status'] == 'shutdown'
    assert body['error'] == 'webapp is shutting down; request rejected'
    assert body['hint'] == webapp_server.KAFKA_FALLBACK_HINT

    # 503 not_ready before the body is read.
    webapp, gateway = _webapp(main_loop, is_ready=False)
    response = await _post(webapp, content=b'x' * 100, headers={'content-type': 'application/json'})
    assert response.status_code == 503
    assert response.json()['status'] == 'not_ready'
    assert response.json()['error'] == 'webapp is starting; main pipeline is not yet ready'

    # 413 before the parse.
    webapp, gateway = _webapp(main_loop, config=_config(max_body='10B'))
    response = await _post(webapp, content=b'not json at all', headers={'content-type': 'application/json'})
    assert response.status_code == 413
    assert response.json()['error'] == 'request_too_large'
    assert response.json()['details'] == 'request body exceeds sources.http.max_body (10 bytes)'

    # 503 capacity after the parse.
    webapp, gateway = _webapp(main_loop)
    webapp._semaphore = asyncio.Semaphore(0)
    response = await _post(webapp)
    assert response.status_code == 503
    body = response.json()
    assert body['status'] == 'capacity'
    assert body['max_concurrent'] == 64
    assert gateway.calls == []


async def test_post_slow_body_answers_408(main_loop, monkeypatch):
    monkeypatch.setattr(webapp_server, 'BODY_READ_TIMEOUT_MARGIN_SECONDS', 0.0)
    webapp, gateway = _webapp(main_loop, config=_config(request_timeout='50ms'))

    async def _dribble():
        yield b'{"value":'
        await asyncio.sleep(5)
        yield b'1}'

    response = await _post(webapp, content=_dribble(), headers={'content-type': 'application/json'})
    assert response.status_code == 408
    assert response.json()['error'] == 'request_timeout'
    assert response.json()['hint'] == 'the request body was not delivered in time'
    assert gateway.calls == []


async def test_post_not_ready_until_follower_restored(main_loop):
    webapp, gateway = _webapp(main_loop)
    gateway.is_ready = False
    response = await _post(webapp)
    assert response.status_code == 503
    assert response.json()['status'] == 'not_ready'
    assert gateway.calls == []

    gateway.is_ready = True
    assert (await _post(webapp)).status_code == 202


async def test_post_422_on_input_model_failure(main_loop):
    webapp, gateway = _webapp(main_loop)

    response = await _post(webapp, {'value': 'not a number'})
    assert response.status_code == 422
    body = response.json()
    assert body['error'] == 'invalid_request'
    assert body['request_id']
    assert body['details'][0]['loc'] == ['value']

    response = await _post(webapp, content=b'{broken', headers={'content-type': 'application/json'})
    assert response.status_code == 422
    assert response.json()['details'][0]['type'] == 'json_invalid'
    assert gateway.calls == []


async def test_post_without_input_model_takes_raw_body(main_loop):
    webapp, gateway = _webapp(main_loop, _RawSplitHandler())

    response = await _post(webapp, content=b'any bytes, not json', headers={'content-type': 'text/plain'})

    assert response.status_code == 202
    assert gateway.calls[0][1]['body'] == b'any bytes, not json'


async def test_post_request_id_from_hook_and_fallback(main_loop):
    webapp, gateway = _webapp(main_loop, _HeaderIdHandler())

    await _post(webapp, headers={'x-request-id': 'order-123'})
    await _post(webapp, headers={'x-request-id': 'bad'})

    assert gateway.calls[0][1]['request_id'] == 'order-123'
    fallback = gateway.calls[1][1]['request_id']
    assert fallback.startswith('req_')


async def test_post_answers_gateway_conflict_and_duplicate(main_loop):
    webapp, gateway = _webapp(main_loop)
    gateway.accept_answer = HttpAnswer(409, b'{"error":"request_id_in_use","request_id":"r-1"}', 'conflict')
    before = _count('tenant-a', 'conflict')

    response = await _post(webapp)

    assert response.status_code == 409
    assert response.json() == {'error': 'request_id_in_use', 'request_id': 'r-1'}
    assert _count('tenant-a', 'conflict') == before + 1


async def test_post_dispatch_timeout_answers_503_unavailable(main_loop, monkeypatch):
    monkeypatch.setattr(webapp_server, 'ACCEPTED_WRITE_SECONDS', 0.0)
    monkeypatch.setattr(webapp_server, 'DISPATCH_MARGIN_SECONDS', 0.0)
    webapp, gateway = _webapp(main_loop, _HeaderIdHandler(), _config(request_timeout='100ms'))
    gateway.hang = True
    before = _count('tenant-a', 'unavailable')

    response = await _post(webapp, headers={'x-request-id': 'order-9'})

    assert response.status_code == 503
    assert response.json() == {
        'status': 'unavailable',
        'error': 'the request could not be stored',
        'request_id': 'order-9',
    }
    assert _count('tenant-a', 'unavailable') == before + 1
    # The slot is given back.
    assert webapp._semaphore._value == 64
    # The call that hung on the main loop was cancelled.
    assert gateway.cancelled.wait(timeout=2.0)


async def test_post_gateway_failure_answers_503_unavailable():
    """With no main loop to reach the gateway raises; the answer is a clean 503, not a 500."""
    webapp, _ = _webapp(None, _HeaderIdHandler())

    async def _refuse(**kwargs):
        raise RuntimeError('the HTTP job gateway runs only on the main loop')

    webapp._jobs.accept = _refuse
    webapp._jobs.is_ready = True

    response = await _post(webapp, headers={'x-request-id': 'order-10'})

    assert response.status_code == 503
    assert response.json() == {
        'status': 'unavailable',
        'error': 'the request could not be stored',
        'request_id': 'order-10',
    }


# ---------------------------------------------------------------------------
# GET
# ---------------------------------------------------------------------------


async def test_get_requires_auth(main_loop):
    webapp, gateway = _webapp(main_loop)

    response = await _get(webapp, {'request_id': 'r-1'}, headers={'Authorization': 'Bearer nope'})

    assert (response.status_code, response.json()) == (401, {'error': 'unauthorized'})
    assert gateway.calls == []


async def test_get_is_not_rate_limited(main_loop):
    config = _config(clients=[WebClientConfig(name='tenant-a', token=TOKEN_A, rpm=1)])
    webapp, gateway = _webapp(main_loop, config=config)

    answers = await _get(webapp, {'request_id': 'r-1'}, times=5)

    assert [answer.status_code for answer in answers] == [200] * 5
    assert len(gateway.calls) == 5


async def test_get_takes_no_concurrency_slot(main_loop):
    webapp, _ = _webapp(main_loop)
    webapp._semaphore = asyncio.Semaphore(0)

    response = await _get(webapp, {'request_id': 'r-1'})

    assert response.status_code == 200


@pytest.mark.parametrize('params', [None, {'request_id': ''}])
async def test_get_missing_request_id_is_422(main_loop, params):
    webapp, gateway = _webapp(main_loop)

    response = await _get(webapp, params)

    assert response.status_code == 422
    assert response.json() == {'error': 'invalid_request', 'details': 'request_id is required'}
    assert gateway.calls == []


async def test_get_answers_gateway_body(main_loop):
    webapp, gateway = _webapp(main_loop)
    gateway.read_answer = HttpAnswer(404, b'{"error":"not_found","request_id":"r-2"}', 'not_found')
    before = _count('tenant-b', 'not_found')

    response = await _get(webapp, {'request_id': 'r-2'}, headers=_auth(TOKEN_B))

    assert response.status_code == 404
    assert response.content == b'{"error":"not_found","request_id":"r-2"}'
    assert response.headers['content-type'] == 'application/json'
    assert gateway.calls == [('read', {'request_id': 'r-2', 'client': 'tenant-b'})]
    assert _count('tenant-b', 'not_found') == before + 1

    gateway.read_answer = HttpAnswer(200, b'{"request_id":"r-2","status":"done","result":null}', 'status_read')
    response = await _get(webapp, {'request_id': 'r-2'}, headers=_auth(TOKEN_B))
    assert json.loads(response.content)['status'] == 'done'


async def test_get_shutdown_and_not_ready_503(main_loop):
    webapp, gateway = _webapp(main_loop)
    webapp.shutdown_event.set()
    response = await _get(webapp, {'request_id': 'r-1'})
    assert response.status_code == 503
    assert response.json()['status'] == 'shutdown'

    webapp, gateway = _webapp(main_loop, is_ready=False)
    assert (await _get(webapp, {'request_id': 'r-1'})).json()['status'] == 'not_ready'

    webapp, gateway = _webapp(main_loop)
    gateway.is_ready = False
    assert (await _get(webapp, {'request_id': 'r-1'})).json()['status'] == 'not_ready'
    assert gateway.calls == []


async def test_get_dispatch_timeout_answers_503_unavailable(main_loop, monkeypatch):
    monkeypatch.setattr(webapp_server, 'STATUS_WAIT_SECONDS', 0.0)
    monkeypatch.setattr(webapp_server, 'DISPATCH_MARGIN_SECONDS', 0.0)
    webapp, gateway = _webapp(main_loop, config=_config(request_timeout='100ms'))
    gateway.hang = True

    response = await _get(webapp, {'request_id': 'r-3'})

    assert response.status_code == 503
    assert response.json() == {'status': 'unavailable', 'error': 'the status could not be read', 'request_id': 'r-3'}
    assert gateway.cancelled.wait(timeout=2.0)


async def test_failed_dispatches_of_one_route_log_one_line_per_minute(main_loop):
    webapp, _ = _webapp(main_loop)
    now = [100.0]
    webapp._dispatch_log = RateLimitedLog(lambda: now[0])

    async def _fail():
        raise RuntimeError('refused')

    async def _dispatch(route: str) -> None:
        answer = await webapp._dispatch_job_call(_fail(), budget=1.0, request_id='r-1', client='tenant-a', route=route)
        assert answer is None

    with capture_logs() as cap:
        for _ in range(3):
            await _dispatch('status')
        await _dispatch('submit')
        now[0] += 61.0
        await _dispatch('status')

    lines = [(entry['route'], entry['suppressed']) for entry in cap if entry['event'] == 'split_http_dispatch_failed']
    assert lines == [('status', 0), ('submit', 0), ('status', 2)]


# ---------------------------------------------------------------------------
# The loop the gateway runs on, and the handler shape job mode needs
# ---------------------------------------------------------------------------


async def test_gateway_calls_run_on_the_main_loop(main_loop):
    webapp, gateway = _webapp(main_loop)

    await _post(webapp)
    await _get(webapp, {'request_id': 'r-1'})

    assert gateway.threads == [main_loop.thread_id, main_loop.thread_id]
    assert main_loop.thread_id != threading.get_ident()


def test_job_mode_needs_no_http_hooks_or_models(main_loop):
    webapp, _ = _webapp(main_loop, _ModelSplitHandler())

    assert webapp._runner is None
    assert webapp._http_request_type is _Body


def test_sync_mode_is_kept_when_split_http_is_off():
    """A stub app whose flag is not exactly True keeps the synchronous mode and its checks."""
    app = MagicMock()
    app._handler = _ModelSplitHandler()
    app._split_http_enabled = False
    with pytest.raises(Exception, match='HttpRequestT'):
        WebApp(app, _config())


async def test_post_with_a_closed_main_loop_answers_503_and_closes_the_call(main_loop):
    """A main loop that is already closed never starts the call; it is closed, not left unawaited."""
    webapp, gateway = _webapp(main_loop, _HeaderIdHandler())
    closed = asyncio.new_event_loop()
    closed.close()
    webapp._app.main_loop = closed

    response = await _post(webapp, headers={'x-request-id': 'order-11'})

    assert response.status_code == 503
    assert response.json()['request_id'] == 'order-11'
    assert gateway.calls == []


def test_job_mode_without_its_runtime_fails_at_construction():
    app = MagicMock()
    app._handler = _ModelSplitHandler()
    app._split_http_enabled = True
    app._split = None
    with pytest.raises(RuntimeError, match='runtime of HTTP split jobs was not started'):
        WebApp(app, _config())
