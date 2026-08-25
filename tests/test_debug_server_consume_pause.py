"""Tests for the consume-pause API routes (/api/debug/consume-pause*).

The controller mechanics are covered by test_consume_pause.py; here the
HTTP layer is exercised: the always-200 state endpoint (with ``enabled``
as the UI's hide signal), the opt-in 403 gate on the mutating routes, the
422/503 error mapping, resume idempotency, and the auth gate.

Isolation: ``AsyncMock`` recorder, ``MagicMock`` DrakkarApp carrying a
real ``DrakkarConfig`` and a REAL ``ConsumePauseController`` bound to it —
no Kafka, no sockets.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, UIConsumePauseConfig
from drakkar.consume_pause import ConsumePauseController
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config


def _mock_recorder() -> AsyncMock:
    rec = AsyncMock(spec=EventRecorder)
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
    return rec


def _mock_app(*, enabled: bool = True, consumer: AsyncMock | None = None) -> MagicMock:
    cfg = DrakkarConfig()
    cfg.ui.release.enabled = False
    cfg.ui.consume_pause = UIConsumePauseConfig(enabled=enabled, durations_seconds=[15, 60, 300])

    app = MagicMock()
    app._worker_id = 'test-worker'
    app._config = cfg
    app.config = cfg
    # The real controller against the mock app: it reads these attributes.
    app._consumer = consumer if consumer is not None else AsyncMock()
    app._processors = {0: object(), 1: object()}
    app._stalled_partitions = set()
    app._paused = False
    app._background_tasks = set()
    app.consume_pause = ConsumePauseController(app)
    app.cache_engine = None
    app._cache_engine = None
    return app


def _client(app: MagicMock, ui_config=None) -> AsyncClient:
    ui_config = ui_config or make_ui_config(enabled=True)
    recorder = _mock_recorder()
    recorder.config = ui_config
    fastapi_app = create_ui_app(ui_config, recorder, app)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


# --- GET state -----------------------------------------------------------------


async def test_state_carries_enabled_flag_and_presets_when_disabled():
    """The state endpoint answers 200 even when the feature is off —
    enabled:false in the body is the UI's hide signal."""
    async with _client(_mock_app(enabled=False)) as client:
        resp = await client.get('/api/v1/debug/consume-pause')
    assert resp.status_code == 200
    body = resp.json()
    assert body['enabled'] is False
    assert body['active'] is False
    assert body['resume_at_ms'] is None
    assert body['durations_seconds'] == [15, 60, 300]


async def test_state_served_under_v1_alias():
    async with _client(_mock_app()) as client:
        resp = await client.get('/api/v1/debug/consume-pause')
    assert resp.status_code == 200


# --- opt-in gate ---------------------------------------------------------------


async def test_mutating_routes_403_when_disabled():
    async with _client(_mock_app(enabled=False)) as client:
        pause = await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 15})
        resume = await client.post('/api/v1/debug/consume-resume')
    assert pause.status_code == 403
    assert 'ui.consume_pause.enabled' in pause.json()['error']
    assert resume.status_code == 403


async def test_routes_require_token_when_configured():
    async with _client(_mock_app(), make_ui_config(enabled=True, auth_token='secret-t')) as client:
        anonymous = await client.get('/api/v1/debug/consume-pause')
        authed = await client.get('/api/v1/debug/consume-pause', headers={'Authorization': 'Bearer secret-t'})
    assert anonymous.status_code == 401
    assert authed.status_code == 200


# --- pause / resume ------------------------------------------------------------


async def test_pause_pauses_consumer_and_returns_active_state():
    app = _mock_app()
    async with _client(app) as client:
        resp = await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 60})
    assert resp.status_code == 200
    body = resp.json()
    assert body['active'] is True
    assert body['requested_seconds'] == 60
    assert body['resume_at_ms'] is not None
    app._consumer.pause.assert_awaited_once_with([0, 1])
    await app.consume_pause.resume()  # cancel the 60s timer


async def test_pause_duration_bounds_are_422():
    app = _mock_app()
    async with _client(app) as client:
        low = await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 0})
        high = await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 3601})
    assert low.status_code == 422
    assert high.status_code == 422
    app._consumer.pause.assert_not_awaited()


async def test_pause_without_running_consumer_is_503():
    app = _mock_app()
    app._consumer = None
    async with _client(app) as client:
        resp = await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 15})
    assert resp.status_code == 503
    assert 'Consumer' in resp.json()['detail']


async def test_resume_ends_the_pause_and_is_idempotent():
    app = _mock_app()
    async with _client(app) as client:
        await client.post('/api/v1/debug/consume-pause', json={'duration_seconds': 3600})
        first = await client.post('/api/v1/debug/consume-resume')
        second = await client.post('/api/v1/debug/consume-resume')
    assert first.status_code == 200
    assert first.json()['active'] is False
    assert second.status_code == 200
    app._consumer.resume.assert_awaited_once_with([0, 1])
