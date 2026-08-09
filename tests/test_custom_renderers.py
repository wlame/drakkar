"""Tests for the custom cell renderers config and serving route.

Fully isolated: no real database, no network, no docker. The DrakkarApp
boot tests use a real (in-memory) config, per ``tests/test_uipages.py``'s
``app_factory`` pattern; the route tests use a MagicMock app stand-in and a
real ASGI request, per ``tests/test_config_reference_api.py``.
"""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
    UIConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

RENDERERS_JS = 'export default { orderCard(value, row, cell) { return document.createElement("b") } }\n'


@pytest.fixture
def renderers_file(tmp_path):
    path = tmp_path / 'custom-renderers.js'
    path.write_text(RENDERERS_JS)
    return path


def _minimal_config(ui: UIConfig) -> DrakkarConfig:
    """Smallest DrakkarConfig that satisfies DrakkarApp.__init__."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=ui,
    )


@pytest.fixture
def app_factory():
    """Build a DrakkarApp from flat ``UIConfig`` overrides.

    Construction is synchronous and never touches Kafka, so no mocking is
    needed to exercise the __init__-time validation.
    """

    def _build(*, ui_overrides: dict | None = None) -> DrakkarApp:
        class _Handler(BaseDrakkarHandler):
            async def arrange(self, messages, pending):
                return []

        ui = make_ui_config(**(ui_overrides or {}))
        return DrakkarApp(handler=_Handler(), config=_minimal_config(ui))

    return _build


def _mock_app(config: DrakkarConfig) -> MagicMock:
    """Minimal MagicMock DrakkarApp stand-in — mirrors ``mock_app`` in test_debug_api_v1.py."""
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 60
    app.processors = {}
    app._config = config
    app._config.ui.release.enabled = False
    app.cache_engine = None
    app.handler = None
    app._consumer = None
    return app


def _make_client(ui_config: UIConfig) -> AsyncClient:
    recorder = AsyncMock(spec=EventRecorder)
    recorder._db = None
    recorder._reader_db = None
    recorder.reader_db = None
    recorder.config = ui_config
    app = _mock_app(DrakkarConfig(ui=ui_config))
    fastapi_app = create_ui_app(ui_config, recorder, app)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


@pytest.fixture
async def client():
    async with _make_client(make_ui_config()) as c:
        yield c


@pytest.fixture
async def client_with_renderers(renderers_file):
    async with _make_client(make_ui_config(custom_renderers_path=str(renderers_file))) as c:
        yield c


def test_app_boot_fails_when_renderers_path_missing(app_factory, tmp_path):
    with pytest.raises(ValueError, match='custom_renderers_path'):
        app_factory(ui_overrides={'custom_renderers_path': str(tmp_path / 'nope.js')})


async def test_app_boot_succeeds_when_renderers_path_exists(app_factory, renderers_file):
    app_factory(ui_overrides={'custom_renderers_path': str(renderers_file)})  # must not raise


async def test_renderers_route_serves_content_with_etag(client_with_renderers):
    res = await client_with_renderers.get('/api/v1/ui/renderers.js')
    assert res.status_code == 200
    assert res.headers['content-type'].startswith('text/javascript')
    assert res.headers['etag'].startswith('"')
    assert res.headers['cache-control'] == 'no-cache'
    assert 'orderCard' in res.text


async def test_renderers_route_returns_304_on_matching_etag(client_with_renderers):
    first = await client_with_renderers.get('/api/v1/ui/renderers.js')
    res = await client_with_renderers.get('/api/v1/ui/renderers.js', headers={'If-None-Match': first.headers['etag']})
    assert res.status_code == 304
    assert res.headers['etag'] == first.headers['etag']
    assert res.text == ''


async def test_renderers_route_404s_with_reason_when_unconfigured(client):
    res = await client.get('/api/v1/ui/renderers.js')
    assert res.status_code == 404
    assert res.json()['enabled'] is False
