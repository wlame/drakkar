"""Tests for the runtime-health endpoints (drakkar/uiserver/routes_runtime.py)."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, RuntimeHealthConfig
from drakkar.runtimehealth import RuntimeHealthMonitor
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config


@pytest.fixture
def mock_app():
    # Mocked because the routes only need the app as a handle bag; the
    # real DrakkarApp drags in Kafka/executor construction.
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 120
    app._config = DrakkarConfig()
    app._config.ui.release.enabled = False  # hermetic: no bundle fetch
    app._runtime_health = None
    return app


@pytest.fixture
async def client(mock_app):
    fastapi_app = create_ui_app(make_ui_config(enabled=True, port=8080, db_dir='/tmp'), MagicMock(), mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


async def test_health_endpoint_404_when_monitor_disabled(client):
    resp = await client.get('/api/v1/runtime/health')
    assert resp.status_code == 404
    assert resp.json()['enabled'] is False


async def test_health_endpoint_returns_snapshot(client, mock_app):
    monitor = RuntimeHealthMonitor(RuntimeHealthConfig(), recorder=None)
    monitor._heartbeat = time.monotonic()
    monitor._window.add(time.time(), 0.005)
    mock_app._runtime_health = monitor

    resp = await client.get('/api/v1/runtime/health')
    assert resp.status_code == 200
    body = resp.json()
    assert body['enabled'] is True
    assert body['state'] == 'healthy'
    assert body['unit_label'] == 'tasks'
    assert len(body['window']) == 1
    assert body['recent_stalls'] == []


async def test_health_endpoint_reports_stalled_without_touching_the_loop(client, mock_app):
    monitor = RuntimeHealthMonitor(RuntimeHealthConfig(), recorder=None)
    monitor._heartbeat = time.monotonic() - 60  # heartbeat long silent
    mock_app._runtime_health = monitor

    resp = await client.get('/api/v1/runtime/health')
    assert resp.json()['state'] == 'stalled'


async def test_units_endpoint_returns_census(client):
    # mock_app.main_loop is a MagicMock, so dispatch_to_loop falls back to
    # awaiting inline on the test loop — the census sees this test's tasks.
    resp = await client.get('/api/debug/runtime/units')
    assert resp.status_code == 200
    body = resp.json()
    assert body['unit_label'] == 'tasks'
    assert body['total'] >= 1
    assert all({'name', 'location', 'count', 'example'} <= set(row) for row in body['units'])


async def test_units_endpoint_v1_alias(client):
    resp = await client.get('/api/v1/debug/runtime/units')
    assert resp.status_code == 200
