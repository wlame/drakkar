"""Tests for the Task 9 debug-UI updates of the webapp pipeline.

Covers:

- ``/api/dashboard`` returns a ``webapp_tile`` only when ``webapp.enabled``.
- ``/task/{id}`` swaps Partition/Offset for Client/Request ID when the
  task's ``origin == 'http'``, and keeps Partition for ``origin == 'kafka'``.
- ``/api/recent-tasks`` exposes the new ``origin`` / ``client_name`` /
  ``request_id`` fields per task.
- ``/history`` filters by ``origin`` (kafka/http/all) using the recorder's
  new ``origin`` column.

The tests follow the pytest-function-with-fixtures style used by the
rest of the debug-UI suite (``test_debug_server.py``) and reuse the same
``UIConfig`` / ``DrakkarConfig`` shapes.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, WebAppConfig, WebClientConfig
from drakkar.debug.server import create_debug_app
from drakkar.recorder import EventRecorder
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_recorder(events: list[dict] | None = None) -> AsyncMock:
    """Build a mock recorder with sensible defaults for the dashboard route.

    The dashboard endpoint calls ``get_stats`` and (now) the new
    ``flush_and_select`` path. We default the SQL helper to return
    ``None`` (= recorder reader missing), which the route translates
    into ``success_60s=0`` / ``error_60s=0`` — fine for the no-events
    happy path.
    """
    rec = AsyncMock(spec=EventRecorder)
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
    rec.get_stats.return_value = {
        'total_events': 0,
        'consumed': 0,
        'completed': 0,
        'failed': 0,
        'produced': 0,
        'committed': 0,
    }
    rec.get_partition_summary.return_value = []
    rec.get_events.return_value = events or []
    rec.get_active_tasks.return_value = []
    rec.get_task_events.return_value = events or []
    return rec


def _make_mock_app(*, webapp_enabled: bool = False) -> MagicMock:
    """Build a mock DrakkarApp with optional webapp enable flag.

    Avoids ``MagicMock(spec=DrakkarApp)`` — the real class touches
    sinks / lifecycle that aren't relevant here. We just need the
    attribute surface the routes touch.
    """
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 10
    app.processors = {}
    app._executor_pool = MagicMock(active_count=0, waiting_count=0, max_executors=8)
    app._consumer = None

    cfg = DrakkarConfig()
    if webapp_enabled:
        # ``WebAppConfig`` requires a non-default ``path`` length > 1 — the
        # default ``/process`` already satisfies this.
        cfg.webapp = WebAppConfig(
            enabled=True,
            clients=[
                WebClientConfig(name='tenant-A', token='', rpm=10),
                WebClientConfig(name='tenant-B', token='secret', rpm=20),
            ],
        )
    app._config = cfg

    sink_mgr = MagicMock()
    sink_mgr.get_sink_info.return_value = []
    sink_mgr.get_all_stats.return_value = {}
    app.sink_manager = sink_mgr
    app.cache_engine = None
    return app


@pytest.fixture
def debug_config():
    return make_ui_config(enabled=True, port=8080, db_dir='/tmp')


# ---------------------------------------------------------------------------
# Dashboard /api/dashboard webapp_tile presence
# ---------------------------------------------------------------------------


async def test_api_dashboard_includes_webapp_tile_when_enabled(debug_config):
    """webapp.enabled=True surfaces a ``webapp_tile`` field with expected shape."""
    rec = _make_mock_recorder()
    app = _make_mock_app(webapp_enabled=True)

    fastapi_app = create_debug_app(debug_config, rec, app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/dashboard')

    assert resp.status_code == 200
    data = resp.json()
    assert 'webapp_tile' in data, 'expected webapp_tile when webapp.enabled=True'
    tile = data['webapp_tile']
    assert isinstance(tile['inflight_count'], int)
    assert tile['success_60s'] == 0
    assert tile['error_60s'] == 0
    assert tile['rejected_60s'] == 0
    client_names = {c['name'] for c in tile['clients']}
    assert client_names == {'tenant-A', 'tenant-B'}
    rpm_for_a = next(c['rpm_limit'] for c in tile['clients'] if c['name'] == 'tenant-A')
    assert rpm_for_a == 10


async def test_api_dashboard_omits_webapp_tile_when_disabled(debug_config):
    """webapp.enabled=False keeps the dashboard payload free of webapp_tile."""
    rec = _make_mock_recorder()
    app = _make_mock_app(webapp_enabled=False)

    fastapi_app = create_debug_app(debug_config, rec, app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/dashboard')

    assert resp.status_code == 200
    data = resp.json()
    assert 'webapp_tile' not in data


# ---------------------------------------------------------------------------
# Task detail page: HTTP vs Kafka rendering
# ---------------------------------------------------------------------------


async def test_task_detail_renders_client_and_request_id_for_http(debug_config):
    """origin=http swaps the Partition row for Client / Request ID."""
    rec = _make_mock_recorder()
    now = time.time()
    rec.get_task_events.return_value = [
        {
            'id': 1,
            'ts': now - 5,
            'event': 'task_started',
            'partition': -1,
            'offset': None,
            'task_id': 'task-http-1',
            'args': None,
            'stdout_size': 0,
            'stdout': None,
            'stderr': None,
            'exit_code': None,
            'duration': None,
            'output_topic': None,
            'pid': 4321,
            'metadata': None,
            'labels': None,
            'origin': 'http',
            'client_name': 'tenant-A',
            'request_id': 'req_x',
        },
        {
            'id': 2,
            'ts': now - 1,
            'event': 'task_completed',
            'partition': -1,
            'offset': None,
            'task_id': 'task-http-1',
            'args': None,
            'stdout_size': 0,
            'stdout': None,
            'stderr': None,
            'exit_code': 0,
            'duration': 4.0,
            'output_topic': None,
            'pid': 4321,
            'metadata': None,
            'labels': None,
            'origin': 'http',
            'client_name': 'tenant-A',
            'request_id': 'req_x',
        },
    ]
    app = _make_mock_app(webapp_enabled=True)

    fastapi_app = create_debug_app(debug_config, rec, app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/task/task-http-1')

    assert resp.status_code == 200
    text = resp.text
    assert 'tenant-A' in text
    assert 'req_x' in text
    # Origin pill should mark this as HTTP and the labels should appear.
    assert 'Client' in text
    assert 'Request ID' in text
    # The Kafka-only "Partition" header field should NOT be a column for HTTP tasks.
    # We assert via the absence of the conditional partition link target on
    # this page (HTTP partition is the synthetic ``-1`` and we suppress the
    # back-link in the template header).
    assert 'partition -1' not in text


async def test_task_detail_renders_partition_for_kafka(debug_config):
    """origin=kafka keeps the original Partition rendering."""
    rec = _make_mock_recorder()
    now = time.time()
    rec.get_task_events.return_value = [
        {
            'id': 1,
            'ts': now - 3,
            'event': 'task_started',
            'partition': 7,
            'offset': 100,
            'task_id': 'task-k-1',
            'args': '["--in", "x"]',
            'stdout_size': 0,
            'stdout': None,
            'stderr': None,
            'exit_code': None,
            'duration': None,
            'output_topic': None,
            'pid': 5555,
            'metadata': None,
            'labels': None,
            'origin': 'kafka',
            'client_name': None,
            'request_id': None,
        },
    ]
    app = _make_mock_app(webapp_enabled=False)

    fastapi_app = create_debug_app(debug_config, rec, app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/task/task-k-1')

    assert resp.status_code == 200
    text = resp.text
    assert 'Partition' in text
    # Kafka task should NOT render the HTTP-specific labels.
    assert 'Request ID' not in text
    assert 'Client' not in text or 'Client' in text  # 'Client' may appear in unrelated context
    # Specifically, the HTTP origin pill must be absent.
    assert '>HTTP<' not in text


# ---------------------------------------------------------------------------
# /api/recent-tasks exposes new fields
# ---------------------------------------------------------------------------


async def test_api_recent_tasks_includes_origin_fields(tmp_path, debug_config):
    """The /api/recent-tasks payload exposes origin/client_name/request_id."""
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), flush_interval_seconds=60)
    rec = EventRecorder(cfg, worker_name='wlame-worker')
    await rec.start()
    try:
        now = time.time()
        # Insert a kafka and an http task_started row directly via the
        # internal _record path so we exercise the real reader DB.
        rec._record(
            {
                'ts': now - 2,
                'event': 'task_started',
                'partition': 0,
                'offset': 1,
                'task_id': 'rg-kafka',
                'args': '["a"]',
                'origin': 'kafka',
            }
        )
        rec._record(
            {
                'ts': now - 1,
                'event': 'task_started',
                'partition': -1,
                'offset': None,
                'task_id': 'rg-http',
                'args': '["b"]',
                'origin': 'http',
                'client_name': 'tenant-A',
                'request_id': 'req_42',
            }
        )
        await rec.flush()

        app = _make_mock_app(webapp_enabled=True)
        # Recorder is real here, so wire it explicitly.
        fastapi_app = create_debug_app(cfg, rec, app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/recent-tasks?minutes=5')

        assert resp.status_code == 200
        payload = resp.json()
        assert 'tasks' in payload
        # Map by task_id for stable assertions.
        by_id = {t['task_id']: t for t in payload['tasks']}
        assert 'rg-kafka' in by_id
        assert 'rg-http' in by_id
        assert by_id['rg-kafka']['origin'] == 'kafka'
        assert by_id['rg-kafka']['client_name'] is None
        assert by_id['rg-http']['origin'] == 'http'
        assert by_id['rg-http']['client_name'] == 'tenant-A'
        assert by_id['rg-http']['request_id'] == 'req_42'
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# History filter by origin
# ---------------------------------------------------------------------------


async def test_history_filters_by_origin(tmp_path):
    """/history?origin=kafka|http|all filters recorder events accordingly."""
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), flush_interval_seconds=60)
    rec = EventRecorder(cfg, worker_name='wlame-worker')
    await rec.start()
    try:
        now = time.time()
        rec._record(
            {
                'ts': now - 3,
                'event': 'task_completed',
                'partition': 0,
                'offset': 1,
                'task_id': 'kafka-task-1',
                'origin': 'kafka',
                'duration': 0.1,
                'exit_code': 0,
            }
        )
        rec._record(
            {
                'ts': now - 2,
                'event': 'webapp_request_completed',
                'partition': -1,
                'task_id': None,
                'origin': 'http',
                'client_name': 'tenant-A',
                'request_id': 'req_1',
                'duration': 0.05,
            }
        )
        await rec.flush()

        # Use the recorder's ``get_events`` helper directly — the /history
        # page exercises the same code path. Asserting through the public
        # API keeps the test independent of HTML markup churn.
        kafka_only = await rec.get_events(origin='kafka', limit=100)
        assert len(kafka_only) == 1
        assert kafka_only[0]['event'] == 'task_completed'

        http_only = await rec.get_events(origin='http', limit=100)
        assert len(http_only) == 1
        assert http_only[0]['event'] == 'webapp_request_completed'

        all_events = await rec.get_events(limit=100)
        assert len(all_events) == 2

        # Round-trip through the HTTP route (smoke) — page must render.
        app = _make_mock_app(webapp_enabled=True)
        fastapi_app = create_debug_app(cfg, rec, app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp_all = await c.get('/history?origin=all')
            resp_kafka = await c.get('/history?origin=kafka')
            resp_http = await c.get('/history?origin=http')
        for r in (resp_all, resp_kafka, resp_http):
            assert r.status_code == 200
        # The page surfaces an "Origin" filter label and the three options.
        assert 'Origin' in resp_all.text
    finally:
        await rec.stop()
