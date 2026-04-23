"""Tests for Drakkar debug web UI."""

import asyncio
import json
import os
import re
import time
import typing
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel
from starlette.testclient import TestClient

from drakkar.config import DebugConfig, DrakkarConfig
from drakkar.debug_server import (
    _format_ts,
    _format_ts_full,
    _format_ts_ms,
    _format_uptime,
    _worker_group,
    create_debug_app,
)
from drakkar.recorder import EventRecorder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_recorder():
    rec = AsyncMock(spec=EventRecorder)
    rec.get_stats.return_value = {
        'total_events': 42,
        'consumed': 20,
        'completed': 15,
        'failed': 2,
        'produced': 15,
        'committed': 10,
        'oldest_event': time.time() - 3600,
        'newest_event': time.time(),
    }
    rec.get_partition_summary.return_value = [
        {
            'partition': 0,
            'last_consumed': time.time() - 10,
            'last_committed': time.time() - 15,
            'last_committed_offset': 100,
            'consumed_count': 50,
            'completed_count': 45,
            'failed_count': 1,
        },
        {
            'partition': 1,
            'last_consumed': time.time() - 5,
            'last_committed': time.time() - 8,
            'last_committed_offset': 200,
            'consumed_count': 30,
            'completed_count': 30,
            'failed_count': 0,
        },
    ]
    rec.get_events.return_value = [
        {
            'id': 1,
            'ts': time.time() - 60,
            'event': 'consumed',
            'partition': 0,
            'offset': 42,
            'task_id': None,
            'args': None,
            'stdout_size': 0,
            'stdout': None,
            'stderr': None,
            'exit_code': None,
            'duration': None,
            'output_topic': None,
            'metadata': None,
        },
        {
            'id': 2,
            'ts': time.time() - 55,
            'event': 'task_completed',
            'partition': 0,
            'offset': None,
            'task_id': 't-42',
            'args': '["--input", "f.txt"]',
            'stdout_size': 1024,
            'stdout': None,
            'stderr': None,
            'exit_code': 0,
            'duration': 1.5,
            'output_topic': None,
            'metadata': None,
        },
    ]
    rec.get_trace.return_value = rec.get_events.return_value
    rec.cross_trace.return_value = [{**e, 'worker_name': 'test-worker'} for e in rec.get_events.return_value]
    rec.get_active_tasks.return_value = []
    return rec


@pytest.fixture
def mock_app():
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 120
    app.processors = {}
    app._config = DrakkarConfig()

    pool = MagicMock()
    pool.active_count = 2
    pool.waiting_count = 0
    pool.max_executors = 8
    app._executor_pool = pool

    app._consumer = None

    sink_mgr = MagicMock()
    sink_mgr.get_sink_info.return_value = [
        {'sink_type': 'kafka', 'name': 'results'},
        {'sink_type': 'postgres', 'name': 'main-db'},
    ]
    from drakkar.sinks.manager import SinkStats

    sink_mgr.get_all_stats.return_value = {
        ('kafka', 'results'): SinkStats(
            delivered_count=100,
            delivered_payloads=250,
            error_count=2,
            retry_count=1,
            last_delivery_ts=time.time() - 5,
            last_delivery_duration=0.012,
        ),
        ('postgres', 'main-db'): SinkStats(
            delivered_count=80,
            delivered_payloads=80,
            error_count=0,
            retry_count=0,
            last_delivery_ts=time.time() - 10,
            last_delivery_duration=0.045,
            last_error=None,
        ),
    }
    app.sink_manager = sink_mgr
    return app


@pytest.fixture
def debug_config():
    return DebugConfig(enabled=True, port=8080, db_dir='/tmp')


@pytest.fixture
async def client(debug_config, mock_recorder, mock_app):
    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


# ---------------------------------------------------------------------------
# 1. Pure utility functions: _format_ts
# ---------------------------------------------------------------------------


class TestFormatTs:
    def test_format_ts_none_returns_empty(self):
        assert _format_ts(None) == ''

    def test_format_ts_returns_hms(self):
        result = _format_ts(1000.0)
        assert result != ''
        assert re.match(r'\d{2}:\d{2}:\d{2}$', result)

    def test_format_ts_ms_none_returns_empty(self):
        assert _format_ts_ms(None) == ''

    def test_format_ts_ms_returns_hms_millis(self):
        result = _format_ts_ms(1000.0)
        assert result != ''
        assert re.match(r'\d{2}:\d{2}:\d{2}\.\d{3}$', result)

    def test_format_ts_full_none_returns_empty(self):
        assert _format_ts_full(None) == ''

    def test_format_ts_full_returns_datetime_millis(self):
        result = _format_ts_full(1000.0)
        assert result != ''
        assert re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$', result)


class TestFormatUptime:
    def test_seconds(self):
        assert _format_uptime(0) == '0s'
        assert _format_uptime(45) == '45s'
        assert _format_uptime(59) == '59s'

    def test_minutes(self):
        assert _format_uptime(60) == '1m 0s'
        assert _format_uptime(90) == '1m 30s'
        assert _format_uptime(3599) == '59m 59s'

    def test_hours(self):
        assert _format_uptime(3600) == '1h 0m'
        assert _format_uptime(7200 + 1800) == '2h 30m'
        assert _format_uptime(86399) == '23h 59m'

    def test_days(self):
        assert _format_uptime(86400) == '1d 0h'
        assert _format_uptime(86400 * 3 + 3600 * 5) == '3d 5h'
        assert _format_uptime(86400 * 29 + 3600 * 23) == '29d 23h'

    def test_months(self):
        assert _format_uptime(86400 * 30) == '1mo 0d'
        assert _format_uptime(86400 * 75) == '2mo 15d'
        assert _format_uptime(86400 * 364) == '12mo 4d'

    def test_years(self):
        assert _format_uptime(86400 * 365) == '1y 0mo'
        assert _format_uptime(86400 * 365 * 2 + 86400 * 90) == '2y 3mo'

    def test_fractional_seconds_truncated(self):
        assert _format_uptime(45.7) == '45s'
        assert _format_uptime(90.999) == '1m 30s'


# ---------------------------------------------------------------------------
# 1b. Pure utility functions: _worker_group
# ---------------------------------------------------------------------------


class TestWorkerGroup:
    def test_strips_trailing_number_with_dash(self):
        assert _worker_group('worker-1') == 'worker'

    def test_compound_name_with_trailing_number(self):
        assert _worker_group('worker-vip-2') == 'worker-vip'

    def test_multi_digit_trailing_number(self):
        assert _worker_group('slow-worker-05') == 'slow-worker'

    def test_number_without_separator(self):
        assert _worker_group('worker15') == 'worker'

    def test_no_trailing_number(self):
        assert _worker_group('single') == 'single'

    def test_strips_trailing_number_multiple(self):
        assert _worker_group('worker-3') == 'worker'
        assert _worker_group('worker-15') == 'worker'

    def test_preserves_middle_numbers(self):
        assert _worker_group('worker-vip-1') == 'worker-vip'
        assert _worker_group('worker-vip-2') == 'worker-vip'

    def test_underscore_separator(self):
        assert _worker_group('slow_worker_05') == 'slow_worker'

    def test_no_trailing_number_compound(self):
        assert _worker_group('worker-vip') == 'worker-vip'
        assert _worker_group('special') == 'special'

    def test_only_digits_returns_itself(self):
        assert _worker_group('123') == '123'


# ---------------------------------------------------------------------------
# 2. _build_prometheus_links (accessed via create_debug_app internals)
# ---------------------------------------------------------------------------


class TestBuildPrometheusLinks:
    async def test_empty_prometheus_url_returns_empty_dicts(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', prometheus_url='')
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)

        # The dashboard endpoint invokes _build_prometheus_links via the
        # template context. We hit /api/dashboard which does NOT include prom
        # links, so we test via the HTML dashboard that renders them.
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        # no prometheus links should appear in the page
        assert 'prometheus' not in resp.text.lower() or 'graph?g0' not in resp.text

    async def test_prometheus_url_set_returns_links(self, mock_recorder, mock_app):
        cfg = DebugConfig(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='cluster="test"',
        )
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        # prometheus graph links should be in the rendered HTML
        assert 'http://prom:9090/graph' in resp.text

    async def test_prometheus_links_card_keys(self, mock_recorder, mock_app):
        """Verify _build_prometheus_links returns expected card/worker/cluster keys."""
        cfg = DebugConfig(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='cluster="prod"',
        )
        # Access _build_prometheus_links by extracting it from the closure.
        # We do this by creating the app and finding the inner function.
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)

        # The function is used inside the dashboard route. We can verify its
        # output by checking the rendered dashboard contains links for each card.
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        body = resp.text
        # card links contain metric names
        assert 'drakkar_offset_lag' in body
        assert 'drakkar_messages_consumed_total' in body
        # cluster links are present when prometheus_cluster_label is set
        assert 'cluster' in body

    async def test_prometheus_links_no_cluster_label(self, mock_recorder, mock_app):
        """When prometheus_cluster_label is empty, cluster_links should be empty."""
        cfg = DebugConfig(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='',
        )
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        # cluster-scoped sum() queries should not appear when label is empty
        # (sum%28 is URL-encoded 'sum(')
        assert 'sum%28rate%28' not in resp.text
        assert 'sum%28drakkar' not in resp.text


# ---------------------------------------------------------------------------
# 3. JSON API endpoints
# ---------------------------------------------------------------------------


class TestApiDashboard:
    async def test_returns_200_with_stats(self, client):
        resp = await client.get('/api/dashboard')
        assert resp.status_code == 200
        data = resp.json()
        assert 'stats' in data
        assert 'uptime' in data
        assert 'partition_count' in data
        assert data['partition_count'] == 0
        assert data['stats']['total_events'] == 42

    async def test_uptime_is_positive(self, client):
        resp = await client.get('/api/dashboard')
        data = resp.json()
        assert data['uptime'] > 0

    async def test_pool_info(self, client):
        resp = await client.get('/api/dashboard')
        data = resp.json()
        assert data['pool_active'] == 2
        assert data['pool_max'] == 8


class TestApiSinks:
    async def test_returns_200_json_list(self, client):
        resp = await client.get('/api/sinks')
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2

    async def test_kafka_sink_stats(self, client):
        resp = await client.get('/api/sinks')
        data = resp.json()
        kafka_sink = next(s for s in data if s['sink_type'] == 'kafka')
        assert kafka_sink['name'] == 'results'
        assert kafka_sink['delivered_count'] == 100
        assert kafka_sink['delivered_payloads'] == 250
        assert kafka_sink['error_count'] == 2
        assert kafka_sink['retry_count'] == 1
        assert kafka_sink['last_delivery_duration'] == 0.012

    async def test_postgres_sink_stats(self, client):
        resp = await client.get('/api/sinks')
        data = resp.json()
        pg_sink = next(s for s in data if s['sink_type'] == 'postgres')
        assert pg_sink['name'] == 'main-db'
        assert pg_sink['delivered_count'] == 80
        assert pg_sink['error_count'] == 0
        assert pg_sink['last_error'] is None


class TestApiDebugDatabases:
    async def test_empty_dir_returns_empty_list(self, tmp_path, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases')
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_lists_db_files(self, tmp_path, mock_recorder, mock_app):
        import sqlite3

        from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG

        db_path = tmp_path / 'worker-1-2026-03-24__10_00_00.db'
        db = sqlite3.connect(str(db_path))
        db.executescript(SCHEMA_WORKER_CONFIG)
        db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url,
                kafka_brokers, source_topic, consumer_group, binary_path,
                max_executors, task_timeout_seconds, max_retries, window_size,
                sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, 'worker-1', 'main', '10.0.0.1', 8080, NULL,
                       'kafka:9092', 'topic', 'grp', '/bin/rg',
                       4, 120, 3, 10, '{}', '{}', 1000.0, '1970-01-01 00:16:40.000')""",
        )
        db.executescript(SCHEMA_EVENTS)
        db.execute(
            "INSERT INTO events (ts, dt, event, partition) VALUES (1000.0, '1970-01-12 13:46:40.000', 'consumed', 0)"
        )
        db.execute(
            "INSERT INTO events (ts, dt, event, partition) VALUES (1001.0, '1970-01-12 13:50:01.000', 'task_completed', 0)"
        )
        db.commit()
        db.close()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases')

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]['worker_name'] == 'worker-1'
        assert data[0]['cluster_name'] == 'main'
        assert data[0]['event_count'] == 2
        assert data[0]['event_counts']['consumed'] == 1
        assert data[0]['event_counts']['task_completed'] == 1


class TestApiDebugProcessors:
    async def test_returns_200_with_empty_processors(self, client):
        resp = await client.get('/api/debug/processors')
        assert resp.status_code == 200
        data = resp.json()
        assert 'processors' in data
        assert isinstance(data['processors'], dict)
        assert len(data['processors']) == 0
        assert data['pool_active'] == 2
        assert data['pool_max'] == 8

    async def test_returns_processor_state(self, debug_config, mock_recorder, mock_app):
        proc = MagicMock()
        proc.queue_size = 10
        proc.inflight_count = 3
        proc._arranging = False
        proc._arrange_start = 0
        proc._arrange_labels = []
        proc._active_tasks = []

        tracker = MagicMock()
        tracker.pending_count = 5
        tracker.completed_count = 20
        tracker.total_tracked = 25
        tracker.last_committed = 99
        tracker.committable.return_value = 100
        tracker._sorted_offsets = [95, 96, 97, 98, 99]
        tracker._offsets = {95: 'completed', 96: 'completed', 97: 'pending', 98: 'pending', 99: 'pending'}
        proc.offset_tracker = tracker

        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/processors')

        assert resp.status_code == 200
        data = resp.json()
        assert '0' in data['processors']
        p = data['processors']['0']
        assert p['queue_size'] == 10
        assert p['inflight_count'] == 3
        assert p['pending_count'] == 5
        assert p['completed_count'] == 20
        assert p['last_committed'] == 99


class TestApiWorkers:
    async def test_returns_200_with_current_worker(self, client, mock_recorder):
        mock_recorder.discover_workers.return_value = []
        resp = await client.get('/api/workers')
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['worker_name'] == 'test-worker'
        assert data[0]['is_current'] is True

    async def test_includes_discovered_workers(self, client, mock_recorder):
        mock_recorder.discover_workers.return_value = [
            {'worker_name': 'worker-2', 'ip_address': '10.0.0.2', 'debug_port': 8080},
        ]
        resp = await client.get('/api/workers')
        data = resp.json()
        assert len(data) == 2
        names = [w['worker_name'] for w in data]
        assert 'test-worker' in names
        assert 'worker-2' in names


# ---------------------------------------------------------------------------
# 4. Debug download endpoint
# ---------------------------------------------------------------------------


class TestDebugDownload:
    async def test_download_existing_file(self, tmp_path, mock_recorder, mock_app):
        db_path = tmp_path / 'test.db'
        db_path.write_bytes(b'fake-sqlite')

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/test.db')

        assert resp.status_code == 200
        assert resp.content == b'fake-sqlite'

    async def test_download_directory_traversal_blocked(self, tmp_path, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/../etc/passwd')
        assert resp.status_code in (400, 404)

    async def test_download_nonexistent_file(self, tmp_path, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/nonexistent.db')
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Existing coverage: HTML pages, WebSocket, sinks, workers
# ---------------------------------------------------------------------------


async def test_dashboard_returns_200(client):
    resp = await client.get('/')
    assert resp.status_code == 200
    assert 'Drakkar' in resp.text
    assert 'test-worker' in resp.text


async def test_dashboard_shows_stats(client):
    resp = await client.get('/')
    assert '42' in resp.text  # total_events
    assert '20' in resp.text  # consumed
    assert '2 / 8' in resp.text  # pool usage


async def test_partitions_page(client):
    resp = await client.get('/partitions')
    assert resp.status_code == 200
    assert 'Partitions' in resp.text


async def test_partition_detail_page(client):
    resp = await client.get('/partitions/0')
    assert resp.status_code == 200
    assert 'Partition 0' in resp.text
    assert 'consumed' in resp.text


async def test_partition_detail_pagination(client):
    resp = await client.get('/partitions/0?page=1')
    assert resp.status_code == 200


async def test_live_page(client):
    resp = await client.get('/live')
    assert resp.status_code == 200
    assert 'Live Pipeline' in resp.text
    assert '2 / 8' in resp.text


async def test_debug_trace_api(client):
    resp = await client.get('/api/debug/trace?partition=0&offset=42')
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


async def test_history_page(client):
    resp = await client.get('/history')
    assert resp.status_code == 200
    assert 'Event History' in resp.text


async def test_history_with_filters(client):
    resp = await client.get('/history?partition=0&event_type=consumed')
    assert resp.status_code == 200


async def test_history_pagination(client):
    resp = await client.get('/history?page=2')
    assert resp.status_code == 200


async def test_dashboard_no_partitions(client, mock_recorder):
    mock_recorder.get_stats.return_value = {'total_events': 0}
    resp = await client.get('/')
    assert resp.status_code == 200


async def test_partitions_page_with_live_processors(debug_config, mock_recorder, mock_app):
    """Partitions page enriches data with live processor state."""
    proc = MagicMock()
    proc.queue_size = 5
    proc.offset_tracker.pending_count = 3
    mock_app.processors = {0: proc}

    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/partitions')
    assert resp.status_code == 200


async def test_live_page_has_tabs_and_ws(debug_config, mock_recorder, mock_app):
    """Live page has tab panels, JS targets, and WebSocket code.

    The three completion-hook tabs (task/message/window results) render
    conditionally based on hook_flags; mock_app's MagicMock handler
    auto-reports all three as "implemented" (class-level getattr returns
    ``None`` which is not identical to the base class's method), so the
    default context renders all three — this test verifies the full set.
    """
    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/live')
    assert resp.status_code == 200
    # Always-on tabs
    assert 'panel-arrange' in resp.text
    assert 'panel-execute' in resp.text
    # Old Collect tab was removed — replaced by completion-hook tabs
    assert 'panel-collect' not in resp.text
    # Completion-hook tabs (visible because mock handler appears to
    # override all three — see docstring)
    assert 'panel-task-results' in resp.text
    assert 'panel-message-results' in resp.text
    assert 'panel-window-results' in resp.text
    assert 'allTasks' in resp.text
    assert '/ws' in resp.text


# --- WebSocket tests ---


async def test_websocket_receives_events(debug_config, mock_recorder, mock_app):
    """WebSocket endpoint streams recorder events to connected clients."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_debug_app(debug_config, real_recorder, mock_app)

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws') as ws:
        real_recorder._record(
            {
                'ts': time.time(),
                'event': 'task_started',
                'partition': 3,
                'task_id': 'ws-test-1',
                'args': '["hello"]',
            }
        )

        data = ws.receive_text()
        event = json.loads(data)
        assert event['event'] == 'task_started'
        assert event['task_id'] == 'ws-test-1'
        assert event['partition'] == 3


async def test_websocket_multiple_events(debug_config, mock_recorder, mock_app):
    """WebSocket receives multiple events in order."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_debug_app(debug_config, real_recorder, mock_app)

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws') as ws:
        for i in range(3):
            real_recorder._record(
                {
                    'ts': time.time(),
                    'event': 'consumed',
                    'partition': i,
                    'offset': i * 10,
                }
            )

        for i in range(3):
            data = ws.receive_text()
            event = json.loads(data)
            assert event['event'] == 'consumed'
            assert event['partition'] == i


async def test_websocket_cleanup_on_disconnect(debug_config, mock_recorder, mock_app):
    """Subscriber queue is removed when WebSocket disconnects."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_debug_app(debug_config, real_recorder, mock_app)

    assert len(real_recorder._ws_subscribers) == 0

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws') as ws:
        assert len(real_recorder._ws_subscribers) == 1
        real_recorder._record({'ts': time.time(), 'event': 'test'})
        ws.receive_text()

    assert len(real_recorder._ws_subscribers) == 0


# --- /api/live/arrange-tasks (Arrange tab state lookup) ---
#
# The endpoint queries the recorder's SQLite for a specific set of
# task_ids, ignoring the ws_min_duration_ms filter and the 10-minute
# timeline window that /api/recent-tasks applies. The Arrange tab uses
# it to keep the sidebar fresh for batches outside the timeline window.


async def _start_live_recorder(tmp_path):
    """Spin up a real EventRecorder with on-disk SQLite for endpoint tests.

    Short flush interval so buffered events land in the DB before the
    endpoint reads — the endpoint itself also ``await recorder._flush()``s
    defensively, but the fixture helper avoids relying on that alone.
    """
    from drakkar.recorder import EventRecorder

    cfg = DebugConfig(enabled=True, db_dir=str(tmp_path), flush_interval_seconds=60)
    rec = EventRecorder(cfg, worker_name='test-arrange-tasks')
    await rec.start()
    return rec


async def test_arrange_tasks_empty_ids_returns_empty_map(mock_recorder, mock_app, debug_config):
    """POST with no task_ids → empty map, no recorder flush/query."""
    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/live/arrange-tasks', json={'task_ids': []})
    assert resp.status_code == 200
    assert resp.json() == {}


async def test_arrange_tasks_returns_running_state(tmp_path, mock_app, debug_config):
    """task_started with no completion → status='running', duration=None."""
    rec = await _start_live_recorder(tmp_path)
    try:
        from drakkar.models import ExecutorTask

        task = ExecutorTask(
            task_id='rg-running-1',
            args=['--x', '1'],
            source_offsets=[42, 43],
        )
        rec.record_task_started(task, partition=7)

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/live/arrange-tasks', json={'task_ids': ['rg-running-1']})
        body = resp.json()
        assert 'rg-running-1' in body
        t = body['rg-running-1']
        assert t['status'] == 'running'
        assert t['partition'] == 7
        assert t['source_offsets'] == [42, 43]
        assert t['end_ts'] is None
        assert t['duration'] is None
    finally:
        await rec.stop()


async def test_arrange_tasks_returns_completed_state(tmp_path, mock_app, debug_config):
    """task_started + task_completed collapses to status='completed' with duration."""
    rec = await _start_live_recorder(tmp_path)
    try:
        from drakkar.models import ExecutorResult, ExecutorTask

        task = ExecutorTask(task_id='rg-done-1', args=['--x', '1'], source_offsets=[100])
        rec.record_task_started(task, partition=3)
        result = ExecutorResult(
            exit_code=0,
            stdout='ok',
            stderr='',
            duration_seconds=0.123,
            task=task,
        )
        rec.record_task_completed(result, partition=3)

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/live/arrange-tasks', json={'task_ids': ['rg-done-1']})
        body = resp.json()
        t = body['rg-done-1']
        assert t['status'] == 'completed'
        assert t['duration'] == 0.123
        assert t['partition'] == 3
        assert t['source_offsets'] == [100]
        assert t['end_ts'] is not None
    finally:
        await rec.stop()


async def test_arrange_tasks_returns_failed_state(tmp_path, mock_app, debug_config):
    """task_failed → status='failed' with exit_code surfaced."""
    rec = await _start_live_recorder(tmp_path)
    try:
        from drakkar.models import ExecutorError, ExecutorTask

        task = ExecutorTask(task_id='rg-fail-1', args=['--fail'], source_offsets=[200])
        rec.record_task_started(task, partition=9)
        err = ExecutorError(
            exit_code=2,
            stderr='boom',
            stdout='',
            duration_seconds=0.05,
            task=task,
        )
        rec.record_task_failed(task, err, partition=9)

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/live/arrange-tasks', json={'task_ids': ['rg-fail-1']})
        body = resp.json()
        t = body['rg-fail-1']
        assert t['status'] == 'failed'
        assert t['partition'] == 9
        assert t['exit_code'] == 2
    finally:
        await rec.stop()


async def test_arrange_tasks_unknown_id_absent_from_response(tmp_path, mock_app, debug_config):
    """IDs not in the DB aren't fabricated — just absent from the map."""
    rec = await _start_live_recorder(tmp_path)
    try:
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/live/arrange-tasks',
                json={'task_ids': ['never-recorded-1', 'never-recorded-2']},
            )
        assert resp.status_code == 200
        assert resp.json() == {}
    finally:
        await rec.stop()


async def test_arrange_tasks_batch_lookup_mixed_states(tmp_path, mock_app, debug_config):
    """Multiple task_ids in one request return correct per-task state —
    running / completed / failed / unknown all in the same response."""
    rec = await _start_live_recorder(tmp_path)
    try:
        from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask

        t_run = ExecutorTask(task_id='rg-mix-running', args=[], source_offsets=[1])
        t_ok = ExecutorTask(task_id='rg-mix-ok', args=[], source_offsets=[2])
        t_fail = ExecutorTask(task_id='rg-mix-fail', args=[], source_offsets=[3])
        rec.record_task_started(t_run, partition=0)
        rec.record_task_started(t_ok, partition=0)
        rec.record_task_started(t_fail, partition=0)
        rec.record_task_completed(
            ExecutorResult(exit_code=0, stdout='', stderr='', duration_seconds=0.5, task=t_ok),
            partition=0,
        )
        rec.record_task_failed(
            t_fail,
            ExecutorError(exit_code=1, stderr='x', stdout='', duration_seconds=0.1, task=t_fail),
            partition=0,
        )

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/live/arrange-tasks',
                json={
                    'task_ids': ['rg-mix-running', 'rg-mix-ok', 'rg-mix-fail', 'rg-mix-missing'],
                },
            )
        body = resp.json()
        assert body['rg-mix-running']['status'] == 'running'
        assert body['rg-mix-ok']['status'] == 'completed'
        assert body['rg-mix-fail']['status'] == 'failed'
        assert 'rg-mix-missing' not in body
    finally:
        await rec.stop()


# --- /api/live/{task,message,window}-results — completion-hook feeds ---
#
# These feed the three tabs that replaced the old "Collect" tab in
# /live. Each endpoint is a LIMIT-N indexed scan on the recorder's
# events table — no joins (task-results does one extra batch lookup by
# task_id for exec-duration / status, still a single query).


async def test_task_results_returns_latest_n(tmp_path, mock_app, debug_config):
    """Recent task_complete events, newest first, with paired exec state."""
    rec = await _start_live_recorder(tmp_path)
    try:
        from drakkar.models import ExecutorResult, ExecutorTask

        # Record a subprocess outcome then the hook completion for the
        # same task_id — the endpoint pairs them by task_id.
        t1 = ExecutorTask(task_id='rg-tr-1', args=[], source_offsets=[10, 11])
        rec.record_task_started(t1, partition=2)
        rec.record_task_completed(
            ExecutorResult(exit_code=0, stdout='', stderr='', duration_seconds=0.5, task=t1),
            partition=2,
        )
        rec.record_task_complete(task_id='rg-tr-1', partition=2, duration=0.012, output_message_count=3)

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/task-results?limit=10')
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 1
        row = body[0]
        assert row['task_id'] == 'rg-tr-1'
        assert row['partition'] == 2
        assert row['hook_duration'] == 0.012
        # Paired from task_completed
        assert row['exec_duration'] == 0.5
        assert row['status'] == 'completed'
        # Pulled from metadata.output_message_count
        assert row['output_message_count'] == 3
        # source_offsets paired from the task_started event's metadata —
        # essential for rendering the message source in the Task Results tab.
        assert row['source_offsets'] == [10, 11]
    finally:
        await rec.stop()


async def test_task_results_missing_exec_pair_surfaces_null_status(
    tmp_path,
    mock_app,
    debug_config,
):
    """task_complete without a matching task_completed → status=None
    (surfaces as "?" in the UI, not a fabricated success/failure)."""
    rec = await _start_live_recorder(tmp_path)
    try:
        rec.record_task_complete(
            task_id='rg-orphan',
            partition=1,
            duration=0.008,
            output_message_count=1,
        )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/task-results')
        body = resp.json()
        assert len(body) == 1
        assert body[0]['status'] is None
        assert body[0]['exec_duration'] is None
    finally:
        await rec.stop()


async def test_message_results_returns_latest_n(tmp_path, mock_app, debug_config):
    """message_complete events are returned with their metadata expanded
    into the top-level response (succeeded/failed/replaced/outputs)."""
    rec = await _start_live_recorder(tmp_path)
    try:
        rec.record_message_complete(
            partition=3,
            offset=100,
            duration=0.025,
            task_count=5,
            succeeded=4,
            failed=1,
            replaced=0,
            output_message_count=7,
        )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results')
        body = resp.json()
        assert len(body) == 1
        row = body[0]
        assert row['partition'] == 3
        assert row['offset'] == 100
        assert row['duration'] == 0.025
        assert row['task_count'] == 5
        assert row['succeeded'] == 4
        assert row['failed'] == 1
        assert row['replaced'] == 0
        assert row['output_message_count'] == 7
        # No matching consumed event in the DB — end_to_end is None rather
        # than a fabricated zero.
        assert row['end_to_end_duration'] is None
    finally:
        await rec.stop()


async def test_message_results_end_to_end_duration_paired_from_consumed(
    tmp_path,
    mock_app,
    debug_config,
):
    """When a consumed event exists for (partition, offset), the response
    carries end_to_end_duration = message_complete.ts - consumed.ts."""
    rec = await _start_live_recorder(tmp_path)
    try:
        # Record consumed first, then message_complete. The recorder uses
        # time.time() for ``ts`` so we just ensure a minimal wall-clock
        # gap between the two and assert the difference is measurable.
        from drakkar.models import SourceMessage

        msg = SourceMessage(topic='t', partition=3, offset=100, value=b'{}', timestamp=1000)
        rec.record_consumed(msg)
        import asyncio

        await asyncio.sleep(0.02)
        rec.record_message_complete(
            partition=3,
            offset=100,
            duration=0.01,
            task_count=1,
            succeeded=1,
            failed=0,
            replaced=0,
            output_message_count=1,
        )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results')
        body = resp.json()
        assert len(body) == 1
        row = body[0]
        # End-to-end = message_complete.ts - consumed.ts. We slept 20ms so
        # it's comfortably > 0. Upper bound is loose to avoid flakiness
        # on slow CI runners.
        assert row['end_to_end_duration'] is not None
        assert row['end_to_end_duration'] > 0.0
        assert row['end_to_end_duration'] < 5.0
    finally:
        await rec.stop()


async def test_message_results_end_to_end_picks_most_recent_prior_consumed(
    tmp_path,
    mock_app,
    debug_config,
):
    """If the same (partition, offset) was consumed multiple times (e.g.
    replay after restart), the pairing picks the most-recent consumed
    event prior to the message_complete — not the oldest."""
    rec = await _start_live_recorder(tmp_path)
    try:
        import asyncio

        from drakkar.models import SourceMessage

        msg = SourceMessage(topic='t', partition=3, offset=100, value=b'{}', timestamp=1000)
        # Old consumed event (simulates a previous replay)
        rec.record_consumed(msg)
        await asyncio.sleep(0.05)
        # Newer consumed event — the one that this message_complete belongs to
        rec.record_consumed(msg)
        await asyncio.sleep(0.02)
        rec.record_message_complete(
            partition=3,
            offset=100,
            duration=0.01,
            task_count=1,
            succeeded=1,
            failed=0,
            replaced=0,
            output_message_count=1,
        )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results')
        body = resp.json()
        row = body[0]
        # If pairing picked the oldest consumed it would be > 0.06s.
        # Picking the most-recent-prior consumed puts it between the 20ms
        # sleep and the 50ms total gap — assert < 50ms with margin.
        assert row['end_to_end_duration'] is not None
        assert row['end_to_end_duration'] < 0.05
    finally:
        await rec.stop()


async def test_window_results_returns_latest_n(tmp_path, mock_app, debug_config):
    """window_complete events carry window_id + task/output counts."""
    rec = await _start_live_recorder(tmp_path)
    try:
        rec.record_window_complete(
            partition=5,
            window_id=42,
            duration=1.2,
            task_count=20,
            output_message_count=35,
        )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/window-results')
        body = resp.json()
        assert len(body) == 1
        row = body[0]
        assert row['partition'] == 5
        assert row['window_id'] == 42
        assert row['duration'] == 1.2
        assert row['task_count'] == 20
        assert row['output_message_count'] == 35
    finally:
        await rec.stop()


async def test_completion_endpoints_empty_when_no_events(tmp_path, mock_app, debug_config):
    """All three endpoints return [] on an empty DB, no 500s."""
    rec = await _start_live_recorder(tmp_path)
    try:
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for url in (
                '/api/live/task-results',
                '/api/live/message-results',
                '/api/live/window-results',
            ):
                resp = await c.get(url)
                assert resp.status_code == 200, url
                assert resp.json() == [], url
    finally:
        await rec.stop()


async def test_completion_endpoints_ordered_desc_and_limited(tmp_path, mock_app, debug_config):
    """Latest events first, and limit caps the response length."""
    rec = await _start_live_recorder(tmp_path)
    try:
        for off in range(5):
            rec.record_message_complete(
                partition=1,
                offset=off,
                duration=0.01,
                task_count=1,
                succeeded=1,
                failed=0,
                replaced=0,
                output_message_count=1,
            )
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results?limit=3')
        body = resp.json()
        assert len(body) == 3
        # Newest first — offsets 4, 3, 2
        assert body[0]['offset'] == 4
        assert body[1]['offset'] == 3
        assert body[2]['offset'] == 2
    finally:
        await rec.stop()


# --- /api/live/sink-breakdown — group produced events by sink name ---


async def test_sink_breakdown_groups_by_output_topic(tmp_path, mock_app, debug_config):
    """Produced events for (partition, offsets) collapsed by output_topic."""
    rec = await _start_live_recorder(tmp_path)
    try:
        # Build three produced payloads with distinct sink names. The
        # recorder keys ``output_topic`` on ``payload.sink``, so our test
        # payloads set that attribute directly.
        from pydantic import BaseModel as _BaseModel

        class _Payload(_BaseModel):
            sink: str

        for off in (10, 10, 11):
            rec.record_produced(
                _Payload(sink='kafka.results'),
                source_partition=7,
                source_offset=off,
            )
        rec.record_produced(
            _Payload(sink='postgres.main'),
            source_partition=7,
            source_offset=10,
        )
        rec.record_produced(
            _Payload(sink='redis.cache'),
            source_partition=7,
            source_offset=11,
        )
        # Different partition — must not leak into the breakdown
        rec.record_produced(
            _Payload(sink='kafka.results'),
            source_partition=8,
            source_offset=10,
        )

        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/live/sink-breakdown',
                json={'partition': 7, 'offsets': [10, 11]},
            )
        body = resp.json()
        assert body == {'kafka.results': 3, 'postgres.main': 1, 'redis.cache': 1}
    finally:
        await rec.stop()


async def test_sink_breakdown_empty_offsets_returns_empty_map(
    tmp_path,
    mock_app,
    debug_config,
):
    """Empty offsets list short-circuits, no SQL, returns {}."""
    rec = await _start_live_recorder(tmp_path)
    try:
        fastapi_app = create_debug_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/live/sink-breakdown', json={'partition': 0, 'offsets': []})
        assert resp.status_code == 200
        assert resp.json() == {}
    finally:
        await rec.stop()


# --- Hook-flag detection — hides unused completion-hook tabs ---


def test_hook_flags_no_overrides_returns_all_false():
    """Plain BaseDrakkarHandler subclass with no hook overrides → all False."""
    from drakkar.debug_server import _hook_flags
    from drakkar.handler import BaseDrakkarHandler

    class H(BaseDrakkarHandler):
        pass

    flags = _hook_flags(H())
    assert flags == {
        'task_complete': False,
        'message_complete': False,
        'window_complete': False,
    }


def test_hook_flags_detects_overrides():
    """Each overridden hook flips its flag; non-overridden stay False."""
    from drakkar.debug_server import _hook_flags
    from drakkar.handler import BaseDrakkarHandler
    from drakkar.models import CollectResult, ExecutorResult, MessageGroup

    class H(BaseDrakkarHandler):
        async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
            return None

        async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
            return None

        # on_window_complete NOT overridden — should stay False.

    flags = _hook_flags(H())
    assert flags['task_complete'] is True
    assert flags['message_complete'] is True
    assert flags['window_complete'] is False


# --- Sinks page and API ---


async def test_sinks_page_returns_200(client):
    resp = await client.get('/sinks')
    assert resp.status_code == 200
    assert 'Sinks' in resp.text


async def test_sinks_page_shows_configured_sinks(client):
    resp = await client.get('/sinks')
    assert 'kafka' in resp.text
    assert 'results' in resp.text
    assert 'postgres' in resp.text
    assert 'main-db' in resp.text


async def test_sinks_page_shows_stats(client):
    resp = await client.get('/sinks')
    assert '100' in resp.text  # delivered_count
    assert '250' in resp.text  # delivered_payloads


async def test_sinks_page_shows_errors(client):
    resp = await client.get('/sinks')
    assert '2' in resp.text


async def test_sinks_nav_link(client):
    """Sinks link appears in the navigation bar."""
    resp = await client.get('/')
    assert 'href="/sinks"' in resp.text


# --- Workers autodiscovery API ---


async def test_api_workers_includes_current(client, mock_recorder):
    """Current worker is always included in the list with is_current=True."""
    mock_recorder.discover_workers.return_value = [
        {'worker_name': 'worker-2', 'ip_address': '10.0.0.2', 'debug_port': 8080},
    ]
    resp = await client.get('/api/workers')
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    current = [w for w in data if w['is_current']]
    assert len(current) == 1
    assert current[0]['worker_name'] == 'test-worker'


async def test_api_workers_only_self_when_no_others(client, mock_recorder):
    """Even with no discovered workers, current worker appears."""
    mock_recorder.discover_workers.return_value = []
    resp = await client.get('/api/workers')
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]['worker_name'] == 'test-worker'
    assert data[0]['is_current'] is True


async def test_api_workers_uses_debug_url(client, mock_recorder):
    """When debug_url is set, it is returned as the url field."""
    mock_recorder.discover_workers.return_value = [
        {'worker_name': 'w-1', 'ip_address': '10.0.0.2', 'debug_port': 8080, 'debug_url': 'http://localhost:8081/'},
    ]
    resp = await client.get('/api/workers')
    data = resp.json()
    w1 = next(w for w in data if w['worker_name'] == 'w-1')
    assert w1['url'] == 'http://localhost:8081/'


async def test_api_workers_falls_back_to_ip_port(client, mock_recorder):
    """When debug_url is not set, url falls back to http://ip:port/."""
    mock_recorder.discover_workers.return_value = [
        {'worker_name': 'w-1', 'ip_address': '10.0.0.5', 'debug_port': 9090, 'debug_url': None},
    ]
    resp = await client.get('/api/workers')
    data = resp.json()
    w1 = next(w for w in data if w['worker_name'] == 'w-1')
    assert w1['url'] == 'http://10.0.0.5:9090/'


async def test_api_workers_grouped_by_cluster(client, mock_recorder, mock_app):
    """Workers are grouped by cluster_name, with clustered first."""
    mock_app._cluster_name = 'alpha'
    mock_recorder.discover_workers.return_value = [
        {'worker_name': 'w-3', 'ip_address': '10.0.0.3', 'debug_port': 8080, 'cluster_name': 'beta'},
        {'worker_name': 'w-1', 'ip_address': '10.0.0.1', 'debug_port': 8080, 'cluster_name': 'alpha'},
        {'worker_name': 'w-lone', 'ip_address': '10.0.0.9', 'debug_port': 8080, 'cluster_name': None},
        {'worker_name': 'w-2', 'ip_address': '10.0.0.2', 'debug_port': 8080, 'cluster_name': 'alpha'},
    ]
    resp = await client.get('/api/workers')
    data = resp.json()
    names = [w['worker_name'] for w in data]
    clusters = [w['cluster'] for w in data]
    assert names == ['test-worker', 'w-1', 'w-2', 'w-3', 'w-lone']
    assert clusters == ['alpha', 'alpha', 'alpha', 'beta', '']


async def test_api_workers_unclustered_at_end(client, mock_recorder, mock_app):
    """Workers without cluster_name appear at the end, sorted by name."""
    mock_app._cluster_name = ''
    mock_recorder.discover_workers.return_value = [
        {'worker_name': 'z-worker', 'ip_address': '10.0.0.1', 'debug_port': 8080, 'cluster_name': None},
        {'worker_name': 'a-worker', 'ip_address': '10.0.0.2', 'debug_port': 8080, 'cluster_name': 'prod'},
    ]
    resp = await client.get('/api/workers')
    data = resp.json()
    names = [w['worker_name'] for w in data]
    assert names == ['a-worker', 'test-worker', 'z-worker']


# --- Debug databases page and API ---


@pytest.fixture
async def debug_client(tmp_path, mock_recorder, mock_app):
    """Client with a real db_dir for debug database endpoints."""
    cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


async def test_debug_page_returns_200(debug_client):
    resp = await debug_client.get('/debug')
    assert resp.status_code == 200
    assert 'Message Trace' in resp.text
    assert 'Databases' in resp.text


async def test_api_debug_databases_empty(debug_client):
    resp = await debug_client.get('/api/debug/databases')
    assert resp.status_code == 200
    assert resp.json() == []


async def test_api_debug_databases_lists_files(tmp_path, mock_recorder, mock_app):
    import sqlite3

    from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG

    db_path = tmp_path / 'worker-1-2026-03-24__10_00_00.db'
    db = sqlite3.connect(str(db_path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, debug_url,
            kafka_brokers, source_topic, consumer_group, binary_path,
            max_executors, task_timeout_seconds, max_retries, window_size,
            sinks_json, env_vars_json, created_at, created_at_dt)
           VALUES (1, 'worker-1', 'main', '10.0.0.1', 8080, NULL,
                   'kafka:9092', 'topic', 'grp', '/bin/rg',
                   4, 120, 3, 10, '{}', '{}', 1000.0, '1970-01-01 00:16:40.000')""",
    )
    db.executescript(SCHEMA_EVENTS)
    db.execute(
        "INSERT INTO events (ts, dt, event, partition) VALUES (1000.0, '1970-01-12 13:46:40.000', 'consumed', 0)"
    )
    db.execute(
        "INSERT INTO events (ts, dt, event, partition) VALUES (1001.0, '1970-01-12 13:50:01.000', 'task_completed', 0)"
    )
    db.commit()
    db.close()

    cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/debug/databases')

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]['worker_name'] == 'worker-1'
    assert data[0]['cluster_name'] == 'main'
    assert data[0]['event_count'] == 2
    assert data[0]['event_counts']['consumed'] == 1
    assert data[0]['event_counts']['task_completed'] == 1


async def test_api_debug_databases_skips_symlinks(tmp_path, mock_recorder, mock_app):
    import sqlite3

    from drakkar.recorder import SCHEMA_EVENTS

    db_path = tmp_path / 'w1.db'
    db = sqlite3.connect(str(db_path))
    db.executescript(SCHEMA_EVENTS)
    db.commit()
    db.close()
    os.symlink('w1.db', str(tmp_path / 'w1-live.db'))

    cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/debug/databases')

    data = resp.json()
    filenames = [d['filename'] for d in data]
    assert 'w1.db' in filenames
    assert 'w1-live.db' not in filenames


async def test_api_debug_merge(tmp_path, mock_recorder, mock_app):
    import sqlite3

    from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG

    for name in ['w1', 'w2']:
        p = tmp_path / f'{name}.db'
        db = sqlite3.connect(str(p))
        db.executescript(SCHEMA_WORKER_CONFIG)
        db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url,
                kafka_brokers, source_topic, consumer_group, binary_path,
                max_executors, task_timeout_seconds, max_retries, window_size,
                sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, 'main', '10.0.0.1', 8080, NULL,
                       'k:9092', 't', 'g', '/bin/x', 4, 60, 2, 5, '{}', '{}', 1000.0, '1970-01-01 00:16:40.000')""",
            [name],
        )
        db.executescript(SCHEMA_EVENTS)
        db.execute(
            "INSERT INTO events (ts, dt, event, partition) VALUES (1000.0, '1970-01-12 13:46:40.000', 'consumed', 0)"
        )
        db.commit()
        db.close()

    cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post(
            '/api/debug/merge',
            json={'filenames': ['w1.db', 'w2.db']},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data['worker_count'] == 2
    assert data['event_count'] == 2
    assert data['cluster_name'] == 'main'
    assert data['filename'].startswith('merged-')
    assert os.path.isfile(os.path.join(str(tmp_path), data['filename']))


async def test_api_debug_merge_rejects_single_file(debug_client):
    resp = await debug_client.post(
        '/api/debug/merge',
        json={'filenames': ['only-one.db']},
    )
    assert resp.status_code == 400


async def test_api_debug_merge_rejects_traversal(debug_client):
    resp = await debug_client.post(
        '/api/debug/merge',
        json={'filenames': ['../etc/passwd', 'a.db']},
    )
    assert resp.status_code == 400


async def test_debug_download(tmp_path, mock_recorder, mock_app):
    db_path = tmp_path / 'test.db'
    db_path.write_bytes(b'fake-sqlite')

    cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/debug/download/test.db')

    assert resp.status_code == 200
    assert resp.content == b'fake-sqlite'


async def test_debug_download_rejects_traversal(debug_client):
    """Traversal attempts are blocked — either 400 or 404 (no file served)."""
    resp = await debug_client.get('/debug/download/../../../etc/passwd')
    assert resp.status_code in (400, 404)


async def test_debug_download_missing_file(debug_client):
    resp = await debug_client.get('/debug/download/nonexistent.db')
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Additional coverage tests
# ---------------------------------------------------------------------------


class TestGetSinkUiLinksEmpty:
    """Cover _get_sink_ui_links when sink_manager is falsy (line 84)."""

    async def test_no_sink_manager_returns_no_links(self, debug_config, mock_recorder, mock_app):
        mock_app.sink_manager = None
        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200


class TestGetSinkUiLinksWithUrls:
    """Cover _get_sink_ui_links filtering/deduplication (lines 91-92)."""

    async def test_sinks_with_ui_url_appear_in_nav(self, debug_config, mock_recorder, mock_app):
        sink_mgr = mock_app.sink_manager
        sink_mgr.get_sink_info.return_value = [
            {'sink_type': 'kafka', 'name': 'results', 'ui_url': 'http://kafka-ui:8080'},
            {'sink_type': 'kafka', 'name': 'results2', 'ui_url': 'http://kafka-ui:8080'},
            {'sink_type': 'postgres', 'name': 'main-db'},
        ]
        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'http://kafka-ui:8080' in resp.text


class TestGetLagWithConsumer:
    """Cover _get_lag calling consumer.get_partition_lag (lines 107-112)."""

    async def test_lag_data_on_partitions_page(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_partition_lag.return_value = {
            0: {'committed': 100, 'high_watermark': 150, 'lag': 50},
        }
        mock_app._consumer = consumer

        proc = MagicMock()
        proc.queue_size = 5
        proc.offset_tracker.pending_count = 3
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/partitions')
        assert resp.status_code == 200
        consumer.get_partition_lag.assert_awaited_once()

    async def test_lag_exception_returns_empty(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_partition_lag.side_effect = RuntimeError('connection lost')
        mock_app._consumer = consumer

        proc = MagicMock()
        proc.queue_size = 0
        proc.offset_tracker.pending_count = 0
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/partitions')
        assert resp.status_code == 200


class TestDashboardTotalLag:
    """Cover dashboard get_total_lag block (lines 261-264)."""

    async def test_dashboard_shows_total_lag(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_total_lag.return_value = 1234
        mock_app._consumer = consumer

        proc = MagicMock()
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert '1234' in resp.text or '1,234' in resp.text

    async def test_dashboard_total_lag_exception_shows_zero(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_total_lag.side_effect = RuntimeError('fail')
        mock_app._consumer = consumer

        proc = MagicMock()
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200


class TestDashboardCustomLinks:
    """Cover custom_links template expansion (lines 268-278)."""

    async def test_custom_links_rendered(self, mock_recorder, mock_app):
        cfg = DebugConfig(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            custom_links=[
                {'name': 'Grafana', 'url': 'http://grafana/{worker_id}'},
                {'name': 'Logs', 'url': 'http://logs/{cluster_name}'},
            ],
        )
        mock_app._worker_id = 'worker-7'
        mock_app._cluster_name = 'prod'

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'http://grafana/worker-7' in resp.text
        assert 'http://logs/prod' in resp.text


class TestLivePageWithTasks:
    """Cover live page task processing (lines 350, 357-367, 383-384)."""

    async def test_live_page_with_active_and_pending_tasks(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_active_tasks.return_value = [
            {'task_id': 'task-active-1', 'ts': now - 5, 'event': 'task_started'},
        ]

        pending_task = MagicMock()
        pending_task.args = '["--fast"]'
        pending_task.source_offsets = [10, 11]

        proc = MagicMock()
        proc.partition_id = 0
        proc._pending_tasks = {'task-active-1': pending_task, 'task-pending-1': pending_task}
        proc._arranging = False
        proc._active_tasks = []

        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/live')
        assert resp.status_code == 200
        assert 'Live Pipeline' in resp.text

    async def test_live_page_with_arranging_processor(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_active_tasks.return_value = []

        proc = MagicMock()
        proc.partition_id = 0
        proc._pending_tasks = {}
        proc._arranging = True
        proc._arrange_start = now - 2.5
        proc._arrange_labels = ['label-a', 'label-b']
        proc._active_tasks = []

        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/live')
        assert resp.status_code == 200
        assert 'Arrange' in resp.text


class TestTaskDetailPage:
    """Cover /task/{task_id} page (lines 412-437)."""

    async def test_task_detail_with_events(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_task_events.return_value = [
            {
                'id': 1,
                'ts': now - 10,
                'event': 'task_started',
                'partition': 0,
                'offset': None,
                'task_id': 'task-abc',
                'args': '["--input", "f.txt"]',
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': None,
                'duration': None,
                'output_topic': None,
                'pid': 1234,
                'metadata': json.dumps({'source_offsets': [10, 11], 'slot': 2}),
            },
            {
                'id': 2,
                'ts': now - 5,
                'event': 'task_completed',
                'partition': 0,
                'offset': None,
                'task_id': 'task-abc',
                'args': None,
                'stdout_size': 512,
                'stdout': 'output data',
                'stderr': None,
                'exit_code': 0,
                'duration': 5.0,
                'output_topic': None,
                'pid': 1234,
                'metadata': None,
            },
        ]

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-abc')
        assert resp.status_code == 200
        assert 'task-abc' in resp.text
        assert '5.0' in resp.text or '5.00' in resp.text

    async def test_task_detail_retry_key_strips_suffix(self, debug_config, mock_recorder, mock_app):
        mock_recorder.get_task_events.return_value = []

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-abc:r1234567.89')
        assert resp.status_code == 200
        mock_recorder.get_task_events.assert_awaited_with('task-abc')

    async def test_task_detail_no_started_event(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_task_events.return_value = [
            {
                'id': 1,
                'ts': now - 5,
                'event': 'task_failed',
                'partition': 0,
                'offset': None,
                'task_id': 'task-xyz',
                'args': None,
                'stdout_size': 0,
                'stdout': None,
                'stderr': 'error msg',
                'exit_code': 1,
                'duration': None,
                'output_topic': None,
                'pid': 5678,
                'metadata': None,
            },
        ]

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-xyz')
        assert resp.status_code == 200

    async def test_task_detail_duration_computed_from_timestamps(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_task_events.return_value = [
            {
                'id': 1,
                'ts': now - 10,
                'event': 'task_started',
                'partition': 0,
                'offset': None,
                'task_id': 'task-dur',
                'args': None,
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': None,
                'duration': None,
                'output_topic': None,
                'pid': None,
                'metadata': None,
            },
            {
                'id': 2,
                'ts': now - 3,
                'event': 'task_completed',
                'partition': 0,
                'offset': None,
                'task_id': 'task-dur',
                'args': None,
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': 0,
                'duration': None,
                'output_topic': None,
                'pid': None,
                'metadata': None,
            },
        ]

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-dur')
        assert resp.status_code == 200


class TestMergeEndpointDotPrefixed:
    """Cover merge endpoint rejecting dot-prefixed filenames (line 579)."""

    async def test_merge_rejects_dot_prefixed_filename(self, tmp_path, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/merge',
                json={'filenames': ['.hidden.db', 'normal.db']},
            )
        assert resp.status_code == 400


class TestMergeEndpointFileNotFound:
    """Cover merge endpoint file-not-found branch (line 579)."""

    async def test_merge_nonexistent_file_returns_404(self, tmp_path, mock_recorder, mock_app):
        # Create one valid file so only the second triggers the 404
        (tmp_path / 'exists.db').write_bytes(b'fake')

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/merge',
                json={'filenames': ['exists.db', 'missing.db']},
            )
        assert resp.status_code == 404
        assert 'missing.db' in resp.json()['error']


class TestApiDebugMetrics:
    """Cover /api/debug/metrics endpoint (lines 611-613)."""

    async def test_returns_metrics_list(self, debug_config, mock_recorder, mock_app):
        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/metrics')
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


class TestDownloadRealpathTraversal:
    """Cover realpath canonicalization in download endpoint."""

    async def test_download_symlink_outside_db_dir_blocked(self, tmp_path, mock_recorder, mock_app):
        """A symlink inside db_dir pointing outside is blocked by realpath check."""
        outside_dir = tmp_path / 'outside'
        outside_dir.mkdir()
        secret = outside_dir / 'secret.db'
        secret.write_bytes(b'secret-data')

        db_dir = tmp_path / 'db'
        db_dir.mkdir()
        # create symlink inside db_dir pointing outside
        link = db_dir / 'escape.db'
        link.symlink_to(secret)

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(db_dir))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/escape.db')
        assert resp.status_code == 400


class TestDownloadDotPrefixed:
    """Cover download endpoint blocking dot-prefixed files (line 620)."""

    async def test_download_dot_prefixed_blocked(self, tmp_path, mock_recorder, mock_app):
        hidden = tmp_path / '.secret.db'
        hidden.write_bytes(b'secret')

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/.secret.db')
        assert resp.status_code == 400


class TestApiEvents:
    """Cover /api/events endpoint with filters (lines 640-668)."""

    async def _make_client_with_db(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'live.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        for i in range(5):
            await db.execute(
                'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
                (now - i, '2026-04-02', 'consumed' if i % 2 == 0 else 'task_completed', i % 2, i, f'task-{i}'),
            )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        client = AsyncClient(transport=transport, base_url='http://test')
        return client, db

    async def test_events_no_filter(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_db(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/events')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 5
        await db.close()

    async def test_events_filter_by_partition(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_db(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/events?partitions=0')
        assert resp.status_code == 200
        data = resp.json()
        assert all(e['partition'] == 0 for e in data)
        await db.close()

    async def test_events_filter_by_event_type(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_db(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/events?event_types=consumed')
        assert resp.status_code == 200
        data = resp.json()
        assert all(e['event'] == 'consumed' for e in data)
        await db.close()

    async def test_events_filter_by_after_id(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_db(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/events?after_id=3')
        assert resp.status_code == 200
        data = resp.json()
        assert all(e['id'] > 3 for e in data)
        await db.close()

    async def test_events_no_db_returns_empty(self, debug_config, mock_recorder, mock_app):
        mock_recorder._db = None
        mock_recorder._flush = AsyncMock()

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/events')
        assert resp.status_code == 200
        assert resp.json() == []


class TestApiRecentTasks:
    """Cover /api/recent-tasks endpoint (lines 673-742)."""

    async def _make_client_with_task_events(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'live.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        # task-1: started and completed
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid, metadata) '
            "VALUES (?, ?, 'task_started', 0, 'task-1', '[\"--fast\"]', 100, ?)",
            (now - 30, '2026-04-02', json.dumps({'slot': 1})),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid) '
            "VALUES (?, ?, 'task_completed', 0, 'task-1', 1.5, 100)",
            (now - 28, '2026-04-02'),
        )
        # task-2: started but not completed (running)
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 1, 'task-2', '[\"--slow\"]', 200)",
            (now - 10, '2026-04-02'),
        )
        # task-3: started, then retried (two task_started events)
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'task-3', '[\"--retry\"]', 300)",
            (now - 20, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'task-3', '[\"--retry\"]', 301)",
            (now - 15, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid) '
            "VALUES (?, ?, 'task_completed', 0, 'task-3', 2.0, 301)",
            (now - 13, '2026-04-02'),
        )
        # task-4: failed
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 1, 'task-4', '[]', 400)",
            (now - 8, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid) '
            "VALUES (?, ?, 'task_failed', 1, 'task-4', 0.5, 400)",
            (now - 7, '2026-04-02'),
        )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        client = AsyncClient(transport=transport, base_url='http://test')
        return client, db

    async def test_recent_tasks_returns_task_entries(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        assert resp.status_code == 200
        data = resp.json()
        assert 'tasks' in data
        assert 'lane_count' in data
        assert data['lane_count'] == 8
        tasks = data['tasks']
        assert len(tasks) >= 1
        await db.close()

    async def test_recent_tasks_completed_status(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-1']['status'] == 'completed'
        assert tasks_by_id['task-1']['duration'] == 1.5
        await db.close()

    async def test_recent_tasks_running_status(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-2']['status'] == 'running'
        assert tasks_by_id['task-2']['end_ts'] is None
        await db.close()

    async def test_recent_tasks_failed_status(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-4']['status'] == 'failed'
        await db.close()

    async def test_recent_tasks_retry_creates_archive_entry(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        task_ids = [t['task_id'] for t in data['tasks']]
        # The retry should create an archived entry with :r prefix
        retry_entries = [tid for tid in task_ids if ':r' in tid]
        assert len(retry_entries) >= 1
        await db.close()

    async def test_recent_tasks_no_db_returns_empty(self, debug_config, mock_recorder, mock_app):
        mock_recorder._db = None
        mock_recorder._flush = AsyncMock()

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/recent-tasks')
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_recent_tasks_slot_extracted_from_metadata(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-1']['slot'] == 1
        await db.close()


class TestApiDashboardWithConsumerLag:
    """Cover /api/dashboard with consumer lag (lines 754-757)."""

    async def test_api_dashboard_includes_lag(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_total_lag.return_value = 42
        mock_app._consumer = consumer

        proc = MagicMock()
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/dashboard')
        assert resp.status_code == 200
        data = resp.json()
        assert data['total_lag'] == 42


class TestProcessorDiagnosticsArrangeAndStuck:
    """Cover processor diagnostics: arrange info + stuck tasks (lines 805, 827-832, 839)."""

    async def test_processor_with_arrange_info(self, debug_config, mock_recorder, mock_app):
        proc = MagicMock()
        proc.queue_size = 10
        proc.inflight_count = 3
        proc._arranging = True
        proc._arrange_start = time.time() - 5.0
        proc._arrange_labels = ['label-x', 'label-y']
        proc._active_tasks = []

        tracker = MagicMock()
        tracker.pending_count = 2
        tracker.completed_count = 10
        tracker.total_tracked = 12
        tracker.last_committed = 50
        tracker.committable.return_value = 51
        tracker._sorted_offsets = [49, 50, 51]
        tracker._offsets = {49: 'completed', 50: 'completed', 51: 'pending'}
        proc.offset_tracker = tracker

        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/processors')
        assert resp.status_code == 200
        data = resp.json()
        p = data['processors']['0']
        assert p['arranging'] is True
        assert p['arrange'] is not None
        assert p['arrange']['message_count'] == 2
        assert 'label-x' in p['arrange']['labels']

    async def test_processor_with_stuck_tasks(self, debug_config, mock_recorder, mock_app):
        proc = MagicMock()
        proc.queue_size = 5
        proc.inflight_count = 1
        proc._arranging = False
        proc._arrange_start = 0
        proc._arrange_labels = []

        # Create a mock task that is not done
        stuck_task = MagicMock()
        stuck_task.done.return_value = False
        stuck_task.get_name.return_value = 'stuck-task-1'
        frame = MagicMock()
        frame.f_code.co_filename = '/app/worker.py'
        frame.f_lineno = 42
        frame.f_code.co_name = 'process_message'
        stuck_task.get_stack.return_value = [frame]
        proc._active_tasks = [stuck_task]

        tracker = MagicMock()
        tracker.pending_count = 1
        tracker.completed_count = 5
        tracker.total_tracked = 6
        tracker.last_committed = 20
        tracker.committable.return_value = 21
        tracker._sorted_offsets = [20, 21]
        tracker._offsets = {20: 'completed', 21: 'pending'}
        proc.offset_tracker = tracker

        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/processors')
        assert resp.status_code == 200
        data = resp.json()
        p = data['processors']['0']
        assert 'stuck_tasks' in p
        assert len(p['stuck_tasks']) == 1
        assert p['stuck_tasks'][0]['name'] == 'stuck-task-1'
        assert '/app/worker.py:42 in process_message' in p['stuck_tasks'][0]['stack'][0]


class TestDebugPage:
    """Cover debug page with config_summary."""

    async def test_debug_page_shows_config_summary(self, tmp_path, mock_recorder, mock_app):
        mock_app.config_summary = 'worker=test-worker topic=events group=drakkar'
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug')
        assert resp.status_code == 200
        assert 'worker=test-worker' in resp.text

    async def test_probe_tab_rendered_in_debug_html(self, tmp_path, mock_recorder, mock_app):
        """``GET /debug`` → HTML includes the Message Probe tab button, panel,
        the visible header, the results container id, AND the rendering
        helpers (``renderProbeReport`` + section markers) that Task 7 adds.
        Keeps the UI work honest without depending on a JS test stack — the
        endpoint behavior itself is covered by the probe endpoint tests above.
        """
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug')
        assert resp.status_code == 200
        html = resp.text
        # Tab button + panel pair keep the tab switcher working.
        assert 'data-tab="probe"' in html
        assert 'data-tab-panel="probe"' in html
        # Visible header + results container id — anchors for later task
        # rendering.
        assert 'Message Probe' in html
        assert 'probe-result' in html
        # Task 7: rendering helpers must be present so the client can turn
        # a DebugReport into A/B/C cards. These are inline-script markers,
        # so a substring check is enough to prove the functions were
        # injected.
        assert 'renderProbeReport' in html
        assert 'renderProbeSectionInput' in html
        assert 'renderProbeSectionArrange' in html
        assert 'renderProbeSectionTasks' in html
        # Section wrapper ids are used by the tests and also by future
        # Task-8 cross-linking (replacement_for / retry_of scroll targets).
        assert 'probe-section-input' in html
        assert 'probe-section-arrange' in html
        assert 'probe-section-tasks' in html
        # Task 8: the right-side task-detail sidebar and its helpers.
        # The sidebar container is rendered once in the probe tab panel.
        assert 'id="probe-task-sidebar"' in html
        # openTaskSidebar is the real (non-stub) implementation — its name
        # is referenced both by the row onclick and by the scroll-to-row
        # helper.
        assert 'openTaskSidebar' in html
        # ESC-close marker: a document-level keydown listener that dismisses
        # the sidebar. The function name is the simplest stable marker.
        assert 'closeTaskSidebar' in html
        # Task 9: section helpers and wrapper ids for on_message_complete /
        # on_window_complete / Planned sink outputs. Presence of the
        # function names proves the helpers were injected; the section ids
        # prove ``renderProbeReport`` wires them in.
        assert 'renderProbeSectionOnMessageComplete' in html
        assert 'renderProbeSectionOnWindowComplete' in html
        assert 'renderProbeSectionSinks' in html
        assert 'probe-section-message-complete' in html
        assert 'probe-section-window-complete' in html
        assert 'probe-section-sinks' in html
        # Task 10: Cache calls / Timeline / Errors sections + the toolbar +
        # the input-form chip polish. All surfaces are script-injected so a
        # substring check is enough — the endpoint behavior is covered by
        # the probe endpoint tests below.
        assert 'renderProbeSectionCacheCalls' in html
        assert 'renderProbeSectionTimeline' in html
        assert 'renderProbeSectionErrors' in html
        assert 'renderProbeToolbar' in html
        assert 'probe-section-cache-calls' in html
        assert 'probe-section-timeline' in html
        assert 'probe-section-errors' in html
        assert 'probe-result-toolbar' in html
        assert 'collapseProbeFormToChip' in html
        assert 'probe-form-chip' in html


class TestDebugServerClass:
    """Cover DebugServer start/stop (lines 945-977)."""

    async def test_start_creates_server_and_thread(self, debug_config, mock_recorder, mock_app):
        from unittest.mock import patch

        from drakkar.debug_server import DebugServer

        server = DebugServer(debug_config, mock_recorder, mock_app)
        assert server._server is None
        assert server._thread is None

        with (
            patch('drakkar.debug_server.uvicorn.Server') as mock_uvi_server,
            patch('drakkar.debug_server.uvicorn.Config') as mock_uvi_config,
            patch('drakkar.debug_server.threading.Thread') as mock_thread,
            patch('drakkar.debug_server.logger') as mock_logger,
        ):
            mock_uvi_server.return_value = MagicMock()
            mock_uvi_config.return_value = MagicMock()
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance
            mock_logger.ainfo = AsyncMock()

            await server.start()

            mock_uvi_server.assert_called_once()
            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()
            assert server._server is not None
            assert server._thread is not None

    async def test_stop_signals_exit_and_joins(self, debug_config, mock_recorder, mock_app):
        from unittest.mock import patch

        from drakkar.debug_server import DebugServer

        server = DebugServer(debug_config, mock_recorder, mock_app)
        server._server = MagicMock()
        server._thread = MagicMock()

        with patch('drakkar.debug_server.logger') as mock_logger:
            mock_logger.ainfo = AsyncMock()
            await server.stop()

        assert server._server.should_exit is True
        server._thread.join.assert_called_once_with(timeout=5.0)

    async def test_stop_when_not_started(self, debug_config, mock_recorder, mock_app):
        from unittest.mock import patch

        from drakkar.debug_server import DebugServer

        server = DebugServer(debug_config, mock_recorder, mock_app)

        with patch('drakkar.debug_server.logger') as mock_logger:
            mock_logger.ainfo = AsyncMock()
            await server.stop()  # should not raise


# ---------------------------------------------------------------------------
# 6. Additional edge-case coverage
# ---------------------------------------------------------------------------


class TestTaskDetailEdgeCases:
    """Cover JSON decode exception paths in task_detail (lines 428-429, 434-435)."""

    async def test_task_detail_invalid_metadata_json(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_task_events.return_value = [
            {
                'id': 1,
                'ts': now - 10,
                'event': 'task_started',
                'partition': 0,
                'offset': None,
                'task_id': 'task-bad-meta',
                'args': '["ok"]',
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': None,
                'duration': None,
                'output_topic': None,
                'pid': None,
                'metadata': 'not-valid-json{{{',
            },
            {
                'id': 2,
                'ts': now - 5,
                'event': 'task_completed',
                'partition': 0,
                'offset': None,
                'task_id': 'task-bad-meta',
                'args': None,
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': 0,
                'duration': 5.0,
                'output_topic': None,
                'pid': None,
                'metadata': None,
            },
        ]

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-bad-meta')
        assert resp.status_code == 200

    async def test_task_detail_invalid_args_json(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_task_events.return_value = [
            {
                'id': 1,
                'ts': now - 10,
                'event': 'task_started',
                'partition': 0,
                'offset': None,
                'task_id': 'task-bad-args',
                'args': 'not-json!!!',
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': None,
                'duration': None,
                'output_topic': None,
                'pid': None,
                'metadata': None,
            },
            {
                'id': 2,
                'ts': now - 5,
                'event': 'task_completed',
                'partition': 0,
                'offset': None,
                'task_id': 'task-bad-args',
                'args': None,
                'stdout_size': 0,
                'stdout': None,
                'stderr': None,
                'exit_code': 0,
                'duration': 5.0,
                'output_topic': None,
                'pid': None,
                'metadata': None,
            },
        ]

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-bad-args')
        assert resp.status_code == 200


class TestApiRecentTasksEdgeCases:
    """Cover edge cases in api_recent_tasks (lines 697, 715-716)."""

    async def test_events_without_task_id_are_skipped(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'live.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        # event with no task_id (e.g. a 'consumed' event)
        await db.execute(
            "INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, 'task_started', 0, 99)",
            (now - 10, '2026-04-02'),
        )
        # event with task_id
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'real-task', '[]', 100)",
            (now - 5, '2026-04-02'),
        )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        assert resp.status_code == 200
        data = resp.json()
        task_ids = [t['task_id'] for t in data['tasks']]
        assert 'real-task' in task_ids
        await db.close()

    async def test_events_with_invalid_metadata_json(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'live.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid, metadata) '
            "VALUES (?, ?, 'task_started', 0, 'task-bad', '[]', 100, 'invalid{json')",
            (now - 5, '2026-04-02'),
        )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        assert resp.status_code == 200
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-bad']['slot'] is None
        await db.close()


class TestPrometheusWorkerLabel:
    """Cover _build_prometheus_links with prometheus_worker_label set (line 142)."""

    async def test_prometheus_worker_label_used_in_links(self, mock_recorder, mock_app):
        cfg = DebugConfig(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_worker_label='job="drakkar",instance="{worker_id}"',
        )
        mock_app._worker_id = 'worker-42'

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'worker-42' in resp.text
        assert 'http://prom:9090/graph' in resp.text


class TestApiDashboardLagException:
    """Cover consumer lag exception in /api/dashboard (lines 756-757)."""

    async def test_api_dashboard_lag_exception_returns_zero(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_total_lag.side_effect = RuntimeError('connection lost')
        mock_app._consumer = consumer

        proc = MagicMock()
        mock_app.processors = {0: proc}

        fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/dashboard')
        assert resp.status_code == 200
        data = resp.json()
        assert data['total_lag'] == 0


# ---------------------------------------------------------------------------
# Auth token tests
# ---------------------------------------------------------------------------


class TestAuthToken:
    """Test auth_token protection on sensitive endpoints."""

    PROTECTED_ROUTES: typing.ClassVar[list[tuple[str, str]]] = [
        ('GET', '/api/debug/databases'),
        ('GET', '/debug/download/test.db'),
    ]

    async def test_protected_routes_require_token(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for method, path in self.PROTECTED_ROUTES:
                resp = await c.request(method, path)
                assert resp.status_code == 401, f'{method} {path} should require auth'

    async def test_protected_routes_accept_bearer_header(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        headers = {'Authorization': 'Bearer secret-123'}
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases', headers=headers)
            assert resp.status_code == 200

    async def test_protected_routes_accept_query_param(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases?token=secret-123')
            assert resp.status_code == 200

    async def test_wrong_token_returns_401(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases', headers={'Authorization': 'Bearer wrong'})
            assert resp.status_code == 401

    async def test_no_auth_when_token_empty(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases')
            assert resp.status_code == 200

    async def test_unprotected_routes_always_accessible(self, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for path in ['/', '/partitions', '/sinks', '/live', '/history', '/debug']:
                resp = await c.get(path)
                assert resp.status_code == 200, f'{path} should be accessible without auth'

    async def test_merge_requires_token(self, tmp_path, mock_recorder, mock_app):
        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
        mock_recorder._config = cfg
        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/merge', json={'filenames': []})
            assert resp.status_code == 401
            resp = await c.post(
                '/api/debug/merge',
                json={'filenames': []},
                headers={'Authorization': 'Bearer secret-123'},
            )
            assert resp.status_code != 401


# ---------------------------------------------------------------------------
# Periodic tasks API
# ---------------------------------------------------------------------------


class TestApiPeriodicTasks:
    """Tests for /api/debug/periodic endpoint."""

    async def test_periodic_empty_when_no_events(self, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = '/tmp/test-periodic-empty.db'
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp')
        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/periodic')
        assert resp.status_code == 200
        assert resp.json() == []
        await db.close()

    async def test_periodic_returns_task_runs(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'periodic.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        await db.execute(
            'INSERT INTO events (ts, dt, event, task_id, duration, exit_code, metadata) '
            "VALUES (?, ?, 'periodic_run', 'refresh_cache', 0.15, 0, ?)",
            (now - 60, '2026-04-07', '{"status": "ok"}'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, task_id, duration, exit_code, metadata) '
            "VALUES (?, ?, 'periodic_run', 'refresh_cache', 0.22, 0, ?)",
            (now - 30, '2026-04-07', '{"status": "ok"}'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, task_id, duration, exit_code, metadata) '
            "VALUES (?, ?, 'periodic_run', 'health_check', 0.01, 1, ?)",
            (now - 10, '2026-04-07', '{"status": "error", "error": "connection refused"}'),
        )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/periodic')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

        # sorted by name
        health = next(t for t in data if t['name'] == 'health_check')
        cache = next(t for t in data if t['name'] == 'refresh_cache')

        assert health['last_status'] == 'error'
        assert health['last_error'] == 'connection refused'
        assert health['total_ok'] == 0
        assert health['total_error'] == 1

        assert cache['last_status'] == 'ok'
        assert cache['total_ok'] == 2
        assert cache['total_error'] == 0
        assert len(cache['recent']) == 2

        await db.close()


# ---------------------------------------------------------------------------
# Label trace API tests
# ---------------------------------------------------------------------------


class TestApiLabelTrace:
    """Tests for /api/debug/label-keys and /api/debug/trace-by-label endpoints."""

    async def _make_client_with_labeled_events(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'labels.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        now = time.time()
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, labels) '
            "VALUES (?, ?, 'task_started', 0, 'task-a', ?)",
            (now - 20, '2026-04-08', '{"request_id": "req-123", "user": "alice"}'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, labels) '
            "VALUES (?, ?, 'task_completed', 0, 'task-a', 1.5, ?)",
            (now - 18, '2026-04-08', '{"request_id": "req-123", "user": "alice"}'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, labels) '
            "VALUES (?, ?, 'task_started', 1, 'task-b', ?)",
            (now - 10, '2026-04-08', '{"request_id": "req-456", "user": "bob"}'),
        )
        await db.execute(
            "INSERT INTO events (ts, dt, event, partition, task_id) VALUES (?, ?, 'task_started', 2, 'task-no-labels')",
            (now - 5, '2026-04-08'),
        )
        await db.commit()

        cfg = DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg
        mock_recorder._worker_name = 'test-worker'
        mock_recorder._cluster_name = ''
        mock_recorder._db_path = db_path

        # wire cross_trace_by_label to actually query the real DB
        from drakkar.recorder import _LABEL_TRACE_QUERY

        async def _real_cross_trace(label_key, label_value):
            json_path = f'$.{label_key}'
            async with db.execute(_LABEL_TRACE_QUERY, [json_path, label_value]) as cursor:
                columns = [d[0] for d in cursor.description]
                rows = await cursor.fetchall()
                events = [dict(zip(columns, row, strict=False)) for row in rows]
            for ev in events:
                ev['worker_name'] = 'test-worker'
            return events

        mock_recorder.cross_trace_by_label = _real_cross_trace

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        client = AsyncClient(transport=transport, base_url='http://test')
        return client, db

    async def test_label_keys_returns_distinct_keys(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_labeled_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/debug/label-keys')
        assert resp.status_code == 200
        keys = resp.json()
        assert 'request_id' in keys
        assert 'user' in keys
        assert len(keys) == 2
        await db.close()

    async def test_label_keys_empty_when_no_labels(self, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db = await aiosqlite.connect(':memory:')
        await db.executescript(SCHEMA_EVENTS)

        cfg = DebugConfig(enabled=True, port=8080, db_dir='/tmp')
        mock_recorder._db = db
        mock_recorder._flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder._config = cfg

        fastapi_app = create_debug_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/label-keys')
        assert resp.status_code == 200
        assert resp.json() == []
        await db.close()

    async def test_trace_by_label_finds_matching_events(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_labeled_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/debug/trace-by-label?key=request_id&value=req-123')
        assert resp.status_code == 200
        events = resp.json()
        task_ids = {e['task_id'] for e in events}
        assert 'task-a' in task_ids
        assert 'task-b' not in task_ids
        assert 'task-no-labels' not in task_ids
        event_types = {e['event'] for e in events}
        assert 'task_started' in event_types
        assert 'task_completed' in event_types
        await db.close()

    async def test_trace_by_label_no_match_returns_empty(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_labeled_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/debug/trace-by-label?key=request_id&value=nonexistent')
        assert resp.status_code == 200
        assert resp.json() == []
        await db.close()

    async def test_trace_by_label_user_key(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_labeled_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/debug/trace-by-label?key=user&value=bob')
        assert resp.status_code == 200
        events = resp.json()
        assert len(events) >= 1
        assert all(e['task_id'] == 'task-b' for e in events)
        await db.close()


# ---------------------------------------------------------------------------
# 9. /api/debug/probe — Message Probe endpoint
# ---------------------------------------------------------------------------
#
# These tests exercise the full HTTP path of the Message Probe feature
# end-to-end: the endpoint wiring, the ``DebugRunner`` instantiation, the
# concurrency lock, the ``use_cache`` forwarding to ``DebugCacheProxy``,
# and the wall-clock timeout fallback to ``latest_partial_report``.
#
# The fixture ``_probe_mock_app`` extends the default ``mock_app`` with a
# real ``BaseDrakkarHandler`` instance, a real ``ExecutorPool`` (bogus
# binary path — only ``precomputed`` tasks are used), and the attributes
# that ``DebugRunner`` reads directly. Tests craft tiny per-scenario
# handlers by mutating this one instance.


@pytest.fixture
def _probe_mock_app(mock_app):
    """Extend ``mock_app`` with handler + real ExecutorPool for probe tests.

    The base ``mock_app`` fixture uses ``MagicMock`` for most fields; the
    probe endpoint instead needs a real ``BaseDrakkarHandler`` (for the
    ``cache`` swap) and a real ``ExecutorPool`` (so precomputed tasks
    hit the actual fast path). We keep the rest of the mock intact so
    other routes in the same app still render.
    """
    from drakkar.executor import ExecutorPool
    from drakkar.handler import BaseDrakkarHandler

    # Default no-op handler; individual tests monkey-patch methods.
    mock_app.handler = BaseDrakkarHandler()
    mock_app._executor_pool = ExecutorPool(
        binary_path='/nonexistent/binary/should-never-run',
        max_executors=2,
        task_timeout_seconds=5,
    )
    # Replace the MagicMock-based config with a real one so the endpoint
    # can read ``executor.task_timeout_seconds`` without TypeError.
    mock_app._config = DrakkarConfig()
    return mock_app


def _make_precomputed_task(task_id: str, offset: int, stdout: str = 'ok'):
    """Build a precomputed ExecutorTask used across probe tests.

    Imported lazily to avoid pulling drakkar.models into the module-level
    import block (keeps the existing import ordering untouched for the
    other tests in this file).
    """
    from drakkar.models import ExecutorTask, PrecomputedResult, make_task_id

    return ExecutorTask(
        task_id=task_id or make_task_id(),
        source_offsets=[offset],
        labels={},
        stdin=None,
        precomputed=PrecomputedResult(
            stdout=stdout,
            stderr='',
            exit_code=0,
            duration_seconds=0.001,
        ),
    )


class _ProbePayload(BaseModel):
    """BaseModel used as the payload body of test sink records.

    Kept as a separate top-level class so each KafkaPayload we emit in
    tests has a proper ``data: BaseModel`` (the ``CollectResult``
    serializer rejects raw dicts).
    """

    ok: bool = True
    note: str = 'probe'


class _ProbeTestHandler:
    """Minimal duck-typed handler for the probe endpoint tests.

    Not a ``BaseDrakkarHandler`` subclass because the tests here only
    care about hook dispatch, not generic input/output models. The
    ``cache`` attribute starts as a ``NoOpCache`` so ``DebugCacheProxy``
    has something to wrap; individual tests may swap in a real ``Cache``.
    """

    def __init__(self, *, task_count: int = 1) -> None:
        from drakkar.cache import NoOpCache
        from drakkar.models import CollectResult, KafkaPayload

        self._task_count = task_count
        self._CollectResult = CollectResult
        self._KafkaPayload = KafkaPayload
        self.cache = NoOpCache()
        self.arrange_calls = 0
        self.on_task_complete_calls = 0
        self.on_message_complete_calls = 0
        self.on_window_complete_calls = 0

    def message_label(self, msg):
        return f'{msg.partition}:{msg.offset}'

    def deserialize_message(self, msg):
        return msg

    async def arrange(self, messages, pending):
        self.arrange_calls += 1
        msg = messages[0]
        return [_make_precomputed_task(task_id=f't-{i}', offset=msg.offset) for i in range(self._task_count)]

    async def on_task_complete(self, result):
        self.on_task_complete_calls += 1
        return self._CollectResult(
            kafka=[self._KafkaPayload(sink='results', key=result.task.task_id.encode(), data=_ProbePayload())],
        )

    async def on_message_complete(self, group):
        self.on_message_complete_calls += 1
        return None

    async def on_window_complete(self, results, source_messages):
        self.on_window_complete_calls += 1
        return None

    async def on_error(self, task, error):
        from drakkar.models import ErrorAction

        return ErrorAction.SKIP


async def test_probe_endpoint_valid_body_returns_report(mock_recorder, debug_config, _probe_mock_app):
    """POST with a valid ProbeInput → 200 + DebugReport with the expected keys."""
    _probe_mock_app.handler = _ProbeTestHandler(task_count=1)

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post(
            '/api/debug/probe',
            json={'value': '{"hello": "world"}', 'key': 'k-1', 'partition': 3, 'offset': 42},
        )
    assert resp.status_code == 200
    body = resp.json()
    # Every top-level key of DebugReport should be present — the endpoint
    # serializes the full model.
    for key in (
        'input',
        'arrange',
        'tasks',
        'on_message_complete',
        'on_window_complete',
        'planned_sink_payloads',
        'cache_calls',
        'cache_summary',
        'timing',
        'errors',
        'truncated',
    ):
        assert key in body, f'missing key: {key}'
    assert body['truncated'] is False
    assert body['input']['value'] == '{"hello": "world"}'
    assert body['input']['partition'] == 3
    assert body['input']['offset'] == 42
    assert len(body['tasks']) == 1


async def test_probe_endpoint_empty_body_returns_422(mock_recorder, debug_config, _probe_mock_app):
    """POST with ``{}`` → 422 because ``value`` is a required field."""
    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={})
    assert resp.status_code == 422
    # FastAPI's validation response includes a ``detail`` list with one
    # entry per missing/invalid field.
    detail = resp.json()['detail']
    assert any('value' in (err.get('loc') or []) for err in detail)


async def test_probe_endpoint_concurrent_calls_serialize(mock_recorder, debug_config, _probe_mock_app):
    """Two overlapping POSTs → both return 200; the runner's lock serializes them.

    We rely on the probe lock inside ``DebugRunner`` to guarantee no
    interleaving. This test only asserts both complete successfully —
    proving serialization requires timing assertions that are flaky; the
    lock's correctness is exercised directly in ``test_debug_runner.py``.
    """
    _probe_mock_app.handler = _ProbeTestHandler(task_count=1)

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp_a_coro = c.post('/api/debug/probe', json={'value': 'a', 'offset': 1})
        resp_b_coro = c.post('/api/debug/probe', json={'value': 'b', 'offset': 2})
        resp_a, resp_b = await asyncio.gather(resp_a_coro, resp_b_coro)
    assert resp_a.status_code == 200
    assert resp_b.status_code == 200
    # Both echoed their own input value — proof that the endpoint didn't
    # mix up requests when they overlap.
    values = {resp_a.json()['input']['value'], resp_b.json()['input']['value']}
    assert values == {'a', 'b'}


async def test_probe_endpoint_use_cache_true_sees_seeded_value(mock_recorder, debug_config, _probe_mock_app):
    """use_cache=True: handler sees a hit on a pre-seeded key."""
    from drakkar.cache import Cache

    real_cache = Cache(origin_worker_id='worker-probe-test')
    real_cache.set('known', {'v': 1})

    handler = _ProbeTestHandler(task_count=1)
    handler.cache = real_cache
    # Capture what the handler sees at arrange time.
    observed: dict = {}
    original_arrange = handler.arrange

    async def arrange_with_read(messages, pending):
        observed['value'] = await handler.cache.get('known')
        return await original_arrange(messages, pending)

    handler.arrange = arrange_with_read  # type: ignore[method-assign]
    _probe_mock_app.handler = handler

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x', 'use_cache': True})
    assert resp.status_code == 200
    assert observed['value'] == {'v': 1}
    # The cache_calls log should show a hit for the seeded key.
    calls = resp.json()['cache_calls']
    first_get = next(c for c in calls if c['op'] == 'get' and c['key'] == 'known')
    assert first_get['outcome'] == 'hit'


async def test_probe_endpoint_use_cache_false_sees_miss(mock_recorder, debug_config, _probe_mock_app):
    """use_cache=False: the seeded key is invisible to the handler (miss)."""
    from drakkar.cache import Cache

    real_cache = Cache(origin_worker_id='worker-probe-test')
    real_cache.set('known', {'v': 1})

    handler = _ProbeTestHandler(task_count=1)
    handler.cache = real_cache
    observed: dict = {}
    original_arrange = handler.arrange

    async def arrange_with_read(messages, pending):
        observed['value'] = await handler.cache.get('known')
        return await original_arrange(messages, pending)

    handler.arrange = arrange_with_read  # type: ignore[method-assign]
    _probe_mock_app.handler = handler

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x', 'use_cache': False})
    assert resp.status_code == 200
    assert observed['value'] is None
    calls = resp.json()['cache_calls']
    first_get = next(c for c in calls if c['op'] == 'get' and c['key'] == 'known')
    assert first_get['outcome'] == 'miss'


async def test_probe_endpoint_does_not_mutate_cache(mock_recorder, debug_config, _probe_mock_app):
    """Handler calls cache.set during probe → live cache state unchanged.

    Takes a snapshot of the real Cache's internal ``_dirty`` dict and
    ``_bytes_sum`` accumulator before the request; re-reads them after
    and asserts equality. This is the core safety-guarantee test for
    the probe on the endpoint side — the runner's version of the same
    check lives in ``test_debug_runner.py``.
    """
    from drakkar.cache import Cache

    real_cache = Cache(origin_worker_id='worker-probe-test')

    handler = _ProbeTestHandler(task_count=1)
    handler.cache = real_cache
    original_arrange = handler.arrange

    async def arrange_with_writes(messages, pending):
        handler.cache.set('write-a', {'v': 1})
        handler.cache.set('write-b', {'v': 2})
        return await original_arrange(messages, pending)

    handler.arrange = arrange_with_writes  # type: ignore[method-assign]
    _probe_mock_app.handler = handler

    # Snapshot the dirty map BEFORE the request.
    dirty_before = dict(real_cache._dirty)
    bytes_before = real_cache._bytes_sum

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x'})
    assert resp.status_code == 200
    # Same keys, same byte accumulator — proof that the two ``set``
    # calls inside the handler were suppressed by DebugCacheProxy.
    assert dict(real_cache._dirty) == dirty_before
    assert real_cache._bytes_sum == bytes_before


async def test_probe_endpoint_handler_arrange_raises_returns_200_with_errors(
    mock_recorder, debug_config, _probe_mock_app
):
    """Handler arrange raises → endpoint returns 200 with errors populated, NOT 500."""
    handler = _ProbeTestHandler(task_count=1)

    async def arrange_raises(messages, pending):
        raise RuntimeError('boom-from-arrange')

    handler.arrange = arrange_raises  # type: ignore[method-assign]
    _probe_mock_app.handler = handler

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x'})
    # Error is captured in the report, not bubbled as a 500.
    assert resp.status_code == 200
    body = resp.json()
    assert body['arrange']['error'] is not None
    assert any(err['stage'] == 'arrange' for err in body['errors'])
    assert any('boom-from-arrange' in err['message'] for err in body['errors'])


async def test_probe_endpoint_timeout_returns_partial_report_with_truncated_true(
    mock_recorder, debug_config, _probe_mock_app, monkeypatch
):
    """Handler on_task_complete sleeps past the wall-clock timeout → 200 + truncated=True.

    Set ``task_timeout_seconds=1`` (the minimum ExecutorConfig allows)
    and monkey-patch ``PROBE_TIMEOUT_HEADROOM_SECONDS`` to ``-1.9`` so
    the total wait_for timeout is ``2*1 + (-1.9) = 0.1s``. The
    handler's on_task_complete then sleeps 1s, guaranteeing wait_for
    fires and cancels the run. Verifies:
      1. Response is 200 (not 504)
      2. ``truncated=True`` in the body
      3. ``handler.cache`` is the ORIGINAL object after the request
         (the runner's ``finally`` block ran during cancellation cascade)
    """
    import drakkar.debug_server as debug_server_mod
    from drakkar.config import ExecutorConfig

    handler = _ProbeTestHandler(task_count=1)
    original_cache = handler.cache  # NoOpCache instance

    async def on_task_complete_slow(result):
        # Sleep longer than the test timeout so wait_for is forced to cancel.
        await asyncio.sleep(1.0)
        return None

    handler.on_task_complete = on_task_complete_slow  # type: ignore[method-assign]
    _probe_mock_app.handler = handler
    # Build a DrakkarConfig with the minimum allowed per-task timeout.
    # Total endpoint timeout = 2 * 1 + PROBE_TIMEOUT_HEADROOM_SECONDS.
    # With a -1.9 headroom that lands at 0.1s, which is well below the
    # handler's 1s sleep.
    _probe_mock_app._config = DrakkarConfig(
        executor=ExecutorConfig(task_timeout_seconds=1, binary_path='/nonexistent'),
    )
    monkeypatch.setattr(debug_server_mod, 'PROBE_TIMEOUT_HEADROOM_SECONDS', -1.9)

    fastapi_app = create_debug_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x'})
    assert resp.status_code == 200
    body = resp.json()
    assert body['truncated'] is True
    # The arrange stage completed before on_task_complete blocked, so it
    # should be present in the partial report.
    assert body['arrange']['duration_seconds'] is not None
    # handler.cache was restored to the original NoOpCache — the finally
    # block in DebugRunner.run ran during the cancellation cascade.
    assert handler.cache is original_cache
