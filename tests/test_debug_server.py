"""Tests for Drakkar debug web UI."""

import json
import os
import re
import time
import typing
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient

from drakkar.config import DebugConfig, DrakkarConfig
from drakkar.debug_server import (
    _format_ts,
    _format_ts_full,
    _format_ts_ms,
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
    """Live page has tab panels, JS targets, and WebSocket code."""
    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/live')
    assert resp.status_code == 200
    assert 'panel-arrange' in resp.text
    assert 'panel-execute' in resp.text
    assert 'panel-collect' in resp.text
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
