"""Tests for Drakkar debug web UI."""

import json
import os
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient

from drakkar.config import DebugConfig
from drakkar.debug_server import _worker_group, create_debug_app
from drakkar.recorder import EventRecorder


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
    rec.cross_trace.return_value = [
        {**e, 'worker_name': 'test-worker'} for e in rec.get_events.return_value
    ]
    rec.get_active_tasks.return_value = []
    return rec


@pytest.fixture
def mock_app():
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 120
    app.processors = {}

    pool = MagicMock()
    pool.active_count = 2
    pool.max_workers = 8
    app._executor_pool = pool

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
    # use a real recorder for subscribe/unsubscribe
    real_recorder = EventRecorder(debug_config)
    # don't start (no DB needed) — just use subscribe/broadcast
    real_recorder._running = True

    fastapi_app = create_debug_app(debug_config, real_recorder, mock_app)

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws') as ws:
        # send an event through the recorder
        real_recorder._record(
            {
                'ts': time.time(),
                'event': 'task_started',
                'partition': 3,
                'task_id': 'ws-test-1',
                'args': '["hello"]',
            }
        )

        # should receive it via websocket
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

    # after disconnect, subscriber should be cleaned up
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
    # kafka/results has 2 errors
    assert '2' in resp.text


async def test_api_sinks_returns_json(client):
    resp = await client.get('/api/sinks')
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    kafka_sink = next(s for s in data if s['sink_type'] == 'kafka')
    assert kafka_sink['name'] == 'results'
    assert kafka_sink['delivered_count'] == 100
    assert kafka_sink['delivered_payloads'] == 250
    assert kafka_sink['error_count'] == 2
    assert kafka_sink['retry_count'] == 1
    assert kafka_sink['last_delivery_duration'] == 0.012


async def test_api_sinks_postgres_stats(client):
    resp = await client.get('/api/sinks')
    data = resp.json()
    pg_sink = next(s for s in data if s['sink_type'] == 'postgres')
    assert pg_sink['name'] == 'main-db'
    assert pg_sink['delivered_count'] == 80
    assert pg_sink['error_count'] == 0
    assert pg_sink['last_error'] is None


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
    # clustered first sorted by cluster then name, unclustered at end
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
    # a-worker (prod cluster) first, then test-worker and z-worker (unclustered)
    assert names == ['a-worker', 'test-worker', 'z-worker']


# --- _worker_group helper ---


def test_worker_group_strips_trailing_number():
    assert _worker_group('worker-1') == 'worker'
    assert _worker_group('worker-3') == 'worker'
    assert _worker_group('worker-15') == 'worker'


def test_worker_group_strips_number_without_separator():
    assert _worker_group('worker15') == 'worker'


def test_worker_group_preserves_middle_numbers():
    assert _worker_group('worker-vip-1') == 'worker-vip'
    assert _worker_group('worker-vip-2') == 'worker-vip'


def test_worker_group_with_underscore_separator():
    assert _worker_group('slow_worker_05') == 'slow_worker'


def test_worker_group_complex_names():
    assert _worker_group('slow-worker-01') == 'slow-worker'
    assert _worker_group('slow-worker-05') == 'slow-worker'


def test_worker_group_no_trailing_number():
    assert _worker_group('worker-vip') == 'worker-vip'
    assert _worker_group('special') == 'special'


def test_worker_group_only_number():
    """A name that is only digits should return itself (not empty)."""
    assert _worker_group('123') == '123'


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

    # create a fake DB file
    db_path = tmp_path / 'worker-1-2026-03-24__10_00_00.db'
    db = sqlite3.connect(str(db_path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, debug_url,
            kafka_brokers, source_topic, consumer_group, binary_path,
            max_workers, task_timeout_seconds, max_retries, window_size,
            sinks_json, env_vars_json, created_at, created_at_dt)
           VALUES (1, 'worker-1', 'main', '10.0.0.1', 8080, NULL,
                   'kafka:9092', 'topic', 'grp', '/bin/rg',
                   4, 120, 3, 10, '{}', '{}', 1000.0, '1970-01-01 00:16:40.000')""",
    )
    db.executescript(SCHEMA_EVENTS)
    db.execute("INSERT INTO events (ts, dt, event, partition) VALUES (1000.0, '1970-01-12 13:46:40.000', 'consumed', 0)")
    db.execute("INSERT INTO events (ts, dt, event, partition) VALUES (1001.0, '1970-01-12 13:50:01.000', 'task_completed', 0)")
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
    import os
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
                max_workers, task_timeout_seconds, max_retries, window_size,
                sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, 'main', '10.0.0.1', 8080, NULL,
                       'k:9092', 't', 'g', '/bin/x', 4, 60, 2, 5, '{}', '{}', 1000.0, '1970-01-01 00:16:40.000')""",
            [name],
        )
        db.executescript(SCHEMA_EVENTS)
        db.execute("INSERT INTO events (ts, dt, event, partition) VALUES (1000.0, '1970-01-12 13:46:40.000', 'consumed', 0)")
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
    # merged file should exist
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
