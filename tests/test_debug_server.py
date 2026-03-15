"""Tests for Drakkar debug web UI."""

import time
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DebugConfig, DrakkarConfig, ExecutorConfig, MetricsConfig
from drakkar.debug_server import create_debug_app
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
            'partition': 0, 'last_consumed': time.time() - 10,
            'last_committed': time.time() - 15, 'last_committed_offset': 100,
            'consumed_count': 50, 'completed_count': 45, 'failed_count': 1,
        },
        {
            'partition': 1, 'last_consumed': time.time() - 5,
            'last_committed': time.time() - 8, 'last_committed_offset': 200,
            'consumed_count': 30, 'completed_count': 30, 'failed_count': 0,
        },
    ]
    rec.get_events.return_value = [
        {
            'id': 1, 'ts': time.time() - 60, 'event': 'consumed',
            'partition': 0, 'offset': 42, 'task_id': None,
            'args': None, 'stdout_size': 0, 'stdout': None,
            'stderr': None, 'exit_code': None, 'duration': None,
            'output_topic': None, 'metadata': None,
        },
        {
            'id': 2, 'ts': time.time() - 55, 'event': 'task_completed',
            'partition': 0, 'offset': None, 'task_id': 't-42',
            'args': '["--input", "f.txt"]', 'stdout_size': 1024,
            'stdout': None, 'stderr': None, 'exit_code': 0,
            'duration': 1.5, 'output_topic': None, 'metadata': None,
        },
    ]
    rec.get_trace.return_value = rec.get_events.return_value
    rec.get_active_tasks.return_value = []
    return rec


@pytest.fixture
def mock_app():
    app = MagicMock()
    app._worker_id = "test-worker"
    app._start_time = time.monotonic() - 120
    app.processors = {}

    pool = MagicMock()
    pool.active_count = 2
    pool.max_workers = 8
    app._executor_pool = pool
    return app


@pytest.fixture
def debug_config():
    return DebugConfig(enabled=True, port=8080, db_path="/tmp/test.db")


@pytest.fixture
async def client(debug_config, mock_recorder, mock_app):
    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_dashboard_returns_200(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "Drakkar" in resp.text
    assert "test-worker" in resp.text


async def test_dashboard_shows_stats(client):
    resp = await client.get("/")
    assert "42" in resp.text  # total_events
    assert "20" in resp.text  # consumed
    assert "2 / 8" in resp.text  # pool usage


async def test_partitions_page(client):
    resp = await client.get("/partitions")
    assert resp.status_code == 200
    assert "Partitions" in resp.text


async def test_partition_detail_page(client):
    resp = await client.get("/partitions/0")
    assert resp.status_code == 200
    assert "Partition 0" in resp.text
    assert "consumed" in resp.text


async def test_partition_detail_pagination(client):
    resp = await client.get("/partitions/0?page=1")
    assert resp.status_code == 200


async def test_executors_page(client):
    resp = await client.get("/executors")
    assert resp.status_code == 200
    assert "Active Executors" in resp.text
    assert "2 / 8" in resp.text


async def test_trace_page(client):
    resp = await client.get("/trace/0/42")
    assert resp.status_code == 200
    assert "Message Trace" in resp.text
    assert "partition=0" in resp.text
    assert "offset=42" in resp.text


async def test_history_page(client):
    resp = await client.get("/history")
    assert resp.status_code == 200
    assert "Event History" in resp.text


async def test_history_with_filters(client):
    resp = await client.get("/history?partition=0&event_type=consumed")
    assert resp.status_code == 200


async def test_history_pagination(client):
    resp = await client.get("/history?page=2")
    assert resp.status_code == 200


async def test_dashboard_no_partitions(client, mock_recorder):
    mock_recorder.get_stats.return_value = {'total_events': 0}
    resp = await client.get("/")
    assert resp.status_code == 200


async def test_partitions_page_with_live_processors(debug_config, mock_recorder, mock_app):
    """Partitions page enriches data with live processor state."""
    proc = MagicMock()
    proc.queue_size = 5
    proc.offset_tracker.pending_count = 3
    mock_app.processors = {0: proc}

    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.get("/partitions")
    assert resp.status_code == 200


async def test_executors_page_with_live_tasks(debug_config, mock_recorder, mock_app):
    """Executors page shows live in-memory tasks from processors."""
    proc = MagicMock()
    proc.partition_id = 0
    task = MagicMock()
    task.args = ["--test"]
    task.source_offsets = [10]
    proc._pending_tasks = {"live-t1": task}
    mock_app.processors = {0: proc}

    fastapi_app = create_debug_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.get("/executors")
    assert resp.status_code == 200
    assert "live-t1" in resp.text
