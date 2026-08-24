"""Tests for Drakkar debug web UI."""

import asyncio
import json
import os
import re
import time
import typing
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import quote

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel
from starlette.testclient import TestClient

from drakkar.config import DrakkarConfig, UITimelineConfig
from drakkar.recorder import EventRecorder
from drakkar.recorder.archive import archive_file_name
from drakkar.uiserver import routes_live
from drakkar.uiserver.server import (
    UIDeps,
    create_ui_app,
    format_ts,
    format_ts_full,
    format_ts_ms,
    format_uptime,
    origin_allowed,
    worker_group,
)
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_recorder():
    rec = AsyncMock(spec=EventRecorder)
    # Default the connection attrs to None so endpoints that check
    # ``recorder.reader_db or recorder._db`` see a clean absence rather
    # than an auto-generated MagicMock truthy value. Tests that exercise
    # real DB queries set both explicitly.
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
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
    return rec


@pytest.fixture
def mock_app():
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 120
    app.processors = {}
    app._config = DrakkarConfig()
    # UI hosting defaults ON and resolves against the real user cache /
    # GitHub at UIServer.start(); tests must stay hermetic.
    app._config.ui.release.enabled = False

    pool = MagicMock()
    pool.active_count = 2
    pool.waiting_count = 0
    pool.max_executors = 8
    # Real set, not a MagicMock: the live overview iterates it to split
    # in-flight tasks into running vs pending.
    pool.running_task_ids = set()
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
    return make_ui_config(enabled=True, port=8080, db_dir='/tmp')


@pytest.fixture
async def client(debug_config, mock_recorder, mock_app):
    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


# ---------------------------------------------------------------------------
# 1. Pure utility functions: format_ts
# ---------------------------------------------------------------------------


class TestFormatTs:
    def test_format_ts_none_returns_empty(self):
        assert format_ts(None) == ''

    def test_format_ts_returns_hms(self):
        result = format_ts(1000.0)
        assert result != ''
        assert re.match(r'\d{2}:\d{2}:\d{2}$', result)

    def test_format_ts_ms_none_returns_empty(self):
        assert format_ts_ms(None) == ''

    def test_format_ts_ms_returns_hms_millis(self):
        result = format_ts_ms(1000.0)
        assert result != ''
        assert re.match(r'\d{2}:\d{2}:\d{2}\.\d{3}$', result)

    def test_format_ts_full_none_returns_empty(self):
        assert format_ts_full(None) == ''

    def test_format_ts_full_returns_datetime_millis(self):
        result = format_ts_full(1000.0)
        assert result != ''
        assert re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$', result)


class TestFormatUptime:
    def test_seconds(self):
        assert format_uptime(0) == '0s'
        assert format_uptime(45) == '45s'
        assert format_uptime(59) == '59s'

    def test_minutes(self):
        assert format_uptime(60) == '1m 0s'
        assert format_uptime(90) == '1m 30s'
        assert format_uptime(3599) == '59m 59s'

    def test_hours(self):
        assert format_uptime(3600) == '1h 0m'
        assert format_uptime(7200 + 1800) == '2h 30m'
        assert format_uptime(86399) == '23h 59m'

    def test_days(self):
        assert format_uptime(86400) == '1d 0h'
        assert format_uptime(86400 * 3 + 3600 * 5) == '3d 5h'
        assert format_uptime(86400 * 29 + 3600 * 23) == '29d 23h'

    def test_months(self):
        assert format_uptime(86400 * 30) == '1mo 0d'
        assert format_uptime(86400 * 75) == '2mo 15d'
        assert format_uptime(86400 * 364) == '12mo 4d'

    def test_years(self):
        assert format_uptime(86400 * 365) == '1y 0mo'
        assert format_uptime(86400 * 365 * 2 + 86400 * 90) == '2y 3mo'

    def test_fractional_seconds_truncated(self):
        assert format_uptime(45.7) == '45s'
        assert format_uptime(90.999) == '1m 30s'


# ---------------------------------------------------------------------------
# 1b. Pure utility functions: worker_group
# ---------------------------------------------------------------------------


class TestWorkerGroup:
    def test_strips_trailing_number_with_dash(self):
        assert worker_group('worker-1') == 'worker'

    def test_compound_name_with_trailing_number(self):
        assert worker_group('worker-vip-2') == 'worker-vip'

    def test_multi_digit_trailing_number(self):
        assert worker_group('slow-worker-05') == 'slow-worker'

    def test_number_without_separator(self):
        assert worker_group('worker15') == 'worker'

    def test_no_trailing_number(self):
        assert worker_group('single') == 'single'

    def test_strips_trailing_number_multiple(self):
        assert worker_group('worker-3') == 'worker'
        assert worker_group('worker-15') == 'worker'

    def test_preserves_middle_numbers(self):
        assert worker_group('worker-vip-1') == 'worker-vip'
        assert worker_group('worker-vip-2') == 'worker-vip'

    def test_underscore_separator(self):
        assert worker_group('slow_worker_05') == 'slow_worker'

    def test_no_trailing_number_compound(self):
        assert worker_group('worker-vip') == 'worker-vip'
        assert worker_group('special') == 'special'

    def test_only_digits_returns_itself(self):
        assert worker_group('123') == '123'


# ---------------------------------------------------------------------------
# 2. _build_prometheus_links (accessed via create_ui_app internals)
# ---------------------------------------------------------------------------


class TestBuildPrometheusLinks:
    async def test_empty_prometheus_url_returns_empty_dicts(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', prometheus_url='')
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)

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
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='cluster="test"',
        )
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        # prometheus graph links should be in the rendered HTML
        assert 'http://prom:9090/graph' in resp.text

    async def test_prometheus_links_card_keys(self, mock_recorder, mock_app):
        """Verify _build_prometheus_links returns expected card/worker/cluster keys."""
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='cluster="prod"',
        )
        # Access _build_prometheus_links by extracting it from the closure.
        # We do this by creating the app and finding the inner function.
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)

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
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_rate_interval='5m',
            prometheus_cluster_label='',
        )
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        # Contract v1.12 fields.
        assert data[0]['kind'] == 'recorder'
        assert data[0]['live_for'] == ''
        assert data[0]['stats_pending'] is False
        assert data[0]['cache_entry_count'] is None

    async def test_live_symlink_marks_in_use_and_cache_db_is_typed(self, tmp_path, mock_recorder, mock_app):
        import sqlite3

        from drakkar.recorder import SCHEMA_EVENTS

        current = tmp_path / 'worker-1-2026-03-24__11_00_00.db'
        db = sqlite3.connect(str(current))
        db.executescript(SCHEMA_EVENTS)
        db.commit()
        db.close()
        os.symlink(str(current), str(tmp_path / 'worker-1-live.db'))

        cache_target = tmp_path / 'worker-1-cache.db.actual'
        db = sqlite3.connect(str(cache_target))
        db.execute('CREATE TABLE cache_entries (key TEXT PRIMARY KEY, value TEXT)')
        db.execute("INSERT INTO cache_entries VALUES ('k', 'v')")
        db.commit()
        db.close()
        os.symlink(str(cache_target), str(tmp_path / 'worker-1-cache.db'))

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases')

        rows = {r['filename']: r for r in resp.json()}
        # The recorder DB currently written by worker-1 is highlighted.
        assert rows[current.name]['live_for'] == 'worker-1'
        assert rows[current.name]['kind'] == 'recorder'
        # The cache DB appears under its stable symlink name, typed, and
        # in-use because its worker's recorder is live in the same dir.
        assert rows['worker-1-cache.db']['kind'] == 'cache'
        assert rows['worker-1-cache.db']['cache_entry_count'] == 1
        assert rows['worker-1-cache.db']['live_for'] == 'worker-1'
        # The symlinks themselves are not rows.
        assert 'worker-1-live.db' not in rows


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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/test.db')

        assert resp.status_code == 200
        assert resp.content == b'fake-sqlite'

    async def test_download_directory_traversal_blocked(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/../etc/passwd')
        assert resp.status_code in (400, 404)

    async def test_download_nonexistent_file(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/nonexistent.db')
        assert resp.status_code == 404


class TestMemoryOnlyRecorderServesNoFiles:
    """``ui.recorder.db_dir: ""`` is the documented memory-only mode.

    With it, ``os.path.join('', name)`` is just ``name`` and
    ``os.path.realpath('')`` is the worker's CWD, so the old
    prefix-string containment check passed for **any** regular file in the
    directory the worker was started in — including its own
    ``drakkar.yaml`` with SASL passwords and sink DSNs. The UI is
    unauthenticated by design, so that was reachable by anyone who could
    reach the port.

    There is no database directory in this mode, so these endpoints have
    nothing legitimate to serve and must say so.
    """

    @staticmethod
    def _app_in(cwd, monkeypatch, mock_recorder, mock_app):
        monkeypatch.chdir(cwd)
        cfg = make_ui_config(enabled=True, port=8080, db_dir='')
        return ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))

    async def test_download_cannot_read_a_file_from_the_working_directory(
        self, tmp_path, monkeypatch, mock_recorder, mock_app
    ):
        (tmp_path / 'drakkar.yaml').write_text('kafka:\n  brokers: secret:9092\n')
        transport = self._app_in(tmp_path, monkeypatch, mock_recorder, mock_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/drakkar.yaml')
        assert resp.status_code == 404
        assert b'secret:9092' not in resp.content

    async def test_download_of_a_plain_db_name_is_also_refused(self, tmp_path, monkeypatch, mock_recorder, mock_app):
        (tmp_path / 'w1.db').write_bytes(b'fake-sqlite')
        transport = self._app_in(tmp_path, monkeypatch, mock_recorder, mock_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/w1.db')
        assert resp.status_code == 404

    async def test_merge_refuses_to_write_into_the_working_directory(
        self, tmp_path, monkeypatch, mock_recorder, mock_app
    ):
        """The write half of the same hole: merge would have created its
        output next to whatever the worker was started beside."""
        for name in ('a.db', 'b.db'):
            (tmp_path / name).write_bytes(b'fake-sqlite')
        transport = self._app_in(tmp_path, monkeypatch, mock_recorder, mock_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/merge', json={'filenames': ['a.db', 'b.db']})
        assert resp.status_code == 404
        assert not list(tmp_path.glob('merged-*.db'))

    async def test_archive_download_is_refused(self, tmp_path, monkeypatch, mock_recorder, mock_app):
        transport = self._app_in(tmp_path, monkeypatch, mock_recorder, mock_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives/drakkar-c1-20260823__10_00_00-20260823__11_00_00.db.gz')
        assert resp.status_code == 404


class TestDownloadContainment:
    """A resolved path must stay inside db_dir, whatever the name looks like."""

    async def test_symlink_escaping_db_dir_is_refused(self, tmp_path, mock_recorder, mock_app):
        outside = tmp_path / 'outside'
        outside.mkdir()
        (outside / 'secrets.db').write_bytes(b'not-yours')
        db_dir = tmp_path / 'dbs'
        db_dir.mkdir()
        os.symlink(outside / 'secrets.db', db_dir / 'escape.db')

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(db_dir))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/escape.db')
        assert resp.status_code in (400, 404)
        assert b'not-yours' not in resp.content


class TestDownloadServesRecorderFilesOnly:
    """The endpoint exists to hand over recorder databases, nothing else.

    ``db_dir`` defaults to ``/tmp``, which is shared with every other
    program on the host, and the debug UI is unauthenticated by design.
    Checking only for separators and header-injection characters meant any
    readable file in that directory could be fetched by exact name.
    """

    @pytest.mark.parametrize(
        'name',
        [
            'other-app-export.json',
            'id_rsa',
            'notes.txt',
            'archive.db.gz',  # archives have their own route + regex
            'backup.db.bak',
            'w1.dbx',
        ],
    )
    async def test_non_database_names_are_refused(self, tmp_path, mock_recorder, mock_app, name):
        (tmp_path / name).write_bytes(b'not-a-recorder-file')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/' + quote(name, safe=''))
        assert resp.status_code in (400, 404), name
        assert b'not-a-recorder-file' not in resp.content

    @pytest.mark.parametrize(
        'name',
        [
            'worker-1-2026-03-16__14_55_00.db',  # make_db_path
            'worker-1-live.db',  # live symlink
            'worker-1-cache.db',  # handler-cache symlink
            'merged-2026-03-16__14_55_00.db',  # merge output
            'w1.db',
        ],
    )
    async def test_recorder_database_names_are_served(self, tmp_path, mock_recorder, mock_app, name):
        (tmp_path / name).write_bytes(b'fake-sqlite')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/' + quote(name, safe=''))
        assert resp.status_code == 200, name
        assert resp.content == b'fake-sqlite'

    async def test_merge_sources_are_restricted_the_same_way(self, tmp_path, mock_recorder, mock_app):
        """Merge reads its sources through the same gate — otherwise the
        contents of an arbitrary file could be pulled into a merged DB and
        downloaded from there."""
        (tmp_path / 'a.db').write_bytes(b'fake-sqlite')
        (tmp_path / 'secrets.json').write_bytes(b'{}')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/merge', json={'filenames': ['a.db', 'secrets.json']})
        assert resp.status_code == 400
        assert 'secrets.json' in resp.json()['error']


# ---------------------------------------------------------------------------
# 4b. Archive list + download endpoints
# ---------------------------------------------------------------------------


class TestDebugArchivesList:
    async def test_list_empty_db_dir(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives')
        assert resp.status_code == 200
        assert resp.json() == {'archives': []}

    async def test_list_happy_path_shape_and_sort_order(self, tmp_path, mock_recorder, mock_app):
        day0 = datetime(2026, 8, 6, 0, 0, 0, tzinfo=UTC).timestamp()
        day1 = day0 + 86400
        day2 = day0 + 2 * 86400
        older_name = archive_file_name('search-fleet', day0, day1)
        newer_name = archive_file_name('search-fleet', day1, day2)
        (tmp_path / older_name).write_bytes(b'a' * 11)
        (tmp_path / newer_name).write_bytes(b'b' * 22)

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives')

        assert resp.status_code == 200
        body = resp.json()
        assert list(body.keys()) == ['archives']
        assert body['archives'] == [
            {
                'name': newer_name,
                'cluster': 'search-fleet',
                'from_ts': day1,
                'to_ts': day2,
                'size_bytes': 22,
            },
            {
                'name': older_name,
                'cluster': 'search-fleet',
                'from_ts': day0,
                'to_ts': day1,
                'size_bytes': 11,
            },
        ]

    async def test_list_excludes_dot_temp_unreadable_and_raw_db(self, tmp_path, mock_recorder, mock_app):
        end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
        start = end - 86400
        real_name = archive_file_name('search-fleet', start, end)
        (tmp_path / real_name).write_bytes(b'real')
        # in-flight compress temp for the same window
        (tmp_path / f'.{real_name}.4242.tmp').write_bytes(b'partial')
        # quarantined raw source, never matches the archive suffix
        (tmp_path / 'worker-1-2026-08-08__00_00_00.db.unreadable').write_bytes(b'bad')
        # a plain raw recorder db, never archived
        (tmp_path / 'worker-1-2026-08-08__00_00_00.db').write_bytes(b'raw')

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives')

        assert resp.status_code == 200
        names = [entry['name'] for entry in resp.json()['archives']]
        assert names == [real_name]


class TestDebugArchivesDownload:
    async def test_download_happy_path_round_trips_bytes(self, tmp_path, mock_recorder, mock_app):
        end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
        name = archive_file_name('search-fleet', end - 86400, end)
        (tmp_path / name).write_bytes(b'gzip-bytes')

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get(f'/api/debug/archives/{name}')

        assert resp.status_code == 200
        assert resp.content == b'gzip-bytes'
        assert resp.headers['content-type'] == 'application/gzip'
        assert 'attachment' in resp.headers['content-disposition']

    async def test_download_bad_name_returns_404(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives/not-a-valid-archive-name.db.gz')
        assert resp.status_code == 404

    async def test_download_traversal_attempt_returns_404(self, tmp_path, mock_recorder, mock_app):
        outside = tmp_path / 'outside'
        outside.mkdir()
        secret = outside / 'x.db.gz'
        secret.write_bytes(b'secret')
        db_dir = tmp_path / 'db'
        db_dir.mkdir()

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(db_dir))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives/../db.gz')
        assert resp.status_code == 404

    async def test_download_missing_file_returns_404(self, tmp_path, mock_recorder, mock_app):
        end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
        name = archive_file_name('search-fleet', end - 86400, end)

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get(f'/api/debug/archives/{name}')
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


@pytest.mark.parametrize('path', ['/', '/partitions', '/sinks', '/live', '/history', '/debug'])
@pytest.mark.parametrize('helper', ['pausableInterval', 'visibilityGate'])
async def test_shared_helper_is_defined_before_it_is_called(client, path, helper):
    """The shared visibility helpers must precede every call site in the markup.

    Page scripts live in the ``content`` block, which renders BEFORE the
    script block at the end of ``<body>``. A helper defined down there is
    undefined when a page calls it, and the page's whole init IIFE dies. The
    helpers therefore live in ``<head>``; this test pins that placement.
    """
    html = (await client.get(path)).text
    definition = html.find(f'function {helper}(')
    calls = [m.start() for m in re.finditer(rf'(?<!function )\b{helper}\(', html)]
    if not calls:
        pytest.skip(f'{path} does not use {helper}')
    assert definition != -1, f'{path} calls {helper} but never defines it'
    early = [c for c in calls if c < definition]
    assert not early, f'{path} calls {helper} at {early} before its definition at {definition}'


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

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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
    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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


def ws_recv_events(ws, count: int = 1) -> list[dict]:
    """Read frames until at least ``count`` events have arrived.

    Each frame is ``{"dropped": N, "events": [...]}`` — the server drains
    whatever is queued into one frame, so how many events land per frame is
    a timing detail. Tests assert on the event sequence, never on where the
    frame boundaries fell.
    """
    out: list[dict] = []
    while len(out) < count:
        out.extend(json.loads(ws.receive_text())['events'])
    return out


def ws_recv_frame(ws) -> dict:
    """Read exactly one frame envelope (for asserting on ``dropped``)."""
    return json.loads(ws.receive_text())


async def test_websocket_receives_events(debug_config, mock_recorder, mock_app):
    """WebSocket endpoint streams recorder events to connected clients."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_ui_app(debug_config, real_recorder, mock_app)

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

        (event,) = ws_recv_events(ws)
        assert event['event'] == 'task_started'
        assert event['task_id'] == 'ws-test-1'
        assert event['partition'] == 3


async def test_websocket_multiple_events(debug_config, mock_recorder, mock_app):
    """WebSocket receives multiple events in order."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_ui_app(debug_config, real_recorder, mock_app)

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

        events = ws_recv_events(ws, 3)
        assert [e['partition'] for e in events] == [0, 1, 2], 'order must be preserved'
        assert all(e['event'] == 'consumed' for e in events)


async def test_websocket_event_filter_limits_the_stream(debug_config, mock_recorder, mock_app):
    """``?events=`` keeps unrendered event types off the wire entirely.

    A page showing only the executor timeline should not receive the
    ``task_complete`` event a handler emits once per task — on a fan-out
    workload that is the largest event class by count.
    """
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_ui_app(debug_config, real_recorder, mock_app)

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws?events=task_started,arranged') as ws:
        real_recorder._record({'ts': time.time(), 'event': 'task_complete', 'partition': 0})
        real_recorder._record({'ts': time.time(), 'event': 'produced', 'partition': 0})
        real_recorder._record({'ts': time.time(), 'event': 'task_started', 'partition': 7})

        # Only the allowed type arrives; the filtered ones never queued, so
        # the first frame carries task_started and nothing before it.
        (event,) = ws_recv_events(ws)
        assert event['event'] == 'task_started'
        assert event['partition'] == 7


async def test_websocket_reports_dropped_events(debug_config, mock_recorder, mock_app):
    """A client that fell behind is told, so it can resync rather than drift.

    Silent drops are why live counts could disagree with the database with
    no visible cause.
    """
    import queue as queue_mod

    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_ui_app(debug_config, real_recorder, mock_app)

    with TestClient(fastapi_app) as tc, tc.websocket_connect('/ws') as ws:
        # Shrink the connected subscriber's queue so overflow is deterministic
        # rather than a matter of how fast the sender coroutine drains.
        (sub,) = list(real_recorder._ws_subscribers)
        sub.queue = queue_mod.Queue(maxsize=1)

        real_recorder._record({'ts': time.time(), 'event': 'kept', 'partition': 0})
        real_recorder._record({'ts': time.time(), 'event': 'lost', 'partition': 0})
        real_recorder._record({'ts': time.time(), 'event': 'lost', 'partition': 0})

        frame = ws_recv_frame(ws)
        assert frame['dropped'] == 2
        assert [e['event'] for e in frame['events']] == ['kept']


async def test_websocket_cleanup_on_disconnect(debug_config, mock_recorder, mock_app):
    """Subscriber queue is removed when WebSocket disconnects."""
    real_recorder = EventRecorder(debug_config)
    real_recorder._running = True

    fastapi_app = create_ui_app(debug_config, real_recorder, mock_app)

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

    cfg = make_ui_config(enabled=True, db_dir=str(tmp_path), flush_interval_seconds=60)
    rec = EventRecorder(cfg, worker_name='test-arrange-tasks')
    await rec.start()
    return rec


async def test_arrange_tasks_empty_ids_returns_empty_map(mock_recorder, mock_app, debug_config):
    """POST with no task_ids → empty map, no recorder flush/query."""
    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        # Record consumed first, then message_complete. The recorder stamps
        # ``ts`` with time.time(), so inject deterministic timestamps and
        # assert the exact difference — no wall-clock sleeps.
        from drakkar.models import SourceMessage

        msg = SourceMessage(topic='t', partition=3, offset=100, value=b'{}', timestamp=1000)
        with patch('time.time', return_value=1000.0):
            rec.record_consumed(msg)
        with patch('time.time', return_value=1000.25):
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results')
        body = resp.json()
        assert len(body) == 1
        row = body[0]
        # End-to-end = message_complete.ts - consumed.ts = exactly 0.25s.
        assert row['end_to_end_duration'] == pytest.approx(0.25)
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
        from drakkar.models import SourceMessage

        msg = SourceMessage(topic='t', partition=3, offset=100, value=b'{}', timestamp=1000)
        # Deterministic injected timestamps: old consumed at t=1000 (a
        # previous replay), the real consumed at t=1005, and the
        # message_complete at t=1005.25.
        with patch('time.time', return_value=1000.0):
            rec.record_consumed(msg)
        with patch('time.time', return_value=1005.0):
            rec.record_consumed(msg)
        with patch('time.time', return_value=1005.25):
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/live/message-results')
        body = resp.json()
        row = body[0]
        # If pairing picked the oldest consumed it would report 5.25s.
        # Picking the most-recent-prior consumed gives exactly 0.25s.
        assert row['end_to_end_duration'] == pytest.approx(0.25)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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

        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
        fastapi_app = create_ui_app(debug_config, rec, mock_app)
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
    from drakkar.handler import BaseDrakkarHandler
    from drakkar.uiserver.server import hook_flags

    class H(BaseDrakkarHandler):
        pass

    flags = hook_flags(H())
    assert flags == {
        'task_complete': False,
        'message_complete': False,
        'window_complete': False,
    }


def test_hook_flags_detects_overrides():
    """Each overridden hook flips its flag; non-overridden stay False."""
    from drakkar.handler import BaseDrakkarHandler
    from drakkar.models import CollectResult, ExecutorResult, MessageGroup
    from drakkar.uiserver.server import hook_flags

    class H(BaseDrakkarHandler):
        async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
            return None

        async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
            return None

        # on_window_complete NOT overridden — should stay False.

    flags = hook_flags(H())
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


async def test_api_workers_current_worker_always_online(client, mock_recorder):
    """The current worker reports online=True with last_seen_ts = now."""
    mock_recorder.discover_workers.return_value = []
    before = time.time()
    resp = await client.get('/api/workers')
    after = time.time()
    me = resp.json()[0]
    assert me['is_current'] is True
    assert me['online'] is True
    assert before <= me['last_seen_ts'] <= after


async def test_api_workers_passes_through_peer_liveness_fields(client, mock_recorder):
    """Peer last_seen_ts/online from discover_workers reach the response unchanged."""
    mock_recorder.discover_workers.return_value = [
        {
            'worker_name': 'w-crashed',
            'ip_address': '10.0.0.2',
            'debug_port': 8080,
            'last_seen_ts': None,
            'online': False,
        },
        {
            'worker_name': 'w-alive',
            'ip_address': '10.0.0.3',
            'debug_port': 8080,
            'last_seen_ts': 1234.5,
            'online': True,
        },
    ]
    resp = await client.get('/api/workers')
    data = {w['worker_name']: w for w in resp.json()}
    assert data['w-crashed']['online'] is False
    assert data['w-crashed']['last_seen_ts'] is None
    assert data['w-alive']['online'] is True
    assert data['w-alive']['last_seen_ts'] == 1234.5


# --- Debug databases page and API ---


@pytest.fixture
async def debug_client(tmp_path, mock_recorder, mock_app):
    """Client with a real db_dir for debug database endpoints."""
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


async def test_debug_page_returns_200(debug_client):
    resp = await debug_client.get('/debug')
    assert resp.status_code == 200
    assert 'Message Trace' in resp.text
    assert 'Databases' in resp.text


async def test_api_debug_databases_skips_symlinks(tmp_path, mock_recorder, mock_app):
    import sqlite3

    from drakkar.recorder import SCHEMA_EVENTS

    db_path = tmp_path / 'w1.db'
    db = sqlite3.connect(str(db_path))
    db.executescript(SCHEMA_EVENTS)
    db.commit()
    db.close()
    os.symlink('w1.db', str(tmp_path / 'w1-live.db'))

    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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


# ---------------------------------------------------------------------------
# 5. Additional coverage tests
# ---------------------------------------------------------------------------


class TestGetSinkUiLinksEmpty:
    """Cover _get_sink_ui_links when sink_manager is falsy (line 84)."""

    async def test_no_sink_manager_returns_no_links(self, debug_config, mock_recorder, mock_app):
        mock_app.sink_manager = None
        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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
        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200


class TestDashboardCustomLinks:
    """Cover custom_links template expansion (lines 268-278)."""

    async def test_custom_links_rendered(self, mock_recorder, mock_app):
        cfg = make_ui_config(
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

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'http://grafana/worker-7' in resp.text
        assert 'http://logs/prod' in resp.text


class TestLivePageWithTasks:
    """Cover live page task processing (lines 350, 357-367, 383-384)."""

    async def test_live_page_with_active_and_pending_tasks(self, debug_config, mock_recorder, mock_app):
        pending_task = MagicMock()
        pending_task.args = '["--fast"]'
        pending_task.source_offsets = [10, 11]

        proc = MagicMock()
        proc.partition_id = 0
        proc._pending_tasks = {'task-active-1': pending_task, 'task-pending-1': pending_task}
        proc._arranging = False
        proc._active_tasks = []

        mock_app.processors = {0: proc}
        mock_app._executor_pool.running_task_ids = {'task-active-1'}

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/live')
        assert resp.status_code == 200
        assert 'Live Pipeline' in resp.text

    async def test_live_page_with_arranging_processor(self, debug_config, mock_recorder, mock_app):
        now = time.time()

        proc = MagicMock()
        proc.partition_id = 0
        proc._pending_tasks = {}
        proc._arranging = True
        proc._arrange_start = now - 2.5
        proc._arrange_labels = ['label-a', 'label-b']
        proc._active_tasks = []

        mock_app.processors = {0: proc}

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-abc')
        assert resp.status_code == 200
        assert 'task-abc' in resp.text
        assert '5.0' in resp.text or '5.00' in resp.text

    async def test_task_detail_retry_key_strips_suffix(self, debug_config, mock_recorder, mock_app):
        mock_recorder.get_task_events.return_value = []

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/task/task-dur')
        assert resp.status_code == 200


class TestMergeEndpointDotPrefixed:
    """Cover merge endpoint rejecting dot-prefixed filenames (line 579)."""

    async def test_merge_rejects_dot_prefixed_filename(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(db_dir))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/download/escape.db')
        assert resp.status_code == 400


class TestDownloadDotPrefixed:
    """Cover download endpoint blocking dot-prefixed files (line 620)."""

    async def test_download_dot_prefixed_blocked(self, tmp_path, mock_recorder, mock_app):
        hidden = tmp_path / '.secret.db'
        hidden.write_bytes(b'secret')

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))

        # Same connection for read and write paths in this unit-test
        # shim — the dedicated reader connection that the real recorder
        # opens is not needed here because there's no writer contention.
        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        mock_recorder._reader_db = None
        mock_recorder.reader_db = None
        mock_recorder.flush = AsyncMock()

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid, stdout_size) '
            "VALUES (?, ?, 'task_completed', 0, 'task-1', 1.5, 100, 2048)",
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        # The debug_server calls recorder.flush() (public API) and
        # recorder.config (public property) — both expose formerly-private
        # attributes. Stub the public names directly so the specced mock
        # returns the real config instead of a MagicMock (which breaks when
        # the endpoint reads recorder.config.ws_min_duration_ms).
        mock_recorder.flush = AsyncMock()
        mock_recorder.config = cfg
        mock_recorder._buffer = []
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

    async def test_recent_tasks_carries_cost_and_speed_from_metadata(self, tmp_path, mock_recorder, mock_app):
        """Contract v1.16: throughput-counted completions surface cost/speed
        on the row; uncounted ones (task-1 here) carry no keys at all."""
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        now = time.time()
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'task-5', '[]', 500)",
            (now - 6, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid, metadata) '
            "VALUES (?, ?, 'task_completed', 0, 'task-5', 2.0, 500, ?)",
            (now - 4, '2026-04-02', json.dumps({'cost': 8388608.0, 'speed': 4194304.0})),
        )
        await db.commit()

        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')

        tasks_by_id = {t['task_id']: t for t in resp.json()['tasks']}
        assert tasks_by_id['task-5']['cost'] == 8388608.0
        assert tasks_by_id['task-5']['speed'] == 4194304.0
        assert 'speed' not in tasks_by_id['task-1']
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

    async def test_recent_tasks_no_db_returns_empty_flagged_payload(self, debug_config, mock_recorder, mock_app):
        """No reader connection is a degraded read, not an empty timeline.

        The response keeps the documented object shape and says so with
        ``unavailable``; a bare ``[]`` broke every client reading
        ``payload.tasks``.
        """
        mock_recorder._db = None
        mock_recorder._reader_db = None
        mock_recorder.reader_db = None
        mock_recorder.flush = AsyncMock()

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/recent-tasks')
        assert resp.status_code == 200
        assert resp.json() == {
            'tasks': [],
            'lane_count': mock_app._executor_pool.max_executors,
            'truncated': False,
            'unavailable': True,
        }

    async def test_recent_tasks_slot_extracted_from_metadata(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-1']['slot'] == 1
        await db.close()

    async def test_recent_tasks_stdout_size_present_on_completion(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-1']['stdout_size'] == 2048
        assert tasks_by_id['task-2']['stdout_size'] is None
        await db.close()

    async def test_recent_tasks_failed_task_stdout_size_is_null(self, tmp_path, mock_recorder, mock_app):
        """A task_failed row's stdout_size column defaults to 0 at the schema level — the
        handler must not surface that as a real byte count."""
        client, db = await self._make_client_with_task_events(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=5')
        data = resp.json()
        tasks_by_id = {t['task_id']: t for t in data['tasks']}
        assert tasks_by_id['task-4']['status'] == 'failed'
        assert tasks_by_id['task-4']['stdout_size'] is None
        await db.close()


class TestApiRecentTasksDepthAndWindow:
    """Cover the ui.timeline-driven `limit`/`minutes` behavior of /api/recent-tasks."""

    async def _make_client(self, tmp_path, mock_recorder, mock_app, *, timeline=None, pool_present=True):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'live.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)

        kwargs = {'enabled': True, 'port': 8080, 'db_dir': str(tmp_path)}
        if timeline is not None:
            kwargs['timeline'] = timeline
        cfg = make_ui_config(**kwargs)

        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder.config = cfg
        mock_recorder._buffer = []
        if not pool_present:
            mock_app._executor_pool = None
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        client = AsyncClient(transport=transport, base_url='http://test')
        return client, db

    async def _seed_sequential_tasks(self, db, count, *, gap=10.0, duration=1.0):
        # Tasks spaced ``gap`` seconds apart, each a clean start+complete
        # pair, so chronological event order matches task order exactly —
        # no interleaving to reason about when a test wants a specific
        # "newest N" subset.
        now = time.time()
        for i in range(1, count + 1):
            start_ts = now - (count - i + 1) * gap
            await db.execute(
                'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
                "VALUES (?, ?, 'task_started', 0, ?, '[]', 100)",
                (start_ts, '2026-04-02', f'task-{i}'),
            )
            await db.execute(
                'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid, stdout_size) '
                "VALUES (?, ?, 'task_completed', 0, ?, ?, 100, ?)",
                (start_ts + duration, '2026-04-02', f'task-{i}', duration, i * 10),
            )
        await db.commit()

    async def test_limit_trims_to_newest_tasks_by_start_ts(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app)
        await self._seed_sequential_tasks(db, 12)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=60&limit=5')
        data = resp.json()
        assert {t['task_id'] for t in data['tasks']} == {f'task-{i}' for i in range(8, 13)}
        assert data['truncated'] is True
        await db.close()

    async def test_limit_not_reached_reports_untruncated(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app)
        await self._seed_sequential_tasks(db, 3)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=60&limit=5')
        data = resp.json()
        assert len(data['tasks']) == 3
        assert data['truncated'] is False
        await db.close()

    async def test_default_limit_pool_absent_falls_back_to_8(self, tmp_path, mock_recorder, mock_app):
        """No executor pool attached: default limit is history_factor x DEFAULT_LANE_COUNT (8)."""
        timeline = UITimelineConfig(history_factor=1)
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app, timeline=timeline, pool_present=False)
        await self._seed_sequential_tasks(db, 10)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=60')
        data = resp.json()
        assert len(data['tasks']) == 8
        assert {t['task_id'] for t in data['tasks']} == {f'task-{i}' for i in range(3, 11)}
        await db.close()

    async def test_default_limit_uses_pool_max_executors_when_present(self, tmp_path, mock_recorder, mock_app):
        """Pool attached: default limit is history_factor x pool.max_executors, distinct from the
        pool-absent fallback of 8 — uses max_executors=3 so the two paths can't be confused."""
        timeline = UITimelineConfig(history_factor=2)
        mock_app._executor_pool.max_executors = 3
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app, timeline=timeline)
        await self._seed_sequential_tasks(db, 10)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=60')
        data = resp.json()
        assert len(data['tasks']) == 6
        assert {t['task_id'] for t in data['tasks']} == {f'task-{i}' for i in range(5, 11)}
        await db.close()

    async def test_default_limit_clamped_when_history_factor_is_absurd(
        self, tmp_path, mock_recorder, mock_app, monkeypatch
    ):
        """An unbounded history_factor x pool size must not reach the DB query unclamped."""
        captured: dict[str, object] = {}
        original = UIDeps.flush_and_select

        async def spy(self, query, params=()):
            captured['params'] = params
            return await original(self, query, params)

        monkeypatch.setattr(UIDeps, 'flush_and_select', spy)

        timeline = UITimelineConfig(history_factor=1_000_000)
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app, timeline=timeline, pool_present=False)
        await self._seed_sequential_tasks(db, 3)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=60')
        assert resp.status_code == 200
        # The derived limit (1_000_000 x 8) must have been clamped to
        # RECENT_TASKS_MAX_LIMIT before becoming the query's row cap.
        assert captured['params'][1] == routes_live.RECENT_TASKS_MAX_LIMIT * 3
        data = resp.json()
        assert len(data['tasks']) == 3
        assert data['truncated'] is False
        await db.close()

    async def test_minutes_1440_is_accepted(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=1440')
        assert resp.status_code == 200
        await db.close()

    async def test_minutes_above_1440_rejected(self, tmp_path, mock_recorder, mock_app):
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app)
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=99999')
        assert resp.status_code == 422
        await db.close()

    async def test_minutes_clamped_to_configured_max_age(self, tmp_path, mock_recorder, mock_app):
        timeline = UITimelineConfig(max_age_minutes=60)
        client, db = await self._make_client(tmp_path, mock_recorder, mock_app, timeline=timeline)
        now = time.time()
        # Inside the 60-minute clamp.
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'recent-task', '[]', 100)",
            (now - 10 * 60, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid) '
            "VALUES (?, ?, 'task_completed', 0, 'recent-task', 1.0, 100)",
            (now - 9 * 60, '2026-04-02'),
        )
        # Outside the 60-minute clamp but inside the requested 120-minute window.
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
            "VALUES (?, ?, 'task_started', 0, 'old-task', '[]', 200)",
            (now - 70 * 60, '2026-04-02'),
        )
        await db.execute(
            'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid) '
            "VALUES (?, ?, 'task_completed', 0, 'old-task', 1.0, 200)",
            (now - 69 * 60, '2026-04-02'),
        )
        await db.commit()
        async with client as c:
            resp = await c.get('/api/recent-tasks?minutes=120')
        data = resp.json()
        task_ids = {t['task_id'] for t in data['tasks']}
        assert 'recent-task' in task_ids
        assert 'old-task' not in task_ids
        await db.close()


class TestApiDashboardWithConsumerLag:
    """Cover /api/dashboard with consumer lag (lines 754-757)."""

    async def test_api_dashboard_includes_lag(self, debug_config, mock_recorder, mock_app):
        consumer = AsyncMock()
        consumer.get_total_lag.return_value = 42
        mock_app._consumer = consumer

        proc = MagicMock()
        mock_app.processors = {0: proc}

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug')
        assert resp.status_code == 200
        assert 'worker=test-worker' in resp.text

    async def test_probe_tab_rendered_in_debug_html(self, tmp_path, mock_recorder, mock_app):
        """``GET /debug`` → HTML includes the Message Probe tab button, panel,
        the visible header, the results container id, AND the rendering
        helpers (``renderProbeReport`` + section markers).
        Keeps the UI work honest without depending on a JS test stack — the
        endpoint behavior itself is covered by the probe endpoint tests above.
        """
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        # Rendering helpers must be present so the client can turn
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
        # The right-side task-detail sidebar and its helpers.
        # The sidebar container is rendered once in the probe tab panel.
        assert 'id="probe-task-sidebar"' in html
        # openTaskSidebar is the real (non-stub) implementation — its name
        # is referenced both by the row onclick and by the scroll-to-row
        # helper.
        assert 'openTaskSidebar' in html
        # ESC-close marker: a document-level keydown listener that dismisses
        # the sidebar. The function name is the simplest stable marker.
        assert 'closeTaskSidebar' in html
        # Section helpers and wrapper ids for on_message_complete /
        # on_window_complete / Planned sink outputs. Presence of the
        # function names proves the helpers were injected; the section ids
        # prove ``renderProbeReport`` wires them in.
        assert 'renderProbeSectionOnMessageComplete' in html
        assert 'renderProbeSectionOnWindowComplete' in html
        assert 'renderProbeSectionSinks' in html
        assert 'probe-section-message-complete' in html
        assert 'probe-section-window-complete' in html
        assert 'probe-section-sinks' in html
        # Cache calls / Timeline / Errors sections + the toolbar +
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


class TestUIServerClass:
    """Cover UIServer start/stop (lines 945-977)."""

    async def test_start_creates_server_and_thread(self, debug_config, mock_recorder, mock_app):
        # NOTE: uses a real thread — patching ``threading.Thread`` would also
        # break the worker threads ``asyncio.to_thread`` needs for the
        # bind-readiness wait inside ``start()``.
        from unittest.mock import patch

        from drakkar.uiserver.server import UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)
        assert server._server is None
        assert server._thread is None

        fake_uvicorn = MagicMock()
        fake_uvicorn.started = True  # pretend the bind succeeded instantly
        fake_uvicorn.run = MagicMock()

        with (
            patch('drakkar.uiserver.server.uvicorn.Server', return_value=fake_uvicorn) as mock_uvi_server,
            patch('drakkar.uiserver.server.uvicorn.Config') as mock_uvi_config,
            patch('drakkar.uiserver.server.logger') as mock_logger,
        ):
            mock_uvi_config.return_value = MagicMock()
            mock_logger.ainfo = AsyncMock()

            await server.start()

            mock_uvi_server.assert_called_once()
            assert server._server is fake_uvicorn
            assert server._thread is not None
            assert server._thread.name == 'drakkar-ui-server'
            server._thread.join(timeout=2.0)
            fake_uvicorn.run.assert_called_once()

    async def test_stop_signals_exit_and_joins(self, debug_config, mock_recorder, mock_app):
        from unittest.mock import patch

        from drakkar.uiserver.server import UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)
        server._server = MagicMock()
        server._thread = MagicMock()

        with patch('drakkar.uiserver.server.logger') as mock_logger:
            mock_logger.ainfo = AsyncMock()
            await server.stop()

        assert server._server.should_exit is True
        server._thread.join.assert_called_once_with(timeout=5.0)

    async def test_stop_when_not_started(self, debug_config, mock_recorder, mock_app):
        from unittest.mock import patch

        from drakkar.uiserver.server import UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)

        with patch('drakkar.uiserver.server.logger') as mock_logger:
            mock_logger.ainfo = AsyncMock()
            await server.stop()  # should not raise

    async def test_start_with_dead_thread_raises_bind_error(self, debug_config, mock_recorder, mock_app):
        """A server thread that exits before serving (e.g. port in use) must raise.

        Mirrors the Go backend, where a debug-server bind failure is fatal:
        without the UI server the worker has no probe surface at all.
        """
        from unittest.mock import patch

        from drakkar.uiserver.server import UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)

        # ``run`` returns immediately with ``started`` still False — the
        # shape uvicorn leaves behind after sys.exit(1) on a bind failure.
        fake_uvicorn = MagicMock()
        fake_uvicorn.started = False
        fake_uvicorn.run = lambda: None

        with (
            patch('drakkar.uiserver.server.uvicorn.Server', return_value=fake_uvicorn),
            patch('drakkar.uiserver.server.logger') as mock_logger,
        ):
            mock_logger.ainfo = AsyncMock()
            mock_logger.aerror = AsyncMock()
            with pytest.raises(RuntimeError, match='already in use'):
                await server.start()
            mock_logger.aerror.assert_awaited_once()
            assert mock_logger.aerror.await_args.args[0] == 'ui_server_start_failed'

    async def test_start_returns_once_uvicorn_reports_started(self, debug_config, mock_recorder, mock_app):
        """start() waits for uvicorn's ``started`` flag, then logs success."""
        from unittest.mock import patch

        from drakkar.uiserver.server import UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)

        fake_uvicorn = MagicMock()
        fake_uvicorn.started = False

        def _run() -> None:
            # simulate a successful bind: uvicorn flips ``started`` in-thread
            fake_uvicorn.started = True

        fake_uvicorn.run = _run

        with (
            patch('drakkar.uiserver.server.uvicorn.Server', return_value=fake_uvicorn),
            patch('drakkar.uiserver.server.logger') as mock_logger,
        ):
            mock_logger.ainfo = AsyncMock()
            await server.start()
            events = [call.args[0] for call in mock_logger.ainfo.await_args_list]
            assert 'ui_server_started' in events


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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))

        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            prometheus_url='http://prom:9090',
            prometheus_worker_label='job="drakkar",instance="{worker_id}"',
        )
        mock_app._worker_id = 'worker-42'

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
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
        ('GET', '/api/debug/archives'),
        ('GET', '/api/debug/archives/test-2026-08-08_00-00__2026-08-09_00-00.db.gz'),
    ]

    async def test_protected_routes_require_token(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for method, path in self.PROTECTED_ROUTES:
                resp = await c.request(method, path)
                assert resp.status_code == 401, f'{method} {path} should require auth'

    async def test_protected_routes_accept_bearer_header(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        headers = {'Authorization': 'Bearer secret-123'}
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases', headers=headers)
            assert resp.status_code == 200

    async def test_protected_routes_accept_query_param(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases?token=secret-123')
            assert resp.status_code == 200

    async def test_archives_list_accepts_bearer_header(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives', headers={'Authorization': 'Bearer secret-123'})
            assert resp.status_code == 200

    async def test_archives_list_accepts_query_param(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/archives?token=secret-123')
            assert resp.status_code == 200

    async def test_wrong_token_returns_401(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases', headers={'Authorization': 'Bearer wrong'})
            assert resp.status_code == 401

    async def test_no_auth_when_token_empty(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/databases')
            assert resp.status_code == 200

    async def test_all_ui_routes_require_token_when_configured(self, mock_recorder, mock_app):
        """With auth_token set, every UI/API route is gated — only the
        Kubernetes probes stay public."""
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for path in ['/', '/partitions', '/sinks', '/live', '/history', '/debug']:
                resp = await c.get(path)
                assert resp.status_code == 401, f'{path} should require auth when token is set'
                resp = await c.get(path, headers={'Authorization': 'Bearer secret-123'})
                assert resp.status_code == 200, f'{path} should be accessible with the token'

    async def test_probes_stay_public_with_token_configured(self, mock_recorder, mock_app):
        """/healthz and /readyz must remain unauthenticated — Kubernetes
        probes cannot send bearer tokens."""
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/healthz')
            assert resp.status_code == 200
            resp = await c.get('/readyz')
            assert resp.status_code in (200, 503)  # readiness depends on mock state

    async def test_ui_routes_open_when_no_token_configured(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            for path in ['/', '/partitions', '/sinks', '/live', '/history', '/debug']:
                resp = await c.get(path)
                assert resp.status_code == 200, f'{path} should be open when no token is configured'

    async def test_merge_requires_token(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

    async def test_probe_requires_token(self, mock_recorder, _probe_mock_app):
        """/api/debug/probe is sensitive — no auth → 401, with auth → 200.

        Regression for code review phase 1 iteration 3 issue 1: the
        probe endpoint runs user handlers with a real executor-pool slot
        and reads the real cache when ``use_cache=True``, so it must be
        gated by ``auth_token`` like the other sensitive debug
        endpoints (``/api/debug/merge``, ``/api/debug/databases``,
        ``/debug/download/{filename}``).
        """
        _probe_mock_app.handler = _ProbeTestHandler(task_count=1)
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, _probe_mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            # No auth header → 401.
            resp = await c.post('/api/debug/probe', json={'value': 'x'})
            assert resp.status_code == 401
            # Correct bearer token → 200.
            resp = await c.post(
                '/api/debug/probe',
                json={'value': 'x'},
                headers={'Authorization': 'Bearer secret-123'},
            )
            assert resp.status_code == 200

    async def test_merge_disabled_returns_403_naming_the_config_key(self, tmp_path, mock_recorder, mock_app):
        """``ui.merge_enabled=false`` closes the one UI endpoint that writes files."""
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='', merge_enabled=False)
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/merge', json={'filenames': ['a.db', 'b.db']})
            assert resp.status_code == 403
            assert 'ui.merge_enabled' in resp.json()['error']

    async def test_merge_gate_precedes_body_validation(self, tmp_path, mock_recorder, mock_app):
        """A disabled endpoint answers 403 even for a malformed body.

        Otherwise a caller could distinguish "disabled" from "enabled but
        your JSON was bad", and the request body would be parsed on an
        endpoint the operator has switched off.
        """
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='', merge_enabled=False)
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/merge',
                content=b'not json at all',
                headers={'Content-Type': 'application/json'},
            )
            assert resp.status_code == 403

    async def test_probe_disabled_returns_403_naming_the_config_key(self, mock_recorder, _probe_mock_app):
        """``ui.probe_enabled=false`` closes the endpoint that runs caller bytes."""
        _probe_mock_app.handler = _ProbeTestHandler(task_count=1)
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='', probe_enabled=False)
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, _probe_mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/probe', json={'value': 'x'})
            assert resp.status_code == 403
            assert 'ui.probe_enabled' in resp.json()['error']

    async def test_probe_gate_holds_even_with_a_valid_token(self, mock_recorder, _probe_mock_app):
        """The gate is independent of auth, so it still refuses an authenticated caller.

        This is the point of the switch: an operator who *has* configured a
        token may still want the probe closed, because a valid token is not
        the same as consent to burn executor slots on demand.
        """
        _probe_mock_app.handler = _ProbeTestHandler(task_count=1)
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123', probe_enabled=False)
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, _probe_mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/probe',
                json={'value': 'x'},
                headers={'Authorization': 'Bearer secret-123'},
            )
            assert resp.status_code == 403

    async def test_probe_and_merge_are_enabled_by_default(self, tmp_path, mock_recorder, _probe_mock_app):
        """Defaults preserve behaviour — the gates are opt-in, not opt-out."""
        _probe_mock_app.handler = _ProbeTestHandler(task_count=1)
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='')
        assert cfg.probe_enabled is True
        assert cfg.merge_enabled is True
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, _probe_mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            assert (await c.post('/api/debug/probe', json={'value': 'x'})).status_code == 200
            # Two non-existent files: past the gate, into ordinary validation.
            resp = await c.post('/api/debug/merge', json={'filenames': ['a.db', 'b.db']})
            assert resp.status_code == 404

    async def test_non_ascii_bearer_token_returns_401_not_500(self, mock_recorder, mock_app):
        """Non-ASCII bearer tokens must fail-closed to 401 instead of 500.

        Regression for phase 4 review finding: ``secrets.compare_digest``
        raises ``TypeError: comparing strings with non-ASCII characters is
        not supported`` when either operand contains non-ASCII. Without
        the ``try/except TypeError`` in ``_token_matches``, FastAPI's
        default exception handler would turn that into a 500 — noisy,
        attacker-triggerable, and violating the documented 401 contract.

        httpx refuses to serialize non-ASCII str header values (raises
        ``UnicodeEncodeError`` on the client side), so we pass raw bytes
        — this matches what a hostile HTTP client at the wire level would
        actually send and funnels through starlette's header decoder the
        same way a real request would.
        """
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            # UTF-8 encoded bytes for the non-ASCII o-circumflex (U+00F4).
            # Using bytes bypasses httpx's ASCII-only string check and
            # reaches starlette's header decoder as a raw byte sequence,
            # which then surfaces as a non-ASCII str in ``request.headers``.
            resp = await c.get(
                '/api/debug/databases',
                headers={'Authorization': b'Bearer t\xc3\xb4ken'},
            )
            assert resp.status_code == 401, f'non-ASCII bearer should 401, got {resp.status_code}'

    async def test_non_ascii_configured_token_returns_401_not_500(self, mock_recorder, mock_app):
        """Operator-error path: non-ASCII ``auth_token`` in YAML → clean 401s, not 500s.

        Exercises the other TypeError arm: when the *configured* token
        itself is non-ASCII, every auth check must fail-closed with 401,
        not explode into 500s that fill logs and trip 5xx alerting.
        """
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='non-ascïi')
        mock_recorder.config = cfg
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            # Plain ASCII provided token → compare_digest raises TypeError
            # on the *configured* (non-ASCII) operand; handler must map
            # that to 401 instead of 500.
            resp = await c.get(
                '/api/debug/databases',
                headers={'Authorization': 'Bearer secret-123'},
            )
            assert resp.status_code == 401, f'ASCII provided + bad config → 401, got {resp.status_code}'


# ---------------------------------------------------------------------------
# WebSocket auth + origin validation tests
# ---------------------------------------------------------------------------


class TestWebSocketAuth:
    """Test ``/ws`` authentication and origin validation.

    Uses starlette's ``TestClient`` which is synchronous; ``websocket_connect``
    raises ``WebSocketDisconnect`` when the server closes during handshake
    (or on the first receive after accept). The disconnect exception exposes
    the close code + reason for assertion.
    """

    def test_auth_uses_timing_safe_compare(self, mock_recorder, mock_app, monkeypatch):
        """The auth code path must call ``secrets.compare_digest`` for token check.

        Stronger than a "was it called?" assertion: we capture the exact
        ``(a, b)`` pair passed to ``compare_digest`` and verify both that the
        provided token and the configured token reach it AND that the auth
        code path uses the result (the endpoint returns 200 for a valid
        token and 401 for an invalid one). A buggy implementation that
        called ``compare_digest`` but ignored the return value would fail
        the 401 half of this test.
        """
        import secrets as secrets_mod

        from drakkar.uiserver import server as ds

        captured_calls: list[tuple[str, str]] = []
        real_compare = secrets_mod.compare_digest

        def capturing_compare(a, b):
            # Record the exact argument pair (cast to str so the assertion
            # below doesn't depend on bytes/str layout — compare_digest
            # accepts either, and we only care about operand identity).
            captured_calls.append((a if isinstance(a, str) else a.decode(), b if isinstance(b, str) else b.decode()))
            return real_compare(a, b)

        monkeypatch.setattr(ds.secrets, 'compare_digest', capturing_compare)

        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)

        async def _call():
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                # Positive case: valid token → 200. Bad-token case: 401.
                ok = await c.get('/api/debug/databases', headers={'Authorization': 'Bearer secret-123'})
                bad = await c.get('/api/debug/databases', headers={'Authorization': 'Bearer wrong'})
                return ok.status_code, bad.status_code

        ok_status, bad_status = asyncio.run(_call())

        # The helper must have been called with the provided + configured
        # token pair (order: provided, configured — matches
        # ``_token_matches`` signature).
        assert ('secret-123', 'secret-123') in captured_calls, (
            f'expected (provided, configured) pair in compare_digest calls; got {captured_calls}'
        )
        assert ('wrong', 'secret-123') in captured_calls, (
            f'bad-token attempt should also reach compare_digest; got {captured_calls}'
        )
        # The result must actually gate auth — buggy impl ignoring the
        # return value would still pass both compare_digest checks but
        # fail one of the status assertions.
        assert ok_status == 200, f'valid token should be accepted; got {ok_status}'
        assert bad_status == 401, f'invalid token should be rejected; got {bad_status}'

    def test_ws_without_token_when_auth_required_is_rejected(self, mock_recorder, mock_app):
        """WS connect without token while ``auth_token`` is set is rejected."""
        from starlette.websockets import WebSocketDisconnect

        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with TestClient(fastapi_app) as tc:  # noqa: SIM117
            with pytest.raises(WebSocketDisconnect) as exc_info:
                with tc.websocket_connect('/ws') as ws:
                    # If the server closes during handshake, receive raises.
                    ws.receive_text()
        assert exc_info.value.code == 4401

    def test_ws_wrong_origin_rejected(self, mock_recorder, mock_app):
        """WS with ``Origin`` outside ``allowed_ws_origins`` is closed 4403."""
        from starlette.websockets import WebSocketDisconnect

        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            auth_token='secret-123',
            allowed_ws_origins=['https://ops.internal'],
        )
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with TestClient(fastapi_app) as tc:  # noqa: SIM117
            with pytest.raises(WebSocketDisconnect) as exc_info:
                with tc.websocket_connect(
                    '/ws?token=secret-123',
                    headers={'origin': 'https://evil.com'},
                ) as ws:
                    ws.receive_text()
        assert exc_info.value.code == 4403

    def test_ws_correct_token_and_origin_accepted(self, mock_recorder, mock_app):
        """WS with valid token + allowlisted origin is accepted and streams events."""
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            auth_token='secret-123',
            allowed_ws_origins=['https://ops.internal'],
        )
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with (
            TestClient(fastapi_app) as tc,
            tc.websocket_connect(
                '/ws?token=secret-123',
                headers={'origin': 'https://ops.internal'},
            ) as ws,
        ):
            real_recorder._record({'ts': time.time(), 'event': 'ws_auth_ok', 'partition': 0})
            (event,) = ws_recv_events(ws)
            assert event['event'] == 'ws_auth_ok'

    def test_ws_no_auth_configured_still_works(self, mock_recorder, mock_app):
        """Default dev workflow: ``auth_token=''`` → connection accepted without token."""
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='')
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with (
            TestClient(fastapi_app) as tc,
            tc.websocket_connect('/ws') as ws,
        ):
            real_recorder._record({'ts': time.time(), 'event': 'dev_mode_ok', 'partition': 0})
            (event,) = ws_recv_events(ws)
            assert event['event'] == 'dev_mode_ok'

    def test_ws_same_origin_fallback_accepted(self, mock_recorder, mock_app):
        """With ``auth_token`` set + empty allowlist, Origin's host must equal Host."""
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            auth_token='secret-123',
            allowed_ws_origins=[],
        )
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        # TestClient uses ``testserver`` as the Host by default. Match it in
        # the Origin header to satisfy the same-origin fallback.
        with (
            TestClient(fastapi_app) as tc,
            tc.websocket_connect(
                '/ws?token=secret-123',
                headers={'origin': 'http://testserver'},
            ) as ws,
        ):
            real_recorder._record({'ts': time.time(), 'event': 'same_origin_ok', 'partition': 0})
            (event,) = ws_recv_events(ws)
            assert event['event'] == 'same_origin_ok'

    def test_ws_same_origin_fallback_rejects_cross_origin(self, mock_recorder, mock_app):
        """Empty allowlist + mismatched Origin Host → 4403."""
        from starlette.websockets import WebSocketDisconnect

        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            auth_token='secret-123',
            allowed_ws_origins=[],
        )
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with TestClient(fastapi_app) as tc:  # noqa: SIM117
            with pytest.raises(WebSocketDisconnect) as exc_info:
                with tc.websocket_connect(
                    '/ws?token=secret-123',
                    headers={'origin': 'https://evil.com'},
                ) as ws:
                    ws.receive_text()
        assert exc_info.value.code == 4403

    def test_ws_non_ascii_token_closes_with_4401(self, mock_recorder, mock_app):
        """Non-ASCII token on WS handshake closes with 4401, not a server error.

        Regression for phase 4 review finding: ``secrets.compare_digest``
        raises ``TypeError`` on non-ASCII operands. In the WS endpoint the
        auth check runs *before* ``ws.close(code=4401, ...)`` — if the
        TypeError escaped, the handshake would abort with a generic
        server error instead of the documented 4401 close code.
        """
        from starlette.websockets import WebSocketDisconnect

        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-123')
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        # Non-ASCII ``?token=`` query param — must funnel through the same
        # handshake path that closes with 4401 on bad creds.
        with TestClient(fastapi_app) as tc:  # noqa: SIM117
            with pytest.raises(WebSocketDisconnect) as exc_info:
                with tc.websocket_connect('/ws?token=non-asc%C3%AFi') as ws:
                    ws.receive_text()
        assert exc_info.value.code == 4401, f'expected 4401 close, got {exc_info.value.code}'

    def test_ws_same_origin_mixed_case_host_accepted(self, mock_recorder, mock_app):
        """Same-origin fallback must be case-insensitive on host.

        The TestClient sends both Origin and Host headers; making them differ
        only in casing verifies the helper lowercases both sides before
        comparing. Without the normalization, this handshake would be closed
        with 4403 even though the two headers point to the same origin.
        """
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir='/tmp',
            auth_token='secret-123',
            allowed_ws_origins=[],
        )
        real_recorder = EventRecorder(cfg)
        real_recorder._running = True
        fastapi_app = create_ui_app(cfg, real_recorder, mock_app)

        with (
            TestClient(fastapi_app) as tc,
            tc.websocket_connect(
                '/ws',
                headers={
                    'authorization': 'Bearer secret-123',
                    # Upper-case host in Origin, lower-case in Host — case-
                    # insensitive compare accepts this.
                    'origin': 'http://TESTSERVER',
                    'host': 'testserver',
                },
            ) as ws,
        ):
            real_recorder._record({'ts': time.time(), 'event': 'case_ok', 'partition': 0})
            (event,) = ws_recv_events(ws)
            assert event['event'] == 'case_ok'


# ---------------------------------------------------------------------------
# ``origin_allowed`` helper — direct unit tests
# ---------------------------------------------------------------------------
# The helper lives at module scope (not nested inside ``create_ui_app``),
# so we can call it directly with synthetic ``UIConfig`` values and
# hand-crafted origin/host strings. This gives coverage of the absent-Origin
# branch that TestClient can't exercise (it always sends Origin), and of the
# default-port normalization that browsers apply inconsistently to Origin vs.
# Host. Each test is a plain function — no FastAPI app, no event loop.


class TestOriginAllowedHelper:
    """Direct unit tests for ``origin_allowed`` — no TestClient required."""

    def test_absent_origin_accepted(self):
        """Non-browser clients omit Origin — accept (token was already checked)."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed(None, 'testserver', cfg) is True

    def test_absent_origin_accepted_even_with_allowlist(self):
        """Absent Origin is accepted regardless of allowlist — we err on the
        side of letting authenticated tools connect rather than rejecting a
        missing header a malicious browser couldn't omit anyway."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal'])
        assert origin_allowed(None, 'testserver', cfg) is True

    def test_allowlist_match_accepted(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal'])
        assert origin_allowed('https://ops.internal', 'ignored', cfg) is True

    def test_allowlist_case_insensitive(self):
        """Uppercase Origin must match lowercase allowlist entry."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal'])
        assert origin_allowed('https://OPS.INTERNAL', 'ignored', cfg) is True

    def test_allowlist_default_port_ignored(self):
        """Browsers strip :443 from https Origin but operators may write it
        explicitly in the allowlist. Normalization must treat the two forms
        as equal."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal:443'])
        assert origin_allowed('https://ops.internal', 'ignored', cfg) is True

    def test_allowlist_miss_rejected(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal'])
        assert origin_allowed('https://evil.com', 'ignored', cfg) is False

    def test_same_origin_exact_match(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://testserver', 'testserver', cfg) is True

    def test_same_origin_case_insensitive(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://Example.com', 'example.com', cfg) is True

    def test_same_origin_default_port_on_origin_missing_on_host(self):
        """Browser may send ``Origin: http://example.com`` (no port) while
        the Host header is ``example.com:80``. Normalization collapses both
        to ``(example.com, None)`` so the compare succeeds."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://example.com', 'example.com:80', cfg) is True

    def test_same_origin_default_port_on_host_missing_on_origin(self):
        """Inverse of the above: ``Origin: http://example.com:80`` + Host
        ``example.com`` must also match."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://example.com:80', 'example.com', cfg) is True

    def test_same_origin_non_default_port_mismatch_rejected(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://example.com:8080', 'example.com:9090', cfg) is False

    def test_same_origin_ipv6_host_bracketed(self):
        """RFC 7230 allows ``Host: [::1]:8080`` for IPv6 literals."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://[::1]:8080', '[::1]:8080', cfg) is True

    def test_same_origin_host_mismatch_rejected(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('https://evil.com', 'testserver', cfg) is False

    def test_malformed_origin_rejected(self):
        """Garbage in the Origin header must not cause the helper to raise —
        return False so the endpoint closes the connection cleanly."""
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        # No scheme → urlparse returns empty hostname → mismatch
        assert origin_allowed('not-a-url', 'testserver', cfg) is False


# ---------------------------------------------------------------------------
# Periodic tasks API
# ---------------------------------------------------------------------------


class TestApiPeriodicTasks:
    """Tests for /api/debug/periodic endpoint."""

    async def test_periodic_empty_when_no_events(self, tmp_path, mock_recorder, mock_app):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        # Use ``tmp_path`` rather than a hardcoded ``/tmp`` filename so
        # the test starts from an empty DB on every run. The webapp-release
        # schema change made ``CREATE INDEX idx_events_origin`` reference
        # the new ``origin`` column — leaving a stale legacy DB behind
        # would fail with ``no such column`` because ``CREATE TABLE IF
        # NOT EXISTS`` is a no-op against an existing legacy schema.
        db_path = str(tmp_path / 'test-periodic-empty.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)
        await db.commit()

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg
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

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp')
        mock_recorder._db = db
        mock_recorder._reader_db = db
        mock_recorder.reader_db = db
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = cfg

        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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
    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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
    import drakkar.uiserver.routes_debug as routes_debug_mod
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
    monkeypatch.setattr(routes_debug_mod, 'PROBE_TIMEOUT_HEADROOM_SECONDS', -1.9)

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
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


async def test_probe_endpoint_timeout_partial_includes_sink_records_from_completed_hooks(
    mock_recorder, debug_config, _probe_mock_app, monkeypatch
):
    """When timeout fires after on_task_complete produced a sink record, the partial captures it.

    Regression test for the "planned_sink_payloads lost on timeout" bug.
    Before the ``RunState`` refactor, the sink-flattening only ran at
    the very end of ``_run_stages``, so timeout cancellation lost any
    records captured by completed hooks. The per-run state now
    flattens on every ``to_report`` call so the partial sees them.
    """
    import drakkar.uiserver.routes_debug as routes_debug_mod
    from drakkar.config import ExecutorConfig

    handler = _ProbeTestHandler(task_count=1)
    _probe_mock_app.handler = handler

    # on_message_complete is the slow hook — it sleeps past the timeout,
    # but on_task_complete has already fired and produced a kafka record.
    async def on_message_complete_slow(group):
        await asyncio.sleep(1.0)
        return None

    handler.on_message_complete = on_message_complete_slow  # type: ignore[method-assign]

    _probe_mock_app._config = DrakkarConfig(
        executor=ExecutorConfig(task_timeout_seconds=1, binary_path='/nonexistent'),
    )
    monkeypatch.setattr(routes_debug_mod, 'PROBE_TIMEOUT_HEADROOM_SECONDS', -1.9)

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x'})
    assert resp.status_code == 200
    body = resp.json()
    assert body['truncated'] is True
    # _ProbeTestHandler emits one kafka record in on_task_complete;
    # that hook completed BEFORE the timeout fired, so its record
    # must be in the partial report.
    kafka_records = [r for r in body['planned_sink_payloads'] if r['sink_type'] == 'kafka']
    assert len(kafka_records) >= 1
    assert kafka_records[0]['origin_stage'].startswith('task_complete:')


async def test_probe_endpoint_empty_value_still_runs(mock_recorder, debug_config, _probe_mock_app):
    """POST with ``{"value": ""}`` succeeds — empty value is a valid probe input.

    Empty value is useful for probing handlers that short-circuit on
    ``msg.value == b''`` or key-only messages (Kafka tombstone-style).
    """
    _probe_mock_app.handler = _ProbeTestHandler(task_count=0)

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': ''})
    assert resp.status_code == 200
    body = resp.json()
    assert body['input']['value'] == ''


async def test_probe_endpoint_dispatches_to_drakkar_main_loop_when_different(
    mock_recorder, debug_config, _probe_mock_app
):
    """Regression: the ``ExecutorPool`` priority-gate crash when the
    endpoint runs on a separate event loop from the pipeline.

    The debug FastAPI server runs in its own thread + event loop so
    heavy requests don't block the pipeline. ``ExecutorPool._gate``
    is a custom asyncio primitive whose internal futures are bound to
    the main loop where the pool was constructed — acquiring a slot
    from a different loop raises "bound to a different event loop" as
    soon as the pool is contended. The endpoint must therefore
    dispatch ``runner.run`` back to the main loop via
    ``asyncio.run_coroutine_threadsafe``.

    This test spawns a real background thread with its own loop,
    exposes it as ``drakkar_app.main_loop``, records which loop the
    handler's ``arrange`` actually runs on, and asserts it's the main
    loop — not the endpoint's loop.
    """
    import threading

    main_loop_box: list[asyncio.AbstractEventLoop | None] = [None]
    loop_ready = threading.Event()

    def thread_body():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        main_loop_box[0] = loop
        loop_ready.set()
        loop.run_forever()

    thread = threading.Thread(target=thread_body, name='test-main-loop', daemon=True)
    thread.start()
    loop_ready.wait(timeout=5)
    main_loop = main_loop_box[0]
    assert main_loop is not None

    try:
        observed_loops: list[asyncio.AbstractEventLoop] = []

        handler = _ProbeTestHandler(task_count=1)
        original_arrange = handler.arrange

        async def arrange_recording_loop(messages, pending):
            observed_loops.append(asyncio.get_running_loop())
            return await original_arrange(messages, pending)

        handler.arrange = arrange_recording_loop  # type: ignore[method-assign]
        _probe_mock_app.handler = handler
        _probe_mock_app.main_loop = main_loop

        fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post('/api/debug/probe', json={'value': '{"hello": "world"}'})

        assert resp.status_code == 200
        # arrange ran exactly once, on the main loop (not the test's).
        assert len(observed_loops) == 1
        assert observed_loops[0] is main_loop
        assert observed_loops[0] is not asyncio.get_running_loop()
    finally:
        main_loop.call_soon_threadsafe(main_loop.stop)
        thread.join(timeout=5)


async def test_probe_endpoint_defaults_topic_to_configured_source_topic(mock_recorder, debug_config, _probe_mock_app):
    """POST with no ``topic`` in body → probe sees ``config.kafka.source_topic``.

    Handlers that key off ``msg.topic`` would see an empty string
    without this default — the endpoint substitutes the configured
    source topic so the probe produces a realistic ``SourceMessage``.
    """
    from drakkar.config import ExecutorConfig, KafkaConfig

    _probe_mock_app.handler = _ProbeTestHandler(task_count=0)
    _probe_mock_app._config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/nonexistent'),
        kafka=KafkaConfig(brokers='host:9092', source_topic='configured-input-topic', consumer_group='grp'),
    )

    fastapi_app = create_ui_app(debug_config, mock_recorder, _probe_mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.post('/api/debug/probe', json={'value': 'x'})
    assert resp.status_code == 200
    body = resp.json()
    # The configured source topic appears as the echo, since the request
    # left topic empty.
    assert body['input']['topic'] == 'configured-input-topic'


# ---------------------------------------------------------------------------
# Cross-thread aiosqlite dispatch regression test
#
# Mirrors the probe-endpoint regression at line ~3696. The debug FastAPI
# server runs on its own thread + event loop, so any ``aiosqlite``
# connection opened on the pipeline's main loop cannot be awaited from
# the endpoint's loop directly — ``run_coroutine_threadsafe`` must
# marshal the call back. We exercise ``/api/events`` here because it's
# the simplest aiosqlite-reading endpoint; the same helper
# (``drakkar.concurrency.dispatch_to_loop``) wraps every cross-thread
# read in the module.
# ---------------------------------------------------------------------------


async def test_api_events_dispatches_to_drakkar_main_loop_when_different(
    tmp_path, mock_recorder, debug_config, mock_app
):
    """Regression: cross-thread aiosqlite reads must run on the main loop.

    Spawns a real background thread with its own event loop, opens an
    aiosqlite connection on that loop, wires it into the recorder, and
    then hits ``/api/events`` from the test's loop. The assertion is
    that the ``aiosqlite._db.execute`` call runs on the main loop, not
    the endpoint's loop — without the dispatch helper, the cursor
    would be bound to the main loop and awaiting it from the test's
    loop raises ``"... is bound to a different event loop"`` once the
    connection has any contention.
    """
    import threading

    import aiosqlite

    from drakkar.recorder import SCHEMA_EVENTS

    main_loop_box: list[asyncio.AbstractEventLoop | None] = [None]
    loop_ready = threading.Event()

    def thread_body():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        main_loop_box[0] = loop
        loop_ready.set()
        loop.run_forever()

    thread = threading.Thread(target=thread_body, name='test-main-loop', daemon=True)
    thread.start()
    loop_ready.wait(timeout=5)
    main_loop = main_loop_box[0]
    assert main_loop is not None

    try:
        # Open the aiosqlite connection on the main loop, not the test
        # loop. This mirrors production: the recorder creates ``_db``
        # during ``EventRecorder.start()`` which runs on ``DrakkarApp``'s
        # loop. The cursor is bound to the loop where ``connect``
        # resolves; awaiting the cursor from another loop is the bug.
        async def _open_db():
            db_path = str(tmp_path / 'live.db')
            db = await aiosqlite.connect(db_path)
            await db.executescript(SCHEMA_EVENTS)
            now = time.time()
            for i in range(3):
                await db.execute(
                    'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
                    (now - i, '2026-04-22', 'consumed', 0, i, f'task-{i}'),
                )
            await db.commit()
            return db

        db_future = asyncio.run_coroutine_threadsafe(_open_db(), main_loop)
        db = await asyncio.wrap_future(db_future)

        # Record which loop the ``execute`` actually runs on by wrapping
        # the connection with a small spy. The real aiosqlite connection
        # is kept so commits, fetchall, etc. still work normally.
        observed_loops: list[asyncio.AbstractEventLoop] = []

        class _LoopObservingDB:
            """Passthrough wrapper that records the loop on each execute."""

            def __init__(self, wrapped: aiosqlite.Connection) -> None:
                self._wrapped = wrapped

            def execute(self, *args, **kwargs):
                observed_loops.append(asyncio.get_running_loop())
                return self._wrapped.execute(*args, **kwargs)

            def __getattr__(self, name: str):
                return getattr(self._wrapped, name)

        # Route both writer and reader through the loop-observing spy so
        # we can assert the debug-server read path stays on the main loop
        # whether it reads via ``_db`` or ``reader_db`` — the production
        # endpoint picks the reader first but both must be bound to the
        # main loop.
        observer = _LoopObservingDB(db)
        mock_recorder._db = observer
        mock_recorder._reader_db = observer
        mock_recorder.reader_db = observer
        mock_recorder.flush = AsyncMock()
        mock_recorder._buffer = []
        mock_recorder.config = debug_config
        mock_app.main_loop = main_loop

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/events')

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

        # The SELECT ran exactly once, on the main loop (not the test's).
        assert len(observed_loops) >= 1
        assert all(lo is main_loop for lo in observed_loops), f'expected all SELECTs on main loop, got {observed_loops}'
        assert main_loop is not asyncio.get_running_loop()

        # Close the DB on the main loop to avoid "future is attached to
        # a different loop" during teardown.
        close_future = asyncio.run_coroutine_threadsafe(db.close(), main_loop)
        await asyncio.wrap_future(close_future)
    finally:
        main_loop.call_soon_threadsafe(main_loop.stop)
        thread.join(timeout=5)


async def test_api_events_falls_back_to_inline_when_main_loop_is_mock(tmp_path, mock_recorder, debug_config, mock_app):
    """When ``main_loop`` is a ``MagicMock`` (the default unit-test path),
    ``drakkar.concurrency.dispatch_to_loop`` must fall back to inline
    execution so the test suite doesn't need a real background thread.
    Regression for the MagicMock-guard in
    ``isinstance(target_loop, asyncio.AbstractEventLoop)``.
    """
    import aiosqlite

    from drakkar.recorder import SCHEMA_EVENTS

    db_path = str(tmp_path / 'live.db')
    db = await aiosqlite.connect(db_path)
    await db.executescript(SCHEMA_EVENTS)
    await db.execute(
        'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
        (time.time(), '2026-04-22', 'consumed', 0, 1, 'task-1'),
    )
    await db.commit()

    mock_recorder._db = db
    mock_recorder._reader_db = db
    mock_recorder.reader_db = db
    mock_recorder.flush = AsyncMock()
    mock_recorder._buffer = []
    mock_recorder.config = debug_config
    # ``mock_app.main_loop`` is a ``MagicMock`` by default; the helper
    # should see it's not an ``asyncio.AbstractEventLoop`` and execute
    # inline on the current loop without error.

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/events')
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    await db.close()


async def test_api_debug_processors_snapshot_runs_on_main_loop(mock_recorder, debug_config, mock_app):
    """The ``/api/debug/processors`` snapshot reads private mutable state
    on ``PartitionProcessor`` that is mutated exclusively by the main
    loop (``_sorted_offsets``, ``_arranging``, ``_arrange_labels``,
    etc.). The snapshot-building coroutine must therefore run on the
    main loop for internal consistency.
    """
    import threading

    main_loop_box: list[asyncio.AbstractEventLoop | None] = [None]
    loop_ready = threading.Event()

    def thread_body():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        main_loop_box[0] = loop
        loop_ready.set()
        loop.run_forever()

    thread = threading.Thread(target=thread_body, name='test-main-loop-proc', daemon=True)
    thread.start()
    loop_ready.wait(timeout=5)
    main_loop = main_loop_box[0]
    assert main_loop is not None

    try:
        observed_loops: list[asyncio.AbstractEventLoop] = []

        # A minimal processor stub: the endpoint reads a handful of
        # attributes. We record which loop the read happens on by
        # intercepting one of the reads.
        class _ObservingOffsets:
            def __init__(self):
                self._data = [1, 2, 3]

            def __getitem__(self, idx):
                observed_loops.append(asyncio.get_running_loop())
                return self._data[idx]

        proc = MagicMock()
        proc.offset_tracker = MagicMock()
        proc.offset_tracker._sorted_offsets = _ObservingOffsets()
        proc.offset_tracker._offsets = {1: 'pending', 2: 'pending', 3: 'done'}
        proc.offset_tracker.pending_count = 2
        proc.offset_tracker.completed_count = 1
        proc.offset_tracker.total_tracked = 3
        proc.offset_tracker.last_committed = 0
        proc.offset_tracker.committable.return_value = 0
        proc.queue_size = 0
        proc.inflight_count = 0
        proc._arranging = False
        proc._arrange_start = 0.0
        proc._arrange_labels = []
        proc._active_tasks = []

        mock_app.processors = {0: proc}
        mock_app.main_loop = main_loop

        fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/processors')

        assert resp.status_code == 200
        # The snapshot touched ``_sorted_offsets[:20]`` which goes
        # through ``__getitem__`` at least once. All observations must
        # be on the main loop.
        assert len(observed_loops) >= 1
        assert all(lo is main_loop for lo in observed_loops), (
            f'expected all processor reads on main loop, got {observed_loops}'
        )
    finally:
        main_loop.call_soon_threadsafe(main_loop.stop)
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# _flush_and_select helper: consolidates the "flush + read via
# reader_db with writer fallback" pattern. The helper is a closure inside
# ``create_ui_app`` so it isn't imported directly — instead these tests
# exercise it through ``/api/events`` which routes a minimal SELECT through
# the helper.
# ---------------------------------------------------------------------------


async def test_flush_and_select_helper_returns_columns_and_rows(tmp_path, mock_recorder, debug_config, mock_app):
    """The helper returns (columns, rows) from a SELECT on the recorder
    DB and flushes before reading. Exercised via ``/api/events`` because
    the helper is a closure inside ``create_ui_app``.
    """
    import aiosqlite

    from drakkar.recorder import SCHEMA_EVENTS

    db_path = str(tmp_path / 'live.db')
    writer = await aiosqlite.connect(db_path)
    await writer.executescript(SCHEMA_EVENTS)
    # Seed two rows with distinguishable ids so the helper's tuple
    # construction is visible in the response.
    now = time.time()
    await writer.execute(
        'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
        (now, '2026-04-22', 'consumed', 0, 1, 'task-a'),
    )
    await writer.execute(
        'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
        (now - 1, '2026-04-22', 'consumed', 1, 2, 'task-b'),
    )
    await writer.commit()

    # Separate reader connection so we can verify the helper preferred it
    # over the writer. Both point at the same file — typical WAL-mode
    # recorder setup.
    reader = await aiosqlite.connect(db_path)

    flush_calls = 0

    async def _flush():
        nonlocal flush_calls
        flush_calls += 1

    mock_recorder._db = writer
    mock_recorder._reader_db = reader
    mock_recorder.reader_db = reader
    # Debug server uses the public ``recorder.flush()`` (MED-4). Stub the
    # public name so the helper's ``await recorder.flush()`` actually hits
    # our counter — setting ``_flush`` would be a no-op because the mock
    # spec routes calls through the real attribute name.
    mock_recorder.flush = _flush
    mock_recorder.config = debug_config
    mock_recorder._buffer = []

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/events')

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    # Column names surface as dict keys — direct evidence the helper
    # returned ``columns`` alongside ``rows``.
    assert {'task_id', 'partition', 'offset', 'event'}.issubset(data[0].keys())
    task_ids = {e['task_id'] for e in data}
    assert task_ids == {'task-a', 'task-b'}
    # Helper always flushes the recorder before SELECTing.
    assert flush_calls == 1

    await writer.close()
    await reader.close()


async def test_flush_and_select_helper_returns_none_when_db_missing(debug_config, mock_recorder, mock_app):
    """When ``reader_db`` is None the helper must return ``None``;
    the endpoint maps that onto its empty-response shape (``[]`` for
    ``/api/events``).
    """
    mock_recorder._db = None
    mock_recorder._reader_db = None
    mock_recorder.reader_db = None

    flush_calls = 0

    async def _flush():
        nonlocal flush_calls
        flush_calls += 1

    # Public-API stubbing (MED-4) — see the helper-returns-columns test
    # above for rationale.
    mock_recorder.flush = _flush
    mock_recorder.config = debug_config
    mock_recorder._buffer = []

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/events')

    assert resp.status_code == 200
    assert resp.json() == []
    # Flush still runs — only the SELECT is skipped. Matches the helper's
    # original per-site behaviour (flush is unconditional).
    assert flush_calls == 1


async def test_flush_and_select_logs_the_missing_reader_once_per_interval(
    debug_config, mock_recorder, mock_app, monkeypatch
):
    """A degraded read must name its cause in the log, without flooding it.

    Every open page re-polls, so the warning is rate-limited per cause —
    but silence is what left the last incident undiagnosed.
    """
    from structlog.testing import capture_logs

    from drakkar.uiserver import server as server_module

    monkeypatch.setattr(server_module, '_degraded_read_last_logged', {})

    mock_recorder._db = None
    mock_recorder._reader_db = None
    mock_recorder.reader_db = None
    mock_recorder.flush = AsyncMock()
    mock_recorder.config = debug_config
    mock_recorder._buffer = []

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    with capture_logs() as cap:
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            first = await c.get('/api/events')
            second = await c.get('/api/events')

    assert first.status_code == second.status_code == 200
    warnings = [e for e in cap if e['event'] == 'ui_read_reader_db_absent']
    assert len(warnings) == 1
    assert warnings[0]['log_level'] == 'warning'


async def test_flush_and_select_logs_a_dispatch_failure_as_its_own_cause(
    debug_config, mock_recorder, mock_app, monkeypatch
):
    """Reader-absent and dispatch-timeout are different problems; the log says which."""
    from structlog.testing import capture_logs

    from drakkar.uiserver import server as server_module

    monkeypatch.setattr(server_module, '_degraded_read_last_logged', {})

    async def never_answers(self, coro, default=None):
        coro.close()  # the dispatch never ran it; don't leave it un-awaited
        return default

    monkeypatch.setattr(UIDeps, 'dispatch_bounded', never_answers)

    mock_recorder.config = debug_config
    mock_recorder._buffer = []

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    with capture_logs() as cap:
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/events')

    assert resp.status_code == 200
    assert resp.json() == []
    events = [e['event'] for e in cap]
    assert 'ui_read_dispatch_unavailable' in events
    assert 'ui_read_reader_db_absent' not in events


async def test_flush_and_select_helper_prefers_reader_db_over_writer(tmp_path, mock_recorder, debug_config, mock_app):
    """Sanity check that the helper reads through ``reader_db`` not
    ``_db`` when both are set (the whole point of the helper). Uses two
    independent DB files: a writer with one row, a reader with three.
    A helper that read the writer would return 1 row; the endpoint
    must return 3.
    """
    import aiosqlite

    from drakkar.recorder import SCHEMA_EVENTS

    writer_path = str(tmp_path / 'writer.db')
    reader_path = str(tmp_path / 'reader.db')
    writer = await aiosqlite.connect(writer_path)
    reader = await aiosqlite.connect(reader_path)
    await writer.executescript(SCHEMA_EVENTS)
    await reader.executescript(SCHEMA_EVENTS)

    now = time.time()
    await writer.execute(
        'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
        (now, '2026-04-22', 'consumed', 0, 1, 'writer-only'),
    )
    for i in range(3):
        await reader.execute(
            'INSERT INTO events (ts, dt, event, partition, offset, task_id) VALUES (?, ?, ?, ?, ?, ?)',
            (now - i, '2026-04-22', 'consumed', 0, 10 + i, f'reader-{i}'),
        )
    await writer.commit()
    await reader.commit()

    mock_recorder._db = writer
    mock_recorder._reader_db = reader
    mock_recorder.reader_db = reader
    mock_recorder.flush = AsyncMock()
    mock_recorder._buffer = []
    mock_recorder.config = debug_config

    fastapi_app = create_ui_app(debug_config, mock_recorder, mock_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/api/events')

    assert resp.status_code == 200
    data = resp.json()
    # Reader has 3 rows; writer has 1. Seeing 3 (and none of them being
    # the writer's row) is conclusive evidence that the helper routed
    # through reader_db.
    assert len(data) == 3
    assert all(e['task_id'].startswith('reader-') for e in data)

    await writer.close()
    await reader.close()


# ---------------------------------------------------------------------------
# Kubernetes probes: /healthz and /readyz
# ---------------------------------------------------------------------------


@pytest.fixture
def probe_app(mock_app):
    """mock_app variant with the probe-relevant state set explicitly.

    ``mock_app`` is a ``MagicMock`` — any attribute access returns a truthy
    ``MagicMock``, which would make ``drakkar_app.is_ready`` and
    ``sink_manager.all_connected()`` evaluate as ``True`` by accident. This
    fixture pins the two signals to concrete values so probe tests assert
    on them deterministically. Individual tests flip the values via
    ``probe_app.is_ready`` / ``probe_app.sink_manager.all_connected.return_value``.
    """
    mock_app.is_ready = False
    mock_app.sink_manager.all_connected.return_value = True
    mock_app.sink_manager.disconnected_sink_names.return_value = []
    return mock_app


@pytest.fixture
async def probe_client(debug_config, mock_recorder, probe_app):
    fastapi_app = create_ui_app(debug_config, mock_recorder, probe_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        yield c


async def test_healthz_returns_200_without_auth(debug_config, mock_recorder, probe_app):
    """Liveness probe must work even when an auth_token is configured.

    Kubernetes probes have no way to supply a bearer token, so ``/healthz``
    must accept anonymous requests regardless of ``config.auth_token``.
    """
    cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-token')
    fastapi_app = create_ui_app(cfg, mock_recorder, probe_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        resp = await c.get('/healthz')

    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok'}


async def test_healthz_ignores_supplied_auth_token(probe_client):
    """A client that sends an Authorization header to /healthz is not rejected."""
    resp = await probe_client.get('/healthz', headers={'Authorization': 'Bearer irrelevant'})
    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok'}


async def test_readyz_returns_503_before_is_ready(probe_client, probe_app):
    """/readyz must fail with 503 until the first poll cycle completes."""
    probe_app.is_ready = False
    probe_app.sink_manager.all_connected.return_value = True

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 503
    body = resp.json()
    assert body['status'] == 'not_ready'
    assert 'not_started' in body['reasons']


async def test_readyz_returns_200_when_ready_and_all_connected(probe_client, probe_app):
    """/readyz succeeds only when both is_ready AND all sinks connected."""
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.sink_manager.disconnected_sink_names.return_value = []

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 200
    assert resp.json() == {'status': 'ready'}


async def test_readyz_returns_503_when_sink_disconnected(probe_client, probe_app):
    """/readyz surfaces each disconnected sink id in the reasons array."""
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = False
    probe_app.sink_manager.disconnected_sink_names.return_value = ['kafka:results']

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 503
    body = resp.json()
    assert body['status'] == 'not_ready'
    assert 'sink_kafka:results_not_connected' in body['reasons']


class _DeadProcessorStub:
    """Minimal stand-in for a PartitionProcessor that gave up."""

    def __init__(self, dead: bool) -> None:
        self.is_dead = dead


async def test_readyz_returns_503_when_a_partition_processor_died(probe_client, probe_app):
    """A dead partition loop must take the pod out of rotation.

    Nothing drains that partition's queue and nothing commits its offsets
    for the life of the process, so a worker that keeps reporting ready
    holds an assignment it cannot serve — the failure stays invisible
    until consumer lag alerts fire, with nothing naming the cause.
    """
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.processors = {3: _DeadProcessorStub(dead=True)}

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 503
    body = resp.json()
    assert body['status'] == 'not_ready'
    assert 'partition_3_processor_died' in body['reasons']


async def test_readyz_stays_200_while_partitions_are_alive(probe_client, probe_app):
    """Live processors must not trip readiness."""
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.processors = {0: _DeadProcessorStub(dead=False), 1: _DeadProcessorStub(dead=False)}

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 200


async def test_readyz_names_every_dead_partition(probe_client, probe_app):
    """Each dead partition is named, so the reason list identifies all of them."""
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.processors = {
        0: _DeadProcessorStub(dead=True),
        1: _DeadProcessorStub(dead=False),
        2: _DeadProcessorStub(dead=True),
    }

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 503
    reasons = resp.json()['reasons']
    assert 'partition_0_processor_died' in reasons
    assert 'partition_2_processor_died' in reasons
    assert 'partition_1_processor_died' not in reasons


async def test_readyz_reports_multiple_reasons(probe_client, probe_app):
    """When both the worker isn't ready and a sink is down, /readyz lists both."""
    probe_app.is_ready = False
    probe_app.sink_manager.all_connected.return_value = False
    probe_app.sink_manager.disconnected_sink_names.return_value = ['postgres:main-db']

    resp = await probe_client.get('/readyz')

    assert resp.status_code == 503
    body = resp.json()
    reasons = body['reasons']
    assert 'not_started' in reasons
    assert 'sink_postgres:main-db_not_connected' in reasons


async def test_readyz_ignores_supplied_auth_token(probe_client, probe_app):
    """A bearer token on /readyz does not change its response code."""
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.sink_manager.disconnected_sink_names.return_value = []

    resp = await probe_client.get('/readyz', headers={'Authorization': 'Bearer anything'})

    assert resp.status_code == 200
    assert resp.json() == {'status': 'ready'}


async def test_readyz_with_auth_token_configured_still_accessible(debug_config, mock_recorder, probe_app):
    """When an auth_token is configured globally, /readyz remains unauthenticated."""
    cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp', auth_token='secret-token')
    probe_app.is_ready = True
    probe_app.sink_manager.all_connected.return_value = True
    probe_app.sink_manager.disconnected_sink_names.return_value = []

    fastapi_app = create_ui_app(cfg, mock_recorder, probe_app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as c:
        # No Authorization header supplied — still 200.
        resp = await c.get('/readyz')

    assert resp.status_code == 200
    assert resp.json() == {'status': 'ready'}


class TestBuiltinUIBadge:
    """The built-in fallback pages label themselves so operators can tell
    them apart from a served drakkar-ui release (which shows its version in
    its own header instead)."""

    async def test_builtin_pages_show_the_badge(self, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp')
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'built-in UI' in resp.text

    async def test_spa_mode_does_not_show_the_badge(self, tmp_path, mock_recorder, mock_app):
        bundle = tmp_path / 'v1.0.0'
        bundle.mkdir()
        (bundle / 'index.html').write_text('<html><body>spa</body></html>')
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp')
        fastapi_app = create_ui_app(cfg, mock_recorder, mock_app, ui_root=bundle)
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        assert 'built-in UI' not in resp.text


# ---------------------------------------------------------------------------
# Kafka-UI deep-link builder
# ---------------------------------------------------------------------------


class TestKafkaUiMessageUrl:
    """``UIDeps.kafka_ui_message_url`` — feature-toggle + URL shape."""

    def _make_deps(self, mock_recorder, mock_app) -> UIDeps:
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp')
        return UIDeps(
            config=cfg,
            recorder=mock_recorder,
            drakkar_app=mock_app,
            templates=MagicMock(),
        )

    def test_returns_empty_string_when_kafka_ui_not_configured(self, mock_recorder, mock_app):
        """Missing ui_url/ui_cluster_name acts as a feature toggle — ''."""
        deps = self._make_deps(mock_recorder, mock_app)
        assert deps.kafka_ui_message_url('topic-a', 0, 42) == ''

    def test_builds_seek_url_with_encoded_cluster_and_topic(self, mock_recorder, mock_app):
        """Configured Kafka-UI base + cluster produce the seekTo deep link,
        URL-encoding the cluster name and topic."""
        mock_app._config.kafka.ui_url = 'http://kafka-ui.internal:8080/'
        mock_app._config.kafka.ui_cluster_name = 'prod cluster'
        deps = self._make_deps(mock_recorder, mock_app)

        url = deps.kafka_ui_message_url('events.raw', 3, 1200)

        assert url == (
            'http://kafka-ui.internal:8080/ui/clusters/prod%20cluster/all-topics/'
            'events.raw/messages?seekType=OFFSET&seekTo=3%3A%3A1200&limit=1'
        )

    def test_returns_empty_string_for_empty_topic(self, mock_recorder, mock_app):
        mock_app._config.kafka.ui_url = 'http://kafka-ui.internal:8080'
        mock_app._config.kafka.ui_cluster_name = 'prod'
        deps = self._make_deps(mock_recorder, mock_app)
        assert deps.kafka_ui_message_url('', 0, 0) == ''


# ---------------------------------------------------------------------------
# dispatch_bounded: main-loop timeout degrades instead of hanging
# ---------------------------------------------------------------------------


async def test_dispatch_bounded_returns_default_on_main_loop_timeout(mock_recorder, mock_app, monkeypatch):
    """A wedged main loop (here: a loop that is never run) must not hang the
    UI request — dispatch_bounded logs and returns the caller's default."""
    from drakkar.uiserver import server as server_mod

    monkeypatch.setattr(server_mod, 'MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS', 0.05)

    stuck_loop = asyncio.new_event_loop()  # real loop, never run — wedged
    try:
        mock_app.main_loop = stuck_loop
        cfg = make_ui_config(enabled=True, port=8080, db_dir='/tmp')
        deps = UIDeps(config=cfg, recorder=mock_recorder, drakkar_app=mock_app, templates=MagicMock())

        async def _never_answers():
            return 'unreachable'

        # Keep a handle so the never-started coroutine can be closed —
        # otherwise its GC emits a "never awaited" RuntimeWarning.
        coro = _never_answers()

        from structlog.testing import capture_logs

        with capture_logs() as cap:
            result = await deps.dispatch_bounded(coro, default='degraded')

        assert result == 'degraded'
        assert any(e['event'] == 'ui_main_loop_dispatch_timeout' for e in cap)
        coro.close()
    finally:
        stuck_loop.close()


# ---------------------------------------------------------------------------
# UIServer._wait_until_serving: dead thread means the bind failed
# ---------------------------------------------------------------------------


def test_wait_until_serving_raises_when_server_thread_died(debug_config, mock_recorder, mock_app):
    """uvicorn exits its daemon thread on a bind failure; the startup wait
    must surface that as a loud RuntimeError instead of timing out."""
    import threading
    from types import SimpleNamespace

    from drakkar.uiserver.server import UIServer

    server = UIServer(debug_config, mock_recorder, mock_app)
    server._server = SimpleNamespace(started=False)
    dead = threading.Thread(target=lambda: None)
    dead.start()
    dead.join()
    server._thread = dead

    with pytest.raises(RuntimeError, match='already in use'):
        server._wait_until_serving(timeout=1.0)


# ---------------------------------------------------------------------------
# UIServer._resolve_ui_bundle: resolution errors are never fatal
# ---------------------------------------------------------------------------


class TestResolveUiBundle:
    """The bundle ladder degrades to the built-in pages on errors."""

    async def test_resolve_error_returns_none(self, debug_config, mock_recorder, mock_app, monkeypatch):
        import drakkar.uihost
        from drakkar.uiserver.server import UIServer

        mock_app._config.ui.release.enabled = True

        def _boom(release_cfg):
            raise RuntimeError('network down')

        monkeypatch.setattr(drakkar.uihost, 'resolve', _boom)
        server = UIServer(debug_config, mock_recorder, mock_app)

        from structlog.testing import capture_logs

        with capture_logs() as cap:
            assert await server._resolve_ui_bundle() is None
        assert any(e['event'] == 'ui_resolve_failed' for e in cap)

    async def test_resolve_returning_none_yields_none(self, debug_config, mock_recorder, mock_app, monkeypatch):
        import drakkar.uihost
        from drakkar.uiserver.server import UIServer

        mock_app._config.ui.release.enabled = True
        monkeypatch.setattr(drakkar.uihost, 'resolve', lambda release_cfg: None)
        server = UIServer(debug_config, mock_recorder, mock_app)

        assert await server._resolve_ui_bundle() is None


# ---------------------------------------------------------------------------
# Host-header parsing + origin allowlist edge cases
# ---------------------------------------------------------------------------


class TestParseHostHeaderEdgeCases:
    """Malformed / unusual Host headers must degrade, never raise."""

    def test_empty_header_returns_empty_host(self):
        from drakkar.uiserver.server_helpers import parse_host_header

        assert parse_host_header('') == ('', None)

    def test_unclosed_ipv6_bracket_treated_as_whole_host(self):
        from drakkar.uiserver.server_helpers import parse_host_header

        assert parse_host_header('[::1') == ('[::1', None)

    def test_ipv6_with_non_numeric_port_falls_back_to_none(self):
        from drakkar.uiserver.server_helpers import parse_host_header

        assert parse_host_header('[::1]:banana') == ('::1', None)

    def test_hostname_with_non_numeric_port_falls_back_to_none(self):
        from drakkar.uiserver.server_helpers import parse_host_header

        # A non-numeric "port" means the header was never host:port — the
        # whole string is kept as the host, with no port.
        assert parse_host_header('example.com:banana') == ('example.com:banana', None)


class TestOriginAllowedMalformedUrls:
    """urlparse raises ValueError on invalid IPv6 URLs — the helper must
    treat those as a rejection (or skip the bad allowlist entry), not crash."""

    def test_malformed_origin_with_allowlist_rejected(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=['https://ops.internal'])
        assert origin_allowed('http://[::1', 'testserver', cfg) is False

    def test_malformed_allowlist_entry_skipped_but_valid_entry_matches(self):
        cfg = make_ui_config(
            auth_token='secret',
            allowed_ws_origins=['http://[::1', 'https://ops.internal'],
        )
        assert origin_allowed('https://ops.internal', 'ignored', cfg) is True

    def test_malformed_origin_same_origin_fallback_rejected(self):
        cfg = make_ui_config(auth_token='secret', allowed_ws_origins=[])
        assert origin_allowed('http://[::1', 'testserver', cfg) is False


# ---------------------------------------------------------------------------
# backend_version: installed-package lookup with source-tree fallback
# ---------------------------------------------------------------------------


def test_backend_version_falls_back_to_package_dunder(monkeypatch):
    """When py-drakkar is not installed as a distribution (editable /
    source-tree runs), the helper falls back to ``drakkar.__version__``."""
    import importlib.metadata

    from drakkar import __version__
    from drakkar.uiserver.server_helpers import backend_version

    def _not_installed(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, 'version', _not_installed)

    assert backend_version() == __version__


# ---------------------------------------------------------------------------
# HTTP transport limits
# ---------------------------------------------------------------------------


class TestUIServerBodyLimit:
    """The UI server takes a body on exactly two routes, and the probe's own
    10 MB limit is a pydantic field constraint — it fires only AFTER the whole
    body has been buffered, which is the wrong end of the problem.
    """

    async def test_oversized_declared_body_is_rejected_before_buffering(self, tmp_path, mock_recorder, mock_app):
        from drakkar.uiserver.server import MAX_BODY_BYTES

        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/probe',
                content=b'{}',
                headers={'content-length': str(MAX_BODY_BYTES + 1), 'content-type': 'application/json'},
            )
        assert resp.status_code == 413
        assert resp.json()['max_bytes'] == MAX_BODY_BYTES

    async def test_malformed_content_length_is_a_400(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.post(
                '/api/debug/probe',
                content=b'{}',
                headers={'content-length': 'not-a-number', 'content-type': 'application/json'},
            )
        assert resp.status_code == 400

    async def test_normal_requests_pass_through(self, tmp_path, mock_recorder, mock_app):
        """The middleware must not disturb ordinary traffic."""
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        transport = ASGITransport(app=create_ui_app(cfg, mock_recorder, mock_app))
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/healthz')
        assert resp.status_code == 200

    async def test_uvicorn_config_carries_the_transport_limits(self, debug_config, mock_recorder, mock_app):
        """Mirrors the Go backend's http.Server hardening, which the Python
        UI server was missing entirely — on the process that also answers
        the Kubernetes probes."""
        from unittest.mock import patch

        from drakkar.uiserver.server import KEEP_ALIVE_TIMEOUT_SECONDS, MAX_HEADER_BYTES, UIServer

        server = UIServer(debug_config, mock_recorder, mock_app)
        fake_uvicorn = MagicMock()
        fake_uvicorn.started = True
        fake_uvicorn.run = MagicMock()

        with (
            patch('drakkar.uiserver.server.uvicorn.Server', return_value=fake_uvicorn),
            patch('drakkar.uiserver.server.uvicorn.Config') as mock_uvi_config,
            patch('drakkar.uiserver.server.logger') as mock_logger,
        ):
            mock_uvi_config.return_value = MagicMock()
            mock_logger.ainfo = AsyncMock()
            await server.start()
            if server._thread is not None:
                server._thread.join(timeout=2.0)

        kwargs = mock_uvi_config.call_args.kwargs
        assert kwargs['timeout_keep_alive'] == KEEP_ALIVE_TIMEOUT_SECONDS
        assert kwargs['h11_max_incomplete_event_size'] == MAX_HEADER_BYTES
