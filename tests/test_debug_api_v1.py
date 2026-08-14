"""Tests for the /api/v1 contract surface (UI contract v1).

Covers:
  * ``/api/v1`` aliases of every legacy JSON route (same payload, same auth).
  * The three v1-only endpoints: ``/api/v1/partitions``,
    ``/api/v1/task/{id}``, ``/api/v1/live/overview``.
  * The contract reconciliations: events 422 on malformed partitions CSV,
    probe report omitting ``traceback``, ``custom`` planned-sink payloads,
    and merge/download/trace/trace-by-label input hardening.
"""

import time
import typing
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import quote

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from drakkar.config import DrakkarConfig, TimelineColorRule, TimelineLabels, TimelineRuleCondition, UITimelineConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

_INT32_MAX = 2**31 - 1

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_recorder():
    rec = AsyncMock(spec=EventRecorder)
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
    rec.get_stats.return_value = {'total_events': 3, 'consumed': 2, 'completed': 1}
    rec.get_partition_summary.return_value = []
    rec.get_task_events.return_value = []
    rec.get_active_tasks.return_value = []
    # fresh list per call: the /api/workers handler appends the current
    # worker to whatever discover_workers returns
    rec.discover_workers.side_effect = lambda: []
    rec.cross_trace.return_value = []
    rec.cross_trace_by_label.return_value = []
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
    # ``cache_engine=None`` makes the cache routes 404 (disabled); a plain
    # MagicMock would be truthy and send them down the real-reader path.
    app.cache_engine = None
    # ``handler=None`` pins hook_flags to the deterministic all-False branch.
    app.handler = None
    app._consumer = None
    # ``_offload_pool=None`` keeps the overview's optional ``offload`` key
    # absent (key-presence is the contract's feature flag); a bare
    # MagicMock would be truthy and leak an unserializable snapshot in.
    app._offload_pool = None
    # No declared UI pages by default; client_with_pages overrides.
    app.ui_pages = []

    pool = MagicMock()
    pool.active_count = 2
    pool.waiting_count = 1
    pool.max_executors = 8
    app._executor_pool = pool

    sink_mgr = MagicMock()
    sink_mgr.get_sink_info.return_value = [{'sink_type': 'kafka', 'name': 'results'}]
    sink_mgr.get_all_stats.return_value = {}
    app.sink_manager = sink_mgr
    return app


@pytest.fixture
def debug_config(tmp_path):
    return make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))


def make_client(cfg, recorder, app) -> AsyncClient:
    fastapi_app = create_ui_app(cfg, recorder, app)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


@pytest.fixture
async def client(debug_config, mock_recorder, mock_app):
    mock_recorder.config = debug_config
    async with make_client(debug_config, mock_recorder, mock_app) as c:
        yield c


@pytest.fixture
async def client_with_link_bases(tmp_path, mock_recorder, mock_app):
    cfg = make_ui_config(
        enabled=True, port=8080, db_dir=str(tmp_path), link_bases={'jira': 'https://jira.internal.example.com'}
    )
    mock_app.config_summary = '[test-worker]'
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app) as c:
        yield c


@pytest.fixture
async def client_with_pages(tmp_path, mock_recorder, mock_app):
    """A client whose app declares one page, mirroring the uipages golden fixture."""
    from drakkar.probe import Column
    from drakkar.uipages import AnnotationsSource, Page, Widget, build_pages

    page = Page(
        slug='orders',
        title='Orders',
        widgets=[
            Widget(
                title='Recent orders',
                view='table',
                source=AnnotationsSource(kind_prefix='order.', limit=100),
                columns={'order_id': Column(link_template='{shop_admin}/orders/{value}')},
            )
        ],
    )
    mock_app.ui_pages = build_pages([page])
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app) as c:
        yield c


# ---------------------------------------------------------------------------
# Work package A: /api/v1 aliases
# ---------------------------------------------------------------------------


LEGACY_TO_V1 = [
    ('/api/dashboard', '/api/v1/dashboard'),
    ('/api/sinks', '/api/v1/sinks'),
    ('/api/workers', '/api/v1/workers'),
    ('/api/debug/processors', '/api/v1/debug/processors'),
    ('/api/events', '/api/v1/events'),
    ('/api/recent-tasks', '/api/v1/recent-tasks'),
    ('/api/live/task-results', '/api/v1/live/task-results'),
    ('/api/live/message-results', '/api/v1/live/message-results'),
    ('/api/live/window-results', '/api/v1/live/window-results'),
    ('/api/debug/databases', '/api/v1/debug/databases'),
    ('/api/debug/label-keys', '/api/v1/debug/label-keys'),
    ('/api/debug/periodic', '/api/v1/debug/periodic'),
    ('/api/debug/archives', '/api/v1/debug/archives'),
]


class TestV1Aliases:
    @pytest.mark.parametrize(('legacy', 'v1'), LEGACY_TO_V1)
    async def test_v1_alias_matches_legacy_payload(self, client, legacy, v1):
        legacy_resp = await client.get(legacy)
        v1_resp = await client.get(v1)
        assert legacy_resp.status_code == 200
        assert v1_resp.status_code == 200
        legacy_json = legacy_resp.json()
        v1_json = v1_resp.json()
        if isinstance(legacy_json, dict):
            # uptime is wall-clock and differs between the two calls
            legacy_json.pop('uptime', None)
            v1_json.pop('uptime', None)
        assert v1_json == legacy_json

    async def test_download_alias_serves_file(self, tmp_path, mock_recorder, mock_app):
        (tmp_path / 'w1.db').write_bytes(b'sqlite-bytes')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            legacy = await c.get('/debug/download/w1.db')
            v1 = await c.get('/api/v1/debug/download/w1.db')
        assert legacy.status_code == 200
        assert v1.status_code == 200
        assert legacy.content == v1.content == b'sqlite-bytes'
        assert v1.headers['content-type'] == 'application/x-sqlite3'
        assert v1.headers['cache-control'] == 'no-store, private'

    async def test_archive_download_alias_serves_file(self, tmp_path, mock_recorder, mock_app):
        from drakkar.recorder.archive import archive_file_name

        name = archive_file_name('search-fleet', 0.0, 86400.0)
        (tmp_path / name).write_bytes(b'gzip-bytes')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            legacy = await c.get(f'/api/debug/archives/{name}')
            v1 = await c.get(f'/api/v1/debug/archives/{name}')
        assert legacy.status_code == 200
        assert v1.status_code == 200
        assert legacy.content == v1.content == b'gzip-bytes'
        assert v1.headers['content-type'] == 'application/gzip'
        assert v1.headers['cache-control'] == 'no-store, private'

    async def test_merge_alias_validates_body(self, client):
        resp = await client.post('/api/v1/debug/merge', json={'filenames': ['only-one.db']})
        assert resp.status_code == 400
        assert resp.json() == {'error': 'Select at least 2 databases'}

    @pytest.mark.parametrize('path', ['/api/debug/cache/stats', '/api/v1/debug/cache/stats'])
    async def test_cache_alias_404_when_disabled(self, client, path):
        resp = await client.get(path)
        assert resp.status_code == 404
        assert resp.json() == {'detail': 'Cache is disabled'}

    @pytest.mark.parametrize('path', ['/api/partitions', '/api/task/some-id', '/api/live/overview', '/api/identity'])
    async def test_new_endpoints_have_no_legacy_alias(self, client, path):
        resp = await client.get(path)
        assert resp.status_code == 404

    async def test_probes_and_pages_stay_unprefixed(self, client):
        assert (await client.get('/healthz')).status_code == 200
        assert (await client.get('/api/v1/healthz')).status_code == 404
        assert (await client.get('/api/v1/readyz')).status_code == 404


class TestV1Auth:
    V1_PROTECTED: typing.ClassVar[list[str]] = [
        '/api/v1/dashboard',
        '/api/v1/partitions',
        '/api/v1/task/t-1',
        '/api/v1/live/overview',
        '/api/v1/identity',
        '/api/v1/events',
        '/api/v1/debug/databases',
        '/api/v1/debug/download/test.db',
        '/api/v1/debug/cache/stats',
        '/api/v1/debug/archives',
        '/api/v1/debug/archives/test-2026-08-08_00-00__2026-08-09_00-00.db.gz',
    ]

    def _make_authed_client(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
        mock_recorder.config = cfg
        return make_client(cfg, mock_recorder, mock_app)

    async def test_v1_routes_require_token(self, tmp_path, mock_recorder, mock_app):
        async with self._make_authed_client(tmp_path, mock_recorder, mock_app) as c:
            for path in self.V1_PROTECTED:
                resp = await c.get(path)
                assert resp.status_code == 401, f'{path} should require auth'
                assert resp.json() == {'detail': 'Invalid or missing auth token'}

    async def test_v1_routes_accept_bearer_header(self, tmp_path, mock_recorder, mock_app):
        headers = {'Authorization': 'Bearer secret-123'}
        async with self._make_authed_client(tmp_path, mock_recorder, mock_app) as c:
            for path in ('/api/v1/dashboard', '/api/v1/partitions', '/api/v1/live/overview'):
                resp = await c.get(path, headers=headers)
                assert resp.status_code == 200, f'{path} should accept the bearer token'

    async def test_v1_routes_accept_query_param(self, tmp_path, mock_recorder, mock_app):
        async with self._make_authed_client(tmp_path, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/live/overview?token=secret-123')
            assert resp.status_code == 200

    async def test_v1_wrong_token_returns_401(self, tmp_path, mock_recorder, mock_app):
        async with self._make_authed_client(tmp_path, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/dashboard', headers={'Authorization': 'Bearer wrong'})
            assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Work package B1: GET /api/v1/partitions
# ---------------------------------------------------------------------------


class TestApiV1Partitions:
    async def test_empty_db_returns_empty_list(self, client):
        resp = await client.get('/api/v1/partitions')
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_rows_enriched_and_sorted(self, debug_config, mock_recorder, mock_app):
        # deliberately unsorted input: the endpoint must sort by partition
        mock_recorder.get_partition_summary.return_value = [
            {
                'partition': 1,
                'last_consumed': 100.0,
                'last_committed': 99.0,
                'last_committed_offset': 7,
                'consumed_count': 3,
                'completed_count': 2,
                'failed_count': 1,
            },
            {
                'partition': 0,
                'last_consumed': 50.0,
                'last_committed': 49.0,
                'last_committed_offset': 5,
                'consumed_count': 1,
                'completed_count': 1,
                'failed_count': 0,
            },
        ]
        proc = MagicMock()
        proc.queue_size = 4
        proc.offset_tracker.pending_count = 2
        mock_app.processors = {0: proc}
        mock_recorder.config = debug_config

        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/partitions')
        assert resp.status_code == 200
        rows = resp.json()
        assert [r['partition'] for r in rows] == [0, 1]

        live = rows[0]
        assert live['is_live'] is True
        assert live['queue_size'] == 4
        assert live['pending_offsets'] == 2
        # no consumer → lag falls back to the recorded committed offset
        assert live['committed_offset'] == 5
        assert live['high_watermark'] is None
        assert live['lag'] == 0

        dead = rows[1]
        assert dead['is_live'] is False
        assert dead['queue_size'] == 0
        assert dead['pending_offsets'] == 0
        # summary columns pass through unchanged
        assert dead['consumed_count'] == 3
        assert dead['failed_count'] == 1
        assert dead['last_committed_offset'] == 7

    async def test_lag_columns_from_consumer(self, debug_config, mock_recorder, mock_app):
        mock_recorder.get_partition_summary.return_value = [
            {
                'partition': 0,
                'last_consumed': 50.0,
                'last_committed': 49.0,
                'last_committed_offset': 5,
                'consumed_count': 1,
                'completed_count': 1,
                'failed_count': 0,
            },
        ]
        proc = MagicMock()
        proc.queue_size = 0
        proc.offset_tracker.pending_count = 0
        mock_app.processors = {0: proc}
        consumer = MagicMock()
        consumer.get_partition_lag = AsyncMock(return_value={0: {'committed': 10, 'high_watermark': 15, 'lag': 5}})
        mock_app._consumer = consumer
        mock_recorder.config = debug_config

        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/partitions')
        row = resp.json()[0]
        assert row['committed_offset'] == 10
        assert row['high_watermark'] == 15
        assert row['lag'] == 5


# ---------------------------------------------------------------------------
# Work package B2: GET /api/v1/task/{id}
# ---------------------------------------------------------------------------


TASK_DETAIL_KEYS = {
    'task_id',
    'events',
    'started',
    'completed',
    'failed',
    'duration',
    'source_offsets',
    'args',
    'labels',
    'task_env',
    'partition',
    'pid',
    'exit_code',
    'binary_path',
    'origin',
    'client_name',
    'request_id',
    'webapp_request_body',
    'webapp_response_body',
}


class TestApiV1TaskDetail:
    def _task_events(self, now: float) -> list[dict]:
        import json as json_mod

        return [
            {
                'id': 1,
                'ts': now - 10,
                'event': 'task_started',
                'partition': 3,
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
                'labels': '{"team": "core"}',
                'metadata': json_mod.dumps({'source_offsets': [10, 11], 'env': {'MODE': 'x'}}),
            },
            {
                'id': 2,
                'ts': now - 5,
                'event': 'task_completed',
                'partition': 3,
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
                'labels': None,
                'metadata': None,
            },
        ]

    async def test_shape_and_values(self, client, mock_recorder):
        mock_recorder.get_task_events.return_value = self._task_events(time.time())
        resp = await client.get('/api/v1/task/task-abc')
        assert resp.status_code == 200
        data = resp.json()
        assert set(data) == TASK_DETAIL_KEYS
        assert data['task_id'] == 'task-abc'
        assert data['duration'] == 5.0
        assert data['exit_code'] == 0
        assert data['partition'] == 3
        assert data['pid'] == 1234
        assert data['source_offsets'] == [10, 11]
        assert data['task_env'] == {'MODE': 'x'}
        assert data['args'] == ['--input', 'f.txt']
        assert data['labels'] == {'team': 'core'}
        assert data['origin'] == 'kafka'
        assert data['client_name'] is None
        assert data['request_id'] is None
        assert data['webapp_request_body'] is None
        assert data['webapp_response_body'] is None
        # stdout/stderr stay inside the event rows, not top-level
        assert len(data['events']) == 2
        assert data['events'][1]['stdout'] == 'output data'
        assert data['started']['event'] == 'task_started'
        assert data['completed']['event'] == 'task_completed'
        assert data['failed'] is None

    async def test_retry_suffix_stripped_for_lookup(self, client, mock_recorder):
        mock_recorder.get_task_events.return_value = []
        resp = await client.get('/api/v1/task/task-abc:r1234567.89')
        assert resp.status_code == 200
        mock_recorder.get_task_events.assert_awaited_with('task-abc')
        # the requested id (with suffix) is echoed back
        assert resp.json()['task_id'] == 'task-abc:r1234567.89'

    async def test_unknown_task_returns_null_fields(self, client, mock_recorder):
        mock_recorder.get_task_events.return_value = []
        resp = await client.get('/api/v1/task/no-such-task')
        assert resp.status_code == 200
        data = resp.json()
        assert set(data) == TASK_DETAIL_KEYS
        assert data['events'] == []
        for key in ('started', 'completed', 'failed', 'duration', 'partition', 'pid', 'exit_code'):
            assert data[key] is None, key
        assert data['origin'] == 'kafka'

    async def test_duration_computed_from_timestamps(self, client, mock_recorder):
        now = time.time()
        events = self._task_events(now)
        events[1]['duration'] = None  # force the ts-difference fallback
        mock_recorder.get_task_events.return_value = events
        resp = await client.get('/api/v1/task/task-abc')
        assert resp.json()['duration'] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Work package B3: GET /api/v1/live/overview
# ---------------------------------------------------------------------------


LIVE_OVERVIEW_KEYS = {
    'worker_id',
    'running_tasks',
    'pending_tasks',
    'arranging',
    'pool_active',
    'pool_waiting',
    'pool_max',
    'partition_count',
    'max_ui_rows',
    'ws_min_duration_ms',
    'hook_flags',
    'kafka_ui_base',
    'kafka_ui_cluster',
    'kafka_source_topic',
}


class TestApiV1LiveOverview:
    async def test_shape_with_defaults(self, client):
        resp = await client.get('/api/v1/live/overview')
        assert resp.status_code == 200
        data = resp.json()
        assert set(data) == LIVE_OVERVIEW_KEYS
        assert data['worker_id'] == 'test-worker'
        assert data['running_tasks'] == {}
        assert data['pending_tasks'] == {}
        assert data['arranging'] == []
        assert data['pool_active'] == 2
        assert data['pool_waiting'] == 1
        assert data['pool_max'] == 8
        assert data['partition_count'] == 0
        assert data['max_ui_rows'] == 5000
        assert data['ws_min_duration_ms'] == 500
        # handler=None → all hook flags off
        assert data['hook_flags'] == {
            'task_complete': False,
            'message_complete': False,
            'window_complete': False,
        }
        # Kafka-UI knobs unconfigured → empty strings; source topic has a default
        assert data['kafka_ui_base'] == ''
        assert data['kafka_ui_cluster'] == ''
        assert data['kafka_source_topic'] == 'input-events'
        # No offload pool wired → the optional key is ABSENT, not null —
        # key-presence is the feature flag the UI keys off (contract v1.10).
        assert 'offload' not in data

    async def test_offload_key_present_when_pool_wired(self, debug_config, mock_recorder, mock_app):
        offload_pool = MagicMock()
        offload_pool.snapshot.return_value = {'running': 1, 'queued': 3, 'max_threads': 2}
        mock_app._offload_pool = offload_pool
        mock_recorder.config = debug_config

        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/live/overview')
        data = resp.json()
        assert data['offload'] == {'running': 1, 'queued': 3, 'max_threads': 2}

    async def test_running_pending_split_and_arranging(self, debug_config, mock_recorder, mock_app):
        now = time.time()
        mock_recorder.get_active_tasks.return_value = [
            {'task_id': 'task-run', 'ts': now - 5, 'event': 'task_started'},
        ]
        pending_task = MagicMock()
        pending_task.args = '["--fast"]'
        pending_task.source_offsets = [10, 11]

        proc = MagicMock()
        proc.partition_id = 0
        proc._pending_tasks = {'task-run': pending_task, 'task-wait': pending_task}
        proc._arranging = True
        proc._arrange_start = now - 2.5
        proc._arrange_labels = [f'label-{i}' for i in range(12)]
        mock_app.processors = {0: proc}
        mock_recorder.config = debug_config

        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/live/overview')
        data = resp.json()
        assert set(data['running_tasks']) == {'task-run'}
        assert set(data['pending_tasks']) == {'task-wait'}
        assert data['running_tasks']['task-run'] == {
            'task_id': 'task-run',
            'args': '["--fast"]',
            'partition': 0,
            'source_offsets': [10, 11],
        }
        assert len(data['arranging']) == 1
        arrange = data['arranging'][0]
        assert arrange['partition'] == 0
        assert arrange['message_count'] == 12
        assert len(arrange['labels']) == 10  # capped at 10 labels per entry
        assert arrange['duration'] >= 2.0
        assert data['partition_count'] == 1

    async def test_hook_flags_reflect_handler_overrides(self, debug_config, mock_recorder, mock_app):
        from drakkar.handler import BaseDrakkarHandler
        from drakkar.models import CollectResult, ExecutorResult

        class H(BaseDrakkarHandler):
            async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
                return None

        mock_app.handler = H()
        mock_recorder.config = debug_config
        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/live/overview')
        assert resp.json()['hook_flags'] == {
            'task_complete': True,
            'message_complete': False,
            'window_complete': False,
        }

    async def test_kafka_ui_config_surfaces(self, debug_config, mock_recorder, mock_app):
        mock_app._config.kafka.ui_url = 'http://kafka-ui:8080/'
        mock_app._config.kafka.ui_cluster_name = 'local'
        mock_app._config.kafka.source_topic = 'events-in'
        mock_recorder.config = debug_config
        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/live/overview')
        data = resp.json()
        assert data['kafka_ui_base'] == 'http://kafka-ui:8080'  # trailing slash stripped
        assert data['kafka_ui_cluster'] == 'local'
        assert data['kafka_source_topic'] == 'events-in'


# ---------------------------------------------------------------------------
# Reconciliation 6: malformed partitions CSV → 422
# ---------------------------------------------------------------------------


class TestEventsMalformedPartitions:
    @pytest.mark.parametrize('path', ['/api/events', '/api/v1/events'])
    async def test_malformed_partitions_csv_returns_422(self, client, path):
        resp = await client.get(path, params={'partitions': '0,abc'})
        assert resp.status_code == 422
        detail = resp.json()['detail']
        assert detail[0]['loc'] == ['query', 'partitions']
        assert detail[0]['msg'] == 'Input should be a valid integer'

    async def test_valid_partitions_csv_still_ok(self, client):
        resp = await client.get('/api/events', params={'partitions': '0, 1'})
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Reconciliation 7: probe report omits traceback (decision D14)
# ---------------------------------------------------------------------------


class TestProbeErrorTracebackOmitted:
    def test_probe_error_dump_has_no_traceback_key(self):
        from drakkar.uiserver.runner_models import ProbeError

        err = ProbeError(
            stage='arrange',
            exception_class='ValueError',
            message='boom',
            traceback='Traceback (most recent call last): ...',
            occurred_at_ms=1.0,
        )
        # still captured internally (runner tests read it)
        assert err.traceback.startswith('Traceback')
        dump = err.model_dump(mode='json')
        assert 'traceback' not in dump
        assert dump['exception_class'] == 'ValueError'

    def test_debug_report_omits_traceback_everywhere(self):
        from drakkar.uiserver.runner_models import (
            DebugReport,
            ProbeError,
            ProbeInput,
            ProbeStageResult,
        )

        err = ProbeError(
            stage='deserialize',
            exception_class='ValueError',
            message='bad payload',
            traceback='tb-text',
            occurred_at_ms=0.5,
        )
        report = DebugReport(
            input=ProbeInput(value='x'),
            deserialize_error=err,
            arrange=ProbeStageResult(),
            errors=[err],
        )
        dump = report.model_dump(mode='json')
        assert 'traceback' not in dump['deserialize_error']
        assert 'traceback' not in dump['errors'][0]
        assert dump['errors'][0]['message'] == 'bad payload'


# ---------------------------------------------------------------------------
# Reconciliation 9: planned_sink_payloads includes custom sinks
# ---------------------------------------------------------------------------


class TestPlannedSinkCustomPayloads:
    def test_flatten_includes_custom_payloads(self):
        from drakkar.models import CollectResult, CustomPayload
        from drakkar.uiserver.runner import DebugSinkCollector

        class _Doc(BaseModel):
            answer: int = 42

        collector = DebugSinkCollector()
        collector.entries.append(
            ('task_complete:t-1', CollectResult(custom=[CustomPayload(sink='my-plugin', data=_Doc())])),
        )
        records = collector.flatten()
        assert len(records) == 1
        record = records[0]
        assert record.sink_type == 'custom'
        assert record.destination == 'my-plugin'
        assert record.origin_stage == 'task_complete:t-1'
        assert record.payload == {'answer': 42}
        assert record.extras == {'sink_instance': 'my-plugin'}


# ---------------------------------------------------------------------------
# Reconciliation 10: merge/download/trace/trace-by-label input hardening
# ---------------------------------------------------------------------------


class TestMergeInputHardening:
    @pytest.mark.parametrize('path', ['/api/debug/merge', '/api/v1/debug/merge'])
    async def test_malformed_json_body_returns_400(self, client, path):
        resp = await client.post(path, content=b'{not json', headers={'Content-Type': 'application/json'})
        assert resp.status_code == 400
        assert resp.json() == {'error': 'Invalid JSON body'}

    @pytest.mark.parametrize(
        'body',
        [
            ['a.db', 'b.db'],  # not an object
            {'filenames': 'a.db'},  # filenames not a list
            {'filenames': ['a.db', 5]},  # non-string entry
        ],
    )
    async def test_wrong_body_shape_returns_400(self, client, body):
        resp = await client.post('/api/debug/merge', json=body)
        assert resp.status_code == 400
        assert resp.json() == {'error': 'Invalid JSON body'}

    @pytest.mark.parametrize(
        'bad_name',
        ['evil";x.db', 'evil;.db', 'evil\n.db', 'evil\x00.db', 'evil\x7f.db'],
    )
    async def test_unsafe_filename_chars_rejected(self, client, bad_name):
        resp = await client.post('/api/debug/merge', json={'filenames': [bad_name, 'other.db']})
        assert resp.status_code == 400
        assert 'Invalid filename' in resp.json()['error']


class TestDownloadFilenameHardening:
    @pytest.mark.parametrize(
        'bad_name',
        ['evil".db', 'evil;.db', 'evil\n.db', 'evil\x1f.db', 'evil\x7f.db'],
    )
    async def test_unsafe_filename_chars_rejected(self, client, bad_name):
        resp = await client.get('/debug/download/' + quote(bad_name, safe=''))
        assert resp.status_code == 400
        assert resp.json() == {'error': 'Invalid filename'}

    async def test_v1_download_rejects_unsafe_chars_too(self, client):
        resp = await client.get('/api/v1/debug/download/' + quote('evil;.db', safe=''))
        assert resp.status_code == 400
        assert resp.json() == {'error': 'Invalid filename'}


class TestTraceInt32Range:
    @pytest.mark.parametrize('path', ['/api/debug/trace', '/api/v1/debug/trace'])
    @pytest.mark.parametrize('partition', [-1, _INT32_MAX + 1])
    async def test_out_of_range_partition_returns_422(self, client, path, partition):
        resp = await client.get(path, params={'partition': partition, 'offset': 0})
        assert resp.status_code == 422
        detail = resp.json()['detail']
        assert detail[0]['loc'] == ['query', 'partition']

    async def test_max_int32_partition_accepted(self, client):
        resp = await client.get('/api/debug/trace', params={'partition': _INT32_MAX, 'offset': 0})
        assert resp.status_code == 200
        assert resp.json() == []


class TestTraceByLabelEmptyParams:
    @pytest.mark.parametrize(
        'params',
        [
            {'key': '', 'value': 'v'},
            {'key': 'k', 'value': ''},
            {'value': 'v'},  # key missing entirely
            {'key': 'k'},  # value missing entirely
        ],
    )
    @pytest.mark.parametrize('path', ['/api/debug/trace-by-label', '/api/v1/debug/trace-by-label'])
    async def test_empty_or_missing_key_value_returns_422(self, client, path, params):
        resp = await client.get(path, params=params)
        assert resp.status_code == 422

    async def test_valid_key_value_ok(self, client):
        resp = await client.get('/api/debug/trace-by-label', params={'key': 'k', 'value': 'v'})
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Work package F2: GET /api/v1/identity (v1-only)
# ---------------------------------------------------------------------------


class TestApiV1Identity:
    async def test_shape_without_cluster(self, debug_config, mock_recorder, mock_app):
        mock_app.config_summary = '[test-worker] topic=input-events group=drakkar-workers'
        mock_recorder.config = debug_config
        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/identity')
        assert resp.status_code == 200
        data = resp.json()
        assert set(data) == {
            'worker_id',
            'cluster',
            'config_summary',
            'backend',
            'backend_version',
            'ui_version',
            'ui_source',
            'link_bases',
            'custom_renderers',
            'timeline',
        }
        assert data['worker_id'] == 'test-worker'
        assert data['cluster'] is None  # empty cluster name serializes as null
        assert data['config_summary'] == '[test-worker] topic=input-events group=drakkar-workers'
        assert data['link_bases'] == {}  # unset link_bases still reports as an empty object
        assert data['custom_renderers'] is False  # unset custom_renderers_path reports as False
        # v1.2: backend flavor + versions. Built-in pages serve in this
        # fixture, so the UI fields report the builtin fallback.
        assert data['backend'] == 'python'
        assert isinstance(data['backend_version'], str) and data['backend_version']
        assert data['ui_version'] is None
        assert data['ui_source'] == 'builtin'

    async def test_ui_fields_report_served_bundle(self, tmp_path, debug_config, mock_recorder, mock_app):
        """SPA mode: identity reports the release tag the backend serves."""
        bundle_dir = tmp_path / 'v1.0.0'
        bundle_dir.mkdir()
        (bundle_dir / 'index.html').write_text('<html></html>')
        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = debug_config
        fastapi_app = create_ui_app(
            debug_config,
            mock_recorder,
            mock_app,
            ui_root=bundle_dir,
            ui_version='v1.0.0',
        )
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as c:
            data = (await c.get('/api/v1/identity')).json()
        assert data['ui_version'] == 'v1.0.0'
        assert data['ui_source'] == 'release'

    async def test_ui_fields_report_embedded_bundle(self, tmp_path, debug_config, mock_recorder, mock_app):
        """The package-baked bundle reports ui_source='embedded' (v1.2)."""
        from drakkar.uihost import EMBEDDED_BUNDLE_DIR, embedded_bundle_version

        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = debug_config
        fastapi_app = create_ui_app(
            debug_config,
            mock_recorder,
            mock_app,
            ui_root=EMBEDDED_BUNDLE_DIR,
            ui_version=embedded_bundle_version(),
            ui_source='embedded',
        )
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as c:
            data = (await c.get('/api/v1/identity')).json()
            index = await c.get('/')
        assert data['ui_source'] == 'embedded'
        # The embedded bundle is a REAL drakkar-ui release: it carries its
        # tag (from the VERSION file `just embed-ui` writes) and serves a
        # built SPA index, not the retired stub page.
        assert data['ui_version'] == embedded_bundle_version()
        assert data['ui_version'] is not None
        assert index.status_code == 200
        assert 'assets/' in index.text

    async def test_cluster_name_surfaces(self, debug_config, mock_recorder, mock_app):
        mock_app._cluster_name = 'main'
        mock_app.config_summary = '[test-worker/main] topic=input-events'
        mock_recorder.config = debug_config
        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/identity')
        assert resp.json()['cluster'] == 'main'

    async def test_requires_auth_when_token_set(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            assert (await c.get('/api/v1/identity')).status_code == 401
            ok = await c.get('/api/v1/identity', headers={'Authorization': 'Bearer secret-123'})
            assert ok.status_code == 200

    async def test_identity_includes_link_bases(self, client_with_link_bases):
        res = await client_with_link_bases.get('/api/v1/identity')
        assert res.status_code == 200
        assert res.json()['link_bases'] == {'jira': 'https://jira.internal.example.com'}

    async def test_identity_reports_custom_renderers_when_configured(self, tmp_path, mock_recorder, mock_app):
        renderers_path = tmp_path / 'custom-renderers.js'
        renderers_path.write_text('export default {}\n')
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), custom_renderers_path=str(renderers_path))
        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/identity')
        assert resp.json()['custom_renderers'] is True

    async def test_identity_reports_default_timeline(self, debug_config, mock_recorder, mock_app):
        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = debug_config
        async with make_client(debug_config, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/identity')
        timeline = resp.json()['timeline']
        assert timeline['color_rules'] == []
        assert timeline['labels'] == {}

    async def test_identity_reports_configured_timeline(self, tmp_path, mock_recorder, mock_app):
        timeline_cfg = UITimelineConfig(
            history_factor=3,
            max_age_minutes=45,
            color_rules=[
                # A single-condition rule constructed from a bare dict, to
                # pin that ``when`` always serializes as a list even though
                # config-loading coerces one condition into a one-item list.
                TimelineColorRule(when={'label': 'priority', 'op': 'eq', 'value': 'high'}, color='red'),
                TimelineColorRule(
                    name='slow',
                    when=[TimelineRuleCondition(field='duration', op='gt', value=5)],
                    color='#336699',
                ),
            ],
            labels=TimelineLabels(tag='env', marker='urgent'),
        )
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), timeline=timeline_cfg)
        mock_app.config_summary = '[test-worker]'
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/identity')
        assert resp.json()['timeline'] == {
            'history_factor': 3,
            'max_age_minutes': 45,
            'color_rules': [
                {'name': '', 'when': [{'label': 'priority', 'op': 'eq', 'value': 'high'}], 'color': 'red'},
                {'name': 'slow', 'when': [{'field': 'duration', 'op': 'gt', 'value': 5}], 'color': '#336699'},
            ],
            'labels': {'tag': 'env', 'marker': 'urgent'},
        }


# ---------------------------------------------------------------------------
# Declared UI pages: GET /api/v1/pages (v1-only)
# ---------------------------------------------------------------------------


class TestApiV1Pages:
    async def test_pages_endpoint_returns_declared_pages(self, client_with_pages):
        res = await client_with_pages.get('/api/v1/pages')
        assert res.status_code == 200
        body = res.json()
        assert [p['slug'] for p in body] == ['orders']
        assert body[0]['widgets'][0]['source']['kind'] == 'annotations'

    async def test_pages_endpoint_empty_without_declarations(self, client):
        res = await client.get('/api/v1/pages')
        assert res.status_code == 200
        assert res.json() == []

    async def test_pages_endpoint_requires_auth_when_token_set(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            assert (await c.get('/api/v1/pages')).status_code == 401
            ok = await c.get('/api/v1/pages', headers={'Authorization': 'Bearer secret-123'})
            assert ok.status_code == 200

    async def test_pages_endpoint_has_no_legacy_alias(self, client):
        assert (await client.get('/api/pages')).status_code == 404


# ---------------------------------------------------------------------------
# Work package F1: optional `links` key on GET /api/v1/dashboard
# ---------------------------------------------------------------------------


class TestDashboardLinks:
    @pytest.mark.parametrize('path', ['/api/dashboard', '/api/v1/dashboard'])
    async def test_links_absent_when_unconfigured(self, client, path):
        resp = await client.get(path)
        assert resp.status_code == 200
        assert 'links' not in resp.json()

    async def test_links_present_with_prometheus_url(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), prometheus_url='http://prom:9090')
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/dashboard')
        links = resp.json()['links']
        assert set(links) == {'card_links', 'worker_links', 'cluster_links', 'custom_links'}
        assert set(links['card_links']) == {'lag', 'consumed', 'completed', 'failed', 'produced'}
        for url in links['card_links'].values():
            assert url.startswith('http://prom:9090/graph?')
        # worker links: grouped {category, links:[[name, url], ...]} cards
        categories = [group['category'] for group in links['worker_links']]
        assert categories == ['Throughput', 'Latency', 'Health', 'Errors']
        for group in links['worker_links']:
            for entry in group['links']:
                name, url = entry  # tuples serialize as 2-element arrays
                assert isinstance(name, str)
                assert url.startswith('http://prom:9090/graph?')
        # no cluster label configured → no cluster links; no custom links
        assert links['cluster_links'] == []
        assert links['custom_links'] == []

    async def test_cluster_links_with_cluster_label(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(
            enabled=True,
            port=8080,
            db_dir=str(tmp_path),
            prometheus_url='http://prom:9090',
            prometheus_cluster_label='cluster="main"',
        )
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/dashboard')
        cluster_links = resp.json()['links']['cluster_links']
        assert cluster_links, 'cluster label configured — cluster links expected'
        for entry in cluster_links:
            name, url = entry
            assert isinstance(name, str)
            assert url.startswith('http://prom:9090/graph?')

    async def test_custom_links_alone_enable_key_and_pass_verbatim(self, tmp_path, mock_recorder, mock_app):
        custom = [{'name': 'Grafana', 'url': 'http://grafana/{worker_id}'}]
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), custom_links=custom)
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            resp = await c.get('/api/v1/dashboard')
        links = resp.json()['links']
        # prometheus unconfigured → empty prometheus containers, custom verbatim
        assert links['card_links'] == {}
        assert links['worker_links'] == []
        assert links['cluster_links'] == []
        assert links['custom_links'] == custom  # URL templates NOT expanded

    async def test_legacy_alias_carries_links_too(self, tmp_path, mock_recorder, mock_app):
        cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), prometheus_url='http://prom:9090')
        mock_recorder.config = cfg
        async with make_client(cfg, mock_recorder, mock_app) as c:
            legacy = await c.get('/api/dashboard')
            v1 = await c.get('/api/v1/dashboard')
        assert legacy.json()['links'] == v1.json()['links']
