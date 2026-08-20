"""Tests for ``GET /api/v1/config-reference``.

Covers the response shape, secret masking (including the ``webapp.clients``
special case — its ``token`` has no per-element metadata path, see
``drakkar.uiserver.routes_config_reference``), ``is_default`` computation,
dynamic-instance expansion (the ``*`` template entry plus one entry per
configured sink instance), and env-var passthrough for static fields.

Fully isolated: no real database, no network, no docker — the recorder is
an ``AsyncMock`` never actually queried by this endpoint, and the app
config is a real (in-memory) ``DrakkarConfig`` built directly in each test.
"""

import time
from unittest.mock import AsyncMock, MagicMock

from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, KafkaSinkConfig, PostgresSinkConfig, WebClientConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver.routes_config_reference import SECRET_MASK
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_app(config: DrakkarConfig) -> MagicMock:
    """Build a MagicMock ``DrakkarApp`` stand-in carrying a real ``DrakkarConfig``.

    Mirrors ``mock_app`` in ``tests/test_debug_api_v1.py`` — only the bits
    the config-reference endpoint and its router-mounting code path touch.
    """
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


async def get_config_reference(config: DrakkarConfig) -> dict:
    """Round-trip ``config`` through a real ASGI request and return the parsed JSON body.

    Reuses ``config.ui`` as the debug-server's own ``UIConfig`` (rather than
    building an unrelated standalone one) so a test that sets
    ``config.ui.auth_token`` gets a server that is actually gated by that
    same token — mirroring how ``UIServer.start()`` wires the real app.
    """
    ui_config = config.ui
    recorder = AsyncMock(spec=EventRecorder)
    recorder._db = None
    recorder._reader_db = None
    recorder.reader_db = None
    recorder.config = ui_config
    app = make_app(config)
    fastapi_app = create_ui_app(ui_config, recorder, app)
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as client:
        headers = {}
        if ui_config.auth_token:
            headers['Authorization'] = f'Bearer {ui_config.auth_token}'
        resp = await client.get('/api/v1/config-reference', headers=headers)
    assert resp.status_code == 200, resp.text
    return resp.json()


def all_entries(body: dict) -> list[dict]:
    return [entry for group in body['groups'] for entry in group['entries']]


def entries_by_path(body: dict, path: str) -> list[dict]:
    return [e for e in all_entries(body) if e['path'] == path]


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


class TestShape:
    async def test_response_has_expected_groups(self):
        body = await get_config_reference(DrakkarConfig())
        group_keys = [g['key'] for g in body['groups']]
        assert group_keys == [
            'root',
            'kafka',
            'executor',
            'sinks',
            'dlq',
            'metrics',
            'throughput',
            'runtime_health',
            'offload',
            'logging',
            'ui',
            'cache',
            'webapp',
        ]

    async def test_group_has_title_and_doc_anchor(self):
        body = await get_config_reference(DrakkarConfig())
        kafka_group = next(g for g in body['groups'] if g['key'] == 'kafka')
        assert kafka_group['title']
        assert kafka_group['doc_anchor']

    async def test_entry_has_every_field(self):
        body = await get_config_reference(DrakkarConfig())
        entry = entries_by_path(body, 'kafka.brokers')[0]
        assert set(entry) == {
            'path',
            'env',
            'description',
            'full_description',
            'type',
            'value',
            'default',
            'is_default',
            'secret',
        }

    async def test_requires_auth_when_token_configured(self):
        ui_config = make_ui_config(enabled=True, port=8080, auth_token='top-secret')
        recorder = AsyncMock(spec=EventRecorder)
        recorder._db = None
        recorder._reader_db = None
        recorder.reader_db = None
        recorder.config = ui_config
        app = make_app(DrakkarConfig())
        fastapi_app = create_ui_app(ui_config, recorder, app)
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as client:
            resp = await client.get('/api/v1/config-reference')
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------


class TestMasking:
    async def test_configured_ui_auth_token_is_masked(self):
        config = DrakkarConfig()
        config.ui.auth_token = 'super-secret-token'
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'ui.auth_token')[0]
        assert entry['value'] == SECRET_MASK
        assert 'super-secret-token' not in body_as_text(body)

    async def test_unset_secret_is_empty_and_default(self):
        body = await get_config_reference(DrakkarConfig())
        entry = entries_by_path(body, 'ui.auth_token')[0]
        assert entry['value'] == ''
        assert entry['is_default'] is True

    async def test_webapp_client_token_is_masked(self):
        config = DrakkarConfig()
        config.webapp.clients = [
            WebClientConfig(name='tenant-a', token='tenant-a-bearer-token', rpm=10),
            WebClientConfig(name='anonymous', token='', rpm=4),
        ]
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'webapp.clients')[0]
        tokens = [c['token'] for c in entry['value']]
        assert tokens == [SECRET_MASK, '']
        assert 'tenant-a-bearer-token' not in body_as_text(body)

    async def test_configured_kafka_sink_password_is_masked(self):
        from pydantic import SecretStr

        config = DrakkarConfig()
        config.sinks.kafka['results'] = KafkaSinkConfig(topic='results-topic')
        config.sinks.kafka['results'].security.sasl_password = SecretStr('kafka-sasl-secret')
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'sinks.kafka.results.security.sasl_password')[0]
        assert entry['value'] == SECRET_MASK
        assert 'kafka-sasl-secret' not in body_as_text(body)

    async def test_configured_postgres_dsn_is_masked_and_never_leaks_raw(self):
        """dsn is a plain ``str`` field (drakkar_secret), not a pydantic SecretStr.

        Unlike ``sasl_password`` above — where pydantic's own SecretStr
        serializer already masks the value before this endpoint's code ever
        runs — a plain ``str`` secret field reaches
        ``model_dump(mode='json')`` with the raw credential intact. This
        endpoint's own masking is the ONLY thing standing between that raw
        DSN and the response body, so this test asserts the real string
        (not just the empty default) never appears anywhere in the full
        serialized response, not merely in the one field we'd think to check.
        """
        raw_dsn = 'postgresql://svc_user:hunter2@db.internal:5432/main'
        config = DrakkarConfig()
        config.sinks.postgres['main-db'] = PostgresSinkConfig(dsn=raw_dsn)
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'sinks.postgres.main-db.dsn')[0]
        assert entry['value'] == SECRET_MASK
        assert entry['secret'] is True
        assert raw_dsn not in body_as_text(body)
        assert 'hunter2' not in body_as_text(body)


def body_as_text(body: dict) -> str:
    import json

    return json.dumps(body)


# ---------------------------------------------------------------------------
# is_default
# ---------------------------------------------------------------------------


class TestIsDefault:
    async def test_default_field_reports_true(self):
        body = await get_config_reference(DrakkarConfig())
        entry = entries_by_path(body, 'executor.max_executors')[0]
        assert entry['value'] == entry['default'] == 4
        assert entry['is_default'] is True

    async def test_non_default_field_reports_false(self):
        config = DrakkarConfig()
        config.executor.max_executors = 16
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'executor.max_executors')[0]
        assert entry['value'] == 16
        assert entry['default'] == 4
        assert entry['is_default'] is False


# ---------------------------------------------------------------------------
# Dynamic expansion
# ---------------------------------------------------------------------------


class TestDynamicExpansion:
    async def test_two_kafka_sinks_yield_two_entries_plus_template(self):
        config = DrakkarConfig()
        config.sinks.kafka['results'] = KafkaSinkConfig(topic='results-topic')
        config.sinks.kafka['audit'] = KafkaSinkConfig(topic='audit-topic')
        body = await get_config_reference(config)

        template = entries_by_path(body, 'sinks.kafka.*.topic')
        assert len(template) == 1
        assert template[0]['value'] is None
        assert template[0]['is_default'] is True

        results = entries_by_path(body, 'sinks.kafka.results.topic')
        audit = entries_by_path(body, 'sinks.kafka.audit.topic')
        assert len(results) == 1
        assert len(audit) == 1
        assert results[0]['value'] == 'results-topic'
        assert audit[0]['value'] == 'audit-topic'
        assert results[0]['env'] is None  # dynamic paths carry no env var

    async def test_template_entry_present_with_zero_instances(self):
        body = await get_config_reference(DrakkarConfig())
        template = entries_by_path(body, 'sinks.postgres.*.dsn')
        assert len(template) == 1
        assert template[0]['value'] is None
        assert template[0]['is_default'] is True
        # No configured postgres instances -> no expanded (non-template)
        # entries under sinks.postgres.* at all.
        concrete_postgres_entries = [
            e['path']
            for group in body['groups']
            for e in group['entries']
            if e['path'].startswith('sinks.postgres.') and '*' not in e['path']
        ]
        assert concrete_postgres_entries == []


# ---------------------------------------------------------------------------
# Env passthrough
# ---------------------------------------------------------------------------


class TestEnvPassthrough:
    async def test_kafka_brokers_reports_its_env_var(self):
        body = await get_config_reference(DrakkarConfig())
        entry = entries_by_path(body, 'kafka.brokers')[0]
        assert entry['env'] == 'DK_KAFKA__BROKERS'
