"""Tests for ``GET /api/v1/config-reference``.

Covers the response shape, secret masking (including the ``sources.http.clients``
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

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from drakkar.config import (
    DrakkarConfig,
    KafkaSinkConfig,
    PostgresConnectionConfig,
    PostgresSinkConfig,
    WebClientConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.recorder import EventRecorder
from drakkar.uiserver.routes_config_reference import SECRET_MASK
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_app(config: DrakkarConfig, handler: object = None) -> MagicMock:
    """Build a MagicMock ``DrakkarApp`` stand-in carrying a real ``DrakkarConfig``.

    Mirrors ``mock_app`` in ``tests/test_debug_api_v1.py`` — only the bits
    the config-reference endpoint and its router-mounting code path touch.
    ``handler`` feeds the runtime app-config group; ``None`` (the default)
    exercises the no-handler path most endpoint tests want.
    """
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 60
    app.processors = {}
    app._config = config
    app._config.ui.release.enabled = False
    app.cache_engine = None
    app.handler = handler
    app._consumer = None
    return app


async def get_config_reference(config: DrakkarConfig, handler: object = None) -> dict:
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
    app = make_app(config, handler=handler)
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
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        group_keys = [g['key'] for g in body['groups']]
        assert group_keys == [
            'root',
            'kafka',
            'sources',
            'executor',
            'sinks',
            'dlq',
            'metrics',
            'throughput',
            'runtime_health',
            'io',
            'offload',
            'logging',
            'ui',
            'cache',
            'storage',
            'shared',
            'history',
            'periodic',
            'fleet',
            'connections',
        ]

    async def test_group_has_title_and_doc_anchor(self):
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        kafka_group = next(g for g in body['groups'] if g['key'] == 'kafka')
        assert kafka_group['title']
        assert kafka_group['doc_anchor']

    async def test_entry_has_every_field(self):
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
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
        app = make_app(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        fastapi_app = create_ui_app(ui_config, recorder, app)
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as client:
            resp = await client.get('/api/v1/config-reference')
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------


class TestMasking:
    async def test_configured_ui_auth_token_is_masked(self):
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.ui.auth_token = 'super-secret-token'
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'ui.auth_token')[0]
        assert entry['value'] == SECRET_MASK
        assert 'super-secret-token' not in body_as_text(body)

    async def test_unset_secret_is_empty_and_default(self):
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        entry = entries_by_path(body, 'ui.auth_token')[0]
        assert entry['value'] == ''
        assert entry['is_default'] is True

    async def test_http_source_client_token_is_masked(self):
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.sources.http.clients = [
            WebClientConfig(name='tenant-a', token='tenant-a-bearer-token', rpm=10),
            WebClientConfig(name='anonymous', token='', rpm=4),
        ]
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'sources.http.clients')[0]
        tokens = [c['token'] for c in entry['value']]
        assert tokens == [SECRET_MASK, '']
        assert 'tenant-a-bearer-token' not in body_as_text(body)

    async def test_executor_env_values_are_masked_by_key_name(self):
        """``executor.env`` is the documented way to hand credentials to the
        handler binary, and the config page returned it verbatim.

        The recorder already sanitizes the SAME env by key name before
        storing it, so the two surfaces disagreed about what is secret.
        """
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.executor.env = {
            'MY_API_KEY': 'sk-live-abcdef',
            'DB_PASSWORD': 'hunter2',
            'AWS_SECRET_ACCESS_KEY': 'wJalrXUtnFEMI',
            'LOG_LEVEL': 'debug',
        }
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'executor.env')[0]
        assert entry['value']['MY_API_KEY'] == '***'
        assert entry['value']['DB_PASSWORD'] == '***'
        assert entry['value']['AWS_SECRET_ACCESS_KEY'] == '***'
        # Non-secret keys stay readable — the page is an operator tool.
        assert entry['value']['LOG_LEVEL'] == 'debug'
        text = body_as_text(body)
        for secret in ('sk-live-abcdef', 'hunter2', 'wJalrXUtnFEMI'):
            assert secret not in text

    async def test_executor_env_url_credentials_are_redacted(self):
        """A key name that looks innocent can still hold a DSN."""
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.executor.env = {'UPSTREAM': 'postgres://user:s3cr3t@db:5432/app'}
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'executor.env')[0]
        assert 's3cr3t' not in entry['value']['UPSTREAM']
        assert 's3cr3t' not in body_as_text(body)

    @pytest.mark.parametrize(
        'path',
        ['kafka.client_config', 'dlq.client_config'],
    )
    async def test_client_config_secrets_are_masked(self, path):
        """librdkafka passthrough can carry sasl.password / ssl.key.password
        — only four keys are reserved, so anything may appear here."""
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        section = config.kafka if path.startswith('kafka') else config.dlq
        section.client_config = {
            'sasl.password': 'broker-secret',
            'ssl.key.password': 'key-secret',
            'socket.timeout.ms': '5000',
        }
        body = await get_config_reference(config)
        entry = entries_by_path(body, path)[0]
        assert entry['value']['sasl.password'] == '***'
        assert entry['value']['ssl.key.password'] == '***'
        assert entry['value']['socket.timeout.ms'] == '5000'
        text = body_as_text(body)
        assert 'broker-secret' not in text
        assert 'key-secret' not in text

    async def test_kafka_sink_client_config_secrets_are_masked(self):
        """The dynamic (wildcard) path must be covered too."""
        from drakkar.config import KafkaSinkConfig

        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.sinks.kafka = {
            'primary_output_topic': KafkaSinkConfig(
                topic='out',
                client_config={'sasl.password': 'sink-secret', 'acks': 'all'},
            )
        }
        body = await get_config_reference(config)
        entry = entries_by_path(body, 'sinks.kafka.primary_output_topic.client_config')[0]
        assert entry['value']['sasl.password'] == '***'
        assert entry['value']['acks'] == 'all'
        assert 'sink-secret' not in body_as_text(body)

    async def test_plugin_options_are_masked_like_executor_env(self):
        """A provider plugin takes its credentials through ``plugin_options``, a free map like ``executor.env``."""
        options = {'token': 'abc', 'url': 'redis://:pw@h/0'}
        config = DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            fleet={'registry': {'plugin_options': options}},
            periodic={'election': {'plugin_options': options}},
        )
        config.executor.env = dict(options)
        body = await get_config_reference(config)
        env_value = entries_by_path(body, 'executor.env')[0]['value']

        assert env_value == {'token': '***', 'url': 'redis://***:***@h/0'}
        for path in ('fleet.registry.plugin_options', 'periodic.election.plugin_options'):
            assert entries_by_path(body, path)[0]['value'] == env_value
        assert ':pw@' not in body_as_text(body)

    async def test_nested_plugin_options_are_masked_at_every_depth(self):
        """Plugin options are free-form, so a secret can sit in a nested mapping or in a list of mappings."""
        options = {
            'pool': {'password': 'nested-secret', 'user': 'svc', 'size': 4},
            'credentials': {'value': 'below-a-secret-key'},
            'servers': [{'url': 'redis://:list-secret@h1/0'}, 'redis://:bare-secret@h2/0'],
            'api_keys': ['key-secret'],
            'port': 6379,
        }
        config = DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            fleet={'registry': {'plugin_options': options}},
            periodic={'election': {'plugin_options': options}},
        )
        body = await get_config_reference(config)

        for path in ('fleet.registry.plugin_options', 'periodic.election.plugin_options'):
            assert entries_by_path(body, path)[0]['value'] == {
                'pool': {'password': '***', 'user': 'svc', 'size': 4},
                # Everything below a secret-looking key is masked, whatever its own key.
                'credentials': {'value': '***'},
                'servers': [{'url': 'redis://***:***@h1/0'}, 'redis://***:***@h2/0'],
                'api_keys': ['***'],
                'port': 6379,
            }
        text = body_as_text(body)
        for secret in ('nested-secret', 'below-a-secret-key', 'list-secret', 'bare-secret', 'key-secret'):
            assert secret not in text

    async def test_numbers_and_booleans_under_a_secret_looking_plugin_option_key_are_masked(self):
        """A secret written as a YAML number or boolean is still a secret; ``None`` and other keys keep their value."""
        options = {
            'token': 123456,
            'enabled': True,
            'auth': {'count': 0, 'strict': False},
            'password': None,
            'port': 6379,
            'api_keys': [1234, 5678],
        }
        config = DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            fleet={'registry': {'plugin_options': options}},
            periodic={'election': {'plugin_options': options}},
        )
        body = await get_config_reference(config)

        for path in ('fleet.registry.plugin_options', 'periodic.election.plugin_options'):
            assert entries_by_path(body, path)[0]['value'] == {
                'token': '***',
                'enabled': True,
                'auth': {'count': '***', 'strict': '***'},
                'password': None,
                'port': 6379,
                'api_keys': ['***', '***'],
            }
        assert '123456' not in body_as_text(body)

    async def test_configured_kafka_sink_password_is_masked(self):
        from pydantic import SecretStr

        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
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
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
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
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        entry = entries_by_path(body, 'executor.max_executors')[0]
        assert entry['value'] == entry['default'] == 4
        assert entry['is_default'] is True

    async def test_non_default_field_reports_false(self):
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
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
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
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
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
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
# Fields with an alias are reported under their YAML key
# ---------------------------------------------------------------------------


class TestAliasedFields:
    async def test_the_live_schema_of_a_connection_is_answered_under_the_yaml_key(self):
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.connections.postgres['main'] = PostgresConnectionConfig(
            dsn='postgresql://u:pw@h/db', schema_name='audit'
        )
        body = await get_config_reference(config)

        entry = entries_by_path(body, 'connections.postgres.main.schema')[0]
        assert entry['value'] == 'audit'
        assert entry['default'] == 'drakkar'
        assert entry['is_default'] is False
        assert entry['env'] is None  # dynamic paths carry no env var
        assert entries_by_path(body, 'connections.postgres.*.schema')[0]['value'] is None
        assert not [e['path'] for e in all_entries(body) if 'schema_name' in e['path']]

    async def test_the_secret_of_a_connection_is_still_masked(self):
        config = DrakkarConfig(sources={'kafka': {'enabled': True}})
        config.connections.postgres['main'] = PostgresConnectionConfig(dsn='postgresql://u:hunter2@h/db')
        body = await get_config_reference(config)

        assert entries_by_path(body, 'connections.postgres.main.dsn')[0]['value'] == SECRET_MASK
        assert 'hunter2' not in str(body)

    async def test_an_aliased_field_of_the_app_config_is_answered_under_its_yaml_key(self):
        class AliasedAppConfig(BaseModel):
            model_config = ConfigDict(populate_by_name=True)

            schema_name: str = Field(default='drakkar', alias='schema')

        class AliasedHandler(BaseDrakkarHandler):
            app_config_model = AliasedAppConfig
            app_env_prefix = 'MYAPP_'

            async def arrange(self, messages, pending):
                return []

        handler = AliasedHandler()
        handler._app_config = AliasedAppConfig(schema_name='audit')
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)

        entry = entries_by_path(body, 'app.schema')[0]
        assert entry['value'] == 'audit'
        assert entry['env'] == 'MYAPP_SCHEMA'
        assert entry['is_default'] is False


class _KeyedSection(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    section_name: str = Field(default='s', alias='section')


class _KeyedAppConfig(BaseModel):
    """App config whose fields have a serialization alias, a validation alias, or both."""

    model_config = ConfigDict(populate_by_name=True)

    only_serialized: str = Field(default='a', serialization_alias='only_serialized_out')
    both_aliases: str = Field(default='b', alias='in_key', serialization_alias='out_key')
    nested: _KeyedSection = Field(default_factory=_KeyedSection)
    by_name: dict[str, _KeyedSection] = Field(default_factory=dict)


class TestSerializationAliasFields:
    async def test_every_row_of_an_app_config_is_present_under_its_yaml_key(self):
        class KeyedHandler(BaseDrakkarHandler):
            app_config_model = _KeyedAppConfig
            app_env_prefix = 'MYAPP_'

            async def arrange(self, messages, pending):
                return []

        handler = KeyedHandler()
        handler._app_config = _KeyedAppConfig(
            only_serialized='x',
            both_aliases='y',
            nested=_KeyedSection(section_name='n'),
            by_name={'one': _KeyedSection(section_name='o')},
        )
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)

        values = {
            path: entries_by_path(body, path)[0]['value']
            for path in ('app.only_serialized', 'app.in_key', 'app.nested.section', 'app.by_name.one.section')
        }
        assert values == {
            'app.only_serialized': 'x',
            'app.in_key': 'y',
            'app.nested.section': 'n',
            'app.by_name.one.section': 'o',
        }
        assert entries_by_path(body, 'app.in_key')[0]['env'] == 'MYAPP_IN_KEY'
        assert {entry['path'] for entry in body['groups'][-1]['entries']} == {
            'app.only_serialized',
            'app.in_key',
            'app.nested.section',
            'app.by_name.*.section',
            'app.by_name.one.section',
        }


# ---------------------------------------------------------------------------
# Env passthrough
# ---------------------------------------------------------------------------


class TestEnvPassthrough:
    async def test_kafka_brokers_reports_its_env_var(self):
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        entry = entries_by_path(body, 'kafka.brokers')[0]
        assert entry['env'] == 'DK_KAFKA__BROKERS'


# ---------------------------------------------------------------------------
# Runtime app-config group (docs/app-config.md, contract v1.17)
# ---------------------------------------------------------------------------


class ScoringSection(BaseModel):
    """Nested model inside the demo app config."""

    url: str = 'http://localhost:9000/score'
    timeout_seconds: int = 5


class ReferenceAppConfig(BaseModel):
    """Demo user model: descriptions, both secret conventions, nesting."""

    priority_threshold: int = Field(
        default=10,
        description='Tasks scoring above this are prioritized. Everything else queues normally.',
    )
    api_key: SecretStr = SecretStr('')
    webhook_token: str = Field(default='', json_schema_extra={'drakkar_secret': True})
    scoring: ScoringSection = Field(default_factory=ScoringSection)


class AppConfigHandler(BaseDrakkarHandler):
    """Handler declaring the app config, with a loaded instance attached
    the way ``DrakkarApp._wire_app_config`` attaches it."""

    app_config_model = ReferenceAppConfig
    app_env_prefix = 'MYAPP_'

    async def arrange(self, messages, pending):
        return []


def make_app_config_handler(instance: ReferenceAppConfig) -> AppConfigHandler:
    handler = AppConfigHandler()
    handler._app_config = instance
    return handler


class TestAppConfigGroup:
    async def test_group_present_with_expected_identity(self):
        handler = make_app_config_handler(ReferenceAppConfig())
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        group = body['groups'][-1]
        assert group['key'] == 'app'
        assert group['title'] == 'Application'
        assert group['doc_anchor'] == 'app-config'

    async def test_group_absent_when_no_handler(self):
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        assert 'app' not in [g['key'] for g in body['groups']]

    async def test_group_absent_when_handler_declares_no_model(self):
        class PlainHandler(BaseDrakkarHandler):
            async def arrange(self, messages, pending):
                return []

        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=PlainHandler())
        assert 'app' not in [g['key'] for g in body['groups']]

    async def test_entries_carry_app_prefixed_paths_and_user_env_names(self):
        handler = make_app_config_handler(ReferenceAppConfig())
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        entry = entries_by_path(body, 'app.priority_threshold')[0]
        assert entry['env'] == 'MYAPP_PRIORITY_THRESHOLD'
        assert entry['type'] == 'integer'
        assert entry['description'] == 'Tasks scoring above this are prioritized.'
        assert entry['full_description'].endswith('Everything else queues normally.')

    async def test_nested_model_walks_with_double_underscore_env(self):
        handler = make_app_config_handler(ReferenceAppConfig(scoring=ScoringSection(url='http://scoring-svc:9000')))
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        url_entry = entries_by_path(body, 'app.scoring.url')[0]
        assert url_entry['env'] == 'MYAPP_SCORING__URL'
        assert url_entry['value'] == 'http://scoring-svc:9000'
        assert url_entry['is_default'] is False
        timeout_entry = entries_by_path(body, 'app.scoring.timeout_seconds')[0]
        assert timeout_entry['value'] == 5
        assert timeout_entry['is_default'] is True

    async def test_secretstr_field_is_masked_and_never_leaks_raw(self):
        handler = make_app_config_handler(ReferenceAppConfig(api_key=SecretStr('raw-secret-key')))
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        entry = entries_by_path(body, 'app.api_key')[0]
        assert entry['secret'] is True
        assert entry['value'] == SECRET_MASK
        # is_default computed BEFORE masking: the live value differs from
        # the (empty) default even though both would mask identically.
        assert entry['is_default'] is False
        assert 'raw-secret-key' not in str(body)

    async def test_drakkar_secret_marker_field_is_masked(self):
        handler = make_app_config_handler(ReferenceAppConfig(webhook_token='raw-webhook-token'))
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        entry = entries_by_path(body, 'app.webhook_token')[0]
        assert entry['secret'] is True
        assert entry['value'] == SECRET_MASK
        assert 'raw-webhook-token' not in str(body)

    async def test_unset_secret_stays_visible_as_empty_and_default(self):
        handler = make_app_config_handler(ReferenceAppConfig())
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        entry = entries_by_path(body, 'app.api_key')[0]
        assert entry['value'] == ''
        assert entry['is_default'] is True

    async def test_default_scalar_reports_is_default_true(self):
        handler = make_app_config_handler(ReferenceAppConfig())
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        entry = entries_by_path(body, 'app.priority_threshold')[0]
        assert entry['value'] == 10
        assert entry['default'] == 10
        assert entry['is_default'] is True

    async def test_framework_groups_unchanged_by_app_group(self):
        """Adding the runtime group appends data — the 14 static groups keep
        their keys and order."""
        handler = make_app_config_handler(ReferenceAppConfig())
        body = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}), handler=handler)
        static_keys = [g['key'] for g in body['groups'][:-1]]
        baseline = await get_config_reference(DrakkarConfig(sources={'kafka': {'enabled': True}}))
        assert static_keys == [g['key'] for g in baseline['groups']]
