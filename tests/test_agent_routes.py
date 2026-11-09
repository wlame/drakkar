"""The agent routes: the switch, the token, the parameters, the index, the manual, the config and the metrics."""

from __future__ import annotations

import asyncio
import datetime
import json
import math
import os
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from pydantic import BaseModel, Field, SecretStr

import drakkar as dk
from drakkar.agent.config_view import (
    SECRET_KEY_WORDS,
    SECRET_TEXT,
    AppConfigView,
    ConfigLeaf,
    ConfigSources,
    advice_of,
    app_entries,
    expand_path,
    framework_entries,
    is_secret_app_path,
    mask_value,
    masked_text,
    value_text,
)
from drakkar.agent.metrics_view import (
    MAX_SAMPLES,
    MetricNotes,
    MetricsQuery,
    bound_text,
    exposed_name,
    json_number,
    metrics_answer,
)
from drakkar.agent.params import CONFIG_PARTS, MANUAL_PARTS, START_HERE, STATE_PARTS
from drakkar.agent.registry import (
    approx_tokens,
    dumps,
    load_registry,
    select_manual,
    selection_json,
)
from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from drakkar.uiserver.routes_agent import AgentSurface, token_matches
from drakkar.uiserver.server import create_api_app, create_ui_app
from tests.conftest import kafka_connections
from tests.test_sources_validation import In, KafkaOnlyHandler, Out

CONTRACT = json.loads((Path(__file__).parent / 'fixtures' / 'agent_contract.json').read_text(encoding='utf-8'))
MESSAGES = CONTRACT['messages']
TOKEN = 'agent-secret-token'
AGENT_GET_PATHS = ['/api/v1/agent', '/api/v1/agent/manual', '/api/v1/agent/config', '/api/v1/agent/metrics']


def _config(**sections: Any) -> DrakkarConfig:
    values: dict[str, Any] = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'ui': {'enabled': False, 'release': {'enabled': False}},
        'agent': {'enabled': True},
    }
    return DrakkarConfig(**{**values, **sections})


def _drakkar_app(config: DrakkarConfig) -> MagicMock:
    app = MagicMock()
    app._config = config
    app._worker_id = 'worker-a'
    app._cluster_name = 'cluster-a'
    app._instance = '0123456789abcdef'
    app._recorder = None
    app._config_origins = None
    app._config_before_on_startup = None
    app.handler = KafkaOnlyHandler()
    return app


def _client(app: Any) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


async def _get(config: DrakkarConfig, path: str, **kwargs: Any):
    async with _client(create_api_app(_drakkar_app(config))) as client:
        return await client.get(path, **kwargs)


# --- the switch, the token and the common answer rules ---


@pytest.mark.parametrize('path', AGENT_GET_PATHS)
async def test_agent_off_answers_404_on_every_route(path):
    response = await _get(_config(agent={'enabled': False}), path, params={'detail': 'nope'})

    assert response.status_code == 404
    assert response.json() == {'error': MESSAGES['agent_off']}
    assert response.headers['cache-control'] == 'no-store'


@pytest.mark.parametrize('path', AGENT_GET_PATHS)
async def test_agent_off_answers_404_with_the_ui_on(path):
    config = _config(agent={'enabled': False}, ui={'enabled': True, 'release': {'enabled': False}})
    app = create_ui_app(config.ui, AsyncMock(), _drakkar_app(config))

    async with _client(app) as client:
        response = await client.get(path)

    assert (response.status_code, response.json()) == (404, {'error': MESSAGES['agent_off']})


@pytest.mark.parametrize(
    'headers',
    [
        {},
        {'Authorization': 'Bearer wrong'},
        {'Authorization': TOKEN},
        {'Authorization': 'Bearer tôken'.encode('latin-1')},
    ],
)
@pytest.mark.parametrize('path', AGENT_GET_PATHS)
async def test_a_missing_or_wrong_token_answers_401(path, headers):
    response = await _get(_config(agent={'enabled': True, 'auth_token': TOKEN}), path, headers=headers)

    assert response.status_code == 401
    assert response.json() == {'error': MESSAGES['token_required']}
    assert response.headers['cache-control'] == 'no-store'


async def test_the_switch_is_checked_before_the_token():
    response = await _get(_config(agent={'enabled': False, 'auth_token': TOKEN}), '/api/v1/agent')

    assert response.status_code == 404


async def test_the_token_is_checked_before_the_parameters():
    response = await _get(
        _config(agent={'enabled': True, 'auth_token': TOKEN}), '/api/v1/agent/manual', params={'parts': 'nope'}
    )

    assert response.status_code == 401


async def test_the_right_token_answers_200():
    response = await _get(
        _config(agent={'enabled': True, 'auth_token': TOKEN}),
        '/api/v1/agent',
        headers={'Authorization': f'Bearer {TOKEN}'},
    )

    assert response.status_code == 200


async def test_the_ui_token_and_the_query_token_do_not_open_the_agent_routes():
    config = _config(
        agent={'enabled': True, 'auth_token': TOKEN},
        ui={'enabled': True, 'auth_token': 'ui-token', 'release': {'enabled': False}},
    )
    app = create_ui_app(config.ui, AsyncMock(), _drakkar_app(config))

    async with _client(app) as client:
        by_ui_token = await client.get('/api/v1/agent', headers={'Authorization': 'Bearer ui-token'})
        by_query = await client.get('/api/v1/agent', params={'token': TOKEN})
        by_agent_token = await client.get('/api/v1/agent', headers={'Authorization': f'Bearer {TOKEN}'})

    assert (by_ui_token.status_code, by_query.status_code, by_agent_token.status_code) == (401, 401, 200)


@pytest.mark.parametrize(
    ('header', 'expected'),
    [
        ('Bearer abc', True),
        ('Bearer  abc ', True),
        ('bearer abc', True),
        ('BEARER abc', True),
        ('Basic abc', False),
        ('Bearer abd', False),
        ('', False),
    ],
)
def test_token_matches_needs_the_bearer_scheme_in_any_letter_case(header, expected):
    assert token_matches(header, 'abc') is expected


@pytest.mark.parametrize(
    ('path', 'params', 'message'),
    [
        ('/api/v1/agent/manual', {'parts': 'nope', 'detail': 'x', 'format': 'x'}, None),
        ('/api/v1/agent/manual', {'parts': 'concepts,', 'detail': 'x'}, None),
        ('/api/v1/agent/manual', {'detail': 'x', 'format': 'x'}, MESSAGES['detail']),
        ('/api/v1/agent/manual', {'format': 'x', 'topic': 'nope'}, MESSAGES['format']),
        ('/api/v1/agent/config', {'parts': 'nope', 'detail': 'x'}, None),
        ('/api/v1/agent/config', {'detail': 'Full'}, MESSAGES['detail']),
        ('/api/v1/agent/metrics', {'detail': '', 'names': 'x'}, MESSAGES['detail']),
    ],
)
async def test_the_first_failing_parameter_answers_400(path, params, message):
    response = await _get(_config(), path, params=params)

    assert response.status_code == 400
    assert response.headers['cache-control'] == 'no-store'
    if message is None:
        known = CONFIG_PARTS if path.endswith('config') else MANUAL_PARTS
        bad = params['parts'].split(',')[-1] if params['parts'].endswith(',') else params['parts']
        message = MESSAGES['unknown_part'].format(part=bad, known=', '.join(sorted(known)))
    assert response.json() == {'error': message}


async def test_unknown_parameters_are_ignored():
    response = await _get(_config(), '/api/v1/agent', params={'parts': 'nope', 'detail': 'nope', 'whatever': '1'})

    assert response.status_code == 200


# --- the index ---


async def test_the_index_has_the_contract_shape():
    response = await _get(_config(agent={'enabled': True, 'include_payloads': True}), '/api/v1/agent')

    assert response.headers['content-type'] == 'application/json'
    assert response.headers['cache-control'] == 'no-store'
    index = response.json()
    assert list(index) == [
        'schema',
        'framework',
        'worker',
        'capabilities',
        'include_payloads',
        'app_notes',
        'routes',
        'start_here',
    ]
    assert index['schema'] == 1
    assert index['framework']['backend'] == 'python'
    assert index['framework']['registry_version'] == load_registry().version
    assert index['worker'] == {'name': 'worker-a', 'cluster': 'cluster-a', 'instance': '0123456789abcdef'}
    assert index['include_payloads'] is True
    assert index['app_notes'] is False
    assert index['start_here'] == START_HERE
    assert [(row['path'], row['parts']) for row in index['routes']] == [
        ('/api/v1/agent/manual', list(MANUAL_PARTS)),
        ('/api/v1/agent/config', list(CONFIG_PARTS)),
        ('/api/v1/agent/metrics', []),
        ('/api/v1/agent/state', list(STATE_PARTS)),
        ('/api/v1/agent/diagnose', []),
    ]
    assert all(list(row) == ['path', 'parts', 'approx_tokens', 'use'] and row['use'] for row in index['routes'])
    assert index['routes'][2]['approx_tokens'] == {'all': 0}
    assert index['routes'][3]['approx_tokens'] == dict.fromkeys(STATE_PARTS, 0)
    assert index['routes'][4]['approx_tokens'] == {'all': 0}


async def test_the_index_follows_the_contract_fixture():
    pinned = CONTRACT['index']
    parts = {'manual': MANUAL_PARTS, 'config': CONFIG_PARTS, 'state': STATE_PARTS, None: ()}

    index = (await _get(_config(), '/api/v1/agent')).json()

    assert index['schema'] == pinned['schema']
    assert list(index) == pinned['keys']
    assert list(index['framework']) == pinned['framework_keys']
    assert list(index['worker']) == pinned['worker_keys']
    assert [list(row) for row in index['routes']] == [pinned['route_keys']] * len(pinned['routes'])
    assert [(row['path'], row['parts']) for row in index['routes']] == [
        (row['path'], list(parts[row['parts']])) for row in pinned['routes']
    ]
    assert index['routes'][2]['approx_tokens'] == {pinned['whole_answer_key']: 0}
    assert index['start_here'] == pinned['start_here'] == START_HERE


def test_the_answer_constants_follow_the_contract_fixture():
    pinned = CONTRACT['answers']

    assert pinned['max_samples'] == MAX_SAMPLES
    assert pinned['secret_text'] == SECRET_TEXT
    assert list(SECRET_KEY_WORDS) == pinned['secret_key_words']


async def test_the_index_names_an_empty_cluster_null():
    config = _config()
    drakkar_app = _drakkar_app(config)
    drakkar_app._cluster_name = ''

    async with _client(create_api_app(drakkar_app)) as client:
        index = (await client.get('/api/v1/agent')).json()

    assert index['worker']['cluster'] is None


async def test_the_index_sizes_are_the_brief_answers_of_each_part():
    async with _client(create_api_app(_drakkar_app(_config()))) as client:
        index = (await client.get('/api/v1/agent')).json()
        manual = {
            part: approx_tokens((await client.get('/api/v1/agent/manual', params={'parts': part})).text)
            for part in MANUAL_PARTS
        }
        config = {
            part: approx_tokens((await client.get('/api/v1/agent/config', params={'parts': part})).text)
            for part in CONFIG_PARTS
        }
        metrics_text = (await client.get('/api/v1/agent/metrics')).text
        await client.get('/api/v1/agent/metrics', params={'prefix': 'drakkar_pending'})
        after = (await client.get('/api/v1/agent')).json()

    assert index['routes'][0]['approx_tokens'] == manual
    assert index['routes'][1]['approx_tokens'] == config
    assert all(value > 0 for value in manual.values() if value)
    assert after['routes'][2]['approx_tokens'] == {'all': approx_tokens(metrics_text)}


# --- the manual ---


async def test_the_manual_answers_the_selection_bytes():
    registry = load_registry()

    response = await _get(_config(), '/api/v1/agent/manual', params={'parts': 'rules,concepts', 'detail': 'full'})

    expected = selection_json(select_manual(registry, parts=['concepts', 'rules'], detail='full'), {})
    assert response.status_code == 200
    assert response.content == dumps(expected).encode('utf-8')


async def test_the_manual_fills_the_promql_placeholders():
    config = _config(
        ui={
            'enabled': False,
            'release': {'enabled': False},
            'prometheus': {'worker_label': 'worker="{worker_id}"', 'cluster_label': 'cluster="{cluster_name}"'},
        }
    )

    response = await _get(config, '/api/v1/agent/manual', params={'parts': 'metrics', 'detail': 'full'})

    text = response.text
    assert '{{worker}}' not in text
    assert 'worker=\\"worker-a\\"' in text


async def test_the_manual_in_markdown():
    response = await _get(_config(), '/api/v1/agent/manual', params={'parts': 'rules', 'format': 'markdown'})

    assert response.status_code == 200
    assert response.headers['content-type'] == 'text/markdown; charset=utf-8'
    assert response.headers['cache-control'] == 'no-store'
    assert response.text.startswith('# rules\n\n## ')


async def test_a_topic_answers_its_entry_and_its_references():
    response = await _get(_config(), '/api/v1/agent/manual', params={'topic': 'concept:agent-surface'})

    parts = response.json()['parts']
    (topic,) = parts['concepts']
    assert topic['key'] == 'agent-surface'
    assert 'body' in topic
    assert {entry['key'] for entry in parts['routes']} == {'GET /api/v1/agent', 'GET /api/v1/agent/diagnose'}


@pytest.mark.parametrize('topic', CONTRACT['topic_not_found'])
async def test_a_topic_that_names_nothing_answers_404(topic):
    response = await _get(_config(), '/api/v1/agent/manual', params={'topic': topic})

    assert response.status_code == 404
    assert response.json() == {'error': MESSAGES['no_such_topic'].format(ref=topic)}


# --- the config ---

SECRETS = {
    'dsn': 'postgresql://pg-user:pg-pass-1@db:5432/app',
    'redis': 'redis://:redis-pass-2@cache:6379/0',
    'consul': 'consul-token-3',
    'http_url': 'https://hook-user:hook-pass-4@hooks.example/in',
    'prometheus': 'https://prom-user:prom-pass-5@prom.example',
    'env': 'env-secret-6',
    'sasl': 'sasl-pass-7',
    'client': 'client-token-8',
    'ui': 'ui-token-9',
    'agent': 'agent-token-10',
    'app': 'app-key-11',
    'header': 'header-value-12',
}


def _secret_config() -> DrakkarConfig:
    return DrakkarConfig(
        connections={
            'kafka': {'main': {'client_config': {'sasl.password': SECRETS['sasl']}}},
            'postgres': {'main_db': {'dsn': SECRETS['dsn']}},
            'redis': {'main_cache': {'url': SECRETS['redis']}},
            'consul': {'main_consul': {'token': SECRETS['consul']}},
        },
        sources={
            'kafka': {'enabled': True, 'startup_align': {'enabled': False}},
            'http': {'enabled': True, 'clients': [{'name': 'partner', 'token': SECRETS['client']}]},
        },
        executor={'env': {'API_TOKEN': SECRETS['env'], 'SERVICE_URL': 'http://u:p@svc'}},
        sinks={
            'http': {
                'webhook_out': {'url': SECRETS['http_url'], 'headers': {'X-Key': SECRETS['header']}},
            }
        },
        metrics={'enabled': False},
        runtime_health={'enabled': False},
        ui={
            'enabled': True,
            'auth_token': SECRETS['ui'],
            'release': {'enabled': False},
            'prometheus': {'url': SECRETS['prometheus']},
        },
        agent={'enabled': True, 'auth_token': SECRETS['agent']},
        app={'billing_api_key': SECRETS['app'], 'region': 'eu'},
    )


async def test_no_secret_leaves_through_the_config_route():
    config = _secret_config()
    drakkar_app = _drakkar_app(config)

    async with _client(create_api_app(drakkar_app)) as client:
        response = await client.get(
            '/api/v1/agent/config',
            params={'detail': 'full'},
            headers={'Authorization': f'Bearer {SECRETS["agent"]}'},
        )

    assert response.status_code == 200
    text = response.text
    for secret in [*SECRETS.values(), 'pg-pass-1', 'redis-pass-2', 'hook-pass-4', 'prom-pass-5']:
        assert secret not in text, secret
    entries = {entry['path']: entry for entry in response.json()['entries']}
    assert entries['connections.postgres.main_db.dsn']['value'] == '***'
    assert entries['connections.postgres.main_db.dsn']['secret'] is True
    assert entries['agent.auth_token']['value'] == '***'
    assert entries['agent.auth_token']['default'] == ''
    assert entries['ui.auth_token']['value'] == '***'
    assert entries['app.billing_api_key'] == {
        'path': 'app.billing_api_key',
        'value': '***',
        'default': '',
        'origin': 'yaml',
        'type': 'string',
        'secret': True,
        'meaning': '',
    }
    assert entries['app.region']['value'] == 'eu'
    assert entries['sinks.http.webhook_out.url']['value'] == 'https://***:***@hooks.example/in'
    assert json.loads(entries['executor.env']['value']) == {'API_TOKEN': '***', 'SERVICE_URL': 'http://***:***@svc'}
    assert json.loads(entries['sources.http.clients']['value'])[0]['token'] == '***'
    assert json.loads(entries['connections.kafka.main.client_config']['value']) == {'sasl.password': '***'}


async def test_config_entries_are_sorted_with_real_map_names_and_filtered_by_parts():
    response = await _get(
        _secret_config(),
        '/api/v1/agent/config',
        params={'parts': 'connections,agent'},
        headers={'Authorization': f'Bearer {SECRETS["agent"]}'},
    )

    paths = [entry['path'] for entry in response.json()['entries']]
    assert paths == sorted(paths, key=lambda path: path.encode())
    assert {path.split('.')[0] for path in paths} == {'connections', 'agent'}
    assert 'connections.postgres.main_db.pool_max' in paths
    assert not any('*' in path for path in paths)


async def test_config_values_are_canonical_texts_and_advice_comes_only_in_full():
    async with _client(create_api_app(_drakkar_app(_config()))) as client:
        brief = (await client.get('/api/v1/agent/config', params={'parts': 'executor'})).json()['entries']
        full = (await client.get('/api/v1/agent/config', params={'parts': 'executor', 'detail': 'full'})).json()[
            'entries'
        ]

    by_path = {entry['path']: entry for entry in brief}
    assert by_path['executor.task_timeout']['value'] == '2m'
    assert by_path['executor.task_timeout']['type'] == 'duration'
    assert by_path['executor.max_executors']['value'] == '4'
    assert by_path['executor.binary_path']['value'] == 'null'
    assert by_path['executor.max_executors']['meaning']
    assert list(by_path['executor.max_executors']) == CONTRACT['answers']['config_entry_keys'][:-1]
    assert not any('advice' in entry for entry in brief)
    assert any(list(entry) == CONTRACT['answers']['config_entry_keys'] for entry in full)


async def test_a_config_built_in_code_reports_changed_values_as_given():
    response = await _get(_config(executor={'max_executors': 9}), '/api/v1/agent/config', params={'parts': 'executor'})

    by_path = {entry['path']: entry for entry in response.json()['entries']}
    assert by_path['executor.max_executors']['origin'] == 'yaml'
    assert by_path['executor.window_size']['origin'] == 'default'


ORIGINS_YAML = """
connections:
  kafka:
    main: {}
sources:
  kafka:
    enabled: true
    startup_align:
      enabled: false
executor:
  window_size: 50
  max_retries: 5
metrics:
  enabled: false
runtime_health:
  enabled: false
ui:
  enabled: false
  release:
    enabled: false
agent:
  enabled: true
"""


class _StartupHandler(KafkaOnlyHandler):
    async def on_startup(self, config):
        config.executor.max_retries = 7
        return config


async def test_config_origins_from_the_file_the_environment_and_on_startup(tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith('DK_')]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(ORIGINS_YAML)
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTORS', '3')
    app = DrakkarApp(handler=_StartupHandler(), config_path=path, worker_id='w-1')
    await app._lifecycle._run_on_startup()

    async with _client(create_api_app(app)) as client:
        entries = (await client.get('/api/v1/agent/config', params={'parts': 'executor'})).json()['entries']

    origins = {entry['path']: entry['origin'] for entry in entries}
    assert origins['executor.window_size'] == 'yaml'
    assert origins['executor.max_executors'] == 'env'
    assert origins['executor.max_retries'] == 'on_startup'
    assert origins['executor.task_timeout'] == 'default'


class _AppSettings(BaseModel):
    region: str = Field(default='eu', description='The region.')
    db_password: SecretStr = SecretStr('')
    limits: dict[str, int] = Field(default_factory=dict)


class _AppHandler(dk.BaseDrakkarHandler[In, Out]):
    app_config_model: ClassVar[type[BaseModel] | None] = _AppSettings
    app_env_prefix: ClassVar[str] = 'AGENTTEST_'

    async def arrange(self, messages, pending):
        return []


async def test_app_entries_follow_the_handler_model(tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith(('DK_', 'AGENTTEST_'))]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(ORIGINS_YAML + 'app:\n  db_password: pw-123\n')
    monkeypatch.setenv('AGENTTEST_REGION', 'us')
    app = DrakkarApp(handler=_AppHandler(), config_path=path, worker_id='w-1')

    async with _client(create_api_app(app)) as client:
        response = await client.get('/api/v1/agent/config', params={'parts': 'app'})

    assert 'pw-123' not in response.text
    entries = {entry['path']: entry for entry in response.json()['entries']}
    assert entries['app.region'] == {
        'path': 'app.region',
        'value': 'us',
        'default': 'eu',
        'origin': 'env',
        'type': 'string',
        'secret': False,
        'meaning': '',
    }
    assert (entries['app.db_password']['value'], entries['app.db_password']['origin']) == ('***', 'yaml')
    assert entries['app.limits']['origin'] == 'default'


def test_app_entries_of_a_raw_section_flatten_maps():
    view = AppConfigView(values={'a': {'b': 1, 'secret_x': 's'}, 'c': [1]}, yaml_section={'a': {'b': 1}})

    entries = {entry['path']: entry for entry in app_entries(view)}

    assert entries['app.a.b']['value'] == '1'
    assert entries['app.a.b']['type'] == 'integer'
    assert entries['app.a.secret_x']['value'] == '***'
    assert entries['app.c']['type'] == 'array'


@pytest.mark.parametrize(
    ('segments', 'expected'),
    [(['DB_DSN'], True), (['apiKey'], True), (['Passwords', 'x'], True), (['MyToken'], True), (['region'], False)],
)
def test_secret_app_names(segments, expected):
    assert is_secret_app_path(segments) is expected


@pytest.mark.parametrize(
    ('value', 'text'),
    [('a', 'a'), (4, '4'), (True, 'true'), (None, 'null'), ([], '[]'), ({'k': 'é'}, '{"k":"é"}')],
)
def test_value_text(value, text):
    assert value_text(value) == text


@pytest.mark.parametrize(
    ('value', 'secret', 'text'),
    [
        ('postgresql://u:p@h/db', True, '***'),
        ('', True, ''),
        ({}, True, ''),
        ({'a': 'b'}, True, '***'),
        ('http://u:p@prom', False, 'http://***:***@prom'),
        ('http://prom?token=abc', False, 'http://prom?token=***'),
        (datetime.date(2026, 1, 2), False, '2026-01-02'),
    ],
)
def test_masked_text(value, secret, text):
    assert masked_text(value, secret=secret) == text


@pytest.mark.parametrize('case', CONTRACT['config_masking_cases'])
def test_config_masking_cases(case):
    assert mask_value(case['value']) == case['masked']
    assert masked_text(case['value'], secret=False) == value_text(case['masked'])


def test_expand_path_takes_every_map_name():
    tree = {'sinks': {'kafka': {'b': {'topic': 't2'}, 'a': {'topic': 't1'}}}}

    assert expand_path(tree, ['sinks', 'kafka', '*', 'topic']) == [
        (['sinks', 'kafka', 'b', 'topic'], 't2'),
        (['sinks', 'kafka', 'a', 'topic'], 't1'),
    ]
    assert expand_path(tree, ['sinks', 'nope']) == []
    assert expand_path({'a': 1}, ['a', 'b']) == []


def test_advice_of_a_block_entry_covers_its_keys():
    advice = {'sinks.defaults.*': 'block', 'executor.max_executors': 'own'}

    assert advice_of(advice, 'executor.max_executors') == 'own'
    assert advice_of(advice, 'sinks.defaults.max_retries') == 'block'
    assert advice_of(advice, 'executor.window_size') is None


def test_config_leaf_is_a_plain_record():
    leaf = ConfigLeaf(path='a.b', description='d', type='string', default='', secret=False)

    assert leaf.path == 'a.b'


# --- the metrics ---


def _families(registry: CollectorRegistry) -> list[Any]:
    return list(registry.collect())


def _fake_registry(*keys: str) -> Any:
    entry = {
        'meaning': 'm',
        'healthy': 'h',
        'promql': [{'title': 't', 'query': 'rate(x{ {{worker}} }[5m])'}],
        'related': ['concept:pipeline'],
    }
    return MagicMock(parts={'metrics': tuple({'key': key, 'summary': 's', **entry} for key in keys)})


def _answer(families: list[Any], query: MetricsQuery | None = None, *, full: bool = False, **kwargs: Any):
    return metrics_answer(
        families=families,
        registry=kwargs.pop('registry', _fake_registry('fw_events_total', 'fw_queue', 'fw_seconds')),
        handler_families=kwargs.pop('handler_families', frozenset({'app_hits'})),
        query=query or MetricsQuery(),
        full=full,
        promql_filters={'worker': 'w="1"', 'cluster': ''},
        notes=kwargs.pop('notes', MetricNotes()),
    )


def test_metrics_lists_framework_and_handler_metrics_only():
    registry = CollectorRegistry()
    Counter('fw_events', 'Events.', ['partition'], registry=registry).labels('2').inc(3)
    Gauge('other_gauge', 'Not ours.', registry=registry).set(1)
    Counter('app_hits', 'Hits.', registry=registry).inc()

    answer = _answer(_families(registry), notes=MetricNotes(meaning={'app_hits_total': 'Hits of the app.'}))

    assert [metric['name'] for metric in answer['metrics']] == ['app_hits_total', 'fw_events_total']
    fw = answer['metrics'][1]
    assert fw == {
        'name': 'fw_events_total',
        'type': 'counter',
        'help': 'Events.',
        'meaning': 'm',
        'healthy': 'h',
        'samples': [{'labels': {'partition': '2'}, 'value': 3}],
        'samples_total': 1,
    }
    assert answer['metrics'][0]['meaning'] == 'Hits of the app.'
    assert answer['metrics'][0]['healthy'] is None
    assert answer['unknown'] == []


def test_metrics_full_fills_promql_and_adds_related():
    registry = CollectorRegistry()
    Gauge('fw_queue', 'Queue.', registry=registry).set(2.5)

    (metric,) = _answer(_families(registry), full=True)['metrics']

    assert list(metric) == [
        'name',
        'type',
        'help',
        'meaning',
        'healthy',
        'promql',
        'related',
        'samples',
        'samples_total',
    ]
    assert metric['promql'] == [{'title': 't', 'query': 'rate(x{ w="1" }[5m])'}]
    assert metric['samples'] == [{'labels': {}, 'value': 2.5}]


def test_metrics_cap_the_samples_and_sort_them_by_label_values():
    registry = CollectorRegistry()
    gauge = Gauge('fw_queue', 'Queue.', ['zone', 'partition'], registry=registry)
    for number in range(150):
        gauge.labels(zone='a', partition=str(number)).set(number)

    (metric,) = _answer(_families(registry))['metrics']

    assert metric['samples_total'] == 150
    assert len(metric['samples']) == MAX_SAMPLES
    assert list(metric['samples'][0]['labels']) == ['partition', 'zone']
    values = [sample['labels']['partition'] for sample in metric['samples']]
    assert values == sorted(values, key=str.encode)


def test_metrics_report_a_histogram_with_count_sum_and_buckets():
    registry = CollectorRegistry()
    histogram = Histogram('fw_seconds', 'Durations.', ['stage'], buckets=(0.005, 1, 2.5), registry=registry)
    histogram.labels('run').observe(0.5)
    histogram.labels('run').observe(2)

    (metric,) = _answer(_families(registry))['metrics']

    assert metric['samples'] == [
        {
            'labels': {'stage': 'run'},
            'count': 2,
            'sum': 2.5,
            'buckets': [
                {'le': '0.005', 'count': 0},
                {'le': '1', 'count': 1},
                {'le': '2.5', 'count': 2},
                {'le': '+Inf', 'count': 2},
            ],
        }
    ]


def test_metrics_filter_by_names_and_prefix_and_list_unknown_names():
    registry = CollectorRegistry()
    Counter('fw_events', 'Events.', registry=registry)
    Gauge('fw_queue', 'Queue.', registry=registry)
    Counter('app_hits', 'Hits.', registry=registry)

    by_names = _answer(_families(registry), MetricsQuery(names=['fw_queue', 'nope', 'nope', 'fw_events']))
    by_prefix = _answer(_families(registry), MetricsQuery(prefix='app_'))
    both = _answer(_families(registry), MetricsQuery(names=['fw_queue'], prefix='app_'))

    assert [metric['name'] for metric in by_names['metrics']] == ['fw_queue']
    assert by_names['unknown'] == ['nope', 'fw_events']
    assert [metric['name'] for metric in by_prefix['metrics']] == ['app_hits_total']
    assert [metric['name'] for metric in both['metrics']] == ['app_hits_total', 'fw_queue']


def test_a_metric_with_no_samples_is_listed_with_an_empty_list():
    registry = CollectorRegistry()
    Gauge('fw_queue', 'Queue.', ['partition'], registry=registry)

    (metric,) = _answer(_families(registry))['metrics']

    assert (metric['samples'], metric['samples_total']) == ([], 0)


@pytest.mark.parametrize(
    ('bound', 'text'),
    [(0.005, '0.005'), (1.0, '1'), (30.0, '30'), (math.inf, '+Inf'), (-math.inf, '-Inf'), (1e-05, '1e-05')],
)
def test_bound_text(bound, text):
    assert bound_text(bound) == text


@pytest.mark.parametrize(('value', 'number'), [(3.0, 3), (2.5, 2.5), (math.nan, None), (math.inf, None)])
def test_json_number(value, number):
    assert json_number(value) == number


@pytest.mark.parametrize(
    ('name', 'kind', 'exposed'), [('a', 'counter', 'a_total'), ('a', 'info', 'a_info'), ('a', 'gauge', 'a')]
)
def test_exposed_name(name, kind, exposed):
    assert exposed_name(name, kind) == exposed


async def test_the_metrics_route_reads_the_framework_metrics():
    response = await _get(
        _config(), '/api/v1/agent/metrics', params={'names': 'drakkar_pending_tasks,no_such_metric', 'detail': 'full'}
    )

    answer = response.json()
    assert response.headers['cache-control'] == 'no-store'
    assert [metric['name'] for metric in answer['metrics']] == ['drakkar_pending_tasks']
    assert answer['unknown'] == ['no_such_metric']
    assert answer['metrics'][0]['meaning']


def test_the_surface_reads_no_handler_metrics_without_a_handler():
    drakkar_app = _drakkar_app(_config())
    drakkar_app.handler = None

    assert AgentSurface(drakkar_app).handler_metric_families() == frozenset()


# --- a worker with the UI off ---


async def test_a_worker_with_the_ui_off_serves_the_agent_routes(monkeypatch):
    listener = MagicMock()
    listener.started = True
    monkeypatch.setattr('drakkar.uiserver.server.LoopLoggingServer', MagicMock(return_value=listener))
    uvicorn_config = MagicMock()
    monkeypatch.setattr('drakkar.uiserver.server.uvicorn.Config', uvicorn_config)
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=_config(recorder={'enabled': False}), worker_id='w-1')

    await app._lifecycle._start_recorder_and_api()

    async with _client(uvicorn_config.call_args.kwargs['app']) as client:
        assert (await client.get('/api/v1/identity')).status_code == 404
        for path in AGENT_GET_PATHS:
            assert (await client.get(path)).status_code == 200, path


@pytest.mark.parametrize(
    ('value', 'kind'),
    [(1.5, 'number'), (True, 'boolean'), ({}, 'object'), (None, 'null'), ('x', 'string')],
)
def test_app_entries_of_a_raw_section_name_the_json_type(value, kind):
    (entry,) = app_entries(AppConfigView(values={'item': value}))

    assert entry['type'] == kind


def test_a_key_that_on_startup_added_has_the_on_startup_origin():
    leaf = ConfigLeaf(path='sinks.kafka.*.topic', description='d', type='string', default='', secret=False)

    (entry,) = framework_entries(
        leaves=[leaf],
        config_dump={'sinks': {'kafka': {'added_out': {'topic': 't'}}}},
        sources=ConfigSources(origins=None, before_on_startup={'sinks': {'kafka': {}}}),
        parts=['sinks'],
        registry=load_registry(),
        full=False,
    )

    assert (entry['path'], entry['origin']) == ('sinks.kafka.added_out.topic', 'on_startup')


# --- secrets: every secret leaf of the metadata, and secrets inside values ---


def _set_path(tree: dict[str, Any], segments: list[str], value: Any) -> None:
    for segment in segments[:-1]:
        tree = tree.setdefault(segment, {})
    tree[segments[-1]] = value


def _secret_for(path: str, number: int) -> tuple[Any, str]:
    """A value for a secret leaf and the secret text inside it."""
    secret = f'leaf-secret-{number}'
    if path.endswith('.dsn'):
        return f'postgresql://user:{secret}@db:5432/app', secret
    if path.startswith('connections.redis.'):
        return f'redis://:{secret}@cache:6379/0', secret
    if path.endswith('.uri'):
        return f'mongodb://user:{secret}@mongo:27017/app', secret
    if path.endswith('.headers'):
        return {'X-Api': secret}, secret
    return secret, secret


def _every_secret_config() -> tuple[dict[str, Any], list[str]]:
    from drakkar.agent.registry import read_config_metadata

    values: dict[str, Any] = {
        'connections': {'kafka': {'main': {}}},
        'sources': {'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'ui': {'enabled': True, 'release': {'enabled': False}},
        'agent': {'enabled': True},
        'sinks': {'http': {'hook-out': {'url': 'https://hooks.example/in'}}},
    }
    secrets: list[str] = []
    for number, row in enumerate(row for row in read_config_metadata() if row['secret']):
        value, secret = _secret_for(row['path'], number)
        segments = [('main-one' if segment == '*' else segment) for segment in row['path'].split('.')]
        if segments[:2] == ['sinks', 'http']:
            segments[2] = 'hook-out'
        _set_path(values, segments, value)
        secrets.append(secret)
    inner = {
        'sasl.password': 'inner-secret-1',
        'sasl.oauthbearer.config': 'clientSecret=inner-secret-2',
        'ssl.key.pem': 'inner-secret-3',
    }
    values['connections']['kafka']['main']['client_config'] = dict(inner)
    values['dlq'] = {'client_config': dict(inner)}
    values['sinks']['kafka'] = {'results-out': {'topic': 'results', 'client_config': dict(inner)}}
    values['executor'] = {'env': {'SERVICE_CREDENTIAL': 'inner-secret-4', 'URL': 'http://u:inner-secret-5@svc'}}
    values['sources']['http'] = {'enabled': True, 'clients': [{'name': 'partner', 'token': 'inner-secret-6'}]}
    values['app'] = {
        'services': [{'name': 'a', 'password': 'inner-secret-7'}],
        'extra': {'api_key': 'inner-secret-8'},
        'billing_dsn': 'inner-secret-9',
    }
    secrets += [f'inner-secret-{number}' for number in range(1, 10)]
    return values, secrets


async def test_no_secret_leaf_of_the_metadata_and_no_inner_secret_leaves():
    values, secrets = _every_secret_config()
    config = DrakkarConfig(**values)
    token = config.agent.auth_token

    response = await _get(
        config, '/api/v1/agent/config', params={'detail': 'full'}, headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == 200
    assert len(secrets) >= 18
    for secret in secrets:
        assert secret not in response.text, secret
    entries = {entry['path']: entry for entry in response.json()['entries']}
    secret_paths = {path for path, entry in entries.items() if entry['secret']}
    for kind in ('kafka', 'postgres', 'redis', 'mongo', 'consul'):
        assert any(path.startswith(f'connections.{kind}.main-one.') for path in secret_paths), kind
    assert json.loads(entries['app.services']['value']) == [{'name': 'a', 'password': '***'}]


class _Service(BaseModel):
    name: str
    password: str


class _NestedAppSettings(BaseModel):
    services: list[_Service] = Field(default_factory=list)
    extra: dict[str, str] = Field(default_factory=dict)


class _NestedAppHandler(dk.BaseDrakkarHandler[In, Out]):
    app_config_model: ClassVar[type[BaseModel] | None] = _NestedAppSettings
    app_env_prefix: ClassVar[str] = 'AGENTNESTED_'

    async def arrange(self, messages, pending):
        return []


async def test_secrets_inside_the_values_of_an_app_model_are_masked(tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith(('DK_', 'AGENTNESTED_'))]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        ORIGINS_YAML
        + 'app:\n  services:\n    - name: a\n      password: hunter3\n  extra:\n    api_key: hunter4\n    region: eu\n'
    )
    app = DrakkarApp(handler=_NestedAppHandler(), config_path=path, worker_id='w-1')

    async with _client(create_api_app(app)) as client:
        response = await client.get('/api/v1/agent/config', params={'parts': 'app'})

    assert 'hunter3' not in response.text
    assert 'hunter4' not in response.text
    entries = {entry['path']: entry for entry in response.json()['entries']}
    assert json.loads(entries['app.extra']['value']) == {'api_key': '***', 'region': 'eu'}


async def test_an_env_variable_reaches_a_hyphenated_map_name(tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith('DK_')]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(ORIGINS_YAML + 'sinks:\n  kafka:\n    results-out:\n      topic: results\n')
    monkeypatch.setenv('DK_SINKS__KAFKA__RESULTS_OUT__FLUSH_TIMEOUT', '10s')
    app = DrakkarApp(handler=KafkaOnlyHandler(), config_path=path, worker_id='w-1')

    async with _client(create_api_app(app)) as client:
        entries = (await client.get('/api/v1/agent/config', params={'parts': 'sinks'})).json()['entries']

    by_path = {entry['path']: entry for entry in entries}
    assert (
        by_path['sinks.kafka.results-out.flush_timeout']['origin'],
        by_path['sinks.kafka.results-out.flush_timeout']['value'],
    ) == ('env', '10s')
    assert by_path['sinks.kafka.results-out.topic']['origin'] == 'yaml'


class _IgnoredChangeHandler(KafkaOnlyHandler):
    async def on_startup(self, config):
        config.storage.dir = '/changed/by/hook'
        config.sinks.kafka['results-out'].max_retries = 9
        config.executor.max_retries = 7
        return config


async def test_a_setting_the_worker_ignores_after_on_startup_shows_the_value_it_uses(tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith('DK_')]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        ORIGINS_YAML + 'storage:\n  dir: /from/yaml\nsinks:\n  kafka:\n    results-out:\n      topic: results\n'
    )
    app = DrakkarApp(handler=_IgnoredChangeHandler(), config_path=path, worker_id='w-1')
    await app._lifecycle._run_on_startup()

    async with _client(create_api_app(app)) as client:
        entries = (await client.get('/api/v1/agent/config')).json()['entries']

    by_path = {entry['path']: (entry['value'], entry['origin']) for entry in entries}
    assert by_path['storage.dir'] == ('/from/yaml', 'yaml')
    assert by_path['sinks.kafka.results-out.max_retries'] == ('null', 'default')
    assert by_path['executor.max_retries'] == ('7', 'on_startup')


# --- loading off the API loop ---


async def test_health_answers_while_the_first_agent_request_loads_the_registry(monkeypatch):
    import threading

    started = threading.Event()
    release = threading.Event()
    calls = []

    def slow_load():
        try:
            asyncio.get_running_loop()
            calls.append('on a loop')
        except RuntimeError:
            calls.append('on a worker thread')
        started.set()
        release.wait(timeout=5)
        return load_registry()

    monkeypatch.setattr('drakkar.uiserver.routes_agent.load_registry', slow_load)
    drakkar_app = _drakkar_app(_config())
    drakkar_app.is_ready = True

    async with _client(create_api_app(drakkar_app)) as client:
        first = asyncio.create_task(client.get('/api/v1/agent'))
        second = asyncio.create_task(client.get('/api/v1/agent/manual'))
        await asyncio.to_thread(started.wait, 5)
        health = await asyncio.wait_for(client.get('/healthz'), timeout=2)
        release.set()
        answers = await asyncio.gather(first, second)

    assert health.status_code == 200
    assert [answer.status_code for answer in answers] == [200, 200]
    assert calls == ['on a worker thread']


async def test_the_manual_sizes_follow_the_handler_entries():
    surface = AgentSurface(_drakkar_app(_config()))
    await surface.ensure_loaded()
    before = surface.manual_tokens['app']

    surface.app_entries = [{'key': 'app:concept:billing', 'summary': 'How the billing works.'}]
    await surface.ensure_loaded()

    assert surface.manual_tokens['app'] > before
    assert surface.index()['app_notes'] is True
