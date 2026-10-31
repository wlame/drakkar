"""The ``api`` and ``ui`` sections: which routes a worker serves, and what it reports it can do."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import jsonschema
import pytest
import yaml
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig, UIConfig
from drakkar.fleet.service import PeerView
from drakkar.models import ExecutorTask
from drakkar.uiserver.routes_openapi import SPEC_PATH
from drakkar.uiserver.routes_pages import peer_worker_row
from drakkar.uiserver.server import APIServer, create_api_app, create_ui_app
from drakkar.uiserver.server_helpers import capabilities_of
from tests.conftest import kafka_connections
from tests.test_openapi_parity import _app_routes
from tests.test_sources_validation import KafkaOnlyHandler

UI_NEEDS_API = 'ui.enabled needs api.enabled; set ui.enabled to false, or set api.enabled to true'
AUTODISCOVER_NEEDS_API = 'autodiscover.enabled needs api.enabled: peers reach a worker through its API'
PEER_ROUTE = '/api/v1/peer/cache/changes'
TOKEN = 'secret-token'
CONTRACT = json.loads((Path(__file__).parent / 'fixtures' / 'config_contract.json').read_text(encoding='utf-8'))


def _config(**sections: Any) -> DrakkarConfig:
    """A config with the Kafka source on; ``sections`` replace the defaults."""
    values: dict[str, Any] = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'executor': {'binary_path': '/usr/bin/echo'},
        'sinks': {'kafka': {'output_results': {'topic': 'output-results'}}},
    }
    return DrakkarConfig(**{**values, **sections})


def _serving_engine() -> MagicMock:
    engine = MagicMock()
    engine.serves_changes = True
    engine.changes = AsyncMock(return_value={'rows': [], 'more': False})
    return engine


def _drakkar_app(config: DrakkarConfig) -> MagicMock:
    """The parts of a worker that the always-on routes read."""
    app = MagicMock()
    app._config = config
    app.is_ready = True
    app.processors = {}
    app.sink_manager.all_connected.return_value = True
    app.cache_engine = _serving_engine()
    return app


def _client(app: Any) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


async def _websocket_answer(app: Any, path: str) -> list[dict[str, Any]]:
    """Open a WebSocket on ``app`` at the ASGI level; returns what the app sent back."""
    sent: list[dict[str, Any]] = []
    incoming = [{'type': 'websocket.connect'}]

    async def receive() -> dict[str, Any]:
        return incoming.pop(0) if incoming else {'type': 'websocket.disconnect', 'code': 1000}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        'type': 'websocket',
        'asgi': {'version': '3.0'},
        'scheme': 'ws',
        'path': path,
        'raw_path': path.encode(),
        'root_path': '',
        'query_string': b'',
        'headers': [(b'host', b'test')],
        'server': ('test', 80),
        'client': ('127.0.0.1', 50000),
        'subprotocols': [],
    }
    await app(scope, receive, send)
    return sent


def _gated_get_paths() -> list[str]:
    """Every GET path of the contract except the three that need only ``api.enabled``."""
    doc = yaml.safe_load(SPEC_PATH.read_text())
    always_on = {'/healthz', '/readyz', PEER_ROUTE}
    return sorted(path for path, item in doc['paths'].items() if 'get' in item and path not in always_on)


# --- the routes of a worker with the UI off ---


async def test_ui_off_serves_health_ready_and_peer_only():
    app = create_api_app(_drakkar_app(_config(ui={'enabled': False})))

    async with _client(app) as client:
        assert (await client.get('/healthz')).json() == {'status': 'ok'}
        assert (await client.get('/readyz')).json() == {'status': 'ready'}
        peer = await client.get(PEER_ROUTE, params={'scopes': 'cluster'})
        assert (peer.status_code, peer.json()) == (200, {'rows': [], 'more': False})

        for path in [*_gated_get_paths(), '/', '/ws', '/docs/', '/api-docs', '/some/page']:
            assert (await client.get(path)).status_code == 404, path

    assert _app_routes(app) == {('GET', '/healthz'), ('GET', '/readyz'), ('GET', PEER_ROUTE)}


async def test_ui_off_refuses_a_websocket():
    app = create_api_app(_drakkar_app(_config(ui={'enabled': False})))

    sent = await _websocket_answer(app, '/ws')

    assert [message['type'] for message in sent] == ['websocket.close']


async def test_ui_off_serves_health_with_no_recorder_in_the_worker():
    drakkar_app = _drakkar_app(_config(ui={'enabled': False}, recorder={'enabled': False}))
    drakkar_app._recorder = None

    async with _client(create_api_app(drakkar_app)) as client:
        assert (await client.get('/readyz')).status_code == 200


async def test_auth_token_guards_only_ui_routes():
    config = _config(ui={'auth_token': TOKEN})
    drakkar_app = _drakkar_app(config)

    app = create_ui_app(config.ui, AsyncMock(), drakkar_app)

    async with _client(app) as client:
        assert (await client.get('/healthz')).status_code == 200
        assert (await client.get('/readyz')).status_code == 200
        assert (await client.get(PEER_ROUTE, params={'scopes': 'cluster'})).status_code == 200

        assert (await client.get('/api/v1/identity')).status_code == 401
        assert (await client.get('/api/v1/workers')).status_code == 401
        assert (await client.get('/some/page')).status_code == 401
    sent = await _websocket_answer(app, '/ws')
    assert [(message['type'], message.get('code')) for message in sent] == [('websocket.close', 4401)]


# --- the server and its config ---


def _app_with_fake_listener(monkeypatch: pytest.MonkeyPatch, **sections: Any) -> tuple[DrakkarApp, MagicMock]:
    """A worker whose API server runs no real listener; returns it and ``uvicorn.Config``."""
    listener = MagicMock()
    listener.started = True
    monkeypatch.setattr('drakkar.uiserver.server.LoopLoggingServer', MagicMock(return_value=listener))
    uvicorn_config = MagicMock()
    monkeypatch.setattr('drakkar.uiserver.server.uvicorn.Config', uvicorn_config)
    return DrakkarApp(handler=KafkaOnlyHandler(), config=_config(**sections), worker_id='w-1'), uvicorn_config


async def test_ui_off_fetches_no_bundle(monkeypatch):
    resolve = MagicMock()
    monkeypatch.setattr('drakkar.uihost.resolve', resolve)
    app, uvicorn_config = _app_with_fake_listener(
        monkeypatch,
        api={'host': '0.0.0.0', 'port': 8123},
        ui={'enabled': False},
        recorder={'enabled': False},
    )

    await app._lifecycle._start_recorder_and_api()

    resolve.assert_not_called()
    assert app._recorder is None
    assert isinstance(app._api_server, APIServer)
    assert uvicorn_config.call_args.kwargs['host'] == '0.0.0.0'
    assert uvicorn_config.call_args.kwargs['port'] == 8123
    async with _client(uvicorn_config.call_args.kwargs['app']) as client:
        assert (await client.get('/healthz')).status_code == 200
        assert (await client.get('/api/v1/identity')).status_code == 404


async def test_the_ui_without_a_recorder_refuses_to_start():
    config = _config()
    server = APIServer(config.api, _drakkar_app(config), ui=config.ui, recorder=None)

    with pytest.raises(ValueError, match='the UI routes need a recorder'):
        await server.start()


async def test_a_ui_section_that_is_off_builds_the_api_app_only():
    config = _config(ui={'enabled': False})
    server = APIServer(config.api, _drakkar_app(config), ui=config.ui, recorder=None)

    app = await server._build_app()

    assert _app_routes(app) == {('GET', '/healthz'), ('GET', '/readyz'), ('GET', PEER_ROUTE)}


async def test_api_off_starts_no_server(monkeypatch):
    server = MagicMock(return_value=AsyncMock())
    monkeypatch.setattr('drakkar.uiserver.server.APIServer', server)
    config = _config(api={'enabled': False}, ui={'enabled': False}, recorder={'enabled': False})
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')

    with capture_logs() as logs:
        await app._lifecycle._start_recorder_and_api()

    server.assert_not_called()
    assert app._api_server is None
    (warning,) = [entry for entry in logs if entry['event'] == 'ui_disabled_no_probes']
    assert warning['log_level'] == 'warning'
    assert 'api.enabled' in warning['reason']


async def test_ui_off_logs_no_missing_probes_warning_while_the_api_is_on(monkeypatch):
    monkeypatch.setattr('drakkar.uiserver.server.APIServer', MagicMock(return_value=AsyncMock()))
    config = _config(ui={'enabled': False}, recorder={'enabled': False})
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')

    with capture_logs() as logs:
        await app._lifecycle._start_recorder_and_api()

    assert [entry for entry in logs if entry['event'] == 'ui_disabled_no_probes'] == []


def test_ui_on_needs_api_with_the_message():
    with pytest.raises(ValidationError, match=UI_NEEDS_API):
        _config(api={'enabled': False})


def test_autodiscover_needs_api_with_the_message():
    with pytest.raises(ValidationError, match=AUTODISCOVER_NEEDS_API):
        _config(
            api={'enabled': False},
            ui={'enabled': False},
            storage={'shared_dir': '/tmp/shared'},
            autodiscover={'enabled': True, 'provider': 'filesystem'},
        )


def test_the_api_checks_run_right_after_the_source_check():
    with pytest.raises(ValidationError, match='no input source enabled'):
        DrakkarConfig(api={'enabled': False})
    # Ahead of a later check: this recorder would fail on its empty storage.dir.
    with pytest.raises(ValidationError, match=UI_NEEDS_API):
        _config(api={'enabled': False}, storage={'dir': ''})


def test_a_hook_that_turns_the_ui_on_without_the_api_gets_the_load_message():
    config = _config(api={'enabled': False}, ui={'enabled': False}, recorder={'enabled': False})
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')
    config.ui.enabled = True

    with pytest.raises(ValueError, match=UI_NEEDS_API):
        app._lifecycle._check_config_after_startup()


def test_a_hook_that_turns_the_postgres_recorder_on_without_its_connection_gets_the_load_message():
    config = _config(ui={'enabled': False, 'release': {'enabled': False}}, recorder={'enabled': False})
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')
    config.recorder.enabled = True
    config.recorder.provider = 'postgres'

    with pytest.raises(
        ValueError, match="recorder.postgres.connection 'main' is not defined under connections.postgres"
    ):
        app._lifecycle._check_config_after_startup()


def test_a_hook_that_turns_the_sqlite_recorder_on_without_a_folder_gets_the_load_message():
    config = _config(
        storage={'dir': ''}, ui={'enabled': False, 'release': {'enabled': False}}, recorder={'enabled': False}
    )
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')
    config.recorder.enabled = True

    with pytest.raises(ValueError, match='recorder.provider is sqlite but storage.dir is empty'):
        app._lifecycle._check_config_after_startup()


def test_the_contract_fixture_pins_both_messages_in_order():
    by_name = {case['name']: case.get('message') for case in CONTRACT['validation']}
    assert by_name['ui_needs_api'] == UI_NEEDS_API
    assert by_name['autodiscover_needs_api'] == AUTODISCOVER_NEEDS_API
    assert by_name['ui_and_autodiscover_need_api_ui_first'] == UI_NEEDS_API


def test_card_url_comes_from_api(monkeypatch):
    monkeypatch.setattr('drakkar.app.detect_worker_ip', lambda: '10.1.2.3')

    derived = DrakkarApp(handler=KafkaOnlyHandler(), config=_config(api={'port': 8123}), worker_id='w-1')
    given = DrakkarApp(
        handler=KafkaOnlyHandler(),
        config=_config(api={'port': 8123, 'public_url': 'https://w-1.example/'}),
        worker_id='w-1',
    )

    assert derived.worker_identity.url == 'http://10.1.2.3:8123/'
    assert given.worker_identity.url == 'https://w-1.example/'


async def test_api_section_loads_from_yaml_and_env(tmp_path, monkeypatch):
    from drakkar.config import load_config

    path = tmp_path / 'worker.yaml'
    path.write_text(
        'connections: {kafka: {main: {}}}\n'
        'sources: {kafka: {enabled: true}}\n'
        'api: {host: 0.0.0.0, port: 8123}\n'
        'ui: {release: {enabled: false}}\n'
    )
    monkeypatch.setenv('DK_API__PUBLIC_URL', 'https://w-1.example/')

    config = load_config(path)

    assert (config.api.enabled, config.api.host, config.api.port) == (True, '0.0.0.0', 8123)
    assert config.api.public_url == 'https://w-1.example/'


def test_config_summary_reads_the_api_port():
    assert ' api=8123 ui=on ' in _config(api={'port': 8123}).config_summary('w-1')
    assert ' api=8123 ui=off ' in _config(api={'port': 8123}, ui={'enabled': False}).config_summary('w-1')
    assert ' api=off ui=off ' in _config(api={'enabled': False}, ui={'enabled': False}).config_summary('w-1')


@pytest.mark.parametrize('port', [0, 65536])
def test_api_port_keeps_its_bounds(port):
    with pytest.raises(ValidationError, match=r'api\.port'):
        _config(api={'port': port})


# --- what the worker reports it can do ---

_MEMORY_ON = {'enabled': True, 'window_seconds': 120, 'max_rows': 12000}
_MEMORY_OFF = {'enabled': False, 'window_seconds': 120, 'max_rows': 12000}


def _capabilities_schema() -> dict[str, Any]:
    doc = yaml.safe_load(SPEC_PATH.read_text())
    return {**doc['components']['schemas']['Capabilities'], 'additionalProperties': False}


@pytest.mark.parametrize(
    ('ui_enabled', 'recorder', 'autodiscover', 'expected_recorder', 'expected_memory', 'expected_autodiscover'),
    [
        (
            True,
            {'enabled': True, 'provider': 'sqlite'},
            {'enabled': False},
            {'enabled': True, 'provider': 'sqlite', 'history_search': True},
            _MEMORY_ON,
            {'enabled': False, 'provider': None},
        ),
        (
            True,
            {'enabled': False, 'provider': 'sqlite'},
            {'enabled': True, 'provider': 'filesystem'},
            {'enabled': False, 'provider': 'none', 'history_search': False},
            _MEMORY_ON,
            {'enabled': True, 'provider': 'filesystem'},
        ),
        (
            False,
            {'enabled': True, 'provider': 'postgres'},
            {'enabled': False, 'provider': 'filesystem'},
            {'enabled': True, 'provider': 'postgres', 'history_search': True},
            _MEMORY_OFF,
            {'enabled': False, 'provider': None},
        ),
        (
            False,
            {'enabled': False},
            {'enabled': True, 'provider': 'filesystem'},
            {'enabled': False, 'provider': 'none', 'history_search': False},
            _MEMORY_OFF,
            {'enabled': True, 'provider': 'filesystem'},
        ),
    ],
)
def test_capabilities_match_the_contract(
    ui_enabled, recorder, autodiscover, expected_recorder, expected_memory, expected_autodiscover
):
    config = _config(
        ui={'enabled': ui_enabled},
        recorder=recorder,
        autodiscover=autodiscover,
        storage={'shared_dir': '/tmp/shared'},
        executor={'binary_path': '/usr/bin/echo', 'max_executors': 4},
        connections={**kafka_connections(), 'postgres': {'main': {'dsn': 'postgresql://h/db'}}},
    )
    # A worker with the UI on has a memory store; one with both off has no recorder at all.
    if ui_enabled:
        worker_recorder = SimpleNamespace(memory_store=object())
    elif config.recorder.enabled:
        worker_recorder = SimpleNamespace(memory_store=None)
    else:
        worker_recorder = None

    capabilities = capabilities_of(config, worker_recorder)

    assert capabilities == {
        'recorder': expected_recorder,
        'memory': expected_memory,
        'autodiscover': expected_autodiscover,
        'cache_exchange': False,
    }
    jsonschema.validate(capabilities, _capabilities_schema())


@pytest.mark.parametrize(
    ('cache', 'autodiscover_on', 'expected'),
    [
        ({'enabled': True}, True, True),
        ({'enabled': False}, True, False),
        ({'enabled': True, 'exchange': {'enabled': False}}, True, False),
        ({'enabled': True}, False, False),
    ],
)
def test_cache_exchange_capability_needs_cache_exchange_and_autodiscover(cache, autodiscover_on, expected):
    config = _config(
        cache=cache,
        storage={'shared_dir': '/tmp/shared'},
        autodiscover={'enabled': autodiscover_on, 'provider': 'filesystem'},
    )

    assert capabilities_of(config, None)['cache_exchange'] is expected


def test_memory_capability_is_off_when_the_recorder_holds_no_store():
    config = _config(recorder={'capture': {'events': False}})

    assert capabilities_of(config, SimpleNamespace(memory_store=None))['memory']['enabled'] is False


async def test_identity_carries_the_capabilities():
    config = _config()
    drakkar_app = _drakkar_app(config)
    drakkar_app.handler = None
    drakkar_app._worker_id = 'w-1'
    drakkar_app._cluster_name = ''
    drakkar_app.config_summary = 'summary'
    recorder = AsyncMock()
    recorder.memory_store = object()

    async with _client(create_ui_app(config.ui, recorder, drakkar_app)) as client:
        body = (await client.get('/api/v1/identity')).json()

    assert body['capabilities'] == capabilities_of(config, recorder)
    assert body['capabilities']['memory']['enabled'] is True


def _peer(**features: Any) -> PeerView:
    card = {'schema': 1, 'worker': 'w-2', 'cluster': 'search', 'url': 'http://w-2:8080/', 'features': features}
    return PeerView(card=card, age_seconds=1.0, online=True, last_seen_ts=1_760_000_000.0)


@pytest.mark.parametrize(
    ('view', 'expected'),
    [
        (_peer(ui=False), False),
        (_peer(ui=True), True),
        (_peer(), None),
        (_peer(ui='yes'), None),
        (
            PeerView(
                card={'schema': 1, 'worker': 'w-2', 'cluster': '', 'url': 'u'},
                age_seconds=None,
                online=None,
                last_seen_ts=None,
            ),
            None,
        ),
    ],
)
def test_worker_row_carries_ui_enabled_from_the_card(view, expected):
    row = peer_worker_row(view, own_cluster='search', own_hash='0123456789abcdef')

    assert row['ui_enabled'] is expected


@pytest.mark.parametrize('ui_enabled', [True, False])
def test_card_features_say_whether_the_ui_is_on(ui_enabled):
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=_config(ui={'enabled': ui_enabled}), worker_id='w-1')

    assert app._lifecycle._card_features()['ui'] is ui_enabled


# --- the recorder of a worker with no UI ---


async def test_ui_off_defers_no_started_event_for_subscribers_that_cannot_exist(tmp_path, monkeypatch):
    monkeypatch.setattr('drakkar.uiserver.server.APIServer', MagicMock(return_value=AsyncMock()))
    (tmp_path / 'storage').mkdir()
    config = _config(
        ui={'enabled': False},
        recorder={'flush_interval': '60s'},
        storage={'dir': str(tmp_path / 'storage')},
    )
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')
    await app._lifecycle._start_recorder_and_api()
    assert app._recorder is not None
    try:
        task = ExecutorTask(task_id='t-1', args=[], source_offsets=[1], metadata={})
        app._recorder.record_task_started(task, 0)

        assert app._recorder._fanout.take_deferred('t-1') is None
    finally:
        await app._recorder.stop()


def test_ui_config_has_no_transport_keys():
    for key in ('host', 'port', 'public_url'):
        assert key not in UIConfig.model_fields
