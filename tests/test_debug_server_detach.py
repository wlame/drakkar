"""The operator detach routes: ``GET /api/v1/debug/detach``, ``POST /api/v1/debug/detach`` and ``POST /api/v1/debug/attach``.

The state machine is covered by ``test_detach_controller.py``; here the HTTP
layer is exercised against a real ``DetachController`` bound to a mocked app
with a real config: the always-200 state, the 403 switch, the 422 checks, a
POST that answers before its transition ends, and the auth gate.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, UIConfig
from drakkar.detach import ACTION_DETACH, OUTCOME_DETACHED, REASON_MAX_CHARS, DetachController
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections

STATE = '/api/v1/debug/detach'
DETACH = '/api/v1/debug/detach'
ATTACH = '/api/v1/debug/attach'


def _config(*, feature: bool = True, ui_switch: bool = True, kafka: bool = True) -> DrakkarConfig:
    """A config with the Kafka source, the detach feature and its UI switch as given."""
    values: dict[str, Any] = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': kafka, 'detach': {'enabled': feature}}},
        'storage': {'shared_dir': '/srv/drakkar-shared'},
        'ui': {'enabled': True, 'release': {'enabled': False}, 'detach_enabled': ui_switch},
        'recorder': {'enabled': False},
    }
    if not kafka:
        values['sources']['http'] = {'enabled': True}
    if feature:
        values['autodiscover'] = {'enabled': True, 'provider': 'filesystem'}
    return DrakkarConfig(**values)


def _app(config: DrakkarConfig, *, ready: bool = True) -> MagicMock:
    """A mocked app with a real config and a real detach controller."""
    app = MagicMock()
    app._config = config
    app.config = config
    app._stopping = False
    app._cluster_name = ''
    app.fleet = None
    app.cluster_election = None
    source = MagicMock()
    source.is_ready = ready
    source._poll_waits_end.return_value = False
    app.kafka_source = source if config.sources.kafka.enabled else None
    app.detach = DetachController(app)
    return app


def _client(app: MagicMock, ui_config: UIConfig | None = None) -> AsyncClient:
    recorder = AsyncMock(spec=EventRecorder)
    recorder.memory_store = None
    fastapi_app = create_ui_app(ui_config or UIConfig(enabled=True), recorder, app)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


async def _settle(controller: DetachController) -> None:
    """Wait for every task the controller started."""
    await asyncio.gather(*list(controller._tasks))


async def test_state_with_the_feature_off_answers_200_with_enabled_false():
    async with _client(_app(_config(feature=False, ui_switch=False))) as client:
        resp = await client.get(STATE)

    assert resp.status_code == 200
    body = resp.json()
    assert body['enabled'] is False
    assert body['ui_enabled'] is False
    assert body['state'] == 'attached'
    assert body['last_request'] is None


async def test_state_with_the_feature_on_carries_the_durations():
    async with _client(_app(_config())) as client:
        body = (await client.get(STATE)).json()

    assert (body['enabled'], body['ui_enabled']) == (True, True)
    assert (body['max_duration_seconds'], body['default_duration_seconds']) == (86400, 3600)
    assert (body['origin'], body['since_ms'], body['until_ms']) == (None, None, None)


async def test_post_routes_answer_403_naming_the_switch_when_it_is_off():
    app = _app(_config(ui_switch=False))
    async with _client(app) as client:
        detach = await client.post(DETACH, json={'duration_seconds': 60})
        attach = await client.post(ATTACH)

    assert detach.status_code == 403
    assert 'ui.detach_enabled' in detach.json()['detail']
    assert attach.status_code == 403
    assert 'ui.detach_enabled' in attach.json()['detail']
    assert app.detach._tasks == set()


async def test_post_routes_answer_403_without_a_kafka_source():
    app = _app(_config(kafka=False))
    async with _client(app) as client:
        detach = await client.post(DETACH, json={'duration_seconds': 60})
        attach = await client.post(ATTACH)

    assert (detach.status_code, attach.status_code) == (403, 403)
    assert app.detach._tasks == set()


async def test_detach_with_a_duration_out_of_range_answers_422():
    app = _app(_config())
    async with _client(app) as client:
        short = await client.post(DETACH, json={'duration_seconds': 5})
        long = await client.post(DETACH, json={'duration_seconds': 86401})

    assert short.status_code == 422
    assert 'duration must be from 10 to 86400 seconds' in short.json()['detail']
    assert long.status_code == 422
    assert app.detach._tasks == set()
    assert app.detach.wire_state()['last_request'] is None


async def test_detach_with_a_body_that_does_not_parse_answers_422():
    async with _client(_app(_config())) as client:
        missing = await client.post(DETACH, json={'reason': 'no duration'})
        wrong = await client.post(DETACH, json={'duration_seconds': 'soon'})

    assert (missing.status_code, wrong.status_code) == (422, 422)


async def test_detach_answers_202_before_the_transition_ends():
    app = _app(_config())
    controller = app.detach
    release = asyncio.Event()
    asked: list[dict[str, Any]] = []

    async def slow_detach(**kwargs: Any) -> None:
        asked.append(kwargs)
        await release.wait()
        controller._note_request('operator', ACTION_DETACH, OUTCOME_DETACHED, 1_762_000_000.0)

    controller.detach = slow_detach
    async with _client(app) as client:
        resp = await client.post(DETACH, json={'duration_seconds': 120, 'reason': 'x' * (REASON_MAX_CHARS + 50)})
        assert resp.status_code == 202
        assert resp.json()['last_request'] is None
        assert len(controller._tasks) == 1

        release.set()
        await _settle(controller)
        later = (await client.get(STATE)).json()

    assert later['last_request'] == {
        'origin': 'operator',
        'action': 'detach',
        'outcome': 'detached',
        'at_ms': 1_762_000_000_000,
    }
    (call,) = asked
    assert call['duration'] == 120
    assert call['reason'] == 'x' * REASON_MAX_CHARS


async def test_a_refused_detach_shows_its_reason_in_last_request():
    app = _app(_config(), ready=False)
    async with _client(app) as client:
        resp = await client.post(DETACH, json={'duration_seconds': 60})
        await _settle(app.detach)
        later = (await client.get(STATE)).json()

    assert resp.status_code == 202
    assert later['state'] == 'attached'
    assert (later['last_request']['action'], later['last_request']['outcome']) == ('detach', 'not_running')


async def test_attach_on_an_attached_worker_answers_202_and_changes_nothing():
    app = _app(_config())
    async with _client(app) as client:
        resp = await client.post(ATTACH)
        await _settle(app.detach)
        later = (await client.get(STATE)).json()

    assert resp.status_code == 202
    assert resp.json()['state'] == 'attached'
    assert later['state'] == 'attached'
    assert (later['last_request']['origin'], later['last_request']['action']) == ('operator', 'attach')
    assert later['last_request']['outcome'] == 'attached'


async def test_routes_need_the_token_when_one_is_configured():
    app = _app(_config())
    async with _client(app, UIConfig(enabled=True, auth_token='secret-t')) as client:
        anonymous = [
            (await client.get(STATE)).status_code,
            (await client.post(DETACH, json={'duration_seconds': 60})).status_code,
            (await client.post(ATTACH)).status_code,
        ]
        authed = await client.get(STATE, headers={'Authorization': 'Bearer secret-t'})

    assert anonymous == [401, 401, 401]
    assert authed.status_code == 200
    assert app.detach._tasks == set()
