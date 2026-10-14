"""Cross-origin reads for the browser fan-out (trace, trace-by-label, databases).

The page served by one worker reads these three paths from every worker of
its cluster. They answer cross-origin requests with the same origin rule as
the WebSocket (``origin_allowed``, ``ui.allowed_ws_origins``); a preflight
never needs the token, because a browser sends none with it.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

PEER_PAGE = 'http://10.0.0.2:8080'
FANOUT = ['/api/v1/debug/trace', '/api/v1/debug/trace-by-label', '/api/v1/debug/databases']


def _client(*, token: str = '', allowed: list[str] | None = None) -> AsyncClient:
    drakkar_app = MagicMock()
    drakkar_app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
    drakkar_app._config.ui.release.enabled = False
    config = make_ui_config(enabled=True, port=8080, auth_token=token, allowed_ws_origins=allowed or [])
    recorder = MagicMock(storage_dir='', shared_dir='', db_path='')
    app = create_ui_app(config, recorder, drakkar_app)
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://10.0.0.1:8080')


def _preflight_headers(origin: str) -> dict[str, str]:
    return {
        'Origin': origin,
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'authorization',
    }


@pytest.mark.parametrize('path', FANOUT)
async def test_an_allowed_preflight_answers_204_without_a_token(path):
    async with _client(token='t', allowed=[PEER_PAGE]) as client:
        resp = await client.options(path, headers=_preflight_headers(PEER_PAGE))

    assert resp.status_code == 204
    assert resp.headers['access-control-allow-origin'] == PEER_PAGE
    assert resp.headers['access-control-allow-methods'] == 'GET'
    assert resp.headers['access-control-allow-headers'] == 'Authorization'
    assert resp.headers['access-control-max-age'] == '600'
    assert resp.headers['vary'] == 'Origin'


async def test_a_preflight_from_a_peer_page_passes_without_token_or_allowlist():
    async with _client() as client:
        resp = await client.options('/api/v1/debug/trace', headers=_preflight_headers(PEER_PAGE))

    assert resp.status_code == 204


@pytest.mark.parametrize(
    ('token', 'allowed'),
    [('t', []), ('', ['http://10.0.0.3:8080'])],
    ids=['token-without-allowlist', 'origin-not-in-allowlist'],
)
async def test_a_refused_preflight_answers_403(token, allowed):
    async with _client(token=token, allowed=allowed) as client:
        resp = await client.options('/api/v1/debug/databases', headers=_preflight_headers(PEER_PAGE))

    assert resp.status_code == 403
    assert resp.json() == {'error': 'Origin not allowed'}
    assert 'access-control-allow-origin' not in resp.headers


async def test_an_allowed_cross_origin_get_carries_the_cors_headers():
    async with _client() as client:
        resp = await client.get('/api/v1/debug/databases', headers={'Origin': PEER_PAGE})

    assert resp.status_code == 200
    assert resp.headers['access-control-allow-origin'] == PEER_PAGE
    assert resp.headers['vary'] == 'Origin'


async def test_a_refused_cross_origin_get_carries_no_cors_header_and_keeps_auth():
    async with _client(token='t') as client:
        resp = await client.get('/api/v1/debug/databases', headers={'Origin': PEER_PAGE})

    assert resp.status_code == 401
    assert 'access-control-allow-origin' not in resp.headers


async def test_other_paths_send_no_cors_headers():
    async with _client() as client:
        get = await client.get('/api/v1/debug/archives', headers={'Origin': PEER_PAGE})
        preflight = await client.options('/api/v1/debug/archives', headers=_preflight_headers(PEER_PAGE))

    assert 'access-control-allow-origin' not in get.headers
    assert preflight.status_code != 204
    assert 'access-control-allow-origin' not in preflight.headers
