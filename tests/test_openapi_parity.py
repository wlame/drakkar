"""Route-parity pin: the served surface must equal the vendored OpenAPI spec.

The spec (``drakkar/uiserver/openapi.yaml``) is the byte-identical vendored
copy of ``drakkar-ui/docs/openapi-v1.yaml`` — the canonical description of
the contract surface. This test walks the live FastAPI route table and
asserts set-equality with the spec's ``paths``, so any endpoint added,
removed, or renamed on only one side fails CI here (the Go backend pins the
same spec with its own copy of this test). Legacy unprefixed ``/api/*``
aliases, ``/ws``, ``/docs``, and the SPA catch-all are deliberately outside
the pinned surface.
"""

import time
from unittest.mock import AsyncMock, MagicMock

import yaml
from fastapi.routing import APIRoute
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver.routes_openapi import SPEC_PATH
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

_HTTP_METHODS = {'get', 'post', 'put', 'delete', 'patch'}


def _spec_routes() -> set[tuple[str, str]]:
    doc = yaml.safe_load(SPEC_PATH.read_text())
    return {(method.upper(), path) for path, item in doc['paths'].items() for method in item if method in _HTTP_METHODS}


def _app_routes(app) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        path = route.path
        if not (path.startswith('/api/v1/') or path in ('/healthz', '/readyz')):
            continue  # legacy aliases, /ws, /docs, SPA catch-all: out of scope
        path = path.replace(':path}', '}')  # FastAPI converter suffix
        for method in route.methods or ():
            if method in ('HEAD', 'OPTIONS'):
                continue
            out.add((method, path))
    return out


def _stub_app():
    recorder = AsyncMock(spec=EventRecorder)
    recorder.config = make_ui_config()
    app = MagicMock()
    app._worker_id = 'parity-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    return create_ui_app(make_ui_config(), recorder, app)


def test_served_routes_equal_spec():
    served = _app_routes(_stub_app())
    spec = _spec_routes()
    assert served == spec, (
        f'route/spec drift.\nserved-only: {sorted(served - spec)}\nspec-only: {sorted(spec - served)}'
    )


async def test_openapi_json_serves():
    transport = ASGITransport(app=_stub_app())
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        body = (await client.get('/api/v1/openapi.json')).json()
    assert body['openapi'].startswith('3.1')
    assert '/api/v1/identity' in body['paths']


async def test_docs_page_serves():
    transport = ASGITransport(app=_stub_app())
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        page = await client.get('/docs')
        assert page.status_code == 200
        assert 'swagger-ui-bundle.js' in page.text
        js = await client.get('/docs/swagger-ui-bundle.js')
        assert js.status_code == 200 and len(js.content) > 100_000
        css = await client.get('/docs/swagger-ui.css')
        assert css.status_code == 200


async def test_docs_page_propagates_token():
    """Opened with ?token=, the page carries it into asset and spec URLs."""
    transport = ASGITransport(app=_stub_app())
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        page = await client.get('/docs', params={'token': 'abc'})
    assert '/docs/swagger-ui.css?token=abc' in page.text
    assert '/api/v1/openapi.json?token=abc' in page.text


async def test_docs_gated_when_token_configured():
    recorder = AsyncMock(spec=EventRecorder)
    cfg = make_ui_config(auth_token='secret-123')
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'parity-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    fastapi_app = create_ui_app(cfg, recorder, app)
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        assert (await client.get('/docs')).status_code == 401
        assert (await client.get('/api/v1/openapi.json')).status_code == 401
        ok = await client.get('/docs', params={'token': 'secret-123'})
        assert ok.status_code == 200


async def test_framework_openapi_route_is_not_served():
    """FastAPI's auto-generated /openapi.json must not exist.

    It is created by FastAPI itself, so it carries none of our auth
    dependencies: with a token configured it answered 200 with the full route
    table — every /api/debug/* path included — while the vendored spec at
    /api/v1/openapi.json correctly returned 401. Only the vendored, gated
    route may serve the contract.
    """
    recorder = AsyncMock(spec=EventRecorder)
    cfg = make_ui_config(auth_token='secret-123')
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'parity-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    transport = ASGITransport(app=create_ui_app(cfg, recorder, app))
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        assert (await client.get('/openapi.json')).status_code == 404
        # The gated route still exists and still demands the token.
        assert (await client.get('/api/v1/openapi.json')).status_code == 401


async def test_docs_page_escapes_reflected_token():
    """A hostile ?token= value must not break out of the HTML/JS context."""
    transport = ASGITransport(app=_stub_app())
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        page = await client.get('/docs', params={'token': '"><script>alert(1)</script>'})
    assert '<script>alert(1)</script>' not in page.text
    assert 'token=%22%3E%3Cscript%3Ealert%281%29%3C%2Fscript%3E' in page.text
