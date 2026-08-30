"""Tests for the operator docs site router (:mod:`drakkar.uiserver.routes_docs`).

Covers the contract's serving rules for ``GET /docs/``: containment inside
the configured directory, directory requests resolving to ``index.html``,
the hint-404 when the feature is unconfigured or its directory is gone, the
shared auth gate, and the ordering guarantees against the SPA catch-all and
the relocated Swagger page at ``/api-docs``.
"""

import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, UIDocsConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

SITE_FILES = {
    'index.html': b'<html><body>Docs home</body></html>',
    'guide/index.html': b'<html><body>Guide index</body></html>',
    'guide/deep.html': b'<html><body>Deep page</body></html>',
    'assets/site.css': b'body { color: rebeccapurple; }',
}


@pytest.fixture
def docs_site(tmp_path) -> Path:
    """A prebuilt static site on disk, like an mkdocs ``site/`` output."""
    root = tmp_path / 'site'
    for name, body in SITE_FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return root


@pytest.fixture
def secret_outside(tmp_path) -> Path:
    """A file next to (not inside) the site root — containment must hide it."""
    path = tmp_path / 'secrets.txt'
    path.write_bytes(b'top secret')
    return path


def make_docs_app(tmp_path, site_dir: str | None = None, auth_token: str = ''):
    """Build a UI app whose ``ui.docs.site_dir`` is ``site_dir`` (or unset)."""
    docs = UIDocsConfig(site_dir=site_dir) if site_dir is not None else UIDocsConfig()
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token=auth_token, docs=docs)
    recorder = AsyncMock(spec=EventRecorder)
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'docs-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    return create_ui_app(cfg, recorder, app)


def make_docs_client(tmp_path, **kwargs) -> AsyncClient:
    fastapi_app = make_docs_app(tmp_path, **kwargs)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


@pytest.fixture
async def docs_client(tmp_path, docs_site):
    async with make_docs_client(tmp_path, site_dir=str(docs_site)) as client:
        yield client


# --- serving ---------------------------------------------------------------


async def test_docs_root_serves_index_html(docs_client):
    resp = await docs_client.get('/docs/')
    assert resp.status_code == 200
    assert resp.content == SITE_FILES['index.html']
    assert resp.headers['content-type'].startswith('text/html')


async def test_docs_bare_path_redirects_to_trailing_slash(docs_client):
    resp = await docs_client.get('/docs')
    assert resp.status_code == 307
    assert resp.headers['location'] == '/docs/'


async def test_docs_serves_nested_page(docs_client):
    resp = await docs_client.get('/docs/guide/deep.html')
    assert resp.status_code == 200
    assert resp.content == SITE_FILES['guide/deep.html']
    assert resp.headers['content-type'].startswith('text/html')


async def test_docs_serves_asset_with_its_content_type(docs_client):
    resp = await docs_client.get('/docs/assets/site.css')
    assert resp.status_code == 200
    assert resp.content == SITE_FILES['assets/site.css']
    assert 'css' in resp.headers['content-type']


async def test_docs_directory_request_serves_its_index(docs_client):
    """``/docs/guide/`` and ``/docs/guide`` both resolve to the directory's index."""
    for path in ('/docs/guide/', '/docs/guide'):
        resp = await docs_client.get(path)
        assert resp.status_code == 200, path
        assert resp.content == SITE_FILES['guide/index.html'], path


async def test_docs_missing_file_returns_404_not_the_spa_shell(docs_client):
    resp = await docs_client.get('/docs/no/such/page.html')
    assert resp.status_code == 404
    assert b'Docs home' not in resp.content


# --- containment -----------------------------------------------------------


# Percent-encoded rather than literal '../': httpx (like every proxy and
# browser) collapses literal dot segments before the request is sent, so a
# literal path would never reach the router this is testing. The encoded
# form arrives intact and decodes to the same traversal inside Starlette.
@pytest.mark.parametrize(
    'path',
    [
        '/docs/%2e%2e/secrets.txt',
        '/docs/guide/%2e%2e/%2e%2e/secrets.txt',
        '/docs/%2e%2e%2f%2e%2e%2fetc%2fpasswd',
        '/docs/..%2fsecrets.txt',
    ],
)
async def test_docs_traversal_is_404_and_leaks_nothing(docs_client, secret_outside, path):
    resp = await docs_client.get(path)
    assert resp.status_code == 404, path
    assert b'top secret' not in resp.content, path


# --- unconfigured / missing directory --------------------------------------


async def test_docs_unconfigured_returns_hint_404(tmp_path):
    async with make_docs_client(tmp_path) as client:
        resp = await client.get('/docs/')
    assert resp.status_code == 404
    body = resp.json()
    assert 'ui.docs.site_dir' in body['detail']
    assert body['error']


async def test_docs_configured_but_missing_directory_returns_same_hint_404(tmp_path):
    async with make_docs_client(tmp_path, site_dir=str(tmp_path / 'never-built')) as client:
        resp = await client.get('/docs/')
    assert resp.status_code == 404
    assert 'ui.docs.site_dir' in resp.json()['detail']


async def test_docs_routes_registered_even_when_unconfigured(tmp_path):
    """Unconditional registration: /docs* is ours whatever the config says.

    Asserted through behaviour rather than the route table — with no bundle
    resolved the SPA catch-all answers 503, so a 307/404 here proves the
    docs router matched first.
    """
    async with make_docs_client(tmp_path) as client:
        redirect = await client.get('/docs')
        assert redirect.status_code == 307
        nested = await client.get('/docs/guide/deep.html')
        assert nested.status_code == 404
        assert 'ui.docs.site_dir' in nested.json()['detail']


# --- auth ------------------------------------------------------------------


async def test_docs_requires_token_when_configured(tmp_path, docs_site):
    async with make_docs_client(tmp_path, site_dir=str(docs_site), auth_token='secret-123') as client:
        assert (await client.get('/docs/')).status_code == 401
        ok = await client.get('/docs/', headers={'Authorization': 'Bearer secret-123'})
        assert ok.status_code == 200
        assert ok.content == SITE_FILES['index.html']


async def test_docs_hint_404_is_also_gated(tmp_path):
    """An unconfigured site must not reveal its state to an unauthenticated caller."""
    async with make_docs_client(tmp_path, auth_token='secret-123') as client:
        assert (await client.get('/docs/')).status_code == 401


# --- neighbours: Swagger relocation and the SPA catch-all ------------------


async def test_swagger_moved_to_api_docs(tmp_path):
    async with make_docs_client(tmp_path) as client:
        page = await client.get('/api-docs')
        assert page.status_code == 200
        assert 'swagger-ui-bundle.js' in page.text
        assert (await client.get('/api-docs/swagger-ui-bundle.js')).status_code == 200
        assert (await client.get('/api-docs/swagger-ui.css')).status_code == 200


async def test_docs_path_no_longer_serves_swagger(tmp_path, docs_site):
    async with make_docs_client(tmp_path, site_dir=str(docs_site)) as client:
        resp = await client.get('/docs/')
    assert 'swagger-ui-bundle.js' not in resp.text


async def test_spa_still_owns_non_docs_paths(tmp_path, docs_site):
    """The docs router is registered before the SPA and takes only ``/docs*``."""
    from tests.test_uihost import BUNDLE_FILES, seed_cache

    ui_root = seed_cache(tmp_path, 'v1.0.0')
    docs = UIDocsConfig(site_dir=str(docs_site))
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), docs=docs)
    recorder = AsyncMock(spec=EventRecorder)
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'docs-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    fastapi_app = create_ui_app(cfg, recorder, app, ui_root=ui_root)
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test') as client:
        spa = await client.get('/partitions')
        assert spa.status_code == 200
        assert spa.content == BUNDLE_FILES['index.html']
        served_docs = await client.get('/docs/')
        assert served_docs.status_code == 200
        assert served_docs.content == SITE_FILES['index.html']
