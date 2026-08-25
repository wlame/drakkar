"""Tests for the drakkar-ui bundle host (:mod:`drakkar.uihost`) and SPA serving.

Covers:
  * the fetch engine against a local stub GitHub-API server (no real
    network): happy path, cache-hit short-circuit, check_update with a
    cached latest, newest-cached fallback, traversal/oversize/missing-index
    rejection, atomic ``.incoming`` cleanup;
  * the ``ui.enabled`` serving mode: SPA files + History-API fallback,
    JSON/probe/download precedence, auth gating, Jinja pages absent;
  * the ``ui`` config block defaults and validation.
"""

import io
import json
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, UIConfig, UIReleaseConfig
from drakkar.recorder import EventRecorder
from drakkar.uihost import (
    ResolvedBundle,
    default_cache_root,
    newest_cached_version,
    resolve,
)
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Stub GitHub server
# ---------------------------------------------------------------------------


class StubGitHub:
    """A local HTTP server standing in for the GitHub API + asset CDN.

    ``routes`` maps request paths to ``(status, content_type, body)``;
    every request path is recorded in ``requests`` so tests can assert on
    (the absence of) network traffic.
    """

    def __init__(self) -> None:
        self.routes: dict[str, tuple[int, str, bytes]] = {}
        self.requests: list[str] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stub.requests.append(self.path)
                entry = stub.routes.get(self.path)
                if entry is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                status, content_type, body = entry
                self.send_response(status)
                if 300 <= status < 400:
                    # Redirect entries carry the Location in the second slot.
                    self.send_header('Location', content_type)
                else:
                    self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args) -> None:  # silence stderr noise
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address[:2]
        return f'http://{host}:{port}'

    def add_release(self, repo: str, tag: str, asset_bytes: bytes | None, *, latest: bool = False) -> None:
        """Register a release on the REST-API routes with one tar.gz asset.

        Only API routes are registered, so the engine's direct-URL attempt
        404s and these releases exercise the API fallback path.
        """
        asset_path = f'/assets/{tag}.tar.gz'
        release = {
            'tag_name': tag,
            'assets': [{'name': f'drakkar-ui-{tag}.tar.gz', 'browser_download_url': f'{self.base_url}{asset_path}'}],
        }
        body = json.dumps(release).encode()
        self.routes[f'/repos/{repo}/releases/tags/{tag}'] = (200, 'application/json', body)
        if latest:
            self.routes[f'/repos/{repo}/releases/latest'] = (200, 'application/json', body)
        if asset_bytes is not None:
            self.routes[asset_path] = (200, 'application/octet-stream', asset_bytes)

    def add_direct_release(self, repo: str, tag: str, asset_bytes: bytes, *, latest: bool = False) -> None:
        """Register a release on the plain-web (github.com-style) routes ONLY.

        No REST-API routes at all — releases registered this way prove the
        primary, API-free path (the one immune to the anonymous rate limit).
        """
        self.routes[f'/{repo}/releases/download/{tag}/drakkar-ui-{tag}.tar.gz'] = (
            200,
            'application/octet-stream',
            asset_bytes,
        )
        if latest:
            self.routes[f'/{repo}/releases/latest'] = (302, f'{self.base_url}/{repo}/releases/tag/{tag}', b'')

    def add_direct_checksum(self, repo: str, tag: str, body: bytes) -> None:
        """Register the optional ``.sha256`` sidecar for a direct release."""
        self.routes[f'/{repo}/releases/download/{tag}/drakkar-ui-{tag}.tar.gz.sha256'] = (
            200,
            'text/plain',
            body,
        )

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub_github():
    stub = StubGitHub()
    yield stub
    stub.close()


def make_tar_gz(files: dict[str, bytes], symlinks: dict[str, str] | None = None) -> bytes:
    """Build an in-memory gzipped tarball from ``{name: content}`` (+ optional symlinks)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        for name, target in (symlinks or {}).items():
            info = tarfile.TarInfo(name=name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            tar.addfile(info)
    return buf.getvalue()


BUNDLE_FILES = {
    'index.html': b'<!doctype html><title>real ui</title>',
    'assets/app.js': b'console.log("ui");',
}


def ui_config(tmp_path: Path, **overrides) -> UIReleaseConfig:
    # check_update defaults OFF here (unlike the production default) so each
    # test opts into the latest-lookup request explicitly — request-sequence
    # assertions stay deterministic.
    defaults = {
        'enabled': True,
        'check_update': False,
        'cache_dir': str(tmp_path / 'cache'),
        'repo': 'wlame/drakkar-ui',
    }
    defaults.update(overrides)
    return UIReleaseConfig(**defaults)


def seed_cache(tmp_path: Path, version: str, files: dict[str, bytes] | None = None) -> Path:
    """Pre-populate the cache dir with a usable bundle for ``version``."""
    bundle_dir = tmp_path / 'cache' / version
    for name, content in (files or BUNDLE_FILES).items():
        target = bundle_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return bundle_dir


# ---------------------------------------------------------------------------
# Fetch engine + resolution order
# ---------------------------------------------------------------------------


def test_fetch_happy_path_extracts_bundle(stub_github, tmp_path):
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert bundle.root == tmp_path / 'cache' / 'v1.0.0'
    assert (bundle.root / 'index.html').read_bytes() == BUNDLE_FILES['index.html']
    assert (bundle.root / 'assets' / 'app.js').read_bytes() == BUNDLE_FILES['assets/app.js']
    # extraction staging dir swapped away cleanly
    assert not (tmp_path / 'cache' / 'v1.0.0.incoming').exists()


def test_fetch_via_direct_url_without_api(stub_github, tmp_path):
    # The primary path: the conventional github.com asset URL serves the
    # bundle with ZERO REST-API routes on the stub — the path that keeps
    # working under the anonymous API rate limit.
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert (bundle.root / 'index.html').read_bytes() == BUNDLE_FILES['index.html']
    # Two plain-web requests, zero API requests: the tarball plus the
    # optional .sha256 sidecar probe (404 here → verification skipped).
    assert stub_github.requests == [
        '/wlame/drakkar-ui/releases/download/v1.0.0/drakkar-ui-v1.0.0.tar.gz',
        '/wlame/drakkar-ui/releases/download/v1.0.0/drakkar-ui-v1.0.0.tar.gz.sha256',
    ]


def test_latest_resolves_via_redirect_without_api(stub_github, tmp_path):
    # Latest-tag resolution through the github.com redirect alone — no REST
    # API routes served; the whole check_update+fetch flow stays API-free.
    stub_github.add_direct_release('wlame/drakkar-ui', 'v2.0.0', make_tar_gz(BUNDLE_FILES), latest=True)
    cfg = ui_config(tmp_path, pinned_version='', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert bundle.root == tmp_path / 'cache' / 'v2.0.0'
    assert '/repos/' not in ''.join(stub_github.requests)


def test_cache_hit_short_circuits_without_network(stub_github, tmp_path):
    seed_cache(tmp_path, 'v1.0.0')
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert bundle.root == tmp_path / 'cache' / 'v1.0.0'
    assert stub_github.requests == []


def test_check_update_with_cached_latest_does_not_redownload(stub_github, tmp_path):
    # latest resolves to v1.2.0 which is already cached: only the update
    # check hits the network — release tags are immutable, no re-download.
    stub_github.add_release('wlame/drakkar-ui', 'v1.2.0', None, latest=True)
    seed_cache(tmp_path, 'v1.2.0')
    cfg = ui_config(tmp_path, pinned_version='v1.0.0', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert bundle.root == tmp_path / 'cache' / 'v1.2.0'
    # Only the update check hits the network: the direct redirect attempt
    # (404 on this API-only stub) followed by the API fallback — and no
    # asset download at all.
    assert stub_github.requests == [
        '/wlame/drakkar-ui/releases/latest',
        '/repos/wlame/drakkar-ui/releases/latest',
    ]


def test_update_check_failure_keeps_pinned_version(stub_github, tmp_path):
    # /releases/latest 404s; the pinned version still fetches.
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert bundle.root.name == 'v1.0.0'


def test_newest_cached_fallback_when_latest_lookup_fails(stub_github, tmp_path):
    # No pinned version and the latest lookup fails: the newest cached
    # version serves, ordered by semver (v1.10.0 > v1.2.0) above
    # non-semver names ('zzz' loses despite sorting last lexicographically).
    for version in ('v1.2.0', 'v1.10.0', 'zzz'):
        seed_cache(tmp_path, version)
    cfg = ui_config(tmp_path, pinned_version='', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert bundle.root == tmp_path / 'cache' / 'v1.10.0'


def test_newest_cached_skips_incomplete_and_incoming_dirs(tmp_path):
    seed_cache(tmp_path, 'v1.0.0')
    # a newer version without index.html (unusable) and an in-flight
    # extraction dir must both lose to the older complete bundle
    (tmp_path / 'cache' / 'v2.0.0').mkdir(parents=True)
    seed_cache(tmp_path, 'v3.0.0.incoming')

    assert newest_cached_version(tmp_path / 'cache') == 'v1.0.0'


def test_newest_cached_returns_none_for_missing_root(tmp_path):
    assert newest_cached_version(tmp_path / 'nope') is None


def test_traversal_member_rejected_and_incoming_cleaned(stub_github, tmp_path):
    evil = make_tar_gz({'index.html': b'x', '../evil.txt': b'pwned'})
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', evil)
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert not (tmp_path / 'cache' / 'evil.txt').exists()
    assert not (tmp_path / 'cache' / 'v1.0.0').exists()
    assert not (tmp_path / 'cache' / 'v1.0.0.incoming').exists()


def test_symlink_members_skipped(stub_github, tmp_path):
    tarball = make_tar_gz(BUNDLE_FILES, symlinks={'passwd': '/etc/passwd'})
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', tarball)
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert not (bundle.root / 'passwd').exists()


def test_oversize_extraction_rejected(stub_github, tmp_path, monkeypatch):
    monkeypatch.setattr('drakkar.uihost.fetch.MAX_BUNDLE_BYTES', 16)
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz({'index.html': b'x' * 64}))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert not (tmp_path / 'cache' / 'v1.0.0.incoming').exists()


def test_oversize_download_rejected(stub_github, tmp_path, monkeypatch):
    monkeypatch.setattr('drakkar.uihost.fetch.MAX_ASSET_BYTES', 16)
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'


def test_bundle_without_index_rejected(stub_github, tmp_path):
    stub_github.add_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz({'assets/app.js': b'js'}))
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert not (tmp_path / 'cache' / 'v1.0.0').exists()
    assert not (tmp_path / 'cache' / 'v1.0.0.incoming').exists()


def test_release_without_tarball_asset_rejected(stub_github, tmp_path):
    body = json.dumps(
        {'tag_name': 'v1.0.0', 'assets': [{'name': 'notes.zip', 'browser_download_url': 'http://x/notes.zip'}]}
    ).encode()
    stub_github.routes['/repos/wlame/drakkar-ui/releases/tags/v1.0.0'] = (200, 'application/json', body)
    cfg = ui_config(tmp_path, pinned_version='v1.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'


def test_empty_release_repo_disables_fetching(stub_github, tmp_path):
    cfg = ui_config(tmp_path, repo='', pinned_version='v1.0.0', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert stub_github.requests == []


def test_nothing_resolvable_returns_none_rather_than_a_baked_in_bundle(tmp_path):
    """No cache and no reachable release source is not an error.

    There is deliberately no bundle baked into the package: one download at
    any point in a worker's life fills a cache that every later start — and
    every co-located worker of either backend — reads. Until then the worker
    runs API-only and the UI server says how to supply a bundle.
    """
    assert resolve(ui_config(tmp_path, repo='')) is None


def test_default_cache_root_honors_xdg(monkeypatch, tmp_path):
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'xdg'))
    assert default_cache_root() == tmp_path / 'xdg' / 'drakkar' / 'ui'

    monkeypatch.delenv('XDG_CACHE_HOME')
    assert default_cache_root() == Path.home() / '.cache' / 'drakkar' / 'ui'


# ---------------------------------------------------------------------------
# ui config block
# ---------------------------------------------------------------------------


def test_ui_config_defaults():
    # Default-ON with an update check: workers serve the latest
    # fetched/cached release and keep the Jinja pages when nothing is
    # fetchable (matches the Go backend's DefaultUIConfig).
    cfg = DrakkarConfig()
    assert cfg.ui.enabled is True
    assert cfg.ui.release.enabled is True
    assert cfg.ui.release.repo == 'wlame/drakkar-ui'
    assert cfg.ui.release.pinned_version == ''
    assert cfg.ui.release.cache_dir == ''
    assert cfg.ui.release.check_update is True


def test_ui_config_rejects_repo_without_slash():
    with pytest.raises(ValidationError, match='owner/name'):
        UIReleaseConfig(repo='drakkar-ui')


def test_ui_config_env_overrides(monkeypatch):
    from drakkar.config import load_config

    monkeypatch.setenv('DK_UI__ENABLED', 'true')
    monkeypatch.setenv('DK_UI__RELEASE__PINNED_VERSION', 'v9.9.9')
    monkeypatch.setenv('DK_UI__RELEASE__CHECK_UPDATE', 'true')
    cfg = load_config()
    assert cfg.ui.enabled is True
    assert cfg.ui.release.pinned_version == 'v9.9.9'
    assert cfg.ui.release.check_update is True


# ---------------------------------------------------------------------------
# SPA serving (create_ui_app with ui_root)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_recorder():
    rec = AsyncMock(spec=EventRecorder)
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
    rec.get_stats.return_value = {'total_events': 0}
    rec.get_partition_summary.return_value = []
    rec.get_task_events.return_value = []
    rec.discover_workers.side_effect = lambda: []
    return rec


@pytest.fixture
def mock_app():
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 60
    app.processors = {}
    app._config = DrakkarConfig()
    app.cache_engine = None
    app.handler = None
    app._consumer = None

    pool = MagicMock()
    pool.active_count = 0
    pool.waiting_count = 0
    pool.max_executors = 4
    # Real set, not a MagicMock: the live overview iterates it to split
    # in-flight tasks into running vs pending.
    pool.running_task_ids = set()
    app._executor_pool = pool

    sink_mgr = MagicMock()
    sink_mgr.get_sink_info.return_value = []
    sink_mgr.get_all_stats.return_value = {}
    sink_mgr.all_connected.return_value = True
    app.sink_manager = sink_mgr
    app.is_ready = True
    return app


@pytest.fixture
def ui_bundle_dir(tmp_path) -> Path:
    return seed_cache(tmp_path, 'v1.0.0')


def make_client(cfg, recorder, app, ui_root=None) -> AsyncClient:
    fastapi_app = create_ui_app(cfg, recorder, app, ui_root=ui_root)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


@pytest.fixture
async def spa_client(tmp_path, mock_recorder, mock_app, ui_bundle_dir):
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app, ui_root=ui_bundle_dir) as c:
        yield c


async def test_spa_serves_index_at_root(spa_client):
    resp = await spa_client.get('/')
    assert resp.status_code == 200
    assert resp.content == BUNDLE_FILES['index.html']
    assert resp.headers['content-type'].startswith('text/html')


async def test_spa_serves_bundle_asset(spa_client):
    resp = await spa_client.get('/assets/app.js')
    assert resp.status_code == 200
    assert resp.content == BUNDLE_FILES['assets/app.js']
    assert 'javascript' in resp.headers['content-type']


async def test_spa_unknown_path_returns_index_200(spa_client):
    for path in ('/partitions', '/task/task-abc', '/history', '/live', '/debug', '/no/such/route'):
        resp = await spa_client.get(path)
        assert resp.status_code == 200, path
        assert resp.content == BUNDLE_FILES['index.html'], path


async def test_spa_traversal_falls_back_to_index(spa_client):
    resp = await spa_client.get('/%2e%2e/%2e%2e/etc/passwd')
    assert resp.status_code == 200
    assert resp.content == BUNDLE_FILES['index.html']


async def test_spa_mode_keeps_json_api(spa_client):
    resp = await spa_client.get('/api/v1/dashboard')
    assert resp.status_code == 200
    payload = resp.json()
    assert 'stats' in payload
    # the legacy alias keeps working too
    legacy = await spa_client.get('/api/v1/dashboard')
    assert legacy.status_code == 200


async def test_spa_mode_keeps_probes_public(spa_client):
    resp = await spa_client.get('/healthz')
    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok'}


async def test_spa_mode_keeps_download_precedence(tmp_path, mock_recorder, mock_app, ui_bundle_dir):
    (tmp_path / 'w1.db').write_bytes(b'sqlite-bytes')
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app, ui_root=ui_bundle_dir) as c:
        resp = await c.get('/api/v1/debug/download/w1.db')
    assert resp.status_code == 200
    assert resp.content == b'sqlite-bytes'


async def test_spa_mode_auth_gates_pages_like_html(tmp_path, mock_recorder, mock_app, ui_bundle_dir):
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path), auth_token='secret-123')
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app, ui_root=ui_bundle_dir) as c:
        # SPA paths require the token...
        assert (await c.get('/')).status_code == 401
        assert (await c.get('/partitions')).status_code == 401
        ok = await c.get('/', headers={'Authorization': 'Bearer secret-123'})
        assert ok.status_code == 200
        assert ok.content == BUNDLE_FILES['index.html']
        # ...while the probes stay public.
        assert (await c.get('/healthz')).status_code == 200


async def test_spa_owns_every_page_path(spa_client):
    """The SPA's client-side router owns navigation, so every page path —
    known or not — serves the bundle shell with a 200."""
    for path in ('/', '/partitions', '/history', '/sinks', '/live', '/debug'):
        resp = await spa_client.get(path)
        assert resp.content == BUNDLE_FILES['index.html'], path


async def test_without_a_bundle_pages_report_503_and_the_api_still_answers(tmp_path, mock_recorder, mock_app):
    """No bundle is not an outage. There is no built-in HTML fallback, so
    page requests say what is missing while everything else keeps working."""
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    mock_recorder.config = cfg
    async with make_client(cfg, mock_recorder, mock_app) as c:
        page = await c.get('/')
        assert page.status_code == 503
        body = page.json()
        assert body['error'] == 'UI bundle not available'
        assert any('drakkar-ui fetch' in remedy for remedy in body['remedies'])
        # Every non-API path answers the same way — there is nothing to 404 on.
        assert (await c.get('/no/such/route')).status_code == 503
        # The probe surface is untouched.
        assert (await c.get('/healthz')).status_code == 200


async def test_ui_server_resolve_ui_bundle_disabled_returns_none(tmp_path, mock_recorder, mock_app):
    from drakkar.uiserver.server import UIServer

    mock_app._config.ui = UIConfig(release=ui_config(tmp_path, enabled=False))
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    server = UIServer(cfg, mock_recorder, mock_app)
    assert await server._resolve_ui_bundle() is None


async def test_ui_server_resolve_ui_bundle_enabled_resolves(tmp_path, mock_recorder, mock_app):
    from drakkar.uiserver.server import UIServer

    mock_app._config.ui = UIConfig(release=ui_config(tmp_path, repo=''))
    seed_cache(tmp_path, 'v1.0.0')
    cfg = make_ui_config(enabled=True, port=8080, db_dir=str(tmp_path))
    server = UIServer(cfg, mock_recorder, mock_app)
    bundle = await server._resolve_ui_bundle()
    assert bundle is not None
    assert bundle.root == tmp_path / 'cache' / 'v1.0.0'
    # Identity v1.2 rides on this: the bundle knows its version tag.
    assert bundle.version == 'v1.0.0'


def test_resolved_bundle_is_frozen(tmp_path):
    bundle = ResolvedBundle(root=tmp_path, source='cache')
    with pytest.raises(AttributeError):
        bundle.root = tmp_path / 'other'  # ty: ignore[invalid-assignment]


def test_install_bundle_keeps_winners_copy(tmp_path):
    # Shared-cache convergence: a worker finishing extraction after another
    # already installed the same (immutable) tag discards its staging copy —
    # a valid dest_dir is never replaced.
    from drakkar.uihost.fetch import _install_bundle

    dest = tmp_path / 'v1.0.0'
    dest.mkdir()
    (dest / 'index.html').write_text('winner')
    incoming = tmp_path / 'v1.0.0.abcd.incoming'
    incoming.mkdir()
    (incoming / 'index.html').write_text('loser')

    _install_bundle(incoming, dest)

    assert (dest / 'index.html').read_text() == 'winner'
    assert not incoming.exists()


def test_install_bundle_replaces_invalid_leftover(tmp_path):
    from drakkar.uihost.fetch import _install_bundle

    dest = tmp_path / 'v1.0.0'
    dest.mkdir()
    (dest / 'half-extracted.js').write_text('junk')  # no index.html
    incoming = tmp_path / 'v1.0.0.abcd.incoming'
    incoming.mkdir()
    (incoming / 'index.html').write_text('fresh')

    _install_bundle(incoming, dest)

    assert (dest / 'index.html').read_text() == 'fresh'
    assert not incoming.exists()


def test_resolve_serves_cache_when_fetch_loses_race(stub_github, tmp_path, monkeypatch):
    # Thundering-herd simulation: our download fails, but by the time resolve
    # rechecks, "another worker" has installed the wanted version into the
    # shared cache. The loser must serve that copy instead of degrading.
    import drakkar.uihost as uihost_mod

    cfg = ui_config(tmp_path, pinned_version='v1.0.0')
    dest = tmp_path / 'cache' / 'v1.0.0'

    def racing_download(url, out, *, deadline=None):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / 'index.html').write_text('RACE-WINNER')
        raise uihost_mod.FetchError('download interrupted')

    monkeypatch.setattr('drakkar.uihost.fetch._download', racing_download)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert (bundle.root / 'index.html').read_text() == 'RACE-WINNER'


def test_resolve_unpinned_fetch_failure_falls_back_to_newest_cached(stub_github, tmp_path):
    # An unpinned worker whose resolved-latest download fails serves the
    # newest cached release, not the placeholder.
    stub_github.routes['/wlame/drakkar-ui/releases/latest'] = (
        302,
        f'{stub_github.base_url}/wlame/drakkar-ui/releases/tag/v9.0.0',
        b'',
    )  # latest resolves, but v9.0.0 has no asset routes at all
    seed_cache(tmp_path, 'v1.0.0')
    cfg = ui_config(tmp_path, pinned_version='', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert bundle.root == tmp_path / 'cache' / 'v1.0.0'


def test_resolve_pinned_fetch_failure_stays_strict(stub_github, tmp_path):
    # A pin that cannot be fetched must NOT serve a different cached version.
    seed_cache(tmp_path, 'v1.0.0')
    cfg = ui_config(tmp_path, pinned_version='v9.0.0')

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'


def test_resolve_pinned_cached_beats_unfetchable_latest(stub_github, tmp_path):
    # Pinned v1 cached; update check resolves v2 which cannot be fetched →
    # the cached pin serves.
    stub_github.routes['/wlame/drakkar-ui/releases/latest'] = (
        302,
        f'{stub_github.base_url}/wlame/drakkar-ui/releases/tag/v2.0.0',
        b'',
    )
    seed_cache(tmp_path, 'v1.0.0')
    cfg = ui_config(tmp_path, pinned_version='v1.0.0', check_update=True)

    bundle = resolve(cfg, api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'cache'
    assert bundle.root == tmp_path / 'cache' / 'v1.0.0'


# --- sha256 checksum sidecar verification ----------------------------------


def _checksum_body(tarball: bytes, tag: str) -> bytes:
    import hashlib

    return f'{hashlib.sha256(tarball).hexdigest()}  drakkar-ui-{tag}.tar.gz\n'.encode()


def test_fetch_verifies_published_checksum(stub_github, tmp_path):
    """A matching .sha256 sidecar lets the install proceed normally."""
    tarball = make_tar_gz(BUNDLE_FILES)
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', tarball)
    stub_github.add_direct_checksum('wlame/drakkar-ui', 'v1.0.0', _checksum_body(tarball, 'v1.0.0'))

    bundle = resolve(ui_config(tmp_path, pinned_version='v1.0.0'), api_base=stub_github.base_url)

    assert bundle is not None
    assert bundle.source == 'fetched'
    assert (tmp_path / 'cache' / 'v1.0.0' / 'index.html').is_file()


def test_fetch_rejects_checksum_mismatch(stub_github, tmp_path):
    """A present-but-wrong digest aborts the install before extraction.

    resolve() reports nothing usable and nothing lands in the cache — a
    tampered or corrupted asset must never be served.
    """
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    stub_github.add_direct_checksum('wlame/drakkar-ui', 'v1.0.0', ('f' * 64 + '\n').encode())

    bundle = resolve(ui_config(tmp_path, pinned_version='v1.0.0'), api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert not (tmp_path / 'cache' / 'v1.0.0').exists()


def test_fetch_rejects_malformed_checksum_asset(stub_github, tmp_path):
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    stub_github.add_direct_checksum('wlame/drakkar-ui', 'v1.0.0', b'definitely not a digest')

    bundle = resolve(ui_config(tmp_path, pinned_version='v1.0.0'), api_base=stub_github.base_url)

    assert bundle is None, 'nothing usable resolved — the worker runs API-only'
    assert not (tmp_path / 'cache' / 'v1.0.0').exists()


def test_fetch_release_checksum_mismatch_error_message(stub_github, tmp_path):
    """The mismatch error text is byte-identical to the Go backend's."""
    from drakkar.uihost import fetch_release

    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    stub_github.add_direct_checksum('wlame/drakkar-ui', 'v1.0.0', ('a' * 64).encode())

    with pytest.raises(Exception, match=r'release v1\.0\.0 bundle checksum mismatch: expected a{64}, got [0-9a-f]{64}'):
        fetch_release(
            stub_github.base_url,
            'wlame/drakkar-ui',
            'v1.0.0',
            tmp_path / 'cache' / 'v1.0.0',
            download_base=stub_github.base_url,
        )
