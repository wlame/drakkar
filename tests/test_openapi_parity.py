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

import jsonschema
import yaml
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


def _resolve_schema(schemas: dict, name: str) -> dict:
    """Inline local ``$ref`` pointers in a vendored component schema.

    Deliberately simple: every ``$ref`` in this file is a bare
    ``{'$ref': '#/components/schemas/Name'}`` with no sibling keywords (no
    ``oneOf``/``allOf`` merging needed for the schemas this module
    exercises), so a plain recursive substitution is enough.
    """

    def resolve(node):
        if isinstance(node, dict):
            if set(node) == {'$ref'}:
                return resolve(schemas[node['$ref'].rsplit('/', 1)[-1]])
            return {key: resolve(value) for key, value in node.items()}
        if isinstance(node, list):
            return [resolve(item) for item in node]
        return node

    return resolve(schemas[name])


def _walk_routes(routes, seen=None):
    """Yield every route object reachable from ``routes``, nesting included.

    How an included router's routes reach the app is a FastAPI
    implementation detail that has changed twice: they used to be flattened
    onto the app, then kept behind a nested container carrying ``.routes``,
    and since 0.14x they sit behind a lazy ``_IncludedRouter`` that resolves
    only when a request arrives. This handles all three, because a version
    that hides them would otherwise report every natively-registered
    ``/api/v1`` route as missing while the app serves them perfectly well.

    The lazy wrapper is asked for its EFFECTIVE routes rather than being
    unwrapped by hand: the objects it returns carry the final path with the
    include prefix already applied, which is exactly what "served" means
    here. Reading ``original_router.routes`` instead would report the paths
    without their prefix.
    """
    seen = set() if seen is None else seen
    for route in routes:
        if id(route) in seen:
            continue  # cycle guard: a router may reference itself
        seen.add(id(route))
        yield route
        resolve = getattr(route, 'effective_route_contexts', None)
        if callable(resolve):
            yield from _walk_routes(list(resolve()), seen)
            continue
        nested = getattr(route, 'routes', None)
        if nested:
            yield from _walk_routes(nested, seen)


def _app_routes(app) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for route in _walk_routes(app.routes):
        # Duck-typed rather than ``isinstance(route, APIRoute)``: an
        # unrecognised route class must not be skipped silently, or the
        # assertion below blames the app for endpoints it really serves.
        path = getattr(route, 'path', None)
        if path is None:
            continue  # APIRouter and friends expose no path of their own
        if not (path.startswith('/api/v1/') or path in ('/healthz', '/readyz')):
            continue  # legacy aliases, /ws, /docs, SPA catch-all: out of scope
        path = path.replace(':path}', '}')  # FastAPI converter suffix
        for method in getattr(route, 'methods', None) or ():
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


async def test_identity_and_recent_tasks_payloads_match_schemas():
    """Route-parity above only compares path *sets*; this validates actual response
    bodies against the vendored openapi.yaml component schemas (local $ref resolved),
    catching drift the path-set check can't — a field renamed or dropped on one side
    without the other."""
    import aiosqlite

    from drakkar.config import TimelineColorRule, UITimelineConfig
    from drakkar.recorder import SCHEMA_EVENTS

    schemas = yaml.safe_load(SPEC_PATH.read_text())['components']['schemas']

    # A non-default timeline config so color_rules/labels aren't validated
    # as trivially-empty.
    timeline_cfg = UITimelineConfig(
        color_rules=[TimelineColorRule(when={'field': 'status', 'op': 'eq', 'value': 'failed'}, color='red')],
    )
    cfg = make_ui_config(timeline=timeline_cfg)

    # A single shared in-memory connection standing in for both the writer
    # and reader handles, same pattern as the debug-server route tests —
    # one real completed task exercises stdout_size/truncated for real,
    # rather than the early-return empty-list shape a missing DB produces.
    db = await aiosqlite.connect(':memory:')
    await db.executescript(SCHEMA_EVENTS)
    now = time.time()
    await db.execute(
        'INSERT INTO events (ts, dt, event, partition, task_id, args, pid) '
        "VALUES (?, ?, 'task_started', 0, 'task-1', '[]', 100)",
        (now - 10, '2026-04-02'),
    )
    await db.execute(
        'INSERT INTO events (ts, dt, event, partition, task_id, duration, pid, stdout_size) '
        "VALUES (?, ?, 'task_completed', 0, 'task-1', 1.0, 100, 512)",
        (now - 9, '2026-04-02'),
    )
    await db.commit()

    recorder = AsyncMock(spec=EventRecorder)
    recorder._db = db
    recorder._reader_db = db
    recorder.reader_db = db
    recorder.flush = AsyncMock()
    recorder._buffer = []
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'schema-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    app.config_summary = '[schema-worker]'
    app._executor_pool = None

    transport = ASGITransport(app=create_ui_app(cfg, recorder, app))
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        identity = (await client.get('/api/v1/identity')).json()
        recent_tasks = (await client.get('/api/v1/recent-tasks?minutes=5')).json()
    await db.close()

    jsonschema.validate(identity, _resolve_schema(schemas, 'Identity'), cls=jsonschema.Draft202012Validator)
    jsonschema.validate(recent_tasks, _resolve_schema(schemas, 'RecentTasks'), cls=jsonschema.Draft202012Validator)
    # Live data carries no degradation flag at all.
    assert 'unavailable' not in recent_tasks


async def test_recent_tasks_degraded_read_keeps_the_documented_shape(monkeypatch):
    """A recorder read that cannot answer must still return a RecentTasks object.

    It used to return a bare ``[]``, so every client iterating
    ``payload.tasks`` threw instead of degrading — the page froze with no
    visible cause.
    """
    from drakkar.uiserver.server import UIDeps

    schemas = yaml.safe_load(SPEC_PATH.read_text())['components']['schemas']

    async def unavailable_read(self, query, params=()):
        return None

    monkeypatch.setattr(UIDeps, 'flush_and_select', unavailable_read)

    cfg = make_ui_config()
    recorder = AsyncMock(spec=EventRecorder)
    recorder.config = cfg
    app = MagicMock()
    app._worker_id = 'degraded-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic()
    app._config = DrakkarConfig()
    app.config_summary = '[degraded-worker]'
    app._executor_pool.max_executors = 4

    transport = ASGITransport(app=create_ui_app(cfg, recorder, app))
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        response = await client.get('/api/v1/recent-tasks?minutes=5')

    assert response.status_code == 200
    payload = response.json()
    assert payload == {'tasks': [], 'lane_count': 4, 'truncated': False, 'unavailable': True}
    jsonschema.validate(payload, _resolve_schema(schemas, 'RecentTasks'), cls=jsonschema.Draft202012Validator)
