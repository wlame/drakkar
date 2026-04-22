"""Tests for the debug UI cache page and cache API endpoints.

Task 16 adds four debug-UI surfaces for the framework cache:

- ``GET /debug/cache`` — HTML page with stats, filter bar, entries table
- ``GET /api/debug/cache/entries`` — paginated JSON listing of rows, with
  scope/prefix/expired_only filters
- ``GET /api/debug/cache/entry/{key}`` — single row with decoded value
  and full metadata (used by the detail side panel)
- ``GET /api/debug/cache/stats`` — snapshot of the Prometheus cache
  gauges (entries/bytes in memory + DB)

Plus a related modification:

- ``GET /api/debug/periodic`` now surfaces ``system: bool`` on each task
  entry (derived from ``metadata.system``, default False when absent)

When ``cache.enabled=false`` the HTML page returns 404 and the nav link
in ``base.html`` is hidden — tested here via both the /debug/cache endpoint
and by rendering the dashboard and checking for the Cache nav link.

Isolation: ``tmp_path`` for DB files, ``AsyncMock`` for the recorder,
``MagicMock`` for DrakkarApp. A real ``CacheEngine`` is only stood up in
the test cases that exercise the entries/entry/stats endpoints — those
want real SQL round-trips against the schema.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.cache import Cache, CacheEngine
from drakkar.config import CacheConfig, DebugConfig, DrakkarConfig
from drakkar.debug_server import create_debug_app
from drakkar.recorder import EventRecorder

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_recorder():
    """Same shape as test_debug_server.py — minimal recorder spy."""
    rec = AsyncMock(spec=EventRecorder)
    rec.get_stats.return_value = {
        'total_events': 0,
        'consumed': 0,
        'completed': 0,
        'failed': 0,
        'produced': 0,
        'committed': 0,
        'oldest_event': None,
        'newest_event': None,
    }
    rec.get_partition_summary.return_value = []
    rec.get_events.return_value = []
    rec.get_active_tasks.return_value = []
    rec._db = None  # default: no events DB (periodic endpoint short-circuits)
    rec._flush = AsyncMock()
    rec._buffer = []
    return rec


def _make_mock_app(cache_engine: CacheEngine | None = None):
    """Build a MagicMock that matches DrakkarApp's attribute surface.

    ``cache_engine`` is the only param we vary across test cases. All other
    attributes are wired to safe no-op defaults so debug_server routes can
    render without falling over.
    """
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._start_time = time.monotonic() - 120
    app.processors = {}
    app._config = DrakkarConfig()

    pool = MagicMock()
    pool.active_count = 0
    pool.waiting_count = 0
    pool.max_executors = 4
    app._executor_pool = pool
    app._consumer = None

    sink_mgr = MagicMock()
    sink_mgr.get_sink_info.return_value = []
    sink_mgr.get_all_stats.return_value = {}
    app.sink_manager = sink_mgr

    # Critical for Task 16: the debug server reads cache_engine off the app
    # to decide which routes work and which return 404.
    app.cache_engine = cache_engine
    app._cache_engine = cache_engine

    return app


@pytest.fixture
def debug_config(tmp_path):
    return DebugConfig(enabled=True, port=8080, db_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# 1. /api/debug/periodic surfaces `system` field
# ---------------------------------------------------------------------------


class TestApiPeriodicSystemField:
    """``/api/debug/periodic`` must surface ``system: bool`` derived from
    ``metadata.system`` so the debug UI can render the ``[system]`` pill."""

    async def _setup_recorder_with_events(self, tmp_path, mock_recorder, events):
        import aiosqlite

        from drakkar.recorder import SCHEMA_EVENTS

        db_path = str(tmp_path / 'events.db')
        db = await aiosqlite.connect(db_path)
        await db.executescript(SCHEMA_EVENTS)
        for ev in events:
            await db.execute(
                'INSERT INTO events (ts, dt, event, task_id, duration, exit_code, metadata) '
                "VALUES (?, ?, 'periodic_run', ?, ?, ?, ?)",
                ev,
            )
        await db.commit()
        mock_recorder._db = db
        return db

    async def test_system_true_when_metadata_has_system(self, tmp_path, mock_recorder, debug_config):
        """A periodic_run event whose metadata JSON has ``system: true`` is
        returned with ``system: true`` at the top level of each task entry."""
        now = time.time()
        db = await self._setup_recorder_with_events(
            tmp_path,
            mock_recorder,
            [
                (now - 10, '2026-04-21', 'cache.flush', 0.002, 0, '{"status": "ok", "system": true}'),
            ],
        )
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app())
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/periodic')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]['name'] == 'cache.flush'
        assert data[0]['system'] is True
        await db.close()

    async def test_system_false_when_metadata_absent(self, tmp_path, mock_recorder, debug_config):
        """When metadata has no ``system`` key (e.g. a user @periodic task),
        the endpoint defaults ``system`` to False at the top level."""
        now = time.time()
        db = await self._setup_recorder_with_events(
            tmp_path,
            mock_recorder,
            [
                (now - 10, '2026-04-21', 'user.refresh_cache', 0.05, 0, '{"status": "ok"}'),
            ],
        )
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app())
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/periodic')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]['name'] == 'user.refresh_cache'
        assert data[0]['system'] is False
        await db.close()

    async def test_system_mixed_user_and_system_tasks(self, tmp_path, mock_recorder, debug_config):
        """A mix of system and user periodic tasks render with the correct
        flag per-task — proves the field is per-task not global."""
        now = time.time()
        db = await self._setup_recorder_with_events(
            tmp_path,
            mock_recorder,
            [
                (now - 30, '2026-04-21', 'cache.sync', 0.001, 0, '{"status": "ok", "system": true}'),
                (now - 20, '2026-04-21', 'user.cleanup', 0.1, 0, '{"status": "ok"}'),
                (now - 10, '2026-04-21', 'cache.flush', 0.002, 0, '{"status": "ok", "system": true}'),
            ],
        )
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app())
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/periodic')
        assert resp.status_code == 200
        data = resp.json()
        by_name = {t['name']: t for t in data}
        assert by_name['cache.flush']['system'] is True
        assert by_name['cache.sync']['system'] is True
        assert by_name['user.cleanup']['system'] is False
        await db.close()


# ---------------------------------------------------------------------------
# 2. /debug/cache HTML page — 200 when enabled, 404 when disabled
# ---------------------------------------------------------------------------


class TestCachePageRoute:
    """The /debug/cache HTML page renders when the cache is enabled and
    returns 404 when it's disabled (so stale browser tabs don't show a
    half-broken page)."""

    async def test_cache_page_404_when_engine_none(self, mock_recorder, debug_config):
        """No cache_engine → 404. This covers both ``cache.enabled=false``
        and the "engine failed to start" case (both result in None)."""
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=None))
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/debug/cache')
        assert resp.status_code == 404

    async def test_cache_page_200_when_engine_present(self, tmp_path, mock_recorder, debug_config):
        """A live cache engine → 200 with the cache template rendered."""
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/debug/cache')
            assert resp.status_code == 200
            # Basic smoke: the cache template must land under 'Cache' heading
            assert 'Cache' in resp.text
        finally:
            await engine.stop()


# ---------------------------------------------------------------------------
# 3. /api/debug/cache/entries — paginated listing
# ---------------------------------------------------------------------------


async def _start_live_engine(tmp_path) -> CacheEngine:
    """Helper: build + start a real CacheEngine against a tmp_path directory.

    We use the real engine instead of a mock because the entries/entry/stats
    endpoints hit actual SQL — mocking aiosqlite cursors would be more code
    than a real :memory:-style DB on disk. ``tmp_path`` gives us test
    isolation per-function.
    """
    cache_cfg = CacheConfig(
        enabled=True,
        db_dir=str(tmp_path),
        flush_interval_seconds=60.0,
        cleanup_interval_seconds=60.0,
        peer_sync={'enabled': False},
    )
    debug_cfg = DebugConfig(enabled=True, db_dir=str(tmp_path), store_config=False)
    engine = CacheEngine(
        config=cache_cfg,
        debug_config=debug_cfg,
        worker_id='test-worker',
        cluster_name='',
        recorder=None,
    )
    handler_cache = Cache(origin_worker_id='test-worker')
    engine.attach_cache(handler_cache)
    await engine.start()
    return engine


async def _write_entry(
    engine: CacheEngine,
    *,
    key: str,
    value: str,
    scope: str = 'local',
    expires_at_ms: int | None = None,
) -> None:
    """Write a single row directly to the engine's writer DB.

    Bypasses the Cache.set → flush path so tests can control exact
    timestamps and scope values without running the flush periodic.
    """
    assert engine._writer_db is not None
    now_ms = int(time.time() * 1000)
    await engine._writer_db.execute(
        'INSERT OR REPLACE INTO cache_entries '
        '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, expires_at_ms, origin_worker_id) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (key, scope, value, len(value), now_ms, now_ms, expires_at_ms, 'test-worker'),
    )
    await engine._writer_db.commit()


class TestApiCacheEntries:
    """``GET /api/debug/cache/entries`` returns a paginated JSON list."""

    async def test_returns_404_when_cache_disabled(self, mock_recorder, debug_config):
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=None))
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/cache/entries')
        assert resp.status_code == 404

    async def test_empty_list_when_no_entries(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries')
            assert resp.status_code == 200
            body = resp.json()
            assert body['entries'] == []
            assert body['total'] == 0
        finally:
            await engine.stop()

    async def test_lists_entries(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='k1', value='"v1"', scope='local')
            await _write_entry(engine, key='k2', value='"v2"', scope='cluster')

            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries')
            assert resp.status_code == 200
            body = resp.json()
            assert body['total'] == 2
            keys = {e['key'] for e in body['entries']}
            assert keys == {'k1', 'k2'}
        finally:
            await engine.stop()

    async def test_default_limit_is_100(self, tmp_path, mock_recorder, debug_config):
        """Default limit is 100 per the plan spec."""
        engine = await _start_live_engine(tmp_path)
        try:
            # Insert 150 entries so we can see the limit kick in
            for i in range(150):
                await _write_entry(engine, key=f'k{i:03d}', value=f'"v{i}"')

            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries')
            body = resp.json()
            assert len(body['entries']) == 100  # default clamp
            assert body['total'] == 150  # total unfiltered
        finally:
            await engine.stop()

    async def test_limit_over_1000_rejected_with_422(self, tmp_path, mock_recorder, debug_config):
        """``limit > 1000`` is rejected by FastAPI validation with a 422.

        The bound is enforced at the Query layer (``le=1000``) so a
        misbehaving UI gets immediate feedback rather than silently
        receiving fewer rows than requested — prior behavior clamped the
        value server-side, which can mask UI bugs.
        """
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?limit=2000')
            # FastAPI's Query(le=1000) rejects with 422 Unprocessable Entity.
            assert resp.status_code == 422
        finally:
            await engine.stop()

    async def test_limit_at_max_accepted(self, tmp_path, mock_recorder, debug_config):
        """``limit=1000`` is the upper bound and returns 200."""
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?limit=1000')
            assert resp.status_code == 200
            body = resp.json()
            assert body['limit'] == 1000
        finally:
            await engine.stop()

    async def test_limit_zero_returns_empty(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='k1', value='"v1"')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?limit=0')
            assert resp.status_code == 200
            body = resp.json()
            assert body['entries'] == []
            # ``total`` still reflects what matched the filters
            assert body['total'] == 1
        finally:
            await engine.stop()

    async def test_offset_beyond_total_returns_empty(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='k1', value='"v1"')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?offset=50')
            assert resp.status_code == 200
            body = resp.json()
            assert body['entries'] == []
            assert body['total'] == 1
        finally:
            await engine.stop()

    async def test_filter_by_scope(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='l1', value='"v"', scope='local')
            await _write_entry(engine, key='c1', value='"v"', scope='cluster')
            await _write_entry(engine, key='g1', value='"v"', scope='global')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?scope=cluster')
            body = resp.json()
            assert body['total'] == 1
            assert body['entries'][0]['key'] == 'c1'
            assert body['entries'][0]['scope'] == 'cluster'
        finally:
            await engine.stop()

    async def test_filter_by_prefix(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='user:alice', value='"v"')
            await _write_entry(engine, key='user:bob', value='"v"')
            await _write_entry(engine, key='session:xyz', value='"v"')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?prefix=user:')
            body = resp.json()
            assert body['total'] == 2
            keys = {e['key'] for e in body['entries']}
            assert keys == {'user:alice', 'user:bob'}
        finally:
            await engine.stop()

    async def test_filter_expired_only(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            # past: expired 10 seconds ago
            past_ms = int(time.time() * 1000) - 10_000
            # future: expires in 10 seconds
            future_ms = int(time.time() * 1000) + 10_000
            await _write_entry(engine, key='live', value='"v"', expires_at_ms=future_ms)
            await _write_entry(engine, key='never', value='"v"', expires_at_ms=None)
            await _write_entry(engine, key='dead', value='"v"', expires_at_ms=past_ms)
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entries?expired_only=true')
            body = resp.json()
            assert body['total'] == 1
            assert body['entries'][0]['key'] == 'dead'
        finally:
            await engine.stop()


# ---------------------------------------------------------------------------
# 4. /api/debug/cache/entry/{key} — single-entry detail
# ---------------------------------------------------------------------------


class TestApiCacheEntryDetail:
    async def test_returns_404_when_cache_disabled(self, mock_recorder, debug_config):
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=None))
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/cache/entry/foo')
        assert resp.status_code == 404

    async def test_returns_404_when_key_missing(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entry/does-not-exist')
            assert resp.status_code == 404
        finally:
            await engine.stop()

    async def test_returns_entry_with_decoded_value(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            await _write_entry(engine, key='my-key', value='{"a": 1, "b": "hi"}', scope='global')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entry/my-key')
            assert resp.status_code == 200
            body = resp.json()
            assert body['key'] == 'my-key'
            assert body['scope'] == 'global'
            # decoded dict (not a raw JSON string)
            assert body['value'] == {'a': 1, 'b': 'hi'}
            # metadata fields
            assert body['size_bytes'] == len('{"a": 1, "b": "hi"}')
            assert body['origin_worker_id'] == 'test-worker'
            assert body['created_at_ms'] > 0
            assert body['updated_at_ms'] > 0
            assert body['expires_at_ms'] is None
        finally:
            await engine.stop()

    async def test_returns_raw_value_when_decode_fails(self, tmp_path, mock_recorder, debug_config):
        """Corrupt JSON in storage shouldn't 500 the endpoint — fall back to raw."""
        engine = await _start_live_engine(tmp_path)
        try:
            # not valid JSON on purpose — simulates legacy data or corruption
            await _write_entry(engine, key='weird', value='not valid json {')
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/entry/weird')
            assert resp.status_code == 200
            body = resp.json()
            # Falls through to raw string — the UI can display it as-is
            assert body['raw_value'] == 'not valid json {'
        finally:
            await engine.stop()


# ---------------------------------------------------------------------------
# 5. /api/debug/cache/stats — gauge snapshot
# ---------------------------------------------------------------------------


class TestApiCacheStats:
    async def test_returns_404_when_cache_disabled(self, mock_recorder, debug_config):
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=None))
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/api/debug/cache/stats')
        assert resp.status_code == 404

    async def test_stats_match_gauge_values(self, tmp_path, mock_recorder, debug_config):
        """The stats endpoint returns the four cache gauges. Here we exercise
        the gauge-to-JSON path by writing a few entries and checking that
        memory counts match what we put in."""
        engine = await _start_live_engine(tmp_path)
        assert engine._cache is not None
        try:
            engine._cache.set('a', 'value1')
            engine._cache.set('b', 'value2')

            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/api/debug/cache/stats')
            assert resp.status_code == 200
            body = resp.json()
            assert 'entries_in_memory' in body
            assert 'bytes_in_memory' in body
            assert 'entries_in_db' in body
            assert 'bytes_in_db' in body
            # We set 2 entries via Cache.set — memory gauge should reflect 2
            assert body['entries_in_memory'] == 2
            # bytes is running sum, must be positive
            assert body['bytes_in_memory'] > 0
        finally:
            await engine.stop()


# ---------------------------------------------------------------------------
# 6. Nav link in base.html — conditional on cache enabled
# ---------------------------------------------------------------------------


class TestCacheNavLink:
    async def test_nav_shows_cache_link_when_enabled(self, tmp_path, mock_recorder, debug_config):
        engine = await _start_live_engine(tmp_path)
        try:
            fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=engine))
            transport = ASGITransport(app=fastapi_app)
            async with AsyncClient(transport=transport, base_url='http://test') as c:
                resp = await c.get('/')
            assert resp.status_code == 200
            # 'Cache' nav link anchored to /debug/cache must appear
            assert '/debug/cache' in resp.text
            assert 'Cache' in resp.text
        finally:
            await engine.stop()

    async def test_nav_hides_cache_link_when_disabled(self, mock_recorder, debug_config):
        fastapi_app = create_debug_app(debug_config, mock_recorder, _make_mock_app(cache_engine=None))
        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url='http://test') as c:
            resp = await c.get('/')
        assert resp.status_code == 200
        # The Cache nav entry should not appear when the engine is absent
        assert '/debug/cache' not in resp.text
