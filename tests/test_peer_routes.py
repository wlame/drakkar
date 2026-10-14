"""``GET /api/v1/peer/cache/changes``: the public route peers pull cache rows from.

The 400 answers come from ``tests/fixtures/cache_changes.json`` (shared with
every implementation); the pages themselves are tested on the engine in
``tests/test_cache_changes.py``.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.cache import LWW_UPSERT_SQL, Cache, CacheEngine
from drakkar.config import CacheConfig, CacheExchangeConfig, DrakkarConfig
from drakkar.uiserver import server as server_module
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'cache_changes.json').read_text(encoding='utf-8'))
TOKEN = 'secret-token'
ROUTE = '/api/v1/peer/cache/changes'


async def _engine(tmp_path: Path, *, exchange: bool = True) -> CacheEngine:
    engine = CacheEngine(
        config=CacheConfig(enabled=True, flush_interval_seconds=3600, cleanup_interval_seconds=3600),
        exchange=CacheExchangeConfig(enabled=exchange, interval_seconds=3600),
        storage_dir=str(tmp_path),
        worker_id='w-a',
        cluster_name='search',
    )
    engine.attach_cache(Cache(origin_worker_id='w-a'))
    await engine.start()
    return engine


def _ui(cache_engine: CacheEngine | None, *, token: str = TOKEN):
    drakkar_app = MagicMock()
    drakkar_app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
    drakkar_app._config.ui.release.enabled = False
    drakkar_app.cache_engine = cache_engine
    drakkar_app._cache_engine = cache_engine
    config = make_ui_config(enabled=True, port=8080, auth_token=token)
    return create_ui_app(config, MagicMock(), drakkar_app)


def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


@pytest.fixture
async def engine(tmp_path):
    engine = await _engine(tmp_path)
    now_ms = int(time.time() * 1000)
    assert engine._writer_db is not None
    await engine._writer_db.executemany(
        LWW_UPSERT_SQL,
        [
            ('k1', 'cluster', '1', 1, now_ms, now_ms, None, 'w-a'),
            ('k2', 'global', '2', 1, now_ms, now_ms + 1, None, 'w-a'),
        ],
    )
    await engine._writer_db.commit()
    yield engine
    await engine.stop()


async def test_the_peer_route_needs_no_token_while_the_ui_has_one(engine):
    async with _client(_ui(engine)) as client:
        resp = await client.get(ROUTE, params={'scopes': 'cluster,global'})
        gated = await client.get('/api/v1/debug/cache/stats')

    assert resp.status_code == 200
    assert [row['key'] for row in resp.json()['rows']] == ['k1', 'k2']
    assert resp.json()['more'] is False
    assert gated.status_code == 401


@pytest.mark.parametrize(
    'case',
    [case for case in FIXTURE['validation'] if case['status'] == 400],
    ids=[case['name'] for case in FIXTURE['validation'] if case['status'] == 400],
)
async def test_a_bad_request_answers_the_fixture_message(engine, case):
    async with _client(_ui(engine)) as client:
        resp = await client.get(ROUTE, params=case['params'])

    assert resp.status_code == 400
    assert resp.json() == case['body']


async def test_a_repeated_parameter_takes_its_last_value(engine):
    async with _client(_ui(engine)) as client:
        resp = await client.get(f'{ROUTE}?scopes=local&scopes=global')

    assert resp.status_code == 200
    assert [row['key'] for row in resp.json()['rows']] == ['k2']


async def test_no_cache_engine_answers_404(tmp_path):
    async with _client(_ui(None)) as client:
        resp = await client.get(ROUTE, params={'scopes': 'cluster'})

    assert resp.status_code == 404
    assert resp.json() == {'error': 'The cache exchange is off on this worker'}


async def test_the_exchange_off_answers_404(tmp_path):
    engine = await _engine(tmp_path, exchange=False)
    try:
        async with _client(_ui(engine)) as client:
            resp = await client.get(ROUTE, params={'scopes': 'cluster'})
    finally:
        await engine.stop()

    assert resp.status_code == 404


async def test_a_stopped_engine_answers_404_not_500(tmp_path):
    # Shutdown stops the cache before the UI server: late peer requests
    # find no reader.
    engine = await _engine(tmp_path)
    await engine.stop()

    async with _client(_ui(engine)) as client:
        resp = await client.get(ROUTE, params={'scopes': 'cluster'})

    assert resp.status_code == 404


async def test_a_read_that_does_not_answer_in_time_is_503(engine, monkeypatch):
    monkeypatch.setattr(server_module, 'MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS', 0.05)

    async def stuck(query, now_ms):
        await asyncio.Event().wait()

    monkeypatch.setattr(engine, 'changes', stuck)

    async with _client(_ui(engine)) as client:
        resp = await client.get(ROUTE, params={'scopes': 'cluster'})

    assert resp.status_code == 503
    assert resp.json() == {'error': 'The cache did not answer in time'}
