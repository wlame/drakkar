"""``GET /api/v1/debug/split``: the answer shape with the feature off and the ``limit`` check."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, UIConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver import routes_split
from drakkar.uiserver import server as server_module
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections

SPLIT = '/api/v1/debug/split'


def _client(ui_config: UIConfig | None = None, main_loop: Any = None) -> AsyncClient:
    app = MagicMock()
    app._split = None
    if main_loop is not None:
        app.main_loop = main_loop
    app._config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    app._config.ui.release.enabled = False
    recorder = AsyncMock(spec=EventRecorder)
    recorder.memory_store = None
    recorder.worker_name = 'w-1'
    recorder.instance = 'inst-1'
    fastapi_app = create_ui_app(ui_config or UIConfig(enabled=True), recorder, app)
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


async def test_split_state_with_the_feature_off_answers_the_disabled_shape():
    async with _client() as client:
        resp = await client.get(SPLIT)

    assert resp.status_code == 200
    assert resp.json() == {
        'enabled': False,
        'worker': {'name': 'w-1', 'instance': 'inst-1'},
        'source_topic': None,
        'consumer_group': None,
        'parts_topic': None,
        'results_topic': None,
        'parts_partition_base': None,
        'settings': None,
        'partitions': [],
        'jobs': [],
        'jobs_total': 0,
        'result_write_failures_total': 0,
    }


@pytest.mark.parametrize('limit', [0, 501])
async def test_split_state_with_a_limit_out_of_range_answers_422(limit):
    async with _client() as client:
        resp = await client.get(SPLIT, params={'limit': limit})

    assert resp.status_code == 422


@contextmanager
def _loop_in_thread(*, running: bool) -> Iterator[asyncio.AbstractEventLoop]:
    """A second event loop on a thread of its own; ``running=False`` gives a loop that never answers."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True) if running else None
    if thread is not None:
        thread.start()
    try:
        yield loop
    finally:
        if thread is not None:
            loop.call_soon_threadsafe(loop.stop)
        else:
            # The loop wakes up late and runs every call queued on it while it slept.
            thread = threading.Thread(target=loop.run_until_complete, args=(asyncio.sleep(0.01),), daemon=True)
            thread.start()
        thread.join(timeout=5)
        loop.close()


async def test_split_state_is_built_on_the_main_loop(monkeypatch):
    built_on: list[tuple[asyncio.AbstractEventLoop, dict[str, Any]]] = []

    def fake_view(runtime, settings, **kwargs):
        built_on.append((asyncio.get_running_loop(), {'runtime': runtime, 'settings': settings, **kwargs}))
        return {'enabled': False, 'marker': 'from-view'}

    monkeypatch.setattr(routes_split, 'build_split_state', fake_view)
    with _loop_in_thread(running=True) as main_loop:
        async with _client(main_loop=main_loop) as client:
            resp = await client.get(SPLIT, params={'limit': 7})

    assert resp.status_code == 200
    assert resp.json() == {'enabled': False, 'marker': 'from-view'}
    ((loop, kwargs),) = built_on
    assert loop is main_loop
    assert kwargs['limit'] == 7
    assert kwargs['runtime'] is None
    assert (kwargs['worker'], kwargs['instance']) == ('w-1', 'inst-1')
    assert kwargs['source_topic'] == 'input-events'
    assert isinstance(kwargs['result_write_failures'], int)


async def test_split_state_limit_500_is_accepted():
    async with _client() as client:
        resp = await client.get(SPLIT, params={'limit': 500})

    assert resp.status_code == 200


async def test_split_state_answers_503_when_the_main_loop_does_not_answer(monkeypatch):
    monkeypatch.setattr(server_module, 'MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS', 0.05)
    with _loop_in_thread(running=False) as stuck_loop:
        async with _client(main_loop=stuck_loop) as client:
            resp = await client.get(SPLIT)

    assert resp.status_code == 503
    assert resp.json() == {'detail': 'the main loop did not answer in time'}


async def test_split_state_needs_the_token_when_one_is_configured():
    async with _client(UIConfig(enabled=True, auth_token='secret-t')) as client:
        anonymous = await client.get(SPLIT)
        authed = await client.get(SPLIT, headers={'Authorization': 'Bearer secret-t'})

    assert anonymous.status_code == 401
    assert authed.status_code == 200
