"""``GET /api/v1/debug/split``: the answer shape with the feature off and the ``limit`` check."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, UIConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections

SPLIT = '/api/v1/debug/split'


def _client() -> AsyncClient:
    app = MagicMock()
    app._config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    app._config.ui.release.enabled = False
    recorder = AsyncMock(spec=EventRecorder)
    recorder.memory_store = None
    recorder.worker_name = 'w-1'
    recorder.instance = 'inst-1'
    fastapi_app = create_ui_app(UIConfig(enabled=True), recorder, app)
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
