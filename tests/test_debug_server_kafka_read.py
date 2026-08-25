"""Tests for the ad-hoc Kafka read API routes (/api/debug/kafka/*).

The core reader is covered by test_kafka_read.py; here the reader
functions are patched and the HTTP layer is exercised: alias addressing
(and that raw topic names never leak into responses), the
``ui.kafka_read_enabled`` 403 gate, the ``auth_token`` gate, error
mapping (404/422/502), the NDJSON stream shape including the mid-stream
error line, and the startup exposure warning matrix.

Isolation: ``AsyncMock`` recorder, ``MagicMock`` DrakkarApp carrying a
real ``DrakkarConfig`` (the router builds its alias table from
``drakkar_app._config``) — no Kafka, no sockets.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog.testing
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig, KafkaConfig, KafkaSinkConfig, SinksConfig
from drakkar.kafka_read import (
    AliasTarget,
    KafkaReadMessage,
    KafkaReadNotFound,
    KafkaReadUnavailable,
)
from drakkar.kafka_security import KafkaSecurityConfig
from drakkar.recorder import EventRecorder
from drakkar.uiserver import routes_kafka_read
from drakkar.uiserver.routes_kafka_read import _warn_if_exposed_without_ui_auth
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_recorder() -> AsyncMock:
    rec = AsyncMock(spec=EventRecorder)
    rec._db = None
    rec._reader_db = None
    rec.reader_db = None
    return rec


def _drakkar_config() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='main:9092', source_topic='input-events'),
        sinks=SinksConfig(kafka={'search-results-kafka-sink': KafkaSinkConfig(topic='search-results')}),
    )


def _mock_app(config: DrakkarConfig | None = None) -> MagicMock:
    app = MagicMock()
    app._worker_id = 'test-worker'
    app._cluster_name = ''
    app._config = config or _drakkar_config()
    app._config.ui.release.enabled = False
    app._consumer = None
    app.cache_engine = None
    app._cache_engine = None
    return app


def _client(ui_config=None, config: DrakkarConfig | None = None) -> AsyncClient:
    ui_config = ui_config or make_ui_config(enabled=True)
    recorder = _mock_recorder()
    recorder.config = ui_config
    fastapi_app = create_ui_app(ui_config, recorder, _mock_app(config))
    return AsyncClient(transport=ASGITransport(app=fastapi_app), base_url='http://test')


def _message(**overrides) -> KafkaReadMessage:
    defaults = dict(
        alias='source',
        partition=0,
        offset=7,
        timestamp_ms=1_700_000_000_000,
        key='k1',
        key_encoding='utf-8',
        payload='{"n": 1}',
        payload_encoding='utf-8',
        payload_size_bytes=8,
        headers=[],
    )
    defaults.update(overrides)
    return KafkaReadMessage(**defaults)


# ---------------------------------------------------------------------------
# /api/debug/kafka/topics
# ---------------------------------------------------------------------------


async def test_topics_lists_aliases_without_raw_topic_names():
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/topics')
    assert resp.status_code == 200
    body = resp.json()
    aliases = {t['alias']: t['kind'] for t in body['topics']}
    assert aliases == {'source': 'source', 'dlq': 'dlq', 'search-results-kafka-sink': 'sink'}
    # the raw topic names must not leak anywhere in the response
    assert 'input-events' not in resp.text
    assert 'search-results"' not in resp.text  # topic 'search-results' (alias contains it as prefix)


async def test_topics_served_under_v1_alias():
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/topics')
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# gates: kafka_read_enabled + auth_token
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'path',
    [
        '/api/v1/debug/kafka/topics',
        '/api/v1/debug/kafka/source/message?partition=0&offset=0',
        '/api/v1/debug/kafka/source/messages?from_ts=0',
    ],
)
async def test_kafka_read_disabled_serves_403(path):
    async with _client(make_ui_config(enabled=True, kafka_read_enabled=False)) as client:
        resp = await client.get(path)
    assert resp.status_code == 403
    assert 'ui.kafka_read_enabled' in resp.json()['error']


async def test_kafka_read_requires_token_when_configured():
    async with _client(make_ui_config(enabled=True, auth_token='secret-t')) as client:
        anonymous = await client.get('/api/v1/debug/kafka/topics')
        authed = await client.get('/api/v1/debug/kafka/topics', headers={'Authorization': 'Bearer secret-t'})
    assert anonymous.status_code == 401
    assert authed.status_code == 200


# ---------------------------------------------------------------------------
# /api/debug/kafka/{alias}/message
# ---------------------------------------------------------------------------


async def test_message_unknown_alias_404_names_valid_aliases():
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/nope/message?partition=0&offset=0')
    assert resp.status_code == 404
    assert 'source' in resp.json()['detail']


async def test_message_returns_fetched_record(monkeypatch):
    fetched = _message()
    monkeypatch.setattr(routes_kafka_read, 'fetch_message', AsyncMock(return_value=fetched))
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/message?partition=0&offset=7')
    assert resp.status_code == 200
    assert resp.json() == fetched.model_dump()


async def test_message_passes_resolved_target_and_coordinates(monkeypatch):
    fetch = AsyncMock(return_value=_message())
    monkeypatch.setattr(routes_kafka_read, 'fetch_message', fetch)
    async with _client() as client:
        await client.get('/api/v1/debug/kafka/search-results-kafka-sink/message?partition=2&offset=9')
    target, partition, offset = fetch.await_args.args
    assert isinstance(target, AliasTarget)
    assert target.topic == 'search-results'
    assert (partition, offset) == (2, 9)


@pytest.mark.parametrize(
    ('exc', 'status'),
    [
        (KafkaReadNotFound('offset gone'), 404),
        (KafkaReadUnavailable('brokers down'), 502),
    ],
)
async def test_message_maps_reader_errors_to_http(monkeypatch, exc, status):
    monkeypatch.setattr(routes_kafka_read, 'fetch_message', AsyncMock(side_effect=exc))
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/message?partition=0&offset=7')
    assert resp.status_code == status
    assert resp.json()['detail'] == str(exc)


# ---------------------------------------------------------------------------
# /api/debug/kafka/{alias}/messages (NDJSON)
# ---------------------------------------------------------------------------


def _stream_stub(items):
    """Build a stream_messages replacement yielding the given items;
    an exception instance in the list is raised at that point."""

    def _factory(target, **kwargs):
        async def agen():
            for item in items:
                if isinstance(item, Exception):
                    raise item
                yield item

        return agen()

    return _factory


async def test_messages_streams_ndjson_lines(monkeypatch):
    msgs = [_message(offset=1), _message(offset=2)]
    monkeypatch.setattr(routes_kafka_read, 'stream_messages', _stream_stub(msgs))
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/messages?from_ts=0')
    assert resp.status_code == 200
    assert resp.headers['content-type'].startswith('application/x-ndjson')
    lines = [json.loads(line) for line in resp.text.splitlines()]
    assert [line['offset'] for line in lines] == [1, 2]


async def test_messages_empty_window_is_empty_200(monkeypatch):
    monkeypatch.setattr(routes_kafka_read, 'stream_messages', _stream_stub([]))
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/messages?from_ts=0')
    assert resp.status_code == 200
    assert resp.text == ''


async def test_messages_first_item_error_maps_to_http_status(monkeypatch):
    monkeypatch.setattr(
        routes_kafka_read, 'stream_messages', _stream_stub([KafkaReadNotFound('Partition 7 does not exist')])
    )
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/messages?from_ts=0&partition=7')
    assert resp.status_code == 404


async def test_messages_mid_stream_failure_emits_error_line(monkeypatch):
    monkeypatch.setattr(
        routes_kafka_read,
        'stream_messages',
        _stream_stub([_message(offset=1), _message(offset=2), KafkaReadUnavailable('brokers went away')]),
    )
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/messages?from_ts=0')
    assert resp.status_code == 200  # headers were committed before the failure
    lines = [json.loads(line) for line in resp.text.splitlines()]
    assert [line.get('offset') for line in lines[:2]] == [1, 2]
    assert lines[-1] == {'error': 'brokers went away'}


async def test_messages_to_ts_before_from_ts_is_422():
    async with _client() as client:
        resp = await client.get('/api/v1/debug/kafka/source/messages?from_ts=100&to_ts=50')
    assert resp.status_code == 422
    assert resp.json()['detail'][0]['loc'] == ['query', 'to_ts']


# ---------------------------------------------------------------------------
# startup exposure warning
# ---------------------------------------------------------------------------


def _aliases(protocol: str = 'PLAINTEXT') -> dict[str, AliasTarget]:
    security = (
        KafkaSecurityConfig()
        if protocol == 'PLAINTEXT'
        else KafkaSecurityConfig(
            protocol='SASL_PLAINTEXT', sasl_mechanism='PLAIN', sasl_username='u', sasl_password='p'
        )
    )
    return {
        'source': AliasTarget(
            alias='source', kind='source', topic='t', brokers='b:9092', security=security, client_config={}
        )
    }


@pytest.mark.parametrize(
    ('auth_token', 'enabled', 'protocol', 'expect_warning'),
    [
        ('', True, 'SASL_PLAINTEXT', True),  # secured Kafka, open UI → warn
        ('secret', True, 'SASL_PLAINTEXT', False),  # UI auth closes the gap
        ('', False, 'SASL_PLAINTEXT', False),  # API closed → nothing exposed
        ('', True, 'PLAINTEXT', False),  # Kafka itself is open → nothing new exposed
    ],
)
def test_exposure_warning_matrix(auth_token, enabled, protocol, expect_warning):
    with structlog.testing.capture_logs() as logs:
        _warn_if_exposed_without_ui_auth(auth_token, enabled, _aliases(protocol))
    events = [log['event'] for log in logs]
    assert ('kafka_read_exposed_without_ui_auth' in events) is expect_warning


def test_exposure_warning_names_the_exposed_aliases():
    with structlog.testing.capture_logs() as logs:
        _warn_if_exposed_without_ui_auth('', True, _aliases('SASL_PLAINTEXT'))
    (warning,) = [log for log in logs if log['event'] == 'kafka_read_exposed_without_ui_auth']
    assert warning['aliases'] == ['source']
