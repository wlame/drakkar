"""Tests for individual sink implementations."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pydantic import BaseModel

from drakkar.config import (
    FileSinkConfig,
    HttpSinkConfig,
    KafkaSinkConfig,
    MongoSinkConfig,
    PostgresSinkConfig,
    RedisSinkConfig,
)
from drakkar.models import (
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MongoPayload,
    PostgresPayload,
    RedisPayload,
)


class SampleOutput(BaseModel):
    request_id: str = 'abc'
    answer: str = '42'


def _make_future():
    """Create a resolved asyncio.Future for mocking produce() return values."""
    f = asyncio.get_event_loop().create_future()
    f.set_result(None)
    return f


# =============================================================================
# Kafka sink
# =============================================================================


@pytest.fixture
def kafka_sink_config():
    return KafkaSinkConfig(topic='test-results')


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_connect(mock_cls, kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    mock_cls.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
    assert sink._producer is not None


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_uses_config_brokers_over_fallback(mock_cls):
    config = KafkaSinkConfig(topic='t', brokers='custom:9092')
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('out', config, brokers_fallback='fallback:9092')
    await sink.connect()

    call_args = mock_cls.call_args[0][0]
    assert call_args['bootstrap.servers'] == 'custom:9092'


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_single(mock_cls, kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    payload = KafkaPayload(key=b'k1', data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs['topic'] == 'test-results'
    assert call_kwargs['key'] == b'k1'
    assert b'"request_id":"r1"' in call_kwargs['value']
    mock_producer.flush.assert_called_once()


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_batch(mock_cls, kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    payloads = [
        KafkaPayload(key=b'k1', data=SampleOutput(request_id='r1')),
        KafkaPayload(key=b'k2', data=SampleOutput(request_id='r2')),
        KafkaPayload(data=SampleOutput(request_id='r3')),
    ]
    await sink.deliver(payloads)

    assert mock_producer.produce.call_count == 3
    mock_producer.flush.assert_called_once()


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_empty(mock_cls, kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    await sink.deliver([])
    mock_producer.produce.assert_not_called()


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_error_increments_metrics(mock_cls, kafka_sink_config):
    from drakkar.metrics import sink_deliver_errors
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = RuntimeError('broker down')
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    labels = {'sink_type': 'kafka', 'sink_name': 'results'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(RuntimeError, match='broker down'):
        await sink.deliver([KafkaPayload(data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_success_increments_metrics(mock_cls, kafka_sink_config):
    from drakkar.metrics import sink_payloads_delivered
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    labels = {'sink_type': 'kafka', 'sink_name': 'results'}
    before = sink_payloads_delivered.labels(**labels)._value.get()

    await sink.deliver([KafkaPayload(data=SampleOutput()), KafkaPayload(data=SampleOutput())])

    assert sink_payloads_delivered.labels(**labels)._value.get() == before + 2


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_close(mock_cls, kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()
    await sink.close()

    mock_producer.close.assert_called_once()
    assert sink._producer is None


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_close_not_connected(mock_cls, kafka_sink_config):
    """Close on unconnected sink is a no-op."""
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.close()  # should not raise


def test_kafka_sink_topic_property(kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    assert sink.topic == 'test-results'


def test_kafka_sink_type():
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('x', KafkaSinkConfig(topic='t'))
    assert sink.sink_type == 'kafka'


# =============================================================================
# PostgreSQL sink
# =============================================================================


class DBResultModel(BaseModel):
    id: int = 1
    status: str = 'done'
    score: float = 0.95


@pytest.fixture
def pg_sink_config():
    return PostgresSinkConfig(dsn='postgresql://localhost/testdb')


def _mock_asyncpg_pool():
    """Create a mock asyncpg pool with async context manager for acquire()."""
    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_pool, mock_conn


class _FakeAcquireCtx:
    """Async context manager that returns a mock connection."""

    def __init__(self, conn: AsyncMock) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        pass


def _make_pg_sink(pg_sink_config):
    """Helper: create a PostgresSink with mocked asyncpg pool, return (sink, mock_conn, mock_pool)."""
    from unittest.mock import MagicMock

    from drakkar.sinks.postgres import PostgresSink

    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value = _FakeAcquireCtx(mock_conn)
    mock_pool.close = AsyncMock()

    sink = PostgresSink('main', pg_sink_config)
    sink._pool = mock_pool  # bypass connect() to avoid mocking create_pool
    return sink, mock_conn, mock_pool


async def test_postgres_sink_connect(pg_sink_config):
    from drakkar.sinks.postgres import PostgresSink

    sink = PostgresSink('main', pg_sink_config)
    mock_pool = AsyncMock()

    async def fake_create_pool(**kwargs):
        return mock_pool

    with patch(
        'drakkar.sinks.postgres.asyncpg.create_pool', side_effect=fake_create_pool
    ) as mock_cp:
        await sink.connect()
        mock_cp.assert_called_once_with(
            dsn='postgresql://localhost/testdb',
            min_size=2,
            max_size=10,
        )
    assert sink.pool is mock_pool


async def test_postgres_sink_deliver(pg_sink_config):
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payload = PostgresPayload(table='results', data=DBResultModel(id=42, status='ok'))
    await sink.deliver([payload])

    mock_conn.execute.assert_called_once()
    query = mock_conn.execute.call_args[0][0]
    assert '"results"' in query
    assert '"id"' in query
    assert '"status"' in query
    assert '"score"' in query


async def test_postgres_sink_deliver_batch(pg_sink_config):
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='results', data=DBResultModel(id=2)),
    ]
    await sink.deliver(payloads)

    assert mock_conn.execute.call_count == 2


async def test_postgres_sink_deliver_empty(pg_sink_config):
    sink, _, mock_pool = _make_pg_sink(pg_sink_config)

    await sink.deliver([])
    mock_pool.acquire.assert_not_called()


async def test_postgres_sink_sql_injection_table(pg_sink_config):
    """Reject suspicious table names."""
    sink, _, _ = _make_pg_sink(pg_sink_config)

    payload = PostgresPayload(table='users; DROP TABLE users--', data=DBResultModel())
    with pytest.raises(ValueError, match='Invalid SQL identifier'):
        await sink.deliver([payload])


async def test_postgres_sink_sql_injection_column():
    """Reject suspicious column names via _quote_ident."""
    from drakkar.sinks.postgres import _quote_ident

    assert _quote_ident('valid_name') == '"valid_name"'
    with pytest.raises(ValueError):
        _quote_ident('col; DROP TABLE x')


async def test_postgres_sink_deliver_error_increments_metrics(pg_sink_config):
    from drakkar.metrics import sink_deliver_errors

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)
    mock_conn.execute.side_effect = RuntimeError('connection lost')

    labels = {'sink_type': 'postgres', 'sink_name': 'main'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(RuntimeError, match='connection lost'):
        await sink.deliver([PostgresPayload(table='t', data=DBResultModel())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_postgres_sink_close(pg_sink_config):
    sink, _, mock_pool = _make_pg_sink(pg_sink_config)
    await sink.close()

    mock_pool.close.assert_called_once()
    assert sink.pool is None


async def test_postgres_sink_close_not_connected(pg_sink_config):
    from drakkar.sinks.postgres import PostgresSink

    sink = PostgresSink('main', pg_sink_config)
    await sink.close()  # should not raise


def test_postgres_sink_type(pg_sink_config):
    from drakkar.sinks.postgres import PostgresSink

    sink = PostgresSink('main', pg_sink_config)
    assert sink.sink_type == 'postgres'


# =============================================================================
# MongoDB sink
# =============================================================================


@pytest.fixture
def mongo_sink_config():
    return MongoSinkConfig(uri='mongodb://localhost:27017', database='testdb')


def _make_mongo_sink(mongo_sink_config):
    """Helper: create a MongoSink with mocked motor client."""
    from unittest.mock import MagicMock

    from drakkar.sinks.mongo import MongoSink

    mock_collection = AsyncMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)

    sink = MongoSink('analytics', mongo_sink_config)
    sink._client = mock_client
    sink._db = mock_db
    return sink, mock_collection, mock_client


async def test_mongo_sink_connect(mongo_sink_config):
    from drakkar.sinks.mongo import MongoSink

    with patch('motor.motor_asyncio.AsyncIOMotorClient') as mock_cls:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_cls.return_value = mock_client

        sink = MongoSink('analytics', mongo_sink_config)
        await sink.connect()

        mock_cls.assert_called_once_with('mongodb://localhost:27017')
        assert sink._db is mock_db


async def test_mongo_sink_deliver(mongo_sink_config):
    sink, mock_collection, _ = _make_mongo_sink(mongo_sink_config)

    payload = MongoPayload(collection='results', data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    mock_collection.insert_one.assert_called_once()
    doc = mock_collection.insert_one.call_args[0][0]
    assert doc['request_id'] == 'r1'
    assert doc['answer'] == '42'


async def test_mongo_sink_deliver_batch(mongo_sink_config):
    sink, mock_collection, _ = _make_mongo_sink(mongo_sink_config)

    payloads = [
        MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
        MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
    ]
    await sink.deliver(payloads)

    assert mock_collection.insert_one.call_count == 2


async def test_mongo_sink_deliver_empty(mongo_sink_config):
    sink, mock_collection, _ = _make_mongo_sink(mongo_sink_config)

    await sink.deliver([])
    mock_collection.insert_one.assert_not_called()


async def test_mongo_sink_deliver_error_increments_metrics(mongo_sink_config):
    from drakkar.metrics import sink_deliver_errors

    sink, mock_collection, _ = _make_mongo_sink(mongo_sink_config)
    mock_collection.insert_one.side_effect = RuntimeError('connection refused')

    labels = {'sink_type': 'mongo', 'sink_name': 'analytics'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(RuntimeError, match='connection refused'):
        await sink.deliver([MongoPayload(collection='c', data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_mongo_sink_close(mongo_sink_config):
    sink, _, mock_client = _make_mongo_sink(mongo_sink_config)
    await sink.close()

    mock_client.close.assert_called_once()
    assert sink._client is None
    assert sink._db is None


async def test_mongo_sink_close_not_connected(mongo_sink_config):
    from drakkar.sinks.mongo import MongoSink

    sink = MongoSink('analytics', mongo_sink_config)
    await sink.close()  # should not raise


def test_mongo_sink_type(mongo_sink_config):
    from drakkar.sinks.mongo import MongoSink

    sink = MongoSink('analytics', mongo_sink_config)
    assert sink.sink_type == 'mongo'


# =============================================================================
# HTTP sink
# =============================================================================


@pytest.fixture
def http_sink_config():
    return HttpSinkConfig(url='https://api.example.com/results')


def _make_http_sink(http_sink_config):
    """Helper: create an HttpSink with a mocked httpx client."""
    from drakkar.sinks.http import HttpSink

    mock_client = AsyncMock()
    sink = HttpSink('webhook', http_sink_config)
    sink._client = mock_client
    return sink, mock_client


def _mock_response(status_code: int = 200):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f'{status_code} error',
            request=MagicMock(),
            response=resp,
        )
    return resp


async def test_http_sink_connect(http_sink_config):
    from drakkar.sinks.http import HttpSink

    sink = HttpSink('webhook', http_sink_config)
    await sink.connect()

    assert sink._client is not None
    assert sink._client.timeout.connect == 30
    await sink.close()


async def test_http_sink_connect_custom_headers():
    from drakkar.sinks.http import HttpSink

    config = HttpSinkConfig(
        url='https://api.example.com',
        headers={'Authorization': 'Bearer token123'},
    )
    sink = HttpSink('webhook', config)
    await sink.connect()

    assert 'Authorization' in sink._client.headers
    await sink.close()


async def test_http_sink_deliver(http_sink_config):

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(200)

    payload = HttpPayload(data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    mock_client.request.assert_called_once()
    call_kwargs = mock_client.request.call_args[1]
    assert call_kwargs['method'] == 'POST'
    assert call_kwargs['url'] == 'https://api.example.com/results'
    assert b'"request_id":"r1"' in call_kwargs['content'].encode()


async def test_http_sink_deliver_batch(http_sink_config):

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(200)

    payloads = [
        HttpPayload(data=SampleOutput(request_id='r1')),
        HttpPayload(data=SampleOutput(request_id='r2')),
    ]
    await sink.deliver(payloads)

    assert mock_client.request.call_count == 2


async def test_http_sink_deliver_empty(http_sink_config):
    sink, mock_client = _make_http_sink(http_sink_config)

    await sink.deliver([])
    mock_client.request.assert_not_called()


async def test_http_sink_deliver_4xx_raises(http_sink_config):
    import httpx

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(400)

    with pytest.raises(httpx.HTTPStatusError):
        await sink.deliver([HttpPayload(data=SampleOutput())])


async def test_http_sink_deliver_5xx_raises(http_sink_config):
    import httpx

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(500)

    with pytest.raises(httpx.HTTPStatusError):
        await sink.deliver([HttpPayload(data=SampleOutput())])


async def test_http_sink_deliver_timeout_raises(http_sink_config):
    import httpx

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.side_effect = httpx.ConnectTimeout('timeout')

    with pytest.raises(httpx.ConnectTimeout):
        await sink.deliver([HttpPayload(data=SampleOutput())])


async def test_http_sink_deliver_error_increments_metrics(http_sink_config):
    import httpx

    from drakkar.metrics import sink_deliver_errors

    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(500)

    labels = {'sink_type': 'http', 'sink_name': 'webhook'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(httpx.HTTPStatusError):
        await sink.deliver([HttpPayload(data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_http_sink_custom_method():

    config = HttpSinkConfig(url='https://api.example.com', method='PUT')
    from drakkar.sinks.http import HttpSink

    sink = HttpSink('webhook', config)
    mock_client = AsyncMock()
    mock_client.request.return_value = _mock_response(200)
    sink._client = mock_client

    await sink.deliver([HttpPayload(data=SampleOutput())])

    assert mock_client.request.call_args[1]['method'] == 'PUT'


async def test_http_sink_close(http_sink_config):
    sink, mock_client = _make_http_sink(http_sink_config)
    await sink.close()

    mock_client.aclose.assert_called_once()
    assert sink._client is None


async def test_http_sink_close_not_connected(http_sink_config):
    from drakkar.sinks.http import HttpSink

    sink = HttpSink('webhook', http_sink_config)
    await sink.close()  # should not raise


def test_http_sink_type(http_sink_config):
    from drakkar.sinks.http import HttpSink

    sink = HttpSink('webhook', http_sink_config)
    assert sink.sink_type == 'http'


# =============================================================================
# Redis sink
# =============================================================================


@pytest.fixture
def redis_sink_config():
    return RedisSinkConfig(url='redis://localhost:6379/0', key_prefix='drakkar:')


def _make_redis_sink(redis_sink_config):
    """Helper: create a RedisSink with a mocked redis client."""
    from drakkar.sinks.redis import RedisSink

    mock_client = AsyncMock()
    sink = RedisSink('cache', redis_sink_config)
    sink._client = mock_client
    return sink, mock_client


async def test_redis_sink_connect(redis_sink_config):
    from drakkar.sinks.redis import RedisSink

    with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        sink = RedisSink('cache', redis_sink_config)
        await sink.connect()

        mock_from_url.assert_called_once_with('redis://localhost:6379/0')
        assert sink._client is mock_client


async def test_redis_sink_deliver_without_ttl(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)

    payload = RedisPayload(key='result:abc', data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    mock_client.set.assert_called_once()
    call_args = mock_client.set.call_args
    assert call_args[0][0] == 'drakkar:result:abc'
    assert '"request_id":"r1"' in call_args[0][1]
    assert 'ex' not in call_args[1]


async def test_redis_sink_deliver_with_ttl(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)

    payload = RedisPayload(key='cache:abc', data=SampleOutput(), ttl=3600)
    await sink.deliver([payload])

    mock_client.set.assert_called_once()
    call_kwargs = mock_client.set.call_args[1]
    assert call_kwargs['ex'] == 3600


async def test_redis_sink_deliver_batch(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)

    payloads = [
        RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
        RedisPayload(key='k2', data=SampleOutput(request_id='r2'), ttl=60),
    ]
    await sink.deliver(payloads)

    assert mock_client.set.call_count == 2


async def test_redis_sink_deliver_empty(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([])
    mock_client.set.assert_not_called()


async def test_redis_sink_key_prefix(redis_sink_config):
    """Key prefix from config is prepended to payload key."""
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(key='mykey', data=SampleOutput())])

    full_key = mock_client.set.call_args[0][0]
    assert full_key == 'drakkar:mykey'


async def test_redis_sink_no_prefix():
    """Empty key prefix passes key through as-is."""
    from drakkar.sinks.redis import RedisSink

    config = RedisSinkConfig(url='redis://localhost:6379/0', key_prefix='')
    mock_client = AsyncMock()
    sink = RedisSink('cache', config)
    sink._client = mock_client

    await sink.deliver([RedisPayload(key='raw-key', data=SampleOutput())])

    full_key = mock_client.set.call_args[0][0]
    assert full_key == 'raw-key'


async def test_redis_sink_deliver_error_increments_metrics(redis_sink_config):
    from drakkar.metrics import sink_deliver_errors

    sink, mock_client = _make_redis_sink(redis_sink_config)
    mock_client.set.side_effect = RuntimeError('connection refused')

    labels = {'sink_type': 'redis', 'sink_name': 'cache'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(RuntimeError, match='connection refused'):
        await sink.deliver([RedisPayload(key='k', data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_redis_sink_close(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)
    await sink.close()

    mock_client.aclose.assert_called_once()
    assert sink._client is None


async def test_redis_sink_close_not_connected(redis_sink_config):
    from drakkar.sinks.redis import RedisSink

    sink = RedisSink('cache', redis_sink_config)
    await sink.close()  # should not raise


def test_redis_sink_type(redis_sink_config):
    from drakkar.sinks.redis import RedisSink

    sink = RedisSink('cache', redis_sink_config)
    assert sink.sink_type == 'redis'


# =============================================================================
# Filesystem sink
# =============================================================================


async def test_file_sink_connect_no_base_path():
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    await sink.connect()  # no-op, should not raise


async def test_file_sink_connect_valid_base_path(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    await sink.connect()  # should not raise


async def test_file_sink_connect_invalid_base_path():
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path='/nonexistent/path'))
    with pytest.raises(FileNotFoundError, match='base_path does not exist'):
        await sink.connect()


async def test_file_sink_deliver_creates_file(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    out_file = tmp_path / 'output.jsonl'

    payload = FilePayload(path=str(out_file), data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    assert out_file.exists()
    lines = out_file.read_text().splitlines()
    assert len(lines) == 1
    assert '"request_id":"r1"' in lines[0]


async def test_file_sink_deliver_appends_to_existing(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    out_file = tmp_path / 'output.jsonl'
    out_file.write_text('{"existing":"line"}\n')

    payload = FilePayload(path=str(out_file), data=SampleOutput(request_id='r2'))
    await sink.deliver([payload])

    lines = out_file.read_text().splitlines()
    assert len(lines) == 2
    assert lines[0] == '{"existing":"line"}'
    assert '"request_id":"r2"' in lines[1]


async def test_file_sink_deliver_batch(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    out_file = tmp_path / 'batch.jsonl'

    payloads = [
        FilePayload(path=str(out_file), data=SampleOutput(request_id='r1')),
        FilePayload(path=str(out_file), data=SampleOutput(request_id='r2')),
        FilePayload(path=str(out_file), data=SampleOutput(request_id='r3')),
    ]
    await sink.deliver(payloads)

    lines = out_file.read_text().splitlines()
    assert len(lines) == 3


async def test_file_sink_deliver_different_files(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    file_a = tmp_path / 'a.jsonl'
    file_b = tmp_path / 'b.jsonl'

    payloads = [
        FilePayload(path=str(file_a), data=SampleOutput(request_id='r1')),
        FilePayload(path=str(file_b), data=SampleOutput(request_id='r2')),
    ]
    await sink.deliver(payloads)

    assert file_a.exists()
    assert file_b.exists()
    assert '"r1"' in file_a.read_text()
    assert '"r2"' in file_b.read_text()


async def test_file_sink_deliver_empty():
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    await sink.deliver([])  # should not raise


async def test_file_sink_deliver_missing_parent_dir(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    bad_path = tmp_path / 'nonexistent' / 'output.jsonl'

    with pytest.raises(FileNotFoundError, match='Parent directory does not exist'):
        await sink.deliver([FilePayload(path=str(bad_path), data=SampleOutput())])


async def test_file_sink_deliver_error_increments_metrics(tmp_path):
    from drakkar.metrics import sink_deliver_errors
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    bad_path = tmp_path / 'nonexistent' / 'output.jsonl'

    labels = {'sink_type': 'filesystem', 'sink_name': 'output'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(FileNotFoundError):
        await sink.deliver([FilePayload(path=str(bad_path), data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_file_sink_close():
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    await sink.close()  # no-op, should not raise


def test_file_sink_type():
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig())
    assert sink.sink_type == 'filesystem'


# =============================================================================
# DLQ sink
# =============================================================================


def _make_dlq_sink():
    """Helper: create a DLQSink with a mocked AIOProducer."""
    from drakkar.sinks.dlq import DLQSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    sink = DLQSink(topic='test-dlq', brokers='localhost:9092')
    sink._producer = mock_producer
    return sink, mock_producer


def _sample_delivery_error():
    from drakkar.models import DeliveryError

    return DeliveryError(
        sink_name='results',
        sink_type='kafka',
        error='BrokerNotAvailableError: broker down',
        payloads=[SampleOutput(request_id='r1')],
    )


@patch('drakkar.sinks.dlq.AIOProducer')
async def test_dlq_sink_connect(mock_cls):
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='my-dlq', brokers='kafka:9092')
    await sink.connect()

    mock_cls.assert_called_once_with({'bootstrap.servers': 'kafka:9092'})
    assert sink._producer is not None


async def test_dlq_sink_send():
    sink, mock_producer = _make_dlq_sink()
    error = _sample_delivery_error()

    await sink.send(error, partition_id=3, attempt_count=2)

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs['topic'] == 'test-dlq'

    import json

    body = json.loads(call_kwargs['value'])
    assert body['sink_name'] == 'results'
    assert body['sink_type'] == 'kafka'
    assert 'BrokerNotAvailableError' in body['error']
    assert body['partition'] == 3
    assert body['attempt_count'] == 2
    assert len(body['original_payloads']) == 1


async def test_dlq_sink_send_increments_metric():
    from drakkar.metrics import sink_dlq_messages

    sink, _ = _make_dlq_sink()
    before = sink_dlq_messages._value.get()

    await sink.send(_sample_delivery_error(), partition_id=0)

    assert sink_dlq_messages._value.get() == before + 1


async def test_dlq_sink_send_not_connected():
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    # should log warning but not raise
    await sink.send(_sample_delivery_error(), partition_id=0)


async def test_dlq_sink_send_produce_failure():
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = RuntimeError('kafka down')
    sink._producer = mock_producer

    # should log error but not raise (DLQ is last resort)
    await sink.send(_sample_delivery_error(), partition_id=0)


async def test_dlq_message_serialization():
    from drakkar.sinks.dlq import DLQMessage

    error = _sample_delivery_error()
    msg = DLQMessage(delivery_error=error, partition_id=5, attempt_count=3)
    data = msg.serialize()

    import json

    parsed = json.loads(data)
    assert parsed['sink_name'] == 'results'
    assert parsed['sink_type'] == 'kafka'
    assert parsed['partition'] == 5
    assert parsed['attempt_count'] == 3
    assert 'timestamp' in parsed
    assert len(parsed['original_payloads']) == 1


async def test_dlq_sink_close():
    sink, mock_producer = _make_dlq_sink()
    await sink.close()

    mock_producer.close.assert_called_once()
    assert sink._producer is None


async def test_dlq_sink_close_not_connected():
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    await sink.close()  # should not raise


def test_dlq_sink_topic():
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='my-dlq', brokers='localhost:9092')
    assert sink.topic == 'my-dlq'
    assert sink.sink_type == 'dlq'
