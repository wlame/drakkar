"""Tests for individual sink implementations."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from drakkar.config import KafkaSinkConfig, PostgresSinkConfig
from drakkar.models import KafkaPayload, PostgresPayload


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
