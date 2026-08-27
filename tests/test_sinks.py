"""Tests for individual sink implementations."""

import asyncio
import inspect
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pydantic import BaseModel, create_model
from structlog.testing import capture_logs

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
from drakkar.sinks.mongo import MongoWriteError


class SampleOutput(BaseModel):
    request_id: str = 'abc'
    answer: str = '42'


class _JobKey(BaseModel):
    """A minimal equality filter for the Mongo operation tests."""

    id: int = 7


class _Empty(BaseModel):
    """Dumps to {} — the shape both unbounded-write guards must refuse."""


def _make_mock_message():
    """Create a mock confluent_kafka Message for resolved delivery futures."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = 'test-results'
    msg.partition.return_value = 0
    msg.offset.return_value = 42
    return msg


def _make_future():
    """Create a resolved asyncio.Future for mocking produce() return values."""
    f = asyncio.get_event_loop().create_future()
    f.set_result(_make_mock_message())
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
    mock_producer.flush.return_value = 0
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
    mock_producer.flush.return_value = 0
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


async def test_kafka_sink_deliver_not_connected_raises(kafka_sink_config):
    """A silent return would let the offset commit past lost payloads."""
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    with pytest.raises(RuntimeError, match='not connected'):
        await sink.deliver([KafkaPayload(data=SampleOutput())])


async def test_kafka_sink_deliver_empty_not_connected_returns(kafka_sink_config):
    """The empty-batch early return stays silent even before connect()."""
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.deliver([])  # must not raise


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
    mock_producer.flush.return_value = 0
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


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_with_falsy_producer(mock_cls, kafka_sink_config):
    """Deliver works even when AIOProducer.__len__ returns 0 (empty queue).

    AIOProducer defines __len__ but not __bool__, so bool(producer) is False
    when the internal queue is empty. We must use 'is None' checks, not truthiness.
    """
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_producer.flush.return_value = 0
    mock_producer.__len__ = lambda self: 0  # empty queue → bool() returns False
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    await sink.deliver([KafkaPayload(data=SampleOutput())])
    mock_producer.produce.assert_called_once()


def test_kafka_sink_topic_property(kafka_sink_config):
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    assert sink.topic == 'test-results'


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_flush_incomplete_raises_timeout(mock_cls, kafka_sink_config):
    """A flush that leaves messages queued is a transient failure.

    ``TimeoutError``, not ``RuntimeError``: the sink manager classifies it
    as transient, so the circuit breaker sees the broker outage and an
    idempotent sink still gets its fast-retry. A ``RuntimeError`` would look
    like a payload bug and skip both.
    """
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_producer.flush.return_value = 3
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    with pytest.raises(TimeoutError, match=r'flush timed out.*3 message'):
        await sink.deliver([KafkaPayload(data=SampleOutput())])


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_flush_is_bounded_by_the_configured_timeout(mock_cls, kafka_sink_config):
    """``flush()`` with no argument is librdkafka's ``flush(-1)``: it blocks
    until ``message.timeout.ms`` (300s by default) against a wedged broker,
    holding one of the producer's executor threads while it waits. The outer
    delivery timeout cannot rescue that — cancelling the await does not stop
    the thread — so the bound has to reach librdkafka itself.
    """
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_producer.flush.return_value = 0
    mock_cls.return_value = mock_producer

    cfg = kafka_sink_config.model_copy(update={'flush_timeout_seconds': 12.5})
    sink = KafkaSink('results', cfg, brokers_fallback='localhost:9092')
    await sink.connect()
    await sink.deliver([KafkaPayload(data=SampleOutput())])

    mock_producer.flush.assert_called_once_with(12.5)


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_future_none_raises(mock_cls, kafka_sink_config):
    """Delivery fails if a produce future resolves to None."""
    from drakkar.sinks.kafka import KafkaSink

    def _make_none_future():
        f = asyncio.get_event_loop().create_future()
        f.set_result(None)
        return f

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_none_future()
    mock_producer.flush.return_value = 0
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    with pytest.raises(RuntimeError, match='future resolved to None'):
        await sink.deliver([KafkaPayload(data=SampleOutput())])


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_deliver_future_with_error_raises(mock_cls, kafka_sink_config):
    """Delivery fails if a produce future contains a Kafka error."""
    from drakkar.sinks.kafka import KafkaSink

    def _make_error_future():
        msg = MagicMock()
        msg.error.return_value = MagicMock(__str__=lambda self: 'MSG_SIZE_TOO_LARGE')
        f = asyncio.get_event_loop().create_future()
        f.set_result(msg)
        return f

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_error_future()
    mock_producer.flush.return_value = 0
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    with pytest.raises(RuntimeError, match='delivery error'):
        await sink.deliver([KafkaPayload(data=SampleOutput())])


@pytest.mark.parametrize('sink_name', ['kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'])
def test_sink_type_reports_backend_name(sink_name, tmp_path):
    """Every sink's ``sink_type`` equals its backend name (used as a metrics label)."""
    from drakkar.sinks.filesystem import FileSink
    from drakkar.sinks.http import HttpSink
    from drakkar.sinks.kafka import KafkaSink
    from drakkar.sinks.mongo import MongoSink
    from drakkar.sinks.postgres import PostgresSink
    from drakkar.sinks.redis import RedisSink

    factories = {
        'kafka': lambda: KafkaSink('x', KafkaSinkConfig(topic='t')),
        'postgres': lambda: PostgresSink('main', PostgresSinkConfig(dsn='postgresql://localhost/testdb')),
        'mongo': lambda: MongoSink('analytics', MongoSinkConfig(uri='mongodb://localhost:27017', database='testdb')),
        'http': lambda: HttpSink('webhook', HttpSinkConfig(url='https://api.example.com/results')),
        'redis': lambda: RedisSink('cache', RedisSinkConfig(url='redis://localhost:6379/0', key_prefix='drakkar:')),
        'filesystem': lambda: FileSink('output', FileSinkConfig(base_path=str(tmp_path))),
    }
    assert factories[sink_name]().sink_type == sink_name


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

    with patch('drakkar.sinks.postgres.asyncpg.create_pool', side_effect=fake_create_pool) as mock_cp:
        await sink.connect()
        mock_cp.assert_called_once_with(
            dsn='postgresql://localhost/testdb',
            min_size=2,
            max_size=10,
            # The delivery budget doubles as the driver's command timeout so
            # a wedged server raises asyncpg's own error, naming the query,
            # rather than being cancelled anonymously by the manager.
            command_timeout=30.0,
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
    """Rows sharing a (table, column-set) go out as ONE multi-row INSERT."""
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='results', data=DBResultModel(id=2)),
    ]
    await sink.deliver(payloads)

    assert mock_conn.execute.call_count == 1
    query, *values = mock_conn.execute.call_args[0]
    assert query.count('INSERT INTO') == 1
    # Two value tuples, with parameters numbered continuously across them.
    assert '$1' in query and f'${len(values)}' in query
    assert query.count('), (') == 1


async def test_postgres_sink_groups_only_consecutive_runs(pg_sink_config):
    """Payloads batch only with adjacent same-shaped neighbours.

    Global bucketing would merge the two 'results' rows into one statement
    and run it before 'audit', reordering payload 2 past payload 3. That is
    harmless for INSERT but a lost update once UPDATE exists, so grouping is
    restricted to consecutive runs and execution order always equals payload
    order.
    """
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='audit', data=DBResultModel(id=2)),
        PostgresPayload(table='results', data=DBResultModel(id=3)),
    ]
    await sink.deliver(payloads)

    assert mock_conn.execute.call_count == 3
    tables = [call[0][0].split()[2] for call in mock_conn.execute.call_args_list]
    assert tables == ['"results"', '"audit"', '"results"'], 'execution order must follow payload order'


async def test_postgres_sink_batches_adjacent_same_shape_payloads(pg_sink_config):
    """Adjacent same-shaped payloads still collapse into one statement."""
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='results', data=DBResultModel(id=2)),
        PostgresPayload(table='audit', data=DBResultModel(id=3)),
    ]
    await sink.deliver(payloads)

    assert mock_conn.execute.call_count == 2
    assert mock_conn.execute.call_args_list[0][0][0].count('), (') == 1


async def test_postgres_sink_batch_failure_falls_back_per_row(pg_sink_config):
    """A failed batch is retried row-by-row so the error names the bad row.

    Without this, batching would coarsen failure granularity: one bad row
    would fail the whole statement and the operator would lose which
    payload caused it. Divergence #18 promises identical attribution.
    """
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    calls: list[str] = []

    async def execute(query, *values):
        calls.append(query)
        # Fail only the multi-row statement, not the per-row retries.
        if query.count('), (') >= 1:
            raise RuntimeError('batch rejected')

    mock_conn.execute.side_effect = execute

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='results', data=DBResultModel(id=2)),
    ]
    await sink.deliver(payloads)

    # One batch attempt + one statement per row.
    assert len(calls) == 3
    assert calls[0].count('), (') == 1
    assert all(c.count('), (') == 0 for c in calls[1:])


async def test_postgres_sink_bad_payload_keeps_preceding_side_effects(pg_sink_config):
    """The rows before an invalid payload are still inserted, then it raises.

    The pre-batching loop executed each row as it went, so a bad payload
    halfway through left the earlier rows committed. Building all rows up
    front must not silently turn that into all-or-nothing.
    """
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(table='results', data=DBResultModel(id=1)),
        PostgresPayload(table='bad; DROP TABLE x--', data=DBResultModel(id=2)),
        PostgresPayload(table='results', data=DBResultModel(id=3)),
    ]
    with pytest.raises(ValueError, match='Invalid SQL identifier'):
        await sink.deliver(payloads)

    # Only the row before the bad payload was executed.
    assert mock_conn.execute.call_count == 1
    assert '"results"' in mock_conn.execute.call_args[0][0]


async def test_postgres_sink_deliver_empty(pg_sink_config):
    sink, _, mock_pool = _make_pg_sink(pg_sink_config)

    await sink.deliver([])
    mock_pool.acquire.assert_not_called()


async def test_postgres_sink_deliver_not_connected_raises(pg_sink_config):
    """A silent return would let the offset commit past lost payloads."""
    from drakkar.sinks.postgres import PostgresSink

    sink = PostgresSink('main', pg_sink_config)
    with pytest.raises(RuntimeError, match='not connected'):
        await sink.deliver([PostgresPayload(table='results', data=SampleOutput())])


async def test_postgres_sink_sql_injection_table(pg_sink_config):
    """Reject suspicious table names."""
    sink, _, _ = _make_pg_sink(pg_sink_config)

    payload = PostgresPayload(table='users; DROP TABLE users--', data=DBResultModel())
    with pytest.raises(ValueError, match='Invalid SQL identifier'):
        await sink.deliver([payload])


async def test_postgres_sink_sql_injection_column():
    """Reject suspicious column names via quote_ident.

    The exhaustive rejection cases live in ``tests/test_pgsql.py`` against
    the pure module; this keeps a sink-level smoke check so the injection
    defence stays visibly wired into the sink's own test file.
    """
    from drakkar.sinks.pgsql import quote_ident

    assert quote_ident('valid_name') == '"valid_name"'
    with pytest.raises(ValueError):
        quote_ident('col; DROP TABLE x')


async def test_postgres_sink_statement_without_any_configured_says_so(pg_sink_config):
    """The error names the empty config, not just the unknown key.

    An operator who forgot the ``statements:`` block entirely gets told that,
    rather than a bare "unknown statement" that reads like a typo.
    """
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    with pytest.raises(ValueError, match='<none configured>'):
        await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job')])
    mock_conn.execute.assert_not_called()


@pytest.fixture
def pg_stmt_config():
    return PostgresSinkConfig(
        dsn='postgresql://localhost/testdb',
        statements={
            'claim_job': 'UPDATE jobs SET status = :status, attempts = attempts + 1 WHERE id = :id',
            'sweep': 'DELETE FROM jobs WHERE done',
        },
    )


def _make_pg_sink_with_statements(config):
    """Sink with mocked pool AND compiled statements, bypassing connect()."""
    from drakkar.sinks.pgsql import compile_named_statement

    sink, mock_conn, mock_pool = _make_pg_sink(config)
    sink._statements = {name: compile_named_statement(sql) for name, sql in config.statements.items()}
    return sink, mock_conn, mock_pool


async def test_postgres_sink_connect_compiles_statements(pg_stmt_config):
    from drakkar.sinks.postgres import PostgresSink

    sink = PostgresSink('main', pg_stmt_config)

    async def fake_create_pool(**kwargs):
        return AsyncMock()

    with patch('drakkar.sinks.postgres.asyncpg.create_pool', side_effect=fake_create_pool):
        await sink.connect()

    sql, names = sink._statements['claim_job']
    assert sql == 'UPDATE jobs SET status = $1, attempts = attempts + 1 WHERE id = $2'
    assert names == ['status', 'id']


async def test_postgres_sink_statement_binds_params_in_declared_order(pg_stmt_config):
    from drakkar.models import PostgresOp

    class ClaimParams(BaseModel):
        id: int = 42
        status: str = 'running'

    sink, mock_conn, _ = _make_pg_sink_with_statements(pg_stmt_config)

    await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job', params=ClaimParams())])

    query, *values = mock_conn.execute.call_args[0]
    assert query == 'UPDATE jobs SET status = $1, attempts = attempts + 1 WHERE id = $2'
    assert values == ['running', 42], 'values follow placeholder order, not field order'


async def test_postgres_sink_statement_without_params(pg_stmt_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink_with_statements(pg_stmt_config)

    await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='sweep')])

    query, *values = mock_conn.execute.call_args[0]
    assert query == 'DELETE FROM jobs WHERE done'
    assert values == []


async def test_postgres_sink_statement_batch_uses_executemany(pg_stmt_config):
    from drakkar.models import PostgresOp

    class ClaimParams(BaseModel):
        id: int
        status: str = 'running'

    sink, mock_conn, _ = _make_pg_sink_with_statements(pg_stmt_config)

    await sink.deliver(
        [PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job', params=ClaimParams(id=i)) for i in (1, 2)]
    )

    mock_conn.executemany.assert_called_once()
    _, args = mock_conn.executemany.call_args[0]
    assert args == [['running', 1], ['running', 2]]


async def test_postgres_sink_unknown_statement_names_configured_ones(pg_stmt_config):
    from drakkar.models import PostgresOp

    sink, _, _ = _make_pg_sink_with_statements(pg_stmt_config)

    with pytest.raises(ValueError, match='Unknown postgres statement'):
        await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='nope')])


async def test_postgres_sink_statement_param_mismatch(pg_stmt_config):
    from drakkar.models import PostgresOp

    class Missing(BaseModel):
        id: int = 1

    class Extra(BaseModel):
        id: int = 1
        status: str = 'running'
        bogus: int = 9

    sink, _, _ = _make_pg_sink_with_statements(pg_stmt_config)

    with pytest.raises(ValueError, match='missing'):
        await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job', params=Missing())])
    with pytest.raises(ValueError, match='unexpected'):
        await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job', params=Extra())])


async def test_postgres_sink_statement_requires_params_when_declared(pg_stmt_config):
    from drakkar.models import PostgresOp

    sink, _, _ = _make_pg_sink_with_statements(pg_stmt_config)

    with pytest.raises(ValueError, match='missing'):
        await sink.deliver([PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job')])


class DBKeyModel(BaseModel):
    id: int = 1


class DBNullKeyModel(BaseModel):
    id: int = 1
    claimed_by: str | None = None


class DBEmptyModel(BaseModel):
    """A model that passes field-presence validation but dumps to {}."""


async def test_postgres_sink_update_single(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver(
        [PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(id=7), where=DBKeyModel(id=42))]
    )

    mock_conn.execute.assert_called_once()
    query, *values = mock_conn.execute.call_args[0]
    assert query == 'UPDATE "jobs" SET "id" = $1 WHERE "id" = $2'
    assert values == [7, 42]


async def test_postgres_sink_update_batch_uses_executemany(pg_sink_config):
    """Same-shaped updates go out as one prepared statement with N arg tuples."""
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(id=i), where=DBKeyModel(id=i))
        for i in (1, 2, 3)
    ]
    await sink.deliver(payloads)

    mock_conn.executemany.assert_called_once()
    query, args = mock_conn.executemany.call_args[0]
    assert query == 'UPDATE "jobs" SET "id" = $1 WHERE "id" = $2'
    assert args == [[1, 1], [2, 2], [3, 3]]
    mock_conn.execute.assert_not_called()


async def test_postgres_sink_update_null_predicate_renders_is_null(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver(
        [
            PostgresPayload(
                op=PostgresOp.UPDATE,
                table='jobs',
                data=DBKeyModel(id=1),
                where=DBNullKeyModel(id=5, claimed_by=None),
            )
        ]
    )

    query, *values = mock_conn.execute.call_args[0]
    assert query == 'UPDATE "jobs" SET "id" = $1 WHERE "id" = $2 AND "claimed_by" IS NULL'
    assert values == [1, 5], 'the IS NULL column must not consume a parameter'


async def test_postgres_sink_update_batch_failure_falls_back_per_payload(pg_sink_config):
    """executemany is atomic, so per-payload retry cannot double-write."""
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)
    mock_conn.executemany.side_effect = RuntimeError('batch rejected')

    payloads = [
        PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(id=i), where=DBKeyModel(id=i))
        for i in (1, 2)
    ]
    await sink.deliver(payloads)

    assert mock_conn.executemany.call_count == 1
    assert mock_conn.execute.call_count == 2


async def test_postgres_sink_update_rejects_empty_where_mapping(pg_sink_config):
    """Second guard: a model that DUMPS empty would render UPDATE with no WHERE.

    The model validator only checks that `where` is not None, and an empty
    model satisfies that — so without this build-time check the statement
    would rewrite every row in the table.
    """
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    with pytest.raises(ValueError, match='empty mapping'):
        await sink.deliver(
            [PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(), where=DBEmptyModel())]
        )
    mock_conn.execute.assert_not_called()


async def test_postgres_sink_upsert_renders_on_conflict(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver(
        [PostgresPayload(op=PostgresOp.UPSERT, table='totals', data=DBResultModel(id=1), conflict=['id'])]
    )

    query = mock_conn.execute.call_args[0][0]
    assert query.startswith('INSERT INTO "totals"')
    assert 'ON CONFLICT ("id") DO UPDATE SET' in query
    assert '"status" = EXCLUDED."status"' in query
    assert '"id" = EXCLUDED."id"' not in query, 'conflict column must not be overwritten'


async def test_postgres_sink_upsert_respects_update_columns(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver(
        [
            PostgresPayload(
                op=PostgresOp.UPSERT,
                table='totals',
                data=DBResultModel(id=1),
                conflict=['id'],
                update_columns=['status'],
            )
        ]
    )

    query = mock_conn.execute.call_args[0][0]
    assert 'DO UPDATE SET "status" = EXCLUDED."status"' in query
    assert 'score' not in query.split('DO UPDATE')[1]


async def test_postgres_sink_upsert_rejects_unknown_update_columns(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, _, _ = _make_pg_sink(pg_sink_config)

    payload = PostgresPayload(
        op=PostgresOp.UPSERT,
        table='t',
        data=DBResultModel(),
        conflict=['id'],
        update_columns=['nonexistent'],
    )
    with pytest.raises(ValueError, match='not present in data'):
        await sink.deliver([payload])


async def test_postgres_sink_upsert_batches_adjacent(pg_sink_config):
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(op=PostgresOp.UPSERT, table='totals', data=DBResultModel(id=i), conflict=['id']) for i in (1, 2)
    ]
    await sink.deliver(payloads)

    assert mock_conn.execute.call_count == 1
    assert mock_conn.execute.call_args[0][0].count('), (') == 1


async def test_postgres_sink_rejects_empty_data_mapping(pg_sink_config):
    """Zero columns would render `INSERT INTO t () VALUES ()`."""
    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    with pytest.raises(ValueError, match='empty mapping'):
        await sink.deliver([PostgresPayload(table='t', data=DBEmptyModel())])
    mock_conn.execute.assert_not_called()


async def test_postgres_sink_upsert_conflict_column_need_not_be_in_data(pg_sink_config):
    """Postgres allows a conflict target on a column the statement doesn't insert.

    A unique index on a defaulted or generated column is legitimate, so
    `conflict` entries are validated as identifiers only — never against the
    data columns.
    """
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver(
        [PostgresPayload(op=PostgresOp.UPSERT, table='t', data=DBKeyModel(id=1), conflict=['generated_key'])]
    )

    query = mock_conn.execute.call_args[0][0]
    assert 'ON CONFLICT ("generated_key") DO UPDATE SET "id" = EXCLUDED."id"' in query


async def test_postgres_sink_upsert_chunks_at_the_bind_parameter_cap(pg_sink_config):
    """A run too large for one statement splits, preserving order."""
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    payloads = [
        PostgresPayload(op=PostgresOp.UPSERT, table='totals', data=DBResultModel(id=i), conflict=['id'])
        for i in range(5)
    ]
    # DBResultModel has 3 columns, so a cap of 6 parameters allows 2 rows per
    # statement. Patching beats building 20k payloads.
    with patch('drakkar.sinks.postgres.MAX_INSERT_PARAMS', 6):
        await sink.deliver(payloads)

    # 5 rows at 2 per statement => two multi-row statements plus one single row.
    assert mock_conn.execute.call_count == 3
    tuple_counts = [c[0][0].count('), (') for c in mock_conn.execute.call_args_list]
    assert tuple_counts == [1, 1, 0]


async def test_postgres_sink_never_exceeds_the_driver_argument_limit(pg_sink_config):
    """Boundary check with the real cap: a run whose rows x columns crosses
    32767 must split, and no single statement may carry more arguments than
    asyncpg accepts. 1000 rows x 33 columns = 33000 is inside the fan-out
    this project targets, so this is not a theoretical size.
    """
    from drakkar.models import PostgresOp

    # 33 columns, matching the ticket's worked example.
    WideModel = create_model('WideModel', **{f'c{i}': (int, ...) for i in range(33)})

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)
    payloads = [
        PostgresPayload(op=PostgresOp.INSERT, table='wide', data=WideModel(**{f'c{i}': i for i in range(33)}))
        for _ in range(1000)
    ]

    await sink.deliver(payloads)

    assert mock_conn.execute.call_count > 1, 'an oversized run must be chunked'
    for call in mock_conn.execute.call_args_list:
        # call[0] is (query, *values) — everything after the query is a bind
        # parameter, and asyncpg raises InterfaceError above 32767 of them.
        assert len(call[0]) - 1 <= 32767


async def test_postgres_sink_logs_when_a_batch_falls_back_to_per_row(pg_sink_config):
    """The fallback used to swallow the batch error entirely, so a run that
    silently degraded from one statement to hundreds looked identical to a
    healthy one.
    """
    from drakkar.metrics import sink_batch_fallbacks
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)
    calls = {'n': 0}

    async def fail_the_batch(query, *values):
        calls['n'] += 1
        if calls['n'] == 1:
            raise RuntimeError('deadlock detected')
        return None

    mock_conn.execute.side_effect = fail_the_batch
    payloads = [PostgresPayload(op=PostgresOp.INSERT, table='totals', data=DBResultModel(id=i)) for i in range(3)]
    before = sink_batch_fallbacks.labels(sink_type='postgres', sink_name='main')._value.get()

    with capture_logs() as cap:
        await sink.deliver(payloads)

    events = [entry for entry in cap if entry['event'] == 'sink_batch_fallback_per_row']
    assert len(events) == 1
    assert events[0]['log_level'] == 'warning'
    assert events[0]['rows'] == 3
    assert 'deadlock detected' in events[0]['error']
    assert sink_batch_fallbacks.labels(sink_type='postgres', sink_name='main')._value.get() == before + 1


async def test_postgres_sink_mixed_ops_execute_in_payload_order(pg_sink_config):
    """A window mixing ops must reach the DB in the order the handler returned."""
    from drakkar.models import PostgresOp

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    order: list[str] = []
    mock_conn.execute.side_effect = lambda q, *a: order.append(q.split()[0])

    await sink.deliver(
        [
            PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(), where=DBKeyModel()),
            PostgresPayload(table='audit', data=DBResultModel()),
            PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=DBKeyModel(), where=DBKeyModel()),
        ]
    )

    assert order == ['UPDATE', 'INSERT', 'UPDATE']


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


# =============================================================================
# MongoDB sink
# =============================================================================


@pytest.fixture
def mongo_sink_config():
    return MongoSinkConfig(uri='mongodb://localhost:27017', database='testdb')


async def _make_mongo_sink(mongo_sink_config):
    """Helper: create a connected MongoSink with a mocked PyMongo client.

    Goes through the real ``connect()`` rather than assigning ``_db``
    directly, so operator-authored statements are compiled by the code that
    ships instead of by a second copy living in the tests.

    ``db[name]`` returns a DISTINCT collection mock per name, recorded in
    the returned registry. The previous helper handed back one shared mock
    for every name, which made any assertion about WHICH collection received
    a write vacuous — and grouping is precisely a per-collection dispatch
    property. Every method the sink calls (``bulk_write``, ``insert_one``,
    ``update_one``, …) is a coroutine on PyMongo's real ``AsyncCollection``,
    so ``AsyncMock`` is the honest shape and a missing ``await`` fails
    loudly.
    """
    from unittest.mock import MagicMock

    from drakkar.sinks.mongo import MongoSink

    collections: dict[str, AsyncMock] = {}

    def _collection(name):
        return collections.setdefault(name, AsyncMock())

    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(side_effect=_collection)
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    # close() is a coroutine on PyMongo's AsyncMongoClient (it was sync on
    # motor). An AsyncMock records awaits separately from calls, so a missing
    # `await` in MongoSink.close() fails assert_awaited_once() instead of
    # silently leaking the client.
    mock_client.close = AsyncMock()

    sink = MongoSink('analytics', mongo_sink_config)
    with patch('pymongo.AsyncMongoClient', return_value=mock_client):
        await sink.connect()
    return sink, collections, mock_client


def _mongo_collection(collections, name='results'):
    """The recorded mock for one collection, created on demand."""
    return collections.setdefault(name, AsyncMock())


async def test_mongo_sink_connect(mongo_sink_config):
    from drakkar.sinks.mongo import MongoSink

    with patch('pymongo.AsyncMongoClient') as mock_cls:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_cls.return_value = mock_client

        sink = MongoSink('analytics', mongo_sink_config)
        await sink.connect()

        # PyMongo leaves socketTimeoutMS unset by default — unbounded.
        mock_cls.assert_called_once_with(
            'mongodb://localhost:27017',
            socketTimeoutMS=30000,
            connectTimeoutMS=30000,
        )
        assert sink._db is mock_db


async def test_mongo_sink_deliver(mongo_sink_config):
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)

    payload = MongoPayload(collection='results', data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    mock_collection.insert_one.assert_called_once()
    doc = mock_collection.insert_one.call_args[0][0]
    assert doc['request_id'] == 'r1'
    assert doc['answer'] == '42'


async def test_mongo_sink_deliver_batch(mongo_sink_config):
    """Payloads sharing a collection go out as ONE ordered bulk write."""
    from pymongo import InsertOne

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)

    payloads = [
        MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
        MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
    ]
    await sink.deliver(payloads)

    assert mock_collection.insert_one.await_count == 0
    mock_collection.bulk_write.assert_awaited_once()
    models, kwargs = mock_collection.bulk_write.await_args
    assert all(isinstance(m, InsertOne) for m in models[0])
    assert [m._doc['request_id'] for m in models[0]] == ['r1', 'r2'], 'payload order must be preserved'
    # ordered=True is what makes the writeErrors index positionally
    # meaningful, so it is passed explicitly rather than left to the default.
    assert kwargs['ordered'] is True


async def test_mongo_sink_single_payload_uses_insert_one(mongo_sink_config):
    """A one-document group keeps the exact call the per-payload loop made."""
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)

    await sink.deliver([MongoPayload(collection='results', data=SampleOutput(request_id='r1'))])

    assert mock_collection.insert_one.await_count == 1
    assert mock_collection.bulk_write.await_count == 0


async def test_mongo_sink_attributes_the_failing_payload(mongo_sink_config):
    """A bulk write error names the payload that caused it, and re-sends nothing.

    writeErrors[*].index is positionally aligned with the models submitted,
    and ordered=True guarantees everything before the failure was applied
    and everything after was not attempted — so the culprit is exact.
    """
    from pymongo.errors import BulkWriteError

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)
    mock_collection.bulk_write.side_effect = BulkWriteError(
        {'writeErrors': [{'index': 1, 'code': 11000, 'errmsg': 'E11000 duplicate key'}], 'nInserted': 1}
    )

    payloads = [
        MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
        MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
        MongoPayload(collection='results', data=SampleOutput(request_id='r3')),
    ]

    with pytest.raises(MongoWriteError, match='1 of 3'):
        await sink.deliver(payloads)

    assert mock_collection.bulk_write.await_count == 1, 'nothing may be re-sent'
    assert mock_collection.insert_one.await_count == 0


async def test_mongo_sink_write_error_names_the_operation_not_the_document(mongo_sink_config):
    """The label carries the op and collection, never message content."""
    from pymongo.errors import BulkWriteError

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)
    mock_collection.bulk_write.side_effect = BulkWriteError({'writeErrors': [{'index': 0, 'errmsg': 'boom'}]})

    with pytest.raises(MongoWriteError, match='insert collection=results') as excinfo:
        await sink.deliver(
            [
                MongoPayload(collection='results', data=SampleOutput(request_id='secret-1')),
                MongoPayload(collection='results', data=SampleOutput(request_id='secret-2')),
            ]
        )

    assert 'secret-1' not in str(excinfo.value)


async def test_mongo_sink_write_error_is_not_transient(mongo_sink_config):
    """A duplicate key fails identically on every retry.

    Subclassing the builtin ConnectionError would make SinkManager retry it,
    burning the retry budget and — for a batch containing an insert —
    duplicating documents.
    """
    assert not issubclass(MongoWriteError, ConnectionError)
    assert not issubclass(MongoWriteError, TimeoutError)


async def test_mongo_sink_connection_failure_propagates_without_replay(mongo_sink_config):
    """A connection-level failure is not a BulkWriteError.

    There the framework cannot know what was applied, so the error
    propagates with the whole run and nothing is re-sent.
    """
    from pymongo.errors import ConnectionFailure

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)
    mock_collection.bulk_write.side_effect = ConnectionFailure('connection reset')

    with pytest.raises(Exception, match='connection reset'):
        await sink.deliver(
            [
                MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
                MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
            ]
        )

    assert mock_collection.bulk_write.await_count == 1
    assert mock_collection.insert_one.await_count == 0


async def test_mongo_sink_deliver_empty(mongo_sink_config):
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections)

    await sink.deliver([])
    mock_collection.insert_one.assert_not_called()


async def test_mongo_sink_deliver_not_connected_raises(mongo_sink_config):
    """A silent return would let the offset commit past lost payloads."""
    from drakkar.sinks.mongo import MongoSink

    sink = MongoSink('analytics', mongo_sink_config)
    with pytest.raises(RuntimeError, match='not connected'):
        await sink.deliver([MongoPayload(collection='c', data=SampleOutput())])


async def test_mongo_sink_deliver_error_increments_metrics(mongo_sink_config):
    from drakkar.metrics import sink_deliver_errors

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    mock_collection = _mongo_collection(collections, 'c')
    mock_collection.insert_one.side_effect = RuntimeError('connection refused')

    labels = {'sink_type': 'mongo', 'sink_name': 'analytics'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(RuntimeError, match='connection refused'):
        await sink.deliver([MongoPayload(collection='c', data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_mongo_sink_close(mongo_sink_config):
    sink, _, mock_client = await _make_mongo_sink(mongo_sink_config)
    await sink.close()

    # assert_awaited_once, not assert_called_once: a call is recorded even
    # when the coroutine is never awaited, which is exactly the bug this
    # guards against.
    mock_client.close.assert_awaited_once()
    assert sink._client is None
    assert sink._db is None


async def test_mongo_sink_close_not_connected(mongo_sink_config):
    from drakkar.sinks.mongo import MongoSink

    sink = MongoSink('analytics', mongo_sink_config)
    await sink.close()  # should not raise


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


async def test_http_sink_connect_no_hardcoded_content_type():
    """Verify connect() does not set Content-Type in client default headers.

    The encoder provides Content-Type per request in deliver(), so the client
    should not have a hardcoded value that would require filtering.
    """
    from drakkar.sinks.http import HttpSink

    config = HttpSinkConfig(url='https://api.example.com/results')
    sink = HttpSink('webhook', config)
    await sink.connect()

    # Check that no content-type variant is in the client headers
    has_content_type = any(k.lower() == 'content-type' for k in sink._client.headers)
    assert not has_content_type
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
    assert b'"request_id":"r1"' in call_kwargs['content']


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


async def test_http_sink_deliver_not_connected_raises(http_sink_config):
    """A silent return would let the offset commit past lost payloads."""
    from drakkar.sinks.http import HttpSink

    sink = HttpSink('webhook', http_sink_config)
    with pytest.raises(RuntimeError, match='not connected'):
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


async def test_http_sink_json_encoding_is_unchanged_by_default(http_sink_config):
    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver([HttpPayload(data=SampleOutput(request_id='r1'))])

    call_kwargs = mock_client.request.call_args[1]
    assert call_kwargs['content'] == b'{"request_id":"r1","answer":"42"}'
    assert call_kwargs['headers']['Content-Type'] == 'application/json'


async def test_http_sink_form_encoding_sends_urlencoded_body():
    config = HttpSinkConfig(url='https://api.example.com/results', encoding='form')
    sink, mock_client = _make_http_sink(config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver([HttpPayload(data=SampleOutput(request_id='r1'))])

    call_kwargs = mock_client.request.call_args[1]
    assert call_kwargs['content'] == b'answer=42&request_id=r1'
    assert call_kwargs['headers']['Content-Type'] == 'application/x-www-form-urlencoded'


async def test_http_sink_multipart_encoding_sends_multipart_body():
    config = HttpSinkConfig(url='https://api.example.com/results', encoding='multipart')
    sink, mock_client = _make_http_sink(config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver([HttpPayload(data=SampleOutput(request_id='r1'))])

    call_kwargs = mock_client.request.call_args[1]
    content_type = call_kwargs['headers']['Content-Type']
    assert content_type.startswith('multipart/form-data; boundary=')
    boundary = content_type.rsplit('=', 1)[1]
    assert call_kwargs['content'].startswith(f'--{boundary}\r\n'.encode())
    assert b'name="request_id"' in call_kwargs['content']


async def test_http_sink_multipart_boundary_differs_per_request():
    config = HttpSinkConfig(url='https://api.example.com/results', encoding='multipart')
    sink, mock_client = _make_http_sink(config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver(
        [
            HttpPayload(data=SampleOutput(request_id='r1')),
            HttpPayload(data=SampleOutput(request_id='r2')),
        ]
    )

    first = mock_client.request.call_args_list[0][1]['headers']['Content-Type']
    second = mock_client.request.call_args_list[1][1]['headers']['Content-Type']
    assert first != second


async def test_http_sink_configured_headers_still_reach_the_request():
    config = HttpSinkConfig(
        url='https://api.example.com/results',
        encoding='form',
        headers={'Authorization': 'Bearer t'},
    )
    sink, mock_client = _make_http_sink(config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver([HttpPayload(data=SampleOutput(request_id='r1'))])

    assert mock_client.request.call_args[1]['headers']['Authorization'] == 'Bearer t'


@pytest.mark.parametrize(
    'content_type_key',
    ['Content-Type', 'content-type', 'CONTENT-TYPE'],
)
async def test_http_sink_headers_cannot_override_content_type(content_type_key):
    # The config validator rejects this, but a caller can build the config
    # in Python and never validate it. The encoder's Content-Type must still
    # win. HTTP header names are case-insensitive, so test all common cases
    # to verify the sink removes any variant and replaces it with the encoder's.
    config = HttpSinkConfig.model_construct(
        url='https://api.example.com/results',
        method='POST',
        timeout_seconds=30,
        max_retries=3,
        ui_url='',
        encoding='form',
        headers={content_type_key: 'application/vnd.custom+json'},
    )
    sink, mock_client = _make_http_sink(config)
    mock_client.request.return_value = _mock_response(200)

    await sink.deliver([HttpPayload(data=SampleOutput(request_id='r1'))])

    call_kwargs = mock_client.request.call_args[1]
    headers = call_kwargs['headers']
    # Exactly one Content-Type, set to the encoder's value
    assert headers['Content-Type'] == 'application/x-www-form-urlencoded'
    # Verify no duplicate header with different case exists
    assert sum(1 for k in headers if k.lower() == 'content-type') == 1


# =============================================================================
# Redis sink
# =============================================================================


@pytest.fixture
def redis_sink_config():
    return RedisSinkConfig(url='redis://localhost:6379/0', key_prefix='drakkar:')


def _make_redis_sink(redis_sink_config):
    """Helper: create a RedisSink with a mocked redis client.

    The pipeline mock mirrors redis-py's REAL shape, which is asymmetric:
    ``client.pipeline()`` is synchronous and returns a Pipeline, the
    pipeline's command methods (``set``, ``delete``, …) are synchronous and
    queue the command, and only ``execute()`` is awaitable.

    A plain ``AsyncMock`` gets this wrong — ``client.pipeline(...)`` returns
    a coroutine, so ``pipe.set(...)`` raises ``AttributeError`` and the
    batch path never actually runs under test. Reach the queued commands
    via ``mock_client.pipeline.return_value``.
    """
    from drakkar.sinks.redis import RedisSink

    mock_client = AsyncMock()
    pipe = MagicMock()

    async def _execute(raise_on_error: bool = True) -> list[object]:
        """Return one result per queued command, as the real pipeline does.

        A fixed ``[]`` would be the same class of dishonesty as the
        coroutine-returning ``pipeline()`` this helper was written to fix:
        attribution zips results against commands, so a wrongly-sized list
        would let the sink drop failures on the floor under test.
        """
        queued = [call for call in pipe.mock_calls if not call[0].startswith('execute')]
        return [True] * len(queued)

    pipe.execute = AsyncMock(side_effect=_execute)
    mock_client.pipeline = MagicMock(return_value=pipe)

    def _fake_script(body: str) -> AsyncMock:
        """Stand in for redis-py's AsyncScript.

        A real one issues EVALSHA against whatever client it is handed — a
        pipeline QUEUES it (synchronously, returning the pipeline), a client
        AWAITS it. Modelling that matters: without it the script contributes
        no queued command, and the result list would not line up with the
        commands the sink thinks it sent.
        """

        async def _call(keys: list | None = None, args: list | None = None, client: object = None) -> None:
            queued = client.evalsha(f'sha:{body[:12]}', len(keys or []), *(keys or []), *(args or []))
            if inspect.isawaitable(queued):
                await queued

        return AsyncMock(side_effect=_call)

    # register_script is SYNCHRONOUS in redis-py — it computes the SHA1
    # locally with no round trip — and returns an AsyncScript that IS
    # awaitable. An AsyncMock would get both halves wrong.
    mock_client.register_script = MagicMock(side_effect=_fake_script)

    sink = RedisSink('cache', redis_sink_config)
    sink._client = mock_client
    return sink, mock_client


async def _connect_redis_sink(redis_sink_config):
    """Helper: a RedisSink taken through the REAL connect().

    Script tests go this way rather than poking ``sink._scripts``, so what
    they exercise is the registration the sink actually performs.
    """
    from drakkar.sinks.redis import RedisSink

    sink, mock_client = _make_redis_sink(redis_sink_config)
    sink._client = None
    with patch('redis.asyncio.from_url', return_value=mock_client):
        await sink.connect()
    assert isinstance(sink, RedisSink)
    return sink, mock_client


# --- named Lua scripts ---


def _script_config(**overrides):
    from drakkar.config import RedisSinkConfig

    return RedisSinkConfig(
        url='redis://localhost:6379/0',
        key_prefix='drakkar:',
        scripts={
            'push_and_cap': "redis.call('LPUSH', KEYS[1], ARGV[1])\nredis.call('LTRIM', KEYS[1], 0, ARGV[2])",
            'claim_once': "return redis.call('SET', KEYS[1], ARGV[1], 'NX')",
        },
        **overrides,
    )


# --- golden wire-level commands ---
#
# tests/fixtures/redis_commands.json pins the emitted argument vectors.
# The cases run through the sink's OWN command builder into a REAL redis-py
# client whose transport is intercepted, so what is pinned is the argument
# vector redis-py actually sends — not a test-local idea of it. A divergence
# between the two backends fails here instead of reaching an operator's Redis.

_REDIS_COMMAND_CORPUS = json.loads((Path(__file__).parent / 'fixtures' / 'redis_commands.json').read_text())


def _corpus_data_model(fields: dict):
    """Build a model whose model_dump_json() yields the fixture's object."""
    return create_model('CorpusData', **{name: (Any, value) for name, value in fields.items()})()


async def _capture_wire_command(config, payload: RedisPayload) -> list:
    """Deliver one payload and return the argv redis-py put on the wire.

    The client is a real ``redis.asyncio.Redis`` that never connects: only
    ``execute_command`` is replaced, and every method above it — argument
    assembly, keyword expansion, the EVALSHA hashing — is redis-py's own.
    That is what makes these vectors trustworthy as a wire-level
    contract rather than a restatement of this repo's code.
    """
    import redis.asyncio as aioredis

    from drakkar.sinks.redis import RedisSink

    client = aioredis.Redis(host='127.0.0.1', port=1)
    captured: list[tuple] = []
    client.execute_command = AsyncMock(side_effect=lambda *args, **kw: captured.append(args))

    sink = RedisSink('cache', config)
    with patch('redis.asyncio.from_url', return_value=client):
        await sink.connect()
    await sink.deliver([payload])

    assert len(captured) == 1, f'expected exactly one command, got {captured}'
    return list(captured[0])


@pytest.mark.parametrize('case', _REDIS_COMMAND_CORPUS['cases'], ids=lambda c: c['case'])
async def test_redis_command_corpus(case):
    """The sink must issue this command with these arguments, in this order."""
    from drakkar.config import RedisSinkConfig

    config = RedisSinkConfig(
        url='redis://localhost:6379/0',
        key_prefix=_REDIS_COMMAND_CORPUS['key_prefix'],
        scripts=_REDIS_COMMAND_CORPUS['scripts'],
    )
    fields = dict(case['payload'])
    if fields.get('data') is not None:
        fields['data'] = _corpus_data_model(fields['data'])

    command = await _capture_wire_command(config, RedisPayload(**fields))

    assert command == case['command']


def test_redis_command_corpus_covers_every_op():
    """A new op without a vector must fail here, not be quietly unpinned."""
    from drakkar.models import RedisOp

    covered = {case['payload']['op'] for case in _REDIS_COMMAND_CORPUS['cases']}
    assert covered == {op.value for op in RedisOp}


# --- per-batch retry safety ---


@pytest.mark.parametrize(
    ('payloads', 'expected'),
    [
        # Write-replace on a fixed key/field, or a removal — all converge.
        ([{'key': 'k', 'data': None}], True),
        ([{'op': 'delete', 'key': 'k'}], True),
        ([{'op': 'expire', 'key': 'k', 'ttl': 60}], True),
        ([{'op': 'hset', 'key': 'k', 'fields': {'a': 1}}], True),
        ([{'op': 'hdel', 'key': 'k', 'fields': ['a']}], True),
        ([{'op': 'sadd', 'key': 'k', 'members': ['m']}], True),
        ([{'op': 'srem', 'key': 'k', 'members': ['m']}], True),
        ([{'op': 'zadd', 'key': 'k', 'members': {'m': 1.0}}], True),
        ([{'op': 'trim', 'key': 'k', 'start': 0, 'stop': 9}], True),
        # A batch of several convergent ops is still safe.
        ([{'op': 'delete', 'key': 'k'}, {'op': 'sadd', 'key': 's', 'members': ['m']}], True),
        # INCRBY accumulates.
        ([{'op': 'incrby', 'key': 'k', 'amount': 1}], False),
        # PUSH appends a duplicate element.
        ([{'op': 'push', 'key': 'k', 'data': None}], False),
        # Operator Lua is opaque — the framework cannot tell.
        ([{'op': 'script', 'script': 's', 'keys': ['k']}], False),
        # One unsafe payload vetoes an otherwise safe batch.
        ([{'op': 'delete', 'key': 'k'}, {'op': 'incrby', 'key': 'c', 'amount': 1}], False),
        # Vacuously safe — nothing to duplicate.
        ([], True),
    ],
)
def test_redis_sink_batch_idempotent(redis_sink_config, payloads, expected):
    sink, _ = _make_redis_sink(redis_sink_config)
    # `data: None` in the table means "this op needs a payload model"; the
    # value itself is irrelevant to the retry decision.
    built = []
    for kwargs in payloads:
        if 'data' in kwargs:
            kwargs = {**kwargs, 'data': SampleOutput(request_id='r')}
        built.append(RedisPayload(**kwargs))

    assert sink.batch_idempotent(built) is expected


def test_redis_sink_keeps_the_class_flag_for_the_set_shaped_ops(redis_sink_config):
    """The type-level flag stays True; the per-batch hook narrows it."""
    sink, _ = _make_redis_sink(redis_sink_config)

    assert sink.idempotent is True
    assert sink.batch_idempotent([RedisPayload(op='incrby', key='k', amount=1)]) is False


async def test_redis_sink_exposes_its_client_after_connect(redis_sink_config):
    """Mirrors PostgresSink.pool — the prerequisite for read-modify-write.

    Reads are out of scope for the sink itself (a sink discards results), so
    a handler that needs one goes through the client. Reachable only from a
    plugin sink subclass today; see the property's docstring.
    """
    from drakkar.sinks.redis import RedisSink

    sink = RedisSink('cache', redis_sink_config)
    assert sink.client is None, 'no client before connect()'

    connected, mock_client = await _connect_redis_sink(redis_sink_config)
    assert connected.client is mock_client

    await connected.close()
    assert connected.client is None, 'close() must not leave a dead client exposed'


def test_every_redis_op_has_a_renderer_or_the_script_path():
    """Adding an op to the enum without wiring it must fail here, loudly.

    The build path indexes the renderer table directly, so a missing op
    would otherwise surface as a KeyError at delivery time.
    """
    from drakkar.models import RedisOp
    from drakkar.sinks.redis import _COMMAND_RENDERERS

    unwired = {op.value for op in RedisOp} - {op.value for op in _COMMAND_RENDERERS} - {RedisOp.SCRIPT.value}
    assert not unwired, f'ops with no renderer: {sorted(unwired)}'


async def test_redis_sink_registers_every_configured_script_at_connect():
    """register_script computes the SHA1 locally, so connect() stays cheap.

    No round trip means a briefly unavailable Redis does not fail startup.
    """
    sink, mock_client = await _connect_redis_sink(_script_config())

    registered = [call.args[0] for call in mock_client.register_script.call_args_list]
    assert len(registered) == 2
    assert any('LPUSH' in body for body in registered)
    assert set(sink._scripts) == {'push_and_cap', 'claim_once'}


async def test_redis_sink_registers_nothing_when_no_scripts_are_configured(redis_sink_config):
    sink, mock_client = await _connect_redis_sink(redis_sink_config)

    mock_client.register_script.assert_not_called()
    assert sink._scripts == {}


async def test_redis_sink_runs_a_named_script_with_prefixed_keys():
    """EVERY entry of `keys` is prefixed, not just the single-key ops.

    The prefix is the sink instance's namespace; a script that bypassed it
    could reach keys outside that namespace.
    """
    sink, _ = await _connect_redis_sink(_script_config())
    script = sink._scripts['push_and_cap']

    await sink.deliver(
        [
            RedisPayload(
                op='script',
                script='push_and_cap',
                keys=['recent', 'recent:meta'],
                args=['{"a":1}', 100],
            )
        ]
    )

    script.assert_awaited_once()
    assert script.await_args.kwargs['keys'] == ['drakkar:recent', 'drakkar:recent:meta']
    assert script.await_args.kwargs['args'] == ['{"a":1}', 100]


async def test_redis_sink_queues_a_script_onto_the_pipeline():
    """A script batches with plain commands — it is queued, not executed early.

    AsyncScript.__call__ detects a Pipeline and registers itself, so
    Pipeline.execute() does SCRIPT EXISTS / SCRIPT LOAD first. NOSCRIPT
    recovery is redis-py's job, not ours.
    """
    sink, mock_client = await _connect_redis_sink(_script_config())
    pipe = mock_client.pipeline.return_value
    script = sink._scripts['claim_once']

    await sink.deliver(
        [
            RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
            RedisPayload(op='script', script='claim_once', keys=['lock'], args=['owner']),
        ]
    )

    mock_client.pipeline.assert_called_once_with(transaction=False)
    assert script.await_args.kwargs['client'] is pipe


async def test_redis_sink_unknown_script_names_the_configured_ones():
    sink, mock_client = await _connect_redis_sink(_script_config())

    with pytest.raises(ValueError, match='unknown redis script') as excinfo:
        await sink.deliver([RedisPayload(op='script', script='nope', keys=['k'])])

    # Sorted, so the operator sees what IS available rather than just a miss.
    assert 'claim_once, push_and_cap' in str(excinfo.value)
    mock_client.pipeline.assert_not_called()


async def test_redis_sink_unknown_script_says_so_when_none_are_configured(redis_sink_config):
    sink, _ = await _connect_redis_sink(redis_sink_config)

    with pytest.raises(ValueError, match='<none configured>'):
        await sink.deliver([RedisPayload(op='script', script='push_and_cap', keys=['k'])])


async def test_redis_sink_script_failure_is_attributed_by_name_not_body():
    """DLQ entries and logs carry the script NAME — a body can leak row data."""
    from drakkar.sinks.redis import RedisCommandError

    sink, mock_client = await _connect_redis_sink(_script_config())
    pipe = mock_client.pipeline.return_value
    pipe.execute = AsyncMock(return_value=[True, Exception('ERR user_script failed')])

    with pytest.raises(RedisCommandError) as excinfo:
        await sink.deliver(
            [
                RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
                RedisPayload(op='script', script='push_and_cap', keys=['recent'], args=['x']),
            ]
        )

    message = str(excinfo.value)
    assert 'push_and_cap' in message
    assert 'LPUSH' not in message, 'the script body must never reach an error message'


async def test_redis_sink_connect(redis_sink_config):
    from drakkar.sinks.redis import RedisSink

    with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        sink = RedisSink('cache', redis_sink_config)
        await sink.connect()

        # redis-py leaves socket_timeout at None by default — unbounded.
        mock_from_url.assert_called_once_with(
            'redis://localhost:6379/0',
            socket_timeout=30.0,
            socket_connect_timeout=30.0,
        )
        assert sink._client is mock_client


async def test_redis_plain_command_queues_and_executes_the_same_call():
    """One command shape drives both paths, so neither knows which ops exist.

    ``queue`` is async even though pipeline command methods are synchronous
    — a script must be awaited to queue, and one uniform call site is worth
    the harmless await.
    """
    from drakkar.models import RedisOp
    from drakkar.sinks.redis import _PlainCommand

    command = _PlainCommand(
        op=RedisOp.SET,
        label='set key=drakkar:k',
        method='set',
        args=('drakkar:k', '{}'),
        kwargs={'ex': 60},
    )

    pipe = MagicMock()
    await command.queue(pipe)
    pipe.set.assert_called_once_with('drakkar:k', '{}', ex=60)

    client = AsyncMock()
    await command.execute(client)
    client.set.assert_awaited_once_with('drakkar:k', '{}', ex=60)


@pytest.mark.parametrize(
    ('payload_kwargs', 'method', 'args', 'kwargs'),
    [
        (
            {'op': 'delete', 'key': 'session:42'},
            'delete',
            ('drakkar:session:42',),
            {},
        ),
        (
            {'op': 'expire', 'key': 'session:42', 'ttl': 900},
            'expire',
            ('drakkar:session:42', 900),
            {},
        ),
        (
            {'op': 'incrby', 'key': 'hits:today', 'amount': 5},
            'incrby',
            ('drakkar:hits:today', 5),
            {},
        ),
        # A negative or zero amount is legitimate.
        (
            {'op': 'incrby', 'key': 'hits:today', 'amount': -1},
            'incrby',
            ('drakkar:hits:today', -1),
            {},
        ),
        (
            {'op': 'trim', 'key': 'recent', 'start': 0, 'stop': 99},
            'ltrim',
            ('drakkar:recent', 0, 99),
            {},
        ),
    ],
)
async def test_redis_sink_renders_keyed_scalar_commands(redis_sink_config, payload_kwargs, method, args, kwargs):
    """Command name and argument order per op — the parity surface."""
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(**payload_kwargs)])

    call = getattr(mock_client, method)
    call.assert_awaited_once_with(*args, **kwargs)


@pytest.mark.parametrize(
    ('side', 'method'),
    [(None, 'lpush'), ('left', 'lpush'), ('right', 'rpush')],
)
async def test_redis_sink_push_honours_side_and_defaults_to_left(redis_sink_config, side, method):
    """`side` selects the command; omitting it means LPUSH."""
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(op='push', key='recent', data=SampleOutput(request_id='r1'), side=side)])

    call = getattr(mock_client, method)
    call.assert_awaited_once()
    assert call.call_args.args[0] == 'drakkar:recent'
    # One list element is one serialized object, exactly as for SET.
    assert '"request_id":"r1"' in call.call_args.args[1]


@pytest.mark.parametrize(
    ('payload_kwargs', 'method', 'args', 'kwargs'),
    [
        # HSET takes a mapping so several fields go in one command.
        (
            {'op': 'hset', 'key': 'session:42', 'fields': {'ip': '10.0.0.1', 'hits': 3}},
            'hset',
            ('drakkar:session:42',),
            {'mapping': {'ip': '10.0.0.1', 'hits': 3}},
        ),
        (
            {'op': 'hdel', 'key': 'session:42', 'fields': ['ip', 'hits']},
            'hdel',
            ('drakkar:session:42', 'ip', 'hits'),
            {},
        ),
        (
            {'op': 'sadd', 'key': 'seen', 'members': ['a', 'b']},
            'sadd',
            ('drakkar:seen', 'a', 'b'),
            {},
        ),
        (
            {'op': 'srem', 'key': 'seen', 'members': ['a']},
            'srem',
            ('drakkar:seen', 'a'),
            {},
        ),
        # ZADD's mapping stays keyed by MEMBER — that is redis-py's own
        # signature, and it flips to `score member` on the wire itself.
        (
            {'op': 'zadd', 'key': 'leaderboard', 'members': {'alice': 12.5}},
            'zadd',
            ('drakkar:leaderboard', {'alice': 12.5}),
            {},
        ),
    ],
)
async def test_redis_sink_renders_collection_commands(redis_sink_config, payload_kwargs, method, args, kwargs):
    """Command name and argument order per op — the parity surface."""
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(**payload_kwargs)])

    getattr(mock_client, method).assert_awaited_once_with(*args, **kwargs)


async def test_redis_sink_sorts_mapping_arguments_but_not_lists(redis_sink_config):
    """Mappings emit sorted; caller-supplied lists keep their order.

    Argument order changes neither HSET's nor ZADD's end state, but it does
    change the emitted command — and a mapping decoded from a payload has
    no key order to preserve, so sorting is the only rule available.
    A list is a sequence both can preserve, so it is left alone.
    """
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(op='hset', key='s', fields={'zulu': 1, 'alpha': 2})])
    assert list(mock_client.hset.call_args.kwargs['mapping']) == ['alpha', 'zulu']

    await sink.deliver([RedisPayload(op='zadd', key='lb', members={'zoe': 1.0, 'amy': 2.0})])
    assert list(mock_client.zadd.call_args.args[1]) == ['amy', 'zoe']

    await sink.deliver([RedisPayload(op='sadd', key='seen', members=['zulu', 'alpha'])])
    assert mock_client.sadd.call_args.args[1:] == ('zulu', 'alpha')

    await sink.deliver([RedisPayload(op='hdel', key='s', fields=['zulu', 'alpha'])])
    assert mock_client.hdel.call_args.args[1:] == ('zulu', 'alpha')


async def test_redis_sink_passes_field_values_through_untouched(redis_sink_config):
    """redis-py's encoder handles str/int/float — the framework must not stringify."""
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([RedisPayload(op='hset', key='s', fields={'a': 1, 'b': 2.5, 'c': 'three'})])

    mapping = mock_client.hset.call_args.kwargs['mapping']
    assert mapping == {'a': 1, 'b': 2.5, 'c': 'three'}
    assert [type(v) for v in mapping.values()] == [int, float, str]


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
    """A multi-payload delivery goes through ONE pipeline, not per-key SETs.

    This previously asserted ``mock_client.set.call_count == 2`` and passed
    by exercising the per-key fallback: the mock made ``pipeline()`` return
    a coroutine, ``pipe.set`` raised AttributeError, and a bare
    ``except Exception: pass`` swallowed it. The pipeline path had no real
    coverage at all.
    """
    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value

    payloads = [
        RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
        RedisPayload(key='k2', data=SampleOutput(request_id='r2'), ttl=60),
    ]
    await sink.deliver(payloads)

    # Exactly one pipeline, non-transactional, executed once.
    mock_client.pipeline.assert_called_once_with(transaction=False)
    pipe.execute.assert_awaited_once()
    # Both commands were queued on the pipeline, in payload order…
    assert pipe.set.call_count == 2
    assert [c.args[0] for c in pipe.set.call_args_list] == ['drakkar:k1', 'drakkar:k2']
    # …with the TTL riding on the payload that carries one.
    assert pipe.set.call_args_list[0].kwargs == {}
    assert pipe.set.call_args_list[1].kwargs == {'ex': 60}
    # …and nothing went through the per-key path.
    mock_client.set.assert_not_called()


async def test_redis_sink_pipeline_failure_propagates_without_replay(redis_sink_config):
    """A failing ``execute()`` surfaces the error instead of being swallowed.

    The deleted ``except Exception: pass`` turned every pipeline failure —
    including bugs like the broken mock above — into a silent fallback.
    """
    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value
    pipe.execute = AsyncMock(side_effect=RuntimeError('pipeline exploded'))

    payloads = [
        RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
        RedisPayload(key='k2', data=SampleOutput(request_id='r2')),
    ]
    with pytest.raises(RuntimeError, match='pipeline exploded'):
        await sink.deliver(payloads)


async def test_redis_sink_pipeline_is_executed_without_raising_on_error(redis_sink_config):
    """``raise_on_error=False`` is what makes positional attribution possible.

    It returns a list aligned with the queued commands, per-command errors
    present as exception OBJECTS rather than raised — so the failing payload
    can be named without re-sending the ones that succeeded.
    """
    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value
    pipe.execute = AsyncMock(return_value=[True, True])

    await sink.deliver(
        [
            RedisPayload(key='k1', data=SampleOutput(request_id='r1')),
            RedisPayload(key='k2', data=SampleOutput(request_id='r2')),
        ]
    )
    pipe.execute.assert_awaited_once_with(raise_on_error=False)


async def test_redis_sink_names_the_failing_command_without_resending_anything(redis_sink_config):
    """A per-command error names that payload and re-sends nothing.

    This is what makes non-idempotent commands safe to batch at all: the
    old fallback re-sent the whole batch, which would double-apply an
    INCRBY that had already been applied.
    """
    from drakkar.sinks.redis import RedisCommandError

    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value
    wrongtype = Exception('WRONGTYPE Operation against a key holding the wrong kind of value')
    pipe.execute = AsyncMock(return_value=[True, wrongtype, True])

    payloads = [RedisPayload(key=f'k{i}', data=SampleOutput(request_id=f'r{i}')) for i in range(3)]
    with pytest.raises(RedisCommandError) as excinfo:
        await sink.deliver(payloads)

    message = str(excinfo.value)
    assert 'drakkar:k1' in message, 'the error must name the failing key'
    assert '1 of 3' in message, 'the error must report how many failed'
    assert excinfo.value.__cause__ is wrongtype
    # Nothing was re-sent: no per-command path, one pipeline only.
    mock_client.set.assert_not_called()
    mock_client.pipeline.assert_called_once()


async def test_redis_sink_reports_every_failure_but_names_the_first(redis_sink_config):
    """Several failures are counted; the first is the one an operator chases."""
    from drakkar.sinks.redis import RedisCommandError

    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value
    pipe.execute = AsyncMock(return_value=[Exception('first'), True, Exception('third')])

    payloads = [RedisPayload(key=f'k{i}', data=SampleOutput(request_id=f'r{i}')) for i in range(3)]
    with pytest.raises(RedisCommandError, match='2 of 3'):
        await sink.deliver(payloads)


async def test_redis_sink_rejects_a_result_list_that_does_not_match_the_batch(redis_sink_config):
    """A short result list must be loud, not a silently dropped failure.

    redis-py guarantees one result per queued command; if that ever stops
    holding, zipping leniently would discard the failures at the tail.
    """
    sink, mock_client = _make_redis_sink(redis_sink_config)
    pipe = mock_client.pipeline.return_value
    pipe.execute = AsyncMock(return_value=[True])  # one result for two commands

    payloads = [RedisPayload(key=f'k{i}', data=SampleOutput(request_id=f'r{i}')) for i in range(2)]
    with pytest.raises(ValueError, match='argument 2 is shorter'):
        await sink.deliver(payloads)


async def test_redis_command_error_is_never_classified_as_transient():
    """A WRONGTYPE must not be retried — it fails identically every time."""
    from drakkar.sinks.redis import RedisCommandError

    assert not issubclass(RedisCommandError, ConnectionError)
    assert not issubclass(RedisCommandError, TimeoutError)


@pytest.mark.parametrize(
    ('redis_error_name', 'expected_builtin'),
    [
        ('ConnectionError', ConnectionError),
        ('TimeoutError', TimeoutError),
    ],
)
async def test_redis_sink_remaps_transient_errors_to_builtins(redis_sink_config, redis_error_name, expected_builtin):
    """redis-py's transient errors surface as the BUILTIN equivalents.

    ``SinkManager._TRANSIENT_ERRORS`` matches the builtin
    ``ConnectionError``/``TimeoutError``, but ``redis.exceptions.*``
    inherit only from ``RedisError``. Without this remap a dropped Redis
    connection was never eligible for the fast-retry, so
    ``RedisSink.idempotent = True`` did nothing at all — while the
    backend, which classifies structurally, has always retried.
    """
    import redis.exceptions

    redis_error = getattr(redis.exceptions, redis_error_name)
    # Guard the premise: if these ever start inheriting from the builtins,
    # the remap becomes redundant and this test should be revisited.
    assert not issubclass(redis_error, expected_builtin)

    sink, mock_client = _make_redis_sink(redis_sink_config)
    mock_client.set = AsyncMock(side_effect=redis_error('connection lost'))

    with pytest.raises(expected_builtin, match='connection lost'):
        await sink.deliver([RedisPayload(key='k1', data=SampleOutput(request_id='r1'))])


async def test_redis_sink_transient_remap_preserves_the_original_error(redis_sink_config):
    """The redis-py error is chained, so nothing is lost in translation."""
    import redis.exceptions

    sink, mock_client = _make_redis_sink(redis_sink_config)
    original = redis.exceptions.ConnectionError('connection lost')
    mock_client.set = AsyncMock(side_effect=original)

    with pytest.raises(ConnectionError) as excinfo:
        await sink.deliver([RedisPayload(key='k1', data=SampleOutput(request_id='r1'))])

    assert excinfo.value.__cause__ is original


async def test_redis_sink_leaves_non_transient_errors_alone(redis_sink_config):
    """A command error is NOT a transient — remapping it would make the
    manager retry a request that will fail identically every time.
    """
    import redis.exceptions

    sink, mock_client = _make_redis_sink(redis_sink_config)
    mock_client.set = AsyncMock(side_effect=redis.exceptions.ResponseError('WRONGTYPE'))

    with pytest.raises(redis.exceptions.ResponseError, match='WRONGTYPE'):
        await sink.deliver([RedisPayload(key='k1', data=SampleOutput(request_id='r1'))])


async def test_redis_sink_deliver_empty(redis_sink_config):
    sink, mock_client = _make_redis_sink(redis_sink_config)

    await sink.deliver([])
    mock_client.set.assert_not_called()


async def test_redis_sink_deliver_not_connected_raises(redis_sink_config):
    """A silent return would let the offset commit past lost payloads."""
    from drakkar.sinks.redis import RedisSink

    sink = RedisSink('cache', redis_sink_config)
    with pytest.raises(RuntimeError, match='not connected'):
        await sink.deliver([RedisPayload(key='k', data=SampleOutput())])


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


# =============================================================================
# Filesystem sink
# =============================================================================


def test_file_sink_config_requires_base_path():
    """base_path is required (min_length=1) — empty string raises ValidationError."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FileSinkConfig(base_path='')


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

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    out_file = tmp_path / 'output.jsonl'

    payload = FilePayload(path='output.jsonl', data=SampleOutput(request_id='r1'))
    await sink.deliver([payload])

    assert out_file.exists()
    lines = out_file.read_text().splitlines()
    assert len(lines) == 1
    assert '"request_id":"r1"' in lines[0]


async def test_file_sink_deliver_appends_to_existing(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    out_file = tmp_path / 'output.jsonl'
    out_file.write_text('{"existing":"line"}\n')

    payload = FilePayload(path='output.jsonl', data=SampleOutput(request_id='r2'))
    await sink.deliver([payload])

    lines = out_file.read_text().splitlines()
    assert len(lines) == 2
    assert lines[0] == '{"existing":"line"}'
    assert '"request_id":"r2"' in lines[1]


async def test_file_sink_deliver_batch(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    out_file = tmp_path / 'batch.jsonl'

    payloads = [
        FilePayload(path='batch.jsonl', data=SampleOutput(request_id='r1')),
        FilePayload(path='batch.jsonl', data=SampleOutput(request_id='r2')),
        FilePayload(path='batch.jsonl', data=SampleOutput(request_id='r3')),
    ]
    await sink.deliver(payloads)

    lines = out_file.read_text().splitlines()
    assert len(lines) == 3


async def test_file_sink_deliver_different_files(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))

    payloads = [
        FilePayload(path='a.jsonl', data=SampleOutput(request_id='r1')),
        FilePayload(path='b.jsonl', data=SampleOutput(request_id='r2')),
    ]
    await sink.deliver(payloads)

    file_a = tmp_path / 'a.jsonl'
    file_b = tmp_path / 'b.jsonl'
    assert file_a.exists()
    assert file_b.exists()
    assert '"r1"' in file_a.read_text()
    assert '"r2"' in file_b.read_text()


async def test_file_sink_deliver_interleaved_files_preserves_per_file_order(tmp_path):
    """Grouping by target file must keep payload order within each file."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payloads = [
        FilePayload(path='a.jsonl', data=SampleOutput(request_id='a1')),
        FilePayload(path='b.jsonl', data=SampleOutput(request_id='b1')),
        FilePayload(path='a.jsonl', data=SampleOutput(request_id='a2')),
        FilePayload(path='b.jsonl', data=SampleOutput(request_id='b2')),
    ]
    await sink.deliver(payloads)

    lines_a = (tmp_path / 'a.jsonl').read_text().splitlines()
    lines_b = (tmp_path / 'b.jsonl').read_text().splitlines()
    assert ['"a1"' in ln for ln in lines_a] == [True, False]
    assert '"a2"' in lines_a[1]
    assert '"b1"' in lines_b[0]
    assert '"b2"' in lines_b[1]


async def test_file_sink_deliver_offloads_batch_in_one_thread_hop(tmp_path):
    """The blocking resolve+write work runs in ONE to_thread call per batch."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payloads = [FilePayload(path=f'f{i}.jsonl', data=SampleOutput()) for i in range(3)]

    with patch('drakkar.sinks.filesystem.asyncio.to_thread', wraps=asyncio.to_thread) as to_thread:
        await sink.deliver(payloads)

    to_thread.assert_called_once()
    assert all((tmp_path / f'f{i}.jsonl').exists() for i in range(3))


async def test_file_sink_deliver_validates_all_paths_before_writing(tmp_path):
    """A traversal payload anywhere in the batch means nothing is written."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payloads = [
        FilePayload(path='good.jsonl', data=SampleOutput()),
        FilePayload(path='../escape.jsonl', data=SampleOutput()),
    ]
    with pytest.raises(ValueError, match='Path traversal detected'):
        await sink.deliver(payloads)

    assert not (tmp_path / 'good.jsonl').exists()


async def test_file_sink_deliver_empty(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    await sink.deliver([])  # should not raise


async def test_file_sink_deliver_missing_parent_dir(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    bad_path = 'nonexistent/output.jsonl'

    with pytest.raises(FileNotFoundError, match='Parent directory does not exist'):
        await sink.deliver([FilePayload(path=bad_path, data=SampleOutput())])


async def test_file_sink_deliver_error_increments_metrics(tmp_path):
    from drakkar.metrics import sink_deliver_errors
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    bad_path = 'nonexistent/output.jsonl'

    labels = {'sink_type': 'filesystem', 'sink_name': 'output'}
    before = sink_deliver_errors.labels(**labels)._value.get()

    with pytest.raises(FileNotFoundError):
        await sink.deliver([FilePayload(path=bad_path, data=SampleOutput())])

    assert sink_deliver_errors.labels(**labels)._value.get() == before + 1


async def test_file_sink_close(tmp_path):
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    await sink.close()  # no-op, should not raise


# --- Path containment tests ---


async def test_file_sink_relative_path_works(tmp_path):
    """Normal relative path within base_path works."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payload = FilePayload(path='subdir/output.jsonl', data=SampleOutput(request_id='r1'))
    (tmp_path / 'subdir').mkdir()
    await sink.deliver([payload])

    assert (tmp_path / 'subdir' / 'output.jsonl').exists()


async def test_file_sink_traversal_dotdot_raises(tmp_path):
    """../traversal outside base_path raises ValueError."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payload = FilePayload(path='../escape.jsonl', data=SampleOutput())

    with pytest.raises(ValueError, match='Path traversal detected'):
        await sink.deliver([payload])


async def test_file_sink_absolute_path_outside_base_raises(tmp_path):
    """Absolute path outside base_path raises ValueError."""
    from drakkar.sinks.filesystem import FileSink

    sink = FileSink('output', FileSinkConfig(base_path=str(tmp_path)))
    payload = FilePayload(path='/etc/passwd', data=SampleOutput())

    with pytest.raises(ValueError, match='Path traversal detected'):
        await sink.deliver([payload])


# =============================================================================
# DLQ sink
# =============================================================================


def _make_dlq_sink():
    """Helper: create a DLQSink with a mocked AIOProducer."""
    from drakkar.sinks.dlq import DLQSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    # 0 = the broker took everything; send() reads a non-zero remainder as
    # "the DLQ write was not confirmed".
    mock_producer.flush.return_value = 0
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


def test_dlq_message_serialize_fallback_logs_warning():
    """The str(p) fallback stays, but the degraded entry must leave a trace."""
    from drakkar.sinks.dlq import DLQMessage

    class _Broken(SampleOutput):
        def model_dump_json(self, **kwargs):
            raise RuntimeError('cannot serialize')

    from drakkar.models import DeliveryError

    error = DeliveryError(sink_name='results', sink_type='kafka', error='boom', payloads=[_Broken()])
    msg = DLQMessage(delivery_error=error, partition_id=0)

    with patch('drakkar.sinks.dlq.logger') as mock_logger:
        serialized = msg.serialize()

    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args[0][0] == 'dlq_payload_serialization_fallback'
    assert mock_logger.warning.call_args[1]['payload_type'] == '_Broken'
    entry = json.loads(serialized)
    # The fallback representation still reached the DLQ entry.
    assert len(entry['original_payloads']) == 1
    assert 'request_id' in entry['original_payloads'][0]


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


async def test_dlq_sink_send_failure_increments_counter():
    """When DLQ send fails, dlq_send_failures counter increments."""
    from drakkar.metrics import dlq_send_failures
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = RuntimeError('kafka down')
    sink._producer = mock_producer

    before = dlq_send_failures._value.get()
    await sink.send(_sample_delivery_error(), partition_id=0)
    assert dlq_send_failures._value.get() == before + 1


async def test_dlq_sink_send_delivery_report_failure_increments_counter():
    """H3: produce() can succeed while the delivery-report future rejects
    (e.g. broker ACK timeout, topic authorization denied). This mode must
    be caught and reflected in the failure metric — not silently lost.

    The pre-fix code relied on flush() to raise, which masked delivery-
    report failures under a generic "flush failed" path with less context.
    """
    from drakkar.metrics import dlq_send_failures, sink_dlq_messages
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')

    # produce() succeeds and returns a future that will REJECT.
    async def produce_returns_failed_future(**kwargs):
        f = asyncio.get_event_loop().create_future()
        f.set_exception(RuntimeError('delivery report: broker ack timeout'))
        return f

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = produce_returns_failed_future
    sink._producer = mock_producer

    failures_before = dlq_send_failures._value.get()
    successes_before = sink_dlq_messages._value.get()

    # Must NOT raise — DLQ is last-resort; caller should keep going.
    await sink.send(_sample_delivery_error(), partition_id=7)

    # Delivery-report failure must be observable via the counter.
    assert dlq_send_failures._value.get() == failures_before + 1
    # And the success counter must NOT have incremented.
    assert sink_dlq_messages._value.get() == successes_before


async def test_dlq_sink_send_flushes_the_producer():
    """``AIOProducer`` hands messages to librdkafka only once its internal
    buffer reaches ``batch_size`` (1000) or the ``buffer_timeout`` (1.0 s)
    inactivity timer fires. A DLQ write is a single message, so without an
    explicit flush every ``send()`` waits out that timer — one second per
    failed batch on the hot failure path. Same reasoning as ``KafkaSink``.
    """
    sink, mock_producer = _make_dlq_sink()
    mock_producer.flush.return_value = 0

    assert await sink.send(_sample_delivery_error(), partition_id=0) is True

    mock_producer.produce.assert_called_once()
    mock_producer.flush.assert_called_once()


async def test_dlq_sink_send_returns_false_when_flush_leaves_messages_queued():
    """A non-zero flush remainder means the broker never took the message."""
    sink, mock_producer = _make_dlq_sink()
    mock_producer.flush.return_value = 2

    assert await sink.send(_sample_delivery_error(), partition_id=0) is False


async def test_dlq_sink_send_returns_false_on_delivery_report_error():
    """The delivery future resolves to a Message, and a failed delivery is
    carried in ``Message.error()`` rather than raised. Reporting success
    there would commit the source offsets past payloads no broker holds.
    """
    sink, mock_producer = _make_dlq_sink()
    mock_producer.flush.return_value = 0
    failed = _make_mock_message()
    failed.error.return_value = 'BrokerNotAvailable'

    def _failing_future(**_kwargs):
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(failed)
        return fut

    mock_producer.produce.side_effect = _failing_future

    assert await sink.send(_sample_delivery_error(), partition_id=0) is False


async def test_dlq_sink_send_returns_false_when_future_resolves_to_none():
    """Defensive twin of the KafkaSink guard: a ``None`` delivery report
    carries no confirmation, so it must not be read as one.
    """
    sink, mock_producer = _make_dlq_sink()
    mock_producer.flush.return_value = 0

    def _none_future(**_kwargs):
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(None)
        return fut

    mock_producer.produce.side_effect = _none_future

    assert await sink.send(_sample_delivery_error(), partition_id=0) is False


async def test_dlq_message_serialization_cross_backend_golden():
    """Byte-stability pin — this exact literal is the contract.

    DLQ JSON byte-stability is contractual: tooling byte-compares entries.
    The envelope carries ``json.dumps`` default separators (``", "`` and
    ``": "``).

    The embedded payload strings stay **compact** — they come from
    ``model_dump_json()``. The flight recorder likewise encodes compact.
    That asymmetry is deliberate; do not unify the two.

    The sibling test below parses the JSON before asserting, so it cannot see
    separator drift — which is exactly how the two backends diverged
    unnoticed until an audit generated both outputs and diffed them.
    """
    from drakkar.models import DeliveryError
    from drakkar.sinks.dlq import DLQMessage

    error = DeliveryError(
        sink_name='results',
        sink_type='kafka',
        error='connection refused',
        payloads=[SampleOutput()],
    )
    msg = DLQMessage(delivery_error=error, partition_id=2, attempt_count=1)

    with patch('drakkar.sinks.dlq.time.time', return_value=1700000000.0):
        data = msg.serialize()

    assert data == (
        b'{"original_payloads": ["{\\"request_id\\":\\"abc\\",\\"answer\\":\\"42\\"}"], '
        b'"sink_name": "results", "sink_type": "kafka", "error": "connection refused", '
        b'"timestamp": 1700000000.0, "partition": 2, "attempt_count": 1}'
    )


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


# =============================================================================
# close() exception paths — each sink catches Exception from inner resource
# =============================================================================


@patch('drakkar.sinks.kafka.AIOProducer')
async def test_kafka_sink_close_exception_is_caught(mock_cls, kafka_sink_config):
    """Kafka close() catches exception from producer.close() and still sets producer to None."""
    from drakkar.sinks.kafka import KafkaSink

    mock_producer = AsyncMock()
    mock_producer.close.side_effect = RuntimeError('transport error')
    mock_cls.return_value = mock_producer

    sink = KafkaSink('results', kafka_sink_config, brokers_fallback='localhost:9092')
    await sink.connect()

    await sink.close()  # should not raise

    mock_producer.close.assert_called_once()
    assert sink._producer is None


async def test_postgres_sink_close_exception_is_caught(pg_sink_config):
    """Postgres close() catches exception from pool.close() and still sets pool to None."""
    sink, _, mock_pool = _make_pg_sink(pg_sink_config)
    mock_pool.close.side_effect = RuntimeError('connection reset')

    await sink.close()  # should not raise

    mock_pool.close.assert_called_once()
    assert sink.pool is None


async def test_mongo_sink_close_exception_is_caught(mongo_sink_config):
    """Mongo close() catches exception from client.close() and still sets client to None."""
    sink, _, mock_client = await _make_mongo_sink(mongo_sink_config)
    mock_client.close.side_effect = RuntimeError('network unreachable')

    await sink.close()  # should not raise

    mock_client.close.assert_called_once()
    assert sink._client is None
    assert sink._db is None


async def test_http_sink_close_exception_is_caught(http_sink_config):
    """HTTP close() catches exception from client.aclose() and still sets client to None."""
    sink, mock_client = _make_http_sink(http_sink_config)
    mock_client.aclose.side_effect = RuntimeError('aclose failed')

    await sink.close()  # should not raise

    mock_client.aclose.assert_called_once()
    assert sink._client is None


async def test_redis_sink_close_exception_is_caught(redis_sink_config):
    """Redis close() catches exception from client.aclose() and still sets client to None."""
    sink, mock_client = _make_redis_sink(redis_sink_config)
    mock_client.aclose.side_effect = RuntimeError('connection refused')

    await sink.close()  # should not raise

    mock_client.aclose.assert_called_once()
    assert sink._client is None


async def test_dlq_sink_close_exception_is_caught():
    """DLQ close() catches exception from producer.close() and still sets producer to None."""
    sink, mock_producer = _make_dlq_sink()
    mock_producer.close.side_effect = RuntimeError('flush timeout')

    await sink.close()  # should not raise

    mock_producer.close.assert_called_once()
    assert sink._producer is None


# =============================================================================
# DLQ serialize fallback + deliver NotImplementedError
# =============================================================================


def test_dlq_message_serialize_fallback_on_model_dump_failure():
    """When model_dump_json() raises, serialize() falls back to str(payload)."""
    from drakkar.models import DeliveryError
    from drakkar.sinks.dlq import DLQMessage

    broken_payload = MagicMock(spec=BaseModel)
    broken_payload.model_dump_json.side_effect = TypeError('not serializable')

    error = DeliveryError(
        sink_name='results',
        sink_type='kafka',
        error='some error',
        payloads=[broken_payload],
    )
    msg = DLQMessage(delivery_error=error, partition_id=1, attempt_count=1)
    data = msg.serialize()

    import json

    parsed = json.loads(data)
    # fallback should be str(broken_payload)
    assert len(parsed['original_payloads']) == 1
    assert parsed['original_payloads'][0] == str(broken_payload)


async def test_dlq_sink_deliver_raises_not_implemented():
    """DLQSink.deliver() always raises NotImplementedError."""
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    with pytest.raises(NotImplementedError, match=r'Use DLQSink\.send'):
        await sink.deliver([SampleOutput()])


# =============================================================================
# read_dlq_entries — the generator used by scripts/replay_dlq.py
# =============================================================================


def _mock_dlq_message(value_bytes: bytes | None, *, offset: int = 0, partition: int = 0, error=None):
    """Build a MagicMock that matches the confluent_kafka Message surface
    needed by ``read_dlq_entries``.

    Only the handful of methods actually called (``error``, ``value``,
    ``offset``, ``partition``) are stubbed — everything else falls through
    to MagicMock defaults.
    """
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = value_bytes
    msg.offset.return_value = offset
    msg.partition.return_value = partition
    return msg


async def test_read_dlq_entries_drains_and_stops_on_idle():
    """The reader yields every well-formed message then exits when the
    broker reports empty polls for ``idle_polls_before_stop`` ticks.
    """
    import json as _json

    from drakkar.sinks import dlq as dlq_module

    entries = [
        _json.dumps({'original_payloads': ['{"id": 1}']}).encode(),
        _json.dumps({'original_payloads': ['{"id": 2}']}).encode(),
    ]
    messages = [_mock_dlq_message(b, offset=i) for i, b in enumerate(entries)]

    fake_consumer = AsyncMock()
    fake_consumer.consume.side_effect = [messages, [], []]

    with patch.object(dlq_module, 'AIOConsumer', return_value=fake_consumer):
        yielded = []
        async for entry in dlq_module.read_dlq_entries(
            topic='test-dlq',
            brokers='localhost:9092',
            idle_polls_before_stop=2,
            poll_timeout=0.01,
        ):
            yielded.append(entry)

    assert len(yielded) == 2
    assert [e['_kafka_offset'] for e in yielded] == [0, 1]
    assert yielded[0]['original_payloads'] == ['{"id": 1}']
    fake_consumer.close.assert_awaited()


async def test_read_dlq_entries_honors_limit():
    """When ``limit`` is set, iteration stops after exactly that many
    entries even if more messages are available in the batch.
    """
    import json as _json

    from drakkar.sinks import dlq as dlq_module

    entries = [_json.dumps({'original_payloads': [f'{{"id": {i}}}']}).encode() for i in range(5)]
    messages = [_mock_dlq_message(b, offset=i) for i, b in enumerate(entries)]

    fake_consumer = AsyncMock()
    fake_consumer.consume.side_effect = [messages, []]

    with patch.object(dlq_module, 'AIOConsumer', return_value=fake_consumer):
        yielded = []
        async for entry in dlq_module.read_dlq_entries(
            topic='test-dlq',
            brokers='localhost:9092',
            limit=3,
            poll_timeout=0.01,
        ):
            yielded.append(entry)

    assert len(yielded) == 3
    assert [e['_kafka_offset'] for e in yielded] == [0, 1, 2]


async def test_read_dlq_entries_skips_invalid_json():
    """A message whose value is not valid JSON is logged and skipped
    without aborting the iteration.
    """
    import json as _json

    from drakkar.sinks import dlq as dlq_module

    valid = _json.dumps({'original_payloads': ['{"id": 1}']}).encode()
    invalid = b'this-is-not-json'
    messages = [
        _mock_dlq_message(invalid, offset=0),
        _mock_dlq_message(valid, offset=1),
    ]

    fake_consumer = AsyncMock()
    fake_consumer.consume.side_effect = [messages, []]

    with patch.object(dlq_module, 'AIOConsumer', return_value=fake_consumer):
        yielded = []
        async for entry in dlq_module.read_dlq_entries(
            topic='test-dlq',
            brokers='localhost:9092',
            idle_polls_before_stop=1,
            poll_timeout=0.01,
        ):
            yielded.append(entry)

    # Only the well-formed message makes it through.
    assert len(yielded) == 1
    assert yielded[0]['_kafka_offset'] == 1


async def test_read_dlq_entries_skips_partition_eof():
    """Broker-issued ``_PARTITION_EOF`` messages are NOT treated as entries."""
    import json as _json

    from confluent_kafka import KafkaError

    from drakkar.sinks import dlq as dlq_module

    eof_error = MagicMock()
    eof_error.code.return_value = KafkaError._PARTITION_EOF

    valid = _json.dumps({'original_payloads': ['{"id": 1}']}).encode()
    messages = [
        _mock_dlq_message(None, offset=0, error=eof_error),
        _mock_dlq_message(valid, offset=1),
    ]

    fake_consumer = AsyncMock()
    fake_consumer.consume.side_effect = [messages, []]

    with patch.object(dlq_module, 'AIOConsumer', return_value=fake_consumer):
        yielded = []
        async for entry in dlq_module.read_dlq_entries(
            topic='test-dlq',
            brokers='localhost:9092',
            idle_polls_before_stop=1,
            poll_timeout=0.01,
        ):
            yielded.append(entry)

    assert len(yielded) == 1
    assert yielded[0]['_kafka_offset'] == 1


@pytest.mark.parametrize(
    ('ops', 'expected'),
    [
        (['update'], True),
        (['upsert'], True),
        (['update', 'upsert'], True),
        (['insert'], False),
        (['update', 'insert'], False),
        (['statement'], False),
        (['update', 'statement'], False),
    ],
)
def test_postgres_sink_batch_idempotent(pg_sink_config, ops, expected):
    """UPDATE/UPSERT converge on re-delivery; INSERT duplicates and statement SQL is opaque."""
    from drakkar.models import PostgresOp

    sink, _, _ = _make_pg_sink(pg_sink_config)

    builders = {
        'insert': lambda: PostgresPayload(table='t', data=DBResultModel()),
        'update': lambda: PostgresPayload(op=PostgresOp.UPDATE, table='t', data=DBResultModel(), where=DBKeyModel()),
        'upsert': lambda: PostgresPayload(op=PostgresOp.UPSERT, table='t', data=DBResultModel(), conflict=['id']),
        'statement': lambda: PostgresPayload(op=PostgresOp.STATEMENT, statement='claim_job'),
    }
    payloads = [builders[op]() for op in ops]
    assert sink.batch_idempotent(payloads) is expected


def test_postgres_sink_batch_idempotent_empty_batch_is_safe(pg_sink_config):
    """An empty batch has nothing to duplicate, so retrying it changes nothing."""
    sink, _, _ = _make_pg_sink(pg_sink_config)

    assert sink.batch_idempotent([]) is True


# --- golden rendered SQL ---------------------------------------------------
#
# tests/fixtures/pg_rendered_sql.json pins the emitted statements.
# These cases run through the sink's own unit builder and renderers rather
# than a test-local copy of that logic, so a divergence between the two
# backends fails here instead of reaching an operator's database.

_RENDERED_SQL_CORPUS = json.loads((Path(__file__).parent / 'fixtures' / 'pg_rendered_sql.json').read_text())


def _golden_model(pairs):
    """Build a model whose model_dump() yields the fixture's columns in order."""
    return create_model('GoldenModel', **{name: (Any, value) for name, value in pairs})()


def _golden_payloads(case):
    """Turn one fixture case into the payloads a handler would have returned."""
    from drakkar.models import PostgresOp

    op = PostgresOp(case['op'])
    if op is PostgresOp.UPDATE:
        (row,) = case['rows']
        return [
            PostgresPayload(
                op=op,
                table=case['table'],
                data=_golden_model(row),
                where=_golden_model(case['where']),
            )
        ]
    extra = {}
    if op is PostgresOp.UPSERT:
        extra = {'conflict': case['conflict'], 'update_columns': case['update_columns']}
    return [PostgresPayload(op=op, table=case['table'], data=_golden_model(row), **extra) for row in case['rows']]


@pytest.mark.parametrize('case', _RENDERED_SQL_CORPUS['cases'], ids=lambda c: c['case'])
def test_postgres_rendered_sql_corpus(pg_sink_config, case):
    """The sink must emit this SQL and bind these values, in this order."""
    sink, _, _ = _make_pg_sink(pg_sink_config)

    units = [sink._build_unit(p) for p in _golden_payloads(case)]
    if len(units) > 1:
        keys = {u.group_key for u in units}
        assert len(keys) == 1, 'a multi-row case must describe one batchable run'

    from drakkar.sinks.postgres import _StmtUnit

    first = units[0]
    # An update is one fixed statement; an insert or upsert renders against
    # the number of rows batched into it.
    sql = first.sql if isinstance(first, _StmtUnit) else first.render(len(units))
    values = [v for u in units for v in u.values]

    assert sql == case['expected_sql']
    assert values == case['expected_values']


@pytest.mark.parametrize('name', _RENDERED_SQL_CORPUS['identifiers']['valid'])
def test_postgres_rendered_sql_corpus_valid_identifiers(name):
    from drakkar.sinks.pgsql import quote_ident

    assert quote_ident(name) == f'"{name}"'


@pytest.mark.parametrize('name', _RENDERED_SQL_CORPUS['identifiers']['invalid'])
def test_postgres_rendered_sql_corpus_invalid_identifiers(name):
    """The injection defence is a parity surface — it must not diverge."""
    from drakkar.sinks.pgsql import quote_ident

    with pytest.raises(ValueError, match='Invalid SQL identifier'):
        quote_ident(name)


async def test_postgres_sink_emits_columns_in_sorted_order(pg_sink_config):
    """Columns are sorted, not left in model-declaration order.

    Payload data decoded into a mapping has no field order to preserve, so
    sorting is the only rule that can be honoured unconditionally.
    """

    class Declared(BaseModel):
        request_id: str = 'r'
        answer: int = 1

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver([PostgresPayload(table='results', data=Declared())])

    query, *values = mock_conn.execute.call_args[0]
    assert query == 'INSERT INTO "results" ("answer", "request_id") VALUES ($1, $2)'
    assert values == [1, 'r'], 'values must follow the sorted column order'


async def test_postgres_sink_update_sorts_set_and_predicate_columns(pg_sink_config):
    from drakkar.models import PostgresOp

    class Declared(BaseModel):
        status: str = 'done'
        finished_at: str = 't1'

    class Key(BaseModel):
        owner: str = 'me'
        id: int = 42

    sink, mock_conn, _ = _make_pg_sink(pg_sink_config)

    await sink.deliver([PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=Declared(), where=Key())])

    query, *values = mock_conn.execute.call_args[0]
    assert query == 'UPDATE "jobs" SET "finished_at" = $1, "status" = $2 WHERE "id" = $3 AND "owner" = $4'
    assert values == ['t1', 'done', 42, 'me']


async def test_mongo_sink_groups_into_consecutive_runs(mongo_sink_config):
    """Execution order equals payload order, so grouping is run-based.

    Global bucketing — what this sink used to do — would send both 'results'
    documents in one insert_many and defer 'audit' past them. That is
    harmless for inserts and a lost write once updates and deletes exist,
    so the rule is uniform rather than per-op.
    """
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver(
        [
            MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
            MongoPayload(collection='audit', data=SampleOutput(request_id='a1')),
            MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
        ]
    )

    # Three runs of one document each, so every write is an insert_one and
    # each lands on its OWN collection.
    assert collections['results'].insert_one.await_count == 2
    assert collections['audit'].insert_one.await_count == 1
    assert collections['results'].bulk_write.await_count == 0


async def test_mongo_sink_batches_an_uninterrupted_run(mongo_sink_config):
    """Adjacent same-collection documents still travel together."""
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver(
        [
            MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
            MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
            MongoPayload(collection='audit', data=SampleOutput(request_id='a1')),
        ]
    )

    assert collections['results'].bulk_write.await_count == 1
    models = collections['results'].bulk_write.await_args[0][0]
    assert [m._doc['request_id'] for m in models] == ['r1', 'r2']
    assert collections['audit'].insert_one.await_count == 1


async def test_mongo_sink_builds_every_declarative_op(mongo_sink_config):
    """Each op maps to its driver write model, with the arguments Mongo needs."""
    from pymongo import DeleteMany, DeleteOne, UpdateMany, UpdateOne

    cases = [
        (
            MongoPayload(op='update_one', collection='jobs', data=SampleOutput(request_id='r'), filter=_JobKey(id=7)),
            UpdateOne,
        ),
        (
            MongoPayload(op='update_many', collection='jobs', data=SampleOutput(request_id='r'), filter=_JobKey(id=7)),
            UpdateMany,
        ),
        (
            MongoPayload(op='upsert', collection='jobs', data=SampleOutput(request_id='r'), filter=_JobKey(id=7)),
            UpdateOne,
        ),
        (MongoPayload(op='delete_one', collection='jobs', filter=_JobKey(id=7)), DeleteOne),
        (MongoPayload(op='delete_many', collection='jobs', filter=_JobKey(id=7)), DeleteMany),
    ]
    for payload, want_model in cases:
        sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
        # Two payloads so the run takes the bulk path, where the model is
        # what actually goes to the driver.
        await sink.deliver([payload, payload])

        models = collections['jobs'].bulk_write.await_args[0][0]
        assert all(isinstance(m, want_model) for m in models), payload.op


async def test_mongo_sink_update_wraps_data_in_a_set_assignment(mongo_sink_config):
    """The declarative tier assigns fields; anything richer is a statement."""
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver(
        [MongoPayload(op='update_one', collection='jobs', data=SampleOutput(request_id='r1'), filter=_JobKey(id=7))]
    )

    predicate, update = collections['jobs'].update_one.await_args[0]
    assert predicate == {'id': 7}
    assert update == {'$set': {'request_id': 'r1', 'answer': '42'}}


async def test_mongo_sink_upsert_passes_the_upsert_flag(mongo_sink_config):
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver(
        [MongoPayload(op='upsert', collection='jobs', data=SampleOutput(request_id='r1'), filter=_JobKey(id=7))]
    )

    assert collections['jobs'].update_one.await_args[1] == {'upsert': True}


async def test_mongo_sink_delete_sends_only_the_filter(mongo_sink_config):
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver([MongoPayload(op='delete_many', collection='staging', filter=_JobKey(id=7))])

    assert collections['staging'].delete_many.await_args[0] == ({'id': 7},)


async def test_mongo_sink_rejects_a_filter_that_dumps_empty(mongo_sink_config):
    """The second guard: an empty dumped filter matches EVERY document.

    The validator rejects a missing filter at construction; this catches a
    model that dumps to nothing, which a payload mutated afterwards could
    still produce.
    """
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    with pytest.raises(ValueError, match='matches every document'):
        await sink.deliver([MongoPayload(op='delete_many', collection='staging', filter=_Empty())])

    assert 'staging' not in collections, 'the guard must fire before the collection is reached'


async def test_mongo_sink_rejects_data_that_dumps_empty(mongo_sink_config):
    """An empty $set is a malformed update."""
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    with pytest.raises(ValueError, match="empty 'data'"):
        await sink.deliver([MongoPayload(op='update_one', collection='jobs', data=_Empty(), filter=_JobKey(id=7))])

    assert 'jobs' not in collections, 'the guard must fire before the collection is reached'


async def test_mongo_sink_mixes_operations_in_one_bulk_write(mongo_sink_config):
    """One round trip can carry an insert, an update and a delete.

    insert_many could not express this at all, which is the third reason the
    execution path moved to bulk_write.
    """
    from pymongo import DeleteOne, InsertOne, UpdateOne

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    await sink.deliver(
        [
            MongoPayload(collection='jobs', data=SampleOutput(request_id='r1')),
            MongoPayload(op='update_one', collection='jobs', data=SampleOutput(request_id='r2'), filter=_JobKey(id=7)),
            MongoPayload(op='delete_one', collection='jobs', filter=_JobKey(id=8)),
        ]
    )

    models = collections['jobs'].bulk_write.await_args[0][0]
    assert [type(m) for m in models] == [InsertOne, UpdateOne, DeleteOne]


def _statement_config(**overrides):
    """A Mongo sink config carrying two operator-authored statements."""
    statements = {
        'record_attempt': {
            'collection': 'jobs',
            'op': 'update_one',
            'filter': {'_id': ':id'},
            'update': {'$set': {'last_seen': ':now'}, '$inc': {'attempts': 1}},
        },
        'sweep': {
            'collection': 'staging',
            'op': 'delete_many',
            'filter': {'batch': ':batch'},
        },
    }
    return MongoSinkConfig(uri='mongodb://localhost:27017', database='testdb', statements=statements, **overrides)


class _AttemptParams(BaseModel):
    id: int = 42
    now: str = '2026-08-02T00:00:00Z'


class _SweepParams(BaseModel):
    batch: str = 'b-1'


async def test_mongo_sink_runs_a_statement_by_name():
    """The statement supplies collection, op and shape; the payload only values."""
    sink, collections, _ = await _make_mongo_sink(_statement_config())

    await sink.deliver([MongoPayload(op='statement', statement='record_attempt', params=_AttemptParams())])

    predicate, update = collections['jobs'].update_one.await_args[0]
    assert predicate == {'_id': 42}
    # $inc survives untouched — it is exactly what the declarative tier
    # cannot express, and the reason the escape hatch exists.
    assert update == {'$set': {'last_seen': '2026-08-02T00:00:00Z'}, '$inc': {'attempts': 1}}


async def test_mongo_sink_statement_preserves_bound_value_types():
    """A comparison against "42" would silently match no numeric _id."""
    sink, collections, _ = await _make_mongo_sink(_statement_config())

    await sink.deliver([MongoPayload(op='statement', statement='record_attempt', params=_AttemptParams(id=7))])

    predicate, _ = collections['jobs'].update_one.await_args[0]
    assert predicate['_id'] == 7
    assert isinstance(predicate['_id'], int)


async def test_mongo_sink_statement_uses_its_own_collection_and_op():
    sink, collections, _ = await _make_mongo_sink(_statement_config())

    await sink.deliver([MongoPayload(op='statement', statement='sweep', params=_SweepParams())])

    assert collections['staging'].delete_many.await_args[0] == ({'batch': 'b-1'},)


async def test_mongo_sink_statements_are_compiled_once_at_connect():
    """Compiled at connect, so a delivery is a copy plus N assignments."""
    sink, _, _ = await _make_mongo_sink(_statement_config())

    assert sorted(sink._statements) == ['record_attempt', 'sweep']
    assert sink._statements['record_attempt'].template.params == ('id', 'now')


async def test_mongo_sink_unknown_statement_names_the_configured_ones():
    """The operator sees what IS available, not just the miss."""
    sink, collections, _ = await _make_mongo_sink(_statement_config())

    with pytest.raises(ValueError, match='record_attempt, sweep'):
        await sink.deliver([MongoPayload(op='statement', statement='nope')])

    assert collections == {}


async def test_mongo_sink_unknown_statement_with_none_configured(mongo_sink_config):
    sink, _, _ = await _make_mongo_sink(mongo_sink_config)

    with pytest.raises(ValueError, match='<none configured>'):
        await sink.deliver([MongoPayload(op='statement', statement='nope')])


async def test_mongo_sink_statement_rejects_missing_params():
    sink, _, _ = await _make_mongo_sink(_statement_config())

    class _Partial(BaseModel):
        id: int = 1

    with pytest.raises(ValueError, match="'now'"):
        await sink.deliver([MongoPayload(op='statement', statement='record_attempt', params=_Partial())])


async def test_mongo_sink_statement_rejects_extra_params():
    """A silently ignored key is almost always a typo in the payload model."""
    sink, _, _ = await _make_mongo_sink(_statement_config())

    class _Extra(BaseModel):
        id: int = 1
        now: str = 'x'
        nope: int = 2

    with pytest.raises(ValueError, match='typo'):
        await sink.deliver([MongoPayload(op='statement', statement='record_attempt', params=_Extra())])


async def test_mongo_sink_statement_error_names_the_statement_not_its_document():
    sink, _, _ = await _make_mongo_sink(_statement_config())

    class _Partial(BaseModel):
        id: int = 1

    with pytest.raises(ValueError, match="mongo statement 'record_attempt'"):
        await sink.deliver([MongoPayload(op='statement', statement='record_attempt', params=_Partial())])


async def test_mongo_sink_remaps_connection_failure_to_the_builtin(mongo_sink_config):
    """SinkManager matches the BUILTIN ConnectionError, not PyMongo's.

    pymongo.errors.ConnectionFailure inherits only from PyMongoError, so
    without this remapping a dropped connection could never be fast-retried
    — the same latent defect the Redis sink had.
    """
    from pymongo.errors import ConnectionFailure

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    _mongo_collection(collections).insert_one.side_effect = ConnectionFailure('connection reset')

    with pytest.raises(ConnectionError, match='connection reset') as excinfo:
        await sink.deliver([MongoPayload(collection='results', data=SampleOutput())])

    assert isinstance(excinfo.value.__cause__, ConnectionFailure), 'the original must be chained'


async def test_mongo_sink_remaps_network_timeout_to_the_builtin(mongo_sink_config):
    """NetworkTimeout inherits from ConnectionFailure, so order matters."""
    from pymongo.errors import NetworkTimeout

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    _mongo_collection(collections).insert_one.side_effect = NetworkTimeout('timed out')

    with pytest.raises(TimeoutError, match='timed out'):
        await sink.deliver([MongoPayload(collection='results', data=SampleOutput())])


async def test_mongo_sink_does_not_remap_a_write_error(mongo_sink_config):
    """A duplicate key fails identically on every retry, so it stays put."""
    from pymongo.errors import DuplicateKeyError

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    _mongo_collection(collections).insert_one.side_effect = DuplicateKeyError('E11000')

    with pytest.raises(DuplicateKeyError):
        await sink.deliver([MongoPayload(collection='results', data=SampleOutput())])


@pytest.mark.parametrize(
    ('payloads', 'expected'),
    [
        # $set against a fixed filter, and removal, both converge.
        ([{'op': 'update_one'}], True),
        ([{'op': 'update_many'}], True),
        ([{'op': 'upsert'}], True),
        ([{'op': 'delete_one'}], True),
        ([{'op': 'delete_many'}], True),
        ([{'op': 'update_one'}, {'op': 'delete_many'}], True),
        # A plain insert duplicates documents.
        ([{'op': 'insert'}], False),
        # Operator MQL is opaque — $inc accumulates and we cannot tell.
        ([{'op': 'statement'}], False),
        # One unsafe payload vetoes an otherwise safe batch.
        ([{'op': 'delete_one'}, {'op': 'insert'}], False),
        # Vacuously safe — nothing to duplicate.
        ([], True),
    ],
)
async def test_mongo_sink_batch_idempotent(mongo_sink_config, payloads, expected):
    sink, _, _ = await _make_mongo_sink(mongo_sink_config)

    built = []
    for spec in payloads:
        op = spec['op']
        if op == 'statement':
            built.append(MongoPayload(op=op, statement='s'))
        elif op == 'insert':
            built.append(MongoPayload(op=op, collection='c', data=SampleOutput()))
        elif op.startswith('delete'):
            built.append(MongoPayload(op=op, collection='c', filter=_JobKey()))
        else:
            built.append(MongoPayload(op=op, collection='c', data=SampleOutput(), filter=_JobKey()))

    assert sink.batch_idempotent(built) is expected


async def test_mongo_sink_keeps_the_class_flag_as_the_conservative_fallback(mongo_sink_config):
    """The type-level flag stays False; the per-batch hook widens it."""
    sink, _, _ = await _make_mongo_sink(mongo_sink_config)

    assert sink.idempotent is False
    assert sink.batch_idempotent([MongoPayload(op='delete_many', collection='c', filter=_JobKey())]) is True


async def test_mongo_sink_partial_side_effects_precede_a_build_error(mongo_sink_config):
    """A payload the sink cannot build stops the delivery — after the ones
    before it have been written, exactly as the pre-batching loop did."""
    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)

    with pytest.raises(ValueError, match='matches every document'):
        await sink.deliver(
            [
                MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
                MongoPayload(op='delete_many', collection='results', filter=_Empty()),
            ]
        )

    assert collections['results'].insert_one.await_count == 1


async def test_mongo_sink_reraises_a_bulk_error_carrying_no_write_errors(mongo_sink_config):
    """Without an index there is no payload to blame, so it propagates as-is."""
    from pymongo.errors import BulkWriteError

    sink, collections, _ = await _make_mongo_sink(mongo_sink_config)
    _mongo_collection(collections).bulk_write.side_effect = BulkWriteError({'nInserted': 0})

    with pytest.raises(BulkWriteError):
        await sink.deliver(
            [
                MongoPayload(collection='results', data=SampleOutput(request_id='r1')),
                MongoPayload(collection='results', data=SampleOutput(request_id='r2')),
            ]
        )


async def test_dlq_sink_flush_is_bounded_by_the_configured_timeout():
    """Same reasoning as the Kafka sink, and it matters more here: a DLQ
    write that blocks for ``message.timeout.ms`` stalls the partition it was
    meant to rescue.
    """
    from drakkar.sinks.dlq import DLQSink

    mock_producer = AsyncMock()
    mock_producer.produce.side_effect = lambda **kw: _make_future()
    mock_producer.flush.return_value = 0
    sink = DLQSink(topic='test-dlq', brokers='localhost:9092', flush_timeout_seconds=7.5)
    sink._producer = mock_producer

    assert await sink.send(_sample_delivery_error(), partition_id=0) is True

    mock_producer.flush.assert_called_once_with(7.5)
