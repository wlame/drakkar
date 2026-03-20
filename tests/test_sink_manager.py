"""Tests for SinkManager — routing, validation, delivery, error handling."""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MongoPayload,
    PostgresPayload,
    RedisPayload,
)
from drakkar.sinks.base import BaseSink
from drakkar.sinks.manager import AmbiguousSinkError, SinkManager, SinkNotConfiguredError

# --- Test helpers ---


class SampleData(BaseModel):
    value: int = 1


class FakeSink(BaseSink):
    """In-memory sink for testing."""

    sink_type = 'kafka'

    def __init__(self, name: str, sink_type: str = 'kafka') -> None:
        super().__init__(name)
        self.sink_type = sink_type
        self.connected = False
        self.closed = False
        self.delivered: list[list[BaseModel]] = []
        self.fail_on_deliver = False

    async def connect(self) -> None:
        self.connected = True

    async def deliver(self, payloads: list[BaseModel]) -> None:
        if self.fail_on_deliver:
            raise RuntimeError('delivery failed')
        self.delivered.append(payloads)

    async def close(self) -> None:
        self.closed = True


class FailCloseSink(FakeSink):
    async def close(self) -> None:
        raise RuntimeError('close failed')


# --- Registration ---


def test_register_sink():
    mgr = SinkManager()
    sink = FakeSink('results')
    mgr.register(sink)
    assert mgr.sink_count == 1
    assert ('kafka', 'results') in mgr.sinks


def test_register_multiple_types():
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))
    mgr.register(FakeSink('main', sink_type='postgres'))
    assert mgr.sink_count == 2


def test_register_duplicate_raises():
    mgr = SinkManager()
    mgr.register(FakeSink('results'))
    with pytest.raises(ValueError, match='Duplicate sink'):
        mgr.register(FakeSink('results'))


def test_register_same_name_different_types():
    mgr = SinkManager()
    mgr.register(FakeSink('main', sink_type='kafka'))
    mgr.register(FakeSink('main', sink_type='postgres'))
    assert mgr.sink_count == 2


# --- Connect / Close lifecycle ---


async def test_connect_all():
    mgr = SinkManager()
    s1 = FakeSink('a', sink_type='kafka')
    s2 = FakeSink('b', sink_type='postgres')
    mgr.register(s1)
    mgr.register(s2)

    await mgr.connect_all()
    assert s1.connected
    assert s2.connected


async def test_close_all():
    mgr = SinkManager()
    s1 = FakeSink('a', sink_type='kafka')
    s2 = FakeSink('b', sink_type='postgres')
    mgr.register(s1)
    mgr.register(s2)

    await mgr.close_all()
    assert s1.closed
    assert s2.closed


async def test_close_all_logs_errors_but_continues():
    """If one sink fails to close, the others still close."""
    mgr = SinkManager()
    s1 = FailCloseSink('fail', sink_type='kafka')
    s2 = FakeSink('ok', sink_type='postgres')
    mgr.register(s1)
    mgr.register(s2)

    await mgr.close_all()  # should not raise
    assert s2.closed


# --- resolve_sink ---


def test_resolve_single_sink_default():
    """Empty sink name resolves to the only sink of that type."""
    mgr = SinkManager()
    sink = FakeSink('results')
    mgr.register(sink)

    resolved = mgr.resolve_sink('kafka', '')
    assert resolved is sink


def test_resolve_explicit_name():
    mgr = SinkManager()
    s1 = FakeSink('results')
    s2 = FakeSink('notifications')
    mgr.register(s1)
    mgr.register(s2)

    assert mgr.resolve_sink('kafka', 'results') is s1
    assert mgr.resolve_sink('kafka', 'notifications') is s2


def test_resolve_ambiguous_raises():
    """Empty sink name with multiple sinks of same type raises."""
    mgr = SinkManager()
    mgr.register(FakeSink('results'))
    mgr.register(FakeSink('notifications'))

    with pytest.raises(AmbiguousSinkError, match="2 'kafka' sinks"):
        mgr.resolve_sink('kafka', '')


def test_resolve_unknown_type_raises():
    mgr = SinkManager()
    with pytest.raises(SinkNotConfiguredError, match="No 'kafka' sink configured"):
        mgr.resolve_sink('kafka', '')


def test_resolve_unknown_name_raises():
    mgr = SinkManager()
    mgr.register(FakeSink('results'))

    with pytest.raises(SinkNotConfiguredError, match="'kafka'/'nonexistent'"):
        mgr.resolve_sink('kafka', 'nonexistent')


# --- validate_collect ---


def test_validate_collect_all_configured():
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))
    mgr.register(FakeSink('db', sink_type='postgres'))

    result = CollectResult(
        kafka=[KafkaPayload(sink='out', data=SampleData())],
        postgres=[PostgresPayload(sink='db', table='t', data=SampleData())],
    )
    mgr.validate_collect(result)  # should not raise


def test_validate_collect_default_sink():
    """Empty sink name is OK when exactly one sink of that type exists."""
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    mgr.validate_collect(result)  # should not raise


def test_validate_collect_unconfigured_type_raises():
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))

    result = CollectResult(
        kafka=[KafkaPayload(data=SampleData())],
        postgres=[PostgresPayload(table='t', data=SampleData())],
    )
    with pytest.raises(SinkNotConfiguredError, match="No 'postgres' sink"):
        mgr.validate_collect(result)


def test_validate_collect_ambiguous_raises():
    mgr = SinkManager()
    mgr.register(FakeSink('a'))
    mgr.register(FakeSink('b'))

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    with pytest.raises(AmbiguousSinkError):
        mgr.validate_collect(result)


def test_validate_collect_empty_result():
    mgr = SinkManager()
    result = CollectResult()
    mgr.validate_collect(result)  # no payloads → no validation needed


def test_validate_all_sink_types():
    """Validation works for all six sink types."""
    mgr = SinkManager()
    mgr.register(FakeSink('k', sink_type='kafka'))
    mgr.register(FakeSink('p', sink_type='postgres'))
    mgr.register(FakeSink('m', sink_type='mongo'))
    mgr.register(FakeSink('h', sink_type='http'))
    mgr.register(FakeSink('r', sink_type='redis'))
    mgr.register(FakeSink('f', sink_type='filesystem'))

    result = CollectResult(
        kafka=[KafkaPayload(sink='k', data=SampleData())],
        postgres=[PostgresPayload(sink='p', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='m', collection='c', data=SampleData())],
        http=[HttpPayload(sink='h', data=SampleData())],
        redis=[RedisPayload(sink='r', key='k', data=SampleData())],
        files=[FilePayload(sink='f', path='/tmp/x', data=SampleData())],
    )
    mgr.validate_collect(result)  # should not raise


# --- deliver_all ---


async def test_deliver_all_success():
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData(value=42))])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(sink.delivered) == 1
    assert len(sink.delivered[0]) == 1
    on_error.assert_not_called()


async def test_deliver_all_multiple_sinks():
    mgr = SinkManager()
    kafka_sink = FakeSink('out', sink_type='kafka')
    pg_sink = FakeSink('db', sink_type='postgres')
    mgr.register(kafka_sink)
    mgr.register(pg_sink)

    result = CollectResult(
        kafka=[KafkaPayload(sink='out', data=SampleData())],
        postgres=[PostgresPayload(sink='db', table='t', data=SampleData())],
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(kafka_sink.delivered) == 1
    assert len(pg_sink.delivered) == 1


async def test_deliver_all_groups_by_sink():
    """Multiple payloads to the same sink are delivered in one batch."""
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    result = CollectResult(
        kafka=[
            KafkaPayload(data=SampleData(value=1)),
            KafkaPayload(data=SampleData(value=2)),
            KafkaPayload(data=SampleData(value=3)),
        ]
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(sink.delivered) == 1  # one batch
    assert len(sink.delivered[0]) == 3  # three payloads


async def test_deliver_all_routes_to_named_sinks():
    """Payloads with different sink names go to different sink instances."""
    mgr = SinkManager()
    s1 = FakeSink('results', sink_type='kafka')
    s2 = FakeSink('notifications', sink_type='kafka')
    mgr.register(s1)
    mgr.register(s2)

    result = CollectResult(
        kafka=[
            KafkaPayload(sink='results', data=SampleData(value=1)),
            KafkaPayload(sink='notifications', data=SampleData(value=2)),
        ]
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(s1.delivered) == 1
    assert len(s2.delivered) == 1


async def test_deliver_all_empty_result():
    mgr = SinkManager()
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(CollectResult(), on_delivery_error=on_error, partition_id=0)
    on_error.assert_not_called()


# --- Delivery error handling ---


async def test_deliver_error_skip():
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    on_error.assert_called_once()
    error_arg: DeliveryError = on_error.call_args[0][0]
    assert error_arg.sink_name == 'out'
    assert error_arg.sink_type == 'kafka'
    assert 'delivery failed' in error_arg.error


async def test_deliver_error_retry_then_succeed():
    """RETRY action retries delivery. If it succeeds on retry, no DLQ."""
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    call_count = 0
    original_deliver = sink.deliver

    async def flaky_deliver(payloads: list) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError('transient error')
        await original_deliver(payloads)

    sink.deliver = flaky_deliver  # type: ignore[assignment]
    on_error = AsyncMock(return_value=DeliveryAction.RETRY)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert call_count == 2  # first fail + retry succeed
    on_error.assert_called_once()
    assert len(sink.delivered) == 1


async def test_deliver_error_retry_exhausted_falls_to_dlq():
    """After max_retries, falls through even with RETRY action."""
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.RETRY)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, max_retries=2)

    assert on_error.call_count == 2  # called on each failed attempt


async def test_deliver_error_dlq_action():
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    on_error.assert_called_once()


async def test_deliver_partial_failure():
    """If one sink fails, the other still delivers."""
    mgr = SinkManager()
    good_sink = FakeSink('good', sink_type='kafka')
    bad_sink = FakeSink('bad', sink_type='postgres')
    bad_sink.fail_on_deliver = True
    mgr.register(good_sink)
    mgr.register(bad_sink)

    result = CollectResult(
        kafka=[KafkaPayload(sink='good', data=SampleData())],
        postgres=[PostgresPayload(sink='bad', table='t', data=SampleData())],
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(good_sink.delivered) == 1  # kafka succeeded
    on_error.assert_called_once()  # postgres failed


# --- BaseSink repr ---


def test_sink_repr():
    sink = FakeSink('results')
    assert "type='kafka'" in repr(sink)
    assert "name='results'" in repr(sink)
