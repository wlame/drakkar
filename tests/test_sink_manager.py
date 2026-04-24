"""Tests for SinkManager — routing, validation, delivery, error handling."""

import asyncio
import time
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


# --- connect_all parallelism ---


class SlowConnectSink(FakeSink):
    """FakeSink whose connect() sleeps to simulate real network/DB latency."""

    def __init__(self, name: str, sink_type: str = 'kafka', connect_delay: float = 0.1) -> None:
        super().__init__(name=name, sink_type=sink_type)
        self.connect_delay = connect_delay

    async def connect(self) -> None:
        await asyncio.sleep(self.connect_delay)
        self.connected = True


class FailConnectSink(FakeSink):
    """FakeSink whose connect() raises — exercises fail-fast semantics."""

    async def connect(self) -> None:
        raise RuntimeError('connect failed')


async def test_connect_all_runs_sinks_in_parallel():
    """Three sinks each sleeping 0.1s should connect in ~0.1s total, not 0.3s.

    Threshold is 0.2s — generous slack to avoid CI scheduler-variance flake
    while still failing loudly if the implementation regressed to serial.
    """
    mgr = SinkManager()
    mgr.register(SlowConnectSink(name='a', sink_type='kafka', connect_delay=0.1))
    mgr.register(SlowConnectSink(name='b', sink_type='postgres', connect_delay=0.1))
    mgr.register(SlowConnectSink(name='c', sink_type='mongo', connect_delay=0.1))

    start = time.monotonic()
    await mgr.connect_all()
    elapsed = time.monotonic() - start

    assert elapsed < 0.2, f'expected parallel connect (<0.2s), got {elapsed:.3f}s'
    for sink in mgr.sinks.values():
        assert sink.connected  # type: ignore[attr-defined]


async def test_connect_all_raises_when_any_sink_fails():
    """If one sink.connect() raises, connect_all propagates the first failure.

    Semantic note: the other sinks may be mid-connect when the first failure
    fires; asyncio.gather cancels their coroutines. This is NOT atomic
    all-or-nothing — callers must not assume every sink is cleanly connected
    or cleanly untouched after a raise.
    """
    mgr = SinkManager()
    mgr.register(FailConnectSink(name='bad', sink_type='kafka'))
    mgr.register(FakeSink(name='ok', sink_type='postgres'))

    with pytest.raises(RuntimeError, match='connect failed'):
        await mgr.connect_all()


async def test_connect_all_with_zero_sinks():
    """Empty sink list — asyncio.gather() with no args returns empty tuple cleanly."""
    mgr = SinkManager()
    await mgr.connect_all()  # should not raise


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


# --- SinkStats tracking ---


def test_get_sink_info_returns_all_sinks():
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))
    mgr.register(FakeSink('db', sink_type='postgres'))
    info = mgr.get_sink_info()
    assert len(info) == 2
    types = {i['sink_type'] for i in info}
    names = {i['name'] for i in info}
    assert types == {'kafka', 'postgres'}
    assert names == {'out', 'db'}


def test_get_sink_info_empty():
    mgr = SinkManager()
    assert mgr.get_sink_info() == []


def test_get_all_stats_empty():
    mgr = SinkManager()
    assert mgr.get_all_stats() == {}


def test_stats_initialized_on_register():
    mgr = SinkManager()
    mgr.register(FakeSink('out', sink_type='kafka'))
    stats = mgr.get_all_stats()
    assert ('kafka', 'out') in stats
    s = stats[('kafka', 'out')]
    assert s.delivered_count == 0
    assert s.error_count == 0


async def test_stats_updated_on_successful_delivery():
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData(value=1)), KafkaPayload(data=SampleData(value=2))])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    stats = mgr.get_all_stats()
    s = stats[('kafka', 'out')]
    assert s.delivered_count == 1
    assert s.delivered_payloads == 2
    assert s.last_delivery_ts is not None
    assert s.last_delivery_duration is not None
    assert s.last_delivery_duration >= 0
    assert s.error_count == 0


async def test_stats_updated_on_error():
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    stats = mgr.get_all_stats()
    s = stats[('kafka', 'out')]
    assert s.error_count == 1
    assert s.last_error == 'delivery failed'
    assert s.last_error_ts is not None
    assert s.delivered_count == 0


async def test_stats_updated_on_retry_then_success():
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

    stats = mgr.get_all_stats()
    s = stats[('kafka', 'out')]
    assert s.delivered_count == 1  # succeeded on retry
    assert s.error_count == 1  # one failure before success
    assert s.retry_count == 1
    assert s.last_error == 'transient error'


async def test_stats_multiple_sinks_tracked_independently():
    mgr = SinkManager()
    k_sink = FakeSink('k', sink_type='kafka')
    p_sink = FakeSink('p', sink_type='postgres')
    p_sink.fail_on_deliver = True
    mgr.register(k_sink)
    mgr.register(p_sink)

    result = CollectResult(
        kafka=[KafkaPayload(sink='k', data=SampleData())],
        postgres=[PostgresPayload(sink='p', table='t', data=SampleData())],
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    stats = mgr.get_all_stats()
    assert stats[('kafka', 'k')].delivered_count == 1
    assert stats[('kafka', 'k')].error_count == 0
    assert stats[('postgres', 'p')].delivered_count == 0
    assert stats[('postgres', 'p')].error_count == 1


async def test_stats_with_recorder():
    """Stats are recorded to EventRecorder when provided."""
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    from unittest.mock import MagicMock

    recorder = MagicMock()

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, recorder=recorder)

    recorder.record_sink_delivery.assert_called_once()
    call_kwargs = recorder.record_sink_delivery.call_args
    assert call_kwargs[1]['sink_type'] == 'kafka'
    assert call_kwargs[1]['sink_name'] == 'out'
    assert call_kwargs[1]['payload_count'] == 1


async def test_prometheus_retry_counter_incremented():
    """sink_delivery_retries Prometheus counter increments on RETRY."""
    from drakkar.metrics import sink_delivery_retries

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

    before = sink_delivery_retries.labels(sink_type='kafka', sink_name='out')._value.get()
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    after = sink_delivery_retries.labels(sink_type='kafka', sink_name='out')._value.get()
    assert after == before + 1


async def test_prometheus_skip_counter_incremented():
    """sink_deliveries_skipped Prometheus counter increments on SKIP."""
    from drakkar.metrics import sink_deliveries_skipped

    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    before = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    after = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()
    assert after == before + 1


async def test_prometheus_skip_not_incremented_on_dlq():
    """DLQ action should NOT increment skip counter."""
    from drakkar.metrics import sink_deliveries_skipped

    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)

    before = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    after = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()
    assert after == before  # no change


async def test_prometheus_retry_counter_per_sink():
    """Retry counter uses correct sink labels."""
    from drakkar.metrics import sink_delivery_retries

    mgr = SinkManager()
    sink_a = FakeSink('a', sink_type='kafka')
    sink_b = FakeSink('b', sink_type='postgres')
    sink_b.fail_on_deliver = True
    mgr.register(sink_a)
    mgr.register(sink_b)

    call_count = 0

    async def flaky_deliver(payloads: list) -> None:
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            raise RuntimeError('error')

    sink_b.deliver = flaky_deliver  # type: ignore[assignment]
    on_error = AsyncMock(return_value=DeliveryAction.RETRY)

    before_a = sink_delivery_retries.labels(sink_type='kafka', sink_name='a')._value.get()
    before_b = sink_delivery_retries.labels(sink_type='postgres', sink_name='b')._value.get()

    result = CollectResult(
        kafka=[KafkaPayload(sink='a', data=SampleData())],
        postgres=[PostgresPayload(sink='b', table='t', data=SampleData())],
    )
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    after_a = sink_delivery_retries.labels(sink_type='kafka', sink_name='a')._value.get()
    after_b = sink_delivery_retries.labels(sink_type='postgres', sink_name='b')._value.get()
    assert after_a == before_a  # kafka had no errors
    assert after_b == before_b + 1  # postgres retried once


async def test_stats_error_with_recorder():
    """Errors are recorded to EventRecorder when provided."""
    mgr = SinkManager()
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    from unittest.mock import MagicMock

    recorder = MagicMock()

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, recorder=recorder)

    recorder.record_sink_error.assert_called_once()
    call_kwargs = recorder.record_sink_error.call_args
    assert call_kwargs[1]['sink_type'] == 'kafka'
    assert call_kwargs[1]['sink_name'] == 'out'
    assert 'delivery failed' in call_kwargs[1]['error']
    assert call_kwargs[1]['attempt'] == 1


# --- deliver_all parallelism ---


class SlowDeliverSink(FakeSink):
    """FakeSink whose deliver() sleeps before recording — used for timing tests."""

    def __init__(self, name: str, sink_type: str = 'kafka', deliver_delay: float = 0.0) -> None:
        super().__init__(name=name, sink_type=sink_type)
        self.deliver_delay = deliver_delay

    async def deliver(self, payloads: list[BaseModel]) -> None:
        await asyncio.sleep(self.deliver_delay)
        if self.fail_on_deliver:
            raise RuntimeError('delivery failed')
        self.delivered.append(payloads)


async def test_deliver_all_runs_sinks_in_parallel():
    """Three sinks with different latencies should deliver in ~max(latencies).

    With sequential delivery total would be 50 + 100 + 200 = 350ms. With gather
    it should be ~200ms. Threshold 0.30s leaves generous slack for CI scheduler
    variance while still failing loudly if the implementation regressed to serial.
    """
    mgr = SinkManager()
    mgr.register(SlowDeliverSink(name='fast', sink_type='kafka', deliver_delay=0.05))
    mgr.register(SlowDeliverSink(name='medium', sink_type='postgres', deliver_delay=0.1))
    mgr.register(SlowDeliverSink(name='slow', sink_type='mongo', deliver_delay=0.2))

    result = CollectResult(
        kafka=[KafkaPayload(sink='fast', data=SampleData())],
        postgres=[PostgresPayload(sink='medium', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='slow', collection='c', data=SampleData())],
    )
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    start = time.monotonic()
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    elapsed = time.monotonic() - start

    assert elapsed < 0.30, f'expected parallel deliver (<0.30s), got {elapsed:.3f}s'
    stats = mgr.get_all_stats()
    assert stats[('kafka', 'fast')].delivered_count == 1
    assert stats[('postgres', 'medium')].delivered_count == 1
    assert stats[('mongo', 'slow')].delivered_count == 1


async def test_deliver_all_one_sink_failure_does_not_block_others():
    """A slow, always-failing sink must not drag down siblings' wall-clock.

    The failing sink retries (each attempt takes deliver_delay), so serial
    execution would pin total time to failing_sink_retries * delay + siblings.
    With gather, the two successful sinks complete in ~their own latency.
    """
    mgr = SinkManager()
    good_a = SlowDeliverSink(name='good_a', sink_type='kafka', deliver_delay=0.05)
    good_b = SlowDeliverSink(name='good_b', sink_type='postgres', deliver_delay=0.05)
    bad = SlowDeliverSink(name='bad', sink_type='mongo', deliver_delay=0.15)
    bad.fail_on_deliver = True
    mgr.register(good_a)
    mgr.register(good_b)
    mgr.register(bad)

    result = CollectResult(
        kafka=[KafkaPayload(sink='good_a', data=SampleData())],
        postgres=[PostgresPayload(sink='good_b', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='bad', collection='c', data=SampleData())],
    )
    # DLQ action: bad sink stops after one attempt; total time dominated by
    # bad sink's single 0.15s attempt rather than 3x retries.
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)

    start = time.monotonic()
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    elapsed = time.monotonic() - start

    assert len(good_a.delivered) == 1
    assert len(good_b.delivered) == 1
    # bad sink never appended to delivered (failed), but others did
    assert len(bad.delivered) == 0
    # Total should be ~0.15s (max of fast siblings + single bad attempt),
    # not sum. Generous slack for CI variance.
    assert elapsed < 0.30, f'expected parallel deliver with one failure (<0.30s), got {elapsed:.3f}s'


async def test_deliver_all_with_all_sinks_failing_routes_all_to_dlq():
    """All three sinks fail simultaneously — DLQ handler called for each, no unhandled exceptions."""
    mgr = SinkManager()
    s1 = FakeSink('a', sink_type='kafka')
    s2 = FakeSink('b', sink_type='postgres')
    s3 = FakeSink('c', sink_type='mongo')
    s1.fail_on_deliver = True
    s2.fail_on_deliver = True
    s3.fail_on_deliver = True
    mgr.register(s1)
    mgr.register(s2)
    mgr.register(s3)

    # Track each DLQ-routed error so we can assert all three sinks reach the handler.
    dlq_calls: list[DeliveryError] = []

    async def dlq_handler(error: DeliveryError) -> DeliveryAction:
        dlq_calls.append(error)
        return DeliveryAction.DLQ

    result = CollectResult(
        kafka=[KafkaPayload(sink='a', data=SampleData())],
        postgres=[PostgresPayload(sink='b', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='c', collection='c', data=SampleData())],
    )

    # Must not raise — gather with return_exceptions=True absorbs per-sink errors.
    await mgr.deliver_all(result, on_delivery_error=dlq_handler, partition_id=0)

    assert len(dlq_calls) == 3
    routed_sink_names = {err.sink_name for err in dlq_calls}
    assert routed_sink_names == {'a', 'b', 'c'}

    # All three sinks show one error recorded in stats, no successful deliveries.
    stats = mgr.get_all_stats()
    for key in [('kafka', 'a'), ('postgres', 'b'), ('mongo', 'c')]:
        assert stats[key].error_count == 1
        assert stats[key].delivered_count == 0


async def test_deliver_all_per_sink_retries_still_work():
    """One sink fails twice then succeeds on the third try; others succeed once.

    Verifies that the retry loop inside _deliver_to_sink runs per-sink when
    dispatched via gather — the flaky sink gets 3 attempts while siblings
    complete in their single attempt.
    """
    mgr = SinkManager()
    flaky = FakeSink('flaky', sink_type='kafka')
    ok_a = FakeSink('ok_a', sink_type='postgres')
    ok_b = FakeSink('ok_b', sink_type='mongo')
    mgr.register(flaky)
    mgr.register(ok_a)
    mgr.register(ok_b)

    call_count = 0
    original_deliver = flaky.deliver

    async def flaky_deliver(payloads: list) -> None:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError(f'transient error #{call_count}')
        await original_deliver(payloads)

    flaky.deliver = flaky_deliver  # type: ignore[assignment]
    on_error = AsyncMock(return_value=DeliveryAction.RETRY)

    result = CollectResult(
        kafka=[KafkaPayload(sink='flaky', data=SampleData())],
        postgres=[PostgresPayload(sink='ok_a', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='ok_b', collection='c', data=SampleData())],
    )
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, max_retries=5)

    # All three sinks eventually succeed.
    assert len(flaky.delivered) == 1
    assert len(ok_a.delivered) == 1
    assert len(ok_b.delivered) == 1

    stats = mgr.get_all_stats()
    assert stats[('kafka', 'flaky')].delivered_count == 1
    assert stats[('kafka', 'flaky')].retry_count == 2  # retried twice before success
    assert stats[('postgres', 'ok_a')].delivered_count == 1
    assert stats[('postgres', 'ok_a')].retry_count == 0
    assert stats[('mongo', 'ok_b')].delivered_count == 1
    assert stats[('mongo', 'ok_b')].retry_count == 0


async def test_deliver_all_preserves_sink_stats_per_sink_under_gather():
    """Concurrent delivery must not cross-contaminate per-sink stats.

    One sink fails, two succeed — each sink's delivered_count / error_count /
    last_error must reflect only its own deliveries, never a sibling's.
    """
    mgr = SinkManager()
    s_fail = FakeSink('fail', sink_type='kafka')
    s_ok_a = FakeSink('ok_a', sink_type='postgres')
    s_ok_b = FakeSink('ok_b', sink_type='mongo')
    s_fail.fail_on_deliver = True
    mgr.register(s_fail)
    mgr.register(s_ok_a)
    mgr.register(s_ok_b)

    result = CollectResult(
        kafka=[KafkaPayload(sink='fail', data=SampleData())],
        postgres=[PostgresPayload(sink='ok_a', table='t', data=SampleData())],
        mongo=[MongoPayload(sink='ok_b', collection='c', data=SampleData())],
    )
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    stats = mgr.get_all_stats()

    fail_stats = stats[('kafka', 'fail')]
    assert fail_stats.delivered_count == 0
    assert fail_stats.error_count == 1
    assert fail_stats.last_error == 'delivery failed'
    assert fail_stats.last_error_ts is not None

    ok_a_stats = stats[('postgres', 'ok_a')]
    assert ok_a_stats.delivered_count == 1
    assert ok_a_stats.error_count == 0
    assert ok_a_stats.last_error is None
    assert ok_a_stats.last_error_ts is None

    ok_b_stats = stats[('mongo', 'ok_b')]
    assert ok_b_stats.delivered_count == 1
    assert ok_b_stats.error_count == 0
    assert ok_b_stats.last_error is None
    assert ok_b_stats.last_error_ts is None
