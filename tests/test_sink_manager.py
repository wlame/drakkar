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
from drakkar.sinks.manager import (
    CIRCUIT_OPEN_ERROR,
    AmbiguousSinkError,
    SinkManager,
    SinkNotConfiguredError,
)

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

    Upper threshold 0.15s — tight enough to rule out partial-parallelism
    (2-of-3 serial would land around ~0.2s). Lower bound 0.09s rules out
    a degenerate implementation that returns before any sink actually
    slept for its full delay. Both bounds fail loudly if a regression
    partially or fully serializes connect_all.
    """
    mgr = SinkManager()
    mgr.register(SlowConnectSink(name='a', sink_type='kafka', connect_delay=0.1))
    mgr.register(SlowConnectSink(name='b', sink_type='postgres', connect_delay=0.1))
    mgr.register(SlowConnectSink(name='c', sink_type='mongo', connect_delay=0.1))

    start = time.monotonic()
    await mgr.connect_all()
    elapsed = time.monotonic() - start

    assert 0.09 <= elapsed < 0.15, f'expected parallel connect in [0.09s, 0.15s), got {elapsed:.3f}s'
    for sink in mgr.sinks.values():
        assert sink.connected  # type: ignore[attr-defined]


async def test_connect_all_raises_when_any_sink_fails():
    """If one sink.connect() raises, connect_all propagates the first failure.

    Semantic note: connect_all uses ``return_exceptions=True`` internally so
    every sink's connect runs to completion. The first exception observed
    (by iteration order) is what propagates. Sinks that connected
    successfully are closed before re-raising — see the partial-failure
    cleanup tests below for the cleanup invariants.
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


# --- connect_all partial-failure cleanup ---


class TrackingCloseSink(FakeSink):
    """FakeSink that records whether close() was called.

    Used by the partial-failure cleanup tests to assert that connect_all
    closes the already-connected sinks when a peer's connect fails.
    """

    def __init__(self, name: str, sink_type: str = 'kafka', fail_on_connect: bool = False) -> None:
        super().__init__(name=name, sink_type=sink_type)
        self.fail_on_connect = fail_on_connect
        self.close_call_count = 0

    async def connect(self) -> None:
        if self.fail_on_connect:
            raise RuntimeError(f'connect failed: {self.name}')
        self.connected = True

    async def close(self) -> None:
        # Count calls rather than overwriting — lets tests distinguish
        # "called once" from "not called" from "called twice".
        self.close_call_count += 1
        self.closed = True


class TrackingCloseFailSink(TrackingCloseSink):
    """TrackingCloseSink whose close() raises — exercises cleanup-of-cleanup path."""

    async def close(self) -> None:
        self.close_call_count += 1
        raise RuntimeError(f'close failed: {self.name}')


async def test_connect_all_cleans_up_successfully_connected_sinks_on_partial_failure():
    """When sink[1] fails to connect, the already-connected sinks[0] and sinks[2] get close()d.

    Partial-failure cleanup: the operator expects startup to be atomic from
    a resource-leak standpoint — a failed ``connect_all`` must not leave
    open connections hanging around until process exit.
    """
    mgr = SinkManager()
    s0 = TrackingCloseSink(name='s0', sink_type='kafka')
    s1 = TrackingCloseSink(name='s1', sink_type='postgres', fail_on_connect=True)
    s2 = TrackingCloseSink(name='s2', sink_type='mongo')
    mgr.register(s0)
    mgr.register(s1)
    mgr.register(s2)

    # The original connect exception from s1 must propagate — this is the
    # signal the operator needs to see to diagnose the startup failure.
    with pytest.raises(RuntimeError, match='connect failed: s1'):
        await mgr.connect_all()

    # s0 and s2 connected successfully, so their close() must have been
    # called exactly once to release resources.
    assert s0.close_call_count == 1, f's0.close should have been called once, got {s0.close_call_count}'
    assert s2.close_call_count == 1, f's2.close should have been called once, got {s2.close_call_count}'

    # s1 never connected (its connect raised) — there's nothing to close,
    # so we must NOT have called close() on it. Calling close() on a sink
    # that was never connected could crash or corrupt state.
    assert s1.close_call_count == 0, (
        f's1.close should NOT have been called (it never connected), got {s1.close_call_count}'
    )


async def test_connect_all_cleanup_close_failure_does_not_mask_original_exception():
    """If close() also raises during cleanup, the ORIGINAL connect exception still propagates.

    Cleanup errors are a distraction from the root cause — the operator
    needs to see WHY startup failed, not WHY cleanup failed after that.
    The test also checks that a failing close() on one sink does not block
    close() of the others (return_exceptions=True on the cleanup gather).
    """
    mgr = SinkManager()
    # s0 connects, but its close() raises — simulates a flaky cleanup path.
    s0 = TrackingCloseFailSink(name='s0', sink_type='kafka')
    # s1 fails to connect — this is the exception the operator should see.
    s1 = TrackingCloseSink(name='s1', sink_type='postgres', fail_on_connect=True)
    # s2 connects cleanly — its close() must still be called even though
    # s0's close() raised first.
    s2 = TrackingCloseSink(name='s2', sink_type='mongo')
    mgr.register(s0)
    mgr.register(s1)
    mgr.register(s2)

    # The ORIGINAL exception (s1's connect failure) propagates — NOT s0's
    # close failure.
    with pytest.raises(RuntimeError, match='connect failed: s1'):
        await mgr.connect_all()

    # Both s0 and s2 had close() called. s0's close raised, but that
    # must not have prevented s2.close() from being called.
    assert s0.close_call_count == 1
    assert s2.close_call_count == 1, (
        's2.close must still run even if s0.close raised — cleanup gather uses return_exceptions=True'
    )


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
    from unittest.mock import MagicMock

    recorder = MagicMock()
    mgr = SinkManager(recorder=recorder)
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

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
    from unittest.mock import MagicMock

    recorder = MagicMock()
    mgr = SinkManager(recorder=recorder)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)

    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    recorder.record_sink_error.assert_called_once()
    call_kwargs = recorder.record_sink_error.call_args
    assert call_kwargs[1]['sink_type'] == 'kafka'
    assert call_kwargs[1]['sink_name'] == 'out'
    assert 'delivery failed' in call_kwargs[1]['error']
    assert call_kwargs[1]['attempt'] == 1


async def test_deliver_all_tolerates_none_recorder_and_none_dlq_sink_on_failure():
    """Contract test: SinkManager with recorder=None and dlq_sink=None must
    not ``AttributeError`` when a sink fails. The None-tolerance of the
    internal guards (``if self._recorder is not None``, ``if self._dlq_sink
    is not None``) is what lets ``_async_run`` skip recorder wiring when
    ``debug.enabled=False`` — and lets tests construct a bare manager for
    unrelated assertions. Locks in the public contract introduced by Task 5.
    """
    mgr = SinkManager(recorder=None, dlq_sink=None)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True  # every delivery raises
    mgr.register(sink)

    # Handler returns SKIP so the failing sink takes the SKIP exit path
    # without retrying. The test does NOT care about the handler's return
    # value — only that the None guards along the delivery path survive.
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # The critical assertion: this call must not raise AttributeError or
    # similar from a bare ``None.record_*(...)`` or ``None.send(...)`` call.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    # Sanity: the handler WAS consulted (proving we reached the error path),
    # and stats reflect the single failure.
    on_error.assert_awaited_once()
    stats = mgr.get_all_stats()[('kafka', 'out')]
    assert stats.error_count == 1


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
    it should be ~max(delays) = 200ms. Upper bound 0.25s rules out
    partial-parallelism (2-of-3 serial lands around ~300ms). Lower bound 0.19s
    rules out a degenerate implementation that returns before the slowest sink
    finishes its sleep. Both bounds fail loudly if the implementation regressed
    or was never actually parallelized.
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

    assert 0.19 <= elapsed < 0.25, f'expected parallel deliver in [0.19s, 0.25s), got {elapsed:.3f}s'
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


# --- Circuit breaker (Phase 2 Task 7) ---
#
# The per-sink circuit breaker trips after `failure_threshold` consecutive
# terminal failures (retries exhausted). While open, deliveries bypass the
# sink entirely and route to DLQ. Cooldown elapses → next delivery is a
# half-open probe → success closes, failure reopens with a renewed cooldown.


def _make_breaker_manager(failure_threshold: int = 3, cooldown_seconds: float = 30.0) -> SinkManager:
    """Build a SinkManager with a per-test circuit breaker config.

    Keeping the threshold small (3) means tests only need a handful of
    failures to exercise the trip transition.
    """
    from drakkar.config import CircuitBreakerConfig

    return SinkManager(
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
        )
    )


async def test_circuit_trips_after_threshold_consecutive_failures():
    """N consecutive terminal failures → circuit trips → next delivery is skipped."""
    from drakkar.metrics import sink_circuit_open

    mgr = _make_breaker_manager(failure_threshold=3, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    # DLQ action means each delivery becomes a terminal failure (no retries).
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Three back-to-back deliveries all fail: circuit closed → open on 3rd.
    for _ in range(3):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert sink.circuit_state == 'open'
    gauge_value = sink_circuit_open.labels(sink_type='kafka', sink_name='out')._value.get()
    assert gauge_value == 1.0

    # The 4th delivery must skip the sink — sink.deliver is not called because
    # the breaker bypasses it. We detect this by checking the sink never
    # appended anything and the on_error handler got a "circuit open" error.
    skipped_errors: list[DeliveryError] = []

    async def capture(error: DeliveryError) -> DeliveryAction:
        skipped_errors.append(error)
        return DeliveryAction.DLQ

    await mgr.deliver_all(result, on_delivery_error=capture, partition_id=0)

    assert len(skipped_errors) == 1
    assert skipped_errors[0].error == CIRCUIT_OPEN_ERROR
    assert skipped_errors[0].sink_name == 'out'
    # Sink was never touched for the skipped delivery — fail_on_deliver
    # would have raised if it had been called. The only source of errors
    # is the breaker gate reporting via on_delivery_error.


async def test_circuit_skipped_delivery_routes_to_dlq():
    """When the breaker is open, payloads reach the DLQ handler with error='circuit open'."""
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    # Trip the circuit first.
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData(value=1))])
    for _ in range(2):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    assert sink.circuit_state == 'open'

    # Circuit is open — next delivery routes to DLQ directly.
    dlq_payloads: list[list] = []

    async def dlq_handler(error: DeliveryError) -> DeliveryAction:
        dlq_payloads.append(error.payloads)
        return DeliveryAction.DLQ

    skip_result = CollectResult(kafka=[KafkaPayload(data=SampleData(value=99))])
    await mgr.deliver_all(skip_result, on_delivery_error=dlq_handler, partition_id=0)

    # DLQ received the payloads without the sink being touched.
    assert len(dlq_payloads) == 1
    assert len(dlq_payloads[0]) == 1


async def test_circuit_intermittent_failures_do_not_trip():
    """fail, succeed, fail, succeed — consecutive counter resets on each success."""
    mgr = _make_breaker_manager(failure_threshold=3, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    call_count = 0
    original_deliver = sink.deliver

    async def flaky(payloads: list) -> None:
        nonlocal call_count
        call_count += 1
        # Alternate failure / success — never three in a row.
        if call_count % 2 == 1:
            raise RuntimeError('transient')
        await original_deliver(payloads)

    sink.deliver = flaky  # type: ignore[assignment]
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)  # each failure terminal
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # 10 deliveries, 5 fail + 5 succeed, alternating. Each success resets
    # the consecutive counter so the threshold (3) is never reached.
    for _ in range(10):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert sink.circuit_state == 'closed'


async def test_circuit_cooldown_elapses_half_open_success_closes():
    """After cooldown, the next delivery is a half-open probe; success closes the circuit.

    Cooldown 0.2s + sleep 0.3s leaves a 100ms margin above the cooldown to
    tolerate CI scheduler variance without flaking. The margin was 50ms
    before (0.05s cooldown + 0.1s sleep) which is too tight under load.
    """
    from drakkar.metrics import sink_circuit_open

    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=0.2)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Trip the circuit.
    for _ in range(2):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    assert sink.circuit_state == 'open'

    # Wait past the cooldown, then stop failing so the probe succeeds.
    await asyncio.sleep(0.3)
    sink.fail_on_deliver = False

    # Next delivery is the half-open probe — it runs the real deliver path
    # and succeeds, closing the circuit.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert sink.circuit_state == 'closed'
    gauge_value = sink_circuit_open.labels(sink_type='kafka', sink_name='out')._value.get()
    assert gauge_value == 0.0
    assert len(sink.delivered) == 1  # the successful probe


async def test_circuit_half_open_failure_reopens_with_renewed_cooldown():
    """Half-open probe failure → circuit snaps back to open + new cooldown."""
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=0.2)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Trip the circuit.
    for _ in range(2):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    assert sink.circuit_state == 'open'
    first_opened_at = sink._circuit_opened_at
    assert first_opened_at is not None

    # Wait past cooldown — the next delivery becomes a probe, but the sink
    # is still failing so the probe fails too. The circuit should reopen.
    await asyncio.sleep(0.3)
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert sink.circuit_state == 'open'
    # The open timestamp should have been renewed — second reopening is
    # strictly later than the first.
    second_opened_at = sink._circuit_opened_at
    assert second_opened_at is not None
    assert second_opened_at > first_opened_at


async def test_circuit_trips_counter_increments_on_each_trip():
    """sink_circuit_trips_total ticks on every transition into open (initial + reopens)."""
    from drakkar.metrics import sink_circuit_trips

    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=0.2)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    before = sink_circuit_trips.labels(sink_type='kafka', sink_name='out')._value.get()

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Trip #1 — closed → open.
    for _ in range(2):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    after_first = sink_circuit_trips.labels(sink_type='kafka', sink_name='out')._value.get()
    assert after_first == before + 1

    # Wait past cooldown (0.2s), fail the half-open probe → trip #2
    # (open → half_open → open). 100ms margin above cooldown to avoid
    # CI scheduler flake.
    await asyncio.sleep(0.3)
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    after_second = sink_circuit_trips.labels(sink_type='kafka', sink_name='out')._value.get()
    assert after_second == before + 2


async def test_circuit_preserves_stats_on_skip():
    """Skipped (circuit-open) deliveries update error_count and last_error = 'circuit open'."""
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Trip.
    for _ in range(2):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    # Snapshot stats pre-skip.
    stats_before = mgr.get_all_stats()[('kafka', 'out')]
    error_before = stats_before.error_count

    # Run one skipped delivery — error_count should tick up by one, and the
    # sentinel last_error preserves semantics for the debug UI.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    stats_after = mgr.get_all_stats()[('kafka', 'out')]
    assert stats_after.error_count == error_before + 1
    assert stats_after.last_error == CIRCUIT_OPEN_ERROR
    assert stats_after.last_error_ts is not None
    # No delivery completed — delivered_count did not change.
    assert stats_after.delivered_count == stats_before.delivered_count


async def test_circuit_retries_with_in_run_failures_do_not_double_count():
    """A single failing delivery with N retries counts as ONE consecutive failure.

    With max_retries=3 and RETRY action, a failing sink makes up to 3 attempts
    — but the breaker only sees the terminal outcome, not each retry. That
    means failure_threshold=3 should not trip after a single batch; it needs
    three independent batches with retries exhausted.
    """
    mgr = _make_breaker_manager(failure_threshold=3, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    on_error = AsyncMock(return_value=DeliveryAction.RETRY)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # One batch — retries exhausted → breaker sees ONE failure.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, max_retries=3)
    assert sink.circuit_state == 'closed'
    assert sink._consecutive_failures == 1

    # Two more batches — now at 3, should trip.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, max_retries=3)
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0, max_retries=3)
    assert sink.circuit_state == 'open'


async def test_circuit_skip_action_does_not_trip_breaker():
    """When the handler returns ``DeliveryAction.SKIP`` (drop + continue), the
    failure is intentional-drop, not a downstream-health signal. The breaker
    must NOT tick its consecutive-failure counter on a SKIP — otherwise an
    operator who drops a malformed batch would unintentionally help trip the
    sink offline.

    Terminal here is SKIP, not retries-exhausted, so the existing RETRY-path
    test (``test_circuit_retries_with_in_run_failures_do_not_double_count``)
    doesn't cover this shape.
    """
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    # Every delivery fails, but the operator SKIPs — the breaker should
    # stay closed indefinitely (the failures aren't a health signal).
    on_error = AsyncMock(return_value=DeliveryAction.SKIP)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    for _ in range(5):
        await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    # Threshold is 2 — 5 SKIPs would easily exceed it if SKIPs counted.
    assert sink.circuit_state == 'closed'
    assert sink._consecutive_failures == 0


async def test_circuit_success_resets_consecutive_failure_counter():
    """After partial failures, a single success must snap the consecutive
    counter back to zero so the breaker only trips on GENUINELY-CONSECUTIVE
    failures, not an accumulated total.

    Complements ``test_circuit_intermittent_failures_do_not_trip`` — that
    test walks an alternating pattern; this one asserts the reset is
    explicit and survives retries-exhausted terminals.
    """
    mgr = _make_breaker_manager(failure_threshold=3, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    # DLQ terminal so each batch lands as one consecutive failure.
    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Two failures — counter climbs to 2, threshold not yet hit.
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    assert sink._consecutive_failures == 2
    assert sink.circuit_state == 'closed'

    # Flip to success — the counter must reset to zero.
    sink.fail_on_deliver = False
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)
    assert sink._consecutive_failures == 0
    assert sink.circuit_state == 'closed'


async def test_circuit_concurrent_probes_respect_single_flight():
    """Two concurrent deliveries against a half-open breaker must NOT both
    issue the probe — only the first claim wins, the others skip.

    Regression for HIGH-2: ``deliver_all`` uses ``asyncio.gather`` across
    sinks/groups, so under a recovering downstream two batches could both
    observe ``half_open`` in the same event-loop tick and both let the
    probe through, slamming the downstream with concurrent probes and
    defeating the "single probe" intent.

    Strategy: force the circuit into half_open manually (synthetic state
    transition is cheaper than waiting through cooldown), then fire two
    ``deliver_all`` calls concurrently. Exactly one should reach the sink.
    """
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    # Place the breaker into half_open with NO probe in flight, as would
    # happen naturally when cooldown elapses on the first ``should_skip_delivery``
    # call of a new batch. _probe_inflight starts False so the first
    # caller will claim it. These private attributes are touched directly to
    # inject synthetic state — the public API offers read-only views.
    sink._circuit_state = 'half_open'
    sink._circuit_opened_at = 0.0
    sink._probe_inflight = False

    # Make the sink's deliver slow so the two concurrent callers overlap
    # in the gather — otherwise the first finishes before the second even
    # reaches ``should_skip_delivery``.
    delivered_calls: list[list] = []

    async def slow_deliver(payloads: list) -> None:
        await asyncio.sleep(0.1)
        delivered_calls.append(payloads)

    sink.deliver = slow_deliver  # type: ignore[assignment]

    on_error = AsyncMock(return_value=DeliveryAction.DLQ)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Two concurrent deliver_all invocations — both target the same sink.
    await asyncio.gather(
        mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0),
        mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0),
    )

    # Exactly ONE delivery reached the sink. The other caller saw
    # half_open + probe_inflight=True and skipped, routing its batch to
    # the handler (on_error invoked with circuit-open error).
    assert len(delivered_calls) == 1, f'exactly one probe should have reached the sink, got {len(delivered_calls)}'
    assert on_error.await_count == 1, (
        f'the skipped caller should have invoked on_delivery_error exactly once, got {on_error.await_count}'
    )


async def test_circuit_half_open_skip_clears_probe_inflight():
    """A half-open probe that fails and is then SKIPped must release the
    probe slot — otherwise the circuit is wedged forever.

    Regression for the review finding: the SKIP branch exits without
    calling ``record_success`` or ``record_failure``, so before the fix
    it left ``probe_inflight=True`` indefinitely. Subsequent
    ``should_skip_delivery`` calls saw ``half_open + inflight=True`` and
    returned True every time, permanently routing every batch to DLQ even
    though the sink might be healthy.

    Scenario walked here:
      1. Sink is in half_open with the probe slot free.
      2. Delivery #1 claims the probe slot, ``sink.deliver`` raises,
         handler returns SKIP. No ``record_*`` is called, so without
         the try/finally guard the probe flag would leak.
      3. Delivery #2 must NOT see a leaked probe flag — it must be
         allowed to probe (and in this test succeeds, closing the
         circuit).
    """
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    mgr.register(sink)

    # Force half_open state with probe slot free — the natural promotion
    # path via cooldown-elapsed is covered by other tests; here we want
    # to exercise the SKIP-path leak regardless of how we got to half_open.
    # Private attributes touched directly for synthetic state injection; the
    # public API offers read-only views (``circuit_state`` / ``probe_inflight``).
    sink._circuit_state = 'half_open'
    sink._circuit_opened_at = 0.0
    sink._probe_inflight = False

    on_error = AsyncMock(return_value=DeliveryAction.SKIP)
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])

    # Delivery #1: probe fails, handler returns SKIP. Before the fix,
    # this left ``probe_inflight=True`` forever because neither
    # ``record_success`` nor ``record_failure`` was called.
    sink.fail_on_deliver = True
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    # Invariant: after a SKIPped probe the inflight flag must be clear,
    # so the next delivery can legitimately probe the sink again.
    assert not sink.probe_inflight, 'SKIP on a half-open probe must release the probe slot — it was left True'
    # Circuit should still be half_open (we didn't record success or
    # failure, just a neutral release). A subsequent successful delivery
    # will close it via ``record_success``.
    assert sink.circuit_state == 'half_open'

    # Delivery #2: this call MUST be allowed to probe. If the leak
    # regression returns, ``should_skip_delivery`` would skip this one
    # too, ``sink.deliver`` would never run, and ``delivered`` would be
    # empty — the assertion below would fail.
    sink.fail_on_deliver = False
    await mgr.deliver_all(result, on_delivery_error=on_error, partition_id=0)

    assert len(sink.delivered) == 1, (
        'after a SKIPped half-open probe, the NEXT delivery must be allowed to probe again — '
        f'probe flag leak would skip it instead, got delivered={len(sink.delivered)}'
    )
    # Successful probe closes the circuit.
    assert sink.circuit_state == 'closed'


async def test_circuit_half_open_handler_raises_clears_probe_inflight():
    """If ``on_delivery_error`` itself raises, the probe slot must still
    be released — the handler exception must not leak circuit-breaker
    state.

    Same shape as the SKIP-leak regression, but instead of SKIP the
    handler propagates an exception. Without the try/finally guard in
    ``_deliver_to_sink`` the probe flag would leak identically, wedging
    the circuit forever.
    """
    mgr = _make_breaker_manager(failure_threshold=2, cooldown_seconds=30.0)
    sink = FakeSink('out', sink_type='kafka')
    sink.fail_on_deliver = True
    mgr.register(sink)

    # Synthetic half_open state, probe slot free.
    sink._circuit_state = 'half_open'
    sink._circuit_opened_at = 0.0
    sink._probe_inflight = False

    # Handler raises — simulates a buggy user handler that crashes.
    async def raising_handler(err: DeliveryError) -> DeliveryAction:
        raise RuntimeError('handler crashed')

    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    with pytest.raises(RuntimeError, match='handler crashed'):
        await mgr.deliver_all(result, on_delivery_error=raising_handler, partition_id=0)

    # Even though the handler raised, the probe flag must have been
    # released by the try/finally guard. Without it, every future
    # delivery would see ``half_open + inflight=True`` and skip.
    assert not sink.probe_inflight, 'a raising on_delivery_error on a half-open probe must release the probe slot'
