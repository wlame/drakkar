"""Tests for the P1 reliability batch (2026-06-09 review follow-up).

Covers:
- zombie suppression: tasks finishing after a revoke/shutdown drain timeout
  must not deliver to sinks or commit offsets
- _stop_processor / _shutdown wiring of the suppression flag
- _drain_all_processors draining from an explicit snapshot
- cache flush serialization (lock) and consecutive-failure escalation
- KafkaSink collecting outstanding produce futures when flush fails
- circuit breaker observing on_delivery_error raising
- consumer lag queries counting errors instead of failing silently
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel as BM

from drakkar.app import DrakkarApp
from drakkar.cache.engine import FLUSH_FAILURE_ESCALATION_THRESHOLD, CacheEngine
from drakkar.cache.memory import Cache
from drakkar.config import (
    CacheConfig,
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import (
    cache_flush_failures,
    consumer_errors,
    suppressed_zombie_deliveries,
)
from drakkar.models import (
    CollectResult,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.sinks.kafka import KafkaSink
from tests.conftest import make_ui_config, wait_for
from tests.sink_mocks import setup_app_sinks as _setup_app_sinks
from tests.test_app import SimpleHandler


class _Out(BM):
    v: str = ''


def make_msg(offset: int = 0) -> SourceMessage:
    return SourceMessage(topic='test', partition=0, offset=offset, value=b'{"x": 1}', timestamp=1000)


class _EmitHandler(BaseDrakkarHandler):
    """Returns one task per message; emits a payload per task."""

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f't-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

    async def on_task_complete(self, result):
        return CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])


@pytest.fixture
def echo_pool() -> ExecutorPool:
    return ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)


@pytest.fixture
def app_config() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10, window_size=5),
        sinks=SinksConfig(kafka={'results': KafkaSinkConfig(topic='test-out')}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


# --- zombie suppression -----------------------------------------------------


async def test_suppressed_processor_neither_delivers_nor_commits(echo_pool):
    collected: list[CollectResult] = []
    committed: list[tuple[int, int]] = []
    before = suppressed_zombie_deliveries.labels(partition='0')._value.get()

    async def on_collect(result, partition_id):
        collected.append(result)

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=_EmitHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=on_collect,
        on_commit=on_commit,
    )
    proc.suppress_deliveries()
    proc.enqueue(make_msg(offset=10))
    proc.start()
    # The task completes and the tracker settles, but nothing leaves the
    # processor. wait on internal settlement, not on observable effects.
    await wait_for(lambda: proc.inflight_count == 0 and proc.queue_size == 0, timeout=5)
    await asyncio.sleep(0.1)
    await proc.stop()

    assert collected == [], 'suppressed processor must not deliver to sinks'
    assert committed == [], 'suppressed processor must not commit offsets'
    assert suppressed_zombie_deliveries.labels(partition='0')._value.get() > before


async def test_stop_processor_drain_timeout_sets_suppression(app_config):
    app_config.executor.drain_timeout_seconds = 0.05
    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    app._consumer = AsyncMock()

    proc = PartitionProcessor(
        partition_id=0,
        handler=SimpleHandler(),
        executor_pool=ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10),
        window_size=5,
    )
    # A pending offset that never completes wedges drain until the timeout.
    proc._offset_tracker.register(42)

    await app._lifecycle._stop_processor(proc)

    assert proc._deliveries_suppressed, 'drain timeout must mark in-flight tasks as zombies'
    app._consumer.commit.assert_not_called()


# --- drain snapshot ----------------------------------------------------------


class _FakeOffsetTracker:
    def has_pending(self) -> bool:
        return True


class _FakeProcessor:
    """Just enough surface for _drain_all_processors."""

    queue_size = 0
    inflight_count = 0

    def __init__(self):
        self.offset_tracker = _FakeOffsetTracker()
        self.drained = False

    async def drain(self) -> None:
        self.drained = True


async def test_drain_all_processors_uses_snapshot_not_live_dict(app_config):
    """A processor popped from app._processors (concurrent rebalance) must
    still be drained when it is part of the shutdown snapshot."""
    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    ghost = _FakeProcessor()
    assert ghost.offset_tracker.has_pending()
    # NOT registered in app._processors — simulates the revoke-pop race.
    assert app._processors == {}

    await app._lifecycle._drain_all_processors([ghost])

    assert ghost.drained, 'snapshot processors must be drained even after being popped'


# --- cache flush lock + escalation -------------------------------------------


def _make_engine() -> CacheEngine:
    engine = CacheEngine(
        config=CacheConfig(enabled=True),
        ui_config=make_ui_config(enabled=False, db_dir=''),
        worker_id='w-test',
        cluster_name='',
        recorder=None,
    )
    engine.attach_cache(Cache(origin_worker_id='w-test'))
    return engine


async def test_flush_once_serializes_concurrent_calls():
    engine = _make_engine()
    concurrency = {'current': 0, 'max': 0}

    async def slow_flush():
        concurrency['current'] += 1
        concurrency['max'] = max(concurrency['max'], concurrency['current'])
        await asyncio.sleep(0.05)
        concurrency['current'] -= 1

    engine._flush_locked = slow_flush

    await asyncio.gather(engine._flush_once(), engine._flush_once(), engine._flush_once())

    assert concurrency['max'] == 1, 'flush cycles must never overlap'


async def test_flush_failure_streak_escalates_and_resets():
    engine = _make_engine()
    before = cache_flush_failures._value.get()

    async def broken_flush():
        raise RuntimeError('disk on fire')

    engine._flush_locked = broken_flush
    for _ in range(FLUSH_FAILURE_ESCALATION_THRESHOLD):
        with pytest.raises(RuntimeError):
            await engine._flush_once()

    assert engine._consecutive_flush_failures == FLUSH_FAILURE_ESCALATION_THRESHOLD
    assert cache_flush_failures._value.get() == before + FLUSH_FAILURE_ESCALATION_THRESHOLD

    async def ok_flush():
        return None

    engine._flush_locked = ok_flush
    await engine._flush_once()
    assert engine._consecutive_flush_failures == 0, 'a successful flush resets the streak'


# --- KafkaSink outstanding-future collection ----------------------------------


def _done_future(result) -> asyncio.Future:
    fut = asyncio.get_running_loop().create_future()
    fut.set_result(result)
    return fut


async def test_kafka_sink_ignores_producer_wide_remainder_when_its_own_futures_acked():
    """One producer serves every partition loop, so ``flush()``'s remainder
    counts other partitions' messages too. Judging a batch by it re-produces
    groups the broker already acknowledged — duplicates on the output topic —
    or ships delivered payloads to the DLQ for a replay that duplicates them
    again.
    """
    sink = KafkaSink(name='k', config=KafkaSinkConfig(topic='out'), brokers_fallback='localhost:9092')
    ok_report = MagicMock()
    ok_report.error.return_value = None

    producer = AsyncMock()
    producer.produce.side_effect = lambda **kw: _done_future(ok_report)
    producer.flush.return_value = 2  # another partition's messages, not ours
    sink._producer = producer

    payloads = [KafkaPayload(data=_Out(v='a')), KafkaPayload(data=_Out(v='b'))]
    await sink.deliver(payloads)  # must not raise


async def test_kafka_sink_reports_a_broker_error_carried_in_its_own_report():
    """A failed delivery arrives inside ``Message.error()`` rather than being
    raised, so a resolved future is not on its own a confirmation. This path
    is unchanged by the producer-wide-remainder fix and must stay a failure.
    """
    sink = KafkaSink(name='k', config=KafkaSinkConfig(topic='out'), brokers_fallback='localhost:9092')
    err_report = MagicMock()
    err_report.error.return_value = 'BROKER_DOWN'

    producer = AsyncMock()
    producer.produce.side_effect = lambda **kw: _done_future(err_report)
    producer.flush.return_value = 0  # the producer drained cleanly
    sink._producer = producer

    with pytest.raises(RuntimeError, match='BROKER_DOWN'):
        await sink.deliver([KafkaPayload(data=_Out(v='a'))])


async def test_kafka_sink_timeout_survives_an_unresolved_future_without_cancelling_it():
    """The abandoned-future collection must not cancel the delivery futures:
    the producer thread sets their result when the report finally arrives, and
    setting a result on a cancelled future raises inside that thread.
    """
    cfg = KafkaSinkConfig(topic='out', flush_timeout_seconds=0.05)
    sink = KafkaSink(name='k', config=cfg, brokers_fallback='localhost:9092')

    stuck: list[asyncio.Future] = []

    def _never_resolves(**kw):
        fut = asyncio.get_running_loop().create_future()
        stuck.append(fut)
        return fut

    producer = AsyncMock()
    producer.produce.side_effect = _never_resolves
    producer.flush.return_value = 0
    sink._producer = producer

    with pytest.raises(TimeoutError, match='not acknowledged'):
        await sink.deliver([KafkaPayload(data=_Out(v='a'))])

    assert not stuck[0].cancelled(), 'an abandoned delivery future must stay settable'
    stuck[0].set_result(MagicMock())  # the late report must not raise


async def test_kafka_sink_collects_futures_when_flush_raises():
    sink = KafkaSink(name='k', config=KafkaSinkConfig(topic='out'), brokers_fallback='localhost:9092')
    err_report = MagicMock()
    err_report.error.return_value = 'BROKER_DOWN'

    producer = AsyncMock()
    producer.produce.side_effect = lambda **kw: _done_future(err_report)
    producer.flush.side_effect = RuntimeError('flush blew up')
    sink._producer = producer

    with pytest.raises(RuntimeError, match='flush blew up'):
        await sink.deliver([KafkaPayload(data=_Out(v='a'))])


# --- circuit breaker sees on_delivery_error raising ----------------------------


async def test_on_delivery_error_raise_records_breaker_failure(app_config):
    class RaisingHandler(SimpleHandler):
        async def on_delivery_error(self, error):
            raise RuntimeError('handler bug')

    app = DrakkarApp(handler=RaisingHandler(), config=app_config)
    _setup_app_sinks(app)
    kafka_sink = app._sink_manager._sinks[('kafka', 'results')]
    kafka_sink.deliver.side_effect = RuntimeError('broker down')

    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    with pytest.raises(RuntimeError, match='handler bug'):
        await app._handle_collect(result, partition_id=0)

    kafka_sink.record_failure.assert_called_once()


# --- consumer lag query error accounting ---------------------------------------


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(brokers='localhost:9092', source_topic='test-source', consumer_group='g')


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag_counts_error_on_committed_failure(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_inner.committed.side_effect = RuntimeError('broker election')
    mock_cls.return_value = mock_inner

    before = consumer_errors._value.get()
    consumer = KafkaConsumer(kafka_config)
    assert await consumer.get_total_lag([0, 1]) == 0
    assert consumer_errors._value.get() == before + 1


@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag_counts_errors_per_failed_watermark(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_inner.committed.return_value = []
    mock_inner.get_watermark_offsets.side_effect = RuntimeError('broker election')
    mock_cls.return_value = mock_inner

    before = consumer_errors._value.get()
    consumer = KafkaConsumer(kafka_config)
    result = await consumer.get_partition_lag([0, 1])
    assert result[0]['lag'] == 0
    assert result[1]['lag'] == 0
    assert consumer_errors._value.get() == before + 2
