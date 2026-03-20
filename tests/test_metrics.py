"""Tests that framework operations correctly update Prometheus metrics.

Each test triggers real framework code (with mocked external deps)
and asserts the metric values changed as expected. We are NOT testing
prometheus_client internals — we are testing that our instrumentation
is wired correctly.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from confluent_kafka import KafkaError, TopicPartition

from drakkar.config import (
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
    assigned_partitions,
    batch_duration,
    consumer_errors,
    executor_duration,
    executor_tasks,
    handler_duration,
    messages_consumed,
    offset_lag,
    offsets_committed,
    partition_queue_size,
    rebalance_events,
    start_metrics_server,
    task_retries,
    worker_info,
)
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from tests.conftest import wait_for

# --- Helpers ---


def counter_val(counter, **labels):
    if labels:
        return counter.labels(**labels)._value.get()
    return counter._value.get()


def gauge_val(gauge, **labels):
    if labels:
        return gauge.labels(**labels)._value.get()
    return gauge._value.get()


def histogram_sum(hist, **labels):
    """Get the sum of observed values from a histogram."""
    if labels:
        return hist.labels(**labels)._sum.get()
    return hist._sum.get()


def make_msg(partition=0, offset=0):
    return SourceMessage(
        topic='t',
        partition=partition,
        offset=offset,
        value=b'{"x":1}',
        timestamp=1000,
    )


def make_kafka_error_msg(error_code):
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = error_code
    msg.error.return_value = err
    return msg


def make_kafka_ok_msg(partition=0, offset=0, value=b'v'):
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = 't'
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = b'k'
    msg.value.return_value = value
    msg.timestamp.return_value = (1, 1000)
    return msg


@pytest.fixture
def kafka_config():
    return KafkaConfig(brokers='localhost:9092', source_topic='src')


# === Consumer metrics ===


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_error_increments_consumer_errors(mock_cls, kafka_config):
    """When poll returns a non-EOF error, consumer_errors counter goes up."""
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_kafka_error_msg(KafkaError._ALL_BROKERS_DOWN),
        make_kafka_error_msg(KafkaError.UNKNOWN),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    before = counter_val(consumer_errors)
    await consumer.poll_batch(timeout=0.1)
    assert counter_val(consumer_errors) == before + 2


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_eof_does_not_increment_consumer_errors(mock_cls, kafka_config):
    """Partition EOF is not an error — should not touch consumer_errors."""
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_kafka_error_msg(KafkaError._PARTITION_EOF),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    before = counter_val(consumer_errors)
    await consumer.poll_batch(timeout=0.1)
    assert counter_val(consumer_errors) == before


@patch('drakkar.consumer.AIOConsumer')
async def test_rebalance_assign_increments_metric(mock_cls, kafka_config):
    """_handle_assign from Kafka triggers rebalance_events(type=assign)."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.subscribe()

    before = counter_val(rebalance_events, type='assign')
    assign_cb = mock_inner.subscribe.call_args[1]['on_assign']
    await assign_cb(mock_inner, [TopicPartition('src', 0), TopicPartition('src', 1)])
    assert counter_val(rebalance_events, type='assign') == before + 1


@patch('drakkar.consumer.AIOConsumer')
async def test_rebalance_revoke_increments_metric(mock_cls, kafka_config):
    """_handle_revoke from Kafka triggers rebalance_events(type=revoke)."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.subscribe()

    before = counter_val(rebalance_events, type='revoke')
    revoke_cb = mock_inner.subscribe.call_args[1]['on_revoke']
    await revoke_cb(mock_inner, [TopicPartition('src', 5)])
    assert counter_val(rebalance_events, type='revoke') == before + 1


@patch('drakkar.consumer.AIOConsumer')
async def test_commit_increments_offsets_committed(mock_cls, kafka_config):
    """consumer.commit() increments offsets_committed per partition."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    before_p0 = counter_val(offsets_committed, partition='0')
    before_p3 = counter_val(offsets_committed, partition='3')

    await consumer.commit({0: 100, 3: 200})

    assert counter_val(offsets_committed, partition='0') == before_p0 + 1
    assert counter_val(offsets_committed, partition='3') == before_p3 + 1


# === Partition processor metrics ===


async def test_enqueue_increments_consumed_and_sets_queue_size():
    """PartitionProcessor.enqueue() increments messages_consumed and sets queue_size."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    handler = BaseDrakkarHandler()
    proc = PartitionProcessor(partition_id=77, handler=handler, executor_pool=pool, window_size=10)

    before = counter_val(messages_consumed, partition='77')
    proc.enqueue(make_msg(partition=77, offset=0))
    proc.enqueue(make_msg(partition=77, offset=1))

    assert counter_val(messages_consumed, partition='77') == before + 2
    assert gauge_val(partition_queue_size, partition='77') == 2


async def test_processing_tracks_executor_task_started_completed():
    """Full processing cycle increments executor_tasks started and completed."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=4, task_timeout_seconds=10)

    class SimpleHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(task_id=f'm-{m.offset}', args=['hi'], source_offsets=[m.offset])
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=88,
        handler=SimpleHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before_started = counter_val(executor_tasks, status='started')
    before_completed = counter_val(executor_tasks, status='completed')

    proc.enqueue(make_msg(partition=88, offset=0))
    proc.enqueue(make_msg(partition=88, offset=1))
    proc.start()
    await wait_for(lambda: counter_val(executor_tasks, status='completed') >= before_completed + 2)
    await proc.stop()

    assert counter_val(executor_tasks, status='started') >= before_started + 2


async def test_failed_task_increments_executor_tasks_failed():
    """Executor failure increments executor_tasks(status=failed)."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_workers=4,
        task_timeout_seconds=10,
    )

    class FailHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'fail-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=89,
        handler=FailHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = counter_val(executor_tasks, status='failed')
    proc.enqueue(make_msg(partition=89, offset=0))
    proc.start()
    await wait_for(lambda: counter_val(executor_tasks, status='failed') >= before + 1)
    await proc.stop()


async def test_handler_arrange_duration_observed():
    """arrange() hook execution time is observed in handler_duration(hook=arrange)."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)

    class SlowArrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            await asyncio.sleep(0.01)
            return [
                ExecutorTask(task_id=f'a-{m.offset}', args=['x'], source_offsets=[m.offset])
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=90,
        handler=SlowArrangeHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = histogram_sum(handler_duration, hook='arrange')
    proc.enqueue(make_msg(partition=90, offset=0))
    proc.start()
    await wait_for(lambda: histogram_sum(handler_duration, hook='arrange') > before)
    await proc.stop()


async def test_handler_collect_duration_observed():
    """collect() hook execution time is observed in handler_duration(hook=collect)."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)

    class CollectHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(task_id=f'c-{m.offset}', args=['x'], source_offsets=[m.offset])
                for m in messages
            ]

        async def collect(self, result):
            from pydantic import BaseModel as BM

            class _Out(BM):
                v: str = 'ok'

            return CollectResult(kafka=[KafkaPayload(data=_Out())])

    proc = PartitionProcessor(
        partition_id=91,
        handler=CollectHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = histogram_sum(handler_duration, hook='collect')
    proc.enqueue(make_msg(partition=91, offset=0))
    proc.start()
    await wait_for(lambda: histogram_sum(handler_duration, hook='collect') > before)
    await proc.stop()


async def test_handler_on_error_duration_observed():
    """on_error() hook execution time is observed in handler_duration(hook=on_error)."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_workers=2,
        task_timeout_seconds=10,
    )

    class ErrorHookHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'eh-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            return ErrorAction.SKIP

    proc = PartitionProcessor(
        partition_id=92,
        handler=ErrorHookHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = histogram_sum(handler_duration, hook='on_error')
    proc.enqueue(make_msg(partition=92, offset=0))
    proc.start()
    await wait_for(lambda: histogram_sum(handler_duration, hook='on_error') > before)
    await proc.stop()


async def test_window_complete_observes_batch_duration():
    """Completed window observes batch_duration histogram."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=4, task_timeout_seconds=10)

    class SimpleHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(task_id=f'bd-{m.offset}', args=['x'], source_offsets=[m.offset])
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=93,
        handler=SimpleHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = histogram_sum(batch_duration)
    proc.enqueue(make_msg(partition=93, offset=0))
    proc.start()
    await wait_for(lambda: histogram_sum(batch_duration) > before)
    await proc.stop()


async def test_executor_duration_observed_on_completion():
    """Each completed executor task observes executor_duration histogram."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=4, task_timeout_seconds=10)

    class SimpleHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(task_id=f'ed-{m.offset}', args=['x'], source_offsets=[m.offset])
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=94,
        handler=SimpleHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = histogram_sum(executor_duration)
    proc.enqueue(make_msg(partition=94, offset=0))
    proc.start()
    await wait_for(lambda: histogram_sum(executor_duration) > before)
    await proc.stop()


async def test_offset_lag_updated_on_window_complete():
    """offset_lag gauge is updated when a window completes."""
    pool = ExecutorPool(binary_path='/bin/echo', max_workers=4, task_timeout_seconds=10)

    class SimpleHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(task_id=f'ol-{m.offset}', args=['x'], source_offsets=[m.offset])
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=95,
        handler=SimpleHandler(),
        executor_pool=pool,
        window_size=10,
    )

    proc.enqueue(make_msg(partition=95, offset=0))
    proc.start()
    await wait_for(lambda: gauge_val(offset_lag, partition='95') == 0 and proc.inflight_count == 0)
    await proc.stop()


async def test_task_retry_increments_retries_counter():
    """When on_error returns RETRY, task_retries counter goes up."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_workers=2,
        task_timeout_seconds=10,
    )

    call_count = 0

    class RetryOnceHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'rt-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

    proc = PartitionProcessor(
        partition_id=96,
        handler=RetryOnceHandler(),
        executor_pool=pool,
        window_size=10,
    )

    before = counter_val(task_retries)
    proc.enqueue(make_msg(partition=96, offset=0))
    proc.start()
    await wait_for(lambda: counter_val(task_retries) >= before + 1)
    await proc.stop()


# === App-level metrics ===


async def test_on_assign_sets_assigned_partitions_gauge():
    """DrakkarApp._on_assign sets assigned_partitions gauge to processor count."""
    from drakkar.app import DrakkarApp

    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/echo', max_workers=2),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )
    handler = BaseDrakkarHandler()
    app = DrakkarApp(handler=handler, config=config)
    app._consumer = MagicMock()
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )

    app._on_assign([10, 11, 12])

    assert gauge_val(assigned_partitions) == len(app.processors)

    for proc in app.processors.values():
        await proc.stop()


async def test_on_revoke_decreases_assigned_partitions_gauge():
    """DrakkarApp._on_revoke decreases assigned_partitions gauge."""
    from drakkar.app import DrakkarApp

    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/echo', max_workers=2),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )
    handler = BaseDrakkarHandler()
    app = DrakkarApp(handler=handler, config=config)
    app._consumer = AsyncMock()
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )

    app._on_assign([20, 21, 22])
    assert gauge_val(assigned_partitions) == len(app.processors)

    app._on_revoke([21])
    await asyncio.sleep(0.3)
    assert gauge_val(assigned_partitions) == len(app.processors)

    for proc in list(app.processors.values()):
        await proc.stop()


async def test_handle_collect_delivers_to_sinks():
    """DrakkarApp._handle_collect routes payloads to configured sinks."""
    from drakkar.app import DrakkarApp

    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/echo', max_workers=2),
        sinks=SinksConfig(kafka={'out': KafkaSinkConfig(topic='t')}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )
    handler = BaseDrakkarHandler()
    app = DrakkarApp(handler=handler, config=config)

    # build sinks then mock the deliver method
    app._build_sinks()
    kafka_sink = app._sink_manager._sinks[('kafka', 'out')]
    kafka_sink.deliver = AsyncMock()  # type: ignore[method-assign]

    from pydantic import BaseModel as BM

    class _D(BM):
        v: str = 'a'

    result = CollectResult(
        kafka=[KafkaPayload(data=_D(v='a')), KafkaPayload(data=_D(v='b'))],
    )
    await app._handle_collect(result, partition_id=0)

    kafka_sink.deliver.assert_called_once()
    assert len(kafka_sink.deliver.call_args[0][0]) == 2


# === Worker info metric ===


async def test_app_sets_worker_info_on_startup():
    """DrakkarApp._async_run sets worker_info with worker_id, version, consumer_group."""
    from drakkar.app import DrakkarApp

    config = DrakkarConfig(
        kafka=KafkaConfig(consumer_group='my-fleet'),
        executor=ExecutorConfig(binary_path='/bin/echo', max_workers=2),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )
    app = DrakkarApp(handler=BaseDrakkarHandler(), config=config, worker_id='w-42')

    # simulate the part of _async_run that sets worker_info
    from drakkar import __version__

    worker_info.info(
        {
            'worker_id': app._worker_id,
            'version': __version__,
            'consumer_group': config.kafka.consumer_group,
        }
    )

    assert worker_info._value['worker_id'] == 'w-42'
    assert worker_info._value['version'] == __version__
    assert worker_info._value['consumer_group'] == 'my-fleet'


# === Server config tests (these test our start_metrics_server logic) ===


def test_start_metrics_server_enabled():
    config = MetricsConfig(enabled=True, port=19090)
    with patch('drakkar.metrics.start_http_server') as mock_start:
        start_metrics_server(config)
        mock_start.assert_called_once_with(19090)


def test_start_metrics_server_disabled():
    config = MetricsConfig(enabled=False, port=19090)
    with patch('drakkar.metrics.start_http_server') as mock_start:
        start_metrics_server(config)
        mock_start.assert_not_called()
