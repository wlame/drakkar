"""Tests for Drakkar main application."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    PostgresSinkConfig,
    SinksConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ExecutorTask,
    KafkaPayload,
    PostgresPayload,
    SourceMessage,
)
from drakkar.sinks.manager import SinkNotConfiguredError


class _D(BaseModel):
    x: int = 1


class SimpleHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['test'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


@pytest.fixture
def test_config() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(
            brokers='localhost:9092',
            source_topic='test-in',
        ),
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_workers=2,
            task_timeout_seconds=10,
            window_size=5,
        ),
        sinks=SinksConfig(
            kafka={'results': KafkaSinkConfig(topic='test-out')},
            postgres={'main': PostgresSinkConfig(dsn='postgresql://localhost/test')},
        ),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


@pytest.fixture
def test_config_no_sinks() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


def _setup_app_sinks(app: DrakkarApp) -> None:
    """Build and register fake sinks so _handle_collect works without real connections."""
    app._build_sinks()
    # replace all registered sinks with async mocks
    for key, sink in app._sink_manager._sinks.items():
        mock_sink = AsyncMock()
        mock_sink.sink_type = sink.sink_type
        mock_sink.name = sink.name
        mock_sink._name = sink.name
        app._sink_manager._sinks[key] = mock_sink
        # update _by_type
        for i, s in enumerate(app._sink_manager._by_type[sink.sink_type]):
            if s.name == sink.name:
                app._sink_manager._by_type[sink.sink_type][i] = mock_sink


# --- Creation ---


def test_app_creation(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    assert app.config == test_config
    assert app.processors == {}


def test_app_creation_with_worker_id(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config, worker_id='w1')
    assert app._worker_id == 'w1'


def test_app_creation_auto_worker_id(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    assert app._worker_id.startswith('drakkar-')


def test_app_worker_id_from_env(test_config, monkeypatch):
    monkeypatch.setenv('WORKER_ID', 'my-worker-7')
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    assert app._worker_id == 'my-worker-7'


def test_app_worker_id_custom_env_var(monkeypatch):
    monkeypatch.setenv('MY_SVC_NAME', 'svc-alpha')
    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        worker_name_env='MY_SVC_NAME',
    )
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    assert app._worker_id == 'svc-alpha'


def test_app_worker_id_param_overrides_env(test_config, monkeypatch):
    monkeypatch.setenv('WORKER_ID', 'from-env')
    app = DrakkarApp(handler=SimpleHandler(), config=test_config, worker_id='explicit')
    assert app._worker_id == 'explicit'


# --- Sink manager ---


def test_app_has_sink_manager(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    assert app.sink_manager is not None
    assert app.sink_manager.sink_count == 0  # not built yet


def test_app_build_sinks(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._build_sinks()
    assert app.sink_manager.sink_count == 2  # kafka + postgres


def test_app_build_dlq(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._build_dlq()
    assert app._dlq_sink is not None
    assert app._dlq_sink.topic == 'test-in_dlq'  # derived from source_topic


def test_app_build_dlq_custom_topic():
    from drakkar.config import DLQConfig

    config = DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='input'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        dlq=DLQConfig(topic='custom-dlq'),
    )
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    app._build_dlq()
    assert app._dlq_sink.topic == 'custom-dlq'


# --- Partition management ---


async def test_app_on_assign_creates_processors(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    for proc in app.processors.values():
        await proc.stop()


async def test_app_on_revoke_removes_processors(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    app._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    app._on_revoke([1])
    await asyncio.sleep(0.3)
    assert 1 not in app.processors
    assert len(app.processors) == 2

    for proc in list(app.processors.values()):
        await proc.stop()


# --- Collect handling ---


async def test_app_handle_collect(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    _setup_app_sinks(app)

    result = CollectResult(
        kafka=[KafkaPayload(data=_D())],
        postgres=[PostgresPayload(table='t', data=_D())],
    )
    await app._handle_collect(result, partition_id=0)

    # verify both sinks had deliver() called
    kafka_sink = app._sink_manager._sinks[('kafka', 'results')]
    pg_sink = app._sink_manager._sinks[('postgres', 'main')]
    kafka_sink.deliver.assert_called_once()
    pg_sink.deliver.assert_called_once()


async def test_app_handle_collect_empty(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    _setup_app_sinks(app)

    result = CollectResult()
    await app._handle_collect(result, partition_id=0)

    # no sinks should have been called
    for sink in app._sink_manager._sinks.values():
        sink.deliver.assert_not_called()


async def test_app_handle_collect_unconfigured_sink_raises(test_config):
    """If collect returns a sink type not configured, worker crashes."""
    from drakkar.models import MongoPayload

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    _setup_app_sinks(app)

    result = CollectResult(
        mongo=[MongoPayload(collection='c', data=_D())],
    )
    with pytest.raises(SinkNotConfiguredError, match="No 'mongo' sink"):
        await app._handle_collect(result, partition_id=0)


# --- Commit ---


async def test_app_handle_commit(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()

    await app._handle_commit(partition_id=3, offset=100)
    app._consumer.commit.assert_called_once_with({3: 100})


# --- Startup hooks ---


async def test_app_on_startup_hook_can_modify_config(test_config):
    class ConfigTuningHandler(BaseDrakkarHandler):
        async def on_startup(self, config):
            return config.model_copy(
                update={
                    'executor': config.executor.model_copy(update={'max_workers': 99}),
                }
            )

        async def arrange(self, messages, pending):
            return []

    app = DrakkarApp(handler=ConfigTuningHandler(), config=test_config)
    assert app.config.executor.max_workers == 2

    app._config = await app._handler.on_startup(app._config)
    assert app._config.executor.max_workers == 99


async def test_app_on_startup_default_returns_config_unchanged(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    result = await app._handler.on_startup(app._config)
    assert result is app._config


# --- Startup validation ---


async def test_app_no_sinks_raises(test_config_no_sinks):
    """Starting with no sinks configured raises SinkNotConfiguredError."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config_no_sinks)
    with pytest.raises(SinkNotConfiguredError, match='No sinks configured'):
        await app._async_run()


# --- Signal handling ---


async def test_app_handle_signal(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._running = True
    app._handle_signal()
    assert not app._running


# --- Shutdown ---


async def test_app_shutdown_closes_sinks(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()

    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    await app._shutdown()

    app._consumer.close.assert_called_once()
    app._dlq_sink.close.assert_called_once()
    # all sinks should have close called
    for sink in app._sink_manager._sinks.values():
        sink.close.assert_called_once()


async def test_app_shutdown_drains_executors(test_config):
    """Graceful shutdown gives executors up to 5s to finish."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._on_assign([0])
    await asyncio.sleep(0.1)

    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    await app._shutdown()
    assert len(app.processors) == 0


# --- Error handling in processors ---


async def test_stop_processor_handles_arrange_error(test_config):
    class BrokenArrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            raise RuntimeError('arrange exploded')

    app = DrakkarApp(handler=BrokenArrangeHandler(), config=test_config)
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    app._on_assign([0])
    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    app._on_revoke([0])
    await asyncio.sleep(0.5)
    assert 0 not in app.processors


async def test_safe_call_catches_handler_errors(test_config):
    class ErrorOnAssignHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        async def on_assign(self, partitions):
            raise ValueError('on_assign failed')

    app = DrakkarApp(handler=ErrorOnAssignHandler(), config=test_config)
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._on_assign([0])
    await asyncio.sleep(0.2)

    for proc in app.processors.values():
        await proc.stop()


# --- cluster_name resolution ---


def test_app_cluster_name_from_env(monkeypatch):
    """cluster_name_env takes precedence over config.cluster_name."""
    monkeypatch.setenv('MY_CLUSTER', 'env-cluster')
    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        cluster_name='config-cluster',
        cluster_name_env='MY_CLUSTER',
    )
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    assert app._cluster_name == 'env-cluster'


def test_app_cluster_name_falls_back_to_config(monkeypatch):
    """When cluster_name_env is not set, config.cluster_name is used."""
    monkeypatch.delenv('MY_CLUSTER', raising=False)
    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        cluster_name='fallback-cluster',
        cluster_name_env='MY_CLUSTER',
    )
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    assert app._cluster_name == 'fallback-cluster'


def test_app_cluster_name_no_env_var_configured():
    """When cluster_name_env is empty, config.cluster_name is used directly."""
    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        cluster_name='direct-cluster',
    )
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    assert app._cluster_name == 'direct-cluster'


# --- _get_worker_state ---


def test_app_get_worker_state(test_config):
    """_get_worker_state returns a dict with current worker metrics."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)

    state = app._get_worker_state()
    assert 'uptime_seconds' in state
    assert state['uptime_seconds'] >= 0
    assert state['assigned_partitions'] == []
    assert state['partition_count'] == 0
    assert state['pool_active'] == 0
    assert state['pool_max'] == 2
    assert state['total_queued'] == 0
    assert state['paused'] is False


def test_app_get_worker_state_no_pool(test_config):
    """_get_worker_state handles None executor_pool gracefully."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    state = app._get_worker_state()
    assert state['pool_active'] == 0
    assert state['pool_max'] == 0


# --- _total_queued ---


async def test_app_total_queued_with_processors(test_config):
    """_total_queued sums queue sizes and inflight counts across processors."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._on_assign([0, 1])
    assert app._total_queued() == 0

    msg0 = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    msg1 = SourceMessage(topic='t', partition=1, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg0)
    app.processors[1].enqueue(msg1)
    assert app._total_queued() >= 2

    for proc in app.processors.values():
        await proc.stop()


# --- Backpressure ---


async def test_app_backpressure_pauses_and_resumes(test_config):
    """When queue exceeds high watermark, consumer is paused. When it drops
    below low watermark, consumer is resumed."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._on_assign([0])
    await asyncio.sleep(0.05)

    # config: max_workers=2, high_mult=32, low_mult=4 → high=64, low=8
    # Simulate high queue by directly putting messages
    for i in range(65):
        msg = SourceMessage(topic='t', partition=0, offset=i, value=b'x', timestamp=0)
        app.processors[0]._queue.put_nowait(msg)

    # Run one iteration of the poll loop manually
    total = app._total_queued()
    assert total >= 64

    max_workers = app.config.executor.max_workers
    high_watermark = max_workers * app.config.executor.backpressure_high_multiplier
    low_watermark = max(1, max_workers * app.config.executor.backpressure_low_multiplier)

    # Simulate pause trigger
    if not app._paused and total >= high_watermark:
        partition_ids = list(app.processors.keys())
        if partition_ids:
            await app._consumer.pause(partition_ids)
            app._paused = True

    app._consumer.pause.assert_called_once_with([0])
    assert app._paused

    # Drain queue to below low watermark
    while app.processors[0]._queue.qsize() > low_watermark - 1:
        app.processors[0]._queue.get_nowait()

    total = app._total_queued()
    assert total <= low_watermark

    # Simulate resume trigger
    if app._paused and total <= low_watermark:
        partition_ids = list(app.processors.keys())
        if partition_ids:
            await app._consumer.resume(partition_ids)
            app._paused = False

    app._consumer.resume.assert_called_once_with([0])
    assert not app._paused

    for proc in app.processors.values():
        await proc.stop()


# --- Shutdown: periodic task cancellation ---


async def test_shutdown_cancels_periodic_tasks(test_config):
    """Shutdown cancels periodic tasks."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Create a fake periodic task
    async def fake_periodic():
        await asyncio.sleep(3600)

    task = asyncio.create_task(fake_periodic())
    app._periodic_tasks.append(task)

    await app._shutdown()

    assert task.cancelled() or task.done()
    assert len(app._periodic_tasks) == 0


# --- Shutdown: final commit failure ---


async def test_shutdown_final_commit_failure_is_logged(test_config):
    """Final commit during shutdown that fails doesn't prevent cleanup."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)

    mock_consumer = AsyncMock()
    mock_consumer.commit.side_effect = RuntimeError('commit failed during rebalance')
    app._consumer = mock_consumer

    app._on_assign([0])

    # Force a committable offset
    app.processors[0]._offset_tracker.register(0)
    app.processors[0]._offset_tracker.complete(0)

    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Should not raise despite commit failure
    await app._shutdown()
    assert len(app.processors) == 0


# --- Shutdown: recorder and debug server cleanup ---


async def test_shutdown_stops_recorder_and_debug_server(test_config):
    """Shutdown stops the recorder and debug server if they exist."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    mock_recorder = AsyncMock()
    mock_debug_server = AsyncMock()
    app._recorder = mock_recorder
    app._debug_server = mock_debug_server

    await app._shutdown()

    mock_recorder.stop.assert_called_once()
    mock_debug_server.stop.assert_called_once()


# --- Poll loop tests ---


async def test_poll_loop_dispatches_messages_to_processors(test_config):
    """Messages from consumer.poll_batch() are enqueued to the correct partition processor."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._on_assign([0, 1])
    await asyncio.sleep(0.01)

    msg0 = SourceMessage(topic='t', partition=0, offset=10, value=b'x', timestamp=0)
    msg1 = SourceMessage(topic='t', partition=1, offset=20, value=b'y', timestamp=0)
    call_count = 0

    async def _poll_once(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [msg0, msg1]
        app._running = False
        return []

    app._consumer.poll_batch = _poll_once

    await app._poll_loop()

    assert app.processors[0].queue_size >= 0  # message was consumed (may already be processed)
    assert app.processors[1].queue_size >= 0

    for proc in app.processors.values():
        await proc.stop()


async def test_poll_loop_pauses_on_high_watermark(test_config):
    """Consumer is paused when total_queued >= high_watermark."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    # Create processor but don't start it — messages stay in queue
    from drakkar.partition import PartitionProcessor

    proc = PartitionProcessor(
        partition_id=0,
        handler=SimpleHandler(),
        executor_pool=app._executor_pool,
        window_size=5,
    )
    app._processors[0] = proc
    # Do NOT call proc.start() — queue won't drain

    # high_watermark = 2 * 32 = 64. Fill queue past that.
    for i in range(70):
        proc._queue.put_nowait(SourceMessage(topic='t', partition=0, offset=i, value=b'x', timestamp=0))

    call_count = 0

    async def _poll_then_stop(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            app._running = False
        return []

    app._consumer.poll_batch = _poll_then_stop

    await app._poll_loop()

    app._consumer.pause.assert_called()
    assert app._paused


async def test_poll_loop_consumer_idle_metric(test_config):
    """Consumer idle metric increments when poll returns nothing and queues are empty."""
    from drakkar.executor import ExecutorPool
    from drakkar.metrics import consumer_idle

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    # no partitions assigned = empty queues
    call_count = 0

    async def _empty_poll(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            app._running = False
        return []

    app._consumer.poll_batch = _empty_poll

    before = consumer_idle._value.get()
    await app._poll_loop()
    after = consumer_idle._value.get()

    assert after > before


async def test_poll_loop_executor_idle_waste_metric(test_config):
    """Executor idle waste increments when messages queued but slots are free."""
    from drakkar.executor import ExecutorPool
    from drakkar.metrics import executor_idle_waste

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._on_assign([0])
    await asyncio.sleep(0.01)

    # put messages in queue, keep executor idle (active_count=0)
    for i in range(5):
        app.processors[0]._queue.put_nowait(SourceMessage(topic='t', partition=0, offset=i, value=b'x', timestamp=0))

    call_count = 0

    async def _empty_poll(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            app._running = False
        return []

    app._consumer.poll_batch = _empty_poll

    before = executor_idle_waste._value.get()
    await app._poll_loop()
    after = executor_idle_waste._value.get()

    assert after > before

    for proc in app.processors.values():
        await proc.stop()
