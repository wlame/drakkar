"""Tests for Drakkar main application."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    PostgresConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    DBRow,
    ExecutorTask,
    OutputMessage,
    SourceMessage,
)


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
            target_topic='test-out',
        ),
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_workers=2,
            task_timeout_seconds=10,
            window_size=5,
        ),
        postgres=PostgresConfig(dsn='postgresql://localhost/test'),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


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
    from drakkar.config import DrakkarConfig, ExecutorConfig

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


@patch('drakkar.app.KafkaConsumer')
@patch('drakkar.app.KafkaProducer')
@patch('drakkar.app.DBWriter')
@patch('drakkar.app.start_metrics_server')
async def test_app_on_assign_creates_processors(
    mock_metrics, mock_db_cls, mock_producer_cls, mock_consumer_cls, test_config
):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()
    app._producer = MagicMock()
    app._db_writer = AsyncMock()

    app._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    for proc in app.processors.values():
        await proc.stop()


@patch('drakkar.app.KafkaConsumer')
@patch('drakkar.app.KafkaProducer')
@patch('drakkar.app.DBWriter')
@patch('drakkar.app.start_metrics_server')
async def test_app_on_revoke_removes_processors(
    mock_metrics, mock_db_cls, mock_producer_cls, mock_consumer_cls, test_config
):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )
    app._consumer = AsyncMock()
    app._producer = MagicMock()
    app._db_writer = AsyncMock()

    app._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    app._on_revoke([1])
    await asyncio.sleep(0.3)  # allow drain+stop coroutine
    assert 1 not in app.processors
    assert len(app.processors) == 2

    for proc in list(app.processors.values()):
        await proc.stop()


async def test_app_handle_collect(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._producer = AsyncMock()
    app._db_writer = AsyncMock()

    result = CollectResult(
        output_messages=[OutputMessage(value=b'test')],
        db_rows=[DBRow(table='t', data={'x': 1})],
    )
    await app._handle_collect(result, partition_id=0)

    app._producer.produce_batch.assert_called_once()
    app._db_writer.write.assert_called_once()


async def test_app_handle_collect_empty(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._producer = AsyncMock()
    app._db_writer = AsyncMock()

    result = CollectResult()
    await app._handle_collect(result, partition_id=0)

    app._producer.produce_batch.assert_not_called()
    app._db_writer.write.assert_not_called()


async def test_app_handle_commit(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._consumer = AsyncMock()

    await app._handle_commit(partition_id=3, offset=100)
    app._consumer.commit.assert_called_once_with({3: 100})


async def test_app_on_startup_hook_can_modify_config(test_config):
    class ConfigTuningHandler(BaseDrakkarHandler):
        async def on_startup(self, config):
            return config.model_copy(
                update={
                    'executor': config.executor.model_copy(
                        update={
                            'max_workers': 99,
                        }
                    ),
                }
            )

        async def arrange(self, messages, pending):
            return []

    app = DrakkarApp(handler=ConfigTuningHandler(), config=test_config)
    assert app.config.executor.max_workers == 2  # original

    # simulate the on_startup call from _async_run
    app._config = await app._handler.on_startup(app._config)
    assert app._config.executor.max_workers == 99


async def test_app_on_startup_default_returns_config_unchanged(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)

    result = await app._handler.on_startup(app._config)
    assert result is app._config


async def test_app_handle_signal(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._running = True
    app._handle_signal()
    assert not app._running


async def test_app_shutdown_closes_all_components(test_config):
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._producer = AsyncMock()
    app._consumer = AsyncMock()
    app._db_writer = AsyncMock()

    await app._shutdown()

    app._producer.close.assert_called_once()
    app._consumer.close.assert_called_once()
    app._db_writer.close.assert_called_once()


async def test_app_shutdown_drains_executors(test_config):
    """Graceful shutdown gives executors up to 5s to finish."""
    handler = SimpleHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    app._consumer = AsyncMock()
    app._producer = AsyncMock()
    app._db_writer = AsyncMock()

    # create a processor with an executor pool
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )
    app._on_assign([0])
    await asyncio.sleep(0.1)

    # enqueue a message so the processor has work
    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    await app._shutdown()

    # after shutdown, processors should be cleared
    assert len(app.processors) == 0


async def test_stop_processor_handles_arrange_error(test_config):
    """If arrange() raises during stop, _stop_processor catches it
    instead of leaving 'Task exception was never retrieved'."""

    class BrokenArrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            raise RuntimeError('arrange exploded')

    app = DrakkarApp(handler=BrokenArrangeHandler(), config=test_config)
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )
    app._consumer = AsyncMock()
    app._producer = MagicMock()
    app._db_writer = AsyncMock()

    app._on_assign([0])
    # enqueue a message that will trigger the broken arrange
    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    # revoke should not raise or produce "Task exception was never retrieved"
    app._on_revoke([0])
    await asyncio.sleep(0.5)

    assert 0 not in app.processors


async def test_safe_call_catches_handler_errors(test_config):
    """_safe_call wraps async callbacks so exceptions don't go unretrieved."""

    class ErrorOnAssignHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        async def on_assign(self, partitions):
            raise ValueError('on_assign failed')

    app = DrakkarApp(handler=ErrorOnAssignHandler(), config=test_config)
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo', max_workers=2, task_timeout_seconds=10
    )
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    # should not raise — _safe_call catches it
    app._on_assign([0])
    await asyncio.sleep(0.2)

    for proc in app.processors.values():
        await proc.stop()
