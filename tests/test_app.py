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
