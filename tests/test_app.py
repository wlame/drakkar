"""Tests for Drakkar main application."""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp, warn_if_ui_unauthenticated
from drakkar.app_security import SIDE_EFFECTING_ENDPOINTS
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    PostgresSinkConfig,
    SinksConfig,
    UIConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ExecutorTask,
    KafkaPayload,
    PostgresPayload,
    SourceMessage,
)
from drakkar.sinks.manager import CIRCUIT_OPEN_ERROR, SinkNotConfiguredError
from tests.conftest import make_ui_config, wait_for


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


class WebCapableHandler(BaseDrakkarHandler[_D, _D, _D, _D]):
    """A handler with all four Generic slots filled in.

    ``DrakkarApp.__init__`` fails fast on ``webapp.enabled=True`` unless the
    handler declares concrete HTTP request/response models and overrides
    both HTTP hooks (see ``validate_webapp_handler``). ``SimpleHandler``
    above deliberately leaves those slots at their defaults, so tests that
    need a webapp-enabled app to actually construct use this handler instead.
    """

    async def arrange(self, messages, pending):
        return []

    async def arrange_http_request(self, req, pending):
        return []

    async def on_http_request_complete(self, group):
        return _D()


@pytest.fixture
def test_config() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(
            brokers='localhost:9092',
            source_topic='test-in',
        ),
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_executors=2,
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
        # Circuit breaker hooks are sync, called by SinkManager per delivery.
        # AsyncMock would return truthy coroutines for should_skip_delivery
        # (treated as "skip me"), so override with plain MagicMocks that
        # return sensible defaults for non-circuit-breaker tests.
        mock_sink.should_skip_delivery = MagicMock(return_value=False)
        mock_sink.record_success = MagicMock()
        mock_sink.record_failure = MagicMock()
        # ``circuit_state`` / ``probe_inflight`` are read-only properties on
        # the real BaseSink; on the AsyncMock they'd be auto-created coroutines
        # which make the SinkManager's ``probe_claimed`` check truthy. Pin to
        # plain values so non-circuit-breaker tests behave like a closed circuit.
        mock_sink.circuit_state = 'closed'
        mock_sink.probe_inflight = False
        # ``mark_connected`` / ``mark_disconnected`` are sync helpers called
        # by SinkManager around connect/close so the readiness probe signal
        # flips cleanly. AsyncMock would return unawaited coroutines and
        # surface warnings under shutdown — pin them to plain MagicMocks.
        mock_sink.mark_connected = MagicMock()
        mock_sink.mark_disconnected = MagicMock()
        mock_sink.is_connected = False
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


def test_app_is_ready_starts_false(test_config):
    """is_ready is False until the first poll cycle completes.

    The readiness signal drives the /readyz Kubernetes probe — it MUST
    report "not ready" immediately after construction, before the worker
    has even started, otherwise a pod would receive traffic during its
    cold-start window.
    """
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    assert app.is_ready is False


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

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._lifecycle._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    for proc in app.processors.values():
        await proc.stop()


async def test_app_on_revoke_removes_processors(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)

    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    app._lifecycle._on_assign([0, 1, 2])
    assert len(app.processors) == 3

    await app._lifecycle._on_revoke([1])
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


async def test_app_routes_circuit_open_to_dlq_sink(test_config):
    """HIGH-3 integration: when a sink's circuit breaker is open, the app-level
    ``_handle_collect`` must route skipped payloads straight to the DLQ sink
    rather than consult ``on_delivery_error``. The breaker already decided the
    sink is unhealthy — SKIP would silently drop data, RETRY would hammer the
    recovering downstream, and neither belongs in the handler's control loop.

    This is the end-to-end counterpart of
    ``tests/test_sink_manager.py::test_circuit_skipped_delivery_routes_to_dlq``
    — it verifies the ``dlq_sink`` argument is actually threaded through
    ``DrakkarApp._handle_collect`` into ``SinkManager.deliver_all`` and that
    the DLQ sink's ``send`` is invoked without handler approval.
    """
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    _setup_app_sinks(app)

    # Wire a fake DLQ sink so the force-DLQ branch has a target.
    # SinkManager reads ``self._dlq_sink`` from its own instance state, so
    # use ``attach_runtime`` — the same one-time wiring ``_async_run`` does
    # in production. Recorder stays None for this test.
    dlq_sink = AsyncMock()
    app._dlq_sink = dlq_sink
    app._sink_manager.attach_runtime(recorder=None, dlq_sink=dlq_sink)

    # Force one of the kafka sinks into open-circuit state. `_setup_app_sinks`
    # replaced the real sink with an AsyncMock, so we override the circuit
    # breaker hook to return True (meaning "skip delivery") — this simulates
    # the breaker being open without having to walk the state machine.
    kafka_sink = app._sink_manager._sinks[('kafka', 'results')]
    kafka_sink.should_skip_delivery = MagicMock(return_value=True)

    # Also open the circuit on the postgres sink so we don't branch into
    # the "only one sink tripped" case. Simpler to have both skip.
    pg_sink = app._sink_manager._sinks[('postgres', 'main')]
    pg_sink.should_skip_delivery = MagicMock(return_value=True)

    result = CollectResult(
        kafka=[KafkaPayload(data=_D())],
        postgres=[PostgresPayload(table='t', data=_D())],
    )

    # Deliver — both sinks skip, both force-DLQ.
    await app._handle_collect(result, partition_id=0)

    # Neither sink was touched.
    kafka_sink.deliver.assert_not_called()
    pg_sink.deliver.assert_not_called()

    # DLQ received BOTH skipped batches (exactly one call per sink group).
    # HIGH-3 intent: the breaker's open state triggers direct DLQ routing
    # from SinkManager — the app's on_delivery_error handler is NOT invoked,
    # so there's no second call fan-out through the handler path.
    assert dlq_sink.send.await_count == 2
    sent_errors = [call.args[0] for call in dlq_sink.send.await_args_list]
    assert {e.sink_type for e in sent_errors} == {'kafka', 'postgres'}
    # Every error must carry the "circuit open" sentinel so dashboards can
    # filter DLQ entries routed by the breaker vs by terminal delivery failure.
    for err in sent_errors:
        assert err.error == CIRCUIT_OPEN_ERROR


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
                    'executor': config.executor.model_copy(update={'max_executors': 99}),
                }
            )

        async def arrange(self, messages, pending):
            return []

    app = DrakkarApp(handler=ConfigTuningHandler(), config=test_config)
    assert app.config.executor.max_executors == 2

    app._config = await app._handler.on_startup(app._config)
    assert app._config.executor.max_executors == 99


async def test_app_on_startup_default_returns_config_unchanged(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    result = await app._handler.on_startup(app._config)
    assert result is app._config


# --- Startup validation ---


async def test_app_no_sinks_raises(test_config_no_sinks):
    """Starting with no sinks configured raises SinkNotConfiguredError."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config_no_sinks)
    with pytest.raises(SinkNotConfiguredError, match='No sinks configured'):
        await app._lifecycle._async_run()


# --- Signal handling ---


async def test_app_handle_signal(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._running = True
    app._lifecycle._handle_signal()
    assert not app._running


# --- Shutdown ---


async def test_app_shutdown_closes_sinks(test_config):
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()

    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    await app._lifecycle._shutdown()

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

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._lifecycle._on_assign([0])
    await asyncio.sleep(0.1)

    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    await app._lifecycle._shutdown()
    assert len(app.processors) == 0


# --- Error handling in processors ---


async def test_stop_processor_handles_arrange_error(test_config):
    class BrokenArrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            raise RuntimeError('arrange exploded')

    app = DrakkarApp(handler=BrokenArrangeHandler(), config=test_config)
    from drakkar.executor import ExecutorPool

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    app._lifecycle._on_assign([0])
    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    app.processors[0].enqueue(msg)
    await asyncio.sleep(0.3)

    await app._lifecycle._on_revoke([0])
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

    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._lifecycle._on_assign([0])
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
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

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
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = MagicMock()
    app._consumer.commit = AsyncMock()

    app._lifecycle._on_assign([0, 1])
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
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._lifecycle._on_assign([0])
    await asyncio.sleep(0.05)

    # config: max_executors=2, high_mult=32, low_mult=4 → high=64, low=8
    # Simulate high queue by directly putting messages
    for i in range(65):
        msg = SourceMessage(topic='t', partition=0, offset=i, value=b'x', timestamp=0)
        app.processors[0]._queue.put_nowait(msg)

    # Run one iteration of the poll loop manually
    total = app._total_queued()
    assert total >= 64

    max_executors = app.config.executor.max_executors
    high_watermark = max_executors * app.config.executor.backpressure_high_multiplier
    low_watermark = max(1, max_executors * app.config.executor.backpressure_low_multiplier)

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


async def test_newly_assigned_partition_is_paused_when_backpressure_active(test_config):
    """H5: when _on_assign fires while the consumer is already paused for
    backpressure, the newly-added partition must be paused immediately.

    Without this, the new partition bypasses the backpressure gate until
    the next _poll_loop tick, during which the consumer can deliver
    unbounded messages from it (causing memory spikes and queue overflow).
    """
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    # Simulate the poll loop having already entered the paused state.
    app._paused = True
    app._lifecycle._on_assign([0])
    app._lifecycle._on_assign([5, 7])

    # Let the background pause task run.
    await asyncio.sleep(0.05)

    # The second assign should have triggered a pause on the new partitions.
    assert any(call.args[0] == [5, 7] for call in app._consumer.pause.call_args_list), (
        f'expected pause([5,7]), got calls: {app._consumer.pause.call_args_list}'
    )

    for proc in list(app.processors.values()):
        await proc.stop()


async def test_newly_assigned_partition_not_paused_when_unpaused(test_config):
    """When backpressure is NOT active, _on_assign must not spuriously pause
    the new partition — otherwise the consumer would never deliver.
    """
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    assert not app._paused
    app._lifecycle._on_assign([0, 1])
    await asyncio.sleep(0.05)

    # Pause must NOT have been invoked for these assignments.
    assert app._consumer.pause.call_count == 0

    for proc in list(app.processors.values()):
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

    await app._lifecycle._shutdown()

    assert task.cancelled() or task.done()
    assert len(app._periodic_tasks) == 0


# --- Shutdown: final commit failure ---


async def test_shutdown_final_commit_failure_is_logged(test_config):
    """Final commit during shutdown that fails doesn't prevent cleanup."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

    mock_consumer = AsyncMock()
    mock_consumer.commit.side_effect = RuntimeError('commit failed during rebalance')
    app._consumer = mock_consumer

    app._lifecycle._on_assign([0])

    # Force a committable offset
    app.processors[0]._offset_tracker.register(0)
    app.processors[0]._offset_tracker.complete(0)

    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Should not raise despite commit failure
    await app._lifecycle._shutdown()
    assert len(app.processors) == 0


# --- Bug #3: shutdown must await background tasks before closing consumer ---


async def test_shutdown_awaits_background_tasks_before_closing_consumer(test_config):
    """_stop_processor scheduled by _on_revoke runs in the background and
    uses self._consumer. Shutdown must wait for those tasks to finish
    before closing the consumer, or commits race with close().
    """
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

    order: list[str] = []
    mock_consumer = AsyncMock()

    async def slow_commit(*args, **kwargs):
        # simulate real commit work taking time
        await asyncio.sleep(0.2)
        order.append('commit')

    async def record_close():
        order.append('close')

    mock_consumer.commit = slow_commit
    mock_consumer.close = record_close
    app._consumer = mock_consumer

    # Simulate a background task like _stop_processor would be:
    # it awaits commit and must complete before consumer.close().
    async def fake_background_commit():
        await mock_consumer.commit({0: 5})

    app._background_tasks.add(asyncio.create_task(fake_background_commit()))

    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    await app._lifecycle._shutdown()

    # commit MUST appear before close — proving background task completed first
    assert order == ['commit', 'close'], f'ordering violated: {order}'


# --- H2: commit on drain timeout must be skipped to avoid data loss ---


async def test_stop_processor_skips_commit_on_drain_timeout(test_config):
    """When _stop_processor's drain times out, it must NOT commit offsets.

    Tasks may still be in flight; committing their watermark would tell
    Kafka they are done and on partition reassign the replay would skip
    them, silently losing data.
    """
    from drakkar.executor import ExecutorPool
    from drakkar.partition import PartitionProcessor

    # tiny drain timeout to force the TimeoutError path
    test_config.executor.drain_timeout_seconds = 0.05

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()

    # Build a processor with a stuck drain: non-empty queue, _running stays False
    # but the offset tracker holds a pending offset so drain() never exits.
    proc = PartitionProcessor(
        partition_id=0,
        handler=SimpleHandler(),
        executor_pool=app._executor_pool,
        window_size=5,
    )
    # register an offset and never complete it — drain() will spin until timeout
    proc._offset_tracker.register(42)

    await app._lifecycle._stop_processor(proc)

    # After a drain timeout, commit MUST NOT have been called for this partition.
    # (Consumer may have been called for other reasons earlier — but not commit.)
    assert app._consumer.commit.call_count == 0, (
        f'commit should have been skipped after drain timeout, got: {app._consumer.commit.call_args_list}'
    )


async def test_shutdown_skips_commit_on_drain_timeout(test_config):
    """Same invariant as _stop_processor but for the main _shutdown path."""
    from drakkar.executor import ExecutorPool

    test_config.executor.drain_timeout_seconds = 0.05

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0])
    # register an offset without completing — drain will hang until timeout
    app.processors[0]._offset_tracker.register(99)

    await app._lifecycle._shutdown()

    # Commit must not have been called for the hung partition.
    assert app._consumer.commit.call_count == 0, (
        f'shutdown committed past in-flight work on timeout: {app._consumer.commit.call_args_list}'
    )


# --- Shutdown: recorder and debug server cleanup ---


async def test_shutdown_stops_recorder_and_ui_server(test_config):
    """Shutdown stops the recorder and debug server if they exist."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    mock_recorder = AsyncMock()
    mock_ui_server = AsyncMock()
    app._recorder = mock_recorder
    app._ui_server = mock_ui_server

    await app._lifecycle._shutdown()

    mock_recorder.stop.assert_called_once()
    mock_ui_server.stop.assert_called_once()


# --- Poll loop tests ---


async def test_poll_loop_dispatches_messages_to_processors(test_config):
    """Messages from consumer.poll_batch() are enqueued to the correct partition processor."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._lifecycle._on_assign([0, 1])
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

    await app._lifecycle._poll_loop()

    assert app.processors[0].queue_size >= 0  # message was consumed (may already be processed)
    assert app.processors[1].queue_size >= 0

    for proc in app.processors.values():
        await proc.stop()


async def test_poll_loop_pauses_on_high_watermark(test_config):
    """Consumer is paused when total_queued >= high_watermark."""
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
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

    await app._lifecycle._poll_loop()

    app._consumer.pause.assert_called()
    assert app._paused


async def test_poll_loop_consumer_idle_metric(test_config):
    """Consumer idle metric increments when poll returns nothing and queues are empty."""
    from drakkar.executor import ExecutorPool
    from drakkar.metrics import consumer_idle

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
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
    await app._lifecycle._poll_loop()
    after = consumer_idle._value.get()

    assert after > before


async def test_poll_loop_executor_idle_waste_metric(test_config):
    """Executor idle waste increments when messages queued but slots are free."""
    from drakkar.executor import ExecutorPool
    from drakkar.metrics import executor_idle_waste

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    app._running = True

    app._lifecycle._on_assign([0])
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
    await app._lifecycle._poll_loop()
    after = executor_idle_waste._value.get()

    assert after > before

    for proc in app.processors.values():
        await proc.stop()


# --- Task 7: Debug security startup gate ---


@pytest.fixture
def config_with_ui(test_config_no_sinks):
    """Factory fixture — returns a builder that swaps ``ui`` on the no-sinks baseline.

    The security gating tests only read ``config.ui``, so reusing the
    existing minimal config and overriding the ``ui`` field keeps the test
    setup a single line without duplicating the Kafka/executor/sinks scaffold.
    """

    def _build(ui_cfg: UIConfig) -> DrakkarConfig:
        return test_config_no_sinks.model_copy(update={'ui': ui_cfg})

    return _build


# --- Unauthenticated debug-UI startup warning ---
#
# Auth is opt-in: ``ui.auth_token`` defaults to empty, which disables auth
# on the entire debug UI. The framework emits a structured warning at startup
# whenever debug is enabled without a token, so the unauthenticated posture
# is visible in logs and operators have a clear path to enable auth.
#
# These tests pin down the helper's invariants:
#   - warns when ui.enabled AND auth_token == ''
#   - does NOT warn when auth_token is set (any non-empty value)
#   - does NOT warn when ui.enabled is False
#   - whitespace-only tokens are treated as empty (the field validator strips
#     them on load, so the helper sees ``''`` and warns)
#   - warning carries host/port + remediation pointers (YAML key + env var)


def _captured_unauth_events(records: list[dict]) -> list[dict]:
    """Filter captured structlog records down to the unauthenticated-UI events."""
    return [e for e in records if e.get('event') == 'ui_unauthenticated']


def test_warn_if_ui_unauthenticated_emits_warning(config_with_ui):
    """debug.enabled + empty auth_token emits a structured warning naming host:port."""
    config = config_with_ui(make_ui_config(enabled=True, host='0.0.0.0', auth_token=''))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)

    events = _captured_unauth_events(cap)
    assert len(events) == 1
    record = events[0]
    assert record['log_level'] == 'warning'
    assert record['host'] == '0.0.0.0'
    assert record['port'] == config.ui.port
    # The remediation message must name BOTH opt-in paths (YAML key + env
    # var) so operators don't have to guess which knob to turn.
    assert 'ui.auth_token' in record['message']
    assert 'DK_UI__AUTH_TOKEN' in record['message']
    # The rationale is part of the message — operators reading the warning
    # should immediately understand WHY this is opt-in. It must stay
    # truthful: the message used to claim the UI was read-only outright,
    # which two endpoints have never been.
    assert 'read-only' in record['message']
    assert 'but not all' in record['message']


def test_warn_if_ui_unauthenticated_names_the_side_effecting_endpoints(config_with_ui):
    """The warning must name what an unauthenticated caller can actually DO.

    The old text asserted the UI "cannot stop workers, replay messages, or
    modify state" and left it there — true as far as it went, but it let an
    operator conclude the exposure was read-only when probe executes
    caller-supplied bytes and merge writes files. Each live endpoint is
    named alongside the key that closes it.
    """
    config = config_with_ui(make_ui_config(enabled=True, auth_token=''))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)

    message = _captured_unauth_events(cap)[0]['message']
    for endpoint in SIDE_EFFECTING_ENDPOINTS:
        assert endpoint.route in message
        assert f'{endpoint.config_key}=false' in message


@pytest.mark.parametrize(
    ('probe_enabled', 'merge_enabled', 'expected_routes'),
    [
        (True, False, ['POST /api/debug/probe']),
        (False, True, ['POST /api/debug/merge']),
    ],
)
def test_warn_if_ui_unauthenticated_names_only_live_endpoints(
    config_with_ui, probe_enabled: bool, merge_enabled: bool, expected_routes: list[str]
):
    """An operator who already closed one endpoint isn't warned about it."""
    config = config_with_ui(
        make_ui_config(
            enabled=True,
            auth_token='',
            probe_enabled=probe_enabled,
            merge_enabled=merge_enabled,
        )
    )
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)

    message = _captured_unauth_events(cap)[0]['message']
    for endpoint in SIDE_EFFECTING_ENDPOINTS:
        if endpoint.route in expected_routes:
            assert endpoint.route in message
        else:
            assert endpoint.route not in message


def test_warn_if_ui_unauthenticated_reports_a_fully_read_only_ui(config_with_ui):
    """With both side-effecting endpoints closed, the read-only claim is true again."""
    config = config_with_ui(make_ui_config(enabled=True, auth_token='', probe_enabled=False, merge_enabled=False))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)

    message = _captured_unauth_events(cap)[0]['message']
    assert 'Every endpoint still served is read-only' in message
    assert 'but not all' not in message


def test_warn_if_ui_unauthenticated_silent_when_token_set(config_with_ui):
    """Setting auth_token (any non-empty value) silences the unauthenticated warning."""
    config = config_with_ui(make_ui_config(enabled=True, host='0.0.0.0', auth_token='secret-token'))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)
    assert _captured_unauth_events(cap) == []


def test_warn_if_ui_unauthenticated_warns_on_loopback_too(config_with_ui):
    """Warning fires regardless of host — even on 127.0.0.1 with empty token,
    so the unauthenticated posture is always visible in logs (no host-based
    silent path that masks the same default in dev vs prod)."""
    config = config_with_ui(make_ui_config(enabled=True, host='127.0.0.1', auth_token=''))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)
    assert len(_captured_unauth_events(cap)) == 1


def test_warn_if_ui_unauthenticated_silent_when_debug_disabled(config_with_ui):
    """debug.enabled=False means no debug server starts, so the warning is skipped."""
    config = config_with_ui(make_ui_config(enabled=False, host='0.0.0.0', auth_token=''))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)
    assert _captured_unauth_events(cap) == []


def test_warn_if_ui_unauthenticated_whitespace_token_treated_as_empty(config_with_ui):
    """A token of only spaces is stripped to empty by the field validator, so
    the helper sees ``''`` and warns. This prevents a 'spaces token' from
    silently looking secured to a careless operator."""
    config = config_with_ui(make_ui_config(enabled=True, host='0.0.0.0', auth_token='   '))
    # Pre-condition: the field validator already stripped the whitespace.
    assert config.ui.auth_token == ''
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)
    assert len(_captured_unauth_events(cap)) == 1


@pytest.mark.parametrize('host', ['127.0.0.1', 'localhost', '::1', '0.0.0.0', 'worker.internal'])
def test_warn_if_ui_unauthenticated_host_independent(config_with_ui, host: str):
    """Warning fires for every host — auth gating is no longer host-based."""
    config = config_with_ui(make_ui_config(enabled=True, host=host, auth_token=''))
    with capture_logs() as cap:
        warn_if_ui_unauthenticated(config)
    events = _captured_unauth_events(cap)
    assert len(events) == 1
    assert events[0]['host'] == host


# --- Rebalance mid-window: revoke while tasks are in flight ---
#
# These tests pin down the behaviour of the partition-revocation path when
# the revoked partition still has work in flight (in subprocess execution
# OR mid-arrange). The rebalance-mid-window scenario is the single most
# important failure mode the processor must handle correctly: Kafka
# reassigns partitions while the worker still has committed-offset
# candidates it has NOT yet finished processing.
#
# Invariants asserted:
#   1. No zombie processor — the revoked partition is removed from
#      ``app.processors`` once the background ``_stop_processor`` task
#      completes.
#   2. No silent offset loss — ``_stop_processor`` only commits the
#      watermark when ``drain()`` completes cleanly. On drain timeout
#      the commit is SKIPPED so the reassigned owner re-reads the
#      in-flight messages (at-least-once, not silently dropped).
#   3. No exceptions escape the poll loop / revoke callback. Anything
#      raised inside the handler or ``_stop_processor`` is logged via
#      ``_safe_call`` or swallowed with a warning.
#   4. In-flight subprocess tasks are either awaited (drain cleanly) or
#      cancelled, which triggers the executor's ``finally`` block that
#      kills the orphaned subprocess — no dangling processes remain.


class _SleepingArrangeHandler(BaseDrakkarHandler):
    """Arrange returns one sleeping subprocess task per message.

    Each task sleeps ``sleep_seconds`` inside a Python subprocess, so the
    task is genuinely in-flight from the executor's point of view. Using
    real subprocesses (not ``PrecomputedResult``) is what makes this a
    meaningful rebalance-mid-window test: a precomputed result would
    complete instantly and there would be no in-flight work to race
    against.
    """

    def __init__(self, sleep_seconds: float = 0.5) -> None:
        self._sleep_seconds = sleep_seconds

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['-c', f'import time; time.sleep({self._sleep_seconds}); print("done")'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


async def test_revoke_mid_window_no_offset_loss(test_config):
    """Revoking a partition with in-flight subprocess tasks must not
    commit offsets of unfinished messages and must tear the processor
    down cleanly.

    Timing setup: 3 messages each backed by a subprocess that sleeps
    well beyond the drain window. ``drain_timeout_seconds = 1`` (the
    config minimum) forces ``_stop_processor`` onto the timeout path
    where it SKIPS the final watermark commit to avoid silent data
    loss. The test then asserts the critical invariants irrespective
    of how many individual subprocesses managed to complete naturally
    before teardown.
    """
    from drakkar.executor import ExecutorPool

    # drain_timeout minimum is 1s (ge=1 on the Pydantic field). Subprocess
    # sleeps long enough that drain() is guaranteed to hit the timeout
    # path with at least one task still in flight.
    test_config.executor.drain_timeout_seconds = 1
    test_config.executor.window_size = 10

    sleep_seconds = 2.0
    app = DrakkarApp(handler=_SleepingArrangeHandler(sleep_seconds=sleep_seconds), config=test_config)
    # task_timeout_seconds must be > drain_timeout + sleep to ensure the
    # subprocesses do not time out themselves (isolates the variable).
    app._executor_pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
        task_timeout_seconds=10,
    )
    mock_consumer = AsyncMock()
    app._consumer = mock_consumer

    app._lifecycle._on_assign([0])
    processor = app.processors[0]

    # Enqueue 3 messages; arrange will schedule 3 subprocess tasks.
    for offset in (100, 101, 102):
        processor.enqueue(SourceMessage(topic='t', partition=0, offset=offset, value=b'x', timestamp=0))

    # Wait until at least 1 subprocess task is genuinely in flight.
    # inflight_count > 0 is the first signal: arrange returned and
    # _execute_and_track was scheduled. But the coroutine may not have
    # actually launched the subprocess yet — we need a stronger signal.
    # Poll for executor_pool gate acquisition (every in-flight task
    # acquires a slot from the priority gate before starting the
    # subprocess) so we know a subprocess really is running before we
    # fire the revoke.
    assert app._executor_pool is not None
    gate = app._executor_pool._gate
    max_executors = 4  # matches the ExecutorPool constructor above

    await wait_for(
        lambda: processor.inflight_count >= 1 and gate.available < max_executors,
        timeout=5,
    )
    assert processor.inflight_count >= 1, 'precondition: at least one task must be in flight'
    assert gate.available < max_executors, 'precondition: at least one executor pool slot must be held'

    # Fire revoke. _on_revoke pops the processor from app.processors
    # synchronously, then schedules _stop_processor as a background task.
    await app._lifecycle._on_revoke([0])
    assert 0 not in app.processors, 'processor must be popped from app.processors immediately on revoke'

    # Wait for the background _stop_processor task to finish draining,
    # committing (or skipping commit), and running processor.stop().
    # Upper bound: drain_timeout (1s) + stop() wait_for (10s) + slack.
    await wait_for(lambda: len(app._background_tasks) == 0, timeout=15)

    # Invariant 1: no zombie processor.
    assert 0 not in app.processors
    # Invariant 2: the processor's run-loop task is fully torn down.
    assert processor._task is None
    # Invariant 3: signal_stop flipped _running=False.
    assert processor._running is False

    # Invariant 4: no offset loss.
    #
    # Subprocesses may finish naturally while the run-loop awaits them
    # post-signal_stop — those legitimately-completed tasks commit
    # through the normal _finalize_message_tracker path. A clean finalize
    # of all three messages yields watermark 103 (last-offset + 1) from
    # the NATURAL commit path, which is CORRECT (work actually finished)
    # and is NOT offset loss.
    #
    # The bug we're guarding against is ``_stop_processor`` advancing
    # past unfinished work — that would surface as watermark > 103 (we
    # only enqueued 100-102). A watermark of exactly 103 is either the
    # happy-path finalize of all three, or the end of a clean drain —
    # both correct.
    committed_watermarks = [next(iter(call.args[0].values())) for call in mock_consumer.commit.call_args_list]
    if committed_watermarks:
        # Watermark must be monotonic — no re-commit of a lower value.
        assert committed_watermarks == sorted(committed_watermarks), (
            f'watermarks must be monotonically non-decreasing, got: {committed_watermarks}'
        )
        # Last-known-message + 1 is the upper bound for a correct
        # watermark. Values > 103 would mean the code was tricked into
        # advancing past unfinished work.
        assert max(committed_watermarks) <= 103, (
            f'commit watermark {max(committed_watermarks)} exceeds last-known-message+1 (103); '
            f'possible offset loss. All commits: {mock_consumer.commit.call_args_list}'
        )

    # Invariant 5: no orphaned message trackers. Every tracker must refer
    # to an enqueued offset (100-102). With drain_timeout the inflight-
    # finalize path may leave trackers for in-flight offsets, but we
    # enqueued exactly 3, so the bound is 3.
    assert len(processor._message_trackers) <= 3
    for offset in processor._message_trackers:
        assert offset in (100, 101, 102), f'tracker for unexpected offset: {offset}'


async def test_revoke_mid_window_clean_drain_commits_finished_messages(test_config):
    """When drain completes cleanly (tasks finish within drain_timeout),
    the final commit MUST include every processed message's watermark.

    Complement to ``test_revoke_mid_window_no_offset_loss``: same flow,
    but with drain_timeout generous enough that all subprocess tasks
    finish naturally. Asserts commits happen for every processed offset.
    """
    from drakkar.executor import ExecutorPool

    # Generous drain timeout — subprocesses (0.15s each) finish well within it.
    test_config.executor.drain_timeout_seconds = 5
    test_config.executor.window_size = 10

    app = DrakkarApp(handler=_SleepingArrangeHandler(sleep_seconds=0.15), config=test_config)
    app._executor_pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
        task_timeout_seconds=10,
    )
    mock_consumer = AsyncMock()
    app._consumer = mock_consumer

    app._lifecycle._on_assign([0])
    processor = app.processors[0]

    for offset in (200, 201, 202):
        processor.enqueue(SourceMessage(topic='t', partition=0, offset=offset, value=b'x', timestamp=0))

    await wait_for(lambda: processor.inflight_count >= 1, timeout=5)

    await app._lifecycle._on_revoke([0])
    await wait_for(lambda: len(app._background_tasks) == 0, timeout=10)

    # Processor removed, no zombie.
    assert 0 not in app.processors
    assert processor._task is None

    # Every offset 200-202 processed + committed. Commit watermark is
    # next-to-read, so highest commit should be 203.
    assert mock_consumer.commit.call_count >= 1, (
        f'clean drain should have committed finished offsets; got: {mock_consumer.commit.call_args_list}'
    )
    committed_watermarks = [next(iter(call.args[0].values())) for call in mock_consumer.commit.call_args_list]
    assert max(committed_watermarks) == 203, (
        f'watermark commit must be next-to-read (203) after processing 200-202; got: {committed_watermarks}'
    )


async def test_revoke_while_arrange_running(test_config):
    """Revocation fires while ``arrange()`` is still awaiting (e.g. a DB
    lookup). The processor must stop cleanly without wedging.

    This simulates the common operational case where a worker is
    mid-arrange (slow handler doing an I/O call) when Kafka rebalances.
    Expected behaviour:
      - signal_stop flips _running=False
      - arrange() eventually completes (we can't cancel it in-place —
        user code is awaiting)
      - the run-loop observes _running=False on its next iteration and
        exits the main loop
      - drain() completes (no pending offsets or in-flight tasks
        because arrange never finished producing any)
      - processor removed from app.processors
      - no exceptions escape
    """
    from drakkar.executor import ExecutorPool

    # Arrange holds the processor in arrange() for this long. drain_timeout
    # must be larger than this so the arrange can complete and the run
    # loop can exit cleanly without force-cancel.
    arrange_sleep = 0.3

    class SlowArrangeHandler(BaseDrakkarHandler):
        def __init__(self) -> None:
            self.arrange_fired = asyncio.Event()

        async def arrange(self, messages, pending):
            self.arrange_fired.set()
            await asyncio.sleep(arrange_sleep)
            # Return no tasks — the point of the test is to fire revoke
            # while arrange is awaiting, not to test task fan-out.
            return []

    handler = SlowArrangeHandler()

    test_config.executor.drain_timeout_seconds = 5  # plenty of headroom
    app = DrakkarApp(handler=handler, config=test_config)
    app._executor_pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    mock_consumer = AsyncMock()
    app._consumer = mock_consumer

    app._lifecycle._on_assign([0])
    processor = app.processors[0]

    processor.enqueue(SourceMessage(topic='t', partition=0, offset=50, value=b'x', timestamp=0))

    # Wait until arrange() is actually awaiting — this is the mid-arrange
    # revoke scenario we want to exercise.
    await handler.arrange_fired.wait()
    assert processor._arranging is True, 'processor must be mid-arrange when revoke fires'

    # Fire revoke mid-arrange.
    await app._lifecycle._on_revoke([0])
    assert 0 not in app.processors

    # The background _stop_processor must complete — processor stops
    # cleanly without wedging. Allow enough time for arrange_sleep +
    # drain + stop overhead.
    await wait_for(lambda: len(app._background_tasks) == 0, timeout=10)

    # Processor is fully torn down, no zombie.
    assert 0 not in app.processors
    assert processor._task is None
    assert processor._running is False
    # arrange() produced zero tasks, nothing pending — no commit needed.
    # The key assertion is we did NOT wedge waiting on arrange or
    # somewhere in stop(). If the test reaches here, clean stop worked.
    assert processor.inflight_count == 0
    assert not processor._offset_tracker.has_pending()


# --- AppLifecycle: focused unit tests ---


async def test_lifecycle_shutdown_drains_and_flips_ready(test_config):
    """Construct AppLifecycle directly against a DrakkarApp and verify
    that _shutdown drains the processors collection and flips is_ready
    off — the two invariants the lifecycle owns on the way out.

    This is the focused unit test promised by Phase 4 Task 3: it exercises
    the lifecycle as an isolated object rather than indirectly via
    ``app._shutdown``, so a future regression in the back-reference wiring
    will surface here even if the higher-level integration tests still pass.
    """
    from drakkar.executor import ExecutorPool
    from drakkar.lifecycle import AppLifecycle

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Pretend we're already running and ready — _shutdown must flip both off.
    app._running = True
    app.is_ready = True

    # Spin up one processor so the drain path actually has work to do.
    app._lifecycle._on_assign([0])
    assert 0 in app.processors

    # Construct a fresh AppLifecycle around the same app instance and
    # invoke _shutdown through it directly. Both lifecycles share state
    # via the back-reference, so they observe the same processor map.
    lifecycle = AppLifecycle(app)
    await lifecycle._shutdown()

    # Invariants the lifecycle owns on the way out:
    #   - is_ready is False (so a Kubernetes readiness probe fails fast)
    #   - all processors drained out of app._processors (no zombies)
    #   - sinks closed (delegated to SinkManager.close_all)
    assert app.is_ready is False
    assert len(app.processors) == 0
    app._consumer.close.assert_called_once()


def test_lifecycle_handle_signal_flips_running_off(test_config):
    """_handle_signal must trigger graceful shutdown by flipping the app's
    _running flag — the poll loop reads this on every iteration and exits
    when False. The signal handler is sync (registered via
    loop.add_signal_handler) so this test is sync too.
    """
    from drakkar.lifecycle import AppLifecycle

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._running = True
    lifecycle = AppLifecycle(app)

    lifecycle._handle_signal()

    assert app._running is False


async def test_lifecycle_back_reference_shares_state(test_config):
    """AppLifecycle holds only a back-reference to DrakkarApp; mutations
    on either side must be visible on the other. This guards against a
    future refactor that accidentally copies app state into the lifecycle.
    """
    from drakkar.lifecycle import AppLifecycle

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    lifecycle = AppLifecycle(app)

    # Mutate via app, observe via lifecycle.
    app._running = True
    assert lifecycle._app._running is True

    # Mutate via lifecycle, observe via app.
    lifecycle._app._paused = True
    assert app._paused is True


# --- Lifecycle: watchdog wiring -------------------------------------------


async def test_lifecycle_shutdown_marks_watchdog_clean_on_clean_drain(test_config, tmp_path):
    """``_shutdown`` calls ``mark_clean`` on the watchdog after a clean
    drain so the next startup does not falsely flag this run as OOM-killed.
    """
    from drakkar.lifecycle import AppLifecycle
    from drakkar.watchdog import WatchdogFile

    # Force the watchdog dir to a writable temp path so the test never
    # races a real on-disk file from a developer machine.
    test_config.ui.recorder.db_dir = str(tmp_path)

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()
    app._running = True

    lifecycle = AppLifecycle(app)
    # Pre-populate the watchdog as ``_async_run`` would. We construct it
    # explicitly because this test bypasses the full startup sequence.
    watchdog = WatchdogFile(data_dir=tmp_path, worker_id=app._worker_id)
    watchdog.write()
    lifecycle._watchdog = watchdog

    assert watchdog.path.exists()
    await lifecycle._shutdown()
    # mark_clean writes ``CLEAN_EXIT`` then unlinks → file is gone.
    assert not watchdog.path.exists()


async def test_lifecycle_shutdown_marks_watchdog_clean_on_drain_timeout(test_config, tmp_path, monkeypatch):
    """A drain timeout is NOT an OOM kill. The watchdog must still be
    marked clean so dashboards stop conflating "shutdown was slow" with
    "process was SIGKILLed". ``drakkar_drain_timeout_hit_total`` already
    captures the slow-drain signal separately.
    """
    from drakkar.lifecycle import AppLifecycle
    from drakkar.watchdog import WatchdogFile

    test_config.ui.recorder.db_dir = str(tmp_path)
    # Tighten the drain timeout so the test runs quickly.
    test_config.executor.drain_timeout_seconds = 1

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    lifecycle = AppLifecycle(app)

    # Force ``_drain_all_processors`` to time out. Patch on the
    # instance so other tests are unaffected.
    async def _slow_drain() -> None:
        await asyncio.sleep(10)  # well past drain_timeout_seconds

    monkeypatch.setattr(lifecycle, '_drain_all_processors', _slow_drain)

    watchdog = WatchdogFile(data_dir=tmp_path, worker_id=app._worker_id)
    watchdog.write()
    lifecycle._watchdog = watchdog

    await lifecycle._shutdown()
    # File is unlinked by ``mark_clean`` even though drain timed out —
    # the drain-timeout counter, not the OOM counter, owns that signal.
    assert not watchdog.path.exists()


async def test_lifecycle_shutdown_no_watchdog_does_not_crash(test_config):
    """When ``debug.db_dir`` is empty the watchdog is skipped entirely;
    ``_shutdown`` must still complete without referencing the missing
    watchdog object.
    """
    from drakkar.lifecycle import AppLifecycle

    test_config.ui.recorder.db_dir = ''  # disk-less mode

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    lifecycle = AppLifecycle(app)
    assert lifecycle._watchdog is None

    # No assertion needed beyond "does not raise" — the guard inside
    # ``_shutdown`` must short-circuit on ``self._watchdog is None``.
    await lifecycle._shutdown()


async def test_lifecycle_shutdown_drain_exception_still_runs_teardown(test_config, tmp_path, monkeypatch):
    """A non-TimeoutError out of ``_drain_all_processors`` must NOT skip
    the rest of shutdown. The watchdog must still be marked clean (it's
    not an OOM kill) and every subsystem teardown call must still run.
    Without the try/finally restructure in ``_shutdown``, a drain bug
    (RuntimeError, OSError, etc.) would leak open connections and
    leave a stale watchdog file the next startup would mis-classify as
    a SIGKILL.
    """
    from drakkar.lifecycle import AppLifecycle
    from drakkar.watchdog import WatchdogFile

    test_config.ui.recorder.db_dir = str(tmp_path)

    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    lifecycle = AppLifecycle(app)

    # Force the drain coroutine to raise something other than TimeoutError.
    async def _exploding_drain() -> None:
        raise RuntimeError('drain bug — processor invariant violated')

    monkeypatch.setattr(lifecycle, '_drain_all_processors', _exploding_drain)

    # Pre-populate a watchdog file so we can verify ``mark_clean`` ran.
    watchdog = WatchdogFile(data_dir=tmp_path, worker_id=app._worker_id)
    watchdog.write()
    lifecycle._watchdog = watchdog
    assert watchdog.path.exists()

    # Track sink_manager.close_all and consumer.close so we can assert
    # the teardown phase runs fully even when drain blew up.
    sink_close_mock = AsyncMock()
    monkeypatch.setattr(app._sink_manager, 'close_all', sink_close_mock)
    dlq_close_mock = AsyncMock()
    app._dlq_sink.close = dlq_close_mock
    consumer_close_mock = AsyncMock()
    app._consumer.close = consumer_close_mock

    # The drain raised a non-TimeoutError but ``_shutdown`` itself must NOT.
    # Every teardown step must still have executed.
    await lifecycle._shutdown()

    assert not watchdog.path.exists(), 'mark_clean must run even on drain exception'
    sink_close_mock.assert_awaited_once()
    dlq_close_mock.assert_awaited_once()
    consumer_close_mock.assert_awaited_once()


async def test_lifecycle_claim_watchdog_slot_tolerates_oserror(test_config, tmp_path, monkeypatch):
    """A failing ``watchdog.write()`` (e.g. read-only mount, permission
    denied, no space) must NOT propagate out of
    ``AppLifecycle._claim_watchdog_slot`` — the watchdog is
    observability-only and the rest of the worker is fully functional
    without it. The lifecycle must catch ``OSError``, log a warning,
    and disable the watchdog by setting ``self._watchdog = None`` so
    later ``mark_clean`` calls short-circuit cleanly.
    """
    from drakkar.lifecycle import AppLifecycle
    from drakkar.watchdog import WatchdogFile

    test_config.ui.recorder.db_dir = str(tmp_path)
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    lifecycle = AppLifecycle(app)

    watchdog = WatchdogFile(data_dir=tmp_path, worker_id=app._worker_id)

    def _failing_write() -> None:
        raise OSError('Read-only file system')

    # Replace the bound ``write`` method so the lifecycle helper hits
    # the ``except OSError`` branch.
    monkeypatch.setattr(watchdog, 'write', _failing_write)
    lifecycle._watchdog = watchdog

    # The contract: no exception escapes, watchdog is disabled.
    await lifecycle._claim_watchdog_slot()

    assert lifecycle._watchdog is None

    # And mark_clean during shutdown is now a no-op (idempotent guard).
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()
    await lifecycle._shutdown()  # must not raise


async def test_lifecycle_claim_watchdog_slot_no_op_when_disabled(test_config):
    """When the watchdog is already disabled (disk-less mode, prior
    write failure), ``_claim_watchdog_slot`` is a clean no-op.
    """
    from drakkar.lifecycle import AppLifecycle

    test_config.ui.recorder.db_dir = ''  # disk-less
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    lifecycle = AppLifecycle(app)
    assert lifecycle._watchdog is None

    # Must not raise, must leave the field as None.
    await lifecycle._claim_watchdog_slot()
    assert lifecycle._watchdog is None


# --- Boot sequence ordering and failure paths ---
#
# The startup sequence was extracted into named boot-step methods on
# ``AppLifecycle`` so each step is independently testable. These tests
# pin down designed behaviours that were previously impossible to check
# in isolation: the relative ordering between two steps, and each step's
# documented failure-tolerance contract.


async def test_boot_connects_sinks_before_subscribing_the_consumer(test_config, monkeypatch):
    """Subscribing before sinks are up would accept messages nothing can deliver."""
    cfg = test_config.model_copy(deep=True)
    # Skip the wall-clock alignment sleep between _start_consumer and subscribe.
    cfg.kafka.startup_align_enabled = False
    app = DrakkarApp(handler=SimpleHandler(), config=cfg)
    calls: list[str] = []

    def recorder(name):
        async def _step():
            calls.append(name)

        return _step

    for step in (
        '_setup_watchdog',
        '_build_executor_pool',
        '_start_observability',
        '_start_ui_and_recorder',
        '_start_cache',
        '_start_webapp',
    ):
        monkeypatch.setattr(app._lifecycle, step, recorder('other'))
    monkeypatch.setattr(app._lifecycle, '_connect_sinks', recorder('sinks'))

    # _async_run calls app._consumer.subscribe() AFTER _start_consumer returns,
    # so the stub must leave a usable consumer behind or that line raises
    # AttributeError on None.
    async def fake_start_consumer():
        calls.append('consumer')
        app._consumer = AsyncMock()

    monkeypatch.setattr(app._lifecycle, '_start_consumer', fake_start_consumer)
    monkeypatch.setattr(app._lifecycle, '_claim_watchdog_slot', recorder('watchdog_claim'))
    monkeypatch.setattr(app._lifecycle, '_poll_loop', recorder('poll'))
    monkeypatch.setattr(app._lifecycle, '_shutdown', recorder('shutdown'))

    await app._lifecycle._async_run()

    assert calls.index('sinks') < calls.index('consumer')


async def test_webapp_construction_failure_does_not_abort_startup(test_config, monkeypatch):
    """The webapp is optional infrastructure; sinks and the consumer are the critical path."""
    cfg = test_config.model_copy(deep=True)
    cfg.webapp.enabled = True
    # WebCapableHandler (not SimpleHandler) — DrakkarApp.__init__ validates
    # the handler declares HTTP request/response models whenever
    # webapp.enabled=True, before this test can even reach _start_webapp.
    app = DrakkarApp(handler=WebCapableHandler(), config=cfg)

    def explode(*a, **kw):
        raise RuntimeError('port already bound')

    monkeypatch.setattr('drakkar.webapp.WebApp', explode)

    with capture_logs() as cap:
        await app._lifecycle._start_webapp()

    assert app._webapp is None
    assert any(e['event'] == 'webapp_start_failed' for e in cap)


async def test_ui_disabled_warns_that_probes_are_unserved(test_config):
    """The UI server is the ONLY probe surface — disabling it must be loud."""
    cfg = test_config.model_copy(deep=True)
    cfg.ui.enabled = False
    app = DrakkarApp(handler=SimpleHandler(), config=cfg)

    with capture_logs() as cap:
        await app._lifecycle._start_ui_and_recorder()

    assert app._recorder is None
    assert any(e['event'] == 'ui_disabled_no_probes' for e in cap)


async def test_empty_db_dir_disables_the_watchdog_rather_than_using_cwd(test_config):
    """Falling back to CWD would break the no-on-disk-state promise on read-only mounts."""
    cfg = test_config.model_copy(deep=True)
    cfg.ui.recorder.db_dir = ''
    app = DrakkarApp(handler=SimpleHandler(), config=cfg)

    with capture_logs() as cap:
        await app._lifecycle._setup_watchdog()

    assert app._lifecycle._watchdog is None
    assert any(e['event'] == 'watchdog_disabled_no_db_dir' for e in cap)


async def test_watchdog_write_failure_disables_it_and_continues(test_config, monkeypatch):
    """Observability must never block startup — a read-only mount is not fatal."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    app._lifecycle._watchdog = MagicMock()
    app._lifecycle._watchdog.write.side_effect = OSError('read-only file system')
    app._lifecycle._watchdog.path = Path('/nowhere/wd')

    with capture_logs() as cap:
        await app._lifecycle._claim_watchdog_slot()

    assert app._lifecycle._watchdog is None
    assert any(e['event'] == 'watchdog_write_failed' for e in cap)


# --- Remaining boot-step bodies (beyond the brief's five) ---
#
# The five tests above cover the ordering invariant and the failure/disabled
# branches called out in the brief. They leave the *successful* body of most
# boot steps unexercised — this section closes the largest of those gaps
# with the same direct-call technique, each test still pinned to a real
# designed behaviour rather than a line-touching smoke call.


async def test_build_executor_pool_wires_config_and_handler_priority(test_config):
    """A wiring slip here would silently run the wrong executor count or
    drop the handler's task-priority override back to plain FIFO."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)

    await app._lifecycle._build_executor_pool()

    pool = app._executor_pool
    assert pool is not None
    assert pool.max_executors == test_config.executor.max_executors
    # The handler's own ``task_priority`` must be the pool's live priority
    # function — losing this wiring silently reverts every handler to the
    # framework's default (source-offset) ordering.
    assert pool._priority_fn == app._handler.task_priority


async def test_start_observability_logs_discovered_user_metrics(test_config):
    """A handler-declared Prometheus metric must be surfaced at startup so
    an operator can confirm it's wired without waiting for the first sample."""
    from prometheus_client import Counter

    class MetricHandler(SimpleHandler):
        widgets_seen = Counter('test_boot_widgets_seen_total', 'Widgets seen by the boot-sequence test')

    app = DrakkarApp(handler=MetricHandler(), config=test_config)

    with capture_logs() as cap:
        await app._lifecycle._start_observability()

    events = [e for e in cap if e['event'] == 'user_metrics_discovered']
    assert len(events) == 1
    assert events[0]['metrics'] == ['test_boot_widgets_seen (counter)']


async def test_async_run_swallows_cancelled_error_and_still_shuts_down(test_config, monkeypatch):
    """A cancelled poll loop must not crash the worker — shutdown still runs
    so in-flight work drains and offsets get committed."""
    cfg = test_config.model_copy(deep=True)
    cfg.kafka.startup_align_enabled = False
    app = DrakkarApp(handler=SimpleHandler(), config=cfg)

    for step in (
        '_setup_watchdog',
        '_build_executor_pool',
        '_start_observability',
        '_start_ui_and_recorder',
        '_start_cache',
        '_connect_sinks',
        '_start_webapp',
        '_claim_watchdog_slot',
    ):
        monkeypatch.setattr(app._lifecycle, step, AsyncMock())

    async def fake_start_consumer():
        app._consumer = AsyncMock()

    monkeypatch.setattr(app._lifecycle, '_start_consumer', fake_start_consumer)

    async def raise_cancelled():
        raise asyncio.CancelledError

    monkeypatch.setattr(app._lifecycle, '_poll_loop', raise_cancelled)
    shutdown_mock = AsyncMock()
    monkeypatch.setattr(app._lifecycle, '_shutdown', shutdown_mock)

    # Must not raise: CancelledError from the poll loop is expected during
    # normal shutdown signalling, not a crash.
    await app._lifecycle._async_run()

    shutdown_mock.assert_awaited_once()


async def test_async_run_logs_wall_clock_alignment_window(test_config, monkeypatch):
    """The alignment log events must name the exact wall-clock boundary a
    fleet converges on, and report how long this worker actually waited."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)
    assert app.config.kafka.startup_align_enabled  # precondition: default is on

    for step in (
        '_setup_watchdog',
        '_build_executor_pool',
        '_start_observability',
        '_start_ui_and_recorder',
        '_start_cache',
        '_connect_sinks',
        '_start_webapp',
        '_claim_watchdog_slot',
        '_poll_loop',
        '_shutdown',
    ):
        monkeypatch.setattr(app._lifecycle, step, AsyncMock())

    async def fake_start_consumer():
        app._consumer = AsyncMock()

    monkeypatch.setattr(app._lifecycle, '_start_consumer', fake_start_consumer)
    monkeypatch.setattr('drakkar.lifecycle.wait_for_aligned_startup', AsyncMock(return_value=0.25))

    with capture_logs() as cap:
        await app._lifecycle._async_run()

    waiting = next(e for e in cap if e['event'] == 'startup_align_waiting')
    done = next(e for e in cap if e['event'] == 'startup_align_done')
    # The published target must actually sit on an interval boundary —
    # that's the entire point of the alignment sleep.
    assert waiting['target_wall_unix'] % app.config.kafka.startup_align_interval_seconds == 0
    assert done['slept_seconds'] == 0.25


async def test_connect_sinks_wires_recorder_and_dlq_after_connecting(test_config, monkeypatch):
    """Delivery failures can only route to the DLQ once this step has run —
    attach_runtime is the one-shot wiring that makes that possible."""
    app = DrakkarApp(handler=SimpleHandler(), config=test_config)

    connect_all_mock = AsyncMock()
    monkeypatch.setattr(app, '_build_sinks', lambda: None)
    monkeypatch.setattr(app._sink_manager, 'connect_all', connect_all_mock)
    monkeypatch.setattr(app, '_build_dlq', lambda: None)
    fake_dlq = AsyncMock()
    fake_dlq.topic = 'test-in_dlq'
    app._dlq_sink = fake_dlq

    with capture_logs() as cap:
        await app._lifecycle._connect_sinks()

    connect_all_mock.assert_awaited_once()
    fake_dlq.connect.assert_awaited_once()
    # The sink manager needs the live DLQ reference to route circuit-open /
    # delivery-failure errors — this is the wiring step that supplies it.
    assert app._sink_manager._dlq_sink is fake_dlq
    sinks_events = [e for e in cap if e['event'] == 'sinks_configured']
    assert len(sinks_events) == 1
    assert sinks_events[0]['dlq_topic'] == 'test-in_dlq'


async def test_start_consumer_exposes_connected_postgres_pool_to_on_ready(test_config):
    """on_ready must see the already-connected postgres pool so a handler
    can run startup queries (migrations, lookup tables) on the same
    connection the rest of the worker uses, instead of opening its own."""

    class CapturingHandler(SimpleHandler):
        seen_pg_pool: object = 'unset'

        async def on_ready(self, config, db_pool):
            self.seen_pg_pool = db_pool

    handler = CapturingHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    _setup_app_sinks(app)
    pg_sink = next(sink for (sink_type, _), sink in app._sink_manager.sinks.items() if sink_type == 'postgres')
    pg_sink.pool = 'sentinel-pool'

    await app._lifecycle._start_consumer()

    assert app._consumer is not None
    assert handler.seen_pg_pool == 'sentinel-pool'


async def test_start_consumer_schedules_declared_periodic_tasks(test_config):
    """A handler's ``@periodic`` method must actually be scheduled at
    startup — silently dropping it would mean the interval task never runs
    and the handler's periodic work (cleanup, refresh, sync) never fires."""
    from drakkar.periodic import periodic

    class PeriodicHandler(SimpleHandler):
        @periodic(seconds=3600)
        async def sweep(self) -> None:
            pass

    handler = PeriodicHandler()
    app = DrakkarApp(handler=handler, config=test_config)
    _setup_app_sinks(app)

    await app._lifecycle._start_consumer()

    try:
        assert len(app._periodic_tasks) == 1
        assert app._periodic_tasks[0].get_name() == 'periodic:sweep'
    finally:
        for task in app._periodic_tasks:
            task.cancel()
        await asyncio.gather(*app._periodic_tasks, return_exceptions=True)


async def test_start_cache_wires_a_persisting_cache_when_enabled(test_config, tmp_path):
    """The handler's ``self.cache`` must become the real, persisting cache
    once cache.enabled=true — leaving the default NoOpCache stub in place
    would make every set/get silently discard instead of persisting."""
    cfg = test_config.model_copy(deep=True)
    cfg.cache.enabled = True
    cfg.cache.db_dir = str(tmp_path)
    app = DrakkarApp(handler=SimpleHandler(), config=cfg)

    await app._lifecycle._start_cache()

    try:
        assert app._cache_engine is not None
        app._handler.cache.set('k', 'v')
        assert app._handler.cache.peek('k') == 'v'
    finally:
        await app._cache_engine.stop()
