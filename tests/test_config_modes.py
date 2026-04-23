"""Tests for configuration mode combinations.

Validates that the framework handles all reasonable combinations of config
flags consistently — debug on/off, metrics on/off, store_events/config/state
toggles, db_dir empty, max_retries=0, window_size=1, single vs multiple
sinks, and various edge cases.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from drakkar.app import DrakkarApp
from drakkar.config import (
    DebugConfig,
    DLQConfig,
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.recorder import EventRecorder
from drakkar.sinks.manager import SinkNotConfiguredError
from tests.conftest import wait_for

# --- Helpers ---


class _D(BaseModel):
    x: int = 1


class SimpleHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['hello'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


class RetryHandler(BaseDrakkarHandler):
    """Handler that always asks for retries."""

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f'fail-{msg.offset}',
                args=['-c', 'import sys; sys.exit(1)'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]

    async def on_error(self, task, error):
        return ErrorAction.RETRY


class CollectHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['hello'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]

    async def on_task_complete(self, result):
        return CollectResult(
            kafka=[KafkaPayload(data=_D())],
        )


def make_msg(partition=0, offset=0) -> SourceMessage:
    return SourceMessage(topic='t', partition=partition, offset=offset, value=b'x', timestamp=0)


def make_config(**overrides) -> DrakkarConfig:
    """Build a DrakkarConfig with sensible test defaults, allowing overrides."""
    defaults = {
        'kafka': KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        'executor': ExecutorConfig(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10),
        'sinks': SinksConfig(kafka={'out': KafkaSinkConfig(topic='test-out')}),
        'metrics': MetricsConfig(enabled=False),
        'logging': LoggingConfig(level='WARNING', format='console'),
        'debug': DebugConfig(enabled=False),
    }
    defaults.update(overrides)
    return DrakkarConfig(**defaults)


def _setup_app_sinks(app: DrakkarApp) -> None:
    """Replace real sinks with async mocks."""
    app._build_sinks()
    for key, sink in app._sink_manager._sinks.items():
        mock_sink = AsyncMock()
        mock_sink.sink_type = sink.sink_type
        mock_sink.name = sink.name
        mock_sink._name = sink.name
        app._sink_manager._sinks[key] = mock_sink
        for i, s in enumerate(app._sink_manager._by_type[sink.sink_type]):
            if s.name == sink.name:
                app._sink_manager._by_type[sink.sink_type][i] = mock_sink


# ============================================================================
# 1. Debug enabled vs disabled — app-level integration
# ============================================================================


class TestDebugModes:
    """Verify app works identically with debug on and off."""

    def test_app_creation_debug_disabled(self):
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        assert app._recorder is None
        assert app._debug_server is None

    def test_app_creation_debug_enabled_no_server_yet(self):
        config = make_config(debug=DebugConfig(enabled=True))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        # recorder and debug_server are set during _async_run, not __init__
        assert app._recorder is None
        assert app._debug_server is None

    async def test_on_assign_without_recorder(self):
        """Partition assignment works when debug is disabled (recorder=None)."""
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        app._consumer = MagicMock()
        app._consumer.commit = AsyncMock()

        assert app._recorder is None
        app._on_assign([0, 1])
        assert len(app.processors) == 2

        for proc in app.processors.values():
            await proc.stop()

    async def test_on_revoke_without_recorder(self):
        """Partition revocation works when debug is disabled."""
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        app._consumer = AsyncMock()

        app._on_assign([0, 1])
        app._on_revoke([0])
        await asyncio.sleep(0.3)

        assert 0 not in app.processors
        for proc in app.processors.values():
            await proc.stop()

    async def test_handle_commit_without_recorder(self):
        """Offset commits work when debug is disabled."""
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._consumer = AsyncMock()

        await app._handle_commit(partition_id=0, offset=10)
        app._consumer.commit.assert_called_once_with({0: 10})

    async def test_handle_collect_without_recorder(self):
        """Sink delivery works when debug is disabled."""
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult(kafka=[KafkaPayload(data=_D())])
        await app._handle_collect(result, partition_id=0)

        sink = app._sink_manager._sinks[('kafka', 'out')]
        sink.deliver.assert_called_once()

    async def test_shutdown_without_debug(self):
        """Shutdown completes cleanly when debug is disabled."""
        config = make_config(debug=DebugConfig(enabled=False))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._consumer = AsyncMock()
        _setup_app_sinks(app)
        app._dlq_sink = AsyncMock()

        assert app._recorder is None
        assert app._debug_server is None
        await app._shutdown()

        app._consumer.close.assert_called_once()
        app._dlq_sink.close.assert_called_once()

    async def test_shutdown_with_debug_mocked(self):
        """Shutdown stops recorder and debug_server if they exist."""
        config = make_config(debug=DebugConfig(enabled=True))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._consumer = AsyncMock()
        _setup_app_sinks(app)
        app._dlq_sink = AsyncMock()
        app._recorder = AsyncMock()
        app._debug_server = AsyncMock()

        await app._shutdown()

        app._recorder.stop.assert_called_once()
        app._debug_server.stop.assert_called_once()


# ============================================================================
# 2. Partition processor with and without recorder
# ============================================================================


class TestPartitionProcessorRecorderModes:
    """PartitionProcessor must work identically with recorder=None vs real."""

    async def test_processor_without_recorder_processes_message(self):
        handler = SimpleHandler()
        pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        committed = []

        async def on_commit(pid, offset):
            committed.append((pid, offset))

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            recorder=None,
            on_commit=on_commit,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))
        await wait_for(lambda: len(committed) > 0)
        await proc.stop()

        assert committed[0] == (0, 1)

    async def test_processor_with_recorder_processes_message(self, tmp_path):
        handler = SimpleHandler()
        pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        committed = []

        async def on_commit(pid, offset):
            committed.append((pid, offset))

        debug_cfg = DebugConfig(enabled=True, db_dir=str(tmp_path), flush_interval_seconds=60)
        recorder = EventRecorder(debug_cfg, worker_name='test-w', cluster_name='')
        await recorder.start()

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            recorder=recorder,
            on_commit=on_commit,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))
        await wait_for(lambda: len(committed) > 0)
        await proc.stop()
        await recorder.stop()

        assert committed[0] == (0, 1)


# ============================================================================
# 3. Recorder with all store_* flag combinations
# ============================================================================


class TestRecorderStoreFlags:
    """Verify recorder handles all store_events/config/state flag combos."""

    @pytest.mark.parametrize(
        'store_events,store_config,store_state',
        [
            (True, True, True),
            (True, True, False),
            (True, False, True),
            (True, False, False),
            (False, True, True),
            (False, True, False),
            (False, False, True),
            (False, False, False),
        ],
    )
    async def test_recorder_start_stop_with_flag_combos(self, tmp_path, store_events, store_config, store_state):
        """Recorder starts and stops cleanly with any combination of store flags."""
        cfg = DebugConfig(
            enabled=True,
            db_dir=str(tmp_path),
            store_events=store_events,
            store_config=store_config,
            store_state=store_state,
            flush_interval_seconds=60,
        )
        rec = EventRecorder(cfg, worker_name='test', cluster_name='')
        await rec.start()

        # record some events regardless of flags
        msg = make_msg()
        rec.record_consumed(msg)

        # write_config should be safe regardless
        from drakkar.config import DrakkarConfig, ExecutorConfig

        dummy_config = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'))
        await rec.write_config(dummy_config)

        # get_events should return empty if store_events is False
        events = await rec.get_events()
        if not store_events:
            assert events == []

        await rec.stop()

    async def test_recorder_db_dir_empty_is_memory_only(self, tmp_path):
        """With db_dir='', recorder runs in memory-only mode — no crash."""
        cfg = DebugConfig(
            enabled=True,
            db_dir='',
            store_events=True,
            store_config=True,
            store_state=True,
            flush_interval_seconds=60,
        )
        rec = EventRecorder(cfg, worker_name='test', cluster_name='')
        await rec.start()

        # should not crash despite requesting all storage features
        msg = make_msg()
        rec.record_consumed(msg)

        events = await rec.get_events()
        assert events == []

        from drakkar.config import DrakkarConfig, ExecutorConfig

        dummy_config = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'))
        await rec.write_config(dummy_config)

        await rec.stop()

    async def test_recorder_discover_workers_requires_config_and_db(self, tmp_path):
        """discover_workers returns empty when store_config=False or db_dir=''."""
        cfg_no_config = DebugConfig(
            enabled=True,
            db_dir=str(tmp_path),
            store_config=False,
            flush_interval_seconds=60,
        )
        rec = EventRecorder(cfg_no_config, worker_name='test', cluster_name='')
        await rec.start()
        workers = await rec.discover_workers()
        assert workers == []
        await rec.stop()

        cfg_no_db = DebugConfig(
            enabled=True,
            db_dir='',
            store_config=True,
            flush_interval_seconds=60,
        )
        rec2 = EventRecorder(cfg_no_db, worker_name='test', cluster_name='')
        await rec2.start()
        workers = await rec2.discover_workers()
        assert workers == []
        await rec2.stop()

    @pytest.mark.parametrize('store_state', [True, False])
    async def test_recorder_state_sync_respects_flag(self, tmp_path, store_state):
        """State sync only writes when store_state=True and db exists."""
        cfg = DebugConfig(
            enabled=True,
            db_dir=str(tmp_path),
            store_events=False,
            store_config=False,
            store_state=store_state,
            flush_interval_seconds=60,
            state_sync_interval_seconds=1,
        )
        rec = EventRecorder(cfg, worker_name='test', cluster_name='')
        rec.set_state_provider(
            lambda: {
                'uptime_seconds': 1.0,
                'assigned_partitions': [],
                'partition_count': 0,
                'pool_active': 0,
                'pool_max': 4,
                'total_queued': 0,
                'paused': False,
            }
        )
        await rec.start()

        # manually trigger state sync
        await rec._sync_state()

        if store_state and rec._db:
            async with rec._db.execute('SELECT COUNT(*) FROM worker_state') as cur:
                row = await cur.fetchone()
            assert row[0] >= 1
        await rec.stop()


# ============================================================================
# 4. Executor edge cases: max_retries=0, window_size=1, no binary_path
# ============================================================================


class TestExecutorEdgeCases:
    async def test_max_retries_zero_no_retries_happen(self):
        """With max_retries=0, failed tasks are immediately skipped."""
        handler = RetryHandler()
        pool = ExecutorPool(binary_path=sys.executable, max_executors=2, task_timeout_seconds=10)

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            max_retries=0,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))

        # wait for processing to complete — task should fail once and be skipped
        # Use last_committed because completed_count resets to 0 after acknowledge_commit
        await wait_for(lambda: proc.offset_tracker.last_committed is not None, timeout=5.0)
        await proc.stop()

    async def test_max_retries_one_allows_single_retry(self):
        """With max_retries=1, task is retried once then skipped."""
        call_count = 0

        class CountingRetryHandler(BaseDrakkarHandler):
            async def arrange(self, messages, pending):
                return [
                    ExecutorTask(
                        task_id=f'fail-{msg.offset}',
                        args=['-c', 'import sys; sys.exit(1)'],
                        source_offsets=[msg.offset],
                    )
                    for msg in messages
                ]

            async def on_error(self, task, error):
                nonlocal call_count
                call_count += 1
                return ErrorAction.RETRY

        handler = CountingRetryHandler()
        pool = ExecutorPool(binary_path=sys.executable, max_executors=2, task_timeout_seconds=10)

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            max_retries=1,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))

        await wait_for(lambda: proc.offset_tracker.last_committed is not None, timeout=5.0)
        await proc.stop()

        # on_error called twice: once for initial failure, once for retry failure
        assert call_count == 2

    async def test_window_size_one_processes_single_message_per_window(self):
        """With window_size=1, each message forms its own window."""
        handler = SimpleHandler()
        pool = ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)
        committed = []

        async def on_commit(pid, offset):
            committed.append((pid, offset))

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=1,
            on_commit=on_commit,
        )
        proc.start()

        # enqueue 3 messages
        for i in range(3):
            proc.enqueue(make_msg(offset=i))

        await wait_for(lambda: len(committed) >= 1 and committed[-1][1] == 3, timeout=5.0)
        await proc.stop()

        # all 3 messages processed and committed
        assert any(c[1] == 3 for c in committed)

    async def test_no_binary_path_task_fails_with_clear_error(self):
        """When neither pool nor task has binary_path, task fails cleanly."""
        from drakkar.executor import ExecutorTaskError

        pool = ExecutorPool(binary_path=None, max_executors=2, task_timeout_seconds=10)
        task = ExecutorTask(
            task_id='no-binary',
            args=['hello'],
            source_offsets=[0],
            binary_path=None,
        )
        with pytest.raises(ExecutorTaskError) as exc_info:
            await pool.execute(task)

        assert exc_info.value.result.exit_code == -1
        assert 'binary_path' in exc_info.value.result.stderr.lower() or 'binary' in exc_info.value.error.stderr.lower()

    async def test_task_binary_overrides_pool_binary(self):
        """Per-task binary_path overrides pool-level binary_path."""
        pool = ExecutorPool(binary_path='/bin/false', max_executors=2, task_timeout_seconds=10)
        task = ExecutorTask(
            task_id='override',
            args=['hello'],
            source_offsets=[0],
            binary_path='/bin/echo',
        )
        result = await pool.execute(task)
        assert result.exit_code == 0
        assert 'hello' in result.stdout


# ============================================================================
# 5. Sinks: no sinks, single sink default resolution, empty collect
# ============================================================================


class TestSinkModes:
    async def test_no_sinks_raises_on_async_run(self):
        """Starting with no sinks raises SinkNotConfiguredError."""
        config = make_config(sinks=SinksConfig())
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        with pytest.raises(SinkNotConfiguredError, match='No sinks configured'):
            await app._async_run()

    async def test_single_sink_default_resolution(self):
        """With one kafka sink, empty sink name in payload auto-resolves."""
        config = make_config(sinks=SinksConfig(kafka={'only': KafkaSinkConfig(topic='out')}))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult(kafka=[KafkaPayload(data=_D())])  # sink='' default
        await app._handle_collect(result, partition_id=0)

        sink = app._sink_manager._sinks[('kafka', 'only')]
        sink.deliver.assert_called_once()

    async def test_multiple_sinks_require_explicit_name(self):
        """With two kafka sinks, empty sink name raises AmbiguousSinkError."""
        from drakkar.sinks.manager import AmbiguousSinkError

        config = make_config(
            sinks=SinksConfig(
                kafka={
                    'primary': KafkaSinkConfig(topic='out-1'),
                    'secondary': KafkaSinkConfig(topic='out-2'),
                }
            )
        )
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult(kafka=[KafkaPayload(data=_D())])  # sink='' ambiguous
        with pytest.raises(AmbiguousSinkError):
            await app._handle_collect(result, partition_id=0)

    async def test_multiple_sinks_explicit_name_works(self):
        """With two kafka sinks, explicit sink name routes correctly."""
        config = make_config(
            sinks=SinksConfig(
                kafka={
                    'primary': KafkaSinkConfig(topic='out-1'),
                    'secondary': KafkaSinkConfig(topic='out-2'),
                }
            )
        )
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult(
            kafka=[KafkaPayload(sink='secondary', data=_D())],
        )
        await app._handle_collect(result, partition_id=0)

        primary = app._sink_manager._sinks[('kafka', 'primary')]
        secondary = app._sink_manager._sinks[('kafka', 'secondary')]
        primary.deliver.assert_not_called()
        secondary.deliver.assert_called_once()

    async def test_empty_collect_result_skips_delivery(self):
        """Empty CollectResult does not trigger any sink delivery."""
        config = make_config()
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult()
        await app._handle_collect(result, partition_id=0)

        for sink in app._sink_manager._sinks.values():
            sink.deliver.assert_not_called()

    async def test_collect_to_unconfigured_sink_type_raises(self):
        """Routing to a sink type that isn't configured raises."""
        from drakkar.models import HttpPayload

        config = make_config(sinks=SinksConfig(kafka={'out': KafkaSinkConfig(topic='t')}))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        _setup_app_sinks(app)

        result = CollectResult(http=[HttpPayload(data=_D())])
        with pytest.raises(SinkNotConfiguredError):
            await app._handle_collect(result, partition_id=0)


# ============================================================================
# 6. DLQ defaults and custom config
# ============================================================================


class TestDLQModes:
    def test_dlq_default_topic_derived_from_source(self):
        config = make_config()
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._build_dlq()
        assert app._dlq_sink.topic == 'test-in_dlq'

    def test_dlq_custom_topic(self):
        config = make_config(dlq=DLQConfig(topic='my-dlq'))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._build_dlq()
        assert app._dlq_sink.topic == 'my-dlq'

    def test_dlq_custom_brokers(self):
        config = make_config(dlq=DLQConfig(brokers='dlq-kafka:9092'))
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._build_dlq()
        assert app._dlq_sink._brokers == 'dlq-kafka:9092'

    def test_dlq_empty_brokers_inherits_from_kafka(self):
        config = make_config(
            kafka=KafkaConfig(brokers='main-kafka:9092'),
            dlq=DLQConfig(brokers=''),
        )
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._build_dlq()
        assert app._dlq_sink._brokers == 'main-kafka:9092'


# ============================================================================
# 7. Metrics enabled vs disabled
# ============================================================================


# ============================================================================
# 8. Combined mode stress: debug=off + processing full pipeline
# ============================================================================


class TestCombinedModes:
    async def test_full_pipeline_debug_off_metrics_off(self):
        """Full message -> arrange -> execute -> collect -> commit with
        debug disabled and metrics disabled."""
        config = make_config(
            debug=DebugConfig(enabled=False),
            metrics=MetricsConfig(enabled=False),
        )
        handler = CollectHandler()
        app = DrakkarApp(handler=handler, config=config)
        app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        app._consumer = MagicMock()
        app._consumer.commit = AsyncMock()
        _setup_app_sinks(app)
        app._dlq_sink = AsyncMock()

        app._on_assign([0])
        proc = app.processors[0]
        proc.enqueue(make_msg(offset=0))

        await wait_for(
            lambda: app._consumer.commit.call_count > 0,
            timeout=5.0,
        )
        await proc.stop()

        sink = app._sink_manager._sinks[('kafka', 'out')]
        sink.deliver.assert_called()
        app._consumer.commit.assert_called()

    async def test_full_pipeline_window_size_1_max_retries_0(self):
        """Pipeline works with window_size=1 and max_retries=0."""
        config = make_config(
            executor=ExecutorConfig(
                binary_path='/bin/echo',
                max_executors=2,
                task_timeout_seconds=10,
                window_size=1,
                max_retries=0,
            ),
            debug=DebugConfig(enabled=False),
            metrics=MetricsConfig(enabled=False),
        )
        handler = CollectHandler()
        app = DrakkarApp(handler=handler, config=config)
        app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
        app._consumer = MagicMock()
        app._consumer.commit = AsyncMock()
        _setup_app_sinks(app)
        app._dlq_sink = AsyncMock()

        app._on_assign([0])
        proc = app.processors[0]

        for i in range(3):
            proc.enqueue(make_msg(offset=i))

        await wait_for(
            lambda: (
                app._consumer.commit.call_count >= 1
                and any(call.args[0].get(0, 0) >= 3 for call in app._consumer.commit.call_args_list)
            ),
            timeout=5.0,
        )
        await proc.stop()

    async def test_failing_task_with_max_retries_0_debug_off(self):
        """Failed task with max_retries=0 and debug off is skipped cleanly."""
        config = make_config(
            executor=ExecutorConfig(
                binary_path=sys.executable,
                max_executors=2,
                task_timeout_seconds=10,
                max_retries=0,
            ),
            debug=DebugConfig(enabled=False),
        )
        handler = RetryHandler()  # asks for RETRY but max_retries=0 blocks it
        app = DrakkarApp(handler=handler, config=config)
        app._executor_pool = ExecutorPool(binary_path=sys.executable, max_executors=2, task_timeout_seconds=10)
        app._consumer = MagicMock()
        app._consumer.commit = AsyncMock()
        _setup_app_sinks(app)
        app._dlq_sink = AsyncMock()

        app._on_assign([0])
        proc = app.processors[0]
        proc.enqueue(make_msg(offset=0))

        # task should fail and be skipped (no infinite retry loop)
        await wait_for(
            lambda: app._consumer.commit.call_count > 0,
            timeout=5.0,
        )
        await proc.stop()


# ============================================================================
# 9. Backpressure edge cases with different multipliers
# ============================================================================


class TestBackpressureEdgeCases:
    async def test_backpressure_with_multiplier_1(self):
        """Low multipliers don't cause division by zero or infinite loops."""
        config = make_config(
            executor=ExecutorConfig(
                binary_path='/bin/echo',
                max_executors=1,
                task_timeout_seconds=10,
                backpressure_high_multiplier=1,
                backpressure_low_multiplier=1,
            ),
        )
        app = DrakkarApp(handler=SimpleHandler(), config=config)
        app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=1, task_timeout_seconds=10)
        app._consumer = AsyncMock()
        app._running = True

        max_executors = config.executor.max_executors
        high_watermark = max_executors * config.executor.backpressure_high_multiplier
        low_watermark = max(1, max_executors * config.executor.backpressure_low_multiplier)

        # high=1, low=1 — pause/resume thresholds are the same
        assert high_watermark == 1
        assert low_watermark == 1

        app._on_assign([0])
        proc = app.processors[0]
        proc._queue.put_nowait(make_msg(offset=0))

        total = app._total_queued()
        assert total >= 1

        # simulate pause
        if not app._paused and total >= high_watermark:
            await app._consumer.pause([0])
            app._paused = True

        assert app._paused
        app._consumer.pause.assert_called_once()

        # drain to 0
        while not proc._queue.empty():
            proc._queue.get_nowait()

        total = app._total_queued()
        # total is 0, low_watermark is 1 → 0 <= 1 → resume
        if app._paused and total <= low_watermark:
            await app._consumer.resume([0])
            app._paused = False

        assert not app._paused
        await proc.stop()


# ============================================================================
# 10. Consumer without callbacks
# ============================================================================


class TestConsumerCallbackModes:
    async def test_consumer_creation_without_callbacks(self):
        """KafkaConsumer can be created with no assign/revoke callbacks."""
        from drakkar.consumer import KafkaConsumer

        consumer = KafkaConsumer(
            config=KafkaConfig(brokers='localhost:9092'),
            on_assign=None,
            on_revoke=None,
        )
        assert consumer._on_assign_cb is None
        assert consumer._on_revoke_cb is None

    async def test_consumer_creation_with_callbacks(self):
        from drakkar.consumer import KafkaConsumer

        assign_cb = MagicMock()
        revoke_cb = MagicMock()
        consumer = KafkaConsumer(
            config=KafkaConfig(brokers='localhost:9092'),
            on_assign=assign_cb,
            on_revoke=revoke_cb,
        )
        assert consumer._on_assign_cb is assign_cb
        assert consumer._on_revoke_cb is revoke_cb


# ============================================================================
# 11. Processor without callbacks (on_collect, on_commit)
# ============================================================================


class TestProcessorCallbackModes:
    async def test_processor_without_callbacks_processes_message(self):
        """Processor works even without on_collect or on_commit callbacks."""
        handler = SimpleHandler()
        pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            on_collect=None,
            on_commit=None,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))

        # Without on_commit, offsets go COMPLETED → acknowledged immediately,
        # so last_committed is the stable indicator
        await wait_for(lambda: proc.offset_tracker.last_committed is not None, timeout=5.0)
        await proc.stop()

    async def test_processor_with_collect_but_no_commit(self):
        """Processor collects results but skips commit when callback is None."""
        collected = []
        handler = CollectHandler()
        pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

        async def on_collect(result, partition_id):
            collected.append(result)

        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=5,
            on_collect=on_collect,
            on_commit=None,
        )
        proc.start()
        proc.enqueue(make_msg(offset=0))

        await wait_for(lambda: len(collected) > 0, timeout=5.0)
        await proc.stop()

        assert len(collected) >= 1


# ========== Config Summary ==========


class TestConfigSummary:
    """Tests for DrakkarConfig.config_summary() one-liner."""

    def test_default_config_summary_format(self):
        cfg = make_config()
        summary = cfg.config_summary(worker_id='worker-01', cluster_name='prod')
        assert summary.startswith('[worker-01/prod]')
        assert 'topic=test-in' in summary
        assert 'group=drakkar-workers' in summary
        assert 'exec=2w/' in summary
        assert 'retries=' in summary
        assert 'debug=off' in summary
        assert 'sinks=[kf:out]' in summary
        assert 'log=WARNING' in summary

    def test_summary_no_cluster(self):
        cfg = make_config()
        summary = cfg.config_summary(worker_id='w1')
        assert summary.startswith('[w1]')
        assert '/prod' not in summary.split(']')[0]

    def test_summary_no_worker_id(self):
        cfg = make_config()
        summary = cfg.config_summary()
        assert summary.startswith('[?]')

    def test_summary_debug_on(self):
        cfg = make_config(debug=DebugConfig(enabled=True, port=8080))
        summary = cfg.config_summary(worker_id='w')
        assert 'debug=on:8080' in summary

    def test_summary_metrics_enabled(self):
        cfg = make_config(metrics=MetricsConfig(enabled=True, port=9090))
        summary = cfg.config_summary(worker_id='w')
        assert 'metrics=9090' in summary

    def test_summary_metrics_disabled(self):
        cfg = make_config(metrics=MetricsConfig(enabled=False))
        summary = cfg.config_summary(worker_id='w')
        assert 'metrics=off' in summary

    def test_summary_custom_dlq_topic(self):
        cfg = make_config(dlq=DLQConfig(topic='my-dlq'))
        summary = cfg.config_summary(worker_id='w')
        assert 'dlq=my-dlq' in summary

    def test_summary_default_dlq(self):
        cfg = make_config()
        summary = cfg.config_summary(worker_id='w')
        assert 'dlq=on' in summary

    def test_summary_multiple_sink_types(self):
        from drakkar.config import PostgresSinkConfig

        cfg = make_config(
            sinks=SinksConfig(
                kafka={'events': KafkaSinkConfig(topic='t1'), 'alerts': KafkaSinkConfig(topic='t2')},
                postgres={'main': PostgresSinkConfig(dsn='postgresql://localhost/db')},
            ),
        )
        summary = cfg.config_summary(worker_id='w')
        assert 'kf:events,alerts' in summary
        assert 'pg:main' in summary

    def test_summary_no_sinks(self):
        cfg = make_config(sinks=SinksConfig())
        summary = cfg.config_summary(worker_id='w')
        assert 'sinks=[none]' in summary

    def test_summary_executor_params(self):
        cfg = make_config(
            executor=ExecutorConfig(
                binary_path='/bin/echo',
                max_executors=8,
                window_size=200,
                max_retries=5,
                task_timeout_seconds=300,
            ),
            kafka=KafkaConfig(max_poll_records=50),
        )
        summary = cfg.config_summary(worker_id='w')
        assert 'exec=8w/200win/50poll' in summary
        assert 'retries=5/300s' in summary

    def test_summary_cache_off_by_default(self):
        """Disabled cache renders as 'cache=off' — minimal footprint in logs."""
        from drakkar.config import CacheConfig

        cfg = make_config(cache=CacheConfig(enabled=False))
        summary = cfg.config_summary(worker_id='w')
        assert 'cache=off' in summary

    def test_summary_cache_on_with_default_intervals(self):
        """Enabled cache with integer-second defaults renders as
        'cache=on:f=3s/s=30s/c=60s' — :g format keeps integer intervals clean.
        """
        from drakkar.config import CacheConfig

        cfg = make_config(cache=CacheConfig(enabled=True))
        summary = cfg.config_summary(worker_id='w')
        assert 'cache=on:f=3s/s=30s/c=60s' in summary

    def test_summary_cache_on_with_fractional_flush(self):
        """Fractional intervals render with decimal via :g format — e.g. 0.5 → '0.5s'."""
        from drakkar.config import CacheConfig

        cfg = make_config(cache=CacheConfig(enabled=True, flush_interval_seconds=0.5))
        summary = cfg.config_summary(worker_id='w')
        assert 'cache=on:f=0.5s/s=30s/c=60s' in summary

    def test_summary_cache_on_with_peer_sync_disabled(self):
        """peer_sync.enabled=false renders the sync slot as 'off' instead of an interval."""
        from drakkar.config import CacheConfig, CachePeerSyncConfig

        cfg = make_config(
            cache=CacheConfig(
                enabled=True,
                peer_sync=CachePeerSyncConfig(enabled=False),
            )
        )
        summary = cfg.config_summary(worker_id='w')
        assert 'cache=on:f=3s/s=off/c=60s' in summary

    def test_summary_cache_on_with_max_memory_entries(self):
        """max_memory_entries renders as 'max=N' when set, absent only when
        explicitly None. The default 10_000 cap is included in the summary so
        operators can verify the effective limit at a glance."""
        from drakkar.config import CacheConfig

        cfg_with = make_config(cache=CacheConfig(enabled=True, max_memory_entries=5000))
        summary_with = cfg_with.config_summary(worker_id='w')
        assert 'max=5000' in summary_with

        # Default config now carries max_memory_entries=10_000 — render it.
        cfg_default = make_config(cache=CacheConfig(enabled=True))
        summary_default = cfg_default.config_summary(worker_id='w')
        assert 'max=10000' in summary_default

        # Explicit None (unbounded) is the only case where 'max=' is absent.
        cfg_unbounded = make_config(cache=CacheConfig(enabled=True, max_memory_entries=None))
        summary_unbounded = cfg_unbounded.config_summary(worker_id='w')
        assert 'max=' not in summary_unbounded
