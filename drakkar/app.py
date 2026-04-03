"""Main Drakkar application — wires all components together.

Orchestrates Kafka consumption, subprocess execution, sink delivery,
and offset management. Uses the pluggable sink system for output.
"""

import asyncio
import os
import signal
import time
from collections.abc import Coroutine
from pathlib import Path

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.config import DrakkarConfig, load_config
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.logging import setup_logging
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    discover_handler_metrics,
    start_metrics_server,
    total_queued,
    worker_info,
)
from drakkar.models import CollectResult, DeliveryAction, DeliveryError
from drakkar.partition import PartitionProcessor
from drakkar.periodic import discover_periodic_tasks, run_periodic_task
from drakkar.recorder import EventRecorder
from drakkar.sinks.dlq import DLQSink
from drakkar.sinks.filesystem import FileSink
from drakkar.sinks.http import HttpSink
from drakkar.sinks.kafka import KafkaSink
from drakkar.sinks.manager import SinkManager, SinkNotConfiguredError
from drakkar.sinks.mongo import MongoSink
from drakkar.sinks.postgres import PostgresSink
from drakkar.sinks.redis import RedisSink

logger = structlog.get_logger()


class DrakkarApp:
    """Main application that orchestrates Kafka consumption, subprocess
    execution, and result delivery to configured sinks.
    """

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        config_path: str | Path | None = None,
        config: DrakkarConfig | None = None,
        worker_id: str = '',
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = load_config(config_path)

        self._handler = handler
        self._worker_id = worker_id or os.environ.get(self._config.worker_name_env, '') or f'drakkar-{id(self):x}'
        self._cluster_name = ''
        if self._config.cluster_name_env:
            self._cluster_name = os.environ.get(self._config.cluster_name_env, '')
        if not self._cluster_name:
            self._cluster_name = self._config.cluster_name
        self._start_time = time.monotonic()

        self._executor_pool: ExecutorPool | None = None
        self._consumer: KafkaConsumer | None = None
        self._sink_manager: SinkManager = SinkManager()
        self._dlq_sink: DLQSink | None = None
        self._recorder: EventRecorder | None = None
        self._debug_server = None

        self._processors: dict[int, PartitionProcessor] = {}
        self._running = False
        self._paused = False
        self._background_tasks: set[asyncio.Task] = set()
        self._periodic_tasks: list[asyncio.Task] = []
        self._config_summary: str = ''

    @property
    def config(self) -> DrakkarConfig:
        return self._config

    @property
    def processors(self) -> dict[int, PartitionProcessor]:
        return self._processors

    @property
    def recorder(self) -> EventRecorder | None:
        return self._recorder

    @property
    def sink_manager(self) -> SinkManager:
        return self._sink_manager

    @property
    def config_summary(self) -> str:
        return self._config_summary

    def run(self) -> None:
        """Start the application. Blocks until shutdown."""
        setup_logging(
            self._config.logging,
            worker_id=self._worker_id,
            consumer_group=self._config.kafka.consumer_group,
            version=__version__,
        )
        asyncio.run(self._async_run())

    def _build_sinks(self) -> None:
        """Create sink instances from config and register with SinkManager."""
        kafka_brokers = self._config.kafka.brokers

        for name, cfg in self._config.sinks.kafka.items():
            self._sink_manager.register(KafkaSink(name, cfg, brokers_fallback=kafka_brokers))

        for name, cfg in self._config.sinks.postgres.items():
            self._sink_manager.register(PostgresSink(name, cfg))

        for name, cfg in self._config.sinks.mongo.items():
            self._sink_manager.register(MongoSink(name, cfg))

        for name, cfg in self._config.sinks.http.items():
            self._sink_manager.register(HttpSink(name, cfg))

        for name, cfg in self._config.sinks.redis.items():
            self._sink_manager.register(RedisSink(name, cfg))

        for name, cfg in self._config.sinks.filesystem.items():
            self._sink_manager.register(FileSink(name, cfg))

    def _build_dlq(self) -> None:
        """Create the DLQ sink from config."""
        dlq_topic = self._config.dlq.topic or f'{self._config.kafka.source_topic}_dlq'
        dlq_brokers = self._config.dlq.brokers or self._config.kafka.brokers
        self._dlq_sink = DLQSink(topic=dlq_topic, brokers=dlq_brokers)

    async def _async_run(self) -> None:
        log = logger.bind(worker_id=self._worker_id)

        bind_contextvars(hook='on_startup')
        self._config = await self._handler.on_startup(self._config)
        unbind_contextvars('hook')

        self._config_summary = self._config.config_summary(
            worker_id=self._worker_id,
            cluster_name=self._cluster_name,
        )
        await log.ainfo('drakkar_starting', category='lifecycle', config=self._config_summary)

        # validate at least one sink is configured
        if self._config.sinks.is_empty:
            raise SinkNotConfiguredError('No sinks configured. Add at least one sink to the sinks: section in config.')

        self._executor_pool = ExecutorPool(
            binary_path=self._config.executor.binary_path,
            max_workers=self._config.executor.max_workers,
            task_timeout_seconds=self._config.executor.task_timeout_seconds,
        )

        start_metrics_server(self._config.metrics)
        worker_info.info(
            {
                'worker_id': self._worker_id,
                'version': __version__,
                'consumer_group': self._config.kafka.consumer_group,
            }
        )

        user_metrics = discover_handler_metrics(self._handler)
        if user_metrics:
            await log.ainfo(
                'user_metrics_discovered',
                category='lifecycle',
                metrics=[f'{m._name} ({m._type})' for m in user_metrics.values()],
            )

        if self._config.debug.enabled:
            self._recorder = EventRecorder(
                self._config.debug,
                worker_name=self._worker_id,
                cluster_name=self._cluster_name,
            )
            self._recorder.set_state_provider(self._get_worker_state)
            await self._recorder.start()
            await self._recorder.write_config(self._config)

            from drakkar.debug_server import DebugServer

            self._debug_server = DebugServer(
                config=self._config.debug,
                recorder=self._recorder,
                app=self,
            )
            await self._debug_server.start()

        # build and connect sinks
        self._build_sinks()
        await self._sink_manager.connect_all()

        # build and connect DLQ
        self._build_dlq()
        assert self._dlq_sink is not None
        await self._dlq_sink.connect()

        # log sink topology
        await log.ainfo(
            'sinks_configured',
            category='lifecycle',
            sinks=self._config.sinks.summary(),
            dlq_topic=self._dlq_sink.topic,
        )

        self._consumer = KafkaConsumer(
            config=self._config.kafka,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # expose postgres pool for on_ready if available
        pg_pool = None
        for (sink_type, _), sink in self._sink_manager.sinks.items():
            if sink_type == 'postgres' and hasattr(sink, 'pool'):
                pg_pool = sink.pool
                break

        bind_contextvars(hook='on_ready')
        await self._handler.on_ready(self._config, pg_pool)
        unbind_contextvars('hook')

        # start periodic tasks declared on the handler
        for name, method, meta in discover_periodic_tasks(self._handler):
            task = asyncio.create_task(
                run_periodic_task(
                    name=name,
                    coro_fn=method,
                    seconds=meta.seconds,
                    on_error=meta.on_error,
                ),
                name=f'periodic:{name}',
            )
            self._periodic_tasks.append(task)

        await self._consumer.subscribe()
        self._running = True

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal)

        try:
            await self._poll_loop()
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    def _total_queued(self) -> int:
        """Total messages buffered across all partition queues + in-flight tasks."""
        return sum(p.queue_size + p.inflight_count for p in self._processors.values())

    def _get_worker_state(self) -> dict:
        """Return current worker state for the recorder's state sync."""
        return {
            'uptime_seconds': time.monotonic() - self._start_time,
            'assigned_partitions': sorted(self._processors.keys()),
            'partition_count': len(self._processors),
            'pool_active': self._executor_pool.active_count if self._executor_pool else 0,
            'pool_max': self._executor_pool.max_workers if self._executor_pool else 0,
            'total_queued': self._total_queued(),
            'paused': self._paused,
        }

    async def _poll_loop(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume."""
        assert self._consumer is not None
        assert self._executor_pool is not None
        max_workers = self._config.executor.max_workers
        high_watermark = max_workers * self._config.executor.backpressure_high_multiplier
        low_watermark = max(1, max_workers * self._config.executor.backpressure_low_multiplier)

        while self._running:
            total = self._total_queued()
            total_queued.set(total)

            if self._paused and total <= low_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    await self._consumer.resume(partition_ids)
                    self._paused = False
                    backpressure_active.set(0)

            if not self._paused and total >= high_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    await self._consumer.pause(partition_ids)
                    self._paused = True
                    backpressure_active.set(1)

            messages = await self._consumer.poll_batch()
            for msg in messages:
                processor = self._processors.get(msg.partition)
                if processor:
                    processor.enqueue(msg)

            if not messages:
                await asyncio.sleep(0.05)

    def _on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        assert self._executor_pool is not None
        if self._recorder:
            self._recorder.record_assigned(partition_ids)
        for pid in partition_ids:
            if pid not in self._processors:
                processor = PartitionProcessor(
                    partition_id=pid,
                    handler=self._handler,
                    executor_pool=self._executor_pool,
                    window_size=self._config.executor.window_size,
                    max_retries=self._config.executor.max_retries,
                    on_collect=self._handle_collect,
                    on_commit=self._handle_commit,
                    recorder=self._recorder,
                )
                self._processors[pid] = processor
                processor.start()

        assigned_partitions.set(len(self._processors))

        async def _on_assign_with_ctx() -> None:
            bind_contextvars(hook='on_assign', partitions=partition_ids)
            try:
                await self._handler.on_assign(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_assign_with_ctx()))
        self._background_tasks.add(t)
        t.add_done_callback(self._background_tasks.discard)

    def _on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation."""
        if self._recorder:
            self._recorder.record_revoked(partition_ids)
        for pid in partition_ids:
            processor = self._processors.pop(pid, None)
            if processor:
                t = asyncio.ensure_future(self._stop_processor(processor))
                self._background_tasks.add(t)
                t.add_done_callback(self._background_tasks.discard)

        assigned_partitions.set(len(self._processors))

        async def _on_revoke_with_ctx() -> None:
            bind_contextvars(hook='on_revoke', partitions=partition_ids)
            try:
                await self._handler.on_revoke(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_revoke_with_ctx()))
        self._background_tasks.add(t)
        t.add_done_callback(self._background_tasks.discard)

    async def _safe_call(self, coro: Coroutine) -> None:
        """Run a coroutine and log any exception instead of leaving it unretrieved."""
        try:
            await coro
        except Exception as e:
            logger.warning('async_callback_failed', category='lifecycle', error=str(e))

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        """Drain in-flight tasks, commit final offsets, then stop."""
        try:
            processor._running = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=self._config.executor.drain_timeout_seconds)
            except TimeoutError:
                pass
            committable = processor.offset_tracker.committable()
            if committable is not None and self._consumer:
                try:
                    await self._consumer.commit({processor.partition_id: committable})
                    processor.offset_tracker.acknowledge_commit(committable)
                except Exception:
                    pass
            await processor.stop()
        except Exception as e:
            logger.warning(
                'stop_processor_failed',
                category='lifecycle',
                partition=processor.partition_id,
                error=str(e),
            )

    async def _handle_collect(self, result: CollectResult, partition_id: int) -> None:
        """Deliver CollectResult payloads to configured sinks.

        Validates that all payload sink types are configured, then delivers
        via SinkManager. On delivery error, calls the handler's
        on_delivery_error hook and handles DLQ/RETRY/SKIP.
        """
        if not result.has_outputs:
            return

        self._sink_manager.validate_collect(result)

        async def _on_delivery_error(error: 'DeliveryError') -> DeliveryAction:
            bind_contextvars(hook='on_delivery_error', sink_type=error.sink_type, sink_name=error.sink_name)
            try:
                action = await self._handler.on_delivery_error(error)
            finally:
                unbind_contextvars('hook', 'sink_type', 'sink_name')
            if action == DeliveryAction.DLQ and self._dlq_sink:
                await self._dlq_sink.send(error, partition_id=partition_id)
            return action

        await self._sink_manager.deliver_all(
            result,
            on_delivery_error=_on_delivery_error,
            partition_id=partition_id,
            recorder=self._recorder,
        )

        if self._recorder:
            for payload in result.kafka:
                self._recorder.record_produced(payload, source_partition=partition_id)

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        if self._consumer:
            await self._consumer.commit({partition_id: offset})
        if self._recorder:
            self._recorder.record_committed(partition_id, offset)

    def _handle_signal(self) -> None:
        """Handle shutdown signals."""
        logger.info('shutdown_signal_received', category='lifecycle')
        self._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown: cancel periodic tasks, drain executors, commit offsets, close sinks."""
        log = logger.bind(worker_id=self._worker_id)
        await log.ainfo('drakkar_shutting_down', category='lifecycle')

        # cancel periodic tasks
        for task in self._periodic_tasks:
            task.cancel()
        if self._periodic_tasks:
            await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
            self._periodic_tasks.clear()

        for processor in list(self._processors.values()):
            processor._running = False

        await log.ainfo('draining_executors', category='lifecycle', timeout=5)
        try:
            await asyncio.wait_for(self._drain_all_processors(), timeout=self._config.executor.drain_timeout_seconds)
            await log.ainfo('executors_drained', category='lifecycle')
        except TimeoutError:
            await log.awarning('drain_timeout', category='lifecycle', msg='some executors did not finish in 5s')

        for processor in list(self._processors.values()):
            committable = processor.offset_tracker.committable()
            if committable is not None and self._consumer:
                try:
                    await self._consumer.commit({processor.partition_id: committable})
                except Exception as e:
                    await log.awarning(
                        'final_commit_failed',
                        category='kafka',
                        partition=processor.partition_id,
                        error=str(e),
                    )

        for processor in list(self._processors.values()):
            await processor.stop()
        self._processors.clear()

        if self._recorder:
            await self._recorder.stop()

        if self._debug_server:
            await self._debug_server.stop()

        # close all sinks and DLQ
        await self._sink_manager.close_all()
        if self._dlq_sink:
            await self._dlq_sink.close()

        if self._consumer:
            await self._consumer.close()

        await log.ainfo('drakkar_stopped', category='lifecycle')

    async def _drain_all_processors(self) -> None:
        """Wait for all partition processors to finish queued + in-flight work."""
        drain_tasks = [
            processor.drain()
            for processor in self._processors.values()
            if processor.queue_size > 0 or processor.offset_tracker.has_pending() or processor.inflight_count > 0
        ]
        if drain_tasks:
            await asyncio.gather(*drain_tasks)
