"""Main Drakkar application — wires all components together."""

import asyncio
import signal
import time
from collections.abc import Coroutine
from pathlib import Path

import structlog

from drakkar import __version__
from drakkar.config import DrakkarConfig, load_config
from drakkar.consumer import KafkaConsumer
from drakkar.db import DBWriter
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.logging import setup_logging
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    messages_produced,
    start_metrics_server,
    total_queued,
    worker_info,
)
from drakkar.models import CollectResult
from drakkar.partition import PartitionProcessor
from drakkar.producer import KafkaProducer
from drakkar.recorder import EventRecorder

logger = structlog.get_logger()


class DrakkarApp:
    """Main application that orchestrates Kafka consumption, subprocess
    execution, and result production.
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
        self._worker_id = worker_id or f'drakkar-{id(self):x}'
        self._start_time = time.monotonic()

        self._executor_pool: ExecutorPool | None = None
        self._consumer: KafkaConsumer | None = None
        self._producer: KafkaProducer | None = None
        self._db_writer: DBWriter | None = None
        self._recorder: EventRecorder | None = None
        self._debug_server = None

        self._processors: dict[int, PartitionProcessor] = {}
        self._running = False
        self._paused = False

    @property
    def config(self) -> DrakkarConfig:
        return self._config

    @property
    def processors(self) -> dict[int, PartitionProcessor]:
        return self._processors

    @property
    def recorder(self) -> EventRecorder | None:
        return self._recorder

    def run(self) -> None:
        """Start the application. Blocks until shutdown."""
        setup_logging(
            self._config.logging,
            worker_id=self._worker_id,
            consumer_group=self._config.kafka.consumer_group,
            version=__version__,
        )
        asyncio.run(self._async_run())

    async def _async_run(self) -> None:
        log = logger.bind(worker_id=self._worker_id)

        self._config = await self._handler.on_startup(self._config)

        await log.ainfo('drakkar_starting', category='lifecycle')

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

        if self._config.debug.enabled:
            self._recorder = EventRecorder(self._config.debug)
            await self._recorder.start()

            from drakkar.debug_server import DebugServer

            self._debug_server = DebugServer(
                config=self._config.debug,
                recorder=self._recorder,
                app=self,
            )
            await self._debug_server.start()

        self._loop = asyncio.get_running_loop()
        self._consumer = KafkaConsumer(
            config=self._config.kafka,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            loop=self._loop,
        )
        self._producer = KafkaProducer(config=self._config.kafka)

        self._db_writer = DBWriter(config=self._config.postgres)
        await self._db_writer.connect()

        await self._handler.on_ready(self._config, self._db_writer.pool)

        self._consumer.subscribe()
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

    async def _poll_loop(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume.

        Pauses all partitions when total buffered messages exceed
        high_watermark (2x max_workers). Resumes when they drop below
        low_watermark (max_workers / 2). This ensures we only hold
        enough data in memory to keep the executor pool busy.
        """
        max_workers = self._config.executor.max_workers
        high_watermark = max_workers * 32
        low_watermark = max(1, max_workers * 4)

        while self._running:
            total = self._total_queued()
            total_queued.set(total)

            if self._paused and total <= low_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    self._consumer.resume(partition_ids)
                    self._paused = False
                    backpressure_active.set(0)

            if not self._paused and total >= high_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    self._consumer.pause(partition_ids)
                    self._paused = True
                    backpressure_active.set(1)

            # always call poll_batch for heartbeats — paused partitions
            # return no messages but the consumer stays in the group
            messages = await self._consumer.poll_batch()
            for msg in messages:
                processor = self._processors.get(msg.partition)
                if processor:
                    processor.enqueue(msg)

            if not messages:
                await asyncio.sleep(0.05)

    def _on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        if self._recorder:
            self._recorder.record_assigned(partition_ids)
        for pid in partition_ids:
            if pid not in self._processors:
                processor = PartitionProcessor(
                    partition_id=pid,
                    handler=self._handler,
                    executor_pool=self._executor_pool,
                    window_size=self._config.executor.window_size,
                    on_collect=self._handle_collect,
                    on_commit=self._handle_commit,
                    recorder=self._recorder,
                )
                self._processors[pid] = processor
                processor.start()

        assigned_partitions.set(len(self._processors))
        _task = asyncio.ensure_future(self._safe_call(self._handler.on_assign(partition_ids)))

    def _on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation."""
        if self._recorder:
            self._recorder.record_revoked(partition_ids)
        for pid in partition_ids:
            processor = self._processors.pop(pid, None)
            if processor:
                _task = asyncio.ensure_future(self._stop_processor(processor))

        assigned_partitions.set(len(self._processors))
        _task = asyncio.ensure_future(self._safe_call(self._handler.on_revoke(partition_ids)))

    async def _safe_call(self, coro: Coroutine) -> None:
        """Run a coroutine and log any exception instead of leaving it unretrieved."""
        try:
            await coro
        except Exception as e:
            logger.warning('async_callback_failed', category='lifecycle', error=str(e))

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        """Drain in-flight tasks (up to 5s), commit final offsets, then stop."""
        try:
            processor._running = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=5.0)
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
        """Process collect results: produce messages + write DB rows."""
        if result.output_messages and self._producer:
            await self._producer.produce_batch(result.output_messages)
            messages_produced.inc(len(result.output_messages))
            if self._recorder:
                for msg in result.output_messages:
                    self._recorder.record_produced(msg, source_partition=partition_id)

        if result.db_rows and self._db_writer:
            await self._db_writer.write(result.db_rows)

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        try:
            if self._consumer:
                await self._consumer.commit({partition_id: offset})
            if self._recorder:
                self._recorder.record_committed(partition_id, offset)
        except Exception as e:
            logger.warning(
                'commit_failed',
                category='kafka',
                partition=partition_id,
                offset=offset,
                error=str(e),
            )

    def _handle_signal(self) -> None:
        """Handle shutdown signals."""
        logger.info('shutdown_signal_received', category='lifecycle')
        self._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown: flush recorder, drain executors (up to 5s),
        commit offsets, disconnect from Kafka and DB.
        """
        log = logger.bind(worker_id=self._worker_id)
        await log.ainfo('drakkar_shutting_down', category='lifecycle')

        # 1. stop accepting new messages — processors stop polling their queues
        for processor in list(self._processors.values()):
            processor._running = False

        # 2. give in-flight executors up to 5 seconds to finish
        await log.ainfo('draining_executors', category='lifecycle', timeout=5)
        try:
            await asyncio.wait_for(self._drain_all_processors(), timeout=5.0)
            await log.ainfo('executors_drained', category='lifecycle')
        except TimeoutError:
            await log.awarning(
                'drain_timeout', category='lifecycle', msg='some executors did not finish in 5s'
            )

        # 3. commit any remaining offsets
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

        # 4. cancel processor tasks
        for processor in list(self._processors.values()):
            await processor.stop()
        self._processors.clear()

        # 5. flush recorder and stop debug server
        if self._recorder:
            await self._recorder.stop()

        if self._debug_server:
            await self._debug_server.stop()

        # 6. flush producer and disconnect
        if self._producer:
            await self._producer.flush()
            self._producer.close()

        if self._consumer:
            self._consumer.close()

        if self._db_writer:
            await self._db_writer.close()

        await log.ainfo('drakkar_stopped', category='lifecycle')

    async def _drain_all_processors(self) -> None:
        """Wait for all partition processors to finish queued + in-flight work."""
        drain_tasks = [
            processor.drain()
            for processor in self._processors.values()
            if processor.queue_size > 0
            or processor.offset_tracker.has_pending()
            or processor.inflight_count > 0
        ]
        if drain_tasks:
            await asyncio.gather(*drain_tasks)
