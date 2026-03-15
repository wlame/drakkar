"""Main Drakkar application — wires all components together."""

import asyncio
import signal
import time
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
    messages_produced,
    start_metrics_server,
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
        worker_id: str = "",
    ):
        if config is not None:
            self._config = config
        else:
            self._config = load_config(config_path)

        self._handler = handler
        self._worker_id = worker_id or f"drakkar-{id(self):x}"
        self._start_time = time.monotonic()

        self._executor_pool: ExecutorPool | None = None
        self._consumer: KafkaConsumer | None = None
        self._producer: KafkaProducer | None = None
        self._db_writer: DBWriter | None = None
        self._recorder: EventRecorder | None = None
        self._debug_server = None

        self._processors: dict[int, PartitionProcessor] = {}
        self._running = False

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
        setup_logging(self._config.logging, worker_id=self._worker_id)
        asyncio.run(self._async_run())

    async def _async_run(self) -> None:
        log = logger.bind(worker_id=self._worker_id)

        self._config = await self._handler.on_startup(self._config)

        await log.ainfo("drakkar_starting", config=self._config.model_dump())

        self._executor_pool = ExecutorPool(
            binary_path=self._config.executor.binary_path,
            max_workers=self._config.executor.max_workers,
            task_timeout_seconds=self._config.executor.task_timeout_seconds,
        )

        start_metrics_server(self._config.metrics)
        worker_info.info({
            'worker_id': self._worker_id,
            'version': __version__,
            'consumer_group': self._config.kafka.consumer_group,
        })

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

        self._consumer = KafkaConsumer(
            config=self._config.kafka,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._producer = KafkaProducer(config=self._config.kafka)

        self._db_writer = DBWriter(config=self._config.postgres)
        await self._db_writer.connect()

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

    async def _poll_loop(self) -> None:
        """Main polling loop — distributes messages to partition processors."""
        while self._running:
            messages = await self._consumer.poll_batch()
            for msg in messages:
                processor = self._processors.get(msg.partition)
                if processor:
                    processor.enqueue(msg)

            if not messages:
                await asyncio.sleep(0.01)

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
        asyncio.ensure_future(self._handler.on_assign(partition_ids))

    def _on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation."""
        if self._recorder:
            self._recorder.record_revoked(partition_ids)
        for pid in partition_ids:
            processor = self._processors.pop(pid, None)
            if processor:
                asyncio.ensure_future(self._stop_processor(processor))

        assigned_partitions.set(len(self._processors))
        asyncio.ensure_future(self._handler.on_revoke(partition_ids))

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        committable = processor.offset_tracker.committable()
        if committable is not None and self._consumer:
            await self._consumer.commit({processor.partition_id: committable})
            processor.offset_tracker.acknowledge_commit(committable)
        await processor.stop()

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
        if self._consumer:
            await self._consumer.commit({partition_id: offset})
        if self._recorder:
            self._recorder.record_committed(partition_id, offset)

    def _handle_signal(self) -> None:
        """Handle shutdown signals."""
        logger.info("shutdown_signal_received")
        self._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown sequence."""
        log = logger.bind(worker_id=self._worker_id)
        await log.ainfo("drakkar_shutting_down")

        for processor in list(self._processors.values()):
            await processor.stop()
        self._processors.clear()

        if self._debug_server:
            await self._debug_server.stop()

        if self._recorder:
            await self._recorder.stop()

        if self._producer:
            await self._producer.flush()
            self._producer.close()

        if self._consumer:
            self._consumer.close()

        if self._db_writer:
            await self._db_writer.close()

        await log.ainfo("drakkar_stopped")
