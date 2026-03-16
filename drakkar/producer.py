"""Kafka producer wrapper for Drakkar framework."""

import asyncio
import time

import structlog
from confluent_kafka import KafkaException, Producer

from drakkar.config import KafkaConfig
from drakkar.metrics import produce_duration, producer_errors
from drakkar.models import OutputMessage

logger = structlog.get_logger()


class KafkaProducer:
    """Wraps confluent_kafka.Producer with async produce and delivery tracking."""

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer = Producer(
            {
                'bootstrap.servers': config.brokers,
            }
        )

    async def produce(self, message: OutputMessage) -> None:
        """Produce a single message to the target topic.

        Uses a delivery callback and asyncio.Future for async waiting.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        start = time.monotonic()

        def delivery_callback(err: object, msg: object) -> None:
            if err:
                producer_errors.inc()
                if not future.done():
                    loop.call_soon_threadsafe(
                        future.set_exception,
                        KafkaException(err),
                    )
            else:
                if not future.done():
                    loop.call_soon_threadsafe(future.set_result, None)

        self._producer.produce(
            topic=self._config.target_topic,
            key=message.key,
            value=message.value,
            callback=delivery_callback,
        )
        self._producer.poll(0)

        await future
        produce_duration.observe(time.monotonic() - start)

    async def produce_batch(self, messages: list[OutputMessage]) -> None:
        """Produce multiple messages concurrently."""
        if not messages:
            return
        await asyncio.gather(*[self.produce(msg) for msg in messages])

    async def flush(self, timeout: float = 10.0) -> None:
        """Flush all pending messages."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._producer.flush, timeout)

    def close(self) -> None:
        """Flush and close the producer."""
        self._producer.flush(timeout=30.0)
