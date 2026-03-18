"""Kafka producer wrapper for Drakkar framework.

Uses confluent_kafka's AIOProducer for native asyncio integration.
AIOProducer handles delivery callback polling internally — no manual
poll() loop needed.
"""

import asyncio
import time

import structlog
from confluent_kafka.aio import AIOProducer

from drakkar.config import KafkaConfig
from drakkar.metrics import produce_duration, producer_errors
from drakkar.models import OutputMessage

logger = structlog.get_logger()


class KafkaProducer:
    """Wraps confluent_kafka.AIOProducer for native async produce."""

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer = AIOProducer(
            {
                'bootstrap.servers': config.brokers,
            }
        )

    async def produce(self, message: OutputMessage) -> None:
        """Produce a single message to the target topic."""
        start = time.monotonic()
        try:
            delivery_future = await self._producer.produce(
                topic=self._config.target_topic,
                key=message.key,
                value=message.value,
            )
            await delivery_future
        except Exception:
            producer_errors.inc()
            raise
        produce_duration.observe(time.monotonic() - start)

    async def produce_batch(self, messages: list[OutputMessage]) -> None:
        """Produce multiple messages concurrently.

        Submits all messages, flushes to ensure they're in flight,
        then awaits all delivery futures concurrently.
        """
        if not messages:
            return
        start = time.monotonic()
        futures = []
        for msg in messages:
            try:
                f = await self._producer.produce(
                    topic=self._config.target_topic,
                    key=msg.key,
                    value=msg.value,
                )
                futures.append(f)
            except Exception:
                producer_errors.inc()
                raise
        # flush buffered messages so deliveries are in flight
        await self._producer.flush()
        # wait for all delivery confirmations
        await asyncio.gather(*futures)
        produce_duration.observe(time.monotonic() - start)

    async def flush(self, timeout: float = 10.0) -> None:
        """Flush all pending messages."""
        await self._producer.flush(timeout)

    async def close(self) -> None:
        """Flush and close the producer."""
        await self._producer.close()
