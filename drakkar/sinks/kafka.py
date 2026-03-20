"""Kafka sink — produces messages to a Kafka topic.

Uses confluent_kafka's AIOProducer for native asyncio integration.
Serializes each KafkaPayload's data field via model_dump_json().encode()
and produces it with the payload's key to the configured topic.
"""

import asyncio
import time

import structlog
from confluent_kafka.aio import AIOProducer

from drakkar.config import KafkaSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import KafkaPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class KafkaSink(BaseSink):
    """Produces messages to a Kafka topic.

    Each KafkaPayload is serialized:
        - value = payload.data.model_dump_json().encode()
        - key = payload.key (passthrough bytes)

    If the sink config has empty brokers, falls back to the
    shared kafka.brokers from the main Kafka config.
    """

    sink_type = 'kafka'

    def __init__(self, name: str, config: KafkaSinkConfig, brokers_fallback: str = '') -> None:
        super().__init__(name)
        self._config = config
        self._brokers = config.brokers or brokers_fallback
        self._producer: AIOProducer | None = None

    @property
    def topic(self) -> str:
        """The Kafka topic this sink produces to."""
        return self._config.topic

    async def connect(self) -> None:
        """Create the AIOProducer connection."""
        self._producer = AIOProducer({'bootstrap.servers': self._brokers})
        await logger.ainfo(
            'kafka_sink_connected',
            category='sink',
            sink_name=self._name,
            topic=self._config.topic,
            brokers=self._brokers,
        )

    async def deliver(self, payloads: list[KafkaPayload]) -> None:  # type: ignore[override]
        """Produce all payloads to the Kafka topic.

        Submits all messages, flushes to push them into flight,
        then awaits all delivery futures concurrently.
        """
        if not payloads or not self._producer:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        futures = []
        try:
            for payload in payloads:
                value = payload.data.model_dump_json().encode()
                f = await self._producer.produce(
                    topic=self._config.topic,
                    key=payload.key,
                    value=value,
                )
                futures.append(f)
            await self._producer.flush()
            await asyncio.gather(*futures)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    async def close(self) -> None:
        """Flush pending messages and close the producer."""
        if self._producer:
            try:
                await self._producer.close()
            except Exception as e:
                await logger.awarning(
                    'kafka_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._producer = None
