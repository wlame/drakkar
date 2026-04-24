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
from drakkar.utils import redact_url

logger = structlog.get_logger()


class KafkaSink(BaseSink[KafkaPayload]):
    """Produces messages to a Kafka topic.

    Each KafkaPayload is serialized:
        - value = payload.data.model_dump_json().encode()
        - key = payload.key (passthrough bytes)

    If the sink config has empty brokers, falls back to the
    shared kafka.brokers from the main Kafka config.
    """

    sink_type = 'kafka'

    # The default ``AIOProducer`` configuration does NOT enable
    # ``enable.idempotence=true`` (see ``connect()`` below). Without the
    # broker-side deduplication that flag unlocks, a retried produce can
    # land the same logical message twice under broker-failover / timeout
    # scenarios. We therefore keep ``idempotent=False`` here as the safe
    # default. Operators who configure the underlying producer with
    # ``enable.idempotence=true`` + a stable message key + ``acks=all``
    # can subclass ``KafkaSink`` and flip this flag to opt into automatic
    # transient-error retry by the SinkManager.
    idempotent = False

    def __init__(self, name: str, config: KafkaSinkConfig, brokers_fallback: str = '') -> None:
        super().__init__(name, ui_url=config.ui_url)
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
            brokers=redact_url(self._brokers),
        )

    async def deliver(self, payloads: list[KafkaPayload]) -> None:
        """Produce all payloads to the Kafka topic.

        Submits all messages, flushes to push them into flight,
        then awaits all delivery futures and verifies broker acknowledgement.
        """
        if not payloads or self._producer is None:
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

            remaining = await self._producer.flush()
            if remaining and remaining > 0:
                raise RuntimeError(
                    f'Kafka flush incomplete: {remaining} message(s) still in queue '
                    f'(topic={self._config.topic!r}, sink={self._name!r})'
                )
            results = await asyncio.gather(*futures)
            for i, result in enumerate(results):
                if result is None:
                    raise RuntimeError(
                        f'Kafka delivery future resolved to None for message {i} '
                        f'(topic={self._config.topic!r}, sink={self._name!r})'
                    )
                if hasattr(result, 'error') and result.error() is not None:
                    raise RuntimeError(
                        f'Kafka delivery error for message {i}: {result.error()} '
                        f'(topic={self._config.topic!r}, sink={self._name!r})'
                    )

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    async def close(self) -> None:
        """Flush pending messages and close the producer."""
        if self._producer is not None:
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
