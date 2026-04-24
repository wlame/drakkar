"""Dead letter queue sink — writes failed delivery payloads to a Kafka topic.

When a sink delivery fails and the on_delivery_error hook returns DLQ,
the framework uses this sink to persist the failed payloads with error
metadata so they can be investigated and reprocessed later.
"""

import json
import time
import uuid
from collections.abc import AsyncIterator

import structlog
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.aio import AIOConsumer, AIOProducer
from pydantic import BaseModel

from drakkar.metrics import dlq_send_failures, sink_dlq_messages
from drakkar.models import DeliveryError
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

logger = structlog.get_logger()


class DLQMessage:
    """Wraps a failed payload with error metadata for the dead letter queue.

    Serialized as JSON with fields:
        - original_payload: the failed payload serialized as JSON string
        - sink_name: which sink failed
        - sink_type: type of the failed sink
        - error: error message string
        - timestamp: when the failure occurred (unix epoch)
        - partition: source partition the message came from
        - attempt_count: how many delivery attempts were made
    """

    def __init__(
        self,
        delivery_error: DeliveryError,
        partition_id: int,
        attempt_count: int = 1,
    ) -> None:
        self.delivery_error = delivery_error
        self.partition_id = partition_id
        self.attempt_count = attempt_count

    def serialize(self) -> bytes:
        """Serialize the DLQ message to JSON bytes."""
        payloads_json = []
        for p in self.delivery_error.payloads:
            try:
                payloads_json.append(p.model_dump_json())
            except Exception:
                payloads_json.append(str(p))

        msg = {
            'original_payloads': payloads_json,
            'sink_name': self.delivery_error.sink_name,
            'sink_type': self.delivery_error.sink_type,
            'error': self.delivery_error.error,
            'timestamp': time.time(),
            'partition': self.partition_id,
            'attempt_count': self.attempt_count,
        }
        return json.dumps(msg).encode()


class DLQSink(BaseSink[BaseModel]):
    """Produces failed delivery payloads to a dead letter queue Kafka topic.

    Used internally by the framework when on_delivery_error returns DLQ.
    Not registered in the SinkManager — managed separately by DrakkarApp.
    """

    sink_type = 'dlq'

    # Same rationale as ``KafkaSink.idempotent`` — the underlying
    # ``AIOProducer`` is not configured with ``enable.idempotence=true``,
    # so a retried produce can land the same DLQ record twice. Duplicate
    # DLQ entries are harmless (operators inspect them manually), but we
    # still default to ``False`` because DLQSink is driven through the
    # dedicated ``send()`` path, not through the normal
    # ``deliver()`` retry loop — SinkManager never retries DLQSink
    # automatically. Kept explicit here for clarity of the contract.
    idempotent = False

    def __init__(self, topic: str, brokers: str) -> None:
        super().__init__('dlq')
        self._topic = topic
        self._brokers = brokers
        self._producer: AIOProducer | None = None

    @property
    def topic(self) -> str:
        """The Kafka topic this DLQ writes to."""
        return self._topic

    async def connect(self) -> None:
        """Create the AIOProducer for the DLQ topic."""
        self._producer = AIOProducer({'bootstrap.servers': self._brokers})
        await logger.ainfo(
            'dlq_sink_connected',
            category='sink',
            topic=self._topic,
            brokers=redact_url(self._brokers),
        )

    async def deliver(self, payloads: list[BaseModel]) -> None:
        """Not used directly — use send() instead."""
        raise NotImplementedError('Use DLQSink.send() instead of deliver()')

    async def send(
        self,
        delivery_error: DeliveryError,
        partition_id: int,
        attempt_count: int = 1,
    ) -> None:
        """Write a failed delivery to the DLQ topic.

        Wraps the error in a DLQMessage with metadata and produces it to the
        configured DLQ Kafka topic. Does NOT raise on failure — the DLQ is
        the last resort and propagating the exception would cause the
        partition pipeline to stall with no safe recovery. Failures are
        instead reported via:
          - the ``dlq_send_failures`` Prometheus counter (alert on this!)
          - a CRITICAL-severity structured log entry with full context

        Operators MUST configure alerting on ``drakkar_dlq_send_failures_total``
        — a non-zero value means messages are being silently lost.
        """
        if self._producer is None:
            await logger.awarning('dlq_send_skipped_not_connected', category='sink')
            return

        msg = DLQMessage(
            delivery_error=delivery_error,
            partition_id=partition_id,
            attempt_count=attempt_count,
        )
        serialized = msg.serialize()
        try:
            # Produce enqueues the message and returns a future that resolves
            # on the delivery report. Awaiting the future alone is sufficient
            # — the AIOProducer flushes its internal queue as needed. An
            # explicit flush() would only mask delivery-report failures
            # behind a generic "flush failed" exception with less context.
            future = await self._producer.produce(
                topic=self._topic,
                value=serialized,
            )
            await future
            sink_dlq_messages.inc()
            await logger.ainfo(
                'dlq_message_sent',
                category='sink',
                sink_name=delivery_error.sink_name,
                sink_type=delivery_error.sink_type,
                partition=partition_id,
                payload_count=len(delivery_error.payloads),
            )
        except Exception as e:
            dlq_send_failures.inc()
            # CRITICAL: the DLQ itself has failed after the original sink
            # already failed. These payloads are effectively lost until the
            # operator intervenes. Include full context so alerting tools
            # surface enough to act on without a dashboard dive.
            await logger.acritical(
                'dlq_send_failed',
                category='sink',
                error=str(e),
                error_type=type(e).__name__,
                dlq_topic=self._topic,
                source_sink_type=delivery_error.sink_type,
                source_sink_name=delivery_error.sink_name,
                partition=partition_id,
                payload_count=len(delivery_error.payloads),
                payload_bytes=len(serialized),
                attempt_count=attempt_count,
                action='ALERT: message lost — investigate DLQ producer/broker',
            )

    async def close(self) -> None:
        """Flush and close the DLQ producer."""
        if self._producer is not None:
            try:
                await self._producer.close()
            except Exception as e:
                await logger.awarning(
                    'dlq_sink_close_error',
                    category='sink',
                    error=str(e),
                )
            self._producer = None


async def read_dlq_entries(
    topic: str,
    brokers: str,
    limit: int | None = None,
    poll_timeout: float = 2.0,
    idle_polls_before_stop: int = 2,
) -> AsyncIterator[dict]:
    """Consume DLQ entries from the DLQ Kafka topic, from the beginning.

    Opens a consumer with a unique, throwaway consumer group so every invocation
    reads the entire topic from offset 0. The iteration stops when the broker
    reports no new messages for ``idle_polls_before_stop`` consecutive polls —
    this is the standard "drained" heuristic for one-shot readers since Kafka
    has no real "end of topic" signal beyond _PARTITION_EOF events (which
    confluent-kafka emits only when the client is configured to surface them).

    Each yielded value is the parsed JSON payload written by ``DLQSink.send()``
    (see the ``DLQMessage.serialize()`` docstring for the field layout).

    Yields at most ``limit`` entries when specified; yields indefinitely until
    drained otherwise. The caller is expected to iterate in an async for loop
    and handle KeyboardInterrupt / early termination itself.

    This function is intentionally minimal — it exists so ad-hoc replay /
    inspection tools (e.g. ``scripts/replay_dlq.py``) do not need to know
    about confluent-kafka wiring. It is NOT part of the hot-path framework
    code and is not exercised by the worker runtime.
    """
    group_id = f'drakkar-dlq-reader-{uuid.uuid4().hex[:12]}'
    consumer = AIOConsumer(
        {
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        }
    )
    try:
        await consumer.subscribe([topic])

        yielded = 0
        idle_polls = 0
        while True:
            if limit is not None and yielded >= limit:
                return
            raw_messages = await consumer.consume(
                num_messages=100,
                timeout=poll_timeout,
            )
            if not raw_messages:
                idle_polls += 1
                if idle_polls >= idle_polls_before_stop:
                    return
                continue
            idle_polls = 0
            for msg in raw_messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    await logger.awarning(
                        'dlq_reader_error',
                        category='sink',
                        error=str(msg.error()),
                    )
                    continue
                if limit is not None and yielded >= limit:
                    return
                try:
                    entry = json.loads(msg.value())
                except json.JSONDecodeError as e:
                    await logger.awarning(
                        'dlq_reader_invalid_json',
                        category='sink',
                        error=str(e),
                        offset=msg.offset(),
                    )
                    continue
                entry['_kafka_offset'] = msg.offset()
                entry['_kafka_partition'] = msg.partition()
                yield entry
                yielded += 1
    finally:
        try:
            await consumer.close()
        except Exception as e:
            await logger.awarning(
                'dlq_reader_close_error',
                category='sink',
                error=str(e),
            )


# Keep the symbol available for consumers that want to build their own
# TopicPartition references when extending the reader (e.g. seeking to an
# offset). Intentionally re-exported from the ``confluent_kafka`` shim so
# that operator scripts need not import from ``confluent_kafka`` directly.
__all__ = ['DLQMessage', 'DLQSink', 'TopicPartition', 'read_dlq_entries']
