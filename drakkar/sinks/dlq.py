"""Dead letter queue sink — writes failed delivery payloads to a Kafka topic.

When a sink delivery fails and the on_delivery_error hook returns DLQ,
the framework uses this sink to persist the failed payloads with error
metadata so they can be investigated and reprocessed later.
"""

import json
import time

import structlog
from confluent_kafka.aio import AIOProducer
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
