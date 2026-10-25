"""Dead letter queue sink — writes DLQ records to a Kafka topic.

A DLQ record is the failed source message itself (key and value unchanged)
plus ``dk-`` headers that say where and why it failed; see
``drakkar.sinks.dlq_record``. The framework writes one when a message cannot
be parsed, a task or a delivery ends in the DLQ, or a sink's circuit is open.
"""

import asyncio
import functools
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime

import structlog
from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.aio import AIOConsumer
from pydantic import BaseModel, ConfigDict, Field

from drakkar.kafka_security import KafkaSecurityConfig, merge_client_config
from drakkar.metrics import dlq_send_failures, sink_dlq_messages
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq_record import DLQRecord, Header, render_headers
from drakkar.sinks.kafka_delivery import settle_budget
from drakkar.utils import redact_url

logger = structlog.get_logger()

# Mirrors ``DLQConfig.flush_timeout_seconds``' default, for a DLQSink built
# outside a ``DrakkarApp`` (tests, tools).
DEFAULT_FLUSH_TIMEOUT_SECONDS = 30.0

DeliveryReport = tuple[KafkaError | None, Message | None]


class DLQEntry(BaseModel):
    """One record read back from the DLQ topic."""

    model_config = ConfigDict(frozen=True)

    key: bytes | None = Field(description='The record key: the source message key, None when it had none.')
    value: bytes = Field(description='The record value: the source message value, unchanged.')
    headers: list[Header] = Field(description='The record headers in their order: source headers, then dk- headers.')
    partition: int = Field(description='The DLQ topic partition the record was read from.')
    offset: int = Field(description='The DLQ topic offset of the record.')


class DLQSink(BaseSink[BaseModel]):
    """Produces DLQ records to the dead letter queue Kafka topic.

    Used internally by the framework. Not registered in the SinkManager —
    managed separately by DrakkarApp.
    """

    sink_type = 'dlq'

    # DLQSink inherits ``BaseSink.idempotent = False``. The manager's
    # retry loop never runs against this sink because DLQSink is driven
    # through its dedicated ``send()`` path (not the normal ``deliver()``
    # loop) and does NOT enable broker-side dedup in the producer config.
    # Duplicate DLQ entries are harmless (operators inspect them manually)
    # but we keep the conservative default so nothing ever retries.

    def __init__(
        self,
        topic: str,
        brokers: str,
        *,
        worker: str,
        instance: str,
        security: KafkaSecurityConfig | None = None,
        client_config: Mapping[str, str] | None = None,
        flush_timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__('dlq')
        self._topic = topic
        self._brokers = brokers
        # Stamped on every record as dk-worker and dk-instance: the sink is
        # the one place that knows which worker writes the record.
        self._worker = worker
        self._instance = instance
        # Bound on the per-write flush. Unbounded, librdkafka waits out
        # ``message.timeout.ms`` (300s) against a wedged broker while holding
        # an executor thread — and the DLQ is the last resort, so a write
        # that blocks that long stalls the partition it was meant to rescue.
        self._flush_timeout_seconds = flush_timeout_seconds
        # Already resolved by the caller (DrakkarApp._build_dlq), which
        # applies the same empty-brokers-inherits rule the Kafka sinks use.
        self._security = security or KafkaSecurityConfig()
        self._client_config = dict(client_config or {})
        # A plain (sync) producer: the asyncio producer of the client cannot
        # send headers. Its blocking calls run in the loop's executor.
        self._producer: Producer | None = None

    @property
    def topic(self) -> str:
        """The Kafka topic this DLQ writes to."""
        return self._topic

    async def connect(self) -> None:
        """Create the producer for the DLQ topic."""
        self._producer = Producer(
            merge_client_config({'bootstrap.servers': self._brokers}, self._security, self._client_config)
        )
        await logger.ainfo(
            'dlq_sink_connected',
            category='sink',
            topic=self._topic,
            brokers=redact_url(self._brokers),
            security=self._security.describe(),
        )

    async def deliver(self, payloads: list[BaseModel]) -> None:
        """Not used directly — use send() instead."""
        raise NotImplementedError('Use DLQSink.send() instead of deliver()')

    def _stamp(self, record: DLQRecord) -> DLQRecord:
        """Return ``record`` with this worker's name, instance and the write time set."""
        return record.model_copy(
            update={'worker': self._worker, 'instance': self._instance, 'failed_at': datetime.now(UTC)}
        )

    async def send(self, record: DLQRecord, partition_id: int) -> bool:
        """Write one DLQ record to the DLQ topic.

        Returns ``True`` when the DLQ broker confirmed the write, ``False``
        otherwise. Does NOT raise — the caller decides what a failed DLQ
        write means for its pipeline (the ``dlq.on_send_failure`` strategy).
        Failures are additionally reported via:
          - the ``dlq_send_failures`` Prometheus counter (alert on this!)
          - a CRITICAL-severity structured log entry with full context

        Operators MUST configure alerting on ``drakkar_dlq_send_failures_total``
        — a non-zero value means records are being lost or offsets stall
        behind a broken DLQ.
        """
        producer = self._producer
        if producer is None:
            await logger.awarning('dlq_send_skipped_not_connected', category='sink')
            return False

        stamped = self._stamp(record)
        headers = render_headers(stamped)
        loop = asyncio.get_running_loop()
        report: asyncio.Future[DeliveryReport] = loop.create_future()
        on_delivery = _delivery_callback(loop, report)
        try:
            deadline = time.monotonic() + self._flush_timeout_seconds
            # The flush is per write: the producer buffers for up to a second
            # otherwise, and the DLQ sits on the hot failure paths, so that
            # second would throttle the whole partition.
            remaining = await loop.run_in_executor(
                None,
                functools.partial(
                    _produce_and_flush,
                    producer=producer,
                    topic=self._topic,
                    record=stamped,
                    headers=headers,
                    on_delivery=on_delivery,
                    flush_timeout_seconds=self._flush_timeout_seconds,
                ),
            )

            # ``remaining`` is producer-wide: librdkafka counts every message
            # still queued on this producer, and the DLQ producer serves the
            # whole worker. Treating it as this write's verdict would report a
            # record the broker accepted as lost because some other write is
            # stuck. This write is judged by its own delivery report; the
            # count only explains a timeout.
            _settled, pending = await asyncio.wait([report], timeout=settle_budget(deadline))
            if pending:
                await logger.acritical(
                    'dlq_flush_incomplete',
                    category='sink',
                    dlq_topic=self._topic,
                    remaining=remaining,
                    flush_timeout_seconds=self._flush_timeout_seconds,
                    action='ALERT: DLQ write not acknowledged in time — offsets will stall',
                )
                dlq_send_failures.inc()
                return False
            delivery_error, message = report.result()
            # A failed delivery is reported in the callback, not raised, so
            # the callback firing is not on its own a confirmation.
            failure = _delivery_failure(delivery_error, message)
            if failure is not None:
                await logger.acritical(
                    'dlq_delivery_report_failed',
                    category='sink',
                    dlq_topic=self._topic,
                    error=failure,
                    action='ALERT: DLQ write not confirmed — offsets will stall',
                )
                dlq_send_failures.inc()
                return False
            sink_dlq_messages.inc()
            logger.info(
                'dlq_message_sent',
                category='sink',
                stage=stamped.stage,
                sink=stamped.sink,
                partition=partition_id,
                value_bytes=len(stamped.value),
            )
            return True
        except Exception as e:
            dlq_send_failures.inc()
            # CRITICAL: the DLQ itself has failed after the original failure.
            # Include full context so alerting tools surface enough to act on
            # without a dashboard dive.
            await logger.acritical(
                'dlq_send_failed',
                category='sink',
                error=str(e),
                error_type=type(e).__name__,
                dlq_topic=self._topic,
                stage=stamped.stage,
                sink=stamped.sink,
                partition=partition_id,
                value_bytes=len(stamped.value),
                attempts=stamped.attempts,
                action='ALERT: DLQ record not written — investigate DLQ producer/broker',
            )
            return False

    async def close(self) -> None:
        """Flush and drop the DLQ producer."""
        producer = self._producer
        if producer is None:
            return
        self._producer = None
        try:
            await asyncio.get_running_loop().run_in_executor(None, producer.flush, self._flush_timeout_seconds)
        except Exception as e:
            await logger.awarning(
                'dlq_sink_close_error',
                category='sink',
                error=str(e),
            )


def _delivery_callback(
    loop: asyncio.AbstractEventLoop, report: asyncio.Future[DeliveryReport]
) -> Callable[[KafkaError | None, Message | None], None]:
    """Build the producer's delivery callback for one write.

    librdkafka calls it from inside ``flush()``/``poll()``, so on an executor
    thread, never on the loop: it may only hand the report over with
    ``call_soon_threadsafe``.
    """

    def settle(error: KafkaError | None, message: Message | None) -> None:
        # A send that timed out leaves the future pending, so a late report
        # still settles it, harmlessly. librdkafka serves each report once;
        # the check keeps a second call from raising inside the loop.
        if not report.done():
            report.set_result((error, message))

    def on_delivery(error: KafkaError | None, message: Message | None) -> None:
        try:
            loop.call_soon_threadsafe(settle, error, message)
        except RuntimeError:
            # The loop is closed: a report served by a flush at shutdown has
            # no send left waiting for it.
            pass

    return on_delivery


def _produce_and_flush(
    *,
    producer: Producer,
    topic: str,
    record: DLQRecord,
    headers: list[Header],
    on_delivery: Callable[[KafkaError | None, Message | None], None],
    flush_timeout_seconds: float,
) -> int:
    """Enqueue one record and flush the producer; blocking, so run in an executor."""
    # The client also takes text header values; the record only ever holds bytes.
    wire_headers: list[tuple[str, str | bytes | None]] = [*headers]
    producer.produce(topic=topic, key=record.key, value=record.value, headers=wire_headers, on_delivery=on_delivery)
    return producer.flush(flush_timeout_seconds)


def _delivery_failure(error: KafkaError | None, message: Message | None) -> str | None:
    """The reason a delivery report is not a confirmation, or None when it is one."""
    if error is not None:
        return str(error)
    if message is None:
        return 'no delivery report'
    if message.error() is not None:
        return str(message.error())
    return None


async def read_dlq_entries(
    topic: str,
    brokers: str,
    limit: int | None = None,
    poll_timeout: float = 2.0,
    idle_polls_before_stop: int = 2,
    security: KafkaSecurityConfig | None = None,
    client_config: Mapping[str, str] | None = None,
) -> AsyncIterator[DLQEntry]:
    """Consume DLQ entries from the DLQ Kafka topic, from the beginning.

    Opens a consumer with a unique, throwaway consumer group so every invocation
    reads the entire topic from offset 0. The iteration stops when the broker
    reports no new messages for ``idle_polls_before_stop`` consecutive polls —
    this is the standard "drained" heuristic for one-shot readers since Kafka
    has no real "end of topic" signal beyond _PARTITION_EOF events (which
    confluent-kafka emits only when the client is configured to surface them).

    Each yielded value is one record as ``DLQSink.send()`` wrote it: key,
    value and headers unchanged, plus where it sits on the DLQ topic.

    Yields at most ``limit`` entries when specified; yields indefinitely until
    drained otherwise. The caller is expected to iterate in an async for loop
    and handle KeyboardInterrupt / early termination itself.

    This function is intentionally minimal — it exists so ad-hoc replay /
    inspection tools (e.g. ``scripts/replay_dlq.py``) do not need to know
    about confluent-kafka wiring. It is NOT part of the hot-path framework
    code and is not exercised by the worker runtime.
    """
    group_id = f'drakkar-dlq-reader-{uuid.uuid4().hex[:12]}'
    # A DLQ on a secured cluster is only useful if it can be read back, so
    # the replay path takes the same security settings as the writer.
    consumer = AIOConsumer(
        merge_client_config(
            {
                'bootstrap.servers': brokers,
                'group.id': group_id,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
            },
            security or KafkaSecurityConfig(),
            client_config,
        )
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
                entry = DLQEntry(
                    key=msg.key(),
                    # A record with a null value cannot come from the DLQ
                    # sink; read it as empty rather than stopping the read.
                    value=msg.value() or b'',
                    headers=msg.headers() or [],
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
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
