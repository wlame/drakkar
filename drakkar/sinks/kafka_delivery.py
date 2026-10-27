"""Shared delivery-verdict helpers for the Kafka-backed sinks.

``AIOProducer.flush(timeout)`` is librdkafka's **producer-wide** flush: it
returns how many messages are still queued on the whole producer, not on the
batch that asked for it. One producer serves every partition loop, so that
count says nothing about whether *this* batch was acknowledged. Both Kafka
sinks therefore keep the flush as the nudge that hands buffered messages to
librdkafka, and take their verdict from their own delivery futures instead.
These helpers hold the timing arithmetic that makes the two-step wait share a
single budget.

The DLQ sink and the DLQ replay tool use the plain (sync) ``Producer``
instead, because the asyncio producer cannot send headers. They run
``produce_and_flush`` on a thread of their own and judge each write by its own
delivery report, handed to the loop by ``delivery_callback``.
"""

import asyncio
import time
from collections.abc import Callable, Sequence

from confluent_kafka import KafkaError, Message, Producer

DeliveryReport = tuple[KafkaError | None, Message | None]

# Smallest wait handed to the delivery futures after the flush. A flush that
# burned the entire budget must still let futures whose delivery report is
# already queued on the loop resolve, otherwise an acknowledged batch would be
# reported as a timeout.
MIN_SETTLE_SECONDS = 0.1

# Grace for collecting delivery futures the main path abandoned. They already
# had the whole flush budget, so this only drains reports that landed since.
ABANDONED_FUTURE_GRACE_SECONDS = 0.5


def settle_budget(deadline: float) -> float:
    """Return what is left of ``deadline``, never less than the minimum grace.

    ``deadline`` is a ``time.monotonic()`` stamp taken before the flush, so
    the flush and the wait on the delivery futures share one bound instead of
    each getting a full ``flush_timeout_seconds``.
    """
    return max(deadline - time.monotonic(), MIN_SETTLE_SECONDS)


def future_outcome(future: asyncio.Future) -> object:
    """Return a settled future's exception or its result, never raising."""
    if future.cancelled():
        return asyncio.CancelledError()
    return future.exception() or future.result()


def delivery_callback(
    loop: asyncio.AbstractEventLoop, report: asyncio.Future[DeliveryReport]
) -> Callable[[KafkaError | None, Message | None], None]:
    """Build a sync producer's delivery callback for one write.

    librdkafka calls it from inside ``flush()``/``poll()``, so on an executor
    thread, never on the loop: it may only hand the report over with
    ``call_soon_threadsafe``.
    """

    def settle(error: KafkaError | None, message: Message | None) -> None:
        # A write that timed out leaves the future pending, so a late report
        # still settles it, harmlessly. librdkafka serves each report once;
        # the check keeps a second call from raising inside the loop.
        if not report.done():
            report.set_result((error, message))

    def on_delivery(error: KafkaError | None, message: Message | None) -> None:
        try:
            loop.call_soon_threadsafe(settle, error, message)
        except RuntimeError:
            # The loop is closed: a report served by a flush at shutdown has
            # no write left waiting for it.
            pass

    return on_delivery


def produce_and_flush(
    *,
    producer: Producer,
    topic: str,
    key: bytes | None,
    value: bytes,
    headers: Sequence[tuple[str, bytes | None]],
    on_delivery: Callable[[KafkaError | None, Message | None], None],
    flush_timeout_seconds: float,
) -> int:
    """Enqueue one message with a sync producer and flush it; blocking, so run on a thread.

    Returns the producer-wide count of messages still queued, which only
    explains a timeout: the write is judged by its own delivery report.
    """
    # The client also takes text header values; a DLQ record only ever holds bytes or None.
    wire_headers: list[tuple[str, str | bytes | None]] = [*headers]
    producer.produce(topic=topic, key=key, value=value, headers=wire_headers, on_delivery=on_delivery)
    return producer.flush(flush_timeout_seconds)


def delivery_failure(error: KafkaError | None, message: Message | None) -> str | None:
    """The reason a delivery report is not a confirmation, or None when it is one."""
    if error is not None:
        return str(error)
    if message is None:
        return 'no delivery report'
    if message.error() is not None:
        return str(message.error())
    return None
