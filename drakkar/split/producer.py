"""The producer of the split feature: part records and results-log records.

The asyncio producer of the Kafka client cannot send record headers, so this is
the plain producer. One daemon thread of its own calls ``poll`` in a loop, which
serves the delivery callbacks. ``produce`` itself runs on the event loop: it only
puts the record in the client queue and does not block. Each record has one
future that ends with its delivery report.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, Producer

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.kafka_security import merge_client_config
from drakkar.providers.resilience import RateLimitedLog
from drakkar.split.records import WireRecord

logger = structlog.get_logger()

POLL_TIMEOUT_SECONDS = 0.1
BUFFER_FULL_WAIT_SECONDS = 0.01
# The offset of a delivered record whose delivery report names none.
NO_OFFSET = -1
POLL_THREAD_NAME = 'split-producer'
CLOSED_MESSAGE = 'the split producer was closed before the record was delivered'

# Errors that a retry cannot fix, besides the ones the client marks as fatal.
FINAL_ERROR_CODES: frozenset[int] = frozenset(
    {
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.RECORD_LIST_TOO_LARGE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.CLUSTER_AUTHORIZATION_FAILED,
        KafkaError.TOPIC_EXCEPTION,
        KafkaError._UNKNOWN_TOPIC,
        KafkaError._UNKNOWN_PARTITION,
        KafkaError._AUTHENTICATION,
    }
)


# Errors that say the record or its batch is larger than the client or the broker takes.
TOO_LARGE_ERROR_CODES: frozenset[int] = frozenset({KafkaError.MSG_SIZE_TOO_LARGE, KafkaError.RECORD_LIST_TOO_LARGE})


class SplitProduceError(Exception):
    """A record was not delivered.

    ``final`` is true when sending it again cannot work; ``too_large`` is true
    when the record is larger than the client or the broker takes.
    """

    def __init__(self, message: str, *, final: bool, too_large: bool = False) -> None:
        super().__init__(message)
        self.final = final
        self.too_large = too_large


def is_final_error(error: KafkaError) -> bool:
    """True for an error that a retry cannot fix: a fatal one, or one of the known codes."""
    if error.retriable():
        return False
    return error.fatal() or error.code() in FINAL_ERROR_CODES


def _produce_error(error: KafkaError) -> SplitProduceError:
    return SplitProduceError(str(error), final=is_final_error(error), too_large=error.code() in TOO_LARGE_ERROR_CODES)


def _report_error(error: KafkaError | None, message: Message | None) -> KafkaError | SplitProduceError | None:
    """The failure in a delivery report, or None for a confirmation."""
    if error is not None:
        return error
    if message is None:
        return SplitProduceError('no delivery report', final=False)
    return message.error()


class SplitProducer:
    """Produces split records with one delivery future for each record."""

    def __init__(
        self,
        connection: KafkaConnectionConfig,
        *,
        producer_factory: Callable[[dict], Any] = Producer,
    ) -> None:
        self._connection = connection
        self._producer_factory = producer_factory
        self._producer: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        # One waiter at a time takes the next place in the client queue, so records keep their order.
        self._enqueue_lock = asyncio.Lock()
        self._unsettled: set[asyncio.Future[int]] = set()
        # Only the poll thread uses it.
        self._poll_failure_log = RateLimitedLog(time.monotonic)

    def _client_config(self) -> dict[str, Any]:
        """Brokers, security and raw properties of the connection, then the framework's delivery settings."""
        config = merge_client_config(
            {'bootstrap.servers': self._connection.brokers},
            self._connection.security,
            self._connection.client_config,
        )
        # The partitioner is pinned: a record with no partition must land where every worker puts the same key.
        config.update({'acks': 'all', 'enable.idempotence': True, 'linger.ms': 5, 'partitioner': 'consistent_random'})
        return config

    async def start(self) -> None:
        """Build the client and start the thread that serves delivery reports; a second call does nothing."""
        if self._thread is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._producer = self._producer_factory(self._client_config())
        self._stopping.clear()
        self._thread = threading.Thread(target=self._poll_loop, name=POLL_THREAD_NAME, daemon=True)
        self._thread.start()

    def _poll_loop(self) -> None:
        """Serve delivery callbacks until ``close``; runs on the daemon thread."""
        while not self._stopping.is_set():
            try:
                self._producer.poll(POLL_TIMEOUT_SECONDS)
            except Exception as exc:  # a failed poll must not end the thread: reports would stop for ever
                suppressed = self._poll_failure_log.admit('poll')
                if suppressed is not None:
                    logger.error('split_producer_poll_failed', category='split', error=str(exc), suppressed=suppressed)
                self._stopping.wait(POLL_TIMEOUT_SECONDS)

    async def close(self, timeout: float) -> None:
        """Stop the poll thread, flush what is queued for at most ``timeout`` seconds, and fail what is left.

        After it every future has ended and ``enqueue`` is refused.
        """
        thread, producer = self._thread, self._producer
        if thread is None or producer is None:
            return
        self._stopping.set()
        await asyncio.to_thread(thread.join)
        self._thread = None
        remaining = await asyncio.to_thread(self._flush_and_purge, producer, timeout)
        if remaining:
            logger.warning('split_producer_close_incomplete', category='split', remaining=remaining)
        self._producer = None
        # The poll and flush threads handed every report to the loop queue before they ended, but the
        # awaits above can return with no turn of the loop: a thread job that ends before the loop
        # registers its callback completes the awaited future at once. One turn runs those reports, so
        # only a future the client never reported is failed here.
        await asyncio.sleep(0)
        for future in list(self._unsettled):
            if not future.done():
                future.set_exception(SplitProduceError(CLOSED_MESSAGE, final=False))

    @staticmethod
    def _flush_and_purge(producer: Any, timeout: float) -> int:
        """Flush, then drop what the flush could not deliver and serve the reports of the dropped records."""
        remaining = producer.flush(timeout)
        if remaining:
            producer.purge()
            producer.poll(0)
        return remaining

    def _delivery_callback(self, loop: asyncio.AbstractEventLoop, future: asyncio.Future[int]) -> Callable[..., None]:
        """The callback for one record; the client calls it on the poll thread."""

        def settle(error: KafkaError | None, message: Message | None) -> None:
            if future.done():
                return
            failure = _report_error(error, message)
            if failure is None:
                offset = message.offset() if message is not None else None
                # A report with no error carries the offset of the record. Without one the caller
                # gets NO_OFFSET, which is below every offset: it cannot wait for that record to be read.
                future.set_result(NO_OFFSET if offset is None or offset < 0 else offset)
            elif isinstance(failure, SplitProduceError):
                future.set_exception(failure)
            else:
                future.set_exception(_produce_error(failure))

        def on_delivery(error: KafkaError | None, message: Message | None) -> None:
            try:
                loop.call_soon_threadsafe(settle, error, message)
            except RuntimeError:
                # The loop is closed: nobody waits for this report any more.
                pass

        return on_delivery

    def _started_client(self) -> tuple[asyncio.AbstractEventLoop, Any]:
        """The loop and the client of a started producer.

        Raises:
            RuntimeError: the producer is not started, or it is closing.
        """
        loop, producer = self._loop, self._producer
        if loop is None or producer is None or self._stopping.is_set():
            raise RuntimeError('the split producer is not started')
        return loop, producer

    def _track(self, future: asyncio.Future[int]) -> asyncio.Future[int]:
        """Keep the delivery future of a queued record until it ends, so ``close`` can fail what is left."""
        self._unsettled.add(future)
        future.add_done_callback(self._unsettled.discard)
        return future

    async def enqueue(self, topic: str, record: WireRecord) -> asyncio.Future[int]:
        """Put ``record`` in the client queue and return its delivery future.

        The future gives the offset of the record in its partition. Waits
        only while the client queue is full. A record the client refuses at
        once raises ``SplitProduceError``; a failed delivery ends the future
        with it.
        """
        loop, producer = self._started_client()
        future: asyncio.Future[int] = loop.create_future()
        callback = self._delivery_callback(loop, future)
        async with self._enqueue_lock:
            await self._produce_when_room(producer, topic, record, callback)
        return self._track(future)

    def enqueue_nowait(self, topic: str, record: WireRecord) -> asyncio.Future[int] | None:
        """Put ``record`` in the client queue when it has room now; its delivery future, or None when it has none.

        None also while another caller waits for room, so records keep their
        order. A caller that gets None uses :meth:`enqueue`. A record the
        client refuses for another reason raises ``SplitProduceError``.
        """
        loop, producer = self._started_client()
        if self._enqueue_lock.locked():
            return None
        future: asyncio.Future[int] = loop.create_future()
        try:
            self._produce(producer, topic, record, self._delivery_callback(loop, future))
        except BufferError:
            return None
        return self._track(future)

    @staticmethod
    def _produce(producer: Any, topic: str, record: WireRecord, callback: Callable[..., None]) -> None:
        """Give the record to the client.

        Raises:
            BufferError: the client queue is full.
            SplitProduceError: the client refuses the record.
        """
        try:
            producer.produce(
                topic=topic,
                key=record.key,
                value=record.value,
                headers=list(record.headers),
                partition=record.partition,
                on_delivery=callback,
            )
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if isinstance(error, KafkaError):
                raise _produce_error(error) from exc
            raise SplitProduceError(str(exc), final=False) from exc

    async def _produce_when_room(
        self, producer: Any, topic: str, record: WireRecord, callback: Callable[..., None]
    ) -> None:
        """Give the record to the client; while its queue is full, wait, unless the producer is closing."""
        while True:
            if self._stopping.is_set():
                raise SplitProduceError(CLOSED_MESSAGE, final=False)
            try:
                self._produce(producer, topic, record, callback)
                return
            except BufferError:
                await asyncio.sleep(BUFFER_FULL_WAIT_SECONDS)

    async def publish(self, topic: str, record: WireRecord) -> int:
        """Enqueue ``record`` and wait for its delivery report; the offset of the record in its partition."""
        future = await self.enqueue(topic, record)
        return await future
