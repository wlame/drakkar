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
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, Producer

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.kafka_security import merge_client_config
from drakkar.split.records import WireRecord

logger = structlog.get_logger()

POLL_TIMEOUT_SECONDS = 0.1
BUFFER_FULL_WAIT_SECONDS = 0.01
POLL_THREAD_NAME = 'split-producer'

# Errors that a retry cannot fix, besides the ones the client marks as fatal.
FINAL_ERROR_CODES: frozenset[int] = frozenset(
    {
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.CLUSTER_AUTHORIZATION_FAILED,
        KafkaError.TOPIC_EXCEPTION,
        KafkaError._UNKNOWN_TOPIC,
        KafkaError._UNKNOWN_PARTITION,
        KafkaError._AUTHENTICATION,
    }
)


class SplitProduceError(Exception):
    """A record was not delivered. ``final`` is true when sending it again cannot work."""

    def __init__(self, message: str, *, final: bool) -> None:
        super().__init__(message)
        self.final = final


def is_final_error(error: KafkaError) -> bool:
    """True for an error that a retry cannot fix: a fatal one, or one of the known codes."""
    if error.retriable():
        return False
    return error.fatal() or error.code() in FINAL_ERROR_CODES


def _produce_error(error: KafkaError) -> SplitProduceError:
    return SplitProduceError(str(error), final=is_final_error(error))


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

    def _client_config(self) -> dict[str, Any]:
        """Brokers, security and raw properties of the connection, then the framework's delivery settings."""
        config = merge_client_config(
            {'bootstrap.servers': self._connection.brokers},
            self._connection.security,
            self._connection.client_config,
        )
        config.update({'acks': 'all', 'enable.idempotence': True, 'linger.ms': 5})
        return config

    async def start(self) -> None:
        """Build the client and start the thread that serves delivery reports."""
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
                logger.error('split_producer_poll_failed', category='split', error=str(exc))
                self._stopping.wait(POLL_TIMEOUT_SECONDS)

    async def close(self, timeout: float) -> None:
        """Stop the poll thread, then flush what is queued, for at most ``timeout`` seconds."""
        thread, producer = self._thread, self._producer
        if thread is None or producer is None:
            return
        self._stopping.set()
        await asyncio.to_thread(thread.join)
        self._thread = None
        remaining = await asyncio.to_thread(producer.flush, timeout)
        if remaining:
            logger.warning('split_producer_close_incomplete', category='split', remaining=remaining)

    def _delivery_callback(self, loop: asyncio.AbstractEventLoop, future: asyncio.Future[None]) -> Callable[..., None]:
        """The callback for one record; the client calls it on the poll thread."""

        def settle(error: KafkaError | None, message: Message | None) -> None:
            if future.done():
                return
            failure = _report_error(error, message)
            if failure is None:
                future.set_result(None)
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

    async def enqueue(self, topic: str, record: WireRecord) -> asyncio.Future[None]:
        """Put ``record`` in the client queue and return its delivery future.

        Waits only while the client queue is full. A record the client refuses at once raises
        ``SplitProduceError``; a failed delivery ends the future with it.
        """
        loop = self._loop
        if loop is None or self._producer is None:
            raise RuntimeError('the split producer is not started')
        future: asyncio.Future[None] = loop.create_future()
        callback = self._delivery_callback(loop, future)
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=record.key,
                    value=record.value,
                    headers=list(record.headers),
                    partition=record.partition,
                    on_delivery=callback,
                )
                return future
            except BufferError:
                await asyncio.sleep(BUFFER_FULL_WAIT_SECONDS)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                if isinstance(error, KafkaError):
                    raise _produce_error(error) from exc
                raise SplitProduceError(str(exc), final=False) from exc

    async def publish(self, topic: str, record: WireRecord) -> None:
        """Enqueue ``record`` and wait for its delivery report."""
        future = await self.enqueue(topic, record)
        await future
