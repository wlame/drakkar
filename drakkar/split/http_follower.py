"""The status follower: every worker with HTTP split jobs follows the status topic into an in-memory map.

One consumer without group membership reads every partition of the status
topic from the beginning and never commits. At the start it reads up to the
end it saw at that moment (the restore); then it reads live. Each record goes
through the rules of :class:`~drakkar.split.http_status.StatusMap`, so the
map does not depend on the order in which records arrive. Everything runs on
the main loop: the consumer calls are coroutines of the asyncio consumer,
and records are applied in small chunks with a turn of the loop between them.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import structlog
from confluent_kafka import OFFSET_BEGINNING, TopicPartition
from confluent_kafka.aio import AIOConsumer

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.kafka_security import merge_client_config
from drakkar.metrics import split_http_jobs
from drakkar.providers.resilience import RateLimitedLog, describe_error
from drakkar.split.http_status import STATE_RANK, StatusMap, StatusRecord, parse_status_record

logger = structlog.get_logger()

BATCH_SIZE = 500
POLL_TIMEOUT_SECONDS = 1.0
FETCH_WAIT_MAX_MS = 100
QUERY_TIMEOUT_SECONDS = 5.0
CONSUMER_ERROR_PAUSE_SECONDS = 0.5
LOOP_RESTART_SECONDS = 3.0
RESTORE_BACKOFF_FIRST_SECONDS = 1.0
RESTORE_BACKOFF_MAX_SECONDS = 30.0
# How often a restore asks the broker again where its partitions end.
RESTORE_END_CHECK_SECONDS = 1.0
# A turn of the loop after this many records or bytes of record values: a status can be close to 1 MiB.
CHUNK_RECORDS = 64
CHUNK_BYTES = 1024 * 1024
EXPIRY_SWEEP_SECONDS = 60.0
EXPIRY_SWEEP_LIMIT = 10_000
# At most this many reads wait for an id at once; a read beyond it answers at once.
MAX_STATUS_WAITERS = 1000

SleepFn = Callable[[float], Awaitable[None]]


class StatusTopicUnavailable(Exception):
    """The broker did not describe the status topic, so the restore cannot know its partitions."""


def _restore_backoff(attempt: int) -> float:
    """The wait after failed restore attempt number ``attempt``: 1, 2, 4, ... seconds, at most 30."""
    return min(RESTORE_BACKOFF_FIRST_SECONDS * 2 ** (attempt - 1), RESTORE_BACKOFF_MAX_SECONDS)


class StatusFollower:
    """Keeps the status map of HTTP split jobs current from the status topic.

    ``restored`` and ``is_restored`` are set together, on the main loop, once
    the restore has read up to the end of every partition; other threads read
    the bool. ``group`` is the consumer group id the client runs under; the
    follower only assigns and never commits under it.
    """

    def __init__(
        self,
        *,
        connection: KafkaConnectionConfig,
        topic: str,
        group: str,
        status_map: StatusMap,
        consumer_factory: Callable[[dict], Any] = AIOConsumer,
        clock: Callable[[], float] = time.time,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        self._config = merge_client_config(
            {
                'bootstrap.servers': connection.brokers,
                'group.id': group,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
                'fetch.wait.max.ms': FETCH_WAIT_MAX_MS,
            },
            connection.security,
            connection.client_config,
        )
        self._topic = topic
        self.status_map = status_map
        self._consumer_factory = consumer_factory
        self._clock = clock
        self._sleep = sleep
        self.restored = asyncio.Event()
        self.is_restored = False
        self._consumer: Any = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._started = False
        self._stopped = False
        # The next offset to read of each partition, after the last record the follower applied.
        self._next: dict[int, int] = {}
        # The reads that wait for an id, each with the client it answers.
        self._waiters: dict[str, list[tuple[str, asyncio.Future[StatusRecord | None]]]] = {}
        self._waiting = 0
        self._rate_limited = RateLimitedLog(time.monotonic)

    # --- lifecycle ---

    def start(self) -> None:
        """Start the restore, then the live read and the expiry sweep, in tasks of their own; a second call does nothing."""
        if self._started or self._stopped:
            return
        self._started = True
        self._spawn(self._run())

    async def stop(self) -> None:
        """Stop every loop, end every waiting read with None, close the consumer and clear the gauge."""
        if self._stopped:
            return
        self._stopped = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for waiters in self._waiters.values():
            for _, future in waiters:
                if not future.done():
                    future.set_result(None)
        self._waiters.clear()
        if self._consumer is not None:
            try:
                await self._consumer.close()
            except Exception as exc:
                logger.warning('split_http_status_close_failed', category='split', error=describe_error(exc))
        for state in STATE_RANK:
            split_http_jobs.labels(state=state).set(0)

    async def wait_restored(self, stop: asyncio.Event) -> bool:
        """Wait until the restore ended or ``stop`` is set; True when the map is restored."""
        if self.is_restored or stop.is_set():
            return self.is_restored
        restored = asyncio.ensure_future(self.restored.wait())
        stopped = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({restored, stopped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in (restored, stopped):
                waiter.cancel()
        return self.is_restored

    # --- reads ---

    def get(self, request_id: str) -> StatusRecord | None:
        """The stored status of ``request_id``, if any."""
        return self.status_map.get(request_id)

    async def wait_for(self, request_id: str, timeout: float, *, client: str) -> StatusRecord | None:
        """The status of ``request_id`` for ``client``, waiting up to ``timeout`` seconds for a record of that client.

        A stored status of the client answers at once. A status of another
        client is no answer: the read waits as for an unknown id, so the time
        of the answer does not show that the id exists. A read beyond
        ``MAX_STATUS_WAITERS`` waiting reads answers at once, so a flood of
        polls holds no unbounded number of waits.
        """
        found = self._of_client(request_id, client)
        if found is not None or timeout <= 0 or self._stopped or self._waiting >= MAX_STATUS_WAITERS:
            return found
        future: asyncio.Future[StatusRecord | None] = asyncio.get_running_loop().create_future()
        waiter = (client, future)
        self._waiters.setdefault(request_id, []).append(waiter)
        self._waiting += 1
        try:
            return await asyncio.wait_for(future, timeout)
        except TimeoutError:
            return None
        finally:
            self._waiting -= 1
            self._drop_waiter(request_id, waiter)

    def _of_client(self, request_id: str, client: str) -> StatusRecord | None:
        """The stored status of ``request_id`` when it belongs to ``client``; None otherwise."""
        found = self.status_map.get(request_id)
        return found if found is not None and found.client == client else None

    def _drop_waiter(self, request_id: str, waiter: tuple[str, asyncio.Future[StatusRecord | None]]) -> None:
        """Forget one waiting read; the id leaves the table with its last read."""
        waiters = self._waiters.get(request_id)
        if waiters is None or waiter not in waiters:
            return
        waiters.remove(waiter)
        if not waiters:
            del self._waiters[request_id]

    def _wake(self, request_id: str) -> None:
        """Give every read that waits for ``request_id`` the stored status, when it belongs to the read's client."""
        waiters = self._waiters.get(request_id)
        if not waiters:
            return
        record = self.status_map.get(request_id)
        if record is None:
            return
        for client, future in waiters:
            if client == record.client and not future.done():
                future.set_result(record)

    # --- restore ---

    async def _run(self) -> None:
        """Restore until it works, then read live and sweep expired entries."""
        await self._restore()
        self._spawn(self._supervised('sweep', self._sweep_loop))
        await self._supervised('read', self._read_loop)

    async def _restore(self) -> None:
        """Run restore attempts until one works, with a backoff of 1 to 30 seconds."""
        attempt = 0
        while True:
            attempt += 1
            try:
                await self._restore_attempt()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log_limited('warning', 'restore', 'split_http_status_restore_failed', error=exc, attempt=attempt)
                await self._sleep(_restore_backoff(attempt))

    def _client(self) -> Any:
        """The consumer, built on first use; a restore that failed keeps it for the next attempt."""
        if self._consumer is None:
            self._consumer = self._consumer_factory(self._config)
        return self._consumer

    async def _restore_attempt(self) -> None:
        """Assign every partition from the beginning and read up to the end each had at the start."""
        consumer = self._client()
        started = time.monotonic()
        partitions = await self._partitions(consumer)
        pending: dict[int, int] = {}
        for partition in partitions:
            low, high = await consumer.get_watermark_offsets(
                TopicPartition(self._topic, partition), timeout=QUERY_TIMEOUT_SECONDS
            )
            if high > low:
                pending[partition] = high
        await consumer.assign([TopicPartition(self._topic, partition, OFFSET_BEGINNING) for partition in partitions])
        self._next = {}
        records = 0
        loop = asyncio.get_running_loop()
        last_end_check = loop.time()
        while pending:
            messages = await consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
            records += await self._apply(messages)
            self._drop_reached(pending)
            # On time, whatever the batch held: records of other partitions must not hold a partition whose
            # log was cut below its end.
            if pending and loop.time() - last_end_check >= RESTORE_END_CHECK_SECONDS:
                last_end_check = loop.time()
                await self._check_ends(consumer, pending)
            await asyncio.sleep(0)
        self._set_gauge()
        self.is_restored = True
        self.restored.set()
        logger.info(
            'split_http_status_restored',
            category='split',
            records=records,
            jobs=len(self.status_map),
            seconds=round(time.monotonic() - started, 3),
        )

    async def _partitions(self, consumer: Any) -> list[int]:
        """The partition numbers of the status topic, as the broker describes it."""
        metadata = await consumer.list_topics(self._topic, timeout=QUERY_TIMEOUT_SECONDS)
        found = metadata.topics.get(self._topic)
        if found is None or found.error is not None or not found.partitions:
            error = getattr(found, 'error', None)
            raise StatusTopicUnavailable(f"the status topic '{self._topic}' is not described: {error}")
        return sorted(found.partitions)

    def _drop_reached(self, pending: dict[int, int]) -> None:
        """Forget every partition whose read reached its end."""
        for partition, end in list(pending.items()):
            if self._next.get(partition, 0) >= end:
                del pending[partition]

    async def _check_ends(self, consumer: Any, pending: dict[int, int]) -> None:
        """Forget the partitions that hold nothing more to read below their end.

        The log can be cut below the end read at the start, and offsets can
        hold no record a consumer returns, so a restore that only counted
        records could wait for ever.
        """
        for partition, end in list(pending.items()):
            tp = TopicPartition(self._topic, partition)
            low, high = await consumer.get_watermark_offsets(tp, timeout=QUERY_TIMEOUT_SECONDS)
            end = min(end, high)
            [row] = await consumer.position([tp])
            if low >= end or self._next.get(partition, 0) >= end or row.offset >= end:
                del pending[partition]
            else:
                pending[partition] = end

    # --- live read ---

    async def _read_loop(self) -> None:
        """Read the status topic batch after batch for as long as the follower runs."""
        while True:
            await self._read_once()

    async def _read_once(self) -> None:
        """One batch: consume and apply; pause after a failure."""
        try:
            messages = await self._consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log_limited('warning', 'consume', 'split_http_status_consume_failed', error=exc)
            await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
            return
        await self._apply(messages)
        if any(message.error() for message in messages):
            await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
        else:
            await asyncio.sleep(0)

    async def _apply(self, messages: list[Any]) -> int:
        """Apply a batch to the map, with a turn of the loop between chunks; the number of records read."""
        read = 0
        chunk_records = 0
        chunk_bytes = 0
        now = self._clock()
        for message in messages:
            error = message.error()
            if error:
                self._log_limited('warning', 'consume', 'split_http_status_consume_failed', error=error)
                continue
            read += 1
            value = message.value()
            self._apply_one_safely(message, value, now)
            chunk_records += 1
            chunk_bytes += len(value or b'')
            if chunk_records >= CHUNK_RECORDS or chunk_bytes >= CHUNK_BYTES:
                await asyncio.sleep(0)
                chunk_records = 0
                chunk_bytes = 0
                now = self._clock()
        if read:
            self._set_gauge()
        return read

    def _apply_one_safely(self, message: Any, value: bytes | None, now: float) -> None:
        """Apply one record; an unexpected error is logged and the next record is applied."""
        try:
            self._apply_one(message, value, now)
        except Exception as exc:
            self._log_limited(
                'error',
                'record',
                'split_http_status_record_failed',
                error=exc,
                partition=message.partition(),
                offset=message.offset(),
            )

    def _apply_one(self, message: Any, value: bytes | None, now: float) -> None:
        """Apply one record: a valid one goes to the map and wakes its reads, an invalid one is logged."""
        partition = message.partition()
        offset = message.offset()
        self._next[partition] = max(self._next.get(partition, 0), offset + 1)
        record = parse_status_record(message.key(), message.headers(), value)
        if record is None:
            suppressed = self._rate_limited.admit('invalid')
            if suppressed is not None:
                logger.warning(
                    'split_http_status_record_invalid',
                    category='split',
                    partition=partition,
                    offset=offset,
                    suppressed=suppressed,
                )
            return
        if self.status_map.put(record, now=now):
            self._wake(record.request_id)

    # --- expiry and gauge ---

    async def _sweep_loop(self) -> None:
        """Sweep every ``EXPIRY_SWEEP_SECONDS``."""
        while True:
            await self._sleep(EXPIRY_SWEEP_SECONDS)
            await self._sweep()

    async def _sweep(self) -> None:
        """Remove every ended entry past its retention, ``EXPIRY_SWEEP_LIMIT`` heap items at a time.

        The loop gets a turn between two calls, so a large backlog is removed
        in one sweep without holding the loop. Each sweep also starts a new
        round of the marks of jobs whose end expired; the first sweep comes
        after the restore, so the restore keeps every mark it made.
        """
        self.status_map.forget_old_marks()
        now = self._clock()
        removed = self.status_map.expire(now, limit=EXPIRY_SWEEP_LIMIT)
        while self.status_map.has_due(now):
            await asyncio.sleep(0)
            removed += self.status_map.expire(now, limit=EXPIRY_SWEEP_LIMIT)
        if removed:
            self._set_gauge()

    def _set_gauge(self) -> None:
        for state, count in self.status_map.counts().items():
            split_http_jobs.labels(state=state).set(count)

    # --- helpers ---

    async def _supervised(self, name: str, run: Callable[[], Awaitable[None]]) -> None:
        """Run a loop of the follower; when it ends on an error, log it and start it again after a pause."""
        while True:
            try:
                await run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error('split_http_status_loop_failed', category='split', loop=name, error=describe_error(exc))
            await self._sleep(LOOP_RESTART_SECONDS)

    def _log_limited(self, level: str, key: str, event: str, *, error: object, **fields: Any) -> None:
        """Log a line that can repeat, at most once per key and interval; ``error`` is an exception or a text."""
        suppressed = self._rate_limited.admit(key)
        if suppressed is None:
            return
        text = describe_error(error) if isinstance(error, BaseException) else str(error)
        getattr(logger, level)(event, category='split', error=text, suppressed=suppressed, **fields)

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start a task the follower owns; ``stop`` cancels it."""
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task
