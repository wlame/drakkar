"""In-memory stand-ins for the Kafka clients, shared by the tests of the split feature.

``FakeLog`` models topics: for each (topic, partition) an append-only list with offsets, a low watermark
that a test can raise to simulate expiry, timestamps from a clock the test controls, and committed
offsets for each consumer group. ``FakeProducerClient`` and ``FakeConsumerClient`` work over a log with the
methods the code calls; the consumer's methods are coroutines, as those of the asyncio consumer are.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, KafkaError, TopicPartition

START_MS = 1_700_000_000_000
IDLE_WAIT_SECONDS = 0.005

Headers = list[tuple[str, bytes | None]]


class FakeClock:
    """A clock in milliseconds that a test moves by hand."""

    def __init__(self, start_ms: int = START_MS) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, milliseconds: int) -> None:
        """Move the clock forward."""
        self.now_ms += milliseconds


@dataclass(frozen=True)
class FakeRecord:
    """One record of a topic."""

    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes | None
    headers: Headers


class FakeMessage:
    """A message as the consumer and the delivery report give it."""

    def __init__(
        self,
        *,
        topic: str = '',
        partition: int = 0,
        record: FakeRecord | None = None,
        error: KafkaError | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._record = record
        self._error = error

    def error(self) -> KafkaError | None:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._record.offset if self._record else OFFSET_INVALID

    def key(self) -> bytes | None:
        return self._record.key if self._record else None

    def value(self) -> bytes | None:
        return self._record.value if self._record else None

    def headers(self) -> Headers | None:
        return list(self._record.headers) if self._record else None

    def timestamp(self) -> tuple[int, int]:
        return (1, self._record.timestamp_ms if self._record else -1)


class FakeLog:
    """Topics held in memory. Safe to use from a thread: the producer's poll thread appends to it."""

    def __init__(self, clock: Callable[[], int] | None = None) -> None:
        self.clock = clock or FakeClock()
        self._lock = threading.Lock()
        self._records: dict[tuple[str, int], list[FakeRecord]] = {}
        self._low: dict[tuple[str, int], int] = {}
        self._committed: dict[tuple[str, str, int], int] = {}

    def append(
        self, topic: str, partition: int, *, key: bytes | None, value: bytes | None, headers: Sequence[tuple] = ()
    ) -> int:
        """Add a record to the end of a partition and return its offset."""
        with self._lock:
            records = self._records.setdefault((topic, partition), [])
            offset = self._low.get((topic, partition), 0) + len(records)
            records.append(FakeRecord(offset, self.clock(), key, value, list(headers)))
            return offset

    def watermarks(self, topic: str, partition: int) -> tuple[int, int]:
        """The ``(low, high)`` offsets of a partition."""
        with self._lock:
            low = self._low.get((topic, partition), 0)
            return low, low + len(self._records.get((topic, partition), []))

    def raise_low(self, topic: str, partition: int, offset: int) -> None:
        """Drop the records below ``offset``, as retention does; the low watermark becomes ``offset``."""
        with self._lock:
            low = self._low.get((topic, partition), 0)
            records = self._records.get((topic, partition), [])
            if offset > low + len(records):
                raise ValueError(f'offset {offset} is above the end of the partition')
            self._records[(topic, partition)] = records[max(offset - low, 0) :]
            self._low[(topic, partition)] = max(offset, low)

    def read(self, topic: str, partition: int, offset: int, count: int) -> list[FakeRecord]:
        """Up to ``count`` records from ``offset`` on; offsets below the low watermark start at the low one."""
        with self._lock:
            low = self._low.get((topic, partition), 0)
            records = self._records.get((topic, partition), [])
            start = max(offset, low) - low
            return records[start : start + count]

    def records(self, topic: str, partition: int) -> list[FakeRecord]:
        """All records a partition still holds."""
        with self._lock:
            return list(self._records.get((topic, partition), []))

    def commit(self, group: str, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._committed[(group, topic, partition)] = offset

    def committed(self, group: str, topic: str, partition: int) -> int | None:
        """The committed offset of a group, or None when it has none."""
        with self._lock:
            return self._committed.get((group, topic, partition))


class FailureSwitches:
    """Makes the next calls of named methods raise."""

    def __init__(self) -> None:
        self._failures: dict[str, deque[BaseException]] = {}

    def fail_next(self, method: str, error: BaseException, *, times: int = 1) -> None:
        """Make the next ``times`` calls of ``method`` raise ``error``."""
        self._failures.setdefault(method, deque()).extend([error] * times)

    def _maybe_fail(self, method: str) -> None:
        queue = self._failures.get(method)
        if queue:
            raise queue.popleft()


@dataclass
class _Pending:
    topic: str
    partition: int
    key: bytes | None
    value: bytes | None
    headers: Headers
    on_delivery: Callable[[KafkaError | None, FakeMessage | None], None] | None


class FakeProducerClient(FailureSwitches):
    """The plain producer over a ``FakeLog``: records wait in a queue and are delivered by ``poll`` or ``flush``."""

    def __init__(self, log: FakeLog, *, queue_capacity: int | None = None) -> None:
        super().__init__()
        self.log = log
        self.queue_capacity = queue_capacity
        self.hold_deliveries = False
        self.config: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._pending: deque[_Pending] = deque()
        self._delivery_errors: deque[KafkaError] = deque()
        self._purged: deque[_Pending] = deque()
        self.produced = 0
        self.rejected: list[bytes | None] = []

    def fail_next_deliveries(self, error: KafkaError, *, times: int = 1) -> None:
        """Make the delivery reports of the next ``times`` records carry ``error``; they are not written."""
        self._delivery_errors.extend([error] * times)

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: Callable | None = None,
        timestamp: int = 0,
        headers: Headers | None = None,
    ) -> None:
        self._maybe_fail('produce')
        with self._lock:
            if self.queue_capacity is not None and len(self._pending) >= self.queue_capacity:
                self.rejected.append(value)
                raise BufferError('Local: Queue full')
            self._pending.append(_Pending(topic, max(partition, 0), key, value, list(headers or []), on_delivery))
            self.produced += 1

    def _deliver_all(self) -> int:
        """Deliver every queued record, in order, unless deliveries are held back; returns how many."""
        with self._lock:
            purged, self._purged = list(self._purged), deque()
        for item in purged:
            if item.on_delivery:
                item.on_delivery(KafkaError(KafkaError._PURGE_QUEUE), FakeMessage(topic=item.topic))
        with self._lock:
            if self.hold_deliveries:
                return 0
            batch, self._pending = list(self._pending), deque()
        for item in batch:
            self._deliver(item)
        return len(batch)

    def _deliver(self, item: _Pending) -> None:
        if self._delivery_errors:
            error = self._delivery_errors.popleft()
            if item.on_delivery:
                item.on_delivery(error, FakeMessage(topic=item.topic, partition=item.partition))
            return
        offset = self.log.append(item.topic, item.partition, key=item.key, value=item.value, headers=item.headers)
        if item.on_delivery:
            record = self.log.read(item.topic, item.partition, offset, 1)[0]
            item.on_delivery(None, FakeMessage(topic=item.topic, partition=item.partition, record=record))

    def poll(self, timeout: float = 0) -> int:
        self._maybe_fail('poll')
        served = self._deliver_all()
        if not served:
            time.sleep(min(timeout, IDLE_WAIT_SECONDS))
        return served

    def flush(self, timeout: float = -1) -> int:
        self._maybe_fail('flush')
        self._deliver_all()
        with self._lock:
            return len(self._pending)

    def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None:
        """Drop every queued record; each one gets a delivery report with the purge error, served by ``poll``."""
        with self._lock:
            dropped, self._pending = list(self._pending), deque()
            self._purged.extend(dropped)

    def release_deliveries(self) -> None:
        """Stop holding deliveries back; the next poll delivers what waits."""
        self.hold_deliveries = False


class FakeConsumerClient(FailureSwitches):
    """The asyncio consumer over a ``FakeLog``, with the semantics a real broker check found."""

    def __init__(self, log: FakeLog, *, group_id: str = 'fake-group', auto_offset_reset: str = 'earliest') -> None:
        super().__init__()
        self.log = log
        self.group_id = group_id
        # Where a position below the low watermark goes: 'earliest', 'latest', or 'error' (an error message).
        self.auto_offset_reset = auto_offset_reset
        self.closed = False
        self._positions: dict[tuple[str, int], int] = {}
        self.error_messages: deque[FakeMessage] = deque()

    def _start_offset(self, tp: TopicPartition) -> int:
        """An explicit offset, else the committed offset of the group, else the earliest one."""
        low, high = self.log.watermarks(tp.topic, tp.partition)
        if tp.offset >= 0:
            return tp.offset
        if tp.offset == OFFSET_BEGINNING:
            return low
        if tp.offset == OFFSET_END:
            return high
        committed = self.log.committed(self.group_id, tp.topic, tp.partition)
        return low if committed is None else committed

    def _check_open(self, method: str) -> None:
        self._maybe_fail(method)
        if self.closed:
            raise RuntimeError('Consumer closed')

    async def assign(self, partitions: list[TopicPartition]) -> None:
        self._check_open('assign')
        self._positions = {(tp.topic, tp.partition): self._start_offset(tp) for tp in partitions}

    async def incremental_assign(self, partitions: list[TopicPartition]) -> None:
        self._check_open('incremental_assign')
        for tp in partitions:
            self._positions[(tp.topic, tp.partition)] = self._start_offset(tp)

    async def incremental_unassign(self, partitions: list[TopicPartition]) -> None:
        self._check_open('incremental_unassign')
        for tp in partitions:
            self._positions.pop((tp.topic, tp.partition), None)

    def inject_error(self, error: KafkaError) -> None:
        """Make the next ``consume`` return a message that carries ``error``."""
        self.error_messages.append(FakeMessage(error=error))

    async def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        self._check_open('consume')
        if self.error_messages:
            return [self.error_messages.popleft()]
        messages: list[FakeMessage] = []
        for (topic, partition), position in list(self._positions.items()):
            low, high = self.log.watermarks(topic, partition)
            if position < low:
                if self.auto_offset_reset == 'error':
                    self._positions[(topic, partition)] = low
                    return [FakeMessage(error=KafkaError(KafkaError._AUTO_OFFSET_RESET))]
                position = low if self.auto_offset_reset == 'earliest' else high
                self._positions[(topic, partition)] = position
            records = self.log.read(topic, partition, position, num_messages - len(messages))
            for record in records:
                messages.append(FakeMessage(topic=topic, partition=partition, record=record))
            if records:
                self._positions[(topic, partition)] = records[-1].offset + 1
            if len(messages) >= num_messages:
                break
        if not messages:
            await asyncio.sleep(min(max(timeout, 0), IDLE_WAIT_SECONDS))
        return messages

    async def commit(
        self, message: Any = None, offsets: list[TopicPartition] | None = None, asynchronous: bool = False
    ):
        self._check_open('commit')
        if offsets is None and message is not None:
            offsets = [TopicPartition(message.topic(), message.partition(), message.offset() + 1)]
        if offsets is None:
            offsets = [
                TopicPartition(topic, partition, position) for (topic, partition), position in self._positions.items()
            ]
        for tp in offsets:
            self.log.commit(self.group_id, tp.topic, tp.partition, tp.offset)

    async def committed(self, partitions: list[TopicPartition], timeout: float | None = None) -> list[TopicPartition]:
        self._check_open('committed')
        found = []
        for tp in partitions:
            offset = self.log.committed(self.group_id, tp.topic, tp.partition)
            found.append(TopicPartition(tp.topic, tp.partition, OFFSET_INVALID if offset is None else offset))
        return found

    async def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int]:
        self._check_open('get_watermark_offsets')
        return self.log.watermarks(partition.topic, partition.partition)

    async def close(self) -> None:
        self._maybe_fail('close')
        self.closed = True
