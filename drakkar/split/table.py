"""The job table: what the results log says about the jobs of one source partition.

Pure state with no I/O. Records of the results log are applied in log order; the
table counts the part results of each job, knows the current attempt of each job,
says when a job is complete or timed out, and gives the offset of the results log
from which a restart must read again. The same rules hold in restore and in live
reading, so a table built from any restart position ends equal to one that read
the whole log.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Literal, TypeGuard, cast, get_args

from drakkar.split.models import PartDLQ, PartResult, SinkCounts
from drakkar.split.records import ClosedOutcome, LogEntry, part_result_from_body

BITS_PER_BYTE = 8
MS_PER_SECOND = 1000
# How long a split may show no ``split-done`` and no ``split-cancelled`` before
# a worker that holds the message of the job takes the owner of that split for
# dead. A live owner writes one of the two; one that died wrote neither, and
# only its timeout would end the job. A constant, the same on every worker:
# two owners that judge one attempt must agree. It is long, because a healthy
# split of a large message takes minutes, and taking it over too early runs
# its parts twice.
FOREIGN_SPLIT_GRACE_SECONDS = 300
# A bound on a part number seen before ``split-done`` gives the job's size: the
# bit set grows to the highest part number, so a damaged record must not make it huge.
MAX_PART_INDEX = 2**24 - 1
CLOSED_OUTCOMES: frozenset[str] = frozenset(get_args(ClosedOutcome))

DueKind = Literal['job', 'idle']


class IgnoreReason(StrEnum):
    """Why a record of the results log changed nothing."""

    DUPLICATE = 'duplicate'
    OLD_ATTEMPT = 'old_attempt'
    CLOSED_JOB = 'closed_job'
    UNKNOWN_JOB = 'unknown_job'
    FOREIGN_GROUP = 'foreign_group'
    MALFORMED = 'malformed'


@dataclass
class PartFailure:
    """The failed part with the lowest number seen so far in the current attempt."""

    index: int
    dlq: PartDLQ | None
    error: str | None


@dataclass
class JobState:
    """One job of the table: its current attempt and the running counts of its part results."""

    partition: int
    offset: int
    attempt: int
    attempt_id: str
    instance: str
    request_id: str | None
    first_open_offset: int
    opened_at_ms: int
    last_progress_ms: int
    # The time of the ``open`` record of the current attempt; ``opened_at_ms`` is that of the first attempt.
    attempt_opened_at_ms: int = 0
    parts: int | None = None
    received_count: int = 0
    failed_parts: int = 0
    dlq_parts: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_replaced: int = 0
    deliveries: dict[str, SinkCounts] = field(default_factory=dict)
    first_failure: PartFailure | None = None
    last_result_offset: int | None = None
    crashed_attempts: int = 0
    cancelled: bool = False
    closed: ClosedOutcome | None = None
    # The offset of the ``closed`` record that closed the job: a writer of such a record can tell
    # whether the log took its record or the record of another owner.
    closed_offset: int | None = None
    received_bits: bytearray = field(default_factory=bytearray, repr=False)

    @property
    def is_complete(self) -> bool:
        """True when the number of parts is known and every part has a result."""
        return self.parts is not None and self.received_count == self.parts

    def has_result(self, index: int) -> bool:
        """True when part ``index`` of the current attempt has a counted result."""
        byte, bit = divmod(index, BITS_PER_BYTE)
        if index < 0 or byte >= len(self.received_bits):
            return False
        return bool(self.received_bits[byte] >> bit & 1)

    def snapshot(self) -> JobState:
        """A copy of the job as it is now, which later records of the log do not change.

        The end of a job works on one attempt from its start to its
        ``closed`` record; a new ``open`` resets the live state meanwhile.
        """
        return replace(
            self,
            deliveries={sink: counts.model_copy() for sink, counts in self.deliveries.items()},
            received_bits=bytearray(self.received_bits),
        )

    def due_kind(self, *, now_ms: int, job_timeout: int, idle_timeout: int) -> DueKind | None:
        """The timeout this job has passed, in seconds (``0`` is off); the job timeout wins over the idle one.

        A closed job and a job whose every part has a result are never due:
        a complete job only waits for its owner to end it. The idle timeout
        counts from ``split-done`` and from each new result, so it does not
        run while the split still publishes parts.
        """
        if self.closed is not None or self.is_complete:
            return None
        if job_timeout > 0 and now_ms - self.opened_at_ms >= job_timeout * MS_PER_SECOND:
            return 'job'
        idle_runs = idle_timeout > 0 and self.parts is not None
        if idle_runs and now_ms - self.last_progress_ms >= idle_timeout * MS_PER_SECOND:
            return 'idle'
        return None

    def missing_parts(self) -> list[int]:
        """The part numbers below ``parts`` with no result; empty while ``parts`` is unknown."""
        if self.parts is None:
            return []
        return [index for index in range(self.parts) if not self.has_result(index)]

    def is_split_left_behind(self, now_ms: int) -> bool:
        """Whether the split of the current attempt shows no end after the grace time.

        It has neither ``split-done`` nor ``split-cancelled``, and its
        ``open`` record is older than ``FOREIGN_SPLIT_GRACE_SECONDS``.
        """
        is_splitting = self.closed is None and self.parts is None and not self.cancelled
        waited_ms = now_ms - self.attempt_opened_at_ms
        return is_splitting and waited_ms >= FOREIGN_SPLIT_GRACE_SECONDS * MS_PER_SECOND

    def _mark(self, index: int) -> None:
        """Set the bit of part ``index``, growing the bit set in place."""
        byte, bit = divmod(index, BITS_PER_BYTE)
        if byte >= len(self.received_bits):
            self.received_bits.extend(bytes(byte + 1 - len(self.received_bits)))
        self.received_bits[byte] |= 1 << bit

    def _drop_results_from(self, parts: int) -> None:
        """Forget the bits of part numbers at or above ``parts`` and take them off ``received_count``."""
        kept_bytes, rest = divmod(parts, BITS_PER_BYTE)
        dropped = 0
        if rest and kept_bytes < len(self.received_bits):
            mask = (1 << rest) - 1
            dropped += (self.received_bits[kept_bytes] & ~mask).bit_count()
            self.received_bits[kept_bytes] &= mask
        if rest:
            kept_bytes += 1
        dropped += sum(value.bit_count() for value in self.received_bits[kept_bytes:])
        del self.received_bits[kept_bytes:]
        self.received_count -= dropped

    def _start_attempt(self, entry: LogEntry, instance: str, request_id: str | None, timestamp_ms: int) -> None:
        """Make ``entry`` the current attempt: new attempt fields, empty bit set and counters."""
        self.attempt = entry.ref.attempt
        self.attempt_id = entry.ref.attempt_id
        self.instance = instance
        self.request_id = request_id
        self.last_progress_ms = timestamp_ms
        self.attempt_opened_at_ms = timestamp_ms
        self.parts = None
        self.received_count = 0
        self.failed_parts = 0
        self.dlq_parts = 0
        self.tasks_succeeded = 0
        self.tasks_failed = 0
        self.tasks_replaced = 0
        self.deliveries = {}
        self.first_failure = None
        self.last_result_offset = None
        self.cancelled = False
        self.received_bits = bytearray()

    def _count(self, result: PartResult, offset: int, progress_ms: int) -> None:
        """Add one first result of a part to the counters; ``progress_ms`` is the new progress time."""
        self._mark(result.index)
        self.received_count += 1
        self.tasks_succeeded += result.tasks_succeeded
        self.tasks_failed += result.tasks_failed
        self.tasks_replaced += result.tasks_replaced
        for sink, counts in result.deliveries.items():
            total = self.deliveries.setdefault(sink, SinkCounts())
            total.delivered += counts.delivered
            total.skipped += counts.skipped
            total.failed += counts.failed
        if result.dlq is not None:
            self.dlq_parts += 1
        if result.outcome == 'failed':
            self.failed_parts += 1
            if self.first_failure is None or result.index < self.first_failure.index:
                self.first_failure = PartFailure(index=result.index, dlq=result.dlq, error=result.error)
        self.last_result_offset = offset
        self.last_progress_ms = progress_ms

    def _close(self, outcome: ClosedOutcome) -> None:
        """Mark the job closed and free what a closed job no longer needs.

        A closed job stays in the table until its source offset is committed,
        and a partition can hold many of them behind one open job. What stays
        is what a reader still asks: the partition and the offset, the first
        ``open`` offset (the restart position), the outcome, and the attempt
        and its id, by which a writer of a ``closed`` record tells its own
        record from the record of another owner, and the instance, which the
        event of the close names. The request id and the counters of the
        results are dropped.
        """
        self.closed = outcome
        self.received_bits = bytearray()
        self.deliveries = {}
        self.first_failure = None
        self.request_id = None
        self.received_count = 0
        self.failed_parts = 0
        self.dlq_parts = 0
        self.tasks_succeeded = 0
        self.tasks_failed = 0
        self.tasks_replaced = 0
        self.last_result_offset = None


@dataclass(frozen=True)
class Applied:
    """What one record did to the table."""

    job: JobState | None
    ignored: IgnoreReason | None = None
    opened: bool = False
    completed: bool = False
    closed: bool = False


def _ignored(job: JobState | None, reason: IgnoreReason) -> Applied:
    return Applied(job=job, ignored=reason)


def _is_count(value: Any) -> TypeGuard[int]:
    """A non-negative integer that is not a boolean."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


class JobTable:
    """The jobs of one source partition of one consumer group, built from the results log."""

    def __init__(self, *, partition: int, group: str) -> None:
        self._partition = partition
        self._group = group
        self._jobs: dict[int, JobState] = {}
        self._handlers: dict[str, Callable[[LogEntry, JobState | None, int, int, int], Applied]] = {
            'open': self._apply_open,
            'split-done': self._apply_split_done,
            'split-cancelled': self._apply_split_cancelled,
            'result': self._apply_result,
            'closed': self._apply_closed,
        }

    def __len__(self) -> int:
        return len(self._jobs)

    def apply(self, entry: LogEntry, *, offset: int, timestamp_ms: int, applied_at_ms: int | None = None) -> Applied:
        """Apply one record of the results log, read at ``offset`` with the Kafka timestamp ``timestamp_ms``.

        ``applied_at_ms`` is the clock of the reader at this call, the clock
        that later judges the idle timeout. The progress time of a job is
        the later of the two: the timestamp of a ``result`` comes from the
        clock of the worker that ran the part, and a clock that is behind
        must not make the idle timeout fire early. Without it the progress
        time is the timestamp of the record.
        """
        progress_ms = timestamp_ms if applied_at_ms is None else max(timestamp_ms, applied_at_ms)
        if entry.ref.group != self._group:
            return _ignored(None, IgnoreReason.FOREIGN_GROUP)
        if entry.ref.partition != self._partition:
            return _ignored(None, IgnoreReason.MALFORMED)
        job = self._jobs.get(entry.ref.offset)
        handler = self._handlers.get(entry.kind)
        if handler is None:
            return _ignored(job, IgnoreReason.MALFORMED)
        return handler(entry, job, offset, timestamp_ms, progress_ms)

    def job(self, source_offset: int) -> JobState | None:
        """The job of the source message at ``source_offset``, if the table has it."""
        return self._jobs.get(source_offset)

    def open_jobs(self) -> list[JobState]:
        """The jobs that are not closed, by source offset."""
        return sorted((job for job in self._jobs.values() if job.closed is None), key=lambda job: job.offset)

    def retire_through(self, committed_source_offset: int) -> list[JobState]:
        """Remove and return every job whose source offset is below the committed offset."""
        retired = [job for offset, job in self._jobs.items() if offset < committed_source_offset]
        for job in retired:
            del self._jobs[job.offset]
        return sorted(retired, key=lambda job: job.offset)

    def position(self, next_offset: int) -> int:
        """The results-log offset a restart reads from: the first ``open`` of the oldest job kept."""
        return min((job.first_open_offset for job in self._jobs.values()), default=next_offset)

    def due(self, *, now_ms: int, job_timeout: int, idle_timeout: int) -> list[tuple[JobState, DueKind]]:
        """The open jobs past a timeout with its kind, by the rule of :meth:`JobState.due_kind`."""
        found: list[tuple[JobState, DueKind]] = []
        for job in self.open_jobs():
            kind = job.due_kind(now_ms=now_ms, job_timeout=job_timeout, idle_timeout=idle_timeout)
            if kind is not None:
                found.append((job, kind))
        return found

    def _apply_open(
        self, entry: LogEntry, job: JobState | None, offset: int, timestamp_ms: int, progress_ms: int
    ) -> Applied:
        if job is not None and job.closed is not None:
            return _ignored(job, IgnoreReason.CLOSED_JOB)
        if job is not None and entry.ref.attempt_id == job.attempt_id:
            return _ignored(job, IgnoreReason.DUPLICATE)
        instance = entry.body.get('instance')
        if not isinstance(instance, str):
            return _ignored(job, IgnoreReason.MALFORMED)
        raw_request_id = entry.body.get('request_id')
        request_id = raw_request_id if isinstance(raw_request_id, str) else None
        if job is None:
            job = JobState(
                partition=entry.ref.partition,
                offset=entry.ref.offset,
                attempt=entry.ref.attempt,
                attempt_id=entry.ref.attempt_id,
                instance=instance,
                request_id=request_id,
                first_open_offset=offset,
                opened_at_ms=timestamp_ms,
                last_progress_ms=timestamp_ms,
                attempt_opened_at_ms=timestamp_ms,
            )
            self._jobs[job.offset] = job
        else:
            # An attempt that ended with neither split-done nor split-cancelled crashed its worker.
            if job.parts is None and not job.cancelled:
                job.crashed_attempts += 1
            job._start_attempt(entry, instance, request_id, timestamp_ms)
        return Applied(job=job, opened=True)

    def _current(self, entry: LogEntry, job: JobState | None) -> IgnoreReason | None:
        """Why a record of an attempt does not belong to the current attempt of an open job."""
        if job is None:
            return IgnoreReason.UNKNOWN_JOB
        if job.closed is not None:
            return IgnoreReason.CLOSED_JOB
        if entry.ref.attempt_id != job.attempt_id:
            return IgnoreReason.OLD_ATTEMPT
        return None

    def _apply_split_done(
        self, entry: LogEntry, job: JobState | None, offset: int, timestamp_ms: int, progress_ms: int
    ) -> Applied:
        reason = self._current(entry, job)
        if reason is not None:
            return _ignored(job, reason)
        job = cast(JobState, job)
        parts = entry.body.get('parts')
        # A count above the limit of one job is refused here, so no later reader walks it.
        if not _is_count(parts) or parts > MAX_PART_INDEX + 1:
            return _ignored(job, IgnoreReason.MALFORMED)
        if job.parts is not None:
            return _ignored(job, IgnoreReason.DUPLICATE)
        job.parts = parts
        # Results that came before split-done with a part number at or above ``parts``
        # leave the bit set so that ``is_complete`` is exact. Their task and delivery
        # counts cannot be taken back, because no result body is kept; a log written
        # by an owner never holds such a result.
        job._drop_results_from(parts)
        if job.first_failure is not None and job.first_failure.index >= parts:
            job.first_failure = None
        job.last_progress_ms = progress_ms
        return Applied(job=job, completed=job.is_complete)

    def _apply_split_cancelled(
        self, entry: LogEntry, job: JobState | None, offset: int, timestamp_ms: int, progress_ms: int
    ) -> Applied:
        reason = self._current(entry, job)
        if reason is not None:
            return _ignored(job, reason)
        job = cast(JobState, job)
        if job.cancelled:
            return _ignored(job, IgnoreReason.DUPLICATE)
        job.cancelled = True
        return Applied(job=job)

    def _apply_result(
        self, entry: LogEntry, job: JobState | None, offset: int, timestamp_ms: int, progress_ms: int
    ) -> Applied:
        reason = self._current(entry, job)
        if reason is not None:
            return _ignored(job, reason)
        job = cast(JobState, job)
        try:
            result = part_result_from_body(entry.body)
        except (KeyError, ValueError):
            return _ignored(job, IgnoreReason.MALFORMED)
        if result.index < 0 or result.index > MAX_PART_INDEX:
            return _ignored(job, IgnoreReason.MALFORMED)
        if job.parts is not None and result.index >= job.parts:
            return _ignored(job, IgnoreReason.UNKNOWN_JOB)
        if job.has_result(result.index):
            return _ignored(job, IgnoreReason.DUPLICATE)
        job._count(result, offset, progress_ms)
        return Applied(job=job, completed=job.is_complete)

    def _apply_closed(
        self, entry: LogEntry, job: JobState | None, offset: int, timestamp_ms: int, progress_ms: int
    ) -> Applied:
        outcome = entry.body.get('outcome')
        if job is not None and job.closed is not None:
            return _ignored(job, IgnoreReason.CLOSED_JOB)
        if job is not None and entry.ref.attempt_id != job.attempt_id:
            return _ignored(job, IgnoreReason.OLD_ATTEMPT)
        if not isinstance(outcome, str) or outcome not in CLOSED_OUTCOMES:
            return _ignored(job, IgnoreReason.MALFORMED)
        if job is None:
            # A closed record whose open is gone from the log still marks the job done,
            # so a restart never splits it again; the closed record keeps it in the table.
            job = JobState(
                partition=entry.ref.partition,
                offset=entry.ref.offset,
                attempt=entry.ref.attempt,
                attempt_id=entry.ref.attempt_id,
                instance='',
                request_id=None,
                first_open_offset=offset,
                opened_at_ms=timestamp_ms,
                last_progress_ms=timestamp_ms,
            )
            self._jobs[job.offset] = job
        job._close(cast(ClosedOutcome, outcome))
        job.closed_offset = offset
        return Applied(job=job, closed=True)
