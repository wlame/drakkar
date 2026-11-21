"""The owner of the split jobs of one source partition.

A ``SplitOwner`` belongs to the partition processor of one source partition.
For each accepted source message it runs ``split`` (``splitting.py``). Nothing
yielded: the message is local and runs on this worker. Something yielded: the
message is a job whose offset stays pending until the job ends
(``ending.py``).

The holder follows the log. While this worker owns the partition, the owner
never forgets a job whose source message it holds: for each held message it
acts on what the job table says about the job now, whichever worker instance
wrote the records. Another instance's attempt is followed, an attempt that
another instance cancelled or left behind is started again here, and a
``closed`` record completes the offset. So a job always has an owner that
acts on it.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

import structlog

from drakkar.groups import STREAM_CLOSE_TIMEOUT
from drakkar.metrics import split_jobs
from drakkar.models import SourceMessage
from drakkar.providers.resilience import RateLimitedLog, describe_error
from drakkar.sinks.dlq_record import cut_error
from drakkar.sinks.manager import CollectCallback, MessageDLQState, SinkManager
from drakkar.split import partmode
from drakkar.split.ending import ClosedRecord, EndFacts, EndSettings, JobEnder, local_facts
from drakkar.split.models import PartResult, TimedOutKind
from drakkar.split.producer import NO_OFFSET, SplitProducer
from drakkar.split.records import ClosedOutcome, JobRef, job_id, new_attempt_id, ref_id
from drakkar.split.splitting import CANCEL_WRITE_SECONDS, SplitEnd, SplitResult, SplitRun, SplitSettings
from drakkar.split.table import MS_PER_SECOND, JobState, JobTable
from drakkar.split.tracker import TimeoutKind

logger = structlog.get_logger()

# The failure of a new attempt whose split yields nothing for a job the results log already knows.
NO_PART_FAILURE = 'the split yielded no part for a job that has parts in the log'
# How long a wait for the job tracker to read a record of this owner may last before it is logged, and
# how often it is logged after that. The tracker reads a record back in about a second; a wait that
# lasts means the tracker reads nothing, and the jobs of the partition do not move.
READ_BACK_WARN_FIRST_SECONDS = 30.0
READ_BACK_WARN_NEXT_SECONDS = 60.0
# The waits between two tries to read the end of the results log, after a try that failed.
END_OFFSET_RETRY_FIRST_SECONDS = 0.5
END_OFFSET_RETRY_MAX_SECONDS = 5.0

SleepFn = Callable[[float], Awaitable[None]]


class _Settled(StrEnum):
    """What the job table shows once it has read a ``closed`` record this owner wrote."""

    # The job is closed: the offset completes.
    CLOSED = 'closed'
    # Another attempt is the current one, so every reader ignored the record: the job is still open.
    OPEN = 'open'
    # The owner stopped before the table had read the record.
    STOPPED = 'stopped'


class BeginOutcome(StrEnum):
    """What the owner made of one accepted source message. Only a local message goes on to ``arrange``."""

    # The split yielded nothing: the message runs on this worker.
    LOCAL = 'local'
    # The message is a job; its offset stays pending until the results log shows the job closed. Also a
    # job whose split failed, from the write of its ``closed`` record until the log shows it.
    JOB = 'job'
    # The results log says the job is closed: the owner completed the offset.
    DONE = 'done'
    # The split failed or its attempts are spent, and no record could say so: the owner stalled the offset.
    FAILED = 'failed'


class OwnerHost(Protocol):
    """What the owner needs of the partition processor it belongs to.

    The owner settles the offset of every message that is not local: it
    holds it while the job is open, and completes or stalls it at the end.
    """

    @property
    def deliveries_suppressed(self) -> bool:
        """Whether the processor gave its partition up after a drain that timed out."""
        ...

    def release_window_reservation(self) -> None:
        """Give back what the window reserved in the pending budget: the message has no task on this worker."""
        ...

    def hold_job_offset(self, offset: int) -> None:
        """Keep the offset of an open job pending and out of every drain."""
        ...

    def complete_job_offset(self, offset: int) -> None:
        """Complete the offset of a job that ended, and let the commit follow."""
        ...

    async def stall_offset(self, offset: int) -> None:
        """Leave ``offset`` pending for the life of the process."""
        ...

    def start_job_end(self, message: SourceMessage, end: Callable[[], Awaitable[bool | None]]) -> asyncio.Task:
        """Run ``end`` as a task of the processor.

        Its answer settles the offset of ``message``: true completes it,
        false stalls it, and None leaves it as the owner set it (held again
        for a job that is still open in the results log).
        """
        ...


@dataclass(frozen=True)
class OwnerSettings:
    """What every owner of one worker shares: the producer, the names, and the limits of a job."""

    producer: SplitProducer
    source_group: str
    parts_topic: str
    results_topic: str
    parts_partition_count: int
    worker: str
    instance: str
    max_attempts: int
    results_reader: Callable[[], Any]
    # The next offset the job tracker reads in the results log of a source partition; None while it
    # does not follow the partition (``JobTracker.read_position``).
    read_position: Callable[[int], int | None]
    # The end of the results log of a source partition as the broker has it now (the offset its next
    # record gets); None while the job tracker does not follow the partition (``JobTracker.end_offset``).
    end_offset: Callable[[int], Awaitable[int | None]]
    # ``split.job_timeout`` and ``split.idle_timeout`` in seconds; 0 is off.
    job_timeout: int = 0
    idle_timeout: int = 0
    # Epoch seconds; the clock the job tracker uses for its timeouts.
    clock: Callable[[], float] = time.time
    sleep: SleepFn = asyncio.sleep
    close_timeout: float = STREAM_CLOSE_TIMEOUT
    cancel_write_seconds: float = CANCEL_WRITE_SECONDS

    def split_settings(self) -> SplitSettings:
        """What a run of ``split`` needs."""
        return SplitSettings(
            producer=self.producer,
            parts_topic=self.parts_topic,
            results_topic=self.results_topic,
            parts_partition_count=self.parts_partition_count,
            worker=self.worker,
            instance=self.instance,
            sleep=self.sleep,
            close_timeout=self.close_timeout,
            cancel_write_seconds=self.cancel_write_seconds,
        )

    def end_settings(self) -> EndSettings:
        """What the end of a job needs."""
        return EndSettings(
            producer=self.producer,
            results_topic=self.results_topic,
            source_group=self.source_group,
            results_reader=self.results_reader,
            clock=self.clock,
            sleep=self.sleep,
        )


@dataclass
class _HeldJob:
    """A job whose source message this owner holds."""

    message: SourceMessage
    # Epoch seconds of the first ``open`` of the job, or of the start of its first split here.
    opened_at: float
    # The DLQ state of the original message, shared by the aggregate delivery and the DLQ record of the job.
    state: MessageDLQState = field(default_factory=MessageDLQState)
    # The last attempt this owner opened, and the number of parts its ``split-done`` carries.
    own_attempt_id: str | None = None
    own_parts: int | None = None
    # The current attempt of the results log when the last split started here; None when the log did
    # not have the job. A job the log has cannot become a local message, and a split that fails before
    # its own ``open`` closes this attempt.
    base_ref: JobRef | None = None
    # The highest offset of a record of the results log that this owner wrote for the job and that was
    # acknowledged (``open``, ``split-done``, ``split-cancelled``, ``closed``); -1 for none. The owner
    # decides about the job only on a table that has read past it.
    own_written_through: int = -1
    # Whether the log holds a record of this owner for the job whose delivery report named no offset,
    # and that ``own_written_through`` does not cover yet.
    own_unplaced: bool = False
    # Whether a task waits for the job tracker to read past ``own_written_through``.
    catching_up: bool = False
    # What this owner does for the job now; at most one of the three.
    run: SplitRun | None = None
    settling: bool = False
    end_task: asyncio.Task | None = None

    @property
    def busy(self) -> bool:
        """Whether a split, the write of a failure, or the end of the job is in progress."""
        return self.run is not None or self.settling or self.end_task is not None


class SplitOwner:
    """Splits the messages of one source partition and ends their jobs.

    Every method runs on the event loop. ``begin`` is called by the partition
    loop, one message at a time. The event methods are called by the job
    tracker and return at once. A split runs as a task of the owner; the end
    of a job runs as a task of the processor, one for each job.
    """

    def __init__(
        self,
        settings: OwnerSettings,
        *,
        partition: int,
        table: JobTable,
        host: OwnerHost,
        handler: Any,
        sink_manager: SinkManager,
        on_collect: CollectCallback,
        on_dlq_failure: str,
        partition_label: int | None = None,
    ) -> None:
        self._settings = settings
        self._split_settings = settings.split_settings()
        self._partition = partition
        self._label = partition if partition_label is None else partition_label
        self._table = table
        self._host = host
        self._handler = handler
        self._stopping = False
        self._stopped = asyncio.Event()
        # One future for each wait for the next read of the job tracker in the results log of this
        # partition; a read resolves them all, so no waiter can take the wake of another.
        self._read_waiters: set[asyncio.Future[None]] = set()
        self._log_limit = RateLimitedLog(time.monotonic)
        self._ender = JobEnder(
            settings.end_settings(),
            partition=partition,
            partition_label=self._label,
            handler=handler,
            sink_manager=sink_manager,
            on_collect=on_collect,
            on_dlq_failure=on_dlq_failure,
            stopped=self._stopped,
            deliveries_suppressed=lambda: host.deliveries_suppressed,
            log_limit=self._log_limit,
        )
        # The open jobs whose source message this owner holds, by source offset.
        self._jobs: dict[int, _HeldJob] = {}
        # The open jobs of the restored table whose source message has not arrived yet. One whose
        # message never arrives is closed as orphaned when a later message is accepted.
        self._restored_waiting: set[int] = {job.offset for job in table.open_jobs()}
        # The job whose ``begin`` is waiting now: only its split gives the window reservation back.
        self._beginning: _HeldJob | None = None
        self._runs: set[SplitRun] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._awaited: set[asyncio.Task[Any]] = set()

    # --- what the processor calls ---

    def signal_stop(self) -> None:
        """A stop or a revoke: cancel every split in progress and end every wait. Nothing new starts."""
        self._stopping = True
        self._stopped.set()
        for run in list(self._runs):
            run.interrupt('stop')

    async def stop(self, timeout: float) -> None:
        """Wait at most ``timeout`` seconds for the tasks of the owner, then cancel what is left.

        After ``signal_stop`` these are the cancels of the splits that were
        in progress: each closes its generator and marks its attempt as
        cancelled in the results log.
        """
        tasks = [task for task in self._tasks if not task.done()]
        if not tasks:
            return
        _, late = await asyncio.wait(tasks, timeout=max(timeout, 0.0))
        for task in late:
            task.cancel()
        if late:
            await asyncio.gather(*late, return_exceptions=True)

    async def begin(self, message: SourceMessage) -> BeginOutcome:
        """Decide what one accepted source message is: a local message, a job, a closed job, or a failed one.

        A job that the results log already knows is continued from what the
        job table says about it now; no event that came before the message
        is needed. A closed job completes its offset. A job whose split
        still has to be made (no ``split-done``, and no timeout passed)
        starts its next attempt, unless the attempts are spent. Any other
        job is held, and the rules of a holder act on it at once. Every
        restored job with a lower offset whose message did not arrive is
        closed as orphaned. At a stop this returns at once: the offset is
        held, and a split in progress is cancelled in its own task.
        """
        offset = message.offset
        if self._stopping:
            self._host.hold_job_offset(offset)
            return BeginOutcome.JOB
        self._close_orphans_below(offset)
        self._restored_waiting.discard(offset)
        job = self._table.job(offset)
        if job is None:
            held = _HeldJob(message=message, opened_at=self._settings.clock())
            return await self._until_stop(held, self._start_attempt(held, None))
        if job.closed is not None:
            logger.info(
                'split_job_already_closed',
                category='split',
                partition=self._label,
                job=job_id(self._partition, job.offset, job.attempt),
                outcome=job.closed,
            )
            self._host.complete_job_offset(offset)
            return BeginOutcome.DONE
        held = _HeldJob(message=message, opened_at=job.opened_at_ms / MS_PER_SECOND)
        if job.parts is None and self._due_kind(job) is None:
            return await self._until_stop(held, self._start_attempt(held, job))
        self._hold(held)
        logger.info(
            'split_job_resumed',
            category='split',
            partition=self._label,
            job=job_id(self._partition, job.offset, job.attempt),
            attempt=job.attempt,
            parts=job.parts,
            received=job.received_count,
        )
        self._reconcile(held)
        return BeginOutcome.JOB

    async def end_local(self, message: SourceMessage, result: PartResult) -> bool:
        """End a local message as a job of one part; no record is written to any topic.

        True when the offset may complete, false when it must stall.

        Raises:
            asyncio.CancelledError: a stop ended a wait; the offset stays pending.
        """
        rebuild = functools.partial(local_facts, message, result)
        ended = await self._ender.end(message, MessageDLQState(), rebuild(), '', rebuild=rebuild, closing_ref=None)
        # A local message writes no record, so the answer is never one.
        return ended is True

    # --- what the job tracker tells ---
    #
    # An event carries no decision. It wakes the rules of a holder, which read
    # the job table; so an event that came before the owner held the message
    # is never needed. Only a split in progress is told directly to stop.

    def job_complete(self, job: JobState) -> None:
        """Every part of the current attempt of ``job`` has a result."""
        self._job_changed(job)

    def job_timed_out(self, job: JobState, kind: TimeoutKind) -> None:
        """``job`` passed a timeout: a split of that attempt that runs here stops, and the job ends."""
        held = self._jobs.get(job.offset)
        if held is not None and held.run is not None and held.run.ref.attempt_id == job.attempt_id:
            # The job ends when the run has stopped.
            held.run.interrupt(kind)
        self._job_changed(job)

    def attempt_opened(self, job: JobState) -> None:
        """An ``open`` record made a new attempt the current one of ``job``.

        When it replaces the attempt this owner is splitting, the split
        stops: the log has one current attempt, and every owner follows it.
        An ``open`` of another instance that was read before this owner's
        own is older in the log, so the own attempt replaces it.
        """
        held = self._jobs.get(job.offset)
        if held is not None and held.run is not None:
            run = held.run
            if job.attempt_id == run.ref.attempt_id:
                run.own_open_seen = True
            elif run.own_open_seen:
                run.interrupt('superseded')
        self._job_changed(job)

    def attempt_cancelled(self, job: JobState) -> None:
        """The current attempt of ``job`` was cancelled; when another instance did it, the split starts again here."""
        self._job_changed(job)

    def attempt_stalled(self, job: JobState) -> None:
        """Another instance's split of ``job`` shows no end after the grace time: that owner is taken for dead."""
        self._job_changed(job)

    def job_closed(self, job: JobState) -> None:
        """``job`` got its ``closed`` record, from this owner or from another: closed means done."""
        self._restored_waiting.discard(job.offset)
        held = self._jobs.get(job.offset)
        if held is not None and held.run is not None:
            # The offset completes when the run has stopped.
            held.run.interrupt('closed')
        self._job_changed(job)

    def log_read(self) -> None:
        """The job tracker read on in the results log of this partition: who waits for a record looks again."""
        waiters, self._read_waiters = self._read_waiters, set()
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _job_changed(self, job: JobState) -> None:
        """The results log said something new about ``job``: let the rules of a holder act, if it is held."""
        held = self._jobs.get(job.offset)
        if held is not None:
            self._reconcile(held)

    # --- the rules of a holder ---

    def _reconcile(self, held: _HeldJob) -> None:
        """Act on what the job table says now about a held job.

        Called when the owner takes hold of a job, for every event of the
        job, and whenever something this owner did for it has ended. All it
        needs is in the table and on the clock, so it does not matter which
        events came before the message. While the owner is busy with the job
        (a split, the write of a failure, the end) nothing new starts.

        The owner decides only on a table that has read every record the
        owner itself wrote for the job. The tracker reads a record about a
        second after its acknowledgement; until then the table shows the job
        as it was before, and a rule would take the owner's own new attempt
        for missing: it would split again, or end the job on the older
        attempt. So while the tracker has not read past the last record of
        this owner, nothing is decided, and the rules run again when it has.

        The steps, in this fixed order; the first that holds decides:

        1. the job is closed: complete the offset with no hook;
        2. every part of the current attempt has a result: end the job
           through ``aggregate`` (a complete job is never timed out);
        3. a timeout of the job has passed, by ``split.job_timeout`` and
           ``split.idle_timeout`` on the tracker's clock: end it through
           ``on_job_timeout``;
        4. the current attempt has ``split-done``: wait for its results;
        5. the current attempt is of another instance and was cancelled:
           start the next attempt here;
        6. the current attempt is of another instance and shows no end of
           its split after the grace time: start the next attempt here;
        7. else wait: another instance is splitting, or the tracker has not
           read the records of this owner's own attempt yet.
        """
        offset = held.message.offset
        if self._stopping or held.busy or self._jobs.get(offset) is not held:
            return
        if not self._table_has_own_records(held):
            self._reconcile_when_read(held)
            return
        job = self._table.job(offset)
        if job is None:
            return
        due = self._due_kind(job)
        if job.closed is not None:
            self._complete_closed(held, job)
        elif job.is_complete:
            self._start_end(held, job, '')
        elif due is not None:
            self._start_end(held, job, due)
        elif job.parts is None and job.attempt_id != held.own_attempt_id:
            now_ms = int(self._settings.clock() * MS_PER_SECOND)
            if job.cancelled or job.is_split_left_behind(now_ms):
                self._start_attempt(held, job)

    def _table_has_own_records(self, held: _HeldJob) -> bool:
        """Whether the job tracker has read every record this owner wrote for the job."""
        if held.own_unplaced:
            return False
        return held.own_written_through < 0 or self._has_read(held.own_written_through)

    def _has_read(self, offset: int) -> bool:
        """Whether the job table holds the record at ``offset`` of the results log and every record before it."""
        position = self._settings.read_position(self._partition)
        return position is not None and position > offset

    def _reconcile_when_read(self, held: _HeldJob) -> None:
        """Let the rules of a holder act once the job tracker has read the records of this owner; one task for a job."""
        if not held.catching_up:
            held.catching_up = True
            self._spawn(self._catch_up(held))

    async def _catch_up(self, held: _HeldJob) -> None:
        try:
            read = await self._place_own_records(held) and await self._wait_for_table(
                held, lambda: self._has_read(held.own_written_through), stop_grace=0.0
            )
        finally:
            held.catching_up = False
        if read:
            self._reconcile(held)

    async def _place_own_records(self, held: _HeldJob) -> bool:
        """Find how far the log holds the records of this owner whose delivery report named no offset.

        Such a record was acknowledged, so it lies below the end the log has
        afterwards. The end is read after the acknowledgement, and the last
        offset before it stands for the record: once the tracker has read
        past it, the table has the record. False when the owner stopped
        before the end could be read; the records are then still unplaced.
        """
        while held.own_unplaced:
            # Cleared first: a record that is acknowledged during the read sets it again.
            held.own_unplaced = False
            end: int | None = None
            try:
                end = await self._end_of_log()
            finally:
                if end is None:
                    held.own_unplaced = True
            if end is None:
                return False
            held.own_written_through = max(held.own_written_through, end - 1)
        return True

    async def _end_of_log(self) -> int | None:
        """The end of the results log of this partition as the broker has it now; None when the owner stopped first.

        A read that fails is tried again after 0.5, 1, 2 ... 5 seconds, for
        as long as it takes: without the end the owner could not tell when
        the table has a record, and it never decides without that.
        """
        wait = END_OFFSET_RETRY_FIRST_SECONDS
        while not self._stopped.is_set():
            read = await self._unless_stopped(self._settings.end_offset(self._partition))
            if read is None:
                break
            error = read.exception()
            end = None if error is not None else read.result()
            if end is not None and end > 0:
                return end
            suppressed = self._log_limit.admit('end_offset')
            if suppressed is not None:
                logger.warning(
                    'split_end_offset_read_failed',
                    category='split',
                    partition=self._label,
                    error=describe_error(error) if error is not None else 'the job tracker gave no end of the log',
                    suppressed=suppressed,
                    hint='the end of the results log is read again; the jobs of the partition wait for it',
                )
            await self._unless_stopped(self._settings.sleep(wait))
            wait = min(wait * 2, END_OFFSET_RETRY_MAX_SECONDS)
        return None

    async def _unless_stopped(self, work: Awaitable[Any]) -> asyncio.Future[Any] | None:
        """Wait for ``work`` or for a stop; the finished work, or None when the stop came first.

        Work that the stop overtook is cancelled. An error of the work stays
        in the returned future.
        """
        task = asyncio.ensure_future(work)
        stop = asyncio.ensure_future(self._stopped.wait())
        try:
            await asyncio.wait({task, stop}, return_when=asyncio.FIRST_COMPLETED)
            return task if task.done() else None
        finally:
            stop.cancel()
            if not task.done():
                task.cancel()

    async def _wait_for_table(self, held: _HeldJob, ready: Callable[[], bool], *, stop_grace: float) -> bool:
        """Wait until ``ready`` holds for the job table; false when the owner stopped first.

        ``ready`` is asked again after every read of the job tracker in the
        results log of this partition. After a stop the wait gets
        ``stop_grace`` seconds more, then it ends, so a tracker that reads
        nothing holds no drain. Outside a stop it lasts as long as it takes,
        and a wait that lasts is logged.
        """
        waited = 0.0
        next_warning = READ_BACK_WARN_FIRST_SECONDS
        grace_ends: float | None = None
        while True:
            if ready():
                return True
            if self._stopped.is_set():
                if grace_ends is None:
                    grace_ends = time.monotonic() + stop_grace
                left = grace_ends - time.monotonic()
                if left <= 0:
                    return False
                await self._next_read(left, watch_stop=False)
            elif await self._next_read(next_warning, watch_stop=True):
                waited += next_warning
                next_warning = READ_BACK_WARN_NEXT_SECONDS
                self._warn_read_back(held, waited)

    def _warn_read_back(self, held: _HeldJob, waited: float) -> None:
        """Log a wait for the job tracker that lasts; one line for the partition in each interval."""
        suppressed = self._log_limit.admit('read_back')
        if suppressed is None:
            return
        logger.warning(
            'split_read_back_waiting',
            category='split',
            partition=self._label,
            job=f'{self._partition}:{held.message.offset}',
            offset=held.own_written_through,
            seconds=waited,
            suppressed=suppressed,
            hint='the job tracker has not read a record this worker wrote; the job waits for it',
        )

    async def _next_read(self, seconds: float, *, watch_stop: bool) -> bool:
        """Wait for the next read of the job tracker, for a stop when asked, or ``seconds``; true when the time passed.

        The wait has a future of its own, which is in place before this
        coroutine first gives the loop a turn. So a read that comes after
        the caller looked at the table always ends the wait, however many
        waits the partition has.
        """
        read: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._read_waiters.add(read)
        waits: set[asyncio.Future[Any]] = {read}
        if watch_stop:
            waits.add(asyncio.ensure_future(self._stopped.wait()))
        try:
            done, _ = await asyncio.wait(waits, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
            return not done
        finally:
            self._read_waiters.discard(read)
            for wait in waits:
                wait.cancel()

    def _due_kind(self, job: JobState) -> TimeoutKind | None:
        """The timeout ``job`` has passed now, by the rule and the clock the job tracker uses."""
        settings = self._settings
        return job.due_kind(
            now_ms=int(settings.clock() * MS_PER_SECOND),
            job_timeout=settings.job_timeout,
            idle_timeout=settings.idle_timeout,
        )

    def _complete_closed(self, held: _HeldJob, job: JobState) -> None:
        """Complete the offset of a held job that the results log has as closed; no hook runs."""
        self._drop(held)
        logger.info(
            'split_job_closed_elsewhere',
            category='split',
            partition=self._label,
            job=job_id(self._partition, job.offset, job.attempt),
            outcome=job.closed,
        )
        self._host.complete_job_offset(held.message.offset)

    def _hold(self, held: _HeldJob) -> None:
        """Take a message as a job: the events of its job reach it, and its offset stays out of every drain."""
        offset = held.message.offset
        if self._jobs.get(offset) is not held:
            self._jobs[offset] = held
            self._host.hold_job_offset(offset)

    def _drop(self, held: _HeldJob) -> None:
        """Stop holding a job whose offset this owner has settled."""
        if self._jobs.get(held.message.offset) is held:
            del self._jobs[held.message.offset]

    def _ref_of(self, job: JobState) -> JobRef:
        """The current attempt of a job of the table."""
        return JobRef(
            group=self._settings.source_group,
            partition=self._partition,
            offset=job.offset,
            attempt=job.attempt,
            attempt_id=job.attempt_id,
        )

    def _is_closed(self, offset: int) -> bool:
        """Whether the results log has the job of ``offset`` as closed."""
        job = self._table.job(offset)
        return job is not None and job.closed is not None

    def _close_orphans_below(self, offset: int) -> None:
        """Close every restored job below ``offset`` whose source message did not arrive.

        The source gives its messages in offset order, so such a message was
        passed: it expired, or the position of the group was moved. The
        ``closed`` record is written by a task of the owner, so ``begin``
        does not wait for the results topic.
        """
        for orphan in sorted(candidate for candidate in self._restored_waiting if candidate < offset):
            self._restored_waiting.discard(orphan)
            job = self._table.job(orphan)
            if job is None or job.closed is not None:
                continue
            split_jobs.labels(outcome='orphaned').inc()
            logger.warning(
                'split_job_orphaned',
                category='split',
                partition=self._label,
                job=job_id(self._partition, job.offset, job.attempt),
                received=job.received_count,
                parts=job.parts,
                hint='the source no longer gives the message of this job; it is closed with nothing delivered',
            )
            self._spawn(
                self._ender.write_closed(
                    self._ref_of(job),
                    'orphaned',
                    received=job.received_count,
                    parts=job.parts,
                    duration=self._settings.clock() - job.opened_at_ms / MS_PER_SECOND,
                )
            )

    # --- the split of a job ---

    def _start_attempt(self, held: _HeldJob, job: JobState | None) -> asyncio.Task[BeginOutcome]:
        """Start the next attempt of a job as a task of the owner, or its failure when the attempts are spent.

        ``job`` is what the table has about the job; None for a message the
        log does not know. The table counts an attempt as crashed only when
        a later ``open`` arrives, so the current attempt without
        ``split-done`` and without ``split-cancelled`` is one more.
        """
        if job is not None:
            spent = job.crashed_attempts + (0 if job.cancelled else 1)
            if spent >= self._settings.max_attempts:
                held.settling = True
                # The attempt that the failure closes is named now, with the decision.
                return self._spawn(self._fail_spent_attempts(held, self._ref_of(job), spent))
        attempt = 1 if job is None else job.attempt + 1
        ref = JobRef(
            group=self._settings.source_group,
            partition=self._partition,
            offset=held.message.offset,
            attempt=attempt,
            attempt_id=new_attempt_id(),
        )
        run = SplitRun(
            self._split_settings,
            handler=self._handler,
            message=held.message,
            ref=ref,
            partition_label=self._label,
            on_first_part=functools.partial(self._on_first_part, held),
            crashed_attempts=None if job is None else job.crashed_attempts + (0 if job.cancelled else 1),
            log_limit=self._log_limit,
        )
        held.run = run
        held.base_ref = None if job is None else self._ref_of(job)
        held.own_attempt_id = ref.attempt_id
        held.own_parts = None
        self._runs.add(run)
        return self._spawn(self._run_split(held, run))

    def _on_first_part(self, held: _HeldJob) -> None:
        """A split yielded its first part: the message is a job from here on."""
        self._release_reservation(held)
        self._hold(held)

    def _release_reservation(self, held: _HeldJob) -> None:
        """Give the window reservation back when ``begin`` waits for this job; a job has no task on this worker."""
        if self._beginning is held:
            self._host.release_window_reservation()

    async def _until_stop(self, held: _HeldJob, task: asyncio.Task[BeginOutcome]) -> BeginOutcome:
        """Wait in ``begin`` for what the owner does with a new message; a stop ends the wait at once.

        After a stop the task goes on by itself (the cancel of a split, the
        write of a failure), and the offset is held out of the drain.
        """
        self._beginning = held
        self._awaited.add(task)
        stop = asyncio.ensure_future(self._stopped.wait())
        try:
            await asyncio.wait({task, stop}, return_when=asyncio.FIRST_COMPLETED)
            if task.done():
                return task.result()
        finally:
            stop.cancel()
            self._awaited.discard(task)
            self._beginning = None
        self._host.hold_job_offset(held.message.offset)
        return BeginOutcome.JOB

    async def _run_split(self, held: _HeldJob, run: SplitRun) -> BeginOutcome:
        """Run one attempt of the split of a held message and act on how it ended."""
        offset = held.message.offset
        try:
            result = await run.run()
        finally:
            held.run = None
            self._runs.discard(run)
        held.own_written_through = max(held.own_written_through, result.written_through)
        held.own_unplaced = held.own_unplaced or result.unplaced_record
        is_held = self._jobs.get(offset) is held
        if result.end is SplitEnd.LOCAL and held.base_ref is None:
            return BeginOutcome.LOCAL
        if result.end is SplitEnd.LOCAL:
            # Parts of an earlier attempt may have run already, so the message cannot run here as a
            # local message too; in ``begin`` and in a takeover alike the job fails.
            return await self._fail_split(held, run, SplitResult(end=SplitEnd.FAILED, failure=NO_PART_FAILURE))
        if result.end is SplitEnd.FAILED:
            return await self._fail_split(held, run, result)
        if result.reason == 'superseded':
            self._note_superseded(run.ref)
        if result.end is SplitEnd.DONE:
            held.own_parts = result.parts
        if is_held:
            self._reconcile(held)
        else:
            # Stopped before its first part: nothing was written, and the next owner begins the message.
            self._host.hold_job_offset(offset)
        return BeginOutcome.JOB

    async def _fail_split(self, held: _HeldJob, run: SplitRun, result: SplitResult) -> BeginOutcome:
        """End a job whose split failed: one DLQ record with stage ``split``, then ``closed``."""
        held.settling = True
        self._release_reservation(held)
        logger.error(
            'split_failed',
            category='split',
            partition=self._label,
            job=ref_id(run.ref),
            error=result.failure,
            exc_info=result.cause if result.cause is not None else False,
        )
        # The attempt that ``closed`` names is taken now, with the decision. A split whose own
        # ``open`` is not in the log (it failed before the record, or the record was refused for
        # good) closes the attempt the log had when the split started: a ``closed`` of an attempt
        # the log never saw would be ignored for a job the log knows.
        ref = run.ref if result.open_acknowledged or held.base_ref is None else held.base_ref
        error = f'split failed: {cut_error(result.failure)}'
        may_complete = await self._ender.send_to_dlq(
            held.message, held.state, stage='split', error=error, attempts=None
        )
        return await self._close_failed(held, ref, 'split_failed', may_complete=may_complete)

    async def _fail_spent_attempts(self, held: _HeldJob, ref: JobRef, spent: int) -> BeginOutcome:
        """End a job whose split did not finish in ``split.max_attempts`` attempts: one DLQ record, then ``closed``.

        ``ref`` is the last attempt that was counted, named when the failure
        was decided.
        """
        self._release_reservation(held)
        error = f'split did not finish in {spent} attempts'
        logger.error(
            'split_failed', category='split', partition=self._label, job=ref_id(ref), error=error, attempts=spent
        )
        may_complete = await self._ender.send_to_dlq(
            held.message, held.state, stage='split', error=error, attempts=spent
        )
        return await self._close_failed(held, ref, 'attempts_spent', may_complete=may_complete)

    async def _close_failed(
        self, held: _HeldJob, ref: JobRef, outcome: ClosedOutcome, *, may_complete: bool
    ) -> BeginOutcome:
        """Write ``closed`` for a job whose split failed; its offset is settled later, by what the log shows.

        With no DLQ record under ``stall`` nothing is closed, so a restart
        tries the job again. A stop during the retries of the write leaves
        the offset pending. Once the record is acknowledged the job is held
        like any open job, and a task of the owner waits for the job table
        to read the record (:meth:`_settle_failed`): the caller, which can
        be the partition loop in ``begin``, does not wait for the tracker.
        """
        offset = held.message.offset
        if not may_complete:
            self._drop(held)
            await self._host.stall_offset(offset)
            return BeginOutcome.FAILED
        duration = self._settings.clock() - held.opened_at
        written = await self._ender.write_closed(ref, outcome, received=0, parts=None, duration=duration)
        if written == 'stopped':
            self._host.hold_job_offset(offset)
            return BeginOutcome.JOB
        if written == 'final':
            self._drop(held)
            await self._host.stall_offset(offset)
            return BeginOutcome.FAILED
        record = ClosedRecord(offset=written, attempt_id=ref.attempt_id, outcome=outcome, duration=duration)
        self._hold(held)
        self._spawn(self._settle_failed(held, record))
        return BeginOutcome.JOB

    async def _settle_failed(self, held: _HeldJob, record: ClosedRecord) -> None:
        """Settle the offset of a job whose split failed, once the job table has read its ``closed`` record.

        The log has the job as closed: the offset completes. Another
        instance opened a newer attempt before the record: the log ignores
        the record and the job goes on under that attempt, which this owner
        follows. The DLQ record of the failure is written by then; a job may
        so end with a DLQ record and with a result, as it may with two
        owners. A stop before the record is read leaves the offset pending.
        """
        settled = await self._settle_closed(held, record)
        if settled is _Settled.CLOSED:
            self._host.complete_job_offset(held.message.offset)
        elif settled is _Settled.OPEN:
            self._follow_newer_attempt(held)

    def _note_superseded(self, ref: JobRef) -> None:
        """Count and log an attempt of this owner that another instance's attempt replaced."""
        split_jobs.labels(outcome='superseded').inc()
        logger.warning(
            'split_job_superseded',
            category='split',
            partition=self._label,
            job=ref_id(ref),
            hint='another instance started a new attempt of this job; this owner stopped its split and follows that attempt',
        )

    # --- the end of a job ---

    def _start_end(self, held: _HeldJob, job: JobState, timed_out: TimedOutKind) -> None:
        """Start the end of a job as a task of the processor, once for each job."""
        if timed_out:
            logger.warning(
                'split_job_timed_out',
                category='split',
                partition=self._label,
                job=job_id(self._partition, job.offset, job.attempt),
                kind=timed_out,
                received=job.received_count,
                parts=job.parts,
            )
        # The copy is made with the decision: the end works on the attempt that was judged, also
        # when a newer ``open`` is read before the task of the end has run its first step.
        end = functools.partial(self._end_held, held, job.snapshot(), timed_out)
        held.end_task = self._host.start_job_end(held.message, end)

    async def _end_held(self, held: _HeldJob, attempt: JobState, timed_out: TimedOutKind) -> bool | None:
        """End a held job on ``attempt``, a copy of the job, then settle its offset by what the log shows.

        True completes the offset: the results log has the job as closed.
        False stalls it. None leaves it held: another instance opened a
        newer attempt. When that came before the hook was called, nothing is
        ended. When it came while the end ran, the end still ran to its
        finish, but its ``closed`` record is ignored by every reader of the
        log, so the job is still open. In both cases the owner follows the
        newer attempt.
        """
        offset = held.message.offset
        state = self._table.job(offset)
        if state is None or state.closed is not None:
            # Closed by another owner, or retired by a commit, before this end began: done.
            self._drop(held)
            return True
        if state.attempt_id != attempt.attempt_id:
            return self._follow_newer_attempt(held)
        try:
            ended = await self._end_attempt(held, attempt, self._facts(held, attempt, timed_out), timed_out)
        except asyncio.CancelledError:
            # A stop: the job stays held, and the next owner ends it.
            raise
        except Exception:
            self._drop(held)
            raise
        if ended is False:
            self._drop(held)
            return False
        settled = await self._settle_closed(held, ended if isinstance(ended, ClosedRecord) else None)
        if settled is _Settled.STOPPED:
            raise asyncio.CancelledError('the worker stops before the results log showed the end of the job')
        if settled is _Settled.OPEN:
            return self._follow_newer_attempt(held)
        return True

    async def _end_attempt(
        self, held: _HeldJob, attempt: JobState, facts: EndFacts, timed_out: TimedOutKind
    ) -> bool | ClosedRecord:
        """Run the end of a job on one attempt: the hook, its answer and ``closed`` all use ``attempt``.

        ``attempt`` is a snapshot of the job. A new ``open`` of another
        instance resets the live state of the job; the view of the hook, the
        results it reads back, a second call of the hook, the DLQ record and
        the ``closed`` record must not move to that newer attempt half way.
        """
        offset = held.message.offset
        ref = self._ref_of(attempt)
        return await self._ender.end(
            held.message,
            held.state,
            facts,
            timed_out,
            rebuild=functools.partial(self._facts, held, attempt, timed_out),
            # No second record for a job the log has as closed already.
            closing_ref=lambda: None if self._is_closed(offset) else ref,
        )

    async def _settle_closed(self, held: _HeldJob, record: ClosedRecord | None) -> _Settled:
        """Say what became of a job after this owner wrote ``closed`` for it; every such record ends here.

        The offset of a job completes only when the results log has a
        ``closed`` for it. The record of this owner is ignored when another
        instance's ``open`` came before it in the log, and only the job
        table knows the order. So the owner waits until the table has judged
        the record (:meth:`_closed_was_judged`), then looks at it: the job
        is closed (or retired), and the offset may complete; or another
        attempt is current, and the job is still open. The end of the job is
        counted and logged only when the log took this very record.
        ``record`` is None when the log had the job as closed already and
        nothing was written.
        """
        offset = held.message.offset
        if record is not None:
            if record.offset == NO_OFFSET:
                held.own_unplaced = True
                if not await self._place_own_records(held):
                    return _Settled.STOPPED
            else:
                held.own_written_through = max(held.own_written_through, record.offset)
            judged = functools.partial(self._closed_was_judged, offset, record.attempt_id, held.own_written_through)
            if not await self._wait_for_table(held, judged, stop_grace=partmode.STOP_GRACE_SECONDS):
                return _Settled.STOPPED
        job = self._table.job(offset)
        if job is None or job.closed is not None:
            if record is not None and job is not None and self._log_took(job, record, held.own_written_through):
                self._ender.note_closed(record)
            self._drop(held)
            return _Settled.CLOSED
        logger.warning(
            'split_job_end_superseded',
            category='split',
            partition=self._label,
            job=job_id(self._partition, offset, job.attempt),
            hint='another instance opened a newer attempt before the closed record of this worker; the job stays open',
        )
        return _Settled.OPEN

    def _closed_was_judged(self, offset: int, attempt_id: str, through: int) -> bool:
        """Whether the job table shows what the log made of a ``closed`` record this owner wrote for ``attempt_id``.

        The tracker must have read past ``through``, where the record lies
        at the latest. Then the table must show the job as closed (or
        retired), or another attempt as the current one. While it still
        shows the attempt of the record as open, the record has not been
        read, wherever the tracker is: a reader that takes no ``closed`` for
        the current attempt does not exist. So the owner never takes its own
        unread record for a newer attempt, and never ends a job twice for it.
        """
        if not self._has_read(through):
            return False
        job = self._table.job(offset)
        return job is None or job.closed is not None or job.attempt_id != attempt_id

    @staticmethod
    def _log_took(job: JobState, record: ClosedRecord, through: int) -> bool:
        """Whether the ``closed`` record that closed ``job`` in the table is ``record``.

        A record with a known offset is told by its offset. One whose
        delivery report named no offset is told by what it says and by where
        it can lie: the same attempt and outcome, at or below ``through``.
        """
        if record.offset != NO_OFFSET:
            return job.closed_offset == record.offset
        same = job.attempt_id == record.attempt_id and job.closed == record.outcome
        return same and job.closed_offset is not None and job.closed_offset <= through

    def _follow_newer_attempt(self, held: _HeldJob) -> None:
        """Go on with a job that is still open under an attempt newer than the one this owner ended or failed.

        The offset goes back among the open jobs first, then the rules of a
        holder act on the newer attempt; they may start its end at once. For
        the task of an end the answer None tells the processor to leave the
        offset as it is.
        """
        offset = held.message.offset
        held.end_task = None
        held.settling = False
        self._jobs[offset] = held
        self._host.hold_job_offset(offset)
        self._reconcile(held)

    def _facts(self, held: _HeldJob, attempt: JobState, timed_out: TimedOutKind) -> EndFacts:
        """The view of one attempt of a held job for its hook."""
        # The tracker may not have read the ``split-done`` this owner wrote for the attempt yet.
        parts_written = held.own_parts if held.own_attempt_id == attempt.attempt_id else None
        return self._ender.job_facts(held.message, attempt, timed_out, parts_written=parts_written)

    # --- the tasks of the owner ---

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start a task the owner keeps until it ends; ``stop`` waits for it."""
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        """Forget a task of the owner; log an error that ended it and that ``begin`` does not receive."""
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and task not in self._awaited:
            logger.error(
                'split_owner_task_failed',
                category='split',
                partition=self._label,
                error=describe_error(error),
                exc_info=error,
            )
