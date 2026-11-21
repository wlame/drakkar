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
from drakkar.split.ending import EndFacts, EndSettings, JobEnder, local_facts
from drakkar.split.models import PartResult, SplitJob, TimedOutKind
from drakkar.split.producer import SplitProducer
from drakkar.split.records import ClosedOutcome, JobRef, job_id, new_attempt_id, ref_id
from drakkar.split.splitting import CANCEL_WRITE_SECONDS, SplitEnd, SplitResult, SplitRun, SplitSettings
from drakkar.split.table import MS_PER_SECOND, JobState, JobTable
from drakkar.split.tracker import TimeoutKind

logger = structlog.get_logger()

SleepFn = Callable[[float], Awaitable[None]]


class BeginOutcome(StrEnum):
    """What the owner made of one accepted source message. Only a local message goes on to ``arrange``."""

    # The split yielded nothing: the message runs on this worker.
    LOCAL = 'local'
    # The message is a job; its offset stays pending until the job ends.
    JOB = 'job'
    # The results log says the job is closed: the owner completed the offset.
    DONE = 'done'
    # The split failed or its attempts are spent: the owner completed the offset, or stalled it.
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

    def start_job_end(self, message: SourceMessage, end: Callable[[], Awaitable[bool]]) -> asyncio.Task:
        """Run ``end`` as a task of the processor; its answer completes or stalls the offset of ``message``."""
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
    # What this owner does for the job now; at most one of the three.
    run: SplitRun | None = None
    settling: bool = False
    end_task: asyncio.Task | None = None
    # What the tracker told once and the owner may act on later: a timeout and a stalled attempt, by attempt id.
    timeout: tuple[str, TimeoutKind] | None = None
    stalled_attempt: str | None = None

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
        # A timeout of such a job, kept until its message arrives: the attempt id and the kind.
        self._early_timeouts: dict[int, tuple[str, TimeoutKind]] = {}
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

        A job that the restored table knows is continued: with ``split-done``
        it only waits for its results, without it a new attempt starts, unless
        the attempts are spent. Every restored job with a lower offset whose
        message did not arrive is closed as orphaned. At a stop this returns
        at once: the offset is held, and a split in progress is cancelled in
        its own task.
        """
        offset = message.offset
        if self._stopping:
            self._host.hold_job_offset(offset)
            return BeginOutcome.JOB
        self._close_orphans_below(offset)
        self._restored_waiting.discard(offset)
        early = self._early_timeouts.pop(offset, None)
        job = self._table.job(offset)
        if job is None:
            held = _HeldJob(message=message, opened_at=self._settings.clock())
            return await self._until_stop(held, self._start_attempt(held, None))
        if job.closed is not None:
            self._host.complete_job_offset(offset)
            return BeginOutcome.DONE
        held = _HeldJob(message=message, opened_at=job.opened_at_ms / MS_PER_SECOND)
        if early is not None and early[0] == job.attempt_id:
            held.timeout = early
        if job.parts is None and held.timeout is None:
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

    async def end_job(self, message: SourceMessage, job: SplitJob, *, timed_out: TimedOutKind) -> bool:
        """End a job this owner holds: the hook, its answer, then ``closed``. True when the offset may complete.

        False means the offset must stall (see :meth:`JobEnder.end`).

        Raises:
            asyncio.CancelledError: a stop ended a wait; the offset stays pending.
            RuntimeError: this owner does not hold the message.
        """
        held = self._jobs.get(message.offset)
        if held is None:
            raise RuntimeError(f'the job of offset {message.offset} is not held by this owner')
        state = self._table.job(message.offset)
        first = EndFacts(job=job, first_failure=state.first_failure if state is not None else None)
        return await self._ender.end(
            message,
            held.state,
            first,
            timed_out,
            rebuild=functools.partial(self._facts, held, timed_out),
            closing_ref=functools.partial(self._closing_ref, message.offset),
        )

    async def end_local(self, message: SourceMessage, result: PartResult) -> bool:
        """End a local message as a job of one part; no record is written to any topic.

        Same answer and same error as :meth:`end_job`.
        """
        rebuild = functools.partial(local_facts, message, result)
        return await self._ender.end(message, MessageDLQState(), rebuild(), '', rebuild=rebuild, closing_ref=None)

    # --- what the job tracker tells ---

    def job_complete(self, job: JobState) -> None:
        """Every part of the current attempt of ``job`` has a result."""
        held = self._jobs.get(job.offset)
        if held is not None:
            self._reconcile(held)

    def job_timed_out(self, job: JobState, kind: TimeoutKind) -> None:
        """``job`` passed a timeout: cancel its split when this owner runs it, and end the job."""
        held = self._jobs.get(job.offset)
        if held is None:
            if job.offset in self._restored_waiting:
                self._early_timeouts[job.offset] = (job.attempt_id, kind)
            return
        held.timeout = (job.attempt_id, kind)
        if held.run is not None and held.run.ref.attempt_id == job.attempt_id:
            # The job ends when the run has stopped.
            held.run.interrupt(kind)
            return
        self._reconcile(held)

    def attempt_opened(self, job: JobState) -> None:
        """An ``open`` record made a new attempt the current one of ``job``.

        When it replaces the attempt this owner is splitting, the split
        stops: the log has one current attempt, and every owner follows it.
        An ``open`` of another instance that was read before this owner's
        own is older in the log, so the own attempt replaces it.
        """
        held = self._jobs.get(job.offset)
        if held is None or held.run is None:
            return
        run = held.run
        if job.attempt_id == run.ref.attempt_id:
            run.own_open_seen = True
        elif run.own_open_seen:
            run.interrupt('superseded')

    def attempt_cancelled(self, job: JobState) -> None:
        """The current attempt of ``job`` was cancelled; when another instance did it, the split starts again here."""
        held = self._jobs.get(job.offset)
        if held is not None:
            self._reconcile(held)

    def attempt_stalled(self, job: JobState) -> None:
        """Another instance's split of ``job`` shows no end after the grace time: that owner is taken for dead."""
        held = self._jobs.get(job.offset)
        if held is None:
            return
        held.stalled_attempt = job.attempt_id
        self._reconcile(held)

    def job_closed(self, job: JobState) -> None:
        """``job`` got its ``closed`` record, from this owner or from another: closed means done."""
        self._restored_waiting.discard(job.offset)
        self._early_timeouts.pop(job.offset, None)
        held = self._jobs.get(job.offset)
        if held is None:
            return
        if held.run is not None:
            # The offset completes when the run has stopped.
            held.run.interrupt('closed')
            return
        self._reconcile(held)

    # --- the rules of a holder ---

    def _reconcile(self, held: _HeldJob) -> None:
        """Act on what the results log says now about a held job.

        Called for every event of the job and whenever something this owner
        did for it has ended. While the owner is busy with the job (a split,
        the write of a failure, the end) nothing new starts; an end in
        progress runs to its finish. In this order:

        - the job is closed: complete the offset with no hook;
        - every part has a result: end the job through ``aggregate``;
        - the current attempt timed out: end it through ``on_job_timeout``;
        - the current attempt has ``split-done``: wait for its results;
        - the current attempt is of another instance and was cancelled, or
          shows no end of its split after the grace time: start the next
          attempt here;
        - else wait: another instance is splitting.
        """
        offset = held.message.offset
        if self._stopping or held.busy or self._jobs.get(offset) is not held:
            return
        job = self._table.job(offset)
        if job is None:
            return
        if job.closed is not None:
            self._complete_closed(held, job)
        elif job.is_complete:
            self._start_end(held, job, '')
        elif held.timeout is not None and held.timeout[0] == job.attempt_id:
            self._start_end(held, job, held.timeout[1])
        elif self._is_left_behind(held, job):
            self._start_attempt(held, job)

    @staticmethod
    def _is_left_behind(held: _HeldJob, job: JobState) -> bool:
        """Whether the current attempt is a split of another instance that will not finish.

        Another instance cancelled it, or it shows no end after the grace
        time and its owner is taken for dead.
        """
        is_foreign_split = job.parts is None and job.attempt_id != held.own_attempt_id
        return is_foreign_split and (job.cancelled or held.stalled_attempt == job.attempt_id)

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

    def _closing_ref(self, offset: int) -> JobRef | None:
        """The attempt a ``closed`` record names now: the current one of the log; None when the job is closed."""
        job = self._table.job(offset)
        return None if job is None or job.closed is not None else self._ref_of(job)

    def _close_orphans_below(self, offset: int) -> None:
        """Close every restored job below ``offset`` whose source message did not arrive.

        The source gives its messages in offset order, so such a message was
        passed: it expired, or the position of the group was moved. The
        ``closed`` record is written by a task of the owner, so ``begin``
        does not wait for the results topic.
        """
        for orphan in sorted(candidate for candidate in self._restored_waiting if candidate < offset):
            self._restored_waiting.discard(orphan)
            self._early_timeouts.pop(orphan, None)
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
                return self._spawn(self._fail_spent_attempts(held, job, spent))
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
        is_held = self._jobs.get(offset) is held
        if result.end is SplitEnd.LOCAL and not is_held:
            return BeginOutcome.LOCAL
        if result.end is SplitEnd.LOCAL:
            # Only the partition loop can run a message here, and it has passed this one.
            failure = SplitResult(end=SplitEnd.FAILED, failure='split yielded no part for a job that has an attempt')
            return await self._fail_split(held, run, failure)
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
        self._drop(held)
        logger.error(
            'split_failed',
            category='split',
            partition=self._label,
            job=ref_id(run.ref),
            error=result.failure,
            exc_info=result.cause if result.cause is not None else False,
        )
        error = f'split failed: {cut_error(result.failure)}'
        may_complete = await self._ender.send_to_dlq(
            held.message, held.state, stage='split', error=error, attempts=None
        )
        # A ``closed`` of an attempt whose ``open`` never reached the log would be ignored
        # for a job the log already knows; it then names the current attempt of the log.
        current = self._closing_ref(held.message.offset)
        ref = run.ref if result.open_enqueued or current is None else current
        return await self._close_failed(held, ref, 'split_failed', may_complete=may_complete)

    async def _fail_spent_attempts(self, held: _HeldJob, job: JobState, spent: int) -> BeginOutcome:
        """End a job whose split did not finish in ``split.max_attempts`` attempts: one DLQ record, then ``closed``."""
        self._release_reservation(held)
        self._drop(held)
        ref = self._ref_of(job)
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
        """Write ``closed`` for a job whose split failed, then complete its offset, or stall it.

        With no DLQ record under ``stall`` nothing is closed, so a restart
        tries the job again. A stop during the retries of the write leaves
        the offset pending.
        """
        offset = held.message.offset
        if not may_complete:
            await self._host.stall_offset(offset)
            return BeginOutcome.FAILED
        duration = self._settings.clock() - held.opened_at
        written = await self._ender.write_closed(ref, outcome, received=0, parts=None, duration=duration)
        if written == 'stopped':
            self._host.hold_job_offset(offset)
            return BeginOutcome.JOB
        split_jobs.labels(outcome=outcome).inc()
        if written == 'final':
            await self._host.stall_offset(offset)
        else:
            self._host.complete_job_offset(offset)
        return BeginOutcome.FAILED

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
        held.end_task = self._host.start_job_end(held.message, functools.partial(self._end_held, held, timed_out))

    async def _end_held(self, held: _HeldJob, timed_out: TimedOutKind) -> bool:
        """End a held job from what the table says about it now; it is dropped once its offset is settled."""
        try:
            may_complete = await self._end_now(held, timed_out)
        except asyncio.CancelledError:
            # A stop: the job stays held, and the next owner ends it.
            raise
        except Exception:
            self._drop(held)
            raise
        self._drop(held)
        return may_complete

    async def _end_now(self, held: _HeldJob, timed_out: TimedOutKind) -> bool:
        offset = held.message.offset
        state = self._table.job(offset)
        if state is None:
            raise RuntimeError(f'the results log has no job for offset {offset} any more')
        if state.closed is not None:
            # Another owner closed the job meanwhile: closed means done.
            return True
        return await self.end_job(held.message, self._facts(held, timed_out).job, timed_out=timed_out)

    def _facts(self, held: _HeldJob, timed_out: TimedOutKind) -> EndFacts:
        """The view of a held job for its hook, from the table as it is now.

        Raises:
            RuntimeError: the table no longer has the job.
        """
        state = self._table.job(held.message.offset)
        if state is None:
            raise RuntimeError(f'the results log has no job for offset {held.message.offset} any more')
        # The tracker may not have read the ``split-done`` this owner wrote for the current attempt yet.
        parts_written = held.own_parts if held.own_attempt_id == state.attempt_id else None
        return self._ender.job_facts(held.message, state, timed_out, parts_written=parts_written)

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
