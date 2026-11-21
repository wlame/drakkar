"""The owner side of a split: one source message becomes a job, and the job ends on the worker that owns it.

A ``SplitOwner`` belongs to the partition processor of one source partition.
For each accepted source message it pulls the first item of ``split``. Nothing
yielded: the message is local and runs on this worker. Something yielded: the
message is a job. The owner writes ``open``, publishes the parts, writes
``split-done``, and the partition loop goes on while the offset of the job
stays pending. The job tracker tells the owner when the job is complete or
timed out; the owner then calls ``aggregate`` or ``on_job_timeout``, delivers
the aggregate sinks or writes one DLQ record, writes ``closed``, and only then
lets the offset complete. After a restart the new owner finds the job in the
restored table and continues it.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, Protocol

import structlog
from pydantic import BaseModel
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.config.units import format_duration
from drakkar.groups import STREAM_CLOSE_TIMEOUT, STREAM_END, close_stream, next_stream_item, retrieve_task_outcome
from drakkar.hook_scope import PipelineHookScope, pipeline_hook
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.metrics import (
    dlq_dropped_payloads,
    handler_duration,
    split_job_duration,
    split_jobs,
    split_parts_published,
)
from drakkar.models import CollectResult, SinkDeliveryFailedError, SourceMessage
from drakkar.providers.resilience import RateLimitedLog
from drakkar.request_ids import RequestIdResolver, reset_request_id
from drakkar.sinks.dlq_record import DLQStage, cut_error
from drakkar.sinks.manager import CollectCallback, MessageContext, MessageDLQState, SinkManager
from drakkar.split.models import JobAction, PartResult, SplitJob, SplitResultsUnavailable, TimedOutKind
from drakkar.split.partmode import (
    RETRY_FIRST_WAIT_SECONDS,
    RETRY_MAX_WAIT_SECONDS,
    STOPPED_MESSAGE,
    publish_or_stop,
)
from drakkar.split.producer import SplitProduceError, SplitProducer
from drakkar.split.reader import read_results
from drakkar.split.records import (
    ClosedOutcome,
    JobRef,
    WireRecord,
    closed_body,
    log_record,
    new_attempt_id,
    open_body,
    part_record,
    part_value,
)
from drakkar.split.table import BITS_PER_BYTE, MAX_PART_INDEX, MS_PER_SECOND, JobState, JobTable, PartFailure
from drakkar.split.tracker import TimeoutKind

logger = structlog.get_logger()

# Part records sent and not yet acknowledged during a split. At the limit the
# split waits for all of them, so the client queue and the memory of the owner
# stay bounded whatever the number of parts.
MAX_UNACKED_PARTS = 1000
# The time limit of the ``split-cancelled`` write of a cancelled split.
CANCEL_WRITE_SECONDS = 2.0
# After a failed read of the results inside a hook, the hook is called again
# after this wait, at most this many times; one more failure is a raise of the hook.
RESULTS_RETRY_SECONDS = 30.0
MAX_RESULTS_RETRIES = 20

SleepFn = Callable[[float], Awaitable[None]]
# Why a split in progress stops: a stop or a revoke, another owner's attempt, or a timeout of the job.
InterruptReason = Literal['stop', 'superseded', 'job', 'idle']
ClosedWrite = Literal['written', 'final', 'stopped']


class BeginOutcome(StrEnum):
    """What the owner made of one accepted source message."""

    # The split yielded nothing: the message runs on this worker.
    LOCAL = 'local'
    # The message is a job whose offset stays pending until the job ends.
    JOB = 'job'
    # The results log says the job is closed: the offset completes.
    DONE = 'done'
    # The split failed or its attempts are spent: the offset completes, unless the owner stalled it.
    FAILED = 'failed'


class OwnerHost(Protocol):
    """What the owner needs of the partition processor it belongs to."""

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

    def start_job_end(self, message: SourceMessage, end: Callable[[], Awaitable[bool]]) -> asyncio.Task:
        """Run ``end`` as a task of the processor; its answer completes or stalls the offset of ``message``."""
        ...

    async def stall_offset(self, offset: int) -> None:
        """Leave ``offset`` pending for the life of the process."""
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


@dataclass
class _OwnedJob:
    """A job whose source message this owner holds. A local message has no ``ref``."""

    message: SourceMessage
    ref: JobRef | None
    opened_at: float
    # The DLQ state of the original message, shared by the aggregate delivery and the DLQ record of the job.
    state: MessageDLQState = field(default_factory=MessageDLQState)
    splitting: bool = False
    end_task: asyncio.Task | None = None


@dataclass
class _Split:
    """One run of the ``split`` generator."""

    owned: _OwnedJob
    ref: JobRef
    context: contextvars.Context
    interrupt: asyncio.Future[InterruptReason]
    started_at: float
    # The iterator of the generator; None until ``split`` was called.
    stream: AsyncIterator[Any] | None = None
    pull: asyncio.Task | None = None
    left_scope: PipelineHookScope | None = None
    in_flight: dict[asyncio.Future[None], WireRecord] = field(default_factory=dict)
    open_enqueued: bool = False
    parts: int = 0


@dataclass(frozen=True)
class _EndFacts:
    """What the end of a job reads: the view the hook receives and the first failed part."""

    job: SplitJob
    first_failure: PartFailure | None


class _SplitFailed(Exception):
    """The split cannot go on: the generator raised, an item was refused, or a record can never be produced."""

    def __init__(self, text: str, cause: BaseException | None = None) -> None:
        super().__init__(text)
        self.text = text
        self.cause = cause


class _SplitInterrupted(Exception):
    """A split in progress must stop."""

    def __init__(self, reason: InterruptReason) -> None:
        super().__init__(reason)
        self.reason: InterruptReason = reason


def _error_text(error: BaseException) -> str:
    """The text of an error (its type when it has none), cut as ``dk-error`` is."""
    return cut_error(str(error) or type(error).__name__)


def _job_id(partition: int, offset: int, attempt: int) -> str:
    """A job as log lines show it: ``<partition>:<offset>#<attempt>``."""
    return f'{partition}:{offset}#{attempt}'


def _ref_id(ref: JobRef) -> str:
    return _job_id(ref.partition, ref.offset, ref.attempt)


def _whole_seconds(seconds: float) -> str:
    """The canonical duration text of ``seconds``, cut to whole seconds."""
    return format_duration(int(max(seconds, 0.0)) * MS_PER_SECOND)


def _missing_parts(received_bits: bytes, parts: int | None) -> list[int]:
    """The part numbers below ``parts`` whose bit is not set; empty while ``parts`` is unknown."""
    if parts is None:
        return []
    return [
        index
        for index in range(parts)
        if index // BITS_PER_BYTE >= len(received_bits)
        or not received_bits[index // BITS_PER_BYTE] >> index % BITS_PER_BYTE & 1
    ]


def _local_failure(result: PartResult) -> PartFailure | None:
    """The first failure of a local message: its one part, when that part failed."""
    if result.outcome != 'failed':
        return None
    return PartFailure(index=result.index, dlq=result.dlq, error=result.error)


def _local_view(message: SourceMessage, result: PartResult) -> _EndFacts:
    """The job of one part that a local message is, with its one result held in memory."""

    async def results() -> AsyncIterator[PartResult]:
        yield result

    job = SplitJob(
        source_message=message,
        job_id=_job_id(message.partition, message.offset, 1),
        attempt=1,
        local=True,
        total_parts=1,
        received_parts=1,
        failed_parts=1 if result.outcome == 'failed' else 0,
        dlq_parts=1 if result.dlq is not None else 0,
        tasks_succeeded=result.tasks_succeeded,
        tasks_failed=result.tasks_failed,
        tasks_replaced=result.tasks_replaced,
        deliveries={sink: counts.model_copy() for sink, counts in result.deliveries.items()},
        timed_out='',
        opened_at=result.finished_at - result.duration_seconds,
        duration_seconds=result.duration_seconds,
        missing=list,
        results_source=results,
    )
    return _EndFacts(job=job, first_failure=_local_failure(result))


class SplitOwner:
    """Splits the messages of one source partition and ends their jobs.

    Every method runs on the event loop. ``begin`` is called by the partition
    loop, one message at a time. The three ``job_*`` methods are called by the
    job tracker and return at once. The end of a job runs as a task of the
    processor, one for each job.
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
        self._partition = partition
        self._label = partition if partition_label is None else partition_label
        self._table = table
        self._host = host
        self._handler = handler
        self._sink_manager = sink_manager
        self._on_collect = on_collect
        self._on_dlq_failure = on_dlq_failure
        self._part_data_model: type[BaseModel] | None = getattr(type(handler), 'part_data_model', None)
        # The open jobs whose source message this owner holds, by source offset.
        self._jobs: dict[int, _OwnedJob] = {}
        # The open jobs of the restored table whose source message has not arrived yet. One whose
        # message never arrives is closed as orphaned when a later message is accepted.
        self._restored_waiting: set[int] = {job.offset for job in table.open_jobs()}
        # A timeout of such a job, kept until its message arrives: the attempt id and the kind.
        self._early_timeouts: dict[int, tuple[str, TimeoutKind]] = {}
        self._current: _Split | None = None
        self._stopping = False
        self._stopped = asyncio.Event()
        self._log_limit = RateLimitedLog(time.monotonic)

    # --- what the processor calls ---

    def signal_stop(self) -> None:
        """A stop or a revoke: cancel the split in progress and end every wait. No new end of a job starts."""
        self._stopping = True
        self._stopped.set()
        if self._current is not None:
            self._interrupt(self._current, 'stop')

    async def begin(self, message: SourceMessage) -> BeginOutcome:
        """Decide what one accepted source message is: a local message, a job, a closed job, or a failed one.

        A job that the restored table knows is continued: with ``split-done``
        it only waits for its results, without it a new attempt starts, unless
        the attempts are spent. Before that, every restored job with a lower
        offset whose message did not arrive is closed as orphaned.
        """
        offset = message.offset
        await self._close_orphans_below(offset)
        self._restored_waiting.discard(offset)
        early = self._early_timeouts.pop(offset, None)
        job = self._table.job(offset)
        if job is None:
            return await self._split(message, None)
        if job.closed is not None:
            return BeginOutcome.DONE
        if early is not None and early[0] == job.attempt_id:
            return self._resume(message, job, timed_out=early[1])
        if job.parts is not None:
            return self._resume(message, job, timed_out='')
        # The table counts an attempt as crashed only when a later ``open`` arrives, so the
        # current attempt without ``split-done`` and without ``split-cancelled`` is one more.
        spent = job.crashed_attempts + (0 if job.cancelled else 1)
        if spent >= self._settings.max_attempts:
            return await self._fail_spent_attempts(message, job, spent)
        return await self._split(message, job)

    async def end_job(self, message: SourceMessage, job: SplitJob, *, timed_out: TimedOutKind) -> bool:
        """End a job this owner holds: the hook, its answer, then ``closed``. True when the offset may complete.

        False means the offset must stall: a delivery or a DLQ record could
        not be confirmed under ``dlq.on_send_failure: stall``, or ``closed``
        can never be written. No ``closed`` is written then, so a restart
        ends the job again.

        Raises:
            asyncio.CancelledError: a stop ended a wait; the offset stays pending.
        """
        owned = self._jobs.get(message.offset)
        if owned is None or owned.ref is None:
            raise RuntimeError(f'the job of offset {message.offset} is not held by this owner')
        state = self._table.job(message.offset)
        first = _EndFacts(job=job, first_failure=state.first_failure if state is not None else None)
        return await self._end(owned, first, timed_out, rebuild=functools.partial(self._job_view, owned, timed_out))

    async def end_local(self, message: SourceMessage, result: PartResult) -> bool:
        """End a local message as a job of one part; no record is written to any topic.

        Same answer and same error as :meth:`end_job`.
        """
        owned = _OwnedJob(message=message, ref=None, opened_at=result.finished_at - result.duration_seconds)
        rebuild = functools.partial(_local_view, message, result)
        return await self._end(owned, rebuild(), '', rebuild=rebuild)

    # --- what the job tracker tells ---

    def job_complete(self, job: JobState) -> None:
        """Every part of ``job`` has a result: end it, when this owner holds its message and its split has ended."""
        owned = self._jobs.get(job.offset)
        if owned is None or owned.splitting or not self._is_current(owned, job):
            # A job whose message has not arrived ends when ``begin`` sees the message.
            return
        self._start_end(owned, job, '')

    def job_timed_out(self, job: JobState, kind: TimeoutKind) -> None:
        """``job`` passed a timeout: cancel its split when one runs, and end it through ``on_job_timeout``."""
        owned = self._jobs.get(job.offset)
        if owned is None:
            if job.offset in self._restored_waiting:
                self._early_timeouts[job.offset] = (job.attempt_id, kind)
            return
        if not self._is_current(owned, job):
            return
        if owned.splitting:
            if self._current is not None and self._current.owned is owned:
                self._interrupt(self._current, kind)
            return
        self._start_end(owned, job, kind)

    def job_reopened_elsewhere(self, job: JobState) -> None:
        """Another owner started a new attempt of ``job``: stand down for it and write nothing more."""
        owned = self._jobs.get(job.offset)
        if owned is None:
            return
        if owned.splitting and self._current is not None and self._current.owned is owned:
            # The split loop forgets the job once its generator is closed.
            self._interrupt(self._current, 'superseded')
            return
        del self._jobs[job.offset]
        if owned.end_task is not None and not owned.end_task.done():
            owned.end_task.cancel()
        self._note_superseded(owned)

    # --- restored jobs ---

    @staticmethod
    def _is_current(owned: _OwnedJob, job: JobState) -> bool:
        """Whether the attempt this owner holds is the current attempt of ``job`` in the table."""
        return owned.ref is not None and owned.ref.attempt_id == job.attempt_id

    def _ref_of(self, job: JobState) -> JobRef:
        """The current attempt of a job of the table."""
        return JobRef(
            group=self._settings.source_group,
            partition=self._partition,
            offset=job.offset,
            attempt=job.attempt,
            attempt_id=job.attempt_id,
        )

    def _resume(self, message: SourceMessage, job: JobState, *, timed_out: TimedOutKind) -> BeginOutcome:
        """Take over a restored job without a new split; end it at once when it is complete or timed out."""
        owned = _OwnedJob(message=message, ref=self._ref_of(job), opened_at=job.opened_at_ms / MS_PER_SECOND)
        self._jobs[message.offset] = owned
        logger.info(
            'split_job_resumed',
            category='split',
            partition=self._label,
            job=_job_id(self._partition, job.offset, job.attempt),
            attempt=job.attempt,
            parts=job.parts,
            received=job.received_count,
        )
        self._host.hold_job_offset(message.offset)
        if timed_out or job.is_complete:
            self._start_end(owned, job, timed_out)
        return BeginOutcome.JOB

    async def _close_orphans_below(self, offset: int) -> None:
        """Close every restored job below ``offset`` whose source message did not arrive.

        The source gives its messages in offset order, so such a message was
        passed: it expired, or the position of the group was moved.
        """
        for orphan in sorted(candidate for candidate in self._restored_waiting if candidate < offset):
            self._restored_waiting.discard(orphan)
            self._early_timeouts.pop(orphan, None)
            job = self._table.job(orphan)
            if job is None or job.closed is not None:
                continue
            # The write below can wait for the results topic; the window must not hold its room meanwhile.
            self._host.release_window_reservation()
            split_jobs.labels(outcome='orphaned').inc()
            logger.warning(
                'split_job_orphaned',
                category='split',
                partition=self._label,
                job=_job_id(self._partition, job.offset, job.attempt),
                received=job.received_count,
                parts=job.parts,
                hint='the source no longer gives the message of this job; it is closed with nothing delivered',
            )
            await self._write_closed(
                self._ref_of(job),
                'orphaned',
                received=job.received_count,
                parts=job.parts,
                duration=self._settings.clock() - job.opened_at_ms / MS_PER_SECOND,
            )

    async def _fail_spent_attempts(self, message: SourceMessage, job: JobState, spent: int) -> BeginOutcome:
        """End a job whose split did not finish in ``split.max_attempts`` attempts: one DLQ record, then ``closed``."""
        owned = _OwnedJob(message=message, ref=self._ref_of(job), opened_at=job.opened_at_ms / MS_PER_SECOND)
        error = f'split did not finish in {spent} attempts'
        self._host.release_window_reservation()
        logger.error(
            'split_failed',
            category='split',
            partition=self._label,
            job=_job_id(self._partition, job.offset, job.attempt),
            error=error,
            attempts=spent,
        )
        may_complete = await self._send_to_dlq(owned, stage='split', error=error, attempts=spent)
        return await self._close_failed(owned, self._ref_of(job), 'attempts_spent', may_complete=may_complete)

    async def _close_failed(
        self, owned: _OwnedJob, ref: JobRef, outcome: ClosedOutcome, *, may_complete: bool
    ) -> BeginOutcome:
        """Write ``closed`` for a job that failed in ``begin`` and say what its offset does.

        With no DLQ record under ``stall`` nothing is closed, so a restart
        tries the job again.
        """
        offset = owned.message.offset
        if not may_complete:
            await self._host.stall_offset(offset)
            return BeginOutcome.FAILED
        written = await self._write_closed(
            ref, outcome, received=0, parts=None, duration=self._settings.clock() - owned.opened_at
        )
        if written == 'stopped':
            self._host.hold_job_offset(offset)
            return BeginOutcome.JOB
        split_jobs.labels(outcome=outcome).inc()
        if written == 'final':
            await self._host.stall_offset(offset)
        return BeginOutcome.FAILED

    # --- the split ---

    async def _split(self, message: SourceMessage, restored: JobState | None) -> BeginOutcome:
        """Pull ``split(message)``: nothing yielded is a local message, else publish the job.

        ``restored`` is the job of the table when an earlier attempt of it did
        not finish; the new attempt takes the next number.
        """
        now = self._settings.clock()
        attempt = 1 if restored is None else restored.attempt + 1
        ref = JobRef(
            group=self._settings.source_group,
            partition=self._partition,
            offset=message.offset,
            attempt=attempt,
            attempt_id=new_attempt_id(),
        )
        opened_at = now if restored is None else restored.opened_at_ms / MS_PER_SECOND
        owned = _OwnedJob(message=message, ref=ref, opened_at=opened_at)
        split = _Split(
            owned=owned,
            ref=ref,
            context=contextvars.copy_context(),
            interrupt=asyncio.get_running_loop().create_future(),
            started_at=time.monotonic(),
        )
        self._current = split
        try:
            try:
                stream = self._open_stream(split)
                split.stream = stream
                first = await self._pull(split, stream)
                if first is STREAM_END:
                    return BeginOutcome.LOCAL
                # A job has no task on this worker: the window gives back what it reserved.
                self._host.release_window_reservation()
                self._jobs[message.offset] = owned
                owned.splitting = True
                await self._publish_job(split, stream, first, restored)
            except _SplitFailed as failure:
                return await self._fail_split(split, restored, failure)
            except _SplitInterrupted as interrupted:
                return await self._cancel_split(split, interrupted.reason)
            except asyncio.CancelledError:
                # The partition loop itself is cancelled: no time is left to close the generator.
                if split.pull is not None:
                    split.pull.cancel()
                self._drop_in_flight(split)
                raise
        finally:
            self._current = None
            owned.splitting = False
            if split.left_scope is not None:
                # Only marked: the context of the split, where its token was set, is dropped.
                split.left_scope.ended = True
        return self._finish_split(split)

    def _open_stream(self, split: _Split) -> AsyncIterator[Any]:
        """Call ``split(message)`` and return its async iterator.

        Raises:
            _SplitFailed: the hook raised, or returned something that is no async iterator.
        """
        # Calling an async generator function runs none of its code: that happens in the pulls.
        try:
            result = self._handler.split(split.owned.message)
        except Exception as error:
            raise _SplitFailed(_error_text(error), error) from error
        if not hasattr(result, '__aiter__'):
            close = getattr(result, 'close', None)
            if close is not None:
                # A coroutine of an ``async def`` without a yield: closed, so it does not warn.
                close()
            raise _SplitFailed(f'split must be an async generator, got {type(result).__name__}')
        return result.__aiter__()

    def _bind_pull(self, message: SourceMessage) -> tuple[Any, Any, PipelineHookScope]:
        """Bind the log fields, request id, hook context and pipeline scope of one pull; run in the split's context."""
        bind_contextvars(partition=self._label, hook='split', offset=message.offset)
        request_id_tokens = RequestIdResolver.bind([message.request_id])
        hook_token = bind_hook_context(
            hook='split', partition=self._label, offset=message.offset, offsets=(message.offset,)
        )
        scope = pipeline_hook().__enter__()
        return request_id_tokens, hook_token, scope

    @staticmethod
    def _reset_pull(tokens: tuple[Any, Any, PipelineHookScope]) -> None:
        """Put back what ``_bind_pull`` bound; run in the split's context."""
        request_id_tokens, hook_token, scope = tokens
        scope.end()
        unbind_contextvars('hook', 'offset')
        reset_request_id(request_id_tokens)
        clear_hook_context(hook_token)

    async def _pull(self, split: _Split, stream: AsyncIterator[Any]) -> Any:
        """The next item of the generator, or ``STREAM_END``; each pull is timed and runs in the pipeline hook scope.

        The pull runs as a task, so a stop can cancel it and the generator
        can be closed: ``aclose()`` cannot run while a pull is running.

        Raises:
            _SplitFailed: the generator raised.
            _SplitInterrupted: the split must stop; the pull is left to the close.
        """
        context = split.context
        tokens = context.run(self._bind_pull, split.owned.message)
        started = time.monotonic()
        pull = asyncio.create_task(next_stream_item(stream), context=context)
        # A pull left behind by a stop must not log an unread exception.
        pull.add_done_callback(retrieve_task_outcome)
        split.pull = pull
        try:
            await self._wait(pull, split.interrupt)
            split.pull = None
            try:
                return pull.result()
            except Exception as error:
                raise _SplitFailed(_error_text(error), error) from error
        finally:
            if pull.done():
                context.run(self._reset_pull, tokens)
                handler_duration.labels(hook='split').observe(time.monotonic() - started)
            else:
                # A pull still running holds the context; its scope ends when the split is given up.
                split.left_scope = tokens[2]

    @staticmethod
    async def _wait(target: asyncio.Future[Any], interrupt: asyncio.Future[InterruptReason]) -> None:
        """Wait for ``target``.

        Raises:
            _SplitInterrupted: the split must stop before ``target`` ended; ``target`` is left as it is.
        """
        if not target.done():
            await asyncio.wait({target, interrupt}, return_when=asyncio.FIRST_COMPLETED)
        if not target.done():
            raise _SplitInterrupted(interrupt.result())

    @staticmethod
    def _interrupt(split: _Split, reason: InterruptReason) -> None:
        """Make the split in progress stop at its next wait; the first reason is kept."""
        if not split.interrupt.done():
            split.interrupt.set_result(reason)

    async def _publish_job(
        self, split: _Split, stream: AsyncIterator[Any], first: Any, restored: JobState | None
    ) -> None:
        """Write ``open``, publish every part the generator yields, then write ``split-done``.

        Raises:
            _SplitFailed: an item or a record was refused for good.
            _SplitInterrupted: the split must stop.
        """
        settings = self._settings
        ref, message = split.ref, split.owned.message
        job = _ref_id(ref)
        if restored is not None:
            logger.warning(
                'split_attempt_started',
                category='split',
                partition=self._label,
                job=job,
                attempt=ref.attempt,
                crashed_attempts=restored.crashed_attempts + (0 if restored.cancelled else 1),
            )
        body = open_body(worker=settings.worker, instance=settings.instance, request_id=message.request_id)
        # From here on the record may reach the log, also when its delivery report never comes.
        split.open_enqueued = True
        await self._deliver_record(split, settings.results_topic, log_record(ref, 'open', body))
        logger.info(
            'split_job_opened',
            category='split',
            partition=self._label,
            job=job,
            attempt=ref.attempt,
            request_id=message.request_id,
        )
        item = first
        while item is not STREAM_END:
            await self._send_part(split, item)
            item = await self._pull(split, stream)
        await self._settle_parts(split)
        await self._deliver_record(split, settings.results_topic, log_record(ref, 'split-done', {'parts': split.parts}))
        logger.info(
            'split_job_done',
            category='split',
            partition=self._label,
            job=job,
            parts=split.parts,
            seconds=round(time.monotonic() - split.started_at, 3),
        )

    async def _send_part(self, split: _Split, item: object) -> None:
        """Put the record of the next part in the producer; at the limit of unacknowledged parts, wait for them."""
        settings = self._settings
        message = split.owned.message
        index = split.parts
        if index > MAX_PART_INDEX:
            raise _SplitFailed(f'more than {MAX_PART_INDEX + 1} parts')
        try:
            value = part_value(item)
        except TypeError as error:
            raise _SplitFailed(str(error)) from None
        record = part_record(
            split.ref,
            source_topic=message.topic,
            index=index,
            request_id=message.request_id,
            key=message.key,
            value=value,
            source_headers=message.headers,
            partition_count=settings.parts_partition_count,
        )
        split.parts += 1
        try:
            future = await self._enqueue(split, settings.parts_topic, record)
        except SplitProduceError as error:
            self._check_not_final(error)
            await self._deliver_record(split, settings.parts_topic, record, failed=error)
            split_parts_published.inc()
            return
        split.in_flight[future] = record
        if len(split.in_flight) >= MAX_UNACKED_PARTS:
            await self._settle_parts(split)

    async def _enqueue(self, split: _Split, topic: str, record: WireRecord) -> asyncio.Future[None]:
        """Give ``record`` to the producer and return its delivery future; a wait for room ends when the split stops."""
        enqueue = asyncio.ensure_future(self._settings.producer.enqueue(topic, record))
        try:
            await self._wait(enqueue, split.interrupt)
        finally:
            if not enqueue.done():
                enqueue.cancel()
        return enqueue.result()

    @staticmethod
    def _check_not_final(error: SplitProduceError) -> None:
        """Raise ``_SplitFailed`` for a produce error that no retry can fix."""
        if error.final:
            raise _SplitFailed(str(error)) from error

    async def _settle_parts(self, split: _Split) -> None:
        """Wait for every part record in flight; count the acknowledged ones and deliver again the ones that failed."""
        waiting = [future for future in split.in_flight if not future.done()]
        if waiting:
            # Cancelling the group cancels the futures it waits for.
            every = asyncio.gather(*waiting, return_exceptions=True)
            try:
                await self._wait(every, split.interrupt)
            finally:
                if not every.done():
                    every.cancel()
        settled, split.in_flight = split.in_flight, {}
        failed: list[tuple[WireRecord, SplitProduceError]] = []
        for future, record in settled.items():
            error = future.exception()
            if error is None:
                continue
            if not isinstance(error, SplitProduceError):
                raise _SplitFailed(_error_text(error), error)
            self._check_not_final(error)
            failed.append((record, error))
        split_parts_published.inc(len(settled) - len(failed))
        for record, error in failed:
            await self._deliver_record(split, self._settings.parts_topic, record, failed=error)
            split_parts_published.inc()

    async def _deliver_record(
        self, split: _Split, topic: str, record: WireRecord, *, failed: SplitProduceError | None = None
    ) -> None:
        """Produce one record and wait for its acknowledgement; a failure a retry can fix is tried again.

        The waits are 1, 2, 4 ... 30 seconds, for as long as it takes.
        ``failed`` is the error of a first attempt the caller already made.

        Raises:
            _SplitFailed: the record can never be delivered.
            _SplitInterrupted: the split must stop.
        """
        wait = RETRY_FIRST_WAIT_SECONDS
        error = failed
        while True:
            if error is None:
                try:
                    future = await self._enqueue(split, topic, record)
                    await self._wait(future, split.interrupt)
                    future.result()
                    return
                except SplitProduceError as produce_error:
                    self._check_not_final(produce_error)
                    error = produce_error
            self._note_publish_failure(split, error)
            sleeper = asyncio.ensure_future(self._settings.sleep(wait))
            try:
                await self._wait(sleeper, split.interrupt)
            finally:
                sleeper.cancel()
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)
            error = None

    def _note_publish_failure(self, split: _Split, error: SplitProduceError) -> None:
        suppressed = self._log_limit.admit('publish')
        if suppressed is not None:
            logger.warning(
                'split_publish_failed',
                category='split',
                partition=self._label,
                job=_ref_id(split.ref),
                error=str(error),
                suppressed=suppressed,
            )

    @staticmethod
    def _drop_in_flight(split: _Split) -> None:
        """Stop waiting for the part records in flight; the producer may still deliver them."""
        for future in split.in_flight:
            if future.done():
                if not future.cancelled():
                    # Read, so a failed delivery nobody waits for is not logged as an unread exception.
                    future.exception()
            else:
                future.cancel()
        split.in_flight = {}

    async def _close_stream(self, split: _Split) -> None:
        """Cancel the pull in progress, then close the generator, within the stream-close time; never raises."""
        if split.stream is None:
            return
        pull, split.pull = split.pull, None
        failure = await close_stream(
            split.stream, pull=pull, context=split.context, timeout=self._settings.close_timeout
        )
        if failure is not None:
            logger.error(
                'split_close_failed',
                category='split',
                partition=self._label,
                job=_ref_id(split.ref),
                error='timed out' if failure.timed_out else str(failure.error),
            )

    def _finish_split(self, split: _Split) -> BeginOutcome:
        """The parts are published: the job waits for its results, or ends at once when they are all there.

        A timeout or another owner's attempt that came after the last wait
        of the split is acted on here: the tracker tells each of them once.
        """
        owned = split.owned
        offset = owned.message.offset
        self._host.hold_job_offset(offset)
        reason = split.interrupt.result() if split.interrupt.done() else None
        if reason == 'superseded':
            self._jobs.pop(offset, None)
            self._note_superseded(owned)
            return BeginOutcome.JOB
        job = self._table.job(offset)
        if job is None or job.closed is not None or not self._is_current(owned, job):
            # The tracker has not read the records of this attempt yet; its events follow.
            return BeginOutcome.JOB
        if reason in ('job', 'idle'):
            self._start_end(owned, job, reason)
        elif job.is_complete:
            self._start_end(owned, job, '')
        return BeginOutcome.JOB

    async def _fail_split(self, split: _Split, restored: JobState | None, failure: _SplitFailed) -> BeginOutcome:
        """End a job whose split failed: close the generator, one DLQ record with stage ``split``, then ``closed``."""
        owned = split.owned
        # The writes below can wait for a topic; the window must not hold its room meanwhile.
        self._host.release_window_reservation()
        await self._close_stream(split)
        self._drop_in_flight(split)
        self._jobs.pop(owned.message.offset, None)
        logger.error(
            'split_failed',
            category='split',
            partition=self._label,
            job=_ref_id(split.ref),
            error=failure.text,
            exc_info=failure.cause if failure.cause is not None else False,
        )
        error = f'split failed: {cut_error(failure.text)}'
        may_complete = await self._send_to_dlq(owned, stage='split', error=error, attempts=None)
        # A ``closed`` of an attempt whose ``open`` never reached the log would be ignored
        # for a job the log already knows; it then names the current attempt of the log.
        ref = split.ref if split.open_enqueued or restored is None else self._ref_of(restored)
        return await self._close_failed(owned, ref, 'split_failed', may_complete=may_complete)

    async def _cancel_split(self, split: _Split, reason: InterruptReason) -> BeginOutcome:
        """Stop a split in progress; the offset stays pending.

        A stop, a revoke or a timeout writes ``split-cancelled``, so the
        attempt does not count against ``split.max_attempts``. After a timeout
        the job then ends through ``on_job_timeout``. For another owner's
        attempt nothing more is written and the job is forgotten.
        """
        owned = split.owned
        offset = owned.message.offset
        await self._close_stream(split)
        self._drop_in_flight(split)
        if reason != 'superseded' and split.open_enqueued:
            await self._write_cancelled(split.ref)
        job = self._table.job(offset)
        if reason in ('job', 'idle') and job is not None and job.closed is None:
            owned.splitting = False
            self._host.hold_job_offset(offset)
            self._start_end(owned, job, reason)
            return BeginOutcome.JOB
        self._jobs.pop(offset, None)
        if reason == 'superseded':
            self._note_superseded(owned)
        self._host.hold_job_offset(offset)
        return BeginOutcome.JOB

    async def _write_cancelled(self, ref: JobRef) -> None:
        """Write ``split-cancelled`` within its time limit; a failure is logged and nothing else follows."""
        record = log_record(ref, 'split-cancelled', {})
        try:
            await asyncio.wait_for(
                self._settings.producer.publish(self._settings.results_topic, record),
                timeout=self._settings.cancel_write_seconds,
            )
        except (TimeoutError, SplitProduceError, RuntimeError) as error:
            logger.warning(
                'split_cancel_write_failed',
                category='split',
                partition=self._label,
                job=_ref_id(ref),
                error=str(error) or type(error).__name__,
                hint='the attempt counts against split.max_attempts',
            )

    def _note_superseded(self, owned: _OwnedJob) -> None:
        split_jobs.labels(outcome='superseded').inc()
        logger.warning(
            'split_job_superseded',
            category='split',
            partition=self._label,
            job=_ref_id(owned.ref) if owned.ref is not None else None,
            hint='another owner started a new attempt of this job; this worker writes nothing more for it',
        )

    # --- the end of a job ---

    def _start_end(self, owned: _OwnedJob, job: JobState, timed_out: TimedOutKind) -> None:
        """Start the end of a job as a task of the processor, once for each job; no end starts after a stop."""
        if owned.end_task is not None or self._stopping:
            return
        if timed_out:
            logger.warning(
                'split_job_timed_out',
                category='split',
                partition=self._label,
                job=_job_id(self._partition, job.offset, job.attempt),
                kind=timed_out,
                received=job.received_count,
                parts=job.parts,
            )
        owned.end_task = self._host.start_job_end(owned.message, functools.partial(self._end_held, owned, timed_out))

    async def _end_held(self, owned: _OwnedJob, timed_out: TimedOutKind) -> bool:
        """End a job this owner holds, from what the table says about it now."""
        state = self._table.job(owned.message.offset)
        if state is not None and state.closed is not None:
            # Another owner closed the job meanwhile: closed means done.
            self._jobs.pop(owned.message.offset, None)
            return True
        facts = self._job_view(owned, timed_out)
        return await self.end_job(owned.message, facts.job, timed_out=timed_out)

    def _job_view(self, owned: _OwnedJob, timed_out: TimedOutKind) -> _EndFacts:
        """The view of a job for its hook, built from the table as it is now.

        Raises:
            RuntimeError: the table no longer has the job.
        """
        message = owned.message
        state = self._table.job(message.offset)
        if state is None:
            raise RuntimeError(f'the results log has no job for offset {message.offset} any more')
        settings = self._settings
        opened_at = state.opened_at_ms / MS_PER_SECOND
        job = SplitJob(
            source_message=message,
            job_id=_job_id(self._partition, state.offset, state.attempt),
            attempt=state.attempt,
            local=False,
            total_parts=state.parts,
            received_parts=state.received_count,
            failed_parts=state.failed_parts,
            dlq_parts=state.dlq_parts,
            tasks_succeeded=state.tasks_succeeded,
            tasks_failed=state.tasks_failed,
            tasks_replaced=state.tasks_replaced,
            deliveries={sink: counts.model_copy() for sink, counts in state.deliveries.items()},
            timed_out=timed_out,
            opened_at=opened_at,
            duration_seconds=max(0.0, settings.clock() - opened_at),
            # A copy: the table frees the bit set of a job once it is closed, and the view may outlive that.
            missing=functools.partial(_missing_parts, bytes(state.received_bits), state.parts),
            results_source=functools.partial(
                read_results,
                consumer_factory=settings.results_reader,
                topic=settings.results_topic,
                partition=self._partition,
                job=state,
                group=settings.source_group,
                part_data_model=self._part_data_model,
            ),
        )
        return _EndFacts(job=job, first_failure=state.first_failure)

    async def _end(
        self, owned: _OwnedJob, facts: _EndFacts, timed_out: TimedOutKind, *, rebuild: Callable[[], _EndFacts]
    ) -> bool:
        """Call the hook of the end, act on its answer, then close the job; true when the offset may complete."""
        hook = 'on_job_timeout' if timed_out else 'aggregate'
        facts, answer, raised = await self._ask_hook(hook, owned, facts, rebuild)
        job = facts.job
        failure_text: str | None = None
        if raised is not None:
            failure_text = _error_text(raised)
        elif isinstance(answer, CollectResult):
            failure_text = self._sink_manager.stage_error(answer, 'aggregate')
        elif answer is not None and not isinstance(answer, JobAction):
            failure_text = f'{hook} returned {type(answer).__name__}'

        may_complete = True
        sent_to_dlq = False
        if failure_text is not None:
            logger.error(
                'aggregate_failed',
                category='split',
                partition=self._label,
                job=job.job_id,
                hook=hook,
                error=failure_text,
                exc_info=raised if raised is not None else False,
            )
            error = f'{hook} failed: {failure_text}'
            may_complete = await self._send_to_dlq(owned, stage='aggregate', error=error, attempts=None)
            sent_to_dlq = True
        elif isinstance(answer, CollectResult):
            may_complete = await self._deliver(answer, owned)
            # A failing aggregate sink under ``on_failure: dlq`` put the original message on the DLQ.
            sent_to_dlq = owned.state.dlq_written or owned.state.dlq_lost
        elif answer is JobAction.DLQ:
            may_complete = await self._send_to_dlq(owned, **self._dlq_fields(facts, timed_out))
            sent_to_dlq = True
        if not may_complete:
            # Nothing is closed: after a restart the job ends again.
            self._jobs.pop(owned.message.offset, None)
            return False
        if owned.ref is None:
            split_jobs.labels(outcome='dlq' if sent_to_dlq else 'local').inc()
            return True
        outcome: ClosedOutcome = 'timeout' if timed_out else ('dlq' if sent_to_dlq else 'aggregated')
        return await self._close_job(owned, owned.ref, job, outcome)

    async def _ask_hook(
        self, hook: str, owned: _OwnedJob, facts: _EndFacts, rebuild: Callable[[], _EndFacts]
    ) -> tuple[_EndFacts, Any, Exception | None]:
        """Call ``aggregate`` or ``on_job_timeout``; the facts it saw, its answer, and the error it raised.

        ``SplitResultsUnavailable`` leaving the hook is no failure of the
        hook: after a wait the hook is called again with a job built anew, at
        most ``MAX_RESULTS_RETRIES`` times; one more is a raise of the hook.

        Raises:
            asyncio.CancelledError: a stop ended the wait for the next call.
        """
        failures = 0
        while True:
            try:
                return facts, await self._call_hook(hook, facts.job), None
            except SplitResultsUnavailable as error:
                failures += 1
                suppressed = self._log_limit.admit(f'results:{owned.message.offset}')
                if suppressed is not None:
                    logger.warning(
                        'split_results_unavailable',
                        category='split',
                        partition=self._label,
                        job=facts.job.job_id,
                        hook=hook,
                        error=str(error),
                        failures=failures,
                        suppressed=suppressed,
                    )
                if failures > MAX_RESULTS_RETRIES:
                    return facts, None, error
            except Exception as error:
                return facts, None, error
            await self._sleep_until_stop(RESULTS_RETRY_SECONDS)
            facts = rebuild()

    async def _call_hook(self, hook: str, job: SplitJob) -> Any:
        """Call one hook of the end in the pipeline hook scope, with its hook context, and time it."""
        offset = job.source_message.offset
        bind_contextvars(hook=hook, offset=offset)
        hook_token = bind_hook_context(hook=hook, partition=self._label, offset=offset, offsets=(offset,))
        started = time.monotonic()
        try:
            with pipeline_hook():
                return await getattr(self._handler, hook)(job)
        finally:
            unbind_contextvars('hook', 'offset')
            clear_hook_context(hook_token)
            handler_duration.labels(hook=hook).observe(time.monotonic() - started)

    async def _sleep_until_stop(self, seconds: float) -> None:
        """Wait ``seconds``.

        Raises:
            asyncio.CancelledError: a stop came first.
        """
        sleeper = asyncio.ensure_future(self._settings.sleep(seconds))
        stop = asyncio.ensure_future(self._stopped.wait())
        try:
            await asyncio.wait({sleeper, stop}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            sleeper.cancel()
            stop.cancel()
        if self._stopped.is_set():
            raise asyncio.CancelledError(STOPPED_MESSAGE)

    async def _deliver(self, result: CollectResult, owned: _OwnedJob) -> bool:
        """Deliver the answer of the hook with the context of the original message; false when the offset must stall.

        The rules are those of the result of ``on_message_complete``: an
        unconfirmed delivery stalls the offset, and any other error follows
        ``dlq.on_send_failure``.

        Raises:
            asyncio.CancelledError: the processor gave its partition up; nothing is delivered.
        """
        if self._host.deliveries_suppressed:
            raise asyncio.CancelledError(STOPPED_MESSAGE)
        message = owned.message
        try:
            await self._on_collect(
                result,
                self._label,
                message_context=MessageContext(source_messages=[message], states=[owned.state]),
            )
        except SinkDeliveryFailedError as error:
            logger.error(
                'aggregate_delivery_failed',
                category='sink',
                partition=self._label,
                offset=message.offset,
                error=str(error),
            )
            return False
        except Exception as error:
            stalls = self._on_dlq_failure == 'stall'
            logger.error(
                'aggregate_sink_delivery_failed',
                category='handler',
                partition=self._label,
                offset=message.offset,
                error=str(error),
                offset_action='stall' if stalls else 'commit',
                exc_info=True,
            )
            return not stalls
        return True

    def _dlq_fields(self, facts: _EndFacts, timed_out: TimedOutKind) -> dict[str, Any]:
        """The fields of the DLQ record of a job whose hook answered ``JobAction.DLQ``.

        After a timeout the record says so. Else it carries the DLQ outcome
        of the first failed part, so it looks as the record of an ordinary
        message does; with no such outcome it names that part's error, or the
        choice of the hook.
        """
        job, failure = facts.job, facts.first_failure
        if timed_out:
            parts = '?' if job.total_parts is None else job.total_parts
            waited = _whole_seconds(job.duration_seconds)
            error = f'job timed out after {waited}: {job.received_parts} of {parts} parts'
            return {'stage': 'aggregate', 'error': error, 'attempts': None}
        if failure is not None and failure.dlq is not None:
            dlq = failure.dlq
            return {
                'stage': dlq.stage,
                'error': dlq.error,
                'sink': dlq.sink,
                'task_id': dlq.task_id,
                'attempts': dlq.attempts,
            }
        if failure is not None and failure.error is not None:
            return {'stage': 'aggregate', 'error': f'part {failure.index} failed: {failure.error}', 'attempts': None}
        error = f'aggregate chose the DLQ: {job.failed_parts} of {job.total_parts} parts failed'
        return {'stage': 'aggregate', 'error': error, 'attempts': None}

    async def _send_to_dlq(
        self,
        owned: _OwnedJob,
        *,
        stage: DLQStage,
        error: str,
        attempts: int | None,
        sink: str | None = None,
        task_id: str | None = None,
    ) -> bool:
        """Write the one DLQ record of the original message; true when its offset may complete.

        A record that could not be written follows ``dlq.on_send_failure``,
        as a ``parse`` record does: ``drop`` counts the loss and lets the
        offset complete, ``stall`` holds it. A loss the sink manager already
        counted for this message is not counted again.
        """
        message = owned.message
        already_lost = owned.state.dlq_lost
        written = await self._sink_manager.send_job_dlq(
            MessageContext(source_messages=[message], states=[owned.state]),
            stage=stage,
            error=error,
            sink=sink,
            task_id=task_id,
            attempts=attempts,
            partition_id=self._label,
        )
        if written:
            return True
        if self._on_dlq_failure == 'stall':
            return False
        if not already_lost:
            dlq_dropped_payloads.labels(partition=str(self._label)).inc()
            logger.critical(
                'dlq_failure_payloads_dropped',
                category='split',
                partition=self._label,
                offset=message.offset,
                reason=f'the {stage} record of a split job was not written to the DLQ',
                action='ALERT: source message lost (dlq.on_send_failure=drop)',
            )
        return True

    async def _close_job(self, owned: _OwnedJob, ref: JobRef, job: SplitJob, outcome: ClosedOutcome) -> bool:
        """Write ``closed`` after the answer of the hook was acted on, then count and log the end of the job.

        Raises:
            asyncio.CancelledError: a stop ended the retries of the write; the offset stays pending.
        """
        duration = max(0.0, self._settings.clock() - job.opened_at)
        written = await self._write_closed(
            ref, outcome, received=job.received_parts, parts=job.total_parts, duration=duration
        )
        if written == 'stopped':
            raise asyncio.CancelledError(STOPPED_MESSAGE)
        self._jobs.pop(owned.message.offset, None)
        if written == 'final':
            return False
        split_job_duration.observe(duration)
        split_jobs.labels(outcome=outcome).inc()
        logger.info(
            'split_job_closed',
            category='split',
            partition=self._label,
            job=job.job_id,
            outcome=outcome,
            parts=job.total_parts,
            received=job.received_parts,
            failed_parts=job.failed_parts,
            seconds=round(duration, 3),
        )
        return True

    async def _write_closed(
        self, ref: JobRef, outcome: ClosedOutcome, *, received: int, parts: int | None, duration: float
    ) -> ClosedWrite:
        """Write the ``closed`` record of a job and say how the write ended.

        A failure a retry can fix is tried again after 1, 2, 4 ... 30
        seconds. A stop ends the retries, never the first attempt
        (``stopped``). A record that can never be written is logged
        (``final``).
        """
        body = closed_body(outcome=outcome, received=received, parts=parts, duration_seconds=round(max(duration, 0), 3))
        record = log_record(ref, 'closed', body)
        wait = RETRY_FIRST_WAIT_SECONDS
        while True:
            try:
                await publish_or_stop(self._settings.producer, self._settings.results_topic, record, self._stopped)
                return 'written'
            except SplitProduceError as error:
                self._note_closed_failure(ref, outcome, error)
                if error.final:
                    return 'final'
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
                return 'stopped'
            try:
                await self._sleep_until_stop(wait)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
                return 'stopped'
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)

    def _note_closed_failure(self, ref: JobRef, outcome: ClosedOutcome, error: SplitProduceError) -> None:
        """Log a failed ``closed`` write: an error when it is final, else a rate-limited warning."""
        if error.final:
            logger.error(
                'split_closed_write_failed',
                category='split',
                partition=self._label,
                job=_ref_id(ref),
                outcome=outcome,
                error=str(error),
                final=True,
                hint='the offset stalls; after a restart the job ends again',
            )
            return
        suppressed = self._log_limit.admit('closed')
        if suppressed is not None:
            logger.warning(
                'split_closed_write_failed',
                category='split',
                partition=self._label,
                job=_ref_id(ref),
                outcome=outcome,
                error=str(error),
                final=False,
                suppressed=suppressed,
            )
