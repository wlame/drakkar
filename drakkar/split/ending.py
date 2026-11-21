"""The end of a split job: the hook, its answer, the DLQ record and the ``closed`` record.

A ``JobEnder`` calls ``aggregate`` or ``on_job_timeout`` with a view of the
job, delivers the answer to the aggregate sinks or writes the one DLQ record
of the original message, writes ``closed``, and says whether the source offset
may complete. The order is fixed: the aggregate sinks or the DLQ record, then
``closed``, then the offset. A local message ends the same way, with a job of
one part and no record in any topic.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

import structlog
from pydantic import BaseModel
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.config.units import format_duration
from drakkar.hook_scope import pipeline_hook
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.metrics import dlq_dropped_payloads, handler_duration, split_job_duration, split_jobs
from drakkar.models import CollectResult, SinkDeliveryFailedError, SourceMessage
from drakkar.providers.resilience import RateLimitedLog
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
from drakkar.split.records import ClosedOutcome, JobRef, closed_body, job_id, log_record, ref_id
from drakkar.split.table import BITS_PER_BYTE, MS_PER_SECOND, JobState, PartFailure

logger = structlog.get_logger()

# After a failed read of the results inside a hook, the hook is called again
# after this wait, at most this many times; one more failure is a raise of the hook.
RESULTS_RETRY_SECONDS = 30.0
MAX_RESULTS_RETRIES = 20

SleepFn = Callable[[float], Awaitable[None]]
# How a ``closed`` write ended: acknowledged, refused for good, or given up at a stop.
# How the write of a ``closed`` record ended without the record: it can never be written, or a stop ended the retries.
ClosedWriteFailure = Literal['final', 'stopped']


@dataclass(frozen=True)
class EndSettings:
    """What every end of one worker shares: where ``closed`` goes and how the results are read back."""

    producer: SplitProducer
    results_topic: str
    source_group: str
    results_reader: Callable[[], Any]
    clock: Callable[[], float] = time.time
    sleep: SleepFn = asyncio.sleep


@dataclass(frozen=True)
class EndFacts:
    """What the end of a job reads: the view its hook receives and the first failed part."""

    job: SplitJob
    first_failure: PartFailure | None


@dataclass(frozen=True)
class ClosedRecord:
    """An acknowledged ``closed`` record of a job: where it is in the results log, and what it says.

    ``offset`` is ``NO_OFFSET`` when the delivery report named none.
    ``attempt_id`` is the attempt the record names: every reader of the log
    ignores the record once another attempt is the current one. ``job`` is
    the view of an end through a hook; None for a failed split.
    """

    offset: int
    attempt_id: str
    outcome: ClosedOutcome
    duration: float
    job: SplitJob | None = None


def error_text(error: BaseException) -> str:
    """The text of an error (its type when it has none), cut as ``dk-error`` is."""
    return cut_error(str(error) or type(error).__name__)


def _whole_seconds(seconds: float) -> str:
    """The canonical duration text of ``seconds``, cut to whole seconds."""
    return format_duration(int(max(seconds, 0.0)) * MS_PER_SECOND)


def _missing_parts(received_bits: bytes, parts: int | None) -> list[int]:
    """The part numbers below ``parts`` whose bit is not set; empty while ``parts`` is unknown.

    It looks at every part number with no pause, so it costs time in
    proportion to the number of parts (about 3 seconds for 16 million) and
    holds the event loop that long. It is meant for a timeout handler.
    """
    if parts is None:
        return []
    return [
        index
        for index in range(parts)
        if index // BITS_PER_BYTE >= len(received_bits)
        or not received_bits[index // BITS_PER_BYTE] >> index % BITS_PER_BYTE & 1
    ]


def local_facts(message: SourceMessage, result: PartResult) -> EndFacts:
    """The job of one part that a local message is, with its one result held in memory."""

    async def results() -> AsyncIterator[PartResult]:
        yield result

    job = SplitJob(
        source_message=message,
        job_id=job_id(message.partition, message.offset, 1),
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
    failure = None
    if result.outcome == 'failed':
        failure = PartFailure(index=result.index, dlq=result.dlq, error=result.error)
    return EndFacts(job=job, first_failure=failure)


class JobEnder:
    """Ends the jobs and the local messages of one source partition.

    ``stopped`` is set at a stop or a revoke of the partition: it ends every
    wait. ``deliveries_suppressed`` says whether the processor gave its
    partition up, after which nothing is delivered. ``on_collect`` delivers
    the answer of the hook to the aggregate sinks.
    """

    def __init__(
        self,
        settings: EndSettings,
        *,
        partition: int,
        partition_label: int,
        handler: Any,
        sink_manager: SinkManager,
        on_collect: CollectCallback,
        on_dlq_failure: str,
        stopped: asyncio.Event,
        deliveries_suppressed: Callable[[], bool],
        log_limit: RateLimitedLog,
    ) -> None:
        self._settings = settings
        self._partition = partition
        self._label = partition_label
        self._handler = handler
        self._sink_manager = sink_manager
        self._on_collect = on_collect
        self._on_dlq_failure = on_dlq_failure
        self._stopped = stopped
        self._deliveries_suppressed = deliveries_suppressed
        self._log_limit = log_limit
        self._part_data_model: type[BaseModel] | None = getattr(type(handler), 'part_data_model', None)

    # --- the view of a job ---

    def job_facts(
        self, message: SourceMessage, state: JobState, timed_out: TimedOutKind, *, parts_written: int | None = None
    ) -> EndFacts:
        """The view of one attempt of a job for its hook.

        ``state`` is a snapshot of the job (``JobState.snapshot``): the view
        and the results it reads back stay on that attempt, also when the
        log gets a newer one meanwhile. ``parts_written`` is the number of
        parts the owner itself wrote in the ``split-done`` of the attempt;
        it stands in while the table has not read that record yet.
        """
        settings = self._settings
        opened_at = state.opened_at_ms / MS_PER_SECOND
        parts = state.parts if state.parts is not None else parts_written
        job = SplitJob(
            source_message=message,
            job_id=job_id(self._partition, state.offset, state.attempt),
            attempt=state.attempt,
            local=False,
            total_parts=parts,
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
            missing=functools.partial(_missing_parts, bytes(state.received_bits), parts),
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
        return EndFacts(job=job, first_failure=state.first_failure)

    # --- the end ---

    async def end(
        self,
        message: SourceMessage,
        state: MessageDLQState,
        facts: EndFacts,
        timed_out: TimedOutKind,
        *,
        rebuild: Callable[[], EndFacts],
        closing_ref: Callable[[], JobRef | None] | None,
    ) -> bool | ClosedRecord:
        """Call the hook of the end, act on its answer, then write ``closed``; the acknowledged record.

        For a local message, and for a job the results log already has as
        closed, no record is written and the answer is true.

        ``state`` is the DLQ state of the original message. ``rebuild`` makes
        the facts anew, of the same attempt, for a second call of the hook.
        ``closing_ref`` gives the attempt the ``closed`` record names, asked
        when the record is written; it gives None when the results log
        already has the job as closed, and is None itself for a local
        message, which writes no record.

        False means the offset must stall: a delivery or a DLQ record could
        not be confirmed under ``dlq.on_send_failure: stall``, or ``closed``
        can never be written. Nothing is closed then, so a restart ends the
        job again.

        Raises:
            asyncio.CancelledError: a stop ended a wait; the offset stays pending.
        """
        hook = 'on_job_timeout' if timed_out else 'aggregate'
        facts, answer, raised = await self._ask_hook(hook, message, facts, rebuild)
        job = facts.job
        # The part sinks that delivered belong in ``dk-delivered-sinks`` of a DLQ record of the message.
        state.delivered_sinks.update(sink for sink, counts in job.deliveries.items() if counts.delivered > 0)
        failure_text: str | None = None
        if raised is not None:
            failure_text = error_text(raised)
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
            may_complete = await self.send_to_dlq(message, state, stage='aggregate', error=error, attempts=None)
            sent_to_dlq = True
        elif isinstance(answer, CollectResult):
            may_complete = await self._deliver(answer, message, state)
            # A failing aggregate sink under ``on_failure: dlq`` put the original message on the DLQ.
            sent_to_dlq = state.dlq_written or state.dlq_lost
        elif answer is JobAction.DLQ:
            may_complete = await self.send_to_dlq(message, state, **self._dlq_fields(facts, timed_out))
            sent_to_dlq = True
        if not may_complete:
            return False
        if closing_ref is None:
            split_jobs.labels(outcome='dlq' if sent_to_dlq else 'local').inc()
            return True
        # Every end through ``on_job_timeout`` closes as ``timeout``, whatever the hook answered.
        outcome: ClosedOutcome = 'timeout' if timed_out else ('dlq' if sent_to_dlq else 'aggregated')
        return await self._close(closing_ref(), job, outcome)

    async def _ask_hook(
        self, hook: str, message: SourceMessage, facts: EndFacts, rebuild: Callable[[], EndFacts]
    ) -> tuple[EndFacts, Any, Exception | None]:
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
                suppressed = self._log_limit.admit(f'results:{message.offset}')
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

    async def _deliver(self, result: CollectResult, message: SourceMessage, state: MessageDLQState) -> bool:
        """Deliver the answer of the hook with the context of the original message; false when the offset must stall.

        The rules are those of the result of ``on_message_complete``: an
        unconfirmed delivery stalls the offset, and any other error follows
        ``dlq.on_send_failure``.

        Raises:
            asyncio.CancelledError: the processor gave its partition up; nothing is delivered.
        """
        if self._deliveries_suppressed():
            raise asyncio.CancelledError(STOPPED_MESSAGE)
        try:
            await self._on_collect(
                result,
                self._label,
                message_context=MessageContext(source_messages=[message], states=[state]),
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

    @staticmethod
    def _dlq_fields(facts: EndFacts, timed_out: TimedOutKind) -> dict[str, Any]:
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

    # --- the DLQ record and the closed record ---

    async def send_to_dlq(
        self,
        message: SourceMessage,
        state: MessageDLQState,
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
        already_lost = state.dlq_lost
        written = await self._sink_manager.send_job_dlq(
            MessageContext(source_messages=[message], states=[state]),
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

    async def _close(self, ref: JobRef | None, job: SplitJob, outcome: ClosedOutcome) -> bool | ClosedRecord:
        """Write ``closed`` after the answer of the hook was acted on; the acknowledged record.

        Nothing is counted or logged here: the results log may ignore the
        record, and only the owner sees what the log did with it
        (:meth:`note_closed`). With no ``ref`` the results log has the job
        as closed already, by another owner: nothing is written, and the
        answer is true. False when the record can never be written.

        Raises:
            asyncio.CancelledError: a stop ended the retries of the write; the offset stays pending.
        """
        if ref is None:
            return True
        duration = max(0.0, self._settings.clock() - job.opened_at)
        written = await self.write_closed(
            ref, outcome, received=job.received_parts, parts=job.total_parts, duration=duration
        )
        if written == 'stopped':
            raise asyncio.CancelledError(STOPPED_MESSAGE)
        if written == 'final':
            return False
        return ClosedRecord(offset=written, attempt_id=ref.attempt_id, outcome=outcome, duration=duration, job=job)

    def note_closed(self, record: ClosedRecord) -> None:
        """Count and log the end of a job whose ``closed`` record the results log took.

        A job that ended through a hook is timed and logged; a failed split
        is only counted, its failure was logged when it happened.
        """
        split_jobs.labels(outcome=record.outcome).inc()
        job = record.job
        if job is None:
            return
        split_job_duration.observe(record.duration)
        logger.info(
            'split_job_closed',
            category='split',
            partition=self._label,
            job=job.job_id,
            outcome=record.outcome,
            parts=job.total_parts,
            received=job.received_parts,
            failed_parts=job.failed_parts,
            seconds=round(record.duration, 3),
        )

    async def write_closed(
        self, ref: JobRef, outcome: ClosedOutcome, *, received: int, parts: int | None, duration: float
    ) -> int | ClosedWriteFailure:
        """Write the ``closed`` record of a job; its offset in the results log, or how the write failed.

        A failure a retry can fix is tried again after 1, 2, 4 ... 30
        seconds. A stop ends the retries, never the first attempt
        (``stopped``). A record that can never be written is logged
        (``final``).

        Raises:
            asyncio.CancelledError: the task of the caller was cancelled.
        """
        body = closed_body(outcome=outcome, received=received, parts=parts, duration_seconds=round(max(duration, 0), 3))
        record = log_record(ref, 'closed', body)
        wait = RETRY_FIRST_WAIT_SECONDS
        while True:
            try:
                return await publish_or_stop(
                    self._settings.producer, self._settings.results_topic, record, self._stopped
                )
            except SplitProduceError as error:
                self._note_closed_failure(ref, outcome, error)
                if error.final:
                    return 'final'
            except asyncio.CancelledError:
                if _task_is_cancelled():
                    raise
                return 'stopped'
            try:
                await self._sleep_until_stop(wait)
            except asyncio.CancelledError:
                if _task_is_cancelled():
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
                job=ref_id(ref),
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
                job=ref_id(ref),
                outcome=outcome,
                error=str(error),
                final=False,
                suppressed=suppressed,
            )


def _task_is_cancelled() -> bool:
    """Whether the running task itself was cancelled, as opposed to a wait that a stop ended."""
    current = asyncio.current_task()
    return current is not None and current.cancelling() > 0
