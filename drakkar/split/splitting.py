"""The run of one split: the pulls of the ``split`` generator, the publishing of its parts, its failure and its cancel.

A ``SplitRun`` is one attempt of one job. It pulls the first item of
``split(message)``. Nothing yielded: the message is local. Otherwise it writes
``open`` to the results log, publishes each part to the parts topic with a
bounded number of unacknowledged records, and writes ``split-done``. It writes
no DLQ record and no ``closed`` record: its owner acts on the result it returns.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.groups import STREAM_CLOSE_TIMEOUT, STREAM_END, close_stream, next_stream_item, retrieve_task_outcome
from drakkar.hook_scope import PipelineHookScope, pipeline_hook
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.metrics import handler_duration, split_parts_published
from drakkar.models import SourceMessage
from drakkar.providers.resilience import RateLimitedLog
from drakkar.request_ids import RequestIdResolver, reset_request_id
from drakkar.sinks.dlq_record import cut_error
from drakkar.split.partmode import RETRY_FIRST_WAIT_SECONDS, RETRY_MAX_WAIT_SECONDS
from drakkar.split.producer import NO_OFFSET, SplitProduceError, SplitProducer
from drakkar.split.records import JobRef, WireRecord, log_record, open_body, part_record, part_value, ref_id
from drakkar.split.table import MAX_PART_INDEX

logger = structlog.get_logger()

# Part records sent and not yet acknowledged during a split. At the limit the
# split waits for all of them, so the client queue and the memory of the owner
# stay bounded whatever the number of parts.
MAX_UNACKED_PARTS = 1000
# The time limit of the ``split-cancelled`` write of a cancelled split.
CANCEL_WRITE_SECONDS = 2.0

SleepFn = Callable[[float], Awaitable[None]]
# Why a split in progress stops: a stop or a revoke, the attempt of another
# worker instance, a ``closed`` record of the job, or a timeout of the job.
InterruptReason = Literal['stop', 'superseded', 'closed', 'job', 'idle']
# The reasons after which the attempt is marked as cancelled in the results log.
_CANCEL_RECORD_REASONS: frozenset[str] = frozenset({'stop', 'job', 'idle'})


@dataclass(frozen=True)
class SplitSettings:
    """What every split of one worker shares: the producer, the topics and the identity of the worker."""

    producer: SplitProducer
    parts_topic: str
    results_topic: str
    parts_partition_count: int
    worker: str
    instance: str
    sleep: SleepFn = asyncio.sleep
    close_timeout: float = STREAM_CLOSE_TIMEOUT
    cancel_write_seconds: float = CANCEL_WRITE_SECONDS


class SplitEnd(StrEnum):
    """How a run of ``split`` ended."""

    # The generator yielded nothing: the message runs on this worker.
    LOCAL = 'local'
    # Every part and ``split-done`` are acknowledged.
    DONE = 'done'
    # The generator raised, an item was refused, or a record can never be produced.
    FAILED = 'failed'
    # The split was stopped before its end.
    INTERRUPTED = 'interrupted'


@dataclass(frozen=True)
class SplitResult:
    """What a run of ``split`` came to.

    ``reason`` is the interrupt of an ``INTERRUPTED`` run; a ``DONE`` run
    carries one that came together with its last acknowledgement, when no
    wait of the split was left to see it. ``open_enqueued`` says whether the
    ``open`` record may have reached the log, ``open_acknowledged`` whether
    the log has it.
    """

    end: SplitEnd
    parts: int = 0
    open_enqueued: bool = False
    open_acknowledged: bool = False
    # The highest offset of a record of the results log that this run wrote and that was acknowledged; -1 for none.
    written_through: int = -1
    # Whether the delivery report of an acknowledged record of the results log named no offset.
    unplaced_record: bool = False
    reason: InterruptReason | None = None
    failure: str = ''
    cause: BaseException | None = None


class _SplitFailed(Exception):
    """The split cannot go on."""

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


def _read_outcome(future: asyncio.Future[Any]) -> None:
    """Read the outcome of a delivery future, so a failure nobody waits for is not logged as unread."""
    if not future.cancelled():
        future.exception()


class SplitRun:
    """One run of the ``split`` generator for one attempt of one job.

    ``interrupt`` makes the run stop at its next wait. ``on_first_part`` is
    called once, when the generator yielded its first item and before any
    record is written: from then on the message is a job.
    ``crashed_attempts`` is set for a new attempt of a job the results log
    already knows.
    """

    def __init__(
        self,
        settings: SplitSettings,
        *,
        handler: Any,
        message: SourceMessage,
        ref: JobRef,
        partition_label: int,
        on_first_part: Callable[[], None],
        crashed_attempts: int | None,
        log_limit: RateLimitedLog,
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._message = message
        self.ref = ref
        self._label = partition_label
        self._on_first_part = on_first_part
        self._crashed_attempts = crashed_attempts
        self._log_limit = log_limit
        self._context = contextvars.copy_context()
        self._interrupt: asyncio.Future[InterruptReason] = asyncio.get_running_loop().create_future()
        self._started_at = time.monotonic()
        self._stream: AsyncIterator[Any] | None = None
        self._pull_task: asyncio.Task | None = None
        self._left_scope: PipelineHookScope | None = None
        self._in_flight: dict[asyncio.Future[int], WireRecord] = {}
        self.open_enqueued = False
        self.open_acknowledged = False
        # The highest acknowledged offset of a record this run wrote to the results log; -1 for none.
        self.written_through = -1
        # Whether the delivery report of an acknowledged record of the results log named no offset.
        self.unplaced_record = False
        self.parts = 0
        # Set by the owner when the job tracker read the ``open`` record of this attempt.
        self.own_open_seen = False

    def interrupt(self, reason: InterruptReason) -> None:
        """Make the run stop at its next wait; the first reason is kept."""
        if not self._interrupt.done():
            self._interrupt.set_result(reason)

    @property
    def interrupted(self) -> InterruptReason | None:
        """The reason the run was asked to stop, if it was."""
        return self._interrupt.result() if self._interrupt.done() else None

    async def run(self) -> SplitResult:
        """Run the split to its end and say how it ended; an interrupted or failed run has closed its generator.

        Raises:
            asyncio.CancelledError: the task of the run was cancelled; no time was left to close the generator.
        """
        try:
            try:
                stream = self._open_stream()
                self._stream = stream
                first = await self._pull(stream)
                if first is STREAM_END:
                    return SplitResult(end=SplitEnd.LOCAL)
                self._on_first_part()
                await self._publish(stream, first)
            except _SplitFailed as failure:
                await self._close_stream()
                self._drop_in_flight()
                return self._result(SplitEnd.FAILED, failure=failure.text, cause=failure.cause)
            except _SplitInterrupted as interrupted:
                await self._cancel(interrupted.reason)
                return self._result(SplitEnd.INTERRUPTED, reason=interrupted.reason)
            except asyncio.CancelledError:
                if self._pull_task is not None:
                    self._pull_task.cancel()
                self._drop_in_flight()
                raise
        finally:
            if self._left_scope is not None:
                # Only marked: the context of the split, where its token was set, is dropped.
                self._left_scope.ended = True
        return self._result(SplitEnd.DONE, reason=self.interrupted)

    def _result(
        self,
        end: SplitEnd,
        *,
        reason: InterruptReason | None = None,
        failure: str = '',
        cause: BaseException | None = None,
    ) -> SplitResult:
        return SplitResult(
            end=end,
            parts=self.parts,
            open_enqueued=self.open_enqueued,
            open_acknowledged=self.open_acknowledged,
            written_through=self.written_through,
            unplaced_record=self.unplaced_record,
            reason=reason,
            failure=failure,
            cause=cause,
        )

    # --- the generator ---

    def _open_stream(self) -> AsyncIterator[Any]:
        """Call ``split(message)`` and return its async iterator.

        Raises:
            _SplitFailed: the hook raised, or returned something that is no async iterator.
        """
        # Calling an async generator function runs none of its code: that happens in the pulls.
        try:
            result = self._handler.split(self._message)
        except Exception as error:
            raise _SplitFailed(_error_text(error), error) from error
        if not hasattr(result, '__aiter__'):
            close = getattr(result, 'close', None)
            if close is not None:
                # A coroutine of an ``async def`` without a yield: closed, so it does not warn.
                close()
            raise _SplitFailed(f'split must be an async generator, got {type(result).__name__}')
        return result.__aiter__()

    def _bind_pull(self) -> tuple[Any, Any, PipelineHookScope]:
        """Bind the log fields, request id, hook context and pipeline scope of one pull; run in the split's context."""
        offset = self._message.offset
        bind_contextvars(partition=self._label, hook='split', offset=offset)
        request_id_tokens = RequestIdResolver.bind([self._message.request_id])
        hook_token = bind_hook_context(hook='split', partition=self._label, offset=offset, offsets=(offset,))
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

    async def _pull(self, stream: AsyncIterator[Any]) -> Any:
        """The next item of the generator, or ``STREAM_END``; each pull is timed and runs in the pipeline hook scope.

        The pull runs as a task, so a stop can cancel it and the generator
        can be closed: ``aclose()`` cannot run while a pull is running. So
        the event loop gets a turn at every pull.

        Raises:
            _SplitFailed: the generator raised.
            _SplitInterrupted: the split must stop; the pull is left to the close.
        """
        if self._interrupt.done():
            # Checked before every pull: a generator that never suspends ends each pull in the
            # same loop turn, and the wait below would never see the interrupt.
            raise _SplitInterrupted(self._interrupt.result())
        context = self._context
        tokens = context.run(self._bind_pull)
        started = time.monotonic()
        pull = asyncio.create_task(next_stream_item(stream), context=context)
        # A pull left behind by a stop must not log an unread exception.
        pull.add_done_callback(retrieve_task_outcome)
        self._pull_task = pull
        try:
            await self._wait(pull)
            self._pull_task = None
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
                self._left_scope = tokens[2]

    async def _wait(self, target: asyncio.Future[Any]) -> None:
        """Wait for ``target``.

        Raises:
            _SplitInterrupted: the split must stop before ``target`` ended; ``target`` is left as it is.
        """
        if not target.done():
            await asyncio.wait({target, self._interrupt}, return_when=asyncio.FIRST_COMPLETED)
        if not target.done():
            raise _SplitInterrupted(self._interrupt.result())

    async def _close_stream(self) -> None:
        """Cancel the pull in progress, then close the generator, within the stream-close time; never raises."""
        if self._stream is None:
            return
        pull, self._pull_task = self._pull_task, None
        failure = await close_stream(
            self._stream, pull=pull, context=self._context, timeout=self._settings.close_timeout
        )
        if failure is not None:
            logger.error(
                'split_close_failed',
                category='split',
                partition=self._label,
                job=ref_id(self.ref),
                error='timed out' if failure.timed_out else str(failure.error),
            )

    # --- publishing ---

    async def _publish(self, stream: AsyncIterator[Any], first: Any) -> None:
        """Write ``open``, publish every part the generator yields, then write ``split-done``.

        Raises:
            _SplitFailed: an item or a record was refused for good.
            _SplitInterrupted: the split must stop.
        """
        settings = self._settings
        ref, message = self.ref, self._message
        job = ref_id(ref)
        if self._crashed_attempts is not None:
            logger.warning(
                'split_attempt_started',
                category='split',
                partition=self._label,
                job=job,
                attempt=ref.attempt,
                crashed_attempts=self._crashed_attempts,
            )
        body = open_body(worker=settings.worker, instance=settings.instance, request_id=message.request_id)
        # From here on the record may reach the log, also when its delivery report never comes.
        self.open_enqueued = True
        await self._write_to_log(log_record(ref, 'open', body))
        self.open_acknowledged = True
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
            await self._send_part(item)
            item = await self._pull(stream)
        await self._settle_parts()
        await self._write_to_log(log_record(ref, 'split-done', {'parts': self.parts}))
        logger.info(
            'split_job_done',
            category='split',
            partition=self._label,
            job=job,
            parts=self.parts,
            seconds=round(time.monotonic() - self._started_at, 3),
        )

    async def _send_part(self, item: object) -> None:
        """Put the record of the next part in the producer; at the limit of unacknowledged parts, wait for them."""
        settings = self._settings
        message = self._message
        index = self.parts
        if index > MAX_PART_INDEX:
            raise _SplitFailed(f'more than {MAX_PART_INDEX + 1} parts')
        try:
            value = part_value(item)
        except TypeError as error:
            raise _SplitFailed(str(error)) from None
        record = part_record(
            self.ref,
            source_topic=message.topic,
            index=index,
            request_id=message.request_id,
            key=message.key,
            value=value,
            source_headers=message.headers,
            partition_count=settings.parts_partition_count,
        )
        self.parts += 1
        try:
            future = await self._enqueue(settings.parts_topic, record)
        except SplitProduceError as error:
            self._check_not_final(error)
            await self._deliver_record(settings.parts_topic, record, failed=error)
            split_parts_published.inc()
            return
        self._in_flight[future] = record
        if len(self._in_flight) >= MAX_UNACKED_PARTS:
            await self._settle_parts()

    async def _enqueue(self, topic: str, record: WireRecord) -> asyncio.Future[int]:
        """Give ``record`` to the producer and return its delivery future.

        While the client queue has room this creates no task and does not
        suspend. A wait for room runs as a task, so it ends when the split
        must stop.

        Raises:
            SplitProduceError: the client refuses the record.
            _SplitInterrupted: the split must stop while it waits for room.
        """
        producer = self._settings.producer
        future = producer.enqueue_nowait(topic, record)
        if future is None:
            enqueue = asyncio.ensure_future(producer.enqueue(topic, record))
            try:
                await self._wait(enqueue)
            finally:
                if not enqueue.done():
                    enqueue.cancel()
            future = enqueue.result()
        # Read in every case: a run that stops no longer waits for its records.
        future.add_done_callback(_read_outcome)
        return future

    @staticmethod
    def _check_not_final(error: SplitProduceError) -> None:
        """Raise ``_SplitFailed`` for a produce error that no retry can fix."""
        if error.final:
            raise _SplitFailed(str(error)) from error

    async def _settle_parts(self) -> None:
        """Wait for every part record in flight; count the acknowledged ones and deliver again the ones that failed."""
        waiting = [future for future in self._in_flight if not future.done()]
        if waiting:
            # Cancelling the group cancels the futures it waits for.
            every = asyncio.gather(*waiting, return_exceptions=True)
            # Read in every case: a group that is cancelled at an interrupt has no other reader.
            every.add_done_callback(_read_outcome)
            try:
                await self._wait(every)
            finally:
                if not every.done():
                    every.cancel()
        settled, self._in_flight = self._in_flight, {}
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
            await self._deliver_record(self._settings.parts_topic, record, failed=error)
            split_parts_published.inc()

    async def _write_to_log(self, record: WireRecord) -> None:
        """Write one record of this attempt to the results log and note how far the log holds what this run wrote.

        Raises:
            _SplitFailed: the record can never be delivered.
            _SplitInterrupted: the split must stop.
        """
        self._note_written(await self._deliver_record(self._settings.results_topic, record))

    def _note_written(self, offset: int) -> None:
        """Note an acknowledged record of the results log: its offset, or that its delivery report named none."""
        if offset == NO_OFFSET:
            self.unplaced_record = True
        else:
            self.written_through = max(self.written_through, offset)

    async def _deliver_record(self, topic: str, record: WireRecord, *, failed: SplitProduceError | None = None) -> int:
        """Produce one record and wait for its acknowledgement; a failure a retry can fix is tried again.

        Gives the offset of the record in its partition. The waits are 1, 2,
        4 ... 30 seconds, for as long as it takes. ``failed`` is the error of
        a first attempt the caller already made.

        Raises:
            _SplitFailed: the record can never be delivered.
            _SplitInterrupted: the split must stop.
        """
        wait = RETRY_FIRST_WAIT_SECONDS
        error = failed
        while True:
            if error is None:
                try:
                    future = await self._enqueue(topic, record)
                    await self._wait(future)
                    return future.result()
                except SplitProduceError as produce_error:
                    self._check_not_final(produce_error)
                    error = produce_error
            self._note_publish_failure(error)
            sleeper = asyncio.ensure_future(self._settings.sleep(wait))
            try:
                await self._wait(sleeper)
            finally:
                sleeper.cancel()
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)
            error = None

    def _note_publish_failure(self, error: SplitProduceError) -> None:
        suppressed = self._log_limit.admit('publish')
        if suppressed is not None:
            logger.warning(
                'split_publish_failed',
                category='split',
                partition=self._label,
                job=ref_id(self.ref),
                error=str(error),
                suppressed=suppressed,
            )

    def _drop_in_flight(self) -> None:
        """Stop waiting for the part records in flight; the producer may still deliver them."""
        for future in self._in_flight:
            if not future.done():
                future.cancel()
        self._in_flight = {}

    # --- the cancel ---

    async def _cancel(self, reason: InterruptReason) -> None:
        """Close the generator and, at the same time, mark the attempt as cancelled in the results log.

        The record is written after a stop, a revoke or a timeout, so the
        attempt does not count against ``split.max_attempts``. An attempt
        that another attempt replaced, or whose job is closed, needs none.
        """
        self._drop_in_flight()
        if reason in _CANCEL_RECORD_REASONS and self.open_enqueued:
            await asyncio.gather(self._close_stream(), self._write_cancelled())
        else:
            await self._close_stream()

    async def _write_cancelled(self) -> None:
        """Write ``split-cancelled`` within its time limit; a failure is logged and nothing else follows."""
        record = log_record(self.ref, 'split-cancelled', {})
        try:
            offset = await asyncio.wait_for(
                self._settings.producer.publish(self._settings.results_topic, record),
                timeout=self._settings.cancel_write_seconds,
            )
            self._note_written(offset)
        except (TimeoutError, SplitProduceError, RuntimeError) as error:
            logger.warning(
                'split_cancel_write_failed',
                category='split',
                partition=self._label,
                job=ref_id(self.ref),
                error=str(error) or type(error).__name__,
                hint='the attempt counts against split.max_attempts',
            )
