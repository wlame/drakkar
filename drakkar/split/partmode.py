"""The part side of a split: what a partition processor needs to run a part and report its result.

A part is a message of the parts topic. The worker that runs it reports one
``result`` record to the results log, and only then completes the part's
offset. ``PartMode`` is what the processor receives; ``build_part_result``
turns a finished message into the result; ``ResultWriter`` writes the record
and tries again until it is acknowledged.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from drakkar.metrics import split_result_write_failures
from drakkar.models import CollectResult, SourceMessage, SplitPart
from drakkar.providers.resilience import RateLimitedLog
from drakkar.split.http_message import delivers_to_sinks
from drakkar.split.models import PartResult, SinkCounts
from drakkar.split.producer import SplitProduceError, SplitProducer
from drakkar.split.records import H_REQUEST_ID, Header, JobRef, WireRecord, log_record, result_body
from drakkar.utils import validate_request_id

if TYPE_CHECKING:
    from drakkar.sinks.manager import CollectCallback, MessageContext, MessageDLQState, SinkManager

logger = structlog.get_logger()

# The wait before the second attempt to write a result; each later wait is twice as long, up to the cap.
RETRY_FIRST_WAIT_SECONDS = 1.0
RETRY_MAX_WAIT_SECONDS = 30.0
# How long a write in flight may still wait for its delivery report once a stop is signalled.
STOP_GRACE_SECONDS = 0.5
STOPPED_MESSAGE = 'the worker stops before the part result is acknowledged'

# How many finished parts of one partition may wait for their result to be
# written before the partition takes no further part: twice the window size,
# and at least the floor. A result waits while the results topic cannot be
# written. Without the bound the worker would go on running parts at full
# speed: each one keeps its message and its result in memory, none of their
# offsets can be committed, and after a restart all of them run again and
# their sinks receive every delivery twice. With it the partition's queue
# fills and the fetch pauses, as for any backlog.
MAX_WAITING_REPORTS_PER_WINDOW = 2
MAX_WAITING_REPORTS_FLOOR = 16

ReportPart = Callable[[SourceMessage, PartResult], Awaitable[None]]


async def no_report(message: SourceMessage, result: PartResult) -> None:
    """The ``report`` of a processor whose parts are local messages: their results go to the owner of the partition.

    Raises:
        RuntimeError: always; the processor hands such a result to its owner and never calls this.
    """
    raise RuntimeError('the result of a local message is not reported to a topic')


def max_waiting_reports(window_size: int) -> int:
    """The cap on the finished parts of one partition that wait for their result to be written."""
    return max(MAX_WAITING_REPORTS_FLOOR, MAX_WAITING_REPORTS_PER_WINDOW * window_size)


@dataclass(frozen=True)
class PartMode:
    """What a partition processor needs to treat every message it accepts as a part.

    ``source_group`` is the consumer group of the worker's own source: a part
    of another group is skipped. ``report`` writes the result of a finished
    part; the processor completes the part's offset only after it returns.
    ``part_data_model`` is the handler's model of the data of a part; the
    part side sends the data as the hook returned it and does not check it
    against the model. The processor sets ``stopped`` when a stop is
    signalled, so a report that waits for the results topic can give up.
    """

    worker: str
    max_part_data: int
    part_data_model: type[BaseModel] | None
    report: ReportPart
    source_group: str
    stopped: asyncio.Event = field(default_factory=asyncio.Event)


def part_request_id(headers: Sequence[Header] | None) -> str | None:
    """The request id a part carries in its ``dk-request-id`` header, or None when it has no valid one."""
    for name, value in headers or ():
        if name != H_REQUEST_ID:
            continue
        if value is None:
            return None
        try:
            request_id = value.decode('utf-8')
            validate_request_id(request_id)
        except ValueError:
            # UnicodeDecodeError is a ValueError.
            return None
        return request_id
    return None


def checked_part_data(data: object, max_part_data: int) -> tuple[BaseModel | None, str | None]:
    """The data of a part with the reason it was dropped: ``(data, None)`` or ``(None, data_error)``.

    The data is dropped when it is not a pydantic model, or when its JSON is
    larger than ``max_part_data`` bytes of UTF-8.
    """
    if data is None:
        return None, None
    if not isinstance(data, BaseModel):
        return None, part_data_raised(TypeError(f'part_data returned {type(data).__name__}, want a pydantic model'))
    size = len(data.model_dump_json().encode('utf-8'))
    if size > max_part_data:
        return None, f'part data is {size} bytes, the limit is {max_part_data}'
    return data, None


def part_data_raised(error: BaseException) -> str:
    """The ``data_error`` of a part whose ``part_data`` hook raised ``error``."""
    return f'part_data raised: {error}'


def build_part_result(
    *,
    index: int,
    worker: str,
    tracker_counts: tuple[int, int, int],
    state: MessageDLQState,
    error: str | None,
    data: BaseModel | None,
    data_error: str | None,
    started_at: float,
    finished_at: float,
) -> PartResult:
    """The result of one finished part.

    ``tracker_counts`` holds the tasks that succeeded, failed and were
    replaced. ``state`` is the DLQ state of the part's message in capture
    mode: its captured outcome and its delivery counts go into the result.
    ``started_at`` and ``finished_at`` are epoch seconds. The outcome is
    ``failed`` when ``error`` is set or the state holds a captured outcome.
    """
    succeeded, failed, replaced = tracker_counts
    is_failed = error is not None or state.captured is not None
    return PartResult(
        index=index,
        worker=worker,
        outcome='failed' if is_failed else 'ok',
        tasks_succeeded=succeeded,
        tasks_failed=failed,
        tasks_replaced=replaced,
        deliveries={ref: SinkCounts(**counts.model_dump()) for ref, counts in state.counts.items()},
        dlq=state.captured,
        error=error,
        data=data,
        data_error=data_error,
        duration_seconds=max(0.0, finished_at - started_at),
        finished_at=finished_at,
    )


def log_dropped_delivery(partition: int, request_id: str | None) -> None:
    """Log one result that was not delivered because its job asked for no sink writes."""
    logger.debug('split_http_delivery_dropped', category='split', partition=partition, request_id=request_id)


def part_collect(sink_manager: SinkManager, on_collect: CollectCallback) -> CollectCallback:
    """Wrap ``on_collect`` for the results of a part: a result that names an aggregate sink is not delivered.

    Such a result is an error of the handler, and nothing of it reaches a
    sink. A result of a task, a group or a message gives each of its source
    messages a captured DLQ outcome of stage ``sink`` with the fixed error
    text, so the part ends as a message on the DLQ ends. A window result has
    no source message to carry the outcome: the error is logged.

    After that check, a result is dropped when one of its source messages
    belongs to a job that asked for no sink writes: no sink sees it, and it
    adds no delivery count to the part.
    """

    async def collect(
        result: CollectResult, partition_id: int, message_context: MessageContext | None = None
    ) -> dict | None:
        # A result with no context is left to the delivery, which fails closed for it.
        error = None if message_context is None else sink_manager.stage_error(result, 'part')
        if message_context is None:
            return await on_collect(result, partition_id, message_context=message_context)
        if error is None:
            dropped_for = _sinks_off_message(message_context.source_messages)
            if dropped_for is not None:
                log_dropped_delivery(partition_id, dropped_for.request_id)
                return {}
            return await on_collect(result, partition_id, message_context=message_context)
        if message_context.is_window:
            logger.error('split_stage_error', category='split', partition=partition_id, error=error)
        else:
            await sink_manager.capture_stage_error(message_context, sink=None, error=error)
        return {}

    return collect


def _sinks_off_message(messages: Sequence[SourceMessage]) -> SourceMessage | None:
    """The first of ``messages`` whose job asked for no sink writes, or None."""
    for message in messages:
        if not delivers_to_sinks(message):
            return message
    return None


def _result_ref(part: SplitPart) -> JobRef:
    return JobRef(
        group=part.group,
        partition=part.source_partition,
        offset=part.source_offset,
        attempt=part.attempt,
        attempt_id=part.attempt_id,
    )


async def publish_or_stop(producer: SplitProducer, topic: str, record: WireRecord, stopped: asyncio.Event) -> int:
    """Publish ``record`` and wait for its delivery report; after a stop the write gets only the grace time.

    Gives the offset of the record in its partition.

    The write and the stop are awaited together, so a write that waits for
    room in the client queue ends at a stop too. A write that starts after
    the stop still gets ``STOP_GRACE_SECONDS``.

    Raises:
        SplitProduceError: the record was not delivered.
        asyncio.CancelledError: the stop ended the wait; the record may or may not have been written.
    """
    publish = asyncio.ensure_future(producer.publish(topic, record))
    try:
        if not stopped.is_set():
            stop = asyncio.ensure_future(stopped.wait())
            try:
                await asyncio.wait({publish, stop}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                stop.cancel()
        if not publish.done():
            await asyncio.wait({publish}, timeout=STOP_GRACE_SECONDS)
        if not publish.done():
            # A record already in the client queue may still be written: a reader of
            # the results log ignores a second record of the same kind for one job.
            raise asyncio.CancelledError(STOPPED_MESSAGE)
        return publish.result()
    finally:
        if not publish.done():
            publish.cancel()


class PartReportError(SplitProduceError):
    """The result of a part can never be written; ``attempt`` is the number of the write that said so."""

    def __init__(self, message: str, *, attempt: int) -> None:
        super().__init__(message, final=True)
        self.attempt = attempt


class ResultWriter:
    """Writes the ``result`` record of a finished part and tries again until the record is acknowledged.

    A stop of the part's partition ends the retries, never the first
    attempt: a part that finishes while its partition drains is still
    reported, so it does not run again for nothing. ``stopped`` is the event
    the partition sets at its stop; a write waits on it and never polls.
    ``partition`` is the number the worker shows for the partition, for the
    log lines.
    """

    def __init__(self, producer: SplitProducer, *, results_topic: str, stopped: asyncio.Event, partition: int) -> None:
        self._producer = producer
        self._results_topic = results_topic
        self._stopped = stopped
        self._partition = partition
        self._failure_log = RateLimitedLog(time.monotonic)

    async def write(self, message: SourceMessage, result: PartResult) -> None:
        """Publish the result of the part ``message`` to the results partition of its source partition.

        A failure that a retry can fix is counted, logged and tried again
        after 1, 2, 4 ... 30 seconds, for as long as it takes. Once a stop is
        signalled no wait and no further attempt follows, and an attempt in
        flight gets ``STOP_GRACE_SECONDS`` to end.

        Raises:
            PartReportError: the record can never be delivered (a ``SplitProduceError`` with ``final`` true).
            asyncio.CancelledError: the partition stops; the record may or may not have been written.
        """
        part = message.split
        if part is None:
            raise ValueError('a result can be written only for a message of the parts topic')
        record = log_record(_result_ref(part), 'result', result_body(result))
        wait = RETRY_FIRST_WAIT_SECONDS
        attempt = 0
        while True:
            attempt += 1
            try:
                await self._attempt(record)
                return
            except SplitProduceError as error:
                if error.final:
                    raise PartReportError(str(error), attempt=attempt) from error
                self._note_failure(part, error, attempt)
            await self._wait(wait)
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)

    async def _attempt(self, record: WireRecord) -> None:
        """One write: the place in the client queue and the delivery report, and after a stop only the grace time."""
        await publish_or_stop(self._producer, self._results_topic, record, self._stopped)

    async def _wait(self, seconds: float) -> None:
        """Wait ``seconds`` before the next attempt; a stop ends the wait at once."""
        try:
            await asyncio.wait_for(self._stopped.wait(), timeout=seconds)
        except TimeoutError:
            return
        raise asyncio.CancelledError(STOPPED_MESSAGE)

    def _note_failure(self, part: SplitPart, error: SplitProduceError, attempt: int) -> None:
        split_result_write_failures.inc()
        suppressed = self._failure_log.admit('write')
        if suppressed is not None:
            logger.warning(
                'split_result_write_failed',
                category='split',
                job=part.job_id,
                partition=self._partition,
                part=part.index,
                error=str(error),
                final=False,
                attempt=attempt,
                suppressed=suppressed,
            )
