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
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from drakkar.metrics import split_result_write_failures
from drakkar.models import CollectResult, SourceMessage, SplitPart
from drakkar.providers.resilience import RateLimitedLog
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
# How often a wait or a write in flight asks whether the worker stops.
STOP_CHECK_SECONDS = 0.1
# How long a write in flight may still wait for its delivery report once a stop is signalled.
STOP_GRACE_SECONDS = 0.5
STOPPED_MESSAGE = 'the worker stops before the part result is acknowledged'

ReportPart = Callable[[SourceMessage, PartResult], Awaitable[None]]


@dataclass(frozen=True)
class PartMode:
    """What a partition processor needs to treat every message it accepts as a part.

    ``source_group`` is the consumer group of the worker's own source: a part
    of another group is skipped. ``report`` writes the result of a finished
    part; the processor completes the part's offset only after it returns.
    ``part_data_model`` is the handler's model of the data of a part; the
    part side sends the data as the hook returned it and does not check it
    against the model.
    """

    worker: str
    max_part_data: int
    part_data_model: type[BaseModel] | None
    report: ReportPart
    source_group: str


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


def part_collect(sink_manager: SinkManager, on_collect: CollectCallback) -> CollectCallback:
    """Wrap ``on_collect`` for the results of a part: a result that names an aggregate sink is not delivered.

    Such a result is an error of the handler. Nothing of it reaches a sink,
    and each of its source messages gets a captured DLQ outcome of stage
    ``sink`` with the fixed error text, so the part ends as a message on the
    DLQ ends. A window result has no source message and is delivered as it is.
    """

    async def collect(
        result: CollectResult, partition_id: int, message_context: MessageContext | None = None
    ) -> dict | None:
        if message_context is not None and not message_context.is_window:
            error = sink_manager.stage_error(result, 'part')
            if error is not None:
                await sink_manager.capture_stage_error(message_context, sink=None, error=error)
                return {}
        return await on_collect(result, partition_id, message_context=message_context)

    return collect


def _result_ref(part: SplitPart) -> JobRef:
    return JobRef(
        group=part.group,
        partition=part.source_partition,
        offset=part.source_offset,
        attempt=part.attempt,
        attempt_id=part.attempt_id,
    )


def _retrieve_outcome(future: asyncio.Future[None]) -> None:
    """Read the outcome of a delivery future nobody waits for any more, so no unread error is reported."""
    if not future.cancelled():
        future.exception()


class ResultWriter:
    """Writes the ``result`` record of a finished part and tries again until the record is acknowledged.

    A stop of the worker (or of the part's partition) ends the retries, never
    the first attempt: a part that finishes while its partition drains is
    still reported, so it does not run again for nothing.
    """

    def __init__(self, producer: SplitProducer, *, results_topic: str, stopping: Callable[[], bool]) -> None:
        self._producer = producer
        self._results_topic = results_topic
        self._stopping = stopping
        self._failure_log = RateLimitedLog(time.monotonic)

    async def write(self, message: SourceMessage, result: PartResult) -> None:
        """Publish the result of the part ``message`` to the results partition of its source partition.

        A failure that a retry can fix is counted, logged and tried again
        after 1, 2, 4 ... 30 seconds, for as long as it takes. Once a stop is
        signalled no wait and no further attempt follows, and an attempt in
        flight gets ``STOP_GRACE_SECONDS`` to end.

        Raises:
            SplitProduceError: the record can never be delivered (``final`` is true).
            asyncio.CancelledError: the worker stops; the record may or may not have been written.
        """
        part = message.split
        if part is None:
            raise ValueError('a result can be written only for a message of the parts topic')
        record = log_record(_result_ref(part), 'result', result_body(result))
        wait = RETRY_FIRST_WAIT_SECONDS
        while True:
            try:
                await self._attempt(record)
                return
            except SplitProduceError as error:
                if error.final:
                    raise
                self._note_failure(part, error)
            await self._wait(wait)
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)

    async def _attempt(self, record: WireRecord) -> None:
        """One write: wait for the delivery report, and after a stop only for ``STOP_GRACE_SECONDS`` more."""
        future = await self._producer.enqueue(self._results_topic, record)
        stop_seen_at: float | None = None
        while True:
            done, _ = await asyncio.wait({future}, timeout=STOP_CHECK_SECONDS)
            if done:
                future.result()
                return
            if not self._stopping():
                continue
            now = time.monotonic()
            if stop_seen_at is None:
                stop_seen_at = now
            elif now - stop_seen_at >= STOP_GRACE_SECONDS:
                # The record stays in the client queue and may still be written: a second result
                # of the same part, after the part ran again, is ignored by the owner.
                future.add_done_callback(_retrieve_outcome)
                raise asyncio.CancelledError(STOPPED_MESSAGE)

    async def _wait(self, seconds: float) -> None:
        """Sleep ``seconds`` in short steps; a stop ends the wait at once."""
        deadline = time.monotonic() + seconds
        while True:
            if self._stopping():
                raise asyncio.CancelledError(STOPPED_MESSAGE)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, STOP_CHECK_SECONDS))

    def _note_failure(self, part: SplitPart, error: SplitProduceError) -> None:
        split_result_write_failures.inc()
        suppressed = self._failure_log.admit('write')
        if suppressed is not None:
            logger.warning(
                'split_result_write_failed',
                category='split',
                job=part.job_id,
                part=part.index,
                error=str(error),
                final=False,
                suppressed=suppressed,
            )
