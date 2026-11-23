"""The status writer and the gateway of HTTP split jobs.

The gateway runs the accept and the status read of the HTTP source on the
main loop: the status map, its follower and the producers belong to that
loop, and the HTTP source thread reaches them only through a dispatch.
The writer puts status records into the status topic: an end state with
retries until it is acknowledged, the other states best effort.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import structlog

from drakkar.metrics import split_http_jobs_accepted, split_http_status_reads, split_http_status_write_failures
from drakkar.providers.resilience import RateLimitedLog, describe_error
from drakkar.split.http_follower import MAX_STATUS_WAITERS as MAX_STATUS_WAITERS
from drakkar.split.http_message import source_message_record
from drakkar.split.http_status import StatusRecord, answer_body, status_record
from drakkar.split.partmode import RETRY_FIRST_WAIT_SECONDS, RETRY_MAX_WAIT_SECONDS, publish_or_stop
from drakkar.split.producer import SplitProduceError, SplitProducer
from drakkar.utils import validate_request_id

if TYPE_CHECKING:
    from drakkar.split.runtime import HttpJobsRuntime

logger = structlog.get_logger()

# The longest a POST waits for its ``accepted`` record; the answer does not depend on it.
ACCEPTED_WRITE_SECONDS = 5.0
# The longest a status read waits for the first record of an unknown id.
STATUS_WAIT_SECONDS = 1.0

STORE_FAILED_TEXT = 'the request could not be stored'
TOO_LARGE_DETAIL = 'the request is larger than the broker accepts'
QUEUE_FULL_TEXT = 'the client queue is full'
NOT_ACKNOWLEDGED_TEXT = 'the record was not acknowledged in time'
LOG_KEY = 'status_write'

SleepFn = Callable[[float], Awaitable[None]]
EndWrite = int | Literal['stopped', 'final']


def _json(body: dict[str, Any]) -> bytes:
    """Compact JSON text of an answer body."""
    return json.dumps(body, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def _task_is_cancelled() -> bool:
    """Whether the running task itself was cancelled, as opposed to a wait that a stop ended."""
    current = asyncio.current_task()
    return current is not None and current.cancelling() > 0


def _is_valid_request_id(request_id: str) -> bool:
    """True when ``request_id`` is non-empty and passes the request id rule."""
    if not request_id:
        return False
    try:
        validate_request_id(request_id)
    except ValueError:
        return False
    return True


class StatusWriter:
    """Writes status records of HTTP split jobs to the status topic.

    Every failed write, and every failed attempt of an end-state write,
    counts ``drakkar_split_http_status_write_failures_total`` and logs
    ``split_http_status_write_failed`` at most once per interval.
    """

    def __init__(
        self,
        producer: SplitProducer,
        topic: str,
        *,
        clock: Callable[[], float],
        sleep: SleepFn,
        log_limit: RateLimitedLog,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._clock = clock
        self._sleep = sleep
        self._log_limit = log_limit

    async def write_end(self, record: StatusRecord, stopped: asyncio.Event) -> EndWrite:
        """Write an end-state record until it is acknowledged; its offset, or how the write ended.

        A failure a retry can fix is tried again after 1, 2, 4 ... 30
        seconds. A stop ends the retries (``stopped``). A record that can
        never be written, a too large one included, gives ``final``.

        Raises:
            asyncio.CancelledError: the task of the caller was cancelled.
        """
        wire = status_record(record)
        wait = RETRY_FIRST_WAIT_SECONDS
        while True:
            try:
                return await publish_or_stop(self._producer, self._topic, wire, stopped)
            except SplitProduceError as error:
                self._note_failure(record, error)
                if error.final or error.too_large:
                    return 'final'
            except asyncio.CancelledError:
                if _task_is_cancelled():
                    raise
                return 'stopped'
            try:
                await self._sleep_until_stop(wait, stopped)
            except asyncio.CancelledError:
                if _task_is_cancelled():
                    raise
                return 'stopped'
            wait = min(wait * 2, RETRY_MAX_WAIT_SECONDS)

    async def _sleep_until_stop(self, seconds: float, stopped: asyncio.Event) -> None:
        """Wait ``seconds``.

        Raises:
            asyncio.CancelledError: a stop came first.
        """
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        stop = asyncio.ensure_future(stopped.wait())
        try:
            await asyncio.wait({sleeper, stop}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            sleeper.cancel()
            stop.cancel()
        if stopped.is_set():
            raise asyncio.CancelledError('the worker stops before the status record is acknowledged')

    def write_best_effort(self, record: StatusRecord) -> None:
        """Queue ``record`` when the client queue has room now; drop it otherwise. Never waits."""
        try:
            future = self._producer.enqueue_nowait(self._topic, status_record(record))
        except (SplitProduceError, RuntimeError) as error:
            self._note_failure(record, error)
            return
        if future is None:
            self._note_failure(record, QUEUE_FULL_TEXT)
            return
        future.add_done_callback(lambda done: self._note_outcome(record, done))

    def _note_outcome(self, record: StatusRecord, done: asyncio.Future[int]) -> None:
        """Note a best-effort write whose delivery failed or was given up."""
        if done.cancelled():
            self._note_failure(record, 'the write was cancelled')
            return
        error = done.exception()
        if error is not None:
            self._note_failure(record, error)

    async def write_bounded(self, record: StatusRecord, seconds: float) -> bool:
        """Write ``record`` and wait at most ``seconds`` for its acknowledgement; True when it came."""
        publish = asyncio.ensure_future(self._producer.publish(self._topic, status_record(record)))
        timer = asyncio.ensure_future(self._sleep(seconds))
        try:
            await asyncio.wait({publish, timer}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            timer.cancel()
        if not publish.done():
            publish.cancel()
            self._note_failure(record, NOT_ACKNOWLEDGED_TEXT)
            return False
        error = publish.exception()
        if error is not None:
            self._note_failure(record, error)
            return False
        return True

    def _note_failure(self, record: StatusRecord, error: BaseException | str) -> None:
        """Count a failed write and log it, at most one line per interval."""
        split_http_status_write_failures.inc()
        suppressed = self._log_limit.admit(LOG_KEY)
        if suppressed is None:
            return
        text = describe_error(error) if isinstance(error, BaseException) else error
        logger.warning(
            'split_http_status_write_failed',
            category='split',
            request_id=record.request_id,
            state=record.state,
            error=text,
            suppressed=suppressed,
        )


@dataclass(frozen=True)
class HttpAnswer:
    """What the HTTP source answers: the status code, the JSON body and the request metric's status."""

    status_code: int
    body: bytes
    metric_status: str


class HttpJobGateway:
    """Stores HTTP split job requests and answers their status, on the main loop."""

    def __init__(self, runtime: HttpJobsRuntime, *, clock: Callable[[], float]) -> None:
        self._runtime = runtime
        self._clock = clock

    @property
    def is_ready(self) -> bool:
        """True once the status follower has restored the map; safe to read from any thread."""
        return self._runtime.follower.is_restored

    def _check_loop(self) -> None:
        """Refuse a call from any loop but the one the runtime started on.

        Raises:
            RuntimeError: the call runs on another loop, or the runtime is not started.
        """
        if asyncio.get_running_loop() is not self._runtime.loop:
            raise RuntimeError('the HTTP job gateway runs only on the main loop')

    async def accept(
        self, *, request_id: str, client: str, body: bytes, deliver_sinks: bool, timeout: float
    ) -> HttpAnswer:
        """Store a request as a message of the source topic, or answer the job that has its id."""
        self._check_loop()
        runtime = self._runtime
        known = runtime.follower.get(request_id)
        if known is not None:
            if known.client == client:
                return HttpAnswer(200, answer_body(known), 'duplicate')
            return HttpAnswer(409, _json({'error': 'request_id_in_use', 'request_id': request_id}), 'conflict')
        message = source_message_record(request_id=request_id, client=client, deliver_sinks=deliver_sinks, body=body)
        try:
            partition, offset = await asyncio.wait_for(
                runtime.source_producer.publish_located(runtime.source_topic, message), timeout
            )
        except SplitProduceError as error:
            if error.too_large:
                return HttpAnswer(
                    413,
                    _json({'error': 'request_too_large', 'request_id': request_id, 'details': TOO_LARGE_DETAIL}),
                    'error',
                )
            return self._unavailable(request_id)
        except (TimeoutError, RuntimeError):
            return self._unavailable(request_id)
        accepted = StatusRecord(
            request_id=request_id,
            state='accepted',
            partition=partition,
            offset=offset,
            client=client,
            updated_at=self._clock(),
        )
        await runtime.writer.write_bounded(accepted, ACCEPTED_WRITE_SECONDS)
        split_http_jobs_accepted.inc()
        logger.info('split_http_job_accepted', category='split', request_id=request_id, job=accepted.job, client=client)
        return HttpAnswer(202, _json({'request_id': request_id, 'status': 'accepted'}), 'accepted')

    @staticmethod
    def _unavailable(request_id: str) -> HttpAnswer:
        """The answer to a request that could not be stored."""
        body = {'status': 'unavailable', 'error': STORE_FAILED_TEXT, 'request_id': request_id}
        return HttpAnswer(503, _json(body), 'unavailable')

    async def read(self, *, request_id: str, client: str) -> HttpAnswer:
        """Answer the status of a job; an unknown id first waits a little for its first record.

        An id of another client answers as an unknown one. An id that breaks
        the request id rule never waits.
        """
        self._check_loop()
        follower = self._runtime.follower
        record = follower.get(request_id)
        if (record is None or record.client != client) and _is_valid_request_id(request_id):
            record = await follower.wait_for(request_id, STATUS_WAIT_SECONDS)
        if record is None or record.client != client:
            split_http_status_reads.labels(status='not_found').inc()
            return HttpAnswer(404, _json({'error': 'not_found', 'request_id': request_id}), 'not_found')
        split_http_status_reads.labels(status=record.state).inc()
        return HttpAnswer(200, answer_body(record), 'status_read')
