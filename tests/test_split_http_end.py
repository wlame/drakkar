"""The end of an HTTP split job: its end-state status is acknowledged before ``closed`` and before its offset completes.

Every test drives a real partition processor with its real owner, a real job
tracker and a real producer over the fake Kafka clients. The status side of
the owner is real too (the writer over the same producer, a status map);
only the follower is a stand-in that says the map is restored.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.models import CollectResult, SourceMessage
from drakkar.providers.resilience import RateLimitedLog
from drakkar.split.http_jobs import HttpOwnerSide, StatusWriter, encode_result, end_record
from drakkar.split.http_message import read_http_job, source_message_record
from drakkar.split.http_status import StatusMap, StatusRecord, parse_status_record, status_record
from drakkar.split.models import JobAction, PartDLQ, PartResult, SplitJob
from drakkar.split.records import parse_log_record, result_body
from drakkar.split.tracker import TIMEOUT_CHECK_SECONDS
from tests.split_helpers import FakeMessage
from tests.split_owner_helpers import (
    PARTITION,
    REQUEST_ID,
    RESULTS_TOPIC,
    Item,
    OwnerWorker,
    SplittingHandler,
    World,
    items,
    owner_config,
    part_sink_result,
    source_message,
    summary,
    wait_until,
)

STATUS_TOPIC = 'requests_status'
HTTP_ID = 'req-of-the-http-job'
CLIENT = 'billing-client'
NOT_JSON = b'not json'
FIRST = '1' * 16


@pytest.fixture(autouse=True)
def fast_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the wait of a coalesced commit: these tests wait for the commit of one offset."""
    monkeypatch.setattr(partition_module, 'COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)


def http_message(offset: int = 500, *, value: bytes = b'{"n": 5}', deliver_sinks: bool = True) -> SourceMessage:
    """A message of the source topic that an HTTP split job request stored."""
    message = source_message(offset, value=value)
    record = source_message_record(request_id=HTTP_ID, client=CLIENT, deliver_sinks=deliver_sinks, body=b'')
    message.headers = [*message.headers, *record.headers]
    return message


def is_end_status(item: Any) -> bool:
    """Whether a produced status record carries an end state."""
    parsed = parse_status_record(item.key, item.headers, item.value)
    return parsed is not None and parsed.is_ended


class RestoredFollower:
    """A follower whose map is restored: the owner reads the map through it."""

    def __init__(self, status_map: StatusMap) -> None:
        self.status_map = status_map
        self.is_restored = True

    async def wait_restored(self, stop: asyncio.Event) -> bool:
        return True

    async def caught_up(self, timeout: float) -> bool:
        return True

    def get(self, request_id: str) -> StatusRecord | None:
        return self.status_map.get(request_id)


class HttpWorker:
    """An ``OwnerWorker`` whose owner writes the status of HTTP jobs, with every write and completion in order.

    ``events`` holds ``('status', state)`` when a status record is written,
    ``('results', kind)`` when a record of the results log is written, and
    ``('complete', offset)`` when the processor completes an offset.
    ``status_errors`` are the delivery errors of the next end-state status
    records; a ``running`` record is always delivered.
    While ``hold_status`` is set, the delivery of every end-state record waits
    in ``held_status`` (``('status_held', None)`` in ``events``) until
    :meth:`release_status`.
    """

    def __init__(
        self, world: World, handler: Any, *, max_response: int = 65536, hold_status: bool = False, **options: Any
    ) -> None:
        self.world = world
        self.worker = OwnerWorker(world, handler, http_jobs=True, **options)
        self.events: list[tuple[str, Any]] = []
        self.status_errors: deque[KafkaError] = deque()
        self.hold_status = hold_status
        self.held_status: list[Any] = []
        self.status_map = StatusMap(86400)
        writer = StatusWriter(
            self.worker.runtime.producer,
            STATUS_TOPIC,
            clock=world.seconds,
            sleep=self.worker.sleep,
            log_limit=RateLimitedLog(time.monotonic),
        )
        self.side = HttpOwnerSide(
            writer=writer,
            status_map=self.status_map,
            follower=RestoredFollower(self.status_map),  # ty: ignore[invalid-argument-type]
            max_response=max_response,
            clock=world.seconds,
        )
        self.worker.runtime._owner_status_side = lambda: self.side  # ty: ignore[invalid-assignment]
        self._wrap_deliveries()
        self._wrap_completions()

    def _wrap_deliveries(self) -> None:
        client = self.worker.client
        deliver = client._deliver

        def recorded(item: Any) -> None:
            is_end = item.topic == STATUS_TOPIC and is_end_status(item)
            if is_end and self.hold_status:
                self.events.append(('status_held', None))
                self.held_status.append(item)
                return
            if is_end and self.status_errors:
                self.events.append(('status_refused', None))
                item.on_delivery(self.status_errors.popleft(), FakeMessage(topic=item.topic, partition=item.partition))
                return
            deliver(item)
            if item.topic == STATUS_TOPIC:
                parsed = parse_status_record(item.key, item.headers, item.value)
                self.events.append(('status', None if parsed is None else parsed.state))
            elif item.topic == RESULTS_TOPIC:
                entry = parse_log_record(item.headers, item.value)
                self.events.append(('results', None if entry is None else entry.kind))

        client._deliver = recorded  # ty: ignore[invalid-assignment]
        self._deliver_recorded = recorded

    def release_status(self) -> None:
        """Deliver the held status records, in order, and stop holding new ones."""
        self.hold_status = False
        held, self.held_status = self.held_status, []
        for item in held:
            self._deliver_recorded(item)

    def _wrap_completions(self) -> None:
        tracker = self.worker.processor.offset_tracker
        complete = tracker.complete

        def recorded(offset: int) -> Any:
            self.events.append(('complete', offset))
            return complete(offset)

        tracker.complete = recorded  # ty: ignore[invalid-assignment]

    async def start(self, *messages: SourceMessage) -> HttpWorker:
        await self.worker.start(*messages)
        return self

    def statuses(self, *, running: bool = False) -> list[StatusRecord]:
        """The status records in the status topic, parsed, in order; ``running`` records only when asked."""
        found = []
        for record in self.world.log.records(STATUS_TOPIC, 0):
            parsed = parse_status_record(record.key, record.headers, record.value)
            assert parsed is not None
            if running or parsed.state != 'running':
                found.append(parsed)
        return found

    def status_bodies(self) -> list[dict[str, Any]]:
        """The JSON bodies of the status records that are not ``running``, in order."""
        bodies = [json.loads(record.value) for record in self.world.log.records(STATUS_TOPIC, 0)]
        return [body for body in bodies if body['state'] != 'running']

    def index(self, event: tuple[str, Any]) -> int:
        return self.events.index(event)


def complete_job(world: World, offset: int = 500) -> None:
    """A restored job of one part with its result: it ends as soon as an owner holds its message."""
    world.put_open(offset, FIRST)
    world.put(world.ref(offset, FIRST), 'split-done', {'parts': 1})
    world.put(world.ref(offset, FIRST), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))


class ParsingHandler(SplittingHandler):
    input_model = Item


class ArrangeFails(SplittingHandler):
    async def arrange(self, messages, pending):
        raise ValueError('cannot arrange')


async def timed_out_job(world: World, worker: OwnerWorker) -> None:
    """Move the clock past the timeout and run the timeout check of the tracker."""
    await worker.caught_up()
    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)


# --- every path that ends an HTTP job ---


async def _start_path(path: str, *, hold_status: bool = False) -> tuple[HttpWorker, str | None]:
    """Start one end path up to its last wait; the worker, and the ``closed`` outcome it writes (None for none)."""
    world = World()
    if path in ('aggregated', 'aggregate_dlq', 'aggregate_raised', 'job_action_dlq'):
        answers = {
            'aggregated': summary(),
            'aggregate_dlq': summary(),
            'aggregate_raised': ValueError('bad sum'),
            'job_action_dlq': JobAction.DLQ,
        }
        handler = SplittingHandler({500: items(2)}, answer=answers[path])
        http = HttpWorker(world, handler, aggregate_sink_fails=path == 'aggregate_dlq', hold_status=hold_status)
        await http.start(http_message())
        await wait_until(lambda: world.kinds() == ['open', 'split-done'])
        world.report_all()
        return http, 'aggregated' if path == 'aggregated' else 'dlq'
    if path == 'timeout':
        handler = SplittingHandler({500: items(3)})
        http = HttpWorker(world, handler, config=owner_config(job_timeout='1m'), hold_status=hold_status)
        await http.start(http_message())
        await wait_until(lambda: world.kinds() == ['open', 'split-done'])
        await timed_out_job(world, http.worker)
        return http, 'timeout'
    if path == 'failed_split':
        http = HttpWorker(world, SplittingHandler({500: [RuntimeError('boom')]}), hold_status=hold_status)
        await http.start(http_message())
        return http, 'split_failed'
    if path == 'spent_attempts':
        world.put_open(500, FIRST)
        http = HttpWorker(
            world, SplittingHandler({500: items(1)}), config=owner_config(max_attempts=1), hold_status=hold_status
        )
        await http.start(http_message())
        return http, 'attempts_spent'
    if path == 'orphan':
        complete_job(world)
        http = HttpWorker(world, SplittingHandler(answer=summary()), hold_status=hold_status)
        put_known(http, 500)
        await http.start(source_message(502))
        return http, 'orphaned'
    if path == 'local_done':
        http = await HttpWorker(world, SplittingHandler(answer=summary()), hold_status=hold_status).start(
            http_message()
        )
        return http, None
    if path == 'local_dlq':
        http = await HttpWorker(world, ArrangeFails(), hold_status=hold_status).start(http_message())
        return http, None
    policy = 'dlq' if path == 'parse_dlq' else 'skip'
    http = HttpWorker(
        world, ParsingHandler({500: items(1)}), config=owner_config(on_parse_error=policy), hold_status=hold_status
    )
    await http.start(http_message(value=NOT_JSON))
    return http, None


def _path_ended(http: HttpWorker, path: str) -> bool:
    """Whether a path started by :func:`_start_path` has ended: its offset committed (and, for an orphan, closed)."""
    if path == 'orphan':
        # The event, not the log: the delivery thread appends the record to the log before it notes the event.
        return http.worker.committed == 503 and ('results', 'closed') in http.events
    return http.worker.committed == 501


async def _run_path(path: str) -> tuple[HttpWorker, str | None]:
    """Run one end path to its end; the worker, and the ``closed`` outcome the path writes (None for none)."""
    http, closed = await _start_path(path)
    await wait_until(lambda: _path_ended(http, path))
    return http, closed


def put_known(http: HttpWorker, offset: int) -> None:
    """Put the job of ``offset`` into the status map, as the follower would after its ``running`` record."""
    now = http.world.seconds()
    record = StatusRecord(
        request_id=REQUEST_ID, state='running', partition=PARTITION, offset=offset, client=CLIENT, updated_at=now
    )
    http.status_map.put(record, now=now)


END_PATHS = {
    'aggregated': ('done', None),
    'aggregate_dlq': ('failed', 'dlq'),
    'aggregate_raised': ('failed', 'dlq'),
    'job_action_dlq': ('failed', 'dlq'),
    'timeout': ('failed', 'timeout'),
    'failed_split': ('failed', 'split_failed'),
    'spent_attempts': ('failed', 'attempts_spent'),
    'orphan': ('failed', 'orphaned'),
    'local_done': ('done', None),
    'local_dlq': ('failed', 'dlq'),
    'parse_dlq': ('failed', 'parse'),
    'parse_skip': ('failed', 'parse'),
}


@pytest.mark.parametrize('path', list(END_PATHS))
async def test_every_end_path_acknowledges_status_before_closed_or_completion(path):
    http, closed = await _run_path(path)
    state, _ = END_PATHS[path]
    status_at = http.index(('status', state))

    if closed is not None:
        assert http.world.closed()[0]['outcome'] == closed
        assert status_at < http.index(('results', 'closed'))
    if path != 'orphan':
        assert status_at < http.index(('complete', 500))
    else:
        # The orphan's message was passed; this worker never completes its offset.
        assert ('complete', 500) not in http.events
    await http.worker.stop()


@pytest.mark.parametrize('path', ['aggregated', 'local_done', 'failed_split', 'parse_dlq', 'parse_skip'])
async def test_held_status_delivery_holds_closed_and_completion(path):
    http, closed = await _start_path(path, hold_status=True)
    # The positive event first: the end-state record reached the producer and its delivery is held.
    await wait_until(lambda: ('status_held', None) in http.events)
    # Then many turns of the loop, with no wall-clock wait: every task that could go on has run.
    for _ in range(100):
        await asyncio.sleep(0)

    assert 'closed' not in http.world.kinds()
    assert not [event for event in http.events if event[0] == 'complete']

    http.release_status()
    await wait_until(lambda: _path_ended(http, path))

    released_at = http.index(('status', 'failed' if path not in ('aggregated', 'local_done') else 'done'))
    if closed is not None:
        assert released_at < http.index(('results', 'closed'))
    assert released_at < http.index(('complete', 500))
    await http.worker.stop()


@pytest.mark.parametrize('path', [path for path, (state, _) in END_PATHS.items() if state == 'failed'])
async def test_failed_outcomes_map(path):
    http, _ = await _run_path(path)
    [status] = [record for record in http.statuses() if record.is_ended]
    assert (status.state, status.outcome) == END_PATHS[path]
    assert status.request_id in (HTTP_ID, REQUEST_ID)
    assert status.client == CLIENT
    assert status.error is not None
    await http.worker.stop()


@pytest.mark.parametrize(
    ('path', 'error'),
    [
        ('aggregate_raised', 'the job failed in stage aggregate'),
        ('aggregate_dlq', 'an aggregate sink failed'),
        ('job_action_dlq', 'the job failed in stage aggregate'),
        ('timeout', 'job timed out after 1m1s: 0 of 3 parts'),
        ('failed_split', 'split failed: boom'),
        ('spent_attempts', 'split did not finish in 1 attempts'),
        ('local_dlq', 'the message failed in stage aggregate'),
    ],
)
async def test_failed_error_names_where_the_job_failed(path, error):
    http, _ = await _run_path(path)
    [status] = [record for record in http.statuses() if record.is_ended]
    assert status.error == error
    await http.worker.stop()


@pytest.mark.parametrize(
    ('dlq', 'error'),
    [
        (
            PartDLQ(stage='execute', task_id='t-1', error='exit code 1: SECRET task stderr'),
            'a part failed in stage execute',
        ),
        (
            PartDLQ(stage='sink', sink='kafka/results', error='SECRET payload of the task'),
            'a part failed in stage sink at sink kafka/results',
        ),
    ],
    ids=['execute', 'sink'],
)
async def test_failed_dlq_status_holds_no_task_output_and_the_dlq_record_keeps_it(dlq, error):
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=JobAction.DLQ)
    http = await HttpWorker(world, handler).start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all(outcome='failed', dlq=dlq)
    await wait_until(lambda: http.worker.committed == 501)

    [body] = [body for body in http.status_bodies() if body['state'] == 'failed']
    assert (body['outcome'], body['error']) == ('dlq', error)
    assert 'SECRET' not in json.dumps(body)
    [record] = http.worker.dlq_records()
    assert record.error == dlq.error
    await http.worker.stop()


async def test_failed_error_is_cut_to_2000_bytes():
    world = World()
    handler = ResultHandler({500: items(1)}, answer=summary(), result=ValueError('x' * 5000))
    http = await HttpWorker(world, handler).start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    [body] = [body for body in http.status_bodies() if body['state'] == 'failed']
    assert body['outcome'] == 'response_failed'
    assert len(body['error'].encode('utf-8')) == 2000
    await http.worker.stop()


# --- the normal end ---


class ResultHandler(SplittingHandler):
    """Answers ``http_job_result`` with ``result``: a value, an exception to raise, or a callable of the job."""

    def __init__(self, *args: Any, result: Any = None, events: list | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.result = result
        self.events = events
        self.result_jobs: list[SplitJob] = []

    async def aggregate(self, job: SplitJob) -> Any:
        if self.events is not None:
            self.events.append(('hook', 'aggregate'))
        return await super().aggregate(job)

    async def http_job_result(self, job: SplitJob) -> Any:
        self.result_jobs.append(job)
        if self.events is not None:
            self.events.append(('hook', 'http_job_result'))
        result = self.result
        if isinstance(result, BaseException):
            raise result
        return result(job) if callable(result) else result


async def test_normal_end_order():
    world = World()
    handler = ResultHandler({500: items(2)}, answer=summary(), result={'ok': True})
    http = HttpWorker(world, handler)
    handler.events = http.events
    await http.start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    order = [event for event in http.events if event[0] in ('hook', 'complete') or event == ('results', 'closed')]
    expected = [('hook', 'aggregate'), ('hook', 'http_job_result'), ('results', 'closed'), ('complete', 500)]
    assert order == expected
    assert http.index(('hook', 'http_job_result')) < http.index(('status', 'done')) < http.index(('results', 'closed'))
    assert http.worker.aggregate_sink.calls == 1
    await http.worker.stop()


async def test_done_record_shape():
    world = World()
    handler = ResultHandler({500: items(2)}, answer=summary(), result=Item(n=7))
    http = await HttpWorker(world, handler).start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.clock.advance(3000)
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    [body] = http.status_bodies()
    assert body['state'] == 'done'
    assert (body['job'], body['client'], body['result']) == ('3:500', CLIENT, {'n': 7})
    assert body['parts'] == {'total': 2, 'received': 2, 'failed': 0}
    assert body['duration_seconds'] == 3.0
    [job] = handler.result_jobs
    assert job.source_message.http_job is not None and job.source_message.http_job.request_id == HTTP_ID
    await http.worker.stop()


async def test_done_record_of_a_local_message_has_no_parts():
    world = World()
    handler = ResultHandler(answer=summary(), result=['a', 1])
    http = await HttpWorker(world, handler).start(http_message())
    await wait_until(lambda: http.worker.committed == 501)

    [body] = http.status_bodies()
    assert (body['state'], body['result']) == ('done', ['a', 1])
    assert 'parts' not in body and 'duration_seconds' in body
    assert world.kinds() == []
    await http.worker.stop()


@pytest.mark.parametrize(
    ('result', 'error_start'),
    [
        (RuntimeError('no result'), 'no result'),
        ({1, 2}, 'the result is not JSON: '),
        ('x' * (300 * 1024), f'the result is {300 * 1024 + 2} bytes, the limit is {256 * 1024}'),
    ],
    ids=['raises', 'not_json', 'too_large'],
)
async def test_response_failed_cases(result, error_start):
    world = World()
    handler = ResultHandler({500: items(1)}, answer=summary(), result=result)
    http = await HttpWorker(world, handler, max_response=256 * 1024).start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: http.worker.committed == 501)

    [status] = [record for record in http.statuses() if record.is_ended]
    assert (status.state, status.outcome) == ('failed', 'response_failed')
    assert status.error is not None and status.error.startswith(error_start)
    assert world.closed()[0]['outcome'] == 'aggregated'
    assert http.worker.dlq_records() == []
    [line] = [line for line in logs if line['event'] == 'http_job_result_failed']
    assert (line['log_level'], line['category'], line['job']) == ('error', 'handler', '3:500#1')
    assert line['error'] == status.error
    assert len(handler.result_jobs) == 1
    await http.worker.stop()


def test_encode_result_cases():
    assert encode_result(Item(n=1), 100) == ('{"n":1}', None)
    assert encode_result({'é': float('1.5')}, 100) == ('{"é":1.5}', None)
    assert encode_result(None, 100) == ('null', None)
    text, error = encode_result(float('nan'), 100)
    assert text is None and error is not None and error.startswith('the result is not JSON: ')
    # The size counts UTF-8 bytes: "é" is 2 bytes, and the quotes make 4.
    assert encode_result('é', 3) == (None, 'the result is 4 bytes, the limit is 3')
    assert encode_result('é', 4) == ('"é"', None)


# --- writes that fail ---


async def test_status_write_retried_with_backoff_then_acknowledged():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(1)}, answer=JobAction.DLQ))
    http.status_errors.extend([KafkaError(KafkaError._MSG_TIMED_OUT)] * 2)
    await http.start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()

    await http.worker.sleep.fire(1.0)
    await http.worker.sleep.fire(2.0)
    await wait_until(lambda: http.worker.committed == 501)

    assert [event for event in http.events if event[0].startswith('status') and event[1] != 'running'] == [
        ('status_refused', None),
        ('status_refused', None),
        ('status', 'failed'),
    ]
    assert http.index(('status', 'failed')) < http.index(('results', 'closed'))
    await http.worker.stop()


async def test_stop_during_status_retries_leaves_offset_held():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(1)}, answer=summary()))
    http.status_errors.append(KafkaError(KafkaError._MSG_TIMED_OUT))
    await http.start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.sleep.waiting(1.0))

    http.worker.processor.signal_stop()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)

    assert 'closed' not in world.kinds() and http.statuses() == []
    assert http.worker.pending == 1 and http.worker.commits == []
    assert ('complete', 500) not in http.events
    assert http.worker.processor._abandoned_offsets == {500}
    await http.worker.stop()


async def test_stop_during_status_retries_of_an_unparsed_message_leaves_offset_held():
    world = World()
    http = HttpWorker(world, ParsingHandler(), config=owner_config(on_parse_error='skip'))
    http.status_errors.append(KafkaError(KafkaError._MSG_TIMED_OUT))
    await http.start(http_message(value=NOT_JSON))
    await wait_until(lambda: http.worker.sleep.waiting(1.0))

    http.worker.processor.signal_stop()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)

    assert http.statuses() == [] and http.worker.commits == []
    assert http.worker.pending == 1
    assert http.worker.processor._open_job_offsets == {500}
    await http.worker.stop()


async def test_unparsed_message_after_a_stop_is_left_pending_with_no_end():
    world = World()
    http = await HttpWorker(world, ParsingHandler(), config=owner_config(on_parse_error='dlq')).start()
    processor = http.worker.processor
    message = http_message(value=NOT_JSON)
    message.parse_error = 'not json'
    message.http_job = processor_http_job()
    processor.offset_tracker.register(500)
    processor.signal_stop()

    await processor._end_unparsed(message)

    assert processor._abandoned_offsets == {500}
    assert not processor._active_tasks
    assert http.statuses() == [] and ('complete', 500) not in http.events
    await http.worker.stop()


def processor_http_job() -> Any:
    return read_http_job(http_message().headers)


async def test_final_done_write_replaced_by_response_failed():
    world = World()
    http = HttpWorker(world, ResultHandler({500: items(1)}, answer=summary(), result={'big': 1}))
    http.status_errors.append(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
    await http.start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    [status] = http.statuses()
    assert (status.state, status.outcome) == ('failed', 'response_failed')
    assert status.error is not None and status.error.startswith('the result could not be stored: ')
    assert world.closed()[0]['outcome'] == 'aggregated'
    assert http.index(('status', 'failed')) < http.index(('results', 'closed')) < http.index(('complete', 500))
    await http.worker.stop()


async def test_final_failed_write_stalls():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(1)}, answer=JobAction.DLQ))
    http.status_errors.append(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    await http.start(http_message())
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.stalls == [PARTITION])

    assert http.statuses() == [] and 'closed' not in world.kinds()
    assert http.worker.commits == []
    await http.worker.stop()


async def test_final_failed_write_of_a_failed_split_stalls():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: [RuntimeError('boom')]}))
    http.status_errors.append(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    await http.start(http_message())
    await wait_until(lambda: http.worker.stalls == [PARTITION])

    assert http.statuses() == [] and world.closed() == [] and http.worker.commits == []
    await http.worker.stop()


async def test_stop_during_status_retries_of_a_failed_split_leaves_offset_held():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: [RuntimeError('boom')]}))
    http.status_errors.append(KafkaError(KafkaError._MSG_TIMED_OUT))
    await http.start(http_message())
    await wait_until(lambda: http.worker.sleep.waiting(1.0))

    http.worker.processor.signal_stop()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)

    assert world.closed() == [] and http.worker.commits == [] and http.worker.pending == 1
    assert http.worker.processor._open_job_offsets == {500}
    await http.worker.stop()


async def test_second_end_after_crash_between_done_and_closed():
    world = World()
    complete_job(world)
    # The first owner wrote ``done`` and crashed before ``closed``.
    first_done = StatusRecord(
        request_id=HTTP_ID,
        state='done',
        partition=PARTITION,
        offset=500,
        client=CLIENT,
        updated_at=1.0,
        result_json='1',
    )
    record = status_record(first_done)
    world.log.append(STATUS_TOPIC, 0, key=record.key, value=record.value, headers=record.headers)
    handler = ResultHandler(answer=summary(), result=2)
    http = HttpWorker(world, handler)
    http.status_map.put(first_done, now=world.seconds())

    await http.start(http_message())
    await wait_until(lambda: http.worker.committed == 501)

    assert len(handler.jobs) == 1 and len(handler.result_jobs) == 1
    assert [(status.state, status.result_json) for status in http.statuses()] == [('done', '1'), ('done', '2')]
    assert http.index(('status', 'done')) < http.index(('results', 'closed'))
    await http.worker.stop()


# --- orphans ---


async def test_orphan_without_map_entry_writes_no_status():
    world = World()
    complete_job(world)
    http = await HttpWorker(world, SplittingHandler(answer=summary())).start(source_message(502))
    await wait_until(lambda: 'closed' in world.kinds())

    assert world.closed()[0]['outcome'] == 'orphaned'
    assert http.statuses(running=True) == []
    await http.worker.stop()


async def test_orphan_of_another_job_in_the_map_writes_no_status():
    world = World()
    complete_job(world)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    put_known(http, 400)
    await http.start(source_message(502))
    await wait_until(lambda: 'closed' in world.kinds())

    assert http.statuses(running=True) == []
    await http.worker.stop()


async def test_orphan_status_not_acknowledged_writes_no_closed():
    world = World()
    complete_job(world)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    put_known(http, 500)
    http.status_errors.append(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    await http.start(source_message(502))
    await wait_until(lambda: ('status_refused', None) in http.events)
    await wait_until(lambda: http.worker.committed == 503)
    # The stop waits for the tasks of the owner, so a closed record would be written by now.
    await http.worker.stop()

    assert 'closed' not in world.kinds() and http.statuses(running=True) == []


async def test_orphan_keeps_a_stored_end_status_and_writes_only_closed():
    world = World()
    complete_job(world)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    done = StatusRecord(
        request_id=REQUEST_ID,
        state='done',
        partition=PARTITION,
        offset=500,
        client=CLIENT,
        updated_at=world.seconds(),
        result_json='7',
    )
    http.status_map.put(done, now=world.seconds())
    await http.start(source_message(502))
    await wait_until(lambda: 'closed' in world.kinds())

    assert world.closed()[0]['outcome'] == 'orphaned'
    assert http.statuses(running=True) == []
    assert http.status_map.get(REQUEST_ID) == done
    await http.worker.stop()


async def test_orphan_status_carries_the_client_and_the_parts_of_the_job():
    world = World()
    complete_job(world)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    put_known(http, 500)
    await http.start(source_message(502))
    await wait_until(lambda: 'closed' in world.kinds())

    [body] = http.status_bodies()
    assert (body['state'], body['outcome'], body['client'], body['job']) == ('failed', 'orphaned', CLIENT, '3:500')
    assert body['parts'] == {'total': 1, 'received': 1, 'failed': 0}
    await http.worker.stop()


# --- sinks off ---


class TimeoutAnswers(SplittingHandler):
    async def on_job_timeout(self, job: SplitJob) -> CollectResult:
        self.timeouts.append(job)
        return summary(4)


@pytest.mark.parametrize('sinks_on', [True, False])
async def test_aggregate_result_dropped_when_sinks_off(sinks_on):
    world = World()
    http = await HttpWorker(world, SplittingHandler({500: items(1)}, answer=summary())).start(
        http_message(deliver_sinks=sinks_on)
    )
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: http.worker.committed == 501)

    assert http.worker.aggregate_sink.calls == (1 if sinks_on else 0)
    dropped = [line for line in logs if line['event'] == 'split_http_delivery_dropped']
    assert len(dropped) == (0 if sinks_on else 1)
    assert world.closed()[0]['outcome'] == 'aggregated'
    assert http.statuses()[-1].state == 'done'
    await http.worker.stop()


@pytest.mark.parametrize('sinks_on', [True, False])
async def test_on_job_timeout_result_dropped_when_sinks_off(sinks_on):
    world = World()
    http = HttpWorker(world, TimeoutAnswers({500: items(2)}), config=owner_config(job_timeout='1m'))
    await http.start(http_message(deliver_sinks=sinks_on))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await timed_out_job(world, http.worker)
    await wait_until(lambda: http.worker.committed == 501)

    assert http.worker.aggregate_sink.calls == (1 if sinks_on else 0)
    assert world.closed()[0]['outcome'] == 'timeout'
    [status] = http.statuses()
    assert (status.state, status.outcome) == ('failed', 'timeout')
    await http.worker.stop()


async def test_aggregate_stage_error_still_goes_to_the_dlq_when_sinks_off():
    world = World()
    http = await HttpWorker(world, SplittingHandler({500: items(1)}, answer=part_sink_result())).start(
        http_message(deliver_sinks=False)
    )
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    assert len(http.worker.dlq_records()) == 1
    assert world.closed()[0]['outcome'] == 'dlq'
    assert http.statuses()[-1].outcome == 'dlq'
    await http.worker.stop()


# --- a job of the source topic ---


async def test_kafka_job_end_unchanged():
    world = World()
    handler = ResultHandler({500: items(2)}, answer=summary(), result={'never': True})
    http = await HttpWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    assert world.kinds() == ['open', 'split-done', 'result', 'result', 'closed']
    assert world.closed()[0]['outcome'] == 'aggregated'
    assert http.statuses(running=True) == []
    assert handler.result_jobs == []
    assert http.worker.aggregate_sink.calls == 1
    await http.worker.stop()


async def test_kafka_job_writes_no_running_record_while_its_parts_report():
    world = World()
    http = await HttpWorker(world, SplittingHandler({500: items(3)}, answer=summary())).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    parts = world.parts()
    for part in parts:
        world.clock.advance(6000)
        world.report(part)
        await http.worker.caught_up()
    await wait_until(lambda: http.worker.committed == 501)

    assert http.statuses(running=True) == []
    await http.worker.stop()


# --- edges of the owner and the runtime ---


async def test_orphan_waits_for_the_restored_map_and_a_stop_ends_the_wait_with_no_closed():
    world = World()
    complete_job(world)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    put_known(http, 500)
    follower = http.side.follower
    waits: list[bool] = []

    async def not_restored(stop: asyncio.Event) -> bool:
        waits.append(True)
        await stop.wait()
        return False

    follower.wait_restored = not_restored  # ty: ignore[invalid-assignment]
    await http.start(source_message(502))
    await wait_until(lambda: waits == [True] and http.worker.committed == 503)
    await http.worker.stop()

    assert 'closed' not in world.kinds() and http.statuses(running=True) == []


async def test_fail_unparsed_after_the_owner_stopped_holds_the_offset_and_starts_no_end():
    world = World()
    http = await HttpWorker(world, ParsingHandler()).start()
    owner = http.worker.owner
    assert owner is not None
    message = http_message(value=NOT_JSON)
    message.http_job = read_http_job(message.headers)
    owner.signal_stop()

    owner.fail_unparsed(message)

    assert http.worker.processor._open_job_offsets == {500}
    assert not http.worker.processor._active_tasks
    await http.worker.stop()


async def test_end_unparsed_of_a_message_that_is_no_http_job_writes_nothing():
    world = World()
    http = await HttpWorker(world, ParsingHandler()).start()
    owner = http.worker.owner
    assert owner is not None

    assert await owner.end_unparsed(source_message(500)) is True
    assert http.statuses() == []
    await http.worker.stop()


async def test_owners_of_a_runtime_with_http_jobs_get_its_status_side():
    world = World()
    worker = OwnerWorker(world, SplittingHandler())
    assert worker.runtime._owner_status_side() is None
    status_map = StatusMap(60)
    worker.runtime.http = SimpleNamespace(  # ty: ignore[invalid-assignment]
        writer='writer', status_map=status_map, follower='follower', max_response=1024
    )

    side = worker.runtime._owner_status_side()

    assert side is not None
    assert (side.writer, side.status_map, side.follower, side.max_response) == ('writer', status_map, 'follower', 1024)
    assert side.clock() == world.seconds()


def test_end_record_needs_an_http_job_message():
    with pytest.raises(ValueError, match='HTTP job'):
        end_record(source_message(500), state='failed', parts=None, duration=None, now=1.0)


async def test_write_status_needs_the_status_side():
    world = World()
    worker = await OwnerWorker(world, SplittingHandler()).start()
    owner = worker.owner
    assert owner is not None
    record = StatusRecord(
        request_id=HTTP_ID, state='failed', partition=PARTITION, offset=500, client='', updated_at=1.0
    )

    with pytest.raises(RuntimeError, match='status side'):
        await owner._ender.write_status(record)
    await worker.stop()
