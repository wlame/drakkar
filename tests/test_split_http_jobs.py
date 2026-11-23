"""The status writer and the gateway of HTTP split jobs."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.providers.resilience import RateLimitedLog
from drakkar.split.http_follower import StatusFollower
from drakkar.split.http_jobs import (
    ACCEPTED_WRITE_SECONDS,
    MAX_STATUS_WAITERS,
    STATUS_WAIT_SECONDS,
    HttpJobGateway,
    StatusWriter,
)
from drakkar.split.http_message import read_http_job
from drakkar.split.http_status import PartCounts, StatusMap, StatusRecord, answer_body, parse_status_record
from drakkar.split.producer import SplitProduceError
from drakkar.split.records import WireRecord
from tests.test_split_http_follower import SOURCE_TOPIC, STATUS_TOPIC, RuntimeParts, runtime_config

NOW = 1_800_000_000.0
CLIENT = 'tenant-a'
OTHER = 'tenant-b'


class FakeProducer:
    """A split producer stand-in: each write takes the next scripted outcome.

    An outcome is an int offset, ``(partition, offset)``, an exception, or
    ``'hang'`` for a delivery that never comes. Without a script every write
    lands on partition 7 at the next offset. ``writes`` lists ``(method, topic, record)``.
    """

    def __init__(self, *outcomes: Any, nowait_full: bool = False) -> None:
        self.outcomes = list(outcomes)
        self.writes: list[tuple[str, str, WireRecord]] = []
        self.nowait_full = nowait_full
        self.next_offset = 500

    def _outcome(self) -> Any:
        if self.outcomes:
            return self.outcomes.pop(0)
        self.next_offset += 1
        return (7, self.next_offset - 1)

    async def _deliver(self, outcome: Any) -> Any:
        if outcome == 'hang':
            await asyncio.Event().wait()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def publish(self, topic: str, record: WireRecord) -> int:
        self.writes.append(('publish', topic, record))
        outcome = await self._deliver(self._outcome())
        return outcome[1] if isinstance(outcome, tuple) else outcome

    async def publish_located(self, topic: str, record: WireRecord) -> tuple[int, int]:
        self.writes.append(('publish_located', topic, record))
        return await self._deliver(self._outcome())

    def enqueue_nowait(self, topic: str, record: WireRecord) -> asyncio.Future[int] | None:
        if self.nowait_full:
            return None
        self.writes.append(('enqueue_nowait', topic, record))
        outcome = self._outcome()
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        if isinstance(outcome, BaseException):
            future.set_exception(outcome)
        elif outcome != 'hang':
            future.set_result(outcome[1] if isinstance(outcome, tuple) else outcome)
        return future


async def no_sleep(seconds: float) -> None:
    """A sleep that returns after one turn of the loop."""
    await asyncio.sleep(0)


class Gate:
    """The gateway over a hand-filled status map, a fake source producer and a fake status producer."""

    def __init__(self, *, source: FakeProducer | None = None, status: FakeProducer | None = None, sleep=no_sleep):
        self.map = StatusMap(3600)
        self.follower = StatusFollower(
            connection=KafkaConnectionConfig(brokers='broker:9092'),
            topic=STATUS_TOPIC,
            group='g',
            status_map=self.map,
            clock=lambda: NOW,
        )
        self.follower.is_restored = True
        self.source = source or FakeProducer()
        self.status = status or FakeProducer()
        self.sleeps: list[float] = []

        async def recorded_sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            await sleep(seconds)

        self.writer = StatusWriter(
            self.status, STATUS_TOPIC, clock=lambda: NOW, sleep=recorded_sleep, log_limit=RateLimitedLog(time.monotonic)
        )
        self.runtime = SimpleNamespace(
            follower=self.follower,
            source_producer=self.source,
            source_topic=SOURCE_TOPIC,
            writer=self.writer,
            loop=asyncio.get_running_loop(),
        )
        self.gateway = HttpJobGateway(self.runtime, clock=lambda: NOW)  # ty: ignore[invalid-argument-type]

    def put(self, record: StatusRecord) -> None:
        self.map.put(record, now=NOW)

    async def accept(self, request_id: str = 'req-1', *, client: str = CLIENT, timeout: float = 5.0) -> Any:
        return await self.gateway.accept(
            request_id=request_id, client=client, body=b'{"n":1}', deliver_sinks=True, timeout=timeout
        )


def record(state: str = 'running', *, request_id: str = 'req-1', client: str = CLIENT, **fields: Any) -> StatusRecord:
    return StatusRecord(
        request_id=request_id,
        state=state,  # ty: ignore[invalid-argument-type]
        partition=7,
        offset=500,
        client=client,
        updated_at=NOW - 1,
        **fields,
    )


def body(answer: Any) -> dict[str, Any]:
    return json.loads(answer.body)


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


# --- accept ---


async def test_accept_writes_source_message_then_accepted_and_answers_202():
    gate = Gate(source=FakeProducer((3, 41)))
    with capture_logs() as logs:
        answer = await gate.accept()

    assert (answer.status_code, answer.metric_status, body(answer)) == (
        202,
        'accepted',
        {'request_id': 'req-1', 'status': 'accepted'},
    )
    [(method, topic, message)] = gate.source.writes
    assert (method, topic, message.key, message.value) == ('publish_located', SOURCE_TOPIC, b'req-1', b'{"n":1}')
    job = read_http_job(message.headers)
    assert job is not None and (job.request_id, job.client, job.deliver_sinks) == ('req-1', CLIENT, True)
    [(method, topic, written)] = gate.status.writes
    assert (method, topic) == ('enqueue_nowait', STATUS_TOPIC)
    accepted = parse_status_record(written.key, written.headers, written.value)
    assert accepted is not None
    assert (accepted.state, accepted.job, accepted.client, accepted.updated_at) == ('accepted', '3:41', CLIENT, NOW)
    [line] = [line for line in logs if line['event'] == 'split_http_job_accepted']
    assert (line['request_id'], line['job'], line['client'], line['category']) == ('req-1', '3:41', CLIENT, 'split')


async def test_accept_known_same_client_answers_200_status_and_writes_nothing():
    gate = Gate()
    known = record('running', parts=PartCounts(total=4, received=1, failed=0))
    gate.put(known)

    answer = await gate.accept()

    assert (answer.status_code, answer.metric_status, answer.body) == (200, 'duplicate', answer_body(known))
    assert gate.source.writes == [] and gate.status.writes == []


async def test_accept_known_other_client_answers_409():
    gate = Gate()
    gate.put(record('accepted', client=OTHER))

    answer = await gate.accept()

    assert (answer.status_code, answer.metric_status, body(answer)) == (
        409,
        'conflict',
        {'error': 'request_id_in_use', 'request_id': 'req-1'},
    )
    assert gate.source.writes == []


async def test_accept_too_large_answers_413():
    gate = Gate(source=FakeProducer(SplitProduceError('too large', final=True, too_large=True)))

    answer = await gate.accept()

    assert (answer.status_code, answer.metric_status, body(answer)) == (
        413,
        'error',
        {
            'error': 'request_too_large',
            'request_id': 'req-1',
            'details': 'the request is larger than the broker accepts',
        },
    )
    assert gate.status.writes == []


UNAVAILABLE = {'status': 'unavailable', 'error': 'the request could not be stored', 'request_id': 'req-1'}


@pytest.mark.parametrize(
    'failure',
    [SplitProduceError('broker down', final=False), SplitProduceError('denied', final=True), RuntimeError('closed')],
)
async def test_accept_failure_answers_503(failure):
    gate = Gate(source=FakeProducer(failure))

    answer = await gate.accept()

    assert (answer.status_code, answer.metric_status, body(answer)) == (503, 'unavailable', UNAVAILABLE)
    assert gate.status.writes == []


async def test_accept_timeout_answers_503():
    gate = Gate(source=FakeProducer('hang'))

    answer = await gate.accept(timeout=0.01)

    assert (answer.status_code, answer.metric_status, body(answer)) == (503, 'unavailable', UNAVAILABLE)
    assert gate.status.writes == []


def store_failures(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [line for line in logs if line['event'] == 'split_http_store_failed']


@pytest.mark.parametrize(
    ('failure', 'error'),
    [
        (SplitProduceError('broker down', final=False), 'broker down'),
        (SplitProduceError('too large', final=True, too_large=True), 'too large'),
        ('hang', 'timed out'),
    ],
)
async def test_accept_logs_a_failed_source_write_at_most_once_per_interval(failure, error):
    gate = Gate(source=FakeProducer(failure, failure))
    with capture_logs() as logs:
        await gate.accept(timeout=0.01)
        await gate.accept('req-2', timeout=0.01)

    [line] = store_failures(logs)
    assert (line['log_level'], line['category'], line['request_id'], line['client']) == (
        'warning',
        'split',
        'req-1',
        CLIENT,
    )
    assert (line['error'], line['suppressed']) == (error, 0)


async def test_accept_with_no_offset_in_the_report_writes_no_accepted_record_and_answers_202():
    gate = Gate(source=FakeProducer((7, -1)))

    answer = await gate.accept()

    assert (answer.status_code, body(answer)) == (202, {'request_id': 'req-1', 'status': 'accepted'})
    assert gate.status.writes == []


async def test_write_bounded_reads_the_outcome_of_a_write_whose_caller_was_cancelled():
    delivery: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    class Pending(FakeProducer):
        def enqueue_nowait(self, topic: str, record: WireRecord) -> asyncio.Future[int] | None:
            return delivery

    gate = Gate(status=Pending(), sleep=lambda seconds: asyncio.Event().wait())
    writing = asyncio.ensure_future(gate.writer.write_bounded(record('accepted'), 5.0))
    await asyncio.sleep(0.01)
    writing.cancel()
    await asyncio.gather(writing, return_exceptions=True)

    with capture_logs() as logs:
        delivery.set_exception(SplitProduceError('broker down', final=False))
        await asyncio.sleep(0.01)

    [line] = [line for line in logs if line['event'] == 'split_http_status_write_failed']
    assert (line['state'], line['error']) == ('accepted', 'broker down')


async def test_accept_answers_202_when_accepted_write_fails():
    gate = Gate(status=FakeProducer(SplitProduceError('broker down', final=False)))
    before = sample('drakkar_split_http_status_write_failures_total')
    with capture_logs() as logs:
        answer = await gate.accept()

    assert answer.status_code == 202
    assert sample('drakkar_split_http_status_write_failures_total') == before + 1
    [line] = [line for line in logs if line['event'] == 'split_http_status_write_failed']
    assert (line['request_id'], line['state'], line['error'], line['suppressed']) == (
        'req-1',
        'accepted',
        'broker down',
        0,
    )


async def test_accept_answers_202_when_it_takes_longer_than_5_seconds():
    gate = Gate(status=FakeProducer('hang'))

    with capture_logs() as logs:
        answer = await asyncio.wait_for(gate.accept(), 2)

    assert answer.status_code == 202
    assert gate.sleeps == [ACCEPTED_WRITE_SECONDS]
    assert [line['state'] for line in logs if line['event'] == 'split_http_status_write_failed'] == ['accepted']


async def test_accept_drops_the_accepted_record_at_once_when_the_status_queue_is_full():
    gate = Gate(status=FakeProducer(nowait_full=True))
    with capture_logs() as logs:
        answer = await gate.accept()

    assert answer.status_code == 202
    assert gate.sleeps == [] and gate.status.writes == []
    [line] = [line for line in logs if line['event'] == 'split_http_status_write_failed']
    assert (line['state'], line['error']) == ('accepted', 'the client queue is full')


async def test_gateway_refuses_a_call_from_another_loop():
    gate = Gate()
    gate.runtime.loop = None

    with pytest.raises(RuntimeError, match='main loop'):
        await gate.accept()
    with pytest.raises(RuntimeError, match='main loop'):
        await gate.gateway.read(request_id='req-1', client=CLIENT)


async def test_gateway_is_ready_follows_the_restore():
    gate = Gate()
    assert gate.gateway.is_ready
    gate.follower.is_restored = False
    assert not gate.gateway.is_ready


# --- read ---


@pytest.mark.parametrize(
    ('stored', 'expected'),
    [
        (record('accepted'), {'request_id': 'req-1', 'status': 'accepted'}),
        (
            record('running', parts=PartCounts(total=None, received=2, failed=1)),
            {'request_id': 'req-1', 'status': 'running', 'parts': {'total': None, 'received': 2, 'failed': 1}},
        ),
        (
            record(
                'done', parts=PartCounts(total=3, received=3, failed=0), result_json='{"a":1}', duration_seconds=2.5
            ),
            {
                'request_id': 'req-1',
                'status': 'done',
                'parts': {'total': 3, 'received': 3, 'failed': 0},
                'duration_seconds': 2.5,
                'result': {'a': 1},
            },
        ),
        (
            record('failed', outcome='timeout', error='late'),
            {'request_id': 'req-1', 'status': 'failed', 'outcome': 'timeout', 'error': 'late'},
        ),
    ],
)
async def test_read_each_status(stored, expected):
    gate = Gate()
    gate.put(stored)

    answer = await gate.gateway.read(request_id='req-1', client=CLIENT)

    found = body(answer)
    assert found.pop('updated_at').endswith('Z')
    assert (answer.status_code, answer.metric_status, found) == (200, 'status_read', expected)


NOT_FOUND = {'error': 'not_found', 'request_id': 'req-1'}


async def test_read_other_client_is_404_after_the_wait_of_an_unknown_id():
    gate = Gate()
    gate.put(record('done', client=OTHER, result_json='1'))
    loop = asyncio.get_running_loop()
    started = loop.time()

    answer = await gate.gateway.read(request_id='req-1', client=CLIENT)

    assert (answer.status_code, answer.metric_status, body(answer)) == (404, 'not_found', NOT_FOUND)
    # The same wait as for an unknown id: the time of the answer does not show that the id exists.
    assert STATUS_WAIT_SECONDS - 0.1 <= loop.time() - started < STATUS_WAIT_SECONDS + 0.5


async def test_read_waits_up_to_1_second_for_unknown_id():
    gate = Gate()
    loop = asyncio.get_running_loop()
    started = loop.time()

    answer = await gate.gateway.read(request_id='req-1', client=CLIENT)

    assert (answer.status_code, body(answer)) == (404, NOT_FOUND)
    assert STATUS_WAIT_SECONDS - 0.1 <= loop.time() - started < STATUS_WAIT_SECONDS + 0.5


async def test_read_of_an_unknown_id_answers_the_record_that_arrives_during_the_wait():
    gate = Gate()
    reading = asyncio.ensure_future(gate.gateway.read(request_id='req-1', client=CLIENT))
    await asyncio.sleep(0.01)
    gate.put(record('accepted'))
    gate.follower._wake('req-1')

    answer = await asyncio.wait_for(reading, 0.5)

    assert (answer.status_code, body(answer)['status']) == (200, 'accepted')


@pytest.mark.parametrize('request_id', ['', 'has space', 'x' * 65, 'é'])
async def test_read_invalid_id_does_not_wait(request_id):
    gate = Gate()
    loop = asyncio.get_running_loop()
    started = loop.time()

    answer = await gate.gateway.read(request_id=request_id, client=CLIENT)

    assert answer.status_code == 404 and body(answer) == {'error': 'not_found', 'request_id': request_id}
    assert loop.time() - started < 0.2


def test_the_wait_bound_is_the_follower_bound():
    assert MAX_STATUS_WAITERS == 1000


# --- the writer ---


async def test_write_end_retries_until_acknowledged():
    failure = SplitProduceError('broker down', final=False)
    gate = Gate(status=FakeProducer(failure, failure, failure, 42))
    before = sample('drakkar_split_http_status_write_failures_total')

    written = await gate.writer.write_end(record('failed', outcome='dlq', error='e'), asyncio.Event())

    assert written == 42
    assert gate.sleeps == [1.0, 2.0, 4.0]
    assert len(gate.status.writes) == 4
    assert sample('drakkar_split_http_status_write_failures_total') == before + 3


async def test_write_end_backoff_stops_growing_at_30_seconds():
    failure = SplitProduceError('broker down', final=False)
    gate = Gate(status=FakeProducer(*[failure] * 7, 9))

    assert await gate.writer.write_end(record('done', result_json='1'), asyncio.Event()) == 9
    assert gate.sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


async def test_write_end_stop_returns_stopped():
    stopped = asyncio.Event()

    async def sleep_until_stopped(seconds: float) -> None:
        stopped.set()
        await asyncio.Event().wait()

    gate = Gate(status=FakeProducer(SplitProduceError('broker down', final=False)), sleep=sleep_until_stopped)

    assert await gate.writer.write_end(record('done', result_json='1'), stopped) == 'stopped'
    assert len(gate.status.writes) == 1


async def test_write_end_stop_during_a_hung_write_returns_stopped():
    stopped = asyncio.Event()
    gate = Gate(status=FakeProducer('hang'))
    writing = asyncio.ensure_future(gate.writer.write_end(record('done', result_json='1'), stopped))
    await asyncio.sleep(0.01)
    stopped.set()

    assert await asyncio.wait_for(writing, 2) == 'stopped'


async def test_write_end_cancelled_caller_is_cancelled():
    gate = Gate(status=FakeProducer('hang'))
    writing = asyncio.ensure_future(gate.writer.write_end(record('done', result_json='1'), asyncio.Event()))
    await asyncio.sleep(0.01)
    writing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await writing


@pytest.mark.parametrize(
    'failure',
    [
        SplitProduceError('denied', final=True),
        SplitProduceError('too large', final=False, too_large=True),
    ],
)
async def test_write_end_final_returns_final(failure):
    gate = Gate(status=FakeProducer(failure))
    with capture_logs() as logs:
        written = await gate.writer.write_end(record('done', result_json='1'), asyncio.Event())

    assert written == 'final'
    assert gate.sleeps == []
    assert [line['state'] for line in logs if line['event'] == 'split_http_status_write_failed'] == ['done']


async def test_write_best_effort_queues_without_waiting():
    gate = Gate(status=FakeProducer(3))
    before = sample('drakkar_split_http_status_write_failures_total')

    gate.writer.write_best_effort(record('running'))
    await asyncio.sleep(0)

    [(method, topic, written)] = gate.status.writes
    assert (method, topic) == ('enqueue_nowait', STATUS_TOPIC)
    parsed = parse_status_record(written.key, written.headers, written.value)
    assert parsed is not None and parsed.state == 'running'
    assert sample('drakkar_split_http_status_write_failures_total') == before


@pytest.mark.parametrize('case', ['full', 'refused', 'not_started', 'failed_delivery'])
async def test_write_best_effort_failure_is_counted_and_logged(case):
    if case == 'full':
        status = FakeProducer(nowait_full=True)
    elif case == 'refused':
        status = FakeProducer()
        status.enqueue_nowait = lambda topic, record: (_ for _ in ()).throw(SplitProduceError('bad', final=True))
    elif case == 'not_started':
        status = FakeProducer()
        status.enqueue_nowait = lambda topic, record: (_ for _ in ()).throw(RuntimeError('not started'))
    else:
        status = FakeProducer(SplitProduceError('broker down', final=False))
    gate = Gate(status=status)
    before = sample('drakkar_split_http_status_write_failures_total')

    with capture_logs() as logs:
        gate.writer.write_best_effort(record('running'))
        await asyncio.sleep(0)

    assert sample('drakkar_split_http_status_write_failures_total') == before + 1
    [line] = [line for line in logs if line['event'] == 'split_http_status_write_failed']
    assert (line['request_id'], line['state'], line['category']) == ('req-1', 'running', 'split')


async def test_write_best_effort_cancelled_delivery_is_counted():
    loop = asyncio.get_running_loop()
    future: asyncio.Future[int] = loop.create_future()
    status = FakeProducer()
    status.enqueue_nowait = lambda topic, record: future
    gate = Gate(status=status)
    before = sample('drakkar_split_http_status_write_failures_total')

    gate.writer.write_best_effort(record('running'))
    future.cancel()
    await asyncio.sleep(0)

    assert sample('drakkar_split_http_status_write_failures_total') == before + 1


async def test_write_failures_log_once_per_interval():
    gate = Gate(status=FakeProducer(nowait_full=True))
    with capture_logs() as logs:
        for _ in range(5):
            gate.writer.write_best_effort(record('running'))

    assert len([line for line in logs if line['event'] == 'split_http_status_write_failed']) == 1


# --- metrics ---


async def test_metrics_of_accept_and_read():
    gate = Gate()
    gate.put(record('done', request_id='done-1', result_json='1'))
    accepted = sample('drakkar_split_http_jobs_accepted_total')
    done = sample('drakkar_split_http_status_reads_total', status='done')
    not_found = sample('drakkar_split_http_status_reads_total', status='not_found')

    await gate.accept('new-1')
    await gate.accept('done-1')  # a duplicate is not counted as accepted
    await gate.gateway.read(request_id='done-1', client=CLIENT)
    await gate.gateway.read(request_id='done-1', client=OTHER)
    await gate.gateway.read(request_id='bad id', client=CLIENT)

    assert sample('drakkar_split_http_jobs_accepted_total') == accepted + 1
    assert sample('drakkar_split_http_status_reads_total', status='done') == done + 1
    assert sample('drakkar_split_http_status_reads_total', status='not_found') == not_found + 2


# --- the runtime ---


async def test_runtime_gateway_stores_a_request_through_the_real_producer():
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=True), http_enabled=True)
    await runtime.start()
    http = runtime.http
    assert http is not None
    await asyncio.wait_for(http.follower.restored.wait(), 5)
    try:
        assert http.gateway.is_ready
        answer = await http.gateway.accept(
            request_id='req-9', client=CLIENT, body=b'payload', deliver_sinks=False, timeout=5
        )
    finally:
        await runtime.stop(deadline=0.0)

    assert answer.status_code == 202
    [source] = parts.log.records(SOURCE_TOPIC, 0)
    assert (source.key, source.value) == (b'req-9', b'payload')
    statuses = [
        stored
        for partition in range(parts.counts[STATUS_TOPIC])
        for stored in parts.log.records(STATUS_TOPIC, partition)
    ]
    [status] = statuses
    parsed = parse_status_record(status.key, status.headers, status.value)
    assert parsed is not None and (parsed.state, parsed.job, parsed.client) == (
        'accepted',
        f'0:{source.offset}',
        CLIENT,
    )


async def test_runtime_gateway_refuses_a_call_from_another_thread():
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=True), http_enabled=True)
    await runtime.start()
    http = runtime.http
    assert http is not None
    errors: list[BaseException] = []

    def other_thread() -> None:
        try:
            asyncio.run(http.gateway.read(request_id='req-1', client=CLIENT))
        except RuntimeError as error:
            errors.append(error)

    thread = threading.Thread(target=other_thread)
    thread.start()
    await asyncio.to_thread(thread.join)
    await runtime.stop(deadline=0.0)

    assert len(errors) == 1 and 'main loop' in str(errors[0])


async def test_write_end_cancelled_during_the_retry_wait_is_cancelled():
    async def long_sleep(seconds: float) -> None:
        await asyncio.Event().wait()

    gate = Gate(status=FakeProducer(SplitProduceError('broker down', final=False)), sleep=long_sleep)
    writing = asyncio.ensure_future(gate.writer.write_end(record('done', result_json='1'), asyncio.Event()))
    await asyncio.sleep(0.01)
    writing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await writing
