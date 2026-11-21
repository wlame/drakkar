"""The job tracker: restore, live reading, events, positions, timeouts and release."""

import asyncio
import dataclasses
from collections.abc import Callable

import pytest
from confluent_kafka import KafkaError, KafkaException
from structlog.testing import capture_logs

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.metrics import (
    split_jobs_open,
    split_oldest_job_age,
    split_restoring_partitions,
    split_results_ignored,
    split_results_received,
)
from drakkar.split.models import PartResult
from drakkar.split.records import JobRef, closed_body, log_record, open_body, result_body
from drakkar.split.table import JobState
from drakkar.split.tracker import (
    CONSUMER_ERROR_PAUSE_SECONDS,
    LOOP_RESTART_SECONDS,
    POSITION_COMMIT_SECONDS,
    TIMEOUT_CHECK_SECONDS,
    JobTracker,
)
from tests.split_helpers import START_MS, FakeClock, FakeConsumerClient, FakeLog

TOPIC = 'items_g_split_results'
SOURCE_GROUP = 'items-group'
RESULTS_GROUP = 'items-group-split-results'
PARTITION = 3
ME = 'instance-me'
OTHER = 'instance-other'
ATTEMPT_1 = '1111111111111111'
ATTEMPT_2 = '2222222222222222'


def ref(offset: int, attempt_id: str = ATTEMPT_1, attempt: int = 1, group: str = SOURCE_GROUP) -> JobRef:
    return JobRef(group=group, partition=PARTITION, offset=offset, attempt=attempt, attempt_id=attempt_id)


class Writer:
    """Appends results-log records of partition ``PARTITION`` to a fake log."""

    def __init__(self, log: FakeLog) -> None:
        self.log = log

    def put(self, record) -> int:
        return self.log.append(TOPIC, PARTITION, key=record.key, value=record.value, headers=record.headers)

    def open(self, offset: int, instance: str = ME, attempt_id: str = ATTEMPT_1, attempt: int = 1) -> int:
        body = open_body(worker='w', instance=instance, request_id='req_1')
        return self.put(log_record(ref(offset, attempt_id, attempt), 'open', body))

    def done(self, offset: int, parts: int, attempt_id: str = ATTEMPT_1) -> int:
        return self.put(log_record(ref(offset, attempt_id), 'split-done', {'parts': parts}))

    def result(self, offset: int, index: int, attempt_id: str = ATTEMPT_1) -> int:
        body = result_body(PartResult(index=index, worker='w2', outcome='ok'))
        return self.put(log_record(ref(offset, attempt_id), 'result', body))

    def closed(self, offset: int, attempt_id: str = ATTEMPT_1) -> int:
        body = closed_body(outcome='aggregated', received=1, parts=1, duration_seconds=1.0)
        return self.put(log_record(ref(offset, attempt_id), 'closed', body))


class Listener:
    """Records every event and whether the partition was restoring when it came."""

    def __init__(self, tracker_ref: Callable[[], JobTracker | None]) -> None:
        self.events: list[tuple[str, int, int, str | None]] = []
        self.restoring_at_event: list[bool] = []
        self._tracker_ref = tracker_ref
        self.fail = False

    def _record(self, name: str, partition: int, job: JobState, kind: str | None = None) -> None:
        tracker = self._tracker_ref()
        self.restoring_at_event.append(tracker is not None and tracker.is_restoring(partition))
        self.events.append((name, partition, job.offset, kind))
        if self.fail:
            raise RuntimeError('listener broke')

    def job_complete(self, partition: int, job: JobState) -> None:
        self._record('complete', partition, job)

    def job_timed_out(self, partition: int, job: JobState, kind) -> None:
        self._record('timed_out', partition, job, kind)

    def job_reopened_elsewhere(self, partition: int, job: JobState) -> None:
        self._record('reopened', partition, job)


class FakeSleep:
    """An injectable sleep: every call waits until the test fires its duration."""

    def __init__(self) -> None:
        self.waiters: list[tuple[float, asyncio.Future[None]]] = []
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        future = asyncio.get_running_loop().create_future()
        self.calls.append(seconds)
        self.waiters.append((seconds, future))
        await future

    def waiting(self, seconds: float, count: int = 1) -> bool:
        return sum(1 for s, f in self.waiters if s == seconds and not f.done()) >= count

    async def fire(self, seconds: float, count: int = 1) -> None:
        """Wait for ``count`` sleepers of ``seconds``, wake every one of them, and let the woken work run."""
        await wait_until(lambda: self.waiting(seconds, count))
        woken = [(s, f) for s, f in self.waiters if s == seconds]
        self.waiters = [(s, f) for s, f in self.waiters if s != seconds]
        for _seconds, future in woken:
            if not future.done():
                future.set_result(None)
        await settle()


async def wait_until(condition: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError('condition not met in time')
        await asyncio.sleep(0.001)


async def settle() -> None:
    """Give the tracker's loops time for a few consume rounds."""
    await asyncio.sleep(0.03)


class CountingConsumer(FakeConsumerClient):
    """The fake consumer that also keeps every commit it was asked to make."""

    def __init__(self, log: FakeLog, *, group_id: str) -> None:
        super().__init__(log, group_id=group_id)
        self.commits: list[list[tuple[int, int]]] = []

    async def commit(self, message=None, offsets=None, asynchronous=False):
        await super().commit(message=message, offsets=offsets, asynchronous=asynchronous)
        self.commits.append([(tp.partition, tp.offset) for tp in offsets or []])


class GatedConsumer(CountingConsumer):
    """Holds every ``consume`` until the test opens the gate, so a test can act between two batches."""

    def __init__(self, log: FakeLog, *, group_id: str) -> None:
        super().__init__(log, group_id=group_id)
        self.gate: asyncio.Event | None = None
        self.gated = 0

    async def consume(self, num_messages=1, timeout=-1):
        if self.gate is not None and not self.gate.is_set():
            self.gated += 1
            await self.gate.wait()
        return await super().consume(num_messages=num_messages, timeout=timeout)


class Harness:
    def __init__(self, *, job_timeout: int = 0, idle_timeout: int = 0) -> None:
        self.clock = FakeClock()
        self.log = FakeLog(self.clock)
        self.writer = Writer(self.log)
        self.sleep = FakeSleep()
        self.consumers: list[CountingConsumer] = []
        self.configs: list[dict] = []
        self.tracker: JobTracker | None = None
        self.consumer_class: type[CountingConsumer] = CountingConsumer
        self.listener = Listener(lambda: self.tracker)
        self.tracker = JobTracker(
            connection=KafkaConnectionConfig(brokers='broker:9092'),
            results_topic=TOPIC,
            results_group=RESULTS_GROUP,
            source_group=SOURCE_GROUP,
            instance=ME,
            job_timeout=job_timeout,
            idle_timeout=idle_timeout,
            events=self.listener,
            consumer_factory=self._factory,
            clock=lambda: self.clock.now_ms / 1000,
            sleep=self.sleep,
        )

    def _factory(self, config: dict) -> CountingConsumer:
        self.configs.append(config)
        consumer = self.consumer_class(self.log, group_id=config['group.id'])
        self.consumers.append(consumer)
        return consumer

    @property
    def consumer(self) -> CountingConsumer:
        return self.consumers[0]

    @property
    def t(self) -> JobTracker:
        assert self.tracker is not None
        return self.tracker

    async def follow(self, committed_source_offset: int | None = None):
        return await asyncio.wait_for(
            self.t.follow(PARTITION, committed_source_offset=committed_source_offset), timeout=2
        )


@pytest.fixture
async def harness():
    h = Harness()
    await h.t.start()
    yield h
    await h.t.stop()


def ignored(reason: str) -> float:
    return split_results_ignored.labels(reason=reason)._value.get()


def received() -> float:
    return split_results_received._value.get()


async def test_consumer_config_has_the_results_group_and_no_auto_commit(harness):
    config = harness.configs[0]
    assert config['group.id'] == RESULTS_GROUP
    assert config['bootstrap.servers'] == 'broker:9092'
    assert config['enable.auto.commit'] is False
    assert config['auto.offset.reset'] == 'earliest'


async def test_follow_with_no_committed_position_starts_at_the_earliest_record(harness):
    harness.writer.open(10)
    harness.writer.open(11)
    harness.writer.open(12)
    harness.log.raise_low(TOPIC, PARTITION, 1)
    table = await harness.follow()
    assert [job.offset for job in table.open_jobs()] == [11, 12]
    assert harness.t.table(PARTITION) is table
    assert not harness.t.is_restoring(PARTITION)


async def test_follow_starts_at_the_committed_position(harness):
    harness.writer.open(10)
    harness.writer.open(11)
    harness.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 1)
    table = await harness.follow()
    assert [job.offset for job in table.open_jobs()] == [11]


async def test_restore_sends_no_event_then_completes_a_job_that_finished_with_no_owner(harness):
    harness.writer.open(40)
    harness.writer.done(40, 2)
    harness.writer.result(40, 0)
    harness.writer.result(40, 1)
    harness.writer.open(41)
    before = received()
    table = await harness.follow()
    assert harness.listener.events == [('complete', PARTITION, 40, None)]
    assert harness.listener.restoring_at_event == [False]
    assert table.job(40).is_complete
    assert received() == before


async def test_committed_source_offset_retires_lower_jobs_without_a_record(harness):
    harness.writer.open(10)
    harness.writer.open(20)
    records = len(harness.log.records(TOPIC, PARTITION))
    table = await harness.follow(committed_source_offset=15)
    assert [job.offset for job in table.open_jobs()] == [20]
    assert len(harness.log.records(TOPIC, PARTITION)) == records


async def test_live_result_completes_a_job_once_with_split_done_and_the_last_result_in_one_batch(harness):
    await harness.follow()
    before_received = received()
    before_duplicates = ignored('duplicate')
    harness.writer.open(40)
    harness.writer.result(40, 0)
    harness.writer.result(40, 1)
    harness.writer.done(40, 3)
    harness.writer.result(40, 2)
    harness.writer.result(40, 2)
    await wait_until(lambda: harness.listener.events)
    await settle()
    assert harness.listener.events == [('complete', PARTITION, 40, None)]
    assert received() - before_received == 3
    assert ignored('duplicate') - before_duplicates == 1


async def test_live_records_of_another_group_and_unreadable_records_are_counted(harness):
    harness.writer.put(log_record(ref(5, group='another'), 'open', open_body(worker='w', instance=ME, request_id=None)))
    await harness.follow()
    foreign = ignored('foreign_group')
    malformed = ignored('malformed')
    harness.writer.put(log_record(ref(6, group='another'), 'open', open_body(worker='w', instance=ME, request_id=None)))
    harness.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
    await wait_until(lambda: ignored('malformed') - malformed == 1)
    assert ignored('foreign_group') - foreign == 1


async def test_note_source_commit_retires_a_closed_job_and_moves_the_position(harness):
    harness.writer.open(10)
    harness.writer.closed(10)
    third = harness.writer.open(20)
    await harness.follow()
    await wait_until(lambda: harness.consumer.commits)
    # The closed job is not retired, so the position stays at its open record.
    assert harness.consumer.commits == [[(PARTITION, 0)]]
    harness.t.note_source_commit(PARTITION, 15)
    await wait_until(lambda: len(harness.consumer.commits) == 2)
    assert harness.consumer.commits[1] == [(PARTITION, third)]
    assert harness.log.committed(RESULTS_GROUP, TOPIC, PARTITION) == third
    assert harness.t.table(PARTITION).job(10) is None


async def test_note_source_commit_that_retires_nothing_commits_nothing(harness):
    harness.writer.open(20)
    await harness.follow()
    await wait_until(lambda: harness.consumer.commits)
    harness.t.note_source_commit(PARTITION, 5)
    harness.t.note_source_commit(PARTITION + 1, 50)
    await settle()
    assert len(harness.consumer.commits) == 1


async def test_position_commit_is_sent_at_most_once_a_period_and_only_on_a_change(harness):
    harness.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 0)
    await harness.follow()
    await settle()
    assert harness.consumer.commits == []
    for offset in range(5):
        harness.writer.open(offset)
        harness.writer.closed(offset)
    harness.t.note_source_commit(PARTITION, 5)
    await wait_until(lambda: harness.consumer.commits)
    await settle()
    harness.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
    harness.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
    await settle()
    commits = len(harness.consumer.commits)
    await harness.sleep.fire(POSITION_COMMIT_SECONDS)
    assert len(harness.consumer.commits) == commits + 1
    assert harness.consumer.commits[-1] == [(PARTITION, 12)]
    await harness.sleep.fire(POSITION_COMMIT_SECONDS)
    assert len(harness.consumer.commits) == commits + 1


async def test_a_failed_position_commit_is_logged_and_tried_again_next_period(harness):
    harness.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 0)
    await harness.follow()
    harness.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
    await settle()
    harness.consumer.fail_next('commit', KafkaException(KafkaError(KafkaError._TRANSPORT)))
    with capture_logs() as logs:
        await harness.sleep.fire(POSITION_COMMIT_SECONDS)
    assert [entry['event'] for entry in logs] == ['split_position_commit_failed']
    assert logs[0]['category'] == 'split'
    assert harness.log.committed(RESULTS_GROUP, TOPIC, PARTITION) == 0
    await harness.sleep.fire(POSITION_COMMIT_SECONDS)
    assert harness.log.committed(RESULTS_GROUP, TOPIC, PARTITION) == 1


async def test_a_stray_closed_of_a_retired_job_does_not_hold_the_position(harness):
    harness.writer.open(10)
    await harness.follow(committed_source_offset=15)
    await wait_until(lambda: harness.consumer.commits)
    last = harness.writer.closed(10)
    await wait_until(lambda: len(harness.consumer.commits) == 2)
    assert harness.t.table(PARTITION).job(10) is None
    assert harness.consumer.commits[-1] == [(PARTITION, last + 1)]


async def test_release_drops_the_table_and_stops_events_of_the_partition(harness):
    await harness.follow()
    await harness.t.release(PARTITION)
    harness.writer.open(40)
    harness.writer.done(40, 1)
    harness.writer.result(40, 0)
    await settle()
    assert harness.listener.events == []
    assert harness.t.table(PARTITION) is None
    assert (TOPIC, PARTITION) not in harness.consumer._positions
    await harness.t.release(PARTITION)


async def test_a_restore_that_fails_twice_then_works(harness):
    error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    harness.consumer.fail_next('committed', error, times=2)
    harness.writer.open(10)
    with capture_logs() as logs:
        follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
        await harness.sleep.fire(1.0)
        await harness.sleep.fire(2.0)
        table = await asyncio.wait_for(follow, timeout=2)
    assert [job.offset for job in table.open_jobs()] == [10]
    failures = [entry for entry in logs if entry['event'] == 'split_restore_failed']
    assert [entry['attempt'] for entry in failures] == [1]  # the second line is held back by the rate limit
    assert [entry['event'] for entry in logs if entry['event'].startswith('split_restore')].count(
        'split_restore_finished'
    ) == 1


async def test_a_consume_error_during_a_restore_starts_the_restore_again():
    h = Harness()
    gate = asyncio.Event()
    h.consumer_class = GatedConsumer
    await h.t.start()
    try:
        h.consumer.gate = gate
        h.writer.open(10)
        follow = asyncio.create_task(h.t.follow(PARTITION, committed_source_offset=None))
        await wait_until(lambda: (TOPIC, PARTITION) in h.consumer._positions and h.consumer.gated > 0)
        h.consumer.fail_next('consume', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        gate.set()
        # The restore backs off for 1 s; the read loop pauses for its own error pause.
        await h.sleep.fire(1.0)
        await h.sleep.fire(CONSUMER_ERROR_PAUSE_SECONDS)
        table = await asyncio.wait_for(follow, timeout=2)
        assert [job.offset for job in table.open_jobs()] == [10]
        assert h.sleep.calls.count(CONSUMER_ERROR_PAUSE_SECONDS) == 1
    finally:
        await h.t.stop()


async def test_release_during_a_failing_restore_cancels_follow_and_leaves_no_task(harness):
    harness.consumer.fail_next('get_watermark_offsets', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=10)
    loops = set(harness.t._tasks)
    follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    await wait_until(lambda: harness.sleep.waiting(1.0))
    assert split_restoring_partitions._value.get() == 1
    await harness.t.release(PARTITION)
    with pytest.raises(asyncio.CancelledError):
        await follow
    assert harness.t._tasks == loops
    assert split_restoring_partitions._value.get() == 0
    assert not harness.t.is_restoring(PARTITION)


async def test_a_caller_that_cancels_follow_drops_the_partition(harness):
    harness.consumer.fail_next('committed', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=10)
    follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    await wait_until(lambda: harness.sleep.waiting(1.0))
    follow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await follow
    await settle()
    assert not harness.t.is_restoring(PARTITION)
    assert harness.t.table(PARTITION) is None


async def test_a_second_follow_of_a_followed_partition_starts_again(harness):
    harness.writer.open(10)
    first = await harness.follow()
    second = await harness.follow()
    assert first is not second
    assert [job.offset for job in second.open_jobs()] == [10]


async def test_restoring_gauge_counts_partitions_waiting_for_their_restore(harness):
    harness.consumer.fail_next('committed', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=1)
    follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    await wait_until(lambda: harness.sleep.waiting(1.0))
    assert split_restoring_partitions._value.get() == 1
    assert harness.t.is_restoring(PARTITION)
    assert harness.t.table(PARTITION) is None
    await harness.sleep.fire(1.0)
    await asyncio.wait_for(follow, timeout=2)
    assert split_restoring_partitions._value.get() == 0


async def test_note_source_commit_during_a_restore_retires_at_its_end(harness):
    harness.consumer.fail_next('committed', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=1)
    harness.writer.open(10)
    harness.writer.open(20)
    follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    await wait_until(lambda: harness.sleep.waiting(1.0))
    harness.t.note_source_commit(PARTITION, 15)
    await harness.sleep.fire(1.0)
    table = await asyncio.wait_for(follow, timeout=2)
    assert [job.offset for job in table.open_jobs()] == [20]


async def test_job_timed_out_once_for_each_kind():
    h = Harness(job_timeout=100, idle_timeout=10)
    await h.t.start()
    try:
        h.writer.open(40)
        await h.follow()
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        assert h.listener.events == []
        assert split_jobs_open._value.get() == 1
        h.clock.advance(11_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        assert h.listener.events == [('timed_out', PARTITION, 40, 'idle')]
        assert split_oldest_job_age._value.get() == 11
        h.clock.advance(90_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        assert h.listener.events[1:] == [('timed_out', PARTITION, 40, 'job')]
    finally:
        await h.t.stop()


async def test_a_closed_job_leaves_the_timeout_memory_and_the_gauges():
    h = Harness(job_timeout=0, idle_timeout=10)
    await h.t.start()
    try:
        h.writer.open(40)
        await h.follow()
        h.clock.advance(11_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        h.writer.closed(40)
        await settle()
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        assert split_jobs_open._value.get() == 0
        assert split_oldest_job_age._value.get() == 0
        assert h.listener.events == [('timed_out', PARTITION, 40, 'idle')]
    finally:
        await h.t.stop()


async def test_a_new_attempt_can_time_out_again():
    h = Harness(job_timeout=0, idle_timeout=10)
    await h.t.start()
    try:
        h.writer.open(40)
        await h.follow()
        h.clock.advance(11_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        h.writer.open(40, attempt_id=ATTEMPT_2, attempt=2)
        await settle()
        h.clock.advance(11_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        assert [event[3] for event in h.listener.events] == ['idle', 'idle']
    finally:
        await h.t.stop()


async def test_job_reopened_elsewhere_for_a_job_this_instance_had_open(harness):
    await harness.follow()
    harness.writer.open(40, instance=ME)
    harness.writer.open(41, instance=OTHER)
    harness.writer.open(40, instance=OTHER, attempt_id=ATTEMPT_2, attempt=2)
    harness.writer.open(41, instance=ME, attempt_id=ATTEMPT_2, attempt=2)
    await wait_until(lambda: harness.listener.events)
    await settle()
    assert harness.listener.events == [('reopened', PARTITION, 40, None)]


async def test_a_listener_that_raises_is_logged_and_the_tracker_goes_on(harness):
    await harness.follow()
    harness.listener.fail = True
    with capture_logs() as logs:
        harness.writer.open(40)
        harness.writer.done(40, 1)
        harness.writer.result(40, 0)
        await wait_until(lambda: harness.listener.events)
        harness.writer.open(41)
        harness.writer.done(41, 1)
        harness.writer.result(41, 0)
        await wait_until(lambda: len(harness.listener.events) == 2)
    failed = [entry for entry in logs if entry['event'] == 'split_tracker_listener_failed']
    assert failed and failed[0]['log_level'] == 'error' and failed[0]['category'] == 'split'


async def test_a_consumer_error_in_live_reading_is_logged_and_reading_goes_on(harness):
    await harness.follow()
    with capture_logs() as logs:
        harness.consumer.inject_error(KafkaError(KafkaError._TRANSPORT))
        await harness.sleep.fire(CONSUMER_ERROR_PAUSE_SECONDS)
        harness.consumer.fail_next('consume', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        await harness.sleep.fire(CONSUMER_ERROR_PAUSE_SECONDS)
    events = [entry['event'] for entry in logs]
    assert events.count('split_tracker_consume_failed') == 1  # the second is held back by the rate limit
    harness.writer.open(40)
    harness.writer.done(40, 1)
    harness.writer.result(40, 0)
    await wait_until(lambda: harness.listener.events)


async def test_a_record_without_a_timestamp_takes_the_clock(harness):
    await harness.follow()

    class NoTime:
        def __init__(self, message):
            self._message = message

        def __getattr__(self, name):
            return getattr(self._message, name)

        def timestamp(self):
            return (0, -1)

    original = harness.consumer.consume

    async def consume(num_messages=1, timeout=-1):
        return [NoTime(message) for message in await original(num_messages=num_messages, timeout=timeout)]

    harness.consumer.consume = consume
    harness.clock.advance(5_000)
    harness.writer.open(40)
    await wait_until(lambda: harness.t.table(PARTITION).job(40) is not None)
    assert harness.t.table(PARTITION).job(40).opened_at_ms == START_MS + 5_000


async def test_stop_closes_the_consumer_ends_every_task_and_zeroes_the_gauges():
    h = Harness()
    await h.t.start()
    h.consumer.fail_next('committed', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=10)
    follow = asyncio.create_task(h.t.follow(PARTITION, committed_source_offset=None))
    await wait_until(lambda: h.sleep.waiting(1.0))
    await h.t.stop()
    with pytest.raises(asyncio.CancelledError):
        await follow
    assert h.consumer.closed
    assert asyncio.all_tasks() == {asyncio.current_task()}
    assert split_restoring_partitions._value.get() == 0
    assert split_jobs_open._value.get() == 0
    assert split_oldest_job_age._value.get() == 0
    await h.t.stop()
    await h.t.start()
    await h.t.release(PARTITION)
    h.t.note_source_commit(PARTITION, 1)
    with pytest.raises(asyncio.CancelledError):
        await h.t.follow(PARTITION, committed_source_offset=None)


async def test_a_close_failure_at_stop_is_logged():
    h = Harness()
    await h.t.start()
    h.consumer.fail_next('close', KafkaException(KafkaError(KafkaError._TRANSPORT)))
    with capture_logs() as logs:
        await h.t.stop()
    assert [entry['event'] for entry in logs] == ['split_tracker_close_failed']


async def test_follow_before_start_raises():
    h = Harness()
    with pytest.raises(RuntimeError):
        await h.t.follow(PARTITION, committed_source_offset=None)
    await h.t.stop()


async def test_an_unassign_failure_at_release_is_logged(harness):
    await harness.follow()
    harness.consumer.fail_next('incremental_unassign', KafkaException(KafkaError(KafkaError._TRANSPORT)))
    with capture_logs() as logs:
        await harness.t.release(PARTITION)
    assert [entry['event'] for entry in logs] == ['split_tracker_unassign_failed']


async def test_a_failing_periodic_step_is_logged_and_the_loop_goes_on(harness, monkeypatch):
    calls = []

    def broken():
        calls.append(1)
        raise RuntimeError('broken check')

    monkeypatch.setattr(harness.t, '_check_timeouts', broken)
    with capture_logs() as logs:
        await harness.sleep.fire(TIMEOUT_CHECK_SECONDS)
        await harness.sleep.fire(TIMEOUT_CHECK_SECONDS)
    assert len(calls) == 2
    assert [entry['event'] for entry in logs] == ['split_tracker_periodic_failed'] * 2


async def test_a_restoring_partition_is_left_out_of_commits_and_timeout_checks():
    h = Harness(job_timeout=0, idle_timeout=10)
    await h.t.start()
    try:
        h.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
        h.writer.open(40)
        await h.follow()
        other = PARTITION + 1
        h.consumer.fail_next('committed', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=10)
        follow = asyncio.create_task(h.t.follow(other, committed_source_offset=None))
        await wait_until(lambda: h.sleep.waiting(1.0))
        h.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
        await settle()
        h.clock.advance(11_000)
        await h.sleep.fire(TIMEOUT_CHECK_SECONDS)
        await h.sleep.fire(POSITION_COMMIT_SECONDS)
        assert h.listener.events == [('timed_out', PARTITION, 40, 'idle')]
        assert all(partition == PARTITION for commit in h.consumer.commits for partition, _ in commit)
        await h.t.release(other)
        with pytest.raises(asyncio.CancelledError):
            await follow
    finally:
        await h.t.stop()


async def test_records_of_an_assignment_that_lands_during_a_consume_are_read():
    h = Harness()
    await h.t.start()
    try:
        h.consumer.wait_for_records = True
        h.writer.open(10)
        h.writer.open(11)
        await wait_until(lambda: h.consumer.consumes_waiting > 0)
        # The consume in flight started before the assignment; it returns records of the new one.
        table = await h.follow()
        assert [job.offset for job in table.open_jobs()] == [10, 11]
        h.writer.open(12)
        await wait_until(lambda: h.t.table(PARTITION).job(12) is not None)
    finally:
        await h.t.stop()


async def test_a_failed_seek_back_is_logged_and_tried_again():
    h = Harness()
    await h.t.start()
    try:
        h.consumer.wait_for_records = True
        h.consumer.fail_next('seek', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        h.writer.open(10)
        await wait_until(lambda: h.consumer.consumes_waiting > 0)
        with capture_logs() as logs:
            table = await h.follow()
        assert [job.offset for job in table.open_jobs()] == [10]
        assert 'split_tracker_seek_failed' in [entry['event'] for entry in logs]
    finally:
        await h.t.stop()


async def test_an_error_while_handling_a_record_is_logged_and_reading_goes_on(harness, monkeypatch):
    await harness.follow()
    original = harness.t._apply_live
    calls = []

    def broken_once(*args):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('broken record')
        original(*args)

    monkeypatch.setattr(harness.t, '_apply_live', broken_once)
    with capture_logs() as logs:
        harness.writer.open(40)
        harness.writer.open(41)
        harness.writer.done(41, 1)
        harness.writer.result(41, 0)
        await wait_until(lambda: harness.listener.events)
    assert harness.listener.events == [('complete', PARTITION, 41, None)]
    failed = [entry for entry in logs if entry['event'] == 'split_tracker_record_failed']
    assert failed[0]['log_level'] == 'error' and failed[0]['offset'] == 0


async def test_an_error_on_the_last_restore_record_still_ends_the_restore(harness, monkeypatch):
    harness.writer.open(10)

    def broken(*args):
        raise RuntimeError('broken record')

    monkeypatch.setattr(harness.t, '_apply', broken)
    with capture_logs():
        table = await harness.follow()
    assert len(table) == 0


async def test_a_committed_position_above_the_end_reads_from_the_start(harness):
    harness.writer.open(10)
    harness.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 100)
    with capture_logs() as logs:
        table = await harness.follow()
    assert [job.offset for job in table.open_jobs()] == [10]
    reset = [entry for entry in logs if entry['event'] == 'split_restore_position_reset']
    assert reset[0]['committed'] == 100 and reset[0]['low'] == 0 and reset[0]['high'] == 1
    harness.writer.open(11)
    await wait_until(lambda: table.job(11) is not None)


class SlowUnassignConsumer(CountingConsumer):
    """An unassign that takes a moment, so a later assignment can overtake it unless the tracker waits."""

    async def incremental_unassign(self, partitions):
        await asyncio.sleep(0.02)
        await super().incremental_unassign(partitions)


async def test_a_follow_after_a_cancelled_follow_waits_for_the_pending_unassign():
    h = Harness()
    h.consumer_class = SlowUnassignConsumer
    await h.t.start()
    try:
        gate = asyncio.Event()
        original = h.consumer.get_watermark_offsets

        async def held_watermarks(partition, timeout=None, cached=False):
            await gate.wait()
            return await original(partition, timeout=timeout, cached=cached)

        h.writer.open(10)
        first = asyncio.create_task(h.t.follow(PARTITION, committed_source_offset=None))
        h.consumer.get_watermark_offsets = held_watermarks
        await wait_until(lambda: h.t.is_restoring(PARTITION))
        # Let the first restore assign, then cancel it while its read is still waiting.
        h.consumer.get_watermark_offsets = original
        gate.set()
        await wait_until(lambda: (TOPIC, PARTITION) in h.consumer._positions)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        table = await h.follow()
        assert [job.offset for job in table.open_jobs()] == [10]
        await asyncio.sleep(0.05)
        assert (TOPIC, PARTITION) in h.consumer._positions
    finally:
        await h.t.stop()


class OverstatedEndConsumer(CountingConsumer):
    """Reports, once, an end above the last record, as a log that was cut after the read would."""

    def __init__(self, log: FakeLog, *, group_id: str) -> None:
        super().__init__(log, group_id=group_id)
        self.overstate = 5

    async def get_watermark_offsets(self, partition, timeout=None, cached=False):
        low, high = await super().get_watermark_offsets(partition, timeout=timeout, cached=cached)
        extra, self.overstate = self.overstate, 0
        return low, high + extra


async def test_a_restore_ends_when_the_end_of_the_partition_falls(monkeypatch):
    monkeypatch.setattr('drakkar.split.tracker.RESTORE_END_CHECK_SECONDS', 0)
    h = Harness()
    h.consumer_class = OverstatedEndConsumer
    await h.t.start()
    try:
        h.writer.open(10)
        table = await h.follow()
        assert [job.offset for job in table.open_jobs()] == [10]
    finally:
        await h.t.stop()


async def test_a_failed_end_check_is_logged_and_the_restore_waits(monkeypatch):
    monkeypatch.setattr('drakkar.split.tracker.RESTORE_END_CHECK_SECONDS', 0)
    h = Harness()
    h.consumer_class = OverstatedEndConsumer
    await h.t.start()
    try:
        h.writer.open(10)
        follow = asyncio.create_task(h.t.follow(PARTITION, committed_source_offset=None))
        await wait_until(lambda: h.consumer.overstate == 0)
        h.consumer.fail_next('get_watermark_offsets', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=3)
        with capture_logs() as logs:
            table = await asyncio.wait_for(follow, timeout=2)
        assert len(table) == 1
        assert 'split_restore_end_check_failed' in [entry['event'] for entry in logs]
    finally:
        await h.t.stop()


async def test_a_read_loop_that_fails_is_logged_and_started_again(harness, monkeypatch):
    await harness.follow()
    original = harness.t._read_once
    calls = []

    async def broken_once():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('broken loop')
        await original()

    monkeypatch.setattr(harness.t, '_read_once', broken_once)
    with capture_logs() as logs:
        await harness.sleep.fire(LOOP_RESTART_SECONDS)
    assert [entry['loop'] for entry in logs if entry['event'] == 'split_tracker_loop_failed'] == ['read']
    harness.writer.open(40)
    await wait_until(lambda: harness.t.table(PARTITION).job(40) is not None)


async def test_a_task_that_ends_on_an_error_is_logged(harness):
    async def broken():
        raise RuntimeError('broken task')

    with capture_logs() as logs:
        task = harness.t._spawn(broken())
        await asyncio.wait({task})
    assert [entry['event'] for entry in logs] == ['split_tracker_task_failed']


async def test_two_follows_of_one_partition_at_once_leave_one_restore(harness):
    harness.writer.open(10)
    loops = set(harness.t._tasks)
    first = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    second = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    third = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
    results = await asyncio.wait_for(asyncio.gather(first, second, third, return_exceptions=True), timeout=2)
    tables = [result for result in results if not isinstance(result, BaseException)]
    assert len(tables) == 1 and [job.offset for job in tables[0].open_jobs()] == [10]
    assert sum(isinstance(result, asyncio.CancelledError) for result in results) == 2
    await settle()
    assert {task for task in harness.t._tasks if not task.done()} <= loops | set(harness.t._unassigning.values())


class ErrorRow:
    """A row of ``committed`` that carries an error, as the real client gives for a failed lookup."""

    def __init__(self, error: KafkaError) -> None:
        self.offset = -1001
        self.error = error


async def test_a_committed_row_with_an_error_is_a_failed_restore(harness):
    original = harness.consumer.committed
    calls = []

    async def committed(partitions, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return [ErrorRow(KafkaError(KafkaError._TIMED_OUT))]
        return await original(partitions, timeout=timeout)

    harness.consumer.committed = committed
    harness.writer.open(10)
    harness.writer.open(11)
    harness.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 1)
    with capture_logs() as logs:
        follow = asyncio.create_task(harness.t.follow(PARTITION, committed_source_offset=None))
        await harness.sleep.fire(1.0)
        table = await asyncio.wait_for(follow, timeout=2)
    # The second attempt read the committed position; a row with an error was not taken as "none".
    assert [job.offset for job in table.open_jobs()] == [11]
    assert [entry['event'] for entry in logs].count('split_restore_failed') == 1


class SlowCommitConsumer(CountingConsumer):
    async def commit(self, message=None, offsets=None, asynchronous=False):
        await asyncio.sleep(0.02)
        await super().commit(message=message, offsets=offsets, asynchronous=asynchronous)


async def test_position_commits_do_not_overlap():
    h = Harness()
    h.consumer_class = SlowCommitConsumer
    await h.t.start()
    try:
        h.log.commit(RESULTS_GROUP, TOPIC, PARTITION, 0)
        await h.follow()
        h.log.append(TOPIC, PARTITION, key=b'x', value=b'not a record', headers=[])
        await settle()
        await asyncio.gather(h.t._commit_positions([PARTITION]), h.t._commit_positions([PARTITION]))
        assert h.consumer.commits == [[(PARTITION, 1)]]
    finally:
        await h.t.stop()


def snapshot(table) -> list[dict]:
    jobs = [table.job(offset) for offset in range(0, 100) if table.job(offset) is not None]
    return [dataclasses.asdict(job) for job in jobs]


async def test_a_restart_restores_the_same_jobs_as_live_reading_built():
    first = Harness()
    await first.t.start()
    w = first.writer
    await first.follow()
    w.open(10)  # retired below
    w.closed(10)
    w.open(20)
    w.result(20, 0)
    w.done(20, 3)
    w.result(20, 0)  # a duplicate
    w.open(30)
    w.closed(30)  # closed, not retired
    w.open(40, attempt_id=ATTEMPT_1)
    w.result(40, 1)
    w.open(40, attempt_id=ATTEMPT_2, attempt=2)
    w.result(40, 0, attempt_id=ATTEMPT_1)  # an old attempt
    w.result(40, 0, attempt_id=ATTEMPT_2)
    last = w.done(40, 2, attempt_id=ATTEMPT_2)
    await wait_until(lambda: first.t.table(PARTITION).job(40) is not None and first.t.table(PARTITION).job(40).parts)
    first.t.note_source_commit(PARTITION, 15)
    await wait_until(lambda: first.log.committed(RESULTS_GROUP, TOPIC, PARTITION) == 2)
    live = snapshot(first.t.table(PARTITION))
    await first.t.release(PARTITION)
    await first.t.stop()
    assert last == 13 and [job['offset'] for job in live] == [20, 30, 40]

    second = Harness()
    second.clock = first.clock
    second.log = first.log
    await second.t.start()
    try:
        restored = await second.follow(committed_source_offset=15)
        assert snapshot(restored) == live
    finally:
        await second.t.stop()
