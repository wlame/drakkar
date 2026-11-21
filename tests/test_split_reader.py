"""Reading the part results of one job from the results log."""

import asyncio

import pytest
from confluent_kafka import KafkaError, KafkaException
from pydantic import BaseModel

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.split import reader
from drakkar.split.models import PartResult, SplitResultsExpired, SplitResultsUnavailable
from drakkar.split.reader import read_results, results_consumer_factory
from drakkar.split.records import JobRef, log_record, open_body, result_body
from drakkar.split.table import JobState
from tests.split_helpers import FakeConsumerClient, FakeLog

TOPIC = 'items_g_split_results'
GROUP = 'g'
PARTITION = 2
JOB_OFFSET = 40
CURRENT = 'aaaaaaaaaaaaaaaa'
OLD = 'bbbbbbbbbbbbbbbb'


def ref(*, attempt_id: str = CURRENT, offset: int = JOB_OFFSET, group: str = GROUP, partition: int = PARTITION):
    return JobRef(group=group, partition=partition, offset=offset, attempt=1, attempt_id=attempt_id)


def add(log: FakeLog, record) -> int:
    return log.append(TOPIC, record.partition, key=record.key, value=record.value, headers=record.headers)


def add_result(log: FakeLog, index: int, job_ref: JobRef | None = None, **fields) -> int:
    result = PartResult(index=index, worker='w', outcome='ok', **fields)
    return add(log, log_record(job_ref or ref(), 'result', result_body(result)))


def add_open(log: FakeLog, job_ref: JobRef | None = None) -> int:
    body = open_body(worker='w', instance='i', request_id=None)
    return add(log, log_record(job_ref or ref(), 'open', body))


def job_state(first: int, last: int | None, parts: int | None = None) -> JobState:
    return JobState(
        partition=PARTITION,
        offset=JOB_OFFSET,
        attempt=1,
        attempt_id=CURRENT,
        instance='i',
        request_id=None,
        first_open_offset=first,
        opened_at_ms=0,
        last_progress_ms=0,
        parts=parts,
        last_result_offset=last,
    )


async def read(log: FakeLog, job: JobState, model=None, consumers: list | None = None) -> list[PartResult]:
    def factory():
        consumer = FakeConsumerClient(log, group_id='results', auto_offset_reset='error')
        (consumers if consumers is not None else []).append(consumer)
        return consumer

    return [
        result
        async for result in read_results(
            consumer_factory=factory, topic=TOPIC, partition=PARTITION, job=job, group=GROUP, part_data_model=model
        )
    ]


@pytest.fixture
def log() -> FakeLog:
    return FakeLog()


async def test_yields_only_the_current_attempt_in_log_order(log):
    first = add_open(log)
    add_result(log, 1, ref(attempt_id=OLD))
    add_result(log, 2)
    add_result(log, 0)
    last = add_result(log, 1)
    results = await read(log, job_state(first, last))
    assert [r.index for r in results] == [2, 0, 1]


async def test_repeated_parts_are_dropped(log):
    first = add_open(log)
    add_result(log, 0, tasks_succeeded=1)
    add_result(log, 0, tasks_succeeded=9)
    last = add_result(log, 1)
    results = await read(log, job_state(first, last))
    assert [(r.index, r.tasks_succeeded) for r in results] == [(0, 1), (1, 0)]


async def test_stops_at_the_last_result_offset(log):
    first = add_open(log)
    last = add_result(log, 0)
    add_result(log, 1)
    consumers: list = []
    assert [r.index for r in await read(log, job_state(first, last), consumers=consumers)] == [0]
    assert consumers[0].closed


async def test_job_without_a_counted_result_opens_no_consumer(log):
    consumers: list = []
    assert await read(log, job_state(0, None), consumers=consumers) == []
    assert consumers == []


async def test_skips_records_of_other_jobs_groups_kinds_and_bad_shapes(log):
    first = add_open(log)
    add_result(log, 0, ref(offset=JOB_OFFSET + 1))
    add_result(log, 0, ref(group='other'))
    add_result(log, 0, ref(partition=PARTITION + 1))
    log.append(TOPIC, PARTITION, key=b'not-a-record', value=b'x', headers=[])
    add(log, log_record(ref(), 'result', {'no_part': 1}))
    add(log, log_record(ref(), 'result', {'part': 5, 'worker': 'w', 'outcome': 'nonsense'}))
    add(log, log_record(ref(), 'result', {'part': -1, 'worker': 'w', 'outcome': 'ok'}))
    wrong_key = log_record(ref(), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    log.append(TOPIC, PARTITION, key=b'9:9', value=wrong_key.value, headers=wrong_key.headers)
    last = add_result(log, 3)
    assert [r.index for r in await read(log, job_state(first, last))] == [3]


async def test_part_number_not_below_the_known_part_count_is_skipped(log):
    first = add_open(log)
    add_result(log, 2)
    last = add_result(log, 0)
    assert [r.index for r in await read(log, job_state(first, last, parts=2))] == [0]


class Answer(BaseModel):
    total: int


async def test_data_is_parsed_with_the_model_and_stays_a_dict_when_it_does_not_validate(log):
    first = add_open(log)
    add_result(log, 0, data={'total': 5})
    add_result(log, 1, data={'total': 'many'})
    last = add_result(log, 2)
    results = await read(log, job_state(first, last), model=Answer)
    assert [r.data for r in results] == [Answer(total=5), {'total': 'many'}, None]


async def test_data_stays_a_dict_without_a_model(log):
    first = add_open(log)
    last = add_result(log, 0, data={'total': 5})
    assert (await read(log, job_state(first, last)))[0].data == {'total': 5}


async def test_expired_log_raises_and_closes_the_consumer(log):
    first = add_open(log)
    last = add_result(log, 0)
    log.raise_low(TOPIC, PARTITION, first + 1)
    consumers: list = []
    with pytest.raises(SplitResultsExpired):
        await read(log, job_state(first, last), consumers=consumers)
    assert consumers[0].closed


async def test_client_error_raises_unavailable(log):
    first = add_open(log)
    last = add_result(log, 0)
    consumers: list = []

    def factory():
        consumer = FakeConsumerClient(log)
        consumer.fail_next('consume', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        consumers.append(consumer)
        return consumer

    with pytest.raises(SplitResultsUnavailable):
        async for _ in read_results(
            consumer_factory=factory,
            topic=TOPIC,
            partition=PARTITION,
            job=job_state(first, last),
            group=GROUP,
            part_data_model=None,
        ):
            pass
    assert consumers[0].closed


async def test_error_message_from_the_consumer_raises_unavailable(log):
    first = add_open(log)
    last = add_result(log, 0)

    def factory():
        consumer = FakeConsumerClient(log)
        consumer.inject_error(KafkaError(KafkaError._TRANSPORT))
        return consumer

    with pytest.raises(SplitResultsUnavailable):
        async for _ in read_results(
            consumer_factory=factory,
            topic=TOPIC,
            partition=PARTITION,
            job=job_state(first, last),
            group=GROUP,
            part_data_model=None,
        ):
            pass


class SilentConsumer(FakeConsumerClient):
    """A consumer whose broker returns nothing."""

    async def consume(self, num_messages=1, timeout=-1):
        await asyncio.sleep(0.005)
        return []


class StutteringConsumer(FakeConsumerClient):
    """A consumer that returns one empty poll after its first batch."""

    polls = 0

    async def consume(self, num_messages=1, timeout=-1):
        self.polls += 1
        if self.polls == 2:
            await asyncio.sleep(0.005)
            return []
        return await super().consume(num_messages=1, timeout=timeout)


class RacingConsumer(FakeConsumerClient):
    """The low watermark moves up between the check and the fetch."""

    async def assign(self, partitions):
        await super().assign(partitions)
        self.log.raise_low(TOPIC, PARTITION, partitions[0].offset + 1)


async def test_read_that_makes_no_progress_raises_unavailable(log, monkeypatch):
    first = add_open(log)
    last = add_result(log, 0)
    monkeypatch.setattr(reader, 'NO_PROGRESS_LIMIT_SECONDS', 0.05)
    with pytest.raises(SplitResultsUnavailable, match='no record'):
        await _read_with(SilentConsumer(log), job_state(first, last))


async def _read_with(consumer, job):
    return [
        r
        async for r in read_results(
            consumer_factory=lambda: consumer,
            topic=TOPIC,
            partition=PARTITION,
            job=job,
            group=GROUP,
            part_data_model=None,
        )
    ]


async def test_slow_caller_between_yields_is_not_a_stalled_broker(log, monkeypatch):
    first = add_open(log)
    add_result(log, 0)
    add_result(log, 1)
    last = add_result(log, 2)
    monkeypatch.setattr(reader, 'NO_PROGRESS_LIMIT_SECONDS', 0.05)
    consumer = StutteringConsumer(log)
    got = []
    async for result in read_results(
        consumer_factory=lambda: consumer,
        topic=TOPIC,
        partition=PARTITION,
        job=job_state(first, last),
        group=GROUP,
        part_data_model=None,
    ):
        got.append(result.index)
        await asyncio.sleep(0.1)
    assert got == [0, 1, 2]


async def test_stops_at_the_high_watermark_when_the_last_result_is_not_there(log):
    first = add_open(log)
    add_result(log, 0)
    results = await asyncio.wait_for(read(log, job_state(first, first + 50)), 2)
    assert [r.index for r in results] == [0]


async def test_position_that_fell_below_the_low_watermark_is_expired_not_unavailable(log):
    first = add_open(log)
    add_result(log, 0)
    last = add_result(log, 1)
    with pytest.raises(SplitResultsExpired):
        await _read_with(RacingConsumer(log, auto_offset_reset='error'), job_state(first, last))


async def test_consumer_is_closed_when_the_caller_stops_early(log):
    first = add_open(log)
    for index in range(3):
        add_result(log, index)
    last = add_result(log, 3)
    consumers: list = []
    stream = read_results(
        consumer_factory=lambda: consumers.append(FakeConsumerClient(log)) or consumers[-1],
        topic=TOPIC,
        partition=PARTITION,
        job=job_state(first, last),
        group=GROUP,
        part_data_model=None,
    )
    assert (await anext(stream)).index == 0
    await stream.aclose()
    assert consumers[0].closed


async def test_failed_close_is_logged_and_not_raised(log):
    first = add_open(log)
    last = add_result(log, 0)

    def factory():
        consumer = FakeConsumerClient(log)
        consumer.fail_next('close', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        return consumer

    results = [
        r
        async for r in read_results(
            consumer_factory=factory,
            topic=TOPIC,
            partition=PARTITION,
            job=job_state(first, last),
            group=GROUP,
            part_data_model=None,
        )
    ]
    assert len(results) == 1


def test_results_consumer_factory_builds_a_client_with_the_group_and_no_auto_commit(monkeypatch):
    built = {}

    class Recorder:
        def __init__(self, config, max_workers):
            built.update(config=config, max_workers=max_workers)

    monkeypatch.setattr(reader, 'AIOConsumer', Recorder)
    connection = KafkaConnectionConfig(brokers='broker:9092', client_config={'client.id': 'x'})
    results_consumer_factory(connection, 'g_split_results')()
    config = built['config']
    assert (config['bootstrap.servers'], config['group.id'], config['enable.auto.commit']) == (
        'broker:9092',
        'g_split_results',
        False,
    )
    assert config['client.id'] == 'x'
