"""The status follower of HTTP split jobs, and the parts of the split runtime it brings."""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from drakkar.config.connections import KafkaConnectionConfig
from drakkar.split import http_follower
from drakkar.split.http_follower import StatusFollower
from drakkar.split.http_status import PartCounts, StatusMap, StatusRecord, status_record
from drakkar.split.records import WireRecord
from drakkar.split.runtime import SplitRuntime
from drakkar.uiserver.routes_health import create_health_router
from tests.split_helpers import FakeConsumerClient, FakeLog, FakeProducerClient, wait_until

STATUS_TOPIC = 'items_status'
RESULTS_GROUP = 'g_split_results'
NOW = 1_800_000_000.0
RETENTION = 3600
KIB = 1024


class StatusConsumer(FakeConsumerClient):
    """The fake asyncio consumer with topic metadata and a record of its settings and commits."""

    def __init__(self, log: FakeLog, config: dict, partitions: int) -> None:
        super().__init__(log, group_id=config['group.id'])
        self.config = config
        self.partitions = partitions
        self.commits = 0

    async def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        self._check_open('list_topics')
        found = SimpleNamespace(partitions=dict.fromkeys(range(self.partitions)), error=None)
        return SimpleNamespace(topics={topic: found})

    async def commit(self, *args: Any, **kwargs: Any) -> None:
        self.commits += 1
        await super().commit(*args, **kwargs)


class Clock:
    """A wall clock the test moves by hand."""

    def __init__(self, now: float = NOW) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class World:
    """A log, a clock and the follower over them; ``consumers`` holds every consumer the follower built."""

    def __init__(self, *, partitions: int = 2, sleep: Any = None) -> None:
        self.log = FakeLog()
        self.clock = Clock()
        self.partitions = partitions
        self.consumers: list[StatusConsumer] = []
        self.sleeps: list[float] = []
        self.map = StatusMap(RETENTION)
        self.follower = StatusFollower(
            connection=KafkaConnectionConfig(brokers='broker:9092'),
            topic=STATUS_TOPIC,
            group=RESULTS_GROUP,
            status_map=self.map,
            consumer_factory=self._consumer,
            clock=self.clock,
            sleep=sleep or self._sleep,
        )

    def _consumer(self, config: dict) -> StatusConsumer:
        consumer = StatusConsumer(self.log, config, self.partitions)
        self.consumers.append(consumer)
        return consumer

    async def _sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        await asyncio.sleep(0.001)

    def write(self, record: StatusRecord, partition: int = 0) -> None:
        wire = status_record(record)
        self.log.append(STATUS_TOPIC, partition, key=wire.key, value=wire.value, headers=wire.headers)


def status(request_id: str, state: str = 'accepted', *, offset: int = 500, result: str = '{"n":1}', **fields: Any):
    if state == 'done':
        fields.setdefault('result_json', result)
    if state == 'failed':
        fields.setdefault('outcome', 'timeout')
    return StatusRecord(
        request_id=request_id,
        state=state,  # ty: ignore[invalid-argument-type]
        partition=7,
        offset=offset,
        client=fields.pop('client', 'tenant-a'),
        updated_at=fields.pop('updated_at', NOW),
        **fields,
    )


@pytest.fixture
async def world():
    built = World()
    yield built
    await built.follower.stop()


async def restored(world: World) -> None:
    world.follower.start()
    await asyncio.wait_for(world.follower.restored.wait(), 5)


async def test_follower_restores_to_the_high_watermarks_then_sets_restored(world):
    world.write(status('r1'), partition=0)
    world.write(status('r1', 'running', parts=PartCounts(total=None, received=0, failed=0)), partition=0)
    world.write(status('r2'), partition=1)

    with capture_logs() as logs:
        await restored(world)

    assert world.follower.is_restored
    assert world.follower.get('r1').state == 'running'
    assert world.follower.get('r2').state == 'accepted'
    [line] = [line for line in logs if line['event'] == 'split_http_status_restored']
    assert (line['log_level'], line['category'], line['records'], line['jobs']) == ('info', 'split', 3, 2)
    assert line['seconds'] >= 0


async def test_follower_restore_of_an_empty_topic_ends_at_once(world):
    await restored(world)
    assert world.follower.is_restored and len(world.map) == 0


async def test_follower_restore_ends_when_the_log_was_cut_below_its_end(world):
    world.write(status('r1'), partition=0)
    world.write(status('r2'), partition=0)
    world.log.raise_low(STATUS_TOPIC, 0, 2)
    await restored(world)
    assert world.follower.get('r1') is None


async def test_follower_restore_retries_after_failure(world):
    world.write(status('r1'))
    original = world._consumer

    def failing_once(config: dict) -> StatusConsumer:
        consumer = original(config)
        consumer.fail_next('get_watermark_offsets', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        consumer.fail_next('list_topics', KafkaException(KafkaError(KafkaError._TRANSPORT)))
        return consumer

    world.follower._consumer_factory = failing_once
    with capture_logs() as logs:
        await restored(world)

    assert world.sleeps[:2] == [1.0, 2.0]
    assert world.follower.get('r1') is not None
    failures = [line for line in logs if line['event'] == 'split_http_status_restore_failed']
    assert failures and failures[0]['log_level'] == 'warning' and failures[0]['category'] == 'split'


def test_restore_backoff_doubles_up_to_thirty_seconds():
    assert [http_follower._restore_backoff(n) for n in (1, 2, 3, 5, 6, 9)] == [1, 2, 4, 16, 30, 30]


async def test_follower_applies_live_records_to_the_map(world):
    await restored(world)
    world.write(status('r1'))
    world.write(status('r1', 'done'), partition=1)

    await wait_until(lambda: world.follower.get('r1') is not None and world.follower.get('r1').state == 'done')
    assert world.follower.get('r1').result_json == '{"n":1}'


async def test_follower_skips_and_logs_invalid_records_rate_limited(world):
    await restored(world)
    with capture_logs() as logs:
        for _ in range(3):
            world.log.append(STATUS_TOPIC, 1, key=b'r9', value=b'not json', headers=[('dk-status-version', b'1')])
        world.write(status('r1'))
        await wait_until(lambda: world.follower.get('r1') is not None)

    invalid = [line for line in logs if line['event'] == 'split_http_status_record_invalid']
    assert len(invalid) == 1
    assert (invalid[0]['log_level'], invalid[0]['category']) == ('warning', 'split')
    assert (invalid[0]['partition'], invalid[0]['offset'], invalid[0]['suppressed']) == (1, 0, 0)
    assert world.follower.get('r9') is None


async def test_follower_wait_for_returns_record_that_arrives_within_timeout(world):
    await restored(world)
    waiter = asyncio.ensure_future(world.follower.wait_for('r1', 5.0, client='tenant-a'))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    world.write(status('r1'))
    found = await asyncio.wait_for(waiter, 5)
    assert found is not None and found.state == 'accepted'


async def test_follower_wait_for_returns_a_stored_record_at_once(world):
    world.write(status('r1'))
    await restored(world)
    assert (await world.follower.wait_for('r1', 0.0, client='tenant-a')).request_id == 'r1'


async def test_follower_wait_for_returns_none_after_timeout(world):
    await restored(world)
    assert await world.follower.wait_for('r1', 0.05, client='tenant-a') is None
    assert world.follower._waiting == 0


async def test_follower_wait_for_over_cap_returns_at_once(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'MAX_STATUS_WAITERS', 1)
    await restored(world)
    first = asyncio.ensure_future(world.follower.wait_for('r1', 5.0, client='tenant-a'))
    await asyncio.sleep(0.01)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await world.follower.wait_for('r2', 5.0, client='tenant-a') is None
    assert loop.time() - started < 1.0
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    assert world.follower._waiting == 0


async def test_follower_wait_for_ends_only_on_a_record_of_its_client(world):
    world.write(status('r1', client='tenant-b'))
    await restored(world)
    # A stored record of another client answers nothing at once: the read waits as for an unknown id.
    waiter = asyncio.ensure_future(world.follower.wait_for('r1', 5.0, client='tenant-a'))
    await asyncio.sleep(0.01)
    world.write(status('r1', 'running', client='tenant-b'))
    await wait_until(lambda: world.follower.get('r1').state == 'running')
    await asyncio.sleep(0.01)
    assert not waiter.done()

    world.write(status('r1', 'done', client='tenant-a'))
    found = await asyncio.wait_for(waiter, 5)
    assert found is not None and (found.state, found.client) == ('done', 'tenant-a')
    assert world.follower._waiting == 0 and world.follower._waiters == {}


async def test_follower_wait_for_of_another_client_stays_inside_the_cap(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'MAX_STATUS_WAITERS', 1)
    world.write(status('r1', client='tenant-b'))
    await restored(world)
    first = asyncio.ensure_future(world.follower.wait_for('r1', 5.0, client='tenant-a'))
    await asyncio.sleep(0.01)
    assert not first.done() and world.follower._waiting == 1
    assert await world.follower.wait_for('r1', 5.0, client='tenant-a') is None
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    assert world.follower._waiting == 0


async def test_follower_stop_ends_every_wait_with_none(world):
    await restored(world)
    waiter = asyncio.ensure_future(world.follower.wait_for('r1', 5.0, client='tenant-a'))
    await asyncio.sleep(0.01)
    await world.follower.stop()
    assert await asyncio.wait_for(waiter, 5) is None


async def _sizes_while_restoring(world: World, count: int) -> list[int]:
    """Restore, and note the size of the map at every turn of the loop until it holds ``count`` entries."""
    sizes: list[int] = []

    async def watch() -> None:
        while len(world.map) < count:
            sizes.append(len(world.map))
            await asyncio.sleep(0)
        sizes.append(len(world.map))

    watcher = asyncio.ensure_future(watch())
    await restored(world)
    await asyncio.wait_for(watcher, 5)
    return sizes


async def test_follower_yields_between_chunks(world):
    big = '"' + 'x' * (64 * KIB) + '"'
    for index in range(200):
        world.write(status(f'r{index}', 'done', result=big), partition=index % 2)
    sizes = await _sizes_while_restoring(world, 200)
    partial = {size for size in sizes if 0 < size < 200}
    assert len(partial) > 1


async def test_follower_applies_small_records_in_chunks_of_at_most_64(world):
    # 200 small records of one partition come in one batch; only the record count makes the chunks.
    for index in range(200):
        world.write(status(f'r{index}'))
    sizes = await _sizes_while_restoring(world, 200)
    steps = [after - before for before, after in itertools.pairwise(sizes) if after != before]
    assert {size for size in sizes if 0 < size < 200}
    assert steps and max(steps) <= http_follower.CHUNK_RECORDS


async def test_follower_never_commits_and_uses_the_results_group(world):
    world.write(status('r1'))
    await restored(world)
    world.write(status('r2'))
    await wait_until(lambda: world.follower.get('r2') is not None)

    [consumer] = world.consumers
    assert consumer.config['group.id'] == RESULTS_GROUP
    assert consumer.config['enable.auto.commit'] is False
    assert consumer.config['auto.offset.reset'] == 'earliest'
    assert consumer.config['fetch.wait.max.ms'] == 100
    assert consumer.config['bootstrap.servers'] == 'broker:9092'
    assert consumer.commits == 0
    assert world.log.committed(RESULTS_GROUP, STATUS_TOPIC, 0) is None


async def test_follower_sweeps_expired_entries(world):
    world.write(status('r1', 'done'))
    world.write(status('r2'))
    await restored(world)
    assert world.follower.get('r1') is not None

    world.clock.now = NOW + RETENTION + 1
    await wait_until(lambda: world.follower.get('r1') is None)
    assert world.follower.get('r2') is not None
    assert http_follower.EXPIRY_SWEEP_SECONDS in world.sleeps


def gauge(state: str) -> float | None:
    return REGISTRY.get_sample_value('drakkar_split_http_jobs', {'state': state})


async def test_jobs_gauge_by_state(world):
    world.write(status('r1'))
    world.write(status('r2', 'running', parts=PartCounts(total=3, received=1, failed=0)))
    world.write(status('r3', 'done'))
    world.write(status('r4', 'failed'))
    await restored(world)
    assert [gauge(state) for state in ('accepted', 'running', 'done', 'failed')] == [1, 1, 1, 1]

    world.write(status('r1', 'done'))
    await wait_until(lambda: gauge('done') == 2)
    assert gauge('accepted') == 0

    await world.follower.stop()
    assert [gauge(state) for state in ('accepted', 'running', 'done', 'failed')] == [0, 0, 0, 0]


async def test_wait_restored_answers_false_when_stop_comes_first(world):
    world.follower._consumer_factory = lambda config: (_ for _ in ()).throw(RuntimeError('down'))
    world.follower.start()
    stop = asyncio.Event()
    stop.set()
    assert await world.follower.wait_restored(stop) is False


async def test_wait_restored_answers_true_once_restored(world):
    await restored(world)
    assert await world.follower.wait_restored(asyncio.Event()) is True


async def test_follower_logs_a_consume_failure_and_reads_on(world):
    await restored(world)
    [consumer] = world.consumers
    consumer.fail_next('consume', KafkaException(KafkaError(KafkaError._TRANSPORT)))
    consumer.inject_error(KafkaError(KafkaError._TRANSPORT))
    with capture_logs() as logs:
        world.write(status('r1'))
        await wait_until(lambda: world.follower.get('r1') is not None)
    assert [line for line in logs if line['event'] == 'split_http_status_consume_failed']


async def test_follower_stop_twice_and_without_start_does_nothing():
    built = World()
    await built.follower.stop()
    await built.follower.stop()


# --- the split runtime ---

SOURCE_TOPIC = 'items'
PARTS_TOPIC = 'items_g_split_parts'
RESULTS_TOPIC = 'items_g_split_results'


def runtime_config(*, http: bool, split_connection: str = 'main', **status: Any) -> DrakkarConfig:
    return DrakkarConfig.model_validate(
        {
            'connections': {'kafka': {'main': {}, 'internal': {'brokers': 'internal:9092'}}},
            'sources': {
                'kafka': {'enabled': True, 'topic': SOURCE_TOPIC, 'consumer_group': 'g'},
                'http': {'deliver_to_sinks': True},
            },
            'split': {
                'enabled': True,
                'connection': split_connection,
                'http': {'enabled': http, 'status_topic': STATUS_TOPIC if http else '', **status},
            },
            'ui': {'release': {'enabled': False}},
        }
    )


class RuntimeParts:
    """What a test gives a ``SplitRuntime`` in place of the broker."""

    def __init__(self, *, cleanup: str | None = 'delete', retention_ms: int | None = None) -> None:
        self.log = FakeLog()
        self.clients: list[FakeProducerClient] = []
        self.consumers: list[StatusConsumer] = []
        self.counts = {SOURCE_TOPIC: 2, PARTS_TOPIC: 2, RESULTS_TOPIC: 2, STATUS_TOPIC: 3}
        self.cleanup = cleanup
        self.retention_ms = retention_ms

    def _producer(self, config: dict) -> FakeProducerClient:
        client = FakeProducerClient(self.log)
        client.config = config
        self.clients.append(client)
        return client

    def _consumer(self, config: dict) -> StatusConsumer:
        consumer = StatusConsumer(self.log, config, self.counts[STATUS_TOPIC])
        self.consumers.append(consumer)
        return consumer

    def runtime(self, config: DrakkarConfig, *, http_enabled: bool) -> SplitRuntime:
        async def metadata():
            return self.counts

        async def retention(topic: str):
            return self.retention_ms if topic == STATUS_TOPIC else None

        async def cleanup_policy(topic: str):
            return self.cleanup

        return SplitRuntime(
            config,
            worker='w1',
            instance='0123456789abcdef',
            producer_factory=self._producer,
            metadata=metadata,
            retention=retention,
            cleanup_policy=cleanup_policy,
            consumer_factory=self._consumer,
            results_reader=lambda: None,
            http_enabled=http_enabled,
        )


@pytest.mark.parametrize(('http', 'http_enabled'), [(False, False), (True, False), (False, True)])
async def test_runtime_without_http_builds_no_follower(http, http_enabled):
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=http), http_enabled=http_enabled)
    with capture_logs() as logs:
        await runtime.start()
    await runtime.stop(deadline=0.0)

    assert runtime.http is None
    assert parts.consumers == []
    assert len(parts.clients) == 1
    assert not [line for line in logs if line['event'].startswith('split_http')]


async def test_runtime_with_http_follows_the_status_topic_and_says_so():
    parts = RuntimeParts(cleanup='compact', retention_ms=60_000)
    runtime = parts.runtime(runtime_config(http=True, result_retention='2h', max_response='2KiB'), http_enabled=True)
    with capture_logs() as logs:
        await runtime.start()
    http = runtime.http
    assert http is not None
    await asyncio.wait_for(http.follower.restored.wait(), 5)
    await runtime.stop(deadline=0.0)

    assert (http.status_topic, http.source_topic, http.result_retention, http.max_response) == (
        STATUS_TOPIC,
        SOURCE_TOPIC,
        7200,
        2048,
    )
    assert http.deliver_sinks_default is True
    assert http.follower.get('nothing') is None and http.status_map is http.follower.status_map
    # One connection for the source and the split topics: one producer for both, and one for the status records.
    assert http.source_producer is runtime.producer and len(parts.clients) == 2
    assert http.status_producer is not runtime.producer
    [consumer] = parts.consumers
    assert consumer.config['group.id'] == runtime.results_group
    assert consumer.closed
    [enabled] = [line for line in logs if line['event'] == 'split_http_enabled']
    assert (enabled['category'], enabled['status_topic'], enabled['result_retention_seconds']) == (
        'split',
        STATUS_TOPIC,
        7200,
    )
    details = [line['detail'] for line in logs if line['event'] == 'split_topic_retention_short']
    assert any('cleanup.policy=compact' in detail for detail in details)
    assert any('keeps records for 1m' in detail for detail in details)


async def test_runtime_with_http_on_another_connection_writes_the_source_with_a_producer_of_its_own():
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=True, split_connection='internal'), http_enabled=True)
    await runtime.start()
    http = runtime.http
    assert http is not None
    assert http.source_producer is not runtime.producer
    thread = http.source_producer._thread
    await runtime.stop(deadline=0.0)

    brokers = sorted(client.config['bootstrap.servers'] for client in parts.clients)
    assert brokers == ['internal:9092', 'internal:9092', 'localhost:9092']
    assert thread is not None and not thread.is_alive()
    assert parts.consumers[0].config['bootstrap.servers'] == 'internal:9092'


async def test_runtime_with_http_writes_status_records_through_a_bounded_producer_of_their_own():
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=True), http_enabled=True)
    await runtime.start()
    http = runtime.http
    assert http is not None
    status_thread = http.status_producer._thread
    [split_client, status_client] = parts.clients
    await runtime.stop(deadline=0.0)

    assert http.writer._producer is http.status_producer
    assert status_client.config['message.timeout.ms'] == 30_000
    assert status_client.config['queue.buffering.max.messages'] == 10_000
    assert status_client.config['queue.buffering.max.kbytes'] == 65_536
    assert 'message.timeout.ms' not in split_client.config
    assert status_thread is not None and status_thread.name == 'split-status-producer' and not status_thread.is_alive()


async def test_a_full_status_queue_does_not_delay_a_part_result_write():
    parts = RuntimeParts()
    runtime = parts.runtime(runtime_config(http=True), http_enabled=True)
    await runtime.start()
    http = runtime.http
    assert http is not None
    [_, status_client] = parts.clients
    status_client.queue_capacity = 0
    try:
        running = StatusRecord(
            request_id='r1', state='running', partition=0, offset=5, client='tenant-a', updated_at=NOW
        )
        with capture_logs() as logs:
            http.writer.write_best_effort(running)
            assert not await http.writer.write_bounded(dataclasses.replace(running, state='accepted'), 5.0)
            offset = await asyncio.wait_for(
                runtime.producer.publish(RESULTS_TOPIC, WireRecord(key=b'0:5', value=b'{}', headers=[], partition=0)),
                2,
            )
        assert offset >= 0
        errors = [line['error'] for line in logs if line['event'] == 'split_http_status_write_failed']
        assert errors == ['the client queue is full']
    finally:
        await runtime.stop(deadline=0.0)


async def test_runtime_with_http_fails_on_a_missing_status_topic():
    parts = RuntimeParts()
    del parts.counts[STATUS_TOPIC]
    runtime = parts.runtime(runtime_config(http=True), http_enabled=True)
    with pytest.raises(Exception, match=f"the topic '{STATUS_TOPIC}' does not exist"):
        await runtime.start()
    await runtime.stop(deadline=0.0)
    assert parts.consumers == []


async def test_follower_restore_passes_offsets_that_hold_no_record(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'RESTORE_END_CHECK_SECONDS', 0.0)
    world.write(status('r1'))
    world.log.skip(STATUS_TOPIC, 0, 2)
    await restored(world)
    assert world.follower.get('r1') is not None


async def test_follower_restore_checks_ends_on_time_while_other_partitions_bring_records(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'RESTORE_END_CHECK_SECONDS', 0.05)
    # Partition 0 ends in offsets that hold no record; partition 1 brings a record with every read.
    world.write(status('r1'), partition=0)
    world.log.skip(STATUS_TOPIC, 0, 2)
    world.write(status('busy-0'), partition=1)
    build = world._consumer
    written = iter(range(1, 10**9))

    def busy(config: dict) -> StatusConsumer:
        consumer = build(config)
        consume = consumer.consume

        async def consume_with_traffic(num_messages: int = 1, timeout: float = -1) -> Any:
            world.write(status(f'busy-{next(written)}'), partition=1)
            return await consume(num_messages=num_messages, timeout=timeout)

        consumer.consume = consume_with_traffic  # ty: ignore[invalid-assignment]
        return consumer

    world.follower._consumer_factory = busy
    await restored(world)
    assert world.follower.get('r1') is not None


async def test_follower_sweep_removes_a_backlog_larger_than_its_limit_in_one_sweep(monkeypatch):
    monkeypatch.setattr(http_follower, 'EXPIRY_SWEEP_LIMIT', 2)
    sweep_now = asyncio.Event()
    sweeps = 0

    async def gated_sleep(seconds: float) -> None:
        nonlocal sweeps
        if seconds == http_follower.EXPIRY_SWEEP_SECONDS:
            sweeps += 1
            await sweep_now.wait()
            sweep_now.clear()
        await asyncio.sleep(0.001)

    world = World(sleep=gated_sleep)
    for index in range(7):
        world.write(status(f'r{index}', 'done'))
    await restored(world)
    await wait_until(lambda: sweeps == 1)

    world.clock.now = NOW + RETENTION + 1
    sweep_now.set()
    await wait_until(lambda: sweeps == 2)
    assert len(world.map) == 0
    await world.follower.stop()


async def test_follower_restore_retries_while_the_topic_is_not_described(world):
    world.partitions = 0
    world.follower.start()
    await wait_until(lambda: len(world.sleeps) >= 2)
    assert not world.follower.is_restored
    world.consumers[0].partitions = 2
    await asyncio.wait_for(world.follower.restored.wait(), 5)


async def test_follower_waits_for_the_restore_and_starts_once(world):
    world.follower.start()
    world.follower.start()
    assert await asyncio.wait_for(world.follower.wait_restored(asyncio.Event()), 5) is True
    assert len(world.consumers) == 1


async def test_wait_restored_ends_when_stop_is_set_during_the_wait(world):
    world.partitions = 0
    world.follower.start()
    stop = asyncio.Event()
    waiting = asyncio.ensure_future(world.follower.wait_restored(stop))
    await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(waiting, 5) is False


async def test_follower_logs_a_failed_close(world):
    await restored(world)
    world.consumers[0].fail_next('close', RuntimeError('gone'))
    with capture_logs() as logs:
        await world.follower.stop()
    assert [line for line in logs if line['event'] == 'split_http_status_close_failed']


async def test_follower_logs_a_record_it_cannot_apply_and_applies_the_next(world, monkeypatch):
    await restored(world)
    original = world.map.put
    calls = []

    def put_failing_once(record, *, now):
        calls.append(record.request_id)
        if len(calls) == 1:
            raise RuntimeError('broken')
        return original(record, now=now)

    monkeypatch.setattr(world.map, 'put', put_failing_once)
    with capture_logs() as logs:
        world.write(status('r1'))
        world.write(status('r2'))
        await wait_until(lambda: world.follower.get('r2') is not None)
    [line] = [line for line in logs if line['event'] == 'split_http_status_record_failed']
    assert (line['log_level'], line['partition'], line['offset']) == ('error', 0, 0)
    assert world.follower.get('r1') is None


async def test_follower_restarts_its_read_loop_after_an_unexpected_error(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'LOOP_RESTART_SECONDS', 0.0)
    await restored(world)
    original = world.follower._set_gauge
    calls = []

    def gauge_failing_once():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('broken')
        original()

    monkeypatch.setattr(world.follower, '_set_gauge', gauge_failing_once)
    with capture_logs() as logs:
        world.write(status('r1'))
        await wait_until(lambda: len(calls) >= 1)
        world.write(status('r2'))
        await wait_until(lambda: len(calls) >= 2)
    [line] = [line for line in logs if line['event'] == 'split_http_status_loop_failed']
    assert (line['log_level'], line['loop']) == ('error', 'read')


async def test_follower_keeps_the_end_state_when_an_older_state_arrives_late(world):
    await restored(world)
    world.write(status('r1', 'done'))
    world.write(status('r1'))
    await wait_until(lambda: world.consumers[0]._positions.get((STATUS_TOPIC, 0)) == 2)
    assert world.follower.get('r1').state == 'done'


async def test_follower_restore_forgets_a_job_whose_end_expired_and_takes_a_new_job_of_its_id(world):
    old = NOW - RETENTION - 10
    world.write(status('r1', updated_at=old))
    world.write(status('r1', 'done', updated_at=old))
    # The best-effort ``accepted`` can follow the end state of a short job in the log.
    world.write(status('r1', updated_at=old))
    world.write(status('r1', 'running', updated_at=old, parts=PartCounts(total=None, received=0, failed=0)))
    await restored(world)

    assert world.follower.get('r1') is None
    assert gauge('accepted') == 0 and gauge('running') == 0

    world.write(status('r1', offset=900))
    await wait_until(lambda: world.follower.get('r1') is not None)
    assert (world.follower.get('r1').state, world.follower.get('r1').offset) == ('accepted', 900)


async def test_follower_sweep_starts_a_new_round_of_marks(world):
    await restored(world)
    world.map.put(status('r1', 'done', updated_at=NOW - RETENTION - 10), now=NOW)
    await world.follower._sweep()
    assert not world.map.put(status('r1'), now=NOW)
    await world.follower._sweep()
    assert world.map.put(status('r1'), now=NOW)


async def test_caught_up_is_false_before_the_restore(world):
    assert not await world.follower.caught_up(0.1)


async def test_caught_up_waits_until_the_follower_read_what_the_topic_held_at_the_call(world):
    await restored(world)
    world.write(status('r1'))
    world.write(status('r2'), partition=1)
    assert await world.follower.caught_up(5)
    assert world.follower.get('r1') is not None and world.follower.get('r2') is not None


async def test_caught_up_answers_false_when_the_time_runs_out(world, monkeypatch):
    await restored(world)
    world.write(status('r1'))
    monkeypatch.setattr(world.consumers[0], 'consume', _reads_nothing)
    assert not await world.follower.caught_up(0.2)


async def _reads_nothing(*args: Any, **kwargs: Any) -> list[Any]:
    await asyncio.sleep(0.01)
    return []


async def test_caught_up_answers_false_when_the_broker_fails_and_logs_it(world, monkeypatch):
    await restored(world)

    async def refuse(*args: Any, **kwargs: Any) -> Any:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, 'broker down'))

    monkeypatch.setattr(world.consumers[0], 'get_watermark_offsets', refuse)
    with capture_logs() as logs:
        assert not await world.follower.caught_up(1)
    [line] = [line for line in logs if line['event'] == 'split_http_status_catch_up_failed']
    assert (line['log_level'], line['category']) == ('warning', 'split')


async def test_caught_up_ends_with_false_at_a_stop(world, monkeypatch):
    await restored(world)
    world.write(status('r1'))
    monkeypatch.setattr(world.consumers[0], 'consume', _reads_nothing)
    waiting = asyncio.ensure_future(world.follower.caught_up(5))
    await asyncio.sleep(0.05)
    await world.follower.stop()
    assert await asyncio.wait_for(waiting, 2) is False


async def test_readyz_names_a_status_map_that_is_not_restored():
    follower = SimpleNamespace(is_restored=False)
    app = SimpleNamespace(
        _split_http_enabled=True,
        _split=SimpleNamespace(http=SimpleNamespace(follower=follower)),
        is_ready=True,
        sink_manager=SimpleNamespace(all_connected=lambda: True),
        processors={},
        parts_source=None,
    )
    app.split_http_status_restored = DrakkarApp.split_http_status_restored.fget(app)  # ty: ignore[invalid-argument-type]
    api = FastAPI()
    api.include_router(create_health_router(app))  # ty: ignore[invalid-argument-type]
    async with AsyncClient(transport=ASGITransport(app=api), base_url='http://test') as client:
        response = await client.get('/readyz')
    assert (response.status_code, response.json()) == (
        503,
        {'status': 'not_ready', 'reasons': ['split_http_status_not_restored']},
    )

    follower.is_restored = True
    app.split_http_status_restored = DrakkarApp.split_http_status_restored.fget(app)  # ty: ignore[invalid-argument-type]
    async with AsyncClient(transport=ASGITransport(app=api), base_url='http://test') as client:
        response = await client.get('/readyz')
    assert response.status_code == 200


@pytest.mark.parametrize(('enabled', 'split'), [(False, SimpleNamespace(http=None)), (True, None)])
def test_split_http_status_counts_as_restored_without_the_feature(enabled, split):
    app = SimpleNamespace(_split_http_enabled=enabled, _split=split)
    assert DrakkarApp.split_http_status_restored.fget(app) is True  # ty: ignore[invalid-argument-type]


async def test_caught_up_waits_for_the_batch_behind_the_read_position(world, monkeypatch):
    # The client moves its position past the whole batch when ``consume`` returns; the batch is applied in
    # chunks of 64 with a turn of the loop between them. A wait that starts in such a pause must not answer
    # before the record it waits for, which sits past the first chunk, is in the map.
    await restored(world)
    consumer = world.consumers[0]
    original = consumer.consume
    answers: list[tuple[bool, StatusRecord | None]] = []
    checks: list[asyncio.Task[None]] = []

    async def check() -> None:
        reached = await world.follower.caught_up(5)
        answers.append((reached, world.follower.get('needed')))

    async def consume_then_check(num_messages: int = 1, timeout: float = -1) -> Any:
        messages = await original(num_messages=num_messages, timeout=timeout)
        if messages and not checks:
            checks.append(asyncio.ensure_future(check()))
        return messages

    monkeypatch.setattr(consumer, 'consume', consume_then_check)
    for index in range(3 * http_follower.CHUNK_RECORDS):
        world.write(status(f'filler{index}'))
    world.write(status('needed'))
    await wait_until(lambda: bool(checks))
    await asyncio.wait_for(checks[0], 5)
    [(reached, found)] = answers
    assert reached
    assert found is not None


async def test_follower_restore_leaves_no_entry_for_an_ended_job_and_its_ended_duplicate(world):
    old = NOW - 2 * RETENTION
    world.write(status('r1', offset=500, updated_at=old))
    world.write(status('r1', 'done', offset=500, updated_at=old))
    world.write(status('r1', offset=900, updated_at=old))
    world.write(
        status(
            'r1',
            'failed',
            offset=900,
            outcome='duplicate',
            error='another request with this request_id was stored first',
            updated_at=old,
        )
    )
    await restored(world)
    assert world.follower.get('r1') is None
    assert sum(world.map.counts().values()) == 0


async def test_caught_up_answers_false_at_once_for_a_while_after_a_wait_that_ran_out(world, monkeypatch):
    monkeypatch.setattr(http_follower, 'CATCH_UP_SKIP_SECONDS', 0.3)
    await restored(world)
    world.write(status('r1'))
    consumer = world.consumers[0]
    monkeypatch.setattr(consumer, 'consume', _reads_nothing)
    assert not await world.follower.caught_up(0.1)

    asked: list[int] = []
    original = consumer.get_watermark_offsets

    async def counted(*args: Any, **kwargs: Any) -> Any:
        asked.append(1)
        return await original(*args, **kwargs)

    monkeypatch.setattr(consumer, 'get_watermark_offsets', counted)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert not await world.follower.caught_up(5)
    assert loop.time() - started < 0.05 and asked == []

    await asyncio.sleep(0.35)
    monkeypatch.undo()
    assert await world.follower.caught_up(5)


async def test_caught_up_after_a_failed_broker_call_skips_the_next_waits(world, monkeypatch):
    await restored(world)

    async def refuse(*args: Any, **kwargs: Any) -> Any:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, 'broker down'))

    monkeypatch.setattr(world.consumers[0], 'get_watermark_offsets', refuse)
    assert not await world.follower.caught_up(1)
    monkeypatch.undo()
    assert not await world.follower.caught_up(1)
