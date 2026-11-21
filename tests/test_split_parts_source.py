"""The parts source, the split runtime and their place in the worker: registry, lifecycle, detach, consume pause."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

import drakkar as dk
from drakkar.app import (
    DrakkarApp,
    changed_consumed_settings,
    snapshot_consumed_settings,
)
from drakkar.config import DrakkarConfig
from drakkar.consumer import KafkaConsumer
from drakkar.metrics import assigned_partitions, offsets_committed, split_parts_processed
from drakkar.models import ConfigurationError, ExecutorTask, PrecomputedResult, SourceMessage
from drakkar.sources.http import HttpSource
from drakkar.sources.kafka import KafkaSource, LeaveOutcome
from drakkar.sources.parts import PartsSource, SourcePair
from drakkar.sources.registry import build_sources
from drakkar.split.partmode import ResultWriter
from drakkar.split.records import PARTS_PARTITION_BASE, JobRef, parse_log_record, part_record
from drakkar.split.runtime import SplitRuntime
from tests.conftest import wait_for
from tests.split_helpers import FakeLog, FakeProducerClient
from tests.stream_helpers import make_pool
from tests.test_sources_validation import In, KafkaOnlyHandler, Out

SOURCE_TOPIC = 'orders'
GROUP = 'orders-workers'
PARTS_TOPIC = 'orders_orders-workers_split_parts'
RESULTS_TOPIC = 'orders_orders-workers_split_results'
PARTS_GROUP = 'orders-workers_split_parts'
RESULTS_GROUP = 'orders-workers_split_results'
WORKER = 'worker-a'


class SplittingHandler(dk.BaseDrakkarHandler[In, Out]):
    """A handler that splits; its tasks are precomputed, so no subprocess runs."""

    def __init__(self) -> None:
        self.assigned: list[list[int]] = []
        self.revoked: list[list[int]] = []

    async def split(self, message):
        return
        yield

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{message.offset}',
                source_offsets=[message.offset],
                precomputed=PrecomputedResult(stdout='ok', exit_code=0),
            )
            for message in messages
        ]

    async def on_assign(self, partitions):
        self.assigned.append(partitions)

    async def on_revoke(self, partitions):
        self.revoked.append(partitions)


def split_config(*, enabled: bool = True, **overrides: Any) -> DrakkarConfig:
    values: dict[str, Any] = {
        'connections': {'kafka': {'main': {}, 'internal': {'brokers': 'internal:9092'}}},
        'sources': {
            'kafka': {
                'enabled': True,
                'topic': SOURCE_TOPIC,
                'consumer_group': GROUP,
                'max_poll_records': 77,
                'session_timeout': '50s',
                'startup_align': {'enabled': False},
            }
        },
        'split': {'enabled': enabled, 'connection': 'internal'},
        'storage': {'dir': ''},
        'recorder': {'enabled': False},
        'api': {'enabled': False},
        'ui': {'enabled': False, 'release': {'enabled': False}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout': '1s'},
        'sinks': {'kafka': {'output_results': {'topic': 'output-results'}}},
    }
    values.update(overrides)
    return DrakkarConfig.model_validate(values)


def split_app(handler: Any = None, **config: Any) -> DrakkarApp:
    return DrakkarApp(handler=handler or SplittingHandler(), config=split_config(**config), worker_id=WORKER)


def topic_counts(**changes: int | None) -> dict[str, int]:
    counts: dict[str, int | None] = {SOURCE_TOPIC: 4, PARTS_TOPIC: 8, RESULTS_TOPIC: 4, **changes}
    return {topic: count for topic, count in counts.items() if count is not None}


class FakeRuntimeParts:
    """What a test gives a ``SplitRuntime`` in place of the broker: a log, a producer client and the topic facts."""

    def __init__(self, counts: dict[str, int] | None = None, retentions: dict[str, int | None] | None = None) -> None:
        self.log = FakeLog()
        self.client = FakeProducerClient(self.log)
        self.counts = topic_counts() if counts is None else counts
        self.retentions = retentions or {}

    def runtime(self, config: DrakkarConfig) -> SplitRuntime:
        async def metadata():
            return self.counts

        async def retention(topic: str):
            return self.retentions.get(topic)

        def factory(client_config: dict) -> FakeProducerClient:
            self.client.config = client_config
            return self.client

        return SplitRuntime(
            config,
            worker=WORKER,
            instance='0123456789abcdef',
            producer_factory=factory,
            metadata=metadata,
            retention=retention,
        )


def fake_consumer() -> MagicMock:
    fake = MagicMock()
    for method in ('subscribe', 'unsubscribe', 'close', 'commit', 'pause', 'resume'):
        setattr(fake, method, AsyncMock())
    fake.poll_batch = AsyncMock(return_value=[])
    return fake


async def wired(app: DrakkarApp, parts: FakeRuntimeParts | None = None) -> SplitRuntime:
    """Start the runtime with fakes and bind the sources, as the startup does; each source gets a fake consumer."""
    parts = parts or FakeRuntimeParts()
    runtime = parts.runtime(app.config)
    await runtime.start()
    app._split = runtime
    app._executor_pool = make_pool()
    app._lifecycle._bind_sources()
    for source in (app.kafka_source, app.parts_source):
        assert source is not None
        source.consumer = fake_consumer()
    return runtime


def part_message(offset: int = 0, partition: int = 1) -> SourceMessage:
    ref = JobRef(group=GROUP, partition=3, offset=40, attempt=1, attempt_id='cd' * 8)
    record = part_record(
        ref,
        source_topic=SOURCE_TOPIC,
        index=offset,
        request_id='req-original',
        key=None,
        value=b'{"x": 1}',
        source_headers=[],
        partition_count=4,
    )
    return SourceMessage(
        topic=PARTS_TOPIC,
        partition=partition,
        offset=offset,
        value=b'{"x": 1}',
        timestamp=0,
        headers=list(record.headers),
    )


# --- the registry and the app ---


def test_the_registry_builds_the_parts_source_after_the_kafka_source():
    app = split_app()

    assert [type(source) for source in app.sources] == [KafkaSource, PartsSource]
    assert [source.name for source in app.sources] == ['kafka', 'parts']
    assert type(app.kafka_source) is KafkaSource
    assert isinstance(app.parts_source, PartsSource)


def test_the_parts_source_comes_before_the_http_source():
    config = split_config()
    config.sources.http.enabled = True
    app = MagicMock()
    app._config = config
    app._handler = SplittingHandler()

    assert [type(source) for source in build_sources(app)] == [KafkaSource, PartsSource, HttpSource]


def test_no_parts_source_with_the_feature_off_or_a_handler_that_does_not_split():
    off = split_app(enabled=False)
    assert [type(source) for source in off.sources] == [KafkaSource]
    assert off.parts_source is None

    app = MagicMock()
    app._config = split_config()
    app._handler = KafkaOnlyHandler()
    assert [type(source) for source in build_sources(app)] == [KafkaSource]


def test_a_change_of_split_enabled_in_on_startup_is_reported_as_ignored():
    config = split_config(enabled=False)
    before = snapshot_consumed_settings(config)

    config.split.enabled = True

    assert changed_consumed_settings(before, config) == ['split.enabled']


# --- the runtime ---


async def test_the_runtime_start_checks_the_topics_and_says_the_feature_is_on():
    parts = FakeRuntimeParts(retentions={PARTS_TOPIC: 3_600_000, RESULTS_TOPIC: -1})
    runtime = parts.runtime(split_config())

    with capture_logs() as logs:
        await runtime.start()
    await runtime.stop(deadline=0.0)

    assert (runtime.parts_topic, runtime.results_topic) == (PARTS_TOPIC, RESULTS_TOPIC)
    assert (runtime.parts_group, runtime.results_group, runtime.source_group) == (PARTS_GROUP, RESULTS_GROUP, GROUP)
    assert (runtime.worker, runtime.instance) == (WORKER, '0123456789abcdef')
    # The producer uses the connection of the split topics.
    assert parts.client.config['bootstrap.servers'] == 'internal:9092'
    [enabled] = [line for line in logs if line['event'] == 'split_enabled']
    assert (enabled['log_level'], enabled['category']) == ('info', 'split')
    assert (enabled['parts_topic'], enabled['results_topic']) == (PARTS_TOPIC, RESULTS_TOPIC)
    assert (enabled['parts_group'], enabled['results_group']) == (PARTS_GROUP, RESULTS_GROUP)
    [short] = [line for line in logs if line['event'] == 'split_topic_retention_short']
    assert (short['log_level'], short['category']) == ('warning', 'split')
    assert PARTS_TOPIC in short['detail'] and 'split.job_timeout' in short['detail']


async def test_the_runtime_start_fails_on_a_missing_topic_and_stop_still_closes_the_producer():
    parts = FakeRuntimeParts(counts=topic_counts(**{RESULTS_TOPIC: None}))
    runtime = parts.runtime(split_config())

    with pytest.raises(ConfigurationError, match=f"the topic '{RESULTS_TOPIC}' does not exist"):
        await runtime.start()
    thread = runtime.producer._thread
    await runtime.stop(deadline=0.0)

    assert thread is not None and not thread.is_alive()


async def test_the_runtime_stop_delivers_what_the_producer_still_holds():
    parts = FakeRuntimeParts()
    runtime = parts.runtime(split_config())
    await runtime.start()
    parts.client.hold_deliveries = True
    writer = ResultWriter(runtime.producer, results_topic=RESULTS_TOPIC, stopping=lambda: False)
    message = part_message()
    from drakkar.split.records import parse_part_headers
    from tests.test_split_part_mode import ok_result

    message.split = parse_part_headers(message.headers)
    write = asyncio.create_task(writer.write(message, ok_result()))
    await wait_for(lambda: parts.client.produced == 1)
    parts.client.release_deliveries()

    await runtime.stop(deadline=0.0)
    await asyncio.wait_for(write, 1)

    assert len(parts.log.records(RESULTS_TOPIC, 3)) == 1


# --- the parts source ---


async def test_the_parts_consumer_reads_the_parts_topic_with_the_poll_settings_of_the_source(monkeypatch):
    app = split_app(
        sources={
            'kafka': {
                'enabled': True,
                'topic': SOURCE_TOPIC,
                'consumer_group': GROUP,
                'max_poll_records': 77,
                'session_timeout': '50s',
                # On for the source topic; the parts source must not wait for it again.
                'startup_align': {'enabled': True},
            }
        }
    )
    await wired(app)
    built: list[dict[str, Any]] = []
    waits: list[tuple] = []

    def build(**kwargs: Any) -> MagicMock:
        built.append(kwargs)
        return fake_consumer()

    async def no_wait(*args: Any) -> float:
        waits.append(args)
        return 0.0

    monkeypatch.setattr('drakkar.sources.kafka.KafkaConsumer', build)
    monkeypatch.setattr('drakkar.sources.kafka.wait_for_aligned_startup', no_wait)
    source = app.parts_source
    assert source is not None

    await source.start()
    await source.stop(deadline=0.0)

    [settings] = built
    assert waits == []
    assert settings['connection'].brokers == 'internal:9092'
    assert (settings['source'].topic, settings['source'].consumer_group) == (PARTS_TOPIC, PARTS_GROUP)
    assert (settings['source'].max_poll_records, settings['source'].session_timeout) == (77, 50)
    assert settings['partition_label_base'] == PARTS_PARTITION_BASE
    assert (settings['on_assign'], settings['on_revoke']) == (source.on_assign, source.on_revoke)
    # The source topic's own settings are untouched.
    assert app.config.sources.kafka.topic == SOURCE_TOPIC


async def test_the_parts_source_cannot_start_without_the_runtime():
    app = split_app()
    app._executor_pool = make_pool()
    app._lifecycle._bind_sources()
    source = app.parts_source
    assert source is not None

    with pytest.raises(RuntimeError, match='split runtime'):
        await source.start()


async def test_a_part_runs_through_the_parts_source_and_its_result_reaches_the_results_topic():
    parts = FakeRuntimeParts()
    handler = SplittingHandler()
    app = split_app(handler)
    recorder = MagicMock()
    app._recorder = recorder
    runtime = await wired(app, parts)
    source = app.parts_source
    assert source is not None
    gauge_before = assigned_partitions._value.get()
    committed_before = split_parts_processed.labels(outcome='ok')._value.get()

    source.on_assign([1])
    processor = source.processors[1]
    source._route_messages([part_message(offset=0, partition=1)])
    await wait_for(lambda: source.consumer.commit.await_count == 1)
    await source.on_revoke([1])
    await asyncio.gather(*source.background_tasks)
    await runtime.stop(deadline=0.0)

    # The processor is in part mode and shows the partition as 1000001.
    assert (processor.partition_id, processor.partition_label) == (1, PARTS_PARTITION_BASE + 1)
    part_mode = processor._part_mode
    assert part_mode is not None
    assert (part_mode.worker, part_mode.source_group) == (WORKER, GROUP)
    assert (part_mode.max_part_data, part_mode.part_data_model) == (65536, None)
    # The result went to the partition of the source message, and the commit to the real partition.
    [stored] = parts.log.records(RESULTS_TOPIC, 3)
    entry = parse_log_record(stored.headers, stored.value)
    assert entry is not None
    assert (entry.kind, entry.body['part'], entry.body['worker'], entry.body['outcome']) == ('result', 0, WORKER, 'ok')
    source.consumer.commit.assert_awaited_once_with({1: 1})
    assert split_parts_processed.labels(outcome='ok')._value.get() == committed_before + 1
    # Recorder rows carry the shown number.
    recorder.record_assigned.assert_called_once_with([PARTS_PARTITION_BASE + 1])
    recorder.record_revoked.assert_called_once_with([PARTS_PARTITION_BASE + 1])
    recorder.record_committed.assert_called_once_with(PARTS_PARTITION_BASE + 1, 1)
    # The handler's rebalance hooks and the worker gauges describe the source topic only.
    assert (handler.assigned, handler.revoked) == ([], [])
    assert assigned_partitions._value.get() == gauge_before


async def test_the_parts_source_shows_its_partitions_with_the_base_number():
    app = split_app()
    runtime = await wired(app)
    source = app.parts_source
    assert source is not None
    source.on_assign([0, 2])

    assert source.snapshot()['assigned_partitions'] == [PARTS_PARTITION_BASE, PARTS_PARTITION_BASE + 2]
    assert source.dead_partitions() == []
    source.processors[2]._dead = True
    assert source.dead_partitions() == [PARTS_PARTITION_BASE + 2]
    with capture_logs() as logs:
        await source._pause_dead_partitions(source.consumer)
        await source.pause_stalled_partition(0)
    source.consumer.pause.assert_any_await([2])
    source.consumer.pause.assert_any_await([0])
    assert [line['partitions'] for line in logs if line['event'] == 'partition_paused_on_death'] == [
        [PARTS_PARTITION_BASE + 2]
    ]
    assert [line['partition'] for line in logs if line['event'] == 'partition_paused_on_stall'] == [
        PARTS_PARTITION_BASE
    ]

    await source.on_revoke([0, 2])
    await runtime.stop(deadline=0.0)


async def test_a_report_of_a_partition_gives_up_when_the_partition_or_the_source_stops():
    app = split_app()
    runtime = await wired(app)
    source = app.parts_source
    assert source is not None
    source.on_assign([0, 1])

    assert not source._partition_is_stopping(0)
    source.processors[0].signal_stop()
    assert source._partition_is_stopping(0)
    assert not source._partition_is_stopping(1)
    # A partition this source does not own any more.
    assert source._partition_is_stopping(5)
    source.signal_stop()
    assert source._partition_is_stopping(1)

    await source.on_revoke([0, 1])
    await runtime.stop(deadline=0.0)


async def test_readyz_names_a_dead_partition_of_the_parts_source_with_its_shown_number():
    from fastapi import FastAPI

    from drakkar.uiserver.routes_health import create_health_router

    app = split_app()
    runtime = await wired(app)
    source = app.parts_source
    assert source is not None
    source.on_assign([3])
    source.processors[3]._dead = True
    app._running = True
    for each in app.sources:
        each._ready = True  # ty: ignore[unresolved-attribute]
    api = FastAPI()
    api.include_router(create_health_router(app))

    async with AsyncClient(transport=ASGITransport(app=api), base_url='http://test') as client:
        response = await client.get('/readyz')

    assert response.status_code == 503
    assert response.json()['reasons'] == [f'partition_{PARTS_PARTITION_BASE + 3}_processor_died']
    await source.on_revoke([3])
    await runtime.stop(deadline=0.0)


def test_the_consumer_wrapper_shows_partitions_with_its_label_base(monkeypatch):
    client = MagicMock()
    client.commit = AsyncMock()
    client.pause = AsyncMock()
    monkeypatch.setattr('drakkar.consumer.AIOConsumer', lambda *args, **kwargs: client)
    config = split_config()
    consumer = KafkaConsumer(
        connection=config.declared_connection('split.connection', 'kafka', 'internal'),
        source=config.sources.kafka,
        partition_label_base=PARTS_PARTITION_BASE,
    )
    shown = str(PARTS_PARTITION_BASE + 2)
    before = offsets_committed.labels(partition=shown)._value.get()

    async def scenario() -> list[dict]:
        with capture_logs() as logs:
            await consumer.commit({2: 9})
            await consumer.pause([2])
            await consumer._handle_assign(None, [MagicMock(partition=2)])
            await consumer._handle_revoke(None, [MagicMock(partition=2)])
        return logs

    logs = asyncio.run(scenario())

    # The client gets the real partition; metrics and log lines get the shown one.
    [committed] = client.commit.await_args.kwargs['offsets']
    assert (committed.partition, committed.offset) == (2, 9)
    assert client.pause.await_args.args[0][0].partition == 2
    assert offsets_committed.labels(partition=shown)._value.get() == before + 1
    by_event = {line['event']: line for line in logs}
    assert by_event['offsets_committed']['offsets'] == {PARTS_PARTITION_BASE + 2: 9}
    assert by_event['partitions_paused']['partitions'] == [PARTS_PARTITION_BASE + 2]
    assert by_event['partitions_assigned']['partitions'] == [PARTS_PARTITION_BASE + 2]
    assert by_event['partitions_revoked']['partitions'] == [PARTS_PARTITION_BASE + 2]


# --- detach and the consume pause reach both sources ---


class ScriptedSource:
    """The group membership of one source, scripted."""

    def __init__(self, *, leave: Any = LeaveOutcome.LEFT, join: Any = True) -> None:
        self.leave = leave
        self.join = join
        self.is_ready = True
        self.is_group_member = True
        self.poll_ended = False
        self.calls: list[str] = []

    def _poll_waits_end(self) -> bool:
        return self.poll_ended

    async def leave_group(self) -> LeaveOutcome:
        self.calls.append('leave')
        self.is_group_member = False
        if isinstance(self.leave, Exception):
            raise self.leave
        return self.leave

    async def join_group(self) -> bool:
        self.calls.append('join')
        if isinstance(self.join, Exception):
            raise self.join
        self.is_group_member = bool(self.join)
        return self.join


@pytest.mark.parametrize(
    ('first', 'second', 'expected'),
    [
        (LeaveOutcome.LEFT, LeaveOutcome.LEFT, LeaveOutcome.LEFT),
        (LeaveOutcome.LEFT, LeaveOutcome.TIMED_OUT, LeaveOutcome.TIMED_OUT),
        (LeaveOutcome.TIMED_OUT, LeaveOutcome.STOPPING, LeaveOutcome.STOPPING),
        (LeaveOutcome.STOPPING, LeaveOutcome.LEFT, LeaveOutcome.STOPPING),
    ],
)
async def test_a_leave_of_both_groups_ends_with_the_worse_outcome(first, second, expected):
    source, parts = ScriptedSource(leave=first), ScriptedSource(leave=second)
    pair = SourcePair(source, parts)  # ty: ignore[invalid-argument-type]

    assert await pair.leave_group() == expected
    assert (source.calls, parts.calls) == (['leave'], ['leave'])
    assert not pair.is_group_member


async def test_a_leave_that_fails_for_one_group_fails_after_both_were_asked():
    source, parts = ScriptedSource(leave=RuntimeError('the client refused')), ScriptedSource()
    pair = SourcePair(source, parts)  # ty: ignore[invalid-argument-type]

    with pytest.raises(RuntimeError, match='the client refused'):
        await pair.leave_group()

    assert parts.calls == ['leave']


async def test_a_join_enters_the_groups_the_worker_is_not_in():
    source, parts = ScriptedSource(), ScriptedSource()
    pair = SourcePair(source, parts)  # ty: ignore[invalid-argument-type]
    parts.is_group_member = False

    assert await pair.join_group() is True
    assert (source.calls, parts.calls) == ([], ['join'])
    assert pair.is_group_member

    source.is_group_member = parts.is_group_member = False
    parts.join = False
    assert await pair.join_group() is False
    assert not pair.is_group_member

    parts.join = RuntimeError('subscribe failed')
    with pytest.raises(RuntimeError, match='subscribe failed'):
        await pair.join_group()


def test_the_pair_is_ready_and_polling_only_when_both_sources_are():
    source, parts = ScriptedSource(), ScriptedSource()
    pair = SourcePair(source, parts)  # ty: ignore[invalid-argument-type]

    assert pair.is_ready and not pair._poll_waits_end()
    parts.is_ready = False
    parts.poll_ended = True
    assert not pair.is_ready and pair._poll_waits_end()


def test_the_detach_controller_leaves_and_joins_both_groups():
    with_parts = split_app()
    source = with_parts.detach._source()
    assert isinstance(source, SourcePair)
    assert source.sources == (with_parts.kafka_source, with_parts.parts_source)

    without = split_app(enabled=False)
    assert without.detach._source() is without.kafka_source


async def test_the_consume_pause_pauses_and_resumes_both_sources():
    app = split_app(ui={'enabled': False, 'release': {'enabled': False}, 'consume_pause': {'enabled': True}})
    runtime = await wired(app)
    source, parts = app.kafka_source, app.parts_source
    assert source is not None and parts is not None
    source.on_assign([0])
    parts.on_assign([1, 2])
    parts.stalled_partitions.add(2)

    with capture_logs() as logs:
        await app.consume_pause.pause(60)

    source.consumer.pause.assert_awaited_once_with([0])
    parts.consumer.pause.assert_awaited_once_with([1])
    [started] = [line for line in logs if line['event'] == 'consume_pause_started']
    assert started['partitions'] == [0, PARTS_PARTITION_BASE + 1]
    # A partition assigned to the parts source during the pause is paused too.
    parts.on_assign([3])
    await asyncio.gather(*parts.background_tasks)
    parts.consumer.pause.assert_awaited_with([3])

    await app.consume_pause.resume()

    source.consumer.resume.assert_awaited_once_with([0])
    parts.consumer.resume.assert_awaited_once_with([1, 3])

    # A backpressure pause of the parts source outlives the operator's pause.
    await app.consume_pause.pause(60)
    parts.paused = True
    parts.consumer.resume.reset_mock()
    await app.consume_pause.resume()
    parts.consumer.resume.assert_not_awaited()

    await source.on_revoke([0])
    await parts.on_revoke([1, 2, 3])
    await runtime.stop(deadline=0.0)


async def test_the_consume_pause_skips_a_parts_source_that_has_no_consumer_yet():
    app = split_app(ui={'enabled': False, 'release': {'enabled': False}, 'consume_pause': {'enabled': True}})
    runtime = await wired(app)
    parts = app.parts_source
    assert parts is not None
    parts.consumer = None

    await app.consume_pause.pause(60)
    await app.consume_pause.resume()

    await runtime.stop(deadline=0.0)


# --- the lifecycle ---


def stub_startup(monkeypatch, app: DrakkarApp, parts: FakeRuntimeParts, order: list[str]) -> None:
    """Keep the sinks, the executor and the Kafka clients out of a full start of ``app``."""

    async def connect_all() -> None:
        order.append('sinks_connected')

    monkeypatch.setattr(app._sink_manager, 'connect_all', connect_all)
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)

    def build_runtime() -> SplitRuntime:
        runtime = parts.runtime(app.config)
        start, stop = runtime.start, runtime.stop

        async def recorded_start() -> None:
            order.append('split_started')
            await start()

        async def recorded_stop(deadline: float) -> None:
            order.append('split_stopped')
            await stop(deadline)

        runtime.start = recorded_start  # ty: ignore[invalid-assignment]
        runtime.stop = recorded_stop  # ty: ignore[invalid-assignment]
        return runtime

    monkeypatch.setattr(app._lifecycle, '_build_split_runtime', build_runtime)

    def consumer(**kwargs: Any) -> MagicMock:
        fake = fake_consumer()
        topic = kwargs['source'].topic

        async def subscribe() -> None:
            order.append(f'subscribed:{topic}')

        async def close() -> None:
            order.append(f'closed:{topic}')

        fake.subscribe = subscribe
        fake.close = close
        return fake

    monkeypatch.setattr('drakkar.sources.kafka.KafkaConsumer', consumer)


async def test_the_runtime_starts_after_the_sinks_and_before_the_sources_and_stops_after_them(monkeypatch):
    app = split_app()
    parts = FakeRuntimeParts()
    order: list[str] = []
    stub_startup(monkeypatch, app, parts, order)
    stages: list[Any] = []
    monkeypatch.setattr(app._sink_manager, 'set_split_stages', stages.append)

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    runtime = app._split
    kafka_context = app.kafka_source._context()
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, 10)

    assert order == [
        'sinks_connected',
        'split_started',
        f'subscribed:{SOURCE_TOPIC}',
        f'subscribed:{PARTS_TOPIC}',
        f'closed:{SOURCE_TOPIC}',
        f'closed:{PARTS_TOPIC}',
        'split_stopped',
    ]
    assert isinstance(runtime, SplitRuntime)
    assert kafka_context.split is runtime
    assert stages == [app.config.split_aggregate_sinks]
    assert app._split is None


async def test_a_missing_split_topic_fails_the_worker_start(monkeypatch):
    app = split_app()
    parts = FakeRuntimeParts(counts=topic_counts(**{PARTS_TOPIC: None}))
    order: list[str] = []
    stub_startup(monkeypatch, app, parts, order)

    with pytest.raises(ConfigurationError, match=f"the topic '{PARTS_TOPIC}' does not exist"):
        await app._lifecycle._async_run()

    # No source started, and the producer of the failed runtime was closed.
    assert order == ['sinks_connected', 'split_started', 'split_stopped']


async def test_a_handler_that_splits_with_the_feature_off_logs_split_disabled_once(monkeypatch):
    app = split_app(enabled=False)
    order: list[str] = []
    stub_startup(monkeypatch, app, FakeRuntimeParts(), order)

    with capture_logs() as logs:
        run = asyncio.create_task(app._lifecycle._async_run())
        await wait_for(lambda: app.is_ready)
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, 10)

    disabled = [line for line in logs if line['event'] == 'split_disabled']
    assert [(line['log_level'], line['category']) for line in disabled] == [('info', 'split')]
    assert 'split_started' not in order
    assert app._split is None
    assert app._sink_manager._aggregate_sinks is None


async def test_a_worker_without_split_builds_nothing_and_logs_nothing(monkeypatch):
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=split_config(enabled=False), worker_id=WORKER)
    order: list[str] = []
    stub_startup(monkeypatch, app, FakeRuntimeParts(), order)

    with capture_logs() as logs:
        run = asyncio.create_task(app._lifecycle._async_run())
        await wait_for(lambda: app.is_ready)
        context = app.kafka_source._context()
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, 10)

    assert not [line for line in logs if line['event'].startswith('split_')]
    assert order == ['sinks_connected', f'subscribed:{SOURCE_TOPIC}', f'closed:{SOURCE_TOPIC}']
    assert context.split is None
