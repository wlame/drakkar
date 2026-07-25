"""Tests for the delivery-guarantee fixes from the 2026-06-09 product review.

Covers:
- parse-error stamping in deserialize_message + the kafka.on_parse_error
  policy (skip / dlq / raise)
- the DLQSink.send() bool contract (True = confirmed write)
- SinkDeliveryFailedError propagation from DrakkarApp._handle_collect when
  the DLQ fallback is missing or fails
- offset stalling (no commit) when delivery cannot be confirmed
- drain() staying prompt when offsets are stalled
- recorder DB file permissions
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel as BM

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import dlq_dropped_payloads, message_parse_failures, partition_processor_deaths
from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    KafkaPayload,
    ParseFailurePayload,
    SinkDeliveryFailedError,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.recorder import EventRecorder
from drakkar.sinks.dlq import DLQSink
from tests.conftest import make_ui_config, wait_for
from tests.test_app import SimpleHandler, _setup_app_sinks


class _In(BM):
    x: int


class _Out(BM):
    v: str = ''


def make_msg(offset: int = 0, value: bytes = b'{"x": 1}') -> SourceMessage:
    return SourceMessage(topic='test', partition=0, offset=offset, value=value, timestamp=1000)


class TypedHandler(BaseDrakkarHandler[_In, _Out]):
    def __init__(self):
        self.arranged: list[SourceMessage] = []

    async def arrange(self, messages, pending):
        self.arranged.extend(messages)
        return []


@pytest.fixture
def echo_pool() -> ExecutorPool:
    return ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)


@pytest.fixture
def app_config() -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10, window_size=5),
        sinks=SinksConfig(kafka={'results': KafkaSinkConfig(topic='test-out')}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


# --- deserialize_message parse_error stamping ---


def test_deserialize_sets_parse_error_and_metric():
    handler = TypedHandler()
    before = message_parse_failures.labels(partition='0')._value.get()
    msg = make_msg(value=b'not json')
    handler.deserialize_message(msg)
    assert msg.payload is None
    assert msg.parse_error is not None
    assert message_parse_failures.labels(partition='0')._value.get() == before + 1


def test_deserialize_clears_parse_error_on_success():
    handler = TypedHandler()
    msg = make_msg(value=b'{"x": 7}')
    msg.parse_error = 'stale'
    handler.deserialize_message(msg)
    assert msg.payload == _In(x=7)
    assert msg.parse_error is None


# --- on_parse_error policy in the partition processor ---


async def test_policy_skip_keeps_message_in_window_and_commits(echo_pool):
    """Default policy: unparseable message flows to arrange() with
    payload=None and the offset commits (pre-policy behavior)."""
    handler = TypedHandler()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        on_parse_error='skip',
    )
    proc.enqueue(make_msg(offset=5, value=b'broken'))
    proc.start()
    await wait_for(lambda: any(c[1] == 6 for c in committed), timeout=5)
    await proc.stop()

    assert len(handler.arranged) == 1
    assert handler.arranged[0].payload is None
    assert handler.arranged[0].parse_error is not None


async def test_policy_dlq_excludes_message_and_commits_on_confirmed_send(echo_pool):
    handler = TypedHandler()
    committed: list[tuple[int, int]] = []
    dlq_calls: list[DeliveryError] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    async def dlq_send(error: DeliveryError, partition_id: int) -> bool:
        dlq_calls.append(error)
        return True

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        on_parse_error='dlq',
        dlq_send=dlq_send,
    )
    proc.enqueue(make_msg(offset=5, value=b'broken'))
    proc.start()
    await wait_for(lambda: any(c[1] == 6 for c in committed), timeout=5)
    await proc.stop()

    assert handler.arranged == [], 'unparseable message must not reach arrange()'
    assert len(dlq_calls) == 1
    payload = dlq_calls[0].payloads[0]
    assert isinstance(payload, ParseFailurePayload)
    assert payload.offset == 5
    assert payload.raw_value == 'broken'


async def test_policy_dlq_send_failure_stall_mode_stalls_offset_and_pauses(echo_pool):
    """dlq.on_send_failure=stall: a failed DLQ write leaves the offset
    pending, signals the stall callback once, and drain stays prompt."""
    handler = TypedHandler()
    committed: list[tuple[int, int]] = []
    stalled: list[int] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    async def dlq_send(error: DeliveryError, partition_id: int) -> bool:
        return False  # DLQ broker down

    async def on_stall(pid: int) -> None:
        stalled.append(pid)

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        on_parse_error='dlq',
        dlq_send=dlq_send,
        on_dlq_failure='stall',
        on_stall=on_stall,
    )
    proc.enqueue(make_msg(offset=5, value=b'broken'))
    proc.enqueue(make_msg(offset=6, value=b'also broken'))
    proc.start()
    await wait_for(lambda: proc.queue_size == 0 and not proc._arranging, timeout=5)
    await asyncio.sleep(0.1)

    # Stalled offsets must not wedge drain — only block the commit.
    await asyncio.wait_for(proc.drain(), timeout=2)
    await proc.stop()

    assert not committed, 'offset must NOT commit when the DLQ write failed'
    assert proc.offset_tracker.has_pending(), 'offset stays pending (stalled watermark)'
    assert stalled == [0], 'stall callback must fire exactly once per processor'


async def test_policy_dlq_send_failure_drop_mode_commits_and_counts(echo_pool):
    """Default dlq.on_send_failure=drop: a failed DLQ write is logged +
    counted but the offset commits and the pipeline keeps moving."""
    handler = TypedHandler()
    committed: list[tuple[int, int]] = []
    before = dlq_dropped_payloads.labels(partition='0')._value.get()

    async def on_commit(pid, off):
        committed.append((pid, off))

    async def dlq_send(error: DeliveryError, partition_id: int) -> bool:
        return False  # DLQ broker down

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        on_parse_error='dlq',
        dlq_send=dlq_send,
        # on_dlq_failure defaults to 'drop'
    )
    proc.enqueue(make_msg(offset=5, value=b'broken'))
    proc.start()
    await wait_for(lambda: any(c[1] == 6 for c in committed), timeout=5)
    await proc.stop()

    assert not proc.offset_tracker.has_pending()
    assert dlq_dropped_payloads.labels(partition='0')._value.get() == before + 1


async def test_policy_raise_stops_partition_without_commit(echo_pool):
    handler = TypedHandler()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        on_parse_error='raise',
    )
    proc.start()
    # MessageParseError propagates out of the window loop, which kills it —
    # but the supervisor restarts the loop once (PARTITION_RESTART_LIMIT),
    # so reaching the exited state takes two failing windows. Wait for the
    # restart before enqueuing the second, or both messages land in one
    # window and only crash the loop once.
    restarts = partition_processor_deaths.labels(partition='0', outcome='restarted')
    before = restarts._value.get()
    proc.enqueue(make_msg(offset=5, value=b'broken'))
    await wait_for(lambda: restarts._value.get() > before, timeout=5)
    proc.enqueue(make_msg(offset=6, value=b'broken'))

    await wait_for(lambda: proc._task is not None and proc._task.done(), timeout=5)

    assert proc.is_dead, 'a loop that died twice must be marked dead so /readyz fails'
    assert handler.arranged == []
    assert not committed


# --- per-task delivery failure stalls the offset ---


async def test_task_delivery_failure_stalls_offset(echo_pool):
    """SinkDeliveryFailedError from the per-task sink delivery marks the
    message tracker and the offset never commits."""
    committed: list[tuple[int, int]] = []

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            from drakkar.models import ExecutorTask

            return [ExecutorTask(task_id=f't-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

        async def on_task_complete(self, result):
            return CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])

    async def failing_collect(result: CollectResult, partition_id: int) -> None:
        raise SinkDeliveryFailedError(sink_name='k', sink_type='kafka', reason='dlq down')

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=H(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=failing_collect,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=10))
    proc.start()
    await asyncio.wait_for(proc.drain(), timeout=5)
    await proc.stop()

    assert not committed, 'offset must NOT commit when task payload delivery failed'
    assert proc.offset_tracker.has_pending()


# --- DLQSink.send bool contract ---


async def test_dlq_send_returns_false_when_not_connected():
    sink = DLQSink(topic='dlq', brokers='localhost:9092')
    error = DeliveryError(sink_name='k', sink_type='kafka', error='boom', payloads=[])
    assert await sink.send(error, partition_id=0) is False


# --- app-level DLQ fallback wiring ---


class DLQHandler(SimpleHandler):
    async def on_delivery_error(self, error: DeliveryError) -> DeliveryAction:
        return DeliveryAction.DLQ


def _make_failing_kafka_app(app_config, dlq_sink) -> DrakkarApp:
    app = DrakkarApp(handler=DLQHandler(), config=app_config)
    _setup_app_sinks(app)
    kafka_sink = app._sink_manager._sinks[('kafka', 'results')]
    kafka_sink.deliver.side_effect = RuntimeError('broker down')
    app._dlq_sink = dlq_sink
    return app


class _FailingDLQ:
    async def send(self, error, partition_id, attempt_count=1):
        return False


async def test_handle_collect_raises_when_dlq_send_fails_in_stall_mode(app_config):
    app_config.dlq.on_send_failure = 'stall'
    app = _make_failing_kafka_app(app_config, _FailingDLQ())
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    with pytest.raises(SinkDeliveryFailedError):
        await app._handle_collect(result, partition_id=0)


async def test_handle_collect_raises_when_dlq_missing_in_stall_mode(app_config):
    app_config.dlq.on_send_failure = 'stall'
    app = _make_failing_kafka_app(app_config, None)
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    with pytest.raises(SinkDeliveryFailedError):
        await app._handle_collect(result, partition_id=0)


async def test_handle_collect_drop_mode_swallows_dlq_failure_and_counts(app_config):
    """Default dlq.on_send_failure=drop: the DLQ failure is logged + counted
    but _handle_collect returns normally so the offset can commit."""
    before = dlq_dropped_payloads.labels(partition='7')._value.get()
    app = _make_failing_kafka_app(app_config, _FailingDLQ())
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    await app._handle_collect(result, partition_id=7)  # must not raise
    assert dlq_dropped_payloads.labels(partition='7')._value.get() == before + 1


async def test_handle_collect_drop_mode_swallows_missing_dlq(app_config):
    app = _make_failing_kafka_app(app_config, None)
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    await app._handle_collect(result, partition_id=0)  # must not raise


async def test_handle_collect_succeeds_when_dlq_confirms(app_config):
    sent: list[int] = []

    class OkDLQ:
        async def send(self, error, partition_id, attempt_count=1):
            sent.append(partition_id)
            return True

    app = _make_failing_kafka_app(app_config, OkDLQ())
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    await app._handle_collect(result, partition_id=3)
    assert sent == [3]


# --- circuit-open DLQ failure follows the same strategy ---


def _make_circuit_open_app(app_config, dlq_sink) -> DrakkarApp:
    app = DrakkarApp(handler=DLQHandler(), config=app_config)
    _setup_app_sinks(app)
    app._dlq_sink = dlq_sink
    app._sink_manager.attach_runtime(
        recorder=None,
        dlq_sink=dlq_sink,
        dlq_on_send_failure=app_config.dlq.on_send_failure,
    )
    kafka_sink = app._sink_manager._sinks[('kafka', 'results')]
    kafka_sink.should_skip_delivery = MagicMock(return_value=True)
    return app


async def test_circuit_open_dlq_failure_stall_mode_raises(app_config):
    app_config.dlq.on_send_failure = 'stall'
    app = _make_circuit_open_app(app_config, _FailingDLQ())
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    with pytest.raises(SinkDeliveryFailedError):
        await app._handle_collect(result, partition_id=0)


async def test_circuit_open_dlq_failure_drop_mode_counts(app_config):
    before = dlq_dropped_payloads.labels(partition='9')._value.get()
    app = _make_circuit_open_app(app_config, _FailingDLQ())
    result = CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])
    await app._handle_collect(result, partition_id=9)  # must not raise
    assert dlq_dropped_payloads.labels(partition='9')._value.get() == before + 1


# --- stall → partition pause wiring (lifecycle) ---


async def test_pause_stalled_partition_pauses_consumer_and_records(app_config):
    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    app._consumer = AsyncMock()
    app._recorder = MagicMock()

    await app._lifecycle._pause_stalled_partition(3)

    app._consumer.pause.assert_awaited_once_with([3])
    app._recorder.record_partition_stalled.assert_called_once_with(3)
    assert 3 in app._stalled_partitions


async def test_pause_stalled_partition_survives_pause_error(app_config):
    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    app._consumer = AsyncMock()
    app._consumer.pause.side_effect = RuntimeError('broker gone')

    await app._lifecycle._pause_stalled_partition(2)  # must not raise
    assert 2 in app._stalled_partitions


async def test_revoke_clears_stall_bookkeeping(app_config):
    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    app._stalled_partitions.add(4)

    await app._lifecycle._on_revoke([4])
    await wait_for(lambda: 4 not in app._stalled_partitions)


async def test_revoke_blocks_until_the_drain_commits(app_config, echo_pool):
    """Pins the rebalance contract: the revoke must not return early.

    ``AIOConsumer`` runs rebalance callbacks via
    ``run_coroutine_threadsafe(...).result()``, so librdkafka's rebalance
    thread waits on this coroutine. Returning before the drain finished
    let the rebalance complete while this worker was still processing:
    the new owner started from the last committed offset while in-flight
    work here was still delivering the same messages, duplicating
    everything between the last commit and the drain end.
    """
    committed: list[tuple[int, int]] = []

    app = DrakkarApp(handler=SimpleHandler(), config=app_config)
    app._consumer = AsyncMock()

    async def record_commit(offsets: dict[int, int]) -> None:
        for pid, off in offsets.items():
            committed.append((pid, off))

    app._consumer.commit.side_effect = record_commit

    proc = PartitionProcessor(
        partition_id=4,
        handler=SimpleHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=app._handle_commit,
    )
    app._processors[4] = proc
    proc.start()
    proc.enqueue(make_msg(offset=7))

    # Revoke with the message still unprocessed: the drain inside
    # _stop_processor must finish it AND commit before _on_revoke returns.
    await app._lifecycle._on_revoke([4])

    # No wait_for here: by the time _on_revoke returns, the commit must
    # ALREADY have happened. Polling would hide an early return.
    assert committed == [(4, 8)], f'revoke returned before committing; commits = {committed}'
    assert 4 not in app._processors


# --- recorder DB file permissions ---


async def test_recorder_db_file_owner_only_permissions(tmp_path):
    recorder = EventRecorder(make_ui_config(enabled=False, db_dir=str(tmp_path)), worker_name='permtest')
    await recorder.start()
    try:
        db_files = [f for f in os.listdir(tmp_path) if f.endswith('.db') and not os.path.islink(tmp_path / f)]
        assert db_files, 'recorder must have created a DB file'
        mode = os.stat(tmp_path / db_files[0]).st_mode & 0o777
        assert mode == 0o600, f'recorder DB must be owner-only, got {oct(mode)}'
    finally:
        await recorder.stop()
