"""Delivery of one result by each sink's declared policy, retry budget and dependencies."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest
import structlog
from pydantic import BaseModel
from structlog.testing import capture_logs

import drakkar.sinks.manager as manager_module
from drakkar.config import SinkPolicy, UIConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    KafkaPayload,
    PostgresPayload,
    SinkNotStartedReason,
    SourceMessage,
)
from drakkar.recorder.writer import EventWriter
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq_record import DLQRecord
from drakkar.sinks.manager import (
    CIRCUIT_OPEN_ERROR,
    DeliveryOutcome,
    MessageContext,
    MessageDLQState,
    SinkManager,
)

# --- Helpers ---


@pytest.fixture(autouse=True)
def debug_events_reach_capture_logs(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Let the manager's ``debug`` events reach ``capture_logs``, whatever ran before in this process.

    ``setup_logging`` installs a filtering logger and caches it on first use, so
    a test that ran earlier can leave the module's logger dropping ``debug`` calls.
    """
    saved = structlog.get_config()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        cache_logger_on_first_use=False,
    )
    monkeypatch.setattr(manager_module, 'logger', structlog.get_logger('drakkar.sinks.manager'))
    yield
    structlog.configure(**saved)


class Data(BaseModel):
    value: int = 1


class FakeSink(BaseSink):
    """An in-memory sink; ``fail`` makes every delivery raise."""

    def __init__(self, name: str, sink_type: str) -> None:
        super().__init__(name)
        self.sink_type = sink_type
        self.delivered: list[list[BaseModel]] = []
        self.fail = False

    async def connect(self) -> None:
        pass

    async def deliver(self, payloads: list[BaseModel]) -> None:
        if self.fail:
            raise RuntimeError(f'{self.sink_type} is down')
        self.delivered.append(payloads)

    async def close(self) -> None:
        pass


class RecordingDLQ:
    """DLQSink stand-in: keeps every record it is asked to send."""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.sent: list[tuple[DLQRecord, int]] = []

    async def send(self, record: DLQRecord, partition_id: int) -> bool:
        # One loop turn inside the send, as a real producer call has, so two
        # failing groups really do meet at the message lock.
        await asyncio.sleep(0)
        self.sent.append((record, partition_id))
        return self.ok


class CollectingRecorder(EventWriter):
    """An EventWriter whose rows land in a list."""

    def __init__(self) -> None:
        super().__init__(UIConfig(recorder={'store_events': True}, release={'enabled': False}))
        self.rows: list[dict] = []

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        self.rows.append(event)

    def of(self, event_type: str) -> list[dict]:
        return [row for row in self.rows if row['event'] == event_type]


def policy(on_failure: str = 'dlq', max_retries: int = 2, depends_on: tuple[str, ...] = ()) -> SinkPolicy:
    return SinkPolicy(on_failure=on_failure, max_retries=max_retries, depends_on=depends_on)


def source_message(offset: int = 10, headers: list[tuple[str, bytes | None]] | None = None) -> SourceMessage:
    return SourceMessage(
        topic='orders',
        partition=3,
        offset=offset,
        key=f'key-{offset}'.encode(),
        value=f'value-{offset}'.encode(),
        timestamp=1_700_000_000_000 + offset,
        headers=headers or [],
    )


def message_context(*offsets: int) -> MessageContext:
    messages = [source_message(offset) for offset in offsets or (10,)]
    return MessageContext(source_messages=messages, states=[MessageDLQState() for _ in messages])


def manager_with(*sinks: tuple[FakeSink, SinkPolicy], dlq: RecordingDLQ | None = None) -> SinkManager:
    mgr = SinkManager()
    for sink, sink_policy in sinks:
        mgr.register(sink, policy=sink_policy)
    mgr.attach_runtime(recorder=None, dlq_sink=dlq, dlq_on_send_failure='drop')  # ty: ignore[invalid-argument-type]
    return mgr


async def declared(error: DeliveryError) -> DeliveryAction:
    """The default hook: the sink's declared policy."""
    return error.declared_policy


def both_results() -> CollectResult:
    return CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])


KAFKA = ('kafka', 'out')
POSTGRES = ('postgres', 'main')


# --- The dependency graph ---


async def test_groups_without_dependencies_run_in_parallel():
    started = {'kafka': asyncio.Event(), 'postgres': asyncio.Event()}

    class MeetingSink(FakeSink):
        async def deliver(self, payloads: list[BaseModel]) -> None:
            started[self.sink_type].set()
            other = 'postgres' if self.sink_type == 'kafka' else 'kafka'
            # Each group waits for the other to have started: run one after
            # the other, they would never meet.
            await asyncio.wait_for(started[other].wait(), timeout=2)
            await super().deliver(payloads)

    mgr = manager_with((MeetingSink('out', 'kafka'), policy()), (MeetingSink('main', 'postgres'), policy()))

    outcomes = await mgr.deliver_all(both_results(), declared, 0, message_context=message_context())

    assert outcomes == {KAFKA: DeliveryOutcome.DELIVERED, POSTGRES: DeliveryOutcome.DELIVERED}


async def test_dependent_starts_after_its_dependencies_delivered():
    order: list[str] = []

    class OrderedSink(FakeSink):
        async def deliver(self, payloads: list[BaseModel]) -> None:
            order.append(f'{self.sink_type} start')
            await asyncio.sleep(0.01)
            order.append(f'{self.sink_type} end')

    mgr = manager_with(
        (OrderedSink('out', 'kafka'), policy(depends_on=('postgres/main',))),
        (OrderedSink('main', 'postgres'), policy()),
    )

    outcomes = await mgr.deliver_all(both_results(), declared, 0, message_context=message_context())

    assert order == ['postgres start', 'postgres end', 'kafka start', 'kafka end']
    assert set(outcomes.values()) == {DeliveryOutcome.DELIVERED}


async def test_dependency_without_payloads_counts_as_satisfied():
    kafka = FakeSink('out', 'kafka')
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (FakeSink('main', 'postgres'), policy()))

    outcomes = await mgr.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 0, message_context=message_context()
    )

    assert outcomes == {KAFKA: DeliveryOutcome.DELIVERED}
    assert len(kafka.delivered) == 1


async def test_skipped_dependency_counts_as_satisfied():
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy()))

    outcomes = await mgr.deliver_all(
        both_results(), AsyncMock(return_value=DeliveryAction.SKIP), 0, message_context=message_context()
    )

    assert outcomes == {KAFKA: DeliveryOutcome.DELIVERED, POSTGRES: DeliveryOutcome.SKIPPED}
    assert len(kafka.delivered) == 1


async def test_dlq_dependency_stops_dependents_with_reason_message_dlq():
    from drakkar.metrics import sink_deliveries_not_started

    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy()), dlq=dlq)
    recorder = CollectingRecorder()
    mgr.attach_runtime(recorder=recorder, dlq_sink=dlq)  # ty: ignore[invalid-argument-type]
    counter = sink_deliveries_not_started.labels(sink_type='kafka', sink_name='out', reason='message_dlq')
    before = counter._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), declared, 3, message_context=message_context())

    assert outcomes == {KAFKA: DeliveryOutcome.NOT_STARTED, POSTGRES: DeliveryOutcome.DLQ}
    assert kafka.delivered == []
    assert len(dlq.sent) == 1
    assert counter._value.get() == before + 1
    not_started = [entry for entry in logs if entry['event'] == 'sink_not_started']
    assert [(entry['sink'], entry['reason'], entry['log_level']) for entry in not_started] == [
        ('kafka/out', 'message_dlq', 'debug')
    ]
    assert [json.loads(row['metadata']) for row in recorder.of('sink_not_started')] == [
        {'sink': 'kafka/out', 'reason': 'message_dlq'}
    ]


async def test_window_dependency_failure_stops_dependents_with_reason_dependency_failed():
    from drakkar.metrics import sink_deliveries_not_started

    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy()), dlq=dlq)
    counter = sink_deliveries_not_started.labels(
        sink_type='kafka', sink_name='out', reason=SinkNotStartedReason.DEPENDENCY_FAILED.value
    )
    before = counter._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), declared, 0, message_context=MessageContext.window())

    assert outcomes == {KAFKA: DeliveryOutcome.NOT_STARTED, POSTGRES: DeliveryOutcome.FAILED}
    assert kafka.delivered == []
    assert dlq.sent == []
    assert counter._value.get() == before + 1
    not_started = [entry for entry in logs if entry['event'] == 'sink_not_started']
    assert [(entry['sink'], entry['reason']) for entry in not_started] == [('kafka/out', 'dependency_failed')]


async def test_not_started_dependency_passes_its_reason_down_the_chain():
    """A chain of three: the middle sink never starts, so the last one inherits its reason."""
    first = FakeSink('main', 'postgres')
    first.fail = True
    middle = FakeSink('out', 'kafka')
    last = FakeSink('cache', 'redis')
    mgr = manager_with(
        (first, policy()),
        (middle, policy(depends_on=('postgres/main',))),
        (last, policy(depends_on=('kafka/out',))),
    )
    from drakkar.models import RedisPayload

    result = CollectResult(
        kafka=[KafkaPayload(data=Data())],
        postgres=[PostgresPayload(table='t', data=Data())],
        redis=[RedisPayload(key='k', data=Data())],
    )
    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(result, declared, 0, message_context=MessageContext.window())

    assert outcomes[('redis', 'cache')] == DeliveryOutcome.NOT_STARTED
    reasons = {entry['sink']: entry['reason'] for entry in logs if entry['event'] == 'sink_not_started'}
    assert reasons == {'kafka/out': 'dependency_failed', 'redis/cache': 'dependency_failed'}


async def test_dependency_that_raises_still_releases_its_dependents():
    """A dependency that stalls (raises) settles as failed: its dependent does not wait for ever."""
    from drakkar.models import SinkDeliveryFailedError

    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy(max_retries=0)))
    mgr.attach_runtime(recorder=None, dlq_sink=RecordingDLQ(ok=False), dlq_on_send_failure='stall')  # ty: ignore[invalid-argument-type]

    with capture_logs() as logs, pytest.raises(SinkDeliveryFailedError):
        await asyncio.wait_for(mgr.deliver_all(both_results(), declared, 0, message_context=message_context()), 2)

    assert kafka.delivered == []
    assert [entry['reason'] for entry in logs if entry['event'] == 'sink_not_started'] == ['dependency_failed']


async def test_group_gate_runs_before_each_group_and_its_raise_ends_the_call():
    """The gate is awaited once per group, after its dependencies settled; what it raises comes out of the call."""
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy()))
    gated: list[tuple[str, str]] = []

    async def gate(key: tuple[str, str]) -> None:
        gated.append(key)
        if key == KAFKA:
            raise asyncio.CancelledError('stop before kafka')

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        await mgr.deliver_all(both_results(), declared, -1, message_context=message_context(), before_group_start=gate)

    assert gated == [POSTGRES, KAFKA]
    assert (len(postgres.delivered), kafka.delivered) == (1, [])
    assert [entry for entry in logs if entry['event'] == 'sink_not_started'] == []


async def test_group_gate_runs_before_a_not_started_note():
    """A gate that raises keeps a dependent of a failed dependency from being counted as not started."""
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    mgr = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy(on_failure='dlq')))

    async def gate(key: tuple[str, str]) -> None:
        if key == KAFKA:
            raise asyncio.CancelledError('stop')

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        await mgr.deliver_all(
            both_results(), declared, 0, message_context=MessageContext.window(), before_group_start=gate
        )

    assert [entry for entry in logs if entry['event'] == 'sink_not_started'] == []


def test_message_context_needs_one_state_per_source_message():
    with pytest.raises(ValueError, match='one DLQ state per source message'):
        MessageContext(source_messages=[source_message()], states=[])


def test_message_context_without_messages_is_refused():
    """Only the explicit window form may have no source message: an empty context would hide a lost dlq."""
    with pytest.raises(ValueError, match=r'at least one source message; use MessageContext\.window\(\)'):
        MessageContext(source_messages=[], states=[])

    assert MessageContext.window().source_messages == ()


# --- The hook and the retry budget ---


async def test_hook_sees_attempt_and_retries_left():
    sink = FakeSink('out', 'kafka')
    sink.fail = True
    seen: list[tuple[int, int, DeliveryAction]] = []

    async def hook(error: DeliveryError) -> DeliveryAction:
        seen.append((error.attempt, error.retries_left, error.declared_policy))
        return DeliveryAction.RETRY if error.retries_left else DeliveryAction.SKIP

    mgr = manager_with((sink, policy(on_failure='skip', max_retries=2)))

    outcomes = await mgr.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]), hook, 0, message_context=message_context()
    )

    assert seen == [(1, 2, DeliveryAction.SKIP), (2, 1, DeliveryAction.SKIP), (3, 0, DeliveryAction.SKIP)]
    assert outcomes == {KAFKA: DeliveryOutcome.SKIPPED}


async def test_retry_past_the_budget_applies_the_declared_policy():
    """A hook that answers RETRY for ever: after max_retries the declared policy ends the loop."""
    sink = FakeSink('out', 'kafka')
    sink.fail = True
    dlq = RecordingDLQ()
    hook = AsyncMock(return_value=DeliveryAction.RETRY)
    mgr = manager_with((sink, policy(on_failure='dlq', max_retries=1)), dlq=dlq)

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), hook, 3, message_context=message_context()
        )

    assert hook.await_count == 2
    assert outcomes == {KAFKA: DeliveryOutcome.DLQ}
    record, partition_id = dlq.sent[0]
    assert (record.attempts, partition_id, record.sink) == (2, 3, 'kafka/out')
    spent = [entry for entry in logs if entry['event'] == 'sink_retry_budget_spent']
    assert len(spent) == 1
    assert spent[0]['log_level'] == 'warning'
    assert spent[0]['on_failure'] == 'dlq'


@pytest.mark.parametrize('declared_policy', [DeliveryAction.DLQ, DeliveryAction.SKIP])
async def test_default_hook_returns_the_declared_policy(declared_policy):
    error = DeliveryError(sink_name='out', sink_type='kafka', error='boom', declared_policy=declared_policy)

    assert await BaseDrakkarHandler().on_delivery_error(error) == declared_policy


async def test_raising_hook_is_logged_and_applies_the_declared_policy():
    sink = FakeSink('out', 'kafka')
    sink.fail = True

    async def hook(error: DeliveryError) -> DeliveryAction:
        raise ValueError('handler bug')

    mgr = manager_with((sink, policy(on_failure='skip')))

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), hook, 0, message_context=message_context()
        )

    assert outcomes == {KAFKA: DeliveryOutcome.SKIPPED}
    failed = [entry for entry in logs if entry['event'] == 'delivery_error_hook_failed']
    assert len(failed) == 1
    assert failed[0]['log_level'] == 'error'
    assert failed[0]['sink'] == 'kafka/out'
    assert failed[0]['on_failure'] == 'skip'


async def test_on_failure_skip_counts_and_logs():
    from drakkar.metrics import sink_deliveries_skipped

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    mgr = manager_with((sink, policy(on_failure='skip', max_retries=0)))
    recorder = CollectingRecorder()
    mgr.attach_runtime(recorder=recorder, dlq_sink=None)
    before = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 0, message_context=message_context()
        )

    assert outcomes == {KAFKA: DeliveryOutcome.SKIPPED}
    assert sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get() == before + 1
    assert [entry['log_level'] for entry in logs if entry['event'] == 'sink_delivery_skipped'] == ['warning']
    assert [json.loads(row['metadata'])['action'] for row in recorder.of('sink_error')] == ['skip']


async def test_circuit_open_uses_the_declared_policy_without_the_hook():
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    for sink in (kafka, postgres):
        sink.should_skip_delivery = lambda: True  # ty: ignore[invalid-assignment]
    dlq = RecordingDLQ()
    hook = AsyncMock(return_value=DeliveryAction.RETRY)
    mgr = manager_with((kafka, policy(on_failure='dlq')), (postgres, policy(on_failure='skip')), dlq=dlq)

    outcomes = await mgr.deliver_all(both_results(), hook, 0, message_context=message_context())

    hook.assert_not_awaited()
    assert outcomes == {KAFKA: DeliveryOutcome.DLQ, POSTGRES: DeliveryOutcome.SKIPPED}
    assert kafka.delivered == postgres.delivered == []
    [(record, _)] = dlq.sent
    assert (record.error, record.attempts, record.sink) == (CIRCUIT_OPEN_ERROR, 0, 'kafka/out')


# --- The message is done ---


async def test_dlq_writes_one_record_per_source_message_of_a_task():
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    kafka.fail = postgres.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((kafka, policy(max_retries=0)), (postgres, policy(max_retries=0)), dlq=dlq)
    recorder = CollectingRecorder()
    mgr.attach_runtime(recorder=recorder, dlq_sink=dlq)  # ty: ignore[invalid-argument-type]
    first = source_message(10, headers=[('trace', b'abc'), ('dk-stage', b'old')])
    second = source_message(11)
    states = [MessageDLQState(delivered_sinks={'redis/cache'}), MessageDLQState()]
    context = MessageContext(source_messages=[first, second], states=states)

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), declared, 3, message_context=context)

    # Both groups failed at the same moment: one record per source message, not per group.
    assert sorted(record.source_offset for record, _ in dlq.sent) == [10, 11]
    assert all(state.dlq_written for state in states)
    assert sorted(outcomes.values()) == [DeliveryOutcome.DLQ, DeliveryOutcome.DLQ]
    by_offset = {record.source_offset: record for record, _ in dlq.sent}
    record = by_offset[10]
    assert (record.key, record.value, record.source_headers) == (first.key, first.value, first.headers)
    assert (record.stage, record.attempts, record.delivered_sinks) == ('sink', 1, ['redis/cache'])
    assert (record.source_topic, record.source_partition, record.source_timestamp_ms) == (
        'orders',
        3,
        first.timestamp,
    )
    assert record.request_id is None
    assert [entry['event'] for entry in logs].count('message_already_dlq') == 1
    assert [entry['event'] for entry in logs].count('message_dlq') == 2
    assert sorted(row['offset'] for row in recorder.of('message_dlq')) == [10, 11]


async def test_window_result_dlq_acts_as_skip_with_warning():
    from drakkar.metrics import sink_deliveries_skipped

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((sink, policy(on_failure='dlq', max_retries=0)), dlq=dlq)
    before = sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 0, message_context=MessageContext.window()
        )

    assert outcomes == {KAFKA: DeliveryOutcome.FAILED}
    assert dlq.sent == []
    assert sink_deliveries_skipped.labels(sink_type='kafka', sink_name='out')._value.get() == before + 1
    warnings = [entry for entry in logs if entry['event'] == 'window_delivery_not_dlq']
    assert [(entry['sink'], entry['log_level']) for entry in warnings] == [('kafka/out', 'warning')]


async def test_message_already_dlq_skips_every_group():
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    hook = AsyncMock()
    mgr = manager_with((kafka, policy()), (postgres, policy()))
    context = message_context()
    context.states[0].dlq_written = True

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), hook, 0, message_context=context)

    assert outcomes == {KAFKA: DeliveryOutcome.NOT_STARTED, POSTGRES: DeliveryOutcome.NOT_STARTED}
    assert kafka.delivered == postgres.delivered == []
    hook.assert_not_awaited()
    assert sorted(entry['sink'] for entry in logs if entry['reason'] == 'message_dlq') == ['kafka/out', 'postgres/main']


async def test_delivered_sinks_are_added_to_the_message_state():
    mgr = manager_with((FakeSink('out', 'kafka'), policy()), (FakeSink('main', 'postgres'), policy()))
    context = message_context(10, 11)

    await mgr.deliver_all(both_results(), declared, 0, message_context=context)

    assert [state.delivered_sinks for state in context.states] == [{'kafka/out', 'postgres/main'}] * 2


async def test_http_request_context_writes_the_body_record():
    sink = FakeSink('out', 'kafka')
    sink.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((sink, policy(max_retries=0)), dlq=dlq)
    context = MessageContext.for_request(body=b'{"q": 1}', request_id='req-7', received_ms=1234)

    await mgr.deliver_all(CollectResult(kafka=[KafkaPayload(data=Data())]), declared, -1, message_context=context)

    [(record, _)] = dlq.sent
    assert (record.key, record.value, record.request_id, record.source_timestamp_ms) == (
        None,
        b'{"q": 1}',
        'req-7',
        1234,
    )
    assert (record.source_topic, record.source_partition, record.source_offset) == (None, None, None)


# --- When no record can be written ---


@pytest.mark.parametrize('dlq', [None, RecordingDLQ(ok=False)])
async def test_no_record_in_stall_mode_raises(dlq):
    from drakkar.models import SinkDeliveryFailedError

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    mgr = manager_with((sink, policy(max_retries=0)))
    mgr.attach_runtime(recorder=None, dlq_sink=dlq, dlq_on_send_failure='stall')  # ty: ignore[invalid-argument-type]
    context = message_context()

    with pytest.raises(SinkDeliveryFailedError):
        await mgr.deliver_all(CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 0, message_context=context)

    assert context.states[0].dlq_written is False


async def test_no_record_in_drop_mode_counts_source_messages():
    from drakkar.metrics import dlq_dropped_payloads

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    mgr = manager_with((sink, policy(max_retries=0)), dlq=RecordingDLQ(ok=False))
    before = dlq_dropped_payloads.labels(partition='4')._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 4, message_context=message_context(1, 2)
        )

    assert outcomes == {KAFKA: DeliveryOutcome.FAILED}
    assert dlq_dropped_payloads.labels(partition='4')._value.get() == before + 2
    [dropped] = [entry for entry in logs if entry['event'] == 'dlq_failure_payloads_dropped']
    assert dropped['message_count'] == 2


@pytest.mark.parametrize('on_send_failure', ['stall', 'drop'])
async def test_missing_context_on_a_dlq_outcome_follows_on_send_failure(on_send_failure):
    """No context is not a window: a dlq outcome with no source message to write fails closed."""
    from drakkar.metrics import dlq_dropped_payloads
    from drakkar.models import SinkDeliveryFailedError

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    dlq = RecordingDLQ()
    mgr = manager_with((sink, policy(max_retries=0)))
    mgr.attach_runtime(recorder=None, dlq_sink=dlq, dlq_on_send_failure=on_send_failure)  # ty: ignore[invalid-argument-type]
    result = CollectResult(kafka=[KafkaPayload(data=Data())])
    before = dlq_dropped_payloads.labels(partition='8')._value.get()

    with capture_logs() as logs:
        if on_send_failure == 'stall':
            with pytest.raises(SinkDeliveryFailedError, match='no message context'):
                await mgr.deliver_all(result, declared, 8)
        else:
            assert await mgr.deliver_all(result, declared, 8) == {KAFKA: DeliveryOutcome.FAILED}

    assert dlq.sent == []
    assert not [entry for entry in logs if entry['event'] in ('window_delivery_not_dlq', 'sink_delivery_skipped')]
    if on_send_failure == 'drop':
        assert dlq_dropped_payloads.labels(partition='8')._value.get() == before + 1
        [dropped] = [entry for entry in logs if entry['event'] == 'dlq_failure_payloads_dropped']
        assert dropped['reason'] == 'no message context'


async def test_lost_dlq_write_under_drop_is_counted_once_per_message():
    """Two failing groups of one message, a DLQ that answers False: one counted loss, not two."""
    from drakkar.metrics import dlq_dropped_payloads

    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    kafka.fail = postgres.fail = True
    dlq = RecordingDLQ(ok=False)
    mgr = manager_with((kafka, policy(max_retries=0)), (postgres, policy(max_retries=0)), dlq=dlq)
    context = message_context()
    before = dlq_dropped_payloads.labels(partition='5')._value.get()

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), declared, 5, message_context=context)

    assert outcomes == {KAFKA: DeliveryOutcome.FAILED, POSTGRES: DeliveryOutcome.FAILED}
    assert len(dlq.sent) == 1
    assert dlq_dropped_payloads.labels(partition='5')._value.get() == before + 1
    assert [entry['event'] for entry in logs].count('dlq_failure_payloads_dropped') == 1
    assert context.states[0].dlq_lost is True
    assert context.states[0].dlq_written is False

    # A later result of the same message fails too: no new write, count or line.
    with capture_logs() as later_logs:
        later = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 5, message_context=context
        )

    assert later == {KAFKA: DeliveryOutcome.FAILED}
    assert len(dlq.sent) == 1
    assert dlq_dropped_payloads.labels(partition='5')._value.get() == before + 1
    assert not [
        entry
        for entry in later_logs
        if entry['event'] in ('dlq_failure_payloads_dropped', 'message_already_dlq', 'message_dlq')
    ]


async def test_lost_execute_dlq_write_is_not_written_or_counted_again_by_a_sink_failure():
    """The flag lives in the lock loop both stages share: an execute loss under drop is the message's only loss."""
    from drakkar.metrics import dlq_dropped_payloads
    from drakkar.models import ExecutorError, ExecutorTask

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    dlq = RecordingDLQ(ok=False)
    mgr = manager_with((sink, policy(max_retries=0)), dlq=dlq)
    context = message_context()
    task = ExecutorTask(task_id='t1', source_offsets=[10])
    before = dlq_dropped_payloads.labels(partition='6')._value.get()

    with capture_logs() as logs:
        await mgr.send_execute_dlq(
            context, task=task, error=ExecutorError(task=task, exit_code=1), attempts=1, partition_id=6
        )
        outcomes = await mgr.deliver_all(
            CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 6, message_context=context
        )

    assert outcomes == {KAFKA: DeliveryOutcome.FAILED}
    assert [record.stage for record, _ in dlq.sent] == ['execute']
    assert dlq_dropped_payloads.labels(partition='6')._value.get() == before + 1
    assert [entry['event'] for entry in logs].count('dlq_failure_payloads_dropped') == 1
    assert 'message_already_dlq' not in [entry['event'] for entry in logs]


async def test_dependent_of_a_lost_dlq_write_gets_dependency_failed():
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    postgres.fail = True
    mgr = manager_with(
        (kafka, policy(depends_on=('postgres/main',))), (postgres, policy(max_retries=0)), dlq=RecordingDLQ(ok=False)
    )

    with capture_logs() as logs:
        outcomes = await mgr.deliver_all(both_results(), declared, 0, message_context=message_context())

    assert outcomes == {KAFKA: DeliveryOutcome.NOT_STARTED, POSTGRES: DeliveryOutcome.FAILED}
    assert [entry['reason'] for entry in logs if entry['event'] == 'sink_not_started'] == ['dependency_failed']


@pytest.mark.parametrize(
    ('context_kind', 'dlq_ok', 'expected_action'),
    [
        ('message', True, 'dlq'),
        ('message', False, 'failed'),
        ('window', True, 'skip'),
        ('none', True, 'failed'),
    ],
)
async def test_sink_error_records_the_real_outcome(context_kind, dlq_ok, expected_action):
    """``action: dlq`` only when a record was written; a window result records a skip, a lost record a failure."""
    sink = FakeSink('out', 'kafka')
    sink.fail = True
    mgr = manager_with((sink, policy(max_retries=0)))
    recorder = CollectingRecorder()
    mgr.attach_runtime(recorder=recorder, dlq_sink=RecordingDLQ(ok=dlq_ok))  # ty: ignore[invalid-argument-type]
    contexts = {'message': message_context(), 'window': MessageContext.window(), 'none': None}

    await mgr.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]), declared, 0, message_context=contexts[context_kind]
    )

    assert [json.loads(row['metadata'])['action'] for row in recorder.of('sink_error')] == [expected_action]


async def test_dlq_off_by_config_counts_an_unconfigured_drop():
    from drakkar.metrics import dlq_unconfigured_drops

    sink = FakeSink('out', 'kafka')
    sink.fail = True
    mgr = manager_with((sink, policy(max_retries=0)))
    mgr.attach_runtime(recorder=None, dlq_sink=None, dlq_on_send_failure='stall', dlq_enabled=False)
    before = dlq_unconfigured_drops._value.get()

    outcomes = await mgr.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]), declared, -1, message_context=message_context()
    )

    assert outcomes == {KAFKA: DeliveryOutcome.FAILED}
    assert dlq_unconfigured_drops._value.get() == before + 1


# --- Observability ---


def test_metrics_names_and_help_texts():
    from drakkar.metrics import (
        dlq_dropped_payloads,
        dlq_records,
        dlq_unconfigured_drops,
        sink_deliveries_not_started,
        sink_deliveries_skipped,
        sink_dlq_messages,
    )

    expected = [
        (sink_dlq_messages, 'drakkar_sink_dlq_messages', (), 'DLQ records written, one per source message, all stages'),
        (
            sink_deliveries_skipped,
            'drakkar_sink_deliveries_skipped',
            ('sink_type', 'sink_name'),
            'Sink deliveries skipped: by the handler, by the declared policy, on an open circuit, '
            'or for a window result that cannot reach the DLQ',
        ),
        (dlq_records, 'drakkar_dlq_records', ('stage',), 'DLQ records written by the stage that failed'),
        (
            dlq_dropped_payloads,
            'drakkar_dlq_dropped_payloads',
            ('partition',),
            'Source messages lost because no DLQ record could be written and dlq.on_send_failure is drop',
        ),
        (
            sink_deliveries_not_started,
            'drakkar_sink_deliveries_not_started',
            ('sink_type', 'sink_name', 'reason'),
            'Sink deliveries not started, by reason',
        ),
        (
            dlq_unconfigured_drops,
            'drakkar_dlq_unconfigured_drops',
            (),
            'Payloads (sink path) or source messages (task path) dropped because no DLQ topic is configured',
        ),
    ]
    for metric, name, labels, help_text in expected:
        assert metric._name == name
        assert metric._labelnames == labels
        assert metric._documentation == help_text


def test_recorder_events_message_dlq_and_sink_not_started():
    recorder = CollectingRecorder()

    recorder.record_message_dlq(
        partition=3,
        offset=10,
        stage='sink',
        sink='kafka/out',
        error='boom',
        delivered_sinks=['postgres/main'],
        attempts=3,
    )
    recorder.record_message_dlq(
        partition=None,
        offset=None,
        stage='sink',
        sink='kafka/out',
        error='boom',
        delivered_sinks=[],
        attempts=1,
        request_id='req-7',
    )
    recorder.record_sink_not_started(sink='redis/cache', reason='message_dlq', partition=3)
    recorder.record_sink_error(sink_type='kafka', sink_name='out', error='boom', attempt=3, action='dlq')

    dlq_row, http_row, not_started_row, error_row = recorder.rows
    assert (http_row['partition'], http_row['offset'], http_row['origin'], http_row['request_id']) == (
        -1,
        None,
        'http',
        'req-7',
    )
    assert (dlq_row['event'], dlq_row['partition'], dlq_row['offset']) == ('message_dlq', 3, 10)
    assert json.loads(dlq_row['metadata']) == {
        'stage': 'sink',
        'sink': 'kafka/out',
        'error': 'boom',
        'delivered_sinks': ['postgres/main'],
        'attempts': 3,
    }
    assert (not_started_row['event'], not_started_row['partition']) == ('sink_not_started', 3)
    assert json.loads(not_started_row['metadata']) == {'sink': 'redis/cache', 'reason': 'message_dlq'}
    assert json.loads(error_row['metadata']) == {
        'sink_type': 'kafka',
        'sink_name': 'out',
        'error': 'boom',
        'attempt': 3,
        'action': 'dlq',
    }


def test_sink_not_started_row_of_an_http_request_carries_the_http_markers():
    recorder = CollectingRecorder()

    recorder.record_sink_not_started(sink='redis/cache', reason='message_dlq', partition=-1, request_id='req-7')

    [row] = recorder.rows
    assert (row['event'], row['partition'], row['origin'], row['request_id']) == (
        'sink_not_started',
        -1,
        'http',
        'req-7',
    )
    assert json.loads(row['metadata']) == {'sink': 'redis/cache', 'reason': 'message_dlq'}


# --- The parse record ---


def parse_record(offset: int = 10) -> DLQRecord:
    message = source_message(offset)
    return DLQRecord(
        key=message.key,
        value=message.value,
        source_headers=message.headers,
        stage='parse',
        error='Invalid JSON',
        source_topic=message.topic,
        source_partition=message.partition,
        source_offset=message.offset,
        source_timestamp_ms=message.timestamp,
    )


async def test_a_written_parse_record_logs_and_records_message_dlq():
    dlq = RecordingDLQ()
    recorder = CollectingRecorder()
    mgr = SinkManager()
    mgr.attach_runtime(recorder=recorder, dlq_sink=dlq, dlq_on_send_failure='drop')  # ty: ignore[invalid-argument-type]

    with capture_logs() as logs:
        sent = await mgr.send_parse_dlq(parse_record(), 3)

    assert sent is True
    assert [partition_id for _, partition_id in dlq.sent] == [3]
    [line] = [line for line in logs if line['event'] == 'message_dlq']
    assert (line['stage'], line['partition'], line['offset'], line['sink']) == ('parse', 3, 10, None)
    [row] = recorder.of('message_dlq')
    assert (row['partition'], row['offset']) == (3, 10)
    assert json.loads(row['metadata']) == {
        'stage': 'parse',
        'sink': None,
        'error': 'Invalid JSON',
        'delivered_sinks': [],
        'attempts': None,
    }


@pytest.mark.parametrize('dlq', [RecordingDLQ(ok=False), None], ids=['write_not_confirmed', 'no_dlq_sink'])
async def test_a_parse_record_not_written_logs_no_message_dlq(dlq):
    recorder = CollectingRecorder()
    mgr = SinkManager()
    mgr.attach_runtime(recorder=recorder, dlq_sink=dlq, dlq_on_send_failure='drop')  # ty: ignore[invalid-argument-type]

    with capture_logs() as logs:
        sent = await mgr.send_parse_dlq(parse_record(), 3)

    assert sent is False
    assert [line for line in logs if line['event'] == 'message_dlq'] == []
    assert recorder.of('message_dlq') == []
