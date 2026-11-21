"""The sink manager's support for split: captured DLQ outcomes, delivery counts, job records and sink stages.

A message in capture mode keeps its DLQ outcome in its state instead of
writing a record; every delivery of a message is counted per sink;
``send_job_dlq`` writes one record for an original message; ``stage_error``
refuses a result that names a sink of the other split stage.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.models import (
    CollectResult,
    DeliveryAction,
    ExecutorError,
    ExecutorTask,
    KafkaPayload,
    PostgresPayload,
    RedisPayload,
    SourceMessage,
)
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq_record import render_headers
from drakkar.sinks.manager import DeliveryOutcome, MessageContext, MessageDLQState, SinkManager
from drakkar.split.models import PartDLQ, SinkCounts
from tests.test_sink_delivery_policy import CollectingRecorder, RecordingDLQ, policy

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'dlq_contract.json').read_text(encoding='utf-8'))
JOB_CASES = [case for case in FIXTURE['cases'] if case['input']['stage'] in ('split', 'aggregate')]


class Data(BaseModel):
    value: int = 1


class ScriptedSink(BaseSink):
    """An in-memory sink; ``fail`` makes each delivery raise."""

    def __init__(self, name: str, sink_type: str, *, fail: bool = False) -> None:
        super().__init__(name)
        self.sink_type = sink_type
        self.fail = fail
        self.delivered: list[list[BaseModel]] = []

    async def connect(self) -> None:
        pass

    async def deliver(self, payloads: list[BaseModel]) -> None:
        if self.fail:
            raise RuntimeError(f'{self.sink_type} is down')
        self.delivered.append(payloads)

    async def close(self) -> None:
        pass


def source_message() -> SourceMessage:
    return SourceMessage(
        topic='items',
        partition=2,
        offset=40,
        key=None,
        value=b'{"id": 1}',
        timestamp=1_793_000_000_000,
        headers=[('trace', b'00-ab-01'), ('no-value', None)],
        request_id='req_1',
    )


def capture_context() -> MessageContext:
    return MessageContext(source_messages=[source_message()], states=[MessageDLQState(capture=True)])


def plain_context() -> MessageContext:
    return MessageContext(source_messages=[source_message()], states=[MessageDLQState()])


def build_manager(*sinks: tuple[BaseSink, str], dlq: RecordingDLQ | None, dlq_unconfigured: bool = False):
    recorder = CollectingRecorder()
    manager = SinkManager()
    for sink, on_failure in sinks:
        manager.register(sink, policy=policy(on_failure=on_failure, max_retries=0))
    manager.attach_runtime(
        recorder=recorder,  # ty: ignore[invalid-argument-type]
        dlq_sink=dlq,  # ty: ignore[invalid-argument-type]
        dlq_unconfigured=dlq_unconfigured,
    )
    return manager, recorder


async def declared(error):
    return error.declared_policy


# --- Capture mode ---


async def test_a_sink_failure_in_capture_mode_writes_nothing_and_captures_the_outcome():
    dlq = RecordingDLQ()
    manager, recorder = build_manager((ScriptedSink('main', 'postgres', fail=True), 'dlq'), dlq=dlq)
    dependent = ScriptedSink('extra', 'redis')
    manager.register(dependent, policy=policy(depends_on=('postgres/main',)))
    context = capture_context()
    result = CollectResult(
        postgres=[PostgresPayload(table='t', data=Data())],
        redis=[RedisPayload(key='k', data=Data())],
    )

    with capture_logs() as logs:
        outcomes = await manager.deliver_all(
            result, on_delivery_error=declared, partition_id=2, message_context=context
        )

    state = context.states[0]
    assert dlq.sent == []
    assert outcomes[('postgres', 'main')] == DeliveryOutcome.DLQ
    assert outcomes[('redis', 'extra')] == DeliveryOutcome.NOT_STARTED
    assert state.dlq_written is True
    assert state.captured == PartDLQ(stage='sink', sink='postgres/main', error='postgres is down', attempts=1)
    assert dependent.delivered == []
    assert [json.loads(row['metadata'])['reason'] for row in recorder.of('sink_not_started')] == ['message_dlq']
    assert recorder.of('message_dlq') == []
    assert not [line for line in logs if line['event'] == 'message_dlq']


async def test_a_later_result_of_a_captured_message_does_not_start():
    manager, _ = build_manager((ScriptedSink('main', 'postgres', fail=True), 'dlq'), dlq=RecordingDLQ())
    sink = ScriptedSink('results', 'kafka')
    manager.register(sink)
    context = capture_context()
    await manager.deliver_all(
        CollectResult(postgres=[PostgresPayload(table='t', data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )

    outcomes = await manager.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )

    assert outcomes == {('kafka', 'results'): DeliveryOutcome.NOT_STARTED}
    assert sink.delivered == []


async def test_an_execute_dlq_in_capture_mode_captures_the_task():
    dlq = RecordingDLQ()
    manager, recorder = build_manager(dlq=dlq)
    context = capture_context()
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[40])

    await manager.send_execute_dlq(
        context, task=task, error=ExecutorError(task=task, exit_code=2, stderr='boom'), attempts=3, partition_id=2
    )

    assert dlq.sent == []
    assert recorder.of('message_dlq') == []
    assert context.states[0].dlq_written is True
    assert context.states[0].captured == PartDLQ(stage='execute', task_id='t-1', error='exit code 2: boom', attempts=3)


@pytest.mark.parametrize('dlq_unconfigured', [False, True])
async def test_capture_mode_without_a_dlq_sink_still_captures(dlq_unconfigured):
    manager, _ = build_manager(
        (ScriptedSink('main', 'postgres', fail=True), 'dlq'), dlq=None, dlq_unconfigured=dlq_unconfigured
    )
    context = capture_context()
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[40])

    outcomes = await manager.deliver_all(
        CollectResult(postgres=[PostgresPayload(table='t', data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )
    execute_context = capture_context()
    await manager.send_execute_dlq(
        execute_context, task=task, error=ExecutorError(task=task, kind='timeout'), attempts=1, partition_id=2
    )

    assert outcomes == {('postgres', 'main'): DeliveryOutcome.DLQ}
    assert context.states[0].captured is not None
    assert context.states[0].captured.stage == 'sink'
    assert execute_context.states[0].captured is not None
    assert execute_context.states[0].captured.stage == 'execute'


async def test_the_captured_error_is_cut_as_the_record_would_be():
    manager, _ = build_manager(dlq=None)
    context = capture_context()
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[40])

    await manager.send_execute_dlq(
        context, task=task, error=ExecutorError(task=task, stderr='x' * 3000), attempts=1, partition_id=2
    )

    assert context.states[0].captured is not None
    assert len(context.states[0].captured.error) == 2000


# --- Delivery counts ---


async def test_counts_record_a_delivered_a_skipped_and_a_failed_delivery():
    manager, _ = build_manager(
        (ScriptedSink('main', 'postgres', fail=True), 'skip'),
        (ScriptedSink('results', 'kafka'), 'dlq'),
        (ScriptedSink('cache', 'redis', fail=True), 'dlq'),
        dlq=RecordingDLQ(),
    )
    context = plain_context()
    result = CollectResult(
        postgres=[PostgresPayload(table='t', data=Data()), PostgresPayload(table='t', data=Data())],
        kafka=[KafkaPayload(data=Data())],
    )

    await manager.deliver_all(result, on_delivery_error=declared, partition_id=2, message_context=context)
    await manager.deliver_all(
        CollectResult(kafka=[KafkaPayload(data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )
    await manager.deliver_all(
        CollectResult(redis=[RedisPayload(key='k', data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )

    assert context.states[0].counts == {
        'postgres/main': SinkCounts(skipped=1),
        'kafka/results': SinkCounts(delivered=2),
        'redis/cache': SinkCounts(failed=1),
    }


async def test_counts_leave_out_a_delivery_that_did_not_start():
    manager, _ = build_manager((ScriptedSink('main', 'postgres', fail=True), 'dlq'), dlq=RecordingDLQ())
    manager.register(ScriptedSink('results', 'kafka'), policy=policy(depends_on=('postgres/main',)))
    context = plain_context()

    await manager.deliver_all(
        CollectResult(postgres=[PostgresPayload(table='t', data=Data())], kafka=[KafkaPayload(data=Data())]),
        on_delivery_error=declared,
        partition_id=2,
        message_context=context,
    )

    assert context.states[0].counts == {'postgres/main': SinkCounts(failed=1)}


async def test_a_hook_skip_counts_as_skipped():
    manager, _ = build_manager((ScriptedSink('main', 'postgres', fail=True), 'dlq'), dlq=RecordingDLQ())
    context = plain_context()

    async def skip(error):
        return DeliveryAction.SKIP

    await manager.deliver_all(
        CollectResult(postgres=[PostgresPayload(table='t', data=Data())]),
        on_delivery_error=skip,
        partition_id=2,
        message_context=context,
    )

    assert context.states[0].counts == {'postgres/main': SinkCounts(skipped=1)}


# --- Job records ---


def _job_context(case_input: dict) -> MessageContext:
    message = SourceMessage(
        topic=case_input['source_topic'],
        partition=case_input['source_partition'],
        offset=case_input['source_offset'],
        key=_fixture_bytes(case_input['key']),
        value=_fixture_bytes(case_input['value']) or b'',
        timestamp=case_input['source_timestamp_ms'],
        headers=[(name, _fixture_bytes(value)) for name, value in case_input['source_headers']],
        request_id=case_input['request_id'],
    )
    state = MessageDLQState(delivered_sinks=set(case_input['delivered_sinks']))
    return MessageContext(source_messages=[message], states=[state])


def _fixture_bytes(value: str | dict | None) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return base64.b64decode(value['base64'])
    return value.encode('utf-8')


@pytest.mark.parametrize('case', JOB_CASES, ids=lambda case: case['name'])
async def test_send_job_dlq_writes_the_record_of_the_fixture(case):
    facts = case['input']
    dlq = RecordingDLQ()
    manager, recorder = build_manager(dlq=dlq)
    context = _job_context(facts)

    written = await manager.send_job_dlq(
        context,
        stage=facts['stage'],
        error=facts['error'],
        sink=facts['sink'],
        task_id=facts['task_id'],
        attempts=facts['attempts'],
        partition_id=facts['source_partition'],
    )

    assert written is True
    assert len(dlq.sent) == 1
    record, partition_id = dlq.sent[0]
    stamped = record.model_copy(
        update={
            'worker': facts['worker'],
            'instance': facts['instance'],
            'failed_at': datetime.fromisoformat(facts['failed_at']),
        }
    )
    expected = [(name, _fixture_bytes(value)) for name, value in case['expected_headers']]
    assert render_headers(stamped) == expected
    assert record.key == _fixture_bytes(facts['key'])
    assert record.value == _fixture_bytes(facts['value'])
    assert partition_id == facts['source_partition']
    assert len(recorder.of('message_dlq')) == 1


async def test_send_job_dlq_keeps_a_null_key_and_a_header_with_no_value():
    dlq = RecordingDLQ()
    manager, _ = build_manager(dlq=dlq)

    await manager.send_job_dlq(
        plain_context(), stage='aggregate', error='e', sink=None, task_id=None, attempts=None, partition_id=2
    )

    record, _ = dlq.sent[0]
    assert record.key is None
    assert record.source_headers == [('trace', b'00-ab-01'), ('no-value', None)]
    assert ('no-value', None) in render_headers(record)


async def test_a_second_send_job_dlq_writes_nothing_and_answers_true():
    dlq = RecordingDLQ()
    manager, _ = build_manager(dlq=dlq)
    context = plain_context()
    arguments = {'stage': 'split', 'error': 'e', 'sink': None, 'task_id': None, 'attempts': 3, 'partition_id': 2}

    assert await manager.send_job_dlq(context, **arguments) is True
    assert await manager.send_job_dlq(context, **arguments) is True
    assert len(dlq.sent) == 1


async def test_send_job_dlq_answers_false_when_the_write_fails():
    manager, _ = build_manager(dlq=RecordingDLQ(ok=False))

    written = await manager.send_job_dlq(
        plain_context(), stage='split', error='e', sink=None, task_id=None, attempts=1, partition_id=2
    )

    assert written is False


async def test_send_job_dlq_without_a_dlq_sink_answers_false():
    manager, _ = build_manager(dlq=None)
    context = plain_context()

    written = await manager.send_job_dlq(
        context, stage='split', error='e', sink=None, task_id=None, attempts=1, partition_id=2
    )

    assert written is False
    assert context.states[0].dlq_written is False


async def test_send_job_dlq_with_an_unconfigured_dlq_counts_a_drop():
    manager, _ = build_manager(dlq=None, dlq_unconfigured=True)

    with capture_logs() as logs:
        written = await manager.send_job_dlq(
            plain_context(), stage='aggregate', error='e', sink=None, task_id=None, attempts=None, partition_id=2
        )

    assert written is False
    assert [line['event'] for line in logs] == ['dlq_send_dropped_unconfigured']


async def test_send_job_dlq_refuses_a_message_in_capture_mode():
    manager, _ = build_manager(dlq=RecordingDLQ())

    with pytest.raises(ValueError, match='send_job_dlq needs a message that is not in capture mode'):
        await manager.send_job_dlq(
            capture_context(), stage='split', error='e', sink=None, task_id=None, attempts=1, partition_id=2
        )


# --- Sink stages ---


def stage_manager() -> SinkManager:
    manager = SinkManager()
    manager.register(ScriptedSink('main', 'postgres'))
    manager.register(ScriptedSink('results', 'kafka'))
    manager.register(ScriptedSink('summary', 'kafka'))
    return manager


def test_stage_error_is_none_when_stages_are_not_set():
    manager = stage_manager()
    result = CollectResult(kafka=[KafkaPayload(sink='summary', data=Data())])

    assert manager.stage_error(result, 'part') is None
    assert manager.stage_error(result, 'aggregate') is None


def test_stage_error_refuses_an_aggregate_sink_in_a_part_result():
    manager = stage_manager()
    manager.set_split_stages(frozenset({('kafka', 'summary')}))
    result = CollectResult(
        postgres=[PostgresPayload(table='t', data=Data())],
        kafka=[KafkaPayload(sink='results', data=Data()), KafkaPayload(sink='summary', data=Data())],
    )

    assert manager.stage_error(result, 'part') == 'sink kafka/summary belongs to the aggregate stage of split'


def test_stage_error_refuses_a_part_sink_in_an_aggregate_result():
    manager = stage_manager()
    manager.set_split_stages(frozenset({('kafka', 'summary')}))
    result = CollectResult(
        postgres=[PostgresPayload(table='t', data=Data())],
        kafka=[KafkaPayload(sink='summary', data=Data())],
    )

    assert manager.stage_error(result, 'aggregate') == 'sink postgres/main belongs to the part stage of split'


def test_stage_error_accepts_a_result_of_its_own_stage():
    manager = stage_manager()
    manager.set_split_stages(frozenset({('kafka', 'summary')}))

    assert manager.stage_error(CollectResult(postgres=[PostgresPayload(table='t', data=Data())]), 'part') is None
    assert manager.stage_error(CollectResult(kafka=[KafkaPayload(sink='summary', data=Data())]), 'aggregate') is None


def test_stage_error_resolves_a_default_sink_and_ignores_an_unknown_one():
    manager = stage_manager()
    manager.set_split_stages(frozenset({('postgres', 'main')}))
    result = CollectResult(
        kafka=[KafkaPayload(data=Data()), KafkaPayload(sink='missing', data=Data())],
        postgres=[PostgresPayload(table='t', data=Data())],
    )

    # The kafka payloads name no resolvable sink (two kafka sinks, and a missing name).
    assert manager.stage_error(result, 'part') == 'sink postgres/main belongs to the aggregate stage of split'
