"""The message is done: a DLQ record ends every later delivery of its source message.

The partition gives each task and message result the DLQ state of its source
messages (held by the message tracker), so two results of one message share
one state; ``ErrorAction.DLQ`` from ``on_error`` writes the ``execute``
record at the failure.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    SinkPolicy,
    SinksConfig,
)
from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import dlq_dropped_payloads, dlq_unconfigured_drops
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    KafkaPayload,
    PostgresPayload,
    SinkDeliveryFailedError,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq_record import render_headers
from drakkar.sinks.manager import MessageContext, MessageDLQState, SinkManager
from tests.conftest import wait_for
from tests.test_sink_delivery_policy import CollectingRecorder, RecordingDLQ

OFFSET = 10
FAILURE_STDERR = 'segmentation fault'


class Data(BaseModel):
    value: int = 1


class CountingSink(BaseSink):
    """An in-memory sink that counts its delivery calls; ``fail`` makes each one raise."""

    def __init__(self, name: str, sink_type: str, *, fail: bool = False) -> None:
        super().__init__(name)
        self.sink_type = sink_type
        self.fail = fail
        self.calls = 0
        self.delivered: list[list[BaseModel]] = []

    async def connect(self) -> None:
        pass

    async def deliver(self, payloads: list[BaseModel]) -> None:
        self.calls += 1
        if self.fail:
            raise RuntimeError(f'{self.sink_type} is down')
        self.delivered.append(payloads)

    async def close(self) -> None:
        pass


class ScriptedPool(ExecutorPool):
    """A pool that runs no subprocess: tasks in ``failing`` fail, the others succeed.

    A task with a gate waits for it before it ends, so a test can hold one
    task of a message open while another settles.
    """

    def __init__(
        self, *, failing: set[str] | frozenset[str] = frozenset(), gates: dict[str, asyncio.Event] | None = None
    ) -> None:
        super().__init__(binary_path='/bin/true', max_executors=4, task_timeout_seconds=10)
        self._failing = failing
        self._gates = gates or {}

    async def execute(self, task, recorder=None, partition_id=0):
        gate = self._gates.get(task.task_id)
        if gate is not None:
            await gate.wait()
        if task.task_id in self._failing:
            error = ExecutorError(task=task, exit_code=2, stderr=FAILURE_STDERR)
            result = ExecutorResult(exit_code=2, stdout='', stderr=FAILURE_STDERR, duration_seconds=0.01, task=task)
            raise ExecutorTaskError(error=error, result=result)
        return ExecutorResult(exit_code=0, stdout='ok', stderr='', duration_seconds=0.01, task=task)


class FanOutHandler(BaseDrakkarHandler):
    """Arranges the named tasks for every message; each task result is ``result_for(task_id)``."""

    def __init__(self, task_ids: list[str], results: dict[str, CollectResult] | None = None) -> None:
        self.task_ids = task_ids
        self.results = results or {}

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(task_id=task_id, args=['x'], source_offsets=[msg.offset])
            for msg in messages
            for task_id in self.task_ids
        ]

    async def on_task_complete(self, result):
        return self.results.get(result.task.task_id)


def source_message(offset: int = OFFSET) -> SourceMessage:
    return SourceMessage(
        topic='orders',
        partition=0,
        offset=offset,
        key=b'order-key',
        value=b'{"id": 17}',
        timestamp=1_793_000_000_456,
        headers=[('traceparent', b'00-ab-01')],
    )


def postgres_result() -> CollectResult:
    return CollectResult(postgres=[PostgresPayload(table='t', data=Data())])


def kafka_result() -> CollectResult:
    return CollectResult(kafka=[KafkaPayload(data=Data())])


class Pipeline:
    """One partition processor wired to a real sink manager, a recording DLQ and a recorder."""

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        pool: ExecutorPool,
        *sinks: BaseSink,
        on_dlq_failure: str = 'drop',
        dlq: RecordingDLQ | None = None,
        with_execute_dlq: bool = True,
    ) -> None:
        self.dlq = dlq if dlq is not None else RecordingDLQ()
        self.recorder = CollectingRecorder()
        self.contexts: list[MessageContext | None] = []
        self.manager = SinkManager()
        for sink in sinks:
            self.manager.register(sink, policy=SinkPolicy(on_failure='dlq', max_retries=0, depends_on=()))
        self.manager.attach_runtime(
            recorder=self.recorder,  # ty: ignore[invalid-argument-type]
            dlq_sink=self.dlq,  # ty: ignore[invalid-argument-type]
            dlq_on_send_failure=on_dlq_failure,
        )

        async def on_collect(result, partition_id, message_context=None):
            self.contexts.append(message_context)
            await self.manager.deliver_all(
                result,
                on_delivery_error=handler.on_delivery_error,
                partition_id=partition_id,
                message_context=message_context,
            )

        self.processor = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=pool,
            window_size=10,
            max_retries=3,
            on_collect=on_collect,
            on_dlq_failure=on_dlq_failure,
            execute_dlq=self.manager.send_execute_dlq if with_execute_dlq else None,
        )

    def offset_completed(self) -> bool:
        return self.processor.offset_tracker.committable() is not None

    async def run(self, *messages: SourceMessage) -> None:
        for message in messages or (source_message(),):
            self.processor.enqueue(message)
        self.processor.start()

    async def stop(self) -> None:
        await self.processor.stop()


# --- Sink failures of two results of one message ---


async def test_two_results_failing_together_write_one_record():
    arrived = 0
    both_in_flight = asyncio.Event()

    class MeetingSink(CountingSink):
        async def deliver(self, payloads):
            nonlocal arrived
            arrived += 1
            if arrived == 2:
                both_in_flight.set()
            # Both task results reach the failing sink before either fails,
            # so the two failures meet at the message's lock.
            await both_in_flight.wait()
            raise RuntimeError('postgres is down')

    sink = MeetingSink('main', 'postgres')
    handler = FanOutHandler(['t-a', 't-b'], {'t-a': postgres_result(), 't-b': postgres_result()})
    pipeline = Pipeline(handler, ScriptedPool(), sink)

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    assert len(pipeline.dlq.sent) == 1
    assert len(pipeline.recorder.of('message_dlq')) == 1
    assert len([line for line in logs if line['event'] == 'message_already_dlq']) == 1
    record, _ = pipeline.dlq.sent[0]
    assert (record.stage, record.sink, record.source_offset) == ('sink', 'postgres/main', OFFSET)
    # Both results carried the same state, the tracker's.
    task_states = [context.states[0] for context in pipeline.contexts if context is not None]
    assert len(task_states) == 2
    assert task_states[0] is task_states[1]


async def test_later_task_results_of_a_dlq_message_are_not_delivered():
    record_written = asyncio.Event()

    class SignallingDLQ(RecordingDLQ):
        async def send(self, record, partition_id):
            ok = await super().send(record, partition_id)
            record_written.set()
            return ok

    sink = CountingSink('main', 'postgres', fail=True)
    handler = FanOutHandler(['t-first', 't-later'], {'t-first': postgres_result(), 't-later': postgres_result()})
    pipeline = Pipeline(handler, ScriptedPool(gates={'t-later': record_written}), sink, dlq=SignallingDLQ())

    await pipeline.run()
    await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    assert sink.calls == 1, 'the later result of the message never reached the sink'
    assert len(pipeline.dlq.sent) == 1
    not_started = pipeline.recorder.of('sink_not_started')
    assert len(not_started) == 1
    assert '"reason":"message_dlq"' in not_started[0]['metadata']


async def test_on_message_complete_fires_but_its_result_is_not_delivered():
    groups = []

    class MessageResultHandler(FanOutHandler):
        async def on_message_complete(self, group):
            groups.append(group)
            return kafka_result()

    failing = CountingSink('main', 'postgres', fail=True)
    kafka = CountingSink('out', 'kafka')
    handler = MessageResultHandler(['t-1'], {'t-1': postgres_result()})
    pipeline = Pipeline(handler, ScriptedPool(), failing, kafka)

    await pipeline.run()
    await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    assert len(groups) == 1, 'on_message_complete fired'
    assert kafka.calls == 0, 'the message result was not delivered'
    message_context = pipeline.contexts[-1]
    assert message_context is not None
    assert [message.offset for message in message_context.source_messages] == [OFFSET]
    assert message_context.is_on_dlq
    assert len(pipeline.recorder.of('sink_not_started')) == 1


async def test_offset_completes_after_every_task_settles():
    release = asyncio.Event()

    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    handler = DLQOnError(['t-fails', 't-slow'])
    pipeline = Pipeline(handler, ScriptedPool(failing={'t-fails'}, gates={'t-slow': release}))

    await pipeline.run()
    await wait_for(lambda: len(pipeline.dlq.sent) == 1)
    await asyncio.sleep(0.05)
    assert not pipeline.offset_completed(), 'a task of the message is still running'

    release.set()
    await wait_for(pipeline.offset_completed)
    assert pipeline.processor.offset_tracker.committable() == OFFSET + 1
    await pipeline.stop()


# --- ErrorAction.DLQ ---


async def test_error_action_dlq_writes_the_record_at_the_failure():
    release = asyncio.Event()
    groups = []
    answers = iter([ErrorAction.RETRY, ErrorAction.DLQ])

    class RetryThenDLQ(FanOutHandler):
        async def on_error(self, task, error):
            return next(answers)

        async def on_message_complete(self, group):
            groups.append(group)
            return None

    handler = RetryThenDLQ(['t-fails', 't-slow'])
    pipeline = Pipeline(handler, ScriptedPool(failing={'t-fails'}, gates={'t-slow': release}))

    await pipeline.run()
    # Written at the failure: the other task of the message is still running.
    await wait_for(lambda: len(pipeline.dlq.sent) == 1)
    assert groups == []
    release.set()
    await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    record, _ = pipeline.dlq.sent[0]
    assert record.stage == 'execute'
    assert record.task_id == 't-fails'
    assert record.attempts == 2, 'the first run and one retry'
    assert record.sink is None
    assert record.error == f'exit code 2: {FAILURE_STDERR}'
    assert (record.key, record.value) == (b'order-key', b'{"id": 17}')
    assert (record.source_topic, record.source_partition, record.source_offset) == ('orders', 0, OFFSET)
    assert record.source_headers == [('traceparent', b'00-ab-01')]
    rows = pipeline.recorder.of('message_dlq')
    assert len(rows) == 1
    assert '"stage":"execute"' in rows[0]['metadata']
    assert groups[0].failed == 1, 'the task counts as failed'
    assert groups[0].succeeded == 1


async def test_a_task_of_a_message_already_on_the_dlq_is_only_logged():
    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    handler = DLQOnError(['t-1', 't-2'])
    pipeline = Pipeline(handler, ScriptedPool(failing={'t-1', 't-2'}))

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    assert len(pipeline.dlq.sent) == 1
    already = [line for line in logs if line['event'] == 'message_already_dlq']
    assert len(already) == 1
    assert already[0]['stage'] == 'execute'


@pytest.mark.parametrize(('on_failure', 'expected'), [('skip', ErrorAction.SKIP), ('dlq', ErrorAction.DLQ)])
async def test_executor_on_failure_dlq_is_the_default_hook_answer(tmp_path, on_failure, expected):
    config = DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092'),
        storage={'dir': str(tmp_path / 'storage')},
        sources={'kafka': {'enabled': True, 'topic': 'test-in'}},
        executor=ExecutorConfig(binary_path='/bin/echo', on_failure=on_failure),
        sinks=SinksConfig(kafka={'results': {'topic': 'out'}}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui={'release': {'enabled': False}},
    )
    handler = FanOutHandler(['t-1'])
    DrakkarApp(handler=handler, config=config)
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[OFFSET])

    action = await handler.on_error(task, ExecutorError(task=task))

    assert action is expected
    assert await BaseDrakkarHandler().on_error(task, ExecutorError(task=task)) is ErrorAction.SKIP


async def test_delivered_sinks_header_lists_earlier_deliveries_sorted():
    release = asyncio.Event()

    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    both = CollectResult(
        postgres=[PostgresPayload(table='t', data=Data())],
        kafka=[KafkaPayload(data=Data())],
    )
    postgres = CountingSink('main', 'postgres')
    kafka = CountingSink('results', 'kafka')
    handler = DLQOnError(['t-delivers', 't-fails'], {'t-delivers': both})
    pool = ScriptedPool(failing={'t-fails'}, gates={'t-fails': release})
    pipeline = Pipeline(handler, pool, postgres, kafka)

    await pipeline.run()
    await wait_for(lambda: postgres.calls == 1 and kafka.calls == 1)
    release.set()
    await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    record, _ = pipeline.dlq.sent[0]
    assert record.delivered_sinks == ['kafka/results', 'postgres/main']
    assert ('dk-delivered-sinks', b'kafka/results,postgres/main') in render_headers(record)


async def test_window_results_have_no_tracker_and_no_record():
    class WindowHandler(FanOutHandler):
        async def on_window_complete(self, results, source_messages):
            return postgres_result()

    sink = CountingSink('main', 'postgres', fail=True)
    pipeline = Pipeline(WindowHandler(['t-1']), ScriptedPool(), sink)

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(lambda: sink.calls == 1)
        await wait_for(lambda: any(line['event'] == 'window_delivery_not_dlq' for line in logs))
    await pipeline.stop()

    window_context = pipeline.contexts[-1]
    assert window_context is not None
    assert window_context.is_window
    assert list(window_context.states) == []
    assert pipeline.dlq.sent == []


# --- When the execute record cannot be written ---


async def test_execute_dlq_without_a_writer_under_drop_counts_the_lost_message():
    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    before = dlq_dropped_payloads.labels(partition='0')._value.get()
    pipeline = Pipeline(DLQOnError(['t-1']), ScriptedPool(failing={'t-1'}), with_execute_dlq=False)

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    assert dlq_dropped_payloads.labels(partition='0')._value.get() == before + 1
    dropped = [line for line in logs if line['event'] == 'dlq_failure_payloads_dropped']
    assert len(dropped) == 1
    assert dropped[0]['message_count'] == 1


async def test_execute_dlq_without_a_writer_under_stall_holds_the_offset():
    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    pipeline = Pipeline(
        DLQOnError(['t-1']),
        ScriptedPool(failing={'t-1'}),
        on_dlq_failure='stall',
        with_execute_dlq=False,
    )

    await pipeline.run()
    await wait_for(lambda: OFFSET in pipeline.processor._stalled_offsets)
    await pipeline.stop()

    assert not pipeline.offset_completed()


async def test_failed_execute_dlq_write_under_stall_holds_the_offset():
    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    pipeline = Pipeline(
        DLQOnError(['t-1']),
        ScriptedPool(failing={'t-1'}),
        on_dlq_failure='stall',
        dlq=RecordingDLQ(ok=False),
    )

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(lambda: OFFSET in pipeline.processor._stalled_offsets)
    await pipeline.stop()

    assert not pipeline.offset_completed()
    assert any(line['event'] == 'execute_dlq_failed' for line in logs)
    assert pipeline.recorder.of('message_dlq') == []


async def test_a_raising_execute_dlq_write_still_settles_the_task():
    class DLQOnError(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    pipeline = Pipeline(DLQOnError(['t-1']), ScriptedPool(failing={'t-1'}))

    async def broken_write(message_context, **kwargs):
        raise RuntimeError('broken writer')

    pipeline.processor._execute_dlq = broken_write

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(pipeline.offset_completed)
    await pipeline.stop()

    failed = [line for line in logs if line['event'] == 'execute_dlq_failed']
    assert len(failed) == 1
    assert failed[0]['error'] == 'broken writer'


async def test_send_execute_dlq_without_a_context_follows_on_send_failure():
    manager = SinkManager()
    manager.attach_runtime(recorder=None, dlq_sink=RecordingDLQ(), dlq_on_send_failure='stall')  # ty: ignore[invalid-argument-type]
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[OFFSET])

    with pytest.raises(SinkDeliveryFailedError, match='no message context'):
        await manager.send_execute_dlq(None, task=task, error=ExecutorError(task=task), attempts=1, partition_id=0)


async def test_send_execute_dlq_with_the_dlq_off_counts_an_unconfigured_drop():
    manager = SinkManager()
    manager.attach_runtime(recorder=None, dlq_sink=None, dlq_on_send_failure='stall', dlq_enabled=False)
    task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[OFFSET])
    before = dlq_unconfigured_drops._value.get()

    await manager.send_execute_dlq(
        MessageContext(source_messages=[source_message()], states=[MessageDLQState()]),
        task=task,
        error=ExecutorError(task=task),
        attempts=1,
        partition_id=0,
    )

    assert dlq_unconfigured_drops._value.get() == before + 1


async def test_a_task_result_with_no_tracked_offset_has_no_context_and_fails_closed():
    class StrayTaskHandler(FanOutHandler):
        async def arrange(self, messages, pending):
            # An offset no message of the window has: the handler's mistake.
            return [ExecutorTask(task_id='t-stray', args=['x'], source_offsets=[999])]

    sink = CountingSink('main', 'postgres', fail=True)
    pipeline = Pipeline(StrayTaskHandler(['t-stray'], {'t-stray': postgres_result()}), ScriptedPool(), sink)

    with capture_logs() as logs:
        await pipeline.run()
        await wait_for(lambda: any(line['event'] == 'dlq_failure_payloads_dropped' for line in logs))
    await pipeline.stop()

    assert pipeline.contexts == [None]
    dropped = [line for line in logs if line['event'] == 'dlq_failure_payloads_dropped']
    assert dropped[0]['reason'] == 'no message context'
    assert pipeline.dlq.sent == []
