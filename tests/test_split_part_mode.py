"""Part mode: a partition processor that runs parts of a split job and reports each result.

Every test drives a real ``PartitionProcessor`` with ``part_mode`` set, a
real sink manager and a fake ``report``. No subprocess runs: the pool is
scripted.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError
from pydantic import BaseModel
from structlog.contextvars import get_contextvars
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.budget import PendingBudget
from drakkar.config.connections import KafkaConnectionConfig
from drakkar.config.sinks import DEFAULT_SINK_POLICY
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import (
    delivery_stalled_offsets,
    messages_consumed,
    queued_messages_dropped_at_stop,
    split_parts_processed,
    split_result_write_failures,
    split_results_ignored,
)
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    KafkaPayload,
    MessageGroup,
    PostgresPayload,
    SinkDeliveryFailedError,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.sinks.manager import MessageContext, MessageDLQState, SinkManager
from drakkar.split import partmode
from drakkar.split.models import PartDLQ, PartResult, SinkCounts
from drakkar.split.partmode import PartMode, ResultWriter, build_part_result, part_collect, part_request_id
from drakkar.split.producer import SplitProduceError, SplitProducer
from drakkar.split.records import PARTS_PARTITION_BASE, JobRef, parse_log_record, parse_part_headers, part_record
from tests.conftest import wait_for
from tests.split_helpers import FakeLog, FakeProducerClient
from tests.stream_helpers import make_pool, stream_task
from tests.test_message_dlq import CountingSink, FanOutHandler, ScriptedPool
from tests.test_sink_delivery_policy import CollectingRecorder, RecordingDLQ

GROUP = 'workers'
SOURCE_TOPIC = 'requests'
PARTS_TOPIC = 'requests_workers_split_parts'
RESULTS_TOPIC = 'requests_workers_split_results'
SOURCE_PARTITION = 7
SOURCE_OFFSET = 500
PARTS_PARTITION = 2
SHOWN_PARTITION = PARTS_PARTITION_BASE + PARTS_PARTITION
WORKER = 'worker-b'
REQUEST_ID = 'req-of-the-original'
AGGREGATE_SINK = ('kafka', 'summaries')
STAGE_ERROR = 'sink kafka/summaries belongs to the aggregate stage of split'


class Data(BaseModel):
    value: int = 1


class Summary(BaseModel):
    items_done: int = 0
    note: str = ''


def job_ref(group: str = GROUP) -> JobRef:
    return JobRef(group=group, partition=SOURCE_PARTITION, offset=SOURCE_OFFSET, attempt=1, attempt_id='ab' * 8)


def part_message(
    offset: int = 0,
    *,
    index: int | None = None,
    group: str = GROUP,
    value: bytes = b'{"value": 1}',
    request_id: str | None = REQUEST_ID,
) -> SourceMessage:
    """A message of the parts topic, with the headers the owner of the job writes."""
    record = part_record(
        job_ref(group),
        source_topic=SOURCE_TOPIC,
        index=offset if index is None else index,
        request_id=request_id,
        key=b'request-key',
        value=value,
        source_headers=[('traceparent', b'00-ab-01')],
        partition_count=4,
    )
    return SourceMessage(
        topic=PARTS_TOPIC,
        partition=PARTS_PARTITION,
        offset=offset,
        key=record.key,
        value=value,
        timestamp=1_793_000_000_456,
        headers=list(record.headers),
    )


def plain_message(offset: int = 0) -> SourceMessage:
    """A message of the parts topic that carries no split headers."""
    return SourceMessage(topic=PARTS_TOPIC, partition=PARTS_PARTITION, offset=offset, value=b'{}', timestamp=0)


def postgres_result() -> CollectResult:
    return CollectResult(postgres=[PostgresPayload(table='t', data=Data())])


def aggregate_result() -> CollectResult:
    return CollectResult(kafka=[KafkaPayload(sink='summaries', data=Data())])


class Reports:
    """A ``report`` double: keeps each result, can hold the report open and can fail it."""

    def __init__(self) -> None:
        self.calls: list[tuple[SourceMessage, PartResult]] = []
        self.gate: asyncio.Event | None = None
        self.error: BaseException | None = None

    async def __call__(self, message: SourceMessage, result: PartResult) -> None:
        self.calls.append((message, result))
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error

    @property
    def results(self) -> list[PartResult]:
        return [result for _, result in self.calls]

    def by_index(self) -> dict[int, PartResult]:
        return {result.index: result for result in self.results}


class PartPipeline:
    """One processor in part mode, wired to a real sink manager with one aggregate sink."""

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        pool: ExecutorPool | None = None,
        *sinks: CountingSink,
        producer: Any = None,
        max_part_data: int = 65536,
        window_size: int = 10,
        **processor_kwargs: Any,
    ) -> None:
        self.reports = Reports()
        self.dlq = RecordingDLQ()
        self.recorder = CollectingRecorder()
        self.commits: list[tuple[int, int]] = []
        self.stalls: list[int] = []
        self.manager = SinkManager()
        for sink in (*sinks, CountingSink(AGGREGATE_SINK[1], AGGREGATE_SINK[0])):
            self.manager.register(sink, policy=DEFAULT_SINK_POLICY.model_copy(update={'max_retries': 0}))
        self.manager.attach_runtime(
            recorder=self.recorder,  # ty: ignore[invalid-argument-type]
            dlq_sink=self.dlq,  # ty: ignore[invalid-argument-type]
            dlq_on_send_failure='drop',
        )
        self.manager.set_split_stages(frozenset({AGGREGATE_SINK}))

        async def on_collect(result, partition_id, message_context=None):
            return await self.manager.deliver_all(
                result,
                on_delivery_error=handler.on_delivery_error,
                partition_id=partition_id,
                message_context=message_context,
            )

        async def on_commit(partition_id: int, offset: int) -> None:
            self.commits.append((partition_id, offset))

        async def on_stall(partition_id: int) -> None:
            self.stalls.append(partition_id)

        # With a producer the report is the real writer, which reads the stop of this processor.
        self.stopped = asyncio.Event()
        report: Any = self.reports
        if producer is not None:
            writer = ResultWriter(
                producer, results_topic=RESULTS_TOPIC, stopped=self.stopped, partition=SHOWN_PARTITION
            )
            report = writer.write
        part_mode = PartMode(
            worker=WORKER,
            max_part_data=max_part_data,
            part_data_model=None,
            report=report,
            source_group=GROUP,
            stopped=self.stopped,
        )
        self.processor = PartitionProcessor(
            partition_id=PARTS_PARTITION,
            handler=handler,
            executor_pool=pool if pool is not None else ScriptedPool(),
            window_size=window_size,
            max_retries=3,
            on_collect=part_collect(self.manager, on_collect),
            on_commit=on_commit,
            on_stall=on_stall,
            recorder=self.recorder,  # ty: ignore[invalid-argument-type]
            execute_dlq=self.manager.send_execute_dlq,
            part_mode=part_mode,
            partition_label=SHOWN_PARTITION,
            **processor_kwargs,
        )

    def pending(self) -> int:
        return self.processor.offset_tracker.pending_count

    def all_done(self) -> bool:
        processor = self.processor
        return processor.queue_size == 0 and not processor.offset_tracker.has_pending() and not processor.inflight_count

    async def run(self, *messages: SourceMessage) -> None:
        """Enqueue the messages, run the loop until every offset completed, then stop."""
        self.start(*messages)
        await wait_for(self.all_done)
        await self.processor.stop()

    def start(self, *messages: SourceMessage) -> None:
        for message in messages or (part_message(),):
            self.processor.enqueue(message)
        self.processor.start()


def one_task_handler(**results: CollectResult) -> FanOutHandler:
    return FanOutHandler(['t-a'], dict(results))


# --- the result of a part, and the order of its end ---


async def test_ok_part_reports_counters_deliveries_and_data_before_its_offset_completes():
    class Handler(FanOutHandler):
        async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
            return postgres_result()

        async def part_data(self, group: MessageGroup) -> Summary:
            return Summary(items_done=len(group.results))

    sink = CountingSink('main', 'postgres')
    pipeline = PartPipeline(Handler(['t-a', 't-b'], {'t-a': postgres_result()}), None, sink)
    pipeline.reports.gate = asyncio.Event()
    before = split_parts_processed.labels(outcome='ok')._value.get()

    pipeline.start(part_message(offset=4, index=9))
    await wait_for(lambda: len(pipeline.reports.calls) == 1)
    # The report has not returned: the offset is still pending and nothing is counted.
    assert pipeline.pending() == 1
    assert pipeline.commits == []
    assert split_parts_processed.labels(outcome='ok')._value.get() == before

    pipeline.reports.gate.set()
    await wait_for(pipeline.all_done)
    await pipeline.processor.stop()

    message, result = pipeline.reports.calls[0]
    assert message.split is not None and message.split.job_id == f'{SOURCE_PARTITION}:{SOURCE_OFFSET}#1'
    assert (result.index, result.worker, result.outcome) == (9, WORKER, 'ok')
    assert (result.tasks_succeeded, result.tasks_failed, result.tasks_replaced) == (2, 0, 0)
    assert result.deliveries == {'postgres/main': SinkCounts(delivered=2)}
    assert result.data == Summary(items_done=2)
    assert (result.dlq, result.error, result.data_error) == (None, None, None)
    assert result.finished_at > 1_700_000_000 and result.duration_seconds >= 0
    assert split_parts_processed.labels(outcome='ok')._value.get() == before + 1
    # The commit goes to the real partition of the parts topic.
    assert pipeline.commits == [(PARTS_PARTITION, 5)]
    assert pipeline.dlq.sent == []


async def test_a_failed_task_under_skip_leaves_the_part_ok():
    pipeline = PartPipeline(FanOutHandler(['t-a', 't-bad']), ScriptedPool(failing={'t-bad'}))

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'ok'
    assert (result.tasks_succeeded, result.tasks_failed) == (1, 1)
    assert result.dlq is None


async def test_a_task_sent_to_the_dlq_fails_the_part_and_writes_no_record():
    class Handler(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

    pipeline = PartPipeline(Handler(['t-bad']), ScriptedPool(failing={'t-bad'}))
    before = split_parts_processed.labels(outcome='failed')._value.get()

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.dlq is not None
    assert (result.dlq.stage, result.dlq.task_id, result.dlq.attempts) == ('execute', 't-bad', 1)
    assert 'segmentation fault' in result.dlq.error
    assert pipeline.dlq.sent == []
    assert pipeline.recorder.of('message_dlq') == []
    assert split_parts_processed.labels(outcome='failed')._value.get() == before + 1


@pytest.mark.parametrize('dlq_first', [True, False])
async def test_one_task_of_two_sent_to_the_dlq_ends_the_part_for_the_other_task_too(dlq_first):
    ok_gate, bad_gate = asyncio.Event(), asyncio.Event()

    class Handler(FanOutHandler):
        async def on_error(self, task, error):
            return ErrorAction.DLQ

        async def on_message_complete(self, group):
            return postgres_result()

    sink = CountingSink('main', 'postgres')
    pool = ScriptedPool(failing={'t-bad'}, gates={'t-ok': ok_gate, 't-bad': bad_gate})
    pipeline = PartPipeline(Handler(['t-ok', 't-bad'], {'t-ok': postgres_result()}), pool, sink)
    pipeline.reports.gate = asyncio.Event()
    processor = pipeline.processor

    pipeline.start()
    await wait_for(lambda: processor.inflight_count == 2)
    first, second = (bad_gate, ok_gate) if dlq_first else (ok_gate, bad_gate)
    first.set()
    await wait_for(lambda: processor.inflight_count == 1)
    # One task has ended: nothing is reported while the other one runs.
    assert pipeline.reports.calls == []
    second.set()
    await wait_for(lambda: len(pipeline.reports.calls) == 1)
    # The report is in flight: the offset has not completed.
    assert pipeline.pending() == 1
    assert pipeline.commits == []
    pipeline.reports.gate.set()
    await wait_for(pipeline.all_done)
    await processor.stop()

    [result] = pipeline.reports.results
    assert (result.outcome, result.tasks_succeeded, result.tasks_failed) == ('failed', 1, 1)
    assert result.dlq is not None
    assert (result.dlq.stage, result.dlq.task_id, result.dlq.sink, result.dlq.attempts) == ('execute', 't-bad', None, 1)
    # No record reaches the DLQ sink, and the message result of the part is never delivered.
    assert pipeline.dlq.sent == []
    not_started = pipeline.recorder.of('sink_not_started')
    if dlq_first:
        # The other task ended after the outcome: its result did not start either.
        assert sink.calls == 0
        assert result.deliveries == {}
        assert len(not_started) == 2
    else:
        # The other task had delivered its result before the outcome.
        assert sink.calls == 1
        assert result.deliveries == {'postgres/main': SinkCounts(delivered=1)}
        assert len(not_started) == 1
    assert all('"reason":"message_dlq"' in row['metadata'] for row in not_started)
    assert pipeline.commits == [(PARTS_PARTITION, 1)]


async def test_executor_on_failure_dlq_ends_the_part_with_the_capture():
    class Handler(FanOutHandler):
        async def on_error(self, task, error):
            raise RuntimeError('the hook failed, so executor.on_failure decides')

    pipeline = PartPipeline(Handler(['t-bad']), ScriptedPool(failing={'t-bad'}), executor_on_failure='dlq')

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.dlq is not None and result.dlq.stage == 'execute'
    assert pipeline.dlq.sent == []


async def test_a_sink_under_on_failure_dlq_fails_the_part_with_stage_sink():
    sink = CountingSink('main', 'postgres', fail=True)
    pipeline = PartPipeline(one_task_handler(**{'t-a': postgres_result()}), None, sink)

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.dlq == PartDLQ(stage='sink', sink='postgres/main', error='postgres is down', attempts=1)
    assert result.deliveries == {'postgres/main': SinkCounts(failed=1)}
    assert pipeline.dlq.sent == []


async def test_a_part_whose_message_hook_raised_is_still_reported_ok():
    class Handler(FanOutHandler):
        async def on_message_complete(self, group):
            raise RuntimeError('hook bug')

    pipeline = PartPipeline(Handler(['t-a']))

    with capture_logs() as logs:
        await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'ok'
    assert [line['event'] for line in logs if line['event'] == 'on_message_complete_failed'] == [
        'on_message_complete_failed'
    ]


async def test_a_part_with_no_tasks_is_reported_ok():
    class Handler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

    pipeline = PartPipeline(Handler())

    await pipeline.run(part_message(offset=0), part_message(offset=1))

    assert sorted(result.index for result in pipeline.reports.results) == [0, 1]
    assert {result.outcome for result in pipeline.reports.results} == {'ok'}
    assert pipeline.commits[-1] == (PARTS_PARTITION, 2)


# --- failures that become a failed part result ---


@pytest.mark.parametrize('policy', ['skip', 'dlq', 'raise'])
async def test_a_parse_failure_makes_a_failed_result_whatever_the_parse_policy_says(policy):
    class Handler(FanOutHandler):
        input_model = Data

    handler = Handler(['t-a'])
    pipeline = PartPipeline(handler, on_parse_error=policy)

    await pipeline.run(part_message(offset=0, value=b'not json'), part_message(offset=1))

    results = pipeline.reports.by_index()
    assert results[0].outcome == 'failed'
    assert results[0].error is not None and 'json' in results[0].error.lower()
    assert (results[0].tasks_succeeded, results[0].dlq) == (0, None)
    assert results[1].outcome == 'ok'
    assert pipeline.dlq.sent == []
    assert not pipeline.processor.is_dead
    assert pipeline.commits[-1] == (PARTS_PARTITION, 2)


async def test_a_raise_of_arrange_fails_every_part_of_the_window_and_the_loop_goes_on():
    class Handler(BaseDrakkarHandler):
        def __init__(self) -> None:
            self.calls = 0

        async def arrange(self, messages, pending):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError('cannot arrange postgres://user:secret@db/x ' + 'é' * 3000)
            return [ExecutorTask(task_id=f't-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]

    handler = Handler()
    # No budget: the first window takes both queued messages.
    pipeline = PartPipeline(handler)
    processor = pipeline.processor
    processor.enqueue(part_message(offset=0))
    processor.enqueue(part_message(offset=1))

    with capture_logs() as logs:
        processor.start()
        await wait_for(lambda: len(pipeline.reports.calls) == 2)
        processor.enqueue(part_message(offset=2))
        await wait_for(pipeline.all_done)
        await processor.stop()

    results = pipeline.reports.by_index()
    assert [results[index].outcome for index in (0, 1, 2)] == ['failed', 'failed', 'ok']
    assert results[0].error == results[1].error
    error = results[0].error
    assert error is not None and error.startswith('cannot arrange postgres://')
    assert 'secret' not in error
    assert len(error.encode('utf-8')) <= 2000
    assert handler.calls == 2
    assert not processor.is_dead
    assert pipeline.commits[-1] == (PARTS_PARTITION, 3)
    failed = [line for line in logs if line['event'] == 'split_part_arrange_failed']
    assert len(failed) == 1
    assert (failed[0]['partition'], failed[0]['offsets']) == (SHOWN_PARTITION, [0, 1])
    assert not [line for line in logs if line['event'] == 'partition_processor_restarting']


@pytest.mark.parametrize(
    ('answer', 'error_part'),
    [
        (lambda task: (task,), 'arrange returned tuple, want a list of ExecutorTask'),
        (
            lambda task: [SimpleNamespace(task_id='x', source_offsets=[0], request_id=None, labels={})],
            'arrange returned a list with SimpleNamespace, want a list of ExecutorTask',
        ),
        (lambda task: None, 'NoneType'),
    ],
)
async def test_an_answer_of_arrange_that_is_no_task_list_fails_the_parts_like_a_raise(answer, error_part):
    class Handler(BaseDrakkarHandler):
        def __init__(self) -> None:
            self.calls = 0

        async def arrange(self, messages, pending):
            self.calls += 1
            tasks = [ExecutorTask(task_id=f't-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]
            return answer(tasks[0]) if self.calls == 1 else tasks

    pipeline = PartPipeline(Handler())
    processor = pipeline.processor

    with capture_logs() as logs:
        pipeline.start(part_message(offset=0))
        await wait_for(lambda: len(pipeline.reports.calls) == 1)
        processor.enqueue(part_message(offset=1))
        await wait_for(pipeline.all_done)
        await processor.stop()

    results = pipeline.reports.by_index()
    assert results[0].outcome == 'failed'
    assert results[0].error is not None and error_part in results[0].error
    assert results[1].outcome == 'ok'
    assert not processor.is_dead
    assert pipeline.commits[-1] == (PARTS_PARTITION, 2)
    assert not [line for line in logs if line['event'] == 'partition_processor_restarting']


async def test_a_stream_error_fails_the_part_after_its_started_groups_ended():
    gate = asyncio.Event()

    class Handler(BaseDrakkarHandler):
        def __init__(self) -> None:
            self.completed: list[str] = []
            self.message_hook_calls = 0

        async def arrange_stream(self, message):
            if message.offset == 0:
                yield [ExecutorTask(task_id='held', args=['x'], source_offsets=[0])]
                raise RuntimeError('the stream broke')
            yield [stream_task(f'm{message.offset}', message.offset)]

        async def on_task_complete(self, result):
            self.completed.append(result.task.task_id)
            return None

        async def on_message_complete(self, group):
            self.message_hook_calls += 1
            return None

    handler = Handler()
    pipeline = PartPipeline(handler, ScriptedPool(gates={'held': gate}))

    with capture_logs() as logs:
        pipeline.start(part_message(offset=0), part_message(offset=1))
        # The next part runs while the failed one still waits for its started group.
        await wait_for(lambda: 1 in pipeline.reports.by_index())
        assert 0 not in pipeline.reports.by_index()
        gate.set()
        await wait_for(pipeline.all_done)
        await pipeline.processor.stop()

    results = pipeline.reports.by_index()
    assert (results[0].outcome, results[0].error, results[0].tasks_succeeded) == ('failed', 'the stream broke', 1)
    assert results[1].outcome == 'ok'
    assert 'held' in handler.completed
    # The failed part has no message hook; the other part has one.
    assert handler.message_hook_calls == 1
    assert not pipeline.processor.is_dead
    assert pipeline.commits[-1] == (PARTS_PARTITION, 2)
    assert [line['event'] for line in logs if line['event'] == 'arrange_stream_failed'] == ['arrange_stream_failed']
    assert not [line for line in logs if line['event'] == 'partition_processor_restarting']


async def test_a_broken_stream_rule_fails_the_part():
    class Handler(BaseDrakkarHandler):
        async def arrange_stream(self, message):
            yield [stream_task('other', message.offset + 99)]

    pipeline = PartPipeline(Handler(), make_pool())

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.error


# --- part_data ---


async def test_part_data_that_raises_is_dropped_with_the_reason():
    class Handler(FanOutHandler):
        async def part_data(self, group):
            raise ValueError('no summary')

    pipeline = PartPipeline(Handler(['t-a']))

    with capture_logs() as logs:
        await pipeline.run()

    [result] = pipeline.reports.results
    assert (result.outcome, result.data, result.data_error) == ('ok', None, 'part_data raised: no summary')
    assert [line['partition'] for line in logs if line['event'] == 'part_data_failed'] == [SHOWN_PARTITION]


async def test_part_data_above_the_limit_is_dropped_with_both_sizes():
    data = Summary(items_done=1, note='é' * 600)
    size = len(data.model_dump_json().encode('utf-8'))

    class Handler(FanOutHandler):
        async def part_data(self, group):
            return data

    pipeline = PartPipeline(Handler(['t-a']), max_part_data=1024)

    await pipeline.run()

    [result] = pipeline.reports.results
    assert size > 1024
    assert (result.outcome, result.data) == ('ok', None)
    assert result.data_error == f'part data is {size} bytes, the limit is 1024'


async def test_part_data_that_is_no_model_is_dropped():
    class Handler(FanOutHandler):
        async def part_data(self, group):
            return {'items_done': 1}

    pipeline = PartPipeline(Handler(['t-a']))

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.data is None
    assert result.data_error == 'part_data raised: part_data returned dict, want a pydantic model'


async def test_part_data_alone_gets_the_details_of_the_group():
    seen: list[MessageGroup] = []

    class Handler(FanOutHandler):
        async def part_data(self, group):
            seen.append(group)
            # The hook context of the call names the shown partition.
            assert get_contextvars()['hook'] == 'part_data'
            return None

    pipeline = PartPipeline(Handler(['t-a', 't-bad']), ScriptedPool(failing={'t-bad'}))

    await pipeline.run()

    [group] = seen
    assert [result.task.task_id for result in group.results] == ['t-a']
    assert [error.task.task_id for error in group.errors] == ['t-bad']
    assert len(group.tasks) == 2
    assert pipeline.reports.results[0].data is None


# --- sink stages ---


async def test_a_result_that_names_an_aggregate_sink_is_not_delivered_and_fails_the_part():
    aggregate_sink_calls: list[int] = []

    class Handler(FanOutHandler):
        async def on_message_complete(self, group):
            return postgres_result()

    sink = CountingSink('main', 'postgres')
    pipeline = PartPipeline(Handler(['t-a'], {'t-a': aggregate_result()}), None, sink)
    aggregate = pipeline.manager.sinks[AGGREGATE_SINK]
    aggregate.deliver = lambda payloads: aggregate_sink_calls.append(len(payloads))  # ty: ignore[invalid-assignment]

    await pipeline.run()

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.dlq == PartDLQ(stage='sink', sink=None, error=STAGE_ERROR, attempts=None)
    assert aggregate_sink_calls == []
    # The part ended as a message on the DLQ ends: its message result was not delivered either.
    assert sink.calls == 0
    assert pipeline.dlq.sent == []


async def test_a_window_result_of_parts_is_delivered_as_before():
    class Handler(FanOutHandler):
        async def on_window_complete(self, results, source_messages):
            return postgres_result()

    sink = CountingSink('main', 'postgres')
    pipeline = PartPipeline(Handler(['t-a']), None, sink)

    await pipeline.run()
    await wait_for(lambda: sink.calls == 1)

    assert pipeline.reports.results[0].outcome == 'ok'


async def test_a_window_result_that_names_an_aggregate_sink_is_not_delivered_and_is_logged():
    aggregate_sink_calls: list[int] = []

    class Handler(FanOutHandler):
        async def on_window_complete(self, results, source_messages):
            return aggregate_result()

    pipeline = PartPipeline(Handler(['t-a']))
    aggregate = pipeline.manager.sinks[AGGREGATE_SINK]
    aggregate.deliver = lambda payloads: aggregate_sink_calls.append(len(payloads))  # ty: ignore[invalid-assignment]

    with capture_logs() as logs:
        pipeline.start()
        await wait_for(lambda: any(line['event'] == 'split_stage_error' for line in logs))
        await wait_for(pipeline.all_done)
        await pipeline.processor.stop()

    # A window result has no message that could carry the outcome: the part itself is ok.
    assert aggregate_sink_calls == []
    assert pipeline.reports.results[0].outcome == 'ok'
    [line] = [line for line in logs if line['event'] == 'split_stage_error']
    assert (line['log_level'], line['category'], line['partition'], line['error']) == (
        'error',
        'split',
        SHOWN_PARTITION,
        STAGE_ERROR,
    )


async def test_capture_stage_error_keeps_the_first_outcome_and_needs_capture_mode():
    manager = SinkManager()
    message = part_message()
    state = MessageDLQState(capture=True)
    context = MessageContext(source_messages=[message], states=[state])

    await manager.capture_stage_error(context, sink=None, error='first')
    await manager.capture_stage_error(context, sink=None, error='second')

    assert state.dlq_written
    assert state.captured == PartDLQ(stage='sink', error='first')
    plain = MessageContext(source_messages=[message], states=[MessageDLQState()])
    with pytest.raises(ValueError, match='capture mode'):
        await manager.capture_stage_error(plain, sink=None, error='x')


# --- which messages are parts ---


async def test_records_without_split_headers_or_of_another_group_are_skipped():
    handler = one_task_handler()
    pipeline = PartPipeline(handler)
    malformed = split_results_ignored.labels(reason='malformed')._value.get()
    foreign = split_results_ignored.labels(reason='foreign_group')._value.get()

    with capture_logs() as logs:
        await pipeline.run(
            plain_message(offset=0),
            part_message(offset=1, group='another-group'),
            plain_message(offset=2),
            part_message(offset=3),
        )

    assert [result.index for result in pipeline.reports.results] == [3]
    assert split_results_ignored.labels(reason='malformed')._value.get() == malformed + 2
    assert split_results_ignored.labels(reason='foreign_group')._value.get() == foreign + 1
    assert pipeline.commits[-1] == (PARTS_PARTITION, 4)
    invalid = [line for line in logs if line['event'] == 'split_part_invalid']
    # One line for each reason: the second malformed record is held back.
    assert [(line['partition'], line['offset'], line['reason']) for line in invalid] == [
        (SHOWN_PARTITION, 0, 'malformed'),
        (SHOWN_PARTITION, 1, 'foreign_group'),
    ]
    assert all(line['category'] == 'split' for line in invalid)


async def test_the_request_id_comes_from_the_header_and_the_hook_is_not_asked():
    hook_calls: list[int] = []
    task_request_ids: list[str | None] = []

    class Handler(FanOutHandler):
        def request_id(self, msg):
            hook_calls.append(msg.offset)
            return 'from-the-hook'

        async def on_task_complete(self, result):
            task_request_ids.append(result.task.request_id)
            return None

    pipeline = PartPipeline(Handler(['t-a']))

    await pipeline.run(part_message(offset=0), part_message(offset=1, request_id=None))

    with_header, without_header = (message for message, _ in sorted(pipeline.reports.calls, key=lambda c: c[1].index))
    assert hook_calls == []
    assert with_header.request_id == REQUEST_ID
    assert without_header.request_id is not None and without_header.request_id.startswith('req_')
    assert REQUEST_ID in task_request_ids


def test_part_request_id_refuses_a_header_that_is_no_request_id():
    assert part_request_id([('dk-request-id', b'req-1')]) == 'req-1'
    assert part_request_id([('dk-request-id', b'has space')]) is None
    assert part_request_id([('dk-request-id', b'\xff\xfe')]) is None
    assert part_request_id([('dk-request-id', None)]) is None
    assert part_request_id([('other', b'x')]) is None
    assert part_request_id(None) is None


async def test_what_the_processor_shows_uses_the_shown_partition_number():
    class Handler(FanOutHandler):
        async def on_message_complete(self, group):
            return None

    pipeline = PartPipeline(Handler(['t-a', 't-bad']), ScriptedPool(failing={'t-bad'}))
    consumed = messages_consumed.labels(partition=str(SHOWN_PARTITION))._value.get()
    real_label = messages_consumed.labels(partition=str(PARTS_PARTITION))._value.get()

    await pipeline.run()

    assert pipeline.processor.partition_id == PARTS_PARTITION
    assert pipeline.processor.partition_label == SHOWN_PARTITION
    assert messages_consumed.labels(partition=str(SHOWN_PARTITION))._value.get() == consumed + 1
    assert messages_consumed.labels(partition=str(PARTS_PARTITION))._value.get() == real_label
    rows = [row for row in pipeline.recorder.rows if 'partition' in row]
    assert {row['event'] for row in rows} >= {'consumed', 'arranged', 'task_failed', 'message_complete'}
    assert {row['partition'] for row in rows} == {SHOWN_PARTITION}
    # The handler sees the real partition of the parts topic.
    assert pipeline.reports.calls[0][0].partition == PARTS_PARTITION


async def test_a_processor_without_part_mode_treats_a_part_as_an_ordinary_message():
    hook_calls: list[int] = []
    contexts: list[MessageContext | None] = []

    class Handler(FanOutHandler):
        def request_id(self, msg):
            hook_calls.append(msg.offset)
            return 'from-the-hook'

    async def on_collect(result, partition_id, message_context=None):
        contexts.append(message_context)
        return {}

    processor = PartitionProcessor(
        partition_id=0,
        handler=Handler(['t-a'], {'t-a': postgres_result()}),
        executor_pool=ScriptedPool(),
        window_size=10,
        on_collect=on_collect,
    )
    message = part_message()

    processor.enqueue(message)
    processor.start()
    await wait_for(lambda: processor.queue_size == 0 and not processor.offset_tracker.has_pending())
    await processor.stop()

    # The split headers mean nothing here: the hook names the request and no DLQ outcome is captured.
    assert message.split is None
    assert (hook_calls, message.request_id) == ([0], 'from-the-hook')
    [context] = contexts
    assert context is not None and not context.is_captured
    assert processor.partition_label == processor.partition_id == 0


async def test_a_raise_of_arrange_without_part_mode_still_restarts_the_partition_loop():
    class Handler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            raise RuntimeError('cannot arrange')

    processor = PartitionProcessor(partition_id=0, handler=Handler(), executor_pool=ScriptedPool(), window_size=10)

    with capture_logs() as logs:
        processor.enqueue(part_message(offset=0))
        processor.start()
        await wait_for(lambda: any(line['event'] == 'partition_processor_restarting' for line in logs))
        await processor.stop(timeout=0.2)

    # The offset of the crashed window stays pending, as before.
    assert processor.offset_tracker.pending_count == 1
    assert not [line for line in logs if line['event'] == 'split_part_arrange_failed']


# --- the report: a stop, a final error, an unconfirmed delivery ---


class DownProducer:
    """A producer whose every record fails with an error a retry could fix."""

    def __init__(self) -> None:
        self.attempts = 0

    async def publish(self, topic: str, record: Any) -> None:
        self.attempts += 1
        raise SplitProduceError('broker transport failure', final=False)


class RecordingProducer:
    """A producer that confirms each record at once, or never."""

    def __init__(self, *, confirms: bool = True) -> None:
        self.confirms = confirms
        self.records: list[Any] = []

    async def publish(self, topic: str, record: Any) -> None:
        self.records.append(record)
        if not self.confirms:
            await asyncio.Event().wait()


async def test_a_stop_while_the_report_waits_in_its_backoff_ends_the_drain_with_the_offset_pending():
    producer = DownProducer()
    pipeline = PartPipeline(one_task_handler(), producer=producer)
    processor = pipeline.processor
    failures = split_result_write_failures._value.get()

    with capture_logs() as logs:
        pipeline.start()
        await wait_for(lambda: producer.attempts == 1 and processor.inflight_count == 0)
        # The write failed once and now waits a second before its next attempt.
        processor.signal_stop()
        await asyncio.wait_for(processor.drain(), timeout=10)
        await processor.stop(timeout=1.0)

    assert producer.attempts == 1
    assert split_result_write_failures._value.get() == failures + 1
    # The offset was neither completed nor stalled: the next owner runs the part again.
    assert pipeline.pending() == 1
    assert processor.offset_tracker.committable() is None
    assert pipeline.commits == []
    assert pipeline.stalls == []
    [retry] = [line for line in logs if line['event'] == 'split_result_write_failed']
    assert retry == {
        'event': 'split_result_write_failed',
        'log_level': 'warning',
        'category': 'split',
        'job': f'{SOURCE_PARTITION}:{SOURCE_OFFSET}#1',
        'partition': SHOWN_PARTITION,
        'part': 0,
        'error': 'broker transport failure',
        'final': False,
        'attempt': 1,
        'suppressed': 0,
    }
    assert not [line for line in logs if line['event'] == 'zombie_tasks_cancelled']


async def test_a_part_that_finishes_while_its_partition_drains_is_still_reported():
    gate = asyncio.Event()
    producer = RecordingProducer()
    pipeline = PartPipeline(one_task_handler(), ScriptedPool(gates={'t-a': gate}), producer=producer)
    processor = pipeline.processor

    pipeline.start()
    await wait_for(lambda: processor.inflight_count == 1)
    processor.signal_stop()
    drain = asyncio.create_task(processor.drain())
    gate.set()
    await asyncio.wait_for(drain, timeout=10)
    await processor.stop()

    # The stop cut no first attempt: the result was written and the offset committed.
    assert len(producer.records) == 1
    assert pipeline.pending() == 0
    assert pipeline.commits == [(PARTS_PARTITION, 1)]


async def test_a_write_with_no_delivery_report_holds_a_drain_only_for_the_stop_grace(monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.05)
    gate = asyncio.Event()
    producer = RecordingProducer(confirms=False)
    pipeline = PartPipeline(one_task_handler(), ScriptedPool(gates={'t-a': gate}), producer=producer)
    processor = pipeline.processor

    pipeline.start()
    await wait_for(lambda: processor.inflight_count == 1)
    processor.signal_stop()
    drain = asyncio.create_task(processor.drain())
    gate.set()
    await asyncio.wait_for(drain, timeout=10)
    await processor.stop()

    assert len(producer.records) == 1
    assert pipeline.pending() == 1
    assert pipeline.commits == []
    assert pipeline.stalls == []


async def test_a_stop_during_the_message_hook_still_ends_the_part_with_its_report():
    hook_entered = asyncio.Event()
    hook_may_end = asyncio.Event()

    class Handler(FanOutHandler):
        async def on_message_complete(self, group):
            hook_entered.set()
            await hook_may_end.wait()
            return postgres_result()

    sink = CountingSink('main', 'postgres')
    producer = RecordingProducer()
    pipeline = PartPipeline(Handler(['t-a']), None, sink, producer=producer)
    processor = pipeline.processor

    pipeline.start()
    await asyncio.wait_for(hook_entered.wait(), 2)
    processor.signal_stop()
    drain = asyncio.create_task(processor.drain())
    await asyncio.sleep(0.05)
    # The drain waits for the hook: the part's offset is still pending.
    assert not drain.done()
    assert producer.records == []
    hook_may_end.set()
    await asyncio.wait_for(drain, timeout=10)
    await processor.stop()

    # The message result was delivered, then the part was reported, then its offset was committed.
    assert sink.calls == 1
    assert len(producer.records) == 1
    assert pipeline.commits == [(PARTS_PARTITION, 1)]
    assert pipeline.pending() == 0


async def test_a_report_that_can_never_be_written_stalls_the_offset():
    pipeline = PartPipeline(one_task_handler())
    pipeline.reports.error = partmode.PartReportError('Broker: Unknown topic or partition', attempt=3)
    stalled = delivery_stalled_offsets.labels(partition=str(SHOWN_PARTITION))._value.get()
    processed = split_parts_processed.labels(outcome='ok')._value.get()

    with capture_logs() as logs:
        pipeline.start(part_message(offset=0), part_message(offset=1))
        await wait_for(lambda: len(pipeline.processor._stalled_offsets) == 2)
        await asyncio.wait_for(pipeline.processor.drain(), timeout=10)
        await pipeline.processor.stop()

    assert pipeline.pending() == 2
    # The partition is paused once, and one line is written for both parts.
    assert pipeline.stalls == [PARTS_PARTITION]
    assert pipeline.commits == []
    assert delivery_stalled_offsets.labels(partition=str(SHOWN_PARTITION))._value.get() == stalled + 2
    assert split_parts_processed.labels(outcome='ok')._value.get() == processed
    # The same fields as the writer's retry line.
    [line] = [line for line in logs if line['event'] == 'split_result_write_failed']
    assert line == {
        'event': 'split_result_write_failed',
        'log_level': 'error',
        'category': 'split',
        'job': f'{SOURCE_PARTITION}:{SOURCE_OFFSET}#1',
        'partition': SHOWN_PARTITION,
        'part': 0,
        'error': 'Broker: Unknown topic or partition',
        'final': True,
        'attempt': 3,
        'suppressed': 0,
    }


async def test_a_report_that_fails_for_another_reason_stalls_the_offset_too():
    pipeline = PartPipeline(one_task_handler())
    pipeline.reports.error = RuntimeError('a bug in the report')

    with capture_logs() as logs:
        pipeline.start()
        await wait_for(lambda: pipeline.stalls == [PARTS_PARTITION])
        await pipeline.processor.stop()

    assert pipeline.pending() == 1
    [line] = [line for line in logs if line['event'] == 'split_result_write_failed']
    assert (line['error'], line['final'], line['attempt']) == ('a bug in the report', True, 1)
    assert isinstance(line['exc_info'], RuntimeError)


async def test_a_report_cut_by_the_drain_timeout_leaves_the_offset_pending():
    pipeline = PartPipeline(one_task_handler())
    pipeline.reports.gate = asyncio.Event()

    pipeline.start()
    await wait_for(lambda: len(pipeline.reports.calls) == 1)
    pipeline.processor.suppress_deliveries()
    cancelled = await pipeline.processor.cancel_active_tasks()
    await pipeline.processor.stop()

    assert cancelled == 1
    assert pipeline.pending() == 1
    assert pipeline.stalls == []


async def test_a_report_cancelled_before_it_starts_leaves_the_drain():
    pipeline = PartPipeline(one_task_handler())
    processor = pipeline.processor
    message = part_with_split()
    processor.offset_tracker.register(message.offset)

    processor._spawn_part_report(message, ok_result())
    [report] = processor._active_tasks
    report.cancel()
    await asyncio.gather(report, return_exceptions=True)

    # The report never ran, so nothing was written: the offset stays pending and no drain waits for it.
    assert pipeline.reports.calls == []
    assert pipeline.pending() == 1
    assert not processor._has_drainable_pending()
    assert processor._waiting_reports == 0


async def test_a_part_that_ends_after_the_drain_timed_out_is_not_reported():
    gate = asyncio.Event()
    pipeline = PartPipeline(one_task_handler(), ScriptedPool(gates={'t-a': gate}))

    pipeline.start()
    await wait_for(lambda: pipeline.processor.inflight_count == 1)
    pipeline.processor.suppress_deliveries()
    gate.set()
    await wait_for(lambda: pipeline.processor.inflight_count == 0)
    await pipeline.processor.stop()

    assert pipeline.reports.calls == []
    assert pipeline.pending() == 1


async def test_a_part_with_an_unconfirmed_delivery_stalls_and_is_not_reported():
    class Handler(FanOutHandler):
        part_data_calls = 0

        async def part_data(self, group):
            type(self).part_data_calls += 1
            return None

    handler = Handler(['t-a'], {'t-a': postgres_result()})
    pipeline = PartPipeline(handler)

    async def unconfirmed(result, partition_id, message_context=None):
        raise SinkDeliveryFailedError(sink_name='main', sink_type='postgres', reason='not confirmed')

    pipeline.processor._on_collect = unconfirmed

    pipeline.start()
    await wait_for(lambda: pipeline.stalls == [PARTS_PARTITION])
    await pipeline.processor.stop()

    assert pipeline.reports.calls == []
    assert Handler.part_data_calls == 0


# --- the bound on finished parts that wait for their report ---


class OneGroupStreamHandler(BaseDrakkarHandler):
    """A stream handler with one task for each part."""

    async def arrange_stream(self, message):
        yield [stream_task(f'm{message.offset}', message.offset)]


IN_FLIGHT_TASKS = 4


def small_budget() -> PendingBudget:
    return PendingBudget(max_tasks=IN_FLIGHT_TASKS, max_bytes=1_000_000_000)


def bounded_handler(mode: str) -> BaseDrakkarHandler:
    return one_task_handler() if mode == 'list' else OneGroupStreamHandler()


def test_the_cap_on_waiting_reports_is_twice_the_window_size_and_at_least_sixteen():
    assert partmode.max_waiting_reports(1) == 16
    assert partmode.max_waiting_reports(8) == 16
    assert partmode.max_waiting_reports(100) == 200


@pytest.mark.parametrize('mode', ['list', 'stream'])
async def test_the_loop_takes_no_part_while_too_many_reports_wait_and_goes_on_when_they_end(mode):
    total = 60
    # The budget bounds the parts in flight, as in a worker; the cap bounds the finished ones that wait.
    pipeline = PartPipeline(bounded_handler(mode), make_pool(), window_size=1, budget=small_budget())
    processor = pipeline.processor
    cap = processor._max_waiting_reports
    pipeline.reports.gate = asyncio.Event()

    pipeline.start(*(part_message(offset=offset) for offset in range(total)))
    await wait_for(lambda: len(pipeline.reports.calls) >= cap and processor.inflight_count == 0)
    reported = len(pipeline.reports.calls)
    # The reports are held by the gate of the test; a longer wait proves nothing more.
    await asyncio.sleep(0.05)

    # The loop stopped at the cap (a part that was in flight may pass it by a few), and the rest stay queued.
    assert cap == 16
    assert len(pipeline.reports.calls) == reported
    assert cap <= reported <= cap + IN_FLIGHT_TASKS + 1
    assert processor.queue_size == total - reported
    assert pipeline.commits == []

    pipeline.reports.gate.set()
    await wait_for(pipeline.all_done)
    await processor.stop()

    assert len(pipeline.reports.calls) == total
    assert pipeline.commits[-1] == (PARTS_PARTITION, total)
    assert processor._waiting_reports == 0


@pytest.mark.parametrize('mode', ['list', 'stream'])
async def test_a_stop_wakes_a_loop_that_waits_for_reports_to_end(mode, monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.02)
    producer = RecordingProducer(confirms=False)
    pipeline = PartPipeline(bounded_handler(mode), make_pool(), producer=producer, window_size=1, budget=small_budget())
    processor = pipeline.processor
    dropped = queued_messages_dropped_at_stop._value.get()

    pipeline.start(*(part_message(offset=offset) for offset in range(40)))
    await wait_for(lambda: len(producer.records) >= processor._max_waiting_reports and processor.inflight_count == 0)
    # From now on only the stop can end the loop's wait: its own time bound is far away.
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 30.0)
    await asyncio.sleep(0.1)
    written = len(producer.records)

    processor.signal_stop()
    await asyncio.wait_for(processor.drain(), timeout=10)
    await processor.stop(timeout=1.0)

    # No part was taken after the stop, the queued ones were dropped, and no report was acknowledged.
    assert processor._max_waiting_reports <= written < 40
    assert len(producer.records) == written
    assert queued_messages_dropped_at_stop._value.get() == dropped + 40 - written
    assert pipeline.commits == []
    assert pipeline.pending() == written


# --- the result writer ---


@pytest.fixture
def fast_retries(monkeypatch) -> None:
    monkeypatch.setattr(partmode, 'RETRY_FIRST_WAIT_SECONDS', 0.01)
    monkeypatch.setattr(partmode, 'RETRY_MAX_WAIT_SECONDS', 0.02)
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.02)


@pytest.fixture
def log() -> FakeLog:
    return FakeLog()


@pytest.fixture
def client(log) -> FakeProducerClient:
    return FakeProducerClient(log)


@pytest.fixture
async def producer(client):
    split_producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await split_producer.start()
    yield split_producer
    await split_producer.close(1.0)


def ok_result(index: int = 3) -> PartResult:
    return build_part_result(
        index=index,
        worker=WORKER,
        tracker_counts=(2, 1, 0),
        state=MessageDLQState(capture=True),
        error=None,
        data=Summary(items_done=4),
        data_error=None,
        started_at=1_793_000_000.0,
        finished_at=1_793_000_001.5,
    )


def part_with_split(index: int = 3) -> SourceMessage:
    message = part_message(offset=0, index=index)
    message.split = parse_part_headers(message.headers)
    return message


def result_writer(producer: Any, *, stopped: bool = False) -> tuple[ResultWriter, asyncio.Event]:
    """A writer over ``producer`` with its stop event, set when ``stopped``."""
    event = asyncio.Event()
    if stopped:
        event.set()
    return ResultWriter(producer, results_topic=RESULTS_TOPIC, stopped=event, partition=SHOWN_PARTITION), event


async def test_the_writer_publishes_the_result_record_to_the_partition_of_the_source(producer, log):
    writer, _ = result_writer(producer)

    await writer.write(part_with_split(), ok_result())

    [stored] = log.records(RESULTS_TOPIC, SOURCE_PARTITION)
    entry = parse_log_record(stored.headers, stored.value)
    assert entry is not None
    assert (entry.kind, entry.ref) == ('result', job_ref())
    assert stored.key == f'{SOURCE_PARTITION}:{SOURCE_OFFSET}'.encode()
    assert entry.body['part'] == 3
    assert (entry.body['outcome'], entry.body['tasks_succeeded'], entry.body['tasks_failed']) == ('ok', 2, 1)
    assert entry.body['data'] == {'items_done': 4, 'note': ''}
    assert entry.body['duration_seconds'] == 1.5


async def test_the_writer_tries_again_after_a_failure_a_retry_can_fix(producer, client, log, fast_retries):
    writer, _ = result_writer(producer)
    client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT), times=3)
    failures = split_result_write_failures._value.get()

    with capture_logs() as logs:
        await asyncio.wait_for(writer.write(part_with_split(), ok_result()), 5)

    assert len(log.records(RESULTS_TOPIC, SOURCE_PARTITION)) == 1
    assert split_result_write_failures._value.get() == failures + 3
    # One line for the three failures: the others are held back.
    [line] = [line for line in logs if line['event'] == 'split_result_write_failed']
    assert (line['log_level'], line['final'], line['job'], line['part']) == (
        'warning',
        False,
        f'{SOURCE_PARTITION}:{SOURCE_OFFSET}#1',
        3,
    )
    assert (line['partition'], line['attempt'], line['suppressed']) == (SHOWN_PARTITION, 1, 0)


async def test_the_writer_raises_a_final_error_with_the_number_of_the_attempt(producer, client, log, fast_retries):
    writer, _ = result_writer(producer)
    client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))
    client.fail_next_deliveries(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))

    with pytest.raises(SplitProduceError) as raised:
        await asyncio.wait_for(writer.write(part_with_split(), ok_result()), 5)

    assert raised.value.final
    assert isinstance(raised.value, partmode.PartReportError)
    assert raised.value.attempt == 2
    assert log.records(RESULTS_TOPIC, SOURCE_PARTITION) == []


async def test_a_write_that_starts_while_the_partition_stops_still_makes_its_attempt(producer, log):
    writer, _ = result_writer(producer, stopped=True)

    await asyncio.wait_for(writer.write(part_with_split(), ok_result()), 2)

    assert len(log.records(RESULTS_TOPIC, SOURCE_PARTITION)) == 1


async def test_a_failed_attempt_is_not_tried_again_once_the_partition_stops(producer, client, log):
    writer, _ = result_writer(producer, stopped=True)
    client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))

    # No backoff wait comes first: the write ends well inside the first wait of one second.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(writer.write(part_with_split(), ok_result()), 0.5)

    assert client.produced == 1
    assert log.records(RESULTS_TOPIC, SOURCE_PARTITION) == []


async def test_a_stop_ends_the_wait_between_two_attempts_at_once(producer, client):
    writer, stopped = result_writer(producer)
    client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))

    write = asyncio.create_task(writer.write(part_with_split(), ok_result()))
    await wait_for(lambda: client.produced == 1)
    await asyncio.sleep(0.05)
    assert not write.done()
    stopped.set()

    # The wait of one second is cut by the event, not by a poll.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(write, 0.2)
    assert client.produced == 1


async def test_a_stop_ends_a_write_that_waits_for_its_delivery_report(producer, client, fast_retries):
    writer, stopped = result_writer(producer)
    client.hold_deliveries = True

    write = asyncio.create_task(writer.write(part_with_split(), ok_result()))
    await wait_for(lambda: client.produced == 1)
    assert not write.done()
    stopped.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(write, 1)
    # The record was left in the client queue: its late report is read by nobody and reported by nobody.
    client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))
    client.release_deliveries()
    await asyncio.sleep(0.05)


async def test_a_stop_ends_a_write_that_waits_for_room_in_the_client_queue(log, fast_retries):
    # A client queue with no room: the record never gets its place, so the write waits inside the producer.
    client = FakeProducerClient(log, queue_capacity=0)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    writer, stopped = result_writer(producer)

    write = asyncio.create_task(writer.write(part_with_split(), ok_result()))
    await wait_for(lambda: len(client.rejected) >= 2)
    assert not write.done()
    stopped.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(write, 1)
    # The producer is free again: its place in the queue was given back.
    assert not producer._enqueue_lock.locked()
    await producer.close(0.1)
    assert log.records(RESULTS_TOPIC, SOURCE_PARTITION) == []


async def test_a_cancelled_write_takes_its_record_out_of_the_wait(producer, client):
    writer, _ = result_writer(producer)
    client.hold_deliveries = True

    write = asyncio.create_task(writer.write(part_with_split(), ok_result()))
    await wait_for(lambda: client.produced == 1)
    write.cancel()

    with pytest.raises(asyncio.CancelledError):
        await write
    client.release_deliveries()


async def test_the_writer_refuses_a_message_that_is_no_part(producer):
    writer, _ = result_writer(producer)

    with pytest.raises(ValueError, match='parts topic'):
        await writer.write(plain_message(), ok_result())


# --- the result itself ---


def test_a_result_is_failed_when_it_has_an_error_or_a_captured_outcome():
    def build(state: MessageDLQState, error: str | None) -> PartResult:
        return build_part_result(
            index=0,
            worker=WORKER,
            tracker_counts=(1, 2, 3),
            state=state,
            error=error,
            data=None,
            data_error=None,
            started_at=10.0,
            finished_at=12.5,
        )

    counted = MessageDLQState(capture=True, counts={'postgres/main': SinkCounts(delivered=2, skipped=1)})
    captured = MessageDLQState(capture=True, captured=PartDLQ(stage='execute', task_id='t', error='boom'))

    ok = build(counted, None)
    assert (ok.outcome, ok.tasks_succeeded, ok.tasks_failed, ok.tasks_replaced) == ('ok', 1, 2, 3)
    assert (ok.duration_seconds, ok.finished_at) == (2.5, 12.5)
    assert ok.deliveries == {'postgres/main': SinkCounts(delivered=2, skipped=1)}
    # The result keeps its own counts: a later delivery of the message does not change it.
    counted.counts['postgres/main'].delivered += 1
    assert ok.deliveries['postgres/main'].delivered == 2
    assert build(counted, 'parse failed').outcome == 'failed'
    with_dlq = build(captured, None)
    assert (with_dlq.outcome, with_dlq.dlq) == ('failed', captured.captured)
