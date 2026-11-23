"""Messages of HTTP split jobs in the source, and results that are dropped when a job asked for no sink writes."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import CollectResult, ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.sinks.manager import SinkManager
from drakkar.split.http_message import source_message_record
from drakkar.split.owner import BeginOutcome
from drakkar.split.partmode import PartMode, no_report, part_collect
from drakkar.split.records import ORIGIN_HTTP, part_record
from tests.conftest import wait_for
from tests.split_helpers import wait_until
from tests.split_owner_helpers import Item, OwnerWorker, SplittingHandler, World, items
from tests.split_owner_helpers import source_message as owner_source_message
from tests.stream_helpers import stream_task
from tests.test_message_dlq import CountingSink, FanOutHandler, ScriptedPool
from tests.test_sink_delivery_policy import CollectingRecorder, RecordingDLQ
from tests.test_split_part_mode import (
    GROUP,
    PARTS_PARTITION,
    PARTS_TOPIC,
    SOURCE_TOPIC,
    WORKER,
    Data,
    PartPipeline,
    aggregate_result,
    job_ref,
    postgres_result,
)
from tests.test_split_part_mode import (
    STAGE_ERROR as PART_STAGE_ERROR,
)

HTTP_REQUEST_ID = 'req-of-the-http-job'
HTTP_CLIENT = 'billing-client'
SOURCE_PARTITION = 3


def http_headers(*, deliver_sinks: bool = True, request_id: str = HTTP_REQUEST_ID) -> list[tuple[str, bytes | None]]:
    """The headers a split job request writes on its source message."""
    record = source_message_record(request_id=request_id, client=HTTP_CLIENT, deliver_sinks=deliver_sinks, body=b'{}')
    return list(record.headers)


def source_message(offset: int = 0, *, headers: list | None = None, value: bytes = b'{"value": 1}') -> SourceMessage:
    """A message of the source topic; with ``headers`` it can be one an HTTP job request stored."""
    return SourceMessage(
        topic=SOURCE_TOPIC,
        partition=SOURCE_PARTITION,
        offset=offset,
        key=b'k',
        value=value,
        timestamp=1_793_000_000_456,
        headers=headers or [],
    )


def http_message(offset: int = 0, *, deliver_sinks: bool = True, value: bytes = b'{"value": 1}') -> SourceMessage:
    """A message stored by an HTTP split job request."""
    return source_message(offset, headers=http_headers(deliver_sinks=deliver_sinks), value=value)


def http_part(offset: int, *, deliver_sinks: bool) -> SourceMessage:
    """A message of the parts topic that belongs to an HTTP job."""
    record = part_record(
        job_ref(),
        source_topic=SOURCE_TOPIC,
        index=offset,
        request_id=HTTP_REQUEST_ID,
        key=b'k',
        value=b'{"value": 1}',
        source_headers=[],
        partition_count=4,
        origin=ORIGIN_HTTP,
        deliver_sinks=deliver_sinks,
    )
    return SourceMessage(
        topic=PARTS_TOPIC,
        partition=PARTS_PARTITION,
        offset=offset,
        key=record.key,
        value=record.value,
        timestamp=1_793_000_000_456,
        headers=list(record.headers),
    )


class StubOwner:
    """An owner double: every begun message is local, its end completes it, and an unparsed message is held."""

    def __init__(self) -> None:
        self.host: PartitionProcessor | None = None
        self.begun: list[SourceMessage] = []
        self.ended: list[SourceMessage] = []
        self.unparsed: list[SourceMessage] = []

    def note_delivered(self, offsets: Any) -> None:
        list(offsets)

    async def begin(self, message: SourceMessage) -> BeginOutcome:
        self.begun.append(message)
        return BeginOutcome.LOCAL

    async def end_local(self, message: SourceMessage, result: Any) -> bool:
        self.ended.append(message)
        return True

    def fail_unparsed(self, message: SourceMessage) -> None:
        self.unparsed.append(message)
        # As an owner does: the offset stays pending, out of the drain, until the job's end settles it.
        assert self.host is not None
        self.host.hold_job_offset(message.offset)

    def signal_stop(self) -> None:
        pass

    async def stop(self, deadline_seconds: float) -> None:
        pass


class SourcePipeline:
    """The processor of a source partition on a worker that splits, with a stub owner."""

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        *,
        http_jobs: bool,
        on_parse_error: str = 'skip',
        dlq_ok: bool = True,
        on_dlq_failure: str = 'drop',
    ) -> None:
        self.sink = CountingSink('main', 'postgres')
        self.collected: list[CollectResult] = []
        self.dlq = RecordingDLQ(ok=dlq_ok)
        self.stalls: list[int] = []
        self.manager = SinkManager()
        self.manager.register(self.sink)
        self.manager.attach_runtime(
            recorder=CollectingRecorder(),  # ty: ignore[invalid-argument-type]
            dlq_sink=self.dlq,  # ty: ignore[invalid-argument-type]
            dlq_on_send_failure=on_dlq_failure,
        )

        async def on_collect(result, partition_id, message_context=None):
            self.collected.append(result)
            return await self.manager.deliver_all(
                result,
                on_delivery_error=handler.on_delivery_error,
                partition_id=partition_id,
                message_context=message_context,
            )

        async def on_stall(partition_id: int) -> None:
            self.stalls.append(partition_id)

        part_mode = PartMode(
            worker=WORKER, max_part_data=65536, part_data_model=None, report=no_report, source_group=GROUP
        )
        self.owner = StubOwner()
        self.processor = PartitionProcessor(
            partition_id=SOURCE_PARTITION,
            handler=handler,
            executor_pool=ScriptedPool(),
            window_size=10,
            on_collect=part_collect(self.manager, on_collect),
            on_parse_error=on_parse_error,
            dlq_send=self.manager.send_parse_dlq,
            on_dlq_failure=on_dlq_failure,
            on_stall=on_stall,
            part_mode=part_mode,
            http_jobs=http_jobs,
        )
        self.owner.host = self.processor
        self.processor.attach_split_owner(self.owner)  # ty: ignore[invalid-argument-type]

    def all_done(self) -> bool:
        processor = self.processor
        return processor.queue_size == 0 and not processor.offset_tracker.has_pending() and not processor.inflight_count

    async def run(self, *messages: SourceMessage, settled: Any = None) -> None:
        """Run the loop until ``settled`` holds (by default: no offset is pending), then stop."""
        for message in messages:
            self.processor.enqueue(message)
        self.processor.start()
        await wait_for(settled or self.all_done)
        await self.processor.stop()


class CountingHookHandler(FanOutHandler):
    """Arranges one task per message and counts the calls of its ``request_id`` hook."""

    input_model = Data

    def __init__(self, **results: CollectResult) -> None:
        super().__init__(['t-a'], dict(results))
        self.hook_calls = 0

    def request_id(self, msg):
        self.hook_calls += 1
        return f'req-from-the-hook-{msg.offset}'


# --- what the source makes of the message ---


async def test_http_job_message_takes_request_id_from_header_and_skips_hook():
    handler = CountingHookHandler()
    pipeline = SourcePipeline(handler, http_jobs=True)

    await pipeline.run(http_message())

    [begun] = pipeline.owner.begun
    assert begun.request_id == HTTP_REQUEST_ID
    assert handler.hook_calls == 0


async def test_http_job_message_has_http_job_view():
    pipeline = SourcePipeline(CountingHookHandler(), http_jobs=True)

    await pipeline.run(http_message(deliver_sinks=False))

    [begun] = pipeline.owner.begun
    assert begun.http_job is not None
    assert (begun.http_job.request_id, begun.http_job.client, begun.http_job.deliver_sinks) == (
        HTTP_REQUEST_ID,
        HTTP_CLIENT,
        False,
    )


async def test_ordinary_message_has_no_http_job():
    handler = CountingHookHandler()
    pipeline = SourcePipeline(handler, http_jobs=True)

    await pipeline.run(source_message(), source_message(1, headers=[('dk-origin', b'http')]))

    assert [message.http_job for message in pipeline.owner.begun] == [None, None]
    assert [message.request_id for message in pipeline.owner.begun] == ['req-from-the-hook-0', 'req-from-the-hook-1']
    assert handler.hook_calls == 2


async def test_http_headers_ignored_when_feature_off_and_warned():
    handler = CountingHookHandler(**{'t-a': postgres_result()})
    pipeline = SourcePipeline(handler, http_jobs=False)

    with capture_logs() as logs:
        await pipeline.run(
            http_message(0, deliver_sinks=False),
            http_message(1, deliver_sinks=False),
            settled=lambda: len(pipeline.owner.ended) == 2 and pipeline.sink.calls == 2,
        )

    assert [message.http_job for message in pipeline.owner.begun] == [None, None]
    assert [message.request_id for message in pipeline.owner.begun] == ['req-from-the-hook-0', 'req-from-the-hook-1']
    # The message runs as an ordinary message, with its sinks.
    assert pipeline.sink.calls == 2
    [line] = [line for line in logs if line['event'] == 'split_http_message_unhandled']
    assert (line['log_level'], line['category'], line['partition'], line['offset'], line['suppressed']) == (
        'warning',
        'split',
        SOURCE_PARTITION,
        0,
        0,
    )


async def test_flag_off_changes_nothing(monkeypatch):
    def no_read(headers):
        raise AssertionError('the headers of a message are read only with the flag on')

    monkeypatch.setattr(partition_module, 'read_http_job', no_read)
    handler = CountingHookHandler(**{'t-a': postgres_result()})
    pipeline = SourcePipeline(handler, http_jobs=False)

    await pipeline.run(source_message(), settled=lambda: pipeline.sink.calls == 1 and pipeline.owner.ended)

    assert handler.hook_calls == 1
    assert pipeline.owner.begun[0].http_job is None

    # A part of a Kafka job carries no sinks switch and delivers.
    sink = CountingSink('main', 'postgres')
    parts = PartPipeline(FanOutHandler(['t-a'], {'t-a': postgres_result()}), None, sink)
    await parts.run()
    assert sink.calls == 1


# --- a message that cannot be parsed ---


async def test_unparsed_http_job_message_under_skip_goes_to_the_owner_and_not_to_split():
    pipeline = SourcePipeline(CountingHookHandler(), http_jobs=True, on_parse_error='skip')

    await pipeline.run(http_message(value=b'not json'), settled=lambda: pipeline.owner.unparsed)

    [unparsed] = pipeline.owner.unparsed
    assert unparsed.request_id == HTTP_REQUEST_ID
    assert pipeline.owner.begun == []
    assert pipeline.dlq.sent == []


@pytest.mark.parametrize('dlq_ok', [True, False])
async def test_unparsed_http_job_message_under_dlq_writes_its_record_then_goes_to_the_owner(dlq_ok):
    pipeline = SourcePipeline(CountingHookHandler(), http_jobs=True, on_parse_error='dlq', dlq_ok=dlq_ok)

    await pipeline.run(http_message(value=b'not json'), settled=lambda: pipeline.owner.unparsed)

    assert len(pipeline.dlq.sent) == 1
    [record, _] = pipeline.dlq.sent[0]
    assert (record.stage, record.request_id) == ('parse', HTTP_REQUEST_ID)
    assert pipeline.owner.begun == []
    # The owner settles the offset: the processor did not complete it.
    assert pipeline.processor.offset_tracker.has_pending()


async def test_unparsed_http_job_message_whose_record_stalls_gets_no_owner_end():
    pipeline = SourcePipeline(
        CountingHookHandler(), http_jobs=True, on_parse_error='dlq', dlq_ok=False, on_dlq_failure='stall'
    )

    await pipeline.run(http_message(value=b'not json'), settled=lambda: pipeline.stalls)

    assert pipeline.owner.unparsed == []
    assert pipeline.owner.begun == []


async def test_unparsed_ordinary_message_under_dlq_completes_as_before():
    pipeline = SourcePipeline(CountingHookHandler(), http_jobs=True, on_parse_error='dlq')

    await pipeline.run(source_message(value=b'not json'))

    assert len(pipeline.dlq.sent) == 1
    assert pipeline.owner.unparsed == []


class ParsingSplitHandler(SplittingHandler):
    input_model = Item


async def test_an_unparsed_http_job_message_completes_through_a_real_owner():
    world = World()
    handler = ParsingSplitHandler({500: items(2)})
    worker = OwnerWorker(world, handler, http_jobs=True)
    message = owner_source_message(500, value=b'not json')
    message.headers = http_headers()

    await worker.start(message)
    await wait_until(lambda: worker.committed == 501)
    await worker.stop()

    assert handler.split_calls == []
    assert worker.dlq_records() == []


# --- results of a job that asked for no sink writes ---


class MessageResultHandler(FanOutHandler):
    async def on_message_complete(self, group):
        return postgres_result()


class WindowResultHandler(FanOutHandler):
    async def on_window_complete(self, results, source_messages):
        return postgres_result()


class GroupResultHandler(BaseDrakkarHandler):
    async def arrange_stream(self, message):
        yield [stream_task(f'm{message.offset}', message.offset)]

    async def on_group_complete(self, group, results, errors):
        return postgres_result()


class SharedTaskHandler(BaseDrakkarHandler):
    """One task for the whole window, with every message of it as a source."""

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id='shared', args=['x'], source_offsets=[m.offset for m in messages])]

    async def on_task_complete(self, result):
        return postgres_result()


def part_case(handler: BaseDrakkarHandler, *switches: bool) -> tuple[BaseDrakkarHandler, list[SourceMessage]]:
    return handler, [http_part(offset, deliver_sinks=switch) for offset, switch in enumerate(switches)]


RESULT_KINDS = ['on_task_complete', 'on_message_complete', 'on_group_complete', 'on_window_complete', 'two_parts']


def part_handler_and_messages(kind: str, sinks_on: bool) -> tuple[BaseDrakkarHandler, list[SourceMessage]]:
    if kind == 'on_task_complete':
        return part_case(FanOutHandler(['t-a'], {'t-a': postgres_result()}), sinks_on)
    if kind == 'on_message_complete':
        return part_case(MessageResultHandler(['t-a']), sinks_on)
    if kind == 'on_group_complete':
        return part_case(GroupResultHandler(), sinks_on)
    if kind == 'on_window_complete':
        return part_case(WindowResultHandler(['t-a']), sinks_on)
    # A task of two parts, one of them with no sink writes unless the case has sinks on.
    return part_case(SharedTaskHandler(), True, sinks_on)


@pytest.mark.parametrize('sinks_on', [True, False])
@pytest.mark.parametrize('kind', [*RESULT_KINDS, 'local_message'])
async def test_sinks_off_drops_every_result_kind(kind, sinks_on):
    if kind == 'local_message':
        source = SourcePipeline(CountingHookHandler(**{'t-a': postgres_result()}), http_jobs=True)
        with capture_logs() as logs:
            await source.run(
                http_message(deliver_sinks=sinks_on),
                settled=lambda: source.owner.ended and not source.processor.inflight_count,
            )
        collected, sink_calls = source.collected, source.sink.calls
    else:
        handler, messages = part_handler_and_messages(kind, sinks_on)
        sink = CountingSink('main', 'postgres')
        pipeline = PartPipeline(handler, None, sink, window_size=len(messages))
        seen: list[CollectResult] = []
        deliver_all = pipeline.manager.deliver_all

        async def recording_deliver_all(result, **kwargs):
            seen.append(result)
            return await deliver_all(result, **kwargs)

        pipeline.manager.deliver_all = recording_deliver_all  # ty: ignore[invalid-assignment]
        with capture_logs() as logs:
            await pipeline.run(*messages)
            await asyncio.sleep(0.05)
        collected, sink_calls = seen, sink.calls
    dropped = [line for line in logs if line['event'] == 'split_http_delivery_dropped']
    if sinks_on:
        assert collected and sink_calls >= 1
        assert dropped == []
    else:
        assert collected == []
        assert sink_calls == 0
        assert dropped
        assert {(line['log_level'], line['category'], line['request_id']) for line in dropped} == {
            ('debug', 'split', HTTP_REQUEST_ID)
        }


async def test_part_result_of_sinks_off_part_has_no_deliveries():
    class Handler(FanOutHandler):
        async def part_data(self, group):
            return Data(value=7)

    sink = CountingSink('main', 'postgres')
    pipeline = PartPipeline(Handler(['t-a'], {'t-a': postgres_result()}), None, sink)

    await pipeline.run(http_part(0, deliver_sinks=False))

    [result] = pipeline.reports.results
    assert result.outcome == 'ok'
    assert result.deliveries == {}
    assert result.data == Data(value=7)
    assert sink.calls == 0


async def test_stage_error_still_captured_with_sinks_off():
    pipeline = PartPipeline(FanOutHandler(['t-a'], {'t-a': aggregate_result()}))

    await pipeline.run(http_part(0, deliver_sinks=False))

    [result] = pipeline.reports.results
    assert result.outcome == 'failed'
    assert result.dlq is not None
    assert (result.dlq.stage, result.dlq.error) == ('sink', PART_STAGE_ERROR)
