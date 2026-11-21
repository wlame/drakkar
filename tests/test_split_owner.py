"""The owner side of a split: the split itself, local messages, the end of a job and its failures.

Every test drives a real partition processor with its real owner, a real job
tracker and a real producer over the fake Kafka clients, and a real sink
manager. A fake clock and an injectable sleep stand in for every wait.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.budget import BudgetAccount, PendingBudget
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import dlq_dropped_payloads, split_job_duration, split_jobs, split_parts_published
from drakkar.models import CollectResult, KafkaPayload
from drakkar.split import partmode
from drakkar.split import splitting as splitting_module
from drakkar.split.ending import MAX_RESULTS_RETRIES, RESULTS_RETRY_SECONDS
from drakkar.split.models import JobAction, PartResult, SplitJob, SplitResultsUnavailable
from drakkar.split.records import LOG_HEADER_ORDER, PART_HEADER_ORDER, part_partition, result_body
from drakkar.split.tracker import TIMEOUT_CHECK_SECONDS
from drakkar.uiserver.runner import DebugRunner, ProbeInput
from tests.split_helpers import FakeProducerClient
from tests.split_owner_helpers import (
    GROUP,
    PARTITION,
    PARTS_PARTITIONS,
    PARTS_TOPIC,
    RESULTS_TOPIC,
    SOURCE_TOPIC,
    Item,
    OwnerWorker,
    SplittingHandler,
    World,
    items,
    owner_config,
    part_sink_result,
    source_message,
    summary,
    wait_until,
)
from tests.stream_helpers import make_pool, stream_task
from tests.test_message_dlq import ScriptedPool
from tests.test_split_part_mode import PARTS_PARTITION, PartPipeline, part_message


@pytest.fixture(autouse=True)
def fast_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the wait of a coalesced commit: these tests wait for the commit of one offset."""
    monkeypatch.setattr(partition_module, 'COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)


def jobs_counted(outcome: str) -> float:
    return split_jobs.labels(outcome=outcome)._value.get()


# --- a split, its job and the end of the job ---


async def test_a_split_of_five_parts_writes_open_the_parts_and_split_done():
    world = World()
    handler = SplittingHandler({500: items(5)})
    published = split_parts_published._value.get()
    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler).start(source_message(500))
        await wait_until(lambda: any(line['event'] == 'split_job_done' for line in logs))

    opened, done = world.entries()
    assert (opened.ref.group, opened.ref.partition, opened.ref.offset, opened.ref.attempt) == (GROUP, PARTITION, 500, 1)
    request_id = opened.body['request_id']
    assert request_id.startswith('req_')
    assert opened.body == {'worker': 'worker-a', 'instance': 'a' * 16, 'request_id': request_id, 'origin': 'kafka'}
    assert done.ref == opened.ref and done.body == {'parts': 5}
    # Every record of the results log is keyed by the job and goes to the partition of the source message.
    records = world.log.records(RESULTS_TOPIC, PARTITION)
    assert [record.key for record in records] == [b'3:500', b'3:500']
    assert [name for name, _ in records[0].headers] == list(LOG_HEADER_ORDER)
    # Five part records: the value of each item, the key of the original, the partition rule, the headers.
    parts = world.part_records()
    assert [part.index for _, _, part in parts] == [0, 1, 2, 3, 4]
    for partition, record, part in parts:
        assert partition == part_partition(500, part.index, PARTS_PARTITIONS)
        assert record.key == b'request-key'
        assert record.value == Item(n=part.index).model_dump_json().encode()
        assert [name for name, _ in record.headers] == ['traceparent', *PART_HEADER_ORDER]
        assert (part.source_topic, part.attempt_id, part.origin) == (SOURCE_TOPIC, opened.ref.attempt_id, 'kafka')
        assert dict(record.headers)['dk-request-id'] == request_id.encode()
    assert split_parts_published._value.get() == published + 5
    assert handler.split_calls == [500]
    events = [line['event'] for line in logs if line.get('category') == 'split']
    assert events == [
        'split_enabled',
        'split_restore_started',
        'split_restore_finished',
        'split_job_opened',
        'split_job_done',
    ]
    [opened_line] = [line for line in logs if line['event'] == 'split_job_opened']
    assert (opened_line['job'], opened_line['attempt'], opened_line['request_id']) == ('3:500#1', 1, request_id)
    await worker.stop()


async def test_the_loop_takes_the_next_message_while_a_job_is_open_and_the_watermark_stays():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))

    # The local message 501 ran to its end while the job of 500 is open.
    await wait_until(lambda: len(handler.jobs) == 1 and worker.pending == 1)
    assert handler.jobs[0].local and handler.jobs[0].source_message.offset == 501
    await asyncio.sleep(0.05)
    assert worker.commits == []

    world.report_all()
    await wait_until(lambda: worker.committed == 502)
    await worker.stop()


async def test_five_results_give_one_aggregate_call_then_the_sinks_then_closed_then_the_offset():
    world = World()
    order: list[str] = []

    def answer(job: SplitJob) -> Any:
        order.append(f'aggregate:{world.kinds()[-1]}')
        return summary(job.received_parts)

    handler = SplittingHandler({500: items(5)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    aggregated = jobs_counted('aggregated')

    world.report_all(world.parts()[:4], tasks_succeeded=2, tasks_failed=1)
    await asyncio.sleep(0.05)
    assert handler.jobs == []
    world.clock.advance(7000)
    with capture_logs() as logs:
        world.report(world.parts()[4], tasks_succeeded=2, tasks_replaced=1)
        await wait_until(lambda: worker.committed == 501)

    [job] = handler.jobs
    assert (job.job_id, job.attempt, job.local, job.timed_out) == ('3:500#1', 1, False, '')
    assert (job.total_parts, job.received_parts, job.failed_parts, job.dlq_parts) == (5, 5, 0, 0)
    assert (job.tasks_succeeded, job.tasks_failed, job.tasks_replaced) == (10, 4, 1)
    assert job.missing_parts() == [] and job.duration_seconds == 7.0
    assert job.source_message.offset == 500
    # The aggregate payload reached the aggregate sink, and only then closed was written.
    [[payload]] = worker.aggregate_sink.delivered
    assert payload.data == Item(n=5)
    [closed] = world.closed()
    assert closed == {'outcome': 'aggregated', 'received': 5, 'parts': 5, 'duration_seconds': 7.0}
    assert order == ['aggregate:result']
    assert jobs_counted('aggregated') == aggregated + 1
    [line] = [line for line in logs if line['event'] == 'split_job_closed']
    assert (line['job'], line['outcome'], line['parts'], line['received'], line['failed_parts']) == (
        '3:500#1',
        'aggregated',
        5,
        5,
        0,
    )
    assert worker.dlq_records() == []
    await worker.stop()


async def test_a_local_message_writes_to_no_topic_and_ends_with_one_aggregate_call():
    world = World()
    handler = SplittingHandler(answer=summary(9))
    local = jobs_counted('local')
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.jobs
    assert (job.local, job.job_id, job.timed_out) == (True, '3:500#1', '')
    assert (job.total_parts, job.received_parts, job.failed_parts, job.dlq_parts) == (1, 1, 0, 0)
    assert (job.tasks_succeeded, job.tasks_failed, job.tasks_replaced) == (1, 0, 0)
    assert job.missing_parts() == []
    [result] = [result async for result in job.results()]
    assert (result.index, result.outcome, result.worker) == (0, 'ok', 'worker-a')
    # No record in any topic.
    assert worker.client.produced == 0
    assert world.log.records(RESULTS_TOPIC, PARTITION) == [] and world.part_records() == []
    [[payload]] = worker.aggregate_sink.delivered
    assert payload.data == Item(n=9)
    assert jobs_counted('local') == local + 1
    assert handler.split_calls == [500]
    await worker.stop()


async def test_a_local_message_whose_arrange_raises_ends_in_a_dlq_record():
    class Handler(SplittingHandler):
        async def arrange(self, messages, pending):
            raise ValueError('cannot arrange')

    world = World()
    handler = Handler()
    dlq_jobs = jobs_counted('dlq')
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    # The default aggregate sends a message with a failed part to the DLQ; the loop was not restarted.
    [job] = handler.jobs
    assert (job.local, job.failed_parts) == (True, 1)
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'part 0 failed: cannot arrange')
    assert (record.key, record.value, record.source_offset) == (b'request-key', b'{"n": 5}', 500)
    assert jobs_counted('dlq') == dlq_jobs + 1
    assert not worker.processor.is_dead and worker.client.produced == 0
    await worker.stop()


async def test_the_default_aggregate_writes_the_dlq_record_of_the_first_failed_part():
    world = World()
    handler = SplittingHandler({500: items(4)})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    parts = world.parts()
    dlq_jobs = jobs_counted('dlq')

    failure = {'stage': 'execute', 'task_id': 't-9', 'error': 'exit code 2: boom', 'attempts': 3}
    world.report(parts[3], outcome='failed', error='late parse failure')
    world.report(parts[1], outcome='failed', dlq=failure)
    world.report_all([parts[0], parts[2]])
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.jobs
    assert (job.failed_parts, job.dlq_parts, job.received_parts) == (2, 1, 4)
    # The record is the original message, with the facts of the failed part with the lowest number.
    [record] = worker.dlq_records()
    assert (record.stage, record.task_id, record.error, record.attempts) == ('execute', 't-9', 'exit code 2: boom', 3)
    assert (record.key, record.value) == (b'request-key', b'{"n": 5}')
    assert (record.source_topic, record.source_partition, record.source_offset) == (SOURCE_TOPIC, PARTITION, 500)
    assert record.source_headers == source_message().headers
    assert world.closed()[0]['outcome'] == 'dlq'
    assert world.kinds()[-1] == 'closed'
    assert jobs_counted('dlq') == dlq_jobs + 1
    assert worker.aggregate_sink.calls == 0
    await worker.stop()


async def test_a_failed_part_without_a_dlq_outcome_gives_stage_aggregate_with_its_error():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    first, second = world.parts()

    world.report(first, outcome='failed', error='value is not valid JSON')
    world.report(second, outcome='failed', dlq={'stage': 'sink', 'sink': 'postgres/main', 'error': 'down'})
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.sink, record.task_id) == ('aggregate', None, None)
    assert record.error == 'part 0 failed: value is not valid JSON'
    await worker.stop()


async def test_job_action_dlq_with_no_failed_part_gives_stage_aggregate():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=JobAction.DLQ)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.error, record.attempts) == (
        'aggregate',
        'aggregate chose the DLQ: 0 of 2 parts failed',
        None,
    )
    assert world.closed()[0]['outcome'] == 'dlq'
    await worker.stop()


async def test_job_action_done_and_none_deliver_nothing():
    world = World()
    handler = SplittingHandler(
        {500: items(1), 501: items(1)}, answer=lambda job: [JobAction.DONE, None][len(handler.jobs) - 1]
    )
    worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))
    await wait_until(lambda: world.kinds().count('split-done') == 2)

    world.report_all()
    await wait_until(lambda: worker.committed == 502)

    assert [body['outcome'] for body in world.closed()] == ['aggregated', 'aggregated']
    assert worker.aggregate_sink.calls == 0 and worker.dlq_records() == []
    await worker.stop()


async def test_a_raise_of_aggregate_sends_the_original_message_to_the_dlq():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=ValueError('bad sum'))
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'aggregate failed: bad sum')
    assert world.closed()[0]['outcome'] == 'dlq'
    [failed] = [line for line in logs if line['event'] == 'aggregate_failed']
    assert (failed['log_level'], failed['category'], failed['hook'], failed['error']) == (
        'error',
        'split',
        'aggregate',
        'bad sum',
    )
    assert isinstance(failed['exc_info'], ValueError)
    await worker.stop()


async def test_an_answer_of_aggregate_that_is_no_answer_is_handled_as_a_raise():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=lambda job: {'kafka': []})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'aggregate failed: aggregate returned dict')
    await worker.stop()


async def test_a_payload_of_aggregate_for_a_part_sink_is_handled_as_a_raise_and_nothing_is_delivered():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=part_sink_result())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert record.stage == 'aggregate'
    assert record.error == 'aggregate failed: sink postgres/main belongs to the part stage of split'
    assert worker.part_sink.calls == 0 and worker.aggregate_sink.calls == 0
    await worker.stop()


# --- a split that fails ---


async def test_a_raise_of_split_on_its_third_pull_fails_the_job():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), Item(n=1), RuntimeError('boom')]})
    failed = jobs_counted('split_failed')
    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))
        await wait_until(lambda: worker.committed == 502)

    # The parts already published stay; the log says the job is closed, so no restart splits it again.
    assert world.kinds() == ['open', 'closed']
    [closed] = world.closed()
    assert (closed['outcome'], closed['received'], closed['parts']) == ('split_failed', 0, None)
    assert len(world.parts()) == 2
    [record] = worker.dlq_records()
    assert (record.stage, record.error, record.attempts) == ('split', 'split failed: boom', None)
    assert (record.key, record.value, record.source_offset) == (b'request-key', b'{"n": 5}', 500)
    assert jobs_counted('split_failed') == failed + 1
    assert handler.closed_splits == 2 and handler.pulls == 3
    [line] = [line for line in logs if line['event'] == 'split_failed']
    assert (line['log_level'], line['category'], line['job'], line['error']) == ('error', 'split', '3:500#1', 'boom')
    # The message after it ran as a local message, and no aggregate ran for the failed job.
    assert [job.source_message.offset for job in handler.jobs] == [501]
    await worker.stop()


async def test_a_raise_of_split_before_its_first_part_fails_the_job_with_no_open_record():
    world = World()
    handler = SplittingHandler({500: [RuntimeError('no plan')]})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    assert world.kinds() == ['closed']
    assert world.closed()[0]['outcome'] == 'split_failed'
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('split', 'split failed: no plan')
    await worker.stop()


async def test_a_yielded_dict_fails_the_job():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), {'n': 1}, Item(n=2)]})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert record.stage == 'split'
    assert record.error == 'split failed: a part must be a pydantic model or bytes, got dict'
    assert world.kinds() == ['open', 'closed'] and len(world.parts()) == 1
    assert handler.closed_splits == 1
    await worker.stop()


class SizeLimitedClient(FakeProducerClient):
    """A producer client that refuses a part above its size limit, as the real client does."""

    def produce(self, topic: str, value: bytes | None = None, **kwargs: Any) -> None:
        if topic == PARTS_TOPIC and value is not None and len(value) > 64:
            raise KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
        super().produce(topic, value=value, **kwargs)


async def test_a_part_larger_than_the_client_accepts_fails_the_job_and_bytes_are_written_as_they_are():
    world = World()
    handler = SplittingHandler({500: [b'raw \xff bytes', b'x' * 65]})
    worker = OwnerWorker(world, handler, client=SizeLimitedClient(world.log))
    await worker.start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [(_, stored, _)] = world.part_records()
    assert stored.value == b'raw \xff bytes'
    [record] = worker.dlq_records()
    assert record.stage == 'split' and record.error.startswith('split failed: ')
    assert 'MSG_SIZE_TOO_LARGE' in record.error
    assert world.closed()[0]['outcome'] == 'split_failed'
    await worker.stop()


async def test_a_part_the_broker_refuses_for_good_fails_the_job():
    world = World()
    handler = SplittingHandler({500: items(3)})
    worker = OwnerWorker(world, handler)
    # The first record is ``open``; the delivery report of the second, a part, carries a final error.
    worker.client.hold_deliveries = True
    await worker.start(source_message(500))
    await wait_until(lambda: worker.client.produced == 1)
    worker.client.release_deliveries()
    await wait_until(lambda: world.kinds() == ['open'])
    worker.client.fail_next_deliveries(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert record.stage == 'split' and 'MSG_SIZE_TOO_LARGE' in record.error
    assert world.kinds() == ['open', 'closed']
    await worker.stop()


async def test_a_part_whose_delivery_fails_for_a_while_is_sent_again_and_counted_once():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate, Item(n=1)]})
    worker = OwnerWorker(world, handler)
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    published = split_parts_published._value.get()

    # The record of the second part fails twice with an error a retry can fix.
    worker.client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT), times=2)
    with capture_logs() as logs:
        gate.set()
        await worker.sleep.fire(1.0)
        await worker.sleep.fire(2.0)
        await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    assert [part.index for part in world.parts()] == [0, 1]
    assert world.entries()[-1].body == {'parts': 2}
    assert split_parts_published._value.get() == published + 2
    [line] = [line for line in logs if line['event'] == 'split_publish_failed']
    assert (line['log_level'], line['category'], line['job']) == ('warning', 'split', '3:500#1')
    assert worker.dlq_records() == []
    await worker.stop()


# --- timeouts ---


async def timed_out_job(world: World, worker: OwnerWorker, *, advance_ms: int = 61_000) -> None:
    """Move the clock past the timeout and run the timeout check of the tracker."""
    await worker.caught_up()
    world.clock.advance(advance_ms)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)


async def test_the_default_on_job_timeout_sends_the_original_message_to_the_dlq():
    world = World()
    handler = SplittingHandler({500: items(3)})
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    parts = world.parts()
    world.report(parts[1])
    timed_out = jobs_counted('timeout')

    with capture_logs() as logs:
        await timed_out_job(world, worker)
        await wait_until(lambda: worker.committed == 501)

    [job] = handler.timeouts
    assert (job.timed_out, job.total_parts, job.received_parts) == ('job', 3, 1)
    assert job.missing_parts() == [0, 2]
    assert handler.jobs == []
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'job timed out after 1m1s: 1 of 3 parts')
    [closed] = world.closed()
    assert (closed['outcome'], closed['received'], closed['parts']) == ('timeout', 1, 3)
    assert jobs_counted('timeout') == timed_out + 1
    [line] = [line for line in logs if line['event'] == 'split_job_timed_out']
    assert (line['log_level'], line['job'], line['kind'], line['received'], line['parts']) == (
        'warning',
        '3:500#1',
        'job',
        1,
        3,
    )
    # A late result changes nothing: the job is closed.
    world.report(parts[0])
    await asyncio.sleep(0.05)
    assert len(handler.timeouts) == 1 and handler.jobs == []
    await worker.stop()


async def test_an_on_job_timeout_that_accepts_a_partial_job_delivers_its_aggregate():
    class Handler(SplittingHandler):
        async def on_job_timeout(self, job: SplitJob) -> Any:
            self.timeouts.append(job)
            return await self.aggregate(job)

    world = World()
    handler = Handler({500: items(2)}, answer=lambda job: summary(len(job.missing_parts())))
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='2m', idle_timeout='1m'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    await timed_out_job(world, worker)
    await wait_until(lambda: worker.committed == 501)

    # The idle timeout fired first; the job ends as a timeout whatever the hook answered.
    [job] = handler.timeouts
    assert (job.timed_out, job.missing_parts()) == ('idle', [0, 1])
    [[payload]] = worker.aggregate_sink.delivered
    assert payload.data == Item(n=2)
    assert world.closed()[0]['outcome'] == 'timeout'
    assert worker.dlq_records() == []
    await worker.stop()


async def test_a_timeout_during_a_split_cancels_the_split_and_ends_the_job():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    await timed_out_job(world, worker)
    await wait_until(lambda: worker.committed == 501)

    assert world.kinds() == ['open', 'split-cancelled', 'closed']
    assert handler.closed_splits == 1
    [job] = handler.timeouts
    assert (job.timed_out, job.total_parts, job.received_parts, job.missing_parts()) == ('job', None, 0, [])
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'job timed out after 1m1s: 0 of ? parts')
    assert world.closed()[0] == {'outcome': 'timeout', 'received': 0, 'parts': None, 'duration_seconds': 61.0}
    await worker.stop()


# --- reading the results back ---


class TypedHandler(SplittingHandler):
    part_data_model = Item


async def test_job_results_yields_the_results_of_the_current_attempt():
    world = World()
    seen: list[tuple[int, Any]] = []

    async def answer(job: SplitJob) -> None:
        seen.extend([(result.index, result.data) async for result in job.results()])

    handler = TypedHandler({500: items(3)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    first, second, third = world.parts()

    world.report(second, data={'n': 22})
    # A result of another attempt, and a second result of the same part, are not results of the job.
    world.put(world.ref(500, 'e' * 16, attempt=7), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    world.report(second, data={'n': 99})
    world.report(third)
    world.report(first, data={'n': 11})
    await wait_until(lambda: worker.committed == 501)

    assert seen == [(1, Item(n=22)), (2, None), (0, Item(n=11))]
    await worker.stop()


async def test_results_that_cannot_be_read_twice_call_aggregate_three_times():
    world = World()
    failures = [SplitResultsUnavailable('broker away'), SplitResultsUnavailable('broker away')]
    handler = SplittingHandler({500: items(1)}, answer=lambda job: failures.pop() if failures else summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    with capture_logs() as logs:
        world.report_all()
        for calls in (1, 2):
            await wait_until(lambda calls=calls: len(handler.jobs) == calls)
            assert world.closed() == [] and worker.pending == 1
            # The position commit of the tracker sleeps for the same time.
            await worker.sleep.fire(RESULTS_RETRY_SECONDS, count=2)
        await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 3
    # Each call got a job built anew.
    assert len({id(job) for job in handler.jobs}) == 3
    assert worker.aggregate_sink.calls == 1 and worker.dlq_records() == []
    assert world.closed()[0]['outcome'] == 'aggregated'
    unavailable = [line for line in logs if line['event'] == 'split_results_unavailable']
    assert [(line['log_level'], line['category'], line['failures']) for line in unavailable] == [
        ('warning', 'split', 1)
    ]
    await worker.stop()


async def test_results_that_cannot_be_read_twenty_one_times_end_as_a_raise_of_the_hook():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=SplitResultsUnavailable('broker away'))
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    for calls in range(1, MAX_RESULTS_RETRIES + 1):
        await wait_until(lambda calls=calls: len(handler.jobs) == calls)
        await worker.sleep.fire(RESULTS_RETRY_SECONDS, count=2)
    await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == MAX_RESULTS_RETRIES + 1
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'aggregate failed: broker away')
    assert world.closed()[0]['outcome'] == 'dlq'
    await worker.stop()


async def test_results_that_expired_are_a_raise_of_the_hook_at_once():
    world = World()

    async def answer(job: SplitJob) -> None:
        [_ async for _ in job.results()]

    handler = SplittingHandler({500: items(1)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    world.log.raise_low(RESULTS_TOPIC, PARTITION, 1)
    await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 1
    [record] = worker.dlq_records()
    assert record.stage == 'aggregate' and record.error.startswith('aggregate failed: the results log of partition 3')
    await worker.stop()


# --- the aggregate sinks and the DLQ ---


async def test_a_failing_aggregate_sink_under_dlq_puts_the_original_message_on_the_dlq():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler, aggregate_sink_fails=True).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.sink) == ('sink', 'kafka/summaries')
    assert (record.key, record.value) == (b'request-key', b'{"n": 5}')
    # The job ends as dlq: the record was written by the failing delivery.
    assert world.closed()[0]['outcome'] == 'dlq'
    await worker.stop()


async def test_a_failing_aggregate_sink_under_skip_ends_the_job_as_aggregated():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler, aggregate_sink_fails=True, aggregate_on_failure='skip')
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    assert worker.dlq_records() == [] and world.closed()[0]['outcome'] == 'aggregated'
    await worker.stop()


def dropped_payloads() -> float:
    return dlq_dropped_payloads.labels(partition=str(PARTITION))._value.get()


async def test_a_job_dlq_record_that_cannot_be_written_under_drop_is_counted_once_and_the_offset_completes():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=JobAction.DLQ)
    worker = await OwnerWorker(world, handler, dlq_ok=False).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    dropped = dropped_payloads()

    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: worker.committed == 501)

    assert dropped_payloads() == dropped + 1
    assert [line['log_level'] for line in logs if line['event'] == 'dlq_failure_payloads_dropped'] == ['critical']
    assert world.closed()[0]['outcome'] == 'dlq'
    await worker.stop()


async def test_a_job_dlq_record_that_cannot_be_written_under_stall_stalls_the_offset_with_no_closed():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=JobAction.DLQ)
    worker = OwnerWorker(world, handler, dlq_ok=False, config=owner_config(on_send_failure='stall'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    dropped = dropped_payloads()

    world.report_all()
    await wait_until(lambda: worker.stalls == [PARTITION])

    # Nothing is closed, so a restart ends the job again; the offset holds the watermark.
    assert world.closed() == [] and dropped_payloads() == dropped
    assert worker.processor._stalled_offsets == {500} and worker.commits == []
    # A stalled offset holds no drain.
    await asyncio.wait_for(worker.processor.drain(), timeout=1)
    await worker.stop()


async def test_a_split_failure_whose_dlq_record_cannot_be_written_follows_on_send_failure():
    world = World()
    handler = SplittingHandler({500: [RuntimeError('boom')], 501: [RuntimeError('boom')]})
    dropped = dropped_payloads()
    drop = await OwnerWorker(world, handler, dlq_ok=False).start(source_message(500))
    await wait_until(lambda: drop.committed == 501)
    assert dropped_payloads() == dropped + 1 and world.closed()[0]['outcome'] == 'split_failed'
    await drop.stop()

    stall = OwnerWorker(world, handler, dlq_ok=False, instance='b' * 16, config=owner_config(on_send_failure='stall'))
    await stall.start(source_message(501))
    await wait_until(lambda: stall.stalls == [PARTITION])
    assert len(world.closed()) == 1 and stall.processor._stalled_offsets == {501}
    assert dropped_payloads() == dropped + 1
    await stall.stop()


async def test_an_aggregate_delivery_that_cannot_be_confirmed_under_stall_writes_no_closed():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    config = owner_config(on_send_failure='stall')
    worker = OwnerWorker(world, handler, aggregate_sink_fails=True, dlq_ok=False, config=config)
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: worker.stalls == [PARTITION])

    assert world.closed() == [] and worker.commits == []
    assert [line['category'] for line in logs if line['event'] == 'aggregate_delivery_failed'] == ['sink']
    await worker.stop()


async def test_a_closed_record_that_fails_for_a_while_is_written_again_before_the_offset_completes():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    worker.client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))
    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: worker.sleep.waiting(1.0))
        assert worker.commits == [] and worker.aggregate_sink.calls == 1
        await worker.sleep.fire(1.0)
        await wait_until(lambda: worker.committed == 501)

    assert world.closed()[0]['outcome'] == 'aggregated'
    [line] = [line for line in logs if line['event'] == 'split_closed_write_failed']
    assert (line['log_level'], line['final']) == ('warning', False)
    await worker.stop()


async def test_a_closed_record_that_can_never_be_written_stalls_the_offset():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    worker.client.fail_next_deliveries(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: worker.stalls == [PARTITION])

    assert world.closed() == [] and worker.commits == []
    [line] = [line for line in logs if line['event'] == 'split_closed_write_failed']
    assert (line['log_level'], line['final']) == ('error', True)
    await worker.stop()


# --- what never reaches the split ---


class ParsingHandler(SplittingHandler):
    input_model = Item


async def test_a_message_that_cannot_be_parsed_under_dlq_never_reaches_split():
    world = World()
    handler = ParsingHandler({500: items(2)})
    worker = OwnerWorker(world, handler, config=owner_config(on_parse_error='dlq'))
    await worker.start(source_message(500, value=b'not json'))
    await wait_until(lambda: worker.committed == 501)

    assert handler.split_calls == [] and handler.jobs == []
    [record] = worker.dlq_records()
    assert (record.stage, record.value) == ('parse', b'not json')
    assert worker.client.produced == 0
    await worker.stop()


async def test_a_message_that_cannot_be_parsed_under_skip_reaches_split_with_no_payload():
    world = World()
    payloads: list[Any] = []

    class Handler(ParsingHandler):
        async def split(self, message):
            payloads.append(message.payload)
            return
            yield

    handler = Handler(answer=None)
    worker = await OwnerWorker(world, handler).start(source_message(500, value=b'not json'))
    await wait_until(lambda: worker.committed == 501)

    assert payloads == [None] and len(handler.jobs) == 1
    await worker.stop()


class ProbedHandler(SplittingHandler):
    """Its tasks are precomputed, so a probe runs no subprocess."""

    async def arrange(self, messages, pending):
        return [stream_task(f't-{message.offset}', message.offset) for message in messages]


async def test_a_message_probe_run_calls_neither_split_nor_aggregate_and_writes_to_no_topic():
    world = World()
    handler = ProbedHandler({42: items(3)})
    runner = DebugRunner(handler=handler, executor_pool=make_pool(), app_config=owner_config())

    report = await runner.run(ProbeInput(value='{"n": 1}', key='k', partition=PARTITION, offset=42, topic=SOURCE_TOPIC))

    assert report.arrange.duration_seconds is not None
    assert handler.split_calls == [] and handler.jobs == [] and handler.timeouts == []
    assert world.log.records(RESULTS_TOPIC, PARTITION) == [] and world.part_records() == []


# --- the pending budget, the limits of a split and the stream handler ---


async def test_a_split_in_progress_holds_no_reservation_of_the_pending_budget():
    world = World()
    before_first_part, after_first_part = asyncio.Event(), asyncio.Event()
    arranged: list[list[int]] = []

    class Handler(SplittingHandler):
        async def arrange(self, messages, pending):
            arranged.append([message.offset for message in messages])
            return await super().arrange(messages, pending)

    handler = Handler({500: [before_first_part, Item(n=0), after_first_part, Item(n=1)]}, answer=None)
    budget = PendingBudget(max_tasks=100, max_bytes=1_000_000)
    # Measured, so a window reserves an estimate and not the one slot of an unmeasured window.
    budget.shape.note_window(1, 2, 10)
    worker = OwnerWorker(world, handler, budget=budget)
    worker.processor._windows_seen = partition_module.WARM_UP_WINDOWS
    await worker.start(source_message(500), source_message(501), source_message(502))

    # Until the split yields its first part the window keeps its reservation, as it does during arrange.
    await wait_until(lambda: handler.pulls == 1)
    assert budget.free_tasks < 100
    before_first_part.set()
    # From the first part on the message is a job: it has no task here, and the reservation is given back.
    await wait_until(lambda: handler.pulls == 3)
    assert (budget.free_tasks, budget.free_bytes) == (100, 1_000_000)
    assert arranged == []

    after_first_part.set()
    await wait_until(lambda: len(handler.jobs) == 2)
    # Only the local messages of the window reached arrange, on a reservation of their own.
    assert arranged == [[501, 502]]
    world.report_all()
    await wait_until(lambda: worker.committed == 503)
    assert (budget.free_tasks, budget.free_bytes) == (100, 1_000_000)
    await worker.stop()


async def test_the_local_messages_behind_a_job_are_admitted_in_windows_that_fit_the_pending_budget():
    world = World()
    arranged: list[list[int]] = []
    task_gate, split_gate = asyncio.Event(), asyncio.Event()

    class Handler(SplittingHandler):
        async def arrange(self, messages, pending):
            arranged.append([message.offset for message in messages])
            return await super().arrange(messages, pending)

    handler = Handler({500: [Item(n=0), split_gate]}, answer=None)
    budget = PendingBudget(max_tasks=4, max_bytes=1_000_000)
    budget.shape.note_window(1, 1, 10)
    worker = OwnerWorker(world, handler, budget=budget, pool=ScriptedPool(gates={'t-a': task_gate}))
    worker.processor._windows_seen = partition_module.WARM_UP_WINDOWS
    local = [501, 502, 503, 504, 505, 506]
    await worker.start(source_message(500), *(source_message(offset) for offset in local))
    await wait_until(lambda: handler.pulls == 2)
    # Another loop of the worker holds half of the room while the split runs: two tasks fit.
    other = BudgetAccount(budget)
    other.charge(2, 0)

    split_gate.set()
    await wait_until(lambda: arranged != [])
    for _ in range(50):
        await asyncio.sleep(0)

    # Two of the six local messages fit. All six at once would pass the limit of 4 tasks by a whole window.
    assert arranged == [[501, 502]]
    assert budget.free_tasks == 0

    task_gate.set()
    await wait_until(lambda: [offset for window in arranged for offset in window] == local)
    assert all(len(window) <= 2 for window in arranged)
    world.report_all()
    await wait_until(lambda: worker.committed == 507)
    other.close()
    await worker.stop()


async def test_the_ends_of_local_messages_are_capped_like_waiting_part_reports():
    world = World()
    gate = asyncio.Event()

    async def slow_end(job):
        await gate.wait()

    handler = SplittingHandler(answer=slow_end)
    worker = OwnerWorker(world, handler, window_size=1)
    cap = worker.processor._max_waiting_reports
    offsets = range(500, 500 + cap + 10)
    await worker.start(*(source_message(offset) for offset in offsets))

    await wait_until(lambda: len(handler.jobs) >= cap and worker.processor.inflight_count == 0)
    for _ in range(50):
        await asyncio.sleep(0)

    # The ends wait for their aggregate, so the loop takes no further message. The cap is checked when a
    # window is taken, so the one message that was taken while the cap was not reached yet is above it.
    waiting = len(handler.jobs)
    assert cap <= waiting <= cap + 1 and worker.processor._waiting_reports == waiting
    assert worker.processor.queue_size == len(offsets) - waiting

    gate.set()
    await wait_until(lambda: worker.committed == offsets[-1] + 1)
    assert len(handler.jobs) == len(offsets) and worker.processor._waiting_reports == 0
    await worker.stop()


async def test_a_stop_before_the_local_messages_of_a_window_get_room_again_leaves_their_offsets_pending():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=None)
    budget = PendingBudget(max_tasks=4, max_bytes=1_000_000)
    budget.shape.note_window(1, 1, 10)
    worker = OwnerWorker(world, handler, budget=budget)
    worker.processor._windows_seen = partition_module.WARM_UP_WINDOWS
    await worker.start(source_message(500), source_message(501))
    await wait_until(lambda: handler.pulls == 2)
    # Another loop of the worker takes all the room while the split runs.
    other = BudgetAccount(budget)
    other.charge(4, 0)

    gate.set()
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await asyncio.sleep(0.05)
    assert handler.jobs == []
    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    assert worker.processor._abandoned_offsets == {501} and worker.pending == 2
    other.close()
    await worker.stop()


async def test_more_parts_than_the_log_can_number_fail_the_job(monkeypatch):
    monkeypatch.setattr(splitting_module, 'MAX_PART_INDEX', 1)
    world = World()
    handler = SplittingHandler({500: items(3)})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('split', 'split failed: more than 2 parts')
    assert len(world.parts()) == 2 and world.closed()[0]['outcome'] == 'split_failed'
    await worker.stop()


async def test_a_split_waits_for_its_part_records_at_the_limit_of_unacknowledged_ones(monkeypatch):
    monkeypatch.setattr(splitting_module, 'MAX_UNACKED_PARTS', 2)
    world = World()
    handler = SplittingHandler({500: items(5)})
    worker = OwnerWorker(world, handler)
    worker.client.hold_deliveries = True
    await worker.start(source_message(500))
    await wait_until(lambda: worker.client.produced == 1)
    worker.client.release_deliveries()
    await wait_until(lambda: world.kinds() == ['open'])

    worker.client.hold_deliveries = True
    await wait_until(lambda: worker.client.produced == 3)
    await asyncio.sleep(0.05)
    # Two part records wait for their acknowledgement: the generator is not pulled further.
    assert worker.client.produced == 3 and handler.pulls == 2
    worker.client.release_deliveries()
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    assert [part.index for part in world.parts()] == [0, 1, 2, 3, 4]
    await worker.stop()


async def test_a_split_that_is_no_async_generator_fails_the_job():
    class Handler(SplittingHandler):
        def split(self, message):  # ty: ignore[invalid-method-override]
            return [Item(n=1)]

    world = World()
    worker = await OwnerWorker(world, Handler()).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert record.error == 'split failed: split must be an async generator, got list'
    await worker.stop()


async def test_a_split_cancelled_record_that_cannot_be_written_is_logged_and_the_stop_goes_on():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    worker.client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))
    with capture_logs() as logs:
        worker.processor.signal_stop()
        await asyncio.wait_for(worker.processor.drain(), timeout=1)
        await worker.processor.stop()

    assert world.kinds() == ['open']
    [line] = [line for line in logs if line['event'] == 'split_cancel_write_failed']
    assert (line['log_level'], line['category'], line['job']) == ('warning', 'split', '3:500#1')
    await worker.stop()


class StreamingHandler(SplittingHandler):
    """Splits, and turns a local message into groups of precomputed tasks."""

    def __init__(self, *args: Any, fail_stream: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fail_stream = fail_stream

    async def arrange_stream(self, message):
        yield [stream_task(f'g0-{message.offset}', message.offset)]
        if self.fail_stream:
            raise ValueError('stream broke')
        yield [stream_task(f'g1-{message.offset}', message.offset)]


async def test_a_stream_handler_splits_one_message_and_streams_a_local_one():
    world = World()
    handler = StreamingHandler({500: items(2)}, answer=None)
    worker = await OwnerWorker(world, handler, pool=make_pool()).start(source_message(500), source_message(501))
    await wait_until(lambda: len(handler.jobs) == 1)

    [local] = handler.jobs
    assert (local.local, local.source_message.offset, local.tasks_succeeded) == (True, 501, 2)
    assert world.kinds() == ['open', 'split-done'] and worker.commits == []
    world.report_all()
    await wait_until(lambda: worker.committed == 502)
    await worker.stop()


async def test_a_local_message_whose_stream_fails_ends_as_a_failed_part_and_the_loop_goes_on():
    world = World()
    handler = StreamingHandler(fail_stream=True)
    worker = await OwnerWorker(world, handler, pool=make_pool()).start(source_message(500), source_message(501))
    await wait_until(lambda: worker.committed == 502)

    assert [(job.failed_parts, job.tasks_succeeded) for job in handler.jobs] == [(1, 1), (1, 1)]
    assert [record.error for record in worker.dlq_records()] == ['part 0 failed: stream broke'] * 2
    assert not worker.processor.is_dead
    await worker.stop()


async def test_a_restarted_stream_loop_goes_on_with_the_message_it_held(monkeypatch):
    handler = StreamHandler()
    pipeline = PartPipeline(handler, make_pool())
    waits = 0
    wait_to_take = pipeline.processor._wait_to_take_stream_part

    async def failing_once() -> bool:
        nonlocal waits
        waits += 1
        if waits == 1:
            raise RuntimeError('the loop broke while it held a part')
        return await wait_to_take()

    monkeypatch.setattr(pipeline.processor, '_wait_to_take_stream_part', failing_once)
    await pipeline.run(part_message(0), part_message(1))

    # The held part was not skipped: both parts were reported, and the watermark passed neither unreported.
    assert sorted(pipeline.reports.by_index()) == [0, 1]
    assert pipeline.commits[-1] == (PARTS_PARTITION, 2)
    assert not pipeline.processor.is_dead


class StreamHandler(BaseDrakkarHandler):
    """A stream handler of parts: one group of one precomputed task for each part."""

    async def arrange_stream(self, message):
        yield [stream_task(f't-{message.offset}', message.offset)]


# --- faults on the way ---


async def test_a_record_the_client_refuses_for_a_while_is_sent_again():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate, Item(n=1)]})
    worker = OwnerWorker(world, handler)
    # The first record, ``open``, is refused once with an error a retry can fix.
    worker.client.fail_next('produce', KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT)))
    await worker.start(source_message(500))
    await worker.sleep.fire(1.0)
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    # Then the record of the second part is refused once.
    worker.client.fail_next('produce', KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT)))
    gate.set()
    await worker.sleep.fire(1.0)
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    # Each record was sent again and written once.
    assert [part.index for part in world.parts()] == [0, 1]
    assert world.entries()[-1].body == {'parts': 2}
    await worker.stop()


async def test_a_stop_during_a_split_whose_client_queue_is_full_ends_within_the_limit_of_the_cancel_write():
    world = World()
    handler = SplittingHandler({500: items(3)})
    client = FakeProducerClient(world.log, queue_capacity=1)
    client.hold_deliveries = True
    worker = OwnerWorker(world, handler, client=client)
    await worker.start(source_message(500))
    assert worker.owner is not None
    worker.owner._split_settings = dataclasses.replace(worker.owner._split_settings, cancel_write_seconds=0.05)
    await wait_until(lambda: client.produced == 1)

    with capture_logs() as logs:
        worker.processor.signal_stop()
        await asyncio.wait_for(worker.processor.drain(), timeout=1)
        await asyncio.wait_for(worker.processor.stop(), timeout=1)

    # The split-cancelled record found no room in the client queue within its time limit.
    [line] = [line for line in logs if line['event'] == 'split_cancel_write_failed']
    assert line['error'] == 'TimeoutError'
    assert handler.closed_splits == 1 and worker.pending == 1 and client.produced == 1
    client.release_deliveries()
    await worker.stop()


async def test_a_generator_that_fails_while_it_is_closed_is_logged_and_the_stop_goes_on():
    class Handler(SplittingHandler):
        async def split(self, message):
            try:
                yield Item(n=0)
                await asyncio.Event().wait()
            finally:
                raise RuntimeError('cleanup broke')

    world = World()
    worker = await OwnerWorker(world, Handler()).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    with capture_logs() as logs:
        worker.processor.signal_stop()
        await asyncio.wait_for(worker.processor.drain(), timeout=1)
        await worker.processor.stop()

    [line] = [line for line in logs if line['event'] == 'split_close_failed']
    assert (line['log_level'], line['error']) == ('error', 'cleanup broke')
    assert world.kinds() == ['open', 'split-cancelled']
    await worker.stop()


async def test_a_split_failure_whose_closed_record_can_never_be_written_stalls_the_offset():
    world = World()
    handler = SplittingHandler({500: [RuntimeError('boom')]})
    worker = OwnerWorker(world, handler)
    worker.client.fail_next_deliveries(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    await worker.start(source_message(500))
    await wait_until(lambda: worker.stalls == [PARTITION])

    # The DLQ record exists; the offset does not commit, so a restart ends the job again.
    assert len(worker.dlq_records()) == 1 and world.closed() == [] and worker.commits == []
    await worker.stop()


async def test_a_stop_during_the_retries_of_a_closed_record_leaves_the_offset_pending():
    world = World()
    handler = SplittingHandler({500: [RuntimeError('boom')], 501: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT), times=1)
    await worker.start(source_message(500))
    # The closed record of the failed split waits for its second attempt.
    await wait_until(lambda: worker.sleep.waiting(1.0))

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    assert world.closed() == [] and worker.pending == 1 and worker.commits == []
    assert worker.processor._open_job_offsets == {500}
    await worker.stop()


async def test_a_stop_during_the_closed_write_of_an_ended_job_leaves_the_offset_pending(monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.01)
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    # The closed record stays in the producer: its delivery report never comes.
    worker.client.hold_deliveries = True
    world.report_all()
    await wait_until(lambda: worker.aggregate_sink.calls == 1 and worker.client.produced == 4)
    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    assert worker.pending == 1 and worker.commits == []
    assert worker.processor._abandoned_offsets == {500}
    worker.client.release_deliveries()
    await worker.stop()


async def test_a_payload_for_a_sink_that_is_not_configured_follows_on_send_failure():
    unknown = CollectResult(kafka=[KafkaPayload(sink='no-such-sink', data=Item())])
    world = World()
    handler = SplittingHandler({500: items(1), 501: items(1)}, answer=unknown)
    drop = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    with capture_logs() as logs:
        world.report_all()
        await wait_until(lambda: drop.committed == 501)
    [line] = [line for line in logs if line['event'] == 'aggregate_sink_delivery_failed']
    assert (line['log_level'], line['category'], line['offset_action']) == ('error', 'handler', 'commit')
    assert world.closed()[0]['outcome'] == 'aggregated'
    await drop.stop()

    stall = OwnerWorker(world, handler, instance='b' * 16, config=owner_config(on_send_failure='stall'))
    await stall.start(source_message(501))
    await wait_until(lambda: world.kinds().count('split-done') == 2)
    world.report_all(world.parts(offset=501))
    await wait_until(lambda: stall.stalls == [PARTITION])
    assert len(world.closed()) == 1
    await stall.stop()


async def test_nothing_is_delivered_for_a_job_once_the_processor_gave_its_partition_up():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    worker.processor.suppress_deliveries()
    world.report_all()
    await wait_until(lambda: worker.processor._abandoned_offsets == {500})

    assert worker.aggregate_sink.calls == 0 and world.closed() == [] and worker.commits == []
    await worker.stop()


async def test_an_end_works_on_one_attempt_when_another_instance_opens_the_job_meanwhile():
    world = World()
    gate = asyncio.Event()
    seen: list[tuple[Any, ...]] = []

    async def answer(job: SplitJob) -> Any:
        await gate.wait()
        indexes = sorted([result.index async for result in job.results()])
        seen.append((job.job_id, job.attempt, job.received_parts, job.total_parts, indexes))
        return summary(job.attempt)

    handler = SplittingHandler({500: items(2)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    first = world.entries()[0].ref
    world.report_all()
    await wait_until(lambda: len(handler.jobs) == 1)

    # While the hook runs, another instance opens attempt 2: the live state of the job is reset.
    world.put_open(500, 'e' * 16, attempt=2, instance='b' * 16)
    await worker.caught_up()
    counted = split_jobs.labels(outcome='aggregated')._value.get()
    timed = split_job_duration._sum.get()
    with capture_logs() as logs:
        gate.set()
        await wait_until(lambda: world.kinds()[-1] == 'closed')
        await worker.caught_up()
        await wait_until(lambda: any(line['event'] == 'split_job_end_superseded' for line in logs))
    # The log ignored the closed record of this end, so the end of the job is not counted, timed or logged.
    assert split_jobs.labels(outcome='aggregated')._value.get() == counted
    assert split_job_duration._sum.get() == timed
    assert not any(line['event'] == 'split_job_closed' for line in logs)

    # The end stayed on its attempt: the view, the results it read back, and its closed record.
    assert seen == [('3:500#1', 1, 2, 2, [0, 1])]
    closed = world.entries()[-1]
    assert (closed.ref, closed.body['received'], closed.body['parts']) == (first, 2, 2)
    # That record names an attempt that is no longer current, so the log still has the job as open:
    # the offset is not completed, and the owner holds the message and follows attempt 2.
    assert worker.table is not None and worker.table.job(500).closed is None
    assert worker.commits == [] and worker.pending == 1
    assert worker.owner is not None and 500 in worker.owner._jobs
    assert worker.processor._open_job_offsets == {500}

    # Attempt 2 finishes: the job ends again (the aggregate sinks get a second result), and now it is closed.
    world.put(world.ref(500, 'e' * 16, 2), 'split-done', {'parts': 1})
    world.put(world.ref(500, 'e' * 16, 2), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    await wait_until(lambda: worker.committed == 501)
    assert seen[1] == ('3:500#2', 2, 1, 1, [0])
    assert worker.aggregate_sink.calls == 2
    assert split_jobs.labels(outcome='aggregated')._value.get() == counted + 1
    assert world.entries()[-1].ref.attempt_id == 'e' * 16
    await worker.stop()


async def test_a_second_call_of_the_hook_stays_on_the_attempt_the_end_started_with():
    world = World()
    seen: list[tuple[Any, ...]] = []
    reopened = asyncio.Event()

    async def answer(job: SplitJob) -> Any:
        indexes = sorted([result.index async for result in job.results()])
        seen.append((job.attempt, job.timed_out, job.received_parts, job.total_parts, indexes))
        if len(seen) == 1:
            world.put_open(500, 'e' * 16, attempt=2, instance='b' * 16)
            reopened.set()
            raise SplitResultsUnavailable('broker away')
        return None

    handler = SplittingHandler({500: items(2)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await reopened.wait()
    await worker.caught_up()
    await worker.sleep.fire(RESULTS_RETRY_SECONDS, count=2)
    await wait_until(lambda: len(seen) == 2)

    # The job built anew for the second call is the same attempt, with its counters and its results.
    assert seen == [(1, '', 2, 2, [0, 1]), (1, '', 2, 2, [0, 1])]
    await wait_until(lambda: world.kinds()[-1] == 'closed')
    assert world.entries()[-1].ref.attempt == 1 and worker.commits == []
    await worker.stop()


async def test_an_end_that_has_not_called_its_hook_is_left_when_another_instance_opens_the_job():
    world = World()
    seen: list[tuple[Any, ...]] = []

    async def answer(job: SplitJob) -> Any:
        seen.append((job.attempt, job.received_parts, job.total_parts))
        return summary(job.attempt)

    handler = SplittingHandler({500: items(2)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await worker.caught_up()
    first, second = world.parts()

    # The last result and the open of another instance reach the tracker in one read: the end of
    # attempt 1 is decided, and attempt 2 is current before the task of that end has run a step.
    world.report(first)
    await worker.caught_up()
    world.report(second)
    world.put_open(500, 'e' * 16, attempt=2, instance='b' * 16)
    await worker.caught_up()
    await asyncio.sleep(0.02)

    # No hook was called with a mix of two attempts, and nothing was closed: the owner follows attempt 2.
    assert seen == [] and 'closed' not in world.kinds()
    assert worker.commits == [] and worker.pending == 1
    assert worker.owner is not None and 500 in worker.owner._jobs
    assert worker.processor._open_job_offsets == {500}

    world.put(world.ref(500, 'e' * 16, 2), 'split-done', {'parts': 1})
    world.put(world.ref(500, 'e' * 16, 2), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    await wait_until(lambda: worker.committed == 501)
    assert seen == [(2, 1, 1)]
    assert world.entries()[-1].ref.attempt_id == 'e' * 16
    await worker.stop()


async def test_an_end_that_has_not_called_its_hook_calls_none_when_another_instance_closes_the_job():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await worker.caught_up()
    first, second = world.parts()

    # The last result and the closed record of another owner reach the tracker in one read.
    world.report(first)
    await worker.caught_up()
    world.report(second)
    world.put_closed(500, first.attempt_id)
    await wait_until(lambda: worker.committed == 501)

    # Closed means done: no hook, no delivery, no second closed record.
    assert handler.jobs == [] and worker.aggregate_sink.calls == 0
    assert world.kinds().count('closed') == 1
    await worker.stop()


async def test_a_job_that_another_owner_closed_completes_its_offset_with_no_hook():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    opened = world.entries()[0]
    await worker.caught_up()

    # An owner that was not revoked yet ended the job; then its last result is read here.
    job = worker.table.job(500)
    world.put(opened.ref, 'closed', {'outcome': 'aggregated', 'received': 1, 'parts': 1, 'duration_seconds': 1.0})
    await worker.caught_up()
    worker.owner.job_complete(job)
    await wait_until(lambda: worker.committed == 501)

    assert handler.jobs == [] and worker.aggregate_sink.calls == 0
    await worker.stop()


async def test_a_timeout_that_comes_with_the_last_acknowledgement_of_a_split_still_ends_the_job(monkeypatch):
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]})
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    owner, table = worker.owner, worker.table
    assert owner is not None and table is not None
    enqueue_nowait = worker.runtime.producer.enqueue_nowait

    def enqueue_and_time_out(topic: str, record: Any) -> Any:
        future = enqueue_nowait(topic, record)
        if dict(record.headers).get('dk-split-record') == b'split-done':
            # The timeout passes when no wait of the split is left to see the event of the tracker.
            future.add_done_callback(lambda _: time_out())
        return future

    def time_out() -> None:
        world.clock.advance(61_000)
        owner.job_timed_out(table.job(500), 'job')

    monkeypatch.setattr(worker.runtime.producer, 'enqueue_nowait', enqueue_and_time_out)
    gate.set()
    await wait_until(lambda: worker.committed == 501)

    assert world.kinds() == ['open', 'split-done', 'closed']
    assert [job.timed_out for job in handler.timeouts] == ['job']
    assert world.closed()[0]['outcome'] == 'timeout'
    # The owner wrote split-done itself, so it knows the number of parts before the tracker read the record.
    assert handler.timeouts[0].total_parts == 1 and handler.timeouts[0].missing_parts() == [0]
    [record] = worker.dlq_records()
    assert record.error == 'job timed out after 1m1s: 0 of 1 parts'
    await worker.stop()


async def test_a_failed_split_that_waits_for_its_closed_record_holds_no_reservation():
    world = World()
    handler = SplittingHandler({500: [RuntimeError('boom')]}, answer=None)
    budget = PendingBudget(max_tasks=100, max_bytes=1_000_000)
    budget.shape.note_window(1, 2, 10)
    worker = OwnerWorker(world, handler, budget=budget)
    worker.processor._windows_seen = partition_module.WARM_UP_WINDOWS
    # The closed record of the failed split gets no delivery report for a while.
    worker.client.hold_deliveries = True
    await worker.start(source_message(500), source_message(501))
    await wait_until(lambda: worker.client.produced == 1)
    await asyncio.sleep(0.02)

    assert (budget.free_tasks, budget.free_bytes) == (100, 1_000_000)
    assert handler.jobs == []
    worker.client.release_deliveries()
    # The local message behind it runs on a reservation of its own.
    await wait_until(lambda: worker.committed == 502)
    assert [job.source_message.offset for job in handler.jobs] == [501]
    assert (budget.free_tasks, budget.free_bytes) == (100, 1_000_000)
    await worker.stop()


async def test_a_split_waits_for_room_in_a_full_client_queue_and_a_stop_ends_that_wait():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: items(5), 501: [Item(n=0), gate, Item(n=1), Item(n=2)]})
    client = FakeProducerClient(world.log, queue_capacity=1)
    worker = OwnerWorker(world, handler, client=client)
    await worker.start(source_message(500))
    # One record fits in the queue at a time: the split waits for room and loses no part.
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    assert [part.index for part in world.parts(offset=500)] == [0, 1, 2, 3, 4]

    assert worker.owner is not None
    worker.owner._split_settings = dataclasses.replace(worker.owner._split_settings, cancel_write_seconds=0.05)
    worker.give(source_message(501))
    await wait_until(lambda: len(world.parts(offset=501)) == 1)
    client.hold_deliveries = True
    gate.set()
    # The second part fills the queue; the third finds no room and waits for it.
    await wait_until(lambda: client.produced == 10 and handler.pulls == 9)
    await asyncio.sleep(0.03)
    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=0.5)
    await asyncio.wait_for(worker.processor.stop(), timeout=3)

    assert client.produced == 10 and worker.pending == 2 and worker.commits == []
    assert handler.closed_splits == 2
    client.release_deliveries()
    await worker.runtime.stop(deadline=0.0)


async def test_an_interrupt_is_seen_between_two_parts_of_a_generator_that_never_suspends():
    world = World()
    worker: OwnerWorker

    class Handler(SplittingHandler):
        async def split(self, message):
            for n in range(5000):
                if n == 10:
                    assert worker.owner is not None
                    for run in list(worker.owner._runs):
                        run.interrupt('superseded')
                yield Item(n=n)

    worker = await OwnerWorker(world, Handler()).start(source_message(500))
    await wait_until(lambda: len(world.parts()) > 0)
    await wait_until(lambda: worker.owner is not None and not worker.owner._runs)

    # The pull in progress ends with its part; the next one does not start: the open record and
    # eleven parts were handed to the producer, and no more.
    assert worker.client.produced == 12
    await wait_until(lambda: len(world.parts()) == 11)
    assert [part.index for part in world.parts()] == list(range(11))
    await worker.stop()


async def test_a_split_that_stops_while_it_waits_for_its_part_records_leaves_no_unread_exception(monkeypatch):
    monkeypatch.setattr(splitting_module, 'MAX_UNACKED_PARTS', 2)
    world = World()
    handler = SplittingHandler({500: items(5)})
    worker = OwnerWorker(world, handler)
    unread: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _, context: unread.append(context))
    try:
        await worker.restore()
        worker.run()
        assert worker.owner is not None
        worker.owner._split_settings = dataclasses.replace(worker.owner._split_settings, cancel_write_seconds=0.05)
        worker.give(source_message(500))
        await wait_until(lambda: world.kinds() == ['open'])
        worker.client.hold_deliveries = True
        # Two part records wait for their acknowledgement when the stop comes.
        await wait_until(lambda: worker.client.produced == 3)
        await asyncio.sleep(0.02)
        worker.processor.signal_stop()
        await worker.processor.stop()
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert unread == []
    worker.client.release_deliveries()
    await worker.runtime.stop(deadline=0.0)
