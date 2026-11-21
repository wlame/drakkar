"""A split job across a restart, a revoke or a stop of its owner.

Each restart test builds a second owner (a new tracker, a new processor, a new
instance id) on the same topics and gives it the same uncommitted source
message. The tests of an assignment drive a real Kafka source with the job
tracker over the fake consumer.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, TopicPartition
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.consumer import KafkaConsumer
from drakkar.metrics import split_jobs
from drakkar.models import SourceMessage
from drakkar.sources import kafka as kafka_module
from drakkar.split import partmode
from drakkar.split.models import PartResult, SplitJob, SplitResultsUnavailable
from drakkar.split.partmode import no_report
from drakkar.split.records import JobRef, log_record, open_body, parse_log_record, parse_part_headers, result_body
from drakkar.split.table import FOREIGN_SPLIT_GRACE_SECONDS
from drakkar.split.tracker import TIMEOUT_CHECK_SECONDS
from tests.conftest import wait_for
from tests.split_helpers import FakeConsumerClient
from tests.split_owner_helpers import (
    GROUP,
    PARTITION,
    PARTS_TOPIC,
    RESULTS_GROUP,
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
from tests.test_split_parts_source import FakeRuntimeParts, fake_consumer, split_app, wired

FIRST = '1' * 16
SECOND = '2' * 16
THIRD = '3' * 16
NEW_INSTANCE = 'b' * 16


@pytest.fixture(autouse=True)
def fast_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the wait of a coalesced commit: these tests wait for the commit of one offset."""
    monkeypatch.setattr(partition_module, 'COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)


def jobs_counted(outcome: str) -> float:
    return split_jobs.labels(outcome=outcome)._value.get()


def events(logs: list[dict], name: str) -> list[dict]:
    return [line for line in logs if line['event'] == name]


async def split_and_crash(world: World, plan: list[Any], *, reported: int = 0) -> OwnerWorker:
    """Let a first owner split message 500, report the first ``reported`` parts, then end it with no clean stop."""
    first = await OwnerWorker(world, SplittingHandler({500: plan})).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all(world.parts()[:reported])
    await first.caught_up()
    await first.crash()
    assert first.commits == []
    return first


# --- the rows of the restore table ---


async def test_a_restart_after_split_done_does_not_split_again_and_waits_for_the_missing_results():
    world = World()
    await split_and_crash(world, items(3), reported=1)
    handler = SplittingHandler({500: items(3)}, answer=summary())

    with capture_logs() as logs:
        second = await OwnerWorker(world, handler, instance=NEW_INSTANCE).start(source_message(500))
        await wait_until(lambda: events(logs, 'split_job_resumed') != [])

    [resumed] = events(logs, 'split_job_resumed')
    assert (resumed['log_level'], resumed['job'], resumed['attempt'], resumed['parts'], resumed['received']) == (
        'info',
        '3:500#1',
        1,
        3,
        1,
    )
    await asyncio.sleep(0.05)
    assert handler.split_calls == [] and handler.jobs == []
    assert world.kinds() == ['open', 'split-done', 'result'] and len(world.parts()) == 3
    assert second.pending == 1 and second.commits == []

    world.report_all(world.parts()[1:])
    await wait_until(lambda: second.committed == 501)

    [job] = handler.jobs
    assert (job.attempt, job.total_parts, job.received_parts) == (1, 3, 3)
    opened, closed = world.entries()[0], world.entries()[-1]
    # The new owner closes the attempt the old owner opened.
    assert closed.kind == 'closed' and closed.ref == opened.ref
    assert second.client.produced == 1
    await second.stop()


async def test_results_that_arrived_with_no_owner_are_counted_and_the_job_ends_at_once():
    world = World()
    await split_and_crash(world, items(2))
    world.report_all()
    handler = SplittingHandler({500: items(2)}, answer=summary())

    second = await OwnerWorker(world, handler, instance=NEW_INSTANCE).start(source_message(500))
    await wait_until(lambda: second.committed == 501)

    [job] = handler.jobs
    assert (job.received_parts, job.failed_parts) == (2, 0)
    assert handler.split_calls == [] and second.aggregate_sink.calls == 1
    await second.stop()


async def test_a_restart_during_the_split_starts_attempt_two_and_ignores_the_results_of_attempt_one():
    world = World()
    first = await OwnerWorker(world, SplittingHandler({500: [Item(n=0), Item(n=1), asyncio.Event()]})).start(
        source_message(500)
    )
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 2)
    await first.crash()
    # No clean cancel: the attempt counts as crashed.
    assert world.kinds() == ['open']
    old_parts = world.parts()
    world.report(old_parts[0])
    handler = SplittingHandler({500: items(2)}, answer=summary())

    with capture_logs() as logs:
        second = await OwnerWorker(world, handler, instance=NEW_INSTANCE).start(source_message(500))
        await wait_until(lambda: world.kinds() == ['open', 'result', 'open', 'split-done'])

    [started] = events(logs, 'split_attempt_started')
    assert (started['log_level'], started['job'], started['attempt'], started['crashed_attempts']) == (
        'warning',
        '3:500#2',
        2,
        1,
    )
    first_open, second_open = (entry for entry in world.entries() if entry.kind == 'open')
    assert (second_open.ref.attempt, second_open.body['instance']) == (2, NEW_INSTANCE)
    assert second_open.ref.attempt_id != first_open.ref.attempt_id
    new_parts = world.parts(attempt=2)
    assert [part.attempt_id for part in new_parts] == [second_open.ref.attempt_id] * 2

    # Results of the old attempt count for nothing.
    world.report(old_parts[1])
    await second.caught_up()
    assert handler.jobs == [] and second.pending == 1
    world.report_all(new_parts)
    await wait_until(lambda: second.committed == 501)

    [job] = handler.jobs
    assert (job.job_id, job.attempt, job.received_parts) == ('3:500#2', 2, 2)
    assert world.entries()[-1].ref == second_open.ref
    await second.stop()


async def test_three_crashed_attempts_with_max_attempts_three_end_as_attempts_spent():
    world = World()
    for attempt, attempt_id in enumerate((FIRST, SECOND, THIRD), start=1):
        world.put_open(500, attempt_id, attempt=attempt)
    handler = SplittingHandler({500: items(2)})
    spent = jobs_counted('attempts_spent')

    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler, config=owner_config(max_attempts=3)).start(source_message(500))
        await wait_until(lambda: worker.committed == 501)

    assert handler.split_calls == [] and handler.jobs == []
    [record] = worker.dlq_records()
    assert (record.stage, record.error, record.attempts) == ('split', 'split did not finish in 3 attempts', 3)
    assert (record.key, record.value) == (b'order-key', b'{"n": 5}')
    closed = world.entries()[-1]
    assert (closed.kind, closed.body['outcome'], closed.ref.attempt_id) == ('closed', 'attempts_spent', THIRD)
    assert jobs_counted('attempts_spent') == spent + 1
    [failed] = events(logs, 'split_failed')
    assert (failed['log_level'], failed['attempts']) == ('error', 3)
    await worker.stop()


async def test_two_crashed_attempts_with_max_attempts_three_start_the_third():
    world = World()
    world.put_open(500, FIRST, attempt=1)
    world.put_open(500, SECOND, attempt=2)
    handler = SplittingHandler({500: items(1)})

    worker = await OwnerWorker(world, handler, config=owner_config(max_attempts=3)).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'open', 'open', 'split-done'])

    assert world.entries()[-1].ref.attempt == 3 and handler.split_calls == [500]
    await worker.stop()


async def test_a_cancelled_split_is_not_counted_against_max_attempts():
    world = World()
    for attempt, attempt_id in enumerate((FIRST, SECOND, THIRD), start=1):
        world.put_open(500, attempt_id, attempt=attempt)
        world.put(world.ref(500, attempt_id, attempt), 'split-cancelled', {})
    handler = SplittingHandler({500: items(1)}, answer=summary())

    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler, config=owner_config(max_attempts=3)).start(source_message(500))
        await wait_until(lambda: world.kinds()[-1] == 'split-done')

    [started] = events(logs, 'split_attempt_started')
    assert (started['attempt'], started['crashed_attempts']) == (4, 0)
    assert worker.dlq_records() == []
    world.report_all(world.parts(attempt=4))
    await wait_until(lambda: worker.committed == 501)
    await worker.stop()


async def test_a_restart_after_the_aggregate_sinks_and_before_closed_aggregates_again():
    world = World()
    first_handler = SplittingHandler({500: items(1)}, answer=summary())
    first = await OwnerWorker(world, first_handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    # The closed record never leaves the producer of the first owner.
    first.client.hold_deliveries = True
    world.report_all()
    await wait_until(lambda: first.aggregate_sink.calls == 1 and first.client.produced == 4)
    await first.crash()
    assert world.closed() == [] and first.commits == []
    handler = SplittingHandler({500: items(1)}, answer=summary())

    second = await OwnerWorker(world, handler, instance=NEW_INSTANCE).start(source_message(500))
    await wait_until(lambda: second.committed == 501)

    # The aggregate sinks received the result twice: at-least-once.
    assert (len(first_handler.jobs), len(handler.jobs)) == (1, 1)
    assert second.aggregate_sink.calls == 1
    assert [body['outcome'] for body in world.closed()] == ['aggregated']
    await second.stop()


async def test_a_restart_after_closed_and_before_the_commit_completes_the_offset_with_no_hook():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    world.put_closed(500, FIRST)
    handler = SplittingHandler({500: items(1)}, answer=summary())

    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    assert handler.split_calls == [] and handler.jobs == [] and handler.timeouts == []
    assert worker.client.produced == 0 and worker.dlq_records() == []
    assert world.kinds() == ['open', 'split-done', 'closed']
    await worker.stop()


async def test_a_committed_source_offset_above_a_job_retires_it():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    world.log.commit(GROUP, SOURCE_TOPIC, PARTITION, 501)
    handler = SplittingHandler(answer=None)

    worker = await OwnerWorker(world, handler).start(source_message(501))
    await wait_until(lambda: worker.committed == 502)

    # Committed means done: the job left the table at the restore, and nothing was written for it.
    assert worker.table is not None and len(worker.table) == 0
    assert world.kinds() == ['open', 'split-done'] and worker.dlq_records() == []
    assert [job.source_message.offset for job in handler.jobs] == [501]
    await worker.stop()


async def test_a_closed_job_leaves_the_table_when_its_source_offset_is_committed():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=None)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    world.report_all()
    await wait_until(lambda: worker.committed == 501)
    await worker.caught_up()

    assert worker.table is not None
    await wait_until(lambda: worker.table.job(500) is None)
    await worker.stop()


async def test_a_restored_job_whose_message_never_arrives_is_closed_as_orphaned_when_a_later_one_is_accepted():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    world.put(world.ref(500, FIRST), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    handler = SplittingHandler(answer=None)
    orphaned = jobs_counted('orphaned')

    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler).start(source_message(502))
        await wait_until(lambda: worker.committed == 503)

    closed = world.entries()[-1]
    assert (closed.kind, closed.ref.offset, closed.ref.attempt_id) == ('closed', 500, FIRST)
    assert (closed.body['outcome'], closed.body['received'], closed.body['parts']) == ('orphaned', 1, 2)
    [line] = events(logs, 'split_job_orphaned')
    assert (line['log_level'], line['category'], line['job']) == ('warning', 'split', '3:500#1')
    assert jobs_counted('orphaned') == orphaned + 1
    # No hook ran for the orphan, and no DLQ record exists: its message is gone.
    assert [job.source_message.offset for job in handler.jobs] == [502]
    assert worker.dlq_records() == []
    await worker.stop()


async def test_a_restored_job_that_timed_out_before_its_message_arrived_ends_when_the_message_arrives():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler, config=owner_config(job_timeout='1m')).start()

    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.02)
    # The hook and the DLQ record need the message.
    assert handler.timeouts == [] and world.closed() == []

    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.timeouts
    assert (job.timed_out, job.received_parts, job.total_parts) == ('job', 0, 2)
    assert handler.split_calls == []
    [record] = worker.dlq_records()
    assert record.error == 'job timed out after 1m1s: 0 of 2 parts'
    assert world.closed()[0]['outcome'] == 'timeout'
    await worker.stop()


# --- two owners of one job ---


async def test_an_old_owner_that_reads_the_open_of_a_new_owner_cancels_its_split_and_writes_nothing_more():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    old = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await old.caught_up()
    superseded = jobs_counted('superseded')

    with capture_logs() as logs:
        world.put_open(500, SECOND, attempt=2, instance=NEW_INSTANCE)
        await wait_until(lambda: events(logs, 'split_job_superseded') != [])
    await asyncio.sleep(0.05)

    assert handler.closed_splits == 1
    # No split-cancelled, no closed: the job belongs to the other owner now.
    assert world.kinds() == ['open', 'open'] and old.client.produced == 2
    assert jobs_counted('superseded') == superseded + 1
    assert events(logs, 'split_job_superseded')[0]['log_level'] == 'warning'
    assert old.pending == 1 and old.commits == []
    # The offset holds no drain.
    old.processor.signal_stop()
    await asyncio.wait_for(old.processor.drain(), timeout=1)
    await old.stop()


async def test_an_owner_that_waits_for_results_follows_the_attempt_of_another_instance():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await worker.caught_up()

    world.put_open(500, SECOND, attempt=2, instance=NEW_INSTANCE)
    world.put(world.ref(500, SECOND, 2), 'split-done', {'parts': 1})
    await worker.caught_up()
    # A result of the attempt this owner wrote counts for nothing any more.
    world.report_all()
    await worker.caught_up()
    assert handler.jobs == [] and worker.pending == 1
    world.put(world.ref(500, SECOND, 2), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    await wait_until(lambda: worker.committed == 501)

    # The holder of the message ends the job on the attempt that is current in the log.
    [job] = handler.jobs
    assert (job.job_id, job.attempt, job.received_parts) == ('3:500#2', 2, 1)
    closed = world.entries()[-1]
    assert (closed.kind, closed.ref.attempt_id) == ('closed', SECOND)
    await worker.stop()


# --- stop and revoke ---


async def test_a_stop_during_a_split_cancels_it_cleanly_and_the_offset_stays_pending():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    worker.processor.signal_stop()
    # The drain does not wait for the cancel of the split: the stop of the processor does.
    await asyncio.wait_for(worker.processor.drain(), timeout=0.2)
    await asyncio.wait_for(worker.processor.stop(), timeout=1)

    # The generator was closed and the attempt is marked as cancelled, so it does not count as crashed.
    assert handler.closed_splits == 1
    assert world.kinds() == ['open', 'split-cancelled']
    assert worker.commits == []
    # The message behind it in the window was never begun: its offset is pending too, and out of the drain.
    assert handler.split_calls == [500]
    assert worker.pending == 2 and worker.processor._abandoned_offsets == {501}
    await asyncio.wait_for(worker.stop(), timeout=2)


async def test_a_stop_does_not_wait_for_an_open_job():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))
    await wait_until(lambda: len(handler.jobs) == 1 and worker.pending == 1)

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    assert worker.pending == 1 and worker.commits == [] and world.closed() == []
    # A result that completes the job after the stop starts no end of it.
    world.report_all()
    await worker.caught_up()
    await asyncio.sleep(0.02)
    assert [job.source_message.offset for job in handler.jobs] == [501]
    await asyncio.wait_for(worker.stop(), timeout=2)


async def test_an_aggregate_in_progress_finishes_inside_the_drain_and_its_offset_commits():
    world = World()
    gate = asyncio.Event()

    async def answer(job: SplitJob) -> Any:
        await gate.wait()
        return summary()

    handler = SplittingHandler({500: items(1)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: len(handler.jobs) == 1)

    worker.processor.signal_stop()
    drain = asyncio.create_task(worker.processor.drain())
    await asyncio.sleep(0.05)
    # The drain waits for the end in progress.
    assert not drain.done()
    gate.set()
    await asyncio.wait_for(drain, timeout=1)

    assert world.closed()[0]['outcome'] == 'aggregated' and worker.committed == 501
    await worker.stop()


async def test_a_stop_during_an_aggregate_that_does_not_end_leaves_the_offset_pending():
    world = World()

    async def answer(job: SplitJob) -> Any:
        await asyncio.Event().wait()

    handler = SplittingHandler({500: items(1)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: len(handler.jobs) == 1)

    # What a revoke does when the drain budget ends.
    worker.processor.signal_stop()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(worker.processor.drain(), timeout=0.1)
    worker.processor.suppress_deliveries()
    await worker.processor.cancel_active_tasks()
    await asyncio.wait_for(worker.processor.stop(), timeout=1)

    assert worker.pending == 1 and worker.commits == [] and world.closed() == []
    assert worker.processor._abandoned_offsets == {500}
    await worker.runtime.stop(deadline=0.0)


async def test_a_stop_ends_the_wait_for_results_that_cannot_be_read():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=SplitResultsUnavailable('broker away'))
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: worker.sleep.waiting(30.0, count=2))

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    assert worker.pending == 1 and world.closed() == [] and len(handler.jobs) == 1
    await worker.stop()


# --- the Kafka source: restore on an assignment ---


class GatedConsumer(FakeConsumerClient):
    """A consumer of the job tracker whose first query waits for the test, so a restore takes as long as wanted."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.gate = asyncio.Event()
        self.committed_calls = 0

    async def committed(self, partitions: Any, timeout: float | None = None) -> Any:
        self.committed_calls += 1
        await self.gate.wait()
        return await super().committed(partitions, timeout)


def real_time_parts() -> FakeRuntimeParts:
    """The fakes of a runtime whose records carry the time of the wall clock, which the app's runtime reads."""
    parts = FakeRuntimeParts()
    parts.log.clock = lambda: int(time.time() * 1000)
    return parts


def source_record(offset: int, partition: int = 1) -> SourceMessage:
    return SourceMessage(topic=SOURCE_TOPIC, partition=partition, offset=offset, value=b'{"n": 1}', timestamp=0)


def put_complete_job(parts: FakeRuntimeParts, *, partition: int = 1, offset: int = 40) -> None:
    """Write a job with one part and its result to the results log, as another owner left it."""
    ref = JobRef(group=GROUP, partition=partition, offset=offset, attempt=1, attempt_id=FIRST)
    bodies = [
        ('open', open_body(worker='worker-gone', instance='f' * 16, request_id='req-1')),
        ('split-done', {'parts': 1}),
        ('result', result_body(PartResult(index=0, worker='w', outcome='ok'))),
    ]
    for kind, body in bodies:
        record = log_record(ref, kind, body)  # ty: ignore[invalid-argument-type]
        parts.log.append(RESULTS_TOPIC, partition, key=record.key, value=record.value, headers=record.headers)


async def test_assign_revoke_assign_during_a_slow_restore_starts_the_processor_once_on_a_complete_table():
    handler = SplittingHandler({40: items(1)}, answer=None)
    app = split_app(handler)
    parts = real_time_parts()
    runtime = await wired(app, parts)
    gated = GatedConsumer(parts.log, group_id=RESULTS_GROUP)
    runtime.tracker._consumer_factory = lambda client_config: gated
    put_complete_job(parts)
    source = app.kafka_source
    assert source is not None

    with capture_logs() as logs:
        source.on_assign([1])
        first = source.processors[1]
        await wait_for(lambda: gated.committed_calls == 1, interval=0.001)
        # The partition is paused and its processor waits for the restore.
        assert source.restoring_partitions == {1} and first._task is None
        source.consumer.pause.assert_awaited_once_with([1])
        # What the consumer had fetched already waits in the queue.
        source._route_messages([source_record(40)])
        assert first.queue_size == 1 and source.queued_messages() == 0

        await source.on_revoke([1])
        assert source.restoring_partitions == set() and source._restore_tasks == {}
        assert first._task is None and first.queue_size == 0 and source.processors == {}

        source.on_assign([1])
        second = source.processors[1]
        await wait_for(lambda: gated.committed_calls == 2, interval=0.001)
        assert second is not first and second._task is None
        gated.gate.set()
        await wait_for(lambda: second._task is not None, interval=0.001)
        await wait_for(lambda: source._restore_tasks == {}, interval=0.001)

    assert [line['partition'] for line in events(logs, 'partition_processor_started')] == [1]
    assert source.restoring_partitions == set()
    # Started on a complete table: the job the log holds is there, with its result.
    table = runtime.tracker.table(1)
    assert table is not None and table.job(40).is_complete
    source.consumer.resume.assert_awaited_once_with([1])
    source.consumer.committed_offset.assert_awaited_with(1)

    # The message of the restored job ends it at once; the commit retires the job from the table.
    source._route_messages([source_record(40)])
    await wait_for(lambda: source.consumer.commit.await_count == 1, interval=0.005)
    source.consumer.commit.assert_awaited_once_with({1: 41})
    assert handler.split_calls == [] and len(handler.jobs) == 1
    await wait_for(lambda: table.job(40) is None, interval=0.001)

    await source.on_revoke([1])
    # The revoke dropped the partition from the tracker and the owner from the runtime.
    assert runtime.tracker.table(1) is None and runtime._events.owners() == {}
    await runtime.stop(deadline=0.0)


async def test_no_resume_touches_a_partition_whose_restore_runs():
    app = split_app(
        SplittingHandler(), ui={'enabled': False, 'release': {'enabled': False}, 'consume_pause': {'enabled': True}}
    )
    parts = FakeRuntimeParts()
    runtime = await wired(app, parts)
    gated = GatedConsumer(parts.log, group_id=RESULTS_GROUP)
    runtime.tracker._consumer_factory = lambda client_config: gated
    source = app.kafka_source
    assert source is not None
    source.on_assign([0])
    source.on_assign([1])
    await wait_for(lambda: gated.committed_calls == 2, interval=0.001)
    source.consumer.resume.reset_mock()

    # Backpressure ends, an operator's pause ends: neither resumes a restoring partition.
    source.paused = True
    await source._apply_backpressure(source.consumer, 0, 10, 20)
    await app.consume_pause.pause(60)
    await app.consume_pause.resume()
    source.consumer.resume.assert_not_awaited()
    assert source._paused_partitions() == frozenset({0, 1})

    # A restore that ends while another pause holds the partition leaves the resume to that pause.
    source.paused = False
    await app.consume_pause.pause(60)
    gated.gate.set()
    await wait_for(lambda: source.restoring_partitions == set(), interval=0.001)
    await asyncio.sleep(0.01)
    source.consumer.resume.assert_not_awaited()
    await app.consume_pause.resume()
    source.consumer.resume.assert_awaited_once_with([0, 1])

    await source.on_revoke([0, 1])
    await runtime.stop(deadline=0.0)


async def test_a_stop_of_the_source_during_a_restore_starts_no_processor_and_leaves_no_task():
    app = split_app(SplittingHandler())
    parts = FakeRuntimeParts()
    runtime = await wired(app, parts)
    gated = GatedConsumer(parts.log, group_id=RESULTS_GROUP)
    runtime.tracker._consumer_factory = lambda client_config: gated
    source = app.kafka_source
    assert source is not None
    source.on_assign([2])
    processor = source.processors[2]
    await wait_for(lambda: gated.committed_calls == 1, interval=0.001)
    source._route_messages([source_record(7, partition=2)])

    source.signal_stop()
    assert await asyncio.wait_for(source.drain(deadline=asyncio.get_running_loop().time() + 5), timeout=1)
    await asyncio.wait_for(source.stop(deadline=0.0), timeout=2)

    assert processor._task is None and source._restore_tasks == {} and processor.queue_size == 0
    await runtime.stop(deadline=0.0)


async def test_a_restore_that_fails_for_an_unknown_reason_takes_the_worker_out_of_rotation():
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None

    async def broken_follow(partition: int, *, committed_source_offset: int | None) -> Any:
        raise RuntimeError('the tracker is broken')

    runtime.follow = broken_follow  # ty: ignore[invalid-assignment]
    with capture_logs() as logs:
        source.on_assign([1])
        await wait_for(lambda: source._restore_tasks == {}, interval=0.001)

    assert source.dead_partitions() == [1] and source.restoring_partitions == set()
    [line] = events(logs, 'split_restore_task_failed')
    assert (line['log_level'], line['category'], line['partition']) == ('critical', 'split', 1)
    await source.on_revoke([1])
    await runtime.stop(deadline=0.0)


async def precomputed_arrange(messages: list[SourceMessage], pending: Any) -> list[Any]:
    return [stream_task(f't-{message.offset}', message.offset) for message in messages]


async def test_a_worker_with_the_feature_off_starts_its_processors_at_once_and_never_calls_split():
    handler = SplittingHandler()
    handler.arrange = precomputed_arrange  # ty: ignore[invalid-assignment]
    app = split_app(handler, enabled=False)
    app._executor_pool = make_pool()
    app._lifecycle._bind_sources()
    source = app.kafka_source
    assert source is not None and app.parts_source is None
    source.consumer = fake_consumer()

    source.on_assign([1])
    processor = source.processors[1]
    assert processor._task is not None and processor._owner is None and processor._part_mode is None
    assert source.restoring_partitions == set() and source._restore_tasks == {}
    source._route_messages([source_record(5)])
    await wait_for(lambda: source.consumer.commit.await_count == 1, interval=0.005)

    assert handler.split_calls == [] and handler.jobs == []
    source.consumer.pause.assert_not_awaited()
    await source.on_revoke([1])


async def test_faults_of_the_source_consumer_around_a_restore_are_logged_and_the_processor_still_starts(monkeypatch):
    monkeypatch.setattr(kafka_module, 'RESTORE_RESUME_FIRST_WAIT_SECONDS', 0.01)
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None
    source.consumer.pause.side_effect = RuntimeError('pause refused')
    source.consumer.committed_offset.side_effect = RuntimeError('query refused')
    source.consumer.resume.side_effect = [RuntimeError('resume refused'), None]

    with capture_logs() as logs:
        source.on_assign([1])
        await wait_for(lambda: source._restore_tasks == {}, interval=0.001)

    # The committed offset is unknown, so nothing retires at the restore; the processor runs.
    assert source.processors[1]._task is not None and source.restoring_partitions == set()
    assert [(line['event'], line['log_level']) for line in logs if line.get('category') == 'split'][:4] == [
        ('split_restore_pause_failed', 'error'),
        ('split_committed_offset_unknown', 'warning'),
        ('split_restore_started', 'info'),
        ('split_restore_finished', 'info'),
    ]
    # The resume that failed was tried again: a partition left paused would stop with no sign of it.
    [resume_failed] = events(logs, 'split_restore_resume_failed')
    assert (resume_failed['log_level'], resume_failed['partition']) == ('error', 1)
    assert source.consumer.resume.await_count == 2

    async def broken_release(partition: int) -> None:
        raise RuntimeError('unassign refused')

    runtime.release = broken_release  # ty: ignore[invalid-assignment]
    with capture_logs() as logs:
        await source.on_revoke([1])
    [release_failed] = events(logs, 'split_release_failed')
    assert (release_failed['log_level'], release_failed['partition']) == ('warning', 1)
    await runtime.stop(deadline=0.0)


async def test_a_revoke_cancels_the_retries_of_a_resume_after_a_restore(monkeypatch):
    monkeypatch.setattr(kafka_module, 'RESTORE_RESUME_FIRST_WAIT_SECONDS', 30.0)
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None
    source.consumer.resume.side_effect = RuntimeError('resume refused')
    source.on_assign([1])
    await wait_for(lambda: source.consumer.resume.await_count == 1, interval=0.001)
    assert 1 in source._restore_tasks and source.restoring_partitions == set()

    await asyncio.wait_for(source.on_revoke([1]), timeout=1)

    assert source._restore_tasks == {} and source.processors == {}
    await runtime.stop(deadline=0.0)


async def test_a_failure_while_the_owner_of_a_restored_partition_is_built_takes_the_worker_out_of_rotation():
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None

    def broken_owner_for(partition: int, **kwargs: Any) -> Any:
        raise RuntimeError('no owner')

    runtime.owner_for = broken_owner_for  # ty: ignore[invalid-assignment]
    with capture_logs() as logs:
        source.on_assign([1])
        await wait_for(lambda: source._restore_tasks == {}, interval=0.001)

    # Not left paused with no line: the partition is dead, and /readyz names it.
    assert source.dead_partitions() == [1] and source.restoring_partitions == set()
    assert source.processors[1]._task is None
    [line] = events(logs, 'split_restore_task_failed')
    assert (line['log_level'], line['error']) == ('critical', 'no owner')
    await source.on_revoke([1])
    await runtime.stop(deadline=0.0)


async def test_a_revoke_waits_a_bounded_time_for_a_restore_that_does_not_end(monkeypatch):
    monkeypatch.setattr(kafka_module, 'SPLIT_RELEASE_TIMEOUT_SECONDS', 0.05)
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None
    stuck = asyncio.Event()

    async def follow_that_ignores_its_cancel(partition: int, *, committed_source_offset: int | None) -> Any:
        while not stuck.is_set():
            try:
                await stuck.wait()
            except asyncio.CancelledError:
                continue
        raise asyncio.CancelledError

    runtime.follow = follow_that_ignores_its_cancel  # ty: ignore[invalid-assignment]
    source.on_assign([1])
    await asyncio.sleep(0.01)

    with capture_logs() as logs:
        await asyncio.wait_for(source.on_revoke([1]), timeout=1)

    [line] = events(logs, 'split_restore_cancel_slow')
    assert (line['log_level'], line['partitions']) == ('warning', [1])
    stuck.set()
    await asyncio.sleep(0.01)
    await runtime.stop(deadline=0.0)


async def test_a_commit_of_the_parts_source_is_not_told_to_the_job_tracker():
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source, parts = app.kafka_source, app.parts_source
    assert source is not None and parts is not None
    noted: list[tuple[int, int]] = []
    runtime.note_source_commit = lambda partition, offset: noted.append((partition, offset))  # ty: ignore[invalid-assignment]

    await parts._handle_commit(1, 10)
    await source._handle_commit(2, 20)

    assert noted == [(2, 20)]
    await runtime.stop(deadline=0.0)


async def test_the_consumer_gives_the_committed_offset_of_one_partition(monkeypatch):
    client = MagicMock()
    committed = [TopicPartition('orders', 4, 17)]
    client.committed = AsyncMock(side_effect=lambda tps, timeout=None: committed)
    monkeypatch.setattr('drakkar.consumer.AIOConsumer', lambda *args, **kwargs: client)
    config = owner_config()
    consumer = KafkaConsumer(
        config.declared_connection('sources.kafka.connection', 'kafka', 'main'), config.sources.kafka
    )

    assert await consumer.committed_offset(4) == 17
    committed[0] = TopicPartition('orders', 4, OFFSET_INVALID)
    assert await consumer.committed_offset(4) is None
    client.committed.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
    assert await consumer.committed_offset(4) is None


async def test_the_report_of_a_processor_of_local_messages_refuses_to_be_called():
    with pytest.raises(RuntimeError, match='not reported to a topic'):
        await no_report(source_message(), PartResult(index=0, worker='w', outcome='ok'))


# --- the holder follows the log ---


def foreign_result(world: World, attempt_id: str, attempt: int, index: int = 0) -> None:
    """Write the result of one part of a hand-written attempt."""
    body = result_body(PartResult(index=index, worker='w', outcome='ok'))
    world.put(world.ref(500, attempt_id, attempt), 'result', body)


async def superseded_owner(world: World, handler: SplittingHandler, **config: Any) -> OwnerWorker:
    """An owner whose split of message 500 another instance's ``open`` replaced; it holds the message and follows."""
    worker = OwnerWorker(world, handler, config=owner_config(**config))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    world.put_open(500, SECOND, attempt=2, instance=NEW_INSTANCE)
    await wait_until(lambda: handler.closed_splits == 1)
    await worker.caught_up()
    assert worker.owner is not None and 500 in worker.owner._jobs
    return worker


async def test_a_new_owner_that_a_late_owner_replaced_takes_the_job_back_when_that_owner_is_revoked():
    world = World()
    gate = asyncio.Event()
    new_handler = SplittingHandler({500: [Item(n=0), gate, Item(n=1)]}, answer=summary())
    new = await OwnerWorker(world, new_handler, instance=NEW_INSTANCE).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await new.caught_up()

    # The old owner did not see its revoke yet, and begins the same message after the new owner's open.
    old_handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    old = await OwnerWorker(world, old_handler, instance='c' * 16).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'open'])
    await wait_until(lambda: new_handler.closed_splits == 1)
    # The new owner stopped its split and wrote nothing more for it, but it keeps the message.
    assert new.owner is not None and 500 in new.owner._jobs and new.pending == 1
    assert new.client.produced == 2

    # Now the old owner is revoked: its attempt is cancelled, and nobody else would take the job.
    with capture_logs() as logs:
        old.processor.signal_stop()
        await old.processor.stop()
        await wait_until(lambda: world.kinds()[-1] == 'open')
        gate.set()
        await wait_until(lambda: world.kinds()[-1] == 'split-done')
    assert world.kinds() == ['open', 'open', 'split-cancelled', 'open', 'split-done']
    third = world.entries()[-1]
    assert (third.ref.attempt, third.body) == (3, {'parts': 2})
    assert world.entries()[3].body['instance'] == NEW_INSTANCE
    # The cancelled attempt is not counted: one attempt crashed, the one the old owner replaced.
    [started] = events(logs, 'split_attempt_started')
    assert (started['attempt'], started['crashed_attempts']) == (3, 1)

    world.report_all(world.parts(attempt=3))
    await wait_until(lambda: new.committed == 501)

    assert new_handler.split_calls == [500, 500]
    [job] = new_handler.jobs
    assert (job.attempt, job.received_parts) == (3, 2)
    assert old_handler.jobs == [] and old.commits == []
    await old.runtime.stop(deadline=0.0)
    await new.stop()


async def test_a_resumed_job_that_another_instance_opens_again_is_ended_on_its_new_attempt():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.owner is not None and 500 in worker.owner._jobs)

    # The job was opened by an instance that is gone; a third instance opens it again and its parts finish.
    world.put_open(500, SECOND, attempt=2, instance='e' * 16)
    world.put(world.ref(500, SECOND, 2), 'split-done', {'parts': 1})
    foreign_result(world, SECOND, 2)
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.jobs
    assert (job.attempt, job.received_parts, job.total_parts) == (2, 1, 1)
    assert handler.split_calls == []
    assert world.entries()[-1].ref.attempt_id == SECOND
    await worker.stop()


async def test_a_held_job_that_another_instance_closes_completes_its_offset_with_no_hook():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    opened = world.entries()[0]

    with capture_logs() as logs:
        world.put(opened.ref, 'closed', {'outcome': 'timeout', 'received': 0, 'parts': 2, 'duration_seconds': 1.0})
        await wait_until(lambda: worker.committed == 501)

    # Closed means done: no hook, no second closed record, and the job is dropped.
    assert handler.jobs == [] and handler.timeouts == [] and worker.dlq_records() == []
    assert world.kinds() == ['open', 'split-done', 'closed']
    assert worker.owner is not None and worker.owner._jobs == {}
    [line] = events(logs, 'split_job_closed_elsewhere')
    assert (line['log_level'], line['job'], line['outcome']) == ('info', '3:500#1', 'timeout')
    # Results that come later change nothing.
    world.report_all()
    await worker.caught_up()
    assert handler.jobs == [] and worker.commits == [501]
    await worker.stop()


async def test_a_closed_record_that_comes_during_the_split_stops_the_split_and_completes_the_offset():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()

    world.put(
        world.entries()[0].ref, 'closed', {'outcome': 'dlq', 'received': 0, 'parts': None, 'duration_seconds': 1.0}
    )
    await wait_until(lambda: worker.committed == 501)

    assert handler.closed_splits == 1 and world.kinds() == ['open', 'closed']
    assert handler.jobs == [] and worker.dlq_records() == []
    await worker.stop()


async def test_the_holder_starts_the_next_attempt_when_another_instance_cancels_its_split():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    worker = await superseded_owner(world, handler)
    assert handler.split_calls == [500]

    world.put(world.ref(500, SECOND, 2), 'split-cancelled', {})
    await wait_until(lambda: world.kinds() == ['open', 'open', 'split-cancelled', 'open'])
    gate.set()
    await wait_until(lambda: world.kinds()[-1] == 'split-done')

    last_open = world.entries()[3]
    assert (last_open.ref.attempt, last_open.body['instance']) == (3, 'a' * 16)
    world.report_all(world.parts(attempt=3))
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500, 500] and len(handler.jobs) == 1
    await worker.stop()


async def test_the_holder_starts_the_next_attempt_when_a_split_of_another_instance_shows_no_end():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    worker = await superseded_owner(world, handler)

    # One second before the grace time ends nothing happens.
    world.clock.advance((FOREIGN_SPLIT_GRACE_SECONDS - 1) * 1000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    assert world.kinds() == ['open', 'open'] and handler.split_calls == [500]
    world.clock.advance(1000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: world.kinds() == ['open', 'open', 'open'])
    gate.set()
    await wait_until(lambda: world.kinds()[-1] == 'split-done')

    assert world.entries()[2].ref.attempt == 3
    world.report_all(world.parts(attempt=3))
    await wait_until(lambda: worker.committed == 501)
    assert len(handler.jobs) == 1 and worker.dlq_records() == []
    await worker.stop()


async def test_a_split_of_another_instance_with_no_end_fails_the_job_when_the_attempts_are_spent():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]}, answer=summary())
    worker = await superseded_owner(world, handler, max_attempts=2)

    world.clock.advance(FOREIGN_SPLIT_GRACE_SECONDS * 1000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: worker.committed == 501)

    # The replaced attempt and the one left behind both count: no third call of split.
    assert handler.split_calls == [500]
    [record] = worker.dlq_records()
    assert (record.stage, record.error, record.attempts) == ('split', 'split did not finish in 2 attempts', 2)
    closed = world.entries()[-1]
    assert (closed.kind, closed.body['outcome'], closed.ref.attempt_id) == ('closed', 'attempts_spent', SECOND)
    await worker.stop()


async def test_a_new_attempt_outside_the_partition_loop_that_yields_nothing_fails_the_job():
    class Handler(SplittingHandler):
        async def split(self, message):
            self.split_calls.append(message.offset)
            if len(self.split_calls) > 1:
                return
            try:
                yield Item(n=0)
                await asyncio.Event().wait()
            finally:
                self.closed_splits += 1

    world = World()
    handler = Handler()
    worker = await superseded_owner(world, handler)

    world.put(world.ref(500, SECOND, 2), 'split-cancelled', {})
    await wait_until(lambda: worker.committed == 501)

    # Parts of an earlier attempt may have run: the message cannot run here as a local message too.
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == (
        'split',
        'split failed: the split yielded no part for a job that has parts in the log',
    )
    closed = world.entries()[-1]
    assert (closed.kind, closed.body['outcome'], closed.ref.attempt_id) == ('closed', 'split_failed', SECOND)
    await worker.stop()


# --- a stop, a timeout and an event around a restore; the DLQ record of a job ---


async def test_a_stop_while_an_orphan_is_closed_does_not_hold_the_drain(monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.01)
    world = World()
    world.put_open(400, FIRST)
    world.put(world.ref(400, FIRST), 'split-done', {'parts': 2})
    gate = asyncio.Event()
    handler = SplittingHandler({500: [gate, *items(2)]}, answer=summary())
    worker = OwnerWorker(world, handler)
    # The closed record of the orphan gets no delivery report.
    worker.client.hold_deliveries = True
    await worker.start(source_message(500))
    # The closed write of the orphan does not hold the message behind it: its split has started.
    await wait_until(lambda: worker.client.produced == 1 and handler.split_calls == [500])

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=0.5)
    await asyncio.wait_for(worker.processor.stop(), timeout=1)

    assert handler.closed_splits == 1 and worker.pending == 1 and worker.commits == []
    assert worker.owner is not None and worker.owner._tasks == set()
    worker.client.release_deliveries()
    await worker.runtime.stop(deadline=0.0)


async def test_a_complete_restored_job_is_aggregated_also_when_its_idle_time_has_passed():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    foreign_result(world, FIRST, 1)
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler, config=owner_config(job_timeout='1h', idle_timeout='1m')).start()

    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.02)
    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 1 and handler.timeouts == [] and worker.dlq_records() == []
    assert world.closed()[0]['outcome'] == 'aggregated'
    await worker.stop()


async def test_a_restored_job_that_timed_out_and_then_got_its_last_result_is_aggregated():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = await OwnerWorker(world, handler, config=owner_config(job_timeout='1m')).start()

    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.02)
    foreign_result(world, FIRST, 1)
    await worker.caught_up()
    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    # A complete job is never timed out, whatever was told about it before.
    assert len(handler.jobs) == 1 and handler.timeouts == [] and worker.dlq_records() == []
    await worker.stop()


async def test_a_timeout_between_the_end_of_a_restore_and_the_start_of_the_processor_is_not_lost():
    world = World()
    world.put_open(500, FIRST)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    handler = SplittingHandler({500: items(2)})
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.restore()

    # The tracker is live and tells the timeout once; the owner of the partition does not exist yet.
    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.02)
    worker.run(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.timeouts
    assert (job.timed_out, job.total_parts) == ('job', 2)
    assert world.closed()[0]['outcome'] == 'timeout'
    await worker.stop()


async def test_an_event_of_a_partition_that_is_not_followed_is_dropped():
    world = World()
    worker = OwnerWorker(world, SplittingHandler())
    await worker.restore()
    table = worker.table
    assert table is not None
    world.put_open(500, FIRST)
    await worker.caught_up()

    # Partition 9 is neither followed nor owned; a released partition keeps nothing either.
    worker.runtime._events.job_complete(9, table.job(500))
    await worker.runtime.release(PARTITION)
    worker.runtime._events.job_closed(PARTITION, table.job(500))

    assert worker.runtime._events._kept == {} and worker.runtime._events.owners() == {}
    await worker.runtime.stop(deadline=0.0)


async def test_a_local_message_whose_task_goes_to_the_dlq_names_the_part_sinks_that_delivered():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler()
    handler.task_ids = ['t-a', 't-b']
    handler.results = {'t-a': part_sink_result()}
    pool = ScriptedPool(failing={'t-b'}, gates={'t-b': gate})
    worker = OwnerWorker(world, handler, pool=pool, executor_on_failure='dlq')
    await worker.start(source_message(500))
    await wait_until(lambda: worker.part_sink.calls == 1)
    gate.set()
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.jobs
    assert (job.local, job.failed_parts, job.dlq_parts, job.tasks_succeeded, job.tasks_failed) == (True, 1, 1, 1, 1)
    # One record, with the facts of the failed task, as for an ordinary message.
    [record] = worker.dlq_records()
    assert (record.stage, record.task_id, record.attempts) == ('execute', 't-b', 1)
    assert record.error.startswith('exit code 2: ')
    assert (record.key, record.value, record.source_offset) == (b'order-key', b'{"n": 5}', 500)
    assert record.delivered_sinks == ['postgres/main']
    assert worker.client.produced == 0
    await worker.stop()


async def test_the_dlq_record_of_a_job_names_the_part_sinks_that_delivered():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    first, second = world.parts()

    world.report(first, deliveries={'postgres/main': {'delivered': 2}, 'redis/cache': {'skipped': 1}})
    world.report(second, outcome='failed', error='bad part', deliveries={'mongo/docs': {'delivered': 1}})
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert record.delivered_sinks == ['mongo/docs', 'postgres/main']
    await worker.stop()


class ParsingHandler(SplittingHandler):
    input_model = Item


async def test_a_window_with_a_job_two_local_messages_and_one_that_cannot_be_parsed():
    world = World()
    arranged: list[list[int]] = []

    class Handler(ParsingHandler):
        async def arrange(self, messages, pending):
            arranged.append([message.offset for message in messages])
            return await super().arrange(messages, pending)

    handler = Handler({500: items(2)}, answer=None)
    worker = OwnerWorker(world, handler, config=owner_config(on_parse_error='dlq'))
    await worker.start(
        source_message(500),
        source_message(501),
        source_message(502, value=b'not json'),
        source_message(503),
    )
    await wait_until(lambda: len(handler.jobs) == 2)

    # One window: only its two local messages reached arrange, in one call.
    assert arranged == [[501, 503]]
    assert handler.split_calls == [500, 501, 503]
    [record] = worker.dlq_records()
    assert (record.stage, record.source_offset, record.value) == ('parse', 502, b'not json')
    # The job holds the watermark below everything behind it.
    assert worker.pending == 1 and worker.commits == []
    world.report_all()
    await wait_until(lambda: worker.committed == 504)
    assert [job.source_message.offset for job in handler.jobs] == [501, 503, 500]
    await worker.stop()


async def test_a_delivery_that_fails_after_its_split_stopped_leaves_no_unread_exception():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = OwnerWorker(world, handler)
    worker.client.hold_deliveries = True
    unread: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _, context: unread.append(context))
    try:
        await worker.start(source_message(500))
        # The open record waits for its delivery report when the job is closed by another instance.
        await wait_until(lambda: worker.client.produced == 1)
        world.put_closed(500, FIRST)
        await wait_until(lambda: worker.committed == 501)
        worker.client.fail_next_deliveries(KafkaError(KafkaError._MSG_TIMED_OUT))
        worker.client.release_deliveries()
        await asyncio.sleep(0.05)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert unread == []
    await worker.stop()


# --- a revoke through the Kafka source ---


async def test_a_revoke_during_a_split_and_with_an_open_job_ends_inside_the_drain_bound():
    handler = SplittingHandler({40: items(1), 41: [Item(n=0), asyncio.Event()]})
    app = split_app(handler)
    parts = real_time_parts()
    runtime = await wired(app, parts)
    source = app.kafka_source
    assert source is not None
    source.on_assign([1])
    await wait_for(lambda: source._restore_tasks == {}, interval=0.001)
    processor = source.processors[1]

    def kinds() -> list[tuple[str, int]]:
        entries = [parse_log_record(record.headers, record.value) for record in parts.log.records(RESULTS_TOPIC, 1)]
        return [(entry.kind, entry.ref.offset) for entry in entries if entry is not None]

    def published(offset: int) -> int:
        records = [record for partition in range(8) for record in parts.log.records(PARTS_TOPIC, partition)]
        found = [parse_part_headers(record.headers) for record in records]
        return sum(1 for part in found if part is not None and part.source_offset == offset)

    source._route_messages([source_record(40), source_record(41)])
    # Offset 40 is an open job that waits for its result; offset 41 is in the middle of its split.
    await wait_for(lambda: kinds() == [('open', 40), ('split-done', 40), ('open', 41)], interval=0.001)
    await wait_for(lambda: published(41) == 1, interval=0.001)

    started = time.monotonic()
    # The drain timeout of this worker is one second.
    await asyncio.wait_for(source.on_revoke([1]), timeout=1)
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    # The split was cancelled cleanly; nothing was closed, and nothing was committed.
    assert kinds()[-1] == ('split-cancelled', 41) and handler.closed_splits == 2
    assert processor.offset_tracker.pending_count == 2
    source.consumer.commit.assert_not_awaited()
    assert handler.jobs == [] and source.processors == {}
    assert runtime.tracker.table(1) is None
    await runtime.stop(deadline=0.0)


async def test_the_holder_takes_over_from_a_real_owner_that_died_in_its_split():
    world = World()
    gate = asyncio.Event()
    new_handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    new = await OwnerWorker(world, new_handler, instance=NEW_INSTANCE).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await new.caught_up()
    # Another owner begins the same message, replaces the attempt, and dies with no clean stop.
    old = await OwnerWorker(world, SplittingHandler({500: [Item(n=0), asyncio.Event()]}), instance='c' * 16).start(
        source_message(500)
    )
    await wait_until(lambda: world.kinds() == ['open', 'open'] and new_handler.closed_splits == 1)
    await old.crash()
    assert world.kinds() == ['open', 'open']

    # No record will ever end that attempt; after the grace time its owner is taken for dead.
    await new.caught_up()
    world.clock.advance(FOREIGN_SPLIT_GRACE_SECONDS * 1000)
    await new.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: world.kinds() == ['open', 'open', 'open'])
    gate.set()
    await wait_until(lambda: world.kinds()[-1] == 'split-done')
    world.report_all(world.parts(attempt=3))
    await wait_until(lambda: new.committed == 501)

    [job] = new_handler.jobs
    assert (job.attempt, job.received_parts) == (3, 1)
    await new.stop()


async def test_an_end_in_progress_runs_to_its_finish_when_another_instance_closes_the_job():
    world = World()
    gate = asyncio.Event()

    async def answer(job: SplitJob) -> Any:
        await gate.wait()
        return summary()

    handler = SplittingHandler({500: items(1)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: len(handler.jobs) == 1)

    world.put_closed(500, world.entries()[0].ref.attempt_id)
    await worker.caught_up()
    assert worker.commits == []
    gate.set()
    await wait_until(lambda: worker.committed == 501)

    # A duplicate aggregate is allowed; the job has its closed record already, so no second one is written.
    assert worker.aggregate_sink.calls == 1
    assert world.kinds().count('closed') == 1
    await worker.stop()


async def test_a_raise_of_on_job_timeout_sends_the_message_to_the_dlq_and_closes_as_timeout():
    class Handler(SplittingHandler):
        async def on_job_timeout(self, job: SplitJob) -> Any:
            raise ValueError('cannot decide')

    world = World()
    handler = Handler({500: items(2)})
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await worker.caught_up()

    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: worker.committed == 501)

    [record] = worker.dlq_records()
    assert (record.stage, record.error) == ('aggregate', 'on_job_timeout failed: cannot decide')
    # Every end through on_job_timeout closes as timeout.
    assert world.closed()[0]['outcome'] == 'timeout'
    await worker.stop()


async def test_the_cancel_of_a_split_closes_the_generator_and_writes_split_cancelled_at_the_same_time():
    class Handler(SplittingHandler):
        async def split(self, message):
            try:
                yield Item(n=0)
                await asyncio.Event().wait()
            finally:
                # A generator that needs time to clean up.
                await asyncio.sleep(0.3)
                self.closed_splits += 1

    world = World()
    handler = Handler()
    worker = OwnerWorker(world, handler)
    await worker.restore()
    worker.run()
    assert worker.owner is not None
    worker.owner._split_settings = dataclasses.replace(worker.owner._split_settings, cancel_write_seconds=0.4)
    worker.give(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    # The split-cancelled record gets no delivery report, so its write takes its whole time limit.
    worker.client.hold_deliveries = True

    worker.processor.signal_stop()
    started = time.monotonic()
    # The drain does not wait for the cancel at all.
    await asyncio.wait_for(worker.processor.drain(), timeout=0.1)
    await worker.processor.stop()
    elapsed = time.monotonic() - started

    # One after the other they would take 0.7 seconds.
    assert handler.closed_splits == 1 and 0.35 < elapsed < 0.6
    worker.client.release_deliveries()
    await worker.runtime.stop(deadline=0.0)


async def test_the_stop_of_a_processor_cancels_a_split_cancel_that_does_not_end_in_its_time():
    class Handler(SplittingHandler):
        async def split(self, message):
            try:
                yield Item(n=0)
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(5)

    world = World()
    worker = await OwnerWorker(world, Handler()).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.stop(timeout=0.05), timeout=1)

    assert worker.owner is not None and worker.owner._tasks == set()
    assert worker.pending == 1 and worker.commits == []
    await worker.runtime.stop(deadline=0.0)


async def test_an_error_that_ends_a_task_of_the_owner_is_logged():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    worker = await superseded_owner(world, handler)
    assert worker.owner is not None

    async def broken_send(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError('the DLQ sink broke')

    worker.owner._ender.send_to_dlq = broken_send  # ty: ignore[invalid-assignment]
    handler.plan[500] = [RuntimeError('boom')]
    with capture_logs() as logs:
        world.put(world.ref(500, SECOND, 2), 'split-cancelled', {})
        await wait_until(lambda: events(logs, 'split_owner_task_failed') != [])

    [line] = events(logs, 'split_owner_task_failed')
    assert (line['log_level'], line['category'], line['error']) == ('error', 'split', 'the DLQ sink broke')
    # Nothing says the request reached the DLQ: the offset does not commit.
    assert worker.pending == 1 and worker.commits == []
    await worker.stop()


# --- a job first seen through its message: every step of the rules of a holder reads the table ---


async def live_owner(world: World, handler: SplittingHandler) -> OwnerWorker:
    """An owner that exists before any record of job 500: the tracker reads them live, with no message held."""
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='2m', idle_timeout='1m'))
    return await worker.start()


async def told_before_the_message(world: World, worker: OwnerWorker, *, advance_ms: int) -> None:
    """Let time pass and let the tracker tell what it tells once, while the owner holds no message."""
    await worker.caught_up()
    world.clock.advance(advance_ms)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.02)


async def test_a_job_closed_before_its_message_arrived_completes_the_offset():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = await live_owner(world, handler)
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put_closed(500, FIRST)
    await told_before_the_message(world, worker, advance_ms=0)

    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    assert handler.split_calls == [] and handler.jobs == [] and handler.timeouts == []
    await worker.stop()


async def test_a_job_complete_before_its_message_arrived_is_aggregated_whatever_time_has_passed():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = await live_owner(world, handler)
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    foreign_result(world, FIRST, 1)
    await told_before_the_message(world, worker, advance_ms=10 * 60_000)

    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 1 and handler.timeouts == [] and handler.split_calls == []
    await worker.stop()


@pytest.mark.parametrize(
    ('split_done', 'advance_ms', 'kind'),
    [
        (True, 121_000, 'job'),
        (True, 61_000, 'idle'),
        # No split-done: the idle time does not run, only the job timeout can pass.
        (False, 121_000, 'job'),
    ],
)
async def test_a_job_that_timed_out_before_its_message_arrived_ends_through_on_job_timeout(
    split_done, advance_ms, kind
):
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await live_owner(world, handler)
    # Another instance opens the job after this owner exists; the tracker tells its timeout once, to nobody.
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    if split_done:
        world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    await told_before_the_message(world, worker, advance_ms=advance_ms)
    assert handler.timeouts == []

    worker.give(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [job] = handler.timeouts
    assert (job.timed_out, job.received_parts, job.total_parts) == (kind, 0, 2 if split_done else None)
    assert handler.split_calls == [] and handler.jobs == []
    assert world.closed()[0]['outcome'] == 'timeout'
    await worker.stop()


async def test_a_job_that_was_idle_before_its_message_arrived_ends_when_it_is_idle_again():
    world = World()
    handler = SplittingHandler({500: items(2)})
    worker = await OwnerWorker(world, handler, config=owner_config(job_timeout='1h', idle_timeout='1m')).start()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 2})
    # The idle timeout passes and is told while nobody holds the message; then a result comes.
    await told_before_the_message(world, worker, advance_ms=61_000)
    foreign_result(world, FIRST, 1)
    await worker.caught_up()

    worker.give(source_message(500))
    await wait_until(lambda: worker.owner is not None and 500 in worker.owner._jobs)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    # Not idle now: the job waits for its second result.
    assert handler.timeouts == [] and worker.pending == 1

    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: worker.committed == 501)
    [job] = handler.timeouts
    assert (job.timed_out, job.received_parts, job.total_parts) == ('idle', 1, 2)
    await worker.stop()


async def test_a_job_with_split_done_before_its_message_arrived_waits_for_its_results():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = await live_owner(world, handler)
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-done', {'parts': 1})
    await told_before_the_message(world, worker, advance_ms=1_000)

    worker.give(source_message(500))
    await wait_until(lambda: worker.owner is not None and 500 in worker.owner._jobs)
    await asyncio.sleep(0.02)
    assert handler.jobs == [] and handler.split_calls == [] and worker.pending == 1

    foreign_result(world, FIRST, 1)
    await wait_until(lambda: worker.committed == 501)
    assert len(handler.jobs) == 1
    await worker.stop()


@pytest.mark.parametrize(
    ('cancelled', 'advance_ms', 'crashed_attempts'),
    [
        # Cancelled by its owner: the attempt is not counted.
        (True, 1_000, 0),
        # Left behind for longer than the grace time, and told as stalled before the message came.
        (False, FOREIGN_SPLIT_GRACE_SECONDS * 1000, 1),
        # A message that arrives finds an attempt with no split-done: its split is made again at once.
        (False, 1_000, 1),
    ],
)
async def test_a_job_whose_split_did_not_end_before_its_message_arrived_starts_its_next_attempt(
    cancelled, advance_ms, crashed_attempts
):
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1h'))
    await worker.start()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    if cancelled:
        world.put(world.ref(500, FIRST), 'split-cancelled', {})
    await told_before_the_message(world, worker, advance_ms=advance_ms)

    with capture_logs() as logs:
        worker.give(source_message(500))
        await wait_until(lambda: world.kinds()[-1] == 'split-done')

    [started] = events(logs, 'split_attempt_started')
    assert (started['attempt'], started['crashed_attempts']) == (2, crashed_attempts)
    assert handler.split_calls == [500]
    world.report_all(world.parts(attempt=2))
    await wait_until(lambda: worker.committed == 501)
    await worker.stop()


async def test_a_restored_job_whose_new_split_yields_nothing_fails_as_it_does_in_a_takeover():
    world = World()
    world.put_open(500, FIRST)
    handler = SplittingHandler(answer=summary())
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    # Parts of the first attempt may have run, so the message does not run here as a local message too.
    assert handler.split_calls == [500] and handler.jobs == []
    [record] = worker.dlq_records()
    assert (record.stage, record.error) == (
        'split',
        'split failed: the split yielded no part for a job that has parts in the log',
    )
    closed = world.entries()[-1]
    assert (closed.kind, closed.body['outcome'], closed.ref.attempt_id) == ('closed', 'split_failed', FIRST)
    await worker.stop()


# --- the end of a job and what the log shows ---


async def test_an_end_whose_closed_record_is_not_read_back_before_a_stop_leaves_the_offset_pending(monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.05)
    world = World()
    gate = asyncio.Event()

    async def answer(job: SplitJob) -> Any:
        await gate.wait()
        return summary()

    handler = SplittingHandler({500: items(1)}, answer=answer)
    worker = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: len(handler.jobs) == 1)
    # From here on the tracker reads nothing.
    [consumer] = worker.tracker_consumers
    consumer.fail_next('consume', KafkaException(KafkaError(KafkaError._TRANSPORT)), times=1000)
    # The read that was already waiting gets one more record, of another job; every later read fails.
    world.put_open(900, SECOND)
    await worker.caught_up()

    gate.set()
    await wait_until(lambda: world.kinds()[-1] == 'closed')
    await asyncio.sleep(0.02)
    # The record is in the log, but the table does not show it: the offset is not completed.
    assert worker.commits == [] and worker.pending == 1
    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

    # The drain waited only the grace time; the next owner finds the closed record and completes the offset.
    assert worker.pending == 1 and worker.processor._abandoned_offsets == {500}
    await worker.stop()


async def test_two_owners_that_hold_one_message_both_complete_its_offset_when_the_job_ends():
    world = World()
    gate = asyncio.Event()
    late_handler = SplittingHandler({500: [Item(n=0), gate, Item(n=1)]}, answer=summary())
    new_handler = SplittingHandler({500: items(2)}, answer=summary())
    late = await OwnerWorker(world, late_handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await late.caught_up()
    # The new owner begins the same message: its open replaces the attempt of the late owner, which follows it.
    new = await OwnerWorker(world, new_handler, instance=NEW_INSTANCE).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'open', 'split-done'])
    await late.caught_up()
    await wait_until(lambda: late_handler.closed_splits == 1)

    world.report_all(world.parts(attempt=2))
    await wait_until(lambda: late.committed == 501 and new.committed == 501)

    # Each owner ends the job on attempt 2, or finds the closed record of the other before its own
    # hook ran. The log accepts one closed record, and both offsets complete by it.
    ended = late_handler.jobs + new_handler.jobs
    assert 1 <= len(ended) <= 2 and len(late_handler.jobs) <= 1 and len(new_handler.jobs) <= 1
    assert {job.attempt for job in ended} == {2}
    assert world.kinds().count('closed') == len(ended)
    assert {closed['outcome'] for closed in world.closed()} == {'aggregated'}
    assert late.pending == 0 and new.pending == 0
    await late.stop()
    await new.stop()
