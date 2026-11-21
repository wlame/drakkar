"""A split job across a restart, a revoke or a stop of its owner.

Each restart test builds a second owner (a new tracker, a new processor, a new
instance id) on the same topics and gives it the same uncommitted source
message. The tests of an assignment drive a real Kafka source with the job
tracker over the fake consumer.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, TopicPartition
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.consumer import KafkaConsumer
from drakkar.metrics import split_jobs
from drakkar.models import SourceMessage
from drakkar.split.models import PartResult, SplitJob, SplitResultsUnavailable
from drakkar.split.partmode import no_report
from drakkar.split.records import JobRef, log_record, open_body, result_body
from drakkar.split.tracker import TIMEOUT_CHECK_SECONDS
from tests.conftest import wait_for
from tests.split_helpers import FakeConsumerClient
from tests.split_owner_helpers import (
    GROUP,
    PARTITION,
    RESULTS_GROUP,
    RESULTS_TOPIC,
    SOURCE_TOPIC,
    Item,
    OwnerWorker,
    SplittingHandler,
    World,
    items,
    owner_config,
    source_message,
    summary,
    wait_until,
)
from tests.stream_helpers import make_pool, stream_task
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


async def test_an_owner_that_waits_for_results_stands_down_for_the_attempt_of_another_owner():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    old = await OwnerWorker(world, handler).start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await old.caught_up()

    world.put_open(500, SECOND, attempt=2, instance=NEW_INSTANCE)
    world.put(world.ref(500, SECOND, 2), 'split-done', {'parts': 1})
    world.put(world.ref(500, SECOND, 2), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))
    await old.caught_up()
    await asyncio.sleep(0.05)

    # The job of the other owner is complete, and this owner does not end it.
    assert handler.jobs == [] and world.closed() == [] and old.pending == 1
    await old.stop()


# --- stop and revoke ---


async def test_a_stop_during_a_split_cancels_it_cleanly_and_the_offset_stays_pending():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]})
    worker = await OwnerWorker(world, handler).start(source_message(500), source_message(501))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)

    worker.processor.signal_stop()
    await asyncio.wait_for(worker.processor.drain(), timeout=1)

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
    parts = FakeRuntimeParts()
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
    assert runtime.tracker.table(1) is None and runtime._owners == {}
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


async def test_faults_of_the_source_consumer_around_a_restore_are_logged_and_the_processor_still_starts():
    app = split_app(SplittingHandler())
    runtime = await wired(app)
    source = app.kafka_source
    assert source is not None
    source.consumer.pause.side_effect = RuntimeError('pause refused')
    source.consumer.committed_offset.side_effect = RuntimeError('query refused')
    source.consumer.resume.side_effect = RuntimeError('resume refused')

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
    [resume_failed] = events(logs, 'split_restore_resume_failed')
    assert (resume_failed['log_level'], resume_failed['partition']) == ('error', 1)

    async def broken_release(partition: int) -> None:
        raise RuntimeError('unassign refused')

    runtime.release = broken_release  # ty: ignore[invalid-assignment]
    with capture_logs() as logs:
        await source.on_revoke([1])
    [release_failed] = events(logs, 'split_release_failed')
    assert (release_failed['log_level'], release_failed['partition']) == ('warning', 1)
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
