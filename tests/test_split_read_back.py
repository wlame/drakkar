"""The owner of a split job and the records it wrote itself.

The job tracker reads a record some time after its acknowledgement. These
tests hold the tracker back and check that the owner decides nothing about a
job until the job table has read what the owner wrote for it, also when a
delivery report names no offset.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc

import pytest
from confluent_kafka import KafkaError, KafkaException
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.split import owner as owner_module
from drakkar.split import partmode
from drakkar.split.tracker import TIMEOUT_CHECK_SECONDS
from tests.split_owner_helpers import (
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
from tests.test_split_restart import FIRST, NEW_INSTANCE, SECOND, THIRD, events, jobs_counted


@pytest.fixture(autouse=True)
def fast_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the wait of a coalesced commit: these tests wait for the commit of one offset."""
    monkeypatch.setattr(partition_module, 'COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)


def closed_records(world: World) -> list:
    return [entry for entry in world.entries() if entry.kind == 'closed']


# --- a timeout and a failure while the tracker reads late ---


async def test_a_job_timeout_that_fires_while_the_own_split_runs_calls_the_hook_once_on_the_own_attempt():
    world = World()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-cancelled', {})
    world.clock.advance(30_000)
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]}, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    await worker.restore()
    released = worker.hold_tracker_reads()
    worker.run(source_message(500))
    await wait_until(lambda: len(world.parts()) == 1)

    # The timeout check of the tracker runs on a table that still ends at the attempt of the other
    # instance: the job is due there, but the owner has written an attempt the table has not read.
    world.clock.advance(40_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await asyncio.sleep(0.1)
    assert handler.timeouts == [] and closed_records(world) == []

    released.set()
    await worker.caught_up()
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500]
    assert [(job.attempt, job.timed_out) for job in handler.timeouts] == [(2, 'job')]
    assert [(closed.ref.attempt, closed.body['outcome']) for closed in closed_records(world)] == [(2, 'timeout')]
    await worker.stop()


class _YieldsNothingTheSecondTime(SplittingHandler):
    """Its second split of a message yields nothing; just before, another instance opens the job again."""

    def __init__(self, world: World) -> None:
        super().__init__()
        self.world = world
        self.worker: OwnerWorker | None = None
        self.released: asyncio.Event | None = None

    async def split(self, message):
        self.split_calls.append(message.offset)
        if len(self.split_calls) > 1:
            assert self.worker is not None
            # The tracker reads nothing from here on, so the owner does not see this ``open`` in time.
            self.released = self.worker.hold_tracker_reads()
            self.world.put_open(500, THIRD, attempt=3, instance='d' * 16)
            return
        try:
            yield Item(n=0)
            await asyncio.Event().wait()
        finally:
            self.closed_splits += 1


async def test_a_split_that_fails_before_its_own_open_while_another_instance_opens_the_job_follows_that_attempt():
    world = World()
    handler = _YieldsNothingTheSecondTime(world)
    counted = jobs_counted('split_failed')
    worker = OwnerWorker(world, handler, config=owner_config())
    handler.worker = worker
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    world.put_open(500, SECOND, attempt=2, instance=NEW_INSTANCE)
    await wait_until(lambda: handler.closed_splits == 1)
    await worker.caught_up()

    with capture_logs() as logs:
        # The other instance cancels its attempt: the split starts again here, and yields nothing.
        world.put(world.ref(500, SECOND, 2), 'split-cancelled', {})
        await wait_until(lambda: world.kinds()[-1] == 'closed')
        [closed] = closed_records(world)
        await asyncio.sleep(0.05)
        assert worker.commits == [] and worker.pending == 1
        assert handler.released is not None
        handler.released.set()
        await wait_until(lambda: len(events(logs, 'split_job_end_superseded')) == 1)
        await asyncio.sleep(0.05)

    # The record names the attempt the log had when the split started; the log ignored it for attempt 3.
    assert (closed.ref.attempt_id, closed.body['outcome']) == (SECOND, 'split_failed')
    assert worker.table is not None and worker.owner is not None
    job = worker.table.job(500)
    assert (job.attempt_id, job.closed) == (THIRD, None)
    assert worker.commits == [] and worker.pending == 1 and 500 in worker.owner._jobs
    assert jobs_counted('split_failed') == counted
    assert handler.split_calls == [500, 500] and len(closed_records(world)) == 1
    await worker.stop()


async def test_a_split_whose_open_record_is_refused_for_good_closes_the_attempt_the_log_has():
    world = World()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-cancelled', {})
    handler = SplittingHandler({500: items(1)})
    worker = OwnerWorker(world, handler)
    counted = jobs_counted('split_failed')
    await worker.restore()
    # The ``open`` of the new attempt never reaches the log.
    worker.client.fail_next_deliveries(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))

    worker.run(source_message(500))
    await wait_until(lambda: worker.committed == 501)

    [closed] = closed_records(world)
    assert (closed.ref.attempt_id, closed.body['outcome']) == (FIRST, 'split_failed')
    assert world.kinds().count('open') == 1 and handler.split_calls == [500]
    assert jobs_counted('split_failed') == counted + 1 and len(worker.dlq_records()) == 1
    await worker.stop()


# --- a delivery report that names no offset ---


@pytest.mark.parametrize('kind', ['open', 'split-done'])
async def test_a_record_of_the_split_whose_report_names_no_offset_is_still_waited_for(kind):
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of(kind)
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    released = worker.hold_tracker_reads()
    gate.set()
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()

    # The table has not read the split-done of this owner: nothing is decided, and nothing starts again.
    await asyncio.sleep(0.2)
    assert worker.owner is not None and worker.owner._jobs[500].catching_up
    assert handler.jobs == [] and handler.split_calls == [500] and worker.commits == []

    released.set()
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500] and len(handler.jobs) == 1
    assert world.kinds() == ['open', 'split-done', 'result', 'closed']
    await worker.stop()


async def test_a_split_cancelled_record_whose_report_names_no_offset_is_still_waited_for():
    world = World()
    handler = SplittingHandler({500: [Item(n=0), asyncio.Event()]}, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    worker.hide_offsets_of('split-cancelled')
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    released = worker.hold_tracker_reads()

    # The job timeout stops the split, which marks its attempt as cancelled.
    world.clock.advance(61_000)
    await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    await wait_until(lambda: world.kinds() == ['open', 'split-cancelled'])
    await asyncio.sleep(0.2)
    assert handler.timeouts == [] and worker.commits == []

    released.set()
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500]
    assert [(job.attempt, job.timed_out) for job in handler.timeouts] == [(1, 'job')]
    assert [closed.body['outcome'] for closed in closed_records(world)] == ['timeout']
    await worker.stop()


async def complete_job_with_a_held_tracker(worker: OwnerWorker, *, end_read_failures: int = 0) -> asyncio.Event:
    """Split message 500, report its part, and hold the tracker back just as the end of the job starts.

    The next ``end_read_failures`` reads of the end of the results log fail.
    """
    world = worker.world
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    await worker.caught_up()
    if end_read_failures:
        # In place before the job can end: the owner reads the end right after its closed record.
        [consumer] = worker.tracker_consumers
        failure = KafkaException(KafkaError(KafkaError._TRANSPORT))
        consumer.fail_next('get_watermark_offsets', failure, times=end_read_failures)
    world.report_all()
    await worker.caught_up()
    return worker.hold_tracker_reads()


async def test_a_closed_record_whose_report_names_no_offset_ends_the_job_once():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('closed')
    counted = jobs_counted('aggregated')
    released = await complete_job_with_a_held_tracker(worker)

    with capture_logs() as logs:
        await wait_until(lambda: len(closed_records(world)) == 1)
        await asyncio.sleep(0.2)
        assert len(handler.jobs) == 1 and len(closed_records(world)) == 1 and worker.commits == []
        released.set()
        await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 1 and len(closed_records(world)) == 1
    assert events(logs, 'split_job_end_superseded') == []
    assert len(events(logs, 'split_job_closed')) == 1 and jobs_counted('aggregated') == counted + 1
    await worker.stop()


async def test_no_record_kind_with_a_report_without_an_offset_makes_a_second_split_or_end():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('open', 'split-done', 'split-cancelled', 'closed')

    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: worker.committed == 501)

    assert handler.split_calls == [500] and len(handler.jobs) == 1
    assert world.kinds() == ['open', 'split-done', 'result', 'result', 'closed']
    await worker.stop()


async def test_a_closed_record_that_the_table_has_not_read_is_never_taken_for_a_newer_attempt():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('closed')

    async def end_that_is_too_low(partition: int) -> int:
        return 1

    released = await complete_job_with_a_held_tracker(worker)
    assert worker.owner is not None
    # The owner is told an end below its record: the wait for the offset is over at once.
    worker.owner._settings = dataclasses.replace(worker.owner._settings, end_offset=end_that_is_too_low)

    with capture_logs() as logs:
        await wait_until(lambda: len(closed_records(world)) == 1)
        await asyncio.sleep(0.2)
        # The table still shows the attempt of the record as open: the record is not read yet.
        assert len(handler.jobs) == 1 and len(closed_records(world)) == 1 and worker.commits == []
        released.set()
        await wait_until(lambda: worker.committed == 501)

    assert len(handler.jobs) == 1 and len(closed_records(world)) == 1
    assert events(logs, 'split_job_end_superseded') == []
    await worker.stop()


async def test_a_failed_read_of_the_end_of_the_log_is_tried_again():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('closed')

    # The first read can fail as soon as the closed record is acknowledged.
    with capture_logs() as logs:
        released = await complete_job_with_a_held_tracker(worker, end_read_failures=2)
        await worker.sleep.fire(owner_module.END_OFFSET_RETRY_FIRST_SECONDS)
        await worker.sleep.fire(owner_module.END_OFFSET_RETRY_FIRST_SECONDS * 2)
        released.set()
        await wait_until(lambda: worker.committed == 501)

    # Two failed reads in one interval give one line.
    [failed] = events(logs, 'split_end_offset_read_failed')
    assert (failed['log_level'], failed['partition'], failed['suppressed']) == ('warning', 3, 0)
    assert len(handler.jobs) == 1 and len(closed_records(world)) == 1
    await worker.stop()


async def test_a_stop_while_the_end_of_the_log_cannot_be_read_leaves_the_offset_pending(monkeypatch):
    monkeypatch.setattr(partmode, 'STOP_GRACE_SECONDS', 0.05)
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('closed')
    await complete_job_with_a_held_tracker(worker, end_read_failures=50)
    [consumer] = worker.tracker_consumers
    await wait_until(lambda: worker.sleep.waiting(owner_module.END_OFFSET_RETRY_FIRST_SECONDS))

    await asyncio.wait_for(worker.stop(), timeout=2)

    assert worker.owner is not None and worker.owner._jobs[500].own_unplaced
    assert worker.commits == [] and worker.pending == 1 and len(handler.jobs) == 1
    assert consumer._failures['get_watermark_offsets']


def hold_end_reads(worker: OwnerWorker) -> asyncio.Event:
    """Make every read of the end of the results log by the owner wait until the returned event is set."""
    owner = worker.owner
    assert owner is not None
    read_end = owner._settings.end_offset
    released = asyncio.Event()

    async def held_end(partition: int) -> int | None:
        await released.wait()
        return await read_end(partition)

    owner._settings = dataclasses.replace(owner._settings, end_offset=held_end)
    return released


@pytest.mark.parametrize('event', ['timeout', 'cancelled'])
async def test_nothing_is_decided_while_the_end_of_the_log_is_read_for_records_without_an_offset(event):
    world = World()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put(world.ref(500, FIRST), 'split-cancelled', {})
    world.clock.advance(30_000)
    handler = SplittingHandler({500: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(job_timeout='1m'))
    worker.hide_offsets_of('open', 'split-done')
    await worker.restore()
    tracker_released = worker.hold_tracker_reads()
    worker.run(source_message(500))
    end_released = hold_end_reads(worker)
    assert worker.owner is not None and worker.table is not None
    await wait_until(lambda: world.kinds()[-1] == 'split-done')
    await wait_until(lambda: 500 in worker.owner._jobs and worker.owner._jobs[500].catching_up)
    held = worker.owner._jobs[500]
    # No offset of the own attempt is known, and the end of the log is not read yet.
    assert held.own_written_through == -1 and held.own_unplaced == 1
    assert not worker.owner._table_has_own_records(held)

    # The table still ends at the cancelled attempt of the other instance. An event about it must
    # start neither the end of the job on that attempt nor a second split.
    if event == 'timeout':
        world.clock.advance(40_000)
        await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    else:
        worker.owner.attempt_cancelled(worker.table.job(500))
    await asyncio.sleep(0.1)
    assert handler.timeouts == [] and closed_records(world) == []
    assert handler.split_calls == [500] and world.kinds().count('open') == 2

    end_released.set()
    tracker_released.set()
    await worker.caught_up()
    if event == 'timeout':
        await worker.sleep.fire(TIMEOUT_CHECK_SECONDS)
    else:
        world.report_all(world.parts(attempt=2))
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500] and world.kinds().count('open') == 2
    hooks = [(job.attempt, 'timeout') for job in handler.timeouts] + [(job.attempt, 'ok') for job in handler.jobs]
    assert hooks == [(2, 'timeout' if event == 'timeout' else 'ok')]
    [closed] = closed_records(world)
    assert closed.ref.attempt == 2 and held.own_unplaced == 0
    await worker.stop()


async def test_an_error_of_the_wait_between_two_reads_of_the_end_is_raised_and_the_job_stays_gated():
    world = World()
    gate = asyncio.Event()
    handler = SplittingHandler({500: [Item(n=0), gate]}, answer=summary())
    worker = OwnerWorker(world, handler)
    worker.hide_offsets_of('split-done')
    await worker.start(source_message(500))
    await wait_until(lambda: world.kinds() == ['open'] and len(world.parts()) == 1)
    await worker.caught_up()
    assert worker.owner is not None
    [consumer] = worker.tracker_consumers
    consumer.fail_next('get_watermark_offsets', KafkaException(KafkaError(KafkaError._TRANSPORT)))

    async def broken_sleep(seconds: float) -> None:
        raise RuntimeError('no timer')

    worker.owner._settings = dataclasses.replace(worker.owner._settings, sleep=broken_sleep)
    unread: list[dict] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _, context: unread.append(context))

    with capture_logs() as logs:
        gate.set()
        await wait_until(lambda: len(events(logs, 'split_owner_task_failed')) == 1)
        gc.collect()
        await asyncio.sleep(0.05)

    # The error ended the task that waited, and it was read there.
    [failed] = events(logs, 'split_owner_task_failed')
    assert 'no timer' in failed['error'] and unread == []
    held = worker.owner._jobs[500]
    assert held.own_unplaced == 1 and not held.catching_up and not worker.owner._table_has_own_records(held)

    # The next event of the job reads the end again, and the job goes on.
    worker.owner._settings = dataclasses.replace(worker.owner._settings, sleep=worker.sleep)
    world.report_all()
    await wait_until(lambda: worker.committed == 501)
    assert handler.split_calls == [500] and len(handler.jobs) == 1
    await worker.stop()


# --- the wait for the tracker ---


async def test_a_read_of_the_tracker_wakes_every_wait_for_the_table():
    world = World()
    worker = await OwnerWorker(world, SplittingHandler()).start()
    owner = worker.owner
    assert owner is not None
    held = owner_module._HeldJob(message=source_message(500), opened_at=0.0)
    loop = asyncio.get_running_loop()
    position = 5
    ended: list[str] = []

    def wait_past(name: str, offset: int) -> asyncio.Task:
        task = loop.create_task(owner._wait_for_table(held, lambda: position > offset, stop_grace=0.0))
        task.add_done_callback(lambda _: ended.append(name))
        return task

    def tracker_reads_on() -> None:
        nonlocal position
        position = 8
        owner.log_read()

    def second_wait_then_a_read() -> None:
        wait_past('second', 7)
        loop.call_soon(tracker_reads_on)

    first = wait_past('first', 10)
    await asyncio.sleep(0.01)
    # A read that passes neither offset wakes the first wait. While it is on its way back to its check,
    # a second wait starts, and the tracker reads past the offset of the second.
    loop.call_soon(lambda: loop.call_soon(second_wait_then_a_read))
    owner.log_read()

    await wait_until(lambda: ended == ['second'], timeout=0.5)
    assert not first.done()
    position = 11
    owner.log_read()
    await wait_until(lambda: ended == ['second', 'first'], timeout=0.5)
    await worker.stop()


# --- a job that fails in the partition loop ---


@pytest.mark.parametrize('failure', ['split_failed', 'attempts_spent'])
async def test_a_job_that_fails_does_not_hold_the_partition_loop_until_its_closed_record_is_read(failure):
    world = World()
    plan: dict = {501: items(1)}
    if failure == 'split_failed':
        plan[500] = [Item(n=0), RuntimeError('boom')]
    else:
        for attempt, attempt_id in enumerate((FIRST, SECOND, THIRD), start=1):
            world.put_open(500, attempt_id, attempt=attempt)
    handler = SplittingHandler(plan, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(max_attempts=3))
    counted = jobs_counted(failure)
    await worker.restore()
    released = worker.hold_tracker_reads()

    worker.run(source_message(500), source_message(501))
    # The loop goes on to the next message while the tracker has read nothing of the failure.
    await wait_until(lambda: 501 in handler.split_calls)
    assert [closed.body['outcome'] for closed in closed_records(world)] == [failure]
    assert worker.commits == [] and worker.pending == 2 and jobs_counted(failure) == counted
    assert worker.owner is not None and 500 in worker.owner._jobs

    released.set()
    await wait_until(lambda: worker.committed == 501)
    assert jobs_counted(failure) == counted + 1 and 500 not in worker.owner._jobs
    assert len(worker.dlq_records()) == 1 and len(closed_records(world)) == 1
    await worker.stop()


# --- a job the log has as closed ---


async def test_a_message_whose_job_the_log_has_as_closed_is_logged_when_its_offset_completes():
    world = World()
    world.put_open(500, FIRST, instance=NEW_INSTANCE)
    world.put_closed(500, FIRST, 'aggregated')
    handler = SplittingHandler({500: items(1)})

    with capture_logs() as logs:
        worker = await OwnerWorker(world, handler).start(source_message(500))
        await wait_until(lambda: worker.committed == 501)

    [line] = events(logs, 'split_job_already_closed')
    assert (line['log_level'], line['category'], line['partition']) == ('info', 'split', 3)
    assert (line['job'], line['outcome']) == ('3:500#1', 'aggregated')
    assert handler.split_calls == [] and handler.jobs == []
    await worker.stop()
