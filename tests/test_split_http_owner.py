"""The owner and HTTP split jobs: duplicates of a request id, and the ``running`` status records.

The tests drive a real partition processor with its real owner, job tracker
and producer over the fake Kafka clients (``HttpWorker``). The follower is a
stand-in; one of them is restored only when a test says so. The timer of the
``running`` records is a stand-in too, which a test fires.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable
from typing import Any

import pytest
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.split.http_jobs import running_record
from drakkar.split.http_status import PartCounts, StatusMap, StatusRecord
from drakkar.split.owner import DUPLICATE_CHECK_WAIT_SECONDS, RUNNING_MIN_INTERVAL_SECONDS
from tests.split_owner_helpers import PARTITION, SplittingHandler, World, items, source_message, summary, wait_until
from tests.test_split_http_end import (
    CLIENT,
    FIRST,
    HTTP_ID,
    STATUS_TOPIC,
    HttpWorker,
    RestoredFollower,
    complete_job,
    http_message,
)


@pytest.fixture(autouse=True)
def fast_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the wait of a coalesced commit: these tests wait for the commit of one offset."""
    monkeypatch.setattr(partition_module, 'COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)


class GatedFollower(RestoredFollower):
    """A follower whose map is restored only when the test calls :meth:`restore`."""

    def __init__(self, status_map: StatusMap) -> None:
        super().__init__(status_map)
        self.is_restored = False
        self.restored = asyncio.Event()
        self.waits = 0

    async def wait_restored(self, stop: asyncio.Event) -> bool:
        self.waits += 1
        if self.is_restored or stop.is_set():
            return self.is_restored
        restored = asyncio.ensure_future(self.restored.wait())
        stopped = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({restored, stopped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            restored.cancel()
            stopped.cancel()
        return self.is_restored

    def restore(self) -> None:
        self.is_restored = True
        self.restored.set()


class FakeTimers:
    """The timer of the owner: it records each delay and fires only when the test says so."""

    def __init__(self) -> None:
        self.scheduled: list[tuple[float, asyncio.TimerHandle, Callable[[], None]]] = []

    def __call__(self, delay: float, callback: Callable[[], None]) -> asyncio.TimerHandle:
        # A real handle, far in the future, so its ``cancel`` and ``cancelled`` are the loop's own.
        handle = asyncio.get_running_loop().call_later(3600, callback)
        self.scheduled.append((delay, handle, callback))
        return handle

    def pending(self) -> list[tuple[float, asyncio.TimerHandle, Callable[[], None]]]:
        return [entry for entry in self.scheduled if not entry[1].cancelled()]

    def fire(self) -> None:
        for _, handle, callback in self.pending():
            handle.cancel()
            callback()


def known(offset: int, state: Any = 'accepted', *, partition: int = PARTITION) -> StatusRecord:
    """A status record of ``HTTP_ID`` for the job at ``offset``."""
    return StatusRecord(
        request_id=HTTP_ID, state=state, partition=partition, offset=offset, client=CLIENT, updated_at=1.0
    )


def put(http: HttpWorker, record: StatusRecord) -> None:
    http.status_map.put(record, now=http.world.seconds())


def gate(http: HttpWorker) -> GatedFollower:
    """Give the owner of ``http`` a follower that is not restored yet; call before ``start``."""
    follower = GatedFollower(http.status_map)
    http.side = dataclasses.replace(http.side, follower=follower)  # ty: ignore[invalid-argument-type]
    return follower


async def start_timed(http: HttpWorker, *messages: Any) -> FakeTimers:
    """Start the worker with a stand-in timer for its owner."""
    await http.start(*messages)
    timers = FakeTimers()
    owner = http.worker.owner
    assert owner is not None
    # Set before the processor first runs: ``start`` returns with no loop turn after the owner is built.
    owner._timer = timers  # ty: ignore[invalid-assignment]
    return timers


def running(http: HttpWorker) -> list[PartCounts | None]:
    """The part counters of the ``running`` records, in order."""
    return [record.parts for record in http.statuses(running=True) if record.state == 'running']


def duplicates(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [line for line in logs if line['event'] == 'split_http_duplicate']


# --- duplicates ---


async def test_duplicate_of_lower_offset_in_map_completes_without_hook_or_status():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    http = HttpWorker(world, handler)
    put(http, known(400))
    with capture_logs() as logs:
        await http.start(http_message(500))
        await wait_until(lambda: http.worker.committed == 501)

    assert handler.split_calls == [] and handler.jobs == []
    assert http.statuses(running=True) == [] and world.kinds() == []
    [line] = duplicates(logs)
    assert (line['log_level'], line['category'], line['request_id']) == ('info', 'split', HTTP_ID)
    assert (line['job'], line['first_job']) == (f'{PARTITION}:500', f'{PARTITION}:400')
    await http.worker.stop()


async def test_same_window_duplicate_completes_without_split():
    world = World()
    handler = SplittingHandler({500: items(2), 501: items(2)}, answer=summary())
    http = HttpWorker(world, handler)
    with capture_logs() as logs:
        await http.start(http_message(500), http_message(501))
        await wait_until(lambda: ('complete', 501) in http.events)

    assert handler.split_calls == [500]
    [line] = duplicates(logs)
    assert (line['job'], line['first_job']) == (f'{PARTITION}:501', f'{PARTITION}:500')

    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 502)
    assert {record.job for record in http.statuses(running=True)} == {f'{PARTITION}:500'}
    await http.worker.stop()


async def test_redelivered_first_message_is_the_same_job():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    put(http, known(500))
    with capture_logs() as logs:
        await http.start(http_message(500))
        await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    assert handler.split_calls == [500] and duplicates(logs) == []
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)
    await http.worker.stop()


async def test_redelivered_message_of_a_begun_id_is_not_a_duplicate():
    world = World()
    http = await HttpWorker(world, SplittingHandler()).start()
    owner = http.worker.owner
    assert owner is not None
    owner._begun_ids[HTTP_ID] = 500

    assert owner._first_offset_of(HTTP_ID, 500) is None
    assert owner._first_offset_of(HTTP_ID, 501) == 500
    await http.worker.stop()


async def test_id_reused_after_its_first_job_ended_runs_as_a_new_job():
    world = World()
    handler = SplittingHandler({500: items(1), 900: items(1)}, answer=summary())
    http = await HttpWorker(world, handler).start(http_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    # The status entry of the first job has expired from the map; the client sends the id again.
    with capture_logs() as logs:
        http.worker.give(http_message(900))
        await wait_until(lambda: len(world.parts(offset=900)) == 1)
    world.report_all(world.parts(offset=900))
    await wait_until(lambda: http.worker.committed == 901)

    assert handler.split_calls == [500, 900] and duplicates(logs) == []
    assert [closed['outcome'] for closed in world.closed()] == ['aggregated', 'aggregated']
    await http.worker.stop()


async def test_a_redelivered_message_of_a_closed_job_does_not_keep_its_id():
    world = World()
    world.put_open(500, FIRST)
    world.put_closed(500, FIRST)
    handler = SplittingHandler({900: items(1)}, answer=summary())
    http = await HttpWorker(world, handler).start(http_message(500))
    await wait_until(lambda: http.worker.committed == 501)
    owner = http.worker.owner
    assert owner is not None
    assert owner._begun_ids == {}

    # The client sends the id again later: it is a new job, not a duplicate of the closed one.
    with capture_logs() as logs:
        http.worker.give(http_message(900))
        await wait_until(lambda: len(world.parts(offset=900)) == 1)
    assert handler.split_calls == [900] and duplicates(logs) == []
    await http.worker.stop()


async def test_a_job_the_log_has_is_followed_even_when_the_map_names_a_lower_offset():
    world = World()
    complete_job(world, 500)
    handler = SplittingHandler(answer=summary())
    http = HttpWorker(world, handler)
    # An ended entry of an older job of the same id that the sweep has not removed yet.
    put(http, dataclasses.replace(known(400, 'done'), updated_at=world.seconds()))
    assert http.status_map.get(HTTP_ID) is not None
    with capture_logs() as logs:
        await http.start(http_message(500))
        await wait_until(lambda: http.worker.committed == 501)

    assert duplicates(logs) == []
    assert len(handler.jobs) == 1 and world.kinds()[-1] == 'closed'
    await http.worker.stop()


async def test_a_duplicate_closes_the_orphans_below_it():
    world = World()
    complete_job(world, 400)
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    put(http, known(300))
    with capture_logs() as logs:
        await http.start(http_message(500))
        await wait_until(lambda: http.worker.committed == 501)
        await wait_until(lambda: 'closed' in world.kinds())

    assert len(duplicates(logs)) == 1
    assert world.closed()[0]['outcome'] == 'orphaned'
    await http.worker.stop()


async def test_map_entry_of_another_partition_is_no_duplicate():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    put(http, known(400, partition=PARTITION + 1))
    await http.start(http_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])

    assert handler.split_calls == [500]
    await http.worker.stop()


class LaggingFollower(RestoredFollower):
    """A restored follower that reads a late record while the owner waits for it to catch up."""

    def __init__(self, status_map: StatusMap, *, late: StatusRecord | None = None, answer: bool = True) -> None:
        super().__init__(status_map)
        self.late = late
        self.answer = answer
        self.timeouts: list[float] = []
        self.release = asyncio.Event()

    async def caught_up(self, timeout: float) -> bool:
        self.timeouts.append(timeout)
        if self.late is not None:
            self.status_map.put(self.late, now=self.late.updated_at)
            return self.answer
        await self.release.wait()
        return self.answer


def lagging(http: HttpWorker, **options: Any) -> LaggingFollower:
    """Give the owner of ``http`` a follower that catches up as the test says; call before ``start``."""
    follower = LaggingFollower(http.status_map, **options)
    http.side = dataclasses.replace(http.side, follower=follower)  # ty: ignore[invalid-argument-type]
    return follower


async def test_duplicate_whose_first_status_the_follower_reads_late_is_caught():
    world = World()
    handler = SplittingHandler({500: items(2)}, answer=summary())
    http = HttpWorker(world, handler)
    follower = lagging(http, late=known(400))
    with capture_logs() as logs:
        await http.start(http_message(500))
        await wait_until(lambda: http.worker.committed == 501)

    assert follower.timeouts == [DUPLICATE_CHECK_WAIT_SECONDS] == [5.0]
    assert handler.split_calls == []
    assert len(duplicates(logs)) == 1
    await http.worker.stop()


async def test_begin_goes_on_when_the_follower_does_not_catch_up():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    follower = lagging(http, answer=False)
    await http.start(http_message(500))
    await asyncio.sleep(0.05)
    assert handler.split_calls == []
    follower.release.set()
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    assert handler.split_calls == [500]
    await http.worker.stop()


async def test_a_stop_ends_the_wait_for_the_follower_and_holds_the_offset():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    follower = lagging(http)
    await http.start(http_message(500))
    await wait_until(lambda: follower.timeouts == [DUPLICATE_CHECK_WAIT_SECONDS])

    http.worker.processor.signal_stop()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)

    assert handler.split_calls == [] and world.kinds() == []
    assert http.worker.commits == [] and http.worker.processor._open_job_offsets == {500}
    await http.worker.stop()


async def test_begin_waits_for_restore_and_a_stop_ends_the_wait():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    follower = gate(http)
    await http.start(http_message(500))
    await wait_until(lambda: follower.waits == 1)
    await asyncio.sleep(0.05)
    assert handler.split_calls == []

    http.worker.processor.signal_stop()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)

    assert handler.split_calls == [] and world.kinds() == []
    assert http.worker.commits == [] and http.worker.pending == 1
    assert http.worker.processor._open_job_offsets == {500}
    await http.worker.stop()


async def test_begin_goes_on_once_the_map_is_restored():
    world = World()
    handler = SplittingHandler({500: items(1)}, answer=summary())
    http = HttpWorker(world, handler)
    follower = gate(http)
    await http.start(http_message(500))
    await wait_until(lambda: follower.waits == 1)

    follower.restore()
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: http.worker.committed == 501)

    assert handler.split_calls == [500]
    await http.worker.stop()


async def test_kafka_message_does_not_wait_for_restore():
    world = World()
    http = HttpWorker(world, SplittingHandler(answer=summary()))
    follower = gate(http)
    await http.start(source_message(500))
    await wait_until(lambda: http.worker.committed == 501)

    assert follower.waits == 0
    await http.worker.stop()


# --- running records ---


async def test_running_written_on_open_with_null_total():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(2)}, answer=summary()))
    await start_timed(http, http_message(500))
    await wait_until(lambda: len(running(http)) == 2)

    [first, _] = http.statuses(running=True)
    assert (first.state, first.job, first.client) == ('running', f'{PARTITION}:500', CLIENT)
    assert first.parts == PartCounts(total=None, received=0, failed=0)
    await http.worker.stop()


async def test_running_written_at_once_when_total_known():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(2)}, answer=summary()))
    timers = await start_timed(http, http_message(500))
    await wait_until(lambda: len(running(http)) == 2)

    assert running(http) == [PartCounts(None, 0, 0), PartCounts(2, 0, 0)]
    assert timers.scheduled == []
    await http.worker.stop()


async def test_running_written_at_most_once_per_5_seconds_and_the_last_change_is_written():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(3)}, answer=summary()))
    timers = await start_timed(http, http_message(500))
    await wait_until(lambda: len(running(http)) == 2)
    parts = world.parts()

    world.clock.advance(2000)
    world.report(parts[0])
    await wait_until(lambda: len(timers.pending()) == 1)
    world.report(parts[1])
    await http.worker.caught_up()

    [(delay, _, _)] = timers.pending()
    assert delay == RUNNING_MIN_INTERVAL_SECONDS - 2
    assert len(running(http)) == 2

    timers.fire()
    await wait_until(lambda: len(running(http)) == 3)
    assert running(http)[-1] == PartCounts(3, 2, 0)

    world.report(parts[2])
    await wait_until(lambda: http.worker.committed == 501)
    assert [record.state for record in http.statuses(running=True)][-1] == 'done'
    await http.worker.stop()


async def test_no_running_after_end_state():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(2)}, answer=summary()))
    timers = await start_timed(http, http_message(500))
    await wait_until(lambda: len(running(http)) == 2)
    parts = world.parts()
    world.report(parts[0])
    await wait_until(lambda: len(timers.pending()) == 1)
    [(_, handle, callback)] = timers.pending()

    world.report(parts[1])
    await wait_until(lambda: http.worker.committed == 501)
    assert handle.cancelled()
    # A timer that fired anyway finds no held job and writes nothing.
    callback()
    await asyncio.sleep(0.05)

    states = [record.state for record in http.statuses(running=True)]
    assert states[-1] == 'done' and states.count('done') == 1
    assert 'running' not in states[states.index('done') :]
    await http.worker.stop()


async def test_stop_cancels_the_running_timer():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(3)}, answer=summary()))
    timers = await start_timed(http, http_message(500))
    await wait_until(lambda: len(running(http)) == 2)
    world.report(world.parts()[0])
    await wait_until(lambda: len(timers.pending()) == 1)
    [(_, handle, _)] = timers.pending()

    http.worker.processor.signal_stop()

    assert handle.cancelled()
    await asyncio.wait_for(http.worker.processor.drain(), timeout=10)
    await http.worker.stop()


async def test_running_write_failure_is_logged_and_dropped():
    world = World()
    http = HttpWorker(world, SplittingHandler({500: items(1)}, answer=summary()))
    producer = http.worker.runtime.producer
    enqueue = producer.enqueue_nowait

    def full_for_status(topic: str, record: Any) -> Any:
        return None if topic == STATUS_TOPIC else enqueue(topic, record)

    producer.enqueue_nowait = full_for_status  # ty: ignore[invalid-assignment]
    with capture_logs() as logs:
        await start_timed(http, http_message(500))
        await wait_until(lambda: world.kinds() == ['open', 'split-done'])
        world.report_all()
        await wait_until(lambda: http.worker.committed == 501)

    assert [record.state for record in http.statuses(running=True)] == ['done']
    failed = [line for line in logs if line['event'] == 'split_http_status_write_failed']
    assert failed and failed[0]['state'] == 'running'
    assert failed[0]['error'] == 'the client queue is full'
    await http.worker.stop()


def test_running_record_needs_an_http_job_message():
    with pytest.raises(ValueError, match='HTTP job'):
        running_record(source_message(500), parts=PartCounts(None, 0, 0), now=1.0)


async def test_no_running_after_an_end_state_of_an_attempt_that_another_instance_replaced():
    world = World()
    gate_answer = asyncio.Event()

    async def answer(job: Any) -> Any:
        await gate_answer.wait()
        return summary(job.attempt)

    http = HttpWorker(world, SplittingHandler({500: items(2)}, answer=answer))
    await start_timed(http, http_message(500))
    await wait_until(lambda: world.kinds() == ['open', 'split-done'])
    world.report_all()
    await wait_until(lambda: len(http.worker.handler.jobs) == 1)

    # While the hook runs, another instance opens attempt 2: the done of this end is ignored by the log.
    world.put_open(500, 'e' * 16, attempt=2, instance='b' * 16)
    await http.worker.caught_up()
    with capture_logs() as logs:
        gate_answer.set()
        await wait_until(lambda: any(line['event'] == 'split_job_end_superseded' for line in logs))

    # The owner follows attempt 2, whose counters change; it writes no running record after its done.
    world.put(world.ref(500, 'e' * 16, 2), 'split-done', {'parts': 2})
    await http.worker.caught_up()
    await asyncio.sleep(0.05)
    states = [record.state for record in http.statuses(running=True)]
    assert 'done' in states and 'running' not in states[states.index('done') :]
    await http.worker.stop()
