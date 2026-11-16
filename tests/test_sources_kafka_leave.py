"""KafkaSource: leaving the consumer group with unsubscribe() and joining it again on the same consumer."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from drakkar.metrics import backpressure_active, consumer_idle
from drakkar.sources import kafka as kafka_source_module
from drakkar.sources.kafka import KafkaSource, LeaveOutcome
from tests.test_sources_kafka import _source_with_fake_consumer

# Loop turns a test waits for a condition before it fails. With the idle
# sleep of the poll loop patched to zero, one poll takes a few turns.
MAX_TURNS = 5000


class FakePolls:
    """A consumer whose polls behave as librdkafka's do around unsubscribe and subscribe.

    The revoke after ``unsubscribe()`` and the assignment after ``subscribe()``
    run inside the next poll that starts after the call, on that poll's
    await, as the client runs its rebalance callbacks. A test can hold the
    next poll open with ``hold_next_poll``; the poll then decides at its start
    whether it runs the revoke, so a poll in flight at ``unsubscribe()`` runs
    none.
    """

    def __init__(self, source: KafkaSource) -> None:
        self.source = source
        self.unsubscribed = False
        self.revoke_ran = False
        self.assign_on_join: list[int] = []
        self.assign_due: list[int] | None = None
        self.subscribe_calls = 0
        self.unsubscribe_calls = 0
        self.polls_started = 0
        self.polls_completed = 0
        self._holds: list[asyncio.Event] = []
        self.held_polls = 0
        # When set, the next held poll raises this error once its gate opens,
        # which ends the poll loop.
        self.held_poll_error: Exception | None = None
        # When set, unsubscribe() waits until this event is set.
        self.unsubscribe_gate: asyncio.Event | None = None

    def hold_next_poll(self) -> asyncio.Event:
        """Make the next poll wait until the returned event is set."""
        gate = asyncio.Event()
        self._holds.append(gate)
        return gate

    async def subscribe(self) -> None:
        self.subscribe_calls += 1
        if self.subscribe_calls > 1:
            self.unsubscribed = False
            self.assign_due = list(self.assign_on_join)

    async def unsubscribe(self) -> None:
        self.unsubscribe_calls += 1
        if self.unsubscribe_gate is not None:
            await self.unsubscribe_gate.wait()
        self.unsubscribed = True
        self.revoke_ran = False

    async def poll_batch(self, *_args, **_kwargs) -> list:
        self.polls_started += 1
        revoke_due = self.unsubscribed and not self.revoke_ran
        assign_due, self.assign_due = self.assign_due, None
        if self._holds:
            gate = self._holds.pop(0)
            self.held_polls += 1
            await gate.wait()
            error, self.held_poll_error = self.held_poll_error, None
            if error is not None:
                raise error
        if revoke_due:
            self.revoke_ran = True
            await self.source.on_revoke(sorted(self.source.processors))
        if assign_due is not None:
            self.source.on_assign(assign_due)
        self.polls_completed += 1
        return []


def _processor() -> MagicMock:
    processor = MagicMock(queue_size=0, inflight_count=0, is_dead=False)
    processor.drain = AsyncMock()
    processor.stop = AsyncMock()
    processor.offset_tracker.committable.return_value = None
    return processor


async def _until(condition) -> None:
    for _ in range(MAX_TURNS):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError('the condition did not hold in time')


@pytest.fixture
def running(monkeypatch):
    """A started source with a fake consumer, its poll loop running, and the fake's poll script."""
    monkeypatch.setattr(kafka_source_module, 'POLL_IDLE_SLEEP', 0)
    source, fake = _source_with_fake_consumer(monkeypatch)
    polls = FakePolls(source)
    fake.subscribe = AsyncMock(side_effect=polls.subscribe)
    fake.unsubscribe = AsyncMock(side_effect=polls.unsubscribe)
    fake.poll_batch = AsyncMock(side_effect=polls.poll_batch)
    handler = source._context().handler
    handler.on_revoke = AsyncMock()
    handler.on_assign = AsyncMock()
    return source, fake, polls


async def _start(source: KafkaSource) -> asyncio.Task:
    await source.start()
    task = asyncio.create_task(source.run())
    await _until(lambda: source.is_ready)
    return task


async def _end(source: KafkaSource, run_task: asyncio.Task, *gates: asyncio.Event) -> None:
    source.signal_stop()
    for gate in gates:
        gate.set()
    await asyncio.wait_for(run_task, timeout=2)
    await asyncio.gather(*source.background_tasks, return_exceptions=True)


async def test_leave_group_unsubscribes_and_returns_left_after_the_revoke_poll(running):
    source, fake, polls = running
    run_task = await _start(source)
    source.processors[0] = _processor()
    source.processors[1] = _processor()

    outcome = await asyncio.wait_for(source.leave_group(), timeout=2)

    assert outcome is LeaveOutcome.LEFT
    fake.unsubscribe.assert_awaited_once()
    assert polls.revoke_ran is True
    assert source.processors == {}
    assert source.is_group_member is False
    assert source.is_ready is True
    await _end(source, run_task)


async def test_leave_group_does_not_return_on_a_poll_that_started_before_unsubscribe(running):
    source, _, polls = running
    run_task = await _start(source)
    in_flight = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)

    leave = asyncio.create_task(source.leave_group())
    await _until(lambda: polls.unsubscribe_calls == 1)
    next_poll = polls.hold_next_poll()
    in_flight.set()
    await _until(lambda: polls.held_polls == 2)

    assert polls.revoke_ran is False
    assert not leave.done()
    next_poll.set()
    assert await asyncio.wait_for(leave, timeout=2) is LeaveOutcome.LEFT
    await _end(source, run_task)


async def test_leave_group_with_no_partitions_returns_left(running):
    source, _, polls = running
    run_task = await _start(source)
    completed_before = polls.polls_completed

    outcome = await asyncio.wait_for(source.leave_group(), timeout=2)

    assert outcome is LeaveOutcome.LEFT
    assert polls.polls_completed > completed_before
    assert source.is_group_member is False
    await _end(source, run_task)


async def test_leave_group_returns_stopping_when_a_stop_is_requested_during_the_wait(running):
    source, fake, polls = running
    run_task = await _start(source)
    source.processors[0] = _processor()
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    leave = asyncio.create_task(source.leave_group())
    await _until(lambda: polls.unsubscribe_calls == 1)

    source.signal_stop()
    await asyncio.sleep(0)

    assert leave.done()
    assert leave.result() is LeaveOutcome.STOPPING
    assert fake.subscribe.await_count == 1
    await _end(source, run_task, gate)
    assert fake.subscribe.await_count == 1


async def test_leave_group_returns_stopping_at_once_after_a_stop(running):
    source, fake, _ = running
    run_task = await _start(source)
    source.signal_stop()

    assert await source.leave_group() is LeaveOutcome.STOPPING
    fake.unsubscribe.assert_not_awaited()
    assert source.is_group_member is True
    await _end(source, run_task)


async def test_leave_group_times_out_when_the_revoke_never_ends(running, monkeypatch):
    source, _, polls = running
    monkeypatch.setattr(kafka_source_module, 'LEAVE_MARGIN_SECONDS', 0.05)
    source._context().config.executor.drain_timeout = 0.05
    run_task = await _start(source)
    source.processors[0] = _processor()
    # The poll that would run the revoke never returns, as a revoke callback
    # that never ends holds the consumer's poll.
    revoke_poll = polls.hold_next_poll()
    loop = asyncio.get_running_loop()
    began = loop.time()

    outcome = await asyncio.wait_for(source.leave_group(), timeout=2)

    assert outcome is LeaveOutcome.TIMED_OUT
    assert loop.time() - began >= 0.1
    assert source.is_group_member is False
    await _end(source, run_task, revoke_poll)


async def test_leave_group_keeps_membership_when_unsubscribe_fails(running):
    source, fake, _ = running
    run_task = await _start(source)
    fake.unsubscribe = AsyncMock(side_effect=RuntimeError('broker gone'))

    with pytest.raises(RuntimeError, match='broker gone'):
        await source.leave_group()

    assert source.is_group_member is True
    await _end(source, run_task)


async def test_join_group_subscribes_and_waits_for_one_poll(running):
    source, fake, polls = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)

    join = asyncio.create_task(source.join_group())
    await _until(lambda: fake.subscribe.await_count == 2)
    # The held poll started before subscribe(), so it does not end the join.
    next_poll = polls.hold_next_poll()
    gate.set()
    await _until(lambda: polls.held_polls == 2)
    assert not join.done()
    assert source.is_group_member is False

    next_poll.set()
    assert await asyncio.wait_for(join, timeout=2) is True
    assert source.is_group_member is True
    await _end(source, run_task)


async def test_join_group_returns_false_after_a_stop_request(running):
    source, fake, _ = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    source.signal_stop()

    assert await source.join_group() is False
    assert fake.subscribe.await_count == 1
    assert source.is_group_member is False
    await _end(source, run_task)


async def test_join_group_returns_false_when_a_stop_comes_during_the_wait(running):
    source, fake, polls = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    join = asyncio.create_task(source.join_group())
    await _until(lambda: fake.subscribe.await_count == 2)

    source.signal_stop()

    assert await asyncio.wait_for(join, timeout=2) is False
    assert source.is_group_member is False
    await _end(source, run_task, gate)


async def test_idle_time_is_not_counted_while_not_a_member(running):
    source, _, polls = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    # Let the poll that completed the leave finish its turn before reading.
    started = polls.polls_completed
    await _until(lambda: polls.polls_completed >= started + 1)
    before = consumer_idle._value.get()

    started = polls.polls_completed
    await _until(lambda: polls.polls_completed >= started + 20)
    assert consumer_idle._value.get() == before

    assert await asyncio.wait_for(source.join_group(), timeout=2) is True
    started = polls.polls_completed
    await _until(lambda: polls.polls_completed >= started + 20)
    assert consumer_idle._value.get() > before
    await _end(source, run_task)


async def test_assignment_after_join_under_a_debug_pause_is_paused(running, monkeypatch):
    source, fake, polls = running
    monkeypatch.setattr(kafka_source_module, 'PartitionProcessor', lambda **_kwargs: _processor())
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    source._consume_pause.active = True
    polls.assign_on_join = [0, 1]

    assert await asyncio.wait_for(source.join_group(), timeout=2) is True
    await _until(lambda: sorted(source.processors) == [0, 1])
    await asyncio.gather(*source.background_tasks, return_exceptions=True)

    fake.pause.assert_awaited_with([0, 1])
    await _end(source, run_task)


async def test_revoke_of_the_last_partition_ends_the_backpressure_pause(running):
    source, _, _ = running
    await source.start()
    source.processors[0] = _processor()
    source.paused = True
    backpressure_active.set(1)

    await source.on_revoke([0])

    assert source.paused is False
    assert backpressure_active._value.get() == 0
    await asyncio.gather(*source.background_tasks, return_exceptions=True)


async def test_revoke_of_some_partitions_keeps_the_backpressure_pause(running):
    source, _, _ = running
    await source.start()
    source.processors[0] = _processor()
    source.processors[1] = _processor()
    source.paused = True
    backpressure_active.set(1)

    await source.on_revoke([0])

    assert source.paused is True
    assert backpressure_active._value.get() == 1
    await asyncio.gather(*source.background_tasks, return_exceptions=True)


async def test_leave_group_cancelled_during_unsubscribe_is_not_a_member(running):
    source, _, polls = running
    run_task = await _start(source)
    polls.unsubscribe_gate = asyncio.Event()
    leave = asyncio.create_task(source.leave_group())
    await _until(lambda: polls.unsubscribe_calls == 1)

    leave.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leave

    # The client call may still run on its thread, so membership is not known.
    assert source.is_group_member is False
    await _end(source, run_task, polls.unsubscribe_gate)


async def _end_after_failure(run_task: asyncio.Task) -> None:
    with pytest.raises(RuntimeError, match='poll failed'):
        await asyncio.wait_for(run_task, timeout=2)


async def test_leave_group_returns_stopping_when_the_poll_loop_ends_during_the_wait(running):
    source, fake, polls = running
    run_task = await _start(source)
    source.processors[0] = _processor()
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    leave = asyncio.create_task(source.leave_group())
    await _until(lambda: polls.unsubscribe_calls == 1)

    polls.held_poll_error = RuntimeError('poll failed')
    gate.set()

    assert await asyncio.wait_for(leave, timeout=2) is LeaveOutcome.STOPPING
    assert fake.subscribe.await_count == 1
    await _end_after_failure(run_task)


async def test_join_group_returns_false_when_the_poll_loop_ends_during_the_wait(running):
    source, fake, polls = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    join = asyncio.create_task(source.join_group())
    await _until(lambda: fake.subscribe.await_count == 2)

    polls.held_poll_error = RuntimeError('poll failed')
    gate.set()

    assert await asyncio.wait_for(join, timeout=2) is False
    assert source.is_group_member is False
    await _end_after_failure(run_task)


async def test_leave_and_join_answer_at_once_after_the_poll_loop_ended(running):
    source, fake, polls = running
    run_task = await _start(source)
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    polls.held_poll_error = RuntimeError('poll failed')
    gate.set()
    await _end_after_failure(run_task)

    assert await source.leave_group() is LeaveOutcome.STOPPING
    assert await source.join_group() is False
    fake.unsubscribe.assert_not_awaited()
    assert fake.subscribe.await_count == 1


async def test_join_group_returns_false_when_the_poll_loop_ends_on_a_stop_during_the_wait(running):
    source, fake, polls = running
    run_task = await _start(source)
    assert await asyncio.wait_for(source.leave_group(), timeout=2) is LeaveOutcome.LEFT
    gate = polls.hold_next_poll()
    await _until(lambda: polls.held_polls == 1)
    join = asyncio.create_task(source.join_group())
    await _until(lambda: fake.subscribe.await_count == 2)

    # The stop flag is set without signal_stop(), so only the end of run()
    # can wake the join.
    source._stop_requested = True
    gate.set()
    await asyncio.wait_for(run_task, timeout=2)

    assert await asyncio.wait_for(join, timeout=2) is False
