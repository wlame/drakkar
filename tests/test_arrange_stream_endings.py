"""How a stream ends: an error of the generator, a DLQ outcome, a stop or a revoke.

A stream that ended drains as a list message does. A stream that is still
open at a stop is abandoned: its generator is closed, its tasks are
cancelled, and its offset stays pending without holding the drain.
"""

import asyncio
import contextvars
import time
from unittest.mock import AsyncMock

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar import partition as partition_module
from drakkar.budget import PendingBudget
from drakkar.models import CollectResult, KafkaPayload, SinkDeliveryFailedError
from tests.conftest import wait_for
from tests.stream_helpers import (
    Out,
    RecordingCollect,
    RecordingStreamHandler,
    WithGroupHook,
    make_msg,
    run_until_settled,
    stream_processor,
)

# Far below DEFAULT_STOP_TIMEOUT: a stop that waits for an abandoned stream
# would take the whole timeout.
QUICK_STOP_SECONDS = 2.0


def events(cap: list[dict], name: str) -> list[dict]:
    """The captured log lines named ``name``."""
    return [entry for entry in cap if entry.get('event') == name]


def block_offsets(proc, offsets: set[int], release: asyncio.Event) -> None:
    """Make the tasks of the messages at ``offsets`` wait in the pool until ``release`` is set."""
    execute = proc._executor_pool.execute

    async def blocking_execute(task, recorder, partition_id):
        if set(task.source_offsets) & offsets:
            await release.wait()
        return await execute(task, recorder, partition_id)

    proc._executor_pool.execute = blocking_execute


class OpenAfterFirstGroup(RecordingStreamHandler):
    """Yields one group, then waits in a pull until ``resume`` is set; notes when its ``finally`` runs."""

    def __init__(self, sizes: list[int] | None = None, **kwargs) -> None:
        super().__init__(sizes or [1], **kwargs)
        self.resume = asyncio.Event()
        self.pulls = 0
        self.finally_ran = False

    async def arrange_stream(self, message):
        try:
            self.pulls += 1
            yield self.make_group(message, 0, self.sizes[0])
            self.pulls += 1
            await self.resume.wait()
            yield self.make_group(message, 1, 1)
        finally:
            self.finally_ran = True


class OpenAfterFirstGroupWithHook(OpenAfterFirstGroup, WithGroupHook):
    """``OpenAfterFirstGroup`` that also overrides ``on_group_complete``."""


async def test_generator_error_keeps_started_groups_running_and_reaches_the_supervisor():
    class FailsAfterFirstGroup(WithGroupHook):
        def __init__(self) -> None:
            super().__init__([1])
            self.release = asyncio.Event()

        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            raise ValueError('side api down')

        async def on_task_complete(self, result):
            await self.release.wait()
            return await super().on_task_complete(result)

    handler = FailsAfterFirstGroup()
    proc = stream_processor(handler, on_collect=RecordingCollect(handler.log))
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: events(cap, 'partition_processor_restarting'))
        handler.release.set()
        await wait_for(lambda: 'deliver:group-0' in handler.log)
        await wait_for(lambda: proc.inflight_count == 0)

    assert handler.log.count('group:0:0') == 1
    assert handler.message_groups == []
    assert proc.offset_tracker.has_pending()
    assert 0 in proc._abandoned_offsets
    failed = events(cap, 'arrange_stream_failed')
    assert len(failed) == 1
    assert failed[0]['error'] == 'side api down'
    assert failed[0]['group_index'] == 1
    assert failed[0]['offset'] == 0
    assert events(cap, 'partition_processor_restarting')[0]['error'] == 'side api down'
    await proc.stop()


async def test_offset_of_a_failed_stream_does_not_hold_the_drain():
    class FailsOnFirstMessage(RecordingStreamHandler):
        async def arrange_stream(self, message):
            if message.offset == 0:
                raise RuntimeError('broken stream')
            yield self.make_group(message, 0, 1)

    handler = FailsOnFirstMessage()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.enqueue(make_msg(1))
    proc.start()
    await wait_for(lambda: len(handler.message_groups) == 1 and proc.inflight_count == 0)

    assert proc.offset_tracker.pending_count == 1
    assert not proc._has_drainable_pending()
    await asyncio.wait_for(proc.drain(), timeout=QUICK_STOP_SECONDS)
    await proc.stop()


class DLQOnDelivery(RecordingCollect):
    """An ``on_collect`` double whose delivery of a task result writes the message's DLQ record."""

    async def __call__(self, result, partition_id, message_context=None):
        # The outcome is set before the delivery is logged, so a test that
        # waits for the log line sees the outcome too.
        if any(payload.data.v.startswith('task-') for payload in result.kafka):
            message_context.states[0].dlq_written = True
        return await super().__call__(result, partition_id, message_context)


class TaskResultsHandler(OpenAfterFirstGroupWithHook):
    """Returns a result from every task, so each task result is delivered."""

    async def on_task_complete(self, result):
        await super().on_task_complete(result)
        return CollectResult(kafka=[KafkaPayload(data=Out(v=f'task-{result.task.task_id}'))])


async def test_dlq_outcome_closes_the_stream_and_still_fires_the_message_hook():
    handler = TaskResultsHandler()
    proc = stream_processor(handler, on_collect=DLQOnDelivery(handler.log))
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.message_groups and not proc.offset_tracker.has_pending())

    assert handler.pulls == 2
    assert handler.finally_ran
    assert 'pull:0:1' not in handler.log and 'task:m0-g1-t0' not in handler.log
    assert handler.log.count('group:0:0') == 1
    assert handler.log.count('message:0') == 1
    assert proc._abandoned_offsets == set()
    await proc.stop()


async def test_group_pulled_after_a_dlq_outcome_is_not_started():
    class YieldsAfterTheOutcome(TaskResultsHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            # The outcome is set before the next group comes back.
            await wait_for(lambda: 'deliver:task-m0-g0-t0' in self.log)
            yield self.make_group(message, 1, 2)

    handler = YieldsAfterTheOutcome()
    proc = stream_processor(handler, on_collect=DLQOnDelivery(handler.log))
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.message_groups and not proc.offset_tracker.has_pending())

    assert not any(entry.startswith('task:m0-g1') for entry in handler.log)
    assert [group.index for group in handler.message_groups[0].groups] == [0]
    assert handler.message_groups[0].total == 1
    await proc.stop()


async def test_stop_during_a_pull_cancels_the_pull_and_runs_the_generator_finally():
    handler = OpenAfterFirstGroup()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.pulls == 2 and proc.inflight_count == 0)

    started = time.monotonic()
    proc.signal_stop()
    await proc.stop()

    assert time.monotonic() - started < QUICK_STOP_SECONDS
    assert handler.finally_ran
    assert proc._pull_task is None
    assert handler.message_groups == []
    assert proc.offset_tracker.has_pending()


async def test_signal_stop_after_the_stream_ended_cancels_nothing():
    handler = RecordingStreamHandler([1])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.message_groups)
    assert proc._pull_task is None
    proc.signal_stop()
    await proc.stop()
    assert not proc.offset_tracker.has_pending()


async def test_stop_while_waiting_for_room_abandons_the_stream_at_once():
    budget = PendingBudget(max_tasks=2, max_bytes=1 << 40)
    handler = RecordingStreamHandler([2, 2, 2])
    proc = stream_processor(handler, budget=budget)
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: budget.waiting_loops == 1 and proc.inflight_count == 2)
    assert proc._pull_task is None

    started = time.monotonic()
    proc.signal_stop()
    await wait_for(lambda: proc._loop_ended)

    assert time.monotonic() - started < QUICK_STOP_SECONDS
    # Read before stop() closes the account, so a leak cannot hide.
    assert budget.tasks == 0 and budget.bytes == 0 and budget.reserved_tasks == 0
    assert budget.waiting_loops == 0
    assert handler.log == ['pull:0:0']
    assert proc.offset_tracker.has_pending()
    await proc.stop()


async def test_stop_with_an_open_stream_cancels_its_tasks_and_gives_back_the_budget():
    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)
    handler = OpenAfterFirstGroupWithHook([3])
    proc = stream_processor(handler, budget=budget)
    release = asyncio.Event()
    execute = proc._executor_pool.execute

    async def first_task_settles(task, recorder, partition_id):
        # One task finishes, so the group keeps its output; the others wait.
        if not task.task_id.endswith('t0'):
            await release.wait()
        return await execute(task, recorder, partition_id)

    proc._executor_pool.execute = first_task_settles
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: handler.pulls == 2 and proc.inflight_count == 2 and budget.bytes > 0)

        proc.signal_stop()
        await wait_for(lambda: proc._loop_ended)

    assert proc.inflight_count == 0
    assert budget.tasks == 0 and budget.bytes == 0
    assert handler.group_calls == []
    assert handler.message_groups == []
    assert proc.offset_tracker.has_pending()
    assert proc._stream_runs == {}
    abandoned = events(cap, 'arrange_stream_abandoned')
    assert len(abandoned) == 1 and abandoned[0]['cancelled'] == 2
    await proc.stop()


async def test_stop_after_the_stream_ended_drains_the_message_as_before():
    handler = WithGroupHook([2])
    on_commit = AsyncMock()
    proc = stream_processor(handler, on_commit=on_commit)
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: proc.inflight_count == 2 and not proc._message_trackers[0].stream_open)

    proc.signal_stop()
    draining = asyncio.create_task(proc.drain())
    await asyncio.sleep(0.05)
    assert not draining.done()
    release.set()
    await asyncio.wait_for(draining, timeout=QUICK_STOP_SECONDS)

    assert handler.log.count('message:0') == 1
    assert len(handler.group_calls) == 1
    assert proc._abandoned_offsets == set()
    on_commit.assert_awaited_with(0, 1)
    await proc.stop()


async def test_drain_does_not_wait_for_an_abandoned_stream():
    class EndsThenStaysOpen(OpenAfterFirstGroup):
        async def arrange_stream(self, message):
            if message.offset == 0:
                yield self.make_group(message, 0, 1)
                return
            async for group in super().arrange_stream(message):
                yield group

    handler = EndsThenStaysOpen()
    on_commit = AsyncMock()
    proc = stream_processor(handler, on_commit=on_commit)
    release = asyncio.Event()
    block_offsets(proc, {0, 1}, release)
    proc.enqueue(make_msg(0))
    proc.enqueue(make_msg(1))
    proc.start()
    await wait_for(lambda: handler.pulls == 2 and proc.inflight_count == 2)

    proc.signal_stop()
    draining = asyncio.create_task(proc.drain())
    # Only the tasks of the abandoned message are cancelled.
    await wait_for(lambda: proc.inflight_count == 1)
    assert not draining.done()
    release.set()
    started = time.monotonic()
    await asyncio.wait_for(draining, timeout=QUICK_STOP_SECONDS)

    assert time.monotonic() - started < QUICK_STOP_SECONDS / 2
    assert handler.log.count('message:0') == 1
    assert 'message:1' not in handler.log
    assert proc._abandoned_offsets == {1}
    on_commit.assert_awaited_with(0, 1)
    await proc.stop()


async def test_generator_that_ignores_the_close_does_not_hang_the_stop():
    class Stubborn(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass
            ignored = False
            while True:
                try:
                    yield self.make_group(message, 1, 1)
                except GeneratorExit:
                    # Ignores the framework's close only, so the finalizer
                    # can close it whenever it runs.
                    if ignored:
                        raise
                    ignored = True

    handler = Stubborn()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: proc.inflight_count == 0 and handler.log.count('task:m0-g0-t0') == 1)
        await asyncio.sleep(0.01)
        started = time.monotonic()
        await proc.stop()

    assert time.monotonic() - started < QUICK_STOP_SECONDS
    close_failed = events(cap, 'arrange_stream_close_failed')
    assert len(close_failed) == 1
    assert 'GeneratorExit' in close_failed[0]['error']
    assert 'task:m0-g1-t0' not in handler.log


async def test_pull_that_ignores_the_cancel_is_left_at_the_close_time_bound(monkeypatch):
    monkeypatch.setattr(partition_module, 'STREAM_CLOSE_TIMEOUT', 0.05)
    keep_waiting = asyncio.Event()

    class IgnoresCancel(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            while not keep_waiting.is_set():
                try:
                    await keep_waiting.wait()
                except asyncio.CancelledError:
                    continue
            yield []

    handler = IgnoresCancel()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: proc.inflight_count == 0 and handler.log.count('task:m0-g0-t0') == 1)
        await asyncio.sleep(0.01)
        started = time.monotonic()
        await proc.stop()

    assert time.monotonic() - started < QUICK_STOP_SECONDS
    close_failed = events(cap, 'arrange_stream_close_failed')
    assert [entry['error'] for entry in close_failed] == ['timed out']
    assert proc.offset_tracker.has_pending()
    # Let the pull end so no task outlives the test.
    keep_waiting.set()
    await asyncio.sleep(0.01)


async def test_close_of_an_iterator_without_aclose_and_a_raising_close_are_handled():
    class Iterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    proc = stream_processor(RecordingStreamHandler())
    tracker = partition_module.MessageTracker(source_message=make_msg(0), stream_open=True)
    with capture_logs() as cap:
        await proc._close_stream(Iterator(), tracker)

        class RaisingClose(Iterator):
            async def aclose(self):
                raise OSError('close broke')

        await proc._close_stream(RaisingClose(), tracker)
    assert [entry['error'] for entry in events(cap, 'arrange_stream_close_failed')] == ['close broke']


async def test_pull_that_raises_after_its_cancel_is_logged_as_a_close_failure():
    class RaisesOnCancel(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise RuntimeError('cleanup failed') from None

    handler = RaisesOnCancel()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: proc.inflight_count == 0 and handler.log.count('task:m0-g0-t0') == 1)
        await asyncio.sleep(0.01)
        await proc.stop()
    assert [entry['error'] for entry in events(cap, 'arrange_stream_close_failed')] == ['cleanup failed']


async def test_close_that_hangs_in_the_generator_finally_is_left_at_the_time_bound(monkeypatch):
    monkeypatch.setattr(partition_module, 'STREAM_CLOSE_TIMEOUT', 0.05)

    async def slow_cleanup():
        try:
            yield []
        finally:
            await asyncio.sleep(10)

    stream = slow_cleanup()
    assert await anext(stream) == []
    proc = stream_processor(RecordingStreamHandler())
    tracker = partition_module.MessageTracker(source_message=make_msg(0), stream_open=True)
    with capture_logs() as cap:
        started = time.monotonic()
        await proc._close_stream(stream, tracker)
    assert time.monotonic() - started < QUICK_STOP_SECONDS
    assert [entry['error'] for entry in events(cap, 'arrange_stream_close_failed')] == ['timed out']


async def test_a_cancelled_loop_cancels_the_pull_in_progress():
    handler = OpenAfterFirstGroup()
    proc = stream_processor(handler)
    streaming = asyncio.create_task(proc._process_stream_message(make_msg(0)))
    await wait_for(lambda: handler.pulls == 2 and proc._pull_task is not None)
    pull = proc._pull_task
    streaming.cancel()
    await asyncio.gather(streaming, return_exceptions=True)
    await asyncio.sleep(0)
    assert pull.cancelled()
    assert proc._pull_task is None
    assert handler.finally_ran
    await wait_for(lambda: proc.inflight_count == 0)


async def test_tasks_of_an_abandoned_stream_are_cancelled_before_its_slow_close():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 40)
    closing = asyncio.Event()

    class SlowClose(WithGroupHook):
        async def arrange_stream(self, message):
            try:
                yield self.make_group(message, 0, 1)
                yield self.make_group(message, 1, 1)
            finally:
                closing.set()
                await asyncio.sleep(0.2)

        async def on_task_complete(self, result):
            await super().on_task_complete(result)
            return CollectResult(kafka=[KafkaPayload(data=Out(v=f'task-{result.task.task_id}'))])

    handler = SlowClose()
    proc = stream_processor(handler, budget=budget, on_collect=RecordingCollect(handler.log))
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    proc.enqueue(make_msg(0))
    proc.start()
    # The generator waits at its first yield; the stream waits for room.
    await wait_for(lambda: budget.waiting_loops == 1 and proc.inflight_count == 1)

    proc.signal_stop()
    await asyncio.wait_for(closing.wait(), timeout=QUICK_STOP_SECONDS)
    # The task could end now, while the close is slow.
    release.set()
    await wait_for(lambda: proc._loop_ended)

    assert not any(entry.startswith(('task:', 'deliver:', 'group:', 'message:')) for entry in handler.log)
    assert budget.tasks == 0 and budget.bytes == 0
    await proc.stop()


async def test_a_context_variable_token_taken_before_a_yield_resets_after_it():
    marker: contextvars.ContextVar[str] = contextvars.ContextVar('marker', default='')
    seen: list[str] = []

    class HoldsAToken(RecordingStreamHandler):
        async def arrange_stream(self, message):
            token = marker.set('held')
            yield self.make_group(message, 0, 1)
            seen.append(marker.get())
            yield self.make_group(message, 1, 1)
            seen.append(marker.get())
            marker.reset(token)

    handler = HoldsAToken()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        await run_until_settled(proc)
    assert seen == ['held', 'held']
    assert len(handler.message_groups) == 1
    assert events(cap, 'arrange_stream_failed') == []
    assert marker.get() == ''


async def test_bound_contextvars_across_yields_completes_the_stream():
    class BindsAcrossYields(RecordingStreamHandler):
        async def arrange_stream(self, message):
            with structlog.contextvars.bound_contextvars(job='nightly'):
                yield self.make_group(message, 0, 1)
                self.log.append(f'job:{structlog.contextvars.get_contextvars().get("job")}')
                yield self.make_group(message, 1, 1)

    handler = BindsAcrossYields()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        await run_until_settled(proc)
    assert 'job:nightly' in handler.log
    assert len(handler.message_groups) == 1
    assert events(cap, 'arrange_stream_failed') == []
    assert 'job' not in structlog.contextvars.get_contextvars()


async def test_stop_between_a_fan_out_and_the_next_admission_abandons_the_stream():
    handler = RecordingStreamHandler([2, 2])
    proc = stream_processor(handler)
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    note_shape = proc._note_window_shape

    def stop_after_the_fan_out(message_count, task_count, stdin_bytes):
        # Called right after a group's fan-out, before the next admission.
        note_shape(message_count, task_count, stdin_bytes)
        proc.signal_stop()

    proc._note_window_shape = stop_after_the_fan_out
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: proc._loop_ended)

    assert handler.log == ['pull:0:0']
    assert proc.inflight_count == 0
    assert proc._abandoned_offsets == {0}
    assert proc.offset_tracker.has_pending()
    await proc.stop()


async def test_a_failed_stream_with_tasks_still_running_does_not_hold_the_stop():
    class FailsWithTasksRunning(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 3)
            raise RuntimeError('side api down')

    handler = FailsWithTasksRunning()
    proc = stream_processor(handler)
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: events(cap, 'partition_processor_restarting') and proc.inflight_count == 3)
        started = time.monotonic()
        await proc.stop()

    assert time.monotonic() - started < QUICK_STOP_SECONDS
    assert proc.inflight_count == 0
    assert not any(entry.startswith('task:') for entry in handler.log)
    [cancelled] = events(cap, 'arrange_stream_tasks_cancelled')
    assert cancelled['cancelled'] == 3 and cancelled['offsets'] == [0]


async def test_group_index_of_a_failed_group_start_names_that_group():
    handler = RecordingStreamHandler([1, 2])
    proc = stream_processor(handler)
    start_group = proc._start_group

    async def start_group_then_fail(tracker, tasks, window):
        await start_group(tracker, tasks, window)
        if len(tracker.groups) == 2:
            raise RuntimeError('fan-out broke')

    proc._start_group = start_group_then_fail
    with capture_logs() as cap, pytest.raises(RuntimeError, match='fan-out broke'):
        await proc._process_stream_message(make_msg(0))
    [failed] = events(cap, 'arrange_stream_failed')
    assert failed['group_index'] == 1
    await wait_for(lambda: proc.inflight_count == 0)


async def test_cancelled_group_hook_gives_back_the_kept_bytes():
    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)
    hook_entered = asyncio.Event()

    class SlowGroupHook(OpenAfterFirstGroupWithHook):
        async def on_group_complete(self, group, results, errors):
            hook_entered.set()
            await asyncio.Event().wait()

    handler = SlowGroupHook([2])
    proc = stream_processor(handler, budget=budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await asyncio.wait_for(hook_entered.wait(), timeout=QUICK_STOP_SECONDS)
    assert budget.bytes > 0

    proc.signal_stop()
    await wait_for(lambda: proc._loop_ended)

    assert budget.tasks == 0 and budget.bytes == 0
    assert proc._message_trackers[0].open_windows == {}
    await proc.stop()


async def test_dlq_outcome_while_waiting_gives_up_the_room_and_pulls_nothing():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 40)
    budget.charge(1, 0)
    proc = stream_processor(RecordingStreamHandler(), budget=budget)
    proc._running = True
    tracker = partition_module.MessageTracker(source_message=make_msg(0), stream_open=True)

    # The outcome comes while the stream waits and the budget stays full.
    waiting = asyncio.create_task(proc._admit_next_group(tracker))
    await wait_for(lambda: budget.waiting_loops == 1)
    tracker.dlq.dlq_written = True
    assert await asyncio.wait_for(waiting, timeout=QUICK_STOP_SECONDS) is False
    assert proc._room_waiter is None and budget.waiting_loops == 0

    # The outcome comes while the stream waits, and then the room opens.
    tracker.dlq.dlq_written = False
    waiting = asyncio.create_task(proc._admit_next_group(tracker))
    await wait_for(lambda: budget.waiting_loops == 1)
    tracker.dlq.dlq_written = True
    budget.release(1, 0)
    budget.wake_next()
    assert await asyncio.wait_for(waiting, timeout=QUICK_STOP_SECONDS) is False
    assert budget.reserved_tasks == 0 and not budget.unmeasured_window_open


async def test_stop_and_dlq_outcome_together_end_the_message_and_do_not_abandon_it(monkeypatch):
    # A long wake-up: only the stop's cancel of the pull wakes the stream,
    # so it sees the outcome and the stop at the same check.
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 5.0)
    handler = TaskResultsHandler()
    proc = stream_processor(handler, on_collect=DLQOnDelivery(handler.log))
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.pulls == 2 and proc._message_trackers[0].dlq.dlq_written)
    await wait_for(lambda: proc.inflight_count == 0)

    proc.signal_stop()
    await asyncio.wait_for(proc.drain(), timeout=QUICK_STOP_SECONDS)

    assert proc._abandoned_offsets == set()
    assert handler.log.count('message:0') == 1
    assert not proc.offset_tracker.has_pending()
    await proc.stop()


async def test_a_message_taken_off_the_queue_after_a_stop_is_dropped_unarranged():
    handler = RecordingStreamHandler([1])
    proc = stream_processor(handler)
    get = proc._queue.get

    async def stop_then_get():
        # The stop comes while the loop waits, and a message arrives with it.
        message = await get()
        proc.signal_stop()
        return message

    proc._queue.get = stop_then_get
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: proc._loop_ended)

    assert handler.log == []
    assert not proc.offset_tracker.has_pending()
    assert events(cap, 'arrange_stream_abandoned') == []
    [dropped] = events(cap, 'queued_messages_dropped_at_stop')
    assert dropped['count'] == 1 and dropped['first_offset'] == 0
    await proc.stop()


class FailedDelivery(RecordingCollect):
    """An ``on_collect`` double whose delivery of a task result cannot be confirmed."""

    async def __call__(self, result, partition_id, message_context=None):
        await super().__call__(result, partition_id, message_context)
        if any(payload.data.v.startswith('task-') for payload in result.kafka):
            raise SinkDeliveryFailedError(sink_name='out', sink_type='kafka', reason='dlq down')
        return {}


async def test_a_message_whose_offset_will_stall_stops_pulling_groups():
    handler = TaskResultsHandler()
    proc = stream_processor(handler, on_collect=FailedDelivery(handler.log), on_dlq_failure='stall')
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: 0 in proc._stalled_offsets)

    assert handler.pulls == 2
    assert handler.finally_ran
    assert 'task:m0-g1-t0' not in handler.log
    assert handler.log.count('group:0:0') == 1
    assert handler.log.count('message:0') == 1
    assert proc._abandoned_offsets == set()
    assert proc.offset_tracker.has_pending()
    await proc.stop()


async def test_a_task_that_ends_between_the_stop_and_the_abandon_delivers_nothing():
    handler = TaskResultsHandler([1])
    proc = stream_processor(handler, on_collect=RecordingCollect(handler.log))
    release = asyncio.Event()
    block_offsets(proc, {0}, release)
    start_group = proc._start_group

    async def stop_while_a_task_ends(tracker, tasks, window):
        await start_group(tracker, tasks, window)
        # The stop comes during the fan-out, and the task ends before the
        # stream is abandoned.
        proc.signal_stop()
        release.set()
        await wait_for(lambda: proc.inflight_count == 0)

    proc._start_group = stop_while_a_task_ends
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: proc._loop_ended)

    assert 'task:m0-g0-t0' in handler.log
    assert not any(entry.startswith(('deliver:', 'group:', 'message:')) for entry in handler.log)
    assert proc._abandoned_offsets == {0}
    await proc.stop()


async def test_a_failed_stream_gives_back_its_kept_bytes_at_a_stop():
    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)

    class FailsWithOneTaskDone(WithGroupHook):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 3)
            await wait_for(lambda: 'task:m0-g0-t0' in self.log)
            raise RuntimeError('side api down')

    handler = FailsWithOneTaskDone()
    proc = stream_processor(handler, budget=budget)
    release = asyncio.Event()
    execute = proc._executor_pool.execute

    async def first_task_settles(task, recorder, partition_id):
        # One task finishes, so the group keeps its output; the others wait.
        if not task.task_id.endswith('t0'):
            await release.wait()
        return await execute(task, recorder, partition_id)

    proc._executor_pool.execute = first_task_settles
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        proc.start()
        await wait_for(lambda: events(cap, 'partition_processor_restarting') and proc.inflight_count == 2)
        assert budget.bytes > 0

        proc.signal_stop()
        await wait_for(lambda: proc._loop_ended)

    # Read before stop() closes the account, so a leak cannot hide.
    assert budget.tasks == 0 and budget.bytes == 0
    assert proc._message_trackers[0].open_windows == {}
    assert handler.group_calls == []
    await proc.stop()
