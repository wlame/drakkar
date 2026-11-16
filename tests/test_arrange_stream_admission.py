"""Admission of stream groups against the worker's pending budget.

A stream handler is admitted group by group: before each pull the loop waits
for room and reserves one group's estimate, so the limits are passed by at
most one group for each streaming message.
"""

import asyncio
import itertools

import pytest

from drakkar.budget import PendingBudget, output_bytes, text_bytes
from drakkar.metrics import window_admission_wait
from drakkar.partition import FANOUT_CHUNK_TASKS, PartitionProcessor
from tests.conftest import wait_for
from tests.stream_helpers import (
    RecordingStreamHandler,
    WithGroupHook,
    make_msg,
    make_pool,
    stream_processor,
    stream_task,
)


class GatedStream(RecordingStreamHandler):
    """Yields ``groups`` groups of ``size`` tasks; each task stays pending in its hook until ``release`` is set.

    ``pulls`` counts the times the generator resumed to make a group, and
    ``pending_at_pull`` samples the budget's pending tasks at each of them.
    """

    def __init__(self, groups: int, size: int, budget: PendingBudget | None = None, **kwargs) -> None:
        super().__init__([size] * groups, **kwargs)
        self.release = asyncio.Event()
        self.pulls = 0
        self.pending_at_pull: list[int] = []
        self.budget = budget

    async def arrange_stream(self, message):
        for index, size in enumerate(self.sizes):
            self.pulls += 1
            if self.budget is not None:
                self.pending_at_pull.append(self.budget.tasks)
            yield self.make_group(message, index, size)

    async def on_task_complete(self, result):
        await self.release.wait()
        return await super().on_task_complete(result)


def budget_processor(handler, budget: PendingBudget, partition_id: int = 0) -> PartitionProcessor:
    """A processor on ``budget``."""
    return PartitionProcessor(
        partition_id=partition_id,
        handler=handler,
        executor_pool=make_pool(),
        window_size=10,
        budget=budget,
    )


async def finish(proc: PartitionProcessor) -> None:
    """Wait until every offset and task of ``proc`` is done, then stop it."""
    await wait_for(lambda: proc.queue_size == 0 and not proc.offset_tracker.has_pending() and proc.inflight_count == 0)
    await proc.stop()


async def test_no_group_is_pulled_while_pending_tasks_are_at_the_limit():
    budget = PendingBudget(max_tasks=4, max_bytes=1 << 40)
    handler = GatedStream(groups=4, size=2)
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: budget.tasks == 4)
    # Give the loop many turns: the generator must not be resumed again.
    for _ in range(50):
        await asyncio.sleep(0)
    assert handler.pulls == 2
    assert budget.waiting_loops == 1
    handler.release.set()
    await finish(proc)
    assert handler.pulls == 4
    assert budget.tasks == 0 and budget.reserved_tasks == 0


class OneAtATime(RecordingStreamHandler):
    """Yields ``groups`` groups of ``size`` tasks; a task leaves its hook only when the test gives it a permit."""

    def __init__(self, groups: int, size: int, budget: PendingBudget) -> None:
        super().__init__([size] * groups)
        self.permits = asyncio.Semaphore(0)
        self.budget = budget
        self.pulls = 0
        self.pending_at_pull: list[int] = []
        self.pending_in_hook: list[int] = []

    async def arrange_stream(self, message):
        for index, size in enumerate(self.sizes):
            self.pulls += 1
            self.pending_at_pull.append(self.budget.tasks)
            yield self.make_group(message, index, size)

    async def on_task_complete(self, result):
        self.pending_in_hook.append(self.budget.tasks)
        await self.permits.acquire()
        return await super().on_task_complete(result)


async def test_pending_tasks_never_pass_the_limit_by_more_than_one_group():
    budget = PendingBudget(max_tasks=6, max_bytes=1 << 40)
    handler = OneAtATime(groups=20, size=3, budget=budget)
    proc = budget_processor(handler, budget)
    pending_after_fan_out: list[int] = []
    note_shape = proc._note_window_shape

    def note_shape_and_sample(message_count, task_count, stdin_bytes):
        # Called once per group, right after its fan-out: the peak of the charge.
        pending_after_fan_out.append(budget.tasks)
        note_shape(message_count, task_count, stdin_bytes)

    proc._note_window_shape = note_shape_and_sample
    proc.enqueue(make_msg(0))
    proc.start()

    async def release_when_the_stream_waits():
        # One task settles only while the stream waits for room, so the
        # budget is kept at its limit for the whole message.
        while handler.pulls < 20:
            if budget.waiting_loops:
                handler.permits.release()
            await asyncio.sleep(0.001)
        for _ in range(60):
            handler.permits.release()

    releaser = asyncio.create_task(release_when_the_stream_waits())
    await finish(proc)
    await releaser
    assert handler.pulls == 20
    # A pull starts only below the limit; a group of 3 may then pass it.
    assert max(handler.pending_at_pull) < 6
    assert len(pending_after_fan_out) == 20
    assert 6 <= max(pending_after_fan_out) <= 6 + 3


async def test_a_group_larger_than_the_limit_is_admitted_and_the_stream_goes_on():
    budget = PendingBudget(max_tasks=4, max_bytes=1 << 40)
    handler = RecordingStreamHandler([10, 1])
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await finish(proc)
    assert len(handler.message_groups) == 1
    assert [group.task_count for group in handler.message_groups[0].groups] == [10, 1]
    assert handler.message_groups[0].succeeded == 11
    assert budget.tasks == 0 and budget.bytes == 0


async def test_bytes_limit_holds_the_stream_as_the_tasks_limit_does():
    stdin_size = text_bytes('in')
    budget = PendingBudget(max_tasks=10_000, max_bytes=4 * stdin_size)
    handler = GatedStream(groups=4, size=2)
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    before = window_admission_wait.labels(limit='bytes')._value.get()
    proc.start()
    await wait_for(lambda: budget.bytes >= 4 * stdin_size)
    for _ in range(50):
        await asyncio.sleep(0)
    assert handler.pulls == 2
    assert budget.full_limit() == 'bytes'
    handler.release.set()
    await finish(proc)
    assert handler.pulls == 4
    assert window_admission_wait.labels(limit='bytes')._value.get() > before


async def test_kept_group_output_is_charged_until_the_group_hook_ends():
    budget = PendingBudget(max_tasks=10_000, max_bytes=1 << 40)
    hook_entered = asyncio.Event()
    hook_release = asyncio.Event()
    samples: list[tuple[int, int]] = []

    class SlowGroupHook(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            samples.append((budget.bytes, sum(output_bytes(result) for result in results)))
            hook_entered.set()
            await hook_release.wait()
            return await super().on_group_complete(group, results, errors)

    handler = SlowGroupHook([2])
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await asyncio.wait_for(hook_entered.wait(), timeout=2)
    [(charged, kept)] = samples
    # Every task settled (stdin freed); only the kept output is still charged.
    assert kept > 0 and charged == kept
    assert budget.tasks == 0
    hook_release.set()
    await finish(proc)
    assert budget.bytes == 0
    # The kept output of the group fed the averages, one message per group.
    assert proc._shape.window_kept_bytes == kept


async def test_two_partitions_streaming_share_the_budget_in_turn():
    # Each group fills the budget, so every pull after the first waits in the FIFO list.
    budget = PendingBudget(max_tasks=2, max_bytes=1 << 40)
    order: list[int] = []

    class Logged(OneAtATime):
        def __init__(self, partition: int) -> None:
            super().__init__(groups=3, size=2, budget=budget)
            self.partition = partition

        async def arrange_stream(self, message):
            async for group in super().arrange_stream(message):
                order.append(self.partition)
                yield group

    handlers = [Logged(0), Logged(1)]
    procs = [budget_processor(handler, budget, partition_id=n) for n, handler in enumerate(handlers)]
    for proc in procs:
        proc.enqueue(make_msg(0, partition=proc.partition_id))
        proc.start()

    released = {id(handler): 0 for handler in handlers}

    async def release_one_group_when_full():
        # Only while the budget is full and both streams wait (a stream waits
        # before its last group too, for the pull that ends it), the two
        # tasks that wait in their hook settle: one group's worth at a time.
        while len(order) < 6:
            holders = [h for h in handlers if len(h.pending_in_hook) - released[id(h)] >= 2]
            if budget.tasks == 2 and budget.waiting_loops == 2 and holders:
                [holder] = holders
                holder.permits.release()
                holder.permits.release()
                released[id(holder)] += 2
            await asyncio.sleep(0.001)
        for handler in handlers:
            for _ in range(6):
                handler.permits.release()

    releaser = asyncio.create_task(release_one_group_when_full())
    for proc in procs:
        await finish(proc)
    await releaser
    # The first stream pulls its first group alone, and both then wait in the
    # FIFO list and take turns. The admissions are 0, 0, 1, 0, 1, 0, 1: the
    # fourth admission of the first stream is the pull that ends its stream,
    # so it yields no group.
    assert order == [0, 0, 1, 0, 1, 1]
    assert all(len(handler.message_groups) == 1 for handler in handlers)
    assert budget.tasks == 0 and budget.reserved_tasks == 0


async def test_stop_wakes_a_stream_that_waits_for_room():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 40)
    budget.charge(1, 0)
    proc = budget_processor(RecordingStreamHandler(), budget)
    proc._running = True
    admission = asyncio.create_task(proc._admit_next_group())
    await wait_for(lambda: budget.waiting_loops == 1)
    proc.signal_stop()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert admission.done()
    assert admission.result() is False
    assert budget.reserved_tasks == 0
    budget.release(1, 0)


async def test_admission_wait_is_counted_under_the_limit_that_blocked():
    budget = PendingBudget(max_tasks=2, max_bytes=1 << 40)
    handler = GatedStream(groups=2, size=2)
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    tasks_before = window_admission_wait.labels(limit='tasks')._value.get()
    bytes_before = window_admission_wait.labels(limit='bytes')._value.get()
    proc.start()
    await wait_for(lambda: budget.tasks == 2)
    await asyncio.sleep(0.01)
    handler.release.set()
    await finish(proc)
    assert window_admission_wait.labels(limit='tasks')._value.get() > tasks_before
    assert window_admission_wait.labels(limit='bytes')._value.get() == bytes_before


async def test_without_a_budget_a_stream_pulls_without_waiting():
    handler = RecordingStreamHandler([2, 2])
    proc = stream_processor(handler)
    assert await proc._admit_next_group() is True
    proc.enqueue(make_msg(0))
    proc.start()
    await finish(proc)
    assert len(handler.message_groups) == 1


async def test_a_stream_that_waits_retries_the_commit_on_each_wake_up(monkeypatch):
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 40)
    budget.charge(1, 0)
    proc = budget_processor(RecordingStreamHandler(), budget)
    proc._running = True
    monkeypatch.setattr('drakkar.partition.IDLE_POLL_TIMEOUT', 0.01)
    commits = 0

    async def note_commit_due():
        nonlocal commits
        commits += 1

    proc._note_commit_due = note_commit_due
    admission = asyncio.create_task(proc._admit_next_group())
    await wait_for(lambda: commits >= 2)
    budget.release(1, 0)
    assert await asyncio.wait_for(admission, timeout=2) is True
    # No group was measured yet: the stream holds the one unmeasured slot.
    assert budget.unmeasured_window_open
    proc._end_window_reservation()
    assert not budget.unmeasured_window_open


async def test_the_first_groups_reserve_the_free_room_and_later_ones_their_estimate():
    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)
    proc = budget_processor(RecordingStreamHandler(), budget)
    proc._running = True
    proc._shape.note_window(1, 2, 0)
    assert await proc._admit_next_group() is True
    assert budget.reserved_tasks == 1000
    proc._end_window_reservation()
    proc._windows_seen = 3
    assert await proc._admit_next_group() is True
    # After the warm-up: the group average, with no floor from window_size.
    assert budget.reserved_tasks == 2
    proc._end_window_reservation()


async def test_a_raising_pull_gives_back_its_reservation():
    class Raising(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield [stream_task('a', message.offset)]
            raise RuntimeError('broken stream')

    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)
    proc = budget_processor(Raising(), budget)
    proc._running = True
    with pytest.raises(RuntimeError, match='broken stream'):
        await proc._process_stream_message(make_msg(0))
    assert budget.reserved_tasks == 0 and budget.reserved_bytes == 0
    assert not budget.unmeasured_window_open
    await wait_for(lambda: proc.inflight_count == 0)
    assert budget.tasks == 0


async def test_a_stop_while_the_stream_waits_leaves_its_message_open():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 40)
    budget.charge(1, 0)
    handler = GatedStream(groups=2, size=1)
    proc = budget_processor(handler, budget)
    proc._running = True
    streaming = asyncio.create_task(proc._process_stream_message(make_msg(0)))
    await wait_for(lambda: budget.waiting_loops == 1)
    proc.signal_stop()
    await asyncio.wait_for(streaming, timeout=2)
    assert handler.pulls == 0
    assert proc._message_trackers[0].stream_open
    assert proc.offset_tracker.has_pending()
    budget.release(1, 0)


async def test_an_empty_list_gives_back_its_reservation_and_the_unmeasured_slot():
    class EmptyFirst(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield []
            self.log.append(f'after-empty:{budget.reserved_tasks}:{budget.reserved_bytes}')
            yield self.make_group(message, 0, 2)

    budget = PendingBudget(max_tasks=1000, max_bytes=1 << 40)
    handler = EmptyFirst()
    proc = budget_processor(handler, budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await finish(proc)
    # The empty list gave back the unmeasured slot (else the next pull would
    # wait for it for ever); the next pull took it again and reserved nothing.
    assert 'after-empty:0:0' in handler.log
    assert handler.message_groups[0].succeeded == 2
    assert budget.reserved_tasks == 0 and budget.reserved_bytes == 0
    assert not budget.unmeasured_window_open


async def test_a_stream_of_two_task_groups_reserves_about_its_group_with_default_limits():
    reserved_at_pull: list[tuple[int, int]] = []

    class Sampling(RecordingStreamHandler):
        async def arrange_stream(self, message):
            for index in range(8):
                reserved_at_pull.append((budget.reserved_tasks, budget.reserved_bytes))
                yield self.make_group(message, index, 2)

    # The defaults of executor.max_pending_tasks, max_pending_bytes and window_size.
    budget = PendingBudget(max_tasks=2000, max_bytes=1 << 30)
    proc = PartitionProcessor(
        partition_id=0,
        handler=Sampling(),
        executor_pool=make_pool(),
        window_size=100,
        budget=budget,
    )
    proc.enqueue(make_msg(0))
    proc.start()
    await finish(proc)
    # After the warm-up a pull reserves the group average, not
    # 2000 / 100 = 20 tasks and 1 GiB / 100 bytes.
    assert len(reserved_at_pull) == 8
    for tasks, held_bytes in reserved_at_pull[4:]:
        assert tasks == 2
        assert held_bytes <= 2 * text_bytes('in')


class Splitter(RecordingStreamHandler):
    """Yields ``count`` single-task groups and never awaits, as a CPU-only splitter does."""

    def __init__(self, count: int) -> None:
        super().__init__()
        self.count = count
        self.pulls = 0

    async def arrange_stream(self, message):
        for index in range(self.count):
            self.pulls += 1
            yield self.make_group(message, index, 1)


async def ticks_until_streamed(handler: Splitter) -> list[int]:
    """Run beside the stream, noting ``handler.pulls`` at each of its turns, until every group was pulled."""
    samples = [handler.pulls]
    while handler.pulls < handler.count:
        await asyncio.sleep(0)
        samples.append(handler.pulls)
    return samples


@pytest.mark.parametrize('with_budget', [True, False], ids=['budget', 'no_budget'])
async def test_a_stream_of_small_groups_gives_the_loop_a_turn_every_chunk_of_tasks(with_budget):
    handler = Splitter(count=4 * FANOUT_CHUNK_TASKS)
    if with_budget:
        proc = budget_processor(handler, PendingBudget(max_tasks=100_000, max_bytes=1 << 40))
    else:
        proc = stream_processor(handler)
    ticker = asyncio.create_task(ticks_until_streamed(handler))
    proc.enqueue(make_msg(0))
    proc.start()
    samples = await asyncio.wait_for(ticker, timeout=10)
    await finish(proc)
    steps = [later - earlier for earlier, later in itertools.pairwise(samples)]
    assert max(steps) <= FANOUT_CHUNK_TASKS
    assert len(handler.message_groups) == 1
