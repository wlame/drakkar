"""Backpressure: the pending budget, window admission, stdin release, and the Kafka pause rule.

Hermetic: tasks carry precomputed results, so the executor pool never spawns
a subprocess, and the Kafka consumer is an ``AsyncMock``.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import drakkar.partition as partition_module
from drakkar.app import DrakkarApp
from drakkar.budget import BudgetAccount, MessageShape, PendingBudget, output_bytes, text_bytes
from drakkar.config import DrakkarConfig
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import (
    pending_bytes,
    pending_tasks,
    queued_messages_dropped_at_stop,
    window_admission_wait,
    window_messages,
)
from drakkar.models import (
    ErrorAction,
    ExecutorTask,
    PrecomputedResult,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.webapp.runner import WebappRunner
from tests.conftest import kafka_connections, wait_for, wire_kafka_source
from tests.test_app import SimpleHandler
from tests.test_webapp_runner import _make_config, _make_ctx, _make_stub_app, _RecordingHandler

STDIN = 'x' * 1000
STDOUT = 'y' * 500


def make_msg(offset: int, partition: int = 0) -> SourceMessage:
    return SourceMessage(topic='t', partition=partition, offset=offset, value=b'{}', timestamp=0)


def make_task(task_id: str, offset: int, *, exit_code: int = 0, stdin: str | None = STDIN) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[offset],
        stdin=stdin,
        precomputed=PrecomputedResult(stdout=STDOUT, exit_code=exit_code),
    )


@pytest.fixture
def pool() -> ExecutorPool:
    return ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)


def big_budget() -> PendingBudget:
    return PendingBudget(max_tasks=10_000, max_bytes=1 << 40)


async def collect_next_window(proc: PartitionProcessor) -> list[SourceMessage]:
    """Call ``_collect_window`` as ``_run`` does until it returns a window."""
    while True:
        messages = await proc._collect_window()
        if messages:
            return messages


def idle_processor(
    budget: PendingBudget | None, window_size: int = 10, handler: BaseDrakkarHandler | None = None
) -> PartitionProcessor:
    """A processor whose loop is not started; tests call ``_collect_window`` themselves."""
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler or SimpleHandler(),
        executor_pool=MagicMock(),
        window_size=window_size,
        budget=budget,
    )
    proc._running = True
    return proc


# --------------------------------------------------------------- the budget


def test_budget_charge_and_release_move_counts_and_gauges():
    budget = PendingBudget(max_tasks=10, max_bytes=1000)

    budget.charge(3, 300)
    assert (budget.tasks, budget.bytes) == (3, 300)
    assert pending_tasks._value.get() == 3
    assert pending_bytes._value.get() == 300

    budget.release(3, 300)
    assert (budget.tasks, budget.bytes) == (0, 0)
    assert pending_tasks._value.get() == 0


@pytest.mark.parametrize(
    ('tasks', 'held', 'expected'),
    [(9, 999, None), (10, 0, 'tasks'), (0, 1000, 'bytes'), (10, 1000, 'tasks')],
)
def test_budget_full_limit_names_the_reached_limit_tasks_first(tasks, held, expected):
    budget = PendingBudget(max_tasks=10, max_bytes=1000)
    budget.charge(tasks, held)

    assert budget.full_limit() == expected


async def test_budget_release_wakes_waiters_oldest_first():
    budget = PendingBudget(max_tasks=1, max_bytes=1000)
    budget.charge(1, 0)
    first = budget.add_waiter()
    second = budget.add_waiter()

    budget.release(1, 0)

    assert first.result() is True
    assert not second.done()
    budget.wake_next()
    assert second.result() is True


async def test_budget_wake_next_skips_waiters_that_left():
    budget = PendingBudget(max_tasks=1, max_bytes=1000)
    left = budget.add_waiter()
    left.cancel()
    waiting = budget.add_waiter()

    budget.wake_next()

    assert waiting.result() is True


async def test_budget_wake_next_without_room_wakes_nobody():
    budget = PendingBudget(max_tasks=1, max_bytes=1000)
    budget.charge(1, 0)
    waiter = budget.add_waiter()

    budget.wake_next()

    assert not waiter.done()


def test_budget_account_close_releases_what_it_still_holds_once():
    budget = PendingBudget(max_tasks=10, max_bytes=1000)
    account = BudgetAccount(budget)
    account.charge(4, 400)
    account.release(1, 100)

    account.close()
    account.close()

    assert (budget.tasks, budget.bytes) == (0, 0)


def test_text_bytes_and_output_bytes_measure_the_strings():
    assert text_bytes(None) == 0
    assert text_bytes(STDIN) == sys.getsizeof(STDIN)
    assert output_bytes(MagicMock(stdout='a', stderr='bb')) == sys.getsizeof('a') + sys.getsizeof('bb')
    assert output_bytes(object()) == 0


# ------------------------------------------------------- window admission


async def test_collect_window_waits_at_the_task_limit_until_a_release():
    budget = PendingBudget(max_tasks=2, max_bytes=1 << 30)
    budget.charge(2, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))

    collecting = asyncio.create_task(collect_next_window(proc))
    await asyncio.sleep(0.05)
    assert not collecting.done()

    budget.release(1, 0)

    assert [m.offset for m in await collecting] == [0]


async def test_collect_window_waits_at_the_byte_limit_until_a_release():
    budget = PendingBudget(max_tasks=100, max_bytes=1000)
    budget.charge(0, 1000)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))

    collecting = asyncio.create_task(collect_next_window(proc))
    await asyncio.sleep(0.05)
    assert not collecting.done()

    budget.release(0, 1)

    assert [m.offset for m in await collecting] == [0]


async def test_collect_window_without_room_returns_empty_after_the_idle_bound_and_keeps_its_place(monkeypatch):
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 0.05)
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))
    waited_before = window_admission_wait.labels(limit='tasks')._value.get()

    assert await proc._collect_window() == []
    waiter = proc._room_waiter
    assert await proc._collect_window() == []

    assert proc._room_waiter is waiter
    assert proc.queue_size == 1
    assert window_admission_wait.labels(limit='tasks')._value.get() > waited_before


async def test_collect_window_leaves_at_once_on_stop():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))

    collecting = asyncio.create_task(proc._collect_window())
    await asyncio.sleep(0.01)
    proc.signal_stop()

    assert await asyncio.wait_for(collecting, timeout=0.5) == []


async def test_stop_while_a_message_is_held_drops_it_and_takes_no_window():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))
    assert await proc._collect_window() == []
    dropped_before = queued_messages_dropped_at_stop._value.get()

    proc.signal_stop()
    assert await proc._collect_window() == []
    await proc.stop()
    proc._drop_queued_messages()

    assert proc.queue_size == 0
    assert queued_messages_dropped_at_stop._value.get() - dropped_before == 1


async def test_collect_window_takes_its_turn_when_room_opens_without_a_release():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))
    assert await proc._collect_window() == []
    waiter = proc._room_waiter

    budget.max_tasks = 10
    messages = await proc._collect_window()

    assert len(messages) == 1
    assert waiter.result() is True
    assert proc._room_waiter is None


async def test_collect_window_new_loop_queues_behind_waiters_even_with_room(monkeypatch):
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 5.0)
    budget = PendingBudget(max_tasks=10, max_bytes=1 << 30)
    budget.shape.note_window(message_count=1, task_count=5, stdin_bytes=0)
    budget.charge(10, 0)
    waiting, newcomer = idle_processor(budget), idle_processor(budget)
    waiting.enqueue(make_msg(0))
    newcomer.enqueue(make_msg(0))
    waiting_collect = asyncio.create_task(collect_next_window(waiting))
    await asyncio.sleep(0.01)

    budget._tasks = 5  # room for one message of five tasks opens without a release
    newcomer_collect = asyncio.create_task(collect_next_window(newcomer))
    await asyncio.sleep(0.01)

    assert waiting_collect.done()
    assert not newcomer_collect.done()
    waiting._end_window_reservation()
    budget.release(5, 0)
    assert len(await newcomer_collect) == 1


async def test_collect_window_wakes_waiting_partitions_in_fifo_order_and_the_reservation_holds_the_room(
    monkeypatch,
):
    # Long enough that no wait ends on the time bound and re-checks the room.
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 5.0)
    budget = PendingBudget(max_tasks=3, max_bytes=1 << 30)
    budget.shape.note_window(message_count=1, task_count=1, stdin_bytes=0)
    budget.charge(3, 0)
    first, second = idle_processor(budget), idle_processor(budget)
    first.enqueue(make_msg(0))
    second.enqueue(make_msg(0))
    first_collect = asyncio.create_task(collect_next_window(first))
    await asyncio.sleep(0.01)
    second_collect = asyncio.create_task(collect_next_window(second))
    await asyncio.sleep(0.01)

    budget.release(1, 0)
    await asyncio.sleep(0.01)

    assert first_collect.done()
    assert budget.reserved_tasks == 1
    assert not second_collect.done()
    first._end_window_reservation()
    assert len(await second_collect) == 1


async def test_woken_loop_queues_again_at_the_front_when_the_room_is_gone():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))
    assert await proc._collect_window() == []
    proc._room_waiter.set_result(True)  # woken just after its time bound ended

    assert await proc._collect_window() == []

    assert proc._room_waiter is not None
    assert not proc._room_waiter.done()


async def test_woken_loop_that_is_stopping_passes_the_wake_up_on(monkeypatch):
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 5.0)
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    stopping, next_loop = idle_processor(budget), idle_processor(budget)
    stopping.enqueue(make_msg(0))
    next_loop.enqueue(make_msg(0))
    stopping_collect = asyncio.create_task(stopping._collect_window())
    await asyncio.sleep(0.01)
    next_collect = asyncio.create_task(next_loop._collect_window())
    await asyncio.sleep(0.01)

    budget.release(1, 0)  # wakes the oldest waiter: the stopping loop
    stopping._running = False  # the stop lands before the woken loop runs

    assert await stopping_collect == []
    assert len(await asyncio.wait_for(next_collect, timeout=1.0)) == 1


async def test_wait_behind_other_waiters_counts_under_the_limit_that_last_blocked(monkeypatch):
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 0.02)
    budget = PendingBudget(max_tasks=100, max_bytes=1000)
    budget.charge(0, 1000)
    assert budget.full_limit() == 'bytes'
    budget.release(0, 1000)
    budget.add_waiter()  # an older waiter; the wake-up the newcomer triggers goes to it
    newcomer = idle_processor(budget)
    newcomer.enqueue(make_msg(0))
    before = window_admission_wait.labels(limit='bytes')._value.get()

    assert await newcomer._collect_window() == []

    assert window_admission_wait.labels(limit='bytes')._value.get() > before


async def test_woken_loop_takes_no_window_after_a_stop():
    budget = PendingBudget(max_tasks=1, max_bytes=1 << 30)
    budget.charge(1, 0)
    proc = idle_processor(budget)
    proc.enqueue(make_msg(0))
    assert await proc._collect_window() == []
    budget.release(1, 0)  # wakes the waiter
    proc._running = False  # a stop flag set without resolving the waiter

    assert await proc._wait_for_room() is False


async def test_second_unmeasured_window_waits_for_the_first_to_fan_out(monkeypatch):
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', 5.0)
    budget = big_budget()
    first, second = idle_processor(budget), idle_processor(budget)
    first.enqueue(make_msg(0))
    second.enqueue(make_msg(0))

    assert len(await first._collect_window()) == 1
    assert budget.unmeasured_window_open
    second_collect = asyncio.create_task(collect_next_window(second))
    await asyncio.sleep(0.01)
    assert not second_collect.done()

    first._end_window_reservation()

    assert len(await second_collect) == 1


async def test_warm_up_window_takes_one_message_and_reserves_all_the_free_room():
    budget = PendingBudget(max_tasks=100, max_bytes=1 << 30)
    budget.shape.note_window(message_count=1, task_count=10, stdin_bytes=0)
    proc = idle_processor(budget, window_size=50)
    for offset in range(20):
        proc.enqueue(make_msg(offset))

    messages = await proc._collect_window()

    assert len(messages) == 1
    assert budget.reserved_tasks == 100
    assert budget.full_limit() == 'tasks'
    assert not budget.unmeasured_window_open


class SkipEveryMessageHandler(BaseDrakkarHandler):
    """``arrange`` creates no task for any message."""

    async def arrange(self, messages, pending):
        return []


async def test_partition_whose_windows_create_no_tasks_leaves_the_warm_up():
    """Windows with no tasks count toward the warm-up but not the averages, so they stop reserving the whole budget."""
    budget = PendingBudget(max_tasks=100, max_bytes=1 << 30)
    budget.shape.note_window(message_count=1, task_count=1, stdin_bytes=0)
    proc = idle_processor(budget, window_size=10, handler=SkipEveryMessageHandler())
    for offset in range(partition_module.WARM_UP_WINDOWS + 2):
        proc.enqueue(make_msg(offset))

    for _ in range(partition_module.WARM_UP_WINDOWS):
        messages = await proc._collect_window()
        assert len(messages) == 1
        assert budget.reserved_tasks == 100
        await proc._process_window(messages)
        proc._end_window_reservation()

    assert not proc._shape.measured
    assert not proc._in_warm_up()
    messages = await proc._collect_window()
    # The floor is 100 / 10 = 10 tasks per message: two messages reserve 20, not the whole budget.
    assert len(messages) == 2
    assert budget.reserved_tasks == 20


async def test_collect_window_first_window_of_a_partition_takes_one_message():
    proc = idle_processor(big_budget())
    for offset in range(5):
        proc.enqueue(make_msg(offset))
    observed_before = window_messages._sum.get()

    messages = await proc._collect_window()

    assert [m.offset for m in messages] == [0]
    assert window_messages._sum.get() - observed_before == 1


def test_admitted_window_size_follows_the_moving_averages():
    budget = PendingBudget(max_tasks=100, max_bytes=1 << 30)
    proc = idle_processor(budget, window_size=50)

    proc._note_window_shape(message_count=1, task_count=10, stdin_bytes=0)
    proc._windows_seen = partition_module.WARM_UP_WINDOWS
    assert proc._admitted_window_size() == 10

    proc._note_window_shape(message_count=1, task_count=20, stdin_bytes=0)
    assert proc._shape.tasks == pytest.approx(15.0)
    assert proc._admitted_window_size() == 6


def test_admitted_window_size_is_bound_by_free_bytes():
    budget = PendingBudget(max_tasks=10_000, max_bytes=10_000)
    proc = idle_processor(budget, window_size=5)
    proc._note_window_shape(message_count=1, task_count=1, stdin_bytes=4000)
    proc._windows_seen = partition_module.WARM_UP_WINDOWS

    assert proc._admitted_window_size() == 2
    budget.charge(0, 4000)
    assert proc._admitted_window_size() == 1


def test_admitted_window_size_reserves_at_least_the_floor_for_a_low_average():
    budget = PendingBudget(max_tasks=100, max_bytes=1 << 30)
    proc = idle_processor(budget, window_size=10)
    proc._note_window_shape(message_count=1, task_count=1, stdin_bytes=0)
    proc._windows_seen = partition_module.WARM_UP_WINDOWS

    # The floor is 100 / 10 = 10 tasks per message, so a full window reserves the cap.
    assert proc._admitted_window_size() == 10
    proc._reserve_window(10)
    assert budget.reserved_tasks == 100
    assert budget.full_limit() == 'tasks'


def test_admitted_window_size_takes_at_least_one_message_with_no_room():
    budget = PendingBudget(max_tasks=10, max_bytes=1 << 30)
    proc = idle_processor(budget)
    proc._note_window_shape(message_count=1, task_count=5, stdin_bytes=0)
    budget.charge(10, 0)

    assert proc._admitted_window_size() == 1


def test_admitted_window_size_without_a_budget_is_window_size():
    proc = idle_processor(None, window_size=7)

    assert proc._admitted_window_size() == 7


class FanOutHandler(BaseDrakkarHandler):
    """Each message becomes ``fan_out`` tasks; message and window hooks record what they see."""

    def __init__(self, fan_out: int = 1, exit_code: int = 0) -> None:
        super().__init__()
        self.fan_out = fan_out
        self.exit_code = exit_code
        self.budget: PendingBudget | None = None
        self.stdin_in_on_task_complete: list[str | None] = []
        self.stdin_in_message_hook: list[str | None] = []
        self.stdin_in_window_hook: list[str | None] = []
        self.bytes_in_message_hook: list[int] = []

    async def arrange(self, messages, pending):
        return [
            make_task(f'm{msg.offset}-t{i}', msg.offset, exit_code=self.exit_code)
            for msg in messages
            for i in range(self.fan_out)
        ]

    async def on_task_complete(self, result):
        self.stdin_in_on_task_complete.append(result.task.stdin)
        return None

    async def on_message_complete(self, group):
        self.stdin_in_message_hook.extend(task.stdin for task in group.tasks)
        if self.budget is not None:
            self.bytes_in_message_hook.append(self.budget.bytes)
        return None

    async def on_window_complete(self, results, source_messages):
        self.stdin_in_window_hook.extend(result.task.stdin for result in results)
        return None


async def run_until_settled(proc: PartitionProcessor) -> None:
    proc.start()
    await wait_for(lambda: proc.queue_size == 0 and not proc.offset_tracker.has_pending() and proc.inflight_count == 0)
    await proc.stop()


async def test_admission_lets_one_message_fan_out_past_the_task_limit(pool):
    """The limit is soft: a window with room takes a message whatever its fan-out."""
    budget = PendingBudget(max_tasks=5, max_bytes=1 << 30)
    peak: list[int] = []

    class Peak(FanOutHandler):
        async def on_task_complete(self, result):
            peak.append(budget.tasks)
            return None

    proc = PartitionProcessor(
        partition_id=0, handler=Peak(fan_out=20), executor_pool=pool, window_size=10, budget=budget
    )
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert max(peak) > budget.max_tasks
    assert (budget.tasks, budget.bytes) == (0, 0)


class PeakBudget(PendingBudget):
    """Records the highest charged tasks and bytes."""

    def __init__(self, **kwargs) -> None:
        self.peak_tasks = 0
        self.peak_bytes = 0
        super().__init__(**kwargs)

    def charge(self, tasks: int, held_bytes: int) -> None:
        super().charge(tasks, held_bytes)
        self.peak_tasks = max(self.peak_tasks, self.tasks)
        self.peak_bytes = max(self.peak_bytes, self.bytes)


class SlowArrangeHandler(BaseDrakkarHandler):
    """``arrange`` awaits before it returns; each task is held a while in on_task_complete."""

    def __init__(self, fan_out: int) -> None:
        super().__init__()
        self.fan_out = fan_out

    async def arrange(self, messages, pending):
        await asyncio.sleep(0.01)
        return [make_task(f'm{msg.offset}-t{i}', msg.offset) for msg in messages for i in range(self.fan_out)]

    async def on_task_complete(self, result):
        await asyncio.sleep(0.02)
        return None


async def test_admission_across_partitions_stays_within_the_cap_plus_one_message(pool):
    """Several loops share one budget; arrange awaits, so they all check it while windows are arranged."""
    fan_out = 10
    task_bytes = sys.getsizeof(STDIN)
    budget = PeakBudget(max_tasks=25, max_bytes=25 * task_bytes)
    processors = [
        PartitionProcessor(
            partition_id=pid,
            handler=SlowArrangeHandler(fan_out),
            executor_pool=pool,
            window_size=10,
            budget=budget,
        )
        for pid in range(8)
    ]
    for proc in processors:
        for offset in range(5):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
        proc.start()

    await wait_for(
        lambda: all(
            p.queue_size == 0 and not p.offset_tracker.has_pending() and p.inflight_count == 0 for p in processors
        )
    )
    for proc in processors:
        await proc.stop()

    assert budget.peak_tasks <= budget.max_tasks + fan_out
    assert budget.peak_bytes <= budget.max_bytes + fan_out * task_bytes
    assert (budget.tasks, budget.bytes, budget.reserved_tasks, budget.reserved_bytes) == (0, 0, 0, 0)


class SkipFirstHandler(SlowArrangeHandler):
    """``arrange`` returns no tasks for the offsets in ``skipped``; the others fan out."""

    def __init__(self, fan_out: int, skipped: set[tuple[int, int]]) -> None:
        super().__init__(fan_out)
        self.skipped = skipped

    async def arrange(self, messages, pending):
        await asyncio.sleep(0.01)
        return [
            make_task(f'p{msg.partition}-m{msg.offset}-t{i}', msg.offset)
            for msg in messages
            if (msg.partition, msg.offset) not in self.skipped
            for i in range(self.fan_out)
        ]


def build_processors(budget: PendingBudget, handler: BaseDrakkarHandler, pool, pids) -> list[PartitionProcessor]:
    return [
        PartitionProcessor(partition_id=pid, handler=handler, executor_pool=pool, window_size=10, budget=budget)
        for pid in pids
    ]


async def settle_all(processors: list[PartitionProcessor]) -> None:
    await wait_for(
        lambda: all(
            p.queue_size == 0 and not p.offset_tracker.has_pending() and p.inflight_count == 0 for p in processors
        )
    )
    for proc in processors:
        await proc.stop()


async def test_worker_first_message_skipped_by_arrange_keeps_the_others_within_the_cap_plus_one_message(pool):
    fan_out = 100
    budget = PeakBudget(max_tasks=100, max_bytes=1 << 40)
    handler = SkipFirstHandler(fan_out, skipped={(0, 0)})
    first = build_processors(budget, handler, pool, [0])[0]
    first.enqueue(make_msg(0, partition=0))
    first.start()
    await wait_for(lambda: first.queue_size == 0 and not first.offset_tracker.has_pending())
    assert not budget.shape.measured

    others = build_processors(budget, handler, pool, range(1, 16))
    for proc in others:
        for offset in range(3):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
        proc.start()
    await settle_all([first, *others])

    assert budget.peak_tasks <= budget.max_tasks + fan_out
    assert (budget.tasks, budget.reserved_tasks) == (0, 0)


async def test_every_partition_first_window_without_tasks_stays_within_the_cap_plus_one_message(pool):
    fan_out = 100
    budget = PeakBudget(max_tasks=100, max_bytes=1 << 40)
    handler = SkipFirstHandler(fan_out, skipped={(pid, 0) for pid in range(16)})
    processors = build_processors(budget, handler, pool, range(16))
    for proc in processors:
        for offset in range(3):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
        proc.start()
    await settle_all(processors)

    assert budget.peak_tasks <= budget.max_tasks + fan_out
    assert (budget.tasks, budget.reserved_tasks) == (0, 0)


class SmallFirstHandler(SlowArrangeHandler):
    """The worker's first message (partition 0, offset 0) fans out ``small`` tasks; every other ``fan_out``."""

    def __init__(self, small: int, fan_out: int) -> None:
        super().__init__(fan_out)
        self.small = small

    async def arrange(self, messages, pending):
        await asyncio.sleep(0.01)
        tasks = []
        for msg in messages:
            count = self.small if (msg.partition, msg.offset) == (0, 0) else self.fan_out
            tasks += [make_task(f'p{msg.partition}-m{msg.offset}-t{i}', msg.offset) for i in range(count)]
        return tasks


async def run_small_first(pool, budget, handler, *, window_size: int, partitions: int, messages: int) -> None:
    """Measure the worker's first message alone, then give every partition ``messages`` more."""
    first = PartitionProcessor(
        partition_id=0, handler=handler, executor_pool=pool, window_size=window_size, budget=budget
    )
    first.enqueue(make_msg(0, partition=0))
    first.start()
    await wait_for(lambda: first.queue_size == 0 and not first.offset_tracker.has_pending())
    others = [
        PartitionProcessor(
            partition_id=pid, handler=handler, executor_pool=pool, window_size=window_size, budget=budget
        )
        for pid in range(1, partitions)
    ]
    for proc in [first, *others]:
        for offset in range(1, messages + 1):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
    for proc in others:
        proc.start()
    await settle_all([first, *others])


async def test_small_first_measured_message_keeps_15_partitions_within_the_cap_plus_one_message(pool):
    """A worker average of 1 task per message must not let every loop take a full window on the same room."""
    fan_out = 100
    budget = PeakBudget(max_tasks=200, max_bytes=1 << 40)

    await run_small_first(
        pool, budget, SmallFirstHandler(small=1, fan_out=fan_out), window_size=10, partitions=16, messages=3
    )

    assert budget.peak_tasks <= budget.max_tasks + fan_out
    assert (budget.tasks, budget.reserved_tasks) == (0, 0)


async def test_jump_after_the_first_measured_message_stays_within_the_cap_plus_one_message(pool):
    """First message 10 tasks, the next ones 100: the warm-up keeps windows at one message, not cap / 10."""
    fan_out = 100
    budget = PeakBudget(max_tasks=200, max_bytes=1 << 40)

    await run_small_first(
        pool, budget, SmallFirstHandler(small=10, fan_out=fan_out), window_size=100, partitions=4, messages=4
    )

    assert budget.peak_tasks <= budget.max_tasks + fan_out
    assert (budget.tasks, budget.reserved_tasks) == (0, 0)


class SmallThenLargeHandler(SlowArrangeHandler):
    """Offsets below ``small_until`` fan out one task; later offsets fan out ``fan_out`` tasks."""

    def __init__(self, fan_out: int, small_until: int) -> None:
        super().__init__(fan_out)
        self.small_until = small_until

    async def arrange(self, messages, pending):
        await asyncio.sleep(0.01)
        tasks = []
        for msg in messages:
            count = 1 if msg.offset < self.small_until else self.fan_out
            tasks += [make_task(f'p{msg.partition}-m{msg.offset}-t{i}', msg.offset) for i in range(count)]
        return tasks


async def test_shape_jump_after_the_warm_up_passes_the_cap_by_at_most_one_window(pool):
    """After a run of 1-task messages every partition gets 100-task messages at once.

    The floor of max_pending_tasks / window_size tasks per message makes a full
    window reserve the whole cap, so one window passes on the low average, not
    one window per partition.
    """
    fan_out, window_size, warm_up = 100, 10, partition_module.WARM_UP_WINDOWS
    budget = PeakBudget(max_tasks=200, max_bytes=1 << 40)
    handler = SmallThenLargeHandler(fan_out, small_until=warm_up)
    processors = [
        PartitionProcessor(
            partition_id=pid, handler=handler, executor_pool=pool, window_size=window_size, budget=budget
        )
        for pid in range(8)
    ]
    for proc in processors:
        for offset in range(warm_up):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
        proc.start()
    await wait_for(lambda: all(p.queue_size == 0 and not p.offset_tracker.has_pending() for p in processors))
    assert all(not p._in_warm_up() for p in processors)

    for proc in processors:
        for offset in range(warm_up, warm_up + window_size):
            proc.enqueue(make_msg(offset, partition=proc.partition_id))
    await settle_all(processors)

    assert budget.peak_tasks <= budget.max_tasks + window_size * fan_out
    assert (budget.tasks, budget.reserved_tasks) == (0, 0)


def test_message_shape_average_stays_high_after_a_run_of_small_windows():
    shape = MessageShape()
    shape.note_window(message_count=1, task_count=1000, stdin_bytes=0)
    for _ in range(10):
        shape.note_window(message_count=1, task_count=1, stdin_bytes=0)

    assert shape.tasks > 500


def test_message_shape_average_rises_fast_on_a_larger_window():
    shape = MessageShape()
    shape.note_window(message_count=1, task_count=10, stdin_bytes=0)
    shape.note_window(message_count=1, task_count=1000, stdin_bytes=0)

    assert shape.tasks == pytest.approx(505.0)


async def test_window_without_tasks_does_not_measure_the_averages(pool):
    budget = big_budget()
    handler = SkipFirstHandler(fan_out=1, skipped={(0, 0)})
    proc = build_processors(budget, handler, pool, [0])[0]
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert not proc._shape.measured
    assert not budget.shape.measured
    assert not budget.unmeasured_window_open


async def test_a_raising_arrange_gives_back_its_reservation(pool):
    class Raising(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            raise RuntimeError('arrange broke')

    budget = big_budget()
    budget.shape.note_window(message_count=1, task_count=10, stdin_bytes=1000)
    proc = PartitionProcessor(partition_id=0, handler=Raising(), executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: proc.queue_size == 0 and proc._shape.tasks is None and budget.reserved_tasks == 0)
    # The offset of the failed window stays pending by design, so stop does not wait for it.
    await proc.stop(timeout=0.1)

    assert (budget.reserved_tasks, budget.reserved_bytes) == (0, 0)


# ------------------------------------------------------------ stdin release


async def test_stdin_is_released_after_success_and_message_hooks_see_none(pool):
    handler = FanOutHandler(fan_out=2)
    budget = big_budget()
    handler.budget = budget
    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.stdin_in_on_task_complete == [STDIN, STDIN]
    assert handler.stdin_in_message_hook == [None, None]
    assert handler.stdin_in_window_hook == [None, None]


async def test_stdin_is_released_after_a_terminal_failure_but_kept_for_on_error_and_the_retry(pool):
    seen_in_on_error: list[str | None] = []

    class RetryOnce(FanOutHandler):
        async def on_error(self, task, error):
            seen_in_on_error.append(task.stdin)
            return ErrorAction.RETRY if len(seen_in_on_error) == 1 else ErrorAction.SKIP

    handler = RetryOnce(fan_out=1, exit_code=1)
    proc = PartitionProcessor(
        partition_id=0, handler=handler, executor_pool=pool, window_size=10, max_retries=3, budget=big_budget()
    )
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert seen_in_on_error == [STDIN, STDIN]
    assert handler.stdin_in_message_hook == [None]


async def test_stdin_is_released_from_a_replaced_task_and_its_replacement(pool):
    replacements: list[ExecutorTask] = []

    class ReplaceOnce(FanOutHandler):
        async def arrange(self, messages, pending):
            return [make_task('orig', messages[0].offset, exit_code=1)]

        async def on_error(self, task, error):
            replacement = make_task('repl', task.source_offsets[0], stdin=task.stdin + '!')
            replacements.append(replacement)
            return [replacement]

    budget = big_budget()
    handler = ReplaceOnce()
    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.stdin_in_on_task_complete == [STDIN + '!']
    assert handler.stdin_in_message_hook == [None, None]
    assert replacements[0].stdin is None
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_duplicate_task_ids_give_back_each_charge(pool):
    class Duplicates(FanOutHandler):
        async def arrange(self, messages, pending):
            return [make_task('same', messages[0].offset), make_task('same', messages[0].offset, stdin='z' * 10)]

    budget = big_budget()
    proc = PartitionProcessor(partition_id=0, handler=Duplicates(), executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert proc._task_holds == {}
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_replacement_that_keeps_its_parent_id_gives_back_each_charge(pool):
    class ReplaceWithSameId(FanOutHandler):
        async def arrange(self, messages, pending):
            return [make_task('orig', messages[0].offset, exit_code=1)]

        async def on_error(self, task, error):
            return [make_task('orig', task.source_offsets[0], stdin='small')]

    budget = big_budget()
    proc = PartitionProcessor(
        partition_id=0, handler=ReplaceWithSameId(), executor_pool=pool, window_size=10, budget=budget
    )
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert proc._task_holds == {}
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_task_object_scheduled_twice_by_arrange_keeps_its_stdin_for_both_runs(pool):
    class Twice(FanOutHandler):
        async def arrange(self, messages, pending):
            task = make_task('twice', messages[0].offset)
            return [task, task]

    budget = big_budget()
    handler = Twice()
    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.stdin_in_on_task_complete == [STDIN, STDIN]
    assert handler.stdin_in_message_hook == [None, None]
    assert proc._task_holds == {}
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_task_object_returned_by_on_error_as_its_own_replacement_keeps_its_stdin(pool):
    class FixAndResubmit(FanOutHandler):
        async def arrange(self, messages, pending):
            return [make_task('fixable', messages[0].offset, exit_code=1)]

        async def on_error(self, task, error):
            task.precomputed = PrecomputedResult(stdout=STDOUT, exit_code=0)
            return [task]

    budget = big_budget()
    handler = FixAndResubmit()
    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.stdin_in_on_task_complete == [STDIN]
    assert handler.stdin_in_message_hook == [None, None]
    assert proc._task_holds == {}
    assert (budget.tasks, budget.bytes) == (0, 0)


# ------------------------------------------------------------ pending bytes


async def test_pending_bytes_count_stdin_and_kept_output_and_return_to_zero(pool):
    budget = big_budget()
    stdin_seen: list[int] = []

    class Measure(FanOutHandler):
        async def on_task_complete(self, result):
            stdin_seen.append(budget.bytes)
            return None

    handler = Measure(fan_out=3)
    handler.budget = budget
    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert max(stdin_seen) >= sys.getsizeof(STDIN)
    # In on_message_complete the stdin is released, the kept output is not:
    # three results kept by the message tracker and by the window.
    assert handler.bytes_in_message_hook == [2 * 3 * output_bytes(MagicMock(stdout=STDOUT, stderr=''))]
    assert (budget.tasks, budget.bytes) == (0, 0)


def test_released_kept_output_feeds_the_byte_estimate_per_message():
    budget = PendingBudget(max_tasks=100, max_bytes=10_000)
    proc = idle_processor(budget, window_size=50)
    proc._note_window_shape(message_count=1, task_count=1, stdin_bytes=100)
    proc._windows_seen = partition_module.WARM_UP_WINDOWS
    window = partition_module.Window(window_id=1, source_messages=[make_msg(0), make_msg(1)])
    tracker = partition_module.MessageTracker(source_message=make_msg(0))
    proc._account.charge(0, 1300)
    window.kept_bytes = 1000
    tracker.kept_bytes = 300

    proc._release_kept_bytes(window)
    proc._release_kept_bytes(tracker)

    assert proc._shape.held_bytes == pytest.approx(100 + 500 + 300)
    assert budget.shape.held_bytes == pytest.approx(100 + 500 + 300)
    assert proc._admitted_window_size() == 11
    assert budget.bytes == 0


async def test_stalled_tracker_gives_back_its_kept_bytes(pool):
    budget = big_budget()

    class Stall(FanOutHandler):
        async def on_task_complete(self, result):
            # As a failed sink delivery of the task's result does.
            proc._mark_offsets_delivery_failed(result.task.source_offsets)
            return None

    proc = PartitionProcessor(
        partition_id=0, handler=Stall(fan_out=1), executor_pool=pool, window_size=10, budget=budget
    )
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: 0 in proc._stalled_offsets)
    await proc.stop()

    assert proc._message_trackers[0].results == []
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_stop_gives_back_the_budget_of_cancelled_work(pool):
    budget = big_budget()
    proc = idle_processor(budget)
    proc._account.charge(3, 300)

    await proc.stop()

    assert (budget.tasks, budget.bytes) == (0, 0)


# ------------------------------------------------------- Kafka pause rule


async def run_poll_turns(source, turns: int) -> None:
    calls = 0

    async def poll(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls >= turns:
            source.signal_stop()
        return []

    source.consumer.poll_batch = poll
    await source.run()
    assert calls == turns


@pytest.fixture
def kafka_config(tmp_path) -> DrakkarConfig:
    return DrakkarConfig(
        connections=kafka_connections(),
        storage={'dir': str(tmp_path / 'storage')},
        sources={'kafka': {'enabled': True}},
        executor={'binary_path': '/bin/echo', 'max_executors': 2},
        metrics={'enabled': False},
    )


def kafka_app(config: DrakkarConfig, max_queued_messages: int) -> DrakkarApp:
    config.sources.kafka.max_queued_messages = max_queued_messages
    config.ui.release.enabled = False
    app = DrakkarApp(handler=SimpleHandler(), config=config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    wire_kafka_source(app, AsyncMock())
    return app


def add_processor(app: DrakkarApp, pid: int, queued: int) -> PartitionProcessor:
    proc = idle_processor(None)
    proc._partition_id = pid
    for offset in range(queued):
        proc._queue.put_nowait(make_msg(offset, partition=pid))
    app._processors[pid] = proc
    return proc


async def test_poll_loop_pauses_at_max_queued_messages_and_keeps_polling(kafka_config):
    app = kafka_app(kafka_config, max_queued_messages=10)
    add_processor(app, 0, queued=10)

    await run_poll_turns(app.kafka_source, turns=3)

    app._consumer.pause.assert_awaited_once_with([0])
    assert app._paused


async def test_poll_loop_resumes_at_half_but_not_above_it(kafka_config):
    app = kafka_app(kafka_config, max_queued_messages=10)
    proc = add_processor(app, 0, queued=6)
    app._paused = True

    await run_poll_turns(app.kafka_source, turns=1)
    assert app._paused
    app._consumer.resume.assert_not_awaited()

    proc._queue.get_nowait()
    app.kafka_source._stop_requested = False
    await run_poll_turns(app.kafka_source, turns=1)

    app._consumer.resume.assert_awaited_once_with([0])
    assert not app._paused


async def test_poll_loop_never_resumes_a_stalled_or_dead_partition(kafka_config):
    app = kafka_app(kafka_config, max_queued_messages=10)
    add_processor(app, 0, queued=0)
    add_processor(app, 1, queued=0)
    dead = add_processor(app, 2, queued=0)
    dead._dead = True
    app._stalled_partitions.add(1)
    app._paused = True

    await run_poll_turns(app.kafka_source, turns=1)

    app._consumer.pause.assert_awaited_once_with([2])
    app._consumer.resume.assert_awaited_once_with([0])


async def test_poll_loop_does_not_count_a_dead_partition_queue(kafka_config):
    app = kafka_app(kafka_config, max_queued_messages=10)
    dead = add_processor(app, 0, queued=50)
    dead._dead = True

    await run_poll_turns(app.kafka_source, turns=2)

    assert app.kafka_source.queued_messages() == 0
    assert not app._paused
    # Paused once for its death, never for backpressure.
    app._consumer.pause.assert_awaited_once_with([0])


async def test_dead_partition_pause_failure_is_retried_on_the_next_turn(kafka_config):
    app = kafka_app(kafka_config, max_queued_messages=10)
    dead = add_processor(app, 0, queued=0)
    dead._dead = True
    app._consumer.pause.side_effect = [RuntimeError('broker gone'), None]

    await run_poll_turns(app.kafka_source, turns=2)

    assert app._consumer.pause.await_count == 2
    assert app.kafka_source.dead_partitions_paused == {0}


# ------------------------------------------------------------ HTTP source


async def test_http_request_counts_its_tasks_and_releases_stdin_and_budget(pool):
    budget = big_budget()
    handler = _RecordingHandler()
    seen_running: list[int] = []
    seen_stdin: list[str | None] = []

    async def arrange_impl(req, pending):
        return [make_task('h1', 0), make_task('h2', 0)]

    async def complete_impl(group):
        seen_running.append(budget.tasks)
        seen_stdin.extend(task.stdin for task in group.tasks)
        return None

    handler.arrange_http_request_impl = arrange_impl
    handler.on_http_request_complete_impl = complete_impl
    app = _make_stub_app(handler, pool=pool)
    app._pending_budget = budget
    runner = WebappRunner(app, _make_config())

    await runner.run(_make_ctx())

    assert seen_running == [0]
    assert seen_stdin == [None, None]
    assert (budget.tasks, budget.bytes) == (0, 0)


# ------------------------------------------------------------------ metrics


def test_backpressure_metrics_names_and_help_texts():
    from prometheus_client import Counter, Gauge, Histogram

    from drakkar.metrics import backpressure_active, queued_messages_dropped_at_stop

    expected = [
        (
            pending_tasks,
            Gauge,
            'drakkar_pending_tasks',
            (),
            'Tasks created and not yet settled: waiting for an executor slot, running, '
            'or in on_task_complete and its delivery',
        ),
        (
            pending_bytes,
            Gauge,
            'drakkar_pending_bytes',
            (),
            'Bytes held for pending tasks and kept results: task stdin plus stdout and stderr '
            'kept for on_message_complete and on_window_complete',
        ),
        (
            window_admission_wait,
            Counter,
            'drakkar_window_admission_wait_seconds',
            ('limit',),
            'Seconds partition loops waited for room under executor.max_pending_tasks or '
            'executor.max_pending_bytes before taking a window',
        ),
        (window_messages, Histogram, 'drakkar_window_messages', (), 'Messages taken into one arrange() window'),
        (
            queued_messages_dropped_at_stop,
            Counter,
            'drakkar_queued_messages_dropped_at_stop',
            (),
            'Polled messages left unarranged at stop or revoke; the next owner reads them again',
        ),
        (
            backpressure_active,
            Gauge,
            'drakkar_backpressure_active',
            (),
            'Whether Kafka fetching is paused because queued messages reached sources.kafka.max_queued_messages '
            '(1=paused, 0=flowing)',
        ),
    ]
    for metric, kind, name, labels, help_text in expected:
        assert isinstance(metric, kind)
        assert metric._name == name
        assert metric._labelnames == labels
        assert metric._documentation == help_text
    assert window_messages._upper_bounds[:-1] == [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
