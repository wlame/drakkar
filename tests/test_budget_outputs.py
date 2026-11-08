"""The pending budget charges a task's output while its hooks and delivery hold it.

Hermetic: tasks carry precomputed results, so the executor pool never spawns
a subprocess. Every assertion reads the budget's byte count.
"""

import asyncio
import sys

import pytest
from pydantic import BaseModel

from drakkar.budget import MessageShape, PendingBudget
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import CollectResult, ErrorAction, KafkaPayload
from drakkar.partition import PartitionProcessor
from tests.conftest import wait_for
from tests.test_backpressure import (
    STDIN,
    STDOUT,
    big_budget,
    make_msg,
    make_task,
    run_until_settled,
)

STDIN_BYTES = sys.getsizeof(STDIN)
# A precomputed result has STDOUT on stdout and an empty stderr.
OUTPUT_BYTES = sys.getsizeof(STDOUT) + sys.getsizeof('')


@pytest.fixture
def pool() -> ExecutorPool:
    return ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)


class _Out(BaseModel):
    v: str


class OneTaskHandler(BaseDrakkarHandler):
    """One task per message; no message or window hook, so nothing is kept."""

    def __init__(self, budget: PendingBudget, exit_code: int = 0) -> None:
        super().__init__()
        self.budget = budget
        self.exit_code = exit_code
        self.bytes_in_on_task_complete: list[int] = []
        self.bytes_in_on_error: list[int] = []

    async def arrange(self, messages, pending):
        return [make_task(f'm{msg.offset}', msg.offset, exit_code=self.exit_code) for msg in messages]

    async def on_task_complete(self, result):
        self.bytes_in_on_task_complete.append(self.budget.bytes)
        return None

    async def on_error(self, task, error):
        self.bytes_in_on_error.append(self.budget.bytes)
        return ErrorAction.SKIP


def processor(handler: BaseDrakkarHandler, budget: PendingBudget, pool, **kwargs) -> PartitionProcessor:
    return PartitionProcessor(
        partition_id=0, handler=handler, executor_pool=pool, window_size=10, budget=budget, **kwargs
    )


async def test_output_is_charged_during_on_task_complete_and_delivery_and_released_after(pool):
    budget = big_budget()
    bytes_in_delivery: list[int] = []

    class Deliver(OneTaskHandler):
        async def on_task_complete(self, result):
            await super().on_task_complete(result)
            return CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])

    async def on_collect(result, partition_id, message_context=None):
        bytes_in_delivery.append(budget.bytes)

    handler = Deliver(budget)
    proc = processor(handler, budget, pool, on_collect=on_collect)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_task_complete == [STDIN_BYTES + OUTPUT_BYTES]
    assert bytes_in_delivery == [STDIN_BYTES + OUTPUT_BYTES]
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_output_of_a_failed_task_is_charged_during_on_error_and_released_after(pool):
    budget = big_budget()
    handler = OneTaskHandler(budget, exit_code=1)
    proc = processor(handler, budget, pool)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_error == [STDIN_BYTES + OUTPUT_BYTES]
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_output_of_a_task_whose_delivery_raises_is_released(pool):
    budget = big_budget()

    class Deliver(OneTaskHandler):
        async def on_task_complete(self, result):
            return CollectResult(kafka=[KafkaPayload(data=_Out(v='x'))])

    async def on_collect(result, partition_id, message_context=None):
        raise RuntimeError('delivery state unknown')

    proc = processor(Deliver(budget), budget, pool, on_collect=on_collect)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_result_kept_for_the_window_moves_its_charge_without_a_second_count(pool):
    budget = big_budget()
    bytes_in_window_hook: list[int] = []

    class KeepForWindow(OneTaskHandler):
        async def on_window_complete(self, results, source_messages):
            bytes_in_window_hook.append(budget.bytes)
            return None

    handler = KeepForWindow(budget)
    proc = processor(handler, budget, pool)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_task_complete == [STDIN_BYTES + OUTPUT_BYTES]
    # The stdin is freed when the task settles; the kept output alone remains.
    assert bytes_in_window_hook == [OUTPUT_BYTES]
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_failed_result_kept_for_the_window_moves_its_charge(pool):
    budget = big_budget()
    bytes_in_window_hook: list[int] = []

    class KeepForWindow(OneTaskHandler):
        async def on_window_complete(self, results, source_messages):
            bytes_in_window_hook.append(budget.bytes)
            return None

    proc = processor(KeepForWindow(budget, exit_code=1), budget, pool)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert bytes_in_window_hook == [OUTPUT_BYTES]
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_retry_releases_the_output_of_each_run_once(pool):
    budget = big_budget()

    class RetryOnce(OneTaskHandler):
        async def on_error(self, task, error):
            self.bytes_in_on_error.append(self.budget.bytes)
            return ErrorAction.RETRY if len(self.bytes_in_on_error) == 1 else ErrorAction.SKIP

    handler = RetryOnce(budget, exit_code=1)
    proc = processor(handler, budget, pool, max_retries=3)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_error == [STDIN_BYTES + OUTPUT_BYTES] * 2
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_on_error_replacement_releases_the_failed_output_once(pool):
    budget = big_budget()
    replacement_stdin = 'r' * 300

    class Replace(OneTaskHandler):
        async def on_error(self, task, error):
            self.bytes_in_on_error.append(self.budget.bytes)
            return [make_task('repl', task.source_offsets[0], stdin=replacement_stdin)]

    handler = Replace(budget, exit_code=1)
    proc = processor(handler, budget, pool)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_error == [STDIN_BYTES + OUTPUT_BYTES]
    # The failed task's stdin and output are gone; the replacement holds its own.
    assert handler.bytes_in_on_task_complete == [sys.getsizeof(replacement_stdin) + OUTPUT_BYTES]
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_cancelled_task_releases_its_output(pool):
    budget = big_budget()
    entered = asyncio.Event()

    class Block(OneTaskHandler):
        async def on_task_complete(self, result):
            await super().on_task_complete(result)
            entered.set()
            await asyncio.Event().wait()

    handler = Block(budget)
    proc = processor(handler, budget, pool)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(entered.is_set)

    assert budget.bytes == STDIN_BYTES + OUTPUT_BYTES
    # Checked before stop(), whose account close would hide a leak.
    await proc.cancel_active_tasks()

    assert (budget.tasks, budget.bytes) == (0, 0)
    await proc.stop()


async def test_mixed_run_returns_the_budget_to_zero(pool):
    budget = big_budget()

    class Mixed(BaseDrakkarHandler):
        def __init__(self) -> None:
            super().__init__()
            self.errors = 0

        async def arrange(self, messages, pending):
            return [make_task(f'm{msg.offset}-t{i}', msg.offset, exit_code=i % 2) for msg in messages for i in range(4)]

        async def on_error(self, task, error):
            self.errors += 1
            if self.errors % 3 == 0:
                return [make_task(f'{task.task_id}-r', task.source_offsets[0])]
            return ErrorAction.RETRY if self.errors % 3 == 1 else ErrorAction.SKIP

        async def on_message_complete(self, group):
            return None

        async def on_window_complete(self, results, source_messages):
            return None

    proc = processor(Mixed(), budget, pool, max_retries=1)
    for offset in range(5):
        proc.enqueue(make_msg(offset))

    await run_until_settled(proc)

    assert (budget.tasks, budget.bytes, budget.reserved_tasks, budget.reserved_bytes) == (0, 0, 0, 0)


async def test_output_held_by_task_hooks_does_not_feed_the_byte_estimate(pool):
    budget = big_budget()
    handler = OneTaskHandler(budget)
    proc = processor(handler, budget, pool)
    proc.enqueue(make_msg(0))

    await run_until_settled(proc)

    assert handler.bytes_in_on_task_complete == [STDIN_BYTES + OUTPUT_BYTES]
    assert proc._shape.held_bytes == pytest.approx(STDIN_BYTES)
    assert budget.shape.held_bytes == pytest.approx(STDIN_BYTES)


def test_message_shape_estimate_is_stdin_plus_kept_output():
    shape = MessageShape()
    shape.note_window(message_count=2, task_count=4, stdin_bytes=200)
    shape.note_window_kept(message_count=2, kept_bytes=600)

    assert shape.held_bytes == 100 + 300
