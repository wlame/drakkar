"""Tests for Drakkar partition processor."""

import asyncio

import pytest

from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor, Window


def make_msg(partition: int = 0, offset: int = 0) -> SourceMessage:
    return SourceMessage(
        topic="test",
        partition=partition,
        offset=offset,
        value=b'{"x": 1}',
        timestamp=1000,
    )


class EchoHandler(BaseDrakkarHandler):
    """Test handler that creates one task per message using echo."""

    def __init__(self):
        self.arrange_calls: list[tuple[int, int]] = []
        self.collect_calls: list[str] = []
        self.window_complete_calls: list[int] = []
        self.error_calls: list[str] = []

    async def arrange(self, messages, pending):
        self.arrange_calls.append((len(messages), len(pending.pending_task_ids)))
        return [
            ExecutorTask(
                task_id=f"task-{msg.offset}",
                args=["hello"],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]

    async def collect(self, result):
        self.collect_calls.append(result.task_id)
        return CollectResult(
            output_messages=[OutputMessage(value=result.stdout.encode())],
        )

    async def on_window_complete(self, results, source_messages):
        self.window_complete_calls.append(len(results))
        return None

    async def on_error(self, task, error):
        self.error_calls.append(task.task_id)
        return ErrorAction.SKIP


class EmptyArrangeHandler(BaseDrakkarHandler):
    """Handler that returns no tasks from arrange."""

    async def arrange(self, messages, pending):
        return []


class ErrorHandler(BaseDrakkarHandler):
    """Handler that creates tasks with a failing binary."""

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f"fail-{msg.offset}",
                args=["-c", "import sys; sys.exit(1)"],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


@pytest.fixture
def echo_pool() -> ExecutorPool:
    return ExecutorPool(
        binary_path="/bin/echo",
        max_workers=4,
        task_timeout_seconds=10,
    )


@pytest.fixture
def failing_pool() -> ExecutorPool:
    import sys
    return ExecutorPool(
        binary_path=sys.executable,
        max_workers=4,
        task_timeout_seconds=10,
    )


def test_window_is_complete():
    window = Window(window_id=1, source_messages=[], total_tasks=3, completed_count=3)
    assert window.is_complete

    window2 = Window(window_id=2, source_messages=[], total_tasks=3, completed_count=2)
    assert not window2.is_complete


def test_window_empty_tasks_not_complete():
    window = Window(window_id=1, source_messages=[], total_tasks=0, completed_count=0)
    assert not window.is_complete


async def test_partition_processor_enqueue_and_properties(echo_pool):
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=5,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
    )

    assert proc.partition_id == 5
    assert proc.queue_size == 0

    proc.enqueue(make_msg(partition=5, offset=100))
    assert proc.queue_size == 1


async def test_partition_processor_processes_messages(echo_pool):
    handler = EchoHandler()
    collected: list[CollectResult] = []
    committed: list[tuple[int, int]] = []

    async def on_collect(result, partition_id):
        collected.append(result)

    async def on_commit(partition_id, offset):
        committed.append((partition_id, offset))

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_collect=on_collect,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.enqueue(make_msg(offset=2))

    proc.start()
    await asyncio.sleep(0.5)
    await proc.stop()

    assert len(handler.arrange_calls) >= 1
    assert len(handler.collect_calls) == 3
    assert len(handler.window_complete_calls) >= 1
    assert len(collected) >= 3  # per-task collects
    assert any(c[1] == 3 for c in committed)  # committed offset 3 (next after 0,1,2)


async def test_partition_processor_empty_arrange(echo_pool):
    handler = EmptyArrangeHandler()
    committed: list[tuple[int, int]] = []

    async def on_commit(partition_id, offset):
        committed.append((partition_id, offset))

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=10))
    proc.enqueue(make_msg(offset=11))

    proc.start()
    await asyncio.sleep(0.3)
    await proc.stop()

    # offsets should be committed since arrange returned no tasks
    assert any(c[1] == 12 for c in committed)


async def test_partition_processor_error_handling(failing_pool):
    handler = ErrorHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=failing_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await asyncio.sleep(0.5)
    await proc.stop()


async def test_partition_processor_pending_context(echo_pool):
    """Verify that arrange receives pending task context."""
    pending_sizes = []

    class TrackingHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            pending_sizes.append(len(pending.pending_task_ids))
            return [
                ExecutorTask(
                    task_id=f"task-{messages[0].offset}",
                    args=["slow"],
                    source_offsets=[msg.offset for msg in messages],
                )
            ]

    handler = TrackingHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,  # one message per window
    )

    for i in range(3):
        proc.enqueue(make_msg(offset=i))

    proc.start()
    await asyncio.sleep(0.5)
    await proc.stop()

    assert len(pending_sizes) >= 1
    assert pending_sizes[0] == 0  # first window has no pending tasks


async def test_partition_processor_stop_and_drain(echo_pool):
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=5,
    )

    proc.start()
    await asyncio.sleep(0.1)
    await proc.stop()
    # should not raise


async def test_partition_processor_no_callbacks(echo_pool):
    """Processor works without callbacks (just doesn't produce/commit)."""
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await asyncio.sleep(0.3)
    await proc.stop()

    assert len(handler.collect_calls) == 1
