"""Tests for Drakkar partition processor."""

import asyncio
import sys

import pytest

from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    OutputMessage,
    SourceMessage,
)
from drakkar.partition import MAX_RETRIES, PartitionProcessor, Window
from tests.conftest import wait_for


def make_msg(partition: int = 0, offset: int = 0) -> SourceMessage:
    return SourceMessage(
        topic='test',
        partition=partition,
        offset=offset,
        value=b'{"x": 1}',
        timestamp=1000,
    )


class EchoHandler(BaseDrakkarHandler):
    def __init__(self):
        self.arrange_calls: list[tuple[int, int]] = []
        self.collect_calls: list[str] = []
        self.window_complete_calls: list[int] = []
        self.error_calls: list[str] = []

    async def arrange(self, messages, pending):
        self.arrange_calls.append((len(messages), len(pending.pending_task_ids)))
        return [
            ExecutorTask(
                task_id=f'task-{msg.offset}',
                args=['hello'],
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
    async def arrange(self, messages, pending):
        return []


class ErrorHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f'fail-{msg.offset}',
                args=['-c', 'import sys; sys.exit(1)'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


@pytest.fixture
def echo_pool() -> ExecutorPool:
    return ExecutorPool(
        binary_path='/bin/echo',
        max_workers=4,
        task_timeout_seconds=10,
    )


@pytest.fixture
def failing_pool() -> ExecutorPool:
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
    assert proc.inflight_count == 0

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
    await wait_for(lambda: len(handler.collect_calls) == 3)
    await proc.stop()

    assert len(handler.arrange_calls) >= 1
    assert len(handler.window_complete_calls) >= 1
    assert len(collected) >= 3
    assert any(c[1] == 3 for c in committed)


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
    await wait_for(lambda: any(c[1] == 12 for c in committed))
    await proc.stop()


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
    await wait_for(
        lambda: not proc.offset_tracker.has_pending() and proc.inflight_count == 0, timeout=3
    )
    await proc.stop()


async def test_partition_processor_pending_context(echo_pool):
    pending_sizes = []

    class TrackingHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            pending_sizes.append(len(pending.pending_task_ids))
            return [
                ExecutorTask(
                    task_id=f'task-{messages[0].offset}',
                    args=['slow'],
                    source_offsets=[msg.offset for msg in messages],
                )
            ]

    handler = TrackingHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )

    for i in range(3):
        proc.enqueue(make_msg(offset=i))

    proc.start()
    await wait_for(lambda: len(pending_sizes) >= 3)
    await proc.stop()

    assert pending_sizes[0] == 0


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


async def test_partition_processor_no_callbacks(echo_pool):
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(handler.collect_calls) == 1)
    await proc.stop()


# --- C1: RETRY should not stall the window ---


async def test_retry_does_not_stall_window(failing_pool):
    """When on_error returns RETRY then SKIP, the window still completes
    and offsets are committed. (Fix for C1: RETRY early return bug)
    """
    call_count = 0
    committed: list[tuple[int, int]] = []

    class RetryThenSkipHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'rt-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

    async def on_commit(partition_id, offset):
        committed.append((partition_id, offset))

    proc = PartitionProcessor(
        partition_id=0,
        handler=RetryThenSkipHandler(),
        executor_pool=failing_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0)
    await proc.stop()

    assert any(c[1] == 1 for c in committed)


# --- I10: Retry limit ---


async def test_max_retries_exceeded(failing_pool):
    """After MAX_RETRIES, task is skipped and window completes."""
    error_count = 0

    class AlwaysRetryHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'inf-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal error_count
            error_count += 1
            return ErrorAction.RETRY

    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=AlwaysRetryHandler(),
        executor_pool=failing_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=10)
    await proc.stop()

    assert error_count == MAX_RETRIES + 1
    assert any(c[1] == 1 for c in committed)


# --- I1: Unhandled exception in collect should not stall window ---


async def test_collect_exception_does_not_stall_window(echo_pool):
    """If collect() raises, the window still completes."""
    committed: list[tuple[int, int]] = []

    class BrokenCollectHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'bc-{m.offset}',
                    args=['ok'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def collect(self, result):
            raise RuntimeError('collect exploded')

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=BrokenCollectHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0)
    await proc.stop()

    assert any(c[1] == 1 for c in committed)


# --- Queued message must not be lost on drain ---


async def test_drain_waits_for_queued_messages(echo_pool):
    """drain() must wait for messages in the queue to be processed,
    not just in-flight tasks. A message enqueued but not yet dequeued
    by the processor must still get committed.
    (Reproduces: one partition lag=1 per worker after all work is done)
    """
    committed: list[tuple[int, int]] = []

    class SimpleHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'dq-{m.offset}',
                    args=['ok'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=SimpleHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )

    # start the processor, let it enter _collect_window
    proc.start()
    await asyncio.sleep(0.1)

    # enqueue messages — they go into the queue
    proc.enqueue(make_msg(offset=50))
    proc.enqueue(make_msg(offset=51))
    proc.enqueue(make_msg(offset=52))

    # immediately signal stop and drain
    proc._running = False
    await asyncio.wait_for(proc.drain(), timeout=5.0)

    # the queued messages should have been processed and committed
    assert proc.queue_size == 0
    assert any(c[1] == 53 for c in committed), f'Expected commit of 53, got: {committed}'
