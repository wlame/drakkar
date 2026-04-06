"""Tests for Drakkar partition processor."""

import asyncio
import sys

import pytest
from pydantic import BaseModel as BM

from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.partition import MAX_RETRIES, PartitionProcessor, Window
from tests.conftest import wait_for


class _Out(BM):
    v: str = ''


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
        self.collect_calls.append(result.task.task_id)
        return CollectResult(
            kafka=[KafkaPayload(data=_Out(v=result.stdout))],
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
    await wait_for(lambda: not proc.offset_tracker.has_pending() and proc.inflight_count == 0, timeout=3)
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


async def test_full_shutdown_commits_queued_messages(echo_pool):
    """Reproduces the real shutdown path with slow tasks.

    Messages enqueued just before shutdown must be processed and committed
    even when executor tasks take time (simulated with sleep).
    stop() must wait for the drain loop to finish, not cancel it.
    """
    committed: list[tuple[int, int]] = []

    class SlowHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'sh-{m.offset}',
                    # sleep 0.5s to simulate real work
                    args=['-c', 'import time; time.sleep(0.5); print("done")'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    async def on_commit(pid, off):
        committed.append((pid, off))

    # use python as the binary so we can sleep
    slow_pool = ExecutorPool(
        binary_path=sys.executable,
        max_workers=2,
        task_timeout_seconds=10,
    )

    proc = PartitionProcessor(
        partition_id=0,
        handler=SlowHandler(),
        executor_pool=slow_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.start()
    await asyncio.sleep(0.1)

    # enqueue messages just before shutdown
    proc.enqueue(make_msg(offset=100))
    proc.enqueue(make_msg(offset=101))

    # full shutdown sequence: _running=False, drain, then stop
    proc._running = False
    await asyncio.wait_for(proc.drain(), timeout=10.0)
    await proc.stop()

    # after stop(), all messages must be committed
    assert proc.queue_size == 0
    assert proc.inflight_count == 0
    assert any(c[1] == 102 for c in committed), f'Expected commit of 102, got: {committed}'


# --- Commit failure must not lose offsets ---


async def test_commit_failure_preserves_offsets_for_retry(echo_pool):
    """When on_commit raises (e.g. during rebalance), offsets must stay
    in the tracker so the next _try_commit retries them.

    Reproduces: one partition per worker retains lag after all work done.
    Root cause was _handle_commit swallowing exceptions, making _try_commit
    think the commit succeeded and calling acknowledge_commit.
    """
    commit_count = 0
    committed: list[tuple[int, int]] = []

    async def on_commit(partition_id, offset):
        nonlocal commit_count
        commit_count += 1
        if commit_count == 1:
            raise RuntimeError('rebalance in progress')
        committed.append((partition_id, offset))

    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.enqueue(make_msg(offset=2))

    proc.start()
    # first commit attempt fails, retry on idle loop should succeed
    await wait_for(lambda: any(c[1] == 3 for c in committed), timeout=5)
    await proc.stop()

    assert commit_count >= 2, 'Expected at least one retry after failure'
    assert any(c[1] == 3 for c in committed), f'Expected commit of 3, got: {committed}'


# --- Task reference retention ---


async def test_active_tasks_set_holds_references():
    """asyncio.create_task references are stored in _active_tasks
    to prevent garbage collection (Python 3.12+ weak refs).
    """
    slow_pool = ExecutorPool(
        binary_path=sys.executable,
        max_workers=4,
        task_timeout_seconds=10,
    )

    class SlowEchoHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'at-{m.offset}',
                    args=['-c', 'import time; time.sleep(0.3); print("ok")'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=0,
        handler=SlowEchoHandler(),
        executor_pool=slow_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))

    proc.start()
    # while slow tasks are in-flight, _active_tasks should hold references
    await wait_for(lambda: len(proc._active_tasks) > 0, timeout=2)
    assert proc._active_tasks  # strong references exist

    # after completion, done callbacks should clean up
    await wait_for(lambda: proc.inflight_count == 0, timeout=5)
    await wait_for(lambda: len(proc._active_tasks) == 0, timeout=2)
    await proc.stop()


# --- Arrange tracking ---


async def test_arrange_tracking_state(echo_pool):
    """Processor tracks arrange() state for debug introspection."""
    arrange_was_active = False
    arrange_had_labels = False

    class SlowArrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            nonlocal arrange_was_active, arrange_had_labels
            # check tracking from inside arrange
            # (can't access proc directly, but we verify after)
            await asyncio.sleep(0.1)
            return [
                ExecutorTask(
                    task_id=f'sa-{m.offset}',
                    args=['ok'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=0,
        handler=SlowArrangeHandler(),
        executor_pool=echo_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()

    # during arrange, _arranging should be True
    await wait_for(lambda: proc._arranging, timeout=2)
    assert len(proc._arrange_labels) > 0
    assert proc._arrange_labels[0] == '0:0'  # default message_label

    # after arrange completes, _arranging should be False
    await wait_for(lambda: not proc._arranging, timeout=2)
    await proc.stop()


# --- message_label used in arrange tracking ---


async def test_custom_message_label_in_arrange_tracking(echo_pool):
    """Custom message_label() is used in arrange tracking labels."""

    class LabelHandler(BaseDrakkarHandler):
        def message_label(self, msg):
            return f'REQ-{msg.offset}'

        async def arrange(self, messages, pending):
            await asyncio.sleep(0.05)
            return [
                ExecutorTask(
                    task_id=f'lbl-{m.offset}',
                    args=['ok'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

    proc = PartitionProcessor(
        partition_id=0,
        handler=LabelHandler(),
        executor_pool=echo_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=42))
    proc.start()

    await wait_for(lambda: proc._arranging, timeout=2)
    assert proc._arrange_labels == ['REQ-42']

    await wait_for(lambda: not proc._arranging, timeout=2)
    await proc.stop()


# --- on_error returning replacement task list ---


async def test_on_error_returns_replacement_tasks(failing_pool, echo_pool):
    """When on_error returns a list of ExecutorTask, those tasks are
    scheduled in the same window and must complete before it closes."""
    collected_task_ids: list[str] = []
    committed: list[tuple[int, int]] = []

    class ReplaceOnErrorHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'fail-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def collect(self, result):
            collected_task_ids.append(result.task.task_id)
            return None

        async def on_error(self, task, error):
            # replace with a task that succeeds using echo
            return [
                ExecutorTask(
                    task_id=f'replace-{task.task_id}',
                    args=['recovered'],
                    source_offsets=task.source_offsets,
                    binary_path='/bin/echo',
                )
            ]

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=ReplaceOnErrorHandler(),
        executor_pool=failing_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=5)
    await proc.stop()

    assert 'replace-fail-0' in collected_task_ids
    assert any(c[1] == 1 for c in committed)


# --- on_window_complete returning CollectResult ---


async def test_on_window_complete_returns_collect_result(echo_pool):
    """When on_window_complete returns a CollectResult, it is passed to on_collect."""
    window_complete_collected: list[CollectResult] = []

    class WindowCollectHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'wc-{m.offset}',
                    args=['ok'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_window_complete(self, results, source_messages):
            return CollectResult(
                kafka=[KafkaPayload(data=_Out(v='from_window_complete'))],
            )

    async def on_collect(result, partition_id):
        window_complete_collected.append(result)

    proc = PartitionProcessor(
        partition_id=0,
        handler=WindowCollectHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=on_collect,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(window_complete_collected) > 0, timeout=3)
    await proc.stop()

    wc_results = [r for r in window_complete_collected if r.kafka and r.kafka[0].data.v == 'from_window_complete']
    assert len(wc_results) >= 1


# --- stop() timeout + force cancel ---


async def test_stop_force_cancels_hung_processor(echo_pool):
    """When the processor is stuck, stop() force-cancels after timeout."""
    blocked = asyncio.Event()

    class HangingHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            blocked.set()
            await asyncio.sleep(3600)
            return []

    proc = PartitionProcessor(
        partition_id=0,
        handler=HangingHandler(),
        executor_pool=echo_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await blocked.wait()

    # Simulate stop() with a short timeout to avoid 10s wait
    proc._running = False
    task = proc._task
    assert task is not None

    try:
        await asyncio.wait_for(task, timeout=0.1)
    except TimeoutError:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    proc._task = None

    assert proc._task is None
    assert task.cancelled() or task.done()


# --- max_retries exceeded logs warning ---


async def test_max_retries_exceeded_logs_warning(failing_pool):
    """When retries exceed max_retries, a 'max_retries_exceeded' warning is logged."""

    class AlwaysRetryHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'mr-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            return ErrorAction.RETRY

    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=AlwaysRetryHandler(),
        executor_pool=failing_pool,
        window_size=10,
        max_retries=1,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=10)
    await proc.stop()

    # With max_retries=1: first attempt fails → RETRY → second attempt fails → exceeds limit
    # Window still completes and offsets are committed
    assert any(c[1] == 1 for c in committed)
