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

    async def on_task_complete(self, result):
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
        max_executors=4,
        task_timeout_seconds=10,
    )


@pytest.fixture
def failing_pool() -> ExecutorPool:
    return ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
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


# --- Bug #1: RETRY must not drive _inflight_count negative ---


async def test_retry_inflight_count_does_not_go_negative(failing_pool):
    """Each RETRY must not double-decrement _inflight_count via the finally block.

    Before the fix, the unconditional finally ran on the early return of the
    RETRY branch, popping the pending_tasks entry and decrementing inflight.
    The retry coroutine would then decrement again on its own finally, driving
    the counter below zero. This made drain() exit while retries were still
    pending.
    """
    error_calls = 0

    class RetryOnceThenSkipHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'neg-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal error_calls
            error_calls += 1
            # first call: RETRY; retry also fails → second call: SKIP
            if error_calls == 1:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=RetryOnceThenSkipHandler(),
        executor_pool=failing_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=10)
    await proc.stop()

    assert error_calls == 2
    # After full completion the counter must be exactly 0 — never negative.
    assert proc.inflight_count == 0, f'inflight_count leaked: {proc.inflight_count}'
    # pending_tasks must be empty; the retry should have popped its own entry.
    assert proc._pending_tasks == {}, f'pending leaked: {proc._pending_tasks}'


async def test_retry_exhaustion_leaves_inflight_at_zero(failing_pool):
    """Exhausting MAX_RETRIES must leave inflight_count at exactly 0.

    With the bug, each RETRY→return path double-decremented. For MAX_RETRIES=3
    the counter ended at -3 after a single task fully exhausted retries.
    """

    class AlwaysRetryHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'ex-{m.offset}',
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
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=10)
    await proc.stop()

    assert proc.inflight_count == 0, f'inflight_count leaked: {proc.inflight_count}'
    assert proc._pending_tasks == {}


async def test_retry_keeps_inflight_positive_during_retry_chain(failing_pool):
    """While retries are still pending, inflight_count must stay at >=1.

    Before the fix, the finally decrement ran on the RETRY early return, so
    between the original's exit and the retry coroutine acquiring the
    executor slot, inflight_count could drop to 0 or negative. drain() would
    then observe the zero and exit while the retry was still about to run.
    """
    inflight_observations: list[int] = []
    errors_seen = 0

    class ObservingRetryHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'obs-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal errors_seen
            errors_seen += 1
            # snapshot the counter AT the moment on_error runs — before the
            # RETRY branch schedules its successor and the finally would run.
            inflight_observations.append(proc.inflight_count)
            if errors_seen < 3:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

    proc = PartitionProcessor(
        partition_id=0,
        handler=ObservingRetryHandler(),
        executor_pool=failing_pool,
        window_size=10,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: errors_seen >= 3, timeout=10)
    await proc.stop()

    # Every observation must see inflight >= 1 (the task currently failing)
    assert all(v >= 1 for v in inflight_observations), f'inflight dropped during retry chain: {inflight_observations}'
    assert proc.inflight_count == 0
    assert proc._pending_tasks == {}


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

        async def on_task_complete(self, result):
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
        max_executors=2,
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
        max_executors=4,
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

        async def on_task_complete(self, result):
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


# --- on_error SKIP with mixed success/failure window ---


async def test_skip_in_mixed_window():
    """In a window with multiple tasks, a SKIP'd failure doesn't block
    successful tasks: collect() runs for successes, offsets commit for all."""
    collected_ids: list[str] = []
    committed: list[tuple[int, int]] = []
    error_ids: list[str] = []

    class MixedHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for msg in messages:
                tasks.append(
                    ExecutorTask(
                        task_id=f'mix-{msg.offset}',
                        args=['-c', 'import sys; sys.exit(1)'] if msg.offset == 2 else ['-c', 'print("ok")'],
                        source_offsets=[msg.offset],
                    )
                )
            return tasks

        async def on_task_complete(self, result):
            collected_ids.append(result.task.task_id)
            return None

        async def on_error(self, task, error):
            error_ids.append(task.task_id)
            return ErrorAction.SKIP

    async def on_commit(pid, off):
        committed.append((pid, off))

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
        task_timeout_seconds=10,
    )

    proc = PartitionProcessor(
        partition_id=0,
        handler=MixedHandler(),
        executor_pool=pool,
        window_size=10,
        on_commit=on_commit,
    )

    for i in range(5):
        proc.enqueue(make_msg(offset=i))
    proc.start()
    await wait_for(lambda: len(committed) > 0, timeout=10)
    await proc.stop()

    # collect() called for 4 successes, NOT for offset 2
    assert 'mix-0' in collected_ids
    assert 'mix-1' in collected_ids
    assert 'mix-3' in collected_ids
    assert 'mix-4' in collected_ids
    assert 'mix-2' not in collected_ids

    # on_error called for offset 2
    assert 'mix-2' in error_ids

    # all 5 offsets committed (including the failed one)
    assert any(c[1] == 5 for c in committed)


# --- Concurrent window processing ---


async def test_concurrent_windows_offset_watermark():
    """Offsets commit only after all concurrent windows complete, respecting watermark order.

    Window 1 (offset 0): slow task (0.5s)
    Window 2 (offset 1): fast task (instant)
    Window 3 (offset 2): fast task (instant)

    Offset 1 and 2 finish first, but committable offset stays blocked until
    offset 0 completes. Final commit should be 3 (all three done).
    """
    committed = []
    completion_order = []

    class SlowFirstHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for msg in messages:
                # offset 0 gets a slow binary, others get fast
                if msg.offset == 0:
                    args = ['-c', 'import time; time.sleep(0.5); print("slow")']
                else:
                    args = ['-c', 'print("fast")']
                tasks.append(
                    ExecutorTask(
                        task_id=f'cw-{msg.offset}',
                        args=args,
                        source_offsets=[msg.offset],
                    )
                )
            return tasks

        async def on_task_complete(self, result):
            completion_order.append(result.task.task_id)
            return None

    async def on_commit(partition_id: int, offset: int) -> None:
        committed.append((partition_id, offset))

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
        task_timeout_seconds=10,
    )

    proc = PartitionProcessor(
        partition_id=0,
        handler=SlowFirstHandler(),
        executor_pool=pool,
        window_size=1,  # one message per window → 3 concurrent windows
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.enqueue(make_msg(offset=2))
    proc.start()

    # wait for all three to complete
    await wait_for(lambda: len(completion_order) >= 3, timeout=10)
    await proc.stop()

    # fast tasks (offset 1, 2) should finish before slow (offset 0)
    assert completion_order.index('cw-1') < completion_order.index('cw-0')
    assert completion_order.index('cw-2') < completion_order.index('cw-0')

    # final committed offset should be 3 (all three offsets: 0, 1, 2 → commit 2+1)
    assert committed[-1] == (0, 3)


async def test_concurrent_windows_pending_context():
    """arrange() for window N+1 sees in-flight tasks from window N in PendingContext."""
    seen_pending: list[set[str]] = []

    class TrackPendingHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            seen_pending.append(set(pending.pending_task_ids))
            tasks = []
            for msg in messages:
                tasks.append(
                    ExecutorTask(
                        task_id=f'p-{msg.offset}',
                        args=['-c', 'import time; time.sleep(0.3); print("ok")']
                        if msg.offset == 0
                        else ['-c', 'print("ok")'],
                        source_offsets=[msg.offset],
                    )
                )
            return tasks

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=4,
        task_timeout_seconds=10,
    )

    proc = PartitionProcessor(
        partition_id=0,
        handler=TrackPendingHandler(),
        executor_pool=pool,
        window_size=1,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()

    await wait_for(lambda: len(seen_pending) >= 2, timeout=10)
    await proc.stop()

    # first arrange() sees empty pending
    assert seen_pending[0] == set()
    # second arrange() sees task from first window still in-flight
    assert 'p-0' in seen_pending[1]


# =============================================================================
# on_message_complete + MessageGroup lifecycle
# =============================================================================


async def test_on_message_complete_single_task_success(echo_pool):
    """Single message → single task → hook fires with one result, zero errors."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [ExecutorTask(task_id=f'm-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

        async def on_message_complete(self, group):
            groups.append(group)
            return None

    proc = PartitionProcessor(partition_id=0, handler=H(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=7))
    proc.start()
    await wait_for(lambda: len(groups) == 1)
    await proc.stop()

    g = groups[0]
    assert g.source_message.offset == 7
    assert g.total == 1
    assert g.succeeded == 1
    assert g.failed == 0
    assert g.all_succeeded
    assert g.duration_seconds >= 0


async def test_on_message_complete_fan_out_waits_for_all_tasks(failing_pool):
    """One message with multiple tasks: hook fires ONCE after all complete."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class FanOutHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for m in messages:
                for i in range(3):
                    tasks.append(
                        ExecutorTask(
                            task_id=f'{m.offset}-t{i}',
                            args=['-c', 'import time; time.sleep(0.05); print(42)'],
                            source_offsets=[m.offset],
                        )
                    )
            return tasks

        async def on_message_complete(self, group):
            groups.append(group)
            return None

    proc = PartitionProcessor(partition_id=0, handler=FanOutHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    assert len(groups) == 1, f'expected exactly one hook fire, got {len(groups)}'
    assert groups[0].total == 3
    assert groups[0].succeeded == 3
    assert groups[0].all_succeeded


async def test_on_message_complete_partial_failure(failing_pool):
    """Some tasks succeed, some SKIP — group reports both."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class PartialHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for m in messages:
                tasks.append(ExecutorTask(task_id=f'ok-{m.offset}', args=['-c', 'print(1)'], source_offsets=[m.offset]))
                tasks.append(
                    ExecutorTask(
                        task_id=f'fail-{m.offset}', args=['-c', 'import sys; sys.exit(1)'], source_offsets=[m.offset]
                    )
                )
            return tasks

        async def on_error(self, task, error):
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=PartialHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    g = groups[0]
    assert g.succeeded == 1
    assert g.failed == 1
    assert g.any_failed
    assert not g.all_succeeded


async def test_on_message_complete_all_fail(failing_pool):
    """Every task fails → group has only errors, is still reported."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class AllFailHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'f-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=AllFailHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=3))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    assert groups[0].failed == 1
    assert groups[0].succeeded == 0
    assert not groups[0].all_succeeded


async def test_on_message_complete_retries_fire_hook_once(failing_pool):
    """Retries must NOT fire on_message_complete — only the final outcome does."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []
    attempts = {'n': 0}

    class RetryHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'r-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            attempts['n'] += 1
            if attempts['n'] < 3:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=RetryHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=10)
    await proc.stop()

    assert len(groups) == 1, f'hook fired {len(groups)} times; expected 1'
    assert groups[0].failed == 1


async def test_on_message_complete_replacement_chain(failing_pool):
    """on_error returning a list → group tasks include original + replacements.

    - tasks list preserves full history (debugging)
    - results/errors reflect terminal outcomes only
    - parent_task_id is auto-set on replacements
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []
    replaced_once = {'done': False}

    class ReplaceHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='orig-0',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[messages[0].offset],
                )
            ]

        async def on_error(self, task, error):
            if not replaced_once['done']:
                replaced_once['done'] = True
                return [
                    ExecutorTask(
                        task_id='repl-a',
                        args=['-c', 'print(1)'],
                        source_offsets=task.source_offsets,
                    ),
                    ExecutorTask(
                        task_id='repl-b',
                        args=['-c', 'print(2)'],
                        source_offsets=task.source_offsets,
                    ),
                ]
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=ReplaceHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=11))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    g = groups[0]
    # Full history: original + 2 replacements
    assert len(g.tasks) == 3
    task_ids = {t.task_id for t in g.tasks}
    assert task_ids == {'orig-0', 'repl-a', 'repl-b'}
    # Terminal outcomes: 2 successes (replacements), 0 errors (original was replaced)
    assert g.succeeded == 2
    assert g.failed == 0
    assert g.replaced == 1  # the original
    # parent_task_id auto-populated
    for t in g.tasks:
        if t.task_id.startswith('repl-'):
            assert t.parent_task_id == 'orig-0', f'{t.task_id} parent_task_id={t.parent_task_id}'
        else:
            assert t.parent_task_id is None


async def test_on_message_complete_empty_arrange_fires_hook(echo_pool):
    """arrange() returns [] for a message → hook still fires with empty group."""
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class EmptyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        async def on_message_complete(self, group):
            groups.append(group)

    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=EmptyHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=55))
    proc.start()
    await wait_for(lambda: len(groups) >= 1, timeout=5)
    await wait_for(lambda: any(c[1] == 56 for c in committed), timeout=5)
    await proc.stop()

    assert groups[0].is_empty
    assert groups[0].total == 0
    assert groups[0].source_message.offset == 55


async def test_on_message_complete_exception_does_not_block_offset(echo_pool):
    """If on_message_complete raises, log and proceed — offset still commits."""
    from drakkar.models import MessageGroup

    saw_group: list[MessageGroup] = []
    committed: list[tuple[int, int]] = []

    class BrokenHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [ExecutorTask(task_id=f'b-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

        async def on_message_complete(self, group):
            saw_group.append(group)
            raise RuntimeError('intentional test error')

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=BrokenHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=20))
    proc.start()
    await wait_for(lambda: any(c[1] == 21 for c in committed), timeout=5)
    await proc.stop()

    # Hook fired, raised; offset still committed (no stall)
    assert len(saw_group) == 1
    assert any(c[1] == 21 for c in committed)


async def test_on_message_complete_per_message_commits(echo_pool):
    """Offsets commit per-message as each finishes, not batched to window end.

    Verifies the per-message commit granularity change: slower/later tasks
    in the SAME window should NOT pin a fast-finishing earlier message's
    offset. We check this by seeing intermediate commits between
    per-message completions.
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []
    committed: list[tuple[int, int]] = []

    class Slowdown(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for m in messages:
                # offset 0 completes immediately; offset 1 sleeps briefly.
                dur = 0.3 if m.offset == 1 else 0.0
                tasks.append(
                    ExecutorTask(
                        task_id=f's-{m.offset}',
                        args=['-c', f'import time; time.sleep({dur}); print("ok")'],
                        source_offsets=[m.offset],
                    )
                )
            return tasks

        async def on_message_complete(self, group):
            groups.append(group)

    pool = ExecutorPool(binary_path=sys.executable, max_executors=4, task_timeout_seconds=10)

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=Slowdown(),
        executor_pool=pool,
        window_size=10,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()
    await wait_for(lambda: any(c[1] == 2 for c in committed), timeout=5)
    await proc.stop()

    # Both groups fired
    assert {g.source_message.offset for g in groups} == {0, 1}
    # Offset 1 commit (watermark=2) must have happened
    assert any(c[1] == 2 for c in committed)


async def test_on_message_complete_sink_delivery_exception_does_not_block_offset(echo_pool):
    """If the CollectResult from on_message_complete fails to deliver to a
    sink, the offset must still commit — a sink delivery failure is
    handled via on_delivery_error, not by stalling the partition.
    """

    delivery_calls: list[int] = []
    committed: list[tuple[int, int]] = []

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [ExecutorTask(task_id=f's-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

        async def on_message_complete(self, group):
            return CollectResult(kafka=[KafkaPayload(data=_Out(v='agg'))])

    async def failing_on_collect(result: CollectResult, partition_id: int) -> None:
        delivery_calls.append(partition_id)
        raise RuntimeError('sink down')

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=H(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=failing_on_collect,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=30))
    proc.start()
    await wait_for(lambda: any(c[1] == 31 for c in committed), timeout=5)
    await proc.stop()

    assert delivery_calls, 'on_collect must have been invoked'
    assert any(c[1] == 31 for c in committed), 'offset must commit despite sink failure'


async def test_on_error_replacement_preserves_explicit_parent_task_id(failing_pool):
    """If handler's on_error sets parent_task_id explicitly, framework must
    NOT override it. Lets the user point replacements at a non-obvious
    ancestor (e.g. skip a generation when restarting a chain).
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []
    replaced_once = {'done': False}

    class Handler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='root', args=['-c', 'import sys; sys.exit(1)'], source_offsets=[messages[0].offset]
                )
            ]

        async def on_error(self, task, error):
            if not replaced_once['done']:
                replaced_once['done'] = True
                return [
                    ExecutorTask(
                        task_id='child',
                        args=['-c', 'print(1)'],
                        source_offsets=task.source_offsets,
                        parent_task_id='custom-parent-id',  # explicit
                    ),
                ]
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=Handler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    child_task = next(t for t in groups[0].tasks if t.task_id == 'child')
    assert child_task.parent_task_id == 'custom-parent-id'


async def test_on_window_complete_still_fires_alongside_on_message_complete(echo_pool):
    """Both hooks coexist — on_window_complete sees all results at window end."""
    from drakkar.models import MessageGroup

    msg_groups: list[MessageGroup] = []
    window_calls: list[int] = []

    class Both(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [ExecutorTask(task_id=f'w-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

        async def on_message_complete(self, group):
            msg_groups.append(group)

        async def on_window_complete(self, results, source_messages):
            window_calls.append(len(results))

    proc = PartitionProcessor(partition_id=0, handler=Both(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.enqueue(make_msg(offset=2))
    proc.start()
    await wait_for(lambda: len(window_calls) >= 1 and len(msg_groups) >= 3, timeout=5)
    await proc.stop()

    assert len(msg_groups) == 3
    # on_window_complete saw all 3 results (no retries, no replacements)
    assert window_calls[0] == 3


# =============================================================================
# Fan-IN: one task belonging to multiple source messages
# =============================================================================


async def test_fan_in_single_task_reported_to_all_groups(echo_pool):
    """A task with source_offsets=[a, b, c] belongs to THREE MessageGroups.

    When that task succeeds, its ExecutorResult must appear in all three
    groups' results lists, and each group must fire on_message_complete
    once (not once-per-group-per-task).
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class FanInHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            # ONE task for the whole window — covers every message.
            offsets = [m.offset for m in messages]
            return [ExecutorTask(task_id='batched', args=['shared'], source_offsets=offsets)]

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=FanInHandler(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=100))
    proc.enqueue(make_msg(offset=101))
    proc.enqueue(make_msg(offset=102))
    proc.start()
    await wait_for(lambda: len(groups) == 3, timeout=5)
    await proc.stop()

    # All three messages saw the SAME task and the SAME result.
    offsets_seen = {g.source_message.offset for g in groups}
    assert offsets_seen == {100, 101, 102}
    for g in groups:
        assert g.total == 1, f'expected 1 task per group, got {g.total} for offset {g.source_message.offset}'
        assert g.succeeded == 1
        assert g.tasks[0].task_id == 'batched'
    # All three saw the same result instance (same task -> same ExecutorResult)
    result_ids = {id(g.results[0]) for g in groups}
    assert len(result_ids) == 1, 'all groups should share the same ExecutorResult instance'


async def test_fan_in_task_failure_reported_to_all_groups(failing_pool):
    """A fan-in task that terminally fails must land in errors of every
    group it belongs to — each group reports the same failure once.
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class FanInFailHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='shared-fail',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset for m in messages],
                )
            ]

        async def on_error(self, task, error):
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=FanInFailHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=5))
    proc.enqueue(make_msg(offset=6))
    proc.start()
    await wait_for(lambda: len(groups) == 2, timeout=5)
    await proc.stop()

    for g in groups:
        assert g.failed == 1
        assert g.succeeded == 0


async def test_fan_in_mixed_with_fan_out_waits_for_all(failing_pool):
    """Realistic mix: a window has BOTH a shared task (fan-in) and
    per-message tasks (fan-out). Each message's group must wait for
    BOTH kinds to finish before on_message_complete fires.
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []
    completion_order: list[int] = []

    class MixedHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            # Fan-in: one shared task for ALL messages (fast)
            tasks.append(
                ExecutorTask(
                    task_id='shared-fast',
                    args=['-c', 'print("shared")'],
                    source_offsets=[m.offset for m in messages],
                )
            )
            # Fan-out: per-message task (slower)
            for m in messages:
                tasks.append(
                    ExecutorTask(
                        task_id=f'own-{m.offset}',
                        args=['-c', 'import time; time.sleep(0.15); print("own")'],
                        source_offsets=[m.offset],
                    )
                )
            return tasks

        async def on_message_complete(self, group):
            groups.append(group)
            completion_order.append(group.source_message.offset)

    proc = PartitionProcessor(partition_id=0, handler=MixedHandler(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=20))
    proc.enqueue(make_msg(offset=21))
    proc.start()
    await wait_for(lambda: len(groups) == 2, timeout=5)
    await proc.stop()

    for g in groups:
        # Each message saw BOTH the shared task and its own per-message task.
        assert g.total == 2, f'expected 2 tasks per group, got {g.total}'
        assert g.succeeded == 2
        task_ids = {t.task_id for t in g.tasks}
        assert 'shared-fast' in task_ids
        assert f'own-{g.source_message.offset}' in task_ids


async def test_fan_in_offsets_outside_window_silently_ignored(echo_pool):
    """Defensive: if a task lists source_offsets that AREN'T in the current
    tracker set (e.g. stale offset from a previous window, or a handler bug),
    the framework silently skips them rather than crashing. This is the
    documented behavior — worth pinning.
    """
    from drakkar.models import MessageGroup

    groups: list[MessageGroup] = []

    class StrangeHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='over-reaching',
                    args=['ok'],
                    # offsets 500 and 501 are NOT in the current window
                    source_offsets=[m.offset for m in messages] + [500, 501],
                )
            ]

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=StrangeHandler(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=7))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    # Real tracker got its outcome; bogus 500/501 offsets silently ignored.
    assert groups[0].source_message.offset == 7
    assert groups[0].succeeded == 1


# =============================================================================
# Precomputed tasks flow through the full pipeline
# =============================================================================


async def test_precomputed_task_flows_through_on_task_complete(echo_pool):
    """A precomputed ExecutorTask must reach on_task_complete with the
    synthesized result — handler never sees a difference from a real
    subprocess outcome (other than result.pid is None).
    """
    from drakkar.models import MessageGroup, PrecomputedResult

    collected: list = []
    groups: list[MessageGroup] = []

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'pc-{m.offset}',
                    source_offsets=[m.offset],
                    precomputed=PrecomputedResult(stdout=f'cached-{m.offset}'),
                )
                for m in messages
            ]

        async def on_task_complete(self, result):
            collected.append(result)
            return None

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=H(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=40))
    proc.enqueue(make_msg(offset=41))
    proc.start()
    await wait_for(lambda: len(groups) == 2, timeout=5)
    await proc.stop()

    # Both results delivered to on_task_complete.
    assert {r.stdout for r in collected} == {'cached-40', 'cached-41'}
    # And both came through as successful completions in the message groups.
    for g in groups:
        assert g.succeeded == 1
        assert g.failed == 0
        assert g.results[0].pid is None  # marker: no real subprocess
        assert g.results[0].task.precomputed is not None


async def test_precomputed_mixed_with_real_subprocess_in_one_window(failing_pool):
    """A single window may contain both precomputed and real-subprocess
    tasks. The message's on_message_complete sees both terminal outcomes
    and waits for BOTH before firing.
    """
    from drakkar.models import MessageGroup, PrecomputedResult

    groups: list[MessageGroup] = []

    class Mixed(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            tasks = []
            for m in messages:
                # Each message produces TWO tasks: one precomputed,
                # one real-subprocess — both tied to the same offset.
                tasks.append(
                    ExecutorTask(
                        task_id=f'pc-{m.offset}',
                        source_offsets=[m.offset],
                        precomputed=PrecomputedResult(stdout='from-cache'),
                    )
                )
                tasks.append(
                    ExecutorTask(
                        task_id=f'rs-{m.offset}',
                        args=['-c', 'print("from-subprocess")'],
                        source_offsets=[m.offset],
                    )
                )
            return tasks

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=Mixed(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=50))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    g = groups[0]
    assert g.total == 2
    assert g.succeeded == 2
    # Exactly one result came from the precomputed path (pid is None),
    # the other from the real subprocess (pid is not None).
    pids = {r.pid for r in g.results}
    assert None in pids
    assert any(p is not None for p in pids)


async def test_precomputed_fan_in_across_multiple_messages(echo_pool):
    """A precomputed task with source_offsets=[a, b, c] delivers the
    synthesized result to each of the three message groups — fan-in
    semantics work identically for precomputed and real tasks.
    """
    from drakkar.models import MessageGroup, PrecomputedResult

    groups: list[MessageGroup] = []

    class FanInPC(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='shared-pc',
                    source_offsets=[m.offset for m in messages],
                    precomputed=PrecomputedResult(stdout='shared-cached-answer'),
                )
            ]

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=FanInPC(), executor_pool=echo_pool, window_size=10)
    proc.enqueue(make_msg(offset=60))
    proc.enqueue(make_msg(offset=61))
    proc.enqueue(make_msg(offset=62))
    proc.start()
    await wait_for(lambda: len(groups) == 3, timeout=5)
    await proc.stop()

    # All three groups saw the SAME precomputed result.
    for g in groups:
        assert g.succeeded == 1
        assert g.results[0].stdout == 'shared-cached-answer'
        assert g.results[0].pid is None


async def test_precomputed_failure_routes_through_on_error(failing_pool):
    """A precomputed task with exit_code != 0 must trigger on_error,
    letting the handler RETRY, SKIP, or return replacements exactly
    as it would for a real subprocess failure.
    """
    from drakkar.models import MessageGroup, PrecomputedResult

    groups: list[MessageGroup] = []
    error_hook_calls: list[str] = []

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='pc-fail',
                    source_offsets=[messages[0].offset],
                    precomputed=PrecomputedResult(stdout='', stderr='boom', exit_code=3),
                )
            ]

        async def on_error(self, task, error):
            error_hook_calls.append(task.task_id)
            return ErrorAction.SKIP

        async def on_message_complete(self, group):
            groups.append(group)

    proc = PartitionProcessor(partition_id=0, handler=H(), executor_pool=failing_pool, window_size=10)
    proc.enqueue(make_msg(offset=70))
    proc.start()
    await wait_for(lambda: len(groups) == 1, timeout=5)
    await proc.stop()

    assert error_hook_calls == ['pc-fail']
    assert groups[0].failed == 1
    assert groups[0].succeeded == 0
