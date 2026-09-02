"""Tests for Drakkar partition processor."""

import asyncio
import sys
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel as BM
from structlog.contextvars import get_contextvars

from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler
from drakkar.hookctx import current_hook_context
from drakkar.metrics import executor_timeouts
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.partition import MAX_RETRIES, PARTITION_RESTART_LIMIT, PartitionProcessor, Window
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


# --- A raising on_error must not freeze the commit watermark ---


async def test_raising_on_error_settles_the_task_and_keeps_committing(failing_pool):
    """A broken on_error degrades to SKIP instead of wedging the partition.

    on_error is called from inside an ``except ExecutorTaskError`` clause, so
    a raise there is NOT caught by the sibling ``except Exception`` — Python
    only matches sibling handlers against exceptions from the ``try`` body.
    Before the fix the exception escaped the fire-and-forget task and skipped
    the tracker-settling loop, leaving that offset PENDING forever. Since
    ``committable()`` stops at the first incomplete offset, one broken hook
    call froze every later commit on the partition and forced a full replay
    from the wedge point on the next restart.
    """
    hook_calls = 0

    class BrokenOnErrorHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id=f'broken-{m.offset}',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[m.offset],
                )
                for m in messages
            ]

        async def on_error(self, task, error):
            nonlocal hook_calls
            hook_calls += 1
            raise RuntimeError('handler bug inside on_error')

    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=BrokenOnErrorHandler(),
        executor_pool=failing_pool,
        window_size=10,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()
    # Offset 1 committing proves BOTH messages settled: the watermark only
    # advances past a gap-free prefix, so a wedged offset 0 would cap it at 1.
    await wait_for(lambda: any(c[1] == 2 for c in committed), timeout=10)
    await proc.stop()

    assert hook_calls == 2, 'both failing tasks should have reached on_error'
    assert proc.offset_tracker.pending_count == 0, 'a hook bug left an offset pending'


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
    in the tracker so the next _commit_now retries them.

    Reproduces: one partition per worker retains lag after all work done.
    Root cause was _handle_commit swallowing exceptions, making _commit_now
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


async def test_stop_force_cancels_hung_processor(echo_pool, monkeypatch):
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
    task = proc._task
    assert task is not None

    # stop() hardcodes a 10s grace period before force-cancel; shrink it so
    # the test exercises the real cancel path without the real wait.
    real_wait_for = asyncio.wait_for

    async def fast_wait_for(awaitable, timeout=None):
        if timeout == 10.0:
            timeout = 0.05
        return await real_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(asyncio, 'wait_for', fast_wait_for)

    await proc.stop()

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


class _AggHandler(BaseDrakkarHandler):
    """Handler whose on_message_complete returns an aggregate payload."""

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f's-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

    async def on_message_complete(self, group):
        return CollectResult(kafka=[KafkaPayload(data=_Out(v='agg'))])


def _failing_collect_fixture():
    calls: list[int] = []

    async def failing_on_collect(result: CollectResult, partition_id: int) -> None:
        calls.append(partition_id)
        raise RuntimeError('sink down')

    return calls, failing_on_collect


async def test_on_message_complete_sink_exception_drop_mode_commits(echo_pool):
    """Default dlq.on_send_failure=drop: an unexpected sink-delivery
    exception on the aggregate payload is logged loudly but the offset
    still commits — a handler bug must not wedge the partition."""
    delivery_calls, failing_on_collect = _failing_collect_fixture()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=_AggHandler(),
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


async def test_on_message_complete_sink_exception_stall_mode_stalls_offset(echo_pool):
    """dlq.on_send_failure=stall: an unexpected sink-delivery exception
    means delivery state is unknown — the offset must NOT commit, so the
    message replays after restart instead of being silently lost."""
    delivery_calls, failing_on_collect = _failing_collect_fixture()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=_AggHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=failing_on_collect,
        on_commit=on_commit,
        on_dlq_failure='stall',
    )
    proc.enqueue(make_msg(offset=30))
    proc.start()
    await wait_for(lambda: bool(delivery_calls), timeout=5)
    # Give the (not expected) commit a chance to fire before asserting.
    await asyncio.sleep(0.1)
    await proc.stop()

    assert delivery_calls, 'on_collect must have been invoked'
    assert not committed, 'offset must NOT commit when delivery is unconfirmed'
    assert proc.offset_tracker.has_pending(), 'offset stays pending (stalled watermark)'


class _TaskCollectHandler(BaseDrakkarHandler):
    """Emits its payload from on_task_complete rather than on_message_complete."""

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f'tc-{m.offset}', args=['ok'], source_offsets=[m.offset]) for m in messages]

    async def on_task_complete(self, result):
        return CollectResult(kafka=[KafkaPayload(data=_Out(v='per-task'))])


async def test_on_task_complete_sink_exception_drop_mode_commits(echo_pool):
    """Default dlq.on_send_failure=drop: the offset still commits.

    Mirrors the on_message_complete path — a deterministic handler bug must
    not wedge the partition when the operator chose 'drop'.
    """
    delivery_calls, failing_on_collect = _failing_collect_fixture()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=_TaskCollectHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=failing_on_collect,
        on_commit=on_commit,
    )
    proc.enqueue(make_msg(offset=40))
    proc.start()
    await wait_for(lambda: any(c[1] == 41 for c in committed), timeout=5)
    await proc.stop()

    assert delivery_calls, 'on_collect must have been invoked'


async def test_on_task_complete_sink_exception_stall_mode_stalls_offset(echo_pool):
    """dlq.on_send_failure=stall: the offset must NOT commit.

    This call site previously caught only SinkDeliveryFailedError, so any
    other exception — notably SinkNotConfiguredError / AmbiguousSinkError
    from validate_collect when a payload names a sink that is not
    configured — fell through to the generic task-failure handler. The
    payload was discarded without ever reaching a sink or the DLQ, and the
    offset committed anyway, silently defeating the 'stall' setting the
    operator chose specifically to prevent loss.
    """
    delivery_calls, failing_on_collect = _failing_collect_fixture()
    committed: list[tuple[int, int]] = []

    async def on_commit(pid, off):
        committed.append((pid, off))

    proc = PartitionProcessor(
        partition_id=0,
        handler=_TaskCollectHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_collect=failing_on_collect,
        on_commit=on_commit,
        on_dlq_failure='stall',
    )
    proc.enqueue(make_msg(offset=40))
    proc.start()
    await wait_for(lambda: bool(delivery_calls), timeout=5)
    # Give the (not expected) commit a chance to fire before asserting.
    await asyncio.sleep(0.1)
    await proc.stop()

    assert delivery_calls, 'on_collect must have been invoked'
    assert not committed, 'offset must NOT commit when delivery is unconfirmed'
    assert proc.offset_tracker.has_pending(), 'offset stays pending (stalled watermark)'


async def test_concurrent_commit_now_does_not_resend_a_stale_watermark(echo_pool):
    """Two overlapping commits must not both send the same watermark.

    ``_commit_now`` is read → RPC → acknowledge, and every task completion
    calls it concurrently with the run loop. Without a lock both callers read
    the same ``committable()`` before either acknowledges, so the identical
    offset is committed twice — and with two RPCs in flight at once the broker
    can apply them out of order, moving the group's committed offset
    *backwards* and causing already-processed messages to be replayed.
    """
    commits: list[int] = []
    release = asyncio.Event()

    async def slow_on_commit(pid, off):
        commits.append(off)
        await release.wait()

    proc = PartitionProcessor(
        partition_id=0,
        handler=_TaskCollectHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=slow_on_commit,
    )
    proc.offset_tracker.register(0)
    proc.offset_tracker.complete(0)

    # First commit parks inside the lock, mid-RPC.
    first = asyncio.create_task(proc._commit_now())
    await wait_for(lambda: bool(commits), timeout=2)
    # Second arrives while the first is still in flight.
    second = asyncio.create_task(proc._commit_now())
    await asyncio.sleep(0.05)
    release.set()
    await asyncio.gather(first, second)

    assert commits == [1], f'watermark committed more than once: {commits}'


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


# --- signal_stop: sync counterpart to stop() ---


async def test_signal_stop_sets_running_false_without_blocking(echo_pool):
    """signal_stop() flips _running to False and returns immediately.

    The run-loop task keeps going until it notices the flag — signal_stop
    itself must NOT await task completion (that is stop()'s job).
    """

    class SlowArrangeHandler(BaseDrakkarHandler):
        # Stays inside arrange() long enough that signal_stop cannot race
        # the run-loop to completion — lets us assert the task is alive.
        async def arrange(self, messages, pending):
            await asyncio.sleep(0.5)
            return []

    proc = PartitionProcessor(
        partition_id=0,
        handler=SlowArrangeHandler(),
        executor_pool=echo_pool,
        window_size=1,
    )

    proc.enqueue(make_msg(offset=0))
    proc.start()
    # Wait until the run loop has picked up the message and entered arrange().
    await wait_for(lambda: proc._arranging, timeout=2)
    assert proc._running is True
    assert proc._task is not None

    proc.signal_stop()

    # signal_stop is synchronous: _running must already be False on return
    # and the task must still be alive (mid-arrange sleep).
    assert proc._running is False
    assert proc._task is not None
    assert not proc._task.done()

    # Clean up: await stop() so the run loop completes naturally.
    await proc.stop()


async def test_stop_after_signal_stop_completes_cleanly(echo_pool):
    """signal_stop() followed by stop() drains normally without double-signal issues."""
    committed: list[tuple[int, int]] = []

    async def on_commit(pid: int, offset: int) -> None:
        committed.append((pid, offset))

    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=5,
        on_commit=on_commit,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()

    # Let the processor pick up at least one task before signalling stop.
    await wait_for(lambda: len(handler.collect_calls) >= 1, timeout=5)

    proc.signal_stop()
    # stop() should complete without error: it re-sets _running=False
    # (idempotent) then awaits _task. Queued messages drain as part of
    # _run()'s post-loop block, so the final commit still happens.
    await proc.stop()

    assert proc._task is None
    assert proc._running is False
    # All enqueued offsets drained + committed — no double-drain regression.
    assert proc.queue_size == 0
    assert proc.inflight_count == 0
    assert any(offset == 2 for _, offset in committed), f'Expected commit of 2, got: {committed}'


async def test_signal_stop_is_idempotent(echo_pool):
    """Calling signal_stop multiple times is safe — it just re-sets the flag."""
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )

    proc.start()
    proc.signal_stop()
    proc.signal_stop()
    proc.signal_stop()

    assert proc._running is False

    # And stop() still works after the repeat signals.
    await proc.stop()
    assert proc._task is None


# --- Replacement accounting: window.results vs total_tasks ---
#
# These tests pin down the documented accounting invariant (see
# ``drakkar/partition.py`` ``Window`` docstring and
# ``docs/fan-out.md`` "Replacement accounting — Window vs MessageGroup"):
#
#   - ``total_tasks`` counts EVERY scheduled task (original + replacements).
#   - ``completed_count`` ticks once per terminal outcome OR replacement
#     handoff.
#   - ``window.results`` contains one ``ExecutorResult`` per task
#     invocation that actually ran to a terminal outcome — replaced
#     originals do NOT contribute. So ``len(results)`` can be less than
#     ``total_tasks`` whenever any task was replaced; the gap equals
#     the number of replaced tasks.
#
# The tests are deliberately descriptive — they double as the reference
# specification for operators inspecting window state.


def _wrap_capture_window(proc: PartitionProcessor, captured: list[Window]) -> None:
    """Monkey-patch ``proc._execute_and_track`` to snapshot the Window
    reference as soon as the first task executes against it. We need
    this because ``Window`` is an internal dataclass that isn't exposed
    through any public handler hook.
    """
    original = proc._execute_and_track

    async def wrapped(task, window, retry_count=0):
        if window not in captured:
            captured.append(window)
        return await original(task, window, retry_count)

    proc._execute_and_track = wrapped  # type: ignore[method-assign]


async def test_replacement_window_results_contains_replacements_only(failing_pool):
    """1 original fails → on_error returns 2 replacements → both succeed.

    Invariant: ``total_tasks == 3``, ``completed_count == 3``,
    ``len(window.results) == 2`` (only the two replacement successes;
    the replaced original has no entry).
    """
    captured_windows: list[Window] = []
    window_complete_results_sizes: list[int] = []

    class ReplacementHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='orig-fail',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[messages[0].offset],
                )
            ]

        async def on_error(self, task, error):
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

        async def on_window_complete(self, results, source_messages):
            # Capture the count as the handler actually sees it at hook-fire time.
            window_complete_results_sizes.append(len(results))
            return None

    proc = PartitionProcessor(
        partition_id=0,
        handler=ReplacementHandler(),
        executor_pool=failing_pool,
        window_size=10,
    )
    _wrap_capture_window(proc, captured_windows)

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(window_complete_results_sizes) > 0, timeout=5)
    await proc.stop()

    assert len(captured_windows) == 1, f'expected 1 window, got {len(captured_windows)}'
    w = captured_windows[0]

    # Window-level accounting:
    assert w.total_tasks == 3, f'total_tasks={w.total_tasks} (expected 1 original + 2 replacements)'
    assert w.completed_count == 3, f'completed_count={w.completed_count}'
    assert len(w.tasks) == 3, 'tasks list should contain full history (original + 2 replacements)'
    # The documented invariant: one result per actual execution outcome,
    # so the replaced original is absent.
    assert len(w.results) == 2, (
        f'len(results)={len(w.results)} — expected 2 (both replacements). '
        f'The replaced original must NOT contribute an entry.'
    )
    # Both entries are the replacements, in completion order.
    result_task_ids = {r.task.task_id for r in w.results}
    assert result_task_ids == {'repl-a', 'repl-b'}
    # Both replacements succeeded.
    assert all(r.exit_code == 0 for r in w.results)

    # Sanity: the is_complete property holds (completed_count == total_tasks).
    assert w.is_complete

    # on_window_complete received the same list (by reference) — size matches.
    assert window_complete_results_sizes == [2]


async def test_replacement_cascading_replaced_then_skip(failing_pool):
    """Cascading replacement: original fails → 1 replacement → replacement
    ALSO fails → on_error returns SKIP for the replacement.

    Invariant: ``total_tasks == 2``, ``completed_count == 2``,
    ``len(window.results) == 1`` (just the replacement's SKIP'd
    failure result; the original is omitted as "replaced").

    This pins down that SKIP'd replacements DO append to window.results
    (they are a terminal failure), while a replaced original is the
    only category that does not contribute.
    """
    captured_windows: list[Window] = []
    complete_fired = asyncio.Event()

    class CascadingHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='orig-fail',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[messages[0].offset],
                )
            ]

        async def on_error(self, task, error):
            if task.task_id == 'orig-fail':
                # Replace with another task that will also fail.
                return [
                    ExecutorTask(
                        task_id='repl-also-fail',
                        args=['-c', 'import sys; sys.exit(1)'],
                        source_offsets=task.source_offsets,
                    )
                ]
            # Replacement failed — SKIP, so it becomes a terminal failure.
            return ErrorAction.SKIP

        async def on_window_complete(self, results, source_messages):
            complete_fired.set()
            return None

    proc = PartitionProcessor(
        partition_id=0,
        handler=CascadingHandler(),
        executor_pool=failing_pool,
        window_size=10,
    )
    _wrap_capture_window(proc, captured_windows)

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await asyncio.wait_for(complete_fired.wait(), timeout=5)
    await proc.stop()

    assert len(captured_windows) == 1
    w = captured_windows[0]

    assert w.total_tasks == 2, f'total_tasks={w.total_tasks} (expected 1 original + 1 replacement)'
    assert w.completed_count == 2
    assert len(w.tasks) == 2
    # One entry: the SKIP'd replacement's failure result.
    # The replaced original contributes nothing.
    assert len(w.results) == 1, (
        f"len(results)={len(w.results)} — expected 1 (the replacement that failed and was SKIP'd)."
    )
    result = w.results[0]
    assert result.task.task_id == 'repl-also-fail'
    # Subprocess exited with non-zero — the failure result was appended
    # via the ``window.results.append(e.result)`` branch in
    # ``_execute_and_track`` for terminal-error tasks.
    assert result.exit_code != 0
    assert w.is_complete


async def test_replacement_mixed_with_retries_then_replaced(failing_pool):
    """Mixed: original fails → 2 retries (all fail) → then replaced by 1 success.

    Retries reuse the original invocation's slot — they do NOT bump
    ``total_tasks`` or append to ``window.tasks``. Only the replacement
    list-return adds new entries. The final accounting therefore:

      - ``total_tasks == 2`` (original + 1 replacement)
      - ``completed_count == 2`` (1 for the replaced original, 1 for
        the replacement's terminal success)
      - ``len(window.results) == 1`` (just the replacement's success)
      - ``window.tasks`` contains 2 entries (no per-retry entries)
    """
    captured_windows: list[Window] = []
    complete_fired = asyncio.Event()
    retry_counts: dict[str, int] = {}

    class MixedHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='orig',
                    args=['-c', 'import sys; sys.exit(1)'],
                    source_offsets=[messages[0].offset],
                )
            ]

        async def on_error(self, task, error):
            retry_counts[task.task_id] = retry_counts.get(task.task_id, 0) + 1
            if task.task_id == 'orig' and retry_counts[task.task_id] <= 2:
                # First two errors: RETRY. These hand off to a new
                # coroutine reusing the same inflight slot; no
                # ``total_tasks`` bump, no ``tasks.append``.
                return ErrorAction.RETRY
            if task.task_id == 'orig':
                # Third error: give up on retrying, replace with a
                # task that succeeds.
                return [
                    ExecutorTask(
                        task_id='repl-success',
                        args=['-c', 'print("ok")'],
                        source_offsets=task.source_offsets,
                    )
                ]
            return ErrorAction.SKIP

        async def on_window_complete(self, results, source_messages):
            complete_fired.set()
            return None

    proc = PartitionProcessor(
        partition_id=0,
        handler=MixedHandler(),
        executor_pool=failing_pool,
        window_size=10,
    )
    _wrap_capture_window(proc, captured_windows)

    proc.enqueue(make_msg(offset=0))
    proc.start()
    await asyncio.wait_for(complete_fired.wait(), timeout=10)
    await proc.stop()

    # Sanity: on_error fired 3 times on the 'orig' task (2 RETRIES + 1 replacement decision).
    assert retry_counts.get('orig') == 3

    assert len(captured_windows) == 1
    w = captured_windows[0]

    # Retries did NOT inflate total_tasks — only the replacement did.
    assert w.total_tasks == 2, (
        f'total_tasks={w.total_tasks} — retries reuse the original slot; '
        'only the replacement list-return bumps this counter.'
    )
    assert w.completed_count == 2
    assert len(w.tasks) == 2, 'tasks holds 2 entries (original + replacement); retries are not re-appended'
    assert len(w.results) == 1, f'len(results)={len(w.results)} — expected 1 (the successful replacement).'
    assert w.results[0].task.task_id == 'repl-success'
    assert w.results[0].exit_code == 0
    assert w.is_complete


class _StubFailurePool(ExecutorPool):
    """Pool stand-in that raises a caller-supplied ``ExecutorTaskError``
    instead of spawning a subprocess — lets a test pin the exact
    ``ExecutorError.kind`` driving ``_execute_and_track``'s metric logic
    without depending on real timing/exit-code behaviour."""

    def __init__(self, error: ExecutorError, result: ExecutorResult) -> None:
        super().__init__(binary_path='/bin/true', max_executors=4, task_timeout_seconds=10)
        self._stub_error = error
        self._stub_result = result

    async def execute(self, task, recorder=None, partition_id=0):
        raise ExecutorTaskError(error=self._stub_error, result=self._stub_result)


class _SkipOnceHandler(BaseDrakkarHandler):
    """Single task per message, SKIP on error, signals via on_window_complete."""

    def __init__(self):
        self.window_complete_calls: list[int] = []

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f'stub-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]

    async def on_error(self, task, error):
        return ErrorAction.SKIP

    async def on_window_complete(self, results, source_messages):
        self.window_complete_calls.append(len(results))
        return None


async def test_timeout_kind_increments_timeout_metric():
    """kind='timeout' → executor_timeouts ticks; text is NOT consulted."""
    task = ExecutorTask(task_id='stub-0', args=['x'], source_offsets=[0])
    error = ExecutorError(
        task=task,
        stderr='task timed out',
        exception='Timeout after 5s',
        kind='timeout',
    )
    result = ExecutorResult(exit_code=-1, stdout='', stderr='task timed out', duration_seconds=0.01, task=task)
    stub_pool = _StubFailurePool(error=error, result=result)
    handler = _SkipOnceHandler()

    before = executor_timeouts._value.get()  # type: ignore[attr-defined]

    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=stub_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(handler.window_complete_calls) == 1, timeout=5)
    await proc.stop()

    after = executor_timeouts._value.get()  # type: ignore[attr-defined]
    assert after == before + 1, f'expected executor_timeouts to increment by 1, before={before} after={after}'


async def test_timeout_text_without_timeout_kind_does_not_increment():
    """kind='nonzero_exit' with 'Timeout' in the free text → NO tick (pins
    the refinement: classification is by kind, never by parsing text)."""
    task = ExecutorTask(task_id='stub-0', args=['x'], source_offsets=[0])
    error = ExecutorError(
        task=task,
        exit_code=1,
        stderr='boom',
        exception='Timeout after 5s (misleading text — this is NOT a real timeout)',
        kind='nonzero_exit',
    )
    result = ExecutorResult(exit_code=1, stdout='', stderr='boom', duration_seconds=0.01, task=task)
    stub_pool = _StubFailurePool(error=error, result=result)
    handler = _SkipOnceHandler()

    before = executor_timeouts._value.get()  # type: ignore[attr-defined]

    proc = PartitionProcessor(partition_id=0, handler=handler, executor_pool=stub_pool, window_size=10)
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: len(handler.window_complete_calls) == 1, timeout=5)
    await proc.stop()

    after = executor_timeouts._value.get()  # type: ignore[attr-defined]
    assert after == before, f'expected executor_timeouts NOT to increment, before={before} after={after}'


# --- Run-loop supervision (restart once, then declare the partition dead) ---


class _CrashingHandler(EchoHandler):
    """Raises from ``arrange`` for the first ``crashes`` windows.

    ``arrange`` runs inside ``_process_window``, which is inside the run
    loop's try block — so raising here is the realistic shape of a loop
    death: a handler bug or a transient dependency failure, not a
    contrived injection into framework internals.
    """

    def __init__(self, crashes: int) -> None:
        super().__init__()
        self.remaining_crashes = crashes
        # Separate counter: EchoHandler.arrange_calls is a list of tuples.
        self.arrange_attempts = 0

    async def arrange(self, messages, pending):
        self.arrange_attempts += 1
        if self.remaining_crashes > 0:
            self.remaining_crashes -= 1
            raise RuntimeError('handler exploded')
        return await super().arrange(messages, pending)


async def test_run_loop_restarts_once_after_an_unexpected_error(echo_pool):
    """A single crash must not take the partition out of the pipeline.

    Before supervision the loop caught the exception, logged it, and
    returned — the task completed *successfully*, so nothing restarted it
    and nothing reported it. Messages kept arriving and the queue grew
    with nothing draining it.
    """
    handler = _CrashingHandler(crashes=1)
    collected: list[CollectResult] = []

    async def on_collect(result, partition_id):
        collected.append(result)

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
        on_collect=on_collect,
    )
    proc.start()
    try:
        proc.enqueue(make_msg(partition=0, offset=1))
        await wait_for(lambda: handler.remaining_crashes == 0)  # crash happened

        # The loop is back: a message enqueued after the crash is processed.
        proc.enqueue(make_msg(partition=0, offset=2))
        await wait_for(lambda: len(collected) > 0)  # loop resumed after the crash
        assert not proc.is_dead
    finally:
        await proc.stop()


async def test_restart_leaves_the_crashed_window_uncommitted(echo_pool):
    """Pins the limit of a restart: processing resumes, the watermark does not.

    ``_process_window`` registers each offset BEFORE arrange runs, so a
    crash there leaves those offsets PENDING for the life of the process.
    ``committable()`` stops at the first incomplete offset, so every later
    message completes without ever becoming committable. That is the
    correct at-least-once outcome — those messages were never processed,
    and committing past them would lose them — but it means a restarted
    partition keeps working while its lag climbs, until a rebalance or
    restart hands the offsets to an owner that will redeliver them.
    """
    handler = _CrashingHandler(crashes=1)
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
        window_size=1,
        on_collect=on_collect,
        on_commit=on_commit,
    )
    proc.start()
    try:
        proc.enqueue(make_msg(partition=0, offset=1))
        await wait_for(lambda: handler.remaining_crashes == 0)
        proc.enqueue(make_msg(partition=0, offset=2))
        await wait_for(lambda: len(collected) > 0)

        await asyncio.sleep(0.1)
        assert committed == [], "the crashed window's offset must keep blocking the watermark"
        assert proc.offset_tracker.pending_count == 1
    finally:
        await proc.stop()


async def test_run_loop_dies_after_a_second_crash(echo_pool):
    """A loop failing deterministically is declared dead rather than looped forever."""
    handler = _CrashingHandler(crashes=2)

    proc = PartitionProcessor(
        partition_id=7,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )
    proc.start()
    try:
        proc.enqueue(make_msg(partition=7, offset=1))
        proc.enqueue(make_msg(partition=7, offset=2))
        await wait_for(lambda: proc.is_dead)  # second crash marks the partition dead
        assert 'handler exploded' in proc.death_reason
    finally:
        await proc.stop()


async def test_dead_partition_stops_restarting(echo_pool):
    """Once dead, the loop stays down — no endless restart storm."""
    handler = _CrashingHandler(crashes=10)

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )
    proc.start()
    try:
        for offset in range(5):
            proc.enqueue(make_msg(partition=0, offset=offset))
        await wait_for(lambda: proc.is_dead)

        # PARTITION_RESTART_LIMIT=1 means exactly two arrange attempts:
        # the original run and its one restart.
        calls_when_dead = handler.arrange_attempts
        await asyncio.sleep(0.2)
        assert handler.arrange_attempts == calls_when_dead, 'loop kept restarting after being declared dead'
        assert calls_when_dead == PARTITION_RESTART_LIMIT + 1
    finally:
        await proc.stop()


async def test_clean_shutdown_is_not_treated_as_a_death(echo_pool):
    """The ordinary drain-and-exit path must not count as a crash."""
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )
    proc.start()
    await asyncio.sleep(0.05)
    await proc.stop()

    assert not proc.is_dead
    assert proc.death_reason == ''


async def test_crash_during_shutdown_drain_is_not_restarted(echo_pool):
    """A loop already on its way out is declared dead, not restarted.

    Restarting here would re-enter a drain that has already been
    accounted for by the caller awaiting ``stop()``.
    """
    handler = _CrashingHandler(crashes=1)
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=1,
    )
    proc.start()
    # Queue work, then immediately signal stop so the crash lands on the
    # post-_running drain path rather than the main loop.
    proc.signal_stop()
    proc.enqueue(make_msg(partition=0, offset=1))
    await proc.stop()

    assert handler.arrange_attempts <= 1, 'a shutdown-path crash must not be restarted'


# --- handler annotations: hook context anchoring ---


class _CapturedAnnotation(BM):
    """One annotate() call plus the ambient anchors at the moment it fired."""

    kind: str
    hook: str
    partition: int
    window_id: int | None
    offset: int | None
    task_id: str | None
    offsets: list[int]


class RecordingAnnotator:
    """Stands in for the real Annotator and snapshots the hook context."""

    def __init__(self) -> None:
        self.captured: list[_CapturedAnnotation] = []

    def emit(self, target, kind, data=None, *, labels=None) -> None:
        ctx = current_hook_context()
        assert ctx is not None, f'annotate() from {kind!r} saw no hook context'
        self.captured.append(
            _CapturedAnnotation(
                kind=kind,
                hook=ctx.hook,
                partition=ctx.partition,
                window_id=ctx.window_id,
                offset=ctx.offset,
                task_id=ctx.task_id,
                offsets=list(ctx.offsets),
            )
        )


class AnnotatingHandler(EchoHandler):
    """Annotates from every hook the partition processor invokes."""

    async def arrange(self, messages, pending):
        self.annotate(None, 'from_arrange')
        tasks = await super().arrange(messages, pending)
        for msg in messages:
            self.annotate(msg, 'from_arrange_message')
        for task in tasks:
            self.annotate(task, 'from_arrange_task')
        return tasks

    async def on_task_complete(self, result):
        self.annotate(result.task, 'from_task_complete')
        return await super().on_task_complete(result)

    async def on_window_complete(self, results, source_messages):
        self.annotate(None, 'from_window_complete')
        return await super().on_window_complete(results, source_messages)

    async def on_message_complete(self, group):
        self.annotate(group.source_message, 'from_message_complete')
        return None


class AnnotatingErrorHandler(ErrorHandler):
    """Annotates from on_error, which runs inside an except clause."""

    async def on_error(self, task, error):
        self.annotate(task, 'from_on_error')
        return ErrorAction.SKIP


async def run_annotating_processor(handler, pool, partition_id=2, offsets=(0, 1)):
    """Drive one window through the processor and return the annotator."""
    annotator = RecordingAnnotator()
    handler._annotator = annotator
    proc = PartitionProcessor(
        partition_id=partition_id,
        handler=handler,
        executor_pool=pool,
        window_size=10,
    )
    for offset in offsets:
        proc.enqueue(make_msg(partition=partition_id, offset=offset))
    proc.start()
    await wait_for(lambda: not proc.offset_tracker.has_pending() and proc.inflight_count == 0, timeout=5)
    await proc.stop()
    return annotator


def captured_kind(annotator, kind: str) -> _CapturedAnnotation:
    return next(c for c in annotator.captured if c.kind == kind)


async def test_annotate_from_arrange_is_window_scoped(echo_pool):
    annotator = await run_annotating_processor(AnnotatingHandler(), echo_pool)

    entry = captured_kind(annotator, 'from_arrange')
    assert entry.hook == 'arrange'
    assert entry.partition == 2
    assert entry.window_id is not None
    assert entry.offsets == [0, 1]
    assert entry.offset is None
    assert entry.task_id is None


async def test_annotate_from_on_task_complete_carries_task_anchor(echo_pool):
    annotator = await run_annotating_processor(AnnotatingHandler(), echo_pool)

    entry = captured_kind(annotator, 'from_task_complete')
    assert entry.hook == 'on_task_complete'
    assert entry.partition == 2
    assert entry.task_id is not None
    assert entry.window_id is not None


async def test_annotate_from_on_window_complete_carries_window_offsets(echo_pool):
    annotator = await run_annotating_processor(AnnotatingHandler(), echo_pool)

    entry = captured_kind(annotator, 'from_window_complete')
    assert entry.hook == 'on_window_complete'
    assert entry.offsets == [0, 1]
    assert entry.task_id is None


async def test_annotate_from_on_message_complete_carries_message_offset(echo_pool):
    annotator = await run_annotating_processor(AnnotatingHandler(), echo_pool)

    entry = captured_kind(annotator, 'from_message_complete')
    assert entry.hook == 'on_message_complete'
    assert entry.offset in (0, 1)
    assert entry.offsets == [entry.offset]
    # A message tracker outlives its window, so there is no single window
    # this hook belongs to.
    assert entry.window_id is None


async def test_annotate_from_on_error_carries_task_anchor(failing_pool):
    handler = AnnotatingErrorHandler()
    annotator = await run_annotating_processor(handler, failing_pool, offsets=(0,))

    entry = captured_kind(annotator, 'from_on_error')
    assert entry.hook == 'on_error'
    assert entry.task_id == 'fail-0'
    assert entry.partition == 2


async def test_hook_context_is_cleared_after_the_window(echo_pool):
    await run_annotating_processor(AnnotatingHandler(), echo_pool)

    assert current_hook_context() is None


# --- logging context hygiene ---


class LeakProbeHandler(EchoHandler):
    """on_task_complete raises; on_message_complete snapshots the log context.

    Both hooks run in the SAME coroutine (_execute_and_track awaits the
    tracker finalisation), so the snapshot sees whatever the earlier hook
    left bound.
    """

    def __init__(self):
        super().__init__()
        self.snapshot: dict | None = None

    async def on_task_complete(self, result):
        raise RuntimeError('boom')

    async def on_message_complete(self, group):
        self.snapshot = dict(get_contextvars())
        return None


async def test_raising_on_task_complete_does_not_leak_its_context(echo_pool):
    # The hook's exception is caught by _execute_and_track, which keeps running
    # in the same coroutine. Releasing the binding after the try block left
    # ``task_id`` attached to later work — including the message-level hook,
    # which is not about that task at all.
    #
    # Asserted against the contextvars directly rather than captured logs:
    # ``structlog.get_logger()`` caches its bound logger on first use, so a
    # test-local ``configure`` silently does nothing once any earlier test has
    # logged. Reading the contextvar has no such ordering dependency.
    handler = LeakProbeHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
    )
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: handler.snapshot is not None, timeout=5)
    await proc.stop()

    assert handler.snapshot is not None
    assert 'task_id' not in handler.snapshot, (
        f'on_task_complete leaked task_id into the message-level hook: {handler.snapshot}'
    )
    assert handler.snapshot.get('hook') == 'on_message_complete'


# --- on_window_complete raising must not skip recording or commit ---


class RaisingWindowCompleteHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f'task-{msg.offset}',
                args=['hello'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]

    async def on_window_complete(self, results, source_messages):
        raise RuntimeError('window hook boom')


async def test_raising_on_window_complete_still_records_and_commits(echo_pool):
    """A raising on_window_complete is contained like on_message_complete.

    The hook runs inside a fire-and-forget task; before the fix its
    exception escaped, skipping the window's recorder event and the final
    _commit_now and surfacing only as an unretrieved-task warning.
    """
    handler = RaisingWindowCompleteHandler()
    recorder = MagicMock()
    committed: list[tuple[int, int]] = []

    async def on_commit(partition_id, offset):
        committed.append((partition_id, offset))

    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=on_commit,
        recorder=recorder,
    )

    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()
    await wait_for(lambda: any(c[1] == 2 for c in committed))
    await proc.stop()

    assert recorder.record_window_complete.call_count >= 1
    assert any(c[1] == 2 for c in committed)


# --- shutdown drain must respect window_size ---


async def test_shutdown_drain_chunks_backlog_into_window_size(echo_pool):
    """Stopping with a full queue drains it in window_size chunks.

    Before the fix the drain emptied the whole queue into a single window,
    handing arrange() an unbounded batch at every shutdown/rebalance.
    """
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=5,
    )

    for i in range(15):  # 3x window_size
        proc.enqueue(make_msg(offset=i))

    proc.start()
    # signal_stop before the loop's first await point: _running goes False
    # before the main loop ever checks it, so the whole backlog goes
    # through the shutdown drain path.
    proc.signal_stop()
    await proc.stop()

    assert handler.arrange_calls, 'drain did not process the queued messages'
    window_sizes = [n for n, _ in handler.arrange_calls]
    assert all(n <= 5 for n in window_sizes), f'drain exceeded window_size: {window_sizes}'
    assert sum(window_sizes) == 15


# ---------------------------------------------------------------------------
# Offset-commit coalescing
# ---------------------------------------------------------------------------


class _CountingCommits:
    """Records every commit the processor makes, in order."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    async def __call__(self, partition_id: int, offset: int) -> None:
        self.calls.append(offset)


async def test_finished_messages_share_one_commit_instead_of_one_each(echo_pool, monkeypatch):
    """N messages finishing back-to-back must not cost N broker round trips.

    Every completed message used to trigger a synchronous commit under the
    partition's commit lock. With a low fan-out handler that is one round
    trip per message, and completions queue behind the lock. Commits are now
    coalesced by count or by a short timer, whichever comes first — the
    watermark and the at-least-once guarantee are untouched, only the
    frequency changes.
    """
    commits = _CountingCommits()
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=commits,
    )

    message_count = 20
    for offset in range(message_count):
        proc.enqueue(make_msg(offset=offset))

    proc.start()
    await wait_for(lambda: len(handler.collect_calls) == message_count)
    await proc.stop()

    # The final drain commit is always forced, so at least one.
    assert commits.calls, 'nothing was ever committed'
    assert len(commits.calls) < message_count, (
        f'{len(commits.calls)} commits for {message_count} messages — not coalescing'
    )
    # Nothing lost: the last commit covers every message.
    assert commits.calls[-1] == message_count


async def test_commit_is_forced_once_the_batch_size_is_reached(echo_pool, monkeypatch):
    """The count trigger fires without waiting for the timer."""
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_OFFSETS', 5)
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 30.0)

    commits = _CountingCommits()
    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=commits,
    )
    for offset in range(5):
        proc.enqueue(make_msg(offset=offset))

    proc.start()
    # The 30 s timer cannot have fired; only the count trigger can commit here.
    await wait_for(lambda: commits.calls == [5])
    await proc.stop()


async def test_commit_is_forced_by_the_timer_below_the_batch_size(echo_pool, monkeypatch):
    """A trickle of messages still commits promptly — it must not wait for
    the batch to fill, or a quiet partition would never advance."""
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_OFFSETS', 10_000)
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 0.05)

    commits = _CountingCommits()
    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=commits,
    )
    proc.enqueue(make_msg(offset=0))

    proc.start()
    try:
        await wait_for(lambda: commits.calls == [1])
    finally:
        await proc.stop()


async def test_stop_flushes_a_deferred_commit(echo_pool, monkeypatch):
    """Shutdown must not leave a coalesced commit unsent — those offsets
    would be re-delivered to the next owner for no reason."""
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_OFFSETS', 10_000)
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 30.0)

    commits = _CountingCommits()
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=10,
        on_commit=commits,
    )
    for offset in range(3):
        proc.enqueue(make_msg(offset=offset))

    proc.start()
    await wait_for(lambda: len(handler.collect_calls) == 3)
    assert commits.calls == [], 'the batch and timer thresholds should both still be far away'

    await proc.stop()
    assert commits.calls == [3]


async def test_a_suppressed_partition_never_commits_on_the_timer(echo_pool, monkeypatch):
    """A revoked partition belongs to another worker; a pending coalesced
    commit must not fire and clobber the new owner's progress."""
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)

    commits = _CountingCommits()
    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=commits,
    )
    proc._offset_tracker.register(0)
    proc._offset_tracker.complete(0)
    proc._deliveries_suppressed = True

    await proc._note_commit_due()
    await asyncio.sleep(0.05)
    assert commits.calls == []


async def test_coalescing_never_loses_an_offset_across_a_drain(echo_pool, monkeypatch):
    """The safety property the batching must not break.

    With both triggers pushed out of reach, every commit in this test comes
    from a forced flush. The watermark must still end at exactly the number
    of messages processed — no offset may be skipped, and none may be
    committed before its message finished.
    """
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_OFFSETS', 10_000)
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 30.0)

    commits = _CountingCommits()
    handler = EchoHandler()
    proc = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=echo_pool,
        window_size=4,
        on_commit=commits,
    )
    message_count = 12
    for offset in range(message_count):
        proc.enqueue(make_msg(offset=offset))

    proc.start()
    await wait_for(lambda: len(handler.collect_calls) == message_count)
    proc.signal_stop()
    await proc.drain()

    assert commits.calls[-1] == message_count
    # A commit is a watermark, so the sequence must never go backwards.
    assert commits.calls == sorted(commits.calls)


async def test_deferred_commit_survives_a_failing_broker(echo_pool, monkeypatch):
    """A failed coalesced commit leaves the offsets pending for the retry —
    it must not acknowledge, and must not kill the timer for good."""
    monkeypatch.setattr('drakkar.partition.COMMIT_BATCH_MAX_DELAY_SECONDS', 0.01)
    attempts: list[int] = []

    async def flaky_commit(partition_id: int, offset: int) -> None:
        attempts.append(offset)
        if len(attempts) == 1:
            raise RuntimeError('broker unavailable')

    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
        on_commit=flaky_commit,
    )
    proc._offset_tracker.register(0)
    proc._offset_tracker.complete(0)

    await proc._note_commit_due()
    await wait_for(lambda: len(attempts) >= 1)
    assert proc._offset_tracker.last_committed is None  # the failure was not acknowledged

    await proc._note_commit_due()
    await wait_for(lambda: proc._offset_tracker.last_committed == 1)
    await proc._cancel_commit_flush()


# --- Zombie tasks after a drain timeout ---


class _SleeperHandler(BaseDrakkarHandler):
    """Arranges one long-running subprocess per message."""

    def __init__(self, seconds: float = 30.0) -> None:
        self.seconds = seconds

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f'sleep-{m.offset}',
                args=['-c', f'import time; time.sleep({self.seconds})'],
                source_offsets=[m.offset],
            )
            for m in messages
        ]


def _sleeper_pool(max_executors: int = 2) -> ExecutorPool:
    return ExecutorPool(
        binary_path=sys.executable,
        max_executors=max_executors,
        task_timeout_seconds=120,
    )


async def test_cancel_active_tasks_frees_executor_slots_and_kills_subprocesses():
    """A zombie holds its priority-gate slot and its subprocess until
    ``task_timeout_seconds`` — two minutes by default — while the partitions
    this worker still owns queue behind it. Cancelling releases both at once.
    """
    pool = _sleeper_pool(max_executors=2)
    proc = PartitionProcessor(
        partition_id=0,
        handler=_SleeperHandler(),
        executor_pool=pool,
        window_size=10,
    )
    proc.enqueue(make_msg(offset=0))
    proc.enqueue(make_msg(offset=1))
    proc.start()

    await wait_for(lambda: pool.active_count == 2, timeout=5)

    cancelled = await proc.cancel_active_tasks()

    assert cancelled == 2
    assert proc._active_tasks == set() or all(t.done() for t in proc._active_tasks)
    assert proc.inflight_count == 0, 'the finally in _execute_and_track still decrements'
    await wait_for(lambda: pool.active_count == 0, timeout=5)

    proc.signal_stop()
    await proc.stop(timeout=1.0)


async def test_cancel_active_tasks_leaves_the_offsets_uncommitted():
    """``CancelledError`` is a ``BaseException``, so it propagates past the
    tracker settlement that follows ``_execute_and_track``'s ``finally``. The
    cancelled task's offsets therefore stay pending — committing past work
    this worker abandoned would lose it (AGENTS.md invariant 3).
    """
    committed: list[dict] = []

    async def commit(offsets):
        committed.append(offsets)

    pool = _sleeper_pool()
    proc = PartitionProcessor(
        partition_id=0,
        handler=_SleeperHandler(),
        executor_pool=pool,
        window_size=10,
        on_commit=commit,
    )
    proc.enqueue(make_msg(offset=7))
    proc.start()
    await wait_for(lambda: proc.inflight_count == 1, timeout=5)

    await proc.cancel_active_tasks()
    await proc._commit_now()

    assert proc.offset_tracker.committable() is None
    assert committed == []

    proc.signal_stop()
    await proc.stop(timeout=1.0)


async def test_cancel_active_tasks_is_a_noop_without_in_flight_work(echo_pool):
    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
    )
    assert await proc.cancel_active_tasks() == 0


async def test_stop_after_suppress_does_not_wait_out_its_timeout():
    """After a drain timeout ``_run`` can never leave its drain loop: it spins
    while the zombies are counted in flight. ``stop()`` therefore always burnt
    its whole grace period — on the thread librdkafka's rebalance callback is
    blocked in. Suppressed processors now cancel first.
    """
    pool = _sleeper_pool()
    proc = PartitionProcessor(
        partition_id=0,
        handler=_SleeperHandler(),
        executor_pool=pool,
        window_size=10,
    )
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: proc.inflight_count == 1, timeout=5)

    proc.suppress_deliveries()
    start = asyncio.get_running_loop().time()
    await proc.stop(timeout=10.0)
    elapsed = asyncio.get_running_loop().time() - start

    assert elapsed < 5.0, f'stop() waited {elapsed:.1f}s for a zombie it could cancel'
    assert proc.inflight_count == 0


async def test_stop_cancels_tasks_the_cancelled_loop_left_behind(echo_pool, monkeypatch):
    """Cancelling ``_run`` does not reach the task coroutines it spawned — they
    are separate tasks. Without the sweep at the end of ``stop()`` they keep an
    executor slot and a subprocess with no owner left to read the result.
    """
    pool = _sleeper_pool()
    proc = PartitionProcessor(
        partition_id=0,
        handler=_SleeperHandler(),
        executor_pool=pool,
        window_size=10,
    )
    proc.enqueue(make_msg(offset=0))
    proc.start()
    await wait_for(lambda: proc.inflight_count == 1, timeout=5)

    # Not suppressed: this is the plain shutdown path, where ``stop()`` waits
    # for ``_run`` and then force-cancels it.
    await proc.stop(timeout=0.2)

    assert proc.inflight_count == 0
    await wait_for(lambda: pool.active_count == 0, timeout=5)


async def test_dead_processor_drain_returns_without_waiting(echo_pool):
    """A dead processor has no loop left to empty its queue, so draining it
    always burnt the caller's full budget and then suppressed a commit that
    was in fact safe to make.
    """
    proc = PartitionProcessor(
        partition_id=0,
        handler=EchoHandler(),
        executor_pool=echo_pool,
        window_size=10,
    )
    proc.enqueue(make_msg(offset=0))
    proc._dead = True

    await asyncio.wait_for(proc.drain(), timeout=1.0)
    assert proc.queue_size == 1, 'drain must not consume the queue of a dead processor'
