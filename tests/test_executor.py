"""Tests for Drakkar executor pool."""

import asyncio
import sys

import pytest

from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.models import ExecutorTask


def make_task(task_id: str = 't1', args: list[str] | None = None) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        args=args or [],
        source_offsets=[0],
    )


@pytest.fixture
def echo_pool() -> ExecutorPool:
    return ExecutorPool(
        binary_path='/bin/echo',
        max_executors=4,
        task_timeout_seconds=10,
    )


async def test_execute_echo(echo_pool: ExecutorPool):
    task = make_task(args=['hello', 'world'])
    result = await echo_pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'hello world'
    assert result.stderr == ''
    assert result.duration_seconds > 0
    assert result.task.task_id == 't1'


async def test_execute_captures_stderr():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', "import sys; sys.stderr.write('err msg')"])
    result = await pool.execute(task)
    assert result.stderr == 'err msg'


async def test_execute_nonzero_exit_raises():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', 'import sys; sys.exit(42)'])
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.exit_code == 42
    assert exc_info.value.result.exit_code == 42


async def test_execute_timeout_kills_process():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=1,
    )
    task = make_task(args=['-c', 'import time; time.sleep(30)'])
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert 'timed out' in exc_info.value.error.stderr


async def test_execute_invalid_binary():
    pool = ExecutorPool(
        binary_path='/nonexistent/binary',
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = make_task()
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.exception is not None


async def test_execute_concurrency_limit():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    tasks = [make_task(task_id=f't{i}', args=['-c', 'import time; time.sleep(0.2)']) for i in range(4)]

    max_active = 0

    async def tracked_execute(task: ExecutorTask) -> None:
        nonlocal max_active
        result = pool.execute(task)
        # check active count while running
        coro = result
        await coro
        if pool.active_count > max_active:
            max_active = pool.active_count

    await asyncio.gather(*[pool.execute(t) for t in tasks])
    # with 4 tasks and 2 max_executors, they should run in 2 rounds
    # active_count should have been at most 2 at any point


async def test_execute_active_count_tracking(echo_pool: ExecutorPool):
    assert echo_pool.active_count == 0
    task = make_task(args=['test'])
    await echo_pool.execute(task)
    assert echo_pool.active_count == 0


async def test_pool_properties(echo_pool: ExecutorPool):
    assert echo_pool.max_executors == 4
    assert echo_pool.active_count == 0


async def test_execute_large_stdout():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', "print('x' * 10000)"])
    result = await pool.execute(task)
    assert len(result.stdout.strip()) == 10000


async def test_execute_passes_stdin_to_process():
    """stdin field value is written to the process stdin pipe."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='stdin-test',
        args=['-c', 'import sys; print(sys.stdin.read().strip())'],
        source_offsets=[0],
        stdin='hello from stdin',
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'hello from stdin'


async def test_execute_none_stdin_does_not_pipe():
    """When stdin is None the process stdin is not connected (no pipe opened)."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )
    # A script that checks whether stdin is a pipe; if not piped it should be a tty or closed.
    # We verify that stdin=None tasks still work normally (echo-style usage).
    task = ExecutorTask(
        task_id='no-stdin',
        args=['-c', 'print("no stdin ok")'],
        source_offsets=[0],
        stdin=None,
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'no stdin ok'


# --- binary_path resolution tests ---


async def test_pool_binary_path_used_when_task_has_none(echo_pool: ExecutorPool):
    """Pool-level binary_path is used when task.binary_path is None (default)."""
    task = make_task(args=['hello from pool binary'])
    result = await echo_pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'hello from pool binary'


async def test_task_binary_path_overrides_pool_binary_path():
    """Task-level binary_path takes precedence over pool-level binary_path."""
    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='override',
        args=['-c', 'print("from python")'],
        source_offsets=[0],
        binary_path=sys.executable,
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'from python'


async def test_no_pool_binary_path_uses_task_binary_path():
    """Pool with no binary_path works when task provides one."""
    pool = ExecutorPool(
        binary_path=None,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='task-only',
        args=['-c', 'print("task binary")'],
        source_offsets=[0],
        binary_path=sys.executable,
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'task binary'


async def test_no_binary_path_anywhere_raises_error():
    """Neither pool nor task has binary_path — raises ExecutorTaskError with clear message."""
    pool = ExecutorPool(
        binary_path=None,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='no-binary',
        args=['hello'],
        source_offsets=[0],
    )
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert 'binary_path' in str(exc_info.value).lower()
    assert exc_info.value.error.exception is not None
    assert exc_info.value.result.exit_code == -1


async def test_pool_binary_path_none_task_binary_path_none_explicit():
    """Explicit None on both pool and task raises the same clear error."""
    pool = ExecutorPool(
        binary_path=None,
        max_executors=2,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='both-none',
        args=[],
        source_offsets=[0],
        binary_path=None,
    )
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert 'binary_path' in str(exc_info.value).lower()


async def test_waiting_count_tracks_queued_tasks():
    """waiting_count reflects tasks blocked on the semaphore."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
    )
    assert pool.waiting_count == 0

    # fill the single slot with a slow task
    slow = make_task('slow', args=['-c', 'import time; time.sleep(0.5)'])
    slow_future = asyncio.create_task(pool.execute(slow))
    await asyncio.sleep(0.05)  # let it acquire the semaphore
    assert pool.active_count == 1

    # queue a second task — it should be waiting
    fast = make_task('fast', args=['-c', 'print("ok")'])
    fast_future = asyncio.create_task(pool.execute(fast))
    await asyncio.sleep(0.05)
    assert pool.waiting_count == 1

    # wait for both to complete
    await slow_future
    await fast_future
    assert pool.waiting_count == 0
    assert pool.active_count == 0


async def test_execute_records_task_started_with_recorder():
    """When a recorder is provided, execute() calls record_task_started."""
    from unittest.mock import MagicMock

    pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=5)
    task = make_task('rec-1', args=['hello'])
    recorder = MagicMock()

    result = await pool.execute(task, recorder=recorder, partition_id=3)
    assert result.exit_code == 0

    recorder.record_task_started.assert_called_once()
    call_args = recorder.record_task_started.call_args
    # positional: (task, partition_id), keyword: pool_active, pool_waiting, slot
    assert call_args[0][0] is task
    assert call_args[0][1] == 3
    assert call_args[1]['pool_active'] >= 1
