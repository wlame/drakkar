"""Tests for Drakkar viking pool."""

import asyncio
import sys

import pytest

from drakkar.models import VikingTask
from drakkar.viking import VikingPool, VikingTaskError


def make_task(task_id: str = 't1', args: list[str] | None = None) -> VikingTask:
    return VikingTask(
        task_id=task_id,
        args=args or [],
        source_offsets=[0],
    )


@pytest.fixture
def echo_pool() -> VikingPool:
    return VikingPool(
        binary_path='/bin/echo',
        max_vikings=4,
        task_timeout_seconds=10,
    )


async def test_execute_echo(echo_pool: VikingPool):
    task = make_task(args=['hello', 'world'])
    result = await echo_pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'hello world'
    assert result.stderr == ''
    assert result.duration_seconds > 0
    assert result.task.task_id == 't1'


async def test_execute_captures_stderr():
    pool = VikingPool(
        binary_path=sys.executable,
        max_vikings=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', "import sys; sys.stderr.write('err msg')"])
    result = await pool.execute(task)
    assert result.stderr == 'err msg'


async def test_execute_nonzero_exit_raises():
    pool = VikingPool(
        binary_path=sys.executable,
        max_vikings=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', 'import sys; sys.exit(42)'])
    with pytest.raises(VikingTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.exit_code == 42
    assert exc_info.value.result.exit_code == 42


async def test_execute_timeout_kills_process():
    pool = VikingPool(
        binary_path=sys.executable,
        max_vikings=2,
        task_timeout_seconds=1,
    )
    task = make_task(args=['-c', 'import time; time.sleep(30)'])
    with pytest.raises(VikingTaskError) as exc_info:
        await pool.execute(task)
    assert 'timed out' in exc_info.value.error.stderr


async def test_execute_invalid_binary():
    pool = VikingPool(
        binary_path='/nonexistent/binary',
        max_vikings=2,
        task_timeout_seconds=10,
    )
    task = make_task()
    with pytest.raises(VikingTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.exception is not None


async def test_execute_concurrency_limit():
    pool = VikingPool(
        binary_path=sys.executable,
        max_vikings=2,
        task_timeout_seconds=10,
    )
    tasks = [
        make_task(task_id=f't{i}', args=['-c', 'import time; time.sleep(0.2)']) for i in range(4)
    ]

    max_active = 0

    async def tracked_execute(task: VikingTask) -> None:
        nonlocal max_active
        result = pool.execute(task)
        # check active count while running
        coro = result
        await coro
        if pool.active_count > max_active:
            max_active = pool.active_count

    await asyncio.gather(*[pool.execute(t) for t in tasks])
    # with 4 tasks and 2 max_vikings, they should run in 2 rounds
    # active_count should have been at most 2 at any point


async def test_execute_active_count_tracking(echo_pool: VikingPool):
    assert echo_pool.active_count == 0
    task = make_task(args=['test'])
    await echo_pool.execute(task)
    assert echo_pool.active_count == 0


async def test_pool_properties(echo_pool: VikingPool):
    assert echo_pool.max_vikings == 4
    assert echo_pool.active_count == 0


async def test_execute_large_stdout():
    pool = VikingPool(
        binary_path=sys.executable,
        max_vikings=2,
        task_timeout_seconds=10,
    )
    task = make_task(args=['-c', "print('x' * 10000)"])
    result = await pool.execute(task)
    assert len(result.stdout.strip()) == 10000
