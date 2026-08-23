"""Tests for Drakkar executor pool."""

import asyncio
import contextlib
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from structlog.testing import capture_logs

from drakkar.executor import ExecutorPool, ExecutorTaskError, _trim_incomplete_utf8
from drakkar.models import ExecutorTask, PrecomputedResult
from tests.conftest import wait_for


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
    started_at = time.monotonic()
    result = await echo_pool.execute(task)
    observed_elapsed = time.monotonic() - started_at
    assert result.exit_code == 0
    assert result.stdout.strip() == 'hello world'
    assert result.stderr == ''
    # duration_seconds is rounded to milliseconds (see executor.py), so a
    # subprocess that finishes in well under 0.5ms legitimately rounds to
    # 0.0 — asserting ``> 0`` flakes under CI load. Instead confirm the
    # value is a real, bounded measurement: non-negative, and no larger
    # than the wall-clock time this test itself observed around the call
    # (with slack for scheduling jitter at the measurement boundary).
    # That still catches a regression that stops measuring real time
    # (e.g. a hardcoded or wildly-wrong duration), which is what this
    # assertion is actually meant to guard against.
    assert 0 <= result.duration_seconds <= observed_elapsed + 0.05
    assert result.task.task_id == 't1'


async def test_execute_measures_spawn_time(echo_pool: ExecutorPool):
    task = make_task(args=['hi'])
    started_at = time.monotonic()
    result = await echo_pool.execute(task)
    observed_elapsed = time.monotonic() - started_at
    # Same bounded-measurement idea as duration_seconds above: spawn is a
    # real slice of the observed wall time, never negative, never larger
    # than the whole call.
    assert result.spawn_seconds is not None
    assert 0 <= result.spawn_seconds <= observed_elapsed + 0.05


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
    assert exc_info.value.error.kind == 'nonzero_exit'


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
    assert exc_info.value.error.kind == 'timeout'


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
    assert exc_info.value.error.kind == 'launch_failure'


async def test_execute_concurrency_limit():
    """The semaphore in ExecutorPool.execute must cap concurrent subprocess
    runs at ``max_executors``. This test launches many overlapping tasks
    and samples ``pool.active_count`` from a concurrent sampler coroutine.

    Two assertions make the test meaningful:
    - ``max_observed <= max_executors``  — the cap is respected
    - ``max_observed >= max_executors``  — the pool actually saturated
      (without this, a test that never hits the limit would pass even
      if the semaphore were removed)
    """
    max_executors = 2
    num_tasks = 6
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=max_executors,
        task_timeout_seconds=10,
    )
    # Each task sleeps long enough that many overlap — ensures contention.
    tasks = [make_task(task_id=f't{i}', args=['-c', 'import time; time.sleep(0.15)']) for i in range(num_tasks)]

    max_observed_active = 0
    sampler_stop = asyncio.Event()

    async def sample_active_count() -> None:
        """Poll pool.active_count until signalled to stop.

        Runs in parallel with the gather() below and records the peak.
        Must start with zero and only reflect real slot occupancy.
        """
        nonlocal max_observed_active
        while not sampler_stop.is_set():
            if pool.active_count > max_observed_active:
                max_observed_active = pool.active_count
            await asyncio.sleep(0.005)

    sampler = asyncio.create_task(sample_active_count())
    try:
        await asyncio.gather(*[pool.execute(t) for t in tasks])
    finally:
        sampler_stop.set()
        await sampler

    # Cap is enforced — never more than max_executors running at once.
    assert max_observed_active <= max_executors, f'pool exceeded cap: observed {max_observed_active} > {max_executors}'
    # Pool actually saturated — proves the test exercised the slow path.
    # If the semaphore were removed, the first assertion could still hold
    # by accident; this second assertion would fail (we'd see > 2) OR
    # it confirms we at least reached the cap.
    assert max_observed_active >= max_executors, (
        f'pool never saturated: observed peak {max_observed_active} < {max_executors} — '
        'test did not exercise the semaphore'
    )
    # After everything completes, counters return to zero.
    assert pool.active_count == 0
    assert pool.waiting_count == 0


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
    assert exc_info.value.error.kind == 'launch_failure'


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
    await wait_for(lambda: pool.active_count == 1)  # let it acquire the semaphore

    # queue a second task — it should be waiting
    fast = make_task('fast', args=['-c', 'print("ok")'])
    fast_future = asyncio.create_task(pool.execute(fast))
    await wait_for(lambda: pool.waiting_count == 1)

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


# --- Environment variable tests ---


async def test_no_env_inherits_parent():
    """When no env is configured, subprocess inherits parent environment."""
    pool = ExecutorPool(binary_path=sys.executable, max_executors=2, task_timeout_seconds=10)
    task = make_task(args=['-c', 'import os; print(os.environ.get("PATH", ""))'])
    result = await pool.execute(task)
    assert result.exit_code == 0
    # PATH should be inherited from parent
    assert result.stdout.strip() != ''


async def test_config_env_passed_to_subprocess():
    """Env vars from executor config are available in the subprocess."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        env={'DK_TEST_CONFIG_VAR': 'from_config'},
    )
    task = make_task(args=['-c', 'import os; print(os.environ.get("DK_TEST_CONFIG_VAR", ""))'])
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'from_config'


async def test_task_env_passed_to_subprocess():
    """Env vars from ExecutorTask.env are available in the subprocess."""
    pool = ExecutorPool(binary_path=sys.executable, max_executors=2, task_timeout_seconds=10)
    task = ExecutorTask(
        task_id='t-env',
        args=['-c', 'import os; print(os.environ.get("DK_TEST_TASK_VAR", ""))'],
        source_offsets=[0],
        env={'DK_TEST_TASK_VAR': 'from_task'},
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() == 'from_task'


async def test_task_env_overrides_config_env():
    """Task env overrides config env on key conflict."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        env={'SHARED_VAR': 'from_config', 'CONFIG_ONLY': 'yes'},
    )
    task = ExecutorTask(
        task_id='t-override',
        args=['-c', 'import os; print(os.environ["SHARED_VAR"], os.environ["CONFIG_ONLY"])'],
        source_offsets=[0],
        env={'SHARED_VAR': 'from_task'},
    )
    result = await pool.execute(task)
    assert result.exit_code == 0
    parts = result.stdout.strip().split()
    assert parts[0] == 'from_task'  # task overrides config
    assert parts[1] == 'yes'  # config-only var still present


async def test_env_includes_parent_environment():
    """Merged env includes parent process env vars (like PATH)."""
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        env={'DK_TEST_EXTRA': 'extra'},
    )
    task = make_task(args=['-c', 'import os; print(os.environ.get("PATH", "missing"))'])
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout.strip() != 'missing'


async def test_build_env_returns_none_when_no_custom_env():
    """_build_env returns None when no config env and no task env."""
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=1, task_timeout_seconds=10)
    task = make_task()
    assert pool._build_env(task) is None


async def test_build_env_merges_all_three_layers():
    """_build_env merges parent + config + task with correct priority."""
    import os

    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=10,
        env={'A': 'config', 'B': 'config'},
    )
    task = ExecutorTask(
        task_id='t-merge',
        args=[],
        source_offsets=[0],
        env={'B': 'task', 'C': 'task'},
    )
    result = pool._build_env(task)
    assert result is not None
    assert result['A'] == 'config'  # from config
    assert result['B'] == 'task'  # task overrides config
    assert result['C'] == 'task'  # from task only
    assert result.get('PATH') == os.environ.get('PATH')  # from parent


# --- Bug #2: Cancellation during execute() must not leak pool slots ---


async def test_cancel_while_waiting_does_not_leak_waiting_count():
    """Cancelling execute() while it is blocked on the semaphore must restore
    the waiting counter. Before the fix, the `self._waiting_count += 1` was
    unpaired when cancellation fired before the `async with` body ran.
    """
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
    )
    slow = make_task('slow-hold', args=['-c', 'import time; time.sleep(1.0)'])
    slow_future = asyncio.create_task(pool.execute(slow))
    await wait_for(lambda: pool.active_count == 1)

    waiter = make_task('waiter', args=['-c', 'pass'])
    waiter_future = asyncio.create_task(pool.execute(waiter))
    await wait_for(lambda: pool.waiting_count == 1)

    waiter_future.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_future

    assert pool.waiting_count == 0

    await slow_future
    assert pool.active_count == 0
    assert pool.waiting_count == 0


async def test_cancel_during_execution_does_not_leak_active_count():
    """Cancelling an in-flight execute() must return the slot to the pool.

    A sequence of cancel-and-retry calls must not permanently shrink the
    usable pool. Pre-fix, certain cancellation paths left _active_count
    non-zero, causing the effective concurrency limit to degrade over time.
    """
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
    )

    for i in range(4):
        task = make_task(f'run-{i}', args=['-c', 'import time; time.sleep(1.0)'])
        fut = asyncio.create_task(pool.execute(task))
        await asyncio.sleep(0.05)
        fut.cancel()
        with pytest.raises((asyncio.CancelledError, ExecutorTaskError)):
            await fut

    assert pool.active_count == 0
    assert pool.waiting_count == 0

    good = make_task('good', args=['-c', 'print("ok")'])
    result = await pool.execute(good)
    assert result.exit_code == 0


# --- H4: proc.returncode None must not silently be treated as success ---


# --- H6: subprocess env leak — deny-list filtering ---


async def test_inherit_deny_patterns_filter_drakkar_internals(monkeypatch):
    """DK_* env vars must not leak into the subprocess by default.

    Framework internals (e.g. DK_SINKS__POSTGRES__MAIN__DSN) can
    contain connection strings with embedded credentials. Inheriting them
    unfiltered exposes those secrets to the executor binary.
    """
    monkeypatch.setenv('DK_SINKS__POSTGRES__MAIN__DSN', 'postgres://u:p@h/db')
    monkeypatch.setenv('OTHER_OK_VAR', 'visible')

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
        inherit_deny_patterns=['DK_*'],
    )
    task = make_task(
        args=[
            '-c',
            (
                'import os; '
                'print(os.environ.get("DK_SINKS__POSTGRES__MAIN__DSN", "<missing>")); '
                'print(os.environ.get("OTHER_OK_VAR", "<missing>"))'
            ),
        ],
    )
    result = await pool.execute(task)
    lines = result.stdout.strip().splitlines()
    assert lines[0] == '<missing>', f'DK_* var leaked: {lines[0]!r}'
    assert lines[1] == 'visible', 'non-secret var should still pass through'


async def test_inherit_deny_patterns_filter_common_secret_shapes(monkeypatch):
    """Common secret-name shapes must not leak with the default deny list."""
    monkeypatch.setenv('DB_PASSWORD', 'supersecret')
    monkeypatch.setenv('GITHUB_TOKEN', 'ghp_xxx')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'abc')
    monkeypatch.setenv('NORMAL_VAR', 'ok')

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
        inherit_deny_patterns=['*PASSWORD*', '*TOKEN*', '*SECRET*', '*_KEY'],
    )
    task = make_task(
        args=[
            '-c',
            (
                'import os; '
                'print("DB_PASSWORD", os.environ.get("DB_PASSWORD", "<missing>")); '
                'print("GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", "<missing>")); '
                'print("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "<missing>")); '
                'print("NORMAL_VAR", os.environ.get("NORMAL_VAR", "<missing>"))'
            ),
        ],
    )
    result = await pool.execute(task)
    out = result.stdout
    assert 'DB_PASSWORD <missing>' in out
    assert 'GITHUB_TOKEN <missing>' in out
    assert 'AWS_SECRET_ACCESS_KEY <missing>' in out
    assert 'NORMAL_VAR ok' in out


async def test_inherit_parent_env_false_strips_all_parent_env(monkeypatch):
    """inherit_parent_env=False makes subprocess env strictly config+task."""
    monkeypatch.setenv('PARENT_ONLY_VAR', 'from_parent')

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
        env={'FROM_CONFIG': 'config_value'},
        inherit_parent_env=False,
    )
    task = make_task(
        args=[
            '-c',
            (
                'import os; '
                'print("P:", os.environ.get("PARENT_ONLY_VAR", "<missing>")); '
                'print("C:", os.environ.get("FROM_CONFIG", "<missing>"))'
            ),
        ],
    )
    result = await pool.execute(task)
    assert 'P: <missing>' in result.stdout, 'parent env must be stripped'
    assert 'C: config_value' in result.stdout, 'config env must still pass'


async def test_is_env_key_denied_is_case_insensitive():
    """Deny patterns match regardless of casing — env vars vary in convention."""
    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=10,
        inherit_deny_patterns=['DK_*'],
    )
    assert pool._is_env_key_denied('DK_FOO')
    assert pool._is_env_key_denied('dk_foo')
    assert not pool._is_env_key_denied('HAS_DK_SUFFIX')
    assert not pool._is_env_key_denied('OTHER')


# --- H4: proc.returncode None must not silently be treated as success ---


async def test_none_returncode_treated_as_failure(monkeypatch):
    """If `proc.returncode` is None after communicate() (unexpected under
    normal operation but possible under races), the result must be a
    non-zero exit_code so the pipeline does NOT commit offsets past a task
    that did not cleanly exit.

    Pre-fix: `proc.returncode or 0` masked None as 0 (success).
    """

    class FakeProc:
        pid = 12345
        returncode = None

        async def communicate(self, input=None):
            return b'some output', b''

        def kill(self) -> None:
            FakeProc.returncode = -9

        async def wait(self):
            return FakeProc.returncode

    async def fake_spawn(*args, **kwargs):
        return FakeProc()

    # Replace the subprocess spawner so we can force a None returncode.
    monkeypatch.setattr(asyncio, 'create_subprocess_' + 'exec', fake_spawn)

    pool = ExecutorPool(
        binary_path='/bin/fake',
        max_executors=1,
        task_timeout_seconds=10,
    )
    task = make_task('none-rc', args=['irrelevant'])

    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)

    assert exc_info.value.result.exit_code != 0
    assert exc_info.value.result.exit_code == -1


# =============================================================================
# Precomputed fast track — no subprocess, no pool slot
# =============================================================================


async def test_precomputed_success_returns_result_without_subprocess():
    """When task.precomputed is set, execute() must not spawn a subprocess.
    It returns an ExecutorResult built from the precomputed fields.
    """
    # Deliberately use a bogus binary — if the fast track broke, the
    # subprocess spawn would fail with ENOENT and this test would raise.
    pool = ExecutorPool(
        binary_path='/nonexistent/binary/should-never-run',
        max_executors=1,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='pc-1',
        source_offsets=[42],
        precomputed=PrecomputedResult(
            stdout='hello from cache',
            stderr='',
            exit_code=0,
            duration_seconds=0.002,
        ),
    )

    result = await pool.execute(task)

    assert result.exit_code == 0
    assert result.stdout == 'hello from cache'
    assert result.duration_seconds == 0.002
    assert result.task is task
    assert result.pid is None


async def test_precomputed_non_zero_exit_raises_executor_task_error():
    """Precomputed with exit_code != 0 raises ExecutorTaskError — identical
    semantics to a real subprocess failure, so on_error fires as usual.
    """
    pool = ExecutorPool(
        binary_path='/nonexistent/binary',
        max_executors=1,
        task_timeout_seconds=10,
    )
    task = ExecutorTask(
        task_id='pc-fail',
        source_offsets=[0],
        precomputed=PrecomputedResult(
            stdout='',
            stderr='cached-failure-stderr',
            exit_code=7,
        ),
    )

    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)

    assert exc_info.value.result.exit_code == 7
    assert exc_info.value.error.stderr == 'cached-failure-stderr'
    assert exc_info.value.error.pid is None
    assert exc_info.value.error.kind == 'nonzero_exit'


async def test_precomputed_does_not_consume_pool_slot():
    """Precomputed tasks don't use semaphore slots — running N precomputed
    tasks on a max_executors=1 pool must not serialise them (no slot wait).
    """
    pool = ExecutorPool(
        binary_path='/nonexistent/binary',
        max_executors=1,
        task_timeout_seconds=10,
    )

    # Hold the ONE real slot with a slow real task so any precomputed call
    # that mistakenly waits on the semaphore would deadlock the test.
    hold_pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=10,
    )
    blocker = asyncio.create_task(hold_pool.execute(make_task('blocker', args=['-c', 'import time; time.sleep(0.8)'])))
    await wait_for(lambda: hold_pool.active_count == 1)

    # Run 5 precomputed tasks on a DIFFERENT pool whose semaphore is size 1.
    # If they went through the slot path they'd serialise; here they must
    # all complete in parallel without touching the semaphore at all.
    started = time.monotonic()
    precomputed_results = await asyncio.gather(
        *[
            pool.execute(
                ExecutorTask(
                    task_id=f'pc-parallel-{i}',
                    source_offsets=[i],
                    precomputed=PrecomputedResult(stdout=f'result-{i}'),
                ),
            )
            for i in range(5)
        ]
    )
    elapsed = time.monotonic() - started

    # No pool contention → all 5 finish essentially instantly (< 100ms),
    # not serialised behind anything.
    assert elapsed < 0.2, f'precomputed tasks appear to have serialised: {elapsed}s for 5 tasks'
    assert [r.stdout for r in precomputed_results] == [f'result-{i}' for i in range(5)]
    # active_count never moved on the precomputed pool.
    assert pool.active_count == 0
    assert pool.waiting_count == 0

    await blocker


async def test_precomputed_preserves_duration_when_user_sets_it():
    """If the user supplies a non-zero duration_seconds (e.g. to reflect
    the cache lookup cost), it flows through into the ExecutorResult —
    it is not zeroed by the framework.
    """
    pool = ExecutorPool(binary_path='/bin/true', max_executors=1, task_timeout_seconds=10)
    task = ExecutorTask(
        task_id='pc-dur',
        source_offsets=[0],
        precomputed=PrecomputedResult(stdout='x', duration_seconds=0.123),
    )
    result = await pool.execute(task)
    assert result.duration_seconds == 0.123


async def test_precomputed_increments_tasks_precomputed_counter():
    """drakkar_tasks_precomputed_total increments ONCE per precomputed
    task and NOT for normal subprocess executions.
    """
    from drakkar.metrics import tasks_precomputed

    pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)

    before = tasks_precomputed._value.get()
    # Normal subprocess should NOT increment the counter.
    await pool.execute(make_task('normal', args=['hi']))
    assert tasks_precomputed._value.get() == before

    # Precomputed SHOULD increment it.
    await pool.execute(
        ExecutorTask(
            task_id='pc-m',
            source_offsets=[0],
            precomputed=PrecomputedResult(stdout='x'),
        )
    )
    assert tasks_precomputed._value.get() == before + 1


async def test_precomputed_works_without_binary_path():
    """A precomputed task can have no binary_path on the task AND no
    pool-level binary_path — nothing ever runs, so the binary is irrelevant.
    """
    # Pool without binary_path — would normally error when running a task
    # without its own binary_path. Should be fine for precomputed.
    pool = ExecutorPool(binary_path=None, max_executors=1, task_timeout_seconds=10)
    task = ExecutorTask(
        task_id='pc-nobinary',
        source_offsets=[0],
        precomputed=PrecomputedResult(stdout='ok'),
    )
    result = await pool.execute(task)
    assert result.stdout == 'ok'


# =============================================================================
# Process-group isolation on timeout (POSIX only)
# =============================================================================


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only: os.killpg / start_new_session')
async def test_timeout_kills_grandchildren_via_process_group(tmp_path):
    """A subprocess that forks a background grandchild must have the grandchild
    reaped when the parent times out.

    Before process-group isolation, killing just the parent would leave the
    grandchild running (orphaned, reparented to init). With
    ``start_new_session=True`` + ``os.killpg`` we signal the whole group.

    Strategy:
    - spawn ``bash -c "sleep 60 & echo $! > grandchild.pid; wait"``
    - task_timeout_seconds=1 forces the timeout path
    - after the timeout, poll ``os.kill(grandchild_pid, 0)`` — a signal of 0
      is the POSIX "does this process exist?" probe and raises
      ProcessLookupError once the grandchild is gone
    """
    import os as _os  # local alias to avoid conflicting with test helpers

    pid_file = tmp_path / 'grandchild.pid'

    # ``task_timeout_seconds=2`` gives the bash script a comfortable
    # window to launch ``sleep``, capture its PID via ``echo $!``, and
    # flush that redirection to disk BEFORE the timeout fires. 1s was
    # enough on a warm machine but flaked under CI load when the shell
    # startup cost ate into the budget — and if the PID file isn't on
    # disk yet the test cannot read it to verify the grandchild was
    # reaped.
    pool = ExecutorPool(
        binary_path='/bin/bash',
        max_executors=1,
        task_timeout_seconds=2,
    )
    # The bash script:
    #   1) launches `sleep 60` in the background (the grandchild)
    #   2) writes the grandchild PID to a file we can read from the test
    #   3) `wait`s so bash itself stays alive long enough for the timeout
    #      to fire (otherwise bash exits before we trigger the kill path)
    script = f'sleep 60 & echo $! > {pid_file}; wait'
    task = make_task(args=['-c', script])

    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert 'timed out' in exc_info.value.error.stderr

    # Read the grandchild PID that bash recorded before being killed.
    assert pid_file.exists(), 'grandchild PID file was never written — script timing is off'
    grandchild_pid = int(pid_file.read_text().strip())

    # killpg sends SIGKILL to the whole group; the kernel reaps the grandchild
    # asynchronously, so give it a generous grace window before asserting.
    # 5s (wait_for's default timeout) keeps the test robust on slow/overloaded
    # CI runners.
    def grandchild_gone() -> bool:
        try:
            _os.kill(grandchild_pid, 0)
        except ProcessLookupError:
            return True
        return False

    await wait_for(grandchild_gone)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only: os.killpg')
async def test_killpg_process_lookup_error_swallowed(monkeypatch):
    """If the child exits between ``getpgid`` and ``killpg`` (a benign race),
    the kernel raises ProcessLookupError. The executor must swallow it and
    complete the cleanup without propagating the error up the stack.

    We simulate the race by monkey-patching ``os.killpg`` to raise
    ProcessLookupError on the first call. If the swallow logic is missing,
    the error would surface from the task's ``finally`` and either mask the
    original TimeoutError or propagate as an unexpected exception.
    """
    import os as _os

    calls: list[int] = []

    def raising_killpg(pgid: int, sig: int) -> None:
        calls.append(pgid)
        raise ProcessLookupError('simulated race: child exited before killpg')

    monkeypatch.setattr(_os, 'killpg', raising_killpg)

    # Tight timings to keep the test fast: task_timeout=1s < child sleep=2s.
    # The timeout fires first → ``_kill_process_tree`` runs → mocked ``killpg``
    # raises ProcessLookupError, which the executor must swallow. Because the
    # mock does NOT actually signal the child, ``proc.wait()`` blocks until
    # the child exits naturally — bounded here to ~2s. Previously the child
    # slept for 30s which made this single test take the full 30 seconds.
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=1,
        task_timeout_seconds=1,
    )
    task = make_task(args=['-c', 'import time; time.sleep(2)'])

    # The expected exception is the timeout-driven ExecutorTaskError,
    # NOT a ProcessLookupError leaking out of the finally block.
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)

    assert 'timed out' in exc_info.value.error.stderr
    # Verify our patched killpg was actually called — otherwise the test
    # would pass trivially if the code path skipped killpg entirely.
    assert len(calls) == 1, f'expected exactly one killpg call, got {len(calls)}'


async def test_kill_process_tree_bounded_when_process_ignores_sigkill(monkeypatch):
    """A process wedged in uninterruptible sleep must not hang the shutdown path."""
    from unittest.mock import MagicMock

    from drakkar.executor import ExecutorPool

    proc = MagicMock()
    proc.pid = 4242

    async def never_returns():
        await asyncio.sleep(3600)

    proc.wait = never_returns
    monkeypatch.setattr('os.getpgid', lambda pid: pid)
    monkeypatch.setattr('os.killpg', lambda pgid, sig: None)
    # Shorten the reap-timeout constant so this test doesn't burn the real
    # production 5s wait on every run; the shipped value stays 5.0 in
    # drakkar/executor.py (_KILL_REAP_TIMEOUT_SECONDS) and is not touched here.
    monkeypatch.setattr('drakkar.executor._KILL_REAP_TIMEOUT_SECONDS', 0.05)

    # Must return rather than hang. Outer bound is 40x the monkeypatched
    # timeout above — generous enough that a slow runner won't make this
    # flaky, while staying far below the real 5s production timeout so the
    # test itself stays fast.
    await asyncio.wait_for(ExecutorPool._kill_process_tree(proc), timeout=2.0)


# --- PriorityGate primitive --------------------------------------------------
#
# The gate is a custom semaphore-shaped allocator that selects the next
# waiter from a min-heap rather than a deque. Tests below pin down the
# four guarantees we rely on inside ExecutorPool:
#
#   1. Fast path (uncontended): acquire returns immediately, no heap touch.
#   2. Slow path (contended):   wakes up in priority order; equal-priority
#                               waiters tiebreak FIFO via the seq counter.
#   3. Cancellation:            cancelled waiters become tombstones in the
#                               heap and are skipped by release.
#   4. Cancellation race:       if a waiter is cancelled AFTER receiving
#                               the slot, the slot is released back so it
#                               isn't lost.


from drakkar.executor import PriorityGate, default_priority  # noqa: E402


async def test_priority_gate_fast_path_no_heap_touch():
    """When a slot is free and no one is waiting, acquire returns immediately."""
    gate = PriorityGate(2)
    await gate.acquire(priority=100)
    await gate.acquire(priority=200)
    assert gate.available == 0
    assert gate.waiters == 0


async def test_priority_gate_release_with_no_waiters_bumps_available():
    """Releasing on an empty heap restores the available counter."""
    gate = PriorityGate(1)
    await gate.acquire()
    assert gate.available == 0
    gate.release()
    assert gate.available == 1


async def test_priority_gate_slow_path_serves_lowest_priority_first():
    """Contended acquire wakes up smallest-priority waiter first."""
    gate = PriorityGate(1)
    await gate.acquire(priority=0)  # holder

    woken: list[int] = []

    async def waiter(p: int) -> None:
        await gate.acquire(priority=p)
        woken.append(p)
        gate.release()

    # Deliberately push tasks in INVERTED priority order so any FIFO
    # implementation would wake them in [9, 5, 1] rather than [1, 5, 9].
    tasks = [
        asyncio.create_task(waiter(9)),
        asyncio.create_task(waiter(5)),
        asyncio.create_task(waiter(1)),
    ]
    # Yield a few times so all three waiters reach the heap before the
    # first release fires.
    for _ in range(3):
        await asyncio.sleep(0)

    gate.release()  # frees the slot the holder claimed
    await asyncio.gather(*tasks)

    assert woken == [1, 5, 9]


async def test_priority_gate_equal_priority_is_fifo_tiebreak():
    """Waiters with the same priority wake in arrival order (stable)."""
    gate = PriorityGate(1)
    await gate.acquire()  # holder

    woken: list[str] = []

    async def waiter(name: str) -> None:
        await gate.acquire(priority=42)
        woken.append(name)
        gate.release()

    tasks = [asyncio.create_task(waiter('a'))]
    # Each await sleep gives the scheduler a chance to push 'a' onto the
    # heap before 'b', and 'b' before 'c'.
    await asyncio.sleep(0)
    tasks.append(asyncio.create_task(waiter('b')))
    await asyncio.sleep(0)
    tasks.append(asyncio.create_task(waiter('c')))
    await asyncio.sleep(0)

    gate.release()
    await asyncio.gather(*tasks)

    assert woken == ['a', 'b', 'c']


async def test_priority_gate_max_slots_zero_rejected():
    """The gate refuses ``max_slots < 1`` — an unusable configuration."""
    with pytest.raises(ValueError, match='max_slots must be >= 1'):
        PriorityGate(0)


async def test_priority_gate_cancelled_waiter_becomes_tombstone():
    """A waiter cancelled while pending is skipped by the next release."""
    gate = PriorityGate(1)
    await gate.acquire()  # holder

    woken: list[int] = []

    async def cancellable_waiter() -> None:
        await gate.acquire(priority=1)  # would wake first if alive
        woken.append(1)
        gate.release()

    async def survivor() -> None:
        await gate.acquire(priority=99)
        woken.append(99)
        gate.release()

    cancel_task = asyncio.create_task(cancellable_waiter())
    survivor_task = asyncio.create_task(survivor())
    # Both reach the heap before any release.
    for _ in range(3):
        await asyncio.sleep(0)

    # Cancel the priority-1 waiter; its heap entry stays as a tombstone.
    cancel_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await cancel_task

    # Release the holder's slot. The next release should pop the
    # tombstone, skip it, and resolve the priority-99 waiter.
    gate.release()
    await survivor_task

    # Cancelled waiter never completed; survivor did.
    assert woken == [99]


async def test_priority_gate_cancelled_after_slot_assigned_returns_slot():
    """If a waiter is cancelled AFTER receiving the slot, the slot is given back.

    This pins down the cancellation race in ``acquire``: ``release`` may
    have already resolved the future when ``CancelledError`` fires at
    the await point. The except-branch must call ``_release_slot`` so
    the slot is not silently leaked.
    """
    gate = PriorityGate(1)
    await gate.acquire()  # holder

    waiter_started = asyncio.Event()
    waiter_resumed = asyncio.Event()

    async def slow_consumer() -> None:
        waiter_started.set()
        await gate.acquire(priority=0)
        # If we ever get here, the test setup failed — the cancel below
        # should fire inside ``acquire`` after the slot is granted.
        waiter_resumed.set()
        gate.release()

    waiter_task = asyncio.create_task(slow_consumer())
    await waiter_started.wait()
    # Yield so the waiter is parked on its future.
    await asyncio.sleep(0)

    # Pop the heap so the waiter's future receives the slot, then
    # immediately cancel before the waiter resumes from ``await fut``.
    gate.release()  # this resolves the waiter's future
    waiter_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiter_task

    # The cancelled-after-resolution waiter must have given the slot back.
    # Available count returns to 1 (the holder's release path went to a
    # tombstone-resolved future, that future cancellation released the
    # slot back). Net: gate is fully released.
    assert gate.available == 1
    assert gate.waiters == 0
    # And the body past ``await fut`` was never reached.
    assert not waiter_resumed.is_set()


# --- default_priority -------------------------------------------------------


def test_default_priority_returns_min_source_offset():
    """Default priority is the smallest source_offset across the task's offsets."""
    task = ExecutorTask(task_id='t', source_offsets=[100, 50, 200])
    assert default_priority(task) == 50


def test_default_priority_zero_for_empty_source_offsets():
    """Edge case: tasks with no source_offsets all share priority 0.

    These degrade to FIFO via the gate's seq tiebreaker, which matches
    the pre-priority semaphore behaviour.
    """
    task = ExecutorTask(task_id='t', source_offsets=[])
    assert default_priority(task) == 0


# --- ExecutorPool priority integration ---------------------------------------
#
# These tests verify the wiring: ExecutorPool consults ``priority_fn`` to
# order pending tasks when the pool is saturated, and falls back gracefully
# when ``priority_fn`` raises.


async def test_executor_pool_orders_waiters_by_priority():
    """Saturated pool with priority_fn schedules tasks lowest-priority-first."""
    # Pool size 1 forces serialization. We submit four tasks; the priority
    # function reads ``task.metadata['p']`` so we can inject any order
    # we like and watch the actual execution sequence.
    completion_order: list[str] = []

    pool = ExecutorPool(
        binary_path='/bin/sh',
        max_executors=1,
        task_timeout_seconds=10,
        priority_fn=lambda task: task.metadata.get('p', 0),
    )

    async def run(task_id: str, p: int) -> None:
        task = ExecutorTask(
            task_id=task_id,
            args=['-c', f'echo {task_id}'],
            metadata={'p': p},
            source_offsets=[0],
        )
        result = await pool.execute(task)
        completion_order.append(task.task_id)
        assert result.exit_code == 0

    # Pre-saturate by claiming the only slot via a slow holder, then
    # queue three waiters with priorities [9, 1, 5] — non-monotone so
    # any FIFO scheduler would produce [9, 1, 5] rather than [1, 5, 9].
    holder_started = asyncio.Event()

    async def holder() -> None:
        task = ExecutorTask(
            task_id='holder',
            args=['-c', 'echo holder; sleep 0.05'],
            source_offsets=[0],
        )
        holder_started.set()
        result = await pool.execute(task)
        completion_order.append(task.task_id)
        assert result.exit_code == 0

    holder_task = asyncio.create_task(holder())
    await holder_started.wait()
    await wait_for(lambda: pool.active_count == 1)  # let the holder claim the slot

    waiters = [
        asyncio.create_task(run('p9', 9)),
        asyncio.create_task(run('p1', 1)),
        asyncio.create_task(run('p5', 5)),
    ]
    await asyncio.gather(holder_task, *waiters)

    # Holder finishes first (it had the slot from the start).
    # Then waiters drain in priority order: 1, 5, 9.
    assert completion_order == ['holder', 'p1', 'p5', 'p9']


async def test_executor_pooldefault_priority_is_min_source_offset():
    """When no priority_fn is supplied, smaller source offsets drain first."""
    completion_order: list[int] = []
    pool = ExecutorPool(
        binary_path='/bin/sh',
        max_executors=1,
        task_timeout_seconds=10,
    )

    async def run(offset: int) -> None:
        task = ExecutorTask(
            task_id=f't-{offset}',
            args=['-c', f'echo {offset}'],
            source_offsets=[offset],
        )
        await pool.execute(task)
        completion_order.append(offset)

    # Saturate with a slow holder, then queue waiters in shuffled order.
    holder_started = asyncio.Event()

    async def holder() -> None:
        task = ExecutorTask(
            task_id='holder',
            args=['-c', 'echo holder; sleep 0.05'],
            source_offsets=[-1],
        )
        holder_started.set()
        await pool.execute(task)
        completion_order.append(-1)

    holder_task = asyncio.create_task(holder())
    await holder_started.wait()
    await wait_for(lambda: pool.active_count == 1)

    waiters = [
        asyncio.create_task(run(300)),
        asyncio.create_task(run(100)),
        asyncio.create_task(run(200)),
    ]
    await asyncio.gather(holder_task, *waiters)

    # Holder runs first (priority -1, but it had the slot already).
    # Remaining drain by smallest offset: 100, 200, 300.
    assert completion_order == [-1, 100, 200, 300]


async def test_executor_pool_priority_fn_raises_falls_back_to_default(monkeypatch):
    """A buggy priority_fn must not stall a task; pool falls back + ticks metric."""
    from drakkar import metrics as metrics_module

    metric_calls = 0

    def fake_inc():
        nonlocal metric_calls
        metric_calls += 1

    monkeypatch.setattr(metrics_module.executor_priority_fn_errors, 'inc', fake_inc)

    def angry_priority(task: ExecutorTask) -> int:
        raise RuntimeError('priority hook is buggy')

    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=10,
        priority_fn=angry_priority,
    )
    task = make_task('t-fallback')
    result = await pool.execute(task)

    # Task still runs successfully despite the failing hook.
    assert result.exit_code == 0
    # And the metric was bumped exactly once for the one task.
    assert metric_calls == 1


async def test_executor_pool_priority_does_not_break_precomputed_fast_path():
    """Precomputed tasks skip the gate entirely; priority is irrelevant."""
    captured: list[ExecutorTask] = []

    def priority_fn(task: ExecutorTask) -> int:
        captured.append(task)
        return 0

    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=10,
        priority_fn=priority_fn,
    )
    task = ExecutorTask(
        task_id='t-pre',
        source_offsets=[0],
        precomputed=PrecomputedResult(stdout='cached', exit_code=0),
    )
    result = await pool.execute(task)

    assert result.stdout == 'cached'
    assert result.exit_code == 0
    # Precomputed tasks return BEFORE _compute_priority; priority_fn
    # must never be called for them.
    assert captured == []


async def test_stdout_cap_truncates_and_sets_flag():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=10,
    )
    task = make_task(args=['-c', "import sys; sys.stdout.write('x' * 100)"])
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout == 'x' * 10
    assert result.stdout_truncated is True
    assert result.stderr_truncated is False


async def test_stderr_cap_truncates_and_sets_flag():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stderr_bytes=10,
    )
    task = make_task(args=['-c', "import sys; sys.stderr.write('e' * 100)"])
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stderr == 'e' * 10
    assert result.stderr_truncated is True
    assert result.stdout_truncated is False


async def test_output_exactly_at_cap_not_truncated():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=10,
    )
    task = make_task(args=['-c', "import sys; sys.stdout.write('x' * 10)"])
    result = await pool.execute(task)
    assert result.stdout == 'x' * 10
    assert result.stdout_truncated is False


async def test_caps_apply_per_stream_independently():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=5,
    )
    task = make_task(args=['-c', "import sys; sys.stdout.write('o' * 20); sys.stderr.write('e' * 20)"])
    result = await pool.execute(task)
    assert result.stdout == 'ooooo'
    assert result.stdout_truncated is True
    assert result.stderr == 'e' * 20  # stderr cap unset - unlimited
    assert result.stderr_truncated is False


async def test_capped_capture_drains_past_cap_without_deadlock():
    """A child writing far more than the OS pipe buffer (~64 KiB) must still
    complete with a tiny cap: the reader has to keep draining past the cap.
    A reader that stopped would let the pipe fill, block the child, and turn
    this test into a 30 s timeout failure.
    """
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=30,
        max_stdout_bytes=1024,
    )
    task = make_task(args=['-c', "import sys; sys.stdout.write('y' * (2 * 1024 * 1024))"])
    result = await pool.execute(task)
    assert result.exit_code == 0
    assert result.stdout_truncated is True
    assert len(result.stdout) == 1024


async def test_cap_cut_mid_character_is_utf8_safe():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=5,
    )
    # Three e-acute characters = 6 bytes; the 5-byte cap lands mid-character.
    task = make_task(args=['-c', "import sys; sys.stdout.buffer.write('\\u00e9\\u00e9\\u00e9'.encode())"])
    result = await pool.execute(task)
    assert result.stdout == 'éé'  # 4 bytes retained after the trim
    assert result.stdout_truncated is True


async def test_stdin_still_delivered_with_cap_set():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=1000,
    )
    task = ExecutorTask(
        task_id='t-stdin-cap',
        args=['-c', 'import sys; sys.stdout.write(sys.stdin.read())'],
        stdin='ping',
        source_offsets=[0],
    )
    result = await pool.execute(task)
    assert result.stdout == 'ping'
    assert result.stdout_truncated is False


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        (b'hello', b'hello'),  # ASCII tail untouched
        (b'caf\xc3', b'caf'),  # 1 of 2 bytes of e-acute - strip
        (b'ab\xe2\x82', b'ab'),  # 2 of 3 bytes of euro sign - strip
        (b'ab\xe2', b'ab'),  # 1 of 3 bytes - strip
        (b'ab\xf0\x9f\x98', b'ab'),  # 3 of 4 bytes of emoji - strip
        (b'ab\xf0\x9f', b'ab'),  # 2 of 4 bytes - strip
        (b'ab\xf0', b'ab'),  # 1 of 4 bytes - strip
        (b'caf\xc3\xa9', b'caf\xc3\xa9'),  # complete e-acute - untouched
        (b'ab\xff', b'ab\xff'),  # invalid lead - decode-replace handles it
        (b'ab\x80', b'ab\x80'),  # stray continuation - decode-replace handles it
        (b'a\xc3\xa9\xa9', b'a\xc3\xa9\xa9'),  # complete char + stray continuation
        (b'', b''),  # empty
        (b'\xe2\x82', b''),  # everything stripped
        (b'\x80\x80\x80\x80', b'\x80\x80\x80\x80'),  # continuations only, no lead in last 3
    ],
)
def test_trim_incomplete_utf8_golden(data: bytes, expected: bytes):
    """Cross-backend golden vectors — the Go suite pins the identical table.

    Do not change inputs or outputs without changing the Go twin in the
    same change set.
    """
    assert _trim_incomplete_utf8(data) == expected


async def test_truncation_increments_metric_per_stream():
    from drakkar.metrics import output_truncated

    before_out = output_truncated.labels(stream='stdout')._value.get()
    before_err = output_truncated.labels(stream='stderr')._value.get()

    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=5,
        max_stderr_bytes=5,
    )
    task = make_task(args=['-c', "import sys; sys.stdout.write('o' * 20); sys.stderr.write('e' * 20)"])
    await pool.execute(task)

    assert output_truncated.labels(stream='stdout')._value.get() == before_out + 1
    assert output_truncated.labels(stream='stderr')._value.get() == before_err + 1


async def test_no_truncation_no_metric_increment():
    from drakkar.metrics import output_truncated

    before = output_truncated.labels(stream='stdout')._value.get()
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=1000,
    )
    await pool.execute(make_task(args=['-c', "print('small')"]))
    assert output_truncated.labels(stream='stdout')._value.get() == before


async def test_truncation_emits_warning_log():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=5,
    )
    task = make_task('t-log', args=['-c', "import sys; sys.stdout.write('x' * 20)"])
    with capture_logs() as cap:
        await pool.execute(task)

    events = [e for e in cap if e.get('event') == 'executor_output_truncated']
    assert len(events) == 1
    record = events[0]
    assert record['log_level'] == 'warning'
    assert record['task_id'] == 't-log'
    assert record['stream'] == 'stdout'
    assert record['cap_bytes'] == 5
    assert record['total_bytes'] == 20


async def test_timeout_with_cap_discards_partial_output():
    """Timeout keeps today's contract even when caps are set: the synthetic
    result carries no captured output and the flags stay False."""
    from drakkar.metrics import output_truncated

    before = output_truncated.labels(stream='stdout')._value.get()
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=1,
        max_stdout_bytes=10,
    )
    task = make_task(args=['-c', "import sys, time; sys.stdout.write('x' * 100); sys.stdout.flush(); time.sleep(30)"])
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.kind == 'timeout'
    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stdout_truncated is False
    assert exc_info.value.result.stderr_truncated is False
    assert output_truncated.labels(stream='stdout')._value.get() == before


async def test_precomputed_results_exempt_from_caps():
    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
        max_stdout_bytes=5,
        max_stderr_bytes=5,
    )
    task = ExecutorTask(
        task_id='pc-cap',
        source_offsets=[0],
        precomputed=PrecomputedResult(stdout='x' * 100),
    )
    result = await pool.execute(task)
    assert result.stdout == 'x' * 100
    assert result.stdout_truncated is False
    assert result.stderr_truncated is False


async def test_nonzero_exit_error_carries_truncated_stderr():
    pool = ExecutorPool(
        binary_path=sys.executable,
        max_executors=2,
        task_timeout_seconds=10,
        max_stderr_bytes=5,
    )
    task = make_task(args=['-c', "import sys; sys.stderr.write('e' * 20); sys.exit(3)"])
    with pytest.raises(ExecutorTaskError) as exc_info:
        await pool.execute(task)
    assert exc_info.value.error.exit_code == 3
    assert exc_info.value.error.stderr == 'eeeee'
    assert exc_info.value.result.stderr_truncated is True


async def test_default_unlimited_flags_stay_false(echo_pool):
    result = await echo_pool.execute(make_task(args=['hello']))
    assert result.stdout_truncated is False
    assert result.stderr_truncated is False


def test_parent_env_is_filtered_once_not_rebuilt_per_task():
    """The deny-filtered parent env is computed once, not per task.

    The documented "inherit verbatim" fast path requires an EMPTY deny list,
    but the default ships several patterns — so real deployments always took
    the filtering path and paid a full ``os.environ`` scan (~80us of
    event-loop time) on every task, even though the result is identical
    every time.

    The second half pins the documented consequence of caching: mutating
    ``os.environ`` after the first task does NOT affect later subprocesses.
    Per-task values belong in ``executor.env`` / ``ExecutorTask.env``.
    """
    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=5,
        inherit_parent_env=True,
        inherit_deny_patterns=['DK_*', '*SECRET*'],
    )
    task = make_task()

    with patch.dict(os.environ, {'DK_HIDDEN': 'x', 'MY_SECRET': 'y', 'KEEP_ME': 'z'}):
        first = pool._build_env(task)
        assert first is not None
        assert first['KEEP_ME'] == 'z', 'non-denied vars are inherited'
        assert 'DK_HIDDEN' not in first, 'DK_* must be filtered'
        assert 'MY_SECRET' not in first, '*SECRET* must be filtered'

    # The patch has exited, so os.environ no longer holds those names — but
    # the cached snapshot is unchanged. This is the documented trade-off.
    assert pool._build_env(task) == first


def test_task_env_still_overrides_the_cached_parent_env():
    """Caching the parent snapshot must not freeze per-task overrides."""
    pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=1,
        task_timeout_seconds=5,
        inherit_parent_env=True,
        inherit_deny_patterns=['DK_*'],
        env={'FROM_CONFIG': 'cfg'},
    )
    first = pool._build_env(make_task())
    assert first is not None and first['FROM_CONFIG'] == 'cfg'

    task = make_task()
    task.env = {'FROM_CONFIG': 'task-wins', 'PER_TASK': '1'}
    second = pool._build_env(task)
    assert second is not None
    assert second['FROM_CONFIG'] == 'task-wins', 'task env must beat config env'
    assert second['PER_TASK'] == '1'
    # The earlier result must not have been mutated by the later call.
    assert first['FROM_CONFIG'] == 'cfg'


# --- Per-task logging cost ---


async def test_successful_task_emits_no_log_line():
    """A completed task must not log.

    ``structlog``'s async methods copy the context, set two ContextVars and
    hop through the default thread pool *before* the level filter runs — so
    a per-task ``await logger.adebug(...)`` costs a thread round trip even
    with debug logging switched off. At the throughput this pool targets
    that is over a thousand hops a second, competing with the ``to_thread``
    users (dbstats, archive, FileSink) for the same executor. The flight
    recorder already records ``task_completed`` with more detail.
    """
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    task = ExecutorTask(task_id='t-quiet', args=['hello'], source_offsets=[0])

    with capture_logs() as cap:
        result = await pool.execute(task)

    assert result.exit_code == 0
    executor_events = [entry for entry in cap if entry.get('category') == 'executor']
    assert executor_events == [], f'per-task logging is back: {executor_events}'


def test_no_async_debug_logging_anywhere_in_the_package():
    """``logger.adebug`` is banned in this package.

    Every async structlog variant pays the thread-pool hop before the level
    filter, so a debug call that is switched off in production still costs a
    context copy and a scheduling point. Sync ``logger.debug`` is ~8x cheaper
    and has no scheduling point; use it instead.
    """
    package_root = Path(__file__).parent.parent / 'drakkar'
    offenders = [
        f'{path.relative_to(package_root)}:{lineno}'
        for path in sorted(package_root.rglob('*.py'))
        for lineno, line in enumerate(path.read_text().splitlines(), start=1)
        if 'logger.adebug(' in line
    ]
    assert offenders == [], f'use the sync logger.debug instead: {offenders}'
