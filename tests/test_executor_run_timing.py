"""Run timing of task subprocesses: spawn to exit, measured off the event loop."""

import asyncio
import errno
import os
import signal
import sys
import time

import pytest
from structlog.testing import capture_logs

from drakkar import executor as executor_module
from drakkar import procwatch
from drakkar.executor import ExecutorPool, ExecutorTaskError, measure_run
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import executor_exit_notice, executor_exit_watch_fallbacks
from drakkar.models import ErrorAction, ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.procwatch import (
    DESCENDANT_HELD_PIPES,
    EXITED_BEFORE_WATCH,
    UNAVAILABLE,
    ExitStamp,
    ExitWatch,
    ExitWatcher,
)
from tests.conftest import wait_for

NATIVE_BACKEND = procwatch.BACKENDS.get(sys.platform)
needs_native_backend = pytest.mark.skipif(NATIVE_BACKEND is None, reason='no exit watch backend on this platform')


class StallingWatcher(ExitWatcher):
    """Blocks the event loop right after the watch starts, like a stalled worker."""

    def __init__(self, stall_seconds: float) -> None:
        super().__init__(NATIVE_BACKEND)
        self.stall_seconds = stall_seconds

    def watch(self, proc):
        exit_watch = super().watch(proc)
        time.sleep(self.stall_seconds)
        return exit_watch


def make_pool(watcher: ExitWatcher, binary: str = 'sleep', timeout: int = 10) -> ExecutorPool:
    return ExecutorPool(binary_path=binary, max_executors=2, task_timeout_seconds=timeout, exit_watcher=watcher)


def make_task(*args: str) -> ExecutorTask:
    return ExecutorTask(task_id='t1', args=list(args), source_offsets=[0])


def reaped(pid: int) -> bool:
    """Whether asyncio has reaped the child ``pid`` (no status left to read)."""
    try:
        os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    except ChildProcessError:
        return True
    return False


def process_exists(pid: int) -> bool:
    """Whether ``pid`` names a process (a zombie counts until its parent reaps it)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def notice_count() -> float:
    """How many exit notices the histogram has observed."""
    return sum(bucket.get() for bucket in executor_exit_notice._buckets)


@needs_native_backend
async def test_a_loop_stall_does_not_stretch_the_duration():
    # Wide margins: a slow exec on a loaded runner must not decide the outcome.
    watcher = StallingWatcher(stall_seconds=1.0)
    try:
        result = await make_pool(watcher).execute(make_task('0.05'))
    finally:
        watcher.stop()

    assert result.duration_seconds < 0.5
    assert result.exit_notice_seconds is not None
    assert result.exit_notice_seconds >= 0.5


@needs_native_backend
async def test_a_completed_task_carries_its_spawn_time_and_notice():
    watcher = ExitWatcher(NATIVE_BACKEND)
    before_wall = time.time()
    observed_before = notice_count()
    try:
        result = await make_pool(watcher).execute(make_task('0.05'))
    finally:
        watcher.stop()

    assert result.run_started_at is not None
    assert before_wall <= result.run_started_at <= time.time()
    assert 0.04 <= result.duration_seconds < 1.0
    assert result.exit_notice_seconds is not None
    assert result.exit_notice_seconds >= 0.0
    assert notice_count() == observed_before + 1


@needs_native_backend
async def test_a_nonzero_exit_carries_the_run_timing():
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        with pytest.raises(ExecutorTaskError) as raised:
            await make_pool(watcher, binary='sh').execute(make_task('-c', 'exit 3'))
    finally:
        watcher.stop()

    assert raised.value.result.exit_code == 3
    assert raised.value.result.run_started_at is not None
    assert raised.value.result.exit_notice_seconds is not None


@needs_native_backend
@needs_native_backend
async def test_a_descendant_that_holds_the_pipes_ends_the_run_when_they_close():
    """The child exits at once; its background sleep keeps stdout open for 0.3 s."""
    watcher = ExitWatcher(NATIVE_BACKEND)
    before = executor_exit_watch_fallbacks.labels(reason=DESCENDANT_HELD_PIPES)._value.get()
    try:
        result = await make_pool(watcher, binary='sh').execute(make_task('-c', 'sleep 0.3 & exit 0'))
    finally:
        watcher.stop()

    assert result.duration_seconds >= 0.25
    assert result.exit_notice_seconds is None  # the gap was the descendant, not a loop stall
    assert executor_exit_watch_fallbacks.labels(reason=DESCENDANT_HELD_PIPES)._value.get() == before + 1


@needs_native_backend
async def test_a_timeout_is_timed_on_the_loop_with_no_notice():
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        with pytest.raises(ExecutorTaskError) as raised:
            await make_pool(watcher, timeout=1).execute(make_task('5'))
    finally:
        watcher.stop()

    result = raised.value.result
    assert raised.value.error.kind == 'timeout'
    assert result.run_started_at is not None
    assert 1.0 <= result.duration_seconds < 1.5
    assert result.exit_notice_seconds is None


@needs_native_backend
async def test_a_timeout_whose_child_exited_early_kills_the_descendant(tmp_path):
    """The child exits at once; its background sleep holds stdout until the timeout kills its group."""
    pid_file = tmp_path / 'grandchild.pid'
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        with pytest.raises(ExecutorTaskError) as raised:
            await make_pool(watcher, binary='sh', timeout=1).execute(
                make_task('-c', f'sleep 5 & echo $! > {pid_file}; exit 0')
            )
        # The killed grandchild's pipes close a moment after the kill; let this loop close them.
        await asyncio.sleep(0.3)
    finally:
        watcher.stop()

    result = raised.value.result
    assert raised.value.error.kind == 'timeout'
    assert 1.0 <= result.duration_seconds < 1.5
    assert result.exit_notice_seconds is None
    grandchild = int(pid_file.read_text())
    await wait_for(lambda: not process_exists(grandchild))


async def test_a_child_that_survives_the_kill_costs_one_bounded_wait_and_one_warning(monkeypatch):
    signals: list[int] = []
    monkeypatch.setattr(executor_module, '_KILL_REAP_TIMEOUT_SECONDS', 0.2)
    # killpg that does nothing: the child survives "SIGKILL".
    monkeypatch.setattr(executor_module.os, 'killpg', lambda pgid, sig: signals.append(sig))
    started = time.monotonic()
    with capture_logs() as logs, pytest.raises(ExecutorTaskError) as raised:
        await make_pool(ExitWatcher(None), timeout=1).execute(make_task('3'))
    elapsed = time.monotonic() - started
    os.kill(raised.value.error.pid, signal.SIGKILL)  # the real cleanup
    await wait_for(lambda: reaped(raised.value.error.pid))
    await asyncio.sleep(0.3)  # let asyncio close the pipes and the transport on this loop

    assert len(signals) == 1
    assert [log['event'] for log in logs].count('executor_kill_reap_timeout') == 1
    assert elapsed < 1.0 + 0.2 + 0.5
    assert 1.0 <= raised.value.result.duration_seconds < 1.5


async def test_an_exec_timeout_is_a_launch_failure(monkeypatch):
    """ETIMEDOUT from exec (a binary on a hung network mount) is a TimeoutError too."""
    pool = make_pool(ExitWatcher(None))

    async def timed_out_exec(*args):
        raise OSError(errno.ETIMEDOUT, 'Connection timed out')

    monkeypatch.setattr(pool, '_spawn', timed_out_exec)
    with pytest.raises(ExecutorTaskError) as raised:
        await pool.execute(make_task())

    assert raised.value.error.kind == 'launch_failure'
    assert 'timed out' in raised.value.error.exception
    assert raised.value.result.run_started_at is None


class RetryOnceHandler(BaseDrakkarHandler):
    def __init__(self) -> None:
        self.error_kinds: list[str] = []
        self.completed: list[str] = []

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f't{msg.offset}', args=['0.01'], source_offsets=[msg.offset]) for msg in messages]

    async def on_error(self, task, error):
        self.error_kinds.append(error.kind)
        return ErrorAction.RETRY

    async def on_task_complete(self, result):
        self.completed.append(result.task.task_id)


async def test_an_exec_timeout_reaches_on_error_and_the_retry(monkeypatch):
    pool = make_pool(ExitWatcher(None))
    real_spawn = pool._spawn
    attempts: list[int] = []

    async def flaky_spawn(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError(errno.ETIMEDOUT, 'Connection timed out')
        return await real_spawn(*args)

    monkeypatch.setattr(pool, '_spawn', flaky_spawn)
    handler = RetryOnceHandler()
    processor = PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10)
    processor.enqueue(SourceMessage(topic='t', partition=0, offset=0, value=b'', timestamp=1))
    processor.start()
    await wait_for(lambda: handler.completed == ['t0'])
    await processor.stop()

    assert handler.error_kinds == ['launch_failure']
    assert len(attempts) == 2


async def test_a_launch_failure_has_no_run_timing():
    with pytest.raises(ExecutorTaskError) as raised:
        await make_pool(ExitWatcher(None), binary='/nonexistent/drakkar-binary').execute(make_task())

    assert raised.value.error.kind == 'launch_failure'
    assert raised.value.result.run_started_at is None
    assert raised.value.result.exit_notice_seconds is None


async def test_without_a_watcher_the_run_is_timed_on_the_loop():
    before = executor_exit_watch_fallbacks.labels(reason=UNAVAILABLE)._value.get()

    result = await make_pool(ExitWatcher(None)).execute(make_task('0.05'))

    assert result.run_started_at is not None
    assert result.duration_seconds >= 0.04
    assert result.exit_notice_seconds is None
    assert executor_exit_watch_fallbacks.labels(reason=UNAVAILABLE)._value.get() == before + 1


def test_the_pool_starts_and_stops_its_watcher():
    stopped: list[bool] = []

    class RecordingWatcher(ExitWatcher):
        def stop(self, timeout_seconds: float = 1.0) -> None:
            stopped.append(True)

    pool = make_pool(RecordingWatcher(None))
    assert pool.start_exit_watcher() is None
    pool.stop_exit_watcher()
    assert stopped == [True]


def test_the_pool_uses_the_shared_watcher_by_default():
    pool = ExecutorPool(binary_path='sleep', max_executors=1, task_timeout_seconds=1)
    assert pool._exit_watcher is procwatch.shared_watcher()


@pytest.mark.parametrize(
    ('exited_at', 'output_closed_at', 'held'),
    [
        (None, 10.0, False),  # the exit was not seen yet
        (10.0, None, False),  # the pipes were not seen closing
        (10.0, 9.9, False),  # pipes closed first
        (10.0, 10.04, False),  # within the margin
        (10.0, 10.06, True),  # a descendant held them
    ],
)
async def test_descendant_held_pipes(exited_at, output_closed_at, held):
    protocol = executor_module.TimedStreamProtocol(2**16, asyncio.get_running_loop())
    protocol.exited_at, protocol.output_closed_at = exited_at, output_closed_at
    assert protocol.descendant_held_pipes() is held


@pytest.mark.parametrize(
    ('exit_watch', 'expected'),
    [
        # No watch at all: loop timing, notice unknown.
        (None, (5.0, None)),
        # The host cannot watch: loop timing, notice unknown.
        (ExitWatch(fallback_reason=UNAVAILABLE), (5.0, None)),
        # Exited before the watch: ends at the watch time, notice unknown.
        (ExitWatch(stamp=ExitStamp(monotonic=102.0, wall=0.0), fallback_reason=EXITED_BEFORE_WATCH), (2.0, None)),
        # A live watch with no stamp yet: the loop was first, its reading is as good.
        (ExitWatch(), (5.0, 0.0)),
        # The watcher stamped the exit: ends there, the loop was 4 s late.
        (ExitWatch(stamp=ExitStamp(monotonic=101.0, wall=0.0)), (1.0, 4.0)),
        # A stamp written after the loop's reading: the loop reading wins.
        (ExitWatch(stamp=ExitStamp(monotonic=105.5, wall=0.0)), (5.0, 0.0)),
        # A stamp from before the spawn belongs to another process: loop timing, no notice.
        (ExitWatch(stamp=ExitStamp(monotonic=99.0, wall=0.0)), (5.0, None)),
    ],
)
def test_measure_run(exit_watch, expected):
    assert measure_run(100.0, exit_watch, loop_end=105.0) == expected
