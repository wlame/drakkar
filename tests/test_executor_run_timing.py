"""Run timing of task subprocesses: spawn to exit, measured off the event loop."""

import sys
import time

import pytest

from drakkar import procwatch
from drakkar.executor import ExecutorPool, ExecutorTaskError, measure_run
from drakkar.metrics import executor_exit_notice, executor_exit_watch_fallbacks
from drakkar.models import ExecutorTask
from drakkar.procwatch import EXITED_BEFORE_WATCH, UNAVAILABLE, ExitStamp, ExitWatch, ExitWatcher

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
async def test_a_timeout_ends_the_run_at_the_killed_child():
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        with pytest.raises(ExecutorTaskError) as raised:
            await make_pool(watcher, timeout=1).execute(make_task('5'))
    finally:
        watcher.stop()

    result = raised.value.result
    assert raised.value.error.kind == 'timeout'
    assert result.run_started_at is not None
    assert 1.0 <= result.duration_seconds < 2.0
    assert result.exit_notice_seconds is not None


async def test_a_child_that_survives_the_kill_is_cut_where_the_timeout_fired(monkeypatch):
    real_kill = ExecutorPool._kill_process_tree
    calls: list[int] = []

    async def first_kill_fails(proc):
        calls.append(proc.pid)
        if len(calls) > 1:
            await real_kill(proc)

    monkeypatch.setattr(ExecutorPool, '_kill_process_tree', staticmethod(first_kill_fails))
    with pytest.raises(ExecutorTaskError) as raised:
        await make_pool(ExitWatcher(None), timeout=1).execute(make_task('5'))

    result = raised.value.result
    assert len(calls) == 2  # the timeout branch, then the cleanup that really kills
    assert 1.0 <= result.duration_seconds < 2.0
    assert result.exit_notice_seconds is None
    assert result.run_started_at is not None


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
    ],
)
def test_measure_run(exit_watch, expected):
    assert measure_run(100.0, exit_watch, loop_end=105.0) == expected
