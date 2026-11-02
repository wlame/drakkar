"""Tests for the exit watcher (drakkar/procwatch.py).

The native backend of the host is proved with real short subprocesses. Both
backends are also driven through fake OS primitives, so every branch runs on
every host (the pidfd backend exists on Linux only, kqueue on macOS only).
"""

import asyncio
import os
import queue
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from structlog.testing import capture_logs

from drakkar import procwatch
from drakkar.metrics import executor_exit_watch_fallbacks
from drakkar.procwatch import (
    EXITED_BEFORE_WATCH,
    UNAVAILABLE,
    ExitStamp,
    ExitWatch,
    ExitWatcher,
    KqueueBackend,
    PidfdBackend,
)

NATIVE_BACKEND = procwatch.BACKENDS.get(sys.platform)
needs_native_backend = pytest.mark.skipif(NATIVE_BACKEND is None, reason='no exit watch backend on this platform')


def fallbacks(reason: str) -> float:
    return executor_exit_watch_fallbacks.labels(reason=reason)._value.get()


def fake_proc(pid: int = 4242, returncode: int | None = None) -> SimpleNamespace:
    return SimpleNamespace(pid=pid, returncode=returncode)


class FakeBackend:
    """A backend driven by the test: ``exit(key)`` makes ``poll`` report it."""

    name = 'fake'

    def __init__(self, watch_error: BaseException | None = None) -> None:
        self.watch_error = watch_error
        self.reports: queue.Queue[list[int] | BaseException] = queue.Queue()
        self.watched: list[int] = []
        self.released: list[int] = []
        self.closed = False

    def watch(self, pid: int) -> int:
        if self.watch_error is not None:
            raise self.watch_error
        self.watched.append(pid)
        return pid

    def poll(self) -> list[int]:
        report = self.reports.get()
        if isinstance(report, BaseException):
            raise report
        return report

    def release(self, key: int) -> None:
        self.released.append(key)

    def wake(self) -> None:
        self.reports.put([])

    def close(self) -> None:
        self.closed = True


def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError('condition not met in time')
        time.sleep(0.005)


@pytest.fixture
def fake_watcher():
    backend = FakeBackend()
    watcher = ExitWatcher(lambda: backend, batch_pause_seconds=0.0)
    yield watcher, backend
    watcher.stop()


# --- the native backend, with real subprocesses ---------------------------------


@needs_native_backend
async def test_native_watch_stamps_the_exit_of_a_real_child():
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        started = time.monotonic()
        proc = await asyncio.create_subprocess_exec('sleep', '0.05')
        exit_watch = watcher.watch(proc)
        await proc.wait()
        await asyncio.to_thread(wait_until, lambda: exit_watch.stamp is not None)
    finally:
        watcher.stop()

    assert exit_watch.fallback_reason is None
    assert exit_watch.stamp is not None
    assert 0.04 <= exit_watch.stamp.monotonic - started < 0.5
    # asyncio keeps the exit status: the watch never reaps.
    assert proc.returncode == 0


@needs_native_backend
async def test_native_watch_stamps_the_exit_during_a_loop_stall():
    watcher = ExitWatcher(NATIVE_BACKEND)
    try:
        started = time.monotonic()
        proc = await asyncio.create_subprocess_exec('sleep', '0.05')
        exit_watch = watcher.watch(proc)
        time.sleep(1.0)  # the loop is blocked while the child exits
        await proc.wait()
        loop_saw_exit = time.monotonic()
    finally:
        watcher.stop()

    # Wide margins: a slow exec on a loaded runner must not decide the outcome.
    assert exit_watch.stamp is not None
    assert exit_watch.stamp.monotonic - started < 0.5
    assert loop_saw_exit - exit_watch.stamp.monotonic >= 0.5


@needs_native_backend
def test_native_backend_refuses_a_pid_that_is_not_our_child():
    backend = NATIVE_BACKEND()
    try:
        # pid 1 is alive but is not a child of this process.
        with pytest.raises(ProcessLookupError):
            backend.watch(1)
    finally:
        backend.close()


# --- ExitWatcher, with a fake backend ---------------------------------------------


async def test_watch_hands_the_stamp_to_the_watch_and_releases_the_key(fake_watcher):
    watcher, backend = fake_watcher
    exit_watch = watcher.watch(fake_proc(pid=7))

    assert backend.watched == [7]
    assert exit_watch.stamp is None
    before = time.monotonic()
    backend.reports.put([7, 99])  # 99 is not watched: released, no stamp anywhere
    await asyncio.to_thread(wait_until, lambda: exit_watch.stamp is not None)

    assert exit_watch.stamp.monotonic >= before
    assert abs(exit_watch.stamp.wall - time.time()) < 1.0
    assert backend.released == [7, 99]
    assert watcher.backend_name == 'fake'


async def test_watch_of_an_exited_process_is_stamped_now_and_counted():
    watcher = ExitWatcher(FakeBackend)
    before = fallbacks(EXITED_BEFORE_WATCH)
    exit_watch = watcher.watch(fake_proc(returncode=0))

    assert exit_watch.fallback_reason == EXITED_BEFORE_WATCH
    assert exit_watch.stamp is not None
    assert fallbacks(EXITED_BEFORE_WATCH) == before + 1
    # No thread was needed for that answer.
    assert watcher.backend_name is None


async def test_watch_of_a_reaped_pid_is_stamped_now_and_counted():
    backend = FakeBackend(watch_error=ProcessLookupError())
    watcher = ExitWatcher(lambda: backend)
    before = fallbacks(EXITED_BEFORE_WATCH)
    try:
        exit_watch = watcher.watch(fake_proc())
    finally:
        watcher.stop()

    assert exit_watch.fallback_reason == EXITED_BEFORE_WATCH
    assert exit_watch.stamp is not None
    assert fallbacks(EXITED_BEFORE_WATCH) == before + 1


async def test_a_platform_without_a_backend_times_every_exit_on_the_loop():
    watcher = ExitWatcher(None)
    before = fallbacks(UNAVAILABLE)

    exit_watch = watcher.watch(fake_proc())

    assert exit_watch.fallback_reason == UNAVAILABLE
    assert exit_watch.stamp is None
    assert fallbacks(UNAVAILABLE) == before + 1
    assert watcher.start() is None


def test_a_backend_that_cannot_start_disables_the_watcher_with_one_warning():
    def refuse() -> FakeBackend:
        raise OSError('pidfd_open is not allowed')

    watcher = ExitWatcher(refuse)
    with capture_logs() as logs:
        assert watcher.start() is None
        assert watcher.start() is None

    assert [log['event'] for log in logs] == ['exit_watcher_unavailable']
    assert logs[0]['error'] == 'pidfd_open is not allowed'


async def test_a_kernel_that_refuses_the_watch_disables_the_watcher():
    backend = FakeBackend(watch_error=PermissionError('seccomp'))
    watcher = ExitWatcher(lambda: backend)
    before = fallbacks(UNAVAILABLE)
    try:
        with capture_logs() as logs:
            first = watcher.watch(fake_proc())
        backend.watch_error = None
        second = watcher.watch(fake_proc())
    finally:
        watcher.stop()

    assert first.fallback_reason == UNAVAILABLE
    assert second.fallback_reason == UNAVAILABLE
    assert backend.watched == []
    assert fallbacks(UNAVAILABLE) == before + 2
    assert [log['event'] for log in logs] == ['exit_watch_refused']


async def test_a_failing_thread_leaves_its_watches_to_loop_timing():
    backend = FakeBackend()
    watcher = ExitWatcher(lambda: backend)
    exit_watch = watcher.watch(fake_proc(pid=5))
    with capture_logs() as logs:
        backend.reports.put(RuntimeError('epoll broke'))
        await asyncio.to_thread(wait_until, lambda: not watcher._thread.is_alive())

    assert exit_watch.stamp is None
    assert backend.released == [5]
    assert any(log['event'] == 'exit_watcher_failed' for log in logs)
    assert watcher.watch(fake_proc(pid=6)).fallback_reason == UNAVAILABLE


async def test_stop_ends_the_thread_and_a_later_watch_starts_it_again():
    backends: list[FakeBackend] = []

    def make() -> FakeBackend:
        backends.append(FakeBackend())
        return backends[-1]

    watcher = ExitWatcher(make)
    assert watcher.start() == 'fake'
    first_thread = watcher._thread
    pending = watcher.watch(fake_proc(pid=3))

    watcher.stop()
    watcher.stop()  # a second stop does nothing

    assert not first_thread.is_alive()
    assert backends[0].closed
    assert backends[0].released == [3]  # the pending watch's descriptor is freed
    assert pending.stamp is None
    assert watcher.backend_name is None
    watcher.watch(fake_proc(pid=4))
    assert len(backends) == 2
    assert backends[1].watched == [4]
    watcher.stop()


def test_stop_ignores_a_pending_watch_that_is_gone_already():
    class GoneBackend(FakeBackend):
        def release(self, key: int) -> None:
            raise OSError('closed already')

    backend = GoneBackend()
    watcher = ExitWatcher(lambda: backend)

    async def watch_one() -> None:
        watcher.watch(fake_proc(pid=12))

    asyncio.run(watch_one())
    watcher.stop()

    assert backend.closed


def test_stop_leaves_the_descriptors_open_when_the_thread_does_not_end():
    class StuckBackend(FakeBackend):
        def wake(self) -> None:
            pass  # the thread stays in poll

    backend = StuckBackend()
    watcher = ExitWatcher(lambda: backend)
    watcher.start()
    thread = watcher._thread

    watcher.stop(timeout_seconds=0.01)

    assert not backend.closed
    backend.reports.put([])  # let the thread see the stop and end
    thread.join(1.0)
    assert not thread.is_alive()


async def test_the_thread_pauses_after_each_batch(monkeypatch):
    backend = FakeBackend()
    watcher = ExitWatcher(lambda: backend, batch_pause_seconds=0.25)
    pauses: list[float] = []
    real_sleep = time.sleep

    def record_sleep(seconds: float) -> None:
        # The module is shared, so the test's own polling sleeps come through here too.
        if threading.current_thread().name == 'drakkar-exit-watcher':
            pauses.append(seconds)
            seconds = 0
        real_sleep(seconds)

    monkeypatch.setattr(procwatch.time, 'sleep', record_sleep)
    exit_watch = watcher.watch(fake_proc(pid=8))
    backend.reports.put([8])
    await asyncio.to_thread(wait_until, lambda: bool(pauses))
    watcher.stop()

    assert exit_watch.stamp is not None
    assert pauses[0] == 0.25


async def test_a_forked_child_starts_a_thread_of_its_own(fake_watcher):
    watcher, backend = fake_watcher
    watcher.start()
    watcher._owner_pid = os.getpid() + 1  # as seen from a forked child

    watcher.watch(fake_proc(pid=9))

    assert watcher._owner_pid == os.getpid()
    assert backend.watched == [9]


def test_shared_watcher_is_one_per_process(monkeypatch):
    monkeypatch.setattr(procwatch, '_shared_watcher', None)

    first = procwatch.shared_watcher()

    assert procwatch.shared_watcher() is first
    assert first._backend_factory is procwatch.BACKENDS.get(sys.platform)


def test_exit_watch_defaults_to_no_stamp():
    exit_watch = ExitWatch()
    assert (exit_watch.stamp, exit_watch.fallback_reason) == (None, None)
    assert isinstance(procwatch.stamp_now(), ExitStamp)


# --- PidfdBackend, with fake OS primitives ------------------------------------


class FakeEpoll:
    def __init__(self) -> None:
        self.registered: dict[int, int] = {}
        self.ready: list[tuple[int, int]] = []
        self.closed = False
        self.unregister_error: OSError | None = None

    def register(self, fd: int, events: int) -> None:
        self.registered[fd] = events

    def unregister(self, fd: int) -> None:
        if self.unregister_error is not None:
            raise self.unregister_error
        del self.registered[fd]

    def poll(self) -> list[tuple[int, int]]:
        ready, self.ready = self.ready, []
        return ready

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def pidfd_env(monkeypatch):
    """Fake pidfd_open (handing out real pipe fds) and a fake epoll, on any host."""
    epoll = FakeEpoll()
    opened: list[int] = []
    spare_ends: list[int] = []

    def pidfd_open(pid: int) -> int:
        if pid == 404:
            raise ProcessLookupError(pid)
        read_fd, write_fd = os.pipe()
        spare_ends.append(write_fd)
        opened.append(read_fd)
        return read_fd

    waitid_calls: list[tuple] = []
    waitid_error: list[BaseException] = []

    def waitid(idtype, ident, options):
        waitid_calls.append((idtype, ident, options))
        if waitid_error:
            raise waitid_error[0]
        return None

    monkeypatch.setattr(procwatch, '_pidfd_open', pidfd_open)
    monkeypatch.setattr(procwatch, '_P_PIDFD', 3)
    monkeypatch.setattr(procwatch, '_epoll', lambda: epoll)
    monkeypatch.setattr(procwatch.os, 'waitid', waitid)
    env = SimpleNamespace(epoll=epoll, opened=opened, waitid_calls=waitid_calls, waitid_error=waitid_error)
    yield env
    for fd in spare_ends:
        os.close(fd)


def fd_is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


def test_pidfd_backend_watches_with_a_probe_that_never_reaps(pidfd_env):
    backend = PidfdBackend()
    key = backend.watch(1234)

    assert key == pidfd_env.opened[0]
    assert pidfd_env.waitid_calls == [(3, key, os.WEXITED | os.WNOHANG | os.WNOWAIT)]
    assert key in pidfd_env.epoll.registered

    backend.release(key)
    assert key not in pidfd_env.epoll.registered
    assert not fd_is_open(key)
    backend.close()
    assert pidfd_env.epoll.closed


def test_pidfd_backend_closes_the_fd_of_a_pid_that_is_not_our_child(pidfd_env):
    pidfd_env.waitid_error.append(ChildProcessError())
    backend = PidfdBackend()

    with pytest.raises(ProcessLookupError):
        backend.watch(1234)

    assert not fd_is_open(pidfd_env.opened[0])
    backend.close()


def test_pidfd_backend_closes_the_fd_on_any_other_error(pidfd_env):
    pidfd_env.waitid_error.append(PermissionError('seccomp'))
    backend = PidfdBackend()

    with pytest.raises(PermissionError):
        backend.watch(1234)

    assert not fd_is_open(pidfd_env.opened[0])
    backend.close()


def test_pidfd_backend_passes_a_reaped_pid_through(pidfd_env):
    backend = PidfdBackend()
    with pytest.raises(ProcessLookupError):
        backend.watch(404)
    backend.close()


def test_pidfd_backend_poll_reports_exits_and_drains_the_wake_pipe(pidfd_env):
    backend = PidfdBackend()
    key = backend.watch(1234)
    backend.wake()
    backend.wake()
    pidfd_env.epoll.ready = [(backend._wake_read, 1), (key, 1)]

    assert backend.poll() == [key]
    with pytest.raises(BlockingIOError):
        os.read(backend._wake_read, 1)  # drained
    pidfd_env.epoll.unregister_error = OSError('gone')
    backend.release(key)  # an unregister error still closes the fd
    assert not fd_is_open(key)
    backend.close()


def test_pidfd_backend_needs_its_primitives(monkeypatch):
    monkeypatch.setattr(procwatch, '_pidfd_open', None)
    with pytest.raises(OSError, match='pidfd watch needs'):
        PidfdBackend()


def test_a_full_wake_pipe_is_a_pending_wake():
    read_fd, write_fd = procwatch._wake_pipe()
    try:
        while True:
            try:
                os.write(write_fd, b'\0' * 65536)
            except BlockingIOError:
                break
        procwatch._write_wake(write_fd)  # no raise
        procwatch._drain_pipe(read_fd)
    finally:
        os.close(read_fd)
        os.close(write_fd)


# --- KqueueBackend, with fake OS primitives -----------------------------------


class FakeKqueue:
    def __init__(self) -> None:
        self.changes: list = []
        self.ready: list = []
        self.closed = False
        self.errors: dict[int, OSError] = {}

    def control(self, changes, max_events, timeout=None):
        for change in changes or []:
            error = self.errors.get(change.flags)
            if error is not None:
                raise error
            self.changes.append(change)
        if max_events:
            ready, self.ready = self.ready, []
            return ready
        return []

    def close(self) -> None:
        self.closed = True


def fake_kevent(ident, filter, flags=0, fflags=0):
    return SimpleNamespace(ident=ident, filter=filter, flags=flags, fflags=fflags)


@pytest.fixture
def kqueue_env(monkeypatch):
    kq = FakeKqueue()
    waitid_error: list[BaseException] = []

    def waitid(idtype, ident, options):
        if waitid_error:
            raise waitid_error[0]
        return None

    monkeypatch.setattr(procwatch, '_kqueue', lambda: kq)
    monkeypatch.setattr(procwatch, '_kevent', fake_kevent)
    monkeypatch.setattr(procwatch.os, 'waitid', waitid)
    return SimpleNamespace(kq=kq, waitid_error=waitid_error)


def test_kqueue_backend_registers_a_one_shot_exit_event(kqueue_env):
    backend = KqueueBackend()

    assert backend.watch(1234) == 1234

    change = kqueue_env.kq.changes[-1]
    assert (change.ident, change.filter, change.fflags) == (1234, procwatch._KQ_FILTER_PROC, procwatch._KQ_NOTE_EXIT)
    assert change.flags == procwatch._KQ_EV_ADD | procwatch._KQ_EV_ONESHOT
    backend.release(1234)  # one-shot: nothing to do
    backend.close()
    assert kqueue_env.kq.closed


def test_kqueue_backend_removes_the_registration_of_a_pid_that_is_not_our_child(kqueue_env):
    kqueue_env.waitid_error.append(ChildProcessError())
    backend = KqueueBackend()

    with pytest.raises(ProcessLookupError):
        backend.watch(1234)

    assert kqueue_env.kq.changes[-1].flags == procwatch._KQ_EV_DELETE
    backend.close()


def test_kqueue_backend_ignores_a_failed_removal(kqueue_env):
    kqueue_env.waitid_error.append(ChildProcessError())
    kqueue_env.kq.errors[procwatch._KQ_EV_DELETE] = FileNotFoundError('fired already')
    backend = KqueueBackend()

    with pytest.raises(ProcessLookupError):
        backend.watch(1234)
    backend.close()


def test_kqueue_backend_poll_reports_exits_and_drains_the_wake_pipe(kqueue_env):
    backend = KqueueBackend()
    backend.wake()
    kqueue_env.kq.ready = [
        fake_kevent(backend._wake_read, procwatch._KQ_FILTER_READ),
        fake_kevent(1234, procwatch._KQ_FILTER_PROC),
    ]

    assert backend.poll() == [1234]
    with pytest.raises(BlockingIOError):
        os.read(backend._wake_read, 1)
    backend.close()


def test_kqueue_backend_needs_its_primitives(monkeypatch):
    monkeypatch.setattr(procwatch, '_kqueue', None)
    with pytest.raises(OSError, match='kqueue watch needs'):
        KqueueBackend()


def test_both_backends_are_in_the_platform_table():
    assert {'linux': PidfdBackend, 'darwin': KqueueBackend} == procwatch.BACKENDS


def test_the_lock_guards_registration_against_a_report_in_between(fake_watcher):
    """The thread must not see a key before its watch is in the table."""
    watcher, backend = fake_watcher
    watcher.start()
    entered = threading.Event()
    real_watch = backend.watch

    def slow_watch(pid: int) -> int:
        key = real_watch(pid)
        backend.reports.put([key])  # the exit is reported before the table insert
        entered.set()
        time.sleep(0.05)
        return key

    backend.watch = slow_watch

    async def run() -> ExitWatch:
        return watcher.watch(fake_proc(pid=11))

    exit_watch = asyncio.run(run())
    wait_until(lambda: exit_watch.stamp is not None)
    assert entered.is_set()
