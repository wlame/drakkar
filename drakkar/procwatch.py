"""Exit watcher: reads the clock when a task subprocess exits, off the event loop.

asyncio learns about a child's exit on the event loop: it reads the exit
notification, reaps the child and resumes the waiting coroutine, each step a
loop callback. A loop that is busy for two seconds therefore learns about an
exit two seconds late, and a duration measured in the coroutine grows by the
same amount. This module runs one daemon thread per process that waits for
task subprocesses to exit and reads the clock when the kernel reports the
exit.

The watch never reaps a child. A Linux pidfd and a macOS kqueue ``NOTE_EXIT``
both report the exit while the child is still a zombie, so asyncio keeps its
own ``waitpid`` and the exit status. The only extra wait call is a
``waitid(..., WNOWAIT)`` probe, which leaves the status in place.

The thread never touches the event loop. It writes the stamp into the
:class:`ExitWatch` object, and the task coroutine reads it after the process
has exited. A stamp that is not there yet means the loop handled the exit no
later than the watcher did, so the loop's own reading is as good; the
coroutine never waits for the thread.

Backends are data: :data:`BACKENDS` maps ``sys.platform`` to a backend class.
Any other platform, or a kernel that refuses the watch (seccomp), times exits
on the loop as before and counts the reason on
``drakkar_executor_exit_watch_fallbacks_total``.
"""

from __future__ import annotations

import asyncio
import errno
import os
import select
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import structlog

from drakkar.metrics import executor_exit_watch_fallbacks
from drakkar.providers.resilience import RateLimitedLog

logger = structlog.get_logger()

# Fallback reasons, the label values of the fallback counter.
EXITED_BEFORE_WATCH = 'exited_before_watch'
UNAVAILABLE = 'unavailable'
# Counted by the executor: the child exited, but a descendant kept its output
# pipes open, so the run ends when the pipes close (read on the loop).
DESCENDANT_HELD_PIPES = 'descendant_held_pipes'

# Errors of a watch that say the host can never watch: the watcher turns off
# for the process. Any other OSError (EMFILE, ENOMEM, EAGAIN, ...) is a moment
# of resource pressure and makes only that task fall back.
_PERMANENT_ERRNOS = frozenset({errno.EPERM, errno.EACCES, errno.ENOSYS, errno.EINVAL})

# The thread sleeps this long after each batch of exits before it waits
# again. Every wakeup costs a GIL hand-over with the busy event loop, so the
# pause caps the wakeup rate (at most 1 / BATCH_PAUSE_SECONDS per second) and
# exits that come during it are stamped together at the next wakeup, at most
# this much late. It never delays a task: the coroutine does not wait for the
# thread.
BATCH_PAUSE_SECONDS = 0.005

# Events read per kqueue wakeup. More exits than this wait for the next call.
_KQUEUE_MAX_EVENTS = 1024

# The OS primitives, read once here so tests can replace them on any host.
# ``getattr`` because each one exists on one platform only.
_pidfd_open: Callable[[int], int] | None = getattr(os, 'pidfd_open', None)
_P_PIDFD: int | None = getattr(os, 'P_PIDFD', None)
_epoll: Callable[[], Any] | None = getattr(select, 'epoll', None)
_kqueue: Callable[[], Any] | None = getattr(select, 'kqueue', None)
_kevent: Callable[..., Any] | None = getattr(select, 'kevent', None)
# kqueue and epoll constants (macOS and Linux values as the fallback, so the
# module imports on any host).
_KQ_FILTER_PROC: int = getattr(select, 'KQ_FILTER_PROC', -5)
_KQ_FILTER_READ: int = getattr(select, 'KQ_FILTER_READ', -1)
_KQ_EV_ADD: int = getattr(select, 'KQ_EV_ADD', 0x1)
_KQ_EV_DELETE: int = getattr(select, 'KQ_EV_DELETE', 0x2)
_KQ_EV_ONESHOT: int = getattr(select, 'KQ_EV_ONESHOT', 0x10)
_KQ_NOTE_EXIT: int = getattr(select, 'KQ_NOTE_EXIT', 0x80000000)
_EPOLLIN: int = getattr(select, 'EPOLLIN', 0x1)

# ``waitid`` flags of the "is this still our unreaped child" probe: report an
# exit if there is one, never block, and never consume the status. ``getattr``
# so the module still imports where they do not exist (no backend runs there).
_PROBE_FLAGS: int = getattr(os, 'WEXITED', 0) | getattr(os, 'WNOHANG', 0) | getattr(os, 'WNOWAIT', 0)


@dataclass(frozen=True, slots=True)
class ExitStamp:
    """The moment a process exited, on both clocks.

    ``monotonic`` measures the run time (from a monotonic spawn reading);
    ``wall`` is the same moment in unix seconds.
    """

    monotonic: float
    wall: float


def stamp_now() -> ExitStamp:
    """An :class:`ExitStamp` for this moment."""
    return ExitStamp(monotonic=time.monotonic(), wall=time.time())


class ExitBackend(Protocol):
    """One kernel mechanism that reports process exits to the watcher thread.

    ``watch`` and ``release`` run under the watcher's lock; ``poll`` runs on
    the watcher thread and blocks.
    """

    name: str

    def watch(self, pid: int) -> int:
        """Start watching ``pid``; return the key ``poll`` reports for it.

        Raises ``ProcessLookupError`` when ``pid`` is not an unreaped child of
        this process (it was reaped already, or the pid now names another
        process). Any other ``OSError`` means the host cannot watch.
        """
        ...

    def poll(self) -> list[int]:
        """Block until a watched process exits or :meth:`wake` is called; return the exited keys."""
        ...

    def release(self, key: int) -> None:
        """Stop watching ``key`` and free what the watch holds."""
        ...

    def wake(self) -> None:
        """Make a blocked :meth:`poll` return."""
        ...

    def close(self) -> None:
        """Free the backend's own descriptors."""
        ...


def _wake_pipe() -> tuple[int, int]:
    """A non-blocking pipe whose read end makes a blocked poll return."""
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    return read_fd, write_fd


def _drain_pipe(read_fd: int) -> None:
    """Read every wake byte, so a level-triggered poll does not report the pipe again."""
    try:
        while os.read(read_fd, 4096):
            pass
    except BlockingIOError:
        pass


def _write_wake(write_fd: int) -> None:
    """Write one wake byte; a full pipe already holds a pending wake."""
    try:
        os.write(write_fd, b'\0')
    except BlockingIOError:
        pass


class PidfdBackend:
    """Linux: one pidfd per child, in one epoll set.

    A pidfd becomes readable when its process exits, before the process is
    reaped. The probe ``waitid(P_PIDFD, fd, WNOWAIT)`` after the open proves
    the fd names our own unreaped child: if the pid was reaped and reused in
    between, the probe raises ``ChildProcessError``.
    """

    name = 'pidfd'

    def __init__(self) -> None:
        if _epoll is None or _pidfd_open is None or _P_PIDFD is None:
            raise OSError('pidfd watch needs os.pidfd_open, os.P_PIDFD and select.epoll')
        # Ask the kernel once now, so an old kernel or a seccomp profile that
        # refuses pidfd_open is found at startup, not at the first task.
        os.close(_pidfd_open(os.getpid()))
        self._epoll = _epoll()
        self._wake_read, self._wake_write = _wake_pipe()
        self._epoll.register(self._wake_read, _EPOLLIN)

    def watch(self, pid: int) -> int:
        assert _pidfd_open is not None and _P_PIDFD is not None
        fd = _pidfd_open(pid)
        try:
            os.waitid(_P_PIDFD, fd, _PROBE_FLAGS)
            self._epoll.register(fd, _EPOLLIN)
        except ChildProcessError:
            os.close(fd)
            raise ProcessLookupError(f'pid {pid} is not an unreaped child of this process') from None
        except BaseException:
            os.close(fd)
            raise
        return fd

    def poll(self) -> list[int]:
        keys = []
        for fd, _events in self._epoll.poll():
            if fd == self._wake_read:
                _drain_pipe(fd)
            else:
                keys.append(fd)
        return keys

    def release(self, key: int) -> None:
        try:
            self._epoll.unregister(key)
        except OSError:
            pass
        os.close(key)

    def wake(self) -> None:
        _write_wake(self._wake_write)

    def close(self) -> None:
        self._epoll.close()
        os.close(self._wake_read)
        os.close(self._wake_write)


class KqueueBackend:
    """macOS: one ``EVFILT_PROC`` / ``NOTE_EXIT`` event per child, in one kqueue.

    Registering a reaped pid fails with ``ESRCH`` (``ProcessLookupError``); a
    zombie reports its exit at once. The probe ``waitid(P_PID, pid, WNOWAIT)``
    after the registration proves the pid is still our own unreaped child.
    """

    name = 'kqueue'

    def __init__(self) -> None:
        if _kqueue is None or _kevent is None:
            raise OSError('kqueue watch needs select.kqueue and select.kevent')
        self._kqueue = _kqueue()
        self._wake_read, self._wake_write = _wake_pipe()
        self._kqueue.control([_kevent(self._wake_read, _KQ_FILTER_READ, _KQ_EV_ADD)], 0, 0)

    def watch(self, pid: int) -> int:
        assert _kevent is not None
        self._kqueue.control([_kevent(pid, _KQ_FILTER_PROC, _KQ_EV_ADD | _KQ_EV_ONESHOT, _KQ_NOTE_EXIT)], 0, 0)
        try:
            os.waitid(os.P_PID, pid, _PROBE_FLAGS)
        except ChildProcessError:
            # The registration may name another process now; remove it.
            try:
                self._kqueue.control([_kevent(pid, _KQ_FILTER_PROC, _KQ_EV_DELETE)], 0, 0)
            except OSError:
                pass
            raise ProcessLookupError(f'pid {pid} is not an unreaped child of this process') from None
        return pid

    def poll(self) -> list[int]:
        keys = []
        for event in self._kqueue.control(None, _KQUEUE_MAX_EVENTS, None):
            if event.filter == _KQ_FILTER_READ:
                _drain_pipe(event.ident)
            elif self._has_exited(event.ident):
                keys.append(event.ident)
        return keys

    def _has_exited(self, pid: int) -> bool:
        """Whether a reported pid really exited, or the event was left over from an earlier process of that pid.

        Watches are keyed by pid. A pid reused while the exit event of its
        former process still waits in the kqueue would otherwise be stamped
        with that old exit: a still running child gets its registration back.
        """
        assert _kevent is not None
        try:
            if os.waitid(os.P_PID, pid, _PROBE_FLAGS) is not None:
                return True
        except ChildProcessError:
            return True  # reaped already: it has exited
        try:
            self._kqueue.control([_kevent(pid, _KQ_FILTER_PROC, _KQ_EV_ADD | _KQ_EV_ONESHOT, _KQ_NOTE_EXIT)], 0, 0)
        except ProcessLookupError:
            return True  # it exited in between
        return False

    def release(self, key: int) -> None:
        # A one-shot event leaves the kqueue when it fires.
        pass

    def wake(self) -> None:
        _write_wake(self._wake_write)

    def close(self) -> None:
        self._kqueue.close()
        os.close(self._wake_read)
        os.close(self._wake_write)


def count_fallback(reason: str) -> None:
    """Count one task run timed on the loop instead of by the watcher, by reason."""
    executor_exit_watch_fallbacks.labels(reason=reason).inc()


# The platform table. A platform that is not here times exits on the loop.
BACKENDS: dict[str, Callable[[], ExitBackend]] = {
    'linux': PidfdBackend,
    'darwin': KqueueBackend,
}


class ExitWatch:
    """One watched process.

    ``stamp`` is written once by the watcher thread when the process exits
    (or at watch time for an ``exited_before_watch`` fallback). A plain
    attribute write and read are atomic under the GIL, so no lock is needed
    to read it. ``fallback_reason`` is set when the exit is timed on the loop.
    """

    __slots__ = ('fallback_reason', 'stamp')

    def __init__(self, stamp: ExitStamp | None = None, fallback_reason: str | None = None) -> None:
        self.stamp = stamp
        self.fallback_reason = fallback_reason


class ExitWatcher:
    """Watches task subprocesses for their exit on one daemon thread.

    The thread starts on the first watch (or on :meth:`start`). ``watch``
    runs on the event loop and must be called right after
    ``create_subprocess_exec`` returns, with no ``await`` in between, so no
    reap can happen between the spawn's return and the start of the watch.
    A reap during the spawn's own internal await is still possible, and
    ``returncode`` is set one loop hop after it, so the pid may already be
    free; each backend's ``waitid(..., WNOWAIT)`` probe catches that case.
    """

    def __init__(
        self,
        backend_factory: Callable[[], ExitBackend] | None,
        batch_pause_seconds: float = BATCH_PAUSE_SECONDS,
    ) -> None:
        self._backend_factory = backend_factory
        self._batch_pause_seconds = batch_pause_seconds
        self._lock = threading.Lock()
        self._entries: dict[int, ExitWatch] = {}
        self._backend: ExitBackend | None = None
        self._thread: threading.Thread | None = None
        self._unavailable = backend_factory is None
        # The process that started the thread: a forked child has no thread.
        self._owner_pid: int | None = None
        # One line per 60 s for watches that fail on resource pressure.
        self._failure_log = RateLimitedLog(time.monotonic)

    @property
    def backend_name(self) -> str | None:
        """The running backend's name, or None while no backend runs."""
        return self._backend.name if self._backend is not None else None

    def watch(self, proc: asyncio.subprocess.Process) -> ExitWatch:
        """Start watching ``proc`` for its exit."""
        if proc.returncode is not None:
            return self._fallback(EXITED_BEFORE_WATCH, stamp_now())
        backend = self._started_backend()
        if backend is None:
            return self._fallback(UNAVAILABLE, None)
        exit_watch = ExitWatch()
        try:
            with self._lock:
                self._entries[backend.watch(proc.pid)] = exit_watch
        except ProcessLookupError:
            return self._fallback(EXITED_BEFORE_WATCH, stamp_now())
        except OSError as exc:
            if exc.errno in _PERMANENT_ERRNOS:
                self._disable('exit_watch_refused', exc)
            else:
                self._log_watch_failure(exc)
            return self._fallback(UNAVAILABLE, None)
        return exit_watch

    def _log_watch_failure(self, exc: OSError) -> None:
        """Log a watch that failed on resource pressure, at most one line per 60 s."""
        suppressed = self._failure_log.admit('watch')
        if suppressed is not None:
            logger.warning(
                'exit_watch_failed',
                category='executor',
                error=str(exc),
                suppressed=suppressed,
                hint='this task is timed on the event loop; the watcher stays on',
            )

    @staticmethod
    def _fallback(reason: str, stamp: ExitStamp | None) -> ExitWatch:
        count_fallback(reason)
        return ExitWatch(stamp=stamp, fallback_reason=reason)

    def start(self) -> str | None:
        """Start the thread now, if it is not running; return the backend name, or None."""
        backend = self._started_backend()
        return backend.name if backend is not None else None

    def _started_backend(self) -> ExitBackend | None:
        """The running backend, after starting it on first use; None when the host cannot watch."""
        if self._owner_pid is not None and self._owner_pid != os.getpid():
            # Forked: the thread and the descriptors belong to the parent.
            self._lock = threading.Lock()
            self._entries = {}
            self._backend = None
            self._thread = None
            self._owner_pid = None
        if self._unavailable:
            return None
        if self._backend is not None:
            return self._backend
        assert self._backend_factory is not None
        try:
            backend = self._backend_factory()
        except OSError as exc:
            self._disable('exit_watcher_unavailable', exc)
            return None
        thread = threading.Thread(target=self._run, args=(backend,), name='drakkar-exit-watcher', daemon=True)
        # Under the lock, so the new thread cannot read ``_backend`` before it is set.
        with self._lock:
            self._backend = backend
            self._thread = thread
            try:
                thread.start()
            except RuntimeError as exc:
                # "can't start new thread" (a pids limit): no backend without its thread.
                self._backend = None
                self._thread = None
                backend.close()
                self._disable('exit_watcher_unavailable', exc)
                return None
        self._owner_pid = os.getpid()
        return backend

    def _disable(self, event: str, exc: Exception) -> None:
        """Time every later exit on the loop; log why once."""
        self._unavailable = True
        logger.warning(
            event,
            category='executor',
            error=str(exc),
            hint='task run times are measured on the event loop and include loop stalls',
        )

    def _run(self, backend: ExitBackend) -> None:
        """Thread body: stamp each batch of exits into its watches."""
        try:
            while True:
                keys = backend.poll()
                stamp = stamp_now()
                with self._lock:
                    if self._backend is not backend:
                        return
                    for key in keys:
                        exit_watch = self._entries.pop(key, None)
                        backend.release(key)
                        if exit_watch is not None:
                            exit_watch.stamp = stamp
                if keys:
                    time.sleep(self._batch_pause_seconds)
        except Exception as exc:
            # Pending watches keep no stamp, so their tasks are timed on the loop.
            logger.error('exit_watcher_failed', category='executor', error=str(exc), exc_info=exc)
            with self._lock:
                self._unavailable = True
                _release_all(backend, self._take_keys())

    def _take_keys(self) -> list[int]:
        """Empty the table of pending watches; return their keys.

        Each pending watch is marked ``unavailable`` and counted: nobody will
        stamp it now, so its task is timed on the loop with no notice.
        """
        keys = list(self._entries)
        for exit_watch in self._entries.values():
            exit_watch.fallback_reason = UNAVAILABLE
            count_fallback(UNAVAILABLE)
        self._entries.clear()
        return keys

    def stop(self, timeout_seconds: float = 1.0) -> None:
        """Stop the thread; pending watches are timed on the loop. A later watch starts it again."""
        with self._lock:
            backend, thread = self._backend, self._thread
            if backend is None or thread is None:
                return
            self._backend = None
            self._thread = None
            pending_keys = self._take_keys()
        backend.wake()
        thread.join(timeout_seconds)
        if not thread.is_alive():
            # A thread still inside poll would read closed descriptors; leave
            # them to the process exit in that case.
            _release_all(backend, pending_keys)
            backend.close()


def _release_all(backend: ExitBackend, keys: list[int]) -> None:
    """Free what each pending watch holds (a pidfd on Linux), ignoring one that is gone already."""
    for key in keys:
        try:
            backend.release(key)
        except OSError:
            pass


_shared_watcher: ExitWatcher | None = None


def shared_watcher() -> ExitWatcher:
    """The process's one exit watcher, built on first use for ``sys.platform``."""
    global _shared_watcher
    if _shared_watcher is None:
        _shared_watcher = ExitWatcher(BACKENDS.get(sys.platform))
    return _shared_watcher
