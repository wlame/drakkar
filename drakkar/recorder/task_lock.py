"""The task lock: one holder at a time per cluster task.

This is the lock of the ``filesystem`` election provider
(:class:`drakkar.recorder.election.FilesystemElection`). The lock of task
``<task>`` of a cluster is ``tasks/<cluster>/<task>.lock`` in the shared
folder (:func:`drakkar.recorder.history.task_lock_dir`). It is created with
an exclusive create, so exactly one worker of the cluster holds it. The
holder refreshes the lock's mtime while its run goes on. A lock nobody
refreshed for ``stale_seconds`` (the lease) belongs to a dead holder: the
next worker renames it aside to ``<task>.lock.stale.<token>`` (one rename
wins), deletes it, and creates its own. A holder can re-read the lock and
stop if the token is no longer its own (fencing); the archive pass does so
right before each publish, so a holder that stalled past the stale age
publishes nothing.

Ages are measured on the file server's clock
(:func:`drakkar.recorder.history.folder_server_now`), so no host's wall
clock is compared with another's. The lock body bytes are a contract shared
by every implementation (``tests/fixtures/history_meta.json``, key ``lock``).

All methods are synchronous: callers on the event loop run them in a thread.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import secrets
import stat
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import structlog

from drakkar.config.autodiscover import FILESYSTEM_PROVIDER
from drakkar.dbfiles import DB_FILE_MODE
from drakkar.recorder.history import folder_server_now, serialize_json
from drakkar.recorder.lease import LeaseHolder, holder_from_body
from drakkar.timefmt import format_rfc3339_micro

logger = structlog.get_logger()

# The lock file of a task is its name plus this suffix; a lock moved aside by a
# takeover is the name plus the infix plus the taker's token.
LOCK_SUFFIX = '.lock'
STALE_INFIX = '.lock.stale.'
# The election provider this lock implements: named in the lock body and the takeover log line.
REFRESH_THREAD_NAME = 'drakkar-task-lock'
DEFAULT_REFRESH_SECONDS = 60.0
MAX_LOCK_BYTES = 4096

_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK

# The two calls a contested takeover races on, as module attributes so tests
# can replay what another worker does in between.
_rename_aside = os.rename
_link_back = os.link


def _utc_now() -> datetime:
    return datetime.now(UTC)


class TaskLock:
    """The lock of one task of a cluster, the file ``<name>.lock`` in the cluster's task lock folder."""

    def __init__(
        self,
        folder: str,
        name: str,
        *,
        worker: str,
        instance: str,
        stale_seconds: float,
        scope: str,
        refresh_seconds: float = DEFAULT_REFRESH_SECONDS,
        now: Callable[[], datetime] = _utc_now,
        token: str | None = None,
        auto_refresh: bool = True,
    ) -> None:
        """Prepare the lock of task ``name`` in ``folder``; nothing touches the folder before :meth:`acquire`.

        ``scope`` is the election scope of the task (``<cluster>/<task>``);
        it names the lock in the log lines. ``token`` is the holder's token (16 hex characters are made when
        ``None``). With ``auto_refresh`` a thread refreshes the lock every
        ``refresh_seconds`` while it is held; without it the caller calls
        :meth:`refresh` itself.
        """
        self._folder = folder
        self._name = name
        self._path = os.path.join(folder, name + LOCK_SUFFIX)
        self._scope = scope
        self._worker = worker
        self._instance = instance
        self._stale_seconds = stale_seconds
        self._refresh_seconds = refresh_seconds
        self._now = now
        self._token = token if token is not None else secrets.token_hex(8)
        self._auto_refresh = auto_refresh
        self._stop_refreshing = threading.Event()
        self._refresher: threading.Thread | None = None

    @property
    def token(self) -> str:
        return self._token

    def acquire(self) -> bool:
        """Take the lock; take over a stale one once. False when another worker holds it."""
        if not self._create() and not (self._take_over_stale() and self._create()):
            return False
        if not self._auto_refresh:
            return True
        self._refresher = threading.Thread(target=self._refresh_loop, name=REFRESH_THREAD_NAME, daemon=True)
        self._refresher.start()
        return True

    def is_ours(self) -> bool:
        """True while the lock file still holds this holder's token (the fencing check).

        False when the lock is gone or holds another token. A read that fails
        for another reason (an I/O error on the mount) raises ``OSError``: it
        says nothing about who holds the lock, so it must not read as a
        takeover.
        """
        body = self._read_body()
        return body is not None and body.get('token') == self._token

    def refresh(self) -> bool:
        """Move the lock's mtime to now, while the lock is still ours; False once it is not.

        Raises ``OSError`` when the lock cannot be read (see :meth:`is_ours`).
        """
        if not self.is_ours():
            return False
        try:
            os.utime(self._path, None)
        except OSError:
            # A refresh is best effort: the fencing check decides whether the
            # holder may go on.
            pass
        return True

    def stop_refreshing(self) -> None:
        """Stop the background refresh (used by release, and by tests that simulate a dead holder)."""
        self._stop_refreshing.set()

    def release(self) -> None:
        """Stop refreshing and delete the lock, but only while it holds our token."""
        self.stop_refreshing()
        if not self.is_ours():
            return
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass

    def _refresh_loop(self) -> None:
        # Event.wait returns True once stop_refreshing is called.
        while not self._stop_refreshing.wait(self._refresh_seconds):
            # Best effort, like the mtime move: the fencing check decides
            # whether the holder may go on.
            with contextlib.suppress(OSError):
                self.refresh()

    def _create(self) -> bool:
        body = serialize_json(
            {
                'acquired_at': format_rfc3339_micro(self._now()),
                'instance': self._instance,
                'provider': FILESYSTEM_PROVIDER,
                'token': self._token,
                'worker': self._worker,
            }
        )
        try:
            fd = os.open(self._path, _CREATE_FLAGS, DB_FILE_MODE)
        except FileExistsError:
            return False
        try:
            view = memoryview(body)
            while view:
                view = view[os.write(fd, view) :]
        finally:
            os.close(fd)
        return True

    def _take_over_stale(self) -> bool:
        """Remove a lock older than the stale age; True when the caller may retry the create."""
        try:
            info = os.lstat(self._path)
        except FileNotFoundError:
            return True
        if not stat.S_ISREG(info.st_mode):
            # Not a lock this protocol wrote: never delete it.
            return False
        age = self.server_now() - info.st_mtime
        if age <= self._stale_seconds:
            return False
        try:
            previous = self._read_body() or {}
        except OSError:
            # The body only names the previous holder in the log line below.
            previous = {}
        aside = os.path.join(self._folder, self._name + STALE_INFIX + self._token)
        try:
            _rename_aside(self._path, aside)
        except FileNotFoundError:
            # Another worker renamed it first: it is taking over.
            return False
        if not self._still_stale(aside):
            # Between our check and the rename another taker replaced the
            # stale lock with a fresh one, and we moved that one aside: put
            # it back (link refuses to overwrite) and stand down.
            self._put_back(aside)
            return False
        try:
            os.unlink(aside)
        except FileNotFoundError:
            pass
        logger.warning(
            'periodic_lock_taken_over',
            category='periodic',
            provider=FILESYSTEM_PROVIDER,
            scope=self._scope,
            previous_worker=previous.get('worker'),
            previous_instance=previous.get('instance'),
            age_seconds=round(age, 3),
        )
        return True

    def _still_stale(self, path: str) -> bool:
        try:
            mtime = os.lstat(path).st_mtime
        except FileNotFoundError:
            return False
        return self.server_now() - mtime > self._stale_seconds

    def _put_back(self, aside: str) -> None:
        """Return a fresh lock moved aside by mistake; a hard link never overwrites a newer lock."""
        try:
            _link_back(aside, self._path)
        except OSError as exc:
            # Another worker created a lock meanwhile, or the mount has no
            # hard links. The lock's holder is fenced at its next check; the
            # newer lock's owner runs the task instead.
            logger.warning(
                'periodic_lock_put_back_failed',
                category='periodic',
                scope=self._scope,
                folder=self._folder,
                error=str(exc),
            )
        try:
            os.unlink(aside)
        except OSError:
            pass

    def server_now(self) -> float:
        """The file server's current time, read in the lock's folder; the takeover age checks use it."""
        return folder_server_now(self._folder, self._instance)

    def _read_body(self) -> dict[str, Any] | None:
        """The lock body; ``None`` when there is no lock this protocol wrote (see :func:`read_lock_body`)."""
        return read_lock_body(self._path)


def read_lock_body(path: str) -> dict[str, Any] | None:
    """The body of the lock file ``path``; ``None`` when there is no lock this protocol wrote. Other read errors raise ``OSError``.

    ``None`` for a missing file, a symlink (the open refuses to follow it), a
    file that is not regular, and a body that is not a JSON object.
    """
    try:
        fd = os.open(path, _READ_FLAGS)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            return None
        raise
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return None
        raw = os.read(fd, MAX_LOCK_BYTES)
    finally:
        os.close(fd)
    try:
        body = json.loads(raw.decode('utf-8'))
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def read_lock_holder(path: str, *, now: Callable[[], float] = time.time) -> LeaseHolder | None:
    """Who holds the lock file ``path``; ``None`` when there is none. Read-only and blocking.

    It writes nothing to the lock folder: no clock marker, no change time.
    So the age (time since the lock's last refresh, its mtime) is measured
    against this host's clock (``now``) and is approximate; it is never
    below 0. A lock file whose body is not a lock names no holder, and a lock
    older than its lease is still reported: the store keeps it until a taker
    moves it aside. An error of the read itself raises ``OSError``.
    """
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    body = read_lock_body(path)
    return holder_from_body(body, age_seconds=max(0.0, now() - info.st_mtime), age_approximate=True)
