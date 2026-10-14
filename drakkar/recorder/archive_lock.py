"""The archive lock: one archiver at a time per cluster history folder.

``history/<cluster>/.archive.lock`` is created with an exclusive create, so
exactly one worker of the cluster holds it. The holder refreshes the lock's
mtime while its pass runs. A lock nobody refreshed for ``lock_stale_seconds``
belongs to a dead archiver: the next worker renames it aside (one rename
wins), deletes it, and creates its own. Right before each publish the holder
re-reads the lock and stops if the token is no longer its own (fencing), so
a holder that stalled past the stale age publishes nothing.

Ages are measured on the file server's clock: a freshly touched marker file
``.clock.<instance>`` in the same folder gives "now", so no host's wall clock
is compared with another's. The lock body bytes are a contract shared by
every implementation (``tests/fixtures/history_meta.json``, key ``lock``).

All methods are synchronous: the archive pass runs in a thread.
"""

from __future__ import annotations

import json
import os
import secrets
import stat
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import structlog

from drakkar.dbfiles import DB_FILE_MODE
from drakkar.recorder.history import serialize_json
from drakkar.timefmt import format_rfc3339_micro

logger = structlog.get_logger()

LOCK_NAME = '.archive.lock'
CLOCK_PREFIX = '.clock.'
STALE_PREFIX = '.archive.lock.stale.'
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


class ArchiveLock:
    """The lock of one cluster history folder, held by one archive pass."""

    def __init__(
        self,
        folder: str,
        *,
        worker: str,
        instance: str,
        stale_seconds: float,
        refresh_seconds: float = DEFAULT_REFRESH_SECONDS,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._folder = folder
        self._path = os.path.join(folder, LOCK_NAME)
        self._worker = worker
        self._instance = instance
        self._stale_seconds = stale_seconds
        self._refresh_seconds = refresh_seconds
        self._now = now
        self._token = secrets.token_hex(8)
        self._stop_refreshing = threading.Event()
        self._refresher: threading.Thread | None = None

    @property
    def token(self) -> str:
        return self._token

    def acquire(self) -> bool:
        """Take the lock; take over a stale one once. False when another archiver holds it."""
        if not self._create() and not (self._take_over_stale() and self._create()):
            return False
        self._refresher = threading.Thread(target=self._refresh_loop, name='drakkar-archive-lock', daemon=True)
        self._refresher.start()
        return True

    def is_ours(self) -> bool:
        """True while the lock file still holds this pass's token (the fencing check)."""
        body = self._read_body()
        return body is not None and body.get('token') == self._token

    def refresh(self) -> None:
        """Move the lock's mtime to now, while the lock is still ours."""
        if not self.is_ours():
            return
        try:
            os.utime(self._path, None)
        except OSError:
            # A refresh is best effort: the fencing check before each publish
            # decides whether the pass may go on.
            pass

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
            self.refresh()

    def _create(self) -> bool:
        body = serialize_json(
            {
                'acquired_at': format_rfc3339_micro(self._now()),
                'instance': self._instance,
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
        previous = self._read_body() or {}
        aside = os.path.join(self._folder, STALE_PREFIX + self._token)
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
            'recorder_archive_lock_taken_over',
            category='recorder',
            folder=self._folder,
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
            # hard links. The lock's holder is fenced at its next check and
            # publishes nothing; the newer lock's owner archives instead.
            logger.warning(
                'recorder_archive_lock_put_back_failed',
                category='recorder',
                folder=self._folder,
                error=str(exc),
            )
        try:
            os.unlink(aside)
        except OSError:
            pass

    def server_now(self) -> float:
        """The file server's current time: the mtime of a freshly touched marker file.

        Ages of files in the folder (the lock, work files to sweep) are
        measured against this, so no host's wall clock is compared with
        another's.
        """
        marker = os.path.join(self._folder, f'{CLOCK_PREFIX}{self._instance}')
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, DB_FILE_MODE)
        os.close(fd)
        try:
            os.utime(marker, None)
            return os.lstat(marker).st_mtime
        finally:
            try:
                os.unlink(marker)
            except FileNotFoundError:
                pass

    def _read_body(self) -> dict[str, Any] | None:
        try:
            fd = os.open(self._path, _READ_FLAGS)
        except OSError:
            return None
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                return None
            raw = os.read(fd, MAX_LOCK_BYTES)
        except OSError:
            return None
        finally:
            os.close(fd)
        try:
            body = json.loads(raw.decode('utf-8'))
        except ValueError:
            return None
        return body if isinstance(body, dict) else None
