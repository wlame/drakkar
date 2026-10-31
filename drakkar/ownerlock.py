"""A worker's own folder ``<storage.dir>/<cluster>/<worker>/``: its creation, its owner lock, its start sweep.

At start a worker opens ``<own folder>/.owner.lock`` and takes an exclusive,
non-blocking ``flock`` on it. It keeps the file open for the life of the
process; the kernel drops the lock when the process dies, also on
``kill -9``, so a lock that is held always means a live process. A second
process with the same worker name on the same host cannot take it and does
not start.

``flock`` is reliable only on a local filesystem, which is why
``storage.dir`` must be one. Another worker that finds a folder whose lock is
free may clean it up (the orphan pass of the recorder); it takes the lock
first and holds it while it works.

Every function here does blocking file work: call it in a thread.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import stat

from drakkar.dbfiles import DB_DIR_MODE

# The lock file in each own folder. A dot name: no listing of recorder files matches it.
OWNER_LOCK_NAME = '.owner.lock'
OWNER_LOCK_MODE = 0o600

FOLDER_IN_USE_MESSAGE = (
    'storage.dir: the folder {folder} is in use by another process; '
    'give each worker a unique worker name (worker.name or WORKER_ID)'
)

_OPEN_FLAGS = os.O_RDWR | os.O_NOFOLLOW

# The download temps of the API server (a copy of the live file or of the
# memory store) and their SQLite sidecars: ``.<worker>-<...>.<hex>.download``.
_DOWNLOAD_TEMP_SIDECARS = ('-journal', '-wal', '-shm')


class FolderInUseError(RuntimeError):
    """Another live process holds the owner lock of the folder."""


class OwnerLock:
    """A held owner lock. :meth:`release` closes the file, which drops the lock; safe to call twice."""

    def __init__(self, folder: str, fd: int) -> None:
        self._folder = folder
        self._fd: int | None = fd

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def path(self) -> str:
        return os.path.join(self._folder, OWNER_LOCK_NAME)

    @property
    def held(self) -> bool:
        return self._fd is not None

    def release(self) -> None:
        """Drop the lock by closing the file. The file itself stays in the folder."""
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)


def _try_lock(folder: str, *, create: bool) -> OwnerLock | None:
    """Open the lock file of ``folder`` and take the lock; ``None`` when another process holds it.

    Raises ``FileNotFoundError`` when ``create`` is false and the folder has
    no lock file, and ``OSError`` for any other open failure (a symlink in
    place of the file is refused).
    """
    flags = _OPEN_FLAGS | os.O_CREAT if create else _OPEN_FLAGS
    fd = os.open(os.path.join(folder, OWNER_LOCK_NAME), flags, OWNER_LOCK_MODE)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise
    return OwnerLock(folder, fd)


def take_own_lock(folder: str) -> OwnerLock:
    """Take the owner lock of this worker's own folder, creating the lock file when it is missing.

    Raises:
        FolderInUseError: another live process holds the lock.
    """
    lock = _try_lock(folder, create=True)
    if lock is None:
        raise FolderInUseError(FOLDER_IN_USE_MESSAGE.format(folder=folder))
    return lock


def try_sibling_lock(folder: str) -> OwnerLock | None:
    """Take the owner lock of another worker's folder; ``None`` when it is held or the folder has no lock file.

    Never creates a file: a folder without a lock file is not a worker
    folder, and nothing is written into it.
    """
    with contextlib.suppress(FileNotFoundError):
        return _try_lock(folder, create=False)
    return None


def _make_folder(path: str) -> None:
    """Create one folder with mode 0700; an existing folder keeps its mode."""
    with contextlib.suppress(FileExistsError):
        os.mkdir(path, DB_DIR_MODE)


def download_temp_pattern(worker: str) -> re.Pattern[str]:
    """The names of the download temps of ``worker`` and their sidecars."""
    sidecars = '|'.join(re.escape(suffix) for suffix in _DOWNLOAD_TEMP_SIDECARS)
    return re.compile(rf'\.{re.escape(worker)}-.+\.download({sidecars})?')


def remove_download_temps(folder: str, worker: str) -> int:
    """Delete the download temps of ``worker`` in ``folder``; return how many went.

    Only safe while the caller holds the folder's owner lock: no live
    download of this worker can be writing them then.
    """
    pattern = download_temp_pattern(worker)
    removed = 0
    for name in os.listdir(folder):
        if pattern.fullmatch(name):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(os.path.join(folder, name))
                removed += 1
    return removed


def open_own_folder(storage_dir: str, own_dir: str, worker: str) -> OwnerLock:
    """Create the own folder, take its owner lock, and delete the download temps an earlier process left.

    ``storage_dir`` is created with its missing parents; the cluster folder
    and ``own_dir`` below it one by one, each with mode 0700 (``os.makedirs``
    gives the mode only to the last folder). A folder that exists keeps its
    mode. The lock proves that no other process uses the folder, so the
    sweep cannot delete a temp that a live download still writes.

    Raises:
        FolderInUseError: another live process holds the lock.
        OSError: a folder cannot be created, or the lock file cannot be opened.
    """
    os.makedirs(storage_dir, mode=DB_DIR_MODE, exist_ok=True)
    _make_folder(os.path.dirname(own_dir))
    _make_folder(own_dir)
    lock = take_own_lock(own_dir)
    try:
        remove_download_temps(own_dir, worker)
    except BaseException:
        lock.release()
        raise
    return lock


def unsafe_folder_levels(storage_dir: str, own_dir: str) -> list[tuple[str, str]]:
    """The levels from ``storage_dir`` down to ``own_dir`` that another local user could change, with the reason.

    A level is unsafe when it is world-writable without the sticky bit
    (another user can rename or replace what is in it) or owned by another
    user. ``O_NOFOLLOW`` guards only the last part of a path, so each level
    matters. A level that cannot be read is skipped.
    """
    levels = (storage_dir, os.path.dirname(own_dir), own_dir)
    unsafe: list[tuple[str, str]] = []
    for level in levels:
        try:
            info = os.stat(level)
        except OSError:
            continue
        if info.st_uid != os.geteuid():
            unsafe.append((level, 'other_owner'))
        elif info.st_mode & stat.S_IWOTH and not info.st_mode & stat.S_ISVTX:
            unsafe.append((level, 'world_writable'))
    return unsafe
