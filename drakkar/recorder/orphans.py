"""Orphan folders: the own folders of worker names that no process uses any more.

A worker keeps its own files in ``<storage.dir>/<cluster>/<worker>/``. When a
worker name never starts again (a recreated pod with a new host name, a
renamed worker), its folder stays behind with recorder files that were never
sealed. The leftover pass of every worker of the same cluster also looks at
the sibling folders and, when it can prove the name is dead, seals and
publishes the recorder files under the owner's name and deletes the rest.

The proof has three parts, checked by the recorder: the folder's owner lock
is free (no process on this host holds the folder), every file in it is idle
for the leftover floor, and the registry lists the name as not online. With
no registry nothing can prove the name dead, so the folder is only logged.

Every function here does blocking file work: call it in a thread.
"""

from __future__ import annotations

import contextlib
import os

from drakkar.cache.engine import CACHE_FILE_SUFFIX
from drakkar.config.identity import is_valid_worker_name
from drakkar.ownerlock import OWNER_LOCK_NAME, remove_download_temps
from drakkar.watchdog import WATCHDOG_SUFFIX

_SQLITE_SIDECARS = ('', '-wal', '-shm', '-journal')


def sibling_folders(cluster_dir: str, own_worker: str) -> list[tuple[str, str]]:
    """The other worker folders of ``cluster_dir`` as ``(path, worker)``, sorted by worker.

    A worker folder is a real folder (never a link) whose name passes the
    worker name rule. A missing ``cluster_dir`` has none.
    """
    try:
        with os.scandir(cluster_dir) as scan:
            entries = list(scan)
    except FileNotFoundError:
        return []
    folders = [
        (entry.path, entry.name)
        for entry in entries
        if entry.name != own_worker and is_valid_worker_name(entry.name) and entry.is_dir(follow_symlinks=False)
    ]
    return sorted(folders, key=lambda folder: folder[1])


def folder_is_idle(folder: str, *, now: float, floor_seconds: float) -> bool:
    """True when no file in ``folder`` changed for more than ``floor_seconds`` (local clock).

    The owner lock file is left out: its mtime says when the folder was
    first locked, not when it was last used. A file that vanishes during the
    look counts as a change.
    """
    try:
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.name == OWNER_LOCK_NAME:
                    continue
                if now - entry.stat(follow_symlinks=False).st_mtime <= floor_seconds:
                    return False
    except FileNotFoundError:
        return False
    return True


def remove_orphan_files(folder: str, worker: str) -> bool:
    """Delete the cache file, the watchdog file and the download temps of ``worker``; remove the folder when empty.

    Call it only after every recorder file of the folder was sealed and
    published, while holding the folder's owner lock. The lock file goes
    last, and only when nothing else is left, so a folder that keeps an
    unknown file keeps its lock file too and is looked at again. Returns
    whether the folder is gone.
    """
    for name in (
        *(f'{worker}{CACHE_FILE_SUFFIX}{suffix}' for suffix in _SQLITE_SIDECARS),
        f'{worker}{WATCHDOG_SUFFIX}',
    ):
        with contextlib.suppress(FileNotFoundError):
            os.unlink(os.path.join(folder, name))
    remove_download_temps(folder, worker)
    if os.listdir(folder) != [OWNER_LOCK_NAME]:
        return False
    os.unlink(os.path.join(folder, OWNER_LOCK_NAME))
    try:
        os.rmdir(folder)
    except OSError:
        # A process of that name started between the unlink and here and
        # created its lock file again; the folder is its own now.
        return False
    return True
