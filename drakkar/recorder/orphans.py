"""Orphan folders: the own folders of worker names that no process uses any more.

A worker keeps its own files in ``<storage.dir>/<cluster>/<worker>/``. When a
worker name stops (a recreated pod with a new host name, a renamed worker,
or a worker that stays stopped), its folder stays behind with recorder files
that were never sealed. The leftover pass of every worker of the same
cluster also looks at the sibling folders and, when it can prove the name is
not running, seals and publishes the recorder files under the owner's name
and deletes the rest.

The proof has three parts: the folder's owner lock is free (no process on
this host holds the folder), every file in it is idle for the leftover
floor, and the registry lists the name as not online. With no registry
nothing can prove that, so the folder is only logged.

The folder lock is held only for a few renames: the recorder files move into
a staging folder ``<cluster folder>/.orphan-<worker>-<16 hex>/`` on the same
filesystem, the rest of the folder is deleted, and the lock is released.
The slow part (the seal, and the copy to the shared history on a network
mount) runs from the staging folder under its own lock, so a worker of that
name that starts again meanwhile is never refused. A staging folder whose
lock is free (its sealer died) is finished by a later pass.

Each folder is handled on its own: an error in one is logged (at most once
a minute per folder), counted in ``drakkar_recorder_orphan_failures_total``,
and the pass goes on with the next folder. Nothing here ever fails the
recorder's own leftover pass.

The module-level functions do blocking file work: call them in a thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import secrets
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

from drakkar.cache.engine import CACHE_FILE_SUFFIX
from drakkar.config.identity import is_valid_worker_name
from drakkar.dbfiles import DB_DIR_MODE
from drakkar.metrics import recorder_orphan_failures
from drakkar.ownerlock import (
    OWNER_LOCK_NAME,
    OwnerLock,
    remove_download_temps,
    take_own_lock,
    try_sibling_lock,
)
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder.sealing import own_db_files, publish_in_background, seal_closed_file
from drakkar.watchdog import WATCHDOG_SUFFIX

logger = structlog.get_logger()

_SQLITE_SIDECARS = ('', '-wal', '-shm', '-journal')

STAGING_PREFIX = '.orphan-'
_STAGING_NAME = re.compile(r'\.orphan-([A-Za-z0-9_-]{1,128})-[0-9a-f]{16}')

# A file whose modification time is this far ahead of the local clock (the
# clock was stepped back) keeps its folder from ever looking idle.
FUTURE_MTIME_TOLERANCE_SECONDS = 2.0

# With autodiscover on, a folder whose name the registry cannot judge (no
# fresh refresh, an unknown verdict) for this long is logged once.
UNKNOWN_VERDICT_REPORT_SECONDS = 3600.0

_FOUND_HINTS = {
    'no_registry': (
        'no process holds this folder and its files are idle, but with autodiscover off '
        'the worker cannot prove the name is not running; seal or delete the folder by hand'
    ),
    'registry_unknown': (
        'no process holds this folder and its files are idle, but the registry has not said '
        'for an hour whether the name is online; check the registry provider'
    ),
}


@dataclass(frozen=True)
class IdleCheck:
    """The idle verdict of a folder, and the first file whose time is ahead of the local clock."""

    idle: bool
    future_file: str = ''
    seconds_ahead: float = 0.0


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


def staging_folders(cluster_dir: str) -> list[tuple[str, str]]:
    """The staging folders of ``cluster_dir`` as ``(path, worker)``, sorted by name."""
    try:
        with os.scandir(cluster_dir) as scan:
            entries = list(scan)
    except FileNotFoundError:
        return []
    folders: list[tuple[str, str]] = []
    for entry in entries:
        match = _STAGING_NAME.fullmatch(entry.name)
        if match is not None and entry.is_dir(follow_symlinks=False):
            folders.append((entry.path, match.group(1)))
    return sorted(folders)


def check_idle(folder: str, *, now: float, floor_seconds: float) -> IdleCheck:
    """Whether no file in ``folder`` changed for more than ``floor_seconds`` (local clock).

    The owner lock file is left out: its mtime says when the folder was
    first locked, not when it was last used. A file that vanishes during the
    look counts as a change. A file whose time is ahead of ``now`` keeps the
    folder busy, and the first one is named so the caller can say so.
    """
    try:
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.name == OWNER_LOCK_NAME:
                    continue
                age = now - entry.stat(follow_symlinks=False).st_mtime
                if age < -FUTURE_MTIME_TOLERANCE_SECONDS:
                    return IdleCheck(idle=False, future_file=entry.name, seconds_ahead=-age)
                if age <= floor_seconds:
                    return IdleCheck(idle=False)
    except FileNotFoundError:
        return IdleCheck(idle=False)
    return IdleCheck(idle=True)


def move_to_staging(folder: str, worker: str, cluster_dir: str) -> tuple[str, OwnerLock] | None:
    """Move the recorder files of ``worker`` (with their sidecars) into a new staging folder; ``None`` when it has none.

    Call it while holding the folder's owner lock. The staging folder is
    created with mode 0700 and locked before anything moves into it, so no
    other pass takes it; the caller gets its lock. The moves are renames on
    one filesystem. A failure leaves the files moved so far in the staging
    folder (a later pass finishes it) and raises.
    """
    files = own_db_files(folder, worker)
    if not files:
        return None
    staging = os.path.join(cluster_dir, f'{STAGING_PREFIX}{worker}-{secrets.token_hex(8)}')
    os.mkdir(staging, DB_DIR_MODE)
    lock = take_own_lock(staging)
    try:
        for path in files:
            for suffix in _SQLITE_SIDECARS:
                with contextlib.suppress(FileNotFoundError):
                    os.rename(f'{path}{suffix}', os.path.join(staging, f'{os.path.basename(path)}{suffix}'))
    except BaseException:
        lock.release()
        raise
    return staging, lock


def remove_orphan_files(folder: str, worker: str) -> bool:
    """Delete the cache file, the watchdog file and the download temps of ``worker``; remove the folder when empty.

    Call it after the recorder files left the folder, while holding the
    folder's owner lock. The lock file goes last, and only when nothing else
    is left, so a folder that keeps an unknown file keeps its lock file too
    and is looked at again. Returns whether the folder is gone.
    """
    for name in (
        *(f'{worker}{CACHE_FILE_SUFFIX}{suffix}' for suffix in _SQLITE_SIDECARS),
        f'{worker}{WATCHDOG_SUFFIX}',
    ):
        with contextlib.suppress(FileNotFoundError):
            os.unlink(os.path.join(folder, name))
    remove_download_temps(folder, worker)
    return _remove_if_only_lock_left(folder)


def _remove_if_only_lock_left(folder: str) -> bool:
    """Unlink the lock file and remove ``folder`` when the lock file is all that is left; return whether it is gone."""
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


def _lock_in(result: Any) -> OwnerLock | None:
    """The owner lock a locking call returned: the lock itself, or the lock of a ``(path, lock)`` pair."""
    if isinstance(result, OwnerLock):
        return result
    if isinstance(result, tuple):
        return _lock_in(result[1])
    return None


def _release_unclaimed(task: asyncio.Future[Any]) -> None:
    """Release the lock a locking thread took after its caller was cancelled."""
    if task.cancelled() or task.exception() is not None:
        return
    lock = _lock_in(task.result())
    if lock is not None:
        lock.release()


async def _locking_call(func: Callable[..., Any], *args: Any) -> Any:
    """Run a blocking call that may take an owner lock; a lock taken after the caller was cancelled is released.

    A thread cannot be stopped, so a cancelled caller would otherwise leave
    the lock it takes held until the process ends.
    """
    task = asyncio.ensure_future(asyncio.to_thread(func, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(_release_unclaimed)
        raise


class _Step:
    """The step a folder is at, named in the failure log line."""

    def __init__(self) -> None:
        self.name = 'list'


class OrphanSealer:
    """Seals the orphan folders of one cluster folder for one recorder.

    ``worker_online`` answers whether the registry lists a worker name as
    online (``True``, ``False`` or ``None`` for cannot tell); ``None`` in place
    of the function means no registry runs. ``history_dir`` gives the folder
    the sealed files go to. ``instance`` and ``cluster`` fill the meta files.
    """

    def __init__(
        self,
        *,
        cluster_dir: str,
        own_worker: str,
        worker_online: Callable[[str], bool | None] | None,
        history_dir: Callable[[], str],
        instance: str,
        cluster: str,
        now_monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._cluster_dir = cluster_dir
        self._own_worker = own_worker
        self._worker_online = worker_online
        self._history_dir = history_dir
        self._instance = instance
        self._cluster = cluster
        self._now_monotonic = now_monotonic
        self._failure_log = RateLimitedLog(now_monotonic)
        # (folder, reason) pairs already logged, so each is logged once per process.
        self._reported: set[tuple[str, str]] = set()
        # Monotonic time since when the registry has not judged a folder's name.
        self._unknown_since: dict[str, float] = {}

    async def run(self, floor_seconds: float) -> None:
        """One pass: finish the staging folders a dead sealer left, then look at every sibling folder.

        Never raises, except a cancellation: a failure is logged, counted,
        and the pass goes on with the next folder.
        """
        step = _Step()
        try:
            staged = await asyncio.to_thread(staging_folders, self._cluster_dir)
            siblings = await asyncio.to_thread(sibling_folders, self._cluster_dir, self._own_worker)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._failed(self._cluster_dir, '', step, exc)
            return
        for staging, worker in staged:
            await self._guarded(staging, worker, step, self._finish_staging(staging, worker, step))
        for folder, worker in siblings:
            await self._guarded(folder, worker, step, self._visit(folder, worker, floor_seconds, step))

    async def _guarded(self, folder: str, worker: str, step: _Step, work: Coroutine[Any, Any, None]) -> None:
        """Await ``work``; an error is logged and counted, and the pass goes on."""
        try:
            await work
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._failed(folder, worker, step, exc)

    async def _failed(self, folder: str, worker: str, step: _Step, exc: BaseException) -> None:
        recorder_orphan_failures.inc()
        await self._failure_log.emit(
            folder,
            logger.awarning,
            'recorder_orphan_folder_failed',
            category='recorder',
            folder=folder,
            worker=worker,
            stage=step.name,
            error=str(exc),
            error_type=type(exc).__name__,
        )

    async def _visit(self, folder: str, worker: str, floor_seconds: float, step: _Step) -> None:
        """Seal the folder of ``worker`` when no process holds it, it is idle, and the name is not online."""
        step.name = 'lock'
        lock = await _locking_call(try_sibling_lock, folder)
        if lock is None:
            return
        staged: tuple[str, OwnerLock] | None = None
        try:
            step.name = 'idle'
            check = await asyncio.to_thread(check_idle, folder, now=time.time(), floor_seconds=floor_seconds)
            if check.future_file:
                await self._report_clock_ahead(folder, worker, check)
            if not check.idle or not await self._name_proved_not_running(folder, worker):
                return
            step.name = 'stage'
            staged = await _locking_call(move_to_staging, folder, worker, self._cluster_dir)
            step.name = 'cleanup'
            removed = await asyncio.to_thread(remove_orphan_files, folder, worker)
        except BaseException:
            # The staged files wait for a later pass, which needs the staging lock free.
            if staged is not None:
                await asyncio.to_thread(staged[1].release)
            raise
        finally:
            await asyncio.to_thread(lock.release)
        files = 0
        if staged is not None:
            staging, staging_lock = staged
            try:
                files = await self._seal_staged(staging, worker, step)
                step.name = 'cleanup'
                await asyncio.to_thread(_remove_if_only_lock_left, staging)
            finally:
                await asyncio.to_thread(staging_lock.release)
        await self._log_sealed(folder, worker, files, removed)

    async def _finish_staging(self, staging: str, worker: str, step: _Step) -> None:
        """Seal what a staging folder still holds when its lock is free (its sealer died), then remove it."""
        step.name = 'lock'
        lock = await _locking_call(try_sibling_lock, staging)
        if lock is None:
            return
        try:
            files = await self._seal_staged(staging, worker, step)
            step.name = 'cleanup'
            removed = await asyncio.to_thread(_remove_if_only_lock_left, staging)
        finally:
            await asyncio.to_thread(lock.release)
        await self._log_sealed(staging, worker, files, removed)

    async def _seal_staged(self, staging: str, worker: str, step: _Step) -> int:
        """Seal every recorder file of ``worker`` in ``staging`` and publish it; return how many.

        The meta file names ``worker``, the owner from the file name, not
        this worker. A failure raises and leaves the rest in the staging
        folder for a later pass.
        """
        files = 0
        for path in await asyncio.to_thread(own_db_files, staging, worker):
            step.name = 'seal'
            meta = await seal_closed_file(
                path, worker=worker, instance=self._instance, cluster=self._cluster, sealed_at=datetime.now(UTC)
            )
            step.name = 'publish'
            destination, crossed = await publish_in_background(path, meta, self._history_dir())
            files += 1
            await logger.ainfo(
                'recorder_leftover_sealed',
                category='recorder',
                db_path=path,
                destination=destination,
                crossed_filesystems=crossed,
                size_bytes=meta['size_bytes'],
                events=meta['tables'].get('events', 0),
                orphan_worker=worker,
            )
        return files

    async def _log_sealed(self, folder: str, worker: str, files: int, removed: bool) -> None:
        self._reported = {entry for entry in self._reported if entry[0] != folder}
        self._unknown_since.pop(folder, None)
        await logger.ainfo(
            'recorder_orphan_folder_sealed',
            category='recorder',
            folder=folder,
            worker=worker,
            files=files,
            folder_removed=removed,
        )

    async def _name_proved_not_running(self, folder: str, worker: str) -> bool:
        """True when the registry says ``worker`` is not online; otherwise say why, when it is worth a line."""
        if self._worker_online is None:
            await self._report_found(folder, worker, 'no_registry')
            return False
        verdict = self._worker_online(worker)
        if verdict is None:
            since = self._unknown_since.setdefault(folder, self._now_monotonic())
            if self._now_monotonic() - since >= UNKNOWN_VERDICT_REPORT_SECONDS:
                await self._report_found(folder, worker, 'registry_unknown')
            return False
        self._unknown_since.pop(folder, None)
        return verdict is False

    async def _report_found(self, folder: str, worker: str, reason: str) -> None:
        """Log an orphan folder the pass cannot seal, once per folder and reason per process."""
        if (folder, reason) in self._reported:
            return
        self._reported.add((folder, reason))
        await logger.awarning(
            'recorder_orphan_folder_found',
            category='recorder',
            folder=folder,
            worker=worker,
            reason=reason,
            hint=_FOUND_HINTS[reason],
        )

    async def _report_clock_ahead(self, folder: str, worker: str, check: IdleCheck) -> None:
        """Log once per folder that a file's time is ahead of the local clock, so the folder never looks idle."""
        if (folder, 'clock_ahead') in self._reported:
            return
        self._reported.add((folder, 'clock_ahead'))
        await logger.awarning(
            'recorder_orphan_folder_clock_ahead',
            category='recorder',
            folder=folder,
            worker=worker,
            file=check.future_file,
            seconds_ahead=round(check.seconds_ahead, 3),
        )
