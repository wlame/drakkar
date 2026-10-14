"""Sealing: turning a closed recorder file into an immutable file in the shared history.

A live recorder file is opened only by its owner. When the owner is done
with it (rotation, clean stop) it seals the file:

1. switch the file to a rollback journal (``PRAGMA journal_mode=DELETE``)
   on the last open connection, so closing it leaves no ``-wal``/``-shm``;
2. compute the meta file content on that same connection, then close it;
3. publish: write ``<name>.db.meta.json`` into
   ``<shared>/history/<cluster>/``, then move the file next to it. A rename
   that crosses filesystems (``EXDEV``: a per-pod ``storage.dir`` and a
   shared network mount — the usual fleet layout) becomes a copy into a
   dot-prefixed ``.incoming`` file, fsync, rename, fsync of the folder, and
   only then the deletion of the source.

A file that fails any step stays in ``storage.dir``, closed, and is sealed
again later. Steps 1-2 run on the database driver's own thread; step 3 is
blocking file work on a daemon thread (:func:`publish_in_background`).
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import os
import re
import secrets
import shutil
import sqlite3
import threading
from datetime import datetime
from typing import Any

import aiosqlite

from drakkar.dbfiles import DB_DIR_MODE, DB_FILE_MODE
from drakkar.recorder.helpers import BUSY_TIMEOUT_MS
from drakkar.recorder.history import build_meta, meta_path, write_meta

# A file of this worker is a leftover only when nothing touched it (or its
# WAL) for this long: a second live process with the same name may still be
# writing a fresh one, and network mounts round mtimes.
LEFTOVER_FLOOR_SECONDS = 300.0
INCOMING_SUFFIX = '.incoming'
# The journal switch needs the only open connection to the file. A reader in
# this process (a databases-page load, a trace) holds it for a moment, so the
# seal tries again after each of these pauses (seconds) before it gives up.
SEAL_RETRY_DELAYS: tuple[float, ...] = (0.05, 0.1, 0.2, 0.4, 0.8)
_COPY_CHUNK_BYTES = 1024 * 1024
_TEMP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW

# The move's rename, a module attribute so tests can force the
# cross-filesystem branch on one filesystem.
_rename = os.rename


class SealError(RuntimeError):
    """The file could not be switched to a rollback journal (another connection still holds it)."""


async def seal_connection(
    conn: aiosqlite.Connection,
    *,
    db_path: str,
    worker: str,
    instance: str,
    cluster: str,
    sealed_at: datetime,
) -> dict[str, Any]:
    """Switch the file behind ``conn`` to a rollback journal, build its meta, close it.

    ``conn`` must be the last open connection to the file. It is closed in
    every case. Returns the meta object, with ``size_bytes`` measured after
    the close.
    """
    try:
        # The journal mode cannot change inside a transaction.
        await conn.commit()
        await _switch_to_rollback_journal(conn, db_path)
        meta = await build_meta(
            conn, worker=worker, instance=instance, cluster=cluster, sealed_at=sealed_at, size_bytes=0
        )
    finally:
        await conn.close()
    meta['size_bytes'] = os.path.getsize(db_path)
    return meta


async def _switch_to_rollback_journal(conn: aiosqlite.Connection, db_path: str) -> None:
    """``PRAGMA journal_mode=DELETE``, retried while another connection briefly holds the file."""
    # Read at call time, so the schedule can be shortened in tests.
    for delay in (*SEAL_RETRY_DELAYS, None):
        try:
            async with conn.execute('PRAGMA journal_mode=DELETE') as cursor:
                row = await cursor.fetchone()
            mode = str(row[0]).lower() if row else ''
            if mode == 'delete':
                return
            problem = f'journal_mode stayed {mode!r}'
        except sqlite3.OperationalError as exc:
            # "database is locked": another connection is open right now.
            problem = str(exc)
        if delay is None:
            raise SealError(f'{db_path}: {problem}; another connection holds the file')
        await asyncio.sleep(delay)


async def seal_closed_file(
    db_path: str,
    *,
    worker: str,
    instance: str,
    cluster: str,
    sealed_at: datetime,
) -> dict[str, Any]:
    """Seal a recorder file nobody has open (backlog or leftover); returns its meta.

    It is opened with a normal connection, which replays a WAL a crashed
    process left behind — never with ``immutable=1``, which would not see
    those rows. ``mode=rw`` makes a file that disappeared an error instead
    of a new empty database.
    """
    conn = await aiosqlite.connect(f'file:{db_path}?mode=rw', uri=True)
    try:
        await conn.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
    except Exception:
        await conn.close()
        raise
    return await seal_connection(
        conn, db_path=db_path, worker=worker, instance=instance, cluster=cluster, sealed_at=sealed_at
    )


def publish_in_background(db_path: str, meta: dict[str, Any], folder: str) -> asyncio.Future[tuple[str, bool]]:
    """Run :func:`publish_sealed` on a daemon thread; the future settles with its result.

    A daemon thread rather than ``asyncio.to_thread``: the loop's default
    executor is joined at interpreter exit, so a copy to a hung network
    mount would hold the process past its shutdown budget. A daemon thread
    cut off at exit leaves at most a dot-prefixed ``.incoming`` file (the
    archive pass sweeps it) and the source in ``storage.dir`` (sealed again
    at the next start). A caller that stops waiting (a timeout) cancels the
    future; the thread's late result is then dropped.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[tuple[str, bool]] = loop.create_future()

    def set_result(result: tuple[str, bool]) -> None:
        if not future.done():
            future.set_result(result)

    def set_exception(error: BaseException) -> None:
        if not future.done():
            future.set_exception(error)

    def run() -> None:
        # The loop is gone when the process is already shutting down, so
        # handing the outcome back may fail; the thread then just ends.
        try:
            result = publish_sealed(db_path, meta, folder)
        except BaseException as exc:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(set_exception, exc)
            return
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(set_result, result)

    threading.Thread(target=run, name='drakkar-recorder-publish', daemon=True).start()
    return future


def publish_sealed(db_path: str, meta: dict[str, Any], folder: str) -> tuple[str, bool]:
    """Write the meta file into ``folder``, then move the sealed file there.

    Returns ``(destination, crossed_filesystems)``. The meta file goes first:
    a crash between the two leaves a meta file without its database, which
    readers ignore and the archive pass sweeps, while the database stays in
    ``storage.dir`` to be sealed again.
    """
    os.makedirs(folder, mode=DB_DIR_MODE, exist_ok=True)
    destination = os.path.join(folder, os.path.basename(db_path))
    write_meta(meta_path(destination), meta)
    try:
        _rename(db_path, destination)
        return destination, False
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    _copy_across(db_path, destination)
    return destination, True


def _copy_across(source: str, destination: str) -> None:
    """Copy ``source`` to ``destination`` on another filesystem, durably, then delete the source."""
    folder, name = os.path.split(destination)
    temp = os.path.join(folder, f'.{name}.{secrets.token_hex(8)}{INCOMING_SUFFIX}')
    try:
        with open(source, 'rb') as reader:
            fd = os.open(temp, _TEMP_FLAGS, DB_FILE_MODE)
            with os.fdopen(fd, 'wb') as writer:
                shutil.copyfileobj(reader, writer, _COPY_CHUNK_BYTES)
                writer.flush()
                os.fsync(writer.fileno())
        os.replace(temp, destination)
    except BaseException:
        # Nothing half-copied stays visible under a final name; the source
        # is untouched and is sealed again later.
        try:
            os.unlink(temp)
        except OSError:
            pass
        raise
    _fsync_folder(folder)
    os.unlink(source)


def _fsync_folder(folder: str) -> None:
    """Make the rename durable: fsync the folder entry (best effort where folders cannot be opened)."""
    try:
        fd = os.open(folder, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _own_name_pattern(worker: str) -> re.Pattern[str]:
    """Exactly ``<worker>-YYYY-MM-DD__HH_MM_SS.db``: worker ``a`` never matches files of ``a-b``."""
    return re.compile(rf'^{re.escape(worker)}-\d{{4}}-\d{{2}}-\d{{2}}__\d{{2}}_\d{{2}}_\d{{2}}\.db$')


def own_db_files(storage_dir: str, worker: str) -> list[str]:
    """This worker's recorder files in ``storage_dir``, oldest first (names carry the time).

    Paths are normalized (``./data`` lists as ``data/...``), so they compare
    equal to the recorder's own path however the folder was spelled.
    """
    folder = os.path.normpath(storage_dir)
    try:
        names = os.listdir(folder)
    except FileNotFoundError:
        return []
    pattern = _own_name_pattern(worker)
    return sorted(os.path.join(folder, name) for name in names if pattern.match(name))


def _newest_mtime(db_path: str) -> float | None:
    """The newest mtime of the file and its ``-wal``; ``None`` when the file is gone."""
    try:
        newest = os.stat(db_path).st_mtime
    except FileNotFoundError:
        return None
    try:
        newest = max(newest, os.stat(f'{db_path}-wal').st_mtime)
    except FileNotFoundError:
        pass
    return newest


def find_leftovers(
    storage_dir: str,
    worker: str,
    *,
    exclude: str,
    now: float,
    floor_seconds: float = LEFTOVER_FLOOR_SECONDS,
) -> list[str]:
    """This worker's files other than ``exclude`` that nothing touched for ``floor_seconds`` (local clock)."""
    excluded = os.path.normpath(exclude) if exclude else ''
    leftovers: list[str] = []
    for path in own_db_files(storage_dir, worker):
        if path == excluded:
            continue
        newest = _newest_mtime(path)
        if newest is not None and now - newest > floor_seconds:
            leftovers.append(path)
    return leftovers
