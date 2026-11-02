"""The ``parts`` and the ``worker``, ``recorder`` and ``storage`` blocks of ``GET /api/v1/debug/databases``.

``worker`` and ``recorder`` come from the config and from values the recorder
keeps in memory; they read no file and no database. ``storage`` lists the
worker's own folder ``<storage.dir>/<sanitized cluster>/<worker>/`` and the
folders of the other workers of its cluster on this host, so it is blocking:
call :func:`storage_block` in a thread. No block carries an absolute path or a
secret (a DSN or a token): the debug UI is open by design.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from drakkar.cache.engine import CACHE_FILE_SUFFIX
from drakkar.cluster_tasks import archive_next_slot, archive_scope
from drakkar.config.recorder import PERSISTENCE_POSTGRES, PERSISTENCE_SQLITE
from drakkar.ownerlock import OWNER_LOCK_NAME, download_temp_pattern, owner_lock_held
from drakkar.recorder.orphans import sibling_folders
from drakkar.schedule import sanitize_cluster
from drakkar.timefmt import format_epoch_micro, format_epoch_ns
from drakkar.watchdog import WATCHDOG_SUFFIX

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig
    from drakkar.recorder import EventRecorder
    from drakkar.recorder.postgres import RecorderPostgresSettings
    from drakkar.recorder.postgres_writer import PostgresStageStatus

PART_OWN = 'own'
PART_HISTORY = 'history'
PART_STORAGE = 'storage'
PART_RECORDER = 'recorder'
DATABASES_PARTS = frozenset((PART_OWN, PART_HISTORY, PART_STORAGE, PART_RECORDER))
UNKNOWN_PART_ERROR = "unknown part '{part}'; known: " + ', '.join(sorted(DATABASES_PARTS))

FILE_KIND_CACHE = 'cache'
FILE_KIND_WATCHDOG = 'watchdog'
FILE_KIND_OWNER_LOCK = 'owner_lock'
# The SQLite files beside a WAL database, counted in the cache file's size.
_CACHE_SIDECARS = ('-wal', '-shm')
# The longest the cache row count may take; a count that is not done by then answers no number.
CACHE_COUNT_LIMIT_SECONDS = 1.0
# The progress handler of the cache count runs every this many SQLite steps.
_CACHE_COUNT_CHECK_STEPS = 10_000
# COUNT of a NOT NULL column, not COUNT(*): SQLite answers COUNT(*) with one
# opcode that walks the whole table, which no progress handler can stop.
_CACHE_COUNT_SQL = 'SELECT COUNT(scope) FROM cache_entries'

SECONDS_PER_HOUR = 3600

WorkerOnline = Callable[[str], bool | None]


def parse_parts(text: str | None) -> frozenset[str]:
    """The parts asked for: every part when ``text`` is ``None``, else the items of the comma list.

    Raises:
        ValueError: an item is not a known part (an empty item is one); the
            message names the first such item.
    """
    if text is None:
        return DATABASES_PARTS
    items = text.split(',')
    for item in items:
        if item not in DATABASES_PARTS:
            raise ValueError(UNKNOWN_PART_ERROR.format(part=item))
    return frozenset(items)


def worker_block(recorder: EventRecorder) -> dict[str, Any]:
    """The ``worker`` block: the serving worker's name, its cluster (``None`` when empty) and its instance."""
    return {
        'name': recorder.worker_name,
        'cluster': recorder.cluster_name or None,
        'instance': recorder.instance,
    }


def recorder_block(config: DrakkarConfig, recorder: EventRecorder, *, now: float) -> dict[str, Any]:
    """The ``recorder`` block: the effective provider and what it runs; ``now`` (epoch) sets the next archive slot."""
    memory = config.recorder.memory
    return {
        'enabled': config.recorder.enabled,
        'provider': recorder.persistence,
        'memory': (
            {
                'window_seconds': int(memory.window),
                'max_rows': config.memory_max_rows,
                'max_bytes': memory.max_size,
            }
            if _memory_store_open(recorder)
            else None
        ),
        'sqlite': _sqlite_block(config, now) if recorder.persistence == PERSISTENCE_SQLITE else None,
        'postgres': _postgres_block(recorder) if recorder.persistence == PERSISTENCE_POSTGRES else None,
    }


def _memory_store_open(recorder: EventRecorder) -> bool:
    """Whether the recorder holds an open memory store; a store closed by a stop that races the read counts as none."""
    store = recorder.memory_store
    return store is not None and store.is_open


def _sqlite_block(config: DrakkarConfig, now: float) -> dict[str, Any]:
    """The rotation and the archive schedule of provider ``sqlite``."""
    archive = config.recorder.archive
    return {
        'rotation_interval_seconds': config.recorder.sqlite.rotation_interval * SECONDS_PER_HOUR,
        'archive': {
            'enabled': archive.enabled,
            'scope': archive_scope(config),
            'next_slot_ts': archive_next_slot(config, now),
            'window_seconds': archive.window * SECONDS_PER_HOUR,
            'due_after_seconds': archive.due_after * SECONDS_PER_HOUR,
            'retention_days': archive.retention,
            'merged_retention_days': archive.merged_retention,
        },
    }


def _postgres_block(recorder: EventRecorder) -> dict[str, Any] | None:
    """The table settings of provider ``postgres`` and this worker's writer. The DSN stays out."""
    settings: RecorderPostgresSettings | None = recorder.postgres_settings
    if settings is None:
        return None
    stage = recorder.postgres_stage_status()
    return {
        'connection': settings.connection_name,
        'schema': settings.schema,
        'table': settings.table,
        'initial_retention_hours': settings.retention_hours,
        'max_size_bytes': settings.max_size,
        'store_completed_output': settings.store_completed_output,
        'writer': _writer_block(stage) if stage is not None else None,
    }


def _writer_block(stage: PostgresStageStatus) -> dict[str, Any]:
    """The writer part of the ``postgres`` block, from one stage status read."""
    writer = stage.writer
    upkeep = writer.last_upkeep
    return {
        'started': writer.started,
        'queue_rows': stage.queue_rows + writer.slot_rows,
        'queue_bytes': stage.queue_bytes + writer.slot_bytes,
        'rows_written': writer.rows_written,
        'dropped': {
            'queue_full': stage.dropped_queue_full,
            'rejected': writer.dropped_rejected,
            'no_partition': writer.dropped_no_partition,
            'shutdown': writer.dropped_shutdown,
        },
        'breaker_open': writer.breaker_open,
        'last_upkeep': (
            {
                'at': format_epoch_micro(upkeep.at),
                'held_lock': upkeep.held_lock,
                'created': list(upkeep.created),
                'dropped': list(upkeep.dropped),
                'dropped_for_size': list(upkeep.dropped_for_size),
            }
            if upkeep is not None
            else None
        ),
    }


# --- storage (blocking) ---


@dataclass(frozen=True)
class _FolderFile:
    """One regular file of a folder: its name, size and modification time in nanoseconds."""

    name: str
    size: int
    mtime_ns: int


def _regular_files(folder: str) -> list[_FolderFile]:
    """The regular files directly in ``folder`` (links and folders left out); a file gone mid-listing is skipped.

    Raises :class:`OSError` when the folder itself cannot be listed.
    """
    files = []
    with os.scandir(folder) as entries:
        for entry in entries:
            try:
                if not entry.is_file(follow_symlinks=False):
                    continue
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            files.append(_FolderFile(entry.name, info.st_size, info.st_mtime_ns))
    return files


def storage_block(
    config: DrakkarConfig, recorder: EventRecorder, *, worker_online: WorkerOnline | None
) -> dict[str, Any]:
    """The ``storage`` block. Blocking: it lists the own folder and its siblings and counts the cache rows.

    ``worker_online`` is the registry's verdict for a worker name; ``None``
    when no registry runs, and every sibling's ``online`` is then ``None``.
    """
    storage = config.storage
    own_dir = recorder.storage_dir
    worker = recorder.worker_name
    files, temp_files = _own_files(own_dir, worker) if own_dir else ([], {'count': 0, 'size_bytes': 0})
    return {
        'dir_set': bool(storage.dir),
        'own_folder': f'{sanitize_cluster(recorder.cluster_name)}/{worker}' if own_dir else '',
        'shared_dir_set': bool(storage.shared_dir),
        'shared_is_storage_dir': bool(storage.dir)
        and os.path.normpath(config.resolved_shared_dir) == os.path.normpath(storage.dir),
        'files': files,
        'temp_files': temp_files,
        'siblings': _siblings(own_dir, worker, worker_online) if own_dir else None,
    }


def _own_files(own_dir: str, worker: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """The cache, crash-marker and owner-lock rows of the own folder, and the count and size of its download temps.

    A missing or unreadable own folder has none.
    """
    try:
        listed = {file.name: file for file in _regular_files(own_dir)}
    except OSError:
        return [], {'count': 0, 'size_bytes': 0}
    rows: list[dict[str, Any]] = []
    cache_name = f'{worker}{CACHE_FILE_SUFFIX}'
    if cache_name in listed:
        parts = [listed[cache_name], *(listed[n] for s in _CACHE_SIDECARS if (n := cache_name + s) in listed)]
        rows.append(
            _file_row(
                cache_name,
                FILE_KIND_CACHE,
                size=sum(part.size for part in parts),
                mtime_ns=max(part.mtime_ns for part in parts),
                entries=count_cache_entries(os.path.join(own_dir, cache_name)),
            )
        )
    for name, kind in ((f'{worker}{WATCHDOG_SUFFIX}', FILE_KIND_WATCHDOG), (OWNER_LOCK_NAME, FILE_KIND_OWNER_LOCK)):
        if name in listed:
            rows.append(_file_row(name, kind, size=listed[name].size, mtime_ns=listed[name].mtime_ns, entries=None))
    rows.sort(key=lambda row: row['name'])
    temps = [file for file in listed.values() if download_temp_pattern(worker).fullmatch(file.name)]
    return rows, {'count': len(temps), 'size_bytes': sum(file.size for file in temps)}


def _file_row(name: str, kind: str, *, size: int, mtime_ns: int, entries: int | None) -> dict[str, Any]:
    return {
        'name': name,
        'kind': kind,
        'size_bytes': size,
        'modified_at': format_epoch_ns(mtime_ns),
        'entries': entries,
    }


def count_cache_entries(path: str, limit_seconds: float = CACHE_COUNT_LIMIT_SECONDS) -> int | None:
    """The number of rows of the cache file at ``path``; ``None`` when the count fails or takes over ``limit_seconds``.

    Read-only, on a connection of its own; the cache engine's connections
    are never used. Blocking.
    """
    deadline = time.monotonic() + limit_seconds
    try:
        conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=limit_seconds)
    except sqlite3.Error:
        return None
    with contextlib.closing(conn):
        # A non-zero answer of the handler interrupts the statement.
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), _CACHE_COUNT_CHECK_STEPS)
        try:
            (count,) = conn.execute(_CACHE_COUNT_SQL).fetchone()
        except sqlite3.Error:
            return None
    return count


def _siblings(own_dir: str, worker: str, worker_online: WorkerOnline | None) -> list[dict[str, Any]] | None:
    """The other worker folders of the cluster folder, sorted by worker; ``None`` when it cannot be listed."""
    try:
        folders = sibling_folders(os.path.dirname(own_dir), worker)
    except OSError:
        return None
    rows = []
    for path, name in folders:
        row = _sibling_row(path, name, worker_online)
        if row is not None:
            rows.append(row)
    return rows


def _sibling_row(path: str, worker: str, worker_online: WorkerOnline | None) -> dict[str, Any] | None:
    """One sibling folder's row; ``None`` when the folder went away or cannot be listed."""
    try:
        files = [file for file in _regular_files(path) if file.name != OWNER_LOCK_NAME]
    except OSError:
        return None
    return {
        'worker': worker,
        'files': len(files),
        'size_bytes': sum(file.size for file in files),
        'newest_modified_at': _newest(file.mtime_ns for file in files),
        'in_use': owner_lock_held(path),
        'online': worker_online(worker) if worker_online is not None else None,
    }


def _newest(mtimes_ns: Iterable[int]) -> str | None:
    newest = max(mtimes_ns, default=None)
    return format_epoch_ns(newest) if newest is not None else None
