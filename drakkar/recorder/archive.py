"""Automatic archiving of rotated-out recorder databases.

Rotation leaves a growing pile of ``<worker>-YYYY-MM-DD__HH_MM_SS.db``
files in ``db_dir``. This module folds each finished time window into one
compressed file — ``<cluster>-<from>__<to>.db.gz`` — and deletes the raw
files it merged. Since pruning was removed from rotation, a successful
archive pass (or the operator) is the ONLY thing that removes raw files.

Three ideas carry the design:

* **Windows, not files.** Windows are ``[k*W, (k+1)*W)`` aligned to the
  Unix epoch in UTC, so with the default 24h width they are UTC calendar
  days. A file belongs to the window holding its START timestamp and is
  never split, which is why an archive can carry events slightly past its
  own end — archives partition by file start time, not by event time.
* **One archiver per cluster.** Workers can share a ``db_dir``, so the
  pass groups candidates by the cluster recorded in each file and touches
  only its own group. A lock file elects one worker per cluster per tick;
  the others skip and lose nothing.
* **Sources die last.** Merge, compress, fsync and rename all happen on
  temporary names. Raw files are deleted only after the final archive is
  in place, so any failure costs one tick and no data.

Everything here is synchronous and does real file, SQLite and gzip work.
Callers on the event loop MUST wrap :func:`run_archive_pass` in
``asyncio.to_thread`` — see ``EventRecorder._archive_pass``.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import os
import re
import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from drakkar.dbfiles import secure_db_file
from drakkar.merge import merge_databases

if TYPE_CHECKING:
    from drakkar.config import UIRecorderConfig

logger = structlog.get_logger()

# Cluster used for files whose cluster name is empty, missing or
# unreadable. Also the archive-name prefix those files end up under.
DEFAULT_CLUSTER = 'default'

# A lock older than this is assumed to belong to a worker that died mid
# pass, and is taken over. Two hours is far longer than any archive run
# and far shorter than a shift, so a crashed worker never blocks
# archiving for a day.
LOCK_STALE_SECONDS = 7200

ARCHIVE_SUFFIX = '.db.gz'

# Minute precision in archive names: window bounds always land on a
# multiple of the window width, so seconds carry no information.
ARCHIVE_TS_FORMAT = '%Y-%m-%d_%H-%M'

# SQLite sidecars removed alongside every raw file we merge. Left behind,
# they would accumulate forever — nothing else in db_dir ever cleans them.
_SIDECAR_SUFFIXES = ('-wal', '-shm')

# Trailing timestamp of a raw recorder file. Anchored at the END so worker
# names containing dashes ('search-worker-7') parse correctly.
_DB_NAME_RE = re.compile(r'-(\d{4})-(\d{2})-(\d{2})__(\d{2})_(\d{2})_(\d{2})\.db$')

_UNSAFE_CLUSTER_CHARS = re.compile(r'[^a-zA-Z0-9_-]')

# Copy buffer for the gzip stream. A merged window can be hundreds of MB,
# so the file is streamed rather than read into memory.
_COPY_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class ArchiveCandidate:
    """One raw recorder file considered for archiving."""

    path: str
    start_ts: float
    window_start: float


@dataclass(frozen=True)
class ArchiveWindow:
    """A due time window ``[start, end)`` and the files assigned to it."""

    start: float
    end: float
    files: list[ArchiveCandidate]


def parse_db_start_ts(path: str) -> float:
    """Return the Unix time at which a raw recorder file was opened.

    The name carries it (``<worker>-YYYY-MM-DD__HH_MM_SS.db``, written in
    UTC by ``make_db_path``) so no file has to be opened. Hand-made or
    renamed files fall back to the modification time, and a file that
    disappeared underneath us reports 0.0 — it will simply not survive the
    next stat in :func:`assign_windows`.
    """
    match = _DB_NAME_RE.search(os.path.basename(path))
    if match:
        year, month, day, hour, minute, second = (int(part) for part in match.groups())
        with contextlib.suppress(ValueError):
            return datetime(year, month, day, hour, minute, second, tzinfo=UTC).timestamp()
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def sanitize_cluster(name: str | None) -> str:
    """Return a cluster name safe to embed in a file name.

    Everything outside ``[a-zA-Z0-9_-]`` becomes ``_``; an empty or
    missing name becomes :data:`DEFAULT_CLUSTER`.
    """
    if not name:
        return DEFAULT_CLUSTER
    return _UNSAFE_CLUSTER_CHARS.sub('_', name)


def archive_file_name(cluster: str, start: float, end: float) -> str:
    """Return the archive file name for one window of one cluster."""
    return f'{sanitize_cluster(cluster)}-{_format_bound(start)}__{_format_bound(end)}{ARCHIVE_SUFFIX}'


def _format_bound(ts: float) -> str:
    """Render one window bound the way archive names spell it (UTC)."""
    return datetime.fromtimestamp(ts, tz=UTC).strftime(ARCHIVE_TS_FORMAT)


def assign_windows(
    paths: list[str],
    window_seconds: float,
    now: float,
    rotation_seconds: float,
) -> list[ArchiveWindow]:
    """Group ``paths`` into epoch-aligned windows, returning only due ones.

    A window ``[start, end)`` is DUE when both hold:

    * ``now >= end + window_seconds`` — a full extra window has passed, so
      no worker can still be opening files that belong to it.
    * every assigned file was last written before ``now - rotation_seconds``
      — belt-and-braces against a stalled writer that still holds an old
      file open. A file we cannot stat vetoes its window for this tick.

    Windows come back oldest first, with each window's files sorted by
    path so the merge order is deterministic.
    """
    grouped: dict[float, list[ArchiveCandidate]] = {}
    for path in paths:
        start_ts = parse_db_start_ts(path)
        window_start = (start_ts // window_seconds) * window_seconds
        grouped.setdefault(window_start, []).append(
            ArchiveCandidate(path=path, start_ts=start_ts, window_start=window_start)
        )

    due: list[ArchiveWindow] = []
    mtime_cutoff = now - rotation_seconds
    for window_start, candidates in sorted(grouped.items()):
        window_end = window_start + window_seconds
        if now < window_end + window_seconds:
            continue
        if not all(_is_settled(candidate.path, mtime_cutoff) for candidate in candidates):
            continue
        due.append(
            ArchiveWindow(
                start=window_start,
                end=window_end,
                files=sorted(candidates, key=lambda candidate: candidate.path),
            )
        )
    return due


def _is_settled(path: str, mtime_cutoff: float) -> bool:
    """Whether ``path`` stopped being written before ``mtime_cutoff``."""
    try:
        return os.path.getmtime(path) < mtime_cutoff
    except OSError:
        return False


def run_archive_pass(
    db_dir: str,
    worker_name: str,
    cluster: str,
    cfg: UIRecorderConfig,
    exclude_path: str = '',
) -> None:
    """Archive every due window of this worker's cluster, then expire.

    Blocking by nature (stat, SQLite merge, gzip) — call it through
    ``asyncio.to_thread``, never on the event loop.

    ``exclude_path`` is the live database. It is dropped before window
    assignment, so the file the recorder is writing right now can never be
    merged away underneath it.

    The pass is a no-op unless it finds a due window: nothing is created,
    no lock is taken, and expiry does not run.
    """
    own_cluster = sanitize_cluster(cluster)
    window_seconds = cfg.archive_window_hours * 3600
    rotation_seconds = cfg.rotation_interval_hours * 3600
    now = time.time()

    candidates = [path for path in _list_raw_dbs(db_dir, exclude_path) if _read_cluster(path) == own_cluster]
    windows = assign_windows(candidates, window_seconds, now, rotation_seconds)
    if not windows:
        return

    lock_path = os.path.join(db_dir, f'.archive-{own_cluster}.lock')
    if not _acquire_lock(lock_path, worker_name, now):
        return
    try:
        for window in windows:
            _archive_window(db_dir, own_cluster, window)
        if cfg.archive_retention_days > 0:
            _expire_archives(db_dir, own_cluster, now, cfg.archive_retention_days)
    finally:
        # The lock is released on both paths: a failed window logged
        # itself and the next tick retries, and holding the lock after a
        # crash-free failure would just idle the whole cluster.
        with contextlib.suppress(OSError):
            os.unlink(lock_path)


def _list_raw_dbs(db_dir: str, exclude_path: str) -> list[str]:
    """Return the raw recorder files in ``db_dir`` worth considering.

    Skips the live symlink and any other link, the caller's live database,
    and dot-prefixed names — the latter are the intermediate merge files a
    pass writes, and in a shared ``db_dir`` one worker must not merge away
    another worker's work in progress.
    """
    live = os.path.abspath(exclude_path) if exclude_path else ''
    paths: list[str] = []
    for path in Path(db_dir).glob('*.db'):
        if path.name.startswith('.'):
            continue
        if path.is_symlink() or not path.is_file():
            continue
        if live and os.path.abspath(str(path)) == live:
            continue
        paths.append(str(path))
    return paths


def _read_cluster(path: str) -> str:
    """Return the sanitized cluster a raw file belongs to.

    Read-only URI connection, one small SELECT. A file with no
    ``worker_config`` table, an unreadable file, or one recorded without a
    cluster name groups under :data:`DEFAULT_CLUSTER`.
    """
    try:
        conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    except sqlite3.Error:
        return DEFAULT_CLUSTER
    try:
        row = conn.execute('SELECT cluster_name FROM worker_config WHERE id = 1').fetchone()
    except sqlite3.Error:
        return DEFAULT_CLUSTER
    finally:
        conn.close()
    return sanitize_cluster(row[0] if row else None)


def _acquire_lock(lock_path: str, worker_name: str, now: float) -> bool:
    """Try to become this cluster's archiver for this tick.

    ``O_CREAT|O_EXCL`` is the election: exactly one worker can create the
    file. A lock younger than :data:`LOCK_STALE_SECONDS` means somebody
    else is archiving right now, so we skip silently; an older one is
    assumed abandoned and is taken over with a single unlink-and-retry.
    """
    payload = json.dumps({'pid': os.getpid(), 'worker': worker_name, 'ts': now}).encode()
    for attempt in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if attempt:
                # Lost the race to another worker taking over the same
                # stale lock — theirs is fresh, so this tick is theirs.
                return False
            try:
                age = now - os.path.getmtime(lock_path)
            except OSError:
                return False
            if age < LOCK_STALE_SECONDS:
                return False
            logger.warning(
                'recorder_archive_lock_stale',
                category='recorder',
                lock=lock_path,
                age_seconds=round(age, 1),
            )
            try:
                os.unlink(lock_path)
            except OSError:
                return False
            continue
        except OSError:
            return False
        try:
            with os.fdopen(fd, 'wb') as handle:
                handle.write(payload)
        except OSError:
            with contextlib.suppress(OSError):
                os.unlink(lock_path)
            return False
        return True
    return False


def _archive_window(db_dir: str, cluster: str, window: ArchiveWindow) -> None:
    """Merge, compress and publish one window, then delete its sources.

    Order is what makes this safe: everything lands on temporary names,
    the rename publishes the archive atomically, and only then are the raw
    files removed. Any failure before the rename leaves the sources
    untouched for the next tick.
    """
    final_name = archive_file_name(cluster, window.start, window.end)
    final_path = os.path.join(db_dir, final_name)
    tmp_path = os.path.join(db_dir, f'.{final_name}.tmp')
    merged_path = os.path.join(db_dir, f'.{final_name}.merge.db')
    sources = [candidate.path for candidate in window.files]
    raw_bytes = sum(_file_size(path) for path in sources)

    try:
        # The merged DB and the archive hold the same task args and
        # subprocess output the raw files do, so both are created
        # owner-only before anything writes to them. The final archive
        # inherits the temp file's mode through the rename.
        secure_db_file(merged_path)
        merge_databases(sources, merged_path)
        secure_db_file(tmp_path)
        _compress(merged_path, tmp_path)
        os.rename(tmp_path, final_path)
    except Exception as exc:
        _remove_with_sidecars(merged_path)
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        logger.error(
            'recorder_archive_failed',
            category='recorder',
            cluster=cluster,
            window_start=window.start,
            window_end=window.end,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return

    for path in sources:
        _remove_with_sidecars(path)
    _remove_with_sidecars(merged_path)
    logger.info(
        'recorder_archive_created',
        category='recorder',
        cluster=cluster,
        window_start=window.start,
        window_end=window.end,
        name=final_name,
        file_count=len(sources),
        raw_bytes=raw_bytes,
        compressed_bytes=_file_size(final_path),
    )


def _compress(source_path: str, dest_path: str) -> None:
    """Gzip ``source_path`` into ``dest_path`` and flush it to disk.

    The gzip stream wraps a file object we own so the data can be fsynced
    through its descriptor — without that, the rename below could publish
    a name whose contents are still only in the page cache. Copying in
    chunks keeps a multi-hundred-MB merge off the heap.
    """
    with open(dest_path, 'wb') as raw:
        with gzip.open(raw, 'wb') as compressed, open(source_path, 'rb') as source:
            shutil.copyfileobj(source, compressed, _COPY_CHUNK_BYTES)
        raw.flush()
        os.fsync(raw.fileno())


def _expire_archives(db_dir: str, cluster: str, now: float, retention_days: int) -> None:
    """Delete this cluster's archives whose window ended too long ago.

    The end timestamp comes from the file name, so no archive is opened.
    Other clusters' archives in a shared ``db_dir`` belong to their own
    workers and are never touched.
    """
    cutoff = now - retention_days * 86400
    pattern = re.compile(
        rf'^{re.escape(cluster)}-\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}__'
        rf'(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}})\{ARCHIVE_SUFFIX}$'
    )
    for path in Path(db_dir).glob(f'*{ARCHIVE_SUFFIX}'):
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            end_ts = datetime.strptime(match.group(1), ARCHIVE_TS_FORMAT).replace(tzinfo=UTC).timestamp()
        except ValueError:
            continue
        if end_ts >= cutoff:
            continue
        try:
            os.unlink(path)
        except OSError as exc:
            logger.warning(
                'recorder_archive_expire_failed',
                category='recorder',
                cluster=cluster,
                name=path.name,
                error=str(exc),
            )
            continue
        logger.info(
            'recorder_archive_expired',
            category='recorder',
            cluster=cluster,
            name=path.name,
            window_end=end_ts,
            retention_days=retention_days,
        )


def _file_size(path: str) -> int:
    """Size of ``path`` in bytes, 0 when it cannot be stat-ed."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _remove_with_sidecars(path: str) -> None:
    """Delete a SQLite file together with its ``-wal``/``-shm`` sidecars."""
    for target in (path, *(path + suffix for suffix in _SIDECAR_SUFFIXES)):
        with contextlib.suppress(OSError):
            os.unlink(target)
