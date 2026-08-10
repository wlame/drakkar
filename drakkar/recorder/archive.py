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
import fcntl
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

# Age past which a leftover file from a dead pass — an orphaned lock file,
# an abandoned temporary — is treated as garbage and removed. Two hours is
# far longer than any archive run and far shorter than a shift.
LOCK_STALE_SECONDS = 7200

ARCHIVE_SUFFIX = '.db.gz'

# Raw file the merge engine could not read. Renaming it takes it out of
# the candidate set — so it cannot stall its window forever — while
# keeping the data on disk for an operator to look at.
UNREADABLE_SUFFIX = '.unreadable'

# Temporaries, all dot-prefixed and pid-stamped so two workers racing on
# the same window can never write to each other's files.
_MERGE_TEMP_SUFFIX = '.merge.db'
_COMPRESS_TEMP_SUFFIX = '.tmp'
_PREVIOUS_TEMP_SUFFIX = '.previous.db'
_TEMP_SUFFIXES = (_MERGE_TEMP_SUFFIX, _COMPRESS_TEMP_SUFFIX, _PREVIOUS_TEMP_SUFFIX)

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
    files: tuple[ArchiveCandidate, ...]


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


def _parse_bound(text: str) -> float:
    """Inverse of :func:`_format_bound`: one window bound back to Unix time."""
    return datetime.strptime(text, ARCHIVE_TS_FORMAT).replace(tzinfo=UTC).timestamp()


# Matches exactly what :func:`archive_file_name` writes. Anchored at both
# ends so a dot-prefixed temporary (in-flight compress/merge work) or a
# ``*.unreadable`` quarantine file can never match — the UI archive-list and
# archive-download routes reuse this pattern to identify and parse archives
# by name alone, without opening the file.
ARCHIVE_NAME_RE = re.compile(
    r'^(?P<cluster>[a-zA-Z0-9_-]+)-'
    r'(?P<from_ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2})__'
    r'(?P<to_ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2})'
    r'\.db\.gz$'
)


@dataclass(frozen=True)
class ArchiveInfo:
    """One archive file's identity and size, parsed entirely from its name."""

    name: str
    cluster: str
    from_ts: float
    to_ts: float
    size_bytes: int


def list_archives(db_dir: str) -> list[ArchiveInfo]:
    """Return every archive file in ``db_dir``, newest-first by ``to_ts``.

    Name-only, like :func:`parse_db_start_ts`: cluster and window bounds
    come from the file name :func:`archive_file_name` wrote, never from
    opening the file, so a directory with many archives costs one
    ``listdir`` and one ``stat`` per match — cheap enough to call from a
    request handler. :data:`ARCHIVE_NAME_RE` is anchored against the whole
    name, so dot-prefixed temporaries, ``*.unreadable`` quarantine files
    and raw ``*.db`` files never match.
    """
    results: list[ArchiveInfo] = []
    if not db_dir or not os.path.isdir(db_dir):
        return results
    for entry in os.listdir(db_dir):
        match = ARCHIVE_NAME_RE.fullmatch(entry)
        if not match:
            continue
        try:
            size_bytes = os.stat(os.path.join(db_dir, entry)).st_size
        except OSError:
            # Raced with a delete/rename between listdir and stat — drop it
            # rather than fail the whole listing.
            continue
        try:
            # The name regex only checks digit counts, not calendar
            # validity — a hand-crafted "…-2026-13-08_00-00__….db.gz"
            # matches it but is not a date. Skip such a file instead of
            # letting one stray name fail the whole listing.
            from_ts = _parse_bound(match['from_ts'])
            to_ts = _parse_bound(match['to_ts'])
        except ValueError:
            continue
        results.append(
            ArchiveInfo(
                name=entry,
                cluster=match['cluster'],
                from_ts=from_ts,
                to_ts=to_ts,
                size_bytes=size_bytes,
            )
        )
    results.sort(key=lambda info: info.to_ts, reverse=True)
    return results


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
                files=tuple(sorted(candidates, key=lambda candidate: candidate.path)),
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
    lock_fd = _acquire_lock(lock_path, worker_name, now)
    if lock_fd is None:
        return
    try:
        # Elected: no other pass of this cluster is running, so any
        # leftover temporary here belongs to a dead one.
        _sweep_stale_temps(db_dir, own_cluster, now)
        for window in windows:
            _archive_window(db_dir, own_cluster, window)
        if cfg.archive_retention_days > 0:
            _expire_archives(db_dir, own_cluster, now, cfg.archive_retention_days)
    finally:
        # Released on both paths: a failed window logged itself and the
        # next tick retries, and holding the lock after a crash-free
        # failure would just idle the whole cluster.
        _release_lock(lock_fd, lock_path)


def _list_raw_dbs(db_dir: str, exclude_path: str) -> list[str]:
    """Return the raw recorder files in ``db_dir`` worth considering.

    Skips the live symlink and any other link, the caller's live database,
    and dot-prefixed names — the latter are the intermediate merge files a
    pass writes, and in a shared ``db_dir`` one worker must not merge away
    another worker's work in progress.

    The live database is compared through ``realpath`` on both sides, so a
    symlinked or relative ``db_dir`` cannot make the running worker's own
    file look like a different one.
    """
    live = os.path.realpath(exclude_path) if exclude_path else ''
    paths: list[str] = []
    for path in Path(db_dir).glob('*.db'):
        if path.name.startswith('.'):
            continue
        if path.is_symlink() or not path.is_file():
            continue
        if live and os.path.realpath(str(path)) == live:
            continue
        paths.append(str(path))
    return paths


def _sweep_stale_temps(db_dir: str, cluster: str, now: float) -> None:
    """Delete this cluster's abandoned merge/compress temporaries.

    Only reachable while holding the lock, so nothing being written right
    now can be swept — and the age floor keeps a long merge started by a
    worker that is somehow still alive out of reach as well.
    """
    cutoff = now - LOCK_STALE_SECONDS
    for path in Path(db_dir).glob(f'.{cluster}-*'):
        if not path.name.endswith(_TEMP_SUFFIXES):
            continue
        try:
            if path.stat().st_mtime >= cutoff:
                continue
        except OSError:
            continue
        _remove_with_sidecars(str(path))
        logger.warning(
            'recorder_archive_temp_swept',
            category='recorder',
            cluster=cluster,
            name=path.name,
        )


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


def _acquire_lock(lock_path: str, worker_name: str, now: float) -> int | None:
    """Try to become this cluster's archiver, returning the held lock fd.

    Ownership is decided by ``flock``, not by the file's existence: the
    kernel drops the lock when the holder dies, so a crashed worker never
    leaves a lock that outlives it. The file itself is informational — the
    JSON payload names the pid and worker for an operator reading the
    directory — and a leftover unlocked file is simply re-locked and
    rewritten by the next elected worker.

    The inode check closes the classic unlink race: if the previous holder
    unlinked the file between our ``open`` and our ``flock``, we now hold a
    lock on a detached inode that grants nothing, so we stand down and let
    the next tick retry against the new file.

    Returns the file descriptor to pass to :func:`_release_lock`, or
    ``None`` when another worker owns the lock.
    """
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
    except OSError:
        return None
    try:
        # LOCK_NB: losing the election must cost a syscall, not a wait —
        # this runs on a worker thread every rotation tick.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if os.stat(lock_path).st_ino != os.fstat(fd).st_ino:
            os.close(fd)
            return None
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({'pid': os.getpid(), 'worker': worker_name, 'ts': now}).encode())
    except OSError:
        os.close(fd)
        return None
    return fd


def _release_lock(fd: int, lock_path: str) -> None:
    """Unlink the lock file and drop the lock by closing its descriptor."""
    with contextlib.suppress(OSError):
        os.unlink(lock_path)
    with contextlib.suppress(OSError):
        os.close(fd)


def _archive_window(db_dir: str, cluster: str, window: ArchiveWindow) -> None:
    """Merge, compress and publish one window, then delete its sources.

    Order is what makes this safe: everything lands on temporary names
    carrying our pid, the rename publishes the archive atomically, and only
    then are the raw files removed. Any failure before the rename leaves
    every source untouched for the next tick.

    Two rules protect data that a naive "merge everything, delete
    everything" would lose:

    * Only the sources the merge engine actually read are deleted. One it
      could not read is moved aside as ``<name>.unreadable`` — off the
      candidate list so it cannot stall the window forever, but still on
      disk for an operator.
    * An archive already at the final name is folded back in rather than
      overwritten. It happens when an earlier pass died between publishing
      the archive and finishing the deletion of its sources; the surviving
      sources are already inside it, so the file must never be replaced by
      a merge of what is left. Those sources are deleted rather than
      merged again — their events would otherwise land in the archive
      twice.
    """
    final_name = archive_file_name(cluster, window.start, window.end)
    final_path = os.path.join(db_dir, final_name)
    prefix = os.path.join(db_dir, f'.{final_name}.{os.getpid()}')
    tmp_path = prefix + _COMPRESS_TEMP_SUFFIX
    merged_path = prefix + _MERGE_TEMP_SUFFIX
    previous_path = prefix + _PREVIOUS_TEMP_SUFFIX
    sources = [candidate.path for candidate in window.files]

    # Sources the published archive already contains. They skip the merge
    # (re-merging them would duplicate their events) but are still deleted
    # at the end — their data is provably inside the archive.
    already_covered: list[str] = []

    try:
        merge_sources = list(sources)
        if os.path.exists(final_path):
            _decompress(final_path, previous_path)
            already_archived = _archived_source_files(previous_path)
            already_covered = [path for path in sources if os.path.basename(path) in already_archived]
            merge_sources = [path for path in sources if path not in already_covered]
            if not merge_sources:
                # The whole window is already inside the published archive.
                # Nothing to rewrite — just finish the interrupted cleanup.
                _remove_with_sidecars(previous_path)
                _delete_sources(db_dir, sources)
                logger.info(
                    'recorder_archive_sources_reclaimed',
                    category='recorder',
                    cluster=cluster,
                    window_start=window.start,
                    window_end=window.end,
                    name=final_name,
                    file_count=len(sources),
                )
                return
            merge_sources.append(previous_path)

        raw_bytes = sum(_file_size(path) for path in merge_sources)
        result = merge_databases(merge_sources, merged_path)
        merged = set(result.merged_files)
        if previous_path in merge_sources and previous_path not in merged:
            # The published archive could not be carried over. Abort rather
            # than replace a complete archive with a partial one.
            raise OSError(f'could not fold the existing archive {final_name} into the new one')
        # ``merge_databases`` deletes and recreates its output, so SQLite
        # owns the mode by the time it returns — tighten it now, before the
        # data is compressed. The archive holds the same task args and
        # subprocess output as the raw files, and the final name inherits
        # the compressed temp's mode through the rename.
        secure_db_file(merged_path)
        secure_db_file(tmp_path)
        _compress(merged_path, tmp_path)
        os.rename(tmp_path, final_path)
    except Exception as exc:
        _remove_with_sidecars(merged_path)
        _remove_with_sidecars(previous_path)
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

    _remove_with_sidecars(merged_path)
    _remove_with_sidecars(previous_path)
    covered = merged.union(already_covered)
    deleted = _delete_sources(db_dir, [path for path in sources if path in covered])
    for path in sources:
        if path in covered:
            continue
        _set_aside(path, cluster)
    logger.info(
        'recorder_archive_created',
        category='recorder',
        cluster=cluster,
        window_start=window.start,
        window_end=window.end,
        name=final_name,
        file_count=deleted,
        raw_bytes=raw_bytes,
        compressed_bytes=_file_size(final_path),
    )


def _delete_sources(db_dir: str, sources: list[str]) -> int:
    """Delete merged raw files once the archive is durably in place.

    The directory entry for the new archive is fsynced first: without it a
    crash could leave a directory where the sources are gone and the
    archive's name never made it to disk.
    """
    _sync_dir(db_dir)
    for path in sources:
        _remove_with_sidecars(path)
    return len(sources)


def _set_aside(path: str, cluster: str) -> None:
    """Move a source the merge could not read out of the candidate set.

    The ``-wal``/``-shm`` sidecars move with it: they can hold rows not yet
    checkpointed into the main file, and a preserved database separated
    from its write-ahead log is a database missing its newest data.
    """
    target = path + UNREADABLE_SUFFIX
    try:
        os.rename(path, target)
    except OSError as exc:
        logger.error(
            'recorder_archive_source_skipped',
            category='recorder',
            cluster=cluster,
            name=os.path.basename(path),
            renamed_to=None,
            error=str(exc),
        )
        return
    for suffix in _SIDECAR_SUFFIXES:
        # A missing sidecar is the normal case (SQLite removes both on a
        # clean close), so a failed rename is nothing to report.
        with contextlib.suppress(OSError):
            os.rename(path + suffix, target + suffix)
    logger.error(
        'recorder_archive_source_skipped',
        category='recorder',
        cluster=cluster,
        name=os.path.basename(path),
        renamed_to=os.path.basename(target),
        hint='the file could not be read by the merge and was left on disk instead of archived',
    )


def _archived_source_files(merged_path: str) -> set[str]:
    """Return the raw file names recorded inside an unpacked archive."""
    try:
        conn = sqlite3.connect(f'file:{merged_path}?mode=ro', uri=True)
    except sqlite3.Error:
        return set()
    try:
        return {row[0] for row in conn.execute('SELECT source_file FROM workers') if row[0]}
    except sqlite3.Error:
        return set()
    finally:
        conn.close()


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


def _decompress(source_path: str, dest_path: str) -> None:
    """Unpack a published archive back into a plain SQLite file."""
    secure_db_file(dest_path)
    with gzip.open(source_path, 'rb') as compressed, open(dest_path, 'wb') as raw:
        shutil.copyfileobj(compressed, raw, _COPY_CHUNK_BYTES)


def _sync_dir(db_dir: str) -> None:
    """Flush ``db_dir``'s own entries so a rename survives a crash."""
    try:
        fd = os.open(db_dir, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _expire_archives(db_dir: str, cluster: str, now: float, retention_days: int) -> None:
    """Delete this cluster's archives whose window ended too long ago.

    The end timestamp comes from the file name, so no archive is opened.
    Other clusters' archives in a shared ``db_dir`` belong to their own
    workers and are never touched.
    """
    cutoff = now - retention_days * 86400
    pattern = re.compile(
        rf'^{re.escape(cluster)}-\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}__'
        rf'(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}){re.escape(ARCHIVE_SUFFIX)}$'
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
