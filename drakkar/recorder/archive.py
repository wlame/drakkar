"""Archiving of sealed recorder files in a cluster history folder.

Sealing moves every finished recorder file into ``<shared>/history/<cluster>/``
(see :mod:`drakkar.recorder.sealing`). This module folds each due time window
of that folder into one compressed file — ``<cluster>-<from>__<to>.db.gz``,
in the same folder — and deletes the sealed files (and their meta files) it
merged. A successful archive pass (or the operator) is the ONLY thing that
removes sealed files.

Three ideas carry the design:

* **Windows, not files.** Windows are ``[k*W, (k+1)*W)`` aligned to the
  Unix epoch in UTC, so with the default 24h width they are UTC calendar
  days. A file belongs to the window holding its START timestamp and is
  never split, which is why an archive can carry events slightly past its
  own end — archives partition by file start time, not by event time.
* **One archiver per cluster folder.** Every worker of the cluster runs the
  pass; ``.archive.lock`` (:mod:`drakkar.recorder.archive_lock`) lets one
  of them work at a time. Every ``*.db`` in the folder is sealed, so the
  pass needs no in-use or settled check and reads sources with
  ``immutable=1``.
* **Sources die last, and only by the lock holder.** Merge, compress,
  fsync and rename all happen on dot-prefixed work files named with the
  lock token. Right before publishing, the pass checks the lock still
  holds its token; a pass that lost the lock publishes and deletes
  nothing. Sealed files are deleted only after the archive is in place,
  so any failure costs one tick and no data.

Everything here is synchronous and does real file, SQLite and gzip work.
Callers on the event loop MUST run :func:`run_archive_pass` in a thread —
see ``EventRecorder._archive_pass``.
"""

from __future__ import annotations

import contextlib
import gzip
import os
import re
import shutil
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from drakkar.dbfiles import secure_db_file
from drakkar.merge import merge_databases
from drakkar.metrics import recorder_archive_bytes, recorder_archive_failures, recorder_archive_files
from drakkar.recorder.archive_lock import CLOCK_PREFIX, STALE_PREFIX, ArchiveLock
from drakkar.recorder.history import (
    DEFAULT_CLUSTER,  # noqa: F401  (re-exported: archive names fall back to it)
    META_SUFFIX,
    history_dir,
    list_sealed,
    merged_dir,
    meta_path,
    sanitize_cluster,
)
from drakkar.recorder.sealing import INCOMING_SUFFIX

if TYPE_CHECKING:
    from drakkar.config import HistoryArchiveConfig

logger = structlog.get_logger()

ARCHIVE_SUFFIX = '.db.gz'

# Raw file the merge engine could not read. Renaming it takes it out of
# the candidate set — so it cannot stall its window forever — while
# keeping the data on disk for an operator to look at.
UNREADABLE_SUFFIX = '.unreadable'

# Work files of a pass, all dot-prefixed and named with the lock token.
_MERGE_TEMP_SUFFIX = '.merge.db'
_COMPRESS_TEMP_SUFFIX = '.tmp'
_PREVIOUS_TEMP_SUFFIX = '.previous.db'

# What the sweep removes once it is older than ``lock_stale_seconds``: dot
# files ending in one of these (a pass's work files, a sealing copy in
# progress) or starting with one of these (an age-check marker, a lock set
# aside by a takeover), together with their SQLite sidecars.
_SWEPT_SUFFIXES = (_MERGE_TEMP_SUFFIX, _COMPRESS_TEMP_SUFFIX, _PREVIOUS_TEMP_SUFFIX, INCOMING_SUFFIX)
_SWEPT_PREFIXES = (CLOCK_PREFIX, STALE_PREFIX)

# Minute precision in archive names: window bounds always land on a
# multiple of the window width, so seconds carry no information.
ARCHIVE_TS_FORMAT = '%Y-%m-%d_%H-%M'

# SQLite sidecars removed alongside every file the pass deletes. A sealed
# file has none, but a work file of a pass that died may.
_SIDECAR_SUFFIXES = ('-wal', '-shm', '-journal')

# Trailing timestamp of a raw recorder file. Anchored at the END so worker
# names containing dashes ('search-worker-7') parse correctly.
_DB_NAME_RE = re.compile(r'-(\d{4})-(\d{2})-(\d{2})__(\d{2})_(\d{2})_(\d{2})\.db$')

# Copy buffer for the gzip stream. A merged window can be hundreds of MB,
# so the file is streamed rather than read into memory.
_COPY_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class ArchiveCandidate:
    """One sealed recorder file considered for archiving."""

    path: str
    start_ts: float
    window_start: float


class ArchiveFencedError(RuntimeError):
    """The pass lost the archive lock: another worker took it over."""


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


def list_archives(folder: str) -> list[ArchiveInfo]:
    """Return every archive file in ``folder``, newest-first by ``to_ts``.

    Name-only, like :func:`parse_db_start_ts`: cluster and window bounds
    come from the file name :func:`archive_file_name` wrote, never from
    opening the file, so a directory with many archives costs one
    ``listdir`` and one ``stat`` per match — cheap enough to call from a
    request handler. :data:`ARCHIVE_NAME_RE` is anchored against the whole
    name, so dot-prefixed temporaries, ``*.unreadable`` quarantine files
    and raw ``*.db`` files never match.
    """
    results: list[ArchiveInfo] = []
    if not folder or not os.path.isdir(folder):
        return results
    for entry in os.listdir(folder):
        match = ARCHIVE_NAME_RE.fullmatch(entry)
        if not match:
            continue
        try:
            size_bytes = os.stat(os.path.join(folder, entry)).st_size
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
    due_after_seconds: float,
    now: float,
) -> list[ArchiveWindow]:
    """Group ``paths`` into epoch-aligned windows, returning only due ones.

    A window ``[start, end)`` is DUE when ``now >= end + due_after_seconds``
    (``history.archive.due_after_hours``): the delay lets every worker of
    the cluster seal the files that belong to it. Nothing else holds a
    window back — every file in a history folder is sealed.

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
    for window_start, candidates in sorted(grouped.items()):
        window_end = window_start + window_seconds
        if now < window_end + due_after_seconds:
            continue
        due.append(
            ArchiveWindow(
                start=window_start,
                end=window_end,
                files=tuple(sorted(candidates, key=lambda candidate: candidate.path)),
            )
        )
    return due


def run_archive_pass(
    shared_dir: str,
    *,
    cluster: str,
    worker: str,
    instance: str,
    archive: HistoryArchiveConfig,
    merged_retention_days: int,
    stop: threading.Event | None = None,
) -> None:
    """Archive every due window of this cluster's history folder, then expire and prune.

    Blocking by nature (stat, SQLite merge, gzip) — run it in a thread,
    never on the event loop. Every worker of the cluster calls it; the
    archive lock lets one of them work at a time, and the others return
    right after refreshing the footprint gauges. A pass that took the lock
    and did not finish cleanly (a window failed, the lock was lost, an
    unexpected error) counts in ``drakkar_recorder_archive_failures_total``.

    ``stop`` ends the pass after the window in progress: a worker that
    shuts down does not wait for a long backlog of windows. Expiry and
    pruning are skipped too; the next pass of any worker does them.
    """
    folder = history_dir(shared_dir, cluster)
    own_cluster = sanitize_cluster(cluster)
    # Before the lock: the footprint is most worth reporting on the ticks
    # where another worker does the archiving.
    _observe_archive_footprint(folder, own_cluster)
    if not os.path.isdir(folder):
        return
    lock = ArchiveLock(folder, worker=worker, instance=instance, stale_seconds=archive.lock_stale_seconds)
    if not lock.acquire():
        return
    completed = False
    try:
        completed = _archive_locked(
            folder,
            own_cluster,
            lock,
            archive,
            merged_dir(shared_dir),
            merged_retention_days,
            stop or threading.Event(),
        )
    except ArchiveFencedError as exc:
        logger.warning(
            'recorder_archive_fenced',
            category='recorder',
            cluster=own_cluster,
            name=str(exc),
            detail='another worker took the archive lock over; this pass published and deleted nothing',
        )
    finally:
        # Deletes the lock only while it still holds our token.
        lock.release()
        if not completed:
            recorder_archive_failures.inc()


def _archive_locked(
    folder: str,
    cluster: str,
    lock: ArchiveLock,
    archive: HistoryArchiveConfig,
    merged_folder: str,
    merged_retention_days: int,
    stop: threading.Event,
) -> bool:
    """The pass body, run while holding the lock; False when a window failed."""
    now = time.time()
    # Ages of files in the shared folder are measured on the file server's
    # clock, like the lock's own age.
    server_now = lock.server_now()
    _sweep(folder, cluster, server_now, archive.lock_stale_seconds)
    windows = assign_windows(
        list_sealed(folder),
        archive.window_hours * 3600,
        archive.due_after_hours * 3600,
        now,
    )
    completed = True
    for window in windows:
        # A stop is not a failure: the windows left stay due, and the next
        # pass of any worker of the cluster takes them.
        if stop.is_set():
            return completed
        completed = _archive_window(folder, cluster, window, lock) and completed
    if stop.is_set():
        return completed
    if archive.retention_days > 0:
        _expire_archives(folder, cluster, now, archive.retention_days)
    if merged_retention_days > 0:
        _prune_merged(merged_folder, server_now, merged_retention_days)
    return completed


def _is_work_file(name: str) -> bool:
    """A work file the sweep may remove once it is old enough (sidecars included)."""
    base = name
    for suffix in _SIDECAR_SUFFIXES:
        base = base.removesuffix(suffix)
    return base.startswith('.') and (base.endswith(_SWEPT_SUFFIXES) or base.startswith(_SWEPT_PREFIXES))


def _is_orphan_meta(folder: str, name: str) -> bool:
    """A meta file whose sealed database is not next to it (archived, set aside, or a crash between the two)."""
    if name.startswith('.') or not name.endswith(META_SUFFIX):
        return False
    return not os.path.lexists(os.path.join(folder, name.removesuffix(META_SUFFIX)))


def _sweep(folder: str, cluster: str, now: float, stale_seconds: int) -> None:
    """Delete work files and orphan meta files older than ``stale_seconds``.

    Only reachable while holding the lock, and the age floor keeps
    anything still being written out of reach: a sealing copy in progress
    keeps moving its mtime, and a fresh orphan meta file is a publish
    whose database rename is still to come.
    """
    cutoff = now - stale_seconds
    for name in sorted(os.listdir(folder)):
        if not (_is_work_file(name) or _is_orphan_meta(folder, name)):
            continue
        path = os.path.join(folder, name)
        try:
            if os.lstat(path).st_mtime >= cutoff:
                continue
            os.unlink(path)
        except OSError:
            # Gone already, or not a file this protocol wrote (a folder):
            # leave it.
            continue
        logger.warning(
            'recorder_archive_temp_swept',
            category='recorder',
            cluster=cluster,
            name=name,
        )


def _archive_window(folder: str, cluster: str, window: ArchiveWindow, lock: ArchiveLock) -> bool:
    """Merge, compress and publish one window, then delete its sources; False when it failed.

    Order is what makes this safe: everything lands on work files named
    with the lock token, the rename publishes the archive atomically, and
    only then are the sealed files removed. Any failure before the rename
    leaves every source untouched for the next tick. Right before the
    rename (and before the deletions of the reclaim path) the lock must
    still hold our token; otherwise :class:`ArchiveFencedError` stops the
    whole pass with nothing published and nothing deleted.

    Two rules protect data that a naive "merge everything, delete
    everything" would lose:

    * Only the sources the merge engine actually read are deleted. One it
      could not read is moved aside as ``<name>.unreadable`` — off the
      candidate list so it cannot stall the window forever, but still on
      disk for an operator. Its meta file is left; the sweep removes it as
      an orphan once it is old.
    * An archive already at the final name is folded back in rather than
      overwritten. It happens when an earlier pass died between publishing
      the archive and finishing the deletion of its sources; the surviving
      sources are already inside it, so the file must never be replaced by
      a merge of what is left. Those sources are deleted rather than
      merged again — their events would otherwise land in the archive
      twice.
    """
    final_name = archive_file_name(cluster, window.start, window.end)
    final_path = os.path.join(folder, final_name)
    prefix = os.path.join(folder, f'.{final_name}.{lock.token}')
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
                _fence(lock, final_name)
                _delete_sources(folder, sources)
                logger.info(
                    'recorder_archive_sources_reclaimed',
                    category='recorder',
                    cluster=cluster,
                    window_start=window.start,
                    window_end=window.end,
                    name=final_name,
                    file_count=len(sources),
                )
                return True
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
        # subprocess output as the sealed files, and the final name inherits
        # the compressed temp's mode through the rename.
        secure_db_file(merged_path)
        secure_db_file(tmp_path)
        _compress(merged_path, tmp_path)
        _fence(lock, final_name)
        os.rename(tmp_path, final_path)
    except ArchiveFencedError:
        _remove_work_files(merged_path, previous_path, tmp_path)
        raise
    except Exception as exc:
        _remove_work_files(merged_path, previous_path, tmp_path)
        logger.error(
            'recorder_archive_failed',
            category='recorder',
            cluster=cluster,
            window_start=window.start,
            window_end=window.end,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return False

    _remove_with_sidecars(merged_path)
    _remove_with_sidecars(previous_path)
    covered = merged.union(already_covered)
    deleted = _delete_sources(folder, [path for path in sources if path in covered])
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
    return True


def _fence(lock: ArchiveLock, name: str) -> None:
    """Stop the pass when the lock no longer holds our token (another worker took it over)."""
    if not lock.is_ours():
        raise ArchiveFencedError(name)


def _remove_work_files(*paths: str) -> None:
    for path in paths:
        _remove_with_sidecars(path)


def _delete_sources(folder: str, sources: list[str]) -> int:
    """Delete merged sealed files and their meta files once the archive is durably in place.

    The folder entry for the new archive is fsynced first: without it a
    crash could leave a folder where the sources are gone and the
    archive's name never made it to disk.
    """
    _sync_dir(folder)
    for path in sources:
        _remove_with_sidecars(path)
        with contextlib.suppress(OSError):
            os.unlink(meta_path(path))
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


def _sync_dir(folder: str) -> None:
    """Flush ``folder``'s own entries so a rename survives a crash."""
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


def warn_if_archives_unbounded(archive: HistoryArchiveConfig, folder: str) -> None:
    """Warn at startup when archives are configured to accumulate forever.

    ``history.archive.retention_days: 0`` disables expiry outright, and nothing
    else in the recorder reclaims an archive. It is a legitimate choice —
    some deployments archive onto a volume they prune themselves — but it
    is the one recorder setting whose cost stays invisible until a disk
    fills, so the worker states it once rather than leaving it to be
    discovered. Informational only: the worker starts normally, exactly
    like the unauthenticated-UI warning.
    """
    if not archive.enabled or archive.retention_days > 0:
        return

    logger.warning(
        'recorder_archives_unbounded',
        category='recorder',
        folder=folder,
        window_hours=archive.window_hours,
        message=(
            f'history.archive.retention_days is 0, so compressed recorder archives in '
            f'{folder} are kept forever and nothing reclaims that disk. Watch '
            f'drakkar_recorder_archive_bytes, prune the folder yourself, or set '
            f'history.archive.retention_days (or DK_HISTORY__ARCHIVE__RETENTION_DAYS) '
            f'to a number of days covering the {archive.due_after_hours}h due delay plus one '
            f'{archive.window_hours}h window.'
        ),
    )


def _observe_archive_footprint(folder: str, cluster: str) -> None:
    """Publish this cluster's archive disk footprint as gauges.

    Names carry the cluster, so the folder is filtered down to this
    worker's own archives — the ones its retention setting governs.
    The cluster is read with :data:`ARCHIVE_NAME_RE`, the parser the archive
    listing uses, and compared exactly: a prefix match would also count a
    cluster whose name extends this one after a ``-`` (``search`` and
    ``search-hot``). Only the directory entries are read; no archive is
    opened.
    """
    total_bytes = 0
    count = 0
    for path in Path(folder).glob(f'*{ARCHIVE_SUFFIX}'):
        match = ARCHIVE_NAME_RE.match(path.name)
        if match is None or match['cluster'] != cluster:
            continue
        try:
            total_bytes += path.stat().st_size
        except OSError:
            continue  # expired by a peer's pass between glob and stat
        count += 1
    recorder_archive_bytes.set(total_bytes)
    recorder_archive_files.set(count)


def _expire_archives(folder: str, cluster: str, now: float, retention_days: int) -> None:
    """Delete this cluster's archives whose window ended too long ago.

    The end timestamp comes from the file name, so no archive is opened.
    An archive named for another cluster belongs to that cluster's
    workers and is never touched.
    """
    cutoff = now - retention_days * 86400
    pattern = re.compile(
        rf'^{re.escape(cluster)}-\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}__'
        rf'(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}){re.escape(ARCHIVE_SUFFIX)}$'
    )
    for path in Path(folder).glob(f'*{ARCHIVE_SUFFIX}'):
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


def _prune_merged(merged_folder: str, now: float, retention_days: int) -> None:
    """Delete UI merge results (``merged/*.db``) older than ``retention_days``.

    The archivers of every cluster run this on the same folder, so a file
    another one deleted first is simply skipped. Dot-prefixed names are
    merges still being written.
    """
    cutoff = now - retention_days * 86400
    try:
        names = os.listdir(merged_folder)
    except FileNotFoundError:
        return
    for name in sorted(names):
        if name.startswith('.') or not name.endswith('.db'):
            continue
        path = os.path.join(merged_folder, name)
        try:
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode) or info.st_mtime >= cutoff:
                continue
            os.unlink(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning('recorder_merged_prune_failed', category='recorder', name=name, error=str(exc))
            continue
        logger.info('recorder_merged_pruned', category='recorder', name=name, retention_days=retention_days)


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
