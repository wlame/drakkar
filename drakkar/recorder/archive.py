"""Archiving of sealed recorder files in a cluster history folder.

Sealing moves every finished recorder file into ``<dir>/history/<cluster>/``
(see :mod:`drakkar.recorder.sealing`). This module folds each due time window
of that folder into one compressed file in the same folder and deletes the
sealed files (and their meta files) it merged. A successful archive pass (or
the operator) is the ONLY thing that removes sealed files.

A pass has one of two scopes:

* **The whole cluster.** It takes every sealed file of the folder and writes
  ``<cluster>-<from>__<to>.db.gz``.
* **One worker's own files** (``only_worker``). It takes only the sealed files
  of that worker and writes ``<cluster>-<worker>-<from>__<to>.db.gz``. Its
  retention and its sweep touch only that worker's names, so workers that
  share a folder never take each other's files. It has no election: no other
  worker reads or deletes these files.

Three ideas carry the design:

* **Windows, not files.** Windows are ``[k*W, (k+1)*W)`` aligned to the
  Unix epoch in UTC, so with the default 24h width they are UTC calendar
  days. A file belongs to the window holding its START timestamp and is
  never split, which is why an archive can carry events slightly past its
  own end — archives partition by file start time, not by event time.
* **One archiver per cluster folder, in the cluster scope.** The pass is the
  body of the system cluster task ``history.archive``
  (:mod:`drakkar.cluster_tasks`): at each slot every worker of the cluster
  asks the election for the slot's lease (by default the task lock
  ``tasks/<cluster>/history.archive.lock`` of
  :mod:`drakkar.recorder.task_lock`), and only the one that gets it runs the
  pass.
  Every ``*.db`` in the folder is sealed, so the
  pass needs no in-use or settled check and reads sources with
  ``immutable=1``.
* **Sources die last.** Merge, compress, fsync and rename all happen on
  dot-prefixed work files named with the pass's token. In the cluster scope
  the pass checks through its fence, right before publishing, that the slot's
  lease is still held; a pass that lost it publishes and deletes nothing.
  Sealed files are deleted only after the archive is in place, so any
  failure costs one tick and no data.

Everything here is synchronous and does real file, SQLite and gzip work.
Callers on the event loop MUST run :func:`run_archive_pass` in a thread —
see ``EventRecorder.archive_cluster_task``.
"""

from __future__ import annotations

import contextlib
import gzip
import os
import re
import secrets
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
from drakkar.fleet.card import sanitize_worker_name
from drakkar.merge import merge_databases
from drakkar.metrics import recorder_archive_bytes, recorder_archive_failures, recorder_archive_files
from drakkar.recorder.election import LOSS_TAKEN_OVER, LOSS_UNCONFIRMED
from drakkar.recorder.history import (
    CLOCK_PREFIX,
    META_SUFFIX,
    folder_server_now,
    history_dir,
    list_sealed,
    merged_dir,
    meta_path,
)
from drakkar.recorder.sealing import INCOMING_SUFFIX
from drakkar.schedule import sanitize_cluster

if TYPE_CHECKING:
    from drakkar.config import RecorderArchiveConfig
    from drakkar.recorder.election import ThreadFence

logger = structlog.get_logger()

SECONDS_PER_HOUR = 3600
ARCHIVE_SUFFIX = '.db.gz'

# Raw file the merge engine could not read. Renaming it takes it out of
# the candidate set — so it cannot stall its window forever — while
# keeping the data on disk for an operator to look at.
UNREADABLE_SUFFIX = '.unreadable'

# Work files of a pass, all dot-prefixed and named with the pass's token.
_MERGE_TEMP_SUFFIX = '.merge.db'
_COMPRESS_TEMP_SUFFIX = '.tmp'
_PREVIOUS_TEMP_SUFFIX = '.previous.db'

# A copy of a ``.archive.lock`` file in a history folder, set aside by a takeover.
# It holds no data, so the sweep removes it. The pass takes no lock in this
# folder, and a ``.archive.lock`` file itself stays until an operator deletes it.
ARCHIVE_LOCK_COPY_PREFIX = '.archive.lock.stale.'

# What the sweep removes once it is older than ``sweep_after_seconds``: dot
# files ending in one of these (a pass's work files, a sealing copy in
# progress) or starting with one of these (an age-check marker, a lock copy
# set aside by a takeover), together with their SQLite sidecars.
_SWEPT_SUFFIXES = (_MERGE_TEMP_SUFFIX, _COMPRESS_TEMP_SUFFIX, _PREVIOUS_TEMP_SUFFIX, INCOMING_SUFFIX)
_SWEPT_PREFIXES = (CLOCK_PREFIX, ARCHIVE_LOCK_COPY_PREFIX)

# Minute precision in archive names: window bounds always land on a
# multiple of the window width, so seconds carry no information.
ARCHIVE_TS_FORMAT = '%Y-%m-%d_%H-%M'

# SQLite sidecars removed alongside every file the pass deletes. A sealed
# file has none, but a work file of a pass that died may.
_SIDECAR_SUFFIXES = ('-wal', '-shm', '-journal')

# Trailing timestamp of a raw recorder file. Anchored at the END so worker
# names containing dashes ('search-worker-7') parse correctly.
_DB_NAME_RE = re.compile(r'-(\d{4})-(\d{2})-(\d{2})__(\d{2})_(\d{2})_(\d{2})\.db$')

# The size of the work-file token of a pass with no lease; a lease token has the same size.
_OWN_TOKEN_BYTES = 8

# Copy buffer for the gzip stream. A merged window can be hundreds of MB,
# so the file is streamed rather than read into memory.
_COPY_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class ArchiveCandidate:
    """One sealed recorder file considered for archiving."""

    path: str
    start_ts: float
    window_start: float


# The ``detail`` of ``recorder_archive_fenced`` for each ``reason``; the texts
# are part of the log contract, shared by every implementation.
FENCED_DETAILS: dict[str, str] = {
    LOSS_TAKEN_OVER: 'another worker took the archive lock over; this pass published and deleted nothing',
    LOSS_UNCONFIRMED: 'the election provider could not confirm the lease; this pass published and deleted nothing',
}


class ArchiveFencedError(RuntimeError):
    """The pass lost its election lease: another worker took it over, or the provider could not confirm it.

    ``name`` is the archive the pass was about to publish; ``reason`` and
    ``provider`` are the fence's
    :attr:`~drakkar.recorder.election.ThreadFence.loss_reason` and provider name.
    """

    def __init__(self, name: str, reason: str | None, provider: str) -> None:
        super().__init__(name)
        self.name = name
        self.reason = reason
        self.provider = provider


@dataclass(frozen=True)
class ArchiveWindow:
    """A due time window ``[start, end)`` and the files assigned to it."""

    start: float
    end: float
    files: tuple[ArchiveCandidate, ...]


@dataclass(frozen=True)
class _PassTarget:
    """What one pass works on.

    ``cluster`` is sanitized. ``worker`` is the sanitized name of the worker
    whose own files the pass takes, ``None`` for the whole cluster. ``owner``
    is what the archive names of this pass start with. ``fence`` is the slot's
    lease check, ``None`` for a pass with no election. ``token`` names the
    pass's work files.
    """

    folder: str
    cluster: str
    worker: str | None
    owner: str
    fence: ThreadFence | None
    token: str


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


def is_own_sealed_name(name: str, worker: str) -> bool:
    """Whether the sealed file ``name`` is a file of ``worker``.

    The name minus its time stamp, sanitized, must equal the sanitized worker
    name exactly, so ``w`` never takes a file of ``w-1``. A name with no time
    stamp belongs to nobody. Both sides are sanitized because a recorder file
    carries the worker name as it is, and the archive name carries it sanitized.
    """
    match = _DB_NAME_RE.search(name)
    return match is not None and sanitize_worker_name(name[: match.start()]) == sanitize_worker_name(worker)


def _archive_owner(cluster: str, worker: str | None) -> str:
    """What the archive names of a scope start with: ``<cluster>``, or ``<cluster>-<worker>`` for own files."""
    own_cluster = sanitize_cluster(cluster)
    return own_cluster if worker is None else f'{own_cluster}-{sanitize_worker_name(worker)}'


def archive_file_name(cluster: str, start: float, end: float, *, worker: str | None = None) -> str:
    """Return the archive file name for one window of one cluster, or of one worker's own files."""
    return f'{_archive_owner(cluster, worker)}-{_format_bound(start)}__{_format_bound(end)}{ARCHIVE_SUFFIX}'


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
# by name alone, without opening the file. Cluster and worker names both may
# hold ``-``, so ``owner`` cannot be split into the two: the cluster of an
# archive is the folder it lies in.
ARCHIVE_NAME_RE = re.compile(
    r'^(?P<owner>[a-zA-Z0-9_-]+)-'
    r'(?P<from_ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2})__'
    r'(?P<to_ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2})'
    r'\.db\.gz$'
)


@dataclass(frozen=True)
class ArchiveInfo:
    """One archive file's identity and size: the cluster from its folder, the rest from its name and a ``stat``."""

    name: str
    cluster: str
    from_ts: float
    to_ts: float
    size_bytes: int


def list_archives(folder: str) -> list[ArchiveInfo]:
    """Return every archive file in ``folder``, newest-first by ``to_ts``.

    Name-only, like :func:`parse_db_start_ts`: the window bounds come from the
    file name :func:`archive_file_name` wrote and the cluster from the name of
    ``folder``, never from opening the file, so a directory with many archives
    costs one ``listdir`` and one ``stat`` per match — cheap enough to call
    from a request handler. :data:`ARCHIVE_NAME_RE` is anchored against the whole
    name, so dot-prefixed temporaries, ``*.unreadable`` quarantine files
    and raw ``*.db`` files never match.
    """
    results: list[ArchiveInfo] = []
    if not folder or not os.path.isdir(folder):
        return results
    cluster = os.path.basename(os.path.normpath(folder))
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
                cluster=cluster,
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
    (``recorder.archive.due_after``): the delay lets every worker of
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
    instance: str,
    archive: RecorderArchiveConfig,
    fence: ThreadFence | None = None,
    only_worker: str | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Archive every due window of this cluster's history folder, then expire and prune.

    In the cluster scope this is the body of a won ``history.archive`` slot:
    the slot loop took the lease and releases it, and ``fence`` checks it
    before each publish. The fence runs its checks on the loop and waits for
    them. With ``only_worker`` the pass takes only that worker's sealed files
    and archives, and needs no ``fence``. A pass with neither would delete the
    files of the whole cluster with no lease, so it fails at once.

    Blocking by nature (stat, SQLite merge, gzip) — run it in a thread, never
    on the event loop. A pass that did not finish cleanly (a window failed, the
    lease was lost, an unexpected error) counts in
    ``drakkar_recorder_archive_failures_total``.

    ``stop`` ends the pass after the window in progress: a worker that
    shuts down does not wait for a long backlog of windows. Expiry and
    pruning are skipped too; the next pass does them.

    Raises:
        ValueError: neither ``fence`` nor ``only_worker`` is given.
    """
    if fence is None and only_worker is None:
        raise ValueError('an archive pass needs a fence or only_worker')
    target = _PassTarget(
        folder=history_dir(shared_dir, cluster),
        cluster=sanitize_cluster(cluster),
        worker=None if only_worker is None else sanitize_worker_name(only_worker),
        owner=_archive_owner(cluster, only_worker),
        fence=fence,
        token=fence.token if fence is not None else secrets.token_hex(_OWN_TOKEN_BYTES),
    )
    _observe_footprint_in(target.folder, target.cluster, target.worker)
    stop_event = stop or threading.Event()
    completed = False
    try:
        completed = _archive_folder(target, instance, archive, stop_event)
        if archive.merged_retention > 0 and not stop_event.is_set():
            _prune_merged(merged_dir(shared_dir), instance, archive.merged_retention)
    except ArchiveFencedError as exc:
        logger.warning(
            'recorder_archive_fenced',
            category='recorder',
            provider=exc.provider,
            reason=exc.reason,
            cluster=target.cluster,
            name=exc.name,
            # A lease reads as not held only with a reason; the claim that says less is the fallback.
            detail=FENCED_DETAILS.get(exc.reason or '', FENCED_DETAILS[LOSS_UNCONFIRMED]),
        )
    finally:
        if not completed:
            recorder_archive_failures.inc()


def _sealed_of(target: _PassTarget) -> list[str]:
    """The sealed files this pass may take: all of the folder, or the worker's own."""
    paths = list_sealed(target.folder)
    if target.worker is None:
        return paths
    return [path for path in paths if is_own_sealed_name(os.path.basename(path), target.worker)]


def _archive_folder(
    target: _PassTarget,
    instance: str,
    archive: RecorderArchiveConfig,
    stop: threading.Event,
) -> bool:
    """Sweep the history folder, archive its due windows and expire old archives; False when a window failed.

    A missing folder has nothing to do, which is no failure. Nothing else
    archives the same files at the same time: the slot's lease or the scope of
    the pass keeps other workers away.
    """
    if not os.path.isdir(target.folder):
        return True
    now = time.time()
    # Ages of files in the shared folder are measured on the file server's
    # clock, whichever provider holds the lease.
    server_now = folder_server_now(target.folder, instance)
    _sweep(target, server_now, archive.sweep_after)
    windows = assign_windows(
        _sealed_of(target),
        archive.window * SECONDS_PER_HOUR,
        archive.due_after * SECONDS_PER_HOUR,
        now,
    )
    completed = True
    for window in windows:
        # A stop is not a failure: the windows left stay due, and the next
        # pass takes them.
        if stop.is_set():
            return completed
        completed = _archive_window(target, window) and completed
    if stop.is_set():
        return completed
    if archive.retention > 0:
        _expire_archives(target, now, archive.retention)
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


def _work_file_subject(name: str) -> str:
    """The archive or sealed file a work file is made for: ``<subject>`` of ``.<subject>.<token><suffix>``."""
    base = name
    for suffix in _SIDECAR_SUFFIXES:
        base = base.removesuffix(suffix)
    for suffix in _SWEPT_SUFFIXES:
        base = base.removesuffix(suffix)
    return base.removeprefix('.').rpartition('.')[0]


def _may_sweep(name: str, target: _PassTarget) -> bool:
    """Whether the pass may sweep this old file.

    A pass over the cluster may sweep every work file and orphan meta file. An
    own-scope pass sweeps only those of its worker, plus the markers that belong
    to no worker and hold no data.
    """
    is_work_file = _is_work_file(name)
    if not (is_work_file or _is_orphan_meta(target.folder, name)):
        return False
    worker = target.worker
    if worker is None or name.startswith(_SWEPT_PREFIXES):
        return True
    # A work file may be made for a meta file: ``.<sealed>.meta.json.<token>.tmp``.
    subject = (_work_file_subject(name) if is_work_file else name).removesuffix(META_SUFFIX)
    archive = ARCHIVE_NAME_RE.fullmatch(subject)
    if archive is not None:
        return archive['owner'] == target.owner
    return is_own_sealed_name(subject, worker)


def _sweep(target: _PassTarget, now: float, stale_seconds: int) -> None:
    """Delete work files and orphan meta files older than ``stale_seconds``.

    Only reachable with the files in the pass's care, and the age floor keeps
    anything still being written out of reach: a sealing copy in progress
    keeps moving its mtime, and a fresh orphan meta file is a publish
    whose database rename is still to come.
    """
    folder = target.folder
    cutoff = now - stale_seconds
    for name in sorted(os.listdir(folder)):
        if not _may_sweep(name, target):
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
            cluster=target.cluster,
            name=name,
        )


def _archive_window(target: _PassTarget, window: ArchiveWindow) -> bool:
    """Merge, compress and publish one window, then delete its sources; False when it failed.

    Order is what makes this safe: everything lands on work files named
    with the pass's token, the rename publishes the archive atomically, and
    only then are the sealed files removed. Any failure before the rename
    leaves every source untouched for the next tick. Right before the
    rename (and before the deletions of the reclaim path) a fence must
    still confirm the lease; otherwise :class:`ArchiveFencedError` stops the whole
    pass with nothing published and nothing deleted.

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
    folder = target.folder
    cluster = target.cluster
    fence = target.fence
    final_name = archive_file_name(cluster, window.start, window.end, worker=target.worker)
    final_path = os.path.join(folder, final_name)
    prefix = os.path.join(folder, f'.{final_name}.{target.token}')
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
                _fence(fence, final_name)
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
        _fence(fence, final_name)
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


def _fence(fence: ThreadFence | None, name: str) -> None:
    """Stop the pass when the election does not confirm the slot's lease; a pass with no election has no check."""
    if fence is not None and not fence.is_held():
        raise ArchiveFencedError(name, fence.loss_reason, fence.provider_name)


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


def warn_if_archives_unbounded(archive: RecorderArchiveConfig, folder: str) -> None:
    """Warn at startup when archives are configured to accumulate forever.

    ``recorder.archive.retention: 0`` disables expiry outright, and nothing
    else in the recorder reclaims an archive. It is a legitimate choice —
    some deployments archive onto a volume they prune themselves — but it
    is the one recorder setting whose cost stays invisible until a disk
    fills, so the worker states it once rather than leaving it to be
    discovered. Informational only: the worker starts normally, exactly
    like the unauthenticated-UI warning.
    """
    if not archive.enabled or archive.retention > 0:
        return

    logger.warning(
        'recorder_archives_unbounded',
        category='recorder',
        folder=folder,
        window_hours=archive.window,
        message=(
            f'recorder.archive.retention is 0, so compressed recorder archives in '
            f'{folder} are kept forever and nothing reclaims that disk. Watch '
            f'drakkar_recorder_archive_bytes, prune the folder yourself, or set '
            f'recorder.archive.retention (or DK_RECORDER__ARCHIVE__RETENTION) '
            f'to a duration in whole days, for example 30d, covering the {archive.due_after}h due delay plus one '
            f'{archive.window}h window.'
        ),
    )


def observe_archive_footprint(shared_dir: str, cluster: str) -> None:
    """Publish the archive disk footprint of ``cluster`` in ``shared_dir`` as gauges.

    Blocking (it lists the shared history folder): call it in a thread. A
    worker that does not win an archive slot calls it, so the gauges stay
    current on every worker; a missing folder reports zero.
    """
    _observe_footprint_in(history_dir(shared_dir, cluster), sanitize_cluster(cluster))


def _observe_footprint_in(folder: str, cluster: str, worker: str | None = None) -> None:
    """Publish the archive disk footprint of the sanitized ``cluster`` in its history ``folder`` as gauges.

    Archive names carry their owner, so the folder is filtered down to the
    archives of the cluster and, in an own-scope pass, of the sanitized
    ``worker`` too. The owner is read with :data:`ARCHIVE_NAME_RE`, the parser
    the archive listing uses, and compared exactly: a prefix match would also
    count an owner whose name extends this one after a ``-`` (``search-w`` and
    ``search-w-1``). Only the directory entries are read; no archive is opened.
    """
    owners = {cluster, _archive_owner(cluster, worker)}
    total_bytes = 0
    count = 0
    for path in Path(folder).glob(f'*{ARCHIVE_SUFFIX}'):
        match = ARCHIVE_NAME_RE.match(path.name)
        if match is None or match['owner'] not in owners:
            continue
        try:
            total_bytes += path.stat().st_size
        except OSError:
            continue  # expired by another pass between glob and stat
        count += 1
    recorder_archive_bytes.set(total_bytes)
    recorder_archive_files.set(count)


def _expire_archives(target: _PassTarget, now: float, retention_days: int) -> None:
    """Delete the archives of this pass's owner whose window ended too long ago.

    The end timestamp comes from the file name, so no archive is opened.
    The owner is compared exactly: an archive of another cluster or another
    worker (``search-w-1`` for the owner ``search-w``) is never touched.
    """
    cluster = target.cluster
    cutoff = now - retention_days * 86400
    for path in Path(target.folder).glob(f'*{ARCHIVE_SUFFIX}'):
        match = ARCHIVE_NAME_RE.fullmatch(path.name)
        if match is None or match['owner'] != target.owner:
            continue
        try:
            end_ts = _parse_bound(match['to_ts'])
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


def _prune_merged(merged_folder: str, instance: str, retention_days: int) -> None:
    """Delete UI merge results (``merged/*.db``) older than ``retention_days``, when the folder exists.

    Every archive pass runs this on the same folder, so a file another
    one deleted first is simply skipped. Dot-prefixed names are
    merges still being written. Ages are measured on the file server's clock.
    """
    try:
        names = os.listdir(merged_folder)
    except FileNotFoundError:
        return
    cutoff = folder_server_now(merged_folder, instance) - retention_days * 86400
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
