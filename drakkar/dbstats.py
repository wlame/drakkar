"""Persistent stats cache behind the debug databases page.

Why this exists
---------------
``GET /api/debug/databases`` used to open every ``.db`` file in ``db_dir``
and run ``COUNT(*)`` / ``MIN`` / ``MAX`` / ``GROUP BY`` over its whole
events table on every page load. With hourly rotation and a shared
``db_dir`` that is dozens of full table scans per click — the page took
seconds while producing information that, for rotated files, can never
change again.

The design leans on one fact and one requirement:

- **Rotated DBs are immutable.** Their statistics can be computed once
  and reused forever, keyed by ``(path, mtime_ns, size_bytes)``.
- **The directory is the source of truth, never the cache.** Operators
  reach into ``db_dir`` and delete files. So the file *list* always comes
  from a live ``os.scandir`` — a deleted file disappears from the page on
  the very next request and a new file appears immediately — and the
  cache only supplies *derived statistics* for files that still exist.
  The cache file itself is disposable: delete it and everything rescans.

Three feeders keep the cache warm (all writing through the same
idempotent ``store``):

1. **Rotation** — the recorder computes stats for the just-closed file
   right after the swap, so the most common new file is warm before
   anyone opens the page.
2. **The warmer loop** — a periodic sweep scans whatever is missing
   (legacy files at first boot, files rotated by co-located workers) and
   purges cache rows whose files are gone.
3. **The endpoint itself** — a page request scans at most
   ``ui.recorder.dbstats_inline_scan_limit`` cold files inline; the rest
   render immediately as ``stats_pending`` rows and fill in as the warmer
   catches up.

Live (still-written) recorder DBs get an **incremental delta scan**: the
events table is append-only with a monotonic integer id, so the cache
keeps ``max_event_id`` and each refresh aggregates only ``id >`` that
cursor, merging into the cached per-event counts — new-rows cost instead
of a full ``GROUP BY`` over millions of rows. A cursor that goes
backwards (file replaced) falls back to a full scan.

The cache lives in ``<db_dir>/.dbstats.db`` — dot-prefixed, so the
directory listing itself excludes it. It is shared by every co-located
worker: rows are keyed by absolute path and writes are idempotent
(re-scanning the same immutable file yields the same row), so concurrent
writers need only WAL + a busy timeout, no election. The schema below is
a cross-backend contract — the Go worker reads and writes the same file.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field, replace

from drakkar.dbfiles import secure_db_file
from drakkar.merge import DbStats, _dict_factory, _table_exists, scan_db

DBSTATS_FILENAME = '.dbstats.db'

_LIVE_SUFFIX = '-live.db'
_CACHE_SUFFIX = '-cache.db'

BUSY_TIMEOUT_MS = 5000

# Cross-backend schema — the Go worker creates/reads the identical table.
# ``path`` is the absolute path of the *scanned* file (for cache DBs the
# ``.actual`` target, not the symlink). Version suffix in the table name
# so a future incompatible change can coexist during a mixed-version
# fleet's transition instead of corrupting one side.
SCHEMA_DBSTATS = """
CREATE TABLE IF NOT EXISTS db_stats_v1 (
    path             TEXT PRIMARY KEY,
    mtime_ns         INTEGER NOT NULL,
    size_bytes       INTEGER NOT NULL,
    kind             TEXT NOT NULL,
    worker_name      TEXT NOT NULL DEFAULT '',
    cluster_name     TEXT NOT NULL DEFAULT '',
    event_count      INTEGER NOT NULL DEFAULT 0,
    event_counts     TEXT NOT NULL DEFAULT '{}',
    first_event_ts   REAL,
    last_event_ts    REAL,
    has_events       INTEGER NOT NULL DEFAULT 0,
    has_config       INTEGER NOT NULL DEFAULT 0,
    has_state        INTEGER NOT NULL DEFAULT 0,
    cache_entry_count INTEGER,
    max_event_id     INTEGER,
    scanned_at       REAL NOT NULL
);
"""


@dataclass
class CachedStats:
    """One cache row: the stats plus the identity they were computed for."""

    stats: DbStats
    mtime_ns: int
    size_bytes: int


@dataclass
class DbRow:
    """One row of the databases page.

    ``stats_pending=True`` means only the identity fields (filename, path,
    size, kind-if-known) are meaningful — the file exists but its
    statistics have not been computed yet (cold cache, inline budget
    exhausted). ``live_for`` names the worker currently writing this file
    (resolved from the ``*-live.db`` / ``*-cache.db`` symlinks), or ''.
    """

    stats: DbStats
    live_for: str = ''
    stats_pending: bool = False


@dataclass
class _Listing:
    """What one ``os.scandir`` pass over ``db_dir`` found."""

    # (path, mtime_ns, size) of every regular candidate .db file.
    files: list[tuple[str, int, int]] = field(default_factory=list)
    # realpath(target) -> worker name, from ``<worker>-live.db`` symlinks.
    live_targets: dict[str, str] = field(default_factory=dict)
    # (symlink basename, worker, target path) from ``<worker>-cache.db``
    # symlinks whose target exists.
    cache_links: list[tuple[str, str, str]] = field(default_factory=list)


class DbStatsCache:
    """The ``.dbstats.db`` store. Every method opens its own short-lived
    connection — calls come from arbitrary threads (endpoint offload,
    warmer, rotation) and sqlite3 connections are not shareable across
    them; the open cost is dwarfed by the scans this cache avoids."""

    def __init__(self, db_dir: str) -> None:
        self._db_dir = db_dir
        self._path = os.path.join(db_dir, DBSTATS_FILENAME)

    def _connect(self) -> sqlite3.Connection:
        try:
            return self._connect_once()
        except sqlite3.DatabaseError:
            # A corrupt or non-SQLite cache file (partial write, operator
            # accident in the shared dir) would otherwise silently disable
            # caching forever — every page load degrades to full rescans
            # with nothing telling anyone why. The cache is 100% derived
            # data, so the honest self-heal is: throw it away and rebuild.
            for suffix in ('', '-wal', '-shm'):
                try:
                    os.remove(self._path + suffix)
                except OSError:
                    pass
            return self._connect_once()

    def _connect_once(self) -> sqlite3.Connection:
        # Owner-only before the driver creates the file — same contract as
        # every other DB this framework writes (see drakkar.dbfiles).
        secure_db_file(self._path)
        db = sqlite3.connect(self._path, timeout=BUSY_TIMEOUT_MS / 1000)
        try:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
            db.executescript(SCHEMA_DBSTATS)
        except Exception:
            db.close()
            raise
        return db

    def load_all(self) -> dict[str, CachedStats]:
        """Every cached row, keyed by path. Unreadable cache → empty dict
        (the callers then simply rescan — the cache must never be able to
        break the page)."""
        try:
            db = self._connect()
        except Exception:
            return {}
        try:
            db.row_factory = _dict_factory
            out: dict[str, CachedStats] = {}
            for row in db.execute('SELECT * FROM db_stats_v1'):
                try:
                    counts = json.loads(row['event_counts'])
                except (TypeError, ValueError):
                    counts = {}
                stats = DbStats(
                    path=row['path'],
                    filename=os.path.basename(row['path']),
                    worker_name=row['worker_name'],
                    cluster_name=row['cluster_name'],
                    event_count=row['event_count'],
                    event_counts=counts if isinstance(counts, dict) else {},
                    first_event_ts=row['first_event_ts'],
                    last_event_ts=row['last_event_ts'],
                    has_events=bool(row['has_events']),
                    has_config=bool(row['has_config']),
                    has_state=bool(row['has_state']),
                    size_bytes=row['size_bytes'],
                    kind=row['kind'],
                    max_event_id=row['max_event_id'],
                    cache_entry_count=row['cache_entry_count'],
                )
                out[row['path']] = CachedStats(stats=stats, mtime_ns=row['mtime_ns'], size_bytes=row['size_bytes'])
            return out
        except Exception:
            return {}
        finally:
            db.close()

    def store(self, stats: DbStats, *, mtime_ns: int, size_bytes: int) -> None:
        """Insert-or-replace one row. Best-effort: a locked/lost cache only
        costs a rescan later, never an error on the page path."""
        try:
            db = self._connect()
        except Exception:
            return
        try:
            db.execute(
                'INSERT OR REPLACE INTO db_stats_v1 '
                '(path, mtime_ns, size_bytes, kind, worker_name, cluster_name, event_count, '
                ' event_counts, first_event_ts, last_event_ts, has_events, has_config, '
                ' has_state, cache_entry_count, max_event_id, scanned_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [
                    stats.path,
                    mtime_ns,
                    size_bytes,
                    stats.kind,
                    stats.worker_name,
                    stats.cluster_name,
                    stats.event_count,
                    json.dumps(stats.event_counts, sort_keys=True),
                    stats.first_event_ts,
                    stats.last_event_ts,
                    int(stats.has_events),
                    int(stats.has_config),
                    int(stats.has_state),
                    stats.cache_entry_count,
                    stats.max_event_id,
                    time.time(),
                ],
            )
            db.commit()
        except Exception:
            pass
        finally:
            db.close()

    def purge_except(self, existing_paths: set[str]) -> int:
        """Drop rows whose file is gone; returns how many were dropped.

        Called from the warmer, not the endpoint — the page path stays
        read-mostly, and a stale row for a deleted file is invisible
        anyway (the listing is what decides existence).
        """
        try:
            db = self._connect()
        except Exception:
            return 0
        try:
            rows = db.execute('SELECT path FROM db_stats_v1').fetchall()
            stale = [r[0] for r in rows if r[0] not in existing_paths]
            for path in stale:
                db.execute('DELETE FROM db_stats_v1 WHERE path = ?', [path])
            if stale:
                db.commit()
            return len(stale)
        except Exception:
            return 0
        finally:
            db.close()


def _scan_listing(db_dir: str) -> _Listing:
    """One pass over ``db_dir``: candidate files + the two symlink maps."""
    listing = _Listing()
    if not db_dir or not os.path.isdir(db_dir):
        return listing
    for entry in sorted(os.listdir(db_dir)):
        # Dot-prefixed names are pass-internal state (merge temporaries,
        # this module's own cache file) — never page rows.
        if entry.startswith('.') or not entry.endswith('.db'):
            continue
        full = os.path.join(db_dir, entry)
        if os.path.islink(full):
            # The symlinks ARE the in-use markers: <worker>-live.db points
            # at the recorder DB that worker is writing right now,
            # <worker>-cache.db at its handler-cache file.
            try:
                target = os.path.realpath(full)
            except OSError:
                continue
            if not os.path.isfile(target):
                continue  # dangling link (crashed worker, deleted target)
            if entry.endswith(_LIVE_SUFFIX):
                listing.live_targets[target] = entry.removesuffix(_LIVE_SUFFIX)
            elif entry.endswith(_CACHE_SUFFIX):
                listing.cache_links.append((entry, entry.removesuffix(_CACHE_SUFFIX), target))
            continue
        if not os.path.isfile(full):
            continue
        try:
            st = os.stat(full)
        except OSError:
            continue  # deleted between listdir and stat
        listing.files.append((full, st.st_mtime_ns, st.st_size))
    return listing


def _delta_scan(path: str, cached: CachedStats) -> DbStats | None:
    """Incrementally refresh stats for a still-growing recorder DB.

    Aggregates only ``events.id > cached.max_event_id`` and merges into
    the cached counts — exact because the live events table is
    append-only (retention was removed; rotation switches files instead
    of deleting rows). Returns None whenever the increment cannot be
    trusted (cursor went backwards → the path was recreated; table
    missing; any error) and the caller falls back to a full scan.
    """
    prev = cached.stats
    if not prev.has_events or prev.max_event_id is None:
        return None
    db: sqlite3.Connection | None = None
    try:
        db = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=BUSY_TIMEOUT_MS / 1000)
        db.row_factory = _dict_factory
        db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
        if not _table_exists(db, 'events'):
            return None
        row = db.execute('SELECT MAX(id) as max_id FROM events').fetchone()
        new_max = row['max_id'] if row else None
        if new_max is None or new_max < prev.max_event_id:
            return None

        counts = dict(prev.event_counts)
        event_count = prev.event_count
        first_ts = prev.first_event_ts
        last_ts = prev.last_event_ts
        if new_max > prev.max_event_id:
            row = db.execute(
                'SELECT COUNT(*) as cnt, MIN(ts) as first_ts, MAX(ts) as last_ts FROM events WHERE id > ?',
                [prev.max_event_id],
            ).fetchone()
            if row:
                event_count += row['cnt'] or 0
                if row['first_ts'] is not None:
                    first_ts = row['first_ts'] if first_ts is None else min(first_ts, row['first_ts'])
                if row['last_ts'] is not None:
                    last_ts = row['last_ts'] if last_ts is None else max(last_ts, row['last_ts'])
            for row in db.execute(
                'SELECT event, COUNT(*) as cnt FROM events WHERE id > ? GROUP BY event',
                [prev.max_event_id],
            ):
                counts[row['event']] = counts.get(row['event'], 0) + row['cnt']

        # Cheap metadata refresh: worker_config lands shortly AFTER
        # rotation creates the file, so the first scan of a brand-new live
        # DB may have missed it.
        worker_name, cluster_name = prev.worker_name, prev.cluster_name
        has_config = prev.has_config
        if _table_exists(db, 'worker_config'):
            has_config = True
            row = db.execute('SELECT worker_name, cluster_name FROM worker_config WHERE id = 1').fetchone()
            if row:
                worker_name = row['worker_name'] or ''
                cluster_name = row['cluster_name'] or ''

        return DbStats(
            path=path,
            filename=os.path.basename(path),
            worker_name=worker_name,
            cluster_name=cluster_name,
            event_count=event_count,
            event_counts=counts,
            first_event_ts=first_ts,
            last_event_ts=last_ts,
            has_events=True,
            has_config=has_config,
            has_state=prev.has_state or _table_exists(db, 'worker_state'),
            size_bytes=os.path.getsize(path),
            kind=prev.kind,
            max_event_id=new_max,
            cache_entry_count=prev.cache_entry_count,
        )
    except Exception:
        return None
    finally:
        if db is not None:
            db.close()


def _resolve_row(
    path: str,
    mtime_ns: int,
    size: int,
    cached: dict[str, CachedStats],
    cache: DbStatsCache,
    budget: list[int],
    *,
    display_name: str | None = None,
) -> DbRow:
    """Produce one page row: cached / delta-scanned / fully-scanned / pending.

    ``budget`` is a single-element mutable list so the caller's inline
    full-scan allowance is shared across recorder and cache rows alike.
    A ``budget[0] < 0`` means unlimited (the warmer's sweep).
    """
    entry = cached.get(path)
    if entry is not None and entry.mtime_ns == mtime_ns and entry.size_bytes == size:
        return _finish(entry.stats, display_name)

    if entry is not None:
        # Changed file with a usable cursor — the live DB path. Delta cost
        # is proportional to NEW events only, so it does not count against
        # the inline full-scan budget.
        delta = _delta_scan(path, entry)
        if delta is not None:
            cache.store(delta, mtime_ns=mtime_ns, size_bytes=size)
            return _finish(delta, display_name)

    if budget[0] != 0:
        if budget[0] > 0:
            budget[0] -= 1
        stats = scan_db(path)
        cache.store(stats, mtime_ns=mtime_ns, size_bytes=size)
        return _finish(stats, display_name)

    # Budget exhausted: the file is real (it is in the listing) but its
    # stats wait for the warmer. Reuse the stale cached kind as a hint so
    # the UI can at least badge the row correctly.
    pending = DbStats(
        path=path,
        filename=display_name or os.path.basename(path),
        size_bytes=size,
        kind=entry.stats.kind if entry is not None else 'unknown',
    )
    return DbRow(stats=pending, stats_pending=True)


def _finish(stats: DbStats, display_name: str | None) -> DbRow:
    if display_name is not None and stats.filename != display_name:
        # Cache DBs display under their stable symlink name, not the
        # ``.actual`` target basename. Copy so the cached object (shared
        # via load_all) is not mutated.
        stats = replace(stats, filename=display_name)
    return DbRow(stats=stats)


def collect(db_dir: str, cache: DbStatsCache, *, inline_scan_limit: int) -> list[DbRow]:
    """Build the databases page: live listing + cached/derived statistics.

    ``inline_scan_limit`` caps how many cold files a single call may fully
    scan; ``-1`` means unlimited (the warmer). Rows beyond the cap come
    back with ``stats_pending=True``.
    """
    # Before touching the cache: with no directory there is nothing to
    # list AND nothing to cache — opening the cache here would create a
    # stray ./.dbstats.db in the CWD in memory-only mode (db_dir='').
    if not db_dir or not os.path.isdir(db_dir):
        return []
    listing = _scan_listing(db_dir)
    cached = cache.load_all()
    budget = [inline_scan_limit]
    rows: list[DbRow] = []

    live_workers = set(listing.live_targets.values())

    for path, mtime_ns, size in listing.files:
        row = _resolve_row(path, mtime_ns, size, cached, cache, budget)
        row.live_for = listing.live_targets.get(path, '')
        rows.append(row)

    for symlink_name, worker, target in listing.cache_links:
        try:
            st = os.stat(target)
        except OSError:
            continue
        row = _resolve_row(target, st.st_mtime_ns, st.st_size, cached, cache, budget, display_name=symlink_name)
        # Cache DBs carry no worker_config table, but the symlink name IS
        # the worker's identity — fill it in so the page can group and
        # filter cache rows like any other.
        if not row.stats.worker_name:
            row.stats = replace(row.stats, worker_name=worker)
        # A cache DB has no -live.db of its own; "in use" is approximated
        # by its worker's recorder being live in the same directory. A
        # worker running with ui.enabled=false has no live symlink and its
        # cache shows as not-in-use — documented, eventually-consistent.
        row.live_for = worker if worker in live_workers else ''
        rows.append(row)

    rows.sort(key=lambda r: r.stats.filename)
    return rows


def scan_and_store(path: str, cache: DbStatsCache) -> None:
    """Full-scan one file and cache the result — the rotation hook.

    Called (on a thread) right after the recorder rotates: the just-closed
    file is immutable from here on, so this one scan serves every future
    page load. Best-effort: a vanished or unreadable file simply stays for
    the warmer / inline path to retry.
    """
    try:
        st = os.stat(path)
    except OSError:
        return
    stats = scan_db(path)
    cache.store(stats, mtime_ns=st.st_mtime_ns, size_bytes=st.st_size)


def warm_directory(db_dir: str, cache: DbStatsCache) -> tuple[int, int]:
    """The warmer sweep: scan everything missing, purge everything gone.

    Returns ``(rows_now_cached, purged)`` for the caller's logging. Uses
    an unlimited budget — this runs on a background thread on a periodic
    schedule, never on a request path.
    """
    rows = collect(db_dir, cache, inline_scan_limit=-1)
    listing_paths = {row.stats.path for row in rows}
    # The cache file must survive its own purge pass; it is dot-prefixed
    # so it never appears in rows.
    purged = cache.purge_except(listing_paths)
    return len(rows), purged
