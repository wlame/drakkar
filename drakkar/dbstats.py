"""Statistics of this worker's own recorder files for the databases page.

The databases page lists two kinds of files. Sealed files in the shared
history folder carry their statistics in a meta file written once at seal
time, so nothing here touches them. This worker's own files in
``storage.dir`` — the live file and files closed but not sealed into the
history folder yet — have no meta file, so they are measured on request.

The live file grows, so measuring it again on every page load would
re-run ``COUNT(*)`` / ``GROUP BY`` over the whole events table. Instead
the statistics are kept in memory and refreshed with a **delta scan**: the
events table is append-only with a monotonic integer id, so a refresh
aggregates only the rows past the cached ``max_event_id`` and merges them
into the cached counts. A cursor that goes backwards (the file was
replaced) falls back to a full scan.
"""

from __future__ import annotations

import os
import sqlite3
import threading

import structlog

from drakkar.merge import DbStats, _dict_factory, _table_exists, scan_db

logger = structlog.get_logger()

BUSY_TIMEOUT_MS = 5000


class OwnFileStats:
    """In-memory statistics of the worker's own files, refreshed incrementally.

    One instance per UI server; requests run :meth:`measure` from worker
    threads, so the cache is guarded by a lock.
    """

    def __init__(self) -> None:
        self._cached: dict[str, DbStats] = {}
        self._lock = threading.Lock()

    def measure(self, paths: list[str]) -> list[DbStats]:
        """Statistics of ``paths`` in the given order; a file that vanished is skipped. Blocking."""
        with self._lock:
            results: list[DbStats] = []
            for path in paths:
                if not os.path.isfile(path):
                    continue
                cached = self._cached.get(path)
                stats = (_delta_scan(path, cached) if cached is not None else None) or scan_db(path)
                self._cached[path] = stats
                results.append(stats)
            # Files that left the list (sealed and moved, or deleted) are
            # forgotten, so the cache never outgrows the own files.
            for gone in set(self._cached) - {stats.path for stats in results}:
                del self._cached[gone]
            return results

    def cached_paths(self) -> list[str]:
        """The files whose statistics are cached right now."""
        with self._lock:
            return sorted(self._cached)


def _delta_scan(path: str, prev: DbStats) -> DbStats | None:
    """Refresh the statistics of a still-growing recorder file incrementally.

    Aggregates only ``events.id > prev.max_event_id`` and merges into the
    cached counts — exact because the events table is append-only.
    Returns None whenever the increment cannot be trusted (cursor went
    backwards → the path was recreated; table missing; any error) and the
    caller falls back to a full scan.
    """
    if not prev.has_events or prev.max_event_id is None:
        return None
    db: sqlite3.Connection | None = None
    try:
        db = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=BUSY_TIMEOUT_MS / 1000)
        db.row_factory = _dict_factory
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
        # file may have missed it.
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
    except Exception as exc:
        # The caller falls back to a full scan — an optimization only.
        logger.debug('dbstats_delta_scan_failed', path=path, error=str(exc))
        return None
    finally:
        if db is not None:
            db.close()
