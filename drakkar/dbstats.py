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

The memory store of the recorder is not a file. Its row is measured on
request too, with cheap numbers only: :func:`measure_memory_store`.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Callable
from typing import Any

import structlog

from drakkar.merge import DbStats, _dict_factory, _table_exists, scan_db
from drakkar.recorder.memstore import arm_read_limit, is_read_interrupted

logger = structlog.get_logger()

BUSY_TIMEOUT_MS = 5000


class OwnFileStats:
    """In-memory statistics of the worker's own files, refreshed incrementally.

    One instance per API server; requests run :meth:`measure` from worker
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


# Two statements, not one: ``MIN`` alone and ``MAX`` alone are each one
# lookup in the ``ts`` index, both in one SELECT scan the whole index.
FIRST_TS_QUERY = 'SELECT MIN(ts) FROM events'
LAST_TS_QUERY = 'SELECT MAX(ts) FROM events'
EVENT_COUNTS_QUERY = 'SELECT event, COUNT(*) FROM events GROUP BY event'


def measure_memory_store(
    conn: sqlite3.Connection,
    *,
    worker_name: str,
    fallback_cluster: str,
    event_count: int,
    on_read_stopped: Callable[[], None] | None = None,
) -> dict:
    """The memory row of the databases page, read on a by-name connection to the memory store. Blocking.

    ``event_count`` comes from the caller (the id range the store holds), so
    no ``COUNT(*)`` runs here. The first and last ``ts`` come from the
    ``ts`` index, the per-event counts from one ``GROUP BY event`` over the
    event index, and ``size_bytes`` is the data in use (pages in use times
    the page size), not the memory the store holds. ``cluster_name`` comes
    from the store's ``worker_config`` row, else ``fallback_cluster``. The
    reads of the events and config tables run under the memory read limit,
    and a read it stops answers its empty part (no timestamp, ``{}``, the
    fallback cluster) and calls ``on_read_stopped``; the table probe and the
    page pragmas are single lookups.
    """

    def bounded(query: str) -> list[tuple] | None:
        rows = _bounded_rows(conn, query)
        if rows is None and on_read_stopped is not None:
            on_read_stopped()
        return rows

    first_ts = _single_value(bounded(FIRST_TS_QUERY))
    last_ts = _single_value(bounded(LAST_TS_QUERY))
    counts = bounded(EVENT_COUNTS_QUERY)
    cluster_name = fallback_cluster
    if _table_exists(conn, 'worker_config'):
        configured = _single_value(bounded('SELECT cluster_name FROM worker_config WHERE id = 1'))
        if configured:
            cluster_name = configured
    return {
        'filename': f'{worker_name}-memory',
        'worker_name': worker_name,
        'cluster_name': cluster_name or None,
        'kind': 'memory',
        'event_count': event_count,
        'event_counts': dict(counts or []),
        'first_event_ts': first_ts,
        'last_event_ts': last_ts,
        'size_bytes': _used_bytes(conn),
    }


def _single_value(rows: list[tuple] | None) -> Any:
    """The first column of the first row; ``None`` without a row."""
    return rows[0][0] if rows else None


def _bounded_rows(conn: sqlite3.Connection, query: str) -> list[tuple] | None:
    """The rows of ``query`` under the memory read limit; ``None`` when the limit stopped it."""
    arm_read_limit(conn)
    try:
        return conn.execute(query).fetchall()
    except sqlite3.OperationalError as exc:
        if is_read_interrupted(exc):
            return None
        raise
    finally:
        # Disarmed again: the limit bounds this statement only.
        conn.set_progress_handler(None, 0)


def _used_bytes(conn: sqlite3.Connection) -> int:
    page_count, freelist_count, page_size = (
        conn.execute(f'PRAGMA {pragma}').fetchone()[0] for pragma in ('page_count', 'freelist_count', 'page_size')
    )
    return (page_count - freelist_count) * page_size
