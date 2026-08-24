"""Read side of the flight recorder: every SELECT it answers.

The recorder writes; this module reads. Split out of
:mod:`drakkar.recorder.core` so the queries can be exercised against a
plain SQLite file with no recorder, no event loop wiring and no UI server,
and so the Go backend has one module to diff its own queries against
instead of hunting them through a 3,000-line class.

Two families live here:

- **local** — ``get_events``, ``get_trace``, ``get_task_events``,
  ``get_partition_summary``: straight reads of this worker's database
  through the recorder's dedicated reader connection.
- **cross-worker** — ``cross_trace``, ``cross_trace_by_label``,
  ``discover_workers``: bounded sweeps over the peer databases in
  ``db_dir``, each opened read-only and skipped if it is locked, corrupt,
  or belongs to another cluster.
"""

from __future__ import annotations

import asyncio
import glob
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiosqlite
import structlog

from drakkar.config import UIConfig, UIRecorderConfig
from drakkar.peer_discovery import discover_peer_dbs
from drakkar.recorder.schema import _LABEL_TRACE_QUERY, _TRACE_QUERY

logger = structlog.get_logger()

# Cross-worker sweep bounds. A cross-trace MISS — the common case when an
# operator pastes an offset that is not in this cluster — walks every
# candidate database in db_dir, opening each and running a query. db_dir
# defaults to /tmp and is shared by co-located workers, so it accumulates live
# and rotated files from every worker on the host. The sweep is reached from a
# UI request but executes on the MAIN loop (UI reads of live state hop there),
# so an unbounded scan stalls Kafka polling — and it is trivially repeatable
# by refreshing the page. Values match the Go backend.
CROSS_TRACE_MAX_FILES = 64
CROSS_TRACE_BUDGET_SECONDS = 5.0


class _ScanBudget:
    """Bounds a cross-worker sweep in file count and wall-clock time.

    Records whether it stopped early so the caller can say so, rather than
    silently reporting "not found" for data that was never actually reached —
    a distinction that matters during an incident.
    """

    def __init__(self) -> None:
        self._deadline = time.monotonic() + CROSS_TRACE_BUDGET_SECONDS
        self._remaining = CROSS_TRACE_MAX_FILES
        self.truncated = False

    def allow(self) -> bool:
        """Whether one more database file may be opened (consuming budget)."""
        if self._remaining <= 0 or time.monotonic() > self._deadline:
            self.truncated = True
            return False
        self._remaining -= 1
        return True

    def report(self, op: str) -> None:
        """Log once if the sweep stopped early."""
        if not self.truncated:
            return
        logger.warning(
            'cross_trace_scan_truncated',
            category='recorder',
            op=op,
            max_files=CROSS_TRACE_MAX_FILES,
            budget_seconds=CROSS_TRACE_BUDGET_SECONDS,
            hint='result may be incomplete; narrow db_dir or prune rotated databases',
        )


@dataclass(frozen=True)
class QueryContext:
    """What the query layer needs from the recorder that owns it.

    Deliberately a handful of values and three callables rather than the
    recorder itself: a test can build one over a bare aiosqlite connection,
    and nothing here can reach back into the write path.

    ``reader`` and ``db_path`` are callables, not values, because both
    change under the query layer's feet — the recorder swaps its
    connections and its file on every rotation.

    ``flush`` makes events recorded a moment ago visible to a SELECT; the
    trace queries call it so an operator never sees a half-written
    lifecycle.
    """

    config: UIConfig
    worker_name: str
    cluster_name: str
    reader: Callable[[], aiosqlite.Connection | None]
    db_path: Callable[[], str]
    flush: Callable[[], Awaitable[None]]

    @property
    def store(self) -> UIRecorderConfig:
        """The persistence tier (``ui.recorder.*``)."""
        return self.config.recorder


async def _rows_as_dicts(cursor: aiosqlite.Cursor) -> list[dict]:
    """Materialize an open cursor as one dict per row, keyed by column name."""
    columns = [d[0] for d in cursor.description]
    rows = await cursor.fetchall()
    return [dict(zip(columns, row, strict=False)) for row in rows]


class EventQueries:
    """Every read the recorder answers, local and cross-worker."""

    def __init__(self, ctx: QueryContext) -> None:
        self._ctx = ctx

    @property
    def _reader(self) -> aiosqlite.Connection | None:
        """The connection reads go through, or None when there is none.

        Reads use the dedicated reader connection so they don't serialize
        behind buffered-event flushes on the writer; the recorder falls
        back to the writer when no reader is available (e.g. legacy tests
        that set ``_db`` only).
        """
        return self._ctx.reader()

    def _readable(self) -> aiosqlite.Connection | None:
        """The reader connection, or None when events are not queryable."""
        reader = self._reader
        return reader if reader and self._ctx.store.store_events else None

    async def get_events(
        self,
        partition: int | None = None,
        event_type: str | None = None,
        since: float | None = None,
        origin: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        # Reads go through the dedicated reader connection so they don't
        # serialize behind buffered-event flushes on the writer; fall back
        # to the writer when the reader isn't available (e.g. legacy tests
        # that set ``_db`` only).
        reader = self._readable()
        if reader is None:
            return []
        conditions = []
        params: list = []
        if partition is not None:
            conditions.append('partition = ?')
            params.append(partition)
        if event_type:
            conditions.append('event = ?')
            params.append(event_type)
        if since:
            conditions.append('ts >= ?')
            params.append(since)
        # ``origin`` filter — debug UI uses
        # it to split the history page between Kafka-origin tasks and
        # HTTP-origin webapp requests. Indexed via ``idx_events_origin``.
        if origin:
            conditions.append('origin = ?')
            params.append(origin)
        where = f'WHERE {" AND ".join(conditions)}' if conditions else ''
        query = f'SELECT * FROM events {where} ORDER BY id DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        async with reader.execute(query, params) as cursor:
            return await _rows_as_dicts(cursor)

    async def trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Find all events for tasks matching a label key-value pair."""
        await self._ctx.flush()
        reader = self._readable()
        if reader is None:
            return []
        json_path = f'$.{label_key}'
        async with reader.execute(_LABEL_TRACE_QUERY, [json_path, label_value]) as cursor:
            return await _rows_as_dicts(cursor)

    async def cross_trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Trace by label across all workers in the same cluster."""
        local_events = await self.trace_by_label(label_key, label_value)
        for ev in local_events:
            ev['worker_name'] = self._ctx.worker_name

        if local_events:
            return sorted(local_events, key=lambda e: e.get('ts', 0))

        # Fallback: other workers' live DBs. Same shape as ``cross_trace`` —
        # the blocking directory walk runs in a thread (inline it would stall
        # the main loop for the whole sweep), and the sweep is bounded so a
        # directory full of peers cannot pin the caller.
        if not self._ctx.store.db_dir:
            return []

        json_path = f'$.{label_key}'
        budget = _ScanBudget()
        live_targets = await asyncio.to_thread(self._enumerate_peer_live_dbs)
        for target in live_targets:
            if not budget.allow():
                break
            events = await self._query_db_file(target, _LABEL_TRACE_QUERY, [json_path, label_value])
            if events:
                return sorted(events, key=lambda e: e.get('ts', 0))

        budget.report('cross_trace_by_label')
        return []

    async def get_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Get the full lifecycle of a message by partition and offset."""
        await self._ctx.flush()
        reader = self._readable()
        if reader is None:
            return []
        async with reader.execute(
            _TRACE_QUERY, [partition, msg_offset, partition, msg_offset, partition, msg_offset]
        ) as cursor:
            return await _rows_as_dicts(cursor)

    async def _trace_db_file(
        self,
        db_path: str,
        partition: int,
        msg_offset: int,
    ) -> list[dict]:
        """Run the partition+offset trace query against a DB file."""
        params = [partition, msg_offset, partition, msg_offset, partition, msg_offset]
        return await self._query_db_file(db_path, _TRACE_QUERY, params)

    async def _query_db_file(self, db_path: str, query: str, params: list) -> list[dict]:
        """Run a trace query against a peer DB file read-only.

        Returns matching events tagged with the peer's ``worker_name`` (read
        from its ``worker_config``), or ``[]`` when the file is unreadable,
        lacks an ``events`` table, or belongs to a different cluster.
        """
        try:
            async with aiosqlite.connect(f'file:{db_path}?mode=ro', uri=True) as db:
                # Read worker_name and check cluster membership from worker_config
                worker_name = os.path.basename(db_path)
                async with db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='worker_config'"
                ) as cur:
                    if await cur.fetchone():
                        async with db.execute(
                            'SELECT worker_name, cluster_name FROM worker_config WHERE id = 1'
                        ) as cfg_cur:
                            cfg_row = await cfg_cur.fetchone()
                            if cfg_row:
                                worker_name = cfg_row[0]
                                if self._ctx.cluster_name and cfg_row[1] != self._ctx.cluster_name:
                                    return []

                # Check events table exists
                async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
                    if not await cur.fetchone():
                        return []

                async with db.execute(query, params) as cur:
                    events = await _rows_as_dicts(cur)

                for ev in events:
                    ev['worker_name'] = worker_name
                return events
        except Exception:
            # A locked, corrupt, or vanished peer DB must not poison the
            # cross-worker sweep — skip it and let the caller keep scanning.
            return []

    async def cross_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Trace a message across all workers in the same cluster.

        Search order:
        1. Current worker's live DB
        2. Other workers' live DBs (same cluster)
        3. Rotated (historical) DB files in db_dir, newest first

        Every returned event carries a ``worker_name`` field.
        """
        # 1. Search current worker's live DB
        local_events = await self.get_trace(partition, msg_offset)
        for ev in local_events:
            ev['worker_name'] = self._ctx.worker_name
        if local_events:
            return local_events

        if not self._ctx.store.db_dir:
            return []

        searched_paths: set[str] = set()
        db_path = self._ctx.db_path()
        if db_path:
            searched_paths.add(os.path.realpath(db_path))

        # 2. Fallback: other workers' live DBs. Sorted so the first-match
        # scan visits peers in the same deterministic order as the Go
        # backend (filepath.Glob returns sorted paths).
        budget = _ScanBudget()

        # The directory walks below are synchronous stat/readlink syscalls, and
        # this coroutine runs on the MAIN loop — inline they stall Kafka
        # polling for the whole sweep, unboundedly on a slow or stale mount.
        # Enumeration is offloaded; only the aiosqlite queries stay here.
        live_targets = await asyncio.to_thread(self._enumerate_peer_live_dbs)
        for target in live_targets:
            searched_paths.add(target)
            if not budget.allow():
                break
            events = await self._trace_db_file(target, partition, msg_offset)
            if events:
                return events

        # 3. Fallback: rotated DB files (newest first)
        rotated = await asyncio.to_thread(self._enumerate_rotated_dbs, searched_paths)
        for full in rotated:
            if not budget.allow():
                break
            events = await self._trace_db_file(full, partition, msg_offset)
            if events:
                return events

        budget.report('cross_trace')
        return []

    def _enumerate_peer_live_dbs(self) -> list[str]:
        """Resolve other workers' live-DB symlinks. Blocking; call in a thread.

        Sorted so the first-match scan visits peers in the same deterministic
        order as the Go backend (filepath.Glob returns sorted paths).
        """
        targets: list[str] = []
        live_pattern = os.path.join(self._ctx.store.db_dir, '*-live.db')
        for link_path in sorted(glob.glob(live_pattern)):
            if not os.path.islink(link_path):
                continue
            link_name = os.path.basename(link_path)
            if link_name.removesuffix('-live.db') == self._ctx.worker_name:
                continue
            targets.append(os.path.realpath(link_path))
        return targets

    def _enumerate_rotated_dbs(self, searched_paths: set[str]) -> list[str]:
        """List rotated DB files newest first. Blocking; call in a thread."""
        all_dbs = []
        for entry in os.listdir(self._ctx.store.db_dir):
            if not entry.endswith('.db'):
                continue
            full = os.path.join(self._ctx.store.db_dir, entry)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            if os.path.realpath(full) in searched_paths:
                continue
            all_dbs.append((entry, full))
        # Newest first (the timestamp is in the filename).
        all_dbs.sort(key=lambda x: x[0], reverse=True)
        return [full for _entry, full in all_dbs]

    async def get_task_events(self, task_id: str) -> list[dict]:
        """Get all events for a specific task_id, ordered chronologically."""
        await self._ctx.flush()  # ensure recent events are queryable
        reader = self._readable()
        if reader is None:
            return []
        query = 'SELECT * FROM events WHERE task_id = ? ORDER BY id ASC'
        async with reader.execute(query, [task_id]) as cursor:
            return await _rows_as_dicts(cursor)

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        reader = self._readable()
        if reader is None:
            return []
        query = """
            SELECT
                partition,
                MAX(CASE WHEN event = 'consumed' THEN ts END) as last_consumed,
                MAX(CASE WHEN event = 'committed' THEN ts END) as last_committed,
                MAX(CASE WHEN event = 'committed' THEN offset END) as last_committed_offset,
                COUNT(CASE WHEN event = 'consumed' THEN 1 END) as consumed_count,
                COUNT(CASE WHEN event = 'task_completed' THEN 1 END) as completed_count,
                COUNT(CASE WHEN event = 'task_failed' THEN 1 END) as failed_count
            FROM events
            WHERE partition IS NOT NULL
            GROUP BY partition
            ORDER BY partition
        """
        async with reader.execute(query) as cursor:
            return await _rows_as_dicts(cursor)

    # --- Autodiscovery ---

    # Where a peer's "last heartbeat" timestamp comes from, in preference
    # order. ``worker_state.updated_at`` is written every
    # ``state_sync_interval_seconds``; peers with ``store_state: false``
    # fall back to the newest event timestamp. Both columns are indexed
    # (idx_worker_state_updated, idx_events_ts), so each MAX() is an O(1)
    # index-tail lookup, not a table scan.
    _PEER_LAST_SEEN_SOURCES: tuple[tuple[str, str], ...] = (
        ('worker_state', 'updated_at'),
        ('events', 'ts'),
    )

    @classmethod
    async def _peer_last_seen(cls, db: aiosqlite.Connection) -> float | None:
        """Newest heartbeat timestamp found in a peer DB, or None when neither
        source table has a row (or exists at all)."""
        for table, column in cls._PEER_LAST_SEEN_SOURCES:
            try:
                # f-string SQL is safe here: table/column come from the
                # constant _PEER_LAST_SEEN_SOURCES tuple, never from input.
                async with db.execute(f'SELECT MAX({column}) FROM {table}') as cur:
                    row = await cur.fetchone()
            except Exception:
                continue  # table missing in this peer's DB — try the next source
            if row and row[0] is not None:
                return float(row[0])
        return None

    async def discover_workers(self) -> list[dict]:
        """Scan db_dir for other workers' -live.db symlinks, read their worker_config.

        Symlink scanning is delegated to :func:`discover_peer_dbs`
        (shared with the cache peer-sync loop). We keep the recorder-specific
        step — reading the `worker_config` row out of each resolved DB —
        right here; the cache will supply its own row-reader in a later task.

        Each returned dict carries two liveness fields on top of the raw
        ``worker_config`` columns:

        - ``last_seen_ts`` — newest heartbeat timestamp (see
          :meth:`_peer_last_seen`), or ``None`` when no source is available;
        - ``online`` — True when that heartbeat is no older than
          ``ui.workers_offline_after_seconds``. A crashed or OOM-killed
          worker leaves its ``-live.db`` symlink behind, so it stays listed
          — this flag is what tells the UI it is gone.
        """
        if not self._ctx.store.db_dir or not self._ctx.store.store_config:
            return []
        threshold = self._ctx.config.workers_offline_after_seconds
        now = time.time()
        workers: list[dict] = []
        async for peer_name, target in discover_peer_dbs(
            self._ctx.store.db_dir,
            '-live.db',
            self._ctx.worker_name,
        ):
            try:
                async with aiosqlite.connect(f'file:{target}?mode=ro', uri=True) as db:
                    async with db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='worker_config'"
                    ) as cur:
                        if not await cur.fetchone():
                            continue
                    async with db.execute('SELECT * FROM worker_config WHERE id = 1') as cur:
                        columns = [d[0] for d in cur.description]
                        row = await cur.fetchone()
                        if not row:
                            continue
                        worker = dict(zip(columns, row, strict=False))
                    last_seen = await self._peer_last_seen(db)
                    worker['last_seen_ts'] = last_seen
                    worker['online'] = last_seen is not None and (now - last_seen) <= threshold
                    workers.append(worker)
            except Exception as exc:
                # A locked, corrupt, or vanished peer DB must not poison the
                # whole scan — log it and keep going with the other peers.
                await logger.awarning(
                    'recorder_peer_scan_failed',
                    category='recorder',
                    peer=peer_name,
                    db_path=target,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                continue
        return workers
