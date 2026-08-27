"""Read side of the flight recorder: every SELECT it answers.

The recorder writes; this module reads. Split out of
:mod:`drakkar.recorder.core` so the queries can be exercised against a
plain SQLite file with no recorder, no event loop wiring and no UI server,
rather than hunting them through a 3,000-line class.

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
import json
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

import aiosqlite
import structlog

from drakkar.config import UIConfig, UIRecorderConfig
from drakkar.peer_discovery import discover_peer_dbs
from drakkar.recorder.schema import _LABEL_TRACE_QUERY, _TRACE_QUERY, EventType

logger = structlog.get_logger()

# Cross-worker sweep bounds. A cross-trace MISS — the common case when an
# operator pastes an offset that is not in this cluster — walks every
# candidate database in db_dir, opening each and running a query. db_dir
# defaults to /tmp and is shared by co-located workers, so it accumulates live
# and rotated files from every worker on the host. The sweep is reached from a
# UI request but executes on the MAIN loop (UI reads of live state hop there),
# so an unbounded scan stalls Kafka polling — and it is trivially repeatable
# by refreshing the page.
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
        # scan visits peers in a deterministic order, whatever order the
        # filesystem happens to list them in.
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

        Sorted so the first-match scan visits peers in a deterministic
        order, whatever order the filesystem happens to list them in.
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
        query = f"""
            SELECT
                partition,
                MAX(CASE WHEN event = '{EventType.CONSUMED}' THEN ts END) as last_consumed,
                MAX(CASE WHEN event = '{EventType.COMMITTED}' THEN ts END) as last_committed,
                MAX(CASE WHEN event = '{EventType.COMMITTED}' THEN offset END) as last_committed_offset,
                COUNT(CASE WHEN event = '{EventType.CONSUMED}' THEN 1 END) as consumed_count,
                COUNT(CASE WHEN event = '{EventType.TASK_COMPLETED}' THEN 1 END) as completed_count,
                COUNT(CASE WHEN event = '{EventType.TASK_FAILED}' THEN 1 END) as failed_count
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


# ---------------------------------------------------------------------------
# UI read model
#
# The debug UI answers questions the recorder's own API does not: "what was
# on the timeline in the last two minutes", "what is the state of these
# forty task ids", "how many payloads did this offset produce per sink".
# Each is a SELECT over the same ``events`` table plus a fold of the rows
# into the shape a page renders.
#
# Both halves live here rather than inside the route closures in
# ``drakkar/uiserver/routes_*``. Three reasons: the aggregation can then be
# tested against known rows without building a FastAPI app; schema
# knowledge stops leaking into the presentation layer; and the queries live
# in one module instead of scattered through 900-line closures.
#
# The route keeps what is genuinely its own — parsing query parameters,
# dispatching the read to the main loop, and shaping the JSON response.
#
# The SQL builders return ``(sql, params)`` rather than executing, because
# the UI reads run through ``deps.flush_and_select``: a flush, then a
# SELECT on the recorder's reader connection, dispatched to the main loop
# with a timeout. That execution path belongs to the UI server; the query
# text does not.
# ---------------------------------------------------------------------------

# Events that describe one execution attempt. Every timeline and task-state
# view is a fold over these three.
TASK_LIFECYCLE_EVENTS = (EventType.TASK_STARTED, EventType.TASK_COMPLETED, EventType.TASK_FAILED)

# The same three names as a quoted SQL list. Rendered once from the tuple
# above so a query and the fold that consumes its rows can never disagree
# about which events count as a lifecycle event. Safe to interpolate: the
# values are enum members, never caller input.
_TASK_LIFECYCLE_SQL = ', '.join(f"'{event}'" for event in TASK_LIFECYCLE_EVENTS)

# Columns the timeline needs. Deliberately not ``SELECT *``: ``stdout`` and
# ``stderr`` hold captured subprocess output that no timeline renders, and
# pulling them made the response size track total task output rather than
# task count.
_TIMELINE_COLUMNS = (
    'ts, event, partition, task_id, args, duration, metadata, pid, labels, origin, client_name, request_id, stdout_size'
)

# Columns the task-state lookup needs — the timeline set plus exit_code,
# minus the size fields no caller of that endpoint reads.
_TASK_STATE_COLUMNS = (
    'task_id, event, ts, duration, partition, metadata, exit_code, pid, args, labels, origin, client_name, request_id'
)


def _placeholders(values: Sequence[Any]) -> str:
    """``?,?,?`` for an IN clause. Never interpolates the values themselves."""
    return ','.join(['?'] * len(values))


def events_query(
    *,
    partitions: Sequence[int] | None = None,
    event_types: Sequence[str] | None = None,
    origin: str | None = None,
    after_id: int = 0,
    limit: int = 100,
) -> tuple[str, list[Any]]:
    """The ``/api/v1/events`` listing: newest first, optionally filtered.

    ``after_id`` supports the UI's incremental poll — "everything since the
    id I last saw" — and is ignored when zero. ``origin`` splits
    Kafka-origin tasks from webapp requests (indexed by
    ``idx_events_origin``).
    """
    conditions: list[str] = []
    params: list[Any] = []
    if partitions:
        conditions.append(f'partition IN ({_placeholders(partitions)})')
        params.extend(partitions)
    if event_types:
        conditions.append(f'event IN ({_placeholders(event_types)})')
        params.extend(event_types)
    if origin:
        conditions.append('origin = ?')
        params.append(origin)
    if after_id > 0:
        conditions.append('id > ?')
        params.append(after_id)
    where = f'WHERE {" AND ".join(conditions)}' if conditions else ''
    params.append(limit)
    return f'SELECT * FROM events {where} ORDER BY id DESC LIMIT ?', params


def recent_tasks_query(*, since: float, event_limit: int) -> tuple[str, list[Any]]:
    """Lifecycle events since ``since``, capped at the ``event_limit`` newest.

    The inner query orders DESC to take the most recent rows, and the outer
    re-sorts ASC because the retry grouping in :func:`group_timeline_tasks`
    depends on chronological order.
    """
    query = f"""
            SELECT * FROM (
                SELECT {_TIMELINE_COLUMNS}
                FROM events
                WHERE event IN ({_TASK_LIFECYCLE_SQL})
                AND ts >= ?
                ORDER BY ts DESC
                LIMIT ?
            ) ORDER BY ts ASC
        """
    return query, [since, event_limit]


def task_state_query(task_ids: Sequence[str]) -> tuple[str, list[Any]]:
    """Every lifecycle event for the named tasks, oldest first per task."""
    query = f"""
            SELECT {_TASK_STATE_COLUMNS}
            FROM events
            WHERE task_id IN ({_placeholders(task_ids)})
              AND event IN ({_TASK_LIFECYCLE_SQL})
            ORDER BY task_id, id ASC
        """
    return query, list(task_ids)


def hook_events_query(*, event_name: str, limit: int) -> tuple[str, list[Any]]:
    """The most recent completion-hook events of one kind, newest first."""
    query = (
        'SELECT ts, task_id, partition, offset, duration, metadata FROM events WHERE event = ? ORDER BY id DESC LIMIT ?'
    )
    return query, [event_name, limit]


def sink_breakdown_query(*, partition: int, offsets: Sequence[int]) -> tuple[str, list[Any]]:
    """``produced`` counts per output topic for one partition's offsets."""
    query = (
        f'SELECT output_topic, COUNT(*) as n FROM events '
        f"WHERE event = '{EventType.PRODUCED}' AND partition = ? "
        f'AND offset IN ({_placeholders(offsets)}) GROUP BY output_topic'
    )
    return query, [partition, *offsets]


def parse_json_object(raw: str | None) -> dict:
    """Decode a recorder metadata/labels column, or ``{}``.

    Recorder columns are written by this framework, but a database can be
    hand-edited, truncated mid-write, or written by an older version — and
    a decode failure on one row must not blank a whole page.
    """
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _new_timeline_entry(event: dict) -> dict:
    """One timeline row, built from a ``task_started`` event."""
    metadata = parse_json_object(event.get('metadata'))
    return {
        'task_id': event['task_id'],
        'partition': event.get('partition'),
        'start_ts': event['ts'],
        'end_ts': None,
        'duration': None,
        'status': 'running',
        'args': event.get('args'),
        'pid': event.get('pid'),
        # Populated from the task_completed event; stays null for
        # running and failed tasks.
        'stdout_size': None,
        'slot': metadata.get('slot'),
        'labels': parse_json_object(event.get('labels')) or None,
        'env': metadata.get('env'),
        # Webapp-pipeline columns: ``origin`` defaults to ``'kafka'`` at the
        # schema level, so its absence on an older recorder row still yields
        # a sensible value. ``client_name`` / ``request_id`` are NULL for
        # Kafka tasks.
        'origin': event.get('origin') or 'kafka',
        'client_name': event.get('client_name'),
        'request_id': event.get('request_id'),
    }


def group_timeline_tasks(
    events: Sequence[dict], *, ws_min_duration_seconds: float, limit: int
) -> tuple[list[dict], bool]:
    """Fold lifecycle events into one timeline row per execution attempt.

    ``events`` must be in chronological order.

    Retries produce separate rows: when a second ``task_started`` arrives
    for a task that already has an open row, the earlier attempt is
    archived under a composite key (``<task_id>:r<start_ts>``) and the
    latest attempt keeps the plain ``task_id``, so a live WebSocket event
    still matches the row the UI is drawing. An archived attempt with no
    completion is closed as failed at the moment the retry started —
    without that it would draw as a bar that never ends.

    Fast completed tasks are dropped, matching what the live WebSocket
    stream suppresses at ``ws_min_duration_ms``: a task nobody saw start
    must not appear finishing. Running tasks (duration unknown) and failed
    tasks (always visible) are kept whatever their duration.

    Returns ``(rows, trimmed)`` — ``trimmed`` says the newest ``limit``
    rows by start time were kept and older ones dropped, so the caller can
    tell the UI its window is partial instead of letting it present a
    partial window as complete.
    """
    tasks: dict[str, dict] = {}
    for event in events:
        task_id = event.get('task_id')
        if not task_id:
            continue

        if event['event'] == EventType.TASK_STARTED:
            open_attempt = tasks.get(task_id)
            if open_attempt is not None:
                archive_key = f'{task_id}:r{open_attempt["start_ts"]}'
                tasks[archive_key] = open_attempt
                open_attempt['task_id'] = archive_key
                if open_attempt['end_ts'] is None:
                    open_attempt['end_ts'] = event['ts']
                    open_attempt['status'] = 'failed'
            tasks[task_id] = _new_timeline_entry(event)

        elif event['event'] in (EventType.TASK_COMPLETED, EventType.TASK_FAILED):
            entry = tasks.get(task_id)
            if entry is None:
                continue
            entry['end_ts'] = event['ts']
            entry['status'] = 'completed' if event['event'] == EventType.TASK_COMPLETED else 'failed'
            entry['duration'] = event.get('duration')
            if event.get('pid'):
                entry['pid'] = event['pid']
            if event['event'] == EventType.TASK_COMPLETED:
                entry['stdout_size'] = event.get('stdout_size')
                # Contract v1.16: throughput-counted completions carry
                # cost/speed in their metadata; surface them on the row so
                # the timeline shows per-task speed without re-deriving.
                # Absent for excluded tasks.
                metadata = parse_json_object(event.get('metadata'))
                if 'speed' in metadata:
                    entry['cost'] = metadata.get('cost')
                    entry['speed'] = metadata.get('speed')

    rows = [
        entry
        for entry in tasks.values()
        if entry['start_ts']
        and not (
            entry['status'] == 'completed'
            and entry['duration'] is not None
            and entry['duration'] < ws_min_duration_seconds
        )
    ]

    trimmed = len(rows) > limit
    if trimmed:
        rows.sort(key=lambda row: row['start_ts'])
        rows = rows[-limit:]
    return rows, trimmed


def _new_task_state(task_id: str) -> dict:
    return {
        'task_id': task_id,
        'status': 'unknown',
        'start_ts': None,
        'end_ts': None,
        'duration': None,
        'partition': None,
        'source_offsets': None,
        'pid': None,
        'args': None,
        'labels': None,
        'exit_code': None,
        # Webapp-pipeline columns. The first event row to populate them
        # wins (origin is NOT NULL with default 'kafka', so it is always
        # set; client_name / request_id are NULL for Kafka tasks).
        'origin': 'kafka',
        'client_name': None,
        'request_id': None,
    }


def group_task_states(events: Sequence[dict]) -> dict[str, dict]:
    """Fold lifecycle events into one current-state row per task id.

    Unlike :func:`group_timeline_tasks` this collapses retries: a task that
    was retried reports the outcome of its latest attempt, because the
    caller asked "what is the state of this task", not "draw me every
    attempt". Tasks with no events are simply absent from the result —
    callers treat a missing key as "not in the database yet".
    """
    by_id: dict[str, dict] = {}
    for event in events:
        task_id = event['task_id']
        state = by_id.setdefault(task_id, _new_task_state(task_id))

        if event['event'] == EventType.TASK_STARTED:
            state['start_ts'] = event['ts']
            # ``running`` is provisional — overwritten below if a
            # completion event exists for the same task id.
            if state['status'] == 'unknown':
                state['status'] = 'running'
            state['partition'] = event.get('partition')
            state['pid'] = event.get('pid')
            state['args'] = event.get('args')
            # Origin / client_name / request_id propagate from the
            # task_started row (every recorder write site populates them).
            # Last write wins on retries, matching ``pid``.
            for column in ('origin', 'client_name', 'request_id'):
                if event.get(column):
                    state[column] = event[column]
            state['source_offsets'] = parse_json_object(event.get('metadata')).get('source_offsets')
            state['labels'] = parse_json_object(event.get('labels')) or None

        elif event['event'] in (EventType.TASK_COMPLETED, EventType.TASK_FAILED):
            state['end_ts'] = event['ts']
            state['status'] = 'completed' if event['event'] == EventType.TASK_COMPLETED else 'failed'
            state['duration'] = event.get('duration')
            state['exit_code'] = event.get('exit_code')
            if event.get('pid'):
                state['pid'] = event['pid']
    return by_id


def count_by_topic(rows: Sequence[Sequence[Any]]) -> dict[str, int]:
    """Fold ``(output_topic, count)`` rows into a map, naming the unnamed.

    A ``produced`` event with no ``output_topic`` predates per-sink
    attribution; it is counted under ``(unknown)`` rather than dropped, so
    the totals still add up.
    """
    counts: dict[str, int] = {}
    for topic, count in rows:
        counts[topic or '(unknown)'] = int(count)
    return counts


def task_exec_state_query(task_ids: Sequence[str]) -> tuple[str, list[Any]]:
    """The execution facts the completion-hook feeds pair with each row.

    One query across all three lifecycle events: ``task_started`` carries
    ``source_offsets`` in its metadata (what the UI renders as the message
    source), while the completion events carry the subprocess outcome.
    """
    query = (
        f'SELECT task_id, event, duration, exit_code, metadata '
        f'FROM events WHERE task_id IN ({_placeholders(task_ids)}) '
        f'AND event IN ({_TASK_LIFECYCLE_SQL})'
    )
    return query, list(task_ids)


def group_task_exec_state(events: Sequence[dict]) -> dict[str, dict]:
    """Fold :func:`task_exec_state_query` rows into one summary per task id.

    Retries collapse last-write-wins, matching the rest of the UI's
    "current state of this task" views.
    """
    by_id: dict[str, dict] = {}
    for event in events:
        entry = by_id.setdefault(
            event['task_id'],
            {'exec_duration': None, 'status': None, 'exit_code': None, 'source_offsets': None},
        )
        if event['event'] == EventType.TASK_STARTED:
            source_offsets = parse_json_object(event.get('metadata')).get('source_offsets')
            if isinstance(source_offsets, list):
                entry['source_offsets'] = source_offsets
        else:
            entry['exec_duration'] = event.get('duration')
            entry['status'] = 'completed' if event['event'] == EventType.TASK_COMPLETED else 'failed'
            entry['exit_code'] = event.get('exit_code')
    return by_id


def base_task_id(task_id: str) -> str:
    """Strip the timeline's retry composite key: ``t-abc:r1234.5`` -> ``t-abc``.

    The timeline archives earlier attempts under a composite key (see
    :func:`group_timeline_tasks`), and the UI links to those keys — but
    the recorder only ever wrote the base id.
    """
    return task_id.split(':r')[0]


def _parse_json_value(raw: str | None, fallback: Any = None) -> Any:
    """Decode a JSON column that may hold any type, or return ``fallback``."""
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return fallback


def build_task_detail(task_id: str, events: Sequence[dict]) -> dict:
    """Reconstruct one task's lifecycle from every event recorded for it.

    ``task_id`` is echoed back exactly as asked for — the caller may have
    passed a retry composite key — while the events are whatever the
    recorder holds for the base id.

    Duration prefers the recorded value and falls back to the gap between
    the start and the finish, so a task whose completion row predates the
    duration column still shows a span.
    """
    started = next((event for event in events if event['event'] == EventType.TASK_STARTED), None)
    completed = next((event for event in events if event['event'] == EventType.TASK_COMPLETED), None)
    failed = next((event for event in events if event['event'] == EventType.TASK_FAILED), None)
    finished = completed or failed

    duration = finished['duration'] if finished and finished.get('duration') else None
    if not duration and started and finished:
        duration = finished['ts'] - started['ts']

    start_metadata = parse_json_object(started.get('metadata')) if started else {}
    # ``args`` is a JSON list, but an older row may hold the raw string —
    # show it rather than nothing.
    args = _parse_json_value(started.get('args'), fallback=started.get('args')) if started else None

    # ``origin`` is on every event row; HTTP-origin tasks carry the
    # ``client_name`` / ``request_id`` columns too. The page uses these to
    # swap the Partition/Offset header for Client/Request ID. Last non-empty
    # value wins.
    origin = 'kafka'
    client_name = None
    request_id = None
    for event in events:
        origin = event.get('origin') or origin
        client_name = event.get('client_name') or client_name
        request_id = event.get('request_id') or request_id

    webapp_request_body, webapp_response_body = _webapp_bodies(events) if origin == 'http' else (None, None)

    return {
        'task_id': task_id,
        'events': list(events),
        'started': started,
        'completed': completed,
        'failed': failed,
        'duration': duration,
        'source_offsets': start_metadata.get('source_offsets'),
        'args': args,
        'labels': parse_json_object(started.get('labels')) or None if started else None,
        'task_env': start_metadata.get('env'),
        'partition': started.get('partition') if started else None,
        'pid': (completed or failed or {}).get('pid') or (started or {}).get('pid'),
        'exit_code': finished.get('exit_code') if finished else None,
        'origin': origin,
        'client_name': client_name,
        'request_id': request_id,
        'webapp_request_body': webapp_request_body,
        'webapp_response_body': webapp_response_body,
    }


def _webapp_bodies(events: Sequence[dict]) -> tuple[Any, Any]:
    """The captured request and response bodies of an HTTP-origin task.

    Both come from the ``webapp_request_received`` /
    ``webapp_request_completed`` rows. When the recorder logged only the
    request size and not the payload, the request half reports that size
    with ``recorded: False`` so the page can say "body not recorded"
    rather than showing nothing at all.
    """
    request_body = None
    response_body = None
    for event in events:
        if event['event'] == EventType.WEBAPP_REQUEST_RECEIVED:
            metadata = parse_json_object(event.get('metadata'))
            request_body = metadata.get('body')
            if request_body is None and metadata.get('body_bytes') is not None:
                request_body = {'body_bytes': metadata['body_bytes'], 'recorded': False}
        elif event['event'] == EventType.WEBAPP_REQUEST_COMPLETED:
            response_body = parse_json_object(event.get('metadata')).get('response')
    return request_body, response_body


def consumed_timestamps_query(pairs: Sequence[tuple[int, int]]) -> tuple[str, list[Any]]:
    """``consumed`` timestamps for a set of ``(partition, offset)`` pairs.

    Filters on the two dimensions separately rather than on the pairs
    themselves — SQLite has no row-value IN — so the result is a superset
    that :func:`index_consumed_timestamps` narrows by exact key. That is
    still far cheaper than a query per pair, which is what a page showing
    200 message rows would otherwise issue.
    """
    partitions = sorted({partition for partition, _ in pairs})
    offsets = sorted({offset for _, offset in pairs})
    query = (
        f'SELECT partition, offset, ts FROM events '
        f"WHERE event = '{EventType.CONSUMED}' "
        f'AND partition IN ({_placeholders(partitions)}) AND offset IN ({_placeholders(offsets)})'
    )
    return query, [*partitions, *offsets]


def index_consumed_timestamps(rows: Sequence[Sequence[Any]]) -> dict[tuple[int, int], list[float]]:
    """Group ``(partition, offset, ts)`` rows by their exact key.

    A message can be consumed more than once (redelivery after a restart or
    a rebalance), so each key holds a list — the caller picks the right
    one, see :func:`end_to_end_seconds`.
    """
    by_key: dict[tuple[int, int], list[float]] = {}
    for partition, offset, ts in rows:
        by_key.setdefault((partition, offset), []).append(ts)
    return by_key


def end_to_end_seconds(consumed_timestamps: Sequence[float] | None, completed_ts: float) -> float | None:
    """How long this delivery of the message took, or None if unknown.

    Picks the most recent consume at or before the completion. Anything
    later belongs to a redelivery that has not finished yet, and pairing
    with it would report a negative duration.
    """
    if not consumed_timestamps:
        return None
    started = max((ts for ts in consumed_timestamps if ts <= completed_ts), default=None)
    return None if started is None else completed_ts - started


# ``(result key, event names)`` for the webapp dashboard's 60-second tiles.
# One row per tile so a new outcome class is one entry, not another
# copy-pasted query/unpack pair.
WEBAPP_RATE_TILES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('success_60s', (EventType.WEBAPP_REQUEST_COMPLETED,)),
    ('error_60s', (EventType.WEBAPP_REQUEST_TIMEOUT, EventType.WEBAPP_REQUEST_DROPPED_AFTER_TIMEOUT)),
    ('rejected_60s', (EventType.WEBAPP_REQUEST_RATE_LIMITED, EventType.WEBAPP_REQUEST_AUTH_FAILED)),
)


def event_count_query(*, event_names: Sequence[str], since: float) -> tuple[str, list[Any]]:
    """How many of the named events landed since ``since``."""
    query = f'SELECT COUNT(*) FROM events WHERE event IN ({_placeholders(event_names)}) AND ts >= ?'
    return query, [*event_names, since]
