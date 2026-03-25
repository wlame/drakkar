"""Flight recorder — event log to timestamped SQLite files."""

from __future__ import annotations

import asyncio
import glob
import json
import os
import queue
import socket
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite
import structlog
from pydantic import BaseModel

from drakkar.config import DebugConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig

logger = structlog.get_logger()

SCHEMA_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    dt          TEXT    NOT NULL,
    event       TEXT    NOT NULL,
    partition   INTEGER,
    offset      INTEGER,
    task_id     TEXT,
    args        TEXT,
    stdout_size INTEGER DEFAULT 0,
    stdout      TEXT,
    stderr      TEXT,
    exit_code   INTEGER,
    duration    REAL,
    output_topic TEXT,
    metadata    TEXT,
    pid         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_partition_offset ON events(partition, offset);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_dt ON events(dt);
CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event);
"""

SCHEMA_WORKER_CONFIG = """
CREATE TABLE IF NOT EXISTS worker_config (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    worker_name     TEXT NOT NULL,
    cluster_name    TEXT,
    ip_address      TEXT,
    debug_port      INTEGER,
    debug_url       TEXT,
    kafka_brokers   TEXT,
    source_topic    TEXT,
    consumer_group  TEXT,
    binary_path     TEXT,
    max_workers     INTEGER,
    task_timeout_seconds INTEGER,
    max_retries     INTEGER,
    window_size     INTEGER,
    sinks_json      TEXT,
    env_vars_json   TEXT,
    created_at      REAL NOT NULL,
    created_at_dt   TEXT NOT NULL
);
"""

SCHEMA_WORKER_STATE = """
CREATE TABLE IF NOT EXISTS worker_state (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    uptime_seconds      REAL,
    assigned_partitions TEXT,
    partition_count     INTEGER,
    pool_active         INTEGER,
    pool_max            INTEGER,
    total_queued        INTEGER,
    consumed_count      INTEGER,
    completed_count     INTEGER,
    failed_count        INTEGER,
    produced_count      INTEGER,
    committed_count     INTEGER,
    paused              INTEGER,
    updated_at          REAL NOT NULL,
    updated_at_dt       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_worker_state_updated ON worker_state(updated_at);
"""

_TRACE_QUERY = """
    SELECT * FROM events
    WHERE partition = ? AND (
        offset = ?
        OR task_id IN (
            SELECT e.task_id FROM events e, json_each(json_extract(e.metadata, '$.source_offsets')) j
            WHERE e.partition = ? AND e.event = 'task_started'
            AND j.value = ?
        )
    )
    ORDER BY id ASC
"""


def _format_dt(ts: float) -> str:
    """Format a Unix timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'."""
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f'{dt.microsecond // 1000:03d}'


def _make_db_path(db_dir: str, worker_name: str) -> str:
    """Generate a timestamped DB filename inside db_dir.

    ('/shared', 'worker-1') -> '/shared/worker-1-2026-03-16__14_55_00.db'
    """
    ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
    return str(Path(db_dir) / f'{worker_name}-{ts}.db')


def _live_link_path(db_dir: str, worker_name: str) -> str:
    """Path for the live symlink: {db_dir}/{worker_name}-live.db."""
    return str(Path(db_dir) / f'{worker_name}-live.db')


def _list_db_files(db_dir: str, worker_name: str) -> list[str]:
    """List all timestamped DB files for a worker, oldest first.

    Excludes the -live.db symlink.
    """
    pattern = str(Path(db_dir) / f'{worker_name}-*.db')
    live = _live_link_path(db_dir, worker_name)
    files = [f for f in glob.glob(pattern) if f != live and not os.path.islink(f)]
    files.sort()
    return files


def detect_worker_ip() -> str:
    """Detect the worker's outbound IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


class EventRecorder:
    """Records processing events to timestamped SQLite database files.

    Events are buffered in memory and flushed periodically. On retention
    check, the current DB is finalized and a new timestamped file is
    created. Old DB files beyond retention_hours are deleted.

    A ``{worker_name}-live.db`` symlink points to the current database
    while the worker is running and is removed on graceful shutdown.

    Which tables are created depends on config flags:
    - ``store_events`` -> ``events`` table
    - ``store_config`` -> ``worker_config`` table (enables autodiscovery)
    - ``store_state`` -> ``worker_state`` table (periodic snapshots)
    """

    MAX_BUFFER = 50_000  # default, overridden by config.debug.max_buffer

    def __init__(self, config: DebugConfig, worker_name: str = 'worker', cluster_name: str = '') -> None:
        self._config = config
        self._worker_name = worker_name
        self._cluster_name = cluster_name
        self._buffer: deque[dict] = deque(maxlen=config.max_buffer)
        self._db: aiosqlite.Connection | None = None
        self._db_path: str = ''
        self._flush_task: asyncio.Task | None = None
        self._retention_task: asyncio.Task | None = None
        self._state_task: asyncio.Task | None = None
        self._running = False
        self._ws_subscribers: set[queue.Queue] = set()
        self._state_provider: Callable[[], dict] | None = None
        self._drakkar_config: DrakkarConfig | None = None
        # In-memory counters (used for worker_state regardless of store_events)
        self._counters = {
            'consumed': 0,
            'completed': 0,
            'failed': 0,
            'produced': 0,
            'committed': 0,
        }

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def counters(self) -> dict[str, int]:
        return dict(self._counters)

    def set_state_provider(self, provider: Callable[[], dict]) -> None:
        """Set callback that returns current worker state (uptime, partitions, pool)."""
        self._state_provider = provider

    async def _create_schema(self) -> None:
        """Create tables based on config flags."""
        if not self._db:
            return
        if self._config.store_events:
            await self._db.executescript(SCHEMA_EVENTS)
        if self._config.store_config:
            await self._db.executescript(SCHEMA_WORKER_CONFIG)
        if self._config.store_state:
            await self._db.executescript(SCHEMA_WORKER_STATE)
        await self._db.commit()

    async def start(self) -> None:
        self._running = True
        if self._config.db_dir:
            self._db_path = _make_db_path(self._config.db_dir, self._worker_name)
            self._db = await aiosqlite.connect(self._db_path)
            await self._create_schema()
            self._update_live_link()
            if self._config.store_events:
                self._flush_task = asyncio.create_task(self._flush_loop())
            self._retention_task = asyncio.create_task(self._retention_loop())
            if self._config.store_state:
                self._state_task = asyncio.create_task(self._state_sync_loop())
        await logger.ainfo(
            'recorder_started',
            category='recorder',
            db_path=self._db_path or '(memory only)',
        )

    def subscribe(self) -> queue.Queue:
        """Subscribe to live event stream. Returns a thread-safe queue."""
        q: queue.Queue = queue.Queue(maxsize=10_000)
        self._ws_subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        """Unsubscribe from live event stream."""
        self._ws_subscribers.discard(q)

    def _record(self, event: dict) -> None:
        """Append event to buffer and broadcast to WS subscribers."""
        event['dt'] = _format_dt(event['ts'])
        self._buffer.append(event)
        if self._ws_subscribers:
            for q in self._ws_subscribers:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    pass

    def _update_live_link(self) -> None:
        """Create or update the {worker}-live.db symlink to the current DB."""
        if not self._config.db_dir or not self._db_path:
            return
        link = _live_link_path(self._config.db_dir, self._worker_name)
        target = os.path.basename(self._db_path)
        try:
            tmp = link + '.tmp'
            os.symlink(target, tmp)
            os.replace(tmp, link)
        except OSError:
            pass

    def _remove_live_link(self) -> None:
        """Remove the live symlink on graceful shutdown."""
        if not self._config.db_dir:
            return
        link = _live_link_path(self._config.db_dir, self._worker_name)
        try:
            if os.path.islink(link):
                os.remove(link)
        except OSError:
            pass

    # --- Worker config (autodiscovery) ---

    async def write_config(self, drakkar_config: DrakkarConfig) -> None:
        """Write worker configuration to worker_config table."""
        self._drakkar_config = drakkar_config
        if not self._db or not self._config.store_config:
            return
        env_vars = {name: os.environ.get(name, '') for name in self._config.expose_env_vars}
        sinks: dict[str, list[str]] = {}
        sinks_cfg = drakkar_config.sinks
        if sinks_cfg:
            for sink_type in ('kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'):
                names = list(getattr(sinks_cfg, sink_type, {}).keys())
                if names:
                    sinks[sink_type] = names
        now = time.time()
        await self._db.execute(
            """INSERT OR REPLACE INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_workers, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                self._worker_name,
                self._cluster_name or None,
                detect_worker_ip(),
                self._config.port,
                self._config.debug_url or None,
                drakkar_config.kafka.brokers,
                drakkar_config.kafka.source_topic,
                drakkar_config.kafka.consumer_group,
                drakkar_config.executor.binary_path,
                drakkar_config.executor.max_workers,
                drakkar_config.executor.task_timeout_seconds,
                drakkar_config.executor.max_retries,
                drakkar_config.executor.window_size,
                json.dumps(sinks),
                json.dumps(env_vars),
                now,
                _format_dt(now),
            ],
        )
        await self._db.commit()

    # --- Worker state (periodic snapshots) ---

    async def _state_sync_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.state_sync_interval_seconds)
            await self._sync_state()

    async def _sync_state(self) -> None:
        if not self._db or not self._config.store_state:
            return
        app_state = self._state_provider() if self._state_provider else {}
        now = time.time()
        await self._db.execute(
            """INSERT INTO worker_state
               (uptime_seconds, assigned_partitions, partition_count,
                pool_active, pool_max, total_queued,
                consumed_count, completed_count, failed_count,
                produced_count, committed_count, paused, updated_at, updated_at_dt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                app_state.get('uptime_seconds', 0),
                json.dumps(app_state.get('assigned_partitions', [])),
                app_state.get('partition_count', 0),
                app_state.get('pool_active', 0),
                app_state.get('pool_max', 0),
                app_state.get('total_queued', 0),
                self._counters['consumed'],
                self._counters['completed'],
                self._counters['failed'],
                self._counters['produced'],
                self._counters['committed'],
                int(app_state.get('paused', False)),
                now,
                _format_dt(now),
            ],
        )
        await self._db.commit()

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        if self._retention_task:
            self._retention_task.cancel()
            try:
                await self._retention_task
            except asyncio.CancelledError:
                pass
        if self._state_task:
            self._state_task.cancel()
            try:
                await self._state_task
            except asyncio.CancelledError:
                pass
        await self._flush()
        await self._sync_state()
        if self._db:
            await self._db.close()
            self._db = None
        self._remove_live_link()
        await logger.ainfo('recorder_stopped', category='recorder')

    # --- Recording methods (sync, append to buffer) ---

    def record_consumed(self, msg: SourceMessage) -> None:
        self._counters['consumed'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': 'consumed',
                'partition': msg.partition,
                'offset': msg.offset,
            }
        )

    def record_arranged(
        self,
        partition: int,
        messages: list[SourceMessage],
        tasks: list[ExecutorTask],
        duration: float = 0.0,
        message_labels: list[str] | None = None,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': 'arranged',
                'partition': partition,
                'duration': round(duration, 4),
                'message_count': len(messages),
                'task_count': len(tasks),
                'message_labels': message_labels or [],
                'metadata': json.dumps(
                    {
                        'offsets': [m.offset for m in messages],
                        'task_ids': [t.task_id for t in tasks],
                        'task_count': len(tasks),
                        'message_count': len(messages),
                        'message_labels': message_labels or [],
                    }
                ),
            }
        )

    def record_task_started(
        self,
        task: ExecutorTask,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        slot: int = 0,
    ) -> None:
        stdin_lines = 0
        stdin_size = 0
        if task.stdin:
            stdin_size = len(task.stdin.encode())
            stdin_lines = task.stdin.count('\n') + (1 if task.stdin and not task.stdin.endswith('\n') else 0)
        self._record(
            {
                'ts': time.time(),
                'event': 'task_started',
                'partition': partition,
                'task_id': task.task_id,
                'args': json.dumps(task.args),
                'pool_active': pool_active,
                'pool_waiting': pool_waiting,
                'slot': slot,
                'stdin_lines': stdin_lines,
                'stdin_size': stdin_size,
                'metadata': json.dumps(
                    {
                        'source_offsets': task.source_offsets,
                        'slot': slot,
                    }
                ),
            }
        )

    def record_task_completed(
        self,
        result: ExecutorResult,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
    ) -> None:
        self._counters['completed'] += 1
        entry: dict = {
            'ts': time.time(),
            'event': 'task_completed',
            'partition': partition,
            'task_id': result.task_id,
            'exit_code': result.exit_code,
            'duration': result.duration_seconds,
            'stdout_size': len(result.stdout.encode()),
            'args': json.dumps(result.task.args),
            'pid': result.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
        }
        if self._config.store_output:
            entry['stdout'] = result.stdout
            entry['stderr'] = result.stderr
        self._record(entry)

    def record_task_failed(
        self,
        task: ExecutorTask,
        error: ExecutorError,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
    ) -> None:
        self._counters['failed'] += 1
        entry: dict = {
            'ts': time.time(),
            'event': 'task_failed',
            'partition': partition,
            'task_id': task.task_id,
            'exit_code': error.exit_code,
            'args': json.dumps(task.args),
            'pid': error.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'metadata': json.dumps(
                {
                    'exception': error.exception,
                }
            ),
        }
        if self._config.store_output:
            entry['stderr'] = error.stderr
        self._record(entry)

    def record_collect_completed(
        self,
        task_id: str,
        partition: int,
        duration: float,
        output_message_count: int,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': 'collect_completed',
                'task_id': task_id,
                'partition': partition,
                'duration': round(duration, 4),
                'metadata': json.dumps(
                    {
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_produced(
        self,
        payload: BaseModel,
        source_partition: int,
        source_offset: int | None = None,
    ) -> None:
        self._counters['produced'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': 'produced',
                'partition': source_partition,
                'offset': source_offset,
                'output_topic': getattr(payload, 'sink', ''),
            }
        )

    def record_sink_delivery(
        self,
        sink_type: str,
        sink_name: str,
        payload_count: int,
        duration: float,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': 'sink_delivered',
                'metadata': json.dumps(
                    {
                        'sink_type': sink_type,
                        'sink_name': sink_name,
                        'payload_count': payload_count,
                        'duration': round(duration, 4),
                    }
                ),
            }
        )

    def record_sink_error(
        self,
        sink_type: str,
        sink_name: str,
        error: str,
        attempt: int,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': 'sink_error',
                'metadata': json.dumps(
                    {
                        'sink_type': sink_type,
                        'sink_name': sink_name,
                        'error': error,
                        'attempt': attempt,
                    }
                ),
            }
        )

    def record_committed(self, partition: int, offset: int) -> None:
        self._counters['committed'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': 'committed',
                'partition': partition,
                'offset': offset,
            }
        )

    def record_assigned(self, partitions: list[int]) -> None:
        for p in partitions:
            self._record(
                {
                    'ts': time.time(),
                    'event': 'assigned',
                    'partition': p,
                }
            )

    def record_revoked(self, partitions: list[int]) -> None:
        for p in partitions:
            self._record(
                {
                    'ts': time.time(),
                    'event': 'revoked',
                    'partition': p,
                }
            )

    # --- Query methods (for debug UI, reads current DB) ---

    async def get_events(
        self,
        partition: int | None = None,
        event_type: str | None = None,
        since: float | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        if not self._db or not self._config.store_events:
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
        where = f'WHERE {" AND ".join(conditions)}' if conditions else ''
        query = f'SELECT * FROM events {where} ORDER BY id DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        async with self._db.execute(query, params) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Get the full lifecycle of a message by partition and offset."""
        await self._flush()
        if not self._db or not self._config.store_events:
            return []
        async with self._db.execute(_TRACE_QUERY, [partition, msg_offset, partition, msg_offset]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def _trace_db_file(
        self,
        db_path: str,
        partition: int,
        msg_offset: int,
    ) -> list[dict]:
        """Run trace query against a DB file, return events with worker_name from config."""
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
                                if self._cluster_name and cfg_row[1] != self._cluster_name:
                                    return []

                # Check events table exists
                async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
                    if not await cur.fetchone():
                        return []

                async with db.execute(_TRACE_QUERY, [partition, msg_offset, partition, msg_offset]) as cur:
                    columns = [d[0] for d in cur.description]
                    rows = await cur.fetchall()
                    events = [dict(zip(columns, row, strict=False)) for row in rows]

                for ev in events:
                    ev['worker_name'] = worker_name
                return events
        except Exception:
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
            ev['worker_name'] = self._worker_name
        if local_events:
            return local_events

        if not self._config.db_dir:
            return []

        searched_paths: set[str] = set()
        if self._db_path:
            searched_paths.add(os.path.realpath(self._db_path))

        # 2. Fallback: other workers' live DBs
        live_pattern = os.path.join(self._config.db_dir, '*-live.db')
        for link_path in glob.glob(live_pattern):
            if not os.path.islink(link_path):
                continue
            link_name = os.path.basename(link_path)
            if link_name.removesuffix('-live.db') == self._worker_name:
                continue
            target = os.path.realpath(link_path)
            searched_paths.add(target)
            events = await self._trace_db_file(target, partition, msg_offset)
            if events:
                return events

        # 3. Fallback: rotated DB files (newest first)
        all_dbs = []
        for entry in os.listdir(self._config.db_dir):
            if not entry.endswith('.db'):
                continue
            full = os.path.join(self._config.db_dir, entry)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            if os.path.realpath(full) in searched_paths:
                continue
            all_dbs.append((entry, full))

        # sort newest first (timestamp is in filename)
        all_dbs.sort(key=lambda x: x[0], reverse=True)

        for _entry, full in all_dbs:
            events = await self._trace_db_file(full, partition, msg_offset)
            if events:
                return events

        return []

    async def get_task_events(self, task_id: str) -> list[dict]:
        """Get all events for a specific task_id, ordered chronologically."""
        await self._flush()  # ensure recent events are queryable
        if not self._db or not self._config.store_events:
            return []
        query = 'SELECT * FROM events WHERE task_id = ? ORDER BY id ASC'
        async with self._db.execute(query, [task_id]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        if not self._db or not self._config.store_events:
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
        async with self._db.execute(query) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_active_tasks(self) -> list[dict]:
        """Get tasks that started but haven't completed or failed."""
        await self._flush()
        if not self._db or not self._config.store_events:
            return []
        query = """
            SELECT s.* FROM events s
            WHERE s.event = 'task_started'
            AND s.task_id NOT IN (
                SELECT task_id FROM events
                WHERE event IN ('task_completed', 'task_failed')
                AND task_id IS NOT NULL
            )
            ORDER BY s.ts DESC
        """
        async with self._db.execute(query) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_stats(self) -> dict:
        """Get overall statistics from the event store."""
        if not self._db or not self._config.store_events:
            return {'total_events': 0}
        query = """
            SELECT
                COUNT(*) as total_events,
                COUNT(CASE WHEN event = 'consumed' THEN 1 END) as consumed,
                COUNT(CASE WHEN event = 'task_completed' THEN 1 END) as completed,
                COUNT(CASE WHEN event = 'task_failed' THEN 1 END) as failed,
                COUNT(CASE WHEN event = 'produced' THEN 1 END) as produced,
                COUNT(CASE WHEN event = 'committed' THEN 1 END) as committed,
                MIN(ts) as oldest_event,
                MAX(ts) as newest_event
            FROM events
        """
        async with self._db.execute(query) as cursor:
            columns = [d[0] for d in cursor.description]
            row = await cursor.fetchone()
            return dict(zip(columns, row, strict=False)) if row else {'total_events': 0}

    # --- Autodiscovery ---

    async def discover_workers(self) -> list[dict]:
        """Scan db_dir for other workers' -live.db symlinks, read their worker_config."""
        if not self._config.db_dir or not self._config.store_config:
            return []
        pattern = os.path.join(self._config.db_dir, '*-live.db')
        workers: list[dict] = []
        for link_path in glob.glob(pattern):
            if not os.path.islink(link_path):
                continue
            link_name = os.path.basename(link_path)
            worker_name = link_name.removesuffix('-live.db')
            if worker_name == self._worker_name:
                continue
            try:
                target = os.path.realpath(link_path)
                async with aiosqlite.connect(f'file:{target}?mode=ro', uri=True) as db:
                    async with db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='worker_config'"
                    ) as cur:
                        if not await cur.fetchone():
                            continue
                    async with db.execute('SELECT * FROM worker_config WHERE id = 1') as cur:
                        columns = [d[0] for d in cur.description]
                        row = await cur.fetchone()
                        if row:
                            workers.append(dict(zip(columns, row, strict=False)))
            except Exception:
                continue
        return workers

    # --- Internal flush/retention ---

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.flush_interval_seconds)
            await self._flush()

    async def _flush(self) -> None:
        if not self._db or not self._buffer:
            return
        if not self._config.store_events:
            self._buffer.clear()
            return
        batch = []
        while self._buffer:
            batch.append(self._buffer.popleft())
        columns = [
            'ts',
            'dt',
            'event',
            'partition',
            'offset',
            'task_id',
            'args',
            'stdout_size',
            'stdout',
            'stderr',
            'exit_code',
            'duration',
            'output_topic',
            'metadata',
            'pid',
        ]
        placeholders = ', '.join(['?'] * len(columns))
        col_names = ', '.join(columns)
        query = f'INSERT INTO events ({col_names}) VALUES ({placeholders})'
        rows = [tuple(entry.get(col) for col in columns) for entry in batch]
        await self._db.executemany(query, rows)
        await self._db.commit()

    async def _retention_loop(self) -> None:
        while self._running:
            await asyncio.sleep(3600)  # every hour
            await self._rotate()

    async def _rotate(self) -> None:
        """Rotate: open new DB first, then close old — no query gap."""
        # flush remaining buffer to current DB
        await self._flush()

        # open new DB before closing old — queries keep working
        new_path = _make_db_path(self._config.db_dir, self._worker_name)
        new_db = await aiosqlite.connect(new_path)

        old_db = self._db
        self._db = new_db
        self._db_path = new_path
        await self._create_schema()
        if self._drakkar_config:
            await self.write_config(self._drakkar_config)
        self._update_live_link()

        if old_db:
            await old_db.close()

        # delete DB files older than retention
        cutoff = time.time() - (self._config.retention_hours * 3600)
        for db_file in _list_db_files(self._config.db_dir, self._worker_name):
            if db_file == new_path:
                continue
            try:
                mtime = os.path.getmtime(db_file)
                if mtime < cutoff:
                    os.remove(db_file)
                    await logger.ainfo('recorder_deleted_old_db', category='recorder', path=db_file)
            except OSError:
                pass

        # enforce max file count
        remaining = _list_db_files(self._config.db_dir, self._worker_name)
        max_files = max(1, self._config.retention_max_events // 10_000)
        if len(remaining) > max_files:
            for old_file in remaining[:-max_files]:
                if old_file != new_path:
                    try:
                        os.remove(old_file)
                    except OSError:
                        pass

        await logger.ainfo('recorder_rotated', category='recorder', new_db=self._db_path)
