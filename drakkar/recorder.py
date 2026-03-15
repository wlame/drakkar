"""Flight recorder — event log to timestamped SQLite files."""

import asyncio
import glob
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import structlog

from drakkar.config import DebugConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    SourceMessage,
)

logger = structlog.get_logger()

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
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
    metadata    TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_partition_offset ON events(partition, offset);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event);
"""


def _make_db_path(base_path: str) -> str:
    """Generate a timestamped DB filename from the base path.

    /tmp/drakkar-debug.db -> /tmp/drakkar-debug-2026-03-16__14_55.db
    """
    p = Path(base_path)
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d__%H_%M")
    return str(p.with_stem(f"{p.stem}-{ts}"))


def _list_db_files(base_path: str) -> list[str]:
    """List all rotated DB files matching the base pattern, oldest first."""
    p = Path(base_path)
    pattern = str(p.with_stem(f"{p.stem}-*"))
    files = glob.glob(pattern)
    files.sort()
    return files


class EventRecorder:
    """Records processing events to timestamped SQLite database files.

    Events are buffered in memory and flushed periodically. On retention
    check, the current DB is finalized and a new timestamped file is
    created. Old DB files beyond retention_hours are deleted.
    """

    def __init__(self, config: DebugConfig):
        self._config = config
        self._buffer: deque[dict] = deque()
        self._db: aiosqlite.Connection | None = None
        self._db_path: str = ""
        self._flush_task: asyncio.Task | None = None
        self._retention_task: asyncio.Task | None = None
        self._running = False

    @property
    def db_path(self) -> str:
        return self._db_path

    async def start(self) -> None:
        self._db_path = _make_db_path(self._config.db_path)
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.executescript(SCHEMA)
        await self._db.commit()
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        self._retention_task = asyncio.create_task(self._retention_loop())
        await logger.ainfo("recorder_started", category="recorder", db_path=self._db_path)

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
        await self._flush()
        if self._db:
            await self._db.close()
            self._db = None
        await logger.ainfo("recorder_stopped", category="recorder")

    # --- Recording methods (sync, append to buffer) ---

    def record_consumed(self, msg: SourceMessage) -> None:
        self._buffer.append({
            'ts': time.time(),
            'event': 'consumed',
            'partition': msg.partition,
            'offset': msg.offset,
        })

    def record_arranged(
        self,
        partition: int,
        messages: list[SourceMessage],
        tasks: list[ExecutorTask],
    ) -> None:
        self._buffer.append({
            'ts': time.time(),
            'event': 'arranged',
            'partition': partition,
            'metadata': json.dumps({
                'offsets': [m.offset for m in messages],
                'task_ids': [t.task_id for t in tasks],
                'task_count': len(tasks),
            }),
        })

    def record_task_started(self, task: ExecutorTask, partition: int) -> None:
        self._buffer.append({
            'ts': time.time(),
            'event': 'task_started',
            'partition': partition,
            'task_id': task.task_id,
            'args': json.dumps(task.args),
            'metadata': json.dumps({
                'source_offsets': task.source_offsets,
            }),
        })

    def record_task_completed(self, result: ExecutorResult, partition: int) -> None:
        entry: dict = {
            'ts': time.time(),
            'event': 'task_completed',
            'partition': partition,
            'task_id': result.task_id,
            'exit_code': result.exit_code,
            'duration': result.duration_seconds,
            'stdout_size': len(result.stdout.encode()),
            'args': json.dumps(result.task.args),
        }
        if self._config.store_output:
            entry['stdout'] = result.stdout
            entry['stderr'] = result.stderr
        self._buffer.append(entry)

    def record_task_failed(
        self,
        task: ExecutorTask,
        error: ExecutorError,
        partition: int,
    ) -> None:
        entry: dict = {
            'ts': time.time(),
            'event': 'task_failed',
            'partition': partition,
            'task_id': task.task_id,
            'exit_code': error.exit_code,
            'args': json.dumps(task.args),
            'metadata': json.dumps({
                'exception': error.exception,
            }),
        }
        if self._config.store_output:
            entry['stderr'] = error.stderr
        self._buffer.append(entry)

    def record_produced(
        self,
        msg: OutputMessage,
        source_partition: int,
        source_offset: int | None = None,
    ) -> None:
        self._buffer.append({
            'ts': time.time(),
            'event': 'produced',
            'partition': source_partition,
            'offset': source_offset,
            'output_topic': 'target',
        })

    def record_committed(self, partition: int, offset: int) -> None:
        self._buffer.append({
            'ts': time.time(),
            'event': 'committed',
            'partition': partition,
            'offset': offset,
        })

    def record_assigned(self, partitions: list[int]) -> None:
        for p in partitions:
            self._buffer.append({
                'ts': time.time(),
                'event': 'assigned',
                'partition': p,
            })

    def record_revoked(self, partitions: list[int]) -> None:
        for p in partitions:
            self._buffer.append({
                'ts': time.time(),
                'event': 'revoked',
                'partition': p,
            })

    # --- Query methods (for debug UI, reads current DB) ---

    async def get_events(
        self,
        partition: int | None = None,
        event_type: str | None = None,
        since: float | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        if not self._db:
            return []
        conditions = []
        params: list = []
        if partition is not None:
            conditions.append("partition = ?")
            params.append(partition)
        if event_type:
            conditions.append("event = ?")
            params.append(event_type)
        if since:
            conditions.append("ts >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        async with self._db.execute(query, params) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    async def get_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Get the full lifecycle of a message by partition and offset."""
        if not self._db:
            return []
        query = """
            SELECT * FROM events
            WHERE partition = ? AND (
                offset = ?
                OR task_id IN (
                    SELECT task_id FROM events
                    WHERE partition = ? AND event = 'task_started'
                    AND json_extract(metadata, '$.source_offsets') LIKE ?
                )
            )
            ORDER BY id ASC
        """
        offset_pattern = f"%{msg_offset}%"
        async with self._db.execute(query, [partition, msg_offset, partition, offset_pattern]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        if not self._db:
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
            return [dict(zip(columns, row)) for row in rows]

    async def get_active_tasks(self) -> list[dict]:
        """Get tasks that started but haven't completed or failed."""
        if not self._db:
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
            return [dict(zip(columns, row)) for row in rows]

    async def get_stats(self) -> dict:
        """Get overall statistics from the event store."""
        if not self._db:
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
            return dict(zip(columns, row)) if row else {'total_events': 0}

    # --- Internal flush/retention ---

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.flush_interval_seconds)
            await self._flush()

    async def _flush(self) -> None:
        if not self._db or not self._buffer:
            return
        batch = []
        while self._buffer:
            batch.append(self._buffer.popleft())
        columns = [
            'ts', 'event', 'partition', 'offset', 'task_id', 'args',
            'stdout_size', 'stdout', 'stderr', 'exit_code', 'duration',
            'output_topic', 'metadata',
        ]
        placeholders = ', '.join(['?'] * len(columns))
        col_names = ', '.join(columns)
        query = f"INSERT INTO events ({col_names}) VALUES ({placeholders})"
        rows = [
            tuple(entry.get(col) for col in columns) for entry in batch
        ]
        await self._db.executemany(query, rows)
        await self._db.commit()

    async def _retention_loop(self) -> None:
        while self._running:
            await asyncio.sleep(300)  # every 5 minutes
            await self._rotate()

    async def _rotate(self) -> None:
        """Rotate: close current DB, delete old files, open a new one."""
        # flush remaining buffer to current DB
        await self._flush()

        # close current DB
        if self._db:
            await self._db.close()
            self._db = None

        # delete DB files older than retention
        cutoff = time.time() - (self._config.retention_hours * 3600)
        for db_file in _list_db_files(self._config.db_path):
            try:
                mtime = os.path.getmtime(db_file)
                if mtime < cutoff:
                    os.remove(db_file)
                    await logger.ainfo("recorder_deleted_old_db", category="recorder", path=db_file)
            except OSError:
                pass

        # enforce max file count (keep newest retention_max_events files)
        # use file count as a proxy — simpler than counting rows across files
        remaining = _list_db_files(self._config.db_path)
        max_files = max(1, self._config.retention_max_events // 10_000)
        if len(remaining) > max_files:
            for old_file in remaining[:-max_files]:
                try:
                    os.remove(old_file)
                except OSError:
                    pass

        # open new DB file
        self._db_path = _make_db_path(self._config.db_path)
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.executescript(SCHEMA)
        await self._db.commit()
        await logger.ainfo("recorder_rotated", category="recorder", new_db=self._db_path)
