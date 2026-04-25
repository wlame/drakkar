"""Flight recorder — event log to timestamped SQLite files.

The schemas + standalone helpers (JSON encoding, env-secret sanitization,
DB-file path management, IP detection) live in two sibling modules:

- :mod:`drakkar.recorder_schema`  — DDL constants + canned trace queries.
- :mod:`drakkar.recorder_helpers` — orjson-or-stdlib codec, secret patterns,
  ``_format_dt``, ``_make_db_path``, ``_live_link_path``, ``_list_db_files``,
  ``_open_reader``, ``detect_worker_ip``.

This module re-imports them so external code (and ``mock.patch`` test sites)
can keep using ``drakkar.recorder.X`` paths.
"""

from __future__ import annotations

import asyncio
import contextlib
import glob
import os
import queue

# ``socket`` is re-imported here so test patches like
# ``patch('drakkar.recorder.socket.socket', ...)`` still find the module on
# this attribute path. The actual ``detect_worker_ip`` consumer lives in
# :mod:`drakkar.recorder_helpers`, but ``socket`` is a shared module
# reference — patching ``socket.socket`` via either attribute path replaces
# the class globally for both modules.
import socket  # noqa: F401
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING

import aiosqlite
import structlog
from pydantic import BaseModel

from drakkar.config import DebugConfig
from drakkar.metrics import (
    recorder_buffer_size,
    recorder_dropped_events,
    recorder_flush_batches_dropped,
    recorder_flush_duration,
    recorder_flush_retries,
    recorder_requeue_overflow,
)
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)
from drakkar.peer_discovery import discover_peer_dbs

# Re-exports — keep the previous public surface so tests and external
# imports of ``drakkar.recorder.<name>`` continue working without changes.
# ``_encode_json``, ``_HAS_ORJSON`` and ``_SECRET_ENV_PATTERNS`` are not
# referenced inside this module after the helper extraction but ARE
# imported by tests; the ``noqa: F401`` markers silence the unused-import
# warning while preserving the public attribute path on
# ``drakkar.recorder``.
from drakkar.recorder_helpers import (
    _HAS_ORJSON,  # noqa: F401  (re-exported for tests)
    _SECRET_ENV_PATTERNS,  # noqa: F401  (re-exported for tests)
    _encode_json,  # noqa: F401  (re-exported for tests)
    _encode_json_str,
    _format_dt,
    _list_db_files,
    _live_link_path,
    _make_db_path,
    _open_reader,
    _sanitize_env_value,
    detect_worker_ip,
)
from drakkar.recorder_schema import (
    _LABEL_TRACE_QUERY,
    _TRACE_QUERY,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
)
from drakkar.utils import redact_url

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig

logger = structlog.get_logger()


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
        # Dedicated reader connection used by the debug UI for SELECTs.
        # aiosqlite serializes ops per connection, so without this the UI
        # read path would queue behind buffered-event flushes on the
        # writer. WAL mode (applied to the writer) lets a separate reader
        # connection see consistent snapshots while the writer commits.
        # Opened in ``start()``, rotated alongside ``_db`` in ``_rotate``,
        # closed first in ``stop()`` — closing the reader before the
        # writer avoids the edge case where a SELECT is pending against
        # a connection whose WAL has just been torn down.
        self._reader_db: aiosqlite.Connection | None = None
        self._db_path: str = ''
        self._flush_task: asyncio.Task | None = None
        self._retention_task: asyncio.Task | None = None
        self._state_task: asyncio.Task | None = None
        self._running = False
        self._ws_subscribers: set[queue.Queue] = set()
        self._state_provider: Callable[[], dict] | None = None
        self._drakkar_config: DrakkarConfig | None = None
        # Deferred WS broadcasts: task_started events are held for ws_min_duration_ms
        # before being sent to WebSocket. If the task completes before the threshold,
        # neither start nor completion is sent (fast task, invisible to live UI).
        # Exception: failed tasks always go to WS regardless of duration.
        self._deferred_ws: dict[str, tuple[dict, asyncio.TimerHandle]] = {}
        # In-memory counters (used for worker_state regardless of store_events)
        self._counters = {
            'consumed': 0,
            'completed': 0,
            'failed': 0,
            'produced': 0,
            'committed': 0,
        }
        # Serialize ``_flush`` calls. The periodic ``_flush_loop`` and
        # debug endpoints (via ``_dispatch_to_main_loop``) can both
        # schedule flushes on the main loop. Both paths drain
        # ``self._buffer`` and observe ``recorder_flush_duration``; without
        # a lock, two concurrent flushes could each grab half of the
        # buffer and race on the histogram observation. The lock is
        # bound to whichever loop first invokes ``_flush`` — on the main
        # loop for production flows — and is cheap: a plain asyncio.Lock.
        self._flush_lock = asyncio.Lock()
        # Consecutive OperationalError count for the CURRENTLY re-queued
        # batch. Incremented on each failed flush; reset on any successful
        # flush OR when a batch is dropped after exceeding
        # ``max_flush_retries``. The counter is instance-level (not per
        # batch) because we re-queue the failed rows at the FRONT of the
        # buffer — the very next flush picks them up again, so a single
        # counter suffices. New rows appended while we're in a retry loop
        # simply ride along with the retried batch on the next flush.
        self._flush_failures: int = 0

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def config(self) -> DebugConfig:
        """Read-only access to the debug config — lets the debug server
        inspect ``store_events`` / ``ws_min_duration_ms`` without reaching
        into the private ``_config`` attribute.
        """
        return self._config

    @property
    def reader_db(self) -> aiosqlite.Connection | None:
        """Return the dedicated reader aiosqlite connection.

        The debug UI should use this connection for SELECTs — it runs on a
        separate aiosqlite worker thread from the writer, so UI reads
        don't queue behind buffered-event flushes. Returns ``None`` when
        the recorder has not been started yet or was started without a
        ``db_dir`` (memory-only mode), matching the semantics of
        ``_db``. Callers should check for ``None`` before executing.
        """
        return self._reader_db

    async def flush(self) -> None:
        """Public entry point to flush pending events to the writer DB.

        Exposed for the debug server — it needs to force a flush before
        running SELECTs through ``reader_db`` so the UI sees every event
        recorded up to the query moment. The underlying ``_flush`` method
        is the same; this alias keeps callers off private attributes and
        gives us a stable public API to evolve.
        """
        await self._flush()

    @property
    def counters(self) -> dict[str, int]:
        return dict(self._counters)

    def set_state_provider(self, provider: Callable[[], dict]) -> None:
        """Set callback that returns current worker state (uptime, partitions, pool)."""
        self._state_provider = provider

    async def _create_schema(self, db: aiosqlite.Connection) -> None:
        """Create tables on the given connection based on config flags.

        Accepts an explicit ``db`` argument (rather than using ``self._db``)
        so that callers can initialize a freshly-opened connection BEFORE
        it is installed as the live writer. Used by :meth:`start` during
        first open and by :meth:`_rotate` to prime the new DB ahead of the
        atomic swap — the race window where the new file was live but
        schemaless would otherwise let a concurrent ``_flush`` hit
        ``no such table: events``.
        """
        if self._config.store_events:
            await db.executescript(SCHEMA_EVENTS)
        if self._config.store_config:
            await db.executescript(SCHEMA_WORKER_CONFIG)
        if self._config.store_state:
            await db.executescript(SCHEMA_WORKER_STATE)
        await db.commit()

    async def start(self) -> None:
        self._running = True
        if self._config.db_dir:
            self._db_path = _make_db_path(self._config.db_dir, self._worker_name)
            # Open writer + apply PRAGMA + create schema + open reader in a
            # single try/except. A failure at any of these steps must close
            # whichever connections already opened — otherwise the caller
            # typically doesn't call ``stop()`` on a failed ``start`` and
            # the partially-opened ``_db`` handle leaks its fd + lock.
            try:
                self._db = await aiosqlite.connect(self._db_path)
                # WAL mode is what lets a separate reader connection coexist
                # with the writer without serializing reads behind writes.
                # Applied per-connection (SQLite stores the mode in the DB
                # header; the reader picks it up automatically on open).
                await self._db.execute('PRAGMA journal_mode=WAL')
                await self._create_schema(self._db)
                # Open the dedicated reader connection AFTER schema creation
                # so the reader always sees a ready DB. URI with ``mode=ro``
                # rejects any accidental write attempt through this handle.
                self._reader_db = await _open_reader(self._db_path)
            except Exception:
                # Best-effort cleanup of whichever connections opened before
                # the exception. Reset attrs so a retry of start() starts
                # from scratch and a later stop() is a clean no-op.
                if self._reader_db is not None:
                    with contextlib.suppress(Exception):
                        await self._reader_db.close()
                    self._reader_db = None
                if self._db is not None:
                    with contextlib.suppress(Exception):
                        await self._db.close()
                    self._db = None
                self._running = False
                raise
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

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        """Append event to buffer and broadcast to WS subscribers.

        Observability: when the deque is already at capacity, the next
        append will silently evict the oldest entry. We detect that case
        BEFORE the append (``len == maxlen``) and increment the drop
        counter once per dropped event. After the append, the buffer
        gauge is refreshed to the new depth so operators can watch for
        contention without scraping internal state.
        """
        event['dt'] = _format_dt(event['ts'])
        if not skip_db:
            # deque(maxlen=N) accepts up to N items; when len == maxlen a
            # subsequent append drops the leftmost entry. We count one
            # drop per event lost — prometheus_client Counter.inc() is
            # thread-safe so callers from multiple loops/threads are fine.
            maxlen = self._buffer.maxlen
            if maxlen is not None and len(self._buffer) >= maxlen:
                recorder_dropped_events.inc()
            self._buffer.append(event)
            recorder_buffer_size.set(len(self._buffer))
        if not skip_ws and self._ws_subscribers:
            for q in self._ws_subscribers:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    pass

    def _broadcast_ws(self, event: dict) -> None:
        """Send event to WebSocket subscribers without buffering to DB."""
        if self._ws_subscribers:
            for q in self._ws_subscribers:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    pass

    def _send_deferred_start(self, task_id: str) -> None:
        """Timer callback: task is still running after ws_min_duration_ms, send its start event."""
        entry = self._deferred_ws.pop(task_id, None)
        if entry:
            event, _ = entry
            self._broadcast_ws(event)

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
        """Write worker configuration to worker_config table.

        Security note: this SQLite file is downloadable via the debug UI.
        Any value written here is effectively public to anyone who can
        reach that endpoint. Redact secrets before insertion:
        - kafka_brokers: strip credentials from SASL URIs.
        - env_vars: redact values of any secret-named var; URL-shape
          values have embedded credentials stripped.
        """
        self._drakkar_config = drakkar_config
        if not self._db or not self._config.store_config:
            return
        env_vars = {name: _sanitize_env_value(name, os.environ.get(name, '')) for name in self._config.expose_env_vars}
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
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                self._worker_name,
                self._cluster_name or None,
                detect_worker_ip(),
                self._config.port,
                self._config.debug_url or None,
                redact_url(drakkar_config.kafka.brokers),
                drakkar_config.kafka.source_topic,
                drakkar_config.kafka.consumer_group,
                drakkar_config.executor.binary_path,
                drakkar_config.executor.max_executors,
                drakkar_config.executor.task_timeout_seconds,
                drakkar_config.executor.max_retries,
                drakkar_config.executor.window_size,
                _encode_json_str(sinks),
                _encode_json_str(env_vars),
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
                _encode_json_str(app_state.get('assigned_partitions', [])),
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
        # cancel any pending deferred WS timers
        for _, handle in self._deferred_ws.values():
            handle.cancel()
        self._deferred_ws.clear()

        await self._flush()
        await self._sync_state()
        # Close the reader first. A close failure here should not block
        # the writer close below — the writer close is what actually
        # finalizes pending commits and tears down the WAL.
        if self._reader_db:
            try:
                await self._reader_db.close()
            except Exception as exc:
                await logger.awarning(
                    'recorder_reader_close_failed',
                    category='recorder',
                    error=str(exc),
                )
            self._reader_db = None
        if self._db:
            # Match the ``_rotate`` pattern: a close failure on the writer
            # must not leak an exception out of shutdown. Log + continue so
            # the live-link cleanup and the final ``recorder_stopped`` log
            # still happen.
            try:
                await self._db.close()
            except Exception as exc:
                await logger.awarning(
                    'recorder_writer_close_failed',
                    category='recorder',
                    error=str(exc),
                )
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
                'metadata': _encode_json_str(
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
        precomputed: bool = False,
    ) -> None:
        stdin_lines = 0
        stdin_size = 0
        if task.stdin:
            stdin_size = len(task.stdin.encode())
            stdin_lines = task.stdin.count('\n') + (1 if task.stdin and not task.stdin.endswith('\n') else 0)
        metadata: dict = {
            'source_offsets': task.source_offsets,
            'slot': slot,
        }
        if task.env:
            # Sanitize per-task env values before storing in the recorder DB.
            # The raw task.env stays untouched on the task object (subprocess
            # launch still needs real values); only the RECORDED copy is
            # redacted so the debug UI never exposes secrets.
            metadata['env'] = {k: _sanitize_env_value(k, v) for k, v in task.env.items()}
        if precomputed:
            # Neutral marker: a result was supplied by the handler and no
            # subprocess ran. The framework does not classify the reason
            # (cache, lookup, deterministic shortcut, ...).
            metadata['precomputed'] = True
        entry = {
            'ts': time.time(),
            'event': 'task_started',
            'partition': partition,
            'task_id': task.task_id,
            'args': _encode_json_str(task.args),
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'slot': slot,
            'stdin_lines': stdin_lines,
            'stdin_size': stdin_size,
            'metadata': _encode_json_str(metadata),
            'labels': _encode_json_str(task.labels) if task.labels else None,
        }
        ws_threshold_ms = self._config.ws_min_duration_ms
        if ws_threshold_ms > 0:
            # Defer WS broadcast: only send task_started to live UI if
            # the task is still running after ws_min_duration_ms.
            self._record(entry, skip_ws=True)
            loop = asyncio.get_running_loop()
            handle = loop.call_later(
                ws_threshold_ms / 1000.0,
                self._send_deferred_start,
                task.task_id,
            )
            self._deferred_ws[task.task_id] = (entry, handle)
        else:
            self._record(entry)

    def record_task_completed(
        self,
        result: ExecutorResult,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        precomputed: bool = False,
    ) -> None:
        self._counters['completed'] += 1
        duration_ms = result.duration_seconds * 1000

        # If the task_started was deferred and the task finished before
        # the threshold, neither start nor completion goes to WS.
        # If the threshold already fired (start was sent), send completion too.
        deferred = self._deferred_ws.pop(result.task.task_id, None)
        if deferred:
            _, handle = deferred
            handle.cancel()
            skip_ws = True
        else:
            skip_ws = False

        skip_db = self._config.event_min_duration_ms > 0 and duration_ms < self._config.event_min_duration_ms
        include_output = duration_ms >= self._config.output_min_duration_ms

        entry: dict = {
            'ts': time.time(),
            'event': 'task_completed',
            'partition': partition,
            'task_id': result.task.task_id,
            'exit_code': result.exit_code,
            'duration': result.duration_seconds,
            'stdout_size': len(result.stdout.encode()),
            'pid': result.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'labels': _encode_json_str(result.task.labels) if result.task.labels else None,
        }
        if precomputed:
            # Mirrored on the completion event so downstream queries /
            # dashboards can filter precomputed outcomes without joining
            # to task_started.
            entry['metadata'] = _encode_json_str({'precomputed': True})
        if include_output:
            entry['args'] = _encode_json_str(result.task.args)
        if include_output and self._config.store_output:
            entry['stdout'] = result.stdout
            entry['stderr'] = result.stderr
        self._record(entry, skip_ws=skip_ws, skip_db=skip_db)

        if duration_ms >= self._config.log_min_duration_ms:
            logger.info(
                'slow_task_completed',
                category='recorder',
                task_id=result.task.task_id,
                duration=result.duration_seconds,
                partition=partition,
            )

    def record_task_failed(
        self,
        task: ExecutorTask,
        error: ExecutorError,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        duration_seconds: float | None = None,
    ) -> None:
        self._counters['failed'] += 1

        # Failed tasks ALWAYS go to WS regardless of ws_min_duration_ms.
        # If the task_started was deferred, send it now before the failure event
        # so the live UI sees the full start→fail sequence.
        deferred = self._deferred_ws.pop(task.task_id, None)
        if deferred:
            start_event, handle = deferred
            handle.cancel()
            self._broadcast_ws(start_event)

        if duration_seconds is not None:
            duration_ms = duration_seconds * 1000
            skip_db = self._config.event_min_duration_ms > 0 and duration_ms < self._config.event_min_duration_ms
            include_output = duration_ms >= self._config.output_min_duration_ms
            should_log = duration_ms >= self._config.log_min_duration_ms
        else:
            skip_db = False
            include_output = True
            should_log = True

        entry: dict = {
            'ts': time.time(),
            'event': 'task_failed',
            'partition': partition,
            'task_id': task.task_id,
            'exit_code': error.exit_code,
            'pid': error.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'metadata': _encode_json_str(
                {
                    'exception': error.exception,
                }
            ),
            'labels': _encode_json_str(task.labels) if task.labels else None,
        }
        if duration_seconds is not None:
            entry['duration'] = duration_seconds
        if include_output:
            entry['args'] = _encode_json_str(task.args)
        if include_output and self._config.store_output:
            entry['stderr'] = error.stderr
        self._record(entry, skip_ws=False, skip_db=skip_db)

        if should_log:
            logger.info(
                'slow_task_failed',
                category='recorder',
                task_id=task.task_id,
                duration=duration_seconds,
                partition=partition,
            )

    def record_task_complete(
        self,
        task_id: str,
        partition: int,
        duration: float,
        output_message_count: int,
    ) -> None:
        """Record that on_task_complete() finished for one successful task.

        Event name is ``task_complete`` (the hook name without ``on_``) —
        distinct from ``task_completed`` which marks subprocess exit. The
        two sit next to each other in the pipeline: subprocess ends first
        (task_completed), then the handler's post-processing and sink
        routing run, and this event marks the end of that stage.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'task_complete',
                'task_id': task_id,
                'partition': partition,
                'duration': round(duration, 4),
                'metadata': _encode_json_str(
                    {
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_message_complete(
        self,
        partition: int,
        offset: int,
        duration: float,
        task_count: int,
        succeeded: int,
        failed: int,
        replaced: int,
        output_message_count: int,
    ) -> None:
        """Record that on_message_complete() finished for one source message.

        Fires once per source message, after every task derived from it
        has reached a terminal state. The event corresponds 1:1 with a
        handler ``on_message_complete`` call.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'message_complete',
                'partition': partition,
                'offset': offset,
                'duration': round(duration, 4),
                'metadata': _encode_json_str(
                    {
                        'task_count': task_count,
                        'succeeded': succeeded,
                        'failed': failed,
                        'replaced': replaced,
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_window_complete(
        self,
        partition: int,
        window_id: int,
        duration: float,
        task_count: int,
        output_message_count: int,
    ) -> None:
        """Record that on_window_complete() finished for one arrange() window."""
        self._record(
            {
                'ts': time.time(),
                'event': 'window_complete',
                'partition': partition,
                'duration': round(duration, 4),
                'metadata': _encode_json_str(
                    {
                        'window_id': window_id,
                        'task_count': task_count,
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
                'metadata': _encode_json_str(
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
                'metadata': _encode_json_str(
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

    def record_periodic_run(
        self,
        name: str,
        duration: float,
        status: str,
        error: str = '',
        system: bool = False,
    ) -> None:
        """Record a periodic task execution (success or failure).

        The ``system`` flag distinguishes framework-internal periodic loops
        (``cache.flush``, ``cache.sync``, ``cache.cleanup``) from user-defined
        ``@periodic`` handler methods. It is omitted from the metadata JSON
        when False so existing event rows remain byte-identical to those written
        before the flag was introduced — avoids a metadata schema diff.
        """
        metadata: dict[str, str | bool] = {'status': status}
        if error:
            metadata['error'] = error
        if system:
            metadata['system'] = True
        self._record(
            {
                'ts': time.time(),
                'event': 'periodic_run',
                'task_id': name,
                'duration': duration,
                'exit_code': 0 if status == 'ok' else 1,
                'metadata': _encode_json_str(metadata),
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
        # Reads go through the dedicated reader connection so they don't
        # serialize behind buffered-event flushes on the writer; fall back
        # to the writer when the reader isn't available (e.g. legacy tests
        # that set ``_db`` only).
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
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
        async with reader.execute(query, params) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Find all events for tasks matching a label key-value pair."""
        await self._flush()
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
            return []
        json_path = f'$.{label_key}'
        async with reader.execute(_LABEL_TRACE_QUERY, [json_path, label_value]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def cross_trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Trace by label across all workers in the same cluster."""
        local_events = await self.trace_by_label(label_key, label_value)
        for ev in local_events:
            ev['worker_name'] = self._worker_name

        if local_events:
            return sorted(local_events, key=lambda e: e.get('ts', 0))

        # search other workers' live DBs
        if not self._config.db_dir:
            return []

        json_path = f'$.{label_key}'
        for target in sorted(glob.glob(os.path.join(self._config.db_dir, '*-live.db'))):
            real = os.path.realpath(target)
            if real == self._db_path:
                continue
            try:
                async with aiosqlite.connect(f'file:{real}?mode=ro', uri=True) as db:
                    worker_name = os.path.basename(real)
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
                                        continue

                    async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
                        if not await cur.fetchone():
                            continue

                    async with db.execute(_LABEL_TRACE_QUERY, [json_path, label_value]) as cur:
                        columns = [d[0] for d in cur.description]
                        rows = await cur.fetchall()
                        events = [dict(zip(columns, row, strict=False)) for row in rows]

                    for ev in events:
                        ev['worker_name'] = worker_name
                    if events:
                        return sorted(events, key=lambda e: e.get('ts', 0))
            except Exception:
                continue

        return []

    async def get_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Get the full lifecycle of a message by partition and offset."""
        await self._flush()
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
            return []
        async with reader.execute(_TRACE_QUERY, [partition, msg_offset, partition, msg_offset]) as cursor:
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
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
            return []
        query = 'SELECT * FROM events WHERE task_id = ? ORDER BY id ASC'
        async with reader.execute(query, [task_id]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
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
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_active_tasks(self) -> list[dict]:
        """Get tasks that started but haven't completed or failed."""
        await self._flush()
        reader = self._reader_db or self._db
        if not reader or not self._config.store_events:
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
        async with reader.execute(query) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_stats(self) -> dict:
        """Get overall statistics from in-memory counters (accumulated since worker start)."""
        stats = dict(self._counters)
        stats['total_events'] = sum(self._counters.values())
        return stats

    # --- Autodiscovery ---

    async def discover_workers(self) -> list[dict]:
        """Scan db_dir for other workers' -live.db symlinks, read their worker_config.

        Symlink scanning is delegated to :func:`discover_peer_dbs`
        (shared with the cache peer-sync loop). We keep the recorder-specific
        step — reading the `worker_config` row out of each resolved DB —
        right here; the cache will supply its own row-reader in a later task.
        """
        if not self._config.db_dir or not self._config.store_config:
            return []
        workers: list[dict] = []
        async for _worker_name, target in discover_peer_dbs(
            self._config.db_dir,
            '-live.db',
            self._worker_name,
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
        # The lock serializes concurrent flushes. Without it, the periodic
        # ``_flush_loop`` and a debug-endpoint-initiated flush could
        # interleave on the same deque and histogram — the second flush
        # would typically find an empty buffer and early-return, but the
        # cost of acquiring an uncontended asyncio.Lock is negligible and
        # the safety is worth it.
        async with self._flush_lock:
            # Snapshot ``self._db`` once at the top of the critical section
            # and use the local ``db`` reference for both ``executemany`` and
            # ``commit``. Otherwise a concurrent ``_rotate`` (which does NOT
            # hold ``_flush_lock``) could swap ``self._db`` between the two
            # awaits — we'd commit on the new connection (no-op, since the
            # executemany happened on the old one) and then ``old_db.close()``
            # would discard the uncommitted transaction → data loss.
            db = self._db
            if not db or not self._buffer:
                return
            if not self._config.store_events:
                self._buffer.clear()
                # Reflect the drain in the gauge even when events aren't persisted —
                # buffer length is what the gauge reports, not DB row count.
                recorder_buffer_size.set(0)
                return
            # Take a LOCAL snapshot of the rows to flush BEFORE the DB
            # write. If the write fails with OperationalError we re-queue
            # this exact list at the FRONT of the buffer so ordering is
            # preserved and no events are lost. Using ``list(...)`` freezes
            # the view — a concurrent ``_record`` append between here and
            # the write would land at the END of the deque (because we
            # drain via ``popleft`` below) and is unaffected by the snapshot.
            batch: list[dict] = []
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
                'labels',
            ]
            placeholders = ', '.join(['?'] * len(columns))
            col_names = ', '.join(columns)
            query = f'INSERT INTO events ({col_names}) VALUES ({placeholders})'
            rows = [tuple(entry.get(col) for col in columns) for entry in batch]
            # Observe the full flush body (executemany + commit) — the histogram
            # surfaces disk-I/O latency tail so operators can alert on p99
            # regressions before the buffer backs up enough to drop events.
            flush_start = time.monotonic()
            try:
                await db.executemany(query, rows)
                await db.commit()
            except aiosqlite.OperationalError as exc:
                # Transient DB errors (``database is locked``, ``disk I/O
                # error``, ENOSPC, WAL corruption, etc.) should not cost us
                # the batch. Re-queue the snapshot at the FRONT of the
                # buffer and let the next flush tick retry. ``extendleft``
                # reverses its iterable, so we pre-reverse ``batch`` to
                # preserve the original FIFO ordering within the re-queued
                # rows. Any rows that a concurrent ``_record`` appended
                # while we were awaiting stay at the back — correct, since
                # they are strictly newer than the retried batch.
                #
                # Rotation edge case: if ``_rotate`` ran BETWEEN a failed
                # flush and the retry attempt, the re-queued rows end up
                # written to the NEW DB. For a flight recorder this is
                # acceptable — the rows' ``ts`` field records when they
                # were observed, so their wall-clock position is preserved;
                # only the file-level window association shifts. The
                # alternative (dropping rows on rotation) would be
                # strictly worse — silent data loss with no metric tick.
                self._flush_failures += 1
                recorder_flush_retries.inc()
                if self._flush_failures >= self._config.max_flush_retries:
                    # Give up on this batch: drop it, reset the counter so
                    # the next batch starts from a clean slate, and tick
                    # the drop metric. Without the reset, a transient
                    # outage that eventually recovers would leave the
                    # counter at N and the FIRST post-recovery failure
                    # would immediately trip the drop path again.
                    recorder_flush_batches_dropped.inc()
                    await logger.aerror(
                        'recorder_flush_batch_dropped',
                        category='recorder',
                        attempt=self._flush_failures,
                        batch_size=len(batch),
                        error=str(exc),
                    )
                    self._flush_failures = 0
                    # Buffer state unchanged (batch already popped); reflect
                    # the (possibly post-concurrent-append) depth.
                    recorder_buffer_size.set(len(self._buffer))
                    return
                # Not yet at the retry cap — re-queue and let the next tick
                # try again. Log at WARNING so the operator sees the retry
                # trail before any drop happens.
                #
                # Overflow detection: ``self._buffer`` is bounded by
                # ``deque(maxlen=max_buffer)``. If a concurrent ``_record``
                # append filled the buffer while we were awaiting the flush,
                # ``extendleft`` will silently evict rows from the TAIL
                # (newest events) to honour ``maxlen``. Those rows are lost
                # without any metric tick in ``_record`` (that path only
                # counts drops on the append side). We measure the potential
                # overflow arithmetically — ``(len_before + batch_len) -
                # maxlen`` — and tick ``recorder_requeue_overflow`` by the
                # number of rows the deque had to discard, so operators can
                # alert on this previously silent data-loss path.
                buffer_len_before = len(self._buffer)
                batch_len = len(batch)
                self._buffer.extendleft(reversed(batch))
                maxlen = self._buffer.maxlen
                if maxlen is not None:
                    overflow = (buffer_len_before + batch_len) - maxlen
                    if overflow > 0:
                        recorder_requeue_overflow.inc(overflow)
                        await logger.awarning(
                            'recorder_buffer_overflow_on_requeue',
                            category='recorder',
                            dropped=overflow,
                            buffer_len_before=buffer_len_before,
                            batch_size=batch_len,
                            max_buffer=maxlen,
                        )
                await logger.awarning(
                    'recorder_flush_retry',
                    category='recorder',
                    attempt=self._flush_failures,
                    max_retries=self._config.max_flush_retries,
                    batch_size=len(batch),
                    error=str(exc),
                )
                recorder_buffer_size.set(len(self._buffer))
                return
            # Success path: reset the retry counter and record the histogram.
            self._flush_failures = 0
            recorder_flush_duration.observe(time.monotonic() - flush_start)
            # Post-drain: the gauge should report the new buffer depth. Usually
            # zero, but a concurrent _record() during the await could have
            # appended in between — reading len(self._buffer) is the correct value.
            recorder_buffer_size.set(len(self._buffer))

    async def _retention_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.rotation_interval_minutes * 60)
            await self._rotate()

    async def _rotate(self) -> None:
        """Rotate: open new DB, initialize it fully, then swap — no schemaless window.

        Ordering matters: if we installed ``self._db = new_db`` before
        creating the schema, a concurrent ``_flush_loop`` iteration (or
        any debug-triggered flush) could execute ``INSERT INTO events``
        against the freshly-opened but still-empty file and hit
        ``no such table: events``. The fix is to prepare ``new_db``
        completely before the atomic swap, so any concurrent writer only
        ever sees a ready connection.
        """
        # flush remaining buffer to current DB
        await self._flush()

        # open new DB and initialize schema BEFORE swapping. If schema
        # creation fails, close the orphaned connection and leave
        # self._db untouched so the caller can retry on the next tick.
        new_path = _make_db_path(self._config.db_dir, self._worker_name)
        new_db = await aiosqlite.connect(new_path)
        try:
            await new_db.execute('PRAGMA journal_mode=WAL')
            await self._create_schema(new_db)
        except Exception:
            # Avoid leaking the fd: close the new connection and re-raise.
            # self._db is still the previous (fully-initialized) writer.
            with contextlib.suppress(Exception):
                await new_db.close()
            raise

        # Open the new reader against the new path. Done BEFORE swapping
        # so a concurrent debug-UI read arriving mid-swap never lands on
        # a half-torn-down reader. If the open fails, abort: close the
        # new writer (since we're no longer installing it) and leave the
        # previous writer/reader pair intact so the worker stays usable.
        try:
            new_reader = await _open_reader(new_path)
        except Exception:
            with contextlib.suppress(Exception):
                await new_db.close()
            raise

        # Atomic swap: once these lines execute, any concurrent flush or
        # UI read sees the new, schema-ready connections. We swap writer
        # first so a concurrent SELECT using the stale reader still runs
        # against a (briefly) valid handle; the stale reader is closed
        # below and its next use would correctly raise/fail through the
        # None check in the debug_server helpers.
        old_db = self._db
        old_reader = self._reader_db
        self._db = new_db
        self._reader_db = new_reader
        self._db_path = new_path
        if self._drakkar_config:
            await self.write_config(self._drakkar_config)
        self._update_live_link()

        if old_reader:
            # Close the old reader before the old writer so any pending
            # SELECT returns/errors promptly rather than blocking the
            # writer close. Failures here are non-fatal — the new reader
            # is already live.
            try:
                await old_reader.close()
            except Exception as exc:
                await logger.awarning(
                    'recorder_old_reader_close_failed',
                    category='recorder',
                    error=str(exc),
                )

        if old_db:
            # Don't let a close failure on the outgoing connection
            # propagate — the new writer is already live and functional.
            try:
                await old_db.close()
            except Exception as exc:
                await logger.awarning(
                    'recorder_old_db_close_failed',
                    category='recorder',
                    error=str(exc),
                )

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
