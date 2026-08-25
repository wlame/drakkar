"""Flight recorder — event log to timestamped SQLite files.

The schemas + standalone helpers (JSON encoding, env-secret sanitization,
DB-file path management, IP detection), the archive engine and the live
fan-out live in sibling modules:

- :mod:`drakkar.recorder.schema`  — DDL constants + canned trace queries.
- :mod:`drakkar.recorder.helpers` — orjson-or-stdlib codec, secret patterns,
  ``format_dt``, ``make_db_path``, ``live_link_path``, ``open_reader``,
  ``detect_worker_ip``.
- :mod:`drakkar.recorder.archive` — window math + the archive pass that
  folds rotated-out DB files into compressed per-cluster archives.
- :mod:`drakkar.recorder.fanout`  — the live ``/ws`` fan-out: subscriber
  set, per-client queues, deferred ``task_started`` events.

This module re-imports the helpers it uses so external code (and
``mock.patch`` test sites) can keep using ``drakkar.recorder.X`` paths.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import functools
import os

# ``socket`` is re-imported here so test patches like
# ``patch('drakkar.recorder.socket.socket', ...)`` still find the module on
# this attribute path. The actual ``detect_worker_ip`` consumer lives in
# :mod:`drakkar.recorder.helpers`, but ``socket`` is a shared module
# reference — patching ``socket.socket`` via either attribute path replaces
# the class globally for both modules.
import socket  # noqa: F401
import sqlite3
import time
import weakref
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import aiosqlite
import structlog

from drakkar.config import UIConfig
from drakkar.dbfiles import WAL_SYNCHRONOUS_PRAGMA, atomic_symlink, remove_symlink, secure_db_file
from drakkar.hostinfo import HostSampler, detect_network_fs
from drakkar.metrics import (
    recorder_buffer_size,
    recorder_dropped_events,
    recorder_flush_batches_dropped,
    recorder_flush_duration,
    recorder_flush_retries,
    recorder_requeue_overflow,
)
from drakkar.recorder.archive import run_archive_pass
from drakkar.recorder.helpers import (
    BUSY_TIMEOUT_MS,
    detect_worker_ip,
    encode_json_str,
    format_dt,
    live_link_path,
    make_db_path,
    open_reader,
    sanitize_env_value,
)
from drakkar.recorder.queries import EventQueries, QueryContext
from drakkar.recorder.schema import (
    EVENT_COLUMNS,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
    WEBAPP_REQUIRED_EVENT_COLUMNS,
    EventType,
    RecorderSchemaError,
)
from drakkar.recorder.writer import EventWriter
from drakkar.utils import redact_url

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig

logger = structlog.get_logger()

# Recorders currently running, for the process-wide last-breath hook below.
# A WeakSet on purpose: holding strong references here would pin every
# abandoned recorder (tests routinely start one and drop it) together with
# its aiosqlite connections — whose worker threads are non-daemon and only
# stop from ``Connection.__del__`` — and a single pinned connection then
# blocks interpreter shutdown in ``threading._shutdown`` forever. Weak
# membership keeps abandoned recorders collectable exactly as before, while
# a production recorder stays reachable through the app and gets flushed.
_LIVE_RECORDERS: weakref.WeakSet[EventRecorder] = weakref.WeakSet()
_LAST_BREATH_REGISTERED = False

# Rows per ``executemany`` in one flush. Two jobs: it bounds how long the
# loop is held building row tuples (one ``dict.get`` per column per event,
# so an unchunked 30k-event flush stalls the loop for tens of milliseconds),
# and it bounds what a flush that keeps failing can lose — a dropped chunk
# instead of the whole buffer. 5k rows is ~4 ms of tuple building and, at 19
# columns, 95k bind parameters, comfortably inside SQLite's limits.
FLUSH_CHUNK_ROWS = 5_000

# How often ``_flush_loop`` looks at the buffer while waiting out its
# interval. Small enough that a burst does not sit until the next tick,
# large enough to be invisible next to the workloads the recorder observes.
FLUSH_POLL_SECONDS = 0.25


def _last_breath_flush_all() -> None:
    """The single atexit hook: salvage every still-live recorder's buffer."""
    for rec in list(_LIVE_RECORDERS):
        rec._last_breath_flush()


def _arm_last_breath_hook() -> None:
    """Register the process-wide hook once, on first recorder start."""
    global _LAST_BREATH_REGISTERED
    if not _LAST_BREATH_REGISTERED:
        atexit.register(_last_breath_flush_all)
        _LAST_BREATH_REGISTERED = True


class EventRecorder(EventWriter):
    """Records processing events to timestamped SQLite database files.

    Events are buffered in memory and flushed periodically. On each
    rotation tick, the current DB is finalized and a new timestamped file
    is created.

    A ``{worker_name}-live.db`` symlink points to the current database
    while the worker is running and is removed on graceful shutdown.

    Which tables are created depends on config flags:
    - ``store_events`` -> ``events`` table
    - ``store_config`` -> ``worker_config`` table (enables autodiscovery)
    - ``store_state`` -> ``worker_state`` table (periodic snapshots)
    """

    MAX_BUFFER = 50_000  # default, overridden by config.ui.recorder.max_buffer

    def __init__(self, config: UIConfig, worker_name: str = 'worker', cluster_name: str = '') -> None:
        super().__init__(config)
        self._worker_name = worker_name
        self._cluster_name = cluster_name
        self._buffer: deque[dict] = deque(maxlen=config.recorder.max_buffer)
        # Whether an appended event will ever reach a table. The flush loop
        # is created only when ``store_events`` is on (and only when there
        # is a ``db_dir`` to open a DB in), and ``_flush`` returns early
        # without one — so in the other configurations an append is a leak:
        # the deque fills to ``max_buffer`` and every further event counts
        # as a drop forever. Both inputs are process-lifetime constants, so
        # this is resolved once here rather than re-read on every event.
        self._persists_events: bool = bool(self._store.store_events and self._store.db_dir)
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
        self._rotation_task: asyncio.Task | None = None
        self._state_task: asyncio.Task | None = None
        self._host_sample_task: asyncio.Task | None = None
        # Databases-page stats cache (drakkar.dbstats). Constructed in
        # start() when db_dir is set; the warmer loop keeps it filled and
        # _rotate feeds it the freshly-immutable file. None in memory-only
        # mode — there is no directory to describe.
        self._dbstats: Any = None
        self._dbstats_warm_task: asyncio.Task | None = None
        # Fire-and-forget rotation scans; tracked so they are not GC'd
        # mid-flight and can be awaited nowhere (their failure only costs
        # a later warmer scan).
        self._dbstats_rotate_tasks: set[asyncio.Task] = set()
        # Host resource sampling (RSS/CPU/fds/pressure/NFS) with the
        # previous-tick state its rate fields need.
        self._host_sampler = HostSampler()
        self._running = False
        self._state_provider: Callable[[], dict] | None = None
        self._drakkar_config: DrakkarConfig | None = None
        # Serialize ``_flush`` calls. The periodic ``_flush_loop`` and
        # debug endpoints (via ``drakkar.concurrency.dispatch_to_loop``)
        # can both schedule flushes on the main loop. Both paths drain
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
        # Read side. Given callables rather than values for the connection
        # and the DB path: both are swapped on every rotation.
        self._queries = EventQueries(
            QueryContext(
                config=config,
                worker_name=worker_name,
                cluster_name=cluster_name,
                reader=lambda: self._reader_db or self._db,
                db_path=lambda: self._db_path,
                flush=self._flush,
            )
        )

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def config(self) -> UIConfig:
        """Read-only access to the UI config — lets the UI server inspect
        ``recorder.store_events`` / ``ws_min_duration_ms`` without reaching
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
        if self._store.store_events:
            await db.executescript(SCHEMA_EVENTS)
        if self._store.store_config:
            await db.executescript(SCHEMA_WORKER_CONFIG)
        if self._store.store_state:
            await db.executescript(SCHEMA_WORKER_STATE)
        await db.commit()

    @staticmethod
    async def _verify_events_schema(db: aiosqlite.Connection, db_path: str) -> None:
        """Ensure any pre-existing ``events`` table carries the webapp columns.

        Reads ``PRAGMA table_info(events)`` from the freshly-opened
        connection and verifies every column listed in
        :data:`WEBAPP_REQUIRED_EVENT_COLUMNS` is present. Two cases:

        * No ``events`` table yet → fresh DB; ``PRAGMA`` returns no
          rows; we return without raising and let ``_create_schema``
          build the up-to-date table below.
        * Existing ``events`` table missing one or more required
          columns → pre-webapp-release DB; raise
          :class:`RecorderSchemaError` with operator guidance (delete
          the file under ``db_dir`` and restart). The exception is
          intentionally not caught anywhere in the recorder layer so
          it propagates through ``AppLifecycle._async_run`` and aborts
          worker startup with the message visible in stderr/logs.

        Running BEFORE ``_create_schema`` is intentional — the
        ``CREATE INDEX`` statements in :data:`SCHEMA_EVENTS` reference
        the new columns (``idx_events_origin``, ``idx_events_request_id``)
        and would themselves fail with ``no such column`` if we let them
        run against a legacy table.
        """
        async with db.execute('PRAGMA table_info(events)') as cur:
            # ``table_info`` rows: (cid, name, type, notnull, dflt_value, pk).
            # Returns an empty result set when the ``events`` table does
            # not exist at all — we treat that as "fresh DB, nothing to
            # verify" and let the caller's ``_create_schema`` build it.
            existing = {row[1] async for row in cur}
        if not existing:
            return
        missing = [c for c in WEBAPP_REQUIRED_EVENT_COLUMNS if c not in existing]
        if missing:
            raise RecorderSchemaError(
                f'Recorder DB at {db_path} predates the webapp release '
                f"(missing columns on 'events' table: {', '.join(missing)}); "
                f'delete it (db_dir is documented as disposable) and '
                f'restart the worker. New rotation-cycle DBs include the '
                f'required columns automatically.'
            )

    async def start(self) -> None:
        self._running = True
        if self._store.db_dir:
            await self._warn_if_db_dir_world_writable()
            await self._warn_if_db_dir_network_fs()
            self._db_path = make_db_path(self._store.db_dir, self._worker_name)
            # Open writer + apply PRAGMA + create schema + open reader in a
            # single try/except. A failure at any of these steps must close
            # whichever connections already opened — otherwise the caller
            # typically doesn't call ``stop()`` on a failed ``start`` and
            # the partially-opened ``_db`` handle leaks its fd + lock.
            try:
                # The DB stores task args + subprocess stdout/stderr, which
                # may carry message-derived data. Create it owner-only
                # BEFORE the driver opens it and turns on WAL: SQLite
                # copies this file's mode onto the -wal/-shm sidecars as it
                # creates them, so the order is what makes those
                # owner-only too. See
                # :func:`drakkar.dbfiles.secure_db_file`.
                secure_db_file(self._db_path)
                self._db = await aiosqlite.connect(self._db_path)
                # WAL mode is what lets a separate reader connection coexist
                # with the writer without serializing reads behind writes.
                # Applied per-connection (SQLite stores the mode in the DB
                # header; the reader picks it up automatically on open).
                await self._db.execute('PRAGMA journal_mode=WAL')
                # One fsync per checkpoint instead of one per commit — see
                # WAL_SYNCHRONOUS_PRAGMA for the durability trade.
                await self._db.execute(WAL_SYNCHRONOUS_PRAGMA)
                # Explicit busy_timeout so shared-db_dir contention behaves
                # identically to the Go backend (which has no driver default).
                await self._db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
                # Webapp-release schema check. Run BEFORE ``_create_schema``
                # so a pre-existing
                # legacy DB is rejected before ``CREATE INDEX`` references
                # a missing column. Detection rule: an existing
                # ``events`` table must already carry every column listed
                # in :data:`WEBAPP_REQUIRED_EVENT_COLUMNS`; missing any
                # raises :class:`RecorderSchemaError` with operator
                # guidance. A fresh DB with no ``events`` table yet skips
                # this check — ``_create_schema`` will create the up-to-
                # date schema below. The exception is intentionally left
                # uncaught so it propagates through ``AppLifecycle._async_run``
                # and aborts worker startup.
                if self._store.store_events:
                    await self._verify_events_schema(self._db, self._db_path)
                await self._create_schema(self._db)
                # Open the dedicated reader connection AFTER schema creation
                # so the reader always sees a ready DB. URI with ``mode=ro``
                # rejects any accidental write attempt through this handle.
                self._reader_db = await open_reader(self._db_path)
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
            # Databases-page stats cache + its warmer. Lazy import keeps
            # drakkar.merge (sync sqlite3 scanning) out of the recorder's
            # import chain until a DB-backed recorder actually starts.
            from drakkar.dbstats import DbStatsCache

            self._dbstats = DbStatsCache(self._store.db_dir)
            self._dbstats_warm_task = asyncio.create_task(self._dbstats_warm_loop())
            self._watch_background_task(self._dbstats_warm_task, 'dbstats_warm')
            if self._store.store_events:
                self._flush_task = asyncio.create_task(self._flush_loop())
                self._watch_background_task(self._flush_task, 'flush')
            self._rotation_task = asyncio.create_task(self._rotation_loop())
            self._watch_background_task(self._rotation_task, 'rotation')
            if self._store.store_state:
                self._state_task = asyncio.create_task(self._state_sync_loop())
                self._watch_background_task(self._state_task, 'state_sync')
            if not self._store.archive_enabled:
                # Archiving is the only thing that removes rotated files, so
                # opting out means db_dir grows until the operator prunes it.
                await logger.ainfo(
                    'recorder_archiving_disabled',
                    category='recorder',
                    db_dir=self._store.db_dir,
                    hint='raw recorder database files are never deleted automatically; '
                    'prune db_dir yourself or set ui.recorder.archive_enabled: true',
                )
        # Outside the db_dir branch on purpose: resource samples still feed
        # WS subscribers, so the sampler runs in memory-only mode too.
        self._host_sample_task = asyncio.create_task(self._host_sample_loop())
        self._watch_background_task(self._host_sample_task, 'host_sample')
        # Armed for the whole run, disarmed by a clean stop(): if the
        # interpreter exits any other way (startup failure after this
        # point, an unhandled exception, sys.exit), the last-breath hook
        # salvages whatever the flush loop had not written yet. Membership
        # is weak — see _LIVE_RECORDERS for why that is load-bearing.
        _arm_last_breath_hook()
        _LIVE_RECORDERS.add(self)
        await logger.ainfo(
            'recorder_started',
            category='recorder',
            db_path=self._db_path or '(memory only)',
        )

    def _watch_background_task(self, task: asyncio.Task, name: str) -> None:
        """Log when a recorder background loop ends while still running.

        Last-resort tripwire. The loops guard each iteration, so reaching
        this callback means something outside the guarded work ended the
        task — and a dead loop is invisible otherwise: events stop being
        persisted, DBs stop rotating, and no log line says why. Cancelled
        tasks and deaths after ``stop`` (``self._running`` false) are the
        normal shutdown path and stay silent.
        """

        def on_done(finished: asyncio.Task) -> None:
            if finished.cancelled() or not self._running:
                return
            exc = finished.exception()
            if exc is None:
                return
            # Sync logger: done callbacks run on the loop and cannot await.
            logger.error(
                'recorder_background_task_died',
                category='recorder',
                task=name,
                error=str(exc),
                error_type=type(exc).__name__,
                exc_info=exc,
            )

        task.add_done_callback(on_done)

    async def _warn_if_db_dir_world_writable(self) -> None:
        """Warn when the recorder DB directory is world-writable (e.g. /tmp).

        Recorder DBs persist task args and subprocess output. In a
        world-writable directory other local users can pre-create or
        symlink-swap files, and (before the post-create chmod) read DB
        contents. Best-effort: a stat failure is ignored — the directory
        may not exist yet and ``make_db_path`` / connect handle that.
        """
        try:
            mode = os.stat(self._store.db_dir).st_mode
        except OSError:
            return
        if mode & 0o002:
            await logger.awarning(
                'recorder_db_dir_world_writable',
                category='recorder',
                db_dir=self._store.db_dir,
                hint='recorder DBs store task args and subprocess output; '
                'point debug.db_dir at a directory owned by the worker user '
                '(e.g. /var/lib/drakkar) on shared hosts',
            )

    async def _warn_if_db_dir_network_fs(self) -> None:
        """Warn when the recorder DB directory sits on a network filesystem.

        SQLite on NFS/CIFS is a double hazard: file locking is unreliable
        (corruption risk), and every flush shares fate with the network
        path — when the storage server degrades, the recorder's writes
        block ON THE EVENT LOOP's flush cadence, so the observability
        stack degrades exactly when it is needed most. Warning only; the
        operator may accept the trade-off knowingly.
        """
        hit = detect_network_fs(self._store.db_dir)
        if hit is None:
            return
        mount, fstype = hit
        await logger.awarning(
            'recorder_db_dir_network_fs',
            category='recorder',
            db_dir=self._store.db_dir,
            mount=mount,
            fstype=fstype,
            hint='recorder databases on a network filesystem share fate with '
            'the storage server and risk SQLite lock corruption; point '
            'ui.recorder.db_dir at a local disk',
        )

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        """Append event to buffer and broadcast to WS subscribers.

        Buffering is skipped entirely when this recorder will never persist
        events (memory-only ``db_dir: ''``, or ``store_events: false``):
        nothing would ever drain the deque, so it would pin ``max_buffer``
        events — ``task_completed`` carries the whole subprocess stdout and
        stderr — and report every event after that as a drop. The WS
        fan-out below still runs; the live UI is the point of those modes.

        Observability: when the deque is already at capacity, the next
        append will silently evict the oldest entry. We detect that case
        BEFORE the append (``len == maxlen``) and increment the drop
        counter once per dropped event. After the append, the buffer
        gauge is refreshed to the new depth so operators can watch for
        contention without scraping internal state.
        """
        event['dt'] = format_dt(event['ts'])
        if not skip_db and self._persists_events:
            # deque(maxlen=N) accepts up to N items; when len == maxlen a
            # subsequent append drops the leftmost entry. We count one
            # drop per event lost — prometheus_client Counter.inc() is
            # thread-safe so callers from multiple loops/threads are fine.
            maxlen = self._buffer.maxlen
            if maxlen is not None and len(self._buffer) >= maxlen:
                recorder_dropped_events.inc()
            self._buffer.append(event)
            recorder_buffer_size.set(len(self._buffer))
        if not skip_ws:
            self._fanout.broadcast(event)

    def _update_live_link(self) -> None:
        """Create or update the {worker}-live.db symlink to the current DB."""
        if not self._store.db_dir or not self._db_path:
            return
        atomic_symlink(
            live_link_path(self._store.db_dir, self._worker_name),
            os.path.basename(self._db_path),
        )

    def _remove_live_link(self) -> None:
        """Remove the live symlink on graceful shutdown."""
        if not self._store.db_dir:
            return
        remove_symlink(live_link_path(self._store.db_dir, self._worker_name))

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
        if not self._db or not self._store.store_config:
            return
        env_vars = {name: sanitize_env_value(name, os.environ.get(name, '')) for name in self._config.expose_env_vars}
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
                self._config.public_url or None,
                redact_url(drakkar_config.kafka.brokers),
                drakkar_config.kafka.source_topic,
                drakkar_config.kafka.consumer_group,
                drakkar_config.executor.binary_path,
                drakkar_config.executor.max_executors,
                drakkar_config.executor.task_timeout_seconds,
                drakkar_config.executor.max_retries,
                drakkar_config.executor.window_size,
                encode_json_str(sinks),
                encode_json_str(env_vars),
                now,
                format_dt(now),
            ],
        )
        await self._db.commit()

    # --- Worker state (periodic snapshots) ---

    async def _state_sync_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._store.state_sync_interval_seconds)
            # Same per-iteration guard as ``_flush_loop``: one bad snapshot
            # (a state provider that raises, a locked DB) must not end the
            # loop and freeze ``worker_state`` for the rest of the run.
            try:
                await self._sync_state()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await logger.aerror(
                    'recorder_state_sync_loop_iteration_failed',
                    category='recorder',
                    error=str(exc),
                    error_type=type(exc).__name__,
                    exc_info=exc,
                )

    async def _dbstats_warm_loop(self) -> None:
        """Keep the databases-page stats cache warm and purged.

        Each sweep (thread-offloaded — it is sync sqlite3 work) computes
        stats for files the cache does not know yet and drops rows for
        files an operator deleted from ``db_dir``. When everything is
        cached the sweep costs one directory listing and one SELECT, so
        a short interval is fine. The first sweep runs immediately, so a
        worker booting over a pre-existing directory starts warming
        before anyone opens the page.

        The sweep re-reads only THIS worker's live database. Live files
        change constantly, so a sweep that refreshed every live file made
        each worker read every co-located worker's growing DB once a
        minute — N-squared reads across the fleet, over a directory that
        is typically network-mounted, for a page nobody may have open.
        Those rows carry stdout/stderr, so each scan touches many pages.
        The stats cache is shared, so every worker warming its own file is
        enough to keep the whole page warm; a page request still refreshes
        every row on demand.
        """
        from drakkar.dbstats import warm_directory

        while self._running:
            try:
                _scanned, purged = await asyncio.to_thread(
                    functools.partial(
                        warm_directory,
                        self._store.db_dir,
                        self._dbstats,
                        own_live_db=self._db_path,
                    )
                )
                if purged:
                    await logger.ainfo(
                        'recorder_dbstats_purged',
                        category='recorder',
                        purged=purged,
                        detail='dropped cached stats for database files removed from db_dir',
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Same per-iteration guard as the other loops: one bad sweep
                # must not end warming for the rest of the run.
                await logger.awarning(
                    'recorder_dbstats_warm_failed',
                    category='recorder',
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            await asyncio.sleep(self._store.dbstats_warm_interval_seconds)

    async def _host_sample_loop(self) -> None:
        """Per-tick host sampling: one ``resource_sample`` event per
        state-sync interval.

        Runs whenever the recorder runs — memory-only mode included (the
        events then only feed WS subscribers; nothing touches the DB from
        here). Each sampler is best-effort: a platform without a given
        ``/proc`` source simply omits that field.
        """
        while self._running:
            await asyncio.sleep(self._store.state_sync_interval_seconds)
            # Same per-iteration guard as the other loops: one bad sample
            # must not end the sampler for the rest of the run.
            try:
                self._resource_sample()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await logger.awarning(
                    'recorder_host_sample_failed',
                    category='recorder',
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    def _resource_sample(self) -> None:
        """Record one ``resource_sample`` event from the host sampler.

        The recorder's whole job here is the write: reading the host and
        deriving the rate fields lives in
        :class:`drakkar.hostinfo.HostSampler`.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.RESOURCE_SAMPLE,
                'metadata': encode_json_str(self._host_sampler.sample()),
            }
        )

    async def _sync_state(self) -> None:
        if not self._db or not self._store.store_state:
            return
        app_state = self._state_provider() if self._state_provider else {}
        now = time.time()
        await self._db.execute(
            """INSERT INTO worker_state
               (uptime_seconds, assigned_partitions, partition_count,
                pool_active, pool_max, total_queued,
                consumed_count, completed_count, failed_count,
                produced_count, committed_count, paused,
                health_state, loop_lag_ms, throughput, updated_at, updated_at_dt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                app_state.get('uptime_seconds', 0),
                encode_json_str(app_state.get('assigned_partitions', [])),
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
                # NULL when the runtime-health monitor is off (v1.15).
                app_state.get('health_state'),
                app_state.get('loop_lag_ms'),
                # NULL when throughput.cost_label is off (v1.16).
                encode_json_str(app_state['throughput']) if app_state.get('throughput') else None,
                now,
                format_dt(now),
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
        if self._rotation_task:
            self._rotation_task.cancel()
            try:
                await self._rotation_task
            except asyncio.CancelledError:
                pass
        if self._state_task:
            self._state_task.cancel()
            try:
                await self._state_task
            except asyncio.CancelledError:
                pass
        if self._host_sample_task:
            self._host_sample_task.cancel()
            try:
                await self._host_sample_task
            except asyncio.CancelledError:
                pass
        if self._dbstats_warm_task:
            self._dbstats_warm_task.cancel()
            try:
                await self._dbstats_warm_task
            except asyncio.CancelledError:
                pass
        # Drop any pending deferred start events and disarm the shared sweep.
        self._fanout.close()

        # Final flush + state sync are best-effort: a failure in either must
        # not skip the DB closes, live-link removal, and _LIVE_RECORDERS
        # discard below, or shutdown would leak connections and interpreter
        # exit would try to flush into a recorder that never tore down.
        try:
            await self._flush()
        except Exception as exc:
            await logger.awarning(
                'recorder_final_flush_failed',
                category='recorder',
                error=str(exc),
            )
        try:
            await self._sync_state()
        except Exception as exc:
            await logger.awarning(
                'recorder_final_state_sync_failed',
                category='recorder',
                error=str(exc),
            )
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
        # Clean shutdown flushed everything above — leave the live set so
        # interpreter exit does not reopen the closed DB. Discarding a
        # never-added recorder (start() not reached) is a no-op.
        _LIVE_RECORDERS.discard(self)
        await logger.ainfo('recorder_stopped', category='recorder')

    def _last_breath_flush(self) -> None:
        """Synchronous, best-effort buffer flush at interpreter exit.

        Reached through the process-wide atexit hook while this recorder
        is in ``_LIVE_RECORDERS`` (added by :meth:`start`, removed by a
        clean :meth:`stop`). It fires exactly when the async machinery
        could not do its job — a startup failure after the recorder came
        up, an unhandled exception, a stray ``sys.exit`` —
        and writes the still-buffered rows straight through :mod:`sqlite3`,
        because at interpreter exit the event loop (and aiosqlite's worker
        thread) are already gone. The most interesting events of a dying
        worker are the last ones; this is what keeps them.

        Best-effort by design: a short busy timeout, every failure path
        silent. Cannot fire on SIGKILL / OOM — the watchdog file covers
        detecting those, and whatever the last periodic flush wrote is
        what remains.
        """
        if not self._running or not self._buffer or not self._db_path or not self._store.store_events:
            return
        batch = list(self._buffer)
        self._buffer.clear()

        columns = list(EVENT_COLUMNS[1:])  # everything but the autoincrement id
        query = f'INSERT INTO events ({", ".join(columns)}) VALUES ({", ".join(["?"] * len(columns))})'
        # Same origin coercion as the async flush: NOT NULL column, and the
        # Kafka-path record helpers legitimately leave it unset.
        rows = [
            tuple('kafka' if (col == 'origin' and entry.get(col) is None) else entry.get(col) for col in columns)
            for entry in batch
        ]
        try:
            conn = sqlite3.connect(self._db_path, timeout=1.0)
            try:
                conn.executemany(query, rows)
                conn.commit()
            finally:
                conn.close()
        except Exception:
            # Interpreter teardown is no place to raise; the periodic
            # flushes already wrote everything they could.
            return
        with contextlib.suppress(Exception):
            logger.warning(
                'recorder_last_breath_flush',
                category='recorder',
                events=len(batch),
                db_path=self._db_path,
                detail='process exited without a clean recorder stop; buffered events were salvaged synchronously',
            )

    # --- Recording methods (sync, append to buffer) ---

    @property
    def queries(self) -> EventQueries:
        """The read side (local + cross-worker queries)."""
        return self._queries

    # --- Query API (delegated to :class:`EventQueries`) ---
    #
    # These stay on the recorder because every caller — the UI routes, the
    # webapp debug pages, the tests — reaches the read side through the
    # recorder it already holds. The bodies live in
    # :mod:`drakkar.recorder.queries`.

    async def get_events(
        self,
        partition: int | None = None,
        event_type: str | None = None,
        since: float | None = None,
        origin: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Recent events, newest first, filtered by the given criteria."""
        return await self._queries.get_events(
            partition=partition,
            event_type=event_type,
            since=since,
            origin=origin,
            limit=limit,
            offset=offset,
        )

    async def trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Find all events for tasks matching a label key-value pair."""
        return await self._queries.trace_by_label(label_key, label_value)

    async def cross_trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Trace by label across all workers in the same cluster."""
        return await self._queries.cross_trace_by_label(label_key, label_value)

    async def get_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Get the full lifecycle of a message by partition and offset."""
        return await self._queries.get_trace(partition, msg_offset)

    async def cross_trace(self, partition: int, msg_offset: int) -> list[dict]:
        """Trace a message across all workers in the same cluster."""
        return await self._queries.cross_trace(partition, msg_offset)

    async def get_task_events(self, task_id: str) -> list[dict]:
        """Get all events for a specific task_id, ordered chronologically."""
        return await self._queries.get_task_events(task_id)

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        return await self._queries.get_partition_summary()

    async def get_stats(self) -> dict:
        """Get overall statistics from in-memory counters (accumulated since worker start)."""
        stats = dict(self._counters)
        stats['total_events'] = sum(self._counters.values())
        return stats

    async def discover_workers(self) -> list[dict]:
        """List the other workers of this cluster, with their liveness."""
        return await self._queries.discover_workers()

    # --- Internal flush/retention ---

    async def _wait_until_flush_due(self) -> None:
        """Wait out ``flush_interval_seconds``, or return early on a burst.

        The interval alone is not enough at high event rates: a producer
        filling the buffer faster than one interval's worth would reach
        ``max_buffer`` and start evicting between ticks. Returning as soon as
        the buffer is half full turns that into an early write instead of
        lost events. Polling rather than an ``asyncio.Event`` because
        ``_record`` is also reached from the webapp's own event loop, and
        ``Event.set`` is not safe across loops.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._store.flush_interval_seconds
        maxlen = self._buffer.maxlen
        high_water = maxlen // 2 if maxlen is not None else None
        while self._running:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, FLUSH_POLL_SECONDS))
            if high_water is not None and len(self._buffer) >= high_water:
                return

    async def _flush_loop(self) -> None:
        while self._running:
            await self._wait_until_flush_due()
            # Per-iteration guard: an unexpected raise here used to end the
            # task, so the recorder stopped persisting events for the rest
            # of the process lifetime with nothing in the log. Cancellation
            # must still propagate — that is how ``stop`` ends the loop.
            try:
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await logger.aerror(
                    'recorder_flush_loop_iteration_failed',
                    category='recorder',
                    error=str(exc),
                    error_type=type(exc).__name__,
                    exc_info=exc,
                )

    async def _flush(self) -> None:
        """Write the buffered events to the ``events`` table, in chunks.

        Chunking is what keeps the flush off the loop's critical path. The
        whole buffer used to be turned into row tuples in one comprehension:
        at the target workload a 5 s interval holds tens of thousands of
        events, each costing one ``dict.get`` per column, so the loop stalled
        for tens of milliseconds every flush and runtime health reported it
        as lag. Rows are now built one ``FLUSH_CHUNK_ROWS`` slice at a time,
        and the ``await`` on each chunk's write hands the loop back in
        between.

        The chunk is also the unit of loss. A chunk that still fails after
        ``max_flush_retries`` is dropped; before chunking that was the entire
        buffer, so a fifteen-second SQLite hiccup on a network filesystem
        could cost every event held. See ``docs/observability.md`` for the
        ``max_buffer`` sizing rule.
        """
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
            columns = list(EVENT_COLUMNS[1:])  # everything but the autoincrement id
            placeholders = ', '.join(['?'] * len(columns))
            col_names = ', '.join(columns)
            query = f'INSERT INTO events ({col_names}) VALUES ({placeholders})'

            # Observe the full flush body across every chunk — the histogram
            # surfaces disk-I/O latency tail so operators can alert on p99
            # regressions before the buffer backs up enough to drop events.
            flush_start = time.monotonic()
            # Snapshot the depth: events appended while this flush awaits
            # ride along on the next tick rather than extending this one
            # indefinitely under a fast producer.
            pending = len(self._buffer)
            while pending > 0:
                take = min(pending, FLUSH_CHUNK_ROWS)
                pending -= take
                # Take a LOCAL snapshot of the rows to flush BEFORE the DB
                # write. If the write fails (sqlite3.Error) or is interrupted
                # (cancellation) we re-queue this exact list at the FRONT of
                # the buffer so ordering is preserved and no events are lost.
                # A concurrent ``_record`` append lands at the END of the
                # deque (we drain via ``popleft``) and is unaffected.
                batch: list[dict] = [self._buffer.popleft() for _ in range(take)]
                if not await self._write_chunk(db, query, columns, batch):
                    return
            # Success path: reset the retry counter and record the histogram.
            self._flush_failures = 0
            recorder_flush_duration.observe(time.monotonic() - flush_start)
            # Post-drain: the gauge should report the new buffer depth. Usually
            # zero, but a concurrent _record() during the await could have
            # appended in between — reading len(self._buffer) is the correct value.
            recorder_buffer_size.set(len(self._buffer))

    async def _write_chunk(
        self,
        db: aiosqlite.Connection,
        query: str,
        columns: list[str],
        batch: list[dict],
    ) -> bool:
        """Write one chunk; return whether the flush should keep going.

        ``False`` means this chunk did not land: it was either re-queued for
        the next tick or dropped at the retry cap. Either way the rest of the
        buffer stays put — the events still in it are strictly newer, so
        stopping here preserves order.
        """
        # ``origin`` is NOT NULL in the schema with a DEFAULT of
        # ``'kafka'`` — but ``entry.get(col)`` would yield ``None``
        # when the recording site didn't populate it (Kafka-path
        # helpers below ``record_task_*`` such as ``record_consumed``,
        # ``record_arranged``, ``record_committed``, etc., legitimately
        # leave the new columns out). Coerce ``None`` -> ``'kafka'``
        # at INSERT time so SQLite's column DEFAULT does not need to
        # fight an explicit NULL.
        rows = [
            tuple('kafka' if (col == 'origin' and entry.get(col) is None) else entry.get(col) for col in columns)
            for entry in batch
        ]
        # ``batch_settled`` flips once the popped batch has a final home:
        # committed, re-queued for retry, or deliberately dropped at the
        # retry cap. Any other exit — a CancelledError from a caller's
        # timeout or from ``stop()`` cancelling the flush task, or an
        # unexpected error — reaches the ``finally`` with the flag still
        # False and re-queues the batch, so cancellation mid-write can
        # never silently lose it. (No rollback is attempted there: the
        # write may have landed in the connection's open transaction, so
        # a retried batch can at worst duplicate rows — acceptable for a
        # flight recorder, unlike silent loss.)
        batch_settled = False
        try:
            await db.executemany(query, rows)
            await db.commit()
            batch_settled = True
            return True
        except sqlite3.Error as exc:
            # ``sqlite3.Error``, not just OperationalError: aiosqlite
            # raises the stdlib sqlite3 exception classes, and the whole
            # DatabaseError/IntegrityError/ProgrammingError family must
            # go through the same retry/drop accounting rather than
            # bypassing it.
            #
            # Transient DB errors (``database is locked``, ``disk I/O
            # error``, ENOSPC, WAL corruption, etc.) should not cost us
            # the batch. Re-queue the snapshot at the FRONT of the
            # buffer and let the next flush tick retry.
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
            if self._flush_failures >= self._store.max_flush_retries:
                # Give up on this chunk: drop it, reset the counter so
                # the next chunk starts from a clean slate, and tick
                # the drop metric. Without the reset, a transient
                # outage that eventually recovers would leave the
                # counter at N and the FIRST post-recovery failure
                # would immediately trip the drop path again.
                # Bookkeeping runs before the ``await`` on the logger so
                # a cancellation during the log cannot make the finally
                # re-queue a batch we decided to drop.
                batch_settled = True
                attempt = self._flush_failures
                self._flush_failures = 0
                recorder_flush_batches_dropped.inc()
                # Buffer state unchanged (batch already popped); reflect
                # the (possibly post-concurrent-append) depth.
                recorder_buffer_size.set(len(self._buffer))
                await logger.aerror(
                    'recorder_flush_batch_dropped',
                    category='recorder',
                    attempt=attempt,
                    batch_size=len(batch),
                    error=str(exc),
                )
                return False
            # Not yet at the retry cap — re-queue and let the next tick
            # try again. Log at WARNING so the operator sees the retry
            # trail before any drop happens. The re-queue itself is
            # synchronous and flips ``batch_settled`` before the log
            # await, so a cancellation during the log cannot re-queue
            # the batch a second time in the finally.
            self._requeue_front(batch)
            batch_settled = True
            await logger.awarning(
                'recorder_flush_retry',
                category='recorder',
                attempt=self._flush_failures,
                max_retries=self._store.max_flush_retries,
                batch_size=len(batch),
                error=str(exc),
            )
            return False
        finally:
            if not batch_settled:
                # Interrupted mid-write — typically CancelledError from a
                # caller's timeout or from ``stop()`` cancelling the flush
                # task — with the transaction not committed. Put the batch
                # back so the next flush tick retries it, and let the
                # exception propagate. Everything here is synchronous:
                # awaiting during a cancellation unwind could be cancelled
                # again and skip the re-queue.
                self._requeue_front(batch)

    def _requeue_front(self, batch: list[dict]) -> None:
        """Put a popped-but-unflushed batch back at the FRONT of the buffer.

        ``extendleft`` reverses its iterable, so ``batch`` is pre-reversed to
        preserve the original FIFO ordering within the re-queued rows. Rows
        that a concurrent ``_record`` appended while the flush awaited stay
        at the back — correct, since they are strictly newer.

        Overflow detection: ``self._buffer`` is bounded by
        ``deque(maxlen=max_buffer)``. If concurrent appends filled the buffer
        during the flush's await window, ``extendleft`` silently evicts rows
        from the TAIL (newest events) to honour ``maxlen``. Those rows are
        lost without any metric tick in ``_record`` (that path only counts
        drops on the append side), so the potential overflow is measured
        arithmetically — ``(len_before + batch_len) - maxlen`` — and ticked
        on ``recorder_requeue_overflow`` so operators can alert on this
        otherwise silent data-loss path.

        Fully synchronous (sync logging included): it also runs from the
        cancellation unwind in ``_flush``, where an await could itself be
        cancelled and skip the re-queue.
        """
        buffer_len_before = len(self._buffer)
        batch_len = len(batch)
        self._buffer.extendleft(reversed(batch))
        maxlen = self._buffer.maxlen
        if maxlen is not None:
            overflow = (buffer_len_before + batch_len) - maxlen
            if overflow > 0:
                recorder_requeue_overflow.inc(overflow)
                logger.warning(
                    'recorder_buffer_overflow_on_requeue',
                    category='recorder',
                    dropped=overflow,
                    buffer_len_before=buffer_len_before,
                    batch_size=batch_len,
                    max_buffer=maxlen,
                )
        recorder_buffer_size.set(len(self._buffer))

    async def _rotation_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._store.rotation_interval_hours * 3600)
            # Same guard as ``_flush_loop``: one failed rotation must cost a
            # single interval, not every rotation from here on. The archive
            # pass shares the guard — it runs on the freshly rotated-out
            # files, so a failure there is equally per-tick recoverable.
            try:
                await self._rotate()
                await self._archive_pass()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await logger.aerror(
                    'recorder_rotation_loop_iteration_failed',
                    category='recorder',
                    error=str(exc),
                    error_type=type(exc).__name__,
                    exc_info=exc,
                )

    async def _archive_pass(self) -> None:
        """Fold due windows of rotated-out DB files into compressed archives.

        The whole pass — directory scan, per-file SQLite reads, merge and
        gzip — is blocking work measured in seconds, so it goes to a
        thread. The event loop only ever sees this await.
        """
        if not self._store.archive_enabled or not self._store.db_dir:
            return
        await asyncio.to_thread(
            run_archive_pass,
            db_dir=self._store.db_dir,
            worker_name=self._worker_name,
            cluster=self._cluster_name,
            cfg=self._store,
            # The live DB is the file we are writing right now; the pass
            # must never merge it away underneath us.
            exclude_path=self._db_path,
        )

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
        new_path = make_db_path(self._store.db_dir, self._worker_name)
        # Same ordering contract as ``start``: owner-only before the driver
        # opens the file, so the rotated DB's sidecars are created
        # owner-only too. Rotation previously skipped this entirely, so
        # every post-rotation file was world-readable.
        secure_db_file(new_path)
        new_db = await aiosqlite.connect(new_path)
        try:
            await new_db.execute('PRAGMA journal_mode=WAL')
            # Re-applied per connection: unlike journal_mode, synchronous is
            # not stored in the database header.
            await new_db.execute(WAL_SYNCHRONOUS_PRAGMA)
            await new_db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
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
            new_reader = await open_reader(new_path)
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
        old_path = self._db_path
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

        # The just-closed file is immutable from here on — precompute its
        # databases-page stats now (thread, fire-and-forget) so the page
        # never pays a full scan for it. Failures only cost a later
        # warmer-sweep retry, hence no await and no error handling beyond
        # the task set keeping the reference alive.
        if self._dbstats is not None and old_path:
            from drakkar.dbstats import scan_and_store

            task = asyncio.create_task(asyncio.to_thread(scan_and_store, old_path, self._dbstats))
            self._dbstats_rotate_tasks.add(task)
            task.add_done_callback(self._dbstats_rotate_tasks.discard)

        await logger.ainfo('recorder_rotated', category='recorder', new_db=self._db_path)
