"""Flight recorder — event log to timestamped SQLite files.

The schemas + standalone helpers (JSON encoding, env-secret sanitization,
DB-file path management, IP detection) and the archive engine live in
three sibling modules:

- :mod:`drakkar.recorder.schema`  — DDL constants + canned trace queries.
- :mod:`drakkar.recorder.helpers` — orjson-or-stdlib codec, secret patterns,
  ``format_dt``, ``make_db_path``, ``live_link_path``, ``open_reader``,
  ``detect_worker_ip``.
- :mod:`drakkar.recorder.archive` — window math + the archive pass that
  folds rotated-out DB files into compressed per-cluster archives.

This module re-imports the helpers it uses so external code (and
``mock.patch`` test sites) can keep using ``drakkar.recorder.X`` paths.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import glob
import json
import os
import queue
import resource

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
from collections.abc import Callable, Collection
from dataclasses import dataclass, field

# Bound separately from the ``import queue`` above: WSSubscriber has a field
# NAMED ``queue``, which shadows the module inside the class body and makes
# the annotation there unresolvable. Method bodies are unaffected (class
# attributes are not in their lookup chain), so ``queue.Empty`` and
# ``queue.Full`` still read from the module.
from queue import Queue
from typing import TYPE_CHECKING, Any

import aiosqlite
import structlog
from pydantic import BaseModel

from drakkar.config import UIConfig
from drakkar.dbfiles import secure_db_file
from drakkar.hostinfo import (
    detect_network_fs,
    read_cpu_throttle,
    read_loadavg,
    read_net_io_bytes,
    read_nfs_io_bytes,
    read_nfs_mount_stats,
    read_open_fd_count,
    read_pressure,
    read_self_stats,
)
from drakkar.metrics import (
    recorder_buffer_size,
    recorder_dropped_events,
    recorder_flush_batches_dropped,
    recorder_flush_duration,
    recorder_flush_retries,
    recorder_requeue_overflow,
    recorder_ws_dropped_events,
)
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)
from drakkar.peer_discovery import discover_peer_dbs
from drakkar.recorder.archive import run_archive_pass
from drakkar.recorder.helpers import (
    BUSY_TIMEOUT_MS,
    detect_worker_ip,
    encode_json_str,
    encode_ws_event,
    format_dt,
    live_link_path,
    make_db_path,
    open_reader,
    sanitize_env_value,
)
from drakkar.recorder.schema import (
    _LABEL_TRACE_QUERY,
    _TRACE_QUERY,
    EVENT_COLUMNS,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
    WEBAPP_REQUIRED_EVENT_COLUMNS,
    RecorderSchemaError,
)
from drakkar.timefmt import format_rfc3339_micro
from drakkar.utils import redact_url

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig
    from drakkar.webapp.models import WebRequestContext

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

# Upper bound on the deferred-start sweep period. One sweep expires every
# task_started event whose ws_min_duration_ms threshold has passed, instead of
# one timer per task — the difference between ~10 and ~1200 timer operations
# per second on the main loop for a handler that fans one message out to a
# thousand tasks. The actual period is also capped by the threshold itself
# (see ``EventRecorder._deferred_sweep_interval``).
DEFERRED_SWEEP_INTERVAL_SECONDS = 0.1

# Per-subscriber queue depth. Reached only when a client cannot keep up;
# beyond it events are dropped for that client alone so the recording hot
# path never blocks on a slow browser.
WS_SUBSCRIBER_QUEUE_SIZE = 10_000


class LiveEvent:
    """One recorded event on its way to the live stream, encoded on demand.

    A single instance is shared by every subscriber that wants the event, so
    the JSON encode happens **once no matter how many browsers are open**.
    Previously each ``/ws`` coroutine ran its own ``json.dumps`` over the
    same dict, making serialization cost scale with tab count.

    Encoding is deferred to first read rather than done at fan-out, which
    keeps it on the UI-server thread instead of the main loop — the
    pipeline pays only for one small object allocation per event.

    Two coroutines on the UI loop can reach :attr:`text` before either
    stores its result; there is no ``await`` between the check and the
    assignment, so the worst case is a duplicated encode producing an
    identical string. Not worth a lock.
    """

    __slots__ = ('_text', 'event')

    def __init__(self, event: dict) -> None:
        self.event = event
        self._text: str | None = None

    @property
    def text(self) -> str:
        """The event as the JSON text sent over the wire."""
        if self._text is None:
            self._text = encode_ws_event(self.event)
        return self._text


@dataclass(eq=False)
class WSSubscriber:
    """One live ``/ws`` client's slot in the recorder's fan-out.

    ``eq=False`` keeps identity hashing, so instances can live in a ``set``
    and be removed by identity on unsubscribe.

    ``event_types``
        Optional allowlist of ``event`` names. ``None`` streams everything.
        Set by the client so a page pays only for the events it renders.

    ``queue``
        Thread-safe hand-off from the main loop (which records) to the UI
        server thread (which sends), carrying :class:`LiveEvent` wrappers.
        Prefer the accessors below over touching it directly.

    ``dropped``
        Events discarded because ``queue`` was full. The ``/ws`` route
        reports and clears this, letting the browser resync deliberately
        instead of drifting out of sync with no signal. Incremented from the
        main loop and cleared from the UI thread; a lost increment across
        that boundary would at worst delay one resync by a frame, so the
        plain ``int`` needs no lock.
    """

    event_types: frozenset[str] | None = None
    queue: Queue = field(default_factory=lambda: Queue(maxsize=WS_SUBSCRIBER_QUEUE_SIZE))
    dropped: int = 0

    def get_nowait(self) -> dict:
        """Pop the next event as the dict the client receives.

        Decoded from the wire text, not handed back from the recorder's
        own dict — so what this returns is exactly what the browser sees,
        including the omission of captured subprocess output. Callers that
        stream (the ``/ws`` route) use :meth:`drain_encoded` and never pay
        for the decode.
        """
        return json.loads(self.queue.get_nowait().text)

    def drain_encoded(self, limit: int) -> list[str]:
        """Pop up to ``limit`` events as pre-encoded JSON text.

        The streaming path: it never materializes the dicts, and the
        encoded text is shared with every other subscriber holding the same
        :class:`LiveEvent`.
        """
        out: list[str] = []
        try:
            while len(out) < limit:
                out.append(self.queue.get_nowait().text)
        except queue.Empty:
            pass
        return out

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()

    def take_dropped(self) -> int:
        """Return the drop count since the last call and reset it to zero.

        Subtracts rather than zeroing so an increment landing between the
        read and the write (from the main loop) is not lost.
        """
        n = self.dropped
        if n:
            self.dropped -= n
        return n


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


def _line_count(text: str) -> int:
    """Logical lines in a stream capture: a trailing unterminated line counts."""
    if not text:
        return 0
    return text.count('\n') + (0 if text.endswith('\n') else 1)


def _capped_stdin(stdin: str, max_bytes: int) -> tuple[str, bool]:
    """Cap stored stdin at ``max_bytes`` (0 = unlimited), UTF-8 safe.

    The cut happens on the encoded bytes, then decodes with errors ignored so
    a multi-byte character split by the cap is dropped rather than mangled.
    """
    encoded = stdin.encode()
    if max_bytes <= 0 or len(encoded) <= max_bytes:
        return stdin, False
    return encoded[:max_bytes].decode(errors='ignore'), True


def _failed_metadata(task: ExecutorTask, error: ExecutorError, stdin_max_bytes: int) -> dict:
    """Metadata for a task_failed event: the exception, plus the task's stdin.

    stdin is stored on EVERY failure (capped), independent of the opt-in
    ``store_stdin`` flag — a failure without its input is half a fingerprint,
    and failures are rare enough that the write cost does not matter.
    """
    metadata: dict = {'exception': error.exception}
    if task.stdin:
        stored_stdin, truncated = _capped_stdin(task.stdin, stdin_max_bytes)
        metadata['stdin'] = stored_stdin
        if truncated:
            metadata['stdin_truncated'] = True
    return metadata


class EventRecorder:
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
        self._config = config
        # The persistence tier (ui.recorder.*) is read on hot paths — bind
        # it once so those reads don't repeat the attribute hop.
        self._store = config.recorder
        self._worker_name = worker_name
        self._cluster_name = cluster_name
        self._buffer: deque[dict] = deque(maxlen=config.recorder.max_buffer)
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
        self._netio_task: asyncio.Task | None = None
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
        # Previous /proc/net/dev totals + monotonic read time; the pair a
        # net_io sample diffs against. None until the loop primes it.
        self._netio_prev: tuple[int, int] | None = None
        self._netio_prev_t: float = 0.0
        # NFS byte counters (mountstats) ride the same tick as net_io but
        # keep their own prev pair: mounts can appear/vanish independently
        # of the interface counters, and a missing NFS sample must not
        # break the RX/TX diff (or vice versa).
        self._nfsio_prev: tuple[int, int] | None = None
        # Previous (self_cpu_seconds, children_cpu_seconds, monotonic) for
        # the resource sampler's CPU-percent deltas. None until primed —
        # the first resource_sample carries no CPU fields.
        self._res_prev: tuple[float, float, float] | None = None
        # Previous cumulative cgroup throttling counters
        # (nr_throttled, throttled_usec); the sample reports deltas, so the
        # first tick after start emits nothing for them.
        self._throttle_prev: tuple[int, int] | None = None
        # Previous per-mount NFS op counters {mount: (ops, trans, rtt_ms)}
        # for the per-interval RTT/retransmit derivation. Mounts appear and
        # vanish independently, so this is keyed per mount, not one pair.
        self._nfs_ops_prev: dict[str, tuple[int, int, int]] | None = None
        self._running = False
        self._ws_subscribers: set[WSSubscriber] = set()
        self._state_provider: Callable[[], dict] | None = None
        self._drakkar_config: DrakkarConfig | None = None
        # Deferred WS broadcasts: task_started events are held for ws_min_duration_ms
        # before being sent to WebSocket. If the task completes before the threshold,
        # neither start nor completion is sent (fast task, invisible to live UI).
        # Exception: failed tasks always go to WS regardless of duration.
        #
        # Values are ``(event, monotonic deadline)``. One periodic sweep expires
        # them all — see ``_sweep_deferred``. The earlier design created one
        # ``loop.call_later`` per task and cancelled it on completion, which a
        # fan-out handler turns into thousands of timer-heap operations per
        # second on the MAIN loop. CPython only purges cancelled TimerHandles
        # once they exceed half a heap of more than 100 entries, so at that rate
        # the heap churns continuously.
        self._deferred_ws: dict[str, tuple[dict, float]] = {}
        # The single outstanding sweep timer, or None when nothing is pending.
        # Scheduled on demand and re-armed only while entries remain, so an idle
        # worker holds no timer at all.
        self._deferred_sweep: asyncio.TimerHandle | None = None
        # Sweep period. Expiry is checked in batches rather than per task, so a
        # start event can be broadcast up to this late. That is acceptable for a
        # "this task is still running" signal, and the threshold itself caps the
        # period so a small ws_min_duration_ms is not swamped by it.
        self._deferred_sweep_interval = min(
            DEFERRED_SWEEP_INTERVAL_SECONDS,
            max(config.ws_min_duration_ms / 1000.0, 0.001),
        )
        # In-memory counters (used for worker_state regardless of store_events)
        self._counters = {
            'consumed': 0,
            'completed': 0,
            'failed': 0,
            'produced': 0,
            'committed': 0,
        }
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
        # Outside the db_dir branch on purpose: the net_io heartbeat is
        # WS-only and resource samples still feed WS subscribers, so both
        # should run in memory-only mode too.
        self._netio_task = asyncio.create_task(self._host_sample_loop())
        self._watch_background_task(self._netio_task, 'host_sample')
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

    def subscribe(self, event_types: Collection[str] | None = None) -> WSSubscriber:
        """Subscribe to the live event stream.

        ``event_types`` is an optional allowlist of ``event`` names. A
        subscriber that names the events it renders never pays for the
        rest: the filter is applied at fan-out, before encoding, so an
        unwanted event costs one set-membership test instead of a JSON
        encode, a queue slot and a WebSocket frame. ``None`` means "every
        event" — the back-compatible default for clients that predate the
        parameter.

        The distinction matters under fan-out-heavy workloads. A handler
        that turns one message into a thousand tasks emits a thousand
        ``task_complete`` events per message; a page that renders only the
        executor timeline should not receive any of them.
        """
        sub = WSSubscriber(event_types=frozenset(event_types) if event_types is not None else None)
        self._ws_subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: WSSubscriber) -> None:
        """Unsubscribe from live event stream."""
        self._ws_subscribers.discard(sub)

    def _fanout_ws(self, event: dict) -> None:
        """Push ``event`` to every interested subscriber, dropping when full.

        Iterates a **snapshot** of the subscriber set, not the set itself.
        ``subscribe``/``unsubscribe`` run on the UI-server thread (inside the
        ``/ws`` route) while this runs on the main loop, so iterating the live
        set raises ``RuntimeError: Set changed size during iteration`` the
        moment a browser tab opens or closes mid-fan-out. That exception would
        surface on the message-processing path — ``_record`` is called from
        ``PartitionProcessor.enqueue``, which has no ``except`` — and kill the
        worker. ``list(set)`` completes in C without executing bytecode, so no
        thread switch can occur inside it. The Go backend guards the same
        fan-out with ``subsMu``; this is the CPython-idiomatic equivalent.

        Every interested subscriber receives the **same** :class:`LiveEvent`
        wrapper, so the event is JSON-encoded once regardless of how many
        browsers are connected, and the encode itself happens later on the
        UI thread rather than here on the pipeline loop.

        Subscribers that filtered the event out are skipped before the
        wrapper is even allocated, so a page that renders only the executor
        timeline costs nothing for the thousand ``task_complete`` events a
        fan-out-heavy message produces.

        A subscriber whose queue is full has events dropped, as before, but
        the loss is now **counted**. The ``/ws`` route reports the count to
        the browser so it can resync instead of silently drifting.
        """
        name = event.get('event')
        wrapper: LiveEvent | None = None
        for sub in list(self._ws_subscribers):
            if sub.event_types is not None and name not in sub.event_types:
                continue
            if wrapper is None:
                wrapper = LiveEvent(event)
            try:
                sub.queue.put_nowait(wrapper)
            except queue.Full:
                sub.dropped += 1
                recorder_ws_dropped_events.inc()

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        """Append event to buffer and broadcast to WS subscribers.

        Observability: when the deque is already at capacity, the next
        append will silently evict the oldest entry. We detect that case
        BEFORE the append (``len == maxlen``) and increment the drop
        counter once per dropped event. After the append, the buffer
        gauge is refreshed to the new depth so operators can watch for
        contention without scraping internal state.
        """
        event['dt'] = format_dt(event['ts'])
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
            self._fanout_ws(event)

    def _broadcast_ws(self, event: dict) -> None:
        """Send event to WebSocket subscribers without buffering to DB."""
        if self._ws_subscribers:
            self._fanout_ws(event)

    def _schedule_deferred_sweep(self) -> None:
        """Arm the sweep timer unless one is already outstanding."""
        if self._deferred_sweep is not None:
            return
        loop = asyncio.get_running_loop()
        self._deferred_sweep = loop.call_later(self._deferred_sweep_interval, self._sweep_deferred)

    def _sweep_deferred(self) -> None:
        """Broadcast every deferred start event whose threshold has passed.

        Cost is proportional to the number of EXPIRED entries, not to the
        number pending: ``ws_min_duration_ms`` is fixed for the process, so
        every entry gets the same offset and insertion order is deadline
        order. The loop therefore stops at the first entry that is not due.
        (``record_task_completed`` removes entries from the middle, which
        leaves the relative order of the rest intact.)

        Entries are collected before deletion — mutating a dict while
        iterating it raises.
        """
        self._deferred_sweep = None
        now = time.monotonic()
        due: list[tuple[str, dict]] = []
        for task_id, (event, deadline) in self._deferred_ws.items():
            if deadline > now:
                break
            due.append((task_id, event))
        for task_id, event in due:
            del self._deferred_ws[task_id]
            self._broadcast_ws(event)
        if self._deferred_ws:
            self._schedule_deferred_sweep()

    def _update_live_link(self) -> None:
        """Create or update the {worker}-live.db symlink to the current DB."""
        if not self._store.db_dir or not self._db_path:
            return
        link = live_link_path(self._store.db_dir, self._worker_name)
        target = os.path.basename(self._db_path)
        try:
            tmp = link + '.tmp'
            os.symlink(target, tmp)
            os.replace(tmp, link)
        except OSError:
            pass

    def _remove_live_link(self) -> None:
        """Remove the live symlink on graceful shutdown."""
        if not self._store.db_dir:
            return
        link = live_link_path(self._store.db_dir, self._worker_name)
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
        """
        from drakkar.dbstats import warm_directory

        while self._running:
            try:
                _scanned, purged = await asyncio.to_thread(warm_directory, self._store.db_dir, self._dbstats)
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
        """Per-tick host sampling: the ``net_io`` WS heartbeat + one
        ``resource_sample`` event.

        Runs whenever the recorder runs — memory-only mode included (the
        events then only feed WS subscribers; nothing touches the DB from
        here). On platforms without ``/proc/net/dev`` (macOS) the network
        half announces itself unavailable once and stays silent; the
        resource half still samples what the platform offers (CPU, fds).
        """
        initial = read_net_io_bytes()
        if initial is None:
            await logger.ainfo(
                'recorder_netio_unavailable',
                category='recorder',
                detail='/proc/net/dev is not readable on this platform; network rates will not stream to the UI',
            )
        else:
            self._netio_prev = initial
            self._netio_prev_t = time.monotonic()
        while self._running:
            await asyncio.sleep(self._store.state_sync_interval_seconds)
            # Same per-iteration guard as the other loops: one bad sample
            # must not end the heartbeat for the rest of the run.
            try:
                if initial is not None:
                    self._netio_sample()
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

    def _netio_sample(self) -> None:
        """Diff the interface counters and broadcast one ``net_io`` frame.

        The frame is broadcast-only (never buffered to the DB), so the
        pinned events-table row shape is untouched. Rates are host-wide by
        nature: ``/proc/net/dev`` counts the whole network namespace — this
        process and its subprocesses, plus any neighbours sharing it.
        """
        counters = read_net_io_bytes()
        nfs_counters = read_nfs_io_bytes()
        now = time.monotonic()
        if counters is None:
            return
        prev, prev_t = self._netio_prev, self._netio_prev_t
        nfs_prev = self._nfsio_prev
        # Re-prime unconditionally so a skipped frame (below) still measures
        # the NEXT interval from here, not across the gap.
        self._netio_prev, self._netio_prev_t = counters, now
        self._nfsio_prev = nfs_counters
        if prev is None:
            return
        elapsed = now - prev_t
        rx_delta = counters[0] - prev[0]
        tx_delta = counters[1] - prev[1]
        if elapsed <= 0 or rx_delta < 0 or tx_delta < 0:
            # A negative delta means an interface counter reset (interface
            # bounce, netns change) — drop this interval rather than
            # broadcasting a nonsense rate.
            return
        ts = time.time()
        mib = 1024 * 1024
        metadata = {
            'rx_mib_s': round(rx_delta / elapsed / mib, 3),
            'tx_mib_s': round(tx_delta / elapsed / mib, 3),
            'rx_bytes_total': counters[0],
            'tx_bytes_total': counters[1],
            'interval_s': round(elapsed, 1),
        }
        # NFS rates (contract v1.11, optional keys): the interface counters
        # above cannot see kernel-NFS traffic from inside a container's
        # network namespace — the host's NFS client moves the bytes through
        # the HOST's interfaces. mountstats counters follow the mount
        # namespace instead, so they do see it. Fields appear only when an
        # NFS mount is visible on both ends of the interval and its
        # counters didn't go backwards (remount) — clients hide the readout
        # when the keys are absent.
        if nfs_counters is not None and nfs_prev is not None:
            nfs_read_delta = nfs_counters[0] - nfs_prev[0]
            nfs_write_delta = nfs_counters[1] - nfs_prev[1]
            if nfs_read_delta >= 0 and nfs_write_delta >= 0:
                metadata['nfs_read_mib_s'] = round(nfs_read_delta / elapsed / mib, 3)
                metadata['nfs_write_mib_s'] = round(nfs_write_delta / elapsed / mib, 3)
                metadata['nfs_read_bytes_total'] = nfs_counters[0]
                metadata['nfs_write_bytes_total'] = nfs_counters[1]
        self._broadcast_ws(
            {
                'ts': ts,
                'dt': format_dt(ts),
                'event': 'net_io',
                'metadata': encode_json_str(metadata),
            }
        )

    def broadcast_throughput(self, windows: dict) -> None:
        """Broadcast one ``throughput`` WS frame (contract v1.16).

        Broadcast-only like ``net_io`` — never buffered to the DB, so the
        pinned events-table row shape is untouched. ``windows`` is the
        five-window aggregate from the throughput tracker.
        """
        ts = time.time()
        self._broadcast_ws(
            {
                'ts': ts,
                'dt': format_dt(ts),
                'event': 'throughput',
                'metadata': encode_json_str({'windows': windows}),
            }
        )

    def _resource_sample(self) -> None:
        """Record one ``resource_sample`` event: what this worker consumed.

        The row rides the ordinary events table (metadata JSON, no schema
        change), so rotation, archiving, and the WS stream all carry it —
        an archive alone can answer "what were RSS / CPU / fds / network
        doing at 04:12" long after the process is gone.

        Field notes:

        - ``cpu_children_pct`` comes from ``RUSAGE_CHILDREN``, which only
          counts *reaped* subprocesses — a task's CPU lands in the sample
          for the interval its process exited in, so the series is bursty
          by nature and best read as an average over a few ticks.
        - ``rss_bytes`` / ``threads`` need ``/proc`` and are absent on
          macOS; every field is optional and simply omitted when its
          source is unavailable.
        - The first sample after start carries no ``cpu_*_pct`` — percents
          are deltas and there is nothing to diff against yet.
        """
        now = time.monotonic()
        self_ru = resource.getrusage(resource.RUSAGE_SELF)
        child_ru = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_self = self_ru.ru_utime + self_ru.ru_stime
        cpu_children = child_ru.ru_utime + child_ru.ru_stime
        prev = self._res_prev
        self._res_prev = (cpu_self, cpu_children, now)

        meta: dict[str, Any] = {}
        rss_bytes, threads = read_self_stats()
        if rss_bytes is not None:
            meta['rss_bytes'] = rss_bytes
        if threads is not None:
            meta['threads'] = threads
        open_fds = read_open_fd_count()
        if open_fds is not None:
            meta['open_fds'] = open_fds
        counters = read_net_io_bytes()
        if counters is not None:
            meta['rx_bytes_total'], meta['tx_bytes_total'] = counters
        if prev is not None:
            elapsed = now - prev[2]
            if elapsed > 0:
                meta['cpu_self_pct'] = round(100 * (cpu_self - prev[0]) / elapsed, 1)
                meta['cpu_children_pct'] = round(100 * (cpu_children - prev[1]) / elapsed, 1)
                meta['interval_s'] = round(elapsed, 1)

        # Host-pressure keys (contract v1.15): which resource is the host
        # fighting for. All best-effort and optional, like everything above.
        loadavg = read_loadavg()
        if loadavg is not None:
            meta['load1'], meta['load5'] = round(loadavg[0], 2), round(loadavg[1], 2)
        pressure = read_pressure()
        if pressure is not None:
            for key, value in pressure.items():
                meta[f'psi_{key}'] = value
        throttle = read_cpu_throttle()
        throttle_prev = self._throttle_prev
        self._throttle_prev = throttle
        if throttle is not None and throttle_prev is not None:
            periods_delta = throttle[0] - throttle_prev[0]
            usec_delta = throttle[1] - throttle_prev[1]
            # Negative delta = cgroup replaced (container restart in place);
            # skip the interval, the re-prime above already happened.
            if periods_delta >= 0 and usec_delta >= 0:
                meta['cpu_throttled_periods'] = periods_delta
                meta['cpu_throttled_ms'] = round(usec_delta / 1000, 1)
        meta.update(self._nfs_mount_deltas())

        self._record(
            {
                'ts': time.time(),
                'event': 'resource_sample',
                'metadata': encode_json_str(meta),
            }
        )

    def _nfs_mount_deltas(self) -> dict[str, Any]:
        """Per-interval NFS health per mount: ``{'nfs_mounts': [...]}`` or ``{}``.

        Diffs the cumulative mountstats op counters against the previous
        tick and derives, per mount: operations completed, average server
        round-trip per operation (the shared-storage-contention signal —
        RTT multiplies under server load while byte throughput can look
        normal), and retransmissions (``trans - ops`` — the "server not
        answering" signal). Quiet mounts (no ops, no retrans) are skipped;
        a counter that went backwards (remount) drops the mount for this
        interval and re-primes.
        """
        current = read_nfs_mount_stats()
        prev = self._nfs_ops_prev
        self._nfs_ops_prev = current
        if current is None or prev is None:
            return {}

        mounts: list[dict[str, Any]] = []
        for mount, (ops, trans, rtt_ms) in sorted(current.items()):
            prev_counters = prev.get(mount)
            if prev_counters is None:
                continue  # mount appeared mid-interval — first full interval counts
            ops_delta = ops - prev_counters[0]
            trans_delta = trans - prev_counters[1]
            rtt_delta = rtt_ms - prev_counters[2]
            if ops_delta < 0 or trans_delta < 0 or rtt_delta < 0:
                continue  # counter reset (remount)
            retrans = trans_delta - ops_delta
            if ops_delta == 0 and retrans <= 0:
                continue  # quiet mount — no signal, no row
            mounts.append(
                {
                    'mount': mount,
                    'ops': ops_delta,
                    'rtt_ms': round(rtt_delta / ops_delta, 1) if ops_delta else 0.0,
                    'retrans': max(0, retrans),
                }
            )
        if not mounts:
            return {}
        return {'nfs_mounts': mounts}

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
        if self._netio_task:
            self._netio_task.cancel()
            try:
                await self._netio_task
            except asyncio.CancelledError:
                pass
        if self._dbstats_warm_task:
            self._dbstats_warm_task.cancel()
            try:
                await self._dbstats_warm_task
            except asyncio.CancelledError:
                pass
        # Drop any pending deferred start events and disarm the shared sweep.
        if self._deferred_sweep is not None:
            self._deferred_sweep.cancel()
            self._deferred_sweep = None
        self._deferred_ws.clear()

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
        window_id: int | None = None,
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
                'metadata': encode_json_str(
                    {
                        'offsets': [m.offset for m in messages],
                        'task_ids': [t.task_id for t in tasks],
                        'task_count': len(tasks),
                        'message_count': len(messages),
                        'message_labels': message_labels or [],
                        # Lets the UI group an ``arranged`` row with the
                        # window-scoped annotations emitted from the same
                        # ``arrange()`` call. Per-partition monotonic and
                        # reset on restart — never a global identifier.
                        'window_id': window_id,
                    }
                ),
            }
        )

    def record_annotation(
        self,
        *,
        kind: str,
        partition: int,
        metadata_json: str,
        offset: int | None = None,
        task_id: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Append one handler-emitted annotation to the event log.

        The envelope is already encoded by :class:`~drakkar.annotations.Annotator`,
        which owns the size budgets — by the time a payload reaches here it has
        been measured and accepted, so this method does no validation of its own.

        Anchoring follows the annotation's scope: ``offset`` set for a message,
        ``task_id`` set for a task, neither set for a window (those rows are
        matched through the ``offsets`` list inside ``metadata_json``).
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'annotation',
                'partition': partition,
                'offset': offset,
                'task_id': task_id,
                'metadata': metadata_json,
                'labels': encode_json_str(labels) if labels else None,
                'kind': kind,
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
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
        queue_wait_ms: float | None = None,
    ) -> None:
        stdin_lines = 0
        stdin_size = 0
        if task.stdin:
            stdin_size = len(task.stdin.encode())
            stdin_lines = _line_count(task.stdin)
        metadata: dict = {
            'source_offsets': task.source_offsets,
            'slot': slot,
        }
        if queue_wait_ms is not None:
            # How long the task waited for a pool slot before any work began —
            # the live UI shows it in the timeline hover so wall time splits
            # into waiting vs running.
            metadata['queue_wait_ms'] = queue_wait_ms
        if task.stdin and self._store.store_stdin:
            # Opt-in stdin capture (ui.recorder.store_stdin): the exact input
            # the task consumed, capped at stdin_max_bytes. Rides the existing
            # metadata JSON, so no events-table schema change is involved.
            stored_stdin, stdin_truncated = _capped_stdin(task.stdin, self._store.stdin_max_bytes)
            metadata['stdin'] = stored_stdin
            if stdin_truncated:
                metadata['stdin_truncated'] = True
        elif stdin_size:
            # Content not stored — record the size in metadata anyway. The
            # entry-level stdin_size below is WS-only (dropped at DB insert),
            # and the task detail page needs to say "N bytes consumed, content
            # not stored" instead of showing nothing.
            metadata['stdin_bytes'] = stdin_size
        if task.env:
            # Sanitize per-task env values before storing in the recorder DB.
            # The raw task.env stays untouched on the task object (subprocess
            # launch still needs real values); only the RECORDED copy is
            # redacted so the debug UI never exposes secrets.
            metadata['env'] = {k: sanitize_env_value(k, v) for k, v in task.env.items()}
        if precomputed:
            # Neutral marker: a result was supplied by the handler and no
            # subprocess ran. The framework does not classify the reason
            # (cache, lookup, deterministic shortcut, ...).
            metadata['precomputed'] = True
        # Origin / client_name / request_id default to whatever the
        # ``ExecutorTask`` carries — auto-tagged at ``arrange_*`` time on
        # both the Kafka path (``origin='kafka'`` default) and the
        # webapp path (``WebappRunner`` stamps each task before
        # submission). Explicit kwargs override only when callers need to
        # record an event for a task that doesn't carry those fields
        # (e.g., a synthetic test row).
        resolved_origin = origin if origin is not None else getattr(task, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task, 'request_id', None)
        entry = {
            'ts': time.time(),
            'event': 'task_started',
            'partition': partition,
            'task_id': task.task_id,
            'args': encode_json_str(task.args),
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'slot': slot,
            'stdin_lines': stdin_lines,
            'stdin_size': stdin_size,
            'metadata': encode_json_str(metadata),
            'labels': encode_json_str(task.labels) if task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        ws_threshold_ms = self._config.ws_min_duration_ms
        if ws_threshold_ms > 0:
            # Defer WS broadcast: only send task_started to live UI if
            # the task is still running after ws_min_duration_ms.
            self._record(entry, skip_ws=True)
            self._deferred_ws[task.task_id] = (entry, time.monotonic() + ws_threshold_ms / 1000.0)
            self._schedule_deferred_sweep()
        else:
            self._record(entry)

    def record_task_completed(
        self,
        result: ExecutorResult,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        precomputed: bool = False,
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
        cost: float | None = None,
        speed: float | None = None,
    ) -> None:
        self._counters['completed'] += 1
        duration_ms = result.duration_seconds * 1000

        # If the task_started was deferred and the task finished before
        # the threshold, neither start nor completion goes to WS.
        # If the threshold already fired (start was sent), send completion too.
        # Removing the entry is enough to suppress the pending start event —
        # the shared sweep only broadcasts what is still in the map, so there
        # is no per-task timer to cancel.
        skip_ws = self._deferred_ws.pop(result.task.task_id, None) is not None

        skip_db = self._store.event_min_duration_ms > 0 and duration_ms < self._store.event_min_duration_ms
        include_output = duration_ms >= self._store.output_min_duration_ms

        # See ``record_task_started`` for the resolution rules: explicit
        # kwargs take precedence, otherwise we read from the task that
        # produced the result (auto-tagged by the upstream pipeline).
        task_obj = result.task
        resolved_origin = origin if origin is not None else getattr(task_obj, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task_obj, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task_obj, 'request_id', None)
        entry: dict = {
            'ts': time.time(),
            'event': 'task_completed',
            'partition': partition,
            'task_id': result.task.task_id,
            'exit_code': result.exit_code,
            'duration': result.duration_seconds,
            'stdout_size': len(result.stdout.encode()),
            # WS-frame-only field, like task_started's stdin_lines/stdin_size:
            # not in the pinned events-table column list, so the DB insert
            # drops it and the row shape is unchanged.
            'stdout_lines': _line_count(result.stdout),
            'pid': result.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'labels': encode_json_str(result.task.labels) if result.task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        completed_meta: dict = {}
        if precomputed:
            # Mirrored on the completion event so downstream queries /
            # dashboards can filter precomputed outcomes without joining
            # to task_started.
            completed_meta['precomputed'] = True
        if result.spawn_seconds is not None:
            # Parent-side share of the duration (see ExecutorResult.spawn_seconds);
            # the live UI shows it in the timeline hover detail.
            completed_meta['spawn_ms'] = round(result.spawn_seconds * 1000, 1)
        if cost is not None and speed is not None:
            # Contract v1.16: present only for throughput-counted tasks —
            # the caller applied the counting rules; excluded tasks carry
            # no keys at all, never zeros.
            completed_meta['cost'] = cost
            completed_meta['speed'] = round(speed, 3)
        if completed_meta:
            entry['metadata'] = encode_json_str(completed_meta)
        if include_output:
            entry['args'] = encode_json_str(result.task.args)
        if include_output and self._store.store_output:
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
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self._counters['failed'] += 1

        # Failed tasks ALWAYS go to WS regardless of ws_min_duration_ms.
        # If the task_started was deferred, send it now before the failure event
        # so the live UI sees the full start→fail sequence.
        deferred = self._deferred_ws.pop(task.task_id, None)
        if deferred:
            start_event, _ = deferred
            self._broadcast_ws(start_event)

        if duration_seconds is not None:
            duration_ms = duration_seconds * 1000
            skip_db = self._store.event_min_duration_ms > 0 and duration_ms < self._store.event_min_duration_ms
            include_output = duration_ms >= self._store.output_min_duration_ms
            should_log = duration_ms >= self._config.log_min_duration_ms
        else:
            skip_db = False
            include_output = True
            should_log = True

        # See ``record_task_started`` for the resolution rules.
        resolved_origin = origin if origin is not None else getattr(task, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task, 'request_id', None)
        entry: dict = {
            'ts': time.time(),
            'event': 'task_failed',
            'partition': partition,
            'task_id': task.task_id,
            'exit_code': error.exit_code,
            'pid': error.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'metadata': encode_json_str(_failed_metadata(task, error, self._store.stdin_max_bytes)),
            'labels': encode_json_str(task.labels) if task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        if duration_seconds is not None:
            entry['duration'] = duration_seconds
        if include_output:
            entry['args'] = encode_json_str(task.args)
        if include_output and self._store.store_output:
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
                'metadata': encode_json_str(
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
                'metadata': encode_json_str(
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
                'metadata': encode_json_str(
                    {
                        'window_id': window_id,
                        'task_count': task_count,
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_runtime_health(
        self,
        kind: str,
        state: str,
        lag_ms: float,
        unit_count: int,
    ) -> None:
        """Record one runtime-health transition or periodic sample.

        ``kind`` is ``'transition'`` (state changed) or ``'sample'`` (the
        low-frequency history point). Wire fields are backend-neutral —
        ``unit_count`` counts asyncio tasks here, goroutines on the Go
        backend.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'runtime_health',
                'metadata': encode_json_str(
                    {
                        'kind': kind,
                        'state': state,
                        'lag_ms': lag_ms,
                        'unit_count': unit_count,
                    }
                ),
            }
        )

    def record_runtime_stall(
        self,
        duration_ms: float,
        stacks: list[dict],
        dropped_stacks: int,
        unit_count: int,
    ) -> None:
        """Record one runtime stall with the stacks captured while it lasted.

        ``stacks`` entries are ``{stack, location, count}`` — distinct
        blocking sites the sampler thread saw, with how often each was
        sampled. ``dropped_stacks`` counts distinct sites past the
        ``runtime_health.max_stall_stacks`` cap.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'runtime_stall',
                'duration': round(duration_ms / 1000, 4),
                'metadata': encode_json_str(
                    {
                        'duration_ms': duration_ms,
                        'stacks': stacks,
                        'dropped_stacks': dropped_stacks,
                        'unit_count': unit_count,
                    }
                ),
            }
        )

    def record_runtime_lag_episode(
        self,
        *,
        duration_ms: float,
        peak_lag_ms: float,
        lag_sum_ms: float,
        verdict: str,
        stall_count: int,
        sample_count: int,
        stacks: list[dict],
        dropped_stacks: int,
        unit_count: int,
        cpu_ms: float | None = None,
        cpu_ratio: float | None = None,
        evidence: dict | None = None,
    ) -> None:
        """One closed lag episode (contract v1.15).

        An episode spans the whole time the runtime was degraded or
        stalled; where ``runtime_stall`` answers "what blocked the loop
        just now", this row answers "what was the loop doing across the
        entire bad window, and was it blocked, busy, or starved of CPU".
        ``evidence`` carries optional corroborating host-pressure readings
        (``cpu_throttled_ms``, ``psi_cpu_some_avg10``, ``load1``), merged
        flat into the metadata.
        """
        meta: dict[str, Any] = {
            'duration_ms': duration_ms,
            'peak_lag_ms': peak_lag_ms,
            'lag_sum_ms': lag_sum_ms,
            'verdict': verdict,
            'stall_count': stall_count,
            'sample_count': sample_count,
            'stacks': stacks,
            'dropped_stacks': dropped_stacks,
            'unit_count': unit_count,
        }
        if cpu_ms is not None:
            meta['cpu_ms'] = cpu_ms
        if cpu_ratio is not None:
            meta['cpu_ratio'] = cpu_ratio
        if evidence:
            meta.update(evidence)
        self._record(
            {
                'ts': time.time(),
                'event': 'runtime_lag_episode',
                'duration': round(duration_ms / 1000, 4),
                'metadata': encode_json_str(meta),
            }
        )

    def record_runtime_probe(self, *, lag_ms: float, unit_count: int, stacks: list[dict]) -> None:
        """One opt-in runtime stack probe (contract v1.15).

        Enabled by ``runtime_health.probe_interval_seconds > 0``: a
        flight-recorder profiler sample of what the runtime thread was
        executing, taken regardless of health state.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'runtime_probe',
                'metadata': encode_json_str(
                    {
                        'lag_ms': lag_ms,
                        'unit_count': unit_count,
                        'stacks': stacks,
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
                'metadata': encode_json_str(
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
                'metadata': encode_json_str(
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

    def record_partition_stalled(self, partition: int) -> None:
        """Record that a partition was paused because an offset stalled
        (delivery + DLQ unconfirmed under ``dlq.on_send_failure=stall``)."""
        self._record(
            {
                'ts': time.time(),
                'event': 'partition_stalled',
                'partition': partition,
            }
        )

    def record_offload(
        self,
        *,
        hook: str,
        partition: int | None,
        function: str,
        duration: float,
        queued: float,
        status: str,
        error: str = '',
        window_id: int | None = None,
        offsets: tuple[int, ...] = (),
        offset: int | None = None,
        task_id: str | None = None,
    ) -> None:
        """Record one ``BaseDrakkarHandler.offload()`` call.

        Anchoring mirrors annotations: ``offset`` for a message-anchored
        hook, ``task_id`` for a task-anchored one, neither for window-wide
        hooks (``arrange`` / ``on_window_complete``) whose rows are matched
        through the ``offsets`` list inside ``metadata``. ``partition`` is
        ``None`` when offload() ran outside a framework-invoked hook
        (``on_ready``, a ``@periodic`` method) — the row then lives outside
        any message trace, like ``periodic_run`` events do.

        ``queued`` is the seconds the call waited for a free offload-pool
        thread; sustained non-zero values are the signal that
        ``offload.max_threads`` is undersized for the deployment.
        """
        metadata: dict[str, Any] = {
            'hook': hook,
            'function': function,
            'queued': round(queued, 4),
            'status': status,
            'window_id': window_id,
            # Window-scoped rows only — same trace-matching contract as
            # window-scoped annotations (see record_annotation).
            'offsets': list(offsets) if offset is None and task_id is None else [],
        }
        if error:
            metadata['error'] = error[:500]
        self._record(
            {
                'ts': time.time(),
                'event': 'offload',
                'partition': partition,
                'offset': offset,
                'task_id': task_id,
                'duration': round(duration, 4),
                'metadata': encode_json_str(metadata),
                # Not a DB column; carried for live WS subscribers so the
                # UI can label the row without parsing metadata.
                'hook': hook,
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
                'metadata': encode_json_str(metadata),
            }
        )

    # --- Webapp request lifecycle events ---
    #
    # These helpers record HTTP-origin request lifecycle events to the
    # ``events`` table alongside the per-task rows already produced by
    # ``record_task_*``. They populate the new ``origin`` / ``client_name``
    # / ``request_id`` columns so the debug UI can filter, group, and
    # trace HTTP-originated work without re-deriving the relationship
    # from labels. Every helper is sync and safe to call from either T1
    # (the runner) or T2 (the dependency layer) — appending to the
    # in-memory buffer doesn't touch the DB connection (the flush loop
    # owns that), so the recorder is loop-agnostic at the recording site.

    def record_webapp_request_received(self, ctx: WebRequestContext) -> None:
        """Record entry into the runner for one HTTP request.

        Fires once per request that passed the auth + rate-limit + body
        validation gates and is about to dispatch to T1. Captures the
        client identity, request id, and start timestamp so operators
        can pinpoint the request in the recorder/debug UI before any
        task fan-out.
        """
        body_bytes = self._compute_body_bytes(ctx)
        metadata: dict[str, Any] = {
            'started_at': format_rfc3339_micro(ctx.started_at) if ctx.started_at is not None else None,
        }
        if body_bytes is not None:
            metadata['body_bytes'] = body_bytes
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_received',
                'partition': -1,  # synthetic partition for HTTP origin
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'metadata': encode_json_str(metadata),
            }
        )

    def record_webapp_request_completed(
        self,
        ctx: WebRequestContext,
        status: str,
        duration_ms: float,
    ) -> None:
        """Record successful completion of one HTTP request (status='ok').

        ``status`` is the same label used on ``drakkar_webapp_requests_total``
        — typically ``'ok'`` here, but kept as a parameter so the same
        helper can be reused if the route handler ever needs to record
        a non-ok terminal state without a dedicated helper.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_completed',
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'duration': duration_ms / 1000.0,
                'metadata': encode_json_str(
                    {
                        'status': status,
                        'duration_ms': duration_ms,
                    }
                ),
            }
        )

    def record_webapp_request_timeout(
        self,
        ctx: WebRequestContext,
        duration_ms: float,
    ) -> None:
        """Record a 504 timeout outcome for one HTTP request.

        Fires from T2 (the route handler) when ``asyncio.wait_for`` trips
        its budget. The matching ``record_webapp_request_dropped_after_timeout``
        is fired from T1 if the runner reaches its post-execute gate
        AFTER the timeout — together they let operators distinguish
        "timed out before any work" from "timed out after the executor
        already ran".
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_timeout',
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'duration': duration_ms / 1000.0,
                'metadata': encode_json_str({'duration_ms': duration_ms}),
            }
        )

    def record_webapp_request_rate_limited(
        self,
        client: str,
        rpm_limit: int,
        requests_in_window: int,
    ) -> None:
        """Record a 429 rate-limit outcome for one HTTP request.

        ``client`` is the matched client name (the rate-limit dep only
        runs after auth has resolved a real client, so a configured
        client name is always available). No ``request_id`` because the
        request is rejected before the runner allocates one.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_rate_limited',
                'partition': -1,
                'origin': 'http',
                'client_name': client,
                'metadata': encode_json_str(
                    {
                        'rpm_limit': rpm_limit,
                        'requests_in_window': requests_in_window,
                    }
                ),
            }
        )

    def record_webapp_request_auth_failed(self, token_prefix: str) -> None:
        """Record a 401 auth-failure for one HTTP request.

        ``token_prefix`` is the redacted (first-4-chars + ``...``) token
        produced by :func:`drakkar.webapp.utils.redact_token` — never
        the full token. ``client_name`` is left ``NULL`` because no
        client was matched; the debug UI can filter by ``event =
        'webapp_request_auth_failed'`` instead.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_auth_failed',
                'partition': -1,
                'origin': 'http',
                # No matched client; leave client_name NULL.
                'metadata': encode_json_str({'token_prefix': token_prefix}),
            }
        )

    def record_webapp_request_dropped_after_timeout(
        self,
        ctx: WebRequestContext,
    ) -> None:
        """Record a request dropped on T1 after T2 had already 504'd.

        Fires from the runner's post-execute / pre-on_http_request_complete
        cancellation gates. The user already received a 504 client-side;
        this row marks the wasted work that the framework managed to
        skip thanks to the cooperative cancellation flag.
        """
        self._record(
            {
                'ts': time.time(),
                'event': 'webapp_request_dropped_after_timeout',
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
            }
        )

    @staticmethod
    def _compute_body_bytes(ctx: WebRequestContext) -> int | None:
        """Return the body size in bytes for the recorder metadata, or ``None``.

        Pydantic-modelled requests have a ``model_dump_json`` method we
        use to recover the on-the-wire body size; raw bytes are
        ``len()``ed directly. For the rare test fixture that hands in a
        plain object (no model, no bytes) we fall back to ``None`` rather
        than coerce — better to omit the field than misreport.
        """
        request = ctx.request
        if isinstance(request, bytes | bytearray):
            return len(request)
        # Avoid importing Pydantic at module load — the recorder runs in
        # processes that don't always carry the webapp dependencies.
        # Local import keeps the cost paid only when we actually inspect
        # a webapp request (a small fraction of recorder writes).
        try:
            from pydantic import BaseModel
        except ImportError:  # pragma: no cover — pydantic is a hard dep
            return None
        if isinstance(request, BaseModel):
            try:
                return len(request.model_dump_json().encode('utf-8'))
            except Exception:
                return None
        return None

    # --- Query methods (for debug UI, reads current DB) ---

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
        reader = self._reader_db or self._db
        if not reader or not self._store.store_events:
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
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def trace_by_label(self, label_key: str, label_value: str) -> list[dict]:
        """Find all events for tasks matching a label key-value pair."""
        await self._flush()
        reader = self._reader_db or self._db
        if not reader or not self._store.store_events:
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

        # Fallback: other workers' live DBs. Same shape as ``cross_trace`` —
        # the blocking directory walk runs in a thread (inline it would stall
        # the main loop for the whole sweep), and the sweep is bounded so a
        # directory full of peers cannot pin the caller.
        if not self._store.db_dir:
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
        await self._flush()
        reader = self._reader_db or self._db
        if not reader or not self._store.store_events:
            return []
        async with reader.execute(
            _TRACE_QUERY, [partition, msg_offset, partition, msg_offset, partition, msg_offset]
        ) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

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
                                if self._cluster_name and cfg_row[1] != self._cluster_name:
                                    return []

                # Check events table exists
                async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
                    if not await cur.fetchone():
                        return []

                async with db.execute(query, params) as cur:
                    columns = [d[0] for d in cur.description]
                    rows = await cur.fetchall()
                    events = [dict(zip(columns, row, strict=False)) for row in rows]

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
            ev['worker_name'] = self._worker_name
        if local_events:
            return local_events

        if not self._store.db_dir:
            return []

        searched_paths: set[str] = set()
        if self._db_path:
            searched_paths.add(os.path.realpath(self._db_path))

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
        live_pattern = os.path.join(self._store.db_dir, '*-live.db')
        for link_path in sorted(glob.glob(live_pattern)):
            if not os.path.islink(link_path):
                continue
            link_name = os.path.basename(link_path)
            if link_name.removesuffix('-live.db') == self._worker_name:
                continue
            targets.append(os.path.realpath(link_path))
        return targets

    def _enumerate_rotated_dbs(self, searched_paths: set[str]) -> list[str]:
        """List rotated DB files newest first. Blocking; call in a thread."""
        all_dbs = []
        for entry in os.listdir(self._store.db_dir):
            if not entry.endswith('.db'):
                continue
            full = os.path.join(self._store.db_dir, entry)
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
        await self._flush()  # ensure recent events are queryable
        reader = self._reader_db or self._db
        if not reader or not self._store.store_events:
            return []
        query = 'SELECT * FROM events WHERE task_id = ? ORDER BY id ASC'
        async with reader.execute(query, [task_id]) as cursor:
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    async def get_partition_summary(self) -> list[dict]:
        """Get summary stats per partition from recorded events."""
        reader = self._reader_db or self._db
        if not reader or not self._store.store_events:
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
        if not reader or not self._store.store_events:
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
        if not self._store.db_dir or not self._store.store_config:
            return []
        threshold = self._config.workers_offline_after_seconds
        now = time.time()
        workers: list[dict] = []
        async for peer_name, target in discover_peer_dbs(
            self._store.db_dir,
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

    # --- Internal flush/retention ---

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._store.flush_interval_seconds)
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
            if not self._store.store_events:
                self._buffer.clear()
                # Reflect the drain in the gauge even when events aren't persisted —
                # buffer length is what the gauge reports, not DB row count.
                recorder_buffer_size.set(0)
                return
            # Take a LOCAL snapshot of the rows to flush BEFORE the DB
            # write. If the write fails (sqlite3.Error) or is interrupted
            # (cancellation) we re-queue this exact list at the FRONT of the
            # buffer so ordering is preserved and no events are lost. Using ``list(...)`` freezes
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
                # Webapp-release columns.
                # Default to ``origin='kafka'`` / NULL for the
                # other two when the recording site doesn't populate
                # them — matches the SQL DEFAULT and keeps the SQL
                # statement future-proof if the same column list is
                # used elsewhere (e.g., a future bulk-import path).
                'origin',
                'client_name',
                'request_id',
            ]
            placeholders = ', '.join(['?'] * len(columns))
            col_names = ', '.join(columns)
            query = f'INSERT INTO events ({col_names}) VALUES ({placeholders})'
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
            # Observe the full flush body (executemany + commit) — the histogram
            # surfaces disk-I/O latency tail so operators can alert on p99
            # regressions before the buffer backs up enough to drop events.
            flush_start = time.monotonic()
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
                    # Give up on this batch: drop it, reset the counter so
                    # the next batch starts from a clean slate, and tick
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
                    return
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
                return
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
            # Success path: reset the retry counter and record the histogram.
            self._flush_failures = 0
            recorder_flush_duration.observe(time.monotonic() - flush_start)
            # Post-drain: the gauge should report the new buffer depth. Usually
            # zero, but a concurrent _record() during the await could have
            # appended in between — reading len(self._buffer) is the correct value.
            recorder_buffer_size.set(len(self._buffer))

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
