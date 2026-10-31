"""Flight recorder — event log to timestamped SQLite files.

The schemas + standalone helpers (JSON encoding, env-secret sanitization,
DB-file path management, IP detection), the archive engine and the live
fan-out live in sibling modules:

- :mod:`drakkar.recorder.schema`  — DDL constants + canned trace queries.
- :mod:`drakkar.recorder.helpers` — orjson-or-stdlib codec, secret patterns,
  ``format_dt``, ``make_db_path``, ``open_reader``,
  ``detect_worker_ip``.
- :mod:`drakkar.recorder.sealing` — sealing a closed file (rollback
  journal, meta file) and moving it into the shared history folder.
- :mod:`drakkar.recorder.archive` — window math + the archive pass that
  folds sealed files into compressed archives.
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
import secrets

# ``socket`` is re-imported here so test patches like
# ``patch('drakkar.recorder.socket.socket', ...)`` still find the module on
# this attribute path. The actual ``detect_worker_ip`` consumer lives in
# :mod:`drakkar.recorder.helpers`, but ``socket`` is a shared module
# reference — patching ``socket.socket`` via either attribute path replaces
# the class globally for both modules.
import socket  # noqa: F401
import sqlite3
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import aiosqlite
import structlog

from drakkar.config import APIConfig, LoggingConfig, RecorderConfig, RecorderMemoryConfig, UIConfig
from drakkar.config.recorder import PERSISTENCE_POSTGRES, PERSISTENCE_SQLITE, Persistence
from drakkar.dbfiles import WAL_SYNCHRONOUS_PRAGMA, secure_db_file
from drakkar.hostinfo import HostSampler, detect_network_fs
from drakkar.metrics import (
    recorder_buffer_size,
    recorder_dropped_events,
    recorder_flush_batches_dropped,
    recorder_flush_duration,
    recorder_flush_retries,
    recorder_memory_allocated_bytes,
    recorder_memory_bytes,
    recorder_memory_evicted_rows,
    recorder_memory_oldest_age_seconds,
    recorder_memory_rows,
    recorder_memory_write_failures,
    recorder_postgres_dropped_rows,
    recorder_requeue_overflow,
    recorder_seal_failures,
    recorder_unsealed_files,
)
from drakkar.ownerlock import try_sibling_lock
from drakkar.providers.postgres import POSTGRES_PROVIDER, RECORDER_FEATURE
from drakkar.providers.resilience import RateLimitedLog, ResilientCaller, describe_error
from drakkar.recorder.archive import observe_archive_footprint, run_archive_pass
from drakkar.recorder.election import ThreadFence
from drakkar.recorder.helpers import (
    BUSY_TIMEOUT_MS,
    close_quietly,
    detect_worker_ip,
    encode_json_str,
    format_dt,
    make_db_path,
    open_connection,
    open_reader,
    sanitize_env_value,
)
from drakkar.recorder.history import history_dir
from drakkar.recorder.memstore import (
    EVICT_MIN_INTERVAL_SECONDS,
    MEMORY_CLOSE_WAIT_SECONDS,
    EvictionResult,
    MemoryCoverage,
    MemoryStore,
    delete_id_prefix,
    is_transient_memory_error,
    run_eviction_pass,
)
from drakkar.recorder.orphans import folder_is_idle, remove_orphan_files, sibling_folders
from drakkar.recorder.postgres import PostgresQueue, QueuedPair, RecorderPostgresSettings
from drakkar.recorder.postgres_writer import OpenPool, PostgresLoopThread, PostgresWriter
from drakkar.recorder.queries import EventQueries, QueryContext
from drakkar.recorder.schema import (
    EVENT_COLUMNS,
    INSERT_EVENT_SQL,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
    WEBAPP_REQUIRED_EVENT_COLUMNS,
    EventType,
    RecorderSchemaError,
)
from drakkar.recorder.sealing import (
    LEFTOVER_FLOOR_SECONDS,
    find_leftovers,
    own_db_files,
    publish_in_background,
    seal_closed_file,
    seal_connection,
)
from drakkar.recorder.writer import EventWriter
from drakkar.schedule import ARCHIVE_TASK_NAME
from drakkar.utils import redact_url

if TYPE_CHECKING:
    from drakkar.cluster_tasks import ClusterTask, SlotContext
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

# Rows per ``executemany`` in one stage. Two jobs: it bounds how long the
# loop is held building row tuples (one ``dict.get`` per column per event,
# so an unchunked 30k-event stage stalls the loop for tens of milliseconds),
# and it bounds what a write that keeps failing can lose — a dropped chunk
# instead of the whole buffer. 5k rows is ~4 ms of tuple building and, at 20
# columns, 100k bind parameters, comfortably inside SQLite's limits.
FLUSH_CHUNK_ROWS = 5_000

# The constructor defaults of the values a worker reads from other config
# sections, taken from those sections' own defaults.
DEFAULT_API_PORT: int = APIConfig.model_fields['port'].default
DEFAULT_WS_MIN_DURATION_MS = round(UIConfig.model_fields['ws_min_duration'].default * 1000)
DEFAULT_LOG_MIN_DURATION_MS = round(LoggingConfig.model_fields['min_task_duration'].default * 1000)
# The memory store bounds come from ``recorder.memory``; the worker passes the
# row cap of all its executor slots, so the default row cap is one slot's share.
DEFAULT_MEMORY_WINDOW_SECONDS: int = RecorderMemoryConfig.model_fields['window'].default
DEFAULT_MEMORY_MAX_ROWS: int = RecorderMemoryConfig.model_fields['rows_per_executor'].default
DEFAULT_MEMORY_MAX_BYTES: int = RecorderMemoryConfig.model_fields['max_size'].default
# Resource samples without a recorder file, whose ``sqlite.state_interval`` paces them otherwise.
HOST_SAMPLE_SECONDS = 10
SECONDS_PER_HOUR = 3600

# The event columns a row tuple takes from the event dict; the recorder
# puts the id in front.
_ROW_COLUMNS = EVENT_COLUMNS[1:]

# How often the memory and file loops look at their queues while waiting out
# their interval. Small enough that a burst does not sit until the next tick,
# large enough to be invisible next to the workloads the recorder observes.
FLUSH_POLL_SECONDS = 0.25

# Approximate buffered payload bytes that trigger an early flush, alongside
# the half-full event count. ``recorder.max_buffer_rows`` bounds the buffer by COUNT, which
# says nothing about its size: a task over ``recorder.capture.min_output_duration`` has its
# whole stdout and stderr stored, uncapped, so a workload with large outputs
# reaches gigabytes of resident buffer long before it reaches 25 000 events.
# Bounding both makes the memory a function of throughput rather than of the
# handler's output size. Not configurable on purpose — it is a safety floor,
# not a tuning knob, and flushing early is never wrong.
FLUSH_BYTES_HIGH_WATER = 64 * 1024 * 1024

# Event keys whose values can be arbitrarily large. Summed for the byte
# high-water mark above; everything else in a row is bounded and small.
_HEAVY_EVENT_FIELDS = ('stdout', 'stderr', 'metadata', 'args')

# The name of the thread whose event loop runs stage P with ``postgres``.
POSTGRES_THREAD_NAME = 'drakkar-recorder-postgres'
# The part of the reserve a stop gives the stage P thread to answer each step
# after its COPYs used the time left minus the reserve.
POSTGRES_STOP_ANSWER_SECONDS = 0.1
# The bound of the pool close and of the thread join when the stop has no deadline.
POSTGRES_STOP_STEP_SECONDS = 5.0

# The ``detail`` of ``recorder_archive_pass_abandoned``, for a pass with a lease and one
# over this worker's own files; the texts are part of the log contract.
ARCHIVE_PASS_ABANDONED_DETAIL = 'the pass is fenced at its next check and its lease expires on its own'
ARCHIVE_OWN_PASS_ABANDONED_DETAIL = 'the pass ends after the window in progress'


def _event_weight(event: dict) -> int:
    """Approximate payload size of one buffered event, in characters.

    Characters rather than bytes: ``len`` on a ``str`` is O(1) while
    ``encode`` copies the whole string, and this runs once per recorded event
    on the loop. The count is a lower bound for non-ASCII text, which only
    makes the flush trigger slightly later than the nominal threshold.
    """
    return sum(len(value) for field in _HEAVY_EVENT_FIELDS if isinstance(value := event.get(field), str))


def _row_tuple(row_id: int, event: dict) -> tuple:
    """The ``events`` row of one recorded event, with its id first, in ``EVENT_COLUMNS`` order.

    ``origin`` is NOT NULL in the schema with a DEFAULT of ``'kafka'``, but
    the Kafka-path record helpers leave it unset; ``None`` becomes
    ``'kafka'`` here so the column default never fights an explicit NULL.
    """
    return (
        row_id,
        *(
            'kafka' if (column == 'origin' and event.get(column) is None) else event.get(column)
            for column in _ROW_COLUMNS
        ),
    )


def _time_left(deadline: float | None) -> float | None:
    """Seconds until the monotonic ``deadline``, never below 0; ``None`` without one."""
    return None if deadline is None else max(deadline - time.monotonic(), 0.0)


def _is_running(future: Future[None] | None) -> bool:
    """True while ``future`` is queued or running in its executor."""
    return future is not None and not future.done()


def _log_job_failure(job: str, future: Future[None]) -> None:
    """Log a job of the archive thread that raised, when nothing awaits its result."""
    if future.cancelled() or (exc := future.exception()) is None:
        return
    logger.error(
        'recorder_background_job_failed',
        category='recorder',
        job=job,
        error=str(exc),
        error_type=type(exc).__name__,
        exc_info=exc,
    )


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
    rotation tick a new timestamped file is created and the previous one
    is sealed: switched to a rollback journal, closed, and moved with its
    meta file into ``<shared_dir>/history/<cluster>/``, where it never
    changes again. A clean stop seals the live file the same way. Only
    this recorder ever opens its live file.

    Which tables are created depends on config flags:
    - ``capture.events`` -> ``events`` table
    - ``capture.config`` -> ``worker_config`` table (enables autodiscovery)
    - ``sqlite.state_snapshots`` -> ``worker_state`` table (periodic snapshots)
    """

    def __init__(
        self,
        config: RecorderConfig,
        worker_name: str = 'worker',
        cluster_name: str = '',
        *,
        persistence: Persistence,
        memory_enabled: bool = True,
        api_port: int = DEFAULT_API_PORT,
        api_public_url: str = '',
        ws_min_duration_ms: int = DEFAULT_WS_MIN_DURATION_MS,
        log_min_duration_ms: int = DEFAULT_LOG_MIN_DURATION_MS,
        storage_dir: str = '',
        shared_dir: str = '',
        instance: str = '',
        memory_window_seconds: int = DEFAULT_MEMORY_WINDOW_SECONDS,
        memory_max_rows: int = DEFAULT_MEMORY_MAX_ROWS,
        memory_max_bytes: int = DEFAULT_MEMORY_MAX_BYTES,
        postgres: RecorderPostgresSettings | None = None,
        postgres_open_pool: OpenPool | None = None,
        sibling_dir: str = '',
        worker_online: Callable[[str], bool | None] | None = None,
    ) -> None:
        """Wire a recorder without opening anything.

        ``persistence`` is the effective mode: ``'sqlite'`` writes the
        recorder file in ``storage_dir``, ``'none'`` writes no file. The
        caller passes ``DrakkarConfig.resolved_persistence``; the recorder
        never derives it. ``memory_enabled`` says whether this recorder keeps
        the memory store that serves the UI; without it, rows still get their
        ids and go on to the file or the Postgres queue. ``api_port`` and
        ``api_public_url`` fill the ``worker_config`` row. ``ws_min_duration_ms``
        and ``log_min_duration_ms`` are the shortest task the live stream shows
        and the shortest task that gets a slow-task log line. ``storage_dir`` is
        the worker's own folder (``storage.dir``); ``'sqlite'`` needs it.
        ``shared_dir`` is the folder every worker of the cluster sees
        (``storage.shared_dir``); sealed files go to its ``history/<cluster>/``,
        and it falls back to ``storage_dir``. Both folders resolve the same way
        in either mode. ``instance`` names this process in the meta files
        (random when empty).

        ``memory_window_seconds``, ``memory_max_rows`` and
        ``memory_max_bytes`` bound the memory store: its time window, its row
        cap (the caller multiplies the per-executor cap by the executor count)
        and its data budget in bytes.

        ``postgres`` holds the table settings of the ``'postgres'`` mode,
        which needs them; the other modes ignore it. ``postgres_open_pool``
        replaces the function that opens the write pool of that mode
        (``None``: :func:`drakkar.providers.postgres.open_pool`, looked up
        when the pool opens).

        ``sibling_dir`` is the folder that holds the own folders of every
        worker of this cluster (the parent of ``storage_dir``); empty, the
        leftover pass looks at no orphan folder. ``worker_online`` answers
        whether the registry lists a worker name as online: ``True``,
        ``False`` (no card, or an offline one), or ``None`` (it cannot tell
        yet). ``None`` in place of the function means no registry runs, and
        orphan folders are then only logged.

        The archive pass runs as the cluster task
        :meth:`archive_cluster_task` returns, where the worker's election
        decides which worker of the cluster runs each slot, or through
        :meth:`run_own_archive_pass` over this worker's own files.
        """
        if persistence == PERSISTENCE_SQLITE and not storage_dir:
            raise ValueError('recorder: persistence sqlite needs a storage_dir')
        if persistence == PERSISTENCE_POSTGRES and postgres is None:
            raise ValueError('recorder: persistence postgres needs postgres settings')
        super().__init__(config, ws_min_duration_ms=ws_min_duration_ms, log_min_duration_ms=log_min_duration_ms)
        self._persistence: Persistence = persistence
        self._memory_enabled = memory_enabled
        self._api_port = api_port
        self._api_public_url = api_public_url
        self._has_file: bool = persistence == PERSISTENCE_SQLITE
        # Whether the rows stage M took go on somewhere: to the file queue
        # (``sqlite``) or to the P queue (``postgres``).
        self._has_downstream: bool = persistence in (PERSISTENCE_SQLITE, PERSISTENCE_POSTGRES)
        self._postgres = postgres if persistence == PERSISTENCE_POSTGRES else None
        self._worker_name = worker_name
        self._cluster_name = cluster_name
        self._storage_dir = storage_dir
        self._shared_dir = shared_dir or storage_dir
        self._sibling_dir = sibling_dir
        self._worker_online = worker_online
        # Orphan folders already logged as found, so each is logged once per process.
        self._reported_orphans: set[str] = set()
        self._instance = instance or secrets.token_hex(8)
        self._archive = config.archive
        self._memory_window_seconds = memory_window_seconds
        self._memory_max_rows = memory_max_rows
        self._memory_max_bytes = memory_max_bytes
        # The memory store; opened in start() when this recorder keeps one
        # and capture.events is on, closed last in stop().
        self._memory_store: MemoryStore | None = None
        # The intake: events recorded and not yet taken by stage M.
        # ``_record`` appends (from the main loop and the webapp
        # loop thread); only stage M pops, on the main loop.
        self._intake: deque[dict] = deque(maxlen=config.max_buffer_rows)
        # The file queue: ``(id, row tuple)`` pairs that stage M handed on
        # and stage F has not yet written to the file. The id
        # rides next to the row, never inside the event dict: the fan-out may
        # encode that dict on the API server thread at any time.
        self._file_queue: deque[tuple[int, tuple]] = deque(maxlen=config.max_buffer_rows)
        # The P queue: the same pairs on their way to the Postgres table,
        # bounded by count and by text size; only with ``postgres``.
        self._pg_queue: PostgresQueue | None = (
            PostgresQueue(max_rows=config.max_buffer_rows, max_bytes=self._postgres.queue_max_bytes)
            if self._postgres is not None
            else None
        )
        # Set on the main loop after each memory tick and when an append
        # leaves the P queue at least half full; the Postgres loop, also on
        # the main loop, waits on it and then posts one stage P run.
        self._pg_wakeup = asyncio.Event()
        self._postgres_open_pool = postgres_open_pool
        # Stage P: its writer and the thread whose loop runs it; made in
        # start() when there is a P queue and events to feed it.
        self._pg_thread: PostgresLoopThread | None = None
        self._pg_writer: PostgresWriter | None = None
        self._postgres_task: asyncio.Task | None = None
        # Pairs the P queue dropped and no log line has reported yet; stage M
        # reports them when it ends (the append itself may not await).
        self._pg_unlogged_drops = 0
        # One ``recorder_postgres_rows_dropped`` line per 60 s per reason.
        self._pg_drop_log = RateLimitedLog(time.monotonic)
        # Approximate payload bytes appended since the last stage M. Reset
        # at the stage rather than decremented per drained row: it exists to
        # decide "write now", where over-counting costs one early write and
        # under-counting falls back to the interval.
        self._buffered_bytes = 0
        # Whether an appended event will ever reach a table: with
        # ``capture.events`` off, or with no memory store and no durable
        # store, an append would only fill the deque and count drops forever.
        self._persists_events: bool = self._capture.events and (memory_enabled or self._has_downstream)
        # Row ids, one counter for the process lifetime, assigned when stage
        # M pops a chunk (one consumer under ``_memory_lock`` gives one
        # order; ``_record`` runs on two threads). ``_last_id`` is the last
        # id handed out and kept; ``_floor_id`` the lowest id the memory
        # store holds (``_last_id + 1`` when it is empty).
        self._next_id = 1
        self._last_id = 0
        self._floor_id = 1
        self._evicted = False
        self._evicted_max_ts = 0.0
        # Monotonic start of the last eviction pass; a stage M with a minimum
        # interval (a UI read) skips the pass until that much time passed.
        self._evict_clock: Callable[[], float] = time.monotonic
        self._last_evict_at: float | None = None
        # The memory store holds rows up to this id that must go (a gap
        # left by a failed or cancelled chunk); the next stage M deletes
        # them before it writes anything.
        self._pending_gap_id: int | None = None
        # The writer may hold an open transaction of a chunk whose write was
        # cancelled; the next stage M rolls it back first.
        self._memory_dirty = False
        # Stage M runs under this lock, stage F under ``_flush_lock``. No
        # code takes this lock while it holds ``_flush_lock``.
        self._memory_lock = asyncio.Lock()
        # The memory loop sets it after each tick so the file loop writes on
        # the same tick.
        self._file_wakeup = asyncio.Event()
        # One line per 60 s for each memory write and eviction log event.
        self._memory_log = RateLimitedLog(time.monotonic)
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
        self._memory_task: asyncio.Task | None = None
        self._file_task: asyncio.Task | None = None
        self._rotation_task: asyncio.Task | None = None
        # The sleep of the rotation loop; tests replace it so no test waits for a
        # real interval.
        self._sleep = asyncio.sleep
        self._state_task: asyncio.Task | None = None
        self._host_sample_task: asyncio.Task | None = None
        # Own files closed but not yet in the history folder. ``_backlog``
        # holds files whose seal or move failed (sealed again before the
        # next move); ``_sealed`` holds files sealed at rotation, with their
        # meta, whose move is still to come. Both in memory only: a process
        # that dies with either leaves the files as leftovers, and the next
        # start with the same worker name seals them.
        self._backlog: list[str] = []
        self._sealed: dict[str, dict[str, Any]] = {}
        # Paths a seal or a move works on right now. The rotation, the
        # background sealing job and the leftover task each take a path
        # here first, so no file is ever sealed or moved twice at once.
        self._in_hand: set[str] = set()
        # Moves into the history folder touch the shared mount, which can
        # hang, so they run as background jobs that never hold up the next
        # rotation. One of each runs at a time.
        self._sealing_task: asyncio.Task | None = None
        self._leftover_task: asyncio.Task | None = None
        # Set by stop(). Cancelling an archive slot only stops its await;
        # the pass itself runs on in its thread, so it checks this between
        # windows and ends early instead of holding the process exit.
        self._archive_stop = threading.Event()
        # The pass gets a thread of its own, not one of the default executor:
        # it waits on election calls and log writes that run there, and could
        # otherwise hold the last free thread they need. The footprint
        # observation of a slot this worker did not win runs there too.
        self._archive_executor: ThreadPoolExecutor | None = None
        # The pass the executor runs or last ran, for stop() to wait on.
        self._archive_pass_future: Future[None] | None = None
        self._archive_pass_abandoned_detail = ARCHIVE_PASS_ABANDONED_DETAIL
        # The footprint observation the executor runs or last ran.
        self._footprint_future: Future[None] | None = None
        # How long a leftover must be untouched before it is sealed; an
        # attribute so tests can shorten it.
        self._leftover_floor_seconds = LEFTOVER_FLOOR_SECONDS
        # Host resource sampling (RSS/CPU/fds/pressure/NFS) with the
        # previous-tick state its rate fields need.
        self._host_sampler = HostSampler()
        self._running = False
        self._state_provider: Callable[[], dict] | None = None
        self._drakkar_config: DrakkarConfig | None = None
        # Serializes stage F: the file loop, file reads, rotation and stop
        # can all run it on the main loop, and two at once would each take
        # part of the file queue and race on the duration histogram.
        self._flush_lock = asyncio.Lock()
        # The stage F run that flushes for reads share while it waits for
        # ``_flush_lock`` (see ``_queued_stage_file``): while the disk hangs,
        # each timed-out read would otherwise leave one more waiting run.
        self._waiting_stage_file: asyncio.Task[None] | None = None
        # Flushes still waiting for each shared stage F run: they receive its outcome.
        self._stage_file_receivers: dict[asyncio.Task[None], int] = {}
        # Consecutive failures of the file chunk at the front of the file
        # queue. Reset on a successful stage F or when a chunk is dropped
        # after ``max_flush_retries``; one counter is enough because a
        # failed chunk goes back to the FRONT of the file queue and the next
        # stage F takes it first.
        self._flush_failures: int = 0
        # Read side. Given callables rather than values for the connection
        # and the DB path: both are swapped on every rotation.
        self._queries = EventQueries(
            QueryContext(
                config=config,
                storage_dir=storage_dir if self._has_file else '',
                worker_name=worker_name,
                cluster_name=cluster_name,
                persistence=persistence,
                memory_store=lambda: self._memory_store,
                file_reader=lambda: self._reader_db,
                coverage=self.memory_coverage,
                db_path=lambda: self._db_path,
                flush=self.flush,
                flush_memory=self.flush_memory,
            )
        )

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def persistence(self) -> Persistence:
        """The effective persistence mode, ``'none'``, ``'sqlite'`` or ``'postgres'``."""
        return self._persistence

    @property
    def has_file(self) -> bool:
        """Whether this recorder writes a recorder file."""
        return self._has_file

    @property
    def postgres_settings(self) -> RecorderPostgresSettings | None:
        """The table settings of the ``postgres`` mode; ``None`` in the other modes."""
        return self._postgres

    @property
    def storage_dir(self) -> str:
        """The worker's own folder (``storage.dir``); ``''`` when none is set, which persistence ``none`` allows."""
        return self._storage_dir

    @property
    def worker_name(self) -> str:
        return self._worker_name

    @property
    def cluster_name(self) -> str:
        """The cluster this recorder's worker belongs to; ``''`` when none is set."""
        return self._cluster_name

    @property
    def shared_dir(self) -> str:
        """The folder every worker of the fleet sees; ``''`` when no folder is set."""
        return self._shared_dir

    @property
    def history_dir(self) -> str:
        """Where this recorder's sealed files go; ``''`` when no folder is set."""
        return history_dir(self._shared_dir, self._cluster_name) if self._shared_dir else ''

    @property
    def config(self) -> RecorderConfig:
        """Read-only access to the recorder config, for the API server."""
        return self._config

    @property
    def memory_store(self) -> MemoryStore | None:
        """The memory store; ``None`` before start, after stop, or when this recorder keeps none."""
        return self._memory_store

    @property
    def memory_reader_db(self) -> aiosqlite.Connection | None:
        """The read-only connection of the memory store; ``None`` when there is no open store.

        Run statements through :meth:`MemoryStore.read` (see
        :attr:`memory_store`), not on this connection directly: ``read``
        arms the read limit before each statement.
        """
        store = self._memory_store
        return store.reader if store is not None and store.is_open else None

    def open_memory_reader(self) -> AbstractContextManager[sqlite3.Connection | None]:
        """Open a short-lived connection to the memory store by name, for a worker thread.

        The context yields ``None`` when there is no memory store or it is
        already closed; no read ever creates a new, empty store. See
        :meth:`MemoryStore.open_by_name`.
        """
        if self._memory_store is None:
            return contextlib.nullcontext(None)
        return self._memory_store.open_by_name()

    @property
    def file_reader_db(self) -> aiosqlite.Connection | None:
        """The read-only connection of the live recorder file; ``None`` before start or without a file.

        It runs on its own aiosqlite thread, so file reads do not queue
        behind the file writes.
        """
        return self._reader_db

    def memory_coverage(self) -> MemoryCoverage | None:
        """The id range the memory store holds; ``None`` when there is no memory store."""
        if self._memory_store is None:
            return None
        return MemoryCoverage(
            floor_id=self._floor_id,
            last_id=self._last_id,
            evicted=self._evicted,
            evicted_max_ts=self._evicted_max_ts,
        )

    async def flush(self) -> None:
        """Write the recorded events to the memory store, then to the file.

        For reads of the file: they see every event recorded up to the
        call. Each stage runs shielded, so a caller that is cancelled (a
        read that timed out) never stops a stage halfway; the stage
        finishes on its own.
        """
        await asyncio.shield(self._stage_memory(EVICT_MIN_INTERVAL_SECONDS))
        task = self._queued_stage_file()
        # Count this flush as a receiver of the run's outcome while it
        # waits; a flush cancelled meanwhile (a read that timed out) stops
        # counting before the run ends.
        self._stage_file_receivers[task] = self._stage_file_receivers.get(task, 0) + 1
        try:
            await asyncio.shield(task)
        finally:
            self._forget_stage_file_receiver(task)

    def _forget_stage_file_receiver(self, task: asyncio.Task[None]) -> None:
        """Stop counting one waiting flush for ``task``."""
        left = self._stage_file_receivers.get(task, 0) - 1
        if left > 0:
            self._stage_file_receivers[task] = left
        else:
            self._stage_file_receivers.pop(task, None)

    def _queued_stage_file(self) -> asyncio.Task[None]:
        """The stage F run that has not taken its rows yet, shared by every flush that asks for one.

        A run takes the file queue only once it holds ``_flush_lock``, so a
        flush that joins a waiting run still gets the rows its own stage M
        just queued. A run that holds the lock already took its rows; the
        next flush starts a new run, which then waits.
        """
        task = self._waiting_stage_file
        if task is None or task.done():
            task = asyncio.ensure_future(self._run_queued_stage_file())
            # Every flush that awaited the run may have given up (a read
            # that timed out): then nobody receives a failure, and the
            # callback logs it. A flush still waiting gets it raised.
            task.add_done_callback(self._log_unreceived_stage_file_failure)
            self._waiting_stage_file = task
        return task

    def _log_unreceived_stage_file_failure(self, task: asyncio.Task[None]) -> None:
        """Log the exception of a shared stage F run that no waiting flush receives.

        Runs as the run's first done callback, before any waiting flush
        resumes: a flush still counted in ``_stage_file_receivers`` gets the
        exception raised, and its caller answers it. When every flush gave
        up, this line is the only trace of the failure.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None or self._stage_file_receivers.get(task, 0) > 0:
            return
        logger.error(
            'recorder_flush_loop_iteration_failed',
            category='recorder',
            stage='file',
            error=str(exc),
            error_type=type(exc).__name__,
            exc_info=exc,
        )

    async def _run_queued_stage_file(self) -> None:
        """One shared stage F run: wait for ``_flush_lock``, leave the waiting slot, run stage F."""
        if not self._has_file:
            return
        async with self._flush_lock:
            # This run takes its rows now: a flush that asks from here on needs a new run.
            if self._waiting_stage_file is asyncio.current_task():
                self._waiting_stage_file = None
            await self._stage_file_locked()

    async def flush_memory(self) -> None:
        """Write the recorded events to the memory store only, shielded like :meth:`flush`."""
        await asyncio.shield(self._stage_memory(EVICT_MIN_INTERVAL_SECONDS))

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
        schemaless would otherwise let a concurrent stage F hit
        ``no such table: events``.
        """
        if self._capture.events:
            await db.executescript(SCHEMA_EVENTS)
        if self._capture.config:
            await db.executescript(SCHEMA_WORKER_CONFIG)
        if self._config.sqlite.state_snapshots:
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
          the file under ``storage.dir`` and restart). The exception is
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
                f'delete it (storage.dir is documented as disposable) and '
                f'restart the worker. New rotation-cycle DBs include the '
                f'required columns automatically.'
            )

    async def start(self) -> None:
        self._running = True
        if self._memory_enabled and self._capture.events:
            # Before the file: the memory store holds the recent events for
            # the UI. A SQLite library without memdb fails the start.
            store = MemoryStore(self._instance, store_config=self._capture.config)
            try:
                await store.open()
            except BaseException:
                self._running = False
                raise
            self._memory_store = store
        if self._has_file:
            await self._warn_if_storage_dir_world_writable()
            await self._warn_if_storage_dir_network_fs()
            # Normalized so it compares equal to the leftover scan's paths
            # however storage.dir is spelled ('./data' or 'data/').
            self._db_path = os.path.normpath(make_db_path(self._storage_dir, self._worker_name))
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
                # Explicit busy_timeout so lock contention behaves
                # the same regardless of the driver's own default.
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
                if self._capture.events:
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
                await self._close_memory_store()
                self._running = False
                raise
            if self._capture.events:
                self._file_task = asyncio.create_task(self._file_loop())
                self._watch_background_task(self._file_task, 'file_flush')
            self._rotation_task = asyncio.create_task(self._rotation_loop())
            self._watch_background_task(self._rotation_task, 'rotation')
            if self._config.sqlite.state_snapshots:
                self._state_task = asyncio.create_task(self._state_sync_loop())
                self._watch_background_task(self._state_task, 'state_sync')
            if not self._archive.enabled:
                # Archiving is the only thing that removes rotated files, so
                # opting out means the folder grows until the operator prunes it.
                await logger.ainfo(
                    'recorder_archiving_disabled',
                    category='recorder',
                    folder=self.history_dir,
                    hint='sealed recorder database files are never deleted automatically; '
                    'prune the folder yourself or set recorder.archive.enabled: true',
                )
        if self._persists_events:
            # Stage M runs with a memory store, a durable store, or both.
            self._memory_task = asyncio.create_task(self._memory_loop())
            self._watch_background_task(self._memory_task, 'memory_flush')
            if self._pg_queue is not None:
                self._start_postgres_stage()
        # Outside the file branch on purpose: resource samples still feed
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
            db_path=self._db_path,
            **self._started_fields(),
        )

    def _start_postgres_stage(self) -> None:
        """Make the stage P writer, its thread and the Postgres loop; nothing connects before the first run."""
        settings = self._postgres
        queue = self._pg_queue
        assert settings is not None and queue is not None
        caller = ResilientCaller(
            settings.resilience, feature=RECORDER_FEATURE, provider=POSTGRES_PROVIDER, category='recorder'
        )
        self._pg_writer = PostgresWriter(
            settings,
            cluster=self._cluster_name,
            worker=self._worker_name,
            instance=self._instance,
            queue=queue,
            caller=caller,
            open_pool=self._postgres_open_pool,
        )
        self._pg_thread = PostgresLoopThread(POSTGRES_THREAD_NAME)
        self._postgres_task = asyncio.create_task(self._postgres_loop())
        self._watch_background_task(self._postgres_task, 'postgres_flush')

    def _started_fields(self) -> dict[str, Any]:
        """The mode and memory fields of ``recorder_started``.

        The memory bounds appear only when there is a memory store. With
        ``postgres``, ``postgres_connection`` and ``postgres_table``
        (schema-qualified) name the table.
        """
        fields: dict[str, Any] = {'persistence': self._persistence}
        if self._memory_enabled and self._capture.events:
            fields['memory_window_seconds'] = self._memory_window_seconds
            fields['memory_max_rows'] = self._memory_max_rows
            fields['memory_max_bytes'] = self._memory_max_bytes
        if self._postgres is not None:
            fields['postgres_connection'] = self._postgres.connection_name
            fields['postgres_table'] = self._postgres.qualified_table
        return fields

    async def _close_memory_store(self, wait_seconds: float | None = None) -> None:
        """Close the memory store, if there is one, waiting at most ``wait_seconds`` for by-name readers.

        The store is taken under ``_memory_lock``, so a stage M in flight
        (a UI read's) ends before the writer goes; the close runs after the
        lock is released.
        """
        async with self._memory_lock:
            store, self._memory_store = self._memory_store, None
        if store is not None:
            await store.close(wait_seconds)

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

    async def _warn_if_storage_dir_world_writable(self) -> None:
        """Warn when the own-files folder is world-writable (e.g. /tmp).

        Recorder DBs persist task args and subprocess output. In a
        world-writable directory other local users can pre-create or
        symlink-swap files, and (before the post-create chmod) read DB
        contents. Best-effort: a stat failure is ignored — the directory
        may not exist yet and ``make_db_path`` / connect handle that.
        """
        try:
            mode = os.stat(self._storage_dir).st_mode
        except OSError:
            return
        if mode & 0o002:
            await logger.awarning(
                'recorder_storage_dir_world_writable',
                category='recorder',
                storage_dir=self._storage_dir,
                hint='recorder DBs store task args and subprocess output; '
                'point storage.dir at a folder owned by the worker user '
                '(e.g. /var/lib/drakkar) on shared hosts',
            )

    async def _warn_if_storage_dir_network_fs(self) -> None:
        """Warn when the own-files folder sits on a network filesystem.

        SQLite on NFS/CIFS is a double hazard: file locking is unreliable
        (corruption risk), and every flush shares fate with the network
        path — when the storage server degrades, the recorder's writes
        block ON THE EVENT LOOP's flush cadence, so the observability
        stack degrades exactly when it is needed most. Warning only; the
        operator may accept the trade-off knowingly.
        """
        hit = detect_network_fs(self._storage_dir)
        if hit is None:
            return
        mount, fstype = hit
        await logger.awarning(
            'recorder_storage_dir_network_fs',
            category='recorder',
            storage_dir=self._storage_dir,
            mount=mount,
            fstype=fstype,
            hint='recorder databases on a network filesystem share fate with '
            'the storage server and risk SQLite lock corruption; point '
            'storage.dir at a local disk',
        )

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        """Append the event to the intake and broadcast it to WS subscribers.

        The event dict is never changed after this call: its row id is
        assigned later, by stage M, and rides next to the row. The fan-out
        may encode the same dict on the API server thread at any time.

        Nothing is appended when no table would ever take the event
        (``capture.events`` off, or no store at all), nor for ``skip_db`` events.

        Observability: when the intake is already at capacity, the next
        append silently evicts the oldest entry. That case is detected
        BEFORE the append (``len == maxlen``) and counted once per dropped
        event; after the append the buffer gauge is refreshed.
        """
        event['dt'] = format_dt(event['ts'])
        if not skip_db and self._persists_events:
            # deque(maxlen=N) accepts up to N items; when len == maxlen a
            # subsequent append drops the leftmost entry. We count one
            # drop per event lost — prometheus_client Counter.inc() is
            # thread-safe so callers from multiple loops/threads are fine.
            maxlen = self._intake.maxlen
            if maxlen is not None and len(self._intake) >= maxlen:
                recorder_dropped_events.inc()
            self._intake.append(event)
            self._buffered_bytes += _event_weight(event)
            self._update_buffer_gauge()
        if not skip_ws:
            self._fanout.broadcast(event)

    # --- Worker config (autodiscovery) ---

    async def write_config(self, drakkar_config: DrakkarConfig) -> None:
        """Write worker configuration to the worker_config table of the memory store and of the file.

        Security note: this SQLite file is downloadable via the debug UI.
        Any value written here is effectively public to anyone who can
        reach that endpoint. Redact secrets before insertion:
        - kafka_brokers: strip credentials from SASL URIs.
        - env_vars: redact values of any secret-named var; URL-shape
          values have embedded credentials stripped.
        """
        self._drakkar_config = drakkar_config
        if not self._capture.config:
            return
        row = self._config_row(drakkar_config)
        await self._write_config_to_memory(row)
        if self._db:
            await self._insert_config_row(self._db, row)

    async def _write_config_to_memory(self, row: list[Any]) -> None:
        """Write the worker_config row into the memory store; a failure is logged and never stops the file write."""
        store = self._memory_store
        if store is None:
            return
        # Under the memory lock: a stage M between its insert and its commit
        # must not commit, or roll back, this row with its chunk.
        async with self._memory_lock:
            if not store.is_open:
                return
            writer = store.writer
            # An earlier chunk's open transaction would be committed with
            # this row; the gap delete may run here too, it is harmless.
            await self._settle_previous_chunk(writer)
            if self._memory_dirty:
                await self._log_config_memory_failure('the memory writer holds a transaction it cannot roll back')
                return
            try:
                await self._insert_config_row(writer, row)
            except sqlite3.Error as exc:
                self._memory_dirty = True
                await self._rollback_memory(writer)
                await self._log_config_memory_failure(str(exc))

    async def _log_config_memory_failure(self, error: str) -> None:
        await self._memory_log.emit(
            'recorder_memory_config_write_failed',
            logger.awarning,
            'recorder_memory_config_write_failed',
            category='recorder',
            error=error,
        )

    async def _write_config_to_file(self) -> None:
        """Write the worker_config row into the current file only (a new file after a rotation)."""
        if self._drakkar_config is None or not self._db or not self._capture.config:
            return
        await self._insert_config_row(self._db, self._config_row(self._drakkar_config))

    def _config_row(self, drakkar_config: DrakkarConfig) -> list[Any]:
        """The values of worker_config row 1, secrets redacted."""
        env_vars = {name: sanitize_env_value(name, os.environ.get(name, '')) for name in self._capture.env_vars}
        sinks: dict[str, list[str]] = {}
        sinks_cfg = drakkar_config.sinks
        if sinks_cfg:
            for sink_type in ('kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'):
                names = list(getattr(sinks_cfg, sink_type, {}).keys())
                if names:
                    sinks[sink_type] = names
        # A worker with the Kafka source off reads no topic, so the
        # recorded row says so plainly rather than showing the source
        # config's unused default. ``resolved_consumer_group`` already
        # carries the analogous rule for the group.
        source = drakkar_config.sources.kafka
        source_topic = source.topic if source.enabled else ''
        source_connection = drakkar_config.kafka_source_connection
        kafka_brokers = source_connection.brokers if source_connection is not None else ''
        consumer_group = drakkar_config.resolved_consumer_group

        now = time.time()
        return [
            self._worker_name,
            self._cluster_name or None,
            detect_worker_ip(),
            self._api_port,
            self._api_public_url or None,
            redact_url(kafka_brokers),
            source_topic,
            consumer_group,
            drakkar_config.executor.binary_path,
            drakkar_config.executor.max_executors,
            drakkar_config.executor.task_timeout,
            drakkar_config.executor.max_retries,
            drakkar_config.executor.window_size,
            encode_json_str(sinks),
            encode_json_str(env_vars),
            now,
            format_dt(now),
        ]

    @staticmethod
    async def _insert_config_row(db: aiosqlite.Connection, row: list[Any]) -> None:
        """Replace worker_config row 1 on ``db`` and commit."""
        await db.execute(
            """INSERT OR REPLACE INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            row,
        )
        await db.commit()

    # --- Worker state (periodic snapshots) ---

    async def _state_sync_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.sqlite.state_interval)
            # Same per-iteration guard as the memory loop: one bad snapshot
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

    async def _host_sample_loop(self) -> None:
        """Per-tick host sampling: one ``resource_sample`` event per
        state-sync interval.

        Runs whenever the recorder runs — memory-only mode included (the
        events then only feed WS subscribers; nothing touches the DB from
        here). Each sampler is best-effort: a platform without a given
        ``/proc`` source simply omits that field.
        """
        while self._running:
            await asyncio.sleep(self._host_sample_seconds())
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

    def _host_sample_seconds(self) -> int:
        """The sampling interval: ``sqlite.state_interval`` with a recorder file, a fixed one otherwise."""
        return self._config.sqlite.state_interval if self._has_file else HOST_SAMPLE_SECONDS

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
        if not self._has_file or not self._db or not self._config.sqlite.state_snapshots:
            return
        app_state = self._state_provider() if self._state_provider else {}
        now = time.time()
        await self._db.execute(
            """INSERT INTO worker_state
               (uptime_seconds, assigned_partitions, partition_count,
                pool_active, pool_max, total_queued,
                consumed_count, completed_count, failed_count,
                produced_count, committed_count, paused,
                health_state, loop_lag_ms, updated_at, updated_at_dt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                now,
                format_dt(now),
            ],
        )
        await self._db.commit()

    async def stop(self, timeout: float | None = None, *, reserve_seconds: float = 0.0) -> None:
        """Flush, then seal the live file and move it into the history folder.

        ``timeout`` is the shutdown time left (``None``: no limit). The seal
        itself (journal mode, meta, close) is fast and always runs; the move
        into the history folder runs only while time remains, so a slow
        network mount cannot hold the stop past the deadline. A file that
        is not moved stays in ``storage.dir`` and the next start with the
        same worker name seals it. Last, the stop waits for an archive pass
        still running in its thread, keeping ``reserve_seconds`` of the time
        left for the shutdown steps after the recorder (the election stop and
        the registry withdraw); see :meth:`_wait_for_archive_pass`.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        self._running = False
        self._archive_stop.set()
        for task in (
            self._memory_task,
            self._postgres_task,
            self._file_task,
            self._rotation_task,
            self._state_task,
            self._host_sample_task,
            self._leftover_task,
            self._sealing_task,
        ):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._archive_executor is not None:
            # Not waited for here: a pass in progress ends after its window and
            # its fence still needs this loop. The wait at the end of the stop
            # waits for it, inside the shutdown time left.
            self._archive_executor.shutdown(wait=False)
            self._archive_executor = None
        # Drop any pending deferred start events and disarm the shared sweep.
        self._fanout.close()

        # Final stages + state sync are best-effort: a failure in any must
        # not skip the seal and the _LIVE_RECORDERS discard below, or
        # shutdown would leak connections and interpreter exit would try to
        # flush into a recorder that never tore down. Stage M first, under
        # its own lock.
        try:
            await self._stage_memory(0.0)
        except Exception as exc:
            await logger.awarning('recorder_final_flush_failed', category='recorder', stage='memory', error=str(exc))
        await self._stop_postgres_stage(deadline, reserve_seconds)
        # From the final stage F to the end of the seal, ``_flush_lock`` is
        # held: a stage F run queued by a read meanwhile then waits, finds no
        # writer once it gets the lock, and never writes into the file
        # being sealed. Nothing below awaits ``flush()``.
        sealed: tuple[str, dict[str, Any] | None] | None = None
        async with self._flush_lock:
            await self._final_file_stage()
            if self._db:
                writer, path = self._db, self._db_path
                self._db = None
                meta = await self._seal(path, seal_connection(writer, db_path=path, **self._seal_identity()))
                sealed = (path, meta)
        if sealed is not None and sealed[1] is not None:
            await self._publish_until(sealed[0], sealed[1], deadline)
        # Last: an archive pass still running in its thread checks its lease
        # through the election, which the worker stops after this wait.
        await self._wait_for_archive_pass(deadline, reserve_seconds)
        # The memory store goes last: by-name readers still open get up to
        # MEMORY_CLOSE_WAIT_SECONDS, inside the shutdown time left, to
        # finish, and later opens get nothing.
        left = _time_left(deadline)
        await self._close_memory_store(
            MEMORY_CLOSE_WAIT_SECONDS if left is None else min(MEMORY_CLOSE_WAIT_SECONDS, left)
        )
        # Clean shutdown flushed everything above — leave the live set so
        # interpreter exit does not reopen the closed DB. Discarding a
        # never-added recorder (start() not reached) is a no-op.
        _LIVE_RECORDERS.discard(self)
        await logger.ainfo('recorder_stopped', category='recorder')

    async def _stop_postgres_stage(self, deadline: float | None, reserve_seconds: float) -> None:
        """The final stage P, the write pool close and the thread stop, within the shutdown time left.

        The COPYs of the final stage get the time left minus
        ``reserve_seconds``. The final stage and the close each wait the
        same, plus at most ``POSTGRES_STOP_ANSWER_SECONDS`` of the reserve
        for the thread to answer, and never more than the time left. The
        join waits as long, and at least ``POSTGRES_STOP_ANSWER_SECONDS``
        even when no time is left: a thread that was just told to stop needs
        that long to end, and one still running after it is the only one
        reported. Without a deadline the close and the join wait at most
        ``POSTGRES_STOP_STEP_SECONDS``. The
        final stage is never cancelled: after its wait it still counts and
        logs the rows it drops on the thread. Each step is best-effort: a
        failure is logged and the next step runs.
        """
        thread, writer = self._pg_thread, self._pg_writer
        if thread is None or writer is None:
            return
        self._pg_thread = self._pg_writer = None

        def available() -> float | None:
            left = _time_left(deadline)
            return None if left is None else max(left - reserve_seconds, 0.0)

        def wait_seconds() -> float | None:
            left, usable = _time_left(deadline), available()
            return None if left is None or usable is None else min(left, usable + POSTGRES_STOP_ANSWER_SECONDS)

        def step_seconds() -> float:
            bound = wait_seconds()
            return POSTGRES_STOP_STEP_SECONDS if bound is None else bound

        copy_seconds = available()
        try:
            await thread.wait(lambda: writer.final(copy_seconds), wait_seconds())
        except Exception as exc:
            await self._log_postgres_stop_failure(describe_error(exc))
        try:
            await thread.run(writer.close, step_seconds())
        except Exception as exc:
            await self._log_postgres_stop_failure(describe_error(exc))
        if not await thread.stop(max(step_seconds(), POSTGRES_STOP_ANSWER_SECONDS)):
            await self._log_postgres_stop_failure('the stage P thread did not end in the shutdown time left')

    @staticmethod
    async def _log_postgres_stop_failure(error: str) -> None:
        await logger.awarning('recorder_final_flush_failed', category='recorder', stage='postgres', error=error)

    async def _final_file_stage(self) -> None:
        """The last stage F, the state sync and the reader close at stop; ``_flush_lock`` is held by the caller.

        Each step is best-effort: a failure is logged and the seal still runs.
        """
        if self._has_file:
            try:
                await self._stage_file_locked()
            except Exception as exc:
                await logger.awarning('recorder_final_flush_failed', category='recorder', stage='file', error=str(exc))
        try:
            await self._sync_state()
        except Exception as exc:
            await logger.awarning(
                'recorder_final_state_sync_failed',
                category='recorder',
                error=str(exc),
            )
        # Close the reader first: the seal needs the writer to be the last
        # open connection. A close failure here only makes the seal fail,
        # which leaves the file for the next start.
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

    async def _publish_until(self, path: str, meta: dict[str, Any], deadline: float | None) -> None:
        """Publish a sealed file at stop, waiting no later than ``deadline``."""
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            await self._seal_failed(path, TimeoutError('no shutdown time left to move the file'), 'publish')
            return
        await self._publish(path, meta, timeout=remaining)

    def _last_breath_flush(self) -> None:
        """Synchronous, best-effort write of the waiting rows to the file at interpreter exit.

        Reached through the process-wide atexit hook while this recorder
        is in ``_LIVE_RECORDERS`` (added by :meth:`start`, removed by a
        clean :meth:`stop`). It fires exactly when the async machinery
        could not do its job — a startup failure after the recorder came
        up, an unhandled exception, a stray ``sys.exit`` — and writes
        straight through :mod:`sqlite3`, because at interpreter exit the
        event loop (and aiosqlite's worker thread) are already gone. The
        most interesting events of a dying worker are the last ones; this
        is what keeps them.

        The file queue goes first (its rows already carry ids), then the
        intake with fresh ids, so the file keeps id order; no memory store is
        involved. Without a file it does nothing.

        Best-effort by design: a short busy timeout, every failure path
        silent. Cannot fire on SIGKILL / OOM — the watchdog file covers
        detecting those, and whatever the last periodic write wrote is
        what remains.
        """
        if not self._running or not self._has_file or not self._db_path or not self._capture.events:
            return
        if not self._file_queue and not self._intake:
            return
        rows = [row for _, row in self._file_queue]
        self._file_queue.clear()
        batch = list(self._intake)
        self._intake.clear()
        first_id = self._next_id
        self._next_id += len(batch)
        rows.extend(_row_tuple(first_id + index, entry) for index, entry in enumerate(batch))
        try:
            conn = sqlite3.connect(self._db_path, timeout=1.0)
            try:
                conn.executemany(INSERT_EVENT_SQL, rows)
                conn.commit()
            finally:
                conn.close()
        except Exception:
            # Interpreter teardown is no place to raise; the periodic
            # writes already wrote everything they could.
            return
        with contextlib.suppress(Exception):
            logger.warning(
                'recorder_last_breath_flush',
                category='recorder',
                events=len(rows),
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

    # --- Write path: the memory loop and stage M, the file loop and stage F ---

    def _update_buffer_gauge(self) -> None:
        """Report the rows waiting in memory: the intake plus the file queue."""
        recorder_buffer_size.set(len(self._intake) + len(self._file_queue))

    async def _wait_until_due(
        self,
        queue: deque,
        *,
        watch_bytes: bool,
        wakeup: asyncio.Event | None = None,
    ) -> None:
        """Wait out ``flush_interval``, or return early on a burst.

        The interval alone is not enough at high event rates: a producer
        filling ``queue`` faster than one interval's worth would reach
        ``max_buffer_rows`` and start evicting between ticks. Returning as soon
        as it is half full turns that into an early write instead of lost
        events. ``watch_bytes`` also returns at the payload high-water mark
        of the intake, and ``wakeup`` when that event is set. The intake is
        polled rather than signalled because ``_record`` is also reached
        from the webapp's own event loop, and ``Event.set`` is not safe
        across loops; ``wakeup`` is set on the main loop only.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._config.flush_interval
        maxlen = queue.maxlen
        high_water = maxlen // 2 if maxlen is not None else None
        while self._running:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            pause = min(remaining, FLUSH_POLL_SECONDS)
            if wakeup is None:
                await asyncio.sleep(pause)
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wakeup.wait(), pause)
                if wakeup.is_set():
                    return
            if high_water is not None and len(queue) >= high_water:
                return
            if watch_bytes and self._buffered_bytes >= FLUSH_BYTES_HIGH_WATER:
                return

    async def _memory_loop(self) -> None:
        """Run stage M on the intake's triggers; a hung disk never holds this loop."""
        while self._running:
            await self._wait_until_due(self._intake, watch_bytes=True)
            # Per-iteration guard: an unexpected raise would end the task and
            # stop recording for the rest of the process. Cancellation still
            # propagates — that is how ``stop`` ends the loop.
            try:
                await self._stage_memory(0.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._log_loop_failure('memory', exc)
            if self._has_file:
                self._file_wakeup.set()
            if self._pg_queue is not None:
                self._pg_wakeup.set()

    async def _postgres_loop(self) -> None:
        """Post one stage P run to its thread on the memory loop's tick, a half-full P queue, or the interval.

        The run's result is awaited before the next one is posted, so two
        runs never overlap; each COPY attempt has its own time limit, so a
        run always ends.
        """
        thread, writer = self._pg_thread, self._pg_writer
        assert thread is not None and writer is not None
        while self._running:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._pg_wakeup.wait(), self._config.flush_interval)
            self._pg_wakeup.clear()
            try:
                await thread.run(writer.run_once, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._log_loop_failure('postgres', exc)

    async def _file_loop(self) -> None:
        """Run stage F on the memory loop's tick, when the file queue is half full, or at the interval."""
        while self._running:
            await self._wait_until_due(self._file_queue, watch_bytes=False, wakeup=self._file_wakeup)
            self._file_wakeup.clear()
            try:
                await self._stage_file()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._log_loop_failure('file', exc)

    @staticmethod
    async def _log_loop_failure(stage: str, exc: Exception) -> None:
        await logger.aerror(
            'recorder_flush_loop_iteration_failed',
            category='recorder',
            stage=stage,
            error=str(exc),
            error_type=type(exc).__name__,
            exc_info=exc,
        )

    async def _stage_memory(self, evict_min_interval: float) -> None:
        """Stage M: give the intake rows their ids, write them to the memory store if there is one, hand them on.

        Pops the intake in chunks of at most ``FLUSH_CHUNK_ROWS``. For each
        chunk: assign ids, build the row tuples once, write and commit them
        when this recorder keeps a memory store, then, with a file or
        Postgres, append the ``(id, row)`` pairs to the file queue or the P
        queue. With a memory store, an eviction pass ends the stage.
        A chunk that fails follows the failure rules of
        :meth:`_write_memory_chunk`; a transient failure stops the stage
        and skips the eviction pass (the next tick runs both).
        ``evict_min_interval`` is the least time between two eviction passes
        (0 for the loops, rotation and stop).
        """
        async with self._memory_lock:
            writer: aiosqlite.Connection | None = None
            if self._memory_enabled:
                store = self._memory_store
                if store is None or not store.is_open:
                    return
                writer = store.writer
                if not await self._settle_previous_chunk(writer):
                    # A gap or an open transaction is still there: writing now
                    # would break the one contiguous id range. The next tick
                    # tries again; the intake keeps its rows.
                    self._update_buffer_gauge()
                    return
            # Snapshot the depth: events appended while this stage awaits
            # ride along on the next tick rather than extending this one
            # indefinitely under a fast producer.
            pending = len(self._intake)
            # Reset before draining: the events appended while this stage
            # awaits are counted again as they arrive.
            self._buffered_bytes = 0
            completed = True
            while pending > 0:
                take = min(pending, FLUSH_CHUNK_ROWS)
                pending -= take
                batch = [self._intake.popleft() for _ in range(take)]
                first_id = self._next_id
                self._next_id += take
                rows = [_row_tuple(first_id + index, entry) for index, entry in enumerate(batch)]
                if writer is None:
                    self._hand_on(rows)
                    # No store write gives the loop a turn here, and a full intake is many chunks.
                    await asyncio.sleep(0)
                elif not await self._write_memory_chunk(writer, batch, rows):
                    completed = False
                    break
            self._update_buffer_gauge()
            await self._log_postgres_queue_drops()
            if completed and writer is not None:
                await self._evict(evict_min_interval)

    async def _evict(self, evict_min_interval: float) -> None:
        """The eviction pass at the end of a stage M, under ``_memory_lock``; then set the memory gauges.

        Runs at most once per ``evict_min_interval`` seconds (0: every
        time). The pass's coverage becomes the recorder's; a failed pass is
        logged, and the next one starts again with the window.
        """
        store = self._memory_store
        if store is None:
            return
        started_at = self._evict_clock()
        last = self._last_evict_at
        if evict_min_interval > 0 and last is not None and started_at - last < evict_min_interval:
            return
        self._last_evict_at = started_at
        now = time.time()
        result = await run_eviction_pass(
            store.writer,
            MemoryCoverage(
                floor_id=self._floor_id,
                last_id=self._last_id,
                evicted=self._evicted,
                evicted_max_ts=self._evicted_max_ts,
            ),
            now=now,
            window_seconds=self._memory_window_seconds,
            max_rows=self._memory_max_rows,
            max_bytes=self._memory_max_bytes,
            on_batch=self._note_evicting_batch,
        )
        self._adopt_eviction(result, now)
        if result.failed_reason is not None and result.error is not None:
            await self._log_eviction_failure(result.failed_reason, result.error, result.rollback_error)

    def _note_evicting_batch(self, bound: int, batch_max_ts: float | None) -> None:
        """Before a delete batch: move the coverage past the rows about to go.

        A memory read checks the coverage again after its SELECT; moving it
        before the DELETE means a read that runs after the commit never
        trusts a range it lost. Moved early, the values can only send a read
        to the file. The pass's result sets the final values.
        """
        self._evicted = True
        if batch_max_ts is not None:
            self._evicted_max_ts = max(self._evicted_max_ts, batch_max_ts)
        self._floor_id = max(self._floor_id, bound)

    def _adopt_eviction(self, result: EvictionResult, now: float) -> None:
        """Take the pass's coverage, count its rows by reason and set the gauges.

        After a failed pass only the row gauge is set; the byte and age
        gauges keep the values of the last pass that measured them.
        """
        coverage = result.coverage
        self._floor_id = coverage.floor_id
        self._evicted = coverage.evicted
        self._evicted_max_ts = coverage.evicted_max_ts
        for reason, count in result.evicted_by_reason.items():
            if count:
                recorder_memory_evicted_rows.labels(reason=reason).inc(count)
        if result.rollback_error is not None:
            self._memory_dirty = True
        recorder_memory_rows.set(coverage.rows)
        if result.failed_reason is not None:
            return
        recorder_memory_bytes.set(result.used_bytes)
        recorder_memory_allocated_bytes.set(result.allocated_bytes)
        oldest_age = 0.0 if result.oldest_ts is None else max(0.0, now - result.oldest_ts)
        recorder_memory_oldest_age_seconds.set(oldest_age)

    async def _log_eviction_failure(
        self, reason: str, error: sqlite3.Error, rollback_error: sqlite3.Error | None
    ) -> None:
        """Log ``recorder_memory_eviction_failed``, at most one line per 60 s."""
        fields: dict[str, Any] = {'error': str(error), 'reason': reason}
        if rollback_error is not None:
            fields['rollback_error'] = str(rollback_error)
        await self._memory_log.emit(
            'recorder_memory_eviction_failed',
            logger.awarning,
            'recorder_memory_eviction_failed',
            category='recorder',
            **fields,
        )

    async def _settle_previous_chunk(self, writer: aiosqlite.Connection) -> bool:
        """Before a memory write: roll back an earlier chunk's open transaction and delete a pending gap.

        Returns whether the writer is clean: no open transaction left and no
        gap waiting. A stage M writes no chunk otherwise.
        """
        if self._memory_dirty:
            await self._rollback_memory(writer)
            if self._memory_dirty:
                return False
        if self._pending_gap_id is not None:
            return await self._delete_pending_gap(writer)
        return True

    async def _write_memory_chunk(self, writer: aiosqlite.Connection, batch: list[dict], rows: list[tuple]) -> bool:
        """Write one chunk to the memory store; return whether the stage goes on.

        On any failure the open transaction is rolled back first. A
        transient error (busy or locked) puts the chunk back at the front of
        the intake and gives its ids back: the stage stops and the next tick
        retries it, so nothing is dropped. A final error keeps the store one
        contiguous range: the chunk goes to the file queue (``sqlite``) or
        the P queue (``postgres``) or is lost, and every memory row up to its
        last id is deleted. When
        that rollback or delete fails, the stage stops too, so no later
        chunk lands above the rows still to delete; the next stage M
        deletes them first.

        A cancelled write (``stop()``) does only synchronous bookkeeping:
        without a file the chunk goes back to the intake with its ids, and
        the final stage M writes it; with a file it goes to the file queue
        and the memory store moves its floor past it. The next stage M rolls
        the writer back first either way.
        """
        first_id = rows[0][0]
        settled = False
        try:
            await writer.executemany(INSERT_EVENT_SQL, rows)
            await writer.commit()
            settled = True
        except sqlite3.Error as exc:
            settled = True
            self._memory_dirty = True
            if is_transient_memory_error(exc):
                self._requeue_front(self._intake, batch)
                self._next_id = first_id
                await self._rollback_memory(writer)
                await self._memory_log.emit(
                    'recorder_memory_write_retry',
                    logger.awarning,
                    'recorder_memory_write_retry',
                    category='recorder',
                    error=str(exc),
                    batch_size=len(batch),
                )
                return False
            self._keep_rows_without_memory(rows)
            recorder_memory_write_failures.inc()
            await self._memory_log.emit(
                'recorder_memory_write_failed',
                logger.aerror,
                'recorder_memory_write_failed',
                category='recorder',
                error=str(exc),
                batch_size=len(batch),
            )
            return await self._settle_previous_chunk(writer)
        finally:
            if not settled:
                # Cancelled mid-write. Everything here is synchronous: an
                # await during a cancellation unwind could be cancelled again
                # and skip the bookkeeping.
                self._memory_dirty = True
                if self._has_file:
                    self._keep_rows_without_memory(rows)
                else:
                    self._requeue_front(self._intake, batch)
                    self._next_id = first_id
        self._hand_on(rows)
        return True

    def _hand_on(self, rows: list[tuple]) -> None:
        """Note the chunk's last id and give its ``(id, row)`` pairs to the file or the P queue, if there is one."""
        self._last_id = rows[-1][0]
        if self._has_downstream:
            self._append_downstream([(row[0], row) for row in rows])

    def _keep_rows_without_memory(self, rows: list[tuple]) -> None:
        """Account for a chunk that did not reach the memory store: the downstream queue and a gap.

        The memory store then starts after the chunk: every row up to its
        last id is due for deletion (:meth:`_delete_pending_gap`), the floor
        and ``last_id`` move past it, ``evicted`` is set and
        ``evicted_max_ts`` rises to the chunk's largest ``ts``, so the read
        rules stay right. Synchronous.
        """
        last_id = rows[-1][0]
        if self._has_downstream:
            self._append_downstream([(row[0], row) for row in rows])
        self._pending_gap_id = last_id
        self._last_id = last_id
        self._floor_id = last_id + 1
        self._evicted = True
        # ``ts`` is the second column of a row tuple.
        self._evicted_max_ts = max(self._evicted_max_ts, max(row[1] for row in rows))

    async def _delete_pending_gap(self, writer: aiosqlite.Connection) -> bool:
        """Delete the memory rows up to the pending gap id, in eviction batches; return whether it is done.

        On failure the gap stays pending. When the rollback after the failed
        batch fails too, the writer is marked dirty, so the next memory
        write rolls it back first.
        """
        gap_id = self._pending_gap_id
        if gap_id is None:
            return True
        result = await delete_id_prefix(writer, gap_id + 1, on_batch=self._note_evicting_batch)
        if result.deleted:
            recorder_memory_evicted_rows.labels(reason='gap').inc(result.deleted)
        if result.max_ts is not None:
            self._evicted_max_ts = max(self._evicted_max_ts, result.max_ts)
        if result.error is None:
            self._pending_gap_id = None
            return True
        if result.rollback_error is not None:
            self._memory_dirty = True
        await self._log_eviction_failure('gap', result.error, result.rollback_error)
        return False

    async def _rollback_memory(self, writer: aiosqlite.Connection) -> None:
        """Roll back the memory writer's open transaction, if any."""
        try:
            await writer.rollback()
        except sqlite3.Error as exc:
            await self._memory_log.emit(
                'recorder_memory_rollback_failed',
                logger.awarning,
                'recorder_memory_rollback_failed',
                category='recorder',
                error=str(exc),
            )
            return
        self._memory_dirty = False

    def _append_downstream(self, pairs: list[QueuedPair]) -> None:
        """Hand ``(id, row)`` pairs on: to the file queue with ``sqlite``, to the P queue with ``postgres``.

        Synchronous: it also runs while a cancelled write unwinds. Pairs the
        P queue drops are counted at once and logged when stage M ends.
        """
        queue = self._pg_queue
        if queue is None:
            self._append_file_queue(pairs)
            return
        dropped = queue.append(pairs)
        if dropped:
            recorder_postgres_dropped_rows.labels(reason='queue_full').inc(dropped)
            self._pg_unlogged_drops += dropped
        if queue.past_half():
            self._pg_wakeup.set()

    async def _log_postgres_queue_drops(self) -> None:
        """Log the pairs the P queue dropped since the last line, at most one line per 60 s.

        A line the rate limit holds back keeps its rows: they add up until a
        line is written, so every dropped row reaches a log line.
        """
        rows = self._pg_unlogged_drops
        if not rows:
            return
        written = False

        async def log_and_note(event: str, **fields: Any) -> None:
            nonlocal written
            written = True
            await logger.awarning(event, **fields)

        await self._pg_drop_log.emit(
            'queue_full',
            log_and_note,
            'recorder_postgres_rows_dropped',
            category='recorder',
            reason='queue_full',
            rows=rows,
            error='',
        )
        if written:
            # Drops counted while the line was written wait for the next one.
            self._pg_unlogged_drops -= rows

    def _append_file_queue(self, pairs: list[tuple[int, tuple]]) -> None:
        """Append ``(id, row)`` pairs to the file queue; pairs pushed out of a full queue count as dropped events."""
        maxlen = self._file_queue.maxlen
        if maxlen is not None:
            overflow = len(self._file_queue) + len(pairs) - maxlen
            if overflow > 0:
                recorder_dropped_events.inc(overflow)
        self._file_queue.extend(pairs)

    async def _stage_file(self) -> None:
        """Stage F: write the file queue's stored rows to the live file, in chunks.

        Chunking bounds what a write that keeps failing can lose: a chunk
        that still fails after ``max_flush_retries`` is dropped from the
        file only (a memory store already holds its rows). See
        ``docs/observability.md`` for the ``max_buffer_rows`` sizing rule.
        """
        if not self._has_file:
            return
        async with self._flush_lock:
            await self._stage_file_locked()

    async def _stage_file_locked(self) -> None:
        """Stage F with ``_flush_lock`` held by the caller."""
        # Snapshot ``self._db`` once and use it for every write and commit
        # of this stage: a concurrent ``_rotate`` swaps ``self._db`` under
        # this lock only, so the writes of one stage always land in one file.
        db = self._db
        if not db or not self._file_queue:
            return
        # Observe the whole stage across every chunk — the histogram
        # surfaces the disk-I/O latency tail.
        flush_start = time.monotonic()
        # Snapshot the depth, as stage M does.
        pending = len(self._file_queue)
        while pending > 0:
            take = min(pending, FLUSH_CHUNK_ROWS)
            pending -= take
            batch = [self._file_queue.popleft() for _ in range(take)]
            if not await self._write_file_chunk(db, batch):
                self._update_buffer_gauge()
                return
        self._flush_failures = 0
        recorder_flush_duration.observe(time.monotonic() - flush_start)
        self._update_buffer_gauge()

    async def _write_file_chunk(self, db: aiosqlite.Connection, batch: list[tuple[int, tuple]]) -> bool:
        """Write one chunk of ``(id, row)`` pairs to the file; return whether the stage should keep going.

        ``False`` means this chunk did not land: it was either re-queued for
        the next tick or dropped at the retry cap. Either way the rest of the
        file queue stays put — its rows are strictly newer, so stopping here
        preserves order.
        """
        rows = [row for _, row in batch]
        # ``batch_settled`` flips once the popped batch has a final home:
        # committed, re-queued for retry, or deliberately dropped at the
        # retry cap. Any other exit — a CancelledError from ``stop()``
        # cancelling the file loop, or an unexpected error — reaches the
        # ``finally`` with the flag still False and re-queues the batch, so
        # cancellation mid-write never silently loses it. The rows may
        # already sit in the open transaction; ``INSERT OR IGNORE`` makes
        # the retry a no-op for them.
        batch_settled = False
        try:
            await db.executemany(INSERT_EVENT_SQL, rows)
            await db.commit()
            batch_settled = True
            return True
        except sqlite3.Error as exc:
            # ``sqlite3.Error``, not just OperationalError: aiosqlite raises
            # the stdlib sqlite3 exception classes, and the whole family must
            # go through the same retry/drop accounting.
            #
            # Rotation edge case: if ``_rotate`` runs between a failed write
            # and its retry, the re-queued rows end up in the NEW file. Their
            # ``ts`` keeps their wall-clock position; only the file they sit
            # in shifts, which beats dropping them.
            self._flush_failures += 1
            recorder_flush_retries.inc()
            if self._flush_failures >= self._config.sqlite.max_flush_retries:
                # Give up on this chunk: drop it and reset the counter so the
                # next chunk starts from a clean slate. Bookkeeping runs
                # before the ``await`` on the logger so a cancellation during
                # the log cannot make the finally re-queue a dropped batch.
                batch_settled = True
                attempt = self._flush_failures
                self._flush_failures = 0
                recorder_flush_batches_dropped.inc()
                self._update_buffer_gauge()
                await logger.aerror(
                    'recorder_flush_batch_dropped',
                    category='recorder',
                    attempt=attempt,
                    batch_size=len(batch),
                    error=str(exc),
                )
                return False
            # Not yet at the retry cap — re-queue (synchronously, before the
            # log await) and let the next tick try again.
            self._requeue_front(self._file_queue, batch)
            batch_settled = True
            await logger.awarning(
                'recorder_flush_retry',
                category='recorder',
                attempt=self._flush_failures,
                max_retries=self._config.sqlite.max_flush_retries,
                batch_size=len(batch),
                error=str(exc),
            )
            return False
        finally:
            if not batch_settled:
                # Interrupted mid-write with the transaction not committed.
                # Synchronous: an await during a cancellation unwind could be
                # cancelled again and skip the re-queue.
                self._requeue_front(self._file_queue, batch)

    def _requeue_front(self, queue: deque, batch: list) -> None:
        """Put a popped-but-unwritten batch back at the FRONT of ``queue`` (the intake or the file queue).

        ``extendleft`` reverses its iterable, so ``batch`` is pre-reversed to
        keep its order. Items appended while the write awaited stay at the
        back — correct, since they are strictly newer.

        Overflow detection: the queue is bounded by ``max_buffer``. If
        appends filled it during the write's await, ``extendleft`` silently
        evicts items from the TAIL (the newest) to honour ``maxlen``. Those
        are counted arithmetically — ``(len_before + batch_len) - maxlen`` —
        on ``recorder_requeue_overflow``.

        Fully synchronous (sync logging included): it also runs from a
        cancellation unwind, where an await could itself be cancelled and
        skip the re-queue.
        """
        len_before = len(queue)
        batch_len = len(batch)
        queue.extendleft(reversed(batch))
        maxlen = queue.maxlen
        if maxlen is not None:
            overflow = (len_before + batch_len) - maxlen
            if overflow > 0:
                recorder_requeue_overflow.inc(overflow)
                logger.warning(
                    'recorder_buffer_overflow_on_requeue',
                    category='recorder',
                    dropped=overflow,
                    buffer_len_before=len_before,
                    batch_size=batch_len,
                    max_buffer=maxlen,
                )
        self._update_buffer_gauge()

    async def _rotation_loop(self) -> None:
        while self._running:
            await self._sleep(self._config.sqlite.rotation_interval * SECONDS_PER_HOUR)
            # Same guard as the memory loop: one failed rotation must cost a
            # single interval, not every rotation from here on.
            try:
                await self._rotate()
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
            # Whatever the rotation did, files sealed earlier still wait for
            # their move. Due windows wait for the next archive slot.
            self._start_sealing()

    def archive_cluster_task(self) -> ClusterTask:
        """The system cluster task ``history.archive``, for a pass over the whole cluster.

        Whether the worker runs it is decided by its periodic roster
        (``PeriodicRoster.archive_scope``).

        A won slot runs the archive pass on the recorder's own archive
        thread and waits for it; the pass checks the slot's lease through a
        :class:`~drakkar.recorder.election.ThreadFence` before each publish.
        When the slot loop is cancelled (shutdown) the wait ends at once and
        the pass goes on until :meth:`stop` ends it between windows. A slot
        this worker does not win refreshes the footprint gauges on the same
        thread, without waiting, when that thread is idle.
        """
        # Function-local: drakkar.cluster_tasks imports the recorder package.
        from drakkar.cluster_tasks import ClusterTask

        return ClusterTask(
            name=ARCHIVE_TASK_NAME,
            run=self._run_archive_slot,
            interval=self._archive.effective_interval,
            cron=self._archive.cron,
            lease_seconds=self._archive.lease,
            on_error='continue',
            system=True,
            on_skip=self._observe_footprint_when_idle,
        )

    async def _run_archive_slot(self, ctx: SlotContext) -> None:
        """Run one archive pass for a won slot in the archive thread, fenced by the slot's lease."""
        if self._archive_stop.is_set():
            return
        election = ctx.lease.election
        fence = ThreadFence(
            ctx.lease, election.caller, asyncio.get_running_loop(), provider_name=election.provider_name
        )
        await self._run_pass_in_archive_thread(fence=fence)

    async def run_own_archive_pass(self) -> None:
        """Run one archive pass over this worker's own files in the archive thread, with no election."""
        if self._archive_stop.is_set():
            return
        await self._run_pass_in_archive_thread(only_worker=self._worker_name)

    async def _run_pass_in_archive_thread(
        self, *, fence: ThreadFence | None = None, only_worker: str | None = None
    ) -> None:
        """Run one archive pass in the archive thread and wait for it."""
        # Kept so stop() can wait for a pass that runs on in its thread after
        # the slot loop is cancelled, before the worker stops the election.
        # The detail is set with the future, so it always describes this pass.
        self._archive_pass_abandoned_detail = (
            ARCHIVE_PASS_ABANDONED_DETAIL if only_worker is None else ARCHIVE_OWN_PASS_ABANDONED_DETAIL
        )
        self._archive_pass_future = self._archive_thread().submit(
            functools.partial(
                run_archive_pass,
                self._shared_dir,
                cluster=self._cluster_name,
                instance=self._instance,
                archive=self._archive,
                fence=fence,
                only_worker=only_worker,
                stop=self._archive_stop,
            )
        )
        running = self._archive_pass_future
        try:
            await asyncio.wrap_future(running)
        except asyncio.CancelledError:
            # The slot loop is cancelled (shutdown) and nothing awaits the pass
            # any more, so its failure is logged when it ends.
            running.add_done_callback(functools.partial(_log_job_failure, 'archive'))
            raise

    def _observe_footprint_when_idle(self, outcome: str) -> None:
        """Refresh the archive footprint gauges in the archive thread, unless a pass or an observation runs there."""
        if self._archive_stop.is_set() or _is_running(self._archive_pass_future) or _is_running(self._footprint_future):
            return
        self._footprint_future = self._archive_thread().submit(
            observe_archive_footprint, self._shared_dir, self._cluster_name
        )
        self._footprint_future.add_done_callback(functools.partial(_log_job_failure, 'archive_footprint'))

    def _archive_thread(self) -> ThreadPoolExecutor:
        """The recorder's one archive thread, made on first use."""
        if self._archive_executor is None:
            self._archive_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='drakkar-archive')
        return self._archive_executor

    async def _wait_for_archive_pass(self, deadline: float | None, reserve_seconds: float) -> None:
        """Wait for an archive pass still running in its thread, until only ``reserve_seconds`` of ``deadline`` are left.

        ``stop()`` has asked the pass to end after its current window; until
        then its fence still calls the election provider. A pass ends only
        between windows, so the wait is not capped at a fixed time: a fixed cap would
        abandon a window that the shutdown time could still let finish. It
        ends when the time left is down to ``reserve_seconds`` (the time the
        shutdown steps after the recorder need), or at once when less is
        left. Without a ``deadline`` it lasts until the pass ends. A pass that
        outlives the wait is left to finish on its own and logged as
        ``recorder_archive_pass_abandoned``. A pass with a lease is fenced at
        its next check, because a provider refuses every call after its
        ``stop`` (and every check fails once the loop is closed), and its lease
        runs out. A pass over this worker's own files ends after its window.
        """
        running = self._archive_pass_future
        if running is None or running.done():
            return
        time_left = _time_left(deadline)
        wait_seconds = None if time_left is None else max(time_left - reserve_seconds, 0.0)
        waiting = asyncio.wrap_future(running)
        started = time.monotonic()
        done, _ = await asyncio.wait({waiting}, timeout=wait_seconds)
        if not done:
            waiting.cancel()
            await logger.awarning(
                'recorder_archive_pass_abandoned',
                category='recorder',
                waited_seconds=round(time.monotonic() - started, 3),
                detail=self._archive_pass_abandoned_detail,
            )
            return
        # Marks the error read; the slot whose loop was cancelled logs it (see _run_archive_slot).
        if not waiting.cancelled():
            waiting.exception()

    async def _rotate(self) -> None:
        """Rotate: open new DB, initialize it fully, then swap — no schemaless window.

        Ordering matters: if we installed ``self._db = new_db`` before
        creating the schema, a concurrent file loop iteration (or
        any debug-triggered flush) could execute ``INSERT INTO events``
        against the freshly-opened but still-empty file and hit
        ``no such table: events``. The fix is to prepare ``new_db``
        completely before the atomic swap, so any concurrent writer only
        ever sees a ready connection.

        Without a file (``persistence='none'``) there is nothing to rotate: without
        this guard the new file name would resolve into the current folder.
        """
        if not self._has_file:
            return
        # Write what waits into the current file first: stage M, then
        # stage F, each under its own lock.
        await self._stage_memory(0.0)
        await self._stage_file()

        # open new DB and initialize schema BEFORE swapping. If schema
        # creation fails, close the orphaned connection and leave
        # self._db untouched so the caller can retry on the next tick.
        new_path = os.path.normpath(make_db_path(self._storage_dir, self._worker_name))
        # Same ordering contract as ``start``: owner-only before the driver
        # opens the file, so the rotated DB's sidecars are created
        # owner-only too. Skipping this step here would leave every
        # post-rotation file world-readable.
        secure_db_file(new_path)
        new_db = await open_connection(new_path)
        new_reader: aiosqlite.Connection | None = None
        swapped = False
        try:
            await new_db.execute('PRAGMA journal_mode=WAL')
            # Re-applied per connection: unlike journal_mode, synchronous is
            # not stored in the database header.
            await new_db.execute(WAL_SYNCHRONOUS_PRAGMA)
            await new_db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
            await self._create_schema(new_db)
            # Open the new reader against the new path. Done BEFORE swapping
            # so a concurrent debug-UI read arriving mid-swap never lands on
            # a half-torn-down reader.
            new_reader = await open_reader(new_path)
            # Atomic swap: once these lines execute, any concurrent flush or UI
            # read sees the new, schema-ready connections. The flush lock makes
            # a flush that already took the old writer finish before the swap,
            # so nothing writes the old file after it is sealed.
            async with self._flush_lock:
                old_db = self._db
                old_reader = self._reader_db
                old_path = self._db_path
                self._db = new_db
                self._reader_db = new_reader
                self._db_path = new_path
                # Taken in the same step as the swap: the leftover scan must
                # never pick the old file up while it is sealed here.
                self._in_hand.add(old_path)
                swapped = True
        except BaseException:
            # Any failure before the swap, a cancelled rotation (stop())
            # included, also while it waits for the flush lock, closes the new
            # connections: their threads are not daemons and would hold the
            # process open. self._db is still the previous, fully initialized
            # writer, so the worker stays usable.
            if not swapped:
                for connection in (new_reader, new_db):
                    if connection is not None:
                        await close_quietly(connection)
            raise
        try:
            # The new file needs the config row.
            await self._write_config_to_file()

            if old_reader:
                # Close the old reader before sealing: the seal needs the old
                # writer to be the last open connection. Failures here are
                # non-fatal — the new reader is already live, and a seal that
                # fails leaves the file in the backlog.
                try:
                    await old_reader.close()
                except Exception as exc:
                    await logger.awarning(
                        'recorder_old_reader_close_failed',
                        category='recorder',
                        error=str(exc),
                    )

            if old_db:
                # A seal failure never propagates: the new writer is already
                # live, and the old file waits in the backlog. The move into
                # the history folder is the background sealing job's work.
                meta = await self._seal(old_path, seal_connection(old_db, db_path=old_path, **self._seal_identity()))
                if meta is not None:
                    self._sealed[old_path] = meta
                    self._update_unsealed_gauge()
        except BaseException:
            # A rotation that ends here (cancelled, or the config write
            # failed) still closes the old connections; closing one the seal
            # already closed does nothing. The old file, unsealed, is sealed
            # later as a leftover.
            for connection in (old_reader, old_db):
                if connection is not None:
                    await close_quietly(connection)
            raise
        finally:
            self._in_hand.discard(old_path)

        await logger.ainfo('recorder_rotated', category='recorder', new_db=self._db_path)

    # --- Sealing ---

    def _seal_identity(self) -> dict[str, Any]:
        """Who seals the file and when: the meta file's identity fields."""
        return {
            'worker': self._worker_name,
            'instance': self._instance,
            'cluster': self._cluster_name,
            'sealed_at': datetime.now(UTC),
        }

    async def _seal(self, path: str, sealing: Coroutine[Any, Any, dict[str, Any]]) -> dict[str, Any] | None:
        """Await one seal; on failure the file joins the backlog and ``None`` is returned."""
        try:
            return await sealing
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._seal_failed(path, exc, 'seal')
            return None

    async def _publish(
        self,
        path: str,
        meta: dict[str, Any],
        *,
        timeout: float | None = None,
        event: str = 'recorder_sealed',
    ) -> bool:
        """Move a sealed file and its meta into the history folder; ``event`` names the success log."""
        try:
            destination, crossed = await asyncio.wait_for(
                publish_in_background(path, meta, self.history_dir), timeout=timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._seal_failed(path, exc, 'publish')
            return False
        self._forget(path)
        await logger.ainfo(
            event,
            category='recorder',
            db_path=path,
            destination=destination,
            crossed_filesystems=crossed,
            size_bytes=meta['size_bytes'],
            events=meta['tables'].get('events', 0),
        )
        return True

    async def _seal_failed(self, path: str, exc: BaseException, stage: str) -> None:
        """Count the failure and keep the file in the backlog for the next job."""
        recorder_seal_failures.inc()
        self._sealed.pop(path, None)
        if path not in self._backlog:
            self._backlog.append(path)
        self._update_unsealed_gauge()
        await logger.awarning(
            'recorder_seal_failed',
            category='recorder',
            db_path=path,
            stage=stage,
            error=str(exc),
            error_type=type(exc).__name__,
            backlog=len(self._backlog),
        )

    def _forget(self, path: str) -> None:
        """The file reached the history folder (or vanished): drop it from the waiting lists."""
        self._sealed.pop(path, None)
        if path in self._backlog:
            self._backlog.remove(path)
        self._update_unsealed_gauge()

    def _update_unsealed_gauge(self) -> None:
        recorder_unsealed_files.set(len(self._backlog) + len(self._sealed))

    def _take(self, path: str) -> bool:
        """Claim ``path`` for one seal or move; False when something else works on it."""
        if path in self._in_hand:
            return False
        self._in_hand.add(path)
        return True

    async def _seal_pending(self) -> None:
        """Move sealed files into the history folder, retry the backlog, seal leftovers.

        One file at a time. A backlog file that disappeared (an operator
        deleted it) is dropped. A leftover is a file of this worker name,
        other than the live one, that nothing touched for the leftover
        floor: a crashed earlier process, or a stop without time left to
        move the file.
        """
        if not self._has_file:
            return
        # Snapshots taken first: a file that fails in this job waits for
        # the next one instead of being retried right away.
        sealed = list(self._sealed.items())
        backlog = list(self._backlog)
        for path, meta in sealed:
            if not self._take(path):
                continue
            try:
                await self._publish(path, meta)
            finally:
                self._in_hand.discard(path)
        for path in backlog:
            if not self._take(path):
                continue
            try:
                await self._seal_again(path)
            finally:
                self._in_hand.discard(path)
        leftovers = await asyncio.to_thread(
            find_leftovers,
            self._storage_dir,
            self._worker_name,
            exclude=self._db_path,
            now=time.time(),
            floor_seconds=self._leftover_floor_seconds,
        )
        for path in leftovers:
            if path in self._backlog or path in self._sealed or not self._take(path):
                continue
            try:
                meta = await self._seal(path, seal_closed_file(path, **self._seal_identity()))
                if meta is not None:
                    await self._publish(path, meta, event='recorder_leftover_sealed')
            finally:
                self._in_hand.discard(path)
        await self._seal_orphans()

    # --- Orphan folders ---

    async def _seal_orphans(self) -> None:
        """Look at the own folders of the other worker names of this cluster; seal those whose name is dead.

        One folder at a time, each under its owner lock, so no other
        process can start in it while it is sealed. See
        :mod:`drakkar.recorder.orphans` for the proof that a name is dead.
        """
        if not self._sibling_dir:
            return
        folders = await asyncio.to_thread(sibling_folders, self._sibling_dir, self._worker_name)
        for folder, worker in folders:
            lock = await asyncio.to_thread(try_sibling_lock, folder)
            if lock is None:
                continue
            try:
                await self._handle_orphan(folder, worker)
            finally:
                await asyncio.to_thread(lock.release)

    async def _handle_orphan(self, folder: str, worker: str) -> None:
        """Seal the folder of ``worker`` when it is idle and the registry says the name is not online."""
        idle = await asyncio.to_thread(
            folder_is_idle, folder, now=time.time(), floor_seconds=self._leftover_floor_seconds
        )
        if not idle:
            return
        if self._worker_online is None:
            await self._report_orphan(folder, worker)
            return
        if self._worker_online(worker) is not False:
            return
        files = 0
        for path in await asyncio.to_thread(own_db_files, folder, worker):
            if not await self._seal_orphan_file(path, worker):
                return
            files += 1
        removed = await asyncio.to_thread(remove_orphan_files, folder, worker)
        self._reported_orphans.discard(folder)
        await logger.ainfo(
            'recorder_orphan_folder_sealed',
            category='recorder',
            folder=folder,
            worker=worker,
            files=files,
            folder_removed=removed,
        )

    async def _report_orphan(self, folder: str, worker: str) -> None:
        """Log an orphan folder once per process: with no registry, no name can be proved dead."""
        if folder in self._reported_orphans:
            return
        self._reported_orphans.add(folder)
        await logger.awarning(
            'recorder_orphan_folder_found',
            category='recorder',
            folder=folder,
            worker=worker,
            hint=(
                'no process holds this folder and its files are idle, but with autodiscover off '
                'the worker cannot prove the name is dead; seal or delete the folder by hand'
            ),
        )

    async def _seal_orphan_file(self, path: str, worker: str) -> bool:
        """Seal one recorder file of an orphan folder under its owner's name and publish it; False on failure.

        The meta file names ``worker``, the owner from the folder and file
        name, not this worker. A failure leaves the file in the orphan folder
        for a later pass; it never joins this worker's own backlog.
        """
        stage = 'seal'
        try:
            meta = await seal_closed_file(
                path, worker=worker, instance=self._instance, cluster=self._cluster_name, sealed_at=datetime.now(UTC)
            )
            stage = 'publish'
            destination, crossed = await publish_in_background(path, meta, self.history_dir)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            recorder_seal_failures.inc()
            await logger.awarning(
                'recorder_seal_failed',
                category='recorder',
                db_path=path,
                stage=stage,
                error=str(exc),
                error_type=type(exc).__name__,
                backlog=len(self._backlog),
                orphan_worker=worker,
            )
            return False
        await logger.ainfo(
            'recorder_leftover_sealed',
            category='recorder',
            db_path=path,
            destination=destination,
            crossed_filesystems=crossed,
            size_bytes=meta['size_bytes'],
            events=meta['tables'].get('events', 0),
            orphan_worker=worker,
        )
        return True

    async def _seal_again(self, path: str) -> None:
        """Seal and move one backlog file; drop it when it is gone."""
        if not await asyncio.to_thread(os.path.exists, path):
            self._forget(path)
            return
        meta = await self._seal(path, seal_closed_file(path, **self._seal_identity()))
        if meta is not None:
            await self._publish(path, meta)

    def _start_sealing(self) -> None:
        """Start the background sealing job unless one still runs (it takes new files next time)."""
        if not self._has_file or (self._sealing_task is not None and not self._sealing_task.done()):
            return
        self._sealing_task = asyncio.create_task(self._run_job('sealing', self._seal_pending()))

    async def _run_job(self, job: str, work: Coroutine[Any, Any, None]) -> None:
        """Run one background job; a failure is logged and the next tick runs the job again."""
        try:
            await work
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await logger.aerror(
                'recorder_background_job_failed',
                category='recorder',
                job=job,
                error=str(exc),
                error_type=type(exc).__name__,
                exc_info=exc,
            )

    def start_leftover_sealing(self) -> None:
        """Seal files an earlier process left behind, in the background.

        Called once the worker runs, so startup never waits on it. It looks
        twice: right away, and again once the leftover floor has passed, so
        a file from a quick restart is sealed within minutes rather than
        at the next rotation.
        """
        if not self._has_file or self._leftover_task is not None:
            return
        self._leftover_task = asyncio.create_task(self._run_job('leftover_sealing', self._leftover_sealing()))

    async def _leftover_sealing(self) -> None:
        await self._seal_pending()
        await asyncio.sleep(self._leftover_floor_seconds)
        await self._seal_pending()
