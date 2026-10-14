"""``CacheEngine`` — owns SQLite persistence + peer-sync for one worker's cache.

Two connections: a **writer** for flush/cleanup/sync-UPSERT, and a **reader**
for ``Cache.get()`` DB fallback. aiosqlite spawns a dedicated OS thread per
connection, so SQLite operations run off the event loop; WAL mode allows the
reader to see consistent snapshots while the writer commits.

The engine is constructed by the app during startup; ``start()`` opens the
DB and prepares the periodic loops (flush, cleanup, peer-sync). With an empty
``storage.dir`` (memory-only mode), ``start()`` logs a warning and
puts the engine into an *effectively disabled* state — no connection is
opened, no file is created, and subsequent flush/sync/cleanup calls become
no-ops. The handler still sees a ``Cache`` instance; it just never persists.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite
import httpx
import structlog

from drakkar import metrics
from drakkar.cache.changes import EXCHANGED_SCOPES, MARK_KEY_MAX_CHARS, ChangesQuery, Cursor
from drakkar.cache.exchange import (
    CONNECT_TIMEOUT_SECONDS,
    ENDPOINT,
    PeerBackoff,
    PeerRequestError,
    PeerTarget,
    changes_url,
    fetch_page,
    keep_rows,
    page_params,
    select_targets,
)
from drakkar.cache.memory import Cache
from drakkar.cache.models import _now_ms
from drakkar.cache.sql import (
    BUSY_TIMEOUT_MS,
    CHANGES_FIELDS,
    LWW_UPSERT_SQL,
    OWN_MARK_SQL,
    SCHEMA_CACHE_ENTRIES,
    changes_query,
)
from drakkar.dbfiles import DB_DIR_MODE, WAL_SYNCHRONOUS_PRAGMA, secure_db_file

# Imported at module scope (not inside ``start()``) so that tests can
# ``monkeypatch.setattr(cache_engine_module, 'run_periodic_task', ...)`` —
# a local import would bind to the concrete function each call and defeat
# the spy.
from drakkar.periodic import run_periodic_task

if TYPE_CHECKING:
    from drakkar.config import CacheConfig, CacheExchangeConfig
    from drakkar.fleet.service import PeerView
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# Consecutive failed flush cycles before the engine escalates from the
# periodic-runner's per-failure log to a dedicated ERROR with operator
# guidance. Low enough to fire within ~half a minute at the default
# flush interval, high enough that a single transient blip stays quiet.
FLUSH_FAILURE_ESCALATION_THRESHOLD = 5


class CacheEngine:
    """Owns the SQLite backing store and periodic loops for a single worker's cache.

    Two connections: a **writer** for flush/cleanup/sync-UPSERT, and a
    **reader** for ``Cache.get()`` DB fallback. aiosqlite spawns a dedicated
    OS thread per connection, so SQLite operations run off the event loop;
    WAL mode allows the reader to see consistent snapshots while the writer
    commits.

    The engine is constructed by the app during startup; ``start()`` opens
    the DB and prepares the periodic loops. With an empty ``storage.dir``
    (memory-only mode), ``start()`` logs a warning and puts the engine
    into an *effectively disabled* state — no connection is opened, no file
    is created, and subsequent flush/sync/cleanup calls become no-ops. The
    handler still sees a ``Cache`` instance; it just never persists.
    """

    def __init__(
        self,
        config: CacheConfig,
        exchange: CacheExchangeConfig,
        storage_dir: str,
        worker_id: str,
        cluster_name: str,
        recorder: EventRecorder | None = None,
        peer_views: Callable[[], list[PeerView]] | None = None,
        peer_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Wire up an engine without opening any resources.

        Args:
            config: cache-specific settings (enabled flag, intervals,
                memory cap).
            exchange: ``fleet.cache_exchange`` — whether and how often
                ``cluster``/``global`` rows are pulled from peers.
            storage_dir: the worker's own folder (``storage.dir``); the
                cache file lives here. Empty means memory-only: the engine
                warns at start and stays disabled.
            worker_id: stable identifier for this worker — used for the
                DB filename, the own-origin filter of the rows this worker
                serves, and the ``origin_worker_id`` column on every write.
            cluster_name: used by the cache exchange to decide whether to
                pull ``cluster``-scoped rows from a given peer. Stored
                here on construction so the info is available without
                re-reading config.
            recorder: optional event recorder the periodic loops feed
                ``periodic_run`` events into. Kept None in tests that
                only care about lifecycle / schema, so the flush path
                can avoid requiring the recorder's presence.
            peer_views: the worker registry's peers (cards with each
                peer's URL, cluster, state and ``features.cache`` marks),
                read every sync cycle. ``None`` (no registry) means no peer
                is ever pulled.
            peer_transport: the HTTP transport for peer requests; ``None``
                uses httpx's own. A seam for tests (``httpx.MockTransport``).
        """
        self._config = config
        self._exchange = exchange
        self._storage_dir = storage_dir
        self._worker_id = worker_id
        self._cluster_name = cluster_name
        self._recorder = recorder
        self._peer_views = peer_views
        self._peer_transport = peer_transport
        # The HTTP client for peer requests: made at the first sync cycle
        # that has a peer to pull, closed in ``stop()``.
        self._http: httpx.AsyncClient | None = None
        self._peer_backoff = PeerBackoff()
        # Writer connection. Opened in ``start()``, closed in ``stop()``.
        # Nil when the engine is disabled (config.enabled=False or an
        # empty storage_dir) or has not been started yet.
        self._writer_db: aiosqlite.Connection | None = None
        # Reader connection. Second aiosqlite connection to the same DB
        # file, opened in read-only mode. Used exclusively by ``Cache.get``
        # for DB fallback. aiosqlite spawns one worker thread per
        # connection, so SELECTs run off the event loop and never queue
        # behind a flush/cleanup/sync commit on the writer's thread. WAL
        # mode lets the reader see a consistent snapshot while the writer
        # commits.
        self._reader_db: aiosqlite.Connection | None = None
        # Resolved DB file path, set by ``start()``.
        self._db_path: str = ''
        # The handler-facing Cache that feeds this engine's flush/sync
        # pipelines. Wired in via ``attach_cache`` — the app does this
        # at startup before scheduling the periodic loops. We keep the
        # relation one-to-one because a single worker has a single
        # cache-DB file, and splitting caches across engines would make
        # the ``_dirty`` swap (see ``_flush_once``) ambiguous.
        self._cache: Cache | None = None
        # Serializes every user of the writer connection: flush cycles
        # (periodic + shutdown drain), cleanup's DELETE+commit, and
        # peer-sync's UPSERT+commit. All three share ``_writer_db``, and
        # aiosqlite serializes individual *statements*, not transactions —
        # without one lock, a commit from cleanup or peer-sync could land
        # mid-flush and persist a half-applied flush batch.
        self._writer_lock = asyncio.Lock()
        # Consecutive failed flush cycles. Reset on success. Used to
        # escalate logging when the writer connection is persistently
        # broken (dirty entries accumulate in memory and would be lost
        # if the shutdown drain also fails).
        self._consecutive_flush_failures = 0
        # Task reference for the periodic flush loop. Set in ``start()``
        # after ``run_periodic_task`` is scheduled; cleared in ``stop()``
        # once the task has been cancelled and awaited. None when the
        # engine is effectively disabled (no writer connection) since
        # there's nothing to flush.
        self._flush_task: asyncio.Task | None = None
        # Task reference for the periodic cleanup loop — same lifecycle as
        # the flush task. Runs on its own (typically slower) cadence and
        # reclaims space by deleting rows whose ``expires_at_ms`` is past.
        self._cleanup_task: asyncio.Task | None = None
        # Task reference for the periodic peer-sync loop. Same lifecycle
        # as flush/cleanup; only scheduled when peer sync is effectively
        # enabled (``fleet.cache_exchange.enabled`` and a writable local
        # DB). None when disabled so there's no wakeup cost.
        self._sync_task: asyncio.Task | None = None
        # Whether peer-sync is *effectively* enabled — the config flag is
        # on and ``start()`` opened a writable local DB to apply pulled rows
        # into. ``_sync_once`` short-circuits on this flag to keep the
        # idle-engine cost at O(1).
        self._peer_sync_enabled: bool = False
        # Per-peer cursors: the (updated_at_ms, key) of the last row pulled
        # from each peer; the next page asks for rows after it. In memory
        # only: a restart pulls everything once more, which LWW makes
        # harmless.
        self._peer_cursors: dict[str, Cursor] = {}
        # Shielded peer-commit tasks still running. Pulls of several peers
        # run in parallel, and the sync-cycle deadline can cancel any of
        # their awaiters while the shielded commit keeps running; every
        # such task stays here so the next sync cycle and ``stop()`` await
        # them (bounded) before touching or closing the writer connection.
        self._pending_peer_commits: set[asyncio.Task] = set()
        # High-water marks of the rows this worker originated, per exchanged
        # scope: the newest (updated_at_ms, key). Read from the file at
        # start, raised after every committed flush; the worker card carries
        # them so a peer pulls only when something is new. In memory only.
        self._marks: dict[str, Cursor] = {}

    @property
    def serves_changes(self) -> bool:
        """True while peers may pull from this worker: the exchange is on and the engine runs."""
        return self._peer_sync_enabled and self._reader_db is not None

    def features(self) -> dict[str, Any]:
        """The worker card's ``features``: ``{"cache": {<scope>: {"ms", "key"}}}`` while serving, else ``{}``.

        A scope with no own rows is left out, so ``{"cache": {}}`` means
        "the exchange is on, nothing to pull yet". A mark's key is cut to
        ``MARK_KEY_MAX_CHARS`` characters so the card stays small.
        """
        if not self.serves_changes:
            return {}
        return {
            'cache': {
                scope: {'ms': ms, 'key': key[:MARK_KEY_MAX_CHARS]} for scope, (ms, key) in sorted(self._marks.items())
            }
        }

    async def changes(self, query: ChangesQuery, now_ms: int) -> dict[str, Any] | None:
        """One page of own-origin rows for a peer, or ``None`` when this worker does not serve.

        Reads through the engine's reader connection. ``now_ms`` is the
        server clock that decides expiry. One row past ``limit`` is fetched
        to tell whether more remain.
        """
        reader = self._reader_db
        if not self.serves_changes or reader is None:
            return None
        sql, params = changes_query(
            origin=self._worker_id, scopes=query.scopes, now_ms=now_ms, after=query.after, limit=query.limit + 1
        )
        async with reader.execute(sql, params) as cursor:
            fetched = list(await cursor.fetchall())
        rows = [dict(zip(CHANGES_FIELDS, row, strict=True)) for row in fetched[: query.limit]]
        return {'rows': rows, 'more': len(fetched) > query.limit}

    async def _load_marks(self) -> None:
        """Initialise the high-water marks from the rows already in the file."""
        if self._writer_db is None:
            return
        for scope in EXCHANGED_SCOPES:
            async with self._writer_db.execute(OWN_MARK_SQL, (self._worker_id, scope)) as cursor:
                row = await cursor.fetchone()
            if row is not None:
                self._marks[scope] = (row[0], row[1])

    def _raise_marks(self, upsert_rows: list[tuple]) -> None:
        """Raise the marks from rows this worker just committed.

        A row is a ``LWW_UPSERT_SQL`` parameter tuple: 0 key, 1 scope,
        5 updated_at_ms, 7 origin_worker_id. A row the upsert rejected, or
        one deleted or expired later, can leave a mark that no served row
        matches; a peer then pulls an empty page once and moves its cursor
        up to the mark.
        """
        for row in upsert_rows:
            scope = row[1]
            if scope not in EXCHANGED_SCOPES or row[7] != self._worker_id:
                continue
            candidate = (row[5], row[0])
            current = self._marks.get(scope)
            if current is None or candidate > current:
                self._marks[scope] = candidate

    @property
    def reader_db(self) -> aiosqlite.Connection | None:
        """Public accessor for the reader aiosqlite connection.

        Exposes the read-only connection so external code (notably the
        debug server's cache endpoints) doesn't need to reach into
        ``_reader_db`` directly. Returns ``None`` when the engine is
        disabled or has not been started (no DB is present to read).

        The connection is shared with ``Cache.get``'s DB fallback — we
        don't open a third connection for the UI, since adding a
        fourth aiosqlite worker thread per worker would cost more than
        it saves on read-only queries.
        """
        return self._reader_db

    @staticmethod
    def _cache_db_file_path(db_dir: str, worker_id: str) -> str:
        """Return the cache file path: ``<storage.dir>/<worker>-cache.db``, a plain private file."""
        return str(Path(db_dir) / f'{worker_id}-cache.db')

    def attach_cache(self, cache: Cache) -> None:
        """Associate the handler-facing Cache instance with this engine.

        The engine owns the SQLite side; the Cache owns the in-memory
        side. ``_flush_once`` reads from ``cache._dirty`` (atomic swap)
        and writes to ``_writer_db``. App startup wires these together
        before scheduling the flush periodic.

        Idempotent in the sense that passing the same cache twice is
        a no-op; passing a different cache replaces the binding — tests
        sometimes re-attach a fresh cache after reconfiguring.

        If the engine is already started (reader connection open), we
        also wire the reader through so a late-attached Cache can still
        fall through to the DB on ``get()`` misses.
        """
        self._cache = cache
        if self._reader_db is not None:
            cache.attach_reader_db(self._reader_db)

    def _resolve_db_dir(self) -> str:
        """Return the folder the cache DB lives in (``storage.dir``), or ``''`` for memory-only."""
        return self._storage_dir

    async def _create_schema(self) -> None:
        """Apply the cache schema to the writer connection.

        Uses ``executescript`` so the full DDL block — table + both
        indexes — lands in one round trip. ``CREATE TABLE IF NOT EXISTS``
        makes the operation a no-op against an existing cache DB, so a
        restarted worker picks up the previous rows on the next call to
        ``get()`` (reader connection) or flush.
        """
        if self._writer_db is None:
            raise RuntimeError('writer DB not open')
        await self._writer_db.executescript(SCHEMA_CACHE_ENTRIES)
        # WAL mode must be set per-connection; applied here so the writer
        # is in WAL before any writes. Subsequent reader connections
        # and peer-opened ephemeral connections will see WAL files
        # already on disk.
        await self._writer_db.execute('PRAGMA journal_mode = WAL')
        # One fsync per checkpoint instead of one per commit — see
        # WAL_SYNCHRONOUS_PRAGMA for the durability trade.
        await self._writer_db.execute(WAL_SYNCHRONOUS_PRAGMA)
        # Explicit busy_timeout so lock contention between the engine's own
        # writer and reader behaves the same regardless of the driver's
        # own default.
        await self._writer_db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
        await self._writer_db.commit()

    async def start(self) -> None:
        """Open the writer connection and apply the schema.

        Idempotent: calling ``start()`` on an already-started engine is
        a no-op (the second call would otherwise re-run schema DDL, which
        is safe but wasteful). Returns without opening resources when the
        cache is disabled or ``storage.dir`` is empty — the engine stays
        in effectively-disabled mode with ``_writer_db = None``.
        """
        # already started — keep the existing connection
        if self._writer_db is not None:
            return
        # disabled by config — nothing to do
        if not self._config.enabled:
            return

        db_dir = self._resolve_db_dir()
        if not db_dir:
            # Warn-and-continue (not fail-at-startup): the cache is an
            # optional accelerator, so memory-only mode degrades to
            # no-persistence rather than blocking the worker. The handler
            # still gets a Cache instance; it just won't persist.
            # Peer-sync is automatically disabled because it needs the
            # writer connection to apply UPSERTs.
            await logger.awarning(
                'cache_engine_disabled_no_storage_dir',
                category='cache',
                worker_id=self._worker_id,
                reason='storage.dir is empty (memory-only mode), so there is no cache file',
            )
            return

        # Owner-only when we create it: the cache DB inside holds handler
        # results derived from message payloads. Mode applies to the leaf
        # directory only (Python passes it to the final mkdir; any
        # intermediate parents keep the default 0777 & ~umask), which is
        # the one that actually contains the files.
        os.makedirs(db_dir, mode=DB_DIR_MODE, exist_ok=True)
        self._db_path = self._cache_db_file_path(db_dir, self._worker_id)
        # Owner-only before the driver opens the file and ``_create_schema``
        # turns on WAL, so the -wal/-shm sidecars inherit 0600 as SQLite
        # creates them. The cache DB holds handler results derived from
        # message payloads. See :func:`drakkar.dbfiles.secure_db_file`.
        secure_db_file(self._db_path)
        self._writer_db = await aiosqlite.connect(self._db_path)
        await self._create_schema()
        await self._load_marks()

        # Open the reader connection on a read-only URI. ``file:…?mode=ro``
        # tells SQLite to reject any write attempt, and aiosqlite spawns a
        # fresh worker thread for this connection — so DB-fallback SELECTs
        # from ``Cache.get`` never queue behind a flush/cleanup/sync commit
        # on the writer's thread. WAL mode (set on the writer above) lets
        # the reader see consistent snapshots while the writer commits.
        reader_uri = f'file:{self._db_path}?mode=ro'
        self._reader_db = await aiosqlite.connect(reader_uri, uri=True)
        # Same explicit busy_timeout as the writer.
        await self._reader_db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
        # If a Cache has already been attached (typical — engine.attach_cache
        # happens before start()), wire the reader through now; if it gets
        # attached later, the caller is responsible for calling
        # ``attach_reader_db`` themselves.
        if self._cache is not None:
            self._cache.attach_reader_db(self._reader_db)

        # Peer-sync runs whenever ``fleet.cache_exchange.enabled`` is on: the
        # writer connection it applies pulled rows into is open by now.
        # Logged once at startup so the effective state is visible without
        # spamming the sync loop.
        if not self._exchange.enabled:
            self._peer_sync_enabled = False
            await logger.ainfo(
                'cache_peer_sync_disabled_by_config',
                category='cache',
                worker_id=self._worker_id,
                reason='fleet.cache_exchange.enabled=false in config',
            )
        else:
            self._peer_sync_enabled = True

        # Schedule the periodic flush loop. It runs as a framework-system
        # task (``system=True``) so the debug UI can render a [system] badge
        # alongside user-defined ``@periodic`` handlers, and errors use the
        # "continue" policy — a bad flush cycle must not kill the worker, it
        # just logs and retries next tick. The recorder (if present) logs
        # each run to the event timeline so operators see cache health.
        self._flush_task = asyncio.create_task(
            run_periodic_task(
                name='cache.flush',
                coro_fn=self._flush_once,
                seconds=self._config.flush_interval_seconds,
                on_error='continue',
                recorder=self._recorder,
                system=True,
            ),
            name='cache.flush',
        )

        # Schedule the periodic cleanup loop. Same system/error policy as
        # flush — a bad cleanup cycle logs and retries; it never kills the
        # worker. Runs on its own (typically slower) cadence since row
        # expiration is a background reclaim job, not on the write path.
        self._cleanup_task = asyncio.create_task(
            run_periodic_task(
                name='cache.cleanup',
                coro_fn=self._cleanup_once,
                seconds=self._config.cleanup_interval_seconds,
                on_error='continue',
                recorder=self._recorder,
                system=True,
            ),
            name='cache.cleanup',
        )

        # Schedule the periodic peer-sync loop — but only when peer sync
        # is effectively enabled. When disabled we skip registration
        # entirely so there's zero wakeup cost on workers that don't
        # participate in cross-worker cache sharing.
        if self._peer_sync_enabled:
            self._sync_task = asyncio.create_task(
                run_periodic_task(
                    name='cache.sync',
                    coro_fn=self._sync_once,
                    seconds=self._exchange.interval_seconds,
                    on_error='continue',
                    recorder=self._recorder,
                    system=True,
                ),
                name='cache.sync',
            )

        await logger.ainfo(
            'cache_engine_started',
            category='cache',
            worker_id=self._worker_id,
            db_path=self._db_path,
        )

    async def stop(self) -> None:
        """Cancel periodic tasks, drain remaining dirty ops, close both DB
        connections and the peer HTTP client.

        Ordering inside this method is deliberate:

        1. **Cancel the flush task** so it doesn't race with the final
           drain — otherwise the periodic loop might hit the writer DB
           concurrently with our explicit ``_flush_once`` and interleave.
        2. **Await the cancelled task** — ``run_periodic_task`` re-raises
           ``CancelledError``; ``return_exceptions=True`` on a single task
           would be overkill, so we just ``except CancelledError``.
        3. **Final drain** via ``_flush_once`` — this is what prevents the
           "writes lost on shutdown" window. Any entry sitting in
           ``cache._dirty`` between the last scheduled flush and stop()
           reaches the DB here.
        4. **Close the writer connection** only after the drain commits,
           so the drain's UPSERT/DELETE sees an open connection.
        5. **Detach the reader from the Cache then close it** — future
           ``Cache.get`` calls short-circuit rather than hitting a closed
           connection.
        6. **Close the peer HTTP client** — no sync cycle runs any more.

        Safe to call when ``start()`` never ran or when the engine was in
        disabled mode (no connection opened): every step short-circuits on
        None checks. Matches the recorder's stop() contract so app
        shutdown can call both unconditionally.
        """
        # Step 1 + 2: stop the periodic sync + cleanup + flush tasks cleanly.
        # Cancel sync and cleanup first — they're side-effect-free with
        # respect to the dirty map. Flush's final drain below needs the
        # writer connection open, so we save flush for last.
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                # Expected — run_periodic_task re-raises on cancel.
                pass
            self._flush_task = None

        # A peer-commit task orphaned by a cancelled sync cycle may still
        # be running — await it (bounded) before the final drain and the
        # writer close, so we never close ``_writer_db`` mid-executemany.
        await self._await_pending_peer_commits(timeout=self._exchange.timeout_seconds)

        # Step 3: final drain — catches any dirty ops that accumulated
        # after the last scheduled flush fired, so shutdown isn't a
        # write-loss window.
        if self._writer_db is not None:
            try:
                await self._flush_once()
            except Exception:
                # A final drain that fails is logged but must not prevent
                # stop() from proceeding — otherwise a faulty DB would
                # deadlock shutdown. The loop's continue-on-error policy
                # has already written any error we could report.
                await logger.aexception(
                    'cache_final_drain_failed',
                    category='cache',
                    worker_id=self._worker_id,
                )

        # Step 4: close writer.
        if self._writer_db is not None:
            await self._writer_db.close()
            self._writer_db = None

        # Step 5: close the reader. Detach from the Cache first so a
        # post-stop Cache.get() call returns None instead of crashing on
        # a closed connection.
        if self._reader_db is not None:
            if self._cache is not None:
                self._cache.attach_reader_db(None)
            await self._reader_db.close()
            self._reader_db = None

        # Step 6: close the HTTP client of the peer pulls.
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        await logger.ainfo(
            'cache_engine_stopped',
            category='cache',
            worker_id=self._worker_id,
        )

    # ---- flush loop ---------------------------------------------------------

    async def _flush_once(self) -> None:
        """Run one serialized flush cycle with failure-streak tracking.

        Wraps :meth:`_flush_locked` in ``_writer_lock`` — the lock shared
        by every writer-connection user (flush, cleanup, peer-sync
        commit) — so no other path can commit mid-flush and no two flush
        cycles can interleave a dirty-map swap. Tracks
        consecutive failures and escalates to ERROR once the streak
        reaches ``FLUSH_FAILURE_ESCALATION_THRESHOLD`` — a persistently
        broken writer means dirty entries accumulate in memory and are
        lost if the shutdown drain also fails.
        """
        async with self._writer_lock:
            try:
                await self._flush_locked()
            except Exception as e:
                self._consecutive_flush_failures += 1
                metrics.cache_flush_failures.inc()
                if self._consecutive_flush_failures >= FLUSH_FAILURE_ESCALATION_THRESHOLD:
                    await logger.aerror(
                        'cache_flush_failing_repeatedly',
                        category='cache',
                        consecutive_failures=self._consecutive_flush_failures,
                        error=str(e),
                        error_type=type(e).__name__,
                        hint='cache writes are accumulating in memory and will be '
                        'lost on shutdown if the final drain also fails — '
                        'investigate the cache DB / disk',
                    )
                raise
            else:
                self._consecutive_flush_failures = 0

    async def _flush_locked(self) -> None:
        """Drain one batch of pending mutations from the Cache's dirty map
        to the local SQLite DB.

        A single flush cycle does the following:

        1. **Atomic swap** — take a snapshot of the current ``_dirty``
           map and replace it with a fresh empty dict via
           ``Cache.swap_dirty()``, which performs the swap under the
           Cache's ``threading.Lock`` (``handler.offload()`` threads
           mutate ``_dirty`` concurrently with the loop). Any
           ``Cache.set`` / ``Cache.delete`` landing around the swap
           either wrote to the now-snapshotted dict (flushed this cycle)
           or to the fresh empty dict (picked up next cycle). No writes
           lost.

        2. **Split by op type** — ``Op.SET`` entries go through
           ``LWW_UPSERT_SQL`` as an ``executemany``; ``Op.DELETE`` keys
           go through ``DELETE FROM cache_entries WHERE key = ?`` as a
           separate ``executemany``. Splitting reduces SQLite round trips
           to at most two regardless of batch size — one per op type.

        3. **Single transaction** — all ops land between two ``commit()``
           boundaries so a crash mid-flush leaves the DB in a consistent
           prior-to-this-cycle state; we never half-apply a batch.

        4. **Metric ticks** — ``drakkar_cache_flush_entries_total{op=...}``
           counter advances by the number of op intents drained, not by
           the number of rows actually modified. A SET that loses LWW to
           a newer existing row still ticks the counter (the flush did
           real work; the rejection is a legitimate outcome, not a skip).

        No-op fast paths:

        - Engine not started / disabled (``_writer_db is None``) → return
          immediately; leave ``_dirty`` untouched.
        - No cache attached (``_cache is None``) → return; the engine was
          constructed but never wired to a Cache (tests of pure lifecycle).
        - Empty snapshot after swap → return before any SQL is issued.
          This keeps the idle-engine cost at O(1).
        """
        if self._writer_db is None or self._cache is None:
            return

        # Atomic swap via the Cache's own helper. Keeps the "reach into
        # ``_dirty`` directly" pattern encapsulated on ``Cache`` — the
        # engine doesn't own that mutation detail.
        snapshot = self._cache.swap_dirty()

        if not snapshot:
            return

        # Partition ops. We keep the split simple: two lists, one for
        # UPSERTs, one for DELETEs. Order inside each list follows the
        # snapshot's dict-iteration order, which is insertion order under
        # CPython — so a SET followed by a DELETE on the same key would
        # collapse (DELETE overrides in the dirty map itself), and
        # overwrites are already LWW-resolved at the SQL level below.
        upsert_rows: list[tuple] = []
        delete_keys: list[tuple[str]] = []
        # Local import to avoid a cycle: cache_engine ↔ cache_memory both
        # import each other at module load otherwise. Op is in cache_models
        # which has no dependencies on either, so this is safe.
        from drakkar.cache.models import Op

        for key, op in snapshot.items():
            if op.op is Op.SET:
                entry = op.entry
                if entry is None:
                    # Structural invariant: ``Op.SET`` DirtyOps are constructed
                    # with the freshly-encoded ``CacheEntry`` alongside — an
                    # ``entry is None`` here would indicate a construction bug
                    # upstream, not user input. We use a hard runtime check
                    # so production builds (including ``python -O`` which
                    # strips asserts) still fail loudly.
                    raise RuntimeError(f"Op.SET DirtyOp for key '{key}' missing CacheEntry payload")
                upsert_rows.append(
                    (
                        entry.key,
                        str(entry.scope),  # StrEnum → raw 'local'/'cluster'/'global'
                        entry.value,
                        entry.size_bytes,
                        entry.created_at_ms,
                        entry.updated_at_ms,
                        entry.expires_at_ms,
                        entry.origin_worker_id,
                    )
                )
            else:  # Op.DELETE
                delete_keys.append((key,))

        # Execute under a single transaction. aiosqlite's default isolation
        # starts an implicit transaction on the first DML statement, and
        # ``commit()`` closes it — so the two ``executemany`` calls land
        # together.
        #
        # Cancel-safety: if this coroutine is cancelled (or raises) between
        # the atomic swap above and a successful ``commit()``, the snapshot
        # local would otherwise unwind with the frame and its ops would be
        # lost. The stop() path's final-drain ``_flush_once()`` call would
        # see an already-empty ``_dirty`` and persist nothing.
        #
        # To keep the "nothing lost" guarantee under cancellation, we track
        # whether the commit completed. On any failure path (including
        # CancelledError) we merge the un-committed snapshot back into
        # ``_cache._dirty`` so the next flush — scheduled or the shutdown
        # drain — picks them up. When merging, any key that has a newer op
        # in the current ``_dirty`` (a racing ``set`` landed post-swap) wins
        # over the snapshot entry; otherwise we restore the snapshot op.
        committed = False
        try:
            if upsert_rows:
                await self._writer_db.executemany(LWW_UPSERT_SQL, upsert_rows)
            if delete_keys:
                await self._writer_db.executemany(
                    'DELETE FROM cache_entries WHERE key = ?',
                    delete_keys,
                )
            await self._writer_db.commit()
            committed = True
        finally:
            if not committed:
                # Restore un-committed ops through Cache's helper — the
                # merge logic (skip keys with newer live ops) lives there
                # too so the engine doesn't have to know about race
                # semantics on ``_dirty``. Sync call first so it runs even
                # if the rollback await below is itself cancelled.
                self._cache.restore_dirty(snapshot)
                # Roll back the open transaction. Without this, the
                # partial batch (e.g. a successful UPSERT executemany
                # followed by a failed DELETE executemany) would sit in
                # an open transaction and be silently committed by the
                # next unrelated commit on the shared writer connection.
                with contextlib.suppress(Exception):
                    await self._writer_db.rollback()

        self._raise_marks(upsert_rows)
        # Metric ticks — one label per op type. Incremented even for
        # LWW-rejected UPSERTs (see docstring point 4): the flush did
        # real work. Only ticked on a successful commit — a rolled-back
        # flush did not "do work" from the DB's perspective.
        if upsert_rows:
            metrics.cache_flush_entries.labels(op='set').inc(len(upsert_rows))
        if delete_keys:
            metrics.cache_flush_entries.labels(op='delete').inc(len(delete_keys))

    # ---- cleanup loop -------------------------------------------------------

    async def _cleanup_once(self) -> None:
        """Reclaim space by removing expired entries from the DB, memory,
        and the dirty map — and refresh DB-size gauges.

        Expiration is wall-clock relative: a row's ``expires_at_ms`` is
        compared to the current wall-clock milliseconds. Rows with
        ``expires_at_ms IS NULL`` (no TTL) are never cleaned up; rows with
        a future TTL are preserved; rows with a past TTL are removed.

        Four jobs per cycle:

        1. **DB purge.** ``DELETE FROM cache_entries WHERE expires_at_ms IS
           NOT NULL AND expires_at_ms <= ?`` — one round trip, one commit.
           The partial index ``idx_cache_expires`` (only indexes non-null
           ``expires_at_ms``) makes this a focused scan rather than a full
           table sweep.

        2. **Memory sweep.** Walk ``Cache._memory`` for entries whose
           ``expires_at_ms`` is past, and pop them. Without this, expired
           keys would linger in memory until ``peek``/``get``/``__contains__``
           opportunistically evicts them — cleanup gives us a deterministic
           upper bound on staleness.

        3. **Dirty-map pruning** for expired keys with a pending ``Op.SET``.
           Leaving a pending SET would resurrect a row we just deleted on
           the next flush cycle. We explicitly drop ``Op.SET`` entries for
           expired keys — but leave ``Op.DELETE`` entries alone, since a
           pending DELETE on an expired key is still semantically correct
           (the row is going away either way) and simpler to leave through.

        4. **DB-gauge refresh.** ``drakkar_cache_entries_in_db`` and
           ``drakkar_cache_bytes_in_db`` are updated from a single SELECT.
           Counting DB rows on every ``set``/``get`` would defeat the
           running-sum design used for the in-memory gauges; refreshing at
           cleanup cadence (default 60s) gives operators an approximate but
           bounded-staleness DB view.

        No-op fast paths:

        - Engine not started / disabled (``_writer_db is None``) → return
          immediately without touching anything.
        - No cache attached (``_cache is None``) → DB purge still runs, but
          the memory/dirty-map steps short-circuit. This supports
          lifecycle tests that only want to exercise DB behavior.
        """
        if self._writer_db is None:
            return

        now_ms = _now_ms()

        # Step 1: DB purge. Count the rows first via ``rowcount`` on the
        # cursor so we can increment the ``cleanup_removed`` counter by
        # the real number of rows affected. Uses ``<= ?`` (inclusive of the
        # boundary ms) for consistency with ``Cache._is_expired`` and
        # ``_expire_purge`` — an entry whose ``expires_at_ms`` equals
        # ``now_ms`` is expired and gets cleaned up here too.
        #
        # ``_writer_lock`` serializes this DELETE+commit against flush and
        # peer-sync on the shared writer connection — without it, this
        # commit could land mid-flush and persist a half-applied batch.
        async with self._writer_lock:
            try:
                async with self._writer_db.execute(
                    'DELETE FROM cache_entries WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?',
                    (now_ms,),
                ) as cursor:
                    removed = cursor.rowcount if cursor.rowcount is not None else 0
                await self._writer_db.commit()
            except BaseException:
                # Roll back the open transaction so a failed purge can't be
                # silently committed by the next writer commit.
                with contextlib.suppress(Exception):
                    await self._writer_db.rollback()
                raise

        # Step 2 + 3: memory sweep + dirty-map pruning for expired SETs.
        # Delegate to ``Cache._expire_purge`` — the helper handles both
        # steps atomically and refreshes the memory gauges once after
        # the sweep. Moving this logic onto Cache keeps the memory /
        # bytes-sum / dirty-map triad encapsulated behind the Cache class
        # rather than reaching across the layer from CacheEngine.
        if self._cache is not None:
            self._cache._expire_purge(now_ms)

        # Step 4: refresh DB-size gauges with a single aggregate query. We
        # use ``coalesce(sum(size_bytes), 0)`` so an empty table yields 0
        # rather than NULL. Read-only, but it runs on the writer connection,
        # so take the writer lock to stay out of other paths' transactions.
        async with (
            self._writer_lock,
            self._writer_db.execute('SELECT count(*), coalesce(sum(size_bytes), 0) FROM cache_entries') as cursor,
        ):
            row = await cursor.fetchone()
        if row is not None:
            db_count, db_bytes = row
            metrics.cache_entries_in_db.set(db_count)
            metrics.cache_bytes_in_db.set(db_bytes)

        if removed > 0:
            metrics.cache_cleanup_removed.inc(removed)

    # ---- peer sync ----------------------------------------------------------

    def _http_client(self) -> httpx.AsyncClient:
        """The client for peer requests, made on first use.

        Connect timeout 2 s, request timeout ``timeout_seconds``. Proxy
        environment variables are ignored: peers are workers of the same
        fleet, reached at the URL on their card.
        """
        if self._http is None:
            self._http = httpx.AsyncClient(
                transport=self._peer_transport,
                timeout=httpx.Timeout(self._exchange.timeout_seconds, connect=CONNECT_TIMEOUT_SECONDS),
                follow_redirects=False,
                trust_env=False,
            )
        return self._http

    async def _pull_peer(self, client: httpx.AsyncClient, target: PeerTarget) -> int:
        """Pull ``target`` page by page until ``more`` is false; returns how many rows were applied.

        Each page is applied in one transaction (the LWW upsert), then the
        cursor moves to the page's last row. After a complete pull the
        cursor moves up to the card's mark too (see :class:`PeerTarget`).
        A failed request backs the peer off; a failure applying rows here
        is local and does not.
        """
        # A count, not the rows: a catching-up pull pages through every row
        # of the peer in one cycle, and nothing needs them after the apply.
        applied = 0
        rejected: dict[str, int] = {}
        url = changes_url(target.url)
        try:
            more = True
            while more:
                cursor = self._peer_cursors.get(target.name)
                params = page_params(target.scopes, cursor, self._exchange.page_size)
                rows, more = await fetch_page(client, url, params, timeout=self._exchange.timeout_seconds, after=cursor)
                kept, dropped = keep_rows(rows, target)
                for reason, count in dropped.items():
                    rejected[reason] = rejected.get(reason, 0) + count
                await self._apply_peer_rows(peer_name=target.name, rows=kept)
                applied += len(kept)
                if not rows:
                    # A peer that says "more" with an empty page is faulty;
                    # stop here and keep the cursor where it is.
                    break
                last = rows[-1]
                self._peer_cursors[target.name] = (last['updated_at_ms'], last['key'])
        except PeerRequestError as exc:
            await self._peer_failed(target, exc)
            return applied
        except Exception as exc:
            metrics.cache_sync_errors.labels(peer=target.name).inc()
            await logger.awarning('cache_peer_sync_failed', category='cache', peer=target.name, error=str(exc))
            return applied
        finally:
            if rejected:
                metrics.cache_sync_errors.labels(peer=target.name).inc(sum(rejected.values()))
                await logger.awarning('fleet_peer_rows_rejected', category='fleet', peer=target.name, rejected=rejected)
        self._peer_backoff.succeeded(target.name)
        if not more and target.mark is not None:
            cursor = self._peer_cursors.get(target.name)
            if cursor is None or target.mark > cursor:
                self._peer_cursors[target.name] = target.mark
        return applied

    async def _peer_failed(self, target: PeerTarget, exc: PeerRequestError) -> None:
        """Count a failed request, log it once per backoff episode, and back the peer off."""
        metrics.cache_sync_errors.labels(peer=target.name).inc()
        failures, skip_cycles = self._peer_backoff.failed(target.name)
        if failures == 1:
            await logger.awarning(
                'fleet_peer_request_failed',
                category='fleet',
                peer=target.name,
                url=target.url,
                endpoint=ENDPOINT,
                outcome=exc.outcome,
                error=str(exc),
            )
        await logger.ainfo(
            'fleet_peer_backoff',
            category='fleet',
            peer=target.name,
            skip_cycles=skip_cycles,
            failures=failures,
        )

    async def _await_pending_peer_commits(self, timeout: float) -> None:
        """Await (bounded) the peer-commit tasks orphaned by a cancelled cycle.

        When the sync-cycle deadline cancels an awaiter in
        ``_apply_peer_rows``, its shielded commit task keeps running.
        Called at the start of the next sync cycle and from ``stop()`` so
        the orphans finish before anything else uses — or closes — the
        writer connection. Tasks still running after ``timeout`` stay
        tracked so a later call retries; an orphan's exception is retrieved
        and logged here (its original awaiter was cancelled, so nobody else
        will).
        """
        if not self._pending_peer_commits:
            return
        running = [task for task in self._pending_peer_commits if not task.done()]
        if running:
            # asyncio.wait never cancels what it waits for, so a bounded
            # wait cannot cut a commit short.
            _done, still_running = await asyncio.wait(running, timeout=timeout)
            if still_running:
                await logger.awarning(
                    'cache_peer_commit_still_running',
                    category='cache',
                    timeout_seconds=timeout,
                    running=len(still_running),
                )
        for task in [task for task in self._pending_peer_commits if task.done()]:
            self._pending_peer_commits.discard(task)
            if not task.cancelled() and task.exception() is not None:
                exc = task.exception()
                await logger.awarning(
                    'cache_orphaned_peer_commit_failed',
                    category='cache',
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    async def _sync_once(self) -> dict[str, int]:
        """Pull new rows from the peers under a per-cycle wall-clock deadline.

        Thin wrapper around ``_sync_inner`` that enforces a hard cap on a
        single cycle's total wall-clock time. Without the cap, a slow peer
        (a hung connection, a busy worker) could keep ``_sync_inner`` in
        flight well past the next scheduled cycle and starve the periodic
        task.

        Deadline resolution:
        - ``fleet.cache_exchange.cycle_deadline_seconds`` when set explicitly.
        - Otherwise ``interval_seconds * 0.9`` — strictly less than the
          gap between invocations so the periodic loop always has slack
          to schedule the next cycle even if this one hit the cap.

        On timeout: log a warning, increment ``cache_peer_sync_timeouts``,
        and re-raise ``TimeoutError`` so the periodic-task wrapper
        classifies the run as ``status=error``.
        """
        # A commit orphaned by the PREVIOUS cycle's deadline must finish
        # before this cycle touches the writer — otherwise the two could
        # interleave transactions on the shared connection.
        await self._await_pending_peer_commits(timeout=self._exchange.timeout_seconds)

        cycle_deadline = self._exchange.cycle_deadline_seconds
        # ``None`` is the default: derive from the interval so operators
        # who never tune the deadline still get a reasonable bound. The
        # 0.9 factor gives the periodic loop headroom to schedule the
        # next cycle without overlapping. The 0.1s clamp is a local floor
        # — ``interval_seconds`` is only constrained by ``gt=0``, so a tiny
        # value (e.g. a pathological test config with interval_seconds=0.05)
        # could otherwise yield a deadline too small to be usable.
        deadline = cycle_deadline if cycle_deadline is not None else max(0.1, self._exchange.interval_seconds * 0.9)
        try:
            return await asyncio.wait_for(self._sync_inner(), timeout=deadline)
        except TimeoutError:
            metrics.cache_peer_sync_timeouts.inc()
            await logger.awarning(
                'cache_peer_sync_cycle_timeout',
                category='cache',
                deadline_seconds=deadline,
            )
            # Re-raise so the periodic-task wrapper classifies this run as
            # ``status=error`` — dashboards keyed on
            # ``periodic_task_runs{status='error'}`` already alert on
            # sync failures, and a silent swallow here would hide peer-sync
            # deadline breaches from the usual observability path.
            raise

    async def _sync_inner(self) -> dict[str, int]:
        """Pull new rows from every peer worth a pull, ``max_concurrent_peers`` at a time.

        Returns how many rows were applied per peer that finished. Each peer runs in
        its own task and handles its own failures, so one bad peer never
        affects the others. The deadline in :meth:`_sync_once` cancels the
        peers still pulling or still waiting for a slot; neither counts as a
        failure, and the pages already applied keep their cursor.
        """
        pulled: dict[str, int] = {}
        if self._writer_db is None or not self._peer_sync_enabled or self._peer_views is None:
            return pulled
        targets = select_targets(
            self._peer_views(),
            own_worker=self._worker_id,
            own_cluster=self._cluster_name,
            cursors=self._peer_cursors,
            backoff=self._peer_backoff,
        )
        if not targets:
            return pulled
        client = self._http_client()
        slots = asyncio.Semaphore(self._exchange.max_concurrent_peers)

        async def pull(target: PeerTarget) -> None:
            async with slots:
                pulled[target.name] = await self._pull_peer(client, target)

        async with asyncio.TaskGroup() as group:
            for target in targets:
                group.create_task(pull(target), name=f'cache.sync.{target.name}')
        return pulled

    async def _apply_peer_rows(self, *, peer_name: str, rows: list[tuple]) -> None:
        """UPSERT a batch of peer-supplied rows into the local DB via the
        shared LWW SQL, then invalidate memory for the affected keys.

        The incoming tuple order is the served row's field order
        (``keep_rows`` builds it): ``(key, scope, value, size_bytes,
        created_at_ms, updated_at_ms, expires_at_ms, origin_worker_id)`` —
        the exact binding order ``LWW_UPSERT_SQL`` expects, so no
        translation is needed.

        Cancellation safety: the entire commit + post-commit bookkeeping
        trio (executemany, commit, counter ticks, memory invalidation) is
        wrapped in ``asyncio.shield`` via ``_commit_peer_rows`` so the
        ``_sync_once`` deadline cannot interrupt mid-transaction and leave
        memory stale relative to the DB.
        """
        if not rows:
            return
        if self._writer_db is None:
            return

        # Shield the entire commit + post-commit bookkeeping trio from
        # ``asyncio.wait_for`` cancellation in the ``_sync_once`` deadline
        # wrapper. Two correctness problems drive the wide shield boundary:
        #
        # 1. DB consistency: without a shield, the deadline could fire
        #    between executemany and commit, cancel the current
        #    ``_apply_peer_rows`` mid-transaction, and leave the writer in
        #    an open-transaction state. The NEXT flush/commit would then
        #    commit BOTH batches as one transaction.
        # 2. Memory/DB coherence: if the shield covered only the commit,
        #    ``CancelledError`` would still raise at the shield call site,
        #    skipping the counter ticks and ``_invalidate_memory_keys``
        #    call. The DB would hold the new peer rows but the memory
        #    cache would still hold the STALE pre-sync values.
        #
        # The shielded work runs as a real task we keep a reference to:
        # when the deadline cancels *us*, the task keeps running, and the
        # next sync cycle / ``stop()`` await it via
        # ``_await_pending_peer_commits`` before touching the writer again.
        commit_task = asyncio.create_task(
            self._commit_peer_rows(peer_name=peer_name, rows=rows),
            name='cache.peer_commit',
        )
        self._pending_peer_commits.add(commit_task)
        try:
            await asyncio.shield(commit_task)
        finally:
            # Only forget it when the task actually finished — on
            # cancellation of the awaiter the task is still running and
            # must stay tracked for the bounded await later.
            if commit_task.done():
                self._pending_peer_commits.discard(commit_task)

    async def _commit_peer_rows(self, *, peer_name: str, rows: list[tuple]) -> None:
        """Run the executemany + commit + bookkeeping trio uninterruptibly.

        Factored out so the caller can wrap the call in ``asyncio.shield``
        and keep the deadline's cancellation semantics independent of the
        SQL + post-commit bookkeeping. Post-commit bookkeeping lives here
        — NOT at the caller — so the shield covers all three steps as a
        single cancellation-atomic unit:
        1. executemany + commit (DB state changes)
        2. metric counters (observability of what landed)
        3. memory invalidation (keeps ``Cache._memory`` ≤ DB freshness)
        """
        # Narrow the type for ty: the caller checked ``_writer_db is None``
        # but since this is a separate method the type checker can't see
        # that guard. A local ``assert``-free runtime check is cheap and
        # lets the method stay callable in isolation (e.g. tests).
        if self._writer_db is None:
            return
        # ``_writer_lock`` serializes this UPSERT+commit against flush and
        # cleanup on the shared writer connection.
        async with self._writer_lock:
            try:
                await self._writer_db.executemany(LWW_UPSERT_SQL, rows)
                await self._writer_db.commit()
            except BaseException:
                # Roll back the open transaction so a failed peer batch
                # can't be silently committed by the next writer commit.
                with contextlib.suppress(Exception):
                    await self._writer_db.rollback()
                raise

        # Both counters tick AFTER commit so they stay consistent under
        # commit-failure. Mirrors the flush path's post-commit metric-tick
        # pattern — counters track work that actually landed.
        metrics.cache_sync_entries_fetched.labels(peer=peer_name).inc(len(rows))
        metrics.cache_sync_entries_upserted.labels(peer=peer_name).inc(len(rows))

        # Memory invalidation: unconditional pop for every key we
        # attempted. Simpler than per-row LWW-result bookkeeping, and any
        # caller that needs the freshest value will hit the DB on the next
        # ``get`` which already includes the merged LWW result.
        if self._cache is not None:
            self._cache._invalidate_memory_keys([row[0] for row in rows])
