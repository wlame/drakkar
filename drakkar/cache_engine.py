"""``CacheEngine`` — owns SQLite persistence + peer-sync for one worker's cache.

Two connections: a **writer** for flush/cleanup/sync-UPSERT, and a **reader**
for ``Cache.get()`` DB fallback. aiosqlite spawns a dedicated OS thread per
connection, so SQLite operations run off the event loop; WAL mode allows the
reader to see consistent snapshots while the writer commits.

The engine is constructed by the app during startup; ``start()`` opens the
DB and prepares the periodic loops (flush, cleanup, peer-sync). If neither
``cache.db_dir`` nor ``debug.db_dir`` is set, ``start()`` logs a warning and
puts the engine into an *effectively disabled* state — no connection is
opened, no file is created, and subsequent flush/sync/cleanup calls become
no-ops. The handler still sees a ``Cache`` instance; it just never persists.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite
import structlog

from drakkar import metrics
from drakkar.cache_memory import Cache
from drakkar.cache_models import CacheScope, _now_ms
from drakkar.cache_sql import (
    LWW_UPSERT_SQL,
    PEER_CLUSTER_CACHE_TTL_SECONDS,
    SCHEMA_CACHE_ENTRIES,
)

# Imported at module scope for the same monkeypatch reason — tests replace
# this helper with a fake that yields canned ``(worker_name, target_path)``
# tuples so peer sync can be exercised without a real filesystem.
from drakkar.peer_discovery import discover_peer_dbs

# Imported at module scope (not inside ``start()``) so that tests can
# ``monkeypatch.setattr(cache_engine_module, 'run_periodic_task', ...)`` —
# a local import would bind to the concrete function each call and defeat
# the spy.
from drakkar.periodic import run_periodic_task

if TYPE_CHECKING:
    from drakkar.config import CacheConfig, DebugConfig
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()


class CacheEngine:
    """Owns the SQLite backing store and periodic loops for a single worker's cache.

    Two connections: a **writer** for flush/cleanup/sync-UPSERT, and a
    **reader** for ``Cache.get()`` DB fallback. aiosqlite spawns a dedicated
    OS thread per connection, so SQLite operations run off the event loop;
    WAL mode allows the reader to see consistent snapshots while the writer
    commits.

    The engine is constructed by the app during startup; ``start()`` opens
    the DB and prepares the periodic loops. If neither ``cache.db_dir`` nor
    ``debug.db_dir`` is set, ``start()`` logs a warning and puts the engine
    into an *effectively disabled* state — no connection is opened, no file
    is created, and subsequent flush/sync/cleanup calls become no-ops. The
    handler still sees a ``Cache`` instance; it just never persists.
    """

    def __init__(
        self,
        config: CacheConfig,
        debug_config: DebugConfig,
        worker_id: str,
        cluster_name: str,
        recorder: EventRecorder | None = None,
    ) -> None:
        """Wire up an engine without opening any resources.

        Args:
            config: cache-specific settings (enabled flag, intervals,
                peer-sync, memory cap, optional dedicated db_dir).
            debug_config: referenced only for its ``db_dir`` fallback when
                ``config.db_dir`` is empty. The engine does NOT otherwise
                reach into debug settings.
            worker_id: stable identifier for this worker — used for the
                DB filename, the symlink name, peer-discovery self-filter,
                and the ``origin_worker_id`` column on every write.
            cluster_name: used later by peer-sync to decide whether to
                pull ``cluster``-scoped rows from a given peer. Stored
                here on construction so the info is available without
                re-reading config.
            recorder: optional event recorder the periodic loops feed
                ``periodic_run`` events into. Kept None in tests that
                only care about lifecycle / schema, so the flush path
                can avoid requiring the recorder's presence.
        """
        self._config = config
        self._debug_config = debug_config
        self._worker_id = worker_id
        self._cluster_name = cluster_name
        self._recorder = recorder
        # Writer connection. Opened in ``start()``, closed in ``stop()``.
        # Nil when the engine is disabled (config.enabled=False or no
        # db_dir resolution) or has not been started yet.
        self._writer_db: aiosqlite.Connection | None = None
        # Reader connection. Second aiosqlite connection to the same DB
        # file, opened in read-only mode. Used exclusively by ``Cache.get``
        # for DB fallback. aiosqlite spawns one worker thread per
        # connection, so SELECTs run off the event loop and never queue
        # behind a flush/cleanup/sync commit on the writer's thread. WAL
        # mode lets the reader see a consistent snapshot while the writer
        # commits.
        self._reader_db: aiosqlite.Connection | None = None
        # Resolved DB file path — stored so ``stop()`` can remove the
        # symlink without re-resolving the dir.
        self._db_path: str = ''
        # The handler-facing Cache that feeds this engine's flush/sync
        # pipelines. Wired in via ``attach_cache`` — the app does this
        # at startup before scheduling the periodic loops. We keep the
        # relation one-to-one because a single worker has a single
        # cache-DB file, and splitting caches across engines would make
        # the ``_dirty`` swap (see ``_flush_once``) ambiguous.
        self._cache: Cache | None = None
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
        # enabled (both config flag on AND debug.store_config=True). None
        # when disabled so there's no wakeup cost.
        self._sync_task: asyncio.Task | None = None
        # Whether peer-sync is *effectively* enabled — i.e. both the config
        # flag is on and the startup gating rules passed. Set during
        # ``start()``: peer-sync needs ``debug.store_config`` (to read peer
        # cluster_names from their ``worker_config`` tables) and a writable
        # local DB, so startup may silently downgrade to disabled even if
        # ``config.peer_sync.enabled=True``. ``_sync_once`` short-circuits
        # on this flag to keep the idle-engine cost at O(1).
        self._peer_sync_enabled: bool = False
        # In-process cache of peer ``cluster_name`` values keyed by peer
        # worker_name. Populated lazily from each peer's ``-live.db``
        # ``worker_config`` row and refreshed when the entry's age exceeds
        # ``PEER_CLUSTER_CACHE_TTL_SECONDS``. A cache miss is cheap (one
        # small SELECT), but on a large fleet the N^2 read pattern adds up
        # without this short-lived cache.
        self._peer_cluster_cache: dict[str, tuple[str, float]] = {}
        # Per-peer sync cursors. Maps peer worker_name → max ``updated_at_ms``
        # pulled so far. The next cycle's pull uses ``WHERE updated_at_ms > ?``
        # with this value so we never re-read rows we've already processed.
        #
        # Advancement rules (see ``_advance_cursor`` / ``_sync_once``):
        #  - zero rows     → cursor unchanged (no progress, retry next cycle)
        #  - partial batch → cursor jumps to max(last_row_ts, cursor_ms) to
        #                    stay anchored on the peer's clock (clock-skew
        #                    safe) and never regress on a stale insert
        #  - full batch    → cursor advances to last row's ``updated_at_ms``
        #                    so the next cycle picks up the tail
        #
        # Same-ms edge case: if a peer has more rows sharing ``rows[-1]``'s
        # ``updated_at_ms`` than fit in the ``LIMIT`` (either a pure
        # burst or a mixed-ms batch with spillover at the last ms), the
        # strict ``> cursor_ms`` filter would skip the tail (SQLite's
        # deterministic ordering would keep returning the same
        # LIMIT-bounded prefix even if we stepped the cursor back).
        # ``_sync_once`` runs a follow-up "drain" pull after ANY full
        # batch, keyed on ``updated_at_ms = last_ms AND key > last_key``,
        # that applies all remaining same-ms rows before the cursor
        # advances. See ``_drain_same_ms_tail``.
        #
        # Cursor state is in-memory only. A worker restart resets cursors to
        # zero and the next cycle re-pulls everything from peers — bounded by
        # TTL cleanup and LWW rejection, so no runaway work. Persisting
        # cursors across restarts is a follow-up (marked as such in the plan).
        self._peer_cursors: dict[str, int] = {}

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
    def _cache_link_path(db_dir: str, worker_id: str) -> str:
        """Return the peer-discovery symlink path: ``<db_dir>/<worker>-cache.db``.

        This is the *symlink* name, scanned by ``discover_peer_dbs(...,
        suffix='-cache.db')``. Peers find us by reading this symlink and
        following it to the underlying DB file (see ``_cache_db_file_path``).
        Scoped to ``CacheEngine`` because nothing outside the engine
        builds this path.
        """
        return str(Path(db_dir) / f'{worker_id}-cache.db')

    @staticmethod
    def _cache_db_file_path(db_dir: str, worker_id: str) -> str:
        """Return the actual DB file path: ``<db_dir>/<worker>-cache.db.actual``.

        The cache DB file lives under a ``.actual`` suffix so the stable
        ``<worker>-cache.db`` name can be reserved as a symlink target — the
        same convention peer discovery uses for recorder live links. Since
        the cache does not rotate files in v1, the ``.actual`` file is
        effectively permanent; the indirection exists purely so peer
        discovery's ``os.path.islink`` filter works identically for
        recorder and cache.
        """
        return str(Path(db_dir) / f'{worker_id}-cache.db.actual')

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
        """Return the directory the cache DB should live in, or ''.

        Precedence:
        1. ``cache.db_dir`` if set (operator explicitly isolated the
           cache DB from the event recorder DB).
        2. ``debug.db_dir`` otherwise (default operational setup — cache
           piggybacks on the debug directory).
        3. Empty string → engine runs in disabled mode.
        """
        return self._config.db_dir or self._debug_config.db_dir

    def _update_live_link(self) -> None:
        """Create/refresh the ``<worker>-cache.db`` symlink pointing at the DB file.

        Uses the atomic-rename pattern (write ``.tmp`` symlink, os.replace
        into place) mirrored from the recorder's ``_update_live_link`` —
        this avoids windows where a concurrent peer discovery scan sees
        a half-created link.

        Silently swallows ``OSError`` — on filesystems that don't support
        symlinks (rare on Linux, possible on exotic mounts) the missing
        link just means peers can't discover us, not a fatal error.
        """
        if not self._db_path:
            return
        db_dir = os.path.dirname(self._db_path)
        link = self._cache_link_path(db_dir, self._worker_id)
        target = os.path.basename(self._db_path)
        try:
            tmp = link + '.tmp'
            # clean up stale tmp leftover from a crashed prior run
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            os.symlink(target, tmp)
            os.replace(tmp, link)
        except OSError:
            pass

    def _remove_live_link(self) -> None:
        """Remove the peer-discovery symlink on graceful shutdown.

        Mirrors the recorder's shutdown hygiene so peers don't keep trying
        to pull from a shut-down worker's cache DB. Safe to call when the
        link doesn't exist or the filesystem doesn't support symlinks.
        """
        if not self._db_path:
            return
        db_dir = os.path.dirname(self._db_path)
        link = self._cache_link_path(db_dir, self._worker_id)
        try:
            if os.path.islink(link):
                os.remove(link)
        except OSError:
            pass

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
        await self._writer_db.commit()

    async def start(self) -> None:
        """Open the writer connection and apply the schema.

        Idempotent: calling ``start()`` on an already-started engine is
        a no-op (the second call would otherwise re-run schema DDL, which
        is safe but wasteful). Returns without opening resources when the
        cache is disabled or no db_dir can be resolved — the engine stays
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
            # Warn-and-continue (not fail-at-startup) per the plan. The
            # handler will still get a Cache instance; it just won't
            # persist. Peer-sync is automatically disabled because it
            # needs the writer connection to apply UPSERTs.
            await logger.awarning(
                'cache_engine_disabled_no_db_dir',
                category='cache',
                worker_id=self._worker_id,
                reason='cache.db_dir empty and debug.db_dir empty',
            )
            return

        os.makedirs(db_dir, exist_ok=True)
        self._db_path = self._cache_db_file_path(db_dir, self._worker_id)
        self._writer_db = await aiosqlite.connect(self._db_path)
        await self._create_schema()

        # Open the reader connection on a read-only URI. ``file:…?mode=ro``
        # tells SQLite to reject any write attempt, and aiosqlite spawns a
        # fresh worker thread for this connection — so DB-fallback SELECTs
        # from ``Cache.get`` never queue behind a flush/cleanup/sync commit
        # on the writer's thread. WAL mode (set on the writer above) lets
        # the reader see consistent snapshots while the writer commits.
        reader_uri = f'file:{self._db_path}?mode=ro'
        self._reader_db = await aiosqlite.connect(reader_uri, uri=True)
        # If a Cache has already been attached (typical — engine.attach_cache
        # happens before start()), wire the reader through now; if it gets
        # attached later, the caller is responsible for calling
        # ``attach_reader_db`` themselves.
        if self._cache is not None:
            self._cache.attach_reader_db(self._reader_db)

        self._update_live_link()

        # Gate peer-sync on both config flags. ``peer_sync.enabled=False``
        # (the explicit off-switch) always wins. Otherwise peer-sync also
        # needs ``debug.store_config=True`` — we pull each peer's
        # ``cluster_name`` from their ``worker_config`` table, which only
        # exists when the peer's recorder has ``store_config=True``.
        # Logging once at startup keeps operators aware of the effective
        # state without spamming the sync loop.
        #
        # We use two log levels intentionally so operators can tell the
        # cases apart at a glance:
        #   - explicit off-switch (config says disabled) → INFO ("I turned
        #     it off"), no action needed
        #   - silent downgrade (peer_sync asked for, but store_config off)
        #     → WARNING ("you wanted this but it got disabled"), operator
        #     should decide whether to flip store_config or accept the
        #     degraded mode
        if not self._config.peer_sync.enabled:
            self._peer_sync_enabled = False
            await logger.ainfo(
                'cache_peer_sync_disabled_by_config',
                category='cache',
                worker_id=self._worker_id,
                reason='peer_sync.enabled=False in config',
            )
        elif not self._debug_config.store_config:
            self._peer_sync_enabled = False
            await logger.awarning(
                'cache_peer_sync_disabled_no_store_config',
                category='cache',
                worker_id=self._worker_id,
                reason='peer_sync needs debug.store_config=True to read peer cluster_names',
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
        # participate in cross-worker cache sharing (including workers
        # where ``debug.store_config=False`` silently downgraded sync).
        if self._peer_sync_enabled:
            self._sync_task = asyncio.create_task(
                run_periodic_task(
                    name='cache.sync',
                    coro_fn=self._sync_once,
                    seconds=self._config.peer_sync.interval_seconds,
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
        connections, and remove the live symlink.

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
        6. **Remove the live symlink** last — the row-level data is safe,
           and peers can stop pulling from us once the symlink is gone.

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

        # Step 6: tell peers we're gone.
        self._remove_live_link()
        await logger.ainfo(
            'cache_engine_stopped',
            category='cache',
            worker_id=self._worker_id,
        )

    # ---- flush loop ---------------------------------------------------------

    async def _flush_once(self) -> None:
        """Drain one batch of pending mutations from the Cache's dirty map
        to the local SQLite DB.

        A single flush cycle does the following:

        1. **Atomic swap** — snapshot the current ``_dirty`` map and
           reset it to an empty dict in one tuple-assign operation. Under
           CPython's GIL this is atomic: any ``Cache.set`` /
           ``Cache.delete`` landing during the swap either wrote to the
           now-snapshotted dict (and will be flushed this cycle) or to
           the fresh empty dict (picked up next cycle). No writes lost.

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
        from drakkar.cache_models import Op

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
                # semantics on ``_dirty``.
                self._cache.restore_dirty(snapshot)

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
        cursor = await self._writer_db.execute(
            'DELETE FROM cache_entries WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?',
            (now_ms,),
        )
        removed = cursor.rowcount if cursor.rowcount is not None else 0
        await cursor.close()
        await self._writer_db.commit()

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
        # rather than NULL.
        cursor = await self._writer_db.execute('SELECT count(*), coalesce(sum(size_bytes), 0) FROM cache_entries')
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            db_count, db_bytes = row
            metrics.cache_entries_in_db.set(db_count)
            metrics.cache_bytes_in_db.set(db_bytes)

        if removed > 0:
            metrics.cache_cleanup_removed.inc(removed)

    # ---- peer sync ----------------------------------------------------------

    async def _resolve_peer_cluster(self, peer_worker_name: str, db_dir: str) -> str | None:
        """Return the peer's ``cluster_name``, or ``None`` if it cannot be read.

        Reads ``<db_dir>/<peer>-live.db``'s ``worker_config`` table. Results
        are cached in process for ``PEER_CLUSTER_CACHE_TTL_SECONDS`` keyed
        by peer worker name, so we don't re-open the recorder DB on every
        sync cycle. On a miss (missing symlink, missing table, read error)
        we return ``None`` and the caller treats the peer as "unknown cluster"
        — conservative, since we only want to pull ``scope='global'`` rows
        in that case.

        Args:
            peer_worker_name: the peer's stable identifier from the cache
                symlink basename.
            db_dir: shared directory holding the peer's recorder DBs.

        Returns:
            The peer's ``cluster_name`` string (possibly empty — some
            deployments run without a cluster label) or ``None`` if the
            information is not currently readable. The empty-string case
            is cached like any other value.
        """
        # Per-peer in-process cache with TTL. A missing entry or a stale
        # one falls through to the recorder-DB read below.
        now = time.time()
        cached = self._peer_cluster_cache.get(peer_worker_name)
        if cached is not None:
            cluster_name, stored_at = cached
            if now - stored_at < PEER_CLUSTER_CACHE_TTL_SECONDS:
                return cluster_name

        # The cache DB lives at ``<peer>-cache.db``; the recorder DB lives
        # at ``<peer>-live.db`` alongside it. We open the recorder DB
        # read-only to avoid any chance of writes to a peer's file.
        live_link = str(Path(db_dir) / f'{peer_worker_name}-live.db')
        if not os.path.exists(live_link):
            # Peer's recorder may be down or not configured with
            # ``store_config=True``. Don't populate the cache so next
            # cycle can retry.
            return None

        # Each peer DB query is wrapped in ``asyncio.wait_for`` with the
        # configured timeout so a stuck peer cannot block the sync cycle.
        # ``asyncio.TimeoutError`` propagates into the outer ``except`` below.
        timeout = self._config.peer_sync.timeout_seconds
        try:
            async with aiosqlite.connect(f'file:{live_link}?mode=ro', uri=True) as db:
                async with db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='worker_config'"
                ) as cur:
                    existing_row = await asyncio.wait_for(cur.fetchone(), timeout=timeout)
                    if not existing_row:
                        # Peer exists but didn't enable store_config. Treat
                        # as "no cluster info available" — pull only
                        # ``scope='global'`` rows.
                        await logger.awarning(
                            'cache_peer_missing_worker_config',
                            category='cache',
                            peer=peer_worker_name,
                            db=live_link,
                        )
                        return None
                async with db.execute('SELECT cluster_name FROM worker_config WHERE id = 1') as cur:
                    row = await asyncio.wait_for(cur.fetchone(), timeout=timeout)
                    # ``row`` is None when the table is empty; ``row[0]`` is
                    # None when the column was never populated. Both cases
                    # collapse to an empty string (unclustered peer) — the
                    # ``or ''`` handles both None-row (via the ternary) and
                    # None-column in a single expression.
                    cluster_name = (row[0] if row is not None else None) or ''
        except Exception:
            # Corrupt / inaccessible peer DB — log and move on. Don't
            # cache the failure; next cycle retries.
            await logger.aexception(
                'cache_peer_cluster_resolve_failed',
                category='cache',
                peer=peer_worker_name,
                db=live_link,
            )
            return None

        self._peer_cluster_cache[peer_worker_name] = (cluster_name, now)
        return cluster_name

    def _peer_scope_filter(self, peer_cluster_name: str | None) -> tuple[str, tuple]:
        """Build the ``scope`` predicate + params used when pulling from a peer.

        Rules:
        - Peer in the same cluster as us → pull ``scope IN ('cluster','global')``.
        - Peer in a different cluster → pull ``scope = 'global'`` only.
        - Peer with unknown cluster (read failure) → pull ``scope = 'global'``
          only (conservative — we won't assume a same-cluster match without
          evidence).
        - ``scope='local'`` is never pulled; LOCAL entries stay on the
          originating worker by contract.

        Returns a ``(sql_fragment, params)`` pair ready to splice into the
        larger SELECT in ``_sync_once`` via parameterized binding.
        """
        if peer_cluster_name is not None and peer_cluster_name == self._cluster_name:
            return ('scope IN (?, ?)', (CacheScope.CLUSTER.value, CacheScope.GLOBAL.value))
        return ('scope = ?', (CacheScope.GLOBAL.value,))

    async def _pull_peer_rows(
        self,
        *,
        peer_db_path: str,
        scope_sql: str,
        scope_params: tuple,
        batch_size: int,
        cursor_ms: int,
    ) -> list[tuple]:
        """Open an ephemeral read-only connection to the peer DB and pull rows.

        Each peer sync gets its own short-lived aiosqlite connection. This
        is deliberate: aiosqlite spawns a fresh OS worker thread per
        connection, so peer reads never queue behind our writer's
        flush/cleanup/sync-UPSERT commits. We use a ``file:...?mode=ro`` URI
        so even a misconfigured peer DB can never be written to through our
        connection.

        Filters out expired rows at the DB layer — no point pulling
        already-dead entries across the network boundary just to reject
        them during UPSERT (LWW would let them in if they happened to have
        a newer ``updated_at_ms``, only for cleanup to reap them soon
        after).

        Also filters by the per-peer cursor (``updated_at_ms > cursor_ms``)
        so we skip rows we've already read. Callers pass the current cursor
        from ``_peer_cursors``; on the first sync cycle the cursor is 0 and
        nothing is filtered out.
        """
        now_ms = _now_ms()
        # ORDER BY includes ``key ASC`` as an explicit secondary sort. This
        # is a correctness requirement for the same-ms drain in
        # ``_sync_once``: the drain uses ``rows[-1][0]`` (the last row's
        # key) as a lex-max cursor and queries ``WHERE updated_at_ms = ms
        # AND key > last_key``. Without an explicit ``key ASC`` tiebreaker,
        # SQLite breaks ``updated_at_ms`` ties by rowid (insertion order),
        # which equals the peer's flush order — derived from arbitrary
        # business-logic ``Cache.set`` call order, not lex order. In that
        # case ``rows[-1][0]`` might NOT be the lex-max among the same-ms
        # batch, and ``key > last_key`` would silently skip keys that
        # happen to sort lexicographically below ``last_key`` but were
        # inserted after it. Explicit secondary key sort guarantees
        # ``rows[-1][0]`` IS the lex-max key among the same-ms tail, making
        # the drain's ``key > last_key`` walk correct.
        query = (
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries '
            f'WHERE {scope_sql} '
            'AND (expires_at_ms IS NULL OR expires_at_ms > ?) '
            'AND updated_at_ms > ? '
            'ORDER BY updated_at_ms ASC, key ASC '
            'LIMIT ?'
        )
        params = (*scope_params, now_ms, cursor_ms, batch_size)
        # ``asyncio.wait_for`` wraps the fetch with the configured peer-sync
        # timeout so a slow or wedged peer cannot stall the whole sync
        # cycle. Timeout raises ``asyncio.TimeoutError`` which the caller's
        # per-peer try/except catches and isolates.
        timeout = self._config.peer_sync.timeout_seconds
        async with (
            aiosqlite.connect(f'file:{peer_db_path}?mode=ro', uri=True) as db,
            db.execute(query, params) as cur,
        ):
            # aiosqlite's ``fetchall`` returns ``Iterable[Row]``; we materialize
            # to a concrete tuple-list so callers can index by column position
            # (keys come back at row[0], scope at row[1], etc.) without worrying
            # about cursor lifetime after the ``async with`` closes.
            fetched = await asyncio.wait_for(cur.fetchall(), timeout=timeout)
            return [tuple(row) for row in fetched]

    async def _drain_same_ms_tail(
        self,
        *,
        peer_db_path: str,
        scope_sql: str,
        scope_params: tuple,
        batch_size: int,
        ms: int,
        last_key: str,
    ) -> list[tuple]:
        """Drain any remaining rows at a single peer timestamp ``ms`` that
        spilled past the main pull's ``LIMIT``.

        See the original implementation for the full rationale around
        same-ms bursts and mixed-ms batches with spillover. The follow-up
        query targets exactly ``updated_at_ms = ms AND key > last_key``
        with ``ORDER BY key ASC LIMIT batch_size`` — a deterministic
        key-based walk that makes progress even when every row shares one
        timestamp.
        """
        now_ms = _now_ms()
        # Query shape mirrors ``_pull_peer_rows`` for the SELECT list (so
        # the tuple order is identical and the caller can hand the rows
        # straight to ``_apply_peer_rows``). The WHERE is tighter: we're
        # only interested in one millisecond, and we walk forward by key
        # for a deterministic same-ms tiebreaker. ``ORDER BY key ASC``
        # guarantees ``key > cursor_key`` makes progress.
        query = (
            'SELECT key, scope, value, size_bytes, created_at_ms, '
            'updated_at_ms, expires_at_ms, origin_worker_id '
            'FROM cache_entries '
            f'WHERE {scope_sql} '
            'AND (expires_at_ms IS NULL OR expires_at_ms > ?) '
            'AND updated_at_ms = ? '
            'AND key > ? '
            'ORDER BY key ASC '
            'LIMIT ?'
        )
        drained: list[tuple] = []
        cursor_key = last_key
        # One connection for the whole drain — each iteration used to open
        # and close its own ``aiosqlite.connect`` with the aiosqlite-spawned
        # worker-thread overhead that implies. Hoisting the connection keeps
        # the drain on a single worker thread end-to-end; the ``key > ?``
        # cursor still guarantees forward progress and distinct pages per
        # iteration.
        timeout = self._config.peer_sync.timeout_seconds
        async with aiosqlite.connect(f'file:{peer_db_path}?mode=ro', uri=True) as db:
            while True:
                params = (*scope_params, now_ms, ms, cursor_key, batch_size)
                async with db.execute(query, params) as cur:
                    # ``wait_for`` applies the configured peer-sync timeout to
                    # each page fetch. A peer whose DB is stuck (corrupt WAL,
                    # slow I/O) raises ``asyncio.TimeoutError`` rather than
                    # stalling the whole sync cycle; the per-peer try/except
                    # in ``_sync_once`` catches and isolates the failure.
                    page = await asyncio.wait_for(
                        cur.fetchall(),
                        timeout=timeout,
                    )
                page_rows = [tuple(row) for row in page]
                if not page_rows:
                    break
                drained.extend(page_rows)
                # Last page — no need for another round trip.
                if len(page_rows) < batch_size:
                    break
                # Advance the key cursor and loop; the ``key > ?`` filter
                # guarantees forward progress on the next iteration.
                cursor_key = str(page_rows[-1][0])
        return drained

    def _advance_cursor(
        self,
        *,
        rows_count: int,
        batch_size: int,
        rows: list[tuple],
        cursor_ms: int,
    ) -> int | None:
        """Compute the new cursor value for a peer after a pull, or ``None``
        if it should stay where it is.

        Rules:
        * Zero rows returned → return ``None`` (caller leaves cursor
          unchanged). This prevents a single empty cycle from skipping
          over late-arriving rows whose ``updated_at_ms`` is between
          the old cursor and "now".
        * Full batch (``rows_count == batch_size``) → return the last
          row's ``updated_at_ms``. Same-ms spillover is handled separately
          by the drain in ``_sync_once``; this helper does not need to
          special-case it — the drain is an additive apply before the
          cursor settles here.
        * Partial batch (``0 < rows_count < batch_size``) → return
          ``max(rows[-1].updated_at_ms, cursor_ms)``. Anchoring to the
          observed peer timestamp (rather than our local wall clock) is
          the critical property here: peer rows are written with the
          peer's clock, which can be skewed relative to ours.
        """
        if rows_count == 0:
            return None
        if rows_count >= batch_size:
            # Rows are ordered by updated_at_ms ASC, so row[-1] has the
            # largest timestamp. Column index 5 is updated_at_ms (see the
            # SELECT list in ``_pull_peer_rows``).
            return int(rows[-1][5])
        # Partial batch: track the peer's observed timestamps, not our
        # wall clock. ``max(..., cursor_ms)`` enforces monotonicity.
        return max(int(rows[-1][5]), cursor_ms)

    async def _sync_once(self) -> dict[str, list[tuple]]:
        """Pull one batch of entries from every discovered peer with a
        per-cycle wall-clock deadline.

        Thin wrapper around ``_sync_inner`` that enforces a hard cap on a
        single cycle's total wall-clock time. Without the cap, a single
        slow peer (NFS lag, pathological disk contention, remote hang)
        could keep ``_sync_inner`` in flight well past the next scheduled
        cycle and starve the periodic task.

        Deadline resolution:
        - ``config.peer_sync.cycle_deadline_seconds`` when set explicitly.
        - Otherwise ``interval_seconds * 0.9`` — strictly less than the
          gap between invocations so the periodic loop always has slack
          to schedule the next cycle even if this one hit the cap.

        On timeout: log a warning, increment ``cache_peer_sync_timeouts``,
        and re-raise ``TimeoutError`` so the periodic-task wrapper
        classifies the run as ``status=error``.
        """
        cycle_deadline = self._config.peer_sync.cycle_deadline_seconds
        # ``None`` is the default: derive from the interval so operators
        # who never tune the deadline still get a reasonable bound. The
        # 0.9 factor gives the periodic loop headroom to schedule the
        # next cycle without overlapping. The 0.1s clamp is a local floor
        # — ``interval_seconds`` is only constrained by ``gt=0``, so a tiny
        # value (e.g. a pathological test config with interval_seconds=0.05)
        # could otherwise yield a deadline too small to be usable.
        if cycle_deadline is not None:
            deadline = cycle_deadline
        else:
            deadline = max(0.1, self._config.peer_sync.interval_seconds * 0.9)
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

    async def _sync_inner(self) -> dict[str, list[tuple]]:
        """Pull one batch of entries from every discovered peer and apply
        them to the local DB via LWW UPSERT.

        Uses cursor-based incremental pulls and isolates failures to the
        offending peer so one bad DB cannot break the cycle.
        """
        pulled: dict[str, list[tuple]] = {}

        if self._writer_db is None or not self._peer_sync_enabled:
            return pulled

        # Resolve the dir once per cycle — both peer-discovery and cluster-
        # name resolution (below) use it, so avoid recomputing.
        db_dir = self._resolve_db_dir()
        if not db_dir:
            return pulled

        batch_size = self._config.peer_sync.batch_size
        # Iterate peers. One bad peer must not break the whole sync cycle —
        # isolation is part of the peer-sync contract. Any exception inside
        # the per-peer block: log, tick the error counter, leave the
        # cursor untouched, continue to the next peer.
        async for peer_name, peer_db_path in discover_peer_dbs(
            db_dir,
            '-cache.db',
            self._worker_id,
        ):
            # Cursor: 0 on first encounter, otherwise the last successful
            # advance. A failing peer keeps its previous cursor so next
            # cycle retries the same range.
            cursor_ms = self._peer_cursors.get(peer_name, 0)
            try:
                peer_cluster = await self._resolve_peer_cluster(peer_name, db_dir)
                scope_sql, scope_params = self._peer_scope_filter(peer_cluster)
                rows = await self._pull_peer_rows(
                    peer_db_path=peer_db_path,
                    scope_sql=scope_sql,
                    scope_params=scope_params,
                    batch_size=batch_size,
                    cursor_ms=cursor_ms,
                )
                pulled[peer_name] = rows
                # Apply step — UPSERT pulled rows into our local DB.
                # _apply_peer_rows handles the zero-row fast path internally
                # so we don't branch here.
                await self._apply_peer_rows(peer_name=peer_name, rows=rows)
                # Same-ms tail drain after ANY full batch. If the peer
                # returned a full batch, there may be more rows sharing
                # the LAST row's ``updated_at_ms`` past our LIMIT — see
                # ``_drain_same_ms_tail`` for the full edge analysis.
                if len(rows) >= batch_size:
                    last_ms = int(rows[-1][5])
                    last_key = str(rows[-1][0])
                    drained = await self._drain_same_ms_tail(
                        peer_db_path=peer_db_path,
                        scope_sql=scope_sql,
                        scope_params=scope_params,
                        batch_size=batch_size,
                        ms=last_ms,
                        last_key=last_key,
                    )
                    if drained:
                        await self._apply_peer_rows(peer_name=peer_name, rows=drained)
                        # Include the drained rows in the returned dict
                        # alongside the main pull so callers (tests) see
                        # the full per-peer sync result of this cycle.
                        pulled[peer_name] = rows + drained
                # Advance cursor on success. Zero rows → no change; rules
                # live in ``_advance_cursor``. ``cursor_ms`` is passed so
                # partial-batch advancement can stay monotonic without
                # relying on our local wall clock (peer clocks may drift).
                next_cursor = self._advance_cursor(
                    rows_count=len(rows),
                    batch_size=batch_size,
                    rows=rows,
                    cursor_ms=cursor_ms,
                )
                if next_cursor is not None:
                    self._peer_cursors[peer_name] = next_cursor
            except Exception as exc:
                # Per-peer isolation: log warning, tick error metric,
                # leave cursor untouched. The sync loop itself continues
                # to the next peer — one bad peer cannot break the whole
                # cycle, and a failing peer can recover on next cycle.
                metrics.cache_sync_errors.labels(peer=peer_name).inc()
                await logger.awarning(
                    'cache_peer_sync_failed',
                    category='cache',
                    peer=peer_name,
                    db=peer_db_path,
                    error=str(exc),
                )

        return pulled

    async def _apply_peer_rows(self, *, peer_name: str, rows: list[tuple]) -> None:
        """UPSERT a batch of peer-supplied rows into the local DB via the
        shared LWW SQL, then invalidate memory for the affected keys.

        The incoming tuple order mirrors the SELECT in ``_pull_peer_rows``:
        ``(key, scope, value, size_bytes, created_at_ms, updated_at_ms,
        expires_at_ms, origin_worker_id)`` — which happens to be the exact
        binding order ``LWW_UPSERT_SQL`` expects, so no translation is
        needed.

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
        await asyncio.shield(self._commit_peer_rows(peer_name=peer_name, rows=rows))

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
        await self._writer_db.executemany(LWW_UPSERT_SQL, rows)
        await self._writer_db.commit()

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
