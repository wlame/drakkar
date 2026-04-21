# Handler key/value cache with write-behind SQLite and peer sync

## Overview

Add a framework-provided key/value cache accessible as `self.cache` on every handler. Memory-backed for hot reads, persisted to a per-worker SQLite file (`<worker>-cache.db`) via a periodic write-behind flush, and eventually consistent across workers via a periodic peer-sync loop that pulls from other workers' cache DBs.

**Problem it solves.** Handlers today hand-roll their own caches (e.g. `integration/worker/handler.py`'s `_result_cache` + FIFO eviction + hit/miss counters). Each ad-hoc cache reinvents eviction, metrics, TTL, and has no cross-worker visibility. A framework primitive lets:

- `arrange()` short-circuit tasks with `PrecomputedResult` built from a cache hit
- Multiple workers share computed results (cross-worker cache sync)
- Operators inspect cache state in the debug UI and scrape size via Prometheus

**How it integrates.** Mirrors the shape of the existing `EventRecorder`: per-worker SQLite file in `debug.db_dir`, symlink-based peer discovery, WAL mode, aiosqlite (each connection = one dedicated OS thread so the event loop is never blocked). Peer discovery helper is extracted from recorder and shared. Three periodic loops (flush/sync/cleanup) run on the existing `run_periodic_task` machinery with a new `system=True` flag so they show up in the existing debug UI periodic-tasks view alongside user-defined ones.

## Context (from discovery)

Files and patterns we'll reuse or touch:

- **`drakkar/recorder.py:1166` `EventRecorder.discover_workers()`** — the discovery algorithm we extract and share
- **`drakkar/periodic.py:77` `run_periodic_task()`** — generic async loop wrapper; takes any `coro_fn`, records `periodic_run` events, already integrates with Prometheus. We pass cache loops through it with `system=True`
- **`drakkar/config.py:291` `DebugConfig`** — the config pattern to follow (pydantic `BaseModel`, env_nested_delimiter `__`, graceful fallbacks)
- **`drakkar/config.py:364` `DrakkarConfig.cluster_name`** (+ `cluster_name_env`) — already-existing field. Cache uses the root value; no new cluster field added.
- **`drakkar/config.py:380` `config_summary()`** — where we append the `cache=…` token
- **`drakkar/debug_server.py:710` `/api/debug/periodic`** — we add `system: bool` pass-through from `metadata.system`; template renders a system badge
- **`drakkar/handler.py`** — `BaseDrakkarHandler` / `DrakkarHandler` gain a `cache` attribute (set by the framework at handler construction)
- **`integration/worker/handler.py:115–305`** — the hand-rolled `_result_cache` this feature replaces end-to-end
- **`integration/worker/drakkar.yaml:92–94`** — `redis.cache` sink instance name renamed to `redis.hot_match_cache` to avoid clash with the new top-level `cache:` config section (per `feedback/verbose_naming_in_examples`)
- **`aiosqlite` (verified in `.venv/.../core.py:90`)** — spawns a dedicated `threading.Thread` per `Connection`; SQLite work is off the event-loop thread. Writer/reader split (two connections) prevents `get()` DB fallback from queuing behind `flush`

Project is Python 3.13+, uses `uv`/`ruff`/`ty` per `user/profile`. Pre-1.0 on `polishing` work now on `main` — breaking changes OK per `feedback/breaking_changes_ok`.

## Development Approach

- **Testing approach: TDD (tests first)** per user choice during planning
- Complete each task fully before moving to the next
- Small, focused changes; verify after each per `feedback/verify_each_change`:
  - `uv run pytest tests/<new_file>.py` for the task's tests
  - Periodically `uv run pytest tests/` for full run
  - `uvx ruff check drakkar/ tests/ integration/`
  - `uv run ty check drakkar/`
  - Coverage check at milestones: `uv run pytest tests/ --cov=drakkar --cov-report=term`
- **CRITICAL: every task MUST include new/updated tests** — listed as separate checklist items, not bundled with implementation
- **CRITICAL: all tests must pass before starting the next task** — no exceptions
- Unit tests fully isolated per `feedback/unit_tests_isolated`: `:memory:` aiosqlite, `tmp_path` for filesystem, mocked peer discovery, no docker in `tests/`
- Real-infra scenarios live in `integration/` (docker-compose), touched in Task 17
- Breaking API changes are fine (pre-1.0); no deprecation shims, no `_result_cache` compat layer
- **NO COMMITS from this assistant** per `feedback/commits_policy` — user handles commits manually

## Testing Strategy

- **Unit tests**: required for every task. `:memory:` aiosqlite for DB, `tmp_path` for on-disk scenarios, mocked filesystem for peer discovery
- **Integration tests**: live in `integration/` as docker-compose scenarios. Task 17 rewrites the existing worker demo to use the new cache; a new chaos scenario verifies cross-worker propagation and peer-down resilience
- **Coverage target**: ≥90% on new files; project floor 75% unchanged
- **Lint/type**: ruff + ty must pass on every task before proceeding

## Progress Tracking

- Mark completed items with `[x]` immediately when done (not batched)
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- If scope changes during implementation, update this plan file

## Solution Overview

Three components:

1. **`Cache`** — user-facing class on `handler.cache`. Sync `set`/`peek`/`delete`/`__contains__` (pure memory-dict ops). Async `get(key, *, as_type=None)` (memory → local SQLite fallback, Pydantic-aware revival).
2. **`CacheEngine`** — lifecycle owner. Opens writer + reader SQLite connections, creates symlink, schema-init, launches the three periodic loops (flush/sync/cleanup) via `run_periodic_task(system=True)`, stops on shutdown with final flush drain.
3. **`peer_discovery.discover_peer_dbs()`** — shared helper extracted from recorder; scans `db_dir/*<suffix>.db` symlinks, yields `(worker_name, resolved_path)` pairs. Used by recorder (suffix=`'-live.db'`) and cache (suffix=`'-cache.db'`).

Data model: single table `cache_entries` keyed by `key`, with `scope` (local|cluster|global), `value` (JSON text), `size_bytes`, `created_at_ms`, `updated_at_ms`, `expires_at_ms`, `origin_worker_id`. Two indexes: partial on `expires_at_ms` (cleanup); composite on `(scope, updated_at_ms)` (peer sync cursor).

Concurrency: aiosqlite per-connection thread keeps SQLite off the event loop. Writer connection handles flush/cleanup/sync-UPSERT. Reader connection handles `get()` DB fallback. Peer syncs open ephemeral read-only aiosqlite connections (each gets its own short-lived thread). WAL mode so peer readers don't block our writer.

LWW semantics: `UPSERT ... WHERE excluded.updated_at_ms > cache_entries.updated_at_ms OR (equal AND excluded.origin_worker_id < cache_entries.origin_worker_id)` — identical SQL used by local writes and peer-sync writes, so local and cross-worker paths can't diverge.

**Delete is deliberately local-only.** No tombstones in v1. `delete(key)` removes the row from the local DB (via the next flush), but offers **no propagation to peers** — peers who synced the value keep their copy until it expires via TTL or is overwritten. If a same-key value exists on a peer, it may even be re-pulled into our DB on the next sync cycle (LWW will accept it if its `updated_at_ms` is newer than any local state). This is a documented sharp edge: **use TTL for cross-worker invalidation, not delete.**

Disabled case: `cache.enabled=false` → `handler.cache` is a no-op stub (peek/get return None, set/delete silently discard). Handler code can call cache unconditionally.

Shutdown ordering: `CacheEngine.stop()` runs **before** `EventRecorder.stop()` so the final cache flush can still record its `periodic_run` event to the recorder. The app's shutdown sequence is documented and tested.

Memory bound: when `max_memory_entries` is set, the in-memory dict uses **LRU eviction** — evicted keys fall through to the DB on next read (warms memory again). The DB is the source of truth; eviction cannot lose data. `max_memory_entries=None` (default) means unbounded.

## Technical Details

**Schema** (`cache_entries` table in `<worker>-cache.db`):

```sql
CREATE TABLE cache_entries (
    key               TEXT    NOT NULL PRIMARY KEY,
    scope             TEXT    NOT NULL CHECK(scope IN ('local','cluster','global')),
    value             TEXT    NOT NULL,          -- JSON
    size_bytes        INTEGER NOT NULL,
    created_at_ms     INTEGER NOT NULL,
    updated_at_ms     INTEGER NOT NULL,
    expires_at_ms     INTEGER,                   -- NULL = never
    origin_worker_id  TEXT    NOT NULL
);

CREATE INDEX idx_cache_expires
    ON cache_entries(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
CREATE INDEX idx_cache_scope_updated
    ON cache_entries(scope, updated_at_ms);

PRAGMA journal_mode = WAL;
```

**Config** (new classes in `drakkar/config.py`):

```python
class CachePeerSyncConfig(BaseModel):
    enabled: bool = True
    interval_seconds: float = Field(default=30.0, gt=0)
    batch_size: int = Field(default=500, ge=1)
    timeout_seconds: float = Field(default=5.0, gt=0)
    # Note: peer cluster_name lookup is cached in-process for a hardcoded 300s;
    # not exposed as a config knob in v1 — YAGNI.

class CacheConfig(BaseModel):
    enabled: bool = False
    db_dir: str = ''                                     # empty → fall back to debug.db_dir
    flush_interval_seconds: float = Field(default=3.0, gt=0)
    cleanup_interval_seconds: float = Field(default=60.0, gt=0)
    max_memory_entries: int | None = Field(default=None, ge=1)   # None = unbounded; LRU when set
    peer_sync: CachePeerSyncConfig = Field(default_factory=CachePeerSyncConfig)
```

Gating rules (warn-and-continue, not fail-at-startup):

- `cache.enabled=true` but no `db_dir` anywhere → warning + effective-disable
- `peer_sync.enabled=true` but `debug.store_config=false` → warning + effective-disable of peer sync only

**Handler API**:

```python
class Cache:
    def set(self, key: str, value: Any, *, ttl: float | None = None,
            scope: CacheScope = CacheScope.LOCAL) -> None: ...
    def peek(self, key: str) -> Any | None: ...                   # memory-only
    def delete(self, key: str) -> bool: ...                        # local-only — no peer propagation
    def __contains__(self, key: str) -> bool: ...
    async def get(self, key: str, *, as_type: type | None = None) -> Any | None: ...
```

**Serialization**: JSON-only for v1. Primitives, lists, dicts stored as-is via `json.dumps` / `json.loads`. Pydantic models serialized via `model_dump_json()`; on read, the optional `as_type=SomeModel` kwarg triggers `model_validate()` revival. Deliberately does not support arbitrary-object binary serializers — JSON is portable across worker processes / Python versions, inspectable in the debug UI, and safe for cross-worker transfer.

**Peer sync scope rules** (per peer):

- Peer in same cluster (`cluster_name == self.cluster_name`): pull `scope IN ('cluster','global')`
- Peer in different cluster: pull `scope = 'global'` only
- `scope = 'local'` is never pulled — stays on the originating worker
- `cluster_name` is sourced from the existing `DrakkarConfig.cluster_name` (with `cluster_name_env` fallback); cache does not introduce a new field

**New Prometheus metrics** (in `drakkar/metrics.py`):

- Counters: `drakkar_cache_hits_total{source}`, `drakkar_cache_misses_total`, `drakkar_cache_writes_total{scope}`, `drakkar_cache_deletes_total`, `drakkar_cache_cleanup_removed_total`, `drakkar_cache_flush_entries_total{op}`, `drakkar_cache_sync_entries_fetched_total{peer}`, `drakkar_cache_sync_entries_upserted_total{peer}`, `drakkar_cache_sync_errors_total{peer}`, `drakkar_cache_evictions_total` (from LRU)
- Gauges: `drakkar_cache_entries_in_memory`, `drakkar_cache_bytes_in_memory`, `drakkar_cache_entries_in_db`, `drakkar_cache_bytes_in_db`
- `bytes_in_memory` is maintained as a **running sum** — adjusted on every set/delete/evict/cleanup, never recomputed by walking the dict
- No new histograms — duration comes from existing `periodic_task_duration{name="cache.flush|sync|cleanup"}`

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests, docs — all in-repo
- **Post-Completion** (no checkboxes): user-driven commit reviews, docker-compose manual run, PyPI version bump discussion

## Implementation Steps

### Task 1: Shared peer discovery helper

**Files:**
- Create: `drakkar/peer_discovery.py`
- Create: `tests/test_peer_discovery.py`
- Modify: `drakkar/recorder.py`

- [x] write tests in `tests/test_peer_discovery.py` first (TDD):
  - finds `*-live.db` symlinks matching suffix in a `tmp_path` db_dir
  - excludes self_worker_name
  - skips broken symlinks (target doesn't exist)
  - handles missing db_dir (returns empty, no raise)
  - handles empty db_dir (returns empty)
- [x] create `drakkar/peer_discovery.py` with `async def discover_peer_dbs(db_dir, suffix, self_worker_name) -> AsyncIterator[tuple[str, str]]` — scans the glob, resolves symlinks, yields `(worker_name, resolved_path)` pairs
- [x] add a short comment explaining the symlink convention and why we resolve via `os.path.realpath` (survives recorder rotation)
- [x] refactor `EventRecorder.discover_workers()` in `drakkar/recorder.py:1166` to use the helper for the symlink-scan portion; keep the recorder-specific `worker_config` read afterward
- [x] run `uv run pytest tests/test_peer_discovery.py tests/test_recorder*.py` — all pass (recorder behavior unchanged)
- [x] `uvx ruff check drakkar/peer_discovery.py tests/test_peer_discovery.py` + `uv run ty check drakkar/peer_discovery.py`

### Task 2: Config classes + `cache=` in config summary

**Files:**
- Modify: `drakkar/config.py`
- Modify: `tests/test_config.py` (or wherever `DrakkarConfig`/`config_summary` tests live — discover at task start)
- Create: `tests/test_cache_config.py`

- [x] write tests in `tests/test_cache_config.py` first:
  - defaults: `CacheConfig().enabled is False`, intervals match spec, peer_sync defaults applied
  - env override: `DRAKKAR_CACHE__ENABLED=true` flips the flag
  - nested env: `DRAKKAR_CACHE__PEER_SYNC__INTERVAL_SECONDS=60` sets peer sync interval
  - invalid: `flush_interval_seconds=0` rejected by pydantic; `batch_size=-1` rejected; `max_memory_entries=0` rejected
  - `db_dir` empty stays empty (resolution happens at engine init, not config load)
  - confirm `CacheConfig` has NO `preload_on_start`, `preload_limit`, or `peer_resolution_cache_seconds` fields (those are v1 cuts — enforcement test against future-creep)
- [x] write tests in `tests/test_config.py` for the summary token — **cover both integer and fractional intervals**:
  - `cache=off` when disabled
  - `cache=on:f=3s/s=30s/c=60s` when enabled with defaults (integer-second intervals render cleanly via `:g`)
  - `cache=on:f=0.5s/s=30s/c=60s` when `flush_interval_seconds=0.5` (fractional renders with decimal)
  - `cache=on:f=3s/s=off/c=60s` when peer_sync disabled
  - `cache=on:...max=N` when `max_memory_entries` set, absent otherwise
- [x] add `CachePeerSyncConfig` and `CacheConfig` classes in `drakkar/config.py` after `DebugConfig`
- [x] add `cache: CacheConfig = Field(default_factory=CacheConfig)` to `DrakkarConfig`
- [x] extend `config_summary()` at `drakkar/config.py:380` — add `cache_part` block between `debug_part` and `metrics_part`, use `:g` float format
- [x] run `uv run pytest tests/test_cache_config.py tests/test_config.py` — all pass
- [x] `uvx ruff check drakkar/config.py tests/test_cache_config.py`

### Task 3: Cache models + serialization

**Files:**
- Create: `drakkar/cache.py`
- Create: `tests/test_cache_models.py`

- [x] write tests in `tests/test_cache_models.py`:
  - `CacheScope` has exactly `LOCAL`/`CLUSTER`/`GLOBAL` with string values `'local'`/`'cluster'`/`'global'`
  - `CacheEntry` dataclass: required fields, `size_bytes` populated at construction from serialized-length
  - `_encode(primitive)` returns JSON string + size; `_encode(pydantic_model)` calls `model_dump_json()`
  - `_decode(json_str, as_type=None)` returns parsed value; `_decode(json_str, as_type=PydanticModel)` returns model instance via `model_validate`
  - `_decode` with `as_type=None` on a dict-shaped JSON returns a plain dict (no magic revival)
- [x] create `drakkar/cache.py` with `CacheScope` (str Enum), `CacheEntry` dataclass, `_encode`/`_decode` helpers
- [x] add a short comment on `_encode` explaining the JSON-only choice (portable across workers, inspectable in debug UI, avoids the security and version-fragility of binary serializers for peer-synced data)
- [x] run `uv run pytest tests/test_cache_models.py` — all pass
- [x] `uvx ruff check drakkar/cache.py tests/test_cache_models.py`

### Task 4: Cache API (memory-only, with LRU) — no DB yet

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_api.py`

- [x] write tests in `tests/test_cache_api.py` for memory-only behavior:
  - `set` stores in memory, `peek` returns value
  - `peek` returns None for missing key
  - `peek` returns None for expired entry (and opportunistically evicts)
  - `delete` removes from memory, returns True when present, False when absent
  - `delete` marks a pending `Op.DELETE` in dirty tracking
  - `delete` is **local-only**: no peer-propagation mechanism exists (no tombstone row, no peer send). Asserted by: delete marks dirty with `DELETE` op only — no "send to peer" queue, no cross-worker message. (Real cross-worker behavior tested end-to-end in Task 11/Task 13.)
  - `__contains__` returns True/False correctly; False for expired
  - `set` with each `CacheScope` stores scope in entry
  - `set` with `ttl` sets `expires_at_ms`; no ttl → `expires_at_ms is None`
  - overwriting with `set` updates `updated_at_ms`
  - dirty tracking: `set` marks `Op.SET`; overwriting keeps latest
  - **LRU when `max_memory_entries=N` is set:**
    - setting `N+1` entries evicts the least-recently-used (the first one, since `set` touches recency)
    - `peek(key)` bumps key recency (moves to MRU position)
    - evicted entries do not lose their dirty-tracking entry (still flushed to DB)
    - `drakkar_cache_evictions_total` metric incremented
    - `max_memory_entries=None` → no eviction, dict grows unbounded
- [x] in `drakkar/cache.py`, add `Cache` class with:
  - `_memory: OrderedDict[str, CacheEntry]` (for LRU), `_dirty: dict[str, DirtyOp]`
  - `_origin_worker_id`, `_max_memory_entries`
  - `set`, `peek`, `delete`, `__contains__` — all sync, GIL-safe dict ops only
  - `peek` and `get` (Task 9) bump LRU position via `_memory.move_to_end(key)`
  - `_maybe_evict()` private method called at end of `set` — pops LRU if over cap; evictions do NOT remove the dirty entry (flush still needs to persist the value)
  - expiration check inline in `peek` / `__contains__` (entry can expire between cleanup cycles)
- [x] short comment on the dirty-tracking pattern explaining the upcoming atomic-swap in flush
- [x] short comment on eviction-keeps-dirty: "Evicting from memory is safe because the DB is the source of truth; but we must not drop the dirty op — otherwise the set would be lost before flush persists it."
- [x] run `uv run pytest tests/test_cache_api.py tests/test_cache_models.py` — all pass
- [x] ruff + ty clean

### Task 5: `run_periodic_task` + recorder gain `system` flag

**Files:**
- Modify: `drakkar/periodic.py`
- Modify: `drakkar/recorder.py`
- Modify: `tests/test_periodic.py`
- Modify: `tests/test_recorder.py` (or wherever `record_periodic_run` tests live)

- [x] write tests in `tests/test_periodic.py` (extend existing):
  - `run_periodic_task(system=True, ...)` passes `system=True` through to `recorder.record_periodic_run`
  - default `system=False` preserves existing behavior
- [x] write tests for recorder:
  - `record_periodic_run(name=..., duration=..., status=..., system=True)` stores `"system": true` in the event's `metadata` JSON
  - `system=False` (default) **OMITS the `system` key entirely** from metadata — keeps existing event rows byte-identical, no schema diff for rows written before this change
- [x] add `system: bool = False` kwarg to `run_periodic_task()` in `drakkar/periodic.py:77`
- [x] add `system: bool = False` param to `EventRecorder.record_periodic_run()`; merge into metadata JSON **only when True** (omit when False)
- [x] short comment on the flag explaining it distinguishes framework-internal loops from user `@periodic` handler methods; comment on omit-when-false preserves backwards-compat of existing event rows
- [x] run `uv run pytest tests/test_periodic.py tests/test_recorder*.py` — all pass
- [x] ruff + ty clean

### Task 6: SQLite schema + writer connection + start/stop lifecycle

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_engine_lifecycle.py`

- [x] write tests in `tests/test_cache_engine_lifecycle.py`:
  - `CacheEngine.start()` with `tmp_path` db_dir opens an aiosqlite writer connection, creates the `<worker>-cache.db` file + `<worker>-cache-live.db` symlink
  - schema applied: `cache_entries` table exists with correct columns + both indexes
  - `PRAGMA journal_mode = WAL` set
  - `start()` is idempotent (calling twice doesn't re-create table or error)
  - `stop()` closes the writer connection cleanly
  - `start()` with `db_dir=''` (unresolved) logs warning and marks engine as effectively disabled
  - schema init is a no-op against an existing DB (subsequent startups pick up existing data)
- [x] add `CacheEngine` class skeleton in `drakkar/cache.py` with:
  - `__init__(config, debug_config, worker_id, cluster_name, recorder)` storing refs
  - `async start()` — open writer `aiosqlite.Connection`, set WAL pragma, CREATE TABLE IF NOT EXISTS + indexes, ensure symlink
  - `async stop()` — close writer connection (flush loop added in Task 8)
  - `_resolve_db_dir()` helper — uses `config.db_dir or debug_config.db_dir`; returns empty on miss
  - a module-level constant `LWW_UPSERT_SQL` placeholder (populated in Task 7)
- [x] short comment on the symlink pattern — same convention as recorder's `*-live.db`
- [x] run `uv run pytest tests/test_cache_engine_lifecycle.py` — all pass
- [x] ruff + ty clean

### Task 7: Flush loop with LWW UPSERT helper

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_flush.py`

- [x] write tests in `tests/test_cache_flush.py`:
  - `_flush_once()` with one SET op inserts a row via LWW UPSERT
  - `_flush_once()` with one DELETE op removes existing row
  - batch: N sets + M deletes → exactly N+M rows touched in one transaction (assert via event log or connection spy)
  - atomic dirty swap: a `set` landing during flush (simulated by writing to `_dirty` mid-flush via a mocked executemany) goes to fresh dict and is picked up next cycle
  - LWW UPSERT: pre-existing row with newer `updated_at_ms` than incoming → incoming rejected (row unchanged)
  - LWW UPSERT tiebreak: equal `updated_at_ms`, lexicographically smaller `origin_worker_id` wins
  - empty `_dirty` → flush is a no-op (no SQL executed, no metric increments)
  - `size_bytes` populated correctly on UPSERT
- [x] implement `_flush_once` in `CacheEngine`:
  - atomic swap `snapshot, self._dirty = self._dirty, {}`
  - split snapshot into SET ops (rows to UPSERT) and DELETE ops (keys to DELETE)
  - one `executemany` per op type inside a single transaction
  - emit `drakkar_cache_flush_entries_total{op}` counters
- [x] populate `LWW_UPSERT_SQL` module constant with the UPSERT statement using the `WHERE excluded.updated_at_ms > ... OR (equal AND origin_worker_id <)` guard
- [x] short comment on `LWW_UPSERT_SQL`: "Identical SQL used by local flush (Task 7) and peer sync (Task 12). Single source of truth for LWW semantics."
- [x] short comment on atomic swap: "Tuple assignment swaps dirty→snapshot atomically under GIL. Any `set` landing during flush goes to the fresh `_dirty` dict and is picked up next cycle — no writes lost."
- [x] run `uv run pytest tests/test_cache_flush.py` — all pass
- [x] ruff + ty clean

### Task 8: Register flush loop as system periodic + final drain on stop

**Files:**
- Modify: `drakkar/cache.py`
- Modify: `tests/test_cache_flush.py`

- [x] extend `tests/test_cache_flush.py`:
  - `CacheEngine.start()` launches a task for `cache.flush` via `run_periodic_task(name='cache.flush', ..., system=True)` (assert task registration against a spy)
  - `CacheEngine.stop()` performs one final `_flush_once` before closing the writer connection (tested: put entry in `_dirty`, call stop, verify DB has the row)
  - flush loop error → warning logged, existing `periodic_task_runs{name="cache.flush",status="error"}` metric incremented, loop continues (handled by `run_periodic_task`)
- [x] in `CacheEngine.start()`: after schema init, `asyncio.create_task(run_periodic_task(name='cache.flush', coro_fn=self._flush_once, seconds=self._config.flush_interval_seconds, on_error='continue', recorder=self._recorder, system=True))`
- [x] in `CacheEngine.stop()`: cancel the flush task, `await` its completion (which final-drains via `run_periodic_task`'s cancel handling — confirm or adjust), then call one last `_flush_once` defensively, then close connection
- [x] run `uv run pytest tests/test_cache_flush.py tests/test_cache_engine_lifecycle.py` — all pass
- [x] ruff + ty clean

### Task 9: Reader connection + async `get()` with DB fallback

**Files:**
- Modify: `drakkar/cache.py`
- Modify: `tests/test_cache_api.py`

- [x] extend `tests/test_cache_api.py`:
  - `get` memory hit returns value (no DB access — assert via mocked reader connection)
  - `get` memory miss + DB hit returns value AND warms memory (with LRU-bump behavior if cap is set)
  - `get` memory miss + DB miss returns None
  - `get` with `as_type=SomePydanticModel` revives via `model_validate`
  - `get` on expired entry (in DB) returns None (filters via `expires_at_ms`)
  - `get` on expired entry (in memory) returns None and evicts
  - `get` warm-on-read respects `max_memory_entries` — triggers LRU eviction if cap would be exceeded
  - reader connection used only for reads, writer only for writes (assert no cross-contamination via test doubles)
  - concurrent `get` while flush in progress completes without waiting (assert via event ordering in test)
- [x] add a second `aiosqlite` connection (`_reader_db`) in `CacheEngine.start()`; close in `stop()`
- [x] short comment on the writer/reader split: "Two connections so `get()` DB fallback never queues behind a flush/cleanup/sync commit. aiosqlite spawns one worker thread per connection, and WAL mode lets the reader see consistent snapshots during concurrent writes."
- [x] implement `async Cache.get(key, *, as_type=None)` — check memory + expiration → on miss query DB via `_reader_db` → on DB hit deserialize (Pydantic-aware via `as_type`) → insert into memory (with eviction if needed) → return
- [x] run `uv run pytest tests/test_cache_api.py` — all pass
- [x] ruff + ty clean

### Task 10: Cleanup loop

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_cleanup.py`

- [x] write tests in `tests/test_cache_cleanup.py`:
  - `_cleanup_once` deletes DB rows where `expires_at_ms < now_ms`
  - preserves rows where `expires_at_ms IS NULL` (never expire)
  - preserves rows where `expires_at_ms > now_ms`
  - also pops expired entries from `_memory`
  - drops pending-flush ops (`_dirty`) for expired keys — otherwise next flush would re-insert the row we just deleted
  - refreshes DB gauges `drakkar_cache_entries_in_db` + `drakkar_cache_bytes_in_db` (asserted via the Prometheus collector or a gauge-spy)
  - `cache.cleanup` registered as system periodic task via `run_periodic_task(system=True)`
  - cleanup error → warning + continue (via `run_periodic_task` error handling)
- [x] implement `_cleanup_once` in `CacheEngine`; register via `asyncio.create_task(run_periodic_task(name='cache.cleanup', ..., system=True))` in `start()`
- [x] short comment on the dirty-pop: "If an expired entry had a pending set, drop it — otherwise the next flush would revive a row we just deleted."
- [x] run `uv run pytest tests/test_cache_cleanup.py` — all pass
- [x] ruff + ty clean

### Task 11: Peer sync — discovery + single-peer pull with scope filter

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_sync_pull.py`

- [x] write tests in `tests/test_cache_sync_pull.py` (use mocked filesystem + aiosqlite `:memory:` for "peer" DBs):
  - `_sync_once` calls `discover_peer_dbs(db_dir, '-cache.db', self_worker_id)` with the expected args
  - per-peer: resolves cluster_name from cached map; on first encounter reads peer's `-live.db` `worker_config` table
  - peer in same cluster as self → query uses `scope IN ('cluster','global')`
  - peer in different cluster → query uses `scope = 'global'` only
  - pull excludes expired rows (`expires_at_ms IS NULL OR expires_at_ms > now`)
  - pull respects `LIMIT batch_size`
  - peer without `worker_config` table → skipped with warn
  - disabled `peer_sync.enabled=false` at startup → `_sync_once` is a no-op (zero file opens)
  - `debug.store_config=false` at startup → peer sync silently disabled (effective-disable logged once at startup)
- [x] implement `_sync_once` skeleton in `CacheEngine`:
  - short-circuit if peer sync not effectively enabled
  - call `discover_peer_dbs(db_dir, '-cache.db', self_worker_id)`
  - for each peer: resolve cluster_name (cache hit or SELECT from peer's `-live.db`)
  - open ephemeral read-only aiosqlite connection, run scope-aware query, collect rows (no UPSERT yet — Task 12)
- [x] short comment on ephemeral connection: "Read-only URI (`file:…?mode=ro`). aiosqlite spawns a fresh worker thread for this connection — isolates peer-read cost from our writer/reader."
- [x] short comment on in-process cluster_name cache (300s TTL, not config-exposed): "Hardcoded — peer recorder DBs change cluster_name rarely in practice. Follow-up can expose if needed."
- [x] run `uv run pytest tests/test_cache_sync_pull.py` — all pass
- [x] ruff + ty clean

### Task 12: Peer sync — LWW UPSERT to local DB + memory invalidation

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_sync_apply.py`

- [x] write tests in `tests/test_cache_sync_apply.py`:
  - UPSERT uses the same `LWW_UPSERT_SQL` constant as local flush (assert via mock)
  - peer row with older `updated_at_ms` than local → rejected by LWW
  - peer row with newer `updated_at_ms` than local → accepted, local row updated
  - LWW tiebreak: equal timestamp, lexicographic `origin_worker_id`
  - after UPSERT, memory entries for the updated keys are invalidated (popped from `_memory`) — next read re-fetches from DB
  - zero-row peer pull → no UPSERT executed
  - `drakkar_cache_sync_entries_fetched_total{peer}` counts rows pulled; `sync_entries_upserted_total{peer}` counts rows that passed LWW (may be less than fetched)
  - `size_bytes` in upserted rows equals `len(value)` from peer (no re-encoding)
- [x] extend `_sync_once` with the LWW UPSERT step using `LWW_UPSERT_SQL` via the writer connection
- [x] implement memory invalidation: for each key we attempted to UPSERT, `self._cache._memory.pop(key, None)` (note: pop only, don't try to reason about whether UPSERT actually applied — simpler is safer)
- [x] short comment on the memory invalidation: "We don't know per-row whether LWW accepted; simpler is: drop from memory, let next `get` fall through to DB. Cheaper than a SELECT-back-and-compare."
- [x] run `uv run pytest tests/test_cache_sync_apply.py tests/test_cache_sync_pull.py` — all pass
- [x] ruff + ty clean

### Task 13: Peer sync — cursor advancement, error isolation, register as system periodic

**Files:**
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_sync_cursor.py`

- [x] write tests in `tests/test_cache_sync_cursor.py`:
  - per-peer cursor `_peer_cursors: dict[str, int]` starts at 0 on worker startup
  - after successful pull, cursor advances to max `updated_at_ms` of returned rows
  - if rows returned == batch_size, cursor advances to last row's `updated_at_ms` (next cycle picks up remaining)
  - if rows returned < batch_size, cursor advances to now-ish timestamp (no more to fetch)
  - per-peer isolation: if peer A fails, peer B's cursor and pull are unaffected
  - peer error (connection refused / timeout / corrupt DB / missing file) → warning logged with peer name + error, `drakkar_cache_sync_errors_total{peer}` incremented, loop continues to next peer, worker keeps running
  - **cross-worker `delete` behavior:** key deleted locally, then peer has same key with newer `updated_at_ms` → next sync re-pulls the peer's value (LWW). Documents the "delete is local-only" sharp edge as an explicit regression test.
  - `cache.sync` registered as system periodic task via `run_periodic_task(system=True)`
- [x] implement `_peer_cursors` tracking in `_sync_once`; advance correctly per the rules above
- [x] wrap per-peer logic in try/except; emit warn + metric on failure
- [x] register sync loop in `CacheEngine.start()`: `asyncio.create_task(run_periodic_task(name='cache.sync', ..., system=True))`
- [x] short comment on cursor-in-memory choice: "Cursor resets to 0 on worker restart — we re-pull everything from peers once. Bounded by TTL cleanup and LWW rejection, so no runaway work."
- [x] short comment on per-peer try/except: "One bad peer must not break the whole sync cycle — isolation is part of the peer-sync contract."
- [x] run `uv run pytest tests/test_cache_sync_*.py` — all pass
- [x] ruff + ty clean

### Task 14: Prometheus metrics wiring

**Files:**
- Modify: `drakkar/metrics.py`
- Modify: `drakkar/cache.py`
- Create: `tests/test_cache_metrics.py`

- [x] write tests in `tests/test_cache_metrics.py`:
  - `get` memory hit increments `hits_total{source="memory"}` by 1
  - `get` DB hit increments `hits_total{source="db"}` by 1
  - `get` miss increments `misses_total`
  - `set` with each scope increments `writes_total{scope=...}`
  - `delete` increments `deletes_total`
  - LRU eviction increments `evictions_total`
  - cleanup removal increments `cleanup_removed_total` by correct count
  - flush op increments `flush_entries_total{op="set"|"delete"}` with correct counts
  - sync peer success increments `sync_entries_fetched_total{peer=...}` and `sync_entries_upserted_total{peer=...}` (upserted ≤ fetched when LWW rejects some)
  - sync peer error increments `sync_errors_total{peer=...}`
  - memory gauges: `entries_in_memory` and `bytes_in_memory` reflect live state after set/delete/evict — `bytes_in_memory` is a **running sum** (assert no full-dict walk happens on scrape via a spy)
  - `size_bytes` on entry equals `len(serialized_json)` exactly
  - DB gauges refreshed on cleanup cycle (not on hot path)
- [x] add all cache metric definitions to `drakkar/metrics.py` (counters + gauges per spec)
- [x] wire inline increments in `Cache` (set/get/delete/evict) and `CacheEngine` (flush/sync/cleanup)
- [x] `bytes_in_memory` accounting: maintain a running `_bytes_sum: int` on `Cache`; `+= entry.size_bytes` on set, `-= entry.size_bytes` on delete/evict/cleanup; gauge reads from this field
- [x] short comment: "Running sum, adjusted on mutation — Prometheus scrape reads a single int, never walks the dict."
- [x] run `uv run pytest tests/test_cache_metrics.py` — all pass
- [x] ruff + ty clean

### Task 15: Handler integration + app wiring + no-op stub + shutdown ordering

**Files:**
- Modify: `drakkar/handler.py`
- Modify: `drakkar/app.py`
- Modify: `drakkar/cache.py` (add NoOpCache stub)
- Create: `tests/test_handler_cache.py`

- [x] write tests in `tests/test_handler_cache.py`:
  - handler gains a `cache` attribute (not None) after framework wires it
  - when `cache.enabled=false`, `handler.cache` is a `NoOpCache` stub: `peek`/`get` return None, `set`/`delete` discard silently, `__contains__` returns False
  - NoOpCache's `get` is still a coroutine (awaitable) for API parity with real Cache
  - cache is available from inside `arrange`, `on_task_complete`, `on_message_complete`, `on_window_complete`, `on_error` (test each hook sees the same instance)
  - Pydantic roundtrip end-to-end: handler `set(PrecomputedResult(...))` → `get(..., as_type=PrecomputedResult)` returns typed object
  - **shutdown ordering test:** when `DrakkarApp.stop()` runs, `CacheEngine.stop()` is awaited **before** `EventRecorder.stop()`, so the final flush's `periodic_run` event still records. Asserted via mocked stop ordering in a spy.
- [x] add `cache` attribute to `BaseDrakkarHandler` (and inherit on `DrakkarHandler`); framework sets it before first hook is called
- [x] add `NoOpCache` class in `drakkar/cache.py` with the same method signatures, all returning None/False/void (`get` is async, returns None)
- [x] in `drakkar/app.py`: construct `CacheEngine` alongside the recorder during startup (`DrakkarApp.start`) when `cache.enabled`; attach its `Cache` to the handler; schedule `engine.stop()` in shutdown **before** `recorder.stop()`. Otherwise attach `NoOpCache`.
- [x] short comment on the stub rationale: "No-op stub means handlers can call `self.cache.set(...)` unconditionally — no `if self.cache is not None` guards in user code."
- [x] short comment on shutdown order: "Cache engine stops first so its final flush can still record a periodic_run event to the recorder. Stopping recorder first would drop that row."
- [x] run `uv run pytest tests/test_handler_cache.py tests/test_app*.py` — all pass
- [x] ruff + ty clean

### Task 16: Debug UI — cache page + `/api/debug/periodic` system badge

**Files:**
- Modify: `drakkar/debug_server.py`
- Create: `drakkar/templates/cache.html`
- Modify: `drakkar/templates/base.html`
- Modify: `drakkar/templates/debug.html`
- Create: `tests/test_debug_server_cache.py`

- [ ] write tests in `tests/test_debug_server_cache.py`:
  - `GET /debug/cache` renders template with 200 status when cache enabled; 404 or redirect when disabled
  - `GET /api/debug/cache/entries?limit=100&offset=0` returns paginated JSON; defaults: `limit=100` (max 1000), `offset=0`
  - supports `scope` filter (single scope), `prefix` filter (key LIKE prefix%), `expired_only=true` filter
  - `GET /api/debug/cache/entry/{key}` returns decoded value + metadata for existing key; 404 for missing
  - `GET /api/debug/cache/stats` returns gauge snapshot matching live gauge values
  - `GET /api/debug/periodic` surfaces `system: bool` field derived from `metadata.system` for each task (default False when key absent from metadata)
  - nav link to /debug/cache is present in `base.html` only when cache enabled
  - pagination edge cases: `limit=0` → empty, `limit=2000` → clamped to 1000, `offset` beyond total → empty
- [ ] add the four new routes to `drakkar/debug_server.py` reading from the reader aiosqlite connection for listings; pagination via `limit`/`offset` query params with clamp to 1000
- [ ] modify `/api/debug/periodic` endpoint at `drakkar/debug_server.py:710` to pass `metadata.system` through as top-level `system` field (default False when key absent)
- [ ] create `drakkar/templates/cache.html` with header stats + filter bar + entries table + detail side panel
- [ ] modify `drakkar/templates/debug.html` at line 38 area — render a small `[system]` pill next to task names where `system` is true
- [ ] modify `drakkar/templates/base.html` — add "Cache" nav link, conditionally hidden
- [ ] short comment on the reader-connection reuse in API handlers: "Reader connection is shared with Cache.get fallback — no additional thread for UI queries."
- [ ] run `uv run pytest tests/test_debug_server_cache.py tests/test_debug_server*.py` — all pass
- [ ] ruff + ty clean

### Task 17: Integration demo migration

**Files:**
- Modify: `integration/worker/handler.py`
- Modify: `integration/worker/drakkar.yaml`

- [ ] in `integration/worker/handler.py`:
  - remove `_result_cache`, `_cache_hits`, `_cache_misses`, `MAX_CACHE_ENTRIES`, FIFO eviction block at lines 115–117 and 293–305
  - replace `arrange()` cache-check block (lines 202–238) with `self.cache.peek(cache_key)` fast-path + `await self.cache.get(cache_key)` DB fallback
  - replace `on_task_complete()` cache-populate block with `self.cache.set(cache_key, result.stdout, ttl=3600, scope=CacheScope.LOCAL)`
  - rebuild `cache_key` as a string: `f"match|{pattern}|{file_path}|{repeat}"`
  - drop `cache_size` / `cache_hits` / `cache_misses` fields from log-interval output (now via Prometheus)
  - keep the `source: cache|subprocess` task label — still handler-level semantics
- [ ] in `integration/worker/drakkar.yaml`:
  - rename the redis sink instance from `cache` to `hot_match_cache` (so `redis.cache` → `redis.hot_match_cache`)
  - add top-level `cache:` section enabling the framework cache with verbose naming in comments per `feedback/verbose_naming_in_examples`
- [ ] `grep -r "redis\.cache\|redis/cache" integration/ drakkar/ docs/` — confirm no stray references to the old sink instance name remain after rename
- [ ] run `uvx ruff check integration/worker/` + `uv run ty check integration/worker/`
- [ ] **run full regression: `uv run pytest tests/`** — no unit tests should regress from the handler rename or yaml changes
- [ ] *(integration demo has docker-compose smoke tests, not unit tests — docker verification happens in Post-Completion)*

### Task 18: Documentation — dedicated `docs/cache.md` + cross-links

**Files:**
- Create: `docs/cache.md`
- Modify: `mkdocs.yml`
- Modify: `docs/handler.md`
- Modify: `docs/observability.md`
- Modify: `docs/performance.md`
- Modify: `docs/data-flow.md`
- Modify: `docs/index.md`
- Modify: `README.md`

- [ ] create `docs/cache.md` following `feedback/docs_depth` structure:
  - concept + problem it solves
  - mermaid diagram of memory → flush → SQLite → peer sync
  - minimal copy-pasteable handler example using `self.cache` for result memoization
  - API reference: `set`, `peek`, `get`, `delete`, `__contains__`, `CacheScope` enum
  - decision matrix: cache vs real DB sink vs compute-every-time; LOCAL vs CLUSTER vs GLOBAL when to use each
  - edge cases section:
    - cold cache on restart (lazy warm)
    - TTL vs delete
    - peer unreachable
    - cross-cluster visibility
    - **sharp edge (highlighted): `delete(key)` is local-only. Peers who synced the value keep their copy until TTL. For cross-worker invalidation, use TTL, not delete.**
  - consistency model: eventual; LWW by `updated_at_ms` with `origin_worker_id` tiebreak; no tombstones; durability window ≤ flush_interval_seconds
  - LRU eviction explained when `max_memory_entries` is set
  - debug UI walkthrough: cache page, system badge on periodic tasks, metric table
  - cross-links to `handler.md`, `observability.md`, `performance.md`, `integration.md`
- [ ] add `- Cache: cache.md` to `mkdocs.yml` nav (position appropriately, probably after handler.md)
- [ ] in `docs/handler.md`: add "The cache" subsection under `arrange()` with a snippet and link to `cache.md`
- [ ] in `docs/observability.md`: add rows to the metric table for all new cache metrics
- [ ] in `docs/performance.md`: add "Caching" bullet under throughput mitigations, linking to `cache.md`
- [ ] in `docs/data-flow.md`: mention cache interaction at the arrange + on_task_complete phases
- [ ] in `docs/index.md`: add cache feature bullet
- [ ] in `README.md`: add "Cache (optional)" to feature list
- [ ] run `uvx mkdocs build --strict` (or `uv run mkdocs build --strict`) — must pass with zero broken-link or missing-nav warnings
- [ ] *(docs task — no unit tests; `mkdocs build --strict` is the gate)*

### Task 19: Verify acceptance criteria

- [ ] full test run: `uv run pytest tests/ -v`
- [ ] coverage report: `uv run pytest tests/ --cov=drakkar --cov-report=term --cov-report=html` — verify ≥90% on all new `drakkar/cache.py` / `drakkar/peer_discovery.py`; project floor ≥75%
- [ ] ruff clean: `uvx ruff check drakkar/ tests/ integration/`
- [ ] ty clean: `uv run ty check drakkar/`
- [ ] verify every Overview bullet is implemented
- [ ] verify graceful degradation paths: cache disabled, peer sync disabled, no db_dir, `debug.store_config=false`, single peer down, all peers down, LRU cap reached
- [ ] verify Pydantic roundtrip end-to-end (create a `PrecomputedResult`, set, get with as_type, assert equality)
- [ ] verify `cache=` token appears correctly in startup log summary across configurations (cache off, cache on all intervals, cache on sync off, cache on max set)
- [ ] verify the documented "delete is local-only" sharp edge: write a test that syncs key to peer, deletes locally, runs sync, confirms peer's key still present and is re-pulled

### Task 20: Move plan to completed

- [ ] `mkdir -p docs/plans/completed`
- [ ] `mv docs/plans/20260421-handler-cache.md docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational.*

**User-driven (per `feedback/commits_policy`):**
- User reviews and commits the work manually at a cadence of their choosing. Assistant does not create commits.

**Integration / manual verification (in `integration/`):**
- `cd integration && docker-compose up` — bring up the stack with the rewritten worker demo
- Verify workers start, `<worker>-cache.db` files appear under the configured `db_dir`
- Verify debug UI shows: new Cache nav link, cache page renders entries, periodic tasks view shows `cache.flush` / `cache.sync` / `cache.cleanup` with `[system]` badges
- Kill one worker mid-run, verify peers log a warning for the failed sync + `drakkar_cache_sync_errors_total` increments; restart worker, verify sync resumes
- Inspect Prometheus metrics at `/metrics`, confirm all new cache gauges/counters appear
- Exercise the "delete is local-only" sharp edge: set a key with `scope=CacheScope.CLUSTER`, wait for sync to peer, delete locally, confirm peer still has the key — matches documented behavior

**Public API surface (if user decides to release):**
- `drakkar/__init__.py` version bump if this feature goes out as a tagged release — per `feedback/breaking_changes_ok`, a breaking-change minor or major bump is fine; user decides whether to include `redis.cache` → `redis.hot_match_cache` as a breaking note in the changelog since that's a user-facing yaml rename that affects any downstream user following the integration sample

**Optional follow-ups (explicitly out of scope for v1):**
- Tombstones for cross-worker delete propagation (turns "delete is local-only" into "delete propagates")
- Preload on startup (`preload_on_start` + `preload_limit`) — removed from v1 config
- Exposing `peer_resolution_cache_seconds` as a config knob — hardcoded 300s in v1
- Recorder gets a reader connection too (currently single-connection; not a regression, but a nice follow-up)
- Cache_key label emission from `on_task_complete` so debug UI can cross-link entries to trace view
- Custom codec protocol if a non-JSON format ever becomes necessary
