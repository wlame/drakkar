# AGENTS.md — Drakkar (Python)

Orientation for coding agents. Human-facing docs live in `docs/` (mkdocs site);
this file records only what the code cannot tell you: cross-repo contracts,
invariants with non-obvious rationale, and commands.

## What this is

A Kafka → subprocess-pool → sinks orchestration framework: poll messages,
`arrange()` them into tasks run by a managed subprocess pool, deliver results to
pluggable sinks (Kafka, Postgres, Mongo, Redis, HTTP, files), commit offsets
at-least-once on a per-partition watermark. The framework owns the control loop
and calls into user code; the actual work runs in an external subprocess.

The worker serves its own operator UI: `drakkar-ui`, a versioned SPA
published on GitHub Releases and fetched at startup (see "Decoupled UI
hosting" below). The normative description of the JSON/WS surface under
`/api/v1` is `drakkar-ui/docs/api-contract-v1.md`; any API-surface change
updates that document too.

## Wire contracts (the most important invariant)

These surfaces are contracts, not implementation details. Something else
already depends on their exact bytes — a UI release, an operator's SQL, a
dashboard query, a database file on a shared volume — so a change here is a
breaking change, never a refactor:

- config format (YAML keys, defaults, `DK_` env overrides) and the
  config-summary one-liner
- DLQ JSON byte-stability, metric names and help text, emitted Postgres SQL,
  emitted Redis commands, emitted Mongo statements
- mapping-derived arguments are emitted **sorted** (Postgres columns, Redis
  hset fields / zadd members) so the bytes are reproducible from a payload
  whose decoded form carries no key order; caller-supplied lists keep the
  order they were given
- the `/api/v1` shapes, and the SQLite schemas on disk — a worker reads
  recorder and cache files written by other workers sharing its `db_dir`
- framework datetimes in JSON use one canonical format:
  `drakkar.format_rfc3339_micro` — never `isoformat()` on framework values

Deliberate divergences from what a contract seems to imply are documented at
the point of divergence in code comments, and in `docs/configuration.md` for
Kafka library behaviour.

## Commands

```bash
just test               # unit suite (uv run pytest); pass pytest args through
just cover              # the gate: 95% coverage floor — new code without tests trips it
just ci                 # exactly what CI runs: fmt-check, lint, typecheck, cover, docs-build
just check              # ci + the dependency CVE scan
just integration-up     # docker harness (Kafka, all sinks, workers + load gen)
just drakkar-ui where   # UI-bundle cache CLI
```

Tooling: `uv` only (never pip), `ruff` format + lint (single quotes in code,
double for user-facing text), `ty` for types, pytest (function-based, fixtures,
parametrize).

## Invariants

1. Tests are hermetic — no real network, databases, or containers. UI-bundle
   fetching defaults ON, so fixtures building a real `DrakkarConfig` must set
   `ui.release.enabled = False` (the `mock_app` fixtures do) or use
   `tests/test_uihost.py`'s `StubGitHub` helpers.
2. SQLite files are secured before the driver opens them: both stores call
   `drakkar.dbfiles.secure_db_file(path)` before connecting, because SQLite
   copies the main DB's mode onto the `-wal`/`-shm` sidecars it creates —
   chmod after connect leaves sidecars world-readable.
3. A partition loop that dies is restarted exactly once, then marked dead and
   surfaced on `/readyz`. `_supervise` owns crash handling — never swallow an
   exception in `_run`. A restart does not recover the crashed window's
   offsets: they stay pending by design (committing past unprocessed messages
   would lose them). A dead processor is excluded from every drain: nothing
   will empty its queue, so waiting on one spends the whole budget and
   suppresses the other partitions' final commits with it.
4. `_on_revoke` blocks until the drain commits. librdkafka waits on the
   rebalance callback; returning early re-opens the duplicate-delivery window.
   The whole teardown — drain, final commit, `stop()` — runs against one
   `executor.drain_timeout_seconds` deadline; a step that takes its own budget
   lets the callback overrun `max.poll.interval.ms` and the member is evicted.
   A drain that expires cancels the tasks it waited for. Leaving them running
   holds an executor slot and a subprocess until `task_timeout_seconds` and
   keeps `_run` from ever leaving its drain loop, because a cancelled task's
   offsets stay pending (invariant 3) and nothing settles them.
5. Sink delivery order always equals payload order, and failed batches never
   replay:
   - Postgres batches only adjacent same-shaped payloads (global bucketing
     would reorder an UPDATE past its successor — a lost update). Per-payload
     fallback after a failed batch is safe only because the failed batch wrote
     nothing.
   - Redis pipelines with `raise_on_error=False` and names the failing payload
     positionally. No replay — it would double-apply INCRBY/LPUSH.
   - Mongo sends one ordered `bulk_write` per collection run;
     `writeErrors[*].index` names the failing payload. No replay — PyMongo
     writes generated `_id`s back into documents, so a replay raises
     duplicate-key on an innocent document (this retired an `_id`-stripping
     workaround; do not reintroduce either half).
   - The Kafka sink and the DLQ sink keep their per-write `flush()` — the
     producer buffers up to 1s, so dropping it stalls every delivery. Both
     also check the delivery report: a failed delivery arrives in
     `Message.error()`, it is not raised.
6. Errors are explicit, never silently swallowed. Structured logging via
   structlog, ECS-compatible.

## UI server footguns

- The UI server (`drakkar/uiserver/`) runs on its own thread and event loop.
  Every read of live worker state goes through `dispatch_to_loop(...)` to the
  main loop, plus a dedicated SQLite reader connection. Follow that pattern
  for any new endpoint.
- Route modules are factories returning leaf APIRouters; `server.py` walks
  them to register `/api/v1/...` aliases. Do not nest routers behind a
  combining `include_router` — on FastAPI ≥ 0.139 included routes are hidden
  from the app-level table and the alias walk (a startup guard raises if the
  walk finds nothing).
- Three separate HTTP servers: UI server `:8080`, opt-in webapp `:8090`
  (synchronous ingress, no health routes — readiness is per-request),
  Prometheus exporter `:9090` (not FastAPI). Kubernetes probes live on the UI
  server only.

## Decoupled UI hosting (`drakkar/uihost/`)

Workers fetch the `drakkar-ui` release at startup and cache it under the
user cache dir (`~/.cache/drakkar/ui/<tag>/`), so co-located workers share one
download rather than each keeping their own copy. The concurrency invariant: a valid cached bundle is never
deleted or replaced (`fetch.py`); racing workers converge on the first
installer's copy. Resolution is never fatal, but it can end with nothing:
the wheel carries no bundle, so a worker with an empty cache and no
reachable release source runs API-only and answers page requests with 503
(`uiserver/routes_spa.py`). For an air-gapped deployment, stage a bundle
with `drakkar-ui fetch --version=vX.Y.Z` or point `ui.release.repo` at an
internal mirror.

## Directory map

```
drakkar/
  app.py, lifecycle.py     DrakkarApp wiring + startup/shutdown ordering
  handler.py, models.py    user extension points + typed messages
  consumer.py, partition.py, offsets.py   poll loop, per-partition pipeline, watermarks
  executor.py              subprocess pool (argv exec, process-group kill)
  sinks/                   sink implementations + DLQ + circuit breaker
  cache/                   LWW SQLite cache + peer sync
  recorder/                flight recorder (SQLite event log)
  uiserver/                UI server: routes + message probe runner
  webapp/                  synchronous HTTP ingress pipeline
  uihost/                  drakkar-ui bundle fetch/cache/serve engine
  config/                  pydantic-settings config (YAML + DK_ env overrides),
                           split per domain and re-exported from the package
integration/               docker-compose harness
docs/                      mkdocs documentation site
```
