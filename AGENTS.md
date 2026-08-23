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

It is the reference implementation of a three-repo product:

| repo | role |
|---|---|
| `drakkar` (this) | Python reference implementation, published as `py-drakkar` |
| `drakkar-go` | Go implementation; byte-parity where contractual |
| `drakkar-ui` | the one web UI both backends serve — a versioned SPA on GitHub Releases |

Both backends implement one JSON/WS contract under `/api/v1`; the normative
spec is `drakkar-ui/docs/api-contract-v1.md`. Any API-surface change updates
that document and lands on both backends.

## Cross-backend parity (the most important invariant)

These surfaces are contractual with `drakkar-go` — change them only in lockstep:

- config format (YAML keys, defaults, `DK_` env overrides) and the
  config-summary one-liner
- DLQ JSON byte-stability, metric names, emitted Postgres SQL, emitted Redis
  commands
- mapping-derived arguments are sorted (Postgres columns, Redis hset fields /
  zadd members) because Go decodes payloads into orderless maps;
  caller-supplied lists keep their order
- the `/api/v1` shapes, and the SQLite schemas both backends read
  (`tests/test_cross_backend_db.py` pins interop against fixtures from the Go
  repo's `just gen-db-fixtures`)
- framework datetimes in JSON use one canonical format:
  `drakkar.format_rfc3339_micro` — never `isoformat()` on framework values

Known deliberate divergences from Go are documented in `docs/configuration.md`
(Kafka library differences) and in code comments at the point of divergence.

## Commands

```bash
just test               # unit suite (uv run pytest); pass pytest args through
just cover              # the gate: 95% coverage floor — new code without tests trips it
just ci                 # exactly what CI runs: fmt-check, lint, typecheck, cover
just check              # ci + strict docs build
just integration-up     # docker harness (Kafka, all sinks, workers + load gen)
just drakkar-ui where   # UI-bundle cache CLI (mirrors the Go repo's)
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
   would lose them).
4. `_on_revoke` blocks until the drain commits. librdkafka waits on the
   rebalance callback; returning early re-opens the duplicate-delivery window.
   The wait must stay bounded by `executor.drain_timeout_seconds`.
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
user cache dir (`~/.cache/drakkar/ui/<tag>/`) — the path is byte-identical to
Go's `os.UserCacheDir` on Linux, so co-located workers of both backends share
one download. The concurrency invariant: a valid cached bundle is never
deleted or replaced (`fetch.py`); racing workers converge on the first
installer's copy. Resolution is never fatal — the ladder ends at the release
embedded as package data (`just embed-ui vX.Y.Z` refreshes
`drakkar/uihost/bundle/`, which ships in the wheel). The `drakkar-ui` console
script mirrors the Go repo's CLI command-for-command over the same cache.

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
  templates/               built-in fallback UI (Jinja)
  config.py                pydantic-settings config (YAML + DK_ env overrides)
integration/               docker-compose harness
docs/                      mkdocs documentation site
```
