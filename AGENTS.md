# AGENTS.md — Drakkar (Python)

Orientation for LLM coding agents. Read this before deriving structure from
source. Human-facing docs live in `docs/` (mkdocs site); this file is the
condensed agent map.

## What this is

A **Kafka → subprocess-pool → sinks** orchestration framework: poll messages,
`arrange()` them into tasks run by a managed subprocess pool, deliver results
to pluggable sinks (Kafka, Postgres, Mongo, Redis, HTTP, files), and commit
offsets at-least-once on a per-partition watermark. This is the **reference
implementation** — the most polished one — of a three-repo product:

| repo | role |
|---|---|
| `drakkar` (this) | Python reference implementation, published as `py-drakkar` |
| `drakkar-go` | Go implementation; byte-parity with this repo where contractual (config, DLQ JSON, metric names, config-summary line) |
| `drakkar-ui` | the ONE web UI both backends serve — a versioned static SPA published to GitHub Releases |

Both backends implement one identical JSON/WS contract under **`/api/v1`**;
the normative spec is **`drakkar-ui/docs/api-contract-v1.md`** (the UI repo
owns it as a requirement on backends). Any change to the API surface must
update that document and land on BOTH backends.

Key mental model: the framework owns the control loop and calls *into* user
code (inversion of control). The actual work runs in an **external
subprocess** (`executor.binary_path` + argv); the user's handler only turns
messages into subprocess invocations and subprocess output into sink writes.

## Programming model

Users subclass `BaseDrakkarHandler[In, Out]` (`drakkar/handler.py`) with
Pydantic models as the type parameters (auto de/serialization). `arrange()`
is the one required hook (window of messages → executor tasks); everything
else is optional overrides: `on_task_complete`, `on_message_complete`,
`on_window_complete`, `on_error`, `on_delivery_error`, lifecycle hooks
(`on_startup`, `on_ready`, `pre_shutdown`), periodic tasks, and the webapp
hooks (`arrange_http_request` / `on_http_request_complete` — see
`docs/webapp.md`). `self.cache` is an optional LWW key/value store with peer
sync (`drakkar/cache/`).

## HTTP surfaces (three separate servers, three ports)

| surface | module | default | purpose |
|---|---|---|---|
| UI server | `drakkar/uiserver/` | `:8080` | operator UI + `/api/v1` JSON contract + `/ws` + probes |
| Webapp | `drakkar/webapp/` | `:8090` | opt-in synchronous ingress: one `POST /process` route through the same handler pipeline |
| Metrics | `drakkar/metrics.py` | `:9090` | raw Prometheus exporter (not FastAPI) |

UI-server facts that save reading time:

- Route modules (`routes_pages/live/debug/cache/openapi/spa.py`) are factories
  returning **leaf APIRouters**; `server.py` includes them and then walks the
  routers to register every legacy `/api/...` route under `/api/v1/...` too
  (`register_v1_aliases`). **Do not nest routers behind a combining
  `include_router`** — newer FastAPI includes routers lazily and the alias
  walk (and startup guard) would see no routes. New v1-only endpoints (e.g.
  `GET /api/v1/identity`) are registered directly with the `/api/v1` prefix
  and get no legacy alias.
- `GET /api/v1/identity` reports `backend` ("python"|"go"), `backend_version`,
  `ui_version`, `ui_source` (contract v1.2). `GET /api/v1/openapi.json` serves
  the vendored OpenAPI 3.1 spec (`drakkar/uiserver/openapi.yaml`; canonical
  source `drakkar-ui/docs/openapi-v1.yaml`) and `GET /docs` a self-hosted
  Swagger UI (no CDN) — both auth-gated like their route class.
  `tests/test_openapi_parity.py` pins the served route table to the spec.
- Auth is one optional bearer token for everything (`ui.auth_token`;
  header or `?token=` for downloads/WS); probes and `/ws` self-manage
  (WS close codes 4401/4403).
- The UI server runs on its own thread/event loop; **every read of live
  worker state goes through `dispatch_to_loop(...)`** to the main loop plus a
  dedicated SQLite reader connection. Follow that pattern for any new
  endpoint.
- The Jinja templates (`drakkar/templates/`) are the legacy built-in UI and
  the **UX reference** the SPA was ported from. When `ui.release.enabled`
  resolves a bundle, `create_ui_app(..., ui_root=...)` skips the Jinja page
  routes and mounts the SPA catch-all (`routes_spa.py`: files + History-API
  fallback to `index.html`); `/api*`, `/ws`, probes, and `/debug/download/...`
  keep precedence.

## Decoupled UI hosting (`drakkar/uihost/`) — default ON

At startup (when `ui.enabled`) the worker resolves the latest `drakkar-ui`
GitHub release, caches it under `$XDG_CACHE_HOME/drakkar/ui/<tag>/`
(≈ `~/.cache/drakkar/ui` — byte-identical to Go's `os.UserCacheDir` path on
Linux, so co-located workers of BOTH backends share one download), and serves
it. Config: `ui.release.*` within the merged `ui` section in YAML /
`DK_UI__RELEASE__*` env — identical keys, defaults, and semantics as the Go
backend; change them in lockstep.

Design points every agent should know:

- **Fetch avoids the GitHub REST API**: latest tag via the
  `github.com/<repo>/releases/latest` redirect Location, asset via the
  conventional direct URL (`releases/download/<tag>/drakkar-ui-<tag>.tar.gz`)
  — immune to the anonymous 60 req/h API rate limit. The API is only a
  fallback (private repos via `GITHUB_TOKEN`, renamed assets).
- **Resolution ladder** (never fatal, bounded ~30s, runs in a worker
  thread): cached resolved tag → fetch (with optional `.sha256` sidecar
  verification — binding when the asset exists) → recheck cache (a
  concurrent worker may have installed it) → cached pin → newest cached
  release (unpinned workers only — a pin guarantees contract
  compatibility) → the **release embedded as package data** (`just embed-ui
  vX.Y.Z` refreshes `drakkar/uihost/bundle/` + its VERSION file; identity
  reports `ui_source: "embedded"`). The built-in Jinja pages serve only
  when `ui.release.enabled=false` or resolution errored; they carry a
  "built-in UI" header badge so operators can tell them from a served
  release.
- **Cache management CLI**: the `drakkar-ui` console script
  (`drakkar/uihost/cli.py`, `just drakkar-ui …`) mirrors the Go backend's
  `cmd/drakkar-ui` byte-for-byte (`where` / `fetch --version=vX` /
  `update`; exit codes 0/1/2) over the same shared cache.
- **Shared-cache concurrency invariant**: a valid cached bundle is NEVER
  deleted or replaced (`fetch.py _install_bundle`). Staging dirs use random
  tokens (NOT pid — containerized workers are all pid 1) with a `.incoming`
  suffix that every fallback scan filters out; racing workers converge — one
  installs, the rest discard their copy and serve the winner's.
- Extraction is hardened: zip-slip rejection, symlinks/devices skipped,
  50 MiB download / 100 MiB extracted caps, `index.html`-at-root structural
  validation before the atomic swap.

## Invariants

1. **Contract parity with drakkar-go** — config format (YAML + `DK_` env),
   DLQ JSON byte-stability, metric names, emitted Postgres SQL and emitted
   Redis commands (mapping arguments are SORTED — Postgres columns, Redis
   `hset` fields and `zadd` members — never left in model-declaration or
   dict order, because Go decodes payload data into an orderless map;
   caller-supplied LISTS keep their order), the config-summary one-liner
   (renders `ui=on:8080`; the `ui.release.*` bundle-fetch settings are
   deliberately excluded from it), and the `/api/v1` shapes.
   `kafka.security` / `kafka.client_config` (plus the sink and DLQ
   equivalents) ship on BOTH backends with identical YAML keys, env names,
   defaults, and validation rules; `drakkar/kafka_security.py` and Go's
   `kafka_security.go` are the paired references. Three behaviours differ
   because the Kafka libraries differ (confluent-kafka/librdkafka here,
   franz-go there), all documented in `docs/configuration.md`: Go rejects
   GSSAPI/OAUTHBEARER and `ssl_key_password` at startup, and Go reads
   certificate files at startup while librdkafka fails at first connect.
   Go's `client_config` accepts only keys with a franz equivalent. The
   config-summary line and the recorder schema were deliberately left
   untouched on both sides, so this addition never touched a byte-parity
   surface.
2. **Tooling**: `uv` only (never pip), `ruff` (format + lint, single quotes
   for code / double for user-facing text), `ty` for types, pytest
   (function-based, fixtures, parametrize). Coverage gate **95%**
   (`just cover`) — the suite sits just above it (~95.3%), so the floor is
   a live constraint: new code without tests will trip the gate.
3. **Tests are hermetic** — no real network. UI hosting defaults ON, so test
   fixtures with a real `DrakkarConfig` must set `ui.release.enabled = False`
   (the `mock_app` fixtures do) or use `tests/test_uihost.py`'s `StubGitHub` +
   `ui_config()` helpers.
4. Errors are explicit; never silently swallowed. Structured logging via
   structlog (ECS-compatible).
5. **SQLite files are secured before the driver opens them.** Both stores
   call `drakkar.dbfiles.secure_db_file(path)` *before* connecting —
   SQLite copies the main DB's mode onto the `-wal`/`-shm` sidecars as it
   creates them, so chmod-ing afterwards leaves both sidecars at 0644.
   Never move that call after the connect or after `journal_mode=WAL`.
6. **A partition loop that dies is restarted exactly once**
   (`PARTITION_RESTART_LIMIT`), then marked dead, counted in
   `drakkar_partition_processor_deaths_total`, and surfaced on `/readyz`
   as `partition_<id>_processor_died`. Never swallow an exception in
   `_run` — `_supervise` owns crash handling. A restart resumes
   processing but does NOT recover the crashed window's offsets: they
   stay pending by design (committing past unprocessed messages would
   lose them).
7. **`_on_revoke` blocks until the drain commits.** It is awaited by the
   consumer's rebalance callback, which librdkafka waits on, so returning
   early re-opens the duplicate-delivery window. The wait must stay
   bounded by `executor.drain_timeout_seconds` —
   `run_coroutine_threadsafe(...).result()` has no timeout of its own.
8. **Sink deliveries are batched, and execution order always equals
   payload order.** Postgres batches only with ADJACENT same-shaped
   neighbours — global bucketing would reorder a payload past its
   successor, which is a lost update for `UPDATE` — grouping by op +
   shape (`insert`/`upsert` → one multi-row `VALUES`;
   `update`/`statement` → one `executemany`). Mongo groups by collection.
   Postgres and mongo fall back to per-payload delivery when a batch
   fails, so error attribution stays identical to the Go backend
   (divergence #18); that fallback is safe only because the failed batch
   is atomic and wrote nothing.

   **Redis does NOT fall back.** Its pipeline runs with
   `raise_on_error=False`, which returns one result per queued command
   with per-command errors as VALUES, so the failing payload is named
   positionally and nothing is ever re-sent. Do not reintroduce a replay
   — it would double-apply `INCRBY` and `LPUSH`. A short result list is a
   loud error (`zip(..., strict=True)`) rather than silently dropped
   failures.

   The Kafka sink keeps its per-batch `flush()` — `AIOProducer` buffers to
   `batch_size=1000` / `buffer_timeout=1.0s`, so dropping it would stall
   every delivery on a one-second timer.

## Build / test / run (always via just)

```bash
just test               # full suite (uv run pytest)
just cover              # THE gate: 95% floor
just ci                 # exactly what CI runs: fmt-check lint typecheck cover
just check              # ci + strict docs build
just integration-up     # full docker harness (Kafka, sinks, 3 workers + load gen)
just integration-logs worker-1
just drakkar-ui where   # decoupled-UI cache management CLI (mirrors the Go one)
```

Integration workers expose the UI on `:8081..:8083`; the compose file
mounts the host UI cache so all workers share one bundle download.

## Gotchas

- FastAPI version sensitivity: the v1-alias registration walks
  **router.routes** (leaf routers), not `app.routes` — on FastAPI ≥0.139 the
  app-level table hides included routes. A startup guard raises if the walk
  finds nothing; don't remove it.
- `tests/` pin request-sequence behavior of the fetch engine — the stub
  serves API routes only (`add_release`) or direct-web routes only
  (`add_direct_release`) to prove each path in isolation.
- The webapp has NO health routes; readiness gates are enforced per-request
  (503 envelopes). Kubernetes probes live on the UI server.
- Retry visualization convention: archived task attempts get composite keys
  `task_id:r<start_ts>` — server and SPA both rely on it.
- `pyproject.toml` ships `drakkar/uihost/bundle/**` as package data (a real
  drakkar-ui release + VERSION file); keep it in the wheel.
- Cross-backend DB interop is test-pinned: `tests/test_cross_backend_db.py`
  reads fixture DBs written by the GO engines from `tests/fixtures/go-db/`
  (regenerate with the Go repo's `just gen-db-fixtures`; `just
  gen-db-fixtures` here feeds the Go repo). Framework datetimes in JSON use
  ONE canonical format via `drakkar.format_rfc3339_micro` — never call
  `isoformat()` on framework-controlled values.

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
  uiserver/                UI server: server.py + routes_* + runner (message probe)
  webapp/                  synchronous HTTP ingress pipeline
  uihost/                  drakkar-ui bundle fetch/cache/serve engine
  templates/               legacy Jinja UI (also the SPA's UX reference)
  config.py                pydantic-settings config (YAML + DK_ env overrides)
  kafka_security.py        SASL/TLS config model + librdkafka mapping (leaf module)
integration/               docker-compose harness (Kafka, sinks, workers, load gen)
docs/                      mkdocs documentation site
scripts/                   replay_dlq.py etc.
```
