# Features & Enable Order

Drakkar's optional features depend on each other. This page is the map:
which switch turns what on, what each feature silently needs, and the
order to enable things in as a deployment grows from a minimal worker to
a full-featured fleet. It applies to **both backends** — every toggle,
default, and dependency below is identical in the Python and Go
implementations.

Per-feature guides: [Configuration](configuration.md) ·
[Config Reference](config-reference.md) · [Observability](observability.md) ·
[Cache](cache.md) · [Webapp](webapp.md) ·
[Local Databases](local-databases.md).

## The switches

| Feature | Toggle | Default | What it controls |
|---|---|---|---|
| Metrics exporter | `metrics.enabled` | **on** (`:9090`) | Raw Prometheus endpoint on its own port |
| Operator UI + probes | `ui.enabled` | **on** (`:8080`) | UI server, `/api/v1`, `/ws`, **`/healthz` + `/readyz`**, and the flight recorder |
| Message probe endpoint | `ui.probe_enabled` | **on** | `POST /api/v1/debug/probe` — runs caller bytes through the live handler and real executor pool; `false` = 403, independently of `auth_token` |
| Database merge endpoint | `ui.merge_enabled` | **on** | `POST /api/v1/debug/merge` — writes an unreclaimed `merged-<ts>.db` per call; `false` = 403, independently of `auth_token` |
| Kafka read API | `ui.kafka_read_enabled` | **on** | `GET /api/v1/debug/kafka/*` — pipeline-invisible reads of the configured topics; `false` = 403, independently of `auth_token` |
| Consume pause | `ui.consume_pause.enabled` | **off** | Timed Live-page pause of message intake (auto-resume); off because pausing is production-affecting |
| Recorder persistence | `ui.recorder.db_dir` | `/tmp` | Where event history and worker metadata live; empty = memory-only |
| Recorder archiving | `ui.recorder.archive_enabled` | **on** | Fold rotated-out DB files into per-cluster `.db.gz` window archives; off = raw files are never deleted automatically |
| Decoupled UI bundle | `ui.release.enabled` | **on** | Fetch + serve the drakkar-ui SPA release; with no bundle cached the worker is API-only and page requests answer 503 |
| Prometheus panel + links | `ui.prometheus_url` | off (empty) | Dashboard deep-links into a Prometheus instance |
| Cross-worker fleet view | shared `ui.recorder.db_dir` | — | Worker autodiscovery, workers list, cross-tracing |
| Handler cache | `cache.enabled` | **off** | LWW key/value store for handler code |
| Offload pool | `offload.max_threads` | **on** (2 threads) | [`handler.offload()`](offload.md) — CPU-bound hook work off the event loop; always available, the knob only sizes it |
| Blocking-I/O pool size | `io.max_threads` | `0` (Python's sizing) | Size of asyncio's default `to_thread` executor; `0` keeps `min(32, cpu_count + 4)` — a knob, not a switch |
| Runtime health monitor | `runtime_health.enabled` | **on** | Event-loop lag heartbeat, stall stack sampling, Runtime tab, `runtime_health` events |
| Task cost & throughput | `throughput.cost_label` | **off** (empty) | Naming a numeric task label enables per-task speed and windowed throughput ([Throughput](throughput.md)) |
| Cache peer sync | `cache.peer_sync.enabled` | **on** (when cache on) | Cross-worker cache convergence via the shared directory |
| Synchronous HTTP ingress | `webapp.enabled` | **off** (`:8090`) | POST → handler pipeline → JSON response |
| Aligned startup | `kafka.startup_align_enabled` | **on** | Fleet restarts converge on one rebalance |

Sinks are not a toggle: **at least one sink instance (of any type) is
required** — a worker with zero sinks fails startup. The DLQ is always
built (topic `{source_topic}_dlq`), and the circuit breaker always wraps
sink delivery.

## Dependency rules (and what happens when they're unmet)

The framework prefers degrading over refusing to start — which means
several of these edges are **silent** at startup. Each rule below names
the failure mode so you can decide what your deployment needs.

1. **Probes need the UI server.** `/healthz` and `/readyz` are served
   *only* by the UI server. `ui.enabled: false` means **no probe
   endpoints at all** — Kubernetes liveness/readiness checks simply
   fail. Both backends log the warning `ui_disabled_no_probes` at
   startup; under Kubernetes, keep the UI enabled (auth-token it
   instead of disabling it).
2. **The flight recorder runs only when `ui.enabled`.** UI off = no
   event history, no config snapshot, no state timeline — and none of
   the features below that read them.
3. **Recorder persistence needs a writable `ui.recorder.db_dir`.**
   Empty `db_dir` silently switches to a short in-memory window (lost
   on restart) and disables the OOM watchdog (`{db_dir}/{worker}.watchdog`).
4. **The SPA bundle needs the UI server.** `ui.release.enabled` fetches
   and serves the drakkar-ui release; a fetch failure is non-fatal, and
   release tags are immutable, so a bundle downloaded once serves from the
   shared cache on every later start — offline ones included. There is no
   HTML fallback in the package: with nothing cached the worker runs
   API-only and page requests answer **503** naming how to supply a
   bundle. For an air-gapped deployment, stage one with
   `drakkar-ui fetch --version=vX.Y.Z` or point `ui.release.repo` at an
   internal mirror.
5. **Fleet features need a SHARED `db_dir` + `store_config`.** Worker
   autodiscovery, the workers list, cross-worker links, and cross-worker
   tracing all work by scanning one shared directory for other workers'
   DB files (see [Local Databases](local-databases.md)). Every worker
   must mount it at the **same path**, and `ui.recorder.store_config`
   must stay `true` (default) — a worker without its `worker_config`
   row is invisible to peers. Unshared directories are silent: each
   worker simply sees only itself. Set `ui.public_url` per worker so
   cross-links use a reachable address instead of the bind IP.
6. **The cache engine needs a db_dir too.** `cache.db_dir` falls back
   to `ui.recorder.db_dir`; if both are empty the cache silently runs
   memory-only (WARN `cache_engine_disabled_no_db_dir`) and peer sync
   turns itself off.
7. **Cache peer sync needs `store_config`.** Peers resolve each other's
   cluster from the recorder's `worker_config` row;
   `ui.recorder.store_config: false` silently downgrades peer sync
   (WARN `cache_peer_sync_disabled_no_store_config`). Real cross-worker
   sync additionally needs the shared directory from rule 5.
8. **The webapp needs handler support — checked at construction.** A
   worker with `webapp.enabled` and a handler that lacks the HTTP hooks
   (Python: `arrange_http_request` + `on_http_request_complete` plus the
   3rd/4th generic models; Go: the `drakkar.HTTPHandler` interface)
   fails fast with a `ConfigurationError` on both backends. A webapp
   **bind** failure at startup is non-fatal (the worker continues
   without it); a UI-server bind failure is fatal.
9. **The webapp's dashboard tile needs the UI + recorder.** The tile on
   the operator dashboard reads request counts from recorder events; UI
   off = no tile, memory-only recorder = zeroed counts.
10. **The Prometheus panel needs `ui.prometheus_url`.** Unset = the
    dashboard links section is hidden. The optional
    `ui.prometheus_worker_label` / `ui.prometheus_cluster_label` /
    `ui.custom_links` keys refine it.

## Enable order: minimal → full-featured

Each tier only depends on the tiers before it. Start at 0, stop at the
tier your deployment needs.

### Tier 0 — minimal pipeline

```yaml
kafka:
  brokers: kafka:9092
  source_topic: input-events
executor:
  binary_path: /app/process
sinks:
  kafka:
    results:
      topic: output-events
ui:
  enabled: false          # NOTE: no /healthz or /readyz in this tier!
```

Kafka in, subprocesses, sinks out, metrics on `:9090`. Suitable for
one-off local runs; **not** for Kubernetes (rule 1).

### Tier 1 — operability (UI, probes, history)

```yaml
ui:
  enabled: true            # default
  auth_token: "${UI_TOKEN}"
  recorder:
    db_dir: /var/lib/drakkar
```

Adds the operator UI, `/healthz` + `/readyz`, persistent event history,
and the OOM watchdog. This is the sensible production baseline.

### Tier 2 — the real UI + Prometheus links

```yaml
ui:
  release:
    enabled: true          # default — fetches the drakkar-ui SPA
  prometheus_url: "http://prometheus:9090"
  public_url: "http://worker-1.internal:8080"
```

The worker serves the versioned drakkar-ui SPA (shared per-host cache,
managed with the `drakkar-ui` CLI) and the dashboard gains Prometheus
deep-links.

### Tier 3 — fleet view (multi-worker)

```yaml
ui:
  recorder:
    db_dir: /shared/drakkar   # same volume, same path, EVERY worker
```

Workers discover each other, the UI's worker switcher fills in, and
tracing follows a message across workers. Mixed Python + Go fleets are
supported on the same directory ([spec](local-databases.md)).

### Tier 4 — handler cache + peer sync

```yaml
cache:
  enabled: true
  # db_dir inherits ui.recorder.db_dir — already shared from Tier 3
```

`self.cache` / `h.Cache()` becomes persistent and (thanks to the Tier 3
shared directory) converges across the fleet via LWW peer sync.

### Tier 5 — synchronous HTTP ingress

```yaml
webapp:
  enabled: true
  clients:
    - name: search-service
      token: "${WEBAPP_TOKEN}"
      rpm: 120
```

POST requests flow through the same handler pipeline with per-client
auth, rate limits, and the `max_body_bytes` cap. Requires the HTTP hooks
in your handler (rule 8).

## Quick sanity checklist

- Kubernetes? → `ui.enabled: true` (rule 1), probes pointed at the UI
  port.
- More than one worker? → shared `db_dir`, same mount path everywhere
  (rule 5), `ui.public_url` set per worker.
- Cache not syncing? → check `store_config` (rule 7), the shared
  directory (rule 6), and that both workers are in the same cluster for
  `cluster`-scoped keys.
- Webapp 500s at the first request? → it can't: both backends now
  reject a hook-less handler at startup (rule 8).
- Air-gapped? → stage the bundle once with
  `drakkar-ui fetch --version=vX.Y.Z` into a cache the workers share, or
  point `ui.release.repo` at an internal mirror. `ui.release.enabled: false`
  turns the UI off rather than substituting anything — the worker then
  serves `/api/v1` and the probes only.
