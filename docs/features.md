# Features & Enable Order

Drakkar's optional features depend on each other. This page is the map:
which switch turns what on, what each feature silently needs, and the
order to enable things in as a deployment grows from a minimal worker to
a full-featured fleet.

Per-feature guides: [Configuration](configuration.md) ·
[Config Reference](config-reference.md) · [Observability](observability.md) ·
[Cache](cache.md) · [Input Sources](sources.md) · [Webapp](webapp.md) ·
[Local Databases](local-databases.md).

## The switches

Every feature is optional. A worker can run its main loop only: no API, no UI, no recorder, no autodiscovery.

| Feature | Toggle | Default | What it controls |
|---|---|---|---|
| Metrics exporter | `metrics.enabled` | **on** (`:9090`) | Raw Prometheus endpoint on its own port |
| API server + probes | `api.enabled` | **on** (`:8080`) | The HTTP server: **`/healthz` + `/readyz`** and the peer route |
| Operator UI | `ui.enabled` | **on** (same port) | The pages, the bundle fetch, `/api/v1`, `/ws` and the docs; needs `api.enabled` |
| Message probe endpoint | `ui.probe.enabled` | **on** | `POST /api/v1/debug/probe`: runs caller bytes through the live handler and real executor pool; `false` = 403, independently of `auth_token` |
| Database merge endpoint | `ui.merge_enabled` | **on** | `POST /api/v1/debug/merge`: writes a `merged-<ts>-<token>.db` into `<storage.shared_dir>/merged/` per call, deleted after `recorder.archive.merged_retention` while archiving is on; `false` = 403, independently of `auth_token` |
| Kafka read API | `ui.kafka_read_enabled` | **on** | `GET /api/v1/debug/kafka/*`: pipeline-invisible reads of the configured topics; `false` = 403, independently of `auth_token` |
| Consume pause | `ui.consume_pause.enabled` | **off** | Timed Live-page pause of message intake (auto-resume); off because pausing is production-affecting |
| Flight recorder, memory store | `recorder.enabled` and `ui.enabled` | **on** | Recent events in memory for the UI (`recorder.memory.window`) |
| Flight recorder, durable store | `recorder.enabled`, `recorder.provider` | **on**, `sqlite` | Event history in files in `storage.dir` (`sqlite`) or in a Postgres table (`postgres`) |
| Own storage | `storage.dir` | `/tmp/drakkar` | Where this worker's event history, cache file and crash marker live |
| Recorder archiving | `recorder.archive.enabled` | **on** (provider `sqlite`) | Fold sealed recorder files into `.db.gz` window archives, for the whole cluster (with `autodiscover` and `storage.shared_dir`) or for this worker's own files |
| Decoupled UI bundle | `ui.release.enabled` | **on** | Fetch + serve the drakkar-ui SPA release; with no bundle cached the worker serves its JSON API and WebSocket without pages (page requests answer 503); `ui.enabled: false` turns the UI off |
| Prometheus panel + links | `ui.prometheus.url` | off (empty) | Dashboard deep-links into a Prometheus instance |
| Autodiscover (registry + election) | `autodiscover.enabled`, `autodiscover.provider` | **off** | [Workers find each other](fleet.md) and cluster-scope periodic tasks elect one runner; needs `api.enabled`; provider `filesystem` needs `storage.shared_dir`, the other providers need a connection |
| Cross-worker fleet view | `autodiscover` plus a `storage.shared_dir` every worker mounts | n/a | The workers list, history trace over the cluster's sealed files, cluster-wide database view |
| Handler cache | `cache.enabled` | **off** | LWW key/value store for handler code |
| Cache exchange | `cache.exchange.enabled` | **on** (needs the cache and autodiscover) | Cross-worker cache convergence: each worker pulls the `cluster`/`global` rows of its autodiscover peers over HTTP |
| Offload pool | `threads.offload` | `null` (derived) | [`handler.offload()`](offload.md): CPU-bound hook work off the event loop; always available, the knob only sizes it |
| Blocking-I/O pool size | `threads.io` | `null` (Python's sizing) | Size of asyncio's default `to_thread` executor; `null` keeps `min(32, cpu_count + 4)`: a knob, not a switch |
| Runtime health monitor | `runtime_health.enabled` | **on** | Event-loop lag heartbeat, stall stack sampling, Runtime tab, `runtime_health` events |
| Kafka input source | `sources.kafka.enabled` | **off** | Consumer group, poll loop, `arrange()`, offset commits |
| HTTP input source | `sources.http.enabled` | **off** (`:8090`) | POST, handler pipeline, JSON response |
| Aligned startup | `sources.kafka.startup_align.enabled` | **on** | Fleet restarts converge on one rebalance |
| DLQ | `dlq.enabled` | **on** | Failed messages written to a Kafka topic as DLQ records |

Sources are not optional as a group: **at least one input source must be
enabled**. Both are off by default, so every worker states where its work
comes from. A config with neither fails to load
(`no input source enabled: ...`). See [Input Sources](sources.md).

Sinks are not a toggle either: **at least one sink instance (of any type)
is required when results go to sinks**, that is when the Kafka source is on
or `sources.http.deliver_to_sinks` is true. Such a worker with zero sinks fails
startup. An HTTP-only worker with `deliver_to_sinks: false` (the default)
answers each request with the handler's response and needs no sink. The DLQ is built
whenever the Kafka source is on (topic `{sources.kafka.topic}_dlq`) or
`dlq.topic` is set, and the circuit breaker always wraps sink delivery.

## Dependency rules (and what happens when they're unmet)

Each rule names the failure mode so you can decide what your deployment
needs. A rule that is a config check fails the load with a message that
names the key to change.

1. **Probes need the API server.** `/healthz` and `/readyz` are served
   *only* by the API server. `api.enabled: false` means **no probe
   endpoints at all**: Kubernetes liveness/readiness checks simply
   fail. The worker logs a warning at startup; under Kubernetes, keep the
   API enabled. The UI may be off (`ui.enabled: false`): the probes do not
   depend on it. `ui.enabled` and `autodiscover.enabled` need
   `api.enabled`, and the load fails when it is off.
2. **The recorder and the UI are separate.** The memory store exists to
   serve the UI, so it needs `ui.enabled`. A durable store needs only
   `recorder.enabled`. With both off the worker records nothing.
3. **A durable `sqlite` recorder needs a writable `storage.dir`.** The worker
   creates it (mode `0700`) when it is missing. An empty `storage.dir`
   with `recorder.enabled` and provider `sqlite` fails the load; set
   `recorder.enabled: false` to keep recent events in memory only. With
   provider `postgres`, `storage.dir` may be empty. An empty `storage.dir` also
   disables the OOM watchdog (`{storage.dir}/{worker}.watchdog`).
4. **The SPA bundle needs the UI.** `ui.release.enabled` fetches
   and serves the drakkar-ui release; a fetch failure is non-fatal, and
   release tags are immutable, so a bundle downloaded once serves from the
   shared cache on every later start, offline ones included. There is no
   HTML fallback in the package: with nothing cached the worker serves its
   JSON API and WebSocket without pages, and page requests answer **503** naming how to supply a
   bundle. For an air-gapped deployment, stage one with
   `drakkar-ui fetch --version=vX.Y.Z` or point `ui.release.repo` at an
   internal mirror.
5. **The workers list needs autodiscover; cross-tracing needs a SHARED
   folder.** Workers find each other through [autodiscover](fleet.md).
   It needs `autodiscover.provider` and `api.enabled` and, for the
   `filesystem` provider, a `storage.shared_dir` all workers see. Every worker
   seals its finished recorder files into
   `<storage.shared_dir>/history/<cluster>/`, and a history trace reads the sealed
   files there (see
   [Local Databases](local-databases.md#sealing-and-the-history-folder));
   every worker must see the same shared folder (the mount path may
   differ: no file stores an absolute path). With autodiscover off each
   worker lists only itself. Set `api.public_url` per worker so the card and
   cross-links carry a reachable address instead of the bind IP.
6. **The cache engine needs `storage.dir` too.** The cache file lives
   there, private to the worker; with `storage.dir` empty the cache runs
   memory-only (WARN `cache_engine_disabled_no_storage_dir`) and the cache
   exchange turns itself off.
7. **Cache exchange needs autodiscover and reachable API ports.** A worker
   pulls only the peers that [autodiscover](fleet.md) lists, at the
   URL on each card. With autodiscover off the exchange does nothing and
   the worker logs `cache_exchange_off_no_autodiscover` once at startup.
   Every worker must also reach the API port of its peers: across hosts, bind
   `api.host` to a reachable address or set `api.public_url`. The cache file
   itself is never shared.
8. **The webapp needs handler support, checked at construction.** A
   worker with `sources.http.enabled` and a handler that lacks the HTTP
   hooks (`arrange_http_request` + `on_http_request_complete` plus the
   3rd/4th generic models) fails fast with a `ConfigurationError`. A
   webapp **bind** failure at startup is fatal too: the webapp is an
   input source, so a worker that cannot bind its socket stops rather
   than run with nothing to read. An API-server bind failure is fatal as
   well.
9. **The webapp's dashboard tile needs the UI + recorder.** The tile on
   the operator dashboard reads request counts from recorder events; UI
   off = no tile, recorder off = counts from the memory window only.
10. **The Prometheus panel needs `ui.prometheus.url`.** Unset = the
    dashboard links section is hidden. The optional
    `ui.prometheus.worker_label` / `ui.prometheus.cluster_label` /
    `ui.custom_links` keys refine it.
11. **At least one source must be enabled.** `sources.kafka.enabled` and
    `sources.http.enabled` both default to `false`; a config with neither
    fails to load with `no input source enabled: set
    sources.kafka.enabled or sources.http.enabled`. Handler validation
    follows the enabled sources: `arrange()` is required only with the
    Kafka source, the HTTP hooks only with the HTTP source.
12. **The DLQ needs the Kafka source or an explicit `dlq.topic`.** The
    default DLQ topic is derived from the source topic, so a worker with
    `sources.kafka.enabled: false` and an empty `dlq.topic` builds no DLQ
    producer at all. DLQ sends are then dropped, warned once, and counted
    in `drakkar_dlq_unconfigured_drops_total`. Set `dlq.topic` when an
    HTTP-only worker must not lose failed payloads
    ([the rule](sources.md#the-dlq-without-the-kafka-source)).
13. **Cluster-scope periodic tasks need autodiscover.** A task declared
    with `scope='cluster'` and autodiscover off fails startup.
14. **A consumer names a declared connection.** The Kafka source, every
    Kafka, Postgres, Mongo and Redis sink, the DLQ, the Postgres recorder and
    the `consul`, `postgres` and `redis` autodiscover providers read their
    service from `connections.<kind>.<name>`; a missing entry fails the load.

## Enable order: minimal to full-featured

Each tier only depends on the tiers before it. Start at 0, stop at the
tier your deployment needs.

### Tier 0: choose your sources, then a minimal pipeline

Start here: decide where work comes in. Everything below builds on that
choice. This tier shows the Kafka source; for HTTP, or for both, see
[Input Sources](sources.md).

```yaml
connections:
  kafka:
    main:
      brokers: kafka:9092
sources:
  kafka:
    enabled: true
    topic: input-events
executor:
  binary_path: /app/process
sinks:
  kafka:
    results:
      topic: output-events
recorder:
  enabled: false          # no durable history
api:
  enabled: false          # NOTE: no /healthz or /readyz in this tier!
ui:
  enabled: false
```

Kafka in, subprocesses, sinks out, metrics on `:9090`. Suitable for
one-off local runs; **not** for Kubernetes (rule 1).

### Tier 1: operability (UI, probes, history)

```yaml
api:
  enabled: true            # default
ui:
  enabled: true            # default
  auth_token: "${UI_TOKEN}"
storage:
  dir: /var/lib/drakkar
```

Adds the API server with `/healthz` + `/readyz`, the operator UI,
persistent event history, and the OOM watchdog. This is the sensible
production baseline.

### Tier 2: the real UI + Prometheus links

```yaml
ui:
  release:
    enabled: true          # default: fetches the drakkar-ui SPA
  prometheus:
    url: "http://prometheus:9090"
api:
  public_url: "http://worker-1.internal:8080"
```

The worker serves the versioned drakkar-ui SPA (shared per-host cache,
managed with the `drakkar-ui` CLI) and the dashboard gains Prometheus
deep-links.

### Tier 3: fleet view (multi-worker)

```yaml
storage:
  shared_dir: /shared/drakkar   # the same volume on EVERY worker (any mount path)
autodiscover:
  enabled: true
  provider: filesystem
cluster:
  name: search-cluster
```

Workers discover each other, the UI's worker switcher fills in, and
tracing follows a message across workers. Any worker that implements the
on-disk format can share the directory ([spec](local-databases.md)).

### Tier 4: handler cache + cache exchange

```yaml
cache:
  enabled: true
  # the cache file lives in storage.dir, private to this worker
```

`self.cache` becomes persistent and converges across the cluster: each
worker pulls the `cluster` and `global` rows of the peers it finds through
the Tier 3 autodiscover, over HTTP, and merges them by last-write-wins (see
[Cache exchange](cache.md#cache-exchange)). The workers must reach each
other's API port.

### Tier 5: synchronous HTTP ingress

```yaml
sources:
  http:
    enabled: true
    clients:
      - name: search-service
        token: "${WEBAPP_TOKEN}"
        rpm: 120
```

POST requests flow through the same handler pipeline with per-client
auth, rate limits, and the `max_body` cap. Requires the HTTP hooks
in your handler (rule 8). Turn `sources.kafka.enabled` off in the same
config and the worker serves HTTP only.

## Quick sanity checklist

- Kubernetes? `api.enabled: true` (rule 1), probes pointed at the API
  port.
- More than one worker? `autodiscover` on, one shared folder on every worker (rule 5), `api.public_url` set per worker.
- Cache not syncing? Check that the peers appear in the workers list
  (rule 7), that `drakkar_fleet_peer_requests_total` shows `ok` requests
  (otherwise the API ports are not reachable at the card URLs), and that
  both workers are in the same cluster for `cluster`-scoped keys.
- Token set and the UI shows other workers as unreachable? List every
  worker origin in `ui.allowed_origins`
  ([Cross-origin reads](security.md#cross-origin-reads)).
- Worker will not start with `no input source enabled`? Turn on
  `sources.kafka.enabled`, `sources.http.enabled`, or both (rule 11).
- HTTP-only worker losing failed payloads? Set `dlq.topic`; the derived
  default needs a Kafka source (rule 12).
- Air-gapped? Stage the bundle once with
  `drakkar-ui fetch --version=vX.Y.Z` into a cache the workers share, or
  point `ui.release.repo` at an internal mirror. `ui.release.enabled: false`
  fetches nothing and substitutes nothing: the worker then serves its JSON
  API and WebSocket without pages. `ui.enabled: false` turns the UI off.
