# Webapp

An optional synchronous-HTTP entry point that exposes the same handler
pipeline used for Kafka. When `webapp.enabled=true`, Drakkar runs a
FastAPI server on a dedicated thread accepting POST requests, parses the
body into a user-defined Pydantic model, dispatches the request to the
main pipeline loop, and returns a JSON response built from the executor
fan-out.

The webapp is **opt-in** (`webapp.enabled=false` by default). It coexists
with the Kafka source pipeline rather than replacing it -- the same
worker can consume from Kafka and serve HTTP requests at the same time.

---

## When to use the webapp vs the Kafka source path

| Question | Choose... |
|----------|----------|
| "Does my caller need a synchronous response in one round-trip?" | **Webapp.** Kafka's at-least-once delivery is one-way; a synchronous reply needs HTTP (or a request/reply broker the framework does not provide). |
| "Is my workload high-throughput, restart-resilient, with persistent retries?" | **Kafka source.** Webapp requests live for one HTTP round-trip and are dropped if the worker restarts mid-flight; Kafka offsets re-deliver after a crash. |
| "Do I need per-tenant authentication and rate limits at the worker?" | **Webapp.** Auth + rpm is built in. The Kafka path has no per-producer admission control. |
| "Is the pipeline logic identical for HTTP and Kafka inputs?" | **Both, on the same worker.** Reuse `arrange()` / `arrange_http_request()` and share the executor pool, sinks, cache, and recorder. |
| "Will my caller send 1000s of requests per second from many concurrent clients?" | **Kafka source.** A single worker's webapp is capped by `max_concurrent` (default 64) and rate-limited per client; horizontal scale-out plus the Kafka-fallback pattern below is the path to higher throughput. |

The framework defaults the webapp to a small dev deployment (one
anonymous client, rpm=4). Production deployments should configure named
clients with non-empty tokens and per-client rpm caps -- see
[Config reference](#config-reference).

---

## Enabling

Set `webapp.enabled: true` in the worker config and declare the HTTP
request and response types as the third and fourth generic parameters
to `BaseDrakkarHandler`:

```python
import drakkar as dk
from pydantic import BaseModel


class KafkaIn(BaseModel):
    request_id: str
    pattern: str


class KafkaOut(BaseModel):
    request_id: str
    matches: int


class RankRequest(BaseModel):
    """Body of POST /process."""
    request_id: str
    query: str
    top_k: int = 10


class RankResponse(BaseModel):
    """Body of the JSON response under the ``result`` key."""
    request_id: str
    items: list[str]
    score: float


class RankHandler(dk.BaseDrakkarHandler[KafkaIn, KafkaOut, RankRequest, RankResponse]):
    async def arrange(self, messages, pending):
        # Kafka path -- unchanged.
        ...

    async def arrange_http_request(self, req: RankRequest, pending) -> list[dk.ExecutorTask]:
        # Translate a single HTTP request into one or more executor tasks.
        return [
            dk.ExecutorTask(
                task_id=dk.make_task_id('rank'),
                args=['--query', req.query, '--top-k', str(req.top_k)],
                metadata={'request_id': req.request_id},
            )
        ]

    async def on_http_request_complete(self, group: dk.MessageGroup) -> RankResponse:
        # Build the user-facing response from completed task results.
        items = [r.stdout.strip() for r in group.results]
        return RankResponse(
            request_id=group.request_id or '',
            items=items,
            score=float(group.succeeded),
        )
```

```yaml
webapp:
  enabled: true
  host: 0.0.0.0
  port: 8090
  path: /process
  clients:
    - name: tenant-a
      token: "secret-bearer-token"
      rpm: 60
```

That is the minimum. The handler keeps its existing `arrange()` /
`on_task_complete()` Kafka path; the webapp pipeline reuses the same
executor pool, the same `self.cache`, and the same recorder.

!!! warning "Webapp users must declare four type parameters"
    `BaseDrakkarHandler` is generic over four type parameters. Two-param
    handlers (`BaseDrakkarHandler[InputT, OutputT]`) keep working
    unchanged thanks to PEP 696 default `TypeVars`; the HTTP slots
    default to `None` and the webapp hooks are never called. **When
    `webapp.enabled=true`, the framework reads `HttpRequestT` and
    `HttpResponseT` off the handler subclass at startup and raises
    `ConfigurationError` if either is `None`.** See [Handler hooks](#handler-hooks) below.

---

## Config reference

Top-level `webapp:` block. Every field has a safe default so the example
in [Enabling](#enabling) is enough to get a working endpoint; the table
below documents every knob for production deployments.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Master switch. When `false`, no FastAPI server runs and the handler's HTTP hooks are never invoked. |
| `host` | `str` | `'0.0.0.0'` | Interface uvicorn binds. Use `'127.0.0.1'` to keep the endpoint inside the host (private deployments). |
| `port` | `int` | `8090` | Port uvicorn binds. The webapp does not share a port with the operator UI or the metrics server. |
| `path` | `str` | `'/process'` | Single POST route the framework registers. Must start with `'/'` and contain at least one character after it. |
| `sinks_enabled` | `bool` | `false` | When `true`, calls `on_message_complete(group)` after the executor fan-out and routes returned `CollectResult` payloads through the [SinkManager](sinks.md). When `false`, sinks are skipped entirely and the response carries `sinks: null`. |
| `request_timeout_seconds` | `float` | `30.0` | Per-request budget enforced via `asyncio.wait_for` on the webapp loop. On timeout the client receives a 504 with `status='timeout'` and the runner's post-execute hook is cooperatively cancelled. Must be > 0. |
| `max_concurrent` | `int` | `64` | Per-worker semaphore capacity for in-flight HTTP requests. The 65th concurrent request returns 503 `status='capacity'` immediately rather than queuing. Must be > 0. |
| `max_body_bytes` | `int` | `10485760` (10 MiB) | Cap on a single POST body, counted from the stream (a lying Content-Length cannot bypass it). Oversized requests receive 413 `error='request_too_large'` before parsing. Must be > 0. Same key, default, and 413 envelope on both backends. |
| `clients` | `list[WebClientConfig]` | one anonymous client with rpm=4 | Configured tenants. See the table below. At least one client is required; explicit `clients: []` fails at config load. |

### `webapp.clients[]` — `WebClientConfig`

Each entry defines one tenant with its bearer token and rpm cap.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Tenant name. Used in metric labels (`drakkar_webapp_requests_total{client=...}`), recorder rows, and the response body. Must be non-empty. |
| `token` | `str` | `''` | Bearer token presented in the `Authorization: Bearer <token>` header. **Empty token = anonymous slot** (matches requests with no `Authorization` header). At most one client may have an empty token; non-empty tokens must be unique across clients. Must be **ASCII** — tokens are compared byte-wise against the header, which cannot carry non-ASCII reliably, so a non-ASCII token is rejected at config load rather than failing every request at runtime. A request whose header carries a non-ASCII token gets a plain `401`. |
| `rpm` | `int` | `4` | Per-client requests-per-minute cap, enforced on a 60-second sliding window. Must be > 0. |

```yaml
webapp:
  enabled: true
  host: 0.0.0.0
  port: 8090
  path: /process
  sinks_enabled: false
  request_timeout_seconds: 30.0
  max_concurrent: 64
  max_body_bytes: 10485760
  clients:
    - name: anonymous
      token: ""
      rpm: 4
    - name: tenant-a
      token: "secret-tenant-a-token"
      rpm: 60
    - name: tenant-b
      token: "secret-tenant-b-token"
      rpm: 600
```

Validation runs at config load time. Misconfigurations
(`request_timeout_seconds <= 0`, duplicate non-empty tokens, two clients
with empty tokens, `clients: []`, etc.) raise `ValueError` at startup
with a message naming the offending field.

When every configured client has an empty token, the worker logs a
`webapp_unauthenticated_warning` at startup so private-network
deployments that should have had a token configured surface in log
aggregation.

---

## Handler hooks

`BaseDrakkarHandler` exposes four HTTP-specific hooks alongside the
Kafka-path hooks documented in [Handler](handler.md). They are invoked
only when `webapp.enabled=true` and only for HTTP requests; Kafka
messages never trigger them.

| Hook | When called | Frequency | Returns |
|------|-------------|-----------|---------|
| `arrange_http_request(req, pending)` | One HTTP request passed auth, rate-limit, and body parsing; about to enter the executor pool | Once per HTTP request | `list[ExecutorTask]` |
| `on_http_request_complete(group)` | All tasks from one HTTP request reached a terminal state | Once per HTTP request | `HttpResponseT` (a Pydantic model) |
| `http_request_id(req, headers)` | After body parsing, before task fan-out | Once per HTTP request | `str` (validated to ASCII, no whitespace, ≤64 chars) |
| `http_request_label(req, request_id)` | Before structured-log lines and UI rendering | Once per HTTP request | `str` |

Required overrides: `arrange_http_request` and `on_http_request_complete`.
The defaults raise `NotImplementedError` so a misconfigured deployment
fails fast at the first request rather than silently misbehaving.
`http_request_id` and `http_request_label` are optional -- the framework
generates a fresh id from `make_request_id('req')` and labels the
request with that id when not overridden.

### `arrange_http_request(req, pending) -> list[ExecutorTask]`

Mirrors [`arrange`](handler.md#arrange-required) for the webapp path.
Receives the parsed request body (typed as `HttpRequestT`) and an empty
`PendingContext` -- the synthetic per-request task group has no
sibling-message dedup work to do.

The framework auto-stamps every returned task with `origin='http'`,
`client_name=<matched client>`, and `request_id` (whatever
`http_request_id` returned). Operators see those columns in the recorder
and the operator UI -- no need to set them by hand.

```python
async def arrange_http_request(self, req: RankRequest, pending) -> list[dk.ExecutorTask]:
    return [
        dk.ExecutorTask(
            task_id=dk.make_task_id('rank'),
            args=['--query', req.query, '--top-k', str(req.top_k)],
            metadata={'request_id': req.request_id},
            labels={'kind': 'http', 'query': req.query[:32]},
        )
    ]
```

### `on_http_request_complete(group) -> HttpResponseT`

Mirrors [`on_message_complete`](handler.md#on_message_complete) for the
webapp path but is **required** and returns the user's
`HttpResponseT` model instead of an optional `CollectResult`. The
framework places the returned model under `"result"` in the JSON
response body (see [Response shape](#request-response-examples)).

The `MessageGroup` carries a synthetic `SourceMessage` with `partition=-1`
and a key set to the matched client name; `group.origin == 'http'`,
`group.client_name`, and `group.request_id` are populated from the
request envelope. `source_message.payload` carries the parsed request
model — the same object `arrange_http_request` received — so the
Kafka-path invariant ("`payload` is the parsed input model by the time
hooks fire") holds on the webapp path too; handlers never need to
re-parse `source_message.value`. On this path `group.results` holds
the terminal successes and `group.errors` the terminal failures — the
same `ExecutorError` objects the Kafka path delivers, so shared hook
code behaves identically on both paths. There is no retry machinery
over HTTP (a failure is terminal on its first attempt), and
cancellation aborts the request rather than being recorded as a task
failure.

```python
async def on_http_request_complete(self, group: dk.MessageGroup) -> RankResponse:
    if group.is_empty:
        return RankResponse(request_id=group.request_id or '', items=[], score=0.0)

    items = [line.strip() for r in group.results for line in r.stdout.splitlines() if line.strip()]
    return RankResponse(
        request_id=group.request_id or '',
        items=items,
        score=float(group.succeeded) / max(group.total, 1),
    )
```

When `webapp.sinks_enabled=true`, the framework calls
`on_message_complete(group)` (the same hook used by the Kafka path)
**before** `on_http_request_complete`, routes any returned payloads
through the [SinkManager](sinks.md), and records per-sink outcomes into
the response under `"sinks"`. See [Request / response examples](#request-response-examples).

### `http_request_id(req, headers) -> str`

Override to promote an upstream tracing header into the framework
request id. The default returns a fresh framework-generated id from
`make_request_id('req')`; overrides typically pull `X-Request-ID` or
`X-Correlation-ID` from the headers so a single id flows from caller →
worker → downstream services.

```python
def http_request_id(self, req: RankRequest, headers) -> str:
    upstream = headers.get('x-request-id')
    if upstream:
        return upstream  # validated against the request_id contract before use
    return dk.make_request_id('req')
```

The framework calls `validate_request_id(...)` on the return value (≤64
chars, ASCII, no whitespace). A buggy override that returns a malformed
id surfaces immediately as a 500 -- it never reaches downstream logs.

### `http_request_label(req, request_id) -> str`

Override to embed a business-readable identifier into structured log
lines and the operator UI. The default returns the framework `request_id`
unchanged.

```python
def http_request_label(self, req: RankRequest, request_id: str) -> str:
    return f'{req.query[:16]}/{request_id}'
```

---

## Request / response examples

Once the worker is running with `webapp.enabled=true`, send a POST to
the configured `path` with a JSON body matching `HttpRequestT`. The
examples below assume the [Enabling](#enabling) handler.

### Without sinks (`sinks_enabled: false`, the default)

```bash
curl -sS -X POST http://localhost:8090/process \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer secret-tenant-a-token' \
    -d '{"request_id": "rid-1", "query": "vikings", "top_k": 5}'
```

```python
import httpx

resp = httpx.post(
    'http://localhost:8090/process',
    headers={'Authorization': 'Bearer secret-tenant-a-token'},
    json={'request_id': 'rid-1', 'query': 'vikings', 'top_k': 5},
)
resp.raise_for_status()
print(resp.json())
```

Successful response body (HTTP 200):

```json
{
    "request_id": "rid-1",
    "client": "tenant-a",
    "started_at": "2026-05-06T09:01:00.000000+00:00",
    "finished_at": "2026-05-06T09:01:00.084000+00:00",
    "duration_ms": 84.0,
    "status": "ok",
    "result": {
        "request_id": "rid-1",
        "items": ["match-1", "match-2"],
        "score": 1.0
    },
    "tasks": [
        {"task_id": "rank-...", "exit_code": 0, "duration_ms": 41.5, "retries": 0}
    ],
    "task_summary": {"total": 1, "success": 1, "failed": 0},
    "cache": {"hits": 0, "misses": 0},
    "sinks": null,
    "timeline": [
        {"stage": "arrange", "duration_ms": 0.6},
        {"stage": "execute", "duration_ms": 41.7},
        {"stage": "on_http_request_complete", "duration_ms": 0.4}
    ],
    "error": null
}
```

| Field | Meaning |
|-------|---------|
| `request_id` | The id resolved by `http_request_id(...)`. Same value stamped onto every executor task and recorder row for the request. |
| `client` | The matched `WebClientConfig.name` from auth. Used as a metric label and recorder column. |
| `started_at` / `finished_at` | UTC timestamps marking the request's wall-clock entry/exit at the route handler. |
| `duration_ms` | Server-side wall clock between `started_at` and `finished_at`. Prefer this over external timers when comparing requests across clients. |
| `status` | One of `ok | error | timeout | rate_limited | auth_failed | shutdown | not_ready`. Mirrors the `drakkar_webapp_requests_total` label. |
| `result` | The Pydantic model returned by `on_http_request_complete(group)`, dumped to JSON. `null` on error paths. |
| `tasks` | One compact entry per successful executor task (`task_id`, `exit_code`, `duration_ms`, `retries`). **Subprocess `stdout` / `stderr` are deliberately excluded from the response body** -- read those via the recorder or the operator UI. |
| `task_summary` | Aggregate `total / success / failed` counts for the per-request fan-out. |
| `cache` | Per-request cache hit/miss counts. Reserved for future use; the runner currently emits zeros. |
| `sinks` | `null` when `sinks_enabled=false`. With sinks enabled, an aggregated per-sink-type delivery summary -- see below. |
| `timeline` | One `{stage, duration_ms}` entry per pipeline phase (`arrange`, `execute`, optional `sinks`, `on_http_request_complete`). Lets operators see where time was spent without correlating timestamps across log lines. |
| `error` | Human-readable error message on the error/timeout paths; `null` on the success path. |

### With sinks (`sinks_enabled: true`)

The same request as above with `webapp.sinks_enabled: true` in config
and an `on_message_complete` hook returning a `CollectResult`:

```yaml
webapp:
  enabled: true
  sinks_enabled: true
  # ... rest unchanged
```

```python
async def on_message_complete(self, group: dk.MessageGroup) -> dk.CollectResult | None:
    return dk.CollectResult(
        kafka=[dk.KafkaPayload(data=group.results[0], key=group.request_id.encode())],
    )
```

Successful response body (HTTP 200) -- the `sinks` field is now populated:

```json
{
    "request_id": "rid-1",
    "client": "tenant-a",
    "status": "ok",
    "result": {"request_id": "rid-1", "items": ["match-1"], "score": 1.0},
    "tasks": [{"task_id": "rank-...", "exit_code": 0, "duration_ms": 41.5, "retries": 0}],
    "task_summary": {"total": 1, "success": 1, "failed": 0},
    "cache": {"hits": 0, "misses": 0},
    "sinks": {
        "by_type": {
            "kafka": {"attempted": 1, "delivered": 1, "dlq": 0, "errors": []}
        }
    },
    "timeline": [
        {"stage": "arrange", "duration_ms": 0.6},
        {"stage": "execute", "duration_ms": 41.7},
        {"stage": "sinks", "duration_ms": 5.4},
        {"stage": "on_http_request_complete", "duration_ms": 0.4}
    ]
}
```

The `sinks.by_type` map is keyed by sink type (`kafka`, `postgres`,
`mongo`, ...) and emits one entry per type that the request actually
touched. Each entry carries `attempted`, `delivered`, `dlq`, and an
`errors` list for delivery failures routed through
[`on_delivery_error`](handler.md#on_delivery_error). Sinks that the
request did not touch are omitted -- the response mirrors what was
attempted, not the full configured sink set.

---

## Status codes

Error responses from the runner share a flat envelope shape: `status`
(string), `error` (human-readable), `request_id` (server-side id where
one was allocated), and a per-status set of extra fields. The 401 and
429 gates fire before the runner allocates that envelope, so their
bodies use their own shapes (see the table). The 429 / 503 bodies
include a `hint` field telling the client to switch to the Kafka source
topic for higher throughput and worker-restart resilience.

| HTTP | `status` | When | Sample body |
|------|----------|------|-------------|
| 200 | `ok` | Successful end-to-end execution | See [Request / response examples](#request-response-examples). |
| 401 | `auth_failed` | `Authorization` header is missing-with-no-anonymous-slot, malformed, or names a non-configured token | `{"error": "unauthorized"}` |
| 413 | n/a (`error: request_too_large`) | Body exceeds `max_body_bytes` (enforced before parsing; identical on both backends) | `{"error": "request_too_large", "request_id": "req-...", "details": "request body exceeds webapp.max_body_bytes (10485760 bytes)"}` |
| 422 | n/a (legacy `error: invalid_request`) | Body is missing or fails Pydantic validation against `HttpRequestT` | `{"error": "invalid_request", "request_id": "req-...", "details": [{"loc": ["query"], "msg": "Field required", "type": "missing"}]}` |
| 429 | n/a (`error: rate_limited`) | Per-client rpm window is full (rejected at the gate, before the runner; the response also carries a `Retry-After` header with the wait rounded up to whole seconds) | `{"error": "rate_limited", "client": "tenant-a", "rpm_limit": 60, "retry_after_seconds": 1.7, "hint": "route this workload through the Kafka source topic for higher throughput and worker-restart resilience"}` |
| 408 | n/a (`error: request_timeout`) | The request body was not delivered within `request_timeout_seconds` + 30 s. The size cap alone is not a slow-loris defence — a client can stay under it indefinitely by sending one byte at a time — so the body read is bounded in time too | `{"error": "request_timeout", "request_id": "req-...", "hint": "the request body was not delivered in time"}` |
| 503 | `capacity` | `max_concurrent` semaphore is full at request time | `{"status": "capacity", "error": "webapp is over capacity; request rejected", "request_id": "req-...", "max_concurrent": 64, "hint": "route this workload through the Kafka source topic for higher throughput and worker-restart resilience"}` |
| 503 | `shutdown` | The worker is in the drain phase and refusing new requests | `{"status": "shutdown", "error": "webapp is shutting down; request rejected", "request_id": "req-...", "hint": "route this workload through the Kafka source topic for higher throughput and worker-restart resilience"}` |
| 503 | `not_ready` | The webapp accepted the connection but the main pipeline has not yet completed startup | `{"status": "not_ready", "error": "webapp is starting; main pipeline is not yet ready", "request_id": "req-...", "hint": "route this workload through the Kafka source topic for higher throughput and worker-restart resilience"}` |
| 504 | `timeout` | `request_timeout_seconds` elapsed while the runner was processing | `{"status": "timeout", "error": "request exceeded 30.0 seconds", "request_id": "req-...", "duration_ms": 30001.4, "hint": "route this workload through the Kafka source topic for higher throughput and worker-restart resilience"}` |
| 500 | `error` | A user hook (`arrange_http_request`, `on_http_request_complete`, `http_request_id`, etc.) raised an unhandled exception | `{"status": "error", "request_id": "req-...", "error": "internal error"}` |

The 422 response intentionally uses `error: invalid_request` rather than
a `status` field -- it sits at the body-parse boundary, before the
runner allocates the framework status taxonomy. The other rejection
paths mirror `drakkar_webapp_requests_total{status=...}` exactly so
operators can cross-reference response bodies with Prometheus
dashboards.

Tracebacks from user-hook exceptions are NEVER included in the response
body. The full traceback lands in the structured log
(`webapp_request_handler_error` / `webapp_arrange_http_request_failed`
/ `webapp_on_http_request_complete_failed`) so operators can debug
without the framework leaking internals to clients.

---

## Graceful shutdown semantics

The webapp participates in the worker shutdown sequence to guarantee
that callers see a clean rejection rather than a connection reset
mid-flight.

1. **Drain signal.** `AppLifecycle._shutdown` flips the webapp's
   `shutdown_event` **before** the drain phase begins. The route
   handler checks `shutdown_event.is_set()` at the top of every request
   and returns a 503 `status='shutdown'` immediately for any new
   request that lands during drain. This is the **1-second guarantee
   for new-request rejection**: from the moment shutdown begins, every
   subsequent request gets a 503 within one event-loop tick.
2. **In-flight requests complete.** Already-dispatched requests
   continue running on the main pipeline loop. They are bounded by
   `request_timeout_seconds` -- a slow handler cannot hold shutdown
   open indefinitely.
3. **Uvicorn stop.** Once the drain completes, the framework calls
   `WebApp.stop(drain_timeout)` which signals uvicorn to exit and joins
   the daemon thread within `drain_timeout`. A stuck request handler
   logs `webapp_stop_thread_join_timeout` and the worker proceeds with
   teardown -- the daemon thread is collected when the process exits.

The `not_ready` 503 covers the **other** boundary: between the webapp
thread coming up and the main pipeline completing its first poll. New
requests during that startup window also receive 503 with the
Kafka-fallback hint.

---

## Front it with a proxy

Drakkar bounds what it can bound in-process: the body size
(`max_body_bytes`), the body **read** time (`request_timeout_seconds` +
30 s → `408`), keep-alive idle time (120 s), and the request header
section (64 KiB). Those are backstops against a client that misbehaves by
accident, and against one that is deliberately slow.

They are **not** a substitute for a reverse proxy. This port is served by
the same process that runs the pipeline, so anything that consumes
connections there competes with real work. In any deployment reachable
beyond a trusted network, put nginx / Envoy / a cloud load balancer in
front and let it own connection-level concerns: per-client connection
limits, its own read and header timeouts, request-rate limiting, TLS
termination and body-size rejection at the edge — so a slow or hostile
client never reaches the worker at all.

The same applies to the debug UI port (`ui.port`) and, if enabled, the
Prometheus exporter.

## Load balancer caveat

Webapp state is **per-worker**, not per-cluster:

- The rpm sliding window (`WebClientConfig.rpm`) is enforced
  independently on every worker that exposes the webapp. A 60 rpm cap
  on three workers tolerates 180 rpm cluster-wide if requests load-balance
  evenly across them.
- The `max_concurrent` semaphore is per-worker. A 64-slot cap on three
  workers admits up to 192 concurrent requests cluster-wide.
- The `drakkar_webapp_*` metrics are per-worker; aggregate via
  Prometheus `sum by (...)` for cluster-wide views.

**Choose your load-balancer policy with this in mind:**

| Scenario | Recommended LB policy |
|----------|----------------------|
| Per-tenant rate limits should match the configured rpm regardless of which worker handled the request | Sticky routing by `Authorization` token (or `X-Tenant-ID`). The same client always lands on the same worker; per-worker rpm equals cluster rpm. |
| Workload is uniform and you want even utilisation | Stateless distribution (round-robin, random, least-conn). Effective cluster rpm is `rpm * worker_count`; size the configured rpm down accordingly. |
| Workers run different config (e.g. only worker-1 has `webapp.enabled=true`) | Pin webapp traffic to the enabled workers; the unconfigured workers do not bind the webapp port. The integration scenario uses this layout deliberately to exercise mixed-config deployments. |

The framework does not coordinate rate-limit state across workers. If
you need true cluster-wide rate limits, terminate them at the
load-balancer or an upstream API gateway.

---

## Cross-thread caveat for advanced users

The webapp lives on a separate event loop from the main pipeline:

- **T1** (main loop) -- runs the executor pool, the recorder, sinks,
  Kafka consumer, and `@periodic` tasks.
- **T2** (webapp loop) -- runs uvicorn, the FastAPI route handler, the
  per-request semaphore, and `asyncio.wait_for` for the request
  timeout.

The framework hops requests between the two loops via
`drakkar.concurrency.dispatch_to_loop`. User code never needs to think
about threading on the request path -- `arrange_http_request` and
`on_http_request_complete` always run on T1.

The exception is **a `@periodic` task that needs to reach the webapp
loop**. This is rare; the typical case is a periodic worker action
(refresh a lookup table, write a heartbeat) that should also broadcast
state into the webapp's per-request structures. When that comes up,
`dispatch_to_loop` works in either direction -- pass the webapp loop as
the target:

```python
import drakkar as dk
from drakkar.concurrency import dispatch_to_loop


class MyHandler(dk.BaseDrakkarHandler[KIn, KOut, RankRequest, RankResponse]):
    @dk.periodic(seconds=30)
    async def refresh_webapp_state(self):
        # ``self._app`` and the webapp loop are framework-private; this
        # pattern is intentionally low-level. Most handlers will never
        # need it.
        webapp = self._app._webapp  # type: ignore[attr-defined]
        if webapp is None or webapp._loop is None:
            return  # webapp not enabled or not yet ready
        await dispatch_to_loop(self._refresh_on_t2(), target_loop=webapp._loop)

    async def _refresh_on_t2(self) -> None:
        # Runs on T2. Touch only objects bound to the webapp loop.
        ...
```

The far more common case -- handler hooks reading and writing
`self.cache`, calling `await self._app.something()` -- runs on T1
naturally; `dispatch_to_loop` is not needed.

---

## Priority-scheduling pattern

HTTP requests typically have tighter latency requirements than Kafka
messages (a synchronous client is waiting for the response). The
[`task_priority`](handler.md#task_priority) hook overrides the executor
pool's priority key so HTTP-origin tasks dequeue ahead of Kafka-origin
tasks:

```python
class WebappFirstHandler(dk.BaseDrakkarHandler[KIn, KOut, RankRequest, RankResponse]):
    def task_priority(self, task):
        # Smaller key dequeues first. ``origin == 'http'`` tasks get a
        # priority class of 0; Kafka tasks fall back to the default
        # offset-based key with a leading 1.
        if task.origin == 'http':
            return (0, task.request_id or '')
        return (1, min(task.source_offsets) if task.source_offsets else 0)
```

The default priority key is `min(task.source_offsets)` -- for
Kafka-origin tasks that drains older messages first. HTTP-origin tasks
have an empty `source_offsets` list, so without an override they
tiebreak FIFO via the gate's internal sequence counter. The override
above gives them an explicit priority class.

`task_priority` is also called for retries (with the same task object),
and the framework auto-stamps `origin` / `client_name` / `request_id`
before submission, so the override sees the same values on the first
attempt and on every retry.

---

## Kafka-fallback pattern

The webapp is the right tool for synchronous, low-throughput,
per-request workloads. For high-throughput or restart-resilient
workloads, route through the Kafka source topic instead -- that is what
the `hint` field on every 429 / 503 / 504 response steers clients
toward.

A typical client wrapper:

```python
import httpx


async def submit_with_fallback(client: httpx.AsyncClient, payload: dict) -> dict:
    """Try the webapp first; on capacity / shutdown / timeout, fall back to Kafka."""
    try:
        resp = await client.post('http://worker-1:8090/process', json=payload, timeout=5.0)
    except httpx.TimeoutException:
        await produce_to_kafka('input-events', payload)
        return {'status': 'enqueued', 'channel': 'kafka'}

    if resp.status_code in (429, 503, 504):
        await produce_to_kafka('input-events', payload)
        return {'status': 'enqueued', 'channel': 'kafka'}

    resp.raise_for_status()
    return resp.json()
```

The framework's contract: a successful 200 means the request was
processed end-to-end on this worker; a 429/503/504 means the caller
should retry via Kafka rather than hammering the webapp. Clients that
ignore the hint and tight-loop the webapp will see sustained
`drakkar_webapp_requests_total{status='rate_limited'}` /
`{status='capacity'}` -- correlate those metrics with the relevant
client label and surface the load-balancing problem at the deployer.

---

## Integration scenario

The repo ships with a runnable end-to-end scenario under `integration/`
that wires the webapp pipeline into the same docker-compose stack used
for Kafka demos. It demonstrates:

- **Per-worker enablement** -- only `worker-1` flips
  `webapp.enabled=true` (via the `DK_WEBAPP__ENABLED=true` env var); the
  other two workers leave the shared yaml default of `false` so they
  never bind port 8091 (the integration stack overrides the framework
  default of 8090). This matches the third row of the
  [Load balancer caveat](#load-balancer-caveat) table.
- **Anonymous client at rpm=4** -- the default `load_generator`
  service polls `POST /process` every 10 seconds with no
  `Authorization` header. Steady state is 200 OK; lower
  `INTERVAL_SECONDS` to ~2 seconds and watch 429s appear in the
  generator's stdout.
- **Tenant-A client at rpm=60** -- the optional
  `load_generator_tenant_a` service (gated behind a compose profile)
  presents a bearer token and runs at a tighter interval to exercise
  the higher rpm tier.
- **Priority scheduling** -- the integration handler's
  `task_priority(task)` override returns `(0, 0)` for `origin='http'`
  tasks and `(1, min_offset)` for Kafka tasks, so HTTP-origin tasks
  jump the executor gate while a slow Kafka task is in flight.

### Running it

```bash
cd integration
docker compose up --build         # default: anonymous load_generator only
# or, to also drive the tenant-A path:
docker compose --profile tenant up --build
```

What to expect within ~30 seconds of the stack being healthy:

- `worker-1` logs `webapp_request_received` and `webapp_request_completed`
  events.
- The `load_generator` container prints one line per iteration with the
  status code and a body preview (`"result"`, `"sinks"`, `"errors"`,
  `"hint"`).
- `http://localhost:8081/live` highlights HTTP-origin tasks with the
  origin column populated; the same tasks dequeue ahead of pending
  Kafka tasks at the gate.
- `drakkar_webapp_requests_total{client="anonymous",status="ok"}` climbs
  in Prometheus (`http://localhost:9099`).

### File map

| Path | Purpose |
|------|---------|
| `integration/load_generator/loop.py` | Standalone Python loop that POSTs `RankRequest` payloads on a configurable interval. |
| `integration/load_generator/Dockerfile` | Thin `python:3.13-slim` image; the only dependency is a pinned `httpx`. |
| `integration/load_generator/requirements.txt` | `httpx==0.27.2` (pinned). |
| `integration/worker/handler.py` | 4-param `BaseDrakkarHandler`; defines `arrange_http_request`, `on_http_request_complete`, and the `task_priority` override. |
| `integration/worker/models.py` | Adds `RankRequest` / `RankResponse`. |
| `integration/worker/drakkar.yaml` | `webapp:` block with two clients; `enabled=false` by default and flipped per-worker via env. |
| `integration/docker-compose.yml` | Defines `load_generator` and `load_generator_tenant_a`; exposes webapp port 8091 on `worker-1` and reuses the debug UI server's `/healthz` (port 8081) as the readiness gate — the webapp itself owns no `/healthz` route. |

---

## Related pages

- [Handler](handler.md) -- the full hook surface, including the four
  HTTP-specific hooks
- [Configuration](configuration.md#webapp-webapp) -- the deep config
  table for the `webapp:` block
- [Observability](observability.md#webapp) -- Prometheus metrics, the
  six new recorder event types, and the recorder upgrade story
- [Sinks](sinks.md) -- the same `CollectResult` / sink routing the
  webapp pipeline reuses when `sinks_enabled=true`
