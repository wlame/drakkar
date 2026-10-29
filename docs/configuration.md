# Configuration

Drakkar configuration is built on [pydantic-settings](https://docs.pydantic-dev.github.io/pydantic-settings/) and supports YAML files, environment variable overrides, and inline Python objects. Every option has a sensible default so you can start with a minimal config and grow from there.

!!! tip "Looking for an annotated example?"
    The companion **[Config Reference](config-reference.md)** page is a single `drakkar.yaml` showing **every** field with a one-line comment and its `DK_*` env-var override. Use it to scan what's available; come back here for the deep tables and prose.

---

## Configuration Loading

Drakkar resolves its configuration from three sources, applied in order. Later sources override earlier ones:

1. **Built-in defaults** -- every field has a default value in the Pydantic models.
2. **YAML file** -- structured configuration loaded from disk.
3. **Environment variables** -- `DK_`-prefixed env vars, deep-merged on top of YAML.

### YAML File Resolution

The YAML file path is determined by the first match:

1. The `config_path` argument passed to `DrakkarApp`.
2. The `DK_CONFIG` environment variable.
3. If neither is set, Drakkar runs with env-only configuration (all defaults + env overrides).

```python
# Option 1: explicit path
app = DrakkarApp(handler=MyHandler(), config_path='drakkar.yaml')

# Option 2: env var (set DK_CONFIG=/etc/drakkar/config.yaml)
app = DrakkarApp(handler=MyHandler())

# Option 3: inline config object (no file needed)
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSourceConfig,
    SourcesConfig,
)

app = DrakkarApp(
    handler=MyHandler(),
    config=DrakkarConfig(
        kafka=KafkaConfig(brokers='kafka:9092'),
        sources=SourcesConfig(
            kafka=KafkaSourceConfig(enabled=True, topic='my-events'),
        ),
        executor=ExecutorConfig(max_executors=8),
    ),
)
```

!!! note
    When you pass a `config` object directly, no YAML file or env var loading occurs -- you have full control.

### Environment Variable Overrides

Environment variables use the `DK_` prefix with `__` (double underscore) as the nesting delimiter:

```bash
# Override kafka.brokers
export DK_KAFKA__BROKERS=kafka-prod:9092

# Override executor.max_executors
export DK_EXECUTOR__MAX_EXECUTORS=16

# Override ui.port
export DK_UI__PORT=9000
```

Env vars are parsed into a nested dict structure and deep-merged on top of the YAML values. Leaf values from env vars always win over YAML values; nested dicts are merged recursively. A JSON list or object in a variable for a list or map field is that field's whole value: it replaces the YAML value and is not merged with it.

### Loading Order Summary

```
defaults --> YAML file --> environment variables (deep merge)
```

### Unknown keys

A key that the config does not declare fails the load, in the YAML file and in a
`DK_*` variable. One message lists every unknown key, in sorted order. A line
suggests the closest valid key at the same level when one is close:

```
config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
  executor.max_executer (did you mean 'max_executors'?)
  sinks.kafka.results.max_retires (did you mean 'max_retries'?)
  ui.timeline.events[2].colour (did you mean 'color'?)
```

The names you choose yourself (a sink instance, a connection) are not checked,
but the keys below them are. The free maps `app`, `executor.env` and the HTTP
sink's `headers` accept any key.

A config error does not carry the input of the section that failed (the cut
of the config that the validation library adds to its message). A bad
`connections` block, for example, reports the key and the rule, and does not
print the connection string. The framework's own messages still quote a
value that is not a secret, such as a schema name.

---

## Root Config (`DrakkarConfig`)

Top-level settings that control worker identity and cluster grouping.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker_name_env` | `str` | `'WORKER_ID'` | Name of the environment variable that holds the worker name. Used in logs, metrics, and the operator UI. If the env var is empty or unset, falls back to `drakkar-{hex_id}`. |
| `cluster_name` | `str` | `''` | Logical cluster name for grouping workers in the operator UI. Workers with the same cluster name are displayed together and can cross-trace messages. |
| `cluster_name_env` | `str` | `''` | Name of an environment variable that holds the cluster name. If set and non-empty, overrides the static `cluster_name` value. |
| `app` | `dict` | `{}` | Reserved section for **user-defined application config**: passed through unvalidated to the handler-declared model and exposed as `self.app_config`, with env overrides under the handler's own prefix (never `DK_APP__*`). See [App Config](app-config.md). |

```yaml
worker_name_env: HOSTNAME
cluster_name: search-cluster
cluster_name_env: DK_CLUSTER
```

---

## Kafka connection (`kafka:`)

How this worker reaches the Kafka cluster. The block holds no consumer
settings: the consumer is an input source and lives under
[`sources.kafka`](#kafka-source-sourceskafka). One connection serves three
users — the Kafka source, the [Kafka sinks](#kafka-sink-sinkskafkaname),
and the [DLQ](#dead-letter-queue-dlq) — so a worker with no Kafka source
still needs this block when it writes to Kafka.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `str` | `'localhost:9092'` | Kafka bootstrap servers (comma-separated for multiple brokers). Also used as the fallback for sink and DLQ brokers when they are left empty. |
| `security` | `KafkaSecurityConfig` | PLAINTEXT | Transport authentication and encryption. See [Kafka security](#kafka-security-kafkasecurity). |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties merged after `security`. See [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `ui_url` | `str` | `''` | Base URL of a Kafka-UI instance. With `ui_cluster_name`, the operator UI renders a deep link next to every `partition:offset`. Empty disables the links. |
| `ui_cluster_name` | `str` | `''` | Cluster name as registered in Kafka-UI, used in those deep links. Both fields must be set. |

```yaml
kafka:
  brokers: kafka-1:9092,kafka-2:9092
  ui_url: http://kafka-ui:8080
  ui_cluster_name: search-cluster
```

### Kafka security (`kafka.security`)

The default is `PLAINTEXT`, which emits no librdkafka properties at all — a worker that configures nothing here connects exactly as it did before this block existed. Configure it to reach a cluster that requires authentication or TLS.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protocol` | `'PLAINTEXT'` \| `'SSL'` \| `'SASL_PLAINTEXT'` \| `'SASL_SSL'` | `'PLAINTEXT'` | Wire protocol. `SSL` adds TLS, `SASL_PLAINTEXT` adds authentication without encryption, `SASL_SSL` adds both and is what managed clusters normally require. |
| `sasl_mechanism` | `'PLAIN'` \| `'SCRAM-SHA-256'` \| `'SCRAM-SHA-512'` \| `'GSSAPI'` \| `'OAUTHBEARER'` \| `null` | `null` | Required when `protocol` is a `SASL_*` value, and **rejected** otherwise so a mechanism that would be silently ignored never sits in your config. |
| `sasl_username` | `str` | `''` | Required for `PLAIN` and the SCRAM mechanisms. |
| `sasl_password` | `SecretStr` | `''` | Required for `PLAIN` and the SCRAM mechanisms. Never rendered by `repr()` or `model_dump()`. |
| `ssl_ca_location` | `str` | `''` | Path to a PEM CA bundle. Empty uses the system trust store. |
| `ssl_certificate_location` | `str` | `''` | Client certificate for mutual TLS. |
| `ssl_key_location` | `str` | `''` | Client private key for mutual TLS. Requires `ssl_certificate_location`. |
| `ssl_key_password` | `SecretStr` | `''` | Passphrase for an encrypted client key. |
| `ssl_endpoint_identification_algorithm` | `'https'` \| `'none'` \| `null` | `null` | Unset uses librdkafka's default (`https` — hostname verification on). Set `none` only for internal CAs whose certificates lack matching SANs. |

**Keep credentials out of YAML.** Every field takes a `DK_` environment override, and `DK_*` variables are withheld from executor subprocesses by the default `executor.env_inherit_deny` list — so a password supplied that way never reaches your handler binary.

```bash
export DK_KAFKA__SECURITY__SASL_PASSWORD='s3cret'
```

Incoherent combinations fail at startup rather than at first poll, because librdkafka reports most of them as an opaque connection error long after the mistake was made:

- a `SASL_*` protocol with no `sasl_mechanism`
- `PLAIN`/SCRAM without `sasl_username` **and** `sasl_password`
- a `sasl_mechanism` set while `protocol` is `PLAINTEXT` or `SSL`
- `ssl_key_location` without `ssl_certificate_location`

Certificate paths are *not* checked when config loads. A missing file logs a warning at startup and librdkafka fails the connect — a hard failure at parse time would turn an init-container mount race into a crash loop.

#### Confluent Cloud / Aiven (SASL_SSL + PLAIN)

```yaml
kafka:
  brokers: pkc-abc12.eu-west-1.aws.confluent.cloud:9092
  security:
    protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: MYCLUSTERAPIKEY
    # password via DK_KAFKA__SECURITY__SASL_PASSWORD
```

#### Self-managed cluster with SCRAM and a private CA

```yaml
kafka:
  brokers: kafka-1:9093,kafka-2:9093
  security:
    protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: drakkar
    ssl_ca_location: /etc/ssl/certs/internal-ca.pem
```

#### Mutual TLS (no SASL)

```yaml
kafka:
  brokers: kafka-1:9093
  security:
    protocol: SSL
    ssl_ca_location: /etc/ssl/certs/ca.pem
    ssl_certificate_location: /etc/ssl/certs/client.pem
    ssl_key_location: /etc/ssl/private/client.key
```

#### How sinks and the DLQ get their credentials

A Kafka sink or DLQ whose own `brokers` field is **empty** inherits `kafka.brokers`, `kafka.security`, and `kafka.client_config` together — same cluster, same credentials. This is the usual configuration and needs nothing extra:

```yaml
kafka:
  brokers: kafka-1:9093
  security:
    protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: drakkar

sinks:
  kafka:
    results:
      topic: search-results   # brokers omitted -> inherits security above
```

Setting `brokers` on a sink makes it self-contained: it then uses only its own `security` block, which defaults to `PLAINTEXT`. If the consumer is secured and such a sink has no security block, startup logs a `kafka_security_mismatch` warning naming the sink.

### Raw librdkafka overrides (`client_config`)

For properties the typed block does not model — OAUTHBEARER token settings, Kerberos tuning, connection-pool knobs:

```yaml
kafka:
  client_config:
    sasl.oauthbearer.config: 'principal=drakkar'
    socket.keepalive.enable: 'true'
```

`client_config` is merged **after** `security`, so it wins on any key both set. Four properties are rejected at startup, because each one backs a delivery invariant:

| Rejected key | Why |
|---|---|
| `enable.auto.commit` | At-least-once delivery depends on Drakkar committing on its own per-partition watermark; auto-commit would advance past unprocessed messages. |
| `partition.assignment.strategy` | The drain-on-revoke path assumes cooperative-sticky rebalancing. |
| `group.id` | Set it with `sources.kafka.consumer_group`. |
| `bootstrap.servers` | Set it with `kafka.brokers`. |

Rejecting beats ignoring: an override that is silently dropped looks like it worked.

---

## Input sources (`sources:`)

A worker reads work from Kafka, from HTTP, or from both. Each input is a
**source** under `sources:`. Both are off by default, and **at least one
must be on** — a config with neither fails to load:

```
no input source enabled: set sources.kafka.enabled or sources.http.enabled
```

A source block is validated only when it is enabled. A disabled block that
would not pass validation logs one `source_config_ignored` warning at
startup and does not block the worker.

The [Input Sources](sources.md) page compares the two side by side and
covers readiness, shutdown and the DLQ rule. This section is the field
reference.

| Source | Key | Default | Starts |
|---|---|---|---|
| Kafka consumer | `sources.kafka.enabled` | `false` | Consumer group, poll loop, per-partition processors |
| HTTP ingress | `sources.http.enabled` | `false` | One POST route on its own server thread |

### Kafka source (`sources.kafka`)

Settings for the Kafka consumer that reads input messages. The cluster it
connects to comes from [`kafka:`](#kafka-connection-kafka).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Consume the Kafka topic. When `false`, no consumer group is joined and `arrange()` is never called. |
| `topic` | `str` | `'input-events'` | The Kafka topic to consume messages from. |
| `consumer_group` | `str` | `'drakkar-workers'` | Consumer group ID. All workers in the same group share partition assignments. |
| `max_poll_records` | `int` | `100` | Maximum number of messages returned per poll batch. Higher values improve throughput; lower values reduce latency. |
| `max_poll_interval_ms` | `int` | `300000` | Maximum time (ms) between poll calls before Kafka considers the consumer dead and triggers a rebalance. Increase this if your tasks take a long time. |
| `session_timeout_ms` | `int` | `45000` | Session timeout (ms) for group membership. If the broker does not receive a heartbeat within this window, the consumer is removed from the group. |
| `heartbeat_interval_ms` | `int` | `3000` | Interval (ms) between heartbeats sent to the broker. Should be less than `session_timeout_ms / 3`. |
| `on_parse_error` | `'skip'` \| `'dlq'` \| `'raise'` | `'skip'` | What to do when a message value fails `input_model` parsing. `skip`: the message reaches `arrange()` with `payload=None` and `msg.parse_error` set. `dlq`: the message is excluded from `arrange()` and written to the DLQ topic as a [DLQ record](sinks.md#the-dlq-record) (`dk-stage: parse`: the original key, value and headers, no `dk-attempts`); the offset commits once the DLQ write is confirmed (a failed write follows `dlq.on_send_failure`). `raise`: fail fast — a `MessageParseError` stops the partition processor. |
| `startup_align_enabled` | `bool` | `true` | When `true`, delay the first Kafka subscribe until a shared wall-clock boundary so a fleet of workers converges on a single rebalance. Disable for single-process dev runs. |
| `startup_min_wait_seconds` | `float` | `4.0` | Minimum seconds to sleep before aligning. Acts as a buffer for slow init (DB connects, schema migrations, cache warm-up). Must be `>= 0`. |
| `startup_align_interval_seconds` | `int` | `10` | Alignment interval in seconds. Workers wake at the next `time.time() % interval == 0` boundary — default `10` aligns on `:00/:10/:20/:30/:40/:50` of every minute. Must be `>= 1`. |

When enabled, `topic` and `consumer_group` must be non-empty.

```yaml
kafka:
  brokers: kafka-1:9092,kafka-2:9092

sources:
  kafka:
    enabled: true
    topic: search-requests
    consumer_group: search-workers
    max_poll_records: 200
    max_poll_interval_ms: 600000
    session_timeout_ms: 60000
    heartbeat_interval_ms: 5000
    # Rolling-deploy: serialize group rebalances by waking all workers at
    # the same wall-clock moment instead of whenever each finishes init.
    startup_align_enabled: true
    startup_min_wait_seconds: 4.0
    startup_align_interval_seconds: 10
```

Env overrides follow the keys: `DK_SOURCES__KAFKA__ENABLED`,
`DK_SOURCES__KAFKA__TOPIC`, `DK_SOURCES__KAFKA__CONSUMER_GROUP`, and so on.

#### Staggered startup alignment

During a rolling deploy, workers come up one at a time over a span of seconds. Each fresh `subscribe` call triggers a Kafka consumer-group rebalance, which stalls consumption on every other worker in the group. A fleet of 10 workers that boots over ~15 seconds can cause 10 cascading rebalances and several seconds of effective downtime.

With `startup_align_enabled: true` (the default), each worker runs its normal init (`on_startup`, cache engine, recorder, periodic tasks, sink connects) and then — *before* calling `consumer.subscribe()` — waits until:

1. At least `startup_min_wait_seconds` have elapsed (lets slow-init workers catch up to fast ones).
2. The wall-clock Unix-epoch seconds are a multiple of `startup_align_interval_seconds` (default `10` — i.e. `:00/:10/:20/:30/:40/:50`).

Workers whose init completes anywhere in the same 10-second window then all subscribe at the next boundary within one "tick" of each other, collapsing N rebalances into 1.

The sleep window is logged as `startup_align_waiting` / `startup_align_done` lifecycle events with the target instant so deploy runbooks can account for the deliberate pause.

Tuning:
- **Larger fleets**: keep `startup_align_interval_seconds: 10` (default) — works well up to dozens of workers.
- **Slow-init workers** (seconds of migrations): raise `startup_min_wait_seconds` so the boundary is likely to fall AFTER the slowest worker's init.
- **Very small clusters / dev iteration**: set `startup_align_enabled: false` to skip the pause entirely.

### HTTP source (`sources.http`)

The synchronous HTTP entry point, served by the webapp server. When
`enabled: true`, Drakkar runs a FastAPI server on its own thread that
accepts POST requests and routes them through the same handler pipeline as
Kafka messages. The handler must declare `HttpRequestT` / `HttpResponseT`
as the third and fourth generic parameters of `BaseDrakkarHandler`; missing
types raise `ConfigurationError` at startup.

A bind failure is **fatal**. The HTTP source is an input, so a worker that
cannot bind its socket stops instead of running with nothing to read.

See [Webapp](webapp.md) for the full feature guide (hooks,
request/response shape, status codes, shutdown semantics).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `false` | | Master switch. When `false`, the FastAPI server is not started and the HTTP hooks are not invoked. |
| `host` | `str` | `'0.0.0.0'` | | Interface uvicorn binds. Use `'127.0.0.1'` for host-private deployments. |
| `port` | `int` | `8090` | | Port uvicorn binds. Distinct from the metrics and operator-UI ports. |
| `path` | `str` | `'/process'` | starts with `'/'`, length > 1 | Single POST route the framework registers. |
| `sinks_enabled` | `bool` | `false` | | When `true`, calls `on_message_complete` after the executor fan-out and routes returned `CollectResult` payloads through the [SinkManager](sinks.md). When `false`, sinks are skipped and the response carries `sinks: null`. |
| `request_timeout_seconds` | `float` | `30.0` | > 0 | Per-request budget enforced via `asyncio.wait_for` on the webapp loop. On timeout the client receives a 504 and the runner's post-execute hooks are cooperatively cancelled. |
| `max_concurrent` | `int` | `64` | > 0 | Per-worker semaphore capacity for in-flight HTTP requests. The 65th concurrent request returns 503 `status='capacity'` immediately rather than queuing. |
| `max_body_bytes` | `int` | `10485760` (10 MiB) | > 0 | Cap on a single POST body; oversized requests receive 413 `error='request_too_large'` before the body is buffered. |
| `clients` | `list[WebClientConfig]` | one anonymous client (`name='anonymous'`, `token=''`, `rpm=4`) | length >= 1 | Configured tenants. Empty `clients: []` fails at config load. |

#### HTTP clients (`sources.http.clients[]`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `name` | `str` | required | non-empty | Tenant name. Used in metric labels (`drakkar_webapp_requests_total{client=...}`), recorder rows, and the response body. |
| `token` | `str` | `''` | at most one client may have empty token; non-empty tokens unique | Bearer token presented in `Authorization: Bearer <token>`. Empty token = anonymous slot for requests without an `Authorization` header. |
| `rpm` | `int` | `4` | > 0 | Per-client requests-per-minute cap, enforced on a 60-second sliding window. |

```yaml
sources:
  http:
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
```

Env overrides reach list entries by index:
`DK_SOURCES__HTTP__CLIENTS__0__RPM=10`.

When every configured client has an empty token, the worker logs a `webapp_unauthenticated_warning` at startup so private-network deployments that should have had a token configured surface in log aggregation.

---

## Executor Pool (`executor:`)

Controls the subprocess executor pool that runs user-defined binaries.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `binary_path` | `str \| None` | `None` | min length 1 if set | Default binary path for all tasks. If `None`, each [ExecutorTask](executor.md#executortask) returned by [arrange()](handler.md#arrange-required) must provide its own `binary_path`, otherwise the task fails with a clear error. See [Binary Path Resolution](executor.md#binary-path-resolution). |
| `env` | `dict[str, str]` | `{}` | | Environment variables passed to all executor subprocesses. Merged on top of the (filtered) parent process env. Per-task `ExecutorTask.env` overrides these on conflict. See [Environment Variables](executor.md#environment-variables). |
| `env_inherit_parent` | `bool` | `true` | | When `true`, the parent process env is passed to subprocesses (with `env_inherit_deny` patterns applied). Set `false` to run subprocesses with only `executor.env` + `ExecutorTask.env` — fully isolated from the parent env. |
| `env_inherit_deny` | `list[str]` | see below | | Case-insensitive `fnmatch` patterns matched against parent env var names. Matching vars are **not** inherited by subprocesses even when `env_inherit_parent` is `true`. Default excludes `DK_*` internals and common secret names so operator-configured secrets never leak to executor binaries. Deliberately narrower than the recorder's redaction patterns (see [observability.md](observability.md#worker_config-autodiscovery)) — a match here withholds the variable from your binary, so `*_KEY` stays suffix-anchored and names with common non-secret uses (`AUTH_SERVICE_URL`, `CERT_PATH`, `PRIVATE_SUBNET`) are excluded. Set to `[]` to trust the full parent env. Default patterns: `DK_*`, `*PASSWORD*`, `*PASSWD*`, `*SECRET*`, `*TOKEN*`, `*_KEY`, `*_DSN`, `*CREDENTIAL*`, `*SALT*`. |
| `max_executors` | `int` | `4` | >= 1 | Maximum number of concurrent subprocesses. Controls the `asyncio.Semaphore` size -- tasks beyond this limit wait in a queue. See [Concurrency and Backpressure](executor.md#concurrency-and-backpressure). |
| `task_timeout_seconds` | `int` | `120` | >= 1 | Wall-clock timeout (seconds) per subprocess. If a process exceeds this, it is killed and treated as a failure. |
| `max_stdout_bytes` | `int` | `0` | >= 0 | Maximum bytes of stdout retained per task. `0` = unlimited. Excess output is still read and discarded (the process never blocks on a full pipe); the retained prefix is cut at a UTF-8 character boundary and `ExecutorResult.stdout_truncated` is set. |
| `max_stderr_bytes` | `int` | `0` | >= 0 | Same as `max_stdout_bytes`, for stderr; sets `ExecutorResult.stderr_truncated`. |
| `window_size` | `int` | `100` | >= 1 | Maximum number of messages collected per [arrange()](handler.md#arrange-required) [window](executor.md#windowing). Larger windows allow more batching in `arrange()`; smaller windows reduce latency. |
| `max_retries` | `int` | `3` | >= 0 | Maximum number of retry attempts per failed task (0 = no retries). A task can run up to `max_retries + 1` times total. |
| `on_failure` | `'skip'` \| `'dlq'` | `'skip'` | | What the default [on_error()](handler.md#on_error) returns for a failed task. `dlq` writes the task's source messages to the DLQ (`dk-stage: execute`), and no later delivery of those messages starts. Any other value fails the load with `executor.on_failure must be skip or dlq, got '<v>'`. |
| `drain_timeout_seconds` | `int` | `30` | >= 1 | Maximum time (seconds) to wait for in-flight tasks during shutdown or partition revocation. When drain times out, the still-running tasks are cancelled (their subprocess process group is killed) and their offsets are **not** committed — those messages replay on restart (at-least-once). Tune together with `task_timeout_seconds`. |
| `backpressure_high_multiplier` | `int` | `32` | >= 1 | Multiplier for the pause threshold. When total queued messages reach `max_executors * backpressure_high_multiplier`, Kafka consumption is paused. |
| `backpressure_low_multiplier` | `int` | `4` | >= 1 | Multiplier for the resume threshold. When total queued messages drop to `max(1, max_executors * backpressure_low_multiplier)`, Kafka consumption resumes. |

### Backpressure Formula

[Backpressure](performance.md#backpressure) prevents unbounded memory growth by pausing Kafka consumption when too many messages are buffered:

```
high_watermark = max_executors * backpressure_high_multiplier
low_watermark  = max(1, max_executors * backpressure_low_multiplier)
```

With defaults (`max_executors=4`):

- **High watermark** = 4 * 32 = **128** -- pause consumption
- **Low watermark** = max(1, 4 * 4) = **16** -- resume consumption

The gap between high and low watermarks prevents rapid pause/resume oscillation (hysteresis).

```yaml
executor:
  binary_path: /usr/local/bin/my-processor
  max_executors: 8
  task_timeout_seconds: 300
  window_size: 50
  max_retries: 5
  drain_timeout_seconds: 10
  backpressure_high_multiplier: 16
  backpressure_low_multiplier: 2
```

---

## Sinks (`sinks:`)

Sinks define where processed results are delivered. See [Sinks](sinks.md) for payload models, [routing](sinks.md#routing), and the [delivery lifecycle](sinks.md#delivery-lifecycle). Each sink type is a dictionary mapping instance names to their configuration. You can configure multiple instances of the same type (e.g., two Kafka sinks writing to different topics).

### Keys of every sink instance

Every instance of every type also takes the
[failure policy](sinks.md#failure-policy) keys:

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `on_failure` | `'dlq'` \| `'skip'` | `'dlq'` | | What happens when the retry budget is spent. `dlq`: the source message goes to the DLQ and the message is done. `skip`: this sink's payloads are dropped. |
| `max_retries` | `int` | `2` | whole number, 0 to 10 | Retries after the first attempt, spent only by a hook that returns `RETRY` (default 2). The default hook makes one attempt and then applies the declared `on_failure`. The quick internal retry of an idempotent write does not count. |
| `depends_on` | `list[str]` | `[]` | each entry `<type>/<name>` of a configured sink; no self-reference, no cycle | Sinks that must deliver first inside one result. See [Sink dependencies](sinks.md#sink-dependencies-depends_on). |

Environment overrides: `DK_SINKS__<TYPE>__<NAME>__ON_FAILURE`. The load
messages are listed under [Failure policy](sinks.md#failure-policy).

### Kafka Sink (`sinks.kafka.<name>`)

Produces messages to a Kafka topic.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | `str` | *(required)* | Target Kafka topic for output messages. |
| `brokers` | `str` | `''` | Kafka brokers for this sink. If empty, inherits from `kafka.brokers` (same cluster as the source) — and with them `kafka.security` and `kafka.client_config`. |
| `security` | `KafkaSecurityConfig` | PLAINTEXT | Transport authentication and encryption for this sink. Only consulted when `brokers` is set; an inheriting sink uses the consumer's security instead. Same shape as [Kafka security](#kafka-security-kafkasecurity). |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties for this sink, merged after `security`. Only consulted when `brokers` is set. Same rules as [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `ui_url` | `str` | `''` | URL to a web UI for this sink (e.g., Kafka UI, Kowl). Displayed as a link in the operator UI dashboard. |
| `flush_timeout_seconds` | `float` | `30.0` | Bound on the producer flush that ends every delivery (see below). |

Every batch ends with a producer flush — the `AIOProducer` hands messages to
librdkafka only once its internal buffer fills or its one-second inactivity
timer expires, so without the flush every delivery would wait out that timer.
`flush_timeout_seconds` bounds that flush. Unbounded, it becomes librdkafka's
`flush(-1)`, which against a wedged broker blocks until `message.timeout.ms`
(300 s by default) — and it blocks one of the producer's executor threads
while it waits, so a handful of stuck deliveries starve every other delivery
on the same producer. The outer
[`sinks.delivery_timeout_seconds`](#delivery-timeout-sinksdelivery_timeout_seconds)
cannot rescue that: cancelling the await does not stop the thread, so the
bound has to reach librdkafka itself.

On expiry the delivery fails with a `TimeoutError`, which the sink manager
classifies as transient — so the circuit breaker sees the outage and an
idempotent sink still gets its fast-retry. Keep the value generous: it is a
last resort, not a latency target.

### PostgreSQL Sink (`sinks.postgres.<name>`)

Inserts rows into a PostgreSQL database via an asyncpg connection pool.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `dsn` | `str` | *(required)* | | PostgreSQL connection string (e.g., `postgresql://user:pass@host:5432/db`). |
| `pool_min` | `int` | `2` | >= 1 | Minimum number of connections in the pool. |
| `pool_max` | `int` | `10` | >= 1 | Maximum number of connections in the pool. |
| `statements` | `dict[str, str]` | `{}` | | Operator-authored SQL keyed by name, run by `PostgresPayload(op='statement')` with `:name` placeholder binding. See [statement — arbitrary SQL](sink-write-operations.md#statement-arbitrary-sql-operator-controlled). |
| `ui_url` | `str` | `''` | | URL to a database management UI (e.g., pgAdmin). |

### MongoDB Sink (`sinks.mongo.<name>`)

Inserts documents into a MongoDB database via PyMongo's AsyncMongoClient.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uri` | `str` | *(required)* | MongoDB connection URI (e.g., `mongodb://host:27017`). |
| `database` | `str` | *(required)* | Target database name. |
| `statements` | `dict` | `{}` | Operator-authored MQL keyed by name, run by `MongoPayload(op='statement')` with `:name` placeholder binding. See [statement — arbitrary MQL](sink-write-operations.md#statement-arbitrary-mql-operator-controlled). |
| `ui_url` | `str` | `''` | URL to a MongoDB management UI (e.g., Mongo Express). |

### HTTP Sink (`sinks.http.<name>`)

Sends payloads to an HTTP endpoint (JSON by default).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `url` | `str` | *(required)* | `http://` or `https://` scheme; non-empty host; must **not** be a cloud metadata endpoint | Target URL for HTTP requests. Validated at config load time. |
| `method` | `str` | `'POST'` | | HTTP method to use. |
| `timeout_seconds` | `int` | `30` | >= 1 | Request timeout in seconds. |
| `headers` | `dict[str, str]` | `{}` | | Additional HTTP headers sent with each request. |
| `encoding` | `Literal['json', 'form', 'multipart']` | `'json'` | one of `json`, `form`, `multipart`; `headers` must not set `Content-Type` | Request body format. See [Body encoding](#body-encoding-sinkshttpnameencoding). |
| `max_retries` | `int` | `2` | 0 to 10 | The shared key of [every sink instance](#keys-of-every-sink-instance): retries of a failed delivery, spent by `RETRY`. |
| `ui_url` | `str` | `''` | | URL to a related web UI. |

!!! warning "Cloud metadata endpoints are rejected"
    To prevent accidental IAM-credential leaks via SSRF-like misconfiguration,
    the following hosts cannot be used as an HTTP sink target:
    `169.254.169.254` (AWS / Azure / GCP / Alibaba / OpenStack IMDS),
    `metadata.google.internal`, `metadata.packet.net`, `100.100.100.200`
    (Alibaba), `192.0.0.192` (Oracle). Private, loopback, and internal
    hostnames are **not** blocked — internal webhook services remain
    legitimate targets.

#### Body encoding (`sinks.http.<name>.encoding`)

The HTTP sink sends `application/json` by default. Set `encoding` to
`form` or `multipart` for endpoints that require a form body.

| value | body | Content-Type |
|---|---|---|
| `json` (default) | the payload model serialized as JSON | `application/json` |
| `form` | `name=value` pairs | `application/x-www-form-urlencoded` |
| `multipart` | one part per field | `multipart/form-data; boundary=…` |

For `form` and `multipart` the model is flattened to fields sorted by
name. String values are sent as-is; every other value — number, boolean,
null, object, array — is rendered as compact JSON in that field. Multipart
carries fields only, never files.

The Content-Type always derives from `encoding`, so setting a
`Content-Type` in `headers` is a configuration error — rejected even when
the header agrees with the body the encoder would send anyway. For the
default `encoding: json`, a previously configured `Content-Type:
application/json` was correct and worked, as was `application/json;
charset=utf-8`; the `charset` parameter can no longer be expressed at
all. Per RFC 8259, UTF-8 is JSON's default charset, so receivers should
be unaffected. Remove the header, or change the encoding.

**Byte stability.** The encoder is pinned against golden vectors, so the
body is byte-stable across releases for strings, booleans, null, integers,
objects, arrays, and field ordering. Two things are deliberately left
unpinned because JSON encoders legitimately differ on them: float
formatting (`42.0` versus `42`) and whether U+2028/U+2029 inside an
encoded string is escaped or emitted literally. Both forms are numerically
and semantically equivalent, so a consumer must not depend on either.

### Redis Sink (`sinks.redis.<name>`)

Sets key-value pairs in Redis.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `str` | `'redis://localhost:6379/0'` | Redis connection URL. |
| `key_prefix` | `str` | `''` | Prefix prepended to all keys (e.g., `cache:` produces keys like `cache:my-key`). |
| `scripts` | `dict[str, str]` | `{}` | Operator-authored Lua keyed by name, run by `RedisPayload(op='script')` with `keys`/`args` passed as `KEYS`/`ARGV`. See [script — arbitrary Lua](sink-write-operations.md#script-arbitrary-lua-operator-controlled). |
| `ui_url` | `str` | `''` | URL to a Redis management UI (e.g., RedisInsight). |

### Filesystem Sink (`sinks.filesystem.<name>`)

Appends JSONL lines to files on disk.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | `str` | *(required)* | Base directory. Payload paths are resolved relative to it and must stay contained within it — absolute or traversing paths are rejected. |
| `ui_url` | `str` | `''` | URL to a file browser or related UI. |

### Example: Multiple Named Sinks

```yaml
sinks:
  kafka:
    search-results:
      topic: search-results
    analytics:
      topic: analytics-events
      brokers: kafka-analytics:9092

  postgres:
    main-db:
      dsn: postgresql://user:pass@db:5432/myapp
      pool_min: 5
      pool_max: 20
      ui_url: http://pgadmin.internal:5050

  mongo:
    logs:
      uri: mongodb://mongo:27017
      database: app_logs

  http:
    webhook:
      url: https://api.example.com/webhook
      method: POST
      timeout_seconds: 15
      headers:
        Authorization: "Bearer ${API_TOKEN}"
      max_retries: 5
      on_failure: skip               # a lost notification is acceptable
      depends_on: [postgres/main-db] # notify only after the row is written

  redis:
    cache:
      url: redis://redis:6379/0
      key_prefix: "result:"
      ui_url: http://redis-insight.internal:8001

  filesystem:
    archive:
      base_path: /data/archive
```

### Delivery Timeout (`sinks.delivery_timeout_seconds`)

One budget for one `deliver()` call, framework-internal transient retries
included, and for one `close()` during shutdown. Without it a sink whose
server stops answering while the TCP connection stays open blocks its
partition indefinitely: the circuit breaker only counts a failure when a
call *returns*, so `/readyz` keeps reporting ready while the worker does no
work, and every rebalance then spends the whole drain budget waiting.

A timeout is reported as a transient delivery failure, so it reaches
`on_delivery_error`, counts toward the circuit breaker, and carries a message
naming the sink and the budget.

Sinks whose driver accepts one also apply this value as their own transport
timeout — asyncpg `command_timeout`, redis-py `socket_timeout` /
`socket_connect_timeout`, PyMongo `socketTimeoutMS` / `connectTimeoutMS` —
so the driver raises a specific error just before the outer budget expires.
The HTTP sink keeps its own `timeout_seconds`, which is per request.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `delivery_timeout_seconds` | `float` | `30.0` | > 0 | Budget for one sink delivery and one sink close. Also used as the driver-level socket/command timeout where the driver supports it. |

```yaml
sinks:
  delivery_timeout_seconds: 30.0
```

Keep it below `executor.drain_timeout_seconds`: a rebalance drain waits on
in-flight deliveries, so a delivery budget larger than the drain budget
guarantees drain timeouts when a sink goes unresponsive.

### Circuit Breaker (`sinks.circuit_breaker`)

Every registered sink has a per-instance circuit breaker. The breaker
trips after `failure_threshold` consecutive terminal failures (retries
exhausted + DLQ action) and skips all deliveries for `cooldown_seconds`
before allowing a single probe through. A successful probe closes the
breaker; a failing probe reopens with a fresh cooldown. See
[Sinks → Circuit Breaker](sinks.md#circuit-breaker) for the full state
machine and metrics.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `failure_threshold` | `int` | `5` | >= 1 | Consecutive terminal failures required to trip the breaker. A `SKIP` outcome does NOT count — it's operator intent, not a health signal. |
| `cooldown_seconds` | `float` | `30.0` | >= 0.0 | Seconds the breaker stays open before promoting to half-open and allowing a single probe through. |

```yaml
sinks:
  circuit_breaker:
    failure_threshold: 5
    cooldown_seconds: 30.0
```

The breaker defaults are reasonable for most deployments — lower
`failure_threshold` in latency-sensitive pipelines where one stuck sink
would block the whole delivery fan-out; raise `cooldown_seconds` when
probing a recovering downstream is itself expensive.

---

## Dead Letter Queue (`dlq:`)

Messages that fail to parse, to execute or to deliver can be written to a [DLQ](sinks.md#dead-letter-queue) Kafka topic. Each [DLQ record](sinks.md#the-dlq-record) is the original source message (key, value and headers unchanged) with `dk-` headers that say where and why it failed, for later inspection or replay.

**Whether a DLQ producer is built depends on the Kafka source.** The DLQ derives its default topic from the source topic, so a worker without a Kafka source has nothing to derive from and needs an explicit `dlq.topic`:

| `sources.kafka.enabled` | `dlq.topic` | Result |
|---|---|---|
| on | empty | DLQ built, topic `{sources.kafka.topic}_dlq` |
| on | set | DLQ built with that topic |
| off | set | DLQ built with that topic; brokers from `dlq.brokers`, else `kafka.brokers` |
| off | empty | **No DLQ producer.** The summary line reports `dlq=off`. A DLQ send is dropped, logged once per worker run as `dlq_send_dropped_unconfigured` (then at debug), and counted in `drakkar_dlq_unconfigured_drops_total`. |

The startup log `sinks_configured` reports `dlq_topic: ""` when the DLQ is off. `on_parse_error: dlq` is a Kafka-source setting, so it is only reachable when the Kafka source is on — the DLQ is always present for it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | `str` | `''` | DLQ Kafka topic name. If empty, auto-derived as `{sources.kafka.topic}_dlq` (e.g., `input-events_dlq`) — and with the Kafka source off, empty means no DLQ at all (table above). |
| `brokers` | `str` | `''` | Kafka brokers for the DLQ. If empty, inherits from `kafka.brokers` — and with them `kafka.security` and `kafka.client_config`, on the same same-cluster-means-same-credentials rule the Kafka sinks follow. |
| `security` | `KafkaSecurityConfig` | PLAINTEXT | Transport authentication and encryption for the DLQ producer. Only consulted when `brokers` is set. Same shape as [Kafka security](#kafka-security-kafkasecurity). |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties for the DLQ producer, merged after `security`. Only consulted when `brokers` is set. Same rules as [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `flush_timeout_seconds` | `float` | `30.0` | Bound on the producer flush that ends every DLQ write. Same rationale as [the Kafka sink's](#kafka-sink-sinkskafkaname), and it matters more here: the DLQ is the last resort, so a write that blocks for `message.timeout.ms` stalls the partition it was meant to rescue. On expiry the write is reported as unconfirmed and `on_send_failure` applies. |
| `on_send_failure` | `'drop'` \| `'stall'` | `'drop'` | Strategy when no DLQ record can be written (the write fails, or a payload names a sink that is not configured). `drop`: log + count the loss, commit the offset, keep the pipeline moving. `stall`: leave the offset uncommitted and pause the partition until restart — no loss, at the cost of consumer lag. See [When the DLQ itself fails](sinks.md#when-the-dlq-itself-fails). |

```yaml
dlq:
  topic: failed-events
  brokers: kafka-prod:9092
  flush_timeout_seconds: 30.0
  on_send_failure: drop
```

---

## Metrics (`metrics:`)

Prometheus metrics endpoint configuration.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Enable or disable the Prometheus metrics HTTP server. |
| `port` | `int` | `9090` | 1--65535 | Port for the Prometheus metrics endpoint. |
| `task_label_histograms` | `list[str]` | `[]` | | Task label keys whose numeric values are observed into the `drakkar_task_label_value` histogram at task completion, one time series per key (e.g. a file-size or line-count label). Values that do not parse as finite numbers are skipped. |

```yaml
metrics:
  enabled: true
  port: 9090
  task_label_histograms: [file_size_bytes]
```

---

## Runtime health (`runtime_health:`)

Event-loop lag monitoring and stall introspection — see the dedicated
[Runtime Health](runtime-health.md) page for how the monitor works and
how to read its output.

```yaml
runtime_health:
  enabled: true
  tick_seconds: 0.25
  warn_lag_seconds: 0.1
  stall_seconds: 1.0
  max_stall_stacks: 10
  sample_interval_seconds: 10.0
  history_window_seconds: 900
```

## Blocking I/O pool (`io:`)

Sizing for asyncio's default `to_thread` executor — the single process-wide pool
behind every `asyncio.to_thread(...)` call, handler filesystem reads included.
Distinct from `offload.max_threads`, which sizes the separate pool behind
`handler.offload()`. See [Threads & Pools](threads.md) for the full map.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `max_threads` | `int` | `0` | 0--512 | Worker threads in asyncio's default `to_thread` executor. `0` (the default) keeps Python's own sizing, `min(32, cpu_count + 4)`. Set an explicit value when handler I/O concurrency is capped by the pool — e.g. many-core hosts doing wide blocking filesystem fan-out. Blocking I/O releases the GIL, so large values are legitimate here; the trade-off is pressure on the storage behind the calls. |

```yaml
io:
  max_threads: 0
```

---

## Offload pool (`offload:`)

Thread pool behind [`handler.offload()`](offload.md) — CPU-bound hook work moved
off the event loop. See the dedicated [Offload](offload.md) page.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `max_threads` | `int` | `0` | 0--32 | Worker threads in the shared offload pool. `0` (the default) sizes the pool automatically from the executor pool: `ceil(executor.max_executors / 4)`, minimum 2. Calls beyond this many concurrent offloaded computations queue (FIFO) and show up in the `drakkar_offload_queued` gauge. More threads do not speed up pure-Python work (GIL); set an explicit value only when several partitions routinely offload at once and queueing delay matters, or when the offloaded code releases the GIL (numpy, compiled extensions). |

```yaml
offload:
  max_threads: 0
```

---

## Logging (`logging:`)

Structured logging configuration via structlog.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `level` | `str` | `'INFO'` | | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `format` | `str` | `'json'` | `json` or `console` | Output format. `json` produces machine-readable structured logs. `console` produces colorized human-readable output for local development. |
| `output` | `str` | `'stderr'` | | Log destination: `stderr`, `stdout`, or a file path. File paths support `{worker_id}` and `{cluster_name}` template variables. Parent directories are created automatically. |

```yaml
logging:
  level: INFO
  format: json
  output: stderr  # or stdout, or /var/log/drakkar/{worker_id}.log
```

---

## UI / Flight Recorder (`ui:`)

The `ui` section covers the whole operator-UI feature: the [web UI dashboard](observability.md#operator-ui) (FastAPI server) and its presentation settings at the top level, the [flight recorder](observability.md#flight-recorder) (SQLite-backed event log) under `ui.recorder.*`, drakkar-ui bundle fetching under `ui.release.*`, plus WebSocket live streaming and [worker autodiscovery](observability.md#worker-autodiscovery). This is the largest configuration section.

### Authentication

The UI's auth is **opt-in by default**. With `ui.enabled=true` and `ui.auth_token` empty (the default), the worker emits a structured `ui_unauthenticated` warning at startup naming the bound host:port and the two opt-in paths (YAML key + env var), then continues starting normally. No endpoint stops a worker, replays Kafka messages, mutates sinks, or commits offsets, and Drakkar is intended to run inside a private contour (VPC / internal cluster / operator-only ingress), so the framework treats "unauthenticated + warned" as a reasonable starting point rather than a misconfiguration.

**Most endpoints are read-only, but not all.** Two have real side effects, and each can be closed independently of `auth_token`:

| Endpoint | Effect | Switch |
|----------|--------|--------|
| `POST /api/v1/debug/probe` | Runs caller-supplied bytes through the live handler and the real executor subprocess pool, competing with production traffic for executor slots. (It writes no sinks, recorder rows, cache entries, or offsets — see [Message Probe](observability.md#operator-ui).) | `ui.probe_enabled: false` |
| `POST /api/v1/debug/merge` | Writes a new `merged-<ts>-<token>.db` into `<shared.dir>/merged/`. The archive pass deletes it after `history.merged_retention_days` (default 32); with archiving off, or with that set to `0`, nothing reclaims it, so repeated calls grow unbounded. | `ui.merge_enabled: false` |

Both default to `true`, and both return `403` with `{"error": "..."}` naming the config key when switched off. The switches are deliberately independent of `auth_token` in both directions: a deployment that cannot set a token can still close the endpoints, and a deployment that has one can still close them as defence in depth — a valid token is not the same as consent to burn executor slots on demand. The startup warning names whichever of the two is still enabled, so an operator who has already closed them is not warned about exposure they do not have.

To require auth on the protected endpoints (database download, merge, message probe) and the WebSocket live-event stream, set a strong token:

- **Generate a strong value:** `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- **Configure via YAML:** set `ui.auth_token: <value>` in your config.
- **Or via environment:** export `DK_UI__AUTH_TOKEN=<value>` (overrides YAML when both are set).

When the token is set, protected endpoints reject requests without a matching `Authorization: Bearer <token>` header (or `?token=<token>` query parameter); the WebSocket additionally validates the `Origin` header against `allowed_ws_origins` (or the request's `Host` header), and so do the three cross-origin reads of the UI fan-out. The routes under `/api/v1/peer/` are never protected: other workers call them without a token for the [cache exchange](cache.md#cache-exchange). See [Security posture](security.md#the-public-peer-route). Comparison uses `secrets.compare_digest` for timing-side-channel safety; leading/trailing whitespace in the configured token is stripped on load (a `auth_token: " "` of only spaces is treated as empty and the warning still fires).

The unauthenticated-startup warning runs at `DrakkarApp._async_run()` startup, before the recorder and UI server are constructed. See `drakkar/app_security.py::warn_if_ui_unauthenticated` for the implementation.

### Server Settings

Top-level `ui.*` fields configure the UI server itself and the presentation settings its pages consume.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Enable or disable the entire UI feature. Set to `false` to skip the flight recorder, web UI, and all associated overhead. |
| `host` | `str` | `'127.0.0.1'` | | Bind address for the UI server. Default `127.0.0.1` (localhost only). Use `0.0.0.0` to expose on all interfaces. Auth is opt-in regardless of host — when binding to a non-loopback address inside anything other than a fully-trusted private network, set `auth_token` (see below). |
| `port` | `int` | `8080` | 1--65535 | Port for the web UI (FastAPI server). |
| `auth_token` | `str` | `''` | | Bearer token for sensitive endpoints (database download, merge, message probe) **and** for the WebSocket live-event stream at `/ws`. **Empty (the default) disables auth entirely** — every endpoint is reachable without credentials and the WebSocket skips both token and Origin checks. This is intentional: the UI is intended for private-network deployments, and a startup warning (`ui_unauthenticated`) names the unauthenticated posture in logs along with any side-effecting endpoint still enabled. When set to a non-empty value, protected HTTP endpoints require `Authorization: Bearer <token>` header or `?token=<token>` query parameter; WebSocket connections without a valid token are closed with code 4401. Comparison uses `secrets.compare_digest` to avoid timing side-channels. Leading/trailing whitespace is stripped on config load so `auth_token: " secret "` in YAML still works (and a token of only spaces is treated as empty). Read-only pages (dashboard, live, partitions, sinks, history) are always accessible. `/healthz`, `/readyz` and the routes under `/api/v1/peer/` (the [cache exchange](cache.md#cache-exchange)) never need the token. With a token set, list every worker origin in `allowed_ws_origins` so the UI can read the other workers of the cluster. |
| `probe_enabled` | `bool` | `true` | | Serve `POST /api/v1/debug/probe`. The probe runs caller-supplied bytes through the live handler and the real executor subprocess pool, so it competes with production traffic for executor slots. `false` serves `403` instead — independently of `auth_token` (see [Authentication](#authentication)). Probes never write sinks, recorder rows, cache entries, or offsets, so switching it off costs no pipeline behavior. |
| `merge_enabled` | `bool` | `true` | | Serve `POST /api/v1/debug/merge`. Each call writes a new `merged-<ts>-<token>.db` into `<shared.dir>/merged/`. The archive pass deletes merge results after `history.merged_retention_days`; with archiving off, or with that set to `0`, nothing reclaims them. `false` serves `403` instead — independently of `auth_token`. |
| `kafka_read_enabled` | `bool` | `true` | | Serve `GET /api/v1/debug/kafka/*` — ad-hoc reads of the configured topics (source, DLQ, and each Kafka sink by instance name; never an arbitrary topic). Reads use `assign()`-only consumers that join no consumer group and commit no offsets, so they are invisible to the pipeline. `false` serves `403` instead — independently of `auth_token`. When the resolved Kafka security of any readable topic is not PLAINTEXT while `auth_token` is empty, startup logs a warning naming the exposed aliases. See [Kafka Read API](kafka-read.md). |
| `allowed_ws_origins` | `list[str]` | `[]` | | Explicit allowlist of WebSocket `Origin` header values, consulted on every handshake whether or not `auth_token` is set. The same rule decides the cross-origin reads (CORS) of `/api/v1/debug/trace`, `/api/v1/debug/trace-by-label` and `/api/v1/debug/databases`, which the UI sends to every worker of the cluster; with `auth_token` set, list the origin of every worker here, or those reads fail and the UI shows the other workers as unreachable (see [Cross-origin reads](security.md#cross-origin-reads)). Non-empty list = strict allowlist; any Origin not in the list is rejected with close code 4403. Empty list + non-empty `auth_token` = same-origin fallback: Origin host must match the `Host` header. Empty list + empty `auth_token` = the cross-origin handshakes the cluster view needs are accepted, except a page that is not itself on loopback reaching a worker over loopback -- no peer worker is ever at the browser's own loopback address. Comparison is case-insensitive and normalizes default ports (`:80` for http, `:443` for https) so `https://ops.internal` and `https://ops.internal:443` are equivalent. Missing Origin header (non-browser clients) is always accepted -- a browser cannot omit it. |
| `public_url` | `str` | `''` | | External URL for this worker's UI. Used when workers discover each other -- if set, this URL is advertised instead of the auto-detected `http://{ip}:{port}`. Useful behind load balancers or Kubernetes ingresses. |
| `expose_env_vars` | `list[str]` | `[]` | | List of environment variable names to capture and store in `worker_config.env_vars_json`. Useful for recording deployment metadata (e.g., `['GIT_SHA', 'DEPLOY_ENV', 'K8S_POD_NAME']`). |
| `max_rows` | `int` | `5000` | >= 100 | Maximum number of rows returned to the web UI in list views. |

### Duration Thresholds

These thresholds control which events are recorded, logged, and streamed. See [Duration Thresholds](observability.md#duration-thresholds) for detailed behavior and [Performance Tuning](performance.md) for recommendations. They help reduce noise in high-throughput systems where most tasks complete quickly. The log and WebSocket thresholds are top-level `ui.*` fields; the persistence thresholds live under `ui.recorder.*`.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `log_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to emit a `slow_task_completed` or `slow_task_failed` log message. Set to `0` to log all tasks. |
| `ws_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to broadcast via WebSocket to the live UI. Fast tasks that complete under this threshold are invisible in the live view (reduces UI noise). Failed tasks always appear regardless. Set to `0` to show all tasks. |
| `recorder.event_min_duration_ms` | `int` | `0` | >= 0 | Minimum task duration (ms) to persist to the SQLite database. Set above `0` to skip storing fast tasks entirely. |
| `recorder.output_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to include stdout/stderr in the persisted event record. Tasks under this threshold are recorded but without output data. |

### Prometheus Integration

These settings add clickable Prometheus graph links to the dashboard.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prometheus_url` | `str` | `''` | Base URL of your Prometheus server (e.g., `http://prometheus:9090`). If empty, no Prometheus links are shown in the UI. |
| `prometheus_rate_interval` | `str` | `'5m'` | Rate interval used in PromQL `rate()` expressions for dashboard links (e.g., `1m`, `5m`, `15m`). |
| `prometheus_worker_label` | `str` | `''` | PromQL label filter for worker-scoped queries. Supports template variables: `{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`. If empty, defaults to `instance="{hostname}:{metrics_port}"`. Example: `worker_id="{worker_id}"`. |
| `prometheus_cluster_label` | `str` | `''` | PromQL label filter for cluster-wide queries. Supports the same template variables. Example: `cluster="{cluster_name}"`. If empty, cluster-wide links are not shown. |

### Custom Links

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `custom_links` | `list[dict[str, str]]` | `[]` | List of custom links displayed in the dashboard navigation. Each entry is a dict with `name` and `url` keys. URL values support template variables: `{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`. |

### Probe-Details Link Bases

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `link_bases` | `dict[str, str]` | `{}` | Named URL bases that [probe-details link templates](ui-enrichment.md#link-templates) resolve `{<base>}` tokens against — e.g. `{jira: 'https://jira.internal.example.com'}` resolves `{jira}` in a template like `{jira}/browse/{value}`. Base names are lower-case identifiers (`^[a-z][a-z0-9_]*$`); values must start with `http://` or `https://` and a trailing `/` is stripped. A base a registered layout references but this map omits logs one startup warning; the affected links render as plain text rather than failing. Reported on `GET /api/v1/identity` (empty object when unset). |

```yaml
ui:
  link_bases:
    jira: 'https://jira.internal.example.com'
    jenkins: 'https://jenkins.internal.example.com'
```

### Custom Cell Renderers

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `custom_renderers_path` | `str` | `''` | Path to a deployment-provided JavaScript module (see [Custom cell renderers](ui-enrichment.md#custom-cell-renderers)) served at `GET /api/v1/ui/renderers.js` and dynamically imported by the UI. The module's default export maps renderer names to `(value, row, cell) => HTMLElement` functions, referenced from `probe_field(view='custom', renderer=...)`, `Column(renderer=...)`, or `Element(view='custom', renderer=...)`. The file must exist at boot -- a missing or unreadable path fails startup. Empty (the default) turns the feature off; reported as `custom_renderers: false` on `GET /api/v1/identity`. |

```yaml
ui:
  custom_renderers_path: /etc/drakkar/renderers.js
```

### Consume Pause (`ui.consume_pause:`)

Timed debug pause of message intake, driven from the Live page — see the dedicated
[Consume Pause](consume-pause.md) page.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Serve the consume-pause API (`/api/v1/debug/consume-pause`) and show the pause control on the Live page. Off by default: pausing stops message intake for the chosen duration, which is a production-affecting act — enable it deliberately, for debug-friendly deployments. |
| `durations_seconds` | `list[int]` | `[15, 60, 300, 900]` | Preset pause durations (seconds) offered as one-click buttons on the Live page. Each preset must sit in the same 1--3600 range the API enforces; the API itself accepts any duration in that range regardless of the presets. |

### Probe Details Caps (`ui.probe_details:`)

Write caps for the Message Probe's user-defined details tab (`max_writes`, default
`10000`; `max_total_bytes`, default `5000000`) — see
[Probe User Details](probe-user-details.md) for the feature and when to raise them.

### Timeline Tuning (`ui.timeline:`)

Live-timeline history depth, first-match-wins bar color rules, and label roles —
covered field-by-field on the dedicated [Timeline Tuning](ui-timeline.md) page.
`ui.timeline.events` — handler-declared custom event types drawn as markers, ranges,
or flag pins — is covered on [Custom Timeline Events](ui-timeline-events.md).

### Operator Docs Site (`ui.docs:`)

A prebuilt static docs site the worker serves at `/docs/`, plus the contextual
anchors that link UI surfaces (a label, a sink, a timeline event type, a declared
page) into it — covered field-by-field on the dedicated
[Operator Docs Site](ui-docs.md) page.

### Trace Over History (`ui.trace:`)

Budget of one Message Trace request with `history=1`, which searches the sealed files in the history folder of the worker's own cluster, newest first. With `ui.recorder.persistence: postgres` the request searches the [Postgres table](recorder-postgres.md#the-history-search) instead: `history_max_seconds` is its time limit (at most 3600 seconds on the server) and `history_max_files` does not apply. See [Cross-Worker Trace](observability.md#cross-worker-trace).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `history_max_files` | `int` | `256` | >= 1 | Most sealed history files one trace request with `history=1` opens, newest first. |
| `history_max_seconds` | `float` | `10.0` | > 0 | Time limit for one trace request with `history=1`; the file history search stops and returns what it found. With `ui.recorder.persistence: postgres` a search past the limit answers 503 and returns no rows (the limit is capped at 3600 s). |

### Flight Recorder (`ui.recorder:`)

Flight-recorder persistence — the UI's data store. The recorder keeps recent events in an in-memory database, the memory store, and with `persistence: sqlite` it also writes recorder files into [`storage.dir`](#storage-and-shared). With `persistence: postgres` it also writes the events to one table in Postgres; see [Recorder History in Postgres](recorder-postgres.md) and the `postgres.*` keys below. See [Recent Events in Memory](recent-events.md). Any combination of the `store_*` flags is valid. Archiving is configured under [`history.archive`](#history).

The persistence mode follows this table when `persistence` is not set:

| `persistence` | `storage.dir` | Mode |
|---|---|---|
| not set | a folder | `sqlite` |
| not set | `''` | `none` |
| `none` | anything | `none` |
| `sqlite` | a folder | `sqlite` |
| `sqlite` | `''` | the config does not load |
| `postgres` | anything | `postgres` |

`postgres` is never derived: write it in the config. So a config with `storage.dir: ''` runs in `none` mode with no change. An explicit `sqlite` with an empty `storage.dir` fails the load (whatever `ui.enabled` says) with:

```
ui.recorder.persistence is sqlite but storage.dir is empty; set storage.dir to a folder, or set ui.recorder.persistence to none (or remove it) to keep recorder events in memory only
```

With `postgres`, the load fails when `ui.recorder.postgres.connection` is not declared:

```
ui.recorder.postgres.connection '<name>' is not defined under connections.postgres
```

These checks run after the source check and before the history window check: first the `sqlite` folder check, then the `postgres` connection check. The config summary line shows ` recorder=postgres` in the same place whenever the mode is `postgres`, with or without a `storage.dir`. It shows ` recorder=none` after ` storage=...` (and the optional ` shared=...`) when the mode is `none` and `storage.dir` is not empty. With an empty `storage.dir` the line shows `storage=mem` and no token.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `persistence` | `none` \| `sqlite` \| `postgres` \| `null` | `null` | one of the three when given | Where recorder events live besides the memory store: `sqlite` writes recorder files into `storage.dir`; `none` keeps events in memory only; `postgres` writes them to a table in Postgres and is never derived. Unset, the mode follows the table above. Env `DK_UI__RECORDER__PERSISTENCE`. |
| `postgres.connection` | `str` | `'main'` | a name under `connections.postgres` | The connection entry that holds the DSN and the schema. Env `DK_UI__RECORDER__POSTGRES__CONNECTION`. |
| `postgres.table_prefix` | `str` | `''` | empty, or `^[a-z][a-z0-9_]{0,23}$` | Empty for the table `<schema>.recorder_events`, else `<schema>.<prefix>_recorder_events`. A bad value fails the load with `ui.recorder.postgres.table_prefix '<value>' must be empty, or a lower-case letter followed by at most 23 lower-case letters, digits or underscores`. Env `DK_UI__RECORDER__POSTGRES__TABLE_PREFIX`. |
| `postgres.retention_hours` | `int` | `6` | `1` to `168` | Hours of history the table keeps. Read only when this worker creates the table; after that the value in the table comment rules. Env `DK_UI__RECORDER__POSTGRES__RETENTION_HOURS`. |
| `postgres.store_output` | `bool` | `false` | | Send the `stdout` and `stderr` columns to Postgres. Off, both are written as `NULL`; `stdout_size` and `metadata` stay. Env `DK_UI__RECORDER__POSTGRES__STORE_OUTPUT`. |
| `postgres.batch_rows` | `int` | `5000` | `100` to `50000` | Most rows in one COPY. Env `DK_UI__RECORDER__POSTGRES__BATCH_ROWS`. |
| `postgres.queue_max_mib` | `int` | `128` | `16` to `1024` | Most text data, in MiB, waiting for Postgres. When full, the oldest rows are dropped and counted. Env `DK_UI__RECORDER__POSTGRES__QUEUE_MAX_MIB`. |
| `postgres.resilience.*` | block | see [Resilience](#resilience-block) | | Timeout, retries, backoff and breaker of the recorder calls to Postgres (feature `recorder`). Env `DK_UI__RECORDER__POSTGRES__RESILIENCE__<KEY>`. |
| `memory.window_seconds` | `int` | `120` | `10` to `86400` | Keep recorder events in the memory store this many seconds. Env `DK_UI__RECORDER__MEMORY__WINDOW_SECONDS`. |
| `memory.rows_per_executor` | `int` | `3000` | `100` to `1000000` | Row cap of the memory store per executor slot: the store holds at most this many rows times `executor.max_executors`. The cap does not scale with the window; a longer window needs a larger value (rows per executor per window is about 4 x window / average task seconds). Env `DK_UI__RECORDER__MEMORY__ROWS_PER_EXECUTOR`. |
| `memory.max_mib` | `int` | `256` | `16` to `512` | Budget for the data in the memory store, in MiB (pages in use). It bounds the data, not the memory the store holds. Plan up to about 600 MiB of RAM for the store at the default. Env `DK_UI__RECORDER__MEMORY__MAX_MIB`. |
| `store_events` | `bool` | `true` | | Write processing events (consumed, task_started, task_completed, task_failed, etc.) to the `events` table of the memory store and, with `sqlite`, of the file. Off, there is no memory store. Disable to reduce disk I/O on high-throughput workers. |
| `store_config` | `bool` | `true` | | Write worker configuration to the `worker_config` table, so every recorder file names the worker and cluster that wrote it (history, merges, archives). |
| `store_state` | `bool` | `true` | | Periodically snapshot worker state (uptime, partitions, pool utilization, queue depth, counters) to the `worker_state` table. Used only with `sqlite`; with `none` and `postgres` the key loads and is ignored. |
| `state_sync_interval_seconds` | `int` | `10` | >= 1 | Interval (seconds) between worker state snapshots. |
| `rotation_interval_hours` | `int` | `1` | >= 1 | How often (hours) to rotate the SQLite database file. On rotation, a new timestamped file is created and the old one is sealed; a background job moves it into `<shared.dir>/history/<cluster>/` (see [Sealing](local-databases.md#sealing-and-the-history-folder)). Every rotation tick also retries failed moves. The archive pass runs at its own [slots](local-databases.md#archive-slots), not at the rotation. Rotation itself deletes nothing — see [Archiving](local-databases.md#archiving). Used only with `sqlite`; with `none` and `postgres` the key loads and is ignored. |
| `store_output` | `bool` | `true` | | Include subprocess stdout/stderr in event records. Disable to save disk space when output is large or not needed for debugging. |
| `store_stdin` | `bool` | `false` | | Store each task's stdin content (capped at `stdin_max_bytes`) in the `task_started` event metadata, so the debug UI can show exactly what a task consumed. Off by default: on a high-fan-out workload stdin is the largest payload the recorder would write. Failed tasks always store their stdin (capped) on the `task_failed` event, regardless of this flag. |
| `stdin_max_bytes` | `int` | `65536` | >= 0 | Byte cap for stored stdin content (`store_stdin`, and the always-on failed-task capture). `0` = unlimited. Truncation is flagged as `stdin_truncated` in the event metadata. |
| `flush_interval_seconds` | `int` | `5` | >= 1 | The tick (seconds) of the memory loop, the file loop and, with `postgres`, the Postgres loop. |
| `max_buffer` | `int` | `50000` | >= 1000 | The intake buffer and, with `sqlite`, the file queue each hold at most this many rows; with `postgres`, the Postgres queue too. When one is full, its oldest rows are dropped and counted. |
| `max_flush_retries` | `int` | `3` | >= 1 | Used only with `sqlite`; with `none` and `postgres` the key loads and is ignored. How many times a flush batch is re-queued on transient `OperationalError` (database is locked, disk I/O error, etc.) before the batch is dropped. On drop, `drakkar_recorder_flush_batches_dropped_total` ticks; on each retry `drakkar_recorder_flush_retries_total` ticks. |
| `event_min_duration_ms` | `int` | `0` | >= 0 | See [Duration Thresholds](#duration-thresholds) above. |
| `output_min_duration_ms` | `int` | `500` | >= 0 | See [Duration Thresholds](#duration-thresholds) above. |
| `annotations_enabled` | `bool` | `true` | | Record handler [annotations](annotations.md) (diagnostic records attached to a window, message, or task from inside a hook) as rows in the `events` table. |
| `annotation_max_bytes` | `int` | `16384` | >= 0 | Byte cap for one annotation payload; an oversize record is dropped whole rather than truncated. `0` disables the cap. See [Annotations](annotations.md) for the budgets and the drop policy. |
| `annotation_max_bytes_per_call` | `int` | `262144` | >= 0 | Total annotation bytes one hook invocation may add — bounds a handler annotating every message of a wide window. `0` disables the cap. |
| `annotation_log_max_bytes` | `int` | `2048` | >= 0 | Cap on the payload copy written to the warning log when an annotation is dropped — log lines usually ship to a metered aggregator. |

### UI Release (`ui.release:`)

Decoupled drakkar-ui bundle fetching. The UI ships as its own versioned bundle (the separate drakkar-ui repo, published to GitHub Releases); when enabled, the worker resolves that bundle (cache → fetch) and serves it. A fetch failure is never fatal: release tags are immutable, so a bundle downloaded once serves from the shared cache on every later start — including offline ones. When nothing can be resolved at all the worker runs API-only and page requests answer 503 with how to supply a bundle; there is no built-in HTML fallback. See [Managing the UI bundle cache](observability.md#managing-the-ui-bundle-cache).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `true` | Resolve and serve the drakkar-ui bundle. Off means no UI at all (no fetch, no cache read): the JSON API, the health probes and the event WebSocket keep working, and page requests answer 503. |
| `repo` | `str` | `'wlame/drakkar-ui'` | The `owner/name` GitHub repo that publishes UI bundles. Empty disables fetching — only an already-cached bundle is served, which is how an air-gapped deployment pins itself to a bundle staged with the `drakkar-ui` CLI. |
| `pinned_version` | `str` | `''` | Known-good UI release tag this backend is built against (e.g. `v1.2.0`); the contract is API-major compatible. Empty means "no pinned version". |
| `cache_dir` | `str` | `''` | Bundle cache root override. Empty uses the per-user cache dir (`$XDG_CACHE_HOME/drakkar/ui`, falling back to `~/.cache/drakkar/ui` — so co-located workers share one cache). |
| `check_update` | `bool` | `true` | Resolve the latest release tag on startup instead of only the pinned version. Already-cached versions are never re-downloaded — release tags are immutable. |

```yaml
ui:
  enabled: true
  port: 8080
  public_url: https://drakkar-ui.example.com/worker-1
  expose_env_vars:
    - GIT_SHA
    - DEPLOY_ENV
    - K8S_POD_NAME
  max_rows: 5000
  log_min_duration_ms: 1000
  ws_min_duration_ms: 500
  prometheus_url: http://prometheus:9090
  prometheus_rate_interval: 5m
  prometheus_worker_label: 'worker_id="{worker_id}"'
  prometheus_cluster_label: 'cluster="{cluster_name}"'
  custom_links:
    - name: Grafana Dashboard
      url: http://grafana:3000/d/drakkar?var-worker={worker_id}
    - name: Kibana Logs
      url: http://kibana:5601/app/discover#/?_a=(query:(match_phrase:(worker_id:'{worker_id}')))
  link_bases:
    jira: 'https://jira.internal.example.com'
    jenkins: 'https://jenkins.internal.example.com'
  custom_renderers_path: /etc/drakkar/renderers.js
  recorder:
    store_events: true
    store_config: true
    store_state: true
    state_sync_interval_seconds: 10
    rotation_interval_hours: 1
    store_output: true
    flush_interval_seconds: 5
    max_buffer: 50000
    max_flush_retries: 3
    event_min_duration_ms: 100
    output_min_duration_ms: 1000
  release:
    enabled: true
    repo: wlame/drakkar-ui
    pinned_version: ""
    check_update: true
```

---

## Cache (`cache:`)

A handler-accessible key/value cache with in-memory hot reads, write-behind SQLite persistence, and an optional cross-worker [cache exchange](cache.md#cache-exchange) over HTTP. **Disabled by default** -- when `enabled: false`, `handler.cache` is a no-op stub so handler code can call `self.cache.set(...)` unconditionally without `if` guards.

See [Cache](cache.md) for the full API (`set` / `peek` / `get` / `delete`) and the [periodic loops](cache.md#how-it-flows) (flush, cleanup, exchange).

### Cache Settings

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `false` | | Master switch. When `false`, the cache is a no-op stub. The cache file `<worker_id>-cache.db` lives in [`storage.dir`](#storage-and-shared) and is private to the worker; `storage.dir` does not need to be shared for the cache. With `storage.dir: ""` the engine warns (`cache_engine_disabled_no_storage_dir`) and continues without persistence -- in-memory only, and with no cache exchange. |
| `flush_interval_seconds` | `float` | `3.0` | > 0 | Interval (seconds) for the write-behind loop that drains dirty in-memory entries to SQLite. Lower = less data loss on crash; higher = less write amplification. |
| `cleanup_interval_seconds` | `float` | `60.0` | > 0 | Interval (seconds) for the loop that deletes rows whose `expires_at_ms` has passed and refreshes Prometheus DB-size gauges. |
| `max_memory_entries` | `int \| null` | `10000` | >= 1 or `null` | Cap for the in-memory LRU. `null` = unbounded (a startup warning fires so the choice is visible in logs). The DB is the source of truth, so eviction never loses data -- evicted entries re-warm on the next `get()`. |

```yaml
cache:
  enabled: true
  flush_interval_seconds: 3.0
  cleanup_interval_seconds: 60.0
  max_memory_entries: 10000           # null = unbounded (warns)
```

### Cache exchange (`cache.exchange:`) {#cache-exchange}

How this worker pulls `cluster` and `global` cache rows from its peers over
HTTP (`GET /api/v1/peer/cache/changes`, last-write-wins by `updated_at_ms`),
and whether it serves its own rows to them. The peers come from
[`autodiscover`](#autodiscover): with it off the exchange does nothing and the
worker logs `cache_exchange_off_no_autodiscover` once at startup. See
[Cache](cache.md#cache-exchange).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | When `false`, only the local SQLite is used -- no cross-worker propagation: the worker pulls nothing, its card carries no `features.cache`, and its peer route answers `404`. Flush and cleanup loops still run. Needs `cache.enabled` and a non-empty `storage.dir`. |
| `interval_seconds` | `float` | `30.0` | > 0 | Interval (seconds) between exchange cycles. |
| `page_size` | `int` | `500` | 1-1000 | Maximum rows in one page pulled from a peer. A pull asks for pages until the peer answers `more: false`. |
| `timeout_seconds` | `float` | `5.0` | > 0 | Time limit for one request to a peer; the connect timeout is 2 seconds. One slow peer cannot block the rest. |
| `cycle_deadline_seconds` | `float \| null` | `null` | >= 0.1 and `<` `interval_seconds` | Hard wall-clock cap on a single cycle. `null` derives `interval_seconds * 0.9`. Must be strictly less than `interval_seconds` -- config load fails otherwise so the misconfiguration surfaces at startup. Peers still pulling or waiting when it fires are cut, and are not counted as failures. |
| `max_concurrent_peers` | `int` | `4` | >= 1 | Peers pulled from in parallel. The others wait for a free slot within the same cycle. |

```yaml
cache:
  exchange:
    enabled: true
    interval_seconds: 30.0
    page_size: 500
    timeout_seconds: 5.0
    cycle_deadline_seconds: null      # null = interval_seconds * 0.9
    max_concurrent_peers: 4
```

---

## Own storage and the shared folder (`storage:`, `shared:`) {#storage-and-shared}

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage.dir` | `str` | `'/tmp/drakkar'` | This worker's own files: the live recorder file, the cache file, the crash marker. Only this worker opens them. The worker creates the folder with mode `0700` at startup when it is missing; an existing folder keeps its mode. `''`: no own files, no cache file and no cache exchange; the recorder mode derives to `none` and keeps recent events in memory only. With a `shared.dir` the worker still reads the sealed history. |
| `shared.dir` | `str` | `''` | The folder every worker of one fleet sees: registry cards, sealed history, daily archives, merge results. `''` means the same folder as `storage.dir`, which is right for workers on one host. |

```yaml
storage:
  dir: /var/lib/drakkar        # per-worker volume
shared:
  dir: /mnt/fleet              # one volume all workers mount
```

The [upgrade guide](upgrading.md) explains how to move from the old `ui.recorder.db_dir` key.

---

## History (`history:`) {#history}

What happens to recorder history after a worker is done with a file. See
[Archiving](local-databases.md#archiving).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `archive.enabled` | `bool` | `true` | | Fold recorder files into one compressed archive per cluster and window, then delete the files a pass merged. `false` disables the pass entirely — no sealed recorder file and no merge result is ever deleted automatically. The pass runs only with `ui.recorder.persistence` `sqlite`; with `none` every `history.archive.*` key loads and is ignored. |
| `archive.window_hours` | `int` | `24` | >= 1, and >= `ui.recorder.rotation_interval_hours` | Width of one archive window in hours (UTC epoch-aligned). |
| `archive.due_after_hours` | `int` | `48` | >= 1, and >= `ui.recorder.rotation_interval_hours` | A window is archived this many hours after it ends, so late files of that window are still in it. |
| `archive.retention_days` | `int` | `30` | >= 0; when non-zero, `retention_days * 24 >= due_after_hours + window_hours` | How long archives are kept before deletion. Defaults to 30 days because nothing else reclaims an archive. `0` keeps them forever and logs a `recorder_archives_unbounded` warning at startup. |
| `archive.interval` | `int \| null` | `null` | a whole number, at least `30`; at most one of `interval` and `cron` | Seconds between archive slots, aligned to the epoch (a slot at every whole multiple since 1970-01-01T00:00:00Z). With neither `interval` nor `cron`, a slot every `3600` seconds. At each slot one worker of the cluster runs the archive pass. See [Archive slots](local-databases.md#archive-slots). |
| `archive.cron` | `str \| null` | `null` | a five-field UTC cron expression in the [accepted subset](periodic-tasks.md#cron) | The archive slots as cron fire times, instead of `interval`. |
| `archive.lease_seconds` | `int` | `60` | `40` to `86400` | The lease of an archive slot. The holder refreshes it while the pass runs; a lease nobody refreshed this long belongs to a dead archiver and is taken over. Used by every [election provider](local-databases.md#the-election-provider). |
| `archive.sweep_after_seconds` | `int` | `7200` | >= `60` | An archive work file in the history folder (a merge or compress file, a sealing copy on its way in, an orphan meta file) older than this belongs to a dead worker and is deleted by the pass. Kept apart from the lease, so a short lease does not delete a slow sealing copy. |
| `merged_retention_days` | `int` | `32` | >= 0 | Merge results from the UI (`<shared.dir>/merged/`) are deleted by the archive pass after this many days, so only while `archive.enabled` is true. `0` keeps them forever. |

```yaml
history:
  archive:
    enabled: true
    window_hours: 24
    due_after_hours: 48
    retention_days: 30
    interval: 3600            # or cron: '0 * * * *'; give at most one
    lease_seconds: 60
    sweep_after_seconds: 7200
  merged_retention_days: 32
```

The load checks the schedule keys in this order, with the prefix
`history.archive: `: `give at most one of interval and cron`, the
[cron messages](periodic-tasks.md#cron-messages),
`interval must be a whole number of seconds and at least 30`,
`lease_seconds must be a whole number from 40 to 86400`,
`sweep_after_seconds must be at least 60`.

---

## Autodiscover (`autodiscover:`) {#autodiscover}

How workers find each other, and which worker of a cluster runs each slot of a
cluster periodic task (the archive included). One provider serves both jobs. The
feature is optional and off by default; the pipeline (sources, executor, sinks,
offset commits) does not depend on it. See [Fleet registry](fleet.md) and
[Periodic Tasks](periodic-tasks.md).

A worker with it off publishes no card, reads no peers, runs no election and
pulls no cache rows. A declared `scope='cluster'` task then fails startup.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `false` | | Turn on the registry and the election. |
| `provider` | `str` | `''` | `consul`, `filesystem`, `postgres`, `redis`; required when enabled | `filesystem` (cards in `<storage.shared_dir>/fleet/workers`, locks in `<storage.shared_dir>/tasks/<cluster>`; needs `storage.shared_dir`), `postgres` (tables of the database named by `connection`; see [Fleet registry](fleet.md#postgres) and [The postgres election provider](local-databases.md#the-postgres-election-provider)), `redis` (keys of the Redis server named by `connection`), `consul` (keys of the Consul agent named by `connection`, each held by a session). Every worker of a cluster must use the same provider. |
| `connection` | `str` | `main` | non-empty | The entry of `connections.<provider>` that the `postgres`, `redis` and `consul` providers use. With one of these providers enabled, the entry must be declared. The `filesystem` provider ignores it. |
| `heartbeat` | duration | `10s` | whole seconds, >= `1s` | Time between card publishes. |
| `refresh` | duration | `15s` | whole seconds, >= `1s` | Time between reads of the peer cards. |
| `offline_after` | duration | `1m` | whole seconds, >= `10s`, and >= `2 * heartbeat`; at most `24h` when `provider` is `consul` | A peer whose card is older than this (on the provider's clock) is reported `online: false` in the workers list. A provider that decides liveness itself overrides this rule. The `consul` provider decides it by the session of the card and uses this value for the session's time to live. |
| `prune_after` | duration | `24h` | whole seconds, >= `offline_after` | Cards older than this are deleted. Not used by the `consul` provider: Consul deletes a card when its session ends. |
| `resilience` | block | see [Resilience](#resilience-block) | | Time limit, retries and circuit breaker for the calls to the provider. The registry and the election each build their own caller from this block, so each has its own breaker. |

```yaml
autodiscover:
  enabled: true
  provider: redis
  connection: main
  heartbeat: 10s
  refresh: 15s
  offline_after: 1m
```

### Resilience block (`resilience:`) {#resilience-block}

How the worker calls a provider: a time limit per attempt, retries with
doubling waits, and a circuit breaker. The meaning of each part, the outcomes and
the metrics are on [Fleet registry](fleet.md#resilience).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `timeout` | duration | `5s` | > 0 | Time limit for one attempt of an operation. A retry gets its own limit. |
| `max_retries` | `int` | `2` | >= 0 | Attempts after the first one, made only after a transient error (a timeout, a connection or other OS error). Any other error ends the operation at once. `0` means one attempt. |
| `backoff` | duration | `200ms` | >= 0 | Wait before the first retry. The wait doubles for each further retry. |
| `max_backoff` | duration | `5s` | >= 0, and >= `backoff` | Longest wait between two attempts. |
| `breaker_failures` | `int` | `5` | >= 1 | Failed operations in a row that open the breaker. While it is open, operations are skipped without a call. |
| `breaker_cooldown` | duration | `30s` | >= 0 | Time the breaker stays open before one operation may probe the provider. A probe that succeeds closes the breaker; one that fails reopens it with a fresh cooldown. |

```yaml
autodiscover:
  resilience:
    timeout: 5s
    max_retries: 2
    backoff: 200ms
    max_backoff: 5s
    breaker_failures: 5
    breaker_cooldown: 30s
```

---

## Connections (`connections:`) {#connections}

Named connections to shared services. A provider that keeps its data in
Consul, Redis or Postgres takes the address, the credentials and the key
prefix or schema from here, so they are written once and shared by every
feature that names the connection. `connections` has one map per service
type, and each map goes from a connection name that you choose to its
settings. A feature that names no connection uses `main`. How features name
and share a connection is on [Fleet registry](fleet.md#connections). This
release ships the `postgres`, `redis` and `consul` registry and election
providers, which read `connections.postgres.<name>`, `connections.redis.<name>`
and `connections.consul.<name>`.

Which feature names which entry: `autodiscover.connection` and
`autodiscover.connection`.

### Consul (`connections.consul.<name>`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `address` | `str` | `http://127.0.0.1:8500` | checked when a provider starts: `http://` or `https://`, printable ASCII, no user name, query or fragment (see below) | Base URL of the Consul HTTP API. |
| `token` | `str` | `''` | empty, or printable ASCII without a space (checked when a provider starts) | ACL token, sent with every request in the `X-Consul-Token` header. Empty means none. **Secret.** |
| `datacenter` | `str` | `''` | letters, digits, `_`, `.`, `-` | Datacenter to address, sent as `dc` with every request. Empty means the datacenter of the agent. |
| `key_prefix` | `str` | `drakkar` | segments of letters, digits, `_`, `.`, `-` joined by `/`; no leading or trailing `/`; no segment `.` or `..` | Key prefix of everything a feature stores in Consul. |

The `consul` providers talk to the HTTP API of a Consul agent. Each card or
lease is a key that a session holds. The session's time to live is half of
`autodiscover.offline_after` (cards) or of the task's
`lease_seconds` (leases), at least 10 seconds, and Consul
ends a session at most twice its time to live after the last renew.

The `address` and the `token` are checked when a provider starts. The address
is printable ASCII, with the scheme `http` or `https` (in any case), a host
(a name of letters, digits, `.`, `_` and `-`, or an IPv6 address in brackets,
as in the [Redis URL](fleet.md#redis)), an optional port, and an optional path
of segments of letters, digits, `.`, `_`, `~` and `-`, none of them `.` or `..`,
with no user name, query or fragment. Unlike the Redis URL, the address refuses
an empty host and a port with no digits (a `:` at the end of the host). An address that fails the check fails the
start with `consul connection: the address cannot be parsed`, and a `token`
that is neither empty nor printable ASCII without a space fails it with
`consul connection: the token cannot be sent in a header`. Neither message
quotes the value.

The config load checks the rest, with the path and the rule in the message:

- `connections.consul.<name>.key_prefix '<value>' must be segments of letters, digits, '_', '.' and '-' joined by '/', with no leading or trailing '/', and no segment may be '.' or '..'`
- `connections.consul.<name>.datacenter '<value>' may only contain letters, digits, '_', '.' and '-'`
- `autodiscover.offline_after (<n>) must be <= 86400 when autodiscover.provider is consul`

The client does no retrying of its own: the [`resilience`
block](#resilience-block) of the feature is the only retry and the only time
limit, apart from a connect, which gives up after 5 seconds. It follows no
redirect. With `https://`, the client verifies the certificate of the agent and
checks that it names the host of the address. See
[Security](security.md#provider-credentials) for the trust store, the proxy
variables and the rights of the token, and [Fleet registry:
consul](fleet.md#consul) for the rest.

### Redis (`connections.redis.<name>`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `url` | `str` | `redis://localhost:6379/0` | checked when a provider starts: no query options (`?...`), printable ASCII only, other characters of the user name and the password percent-encoded (see [Fleet registry](fleet.md#redis)) | Redis connection URL, typically with the user and the password in it; `rediss://` is TLS, and the path is the database number. **Secret.** A URL with a `?` fails the start of a provider with `redis connection: the url cannot carry query options`, and a URL that does not parse with `redis connection: the url cannot be parsed`. |
| `key_prefix` | `str` | `drakkar:` | letters, digits, `_`, `:`, `.`, `-`; may be empty | Prefix of every key a feature stores in Redis. |

The `redis` providers need Redis 5.0 or later, standalone or a primary behind a
fixed address; Redis Cluster is not supported. With `rediss://`, the client
verifies the certificate of the server against the system's default
certificate store and checks that the certificate names the host of the URL;
this cannot be switched off. Put a private certificate authority into that
store; `SSL_CERT_FILE` and `SSL_CERT_DIR` replace the default locations of the
store. The client does no retrying of
its own: the [`resilience` block](#resilience-block) of the feature is the only
retry and the only time limit, apart from a connect, which gives up after 5
seconds. The Python package `redis` must be version 7.2 or later (the minimum
was 5.2.0); the [Redis sink](#redis-sink-sinksredisname) uses the same
package. See [Fleet registry: redis](fleet.md#redis) and
[Security](security.md#provider-credentials).

### Postgres (`connections.postgres.<name>`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `dsn` | `str` | required | | asyncpg-compatible connection string, typically with user and password. **Secret.** |
| `schema` | `str` | `drakkar` | lower-case identifier, `[a-z_][a-z0-9_]*` | Schema that holds the tables a feature creates. |
| `manage_schema` | `bool` | `true` | | Create the schema and the tables at the first successful start of a provider that uses the connection. Off, they must already exist: create them with `drakkar-schema postgres --schema=<name>`, which prints the DDL; a provider whose table is missing then fails its start with a message that names the command. For the recorder events table of [`persistence: postgres`](recorder-postgres.md#schema-management-and-ownership) add `--recorder` and, as needed, `--table-prefix=<p>` and `--retention-hours=<n>` (1 to 168, default 6). Partitions are always created by the recorder, so the role must own that table. |
| `pool_min` | `int` | `1` | >= 1 | Minimum connections in the pool. |
| `pool_max` | `int` | `4` | >= 1, and >= `pool_min` | Maximum connections in the pool. The registry and the election share one pool when they name the same connection. |

```yaml
connections:
  consul:
    main:
      address: http://consul.internal:8500
      key_prefix: drakkar
  redis:
    main:
      key_prefix: "drakkar:"
      # url: set from the environment, see below
  postgres:
    main:
      schema: drakkar
      # dsn: set from the environment, see below
```

Put the **secret** fields in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:...@db.internal/drakkar'
export DK_CONNECTIONS__CONSUL__MAIN__TOKEN='...'
export DK_CONNECTIONS__REDIS__MAIN__URL='redis://:...@redis.internal:6379/0'
```

Write connection names in lower case: the environment path is lower-cased
when it is read, so a name with capital letters cannot be set from a `DK_`
variable. A wrong value fails the load with the path and the rule, for
example `connections.postgres.main.schema 'Bad-Name' must be a lower-case
identifier ([a-z_][a-z0-9_]*)`. The message quotes that value, but it never
prints the input of the whole section, so a connection string stays out of
it. See [Security](security.md#provider-credentials).

---

## Settings `on_startup` cannot change

`DrakkarApp.__init__` builds a few things straight from the config it is
given, before the `on_startup` hook ever runs. A hook that changes one of
those settings changes nothing — the object built from the old value
already exists. Rather than dropping the change silently, the worker logs
one `on_startup_config_change_ignored` warning naming every setting it
ignored and what consumed it.

`sources.kafka.enabled` and `sources.http.enabled` are on that list:
handler validation and source construction both happen while the app object
is built, so a hook that flips a source on or off changes nothing. Set them
in YAML, or through `DK_SOURCES__KAFKA__ENABLED` /
`DK_SOURCES__HTTP__ENABLED`.

`storage.dir` is on the list too: the worker creates its own-files folder
before the hook runs, so a hook that moves it changes nothing. Set it in
YAML or through `DK_STORAGE__DIR`.

The full list and the working alternative are on the
[Handler](handler.md#on_startup) page.

---

## Annotated `drakkar.yaml` example

A copy-paste-ready YAML showing **every** field with one-line comments and the matching `DK_*` env-var override sits on its own page: **[Config Reference](config-reference.md)**. Use this page (Configuration) for the deep tables and prose; use the Reference for a quick scan of "what can I change here?"

