# Config Reference

A copy-paste-ready annotated `drakkar.yaml` showing **every** configurable field. Each line carries a one-sentence explanation and the matching `DK_*` environment-variable override. The 📚 link beside each section header takes you to the deep tables and prose in [Configuration](configuration.md).

!!! tip "How to read this page"
    - Pick only the sections you need -- every field has a sane default, so an empty `drakkar.yaml` is a valid config.
    - Comments mark **reasonable values** in `[brackets]` so you know what's typical vs. extreme.
    - Every field can be overridden by an env var. The pattern is `DK_<SECTION>__<FIELD>` (double underscore for nesting). See [Configuration Loading](configuration.md#configuration-loading) for the full precedence order.
    - `DK_CONFIG=/path/to/drakkar.yaml` selects which YAML file to load (it's not a config field itself).

---

## Worker identity

📚 [Deep details](configuration.md#root-config-drakkarconfig)

Top-level fields that identify this worker in logs, metrics, and the operator UI.

```yaml
# Name of the env var that holds this worker's display name.
# Empty / unset env var → falls back to "drakkar-<hex>".
worker_name_env: WORKER_ID         # env: DK_WORKER_NAME_ENV  · reasonable: WORKER_ID, HOSTNAME, K8S_POD_NAME

# Logical cluster name. Workers sharing this name are grouped in the operator UI
# and can cross-trace messages.
cluster_name: ''                   # env: DK_CLUSTER_NAME  · reasonable: search-cluster, prod-east, ''

# Env var that holds the cluster name. If set and non-empty, overrides `cluster_name`.
# Useful when the cluster name comes from the deployment platform (k8s, ECS).
cluster_name_env: ''               # env: DK_CLUSTER_NAME_ENV  · reasonable: K8S_NAMESPACE, DEPLOY_ENV, ''
```

---

## Kafka source (`kafka:`)

📚 [Deep details](configuration.md#kafka-source-kafka) · [Staggered startup](configuration.md#staggered-startup-alignment)

The Kafka consumer that reads input messages. `brokers` doubles as the fallback for sink and DLQ brokers when those are left empty.

```yaml
kafka:
  brokers: localhost:9092          # bootstrap servers, comma-separated. env: DK_KAFKA__BROKERS  · reasonable: kafka-1:9092,kafka-2:9092
  source_topic: input-events       # topic to consume from. env: DK_KAFKA__SOURCE_TOPIC
  consumer_group: drakkar-workers  # consumer-group ID; workers sharing it split partitions. env: DK_KAFKA__CONSUMER_GROUP

  max_poll_records: 100            # messages per poll batch; ↑ throughput / ↓ latency. env: DK_KAFKA__MAX_POLL_RECORDS  · reasonable: 50–500
  max_poll_interval_ms: 300000     # max ms between polls before broker kicks us out. env: DK_KAFKA__MAX_POLL_INTERVAL_MS  · raise for slow tasks
  session_timeout_ms: 45000        # group-membership heartbeat window. env: DK_KAFKA__SESSION_TIMEOUT_MS
  heartbeat_interval_ms: 3000      # heartbeat frequency; should be ≤ session_timeout_ms / 3. env: DK_KAFKA__HEARTBEAT_INTERVAL_MS

  # Policy for messages whose value fails input_model parsing. env: DK_KAFKA__ON_PARSE_ERROR
  #   skip  — message reaches arrange() with payload=None and msg.parse_error set (default)
  #   dlq   — message is excluded from arrange() and written to the DLQ topic as a
  #           ParseFailurePayload; the offset commits only after the DLQ write is confirmed
  #   raise — fail fast: MessageParseError stops the partition processor (schema-broken deploys)
  on_parse_error: skip

  # Transport security. Default PLAINTEXT emits no client properties at all, so a worker
  # that configures nothing here connects exactly as it always did. Kafka sinks and the DLQ
  # inherit this block whenever their own `brokers` field is empty.
  security:
    protocol: PLAINTEXT            # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL. env: DK_KAFKA__SECURITY__PROTOCOL
    sasl_mechanism: null           # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI | OAUTHBEARER.
                                   #   Required for SASL_* protocols, rejected for the others.
                                   #   env: DK_KAFKA__SECURITY__SASL_MECHANISM
    sasl_username: ''              # env: DK_KAFKA__SECURITY__SASL_USERNAME
    sasl_password: ''              # prefer the env override over a YAML literal.
                                   #   env: DK_KAFKA__SECURITY__SASL_PASSWORD
    ssl_ca_location: ''            # PEM CA bundle; empty uses the system trust store.
                                   #   env: DK_KAFKA__SECURITY__SSL_CA_LOCATION
    ssl_certificate_location: ''   # client cert (mutual TLS). env: DK_KAFKA__SECURITY__SSL_CERTIFICATE_LOCATION
    ssl_key_location: ''           # client key (mutual TLS); requires ssl_certificate_location.
                                   #   env: DK_KAFKA__SECURITY__SSL_KEY_LOCATION
    ssl_key_password: ''           # passphrase for an encrypted client key.
                                   #   env: DK_KAFKA__SECURITY__SSL_KEY_PASSWORD
    ssl_endpoint_identification_algorithm: null  # unset = librdkafka default ("https", verification on).
                                   #   Set to "none" ONLY for internal CAs without matching SANs.

  # Escape hatch: raw librdkafka properties, merged AFTER `security` so they win.
  # For options the typed block does not model. These four keys are rejected at startup —
  # each one backs a delivery invariant: enable.auto.commit, partition.assignment.strategy,
  # group.id, bootstrap.servers.
  client_config: {}                # env: DK_KAFKA__CLIENT_CONFIG (JSON object)

  # Kafka-UI deep-link integration: when both are set, the operator UI shows a clickable
  # icon next to every <partition:offset>. Both empty = feature disabled silently.
  ui_url: ''                       # env: DK_KAFKA__UI_URL  · example: http://kafka-ui:8080
  ui_cluster_name: ''              # cluster name registered in Kafka-UI. env: DK_KAFKA__UI_CLUSTER_NAME

  # Rolling-deploy alignment: serialize fleet-wide consumer-group rebalances.
  startup_align_enabled: true      # disable for snappy single-process dev. env: DK_KAFKA__STARTUP_ALIGN_ENABLED
  startup_min_wait_seconds: 4.0    # min sleep before alignment (buffer for slow init). env: DK_KAFKA__STARTUP_MIN_WAIT_SECONDS
  startup_align_interval_seconds: 10  # wake at next time%interval==0 boundary. env: DK_KAFKA__STARTUP_ALIGN_INTERVAL_SECONDS
```

---

## Executor pool (`executor:`)

📚 [Deep details](configuration.md#executor-pool-executor) · [Backpressure formula](configuration.md#backpressure-formula)

Subprocess pool that runs user binaries. Tune `max_executors` and `task_timeout_seconds` first; everything else has good defaults.

```yaml
executor:
  binary_path: null                # default binary for tasks. env: DK_EXECUTOR__BINARY_PATH  · null = ExecutorTask must set its own
  max_executors: 4                 # concurrent subprocesses. env: DK_EXECUTOR__MAX_EXECUTORS  · reasonable: 2–32
  task_timeout_seconds: 120        # wall-clock cap per task. env: DK_EXECUTOR__TASK_TIMEOUT_SECONDS  · reasonable: 30–600
  max_stdout_bytes: 0              # stdout bytes retained per task (0 = unlimited). env: DK_EXECUTOR__MAX_STDOUT_BYTES
  max_stderr_bytes: 0              # stderr bytes retained per task (0 = unlimited). env: DK_EXECUTOR__MAX_STDERR_BYTES
  window_size: 100                 # max messages collected per arrange() window. env: DK_EXECUTOR__WINDOW_SIZE
  max_retries: 3                   # retries per failed task (0 = no retries). env: DK_EXECUTOR__MAX_RETRIES
  drain_timeout_seconds: 30        # graceful-shutdown wait for in-flight tasks. env: DK_EXECUTOR__DRAIN_TIMEOUT_SECONDS

  # Backpressure: pause Kafka consumption when in-flight messages reach
  # max_executors * high_multiplier; resume when down to max_executors * low_multiplier.
  backpressure_high_multiplier: 32 # env: DK_EXECUTOR__BACKPRESSURE_HIGH_MULTIPLIER  · default 32 = pause at 4*32=128 queued
  backpressure_low_multiplier: 4   # env: DK_EXECUTOR__BACKPRESSURE_LOW_MULTIPLIER  · default 4 = resume at max(1,4*4)=16 queued

  # Subprocess environment.
  env: {}                          # env vars passed to all subprocesses. env: DK_EXECUTOR__ENV (JSON)
  env_inherit_parent: true         # pass parent process env (filtered). env: DK_EXECUTOR__ENV_INHERIT_PARENT
  env_inherit_deny:                # fnmatch patterns NOT inherited (case-insensitive). env: DK_EXECUTOR__ENV_INHERIT_DENY (JSON list)
    - 'DK_*'                       # framework internals
    - '*PASSWORD*'
    - '*PASSWD*'
    - '*SECRET*'
    - '*TOKEN*'
    - '*_KEY'
    - '*_DSN'
    - '*CREDENTIAL*'
    - '*SALT*'
```

---

## Sinks (`sinks:`)

📚 [Deep details](configuration.md#sinks-sinks) · [Sinks page](sinks.md)

Where processed results are delivered. Each sink type maps **instance names → config**, so you can have multiple instances of the same type writing to different destinations. All instance keys (e.g. `results`, `analytics`) are user-chosen.

### Kafka sink

📚 [Deep details](configuration.md#kafka-sink-sinkskafkaname)

```yaml
sinks:
  kafka:
    results:                       # instance name (free-form, referenced from handler code)
      topic: search-results        # required. env: DK_SINKS__KAFKA__RESULTS__TOPIC
      brokers: ''                  # empty = inherit kafka.brokers AND kafka.security.
                                   #   env: DK_SINKS__KAFKA__RESULTS__BROKERS
      ui_url: ''                   # link to Kafka-UI/Kowl in the UI dashboard. env: DK_SINKS__KAFKA__RESULTS__UI_URL
      security: {}                 # only consulted when `brokers` is set above; same fields as
                                   #   kafka.security. Leaving brokers empty is the usual choice.
      client_config: {}            # raw librdkafka overrides for this sink
```

### PostgreSQL sink

📚 [Deep details](configuration.md#postgresql-sink-sinkspostgresname)

```yaml
sinks:
  postgres:
    main-db:
      dsn: postgresql://user:pass@db:5432/myapp  # required. env: DK_SINKS__POSTGRES__MAIN_DB__DSN
      pool_min: 2                  # min connections in asyncpg pool. env: DK_SINKS__POSTGRES__MAIN_DB__POOL_MIN  · reasonable: 1–10
      pool_max: 10                 # max connections. env: DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX  · reasonable: 5–50
      ui_url: ''                   # pgAdmin / Adminer URL. env: DK_SINKS__POSTGRES__MAIN_DB__UI_URL
      statements:                  # optional. Operator-authored SQL by name
        claim_job: |               # env: DK_SINKS__POSTGRES__MAIN_DB__STATEMENTS__CLAIM_JOB
          UPDATE jobs SET status = :status WHERE id = :id AND status = 'pending'
```

Statement names must match `^[a-z_][a-z0-9_]*$`; placeholders are written
`:name`. Malformed SQL fails at startup, schema errors at delivery. See
[Sink Write Operations](sink-write-operations.md).

### MongoDB sink

📚 [Deep details](configuration.md#mongodb-sink-sinksmongoname)

```yaml
sinks:
  mongo:
    logs:
      uri: mongodb://mongo:27017   # required. env: DK_SINKS__MONGO__LOGS__URI
      database: app_logs           # required. env: DK_SINKS__MONGO__LOGS__DATABASE
      ui_url: ''                   # Mongo Express URL. env: DK_SINKS__MONGO__LOGS__UI_URL
      statements:                  # optional. Operator-authored MQL by name
        record_attempt:            # env: DK_SINKS__MONGO__LOGS__STATEMENTS__RECORD_ATTEMPT__COLLECTION
          collection: jobs
          op: update_one           # one of: update_one, update_many, upsert, delete_one, delete_many
          filter: { _id: ":id" }   # required, and never empty
          update:                  # required for the update ops, absent for the deletes
            $set: { last_seen: ":now" }
            $inc: { attempts: 1 }
```

Statement names must match `^[a-z_][a-z0-9_]*$`. Values bind through `":name"`
placeholders, whole values only and type-preserved; `"::name"` escapes a literal
leading colon. `$where` and `$function` are rejected at startup at any depth,
including inside aggregation-pipeline stages. Statements are not verified
against the live database. See
[Sink Write Operations](sink-write-operations.md#mongo).

### HTTP sink

📚 [Deep details](configuration.md#http-sink-sinkshttpname)

Cloud metadata endpoints (`169.254.169.254`, `metadata.google.internal`, etc.) are rejected at config load to prevent IAM-credential leaks via misconfiguration.

```yaml
sinks:
  http:
    webhook:
      url: https://api.example.com/webhook  # required, http(s) scheme. env: DK_SINKS__HTTP__WEBHOOK__URL
      method: POST                 # env: DK_SINKS__HTTP__WEBHOOK__METHOD
      timeout_seconds: 30          # request timeout. env: DK_SINKS__HTTP__WEBHOOK__TIMEOUT_SECONDS
      max_retries: 3               # retries per failed request. env: DK_SINKS__HTTP__WEBHOOK__MAX_RETRIES
      headers: {}                  # extra request headers. env: DK_SINKS__HTTP__WEBHOOK__HEADERS (JSON)
      encoding: json               # body format: json | form | multipart. env: DK_SINKS__HTTP__WEBHOOK__ENCODING
      ui_url: ''                   # env: DK_SINKS__HTTP__WEBHOOK__UI_URL
```

### Redis sink

📚 [Deep details](configuration.md#redis-sink-sinksredisname)

```yaml
sinks:
  redis:
    cache:
      url: redis://localhost:6379/0  # env: DK_SINKS__REDIS__CACHE__URL
      key_prefix: ''               # prepended to every key, e.g. "result:". env: DK_SINKS__REDIS__CACHE__KEY_PREFIX
      ui_url: ''                   # RedisInsight URL. env: DK_SINKS__REDIS__CACHE__UI_URL
      scripts:                     # optional. Operator-authored Lua by name
        push_and_cap: |            # env: DK_SINKS__REDIS__CACHE__SCRIPTS__PUSH_AND_CAP
          redis.call('LPUSH', KEYS[1], ARGV[1])
          redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[2]) - 1)
```

Script names must match `^[a-z_][a-z0-9_]*$`; values reach the script as `KEYS`
and `ARGV`, never interpolated. The Lua is not parsed at startup and not checked
against a live server, so a broken script fails at delivery. See
[Sink Write Operations](sink-write-operations.md).

### Filesystem sink

📚 [Deep details](configuration.md#filesystem-sink-sinksfilesystemname)

```yaml
sinks:
  filesystem:
    archive:
      base_path: /data/archive     # required, payload paths are resolved under this. env: DK_SINKS__FILESYSTEM__ARCHIVE__BASE_PATH
      ui_url: ''                   # env: DK_SINKS__FILESYSTEM__ARCHIVE__UI_URL
```

### Circuit breaker (shared default)

📚 [Deep details](configuration.md#circuit-breaker-sinkscircuit_breaker) · [State machine](sinks.md#circuit-breaker)

Applies uniformly to every sink instance. Per-sink overrides are not supported in v1 -- if one sink needs different thresholds, adjust the global default.

```yaml
sinks:
  circuit_breaker:
    failure_threshold: 5           # consecutive terminal failures to trip. env: DK_SINKS__CIRCUIT_BREAKER__FAILURE_THRESHOLD  · reasonable: 3–10
    cooldown_seconds: 30.0         # seconds open before half-open probe. env: DK_SINKS__CIRCUIT_BREAKER__COOLDOWN_SECONDS  · reasonable: 10–120
```

---

## Dead letter queue (`dlq:`)

📚 [Deep details](configuration.md#dead-letter-queue-dlq) · [DLQ behavior](sinks.md#dead-letter-queue)

Failed sink deliveries (after retries are exhausted, or when a circuit breaker is open) are written to this Kafka topic.

```yaml
dlq:
  topic: ''                        # empty = auto-derive "{source_topic}_dlq". env: DK_DLQ__TOPIC
  brokers: ''                      # empty = inherit kafka.brokers. env: DK_DLQ__BROKERS

  # Strategy when the DLQ write itself fails (payloads have nowhere safe to go):
  #   drop  — log CRITICAL + tick drakkar_dlq_dropped_payloads_total, commit the offset,
  #           keep the pipeline moving; the payloads are lost (default)
  #   stall — leave the offset uncommitted and pause the partition; messages are
  #           redelivered after restart/rebalance (no loss, at the cost of lag)
  on_send_failure: drop            # env: DK_DLQ__ON_SEND_FAILURE
```

---

## Metrics (`metrics:`)

📚 [Deep details](configuration.md#metrics-metrics) · [Observability](observability.md)

Prometheus scrape endpoint.

```yaml
metrics:
  enabled: true                    # env: DK_METRICS__ENABLED
  port: 9090                       # 1–65535. env: DK_METRICS__PORT
```

---

## Logging (`logging:`)

📚 [Deep details](configuration.md#logging-logging)

Structured logging via [structlog](https://www.structlog.org/).

```yaml
logging:
  level: INFO                      # DEBUG | INFO | WARNING | ERROR | CRITICAL. env: DK_LOGGING__LEVEL
  format: json                     # "json" for prod, "console" for dev. env: DK_LOGGING__FORMAT
  output: stderr                   # stderr | stdout | file path. env: DK_LOGGING__OUTPUT
                                   # File paths support {worker_id}, {cluster_name} templating
                                   # e.g. /var/log/drakkar/{worker_id}.log
```

---

## UI / Flight Recorder (`ui:`)

📚 [Deep details](configuration.md#ui-flight-recorder-ui) · [Authentication](configuration.md#authentication) · [Operator UI](observability.md#operator-ui)

The largest section. Provides the operator web UI, a SQLite-backed event log (`ui.recorder.*`), drakkar-ui bundle fetching (`ui.release.*`), WebSocket live streaming, worker autodiscovery, and Prometheus deep-links.

```yaml
ui:
  # --- Server ---
  enabled: true                    # master switch for the whole UI feature. env: DK_UI__ENABLED
  host: 127.0.0.1                  # bind address. Use 0.0.0.0 for non-loopback. env: DK_UI__HOST
  port: 8080                       # 1–65535. env: DK_UI__PORT
  public_url: ''                   # external URL advertised to peers (LB / ingress). env: DK_UI__PUBLIC_URL

  # --- Auth (opt-in by default; most endpoints are read-only) ---
  auth_token: ''                   # bearer token; empty = unauthenticated + startup warning. env: DK_UI__AUTH_TOKEN
                                   # Generate: python -c "import secrets; print(secrets.token_urlsafe(32))"
  allowed_ws_origins: []           # WebSocket Origin allowlist. env: DK_UI__ALLOWED_WS_ORIGINS (JSON list)

  # --- Side-effecting endpoints (gated independently of auth_token) ---
  probe_enabled: true              # POST /api/debug/probe; runs caller bytes through the live handler
                                   # and takes executor slots. false = 403. env: DK_UI__PROBE_ENABLED
  merge_enabled: true              # POST /api/debug/merge; writes merged-<ts>.db into recorder.db_dir,
                                   # never reclaimed. false = 403. env: DK_UI__MERGE_ENABLED

  # --- Deployment metadata ---
  expose_env_vars: []              # env vars captured into worker_config table. env: DK_UI__EXPOSE_ENV_VARS (JSON list)
                                   # e.g. ['GIT_SHA', 'DEPLOY_ENV', 'K8S_POD_NAME']

  # --- Presentation ---
  max_rows: 5000                   # max rows returned by UI list endpoints. env: DK_UI__MAX_ROWS

  # --- Duration thresholds (noise filters) ---
  log_min_duration_ms: 500         # min ms to log slow_task_completed/failed. env: DK_UI__LOG_MIN_DURATION_MS
  ws_min_duration_ms: 500          # min ms to broadcast over WebSocket. env: DK_UI__WS_MIN_DURATION_MS

  # --- Prometheus deep-links in the UI ---
  prometheus_url: ''               # e.g. http://prometheus:9090. env: DK_UI__PROMETHEUS_URL
  prometheus_rate_interval: 5m     # rate() interval used in dashboard PromQL. env: DK_UI__PROMETHEUS_RATE_INTERVAL
  prometheus_worker_label: ''      # PromQL label for worker-scoped queries. env: DK_UI__PROMETHEUS_WORKER_LABEL
                                   # Supports {worker_id}, {cluster_name}, {metrics_port}, {debug_port}
                                   # e.g. 'worker_id="{worker_id}"'
  prometheus_cluster_label: ''     # PromQL label for cluster-wide queries. env: DK_UI__PROMETHEUS_CLUSTER_LABEL
                                   # e.g. 'cluster="{cluster_name}"'

  # --- Custom links shown in the dashboard nav ---
  custom_links: []                 # env: DK_UI__CUSTOM_LINKS (JSON list)
                                   # Each entry: {name: "...", url: "..."}; url supports {worker_id} etc.

  # --- Flight recorder (persistence; all flags require non-empty db_dir) ---
  recorder:
    db_dir: /tmp                     # SQLite directory. Empty = no disk persistence. env: DK_UI__RECORDER__DB_DIR
                                     # Use shared FS (NFS, EFS) for cross-worker autodiscovery
    store_events: true               # write per-message events. env: DK_UI__RECORDER__STORE_EVENTS
    store_config: true               # write worker config (enables autodiscovery). env: DK_UI__RECORDER__STORE_CONFIG
    store_state: true                # periodic worker-state snapshots. env: DK_UI__RECORDER__STORE_STATE
    state_sync_interval_seconds: 10  # snapshot frequency. env: DK_UI__RECORDER__STATE_SYNC_INTERVAL_SECONDS

    # --- Database rotation & retention ---
    rotation_interval_minutes: 60    # how often to rotate the SQLite file. env: DK_UI__RECORDER__ROTATION_INTERVAL_MINUTES
    retention_hours: 24              # delete rotated files older than this. env: DK_UI__RECORDER__RETENTION_HOURS
    retention_max_events: 100000     # cap on total events across all files. env: DK_UI__RECORDER__RETENTION_MAX_EVENTS

    # --- Output (stdout/stderr) capture ---
    store_output: true               # include subprocess output in events. env: DK_UI__RECORDER__STORE_OUTPUT
    flush_interval_seconds: 5        # in-memory buffer → SQLite cadence. env: DK_UI__RECORDER__FLUSH_INTERVAL_SECONDS
    max_buffer: 50000                # ring-buffer capacity. env: DK_UI__RECORDER__MAX_BUFFER
    max_flush_retries: 3             # retries on transient SQLite errors. env: DK_UI__RECORDER__MAX_FLUSH_RETRIES
    event_min_duration_ms: 0         # min ms to persist to SQLite (0 = persist all). env: DK_UI__RECORDER__EVENT_MIN_DURATION_MS
    output_min_duration_ms: 500      # min ms to include stdout/stderr in event. env: DK_UI__RECORDER__OUTPUT_MIN_DURATION_MS

  # --- drakkar-ui bundle fetching (never fatal; offline falls back to built-in pages) ---
  release:
    enabled: true                    # resolve + serve the drakkar-ui bundle. env: DK_UI__RELEASE__ENABLED
    repo: wlame/drakkar-ui           # "owner/name" GitHub repo publishing UI bundles. env: DK_UI__RELEASE__REPO
    pinned_version: ''               # known-good UI release tag (e.g. v1.2.0); '' = unpinned. env: DK_UI__RELEASE__PINNED_VERSION
    cache_dir: ''                    # bundle cache root; '' = $XDG_CACHE_HOME/drakkar/ui. env: DK_UI__RELEASE__CACHE_DIR
    check_update: true               # resolve the latest release tag on startup. env: DK_UI__RELEASE__CHECK_UPDATE
```

---

## Cache (`cache:`)

📚 [Deep details](configuration.md#cache-cache) · [Peer Sync](configuration.md#cache-peer-sync) · [Cache page](cache.md)

Optional handler-accessible key/value cache. **Disabled by default**.

```yaml
cache:
  enabled: false                   # master switch; false = no-op stub. env: DK_CACHE__ENABLED
  db_dir: ''                       # SQLite dir; empty = falls back to ui.recorder.db_dir. env: DK_CACHE__DB_DIR
  flush_interval_seconds: 3.0      # write-behind flush cadence. env: DK_CACHE__FLUSH_INTERVAL_SECONDS
  cleanup_interval_seconds: 60.0   # expired-row cleanup cadence. env: DK_CACHE__CLEANUP_INTERVAL_SECONDS
  max_memory_entries: 10000        # in-memory LRU cap; null = unbounded (warns). env: DK_CACHE__MAX_MEMORY_ENTRIES

  # --- Cross-worker peer sync (LWW merge by updated_at_ms) ---
  peer_sync:
    enabled: true                  # env: DK_CACHE__PEER_SYNC__ENABLED
    interval_seconds: 30.0         # peer-sync cycle cadence. env: DK_CACHE__PEER_SYNC__INTERVAL_SECONDS
    batch_size: 500                # max rows pulled per peer per cycle. env: DK_CACHE__PEER_SYNC__BATCH_SIZE
    timeout_seconds: 5.0           # per-peer read timeout. env: DK_CACHE__PEER_SYNC__TIMEOUT_SECONDS
    cycle_deadline_seconds: null   # hard cap on one cycle; null = interval*0.9. env: DK_CACHE__PEER_SYNC__CYCLE_DEADLINE_SECONDS
                                   # Must be strictly < interval_seconds (config load fails otherwise)
```

---

## Webapp (`webapp:`)

📚 [Deep details](configuration.md#webapp-webapp) · [Webapp page](webapp.md)

Optional synchronous-HTTP entry point. **Disabled by default**. When `enabled=true`, the handler must declare `HttpRequestT` / `HttpResponseT` as the third and fourth generic parameters of `BaseDrakkarHandler`.

```yaml
webapp:
  enabled: false                   # master switch; false = no FastAPI server. env: DK_WEBAPP__ENABLED
  host: '0.0.0.0'                  # uvicorn bind interface. env: DK_WEBAPP__HOST
  port: 8090                       # uvicorn bind port. env: DK_WEBAPP__PORT
  path: '/process'                 # single POST route; must start with '/'. env: DK_WEBAPP__PATH
  sinks_enabled: false             # when true, route on_message_complete payloads through SinkManager. env: DK_WEBAPP__SINKS_ENABLED
  request_timeout_seconds: 30.0    # per-request budget; > 0. env: DK_WEBAPP__REQUEST_TIMEOUT_SECONDS
  max_concurrent: 64               # per-worker in-flight cap; > 0. 65th concurrent request 503s. env: DK_WEBAPP__MAX_CONCURRENT
  max_body_bytes: 10485760         # request-body cap in bytes; > 0 (413 beyond); same key/default on both backends. env: DK_WEBAPP__MAX_BODY_BYTES

  # Configured tenants. At least one entry is required; default is one
  # anonymous client with rpm=4 so the webapp works out of the box.
  clients:
    - name: anonymous              # tenant name; non-empty string
      token: ''                    # bearer token; '' = anonymous slot (at most one client)
      rpm: 4                       # per-client requests/minute cap; > 0
    - name: tenant-a
      token: 'secret-tenant-a-token'
      rpm: 60
```

---

## Environment-variable override cheatsheet

The pattern: **`DK_<SECTION>__<FIELD>`** -- prefix `DK_`, double underscore between nesting levels, single underscore within a field name.

| Where | Path → env var |
|-------|---------------|
| Top-level | `cluster_name` → `DK_CLUSTER_NAME` |
| One level deep | `kafka.brokers` → `DK_KAFKA__BROKERS` |
| Two levels deep | `cache.peer_sync.interval_seconds` → `DK_CACHE__PEER_SYNC__INTERVAL_SECONDS` |
| Map key (sink instance) | `sinks.postgres.main-db.dsn` → `DK_SINKS__POSTGRES__MAIN_DB__DSN` |
| List value | `ui.expose_env_vars` → `DK_UI__EXPOSE_ENV_VARS='["GIT_SHA","DEPLOY_ENV"]'` (JSON) |
| Dict value | `executor.env` → `DK_EXECUTOR__ENV='{"FOO":"bar"}'` (JSON) |

Special cases:

- **`DK_CONFIG`** -- selects which YAML file to load; not a config field. Excluded from the env→config merge.
- **Hyphens in map keys** -- `main-db` becomes `MAIN_DB` (single underscore) in the env var. Drakkar lowercases the env path on parse, then matches it against the YAML keys.
- **Booleans** -- `true` / `false` (also `1` / `0`, `yes` / `no`).
- **Nulls** -- omit the env var, or use the literal `null` if the YAML value should be reset.

See [Configuration Loading](configuration.md#configuration-loading) for the precedence order (`defaults → YAML → env`) and the deep-merge semantics.
