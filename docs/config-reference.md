# Config Reference

A copy-paste-ready annotated `drakkar.yaml` showing **every** configurable field. Each line carries a one-sentence explanation and the matching `DK_*` environment-variable override. The 📚 link beside each section header takes you to the deep tables and prose in [Configuration](configuration.md).

!!! tip "How to read this page"
    - Pick only the sections you need -- every field has a sane default, so an empty `drakkar.yaml` is a valid config.
    - Comments mark **reasonable values** in `[brackets]` so you know what's typical vs. extreme.
    - Every field can be overridden by an env var. The pattern is `DK_<SECTION>__<FIELD>` (double underscore for nesting). See [Configuration Loading](configuration.md#configuration-loading) for the full precedence order.
    - `DK_CONFIG=/path/to/drakkar.yaml` selects which YAML file to load (it's not a config field itself).

---

## Worker (`worker:`)

📚 [Deep details](configuration.md#root-config-drakkarconfig)

The name that identifies this worker in logs, metrics, the registry card and the operator UI.

```yaml
worker:
  name: ''                         # literal worker name: 1 to 128 of A-Z a-z 0-9 _ -; any other name fails
                                   # the start. '' and no value from name_env = the host name made safe
                                   # (drakkar-<8 hex> for an empty or a localhost host name). env: DK_WORKER__NAME
  name_env: WORKER_ID              # env var that holds the name; set and not empty, it wins over name. Same rule.
                                   # env: DK_WORKER__NAME_ENV  · reasonable: WORKER_ID, HOSTNAME, K8S_POD_NAME
```

The name is part of the worker's own folder `<storage.dir>/<cluster>/<worker>/`. In Kubernetes
the host name is the pod name, which a container restart keeps; a Deployment gives a recreated pod
a new name. See [Worker names](local-databases.md#worker-names).

---

## Cluster (`cluster:`)

📚 [Deep details](configuration.md#root-config-drakkarconfig)

The cluster this worker belongs to. Workers with one cluster name are grouped in the operator UI,
share the lock scope of cluster tasks and the history folder, and can cross-trace messages.

```yaml
cluster:
  name: ''                         # literal cluster name. '' with autodiscover on logs one startup warning
                                   # naming the scope default. A folder name fleet, history, merged, tasks or
                                   # workers fails the load. env: DK_CLUSTER__NAME  · reasonable: search-cluster, prod-east
  name_env: ''                     # env var that holds the cluster name; set and not empty, it wins over name.
                                   # env: DK_CLUSTER__NAME_ENV  · reasonable: K8S_NAMESPACE, DEPLOY_ENV
```

---

## Input sources (`sources:`)

📚 [Deep details](configuration.md#input-sources-sources) · [Input Sources](sources.md) · [Staggered startup](configuration.md#staggered-startup-alignment) · [Webapp page](webapp.md)

The Kafka consumer and the synchronous HTTP ingress, as peers. Each is optional and enabled explicitly, but at least one of `sources.kafka.enabled` / `sources.http.enabled` must be true.

```yaml
sources:
  kafka:
    enabled: false                   # consume the Kafka topic; off = no consumer group joined, arrange() never called. env: DK_SOURCES__KAFKA__ENABLED
    connection: main                 # the cluster to read: a name under connections.kafka; must be declared. env: DK_SOURCES__KAFKA__CONNECTION
    topic: input-events              # topic to consume from. env: DK_SOURCES__KAFKA__TOPIC
    consumer_group: drakkar-workers  # consumer-group ID; workers sharing it split partitions. env: DK_SOURCES__KAFKA__CONSUMER_GROUP

    max_poll_records: 100            # messages per poll batch; ↑ throughput / ↓ latency. env: DK_SOURCES__KAFKA__MAX_POLL_RECORDS  · reasonable: 50–500
    max_queued_messages: 1000        # pause fetching at this many queued messages, resume at half. env: DK_SOURCES__KAFKA__MAX_QUEUED_MESSAGES  · >= max_poll_records
    max_poll_interval: 5m            # max time between polls before broker kicks us out. env: DK_SOURCES__KAFKA__MAX_POLL_INTERVAL  · raise for slow tasks
    session_timeout: 45s             # group-membership heartbeat window. env: DK_SOURCES__KAFKA__SESSION_TIMEOUT
    heartbeat_interval: 3s           # heartbeat frequency; should be ≤ session_timeout / 3. env: DK_SOURCES__KAFKA__HEARTBEAT_INTERVAL

    # Policy for messages whose value fails input_model parsing. env: DK_SOURCES__KAFKA__ON_PARSE_ERROR
    #   skip  — message reaches arrange() with payload=None and msg.parse_error set (default)
    #   dlq   — message is excluded from arrange() and written to the DLQ topic as a DLQ
    #           record (the original message, dk-stage: parse); the offset commits only
    #           after the DLQ write is confirmed
    #   raise — fail fast: MessageParseError stops the partition processor (schema-broken deploys)
    on_parse_error: skip

    # Rolling-deploy alignment: serialize fleet-wide consumer-group rebalances.
    startup_align:
      enabled: true                  # disable for snappy single-process dev. env: DK_SOURCES__KAFKA__STARTUP_ALIGN__ENABLED
      min_wait: 4s                   # min sleep before alignment (buffer for slow init). env: DK_SOURCES__KAFKA__STARTUP_ALIGN__MIN_WAIT
      interval: 10s                  # whole seconds; wake at next time%interval==0 boundary. env: DK_SOURCES__KAFKA__STARTUP_ALIGN__INTERVAL

  http:
    enabled: false                   # master switch; false = no FastAPI server. env: DK_SOURCES__HTTP__ENABLED
    host: '0.0.0.0'                  # uvicorn bind interface. env: DK_SOURCES__HTTP__HOST
    port: 8090                       # uvicorn bind port. env: DK_SOURCES__HTTP__PORT
    path: '/process'                 # single POST route; must start with '/'. env: DK_SOURCES__HTTP__PATH
    deliver_to_sinks: false          # when true, route on_message_complete payloads through SinkManager. env: DK_SOURCES__HTTP__DELIVER_TO_SINKS
    request_timeout: 30s             # per-request budget; > 0. env: DK_SOURCES__HTTP__REQUEST_TIMEOUT
    max_concurrent: 64               # per-worker in-flight cap; > 0. 65th concurrent request 503s. env: DK_SOURCES__HTTP__MAX_CONCURRENT
    max_body: 10MiB                  # request-body cap; > 0 (413 beyond). env: DK_SOURCES__HTTP__MAX_BODY

    # Configured tenants. At least one entry is required; default is one
    # anonymous client with rpm=4 so the HTTP source works out of the box.
    clients:
      - name: anonymous              # tenant name; non-empty string
        token: ''                    # bearer token; '' = anonymous slot (at most one client)
        rpm: 4                       # per-client requests/minute cap; > 0
      - name: tenant-a
        token: 'secret-tenant-a-token'
        rpm: 60
```

When `sources.http.enabled=true`, the handler must declare `HttpRequestT` / `HttpResponseT` as the third and fourth generic parameters of `BaseDrakkarHandler`.

---

## Executor pool (`executor:`)

📚 [Deep details](configuration.md#executor-pool-executor) · [Backpressure](performance.md#backpressure)

Subprocess pool that runs user binaries. Tune `max_executors` and `task_timeout` first; everything else has good defaults.

```yaml
executor:
  binary_path: null                # default binary for tasks. env: DK_EXECUTOR__BINARY_PATH  · null = ExecutorTask must set its own
  max_executors: 4                 # concurrent subprocesses. env: DK_EXECUTOR__MAX_EXECUTORS  · reasonable: 2–32
  task_timeout: 2m                 # wall-clock cap per task, whole seconds. env: DK_EXECUTOR__TASK_TIMEOUT  · reasonable: 30s–10m
  max_stdout: 0                    # stdout kept per task, a size (0 = no limit). env: DK_EXECUTOR__MAX_STDOUT
  max_stderr: 0                    # stderr kept per task, a size (0 = no limit). env: DK_EXECUTOR__MAX_STDERR
  window_size: 100                 # max messages collected per arrange() window. env: DK_EXECUTOR__WINDOW_SIZE
  max_retries: 3                   # retries per failed task (0 = no retries). env: DK_EXECUTOR__MAX_RETRIES
  on_failure: skip                 # what the default on_error returns: skip | dlq (dlq = the source
                                   #   messages of a failed task go to the DLQ). env: DK_EXECUTOR__ON_FAILURE
  drain_timeout: 30s               # graceful-shutdown wait for in-flight tasks, whole seconds. env: DK_EXECUTOR__DRAIN_TIMEOUT

  # Window admission: a partition loop takes a new arrange() window only while
  # both pending counts are below their limits, and reserves its estimate at once.
  # Soft limits: passed by the admitted window's fan-out beyond its estimate
  # (normally one message; a jump in tasks per message passes them by that jump);
  # drakkar_pending_tasks / drakkar_pending_bytes show the real counts.
  max_pending_tasks: 2000          # unsettled tasks (waiting, running, in on_task_complete). env: DK_EXECUTOR__MAX_PENDING_TASKS  · >= max_executors
  max_pending_bytes: 1GiB          # task stdin + output kept for on_message_complete / on_window_complete.
                                   #   env: DK_EXECUTOR__MAX_PENDING_BYTES  · >= 16MiB; about a quarter of container memory

  # Subprocess environment.
  env: {}                          # env vars passed to all subprocesses. env: DK_EXECUTOR__ENV (JSON)
  env_inherit_parent: true         # pass parent process env (filtered). env: DK_EXECUTOR__ENV_INHERIT_PARENT
  env_inherit_deny:                # fnmatch patterns NOT inherited (case-insensitive). env: DK_EXECUTOR__ENV_INHERIT_DENY (JSON list)
    - 'DK_*'                       # the worker's own config overrides
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

A Kafka, Postgres, Mongo or Redis sink names a connection under [`connections`](#connections-connections). The connection says how to reach the service and log in. The sink says what to do with it, and owns its client or pool, so several sinks may share one connection.

### Keys of every sink instance

📚 [Deep details](configuration.md#keys-of-every-sink-instance) · [Failure policy](sinks.md#failure-policy) · [Circuit breaker](sinks.md#circuit-breaker)

`sinks.defaults` holds the default of five policy keys. An instance may give any of them; a key it does not give reads `sinks.defaults`, and a key it gives wins even when it equals the default. Each sink's policy is resolved once, when the worker is constructed.

```yaml
sinks:
  defaults:
    on_failure: dlq                # dlq | skip: when the retry budget is spent. env: DK_SINKS__DEFAULTS__ON_FAILURE
    max_retries: 2                 # 0-10: retries after the first attempt, spent only by a hook that returns RETRY (the default hook makes one attempt, then applies on_failure). env: DK_SINKS__DEFAULTS__MAX_RETRIES
    delivery_timeout: 30s          # budget for one deliver() and one close(). env: DK_SINKS__DEFAULTS__DELIVERY_TIMEOUT  · reasonable: 10s–2m
    breaker_failures: 5            # consecutive terminal failures to trip the circuit breaker. env: DK_SINKS__DEFAULTS__BREAKER_FAILURES  · reasonable: 3–10
    breaker_cooldown: 30s          # time open before the half-open probe. env: DK_SINKS__DEFAULTS__BREAKER_COOLDOWN  · reasonable: 10s–2m
  kafka:
    results:
      topic: search-results
      on_failure: skip             # the same five keys, on one instance. env: DK_SINKS__KAFKA__RESULTS__ON_FAILURE
      delivery_timeout: 10s        # env: DK_SINKS__KAFKA__RESULTS__DELIVERY_TIMEOUT
      depends_on: []               # sinks to deliver first, as '<type>/<name>' (set it in YAML)
```

### Kafka sink

📚 [Deep details](configuration.md#kafka-sink-sinkskafkaname)

```yaml
sinks:
  kafka:
    results:                       # instance name (free-form, referenced from handler code)
      connection: main             # a name under connections.kafka; must be declared. env: DK_SINKS__KAFKA__RESULTS__CONNECTION
      topic: search-results        # required. env: DK_SINKS__KAFKA__RESULTS__TOPIC
      ui_url: ''                   # link to Kafka-UI/Kowl in the UI dashboard. env: DK_SINKS__KAFKA__RESULTS__UI_URL
      client_config: {}            # raw librdkafka overrides for this sink, merged after the connection's;
                                   #   this sink wins on a shared key
      flush_timeout: 30s           # bound on the per-batch producer flush. env: DK_SINKS__KAFKA__RESULTS__FLUSH_TIMEOUT
                                   #   reasonable: 10s-1m; a last resort, not a latency target
```

### PostgreSQL sink

📚 [Deep details](configuration.md#postgresql-sink-sinkspostgresname)

```yaml
sinks:
  postgres:
    main-db:
      connection: main             # a name under connections.postgres; must be declared. env: DK_SINKS__POSTGRES__MAIN_DB__CONNECTION
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
      connection: main             # a name under connections.mongo; must be declared. env: DK_SINKS__MONGO__LOGS__CONNECTION
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
      request_timeout: 30s         # whole seconds. env: DK_SINKS__HTTP__WEBHOOK__REQUEST_TIMEOUT
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
      connection: main             # a name under connections.redis; must be declared. env: DK_SINKS__REDIS__CACHE__CONNECTION
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

---

## Dead letter queue (`dlq:`)

📚 [Deep details](configuration.md#dead-letter-queue-dlq) · [DLQ behavior](sinks.md#dead-letter-queue)

Messages that fail to parse, to execute or to deliver are written to this Kafka topic as [DLQ records](sinks.md#the-dlq-record): the original message (key, value and headers unchanged) plus `dk-` headers.

```yaml
dlq:
  enabled: true                    # off: no DLQ producer; a dlq outcome follows on_send_failure. env: DK_DLQ__ENABLED
  provider: kafka                  # the only value. env: DK_DLQ__PROVIDER
  connection: main                 # a name under connections.kafka; must be declared when the DLQ has a topic. env: DK_DLQ__CONNECTION
  topic: ''                        # empty = auto-derive "{sources.kafka.topic}_dlq". env: DK_DLQ__TOPIC
                                   #   empty AND sources.kafka.enabled=false = no DLQ producer is
                                   #   built; DLQ sends are dropped and counted
  client_config: {}                # raw librdkafka overrides for the DLQ producer, merged after the
                                   #   connection's; the DLQ wins on a shared key

  # Strategy when no DLQ record can be written (the message has nowhere safe to go):
  #   drop  — log CRITICAL + tick drakkar_dlq_dropped_payloads_total (source messages), commit
  #           the offset, keep the pipeline moving; the message is lost (default)
  #   stall — leave the offset uncommitted and pause the partition; messages are
  #           redelivered after restart/rebalance (no loss, at the cost of lag)
  flush_timeout: 30s               # bound on the per-write producer flush. env: DK_DLQ__FLUSH_TIMEOUT
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
  task_label_histograms: []        # task label keys observed into the
                                   # drakkar_task_label_value histogram, one
                                   # time series per key, e.g. [file_size_bytes]
                                   # env: DK_METRICS__TASK_LABEL_HISTOGRAMS
```

---

## Runtime health (`runtime_health:`)

📚 [Runtime health](runtime-health.md) · [Observability](observability.md)

Event-loop lag monitoring and stall introspection.

```yaml
runtime_health:
  enabled: true                    # env: DK_RUNTIME_HEALTH__ENABLED
  tick: 250ms                      # heartbeat interval. env: DK_RUNTIME_HEALTH__TICK
  warn_lag: 100ms                  # lag above this = degraded. env: DK_RUNTIME_HEALTH__WARN_LAG
  stall: 1s                        # heartbeat silence above this = stalled + stack capture.
                                   # env: DK_RUNTIME_HEALTH__STALL
  max_stall_stacks: 10             # distinct stacks kept per stall. env: DK_RUNTIME_HEALTH__MAX_STALL_STACKS
  sample_interval: 10s             # recorder history sample cadence. env: DK_RUNTIME_HEALTH__SAMPLE_INTERVAL
  history_window: 15m              # in-memory sparkline window, whole seconds, at least 1m. env: DK_RUNTIME_HEALTH__HISTORY_WINDOW
  max_episode: 5m                  # max length of one lag episode before it flushes, at least 10s.
                                   # env: DK_RUNTIME_HEALTH__MAX_EPISODE
  stack_probe_enabled: false       # opt-in stack probe: one runtime_probe event per stack_probe_interval.
                                   # env: DK_RUNTIME_HEALTH__STACK_PROBE_ENABLED
  stack_probe_interval: 1s         # stack probe cadence. env: DK_RUNTIME_HEALTH__STACK_PROBE_INTERVAL
```

---

## Thread pools (`threads:`)

📚 [Threads & Pools](threads.md) · [Offloading CPU-bound hook work](offload.md) · [Runtime health](runtime-health.md)

`io` is asyncio's default `to_thread` executor: every blocking call the handler (or the framework) sends
through `asyncio.to_thread` shares this one process-wide pool. `offload` is the pool behind
`handler.offload()` — CPU-bound hook computations moved off the event loop so the worker stays responsive.

```yaml
threads:
  io: null                         # null = keep Python's default, min(32, cpu_count + 4); otherwise 1 to 512.
                                   # Set explicitly when blocking-I/O fan-out is capped by
                                   # the pool (e.g. many-core hosts reading slow storage).
                                   # env: DK_THREADS__IO
  offload: null                    # concurrent offloaded computations before queueing.
                                   # null = auto: ceil(executor.max_executors / 4), min 2; otherwise 1 to 32.
                                   # env: DK_THREADS__OFFLOAD
```

`0` is refused: `threads.io must be null or from 1 to 512`, `threads.offload must be null or from 1 to 32`.

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
  min_task_duration: 500ms         # shortest task that logs slow_task_completed/failed; 0 logs all.
                                   # env: DK_LOGGING__MIN_TASK_DURATION
```

---

## API (`api:`)

The worker's one HTTP server. It always answers `/healthz`, `/readyz` and `/api/v1/peer/cache/changes`; these three routes are public. The `ui` section adds every other route to the same listener.

```yaml
api:
  enabled: true                    # the HTTP server. Off: no listener, nothing answers the Kubernetes probes,
                                   # and ui.enabled and autodiscover.enabled must both be false. env: DK_API__ENABLED
  host: 127.0.0.1                  # bind address. Use 0.0.0.0 for non-loopback. env: DK_API__HOST
  port: 8080                       # 1–65535. env: DK_API__PORT
  public_url: ''                   # external URL advertised to peers (LB / ingress); it goes on the registry card.
                                   # '' = http://<outbound ip>:<port>/. env: DK_API__PUBLIC_URL
```

Two load errors follow from it:

- `ui.enabled needs api.enabled; set ui.enabled to false, or set api.enabled to true`
- `autodiscover.enabled needs api.enabled: peers reach a worker through its API`

## UI (`ui:`)

📚 [Deep details](configuration.md#operator-ui-ui) · [Authentication](configuration.md#authentication) · [Operator UI](observability.md#operator-ui)

The largest section. Provides the operator web UI, drakkar-ui bundle fetching (`ui.release.*`), WebSocket live streaming and Prometheus deep-links, on the listener of the [`api` section](#api-api). With `ui.enabled: false` those routes answer 404 and the worker fetches no bundle. The event log is the [flight recorder](#flight-recorder-recorder).

```yaml
ui:
  enabled: true                    # the pages, the bundle fetch, the debug and data routes, /ws, the docs.
                                   # Needs api.enabled. The durable recorder does not depend on it; the memory store does.
                                   # env: DK_UI__ENABLED

  # --- Auth (opt-in by default; most endpoints are read-only) ---
  auth_token: ''                   # bearer token; empty = unauthenticated + startup warning. env: DK_UI__AUTH_TOKEN
                                   # Generate: python -c "import secrets; print(secrets.token_urlsafe(32))"
                                   # Never covers /healthz, /readyz and /api/v1/peer/ (the cache exchange).
  allowed_origins: []              # Origin allowlist for /ws and for the cross-origin reads of the UI fan-out
                                   # (debug/trace, debug/trace-by-label, debug/databases). With auth_token set,
                                   # list every worker origin. env: DK_UI__ALLOWED_ORIGINS (JSON list)

  # --- Side-effecting endpoints (gated independently of auth_token; ui.probe.enabled is one more switch) ---
  merge_enabled: true              # POST /api/v1/debug/merge; writes merged-<ts>-<token>.db into
                                   # <storage.shared_dir>/merged/, deleted after recorder.archive.merged_retention.
                                   # false = 403. env: DK_UI__MERGE_ENABLED
  delete_enabled: false            # DELETE /api/v1/debug/files/{path}; deletes one merge result (merged/<name>.db)
                                   # from the Databases view. Nothing else can be deleted. false = 403.
                                   # env: DK_UI__DELETE_ENABLED
  kafka_read_enabled: true         # GET /api/v1/debug/kafka/*; ad-hoc reads of the configured topics
                                   # (source/dlq/sink aliases only). false = 403. env: DK_UI__KAFKA_READ_ENABLED

  # --- Timed consume pause (Live page; OPT-IN — pausing stops message intake) ---
  consume_pause:
    enabled: false                 # serve the pause API + Live-page control. env: DK_UI__CONSUME_PAUSE__ENABLED
    durations: [15s, 1m, 5m, 15m]  # preset buttons, whole seconds from 1s to 1h; the API accepts any value in it.
                                   # env: DK_UI__CONSUME_PAUSE__DURATIONS (JSON list)

  # --- Presentation ---
  max_rows: 5000                   # max rows returned by UI list endpoints. env: DK_UI__MAX_ROWS

  # --- Duration threshold (noise filter); the slow-task log line has logging.min_task_duration ---
  ws_min_duration: 500ms           # shortest task broadcast over WebSocket. env: DK_UI__WS_MIN_DURATION

  # --- Prometheus deep-links in the UI ---
  prometheus:
    url: ''                        # e.g. http://prometheus:9090. env: DK_UI__PROMETHEUS__URL
    rate_interval: 5m              # rate() interval used in dashboard PromQL, a PromQL text. env: DK_UI__PROMETHEUS__RATE_INTERVAL
    worker_label: ''               # PromQL label for worker-scoped queries. env: DK_UI__PROMETHEUS__WORKER_LABEL
                                   # Supports {worker_id}, {cluster_name}, {metrics_port}, {debug_port}
                                   # e.g. 'worker_id="{worker_id}"'
    cluster_label: ''              # PromQL label for cluster-wide queries. env: DK_UI__PROMETHEUS__CLUSTER_LABEL
                                   # e.g. 'cluster="{cluster_name}"'

  # --- Custom links shown in the dashboard nav ---
  custom_links: []                 # env: DK_UI__CUSTOM_LINKS (JSON list)
                                   # Each entry: {name: "...", url: "..."}; url supports {worker_id} etc.

  # --- Named URL bases for probe-details link templates (see ui-enrichment.md) ---
  link_bases: {}                   # env: DK_UI__LINK_BASES (JSON object)
                                   # e.g. {jira: 'https://jira.internal.example.com'}; a template base
                                   # missing here logs a startup warning and renders as plain text.

  # --- Deployment-provided custom cell renderers module (see ui-enrichment.md) ---
  custom_renderers_path: ''        # path to a JS module served at /api/v1/ui/renderers.js. env: DK_UI__CUSTOM_RENDERERS_PATH
                                   # '' = off. Must exist at boot when set. Referenced from
                                   # probe_field(view='custom', renderer=...) / Column(renderer=...).

  # --- drakkar-ui bundle fetching (never fatal; with nothing cached the worker serves no pages) ---
  release:
    enabled: true                    # resolve + serve the drakkar-ui bundle. env: DK_UI__RELEASE__ENABLED
    repo: wlame/drakkar-ui           # "owner/name" GitHub repo publishing UI bundles. env: DK_UI__RELEASE__REPO
    pinned_version: ''               # known-good UI release tag (e.g. v1.2.0); '' = unpinned. env: DK_UI__RELEASE__PINNED_VERSION
    cache_dir: ''                    # bundle cache root; '' = $XDG_CACHE_HOME/drakkar/ui. env: DK_UI__RELEASE__CACHE_DIR
    check_update: true               # resolve the latest release tag on startup. env: DK_UI__RELEASE__CHECK_UPDATE

  # --- Message Probe: its switch and the user-details write caps (see probe-user-details.md) ---
  probe:
    enabled: true                    # POST /api/v1/debug/probe; runs caller bytes through the live handler
                                     # and takes executor slots. false = 403, whatever auth_token says.
                                     # env: DK_UI__PROBE__ENABLED
    max_writes: 10000                # max probe.set/append/update calls per probe run; the first write
                                     # past the cap records one ProbeError, further writes drop silently.
                                     # env: DK_UI__PROBE__MAX_WRITES
    max_total_size: 5MB              # total serialized size of probe-details writes per run; past it,
                                     # writes drop like max_writes. env: DK_UI__PROBE__MAX_TOTAL_SIZE

  # --- Live timeline tuning: history depth, bar color rules, label roles (see ui-timeline.md) ---
  timeline:
    history_factor: 100              # depth = history_factor x executor.max_executors (x8 if no pool), capped at 100000. env: DK_UI__TIMELINE__HISTORY_FACTOR
    max_age: 1h                      # whole minutes from 1m to 24h; oldest task age the timeline shows. env: DK_UI__TIMELINE__MAX_AGE
    color_rules: []                  # first-match-wins bar-coloring rules; max 50. env: DK_UI__TIMELINE__COLOR_RULES (JSON list)
                                     # e.g. [{name: failed, when: {field: status, op: eq, value: failed}, color: red}]
    labels:                          # which task label fills each special role; '' = unbound
      tag: ''                        # env: DK_UI__TIMELINE__LABELS__TAG
      caption: ''                    # env: DK_UI__TIMELINE__LABELS__CAPTION
      highlight: ''                  # env: DK_UI__TIMELINE__LABELS__HIGHLIGHT
      filter: ''                     # env: DK_UI__TIMELINE__LABELS__FILTER
      marker: ''                     # env: DK_UI__TIMELINE__LABELS__MARKER
    events: []                       # declared custom timeline event types (see ui-timeline-events.md); max 50.
                                     # YAML only.
                                     # e.g. [{name: deploy, kind: marker, color: purple, action: link, link: "{grafana}/d?from={ts_ms}"}]

  # --- Operator docs site served at /docs/ + contextual anchors (see ui-docs.md) ---
  docs:
    site_dir: ''                     # directory of a PREBUILT static site (mkdocs site/, Sphinx html/, plain
                                     # HTML); '' = feature off, /docs/ answers a hint-404. env: DK_UI__DOCS__SITE_DIR
    title: Docs                      # nav-entry label + default drawer heading. env: DK_UI__DOCS__TITLE
    anchors: []                      # contextual links from UI surfaces into the site; max 200. YAML only.
                                     # Each entry: {match: {...}, path: 'page.html#frag', title: '...'}; match sets
                                     # exactly one of label (+optional value), sink, event, page.
                                     # e.g. [{match: {sink: archive_results_db}, path: 'operations/#archive-database'}]
```

---

## Cache (`cache:`)

📚 [Deep details](configuration.md#cache-cache) · [Cache page](cache.md)

Optional handler-accessible key/value cache. **Disabled by default**.

```yaml
cache:
  enabled: false                   # master switch; false = no-op stub. The cache file lives in storage.dir. env: DK_CACHE__ENABLED
  flush_interval: 3s               # write-behind flush cadence. env: DK_CACHE__FLUSH_INTERVAL
  cleanup_interval: 1m             # expired-row cleanup cadence. env: DK_CACHE__CLEANUP_INTERVAL
  max_memory_entries: 10000        # in-memory LRU cap; 0 = unbounded (warns). env: DK_CACHE__MAX_MEMORY_ENTRIES
  exchange:                        # cluster/global rows pulled from peers over HTTP; the peers come from autodiscover,
                                   # so with autodiscover off this does nothing and logs one info line at startup.
    enabled: true                  # env: DK_CACHE__EXCHANGE__ENABLED
    interval: 30s                  # cycle period. env: DK_CACHE__EXCHANGE__INTERVAL
    page_size: 500                 # 1-1000 rows per page; pages until the peer says more=false. env: DK_CACHE__EXCHANGE__PAGE_SIZE
    timeout: 5s                    # time limit for one request to a peer (connect: 2s). env: DK_CACHE__EXCHANGE__TIMEOUT
    cycle_deadline: null           # hard cap on one cycle, at least 100ms; null = interval x 0.9; must be < interval.
                                   # env: DK_CACHE__EXCHANGE__CYCLE_DEADLINE
    max_concurrent_peers: 4        # peers pulled from in parallel. env: DK_CACHE__EXCHANGE__MAX_CONCURRENT_PEERS
```

---

## Own storage (`storage:`)

📚 [Upgrading](upgrading.md) · [Local databases](local-databases.md)

The own folders of the workers (each worker's live recorder file, cache file and crash marker, in `<dir>/<cluster>/<worker>/`), and the folder every worker of one cluster sees (registry cards, sealed history, archives, merge results). `dir` must be a local filesystem.

```yaml
storage:
  dir: /tmp/drakkar                # local disk; holds <cluster>/<worker>/ per worker, created with mode 0700
                                   # when missing; '' = no own files; with recorder.enabled and provider
                                   # sqlite the load fails. env: DK_STORAGE__DIR
  shared_dir: ''                   # '' = the same folder as dir (one host). env: DK_STORAGE__SHARED_DIR
```

---

## Flight recorder (`recorder:`)

📚 [Recent events](recent-events.md) · [Postgres history](recorder-postgres.md) · [Archiving](local-databases.md#archiving)

Durable recording of what the worker does. It does not depend on the UI. The memory store exists to serve the UI, so it follows `ui.enabled`:

| `ui.enabled` | `recorder.enabled` | Recorder object | Memory store | Durable store |
|---|---|---|---|---|
| off | off | none | no | no: main loop only |
| on | off | yes | yes | no: the UI shows the memory window |
| off | on | yes | no | yes |
| on | on | yes | yes | yes |

```yaml
recorder:
  enabled: true                    # durable recording. Off: no file and no table; a trace with history=1 answers the
                                   # memory rows. env: DK_RECORDER__ENABLED
  provider: sqlite                 # sqlite (needs storage.dir) | postgres (needs postgres.connection declared under
                                   # connections.postgres). env: DK_RECORDER__PROVIDER
  flush_interval: 5s               # whole seconds; tick of the memory, file and Postgres loops. env: DK_RECORDER__FLUSH_INTERVAL
  max_buffer_rows: 50000           # capacity of the intake, the file queue and the Postgres queue, each.
                                   # env: DK_RECORDER__MAX_BUFFER_ROWS

  # --- The memory store (exists only with ui.enabled) ---
  memory:
    window: 2m                     # keep events in memory this long, 10s to 24h, whole seconds. env: DK_RECORDER__MEMORY__WINDOW
    rows_per_executor: 3000        # row cap = this x executor.max_executors, 100-1000000; about
                                   # 4 x window / average task seconds. env: DK_RECORDER__MEMORY__ROWS_PER_EXECUTOR
    max_size: 256MiB               # data budget of the memory store, 16MiB to 512MiB. env: DK_RECORDER__MEMORY__MAX_SIZE

  # --- What the recorder keeps ---
  capture:
    events: true                   # off: no event rows anywhere, runtime health events included. env: DK_RECORDER__CAPTURE__EVENTS
    config: true                   # write worker config into each file (history attribution). env: DK_RECORDER__CAPTURE__CONFIG
    output: true                   # include subprocess output in events. env: DK_RECORDER__CAPTURE__OUTPUT
    stdin: false                   # store each task's stdin (capped) in task_started metadata; failures always
                                   # store it. env: DK_RECORDER__CAPTURE__STDIN
    max_stdin: 64KiB               # size cap for stored stdin, 0 = no limit. env: DK_RECORDER__CAPTURE__MAX_STDIN
    min_event_duration: 0          # shortest task whose event is written; 0 = all. env: DK_RECORDER__CAPTURE__MIN_EVENT_DURATION
    min_output_duration: 500ms     # shortest task whose stdout/stderr go into the event.
                                   # env: DK_RECORDER__CAPTURE__MIN_OUTPUT_DURATION
    env_vars: []                   # env vars captured into the worker_config table; e.g. ['GIT_SHA', 'K8S_POD_NAME'].
                                   # env: DK_RECORDER__CAPTURE__ENV_VARS (JSON list)

  # --- Handler annotations (see annotations.md; 0 = no limit) ---
  annotations:
    enabled: true                  # record self.annotate(...) rows in the events table. env: DK_RECORDER__ANNOTATIONS__ENABLED
    max_size: 16KiB                # cap per annotation payload; oversize records drop whole. env: DK_RECORDER__ANNOTATIONS__MAX_SIZE
    max_size_per_call: 256KiB      # total annotation size one hook invocation may add.
                                   # env: DK_RECORDER__ANNOTATIONS__MAX_SIZE_PER_CALL
    max_log_size: 2KiB             # cap on the payload copy in the drop-warning log line. env: DK_RECORDER__ANNOTATIONS__MAX_LOG_SIZE

  # --- Recorder files; used only with provider sqlite ---
  sqlite:
    rotation_interval: 1h          # how often to rotate the file, whole hours. env: DK_RECORDER__SQLITE__ROTATION_INTERVAL
    max_flush_retries: 3           # retries on transient file errors. env: DK_RECORDER__SQLITE__MAX_FLUSH_RETRIES
    state_snapshots: true          # periodic worker-state snapshots. env: DK_RECORDER__SQLITE__STATE_SNAPSHOTS
    state_interval: 10s            # snapshot and resource-sample frequency, whole seconds.
                                   # env: DK_RECORDER__SQLITE__STATE_INTERVAL

  # --- Archives of sealed files; used only with provider sqlite (see local-databases.md#archiving) ---
  archive:
    enabled: true                  # fold recorder files into one .db.gz per cluster and window. env: DK_RECORDER__ARCHIVE__ENABLED
    window: 24h                    # archive window, whole hours; must be >= sqlite.rotation_interval. env: DK_RECORDER__ARCHIVE__WINDOW
    due_after: 48h                 # a window is archived this long after it ends, whole hours; must be >=
                                   # sqlite.rotation_interval. env: DK_RECORDER__ARCHIVE__DUE_AFTER
    retention: 30d                 # delete archives older than this, whole days; 0 = keep for ever. Must cover
                                   # due_after + window. env: DK_RECORDER__ARCHIVE__RETENTION
    merged_retention: 32d          # delete UI merge results older than this, whole days; 0 = never.
                                   # env: DK_RECORDER__ARCHIVE__MERGED_RETENTION
    interval: null                 # time between archive slots, aligned to the epoch, whole seconds, at least 30s.
                                   # With neither interval nor cron, a slot every 1h. env: DK_RECORDER__ARCHIVE__INTERVAL
    cron: null                     # five-field UTC cron for the archive slots instead of interval; give at most one.
                                   # env: DK_RECORDER__ARCHIVE__CRON
    lease: 1m                      # lease of an archive slot, 40s to 24h, whole seconds; refreshed while the pass runs,
                                   # taken over when nobody refreshed it this long. env: DK_RECORDER__ARCHIVE__LEASE
    sweep_after: 2h                # archive work files older than this belong to a dead archiver and are removed;
                                   # at least 60s, whole seconds. env: DK_RECORDER__ARCHIVE__SWEEP_AFTER

  # --- The events table; used only with provider postgres (see recorder-postgres.md) ---
  postgres:
    connection: main               # name under connections.postgres. env: DK_RECORDER__POSTGRES__CONNECTION
    table_prefix: ''               # '' -> <schema>.recorder_events; 'heavy' -> <schema>.heavy_recorder_events;
                                   # [a-z][a-z0-9_]{0,23}. env: DK_RECORDER__POSTGRES__TABLE_PREFIX
    initial_retention: 24h         # 1h to 168h, whole hours; written into the table comment when this worker creates the table.
                                   # env: DK_RECORDER__POSTGRES__INITIAL_RETENTION
    store_completed_output: false  # send stdout and stderr to Postgres; the stderr of a task_failed row is always sent.
                                   # env: DK_RECORDER__POSTGRES__STORE_COMPLETED_OUTPUT
    max_output: 64KiB              # longest stdout or stderr value in the table, in UTF-8 bytes; 0 = no limit, else at
                                   # least 1KiB; a longer value keeps its first and last half. env: DK_RECORDER__POSTGRES__MAX_OUTPUT
    max_size: 0                    # largest total size of the table; 0 = no limit, else at least 64MiB; the upkeep drops
                                   # the oldest hours, never the previous or the current one. env: DK_RECORDER__POSTGRES__MAX_SIZE
    batch_rows: 5000               # most rows in one COPY, 100-50000. env: DK_RECORDER__POSTGRES__BATCH_ROWS
    max_queue: 128MiB              # most text waiting for Postgres, 16MiB to 1GiB. env: DK_RECORDER__POSTGRES__MAX_QUEUE
    resilience: {}                 # the shared resilience block: timeout, max_retries, backoff, max_backoff,
                                   # breaker_failures, breaker_cooldown. env: DK_RECORDER__POSTGRES__RESILIENCE__<KEY>

  # --- The history search of the Trace page (trace?history=1) ---
  search:
    max_files: 256                 # most sealed files one search opens; used only with provider sqlite.
                                   # env: DK_RECORDER__SEARCH__MAX_FILES
    timeout: 10s                   # time limit for one search; with postgres also the server statement limit (at most 1h).
                                   # env: DK_RECORDER__SEARCH__TIMEOUT
```

---

## Autodiscover (`autodiscover:`)

📚 [Fleet registry](fleet.md) · [Periodic tasks](periodic-tasks.md) · [Upgrading](upgrading.md)

How workers find each other and elect the one worker that runs each slot of a cluster task. One provider serves both jobs. **Off by default**: a worker with it off publishes no card, reads no peers, runs no election and pulls no cache rows, `/api/v1/workers` lists only this worker, and a declared `scope='cluster'` task fails startup. The pipeline itself does not depend on it.

**Resilience block.** A time limit per attempt, retries with doubling waits (`backoff` up to `max_backoff`), and a circuit breaker that skips the provider after `breaker_failures` failed operations in a row and lets one probe through after `breaker_cooldown`. The registry and the election each build their own caller from this one block, so each has its own breaker. The semantics are on [Fleet registry](fleet.md#resilience).

```yaml
autodiscover:
  enabled: false                   # env: DK_AUTODISCOVER__ENABLED
  provider: ''                     # consul | filesystem | postgres | redis; required when enabled.
                                   # filesystem needs storage.shared_dir: cards in <storage.shared_dir>/fleet/workers/,
                                   # locks in <storage.shared_dir>/tasks/<cluster>/;
                                   # postgres keeps the cards in the table fleet_workers and the locks in the table
                                   # task_locks of connections.postgres.<connection>;
                                   # redis keeps them in keys of connections.redis.<connection>, with an index of the workers;
                                   # consul keeps them in keys of connections.consul.<connection>, each held by a session.
                                   # Every worker of one cluster uses the same provider. env: DK_AUTODISCOVER__PROVIDER
  connection: main                 # entry of connections.<provider>; read by postgres, redis and consul.
                                   # env: DK_AUTODISCOVER__CONNECTION
  heartbeat: 10s                   # card publish period. env: DK_AUTODISCOVER__HEARTBEAT
  refresh: 15s                     # peer card read period. env: DK_AUTODISCOVER__REFRESH
  offline_after: 1m                # card older than this = offline; at least 2 x heartbeat and at least 10s.
                                   # consul: sets the session's time to live (half of this, at least 10 s); at most 24h.
                                   # env: DK_AUTODISCOVER__OFFLINE_AFTER
  prune_after: 24h                 # delete cards older than this; at least offline_after. Not used by consul.
                                   # env: DK_AUTODISCOVER__PRUNE_AFTER
  resilience:
    timeout: 5s                    # time limit for one attempt. env: DK_AUTODISCOVER__RESILIENCE__TIMEOUT
    max_retries: 2                 # extra attempts after a transient error. env: DK_AUTODISCOVER__RESILIENCE__MAX_RETRIES
    backoff: 200ms                 # wait before the first retry, doubling. env: DK_AUTODISCOVER__RESILIENCE__BACKOFF
    max_backoff: 5s                # longest wait between attempts; >= backoff. env: DK_AUTODISCOVER__RESILIENCE__MAX_BACKOFF
    breaker_failures: 5            # failed operations in a row that open the breaker.
                                   # env: DK_AUTODISCOVER__RESILIENCE__BREAKER_FAILURES
    breaker_cooldown: 30s          # open time before one probe operation.
                                   # env: DK_AUTODISCOVER__RESILIENCE__BREAKER_COOLDOWN
```

## Connections (`connections:`)

📚 [Deep details](configuration.md#connections) · [Fleet registry](fleet.md#connections)

A service is declared once. `connections.<kind>.<name>` says how to reach it and log in; a consumer says `connection: <name>` (default `main`), and the entry must be declared. A consumer in use that names an entry not declared fails the load with `<path>.connection '<name>' is not defined under connections.<kind>`. In use means: an enabled Kafka source, every configured sink of a connection kind, the DLQ when it has a destination, the recorder's Postgres table, and `autodiscover` when enabled with provider `consul`, `postgres` or `redis`. A different login to one server is a different connection. The names are user-chosen and written in lower case. Set the secret fields (`sasl_password`, `ssl_key_password`, `token`, `url`, `uri`, `dsn`) from the environment; see [Security](security.md#provider-credentials).

```yaml
connections:
  kafka:
    main:
      brokers: localhost:9092          # bootstrap servers, comma-separated. env: DK_CONNECTIONS__KAFKA__MAIN__BROKERS

      # Transport security. Default PLAINTEXT emits no client properties at all.
      security:
        protocol: PLAINTEXT            # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL. env: DK_CONNECTIONS__KAFKA__MAIN__SECURITY__PROTOCOL
        sasl_mechanism: null           # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI | OAUTHBEARER.
                                       #   Required for SASL_* protocols, rejected for the others.
        sasl_username: ''
        sasl_password: ''              # a secret; prefer the env override over a YAML literal.
                                       #   env: DK_CONNECTIONS__KAFKA__MAIN__SECURITY__SASL_PASSWORD
        ssl_ca_location: ''            # PEM CA bundle; empty uses the system trust store.
        ssl_certificate_location: ''   # client cert (mutual TLS).
        ssl_key_location: ''           # client key (mutual TLS); requires ssl_certificate_location.
        ssl_key_password: ''           # a secret; passphrase for an encrypted client key.
        ssl_endpoint_identification_algorithm: null  # unset = librdkafka default ("https", verification on).
                                       #   Set to "none" ONLY for internal CAs without matching SANs.

      # Escape hatch: raw librdkafka properties, merged AFTER `security` so they win, for every
      # client of this connection. A Kafka sink's or the DLQ's own client_config is merged after
      # this one. These four keys are rejected at startup, because each one backs a delivery
      # invariant: enable.auto.commit, partition.assignment.strategy, group.id, bootstrap.servers.
      client_config: {}                # env: DK_CONNECTIONS__KAFKA__MAIN__CLIENT_CONFIG (JSON object)

      # Kafka-UI deep links of the source topic: when both are set, the operator UI shows a
      # clickable icon next to every <partition:offset>. Both empty = feature disabled silently.
      ui_url: ''                       # env: DK_CONNECTIONS__KAFKA__MAIN__UI_URL  · example: http://kafka-ui:8080
      ui_cluster_name: ''              # cluster name registered in Kafka-UI. env: DK_CONNECTIONS__KAFKA__MAIN__UI_CLUSTER_NAME
  postgres:
    main:
      dsn: postgresql://user:password@host/db  # required; a secret. env: DK_CONNECTIONS__POSTGRES__MAIN__DSN
      schema: drakkar                  # the schema of the tables Drakkar itself keeps; lower-case identifier. env: DK_CONNECTIONS__POSTGRES__MAIN__SCHEMA
      manage_schema: true              # create the schema and tables at the first provider start. false: they must exist;
                                       #   create them with `drakkar-schema postgres --schema=<name>`; add
                                       #   `--recorder [--table-prefix=<p>] [--retention-hours=<n>]` for the
                                       #   recorder events table (the role must own it).
                                       #   env: DK_CONNECTIONS__POSTGRES__MAIN__MANAGE_SCHEMA
      pool_min: 1                      # env: DK_CONNECTIONS__POSTGRES__MAIN__POOL_MIN
      pool_max: 4                      # must be >= pool_min. This pool serves the registry and the election; a Postgres sink
                                       #   has a pool of its own. env: DK_CONNECTIONS__POSTGRES__MAIN__POOL_MAX
  redis:
    main:
      url: redis://localhost:6379/0    # a secret; rediss:// is TLS, the path is the database number; no query options (?...); percent-encode special characters of the password.
                                       #   env: DK_CONNECTIONS__REDIS__MAIN__URL
      state_prefix: 'drakkar:'         # prefix of the keys Drakkar itself keeps; letters, digits, _ : . -; may be empty.
                                       #   env: DK_CONNECTIONS__REDIS__MAIN__STATE_PREFIX
  mongo:
    main:
      uri: mongodb://mongo:27017       # required; a secret. env: DK_CONNECTIONS__MONGO__MAIN__URI
  consul:
    main:
      address: http://127.0.0.1:8500   # http:// or https://, checked when a provider starts. env: DK_CONNECTIONS__CONSUL__MAIN__ADDRESS
      token: ''                        # ACL token, sent in the X-Consul-Token header; a secret. env: DK_CONNECTIONS__CONSUL__MAIN__TOKEN
      datacenter: ''                   # '' = the agent's datacenter. env: DK_CONNECTIONS__CONSUL__MAIN__DATACENTER
      state_prefix: drakkar            # prefix of the keys Drakkar itself keeps; segments of letters, digits, _ . - joined by '/';
                                       #   no leading or trailing '/'; no segment '.' or '..'. env: DK_CONNECTIONS__CONSUL__MAIN__STATE_PREFIX
```

---

## App config

The reserved top-level `app:` section carries **user-defined application
config** — the framework passes it through unvalidated to the
handler-declared Pydantic model and exposes the validated instance as
`self.app_config`. Env overrides use the handler's own prefix (e.g.
`MYAPP_SCORING__URL`), never `DK_APP__*` (rejected at startup). The Debug
UI's config reference renders the model as its own `Application` group,
secrets masked. See [App Config](app-config.md) for the full feature.

---

## Environment-variable override cheatsheet

The pattern: **`DK_<SECTION>__<FIELD>`** -- prefix `DK_`, double underscore between nesting levels, single underscore within a field name.

| Where | Path → env var |
|-------|---------------|
| One level deep | `cluster.name` → `DK_CLUSTER__NAME`, `metrics.port` → `DK_METRICS__PORT` |
| Section, sub-section, field | `sources.kafka.topic` → `DK_SOURCES__KAFKA__TOPIC` |
| Two levels deep | `cache.exchange.interval` → `DK_CACHE__EXCHANGE__INTERVAL` |
| Map key (sink instance) | `sinks.postgres.main-db.pool_max` → `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX` |
| Map key (connection) | `connections.kafka.main.brokers` → `DK_CONNECTIONS__KAFKA__MAIN__BROKERS` |
| List element by index | `sources.http.clients[0].rpm` → `DK_SOURCES__HTTP__CLIENTS__0__RPM` |
| List value | `recorder.capture.env_vars` → `DK_RECORDER__CAPTURE__ENV_VARS='["GIT_SHA","DEPLOY_ENV"]'` (JSON) |
| Dict value | `executor.env` → `DK_EXECUTOR__ENV='{"FOO":"bar"}'` (JSON) |

Special cases:

- **`DK_CONFIG`** -- selects which YAML file to load; not a config field. Excluded from the env→config merge.
- **Names in map keys** -- a sink or connection named `main-db` is `MAIN_DB` in the env var. A segment takes the name in the YAML whose lower-case form with `-` replaced by `_` equals the lower-case segment. With no such name, the variable declares a name of its own (`main_db`). When two names match, the load fails.
- **Unknown keys** -- a variable whose path is not a config key fails the load, as a YAML key does (see [Unknown keys](configuration.md#unknown-keys)).
- **Booleans** -- `true` / `false` (also `1` / `0`, `yes` / `no`).
- **Nulls** -- omit the env var, or use the literal `null` if the YAML value should be reset.

See [Configuration Loading](configuration.md#configuration-loading) for the precedence order (`defaults → YAML → env`) and the deep-merge semantics.
