# Upgrading

This page has two guides. Use the first when you move from release 2.4.0 to
this release. Use the second first when you come from a release older than
2.4.0, then the first.

- **[From 2.4.0 to this release](#from-2-4-0).** The config has one layout
  with one set of rules: unknown keys fail the load, durations and sizes are
  written with units, a service is declared once under `connections`, and every
  feature is optional and says what it needs. There is no migration tool. The
  old keys are not read, and the guide below lists every one of them with
  its new place.
- **[To 2.4.0 from an older release](#to-2-4-0).** Release 2.4.0 moved the
  settings for files, archives, worker discovery and cache exchange into
  new sections.

## Upgrading from 2.4.0 to this release {#from-2-4-0}

This release is not backward compatible. A config written for 2.4.0 **fails
to load**, and the message lists every key that must change. The release
changes these things:

- Every key has its final name and place. A name carries no unit; the value does.
- Services (Kafka, Postgres, Redis, Mongo, Consul) are declared once under
  `connections`, and sources, sinks, the DLQ, the recorder and autodiscover
  name them.
- The flight recorder is a section of its own, `recorder`, and it does not depend
  on the UI. The HTTP server is a section of its own, `api`.
- The worker registry and the election of cluster tasks are one optional feature,
  `autodiscover`, with one provider.
- The DLQ record is the original message, and every sink declares a failure policy.
- The recorder can keep its history in Postgres and recent events in memory.
- The archive has a cluster scope and an own-files scope.
- Plugins, `periodic.tasks`, the throughput feature and the `static` registry are
  removed.

### 1. Plan the upgrade {#plan}

Work in this order:

1. **Drain the old DLQ topic**, or give the new workers a new `dlq.topic`
   ([the DLQ record](#dlq-record)). Records written by an older worker are not
   replayed by the new tool.
2. **Decide how to roll out.** The archive lock moved, so a worker of 2.4.0 and
   a worker of this release cannot see each other's lock. Stop every worker,
   upgrade and start them again; or set `recorder.archive.enabled: false` on every
   worker during the rolling restart and turn it back on at the end
   ([the archive lock](#periodic-tasks-and-the-archive)). The registry card, the
   sealed recorder files, the meta files and the cache exchange route keep their
   format, so workers of both releases read each other's cards and files.
3. **Rewrite the config** with [the steps](#migrate-config) and the
   [table of every old key](#path-table).
4. **Change the handler code** ([handler and code changes](#code-changes)).
5. **Update log queries, dashboards and alerts** ([behaviour](#behaviour)).
6. **Start one worker and read the load message.** Repeat until it starts
   ([check the result](#check)).
7. **Delete the old files and tables** ([old files](#old-files)).

### 2. The rules of the layout {#rules}

Learn these once. Every section follows them.

1. **Unknown keys fail the load**, in the YAML file and in a `DK_` variable. One
   message lists them all, in sorted order, with the closest valid key:

    ```
    config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
      sinks.kafka.results.max_retires (did you mean 'max_retries'?)
      ui.recorder
    ```

    A list element is written `[i]`, for example `ui.timeline.events[2].colour`. The
    names you choose (a sink instance, a connection) are not checked, but the keys
    below them are. The free maps `app`, `executor.env`, `headers` of an HTTP sink,
    `statements`, `scripts` and `client_config` accept any key.
2. **The top level answers operator questions**, in this order: `worker`, `cluster`,
   `connections`, `sources`, `executor`, `threads`, `sinks`, `dlq`, `storage`,
   `recorder`, `cache`, `autodiscover`, `metrics`, `logging`, `runtime_health`, `api`,
   `ui`, `app`.
3. **A service is declared once.** `connections.<kind>.<name>` (kinds `kafka`,
   `postgres`, `redis`, `mongo`, `consul`) says how to reach a service and how to log
   in. A consumer says `connection: <name>`; the default name is `main`. The entry
   must be declared, or the load fails with
   `<path>.connection '<name>' is not defined under connections.<kind>`. The checked
   consumers are an enabled Kafka source, every configured sink of a connection kind,
   the DLQ when it has a destination, `recorder.postgres` when the recorder is
   enabled with provider `postgres`, and `autodiscover` when enabled with provider
   `consul`, `postgres` or `redis`. A Kafka cluster not named `main` therefore needs
   `sources.kafka.connection`, `dlq.connection` and the `connection` of each Kafka
   sink. A different login to one server is a different connection. Each sink owns its
   client or pool.
4. **Choice plus block.** `recorder.provider` (`sqlite` or `postgres`) selects an
   implementation; `recorder.sqlite` and `recorder.postgres` hold its settings. The
   block of the provider you do not select loads and does nothing.
5. **Switches.** A feature with settings is a block with `enabled`
   (`autodiscover.enabled`, `recorder.annotations.enabled`). A switch with no other
   setting is a flat `<feature>_enabled` (`ui.merge_enabled`).
6. **Limits.** `0` means no limit. `null` means "not given: derive it". A limit is
   `max_<thing>`.
7. **Units are in the value, not in the name.** A duration is `0` or terms of
   `<number><unit>` with no spaces, units `d`, `h`, `m`, `s`, `ms` in descending
   order, each at most once: `30s`, `2h45m`, `1.5h`, `500ms`, `30d`. A size is `0`
   or one `<number><unit>` with unit `B`, `KB`, `MB`, `GB`, `TB` (1000) or `KiB`,
   `MiB`, `GiB`, `TiB` (1024): `64KiB`, `256MB`. A bare number other than `0` is
   refused. An environment variable takes the same text
   (`DK_EXECUTOR__TASK_TIMEOUT=5m`). Some keys take whole seconds, hours or days only;
   `90s` for a key that counts whole minutes fails. The messages:

    ```
    <path> needs a unit, for example 30s; got '30'
    <path> is not a duration, got '5 min'; write for example 30s or 2h45m
    <path> needs a unit, for example 64KiB; got '65536'
    <path> is not a size, got '64kb'; write for example 64KiB or 256MB
    <path> must be a whole number of seconds, got '1500ms'
    ```

    (`hours` and `days` replace `seconds` for the keys that count in them.)
8. **One failure vocabulary.** `timeout`, `max_retries` (retries after the first
   attempt), `backoff`, `max_backoff`, `breaker_failures`, `breaker_cooldown`. A sink's
   budget is `delivery_timeout`. A failure event is `on_<event>: <action>`.
9. **Shallow.** At most `section.block.key`, not counting the names you choose and the
   Kafka `security` block.
10. **Environment variables** are `DK_` plus the new path in upper case with `__`
    between the levels. A name that you chose in YAML (`main-db`) is reached by its
    upper-case form with `_`: `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX` sets
    `sinks.postgres.main-db.pool_max`. When two declared names differ only by case or
    by `-` against `_`, such a variable fails the load.

**Every feature is optional.** A worker can run its main loop only. Each feature and
what it needs:

| Feature | Switch | Needs |
|---|---|---|
| API server, `/healthz`, `/readyz`, peer routes | `api.enabled` | nothing |
| Operator UI, `/ws`, `/api/v1/*`, docs | `ui.enabled` | `api.enabled` |
| Kafka source | `sources.kafka.enabled` | `connections.kafka.<connection>` |
| Sinks (`kafka`, `postgres`, `mongo`, `redis`) | the instance | `connections.<kind>.<connection>` |
| DLQ | `dlq.enabled` | `connections.kafka.<dlq.connection>` and a topic |
| Recorder, memory store | `recorder.enabled` or `ui.enabled` | the memory store needs `ui.enabled` |
| Recorder, provider `sqlite` | `recorder.enabled` | `storage.dir` |
| Recorder, provider `postgres` | `recorder.enabled` | `connections.postgres.<recorder.postgres.connection>` |
| Archive | `recorder.archive.enabled` | provider `sqlite`, `storage.dir`; the cluster scope also needs `autodiscover` and `storage.shared_dir` |
| Autodiscover | `autodiscover.enabled` | `autodiscover.provider`, `api.enabled`; `filesystem` needs `storage.shared_dir`; `consul`, `postgres` and `redis` need their connection |
| Cluster-scope periodic tasks | `@periodic(scope='cluster')` | `autodiscover` |
| Cache | `cache.enabled` | `storage.dir` for persistence |
| Cache exchange | `cache.exchange.enabled` | `cache.enabled`, `storage.dir`, `autodiscover` |

### 3. Migrate the config, step by step {#migrate-config}

Do the steps in order. After each one you can start a worker: the load message lists
what is left.

1. **Find your keys.** Search the YAML files **and** the environment (Kubernetes
   manifests, `.env` files, CI variables) for keys of the [table](#path-table). An
   environment variable is `DK_` plus the YAML path with `__` between the levels, so
   `DK_KAFKA__BROKERS` is `kafka.brokers`.
2. **Declare the services.** Move the root `kafka:` section to
   `connections.kafka.main`, every key unchanged (`brokers`, `security`,
   `client_config`, `ui_url`, `ui_cluster_name`). A config with the Kafka source on
   needs this entry even when empty (`main: {}`), because the source and the DLQ name
   `main` by default. Move each `dsn`, `uri` and `url` of a Postgres, Mongo or Redis
   sink to an entry under `connections.postgres`, `connections.mongo` or
   `connections.redis`, and name the entry in the sink's `connection`. Sinks that used
   one server can share one entry. A Kafka sink or the DLQ that has its own `brokers`
   gets an entry of its own (move its `brokers` and `security` there) and names it in
   `connection`. The `client_config` of a sink or of the DLQ stays on it; it is now
   merged after the connection's, so the sink wins on a shared key. A Postgres entry
   also holds `schema`, `manage_schema` and a `pool_min` and `pool_max` for the
   autodiscover tables; the sink's own `pool_min` and `pool_max` stay on the sink.
   Put secrets (`dsn`, `uri`, `url`, `token`, SASL and key passwords) in the
   environment: `DK_CONNECTIONS__POSTGRES__MAIN__DSN`.
3. **Rename the keys and write the units.** Follow the [table](#path-table). `120`
   seconds is `2m`, `500` milliseconds is `500ms`, `1048576` bytes is `1MiB`. The
   environment values change the same way: `DK_EXECUTOR__TASK_TIMEOUT_SECONDS=300`
   becomes `DK_EXECUTOR__TASK_TIMEOUT=5m`.
4. **Choose the recorder.** `ui.recorder.persistence` is gone. Unset with a
   `storage.dir`: write nothing (`recorder.enabled` defaults to `true`, the provider to
   `sqlite`). Unset with an empty `storage.dir`, or `none`: `recorder.enabled: false`.
   `sqlite`: `recorder.provider: sqlite`. `postgres`: `recorder.provider: postgres`. A
   worker with `ui.enabled: false` now records into `storage.dir` too; set
   `recorder.enabled: false` to keep it as it was.
5. **Choose the API and the UI.** `ui.host`, `ui.port` and `ui.public_url` are
   `api.host`, `api.port` and `api.public_url`. `ui.enabled: false` no longer closes
   the port: the worker still serves `/healthz`, `/readyz` and the peer route. To
   open no port at all, set `api.enabled: false` as well.
6. **Choose autodiscover.** If the workers use the workers list, the cache exchange or
   a cluster task, set `autodiscover.enabled: true` and one `autodiscover.provider`
   (`filesystem`, `postgres`, `redis` or `consul`) for the registry **and** the
   election. With `filesystem`, set `storage.shared_dir` to the folder you used as
   `shared.dir`; it no longer falls back to `storage.dir`. The default is off.
7. **Delete the removed keys:** `sinks.custom`, `throughput`, `periodic`,
   `fleet.registry.static`, `fleet.registry.filesystem.dir`, `fleet.registry.consul.mode`,
   `fleet.registry.consul.service_name`, `fleet.registry.redis`,
   `fleet.registry.postgres`, every `plugin_options`, and the top-level `webapp:` section
   (it is `sources.http`).
8. **Start a worker.** Read the message, fix the keys it lists, start again. Then read
   [Check the result](#check).

A config before and after (the keys that change; a Kubernetes `ConfigMap` holds the
same YAML):

```yaml
# before
kafka:
  brokers: kafka-1:9092
sources:
  kafka: {enabled: true, session_timeout_ms: 45000}
executor:
  task_timeout_seconds: 300
sinks:
  postgres:
    results:
      dsn: postgresql://app@db/app
dlq: {flush_timeout_seconds: 30}
ui:
  port: 8080
  recorder: {persistence: sqlite, rotation_interval_hours: 1}
history:
  archive: {retention_days: 30}
shared: {dir: /mnt/fleet}
fleet:
  registry: {provider: filesystem, offline_after_seconds: 60}
```

```yaml
# after
connections:
  kafka:
    main: {brokers: "kafka-1:9092"}
  postgres:
    main: {dsn: "postgresql://app@db/app"}   # better: DK_CONNECTIONS__POSTGRES__MAIN__DSN
sources:
  kafka: {enabled: true, session_timeout: 45s}
executor:
  task_timeout: 5m
sinks:
  postgres:
    results: {connection: main}
dlq: {flush_timeout: 30s}
api: {port: 8080}
recorder:
  provider: sqlite
  sqlite: {rotation_interval: 1h}
  archive: {retention: 30d}
storage: {shared_dir: /mnt/fleet}
autodiscover: {enabled: true, provider: filesystem, offline_after: 1m}
```

### 4. Every old key and its new place {#path-table}

One row for every key of the previous layout. `*` is a name that you choose (a sink
instance, a connection). `<entry>` is a name under `connections.<kind>`. A path that
ends in `[]` is a list element. The environment variable of a new path is `DK_` plus
the path in upper case with `__`; a value takes the same text as in YAML. A row
that says "unchanged" keeps its path, its default and its meaning.

| Old path | New path | How to write the value |
|---|---|---|
| `worker_name_env` | `worker.name_env` |  |
| `cluster_name` | `cluster.name` |  |
| `cluster_name_env` | `cluster.name_env` |  |
| `kafka.brokers` | `connections.kafka.main.brokers` |  |
| `kafka.security.protocol` | `connections.kafka.main.security.protocol` |  |
| `kafka.security.sasl_mechanism` | `connections.kafka.main.security.sasl_mechanism` |  |
| `kafka.security.sasl_username` | `connections.kafka.main.security.sasl_username` |  |
| `kafka.security.sasl_password` | `connections.kafka.main.security.sasl_password` |  |
| `kafka.security.ssl_ca_location` | `connections.kafka.main.security.ssl_ca_location` |  |
| `kafka.security.ssl_certificate_location` | `connections.kafka.main.security.ssl_certificate_location` |  |
| `kafka.security.ssl_key_location` | `connections.kafka.main.security.ssl_key_location` |  |
| `kafka.security.ssl_key_password` | `connections.kafka.main.security.ssl_key_password` |  |
| `kafka.security.ssl_endpoint_identification_algorithm` | `connections.kafka.main.security.ssl_endpoint_identification_algorithm` |  |
| `kafka.client_config` | `connections.kafka.main.client_config` |  |
| `kafka.ui_url` | `connections.kafka.main.ui_url` |  |
| `kafka.ui_cluster_name` | `connections.kafka.main.ui_cluster_name` |  |
| `executor.binary_path` | unchanged |  |
| `executor.env` | unchanged |  |
| `executor.env_inherit_parent` | unchanged |  |
| `executor.env_inherit_deny` | unchanged |  |
| `executor.max_executors` | unchanged |  |
| `executor.task_timeout_seconds` | `executor.task_timeout` | `120` is `2m`; whole seconds |
| `executor.max_stdout_bytes` | `executor.max_stdout` | a size; `0` is no limit |
| `executor.max_stderr_bytes` | `executor.max_stderr` | a size; `0` is no limit |
| `executor.window_size` | unchanged |  |
| `executor.max_retries` | unchanged |  |
| `executor.on_failure` | unchanged |  |
| `executor.drain_timeout_seconds` | `executor.drain_timeout` | `30` is `30s`; whole seconds |
| `executor.backpressure_high_multiplier` | unchanged |  |
| `executor.backpressure_low_multiplier` | unchanged |  |
| `sinks.kafka.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.kafka.*.max_retries` | unchanged |  |
| `sinks.kafka.*.depends_on` | unchanged |  |
| `sinks.kafka.*.topic` | unchanged |  |
| `sinks.kafka.*.brokers` | `connections.kafka.<entry>.brokers` | and `sinks.kafka.<name>.connection: <entry>`; a sink with an empty `brokers` uses `connections.kafka.main` |
| `sinks.kafka.*.ui_url` | unchanged |  |
| `sinks.kafka.*.security.protocol` | `connections.kafka.<entry>.security.protocol` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.sasl_mechanism` | `connections.kafka.<entry>.security.sasl_mechanism` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.sasl_username` | `connections.kafka.<entry>.security.sasl_username` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.sasl_password` | `connections.kafka.<entry>.security.sasl_password` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.ssl_ca_location` | `connections.kafka.<entry>.security.ssl_ca_location` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.ssl_certificate_location` | `connections.kafka.<entry>.security.ssl_certificate_location` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.ssl_key_location` | `connections.kafka.<entry>.security.ssl_key_location` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.ssl_key_password` | `connections.kafka.<entry>.security.ssl_key_password` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.security.ssl_endpoint_identification_algorithm` | `connections.kafka.<entry>.security.ssl_endpoint_identification_algorithm` | a sink with its own cluster names that entry in `sinks.kafka.<name>.connection` |
| `sinks.kafka.*.client_config` | unchanged | merged after the `client_config` of the connection; the sink wins on a shared key |
| `sinks.kafka.*.flush_timeout_seconds` | `sinks.kafka.*.flush_timeout` | `30.0` is `30s` |
| `sinks.postgres.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.postgres.*.max_retries` | unchanged |  |
| `sinks.postgres.*.depends_on` | unchanged |  |
| `sinks.postgres.*.dsn` | `connections.postgres.<entry>.dsn` | and `sinks.postgres.<name>.connection: <entry>` |
| `sinks.postgres.*.pool_min` | unchanged |  |
| `sinks.postgres.*.pool_max` | unchanged |  |
| `sinks.postgres.*.statements` | unchanged |  |
| `sinks.postgres.*.ui_url` | unchanged |  |
| `sinks.mongo.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.mongo.*.max_retries` | unchanged |  |
| `sinks.mongo.*.depends_on` | unchanged |  |
| `sinks.mongo.*.uri` | `connections.mongo.<entry>.uri` | and `sinks.mongo.<name>.connection: <entry>` |
| `sinks.mongo.*.database` | unchanged |  |
| `sinks.mongo.*.statements.*.collection` | unchanged |  |
| `sinks.mongo.*.statements.*.op` | unchanged |  |
| `sinks.mongo.*.statements.*.filter` | unchanged |  |
| `sinks.mongo.*.statements.*.update` | unchanged |  |
| `sinks.mongo.*.ui_url` | unchanged |  |
| `sinks.http.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.http.*.max_retries` | unchanged |  |
| `sinks.http.*.depends_on` | unchanged |  |
| `sinks.http.*.url` | unchanged |  |
| `sinks.http.*.method` | unchanged |  |
| `sinks.http.*.timeout_seconds` | `sinks.http.*.request_timeout` | `30` is `30s`; whole seconds |
| `sinks.http.*.headers` | unchanged |  |
| `sinks.http.*.encoding` | unchanged |  |
| `sinks.http.*.ui_url` | unchanged |  |
| `sinks.redis.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.redis.*.max_retries` | unchanged |  |
| `sinks.redis.*.depends_on` | unchanged |  |
| `sinks.redis.*.url` | `connections.redis.<entry>.url` | and `sinks.redis.<name>.connection: <entry>` |
| `sinks.redis.*.key_prefix` | unchanged |  |
| `sinks.redis.*.scripts` | unchanged |  |
| `sinks.redis.*.ui_url` | unchanged |  |
| `sinks.filesystem.*.on_failure` | unchanged | also takes `delivery_timeout`, `breaker_failures` and `breaker_cooldown` on the instance |
| `sinks.filesystem.*.max_retries` | unchanged |  |
| `sinks.filesystem.*.depends_on` | unchanged |  |
| `sinks.filesystem.*.base_path` | unchanged |  |
| `sinks.filesystem.*.ui_url` | unchanged |  |
| `sinks.custom` | removed | Plugins are not supported. Use one of the six built-in sink types; see the handler changes. |
| `sinks.delivery_timeout_seconds` | `sinks.defaults.delivery_timeout` | `30.0` is `30s`; every sink can override it |
| `sinks.circuit_breaker.failure_threshold` | `sinks.defaults.breaker_failures` | every sink can override it |
| `sinks.circuit_breaker.cooldown_seconds` | `sinks.defaults.breaker_cooldown` | `30.0` is `30s`; every sink can override it |
| `dlq.topic` | unchanged |  |
| `dlq.brokers` | `connections.kafka.<entry>.brokers` | and `dlq.connection: <entry>`; an empty `dlq.brokers` meant `connections.kafka.main` |
| `dlq.security.protocol` | `connections.kafka.<entry>.security.protocol` | name the entry in `dlq.connection` |
| `dlq.security.sasl_mechanism` | `connections.kafka.<entry>.security.sasl_mechanism` | name the entry in `dlq.connection` |
| `dlq.security.sasl_username` | `connections.kafka.<entry>.security.sasl_username` | name the entry in `dlq.connection` |
| `dlq.security.sasl_password` | `connections.kafka.<entry>.security.sasl_password` | name the entry in `dlq.connection` |
| `dlq.security.ssl_ca_location` | `connections.kafka.<entry>.security.ssl_ca_location` | name the entry in `dlq.connection` |
| `dlq.security.ssl_certificate_location` | `connections.kafka.<entry>.security.ssl_certificate_location` | name the entry in `dlq.connection` |
| `dlq.security.ssl_key_location` | `connections.kafka.<entry>.security.ssl_key_location` | name the entry in `dlq.connection` |
| `dlq.security.ssl_key_password` | `connections.kafka.<entry>.security.ssl_key_password` | name the entry in `dlq.connection` |
| `dlq.security.ssl_endpoint_identification_algorithm` | `connections.kafka.<entry>.security.ssl_endpoint_identification_algorithm` | name the entry in `dlq.connection` |
| `dlq.client_config` | unchanged | merged after the `client_config` of the connection; the DLQ wins on a shared key |
| `dlq.flush_timeout_seconds` | `dlq.flush_timeout` | `30.0` is `30s` |
| `dlq.on_send_failure` | unchanged |  |
| `metrics.enabled` | unchanged |  |
| `metrics.port` | unchanged |  |
| `metrics.task_label_histograms` | unchanged |  |
| `throughput.cost_label` | removed | The throughput feature is removed. Delete the key and `DK_THROUGHPUT__*`. |
| `throughput.min_cost` | removed | The throughput feature is removed. Delete the key. |
| `runtime_health.enabled` | unchanged |  |
| `runtime_health.tick_seconds` | `runtime_health.tick` | `0.25` is `250ms` |
| `runtime_health.warn_lag_seconds` | `runtime_health.warn_lag` | `0.1` is `100ms` |
| `runtime_health.stall_seconds` | `runtime_health.stall` | `1.0` is `1s` |
| `runtime_health.max_stall_stacks` | unchanged |  |
| `runtime_health.sample_interval_seconds` | `runtime_health.sample_interval` | `10.0` is `10s` |
| `runtime_health.history_window_seconds` | `runtime_health.history_window` | `900` is `15m`; whole seconds |
| `runtime_health.episode_max_seconds` | `runtime_health.max_episode` | `300.0` is `5m` |
| `runtime_health.probe_interval_seconds` | `runtime_health.stack_probe_enabled` and `runtime_health.stack_probe_interval` | `0.0` (off) is `stack_probe_enabled: false`; a positive value is `stack_probe_enabled: true` with that interval, for example `1s` |
| `io.max_threads` | `threads.io` | `0` is `null`; a value is 1 to 512 |
| `offload.max_threads` | `threads.offload` | `0` is `null`; a value is 1 to 32 |
| `logging.level` | unchanged |  |
| `logging.format` | unchanged |  |
| `logging.output` | unchanged |  |
| `ui.enabled` | unchanged | now needs `api.enabled`; controls only the user-facing UI |
| `ui.host` | `api.host` |  |
| `ui.port` | `api.port` |  |
| `ui.auth_token` | unchanged |  |
| `ui.allowed_ws_origins` | `ui.allowed_origins` |  |
| `ui.probe_enabled` | `ui.probe.enabled` |  |
| `ui.merge_enabled` | unchanged |  |
| `ui.kafka_read_enabled` | unchanged |  |
| `ui.public_url` | `api.public_url` |  |
| `ui.expose_env_vars` | `recorder.capture.env_vars` |  |
| `ui.max_rows` | unchanged |  |
| `ui.log_min_duration_ms` | `logging.min_task_duration` | `500` is `500ms` |
| `ui.ws_min_duration_ms` | `ui.ws_min_duration` | `500` is `500ms` |
| `ui.prometheus_url` | `ui.prometheus.url` |  |
| `ui.prometheus_rate_interval` | `ui.prometheus.rate_interval` | stays a PromQL text such as `5m` |
| `ui.prometheus_worker_label` | `ui.prometheus.worker_label` |  |
| `ui.prometheus_cluster_label` | `ui.prometheus.cluster_label` |  |
| `ui.custom_links` | unchanged |  |
| `ui.link_bases` | unchanged |  |
| `ui.custom_renderers_path` | unchanged |  |
| `ui.consume_pause.enabled` | unchanged |  |
| `ui.consume_pause.durations_seconds` | `ui.consume_pause.durations` | `[15, 60, 300, 900]` is `[15s, 1m, 5m, 15m]`; each entry 1s to 1h |
| `ui.recorder.persistence` | removed | Use `recorder.enabled` and `recorder.provider`: unset with a `storage.dir` needs nothing; unset with an empty `storage.dir` and `none` become `recorder.enabled: false`; `sqlite` is `recorder.provider: sqlite`; `postgres` is `recorder.provider: postgres`. |
| `ui.recorder.memory.window_seconds` | `recorder.memory.window` | `120` is `2m`; whole seconds |
| `ui.recorder.memory.rows_per_executor` | `recorder.memory.rows_per_executor` |  |
| `ui.recorder.memory.max_mib` | `recorder.memory.max_size` | `256` is `256MiB` |
| `ui.recorder.postgres.connection` | `recorder.postgres.connection` |  |
| `ui.recorder.postgres.table_prefix` | `recorder.postgres.table_prefix` |  |
| `ui.recorder.postgres.retention_hours` | `recorder.postgres.initial_retention` | `6` is `6h`, whole hours; the default is now `24h` |
| `ui.recorder.postgres.store_output` | `recorder.postgres.store_completed_output` |  |
| `ui.recorder.postgres.batch_rows` | `recorder.postgres.batch_rows` |  |
| `ui.recorder.postgres.queue_max_mib` | `recorder.postgres.max_queue` | `128` is `128MiB` |
| `ui.recorder.postgres.resilience.timeout_seconds` | `recorder.postgres.resilience.timeout` | `5.0` is `5s` |
| `ui.recorder.postgres.resilience.retries` | `recorder.postgres.resilience.max_retries` |  |
| `ui.recorder.postgres.resilience.backoff_seconds` | `recorder.postgres.resilience.backoff` | `0.2` is `200ms` |
| `ui.recorder.postgres.resilience.backoff_max_seconds` | `recorder.postgres.resilience.max_backoff` | `5.0` is `5s` |
| `ui.recorder.postgres.resilience.breaker.failure_threshold` | `recorder.postgres.resilience.breaker_failures` |  |
| `ui.recorder.postgres.resilience.breaker.cooldown_seconds` | `recorder.postgres.resilience.breaker_cooldown` | `30.0` is `30s` |
| `ui.recorder.store_events` | `recorder.capture.events` |  |
| `ui.recorder.store_config` | `recorder.capture.config` |  |
| `ui.recorder.store_state` | `recorder.sqlite.state_snapshots` |  |
| `ui.recorder.state_sync_interval_seconds` | `recorder.sqlite.state_interval` | `10` is `10s`; whole seconds |
| `ui.recorder.rotation_interval_hours` | `recorder.sqlite.rotation_interval` | `1` is `1h`; whole hours |
| `ui.recorder.store_output` | `recorder.capture.output` |  |
| `ui.recorder.store_stdin` | `recorder.capture.stdin` |  |
| `ui.recorder.stdin_max_bytes` | `recorder.capture.max_stdin` | `65536` is `64KiB` |
| `ui.recorder.flush_interval_seconds` | `recorder.flush_interval` | `5` is `5s`; whole seconds |
| `ui.recorder.max_buffer` | `recorder.max_buffer_rows` |  |
| `ui.recorder.max_flush_retries` | `recorder.sqlite.max_flush_retries` |  |
| `ui.recorder.event_min_duration_ms` | `recorder.capture.min_event_duration` | `0` is `0`; `100` is `100ms` |
| `ui.recorder.output_min_duration_ms` | `recorder.capture.min_output_duration` | `500` is `500ms` |
| `ui.recorder.annotations_enabled` | `recorder.annotations.enabled` |  |
| `ui.recorder.annotation_max_bytes` | `recorder.annotations.max_size` | `16384` is `16KiB` |
| `ui.recorder.annotation_max_bytes_per_call` | `recorder.annotations.max_size_per_call` | `262144` is `256KiB` |
| `ui.recorder.annotation_log_max_bytes` | `recorder.annotations.max_log_size` | `2048` is `2KiB` |
| `ui.release.enabled` | unchanged |  |
| `ui.release.repo` | unchanged |  |
| `ui.release.pinned_version` | unchanged |  |
| `ui.release.cache_dir` | unchanged |  |
| `ui.release.check_update` | unchanged |  |
| `ui.probe_details.max_writes` | `ui.probe.max_writes` |  |
| `ui.probe_details.max_total_bytes` | `ui.probe.max_total_size` | `5000000` is `5MB` |
| `ui.timeline.history_factor` | unchanged |  |
| `ui.timeline.max_age_minutes` | `ui.timeline.max_age` | `60` is `1h`; whole minutes |
| `ui.timeline.color_rules[].name` | unchanged |  |
| `ui.timeline.color_rules[].when[].label` | unchanged |  |
| `ui.timeline.color_rules[].when[].field` | unchanged |  |
| `ui.timeline.color_rules[].when[].op` | unchanged |  |
| `ui.timeline.color_rules[].when[].value` | unchanged |  |
| `ui.timeline.color_rules[].color` | unchanged |  |
| `ui.timeline.labels.tag` | unchanged |  |
| `ui.timeline.labels.caption` | unchanged |  |
| `ui.timeline.labels.highlight` | unchanged |  |
| `ui.timeline.labels.filter` | unchanged |  |
| `ui.timeline.labels.marker` | unchanged |  |
| `ui.timeline.events[].name` | unchanged |  |
| `ui.timeline.events[].kind` | unchanged |  |
| `ui.timeline.events[].color` | unchanged |  |
| `ui.timeline.events[].line` | unchanged |  |
| `ui.timeline.events[].label` | unchanged |  |
| `ui.timeline.events[].enabled` | unchanged |  |
| `ui.timeline.events[].show` | unchanged |  |
| `ui.timeline.events[].link` | unchanged |  |
| `ui.timeline.events[].action` | unchanged |  |
| `ui.trace.history_max_files` | `recorder.search.max_files` |  |
| `ui.trace.history_max_seconds` | `recorder.search.timeout` | `10.0` is `10s` |
| `ui.docs.site_dir` | unchanged |  |
| `ui.docs.title` | unchanged |  |
| `ui.docs.anchors[].match.label` | unchanged |  |
| `ui.docs.anchors[].match.value` | unchanged |  |
| `ui.docs.anchors[].match.sink` | unchanged |  |
| `ui.docs.anchors[].match.event` | unchanged |  |
| `ui.docs.anchors[].match.page` | unchanged |  |
| `ui.docs.anchors[].path` | unchanged |  |
| `ui.docs.anchors[].title` | unchanged |  |
| `cache.enabled` | unchanged |  |
| `cache.flush_interval_seconds` | `cache.flush_interval` | `3.0` is `3s` |
| `cache.cleanup_interval_seconds` | `cache.cleanup_interval` | `60.0` is `1m` |
| `cache.max_memory_entries` | unchanged | `null` (no cap) is `0`; `0` logs a startup warning |
| `storage.dir` | unchanged |  |
| `shared.dir` | `storage.shared_dir` | an empty `shared.dir` fell back to `storage.dir`; this is still true for the history folder, but `autodiscover.provider: filesystem` needs `storage.shared_dir` |
| `history.archive.enabled` | `recorder.archive.enabled` |  |
| `history.archive.window_hours` | `recorder.archive.window` | `24` is `24h`; whole hours |
| `history.archive.due_after_hours` | `recorder.archive.due_after` | `48` is `48h`; whole hours |
| `history.archive.retention_days` | `recorder.archive.retention` | `30` is `30d`; whole days |
| `history.archive.interval` | `recorder.archive.interval` | `3600` is `1h`; at least `30s` |
| `history.archive.cron` | `recorder.archive.cron` |  |
| `history.archive.lease_seconds` | `recorder.archive.lease` | `60` is `1m`; `40s` to `24h` |
| `history.archive.sweep_after_seconds` | `recorder.archive.sweep_after` | `7200` is `2h` |
| `history.merged_retention_days` | `recorder.archive.merged_retention` | `32` is `32d`; whole days |
| `periodic.election.provider` | `autodiscover.provider` | one provider for the registry and the election; the default `filesystem` is now `''` and the feature is off |
| `periodic.election.connection` | `autodiscover.connection` |  |
| `periodic.election.resilience.timeout_seconds` | `autodiscover.resilience.timeout` | the same block as the registry |
| `periodic.election.resilience.retries` | `autodiscover.resilience.max_retries` | the same block as the registry |
| `periodic.election.resilience.backoff_seconds` | `autodiscover.resilience.backoff` | the same block as the registry |
| `periodic.election.resilience.backoff_max_seconds` | `autodiscover.resilience.max_backoff` | the same block as the registry |
| `periodic.election.resilience.breaker.failure_threshold` | `autodiscover.resilience.breaker_failures` | the same block as the registry |
| `periodic.election.resilience.breaker.cooldown_seconds` | `autodiscover.resilience.breaker_cooldown` | the same block as the registry |
| `periodic.election.plugin_options` | removed | Plugins are not supported. Delete the key. |
| `periodic.tasks.*.enabled` | removed | `periodic.tasks` is removed. To stop a task, do not declare it. |
| `periodic.tasks.*.interval` | removed | Change the `interval` of the `@periodic` decorator. |
| `periodic.tasks.*.cron` | removed | Change the `cron` of the `@periodic` decorator. |
| `periodic.tasks.*.lease_seconds` | removed | Change the `lease_seconds` of the `@periodic` decorator. |
| `fleet.registry.provider` | `autodiscover.provider` | `static` and `none` are removed; the default `filesystem` is now `''` and the feature is off; set `autodiscover.enabled: true` |
| `fleet.registry.connection` | `autodiscover.connection` |  |
| `fleet.registry.heartbeat_seconds` | `autodiscover.heartbeat` | `10` is `10s` |
| `fleet.registry.refresh_seconds` | `autodiscover.refresh` | `15` is `15s` |
| `fleet.registry.offline_after_seconds` | `autodiscover.offline_after` | `60` is `1m` |
| `fleet.registry.prune_after_seconds` | `autodiscover.prune_after` | `86400` is `24h` |
| `fleet.registry.resilience.timeout_seconds` | `autodiscover.resilience.timeout` | `5.0` is `5s`; one block for the registry and the election |
| `fleet.registry.resilience.retries` | `autodiscover.resilience.max_retries` | ; one block for the registry and the election |
| `fleet.registry.resilience.backoff_seconds` | `autodiscover.resilience.backoff` | `0.2` is `200ms`; one block for the registry and the election |
| `fleet.registry.resilience.backoff_max_seconds` | `autodiscover.resilience.max_backoff` | `5.0` is `5s`; one block for the registry and the election |
| `fleet.registry.resilience.breaker.failure_threshold` | `autodiscover.resilience.breaker_failures` | ; one block for the registry and the election |
| `fleet.registry.resilience.breaker.cooldown_seconds` | `autodiscover.resilience.breaker_cooldown` | `30.0` is `30s`; one block for the registry and the election |
| `fleet.registry.filesystem.dir` | removed | The card folder is `<storage.shared_dir>/fleet/workers`. |
| `fleet.registry.static.peers[].name` | removed | The `static` provider is removed. Use `filesystem`, `postgres`, `redis` or `consul`. |
| `fleet.registry.static.peers[].cluster` | removed | The `static` provider is removed. |
| `fleet.registry.static.peers[].url` | removed | The `static` provider is removed. |
| `fleet.registry.consul.mode` | removed | The `consul` provider has one mode, `publish`. |
| `fleet.registry.consul.service_name` | removed | The catalog mode is removed. Delete the key. |
| `fleet.registry.plugin_options` | removed | Plugins are not supported. Delete the key. |
| `fleet.cache_exchange.enabled` | `cache.exchange.enabled` |  |
| `fleet.cache_exchange.interval_seconds` | `cache.exchange.interval` | `30.0` is `30s` |
| `fleet.cache_exchange.page_size` | `cache.exchange.page_size` |  |
| `fleet.cache_exchange.timeout_seconds` | `cache.exchange.timeout` | `5.0` is `5s` |
| `fleet.cache_exchange.cycle_deadline_seconds` | `cache.exchange.cycle_deadline` | `null` stays `null` |
| `fleet.cache_exchange.max_concurrent_peers` | `cache.exchange.max_concurrent_peers` |  |
| `connections.consul.*.address` | unchanged |  |
| `connections.consul.*.token` | unchanged |  |
| `connections.consul.*.datacenter` | unchanged |  |
| `connections.consul.*.key_prefix` | `connections.consul.*.state_prefix` |  |
| `connections.redis.*.url` | unchanged |  |
| `connections.redis.*.key_prefix` | `connections.redis.*.state_prefix` |  |
| `connections.postgres.*.dsn` | unchanged |  |
| `connections.postgres.*.schema_name` | `connections.postgres.*.schema` | the YAML key was already `schema` |
| `connections.postgres.*.manage_schema` | unchanged |  |
| `connections.postgres.*.pool_min` | unchanged |  |
| `connections.postgres.*.pool_max` | unchanged |  |
| `sources.kafka.enabled` | unchanged | now needs `connections.kafka.<connection>` |
| `sources.kafka.topic` | unchanged |  |
| `sources.kafka.consumer_group` | unchanged |  |
| `sources.kafka.max_poll_records` | unchanged |  |
| `sources.kafka.max_poll_interval_ms` | `sources.kafka.max_poll_interval` | `300000` is `5m` |
| `sources.kafka.session_timeout_ms` | `sources.kafka.session_timeout` | `45000` is `45s` |
| `sources.kafka.heartbeat_interval_ms` | `sources.kafka.heartbeat_interval` | `3000` is `3s` |
| `sources.kafka.on_parse_error` | unchanged |  |
| `sources.kafka.startup_align_enabled` | `sources.kafka.startup_align.enabled` |  |
| `sources.kafka.startup_min_wait_seconds` | `sources.kafka.startup_align.min_wait` | `4.0` is `4s` |
| `sources.kafka.startup_align_interval_seconds` | `sources.kafka.startup_align.interval` | `10` is `10s`; whole seconds |
| `sources.http.enabled` | unchanged |  |
| `sources.http.host` | unchanged |  |
| `sources.http.port` | unchanged |  |
| `sources.http.path` | unchanged |  |
| `sources.http.sinks_enabled` | `sources.http.deliver_to_sinks` |  |
| `sources.http.request_timeout_seconds` | `sources.http.request_timeout` | `30.0` is `30s` |
| `sources.http.max_concurrent` | unchanged |  |
| `sources.http.max_body_bytes` | `sources.http.max_body` | `10485760` is `10MiB` |
| `sources.http.clients[].name` | unchanged |  |
| `sources.http.clients[].token` | unchanged |  |
| `sources.http.clients[].rpm` | unchanged |  |
| `app` | unchanged | open map; unchanged |

Keys of release 2.4.0 that an earlier build of this release already replaced, and keys of
2.4.0 that the table above does not list:

| Old path | New path | How to write the value |
|---|---|---|
| `fleet.registry.timeout_seconds` | `autodiscover.resilience.timeout` | `5` is `5s`; the value now limits one attempt, and `max_retries: 2` makes up to three attempts. Set `max_retries: 0` for one attempt |
| `history.archive.lock_stale_seconds` | `recorder.archive.lease` and `recorder.archive.sweep_after` | the old value was the lease and the sweep age: set `sweep_after` to it and keep the default `lease` |
| `history.archive.pass_interval_seconds` | `recorder.archive.interval` or `recorder.archive.cron` | `0` (a pass after each rotation) has no counterpart: the pass runs at slots, `1h` by default |
| `history.archive.election` and every key below it | `autodiscover.*` | one provider for the registry and the election |
| `webapp.*` | `sources.http.*` | the `webapp:` section is the HTTP source |
| `DK_UI__RECORDER__DB_DIR` and the other keys of the [guide to 2.4.0](#to-2-4-0) | see that guide, then this table | |

#### The new keys {#new-keys}

These keys have no old key. The defaults work for most deployments.

| Path | Default | What it does |
|---|---|---|
| `worker.name` | `''` | A literal worker name. `worker.name_env` still wins when its variable is set and not empty. |
| `connections.kafka.*`, `connections.mongo.*` | none | The Kafka and Mongo services, declared once. |
| `sources.kafka.connection` | `main` | The Kafka cluster of the source. |
| `sinks.<type>.*.connection` | `main` | The service of a Kafka, Postgres, Mongo or Redis sink. |
| `sinks.defaults.on_failure`, `.max_retries` | `dlq`, `2` | The failure policy of every sink. A sink can override each key. |
| `sinks.*.*.delivery_timeout`, `.breaker_failures`, `.breaker_cooldown` | not given | Per-sink overrides of `sinks.defaults`. |
| `dlq.enabled`, `dlq.provider`, `dlq.connection` | `true`, `kafka`, `main` | A switch for the DLQ, the destination type (`kafka` only) and its cluster. |
| `recorder.enabled`, `recorder.provider` | `true`, `sqlite` | Durable recording on or off, and where it goes. |
| `recorder.postgres.max_output`, `recorder.postgres.max_size` | `64KiB`, `0` | Limits of the Postgres history: one output value, and the whole table. |
| `autodiscover.enabled` | `false` | Turns on the registry and the election. |
| `api.enabled` | `true` | Runs the HTTP server. |

### 5. What changes in behaviour {#behaviour}

The subsections cover one area each. Read the ones that apply to your deployment.

#### Config keys are strict {#strict-keys}

Before this release, a nested section dropped a key it did not know, and the
value stopped applying with no message. Now every unknown key fails the load, in
the YAML file and in a `DK_*` variable, and one message lists them all:

```
config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
  sinks.kafka.results.max_retires (did you mean 'max_retries'?)
  ui.recorder.db_dir
```

1. **Delete or correct each key in the message.** A key that was ignored
   before, for example `rotation_interval_minutes`, must go. This includes the
   keys that used to give a "moved" message and the top-level `webapp:` section,
   which is `sources.http`.
2. **Check variables for hyphenated names.** A variable now reaches the existing
   name: `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX` changes `sinks.postgres.main-db`.
   Before, it made a second instance `main_db`. If two names in the YAML match one
   variable (`main-db` and `main_db`), the load fails: rename one.
3. **Set `schema` of a Postgres connection by that key.** In Python code,
   `PostgresConnectionConfig(schema='audit')` is the only form; the attribute
   stays `schema_name`.
4. **Config models refuse unknown keywords** when you build one in code.
   `DrakkarConfig(**other.model_dump())` fails with a Postgres connection, because
   the dump keys it `schema_name`. Use `drakkar.configmeta.dump_by_yaml_key`.

#### Connections, sources, sinks and the DLQ keys {#services}

These are the details of [step 2](#migrate-config). Do them in order. An old key fails
the load, so the load error tells you what is left.

1. Move the root `kafka:` section to `connections.kafka.main`. Every key keeps its
   name. Every config with the Kafka source on needs this entry, even an empty one
   (`main: {}`), because the source and the DLQ name `main` by default.
2. Give each Kafka sink or DLQ that has its own `brokers` a connection of its own,
   and name it with `connection`. Move its `brokers` and `security` to that entry.
   A sink's or the DLQ's `client_config` stays where it is. It is now merged after
   the connection's, so a sink or DLQ with no `brokers` gains the properties of
   `connections.kafka.main` and wins on a key that both set.
3. Move each `dsn`, `uri` and `url` of a Postgres, Mongo or Redis sink to an entry
   under `connections.postgres`, `connections.mongo` or `connections.redis`, and
   name the entry with `connection` on the sink. Sinks that used one server can
   share one entry. A Postgres connection entry also holds `schema`, `manage_schema`
   and its own `pool_min` and `pool_max`, for the autodiscovery tables only. A
   sink's `pool_min` and `pool_max` stay on the sink.
4. Rename the other keys of these sections with the [table](#path-table), and write each
   duration or size with a unit.
5. Check the two new DLQ keys. `dlq.enabled` is on by default. With `dlq.enabled:
   false` a `dlq` outcome follows `dlq.on_send_failure`, so `stall` holds the offset.
   `dlq.provider` accepts only `kafka`.
6. If a handler changes `sinks.circuit_breaker` or `sinks.delivery_timeout_seconds`
   in `on_startup`, set `sinks.defaults` in the config file instead. Each sink's
   policy is resolved when the worker is constructed. A change of `sinks.defaults`
   or of a sink's `on_failure`, `max_retries`, `delivery_timeout`,
   `breaker_failures`, `breaker_cooldown` or `depends_on` in `on_startup` is logged
   as `on_startup_config_change_ignored` and does nothing. A sink added in
   `on_startup` still gets its policy from the config it was added to.
7. Check tools that read `worker_config.kafka_brokers`. It holds the brokers of the
   source's connection, and `''` on a worker with the Kafka source off.

In code, these names changed: `KafkaConfig` is `KafkaConnectionConfig`,
`CircuitBreakerConfig` is gone, and `KafkaSink`, `PostgresSink`, `MongoSink` and
`RedisSink` take the connection as their third argument.

#### The DLQ record and the failure policy {#dlq-record}

A DLQ record is now the failed source message itself: the original key and
value, byte for byte, its own headers, then `dk-` headers that say where and
why it failed (see [The DLQ record](sinks.md#the-dlq-record)). The JSON
document with `original_payloads` is gone. Every sink also declares what
happens when its delivery fails (see [Failure policy](sinks.md#failure-policy)).
Do these steps in order.

1. **Drain or archive the old DLQ topic first.** Records written by an older
   worker are JSON documents with no headers; records written by this release
   are the original messages with `dk-` headers. The new replay tool never
   republishes an old record: it skips every record without a
   `dk-record-version` header and counts it as `skipped_old_format`. Replay
   the old records with the tool of the older release before you upgrade,
   copy them away, or delete them; or move the new workers to a new
   `dlq.topic`. Old records left in the topic stay there until its retention
   removes them.
2. **Change every consumer of the DLQ topic.** A tool that parsed the JSON
   document must read the record instead: the value is the original message
   value (it may not be JSON or UTF-8), the failure facts are headers
   (`dk-stage`, `dk-error`, `dk-sink`, `dk-attempts`, `dk-source-topic`,
   `dk-source-partition`, `dk-source-offset`, ...). One record is one source
   message, not a list of payloads.
3. **Code that calls `read_dlq_entries`** now gets `DLQEntry(key, value,
   headers, partition, offset)` records, not decoded JSON objects.
   `DLQMessage` and `ParseFailurePayload` are removed; a parse failure is a
   record with `dk-stage: parse`.
4. **Use the new replay tool.** `scripts/replay_dlq.py` republishes the key,
   the value and the source headers of each record, and takes `--stage`. Its
   summary line has `skipped_old_format` instead of `skipped_invalid`, and its
   report lines name `dlq_partition=` and `dlq_offset=`.
5. **Check `sinks.http.<name>.max_retries`.** It is now the shared retry
   budget of every sink: the default is 2 (it was 3, and unused), and a value
   above 10 fails the load with
   `sinks.http.<name>.max_retries must be a whole number from 0 to 10`.
6. **Review your `on_delivery_error` hook.**
    - The hook is called after every failed attempt, with `attempt`,
      `retries_left` and `declared_policy`. `RETRY` is honoured up to the
      sink's `max_retries` (it was a fixed 3 attempts); then the declared
      `on_failure` applies.
    - The default hook returns the sink's declared `on_failure` (`dlq` unless
      you set `skip`), as before for a sink you did not configure.
    - A hook that raises no longer stops the delivery: the worker logs
      `delivery_error_hook_failed` and applies the declared `on_failure`. It
      no longer reaches `dlq.on_send_failure`. The HTTP source follows the
      same rule (it sent such a request to the DLQ before).
    - With the circuit open, the hook is not called even when the worker has
      no DLQ; the declared `on_failure` decides.
7. **Know what "the message is done" means.** After the first DLQ record of
   a message, no later result of that message is delivered, also to sinks that
   did not fail, and `on_message_complete` still runs but its result is not
   delivered. A sink that may lose its payloads without stopping the message
   should declare `on_failure: skip`.
8. **Window results cannot reach the DLQ.** A failed delivery of an
   `on_window_complete` result went to the DLQ before (as the payload JSON).
   A window result has no source message, so `on_failure: dlq` now acts as
   `skip` there and logs `window_delivery_not_dlq`. A handler that needs the
   DLQ for an aggregate delivers it from `on_message_complete` instead, or
   sets `on_failure: skip` on that sink knowingly.
9. **The HTTP response summary.** In the per-type `sinks` summary of an HTTP
    request, the error markers `dlq_unconfigured` and `dlq_send_failed: ...`
    are gone. A sink that did not start reports `<type>/<name>: not started`,
    and a sink that failed with no DLQ record reports
    `<type>/<name>: failed, no DLQ record`. `dlq` counts only when the
    request's DLQ record was written.
10. **Update dashboards and alerts.** `drakkar_dlq_dropped_payloads_total` now
    counts source messages, once each, not payloads. New:
    `drakkar_dlq_records_total{stage}` and
    `drakkar_sink_deliveries_not_started_total{sink_type,sink_name,reason}`.
    The help texts of `drakkar_sink_dlq_messages_total`,
    `drakkar_dlq_dropped_payloads_total`,
    `drakkar_dlq_unconfigured_drops_total` and
    `drakkar_sink_deliveries_skipped_total` changed. The log events
    `dlq_message_sent` and `dlq_send_failed` carry `stage`, `sink` and
    `value_bytes` instead of payload fields. `dlq_payload_serialization_fallback`
    is gone (nothing is serialized any more). The HTTP source's
    `webapp_on_delivery_error_failed` and `webapp_sink_dlq_send_failed` are
    gone: the shared `delivery_error_hook_failed`, `message_dlq`,
    `dlq_send_failed` and `dlq_failure_payloads_dropped` cover those cases.
11. **Protect the DLQ topic like the source topic.** It now holds the original
    messages; see [Security](security.md#the-dlq-topic-holds-original-messages).

#### The flight recorder {#recorder}

The recorder is a root section, `recorder:`, and it does not depend on the UI.
`ui.enabled` controls only the UI. Do these steps in order.

1. **Check workers with `ui.enabled: false`. They now record.** Such a worker used
   to have no recorder at all. It now writes recorder files into `storage.dir`
   (with the default `/tmp/drakkar` too), seals them and runs the archive task,
   with no memory store. To keep the worker as it was, set `recorder.enabled: false`.
2. **Replace `ui.recorder.persistence`.** There is no derive rule any more:

    | The old value | The new keys |
    |---|---|
    | unset, with `storage.dir` set | nothing: `recorder.enabled` defaults to `true`, `recorder.provider` to `sqlite` |
    | unset, with an empty `storage.dir` | `recorder.enabled: false`. This is new: with `sqlite` an empty `storage.dir` now fails the load |
    | `none` | `recorder.enabled: false` |
    | `sqlite` | `recorder.provider: sqlite` |
    | `postgres` | `recorder.provider: postgres` |

3. **Check a Trace search of a worker with the recorder off.** With `none`, a
   `trace?history=1` request used to read the sealed files of the history folder. With
   `recorder.enabled: false` it reads no folder and no table: it answers the memory rows.
4. **Rename the keys** with the [table](#path-table), and write each duration or size with a unit.
   A number with no unit fails the load, except `0`. The environment variables follow the YAML
   paths: `DK_UI__RECORDER__MEMORY__WINDOW_SECONDS` is `DK_RECORDER__MEMORY__WINDOW`.
5. **Check the Postgres default.** `initial_retention` defaults to `24h`; the old key
   defaulted to 6 hours. It only matters when a worker creates the table: an existing
   table keeps the value in its comment. `drakkar-schema` keeps its own default of 6
   hours when you give no `--retention-hours`.
6. **Rename the keys of every resilience block** (`recorder.postgres.resilience` and `autodiscover.resilience`) with the [table](#path-table). The `breaker` block is gone: `breaker_failures` and `breaker_cooldown` sit in the block itself.
7. **Update log queries.** `recorder_started` has no `ignored_keys` field. It has no
   memory fields on a worker with no memory store.
8. **Check a process that reads the Postgres table's comment or the archive messages.**
   The load errors of the archive now start with `recorder.archive: `.

The new layout of a worker with no UI that records into its folder:

```yaml
storage:
  dir: /var/lib/drakkar
  shared_dir: /mnt/fleet
ui:
  enabled: false
recorder:
  sqlite:
    rotation_interval: 1h
  archive:
    retention: 30d
```

In code, `EventRecorder` takes a `RecorderConfig` as its first argument, and the
constructor keywords `archive` and `merged_retention_days` are gone: both come from
`config.archive`. `UIConfig.recorder`, `UIRecorderConfig`, `UITraceConfig`,
`SharedConfig`, `HistoryConfig` and `BreakerConfig` are gone; `HistoryArchiveConfig` is
`RecorderArchiveConfig`.

#### The recorder memory store {#recorder-memory-store}

The recorder keeps its recent events in an in-memory database, and
`recorder.enabled` with `recorder.provider` say whether it also writes
recorder files or a Postgres table. See [Recent Events in Memory](recent-events.md).

- **A memory-only worker is queryable.** A worker with `storage.dir: ''` and
  `recorder.enabled: false` (the old `none`):  the
  History, Trace and Task Detail pages, the memory row of the Databases tab
  and the memory snapshot download read the memory store. They cover the
  memory window only (two minutes by default).
- **Provider `sqlite` needs a folder.** With the recorder on, provider `sqlite` and an empty `storage.dir` the
  config does not load:

    ```
    recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, or set recorder.enabled to false
    ```

- **Stop a worker cleanly before you turn its recorder off.** A worker with the recorder off
  seals nothing, so a recorder file that an earlier `sqlite` run left in
  `storage.dir` stays there and never reaches the history folder. A clean
  stop seals and moves the live file first.
- **Ids no longer start at 1 per file.** The recorder gives each row its id
  in the process. Ids continue across rotations and start again at 1 after a
  restart. A client that polls `/api/v1/events` with `after_id` now sees new
  rows right after a rotation.
- **Give every worker of a cluster the same recorder settings.** A worker with the recorder off
  does not run the archive, so its cluster task hash differs from the
  `sqlite` workers and they log `periodic_cluster_mismatch`.
- **Plan RAM.** With the default budget of 256 MiB, plan up to about 600 MiB
  per worker for the store. See [RAM to plan](recent-events.md#ram-to-plan).
- **`EventRecorder.reader_db` is gone.** Use `file_reader_db` for the live
  file, or `memory_store` for the memory store.

#### The recorder history in Postgres {#recorder-history-in-postgres}

`recorder.provider: postgres` makes every worker
write its events to one table in Postgres, and the trace with history read
that table. Nothing changes until you set it. See
[Recorder History in Postgres](recorder-postgres.md).

- **Switching a worker from `sqlite` to `postgres`.** Stop the worker
  cleanly first: a clean stop seals and moves its live file. A file that a
  worker left behind without a clean stop stays in `storage.dir`. The sealed
  files stay in the history folder; merge, file downloads and the archives
  list still show them, but `trace?history=1` on a `postgres` worker no
  longer searches them. There is no migration of old files into the table.
- **Switch a whole cluster at once.** A cluster that mixes `postgres` and
  `sqlite` workers shows a cluster task mismatch (the archive runs on the
  `sqlite` side only), and its history is split between the files and the
  table.
- **Declare the connection.** `recorder.postgres.connection` (default
  `main`) must name an entry under `connections.postgres`, or the config does
  not load.
- **The role must own the events table.** The worker creates and drops its
  partitions. With an operator-managed schema, add `--recorder` to the
  `drakkar-schema` command and give the table to the worker role.
- **The index set is fixed.** The table has three indexes and its primary
  key. A later `CREATE INDEX` on a filled partitioned table builds on every
  partition under a lock that blocks every writer. A later index change
  needs a new `table_prefix`, or a manual `CREATE INDEX CONCURRENTLY` on each
  partition.
- **Contract v1.26.** With `history=1`, a `postgres` worker answers the rows
  of its cluster from the table, or `503 {"detail": "history search
  failed"}`. A UI of this release shows "(the history search failed)"; an
  older UI shows the same note for a failed history search.

#### The archive lock and periodic tasks {#periodic-tasks-and-the-archive}

**The decorator.** `@periodic` takes `interval` instead of `seconds`. Write
`@periodic(30)` or `@periodic(interval=30)`; `@periodic(seconds=30)` now
raises `TypeError` when the handler module is imported. The decorator also
checks the method name: 1 to 64 characters of `a-z`, `0-9` and `_`, not
starting with a digit. A method named `refreshRates` must be renamed. A
worker-scope task with an interval runs as before. See
[Periodic Tasks](periodic-tasks.md) for `cron=` and `scope='cluster'`.

**The archive keys.** The archive pass does not run after each rotation: it runs at archive slots, at the top of each UTC hour by default. The keys are in the [table](#path-table): `recorder.archive.interval` or `cron`, `recorder.archive.lease` and `recorder.archive.sweep_after`.

The old `lock_stale_seconds` was both the lease and the sweep age. If you
set it, set `sweep_after` to the old value and leave `lease` at its default: the
lease now only says how long the lock of a dead archiver lives.

**Roll out with archiving off, or stop the fleet.** A 2.4.0 worker takes the
archive lock `history/<cluster>/.archive.lock`; a worker of this release
takes `tasks/<cluster>/history.archive.lock` (or the lease of another
election provider). The two cannot see each other's lock, so a worker of
each kind could archive one window at the same time. Do one of these:

- Stop every worker of the fleet, upgrade, and start them again.
- Or set `recorder.archive.enabled: false` (`DK_RECORDER__ARCHIVE__ENABLED=false`)
  on every worker for the rolling restart, and turn it back on once every
  worker runs this release. Due windows wait; nothing is lost, and the next
  archive slot after you turn it on picks them up.

A 2.4.0 card carries no definition hash, so the new workers count a 2.4.0
peer as unknown and log no warning about it. While archiving is off on every
worker, the new workers also hash the same definitions. When you turn
archiving back on one worker at a time, a worker with it on and a worker
with it off hash different definitions (`history.archive` is one of them),
so they log `periodic_cluster_mismatch` and
`drakkar_periodic_cluster_mismatch_peers` is above zero until the last worker
has archiving on again. That is expected.

#### Old lock files {#old-lock-files}

Nothing reads the old locks any more, and the worker does not delete them.
Once no 2.4.0 worker runs, delete:

| Old file or table | What it was |
|---|---|
| `<storage.shared_dir>/history/<cluster>/.archive.lock` | The archive lock of 2.4.0. |
| `<storage.shared_dir>/history/<cluster>/.archive.lock.stale.<token>` | A lock set aside by a takeover. The archive pass also deletes these once they are older than `recorder.archive.sweep_after`. |
| Postgres table `<schema>.archive_locks` | The archive lease of the `postgres` election provider in an older build. Drop it with `DROP TABLE IF EXISTS <schema>.archive_locks;`. The provider now creates `task_locks`. |

**Redis and Consul keys.** The `redis` election provider keeps its leases
under `<state_prefix>task-locks:<cluster>/<task>`, and the `consul` provider
under `<state_prefix>/task-locks/<cluster>/<task>`, with sessions named
`drakkar/task-lock/<cluster>/<task>/<token>`. A Redis user or a Consul
token limited to the key prefix needs no change (the key prefix is `state_prefix` now). A lock key of an older
build ends by itself.

#### The archive scope {#archive-scope}

The archive pass now has two scopes. See
[The two scopes](local-databases.md#archive-scopes).

- With `autodiscover.enabled` and `storage.shared_dir` set, a pass covers the whole
  cluster, as before. It runs with an election, and it writes
  `<cluster>-<from>__<to>.db.gz`.
- In every other case, a pass covers only this worker's files. It runs at the same slots
  with no election and no lease, and it writes `<cluster>-<worker>-<from>__<to>.db.gz`.
  `history.archive` is then not in the definition hash, and the worker counts nothing in
  `drakkar_periodic_cluster_slots_total` for it.

**What to do:**

1. Find workers that have `autodiscover` off, or that have no `storage.shared_dir`. Their
   archive now takes the worker's own sealed files in `<dir>/history/<cluster>/`, where
   `<dir>` is `storage.shared_dir`, else `storage.dir`. To keep the sealed files instead,
   set `recorder.archive.enabled: false`.
2. If a tool reads the archive folder, let it also accept the name with a worker. The name
   does not tell the cluster, because cluster names and worker names can both hold `-`.
   Take the cluster from the folder. The `cluster` field of the archive rows in
   `/api/v1/debug/databases` is now the folder name.
3. Delete a line such as `recorder: {archive: {enabled: false}}` that you added only because
   the workers have no shared folder. Each worker can now archive its own files.

**The own scope needs a stable worker name.** A pass finds its files by the worker name.
Sealed files and archives of a retired name stay until an operator deletes them. Set the
variable that `worker.name_env` names (default `WORKER_ID`) to a name that does not change
across restarts. A worker with no name gets a generated one that changes at each start, and
it logs `archive_own_scope_unstable_worker_name` at startup in this scope. Its sealed files
of earlier starts are never archived.

**Archives that stay in the folder after a change of scope:** a pass deletes only the
archives of its own scope. If a worker changes scope, delete its old archives by hand once
you no longer need them. Their sealed files are already gone.

**Log events and metrics:** `recorder_archive_pass_abandoned` has a second `detail` text for
the own scope, `the pass ends after the window in progress`. The help texts of
`drakkar_recorder_archive_bytes`, `drakkar_recorder_archive_files` and
`drakkar_recorder_archive_failures_total` changed.

**In code:** `archive_task_runs` is `archive_scope`, which answers `cluster`, `worker` or
`None`. `PeriodicRoster.runs_archive` is `PeriodicRoster.archive_scope`. `run_archive_pass`
takes `only_worker`, and needs `fence` or `only_worker`: with neither it raises `ValueError`.
`archive_file_name` takes `worker`.

#### Autodiscover {#autodiscover}

The worker registry and the election of cluster tasks are one optional
feature, `autodiscover`, with one provider. It is **off by default**. A config that
still uses the root sections `fleet` or `periodic` fails the load.

**What to do:**

1. If your workers use the workers list, the cache exchange or a cluster task,
   turn the feature on: `autodiscover.enabled: true` and `autodiscover.provider`.
   A worker with it off publishes no card, reads no peers, runs no election and
   pulls no cache rows, and the workers list shows only this worker.
2. Choose one provider. You can no longer run the registry on one provider and the
   election on another. If your two providers differed, move both to the one
   whose service every worker reaches.
3. With `filesystem`, set `storage.shared_dir`. It no longer falls back to
   `storage.dir`: `autodiscover.provider filesystem needs storage.shared_dir` fails
   the load. Set `storage.shared_dir` to the folder you used before, and the card
   files and locks stay where they are.
4. A handler with `@periodic(..., scope='cluster')` needs the feature on, or the
   worker does not start. Turn the feature on, or give the task `scope='worker'`.
5. An empty `cluster.name` with the feature on logs `cluster_name_empty` once at
   startup. It changes nothing else: the lock scope stays `default`.
6. Move the keys with the [table](#path-table). As environment variables, `DK_FLEET__REGISTRY__*` and
   `DK_PERIODIC__ELECTION__*` become `DK_AUTODISCOVER__*`, and `DK_FLEET__CACHE_EXCHANGE__*` becomes
   `DK_CACHE__EXCHANGE__*`.

**What stays the same:** the `fleet_workers` and `task_locks` tables, the Redis
keys, the Consul paths, the card, the lock body, and the metrics and their labels
`feature="registry"` and `feature="election"`. A cluster keeps its cards and leases
through this change when it keeps its provider and its folder or connection.

**Messages and events:**

- A provider name outside the four now fails the load, with or without `enabled`:
  `autodiscover.provider must be one of consul, filesystem, postgres, redis, got '<v>'`.
- The log events `periodic_cluster_check_off` and `fleet_registry_disabled_no_dir`
  are gone. Both states now stop the worker with a message.
- With `autodiscover` off, an enabled `cache.exchange` logs
  `cache_exchange_off_no_autodiscover` once, and the peer route answers as it does
  for a switched-off exchange.

#### The API server and the UI {#api-and-ui}

The HTTP server is its own section, `api`. The `ui` section holds only what
the UI needs on that server. Move the keys with the [table](#path-table).

The environment variables follow the paths: `DK_UI__PORT` is `DK_API__PORT`,
`DK_UI__PROBE_ENABLED` is `DK_UI__PROBE__ENABLED`, and so on.

**What a worker serves:**

| Route | Needs |
|---|---|
| `/healthz`, `/readyz`, `/api/v1/peer/cache/changes` | `api.enabled` |
| everything else: `/ws`, `/api/v1/*`, `/api-docs`, `/docs`, the pages | `api.enabled` and `ui.enabled` |

With `ui.enabled: false` the worker fetches no bundle and the routes of the second
row answer 404. To run with no listener at all, set `api.enabled: false` and
`ui.enabled: false`. The probes are then gone, and the worker logs
`ui_disabled_no_probes`.

**What can stop a config from loading:**

- `api.enabled: false` with the UI on:
  `ui.enabled needs api.enabled; set ui.enabled to false, or set api.enabled to true`.
- `api.enabled: false` with autodiscover on:
  `autodiscover.enabled needs api.enabled: peers reach a worker through its API`.

**What changed for a worker with the UI off:**

- A deployment that set `ui.enabled: false` so that no port opens must now also
  set `api.enabled: false`.
- The registry now runs on such a worker, so it publishes a card and appears in the
  workers list of its peers. The card's `features.ui` is `false`, and the UI leaves
  that worker out of its fan-out.
- The log event `fleet_registry_disabled_no_ui` is gone.

**In handler or test code:** `drakkar.uiserver.UIServer` is `APIServer`, and
`DrakkarApp.ui_url` is `api_url`. `SideEffectingEndpoint.config_key` of the probe is
`ui.probe.enabled`.

#### Names, executor, threads, cache and the rest {#remaining-sections}

The worker and cluster names are sections of their own, the thread pools are one
section, and every duration and size takes a unit. Change each old path with the
[table](#path-table). A value in seconds, milliseconds, minutes or bytes gets a unit: `120` becomes
`2m`, `500` (ms) becomes `500ms`, `1048576` (bytes) becomes `1MiB`.

The environment variables follow the paths: `DK_WORKER_NAME_ENV` is
`DK_WORKER__NAME_ENV`, `DK_CLUSTER_NAME` is `DK_CLUSTER__NAME`, `DK_IO__MAX_THREADS` is
`DK_THREADS__IO`, `DK_UI__LOG_MIN_DURATION_MS` is `DK_LOGGING__MIN_TASK_DURATION`, and so
on. An environment value takes the same text as the YAML value: `DK_EXECUTOR__TASK_TIMEOUT=5m`.

**What changed in the behaviour:**

- `worker.name` is new. The worker name is the value of the variable that
  `worker.name_env` names when that variable is set and not empty, else `worker.name`,
  else a generated `drakkar-<hex>`. A worker with `worker.name` set has a stable name,
  so the own archive scope does not log `archive_own_scope_unstable_worker_name` for it.
- `cluster.name_env` wins over `cluster.name` when its variable is set and not empty, as
  before.
- `threads.io: 0` and `threads.offload: 0` fail the load with
  `threads.io must be null or from 1 to 512` and `threads.offload must be null or from 1 to 32`.
  Write `null`, or leave the key out, to keep the derived size.
- `cache.max_memory_entries: 0` means no cap and logs
  `cache_max_memory_entries_unbounded` at load; `null` fails the load.
- `runtime_health.stack_probe_enabled` turns the stack probe on. A positive interval
  alone does nothing.
- `ui.timeline.max_age` takes whole minutes from `1m` to `24h`; `90s` fails with
  `must be a whole number of minutes, got '90s'`.
- The slow-task log line (`slow_task_completed`, `slow_task_failed`) follows
  `logging.min_task_duration`.

**The summary line** at startup has a new form:

```
[w-1/search] sources=[kafka:input-events/drakkar-workers/100poll] exec=4w/100win retries=3/120s api=8080 ui=on storage=/tmp/drakkar recorder=sqlite autodiscover=off cache=off metrics=9090 dlq=on sinks=[none] log=INFO
```

- `ui=on:<port>` is now two tokens: `api=<port>` (or `api=off`) and `ui=on` or `ui=off`.
- `recorder=` is always present: `sqlite`, `postgres` or `off`.
- `fleet=<provider>` and `election=<provider>` are one token, `autodiscover=<provider>`
  or `autodiscover=off`.
- The cache token shows `/max=<n>` only for a bounded memory cache.
- `dlq=` follows `dlq.enabled`: `off`, the configured topic, `on` for the topic
  derived from the Kafka source, or `unconfigured` when the DLQ is on but has no topic
  (an HTTP-only worker with no `dlq.topic`).

Update any log query or alert that parses the summary line.

**In handler or test code:** `drakkar.config.IOConfig` and `OffloadConfig` are one
`ThreadsConfig`, and `OffloadPool` takes the pool size (`threads.offload`, or `None`)
instead of a config model. `DrakkarConfig.fleet_summary` and `election_summary` are gone.
`WorkerConfig` and `ClusterConfig` hold the names.

#### Log events, metrics and smaller changes {#logs-and-metrics}

- **The offline limit.** `autodiscover.offline_after` defaults to `1m`; in 2.4.0 the
  limit defaulted to `30` seconds. A value you set explicitly keeps its meaning, but it must
  be at least `10s` and at least twice `heartbeat`. A worker with the old default and a worker with the new one call
  the same peer offline at different times, so set the value on every worker.
- **Provider calls follow one set of rules:** a time limit per attempt, retries with doubling
  waits, and a circuit breaker (`autodiscover.resilience`). An operation makes up to three
  attempts by default; set `max_retries: 0` for one.
- **Log events that were replaced:**

    | Old log event | Now |
    |---|---|
    | `fleet_registry_publish_failed`, `fleet_registry_list_failed`, `fleet_registry_withdraw_failed` | `provider_call_failed` with `feature="registry"` and `op` set to `publish`, `list` or `withdraw`. At most once a minute per operation; `suppressed` says how many failures it stands for. |
    | `fleet_registry_start_failed` | `provider_call_failed` with `op="start"`. The worker does not replace a provider that fails to start: it tries the start again at the next heartbeat. |
    | `recorder_archive_lock_taken_over` | `periodic_lock_taken_over`, in category `periodic`. Fields `provider`, `scope` (such as `main/history.archive`), `previous_worker`, `previous_instance`, `age_seconds`. |
    | `recorder_archive_lock_put_back_failed` | `periodic_lock_put_back_failed`, in category `periodic`. Fields `scope`, `folder`, `error`. |
    | `fleet_registry_disabled_no_ui`, `fleet_registry_disabled_no_dir`, `periodic_cluster_check_off` | Gone. The states they described now stop the worker with a message, or are the normal state of a worker with autodiscover off. |

  `fleet_registry_stop_failed` means that the registry's own stop raised; a provider whose
  `stop` fails shows as `provider_call_failed` with `op="stop"`. `recorder_started` has no
  `ignored_keys` field. `recorder_archive_fenced` carries `provider` and `reason`
  (`taken_over` or `unconfirmed`). `ui_disabled_no_probes` follows `api.enabled`.
- **Metrics.** `drakkar_fleet_registry_errors_total` counts operations that failed after their
  retries (one count per operation) and loop iterations that raised; its `op` label has the
  values `start` and `stop` too. `drakkar_provider_calls_total`,
  `drakkar_provider_call_duration_seconds` and `drakkar_provider_breaker_open` carry the
  label `feature` (`registry`, `election` or `recorder`). `drakkar_task_speed`,
  `drakkar_throughput` and `drakkar_task_rate` do not exist: delete dashboards and alerts on them.
  The DLQ metrics changed with the DLQ record; see [that section](#dlq-record).
  New: `drakkar_recorder_postgres_partitions_dropped_total{reason}`.
- **Config errors do not print the input of the failed section.** A load error names the key
  and the rule, but it adds no cut of the section, so a wrong `connections` block cannot
  print a connection string.
- **The lock body has a `provider` key,** `"provider":"filesystem"` for the `filesystem`
  provider. The registry card has `features.ui` (a boolean) and `features.periodic`.
- **Task events** have no `cost` and `speed` keys in `task_completed` metadata and
  `recent-tasks` rows. There is no `throughput` frame on `/ws` and no
  `worker_state.throughput` column.
- **The `redis` package must be 7.2 or later.** The `redis` providers and the Redis sink use it.
  Installing `py-drakkar` brings a matching version; check it only when you pin `redis`
  yourself.
- **Contract v1.28.** `GET /api/v1/identity` has `capabilities`, and the `Worker` rows of
  `GET /api/v1/workers` have `ui_enabled`.

### 6. Handler and code changes {#code-changes}

A handler and the code around it change in these places. Python names; the config
changes are in [the table](#path-table).

1. **Plugins are not supported.** The entry-point groups `drakkar.sinks`,
   `drakkar.registry_providers` and `drakkar.election_providers` are not read.
   `sinks.custom`, `fleet.registry.plugin_options` and `periodic.election.plugin_options` are
   not config keys, and `CustomPayload`, `CollectResult.custom`, `drakkar.sinks.registry`
   and `resolve_custom_sink` do not exist. A handler that returns `CustomPayload` must
   write the data through one of the built-in sinks (`kafka`, `postgres`, `mongo`, `http`,
   `redis`, `filesystem`) instead: pick the payload class of the sink that fits (for example
   `PostgresPayload(op='statement', ...)` with a named statement, or `HttpPayload`), and move the
   sink's settings into `sinks.<type>.<name>`. The providers are `filesystem`, `postgres`, `redis`
   and `consul`.
2. **`periodic.tasks` is removed.** Delete the section and any `DK_PERIODIC__TASKS__*` variable. To change
   the schedule or the lease of a task, change the arguments of its `@periodic` decorator:
   `interval`, `cron` and `lease_seconds` (these keep their numbers). To stop a task, do not declare it.
   `@periodic` takes `interval` instead of `seconds`: `@periodic(30)` or
   `@periodic(interval=30)` works, `@periodic(seconds=30)` raises `TypeError` when the handler
   module is imported. A method name must be 1 to 64 characters of `a-z`, `0-9` and `_`, not
   starting with a digit. A task with `scope='cluster'` needs `autodiscover`; without it the worker
   does not start (`periodic task '<name>' has scope cluster, but autodiscover is off; ...`).
3. **The throughput feature is removed.** Delete `throughput.cost_label` and `throughput.min_cost`,
   and every reader of `cost`, `speed` and the `throughput` frame. The `cost_label` role of the
   timeline labels does not exist.
4. **`on_delivery_error` and `on_error`.** The hook is called after every failed attempt, with
   `attempt`, `retries_left` and `declared_policy`; see [the DLQ record](#dlq-record) for the rules.
   `ErrorAction.DLQ` writes the execute record at once; `executor.on_failure` applies when an
   `on_error` hook spends `executor.max_retries` with `RETRY` or raises.
5. **Config classes.** `KafkaConfig` is `KafkaConnectionConfig`, `CircuitBreakerConfig` and
   `BreakerConfig` are gone, `IOConfig` and `OffloadConfig` are `ThreadsConfig`, `HistoryArchiveConfig` is
   `RecorderArchiveConfig`, and `UIRecorderConfig`, `UITraceConfig`, `SharedConfig` and `HistoryConfig` are
   gone (`RecorderConfig` and its blocks replace them). `PostgresConnectionConfig(schema='audit')` is
   the only form of its schema key (the attribute stays `schema_name`). Models refuse unknown
   keywords, so `DrakkarConfig(**other.model_dump())` fails with a Postgres connection; use
   `drakkar.configmeta.dump_by_yaml_key`. A config object holds seconds and bytes as numbers, and
   a unit text is accepted at construction (`task_timeout='5m'`).
6. **Sinks.** `KafkaSink`, `PostgresSink`, `MongoSink` and `RedisSink` take the connection as their third
   argument. `DrakkarApp.ui_url` is `api_url`, and `drakkar.uiserver.UIServer` is `APIServer`.
   `SideEffectingEndpoint.config_key` of the probe is `ui.probe.enabled`.
7. **Recorder.** `EventRecorder` takes a `RecorderConfig` as its first argument; the constructor
   keywords `archive` and `merged_retention_days` are gone, both come from `config.archive`.
   `EventRecorder.reader_db` is gone: use `file_reader_db` for the live file or `memory_store` for the
   memory store. `archive_task_runs` is `archive_scope`, and `PeriodicRoster.runs_archive` is
   `PeriodicRoster.archive_scope`. `run_archive_pass` takes `only_worker` and needs a `fence` or
   `only_worker`. `archive_file_name` takes `worker`. `OffloadPool` takes the pool size
   (`threads.offload`, or `None`).
8. **DLQ code.** `read_dlq_entries` yields `DLQEntry(key, value, headers, partition, offset)`.
   `DLQMessage` and `ParseFailurePayload` are removed; a parse failure is a record with
   `dk-stage: parse`. The summary of the replay tool has `skipped_old_format` instead of
   `skipped_invalid`.
9. **Handler startup.** A change of `sinks.defaults`, or of a sink's `on_failure`, `max_retries`,
   `delivery_timeout`, `breaker_failures`, `breaker_cooldown` or `depends_on`, in `on_startup`
   is logged as `on_startup_config_change_ignored` and does nothing; set them in the
   config file. A change of `sources.kafka.enabled`, `sources.http.enabled` or `storage.dir`
   in `on_startup` is ignored the same way.

### 7. Check the result {#check}

1. Start one worker. If a key is still old, the load fails with one message that lists every
   unknown key. If a consumer names a connection that is not declared, or a feature lacks what it
   needs, the message names the key to change ([the rules](#rules)). Repeat until it starts.
2. Read the startup line `drakkar_starting`. Its summary shows the new form:

    ```
    [w-1/search] sources=[kafka:input-events/drakkar-workers/100poll] exec=4w/100win retries=3/120s api=8080 ui=on storage=/var/lib/drakkar shared=/mnt/fleet recorder=sqlite autodiscover=filesystem cache=off metrics=9090 dlq=on sinks=[none] log=INFO
    ```

    `api=<port|off>` and `ui=<on|off>` are two tokens; `recorder=` is always present
    (`sqlite`, `postgres` or `off`); `autodiscover=<provider|off>` replaces `fleet=` and
    `election=`; `dlq=` is `off`, the topic, `on` for the derived topic, or `unconfigured`
    when the DLQ is on with no topic. Update any log query that parses the line.
3. Open `/api/v1/identity` of a worker with the UI on: `capabilities` shows the recorder
   provider, the memory window, autodiscover and the cache exchange as the worker runs them.
4. Start the other workers. With autodiscover on, the workers list shows every peer. With the
   cache on, `drakkar_fleet_peer_requests_total{outcome="ok"}` grows once a peer has new
   `cluster` or `global` rows.
5. Wait for one rotation (`recorder.sqlite.rotation_interval`, default `1h`), or stop one
   worker cleanly. `<storage.shared_dir>/history/<cluster>/` (`<storage.dir>/history/<cluster>/`
   with no shared folder) then holds a sealed recorder file and its meta file. If it stays
   empty, check `drakkar_recorder_unsealed_files` and `recorder_seal_failed`.

### 8. Files and tables to delete {#old-files}

The worker does not delete files of earlier releases. Once no worker of the old release runs,
delete:

| Old file or table | What it was |
|---|---|
| `<shared.dir>/history/<cluster>/.archive.lock` and `.archive.lock.stale.<token>` | The archive lock of 2.4.0. |
| Postgres table `<schema>.archive_locks` | The archive lease of an older election provider. `DROP TABLE IF EXISTS <schema>.archive_locks;`. The provider creates `task_locks`. |
| `<storage.dir>/<worker>-<timestamp>.db` of a worker whose recorder is now off | Left by an earlier `sqlite` run that did not stop cleanly; stop a worker cleanly before you turn its recorder off. |
| Sealed files and archives of a retired worker name, in the own archive scope | The own scope matches files by the worker name; see [the archive scope](#archive-scope). |

## To 2.4.0 from an older release {#to-2-4-0}

Release 2.4.0 moved the settings for files, archives, worker discovery and cache
exchange into four sections: `storage`, `shared`, `history` and `fleet`. Some old keys
moved, a few were removed. The old keys are not read any more: a config that still uses
one **fails to load** with a message that lists every old key it found. This guide ends at
the 2.4.0 layout. Then do [the guide above](#from-2-4-0): its
[table](#path-table) lists the 2.4.0 keys (`shared.dir`, `history.archive.*`,
`fleet.*`) with their final places.

Follow steps 1 to 8 in order. Plan the move to 2.4.0 **with all workers
stopped**: a worker of 2.4.0 does not read the files an older release
wrote, and an older and a newer worker cannot run side by side.

### Step 1. List the keys you use today

Search your YAML files **and** your environment (Kubernetes manifests,
`.env` files, CI variables) for these keys. As environment variables they
are `DK_` plus the YAML path with `__` between the levels.

| YAML path | Environment variable |
|---|---|
| `ui.recorder.db_dir` | `DK_UI__RECORDER__DB_DIR` |
| `ui.recorder.archive_enabled` | `DK_UI__RECORDER__ARCHIVE_ENABLED` |
| `ui.recorder.archive_window_hours` | `DK_UI__RECORDER__ARCHIVE_WINDOW_HOURS` |
| `ui.recorder.archive_retention_days` | `DK_UI__RECORDER__ARCHIVE_RETENTION_DAYS` |
| `ui.recorder.dbstats_warm_interval_seconds` | `DK_UI__RECORDER__DBSTATS_WARM_INTERVAL_SECONDS` |
| `ui.recorder.dbstats_inline_scan_limit` | `DK_UI__RECORDER__DBSTATS_INLINE_SCAN_LIMIT` |
| `ui.workers_offline_after_seconds` | `DK_UI__WORKERS_OFFLINE_AFTER_SECONDS` |
| `cache.db_dir` | `DK_CACHE__DB_DIR` |
| `cache.peer_sync.enabled` | `DK_CACHE__PEER_SYNC__ENABLED` |
| `cache.peer_sync.interval_seconds` | `DK_CACHE__PEER_SYNC__INTERVAL_SECONDS` |
| `cache.peer_sync.batch_size` | `DK_CACHE__PEER_SYNC__BATCH_SIZE` |
| `cache.peer_sync.timeout_seconds` | `DK_CACHE__PEER_SYNC__TIMEOUT_SECONDS` |
| `cache.peer_sync.cycle_deadline_seconds` | `DK_CACHE__PEER_SYNC__CYCLE_DEADLINE_SECONDS` |

### Step 2. Choose a layout

- **One host.** All workers run on one machine (or you run one worker).
  Set only `storage.dir`. Workers of one host can share it: each file name
  carries the worker name.
- **A fleet across hosts.** Give every worker its own `storage.dir` on a
  local disk (for example a per-pod volume), and mount one shared folder
  on every worker as `shared.dir` (for example a `ReadWriteMany` volume).
  Each worker writes its live files only to `storage.dir`. The shared
  folder holds the worker registry, the history, the archives and the merge
  results. A finished recorder file is sealed and moved from `storage.dir`
  into `<shared.dir>/history/<cluster>/` in the background.

`shared.dir` empty means "the same folder as `storage.dir`".

The cache file is no longer shared. Each worker keeps
`<storage.dir>/<worker>-cache.db` to itself, and workers pull each other's
`cluster` and `global` cache rows over HTTP, from the URL on each
[registry card](fleet.md). So `storage.dir` does not need to be shared for
the cache any more, but every worker must reach the UI port of its peers.

### Step 3. Rename the moved keys

| Old YAML path | New YAML path | Old environment variable | New environment variable |
|---|---|---|---|
| `ui.recorder.db_dir` | `storage.dir` (and `shared.dir` for a fleet across hosts) | `DK_UI__RECORDER__DB_DIR` | `DK_STORAGE__DIR` (`DK_SHARED__DIR`) |
| `ui.recorder.archive_enabled` | `history.archive.enabled` | `DK_UI__RECORDER__ARCHIVE_ENABLED` | `DK_HISTORY__ARCHIVE__ENABLED` |
| `ui.recorder.archive_window_hours` | `history.archive.window_hours` | `DK_UI__RECORDER__ARCHIVE_WINDOW_HOURS` | `DK_HISTORY__ARCHIVE__WINDOW_HOURS` |
| `ui.recorder.archive_retention_days` | `history.archive.retention_days` | `DK_UI__RECORDER__ARCHIVE_RETENTION_DAYS` | `DK_HISTORY__ARCHIVE__RETENTION_DAYS` |
| `ui.workers_offline_after_seconds` | `fleet.registry.offline_after_seconds` | `DK_UI__WORKERS_OFFLINE_AFTER_SECONDS` | `DK_FLEET__REGISTRY__OFFLINE_AFTER_SECONDS` |
| `cache.peer_sync.enabled` | `fleet.cache_exchange.enabled` | `DK_CACHE__PEER_SYNC__ENABLED` | `DK_FLEET__CACHE_EXCHANGE__ENABLED` |
| `cache.peer_sync.interval_seconds` | `fleet.cache_exchange.interval_seconds` | `DK_CACHE__PEER_SYNC__INTERVAL_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS` |
| `cache.peer_sync.batch_size` | `fleet.cache_exchange.page_size` | `DK_CACHE__PEER_SYNC__BATCH_SIZE` | `DK_FLEET__CACHE_EXCHANGE__PAGE_SIZE` |
| `cache.peer_sync.timeout_seconds` | `fleet.cache_exchange.timeout_seconds` | `DK_CACHE__PEER_SYNC__TIMEOUT_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__TIMEOUT_SECONDS` |
| `cache.peer_sync.cycle_deadline_seconds` | `fleet.cache_exchange.cycle_deadline_seconds` | `DK_CACHE__PEER_SYNC__CYCLE_DEADLINE_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__CYCLE_DEADLINE_SECONDS` |

`fleet.registry.offline_after_seconds` has a new default in this release:
60 seconds, not 30. See [the guide above](#from-2-4-0).

Two rules changed together with the keys:

- `history.archive.retention_days` must now cover the due delay plus one
  window: `retention_days * 24 >= due_after_hours + window_hours` (or be
  `0`). The default 30 days covers the defaults (48 + 24 hours).
- `page_size` accepts 1 to 1000.

### Step 4. Delete the removed keys

| Removed key | Why |
|---|---|
| `cache.db_dir` | The cache file lives in `storage.dir`. |
| `ui.recorder.dbstats_warm_interval_seconds` | Database statistics are no longer cached in a shared file. |
| `ui.recorder.dbstats_inline_scan_limit` | Same as above. |

### Step 5. Review the new keys

These keys are new. Their defaults work for most deployments.

| Key | Default | What it does |
|---|---|---|
| `storage.dir` | `/tmp/drakkar` | This worker's own files. The worker creates the folder with mode `0700` when it is missing. `''` = memory-only (recorder persistence `none`). |
| `shared.dir` | `''` | The folder all workers see. `''` = `storage.dir`. |
| `history.archive.due_after_hours` | `48` | A window is archived this many hours after it ends. |
| `history.archive.lock_stale_seconds` | `7200` | An archive lock or work file older than this belongs to a dead archiver. The guide above replaces it with `recorder.archive.lease` and `recorder.archive.sweep_after`. |
| `history.merged_retention_days` | `32` | UI merge results older than this are deleted. `0` = never. |
| `fleet.registry.provider` | `filesystem` | How workers find each other: `filesystem`, `postgres`, `redis` or `consul`. Needs `ui.enabled`. |
| `fleet.registry.heartbeat_seconds` | `10` | How often a worker publishes its card. |
| `fleet.registry.refresh_seconds` | `15` | How often a worker reads the other cards. |
| `fleet.registry.prune_after_seconds` | `86400` | Cards older than this are deleted. |
| `fleet.registry.resilience.timeout_seconds` | `5` | Time limit for one attempt of a registry operation. The `resilience` block also holds retries and a circuit breaker; see [Fleet registry](fleet.md#resilience). |
| `fleet.cache_exchange.max_concurrent_peers` | `4` | Peers pulled from in parallel. |
| `ui.trace.history_max_files` | `256` | Most history files one trace request reads. |
| `ui.trace.history_max_seconds` | `10` | Time limit for one trace request over history. |

See [Configuration](configuration.md) for every field, and
[Fleet registry](fleet.md) for how workers find each other.

Two existing keys changed what they cover:

- **`ui.allowed_ws_origins`** also decides the cross-origin reads that the
  UI sends to every worker of the cluster (trace, label trace, databases
  list). **With `ui.auth_token` set, list the origin of every worker
  here**, for example `["http://10.0.3.17:8080", "http://10.0.3.18:8080"]`.
  Otherwise the UI of one worker cannot read the others and lists them as
  unreachable. See [Cross-origin reads](security.md#cross-origin-reads).
- **`ui.auth_token`** does not protect the new routes under
  `/api/v1/peer/`: workers call them on each other without a token. Anyone
  who can reach the UI port can read the `cluster` and `global` cache rows
  of a worker that serves the cache exchange. See
  [The public peer route](security.md#the-public-peer-route).

### Step 6. Kubernetes example

Before:

```yaml
env:
  - name: DK_UI__RECORDER__DB_DIR
    value: /var/lib/drakkar
  - name: DK_CACHE__PEER_SYNC__INTERVAL_SECONDS
    value: "30"
volumeMounts:
  - name: recorder
    mountPath: /var/lib/drakkar
```

After, for a fleet across hosts (a per-pod volume for own files, one
`ReadWriteMany` volume for the fleet):

```yaml
env:
  - name: DK_STORAGE__DIR
    value: /var/lib/drakkar
  - name: DK_SHARED__DIR
    value: /mnt/fleet
  - name: DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS
    value: "30"
volumeMounts:
  - name: own-files          # emptyDir or a per-pod PVC
    mountPath: /var/lib/drakkar
  - name: fleet              # ReadWriteMany PVC, the same on every pod
    mountPath: /mnt/fleet
```

### Step 7. Check the result

1. Start one worker. If a key is still old, the load fails with one
   message that lists every unknown key, for example:

    ```
    config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
      cache.db_dir
      ui.recorder.db_dir
    ```

2. Read the startup line `drakkar_starting`. Its summary shows
   `storage=<folder>`, `shared=<folder>` when the shared folder differs,
   and `fleet=<provider>` (`fleet=off` when the UI is off or no folder
   resolves). When the election provider (`periodic.election.provider`) is
   not `filesystem`, it also shows `election=<provider>` after `fleet=`.
3. Start the other workers. The workers list in the UI shows every peer.
   With the cache on, `drakkar_fleet_peer_requests_total{outcome="ok"}`
   grows on each worker once a peer has new `cluster` or `global` rows.
   Other outcomes mean that a peer could not be reached at the URL on its
   card, or answered with an error; the log event
   `fleet_peer_request_failed` names the peer and the URL.
4. Wait for one rotation (`ui.recorder.rotation_interval_hours`, default 1
   hour), or stop one worker cleanly. Then look in
   `<shared.dir>/history/<cluster>/` (`<storage.dir>/history/<cluster>/`
   when `shared.dir` is empty). It holds a sealed recorder file
   `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` and its meta file
   `<worker>-<YYYY-MM-DD__HH_MM_SS>.db.meta.json`. `<cluster>` is the
   cluster name with every character outside `[a-zA-Z0-9_-]` replaced by
   `_`, or `default` when the cluster name is empty. If the folder stays
   empty, check `drakkar_recorder_unsealed_files` and the log event
   `recorder_seal_failed`. See
   [Sealing](local-databases.md#sealing-and-the-history-folder).

### Step 8. Old files

Upgrade with all workers stopped. The new release does not read the files
an older release wrote and does not migrate them: after the upgrade the
history folders start empty. Delete the old folder, or point `storage.dir`
and `shared.dir` at empty folders. A rolling upgrade from the older release
is not supported.

The worker does not delete the old files. Delete them yourself:

| Old file | What it was |
|---|---|
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` (and its `-wal`, `-shm`) | Rotated recorder files. |
| `<worker>-live.db` | Link to the live recorder file. Workers no longer open each other's live files. |
| `<worker>-cache.db` (a link) and `<worker>-cache.db.actual` (and its `-wal`, `-shm`) | The old cache link and cache file. The cache file is now a plain `<worker>-cache.db`, private to its worker. Delete both **before** the first start: the worker does not remove them, and an old link with that name is followed to the old `.actual` file. |
| `.dbstats.db` | Statistics cache of the Databases page. Sealed files now carry a meta file. |
| `.archive-<cluster>.lock` | The old archive lock. Release 2.4.0 kept the lock in `history/<cluster>/.archive.lock`, and this release keeps it in `tasks/<cluster>/history.archive.lock`; see [Old lock files](#old-lock-files). |
| `<cluster>-<from>__<to>.db.gz` in `storage.dir` | Old archives. The archive pass now works only inside `<shared.dir>/history/<cluster>/`. |
| `merged-*.db` in `storage.dir` | Old UI merge results. New ones go to `<shared.dir>/merged/`. |

!!! note "Old recorder files with the worker's own name"
    An old rotated file named `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` matches
    the [leftover](local-databases.md#leftovers) rule for that worker name.
    If it is still in `storage.dir` when that worker starts, the worker
    seals it and moves it into the history folder like a file of its own.
    Delete the old files before the first start if you do not want them in
    the history.

