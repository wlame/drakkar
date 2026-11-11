# Configuration

Drakkar configuration is built on [pydantic-settings](https://docs.pydantic-dev.github.io/pydantic-settings/) and supports YAML files, environment variable overrides, and inline Python objects. Every option has a sensible default so you can start with a minimal config and grow from there.

!!! tip "Looking for an annotated example?"
    The companion **[Config Reference](config-reference.md)** page is a single `drakkar.yaml` showing **every** field with a one-line comment and its `DK_*` env-var override. Use it to scan what's available; come back here for the deep tables and prose.

---

## The rules

One set of rules covers the whole YAML tree. Learn them once and every section reads the same way.

1. **Unknown keys fail the load.** A key the config does not declare is an error, in the YAML file and in a `DK_` variable. See [Unknown keys](#unknown-keys).
2. **The top level answers operator questions**, in this order: `worker`, `cluster`, `connections`, `sources`, `executor`, `threads`, `sinks`, `dlq`, `storage`, `recorder`, `cache`, `autodiscover`, `metrics`, `logging`, `runtime_health`, `api`, `ui`, `agent`, `app`.
3. **A service is declared once.** `connections.<kind>.<name>` holds how to reach a service and how to log in to it. A consumer (a source, a sink, the DLQ, the recorder, autodiscover) says `connection: <name>`; the default name is `main`. The entry must be declared, or the load fails with `<path>.connection '<name>' is not defined under connections.<kind>`. What the consumer does with the service (a topic, a table prefix, a statement) stays in the consumer. A different login to the same server is a different connection. Each sink owns its client or pool, so several sinks can share one connection and keep their own topic or statements. A connection that no consumer uses is not checked for use.
4. **Choice plus block.** A key that selects an implementation (`provider`) sits next to one block per implementation that has settings of its own: `recorder.sqlite` and `recorder.postgres`. A provider with no settings has no block. The block of the provider that is not selected loads and does nothing.
5. **Switches.** A feature with settings is a block with `enabled`. A switch with no other setting is a flat `<feature>_enabled` (`ui.merge_enabled`). A boolean that reads as a question keeps its name (`manage_schema`).
6. **Limits.** `0` means no limit. `null` means only "not given: derive it". A limit is named `max_<thing>`, never `<thing>_max`. A pair keeps `pool_min` and `pool_max`.
7. **Units are in the value, not in the name.** A duration is written `30s`, `2h45m`, `500ms`; a size is written `64KiB`, `256MB`. See [Durations and sizes](#durations-and-sizes).
8. **One failure vocabulary.** `timeout`, `max_retries` (retries after the first attempt), `backoff`, `max_backoff`, `breaker_failures`, `breaker_cooldown`. A sink's budget is `delivery_timeout`, because the HTTP sink also has `request_timeout`. A failure event is `on_<event>: <action>`.
9. **Shallow.** At most `section.block.key`, not counting the names you choose and the Kafka `security` block.

### Every feature is optional

A worker can run its main loop only: no API, no UI, no recorder, no autodiscovery. Each feature below states what it needs. A missing need fails the load with a message that names the key to change.

| Feature | Key | Needs |
|---|---|---|
| API server (`/healthz`, `/readyz`, peer routes) | `api.enabled` | nothing |
| Operator UI and its data routes | `ui.enabled` | `api.enabled` |
| Agent routes (`/api/v1/agent`) | `agent.enabled` | `api.enabled` |
| Kafka source | `sources.kafka.enabled` | `connections.kafka.<sources.kafka.connection>` |
| HTTP source | `sources.http.enabled` | nothing (at least one source must be on) |
| Kafka, Postgres, Mongo, Redis sinks | `sinks.<type>.<name>` | `connections.<type>.<connection>` |
| DLQ | `dlq.enabled` | `connections.kafka.<dlq.connection>`, and a topic (own, or derived from the Kafka source) |
| Recorder, memory store | `recorder.enabled` and `ui.enabled` | see [Flight recorder](#flight-recorder-recorder) |
| Recorder, durable store, provider `sqlite` | `recorder.enabled` | `storage.dir` |
| Recorder, durable store, provider `postgres` | `recorder.enabled` | `connections.postgres.<recorder.postgres.connection>` |
| Archive of sealed recorder files | `recorder.archive.enabled` | provider `sqlite`, `storage.dir` |
| Autodiscover (registry and election) | `autodiscover.enabled` | `autodiscover.provider`, `api.enabled`; provider `filesystem` needs `storage.shared_dir`; `consul`, `postgres` and `redis` need `connections.<provider>.<autodiscover.connection>` |
| Cluster-scope periodic tasks | declared in code with `scope='cluster'` | `autodiscover` |
| Cache | `cache.enabled` | `storage.dir` for persistence |
| Cache exchange | `cache.exchange.enabled` | `cache.enabled`, `storage.dir`, `autodiscover` |
| Cluster-wide archive | `recorder.archive.enabled` | `autodiscover` and `storage.shared_dir` (otherwise the archive takes this worker's files only) |

### Durations and sizes

Durations and sizes are written as text with a unit. A bare number is refused, except `0`.

| Kind | Form | Examples |
|---|---|---|
| Duration | `0`, or terms `<number><unit>` with no spaces, units in descending order `d`, `h`, `m`, `s`, `ms`, each at most once. The number is ASCII digits with an optional fraction. The value must be whole milliseconds. | `30s`, `2h45m`, `1.5h`, `500ms`, `30d` |
| Size | `0`, or one `<number><unit>` with unit `B`, `KB`, `MB`, `GB`, `TB` (powers of 1000) or `KiB`, `MiB`, `GiB`, `TiB` (powers of 1024). Case-sensitive. The value must be whole bytes. | `64KiB`, `256MB`, `10MiB` |

An environment variable takes the same text: `DK_EXECUTOR__TASK_TIMEOUT=5m`. Some keys take whole seconds, hours or days only (`1.5h` is not a whole number of seconds for `recorder.flush_interval`); the tables below say so. The messages:

```
<path> needs a unit, for example 30s; got '30'
<path> is not a duration, got '5 min'; write for example 30s or 2h45m
<path> needs a unit, for example 64KiB; got '65536'
<path> is not a size, got '64kb'; write for example 64KiB or 256MB
<path> must be a whole number of seconds, got '1500ms'
```

(`hours`, `minutes` and `days` replace `seconds` for the keys that count in them.) A value outside the range of its key prints the range and the value in the same unit form:

```
<path> must be from 16MiB to 512MiB, got 600MiB
<path> must be at least 1s, got 0
<path> must be at most 1d, got 2d
```

The config summary, the `worker_config` table and the reference pages show a value in its canonical form: a duration in descending terms with no zero term (`2h45m`, `500ms`), a size in the unit that gives the smallest whole number (`256MiB`). `ui.prometheus.rate_interval` stays a PromQL text. The code APIs keep their numbers: `@periodic(interval=..., lease_seconds=...)`.

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
    ConnectionsConfig,
    DrakkarConfig,
    ExecutorConfig,
    KafkaConnectionConfig,
    KafkaSourceConfig,
    SourcesConfig,
)

app = DrakkarApp(
    handler=MyHandler(),
    config=DrakkarConfig(
        connections=ConnectionsConfig(
            kafka={'main': KafkaConnectionConfig(brokers='kafka:9092')},
        ),
        sources=SourcesConfig(
            kafka=KafkaSourceConfig(enabled=True, topic='my-events'),
        ),
        executor=ExecutorConfig(max_executors=8, task_timeout='5m'),
    ),
)
```

!!! note
    When you pass a `config` object directly, no YAML file or env var loading occurs -- you have full control.

### Environment Variable Overrides

Environment variables use the `DK_` prefix with `__` (double underscore) as the nesting delimiter. A variable name is the YAML path in upper case, so `sinks.postgres.main-db.pool_max` is `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX`:

```bash
# Override connections.kafka.main.brokers
export DK_CONNECTIONS__KAFKA__MAIN__BROKERS=kafka-prod:9092

# Override executor.max_executors
export DK_EXECUTOR__MAX_EXECUTORS=16

# A duration or a size is the same text as in YAML
export DK_EXECUTOR__TASK_TIMEOUT=5m

# Override api.port
export DK_API__PORT=9000
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

The `worker` and `cluster` sections name this worker and its cluster.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker.name` | `str` | `''` | Literal worker name, used in logs, metrics, the registry card, the operator UI and the own folder `<storage.dir>/<cluster>/<worker>/`. It must be 1 to 128 letters, digits, `_` or `-`; any other name fails the start with a message that gives the safe form. |
| `worker.name_env` | `str` | `'WORKER_ID'` | Name of the environment variable that holds the worker name. When the variable is set and not empty, it wins over `worker.name`; its value follows the same rule. With neither, the worker uses its host name with every other character turned into `_` (in Kubernetes: the pod name), or `drakkar-<8 hex>` when the host name is empty or a localhost name such as `localhost.localdomain`. See [Worker names](local-databases.md#worker-names). |
| `cluster.name` | `str` | `''` | Literal cluster name. Workers with the same cluster name are displayed together, share the lock scope of cluster tasks and the history folder, and can cross-trace messages. Empty with `autodiscover` on logs `cluster_name_empty` once at startup. A name whose folder name is `fleet`, `history`, `merged`, `tasks` or `workers` (any letter case) fails the load. |
| `cluster.name_env` | `str` | `''` | Name of an environment variable that holds the cluster name. When the variable is set and not empty, it wins over `cluster.name`. |
| `app` | `dict` | `{}` | Reserved section for **user-defined application config**: passed through unvalidated to the handler-declared model and exposed as `self.app_config`, with env overrides under the handler's own prefix (never `DK_APP__*`). See [App Config](app-config.md). |

```yaml
worker:
  name_env: HOSTNAME
cluster:
  name: search-cluster
  name_env: DK_CLUSTER
```

---

## Connections (`connections:`) {#connections}

A service is declared here once, and every consumer refers to it by name. `connections` has one map per service kind (`kafka`, `postgres`, `redis`, `mongo`, `consul`), and each map goes from a name that you choose to the settings of that service: where it is and how to log in. A consumer says `connection: <name>`; with no name it uses `main`. The entry it names must be declared. A different login to one server is a different connection.

| Kind | Used by |
|---|---|
| `kafka` | the Kafka source (`sources.kafka.connection`), every Kafka sink, the DLQ (`dlq.connection`) |
| `postgres` | every Postgres sink, the recorder with provider `postgres` (`recorder.postgres.connection`), autodiscover with provider `postgres` |
| `redis` | every Redis sink, autodiscover with provider `redis` |
| `mongo` | every Mongo sink |
| `consul` | autodiscover with provider `consul` |

The load checks only the entries that something uses. A source that is on, every configured sink, the DLQ when it has a destination, the recorder with provider `postgres`, and autodiscover with provider `consul`, `postgres` or `redis` each fail with `<path>.connection '<name>' is not defined under connections.<kind>` when the entry is missing. A worker that reads a Kafka cluster not named `main` therefore sets `sources.kafka.connection`, `dlq.connection` and the `connection` of each Kafka sink.

### Kafka (`connections.kafka.<name>`)

How a worker reaches one Kafka cluster. The block holds no consumer settings: the consumer is an input source and lives under [`sources.kafka`](#kafka-source-sourceskafka). One connection can serve the Kafka source, any number of [Kafka sinks](#kafka-sink-sinkskafkaname) and the [DLQ](#dead-letter-queue-dlq).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `str` | `'localhost:9092'` | Kafka bootstrap servers (comma-separated for multiple brokers). |
| `security` | block | PLAINTEXT | Transport authentication and encryption. See [Kafka security](#kafka-security). |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties merged after `security`. See [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `ui_url` | `str` | `''` | Base URL of a Kafka-UI instance. With `ui_cluster_name`, the operator UI renders a deep link next to every `partition:offset` of the source topic. Empty disables the links. |
| `ui_cluster_name` | `str` | `''` | Cluster name as registered in Kafka-UI, used in those deep links. Both fields must be set. |

```yaml
connections:
  kafka:
    main:
      brokers: kafka-1:9092,kafka-2:9092
      ui_url: http://kafka-ui:8080
      ui_cluster_name: search-cluster
```

#### Kafka security (`security`) {#kafka-security}

The default is `PLAINTEXT`, which emits no librdkafka properties at all. Configure it to reach a cluster that requires authentication or TLS.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protocol` | `'PLAINTEXT'` \| `'SSL'` \| `'SASL_PLAINTEXT'` \| `'SASL_SSL'` | `'PLAINTEXT'` | Wire protocol. `SSL` adds TLS, `SASL_PLAINTEXT` adds authentication without encryption, `SASL_SSL` adds both and is what managed clusters normally require. |
| `sasl_mechanism` | `'PLAIN'` \| `'SCRAM-SHA-256'` \| `'SCRAM-SHA-512'` \| `'GSSAPI'` \| `'OAUTHBEARER'` \| `null` | `null` | Required when `protocol` is a `SASL_*` value, and **rejected** otherwise so a mechanism that would be silently ignored never sits in your config. |
| `sasl_username` | `str` | `''` | Required for `PLAIN` and the SCRAM mechanisms. |
| `sasl_password` | secret | `''` | Required for `PLAIN` and the SCRAM mechanisms. Never rendered in logs, summaries or the config view. |
| `ssl_ca_location` | `str` | `''` | Path to a PEM CA bundle. Empty uses the system trust store. |
| `ssl_certificate_location` | `str` | `''` | Client certificate for mutual TLS. |
| `ssl_key_location` | `str` | `''` | Client private key for mutual TLS. Requires `ssl_certificate_location`. |
| `ssl_key_password` | secret | `''` | Passphrase for an encrypted client key. |
| `ssl_endpoint_identification_algorithm` | `'https'` \| `'none'` \| `null` | `null` | Unset uses librdkafka's default (`https` — hostname verification on). Set `none` only for internal CAs whose certificates lack matching SANs. |

**Keep credentials out of YAML.** Every field takes a `DK_` environment override, and `DK_*` variables are withheld from executor subprocesses by the default `executor.env_inherit_deny` list, so a password supplied that way never reaches your handler binary.

```bash
export DK_CONNECTIONS__KAFKA__MAIN__SECURITY__SASL_PASSWORD='s3cret'
```

Incoherent combinations fail at startup rather than at first poll, because librdkafka reports most of them as an opaque connection error long after the mistake was made:

- a `SASL_*` protocol with no `sasl_mechanism`
- `PLAIN`/SCRAM without `sasl_username` **and** `sasl_password`
- a `sasl_mechanism` set while `protocol` is `PLAINTEXT` or `SSL`
- `ssl_key_location` without `ssl_certificate_location`

Certificate paths are *not* checked when config loads. A missing file logs a warning at startup and librdkafka fails the connect: a hard failure at parse time would turn an init-container mount race into a crash loop.

##### Confluent Cloud / Aiven (SASL_SSL + PLAIN)

```yaml
connections:
  kafka:
    main:
      brokers: pkc-abc12.eu-west-1.aws.confluent.cloud:9092
      security:
        protocol: SASL_SSL
        sasl_mechanism: PLAIN
        sasl_username: MYCLUSTERAPIKEY
        # password via DK_CONNECTIONS__KAFKA__MAIN__SECURITY__SASL_PASSWORD
```

##### Self-managed cluster with SCRAM and a private CA

```yaml
connections:
  kafka:
    main:
      brokers: kafka-1:9093,kafka-2:9093
      security:
        protocol: SASL_SSL
        sasl_mechanism: SCRAM-SHA-512
        sasl_username: drakkar
        ssl_ca_location: /etc/ssl/certs/internal-ca.pem
```

##### Mutual TLS (no SASL)

```yaml
connections:
  kafka:
    main:
      brokers: kafka-1:9093
      security:
        protocol: SSL
        ssl_ca_location: /etc/ssl/certs/ca.pem
        ssl_certificate_location: /etc/ssl/certs/client.pem
        ssl_key_location: /etc/ssl/private/client.key
```

#### How sinks and the DLQ get their credentials

A Kafka sink and the DLQ name a connection with `connection` (default `main`) and use its brokers, `security` and `client_config` as they are. Two sinks that write to different clusters name two connections; two sinks that write to different topics of one cluster name one connection:

```yaml
connections:
  kafka:
    main:
      brokers: kafka-1:9093
      security:
        protocol: SASL_SSL
        sasl_mechanism: SCRAM-SHA-512
        sasl_username: drakkar
    analytics:
      brokers: kafka-analytics:9092

sinks:
  kafka:
    results:
      topic: search-results          # connection main
    events:
      connection: analytics
      topic: analytics-events
```

#### Raw librdkafka overrides (`client_config`) {#raw-librdkafka-overrides-client_config}

For properties the typed block does not model: OAUTHBEARER token settings, Kerberos tuning, connection-pool knobs:

```yaml
connections:
  kafka:
    main:
      client_config:
        sasl.oauthbearer.config: 'principal=drakkar'
        socket.keepalive.enable: 'true'
```

`client_config` is merged **after** `security`, so it wins on any key both set. A Kafka sink and the DLQ also take a `client_config`; it is merged after the connection's, so on a key present in both the sink's or the DLQ's value wins. Four properties are rejected at startup, on every `client_config`, because each one backs a delivery invariant:

| Rejected key | Why |
|---|---|
| `enable.auto.commit` | At-least-once delivery depends on Drakkar committing on its own per-partition watermark; auto-commit would advance past unprocessed messages. |
| `partition.assignment.strategy` | The drain-on-revoke path assumes cooperative-sticky rebalancing. |
| `group.id` | Set it with `sources.kafka.consumer_group`. |
| `bootstrap.servers` | Set it with `connections.kafka.<name>.brokers`. |

Rejecting beats ignoring: an override that is silently dropped looks like it worked.

### Mongo (`connections.mongo.<name>`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uri` | `str` | required | MongoDB connection URI (for example `mongodb://host:27017`). **Secret.** |

The Mongo sink names the connection and adds the database it writes to (`sinks.mongo.<name>.database`).

### Consul (`connections.consul.<name>`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `address` | `str` | `http://127.0.0.1:8500` | checked when a provider starts: `http://` or `https://`, printable ASCII, no user name, query or fragment (see below) | Base URL of the Consul HTTP API. |
| `token` | `str` | `''` | empty, or printable ASCII without a space (checked when a provider starts) | ACL token, sent with every request in the `X-Consul-Token` header. Empty means none. **Secret.** |
| `datacenter` | `str` | `''` | letters, digits, `_`, `.`, `-` | Datacenter to address, sent as `dc` with every request. Empty means the datacenter of the agent. |
| `state_prefix` | `str` | `drakkar` | segments of letters, digits, `_`, `.`, `-` joined by `/`; no leading or trailing `/`; no segment `.` or `..` | Prefix of the keys Drakkar itself keeps in Consul. |

The `consul` provider talks to the HTTP API of a Consul agent. Each card or
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

- `connections.consul.<name>.state_prefix '<value>' must be segments of letters, digits, '_', '.' and '-' joined by '/', with no leading or trailing '/', and no segment may be '.' or '..'`
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
| `state_prefix` | `str` | `drakkar:` | letters, digits, `_`, `:`, `.`, `-`; may be empty | Prefix of the keys Drakkar itself keeps in Redis (cards and locks of autodiscover). The Redis sink's own `key_prefix` is separate. |

The `redis` providers need Redis 5.0 or later, standalone or a primary behind a
fixed address; Redis Cluster is not supported. With `rediss://`, the client
verifies the certificate of the server against the system's default
certificate store and checks that the certificate names the host of the URL;
this cannot be switched off. Put a private certificate authority into that
store; `SSL_CERT_FILE` and `SSL_CERT_DIR` replace the default locations of the
store. The client does no retrying of
its own: the [`resilience` block](#resilience-block) of the feature is the only
retry and the only time limit, apart from a connect, which gives up after 5
seconds. The Python package `redis` must be version 7.2 or later; the [Redis sink](#redis-sink-sinksredisname) uses the same
package. See [Fleet registry: redis](fleet.md#redis) and
[Security](security.md#provider-credentials).

### Postgres (`connections.postgres.<name>`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `dsn` | `str` | required | | asyncpg-compatible connection string, typically with user and password. **Secret.** |
| `schema` | `str` | `drakkar` | lower-case identifier, `[a-z_][a-z0-9_]*` | Schema of the tables Drakkar itself keeps (autodiscover and the recorder). |
| `manage_schema` | `bool` | `true` | | Create the schema and the tables at the first successful start of a provider that uses the connection. Off, they must already exist: create them with `drakkar-schema postgres --schema=<name>`, which prints the DDL; a provider whose table is missing then fails its start with a message that names the command. For the recorder events table of [`recorder.provider: postgres`](recorder-postgres.md#schema-management-and-ownership) add `--recorder` and, as needed, `--table-prefix=<p>` and `--retention-hours=<n>` (1 to 168, default 6). Partitions are always created by the recorder, so the role must own that table. |
| `pool_min` | `int` | `1` | >= 1 | Minimum connections in the pool. |
| `pool_max` | `int` | `4` | >= 1, and >= `pool_min` | Maximum connections in the pool. The autodiscover registry and election share this pool. |

```yaml
connections:
  consul:
    main:
      address: http://consul.internal:8500
      state_prefix: drakkar
  redis:
    main:
      state_prefix: "drakkar:"
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

An environment variable names a connection in lower case with `_` for `-`. When the YAML file already declares the name, the variable reaches it whatever the spelling: `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX=3` sets `sinks.postgres.main-db.pool_max`. When two declared names differ only by case or by `-` against `_`, the variable is ambiguous and the load fails naming both. A wrong value fails the load with the path and the rule, for
example `connections.postgres.main.schema 'Bad-Name' must be a lower-case
identifier ([a-z_][a-z0-9_]*)`. The message quotes that value, but it never
prints the input of the whole section, so a connection string stays out of
it. See [Security](security.md#provider-credentials).

---

## Input sources (`sources:`)

A worker reads work from Kafka, from HTTP, or from both. Each input is a
**source** under `sources:`. Both are off by default, and **at least one
must be on**: a config with neither fails to load:

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
connects to is the entry `connection` names under
[`connections.kafka`](#kafka-connectionskafkaname); an enabled source needs
that entry.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Consume the Kafka topic. When `false`, no consumer group is joined and `arrange()` is never called. |
| `connection` | `str` | `'main'` | The Kafka cluster to read: a name under `connections.kafka`. It must be declared. |
| `topic` | `str` | `'input-events'` | The Kafka topic to consume messages from. |
| `consumer_group` | `str` | `'drakkar-workers'` | Consumer group ID. All workers in the same group share partition assignments. |
| `max_poll_records` | `int` | `100` | Maximum number of messages returned per poll batch. Higher values improve throughput; lower values reduce latency. |
| `max_queued_messages` | `int` | `1000` | Kafka fetching pauses when this many polled messages wait in the queues of live partitions, and resumes at half of it. Must be at least `max_poll_records`: the load fails with `sources.kafka.max_queued_messages (<n>) must be >= sources.kafka.max_poll_records (<m>)` when the source is enabled. A dead or stalled partition stays paused, and a dead partition's queue does not count. |
| `max_poll_interval` | duration | `5m` | Maximum time between poll calls before Kafka considers the consumer dead and triggers a rebalance. Increase this if your tasks take a long time. |
| `session_timeout` | duration | `45s` | Session timeout for group membership. If the broker does not receive a heartbeat within this window, the consumer is removed from the group. |
| `heartbeat_interval` | duration | `3s` | Interval between heartbeats sent to the broker. Should be less than `session_timeout / 3`. |
| `on_parse_error` | `'skip'` \| `'dlq'` \| `'raise'` | `'skip'` | What to do when a message value fails `input_model` parsing. `skip`: the message reaches `arrange()` with `payload=None` and `msg.parse_error` set. `dlq`: the message is excluded from `arrange()` and written to the DLQ topic as a [DLQ record](sinks.md#the-dlq-record) (`dk-stage: parse`: the original key, value and headers, no `dk-attempts`); the offset commits once the DLQ write is confirmed (a failed write follows `dlq.on_send_failure`). `raise`: fail fast: a `MessageParseError` stops the partition processor. |
| `startup_align.enabled` | `bool` | `true` | When `true`, delay the first Kafka subscribe until a shared wall-clock boundary so a fleet of workers converges on a single rebalance. Disable for single-process dev runs. |
| `startup_align.min_wait` | duration | `4s` | Minimum time to sleep before aligning. Acts as a buffer for slow init (DB connects, schema migrations, cache warm-up). |
| `startup_align.interval` | duration | `10s` | Alignment interval, a whole number of seconds, at least `1s`. Workers wake at the next `time.time() % interval == 0` boundary: the default aligns on `:00/:10/:20/:30/:40/:50` of every minute. |

When enabled, `topic` and `consumer_group` must be non-empty.

```yaml
connections:
  kafka:
    main:
      brokers: kafka-1:9092,kafka-2:9092

sources:
  kafka:
    enabled: true
    topic: search-requests
    consumer_group: search-workers
    max_poll_records: 200
    max_poll_interval: 10m
    session_timeout: 1m
    heartbeat_interval: 5s
    # Rolling-deploy: serialize group rebalances by waking all workers at
    # the same wall-clock moment instead of whenever each finishes init.
    startup_align:
      enabled: true
      min_wait: 4s
      interval: 10s
```

Env overrides follow the keys: `DK_SOURCES__KAFKA__ENABLED`,
`DK_SOURCES__KAFKA__TOPIC`, `DK_SOURCES__KAFKA__STARTUP_ALIGN__INTERVAL`, and so on.

#### Staggered startup alignment

During a rolling deploy, workers come up one at a time over a span of seconds. Each fresh `subscribe` call triggers a Kafka consumer-group rebalance, which stalls consumption on every other worker in the group. A fleet of 10 workers that boots over ~15 seconds can cause 10 cascading rebalances and several seconds of effective downtime.

With `startup_align.enabled: true` (the default), each worker runs its normal init (`on_startup`, cache engine, recorder, periodic tasks, sink connects) and then, *before* calling `consumer.subscribe()`, waits until:

1. At least `startup_align.min_wait` has elapsed (lets slow-init workers catch up to fast ones).
2. The wall-clock Unix-epoch seconds are a multiple of `startup_align.interval` (default `10s`, that is `:00/:10/:20/:30/:40/:50`).

Workers whose init completes anywhere in the same 10-second window then all subscribe at the next boundary within one "tick" of each other, collapsing N rebalances into 1.

The sleep window is logged as `startup_align_waiting` / `startup_align_done` lifecycle events with the target instant so deploy runbooks can account for the deliberate pause.

Tuning:

- **Larger fleets**: keep `startup_align.interval: 10s` (default). It works well up to dozens of workers.
- **Slow-init workers** (seconds of migrations): raise `startup_align.min_wait` so the boundary is likely to fall AFTER the slowest worker's init.
- **Very small clusters / dev iteration**: set `startup_align.enabled: false` to skip the pause entirely.

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
| `port` | `int` | `8090` | | Port uvicorn binds. Distinct from the metrics and API ports. |
| `path` | `str` | `'/process'` | starts with `'/'`, length > 1 | Single POST route the framework registers. |
| `deliver_to_sinks` | `bool` | `false` | | When `true`, calls `on_message_complete` after the executor fan-out and routes returned `CollectResult` payloads through the [SinkManager](sinks.md). When `false`, sinks are skipped and the response carries `sinks: null`. |
| `request_timeout` | duration | `30s` | > `0` | Per-request budget enforced via `asyncio.wait_for` on the webapp loop. On timeout the client receives a 504 and the runner's post-execute hooks are cooperatively cancelled. |
| `max_concurrent` | `int` | `64` | > 0 | Per-worker semaphore capacity for in-flight HTTP requests. The 65th concurrent request returns 503 `status='capacity'` immediately rather than queuing. |
| `max_body` | size | `10MiB` | > `0` | Cap on a single POST body; oversized requests receive 413 `error='request_too_large'` before the body is buffered. |
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
    deliver_to_sinks: false
    request_timeout: 30s
    max_concurrent: 64
    max_body: 10MiB
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
| `task_timeout` | duration | `2m` | whole seconds, >= `1s` | Wall-clock time limit per subprocess. If a process exceeds this, it is killed and treated as a failure. |
| `max_stdout` | size | `0` | >= 0 | Most stdout kept per task. `0` = no limit. Excess output is still read and discarded (the process never blocks on a full pipe); the kept prefix is cut at a UTF-8 character boundary and `ExecutorResult.stdout_truncated` is set. |
| `max_stderr` | size | `0` | >= 0 | Same as `max_stdout`, for stderr; sets `ExecutorResult.stderr_truncated`. |
| `window_size` | `int` | `100` | >= 1 | Maximum number of messages collected per [arrange()](handler.md#arrange-required) [window](executor.md#windowing). Larger windows allow more batching in `arrange()`; smaller windows reduce latency. |
| `max_retries` | `int` | `3` | >= 0 | Maximum number of retry attempts per failed task (0 = no retries). A task can run up to `max_retries + 1` times total. |
| `on_failure` | `'skip'` \| `'dlq'` | `'skip'` | | What the default [on_error()](handler.md#on_error) returns for a failed task. `dlq` writes the task's source messages to the DLQ (`dk-stage: execute`), and no later delivery of those messages starts. Any other value fails the load with `executor.on_failure must be skip or dlq, got '<v>'`. |
| `drain_timeout` | duration | `30s` | whole seconds, >= `1s` | Longest wait for in-flight tasks during shutdown or partition revocation. When drain times out, the still-running tasks are cancelled (their subprocess process group is killed) and their offsets are **not** committed — those messages replay on restart (at-least-once). Tune together with `task_timeout`. |
| `max_pending_tasks` | `int` | `2000` | >= `max_executors` | New windows wait while this many tasks are created and not settled (waiting for a slot, running, or in `on_task_complete` and its delivery). Below `max_executors` the load fails with `executor.max_pending_tasks (<n>) must be >= executor.max_executors (<m>)`. A soft limit: a window reserves its estimate when it is taken, so the limit is passed only by its fan-out beyond the estimate — normally one message; a sudden jump in tasks per message passes it by that jump. |
| `max_pending_bytes` | size | `1GiB` | >= `16MiB` | New windows wait while task stdin plus the stdout and stderr of results in `on_task_complete`, `on_error` and the sink delivery, or kept for `on_message_complete` and `on_window_complete`, reach this size. Large outputs slow admission. A soft limit, like `max_pending_tasks`. Size it to about a quarter of the container memory. |

### Backpressure

[Backpressure](performance.md#backpressure) bounds the work a worker holds in
two stages. A partition loop takes a new window only while the pending tasks
are below `max_pending_tasks` and the pending bytes below
`max_pending_bytes`. When loops stop taking windows, the partition queues
fill; the Kafka fetch pauses when the queues of the live partitions hold
`sources.kafka.max_queued_messages` messages and resumes at half of it.

```yaml
executor:
  binary_path: /usr/local/bin/my-processor
  max_executors: 8
  task_timeout: 5m
  window_size: 50
  max_retries: 5
  drain_timeout: 10s
  max_pending_tasks: 1000
  max_pending_bytes: 512MiB
```

---

## Thread pools (`threads:`)

The two thread pools of the worker. See [Threads & Pools](threads.md) for the full map.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `io` | `int \| null` | `null` | null or 1--512 | Worker threads in asyncio's default `to_thread` executor — the single process-wide pool behind every `asyncio.to_thread(...)` call, handler filesystem reads included. `null` (the default) keeps Python's own sizing, `min(32, cpu_count + 4)`. Set an explicit value when handler I/O concurrency is capped by the pool — e.g. many-core hosts doing wide blocking filesystem fan-out. Blocking I/O releases the GIL, so large values are legitimate here; the trade-off is pressure on the storage behind the calls. |
| `offload` | `int \| null` | `null` | null or 1--32 | Worker threads in the pool behind [`handler.offload()`](offload.md) — CPU-bound hook work moved off the event loop. `null` (the default) sizes the pool from the executor pool: `ceil(executor.max_executors / 4)`, minimum 2. Calls beyond this many concurrent offloaded computations queue (FIFO) and show up in the `drakkar_offload_queued` gauge. More threads do not speed up pure-Python work (GIL); set an explicit value only when several partitions routinely offload at once and queueing delay matters, or when the offloaded code releases the GIL (numpy, compiled extensions). |

`0` fails the load: `threads.io must be null or from 1 to 512`, `threads.offload must be null or from 1 to 32`.

```yaml
threads:
  io: null
  offload: null
```

---

## Sinks (`sinks:`)

Sinks define where processed results are delivered. See [Sinks](sinks.md) for payload models, [routing](sinks.md#routing), and the [delivery lifecycle](sinks.md#delivery-lifecycle). Each sink type is a dictionary mapping instance names to their configuration. You can configure multiple instances of the same type (for example two Kafka sinks writing to different topics).

A Kafka, Postgres, Mongo or Redis sink does not carry an address of its own: it names a [connection](#connections) with `connection` (default `main`) and adds what is its own, a topic, a database, statements. Every sink owns its client or pool, so sinks that share a connection do not share a pool. An HTTP sink and a filesystem sink have no connection.

### `sinks.defaults` and the keys of every sink instance {#keys-of-every-sink-instance}

The failure policy is one set of five keys. `sinks.defaults` gives the value for every sink, and every instance of every type can override each key. A key not given on an instance reads `sinks.defaults`; a key given on an instance wins even when it equals the default.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `on_failure` | `'dlq'` \| `'skip'` | `'dlq'` | | What happens when the retry budget is spent. `dlq`: the source message goes to the DLQ and the message is done. `skip`: this sink's payloads are dropped. |
| `max_retries` | `int` | `2` | whole number, 0 to 10 | Retries after the first attempt, spent only by a hook that returns `RETRY`. The default hook makes one attempt and then applies `on_failure`. The quick internal retry of an idempotent write does not count. |
| `delivery_timeout` | duration | `30s` | > `0` | Budget for one delivery, framework-internal retries included, and for one `close()` at shutdown. See [Delivery timeout](#delivery-timeout). |
| `breaker_failures` | `int` | `5` | >= 1 | Consecutive terminal failures that trip the circuit breaker. See [Circuit breaker](#circuit-breaker). |
| `breaker_cooldown` | duration | `30s` | >= `0` | How long the breaker stays open before one probe delivery goes through. |

`depends_on` is on the instance only, never in `defaults`:

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `depends_on` | `list[str]` | `[]` | each entry `<type>/<name>` of a configured sink; no self-reference, no cycle | Sinks that must deliver first inside one result. See [Sink dependencies](sinks.md#sink-dependencies-depends_on). |

```yaml
sinks:
  defaults:
    on_failure: dlq
    max_retries: 2
    delivery_timeout: 30s
    breaker_failures: 5
    breaker_cooldown: 30s
  http:
    webhook:
      url: https://api.example.com/webhook
      max_retries: 5         # this sink only
      on_failure: skip       # a lost notification is acceptable
```

Environment overrides: `DK_SINKS__<TYPE>__<NAME>__ON_FAILURE`, `DK_SINKS__DEFAULTS__MAX_RETRIES`. The load messages are listed under [Failure policy](sinks.md#failure-policy).

### Kafka Sink (`sinks.kafka.<name>`)

Produces messages to a Kafka topic.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | `str` | `'main'` | The Kafka cluster: a name under `connections.kafka`. Brokers, `security` and `client_config` come from it. |
| `topic` | `str` | *(required)* | Target Kafka topic for output messages. |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties for this sink's producer, merged after the connection's. This sink wins on a shared key. Same rules as [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `ui_url` | `str` | `''` | URL to a web UI for this sink (for example Kafka UI, Kowl). Displayed as a link in the operator UI. |
| `flush_timeout` | duration | `30s` | Bound on the producer flush that ends every delivery (see below). |

Every batch ends with a producer flush: the `AIOProducer` hands messages to
librdkafka only once its internal buffer fills or its one-second inactivity
timer expires, so without the flush every delivery would wait out that timer.
`flush_timeout` bounds that flush. Unbounded, it becomes librdkafka's
`flush(-1)`, which against a wedged broker blocks until `message.timeout.ms`
(300 s by default), and it blocks one of the producer's executor threads
while it waits, so a handful of stuck deliveries starve every other delivery
on the same producer. The outer
[`delivery_timeout`](#delivery-timeout)
cannot rescue that: cancelling the await does not stop the thread, so the
bound has to reach librdkafka itself.

On expiry the delivery fails with a `TimeoutError`, which the sink manager
classifies as transient, so the circuit breaker sees the outage and an
idempotent sink still gets its fast-retry. Keep the value generous: it is a
last resort, not a latency target.

### PostgreSQL Sink (`sinks.postgres.<name>`)

Inserts rows into a PostgreSQL database via an asyncpg connection pool.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `connection` | `str` | `'main'` | a name under `connections.postgres` | The database: DSN and login come from the connection. |
| `pool_min` | `int` | `2` | >= 1 | Minimum number of connections in this sink's pool. |
| `pool_max` | `int` | `10` | >= 1 | Maximum number of connections in this sink's pool. |
| `statements` | `dict[str, str]` | `{}` | | Operator-authored SQL keyed by name, run by `PostgresPayload(op='statement')` with `:name` placeholder binding. See [statement: arbitrary SQL](sink-write-operations.md#statement-arbitrary-sql-operator-controlled). |
| `ui_url` | `str` | `''` | | URL to a database management UI (for example pgAdmin). |

### MongoDB Sink (`sinks.mongo.<name>`)

Inserts documents into a MongoDB database via PyMongo's AsyncMongoClient.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | `str` | `'main'` | The server: a name under `connections.mongo`. The URI comes from the connection. |
| `database` | `str` | *(required)* | Target database name. |
| `statements` | `dict` | `{}` | Operator-authored MQL keyed by name, run by `MongoPayload(op='statement')` with `:name` placeholder binding. See [statement: arbitrary MQL](sink-write-operations.md#statement-arbitrary-mql-operator-controlled). |
| `ui_url` | `str` | `''` | URL to a MongoDB management UI (for example Mongo Express). |

### HTTP Sink (`sinks.http.<name>`)

Sends payloads to an HTTP endpoint (JSON by default).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `url` | `str` | *(required)* | `http://` or `https://` scheme; non-empty host; must **not** be a cloud metadata endpoint | Target URL for HTTP requests. Validated at config load time. |
| `method` | `str` | `'POST'` | | HTTP method to use. |
| `request_timeout` | duration | `30s` | whole seconds, >= `1s` | Timeout of one HTTP request. The whole delivery has its own budget, `delivery_timeout`. |
| `headers` | `dict[str, str]` | `{}` | | Additional HTTP headers sent with each request. |
| `encoding` | `Literal['json', 'form', 'multipart']` | `'json'` | one of `json`, `form`, `multipart`; `headers` must not set `Content-Type` | Request body format. See [Body encoding](#body-encoding-sinkshttpnameencoding). |
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
`Content-Type` in `headers` is a configuration error, rejected even when
the header agrees with the body the encoder would send. For the default
`encoding: json` the header is `application/json` with no `charset`
parameter; per RFC 8259, UTF-8 is JSON's default charset, so receivers
need none. Remove the header, or change the encoding.

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
| `connection` | `str` | `'main'` | The Redis server: a name under `connections.redis`. The URL comes from the connection. |
| `key_prefix` | `str` | `''` | Prefix prepended to all keys this sink writes (for example `cache:` produces keys like `cache:my-key`). |
| `scripts` | `dict[str, str]` | `{}` | Operator-authored Lua keyed by name, run by `RedisPayload(op='script')` with `keys`/`args` passed as `KEYS`/`ARGV`. See [script: arbitrary Lua](sink-write-operations.md#script-arbitrary-lua-operator-controlled). |
| `ui_url` | `str` | `''` | URL to a Redis management UI (for example RedisInsight). |

### Filesystem Sink (`sinks.filesystem.<name>`)

Appends JSONL lines to files on disk.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | `str` | *(required)* | Base directory. Payload paths are resolved relative to it and must stay contained within it — absolute or traversing paths are rejected. |
| `ui_url` | `str` | `''` | URL to a file browser or related UI. |

### Example: Multiple Named Sinks

```yaml
connections:
  kafka:
    main:
      brokers: kafka-prod:9092
    analytics:
      brokers: kafka-analytics:9092
  postgres:
    main:
      dsn: postgresql://user:pass@db:5432/myapp   # better: DK_CONNECTIONS__POSTGRES__MAIN__DSN
  mongo:
    main:
      uri: mongodb://mongo:27017
  redis:
    main:
      url: redis://redis:6379/0

sinks:
  kafka:
    search-results:
      topic: search-results
    analytics:
      connection: analytics
      topic: analytics-events

  postgres:
    main-db:
      pool_min: 5
      pool_max: 20
      ui_url: http://pgadmin.internal:5050

  mongo:
    logs:
      database: app_logs

  http:
    webhook:
      url: https://api.example.com/webhook
      method: POST
      request_timeout: 15s
      headers:
        Authorization: "Bearer ${API_TOKEN}"
      max_retries: 5
      on_failure: skip               # a lost notification is acceptable
      depends_on: [postgres/main-db] # notify only after the row is written

  redis:
    cache:
      key_prefix: "result:"
      ui_url: http://redis-insight.internal:8001

  filesystem:
    archive:
      base_path: /data/archive
```

### Delivery timeout (`delivery_timeout`) {#delivery-timeout}

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
timeout (asyncpg `command_timeout`, redis-py `socket_timeout` /
`socket_connect_timeout`, PyMongo `socketTimeoutMS` / `connectTimeoutMS`),
so the driver raises a specific error just before the outer budget expires.
The HTTP sink keeps its own `request_timeout`, which is per request.

Keep it below `executor.drain_timeout`: a rebalance drain waits on
in-flight deliveries, so a delivery budget larger than the drain budget
guarantees drain timeouts when a sink goes unresponsive.

### Circuit breaker (`breaker_failures`, `breaker_cooldown`) {#circuit-breaker}

Every registered sink has a per-instance circuit breaker. The breaker
trips after `breaker_failures` consecutive terminal failures (retries
exhausted + DLQ action) and skips all deliveries for `breaker_cooldown`
before allowing a single probe through. A successful probe closes the
breaker; a failing probe reopens with a fresh cooldown. A `SKIP` outcome
does NOT count: it is operator intent, not a health signal. See
[Sinks: Circuit Breaker](sinks.md#circuit-breaker) for the full state
machine and metrics.

The defaults are reasonable for most deployments. Lower
`breaker_failures` in latency-sensitive pipelines where one stuck sink
would block the whole delivery fan-out; raise `breaker_cooldown` when
probing a recovering downstream is itself expensive.

---

## Dead Letter Queue (`dlq:`)

Messages that fail to parse, to execute or to deliver can be written to a [DLQ](sinks.md#dead-letter-queue) Kafka topic. Each [DLQ record](sinks.md#the-dlq-record) is the original source message (key, value and headers unchanged) with `dk-` headers that say where and why it failed, for later inspection or replay.

The DLQ is a feature with a switch (`dlq.enabled`, default `true`) and a provider (`dlq.provider`, `kafka` is the only value). Its cluster is the entry `dlq.connection` names under `connections.kafka`; the entry is needed when the DLQ has a destination.

**Whether a DLQ producer is built depends on the topic.** The DLQ derives its default topic from the source topic, so a worker without a Kafka source has nothing to derive from and needs an explicit `dlq.topic`:

| `dlq.enabled` | `sources.kafka.enabled` | `dlq.topic` | Result |
|---|---|---|---|
| on | on | empty | DLQ built, topic `{sources.kafka.topic}_dlq` |
| on | on | set | DLQ built with that topic |
| on | off | set | DLQ built with that topic, on the cluster of `dlq.connection` |
| on | off | empty | **No DLQ producer.** The summary line reports `dlq=unconfigured`. A DLQ send is dropped, logged once per worker run as `dlq_send_dropped_unconfigured` (then at debug), and counted in `drakkar_dlq_unconfigured_drops_total`. |
| off | any | any | **No DLQ producer.** The summary line reports `dlq=off`. A `dlq` outcome follows `on_send_failure`, as with no DLQ. |

The startup log `sinks_configured` reports `dlq_topic: ""` when the DLQ has no destination. `on_parse_error: dlq` is a Kafka-source setting, so it is only reachable when the Kafka source is on.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `true` | Write DLQ records. Off, a `dlq` outcome follows `on_send_failure`. |
| `provider` | `str` | `'kafka'` | Where DLQ records go. Any other value fails the load with `dlq.provider must be kafka, got '<v>'`. |
| `connection` | `str` | `'main'` | The Kafka cluster: a name under `connections.kafka`. Brokers, `security` and `client_config` come from it. |
| `topic` | `str` | `''` | DLQ Kafka topic name. Empty derives `{sources.kafka.topic}_dlq` (for example `input-events_dlq`); with the Kafka source off, empty means no DLQ (table above). |
| `client_config` | `dict[str, str]` | `{}` | Raw librdkafka properties for the DLQ producer, merged after the connection's. The DLQ wins on a shared key. Same rules as [the escape hatch](#raw-librdkafka-overrides-client_config). |
| `flush_timeout` | duration | `30s` | Bound on the producer flush that ends every DLQ write. Same rationale as [the Kafka sink's](#kafka-sink-sinkskafkaname), and it matters more here: the DLQ is the last resort, so a write that blocks for `message.timeout.ms` stalls the partition it was meant to rescue. On expiry the write is reported as unconfirmed and `on_send_failure` applies. |
| `on_send_failure` | `'drop'` \| `'stall'` | `'drop'` | Strategy when no DLQ record can be written (the write fails, or a payload names a sink that is not configured). `drop`: log + count the loss, commit the offset, keep the pipeline moving. `stall`: leave the offset uncommitted and pause the partition until restart: no loss, at the cost of consumer lag. See [When the DLQ itself fails](sinks.md#when-the-dlq-itself-fails). |

```yaml
dlq:
  topic: failed-events
  connection: main
  flush_timeout: 30s
  on_send_failure: drop
```

---

## Own storage and the shared folder (`storage:`) {#storage-and-shared}

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dir` | `str` | `'/tmp/drakkar'` | The folder of the own folders: each worker keeps its live recorder file, cache file and crash marker in `<dir>/<sanitized cluster>/<worker>/`, and only that worker opens them. An owner lock in the folder refuses a second process with the same worker name on the host. Must be a **local filesystem**: the lock and SQLite's WAL mode depend on local file locks. The worker creates each missing level with mode `0700` at startup; an existing folder keeps its mode. `''`: no own files, no cache file and no cache exchange; an enabled recorder with provider `sqlite` then fails the load. See [The own folder](local-databases.md#the-own-folder). |
| `shared_dir` | `str` | `''` | The folder every worker of one cluster sees: registry cards (provider `filesystem`), sealed history, archives, merge results. `''` means the same folder as `dir`, which is right for workers on one host. |

```yaml
storage:
  dir: /var/lib/drakkar              # per-worker volume
  shared_dir: /mnt/cluster           # one volume all workers mount
```

---

## Flight recorder (`recorder:`)

The recorder is the event log behind the UI, History, Trace and the task views. It is separate from the UI. Two stores can exist: a **memory store** that serves the UI, and a **durable store** (`recorder.enabled`, provider `sqlite` or `postgres`).

| `ui.enabled` | `recorder.enabled` | Recorder object | Memory store | Durable store |
|---|---|---|---|---|
| off | off | none | no | no: main loop only |
| on | off | yes | yes | no: the UI shows the memory window |
| off | on | yes | no | yes |
| on | on | yes | yes | yes |

The memory store exists to serve the UI. Without it, the recorder still hands every row to the file or Postgres queue; it inserts nothing in memory and evicts nothing, and the memory gauges stay `0`. `recorder.capture.events: false` keeps its meaning: no event rows anywhere. With `recorder.enabled: false`, `trace?history=1` reads no folder and no table: it answers the memory rows, and the provider blocks load and do nothing. See [Recent Events in Memory](recent-events.md) and, for provider `postgres`, [Recorder History in Postgres](recorder-postgres.md).

`recorder.enabled` defaults to `true` and `provider` to `sqlite`; no value is derived. The provider with its needs:

- `sqlite` writes recorder files into [`storage.dir`](#storage-and-shared). An enabled recorder with this provider and an empty `storage.dir` fails the load:

    ```
    recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, or set recorder.enabled to false
    ```

- `postgres` writes one partitioned table. It needs `connections.postgres.<recorder.postgres.connection>`; with the entry missing the load fails with `recorder.postgres.connection '<name>' is not defined under connections.postgres`. `storage.dir` may be empty.

The config summary line shows `recorder=sqlite`, `recorder=postgres` or `recorder=off`.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Durable recording in the store the provider names. Off, nothing is written to a file or a table; with the UI on, the recorder still holds the recent events in memory for it. |
| `provider` | `'sqlite'` \| `'postgres'` | `'sqlite'` | one of the two | Where durable recording goes. |
| `flush_interval` | duration | `5s` | whole seconds, >= `1s` | The tick of the memory stage and of the file or Postgres stage. |
| `max_buffer_rows` | `int` | `50000` | >= 1000 | The intake buffer and, with a durable store, its queue each hold at most this many rows. When one is full, its oldest rows are dropped and counted. |

### Memory store (`recorder.memory`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `window` | duration | `2m` | whole seconds, `10s` to `24h` | Keep recorder events in memory this long. |
| `rows_per_executor` | `int` | `3000` | `100` to `1000000` | Row cap per executor slot: the store holds at most this many rows times `executor.max_executors`. The cap does not scale with the window; a longer window needs a larger value (rows per executor per window is about 4 x window / average task seconds). |
| `max_size` | size | `256MiB` | `16MiB` to `512MiB` | Budget for the data in the store (pages in use). It bounds the data, not the memory the store holds. Plan up to about 600 MiB of RAM at the default. |

### What is recorded (`recorder.capture`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `events` | `bool` | `true` | | Write processing events (consumed, task_started, task_completed, task_failed, ...). Off, no event row goes anywhere, runtime health events included: the live WebSocket still streams, but History, Trace and task queries return nothing. |
| `config` | `bool` | `true` | | Write the worker configuration to the `worker_config` table, so every recorder file names the worker and cluster that wrote it. |
| `output` | `bool` | `true` | | Keep subprocess stdout/stderr content in task events. Off drops output content from all of them. Disable to save space when output is large or not needed. |
| `stdin` | `bool` | `false` | | Keep each task's stdin (capped at `max_stdin`) in the `task_started` event metadata. Off by default: on a high-fan-out workload stdin is the largest payload the recorder would write. Failed tasks always keep their stdin (capped) on the `task_failed` event. |
| `max_stdin` | size | `64KiB` | >= `0` | Size cap for kept stdin. `0` = no limit. A cut is flagged `stdin_truncated` in the event metadata. |
| `min_event_duration` | duration | `0` | >= `0` | Shortest task duration whose task event is written at all. `0` writes everything. |
| `min_output_duration` | duration | `500ms` | >= `0` | Shortest task duration whose args, stdout and stderr go into the written event. Faster tasks keep their row without output data. |
| `env_vars` | `list[str]` | `[]` | | Environment variable names captured into `worker_config.env_vars_json`, for deployment metadata such as `GIT_SHA`, `DEPLOY_ENV`, `K8S_POD_NAME`. |

### Annotations (`recorder.annotations`)

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Record handler [annotations](annotations.md) (diagnostic records attached to a window, message, or task from inside a hook) as rows in the `events` table. |
| `max_size` | size | `16KiB` | >= `0` | Cap for one annotation payload; an oversize record is dropped whole rather than truncated. `0` = no limit. See [Annotations](annotations.md) for the budgets and the drop policy. |
| `max_size_per_call` | size | `256KiB` | >= `0` | Total annotation size one hook invocation may add. `0` = no limit. |
| `max_log_size` | size | `2KiB` | >= `0` | Cap on the payload copy written to the warning log when an annotation is dropped. `0` = no limit. |

### Provider `sqlite` (`recorder.sqlite`)

Used only with provider `sqlite`; with provider `postgres` the block loads and is ignored.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `rotation_interval` | duration | `1h` | whole hours, >= `1h` | How often to rotate the SQLite file. On rotation a new timestamped file is created and the old one is sealed; a background job moves it into `<storage.shared_dir>/history/<cluster>/` (see [Sealing](local-databases.md#sealing-and-the-history-folder)). Every rotation tick also retries failed moves. Rotation itself deletes nothing: see [Archiving](local-databases.md#archiving). |
| `max_flush_retries` | `int` | `3` | >= 1 | How many times a flush batch is re-queued on a transient `OperationalError` (database is locked, disk I/O error) before the batch is dropped. On a drop `drakkar_recorder_flush_batches_dropped_total` ticks; on each retry `drakkar_recorder_flush_retries_total` ticks. |
| `state_snapshots` | `bool` | `true` | | Periodically snapshot worker state (uptime, partitions, pool utilization, queue depth, counters) to the `worker_state` table. |
| `state_interval` | duration | `10s` | whole seconds, >= `1s` | Time between worker state snapshots. |
| `max_queue` | size | `128MiB` | `16MiB` to `1GiB` | The byte bound of the file queue: the characters of the stdout, stderr, metadata and args fields of the rows that wait for the file. Past it the oldest rows leave the file queue (the memory store keeps them) and count on `drakkar_recorder_dropped_events_total`; one row larger than the bound is dropped alone. A failed write puts its rows back at the front and the same trim runs. The file loop starts early at half of it. See [Buffer Mechanics](observability.md#buffer-mechanics). |

### History search (`recorder.search`)

Budget of one Message Trace request with `history=1`, which searches the sealed files in the history folder of the worker's own cluster, newest first. With provider `postgres` the request searches the [Postgres table](recorder-postgres.md#the-history-search) instead: `timeout` is its time limit (at most `1h` on the server) and `max_files` does not apply. With `recorder.enabled: false` the search answers the memory rows. See [Cross-Worker Trace](observability.md#cross-worker-trace).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `max_files` | `int` | `256` | >= 1 | Most sealed history files one search opens, newest first. Used only with provider `sqlite`. |
| `timeout` | duration | `10s` | > `0` | Time limit for one search. With provider `sqlite` the search stops and returns what it found. With provider `postgres` a search past the limit answers 503 and returns no rows. |

### Archive (`recorder.archive`) {#history}

What happens to recorder history after a worker is done with a file. Used only with provider `sqlite`; with provider `postgres` the block loads and does nothing. See [Archiving](local-databases.md#archiving).

The archive has two scopes:

- **Cluster scope**: `autodiscover.enabled` and `storage.shared_dir` are set. One worker of the cluster runs the pass at each slot, chosen by the election, over every worker's sealed files in `<storage.shared_dir>/history/<cluster>/`. The archive is named `<cluster>-<from>__<to>.db.gz`.
- **Own scope**: otherwise. Each worker runs the pass at the same slots with no election and no lease, over its own sealed files only, in `<dir>/history/<cluster>/`, where `<dir>` is `storage.shared_dir` or else `storage.dir`. A file is this worker's when its name minus the time stamp equals the sanitized worker name. The archive is named `<cluster>-<worker>-<from>__<to>.db.gz`, retention deletes only archives with that prefix, and `lease` is not used. Give each worker a stable `worker.name`: files of earlier runs of a worker with a generated name are never matched again.

The pass runs when `recorder.enabled` is true, the provider is `sqlite`, `archive.enabled` is true and `storage.dir` is set. It does not depend on `ui.enabled`.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Fold sealed recorder files into one compressed archive per window, then delete the files a pass merged. `false` disables the pass entirely: no sealed recorder file and no merge result is deleted automatically. |
| `window` | duration | `1d` | whole hours, >= `1h`, and >= `recorder.sqlite.rotation_interval` | Width of one archive window (UTC epoch-aligned). |
| `due_after` | duration | `2d` | whole hours, >= `1h`, and >= `recorder.sqlite.rotation_interval` | A window is archived this long after it ends, so late files of that window are still in it. |
| `retention` | duration | `30d` | whole days, >= `0`; when non-zero, `retention >= due_after + window` | How long archives are kept before deletion. Defaults to 30 days because nothing else reclaims an archive. `0` keeps them for ever and logs a `recorder_archives_unbounded` warning at startup. |
| `merged_retention` | duration | `32d` | whole days, >= `0` | Merge results from the UI (`<dir>/merged/`) are deleted by the archive pass after this long, so only while `enabled` is true. `0` keeps them for ever. |
| `interval` | duration or `null` | `null` | whole seconds, at least `30s`; at most one of `interval` and `cron` | Time between archive slots, aligned to the epoch. With neither `interval` nor `cron`, a slot every `1h`. See [Archive slots](local-databases.md#archive-slots). |
| `cron` | `str` or `null` | `null` | a five-field UTC cron expression in the [accepted subset](periodic-tasks.md#cron) | The archive slots as cron fire times, instead of `interval`. |
| `lease` | duration | `1m` | whole seconds, `40s` to `24h` | The lease of an archive slot in the cluster scope. The holder refreshes it while the pass runs; a lease nobody refreshed this long belongs to a dead archiver and is taken over. Used by every [election provider](local-databases.md#the-election-provider). Not used in the own scope. |
| `sweep_after` | duration | `2h` | whole seconds, >= `1m` | An archive work file in the history folder (a merge or compress file, a sealing copy on its way in, an orphan meta file) older than this belongs to a dead worker and is deleted by the pass. In the own scope the pass sweeps only this worker's files. |

The load checks the schedule keys in this order, with the prefix `recorder.archive: `: `give at most one of interval and cron`, the [cron messages](periodic-tasks.md#cron-messages), `interval must be a whole number of seconds and at least 30`, `lease must be a whole number of seconds from 40 to 86400`, `sweep_after must be at least 60 seconds`. The cross-field checks name `recorder.archive.window`, `.due_after`, `.retention` and `recorder.sqlite.rotation_interval`.

### Provider `postgres` (`recorder.postgres`)

Used only with provider `postgres`. The full guide, with the table, the partitions and the upkeep, is [Recorder History in Postgres](recorder-postgres.md).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `connection` | `str` | `'main'` | a name under `connections.postgres` | The entry that holds the DSN and the schema. |
| `table_prefix` | `str` | `''` | empty, or `^[a-z][a-z0-9_]{0,23}$` | Empty for the table `<schema>.recorder_events`, else `<schema>.<prefix>_recorder_events`. A bad value fails the load with `recorder.postgres.table_prefix '<value>' must be empty, or a lower-case letter followed by at most 23 lower-case letters, digits or underscores`. |
| `initial_retention` | duration | `1d` | whole hours, `1h` to `168h` | Hours of history the table keeps. Read only when this worker creates the table; after that the value in the table comment rules. |
| `store_completed_output` | `bool` | `false` | | Send the `stdout` and `stderr` columns to Postgres. Off, both are written as `NULL`, except the `stderr` of a `task_failed` row, which is always kept; `stdout_size` and `metadata` stay. |
| `max_output` | size | `64KiB` | `0`, or at least `1KiB` | Longest `stdout` or `stderr` value written, in UTF-8 bytes. A longer value keeps its first and last half, joined by a line that says how many bytes were cut. `0` = no limit. |
| `max_size` | size | `0` | `0`, or at least `64MiB` | Largest total size of the table. The upkeep drops the oldest hours while the table is above it, never the previous hour, the current hour or later ones. `0` = no limit. The worker that runs the upkeep applies its own value, so workers sharing a table should use one value. |
| `batch_rows` | `int` | `5000` | `100` to `50000` | Most rows in one COPY. |
| `max_queue` | size | `128MiB` | `16MiB` to `1GiB` | Most text waiting for Postgres. When full, the oldest rows are dropped and counted. |
| `resilience` | block | see [Resilience](#resilience-block) | | Timeout, retries, backoff and breaker of the recorder calls to Postgres (feature `recorder`). |

### Example

```yaml
recorder:
  enabled: true
  provider: sqlite
  flush_interval: 5s
  memory:
    window: 2m
  capture:
    output: true
    min_output_duration: 1s
    env_vars: [GIT_SHA, DEPLOY_ENV, K8S_POD_NAME]
  sqlite:
    rotation_interval: 1h
  archive:
    window: 1d
    due_after: 2d
    retention: 30d
    interval: 1h            # or cron: '0 * * * *'; give at most one
```

---

## Cache (`cache:`)

A handler-accessible key/value cache with in-memory hot reads, write-behind SQLite persistence, and an optional cross-worker [cache exchange](cache.md#cache-exchange) over HTTP. **Disabled by default** -- when `enabled: false`, `handler.cache` is a no-op stub so handler code can call `self.cache.set(...)` unconditionally without `if` guards.

See [Cache](cache.md) for the full API (`set` / `peek` / `get` / `delete`) and the [periodic loops](cache.md#how-it-flows) (flush, cleanup, exchange).

### Cache Settings

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `false` | | Master switch. When `false`, the cache is a no-op stub. The cache file `<worker_id>-cache.db` lives in the worker's own folder under [`storage.dir`](#storage-and-shared) and is private to the worker; `storage.dir` does not need to be shared for the cache. With `storage.dir: ""` the engine warns (`cache_engine_disabled_no_storage_dir`) and continues without persistence -- in-memory only, and with no cache exchange. |
| `flush_interval` | duration | `3s` | > 0 | Time between runs of the write-behind loop that drains dirty in-memory entries to SQLite. Lower = less data loss on crash; higher = less write amplification. |
| `cleanup_interval` | duration | `1m` | > 0 | Time between runs of the loop that deletes rows whose `expires_at_ms` has passed and refreshes Prometheus DB-size gauges. |
| `max_memory_entries` | `int` | `10000` | >= 0 | Cap for the in-memory LRU. `0` = unbounded (a startup warning fires so the choice is visible in logs). The DB is the source of truth, so eviction never loses data -- evicted entries re-warm on the next `get()`. |

```yaml
cache:
  enabled: true
  flush_interval: 3s
  cleanup_interval: 1m
  max_memory_entries: 10000           # 0 = unbounded (warns)
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
| `interval` | duration | `30s` | > 0 | Time between exchange cycles. |
| `page_size` | `int` | `500` | 1-1000 | Maximum rows in one page pulled from a peer. A pull asks for pages until the peer answers `more: false`. |
| `timeout` | duration | `5s` | > 0 | Time limit for one request to a peer; the connect timeout is 2 seconds. One slow peer cannot block the rest. |
| `cycle_deadline` | duration or `null` | `null` | >= `100ms` and `<` `interval` | Hard wall-clock cap on a single cycle. `null` derives `interval * 0.9`. Must be strictly less than `interval` -- config load fails otherwise so the misconfiguration surfaces at startup. Peers still pulling or waiting when it fires are cut, and are not counted as failures. |
| `max_concurrent_peers` | `int` | `4` | >= 1 | Peers pulled from in parallel. The others wait for a free slot within the same cycle. |

```yaml
cache:
  exchange:
    enabled: true
    interval: 30s
    page_size: 500
    timeout: 5s
    cycle_deadline: null              # null = interval * 0.9
    max_concurrent_peers: 4
```

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

## Logging (`logging:`)

Structured logging configuration via structlog.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `level` | `str` | `'INFO'` | | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `format` | `str` | `'json'` | `json`, `ecs` or `console` | Output format. `json` produces machine-readable structured logs with flat keys. `ecs` produces JSON in the Elastic Common Schema, with the framework's own fields under `drakkar` (see [ECS Output](observability.md#ecs-output)). `console` produces colorized human-readable output for local development. |
| `output` | `str` | `'stderr'` | | Log destination: `stderr`, `stdout`, or a file path. File paths support `{worker_id}` and `{cluster_name}` template variables. Parent directories are created automatically. |
| `min_task_duration` | duration | `500ms` | >= 0 | Shortest task duration that emits a `slow_task_completed` or `slow_task_failed` log message. `0` logs all tasks. |
| `message_identity.enabled` | `bool` | `false` | | Write the identity of the incoming message into every log line of its processing, and add it to each task as a label. See [Message Identity](observability.md#message-identity). |
| `message_identity.name` | `str` | `'message_id'` | lower-case identifier, at most 64 characters, not a field the framework writes | The log field and task label name. |
| `message_identity.source` | `str` | `'key'` | `key`, `header:<name>` or `value:<path>` | Where the value is read: the message key, a header (letter case ignored), or a field of the JSON value. |
| `message_identity.max_values` | `int` | `100` | 1 to 10000 | The most values a line that covers several messages carries. |

```yaml
logging:
  level: INFO
  format: json
  output: stderr  # or stdout, or /var/log/drakkar/{worker_id}.log
  min_task_duration: 500ms
```

---

## Runtime health (`runtime_health:`)

Event-loop lag monitoring and stall introspection — see the dedicated
[Runtime Health](runtime-health.md) page for how the monitor works and
how to read its output.

```yaml
runtime_health:
  enabled: true
  tick: 250ms
  warn_lag: 100ms
  stall: 1s
  max_stall_stacks: 10
  sample_interval: 10s
  history_window: 15m
  max_episode: 5m
  stack_probe_enabled: false
  stack_probe_interval: 1s
```

---

## API server (`api:`)

The HTTP server of the worker: one listener, one port. It is a feature of its own, so a worker can run with no listener at all.

| Route | Needs |
|---|---|
| `/healthz`, `/readyz`, `/api/v1/peer/cache/changes` | `api.enabled` |
| `/api/v1/agent` and the routes below it | `api.enabled` and `agent.enabled` |
| everything else (`/ws`, the other `/api/v1/*` routes, `/api-docs`, `/docs`, the pages) | `api.enabled` and `ui.enabled` |

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Run the HTTP server: `/healthz`, `/readyz` and the peer route. Off means no listener, so nothing answers the Kubernetes probes (the worker logs a warning about missing probes), and `ui.enabled`, `autodiscover.enabled` and `agent.enabled` must all be false. |
| `host` | `str` | `'127.0.0.1'` | | Bind address. Default `127.0.0.1` (localhost only). Use `0.0.0.0` to expose on all interfaces. Auth is opt-in regardless of host: when binding to a non-loopback address inside anything other than a fully trusted private network, set `ui.auth_token`. |
| `port` | `int` | `8080` | 1--65535 | Port for the HTTP server (FastAPI). |
| `public_url` | `str` | `''` | | External URL for this worker's HTTP server. Peers and browsers reach the worker through it, and the registry card carries it. Empty derives `http://<outbound ip>:<port>/`. Useful behind load balancers or Kubernetes ingresses. |

The load checks these rules right after the source check, in this order, and fails at the first one that is broken:

```
ui.enabled needs api.enabled; set ui.enabled to false, or set api.enabled to true
autodiscover.enabled needs api.enabled: peers reach a worker through its API
agent.enabled needs api.enabled: the agent routes are served by the API server
```

The same three checks run again after `on_startup`.

A worker with `ui.enabled: false` and `api.enabled: true` still opens the API port for health and peers. To open no port at all, set `api.enabled: false` too.

```yaml
api:
  host: 0.0.0.0
  port: 8080
  public_url: https://drakkar-ui.example.com/worker-1
```

---

## Operator UI (`ui:`)

The `ui` section covers the user-facing UI: the pages, the drakkar-ui bundle fetch, the debug and data routes, the `/ws` live stream and the docs site. It needs `api.enabled`. Off, those routes answer 404, no bundle is fetched, and the recorder keeps no memory store. The [web UI dashboard](observability.md#operator-ui) and its presentation settings are here; the event log it reads is the [flight recorder](#flight-recorder-recorder).

### Authentication

The UI's auth is **opt-in by default**. With `ui.enabled: true` and `ui.auth_token` empty (the default), the worker emits a structured `ui_unauthenticated` warning at startup naming the bound host:port and the two opt-in paths (YAML key + env var), then continues starting normally. No endpoint stops a worker, replays Kafka messages, mutates sinks, or commits offsets, and Drakkar is intended to run inside a private contour (VPC / internal cluster / operator-only ingress), so the framework treats "unauthenticated + warned" as a reasonable starting point rather than a misconfiguration.

**Most endpoints are read-only, but not all.** Three have real side effects, and each can be closed independently of `auth_token`:

| Endpoint | Effect | Switch |
|----------|--------|--------|
| `POST /api/v1/debug/probe` | Runs caller-supplied bytes through the live handler and the real executor subprocess pool, competing with production traffic for executor slots. (It writes no sinks, recorder rows, cache entries, or offsets — see [Message Probe](observability.md#operator-ui).) | `ui.probe.enabled: false` |
| `POST /api/v1/debug/merge` | Writes a new `merged-<ts>-<token>.db` into `<storage.shared_dir>/merged/` (`<storage.dir>/merged/` with no shared folder). The archive pass deletes it after `recorder.archive.merged_retention` (default `32d`); with archiving off, or with that set to `0`, nothing reclaims it, so repeated calls grow unbounded. | `ui.merge_enabled: false` |
| `DELETE /api/v1/debug/files/{path}` | Deletes one merge result, `merged/<name>.db`, from the [Databases view](databases-view.md). Sealed files, archives and every other file are never deleted from the UI. | `ui.delete_enabled: false` (the default) |

The probe and the merge default to `true`; the delete defaults to `false`. Each returns `403` with `{"error": "..."}` naming the config key when switched off. The switches are deliberately independent of `auth_token` in both directions: a deployment that cannot set a token can still close the endpoints, and a deployment that has one can still close them as defence in depth — a valid token is not the same as consent to burn executor slots on demand. The startup warning names whichever of the three is still enabled, so an operator who has already closed them is not warned about exposure they do not have.

To require auth on the protected endpoints (database download, merge, message probe) and the WebSocket live-event stream, set a strong token:

- **Generate a strong value:** `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- **Configure via YAML:** set `ui.auth_token: <value>` in your config.
- **Or via environment:** export `DK_UI__AUTH_TOKEN=<value>` (overrides YAML when both are set).

When the token is set, protected endpoints reject requests without a matching `Authorization: Bearer <token>` header (or `?token=<token>` query parameter); the WebSocket additionally validates the `Origin` header against `allowed_origins` (or the request's `Host` header), and so do the three cross-origin reads of the UI fan-out. The routes under `/api/v1/peer/` are never protected: other workers call them without a token for the [cache exchange](cache.md#cache-exchange). See [Security posture](security.md#the-public-peer-route). Comparison uses `secrets.compare_digest` for timing-side-channel safety; leading/trailing whitespace in the configured token is stripped on load (a `auth_token: " "` of only spaces is treated as empty and the warning still fires).

The unauthenticated-startup warning runs at startup, before the recorder and API server are constructed.

### UI settings

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | needs `api.enabled` | Serve the UI: the pages, the bundle fetch, the debug and data routes, `/ws` and the docs. Off: those routes answer 404, no bundle is fetched, and the recorder keeps no memory store. |
| `auth_token` | `str` | `''` | | Bearer token for sensitive endpoints (database download, merge, message probe) **and** for the WebSocket live-event stream at `/ws`. **Empty (the default) disables auth entirely**: every endpoint is reachable without credentials and the WebSocket skips both token and Origin checks. This is intentional: the UI is intended for private-network deployments, and a startup warning (`ui_unauthenticated`) names the unauthenticated posture in logs along with any side-effecting endpoint still enabled. When set, protected HTTP endpoints require `Authorization: Bearer <token>` or `?token=<token>`; WebSocket connections without a valid token are closed with code 4401. Comparison uses `secrets.compare_digest`. Leading and trailing whitespace is stripped on load (a token of only spaces is treated as empty). Read-only pages are always accessible. `/healthz`, `/readyz` and the routes under `/api/v1/peer/` (the [cache exchange](cache.md#cache-exchange)) never need the token. With a token set, list every worker origin in `allowed_origins` so the UI can read the other workers of the cluster. |
| `allowed_origins` | `list[str]` | `[]` | | Explicit allowlist of WebSocket `Origin` header values, consulted on every handshake whether or not `auth_token` is set. The same rule decides the cross-origin reads (CORS) of `/api/v1/debug/trace`, `/api/v1/debug/trace-by-label` and `/api/v1/debug/databases`, which the UI sends to every worker of the cluster; with `auth_token` set, list the origin of every worker here, or those reads fail and the UI shows the other workers as unreachable (see [Cross-origin reads](security.md#cross-origin-reads)). Non-empty list = strict allowlist; any Origin not in the list is rejected with close code 4403. Empty list + non-empty `auth_token` = same-origin fallback: Origin host must match the `Host` header. Empty list + empty `auth_token` = the cross-origin handshakes the cluster view needs are accepted, except a page that is not itself on loopback reaching a worker over loopback; no peer worker is ever at the browser's own loopback address. Comparison is case-insensitive and normalizes default ports. A missing Origin header (non-browser clients) is always accepted. |
| `max_rows` | `int` | `5000` | >= 100 | Maximum number of rows returned to the web UI in list views. |
| `probe.enabled` | `bool` | `true` | | Serve `POST /api/v1/debug/probe`. The probe runs caller-supplied bytes through the live handler and the real executor subprocess pool, so it competes with production traffic for executor slots. `false` serves `403` instead, independently of `auth_token` (see [Authentication](#authentication)). Probes never write sinks, recorder rows, cache entries, or offsets. |
| `merge_enabled` | `bool` | `true` | | Serve `POST /api/v1/debug/merge`. Each call writes a new `merged-<ts>-<token>.db` into `<storage.shared_dir>/merged/`. The archive pass deletes merge results after `recorder.archive.merged_retention`; with archiving off, or with that set to `0`, nothing reclaims them. `false` serves `403` instead, independently of `auth_token`. |
| `delete_enabled` | `bool` | `false` | | Serve `DELETE /api/v1/debug/files/{path}`, which deletes one merge result (`merged/<name>.db` in the shared folder) from the [Databases view](databases-view.md#what-the-view-can-change). Off: the route answers `403`, and the view offers no delete. Only merge results can be deleted; sealed files, archives and every other file are never deleted from the UI. While this worker sends the file or merges from it, the route answers `409`. Independent of `auth_token`. |
| `kafka_read_enabled` | `bool` | `true` | | Serve `GET /api/v1/debug/kafka/*`: ad-hoc reads of the configured topics (source, DLQ, and each Kafka sink by instance name; never an arbitrary topic). Reads use `assign()`-only consumers that join no consumer group and commit no offsets, so they are invisible to the pipeline. `false` serves `403` instead, independently of `auth_token`. When the Kafka security of any readable topic is not PLAINTEXT while `auth_token` is empty, startup logs a warning naming the exposed aliases. See [Kafka Read API](kafka-read.md). |

### Duration Thresholds

These thresholds control which events are recorded, logged, and streamed. See [Duration Thresholds](observability.md#duration-thresholds) for detailed behavior and [Performance Tuning](performance.md) for recommendations. They help reduce noise in high-throughput systems where most tasks complete quickly. The WebSocket threshold is `ui.ws_min_duration`, the log threshold is [`logging.min_task_duration`](#logging-logging); the persistence thresholds are `recorder.capture.min_event_duration` and `recorder.capture.min_output_duration`.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `ws_min_duration` | duration | `500ms` | >= `0` | Shortest task duration broadcast via WebSocket to the live UI. Fast tasks that complete under this threshold are invisible in the live view (reduces UI noise). Failed tasks always appear regardless. `0` shows all tasks. |

### Prometheus Integration

These settings add clickable Prometheus graph links to the dashboard.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prometheus.url` | `str` | `''` | Base URL of your Prometheus server (e.g., `http://prometheus:9090`). If empty, no Prometheus links are shown in the UI. |
| `prometheus.rate_interval` | `str` | `'5m'` | Rate interval used in PromQL `rate()` expressions for dashboard links (e.g., `1m`, `5m`, `15m`). |
| `prometheus.worker_label` | `str` | `''` | PromQL label filter for worker-scoped queries. Supports template variables: `{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`. If empty, defaults to `instance="{hostname}:{metrics_port}"`. Example: `worker_id="{worker_id}"`. |
| `prometheus.cluster_label` | `str` | `''` | PromQL label filter for cluster-wide queries. Supports the same template variables. Example: `cluster="{cluster_name}"`. If empty, cluster-wide links are not shown. |

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
| `durations` | `list[duration]` | `[15s, 1m, 5m, 15m]` | Preset pause durations offered as one-click buttons on the Live page, each a whole number of seconds. Each preset must sit in the same `1s` to `1h` range the API enforces; the API itself accepts any duration in that range regardless of the presets. |
### Message Probe (`ui.probe:`)

The switch of the Message Probe (`enabled`, default `true`) and the write caps for its
user-defined details tab (`max_writes`, default `10000`; `max_total_size`, default `5MB`) — see
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
[Operator Docs Site](ui-docs.md) page.### UI Release (`ui.release:`)

Decoupled drakkar-ui bundle fetching. The UI ships as its own versioned bundle (the separate drakkar-ui repo, published to GitHub Releases); when enabled, the worker resolves that bundle (cache → fetch) and serves it. A fetch failure is never fatal: release tags are immutable, so a bundle downloaded once serves from the shared cache on every later start — including offline ones. When nothing can be resolved at all the worker serves its JSON API and WebSocket without pages, and page requests answer 503 with how to supply a bundle; there is no built-in HTML fallback. See [Managing the UI bundle cache](observability.md#managing-the-ui-bundle-cache).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `true` | Resolve and serve the drakkar-ui bundle. Off, the worker fetches nothing and reads no cache: the JSON API, the health probes and the event WebSocket keep working, and page requests answer 503. |
| `repo` | `str` | `'wlame/drakkar-ui'` | The `owner/name` GitHub repo that publishes UI bundles. Empty disables fetching — only an already-cached bundle is served, which is how an air-gapped deployment pins itself to a bundle staged with the `drakkar-ui` CLI. |
| `pinned_version` | `str` | `''` | Known-good UI release tag this backend is built against (e.g. `v1.2.0`); the contract is API-major compatible. Empty means "no pinned version". |
| `cache_dir` | `str` | `''` | Bundle cache root override. Empty uses the per-user cache dir (`$XDG_CACHE_HOME/drakkar/ui`, falling back to `~/.cache/drakkar/ui` — so co-located workers share one cache). |
| `check_update` | `bool` | `true` | Resolve the latest release tag on startup instead of only the pinned version. Already-cached versions are never re-downloaded — release tags are immutable. |

```yaml
ui:
  enabled: true
  max_rows: 5000
  ws_min_duration: 500ms
  prometheus:
    url: http://prometheus:9090
    rate_interval: 5m
    worker_label: 'worker_id="{worker_id}"'
    cluster_label: 'cluster="{cluster_name}"'
  custom_links:
    - name: Grafana Dashboard
      url: http://grafana:3000/d/drakkar?var-worker={worker_id}
    - name: Kibana Logs
      url: http://kibana:5601/app/discover#/?_a=(query:(match_phrase:(worker_id:'{worker_id}')))
  link_bases:
    jira: 'https://jira.internal.example.com'
    jenkins: 'https://jenkins.internal.example.com'
  custom_renderers_path: /etc/drakkar/renderers.js
  release:
    enabled: true
    repo: wlame/drakkar-ui
    pinned_version: ""
    check_update: true
```

---

## Agent (`agent:`) {#agent-agent}

The agent routes under `/api/v1/agent`: a read-only surface that explains the worker to an AI agent (the framework knowledge, the effective config, the metrics, the live state and rule-based findings). The API server serves them, so they work with `ui.enabled: false`. The full guide is [Agent Surface](agent.md).

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `false` | needs `api.enabled` | Serve the agent routes. Off, every agent route answers 404 `{"error": "the agent surface is off; set agent.enabled to true"}`. |
| `auth_token` | `str` | `''` | secret | When set, every agent route needs `Authorization: Bearer <token>` (the scheme in any letter case); a missing or wrong token answers 401 `{"error": "agent token required"}`. The comparison takes constant time. `ui.auth_token` and the `?token=` parameter do not apply to these routes. Empty: no token is needed, and with `enabled: true` the worker logs `agent_surface_unauthenticated` (warning, category `agent`) at start. |
| `include_payloads` | `bool` | `false` | | Let `GET /api/v1/agent/state` add `stderr_tail` (the last 2048 bytes of stderr) and `args` to each entry of `recent_failures`. Off, no agent answer carries task output. |

With `enabled: true` and `api.enabled: false` the load fails with `agent.enabled needs api.enabled: the agent routes are served by the API server`. The check runs after the `ui` and `autodiscover` checks of the [API server](#api-server-api), and again after `on_startup`. The config summary line shows `agent=on` or `agent=off`.

```yaml
agent:
  enabled: true
  # auth_token comes from DK_AGENT__AUTH_TOKEN, so the token stays out of the file
```

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
