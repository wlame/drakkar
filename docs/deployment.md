# Deployment

## Network exposure

Before anything below: **a Drakkar worker belongs on a trusted, private
network.** It serves up to three HTTP ports — the operator UI
(`ui.port`, default 8080), the optional webapp ingress
(`sources.http.port`, default 8090) and the optional Prometheus exporter
(`metrics.port`, default 9090) — and none of them is designed to face an
untrusted network. Bind them where only your own infrastructure can reach
them, and put a reverse proxy in front of anything that must travel
further.

Only `/healthz` and `/readyz` on the UI port are meant to be reachable
without a token, because kubelet needs them.

See [Security posture](security.md) for what Drakkar's own controls do and
do not cover.

This page covers operational topics that apply when running Drakkar in a
production environment: Kubernetes probes, rolling restarts, and the
interaction between the UI server and cluster health checks.

## A shared folder plus a shared Postgres

This layout fits a deployment that already runs Postgres. The workers keep
their sealed history in a shared folder, as with the defaults. They find each
other, and choose the one worker that runs each slot of a cluster periodic
task (the archive pass included), through tables in Postgres instead of files in the folder. Both choices use
the same `connections.postgres.main` entry and so share one pool.

```yaml
storage:
  dir: /var/lib/drakkar
shared:
  dir: /mnt/drakkar-shared

connections:
  postgres:
    main:
      schema: drakkar
      manage_schema: true
      pool_min: 1
      pool_max: 4

fleet:
  registry:
    provider: postgres
    connection: main

periodic:
  election:
    provider: postgres
    connection: main
```

Set the connection string in the environment of every worker, for example
from a Kubernetes Secret, not in the YAML file:

```bash
DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:...@db.internal/drakkar'
```

What to check:

- Every worker of the fleet uses the same registry provider and database.
  Every worker of one cluster uses the same election provider. Workers on
  different providers do not see each other's cards or leases.
- The registry needs `ui.enabled: true`.
- The shared folder still holds the sealed recorder files and the archives.
  Only the cards and the leases of the cluster tasks move to Postgres.
- `manage_schema: true` lets the first worker that starts create the schema
  and both tables. Workers that start together take turns through an advisory
  lock. Every worker repeats the idempotent DDL at its first provider start,
  also when the objects exist, so the role of every worker needs `CREATE` on
  the database (for `CREATE SCHEMA IF NOT EXISTS`) and `CREATE` on the schema
  (for the tables and the indexes). The role must also own the table
  `fleet_workers`, because its two `CREATE INDEX IF NOT EXISTS` statements
  check ownership before they skip an index that exists. The simplest case is
  to let the worker role create the objects. For tables that another role
  created, set `manage_schema: false`. `task_locks` needs no ownership.
- With an operator-managed schema, set `manage_schema: false` and run the
  command once, as a role that has the DDL rights:

  ```bash
  drakkar-schema postgres --schema=drakkar | psql "$ADMIN_DSN"
  ```

  The worker role then needs `USAGE` on the schema and `SELECT`, `INSERT`,
  `UPDATE` and `DELETE` on `fleet_workers` and `task_locks` only. A worker
  that finds a table missing fails the provider's start with a message that
  names this command and tries again at the next heartbeat (registry) or the
  next slot of a cluster task (election).
  For the recorder table of `persistence: postgres`, add `--recorder` and,
  as needed, `--table-prefix=<p>` and `--retention-hours=<n>`; see
  [A worker with no disk and a central history](#a-worker-with-no-disk-and-a-central-history).
- During a database outage the pipeline keeps running. The workers list
  keeps its last snapshot, cluster task slots are skipped as
  `election_failed`, an archive pass stops without publishing, and both
  providers recover when the server is back. Watch `drakkar_provider_breaker_open`
  and `drakkar_provider_calls_total` (see [Observability](observability.md)).

More: [Fleet registry: postgres](fleet.md#postgres),
[The election provider](local-databases.md#the-election-provider),
[Security: provider credentials](security.md#provider-credentials).

## A shared folder plus a shared Redis

This layout fits a deployment that already runs Redis. The workers keep their
sealed history in a shared folder, as with the defaults. They find each other,
and choose the one worker that runs each slot of a cluster periodic task
(the archive pass included), through keys in Redis instead of files in the folder. Both choices use the same
`connections.redis.main` entry and so share one client.

```yaml
storage:
  dir: /var/lib/drakkar
shared:
  dir: /mnt/drakkar-shared

connections:
  redis:
    main:
      key_prefix: "drakkar:"

fleet:
  registry:
    provider: redis
    connection: main

periodic:
  election:
    provider: redis
    connection: main
```

Set the URL in the environment of every worker, not in the YAML file. The
database number is the path of the URL:

```bash
DK_CONNECTIONS__REDIS__MAIN__URL='redis://drakkar-fleet:...@redis.internal:6379/3'
```

What to check:

- Every worker of the fleet uses the same registry provider, and the same
  server, database number and `key_prefix`. Every worker of one cluster uses
  the same election provider. Workers on different providers or different
  databases do not see each other's cards or leases.
- The registry needs `ui.enabled: true`.
- The shared folder still holds the sealed recorder files and the archives.
  Only the cards and the leases of the cluster tasks move to Redis.
- Use a **database number of its own** for the fleet, so that the keys of the
  providers stay apart from other data. Use a Redis user of its own too, with
  the rights `~drakkar:* +@read +@write +@scripting +@connection +time` (the
  pattern follows `key_prefix`) and the `default` user off where no other
  client needs it. These are category rights, so they allow more commands than
  the providers send, for example `FLUSHDB` and `KEYS`, and the key pattern
  does not limit those two. A Redis ACL does not limit databases either. ACL
  users need Redis 6.0 or later; on Redis 5.0, use `requirepass` and a database
  number of its own. A complete `ACL SETUSER` line is in
  [Security](security.md#provider-credentials).
- The server is Redis 5.0 or later, standalone or a primary behind a fixed
  address. Redis Cluster is not supported. Use `rediss://` in the URL for TLS.
  The client then verifies the certificate of the server against the
  system's default certificate store and checks that the certificate names
  the host of the URL; this cannot be switched off. Put a private
  certificate authority into that store, or set `SSL_CERT_FILE` or
  `SSL_CERT_DIR`, which replace the default locations of the store. A URL
  with query options (`?...`) is refused when the provider starts, and so is
  a URL with a space, a control character or a non-ASCII character: remove a
  trailing newline from a URL read from a secret file. Write the special
  characters of a password as percent escapes (see the URL rules in
  [Fleet registry](fleet.md#redis)).
- A Redis server that restarts without its data loses the cards and the
  leases, and so does a flush of the database. Eviction can delete them too:
  every key of the providers has a time to live, so any `volatile-*` and any
  `allkeys-*` `maxmemory` policy can evict a live card or a lease. A database
  number of its own does not protect them, because the memory limit and
  eviction cover the whole server. The workers publish their cards again at
  the next heartbeat. An archive pass that held the lease finds it gone at
  its next refresh or check, stops without publishing, and the next slot
  takes the scope. That pass logs `recorder_archive_fenced` with `reason=taken_over`,
  whose detail says that another worker took the archive lock over, although
  no worker did: the provider can only answer that the pass no longer holds
  the lease. Under the default policy, `noeviction`, a full server evicts
  nothing and answers a script that needs more memory with an error that
  names `OOM`. That error is final and counts as a failed operation for the
  breaker.
- During a Redis outage the pipeline keeps running. The workers list keeps its
  last snapshot, cluster task slots are skipped as `election_failed`, an
  archive pass stops without publishing, and both providers recover when the
  server is back. Watch `drakkar_provider_breaker_open` and
  `drakkar_provider_calls_total` (see [Observability](observability.md)).

More: [Fleet registry: redis](fleet.md#redis),
[The redis election provider](local-databases.md#the-redis-election-provider),
[Security: provider credentials](security.md#provider-credentials).

## A shared folder plus Consul and Postgres

This layout fits a deployment that already runs Consul and Postgres. The
workers keep their sealed history in a shared folder, as with the defaults.
They find each other through keys in Consul, and they choose the one worker
that runs each slot of a cluster periodic task (the archive pass included)
through a table in Postgres. The two
features are chosen on their own, so each names its own connection; the
registry uses `connections.consul.main` and the election uses
`connections.postgres.main`.

```yaml
storage:
  dir: /var/lib/drakkar
shared:
  dir: /mnt/drakkar-shared

connections:
  consul:
    main:
      address: http://127.0.0.1:8500   # the agent of the worker's host
      key_prefix: drakkar
  postgres:
    main:
      schema: drakkar
      manage_schema: true

fleet:
  registry:
    provider: consul
    connection: main

periodic:
  election:
    provider: postgres
    connection: main
```

Set the token and the connection string in the environment of every worker,
for example from a Kubernetes Secret, not in the YAML file:

```bash
DK_CONNECTIONS__CONSUL__MAIN__TOKEN='...'
DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:...@db.internal/drakkar'
```

What to check:

- Every worker of the fleet uses the same registry provider, and the same
  Consul cluster, datacenter and `key_prefix`. Every worker of one cluster uses
  the same election provider and the same database and schema. Workers on
  different providers or stores do not see each other's cards or leases.
- The registry needs `ui.enabled: true`.
- The shared folder still holds the sealed recorder files and the archives.
  Only the cards move to Consul, and only the leases of the cluster tasks
  move to Postgres.
- The Consul token needs two rules, in which `drakkar/` is the `key_prefix`:
  `key_prefix "drakkar/" { policy = "write" }` and
  `session_prefix "" { policy = "write" }`. The first allows every key under
  the prefix and the second every session of the datacenter, so give the fleet
  a `key_prefix` of its own and a token that nothing else uses. Without
  `session:write` the first publish fails with a `403`, not the start. See
  [Security](security.md#provider-credentials).
- The Postgres role needs the rights of the election only, because the card
  table `fleet_workers` is not used. With `manage_schema: true` the first
  election start still creates both tables, and the table `fleet_workers`
  stays empty. The role needs `CREATE` on the database and on the schema, and
  must own `fleet_workers`, as in the [Postgres layout](#a-shared-folder-plus-a-shared-postgres).
  With an operator-managed schema, set `manage_schema: false` and run
  `drakkar-schema postgres --schema=drakkar | psql "$ADMIN_DSN"` once; the
  worker role then needs `USAGE` on the schema and `SELECT`, `INSERT`,
  `UPDATE` and `DELETE` on `task_locks`.
- Two clocks decide liveness, and neither is a worker's: Consul ends the
  session of a worker that stopped renewing (half of `offline_after_seconds`,
  at least 10 seconds, and Consul ends a session at most twice that after the
  last renew), and the Postgres server's `now()` decides the length of a
  task lease.
- Point each worker at a Consul agent that stays reachable and healthy,
  usually the agent of its own host. When the cluster marks the agent's node as
  failed, Consul ends the sessions of that node, and the cards of its workers
  leave the lists until the next heartbeat after the node is healthy. Use
  `https://` in the `address` for TLS (see [Security](security.md#provider-credentials)
  for what it verifies).
- During a Consul outage the pipeline keeps running. The workers list keeps its
  last snapshot, and a worker that cannot reach Consul drops out of the lists
  of the others once its session ends. It writes its card again when Consul is
  back, without a restart. During a Postgres outage cluster task slots are
  skipped as `election_failed`, an archive pass stops without publishing, and
  the election recovers when the server is back. Watch
  `drakkar_provider_breaker_open` and `drakkar_provider_calls_total`, which
  carry the label `feature` (`registry` or `election`) for each provider (see
  [Observability](observability.md)).

More: [Fleet registry: consul](fleet.md#consul),
[The postgres election provider](local-databases.md#the-postgres-election-provider),
[Security: provider credentials](security.md#provider-credentials).

## A worker with no writable disk

A worker in a container with no writable disk leaves `storage.dir` empty.
The recorder mode then derives to `none`: the worker keeps its recent events
in the [memory store](recent-events.md) only, and the UI pages read them from
there. The worker writes no recorder file, no cache file and no crash marker,
and it seals, publishes and archives nothing.

```yaml
storage:
  dir: ''
connections:
  redis:
    main:
      key_prefix: "drakkar:"
fleet:
  registry:
    provider: redis
    connection: main
periodic:
  election:
    provider: redis
    connection: main
```

Points to check:

- The `filesystem` registry and election need a folder. With no
  `storage.dir` and no `shared.dir`, use a service provider as above. The
  `filesystem` registry then stays off, and the workers list shows only the
  current worker. A `filesystem` election with no folder stops the start of
  a worker that declares a cluster task.
- With a `shared.dir` mounted, the worker still reads the sealed history of
  its cluster, but it publishes nothing into it.
- Plan RAM for the memory store: up to about 600 MiB per worker with the
  default 256 MiB budget. See [RAM to plan](recent-events.md#ram-to-plan).
- A memory snapshot download holds one more copy of the store in RAM while
  it runs, because there is no folder for a temporary file.
- `ui.recorder.persistence: sqlite` with an empty `storage.dir` fails the
  load.

### A worker with no disk and a central history

A worker with no writable disk can still keep a long history: set
`ui.recorder.persistence: postgres`. Every worker then writes its events to
one table in Postgres, and the trace with history reads the rows of its
cluster from there. No `storage.dir` and no `shared.dir` are needed.

```yaml
storage:
  dir: ''
connections:
  postgres:
    main:
      schema: drakkar                # DSN from DK_CONNECTIONS__POSTGRES__MAIN__DSN
ui:
  recorder:
    persistence: postgres
    postgres:
      connection: main
      retention_hours: 6
fleet:
  registry:
    provider: postgres
    connection: main
periodic:
  election:
    provider: postgres
    connection: main
```

Points to check:

- Size the database first: one busy worker writes about 9 GB per hour, 54 GB
  at the default 6 hours. See [Write volume](recorder-postgres.md#write-volume).
- The role of the connection must own the events table. With an
  operator-managed schema, run once as a role with the DDL rights, then give
  the table to the worker role:

  ```bash
  drakkar-schema postgres --schema=drakkar --recorder --retention-hours=6 | psql "$ADMIN_DSN"
  psql "$ADMIN_DSN" -c 'ALTER TABLE drakkar.recorder_events OWNER TO drakkar_worker'
  ```

- A worker uses up to 7 server connections with the defaults (1 for the
  recorder writes, up to 2 history searches, the shared pool of 1 to 4).
  Size `max_connections` for every worker.
- While Postgres is down, the pipeline keeps running; rows wait in a bounded
  queue, and only that bound drops rows. See
  [While Postgres is down](recorder-postgres.md#while-postgres-is-down).

## Kubernetes probes

Drakkar exposes two dedicated HTTP endpoints for Kubernetes probes on the
UI-server port (``ui.port``, default ``8080``):

| Endpoint   | Purpose    | Success  | Failure                         |
|------------|------------|----------|---------------------------------|
| ``/healthz`` | Liveness   | ``200``  | Restart the pod                 |
| ``/readyz``  | Readiness  | ``200``  | Remove the pod from endpoints   |

Both endpoints are **unauthenticated** — they are the only routes on the
UI server that ignore ``ui.auth_token``. This is intentional: the
kubelet has no facility to supply bearer tokens on probe requests, and
both endpoints expose only liveness / readiness signals with no message
content, partition state, or operator credentials. They must be mounted
for Kubernetes integration to work.

### Probe semantics

- **``/healthz``** — returns ``{"status": "ok"}`` as long as the process
  is running and the FastAPI event loop is responsive. A ``/healthz``
  failure means the process is hung or crashed; the kubelet will restart
  the pod.

- **``/readyz``** — returns ``{"status": "ready"}`` only when the worker
  has completed its startup sequence (consumer subscribed, sinks
  connected, first poll cycle completed), every registered sink is
  currently connected, **and** no partition's processing loop has died.
  Otherwise returns ``{"status": "not_ready", "reasons": [...]}`` with a
  503 status code and a list of machine-readable reasons (e.g.
  ``"not_started"``, ``"sink_kafka:results_not_connected"``,
  ``"partition_3_processor_died"``). The kubelet removes the pod
  from the service endpoints on failure but does NOT restart it — the
  worker is considered recoverable and will self-register once ready.

### Dead partition loops

A partition's processing loop can exit on an unexpected error — a handler
bug, a dependency that fails in a way the framework does not model. Left
alone this is invisible: Kafka keeps the partition assigned, the consumer
keeps enqueuing, and the queue grows with nothing draining it while
offsets stop committing.

The framework restarts a dead loop **once**. A second death is treated as
a deterministic fault: the partition is marked dead, a CRITICAL
``partition_processor_died`` log records the cause and the impact, and
``/readyz`` starts failing with ``partition_<id>_processor_died``. The
pod leaves the service endpoints and, once replaced, the partition is
reassigned to a healthy worker.

``drakkar_partition_processor_deaths_total{partition,outcome}`` counts
both paths — ``outcome="restarted"`` and ``outcome="dead"``. Alert on any
non-zero rate: a restart is a warning, a death means that partition is
stalled until the worker is replaced.

Note what a restart does **not** do. Offsets are registered before the
handler's ``arrange`` runs, so a crash there leaves that window's offsets
uncommitted for the life of the process, and the commit watermark stops
behind them. The restarted loop keeps processing, but its lag climbs until
a rebalance or restart hands those offsets to an owner that redelivers
them. That is the correct at-least-once outcome — those messages were
never processed, so committing past them would lose them.

### Example probe configuration

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  periodSeconds: 5
  failureThreshold: 3
  initialDelaySeconds: 10
```

The ``initialDelaySeconds: 10`` on the readiness probe accommodates the
worker's cold-start sequence: loading config, connecting to Kafka, and
bringing up sinks. Tune upward if the cluster-align wait
(``sources.kafka.startup_align_enabled``) or a large sink fleet extends the
cold-start budget.

### Rolling restarts

During a rolling restart the readiness probe flips ``/readyz`` to 503
as soon as ``_shutdown`` begins — well before sinks are torn down.
Kubernetes removes the pod from the service endpoints immediately, so
in-flight traffic drains to healthy replicas while the stopping pod
finishes committing offsets, draining executors, and closing sinks.
Liveness continues to return 200 until the process actually exits, so
the kubelet does not interpret the graceful-shutdown window as a crash.

A rolling restart that changes a cluster periodic task (its schedule or its
lease) leaves workers of one cluster with different definitions for a
while: they log `periodic_cluster_mismatch` and
`drakkar_periodic_cluster_mismatch_peers` is above zero until the last
worker restarted. See [Definition hash](periodic-tasks.md#definition-hash).
A rolling upgrade from 2.4.0 needs `history.archive.enabled: false` on every
worker until the restart ends, or a stop of the whole fleet; see
[Upgrading](upgrading.md#periodic-tasks-and-the-archive).
