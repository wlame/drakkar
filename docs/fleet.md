# Fleet registry

Workers of one deployment find each other through the **fleet registry**.
Every worker publishes a small **card** about itself and reads the cards of
the others. Two features use the cards:

- the **workers list** of the UI (the worker switcher, the Live page, every
  view that shows more than one worker), served at `GET /api/v1/workers`;
- the **cache exchange**, which pulls cache rows only from the peers the
  registry lists. It takes each peer's URL, cluster, state, liveness and
  `features.cache` marks from the cards (see [Cache](cache.md#cache-exchange)).

The message pipeline does not use the registry. Sources, the executor,
sinks and offset commits run exactly the same with the registry switched
off, slow, or broken: no step of the pipeline waits for a registry call.

## What you get with the defaults

With nothing configured, a worker uses the **filesystem** provider and keeps
the cards in `<shared.dir>/fleet/workers/` (or `<storage.dir>/fleet/workers/`
when `shared.dir` is empty). Workers that see the same folder see each
other. The workers list shows each peer with its state, version and
liveness, and the cache exchange knows where each peer answers and what
it has to pull.

The registry needs the UI server (`ui.enabled: true`), because peers and
browsers reach a worker through the URL on its card. With the UI off, the
registry is off and the worker logs `fleet_registry_disabled_no_ui` once.

## Providers

A **provider** is the part of a feature that talks to a store or a
service. Two features have one, and each is chosen on its own:

| Feature | Key | Default | Plugin entry-point group |
|---|---|---|---|
| Worker registry (this page) | `fleet.registry.provider` | `filesystem` | `drakkar.registry_providers` |
| [Archive election](local-databases.md#the-election-provider) | `history.archive.election.provider` | `filesystem` | `drakkar.election_providers` |

Each feature has its own `connection`, [`resilience`](#resilience) block and
`plugin_options`. The rest of this page is about the registry.

`fleet.registry.provider` selects where cards live:

| Provider | Where the cards are | Liveness |
|---|---|---|
| `filesystem` (default) | one JSON file per worker in a shared folder | from the file times on the file server |
| `static` | nowhere: a fixed peer list in the config | unknown |
| `postgres` | one row per worker in the table `fleet_workers` of a Postgres schema | from the database server's clock (`heartbeat_at` against `now()`) |
| `redis` | one key per worker plus an index of workers, under the key prefix of a Redis connection | from the Redis server's clock (the time of the last publish against `TIME`) |
| `none` | nowhere: no peers | — |
| a plugin name | wherever the plugin keeps them (see [Writing a provider](#writing-a-provider)) | the plugin's own clock, or its own verdict |

With `none` the workers list shows only the current worker, and the cache
exchange pulls nothing. `static` cards come from the config and carry no
`features` object, so the cache exchange pulls every `static` peer each
cycle, without the mark check.

`postgres` and `redis` are built in; see [`postgres`](#postgres) and
[`redis`](#redis). The name `consul` is reserved for a provider of a later
release that keeps its data in a service reached through a [named
connection](#connections). Its own settings sit in `fleet.registry.consul`
(`mode`, `service_name`); a plugin does not read this block, it gets
`plugin_options`. This release ships no provider with the name `consul`.
Two checks apply to a name that uses a connection (`consul`, `redis` and
`postgres`), in this order:

1. The config load checks that the connection the name uses is declared.
   Without `connections.consul.main`, for example, the load fails with
   `fleet.registry.connection 'main' is not defined under connections.consul`.
2. A worker starts with the name only when a provider of that name exists.
   For `postgres` and `redis` it does. For `consul` a plugin of that name
   must be installed. Otherwise startup fails (with the UI on, which the
   registry needs) with a message that lists the providers that exist, for
   example
   `fleet.registry.provider 'consul' is not known; known providers: filesystem, none, postgres, redis, static`.

### `filesystem`

```yaml
storage:
  dir: /var/lib/drakkar          # this worker's own files
shared:
  dir: /mnt/fleet                # seen by every worker of the fleet
fleet:
  registry:
    provider: filesystem         # the default
    # filesystem:
    #   dir: /mnt/fleet-registry # optional; default <shared.dir>/fleet
```

The folder is `fleet.registry.filesystem.dir` when you set it, else
`<shared.dir>/fleet`, else `<storage.dir>/fleet`. When none of these
resolves (memory-only `storage.dir: ''` and no `shared.dir`), the registry
is off and the worker logs `fleet_registry_disabled_no_dir` once.

The two ways of getting a folder fail differently:

- A folder you **set explicitly** must work: if the worker cannot create
  `<dir>/workers/` or cannot write it, **startup fails** with a message that
  names `fleet.registry.filesystem.dir`.
- A **derived** folder is best effort: if it cannot be written, the worker
  runs anyway, counts each failed publish in
  `drakkar_fleet_registry_errors_total{op="publish"}` and retries at the
  next heartbeat.

Layout and rules:

- One card per worker: `workers/<worker>.json`. Characters of the worker
  name outside `[a-zA-Z0-9_-]` become `_` in the file name, so `search.1`
  is stored as `search_1.json`. Two worker names that differ only in such
  characters share one file; give workers names that stay distinct.
- A card is written to `workers/.<worker>.json.<instance>.tmp` and renamed
  over the card, so a reader never sees half a card. A temp file that a
  crashed writer left behind is deleted once it is older than
  `prune_after_seconds`.
- The folder is created with mode 0700 and each card with mode 0600. A card
  holds no secrets; who may read the folder is decided by the folder's own
  permissions on the file server.
- The worker never follows a symbolic link in the folder. When `workers`
  itself is a link (or a file), publishing and listing fail and count as
  registry errors; nothing in the link's target is read or deleted. A link,
  folder or FIFO named like a card is skipped and logged with reason
  `not_a_file`.
- A card file that cannot be opened or read (for example its permissions
  deny this worker) is skipped with reason `unreadable`; one larger than
  4096 bytes with `too_large`; one that is not JSON or not a JSON object
  with `not_json` or `not_object`. The worker logs `fleet_card_invalid` once
  for that file name, and the other cards are still listed. Cards are
  written with mode 0600, so workers that run as different users and share
  the folder cannot read each other's cards; run them as one user.
- A card older than `prune_after_seconds` (default 86400, one day) is
  deleted by the next worker that lists the folder.
- At a clean stop the worker deletes its own card, but only while the card
  still holds this process's instance: if another process with the same
  worker name has written it since, the card stays.

### `static`

A fixed peer list, for deployments without a shared folder:

```yaml
fleet:
  registry:
    provider: static
    static:
      peers:
        - name: search-worker-1
          cluster: search
          url: http://10.0.3.17:8080/
        - name: search-worker-2
          cluster: search
          url: http://10.0.3.18:8080/
```

The worker publishes nothing. Its peers are always listed, and their
liveness is **unknown**: the workers list shows them as neither online nor
offline.

### `postgres`

One row per worker, in a Postgres database that every worker of the fleet
can reach. It fits a deployment that already runs Postgres and has no
shared folder for the cards.

```yaml
connections:
  postgres:
    main:
      schema: drakkar            # the schema that holds the tables
      manage_schema: true        # create the schema and the tables when missing
      # dsn: set from the environment, see below
fleet:
  registry:
    provider: postgres
    connection: main             # the default
```

Set the connection string in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:...@db.internal/drakkar'
```

The registry needs the UI server (`ui.enabled: true`), as with every
provider. Every worker of the fleet uses the same provider and the same
database, schema and `connection`; workers on different stores do not see
each other.

What it keeps and does:

- **The table.** `<schema>.fleet_workers` has one row per worker name:
  `worker` (the primary key), `instance`, `cluster`, `card` (the card's
  canonical bytes as text, the same bytes the `filesystem` provider writes
  to a file) and `heartbeat_at`. Two indexes serve the cluster and the
  heartbeat columns.
- **Publish.** One upsert per heartbeat and per state change. It writes the
  card and sets `heartbeat_at = now()`, with `now()` taken by the database
  server.
- **List.** First it deletes the rows whose `heartbeat_at` is older than
  `prune_after_seconds`, then it reads every row with its age. Both steps
  use the server's `now()`, so a worker that lists also prunes. The age is
  `now() - heartbeat_at`, computed by the server in the same query.
- **Liveness.** The provider gives no `online` verdict. The
  [liveness rule](#liveness) applies: a peer is online while its age is at
  most `offline_after_seconds`. No worker's wall clock enters an age, so
  clock skew between worker hosts, or between a worker and the database, does
  not matter.
- **Duplicate names.** The worker name is the primary key, so two processes
  with one name overwrite each other's row: the last writer wins. A worker
  that lists the other instance's row while it is live by the age rule logs
  `fleet_duplicate_worker_name` (see [Duplicate worker names](#duplicate-worker-names)).
- **Withdraw.** At a clean stop the worker deletes its row, but only while
  the row still holds this process's `instance`.
- **A row that does not decode** is skipped with `fleet_card_invalid` and
  the reason, once for that worker name while the name stays listed. The
  other rows are still listed.

The schema and the pool come from the connection, which is
`connections.postgres.<connection>` (`schema`, `manage_schema`, `pool_min`,
`pool_max`; see [Configuration](configuration.md#connections)).
`fleet.registry.postgres` exists and has no keys. The pool is opened once,
at the first provider start, and the registry and the archive election share
it when they name the same connection. The worker closes it at shutdown,
after both providers have stopped (see [Connections](#connections)).

**The schema.** At the first successful `start` of the provider:

- With `manage_schema: true` (the default), the worker takes an advisory
  lock on the schema name and runs five `CREATE ... IF NOT EXISTS`
  statements in one transaction: the schema, `fleet_workers` with its two
  indexes, and `archive_locks`. The lock makes workers that start together
  take turns, so they do not race on the DDL. Whichever provider starts
  first creates both tables.
- With `manage_schema: false`, the worker only checks that the table it
  needs exists. When the table is missing, `start` fails with
  `table drakkar.fleet_workers does not exist and connections.postgres.main.manage_schema is false; create it with: drakkar-schema postgres --schema=drakkar`
  (with your schema and connection names), and the registry tries the start
  again at the next heartbeat, like any failed start. The check covers
  only the table of the provider that starts; the other provider checks
  its own.

`drakkar-schema postgres --schema=<name>` prints the DDL as plain SQL on
stdout, for an operator to run with a role that has the rights the worker
role does not. It also runs as `python -m drakkar.schema postgres --schema=<name>`:

```bash
drakkar-schema postgres --schema=drakkar | psql "$ADMIN_DSN"
```

It prints the same five statements for both providers, each ending in `;`.
The schema name is a lower-case identifier, `[a-z_][a-z0-9_]*`. A usage
error (a wrong feature name, a missing or invalid `--schema`) exits with
code 2, writes the reason and the usage to stderr and nothing to stdout.
`drakkar-schema --help` prints the usage and exits with 0.

**The database role.**

- With `manage_schema: true`, the worker runs the DDL at every first start,
  also when the objects already exist. The role needs `CREATE` on the
  database (`CREATE SCHEMA IF NOT EXISTS` checks it even when the schema
  exists) and `CREATE` on the schema (for the tables and the indexes). Owning
  the schema is not enough. The role must also own the table
  `fleet_workers`: its two `CREATE INDEX IF NOT EXISTS` statements check
  ownership before they skip an index that exists. The simplest case is to
  let the worker role create the objects. For tables that another role
  created, set `manage_schema: false`. `archive_locks` needs no ownership.
- With `manage_schema: false`, the role needs `USAGE` on the schema and
  `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the tables it uses
  (`fleet_workers`; `archive_locks` for the election). It needs no right to
  create anything, so an operator creates the objects once with
  `drakkar-schema`. See [Security](security.md#provider-credentials).

**During an outage.** A connection that is refused or lost, a server that
restarts and a transaction that must be run again are transient errors. They
are retried under `fleet.registry.resilience` (see [Resilience](#resilience)).
When operations keep failing, the breaker opens after `failure_threshold`
failed operations and the provider is skipped until the cooldown has passed.
The workers list keeps its last snapshot, and
`drakkar_fleet_registry_snapshot_age_seconds` grows. The pipeline does not
wait for any of this. When the server is back, the next probe operation
closes the breaker and the registry recovers without a restart; nothing
needs to be reset, because the pool opens new connections by itself.

### `redis`

One key per worker and one index of the workers, in a Redis database that
every worker of the fleet can reach. It fits a deployment that already runs
Redis and has no shared folder for the cards.

```yaml
connections:
  redis:
    main:
      key_prefix: "drakkar:"     # the default; every key starts with it
      # url: set from the environment, see below
fleet:
  registry:
    provider: redis
    connection: main             # the default
```

Set the URL in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__REDIS__MAIN__URL='redis://drakkar-fleet:...@redis.internal:6379/3'
```

Without it the entry uses `redis://localhost:6379/0`. The registry needs the
UI server (`ui.enabled: true`), as with every provider. Every worker of the
fleet uses the same provider and the same server, database number and
`key_prefix`; workers on different stores do not see each other.

What it keeps and does:

- **The keys.** With the default prefix, `drakkar:fleet:workers:<worker>` is
  the card of one worker: its canonical bytes as text, the same bytes the
  `filesystem` provider writes to a file. `drakkar:fleet:workers` is the
  index: a sorted set with one member per worker, scored by the time of the
  Redis server at that worker's last publish. In `<worker>` and in the
  index member, characters of the worker name outside `[a-zA-Z0-9_-]` become
  `_`.
- **Publish.** One script per heartbeat and per state change. It stores the
  card with a time to live of `prune_after_seconds`, adds the worker to the
  index with the server's `TIME` as its score, and gives the index the same
  time to live.
- **List.** One script, so a database full of other keys costs nothing
  extra. It reads the server's `TIME`, reads the index, fetches each card,
  and removes the index members whose card key has expired. The age of a card
  is the server's time now minus the stored time of its last publish, floored
  at 0.
- **Pruning.** Redis deletes a card when its time to live ends, so the card
  of a dead worker leaves the list `prune_after_seconds` after its last
  publish. The index gets the time to live of the last publish too. When no
  worker publishes for that long, the index goes as well, and cards that are
  still alive at that moment are not listed until their workers publish
  again. A live worker comes back at its next heartbeat.
- **Liveness.** The provider gives no `online` verdict. The
  [liveness rule](#liveness) applies: a peer is online while its age is at
  most `offline_after_seconds`. No worker's wall clock enters an age, and the
  `prune_after_seconds` of the reading worker does not either, so workers with
  different `prune_after_seconds` values can share one server.
- **Duplicate names.** The card key comes from the sanitized worker name, so
  two processes with one name overwrite each other's card: the last writer
  wins. Names that differ only in characters outside `[a-zA-Z0-9_-]` share
  one card too. A worker that lists the other instance's card while it is
  live by the age rule logs `fleet_duplicate_worker_name` (see [Duplicate
  worker names](#duplicate-worker-names)).
- **Withdraw.** At a clean stop the worker deletes its card and its index
  entry, but only while the card still holds this process's `instance`.
- **A card that does not decode** is skipped with `fleet_card_invalid` and
  the reason, once for that worker name while the name stays listed. The
  other cards are still listed.
- **Start.** The provider sends `PING`, so `provider_started` means that the
  server answered.

The address, the database number and the credentials come from the `url` of
the connection, and the key prefix from its `key_prefix`:
`connections.redis.<connection>` (see
[Configuration](configuration.md#connections)). `fleet.registry.redis` exists
and has no keys. The client is created once, at the first provider start, and
the registry and the archive election share it when they name the same
connection. The worker closes it at shutdown, after both providers have
stopped (see [Connections](#connections)).

**The URL.** `redis://` is plain TCP and `rediss://` is TLS. The database
number is the path, for example `/3`. A URL with query options (`?...`) is
refused when the provider starts, with
`redis connection: the url cannot carry query options`. A URL that does not
parse fails the start with `redis connection: the url cannot be parsed`.
Neither message quotes the URL. The registry tries the start again at the
next heartbeat, like any failed start.

**The server.** Redis 5.0 or later, standalone or a primary behind a fixed
address. A script reads `TIME` and then writes, which Redis allows from
version 5.0 on. Redis Cluster is not supported, because the scripts touch
keys that are not all named in `KEYS`.

**The Redis user.** A user of its own needs these rights and no more, on the
keys that start with the prefix: `~<prefix>* +@read +@write +@scripting
+@connection +time`. See [Security](security.md#provider-credentials) for a
complete `ACL SETUSER` line. Use a database number of its own for the fleet,
so no other data is within reach of this user and none is in the way of the
provider.

**During an outage.** The client does not retry on its own; the
[`resilience`](#resilience) block of the registry is the only retry and the
only time limit. A connect gives up after 5 seconds, and a read or a write
ends at `timeout_seconds` of the block. A connection that is refused or lost,
a timeout, and the server replies `BUSY` (a script is running), `LOADING`
(the data set is loading), `MASTERDOWN` and `TRYAGAIN` are transient errors,
and they are retried. Any other reply is final: a wrong password
(`WRONGPASS`), a missing ACL right (`NOPERM`) and `ERR ...` end the operation
at once, and each counts as a failed operation for the breaker. When
operations keep failing, the breaker opens after `failure_threshold` failed
operations and the provider is skipped until the cooldown has passed. The
workers list keeps its last snapshot, and
`drakkar_fleet_registry_snapshot_age_seconds` grows. The pipeline does not
wait for any of this. When the server is back, the next probe operation
closes the breaker and the registry recovers without a restart; nothing
needs to be reset, because the client opens new connections by itself.

## The card

A card is a JSON object, at most 4096 bytes:

```json
{
  "backend": "python",
  "cluster": "search",
  "features": {"cache": {"cluster": {"key": "match|a", "ms": 1760348411000}}},
  "heartbeat_at": "2026-10-13T09:40:11.000210Z",
  "instance": "9c41e0b27fd34a1e",
  "schema": 1,
  "started_at": "2026-10-13T09:12:01.123456Z",
  "state": "running",
  "url": "http://10.0.3.17:8080/",
  "version": "2.4.0",
  "worker": "search-worker-3"
}
```

| Field | Required | Meaning |
|---|---|---|
| `schema` | yes | Card format version, `1`. A card with another value is skipped and the worker logs `fleet_card_schema_unsupported` once for that worker. |
| `worker` | yes | Worker name. Not empty. |
| `cluster` | yes | The worker's cluster name, as configured (may be `""`). |
| `url` | yes | Where the worker's UI server answers: `ui.public_url` when set, else `http://<outbound ip>:<ui.port>/`. Not empty. |
| `instance` | no | 16 random hex characters, new at every process start. Tells two processes with one worker name apart. |
| `backend` | no | The implementation that runs the worker. |
| `version` | no | The worker's release, as `/api/v1/identity` reports it. |
| `state` | no | `starting`, `running` or `draining`. |
| `started_at`, `heartbeat_at` | no | Process start and the time of this publish, on the writer's clock. Informational only: liveness never uses them. |
| `features` | no | An object for optional capabilities. Unknown keys are ignored. `features.cache` is present while the worker serves the [cache exchange](cache.md#cache-exchange) (the cache, `storage.dir` and `fleet.cache_exchange` are on): for each scope (`cluster`, `global`), `{"ms": <updated_at_ms>, "key": <key>}` of the newest row this worker originated. A scope with no such row is absent. A peer pulls only when a mark is past its cursor for this worker. A mark carries at most the first 128 characters of its key, so a long cache key never pushes the card over its 4096-byte limit. |

A card whose required field is missing, empty (`worker`, `url`) or of the
wrong type, or whose optional field has the wrong type, is skipped; the
worker logs `fleet_card_invalid` with the reason once for that worker.
Unknown fields and unknown `state` values are kept and ignored.

The card is written with sorted keys, no spaces between tokens, non-ASCII
characters as raw UTF-8, and no trailing newline, so the same card has the
same bytes whichever worker wrote it.

## States

A worker publishes its card:

- with `starting` right after its UI server listens;
- with `running` once its sources have started;
- with `draining` as soon as shutdown begins;
- again every `heartbeat_seconds` (default 10) with the current state.

A state change is published at once, not at the next heartbeat. At the end
of a clean stop, after the recorder's last write and before the UI server
stops, the worker withdraws its card (see [Shutdown](#resilience) for when it
does not).

## Liveness

The workers list shows a peer as **online** when its card is no older than
`fleet.registry.offline_after_seconds` (default 60), and as **offline**
when it is older. `offline_after_seconds` must be at least twice
`heartbeat_seconds`, so one late heartbeat never marks a worker offline, and
at least 10, whichever provider is used.

A provider whose service decides liveness itself can say so. It sets
`online` on the `PeerCard` it lists (for example, a card that exists only
while a session of its owner lives). The rule is applied in this order:

1. `online` is `true` or `false`: that is the answer. The age is not
   compared with `offline_after_seconds`.
2. `online` is not set and the age is known: online while the age is at most
   `offline_after_seconds`.
3. Neither is known: the peer is neither online nor offline (`null` in the
   workers list, `unknown` in `drakkar_fleet_peers`).

The same answer feeds the workers list, `drakkar_fleet_peers` and the
[duplicate-name check](#duplicate-worker-names). Anything that is not a
boolean in `online` is read as "not set".

The age of a card is measured **on the provider's clock**, never by
comparing one host's wall clock with another's. For the filesystem provider
that clock is the file server's: right after a worker writes its own card,
it reads the card's modification time as a sample of the file server's
time, and from then on adds its own elapsed monotonic time. A peer card's
age is that estimate minus the card's modification time. Two consequences:

- Clock skew between worker hosts does not matter.
- Before a worker's first successful publish it has no sample, so it lists
  every peer with an unknown age: the workers list shows them as neither
  online nor offline until the first publish succeeds.

The `postgres` provider needs no such sample. The database server computes
the age of each row (`now() - heartbeat_at`) in the query that lists the
rows, so every listed card has an age. The `redis` provider needs none
either: the script that lists the cards reads the Redis server's `TIME`, and
the age is that time minus the server's `TIME` at the card's last publish.

Each refresh (every `refresh_seconds`, default 15) keeps the age of every
card at the moment it was listed. The workers list adds the time since that
refresh, so a peer never looks younger because a refresh is slow or failed.

`last_seen_ts` in the workers list is the reading worker's current time
minus the card's age, in epoch seconds. It is `null` when the age is
unknown.

## The workers list

`GET /api/v1/workers` answers from memory, with no storage access: the
current worker plus the peers of the last refresh. The current worker is
always listed and always `online: true`. A peer row carries the fields of
its card that are present; see the UI contract for the full row shape.

## Connections {#connections}

A provider that keeps its data in a service (Consul, Redis, Postgres) does
not carry the address and the credentials itself. The config declares them
once, under `connections.<service>.<name>`, and a feature names the entry.
This release ships the `postgres` and `redis` providers, which read
`connections.postgres.<name>` and `connections.redis.<name>`; the entry for
Consul is reserved, and a provider plugin can use any entry (see [Writing a
provider](#writing-a-provider)).

```yaml
connections:
  postgres:
    main:                                  # used when a feature names no connection
      schema: drakkar
    fleet_database:
      schema: drakkar_fleet
      manage_schema: false
fleet:
  registry:
    provider: postgres
    connection: fleet_database             # default: main
```

Each `postgres` entry also needs its `dsn`, set from the environment, for
example `DK_CONNECTIONS__POSTGRES__MAIN__DSN` and
`DK_CONNECTIONS__POSTGRES__FLEET_DATABASE__DSN`. A `redis` entry has its
`url` the same way, for example `DK_CONNECTIONS__REDIS__MAIN__URL`.

- `connections` has one map per service type: `consul`, `redis` and
  `postgres`. You choose the names. A feature that names none uses `main`.
- `fleet.registry.connection` and `history.archive.election.connection` name
  an entry. The `postgres` and `redis` providers read it; the name `consul`
  is reserved. `filesystem`, `static` and `none` ignore it, and so does
  a plugin with any other name. The config load checks the key by provider name, so a provider
  named `consul`, `redis` or `postgres` (a plugin of that name too) needs its
  connection declared; otherwise the load fails with
  `fleet.registry.connection 'main' is not defined under connections.consul`.
- Features that name the same connection share one client. The first
  feature that needs it opens it; nothing opens at startup.
- At shutdown the worker closes the clients after the last provider has
  stopped, the newest first, inside what is left of
  `executor.drain_timeout_seconds`. A client
  that fails to close is logged (`connection_close_failed`) and the others
  are still closed. When the time runs out first, the worker logs
  `connections_close_timed_out` and the rest go with the process.
- Passwords and tokens belong in the environment, not in the YAML file; see
  [Security](security.md#provider-credentials). Write connection names in
  lower case: the environment path is lower-cased when it is read, so a name
  with capital letters cannot be set from a `DK_` variable.

Every key is in [Configuration](configuration.md#connections).

## Resilience {#resilience}

Every call from the worker to a provider is an **operation**. The registry
makes five: `start`, `publish`, `list`, `withdraw` and `stop`. The archive
election has its own. An operation runs under the `resilience` block of its
feature: `fleet.registry.resilience` and `history.archive.election.resilience`.
Both have the same keys:

| Key | Default | Meaning |
|---|---|---|
| `timeout_seconds` | `5` | Time limit for one attempt. Each retry gets its own limit. |
| `retries` | `2` | Attempts after the first one, made only after a transient error. `0` means one attempt. |
| `backoff_seconds` | `0.2` | Wait before the first retry. The wait doubles for each further retry. |
| `backoff_max_seconds` | `5` | Longest wait between two attempts. At least `backoff_seconds`. |
| `breaker.failure_threshold` | `5` | Failed operations in a row that open the breaker. |
| `breaker.cooldown_seconds` | `30` | Time the breaker stays open before one operation may probe the provider. |

**Retries.** A transient error is a timeout, an `OSError` (a refused or
reset connection, for example), or a `drakkar.providers.resilience.TransientError`
that the provider raises on purpose. Only these are retried. Any other
error ends the operation at once. The `postgres` providers raise
`TransientError` for a Postgres error whose SQLSTATE begins with `08`
(connection exception), `53` (insufficient resources) or `57` (operator
intervention, for example a server restart), and for `40001` (serialization
failure) and `40P01` (deadlock). The `redis` providers raise
`TransientError` for a lost or refused connection and for the server replies
`BUSY`, `LOADING`, `MASTERDOWN` and `TRYAGAIN`; a client timeout counts as a
timeout. Every other reply, such as `WRONGPASS`, `NOPERM` or `ERR ...`, ends
the operation at once. With the defaults one operation takes at
most 3 × 5 s plus the waits of 0.2 s and 0.4 s, which is 15.6 s. The
registry publishes the next card only after the previous publish ended, so
keep the longest operation well below `offline_after_seconds`.

**Breaker.** It counts failed operations, not attempts: an operation fails
when its last attempt fails. One breaker serves all operations of a feature.
Failed operations of any mix (for the registry `start`, `publish`, `list`,
`withdraw` and `stop`) count toward the same `failure_threshold`, a success of
any of them resets the count, and an open breaker skips all of them. After
`failure_threshold` failed operations in a row the breaker opens. While it is open, operations are skipped without a
call. After `cooldown_seconds` the next operation is a probe, and the others
stay skipped until it ends. A probe that succeeds closes the breaker. A
probe that fails opens it again with a fresh cooldown. An operation that
started before the breaker opened does not change it when it ends late.

**Outcomes.** Each operation is counted once in
`drakkar_provider_calls_total` ([Metrics](#metrics)) with one outcome:

| Outcome | Meaning |
|---|---|
| `ok` | The operation succeeded. |
| `error` | It failed with an error that is not a timeout: an error that is never retried, or a transient error on the last attempt. |
| `timeout` | The last attempt timed out, or the time left (a shutdown deadline) was too short to make an attempt. |
| `skipped_open` | The breaker was open. The provider was not called. |
| `not_started` | The provider has not started yet. The provider was not called. |

An operation that made no attempt (`skipped_open`, `not_started`, or a
`timeout` for lack of time) has no duration sample and does not count in
`drakkar_fleet_registry_errors_total`.

**Start.** The registry starts its provider from the publish loop. A
`start` that fails or times out does not replace the provider with `none`.
It is tried again at the next heartbeat, and at once when the worker's
state changes. While the breaker is open, the start is skipped until the
cooldown has passed, and then one start is the probe. Until a start has
succeeded, nothing is published, listed or withdrawn: each publish tick
counts one `publish` as `not_started`, the workers list shows only this
worker, and `drakkar_fleet_registry_snapshot_age_seconds` grows from the
start of the service. When a start succeeds, the worker logs
`provider_started` and the same tick publishes the card. A provider
therefore sees `start` called again after a failed one: it must leave
nothing behind that a second call would trip over.

**Shutdown.** The `withdraw` and the provider's `stop` run once each, with
one attempt, both inside the time that is left of
`executor.drain_timeout_seconds`. Each attempt is cut to the time left. A
provider that never started gets neither call (both count as `not_started`);
with the breaker open both are skipped at once. A stuck provider cannot hold
the shutdown longer than that budget. A card that was not withdrawn stays
in the store until `prune_after_seconds`, and peers show the worker as
offline once the card is older than `offline_after_seconds`.

## Failures

- A failed operation, after its retries, keeps the last snapshot, counts one
  error in `drakkar_fleet_registry_errors_total{op}` and logs
  `provider_call_failed`. That line names the operation, the error and the
  number of attempts, and appears at most once a minute per operation. The
  `suppressed` field says how many failures the line stands for.
- When the breaker opens, the worker logs `provider_breaker_opened` once.
  It logs `provider_breaker_closed` when a probe closes it again.
  `drakkar_provider_breaker_open` is `1` in between.
- While refreshes fail, `drakkar_fleet_registry_snapshot_age_seconds` grows,
  so an alert can tell a stale workers list from an empty one.
- The filesystem provider runs its file calls on its own thread. A call
  stuck on an unresponsive network mount holds only that thread: the next
  attempts time out, and the rest of the worker does not notice.
- The `postgres` provider awaits its queries on the event loop. The time
  limit of an attempt cancels a query that does not answer and closes its
  connection, so a stuck query holds up only that attempt. A release of the
  connection to the pool that is already in progress when the limit comes
  still waits for the server. It holds one pool connection until the server
  answers, and the pool gets the connection back then.
- The `redis` provider awaits its commands on the event loop. The client has
  no read limit of its own: the time limit of an attempt ends a command that
  does not answer, so a paused server holds up only that attempt. A connect
  gives up after 5 seconds.
- A provider that fails to start is retried; see [Resilience](#resilience).
- An iteration of a registry loop can raise outside a provider call, for
  example in the card builder or in the `features` supplier. The iteration
  is counted in `drakkar_fleet_registry_errors_total` (as `op="publish"` for
  the publish loop, `op="list"` for the refresh loop) and logged as
  `fleet_registry_iteration_failed`, at most once a minute per loop. The loop
  goes on. A loop that ends for good is logged as `fleet_registry_loop_failed`
  when the worker stops; the withdraw still runs.

## Duplicate worker names

Two live processes that publish one worker name overwrite each other's
card. When a worker lists a card with its own name, another instance, and
that is live by the [liveness rule](#liveness), it logs `fleet_duplicate_worker_name`
(with the other instance and URL) once for that instance and counts it in
`drakkar_fleet_duplicate_worker_name_total`. Give every worker a unique
name, for example from the pod name through `worker_name_env`.

## Writing a provider

A provider is a class with five async methods and a factory that builds it
from an options mapping:

```python
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext


class EtcdRegistry:
    def __init__(self, options):
        self._address = options['address']

    async def start(self, ctx: RegistryContext) -> None:
        """Prepare the provider. ctx carries the worker name, instance, cluster,
        logger, options and the shared connections. Called again after a
        failure, so leave nothing behind when it raises."""

    async def publish(self, card: dict) -> None:
        """Store the card, replacing any card stored under the same worker name."""

    async def list(self) -> PeerCardList:
        """Every card you can read, this worker's own included, with its age
        on your own clock (None when you cannot tell)."""
        return [PeerCard(card={...}, age_seconds=12.5)]

    async def withdraw(self, worker: str, instance: str) -> None:
        """Delete the worker's card, but only while it still holds this instance."""

    async def stop(self) -> None:
        """Release what start took. Called at most once, last."""


def make_registry(options):
    return EtcdRegistry(options)
```

Register the factory as an entry point of your package:

```toml
[project.entry-points."drakkar.registry_providers"]
etcd = "drakkar_etcd:make_registry"
```

and select it:

```yaml
fleet:
  registry:
    provider: etcd
    plugin_options:
      address: etcd.service:2379
```

`plugin_options` is passed to the factory unchanged; the provider validates
it. The config reference masks its values (see
[Security](security.md#provider-credentials)). A plugin name wins over a
built-in name, so a package can replace `filesystem`. A plugin named
`consul`, `redis` or `postgres` is checked like the provider of that name:
the connection that `fleet.registry.connection` names must be declared.

A plugin that fails to load is logged and skipped
(`provider_entry_point_load_failed`, with the fields `group`, `name`, `value`
and `error`; the other two events of this family are
`provider_entry_point_scan_failed`, with `group` and `error`, and
`provider_entry_point_invalid`; the field `group` names
`drakkar.registry_providers`). Startup then fails only
if the configured name is the missing one, with a message that lists the
providers that exist.

Provider methods are coroutines that run on the worker's event loop, the
same loop that runs the message pipeline. A method that blocks — file or
network I/O without `await`, a synchronous client library — stalls every
partition while it runs, and the time limit cannot interrupt it. Run
blocking work in a thread of your own (for example a one-thread
`concurrent.futures.ThreadPoolExecutor` driven with
`loop.run_in_executor`), so a hung call holds only that thread.

What the worker does, and what the provider does:

| The worker | The provider |
|---|---|
| loops, time limits, retries, the breaker, the thread or task each call runs on | storing the cards |
| building and validating cards | the age of each card on its own clock (`None` when unknown) |
| the snapshot, the duplicate warning | its own liveness verdict (`PeerCard.online`), when its service has one |
| online and offline from the verdict or the age | deleting old cards |

`list()` returns the caller's own card too; the worker leaves it out of the
snapshot and builds its own row from local state.

Rules for the lifecycle methods:

- **`start` is retried.** A `start` that raises or times out is called
  again at the next heartbeat, so it must leave nothing behind that a second
  call would trip over. Nothing else is called before a `start` succeeded.
- **`stop` may not be called.** It is called at most once, and only after a
  `start` that succeeded. It is not called while the breaker is open or when
  the shutdown time is spent, and not for a `start` that the shutdown
  cancelled half way. Open clients lazily, or make sure that a missing
  `stop` leaks nothing that outlives the process.
- **A provider is single-use.** The worker never calls `start` after
  `stop`. The built-in `filesystem` provider raises `provider stopped` on
  such a call, so a misuse shows at once.
- **`online` is optional.** Set `PeerCard.online` to `True` or `False` only
  when your service decides liveness itself. Leave it `None` otherwise: the
  worker then judges `age_seconds` against `offline_after_seconds`.
- **Shared connections.** `ctx.connections` resolves the entries of the
  `connections` config (it is `None` outside a worker, as in a test).
  `await ctx.connections.acquire(kind, name, opener, closer)` returns the
  client of `connections.<kind>.<name>`. It calls `opener(settings)` for the
  first caller only and keeps `closer` for the shutdown, so every feature
  that names the connection shares one client. `ctx.connections.settings(kind, name)`
  returns the declared settings. An undeclared name raises
  `drakkar.providers.connections.UnknownConnectionError`. A plugin takes the
  name from its own `plugin_options`.
- **Raise `drakkar.providers.resilience.TransientError`** for an error that
  is worth a retry and is not already an `OSError` or a timeout.

### The test kit

`drakkar.fleet.testkit` holds the checks every provider should pass. They
need no test framework and raise `AssertionError` with a plain message:

```python
import pytest

from drakkar.fleet import testkit


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS)
async def test_etcd_registry(check, etcd_store):
    await check(lambda: EtcdRegistry({'address': etcd_store.address}), etcd_store.advance, 600)


@pytest.mark.parametrize('check', testkit.LIVENESS_CHECKS)
async def test_etcd_registry_liveness(check, etcd_store):
    await check(lambda: EtcdRegistry({'address': etcd_store.address}), etcd_store.advance, 60)
```

- `make_provider()` returns a new provider that shares storage with the
  others the same check builds (two of them stand for two processes).
- `advance(seconds)` makes every stored card that much older on your clock.
- The last argument of `PUBLISHING_CHECKS` is the age after which your
  provider deletes a card. The last argument of `LIVENESS_CHECKS` is the age
  after which a card whose owner stopped publishing must no longer be
  reported `online`. A provider that leaves `online` at `None` passes them.

`testkit.check_list_shape(provider, ctx)` and
`testkit.check_age_is_none_or_non_negative(provider, ctx)` apply to every
provider, including read-only ones; `testkit.make_context(worker)` builds a
context for them.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `drakkar_fleet_peers` | gauge | `state` (`online`, `offline`, `unknown`) | Peers in the last registry snapshot by liveness state (this worker not counted). |
| `drakkar_fleet_registry_errors_total` | counter | `op` (`start`, `publish`, `list`, `withdraw`, `stop`) | Registry operations that failed or timed out, and loop iterations that raised. An operation counts once, however many attempts it made. An operation that made no attempt (open breaker, not started) does not count. |
| `drakkar_fleet_registry_duration_seconds` | histogram | `op` | Duration of registry operations in seconds, all attempts and the waits between them included. |
| `drakkar_fleet_registry_snapshot_age_seconds` | gauge | — | Seconds since the last successful registry refresh; 0 right after one. |
| `drakkar_fleet_duplicate_worker_name_total` | counter | — | Other live instances seen publishing this worker's name. |
| `drakkar_fleet_peer_requests_total` | counter | `endpoint` (`cache_changes`), `outcome` (`ok`, `http_error`, `timeout`, `transport_error`, `bad_body`) | Requests this worker sent to peer workers, by endpoint and outcome. |
| `drakkar_fleet_peer_request_duration_seconds` | histogram | `endpoint` | Time of one request this worker sent to a peer worker, by endpoint. |

Every provider operation, of the registry and of the archive election, is
also counted by the three provider metrics:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `drakkar_provider_calls_total` | counter | `feature` (`registry`, `election`), `provider`, `op`, `outcome` (`ok`, `error`, `timeout`, `skipped_open`, `not_started`) | Provider operations by feature, provider, operation and [outcome](#resilience). |
| `drakkar_provider_call_duration_seconds` | histogram | `feature`, `provider`, `op` | Duration of one provider operation with its retries. One sample per operation that made an attempt. Buckets run from 5 ms to 30 s. |
| `drakkar_provider_breaker_open` | gauge | `feature`, `provider` | `1` while the circuit breaker of that provider is open, else `0`. |

## Log events

| Event | Level | When |
|---|---|---|
| `fleet_registry_started` | info | The registry service started; names the provider, instance and URL. |
| `fleet_registry_disabled_no_ui` | warning | The UI is off, so the registry is off. |
| `fleet_registry_disabled_no_dir` | warning | The filesystem provider has no folder, so the registry is off. |
| `provider_started` | info | A provider started (fields `feature`, `provider`). After failed starts it is logged once, when a start succeeds. |
| `provider_call_failed` | warning | A provider operation failed after its retries (fields `feature`, `provider`, `op`, `error`, `attempts`, `suppressed`); at most once a minute per operation. `suppressed` counts the failures the line stands for. |
| `provider_breaker_opened` | warning | A closed breaker opened (fields `feature`, `provider`, `failure_threshold`, `cooldown_seconds`). A failed probe does not log it again. |
| `provider_breaker_closed` | info | A probe succeeded and closed the breaker. |
| `fleet_registry_iteration_failed` | error | A registry loop iteration raised outside a provider call (field `loop`: `publish` or `refresh`); at most once a minute per loop, with `suppressed`. The loop goes on. |
| `fleet_registry_stop_failed` | warning | The registry's own stop raised. A provider that fails to stop shows as `provider_call_failed` with `op` `stop`. |
| `fleet_registry_loop_failed` | error | A registry loop ended on an unexpected error; logged at stop, before the card is withdrawn. |
| `connection_close_failed` | warning | A client of a named connection failed to close at shutdown (fields `kind`, `name`, `error`; credentials in the error are removed). |
| `connections_close_timed_out` | warning | The shutdown time ran out while connections closed (field `timeout_seconds`). |
| `connections_close_failed` | warning | Closing the connections raised an error of its own. |
| `fleet_card_invalid` | warning | A card was skipped; once per file name (a file that is unreadable, not a plain file, too large or not a JSON object) or per worker (field rules). |
| `fleet_card_schema_unsupported` | warning | A card of another format version was skipped; once per worker. |
| `fleet_duplicate_worker_name` | warning | Another live process publishes this worker's name; once per instance. |
| `provider_entry_point_scan_failed`, `provider_entry_point_load_failed`, `provider_entry_point_invalid` | warning | A provider plugin could not be found or loaded. `scan_failed` has the fields `group` and `error`; `load_failed` and `invalid` add `name` and `value`. `group` is `drakkar.registry_providers` or `drakkar.election_providers`. |
| `fleet_peer_request_failed` | warning | A request to a peer failed (fields `peer`, `url`, `endpoint`, `outcome`, `error`); once at the start of a run of failures of that peer. |
| `fleet_peer_backoff` | info | A failed peer is skipped for the next cycles (fields `peer`, `skip_cycles`, `failures`); at each failure. |
| `fleet_peer_rows_rejected` | warning | A peer served rows this worker did not accept (field `rejected`: counts by reason `scope` or `origin`); once per peer per cycle. |

## Settings

| Key | Default | Meaning |
|---|---|---|
| `fleet.registry.provider` | `filesystem` | `filesystem`, `static`, `none`, [`postgres`](#postgres), [`redis`](#redis), or a plugin name. `consul` is [reserved](#providers). |
| `fleet.registry.connection` | `main` | The entry of `connections.<provider>` that the `postgres` and `redis` providers use. The name `consul` is reserved. |
| `fleet.registry.heartbeat_seconds` | `10` | Seconds between card publishes. |
| `fleet.registry.refresh_seconds` | `15` | Seconds between reads of the peer cards. |
| `fleet.registry.offline_after_seconds` | `60` | A card older than this is offline. At least `2 * heartbeat_seconds` and at least `10`. |
| `fleet.registry.prune_after_seconds` | `86400` | Cards older than this are deleted. At least `offline_after_seconds`. |
| `fleet.registry.resilience.*` | see [Resilience](#resilience) | Time limit, retries, backoff and breaker for the registry's provider operations. |
| `fleet.registry.filesystem.dir` | `""` | Card folder; empty means `<shared.dir>/fleet`. |
| `fleet.registry.static.peers` | `[]` | The `static` peer list: `name`, `cluster`, `url`. |
| `fleet.registry.consul.mode`, `fleet.registry.consul.service_name` | `publish`, `drakkar` | Settings of a `consul` provider. |
| `fleet.registry.postgres` | `{}` | Block of the `postgres` provider. It has no keys: the schema and the pool come from `connections.postgres.<connection>`. |
| `fleet.registry.redis` | `{}` | Block of the `redis` provider. It has no keys: the address, the credentials and the key prefix come from `connections.redis.<connection>`. |
| `fleet.registry.plugin_options` | `{}` | Options for a provider plugin. |
| `connections.*` | none declared | Named connections to Consul, Redis and Postgres; the `postgres` providers read `connections.postgres.*` and the `redis` providers read `connections.redis.*`. See [Connections](#connections). |

Every key has a `DK_` environment override, for example
`DK_FLEET__REGISTRY__PROVIDER=static`. The full table with constraints is in
[Configuration](configuration.md#fleet-registry).

## What the registry does not do

- It does not authenticate anything. A card is a public statement of where
  a worker answers; anyone who can write to the card store (the folder, for
  the `filesystem` provider) can publish a card. Protect the store like the
  rest of `shared.dir`.
- It is not used by the pipeline: a worker with no peers processes messages
  exactly like one in a large fleet.
- It does not carry cache data. The cards carry only the marks of the
  newest cache rows; the rows themselves travel over HTTP, from the URL
  on the card (see [Cache](cache.md#cache-exchange)). A card in the folder
  therefore decides where a worker sends its exchange requests.
