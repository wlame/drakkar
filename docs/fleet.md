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

The registry is part of the optional feature `autodiscover`, and it is **off
by default**. A worker with it off publishes no card, reads no peers, runs no
election and pulls no cache rows. The workers list shows only this worker, and
a declared `scope='cluster'` periodic task fails startup.

To turn it on, set `autodiscover.enabled: true` and choose one provider. The
registry needs the API server (`api.enabled: true`), because peers and browsers
reach a worker through the URL on its card. With the API off, the config does
not load. The UI may be off: the card then says so in `features.ui`, and the UI
of a peer leaves that worker out.

## Providers

A **provider** is the part of the feature that talks to a store or a service.
One provider serves both the registry (this page) and the
[election of cluster periodic tasks](local-databases.md#the-election-provider),
the archive included. Each has its own caller with its own circuit breaker, and
both take their settings from one [`autodiscover.resilience`](#resilience)
block. The metrics keep the labels `feature="registry"` and `feature="election"`.
The rest of this page is about the registry.

`autodiscover.provider` selects where cards live:

| Provider | Where the cards are | Liveness |
|---|---|---|
| `filesystem` | one JSON file per worker in `storage.shared_dir` | from the file times on the file server |
| `postgres` | one row per worker in the table `fleet_workers` of a Postgres schema | from the database server's clock (`heartbeat_at` against `now()`) |
| `redis` | one key per worker plus an index of workers, under the state prefix of a Redis connection | from the Redis server's clock (the time of the last publish against `TIME`) |
| `consul` | one key per worker under the state prefix of a Consul connection, held by a session of the worker | the session's time to live (online while the key has a session; no age) |

`postgres`, `redis` and `consul` are built in; see [`postgres`](#postgres),
[`redis`](#redis) and [`consul`](#consul). Each keeps its data in a service
reached through a [named connection](#connections).

A name that uses a connection (`consul`, `redis` and `postgres`) is checked
when the config loads: the connection it uses must be declared. Without
`connections.consul.main`, for example, the load fails with
`autodiscover.connection 'main' is not defined under connections.consul`.

The load fails for any other provider name, with or without `enabled`:
`autodiscover.provider must be one of consul, filesystem, postgres, redis, got 'etcd'`.
It also fails when the feature is enabled with no provider:
`autodiscover.enabled is true but autodiscover.provider is empty; choose one of consul, filesystem, postgres, redis`.

### `filesystem`

```yaml
storage:
  dir: /var/lib/drakkar          # this worker's own files
  shared_dir: /mnt/fleet         # seen by every worker of the fleet
autodiscover:
  enabled: true
  provider: filesystem
```

The cards go to `<storage.shared_dir>/fleet/workers/`. An enabled `filesystem`
provider with an empty `storage.shared_dir` fails the load:
`autodiscover.provider filesystem needs storage.shared_dir`.

The folder is best effort: if it cannot be written, the worker runs anyway,
counts each failed publish in
`drakkar_fleet_registry_errors_total{op="publish"}` and retries at the next
heartbeat.

Layout and rules:

- One card per worker: `workers/<worker>.json`. Characters of the worker
  name outside `[a-zA-Z0-9_-]` become `_` in the file name, so `search.1`
  is stored as `search_1.json`. Two worker names that differ only in such
  characters share one file; give workers names that stay distinct.
- A card is written to `workers/.<worker>.json.<instance>.tmp` and renamed
  over the card, so a reader never sees half a card. A temp file that a
  crashed writer left behind is deleted once it is older than
  `prune_after`.
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
- A card older than `prune_after` (default `24h`) is
  deleted by the next worker that lists the folder.
- At a clean stop the worker deletes its own card, but only while the card
  still holds this process's instance: if another process with the same
  worker name has written it since, the card stays.

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
autodiscover:
  enabled: true
  provider: postgres
  connection: main             # the default
```

Set the connection string in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:...@db.internal/drakkar'
```

The registry needs the API server (`api.enabled: true`), as with every
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
  `prune_after`, then it reads every row with its age. Both steps
  use the server's `now()`, so a worker that lists also prunes. The age is
  `now() - heartbeat_at`, computed by the server in the same query.
- **Liveness.** The provider gives no `online` verdict. The
  [liveness rule](#liveness) applies: a peer is online while its age is at
  most `offline_after`. No worker's wall clock enters an age, so
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
The pool is opened once,
at the first provider start, and the registry and the election share
it when they name the same connection. The worker closes it at shutdown,
after both providers have stopped (see [Connections](#connections)).

**The schema.** At the first successful `start` of the provider:

- With `manage_schema: true` (the default), the worker takes an advisory
  lock on the schema name and runs five `CREATE ... IF NOT EXISTS`
  statements in one transaction: the schema, `fleet_workers` with its two
  indexes, and `task_locks`. The lock makes workers that start together
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

Three more flags print the recorder events table of
[`persistence: postgres`](recorder-postgres.md) after the provider
statements, separated by one empty line:

```bash
drakkar-schema postgres --schema=drakkar --recorder --table-prefix=heavy --retention-hours=24 | psql "$ADMIN_DSN"
```

`--recorder` adds the table, its comment `retention_hours=<n>` and its three
indexes. `--table-prefix=<p>` names the table `<p>_recorder_events`
(default: `recorder_events`). `--retention-hours=<n>` sets the comment, 1 to
168, default 6. The prefix and the retention flags need `--recorder`; a
value that breaks its rule, a flag without `--recorder`, or
`--recorder=<value>` is a usage error with exit code 2. The role of the
worker must own the recorder table, because the worker creates and drops its
partitions (see
[Schema management and ownership](recorder-postgres.md#schema-management-and-ownership)).

**The database role.**

- With `manage_schema: true`, the worker runs the DDL at every first start,
  also when the objects already exist. The role needs `CREATE` on the
  database (`CREATE SCHEMA IF NOT EXISTS` checks it even when the schema
  exists) and `CREATE` on the schema (for the tables and the indexes). Owning
  the schema is not enough. The role must also own the table
  `fleet_workers`: its two `CREATE INDEX IF NOT EXISTS` statements check
  ownership before they skip an index that exists. The simplest case is to
  let the worker role create the objects. For tables that another role
  created, set `manage_schema: false`. `task_locks` needs no ownership.
- With `manage_schema: false`, the role needs `USAGE` on the schema and
  `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the tables it uses
  (`fleet_workers`; `task_locks` for the election). It needs no right to
  create anything, so an operator creates the objects once with
  `drakkar-schema`. See [Security](security.md#provider-credentials).

**During an outage.** A connection that is refused or lost, a server that
restarts and a transaction that must be run again are transient errors. They
are retried under `autodiscover.resilience` (see [Resilience](#resilience)).
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
autodiscover:
  enabled: true
  provider: redis
  connection: main             # the default
```

Set the URL in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__REDIS__MAIN__URL='redis://drakkar-fleet:...@redis.internal:6379/3'
```

Without it the entry uses `redis://localhost:6379/0`. The registry needs the
API server (`api.enabled: true`), as with every provider. Every worker of the
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
  card with a time to live of `prune_after`, adds the worker to the
  index with the server's `TIME` as its score, and gives the index the same
  time to live.
- **List.** One script, so a database full of other keys costs nothing
  extra. It reads the server's `TIME`, reads the index, fetches each card,
  and removes the index members whose card key has expired. The age of a card
  is the server's time now minus the stored time of its last publish, floored
  at 0.
- **Pruning.** Redis deletes a card when its time to live ends, so the card
  of a dead worker leaves the list `prune_after` after its last
  publish. The index gets the time to live of the last publish too. When no
  worker publishes for that long, the index goes as well, and cards that are
  still alive at that moment are not listed until their workers publish
  again. A live worker comes back at its next heartbeat.
- **Lost keys.** A Redis server that restarts without its data, or a flush
  of the database, deletes the cards and the index. Eviction can delete them
  too: every key of the providers has a time to live, so any `volatile-*`
  and any `allkeys-*` `maxmemory` policy can evict a live card or the index.
  A database number of its own does not protect them, because the memory
  limit and eviction cover the whole server. The workers publish their cards
  again at the next heartbeat, and the list is complete again. Under the
  default policy, `noeviction`, a full server evicts nothing and answers a
  script that needs more memory with an error that names `OOM`. That error
  is final and counts as a failed operation for the breaker. A lease of a
  cluster task is lost the same way (see [The redis election
  provider](local-databases.md#the-redis-election-provider)).
- **Liveness.** The provider gives no `online` verdict. The
  [liveness rule](#liveness) applies: a peer is online while its age is at
  most `offline_after`. No worker's wall clock enters an age, and the
  `prune_after` of the reading worker does not either, so workers with
  different `prune_after` values can share one server.
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
[Configuration](configuration.md#connections)). The client is created once, at the first provider start, and
the registry and the election share it when they name the same
connection. The worker closes it at shutdown, after both providers have
stopped (see [Connections](#connections)).

**The URL.** `redis://` is plain TCP and `rediss://` is TLS. With
`rediss://`, the client verifies the certificate of the server against the
system's default certificate store and checks that the certificate names the
host of the URL. This cannot be switched off. Put a private certificate
authority into that store; the environment variables `SSL_CERT_FILE` (a file
of certificates) and `SSL_CERT_DIR` (a directory of them) replace the default
locations of the store. The database number is the path, for example `/3`.
A `unix://` URL names the socket file instead, by its absolute path, for
example `unix:///run/redis/redis.sock`.

A URL with query options (`?...`) is refused when the provider starts, with
`redis connection: the url cannot carry query options`. Every other URL goes
through one check before the client reads it, and a URL that fails the check,
or that the client cannot parse, fails the start with
`redis connection: the url cannot be parsed`. Neither message quotes the URL.
The check keeps only the URLs whose meaning is plain:

- Every character is printable ASCII. A space, a tab, a line break, any other
  control character and any non-ASCII character are refused. A trailing
  newline is an error, not something the worker removes: a URL read from a
  secret file often ends with one, so remove it there.
- Every `%` starts an escape of two hex digits, and the escapes decode to
  UTF-8.
- The user name and the password hold only letters, digits, the characters
  `-._~!$&'()*+,;=:@` and escapes. Write any other character as its escape:
  `%` as `%25`, `^` as `%5E`, `/` as `%2F`, `#` as `%23`, `?` as `%3F`, a
  space as `%20`, and a non-ASCII character as the escapes of its UTF-8 bytes
  (`ä` is `%C3%A4`).
- The host is a name of letters, digits, `.`, `_` and `-`, or an IPv6 address
  in brackets (`[::1]`) without a zone index. With no host, the client uses
  `localhost`.
- A port, when the URL has one, is 1 to 65535, written with at most five
  digits. With no port, the client uses 6379.
- The scheme is `redis`, `rediss` or `unix`, in any case.
- The path of a `redis://` or `rediss://` URL is empty, `/`, or `/` followed
  by at most ten digits 0-9 that make a number up to 2147483647, the largest
  that the server's `SELECT` takes. Any other path, such as `/three`, `/0/extra`, `/0/`
  or `/+1`, is refused. The path of a `unix://` URL starts with `/`.

The registry tries a failed start again at the next heartbeat, like any
failed start.

**The server.** Redis 5.0 or later, standalone or a primary behind a fixed
address. A script reads `TIME` and then writes, which Redis allows from
version 5.0 on, unless the server sets `lua-replicate-commands no`. On Redis
5.x and 6.x that setting makes these scripts fail with
`Write commands not allowed after non deterministic commands`; Redis 7.0 and
later ignore the setting. Redis Cluster is not supported, because the scripts
touch keys that are not all named in `KEYS`.

**The Redis user.** Give the providers a Redis user of its own, with these
rights: `~<prefix>* +@read +@write +@scripting +@connection +time`. The key
pattern `~<prefix>*` limits which keys the user may name in a command. The
other rights are categories, so they allow more commands than the providers
send, for example `FLUSHDB` and `KEYS`, and the key pattern does not limit
those two. A Redis ACL does not limit databases either. Use a database number
of its own for the fleet all the same, so that the keys of the providers stay
apart from other data. ACL users need Redis 6.0 or later; on Redis 5.0, use
`requirepass` and a database number of its own. See
[Security](security.md#provider-credentials) for a complete `ACL SETUSER`
line.

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

### `consul`

One key per worker in the key-value store of a Consul agent, each held by a
session of its worker. It fits a deployment that already runs Consul and has
no shared folder for the cards. Consul keeps the time: a card is online while
the session that holds its key lives.

```yaml
connections:
  consul:
    main:
      address: http://127.0.0.1:8500   # the HTTP API of a Consul agent; the default
      key_prefix: drakkar              # the default; every key starts with it
      # token: set from the environment, see below
autodiscover:
  enabled: true
  provider: consul
  connection: main             # the default
  offline_after: 1m    # the default; it also sets the session's time to live
```

Set the ACL token in the environment, not in the YAML file:

```bash
export DK_CONNECTIONS__CONSUL__MAIN__TOKEN='...'
```

Without a token the requests go out without one, and Consul treats them as
the anonymous token. The registry needs the API server (`api.enabled: true`), as
with every provider. Every worker of the fleet uses the same provider and the
same Consul cluster, datacenter and `key_prefix`; workers on different stores
do not see each other.

What it keeps and does:

- **The keys and the sessions.** With the default prefix,
  `drakkar/fleet/workers/<worker>` is the card of one worker: its canonical
  bytes as text, the same bytes the `filesystem` provider writes to a file. In
  `<worker>`, characters of the worker name outside `[a-zA-Z0-9_-]` become
  `_`. A session of the worker holds the key. The session is named
  `drakkar-<worker>-<instance>` (the same stem, then the 16 hex characters of
  the instance). It is created with `Behavior: delete`, so the key goes when
  the session ends, and `LockDelay: 0s`, so another session can take a free
  key at once. The `key_prefix` may have more than one segment, for example
  `team/drakkar`.
- **Publish.** At every heartbeat and every state change the provider renews
  the worker's session, and then writes the card to its key with
  `?acquire=<session>`. A worker with no session, or one whose renew Consul
  answers with `404` (the session ended), creates a new session first. The
  session that already holds the key writes it again, so a republish is the
  same call.
- **The session's time to live.** It is half of `offline_after`,
  rounded up, and at least 10 seconds, which is the least that Consul
  accepts. With the default of 60 it is 30 seconds. Consul ends a session no
  earlier than its time to live after the last renew, and at most twice its
  time to live after it. So the card of a dead worker leaves the list by
  `offline_after`, one second later when that value is odd (the time
  to live is rounded up), and by 20 seconds when the value is below 20 (this
  also holds for an odd value below 20). A live worker renews at every
  heartbeat. If a session ends under a live worker (a heartbeat came late, or
  the agent lost its state), the next publish meets `404` on the renew, creates
  a new session and writes the card again. With this provider
  `offline_after` is at most 86400; a larger value fails the load
  with `autodiscover.offline_after (<n>) must be <= 86400 when autodiscover.provider is consul`.
- **Heartbeat margin.** With this provider, keep `offline_after` at
  least `4 * heartbeat`; the defaults (10 and 60) do. The time to live
  is then at least twice the heartbeat, so a heartbeat that comes a little
  late still renews the session in time. The load accepts
  `offline_after = 2 * heartbeat`, but then the time to live
  equals the heartbeat (for a heartbeat of 10 seconds or more), and Consul
  may end a session as early as its time to live after the last renew: a
  renew that comes a little late can find the session ended, and the card
  is missing from the list until that publish creates a new session and
  writes the card again.
- **List.** One read of every key under `<key_prefix>/fleet/workers/`. A key
  that cannot be a card key is skipped without a log line: the folder key
  itself, which a Consul UI creates, and a key nested deeper. A card is
  online while its key has a session, and the workers list shows it with no
  age, so its `last_seen_ts` is `null`. A key that no session holds was
  written by something other than the provider. It is listed offline, and
  nothing deletes it: `prune_after` is not used with `consul`, because
  Consul deletes a card when its session ends. Remove such a key in Consul.
- **Liveness.** The provider gives its own `online` verdict, so no worker's
  wall clock and no age enter it. See [Liveness](#liveness).
- **Duplicate names.** The card key comes from the sanitized worker name, so
  one name has one card. A worker that restarts under its old name finds the
  key held by its predecessor's session. It knows the session by its name,
  destroys it and takes the card at once, without waiting for the session to
  end. A live twin does the same at its next heartbeat, so two live processes
  with one name take the card from each other at every heartbeat, and each
  logs `fleet_duplicate_worker_name` for the other (see [Duplicate worker
  names](#duplicate-worker-names)). Names that differ only in characters
  outside `[a-zA-Z0-9_-]` share one card in the same way. A key that a
  session of any other name holds is never taken: the publish fails with
  `consul registry: the card key of <worker> is held by a session of another worker`.
- **Withdraw.** At a clean stop the worker destroys its session, which
  deletes its card. A worker that never published sends nothing. If a twin has
  taken the card over, the destroy changes nothing. If the destroy fails, the
  session ends by its time to live.
- **A card that does not decode** is skipped with `fleet_card_invalid` and the
  reason, once for that worker name while the name stays listed. The other
  cards are still listed.
- **Start.** The provider asks the agent for the cluster's leader
  (`GET /v1/status/leader`). So `provider_started` means that the agent
  answered and that the cluster has a leader. It does not mean that the token
  has the rights (see below).

The address, the token, the datacenter and the key prefix come from the
connection: `connections.consul.<connection>` (see
[Configuration](configuration.md#connections)). The client is created
once, at the first provider start, and the registry and the election
share it when they name the same connection. The worker closes it at shutdown,
after both providers have stopped (see [Connections](#connections)).

**The agent.** The provider talks to the HTTP API of one Consul agent, with
`http://` or `https://` in the `address` and the paths `/v1/status/leader`,
`/v1/session/...` and `/v1/kv/...`. The address is checked when the provider
starts. Every character must be printable ASCII. The host is a name of
letters, digits, `.`, `_` and `-` (so an IPv4 address is fine) or an IPv6
address in brackets without a zone index. An empty host is refused. A port is
optional, 1 to 65535, with at most five digits; a `:` with no port digits is
refused. A path is optional too: segments of letters, digits, `.`, `_`, `~` and
`-`, none of them exactly `.` or `..`. A user name, a query and a fragment are
refused. An address that fails the check fails the start with
`consul connection: the address cannot be parsed`, and a token with a space or
a non-ASCII character fails it with
`consul connection: the token cannot be sent in a header`; neither message
quotes the value. The client follows no redirect, so a `3xx` answer is an
error. For `https://` and the proxy variables, see
[Security](security.md#provider-credentials).

A session belongs to the node of the agent that created it. Consul also ends
the sessions of a node when the node's health check (`serfHealth`) fails,
whatever their time to live. So when the cluster marks the agent's node as
failed, the cards of all workers that use that agent leave the list, and the
workers write them again at the next heartbeat after the node is healthy.
Point the workers at an agent that stays reachable and healthy, usually the
agent of their own host.

**The token.** Give the providers a token of their own, with these rights, in
which `drakkar/` is the `key_prefix` of the connection and a `/`:

```hcl
key_prefix "drakkar/" { policy = "write" }
session_prefix "" { policy = "write" }
```

The leader request needs no right. [Security](security.md#provider-credentials)
says what these rights allow beyond the providers' needs. A wrong token does
not show at the start, so look for it at the first publish:

- A token that Consul does not know (`ACL not found`), or one without a right
  (`Permission denied: token with AccessorID ... lacks permission
  'session:write' ...`), passes `start`, because the leader request needs no
  right. The first publish fails with a `403`. A `403` is final: one attempt,
  no retry, and it counts as a failed operation for the breaker. The
  `provider_call_failed` line carries the first line of Consul's answer, cut to
  200 characters, for example
  `consul: PUT /v1/session/create answered 403: Permission denied: ...`.
- A token that cannot read the card keys gets an answer that looks like an
  empty prefix: Consul answers the read with `404` and the header
  `X-Consul-Results-Filtered-By-Acls: true`. The provider treats an answer
  with that header as an error, so `list` fails with
  `consul: GET /v1/kv/drakkar/fleet/workers/ answered with results hidden by the token's rights`
  and does not show an empty fleet. The workers list keeps its last snapshot,
  and `drakkar_fleet_registry_snapshot_age_seconds` grows. A token that cannot
  read a session fails the takeover of a predecessor's card in the same way.
  With no token at all Consul does not flag the read as hidden, so `list` can
  read an empty fleet without an error; the failed publish shows the problem.
- A `datacenter` that Consul does not know makes every request fail with
  `500` and `No path to datacenter`, the leader request of `start` included. A
  `500` is transient, so the operation is retried under the
  [`resilience`](#resilience) block, and it fails with
  `consul: GET /v1/status/leader answered 500: No path to datacenter`. The
  breaker opens after `failure_threshold` failed operations, and then one
  probe goes out after each cooldown. Fix the name; nothing else cures it.

**During an outage.** The client does not retry on its own; the
[`resilience`](#resilience) block of the registry is the only retry and the
only time limit. A connect gives up after 5 seconds, and a read or a write
ends at `timeout_seconds` of the block. A connection that is refused or lost,
a timeout, and the statuses `429`, `500`, `502`, `503` and `504` are transient
errors, and they are retried. Any other status is final: a `403` or a `400`,
for example, ends the operation at once, and it counts as a failed operation
for the breaker. When operations keep failing, the breaker opens after
`failure_threshold` failed operations and the provider is skipped until the
cooldown has passed. The workers list keeps its last snapshot, and
`drakkar_fleet_registry_snapshot_age_seconds` grows. The pipeline does not wait
for any of this.

A worker that cannot reach Consul for longer than its session lives drops out
of the lists of the others, because Consul ends the session and deletes the
card. When Consul is reachable again, the next probe operation closes the
breaker. The worker's renew then meets `404`, creates a new session and writes
the card again. The registry recovers without a restart; nothing needs to be
reset, because the client opens new connections by itself.

## The card

A card is a JSON object, at most 4096 bytes:

```json
{
  "backend": "python",
  "cluster": "search",
  "features": {"cache": {"cluster": {"key": "match|a", "ms": 1760348411000}}, "periodic": {"cluster_hash": "4f53cda18c2baa0c"}},
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
| `url` | yes | Where the worker's API server answers: `api.public_url` when set, else `http://<outbound ip>:<api.port>/`. Not empty. |
| `instance` | no | 16 random hex characters, new at every process start. Tells two processes with one worker name apart. |
| `backend` | no | The implementation that runs the worker. |
| `version` | no | The worker's release, as `/api/v1/identity` reports it. |
| `state` | no | `starting`, `running` or `draining`. |
| `started_at`, `heartbeat_at` | no | Process start and the time of this publish, on the writer's clock. Informational only: liveness never uses them. |
| `features` | no | An object for optional capabilities. Unknown keys are ignored. `features.cache` is present while the worker serves the [cache exchange](cache.md#cache-exchange) (the cache, `storage.dir` and `cache.exchange` are on): for each scope (`cluster`, `global`), `{"ms": <updated_at_ms>, "key": <key>}` of the newest row this worker originated. A scope with no such row is absent. A peer pulls only when a mark is past its cursor for this worker. A mark carries at most the first 128 characters of its key, so a long cache key never pushes the card over its 4096-byte limit. `features.periodic` is on every card from the first publish: `{"cluster_hash": "<16 hex>"}`, the hash of the worker's cluster periodic task definitions (see [Definition hash](periodic-tasks.md#definition-hash)). Peers of the same cluster compare it after each refresh and warn on a difference; a hash in any other form counts as unknown. |

A card whose required field is missing, empty (`worker`, `url`) or of the
wrong type, or whose optional field has the wrong type, is skipped; the
worker logs `fleet_card_invalid` with the reason once for that worker.
Unknown fields and unknown `state` values are kept and ignored.

The card is written with sorted keys, no spaces between tokens, non-ASCII
characters as raw UTF-8, and no trailing newline, so the same card has the
same bytes whichever worker wrote it.

## States

A worker publishes its card:

- with `starting` right after its API server listens;
- with `running` once its sources have started;
- with `draining` as soon as shutdown begins;
- again every `heartbeat` (default 10) with the current state.

A state change is published at once, not at the next heartbeat. At the end
of a clean stop, after the recorder's last write and before the API server
stops, the worker withdraws its card (see [Shutdown](#resilience) for when it
does not).

## Liveness

The workers list shows a peer as **online** when its card is no older than
`autodiscover.offline_after` (default 60), and as **offline**
when it is older. `offline_after` must be at least twice
`heartbeat`, so one late heartbeat never marks a worker offline, and
at least 10, whichever provider is used.

A provider whose service decides liveness itself can say so. It sets
`online` on the `PeerCard` it lists (for example, a card that exists only
while a session of its owner lives). The rule is applied in this order:

1. `online` is `true` or `false`: that is the answer. The age is not
   compared with `offline_after`.
2. `online` is not set and the age is known: online while the age is at most
   `offline_after`.
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

The `consul` provider gives an `online` verdict and no age. A card is online
while a session holds its key and offline when its key has no session. Consul
ends the session of a worker that stopped renewing, and the card goes with it.
So `offline_after` is compared with no age for these cards: it sets the
time to live of the session (see [`consul`](#consul)), and with this provider
it should be at least four times `heartbeat`, not only twice.

Each refresh (every `refresh`, default 15) keeps the age of every
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
Every row also has `cluster_tasks_hash` and `cluster_tasks_match`, which
compare the worker's cluster periodic task definitions with this worker's
(see [Definition hash](periodic-tasks.md#definition-hash)). The archive
runs only on workers with `ui.recorder.persistence: sqlite`, so a cluster
that mixes `none` or `postgres` workers with `sqlite` workers shows
`cluster_tasks_match: false`. A cluster that mixes `none` and `postgres`
workers shows no mismatch (neither runs a cluster task for the recorder), but
its history is split: the trace with history on a `postgres` worker reads the
Postgres table, on a `none` worker the sealed files. Give every worker of a
cluster the same persistence.

## Connections {#connections}

A provider that keeps its data in a service (Consul, Redis, Postgres) does
not carry the address and the credentials itself. The config declares them
once, under `connections.<service>.<name>`, and a feature names the entry.
This release ships the `postgres`, `redis` and `consul` providers, which read
`connections.postgres.<name>`, `connections.redis.<name>` and
`connections.consul.<name>`.

```yaml
connections:
  postgres:
    main:                                  # used when a feature names no connection
      schema: drakkar
    fleet_database:
      schema: drakkar_fleet
      manage_schema: false
autodiscover:
  enabled: true
  provider: postgres
  connection: fleet_database             # default: main
```

Each `postgres` entry also needs its `dsn`, set from the environment, for
example `DK_CONNECTIONS__POSTGRES__MAIN__DSN` and
`DK_CONNECTIONS__POSTGRES__FLEET_DATABASE__DSN`. A `redis` entry has its
`url` the same way, for example `DK_CONNECTIONS__REDIS__MAIN__URL`, and a
`consul` entry its `token`, for example `DK_CONNECTIONS__CONSUL__MAIN__TOKEN`.

- `connections` has one map per service type: `consul`, `redis` and
  `postgres`. You choose the names. A feature that names none uses `main`.
- `autodiscover.connection` and `autodiscover.connection` name
  an entry. The `postgres`, `redis` and `consul` providers read it.
  `filesystem` ignores it. The config load checks the key by
  provider name, so a provider named `consul`, `redis` or `postgres` needs its
  connection declared; otherwise the load fails with
  `autodiscover.connection 'main' is not defined under connections.consul`.
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
makes five: `start`, `publish`, `list`, `withdraw` and `stop`. The election
of cluster periodic tasks has its own. An operation runs under the
`resilience` block of its feature: `autodiscover.resilience` and
`autodiscover.resilience`.
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
the operation at once. The `consul` providers raise `TransientError` for a
lost or refused connection and for the statuses `429`, `500`, `502`, `503`
and `504`; a client timeout counts as a timeout. Every other status, such as
`403`, ends the operation at once. With the defaults one operation takes at
most 3 × 5 s plus the waits of 0.2 s and 0.4 s, which is 15.6 s. The
registry publishes the next card only after the previous publish ended, so
keep the longest operation well below `offline_after`.

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
`start` that fails or times out does not switch the registry off.
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
in the store until `prune_after`, and peers show the worker as
offline once the card is older than `offline_after`. With `consul`, the
session ends by its time to live and Consul deletes the card with it, so the
card leaves the list by `offline_after` (one second later for an odd
value, and by 20 seconds for a value below 20; see [`consul`](#consul)).

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
- The `consul` provider awaits its requests on the event loop. The client has
  no read limit of its own: the time limit of an attempt ends a request that
  does not answer, so a paused agent holds up only that attempt. A connect
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

Every provider operation, of the registry and of the election, is
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
| `fleet_peer_request_failed` | warning | A request to a peer failed (fields `peer`, `url`, `endpoint`, `outcome`, `error`); once at the start of a run of failures of that peer. |
| `fleet_peer_backoff` | info | A failed peer is skipped for the next cycles (fields `peer`, `skip_cycles`, `failures`); at each failure. |
| `fleet_peer_rows_rejected` | warning | A peer served rows this worker did not accept (field `rejected`: counts by reason `scope` or `origin`); once per peer per cycle. |

## Settings

| Key | Default | Meaning |
|---|---|---|
| `autodiscover.enabled` | `false` | Turn on the registry and the election of cluster tasks. |
| `autodiscover.provider` | `''` | [`filesystem`](#filesystem), [`postgres`](#postgres), [`redis`](#redis), [`consul`](#consul). Required when enabled. |
| `autodiscover.connection` | `main` | The entry of `connections.<provider>` that the `postgres`, `redis` and `consul` providers use. |
| `autodiscover.heartbeat` | `10s` | Time between card publishes. |
| `autodiscover.refresh` | `15s` | Time between reads of the peer cards. |
| `autodiscover.offline_after` | `1m` | A card older than this is offline. At least `2 * heartbeat` and at least `10s`. With `consul` it sets the time to live of the worker's session (half of it, at least 10 seconds) and is at most `24h`. |
| `autodiscover.prune_after` | `24h` | Cards older than this are deleted. At least `offline_after`. The `consul` provider does not use it: Consul deletes a card when its session ends. |
| `autodiscover.resilience.*` | see [Resilience](#resilience) | Time limit, retries, backoff and breaker for the provider operations of the registry and the election. |
| `connections.*` | none declared | Named connections to Consul, Redis and Postgres; the `consul` providers read `connections.consul.*`, the `postgres` providers read `connections.postgres.*` and the `redis` providers read `connections.redis.*`. See [Connections](#connections). |

Every key has a `DK_` environment override, for example
`DK_AUTODISCOVER__PROVIDER=redis`. The full table with constraints is in
[Configuration](configuration.md#autodiscover).

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
