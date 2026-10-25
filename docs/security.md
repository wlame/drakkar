# Security posture

!!! danger "Drakkar is an internal tool"

    The operator UI, the `/api/v1` JSON and WebSocket surface, and the
    optional HTTP ingress are built for a **trusted, private network**.
    Do not expose them to the public internet, to a shared corporate
    network you do not control, or to any user population wider than the
    engineers who operate the service.

    Drakkar ships authentication, rate limiting and a number of input
    checks. Treat all of them as **defence in depth — not a security
    perimeter.** The perimeter is your network and the application you
    build on top of Drakkar; the framework does not attempt to be one.

## Where the boundary actually is

A Drakkar worker is infrastructure that sits *inside* a system you own —
what the rest of these docs call a **private contour**: a VPC, an internal
cluster network, an operator-only ingress. It has no user model, no roles,
no tenancy, and no notion of an untrusted caller: anyone who can reach a
port and present the one configured token — or, in the default
configuration, anyone who can reach the port at all — is treated as an
operator of that worker.

That is a deliberate design choice, not an omission. It keeps the operator
tooling useful, and it means the security decisions live where the context
is: with the engineers who deploy the service and with the application in
front of it.

Concretely, the responsibility splits like this.

| Layer | Owns |
|---|---|
| **Your network** | Who can reach the ports at all. This is the real control. |
| **Your ingress / reverse proxy** | TLS, per-client connection limits, request-rate limiting, read timeouts, and any identity your organisation uses (SSO, mTLS, VPN). |
| **Your application** | Authorising *your* end users, validating *their* input, and deciding what data may leave your system. Drakkar sees messages that already passed through it. |
| **Drakkar** | Not shipping obvious footguns: bounded requests, no arbitrary file reads, secrets masked in operator views, an opt-in token for casual protection. |

## What Drakkar does provide

These reduce blast radius and catch mistakes. They are worth turning on.
None of them is designed to withstand a determined attacker who already has
network access.

| Control | What it does |
|---|---|
| **Loopback default** | `ui.host` defaults to `127.0.0.1`, so a worker is not reachable off-host until you change it. |
| **WebSocket Origin check** | Runs on every `/ws` handshake, token or not. A page that is not itself on loopback cannot open a socket to a worker on the browser's loopback — the one route that would otherwise reach past the loopback bind. |
| **UI bearer token** | `ui.auth_token` gates every page and API route (`Authorization: Bearer`, or `?token=` for browser navigation), compared with `secrets.compare_digest`. `/healthz` and `/readyz` stay public for Kubernetes, and the routes under `/api/v1/peer/` stay public for the cache exchange (see [below](#the-public-peer-route)). |
| **WebSocket handshake auth** | With a token set, `/ws` authenticates inside the handshake and validates `Origin` against `ui.allowed_ws_origins` (or the `Host` header), rejecting every other cross-origin handshake. |
| **Cross-origin read rule** | Only three read paths answer cross-origin browser requests: `/api/v1/debug/trace`, `/api/v1/debug/trace-by-label` and `/api/v1/debug/databases`, which the UI reads from every worker of the cluster. They use the same origin rule as `/ws`. The preflight needs no token; the read itself still needs one when a token is set. No other path sends CORS headers. See [below](#cross-origin-reads). |
| **Startup warning** | A worker running the UI without a token logs `ui_unauthenticated` at startup, naming the bind and both opt-in paths. |
| **Independent kill switches** | `ui.probe_enabled` and `ui.merge_enabled` close the two non-read-only endpoints, whether or not a token is set. |
| **Webapp auth + rate limits** | Per-client bearer tokens and a per-client rpm sliding window on the HTTP ingress, with `max_concurrent` shedding over-capacity requests. |
| **Path containment** | Database download and merge take paths relative to `shared.dir` and serve only three shapes: `history/<cluster>/<name>.db`, `history/<cluster>/<archive>.db.gz` and `merged/<name>.db` (merge takes only the `.db` ones). The download answers `404` for any other path, an unsafe segment, a symlink, a path that resolves outside `shared.dir`, or a missing file; the merge answers `400` for a path of another shape. A worker's own files in `storage.dir` are never served by path. The one exception takes no path: `GET /api/v1/debug/download-live` serves a snapshot of the worker's own live recorder file (`source=file`) or of its recorder memory store (`source=memory`), behind the token. The memory snapshot holds the same data as the file: event rows with their task env redacted as in the file, outputs when `store_output` is on, and the redacted `worker_config` row. Its temporary file in `storage.dir` is owner-only (`0600`, created exclusively) and removed before the answer starts; without `storage.dir` the copy stays in RAM. |
| **Filtered peer rows** | The cache exchange applies only the rows a peer originated itself, in the scopes asked of that peer. Peer requests ignore the proxy environment variables and do not follow redirects. |
| **Owner-only files** | Recorder databases, meta files, archives and merge results are created with mode `0600`; `storage.dir`, each cluster history folder, each cluster task lock folder (`tasks/<cluster>/`) and `merged/` are created with mode `0700`. |
| **Secret masking** | The config reference masks secret-flagged fields (including the `token`, `url` and `dsn` of every `connections` entry), `executor.env`, every `*.client_config` and the `plugin_options` of the provider plugins by key name; the flight recorder applies the same redaction before storing task env. |
| **No input echo in config errors** | A load error of the framework config names the key and the rule but does not add the input of the section that failed, so a wrong `connections` block cannot print a connection string. The messages of the framework still quote a value that is not a secret, such as a schema name. The model of your own `app:` section does not inherit this. |
| **Request bounds** | Body size and read-time caps, header-size caps, keep-alive reaping, a bounded probe queue, and deadlines on Kafka reads — so one client cannot hold a worker open indefinitely. |

## What Drakkar does not provide

Stated plainly, so nobody plans around a control that is not there:

- **No user model, roles or permissions.** There is one token per surface.
  A token holder can do everything that surface allows.
- **No audit trail of operator actions.** The flight recorder records what
  the *pipeline* did, not who asked for it.
- **No TLS.** Both servers speak plain HTTP. Terminate TLS in front.
- **No CSRF protection on mutating requests.** A browser session on a
  reachable, unauthenticated UI can be driven by another origin.
- **No token on the peer route.** `GET /api/v1/peer/cache/changes` is
  public, with or without `ui.auth_token`. See
  [The public peer route](#the-public-peer-route).
- **No sandbox around your handler binary.** It runs with the worker's
  privileges and receives message bytes on stdin unvalidated.
- **No authentication between peer workers.** Anything that can write to
  `shared.dir` is trusted as a peer. Every worker of the fleet trusts the
  shared folder: anyone who can write it can plant files that the workers
  archive and serve through the download endpoint. The same holds for the
  [fleet registry](fleet.md): anyone who can write to the card store (the
  card folder, for the `filesystem` provider) can publish a card, and the
  workers list will show it and link to its URL.
  The UI links only `http` and `https` card URLs, but the Live page opens
  a connection to every listed worker and sends `ui.auth_token` with it, so
  a planted card can collect the token. A planted card also makes every
  worker send cache exchange requests to its URL, and the worker applies
  the `cluster` and `global` rows that URL serves under the card's worker
  name (see [Outbound requests to card URLs](#outbound-requests-to-card-urls)).
  Protect the card store like the token itself. Cards hold no secrets — a
  name, a cluster, a URL, a state, a release and the marks of the newest
  cache rows — so reading the store reveals where workers answer and the
  newest `cluster`/`global` cache key of each worker. The election of
  cluster periodic tasks is trusted the same way: whoever can reach its
  store (the folder `tasks/` of the shared folder, for the `filesystem`
  provider) can take or hold a lease, which decides which worker runs a slot
  of a cluster task, the archive pass included. Holding a lease keeps every
  worker from running that task until the lease ends.

The last two are architectural trust boundaries with their own reasoning —
see [the FAQ's trust model](faq.md#security-and-trust-model) and
[`SECURITY.md`](https://github.com/wlame/drakkar/blob/main/SECURITY.md).

## Even inside the private network

### The loopback bind and the operator's browser

The loopback bind stops the network from reaching a worker. It does not stop
the operator's own browser, which is on that machine: WebSockets are not
subject to CORS, so a page on any site could open `ws://127.0.0.1:8080/ws`
and read the live task stream. Every handshake is therefore Origin-checked,
with or without a token.

An untokened worker still accepts the cross-origin handshakes the cluster
view is built on — one worker's page opens a socket to each of its peers —
because a peer is reached over the network, never at the browser's own
loopback address. What it rejects is a page that is not on loopback asking
for a worker that is. Workers co-located on one host, which advertise
loopback addresses on different ports, are unaffected.

The gap this leaves is a page served from another *loopback* port, such as a
local dev server: a worker cannot tell it apart from a co-located sibling.
Set `ui.allowed_ws_origins` to close it — a non-empty list is a strict
allowlist and nothing outside it connects.

### The public peer route

Workers pull each other's cache rows from
`GET /api/v1/peer/cache/changes` and send no token, so `ui.auth_token` does
not protect the routes under `/api/v1/peer/`. **Anyone who can reach the UI
port can read the `cluster` and `global` cache rows of a worker that serves
the cache exchange** — keys, values and timestamps. The route serves only
the rows that worker originated, never `local` rows, and nothing else of the
worker's state.

A worker serves the route while the cache, `storage.dir` and
`fleet.cache_exchange` are on; otherwise it answers `404`. To keep cache
values private:

- keep them in the `local` scope, which is never served;
- set `fleet.cache_exchange.enabled: false` on workers that need no
  exchange;
- let only the workers and the operators reach the UI port.

### Outbound requests to card URLs

Every exchange cycle, a worker sends HTTP `GET` requests to the URL on the
card of each peer it pulls (see [Cache](cache.md#which-peers-are-pulled)).
A card planted in the card folder therefore makes every worker request the
URL it names, which can be any host the workers can reach. The requests
ignore the proxy environment variables and do not follow redirects, and the
answer is read only as a page of cache rows.

The rows such a URL returns are filtered: the worker applies only rows whose
origin is the worker name on the card and whose scope it asked for
(`cluster` only from a card of its own cluster, `global` from any). That
stops `local` rows and rows that claim another worker's origin. It does not
stop the URL from serving `cluster` and `global` rows under the planted
card's own name, and the worker merges them by last-write-wins like the rows
of any peer. Protect the card folder like the token.

### Cross-origin reads

The UI page served by one worker reads the trace, the label trace and the
databases list from every worker of its cluster. Those reads are
cross-origin, so three paths answer CORS: `/api/v1/debug/trace`,
`/api/v1/debug/trace-by-label` and `/api/v1/debug/databases`. They use the
origin rule of `/ws` and the same `ui.allowed_ws_origins` key:

- A non-empty `ui.allowed_ws_origins` is a strict allowlist: only the
  listed origins pass.
- With an empty list, a same-origin request passes.
- **With an empty list and `ui.auth_token` set, every cross-origin read is
  refused.** So with a token, list the origin of every worker of the
  cluster in `ui.allowed_ws_origins`. Otherwise the page of one worker
  cannot read the others, and the UI lists them as unreachable.
- With an empty list and no token, cross-origin reads pass, except a page
  that is not on loopback reaching a worker over loopback.

The preflight (`OPTIONS`) needs no token, because a browser sends none with
it. The read itself still needs the token when one is set. A refused
preflight answers `403`; a refused read gets no CORS header, so the browser
hides the answer from the page. No other path sends CORS headers.

### Provider credentials

The `connections` section holds the address and the credentials of the
shared services that providers use: the Consul ACL `token`, the Redis `url`
(with its password) and the Postgres `dsn` (with user and password). The
`postgres` registry and election providers read `connections.postgres.<name>`:
they open one pool with the `dsn` and use the `schema`. The `redis` providers
read `connections.redis.<name>`: they open one client with the `url` and use
the `key_prefix`. The `consul` providers read `connections.consul.<name>`: they
open one client with the `address` and the `token`, and use the `key_prefix`.
A provider plugin can use any entry through `ctx.connections`. The advice
below applies to the entries you declare and to any provider that uses them.

- **Set them from the environment.** For example a Kubernetes Secret that
  becomes `DK_CONNECTIONS__POSTGRES__MAIN__DSN`, `DK_CONNECTIONS__CONSUL__MAIN__TOKEN`
  or `DK_CONNECTIONS__REDIS__MAIN__URL`. The YAML file is often a ConfigMap
  or sits in version control, and neither is a place for a password. A
  plugin's own secret in `plugin_options` comes from the environment one key
  per variable, for example `DK_FLEET__REGISTRY__PLUGIN_OPTIONS__TOKEN`
  (the plugin gets the key `token` and the value as a string).
- **Give a provider only its own access.** A provider needs the key prefix
  or the schema of its connection and nothing else: a Consul token limited
  to `key_prefix` and to sessions (see below for what its rules allow), a
  Redis user whose key pattern is its `key_prefix` (see below for what the
  pattern limits), a Postgres role
  limited to its `schema` (with `manage_schema: false`, a role that only
  uses tables that already exist). Do not reuse the credentials of a sink,
  and declare a separate named entry for each feature that needs different
  rights.
- **Give the Postgres role the rights it needs and no more.** With
  `manage_schema: true` the worker runs `CREATE ... IF NOT EXISTS` for the
  schema and the tables at the first start of a provider, also when they
  already exist. The role then needs `CREATE` on the database (`CREATE SCHEMA
  IF NOT EXISTS` checks it even when the schema exists) and `CREATE` on the
  schema (for the tables and the indexes); owning the schema is not enough.
  The role must also own the table `fleet_workers`: its two `CREATE INDEX
  IF NOT EXISTS` statements check ownership before they skip an index that
  exists. The simplest case is to let the worker role create the objects;
  `task_locks` needs no ownership. For tables that another role created,
  set `manage_schema: false`.
  For a role without these rights, set `manage_schema: false` and let an
  operator run `drakkar-schema postgres --schema=<name>` once, as a role that
  has the DDL rights; it prints the DDL as plain SQL. The worker role then
  needs `USAGE` on the schema and `SELECT`,
  `INSERT`, `UPDATE` and `DELETE` on `fleet_workers` (registry) and
  `task_locks` (election), and nothing else. A role with these rights can
  publish and delete worker cards and take the lease of any cluster task, so protect the
  schema like the card folder of the `filesystem` provider.
- **The recorder events table of `persistence: postgres` must be owned by
  the worker role.** The worker creates and drops the hourly partitions of
  that table itself, in both `manage_schema` modes, and Postgres lets only
  the owner of a table do that. With `manage_schema: false`, an operator
  creates the table with `drakkar-schema postgres --schema=<name> --recorder`
  (add `--table-prefix=<p>` and `--retention-hours=<n>` as needed) and then
  runs `ALTER TABLE <schema>.<table> OWNER TO <worker role>`. The worker role
  also needs `USAGE` on the schema. The owner can drop that table and its
  history, so give the ownership only to the worker role. See
  [Recorder History in Postgres](recorder-postgres.md#schema-management-and-ownership).
- **The recorder rows hold what the recorder holds.** Every row carries the
  cluster, the worker, the task `args`, the event `metadata` (with the stored
  stdin of `ui.recorder.store_stdin` and the capped stdin of failed tasks) and
  the labels. With `ui.recorder.postgres.store_output: true` it also carries
  `stdout` and `stderr`. Every worker of every cluster on one database writes
  to tables that any role with `SELECT` on them can read, so give read rights
  only to the people who may see the task data of all those clusters. To keep
  stdin out of Postgres, keep `store_stdin` off and lower
  `ui.recorder.stdin_max_bytes`.
- **Give the Consul providers a token of their own.** The `consul` providers
  send `GET /v1/status/leader`, the session requests (`create`, `renew`,
  `destroy` and `info`) and the key requests (a read of one key, a recursive
  read of the card prefix, and a write with `?acquire=`) of the agent's HTTP
  API. The leader request needs no right. The rest needs two rules, in which
  `drakkar/` is the `key_prefix` of the connection and a `/`:

  ```hcl
  key_prefix "drakkar/" { policy = "write" }
  session_prefix "" { policy = "write" }
  ```

  Put them into a policy, make a token of the policy, and set the token from
  the environment (`DK_CONNECTIONS__CONSUL__MAIN__TOKEN`). The `/` after the
  prefix keeps the rule from matching a sibling such as `drakkar-other/`. A
  `key_prefix` with more than one segment, such as `team/drakkar`, gives the
  rule `key_prefix "team/drakkar/"`. A token without `session:write` cannot
  create a session; its first publish fails with a `403`.

  Know what these rights allow beyond what the providers need:

  - `write` on the key prefix lets the token read, write and delete **any**
    key under the prefix, not only the cards and the locks. Whoever holds the
    token can publish and delete worker cards and take the lease of any cluster task. Give
    the fleet a `key_prefix` of its own, so that no other tool keeps keys
    under it.
  - `session_prefix ""` matches every node, so the token can create, renew,
    read and destroy **any session** of the datacenter, also the sessions of
    other applications. Destroying a session that holds a lock breaks that
    application's lock. Each provider destroys its own sessions: the
    registry's withdraw, the election's release, and the new session after a
    refused acquire. In a takeover the registry destroys only a session named
    for the same worker (`drakkar-<worker>-<instance>`); it never destroys a
    session of another name. The token has no such limit.

  The token travels only in the `X-Consul-Token` header, and only when it is
  not empty. The providers never write it into a log line, a metric label, a
  card or an error message. The text that Consul writes in an answer to a
  refused request (the first line, up to 200 characters) does appear in the
  error, and it can name the AccessorID of the token, which is not the secret.

  The client uses the defaults of the HTTP library that the HTTP sink uses.
  With `http://` in the `address`, the token and the cards cross the network
  in clear text: use it only for an agent on the same host or on a network you
  trust. With `https://` the client verifies the certificate of the agent
  against the bundle of certificate authorities that the HTTP library ships
  (the `certifi` bundle), not the store of the operating system, and checks
  that the certificate names the host of the address. This cannot be switched
  off. Use a private certificate authority through `SSL_CERT_FILE` (a file of
  certificates) or `SSL_CERT_DIR` (a directory of them): either replaces the
  bundle. The client sends no client certificate, so an agent that requires one
  on its HTTPS port cannot be used. The proxy environment variables
  (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`) apply to the requests: add the
  agent to `NO_PROXY` when it must not go through a proxy.
  The client follows no redirect.
- **Give the providers a Redis user of their own.** The `redis` providers
  send `EVAL`, `GET` and `PING`, and their scripts call `GET`, `SET`, `DEL`,
  `ZADD`, `ZRANGE`, `ZREM`, `PEXPIRE` and `TIME`. The client also sends its
  connection commands when it connects. The rights
  `~<prefix>* +@read +@write +@scripting +@connection +time` cover all of
  them, where `<prefix>` is the `key_prefix` of the connection. With the
  default prefix:

  ```text
  ACL SETUSER drakkar-fleet on '>change-me' '~drakkar:*' +@read +@write +@scripting +@connection +time
  ACL SETUSER default off
  ```

  The first line makes the user. Its password goes into the `url` of the
  connection, for example
  `redis://drakkar-fleet:change-me@redis.internal:6379/3`, which you set from
  the environment (`DK_CONNECTIONS__REDIS__MAIN__URL`). Any character of the
  password other than a letter, a digit or one of `-._~!$&'()*+,;=:@` goes
  into the URL as a percent escape, for example `^` as `%5E`; the provider
  refuses the URL otherwise (see the URL rules in
  [Fleet registry](fleet.md#redis)). The second line
  switches the `default` user off, so that nobody connects without a
  password; run it only when no other client of the server uses that user.
  Use `rediss://` in the URL for TLS. The client then verifies the
  certificate of the server against the system's default certificate store
  and checks that the certificate names the host of the URL. This cannot be
  switched off: the URL takes no query options, so no option can turn the
  check off or name a file of certificates. Put a private certificate
  authority into that store; `SSL_CERT_FILE` and `SSL_CERT_DIR` replace the
  default locations of the store. ACL users need Redis 6.0 or later. On
  Redis 5.0, set `requirepass` on the server and give the fleet a database
  number of its own; the URL then holds only the password, for example
  `redis://:change-me@redis.internal:6379/3`, and that password allows every
  command.

  Know what these rights allow and what they do not limit:

  - The rights that start with `+@` are category rights. They allow more
    commands than the providers send. For example, `+@write` allows `FLUSHDB`
    and `FLUSHALL`, and `+@read` allows `KEYS`.
  - The key pattern `~drakkar:*` limits only which keys a command may name.
    It does not limit `FLUSHDB`, `FLUSHALL` or `KEYS`. With an empty
    `key_prefix` the pattern is `~*`, and the user may name every key.
  - A Redis ACL does not limit databases. `+@connection` includes `SELECT`, so
    the user can select any database and reach the keys that its pattern
    matches there.

  Give the fleet a **database number of its own** (the path of the URL, for
  example `/3`) all the same. It keeps the keys of the providers apart from
  other data. It does not protect that other data from this user.

  Whoever holds the URL can publish and delete worker cards and take the
  lease of any cluster task. That person can also run every other command that these
  categories allow, so keep the URL secret and use the user for the providers
  only.
- **Operator views mask them.** The config reference shows a mask in place
  of `token`, `url` and `dsn`. For `plugin_options` it masks at every depth
  by key name (`password`, `token`, ...): every value under such a key is
  masked, a string, a number or a boolean alike, and a secret-looking parent
  key such as `credentials` or `auth` masks everything below it. Any other
  string that is a URL loses its embedded credentials.
- **Error text is redacted.** The worker removes credentials from a
  provider's error text before it logs it (`provider_call_failed`,
  `connections_close_failed`, `connection_close_failed`): `user:pass@` in a
  URL and the value of a secret-looking query parameter (`?password=...`)
  become `***`. Only URL-shaped credentials are found this way, so a provider
  plugin must still not put a bare token in the message of an exception.

## Even inside the private network

The UI is an operator tool, and operators see a lot. Read-only pages expose
task `stdout`/`stderr`, task arguments and environment (after redaction),
cache contents, live event streams, and the resolved configuration. If some
of that is sensitive in your deployment — customer payloads in task output,
for instance — then "reachable by any engineer" may already be wider than
you want. Set `ui.auth_token`, or keep the UI on loopback and reach it
through a bastion.

## If you must widen access

Sometimes the UI has to be reachable from more than the host. In that case,
in order of how much they buy you:

1. **Keep it off any untrusted network.** A VPN, a private subnet, or an
   SSH tunnel is worth more than everything below combined.
2. **Put a reverse proxy in front** and let it own TLS, your organisation's
   SSO or mTLS, connection limits, and its own read timeouts. See
   [Front it with a proxy](webapp.md#front-it-with-a-proxy).
3. **Set a strong `ui.auth_token`** — and treat it as a shared secret with
   no revocation story, because that is what it is.
4. **Close what you do not need**: `ui.probe_enabled: false`,
   `ui.merge_enabled: false`, and `ui.enabled: false` entirely on workers
   nobody debugs.
5. **Restrict `storage.dir` and `shared.dir` permissions** to the worker
   user, on every host that mounts them.
6. **Remember the public peer route.** The token does not cover
   `/api/v1/peer/`; see [The public peer route](#the-public-peer-route).

## Reporting something

If you find a way *out* of these boundaries — authentication bypass with a
token set, path traversal in download or bundle extraction, secrets
reaching the recorder despite redaction, remote code execution without a
trusted binary — that is in scope and welcome. See
[`SECURITY.md`](https://github.com/wlame/drakkar/blob/main/SECURITY.md)
for private reporting.

Reports that rest on the design assumptions above — "the UI has no auth by
default", "the handler binary is trusted" — will be closed with a pointer
to this page.
