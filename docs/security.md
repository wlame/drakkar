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
| **Path containment** | Database download and merge take paths relative to `shared.dir` and serve only three shapes: `history/<cluster>/<name>.db`, `history/<cluster>/<archive>.db.gz` and `merged/<name>.db` (merge takes only the `.db` ones). The download answers `404` for any other path, an unsafe segment, a symlink, a path that resolves outside `shared.dir`, or a missing file; the merge answers `400` for a path of another shape. A worker's own files in `storage.dir` are never served by path. The one exception takes no path: `GET /api/v1/debug/download-live` serves a snapshot of the worker's own live recorder file, behind the token. |
| **Filtered peer rows** | The cache exchange applies only the rows a peer originated itself, in the scopes asked of that peer. Peer requests ignore the proxy environment variables and do not follow redirects. |
| **Owner-only files** | Recorder databases, meta files, archives and merge results are created with mode `0600`; `storage.dir`, each cluster history folder and `merged/` are created with mode `0700`. |
| **Secret masking** | The config reference masks secret-flagged fields, `executor.env` and every `*.client_config` by key name; the flight recorder applies the same redaction before storing task env. |
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
  [fleet registry](fleet.md): anyone who can write the card folder can
  publish a card, and the workers list will show it and link to its URL.
  The UI links only `http` and `https` card URLs, but the Live page opens
  a connection to every listed worker and sends `ui.auth_token` with it, so
  a planted card can collect the token. A planted card also makes every
  worker send cache exchange requests to its URL, and the worker applies
  the `cluster` and `global` rows that URL serves under the card's worker
  name (see [Outbound requests to card URLs](#outbound-requests-to-card-urls)).
  Protect the card folder like the token itself. Cards hold no secrets — a
  name, a cluster, a URL, a state, a release and the marks of the newest
  cache rows — so reading the folder reveals where workers answer and the
  newest `cluster`/`global` cache key of each worker.

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
