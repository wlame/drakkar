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
| **UI bearer token** | `ui.auth_token` gates every page and API route (`Authorization: Bearer`, or `?token=` for browser navigation), compared with `secrets.compare_digest`. `/healthz` and `/readyz` stay public for Kubernetes. |
| **WebSocket handshake auth** | `/ws` authenticates inside the handshake and validates `Origin` against `ui.allowed_ws_origins` (or the `Host` header). |
| **Startup warning** | A worker running the UI without a token logs `ui_unauthenticated` at startup, naming the bind and both opt-in paths. |
| **Independent kill switches** | `ui.probe_enabled` and `ui.merge_enabled` close the two non-read-only endpoints, whether or not a token is set. |
| **Webapp auth + rate limits** | Per-client bearer tokens and a per-client rpm sliding window on the HTTP ingress, with `max_concurrent` shedding over-capacity requests. |
| **Path containment** | Database download and merge resolve names inside `ui.recorder.db_dir` and refuse anything that escapes it, is not a recorder database name, or arrives when no directory is configured. |
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
- **No sandbox around your handler binary.** It runs with the worker's
  privileges and receives message bytes on stdin unvalidated.
- **No authentication between peer workers.** Anything that can write to a
  shared `db_dir` is trusted as a peer.

The last two are architectural trust boundaries with their own reasoning —
see [the FAQ's trust model](faq.md#security-and-trust-model) and
[`SECURITY.md`](https://github.com/wlame/drakkar/blob/main/SECURITY.md).

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
5. **Restrict `ui.recorder.db_dir` permissions** to the worker user, on
   every host that shares it.

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
