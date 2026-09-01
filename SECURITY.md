# Security Policy

## Supported versions

Fixes land on the latest minor release only. There are no maintenance
branches: report against the newest release, and upgrade to it before
reporting if you are behind.

| Version | Supported |
|---------|-----------|
| 2.1.x   | yes       |
| < 2.1   | no        |

## Reporting a vulnerability

Report privately — please do not open a public issue. Use [GitHub's
private vulnerability reporting](https://github.com/wlame/drakkar/security/advisories/new)
on this repository, or contact the maintainer directly.

Include the affected version, what an attacker can achieve, and a
reproduction if you have one. Expect an acknowledgement within a week.

## Intended deployment

Drakkar is an **internal tool**. Its operator UI, its `/api/v1` JSON and
WebSocket surface, and its optional HTTP ingress are built for a trusted,
private network and are not intended to be exposed to an untrusted one.

The framework ships authentication, rate limiting and input checks, and
they are worth enabling — but they are **defence in depth, not a security
perimeter**. The perimeter is the operator's network and the application
built on top of Drakkar. [Security posture](docs/security.md) states this
in full, including what the framework deliberately does not provide.

## Trust model — please read before reporting

Drakkar documents explicit trust boundaries in
[Security posture](docs/security.md) and the
[FAQ](docs/faq.md#security-and-trust-model). These are **design
assumptions, not vulnerabilities**, and reports resting on them will be
closed with a pointer here:

- **The handler binary is fully trusted.** `executor.binary_path` is
  operator-configured and message bytes reach its stdin without
  sanitisation.
- **Peer workers sharing a `db_dir` are fully trusted.** Cache and
  recorder peer sync have no cryptographic authentication of peer
  writes.
- **Kafka transport security is configured, not assumed.** `kafka.security`
  supports SASL (PLAIN, SCRAM, GSSAPI, OAUTHBEARER) and TLS including
  mutual TLS; the default is `PLAINTEXT`, which is appropriate only inside
  a trusted network. Running against an untrusted network without
  configuring it is a deployment choice, not a framework vulnerability —
  see [Kafka security](docs/configuration.md#kafka-security-kafkasecurity).

- **The operator UI is an operator tool, not a public surface.**
  Authentication is opt-in (`ui.auth_token`); a startup warning names
  the unauthenticated posture. Most endpoints are read-only, but two are
  not, and each can be closed independently of `auth_token`: the
  **Message Probe** (`ui.probe_enabled`) executes the live handler
  against pasted input and competes with production traffic for
  executor slots, and **Merge** (`ui.merge_enabled`) writes a
  `merged-<ts>.db` into `db_dir` that nothing reclaims. It is intended
  for private-network deployment.

Findings that break *out* of these boundaries — for example remote code
execution without a trusted binary, authentication bypass when
`ui.auth_token` is set, path traversal in the download or
bundle-extraction paths, or secret leakage into the recorder database
despite redaction — are in scope and welcome.
