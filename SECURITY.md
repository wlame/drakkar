# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | yes       |
| < 1.0   | no        |

## Reporting a vulnerability

Report privately — please do not open a public issue. Use [GitHub's
private vulnerability reporting](https://github.com/wlame/drakkar/security/advisories/new)
on this repository, or contact the maintainer directly.

Include the affected version, what an attacker can achieve, and a
reproduction if you have one. Expect an acknowledgement within a week.

## Trust model — please read before reporting

Drakkar documents explicit trust boundaries in the README under
"Security & trust model". These are **design assumptions, not
vulnerabilities**, and reports resting on them will be closed with a
pointer here:

- **The handler binary is fully trusted.** `executor.binary_path` is
  operator-configured and message bytes reach its stdin without
  sanitisation.
- **Peer workers sharing a `db_dir` are fully trusted.** Cache and
  recorder peer sync have no cryptographic authentication of peer
  writes.
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
