# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Kafka transport security.** `kafka.security` configures SASL (PLAIN,
  SCRAM-SHA-256/512, GSSAPI, OAUTHBEARER) and TLS including mutual TLS,
  so the framework can now reach managed and secured clusters —
  Confluent Cloud, AWS MSK, Aiven, Redpanda Cloud, and self-managed
  clusters behind SASL or TLS. It applies to every Kafka client: the
  consumer, Kafka sinks, the DLQ producer, and the DLQ replay reader.

  The default is `PLAINTEXT` and emits no client properties at all, so a
  worker that configures nothing connects exactly as before. Incoherent
  combinations (a SASL protocol with no mechanism, SCRAM without
  credentials, a mechanism on a non-SASL protocol, a TLS key without its
  certificate) now fail at startup instead of surfacing as an opaque
  librdkafka connection error at first poll.

  Passwords are `SecretStr` and never appear in `repr()` or
  `model_dump()`. Prefer the environment overrides — for example
  `DK_KAFKA__SECURITY__SASL_PASSWORD` — over YAML literals; `DK_*`
  variables are already withheld from executor subprocesses.

  A Kafka sink or DLQ whose `brokers` field is empty inherits the
  consumer's brokers *and* its security together. Setting `brokers`
  makes that client self-contained; if it then carries no security while
  the consumer is secured, startup logs a `kafka_security_mismatch`
  warning naming it.

  See [Kafka security](docs/configuration.md#kafka-security-kafkasecurity).

- **`kafka.client_config`** — a raw librdkafka escape hatch, merged after
  `security` so it wins, for properties the typed block does not model.
  Four keys are rejected at startup because each backs a delivery
  invariant: `enable.auto.commit`, `partition.assignment.strategy`,
  `group.id`, `bootstrap.servers`. The same field exists on Kafka sinks
  and the DLQ.

- A `kafka_security` startup log line reports the negotiated protocol and
  mechanism (never credentials). The one-line config summary is
  deliberately unchanged, to preserve byte-parity with the Go backend.

- CI now scans dependencies for known vulnerabilities on every run
  (`pip-audit` against the installed environment), backed by a weekly
  Dependabot job that tracks both Python package and GitHub Actions
  updates.
- A scheduled nightly workflow runs the full Docker-based integration
  harness against real Kafka, Postgres, Mongo, and Redis.

- **HTTP sink body encodings.** `sinks.http.<name>.encoding` selects the
  request body format: `json` (the default, unchanged), `form`
  (`application/x-www-form-urlencoded`), or `multipart`
  (`multipart/form-data`, fields only). For the form encodings the payload
  model is flattened to fields sorted by name, with non-string values
  rendered as compact JSON. Both backends emit byte-identical bodies, with
  two documented exceptions: floats render in each language's native form
  (`42.0` in Python, `42` in Go), and a `json`-encoded payload containing
  U+2028 or U+2029 differs because Go's JSON encoder escapes those two
  characters unconditionally (recorded as divergences #25 and #26 in the
  Go backend).

### Changed

- **An HTTP sink that sets a `Content-Type` header now fails at startup.**
  The `encoding` setting owns the Content-Type, so a `Content-Type` header
  is now rejected even when it agrees with the body it would have
  produced. For `encoding: json` — the default, and the only encoding
  that existed before this change — a header of `Content-Type:
  application/json` was previously correct and worked; so was
  `application/json; charset=utf-8`, which is now unrepresentable, since
  the `charset` parameter can no longer be expressed at all. Per RFC
  8259, UTF-8 is JSON's default charset, so receivers should be
  unaffected. Remove the header, or set `encoding` to the format you
  intended.

- **The MongoDB sink now uses PyMongo's async client instead of the
  deprecated `motor` driver.** No configuration change is required, and
  `motor` is no longer a dependency.
- The test suite now installs `httpx2` alongside `httpx` so Starlette's
  `TestClient` stops warning about the older client. This is a test-only
  change — production code (`drakkar/sinks/http.py`,
  `drakkar/uihost/fetch.py`) still uses `httpx`.
- Fixed-duration sleeps in the test suite that only waited for a
  condition were replaced with condition polling, and a flaky
  echo-duration assertion now bounds against measured wall-clock time
  instead of a near-vacuous positivity check. This does not meaningfully
  change suite runtime; it removes a source of intermittent failures.
- The minimum test coverage floor rose from 75% to 95%.

### Fixed

- The Kubernetes reference manifests in `deploy/k8s/` configured a retired
  `debug:` config section and a `DK_DEBUG__AUTH_TOKEN` environment
  variable, either of which prevents a worker from starting. They now use
  `ui:` and `DK_UI__AUTH_TOKEN`, and a test loads every shipped manifest
  through the real config loader.
- Reaping a subprocess after `SIGKILL` is now bounded at 5 seconds, so a
  process wedged in uninterruptible I/O can no longer hang worker
  shutdown.
- The README's trust-model section now describes all three
  `kafka.on_parse_error` policies (`skip`, `dlq`, `raise`) instead of
  only the default, so the documented behavior for an unparseable
  message matches what actually happens.
- The MongoDB sink's per-document fallback (used when a batch insert
  fails) now strips the `_id` PyMongo writes back into a document's
  dictionary before resending it. Previously that leftover `_id` made the
  retry collide with the document Mongo had already inserted, so the
  fallback reported the wrong document as the failure and gave up before
  reaching the one that actually failed. On a partly-failed batch the
  error now identifies the document that really caused it, and every
  document ahead of it is delivered — at the cost of documents the failed
  batch had already written being written again under a new `_id`.
  Documents after the failing one are still not attempted in that call,
  unchanged from the pre-batching behavior.

### Security

- **`executor.env_inherit_deny` additionally withholds `*PASSWD*` and
  `*SALT*` from subprocess environments.** If a handler binary relies on
  a parent environment variable matching either pattern, pass it
  explicitly via `executor.env` or `ExecutorTask.env`.
- The flight recorder redacts a broader set of secret-looking variable
  names before writing them to its debug database — `*AUTH*`,
  `*PRIVATE*`, `*CERT*`, `*SALT*`, `*PASSWD*`, and `*KEY*` anywhere in the
  name, rather than only as a `_KEY` suffix.
- The reference Kubernetes deployment now runs unprivileged (non-root
  UID/GID, all Linux capabilities dropped, a `RuntimeDefault` seccomp
  profile) with a read-only root filesystem.
- Upgraded the dependencies flagged by the new CVE scan.

## [1.0.0] - 2026-07-03

First stable release. Earlier 0.x releases were pre-stable development
snapshots and are not individually documented here.
