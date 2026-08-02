# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.2] - 2026-08-02

### Added

- **Handlers can expose their own diagnostics in the UI.** `self.annotate(target,
  kind, data)` attaches a structured payload to a source message, an executor
  task, or a whole `arrange()` window from inside any hook, and it shows up on
  that entity's trace in the debug UI. It answers the question the framework's
  own events cannot — not *what* happened, but *why the handler decided it*:
  the candidates a hook considered, the flag that shaped a task's arguments,
  the alternative it rejected.

  The scope comes from the target, so no coordinates are ever passed: a
  `SourceMessage` anchors to that message, an `ExecutorTask` to that task, and
  `None` to the window. The framework resolves partition, window, and offsets
  from an ambient hook context it binds around every hook call.

  Annotations are ordinary rows in the flight recorder's `events` table under a
  new `annotation` event type — no schema column was added, so the pinned
  cross-backend event-row shape is unchanged. Recorder rotation and retention
  expire them like every other event, which is what makes them suitable for
  data that is worth keeping for a day and not worth keeping forever.

  Emission is best-effort and can never affect processing: `annotate()` does
  not raise, does not block, and only appends to the recorder's existing
  buffer. A payload that exceeds a budget is **dropped whole rather than
  truncated** — a half-written structured document still parses and still looks
  complete, so it misleads whoever reads it more effectively than a missing
  record does. Two budgets apply per hook invocation:
  `ui.recorder.annotation_max_bytes` (16 KiB) bounds one payload, and
  `ui.recorder.annotation_max_bytes_per_call` (256 KiB) bounds the total a
  single hook call can add, so one handler annotating a wide window cannot
  exhaust `retention_max_events` and evict every other event. Every drop
  increments `drakkar_recorder_annotations_dropped_total{reason}` and is logged
  with the payload attached; the log falls silent after five drops in one
  invocation while the counter keeps going, so alerting belongs on the metric.
  Set `ui.recorder.annotations_enabled: false` to turn the feature off and
  leave `self.annotate(...)` a no-op.

  See `docs/annotations.md`.

- The `arranged` recorder event's metadata now carries `window_id`, so an
  `arrange()` window can be correlated with the window-scoped annotations
  emitted from the same call.

## [1.4.1] - 2026-08-02

### Changed

- **Maintenance release: dependency and CI updates, no functional change.**
  The locked versions the test suite and CI resolve move to `redis` 8.0.1,
  `pymongo` 4.17.0, `fastapi` 0.141.1, `pytest-asyncio` 1.4.0, and `ty`
  0.0.65. The declared floors in `[project.dependencies]` are unchanged, so
  what an installed py-drakkar resolves is unaffected.

  Two of those needed a change to absorb. FastAPI 0.141 includes routers
  behind a lazy wrapper that resolves only when a request arrives, which the
  OpenAPI route-parity test had to learn — the served surface is identical,
  but the test walked `app.routes` and could no longer see routes reached
  through an include. ty 0.0.65 additionally flags a narrowing gap in the
  Redis sink's sorted-mapping helper, where an `isinstance(value, dict)`
  check leaves the keys typed `object`; the invariant is now stated with an
  explicit cast. Neither alters runtime behaviour.

- The GitHub Actions used by the CI, docs and release workflows moved to
  `checkout@v7`, `deploy-pages@v5`, `upload-pages-artifact@v5` and
  `upload-artifact@v7`.

## [1.4.0] - 2026-08-02

### Added

- **The Mongo sink writes more than inserts.** `MongoPayload.op` selects
  `insert` (the default, so existing handlers are unaffected), `update_one`,
  `update_many`, `upsert`, `delete_one`, `delete_many`, or `statement`. The
  update ops take a `filter` serialized to an equality predicate and assign
  `data` through `$set`; `upsert` is insert-or-set on the same filter. One
  and many stay explicit rather than hiding behind a flag, because the blast
  radius differs by orders of magnitude.

  `filter` is required and may never be empty, guarded twice: the payload
  validator rejects a missing one at construction, and the build step rejects
  one that *dumps* empty. An empty Mongo filter matches every document, so
  `delete_many({})` would empty a collection outright.

- **Operator-authored MQL, invoked by name.** Statements declared under
  `sinks.mongo.<instance>.statements` are compiled at startup and run by a
  payload with `op='statement'` and bound `params`. This is the escape hatch
  for anything the declarative fields cannot express — `$inc`, `$push`,
  computed pipeline updates. Unlike the Postgres and Redis equivalents a
  statement is a structured model rather than a string, because MQL is data:
  it carries its own collection, op, filter, and update.

  Values bind through `":name"` placeholders — whole values only, never a
  fragment of a longer string, never a key, and with their type preserved so
  a numeric field still matches. `"::name"` escapes a literal leading colon.
  `$where` and `$function` are rejected at config load at any depth,
  including inside aggregation-pipeline stages, because both execute
  server-side JavaScript.

- The message probe now reports which operation a Mongo payload plans
  (`extras.op`, plus `extras.filter` for the ops that carry one); a statement
  reports its name as the record's `destination`.

- **The Redis sink issues more than SET.** `RedisPayload.op` selects one
  write command per data type — `set` (the default, so existing handlers
  are unaffected), `delete`, `expire`, `incrby`, `hset`, `hdel`, `push`,
  `trim`, `sadd`, `srem`, `zadd` — with the fields each one needs. A field
  the chosen op does not use is a validation error rather than a silently
  ignored value, and a required collection may not be empty.

- **Operator-authored Lua, invoked by name.** Scripts declared under
  `sinks.redis.<instance>.scripts` are registered at startup and run by a
  payload with `op='script'`, its `keys` and `args` passed as `KEYS` and
  `ARGV`. This is the escape hatch for multi-step or conditional logic, and
  the only way to get server-side atomicity — a pipeline is not a
  transaction. Values are never interpolated into the body, so message
  content cannot alter what runs, and DLQ entries and logs carry the script
  name rather than Lua that could leak row data. Every entry of `keys` is
  key-prefixed, so a script cannot reach outside its sink's namespace.

- `RedisSink.client` exposes the `redis.asyncio` client after connect,
  mirroring `PostgresSink.pool`. Reads stay out of the sink itself, so a
  read-modify-write cycle goes through this.

- The message probe now reports which command a Redis payload plans
  (`extras.op`); a script reports its name as the record's `destination`.

### Changed

- **Mongo deliveries are one ordered bulk write per collection run, and
  nothing is re-sent.** `bulk_write(ordered=True)` replaces `insert_many`:
  execution order equals payload order, execution stops at the first failure,
  and `writeErrors[*].index` names the offending payload exactly. A run can
  now carry heterogeneous operations, which `insert_many` could not express.

- **Mongo payloads batch only with adjacent same-collection neighbours.**
  Payloads were previously bucketed globally, which could execute a payload
  before its predecessor — harmless for inserts, a silently lost write once
  updates and deletes exist.

- `MongoSink` decides retry-safety per batch: updates, upserts and deletes
  converge, so those batches get the transient fast-retry, while any `insert`
  or `statement` payload vetoes it.

- PyMongo's `ConnectionFailure` and `NetworkTimeout` are remapped to the
  builtin `ConnectionError`/`TimeoutError` the sink manager matches, with the
  original chained. They inherit only from `PyMongoError`, so a dropped Mongo
  connection had never been eligible for the fast-retry — the same latent
  defect the Redis sink had. Nothing depended on it while the sink was
  unconditionally non-idempotent; per-batch retry makes it live.

- **Redis pipeline failures are attributed positionally and nothing is
  re-sent.** The pipeline now runs with `raise_on_error=False`, so a
  per-command error names its own payload while the commands that
  succeeded are left alone. The previous behaviour re-sent the batch, which
  is what made a non-idempotent command unsafe to batch at all.

- `RedisSink` decides retry-safety per batch: a batch containing `incrby`,
  `push`, or `script` is not fast-retried, because those accumulate or are
  opaque. Everything else converges and stays retry-safe.

- Redis mapping arguments are emitted in sorted key order (`hset` fields,
  `zadd` members) so both backends issue identical commands;
  caller-supplied lists (`hdel` fields, `sadd`/`srem` members) keep their
  order. New shared corpus: `tests/fixtures/redis_commands.json`.

### Removed

- **The Mongo `_id`-stripping fallback**, and the duplicate writes it
  knowingly accepted. It existed only because the per-document replay re-sent
  documents the failed batch had already written, and PyMongo writes a
  generated `_id` back into every document it is handed — so a resent
  document raised a duplicate-key error on the FIRST document rather than the
  guilty one. Positional attribution removes the replay, so the workaround
  and its cost are both gone. This supersedes the fix shipped in 1.3.0 rather
  than reverting it.

### Fixed

- **Dead-lettered payloads lost their body.** Every sink payload declares
  `data` as `BaseModel`, and pydantic serializes against the declared type
  rather than the instance — so `model_dump_json()` emitted `"data": {}`.
  `DLQSink` serializes payloads exactly that way, which meant every
  dead-lettered record reached the DLQ topic without the data it exists to
  preserve, and `scripts/replay_dlq.py` would have replayed blank rows. All
  six payload types were affected, plus `PostgresPayload`'s `where` and
  `params`. Nothing warned: a user's model genuinely is a `BaseModel`, so
  pydantic considered it correctly serialized.

  The bodies are now annotated `SerializeAsAny`, which restores duck-typed
  serialization, and each payload type has a round-trip test plus one through
  the real `DLQMessage.serialize()` path. The Go backend was never affected —
  it marshals the concrete value — so this also closes an undocumented
  divergence on a surface the parity contract calls byte-stable.

### Added

- **The Postgres sink writes more than inserts.** `PostgresPayload.op`
  selects `insert` (the default, so existing handlers are unaffected),
  `update`, `upsert`, or `statement`. An `update` takes a `where` model
  serialized to an ANDed equality predicate, where a `None` value renders
  `IS NULL` rather than `= NULL`. An `upsert` takes `conflict` columns and
  an optional `update_columns` subset, and renders `DO NOTHING` when every
  inserted column belongs to the conflict target.

- **Operator-authored SQL, invoked by name.** Statements declared under
  `sinks.postgres.<instance>.statements` are compiled once at startup from
  `:name` placeholders to positional parameters and invoked by a payload
  with `op='statement'` and bound `params`. This is the escape hatch for
  SQL the declarative fields cannot express — value-dependent expressions
  and guarded predicates. Parameters are always bound, so message content
  can never reach the statement text, and DLQ entries and logs carry the
  statement name rather than SQL that could leak row data. New docs page:
  `docs/sink-write-operations.md`.

- The message probe now reports which operation a Postgres payload plans
  (`extras.op`, plus `extras.where` and `extras.conflict` for the
  operations that carry them); a named statement reports its name as the
  record's `destination`.

### Changed

- **Postgres payloads now batch only with adjacent same-shaped
  neighbours**, so the order statements reach the database always matches
  the order the handler returned them. Payloads were previously bucketed
  globally, which could execute a payload before its predecessor — harmless
  for inserts, a silently lost write once updates exist.

- `PostgresSink` decides retry-safety per batch: a batch of only `update`
  and `upsert` payloads gets the transient fast-retry, while any `insert`
  or `statement` payload vetoes it. Operator SQL is opaque to the
  framework, so it is never assumed idempotent.

- A `data` model that serializes to an empty mapping is now rejected when
  the statement is built, instead of reaching the database as a syntax
  error.

- **Postgres columns are now emitted in sorted order** rather than in the
  order the payload model declares its fields, and bound values follow the
  same sort. This closes a long-standing difference with the Go backend,
  which decodes payload data into a map with no field order to preserve and
  has always sorted. The emitted SQL is semantically unchanged — columns and
  values stay aligned — but the statement text differs, which matters if you
  assert on it or read it in query logs. `conflict` and an explicit
  `update_columns` keep the order you wrote them in.

## [1.3.1] - 2026-08-02

### Fixed

- **The Redis sink never retried a dropped connection, despite declaring
  itself safe to.** `RedisSink` sets `idempotent = True` so the framework
  retries it on transient errors, but the check matched Python's builtin
  `ConnectionError` and `TimeoutError` while `redis-py` raises its own
  classes, which inherit from `RedisError` instead. No Redis connection
  failure ever qualified. The sink now translates those two errors to
  their builtin equivalents, so a connection reset or timeout gets the
  bounded fast-retry before reaching `on_delivery_error`, and a Redis
  worker rides out a blip that previously surfaced as a delivery failure.
  The Go backend classifies errors structurally and has always retried,
  so this also removes a behavioural difference between the two backends.

  Command errors such as `WRONGTYPE` are deliberately left untranslated —
  retrying one would fail identically every time.

- **A failed Redis pipeline was silently retried key by key.** Any
  exception from the batched write was discarded and the whole batch
  re-sent as individual `SET`s. That masked real errors — including a
  defect that meant the batched path was never exercised by the test
  suite at all — and would double-apply any future command that
  accumulates rather than replaces. Pipeline failures now propagate, and
  transient ones are handled by the retry above.

### Changed

- The Redis sink's `idempotent` comment claimed that setting `EX` as part
  of `SET` prevented a retry from refreshing an already-written key. The
  opposite is true: a retried `SET … EX 3600` restarts the expiry window.
  The comment now records the real behaviour and why relative TTLs are
  still preferred over absolute deadlines.

## [1.3.0] - 2026-08-01

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
