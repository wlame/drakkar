# Kafka Read API

Two debug endpoints (plus a discovery listing) for reading the worker's
Kafka topics directly from the operator UI server — fetch one message by
its exact coordinates, or stream a time window to traverse a topic. Built
for the "what exactly is in that message?" moments: inspecting a DLQ
entry, checking what a sink actually produced, or replaying a time range
by eye while debugging.

Reads are **invisible to the pipeline**: each request uses a short-lived
`assign()`-only consumer that joins no consumer group and commits no
offsets. Nothing about the pipeline's consumer group — or anyone else's —
changes because you looked.

---

## Quick reference

| Endpoint | What it does |
|----------|--------------|
| `GET /api/v1/debug/kafka/topics` | The readable aliases and their kinds |
| `GET /api/v1/debug/kafka/{alias}/message?partition=N&offset=M` | One message with full record metadata |
| `GET /api/v1/debug/kafka/{alias}/messages?from_ts=MS&to_ts=MS&limit=N&partition=N` | NDJSON stream of a time window |

```bash
# What can I read?
curl 'http://worker:8080/api/v1/debug/kafka/topics'
# → {"topics":[{"alias":"source","kind":"source"},{"alias":"dlq","kind":"dlq"},
#              {"alias":"search-results-kafka-sink","kind":"sink"}]}

# One message by coordinates
curl 'http://worker:8080/api/v1/debug/kafka/source/message?partition=2&offset=41337'

# Everything the DLQ received in a one-hour window (NDJSON, one message per line)
curl 'http://worker:8080/api/v1/debug/kafka/dlq/messages?from_ts=1755600000000&to_ts=1755603600000'

# Traverse forward: use the last line's timestamp_ms (+1) as the next from_ts
curl 'http://worker:8080/api/v1/debug/kafka/source/messages?from_ts=1755600000000&limit=1000'
```

With `ui.auth_token` configured, add `-H 'Authorization: Bearer <token>'`
(or `?token=<token>`) like every other API route.

---

## Topic aliases — the addressing model

Clients never name a raw Kafka topic. The `{alias}` in the path is one of:

| Alias | Resolves to |
|-------|-------------|
| `source` | The pipeline input topic (`kafka.source_topic`) |
| `dlq` | The dead-letter topic (`dlq.topic`, or its `{source_topic}_dlq` default) |
| *sink instance name* | That Kafka sink's `topic` — e.g. a sink configured as `sinks.kafka.search-results-kafka-sink` is readable as alias `search-results-kafka-sink` |

Raw topic names and broker addresses appear nowhere in the API — not in
URLs, not in responses (messages echo the *alias*). Only the topics this
worker is configured to touch are reachable; there is deliberately no way
to point the endpoint at an arbitrary topic.

Each alias also carries its own connection settings, with the same
inheritance rule the pipeline uses: a sink or DLQ with empty `brokers`
reads from the consumer's cluster with the consumer's credentials; one
with explicit `brokers` uses its own security block.

A Kafka sink instance named `source` or `dlq` keeps delivering normally,
but is not readable under its name — the reserved aliases win, and
startup logs a `kafka_read_alias_shadowed` warning.

---

## The message shape

```json
{
  "alias": "dlq",
  "partition": 0,
  "offset": 1234,
  "timestamp_ms": 1755601234567,
  "key": "task-8842",
  "key_encoding": "utf-8",
  "payload": "{\"error\": \"parse failure\", ...}",
  "payload_encoding": "utf-8",
  "payload_size_bytes": 512,
  "headers": [{"key": "trace-id", "value": "abc123", "value_encoding": "utf-8"}]
}
```

Kafka payloads, keys, and header values are bytes on the wire, and JSON
cannot carry raw bytes: each such field arrives as UTF-8 text when the
bytes decode, otherwise as base64 — with the encoding flagged alongside,
so binary values round-trip without guessing. `timestamp_ms` is `null`
when the broker reports no usable timestamp. `key` and header values are
`null` when the record has none.

---

## Reading a single message

`GET /api/v1/debug/kafka/{alias}/message?partition=N&offset=M` returns the
one record at those exact coordinates, or:

- **404** — unknown alias or partition; offset outside the partition's
  current `[low, high)` watermarks (the detail names the range); or the
  slot exists but the record was **compacted or deleted** (the detail
  names the next surviving offset, so you can jump there).
- **502** — the brokers did not answer within the server's deadline
  (10 s), or the read itself failed.

The live page's task cards show `(topic, partition, offset)` source
coordinates — those plug directly into this endpoint with alias `source`.

## Streaming a time window

`GET /api/v1/debug/kafka/{alias}/messages?from_ts=...` streams
`application/x-ndjson`: one message JSON object per line, no envelope.

- `from_ts` (required) — epoch **milliseconds**. Each partition starts at
  its first offset with a timestamp at or after this (resolved
  broker-side, no scanning).
- `to_ts` — a partition stops as soon as a message past `to_ts` appears.
- `limit` — total message cap; the server enforces a hard cap of 10 000
  per request either way.
- `partition` — restrict to one partition; default is all partitions.

The stream also ends at the **end-of-topic snapshot** taken when the
request started — messages produced while you stream are not chased, so
a request over a live topic always terminates. Ordering is Kafka's:
monotonic within a partition, best-effort interleaving across partitions.

To traverse a large topic, page by window: take the last line's
`timestamp_ms`, and issue the next request with `from_ts=<that>+1` (or
re-request the same window with a larger `limit` — reads are idempotent).

A broker failure *mid-stream* cannot change the already-committed 200
status; the stream ends with a final `{"error": "..."}` line instead.
Clients should treat any line carrying an `error` key as a terminated
stream.

---

## Security posture

Three layers, all deliberate:

1. **The alias table is the reach boundary.** Only the configured topics
   are readable — the API cannot be pointed at other topics on the
   cluster, even with valid Kafka credentials loaded.
2. **`ui.auth_token`** — the standard optional UI bearer token gates
   these routes like every other `/api/*` route.
3. **`ui.kafka_read_enabled`** (default `true`) — operator kill switch,
   independent of auth, same pattern as `probe_enabled` / `merge_enabled`.
   When `false`, the routes answer `403` naming the config key.

One posture gap gets a loud startup warning: if any readable alias
resolves to a **non-PLAINTEXT** Kafka security config while
`ui.auth_token` is empty (and the API is enabled), the worker logs
`kafka_read_exposed_without_ui_auth` naming the exposed aliases — Kafka
demands credentials, but this API would serve those topics without any.
Serving continues; the admin owns the trade-off, and the fix is one line
(`ui.auth_token`, or `ui.kafka_read_enabled: false`).

Message payloads are served verbatim — the recorder's secret redaction
does not apply here, same caveat as [cache values](cache.md#secrets-in-cache-values).

---

## Operational notes

- Each request builds its own consumer and closes it when the response
  ends (including client disconnects mid-stream) — nothing is pooled,
  nothing leaks. The cost per request is one broker connection + metadata
  round-trip, so this is a debug tool, not a data-plane API.
- The fixed reader `group.id` is `drakkar-ui-read`. It never reaches the
  broker's group coordinator (assign-only consumers don't join groups),
  but clusters that ACL group names can allow it explicitly.
- Reads never block the pipeline: the ad-hoc consumer is a separate
  connection with its own small thread pool, and every broker call
  carries a timeout.

---

## UI integration

drakkar-ui builds two features on this API:

- Every `p:offset` Kafka icon across the app (live cards, history, task
  detail, trace) offers **Probe this message** — it opens the Message
  Probe tab prefilled with the real record fetched by coordinates. With
  an external Kafka-UI configured too, the icon becomes a small action
  menu with both destinations; with neither available, the icon hides.
- A **DLQ** tab on the Debug page lists a time window of the dead-letter
  topic, opens any message in a side panel, and sends it to the probe.

Both appear automatically when the backend serves this API and disappear
when `ui.kafka_read_enabled=false`.

## Related pages

- [Observability](observability.md) — the operator UI this API belongs to
- [Configuration](configuration.md) — `ui.auth_token`, Kafka security blocks
- [Sinks](sinks.md) — Kafka sink instances and their names
- [Cache](cache.md) — the other debug surface that serves message-derived data
