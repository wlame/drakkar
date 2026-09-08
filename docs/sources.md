# Input sources

A Drakkar worker reads work from Kafka, from HTTP, or from both. Each input
is a **source**, configured under `sources:` in the worker config. Both
sources are off by default and at least one must be on, so every worker
states plainly where its work comes from. Everything downstream of a source
— the handler, the executor pool, the sinks, the DLQ, the operator UI — is
the same in all three modes.

Field-by-field settings live in
[Configuration → Input sources](configuration.md#input-sources-sources).
This page explains how the two sources differ and what changes when one of
them is off.

## The two sources side by side

| | Kafka source | HTTP source |
|---|---|---|
| Config block | `sources.kafka` | `sources.http` |
| What starts | A consumer group member, the poll loop, and one processor per assigned partition | One POST route on a server thread, bound to `host:port` |
| Readiness signal | The first poll batch returned | The socket is listening |
| What `signal_stop` does | Stops the poll loop and signals every partition processor | Closes the gate: new requests get 503 `status=shutdown` |
| Drain | Waits for queued and in-flight work on live partitions | Waits for in-flight requests |
| Stop | Commits final offsets when the drain was clean, stops processors, closes the consumer | Stops the server thread and joins it |
| Handler hooks required | `arrange()` | `arrange_http_request()` and `on_http_request_complete()`, plus the 3rd/4th generic types |
| Partition and offset | The real Kafka partition and offset | `partition = -1` and a per-worker monotone offset, so HTTP work stays out of the partition views |
| Delivery guarantee | At-least-once: offsets commit on a per-partition watermark | One HTTP round trip. Nothing is redelivered; the client owns the retry |

Sinks behave the same for both. A Kafka sink works on an HTTP-only worker,
because the cluster connection in [`kafka:`](configuration.md#kafka-connection-kafka)
is separate from the Kafka source.

## Choosing a mode

### Kafka only

The classic pipeline. Messages arrive on a topic, `arrange()` turns each one
into tasks, and offsets commit as the work completes.

```yaml
kafka:
  brokers: kafka:9092

sources:
  kafka:
    enabled: true
    topic: search-requests
    consumer_group: search-workers
```

The handler needs `arrange()`. The HTTP hooks are never called, so a
Kafka-only handler may leave them unimplemented.

### HTTP only

A worker with no consumer group. Callers POST a request and wait for the
result in the response. The same handler code builds the tasks, and results
can still go to sinks.

```yaml
cluster_name: "http cluster"

kafka:
  brokers: "kafka:9092"          # still needed: the sink below is Kafka

sources:
  kafka:
    enabled: false
  http:
    enabled: true
    host: "0.0.0.0"
    port: 8092
    path: "/process"
    sinks_enabled: true
    request_timeout_seconds: 60.0
    max_concurrent: 16
    clients:
      - name: mirror
        token: "a-long-random-token"
        rpm: 600

sinks:
  kafka:
    mirrored_results:
      topic: "search-results-http"

dlq:
  topic: ""                      # Kafka source off + empty topic = DLQ off
```

The handler needs the HTTP hooks. It may leave `arrange()` unimplemented.
Read [What changes in observability](#what-changes-in-observability) before
you deploy one: several Kafka-shaped fields report empty values.

### Both

One worker consumes a topic and serves HTTP at the same time. The two
sources share the executor pool, the sinks and the cache, so an HTTP request
competes with Kafka work for executor slots. Size `executor.max_executors`
for the sum, and use
[`sources.http.max_concurrent`](configuration.md#http-source-sourceshttp) to
cap what the HTTP side can claim.

```yaml
sources:
  kafka:
    enabled: true
    topic: search-requests
    consumer_group: search-workers
  http:
    enabled: true
    port: 8090
```

The handler needs `arrange()` **and** the HTTP hooks. This is the mode the
[Webapp](webapp.md) page describes in detail.

## Readiness and shutdown

**Readiness is composed.** The worker reports ready on `/readyz` when every
enabled source is ready and the sinks are connected. One source that is not
ready holds the whole worker back. On a Kafka-only worker that means the
first poll batch; on an HTTP-only worker it means the socket is listening;
on a mixed worker it means both.

**A bind failure is fatal.** If the HTTP source cannot bind its socket, the
worker stops. The HTTP source is an input, and a worker that cannot accept
requests has nothing to read — so a port collision fails the start instead
of leaving a half-dead worker running.

**Shutdown runs against one deadline.** On `SIGINT` or `SIGTERM` the worker
marks itself not ready, calls `signal_stop` on every source, and then drains
all sources concurrently against a single
`executor.drain_timeout_seconds` budget. Whatever remains of that budget is
what `stop` gets. One shared deadline means the total shutdown time does not
grow with the number of sources.

The lifecycle logs name each step: `sources_starting`, `source_started`,
`source_start_failed`, `sources_draining`, `sources_drained` and
`source_stop_failed`. A disabled source block that would fail validation
logs one `source_config_ignored` warning and does not block startup.

## The DLQ without the Kafka source

The DLQ topic defaults to the source topic plus `_dlq`. With no Kafka
source there is nothing to derive it from, so the DLQ needs an explicit
`dlq.topic`:

| `sources.kafka.enabled` | `dlq.topic` | Result |
|---|---|---|
| on | empty | DLQ built, topic `{sources.kafka.topic}_dlq` |
| on | set | DLQ built with that topic |
| off | set | DLQ built with that topic; brokers from `dlq.brokers`, else `kafka.brokers` |
| off | empty | **No DLQ producer.** The summary line reports `dlq=off`. |

In the last row a DLQ send is dropped. The worker logs
`dlq_send_dropped_unconfigured` once per run (then at debug level) and
counts every drop in `drakkar_dlq_unconfigured_drops_total`. Alert on that
counter if your HTTP-only workers must not lose failed payloads — set
`dlq.topic` and they will not.

## What changes in observability

The config-summary line names the enabled sources:

```
[worker-1/analytics-prod] sources=[kafka:search-requests/drakkar-integration/50poll http:8091] exec=4w/10win retries=3/120s ui=on:8081 cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO
```

The `sources=[...]` token lists the enabled sources in a fixed order, Kafka
first. The Kafka token is `kafka:<topic>/<group>/<max_poll_records>poll`;
the HTTP token is `http:<port>`. An HTTP-only worker shows
`sources=[http:8092]` and nothing else.

With the Kafka source off, the Kafka-shaped surfaces report empty rather
than a default that would be wrong:

| Surface | With the Kafka source off |
|---|---|
| `drakkar_worker_info{consumer_group}` | Empty label. The worker joins no group, so reporting the configured default would put it in every group-scoped dashboard query it has nothing to do with. |
| Bound log field `consumer_group` | Omitted from every log line. |
| Recorder `worker_config` row | `source_topic` and `consumer_group` are written empty. The schema does not change. |
| Recorder `worker_state` row | No assigned partitions, count 0, not paused, nothing queued. |
| Message probe | The request must carry a `topic`. Without one the probe answers 400 `topic is required: the Kafka source is disabled on this worker`. |
| [Kafka read API](kafka-read.md) aliases | `source` answers 404 with `alias 'source' is not available: the Kafka source is disabled`. `dlq` resolves only when `dlq.topic` is set, and otherwise answers `alias 'dlq' is not available: the DLQ is disabled (set dlq.topic)`. Sink-instance aliases still work. |
| `/readyz` | Reports `not_started` until the HTTP source is bound. No partition reasons appear. |

## Custom sources

The source abstraction is internal in this release: the two built-in
sources are the only ones a worker can run, and there is no registration
point for your own.

A public API needs an ingestion contract — how a custom source hands work to
the pipeline and learns the outcome — that the two built-ins do not share in
a general form. Kafka delivers through partition processors with offsets;
HTTP delivers through a request runner with a synchronous reply. That
contract is a later round of work.
