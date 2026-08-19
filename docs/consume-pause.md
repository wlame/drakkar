# Consume Pause (timed debug pause)

A Live-page control that stops message intake for a bounded period —
15 s / 1 m / 5 m / 15 m by default — so you can inspect a live worker
(probe a message, read the DLQ, stare at a timeline) without the pipeline
racing ahead. The pause **auto-resumes at its deadline**, and a Resume
button ends it early at any time.

**Opt-in.** Pausing affects the pipeline's work: while paused the worker
fetches nothing and consumer lag grows. The feature is therefore off by
default and must be enabled deliberately:

```yaml
ui:
  consume_pause:
    enabled: true                        # default: false
    durations_seconds: [15, 60, 300, 900]  # Live-page preset buttons
```

---

## No rebalance — the design guarantee

The pause never leaves the consumer group. It uses partition
`pause()`/`resume()` — the same primitive backpressure uses — while the
poll loop keeps running (returning nothing) and heartbeats continue. The
group coordinator sees a perfectly healthy member; only fetching stops.
No offsets move, nothing is committed, and no rebalance is ever
triggered. On resume, consumption continues exactly where it stopped.

Three pause actors share the consumer, with strict precedence rules:

| Situation | Behavior |
|-----------|----------|
| Backpressure wants to resume during a debug pause | Blocked — queues draining cannot restart fetching while the operator asked for quiet |
| Debug resume while backpressure still holds | Partitions stay paused; the backpressure loop resumes them when queues drain |
| Stall-paused partitions (`dlq.on_send_failure: stall`) | Never touched in either direction — they stay paused until restart/revoke |
| Rebalance assigns new partitions during a pause | They arrive paused — a rebalance cannot leak messages past an active pause |

---

## The Live page control

With the feature enabled, the Live page shows a **Pause consuming** row
with one button per configured preset. While paused, a banner replaces it:

> ⏸ **Consuming paused** — resumes in 0:42 (asked for 1m) · *The consumer
> group is untouched — no rebalance; lag grows while paused.* — **Resume now**

The countdown renders from the server's authoritative `resume_at_ms` and
the page re-polls the state every few seconds, so an auto-resume or a
resume from another tab clears the banner promptly. Clicking a preset
while already paused simply moves the deadline.

## The API (contract v1.14)

| Endpoint | What it does |
|----------|--------------|
| `GET /api/v1/debug/consume-pause` | State: `{enabled, durations_seconds, active, resume_at_ms, requested_seconds}` — always 200 |
| `POST /api/v1/debug/consume-pause` `{"duration_seconds": N}` | Pause for N seconds (1–3600, any value — the presets are UI sugar) |
| `POST /api/v1/debug/consume-resume` | Resume now (idempotent) |

```bash
curl -X POST 'http://worker:8080/api/v1/debug/consume-pause' \
  -H 'Content-Type: application/json' -d '{"duration_seconds": 60}'
curl -X POST 'http://worker:8080/api/v1/debug/consume-resume'
```

The mutating endpoints answer `403` naming the config key when the
feature is disabled, and `503` while the consumer is not running. The
standard optional `ui.auth_token` gates all three, like every API route.

## Observability

- `drakkar_consume_pause_active` gauge (1 while paused) — pair it with
  `drakkar_backpressure_active` on dashboards that watch intake.
- Structured logs: `consume_pause_started` (duration, deadline,
  partitions), `consume_pause_ended`, `consume_pause_deadline_reached`.
- Consumer lag grows for the duration by design; size the pause
  accordingly on busy topics.

## Related pages

- [Observability](observability.md) — the Live page this control lives on
- [Kafka Read API](kafka-read.md) — the natural companion (pause, then
  inspect messages at leisure)
- [Configuration](configuration.md) — `ui.*` reference
