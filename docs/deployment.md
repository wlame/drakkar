# Deployment

This page covers operational topics that apply when running Drakkar in a
production environment: Kubernetes probes, rolling restarts, and the
interaction between the UI server and cluster health checks.

## Kubernetes probes

Drakkar exposes two dedicated HTTP endpoints for Kubernetes probes on the
UI-server port (``ui.port``, default ``8080``):

| Endpoint   | Purpose    | Success  | Failure                         |
|------------|------------|----------|---------------------------------|
| ``/healthz`` | Liveness   | ``200``  | Restart the pod                 |
| ``/readyz``  | Readiness  | ``200``  | Remove the pod from endpoints   |

Both endpoints are **unauthenticated** — they are the only routes on the
UI server that ignore ``ui.auth_token``. This is intentional: the
kubelet has no facility to supply bearer tokens on probe requests, and
both endpoints expose only liveness / readiness signals with no message
content, partition state, or operator credentials. They must be mounted
for Kubernetes integration to work.

### Probe semantics

- **``/healthz``** — returns ``{"status": "ok"}`` as long as the process
  is running and the FastAPI event loop is responsive. A ``/healthz``
  failure means the process is hung or crashed; the kubelet will restart
  the pod.

- **``/readyz``** — returns ``{"status": "ready"}`` only when the worker
  has completed its startup sequence (consumer subscribed, sinks
  connected, first poll cycle completed), every registered sink is
  currently connected, **and** no partition's processing loop has died.
  Otherwise returns ``{"status": "not_ready", "reasons": [...]}`` with a
  503 status code and a list of machine-readable reasons (e.g.
  ``"not_started"``, ``"sink_kafka:results_not_connected"``,
  ``"partition_3_processor_died"``). The kubelet removes the pod
  from the service endpoints on failure but does NOT restart it — the
  worker is considered recoverable and will self-register once ready.

### Dead partition loops

A partition's processing loop can exit on an unexpected error — a handler
bug, a dependency that fails in a way the framework does not model. Left
alone this is invisible: Kafka keeps the partition assigned, the consumer
keeps enqueuing, and the queue grows with nothing draining it while
offsets stop committing.

The framework restarts a dead loop **once**. A second death is treated as
a deterministic fault: the partition is marked dead, a CRITICAL
``partition_processor_died`` log records the cause and the impact, and
``/readyz`` starts failing with ``partition_<id>_processor_died``. The
pod leaves the service endpoints and, once replaced, the partition is
reassigned to a healthy worker.

``drakkar_partition_processor_deaths_total{partition,outcome}`` counts
both paths — ``outcome="restarted"`` and ``outcome="dead"``. Alert on any
non-zero rate: a restart is a warning, a death means that partition is
stalled until the worker is replaced.

Note what a restart does **not** do. Offsets are registered before the
handler's ``arrange`` runs, so a crash there leaves that window's offsets
uncommitted for the life of the process, and the commit watermark stops
behind them. The restarted loop keeps processing, but its lag climbs until
a rebalance or restart hands those offsets to an owner that redelivers
them. That is the correct at-least-once outcome — those messages were
never processed, so committing past them would lose them.

### Example probe configuration

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  periodSeconds: 5
  failureThreshold: 3
  initialDelaySeconds: 10
```

The ``initialDelaySeconds: 10`` on the readiness probe accommodates the
worker's cold-start sequence: loading config, connecting to Kafka, and
bringing up sinks. Tune upward if the cluster-align wait
(``kafka.startup_align_enabled``) or a large sink fleet extends the
cold-start budget.

### Rolling restarts

During a rolling restart the readiness probe flips ``/readyz`` to 503
as soon as ``_shutdown`` begins — well before sinks are torn down.
Kubernetes removes the pod from the service endpoints immediately, so
in-flight traffic drains to healthy replicas while the stopping pod
finishes committing offsets, draining executors, and closing sinks.
Liveness continues to return 200 until the process actually exits, so
the kubelet does not interpret the graceful-shutdown window as a crash.
