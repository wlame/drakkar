# Deployment

This page covers operational topics that apply when running Drakkar in a
production environment: Kubernetes probes, rolling restarts, and the
interaction between the debug server and cluster health checks.

## Kubernetes probes

Drakkar exposes two dedicated HTTP endpoints for Kubernetes probes on the
debug-server port (``debug.port``, default ``8787``):

| Endpoint   | Purpose    | Success  | Failure                         |
|------------|------------|----------|---------------------------------|
| ``/healthz`` | Liveness   | ``200``  | Restart the pod                 |
| ``/readyz``  | Readiness  | ``200``  | Remove the pod from endpoints   |

Both endpoints are **unauthenticated** — they are the only routes on the
debug server that ignore ``debug.auth_token``. This is intentional: the
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
  connected, first poll cycle completed) **and** every registered sink
  is currently connected. Otherwise returns ``{"status": "not_ready",
  "reasons": [...]}`` with a 503 status code and a list of machine-
  readable reasons (e.g. ``"not_started"``,
  ``"sink_kafka:results_not_connected"``). The kubelet removes the pod
  from the service endpoints on failure but does NOT restart it — the
  worker is considered recoverable and will self-register once ready.

### Example probe configuration

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8787
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: 8787
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
