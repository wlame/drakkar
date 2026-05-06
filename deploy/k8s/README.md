# Kubernetes reference manifests

These are reference manifests, **not** a Helm chart. The intent is "copy
into your repo, search-and-replace the placeholders, tune the resource
numbers, then apply." Anything more polished (Helm chart, Kustomize
overlays, KEDA `ScaledObject`) is downstream of this starting point.

## Files

| File              | Kind                       | Purpose                                                                |
|-------------------|----------------------------|------------------------------------------------------------------------|
| `deployment.yaml` | `Deployment`               | Worker pods with `/healthz` + `/readyz` probes and a graceful preStop. |
| `service.yaml`    | `Service` (ClusterIP)      | Internal endpoint for the debug UI and Prometheus scraping.            |
| `configmap.yaml`  | `ConfigMap`                | Worker `drakkar.yaml` mounted at `/etc/drakkar/drakkar.yaml`.          |
| `hpa.yaml`        | `HorizontalPodAutoscaler`  | CPU-based autoscaling starting point.                                  |

## How to apply

```bash
# Validate first (no kubectl call against the cluster).
kubectl apply --dry-run=client -f deploy/k8s/

# Apply for real once placeholders are filled in.
kubectl apply -f deploy/k8s/
```

## Required customisations

Search-and-replace the following placeholders (all wrapped in `<...>`)
before applying:

- `<namespace>` — target namespace (in every manifest).
- `<image>:<tag>` — container image for your built worker (e.g.
  `ghcr.io/wlame/drakkar-worker:0.12.0`) — `deployment.yaml`.
- `<cluster_name>` — logical cluster identifier surfaced in the debug
  UI and metrics — `configmap.yaml`.
- `<kafka_brokers>` — Kafka bootstrap servers, e.g.
  `kafka-bootstrap.kafka:9092` — `configmap.yaml`.
- `<source_topic>` — input Kafka topic — `configmap.yaml`.
- `<binary_path>` — path to your handler executable inside the
  container image (must exist in `<image>`) — `configmap.yaml`.
- `<output_topic>` — Kafka topic the example sink writes to —
  `configmap.yaml`. Tune the `sinks:` block for your real sink set.

You will also want to create the optional Secret referenced by the
Deployment:

```bash
kubectl create secret generic drakkar-secrets \
  --namespace=<namespace> \
  --from-literal=kafka_brokers='<kafka_brokers>' \
  --from-literal=debug_auth_token="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
```

Both secret keys are marked `optional: true` in the Deployment, so the
manifests apply cleanly without the Secret — but the debug UI will be
unauthenticated until `debug_auth_token` is populated.

## Config override patterns

Drakkar reads its YAML config from `DK_CONFIG`, then overlays any
`DK_<SECTION>__<FIELD>` env vars on top. This gives two override paths
with different trade-offs:

| Pattern                                             | When to use                                                    | Cost                                                |
|-----------------------------------------------------|----------------------------------------------------------------|-----------------------------------------------------|
| Edit `configmap.yaml` + `kubectl apply` + rollout   | Bulk changes, new sink instances, structural config edits.     | Requires a rollout to pick up the new file.         |
| Add a `DK_*` env var to `deployment.yaml`           | Single-field tweaks, secrets, per-environment differences.     | The env var wins over the YAML; needs pod restart.  |
| Mix: ConfigMap for shape, env for secrets / sizes   | Recommended default — keep credentials and tunables out of YAML | Two places to look, but clear separation.           |

Examples of common env-var overrides:

```yaml
env:
  - name: DK_KAFKA__BROKERS
    value: "kafka-bootstrap.kafka:9092"
  - name: DK_EXECUTOR__MAX_EXECUTORS
    value: "16"
  - name: DK_DEBUG__AUTH_TOKEN
    valueFrom:
      secretKeyRef:
        name: drakkar-secrets
        key: debug_auth_token
```

## Validation

`kubectl` is the source of truth for whether the manifests are
well-formed. Run a client-side dry run against your local kube context
to catch typos and schema errors without touching the cluster:

```bash
kubectl apply --dry-run=client -f deploy/k8s/
```

Expected output is a list of `<kind>/<name> created (dry run)` lines,
one per manifest. Any error there should be fixed before applying for
real.

## Lag-based scaling

`hpa.yaml` scales on CPU utilization, which correlates with throughput
but does not react to Kafka consumer lag. A worker that is keeping up
at low CPU stays small even when a backlog is growing.

For lag-aware scaling, install [KEDA](https://keda.sh) and replace
`hpa.yaml` with a `ScaledObject` using the `kafka` trigger. Example:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: drakkar-worker
  namespace: <namespace>
spec:
  scaleTargetRef:
    name: drakkar-worker
  minReplicaCount: 3
  maxReplicaCount: 12
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka-bootstrap.kafka:9092
        consumerGroup: drakkar-workers
        topic: input-events
        lagThreshold: "1000"
```

KEDA also supports composite triggers, so you can keep the CPU signal
alongside lag for hybrid scaling.

## Auth & exposure

The debug UI is **read-only by design** — no endpoint stops a worker,
replays Kafka messages, or mutates sinks — but it does expose
subprocess output, cache contents, and live event streams. Out of the
box `debug.auth_token` is empty and a startup warning fires
(`debug_ui_unauthenticated`) so the unauthenticated posture is visible
in logs.

You have two paths to safe deployment:

1. **Keep the Service ClusterIP-only.** No Ingress, no LoadBalancer.
   Reach the UI via `kubectl port-forward svc/drakkar-worker 8080:8080`
   when you need it. This is the default these manifests assume.
2. **Set `DK_DEBUG__AUTH_TOKEN` in a Secret.** The sample
   `deployment.yaml` references `drakkar-secrets/debug_auth_token`
   (optional: true). Populate it with a 32+ character random value
   (`python -c "import secrets; print(secrets.token_urlsafe(32))"`).
   Once set, every protected endpoint and the WebSocket live-event
   stream require `Authorization: Bearer <token>`.

Pick one. Exposing the debug port over a public LoadBalancer or
Ingress without a token is unsafe.

## Notes

- `kubectl` is not available in the dev container these manifests were
  authored in — validation was deferred to whoever applies them. Run
  the dry-run command above against your real cluster before the
  first `apply`.
- The `data` emptyDir volume is per-pod and rebuilt on restart. If you
  want post-mortem access to a worker's recorder/cache SQLite files
  after a pod terminates, swap it for a `PersistentVolumeClaim`
  template and use a `StatefulSet` instead of a `Deployment`.
