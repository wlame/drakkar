# Agent Surface

The agent surface is a read-only set of routes that explains a running worker
to an AI agent. An agent can read what the framework is, what each config key,
metric and recorder event means, what this worker is doing now, and which
problems a set of rules finds. It can do nothing else: no route pauses,
deletes or changes anything.

The surface is off by default. It works with the UI off, because the API
server serves it (like `/healthz`).

The same knowledge is in one registry of YAML files in the package
(`drakkar/agentdocs/`). The routes, the offline command `drakkar-agent` and the
[Agent Reference](agent-reference.md) page all read it. Tests check that every
metric, recorder event and API route of the framework has an entry, so the
registry cannot fall behind the code.

## Turn it on

```yaml
agent:
  enabled: true
  auth_token: ''          # set it; see Security below
  include_payloads: false
```

| Key | Default | Meaning |
|---|---|---|
| `agent.enabled` | `false` | Serve the routes under `/api/v1/agent`. Off, every route answers 404 `{"error": "the agent surface is off; set agent.enabled to true"}`. Needs `api.enabled`: with `api.enabled: false` the load fails with `agent.enabled needs api.enabled: the agent routes are served by the API server`. |
| `agent.auth_token` | `''` | When set, every agent route needs `Authorization: Bearer <token>`. A missing or wrong token answers 401 `{"error": "agent token required"}`. The scheme name is matched in any letter case. The token is compared in constant time. `ui.auth_token` and `?token=` do not apply to these routes. |
| `agent.include_payloads` | `false` | Let `/api/v1/agent/state` add the last 2048 bytes of stderr (`stderr_tail`) and the task arguments (`args`) to each recent failure. Nothing else ever carries task output. |

Environment variables: `DK_AGENT__ENABLED`, `DK_AGENT__AUTH_TOKEN`,
`DK_AGENT__INCLUDE_PAYLOADS`. The config summary line shows `agent=on` or
`agent=off`. See [Configuration](configuration.md#agent-agent).

## The routes

Every route is a `GET` under `/api/v1/agent`. Every answer has
`Cache-Control: no-store`. JSON answers are compact UTF-8 JSON.

| Route | What it answers | Parameters |
|---|---|---|
| `/api/v1/agent` | The index: the framework version and `registry_version`, the worker name, cluster and instance, the capabilities, the routes with their parts and the size of each part (`approx_tokens`), and `start_here`. | none |
| `/api/v1/agent/manual` | The registry: concepts, config advice, metrics, events, recipes, routes, rules, and the handler notes (`app`). | `parts`, `detail`, `format=json\|markdown`, `topic=<kind>:<key>` |
| `/api/v1/agent/config` | The effective config, one entry per key: `path`, `value`, `default`, `origin` (`default`, `yaml`, `env` or `on_startup`), `type`, `secret`, `meaning`, and `advice` with `detail=full`. | `parts` (the root sections), `detail` |
| `/api/v1/agent/metrics` | The framework and handler metrics with their meaning, the healthy values and at most 100 samples per family; `promql` and `related` with `detail=full`. | `names`, `prefix`, `detail` |
| `/api/v1/agent/state` | One snapshot: `health`, `budget`, `partitions`, `sinks`, `recorder`, `autodiscover`, `recent_failures` (the last 20 failed tasks). | `parts` |
| `/api/v1/agent/diagnose` | The findings of the rules, by severity, with their evidence, an `explain` reference and a suggestion; `checked` names every rule that ran and `failed` every rule that could not run. | none |

The common parameters:

- `parts` is a comma list. Without it the route answers every part. An
  unknown or empty item answers 400 `unknown part '<p>'; known: <list>`.
- `detail` is `brief` (the default: `key` and `summary`, and the live
  values) or `full` (every field). Another value answers 400
  `detail must be brief or full`.
- `/api/v1/agent/state` and `/api/v1/agent/diagnose` accept `detail` and
  ignore it; a bad value still answers 400.
- `topic` names one entry by its reference: `concept:pending-budget`,
  `config:executor.max_pending_tasks`, `metric:drakkar_pending_tasks`,
  `event:task_failed`, `recipe:lag-growing`,
  `route:GET /api/v1/debug/databases`, `rule:budget-saturated`,
  `app:concept:<key>`. The answer holds that entry with every field and the
  entries it refers to, with their summary only. A topic that names nothing
  answers 404 `no such topic '<ref>'`. A config path with no advice entry
  is answered from the config metadata: its meaning, type and default.

The index gives the size of each part in tokens (the UTF-8 bytes of the
brief answer divided by 4). An agent reads the index first and asks only
for the parts it can afford.

Secrets never leave the worker. A secret config key shows `***` when it is
set and `''` when it is empty. Inside any list or map value, and in the `app`
section, a value whose key name holds `password`, `passwd`, `secret`,
`token`, `key`, `dsn`, `credential`, `auth`, `private`, `cert`, `salt` or
`oauthbearer.config` (in any letter case) is masked the same way, and a URL
loses its user name and password. These are the words that the UI config
reference masks, so no value is shown here that the UI hides.

The full shapes are in the API contract of the UI
(`docs/api-contract-v1.md`, section "v1.31 changes") and in the OpenAPI
document the worker serves at `/api-docs` when the UI is on.

## The rules

`/api/v1/agent/diagnose` runs every rule on one state snapshot and the
metric values. A rule that fails is listed in `failed` and does not stop the
others; its error text is cut to 500 bytes. Diagnose never reads task output
text, also with `agent.include_payloads: true`.

| Rule | Severity | Finds |
|---|---|---|
| `worker-stalled` | critical | The event loop does not run now. |
| `partition-dead` | critical | A partition loop stopped after its one restart. |
| `sink-breaker-open` | critical | The circuit breaker of a sink is open. |
| `loop-lag-high` | warning | The loop ran late by `runtime_health.warn_lag` or more. |
| `budget-saturated` | warning | The pending tasks or bytes reached their limit. |
| `lag-growing` | warning | The broker lag grew since a call at least 60 seconds earlier. |
| `recorder-dropping` | warning | The recorder dropped events or Postgres rows since the previous call. |
| `peers-offline` | warning | Peers in the registry count as offline. |
| `cluster-tasks-mismatch` | warning | Online peers of the cluster declare other cluster tasks. |
| `pool-saturated` | info | Every executor slot is busy and tasks wait. |
| `partition-paused-by-debug` | info | An operator paused a partition from the UI. |
| `dlq-writing` | info | The worker wrote DLQ records since the previous call. |
| `agent-unauthenticated` | info | The agent routes need no token. |

Three rules compare with the previous call on the same worker process; the
first call never fires them. Each rule's text is in the
[Agent Reference](agent-reference.md#rules).

## Security

The agent routes show the effective config, the worker state and the names
and numbers of the failed tasks. They show no secret and, unless
`agent.include_payloads` is true, no task output.

- Set `agent.auth_token`. With the surface on and no token the worker logs
  `agent_surface_unauthenticated` at start, and the rule
  `agent-unauthenticated` reports it.
- The routes are on the API port, which also serves `/healthz` and the peer
  route. Keep that port on a network you trust, as for the UI. See
  [Security posture](security.md#the-agent-routes).
- Turn on `agent.include_payloads` only when the agent may read task output:
  stderr and arguments can hold customer data.
- Nothing on the surface blocks the worker. Live state is read through the
  main loop with a time limit; a part that could not be read is `null`. The
  registry loads on a worker thread. Handler rules run on threads of their
  own with a limit of 1 second each.

## The handler notes

The framework does not know your business. Your handler can add concepts,
recipes, notes for its own metrics and `app` keys, and its own diagnose
rules with `agent_notes()`. The notes appear in the manual part `app`, on
`/api/v1/agent/config` and `/api/v1/agent/metrics`, and the rules run after
the framework rules on `/api/v1/agent/diagnose`. A short example:

```python
import drakkar as dk


def refunds_waiting(snapshot: dk.AgentSnapshot) -> dk.AgentFinding | None:
    budget = snapshot.part('budget')
    if budget is None or budget['tasks'] < 100:
        return None
    return dk.AgentFinding(evidence={'tasks': budget['tasks']})


class BillingHandler(dk.BaseDrakkarHandler[BillingInput, BillingOutput]):
    def agent_notes(self) -> dk.AgentNotes | None:
        return dk.AgentNotes(
            concepts=[
                dk.AgentTopic(
                    key='refund',
                    summary='A refund message makes one ledger write.',
                    related=['concept:task'],
                ),
            ],
            app_config={'ledger.url': 'The URL of the ledger service.'},
            rules=[
                dk.AgentRule(
                    key='refunds-waiting',
                    severity='warning',
                    title='Many refund tasks wait',
                    suggestion='Check the ledger service latency.',
                    check=refunds_waiting,
                ),
            ],
        )
```

The rules for keys, references and the checks at startup are on the
[Handler](handler.md#agent-notes) page.

## The offline command

`drakkar-agent` (also `python -m drakkar.agent`) prints the same texts with no
running worker:

```bash
drakkar-agent manual --parts=concepts,recipes --format=markdown
drakkar-agent manual --topic=recipe:lag-growing
drakkar-agent config --config=drakkar.yaml --parts=executor,sinks --detail=full
drakkar-agent reference
```

- `manual [--parts=...] [--detail=brief|full] [--format=json|markdown] [--topic=<kind>:<key>]`
  prints the framework manual, without handler notes. PromQL queries keep
  their `{{worker}}` and `{{cluster}}` placeholders.
- `config --config=<path> [--parts=...] [--detail=brief|full]` loads the
  file and the `DK_` variables with the strict loader and prints the
  effective config. The origins are `default`, `yaml` and `env`; the `app`
  section is shown as the file gives it.
- `reference` prints the [Agent Reference](agent-reference.md) page.

Every flag is written `--name=value`. A JSON output ends with one line
break. The exit code is 0, or 2 for a usage error, an unknown topic or a
config that does not load; then the reason goes to stderr and nothing to
stdout.

## Use it from an agent skill

An agent spends less context when it reads the index first and asks for
small parts. A skill text for a coding agent can say:

```markdown
---
name: drakkar-worker
description: Explain or debug a running Drakkar worker through its agent routes.
---

$DRAKKAR_URL is the API server of one worker: its `api.public_url`, or
`http://<host>:<api.port>` (port 8080 by default). Send
`Authorization: Bearer $DRAKKAR_AGENT_TOKEN` with every request.

- A 404 with "the agent surface is off" means `agent.enabled` is false on that
  worker. Stop and ask the operator to turn it on.
- A 401 means the token is missing or wrong. Stop and ask for the token. Do
  not retry with other values.

1. Read `GET /api/v1/agent` first. Note `start_here`, the `registry_version`
   and the `approx_tokens` of each part. Ask only for parts you can afford.
2. Call `GET /api/v1/agent/diagnose`. Start from the findings with the
   highest severity.
3. For each finding, read `GET /api/v1/agent/manual?topic=<explain>` (the
   `explain` reference of the finding), then the recipe it names with
   `topic=recipe:<key>`. URL-encode the reference: a route reference holds
   spaces (`topic=route%3AGET%20%2Fapi%2Fv1%2Fdebug%2Fdatabases`).
4. Check what the recipe asks for with `/api/v1/agent/state?parts=...`,
   `/api/v1/agent/metrics?names=...` and `/api/v1/agent/config?parts=...`.
5. Report what you found and the evidence. Do not guess a config change:
   quote the `advice` of the key (`detail=full`) and its current `origin`.
```

The `registry_version` changes only when the knowledge changes, so an agent
can keep the manual it read before and read it again only when the version
differs.
