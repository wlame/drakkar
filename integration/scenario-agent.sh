#!/usr/bin/env bash
# ============================================================================
# Scenario: the agent routes answer on running workers.
#
# docker-compose.yml turns the agent surface on for worker-1 and for
# headless-recorder (DK_AGENT__ENABLED, DK_AGENT__AUTH_TOKEN); worker-2
# keeps the default (off). The tokens are read from docker-compose.yml, so
# the harness values live in one file. The scenario checks, in order:
#   1. worker-2 GET /api/v1/agent answers 404 with the "surface is off"
#      message,
#   2. worker-1 GET /api/v1/agent answers 401 with no token and with a wrong
#      token, with the "token required" message,
#   3. worker-1 GET /api/v1/agent with the token answers the index: schema
#      1, worker.name worker-1, and the five agent routes,
#   4. worker-1 GET /api/v1/agent/manual?topic=concept:pipeline answers the
#      pipeline concept under parts.concepts,
#   5. worker-1 GET /api/v1/agent/state?parts=budget,partitions answers the
#      budget numbers and a partition list with at least one row, and no
#      other part,
#   6. worker-1 GET /api/v1/agent/diagnose answers findings, checked and
#      failed, and checked lists every framework rule,
#   7. worker-1 GET /api/v1/agent/config?parts=connections lists the
#      connections with the DSN masked, and neither that answer nor the
#      agent part of the config holds a password or a token,
#   8. headless-recorder (ui.enabled false, API on host port 8093), started
#      by the scenario and removed at the end, also after a failure, answers
#      /api/v1/agent with its token and still answers 404 on
#      /api/v1/identity.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-agent.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-agent
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

WORKER_1_PORT=8081
WORKER_2_PORT=8082
HEADLESS_SERVICE=headless-recorder
HEADLESS_PORT=8093
# The API of headless-recorder answers within this long of the start; the
# first run also builds the image.
API_LIMIT_SECONDS=180
# worker-1 lists a Kafka partition within this long.
PARTITIONS_LIMIT_SECONDS=120
# Longer than executor.drain_timeout (30s) of headless-recorder.
STOP_TIMEOUT_SECONDS=60
AGENT_OFF_MESSAGE='the agent surface is off; set agent.enabled to true'
TOKEN_MESSAGE='agent token required'
AGENT_ROUTES='/api/v1/agent/manual /api/v1/agent/config /api/v1/agent/metrics /api/v1/agent/state /api/v1/agent/diagnose'
FRAMEWORK_RULES='worker-stalled loop-lag-high budget-saturated pool-saturated partition-dead partition-paused-by-debug sink-breaker-open lag-growing recorder-dropping dlq-writing peers-offline cluster-tasks-mismatch agent-unauthenticated'
# Text that appears only where a DSN or a URL with its password is printed
# whole.
SECRET_PATTERNS=('drakkar:drakkar@' 'postgresql://drakkar:' 'redis://drakkar:')

scratch=$(mktemp -d)
headless_started=false

# Stop and remove headless-recorder when this scenario started it, then
# remove the scratch folder.
cleanup() {
    if [ "$headless_started" = true ]; then
        "${COMPOSE[@]}" --profile=headless stop --timeout="$STOP_TIMEOUT_SECONDS" "$HEADLESS_SERVICE" >/dev/null 2>&1 || true
        "${COMPOSE[@]}" --profile=headless rm --force "$HEADLESS_SERVICE" >/dev/null 2>&1 || true
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT

# One environment value of a compose service, read from docker-compose.yml:
#   compose_env <service> <name>
compose_env() {
    "${COMPOSE[@]}" --profile=headless config --format=json | python3 -c '
import json, sys
environment = json.load(sys.stdin)["services"][sys.argv[1]].get("environment") or {}
print(environment.get(sys.argv[2]) or "")
' "$1" "$2"
}

# GET a path of the worker on host port $1 into the file $3, with the bearer
# token $4 when given; prints the HTTP status (000 when nothing answered).
#   http_get <port> <path> <body file> [<token>]
http_get() {
    local auth=()
    if [ -n "${4:-}" ]; then
        auth=(-H "Authorization: Bearer $4")
    fi
    # curl's long options take no `=`, so they keep the space form here.
    curl -s --connect-timeout 2 --max-time 30 -o "$3" -w '%{http_code}' "${auth[@]+"${auth[@]}"}" "http://localhost:$1$2" || true
}

# A short form of a file for a failure text.
excerpt() {
    head -c 600 "$1" | tr '\n' ' '
}

# True when the JSON file $1 has the text $2 as its `error`.
error_is() {
    EXPECTED="$2" python3 -c '
import json, os, sys
sys.exit(0 if json.load(open(sys.argv[1])).get("error") == os.environ["EXPECTED"] else 1)
' "$1"
}

headless_api_is_healthy() {
    [ "$(http_get "$HEADLESS_PORT" /healthz /dev/null)" = 200 ]
}

WORKER_1_TOKEN=$(compose_env worker-1 DK_AGENT__AUTH_TOKEN) \
    || fail "the agent token of worker-1 could not be read from docker-compose.yml"
HEADLESS_TOKEN=$(compose_env "$HEADLESS_SERVICE" DK_AGENT__AUTH_TOKEN) \
    || fail "the agent token of ${HEADLESS_SERVICE} could not be read from docker-compose.yml"
CONSUL_TOKEN=$(compose_env worker-1 DK_CONNECTIONS__CONSUL__MAIN__TOKEN) \
    || fail "the Consul token of worker-1 could not be read from docker-compose.yml"
[ -n "$WORKER_1_TOKEN" ] || fail "docker-compose.yml gives worker-1 no DK_AGENT__AUTH_TOKEN"
[ -n "$HEADLESS_TOKEN" ] || fail "docker-compose.yml gives ${HEADLESS_SERVICE} no DK_AGENT__AUTH_TOKEN"
SECRET_PATTERNS+=("$WORKER_1_TOKEN")
[ -z "$CONSUL_TOKEN" ] || SECRET_PATTERNS+=("$CONSUL_TOKEN")

# Fail when the file $1 contains a secret pattern; $2 names the answer.
check_clean() {
    local pattern
    for pattern in "${SECRET_PATTERNS[@]}"; do
        if grep -qF -- "$pattern" "$1"; then
            fail "${2} contains a secret (a pattern of ${#pattern} characters)"
        fi
    done
}

step "Scenario: the agent routes"

# 1. The surface is off on worker-2.
status=$(http_get "$WORKER_2_PORT" /api/v1/agent "$scratch/off.json")
[ "$status" = 404 ] || fail "worker-2 /api/v1/agent answered ${status}, expected 404: $(excerpt "$scratch/off.json")"
error_is "$scratch/off.json" "$AGENT_OFF_MESSAGE" \
    || fail "the 404 of worker-2 does not carry \"${AGENT_OFF_MESSAGE}\": $(excerpt "$scratch/off.json")"
ok "worker-2 (agent off) answers 404 with the surface-off message"

# 2. The token is required on worker-1.
status=$(http_get "$WORKER_1_PORT" /api/v1/agent "$scratch/no-token.json")
[ "$status" = 401 ] || fail "worker-1 /api/v1/agent with no token answered ${status}, expected 401: $(excerpt "$scratch/no-token.json")"
error_is "$scratch/no-token.json" "$TOKEN_MESSAGE" \
    || fail "the 401 of worker-1 does not carry \"${TOKEN_MESSAGE}\": $(excerpt "$scratch/no-token.json")"
status=$(http_get "$WORKER_1_PORT" /api/v1/agent "$scratch/wrong-token.json" "not-the-${WORKER_1_TOKEN}")
[ "$status" = 401 ] || fail "worker-1 /api/v1/agent with a wrong token answered ${status}, expected 401: $(excerpt "$scratch/wrong-token.json")"
ok "worker-1 answers 401 with no token and with a wrong token"

# 3. The index.
status=$(http_get "$WORKER_1_PORT" /api/v1/agent "$scratch/index.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent answered ${status}: $(excerpt "$scratch/index.json")"
verdict=$(ROUTES="$AGENT_ROUTES" python3 -c '
import json, os, sys
answer = json.load(open(sys.argv[1]))
problems = []
if answer.get("schema") != 1:
    problems.append("schema is " + repr(answer.get("schema")))
name = (answer.get("worker") or {}).get("name")
if name != "worker-1":
    problems.append(f"worker.name is {name!r}")
paths = [route.get("path") for route in answer.get("routes") or []]
if paths != os.environ["ROUTES"].split():
    problems.append(f"routes are {paths}")
version = (answer.get("framework") or {}).get("registry_version") or ""
if len(version) != 16:
    problems.append(f"framework.registry_version is {version!r}")
print("; ".join(problems) or "ok")
' "$scratch/index.json")
[ "$verdict" = ok ] || fail "worker-1 index: ${verdict}"
ok "worker-1 index: schema 1, worker-1, the five agent routes"

# 4. One manual topic.
status=$(http_get "$WORKER_1_PORT" "/api/v1/agent/manual?topic=concept:pipeline" "$scratch/topic.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent/manual?topic=concept:pipeline answered ${status}: $(excerpt "$scratch/topic.json")"
verdict=$(python3 -c '
import json, sys
answer = json.load(open(sys.argv[1]))
concepts = (answer.get("parts") or {}).get("concepts") or []
pipeline = [entry for entry in concepts if entry.get("key") == "pipeline"]
if not pipeline:
    keys = [entry.get("key") for entry in concepts]
    print(f"parts.concepts has no pipeline entry (keys {keys})")
elif not pipeline[0].get("body"):
    print("the pipeline entry has no body")
else:
    print("ok")
' "$scratch/topic.json")
[ "$verdict" = ok ] || fail "worker-1 manual topic: ${verdict}"
ok "worker-1 answers the pipeline concept with its body"

# 5. Two parts of the state. worker-1 gets its partitions some time after
# it is ready, so the read is repeated until the list has a row.
state_has_partitions() {
    [ "$(http_get "$WORKER_1_PORT" "/api/v1/agent/state?parts=budget,partitions" "$scratch/state.json" "$WORKER_1_TOKEN")" = 200 ] \
        && python3 -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1])).get("partitions") else 1)' "$scratch/state.json"
}
wait_until "$PARTITIONS_LIMIT_SECONDS" state_has_partitions \
    || fail "worker-1 /api/v1/agent/state?parts=budget,partitions listed no partition within ${PARTITIONS_LIMIT_SECONDS}s: $(excerpt "$scratch/state.json")"
status=$(http_get "$WORKER_1_PORT" "/api/v1/agent/state?parts=budget,partitions" "$scratch/state.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent/state?parts=budget,partitions answered ${status}: $(excerpt "$scratch/state.json")"
state_check=$(python3 -c '
import json, sys
answer = json.load(open(sys.argv[1]))
problems = []
if sorted(answer) != ["budget", "partitions"]:
    problems.append(f"the answer has the parts {sorted(answer)}")
budget = answer.get("budget") or {}
for key in ("tasks", "max_tasks", "bytes", "max_bytes", "pool_active", "pool_max", "pool_waiting", "waiting_loops"):
    value = budget.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        problems.append(f"budget.{key} is {value!r}")
if not problems and budget["max_tasks"] <= 0:
    problems.append("budget.max_tasks is " + str(budget["max_tasks"]))
partitions = answer.get("partitions")
if not isinstance(partitions, list) or not partitions:
    problems.append(f"partitions is {partitions!r}")
else:
    for row in partitions:
        missing = [key for key in ("partition", "queued", "pending_offsets", "committed", "lag", "paused", "paused_by", "dead") if key not in row]
        if missing:
            problems.append(f"partition row {row} has no {missing}")
            break
if problems:
    print("; ".join(problems))
else:
    print("ok", budget["tasks"], budget["max_tasks"], len(partitions))
' "$scratch/state.json")
read -r state_verdict budget_tasks budget_max_tasks partition_count <<<"$state_check"
[ "$state_verdict" = ok ] || fail "worker-1 state: ${state_check}"
ok "worker-1 state: budget ${budget_tasks} of ${budget_max_tasks} tasks, ${partition_count} partitions"

# 6. Diagnose.
status=$(http_get "$WORKER_1_PORT" /api/v1/agent/diagnose "$scratch/diagnose.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent/diagnose answered ${status}: $(excerpt "$scratch/diagnose.json")"
diagnose_check=$(RULES="$FRAMEWORK_RULES" python3 -c '
import json, os, sys
answer = json.load(open(sys.argv[1]))
problems = []
for key in ("findings", "checked", "failed"):
    if not isinstance(answer.get(key), list):
        problems.append(f"{key} is {answer.get(key)!r}")
if not problems:
    rules = os.environ["RULES"].split()
    framework = [rule for rule in answer["checked"] if not rule.startswith("app:")]
    if sorted(framework) != sorted(rules):
        problems.append(f"checked lists {framework}, expected {sorted(rules)}")
    failed = [row.get("rule") for row in answer["failed"]]
    if failed:
        problems.append(f"failed lists {failed}")
if problems:
    print("; ".join(problems))
else:
    print("ok " + str(len(answer["checked"])) + " " + (",".join(row.get("rule", "?") for row in answer["findings"]) or "none"))
' "$scratch/diagnose.json")
read -r diagnose_verdict checked_count findings <<<"$diagnose_check"
[ "$diagnose_verdict" = ok ] || fail "worker-1 diagnose: ${diagnose_check}"
ok "worker-1 diagnose checked ${checked_count} rules; findings: ${findings}"

# 7. The connections of the config, with the secrets masked.
status=$(http_get "$WORKER_1_PORT" "/api/v1/agent/config?parts=connections" "$scratch/connections.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent/config?parts=connections answered ${status}: $(excerpt "$scratch/connections.json")"
check_clean "$scratch/connections.json" "worker-1 /api/v1/agent/config?parts=connections"
verdict=$(python3 -c '
import json, sys
entries = {entry.get("path"): entry for entry in json.load(open(sys.argv[1])).get("entries") or []}
problems = []
for path in ("connections.postgres.main.dsn", "connections.consul.main.token"):
    entry = entries.get(path)
    if entry is None:
        problems.append(f"no entry {path}")
    elif entry.get("value") != "***" or entry.get("secret") is not True:
        problems.append(f"{path} is not masked")
if "connections.redis.main.url" not in entries:
    problems.append("no entry connections.redis.main.url")
outside = sorted(path for path in entries if not path.startswith("connections."))
if outside:
    problems.append(f"entries outside connections: {outside[:3]}")
print("; ".join(problems) or "ok")
' "$scratch/connections.json")
[ "$verdict" = ok ] || fail "worker-1 config connections: ${verdict}"
status=$(http_get "$WORKER_1_PORT" "/api/v1/agent/config?parts=agent" "$scratch/agent-config.json" "$WORKER_1_TOKEN")
[ "$status" = 200 ] || fail "worker-1 /api/v1/agent/config?parts=agent answered ${status}: $(excerpt "$scratch/agent-config.json")"
check_clean "$scratch/agent-config.json" "worker-1 /api/v1/agent/config?parts=agent"
verdict=$(python3 -c '
import json, sys
entries = {entry.get("path"): entry for entry in json.load(open(sys.argv[1])).get("entries") or []}
token = entries.get("agent.auth_token")
enabled = entries.get("agent.enabled")
problems = []
if token is None or token.get("value") != "***" or token.get("origin") != "env":
    problems.append(f"agent.auth_token is {token!r}")
if enabled is None or enabled.get("value") not in (True, "true") or enabled.get("origin") != "env":
    problems.append(f"agent.enabled is {enabled!r}")
print("; ".join(problems) or "ok")
' "$scratch/agent-config.json")
[ "$verdict" = ok ] || fail "worker-1 config agent: ${verdict}"
ok "worker-1 config masks the DSN, the Consul token and the agent token; no password in the answers"

# 8. The agent surface with the UI off.
step "The agent surface of ${HEADLESS_SERVICE} (UI off)"
headless_started=true
"${COMPOSE[@]}" --profile=headless up --detach --build "$HEADLESS_SERVICE" >/dev/null 2>&1 \
    || fail "docker compose could not start ${HEADLESS_SERVICE} (profile headless)"
wait_until "$API_LIMIT_SECONDS" headless_api_is_healthy \
    || fail "GET :${HEADLESS_PORT}/healthz did not answer 200 within ${API_LIMIT_SECONDS}s"
health_waited=$WAITED
status=$(http_get "$HEADLESS_PORT" /api/v1/agent "$scratch/headless-no-token.json")
[ "$status" = 401 ] || fail "${HEADLESS_SERVICE} /api/v1/agent with no token answered ${status}, expected 401: $(excerpt "$scratch/headless-no-token.json")"
status=$(http_get "$HEADLESS_PORT" /api/v1/agent "$scratch/headless-index.json" "$HEADLESS_TOKEN")
[ "$status" = 200 ] || fail "${HEADLESS_SERVICE} /api/v1/agent answered ${status}: $(excerpt "$scratch/headless-index.json")"
verdict=$(SERVICE="$HEADLESS_SERVICE" python3 -c '
import json, os, sys
answer = json.load(open(sys.argv[1]))
name = (answer.get("worker") or {}).get("name")
schema = answer.get("schema")
print("ok" if schema == 1 and name == os.environ["SERVICE"] else f"schema {schema!r}, worker.name {name!r}")
' "$scratch/headless-index.json")
[ "$verdict" = ok ] || fail "${HEADLESS_SERVICE} index: ${verdict}"
status=$(http_get "$HEADLESS_PORT" /api/v1/identity "$scratch/headless-identity.json")
[ "$status" = 404 ] || fail "${HEADLESS_SERVICE} /api/v1/identity answered ${status} with the UI off, expected 404"
ok "${HEADLESS_SERVICE} answered /healthz after ${health_waited}s, /api/v1/agent 401 then 200 with its token, and /api/v1/identity 404"

pass "worker-2 404, worker-1 401 and the index, the pipeline topic, budget ${budget_tasks}/${budget_max_tasks} with ${partition_count} partitions, diagnose of ${checked_count} rules (findings: ${findings}), masked config, and the agent surface of ${HEADLESS_SERVICE} with the UI off"
