#!/usr/bin/env bash
# ============================================================================
# Shared helpers of the provider outage scenarios (scenario-*.sh).
#
# Sourced, not run. Each scenario sets SCENARIO before it calls a verdict
# helper, prints exactly one verdict line, and exits 0 (pass, not
# applicable) or 1 (fail).
#
# HARNESS_PROVIDER names the main cluster's registry and periodic-election
# provider: filesystem (the default), postgres, redis or consul. It must
# match the value the harness was started with
# (just integration-up provider=...).
# ============================================================================

# The variables below are read by the scenarios that source this file.
# shellcheck disable=SC2034
SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCENARIO_DIR")"
HARNESS_PROVIDER="${HARNESS_PROVIDER:-filesystem}"
COMPOSE=(docker compose -f "$SCENARIO_DIR/docker-compose.yml")
MAIN_WORKERS=(worker-1 worker-2 worker-3)
SCENARIO="${SCENARIO:-scenario}"
# Seconds between two checks of a wait_until condition.
POLL_SECONDS=2
# Seconds the last wait_until call waited; scripts print it as a timing.
WAITED=0

# One row per provider, two columns:
#   PROVIDER_SERVICE  the compose service that keeps the provider's state.
#                     The filesystem provider keeps its state in the shared
#                     folder and has no service, so its value is empty.
#   LIVENESS_STYLE    how the list shows a worker that stopped publishing:
#                     offline - its card stays listed with online false
#                               until it is deleted (prune_after_seconds);
#                     gone    - its card leaves the list: the card key
#                               belongs to the worker's session, and the
#                               service deletes the key when the session
#                               ends. A worker started again under the same
#                               name takes the card over from the session of
#                               the worker before it.
case "$HARNESS_PROVIDER" in
    filesystem) PROVIDER_SERVICE="";            LIVENESS_STYLE=offline ;;
    postgres)   PROVIDER_SERVICE=postgres-fleet; LIVENESS_STYLE=offline ;;
    redis)      PROVIDER_SERVICE=redis-fleet;    LIVENESS_STYLE=offline ;;
    consul)     PROVIDER_SERVICE=consul-fleet;   LIVENESS_STYLE=gone ;;
    *) echo "unknown HARNESS_PROVIDER '$HARNESS_PROVIDER': expected filesystem, postgres, redis or consul" >&2; exit 1 ;;
esac

# ANSI colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

ts() { date '+%H:%M:%S'; }

log()  { echo -e "${CYAN}[$(ts)]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(ts)] ⚠${NC}  $*"; }
ok()   { echo -e "${GREEN}[$(ts)] ✓${NC}  $*"; }
step() { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━${NC}\n"; }

# Print the failing check and the FAIL verdict, then exit 1.
fail() {
    echo -e "${RED}[$(ts)] ✗${NC}  $*"
    echo -e "${RED}${BOLD}VERDICT ${SCENARIO} (${HARNESS_PROVIDER}): FAIL: $*${NC}"
    exit 1
}

# Print the PASS verdict, then exit 0.
pass() {
    echo -e "${GREEN}${BOLD}VERDICT ${SCENARIO} (${HARNESS_PROVIDER}): PASS: $*${NC}"
    exit 0
}

# Print the not-applicable verdict, then exit 0.
not_applicable() {
    echo -e "${YELLOW}${BOLD}VERDICT ${SCENARIO} (${HARNESS_PROVIDER}): not applicable: $*${NC}"
    exit 0
}

# The /api/v1/workers answer of the worker whose UI is on host port $1.
workers_json() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf --connect-timeout 2 --max-time 3 "http://localhost:$1/api/v1/workers"
}

# One field of one worker in the list of the worker on host port $1:
#   peer_field <port> <worker name> <field>
# Prints the field as JSON (true, false, null, "text"), `absent` when the
# worker is not listed or has no such field, `unreachable` when the list
# could not be read.
peer_field() {
    local listing
    listing=$(workers_json "$1") || { echo unreachable; return 0; }
    WORKER_NAME="$2" FIELD="$3" python3 -c '
import json, os, sys
rows = json.load(sys.stdin)
for row in rows:
    if row.get("worker_name") == os.environ["WORKER_NAME"]:
        field = os.environ["FIELD"]
        print(json.dumps(row[field]) if field in row else "absent")
        break
else:
    print("absent")
' <<<"$listing"
}

# The value of one sample on the metrics page of host port $1:
#   metric <port> <name> [<label>="<value>",...]
# The labels given must all match; other labels of the sample are ignored.
# Prints the value as a number, `absent` when no sample matches,
# `unreachable` when the page could not be read.
metric() {
    local page
    page=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:$1/metrics") || { echo unreachable; return 0; }
    METRIC_NAME="$2" METRIC_LABELS="${3:-}" python3 -c '
import os, re, sys
LABEL = re.compile(r"(\w+)=\"((?:[^\"\\]|\\.)*)\"")
SAMPLE = re.compile(r"^(\w+)(?:\{(.*)\})?\s+(\S+)")
wanted = dict(LABEL.findall(os.environ["METRIC_LABELS"]))
for line in sys.stdin:
    match = SAMPLE.match(line)
    if not match or match.group(1) != os.environ["METRIC_NAME"]:
        continue
    labels = dict(LABEL.findall(match.group(2) or ""))
    if all(labels.get(key) == value for key, value in wanted.items()):
        print(float(match.group(3)))
        break
else:
    print("absent")
' <<<"$page"
}

# True when the sample printed by `metric` equals the number $4:
#   metric_equals <port> <name> <labels> <number>
metric_equals() {
    local value
    value=$(metric "$1" "$2" "$3")
    python3 -c 'import sys; sys.exit(0 if sys.argv[1] not in ("absent", "unreachable") and float(sys.argv[1]) == float(sys.argv[2]) else 1)' "$value" "$4"
}

# The sum of every series of the counter $2 on the metrics page of host
# port $1 (a counter may carry labels). Prints `unreachable` when the page
# could not be read, 0 when no series exists yet.
#   metric_sum <port> <name>
metric_sum() {
    local page
    page=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:$1/metrics") || { echo unreachable; return 0; }
    METRIC_NAME="$2" python3 -c '
import os, re, sys
SAMPLE = re.compile(r"^(\w+)(?:\{.*\})?\s+(\S+)")
total = 0.0
for line in sys.stdin:
    match = SAMPLE.match(line)
    if match and match.group(1) == os.environ["METRIC_NAME"]:
        total += float(match.group(2))
print(total)
' <<<"$page"
}

# The number of messages the running producer was started with. Reading it
# from the container keeps every check in step with the profile the harness
# was brought up with.
harness_total_messages() {
    local id total
    id=$(container_id producer)
    if [ -z "$id" ] || [ "$(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null)" != true ]; then
        echo "the producer container is not running; start the harness with just integration-up" >&2
        return 1
    fi
    total=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$id" | sed -n 's/^TOTAL_MESSAGES=//p')
    if [ -z "$total" ]; then
        echo "the producer container has no TOTAL_MESSAGES in its environment" >&2
        return 1
    fi
    echo "$total"
}

# The container id of compose service $1.
container_id() {
    "${COMPOSE[@]}" ps -aq "$1"
}

# How often Docker restarted the container of service $1.
restart_count() {
    docker inspect -f '{{.RestartCount}}' "$(container_id "$1")"
}

# One line per service: the restart count and the start time of its
# container. Two equal snapshots mean no container was restarted between
# them (the workers have no restart policy, so the start time is what a
# restart would change).
#   restart_snapshot <service>...
restart_snapshot() {
    local service
    for service in "$@"; do
        echo "${service} restarts=$(restart_count "$service") started=$(docker inspect -f '{{.State.StartedAt}}' "$(container_id "$service")")"
    done
}

# Run a command until it succeeds or the time is up:
#   wait_until <seconds> <command> [args...]
# Returns 0 as soon as the command succeeds within <seconds>, 1 when
# <seconds> pass without a success, also when the try that succeeded ended
# after <seconds>. Sets WAITED to the seconds it waited.
wait_until() {
    local limit=$1
    shift
    local started=$SECONDS
    while true; do
        if "$@"; then
            WAITED=$((SECONDS - started))
            # A slow try that succeeds after the limit is still too late.
            if [ "$WAITED" -gt "$limit" ]; then
                return 1
            fi
            return 0
        fi
        if [ $((SECONDS - started)) -ge "$limit" ]; then
            WAITED=$((SECONDS - started))
            return 1
        fi
        sleep "$POLL_SECONDS"
    done
}

# True when the worker on host port $1 lists worker $2 with `online` $3
# (true, false or null).
peer_online_is() {
    [ "$(peer_field "$1" "$2" online)" = "$3" ]
}

# True when the worker on host port $1 does not list worker $2.
peer_absent() {
    [ "$(peer_field "$1" "$2" worker_name)" = absent ]
}

# True when the worker on host port $1 does not list worker $2, or lists it
# with online false. False when the list could not be read.
peer_absent_or_offline() {
    local online
    online=$(peer_field "$1" "$2" online)
    [ "$online" = absent ] || [ "$online" = false ]
}

# Sleep until <seconds> have passed since the SECONDS value $1:
#   sleep_until_elapsed <start> <seconds>
sleep_until_elapsed() {
    local left=$(($2 - (SECONDS - $1)))
    if [ "$left" -gt 0 ]; then
        sleep "$left"
    fi
}

# The current time in seconds since the epoch, to the millisecond.
epoch_now() {
    python3 -c 'import time; print(f"{time.time():.3f}")'
}

# The seconds from the epoch time $1 to the epoch time $2, to 0.1 s.
seconds_between() {
    python3 -c 'import sys; print(f"{float(sys.argv[2]) - float(sys.argv[1]):.1f}")' "$1" "$2"
}

# The first slot boundary after now of a schedule every $1 seconds of epoch
# time, in whole seconds since the epoch (a cluster task's slots).
#   next_slot_boundary <interval>
next_slot_boundary() {
    python3 -c 'import sys, time; interval = int(sys.argv[1]); print((int(time.time()) // interval + 1) * interval)' "$1"
}

# Sleep until the epoch time $1; return at once when it has passed.
#   sleep_until_epoch <epoch>
sleep_until_epoch() {
    sleep "$(python3 -c 'import sys, time; print(f"{max(0.0, float(sys.argv[1]) - time.time()):.3f}")' "$1")"
}

# True when the epoch time $1 is later than the epoch time $2.
#   epoch_after <epoch> <epoch>
epoch_after() {
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)' "$1" "$2"
}

# The agent's HTTP API of consul-fleet on the host.
CONSUL_HOST_URL=http://localhost:8500

# The management token of consul-fleet, read from docker-compose.yml so the
# harness value lives in one file.
consul_management_token() {
    "${COMPOSE[@]}" config --format=json \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["consul-fleet-init"]["environment"]["CONSUL_HTTP_TOKEN"])'
}

# GET one path of the agent's API with the management token $1:
#   consul_get <token> <path>
consul_get() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf --connect-timeout 2 --max-time 3 -H "X-Consul-Token: $1" "${CONSUL_HOST_URL}$2"
}

# The command that plants an already-due history window into a shared
# folder (integration/plant_history.py). It takes --shared-dir, --cluster,
# --files, --events-per-file and --hours-ago, and prints one line:
#   PLANTED scope=<s> window_start=<epoch> window_end=<epoch> files=<n> archive=<name>
PLANT_HISTORY_CMD=(uv run --project "$REPO_DIR" python "$REPO_DIR/integration/plant_history.py")

# Make the first run of PLANT_HISTORY_CMD start at once: uv creates or
# syncs the project environment here, before any timed wait of a scenario.
plant_history_prepare() {
    "${PLANT_HISTORY_CMD[@]}" --help >/dev/null
}

# The lock of the archive task of the main cluster: the scope is
# <cluster>/<task>, with "main cluster" sanitized. Where each provider keeps
# it (the connections of worker/drakkar.yaml).
LOCK_SCOPE=main_cluster/history.archive
LOCK_FILE="$SCENARIO_DIR/shared/tasks/${LOCK_SCOPE}.lock"
LOCK_POSTGRES_TABLE=drakkar.task_locks
LOCK_REDIS_DATABASE=3
LOCK_REDIS_KEY="drakkar:task-locks:${LOCK_SCOPE}"
LOCK_CONSUL_PATH="/v1/kv/drakkar/task-locks/${LOCK_SCOPE}"

# Read the admin settings that lock_holder needs from the compose config,
# once, into LOCK_ADMIN_USER, LOCK_ADMIN_PASSWORD, LOCK_ADMIN_DATABASE and
# LOCK_ADMIN_TOKEN. Call it in the scenario's own shell before polling
# lock_holder: lock_holder runs in a subshell and cannot keep what it reads.
lock_holder_prepare() {
    local config settings
    case "$HARNESS_PROVIDER" in
        filesystem) return 0 ;;
        consul)
            LOCK_ADMIN_TOKEN=$(consul_management_token) || return 1
            [ -n "$LOCK_ADMIN_TOKEN" ] || { echo "docker-compose.yml gives consul-fleet-init no CONSUL_HTTP_TOKEN" >&2; return 1; }
            return 0
            ;;
    esac
    config=$("${COMPOSE[@]}" config --format=json) || return 1
    # Postgres: the user and database of postgres-fleet's environment.
    # Redis: the first user of redis-fleet's command that is on and has a
    # password (">password"); the default user is off.
    settings=$(PROVIDER="$HARNESS_PROVIDER" python3 -c '
import json, os, sys
services = json.load(sys.stdin)["services"]
if os.environ["PROVIDER"] == "postgres":
    env = services["postgres-fleet"]["environment"]
    print(env["POSTGRES_USER"], env["POSTGRES_DB"])
else:
    words = services["redis-fleet"]["command"]
    starts = [i for i, word in enumerate(words) if word == "--user"] + [len(words)]
    for begin, end in zip(starts, starts[1:]):
        rules = words[begin + 1 : end]
        passwords = [rule[1:] for rule in rules[1:] if rule.startswith(">")]
        if "on" in rules[1:] and passwords:
            print(rules[0], passwords[0])
            break
    else:
        sys.exit("redis-fleet has no user that is on and has a password")
' <<<"$config") || return 1
    case "$HARNESS_PROVIDER" in
        postgres) read -r LOCK_ADMIN_USER LOCK_ADMIN_DATABASE <<<"$settings" ;;
        redis) read -r LOCK_ADMIN_USER LOCK_ADMIN_PASSWORD <<<"$settings" ;;
    esac
}

# The `worker` field of a lock body (JSON on stdin); nothing for no body.
lock_body_worker() {
    python3 -c '
import json, sys
text = sys.stdin.read().strip()
if text:
    print(json.loads(text)["worker"])
'
}

# Print the worker that holds the archive lock of the main cluster, or
# nothing when no lock is held. Returns 1 when the lock could not be read.
# Needs lock_holder_prepare first (filesystem excepted).
#   filesystem - the body of tasks/main_cluster/history.archive.lock in the
#                shared folder;
#   postgres   - the row of the scope in drakkar.task_locks;
#   redis      - the lock key's body on database 3 of redis-fleet;
#   consul     - the lock key's body, only while a session holds the key.
# A lock that its holder left behind when it died stays until another
# worker takes it over (filesystem, postgres) or it expires (redis: the
# key's PX; consul: the session's TTL).
lock_holder() {
    local response status
    case "$HARNESS_PROVIDER" in
        filesystem)
            # The holder writes the body right after it creates the file,
            # so a read can see an empty file for a moment: no holder yet.
            LOCK_FILE="$LOCK_FILE" python3 -c '
import json, os
try:
    with open(os.environ["LOCK_FILE"], encoding="utf-8") as body:
        text = body.read().strip()
except FileNotFoundError:
    text = ""
if text:
    print(json.loads(text)["worker"])
'
            ;;
        postgres)
            "${COMPOSE[@]}" exec -T postgres-fleet psql -q -t -A -v ON_ERROR_STOP=1 \
                -U "$LOCK_ADMIN_USER" -d "$LOCK_ADMIN_DATABASE" \
                -c "SELECT worker FROM ${LOCK_POSTGRES_TABLE} WHERE scope = '${LOCK_SCOPE}'"
            ;;
        redis)
            response=$("${COMPOSE[@]}" exec -T redis-fleet redis-cli --user "$LOCK_ADMIN_USER" \
                --pass "$LOCK_ADMIN_PASSWORD" --no-auth-warning -n "$LOCK_REDIS_DATABASE" \
                GET "$LOCK_REDIS_KEY") || return 1
            lock_body_worker <<<"$response"
            ;;
        consul)
            # Not consul_get: a key that does not exist answers 404, which
            # here means no lock, not a failed read.
            response=$(curl -s --connect-timeout 2 --max-time 3 -H "X-Consul-Token: ${LOCK_ADMIN_TOKEN}" \
                -w '\n%{http_code}' "${CONSUL_HOST_URL}${LOCK_CONSUL_PATH}") || return 1
            status=${response##*$'\n'}
            case "$status" in
                404) return 0 ;;
                200) ;;
                *) echo "GET ${LOCK_CONSUL_PATH} answered ${status}" >&2; return 1 ;;
            esac
            python3 -c '
import base64, json, sys
entry = json.load(sys.stdin)[0]
if entry.get("Session") and entry.get("Value"):
    print(json.loads(base64.b64decode(entry["Value"]))["worker"])
' <<<"${response%$'\n'*}"
            ;;
    esac
}

# The harness_cluster_tick lines that worker-1, worker-2 and worker-3 logged
# since $1 (a time that `docker compose logs --since` takes), one line per
# tick: <worker> <slot>. The handler logs the slot of each run of its
# cluster task. Returns 1 when a log could not be read.
#   cluster_ticks <since>
cluster_ticks() {
    local service logs
    for service in "${MAIN_WORKERS[@]}"; do
        logs=$("${COMPOSE[@]}" logs --no-color --no-log-prefix --since="$1" "$service" 2>/dev/null) || return 1
        WORKER_NAME="$service" python3 -c '
import json, os, sys
for line in sys.stdin:
    start = line.find("{")
    if start < 0:
        continue
    try:
        record = json.loads(line[start:])
    except ValueError:
        continue
    if isinstance(record, dict) and record.get("event") == "harness_cluster_tick":
        print(os.environ["WORKER_NAME"], record.get("slot"))
' <<<"$logs"
    done
}

# The slots that ran more than once, from `cluster_ticks` lines on stdin, one
# line per slot: <slot> <worker>,<worker>[,...]. Nothing when every slot ran
# at most once.
doubled_tick_slots() {
    python3 -c '
import sys
workers = {}
for line in sys.stdin:
    fields = line.split()
    if len(fields) == 2:
        workers.setdefault(fields[1], []).append(fields[0])
for slot in sorted(workers):
    if len(workers[slot]) > 1:
        print(slot, ",".join(workers[slot]))
'
}
