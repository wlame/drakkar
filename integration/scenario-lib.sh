#!/usr/bin/env bash
# ============================================================================
# Shared helpers of the provider outage scenarios (scenario-*.sh).
#
# Sourced, not run. Each scenario sets SCENARIO before it calls a verdict
# helper, prints exactly one verdict line, and exits 0 (pass, not
# applicable) or 1 (fail).
#
# HARNESS_PROVIDER names the main cluster's registry and archive-election
# provider: filesystem (the default), postgres or redis. It must match the
# value the harness was started with (just integration-up provider=...).
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

# The compose service that keeps each provider's state, one row per
# provider. The filesystem provider keeps its state in the shared folder and
# has no service, so its row is empty.
case "$HARNESS_PROVIDER" in
    filesystem) PROVIDER_SERVICE="" ;;
    postgres) PROVIDER_SERVICE=postgres-fleet ;;
    redis) PROVIDER_SERVICE=redis-fleet ;;
    *) echo "unknown HARNESS_PROVIDER '$HARNESS_PROVIDER': expected filesystem, postgres or redis" >&2; exit 1 ;;
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

# Sleep until <seconds> have passed since the SECONDS value $1:
#   sleep_until_elapsed <start> <seconds>
sleep_until_elapsed() {
    local left=$(($2 - (SECONDS - $1)))
    if [ "$left" -gt 0 ]; then
        sleep "$left"
    fi
}
