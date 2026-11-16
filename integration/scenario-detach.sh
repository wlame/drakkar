#!/usr/bin/env bash
# ============================================================================
# Scenario: a worker leaves its consumer group for a bounded time and joins
# it again (detach and attach).
#
# worker-1, worker-2 and worker-3 run with sources.kafka.detach on, a 40 s
# lease, no cooldown, the operator routes on (ui.detach_enabled) and the
# default budget of one detached worker (docker-compose.yml). worker-1 is
# the observer and is never detached or killed. A worker "holds" the
# partitions that its drakkar_assigned_partitions gauge counts; a worker
# holds none when that gauge is 0 and /api/v1/partitions has no live row.
# The checks, each a FAIL that names what it saw:
#   1. An operator detach of worker-2 for 60 s: within drain_timeout + 15 s
#      its state is `detached`, it holds no partition, /readyz answers 200,
#      worker-1 and worker-3 together hold every partition of the topic, and
#      worker-1 lists worker-2 with `detached: true`.
#   2. An operator detach of worker-3 while worker-2 is detached: the
#      outcome is `budget_full`, and worker-3 stays attached and keeps its
#      partitions.
#   3. An operator attach of worker-2: within 30 s its state is `attached`
#      and it holds at least one partition.
#   4. An operator detach of worker-2 for 15 s: the deadline attaches it
#      (state `attached` again), `last_request` is the one of the detach,
#      and worker-2 logged `worker_attached` with cause `deadline`.
#   5. An operator detach of worker-2 for 300 s, then a kill of worker-2
#      (SIGKILL): its slot stays held until the lease ends. A detach of
#      worker-3 right after the kill is `budget_full`. worker-2, started
#      again, is `attached` and takes partitions. A detach of worker-3,
#      tried again every few seconds, succeeds once the dead lease has
#      ended: not before 40 s minus one lease refresh (40 s / 3) after the
#      kill, and within 40 s plus SLOT_FREE_GRACE_SECONDS. worker-3 is then
#      attached by the operator.
#   6. A handler detach, the example of integration/worker/handler.py
#      (detach_on_request): the scenario writes the file
#      detach-request-worker-3 into the shared folder; worker-3 logs
#      harness_detach_result with detached true, its state is `detached`
#      with origin `handler`, and about 20 s later the deadline attaches it
#      (`worker_attached` with cause `deadline` in its log).
# The scenario starts and ends with the three workers attached, so it can
# run twice in a row. On a failure it starts worker-2 again when it is
# killed and asks worker-2 and worker-3 to attach.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=filesystem integration/scenario-detach.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-detach
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

# Host ports of the three main workers (worker/drakkar.yaml, docker-compose.yml).
W1_UI=8081
W2_UI=8082
W3_UI=8083
W1_METRICS=9090
W2_METRICS=9091
W3_METRICS=9093
TOPIC=search-requests

# The settings of docker-compose.yml and worker/drakkar.yaml.
DRAIN_TIMEOUT_SECONDS=5
LEASE_SECONDS=40
# The slot lease is refreshed every lease / 3 seconds, so a killed holder
# refreshed it at most this long before the kill.
LEASE_REFRESH_SECONDS=13
HANDLER_DETACH_SECONDS=20
REQUEST_FILE="$SCENARIO_DIR/shared/detach-request-worker-3"

# Limits.
LEAVE_LIMIT_SECONDS=$((DRAIN_TIMEOUT_SECONDS + 15))
ATTACH_LIMIT_SECONDS=30
OUTCOME_LIMIT_SECONDS=10
DEADLINE_DETACH_SECONDS=15
RESTART_LIMIT_SECONDS=60
SLOT_FREE_GRACE_SECONDS=20
HANDLER_PICKUP_SECONDS=5
START_LIMIT_SECONDS=60

# --- helpers ----------------------------------------------------------------

# One field of the detach state of the worker on host port $1, as JSON
# (true, null, "text", ...); a dotted path reads into last_request:
#   detach_field <port> state
#   detach_field <port> last_request.outcome
# Prints `unreachable` when the state could not be read.
detach_field() {
    local body
    body=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:$1/api/v1/debug/detach") \
        || { echo unreachable; return 0; }
    FIELD_PATH="$2" python3 -c '
import json, os, sys
value = json.load(sys.stdin)
for key in os.environ["FIELD_PATH"].split("."):
    value = value.get(key) if isinstance(value, dict) else None
print(json.dumps(value))
' <<<"$body"
}

# True when the worker on host port $1 is in the detach state $2.
state_is() {
    [ "$(detach_field "$1" state)" = "\"$2\"" ]
}

# POST to a detach route of the worker on host port $1 and fail unless it
# answers 202:
#   post_detach <port> <seconds> <reason>
#   post_attach <port>
post_detach() {
    post_route "$1" detach "{\"duration_seconds\": $2, \"reason\": \"$3\"}"
}
post_attach() {
    post_route "$1" attach '{}'
}
post_route() {
    local code
    # curl's long options take no `=`, so they keep the space form here.
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 5 \
        -X POST -H 'Content-Type: application/json' -d "$3" "http://localhost:$1/api/v1/debug/$2") || code=none
    [ "$code" = 202 ] || fail "POST /api/v1/debug/$2 on port $1 answered ${code}, expected 202"
}

# True when the last request of the worker on host port $1 is newer than
# the at_ms value $2 (null before the first request).
request_newer_than() {
    local at
    at=$(detach_field "$1" last_request.at_ms)
    [ "$at" != null ] && [ "$at" != unreachable ] && { [ "$2" = null ] || [ "$at" -gt "$2" ]; }
}

# The partitions the worker holds by its gauge, as a whole number;
# `unreachable` when the metrics page could not be read.
held_partitions() {
    local value
    value=$(metric "$1" drakkar_assigned_partitions)
    case "$value" in
        absent) echo 0 ;;
        unreachable) echo unreachable ;;
        *) echo "${value%.*}" ;;
    esac
}

# The number of rows of /api/v1/partitions of host port $1 with is_live true.
live_partition_rows() {
    local body
    body=$(curl -sf --connect-timeout 2 --max-time 5 "http://localhost:$1/api/v1/partitions") \
        || { echo unreachable; return 0; }
    python3 -c 'import json, sys; print(sum(1 for row in json.load(sys.stdin) if row.get("is_live")))' <<<"$body"
}

# True when the worker (UI port $1, metrics port $2) holds no partition.
holds_none() {
    [ "$(held_partitions "$2")" = 0 ] && [ "$(live_partition_rows "$1")" = 0 ]
}

# True when the worker on metrics port $1 holds at least one partition.
holds_some() {
    local held
    held=$(held_partitions "$1")
    [ "$held" != unreachable ] && [ "$held" -ge 1 ]
}

# True when worker-1 and worker-3 together hold every partition of the topic.
others_hold_all() {
    local held1 held3
    held1=$(held_partitions "$W1_METRICS")
    held3=$(held_partitions "$W3_METRICS")
    [ "$held1" != unreachable ] && [ "$held3" != unreachable ] && [ $((held1 + held3)) -eq "$TOPIC_PARTITIONS" ]
}

# True when /readyz of host port $1 answers 200.
ready() {
    [ "$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "http://localhost:$1/readyz")" = 200 ]
}

# The log records of service $1 since $2 whose event is $3, one compact
# JSON object per line. Returns 1 when the log could not be read.
#   log_events <service> <since> <event>
log_events() {
    local logs
    logs=$("${COMPOSE[@]}" logs --no-color --no-log-prefix --since="$2" "$1" 2>/dev/null) || return 1
    EVENT_NAME="$3" python3 -c '
import json, os, sys
for line in sys.stdin:
    start = line.find("{")
    if start < 0:
        continue
    try:
        record = json.loads(line[start:])
    except ValueError:
        continue
    if isinstance(record, dict) and record.get("event") == os.environ["EVENT_NAME"]:
        print(json.dumps(record, sort_keys=True))
' <<<"$logs"
}

# True when service $1 logged event $2 since $3 with the field $4 equal to
# the JSON value $5.
#   logged_with <service> <event> <since> <field> <json value>
logged_with() {
    local records
    records=$(log_events "$1" "$3" "$2") || return 1
    FIELD="$4" WANTED="$5" python3 -c '
import json, os, sys
wanted = json.loads(os.environ["WANTED"])
sys.exit(0 if any(json.loads(line).get(os.environ["FIELD"]) == wanted for line in sys.stdin if line.strip()) else 1)
' <<<"$records"
}

# Wait until the last request of the worker on host port $1 is newer than
# the at_ms value $2 and print its outcome; fail after OUTCOME_LIMIT_SECONDS.
await_outcome() {
    POLL_SECONDS=1 wait_until "$OUTCOME_LIMIT_SECONDS" request_newer_than "$1" "$2" \
        || fail "the worker on port $1 recorded no new request within ${OUTCOME_LIMIT_SECONDS}s (last_request: $(detach_field "$1" last_request))"
    detach_field "$1" last_request.outcome
}

# A short description of the detach state and the partitions of a worker,
# for failure messages:  describe <ui port> <metrics port>
describe() {
    echo "state $(detach_field "$1" state), origin $(detach_field "$1" origin), last_request $(detach_field "$1" last_request), gauge $(held_partitions "$2"), live rows $(live_partition_rows "$1")"
}

worker2_killed=false
# On exit: start worker-2 again when the scenario killed it, and ask
# worker-2 and worker-3 to attach, so a failed run leaves no worker away.
clean_up() {
    if [ "$worker2_killed" = true ]; then
        docker start "$(container_id worker-2)" >/dev/null 2>&1 || true
    fi
    rm -f "$REQUEST_FILE"
    for port in "$W2_UI" "$W3_UI"; do
        curl -s -o /dev/null --connect-timeout 2 --max-time 5 -X POST "http://localhost:${port}/api/v1/debug/attach" || true
    done
}
trap clean_up EXIT

# --- start ------------------------------------------------------------------

step "Scenario: detach and attach of the main-cluster workers"

rm -f "$REQUEST_FILE"
TOPIC_PARTITIONS=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 \
    --describe --topic="$TOPIC" 2>/dev/null | grep -c 'Partition: ' || true)
[ "$TOPIC_PARTITIONS" -ge 3 ] || fail "the topic ${TOPIC} has ${TOPIC_PARTITIONS} partitions by kafka-topics.sh, expected at least 3"
log "the topic ${TOPIC} has ${TOPIC_PARTITIONS} partitions"

all_attached_and_holding() {
    state_is "$W1_UI" attached && state_is "$W2_UI" attached && state_is "$W3_UI" attached \
        && holds_some "$W1_METRICS" && holds_some "$W2_METRICS" && holds_some "$W3_METRICS"
}
wait_until "$START_LIMIT_SECONDS" all_attached_and_holding \
    || fail "the three workers are not attached with partitions within ${START_LIMIT_SECONDS}s: worker-1 $(describe "$W1_UI" "$W1_METRICS"); worker-2 $(describe "$W2_UI" "$W2_METRICS"); worker-3 $(describe "$W3_UI" "$W3_METRICS")"
for port in "$W1_UI" "$W2_UI" "$W3_UI"; do
    [ "$(detach_field "$port" enabled)" = true ] && [ "$(detach_field "$port" ui_enabled)" = true ] \
        || fail "the worker on port ${port} does not have detach and its routes on: enabled $(detach_field "$port" enabled), ui_enabled $(detach_field "$port" ui_enabled)"
done
ok "the three workers are attached and hold partitions"

# --- 1. operator detach of worker-2 -----------------------------------------

step "1. operator detach of worker-2 for 60 s"
moved=$(held_partitions "$W2_METRICS")
before=$(detach_field "$W2_UI" last_request.at_ms)
start_at=$(epoch_now)
post_detach "$W2_UI" 60 "harness detach"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
leave_seconds=$(seconds_between "$start_at" "$(epoch_now)")
outcome=$(await_outcome "$W2_UI" "$before")
[ "$outcome" = '"detached"' ] || fail "the outcome of the detach of worker-2 is ${outcome}, expected \"detached\""
[ "$(detach_field "$W2_UI" origin)" = '"operator"' ] || fail "worker-2 is detached with origin $(detach_field "$W2_UI" origin), expected \"operator\""
holds_none "$W2_UI" "$W2_METRICS" || fail "worker-2 is detached and still holds partitions: $(describe "$W2_UI" "$W2_METRICS")"
ready "$W2_UI" || fail "/readyz of worker-2 does not answer 200 while it is detached"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" others_hold_all \
    || fail "worker-1 and worker-3 hold $(held_partitions "$W1_METRICS") + $(held_partitions "$W3_METRICS") partitions ${LEAVE_LIMIT_SECONDS}s after worker-2 left, expected ${TOPIC_PARTITIONS}"
takeover_seconds=$(seconds_between "$start_at" "$(epoch_now)")
worker1_lists_detached() {
    [ "$(peer_field "$W1_UI" worker-2 detached)" = true ]
}
wait_until 10 worker1_lists_detached \
    || fail "worker-1 lists worker-2 with detached $(peer_field "$W1_UI" worker-2 detached), expected true"
ok "worker-2 left in ${leave_seconds}s with ${moved} partitions; worker-1 and worker-3 held all ${TOPIC_PARTITIONS} after ${takeover_seconds}s; worker-1 lists worker-2 detached"

# --- 2. a second detach does not fit the budget -----------------------------

step "2. operator detach of worker-3 while worker-2 is detached"
held3=$(held_partitions "$W3_METRICS")
before=$(detach_field "$W3_UI" last_request.at_ms)
post_detach "$W3_UI" 60 "harness second detach"
outcome=$(await_outcome "$W3_UI" "$before")
[ "$outcome" = '"budget_full"' ] || fail "the detach of worker-3 while worker-2 is detached ended ${outcome}, expected \"budget_full\""
state_is "$W3_UI" attached || fail "worker-3 is not attached after its refused detach: $(describe "$W3_UI" "$W3_METRICS")"
holds_some "$W3_METRICS" || fail "worker-3 holds no partition after its refused detach (it held ${held3}): $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 was refused with budget_full and holds $(held_partitions "$W3_METRICS") partitions"

# --- 3. operator attach of worker-2 -----------------------------------------

step "3. operator attach of worker-2"
start_at=$(epoch_now)
post_attach "$W2_UI"
attached_with_partitions() {
    state_is "$W2_UI" attached && holds_some "$W2_METRICS"
}
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" attached_with_partitions \
    || fail "worker-2 is not attached with a partition ${ATTACH_LIMIT_SECONDS}s after the attach: $(describe "$W2_UI" "$W2_METRICS")"
return_seconds=$(seconds_between "$start_at" "$(epoch_now)")
[ "$(detach_field "$W2_UI" last_request.outcome)" = '"attached"' ] \
    || fail "the outcome of the attach of worker-2 is $(detach_field "$W2_UI" last_request.outcome), expected \"attached\""
ok "worker-2 attached and held $(held_partitions "$W2_METRICS") partitions ${return_seconds}s after the attach"

# --- 4. the deadline ends a detach ------------------------------------------

step "4. operator detach of worker-2 for ${DEADLINE_DETACH_SECONDS} s, ended by the deadline"
since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
before=$(detach_field "$W2_UI" last_request.at_ms)
post_detach "$W2_UI" "$DEADLINE_DETACH_SECONDS" "harness deadline"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
outcome=$(await_outcome "$W2_UI" "$before")
[ "$outcome" = '"detached"' ] || fail "the outcome of the detach of worker-2 is ${outcome}, expected \"detached\""
detach_request=$(detach_field "$W2_UI" last_request)
until_ms=$(detach_field "$W2_UI" until_ms)
POLL_SECONDS=1 wait_until $((DEADLINE_DETACH_SECONDS + ATTACH_LIMIT_SECONDS)) state_is "$W2_UI" attached \
    || fail "worker-2 is not attached $((DEADLINE_DETACH_SECONDS + ATTACH_LIMIT_SECONDS))s after its ${DEADLINE_DETACH_SECONDS}s detach: $(describe "$W2_UI" "$W2_METRICS")"
attached_at=$(epoch_now)
deadline_lag=$(python3 -c 'import sys; print(f"{float(sys.argv[1]) - int(sys.argv[2]) / 1000:.1f}")' "$attached_at" "$until_ms")
[ "$(detach_field "$W2_UI" last_request)" = "$detach_request" ] \
    || fail "the deadline changed last_request of worker-2 from ${detach_request} to $(detach_field "$W2_UI" last_request)"
wait_until 10 logged_with worker-2 worker_attached "$since" cause '"deadline"' \
    || fail "worker-2 logged no worker_attached with cause deadline since ${since} (lines: $(log_events worker-2 "$since" worker_attached | tr '\n' ' '))"
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" holds_some "$W2_METRICS" \
    || fail "worker-2 holds no partition ${ATTACH_LIMIT_SECONDS}s after the deadline attach: $(describe "$W2_UI" "$W2_METRICS")"
ok "the deadline attached worker-2 (seen ${deadline_lag}s after until_ms, with polls of 1 s); last_request unchanged"

# --- 5. a killed detached worker leaves its slot held until the lease ends ---

step "5. operator detach of worker-2 for 300 s, then a kill"
before=$(detach_field "$W2_UI" last_request.at_ms)
post_detach "$W2_UI" 300 "harness detach before a kill"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
outcome=$(await_outcome "$W2_UI" "$before")
[ "$outcome" = '"detached"' ] || fail "the outcome of the detach of worker-2 is ${outcome}, expected \"detached\""

log "killing worker-2"
kill_at=$(epoch_now)
"${COMPOSE[@]}" kill worker-2 >/dev/null 2>&1
worker2_killed=true

before=$(detach_field "$W3_UI" last_request.at_ms)
post_detach "$W3_UI" 60 "harness detach after a kill"
outcome=$(await_outcome "$W3_UI" "$before")
refused_at=$(seconds_between "$kill_at" "$(epoch_now)")
[ "$outcome" = '"budget_full"' ] \
    || fail "the detach of worker-3 ${refused_at}s after the kill of the detached worker-2 ended ${outcome}, expected \"budget_full\" (the dead lease holds the slot)"
state_is "$W3_UI" attached || fail "worker-3 is not attached after its refused detach: $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 refused with budget_full ${refused_at}s after the kill"

log "starting worker-2"
# docker start, not docker compose start: compose would first run the
# one-shot dependencies of the worker again, which takes half a minute.
docker start "$(container_id worker-2)" >/dev/null
worker2_killed=false
attached_with_partitions_after_start() {
    state_is "$W2_UI" attached && holds_some "$W2_METRICS"
}
wait_until "$RESTART_LIMIT_SECONDS" attached_with_partitions_after_start \
    || fail "worker-2 is not attached with a partition ${RESTART_LIMIT_SECONDS}s after its start: $(describe "$W2_UI" "$W2_METRICS")"
restart_seconds=$(seconds_between "$kill_at" "$(epoch_now)")
ok "worker-2 started again: attached and holds $(held_partitions "$W2_METRICS") partitions ${restart_seconds}s after the kill"

# Try the detach of worker-3 again until the dead lease ends.
slot_free_limit=$((LEASE_SECONDS + SLOT_FREE_GRACE_SECONDS))
outcomes=()
while true; do
    before=$(detach_field "$W3_UI" last_request.at_ms)
    post_detach "$W3_UI" 60 "harness detach after a kill"
    outcome=$(await_outcome "$W3_UI" "$before")
    tried_at=$(seconds_between "$kill_at" "$(epoch_now)")
    outcomes+=("${tried_at}s:${outcome//\"/}")
    if [ "$outcome" = '"detached"' ]; then
        break
    fi
    [ "$outcome" = '"budget_full"' ] \
        || fail "a detach of worker-3 ${tried_at}s after the kill ended ${outcome}, expected budget_full or detached (tries: ${outcomes[*]})"
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) < float(sys.argv[2]) else 1)' "$tried_at" "$slot_free_limit" \
        || fail "worker-3 could not detach ${slot_free_limit}s after the kill of worker-2: the dead lease of ${LEASE_SECONDS}s still holds the slot (tries: ${outcomes[*]})"
    sleep 3
done
earliest=$((LEASE_SECONDS - LEASE_REFRESH_SECONDS - 1))
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)' "$tried_at" "$earliest" \
    || fail "worker-3 detached ${tried_at}s after the kill, before the dead lease of ${LEASE_SECONDS}s can end (at least ${earliest}s; tries: ${outcomes[*]})"
holds_none "$W3_UI" "$W3_METRICS" || fail "worker-3 is detached and still holds partitions: $(describe "$W3_UI" "$W3_METRICS")"
slot_free_seconds=$tried_at
ok "the slot was free for worker-3 ${slot_free_seconds}s after the kill (tries: ${outcomes[*]})"

post_attach "$W3_UI"
attached_with_partitions_w3() {
    state_is "$W3_UI" attached && holds_some "$W3_METRICS"
}
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" attached_with_partitions_w3 \
    || fail "worker-3 is not attached with a partition ${ATTACH_LIMIT_SECONDS}s after the attach: $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 attached again"

# --- 6. a handler detach (the example of integration/worker/handler.py) -----

step "6. handler detach of worker-3, asked for with a file"
since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
touch "$REQUEST_FILE"
logged_detached() {
    logged_with worker-3 harness_detach_result "$since" detached true
}
POLL_SECONDS=1 wait_until $((HANDLER_PICKUP_SECONDS + LEAVE_LIMIT_SECONDS)) logged_detached \
    || fail "worker-3 logged no harness_detach_result with detached true since ${since} (lines: $(log_events worker-3 "$since" harness_detach_result | tr '\n' ' '); request file left: $([ -e "$REQUEST_FILE" ] && echo yes || echo no))"
[ ! -e "$REQUEST_FILE" ] || fail "worker-3 detached but the request file ${REQUEST_FILE} is still there"
state_is "$W3_UI" detached || fail "worker-3 logged a handler detach, but its state is not detached: $(describe "$W3_UI" "$W3_METRICS")"
[ "$(detach_field "$W3_UI" origin)" = '"handler"' ] || fail "worker-3 is detached with origin $(detach_field "$W3_UI" origin), expected \"handler\""
handler_until=$(detach_field "$W3_UI" until_ms)
POLL_SECONDS=1 wait_until $((HANDLER_DETACH_SECONDS + ATTACH_LIMIT_SECONDS)) state_is "$W3_UI" attached \
    || fail "worker-3 is not attached $((HANDLER_DETACH_SECONDS + ATTACH_LIMIT_SECONDS))s after its handler detach: $(describe "$W3_UI" "$W3_METRICS")"
handler_lag=$(python3 -c 'import sys; print(f"{float(sys.argv[1]) - int(sys.argv[2]) / 1000:.1f}")' "$(epoch_now)" "$handler_until")
wait_until 10 logged_with worker-3 worker_attached "$since" cause '"deadline"' \
    || fail "worker-3 logged no worker_attached with cause deadline since ${since} (lines: $(log_events worker-3 "$since" worker_attached | tr '\n' ' '))"
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" holds_some "$W3_METRICS" \
    || fail "worker-3 holds no partition ${ATTACH_LIMIT_SECONDS}s after the deadline attach: $(describe "$W3_UI" "$W3_METRICS")"
ok "the handler detach of worker-3 ended by its deadline (seen ${handler_lag}s after until_ms)"

pass "leave ${leave_seconds}s with ${moved} partitions moved, others held all ${TOPIC_PARTITIONS} after ${takeover_seconds}s; budget_full while one is away; attach back with partitions after ${return_seconds}s; deadline attach seen ${deadline_lag}s after until; slot of a killed worker free ${slot_free_seconds}s after the kill; handler detach ended by its deadline"
