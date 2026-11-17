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
# executor.drain_timeout and the lease are read from worker-2's config
# reference (/api/v1/config-reference). The checks, each a FAIL that names
# what it saw:
#   1. An operator detach of worker-2 for 60 s: within drain_timeout + 15 s
#      its state is `detached`, it holds no partition, /readyz answers 200,
#      worker-1 and worker-3 together hold every partition of the topic, and
#      worker-1 lists worker-2 with `detached: true`.
#   2. An operator detach of worker-3 while worker-2 is detached: the
#      outcome is `budget_full`, and worker-3 stays attached with the same
#      number of partitions.
#   3. An operator attach of worker-2: within 30 s its state is `attached`
#      and it holds at least one partition.
#   4. An operator detach of worker-2 for 15 s: the deadline attaches it
#      (state `attached` again), `last_request` is the one of the detach,
#      and worker-2 logged `worker_attached` with cause `deadline`.
#   5. An operator detach of worker-2 for 300 s, then a kill of worker-2
#      (SIGKILL): its slot stays held until the lease ends. A detach of
#      worker-3 right after the kill is `budget_full`. worker-2 is started
#      again, and while it starts a detach of worker-3 is asked again every
#      few seconds (`store_unavailable` and an outcome that could not be
#      read count as a try, not a failure)
#      until it succeeds; worker-2 must be `attached` with partitions within
#      RESTART_LIMIT_SECONDS of its start. Every time is the time of the
#      ask, taken on the host right before the POST, from the kill: the ask
#      that succeeds comes no earlier than the lease minus one lease refresh
#      (lease / 3) minus 2 s, and the asks stop with a FAIL after the lease
#      plus SLOT_FREE_GRACE_SECONDS. When worker-2 was back before that
#      earliest time, at least one ask after its return must be
#      `budget_full`: a restarted worker must not free its old slot.
#      worker-3 is then attached by the operator.
#   6. A handler detach, the example task of the harness worker
#      (detach_on_request): the scenario writes the file
#      detach-request-worker-3 into the shared folder (inside the worker-3
#      container); worker-3 logs harness_detach_result with detached true
#      and removes the file, its state is `detached` with origin `handler`,
#      and about 20 s later the deadline attaches it (`worker_attached` with
#      cause `deadline` in its log).
# A POST that answers 503 (the worker's main loop did not answer in time)
# is asked once more unless the GET shows that it was taken; any other
# answer than 202 is a FAIL that shows the body.
# The scenario starts and ends with the three workers attached, so it can
# run twice in a row. On a failure it starts worker-2 again when it is
# killed, removes the request file and asks worker-2 and worker-3 to attach.
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

# The length of a handler detach: the detach length of the harness worker's example task.
HANDLER_DETACH_SECONDS=20
# The request file of detach_on_request on worker-3, at its path inside the
# container (storage.shared_dir of worker/drakkar.yaml is /shared).
REQUEST_FILE=/shared/detach-request-worker-3

# Limits.
ATTACH_LIMIT_SECONDS=30
DEADLINE_DETACH_SECONDS=15
RESTART_LIMIT_SECONDS=60
SLOT_FREE_GRACE_SECONDS=20
HANDLER_PICKUP_SECONDS=5
START_LIMIT_SECONDS=60
# Seconds between two asks of the check 5 loop.
RETRY_SECONDS=3

# --- helpers ----------------------------------------------------------------
#
# No helper below calls `fail`: a helper whose output is captured with $(...)
# runs in a subshell, and a `fail` there would print its message and the
# verdict into the captured text. The callers call `fail`.

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

# A config value of the worker on host port $1 in whole seconds, read from
# its config reference: config_seconds <port> <path>. The value is a number
# of seconds or a duration text (30s, 2h45m, 500ms). Prints nothing and
# returns 1 when the value cannot be read.
config_seconds() {
    local body
    body=$(curl -sf --connect-timeout 2 --max-time 5 "http://localhost:$1/api/v1/config-reference") || return 1
    CONFIG_PATH="$2" python3 -c '
import json, os, re, sys
UNITS = {"d": 86400, "h": 3600, "m": 60, "s": 1, "ms": 0.001}
for group in json.load(sys.stdin)["groups"]:
    for entry in group["entries"]:
        if entry["path"] != os.environ["CONFIG_PATH"]:
            continue
        value = entry["value"]
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            print(int(value))
            sys.exit(0)
        if isinstance(value, str):
            terms = re.findall(r"(\d+(?:\.\d+)?)(ms|d|h|m|s)", value)
            if terms and "".join(number + unit for number, unit in terms) == value:
                print(int(sum(float(number) * UNITS[unit] for number, unit in terms)))
                sys.exit(0)
        sys.exit(1)
sys.exit(1)
' <<<"$body"
}

# Ask a detach route of the worker on host port $1 and fail unless the
# request is taken:
#   ask_detach <port> <seconds> <reason>
#   ask_attach <port>
# Sets ASK_BEFORE (the at_ms of last_request before the ask, for
# await_outcome) and ASK_AT (the host time right before the POST that was
# taken). A 503 means the main loop did not answer in time: when the GET
# then shows a newer request, or the state `detaching` or `attaching` (a
# transition runs, and last_request is written only when it ends), the ask
# was taken; else it is sent once more.
ask_detach() {
    ask_route "$1" detach "{\"duration_seconds\": $2, \"reason\": \"$3\"}"
}
ask_attach() {
    ask_route "$1" attach '{}'
}
ask_route() {
    local port=$1 route=$2 body=$3 answer code try
    ASK_BEFORE=$(detach_field "$port" last_request.at_ms)
    [ "$ASK_BEFORE" != unreachable ] \
        || fail "the detach state of the worker on port ${port} could not be read before POST /api/v1/debug/${route}"
    for try in 1 2; do
        ASK_AT=$(epoch_now)
        # curl's long options take no `=`, so they keep the space form here.
        answer=$(curl -s -w '\n%{http_code}' --connect-timeout 2 --max-time 10 -X POST \
            -H 'Content-Type: application/json' -d "$body" "http://localhost:${port}/api/v1/debug/${route}") \
            || answer=$'\nnone'
        code=${answer##*$'\n'}
        case "$code" in
            202) return 0 ;;
            503)
                warn "POST /api/v1/debug/${route} on port ${port} answered 503 (try ${try}): ${answer%$'\n'*}"
                sleep 1
                if request_newer_than "$port" "$ASK_BEFORE" || transition_runs "$port"; then
                    return 0
                fi
                ;;
            *) fail "POST /api/v1/debug/${route} on port ${port} answered ${code}, expected 202: ${answer%$'\n'*}" ;;
        esac
    done
    fail "POST /api/v1/debug/${route} on port ${port} answered 503 twice, and the state shows no new request (last_request: $(detach_field "$port" last_request))"
}

# True when the worker on host port $1 is in the state `detaching` or
# `attaching`: a request was taken and its transition still runs.
transition_runs() {
    local state
    state=$(detach_field "$1" state)
    [ "$state" = '"detaching"' ] || [ "$state" = '"attaching"' ]
}

# True when the last request of the worker on host port $1 is newer than
# the at_ms value $2 (null before the first request). The caller makes sure
# that $2 is not `unreachable`.
request_newer_than() {
    local at
    at=$(detach_field "$1" last_request.at_ms)
    [ "$at" != null ] && [ "$at" != unreachable ] && { [ "$2" = null ] || [ "$at" -gt "$2" ]; }
}

# Wait up to $3 seconds until the last request of the worker on host port
# $1 is newer than the at_ms value $2, and print its outcome. Returns 1
# when no newer request came; the caller fails with the state it reads.
#   outcome=$(await_outcome <port> "$ASK_BEFORE" <seconds>) || fail "..."
await_outcome() {
    POLL_SECONDS=1 wait_until "$3" request_newer_than "$1" "$2" || return 1
    detach_field "$1" last_request.outcome
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

# The seconds from the deadline until_ms $2 to the epoch time $1, to 0.1 s;
# `unknown` when $2 is not a number of milliseconds.
seconds_after_until() {
    if [[ "$2" =~ ^[0-9]+$ ]]; then
        python3 -c 'import sys; print(f"{float(sys.argv[1]) - int(sys.argv[2]) / 1000:.1f}")' "$1" "$2"
    else
        echo unknown
    fi
}

# True when the epoch time $1 is at least $2 seconds after the epoch time $3.
#   at_least_after <epoch> <seconds> <epoch>
at_least_after() {
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) - float(sys.argv[3]) >= float(sys.argv[2]) else 1)' "$1" "$2" "$3"
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

# A short description of the detach state and the partitions of a worker,
# for failure messages:  describe <ui port> <metrics port>
describe() {
    echo "state $(detach_field "$1" state), origin $(detach_field "$1" origin), last_request $(detach_field "$1" last_request), gauge $(held_partitions "$2"), live rows $(live_partition_rows "$1")"
}

# The request file is written, read and removed inside the worker-3
# container: the Docker VM of a macOS host can see a change of the host
# folder late, in both directions.
request_file() {
    "${COMPOSE[@]}" exec -T worker-3 "$@" "$REQUEST_FILE"
}
request_file_exists() {
    request_file test -e
}

worker2_killed=false
# On exit: start worker-2 again when the scenario killed it, remove the
# request file and ask worker-2 and worker-3 to attach, so a failed run
# leaves no worker away.
clean_up() {
    if [ "$worker2_killed" = true ]; then
        docker start "$(container_id worker-2)" >/dev/null 2>&1 || true
    fi
    request_file rm -f >/dev/null 2>&1 || true
    for port in "$W2_UI" "$W3_UI"; do
        curl -s -o /dev/null --connect-timeout 2 --max-time 5 -X POST "http://localhost:${port}/api/v1/debug/attach" || true
    done
}
trap clean_up EXIT

# --- start ------------------------------------------------------------------

step "Scenario: detach and attach of the main-cluster workers"

request_file rm -f || fail "the request file ${REQUEST_FILE} could not be removed in the worker-3 container"
TOPIC_PARTITIONS=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 \
    --describe --topic="$TOPIC" 2>/dev/null | grep -c 'Partition: ' || true)
[ "$TOPIC_PARTITIONS" -ge 3 ] || fail "the topic ${TOPIC} has ${TOPIC_PARTITIONS} partitions by kafka-topics.sh, expected at least 3"
log "the topic ${TOPIC} has ${TOPIC_PARTITIONS} partitions"

DRAIN_TIMEOUT_SECONDS=$(config_seconds "$W2_UI" executor.drain_timeout) \
    || fail "executor.drain_timeout could not be read from the config reference of worker-2"
LEASE_SECONDS=$(config_seconds "$W2_UI" sources.kafka.detach.lease) \
    || fail "sources.kafka.detach.lease could not be read from the config reference of worker-2"
# The product bounds a leave by drain_timeout + 10 s; the slot acquire
# through the store comes on top.
LEAVE_LIMIT_SECONDS=$((DRAIN_TIMEOUT_SECONDS + 15))
# A successful detach records its outcome only when its leave has ended.
OUTCOME_LIMIT_SECONDS=$LEAVE_LIMIT_SECONDS
# The slot lease is refreshed every lease / 3 seconds, so a killed holder
# refreshed it at most this long before the kill, and its lease ends no
# earlier than the lease minus this after the kill. The earliest time an ask
# may win the slot keeps a margin of EARLIEST_MARGIN_SECONDS, because ASK_AT
# is taken before the POST and the store call comes after it.
EARLIEST_MARGIN_SECONDS=2
EARLIEST_SLOT_SECONDS=$(python3 -c 'import sys; lease = float(sys.argv[1]); print(f"{lease - lease / 3 - float(sys.argv[2]):.2f}")' \
    "$LEASE_SECONDS" "$EARLIEST_MARGIN_SECONDS")
log "worker-2: executor.drain_timeout ${DRAIN_TIMEOUT_SECONDS}s, sources.kafka.detach.lease ${LEASE_SECONDS}s"

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
ask_detach "$W2_UI" 60 "harness detach"
start_at=$ASK_AT
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
leave_seconds=$(seconds_between "$start_at" "$(epoch_now)")
outcome=$(await_outcome "$W2_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
    || fail "worker-2 recorded no outcome of its detach within ${OUTCOME_LIMIT_SECONDS}s: $(describe "$W2_UI" "$W2_METRICS")"
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
[ "$held3" != unreachable ] || fail "the metrics page of worker-3 could not be read before its detach"
ask_detach "$W3_UI" 60 "harness second detach"
outcome=$(await_outcome "$W3_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
    || fail "worker-3 recorded no outcome of its detach within ${OUTCOME_LIMIT_SECONDS}s: $(describe "$W3_UI" "$W3_METRICS")"
[ "$outcome" = '"budget_full"' ] || fail "the detach of worker-3 while worker-2 is detached ended ${outcome}, expected \"budget_full\""
state_is "$W3_UI" attached || fail "worker-3 is not attached after its refused detach: $(describe "$W3_UI" "$W3_METRICS")"
[ "$(held_partitions "$W3_METRICS")" = "$held3" ] \
    || fail "worker-3 held ${held3} partitions before its refused detach and $(held_partitions "$W3_METRICS") after it: $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 was refused with budget_full and still holds its ${held3} partitions"

# --- 3. operator attach of worker-2 -----------------------------------------

step "3. operator attach of worker-2"
ask_attach "$W2_UI"
start_at=$ASK_AT
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
ask_detach "$W2_UI" "$DEADLINE_DETACH_SECONDS" "harness deadline"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
outcome=$(await_outcome "$W2_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
    || fail "worker-2 recorded no outcome of its detach within ${OUTCOME_LIMIT_SECONDS}s: $(describe "$W2_UI" "$W2_METRICS")"
[ "$outcome" = '"detached"' ] || fail "the outcome of the detach of worker-2 is ${outcome}, expected \"detached\""
detach_request=$(detach_field "$W2_UI" last_request)
until_ms=$(detach_field "$W2_UI" until_ms)
[[ "$until_ms" =~ ^[0-9]+$ ]] || fail "worker-2 is detached with until_ms ${until_ms}, expected a deadline in milliseconds"
POLL_SECONDS=1 wait_until $((DEADLINE_DETACH_SECONDS + ATTACH_LIMIT_SECONDS)) state_is "$W2_UI" attached \
    || fail "worker-2 is not attached $((DEADLINE_DETACH_SECONDS + ATTACH_LIMIT_SECONDS))s after its ${DEADLINE_DETACH_SECONDS}s detach: $(describe "$W2_UI" "$W2_METRICS")"
deadline_lag=$(seconds_after_until "$(epoch_now)" "$until_ms")
[ "$(detach_field "$W2_UI" last_request)" = "$detach_request" ] \
    || fail "the deadline changed last_request of worker-2 from ${detach_request} to $(detach_field "$W2_UI" last_request)"
wait_until 10 logged_with worker-2 worker_attached "$since" cause '"deadline"' \
    || fail "worker-2 logged no worker_attached with cause deadline since ${since} (lines: $(log_events worker-2 "$since" worker_attached | tr '\n' ' '))"
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" holds_some "$W2_METRICS" \
    || fail "worker-2 holds no partition ${ATTACH_LIMIT_SECONDS}s after the deadline attach: $(describe "$W2_UI" "$W2_METRICS")"
ok "the deadline attached worker-2 (seen ${deadline_lag}s after until_ms, with polls of 1 s); last_request unchanged"

# --- 5. a killed detached worker leaves its slot held until the lease ends ---

step "5. operator detach of worker-2 for 300 s, then a kill"
ask_detach "$W2_UI" 300 "harness detach before a kill"
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W2_UI" detached \
    || fail "worker-2 is not detached ${LEAVE_LIMIT_SECONDS}s after the request: $(describe "$W2_UI" "$W2_METRICS")"
outcome=$(await_outcome "$W2_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
    || fail "worker-2 recorded no outcome of its detach within ${OUTCOME_LIMIT_SECONDS}s: $(describe "$W2_UI" "$W2_METRICS")"
[ "$outcome" = '"detached"' ] || fail "the outcome of the detach of worker-2 is ${outcome}, expected \"detached\""

log "killing worker-2"
kill_at=$(epoch_now)
"${COMPOSE[@]}" kill worker-2 >/dev/null 2>&1 || fail "docker compose kill worker-2 failed"
worker2_killed=true

ask_detach "$W3_UI" 60 "harness detach after a kill"
asked=$(seconds_between "$kill_at" "$ASK_AT")
outcome=$(await_outcome "$W3_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
    || fail "worker-3 recorded no outcome of the detach asked ${asked}s after the kill within ${OUTCOME_LIMIT_SECONDS}s: $(describe "$W3_UI" "$W3_METRICS")"
[ "$outcome" = '"budget_full"' ] \
    || fail "the detach of worker-3 asked ${asked}s after the kill of the detached worker-2 ended ${outcome}, expected \"budget_full\" (the dead lease holds the slot)"
state_is "$W3_UI" attached || fail "worker-3 is not attached after its refused detach: $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 refused with budget_full for the ask ${asked}s after the kill"

log "starting worker-2"
# docker start, not docker compose start: compose would first run the
# one-shot dependencies of the worker again, which takes half a minute.
worker2_container=$(container_id worker-2) || fail "the container of worker-2 could not be found to start it again"
[ -n "$worker2_container" ] || fail "the container of worker-2 could not be found to start it again (empty id)"
docker start "$worker2_container" >/dev/null || fail "docker start of the container ${worker2_container} of worker-2 failed"
worker2_killed=false
started_at=$SECONDS
attached_with_partitions_after_start() {
    state_is "$W2_UI" attached && holds_some "$W2_METRICS"
}

# Ask the detach of worker-3 again until the dead lease ends, and watch for
# worker-2 to come back in the same loop, so the time of the slot does not
# depend on how long the start takes. Every time is the time of the ask from
# the kill: an outcome is recorded only when the leave of a successful
# detach has ended, so the time of the answer would add the leave to it.
earliest=$EARLIEST_SLOT_SECONDS
slot_free_limit=$((LEASE_SECONDS + SLOT_FREE_GRACE_SECONDS))
outcomes=()
back_at=''
full_after_back=0
budget_full=0
store_unavailable=0
unreadable=0
while true; do
    if [ -z "$back_at" ] && attached_with_partitions_after_start; then
        back_at=$(epoch_now)
        outcomes+=("back@$(seconds_between "$kill_at" "$back_at")s")
    fi
    ask_detach "$W3_UI" 60 "harness detach after a kill"
    asked=$(seconds_between "$kill_at" "$ASK_AT")
    outcome=$(await_outcome "$W3_UI" "$ASK_BEFORE" "$OUTCOME_LIMIT_SECONDS") \
        || fail "worker-3 recorded no outcome of the detach asked ${asked}s after the kill within ${OUTCOME_LIMIT_SECONDS}s (asks: ${outcomes[*]-none}): $(describe "$W3_UI" "$W3_METRICS")"
    outcomes+=("${asked}s:${outcome//\"/}")
    case "$outcome" in
        '"detached"') break ;;
        '"budget_full"')
            budget_full=$((budget_full + 1))
            [ -z "$back_at" ] || full_after_back=$((full_after_back + 1))
            ;;
        '"store_unavailable"') store_unavailable=$((store_unavailable + 1)) ;;
        # The outcome could not be read this time: a try, not a failure.
        unreachable) unreadable=$((unreadable + 1)) ;;
        *) fail "the detach of worker-3 asked ${asked}s after the kill ended ${outcome}, expected budget_full, store_unavailable or detached (asks: ${outcomes[*]})" ;;
    esac
    if at_least_after "$ASK_AT" "$slot_free_limit" "$kill_at"; then
        counts="${budget_full} budget_full, ${store_unavailable} store_unavailable, ${unreadable} unreadable outcomes"
        if [ "$budget_full" -gt 0 ]; then
            fail "worker-3 could not detach ${slot_free_limit}s after the kill of worker-2 (${counts}): the dead lease of ${LEASE_SECONDS}s still holds the slot (asks: ${outcomes[*]})"
        fi
        fail "worker-3 could not detach ${slot_free_limit}s after the kill of worker-2 (${counts}): no ask was answered budget_full, so the store or the read of the outcome failed on every ask (asks: ${outcomes[*]})"
    fi
    sleep "$RETRY_SECONDS"
done
slot_free_seconds=$asked
at_least_after "$ASK_AT" "$earliest" "$kill_at" \
    || fail "worker-3 detached on the ask ${slot_free_seconds}s after the kill, before the dead lease of ${LEASE_SECONDS}s can end (not before ${earliest}s; asks: ${outcomes[*]})"
if [ -n "$back_at" ] && ! at_least_after "$back_at" "$earliest" "$kill_at" && [ "$full_after_back" -eq 0 ]; then
    fail "worker-2 was back $(seconds_between "$kill_at" "$back_at")s after the kill, before the dead lease can end (${earliest}s), and no ask after its return was budget_full: the restarted worker freed its old slot (asks: ${outcomes[*]})"
fi
POLL_SECONDS=1 wait_until "$LEAVE_LIMIT_SECONDS" state_is "$W3_UI" detached \
    || fail "worker-3 has the outcome detached but its state is not detached: $(describe "$W3_UI" "$W3_METRICS")"
holds_none "$W3_UI" "$W3_METRICS" || fail "worker-3 is detached and still holds partitions: $(describe "$W3_UI" "$W3_METRICS")"

# worker-2 may still be starting; it must be back within its limit.
if [ -z "$back_at" ]; then
    restart_left=$((RESTART_LIMIT_SECONDS - (SECONDS - started_at)))
    [ "$restart_left" -ge 1 ] || restart_left=1
    wait_until "$restart_left" attached_with_partitions_after_start \
        || fail "worker-2 is not attached with a partition ${RESTART_LIMIT_SECONDS}s after its start: $(describe "$W2_UI" "$W2_METRICS")"
    back_at=$(epoch_now)
fi
back_seconds=$(seconds_between "$kill_at" "$back_at")
ok "worker-2 started again: attached and holds $(held_partitions "$W2_METRICS") partitions, seen ${back_seconds}s after the kill"
ok "the slot was free for worker-3 on the ask ${slot_free_seconds}s after the kill, not before ${earliest}s (worker-2 back at ${back_seconds}s; ${full_after_back} budget_full after its return; ${store_unavailable} store_unavailable and ${unreadable} unreadable outcomes in all; asks: ${outcomes[*]})"

ask_attach "$W3_UI"
attached_with_partitions_w3() {
    state_is "$W3_UI" attached && holds_some "$W3_METRICS"
}
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" attached_with_partitions_w3 \
    || fail "worker-3 is not attached with a partition ${ATTACH_LIMIT_SECONDS}s after the attach: $(describe "$W3_UI" "$W3_METRICS")"
ok "worker-3 attached again"

# --- 6. a handler detach (the example task of the harness worker) -----------

step "6. handler detach of worker-3, asked for with a file"
since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
request_file touch || fail "the request file ${REQUEST_FILE} could not be written in the worker-3 container"
logged_detached() {
    logged_with worker-3 harness_detach_result "$since" detached true
}
POLL_SECONDS=1 wait_until $((HANDLER_PICKUP_SECONDS + LEAVE_LIMIT_SECONDS)) logged_detached \
    || fail "worker-3 logged no harness_detach_result with detached true since ${since} (lines: $(log_events worker-3 "$since" harness_detach_result | tr '\n' ' '); request file left: $(request_file_exists && echo yes || echo no))"
if request_file_exists; then
    fail "worker-3 detached but the request file ${REQUEST_FILE} is still there"
fi
state_is "$W3_UI" detached || fail "worker-3 logged a handler detach, but its state is not detached: $(describe "$W3_UI" "$W3_METRICS")"
[ "$(detach_field "$W3_UI" origin)" = '"handler"' ] || fail "worker-3 is detached with origin $(detach_field "$W3_UI" origin), expected \"handler\""
handler_until=$(detach_field "$W3_UI" until_ms)
[[ "$handler_until" =~ ^[0-9]+$ ]] || fail "worker-3 is detached with until_ms ${handler_until}, expected a deadline in milliseconds"
POLL_SECONDS=1 wait_until $((HANDLER_DETACH_SECONDS + ATTACH_LIMIT_SECONDS)) state_is "$W3_UI" attached \
    || fail "worker-3 is not attached $((HANDLER_DETACH_SECONDS + ATTACH_LIMIT_SECONDS))s after its handler detach: $(describe "$W3_UI" "$W3_METRICS")"
handler_lag=$(seconds_after_until "$(epoch_now)" "$handler_until")
wait_until 10 logged_with worker-3 worker_attached "$since" cause '"deadline"' \
    || fail "worker-3 logged no worker_attached with cause deadline since ${since} (lines: $(log_events worker-3 "$since" worker_attached | tr '\n' ' '))"
POLL_SECONDS=1 wait_until "$ATTACH_LIMIT_SECONDS" holds_some "$W3_METRICS" \
    || fail "worker-3 holds no partition ${ATTACH_LIMIT_SECONDS}s after the deadline attach: $(describe "$W3_UI" "$W3_METRICS")"
ok "the handler detach of worker-3 ended by its deadline (seen ${handler_lag}s after until_ms)"

pass "leave ${leave_seconds}s with ${moved} partitions moved, others held all ${TOPIC_PARTITIONS} after ${takeover_seconds}s; budget_full while one is away; attach back with partitions after ${return_seconds}s; deadline attach seen ${deadline_lag}s after until; slot of a killed worker free on the ask ${slot_free_seconds}s after the kill (worker-2 back at ${back_seconds}s, ${full_after_back} budget_full after it, ${store_unavailable} store_unavailable and ${unreadable} unreadable outcomes in all); handler detach ended by its deadline"
