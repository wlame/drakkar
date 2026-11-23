#!/usr/bin/env bash
# ============================================================================
# Scenario: split jobs that arrive over HTTP, against a real broker. A POST on
# the HTTP source of a split worker stores the request as one message of the
# source topic split-requests and answers 202 with its id; from there the
# message is an ordinary split job. Every split worker follows the status
# topic split-requests_status and answers a GET with ?request_id= from
# memory: accepted, running (with part counts), then done (with the result
# of the handler's http_job_result) or failed (with an outcome).
#
# The compose services split-worker-1..3 (profile split) run the handler of
# integration/split-worker/ (see scenario-split.sh for its rules: more than
# 20 items is split into parts of 5, each item is one task). Their HTTP
# source serves /jobs on port 8090, published as 8120, 8121 and 8122, with
# two clients, tenant-a and tenant-b. The handler's request_id hook takes
# the x-request-id header, so the client names the job; http_job_result
# answers {"items": <items counted from job.results()>, "parts": <parts>}
# ("parts" only for a job with parts). sources.http.deliver_to_sinks is off,
# so an HTTP job writes no record to split-items or split-summaries. The
# owner of a job is the worker that logged split_job_opened with the
# request id.
#
# Before each check the three workers answer /readyz with 200, their HTTP
# sources answer a GET (not 503), and the source lag of the group
# drakkar-split is 0; after each check a killed worker is started again.
# Topics are read with the rules of scenario-split.sh (every read covers
# every record the topic had when it began; integration/split_check.py).
# Each check prints one line `CHECK <n> <name>: PASS|FAIL: ...`.
#   1. a large request (60 items, 12 parts) by POST on split-worker-1 with
#      x-request-id: 202 with status accepted; a GET on split-worker-2 shows
#      running with parts.received growing between two reads, then done
#      within 5 minutes with result.items 60 and result.parts 12; no record
#      with its key on split-items or split-summaries.
#   2. a small request (10 items) stays local: it ends done with result.items
#      10 and no parts in the status body; no record with its key on the
#      parts topic.
#   3. a second POST with the id of check 1 (on split-worker-3) answers 200
#      with the status body of the job (done); the source topic has exactly
#      one record with that key, with the headers dk-origin http,
#      dk-request-id, dk-http-client tenant-a and dk-deliver-sinks false, in
#      this order and no other; no worker logged split_job_opened twice.
#   4. a large request; its owner is killed (SIGKILL) during the job and
#      started again; the job ends done with result.items 60.
#   5. a request with one item of mode fail under fail_policy dlq ends failed
#      with outcome dlq; split-requests_dlq has exactly one record with its
#      key whose value equals the posted body byte for byte, and no header
#      of the DLQ record carries a client token.
#   6. a GET of an unknown id answers 404 (after the 1-second wait); a GET of
#      the check-1 id with the tenant-b token answers 404; a POST of that id
#      by tenant-b answers 409 request_id_in_use.
#
# Runs with every provider; the split workers use their own filesystem
# autodiscover in a volume of their own.
#
# Usage: HARNESS_PROVIDER=filesystem integration/scenario-split-http.sh
#        SPLIT_HTTP_CHECKS="1 3" integration/scenario-split-http.sh  (only these;
#        check 3 and check 6 use the id of check 1)
#        SPLIT_BUILD=0 integration/scenario-split-http.sh  (use the image
#        drakkar-harness-split-worker:local as it is)
#
# The scenario removes the split worker containers when it ends.
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
# A failed check must not end the script before the checks after it ran, so
# -e is off.
set -uo pipefail

SCENARIO=scenario-split-http
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

SPLIT=("${COMPOSE[@]}" --profile=split)
SPLIT_SERVICES=(split-worker-1 split-worker-2 split-worker-3)
SOURCE_TOPIC=split-requests
SOURCE_GROUP=drakkar-split
PARTS_TOPIC=split-requests_drakkar-split_split_parts
ITEMS_TOPIC=split-items
SUMMARIES_TOPIC=split-summaries
DLQ_TOPIC=split-requests_dlq
JOBS_PATH=/jobs
# The bearer tokens of the two clients of integration/split-worker/drakkar.yaml.
TOKEN_A=integration-split-tenant-a-token-do-not-use-in-prod
TOKEN_B=integration-split-tenant-b-token-do-not-use-in-prod
# Longer than executor.drain_timeout (30 s) of the split workers.
STOP_TIMEOUT_SECONDS=45
SPLIT_CHECK="$(dirname "${BASH_SOURCE[0]}")/split_check.py"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/scenario-split-http.XXXXXX")
READ_ERROR_FILE="${TMP_DIR}/read.err"
LARGE_ITEMS=60
LARGE_PARTS=12
SMALL_ITEMS=10
READY_LIMIT_SECONDS=240
LAG_LIMIT_SECONDS=240
OPENED_LIMIT_SECONDS=90
END_LIMIT_SECONDS=300
CONSUMER_IDLE_MS=6000
RUN_TOKEN="$(date +%s)-$$"
SPLIT_HTTP_CHECKS="${SPLIT_HTTP_CHECKS:-}"
SPLIT_BUILD="${SPLIT_BUILD:-1}"
# The id of the job of check 1; checks 3 and 6 use it.
LARGE_RID="http-large-${RUN_TOKEN}"
PASSED=0
FAILED=0
FAILED_CHECKS=""
CHECK_LABEL=""
CHECK_VERDICT=""

# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------

api_port() {
    case "$1" in
        split-worker-1) echo 8110 ;;
        split-worker-2) echo 8111 ;;
        split-worker-3) echo 8112 ;;
    esac
}

http_port() {
    case "$1" in
        split-worker-1) echo 8120 ;;
        split-worker-2) echo 8121 ;;
        split-worker-3) echo 8122 ;;
    esac
}

worker_ready() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:$(api_port "$1")/readyz"
}

# The HTTP source of a worker answers a status read with something other
# than 503 (its status follower has restored).
http_ready() {
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 5 \
        -H "Authorization: Bearer ${TOKEN_A}" \
        "http://localhost:$(http_port "$1")${JOBS_PATH}?request_id=ready-probe") || return 1
    [ "$code" != 503 ] && [ "$code" != 000 ]
}

# The split workers that are not ready (/readyz or the HTTP source), on one line.
unready_workers() {
    local service out=""
    for service in "${SPLIT_SERVICES[@]}"; do
        { worker_ready "$service" && http_ready "$service"; } || out="${out} ${service}"
    done
    echo "${out# }"
}

all_ready() {
    [ -z "$(unready_workers)" ]
}

container_running() {
    local id
    id=$(container_id "$1")
    [ -n "$id" ] && [ "$(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null)" = true ]
}

# The last lines of a worker's log, on one line.
worker_log_tail() {
    "${SPLIT[@]}" logs --no-color --no-log-prefix --tail=15 "$1" 2>&1 | tr '\n' ' ' | cut -c1-1500
}

start_workers() {
    env -u SPLIT_JOB_TIMEOUT "${SPLIT[@]}" up --detach --no-build --force-recreate \
        --timeout="$STOP_TIMEOUT_SECONDS" "${SPLIT_SERVICES[@]}" >/dev/null 2>&1
}

# Start again every split worker that is not running.
restore_state() {
    local service
    for service in "${SPLIT_SERVICES[@]}"; do
        if ! container_running "$service"; then
            log "starting ${service} again"
            "${SPLIT[@]}" start "$service" >/dev/null 2>&1 || warn "cannot start ${service}"
        fi
    done
}

cleanup() {
    "${SPLIT[@]}" stop --timeout="$STOP_TIMEOUT_SECONDS" "${SPLIT_SERVICES[@]}" >/dev/null 2>&1 || true
    "${SPLIT[@]}" rm --force "${SPLIT_SERVICES[@]}" >/dev/null 2>&1 || true
}

remove_tmp_dir() {
    cleanup
    rm -rf "$TMP_DIR"
}
trap remove_tmp_dir EXIT

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

# The JSON of one request (the rules of request_json in scenario-split.sh):
#   request_json <id> <items> <sleep ms> [<split pause ms> <fail policy> <special index> <special mode>]
request_json() {
    python3 -c '
import json, sys
rid, count, sleep_ms = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
pause = int(sys.argv[4]) if len(sys.argv) > 4 else 0
policy = sys.argv[5] if len(sys.argv) > 5 else "skip"
special = int(sys.argv[6]) if len(sys.argv) > 6 else -1
mode = sys.argv[7] if len(sys.argv) > 7 else "ok"
items = [{"id": f"item-{n}", "sleep_ms": sleep_ms, "mode": mode if n == special else "ok"} for n in range(count)]
print(json.dumps({"request_id": rid, "items": items, "split_pause_ms": pause, "fail_policy": policy}))
' "$@"
}

# POST the body in file $4 to the HTTP source of worker $1 with token $2 and
# the header x-request-id $3. Writes the answer body to $TMP_DIR/answer.json
# and prints the HTTP status (000 when no answer came).
#   post_job <service> <token> <request id> <body file>
post_job() {
    curl -s -o "$TMP_DIR/answer.json" -w '%{http_code}' --connect-timeout 2 --max-time 40 -X POST \
        -H "Authorization: Bearer $2" -H "x-request-id: $3" -H 'Content-Type: application/json' \
        --data-binary "@$4" "http://localhost:$(http_port "$1")${JOBS_PATH}" || true
}

# GET the status of request $3 on worker $1 with token $2. Writes the
# answer body to $TMP_DIR/answer.json and prints the HTTP status.
#   get_job <service> <token> <request id>
get_job() {
    curl -s -o "$TMP_DIR/answer.json" -w '%{http_code}' --connect-timeout 2 --max-time 10 \
        -H "Authorization: Bearer $2" \
        "http://localhost:$(http_port "$1")${JOBS_PATH}?request_id=$3" || true
}

# The last answer body, on one line (cut).
answer_text() {
    tr '\n' ' ' <"$TMP_DIR/answer.json" 2>/dev/null | cut -c1-600
}

# The fields of the last status body: "<status> <received> <total> <failed>
# <has parts> <outcome>" (`-` for a field that is absent).
answer_fields() {
    python3 -c '
import json, sys
try:
    body = json.load(open(sys.argv[1]))
except Exception:
    print("unreadable - - - - -")
    sys.exit(0)
parts = body.get("parts")
def text(value):
    return "-" if value is None else str(value)
if isinstance(parts, dict):
    print(text(body.get("status")), text(parts.get("received")), text(parts.get("total")), text(parts.get("failed")), "yes", text(body.get("outcome")))
else:
    print(text(body.get("status")), "-", "-", "-", "no", text(body.get("outcome")))
' "$TMP_DIR/answer.json"
}

# One field of the last answer body, as JSON text (`absent` when missing):
#   answer_field <name>
answer_field() {
    python3 -c '
import json, sys
try:
    body = json.load(open(sys.argv[1]))
except Exception:
    print("unreadable")
    sys.exit(0)
print(json.dumps(body[sys.argv[2]], sort_keys=True) if sys.argv[2] in body else "absent")
' "$TMP_DIR/answer.json" "$1"
}

# Poll the status of request $3 on worker $1 (token $2) about once a second
# until it is done or failed, at most $4 seconds. Sets END_STATUS (the
# last status, `none` when no answer was 200), END_SECONDS, the running
# reads RUNNING_READS and their received counts RUNNING_RECEIVED (one line
# "min max"), and keeps the last answer in $TMP_DIR/answer.json. Returns 1
# when no end state came in time.
#   poll_end <service> <token> <request id> <limit seconds>
poll_end() {
    local started=$SECONDS code status received min="" max="" rest
    END_STATUS=none
    RUNNING_READS=0
    RUNNING_RECEIVED="- -"
    while [ $((SECONDS - started)) -lt "$4" ]; do
        code=$(get_job "$1" "$2" "$3")
        if [ "$code" = 200 ]; then
            read -r status received rest <<<"$(answer_fields)"
            END_STATUS=$status
            if [ "$status" = running ]; then
                RUNNING_READS=$((RUNNING_READS + 1))
                if [ "$received" != - ]; then
                    if [ -z "$min" ] || [ "$received" -lt "$min" ]; then min=$received; fi
                    if [ -z "$max" ] || [ "$received" -gt "$max" ]; then max=$received; fi
                fi
                RUNNING_RECEIVED="${min:--} ${max:--}"
            fi
            if [ "$status" = done ] || [ "$status" = failed ]; then
                END_SECONDS=$((SECONDS - started))
                return 0
            fi
        fi
        sleep 1
    done
    END_SECONDS=$((SECONDS - started))
    return 1
}

# ---------------------------------------------------------------------------
# Topics and log lines (the rules of scenario-split.sh)
# ---------------------------------------------------------------------------

kafka_tool() {
    local script=$1
    shift
    "${COMPOSE[@]}" exec -T kafka "/opt/kafka/bin/${script}" --bootstrap-server=localhost:9092 "$@"
}

split_check() {
    python3 "$SPLIT_CHECK" "$@"
}

read_error() {
    cat "$READ_ERROR_FILE" 2>/dev/null || echo "no reason recorded"
}

note_read_error() {
    echo "$*" >"$READ_ERROR_FILE"
}

topic_offsets() {
    local out
    out=$(kafka_tool kafka-get-offsets.sh --topic="$1" --time="$2" 2>"$TMP_DIR/offsets.err") \
        || { note_read_error "kafka-get-offsets.sh failed for $1: $(tail -c 300 "$TMP_DIR/offsets.err")"; return 1; }
    grep -q "^$1:" <<<"$out" || { note_read_error "kafka-get-offsets.sh printed no offset of $1: ${out:0:300}"; return 1; }
    grep "^$1:" <<<"$out"
}

# Every record of topic $1 whose key is $2, one JSON object per line; fails
# when the read did not cover the topic (see records_of in scenario-split.sh).
#   records_of <topic> <key>
records_of() {
    local topic=$1 key=$2 earliest ends total attempt raw out status
    earliest=$(topic_offsets "$topic" -2) || return 1
    ends=$(topic_offsets "$topic" -1) || return 1
    total=$(python3 -c '
import sys
def read(text):
    return {line.rsplit(":", 2)[1]: int(line.rsplit(":", 2)[2]) for line in text.split()}
low, high = read(sys.argv[1]), read(sys.argv[2])
print(sum(high[p] - low.get(p, 0) for p in high))
' "$earliest" "$ends") || { note_read_error "cannot sum the offsets of ${topic}"; return 1; }
    for attempt in 1 2 3; do
        raw=""
        if [ "$total" -gt 0 ]; then
            local limit=(--max-messages="$total" --timeout-ms=20000)
            [ "$attempt" -gt 1 ] && limit=(--timeout-ms="$CONSUMER_IDLE_MS")
            raw=$(kafka_tool kafka-console-consumer.sh --topic="$topic" --from-beginning \
                --property=print.key=true --property=print.timestamp=true \
                --property=print.partition=true --property=print.offset=true \
                --property=print.headers=true \
                --property=key.separator=$'\x1f' --property=line.separator=$'\x1e' \
                --property=headers.separator=$'\x1d' \
                "${limit[@]}" 2>"$TMP_DIR/consumer.err") \
                || { note_read_error "kafka-console-consumer.sh failed on ${topic}: $(tail -c 300 "$TMP_DIR/consumer.err")"; return 1; }
        fi
        out=$(split_check records --key="$key" --earliest="$earliest" --ends="$ends" <<<"$raw" 2>"$TMP_DIR/records.err")
        status=$?
        case "$status" in
            0)
                [ -n "$out" ] && printf '%s\n' "$out"
                return 0
                ;;
            3) ;;
            *)
                note_read_error "the records of ${topic} cannot be read: $(tail -c 300 "$TMP_DIR/records.err")"
                return 1
                ;;
        esac
    done
    note_read_error "three reads of ${topic} did not cover its ${total} records: $(tail -c 300 "$TMP_DIR/records.err")"
    return 1
}

count_records() {
    local rows
    rows=$(records_of "$1" "$2") || return 1
    grep -c . <<<"$rows" || true
}

# Fails with the reason when topic $1 has a record with key $2.
#   no_records <topic> <key>
no_records() {
    local count
    count=$(count_records "$1" "$2") || { read_error; return 1; }
    [ "$count" = 0 ] || { echo "${count} records with the key $2 on $1, expected none"; return 1; }
}

source_offsets() {
    local out rows
    out=$(kafka_tool kafka-consumer-groups.sh --describe --group="$SOURCE_GROUP" 2>"$TMP_DIR/groups.err") \
        || { note_read_error "kafka-consumer-groups.sh failed: $(tail -c 300 "$TMP_DIR/groups.err")"; return 1; }
    rows=$(awk -v topic="$SOURCE_TOPIC" '$2 == topic { print $3, $4, $5 }' <<<"$out")
    [ -n "$rows" ] || { note_read_error "kafka-consumer-groups.sh listed no partition of ${SOURCE_TOPIC}: ${out:0:300}"; return 1; }
    echo "$rows"
}

source_lag_zero() {
    local rows
    rows=$(source_offsets) || return 1
    awk '$3 > 0 && ($2 == "-" || $2 < $3) { lag = 1 } END { exit lag }' <<<"$rows"
}

# The log lines of a worker with event $2, one JSON object per line; a
# non-empty $3 must equal the request_id field.
#   log_events <service> <event> [<request id>]
log_events() {
    "${SPLIT[@]}" logs --no-color --no-log-prefix "$1" 2>/dev/null \
        | EVENT="$2" RID="${3:-}" python3 -c '
import json, os, sys
event, rid = os.environ["EVENT"], os.environ["RID"]
for line in sys.stdin:
    start = line.find("{")
    if start < 0 or event not in line:
        continue
    try:
        record = json.loads(line[start:])
    except ValueError:
        continue
    if not isinstance(record, dict) or record.get("event") != event:
        continue
    if rid and record.get("request_id") != rid:
        continue
    print(json.dumps(record))
'
}

# The first worker that logged event $1 for request $2: prints "<service>
# <job>", nothing when none did.
#   first_logged <event> <request id>
first_logged() {
    local service line
    for service in "${SPLIT_SERVICES[@]}"; do
        line=$(log_events "$service" "$1" "$2" | head -1)
        if [ -n "$line" ]; then
            python3 -c 'import json, sys; print(sys.argv[1], json.loads(sys.argv[2]).get("job"))' "$service" "$line"
            return 0
        fi
    done
}

# The workers that logged event $1 for the job prefix $2 ("<partition>:<offset>#"),
# each with the request_id field of its first such line: "<service>=<request id>"
# on one line.
#   logged_for_job <event> <job prefix>
logged_for_job() {
    local service line out=""
    for service in "${SPLIT_SERVICES[@]}"; do
        line=$(log_events "$service" "$1" | JOB="$2" python3 -c '
import json, os, sys
for line in sys.stdin:
    record = json.loads(line)
    if str(record.get("job", "")).startswith(os.environ["JOB"]):
        print(record.get("request_id", "none"))
        break
')
        [ -n "$line" ] && out="${out} ${service}=${line}"
    done
    echo "${out# }"
}

job_closed_logged() {
    [ -n "$(logged_for_job split_job_closed "$1")" ]
}

logged_anywhere() {
    [ -n "$(first_logged "$@")" ]
}

# How many log lines of event $1 for request $2 the three workers wrote.
count_logged() {
    local service total=0 n
    for service in "${SPLIT_SERVICES[@]}"; do
        n=$(log_events "$service" "$1" "$2" | grep -c . || true)
        total=$((total + n))
    done
    echo "$total"
}

# Error and warning lines of the split workers that name the split HTTP
# path or a traceback, one per line (cut), for the report of a failed check.
worker_errors() {
    local service
    for service in "${SPLIT_SERVICES[@]}"; do
        "${SPLIT[@]}" logs --no-color --no-log-prefix "$service" 2>/dev/null \
            | grep -E '"level": ?"(error|critical)"|Traceback' | tail -3 | cut -c1-400 | sed "s/^/${service}: /"
    done
}

# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

check_pass() {
    CHECK_VERDICT=pass
    echo -e "${GREEN}${BOLD}CHECK ${CHECK_LABEL}: PASS: $*${NC}"
}

cfail() {
    CHECK_VERDICT=fail
    echo -e "${RED}${BOLD}CHECK ${CHECK_LABEL}: FAIL: $*${NC}"
    local errors
    errors=$(worker_errors)
    [ -n "$errors" ] && echo "worker errors:" && echo "$errors"
    return 1
}

check_preconditions() {
    wait_until "$READY_LIMIT_SECONDS" all_ready \
        || { cfail "precondition: not ready within ${READY_LIMIT_SECONDS}s: $(unready_workers)"; return; }
    wait_until "$LAG_LIMIT_SECONDS" source_lag_zero \
        || { cfail "precondition: the source lag is not 0 within ${LAG_LIMIT_SECONDS}s: $(source_offsets | tr '\n' ';')"; return; }
}

#   run_check <number> <name> <function>
run_check() {
    case " ${SPLIT_HTTP_CHECKS:-$1} " in
        *" $1 "*) ;;
        *) return 0 ;;
    esac
    CHECK_LABEL="$1 $2"
    CHECK_VERDICT=""
    step "Check ${CHECK_LABEL}"
    if check_preconditions; then
        "$3"
    fi
    [ -n "$CHECK_VERDICT" ] || cfail "the check ended without a verdict"
    if [ "$CHECK_VERDICT" = pass ]; then
        PASSED=$((PASSED + 1))
    else
        FAILED=$((FAILED + 1))
        FAILED_CHECKS="${FAILED_CHECKS} $1"
    fi
    restore_state
}

# POST request $3 (file $4) to worker $1 with token $2 and expect 202 with
# status accepted and the id. Sets POSTED_AT.
#   post_accepted <service> <token> <request id> <body file>
post_accepted() {
    local code status rid
    POSTED_AT=$(epoch_now)
    code=$(post_job "$1" "$2" "$3" "$4")
    [ "$code" = 202 ] || { cfail "POST of $3 on $1 answered ${code}: $(answer_text)"; return; }
    status=$(answer_field status)
    rid=$(answer_field request_id)
    [ "$status" = '"accepted"' ] && [ "$rid" = "\"$3\"" ] \
        || { cfail "POST of $3 on $1 answered 202 with $(answer_text), expected status accepted and the request id"; return; }
}

# Check the result of the done job in the last answer: result.items $1 and,
# when $2 is not `-`, result.parts $2. Prints the reason and returns 1.
#   check_result <items> <parts or ->
check_result() {
    python3 -c '
import json, sys
body = json.load(open(sys.argv[1]))
items, parts = int(sys.argv[2]), sys.argv[3]
result = body.get("result")
if not isinstance(result, dict) or result.get("items") != items:
    sys.exit(f"result is {result!r}, expected items {items}")
if parts != "-" and result.get("parts") != int(parts):
    sys.exit(f"result is {result!r}, expected parts {parts}")
if "duration_seconds" not in body:
    sys.exit(f"the done body has no duration_seconds: {body}")
' "$TMP_DIR/answer.json" "$1" "$2" 2>&1
}

# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------

check_large_request() {
    local body_file="$TMP_DIR/large.json" reason status received total failed has_parts
    request_json "$LARGE_RID" "$LARGE_ITEMS" 4000 >"$body_file.tmp" && printf '%s' "$(cat "$body_file.tmp")" >"$body_file"
    post_accepted split-worker-1 "$TOKEN_A" "$LARGE_RID" "$body_file" || return
    log "${LARGE_RID}: 202 accepted on split-worker-1"
    poll_end split-worker-2 "$TOKEN_A" "$LARGE_RID" "$END_LIMIT_SECONDS" \
        || { cfail "the GET on split-worker-2 did not show done or failed within ${END_LIMIT_SECONDS}s (last status ${END_STATUS}: $(answer_text))"; return; }
    read -r status received total failed has_parts _ <<<"$(answer_fields)"
    [ "$status" = done ] || { cfail "${LARGE_RID} ended ${status}: $(answer_text)"; return; }
    local rmin rmax
    read -r rmin rmax <<<"$RUNNING_RECEIVED"
    [ "$RUNNING_READS" -ge 2 ] && [ "$rmin" != - ] && [ "$rmax" -gt "$rmin" ] \
        || { cfail "the GET on split-worker-2 read running ${RUNNING_READS} times with received from ${rmin} to ${rmax}: no growth seen"; return; }
    [ "$has_parts" = yes ] && [ "$total" = "$LARGE_PARTS" ] && [ "$received" = "$LARGE_PARTS" ] && [ "$failed" = 0 ] \
        || { cfail "the done body has parts ${received}/${total} failed ${failed}: $(answer_text)"; return; }
    reason=$(check_result "$LARGE_ITEMS" "$LARGE_PARTS") || { cfail "$reason: $(answer_text)"; return; }
    reason=$(no_records "$ITEMS_TOPIC" "$LARGE_RID") || { cfail "$reason"; return; }
    reason=$(no_records "$SUMMARIES_TOPIC" "$LARGE_RID") || { cfail "$reason"; return; }
    check_pass "202 accepted; split-worker-2 read running ${RUNNING_READS} times with received ${rmin} to ${rmax}, then done ${END_SECONDS}s after the POST with result $(answer_field result) and parts ${received}/${total}; no record on ${ITEMS_TOPIC} or ${SUMMARIES_TOPIC}"
}

check_small_request() {
    local rid="http-small-${RUN_TOKEN}" body_file="$TMP_DIR/small.json" reason status has_parts
    printf '%s' "$(request_json "$rid" "$SMALL_ITEMS" 200)" >"$body_file"
    post_accepted split-worker-2 "$TOKEN_A" "$rid" "$body_file" || return
    poll_end split-worker-3 "$TOKEN_A" "$rid" "$END_LIMIT_SECONDS" \
        || { cfail "${rid} did not end within ${END_LIMIT_SECONDS}s (last status ${END_STATUS}: $(answer_text))"; return; }
    read -r status _ _ _ has_parts _ <<<"$(answer_fields)"
    [ "$status" = done ] || { cfail "${rid} ended ${status}: $(answer_text)"; return; }
    [ "$has_parts" = no ] || { cfail "the done body of the local job has parts: $(answer_text)"; return; }
    reason=$(check_result "$SMALL_ITEMS" -) || { cfail "$reason: $(answer_text)"; return; }
    reason=$(no_records "$PARTS_TOPIC" "$rid") || { cfail "$reason"; return; }
    reason=$(no_records "$ITEMS_TOPIC" "$rid") || { cfail "$reason"; return; }
    check_pass "done ${END_SECONDS}s after the POST with result $(answer_field result), no parts in the body; no record on ${PARTS_TOPIC} or ${ITEMS_TOPIC}"
}

# Check the source records of request $1 on stdin: exactly one, with the
# four HTTP job headers in order and no other.
check_source_record() {
    RID="$1" python3 -c '
import json, os, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} records with the key on the source topic, expected 1")
headers = rows[0]["headers"]
wanted = [["dk-origin", "http"], ["dk-request-id", os.environ["RID"]], ["dk-http-client", "tenant-a"], ["dk-deliver-sinks", "false"]]
if headers != wanted:
    sys.exit(f"the source record has the headers {headers}, expected {wanted}")
row = rows[0]
names = [name for name, _ in headers]
print("1 source record at %s:%s with the headers %s" % (row["partition"], row["offset"], names))
' 2>&1
}

check_duplicate_post() {
    local code status rows facts opened
    code=$(post_job split-worker-3 "$TOKEN_A" "$LARGE_RID" "$TMP_DIR/large.json")
    [ "$code" = 200 ] || { cfail "the second POST of ${LARGE_RID} on split-worker-3 answered ${code}: $(answer_text)"; return; }
    status=$(answer_field status)
    [ "$status" = '"done"' ] || { cfail "the second POST answered 200 with $(answer_text), expected the status body of the done job"; return; }
    # Give a second job, if one was wrongly started, time to be read.
    sleep 5
    rows=$(records_of "$SOURCE_TOPIC" "$LARGE_RID") || { cfail "$(read_error)"; return; }
    facts=$(check_source_record "$LARGE_RID" <<<"$rows") || { cfail "$facts"; return; }
    opened=$(count_logged split_job_opened "$LARGE_RID")
    [ "$opened" = 1 ] || { cfail "${opened} split_job_opened lines for ${LARGE_RID}, expected 1"; return; }
    check_pass "the second POST answered 200 with status done; ${facts}; one split_job_opened"
}

check_kill_owner() {
    local rid="http-kill-${RUN_TOKEN}" body_file="$TMP_DIR/kill.json" owner job killed_at status reason closed
    printf '%s' "$(request_json "$rid" "$LARGE_ITEMS" 5000)" >"$body_file"
    post_accepted split-worker-3 "$TOKEN_A" "$rid" "$body_file" || return
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$rid" \
        || { cfail "no worker logged split_job_opened for ${rid} within ${OPENED_LIMIT_SECONDS}s"; return; }
    read -r owner job <<<"$(first_logged split_job_opened "$rid")"
    # Let the split publish some parts, so the kill hits a running job.
    sleep 5
    [ -z "$(log_events "$owner" split_job_closed "$rid")" ] || { cfail "${job} closed before the kill: the items are too short"; return; }
    docker kill "$(container_id "$owner")" >/dev/null || { cfail "cannot kill ${owner}"; return; }
    killed_at=$(epoch_now)
    log "killed the owner ${owner} of ${job}"
    sleep 3
    "${SPLIT[@]}" start "$owner" >/dev/null 2>&1 || { cfail "cannot start ${owner} again"; return; }
    log "started ${owner} again"
    local reader=split-worker-1
    [ "$owner" = split-worker-1 ] && reader=split-worker-2
    poll_end "$reader" "$TOKEN_A" "$rid" "$END_LIMIT_SECONDS" \
        || { cfail "${rid} did not end within ${END_LIMIT_SECONDS}s of the restart (last status ${END_STATUS}: $(answer_text))"; return; }
    read -r status _ <<<"$(answer_fields)"
    [ "$status" = done ] || { cfail "${rid} ended ${status} after the kill of ${owner}: $(answer_text)"; return; }
    reason=$(check_result "$LARGE_ITEMS" "$LARGE_PARTS") || { cfail "$reason: $(answer_text)"; return; }
    # The owner writes `done` before `closed`, so the closed line can come
    # a moment after the GET read done.
    wait_until 30 job_closed_logged "${job%%#*}#" || true
    closed=$(logged_for_job split_job_closed "${job%%#*}#")
    check_pass "killed the owner ${owner} of ${job} and started it again; done $(seconds_between "$killed_at" "$(epoch_now)")s after the kill (read on ${reader}) with result $(answer_field result); split_job_closed (worker=request id): ${closed:-none}"
}

# Check the DLQ records of request $1 on stdin: exactly one, its value equal
# to the file $2, record version 1, and no header value that holds a token.
check_dlq_record() {
    BODY_FILE="$2" python3 -c '
import json, os, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} DLQ records with the key, expected 1")
row = rows[0]
body = open(os.environ["BODY_FILE"]).read()
if row["value"] != body:
    sys.exit("the DLQ value differs from the posted body: " + row["value"][:200])
headers = dict(row["headers"])
if headers.get("dk-record-version") != "1":
    sys.exit("dk-record-version is %r: %s" % (headers.get("dk-record-version"), headers))
for name, value in row["headers"]:
    if "token" in (value or "") or name.lower() == "authorization":
        sys.exit(f"the DLQ record carries a client header: {name}")
print("1 DLQ record, value equal to the posted body, dk-stage %s, dk-error %r" % (headers.get("dk-stage"), (headers.get("dk-error") or "").strip()[:80]))
' 2>&1
}

check_failed_part_dlq() {
    local rid="http-dlq-${RUN_TOKEN}" body_file="$TMP_DIR/dlq.json" status outcome rows facts
    printf '%s' "$(request_json "$rid" 25 300 0 dlq 7 fail)" >"$body_file"
    post_accepted split-worker-1 "$TOKEN_A" "$rid" "$body_file" || return
    poll_end split-worker-3 "$TOKEN_A" "$rid" "$END_LIMIT_SECONDS" \
        || { cfail "${rid} did not end within ${END_LIMIT_SECONDS}s (last status ${END_STATUS}: $(answer_text))"; return; }
    read -r status _ _ _ _ outcome <<<"$(answer_fields)"
    [ "$status" = failed ] && [ "$outcome" = dlq ] || { cfail "${rid} ended ${status} with outcome ${outcome}, expected failed dlq: $(answer_text)"; return; }
    rows=$(records_of "$DLQ_TOPIC" "$rid") || { cfail "$(read_error)"; return; }
    facts=$(check_dlq_record "$rid" "$body_file" <<<"$rows") || { cfail "$facts"; return; }
    check_pass "failed with outcome dlq ${END_SECONDS}s after the POST (error $(answer_field error | cut -c1-100 | sed 's/\\n/ /g')); ${facts}"
}

check_not_found() {
    local code started took
    started=$(epoch_now)
    code=$(get_job split-worker-1 "$TOKEN_A" "http-unknown-${RUN_TOKEN}")
    took=$(seconds_between "$started" "$(epoch_now)")
    [ "$code" = 404 ] && [ "$(answer_field error)" = '"not_found"' ] \
        || { cfail "a GET of an unknown id answered ${code}: $(answer_text)"; return; }
    code=$(get_job split-worker-2 "$TOKEN_B" "$LARGE_RID")
    [ "$code" = 404 ] || { cfail "a GET of ${LARGE_RID} by tenant-b answered ${code}: $(answer_text)"; return; }
    code=$(post_job split-worker-2 "$TOKEN_B" "$LARGE_RID" "$TMP_DIR/large.json")
    [ "$code" = 409 ] && [ "$(answer_field error)" = '"request_id_in_use"' ] \
        || { cfail "a POST of ${LARGE_RID} by tenant-b answered ${code}: $(answer_text), expected 409 request_id_in_use"; return; }
    check_pass "unknown id 404 after ${took}s; the check-1 id by tenant-b: GET 404, POST 409 request_id_in_use"
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

step "Scenario: split jobs over HTTP on ${SPLIT_SERVICES[*]}"
cleanup
if [ "$SPLIT_BUILD" != 0 ]; then
    "${SPLIT[@]}" build split-worker-1 >/dev/null 2>&1 || fail "docker compose could not build split-worker-1 (profile split)"
fi
start_workers || fail "docker compose could not start the split workers (profile split)"
wait_until "$READY_LIMIT_SECONDS" all_ready \
    || fail "split workers not ready within ${READY_LIMIT_SECONDS}s: $(unready_workers)"
ok "split workers ready after ${WAITED}s"

run_check 1 large-request check_large_request
run_check 2 small-request check_small_request
run_check 3 duplicate-post check_duplicate_post
run_check 4 kill-owner check_kill_owner
run_check 5 failed-part-dlq check_failed_part_dlq
run_check 6 not-found check_not_found

TOTAL=$((PASSED + FAILED))
log "${PASSED} checks passed, ${FAILED} failed"
if [ "$FAILED" -gt 0 ]; then
    fail "${PASSED} of ${TOTAL} checks passed; failed:${FAILED_CHECKS}"
fi
pass "${PASSED} of ${TOTAL} checks passed"
