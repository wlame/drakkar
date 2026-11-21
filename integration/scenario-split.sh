#!/usr/bin/env bash
# ============================================================================
# Scenario: split jobs against a real broker. One large request is cut into
# parts that the three split workers run; the owner of the request's source
# partition sums the part results up, delivers one summary, and only then
# commits the request's offset. Kills, restarts, a detach and a broker pause
# must not lose a job or end it twice.
#
# The compose services split-worker-1..3 (profile split) run the handler of
# integration/split-worker/: a request with more than 20 items is split into
# parts of 5 items, each item is one task that sleeps its sleep_ms and exits
# by its mode (ok, fail, hang), on_task_complete writes one record per item to
# split-items, and aggregate writes one summary per request to
# split-summaries (parts, received parts, failed parts, items counted from
# job.results()). A failed item under fail_policy dlq ends its part with a
# dlq outcome. The source topic split-requests has 6 partitions; the request
# id is the Kafka key, and the handler's request_id hook gives it to the log
# lines. The owner of a job is the worker that logged split_job_opened for
# its request id; the job id <partition>:<offset>#<attempt> names it in the
# other log lines. "The source lag is 0" means the committed offset of the
# group drakkar-split is at the end of every partition of split-requests
# (kafka-consumer-groups.sh in the broker container).
#
# Before each check the three workers answer /readyz with 200 and the source
# lag is 0; after each check a killed or stopped worker is started again, a
# detached one is attached, and a paused broker is unpaused, so one failed
# check does not make the next ones fail. Each check prints one line
# `CHECK <n> <name>: PASS|FAIL: ...` with what it measured:
#   1. large request (60 items, 12 parts): at least two workers ran parts of
#      it (drakkar_split_parts_processed_total before and after); one summary
#      with 12 parts, 12 received, 60 items; the committed offset of its
#      partition stayed at or below the request while the parts ran, and was
#      never seen past it before the owner logged split_job_closed; then the
#      source lag is 0.
#   2. small request (10 items): one summary with 1 part, local true, 10
#      items; no record with its key on the parts topic.
#   3. a worker that is not the owner is killed while it runs parts; one
#      summary, every part received.
#   4. the owner is killed after split_job_done while parts run; the parts
#      topic has as many records with the request's key after the end as
#      before the kill; another worker logs split_job_resumed and closes the
#      job; one or two summaries, every part received; the source lag is 0.
#   5. the owner is killed during a slow split (split_pause_ms); another
#      worker logs split_attempt_started and closes the job; one summary of
#      attempt 2 or later with 12 received parts.
#   6. one item fails under fail_policy dlq: exactly one DLQ record with the
#      request's key and the produced value byte for byte, dk-stage execute,
#      dk-record-version 1, a dk-error; no summary; the source lag is 0.
#   7. one item hangs past a job timeout of 90 s (the workers are recreated
#      with SPLIT_JOB_TIMEOUT=90s for this check, and again with the default
#      after it): one DLQ record with dk-stage aggregate and a dk-error that
#      starts with "job timed out after"; the source lag is 0. About two
#      minutes.
#   8. the three workers are restarted one after another during a job (the
#      job is still open when each restart starts); at least one summary,
#      every summary with every part received.
#   9. the owner detaches from its consumer group during a job (operator
#      route); another worker logs split_job_resumed and closes the job; one
#      or two summaries, every part received.
#  10. the broker is paused for 60 s during a job; the job ends with at
#      least one summary with every part received, and no worker container
#      exited or restarted.
#
# Runs with every provider; the split workers use their own filesystem
# autodiscover in a volume of their own.
#
# Usage: HARNESS_PROVIDER=filesystem integration/scenario-split.sh
#        SPLIT_CHECKS="4 5" integration/scenario-split.sh   (only these checks)
#        SPLIT_BUILD=0 integration/scenario-split.sh        (use the image
#        drakkar-harness-split-worker:local as it is, for example one built
#        from a clean export of a commit)
#
# The scenario removes the split worker containers when it ends; the topics
# and the committed offsets stay, and a second run needs none of them.
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
# Every failure is handled where it happens: a failed check must not end the
# script before the checks after it ran, so -e is off.
set -uo pipefail

SCENARIO=scenario-split
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

SPLIT=("${COMPOSE[@]}" --profile=split)
SPLIT_SERVICES=(split-worker-1 split-worker-2 split-worker-3)
SOURCE_TOPIC=split-requests
SOURCE_GROUP=drakkar-split
PARTS_TOPIC=split-requests_drakkar-split_split_parts
SUMMARIES_TOPIC=split-summaries
DLQ_TOPIC=split-requests_dlq
LARGE_ITEMS=60
LARGE_PARTS=12
# The job timeout of check 7: the shortest the config allows is 1m.
SHORT_JOB_TIMEOUT=90s
READY_LIMIT_SECONDS=240
LAG_LIMIT_SECONDS=240
OPENED_LIMIT_SECONDS=90
END_LIMIT_SECONDS=300
# The console consumer stops after this long without a new record.
CONSUMER_IDLE_MS=4000
RUN_TOKEN="$(date +%s)-$$"
# The numbers of the checks to run, separated by spaces; empty runs all ten.
SPLIT_CHECKS="${SPLIT_CHECKS:-}"
# 0: do not build the worker image; use the one that exists.
SPLIT_BUILD="${SPLIT_BUILD:-1}"
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

metrics_port() {
    case "$1" in
        split-worker-1) echo 9111 ;;
        split-worker-2) echo 9112 ;;
        split-worker-3) echo 9113 ;;
    esac
}

worker_ready() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:$(api_port "$1")/readyz"
}

# The split workers whose /readyz does not answer 200, on one line.
unready_workers() {
    local service out=""
    for service in "${SPLIT_SERVICES[@]}"; do
        worker_ready "$service" || out="${out} ${service}"
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

# Start the three split workers from the image (built once by
# split-worker-1), with the job timeout $1 (empty: the compose default).
#   start_workers [<job timeout>]
start_workers() {
    local timeout=${1:-}
    if [ -n "$timeout" ]; then
        SPLIT_JOB_TIMEOUT="$timeout" "${SPLIT[@]}" up --detach --no-build --force-recreate "${SPLIT_SERVICES[@]}" >/dev/null 2>&1
    else
        env -u SPLIT_JOB_TIMEOUT "${SPLIT[@]}" up --detach --no-build --force-recreate "${SPLIT_SERVICES[@]}" >/dev/null 2>&1
    fi
}

# The DK_SPLIT__JOB_TIMEOUT of a worker's running container.
job_timeout_of() {
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$(container_id "$1")" | sed -n 's/^DK_SPLIT__JOB_TIMEOUT=//p'
}

# The detach state of a worker, or `unreachable`.
detach_state() {
    local body
    body=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:$(api_port "$1")/api/v1/debug/detach") \
        || { echo unreachable; return 0; }
    python3 -c 'import json, sys; print(json.load(sys.stdin).get("state"))' <<<"$body"
}

detach_state_is() {
    [ "$(detach_state "$1")" = "$2" ]
}

# POST one detach route of a worker; prints the HTTP status and the body.
#   post_route <service> <detach|attach> <JSON body>
post_route() {
    curl -s -w '\n%{http_code}' --connect-timeout 2 --max-time 10 -X POST \
        -H 'Content-Type: application/json' -d "$3" \
        "http://localhost:$(api_port "$1")/api/v1/debug/$2" || printf '\nnone'
}

kafka_paused() {
    [ "$(docker inspect -f '{{.State.Paused}}' "$(container_id kafka)" 2>/dev/null)" = true ]
}

# Put the split workers and the broker back as each check expects them:
# the broker running, every worker running and attached.
restore_state() {
    local service
    if kafka_paused; then
        docker unpause "$(container_id kafka)" >/dev/null 2>&1 || warn "cannot unpause the broker"
    fi
    for service in "${SPLIT_SERVICES[@]}"; do
        if ! container_running "$service"; then
            log "starting ${service} again"
            "${SPLIT[@]}" start "$service" >/dev/null 2>&1 || warn "cannot start ${service}"
        elif [ "$(detach_state "$service")" = detached ]; then
            log "attaching ${service} again"
            post_route "$service" attach '{}' >/dev/null
        fi
    done
}

cleanup() {
    if kafka_paused; then
        docker unpause "$(container_id kafka)" >/dev/null 2>&1 || true
    fi
    "${SPLIT[@]}" rm --stop --force "${SPLIT_SERVICES[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Requests and topics
# ---------------------------------------------------------------------------

# The request id of request $1 of this run.
request_id() {
    echo "split-${RUN_TOKEN}-$1"
}

# The JSON of one request:
#   request_json <id> <items> <sleep ms> [<split pause ms> <fail policy> <special index> <special mode>]
# The item at <special index> gets <special mode>; every other item is ok.
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

# Produce one request with its id as the key.
#   produce <id> <JSON value>
produce() {
    printf '%s\t%s\n' "$1" "$2" \
        | "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server=localhost:9092 \
            --topic="$SOURCE_TOPIC" \
            --property=parse.key=true \
            --property=key.separator=$'\t' \
            --producer-property=acks=all
}

# Every record of topic $1 whose key is $2, one JSON object per line:
# {"ts", "partition", "offset", "headers": [[name, value], ...], "key",
# "value"} (value as text). Control characters separate the fields, the
# headers and the records, so a header value with a newline or a comma
# stays whole.
records_of() {
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server=localhost:9092 --topic="$1" --from-beginning \
        --property=print.key=true --property=print.timestamp=true \
        --property=print.partition=true --property=print.offset=true \
        --property=print.headers=true \
        --property=key.separator=$'\x1f' --property=line.separator=$'\x1e' \
        --property=headers.separator=$'\x1d' \
        --timeout-ms="$CONSUMER_IDLE_MS" 2>/dev/null \
        | KEY="$2" python3 -c '
import json, os, sys
for record in sys.stdin.read().split("\x1e"):
    fields = record.lstrip("\n").split("\x1f")
    if len(fields) != 6:
        continue
    stamp, partition, offset, headers, key, value = fields
    if key != os.environ["KEY"]:
        continue
    # The tool writes <name>:<value>; a header name has no colon.
    pairs = [] if headers == "NO_HEADERS" else [h.split(":", 1) for h in headers.split("\x1d")]
    print(json.dumps({
        "ts": int(stamp.split(":", 1)[1]),
        "partition": int(partition.split(":", 1)[1]),
        "offset": int(offset.split(":", 1)[1]),
        "headers": pairs,
        "key": key,
        "value": value,
    }))
'
}

count_records() {
    records_of "$1" "$2" | grep -c . || true
}

# The summaries of request $1, one JSON value per line.
summaries_of() {
    records_of "$SUMMARIES_TOPIC" "$1" | python3 -c '
import json, sys
for line in sys.stdin:
    print(json.dumps(json.loads(json.loads(line)["value"])))
'
}

summary_exists() {
    [ "$(summaries_of "$1" | grep -c .)" -ge 1 ]
}

# Check the summaries on stdin: their count is from $1 to $2, and each has
# $3 parts, $3 received parts, no failed part and $4 items. Prints one
# summary line, or the first broken rule and exits 1.
#   check_summaries <min> <max> <parts> <items>
check_summaries() {
    MIN="$1" MAX="$2" PARTS="$3" ITEMS="$4" python3 -c '
import json, os, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
low, high, parts, items = (int(os.environ[k]) for k in ("MIN", "MAX", "PARTS", "ITEMS"))
if not low <= len(rows) <= high:
    sys.exit(f"{len(rows)} summaries, expected {low} to {high}: {rows}")
for row in rows:
    got = (row["parts"], row["received_parts"], row["failed_parts"], row["items"])
    if got != (parts, parts, 0, items):
        sys.exit(f"summary {row} is not {parts} parts, {parts} received, 0 failed, {items} items")
print(f"{len(rows)} summary(ies): " + "; ".join(
    f"{r['"'"'job'"'"']} by {r['"'"'worker_id'"'"']}, {r['"'"'received_parts'"'"']}/{r['"'"'parts'"'"']} parts, {r['"'"'items'"'"']} items"
    for r in rows))
' 2>&1
}

# One line per partition of the source topic: <partition> <committed> <end>,
# the committed offset `-` when the group has none.
source_offsets() {
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server=localhost:9092 --describe --group="$SOURCE_GROUP" 2>/dev/null \
        | awk -v topic="$SOURCE_TOPIC" '$2 == topic { print $3, $4, $5 }'
}

# True when the group has committed the end of every partition that has a
# record. A partition with records and no committed offset has lag.
source_lag_zero() {
    local rows
    rows=$(source_offsets) || return 1
    [ -n "$rows" ] || return 1
    awk '$3 > 0 && ($2 == "-" || $2 < $3) { lag = 1 } END { exit lag }' <<<"$rows"
}

# The committed offset of source partition $1, -1 when there is none.
committed_of() {
    source_offsets | awk -v p="$1" '$1 == p { print ($2 == "-" ? -1 : $2); found = 1 } END { if (!found) print -1 }'
}

# ---------------------------------------------------------------------------
# Log lines
# ---------------------------------------------------------------------------

# The log lines of a worker with event $2, one JSON object per line. A
# non-empty $3 must equal the request_id field; a non-empty $4 must be the
# start of the job field.
#   log_events <service> <event> [<request id>] [<job prefix>]
log_events() {
    "${SPLIT[@]}" logs --no-color --no-log-prefix "$1" 2>/dev/null \
        | EVENT="$2" RID="${3:-}" JOB="${4:-}" python3 -c '
import json, os, sys
event, rid, job = os.environ["EVENT"], os.environ["RID"], os.environ["JOB"]
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
    if job and not str(record.get("job", "")).startswith(job):
        continue
    print(json.dumps(record))
'
}

# The first worker that logged event $1 for request $2 or job prefix $3:
# prints "<service> <job> <outcome>", nothing when none did. A non-empty $4
# names a worker to leave out.
#   first_logged <event> <request id> <job prefix> [<service to skip>]
first_logged() {
    local service line
    for service in "${SPLIT_SERVICES[@]}"; do
        [ "$service" = "${4:-}" ] && continue
        line=$(log_events "$service" "$1" "$2" "$3" | head -1)
        if [ -n "$line" ]; then
            python3 -c 'import json, sys; r = json.loads(sys.argv[2]); print(sys.argv[1], r.get("job"), r.get("outcome", "-"))' "$service" "$line"
            return 0
        fi
    done
}

logged_anywhere() {
    [ -n "$(first_logged "$@")" ]
}

# The job prefix "<partition>:<offset>#" of a job id.
job_prefix() {
    echo "${1%%#*}#"
}

# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

check_pass() {
    CHECK_VERDICT=pass
    echo -e "${GREEN}${BOLD}CHECK ${CHECK_LABEL}: PASS: $*${NC}"
}

# Print the FAIL line of the current check and return 1, so a check ends
# with `cfail "..."; return`.
cfail() {
    CHECK_VERDICT=fail
    echo -e "${RED}${BOLD}CHECK ${CHECK_LABEL}: FAIL: $*${NC}"
    return 1
}

# Wait until the workers are ready and the source lag is 0.
check_preconditions() {
    wait_until "$READY_LIMIT_SECONDS" all_ready \
        || { cfail "precondition: not ready within ${READY_LIMIT_SECONDS}s: $(unready_workers)"; return; }
    wait_until "$LAG_LIMIT_SECONDS" source_lag_zero \
        || { cfail "precondition: the source lag is not 0 within ${LAG_LIMIT_SECONDS}s: $(source_offsets | tr '\n' ';')"; return; }
}

#   run_check <number> <name> <function>
run_check() {
    case " ${SPLIT_CHECKS:-$1} " in
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

# Produce a large request and wait until its owner logged split_job_done.
# Sets RID, OWNER, JOB, PREFIX, PARTITION, OFFSET and PRODUCED_AT, or prints
# the FAIL line and returns 1.
#   start_large_job <request number> <sleep ms> [<split pause ms>]
start_large_job() {
    local opened
    RID=$(request_id "$1")
    PRODUCED_AT=$(epoch_now)
    produce "$RID" "$(request_json "$RID" "$LARGE_ITEMS" "$2" "${3:-0}")" \
        || { cfail "cannot produce ${RID} into ${SOURCE_TOPIC}"; return; }
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$RID" "" \
        || { cfail "no worker logged split_job_opened for ${RID} within ${OPENED_LIMIT_SECONDS}s"; return; }
    opened=$(first_logged split_job_opened "$RID" "")
    read -r OWNER JOB _ <<<"$opened"
    PREFIX=$(job_prefix "$JOB")
    PARTITION=${JOB%%:*}
    OFFSET=${PREFIX#*:}
    OFFSET=${OFFSET%#}
    log "${RID}: job ${JOB} opened on ${OWNER}"
}

wait_split_done() {
    wait_until 60 logged_anywhere split_job_done "" "$PREFIX" \
        || { cfail "no worker logged split_job_done for ${JOB} within 60s (${OWNER}: $(worker_log_tail "$OWNER"))"; return; }
}

# Wait until a worker (other than $1, when given) logged split_job_closed
# for the job. Sets CLOSED_BY, CLOSED_OUTCOME and CLOSED_AT.
wait_closed() {
    local closed
    wait_until "$END_LIMIT_SECONDS" logged_anywhere split_job_closed "" "$PREFIX" "${1:-}" \
        || { cfail "no worker${1:+ other than $1} logged split_job_closed for ${JOB} within ${END_LIMIT_SECONDS}s"; return; }
    CLOSED_AT=$(epoch_now)
    closed=$(first_logged split_job_closed "" "$PREFIX" "${1:-}")
    read -r CLOSED_BY _ CLOSED_OUTCOME <<<"$closed"
}

wait_lag_zero() {
    wait_until "$LAG_LIMIT_SECONDS" source_lag_zero \
        || { cfail "the source lag is not 0 within ${LAG_LIMIT_SECONDS}s of the end of ${JOB}: $(source_offsets | tr '\n' ';')"; return; }
}

# A split worker other than $1 whose executor runs a task now, or nothing.
busy_worker_other_than() {
    local service active
    for service in "${SPLIT_SERVICES[@]}"; do
        [ "$service" = "$1" ] && continue
        active=$(metric "$(metrics_port "$service")" drakkar_executor_pool_active)
        case "$active" in absent | unreachable) continue ;; esac
        if python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "$active"; then
            echo "$service"
            return 0
        fi
    done
}

has_busy_worker_other_than() {
    [ -n "$(busy_worker_other_than "$1")" ]
}

# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------

check_large_request() {
    local service before="" after delta ran_parts=0 ran_list="" held=0 committed closed summary
    for service in "${SPLIT_SERVICES[@]}"; do
        before="${before} $(metric_sum "$(metrics_port "$service")" drakkar_split_parts_processed_total)"
    done
    read -r -a before_values <<<"$before"
    start_large_job 1 4000 || return
    wait_split_done || return
    # Read the committed offset first and the owner's log after it: a
    # commit past the request seen while the log has no split_job_closed yet
    # is a commit before the end of the job.
    while true; do
        committed=$(committed_of "$PARTITION")
        closed=false
        [ -n "$(log_events "$OWNER" split_job_closed "" "$PREFIX")" ] && closed=true
        if [ "$committed" -gt "$OFFSET" ] && [ "$closed" = false ]; then
            cfail "the committed offset of partition ${PARTITION} is ${committed}, past the request at ${OFFSET}, before ${OWNER} logged split_job_closed for ${JOB}"
            return
        fi
        [ "$closed" = false ] && held=$((held + 1))
        [ "$closed" = true ] && [ "$committed" -gt "$OFFSET" ] && break
        if epoch_after "$(epoch_now)" "$(python3 -c 'import sys; print(float(sys.argv[1]) + float(sys.argv[2]))' "$PRODUCED_AT" "$END_LIMIT_SECONDS")"; then
            cfail "within ${END_LIMIT_SECONDS}s: closed logged ${closed}, committed offset ${committed} of partition ${PARTITION} (request at ${OFFSET})"
            return
        fi
        # The commit runs on a timer of the worker; one look a second is
        # enough and keeps the broker tool from piling up.
        sleep 1
    done
    CLOSED_AT=$(epoch_now)
    [ "$held" -ge 1 ] \
        || { cfail "the committed offset was never seen at or below the request while its parts ran"; return; }
    summary=$(summaries_of "$RID" | check_summaries 1 1 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    local i=0
    for service in "${SPLIT_SERVICES[@]}"; do
        after=$(metric_sum "$(metrics_port "$service")" drakkar_split_parts_processed_total)
        delta=$(python3 -c 'import sys; print(int(float(sys.argv[2]) - float(sys.argv[1])))' "${before_values[$i]}" "$after")
        if [ "$delta" -gt 0 ]; then
            ran_parts=$((ran_parts + 1))
        fi
        ran_list="${ran_list} ${service}=${delta}"
        i=$((i + 1))
    done
    [ "$ran_parts" -ge 2 ] \
        || { cfail "only ${ran_parts} worker(s) ran parts of ${RID} (parts processed:${ran_list})"; return; }
    wait_lag_zero || return
    check_pass "${summary}; parts processed:${ran_list}; owner ${OWNER}; offset held in ${held} samples, committed after split_job_closed; $(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s from produce to commit"
}

check_small_request() {
    local rid summary parts produced_at
    rid=$(request_id 2)
    produced_at=$(epoch_now)
    produce "$rid" "$(request_json "$rid" 10 500)" || { cfail "cannot produce ${rid}"; return; }
    wait_until 120 summary_exists "$rid" || { cfail "no summary of ${rid} within 120s"; return; }
    local ended_at
    ended_at=$(epoch_now)
    summary=$(summaries_of "$rid" | python3 -c '
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} summaries, expected 1: {rows}")
row = rows[0]
if (row["local"], row["parts"], row["received_parts"], row["failed_parts"], row["items"]) != (True, 1, 1, 0, 10):
    sys.exit(f"summary {row} is not local with 1 part, 1 received, 0 failed, 10 items")
print(f"1 summary: local, 1 part, 10 items, by {row['"'"'worker_id'"'"']}")
' 2>&1) || { cfail "the summaries of ${rid}: ${summary}"; return; }
    parts=$(count_records "$PARTS_TOPIC" "$rid")
    [ "$parts" = 0 ] || { cfail "${parts} record(s) with the key ${rid} on ${PARTS_TOPIC}, expected none"; return; }
    wait_lag_zero || return
    check_pass "${summary}; 0 records on the parts topic; $(seconds_between "$produced_at" "$ended_at")s from produce to summary"
}

check_kill_part_worker() {
    local victim summary killed_at
    start_large_job 3 6000 || return
    wait_split_done || return
    wait_until 30 has_busy_worker_other_than "$OWNER" \
        || { cfail "no worker other than the owner ${OWNER} ran a task within 30s"; return; }
    victim=$(busy_worker_other_than "$OWNER")
    logged_anywhere split_job_closed "" "$PREFIX" && { cfail "${JOB} ended before the kill: the items are too short"; return; }
    docker kill "$(container_id "$victim")" >/dev/null || { cfail "cannot kill ${victim}"; return; }
    killed_at=$(epoch_now)
    log "killed ${victim} (not the owner ${OWNER}) while it ran parts"
    wait_closed "$victim" || return
    summary=$(summaries_of "$RID" | check_summaries 1 1 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    wait_lag_zero || return
    check_pass "killed ${victim}; ${CLOSED_BY} closed ${JOB} $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill ($(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s from produce); ${summary}"
}

check_kill_owner_after_split() {
    local parts_before parts_after summary killed_at resumed
    start_large_job 4 6000 || return
    wait_split_done || return
    parts_before=$(count_records "$PARTS_TOPIC" "$RID")
    [ "$parts_before" -ge "$LARGE_PARTS" ] \
        || { cfail "${parts_before} records with the key ${RID} on ${PARTS_TOPIC} after split_job_done, expected at least ${LARGE_PARTS}"; return; }
    logged_anywhere split_job_closed "" "$PREFIX" && { cfail "${JOB} ended before the kill: the items are too short"; return; }
    docker kill "$(container_id "$OWNER")" >/dev/null || { cfail "cannot kill ${OWNER}"; return; }
    killed_at=$(epoch_now)
    log "killed the owner ${OWNER} after split_job_done; ${parts_before} part records"
    wait_until 120 logged_anywhere split_job_resumed "" "$PREFIX" "$OWNER" \
        || { cfail "no other worker logged split_job_resumed for ${JOB} within 120s of the kill"; return; }
    resumed=$(first_logged split_job_resumed "" "$PREFIX" "$OWNER")
    resumed=${resumed%% *}
    local resumed_at
    resumed_at=$(epoch_now)
    wait_closed "$OWNER" || return
    parts_after=$(count_records "$PARTS_TOPIC" "$RID")
    [ "$parts_after" = "$parts_before" ] \
        || { cfail "the parts topic has ${parts_after} records with the key ${RID} after the end, ${parts_before} before the kill: parts were published again"; return; }
    summary=$(summaries_of "$RID" | check_summaries 1 2 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    wait_lag_zero || return
    check_pass "killed the owner ${OWNER}; ${resumed} resumed ${JOB} within $(seconds_between "$killed_at" "$resumed_at")s and ${CLOSED_BY} closed it $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill; part records ${parts_before} before and ${parts_after} after; ${summary}"
}

check_kill_owner_during_split() {
    local summary killed_at started
    # 11 pauses of 1.8 s: the split of 12 parts takes about 20 s.
    start_large_job 5 2000 1800 || return
    logged_anywhere split_job_done "" "$PREFIX" \
        && { cfail "${OWNER} logged split_job_done for ${JOB} before the kill: the split was too fast"; return; }
    docker kill "$(container_id "$OWNER")" >/dev/null || { cfail "cannot kill ${OWNER}"; return; }
    killed_at=$(epoch_now)
    [ -z "$(log_events "$OWNER" split_job_done "" "$PREFIX")" ] \
        || { cfail "${OWNER} logged split_job_done for ${JOB} before the kill took effect"; return; }
    log "killed the owner ${OWNER} during the split of ${JOB}"
    wait_until 120 logged_anywhere split_attempt_started "" "$PREFIX" "$OWNER" \
        || { cfail "no other worker logged split_attempt_started for ${JOB} within 120s of the kill"; return; }
    started=$(first_logged split_attempt_started "" "$PREFIX" "$OWNER")
    started=${started%% *}
    wait_closed "$OWNER" || return
    summary=$(summaries_of "$RID" | python3 -c '
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} summaries, expected 1: {rows}")
row = rows[0]
if row["attempt"] < 2 or (row["parts"], row["received_parts"], row["failed_parts"], row["items"]) != (12, 12, 0, 60):
    sys.exit(f"summary {row} is not attempt 2 or later with 12 parts, 12 received, 0 failed, 60 items")
print(f"1 summary: {row['"'"'job'"'"']} by {row['"'"'worker_id'"'"']}, 12/12 parts, 60 items")
' 2>&1) || { cfail "the summaries of ${RID}: ${summary}"; return; }
    wait_lag_zero || return
    check_pass "killed the owner ${OWNER} during the split; ${started} started a new attempt; ${CLOSED_BY} closed ${JOB} $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill; ${summary}"
}

# The facts of the one DLQ record of request $1 on stdin; exits 1 with the
# reason when the rule does not hold:
#   check_dlq_record <expected value> <stage> <error prefix>
check_dlq_record() {
    VALUE="$1" STAGE="$2" PREFIX_TEXT="$3" python3 -c '
import json, os, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} DLQ records with the key, expected 1")
row = rows[0]
if row["value"] != os.environ["VALUE"]:
    sys.exit(f"the DLQ value differs from the produced request: {row['"'"'value'"'"'][:200]}")
headers = dict(row["headers"])
for name, wanted in (("dk-stage", os.environ["STAGE"]), ("dk-record-version", "1")):
    if headers.get(name) != wanted:
        sys.exit(f"header {name} is {headers.get(name)!r}, expected {wanted!r} (headers {headers})")
error = headers.get("dk-error", "")
if not error or not error.startswith(os.environ["PREFIX_TEXT"]):
    sys.exit(f"header dk-error is {error!r}, expected a text that starts with {os.environ['"'"'PREFIX_TEXT'"'"']!r}")
print(f"1 DLQ record, value equal to the request, dk-stage {headers['"'"'dk-stage'"'"']}, dk-error {error.strip()[:80]!r}")
' 2>&1
}

check_failed_part_dlq() {
    local rid value facts summaries produced_at
    rid=$(request_id 6)
    value=$(request_json "$rid" 25 300 0 dlq 7 fail)
    RID=$rid
    produced_at=$(epoch_now)
    produce "$rid" "$value" || { cfail "cannot produce ${rid}"; return; }
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$rid" "" \
        || { cfail "no worker logged split_job_opened for ${rid} within ${OPENED_LIMIT_SECONDS}s"; return; }
    read -r OWNER JOB _ <<<"$(first_logged split_job_opened "$rid" "")"
    PREFIX=$(job_prefix "$JOB")
    wait_closed || return
    [ "$CLOSED_OUTCOME" = dlq ] || { cfail "${JOB} closed with outcome ${CLOSED_OUTCOME}, expected dlq"; return; }
    facts=$(records_of "$DLQ_TOPIC" "$rid" | check_dlq_record "$value" execute "") \
        || { cfail "${facts}"; return; }
    summaries=$(summaries_of "$rid" | grep -c . || true)
    [ "$summaries" = 0 ] || { cfail "${summaries} summaries of ${rid}, expected none"; return; }
    wait_lag_zero || return
    check_pass "${facts}; no summary; ${JOB} closed as dlq by ${CLOSED_BY} $(seconds_between "$produced_at" "$CLOSED_AT")s after produce"
}

check_job_timeout() {
    local rid value facts summaries produced_at timed_out_at service timeout
    log "recreating the split workers with a job timeout of ${SHORT_JOB_TIMEOUT}"
    start_workers "$SHORT_JOB_TIMEOUT" || { cfail "cannot recreate the split workers with SPLIT_JOB_TIMEOUT=${SHORT_JOB_TIMEOUT}"; return; }
    for service in "${SPLIT_SERVICES[@]}"; do
        timeout=$(job_timeout_of "$service")
        [ "$timeout" = "$SHORT_JOB_TIMEOUT" ] || { cfail "${service} runs with DK_SPLIT__JOB_TIMEOUT=${timeout}, expected ${SHORT_JOB_TIMEOUT}"; return; }
    done
    check_preconditions || return
    rid=$(request_id 7)
    value=$(request_json "$rid" 25 300 0 skip 12 hang)
    RID=$rid
    log "this check waits for the job timeout of ${SHORT_JOB_TIMEOUT}: about two minutes"
    produced_at=$(epoch_now)
    produce "$rid" "$value" || { cfail "cannot produce ${rid}"; return; }
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$rid" "" \
        || { cfail "no worker logged split_job_opened for ${rid} within ${OPENED_LIMIT_SECONDS}s"; return; }
    read -r OWNER JOB _ <<<"$(first_logged split_job_opened "$rid" "")"
    PREFIX=$(job_prefix "$JOB")
    # The timeout fires within 5 s after 90 s (the tracker checks every 5 s).
    wait_until 180 logged_anywhere split_job_timed_out "" "$PREFIX" \
        || { cfail "no worker logged split_job_timed_out for ${JOB} within 180s"; return; }
    timed_out_at=$(epoch_now)
    wait_closed || return
    [ "$CLOSED_OUTCOME" = timeout ] || { cfail "${JOB} closed with outcome ${CLOSED_OUTCOME}, expected timeout"; return; }
    facts=$(records_of "$DLQ_TOPIC" "$rid" | check_dlq_record "$value" aggregate "job timed out after") \
        || { cfail "${facts}"; return; }
    summaries=$(summaries_of "$rid" | grep -c . || true)
    [ "$summaries" = 0 ] || { cfail "${summaries} summaries of ${rid}, expected none"; return; }
    wait_lag_zero || return
    check_pass "${facts}; timed out $(seconds_between "$produced_at" "$timed_out_at")s after produce (timeout ${SHORT_JOB_TIMEOUT}); closed as timeout by ${CLOSED_BY}"
}

# After check 7: the workers go back to the default job timeout.
restore_default_timeout() {
    log "recreating the split workers with the default job timeout"
    start_workers "" || warn "cannot recreate the split workers with the default job timeout"
}

check_rolling_restart() {
    local service summary restarted_at
    # Items of 15 s: the 60 items take about 75 s on 12 executor slots, so
    # the job is still open when the last of three graceful restarts starts.
    start_large_job 8 15000 || return
    wait_split_done || return
    for service in "${SPLIT_SERVICES[@]}"; do
        logged_anywhere split_job_closed "" "$PREFIX" \
            && { cfail "${JOB} ended before the restart of ${service}: the items are too short"; return; }
        log "restarting ${service}"
        "${SPLIT[@]}" restart --timeout=45 "$service" >/dev/null 2>&1 || { cfail "cannot restart ${service}"; return; }
        wait_until "$READY_LIMIT_SECONDS" worker_ready "$service" \
            || { cfail "${service} is not ready within ${READY_LIMIT_SECONDS}s of its restart: $(worker_log_tail "$service")"; return; }
        ok "${service} ready ${WAITED}s after its restart"
    done
    restarted_at=$(epoch_now)
    wait_closed || return
    summary=$(summaries_of "$RID" | check_summaries 1 3 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    wait_lag_zero || return
    check_pass "three restarts done $(seconds_between "$PRODUCED_AT" "$restarted_at")s after produce; ${CLOSED_BY} closed ${JOB} $(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s after produce; ${summary}"
}

check_owner_detach() {
    local answer code summary asked_at resumed
    start_large_job 9 6000 || return
    wait_split_done || return
    logged_anywhere split_job_closed "" "$PREFIX" && { cfail "${JOB} ended before the detach: the items are too short"; return; }
    asked_at=$(epoch_now)
    answer=$(post_route "$OWNER" detach '{"duration_seconds": 180, "reason": "scenario-split"}')
    code=${answer##*$'\n'}
    [ "$code" = 202 ] || { cfail "POST /api/v1/debug/detach on ${OWNER} answered ${code}: ${answer%$'\n'*}"; return; }
    wait_until 60 detach_state_is "$OWNER" detached \
        || { cfail "${OWNER} is not detached within 60s (state $(detach_state "$OWNER"))"; return; }
    log "the owner ${OWNER} detached ${WAITED}s after the request"
    wait_until 120 logged_anywhere split_job_resumed "" "$PREFIX" "$OWNER" \
        || { cfail "no other worker logged split_job_resumed for ${JOB} within 120s of the detach"; return; }
    resumed=$(first_logged split_job_resumed "" "$PREFIX" "$OWNER")
    resumed=${resumed%% *}
    wait_closed "$OWNER" || return
    summary=$(summaries_of "$RID" | check_summaries 1 2 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    answer=$(post_route "$OWNER" attach '{}')
    code=${answer##*$'\n'}
    [ "$code" = 202 ] || { cfail "POST /api/v1/debug/attach on ${OWNER} answered ${code}: ${answer%$'\n'*}"; return; }
    wait_until 60 detach_state_is "$OWNER" attached \
        || { cfail "${OWNER} is not attached within 60s (state $(detach_state "$OWNER"))"; return; }
    wait_lag_zero || return
    check_pass "the owner ${OWNER} detached; ${resumed} resumed ${JOB}; ${CLOSED_BY} closed it $(seconds_between "$asked_at" "$CLOSED_AT")s after the detach request; ${summary}"
}

check_broker_pause() {
    local before after summary service paused_at
    start_large_job 10 4000 || return
    wait_split_done || return
    before=$(restart_snapshot "${SPLIT_SERVICES[@]}")
    docker pause "$(container_id kafka)" >/dev/null || { cfail "cannot pause the broker"; return; }
    paused_at=$(epoch_now)
    log "broker paused for 60 s"
    # The length of the outage under test.
    sleep 60
    docker unpause "$(container_id kafka)" >/dev/null || { cfail "cannot unpause the broker"; return; }
    log "broker unpaused"
    wait_closed || return
    summary=$(summaries_of "$RID" | check_summaries 1 2 "$LARGE_PARTS" "$LARGE_ITEMS") \
        || { cfail "the summaries of ${RID}: ${summary}"; return; }
    for service in "${SPLIT_SERVICES[@]}"; do
        container_running "$service" || { cfail "${service} is not running after the broker pause"; return; }
    done
    after=$(restart_snapshot "${SPLIT_SERVICES[@]}")
    [ "$before" = "$after" ] || { cfail "a split worker restarted during the broker pause: before ${before}; after ${after}"; return; }
    wait_lag_zero || return
    check_pass "${CLOSED_BY} closed ${JOB} $(seconds_between "$paused_at" "$CLOSED_AT")s after the pause began (60 s pause); ${summary}; all three workers kept running"
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

step "Scenario: split jobs on ${SPLIT_SERVICES[*]}"
cleanup
if [ "$SPLIT_BUILD" != 0 ]; then
    "${SPLIT[@]}" build split-worker-1 >/dev/null 2>&1 || fail "docker compose could not build split-worker-1 (profile split)"
fi
start_workers "" || fail "docker compose could not start the split workers (profile split)"
wait_until "$READY_LIMIT_SECONDS" all_ready \
    || fail "split workers not ready within ${READY_LIMIT_SECONDS}s: $(unready_workers)"
ok "split workers ready after ${WAITED}s"

run_check 1 large-request check_large_request
run_check 2 small-request check_small_request
run_check 3 kill-part-worker check_kill_part_worker
run_check 4 kill-owner-after-split check_kill_owner_after_split
run_check 5 kill-owner-during-split check_kill_owner_during_split
run_check 6 failed-part-dlq check_failed_part_dlq
run_check 7 job-timeout check_job_timeout
case " ${SPLIT_CHECKS:-7} " in
    *" 7 "*) restore_default_timeout ;;
esac
run_check 8 rolling-restart check_rolling_restart
run_check 9 owner-detach check_owner_detach
run_check 10 broker-pause check_broker_pause

TOTAL=$((PASSED + FAILED))
log "${PASSED} checks passed, ${FAILED} failed"
if [ "$FAILED" -gt 0 ]; then
    fail "${PASSED} of ${TOTAL} checks passed; failed:${FAILED_CHECKS}"
fi
pass "${PASSED} of ${TOTAL} checks passed"
