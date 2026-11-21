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
# `CHECK <n> <name>: PASS|FAIL: ...` with what it measured.
#
# Topics are read with kafka-console-consumer.sh, and every read must cover
# every record the topic had when it began (kafka-get-offsets.sh): a tool
# that fails or stops early fails the check, and is never read as "no
# record" (integration/split_check.py holds these rules). Summaries: one
# per request; two only where two different workers each ended the job, the
# first a worker the check killed, restarted or detached, the second a
# worker that logged split_job_resumed or split_attempt_started for it;
# never three. A stop on purpose waits 45 s, longer than the 30 s drain.
#   1. large request (60 items, 12 parts): the owner's /api/v1/debug/split
#      lists the job (no other worker does) with 12 parts and `received`
#      growing between two reads, and the trace by label split_job returns
#      rows with split_part; exactly 60 item records, one per
#      item, from at least two workers; one summary with 12 parts, 12
#      received, 60 items; the committed offset of its partition, read about
#      once a second, stayed at or below the request until the owner logged
#      split_job_closed; and the commit that moved past the request (its
#      time from __consumer_offsets) is not earlier than the summary record,
#      the closed record of the results log and the split_job_closed line.
#   2. small request (10 items): one summary with 1 part, local true, 10
#      items; exactly 10 item records; no record with its key on the parts
#      topic.
#   3. a worker that is not the owner is killed while it has a part of the
#      request in progress (it wrote some, not all, item records of a part);
#      one summary, every part received, every item has a record, and the
#      victim's unfinished part is in the item records.
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
#      starts with "job timed out after"; then the hung item ends (150 s) and
#      its part writes a result to the closed job: after that result there
#      is still one DLQ record, no summary, one open and one closed record
#      of the job in the results log, and no worker opened it again. About
#      three minutes.
#   8. the three workers are restarted one after another during a job (the
#      job is still open when each restart starts); one or two summaries,
#      every part received.
#   9. the owner detaches from its consumer group during a job (operator
#      route); another worker logs split_job_resumed and closes the job; one
#      or two summaries, every part received.
#  10. the broker is paused for 60 s during a job that is still open when the
#      pause begins; the job ends with every part received (a second summary
#      only by a worker that took the job over), no worker container exited
#      or restarted, and the main cluster's workers, which share the broker,
#      answer /readyz again.
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
ITEMS_TOPIC=split-items
SUMMARIES_TOPIC=split-summaries
DLQ_TOPIC=split-requests_dlq
RESULTS_TOPIC=split-requests_drakkar-split_split_results
# The API ports of the main cluster's workers, which share the broker.
MAIN_WORKER_API_PORTS=(8081 8082 8083)
# The item that hangs in check 7.
HUNG_ITEM=item-12
# Longer than executor.drain_timeout (30 s) of the split workers: a stop
# on purpose lets a worker drain and commit its parts, so they do not run
# again at the next start.
STOP_TIMEOUT_SECONDS=45
SPLIT_CHECK="$(dirname "${BASH_SOURCE[0]}")/split_check.py"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/scenario-split.XXXXXX")
READ_ERROR_FILE="${TMP_DIR}/read.err"
LARGE_ITEMS=60
LARGE_PARTS=12
# The job timeout of check 7: the shortest the config allows is 1m.
SHORT_JOB_TIMEOUT=90s
READY_LIMIT_SECONDS=240
LAG_LIMIT_SECONDS=240
OPENED_LIMIT_SECONDS=90
END_LIMIT_SECONDS=300
# The console consumer stops after this long without a new record.
CONSUMER_IDLE_MS=6000
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
        SPLIT_JOB_TIMEOUT="$timeout" "${SPLIT[@]}" up --detach --no-build --force-recreate \
            --timeout="$STOP_TIMEOUT_SECONDS" "${SPLIT_SERVICES[@]}" >/dev/null 2>&1
    else
        env -u SPLIT_JOB_TIMEOUT "${SPLIT[@]}" up --detach --no-build --force-recreate \
            --timeout="$STOP_TIMEOUT_SECONDS" "${SPLIT_SERVICES[@]}" >/dev/null 2>&1
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
    "${SPLIT[@]}" stop --timeout="$STOP_TIMEOUT_SECONDS" "${SPLIT_SERVICES[@]}" >/dev/null 2>&1 || true
    "${SPLIT[@]}" rm --force "${SPLIT_SERVICES[@]}" >/dev/null 2>&1 || true
}

remove_tmp_dir() {
    cleanup
    rm -rf "$TMP_DIR"
}
trap remove_tmp_dir EXIT

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

# One Kafka command-line tool of the broker container, with the broker's
# address:
#   kafka_tool <script> [args...]
kafka_tool() {
    local script=$1
    shift
    "${COMPOSE[@]}" exec -T kafka "/opt/kafka/bin/${script}" --bootstrap-server=localhost:9092 "$@"
}

# The checks over what the tools print (integration/split_check.py).
split_check() {
    python3 "$SPLIT_CHECK" "$@"
}

# Why the last read of a topic failed, written by records_of and the other
# readers (they run in command substitutions, so a variable would not
# reach the caller).
read_error() {
    cat "$READ_ERROR_FILE" 2>/dev/null || echo "no reason recorded"
}

note_read_error() {
    echo "$*" >"$READ_ERROR_FILE"
}

# The lines <topic>:<partition>:<offset> of topic $1 at the time $2 (-1 the
# end, -2 the earliest). Fails when the tool fails or prints nothing.
topic_offsets() {
    local out
    out=$(kafka_tool kafka-get-offsets.sh --topic="$1" --time="$2" 2>"$TMP_DIR/offsets.err") \
        || { note_read_error "kafka-get-offsets.sh failed for $1: $(tail -c 300 "$TMP_DIR/offsets.err")"; return 1; }
    grep -q "^$1:" <<<"$out" || { note_read_error "kafka-get-offsets.sh printed no offset of $1: ${out:0:300}"; return 1; }
    grep "^$1:" <<<"$out"
}

# Every record of topic $1 whose key is $2, one JSON object per line:
# {"ts", "partition", "offset", "headers": [[name, value], ...], "key",
# "value"} (value as text). Control characters separate the fields, the
# headers and the records, so a header value with a newline or a comma
# stays whole. The read must cover every record the topic had when it
# began (its earliest and end offsets): a failed tool, a broker that does
# not answer or a consumer that stopped early fails the read (status 1,
# the reason in read_error), and is never "no record". The first try stops
# after as many records as the topic had; when records written meanwhile
# took their place, two more tries read until the topic is idle.
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

# The number of records of topic $1 with key $2; fails like records_of.
count_records() {
    local rows
    rows=$(records_of "$1" "$2") || return 1
    grep -c . <<<"$rows" || true
}

summary_exists() {
    local count
    count=$(count_records "$SUMMARIES_TOPIC" "$1") || return 1
    [ "$count" -ge 1 ]
}

# The workers that took the current job over: they logged
# split_job_resumed or split_attempt_started for the job prefix $1.
takers_of() {
    local service out=""
    for service in "${SPLIT_SERVICES[@]}"; do
        if [ -n "$(log_events "$service" split_job_resumed "" "$1")$(log_events "$service" split_attempt_started "" "$1")" ]; then
            out="${out} ${service}"
        fi
    done
    echo "${out# }"
}

# Check the summaries of the current request (RID, PREFIX): each has every
# part received, no failed part and every item. One summary always passes.
# Two pass only when <max> is 2 and two workers each ended the job: they
# differ, the first is one of <disrupted> (the workers this check killed,
# restarted or detached; empty: any), and the second took the job over
# (takers_of). Prints one summary line, or the broken rule and returns 1.
#   check_summaries <max> [<disrupted workers>]
check_summaries() {
    local rows
    rows=$(records_of "$SUMMARIES_TOPIC" "$RID") || { read_error; return 1; }
    split_check summaries --parts="$LARGE_PARTS" --items="$LARGE_ITEMS" --max="$1" \
        --disrupted="${2:-}" --takers="$(takers_of "$PREFIX")" <<<"$rows" 2>&1
}

# Check the item records of request $1: every item of 0..$2-1 has one;
# with "exact" there are exactly $2 records (no item ran twice), else at
# least $2. A victim $4 adds the parts it left unfinished, and with
# "partial" $5 at least one must exist. Prints the counts per worker, or
# the broken rule and returns 1.
#   check_items <request id> <count> <exact|at-least> [<victim> [partial]]
check_items() {
    local rows args=(--count="$2")
    rows=$(records_of "$ITEMS_TOPIC" "$1") || { read_error; return 1; }
    [ "$3" = exact ] && args+=(--exact)
    [ -n "${4:-}" ] && args+=(--victim="$4")
    [ "${5:-}" = partial ] && args+=(--require-partial)
    split_check items "${args[@]}" <<<"$rows" 2>&1
}

items_complete() {
    check_items "$1" "$2" at-least >/dev/null
}

# Wait until every item of request $1 (count $2) has a record, then check
# them by <exact|at-least>. Item records are delivered before the part
# result, so after the summary they are there; the wait covers a slow read.
#   wait_items <request id> <count> <exact|at-least> [<victim> [partial]]
wait_items() {
    wait_until 60 items_complete "$1" "$2" || true
    check_items "$@"
}

# One line per partition of the source topic: <partition> <committed> <end>,
# the committed offset `-` when the group has none. Fails when the tool
# fails or prints no partition of the topic.
source_offsets() {
    local out rows
    out=$(kafka_tool kafka-consumer-groups.sh --describe --group="$SOURCE_GROUP" 2>"$TMP_DIR/groups.err") \
        || { note_read_error "kafka-consumer-groups.sh failed: $(tail -c 300 "$TMP_DIR/groups.err")"; return 1; }
    rows=$(awk -v topic="$SOURCE_TOPIC" '$2 == topic { print $3, $4, $5 }' <<<"$out")
    [ -n "$rows" ] || { note_read_error "kafka-consumer-groups.sh listed no partition of ${SOURCE_TOPIC}: ${out:0:300}"; return 1; }
    echo "$rows"
}

# True when the group has committed the end of every partition that has a
# record. A partition with records and no committed offset has lag.
source_lag_zero() {
    local rows
    rows=$(source_offsets) || return 1
    awk '$3 > 0 && ($2 == "-" || $2 < $3) { lag = 1 } END { exit lag }' <<<"$rows"
}

# The committed offset of source partition $1 (`-` when there is none).
# Fails when the tool fails or does not list the partition: a failed read
# is never taken for an offset that was held.
committed_of() {
    local rows value
    rows=$(source_offsets) || return 1
    value=$(awk -v p="$1" '$1 == p { print $2 }' <<<"$rows")
    [ -n "$value" ] || { note_read_error "kafka-consumer-groups.sh did not list partition $1 of ${SOURCE_TOPIC}"; return 1; }
    echo "$value"
}

# The first commit of the source group on source partition $1 past offset
# $2, read from __consumer_offsets with the broker's offsets formatter (the
# partition of __consumer_offsets that holds the group, read from its
# start): prints "<commit ms> <offset> <ms of the last commit at or below
# $2> <its offset>" (the last two `-` when there is none). Status 4 when no
# such commit is there yet, 1 when the tool fails.
#   commit_past <partition> <offset>
commit_past() {
    local count holder status
    count=$(topic_offsets __consumer_offsets -1 | grep -c .) || return 1
    holder=$(split_check offsets-partition --group="$SOURCE_GROUP" --count="$count") || return 1
    kafka_tool kafka-console-consumer.sh --topic=__consumer_offsets --partition="$holder" --offset=earliest \
        --formatter='kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter' \
        --timeout-ms="$CONSUMER_IDLE_MS" 2>"$TMP_DIR/commits.err" >"$TMP_DIR/commits.out" \
        || { note_read_error "reading __consumer_offsets failed: $(tail -c 300 "$TMP_DIR/commits.err")"; return 1; }
    grep -q 'Processed a total of' "$TMP_DIR/commits.err" \
        || { note_read_error "the read of __consumer_offsets did not end normally: $(tail -c 300 "$TMP_DIR/commits.err")"; return 1; }
    split_check commit --group="$SOURCE_GROUP" --topic="$SOURCE_TOPIC" --partition="$1" --after="$2" <"$TMP_DIR/commits.out"
    status=$?
    [ "$status" = 4 ] && note_read_error "no commit of ${SOURCE_GROUP} on ${SOURCE_TOPIC}:$1 past $2 in __consumer_offsets partition ${holder}"
    return "$status"
}

commit_found() {
    commit_past "$@" >/dev/null
}

# The epoch milliseconds of the first log line of event $2 by worker $1 for
# the job prefix $3.
#   log_ms <service> <event> <job prefix>
log_ms() {
    local line stamp
    line=$(log_events "$1" "$2" "" "$3" | head -1)
    [ -n "$line" ] || return 1
    stamp=$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["timestamp"])' "$line") || return 1
    split_check iso-ms "$stamp"
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
    set_job_position
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

# A worker other than the owner that wrote some, not all, item records of
# a part of the current request: it runs a part of this request now.
# Prints "<service> <unfinished parts>", nothing when there is none.
partial_worker() {
    local rows
    rows=$(records_of "$ITEMS_TOPIC" "$RID") || return 1
    split_check partial-worker --skip="$OWNER" <<<"$rows" 2>/dev/null
}

has_partial_worker() {
    [ -n "$(partial_worker)" ]
}

# The records of the results log of the current job (key
# <partition>:<offset>) of kind $1: one line "<ts> <attempt> <attempt id>"
# each. Fails like records_of.
results_of_kind() {
    local rows
    rows=$(records_of "$RESULTS_TOPIC" "${PARTITION}:${OFFSET}") || return 1
    split_check results --kind="$1" <<<"$rows"
}

# Set PARTITION and OFFSET from the job id in JOB.
set_job_position() {
    PREFIX=$(job_prefix "$JOB")
    PARTITION=${JOB%%:*}
    OFFSET=${PREFIX#*:}
    OFFSET=${OFFSET%#}
}

# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------

# Sample the committed offset of the request's partition until the owner
# logs split_job_closed. Every sample reads the offset first and the log
# after it: a commit past the request seen while the log has no
# split_job_closed yet is a commit before the end of the job. A failed read
# of the tool fails the check. Sets HELD to the samples that showed the
# offset at or below the request (or none) while the job was open.
sample_held_offset() {
    local committed closed deadline
    HELD=0
    deadline=$(python3 -c 'import sys; print(float(sys.argv[1]) + float(sys.argv[2]))' "$PRODUCED_AT" "$END_LIMIT_SECONDS")
    while true; do
        committed=$(committed_of "$PARTITION") \
            || { cfail "the committed offset of partition ${PARTITION} cannot be read: $(read_error)"; return; }
        closed=false
        [ -n "$(log_events "$OWNER" split_job_closed "" "$PREFIX")" ] && closed=true
        [ "$closed" = true ] && return 0
        if [ "$committed" != - ] && [ "$committed" -gt "$OFFSET" ]; then
            cfail "the committed offset of partition ${PARTITION} is ${committed}, past the request at ${OFFSET}, before ${OWNER} logged split_job_closed for ${JOB}"
            return
        fi
        HELD=$((HELD + 1))
        if epoch_after "$(epoch_now)" "$deadline"; then
            cfail "${OWNER} did not log split_job_closed for ${JOB} within ${END_LIMIT_SECONDS}s of the produce"
            return
        fi
        # The commit runs on a timer of the worker; one look a second is
        # enough and keeps the broker tool from piling up.
        sleep 1
    done
}

# The job PARTITION:OFFSET as the split route of worker $1 lists it: prints
# "<received> <parts> <phase>" (parts `null` before split-done), nothing when
# the worker does not list the job or does not answer.
job_view() {
    curl -sf --connect-timeout 2 --max-time 5 "http://localhost:$(api_port "$1")/api/v1/debug/split?limit=500" \
        | P="$PARTITION" O="$OFFSET" python3 -c '
import json, os, sys
state = json.load(sys.stdin)
for job in state.get("jobs", []):
    if str(job["partition"]) == os.environ["P"] and str(job["offset"]) == os.environ["O"]:
        print(job["received"], job["parts"] if job["parts"] is not None else "null", job["phase"])
' 2>/dev/null
}

# The workers whose split route lists the current job, with their view:
# "<service>=<received> <parts> <phase>" one per line.
job_views() {
    local service view
    for service in "${SPLIT_SERVICES[@]}"; do
        view=$(job_view "$service")
        [ -n "$view" ] && echo "${service}=${view}"
    done
}

# Poll the split route until `received` of the current job grew between two
# reads while the job was open. Only the owner may list the job. Prints the
# facts, or the broken rule and returns 1.
check_split_route() {
    local views first_received="" received parts phase listed deadline owners
    deadline=$(python3 -c 'import time; print(time.time() + 90)')
    while true; do
        views=$(job_views)
        owners=$(grep -c . <<<"$views")
        if [ -n "$views" ]; then
            [ "$owners" = 1 ] || { echo "${owners} workers list the job ${PARTITION}:${OFFSET}, expected only its owner: ${views}"; return 1; }
            listed=${views%%=*}
            [ "$listed" = "$OWNER" ] || { echo "${listed} lists the job, but ${OWNER} owns it"; return 1; }
            read -r received parts phase <<<"${views#*=}"
            [ "$parts" = null ] || [ "$parts" = "$LARGE_PARTS" ] \
                || { echo "the route shows ${parts} parts for ${JOB}, expected ${LARGE_PARTS}"; return 1; }
            if [ -z "$first_received" ]; then
                first_received=$received
            elif [ "$received" -gt "$first_received" ] && [ "$parts" = "$LARGE_PARTS" ]; then
                echo "split route of ${OWNER}: received ${first_received} then ${received} of ${parts} parts (phase ${phase})"
                return 0
            fi
        elif [ -n "$first_received" ]; then
            echo "the job ${JOB} left the route of ${OWNER} after received ${first_received}, before it grew"
            return 1
        fi
        if python3 -c 'import sys, time; sys.exit(0 if time.time() > float(sys.argv[1]) else 1)' "$deadline"; then
            echo "received did not grow within 90s (first ${first_received:-none}); views: ${views:-none}"
            return 1
        fi
        sleep 2
    done
}

# One worker's trace by label split_job=<partition>:<offset> returns task
# rows, and the rows carry split_part. Prints the facts or the reason.
check_split_labels() {
    local service body count parts_seen=0 total=0
    for service in "${SPLIT_SERVICES[@]}"; do
        body=$(curl -sf --connect-timeout 2 --max-time 10 -G \
            --data-urlencode "key=split_job" --data-urlencode "value=${PARTITION}:${OFFSET}" \
            "http://localhost:$(api_port "$service")/api/v1/debug/trace-by-label") \
            || { echo "trace-by-label failed on ${service}"; return 1; }
        count=$(python3 -c 'import json, sys; print(len(json.load(sys.stdin)))' <<<"$body") \
            || { echo "trace-by-label on ${service} did not answer JSON"; return 1; }
        total=$((total + count))
        grep -q split_part <<<"$body" && parts_seen=$((parts_seen + 1))
    done
    [ "$total" -ge 1 ] || { echo "no task row with the label split_job=${PARTITION}:${OFFSET} on any worker"; return 1; }
    [ "$parts_seen" -ge 1 ] || { echo "${total} rows with split_job, none with split_part"; return 1; }
    echo "trace by label split_job: ${total} rows over the workers, split_part on ${parts_seen} of them"
}

check_large_request() {
    local route labels pair summary items rows summary_ms closed_rec_ms closed_log_ms commit commit_ms commit_offset below_ms below_offset
    start_large_job 1 4000 || return
    wait_split_done || return
    route=$(check_split_route) || { cfail "${route}"; return; }
    labels=$(check_split_labels) || { cfail "${labels}"; return; }
    sample_held_offset || return
    [ "$HELD" -ge 1 ] \
        || { cfail "the committed offset was never read at or below the request while its parts ran"; return; }
    summary=$(check_summaries 1) || { cfail "the summaries of ${RID}: ${summary}"; return; }
    rows=$(records_of "$SUMMARIES_TOPIC" "$RID") || { cfail "$(read_error)"; return; }
    summary_ms=$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["ts"])' "$rows")
    closed_rec_ms=$(results_of_kind closed) || { cfail "$(read_error)"; return; }
    [ "$(grep -c . <<<"$closed_rec_ms")" = 1 ] \
        || { cfail "the results log has $(grep -c . <<<"$closed_rec_ms") closed records of ${PARTITION}:${OFFSET}, expected 1"; return; }
    closed_rec_ms=${closed_rec_ms%% *}
    closed_log_ms=$(log_ms "$OWNER" split_job_closed "$PREFIX") \
        || { cfail "cannot read the time of split_job_closed of ${JOB} in the log of ${OWNER}"; return; }
    # The commit time is the broker's time of the offset commit, the record
    # times are the producers' times: one clock, the Docker VM's.
    wait_until 90 commit_found "$PARTITION" "$OFFSET" \
        || { cfail "no commit past ${OFFSET} of partition ${PARTITION} within 90s of split_job_closed: $(read_error)"; return; }
    commit=$(commit_past "$PARTITION" "$OFFSET") || { cfail "$(read_error)"; return; }
    read -r commit_ms commit_offset below_ms below_offset <<<"$commit"
    for pair in "summary record:${summary_ms}" "closed record:${closed_rec_ms}" "split_job_closed log line:${closed_log_ms}"; do
        [ "$commit_ms" -ge "${pair##*:}" ] \
            || { cfail "the commit of offset ${commit_offset} of partition ${PARTITION} at ${commit_ms} ms is earlier than the ${pair%%:*} at ${pair##*:} ms"; return; }
    done
    CLOSED_AT=$(python3 -c 'import sys; print(int(sys.argv[1]) / 1000)' "$commit_ms")
    # Without a fault every item runs once.
    items=$(wait_items "$RID" "$LARGE_ITEMS" exact) || { cfail "the item records of ${RID}: ${items}"; return; }
    [ "$(grep -o '=[0-9]*' <<<"$items" | grep -c .)" -ge 2 ] \
        || { cfail "fewer than two workers ran items of ${RID}: ${items}"; return; }
    wait_lag_zero || return
    check_pass "${route}; ${labels}; ${summary}; ${items}; owner ${OWNER}; offset held in ${HELD} samples; commit to ${commit_offset} at +$((commit_ms - summary_ms)) ms after the summary record, +$((commit_ms - closed_rec_ms)) ms after the closed record, +$((commit_ms - closed_log_ms)) ms after split_job_closed (last commit before: ${below_offset} at ${below_ms}); $(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s from produce to commit"
}

check_small_request() {
    local rid summary parts produced_at ended_at items rows
    rid=$(request_id 2)
    produced_at=$(epoch_now)
    produce "$rid" "$(request_json "$rid" 10 500)" || { cfail "cannot produce ${rid}"; return; }
    wait_until 120 summary_exists "$rid" || { cfail "no summary of ${rid} within 120s ($(read_error))"; return; }
    ended_at=$(epoch_now)
    rows=$(records_of "$SUMMARIES_TOPIC" "$rid") || { cfail "$(read_error)"; return; }
    summary=$(python3 -c '
import json, sys
rows = [json.loads(json.loads(line)["value"]) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} summaries, expected 1: {rows}")
row = rows[0]
if (row["local"], row["parts"], row["received_parts"], row["failed_parts"], row["items"]) != (True, 1, 1, 0, 10):
    sys.exit(f"summary {row} is not local with 1 part, 1 received, 0 failed, 10 items")
print(f"1 summary: local, 1 part, 10 items, by {row['"'"'worker_id'"'"']}")
' <<<"$rows" 2>&1) || { cfail "the summaries of ${rid}: ${summary}"; return; }
    items=$(wait_items "$rid" 10 exact) || { cfail "the item records of ${rid}: ${items}"; return; }
    # records_of proves that the read covered the whole parts topic, so 0
    # means no part record, not a failed read.
    parts=$(count_records "$PARTS_TOPIC" "$rid") || { cfail "$(read_error)"; return; }
    [ "$parts" = 0 ] || { cfail "${parts} record(s) with the key ${rid} on ${PARTS_TOPIC}, expected none"; return; }
    wait_lag_zero || return
    check_pass "${summary}; ${items}; 0 records on the parts topic (every record of the topic read); $(seconds_between "$produced_at" "$ended_at")s from produce to summary"
}

check_kill_part_worker() {
    local victim unfinished summary killed_at items
    # Items of 10 s: a part of 5 items on 4 executor slots has 4 records
    # for about 10 s before its fifth, so the kill hits a part in progress.
    start_large_job 3 10000 || return
    wait_split_done || return
    wait_until 60 has_partial_worker \
        || { cfail "no worker other than the owner ${OWNER} had a part of ${RID} in progress within 60s"; return; }
    read -r victim unfinished <<<"$(partial_worker)"
    logged_anywhere split_job_closed "" "$PREFIX" && { cfail "${JOB} ended before the kill: the items are too short"; return; }
    docker kill "$(container_id "$victim")" >/dev/null || { cfail "cannot kill ${victim}"; return; }
    killed_at=$(epoch_now)
    log "killed ${victim} (not the owner ${OWNER}) while it ran parts ${unfinished} of ${RID}"
    wait_closed "$victim" || return
    summary=$(check_summaries 1) || { cfail "the summaries of ${RID}: ${summary}"; return; }
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least "$victim" partial) \
        || { cfail "the item records of ${RID}: ${items}"; return; }
    wait_lag_zero || return
    check_pass "killed ${victim}; ${CLOSED_BY} closed ${JOB} $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill ($(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s from produce); ${summary}; ${items}"
}

check_kill_owner_after_split() {
    local parts_before parts_after summary killed_at resumed resumed_at items
    start_large_job 4 6000 || return
    wait_split_done || return
    parts_before=$(count_records "$PARTS_TOPIC" "$RID") || { cfail "$(read_error)"; return; }
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
    resumed_at=$(epoch_now)
    wait_closed "$OWNER" || return
    parts_after=$(count_records "$PARTS_TOPIC" "$RID") || { cfail "$(read_error)"; return; }
    [ "$parts_after" = "$parts_before" ] \
        || { cfail "the parts topic has ${parts_after} records with the key ${RID} after the end, ${parts_before} before the kill: parts were published again"; return; }
    summary=$(check_summaries 2 "$OWNER") || { cfail "the summaries of ${RID}: ${summary}"; return; }
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least) || { cfail "the item records of ${RID}: ${items}"; return; }
    wait_lag_zero || return
    check_pass "killed the owner ${OWNER}; ${resumed} resumed ${JOB} within $(seconds_between "$killed_at" "$resumed_at")s and ${CLOSED_BY} closed it $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill; part records ${parts_before} before and ${parts_after} after; ${summary}; ${items}"
}

check_kill_owner_during_split() {
    local summary killed_at started rows items
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
    rows=$(records_of "$SUMMARIES_TOPIC" "$RID") || { cfail "$(read_error)"; return; }
    summary=$(python3 -c '
import json, sys
rows = [json.loads(json.loads(line)["value"]) for line in sys.stdin if line.strip()]
if len(rows) != 1:
    sys.exit(f"{len(rows)} summaries, expected 1: {rows}")
row = rows[0]
if row["attempt"] < 2 or (row["parts"], row["received_parts"], row["failed_parts"], row["items"]) != (12, 12, 0, 60):
    sys.exit(f"summary {row} is not attempt 2 or later with 12 parts, 12 received, 0 failed, 60 items")
print(f"1 summary: {row['"'"'job'"'"']} by {row['"'"'worker_id'"'"']}, 12/12 parts, 60 items")
' <<<"$rows" 2>&1) || { cfail "the summaries of ${RID}: ${summary}"; return; }
    # The parts of the first attempt that were published also ran.
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least) || { cfail "the item records of ${RID}: ${items}"; return; }
    wait_lag_zero || return
    check_pass "killed the owner ${OWNER} during the split; ${started} started a new attempt; ${CLOSED_BY} closed ${JOB} $(seconds_between "$killed_at" "$CLOSED_AT")s after the kill; ${summary}; ${items}"
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

# Check the one DLQ record of request $1: <value> <stage> <error prefix>.
# Prints its facts, or the broken rule and returns 1.
dlq_facts() {
    local rows
    rows=$(records_of "$DLQ_TOPIC" "$1") || { read_error; return 1; }
    check_dlq_record "$2" "$3" "$4" <<<"$rows"
}

# The summaries of request $1 must be none; every record of the topic was
# read (records_of), so 0 is not a failed read.
no_summary() {
    local count
    count=$(count_records "$SUMMARIES_TOPIC" "$1") || { read_error; return 1; }
    [ "$count" = 0 ] || { echo "${count} summaries of $1, expected none"; return 1; }
}

check_failed_part_dlq() {
    local rid value facts produced_at reason
    rid=$(request_id 6)
    value=$(request_json "$rid" 25 300 0 dlq 7 fail)
    RID=$rid
    produced_at=$(epoch_now)
    produce "$rid" "$value" || { cfail "cannot produce ${rid}"; return; }
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$rid" "" \
        || { cfail "no worker logged split_job_opened for ${rid} within ${OPENED_LIMIT_SECONDS}s"; return; }
    read -r OWNER JOB _ <<<"$(first_logged split_job_opened "$rid" "")"
    set_job_position
    wait_closed || return
    [ "$CLOSED_OUTCOME" = dlq ] || { cfail "${JOB} closed with outcome ${CLOSED_OUTCOME}, expected dlq"; return; }
    facts=$(dlq_facts "$rid" "$value" execute "") || { cfail "${facts}"; return; }
    reason=$(no_summary "$rid") || { cfail "${reason}"; return; }
    wait_lag_zero || return
    check_pass "${facts}; no summary (every record of the topic read); ${JOB} closed as dlq by ${CLOSED_BY} $(seconds_between "$produced_at" "$CLOSED_AT")s after produce"
}

# The record of the hung item of check 7 is on the items topic.
hung_item_written() {
    local rows
    rows=$(records_of "$ITEMS_TOPIC" "$RID") || return 1
    python3 -c '
import json, sys
items = {json.loads(json.loads(line)["value"])["item"] for line in sys.stdin if line.strip()}
sys.exit(0 if sys.argv[1] in items else 1)
' "$HUNG_ITEM" <<<"$rows"
}

# A result record of the current job came to the results log after its
# closed record.
late_result_written() {
    local closed results
    closed=$(results_of_kind closed) || return 1
    results=$(results_of_kind result) || return 1
    [ -n "$closed" ] || return 1
    LATE_RESULT_MS=$(python3 -c '
import sys
closed = min(int(line.split()[0]) for line in sys.argv[1].splitlines() if line.strip())
late = [int(line.split()[0]) for line in sys.argv[2].splitlines() if line.strip() and int(line.split()[0]) > closed]
print(min(late) - closed if late else "")
' "$closed" "$results")
    [ -n "$LATE_RESULT_MS" ]
}

# The job of check 7 stayed closed: the results log has one open and one
# closed record of it, and no worker opened it again.
job_stayed_closed() {
    local opens closes service opened=0 attempts=0
    opens=$(results_of_kind open) || { read_error; return 1; }
    closes=$(results_of_kind closed) || { read_error; return 1; }
    [ "$(grep -c . <<<"$opens")" = 1 ] || { echo "the results log has $(grep -c . <<<"$opens") open records of ${PARTITION}:${OFFSET}, expected 1: the job was opened again"; return 1; }
    [ "$(grep -c . <<<"$closes")" = 1 ] || { echo "the results log has $(grep -c . <<<"$closes") closed records of ${PARTITION}:${OFFSET}, expected 1"; return 1; }
    for service in "${SPLIT_SERVICES[@]}"; do
        opened=$((opened + $(log_events "$service" split_job_opened "$RID" "" | grep -c .)))
        attempts=$((attempts + $(log_events "$service" split_attempt_started "" "$PREFIX" | grep -c .)))
    done
    [ "$opened" = 1 ] && [ "$attempts" = 0 ] \
        || { echo "split_job_opened logged ${opened} times for ${RID} and split_attempt_started ${attempts} times for ${JOB}, expected 1 and 0"; return 1; }
}

check_job_timeout() {
    local value facts produced_at timed_out_at service timeout reason
    log "recreating the split workers with a job timeout of ${SHORT_JOB_TIMEOUT}"
    start_workers "$SHORT_JOB_TIMEOUT" || { cfail "cannot recreate the split workers with SPLIT_JOB_TIMEOUT=${SHORT_JOB_TIMEOUT}"; return; }
    for service in "${SPLIT_SERVICES[@]}"; do
        timeout=$(job_timeout_of "$service")
        [ "$timeout" = "$SHORT_JOB_TIMEOUT" ] || { cfail "${service} runs with DK_SPLIT__JOB_TIMEOUT=${timeout}, expected ${SHORT_JOB_TIMEOUT}"; return; }
    done
    check_preconditions || return
    RID=$(request_id 7)
    value=$(request_json "$RID" 25 300 0 skip "${HUNG_ITEM#item-}" hang)
    log "this check waits for the job timeout of ${SHORT_JOB_TIMEOUT}, then for the hung item (150 s): about three minutes"
    produced_at=$(epoch_now)
    produce "$RID" "$value" || { cfail "cannot produce ${RID}"; return; }
    wait_until "$OPENED_LIMIT_SECONDS" logged_anywhere split_job_opened "$RID" "" \
        || { cfail "no worker logged split_job_opened for ${RID} within ${OPENED_LIMIT_SECONDS}s"; return; }
    read -r OWNER JOB _ <<<"$(first_logged split_job_opened "$RID" "")"
    set_job_position
    # The timeout fires within 5 s after 90 s (the tracker checks every 5 s).
    wait_until 180 logged_anywhere split_job_timed_out "" "$PREFIX" \
        || { cfail "no worker logged split_job_timed_out for ${JOB} within 180s"; return; }
    timed_out_at=$(epoch_now)
    wait_closed || return
    [ "$CLOSED_OUTCOME" = timeout ] || { cfail "${JOB} closed with outcome ${CLOSED_OUTCOME}, expected timeout"; return; }
    facts=$(dlq_facts "$RID" "$value" aggregate "job timed out after") || { cfail "${facts}"; return; }
    reason=$(no_summary "$RID") || { cfail "${reason}"; return; }
    # The hung item ends about 150 s after its start; its part then writes
    # a result to a job that is closed already.
    wait_until 150 hung_item_written \
        || { cfail "no record of ${HUNG_ITEM} of ${RID} on ${ITEMS_TOPIC} within $(seconds_between "$produced_at" "$(epoch_now)")s of the produce ($(read_error))"; return; }
    wait_until 90 late_result_written \
        || { cfail "no result record of ${PARTITION}:${OFFSET} after its closed record in the results log within 90s of the hung item's record ($(read_error))"; return; }
    facts=$(dlq_facts "$RID" "$value" aggregate "job timed out after") \
        || { cfail "after the late result: ${facts}"; return; }
    reason=$(no_summary "$RID") || { cfail "after the late result: ${reason}"; return; }
    reason=$(job_stayed_closed) || { cfail "after the late result: ${reason}"; return; }
    wait_lag_zero || return
    check_pass "${facts}; no summary; timed out $(seconds_between "$produced_at" "$timed_out_at")s after produce (timeout ${SHORT_JOB_TIMEOUT}); closed as timeout by ${CLOSED_BY}; a late result reached the results log ${LATE_RESULT_MS} ms after the closed record, and the job stayed closed (one open, one closed record, one DLQ record)"
}

# After check 7: the workers go back to the default job timeout.
restore_default_timeout() {
    log "recreating the split workers with the default job timeout"
    start_workers "" || warn "cannot recreate the split workers with the default job timeout"
}

check_rolling_restart() {
    local service summary restarted_at items
    # Items of 15 s: the 60 items take about 75 s on 12 executor slots, so
    # the job is still open when the last of three graceful restarts starts.
    start_large_job 8 15000 || return
    wait_split_done || return
    for service in "${SPLIT_SERVICES[@]}"; do
        logged_anywhere split_job_closed "" "$PREFIX" \
            && { cfail "${JOB} ended before the restart of ${service}: the items are too short"; return; }
        log "restarting ${service}"
        "${SPLIT[@]}" restart --timeout="$STOP_TIMEOUT_SECONDS" "$service" >/dev/null 2>&1 || { cfail "cannot restart ${service}"; return; }
        wait_until "$READY_LIMIT_SECONDS" worker_ready "$service" \
            || { cfail "${service} is not ready within ${READY_LIMIT_SECONDS}s of its restart: $(worker_log_tail "$service")"; return; }
        ok "${service} ready ${WAITED}s after its restart"
    done
    restarted_at=$(epoch_now)
    wait_closed || return
    # Every worker was restarted, so a second summary may come from any two
    # different workers; never three.
    summary=$(check_summaries 2 "${SPLIT_SERVICES[*]}") || { cfail "the summaries of ${RID}: ${summary}"; return; }
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least) || { cfail "the item records of ${RID}: ${items}"; return; }
    wait_lag_zero || return
    check_pass "three restarts done $(seconds_between "$PRODUCED_AT" "$restarted_at")s after produce; ${CLOSED_BY} closed ${JOB} $(seconds_between "$PRODUCED_AT" "$CLOSED_AT")s after produce; ${summary}; ${items}"
}

check_owner_detach() {
    local answer code summary asked_at resumed items
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
    summary=$(check_summaries 2 "$OWNER") || { cfail "the summaries of ${RID}: ${summary}"; return; }
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least) || { cfail "the item records of ${RID}: ${items}"; return; }
    answer=$(post_route "$OWNER" attach '{}')
    code=${answer##*$'\n'}
    [ "$code" = 202 ] || { cfail "POST /api/v1/debug/attach on ${OWNER} answered ${code}: ${answer%$'\n'*}"; return; }
    wait_until 60 detach_state_is "$OWNER" attached \
        || { cfail "${OWNER} is not attached within 60s (state $(detach_state "$OWNER"))"; return; }
    wait_lag_zero || return
    check_pass "the owner ${OWNER} detached; ${resumed} resumed ${JOB}; ${CLOSED_BY} closed it $(seconds_between "$asked_at" "$CLOSED_AT")s after the detach request; ${summary}; ${items}"
}

# The main cluster's workers (worker-1 to worker-3) answer /readyz with 200.
main_cluster_ready() {
    local port
    for port in "${MAIN_WORKER_API_PORTS[@]}"; do
        curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${port}/readyz" || return 1
    done
}

check_broker_pause() {
    local before after summary service paused_at items
    start_large_job 10 4000 || return
    wait_split_done || return
    logged_anywhere split_job_closed "" "$PREFIX" && { cfail "${JOB} ended before the pause: the items are too short"; return; }
    before=$(restart_snapshot "${SPLIT_SERVICES[@]}")
    # The broker is shared: the pause also stops the main cluster and the
    # other workers of the harness for 60 s.
    docker pause "$(container_id kafka)" >/dev/null || { cfail "cannot pause the broker"; return; }
    paused_at=$(epoch_now)
    log "broker paused for 60 s"
    # The length of the outage under test.
    sleep 60
    docker unpause "$(container_id kafka)" >/dev/null || { cfail "cannot unpause the broker"; return; }
    log "broker unpaused"
    wait_closed || return
    # No worker was stopped or moved, so a second summary needs a worker
    # that took the job over (split_job_resumed or split_attempt_started
    # after a rebalance at the end of the pause).
    summary=$(check_summaries 2 "") || { cfail "the summaries of ${RID}: ${summary}"; return; }
    items=$(wait_items "$RID" "$LARGE_ITEMS" at-least) || { cfail "the item records of ${RID}: ${items}"; return; }
    for service in "${SPLIT_SERVICES[@]}"; do
        container_running "$service" || { cfail "${service} is not running after the broker pause"; return; }
    done
    after=$(restart_snapshot "${SPLIT_SERVICES[@]}")
    [ "$before" = "$after" ] || { cfail "a split worker restarted during the broker pause: before ${before}; after ${after}"; return; }
    wait_lag_zero || return
    # Another scenario runs right after this one in the matrix.
    wait_until "$READY_LIMIT_SECONDS" main_cluster_ready \
        || { cfail "the main cluster's workers do not answer /readyz within ${READY_LIMIT_SECONDS}s of the broker pause"; return; }
    check_pass "${CLOSED_BY} closed ${JOB} $(seconds_between "$paused_at" "$CLOSED_AT")s after the pause began (60 s pause); ${summary}; ${items}; all three workers kept running; the main cluster ready again"
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
