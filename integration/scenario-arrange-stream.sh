#!/usr/bin/env bash
# ============================================================================
# Scenario: a stream worker holds the pending tasks of large messages near
# its limit, delivers every group and message record in order, and runs a
# message from its first group again after a restart.
#
# The compose service stream-worker (profile stream) runs the handler of
# integration/stream-worker/: arrange_stream() yields the groups of a
# request one at a time, every task carries its group's name as the label
# `group`, on_group_complete() writes notes['done'] and sends one `group`
# record, and on_message_complete() sends one `final` record with the sum of
# notes['done']. Each record carries the run_id of its worker process. The
# source topic stream-requests and the sink topic stream-records have one
# partition each, so the messages stream one after the other, and the sink
# records are read in delivery order. executor.max_pending_tasks is 40.
#
# The scenario starts the worker, then:
#   1. produces 3 requests of 30 groups of 10 tasks. Until the worker has
#      delivered their 93 records and has no pending task, it samples
#      drakkar_pending_tasks every SAMPLE_SECONDS (each sample is one read
#      of the metrics page; a peak shorter than the sampling interval can be
#      missed). The samples stay at most the limit plus one group (50), and
#      the peak reaches the limit; drakkar_window_admission_wait_seconds_total
#      {limit="tasks"} is above 0, so the stream waited for room;
#   2. reads stream-records: 90 group records (each group of each request
#      once, 10 tasks, 10 succeeded, every task labelled with its group) and
#      3 final records (30 groups, done total 300), each final after the 30
#      group records of its request; drakkar_stream_group_tasks_count is 90
#      and drakkar_stream_group_tasks_sum is 900;
#   3. produces a fourth request and stops the worker (a graceful stop) once
#      at least RESTART_AFTER_GROUPS of its groups were delivered and before
#      its final record; the worker logs arrange_stream_abandoned. It starts
#      the worker again and waits for the final record of that request;
#   4. reads stream-records: the records of the fourth request come from two
#      runs, the first run before the second. The first run delivered at
#      least RESTART_AFTER_GROUPS group records and no final record. The
#      second run delivered each of the 30 groups once, every group of the
#      first run among them (the message ran from its first group again),
#      then its one final record. The first 3 requests were not run again:
#      still 30 group records and one final record each.
#
# Runs with every provider; the stream worker uses none.
#
# Usage: HARNESS_PROVIDER=filesystem integration/scenario-arrange-stream.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-arrange-stream
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

STREAM_SERVICE=stream-worker
STREAM_API_PORT=8095
STREAM_METRICS_PORT=9101
SOURCE_TOPIC=stream-requests
SINK_TOPIC=stream-records
# The labels of the sink's delivery counter; the metrics page writes them in
# any order, so each is matched on its own.
SINK_TYPE_LABEL='sink_type="kafka"'
SINK_NAME_LABEL='sink_name="stream_records"'
GROUPS_PER_REQUEST=30
TASKS_PER_GROUP=10
TASK_SECONDS=0.3
FIRST_REQUESTS=3
# Stop the worker after this many group records of the fourth request.
RESTART_AFTER_GROUPS=8
READY_LIMIT_SECONDS=240
# 3 requests of 300 tasks of 0.3 s on 4 executors run in about 70 s.
RUN_LIMIT_SECONDS=360
STOP_TIMEOUT_SECONDS=60
SAMPLE_SECONDS=0.25
# The console consumer stops after this long without a new record.
CONSUMER_IDLE_MS=10000
RUN_TOKEN="$(date +%s)-$$"

# Stop and remove the stream worker.
cleanup() {
    "${COMPOSE[@]}" --profile=stream rm --stop --force "$STREAM_SERVICE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

stream_ready() {
    curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${STREAM_API_PORT}/readyz"
}

# The last lines of the stream worker's log, on one line.
stream_log_tail() {
    "${COMPOSE[@]}" --profile=stream logs --no-color --tail=30 "$STREAM_SERVICE" 2>&1 | tr '\n' ' ' | cut -c1-2000
}

# The request id of request $1 of this run.
request_id() {
    echo "stream-${RUN_TOKEN}-$1"
}

# Produce request $1 of this run into the source topic.
produce_request() {
    local id
    id=$(request_id "$1")
    printf '%s\t{"request_id": "%s", "groups": %d, "tasks_per_group": %d, "task_seconds": %s}\n' \
        "$id" "$id" "$GROUPS_PER_REQUEST" "$TASKS_PER_GROUP" "$TASK_SECONDS" \
        | "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server=localhost:9092 \
            --topic="$SOURCE_TOPIC" \
            --property=parse.key=true \
            --property=key.separator=$'\t' \
            --producer-property=acks=all
}

# One sample of the stream worker as "<pending tasks> <payloads delivered to
# the stream_records sink>", or "unreachable" when the metrics page could
# not be read. awk keeps a sample short, so the samples come close together.
stream_sample() {
    local page
    page=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:${STREAM_METRICS_PORT}/metrics") || { echo unreachable; return 0; }
    awk -v type_label="$SINK_TYPE_LABEL" -v name_label="$SINK_NAME_LABEL" '
        $1 == "drakkar_pending_tasks" { pending = $2 }
        index($1, "drakkar_sink_payloads_delivered_total{") == 1 && index($1, type_label) > 0 && index($1, name_label) > 0 { delivered += $2 }
        END { printf "%d %d\n", pending, delivered }' <<<"$page"
}

# Sample until the stream worker delivered at least $1 sink payloads and,
# when $2 is "idle", has no pending task, or fail after $3 seconds. Each
# sample must be at most TASK_BOUND pending tasks. Updates PEAK, SAMPLES and
# UNREADABLE; sets DELIVERED to the last delivered count.
sample_until() {
    local wanted=$1 idle=$2 limit=$3 started=$SECONDS sample pending
    DELIVERED=0
    while true; do
        sample=$(stream_sample)
        if [ "$sample" = unreachable ]; then
            UNREADABLE=$((UNREADABLE + 1))
        else
            read -r pending DELIVERED <<<"$sample"
            SAMPLES=$((SAMPLES + 1))
            [ "$pending" -gt "$PEAK" ] && PEAK=$pending
            [ "$pending" -le "$TASK_BOUND" ] \
                || fail "${STREAM_SERVICE} has ${pending} pending tasks, above the limit ${TASK_LIMIT} plus one group (${TASKS_PER_GROUP})"
            if [ "$DELIVERED" -ge "$wanted" ] && { [ "$idle" != idle ] || [ "$pending" = 0 ]; }; then
                return 0
            fi
        fi
        [ $((SECONDS - started)) -lt "$limit" ] \
            || fail "${STREAM_SERVICE} delivered ${DELIVERED} of ${wanted} records within ${limit}s (its log: $(stream_log_tail))"
        sleep "$SAMPLE_SECONDS"
    done
}

# Every record of the sink topic, one line each: <key> TAB <JSON value>.
sink_records() {
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server=localhost:9092 --topic="$SINK_TOPIC" --from-beginning \
        --property=print.key=true --property=key.separator=$'\t' \
        --timeout-ms="$CONSUMER_IDLE_MS" 2>/dev/null || true
}

# Check the records on stdin. Prints one summary line, or the first failed
# check and exits 1.
#   check_records <mode: first|restart>
check_records() {
    MODE="$1" TOKEN="$RUN_TOKEN" GROUP_COUNT="$GROUPS_PER_REQUEST" TASKS="$TASKS_PER_GROUP" \
        FIRST="$FIRST_REQUESTS" MIN_FIRST_RUN="$RESTART_AFTER_GROUPS" python3 -c '
import json, os, sys

groups, tasks, first = int(os.environ["GROUP_COUNT"]), int(os.environ["TASKS"]), int(os.environ["FIRST"])
prefix = "stream-" + os.environ["TOKEN"] + "-"
records = []
for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("\t")
    if not key.startswith(prefix):
        continue
    record = json.loads(value)
    carried = record["request_id"]
    if carried != key:
        sys.exit(f"record key {key} carries request_id {carried}")
    records.append(record)

def bad(text):
    sys.exit(text)

def check_group(record):
    if (record["task_count"], record["succeeded"], record["failed"], record["labels_match"]) != (tasks, tasks, 0, True):
        bad(f"group record {record} is not {tasks} succeeded tasks all labelled with the group")

def check_final(record):
    if (record["group_count"], record["done_total"]) != (groups, groups * tasks):
        bad(f"final record {record} is not {groups} groups with a done total of {groups * tasks}")

def check_complete_run(request, run_records):
    group_records = [r for r in run_records if r["kind"] == "group"]
    finals = [r for r in run_records if r["kind"] == "final"]
    indices = sorted(r["group_index"] for r in group_records)
    if indices != list(range(groups)):
        bad(f"{request}: group indices {indices} are not 0..{groups - 1} once each")
    if len(finals) != 1:
        bad(f"{request}: {len(finals)} final records, expected 1")
    if run_records[-1]["kind"] != "final":
        bad(f"{request}: the final record is not after the {groups} group records")
    for r in group_records:
        check_group(r)
    check_final(finals[0])

by_request = {}
for record in records:
    by_request.setdefault(record["request_id"], []).append(record)

summary = []
for n in range(1, first + 1):
    request = f"{prefix}{n}"
    own = by_request.get(request, [])
    check_complete_run(request, own)
    if len({r["run_id"] for r in own}) != 1:
        bad(f"{request}: records of more than one run")
total_groups = sum(1 for r in records if r["kind"] == "group" and not r["request_id"].endswith(f"-{first + 1}"))
total_finals = sum(1 for r in records if r["kind"] == "final" and not r["request_id"].endswith(f"-{first + 1}"))
summary.append(f"{total_groups} group and {total_finals} final records of the first {first} requests")

if os.environ["MODE"] == "restart":
    request = f"{prefix}{first + 1}"
    own = by_request.get(request, [])
    runs = list(dict.fromkeys(r["run_id"] for r in own))
    if len(runs) != 2:
        bad(f"{request}: records of {len(runs)} runs {runs}, expected 2 (before and after the restart)")
    first_run = [r for r in own if r["run_id"] == runs[0]]
    second_run = [r for r in own if r["run_id"] == runs[1]]
    if own != first_run + second_run:
        bad(f"{request}: the records of the two runs are mixed")
    if any(r["kind"] == "final" for r in first_run):
        bad(f"{request}: the run before the restart delivered a final record")
    first_indices = sorted(r["group_index"] for r in first_run)
    if len(first_indices) < int(os.environ["MIN_FIRST_RUN"]):
        bad(f"{request}: only {len(first_indices)} group records before the restart")
    if len(set(first_indices)) != len(first_indices):
        bad(f"{request}: the run before the restart delivered a group twice: {first_indices}")
    for r in first_run:
        check_group(r)
    check_complete_run(request, second_run)
    replayed = sorted(set(first_indices) & {r["group_index"] for r in second_run if r["kind"] == "group"})
    if replayed != sorted(set(first_indices)):
        bad(f"{request}: groups {sorted(set(first_indices) - set(replayed))} of the first run did not run again")
    summary.append(
        f"{request}: {len(first_run)} group records before the restart (indices {first_indices[0]}..{first_indices[-1]}), "
        f"all {groups} groups again after it, then 1 final record"
    )
print("; ".join(summary))
'
}

step "Scenario: arrange_stream on ${STREAM_SERVICE}"

TASK_LIMIT=$("${COMPOSE[@]}" --profile=stream config --format=json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["stream-worker"]["environment"]["DK_EXECUTOR__MAX_PENDING_TASKS"])') \
    || fail "docker-compose.yml gives ${STREAM_SERVICE} no DK_EXECUTOR__MAX_PENDING_TASKS"
TASK_BOUND=$((TASK_LIMIT + TASKS_PER_GROUP))
PEAK=0
SAMPLES=0
UNREADABLE=0
first_records=$(((GROUPS_PER_REQUEST + 1) * FIRST_REQUESTS))
log "limit ${TASK_LIMIT} pending tasks, groups of ${TASKS_PER_GROUP}: allowed peak ${TASK_BOUND}"

cleanup
"${COMPOSE[@]}" --profile=stream up --detach --build "$STREAM_SERVICE" >/dev/null 2>&1 \
    || fail "docker compose could not start ${STREAM_SERVICE} (profile stream)"
wait_until "$READY_LIMIT_SECONDS" stream_ready \
    || fail "${STREAM_SERVICE} does not answer /readyz with 200 within ${READY_LIMIT_SECONDS}s (its log: $(stream_log_tail))"
ok "${STREAM_SERVICE} ready after ${WAITED}s"

step "Three requests of ${GROUPS_PER_REQUEST} groups of ${TASKS_PER_GROUP} tasks"
for n in $(seq 1 "$FIRST_REQUESTS"); do
    produce_request "$n" || fail "cannot produce request ${n} into ${SOURCE_TOPIC}"
done
started=$SECONDS
sample_until "$first_records" idle "$RUN_LIMIT_SECONDS"
ok "${DELIVERED} records delivered in $((SECONDS - started))s; ${SAMPLES} samples every ${SAMPLE_SECONDS}s (${UNREADABLE} unreadable), peak ${PEAK} pending tasks (allowed ${TASK_BOUND})"
[ "$PEAK" -ge "$TASK_LIMIT" ] \
    || fail "the peak of the pending tasks is ${PEAK}, below the limit ${TASK_LIMIT}: the stream never filled the budget"
admission_wait=$(metric "$STREAM_METRICS_PORT" drakkar_window_admission_wait_seconds_total 'limit="tasks"')
case "$admission_wait" in
    absent | unreachable) fail "${STREAM_SERVICE} has no drakkar_window_admission_wait_seconds_total{limit=\"tasks\"} sample (${admission_wait})" ;;
esac
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "$admission_wait" \
    || fail "the stream never waited for room at the task limit (drakkar_window_admission_wait_seconds_total{limit=\"tasks\"} is ${admission_wait})"
ok "the stream waited ${admission_wait}s in total for room at the task limit"

expected_groups=$((GROUPS_PER_REQUEST * FIRST_REQUESTS))
metric_equals "$STREAM_METRICS_PORT" drakkar_stream_group_tasks_count '' "$expected_groups" \
    || fail "drakkar_stream_group_tasks_count is $(metric "$STREAM_METRICS_PORT" drakkar_stream_group_tasks_count), expected ${expected_groups}"
metric_equals "$STREAM_METRICS_PORT" drakkar_stream_group_tasks_sum '' "$((expected_groups * TASKS_PER_GROUP))" \
    || fail "drakkar_stream_group_tasks_sum is $(metric "$STREAM_METRICS_PORT" drakkar_stream_group_tasks_sum), expected $((expected_groups * TASKS_PER_GROUP))"
ok "drakkar_stream_group_tasks: ${expected_groups} groups, $((expected_groups * TASKS_PER_GROUP)) tasks"

first_summary=$(sink_records | check_records first) || fail "the records on ${SINK_TOPIC}: ${first_summary:-see above}"
ok "$first_summary, each final after the ${GROUPS_PER_REQUEST} group records of its request"

step "A restart in the middle of a message"
restart_request=$((FIRST_REQUESTS + 1))
produce_request "$restart_request" || fail "cannot produce request ${restart_request} into ${SOURCE_TOPIC}"
sample_until "$((first_records + RESTART_AFTER_GROUPS))" any "$RUN_LIMIT_SECONDS"
[ "$DELIVERED" -lt "$((first_records + GROUPS_PER_REQUEST))" ] \
    || fail "request ${restart_request} had delivered ${DELIVERED} records before the stop: too late to stop in the middle"
ok "$((DELIVERED - first_records)) group records of request ${restart_request} delivered; stopping ${STREAM_SERVICE}"
"${COMPOSE[@]}" --profile=stream stop --timeout="$STOP_TIMEOUT_SECONDS" "$STREAM_SERVICE" >/dev/null 2>&1 \
    || fail "docker compose could not stop ${STREAM_SERVICE}"
abandoned=$("${COMPOSE[@]}" --profile=stream logs --no-color --no-log-prefix "$STREAM_SERVICE" 2>&1 | grep -c '"arrange_stream_abandoned"' || true)
[ "$abandoned" -ge 1 ] || fail "${STREAM_SERVICE} logged no arrange_stream_abandoned at the stop (its log: $(stream_log_tail))"
ok "${STREAM_SERVICE} stopped and logged arrange_stream_abandoned"
"${COMPOSE[@]}" --profile=stream start "$STREAM_SERVICE" >/dev/null 2>&1 \
    || fail "docker compose could not start ${STREAM_SERVICE} again"
wait_until "$READY_LIMIT_SECONDS" stream_ready \
    || fail "${STREAM_SERVICE} does not answer /readyz with 200 within ${READY_LIMIT_SECONDS}s of the restart (its log: $(stream_log_tail))"
started=$SECONDS
sample_until "$((GROUPS_PER_REQUEST + 1))" idle "$RUN_LIMIT_SECONDS"
ok "the restarted worker delivered ${DELIVERED} records in $((SECONDS - started))s; peak over the whole scenario ${PEAK} pending tasks"

restart_summary=$(sink_records | check_records restart) || fail "the records on ${SINK_TOPIC}: ${restart_summary:-see above}"
ok "$restart_summary"

pass "peak ${PEAK} pending tasks (limit ${TASK_LIMIT}, allowed ${TASK_BOUND}, ${SAMPLES} samples every ${SAMPLE_SECONDS}s), admission wait ${admission_wait}s; ${restart_summary}"
