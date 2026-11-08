#!/usr/bin/env bash
# ============================================================================
# Scenario: the backpressure task cap holds under load.
#
# Only the task cap is exercised. The byte cap cannot be reached by this
# handler (16 MiB, the least the config allows, against at most a few
# pending tasks with stdin under 1 KiB), so the byte check below is a
# sanity bound, not a test of the byte path.
#
# fast-worker-1 runs with small caps (docker-compose.yml):
# executor.max_pending_tasks 4 and executor.max_pending_bytes 16MiB. The fast
# handler makes one task per file path, and a producer message has one or
# two paths, so one message's fan-out is at most two tasks. The caps are soft
# by about one message: the pending tasks may reach the cap plus two. The
# burst and flood phases of the producer send messages faster than
# fast-worker-1's two executors run them, so the cap fills, and the fast
# cluster works its backlog off for minutes after PRODUCER_DONE.
#
# The check does not depend on where it runs in the scenario list: the load
# is over by the time most scenarios run (the outage scenario, which needs
# the load, comes first). Prometheus keeps the gauge of the whole run. It
# checks, in order:
#   1. fast-worker-1 answers /readyz with 200,
#   2. until the producer printed PRODUCER_DONE and the consumer group of the
#      fast cluster (drakkar-fast) has a lag of 0 on the source topic (at
#      once when both already hold), the samples of drakkar_pending_tasks on
#      fast-worker-1, taken every 0.5 s, stay at most the task cap plus one
#      message's fan-out, and those of drakkar_pending_bytes at most the byte
#      cap plus one message's bytes (a task's stdin here is under 1 KiB; the
#      margin allows 64 KiB),
#   3. the highest drakkar_pending_tasks that Prometheus scraped from
#      fast-worker-1 (every 10 s) over the last 6 hours is at least the cap
#      and at most the cap plus one message's fan-out: the gauge stayed near
#      the cap, and never above the bound, during the load,
#   4. drakkar_window_admission_wait_seconds_total{limit="tasks"} on
#      fast-worker-1 is above 0: admission waited at the task cap,
#   5. delivery of the fast cluster: every produced request id is a key on
#      the symbol-counts topic (the per-message sink of the fast handler).
#      The main cluster's delivery is checked by verify-after-drain.sh.
#
# Runs with every provider (the fast cluster's registry is always
# filesystem).
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-backpressure.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-backpressure
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

CAPPED_SERVICE=fast-worker-1
CAPPED_UI_PORT=8084
CAPPED_METRICS_PORT=9094
# The scrape target of fast-worker-1 in infra/prometheus.yml.
CAPPED_PROMETHEUS_INSTANCE=fast-worker-1:9094
PROMETHEUS_URL=http://localhost:9099
# The most tasks one message gives: the fast handler makes one task per file
# path, and the producer sends one or two paths per message.
FAN_OUT_TASKS=2
# The bytes one message of the fast handler may hold: its stdin, its kept
# output and their bookkeeping, with a wide margin.
MESSAGE_BYTES_MARGIN=65536
FAST_CONSUMER_GROUP=drakkar-fast
SOURCE_TOPIC=search-requests
SINK_TOPIC=symbol-counts
READY_LIMIT_SECONDS=120
# The paced producer phases take about 7 minutes for 5000 messages; the
# limit leaves room for a slow start of the producer (the scenario run
# alone on a fresh harness).
PRODUCER_DONE_LIMIT_SECONDS=2700
# Under the small caps fast-worker-1 runs about 6 tasks per second, so the
# fast cluster needs about 5 minutes after PRODUCER_DONE with the quick
# profile and five times that with the full one; the limit is wide.
FAST_DRAIN_LIMIT_SECONDS=3600
SAMPLE_SECONDS=0.5
LAG_CHECK_EVERY_SECONDS=10
# The console consumer stops after this long without a new record.
CONSUMER_IDLE_MS=20000

# One environment value of the capped service from the compose config.
capped_env() {
    "${COMPOSE[@]}" config --format=json \
        | SERVICE="$CAPPED_SERVICE" KEY="$1" python3 -c 'import json, os, sys; print(json.load(sys.stdin)["services"][os.environ["SERVICE"]]["environment"][os.environ["KEY"]])'
}

# A size such as 16MiB, 1GiB or 1048576 in bytes.
size_in_bytes() {
    python3 -c '
import re, sys
match = re.fullmatch(r"(\d+)\s*(B|KiB|MiB|GiB)?", sys.argv[1])
if not match:
    sys.exit(f"cannot read the size {sys.argv[1]!r}")
units = {None: 1, "B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
print(int(match.group(1)) * units[match.group(2)])
' "$1"
}

capped_ready() {
    curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${CAPPED_UI_PORT}/readyz"
}

# The distinct request ids of the producer's PRODUCER_DONE line; empty
# while it is still sending.
producer_done_count() {
    "${COMPOSE[@]}" logs --no-color producer 2>/dev/null \
        | sed -n 's/.*PRODUCER_DONE distinct_request_ids=\([0-9]*\).*/\1/p' | tail -n 1
}

# The summed lag of the fast cluster's consumer group on the source topic;
# `unknown` while the group is rebalancing or a partition has no committed
# offset yet.
fast_lag() {
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server=localhost:9092 --describe --group="$FAST_CONSUMER_GROUP" 2>/dev/null \
        | awk -v topic="$SOURCE_TOPIC" '
            $2 == topic { rows++; if ($6 ~ /^[0-9]+$/) lag += $6; else unknown = 1 }
            END { if (rows == 0 || unknown) print "unknown"; else print lag + 0 }'
}

# The pending tasks and bytes of the capped worker as "<tasks> <bytes>", or
# "unreachable" when its metrics page could not be read.
pending_sample() {
    local page
    page=$(curl -sf --connect-timeout 2 --max-time 3 "http://localhost:${CAPPED_METRICS_PORT}/metrics") || { echo unreachable; return 0; }
    python3 -c '
import re, sys
values = {}
for line in sys.stdin:
    match = re.match(r"^(drakkar_pending_tasks|drakkar_pending_bytes)(?:\{[^}]*\})?\s+(\S+)", line)
    if match:
        values[match.group(1)] = float(match.group(2))
print(int(values.get("drakkar_pending_tasks", -1)), int(values.get("drakkar_pending_bytes", -1)))
' <<<"$page"
}

# The highest drakkar_pending_tasks Prometheus scraped from the capped worker
# in the last 6 hours; nothing when Prometheus does not answer or has no
# sample.
prometheus_max_pending_tasks() {
    local answer
    # curl's long options take no `=`, so they keep the space form here.
    answer=$(curl -sf --connect-timeout 2 --max-time 5 -G "${PROMETHEUS_URL}/api/v1/query" \
        --data-urlencode "query=max_over_time(drakkar_pending_tasks{instance=\"${CAPPED_PROMETHEUS_INSTANCE}\"}[6h])") || return 0
    python3 -c '
import json, sys
result = json.load(sys.stdin).get("data", {}).get("result", [])
if result:
    print(int(float(result[0]["value"][1])))
' <<<"$answer"
}

step "Scenario: backpressure on ${CAPPED_SERVICE}"

task_cap=$(capped_env DK_EXECUTOR__MAX_PENDING_TASKS) || fail "docker-compose.yml gives ${CAPPED_SERVICE} no DK_EXECUTOR__MAX_PENDING_TASKS"
byte_cap_text=$(capped_env DK_EXECUTOR__MAX_PENDING_BYTES) || fail "docker-compose.yml gives ${CAPPED_SERVICE} no DK_EXECUTOR__MAX_PENDING_BYTES"
byte_cap=$(size_in_bytes "$byte_cap_text") || fail "cannot read DK_EXECUTOR__MAX_PENDING_BYTES '${byte_cap_text}' of ${CAPPED_SERVICE}"
task_bound=$((task_cap + FAN_OUT_TASKS))
byte_bound=$((byte_cap + MESSAGE_BYTES_MARGIN))
TOTAL_MESSAGES=$(harness_total_messages) || fail "cannot read the message total from the producer"
log "caps of ${CAPPED_SERVICE}: ${task_cap} tasks, ${byte_cap_text}; allowed peak ${task_bound} tasks, ${byte_bound} bytes"

wait_until "$READY_LIMIT_SECONDS" capped_ready \
    || fail "${CAPPED_SERVICE} does not answer /readyz with 200 within ${READY_LIMIT_SECONDS}s (its log: $("${COMPOSE[@]}" logs --no-color --tail=20 "$CAPPED_SERVICE" 2>&1 | tr '\n' ' ' | cut -c1-1500))"
ok "${CAPPED_SERVICE} ready"

log "sampling the pending tasks and bytes every ${SAMPLE_SECONDS}s until the producer is done and ${FAST_CONSUMER_GROUP} has worked off its lag"
started=$SECONDS
samples=0
unreachable=0
max_tasks=0
max_bytes=0
next_lag_check=$SECONDS
done_at=""
lag=unknown
while true; do
    sample=$(pending_sample)
    if [ "$sample" = unreachable ]; then
        unreachable=$((unreachable + 1))
    else
        read -r tasks bytes <<<"$sample"
        samples=$((samples + 1))
        [ "$tasks" -gt "$max_tasks" ] && max_tasks=$tasks
        [ "$bytes" -gt "$max_bytes" ] && max_bytes=$bytes
        [ "$tasks" -le "$task_bound" ] \
            || fail "${CAPPED_SERVICE} has ${tasks} pending tasks, above the cap ${task_cap} plus one message's fan-out (${FAN_OUT_TASKS}), $((SECONDS - started))s into the sampling"
        [ "$bytes" -le "$byte_bound" ] \
            || fail "${CAPPED_SERVICE} has ${bytes} pending bytes, above the cap ${byte_cap} plus ${MESSAGE_BYTES_MARGIN}, $((SECONDS - started))s into the sampling"
    fi
    if [ "$SECONDS" -ge "$next_lag_check" ]; then
        next_lag_check=$((SECONDS + LAG_CHECK_EVERY_SECONDS))
        if [ -z "$done_at" ] && [ -n "$(producer_done_count)" ]; then
            done_at=$SECONDS
            log "producer done after $((SECONDS - started))s of sampling; peak so far ${max_tasks} tasks, ${max_bytes} bytes"
        fi
        if [ -n "$done_at" ]; then
            lag=$(fast_lag)
            [ "$lag" = 0 ] && break
            [ $((SECONDS - done_at)) -lt "$FAST_DRAIN_LIMIT_SECONDS" ] \
                || fail "the lag of ${FAST_CONSUMER_GROUP} is not 0 ${FAST_DRAIN_LIMIT_SECONDS}s after PRODUCER_DONE (lag: ${lag})"
        else
            [ $((SECONDS - started)) -lt "$PRODUCER_DONE_LIMIT_SECONDS" ] \
                || fail "the producer did not print PRODUCER_DONE within ${PRODUCER_DONE_LIMIT_SECONDS}s"
        fi
    fi
    sleep "$SAMPLE_SECONDS"
done
sampled_for=$((SECONDS - started))
[ "$samples" -gt 0 ] || fail "no metrics sample of ${CAPPED_SERVICE} could be read in ${sampled_for}s"
ok "${samples} samples in ${sampled_for}s (${unreachable} unreadable): peak ${max_tasks} tasks (allowed ${task_bound}), ${max_bytes} bytes (allowed ${byte_bound}); ${FAST_CONSUMER_GROUP} drained $((SECONDS - done_at))s after PRODUCER_DONE"

prometheus_peak=$(prometheus_max_pending_tasks)
[ -n "$prometheus_peak" ] \
    || fail "Prometheus (${PROMETHEUS_URL}) gave no max_over_time sample of drakkar_pending_tasks for ${CAPPED_PROMETHEUS_INSTANCE}"
[ "$prometheus_peak" -le "$task_bound" ] \
    || fail "Prometheus scraped ${prometheus_peak} pending tasks from ${CAPPED_SERVICE}, above the cap ${task_cap} plus one message's fan-out (${FAN_OUT_TASKS})"
[ "$prometheus_peak" -ge "$task_cap" ] \
    || fail "the highest pending tasks Prometheus scraped from ${CAPPED_SERVICE} is ${prometheus_peak}, below the cap ${task_cap}: the load never filled the cap"
ok "Prometheus peak over the run: ${prometheus_peak} pending tasks (cap ${task_cap}, allowed ${task_bound})"

admission_wait=$(metric "$CAPPED_METRICS_PORT" drakkar_window_admission_wait_seconds_total 'limit="tasks"')
case "$admission_wait" in
    absent | unreachable) fail "${CAPPED_SERVICE} has no drakkar_window_admission_wait_seconds_total{limit=\"tasks\"} sample (${admission_wait})" ;;
esac
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "$admission_wait" \
    || fail "admission never waited at the task cap of ${CAPPED_SERVICE} (drakkar_window_admission_wait_seconds_total{limit=\"tasks\"} is ${admission_wait}): the load did not fill the cap, so the bound was not tested"
ok "admission waited ${admission_wait}s in total at the task cap"

log "reading the keys of ${SINK_TOPIC}"
keys=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server=localhost:9092 --topic="$SINK_TOPIC" --from-beginning \
    --property=print.key=true --property=print.value=false --timeout-ms="$CONSUMER_IDLE_MS" 2>/dev/null) || true
delivery=$(TOTAL="$TOTAL_MESSAGES" python3 -c '
import os, sys
total = int(os.environ["TOTAL"])
keys = {line.strip() for line in sys.stdin if line.strip()}
missing = [f"req-{n:06d}" for n in range(1, total + 1) if f"req-{n:06d}" not in keys]
print(len(keys), len(missing), " ".join(missing[:10]))
' <<<"$keys")
read -r distinct_keys missing_count missing_sample <<<"$delivery"
[ "$missing_count" = 0 ] \
    || fail "${missing_count} of ${TOTAL_MESSAGES} request ids have no record on ${SINK_TOPIC} (${distinct_keys} distinct keys); first missing: ${missing_sample}"
ok "every one of ${TOTAL_MESSAGES} request ids is on ${SINK_TOPIC} (${distinct_keys} distinct keys)"

pass "task cap: peak ${max_tasks} pending tasks (cap ${task_cap}, allowed ${task_bound}); byte sanity bound only: peak ${max_bytes} pending bytes (cap ${byte_cap}, not reachable here); Prometheus peak ${prometheus_peak}, admission wait ${admission_wait}s at the task cap, ${TOTAL_MESSAGES} request ids delivered to ${SINK_TOPIC}"
