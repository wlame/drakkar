#!/usr/bin/env bash
# ============================================================================
# Scenario: each slot of a cluster task runs on exactly one worker.
#
# The harness handler declares harness_cluster_tick, a cluster periodic task
# with a slot every 30 s of epoch time; each run logs harness_cluster_tick
# with its slot. worker-1, worker-2 and worker-3 all declare it, and the
# election provider of the run decides which of them runs each slot. The
# scenario:
#   - waits for the next slot boundary S0 and watches 8 whole slots (S0 to
#     S0 + 210) and 15 s after the last of them ends, so the run of every
#     watched slot has logged its line,
#   - reads the harness_cluster_tick lines of the three workers and fails
#     when a slot ran more than once,
#   - fails when fewer than 7 of the 8 whole slots ran exactly once (a slot
#     whose lease does not come within its acquire window is skipped, not
#     run late, so one missed slot is allowed),
#   - reads /api/v1/workers on worker-2 (UI port 8082) and fails unless the
#     row of worker-1 has cluster_tasks_match false and the rows of worker-3
#     and of worker-2 itself have true. worker-1 turns the archive off
#     (DK_RECORDER__ARCHIVE__ENABLED: "false" in docker-compose.yml), so its
#     cluster task definitions differ. worker-3 matches only because worker-2
#     and worker-3 run the archive with the same keys and the same handler: a
#     config change on one of them must make this check fail.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-cluster-tasks.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-cluster-tasks
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

# The schedule of harness_cluster_tick in the harness handler.
TICK_INTERVAL_SECONDS=30
WATCHED_SLOTS=8
# At least this many of the watched slots must run exactly once.
MIN_SLOTS_RUN_ONCE=7
# The time after the last watched slot ends, for its run to log its line.
LOG_GRACE_SECONDS=15
# The worker whose workers list is read, and how long the list may take to
# show the expected verdicts.
OBSERVER_UI_PORT=8082
OBSERVER_WORKER="worker-2"
API_LIMIT_SECONDS=20
# The expected cluster_tasks_match of each row in the observer's list.
EXPECTED_MATCHES=("worker-1 false" "worker-3 true" "worker-2 true")

step "Scenario: each slot of harness_cluster_tick runs on one worker"

since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
first_slot=$(next_slot_boundary "$TICK_INTERVAL_SECONDS")
last_slot=$((first_slot + (WATCHED_SLOTS - 1) * TICK_INTERVAL_SECONDS))
watch_end=$((last_slot + TICK_INTERVAL_SECONDS + LOG_GRACE_SECONDS))
log "watching the slots ${first_slot} to ${last_slot} (${WATCHED_SLOTS} slots) until ${watch_end}"
sleep_until_epoch "$watch_end"

ticks=$(cluster_ticks "$since") || fail "the logs of the main-cluster workers could not be read for harness_cluster_tick lines"
doubled=$(doubled_tick_slots <<<"$ticks")
[ -z "$doubled" ] \
    || fail "slots of harness_cluster_tick ran twice (slot workers): $(tr '\n' ' ' <<<"$doubled")"
ok "no slot of harness_cluster_tick ran twice"

# One line per watched slot: <slot> <runs> <workers>.
slot_table=$(python3 -c '
import sys
first, interval, count = (int(value) for value in sys.argv[1:4])
workers = {}
for line in sys.stdin:
    fields = line.split()
    if len(fields) == 2:
        workers.setdefault(fields[1], []).append(fields[0])
for index in range(count):
    slot = str(first + index * interval)
    ran = workers.get(slot, [])
    print(slot, len(ran), ",".join(ran) or "-")
' "$first_slot" "$TICK_INTERVAL_SECONDS" "$WATCHED_SLOTS" <<<"$ticks")
log "watched slots (slot runs workers):"
while IFS= read -r row; do
    log "  ${row}"
done <<<"$slot_table"
run_once=$(awk '$2 == 1' <<<"$slot_table" | grep -c . || true)
[ "$run_once" -ge "$MIN_SLOTS_RUN_ONCE" ] \
    || fail "only ${run_once} of the ${WATCHED_SLOTS} watched slots of harness_cluster_tick ran once, expected at least ${MIN_SLOTS_RUN_ONCE} (slot runs workers): $(tr '\n' ';' <<<"$slot_table")"
ok "${run_once} of the ${WATCHED_SLOTS} watched slots ran exactly once"

# True when every row of EXPECTED_MATCHES has its cluster_tasks_match in
# the observer's list.
matches_as_expected() {
    local row worker expected
    for row in "${EXPECTED_MATCHES[@]}"; do
        read -r worker expected <<<"$row"
        [ "$(peer_field "$OBSERVER_UI_PORT" "$worker" cluster_tasks_match)" = "$expected" ] || return 1
    done
}

# The cluster_tasks_match of every row of EXPECTED_MATCHES, as
# <worker>=<value> pairs.
actual_matches() {
    local row worker expected pairs=()
    for row in "${EXPECTED_MATCHES[@]}"; do
        read -r worker expected <<<"$row"
        pairs+=("${worker}=$(peer_field "$OBSERVER_UI_PORT" "$worker" cluster_tasks_match)")
    done
    echo "${pairs[*]}"
}

if ! wait_until "$API_LIMIT_SECONDS" matches_as_expected; then
    actual=$(actual_matches)
    fail "${OBSERVER_WORKER} does not list the expected cluster_tasks_match within ${API_LIMIT_SECONDS}s: expected worker-1=false worker-3=true worker-2=true, got ${actual} (worker-1 must set DK_RECORDER__ARCHIVE__ENABLED: 'false' in docker-compose.yml, worker-2 and worker-3 must not differ)"
fi
ok "${OBSERVER_WORKER} lists $(actual_matches)"

pass "${run_once} of ${WATCHED_SLOTS} slots from ${first_slot} ran once, none twice; ${OBSERVER_WORKER} lists cluster_tasks_match $(actual_matches)"
