#!/usr/bin/env bash
# ============================================================================
# Scenario: the registry's service goes away and comes back.
#
# Pauses the service container of the provider (postgres-fleet for postgres,
# redis-fleet for redis, consul-fleet for consul; the table is in
# scenario-lib.sh) for 90 s while the main cluster runs with that provider,
# twice: a pause after a recovery must behave like the first one, because a
# cancelled call can leave state behind that only a second outage shows. A
# paused container keeps its connections open and answers nothing, so every
# call in flight must end at its own time limit. With consul, the agent ends
# at once on resume every session whose time ran out during the pause (the
# registry's session TTL is 10 s here, so the workers renew sessions that no
# longer exist and create new ones); a lease whose time did not run out is
# kept. Each pause proves that:
#   - worker-1's registry breaker opens within 40 s
#     (drakkar_provider_breaker_open{feature="registry",provider="<provider>"} 1),
#   - within 60 s of the unpause the breaker is closed again and worker-1
#     lists worker-2 and worker-3 online,
#   - no worker container restarted.
# Then it proves that:
#   - every request still reached the sinks (verify-after-drain.sh: the
#     producer printed PRODUCER_DONE, the consumer group's lag is 0, and
#     just verify-delivery passes).
#
# Not applicable with the filesystem provider: it has no service to pause.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-service-outage.sh
# The message total is read from the running producer container.
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-service-outage
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

if [ -z "$PROVIDER_SERVICE" ]; then
    not_applicable "the ${HARNESS_PROVIDER} provider has no service container"
fi

PAUSED_SERVICE=$PROVIDER_SERVICE
WORKER_1_UI_PORT=8081
WORKER_1_METRICS_PORT=9090
BREAKER_METRIC=drakkar_provider_breaker_open
# The provider label of the provider metrics is the provider's name.
REGISTRY_LABELS="feature=\"registry\",provider=\"${HARNESS_PROVIDER}\""
BREAKER_OPEN_LIMIT_SECONDS=40
OUTAGE_SECONDS=90
RECOVERY_LIMIT_SECONDS=60
TASKS_METRIC=drakkar_executor_tasks_total

is_paused=false
# Never leave the service paused, also when a check fails half way.
unpause_on_exit() {
    if [ "$is_paused" = true ]; then
        "${COMPOSE[@]}" unpause "$PAUSED_SERVICE" >/dev/null 2>&1 || true
    fi
}
trap unpause_on_exit EXIT

breaker_is() {
    metric_equals "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS" "$1"
}

peers_online() {
    peer_online_is "$WORKER_1_UI_PORT" worker-2 true && peer_online_is "$WORKER_1_UI_PORT" worker-3 true
}

recovered() {
    breaker_is 0 && peers_online
}

# 1st, 2nd, 3rd for the outage number $1.
ordinal() {
    case "$1" in
        1) echo 1st ;;
        2) echo 2nd ;;
        3) echo 3rd ;;
        *) echo "${1}th" ;;
    esac
}

# True when the counter reading $2 is larger than the reading $1; false when
# either reading could not be taken.
#   tasks_grew <before> <after>
tasks_grew() {
    python3 -c '
import sys
try:
    before, after = float(sys.argv[1]), float(sys.argv[2])
except ValueError:
    sys.exit(1)
sys.exit(0 if after > before else 1)
' "$1" "$2"
}

# One outage: pause the service, see the breaker open, unpause, see the
# breaker close and the peers online, and check that no worker restarted.
# Appends "<opened>s/<recovered>s" for the verdict to OUTAGE_TIMINGS.
#   pause_and_recover <outage number>
OUTAGE_TIMINGS=""
pause_and_recover() {
    local outage=$1 before after tasks_before tasks_after paused_at opened_after recovered_after

    breaker_is 0 || fail "outage ${outage}: the registry breaker is not closed before the pause (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
    peers_online || fail "outage ${outage}: worker-1 does not list worker-2 and worker-3 online before the pause"
    before=$(restart_snapshot "${MAIN_WORKERS[@]}")
    ok "outage ${outage}: breaker closed, worker-2 and worker-3 online"

    log "outage ${outage}: pausing ${PAUSED_SERVICE}"
    tasks_before=$(metric_sum "$WORKER_1_METRICS_PORT" "$TASKS_METRIC")
    "${COMPOSE[@]}" pause "$PAUSED_SERVICE"
    is_paused=true
    paused_at=$SECONDS

    wait_until "$BREAKER_OPEN_LIMIT_SECONDS" breaker_is 1 \
        || fail "outage ${outage}: the registry breaker did not open within ${BREAKER_OPEN_LIMIT_SECONDS}s (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
    opened_after=$WAITED
    ok "outage ${outage}: registry breaker open after ${opened_after}s"

    sleep_until_elapsed "$paused_at" "$OUTAGE_SECONDS"
    tasks_after=$(metric_sum "$WORKER_1_METRICS_PORT" "$TASKS_METRIC")
    tasks_grew "$tasks_before" "$tasks_after" \
        || fail "the pipeline did not process tasks during the $(ordinal "$outage") outage (${TASKS_METRIC}: ${tasks_before} before, ${tasks_after} after)"
    ok "outage ${outage}: worker-1 processed $(python3 -c 'import sys; print(int(float(sys.argv[2]) - float(sys.argv[1])))' "$tasks_before" "$tasks_after") tasks during the pause"
    log "outage ${outage}: unpausing ${PAUSED_SERVICE} after $((SECONDS - paused_at))s"
    "${COMPOSE[@]}" unpause "$PAUSED_SERVICE"
    is_paused=false

    wait_until "$RECOVERY_LIMIT_SECONDS" recovered \
        || fail "outage ${outage}: no recovery within ${RECOVERY_LIMIT_SECONDS}s: breaker $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"), worker-2 online $(peer_field "$WORKER_1_UI_PORT" worker-2 online), worker-3 online $(peer_field "$WORKER_1_UI_PORT" worker-3 online)"
    recovered_after=$WAITED
    ok "outage ${outage}: breaker closed and worker-2, worker-3 online ${recovered_after}s after the unpause"

    after=$(restart_snapshot "${MAIN_WORKERS[@]}")
    [ "$before" = "$after" ] || fail "outage ${outage}: a worker restarted: before [${before}] after [${after}]"
    ok "outage ${outage}: no worker restarted"
    OUTAGE_TIMINGS="${OUTAGE_TIMINGS}${OUTAGE_TIMINGS:+, }outage ${outage} breaker open after ${opened_after}s and recovered ${recovered_after}s after the unpause"
}

step "Scenario: service outage (${PAUSED_SERVICE} paused twice for ${OUTAGE_SECONDS}s)"

pause_and_recover 1
pause_and_recover 2

# The drain wait and the delivery check are one script, run in its own
# process: it prints its own verdict line, and a failure there is this
# scenario's failure.
log "running verify-after-drain.sh"
"$SCENARIO_DIR/verify-after-drain.sh" || fail "verify-after-drain.sh failed (see its verdict above)"

pass "${OUTAGE_TIMINGS}, no restart, delivery verified after the drain"
