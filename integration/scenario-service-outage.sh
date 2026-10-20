#!/usr/bin/env bash
# ============================================================================
# Scenario: the registry's service goes away and comes back.
#
# Pauses the service container of the provider (postgres-fleet for postgres,
# redis-fleet for redis; the table is in scenario-lib.sh) for 90 s while the
# main cluster runs with that provider, twice: a pause after a recovery must
# behave like the first one, because a cancelled call can leave state behind
# that only a second outage shows. A paused container keeps its connections
# open and answers nothing, so every call in flight must end at its own time
# limit. Each pause proves that:
#   - worker-1's registry breaker opens within 40 s
#     (drakkar_provider_breaker_open{feature="registry",provider="<provider>"} 1),
#   - within 60 s of the unpause the breaker is closed again and worker-1
#     lists worker-2 and worker-3 online,
#   - no worker container restarted.
# Then it proves that:
#   - every request still reached the sinks (just verify-delivery, after the
#     producer printed PRODUCER_DONE and the consumer group's lag is 0; the
#     producer keeps running after PRODUCER_DONE, so its exit is not the
#     signal, and the workers drain the backlog long after it).
#
# Not applicable with the filesystem provider: it has no service to pause.
#
# Usage: HARNESS_PROVIDER=redis integration/scenario-service-outage.sh
# Export TOTAL_MESSAGES when the producer ran with a value other than the
# compose default (5000).
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
TOTAL_MESSAGES="${TOTAL_MESSAGES:-5000}"
# The paced producer phases take about 20 minutes for 5000 messages.
PRODUCER_DONE_LIMIT_SECONDS=2700
CONSUMER_GROUP=drakkar-integration
SOURCE_TOPIC=search-requests
# The flood phase sends every id again, and the main cluster works the
# backlog off for an hour or more after the producer is done.
DRAIN_LIMIT_SECONDS=7200
DRAIN_POLL_SECONDS=30

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

# The distinct request ids of the producer's PRODUCER_DONE line; empty
# while it is still sending.
producer_done_count() {
    "${COMPOSE[@]}" logs --no-color producer 2>/dev/null \
        | sed -n 's/.*PRODUCER_DONE distinct_request_ids=\([0-9]*\).*/\1/p' | tail -n 1
}

producer_done() {
    [ -n "$(producer_done_count)" ]
}

# The summed lag of the main cluster's consumer group on the source topic;
# `unknown` while the group is rebalancing or a partition has no committed
# offset yet.
consumer_lag() {
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server=localhost:9092 --describe --group="$CONSUMER_GROUP" 2>/dev/null \
        | awk -v topic="$SOURCE_TOPIC" '
            $2 == topic { rows++; if ($6 ~ /^[0-9]+$/) lag += $6; else unknown = 1 }
            END { if (rows == 0 || unknown) print "unknown"; else print lag + 0 }'
}

drained() {
    local lag
    lag=$(consumer_lag)
    log "consumer lag of ${CONSUMER_GROUP}: ${lag}"
    [ "$lag" = 0 ]
}

# One outage: pause the service, see the breaker open, unpause, see the
# breaker close and the peers online, and check that no worker restarted.
# Appends "<opened>s/<recovered>s" for the verdict to OUTAGE_TIMINGS.
#   pause_and_recover <outage number>
OUTAGE_TIMINGS=""
pause_and_recover() {
    local outage=$1 before after paused_at opened_after recovered_after

    breaker_is 0 || fail "outage ${outage}: the registry breaker is not closed before the pause (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
    peers_online || fail "outage ${outage}: worker-1 does not list worker-2 and worker-3 online before the pause"
    before=$(restart_snapshot "${MAIN_WORKERS[@]}")
    ok "outage ${outage}: breaker closed, worker-2 and worker-3 online"

    log "outage ${outage}: pausing ${PAUSED_SERVICE}"
    "${COMPOSE[@]}" pause "$PAUSED_SERVICE"
    is_paused=true
    paused_at=$SECONDS

    wait_until "$BREAKER_OPEN_LIMIT_SECONDS" breaker_is 1 \
        || fail "outage ${outage}: the registry breaker did not open within ${BREAKER_OPEN_LIMIT_SECONDS}s (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
    opened_after=$WAITED
    ok "outage ${outage}: registry breaker open after ${opened_after}s"

    sleep_until_elapsed "$paused_at" "$OUTAGE_SECONDS"
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

log "waiting for the producer to print PRODUCER_DONE"
wait_until "$PRODUCER_DONE_LIMIT_SECONDS" producer_done \
    || fail "the producer did not print PRODUCER_DONE within ${PRODUCER_DONE_LIMIT_SECONDS}s"
produced=$(producer_done_count)
[ "$produced" = "$TOTAL_MESSAGES" ] || fail "the producer sent ${produced} distinct request ids, expected ${TOTAL_MESSAGES}"
ok "producer done: ${produced} distinct request ids"

log "waiting for the main cluster to drain the backlog"
POLL_SECONDS=$DRAIN_POLL_SECONDS wait_until "$DRAIN_LIMIT_SECONDS" drained \
    || fail "the consumer lag of ${CONSUMER_GROUP} is not 0 after ${DRAIN_LIMIT_SECONDS}s (lag: $(consumer_lag))"
drained_after=$WAITED
ok "backlog drained ${drained_after}s after PRODUCER_DONE"

log "verifying delivery of ${TOTAL_MESSAGES} requests"
(cd "$REPO_DIR" && just verify-delivery "$TOTAL_MESSAGES") || fail "just verify-delivery failed"
ok "verify-delivery passed"

pass "${OUTAGE_TIMINGS}, no restart, drained ${drained_after}s after PRODUCER_DONE, delivery verified"
