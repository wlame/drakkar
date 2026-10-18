#!/usr/bin/env bash
# ============================================================================
# Scenario: the registry's database goes away and comes back.
#
# Pauses the postgres-fleet container for 90 s while the main cluster runs
# with the postgres provider, then proves that:
#   - worker-1's registry breaker opens within 40 s
#     (drakkar_provider_breaker_open{feature="registry",provider="postgres"} 1),
#   - within 60 s of the unpause the breaker is closed again and worker-1
#     lists worker-2 and worker-3 online,
#   - no worker container restarted,
#   - every request still reached the sinks (just verify-delivery, after the
#     producer printed PRODUCER_DONE and the consumer group's lag is 0; the
#     producer keeps running after PRODUCER_DONE, so its exit is not the
#     signal, and the workers drain the backlog long after it).
#
# Not applicable with the filesystem provider: it has no service to pause.
#
# Usage: HARNESS_PROVIDER=postgres integration/scenario-service-outage.sh
# Export TOTAL_MESSAGES when the producer ran with a value other than the
# compose default (5000).
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-service-outage
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

if [ "$HARNESS_PROVIDER" != postgres ]; then
    not_applicable "the filesystem provider has no service container"
fi

DATABASE_SERVICE=postgres-fleet
WORKER_1_UI_PORT=8081
WORKER_1_METRICS_PORT=9090
BREAKER_METRIC=drakkar_provider_breaker_open
REGISTRY_LABELS='feature="registry",provider="postgres"'
BREAKER_OPEN_LIMIT_SECONDS=40
OUTAGE_SECONDS=90
RECOVERY_LIMIT_SECONDS=60
TOTAL_MESSAGES="${TOTAL_MESSAGES:-5000}"
# The paced producer phases take about 20 minutes for 5000 messages.
PRODUCER_DONE_LIMIT_SECONDS=2700
CONSUMER_GROUP=drakkar-integration
SOURCE_TOPIC=search-requests
# The flood phase sends every id again, and the main cluster works the
# backlog off for up to an hour after the producer is done.
DRAIN_LIMIT_SECONDS=5400
DRAIN_POLL_SECONDS=30

is_paused=false
# Never leave the database paused, also when a check fails half way.
unpause_on_exit() {
    if [ "$is_paused" = true ]; then
        "${COMPOSE[@]}" unpause "$DATABASE_SERVICE" >/dev/null 2>&1 || true
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

step "Scenario: service outage (${DATABASE_SERVICE} paused for ${OUTAGE_SECONDS}s)"

breaker_is 0 || fail "the registry breaker is not closed before the outage (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
peers_online || fail "worker-1 does not list worker-2 and worker-3 online before the outage"
before=$(restart_snapshot "${MAIN_WORKERS[@]}")
ok "breaker closed, worker-2 and worker-3 online"

log "pausing ${DATABASE_SERVICE}"
"${COMPOSE[@]}" pause "$DATABASE_SERVICE"
is_paused=true
paused_at=$SECONDS

wait_until "$BREAKER_OPEN_LIMIT_SECONDS" breaker_is 1 \
    || fail "the registry breaker did not open within ${BREAKER_OPEN_LIMIT_SECONDS}s (value: $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"))"
breaker_opened_after=$WAITED
ok "registry breaker open after ${breaker_opened_after}s"

sleep_until_elapsed "$paused_at" "$OUTAGE_SECONDS"
log "unpausing ${DATABASE_SERVICE} after $((SECONDS - paused_at))s"
"${COMPOSE[@]}" unpause "$DATABASE_SERVICE"
is_paused=false

wait_until "$RECOVERY_LIMIT_SECONDS" recovered \
    || fail "no recovery within ${RECOVERY_LIMIT_SECONDS}s: breaker $(metric "$WORKER_1_METRICS_PORT" "$BREAKER_METRIC" "$REGISTRY_LABELS"), worker-2 online $(peer_field "$WORKER_1_UI_PORT" worker-2 online), worker-3 online $(peer_field "$WORKER_1_UI_PORT" worker-3 online)"
recovered_after=$WAITED
ok "breaker closed and worker-2, worker-3 online ${recovered_after}s after the unpause"

after=$(restart_snapshot "${MAIN_WORKERS[@]}")
[ "$before" = "$after" ] || fail "a worker restarted during the outage: before [${before}] after [${after}]"
ok "no worker restarted"

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

pass "breaker open after ${breaker_opened_after}s, closed and peers online ${recovered_after}s after the unpause, no restart, drained ${drained_after}s after PRODUCER_DONE, delivery verified"
