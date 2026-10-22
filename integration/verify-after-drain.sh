#!/usr/bin/env bash
# ============================================================================
# Wait for the producer to finish and the main cluster to drain its backlog,
# then verify delivery of every request (just verify-delivery).
#
# The producer keeps running after it prints PRODUCER_DONE, so its exit is
# not the signal; the signal is the PRODUCER_DONE line and a consumer lag
# of 0 on the source topic. The message total is read from the running
# producer container.
#
# Exits 0 with a PASS verdict, or 1 with a FAIL verdict.
#
# Usage: HARNESS_PROVIDER=consul integration/verify-after-drain.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=verify-after-drain
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

TOTAL_MESSAGES=$(harness_total_messages) || fail "cannot read the message total from the producer"
# The paced producer phases take about 7 minutes for 5000 messages; the
# limit leaves room for a slow start of the producer.
PRODUCER_DONE_LIMIT_SECONDS=2700
CONSUMER_GROUP=drakkar-integration
SOURCE_TOPIC=search-requests
# The flood phase sends every id again, and the main cluster works the
# backlog off for an hour or more after the producer is done.
DRAIN_LIMIT_SECONDS=7200
DRAIN_POLL_SECONDS=30

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

step "Verify after drain (${HARNESS_PROVIDER})"

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

pass "drained ${drained_after}s after PRODUCER_DONE, delivery of ${TOTAL_MESSAGES} requests verified"
