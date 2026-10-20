#!/usr/bin/env bash
# ============================================================================
# Scenario: a worker dies without withdrawing its card.
#
# Kills worker-2 (SIGKILL, so it cannot withdraw its card), then proves that:
#   - within 20 s worker-1 lists worker-2 offline,
#   - within 45 s of the kill worker-2 is gone from the list (the card is
#     deleted after fleet.registry.prune_after_seconds),
#   - after a start, within 20 s worker-1 lists worker-2 online with an
#     instance different from the one before the kill,
#   - worker-1 and worker-3 did not restart.
#
# The archive-election half of this scenario is skipped: the takeover of a
# dead worker's archive lock needs an archive pass, which runs only after a
# rotation (at least one hour).
#
# Usage: HARNESS_PROVIDER=redis integration/scenario-unclean-exit.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-unclean-exit
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

KILLED_WORKER=worker-2
WORKER_1_UI_PORT=8081
OFFLINE_LIMIT_SECONDS=20
PRUNE_LIMIT_SECONDS=45
RESTART_LIMIT_SECONDS=20
ELECTION_SKIP="skipped: the archive lock takeover needs an archive pass, which runs only after a rotation (at least one hour); covered by the election test kit"

step "Scenario: unclean exit of ${KILLED_WORKER}"

peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" true || fail "worker-1 does not list ${KILLED_WORKER} online before the kill"
instance_before=$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)
case "$instance_before" in
    \"*\") ;;
    *) fail "worker-1 lists ${KILLED_WORKER} without an instance (${instance_before})" ;;
esac
before=$(restart_snapshot worker-1 worker-3)
ok "${KILLED_WORKER} online with instance ${instance_before}"

# Start the worker again when a check fails before the start below.
is_killed=false
start_on_exit() {
    if [ "$is_killed" = true ]; then
        "${COMPOSE[@]}" start "$KILLED_WORKER" >/dev/null 2>&1 || true
    fi
}
trap start_on_exit EXIT

log "killing ${KILLED_WORKER}"
"${COMPOSE[@]}" kill "$KILLED_WORKER"
is_killed=true
killed_at=$SECONDS

wait_until "$OFFLINE_LIMIT_SECONDS" peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" false \
    || fail "worker-1 does not list ${KILLED_WORKER} offline within ${OFFLINE_LIMIT_SECONDS}s (online: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" online))"
offline_after=$WAITED
ok "${KILLED_WORKER} offline after ${offline_after}s"

wait_until $((PRUNE_LIMIT_SECONDS - (SECONDS - killed_at))) peer_absent "$WORKER_1_UI_PORT" "$KILLED_WORKER" \
    || fail "worker-1 still lists ${KILLED_WORKER} ${PRUNE_LIMIT_SECONDS}s after the kill (online: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" online))"
pruned_after=$((SECONDS - killed_at))
ok "${KILLED_WORKER} gone from the list ${pruned_after}s after the kill"

log "starting ${KILLED_WORKER}"
"${COMPOSE[@]}" start "$KILLED_WORKER"
is_killed=false

back_with_new_instance() {
    peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" true \
        && [ "$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)" != "$instance_before" ]
}
wait_until "$RESTART_LIMIT_SECONDS" back_with_new_instance \
    || fail "worker-1 does not list ${KILLED_WORKER} online with a new instance ${RESTART_LIMIT_SECONDS}s after the start (online: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" online), instance: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance))"
back_after=$WAITED
instance_after=$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)
ok "${KILLED_WORKER} online ${back_after}s after the start with instance ${instance_after}"

after=$(restart_snapshot worker-1 worker-3)
[ "$before" = "$after" ] || fail "another worker restarted: before [${before}] after [${after}]"
ok "worker-1 and worker-3 did not restart"

warn "$ELECTION_SKIP"
pass "offline after ${offline_after}s, pruned ${pruned_after}s after the kill, back with a new instance ${back_after}s after the start; election half ${ELECTION_SKIP}"
