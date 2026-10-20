#!/usr/bin/env bash
# ============================================================================
# Scenario: one worker loses the network and gets it back.
#
# Disconnects worker-3 from the compose network for 90 s, then proves that:
#   - worker-1 lists worker-3 with the expected liveness:
#       postgres, redis - online: false within 30 s (worker-3 cannot reach
#                    the provider's service, so its card ages on the
#                    service's clock). The check waits for the verdict
#                    instead of reading it at 30 s: a card 30 s old is
#                    deleted (prune_after_seconds), and the peer would then
#                    be absent, not offline,
#       filesystem - online: true after 30 s (the card goes through the
#                    shared folder, which a network cut does not touch),
#   - worker-3's own completed counter (read inside its container) is not
#     lower than before the cut,
#   - within 20 s of the reconnect worker-1 lists worker-3 online,
#   - no worker container restarted.
#
# Usage: HARNESS_PROVIDER=redis integration/scenario-worker-cut-off.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-worker-cut-off
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

CUT_WORKER=worker-3
CUT_WORKER_UI_PORT=8083
WORKER_1_UI_PORT=8081
OFFLINE_CHECK_SECONDS=30
CUT_SECONDS=90
RECONNECT_LIMIT_SECONDS=20
# The dashboard waits up to 5 s for the Kafka lag before it answers without
# it; the completed counter is in the answer either way.
DASHBOARD_READ_SECONDS=15

# The completed-task counter of the cut worker, read inside its container
# because its published ports are gone while it is disconnected.
completed_inside() {
    "${COMPOSE[@]}" exec -T "$CUT_WORKER" python -c \
        "import json, urllib.request; print(json.load(urllib.request.urlopen('http://localhost:${CUT_WORKER_UI_PORT}/api/v1/dashboard', timeout=${DASHBOARD_READ_SECONDS}))['stats']['completed'])"
}

step "Scenario: ${CUT_WORKER} cut off from the network for ${CUT_SECONDS}s"

container=$(container_id "$CUT_WORKER")
network=$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' "$container" | head -n 1)
[ -n "$network" ] || fail "${CUT_WORKER} is on no network"
# Reconnecting without the compose aliases would leave the service name
# unresolvable for the other workers.
alias_args=()
while IFS= read -r alias_name; do
    [ -n "$alias_name" ] && alias_args+=(--alias="$alias_name")
done < <(docker inspect -f "{{range (index .NetworkSettings.Networks \"${network}\").Aliases}}{{.}}{{\"\\n\"}}{{end}}" "$container")

peer_online_is "$WORKER_1_UI_PORT" "$CUT_WORKER" true || fail "worker-1 does not list ${CUT_WORKER} online before the cut"
before=$(restart_snapshot "${MAIN_WORKERS[@]}")
completed_before=$(completed_inside) || fail "the completed counter of ${CUT_WORKER} could not be read before the cut"
ok "${CUT_WORKER} online, completed=${completed_before}, network ${network}"

is_cut=false
# Never leave the worker disconnected, also when a check fails half way.
reconnect_on_exit() {
    if [ "$is_cut" = true ]; then
        docker network connect ${alias_args[@]+"${alias_args[@]}"} "$network" "$container" >/dev/null 2>&1 || true
    fi
}
trap reconnect_on_exit EXIT

log "disconnecting ${CUT_WORKER} from ${network}"
docker network disconnect "$network" "$container"
is_cut=true
cut_at=$SECONDS

# A provider with a service (see the table in scenario-lib.sh) loses it with
# the network; the filesystem provider keeps its shared folder.
if [ -n "$PROVIDER_SERVICE" ]; then
    wait_until "$OFFLINE_CHECK_SECONDS" peer_online_is "$WORKER_1_UI_PORT" "$CUT_WORKER" false \
        || fail "worker-1 does not list ${CUT_WORKER} offline within ${OFFLINE_CHECK_SECONDS}s of the cut (online: $(peer_field "$WORKER_1_UI_PORT" "$CUT_WORKER" online))"
    liveness_while_cut="online false after ${WAITED}s of the cut"
else
    sleep_until_elapsed "$cut_at" "$OFFLINE_CHECK_SECONDS"
    online_while_cut=$(peer_field "$WORKER_1_UI_PORT" "$CUT_WORKER" online)
    [ "$online_while_cut" = true ] \
        || fail "after ${OFFLINE_CHECK_SECONDS}s worker-1 lists ${CUT_WORKER} with online ${online_while_cut}, expected true"
    liveness_while_cut="online true after ${OFFLINE_CHECK_SECONDS}s of the cut"
fi
ok "worker-1 lists ${CUT_WORKER} ${liveness_while_cut}"

completed_during=$(completed_inside) || fail "the completed counter of ${CUT_WORKER} could not be read during the cut"
[ "$completed_during" -ge "$completed_before" ] \
    || fail "${CUT_WORKER} completed counter went down: ${completed_before} before the cut, ${completed_during} during it"
ok "${CUT_WORKER} completed=${completed_during} during the cut (before: ${completed_before})"

sleep_until_elapsed "$cut_at" "$CUT_SECONDS"
log "reconnecting ${CUT_WORKER} after $((SECONDS - cut_at))s"
docker network connect ${alias_args[@]+"${alias_args[@]}"} "$network" "$container"
is_cut=false

wait_until "$RECONNECT_LIMIT_SECONDS" peer_online_is "$WORKER_1_UI_PORT" "$CUT_WORKER" true \
    || fail "worker-1 does not list ${CUT_WORKER} online ${RECONNECT_LIMIT_SECONDS}s after the reconnect (online: $(peer_field "$WORKER_1_UI_PORT" "$CUT_WORKER" online))"
online_after=$WAITED
ok "${CUT_WORKER} online ${online_after}s after the reconnect"

after=$(restart_snapshot "${MAIN_WORKERS[@]}")
[ "$before" = "$after" ] || fail "a worker restarted during the cut: before [${before}] after [${after}]"
ok "no worker restarted"

pass "${liveness_while_cut}, completed ${completed_before} -> ${completed_during}, online ${online_after}s after the reconnect, no restart"
