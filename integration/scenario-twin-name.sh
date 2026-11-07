#!/usr/bin/env bash
# ============================================================================
# Scenario: a second worker with the same worker name is refused at start.
#
# Starts the service worker-3-twin (profile twin in docker-compose.yml): the
# worker name worker-3, the main cluster, autodiscover on with the provider
# of the run, as if a second worker-3 started on another host. It has a
# storage.dir of its own inside its container, so the owner lock of the own
# folder cannot see worker-3; the registry's same-name check must refuse it.
# The check lists the cards before the first publish, waits one heartbeat
# plus 2 s, lists again, and refuses when worker-3's card changed.
#
# Checks, in order:
#   1. before: worker-1 lists worker-3 online; its instance is kept,
#   2. the twin's container exits with a non-zero code within 60 s,
#   3. the twin's log has fleet_worker_name_in_use for worker-3 with the
#      instance of the running worker-3 as other_instance, and the error
#      text "worker name 'worker-3' is used by a live worker",
#   4. after: worker-1 lists worker-3 online with the same instance as
#      before (the twin never published and never took the card over),
#      worker-3 answers /readyz with 200, and no main-cluster worker
#      restarted.
#
# The container of the twin is removed at the end, also after a failure.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-twin-name.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-twin-name
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

TWIN_SERVICE=worker-3-twin
TWIN_PROFILE=twin
TWIN_NAME=worker-3
WORKER_1_UI_PORT=8081
TWIN_NAME_UI_PORT=8083
# The check takes at most two heartbeats plus 5 s (9 s with the 2 s
# heartbeat of worker/drakkar.yaml); the start of the container and the
# imports of the worker come first.
EXIT_LIMIT_SECONDS=60
REFUSAL_TEXT="worker name '${TWIN_NAME}' is used by a live worker"
TWIN_COMPOSE=("${COMPOSE[@]}" --profile="$TWIN_PROFILE")

remove_twin() {
    "${TWIN_COMPOSE[@]}" rm -f -s "$TWIN_SERVICE" >/dev/null 2>&1 || true
}
trap remove_twin EXIT

# The state of the twin's container (running, exited, ...); empty while it
# has none.
twin_state() {
    local id
    id=$("${TWIN_COMPOSE[@]}" ps -aq "$TWIN_SERVICE")
    [ -n "$id" ] || return 0
    docker inspect -f '{{.State.Status}}' "$id"
}

twin_exited() {
    [ "$(twin_state)" = exited ]
}

step "Scenario: a twin of ${TWIN_NAME} starts"

peer_online_is "$WORKER_1_UI_PORT" "$TWIN_NAME" true || fail "worker-1 does not list ${TWIN_NAME} online before the twin starts"
instance_before=$(peer_field "$WORKER_1_UI_PORT" "$TWIN_NAME" instance)
case "$instance_before" in
    \"*\") ;;
    *) fail "worker-1 lists ${TWIN_NAME} without an instance (${instance_before})" ;;
esac
restarts_before=$(restart_snapshot "${MAIN_WORKERS[@]}")
ok "${TWIN_NAME} online with instance ${instance_before}"

remove_twin
log "starting ${TWIN_SERVICE}"
"${TWIN_COMPOSE[@]}" up -d --no-deps "$TWIN_SERVICE" >/dev/null 2>&1 \
    || fail "${TWIN_SERVICE} could not be started: $("${TWIN_COMPOSE[@]}" up -d --no-deps "$TWIN_SERVICE" 2>&1 | tail -n 5 | tr '\n' ' ')"

wait_until "$EXIT_LIMIT_SECONDS" twin_exited \
    || fail "${TWIN_SERVICE} is still $(twin_state) ${EXIT_LIMIT_SECONDS}s after its start: the same-name check did not refuse it (its log: $("${TWIN_COMPOSE[@]}" logs --no-color --no-log-prefix --tail=30 "$TWIN_SERVICE" 2>&1 | tr '\n' ' ' | cut -c1-3000))"
exited_after=$WAITED
twin_id=$("${TWIN_COMPOSE[@]}" ps -aq "$TWIN_SERVICE")
exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$twin_id")
twin_log=$(docker logs "$twin_id" 2>&1)
[ "$exit_code" != 0 ] || fail "${TWIN_SERVICE} exited with code 0; a refused start must fail (its log: $(tail -n 30 <<<"$twin_log" | tr '\n' ' ' | cut -c1-3000))"
ok "${TWIN_SERVICE} exited with code ${exit_code} ${exited_after}s after its start"

refusal=$(WORKER_NAME="$TWIN_NAME" OTHER_INSTANCE="${instance_before//\"/}" python3 -c '
import json, os, sys
for line in sys.stdin:
    start = line.find("{")
    if start < 0:
        continue
    try:
        record = json.loads(line[start:])
    except ValueError:
        continue
    if (
        isinstance(record, dict)
        and record.get("event") == "fleet_worker_name_in_use"
        and record.get("worker") == os.environ["WORKER_NAME"]
    ):
        print(record.get("other_instance"))
        break
' <<<"$twin_log")
[ -n "$refusal" ] \
    || fail "the log of ${TWIN_SERVICE} has no fleet_worker_name_in_use line for ${TWIN_NAME} (its log: $(tail -n 30 <<<"$twin_log" | tr '\n' ' ' | cut -c1-3000))"
[ "$refusal" = "${instance_before//\"/}" ] \
    || fail "fleet_worker_name_in_use names the other instance ${refusal}, expected the running ${TWIN_NAME} (${instance_before})"
grep -qF "$REFUSAL_TEXT" <<<"$twin_log" \
    || fail "the log of ${TWIN_SERVICE} does not carry the refusal text \"${REFUSAL_TEXT}\" (its log: $(tail -n 30 <<<"$twin_log" | tr '\n' ' ' | cut -c1-3000))"
ok "${TWIN_SERVICE} logged fleet_worker_name_in_use with other_instance ${refusal} and the refusal text"

peer_online_is "$WORKER_1_UI_PORT" "$TWIN_NAME" true || fail "worker-1 no longer lists ${TWIN_NAME} online after the twin was refused (online: $(peer_field "$WORKER_1_UI_PORT" "$TWIN_NAME" online))"
instance_after=$(peer_field "$WORKER_1_UI_PORT" "$TWIN_NAME" instance)
[ "$instance_after" = "$instance_before" ] \
    || fail "worker-1 lists ${TWIN_NAME} with instance ${instance_after}, expected ${instance_before}: the twin took the card over"
curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${TWIN_NAME_UI_PORT}/readyz" \
    || fail "${TWIN_NAME} does not answer /readyz with 200 after the twin was refused"
restarts_after=$(restart_snapshot "${MAIN_WORKERS[@]}")
[ "$restarts_before" = "$restarts_after" ] || fail "a main-cluster worker restarted: before [${restarts_before}] after [${restarts_after}]"
ok "${TWIN_NAME} still online with instance ${instance_after} and ready; no main-cluster worker restarted"

pass "the twin of ${TWIN_NAME} was refused ${exited_after}s after its start (exit code ${exit_code}, other_instance ${refusal}); ${TWIN_NAME} kept its card and kept working"
