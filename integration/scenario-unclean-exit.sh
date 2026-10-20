#!/usr/bin/env bash
# ============================================================================
# Scenario: a worker dies without withdrawing its card.
#
# Kills worker-2 (SIGKILL, so it cannot withdraw its card). What it proves
# then follows the provider's liveness style (the table in scenario-lib.sh).
#
# Style offline (filesystem, postgres, redis):
#   - within 20 s worker-1 lists worker-2 offline,
#   - within 45 s of the kill worker-2 is gone from the list (the card is
#     deleted after fleet.registry.prune_after_seconds),
#   - after a start, within 20 s worker-1 lists worker-2 online with an
#     instance different from the one before the kill.
#
# Style gone (consul): the card key belongs to the session of the killed
# worker, and that session lives on after the kill until its TTL runs out.
# The scenario starts worker-2 again at once and proves the takeover of the
# card by the new instance:
#   - right after the kill, the card key is held by a session named for the
#     killed instance (drakkar-worker-2-<instance>), and that session is
#     alive (GET /v1/session/info with the management token on host port
#     8500),
#   - within one heartbeat plus one refresh plus 5 s of the start worker-1
#     lists worker-2 online with a new instance,
#   - the predecessor's session is gone by then, and it was destroyed by the
#     new instance, not ended by its TTL: the agent's debug log has the
#     destroy request for it, sent from worker-2's address, and no "Session
#     TTL expired" line for it. When the host starts the worker too slowly,
#     the session ends by its TTL first and the verdict is FAIL, with the
#     measured times.
#
# With every style, worker-1 and worker-3 did not restart.
#
# The archive-election half of this scenario is skipped: the takeover of a
# dead worker's archive lock needs an archive pass, which runs only after a
# rotation (at least one hour).
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-unclean-exit.sh
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

# The registry timings of worker/drakkar.yaml.
HEARTBEAT_SECONDS=2
REFRESH_SECONDS=3
# The card session's TTL: half of offline_after_seconds (10), and at least 10 s.
SESSION_TTL_SECONDS=10
TAKEOVER_LIMIT_SECONDS=$((HEARTBEAT_SECONDS + REFRESH_SECONDS + 5))
# This Consul image ends a session twice its TTL after the last renew (its
# documented contract allows anything from once to twice the TTL), and the
# killed worker renewed at most one heartbeat before the kill. These bounds
# only explain a failure; the agent log decides how the session ended.
EARLIEST_TTL_END_SECONDS=$((2 * SESSION_TTL_SECONDS - HEARTBEAT_SECONDS))
LATEST_TTL_END_SECONDS=$((2 * SESSION_TTL_SECONDS))
# The agent's HTTP API on the host, and the card key of the killed worker
# (connections.consul.main.key_prefix is "drakkar").
CONSUL_HOST_URL=http://localhost:8500
CONSUL_CARD_PATH="/v1/kv/drakkar/fleet/workers/${KILLED_WORKER}"
# The agent log read at the end covers this much time before the read.
AGENT_LOG_SINCE=5m

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

back_with_new_instance() {
    peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" true \
        && [ "$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)" != "$instance_before" ]
}

# The current time in seconds since the epoch, to the millisecond.
epoch_now() {
    python3 -c 'import time; print(f"{time.time():.3f}")'
}

# The seconds from the epoch time $1 to the epoch time $2, to 0.1 s.
seconds_between() {
    python3 -c 'import sys; print(f"{float(sys.argv[2]) - float(sys.argv[1]):.1f}")' "$1" "$2"
}

# The management token of consul-fleet, read from docker-compose.yml so the
# harness value lives in one file.
consul_management_token() {
    "${COMPOSE[@]}" config --format=json \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["consul-fleet-init"]["environment"]["CONSUL_HTTP_TOKEN"])'
}

# GET one path of the agent's API with the management token $1:
#   consul_get <token> <path>
consul_get() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf --connect-timeout 2 --max-time 3 -H "X-Consul-Token: $1" "${CONSUL_HOST_URL}$2"
}

# The session that holds the card key of the killed worker; empty when the
# key has no session. Fails when the key cannot be read.
card_session() {
    local entry
    entry=$(consul_get "$1" "$CONSUL_CARD_PATH") || return 1
    python3 -c 'import json, sys; print(json.load(sys.stdin)[0].get("Session") or "")' <<<"$entry"
}

# The name of session $2, empty when the agent no longer knows it (200 []).
session_name_of() {
    local info
    info=$(consul_get "$1" "/v1/session/info/$2") || return 1
    python3 -c 'import json, sys; rows = json.load(sys.stdin); print(rows[0]["Name"] if rows else "")' <<<"$info"
}

# The address of the container of service $1 on its compose network.
container_address() {
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(container_id "$1")"
}

# Style offline: offline, pruned, then back with a new instance.
offline_flow() {
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

    wait_until "$RESTART_LIMIT_SECONDS" back_with_new_instance \
        || fail "worker-1 does not list ${KILLED_WORKER} online with a new instance ${RESTART_LIMIT_SECONDS}s after the start (online: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" online), instance: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance))"
    back_after=$WAITED
    instance_after=$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)
    ok "${KILLED_WORKER} online ${back_after}s after the start with instance ${instance_after}"

    outcome="offline after ${offline_after}s, pruned ${pruned_after}s after the kill, back with a new instance ${back_after}s after the start"
}

# Style gone: start again at once, and see the new instance take the card
# over from the predecessor's session while that session is still alive.
takeover_flow() {
    local token expected_name kill_at predecessor predecessor_name alive_at
    local start_at start_done_at listed_at name_after agent_log destroyer worker_address timings

    token=$(consul_management_token) || fail "the management token of consul-fleet could not be read from docker-compose.yml"
    [ -n "$token" ] || fail "docker-compose.yml gives consul-fleet-init no CONSUL_HTTP_TOKEN"
    expected_name="drakkar-${KILLED_WORKER}-${instance_before//\"/}"

    log "killing ${KILLED_WORKER}"
    # Taken before the kill, so every time below is measured from a moment
    # no later than the kill itself.
    kill_at=$(epoch_now)
    "${COMPOSE[@]}" kill "$KILLED_WORKER"
    is_killed=true

    predecessor=$(card_session "$token") || fail "the card key of ${KILLED_WORKER} could not be read after the kill"
    [ -n "$predecessor" ] || fail "the card key of ${KILLED_WORKER} is held by no session after the kill"
    predecessor_name=$(session_name_of "$token" "$predecessor") || fail "the session ${predecessor} could not be read"
    alive_at=$(epoch_now)
    [ "$predecessor_name" = "$expected_name" ] \
        || fail "the session ${predecessor} that holds the card is named '${predecessor_name}' $(seconds_between "$kill_at" "$alive_at")s after the kill (empty: it ended), expected '${expected_name}'"
    ok "card key held by session ${predecessor} (${predecessor_name}), alive $(seconds_between "$kill_at" "$alive_at")s after the kill"

    log "starting ${KILLED_WORKER} at once"
    start_at=$(epoch_now)
    # docker start, not docker compose start: compose would first wait for
    # the worker's one-shot dependencies, and the takeover has only the time
    # that is left of the predecessor's session.
    docker start "$(container_id "$KILLED_WORKER")" >/dev/null
    start_done_at=$(epoch_now)
    is_killed=false
    timings="start issued $(seconds_between "$kill_at" "$start_at")s after the kill, returned after $(seconds_between "$start_at" "$start_done_at")s"

    POLL_SECONDS=1 wait_until "$TAKEOVER_LIMIT_SECONDS" back_with_new_instance \
        || fail "worker-1 does not list ${KILLED_WORKER} online with a new instance ${TAKEOVER_LIMIT_SECONDS}s after the start (${timings}; online: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" online), instance: $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance))"
    listed_at=$(epoch_now)
    timings="${timings}, new instance listed $(seconds_between "$kill_at" "$listed_at")s after the kill"
    ok "${KILLED_WORKER} online with instance $(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance): ${timings}"

    name_after=$(session_name_of "$token" "$predecessor") || fail "the session ${predecessor} could not be read after the takeover"
    [ -z "$name_after" ] \
        || fail "the predecessor's session ${predecessor} is still alive after the new instance was listed (${timings})"

    agent_log=$("${COMPOSE[@]}" logs --no-color --since="$AGENT_LOG_SINCE" consul-fleet 2>&1) \
        || fail "the log of consul-fleet could not be read"
    if grep -qF "Session TTL expired: session=${predecessor}" <<<"$agent_log"; then
        fail "the predecessor's session ${predecessor} ended by its TTL, so the takeover was not shown: the host started ${KILLED_WORKER} too slowly (${timings}; the session ends by its TTL ${EARLIEST_TTL_END_SECONDS}-${LATEST_TTL_END_SECONDS}s after the kill)"
    fi
    destroyer=$(grep -E "url=/v1/session/destroy/${predecessor}[ ?]" <<<"$agent_log" | sed -n 's/.* from=\([0-9.]*\):.*/\1/p' | head -n 1)
    [ -n "$destroyer" ] \
        || fail "the agent log has no destroy request for the predecessor's session ${predecessor} (${timings})"
    worker_address=$(container_address "$KILLED_WORKER")
    [ "$destroyer" = "$worker_address" ] \
        || fail "the predecessor's session ${predecessor} was destroyed from ${destroyer}, not from ${KILLED_WORKER} (${worker_address})"
    ok "the predecessor's session ${predecessor} was destroyed by the new ${KILLED_WORKER} (${worker_address}), not ended by its TTL"

    outcome="takeover: ${timings}, the predecessor's session destroyed by the new instance, not ended by its TTL (TTL end ${EARLIEST_TTL_END_SECONDS}-${LATEST_TTL_END_SECONDS}s after the kill)"
}

case "$LIVENESS_STYLE" in
    offline) offline_flow ;;
    gone) takeover_flow ;;
    *) fail "unknown liveness style '${LIVENESS_STYLE}'" ;;
esac

after=$(restart_snapshot worker-1 worker-3)
[ "$before" = "$after" ] || fail "another worker restarted: before [${before}] after [${after}]"
ok "worker-1 and worker-3 did not restart"

warn "$ELECTION_SKIP"
pass "${outcome}; election half ${ELECTION_SKIP}"
