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
#     deleted after autodiscover.prune_after),
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
# Archive-election half, with every provider, after worker-2 is back. The
# archive is a cluster task: a slot every 30 s of epoch time, a 40 s lease,
# on worker-2 and worker-3 (worker-1 is the observer: its compose
# environment turns the archive off, so it never holds the lock). The winner
# of a slot holds the lock for its whole pass and at least 20 s, also when
# nothing is due, so a held lock alone does not show the planted window's
# pass, and the plant is timed against the slots:
#   - an archive of the planted window left by an earlier run is a FAIL
#     before anything is planted,
#   - 2 s after a slot boundary S0 (the winner of S0 has listed the history
#     folder by then), the history of one hour window twelve hours back is
#     planted into the shared folder (PLANT_HISTORY_CMD, see
#     scenario-lib.sh), so the window is due at once; a plant that ends after
#     S1 - 2 (S1 = S0 + 30) is a FAIL that names the plant's duration,
#   - an archive of the window already published at S1 + 1 is a FAIL (the
#     winner of S0 acquired late and listed the plant),
#   - from S1 + 1, within the 10 s acquire window, a worker H holds the
#     archive lock in two polls in a row: H won S1 and archives the planted
#     window, and H is worker-2 or worker-3,
#   - H is killed at once; its archive is not published yet and every
#     planted file is still there (otherwise FAIL: the planted window is
#     too small),
#   - within 150 s of the kill another worker W holds the lock, the archive
#     of the window exists, W logged recorder_archive_created for it, and
#     every planted file is gone,
#   - with filesystem and postgres, W logged periodic_lock_taken_over with
#     its provider and the archive's scope (redis and consul write no such
#     line),
#   - H, started again, is listed online by worker-1; worker-1 and W did not
#     restart,
#   - no slot of the handler's cluster tick (harness_cluster_tick) ran twice
#     from the plant to the end of the half.
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

# The registry timings of worker/drakkar.yaml.
HEARTBEAT_SECONDS=2
REFRESH_SECONDS=3
# The card session's TTL: half of autodiscover.offline_after (10 s), and at least 10 s.
SESSION_TTL_SECONDS=10
TAKEOVER_LIMIT_SECONDS=$((HEARTBEAT_SECONDS + REFRESH_SECONDS + 5))
# This Consul image ends a session twice its TTL after the last renew (its
# documented contract allows anything from once to twice the TTL), and the
# killed worker renewed at most one heartbeat before the kill. These bounds
# only explain a failure; the agent log decides how the session ended.
EARLIEST_TTL_END_SECONDS=$((2 * SESSION_TTL_SECONDS - HEARTBEAT_SECONDS))
LATEST_TTL_END_SECONDS=$((2 * SESSION_TTL_SECONDS))
# The card key of the killed worker (connections.consul.main.state_prefix is
# "drakkar").
CONSUL_CARD_PATH="/v1/kv/drakkar/fleet/workers/${KILLED_WORKER}"
# The agent log read at the end covers this much time before the read.
AGENT_LOG_SINCE=5m

# The archive settings of worker/drakkar.yaml (recorder.archive).
ARCHIVE_INTERVAL_SECONDS=30
LEASE_SECONDS=40
# A cluster task's slot is won only in the first 10 s after its boundary.
ACQUIRE_WINDOW_SECONDS=10
# The plant starts this long after the slot boundary S0, so the winner of S0
# has listed the history folder before it, and must end this long before the
# next boundary S1, so the winner of S1 lists the whole window.
PLANT_DELAY_SECONDS=2
PLANT_MARGIN_SECONDS=2
# The holder is read from this long after S1, once its winner holds the lock.
HOLDER_DELAY_SECONDS=1
# The planted window: its size makes one archive pass take at least 8 s and
# at most 25 s on the harness (below the 40 s lease). Measured: 50 files of
# 1000 events (about 78 MB) take about 12 to 21 s on an idle harness, and
# longer while the workers are busy; 60 files took up to 24 s idle and more
# than 30 s under load.
PLANTED_FILES=50
PLANTED_EVENTS_PER_FILE=1000
# Twelve hours back stays clear of the files the workers seal themselves in
# the first hours of an uptime; retention_days: 1 still covers it.
PLANTED_HOURS_AGO=12
SHARED_DIR="$SCENARIO_DIR/shared"
# Seconds between two reads of the lock holder.
LOCK_POLL_SECONDS=0.2
# How long the half waits, from S1 + 1, for the holder of S1.
HOLDER_LIMIT_SECONDS=$ACQUIRE_WINDOW_SECONDS
# How long the half waits, from the kill, for the takeover and the published
# archive. The dead holder's lease ends up to one lease after the kill, the
# next slot comes within one interval, and the pass of the planted window
# takes up to about 40 s under load: 110 s, and 40 s of margin.
ELECTION_TAKEOVER_LIMIT_SECONDS=150
# The workers that run the archive: the holder must be one of them.
ARCHIVE_WORKERS="worker-2 worker-3"

step "Scenario: unclean exit of ${KILLED_WORKER}"

peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" true || fail "worker-1 does not list ${KILLED_WORKER} online before the kill"
instance_before=$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)
case "$instance_before" in
    \"*\") ;;
    *) fail "worker-1 lists ${KILLED_WORKER} without an instance (${instance_before})" ;;
esac
before=$(restart_snapshot worker-1 worker-3)
ok "${KILLED_WORKER} online with instance ${instance_before}"

# Every service this scenario killed and has not started again; the EXIT
# trap starts them, so a failed check leaves no worker down for the next
# scenario.
killed_services=()

mark_killed() {
    killed_services+=("$1")
}

mark_started() {
    local service remaining=()
    # The ${a[@]+...} form keeps an empty array legal under set -u in bash 3.
    for service in ${killed_services[@]+"${killed_services[@]}"}; do
        [ "$service" = "$1" ] || remaining+=("$service")
    done
    killed_services=(${remaining[@]+"${remaining[@]}"})
}

start_on_exit() {
    local service
    for service in ${killed_services[@]+"${killed_services[@]}"}; do
        "${COMPOSE[@]}" start "$service" >/dev/null 2>&1 || true
    done
    rm -f "$HOLDER_ERROR_FILE"
}
# Error output of the last lock read (see read_holder), removed on exit.
HOLDER_ERROR_FILE=$(mktemp)
trap start_on_exit EXIT

back_with_new_instance() {
    peer_online_is "$WORKER_1_UI_PORT" "$KILLED_WORKER" true \
        && [ "$(peer_field "$WORKER_1_UI_PORT" "$KILLED_WORKER" instance)" != "$instance_before" ]
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
    mark_killed "$KILLED_WORKER"
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
    mark_started "$KILLED_WORKER"

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
    mark_killed "$KILLED_WORKER"

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
    mark_started "$KILLED_WORKER"
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

# ---------------------------------------------------------------------------
# Archive-election half: kill the holder of the archive lock in the middle
# of a pass, and see another worker take the lock over and publish.
# ---------------------------------------------------------------------------

step "Archive election: the holder of the archive lock dies during a pass"

# The value of field $1 in the PLANTED line $2.
planted_field() {
    sed -n "s/.* $1=\([^ ]*\).*/\1/p" <<<"$2"
}

# True when the archive of the planted window exists.
archive_exists() {
    [ -e "${history_folder}/${archive}" ]
}

# Print the planted files that are still in the history folder, one per line.
planted_left() {
    local name
    for name in "${planted_names[@]}"; do
        if [ -e "${history_folder}/${name}" ]; then
            echo "$name"
        fi
    done
}

# Print the planted files that are no longer in the history folder.
planted_gone() {
    local name
    for name in "${planted_names[@]}"; do
        if [ ! -e "${history_folder}/${name}" ]; then
            echo "$name"
        fi
    done
}

# The holder, or nothing when no lock is held or the read failed. The error
# output of the last failed read is kept in HOLDER_ERROR_FILE.
read_holder() {
    lock_holder 2>"$HOLDER_ERROR_FILE" || true
}

# The error output of the last lock read, or "none" when it was empty, so a
# failing read is not reported as "no holder".
last_holder_error() {
    local text
    text=$(tr '\n' ' ' <"$HOLDER_ERROR_FILE" | cut -c1-300)
    echo "${text:-none}"
}

# True when the JSON log of service $1 since $2 has a line whose fields
# match every key=value pair after it:
#   log_has <service> <since> event=<name> [<key>=<value>...]
log_has() {
    local service=$1 since=$2
    shift 2
    "${COMPOSE[@]}" logs --no-color --no-log-prefix --since="$since" "$service" 2>/dev/null \
        | python3 -c '
import json, sys
wanted = dict(pair.split("=", 1) for pair in sys.argv[1:])
found = False
# Reads to the end even after a match: an early exit breaks the pipe of
# docker compose logs, which then exits 255 and fails the pipeline.
for line in sys.stdin:
    start = line.find("{")
    if found or start < 0:
        continue
    try:
        record = json.loads(line[start:])
    except ValueError:
        continue
    if isinstance(record, dict) and all(str(record.get(key)) == value for key, value in wanted.items()):
        found = True
sys.exit(0 if found else 1)
' "$@"
}

lock_holder_prepare || fail "the admin settings of the ${HARNESS_PROVIDER} lock could not be read from docker-compose.yml"
plant_history_prepare || fail "the history generator could not be prepared (PLANT_HISTORY_CMD in scenario-lib.sh)"
election_before=$(restart_snapshot worker-1 worker-2 worker-3)
plant_since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

slot_0=$(next_slot_boundary "$ARCHIVE_INTERVAL_SECONDS")
slot_1=$((slot_0 + ARCHIVE_INTERVAL_SECONDS))
plant_deadline=$((slot_1 - PLANT_MARGIN_SECONDS))
log "planting ${PLANT_DELAY_SECONDS}s after the archive slot ${slot_0}; the plant must end by ${plant_deadline}"
sleep_until_epoch $((slot_0 + PLANT_DELAY_SECONDS))

# The generator refuses, writing nothing, when the window's archive already
# exists: a leftover of an earlier run would make the checks below lie.
plant_started_at=$(epoch_now)
planted=$("${PLANT_HISTORY_CMD[@]}" --shared-dir="$SHARED_DIR" --cluster="main cluster" \
    --files="$PLANTED_FILES" --events-per-file="$PLANTED_EVENTS_PER_FILE" --hours-ago="$PLANTED_HOURS_AGO" 2>&1) \
    || fail "the history window could not be planted: ${planted}"
plant_ended_at=$(epoch_now)
plant_seconds=$(seconds_between "$plant_started_at" "$plant_ended_at")
if epoch_after "$plant_ended_at" "$plant_deadline"; then
    fail "the plant took ${plant_seconds} s and ended after ${plant_deadline} (${PLANT_MARGIN_SECONDS} s before the archive slot ${slot_1}); the slot winner may have listed a partial window"
fi
planted=$(grep '^PLANTED ' <<<"$planted") || fail "the history generator printed no PLANTED line"
archive=$(planted_field archive "$planted")
history_folder="${SHARED_DIR}/history/$(planted_field scope "$planted")"
planted_names=()
while IFS= read -r name; do
    planted_names+=("$name")
done < <(cd "$history_folder" && ls -- planted-*.db)
[ "${#planted_names[@]}" -eq "$PLANTED_FILES" ] \
    || fail "the history folder holds ${#planted_names[@]} planted files, expected ${PLANTED_FILES} (a leftover of an earlier run?)"
ok "planted ${PLANTED_FILES} files of ${PLANTED_EVENTS_PER_FILE} events in ${plant_seconds} s, window $(planted_field window_start "$planted")-$(planted_field window_end "$planted"), archive ${archive}"

# The winner of S1 holds the lock for its whole pass and at least 20 s, so
# a holder seen in two polls in a row from S1 + 1 on is that winner.
sleep_until_epoch $((slot_1 + HOLDER_DELAY_SECONDS))
if archive_exists; then
    fail "the archive ${archive} was published before the archive slot ${slot_1}: the winner of the slot ${slot_0} acquired more than ${PLANT_DELAY_SECONDS} s late and listed the planted window"
fi
log "waiting up to ${HOLDER_LIMIT_SECONDS}s for the winner of the archive slot ${slot_1} to hold the archive lock"
holder=""
previous=""
holder_deadline=$((SECONDS + HOLDER_LIMIT_SECONDS))
while true; do
    current=$(read_holder)
    if [ -n "$current" ] && [ "$current" = "$previous" ]; then
        holder=$current
        break
    fi
    previous=$current
    [ "$SECONDS" -lt "$holder_deadline" ] \
        || fail "no worker held the archive lock in two polls in a row within ${HOLDER_LIMIT_SECONDS}s of the archive slot ${slot_1} plus ${HOLDER_DELAY_SECONDS}s (archive published: $(archive_exists && echo yes || echo no); last lock read error: $(last_holder_error))"
    sleep "$LOCK_POLL_SECONDS"
done
case " ${ARCHIVE_WORKERS} " in
    *" ${holder} "*) ;;
    *) fail "the archive lock is held by ${holder}, expected one of ${ARCHIVE_WORKERS}: worker-1 must not run the archive (check DK_RECORDER__ARCHIVE__ENABLED: \"false\" on worker-1 in docker-compose.yml)" ;;
esac
ok "${holder} holds the archive lock"

log "killing ${holder}"
election_kill_at=$(epoch_now)
"${COMPOSE[@]}" kill "$holder"
mark_killed "$holder"

if archive_exists; then
    fail "the pass of ${holder} finished before the kill; raise the planted size (files=${PLANTED_FILES}, events=${PLANTED_EVENTS_PER_FILE})"
fi
missing=$(planted_gone)
[ -z "$missing" ] \
    || fail "planted files are gone after the kill but the archive ${archive} is not published: $(tr '\n' ' ' <<<"$missing")"
ok "${holder} killed during its pass: no archive yet, every planted file still there"

log "waiting up to ${ELECTION_TAKEOVER_LIMIT_SECONDS}s from the kill for another worker to take the lock over (lease ${LEASE_SECONDS}s, a slot every ${ARCHIVE_INTERVAL_SECONDS}s)"
takeover_deadline=$(python3 -c 'import sys; print(float(sys.argv[1]) + int(sys.argv[2]))' "$election_kill_at" "$ELECTION_TAKEOVER_LIMIT_SECONDS")
before_deadline() {
    python3 -c 'import sys, time; sys.exit(0 if time.time() < float(sys.argv[1]) else 1)' "$takeover_deadline"
}
taker=""
while true; do
    current=$(read_holder)
    if [ -n "$current" ] && [ "$current" != "$holder" ]; then
        taker=$current
        break
    fi
    if archive_exists; then
        fail "the archive ${archive} was published, but no worker other than ${holder} was seen holding the lock (holder now: '${current}')"
    fi
    before_deadline \
        || fail "no worker took the archive lock of ${holder} over within ${ELECTION_TAKEOVER_LIMIT_SECONDS}s of the kill (holder now: '${current}'; last lock read error: $(last_holder_error))"
    sleep "$LOCK_POLL_SECONDS"
done
taken_over_at=$(epoch_now)
takeover_after=$(seconds_between "$election_kill_at" "$taken_over_at")
ok "${taker} holds the archive lock ${takeover_after}s after the kill"

until archive_exists && [ -z "$(planted_left)" ]; do
    if ! before_deadline; then
        if ! archive_exists; then
            fail "${taker} took the lock over, but the archive ${archive} is not published ${ELECTION_TAKEOVER_LIMIT_SECONDS}s after the kill"
        fi
        fail "the archive ${archive} is published, but planted files are left ${ELECTION_TAKEOVER_LIMIT_SECONDS}s after the kill: $(planted_left | tr '\n' ' ')"
    fi
    sleep "$LOCK_POLL_SECONDS"
done
published_after=$(seconds_between "$election_kill_at" "$(epoch_now)")
ok "archive ${archive} published and every planted file deleted ${published_after}s after the kill"

log_has "$taker" "$plant_since" event=recorder_archive_created name="$archive" \
    || fail "the log of ${taker} has no recorder_archive_created line for ${archive}"
ok "${taker} logged recorder_archive_created for ${archive}"
case "$HARNESS_PROVIDER" in
    filesystem | postgres)
        log_has "$taker" "$plant_since" event=periodic_lock_taken_over provider="$HARNESS_PROVIDER" scope="$LOCK_SCOPE" \
            || fail "the log of ${taker} has no periodic_lock_taken_over line with provider=${HARNESS_PROVIDER} scope=${LOCK_SCOPE}"
        ok "${taker} logged periodic_lock_taken_over with provider=${HARNESS_PROVIDER} scope=${LOCK_SCOPE}"
        ;;
    *) log "${HARNESS_PROVIDER} writes no periodic_lock_taken_over line (the contract); not looked for" ;;
esac

log "starting ${holder}"
"${COMPOSE[@]}" start "$holder"
mark_started "$holder"
wait_until "$RESTART_LIMIT_SECONDS" peer_online_is "$WORKER_1_UI_PORT" "$holder" true \
    || fail "worker-1 does not list ${holder} online ${RESTART_LIMIT_SECONDS}s after the start (online: $(peer_field "$WORKER_1_UI_PORT" "$holder" online))"
ok "${holder} online ${WAITED}s after the start"

election_after=$(restart_snapshot worker-1 worker-2 worker-3)
for service in worker-1 "$taker"; do
    [ "$(grep "^${service} " <<<"$election_before")" = "$(grep "^${service} " <<<"$election_after")" ] \
        || fail "${service} restarted during the election half: before [$(grep "^${service} " <<<"$election_before")] after [$(grep "^${service} " <<<"$election_after")]"
done
ok "worker-1 and ${taker} did not restart"

ticks=$(cluster_ticks "$plant_since") || fail "the logs of the main-cluster workers could not be read for harness_cluster_tick lines"
doubled=$(doubled_tick_slots <<<"$ticks")
[ -z "$doubled" ] \
    || fail "slots of harness_cluster_tick ran twice from the plant to the end of the half (slot workers): $(tr '\n' ' ' <<<"$doubled")"
ok "no slot of harness_cluster_tick ran twice from the plant to the end of the half ($(grep -c . <<<"$ticks" || true) ticks)"

pass "${outcome}; plant ${plant_seconds}s; archive lock of ${holder} taken over by ${taker} ${takeover_after}s after the kill, archive published ${published_after}s after the kill"
