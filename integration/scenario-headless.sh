#!/usr/bin/env bash
# ============================================================================
# Scenario: workers that leave out the UI, the recorder or the API.
#
# Two compose services of the profile `headless` run the symbol-counting
# pipeline in the cluster "headless cluster", each in a consumer group of its
# own (docker-compose.yml). The scenario starts them, and it stops and removes
# them at the end, also after a failure. It checks, in order:
#   1. headless-recorder (ui.enabled false, recorder.enabled true, API on
#      host port 8093) answers 200 on /healthz, and 404 on /api/v1/identity,
#      a route that only the UI adds,
#   2. both workers consume and deliver: after the scenario produces its
#      messages into the source topic, drakkar_sink_payloads_delivered_total
#      of the sink per_message_counts rises by at least the number of
#      messages on the metrics port of each, 9097 and 9098. headless-no-api
#      (api.enabled false, ui.enabled false, recorder.enabled false) has no
#      HTTP server, so there is no readiness wait for it and its metrics port
#      is the only place to see that it works. The group starts at the
#      earliest offset, so a backlog of the topic counts too,
#   3. after a stop, both containers exited with code 0, a sealed recorder
#      file of headless-recorder with its meta file is in the shared
#      folder under history/<cluster>/ and holds a consumed event, and
#      headless-no-api left no recorder file.
# The verdict line carries the numbers of the run.
#
# The messages have no `patterns` or `file_paths`, so the main cluster cannot
# parse them and sends them to its DLQ.
#
# Runs with every provider; the headless workers use none.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-headless.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-headless
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

RECORDER_SERVICE=headless-recorder
NO_API_SERVICE=headless-no-api
HEADLESS_SERVICES=("$RECORDER_SERVICE" "$NO_API_SERVICE")
RECORDER_API_PORT=8093
RECORDER_METRICS_PORT=9097
NO_API_METRICS_PORT=9098
SOURCE_TOPIC=search-requests
SINK_METRIC=drakkar_sink_payloads_delivered_total
SINK_LABELS='sink_name="per_message_counts"'
MESSAGE_COUNT=10
# A path that exists in every image; the count itself does not matter.
MESSAGE_FILE_PATH=/etc/hostname
# The API of headless-recorder answers within this long of the start; the
# first run also builds the image.
API_LIMIT_SECONDS=180
# Both workers deliver the messages within this long of the produce.
DELIVERY_LIMIT_SECONDS=300
# Longer than executor.drain_timeout (30s) of both services.
STOP_TIMEOUT_SECONDS=60
# The events of the shutdown and of the recorder, which tell why a sealed
# file is missing; the names are the same on every backend.
SHUTDOWN_EVENTS='recorder_started|recorder_rotated|recorder_sealed|recorder_seal_failed|recorder_leftover_sealed|recorder_final_flush_failed|recorder_stopped|recorder_stop_failed|drakkar_shutting_down|sources_draining|draining_executors|executors_drained|sources_drained|drain_timeout|stop_processor_drain_timeout|background_task_drain_timeout|drakkar_stopped'
SHARED_DIR="${SCENARIO_DIR}/shared"
HISTORY_DIR="${SHARED_DIR}/history"
# The run's marker in the request ids.
RUN_TOKEN="$(date +%s)-$$"

# Stop and remove both services. The shared folder is a bind mount, so the
# sealed file stays there.
cleanup() {
    "${COMPOSE[@]}" --profile=headless rm --stop --force "${HEADLESS_SERVICES[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The HTTP status of a GET of $1, or 000 when nothing answers.
http_status() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -s -o /dev/null --connect-timeout 2 --max-time 5 -w '%{http_code}' "$1" || true
}

recorder_api_is_healthy() {
    [ "$(http_status "http://localhost:${RECORDER_API_PORT}/healthz")" = 200 ]
}

# The sink counter on the metrics port $1 as a number; 0 while the sample is
# absent, which a worker that has delivered nothing has not registered yet.
#   delivered_on <port>
delivered_on() {
    local value
    value=$(metric "$1" "$SINK_METRIC" "$SINK_LABELS")
    case "$value" in
        absent) echo 0 ;;
        unreachable) return 1 ;;
        *) echo "$value" ;;
    esac
}

# True when the counter on the metrics port $1 is at least $2 above $3:
#   delivered_rose <port> <baseline> <rise>
delivered_rose() {
    local value
    value=$(delivered_on "$1") || return 1
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) - float(sys.argv[2]) >= float(sys.argv[3]) else 1)' \
        "$value" "$2" "$3"
}

both_delivered() {
    delivered_rose "$RECORDER_METRICS_PORT" "$recorder_baseline" "$MESSAGE_COUNT" \
        && delivered_rose "$NO_API_METRICS_PORT" "$no_api_baseline" "$MESSAGE_COUNT"
}

# Produce MESSAGE_COUNT messages that the symbol-counting pipeline reads.
produce_messages() {
    local index
    for index in $(seq 1 "$MESSAGE_COUNT"); do
        printf '{"request_id": "headless-%s-%s", "pattern": "x", "file_path": "%s"}\n' \
            "$RUN_TOKEN" "$index" "$MESSAGE_FILE_PATH"
    done | "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server=localhost:9092 \
        --topic="$SOURCE_TOPIC" \
        --producer-property=acks=all
}

# The exit code of the stopped container of service $1.
exit_code_of() {
    local container
    container=$("${COMPOSE[@]}" --profile=headless ps --all --quiet "$1")
    [ -n "$container" ] || return 1
    docker inspect --format='{{.State.ExitCode}}' "$container"
}

# The sealed recorder files of worker $1 in the shared history folder, one
# path per line; their meta files are not listed. Fails when the history
# folder does not exist.
sealed_files_of() {
    [ -d "$HISTORY_DIR" ] || return 1
    find "$HISTORY_DIR" -type f -name "$1-*.db" | sort
}

# What explains a missing sealed file: the shutdown and recorder lines of
# the log of $1, the recorder files left in the storage folder root, and the
# work files and folders of the history folder. Read before the exit trap
# removes the container.
#   shutdown_diagnostics <service>
shutdown_diagnostics() {
    echo "--- shutdown and recorder log lines of $1"
    "${COMPOSE[@]}" --profile=headless logs --no-color --no-log-prefix "$1" 2>&1 | grep -E "$SHUTDOWN_EVENTS" || echo "(none)"
    echo "--- recorder files of $1 left in ${SHARED_DIR}"
    find "$SHARED_DIR" -maxdepth 1 -name "$1-*" | sort
    echo "--- history folder ${HISTORY_DIR}"
    find "$HISTORY_DIR" -mindepth 1 2>/dev/null | sort || true
}

# The number of consumed events in the SQLite file $1, opened read-only.
consumed_events_in() {
    python3 -c '
import sqlite3, sys
conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
print(conn.execute("SELECT count(*) FROM events WHERE event = ?", ("consumed",)).fetchone()[0])
' "$1"
}

# The sealed files of earlier runs stay in the shared folder; only a file
# that is not in this list was written by this run.
files_before=$(sealed_files_of "$RECORDER_SERVICE") || files_before=

step "Start the headless workers"
"${COMPOSE[@]}" --profile=headless up --detach --build "${HEADLESS_SERVICES[@]}" >/dev/null 2>&1 \
    || fail "docker compose could not start ${HEADLESS_SERVICES[*]} (profile headless)"
ok "started ${HEADLESS_SERVICES[*]}"

step "The API of ${RECORDER_SERVICE}"
wait_until "$API_LIMIT_SECONDS" recorder_api_is_healthy \
    || fail "GET :${RECORDER_API_PORT}/healthz did not answer 200 within ${API_LIMIT_SECONDS}s (last status $(http_status "http://localhost:${RECORDER_API_PORT}/healthz"))"
health_waited=$WAITED
identity_status=$(http_status "http://localhost:${RECORDER_API_PORT}/api/v1/identity")
[ "$identity_status" = 404 ] \
    || fail "GET :${RECORDER_API_PORT}/api/v1/identity answered ${identity_status} with the UI off, expected 404"
ok "GET :${RECORDER_API_PORT}/healthz answered 200 after ${health_waited}s and /api/v1/identity answered 404"

step "Both workers consume and deliver"
recorder_baseline=$(delivered_on "$RECORDER_METRICS_PORT") \
    || fail "the metrics of ${RECORDER_SERVICE} on port ${RECORDER_METRICS_PORT} cannot be read"
no_api_baseline=$(delivered_on "$NO_API_METRICS_PORT") \
    || fail "the metrics of ${NO_API_SERVICE} on port ${NO_API_METRICS_PORT} cannot be read"
produce_output=$(produce_messages 2>&1) \
    || fail "the console producer failed: $(tr '\n' ' ' <<<"$produce_output")"
log "produced ${MESSAGE_COUNT} messages into ${SOURCE_TOPIC}; delivered before: ${RECORDER_SERVICE} ${recorder_baseline}, ${NO_API_SERVICE} ${no_api_baseline}"
wait_until "$DELIVERY_LIMIT_SECONDS" both_delivered \
    || fail "within ${DELIVERY_LIMIT_SECONDS}s of the produce ${SINK_METRIC} rose to $(metric "$RECORDER_METRICS_PORT" "$SINK_METRIC" "$SINK_LABELS") on ${RECORDER_SERVICE} (was ${recorder_baseline}) and to $(metric "$NO_API_METRICS_PORT" "$SINK_METRIC" "$SINK_LABELS") on ${NO_API_SERVICE} (was ${no_api_baseline}), expected a rise of ${MESSAGE_COUNT} on both"
delivery_waited=$WAITED
recorder_delivered=$(delivered_on "$RECORDER_METRICS_PORT")
no_api_delivered=$(delivered_on "$NO_API_METRICS_PORT")
ok "delivered ${recorder_baseline} -> ${recorder_delivered} on ${RECORDER_SERVICE} and ${no_api_baseline} -> ${no_api_delivered} on ${NO_API_SERVICE} ${delivery_waited}s after the produce"

step "Stop the workers and look for the recorder files"
"${COMPOSE[@]}" --profile=headless stop --timeout="$STOP_TIMEOUT_SECONDS" "${HEADLESS_SERVICES[@]}" >/dev/null 2>&1 \
    || fail "docker compose could not stop ${HEADLESS_SERVICES[*]}"
for service in "${HEADLESS_SERVICES[@]}"; do
    code=$(exit_code_of "$service") || fail "the container of ${service} cannot be found after the stop"
    [ "$code" = 0 ] || fail "${service} exited with code ${code} after the stop, expected 0"
done
ok "both containers exited with code 0"
diagnostics=$(shutdown_diagnostics "$RECORDER_SERVICE")
sealed=$(sealed_files_of "$RECORDER_SERVICE" | grep -vxF -f <(echo "$files_before") || true)
if [ -z "$sealed" ]; then
    echo "$diagnostics"
    fail "no new sealed recorder file of ${RECORDER_SERVICE} under ${HISTORY_DIR} after the stop (the lines above show the shutdown)"
fi
sealed_count=0
consumed_total=0
while IFS= read -r file; do
    [ -f "${file}.meta.json" ] || fail "the sealed file ${file} has no meta file ${file}.meta.json"
    consumed=$(consumed_events_in "$file") || fail "the sealed file ${file} cannot be read as SQLite"
    sealed_count=$((sealed_count + 1))
    consumed_total=$((consumed_total + consumed))
done <<<"$sealed"
[ "$consumed_total" -gt 0 ] \
    || fail "the ${sealed_count} sealed file(s) of ${RECORDER_SERVICE} hold no consumed event"
no_api_files=$(sealed_files_of "$NO_API_SERVICE") || no_api_files=
[ -z "$no_api_files" ] || fail "${NO_API_SERVICE} has the recorder off, but a recorder file exists: ${no_api_files}"
ok "${sealed_count} sealed file(s) of ${RECORDER_SERVICE} with meta files hold ${consumed_total} consumed events; ${NO_API_SERVICE} left none"

pass "${RECORDER_SERVICE} answered /healthz after ${health_waited}s and 404 on /api/v1/identity; delivered ${recorder_baseline} -> ${recorder_delivered} and ${no_api_baseline} -> ${no_api_delivered} ${delivery_waited}s after the produce; ${sealed_count} sealed file(s) with ${consumed_total} consumed events; ${NO_API_SERVICE} left no recorder file"
