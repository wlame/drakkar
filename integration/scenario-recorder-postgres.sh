#!/usr/bin/env bash
# ============================================================================
# Scenario: the recorder history of the fast cluster in Postgres.
#
# fast-worker-1 and fast-worker-2 (cluster "fast cluster") run with
# DK_UI__RECORDER__PERSISTENCE: "postgres", the table prefix "fast" and a
# retention of 2 hours (docker-compose.yml), so their recorder events go to
# the table drakkar.fast_recorder_events on postgres-fleet. The main cluster
# keeps its own modes. The scenario checks, in order:
#   1. the table exists, its comment is retention_hours=2, and the
#      partitions of the current hour to 3 hours ahead exist,
#   2. the rows of cluster 'fast cluster' grow over 30 s, with both worker
#      names,
#   3. fast-worker-1 /api/v1/debug/trace?partition=&offset=&history=1, for
#      the (partition, offset) of one consumed row, answers 200 with a
#      non-empty list: every row has worker_name, one row is consumed, and
#      no row has a cluster key,
#   4. a partition of the hour 5 hours ago, created by hand, is gone within
#      60 s of a restart of fast-worker-1 (its start runs an upkeep), and a
#      fast worker logged recorder_postgres_partitions_changed with it
#      (fast-worker-2's own upkeep every 300 s may drop it first),
#   5. while postgres-fleet is paused for 60 s, fast-worker-1's
#      drakkar_recorder_postgres_queue_rows rises above 0; within 60 s of
#      the unpause it falls below batch_rows, and
#      drakkar_recorder_postgres_rows_written_total rises;
#      drakkar_recorder_postgres_dropped_rows_total does not change for
#      any reason,
#   6. while postgres-fleet is paused for 15 s, fast-worker-1's history
#      search answers 503 {"detail": "history search failed"} within
#      ui.trace.history_max_seconds plus 2 s,
#   7. fast-worker-1 /api/v1/debug/databases has exactly one own row, of
#      kind memory.
# The database checks run psql in the postgres-fleet container. The verdict
# line carries the numbers of the run.
#
# Runs with every provider. With provider=postgres the pauses also touch the
# main cluster's registry and election, as the service-outage scenario does.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-recorder-postgres.sh
# ============================================================================

# Some functions below run only through trap or wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-recorder-postgres
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

FAST_UI_PORT=8084
FAST_METRICS_PORT=9094
FAST_SERVICE=fast-worker-1
OTHER_FAST_SERVICE=fast-worker-2
PAUSED_SERVICE=postgres-fleet
# The user and database of postgres-fleet (docker-compose.yml).
PG_USER=drakkar
PG_DATABASE=drakkar_fleet
SCHEMA=drakkar
TABLE=fast_recorder_events
CLUSTER="fast cluster"
RETENTION_COMMENT=retention_hours=2
# The partitions the upkeep keeps ahead of the current hour.
PARTITIONS_AHEAD_HOURS=3
# A hand-made partition this many hours back is past the retention of 2 h.
EXPIRED_HOURS_AGO=5
# The create statement of a partition, as the recorder runs it.
PARTITION_CREATE_TEMPLATE='CREATE TABLE IF NOT EXISTS {schema}.{partition} PARTITION OF {schema}.{table} FOR VALUES FROM ({from}) TO ({to})'
# How long the table may take to appear on a fresh harness.
TABLE_LIMIT_SECONDS=60
# An upkeep runs at the start and every 300 s; right after an hour boundary
# the newest partition may wait for the next one.
PARTITIONS_LIMIT_SECONDS=330
ROWS_GROWTH_SECONDS=30
DROP_LIMIT_SECONDS=60
READY_LIMIT_SECONDS=90
QUEUE_PAUSE_SECONDS=60
DRAIN_LIMIT_SECONDS=60
# ui.recorder.postgres.batch_rows (the default; the fast workers set none).
BATCH_ROWS=5000
SEARCH_PAUSE_SECONDS=15
# ui.trace.history_max_seconds (the default; the fast workers set none).
HISTORY_MAX_SECONDS=10
SEARCH_SLACK_SECONDS=2
SEARCH_FAILED_BODY='{"detail": "history search failed"}'
QUEUE_METRIC=drakkar_recorder_postgres_queue_rows
WRITTEN_METRIC=drakkar_recorder_postgres_rows_written_total
DROPPED_METRIC=drakkar_recorder_postgres_dropped_rows_total

scratch=$(mktemp -d)
is_paused=false
# Never leave the service paused, also when a check fails half way.
cleanup() {
    if [ "$is_paused" = true ]; then
        "${COMPOSE[@]}" unpause "$PAUSED_SERVICE" >/dev/null 2>&1 || true
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT

pause_database() {
    "${COMPOSE[@]}" pause "$PAUSED_SERVICE" >/dev/null
    is_paused=true
}

unpause_database() {
    "${COMPOSE[@]}" unpause "$PAUSED_SERVICE" >/dev/null
    is_paused=false
}

# Run one SQL statement in postgres-fleet; print the rows unaligned, one per
# line, with no header.
psql_query() {
    "${COMPOSE[@]}" exec -T "$PAUSED_SERVICE" psql -q -t -A -v ON_ERROR_STOP=1 \
        -U "$PG_USER" -d "$PG_DATABASE" -c "$1"
}

table_exists() {
    [ "$(psql_query "SELECT to_regclass('${SCHEMA}.${TABLE}') IS NOT NULL")" = t ]
}

# The partitions of the table, sorted, one per line.
listed_partitions() {
    psql_query "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = to_regclass('${SCHEMA}.${TABLE}') ORDER BY c.relname"
}

# The partition name of the epoch hour $1: <table>_pYYYYMMDDHH in UTC.
partition_name() {
    python3 -c '
import sys
from datetime import UTC, datetime
moment = datetime.fromtimestamp(int(sys.argv[2]) * 3600, UTC)
print(f"{sys.argv[1]}_p{moment:%Y%m%d%H}")
' "$TABLE" "$1"
}

# The epoch hour of now, minus $1 hours.
hour_ago() {
    python3 -c 'import sys, time; print(int(time.time() // 3600) - int(sys.argv[1]))' "$1"
}

# The partitions of the current hour to PARTITIONS_AHEAD_HOURS ahead that
# are not listed, one per line.
missing_partitions() {
    local listed hour offset name
    listed=$(listed_partitions) || return 1
    hour=$(hour_ago 0)
    for offset in $(seq 0 "$PARTITIONS_AHEAD_HOURS"); do
        name=$(partition_name $((hour + offset)))
        grep -qxF "$name" <<<"$listed" || echo "$name"
    done
}

partitions_ready() {
    local missing
    missing=$(missing_partitions) || return 1
    [ -z "$missing" ]
}

# The rows of the fast cluster: <count> <workers, comma-separated>.
cluster_rows() {
    psql_query "SELECT count(*) || ' ' || coalesce(string_agg(DISTINCT worker, ',' ORDER BY worker), '-') FROM ${SCHEMA}.${TABLE} WHERE cluster = '${CLUSTER}'"
}

# The docker time now, in seconds, as `docker logs --since` takes it. The
# clock of a container is the clock the log timestamps come from.
docker_clock() {
    "${COMPOSE[@]}" exec -T "$PAUSED_SERVICE" date -u +%Y-%m-%dT%H:%M:%SZ
}

# Print the log of service $1 since the docker time $2.
service_log_since() {
    "${COMPOSE[@]}" logs --no-color --no-log-prefix --since="$2" "$1" 2>/dev/null
}

# True when service $1 logged recorder_postgres_partitions_changed with the
# partition $3 since the docker time $2. The log is read whole first: a
# grep -q that stops reading early would break the pipe under pipefail.
logged_partition_change() {
    local lines
    lines=$(service_log_since "$1" "$2" | grep recorder_postgres_partitions_changed || true)
    grep -qF "$3" <<<"$lines"
}

# The fast workers that logged the drop of the expired partition since the
# docker time it was created, comma-separated, or nothing.
expired_partition_droppers() {
    local service droppers=
    for service in "$FAST_SERVICE" "$OTHER_FAST_SERVICE"; do
        if logged_partition_change "$service" "$created_at" "$expired_partition"; then
            droppers=${droppers:+${droppers},}${service}
        fi
    done
    echo "$droppers"
}

# True when the expired partition is gone and a fast worker logged its drop;
# the workers that logged it are left in $dropped_by.
partition_gone_and_logged() {
    local listed
    listed=$(listed_partitions) || return 1
    grep -qxF "$expired_partition" <<<"$listed" && return 1
    dropped_by=$(expired_partition_droppers)
    [ -n "$dropped_by" ]
}

fast_worker_ready() {
    curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${FAST_UI_PORT}/readyz"
}

# A number from a metric reading, or exit 1 for absent or unreachable.
#   number_of <value>
number_of() {
    case "$1" in
        absent | unreachable) return 1 ;;
        *) echo "$1" ;;
    esac
}

# True when the number $1 is greater than the number $2.
greater() {
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)' "$1" "$2"
}

# A number as a whole number for the verdict line.
whole() {
    python3 -c 'import sys; print(int(float(sys.argv[1])))' "$1"
}

queue_below_batch() {
    local value
    value=$(number_of "$(metric "$FAST_METRICS_PORT" "$QUEUE_METRIC")") || return 1
    greater "$BATCH_ROWS" "$value"
}

written_rose() {
    local value
    value=$(number_of "$(metric "$FAST_METRICS_PORT" "$WRITTEN_METRIC")") || return 1
    greater "$value" "$written_before"
}

step "Scenario: the recorder history of the fast cluster in Postgres"

# Check 1: the table, its retention comment and the partitions ahead.
wait_until "$TABLE_LIMIT_SECONDS" table_exists \
    || fail "check 1: the table ${SCHEMA}.${TABLE} does not exist ${TABLE_LIMIT_SECONDS}s after the scenario started"
comment=$(psql_query "SELECT coalesce(obj_description('${SCHEMA}.${TABLE}'::regclass, 'pg_class'), '<null>')") \
    || fail "check 1: the comment of ${SCHEMA}.${TABLE} could not be read"
[ "$comment" = "$RETENTION_COMMENT" ] \
    || fail "check 1: the comment of ${SCHEMA}.${TABLE} is '${comment}', expected '${RETENTION_COMMENT}'"
if ! wait_until "$PARTITIONS_LIMIT_SECONDS" partitions_ready; then
    fail "check 1: partitions missing ${PARTITIONS_LIMIT_SECONDS}s after the scenario started: $(missing_partitions | tr '\n' ' ')(listed: $(listed_partitions | tr '\n' ' '))"
fi
partitions=$(listed_partitions | tr '\n' ' ')
partitions=${partitions% }
ok "check 1: ${SCHEMA}.${TABLE} has the comment ${comment} and the partitions ${partitions}"

# Check 2: both fast workers keep writing rows.
counted=$(cluster_rows) || fail "check 2: the rows of '${CLUSTER}' could not be counted"
read -r rows_before _ <<<"$counted"
sleep "$ROWS_GROWTH_SECONDS"
counted=$(cluster_rows) || fail "check 2: the rows of '${CLUSTER}' could not be counted"
read -r rows_after workers <<<"$counted"
[ "$rows_after" -gt "$rows_before" ] \
    || fail "check 2: the rows of '${CLUSTER}' went from ${rows_before} to ${rows_after} in ${ROWS_GROWTH_SECONDS}s, expected growth"
[ "$workers" = "fast-worker-1,fast-worker-2" ] \
    || fail "check 2: the rows of '${CLUSTER}' name the workers ${workers}, expected fast-worker-1,fast-worker-2"
ok "check 2: the rows of '${CLUSTER}' went from ${rows_before} to ${rows_after} in ${ROWS_GROWTH_SECONDS}s, workers ${workers}"

# Check 3: the history search of fast-worker-1 finds a consumed message.
consumed=$(psql_query "SELECT \"partition\" || ' ' || \"offset\" FROM ${SCHEMA}.${TABLE} WHERE cluster = '${CLUSTER}' AND event = 'consumed' AND \"partition\" IS NOT NULL AND \"offset\" IS NOT NULL ORDER BY ts DESC LIMIT 1") \
    || fail "check 3: the consumed rows could not be read"
read -r trace_partition trace_offset <<<"$consumed"
[ -n "${trace_offset:-}" ] || fail "check 3: the table has no consumed row of '${CLUSTER}'"
trace_path="/api/v1/debug/trace?partition=${trace_partition}&offset=${trace_offset}&history=1"
# curl's long options take no `=`, so they keep the space form here.
status=$(curl -s --connect-timeout 2 --max-time 30 -o "$scratch/trace.json" -w '%{http_code}' \
    "http://localhost:${FAST_UI_PORT}${trace_path}" || true)
[ "$status" = 200 ] || fail "check 3: GET :${FAST_UI_PORT}${trace_path} answered ${status}: $(head -c 200 "$scratch/trace.json" 2>/dev/null)"
trace_summary=$(python3 -c '
import json, sys
rows = json.load(open(sys.argv[1]))
if not isinstance(rows, list) or not rows:
    sys.exit("an empty answer or no list")
if not all("worker_name" in row for row in rows):
    sys.exit("a row without worker_name")
if not any(row.get("event") == "consumed" for row in rows):
    sys.exit("no consumed row")
if any("cluster" in row for row in rows):
    sys.exit("a row with a cluster key")
workers = ",".join(sorted({str(row["worker_name"]) for row in rows}))
print(f"{len(rows)} rows, workers {workers}")
' "$scratch/trace.json" 2>&1) || fail "check 3: GET :${FAST_UI_PORT}${trace_path} answered ${trace_summary}"
ok "check 3: GET :${FAST_UI_PORT}${trace_path} answered ${trace_summary}"

# Check 4: an upkeep drops an expired partition. The restart of fast-worker-1
# runs one at its start; fast-worker-2's periodic upkeep may come first, so
# the logs of both fast workers count, from the docker time before the create.
expired_hour=$(hour_ago "$EXPIRED_HOURS_AGO")
expired_partition=$(partition_name "$expired_hour")
create_sql=${PARTITION_CREATE_TEMPLATE//\{schema\}/$SCHEMA}
create_sql=${create_sql//\{table\}/$TABLE}
create_sql=${create_sql//\{partition\}/$expired_partition}
create_sql=${create_sql//\{from\}/$((expired_hour * 3600))}
create_sql=${create_sql//\{to\}/$(((expired_hour + 1) * 3600))}
created_at=$(docker_clock) || fail "check 4: the docker clock could not be read"
psql_query "$create_sql" >/dev/null || fail "check 4: the partition ${expired_partition} could not be created"
grep -qxF "$expired_partition" <<<"$(listed_partitions)" \
    || fail "check 4: the partition ${expired_partition} is not listed after it was created"
log "created ${expired_partition}; restarting ${FAST_SERVICE}"
"${COMPOSE[@]}" restart "$FAST_SERVICE" >/dev/null || fail "check 4: ${FAST_SERVICE} could not be restarted"
dropped_by=
if ! wait_until "$DROP_LIMIT_SECONDS" partition_gone_and_logged; then
    still=no
    grep -qxF "$expired_partition" <<<"$(listed_partitions)" && still=yes
    logged_by=$(expired_partition_droppers)
    fail "check 4: ${DROP_LIMIT_SECONDS}s after the restart of ${FAST_SERVICE}: ${expired_partition} still listed ${still}, logged as changed by ${logged_by:-no fast worker}"
fi
drop_seconds=$WAITED
ok "check 4: ${expired_partition} was dropped and logged by ${dropped_by} ${drop_seconds}s after the restart of ${FAST_SERVICE}"
wait_until "$READY_LIMIT_SECONDS" fast_worker_ready \
    || fail "check 4: ${FAST_SERVICE} /readyz did not answer 200 within ${READY_LIMIT_SECONDS}s of its restart"

# Check 5: a paused database keeps the rows in the queue, and nothing is dropped.
queue_before=$(number_of "$(metric "$FAST_METRICS_PORT" "$QUEUE_METRIC")") \
    || fail "check 5: ${QUEUE_METRIC} could not be read"
written_before=$(number_of "$(metric "$FAST_METRICS_PORT" "$WRITTEN_METRIC")") \
    || fail "check 5: ${WRITTEN_METRIC} could not be read"
dropped_before=$(number_of "$(metric_sum "$FAST_METRICS_PORT" "$DROPPED_METRIC")") \
    || fail "check 5: ${DROPPED_METRIC} could not be read"
log "before the pause: queue ${queue_before}, written ${written_before}, dropped ${dropped_before}; pausing ${PAUSED_SERVICE} for ${QUEUE_PAUSE_SECONDS}s"
pause_database
paused_at=$SECONDS
queue_peak=0
while [ $((SECONDS - paused_at)) -lt "$QUEUE_PAUSE_SECONDS" ]; do
    value=$(metric "$FAST_METRICS_PORT" "$QUEUE_METRIC")
    if value=$(number_of "$value") && greater "$value" "$queue_peak"; then
        queue_peak=$value
    fi
    sleep "$POLL_SECONDS"
done
unpause_database
greater "$queue_peak" 0 \
    || fail "check 5: ${QUEUE_METRIC} stayed at 0 while ${PAUSED_SERVICE} was paused for ${QUEUE_PAUSE_SECONDS}s"
wait_until "$DRAIN_LIMIT_SECONDS" queue_below_batch \
    || fail "check 5: ${QUEUE_METRIC} is $(metric "$FAST_METRICS_PORT" "$QUEUE_METRIC") ${DRAIN_LIMIT_SECONDS}s after the unpause, expected below ${BATCH_ROWS}"
drain_seconds=$WAITED
wait_until "$DRAIN_LIMIT_SECONDS" written_rose \
    || fail "check 5: ${WRITTEN_METRIC} stayed at $(metric "$FAST_METRICS_PORT" "$WRITTEN_METRIC") ${DRAIN_LIMIT_SECONDS}s after the unpause (before the pause: ${written_before})"
written_seconds=$WAITED
written_after=$(metric "$FAST_METRICS_PORT" "$WRITTEN_METRIC")
dropped_after=$(number_of "$(metric_sum "$FAST_METRICS_PORT" "$DROPPED_METRIC")") \
    || fail "check 5: ${DROPPED_METRIC} could not be read after the unpause"
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) == float(sys.argv[2]) else 1)' "$dropped_before" "$dropped_after" \
    || fail "check 5: ${DROPPED_METRIC} went from ${dropped_before} to ${dropped_after}, expected no change"
queue_peak=$(whole "$queue_peak")
ok "check 5: queue peak ${queue_peak} rows, below ${BATCH_ROWS} ${drain_seconds}s and written rose ${written_seconds}s after the unpause (${written_before} -> ${written_after}), dropped ${dropped_after} unchanged"

# Check 6: the history search answers 503 in time while the database is paused.
log "pausing ${PAUSED_SERVICE} for ${SEARCH_PAUSE_SECONDS}s"
pause_database
paused_at=$SECONDS
search=$(curl -s --connect-timeout 2 --max-time 30 -o "$scratch/search.json" -w '%{http_code} %{time_total}' \
    "http://localhost:${FAST_UI_PORT}${trace_path}" || true)
sleep_until_elapsed "$paused_at" "$SEARCH_PAUSE_SECONDS"
unpause_database
read -r search_status search_seconds <<<"$search"
search_body=$(cat "$scratch/search.json" 2>/dev/null || true)
[ "$search_status" = 503 ] \
    || fail "check 6: GET :${FAST_UI_PORT}${trace_path} answered ${search_status} after ${search_seconds:-?}s with ${PAUSED_SERVICE} paused, expected 503: $(head -c 200 <<<"$search_body")"
python3 -c 'import json, sys; sys.exit(0 if json.loads(sys.argv[1]) == json.loads(sys.argv[2]) else 1)' "$search_body" "$SEARCH_FAILED_BODY" 2>/dev/null \
    || fail "check 6: the 503 body is $(head -c 200 <<<"$search_body"), expected ${SEARCH_FAILED_BODY}"
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)' "$search_seconds" $((HISTORY_MAX_SECONDS + SEARCH_SLACK_SECONDS)) \
    || fail "check 6: the 503 came after ${search_seconds}s, expected at most $((HISTORY_MAX_SECONDS + SEARCH_SLACK_SECONDS))s"
ok "check 6: GET :${FAST_UI_PORT}${trace_path} answered 503 ${SEARCH_FAILED_BODY} after ${search_seconds}s"

# Check 7: fast-worker-1 has only its memory store among its own databases.
databases=$(curl -sf --connect-timeout 2 --max-time 15 "http://localhost:${FAST_UI_PORT}/api/v1/debug/databases") \
    || fail "check 7: GET :${FAST_UI_PORT}/api/v1/debug/databases could not be read"
own_kinds=$(python3 -c '
import json, sys
own = json.load(sys.stdin).get("own", [])
print(",".join(str(row.get("kind")) for row in own) or "-")
' <<<"$databases") || fail "check 7: GET :${FAST_UI_PORT}/api/v1/debug/databases answered no JSON: $(head -c 200 <<<"$databases")"
[ "$own_kinds" = memory ] \
    || fail "check 7: GET :${FAST_UI_PORT}/api/v1/debug/databases shows own=${own_kinds}, expected exactly one memory row"
ok "check 7: GET :${FAST_UI_PORT}/api/v1/debug/databases shows own=${own_kinds}"

pass "rows ${rows_after} (workers ${workers}); partitions ${partitions}; expired partition dropped by ${dropped_by} ${drop_seconds}s after the restart; queue peak ${queue_peak}, drained ${drain_seconds}s, written ${written_seconds}s after the unpause; 503 after ${search_seconds}s"
