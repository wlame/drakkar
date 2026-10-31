#!/usr/bin/env bash
# ============================================================================
# Scenario: the recorder memory store, on a worker with no recorder file and
# on a worker with one.
#
# worker-1 runs with DK_RECORDER__ENABLED: "false" and a memory window of
# 60 s (DK_RECORDER__MEMORY__WINDOW: "1m" in docker-compose.yml); worker-2
# keeps the recorder file (provider sqlite, the default). The scenario checks,
# in order:
#   1. worker-1 /api/v1/debug/databases: exactly one `own` row, of kind
#      memory, and no live row; history.folders is not empty. The workers
#      seal their own files only at a rotation (hourly), so when the shared
#      history folder is still empty (a fresh harness, this scenario run
#      alone) one small due window is planted first (PLANT_HISTORY_CMD, see
#      scenario-lib.sh),
#   2. worker-1 /api/v1/events?limit=50 returns at least one row (host
#      samples are recorded also while the worker is idle),
#   3. once worker-1 has run for at least 90 s, its metrics show
#      drakkar_recorder_memory_rows > 0,
#      drakkar_recorder_memory_evicted_rows_total{reason="window"} > 0,
#      drakkar_recorder_memory_oldest_age_seconds <= 75 (the window of 60 s,
#      one flush interval and slack) and
#      drakkar_recorder_memory_allocated_bytes >= drakkar_recorder_memory_bytes,
#   4. worker-1 /api/v1/debug/download-live without source answers 200 with
#      a SQLite database (the memory store), and with source=file 404,
#   5. worker-2 /api/v1/debug/databases has a memory row and a live row in
#      `own`, and download-live with source=file and with source=memory both
#      answer a SQLite database.
# The verdict line carries the memory numbers of worker-1 and the count of
# ui_read_memory_timed_out lines in the logs of the main-cluster workers.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-recorder-memory.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-recorder-memory
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

WORKER_1_UI_PORT=8081
WORKER_1_METRICS_PORT=9090
WORKER_2_UI_PORT=8082
# The first 16 bytes of every SQLite database file, as hex.
SQLITE_HEADER_HEX=53514c69746520666f726d6174203300
# worker-1 must have run this long before its eviction is checked: the
# memory window (60 s) plus room for the first eviction pass.
MIN_UPTIME_SECONDS=90
# How long the metrics may take to reach the expected values after that.
METRICS_LIMIT_SECONDS=60
# The oldest event in worker-1's memory store may be at most this old.
MAX_OLDEST_AGE_SECONDS=75
# How long the history folder may take to show a planted file.
HISTORY_LIMIT_SECONDS=20
# One small due window, when nothing is in the history folder yet. Six
# hours back keeps clear of the window of the unclean-exit scenario.
PLANT_HOURS_AGO=6
SHARED_DIR="$SCENARIO_DIR/shared"
# A memory snapshot may take up to 30 s on the worker.
DOWNLOAD_MAX_SECONDS=90

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

# The /api/v1/debug/databases answer of the worker on host port $1.
databases_json() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf --connect-timeout 2 --max-time 15 "http://localhost:$1/api/v1/debug/databases"
}

# A short summary of a databases answer (JSON on stdin), one line:
#   own=<kind>,<kind>,... folders=<n>
databases_summary() {
    python3 -c '
import json, sys
answer = json.load(sys.stdin)
kinds = ",".join(str(row.get("kind")) for row in answer.get("own", [])) or "-"
folders = answer.get("history", {}).get("folders", [])
print(f"own={kinds} folders={len(folders)}")
'
}

# True when the history.folders of a databases answer (JSON on stdin) is not
# empty.
has_history_folders() {
    python3 -c '
import json, sys
sys.exit(0 if json.load(sys.stdin).get("history", {}).get("folders") else 1)
'
}

# True when worker-1's databases answer lists a history folder.
worker_1_history_listed() {
    local answer
    answer=$(databases_json "$WORKER_1_UI_PORT") || return 1
    has_history_folders <<<"$answer"
}

# Download $2 of the worker on host port $1 into the file $3; print the
# HTTP status (000 when no answer came).
#   download <port> <path and query> <file>
download() {
    curl -s --connect-timeout 2 --max-time "$DOWNLOAD_MAX_SECONDS" -o "$3" -w '%{http_code}' \
        "http://localhost:$1$2" || true
}

# The first 16 bytes of file $1, as hex.
first_bytes_hex() {
    python3 -c 'import sys; print(open(sys.argv[1], "rb").read(16).hex())' "$1"
}

# Check that $2 of the worker on host port $1 answers 200 with a SQLite
# database; fail with the check name $3 otherwise.
#   expect_sqlite_download <port> <path and query> <check>
expect_sqlite_download() {
    local file status header
    file=$(mktemp "$scratch/download.XXXXXX")
    status=$(download "$1" "$2" "$file")
    [ "$status" = 200 ] || fail "$3: GET :$1$2 answered ${status}, expected 200 ($(head -c 200 "$file" | tr '\n' ' '))"
    header=$(first_bytes_hex "$file")
    [ "$header" = "$SQLITE_HEADER_HEX" ] \
        || fail "$3: GET :$1$2 answered 200 without the SQLite header (first 16 bytes ${header:-none}, expected ${SQLITE_HEADER_HEX})"
    ok "$3: GET :$1$2 answered 200 with a SQLite database of $(wc -c <"$file" | tr -d ' ') bytes"
    rm -f "$file"
}

# Seconds since the container of compose service $1 started.
container_uptime() {
    docker inspect -f '{{.State.StartedAt}}' "$(container_id "$1")" | python3 -c '
import sys, time
from datetime import datetime
text = sys.stdin.read().strip()
# Docker writes nanoseconds; Python reads at most microseconds.
head, _, rest = text.partition(".")
fraction = "".join(ch for ch in rest if ch.isdigit())[:6]
started = datetime.fromisoformat(f"{head}.{fraction or 0}+00:00")
print(int(time.time() - started.timestamp()))
'
}

# The memory numbers of worker-1, one line:
#   rows=<n> evicted_window=<n> oldest_age=<s> allocated=<bytes> used=<bytes>
memory_numbers() {
    echo "rows=$(metric "$WORKER_1_METRICS_PORT" drakkar_recorder_memory_rows)" \
        "evicted_window=$(metric "$WORKER_1_METRICS_PORT" drakkar_recorder_memory_evicted_rows_total 'reason="window"')" \
        "oldest_age=$(metric "$WORKER_1_METRICS_PORT" drakkar_recorder_memory_oldest_age_seconds)" \
        "allocated=$(metric "$WORKER_1_METRICS_PORT" drakkar_recorder_memory_allocated_bytes)" \
        "used=$(metric "$WORKER_1_METRICS_PORT" drakkar_recorder_memory_bytes)"
}

# True when the memory numbers line $1 meets check 3. A value that is
# absent (no eviction yet) or unreachable does not meet it.
memory_numbers_ok() {
    MAX_OLDEST="$MAX_OLDEST_AGE_SECONDS" python3 -c '
import os, sys
values = dict(pair.split("=", 1) for pair in sys.argv[1].split())
try:
    numbers = {key: float(value) for key, value in values.items()}
except ValueError:
    sys.exit(1)
ok = (
    numbers["rows"] > 0
    and numbers["evicted_window"] > 0
    and numbers["oldest_age"] <= float(os.environ["MAX_OLDEST"])
    and numbers["allocated"] >= numbers["used"]
)
sys.exit(0 if ok else 1)
' "$1"
}

worker_1_memory_ok() {
    memory_numbers_ok "$(memory_numbers)"
}

# The number of ui_read_memory_timed_out lines in the logs of the
# main-cluster workers, as <worker>=<n> pairs.
memory_timeout_counts() {
    local service count pairs=()
    for service in "${MAIN_WORKERS[@]}"; do
        count=$("${COMPOSE[@]}" logs --no-color --no-log-prefix "$service" 2>/dev/null | grep -c ui_read_memory_timed_out || true)
        pairs+=("${service}=${count:-0}")
    done
    echo "${pairs[*]}"
}

step "Scenario: the recorder memory store on worker-1 (no file) and worker-2 (file)"

# Check 1: worker-1 has only the memory store, and the shared history is listed.
answer=$(databases_json "$WORKER_1_UI_PORT") || fail "check 1: worker-1 /api/v1/debug/databases could not be read"
if ! has_history_folders <<<"$answer"; then
    log "the shared history folder is empty: planting one due window of one file"
    "${PLANT_HISTORY_CMD[@]}" --shared-dir="$SHARED_DIR" --files=1 --events-per-file=1 --hours-ago="$PLANT_HOURS_AGO" \
        || fail "check 1: the history folder is empty and planting one file failed"
    wait_until "$HISTORY_LIMIT_SECONDS" worker_1_history_listed \
        || fail "check 1: worker-1 lists no history folder ${HISTORY_LIMIT_SECONDS}s after one file was planted"
    answer=$(databases_json "$WORKER_1_UI_PORT") || fail "check 1: worker-1 /api/v1/debug/databases could not be read"
fi
summary=$(databases_summary <<<"$answer")
python3 -c '
import json, sys
answer = json.load(sys.stdin)
own = answer.get("own", [])
ok = len(own) == 1 and own[0].get("kind") == "memory" and answer.get("history", {}).get("folders")
sys.exit(0 if ok else 1)
' <<<"$answer" || fail "check 1: worker-1 /api/v1/debug/databases shows ${summary}, expected own=memory (exactly one row, no live row) and at least one history folder"
ok "check 1: worker-1 /api/v1/debug/databases shows ${summary}"

# Check 2: worker-1 answers events from the memory store.
events=$(curl -sf --connect-timeout 2 --max-time 10 "http://localhost:${WORKER_1_UI_PORT}/api/v1/events?limit=50") \
    || fail "check 2: worker-1 /api/v1/events?limit=50 could not be read"
event_rows=$(python3 -c 'import json, sys; print(len(json.load(sys.stdin)))' <<<"$events") \
    || fail "check 2: worker-1 /api/v1/events?limit=50 answered no JSON list: $(head -c 200 <<<"$events")"
[ "$event_rows" -gt 0 ] || fail "check 2: worker-1 /api/v1/events?limit=50 answered ${event_rows} rows, expected at least 1"
ok "check 2: worker-1 /api/v1/events?limit=50 answered ${event_rows} rows"

# Check 3: the memory store of worker-1 is bounded by its window.
uptime=$(container_uptime worker-1) || fail "check 3: the start time of worker-1 could not be read"
if [ "$uptime" -lt "$MIN_UPTIME_SECONDS" ]; then
    log "worker-1 has run ${uptime}s; waiting until it has run ${MIN_UPTIME_SECONDS}s"
    sleep $((MIN_UPTIME_SECONDS - uptime))
fi
if ! wait_until "$METRICS_LIMIT_SECONDS" worker_1_memory_ok; then
    fail "check 3: worker-1 memory metrics $(memory_numbers), expected rows > 0, evicted_window > 0, oldest_age <= ${MAX_OLDEST_AGE_SECONDS} and allocated >= used"
fi
numbers=$(memory_numbers)
ok "check 3: worker-1 memory metrics ${numbers}"

# Check 4: worker-1 downloads its memory store and has no live file.
expect_sqlite_download "$WORKER_1_UI_PORT" /api/v1/debug/download-live "check 4"
status=$(download "$WORKER_1_UI_PORT" '/api/v1/debug/download-live?source=file' "$scratch/no-file")
[ "$status" = 404 ] || fail "check 4: GET :${WORKER_1_UI_PORT}/api/v1/debug/download-live?source=file answered ${status}, expected 404"
ok "check 4: GET :${WORKER_1_UI_PORT}/api/v1/debug/download-live?source=file answered 404"

# Check 5: worker-2 has both the memory store and its live file.
answer=$(databases_json "$WORKER_2_UI_PORT") || fail "check 5: worker-2 /api/v1/debug/databases could not be read"
summary=$(databases_summary <<<"$answer")
python3 -c '
import json, sys
kinds = [row.get("kind") for row in json.load(sys.stdin).get("own", [])]
sys.exit(0 if "memory" in kinds and "live" in kinds else 1)
' <<<"$answer" || fail "check 5: worker-2 /api/v1/debug/databases shows ${summary}, expected a memory row and a live row in own"
ok "check 5: worker-2 /api/v1/debug/databases shows ${summary}"
expect_sqlite_download "$WORKER_2_UI_PORT" '/api/v1/debug/download-live?source=file' "check 5"
expect_sqlite_download "$WORKER_2_UI_PORT" '/api/v1/debug/download-live?source=memory' "check 5"

timeouts=$(memory_timeout_counts)
log "ui_read_memory_timed_out lines: ${timeouts}"

pass "worker-1 memory only (${numbers}); worker-2 memory and live file; ui_read_memory_timed_out ${timeouts}"
