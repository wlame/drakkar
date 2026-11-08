#!/usr/bin/env bash
# ============================================================================
# Scenario: the routes of the Databases view answer on running workers.
#
# Checks, in order:
#   1. worker-2 GET /api/v1/debug/databases?parts=own,storage,recorder
#      answers the blocks worker, own, storage and recorder and no history
#      (a part not asked for is left out): worker.name is worker-2,
#      storage.own_folder is main_cluster/worker-2, recorder.provider is
#      sqlite, own holds a live row, and storage.siblings lists worker-1
#      and worker-3 (the three main-cluster workers share one storage.dir),
#   2. worker-2 GET /api/v1/debug/databases?parts=bogus answers 400 with
#      the unknown-part message,
#   3. worker-2 GET /api/v1/debug/slice for the last 15 minutes, at most
#      1000 rows, answers 200 with Content-Type application/x-sqlite3 and
#      X-Drakkar-Slice-Rows above 0; the file opens as SQLite, its events
#      table holds that many rows, and it has the slice and slice_sources
#      tables,
#   4. worker-1 GET /api/v1/debug/autodiscover answers the registry header
#      and lists worker-2 and worker-3; GET /api/v1/debug/autodiscover/leases
#      on worker-2 answers a lease row for history.archive,
#   5. fast-worker-1 (recorder provider postgres) GET
#      /api/v1/debug/databases/postgres answers at least one partition.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-databases-view.sh
# ============================================================================

set -euo pipefail

SCENARIO=scenario-databases-view
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

WORKER_1_UI_PORT=8081
WORKER_2_UI_PORT=8082
FAST_WORKER_1_UI_PORT=8084
OWN_FOLDER=main_cluster/worker-2
SLICE_SECONDS=900
SLICE_MAX_ROWS=1000
UNKNOWN_PART_MESSAGE="unknown part 'bogus'; known: history, own, recorder, storage"
# The first 16 bytes of every SQLite database file, as hex.
SQLITE_HEADER_HEX=53514c69746520666f726d6174203300

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

# GET a path of the worker on host port $1 into the file $3; prints the
# HTTP status (000 when nothing answered).
#   http_get <port> <path> <body file> [<header file>]
http_get() {
    local header_file=${4:-/dev/null}
    # curl's long options take no `=`, so they keep the space form here.
    curl -s --connect-timeout 2 --max-time 90 -o "$3" -D "$header_file" -w '%{http_code}' "http://localhost:$1$2" || true
}

# A short form of a file for a failure text.
excerpt() {
    head -c 600 "$1" | tr '\n' ' '
}

step "Scenario: the Databases view routes"

# 1. The parts of one worker.
status=$(http_get "$WORKER_2_UI_PORT" "/api/v1/debug/databases?parts=own,storage,recorder" "$scratch/parts.json")
[ "$status" = 200 ] || fail "worker-2 /api/v1/debug/databases?parts=own,storage,recorder answered ${status}: $(excerpt "$scratch/parts.json")"
verdict=$(OWN_FOLDER="$OWN_FOLDER" python3 -c '
import json, os, sys
answer = json.load(open(sys.argv[1]))
problems = []
for key in ("worker", "own", "storage", "recorder"):
    if key not in answer:
        problems.append(f"no {key} block")
if "history" in answer:
    problems.append("a history block that was not asked for")
if not problems:
    worker_name = answer["worker"].get("name")
    if worker_name != "worker-2":
        problems.append(f"worker.name is {worker_name!r}")
    storage = answer["storage"]
    own_folder = storage.get("own_folder")
    if own_folder != os.environ["OWN_FOLDER"]:
        problems.append(f"storage.own_folder is {own_folder!r}")
    siblings = {row.get("worker") for row in storage.get("siblings") or []}
    for name in ("worker-1", "worker-3"):
        if name not in siblings:
            problems.append(f"storage.siblings has no {name} (it has {sorted(siblings)})")
    provider = answer["recorder"].get("provider")
    if provider != "sqlite":
        problems.append(f"recorder.provider is {provider!r}")
    kinds = [row.get("kind") for row in answer["own"]]
    if "live" not in kinds:
        problems.append(f"own has no live row (kinds {kinds})")
print("; ".join(problems) or "ok")
' "$scratch/parts.json")
[ "$verdict" = ok ] || fail "worker-2 parts answer: ${verdict}"
ok "worker-2 answers the worker, own, storage and recorder blocks; own folder ${OWN_FOLDER}, siblings worker-1 and worker-3"

# 2. An unknown part.
status=$(http_get "$WORKER_2_UI_PORT" "/api/v1/debug/databases?parts=bogus" "$scratch/bogus.json")
[ "$status" = 400 ] || fail "worker-2 /api/v1/debug/databases?parts=bogus answered ${status}, expected 400: $(excerpt "$scratch/bogus.json")"
grep -qF "$UNKNOWN_PART_MESSAGE" "$scratch/bogus.json" \
    || fail "the 400 of parts=bogus does not carry \"${UNKNOWN_PART_MESSAGE}\": $(excerpt "$scratch/bogus.json")"
ok "parts=bogus answers 400 with the unknown-part message"

# 3. A small slice.
to=$(python3 -c 'import time; print(int(time.time()))')
from=$((to - SLICE_SECONDS))
status=$(http_get "$WORKER_2_UI_PORT" "/api/v1/debug/slice?from=${from}&to=${to}&max_rows=${SLICE_MAX_ROWS}" "$scratch/slice.db" "$scratch/slice.headers")
[ "$status" = 200 ] || fail "worker-2 /api/v1/debug/slice answered ${status}: $(excerpt "$scratch/slice.db")"
# The value of the header $1 of the slice answer (the name in any letter case).
header() {
    tr -d '\r' <"$scratch/slice.headers" \
        | awk -v name="$1" 'index(tolower($0), tolower(name) ":") == 1 { sub(/^[^:]*:[ \t]*/, ""); value = $0 } END { print value }'
}
content_type=$(header Content-Type)
[ "$content_type" = application/x-sqlite3 ] || fail "the slice answered Content-Type '${content_type}', expected application/x-sqlite3"
header_rows=$(header X-Drakkar-Slice-Rows)
case "$header_rows" in
    '' | *[!0-9]*) fail "the slice answered X-Drakkar-Slice-Rows '${header_rows}', expected a number" ;;
esac
[ "$header_rows" -gt 0 ] || fail "the slice of the last ${SLICE_SECONDS}s of worker-2 has no rows"
[ "$(head -c 16 "$scratch/slice.db" | od -An -tx1 | tr -d ' \n')" = "$SQLITE_HEADER_HEX" ] \
    || fail "the slice file does not start with the SQLite header"
slice_check=$(python3 -c '
import sqlite3, sys
db = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type = ?", ("table",))}
missing = [name for name in ("events", "slice", "slice_sources") if name not in tables]
if missing:
    print("missing tables " + ", ".join(missing))
else:
    events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    recorded = db.execute("SELECT rows FROM slice").fetchone()[0]
    sources = db.execute("SELECT COUNT(*) FROM slice_sources").fetchone()[0]
    print(f"{events} {recorded} {sources}")
' "$scratch/slice.db")
read -r slice_events slice_recorded slice_sources <<<"$slice_check"
case "$slice_events" in
    '' | *[!0-9]*) fail "the slice file: ${slice_check}" ;;
esac
[ "$slice_events" = "$header_rows" ] && [ "$slice_recorded" = "$header_rows" ] \
    || fail "the slice file holds ${slice_events} events and its slice table says ${slice_recorded}, the header says ${header_rows}"
ok "slice of the last ${SLICE_SECONDS}s of worker-2: ${slice_events} rows from ${slice_sources} sources, a SQLite file with the slice tables"

# 4. The registry and the leases.
status=$(http_get "$WORKER_1_UI_PORT" /api/v1/debug/autodiscover "$scratch/autodiscover.json")
[ "$status" = 200 ] || fail "worker-1 /api/v1/debug/autodiscover answered ${status}: $(excerpt "$scratch/autodiscover.json")"
verdict=$(PROVIDER="$HARNESS_PROVIDER" python3 -c '
import json, os, sys
answer = json.load(open(sys.argv[1]))
problems = []
registry = answer.get("registry") or {}
provider = registry.get("provider")
if provider != os.environ["PROVIDER"]:
    problems.append(f"registry.provider is {provider!r}")
names = {row.get("worker") for row in answer.get("workers", [])}
for name in ("worker-2", "worker-3"):
    if name not in names:
        problems.append(f"workers has no {name} (it has {sorted(names)})")
print("; ".join(problems) or "ok")
' "$scratch/autodiscover.json")
[ "$verdict" = ok ] || fail "worker-1 autodiscover answer: ${verdict}"
status=$(http_get "$WORKER_2_UI_PORT" /api/v1/debug/autodiscover/leases "$scratch/leases.json")
[ "$status" = 200 ] || fail "worker-2 /api/v1/debug/autodiscover/leases answered ${status}: $(excerpt "$scratch/leases.json")"
lease_status=$(python3 -c '
import json, sys
answer = json.load(open(sys.argv[1]))
for row in answer.get("leases", []):
    if row.get("task") == "history.archive":
        print(row.get("status"))
        break
' "$scratch/leases.json")
[ -n "$lease_status" ] || fail "worker-2 lists no lease row for history.archive: $(excerpt "$scratch/leases.json")"
ok "autodiscover lists worker-2 and worker-3 with provider ${HARNESS_PROVIDER}; the history.archive lease row of worker-2 has status ${lease_status}"

# 5. The Postgres partitions of the fast cluster.
status=$(http_get "$FAST_WORKER_1_UI_PORT" /api/v1/debug/databases/postgres "$scratch/postgres.json")
[ "$status" = 200 ] || fail "fast-worker-1 /api/v1/debug/databases/postgres answered ${status}: $(excerpt "$scratch/postgres.json")"
partitions=$(python3 -c 'import json, sys; print(len(json.load(open(sys.argv[1])).get("partitions", [])))' "$scratch/postgres.json")
[ "$partitions" -gt 0 ] || fail "fast-worker-1 lists no partition of its recorder table: $(excerpt "$scratch/postgres.json")"
ok "fast-worker-1 lists ${partitions} partitions of its recorder table"

pass "parts, unknown part, slice of ${slice_events} rows, autodiscover, leases (history.archive ${lease_status}) and ${partitions} Postgres partitions answer as expected"
