#!/usr/bin/env bash
# ============================================================================
# Scenario: the DLQ record of a source message that cannot be parsed.
#
# The main cluster runs with DK_SOURCES__KAFKA__ON_PARSE_ERROR: "dlq"
# (docker-compose.yml), and the harness input model reads the message value
# as JSON. The scenario produces one message whose value is not JSON into the
# source topic, with the console producer of the kafka service: a key and a
# value unique to this run, and three headers, the last of them a stale
# `dk-stage` header such as a replayed record carries. Then it checks, in
# order:
#   1. worker-1 /api/v1/debug/kafka/topics lists the `source` and `dlq`
#      aliases,
#   2. within RECORD_LIMIT_SECONDS a record with the produced key is on the
#      DLQ topic (worker-1 /api/v1/debug/kafka/dlq/messages, a time window
#      from shortly before the produce; the message may wait behind the
#      backlog of its partition),
#   3. the record: the key and the value are the produced bytes; the headers
#      are exactly the two source headers that do not start with `dk-`, in
#      their order, then the `dk-` headers of the `parse` case of
#      dlq_contract.json, in its order (no `dk-attempts`, `dk-sink`,
#      `dk-task-id`, `dk-delivered-sinks` or `dk-request-id`);
#      `dk-record-version` is 1, `dk-stage` is parse, `dk-error` is not
#      empty, `dk-source-topic` is the source topic, `dk-worker` is a worker
#      of the main cluster, `dk-instance` is 16 hex characters and
#      `dk-failed-at` is an RFC 3339 time in UTC with microseconds,
#   4. the source message at `dk-source-partition` and `dk-source-offset`
#      (worker-1 /api/v1/debug/kafka/source/message) is the produced one:
#      the same key, value and headers, and its timestamp is
#      `dk-source-timestamp`. The console producer prints no offset, so this
#      read is how the scenario learns the offset of the produced message.
# The verdict line carries the coordinates of both records and the worker
# that wrote the DLQ record.
#
# Runs with every provider.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-dlq-record.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=scenario-dlq-record
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

WORKER_1_UI_PORT=8081
SOURCE_TOPIC=search-requests
# How long the main cluster may take to write the record. The message waits
# behind the messages of its partition that are not consumed yet.
RECORD_LIMIT_SECONDS=600
# Each DLQ read opens a new consumer on worker-1, so poll less often.
POLL_SECONDS=5
# The DLQ read starts this long before the produce, by the clock of the
# kafka container: the broker and the workers share that clock, the host
# may not.
WINDOW_MARGIN_MS=30000

RUN_TOKEN="$(date +%s)-$$"
MESSAGE_KEY="dlq-record-${RUN_TOKEN}"
# Not JSON, so the harness input model fails to parse it.
MESSAGE_VALUE="not-json dlq-record check ${RUN_TOKEN}"
# The headers of the produced message, in the console producer's form
# (name:value, joined by commas). The record keeps the first two and drops
# the stale dk-stage header.
MESSAGE_HEADERS="traceparent:00-4bf92f3577b34da6-01,harness-check:dlq-record,dk-stage:replayed"

# The NDJSON line of the DLQ record with the produced key; set by
# dlq_record_found.
RECORD_JSON=""

# GET a path of worker-1's UI; prints the body.
#   worker_1_get <path and query> <max seconds>
worker_1_get() {
    # curl's long options take no `=`, so they keep the space form here.
    curl -sf --connect-timeout 2 --max-time "$2" "http://localhost:${WORKER_1_UI_PORT}$1"
}

# True when worker-1 serves both read aliases that the scenario needs.
read_aliases_listed() {
    local answer
    answer=$(worker_1_get /api/v1/debug/kafka/topics 5) || return 1
    python3 -c '
import json, sys
aliases = {topic["alias"] for topic in json.load(sys.stdin)["topics"]}
sys.exit(0 if {"source", "dlq"} <= aliases else 1)
' <<<"$answer"
}

# The clock of the kafka container, in epoch milliseconds.
kafka_clock_ms() {
    local seconds
    seconds=$("${COMPOSE[@]}" exec -T kafka date +%s)
    echo $((seconds * 1000))
}

# Produce the message: one line of headers, key and value, split by tabs.
produce_message() {
    printf '%s\t%s\t%s\n' "$MESSAGE_HEADERS" "$MESSAGE_KEY" "$MESSAGE_VALUE" \
        | "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server=localhost:9092 \
            --topic="$SOURCE_TOPIC" \
            --property=parse.key=true \
            --property=parse.headers=true \
            --producer-property=acks=all
}

# True when the DLQ topic holds a record with the produced key since the
# epoch milliseconds $1; sets RECORD_JSON to its line.
#   dlq_record_found <from_ts>
dlq_record_found() {
    local stream line
    stream=$(worker_1_get "/api/v1/debug/kafka/dlq/messages?from_ts=$1" 60) || return 1
    line=$(MESSAGE_KEY="$MESSAGE_KEY" python3 -c '
import base64, json, os, sys
for text in sys.stdin:
    text = text.strip()
    if not text:
        continue
    message = json.loads(text)
    if "error" in message:
        print("the DLQ read ended with an error:", message["error"], file=sys.stderr)
        sys.exit(1)
    key = message.get("key")
    if key is not None and message.get("key_encoding") == "base64":
        key = base64.b64decode(key).decode("utf-8", "replace")
    if key == os.environ["MESSAGE_KEY"]:
        print(text)
        break
' <<<"$stream") || return 1
    [ -n "$line" ] || return 1
    RECORD_JSON=$line
}

# Check the DLQ record (JSON on stdin). Prints one line:
#   ok <dlq partition> <dlq offset> <source partition> <source offset> <source timestamp> <worker>
# or `fail <reason>`.
check_record() {
    MESSAGE_KEY="$MESSAGE_KEY" MESSAGE_VALUE="$MESSAGE_VALUE" SOURCE_TOPIC="$SOURCE_TOPIC" \
        WORKERS="${MAIN_WORKERS[*]}" python3 -c '
import base64, json, os, re, sys

# The dk- headers of the parse case of dlq_contract.json, in its order.
PARSE_DK_HEADERS = [
    "dk-record-version",
    "dk-stage",
    "dk-error",
    "dk-source-topic",
    "dk-source-partition",
    "dk-source-offset",
    "dk-source-timestamp",
    "dk-worker",
    "dk-instance",
    "dk-failed-at",
]
SOURCE_HEADERS = [("traceparent", "00-4bf92f3577b34da6-01"), ("harness-check", "dlq-record")]
FAILED_AT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")


def text(value, encoding):
    if value is None or encoding != "base64":
        return value
    return base64.b64decode(value).decode("utf-8", "replace")


def fail(reason):
    print(f"fail {reason}")
    sys.exit(0)


record = json.load(sys.stdin)
expected_key = os.environ["MESSAGE_KEY"]
expected_value = os.environ["MESSAGE_VALUE"]
key = text(record.get("key"), record.get("key_encoding"))
if key != expected_key:
    fail(f"the key is {key!r}, expected {expected_key!r}")
value = text(record.get("payload"), record.get("payload_encoding"))
if value != expected_value:
    fail(f"the value is {value!r}, expected the produced bytes {expected_value!r}")
headers = [(h["key"], text(h.get("value"), h.get("value_encoding"))) for h in record.get("headers", [])]
names = [name for name, _ in headers]
expected_names = [name for name, _ in SOURCE_HEADERS] + PARSE_DK_HEADERS
if names != expected_names:
    fail(f"the headers are {names}, expected {expected_names}")
if headers[: len(SOURCE_HEADERS)] != SOURCE_HEADERS:
    fail(f"the source headers are {headers[: len(SOURCE_HEADERS)]}, expected {SOURCE_HEADERS}")
dk = dict(headers[len(SOURCE_HEADERS) :])
checks = [
    (dk["dk-record-version"] == "1", "dk-record-version is not 1"),
    (dk["dk-stage"] == "parse", "dk-stage is not parse"),
    (bool(dk["dk-error"]), "dk-error is empty"),
    (dk["dk-source-topic"] == os.environ["SOURCE_TOPIC"], "dk-source-topic is not the source topic"),
    (dk["dk-source-partition"].isdigit(), "dk-source-partition is not a decimal number"),
    (dk["dk-source-offset"].isdigit(), "dk-source-offset is not a decimal number"),
    (dk["dk-source-timestamp"].isdigit(), "dk-source-timestamp is not a decimal number"),
    (dk["dk-worker"] in os.environ["WORKERS"].split(), "dk-worker is not a worker of the main cluster"),
    (re.fullmatch(r"[0-9a-f]{16}", dk["dk-instance"] or "") is not None, "dk-instance is not 16 hex characters"),
    (FAILED_AT.match(dk["dk-failed-at"] or "") is not None, "dk-failed-at is not an RFC 3339 time in UTC with microseconds"),
]
for passed, reason in checks:
    if not passed:
        fail(f"{reason}: {dk}")
print(
    "ok",
    record["partition"],
    record["offset"],
    dk["dk-source-partition"],
    dk["dk-source-offset"],
    dk["dk-source-timestamp"],
    dk["dk-worker"],
)
'
}

# Check the source message (JSON on stdin) against the produced message and
# the source timestamp $1. Prints `ok` or `fail <reason>`.
#   check_source_message <source timestamp>
check_source_message() {
    MESSAGE_KEY="$MESSAGE_KEY" MESSAGE_VALUE="$MESSAGE_VALUE" MESSAGE_HEADERS="$MESSAGE_HEADERS" \
        SOURCE_TIMESTAMP="$1" python3 -c '
import base64, json, os, sys


def text(value, encoding):
    if value is None or encoding != "base64":
        return value
    return base64.b64decode(value).decode("utf-8", "replace")


message = json.load(sys.stdin)
expected_key = os.environ["MESSAGE_KEY"]
expected_value = os.environ["MESSAGE_VALUE"]
expected_timestamp = os.environ["SOURCE_TIMESTAMP"]
timestamp = message.get("timestamp_ms")
produced_headers = [tuple(pair.split(":", 1)) for pair in os.environ["MESSAGE_HEADERS"].split(",")]
headers = [(h["key"], text(h.get("value"), h.get("value_encoding"))) for h in message.get("headers", [])]
key = text(message.get("key"), message.get("key_encoding"))
value = text(message.get("payload"), message.get("payload_encoding"))
if key != expected_key:
    print(f"fail its key is {key!r}, expected {expected_key!r}")
elif value != expected_value:
    print(f"fail its value is {value!r}, expected {expected_value!r}")
elif headers != produced_headers:
    print(f"fail its headers are {headers}, expected {produced_headers}")
elif str(timestamp) != expected_timestamp:
    print(f"fail its timestamp is {timestamp}, dk-source-timestamp is {expected_timestamp}")
else:
    print("ok")
'
}

step "Read aliases of worker-1"
read_aliases_listed \
    || fail "worker-1 /api/v1/debug/kafka/topics does not list the source and dlq aliases ($(worker_1_get /api/v1/debug/kafka/topics 5 || echo unreachable))"
ok "worker-1 serves the source and dlq read aliases"

step "Produce one message that cannot be parsed"
from_ts=$(($(kafka_clock_ms) - WINDOW_MARGIN_MS))
produce_output=$(produce_message 2>&1) \
    || fail "the console producer failed: $(tr '\n' ' ' <<<"$produce_output")"
ok "produced key ${MESSAGE_KEY} with value '${MESSAGE_VALUE}' into ${SOURCE_TOPIC}"

step "Wait for the DLQ record"
wait_until "$RECORD_LIMIT_SECONDS" dlq_record_found "$from_ts" \
    || fail "no record with key ${MESSAGE_KEY} on the DLQ topic within ${RECORD_LIMIT_SECONDS} s (read from ${from_ts} ms)"
ok "the record is on the DLQ topic after ${WAITED} s"

step "Check the record"
record_verdict=$(check_record <<<"$RECORD_JSON")
case "$record_verdict" in
    "ok "*) ;;
    *) fail "the DLQ record: ${record_verdict#fail }" ;;
esac
read -r _ dlq_partition dlq_offset source_partition source_offset source_timestamp record_worker <<<"$record_verdict"
ok "the record keeps the key, the value and the source headers, and its dk- headers are those of the parse case, in order"

step "Check the source message at the record's offset"
source_message=$(worker_1_get "/api/v1/debug/kafka/source/message?partition=${source_partition}&offset=${source_offset}" 30) \
    || fail "worker-1 could not read the source message at ${source_partition}:${source_offset}"
source_verdict=$(check_source_message "$source_timestamp" <<<"$source_message")
[ "$source_verdict" = ok ] \
    || fail "the source message at ${source_partition}:${source_offset}: ${source_verdict#fail }"
ok "the source message at ${source_partition}:${source_offset} is the produced one"

pass "the record of key ${MESSAGE_KEY} (source ${source_partition}:${source_offset}) is at dlq ${dlq_partition}:${dlq_offset}, written by ${record_worker} after ${WAITED} s, with stage parse, version 1 and the headers in order"
