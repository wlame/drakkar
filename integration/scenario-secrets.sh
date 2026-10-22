#!/usr/bin/env bash
# ============================================================================
# Scenario: the provider credentials never leave the worker.
#
# docker-compose.yml gives the main-cluster workers the Postgres DSN with
# its password (DK_CONNECTIONS__POSTGRES__MAIN__DSN), the Redis URL with
# its password (DK_CONNECTIONS__REDIS__MAIN__URL) and the Consul ACL token
# (DK_CONNECTIONS__CONSUL__MAIN__TOKEN). This proves that none of the three
# reaches any of the places an operator reads:
#   - the config reference of each worker (GET /api/v1/config-reference),
#     which must list connections.consul.main.token with its value masked,
#   - the logs of worker-1, worker-2 and worker-3,
#   - the metrics page of each worker.
# A hit prints the matching lines and where they came from. The Consul token
# is read from docker-compose.yml, so the harness value lives in one file.
#
# Runs with every provider: all three connections are configured whichever
# provider the main cluster uses.
#
# Usage: HARNESS_PROVIDER=consul integration/scenario-secrets.sh
# ============================================================================

set -euo pipefail

SCENARIO=scenario-secrets
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

# The Consul tokens that docker-compose.yml gives the main-cluster workers,
# one per line and each once: consul_worker_tokens <service>...
consul_worker_tokens() {
    "${COMPOSE[@]}" config --format=json | python3 -c '
import json, sys
services = json.load(sys.stdin)["services"]
tokens = {services[name]["environment"]["DK_CONNECTIONS__CONSUL__MAIN__TOKEN"] for name in sys.argv[1:]}
print("\n".join(sorted(tokens)))
' "$@"
}

CONSUL_TOKENS=$(consul_worker_tokens "${MAIN_WORKERS[@]}") \
    || fail "the Consul token of the main-cluster workers could not be read from docker-compose.yml"
[ -n "$CONSUL_TOKENS" ] || fail "docker-compose.yml gives the main-cluster workers no DK_CONNECTIONS__CONSUL__MAIN__TOKEN"
# Text that appears only where a DSN or a URL with its password is printed
# whole (a redacted one reads ://***:***@), and the Consul token itself, a
# value distinct from every other harness value.
SECRET_PATTERNS=('drakkar:drakkar@' 'postgresql://drakkar:' 'redis://drakkar:')
while IFS= read -r consul_token; do
    SECRET_PATTERNS+=("$consul_token")
done <<<"$CONSUL_TOKENS"
UI_PORTS=(8081 8082 8083)
METRICS_PORTS=(9090 9091 9093)
# The config reference must list every connection, or an answer without them
# would pass the check. The reference lists every configured connection,
# whichever provider uses it.
CONNECTION_PATHS=('connections.postgres.main.dsn' 'connections.redis.main.url' 'connections.consul.main.token')
# The config reference shows a secret that is set as this mask; an empty
# secret is shown as it is.
SECRET_MASK='••••••'
TOKEN_PATH='connections.consul.main.token'

# Fail when the text on stdin contains a secret pattern: check_clean <source>
check_clean() {
    local text pattern
    text=$(cat)
    [ -n "$text" ] || fail "${1} is empty"
    for pattern in "${SECRET_PATTERNS[@]}"; do
        if grep -qF -- "$pattern" <<<"$text"; then
            echo "--- lines of ${1} with '${pattern}':"
            grep -F -- "$pattern" <<<"$text" | head -n 5
            fail "${1} contains '${pattern}'"
        fi
    done
    ok "${1}: no secret"
}

# The value and the secret flag of one row of the config reference on stdin:
#   reference_row <path>
# Prints `<value as JSON> secret=<true|false>`, or `absent`.
reference_row() {
    ROW_PATH="$1" python3 -c '
import json, os, sys
for group in json.load(sys.stdin)["groups"]:
    for entry in group["entries"]:
        if entry["path"] == os.environ["ROW_PATH"]:
            print(json.dumps(entry["value"], ensure_ascii=False), "secret=" + json.dumps(entry["secret"]))
            sys.exit(0)
print("absent")
'
}

step "Scenario: secrets stay out of the config API, the logs and the metrics"

for port in "${UI_PORTS[@]}"; do
    # curl's long options take no `=`, so they keep the space form here.
    reference=$(curl -sf --connect-timeout 2 --max-time 10 "http://localhost:${port}/api/v1/config-reference") \
        || fail "the config reference on port ${port} could not be read"
    for connection_path in "${CONNECTION_PATHS[@]}"; do
        grep -qF -- "\"path\":\"${connection_path}\"" <<<"$reference" \
            || fail "the config reference on port ${port} does not list ${connection_path}"
    done
    token_row=$(reference_row "$TOKEN_PATH" <<<"$reference")
    [ "$token_row" = "\"${SECRET_MASK}\" secret=true" ] \
        || fail "the config reference on port ${port} shows ${TOKEN_PATH} as ${token_row}, expected the mask \"${SECRET_MASK}\" with secret=true"
    check_clean "config reference :${port}" <<<"$reference"
done

for service in "${MAIN_WORKERS[@]}"; do
    "${COMPOSE[@]}" logs --no-color "$service" 2>&1 | check_clean "logs of ${service}"
done

for port in "${METRICS_PORTS[@]}"; do
    page=$(curl -sf --connect-timeout 2 --max-time 5 "http://localhost:${port}/metrics") \
        || fail "the metrics page on port ${port} could not be read"
    check_clean "metrics :${port}" <<<"$page"
done

pass "no DSN or URL password and no Consul token in ${#UI_PORTS[@]} config references (${TOKEN_PATH} masked), ${#MAIN_WORKERS[@]} worker logs, ${#METRICS_PORTS[@]} metrics pages"
