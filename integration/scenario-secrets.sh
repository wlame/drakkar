#!/usr/bin/env bash
# ============================================================================
# Scenario: the provider passwords never leave the worker.
#
# docker-compose.yml gives the main-cluster workers the Postgres DSN with
# its password (DK_CONNECTIONS__POSTGRES__MAIN__DSN) and the Redis URL with
# its password (DK_CONNECTIONS__REDIS__MAIN__URL). This proves that neither
# password reaches any of the places an operator reads:
#   - the config reference of each worker (GET /api/v1/config-reference),
#   - the logs of worker-1, worker-2 and worker-3,
#   - the metrics page of each worker.
# A hit prints the matching lines and where they came from.
#
# Runs with every provider: both connections are configured whichever
# provider the main cluster uses.
#
# Usage: HARNESS_PROVIDER=redis integration/scenario-secrets.sh
# ============================================================================

set -euo pipefail

SCENARIO=scenario-secrets
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

# Text that appears only where a DSN or a URL with its password is printed
# whole; a redacted one reads ://***:***@.
SECRET_PATTERNS=('drakkar:drakkar@' 'postgresql://drakkar:' 'redis://drakkar:')
UI_PORTS=(8081 8082 8083)
METRICS_PORTS=(9090 9091 9093)
# The config reference must list both connections, or an answer without them
# would pass the check. The reference lists every configured connection,
# whichever provider uses it.
CONNECTION_PATHS=('connections.postgres.main.dsn' 'connections.redis.main.url')

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

step "Scenario: secrets stay out of the config API, the logs and the metrics"

for port in "${UI_PORTS[@]}"; do
    # curl's long options take no `=`, so they keep the space form here.
    reference=$(curl -sf --connect-timeout 2 --max-time 10 "http://localhost:${port}/api/v1/config-reference") \
        || fail "the config reference on port ${port} could not be read"
    for connection_path in "${CONNECTION_PATHS[@]}"; do
        grep -qF -- "\"path\":\"${connection_path}\"" <<<"$reference" \
            || fail "the config reference on port ${port} does not list ${connection_path}"
    done
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

pass "no DSN or URL password in ${#UI_PORTS[@]} config references, ${#MAIN_WORKERS[@]} worker logs, ${#METRICS_PORTS[@]} metrics pages"
