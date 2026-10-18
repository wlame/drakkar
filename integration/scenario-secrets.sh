#!/usr/bin/env bash
# ============================================================================
# Scenario: the database password never leaves the worker.
#
# docker-compose.yml gives the main-cluster workers the registry DSN with
# its password (DK_CONNECTIONS__POSTGRES__MAIN__DSN). This proves that the
# password reaches none of the places an operator reads:
#   - the config reference of each worker (GET /api/v1/config-reference),
#   - the logs of worker-1, worker-2 and worker-3,
#   - the metrics page of each worker.
# A hit prints the matching lines and where they came from.
#
# Runs with both providers: the DSN is configured either way.
#
# Usage: HARNESS_PROVIDER=postgres integration/scenario-secrets.sh
# ============================================================================

set -euo pipefail

SCENARIO=scenario-secrets
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

# Text that appears only where a DSN with its password is printed whole.
SECRET_PATTERNS=('drakkar:drakkar@' 'postgresql://drakkar:')
UI_PORTS=(8081 8082 8083)
METRICS_PORTS=(9090 9091 9093)
# The config reference must list the connection, or an empty answer would
# pass the check.
CONNECTION_DSN_PATH='"path":"connections.postgres.main.dsn"'

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
    reference=$(curl -sf --connect-timeout 2 --max-time 10 "http://localhost:${port}/api/v1/config-reference") \
        || fail "the config reference on port ${port} could not be read"
    grep -qF -- "$CONNECTION_DSN_PATH" <<<"$reference" \
        || fail "the config reference on port ${port} does not list connections.postgres.main.dsn"
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

pass "no DSN password in ${#UI_PORTS[@]} config references, ${#MAIN_WORKERS[@]} worker logs, ${#METRICS_PORTS[@]} metrics pages"
