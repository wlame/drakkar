#!/usr/bin/env bash
# ============================================================================
# Wait until the main cluster is ready for the scenarios: worker-1 (UI port
# 8081) lists worker-2 and worker-3 online, and worker-1, worker-2 and
# worker-3 answer /readyz with 200. A worker publishes its card before its
# sources start, so the card alone is not enough: a scenario that cuts a
# worker off before its Kafka source has started makes that worker exit.
#
# Exits 0 as soon as both checks pass, and 1 after READY_LIMIT_SECONDS with
# the last list worker-1 gave and the ports that were not ready.
#
# Usage: HARNESS_PROVIDER=consul integration/wait-ready.sh
# ============================================================================

# Some functions below run only through wait_until.
# shellcheck disable=SC2329
set -euo pipefail

SCENARIO=wait-ready
# shellcheck source=integration/scenario-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/scenario-lib.sh"

WORKER_1_UI_PORT=8081
READY_LIMIT_SECONDS=300
# The UI ports of worker-1, worker-2 and worker-3.
WORKER_UI_PORTS=(8081 8082 8083)

# The UI ports among WORKER_UI_PORTS whose /readyz does not answer 200.
unready_ports() {
    local port
    for port in "${WORKER_UI_PORTS[@]}"; do
        # curl's long options take no `=`, so they keep the space form here.
        curl -sf -o /dev/null --connect-timeout 2 --max-time 3 "http://localhost:${port}/readyz" || echo "$port"
    done
}

cluster_ready() {
    peer_online_is "$WORKER_1_UI_PORT" worker-2 true && peer_online_is "$WORKER_1_UI_PORT" worker-3 true \
        && [ -z "$(unready_ports)" ]
}

# The workers worker-1 lists with their online flag, for the failure text.
last_list() {
    workers_json "$WORKER_1_UI_PORT" \
        | python3 -c 'import json, sys; print(", ".join(f"{r.get(\"worker_name\")} online={r.get(\"online\")}" for r in json.load(sys.stdin)) or "empty list")' \
        2>/dev/null || echo "no answer from worker-1 on port ${WORKER_1_UI_PORT}"
}

log "waiting up to ${READY_LIMIT_SECONDS}s for worker-1 to list worker-2 and worker-3 online and for every worker to be ready"
if wait_until "$READY_LIMIT_SECONDS" cluster_ready; then
    ok "worker-2 and worker-3 online and worker-1 to worker-3 ready after ${WAITED}s"
    exit 0
fi
echo "not ready after ${READY_LIMIT_SECONDS}s; last list of worker-1: $(last_list); /readyz not 200 on ports: $(unready_ports | tr '\n' ' ')" >&2
exit 1
