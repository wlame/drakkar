#!/usr/bin/env bash
# ============================================================================
# Wait until the main cluster is ready for the scenarios: worker-1 (UI port
# 8081) lists worker-2 and worker-3 online.
#
# Exits 0 as soon as both are online, and 1 after READY_LIMIT_SECONDS with
# the last list worker-1 gave.
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

peers_online() {
    peer_online_is "$WORKER_1_UI_PORT" worker-2 true && peer_online_is "$WORKER_1_UI_PORT" worker-3 true
}

# The workers worker-1 lists with their online flag, for the failure text.
last_list() {
    workers_json "$WORKER_1_UI_PORT" \
        | python3 -c 'import json, sys; print(", ".join(f"{r.get(\"worker_name\")} online={r.get(\"online\")}" for r in json.load(sys.stdin)) or "empty list")' \
        2>/dev/null || echo "no answer from worker-1 on port ${WORKER_1_UI_PORT}"
}

log "waiting up to ${READY_LIMIT_SECONDS}s for worker-1 to list worker-2 and worker-3 online"
if wait_until "$READY_LIMIT_SECONDS" peers_online; then
    ok "worker-2 and worker-3 online after ${WAITED}s"
    exit 0
fi
echo "not ready after ${READY_LIMIT_SECONDS}s; last list of worker-1: $(last_list)" >&2
exit 1
