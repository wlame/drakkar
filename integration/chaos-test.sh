#!/usr/bin/env bash
# ============================================================================
# Chaos Test: Rolling Outage
#
# Simulates cascading worker failures and recoveries during message processing.
# Validates that Drakkar's cooperative-sticky rebalancing handles partition
# reassignment correctly and no messages are lost.
#
# Timeline:
#   t1 — ~3000/10000 messages processed (≈1/3 progress)
#   t2 = t1 + 30s  — worker-2 stops
#   t3 = t2 + 120s — worker-3 stops (only worker-1 alive)
#   t4 = t3 + 240s — worker-2 starts again
#   t5 = t4        — worker-1 stops (immediately after worker-2 returns)
#   t6 = t5 + 60s  — worker-1 starts again
#   t7 = t6 + 60s  — worker-3 starts again (all workers back)
#
# Fast-worker cluster is NOT touched.
#
# Usage:
#   cd integration
#   docker-compose up --build -d
#   ./chaos-test.sh
# ============================================================================

set -euo pipefail

COMPOSE="docker-compose"
PROGRESS_TARGET=3000
POLL_INTERVAL=5

# ANSI colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

ts() { date '+%H:%M:%S'; }

log()  { echo -e "${CYAN}[$(ts)]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(ts)] ⚠${NC}  $*"; }
ok()   { echo -e "${GREEN}[$(ts)] ✓${NC}  $*"; }
fail() { echo -e "${RED}[$(ts)] ✗${NC}  $*"; }
step() { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━${NC}\n"; }

# Get total completed tasks across all running workers (ripgrep cluster only)
get_completed() {
    local total=0
    for port in 8081 8082 8083; do
        local val
        val=$(curl -s --connect-timeout 2 --max-time 3 "http://localhost:${port}/api/dashboard" 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['stats']['completed'])" 2>/dev/null) || continue
        total=$((total + val))
    done
    echo "$total"
}

# Get completed tasks for a specific worker
get_worker_completed() {
    local port=$1
    curl -s --connect-timeout 2 --max-time 3 "http://localhost:${port}/api/dashboard" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['stats']['completed'])" 2>/dev/null || echo "0"
}

# Get partition count for a specific worker
get_worker_partitions() {
    local port=$1
    curl -s --connect-timeout 2 --max-time 3 "http://localhost:${port}/api/dashboard" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['partition_count'])" 2>/dev/null || echo "?"
}

# Print worker status line
worker_status() {
    local w1_c w2_c w3_c w1_p w2_p w3_p
    w1_c=$(get_worker_completed 8081); w1_p=$(get_worker_partitions 8081)
    w2_c=$(get_worker_completed 8082); w2_p=$(get_worker_partitions 8082)
    w3_c=$(get_worker_completed 8083); w3_p=$(get_worker_partitions 8083)
    local total=$((w1_c + w2_c + w3_c))
    log "workers: w1=${w1_c}(${w1_p}p) w2=${w2_c}(${w2_p}p) w3=${w3_c}(${w3_p}p) │ total=${total}"
}

# Wait until total completed >= target
wait_for_progress() {
    local target=$1
    local label=$2
    log "waiting for ${target} completed tasks (${label})..."
    while true; do
        local completed
        completed=$(get_completed)
        if [ "$completed" -ge "$target" ] 2>/dev/null; then
            ok "reached ${completed} completed (target: ${target})"
            return
        fi
        worker_status
        sleep "$POLL_INTERVAL"
    done
}

# Wait N seconds with periodic status
wait_with_status() {
    local seconds=$1
    local label=$2
    log "waiting ${seconds}s (${label})..."
    local elapsed=0
    while [ "$elapsed" -lt "$seconds" ]; do
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
        worker_status
    done
}

stop_worker() {
    local name=$1
    warn "STOPPING ${name}"
    $COMPOSE stop "$name" 2>/dev/null
    ok "${name} stopped"
}

start_worker() {
    local name=$1
    ok "STARTING ${name}"
    $COMPOSE start "$name" 2>/dev/null
    ok "${name} started"
}

# ============================================================================
# Main scenario
# ============================================================================

step "Rolling Outage — Chaos Test"
log "scenario: cascading failures across 3 workers"
log "target: ${PROGRESS_TARGET} tasks before first outage"
echo ""

# Wait for all workers to become reachable (up to 120s)
log "waiting for workers to become reachable..."
for port in 8081 8082 8083; do
    retries=0
    while ! curl -s --connect-timeout 2 --max-time 3 "http://localhost:${port}/api/dashboard" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "$retries" -ge 24 ]; then
            fail "worker on :${port} not reachable after 120s — is docker-compose up?"
            exit 1
        fi
        sleep 5
    done
    ok "worker on :${port} is up"
done

# ── t1: wait for ~1/3 progress ──
step "T1: Waiting for ${PROGRESS_TARGET}/10000 tasks completed"
wait_for_progress "$PROGRESS_TARGET" "1/3 progress"
worker_status

# ── t2: worker-2 stops (t1 + 30s) ──
step "T2: Worker-2 outage in 30s"
wait_with_status 30 "pre-outage buffer"
stop_worker "worker-2"
worker_status

# ── t3: worker-3 stops (t2 + 120s) ──
step "T3: Worker-3 outage in 120s (only worker-1 will survive)"
wait_with_status 120 "worker-2 is down, worker-1+3 processing"
stop_worker "worker-3"
log "only worker-1 is alive now"
worker_status

# ── t4: worker-2 returns (t3 + 240s) ──
step "T4: Worker-2 recovery in 240s"
wait_with_status 240 "only worker-1 processing all partitions"
start_worker "worker-2"

# ── t5: worker-1 stops immediately ──
step "T5: Worker-1 outage (simultaneous with worker-2 recovery)"
stop_worker "worker-1"
log "worker-2 is now the only worker"
worker_status

# ── t6: worker-1 returns (t5 + 60s) ──
step "T6: Worker-1 recovery in 60s"
wait_with_status 60 "only worker-2 processing"
start_worker "worker-1"
worker_status

# ── t7: worker-3 returns (t6 + 60s) ──
step "T7: Worker-3 recovery in 60s (full cluster restored)"
wait_with_status 60 "worker-1+2 processing"
start_worker "worker-3"
ok "all 3 workers are back online"
worker_status

# ── Final: wait for completion ──
step "Final: Waiting for processing to stabilize"
wait_with_status 60 "post-recovery stabilization"

step "Results"
worker_status
echo ""
log "scenario complete — check debug UIs for rebalance events:"
log "  worker-1: http://localhost:8081/history"
log "  worker-2: http://localhost:8082/history"
log "  worker-3: http://localhost:8083/history"
