#!/bin/sh
# Runs rg $1 times with pattern $2 against path $3.
# Optional: --fail=0.05 as $4 to simulate random failures (5% chance).
# Failure happens AFTER doing real work (mid-execution), not at start.
#
# Each iteration searches recursively and sleeps 0.2s to simulate
# realistic CPU-bound work. A repeat=5 task takes ~1s.
REPEAT=$1
PATTERN=$2
TARGET=$3
FAIL_RATE="${4:-0}"

# Strip --fail= prefix if present
FAIL_RATE=$(echo "$FAIL_RATE" | sed 's/^--fail=//')

# Decide fail point upfront: pick a random iteration to fail at (if failing)
SHOULD_FAIL=0
FAIL_AT=0
if [ "$FAIL_RATE" != "0" ]; then
    THRESHOLD=$(echo "$FAIL_RATE * 1000" | bc 2>/dev/null || echo "0")
    ROLL=$(( $(od -An -tu2 -N2 /dev/urandom | tr -d ' ') % 1000 ))
    if [ "$ROLL" -lt "${THRESHOLD%.*}" ] 2>/dev/null; then
        SHOULD_FAIL=1
        # fail somewhere between 50% and 90% through the work
        if [ "$REPEAT" -gt 2 ]; then
            HALF=$(( REPEAT / 2 ))
            RANGE=$(( REPEAT - HALF ))
            FAIL_AT=$(( HALF + $(od -An -tu2 -N2 /dev/urandom | tr -d ' ') % RANGE ))
        else
            FAIL_AT=1
        fi
    fi
fi

i=1
while [ "$i" -lt "$REPEAT" ]; do
    # check if this is the fail point
    if [ "$SHOULD_FAIL" = "1" ] && [ "$i" -ge "$FAIL_AT" ]; then
        echo "SIMULATED FAILURE at iteration $i/$REPEAT (fail point $FAIL_AT)" >&2
        exit 1
    fi

    rg --no-filename --no-line-number -r '' "$PATTERN" "$TARGET" > /dev/null 2>&1 || true
    sleep 0.2
    i=$((i + 1))
done

# last run — output goes to stdout
rg --no-filename --no-line-number "$PATTERN" "$TARGET" 2>/dev/null || true
