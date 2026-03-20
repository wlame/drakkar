#!/bin/sh
# Runs rg $1 times with pattern $2 against path $3.
# Optional: --fail=0.2 as $4 to simulate random failures (20% chance).
#
# Each iteration searches recursively and sleeps 0.2s to simulate
# realistic CPU-bound work. A repeat=5 task takes ~1s.
REPEAT=$1
PATTERN=$2
TARGET=$3
FAIL_RATE="${4:-0}"

# Strip --fail= prefix if present
FAIL_RATE=$(echo "$FAIL_RATE" | sed 's/^--fail=//')

# Check for random failure
if [ "$FAIL_RATE" != "0" ]; then
    # Generate random number 0-999, compare with fail rate * 1000
    THRESHOLD=$(echo "$FAIL_RATE * 1000" | bc 2>/dev/null || echo "0")
    ROLL=$(( $(od -An -tu2 -N2 /dev/urandom | tr -d ' ') % 1000 ))
    if [ "$ROLL" -lt "${THRESHOLD%.*}" ] 2>/dev/null; then
        echo "SIMULATED FAILURE: random roll $ROLL < threshold $THRESHOLD" >&2
        exit 1
    fi
fi

i=1
while [ "$i" -lt "$REPEAT" ]; do
    rg --no-filename --no-line-number -r '' "$PATTERN" "$TARGET" > /dev/null 2>&1 || true
    sleep 0.2
    i=$((i + 1))
done

# last run — output goes to stdout
rg --no-filename --no-line-number "$PATTERN" "$TARGET" 2>/dev/null || true
