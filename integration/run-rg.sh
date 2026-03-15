#!/bin/sh
# Runs rg $1 times with pattern $2 against path $3.
# Each iteration searches recursively and sleeps 0.2s to simulate
# realistic CPU-bound work. A repeat=5 task takes ~1s, repeat=15
# takes ~3s, repeat=200 takes ~40s.
REPEAT=$1
PATTERN=$2
TARGET=$3

i=1
while [ "$i" -lt "$REPEAT" ]; do
    # search recursively (more work per iteration)
    rg --no-filename --no-line-number -r '' "$PATTERN" "$TARGET" > /dev/null 2>&1 || true
    # sleep 200ms per iteration to simulate real compute
    sleep 0.2
    i=$((i + 1))
done

# last run — output goes to stdout
rg --no-filename --no-line-number "$PATTERN" "$TARGET" 2>/dev/null || true
