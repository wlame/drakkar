#!/bin/sh
# Count total characters (symbols) in a file or directory.
# Reads JSON from stdin with a "file_path" key.
# Outputs JSON: {"file_path": "...", "symbol_count": N}

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['file_path'])")

# The one path that fails on purpose: scenario-recorder-postgres.sh sends it
# to see that a failed task keeps its stderr, cut to its two halves. The
# pause keeps the task above min_output_duration, or its output is not stored.
if [ "$FILE_PATH" = "/harness/long-stderr-failure" ]; then
    sleep 0.3
    {
        printf 'STDERR-HEAD '
        head -c 8000 /dev/zero | tr '\0' 'x'
        printf ' STDERR-TAIL\n'
    } >&2
    exit 3
fi

if [ -d "$FILE_PATH" ]; then
    COUNT=$(find "$FILE_PATH" -type f -exec cat {} + 2>/dev/null | wc -c)
elif [ -f "$FILE_PATH" ]; then
    COUNT=$(wc -c < "$FILE_PATH")
else
    COUNT=0
fi

# trim whitespace from wc output
COUNT=$(echo "$COUNT" | tr -d '[:space:]')

echo "{\"file_path\": \"$FILE_PATH\", \"symbol_count\": $COUNT}"
