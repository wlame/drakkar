#!/bin/sh
# Count total characters (symbols) in a file or directory.
# Reads JSON from stdin with a "file_path" key.
# Outputs JSON: {"file_path": "...", "symbol_count": N}

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['file_path'])")

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
