#!/bin/sh
# One task of the split worker: wait the given seconds, then exit by the
# item's mode (`fail` exits 1, `ok` and `hang` exit 0; a `hang` item gets a
# long wait from the handler).
#   split-task <seconds> <mode>
sleep "$1"
if [ "$2" = fail ]; then
    echo "item failed on purpose" >&2
    exit 1
fi
printf '{"mode": "%s"}\n' "$2"
