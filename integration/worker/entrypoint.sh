#!/bin/bash
set -euo pipefail

# Set debug DB base path with worker name — the recorder handles
# timestamping (appends -YYYY-MM-DD__HH_MM_SS) and rotation automatically.
export DRAKKAR_DEBUG__DB_PATH="/shared/drakkar-debug-${WORKER_ID:-worker}.db"

echo "debug db base path: ${DRAKKAR_DEBUG__DB_PATH}"

exec "$@"
