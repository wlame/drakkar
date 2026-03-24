# Shared Directory

This directory is mounted into all Drakkar integration test workers as `/shared`.

## Purpose

Stores SQLite debug databases from each worker. The entrypoint sets a per-worker
base path (e.g. `drakkar-debug-worker-1.db`), and the recorder automatically
creates timestamped files on each startup and rotation:

```
drakkar-debug-{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db
```

Because the directory is a host mount, databases **persist across restarts** of
the integration test environment.

## Rotation

The recorder handles rotation automatically:

- Old files beyond `retention_hours` (default 24h) are deleted
- File count is capped based on `retention_max_events`
- A new timestamped file is created on each startup and periodic rotation

## Example contents

```
shared/
├── README.md
├── drakkar-debug-worker-1-2026-03-23__14_55_00.db
├── drakkar-debug-worker-1-2026-03-23__16_10_30.db
├── drakkar-debug-worker-2-2026-03-23__14_55_01.db
├── drakkar-debug-worker-3-2026-03-23__14_55_01.db
└── ...
```

## Querying

```bash
# Find the latest file for a worker
ls -t integration/shared/drakkar-debug-worker-1-*.db | head -1

# Query it
sqlite3 integration/shared/drakkar-debug-worker-1-2026-03-23__14_55_00.db "SELECT count(*) FROM events"
```

## Cleanup

```bash
# Remove all databases
rm -f integration/shared/*.db

# Remove only one worker's history
rm -f integration/shared/drakkar-debug-worker-1-*.db
```
