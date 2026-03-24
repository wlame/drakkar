# Shared Directory

This directory is mounted into all Drakkar integration test workers as `/shared`.

## Purpose

Stores SQLite debug databases from each worker. The recorder generates
timestamped filenames from the worker name:

```
{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db
```

A `{worker_name}-live.db` symlink points to the current database while the
worker is running. On graceful shutdown, the symlink is removed and only the
original file remains.

Because the directory is a host mount, databases **persist across restarts**.

## Rotation

The recorder handles rotation automatically:

- Old files beyond `retention_hours` (default 24h) are deleted
- File count is capped based on `retention_max_events`
- A new timestamped file is created on each startup and periodic rotation

## Example contents

```
shared/
├── README.md
├── worker-1-2026-03-23__14_55_00.db
├── worker-1-2026-03-23__16_10_30.db
├── worker-1-live.db -> worker-1-2026-03-23__16_10_30.db
├── worker-2-2026-03-23__14_55_01.db
├── worker-2-live.db -> worker-2-2026-03-23__14_55_01.db
└── ...
```

After shutdown (live symlinks removed):

```
shared/
├── README.md
├── worker-1-2026-03-23__14_55_00.db
├── worker-1-2026-03-23__16_10_30.db
├── worker-2-2026-03-23__14_55_01.db
└── ...
```

## Querying

```bash
# While running — use the live symlink
sqlite3 integration/shared/worker-1-live.db "SELECT count(*) FROM events"

# After shutdown — find the latest file
ls -t integration/shared/worker-1-*.db | head -1

# Query a specific run
sqlite3 integration/shared/worker-1-2026-03-23__14_55_00.db ".tables"
```

## Cleanup

```bash
# Remove all databases
rm -f integration/shared/*.db

# Remove only one worker's history
rm -f integration/shared/worker-1-*.db
```
