"""Drakkar CLI tools.

Provides command-line utilities installed with the drakkar package.

Usage:
    drakkar-merge-debug /tmp/drakkar-debug-*.db -o merged.db
    drakkar-merge-debug /path/to/folder/ -o merged.db
"""

import argparse
import glob
import os
import sqlite3
import sys


def _find_db_files(paths: list[str]) -> list[str]:
    """Expand paths — supports globs, directories, and individual files."""
    files = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(sorted(glob.glob(os.path.join(p, '*.db'))))
        elif '*' in p or '?' in p:
            files.extend(sorted(glob.glob(p)))
        elif os.path.isfile(p):
            files.append(p)
    return files


def merge_debug() -> None:
    """Merge multiple Drakkar debug SQLite files into one chronological table.

    The output file contains a single ``events`` table with all events from
    all input files, sorted by timestamp. A ``source_file`` column is added
    so you can trace which DB each event came from.

    Query examples::

        sqlite3 merged.db "SELECT event, COUNT(*) c FROM events GROUP BY event ORDER BY c DESC"
        sqlite3 merged.db "SELECT * FROM events WHERE event='task_failed' ORDER BY ts"
        sqlite3 merged.db "SELECT source_file, COUNT(*) FROM events GROUP BY source_file"
    """
    parser = argparse.ArgumentParser(
        prog='drakkar-merge-debug',
        description='Merge Drakkar debug SQLite files into one chronological database',
    )
    parser.add_argument('paths', nargs='+', help='DB files, glob patterns, or directories')
    parser.add_argument('-o', '--output', default='merged-debug.db', help='Output file (default: merged-debug.db)')
    args = parser.parse_args()

    files = _find_db_files(args.paths)
    if not files:
        print('No .db files found', file=sys.stderr)
        sys.exit(1)

    output_path = args.output
    if os.path.exists(output_path):
        os.remove(output_path)

    out = sqlite3.connect(output_path)
    out.execute("""
        CREATE TABLE events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           REAL NOT NULL,
            event        TEXT NOT NULL,
            partition    INTEGER,
            offset       INTEGER,
            task_id      TEXT,
            args         TEXT,
            stdout_size  INTEGER,
            stdout       TEXT,
            stderr       TEXT,
            exit_code    INTEGER,
            duration     REAL,
            output_topic TEXT,
            metadata     TEXT,
            pid          INTEGER,
            source_file  TEXT
        )
    """)
    out.execute('CREATE INDEX idx_merged_ts ON events(ts)')
    out.execute('CREATE INDEX idx_merged_event ON events(event)')
    out.execute('CREATE INDEX idx_merged_task_id ON events(task_id)')
    out.execute('CREATE INDEX idx_merged_partition ON events(partition)')

    total = 0
    print(f'Merging {len(files)} files:')

    for db_path in files:
        basename = os.path.basename(db_path)
        try:
            src = sqlite3.connect(db_path)
            src.row_factory = sqlite3.Row
            rows = src.execute('SELECT * FROM events ORDER BY ts').fetchall()
            src.close()
        except Exception as e:
            print(f'  skip {basename}: {e}', file=sys.stderr)
            continue

        for row in rows:
            out.execute(
                """INSERT INTO events
                   (ts, event, partition, offset, task_id, args, stdout_size,
                    stdout, stderr, exit_code, duration, output_topic, metadata, pid, source_file)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row['ts'],
                    row['event'],
                    row['partition'],
                    row['offset'],
                    row['task_id'],
                    row['args'],
                    row['stdout_size'],
                    row['stdout'],
                    row['stderr'],
                    row['exit_code'],
                    row['duration'],
                    row['output_topic'],
                    row['metadata'],
                    row['pid'],
                    basename,
                ),
            )
        total += len(rows)
        print(f'  {basename}: {len(rows)} events')

    out.commit()
    out.close()
    print(f'\nMerged {total} events from {len(files)} files into {output_path}')
