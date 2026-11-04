"""Reading a SQLite table in bounded batches.

A copier that reads a whole table with one ``fetchall()`` holds every row of
it in memory at once: a sealed recorder file of a busy hour holds hundreds of
MiB of task output. These helpers read in keyset batches by ``id`` instead.
A batch ends at a row count or at a number of text bytes, whichever comes
first, and each batch is one statement that is reset before the next one, so
no read stays open between batches.

The slice download and the merge both copy with these helpers.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

# Rows per read batch, at most.
BATCH_ROWS = 5000
# Text bytes per read batch, at most (plus the row that reaches the bound).
BATCH_BYTES = 8 * 1024 * 1024
# Below every SQLite rowid: the keyset start of a table read.
NO_ID = -(2**63)
# The memory cost counted for each value besides its text or blob length.
_VALUE_OVERHEAD_BYTES = 8


def row_bytes(values: Iterable[Any]) -> int:
    """The bytes a row holds in memory, roughly: its text and blob lengths plus a small cost per value."""
    total = 0
    for value in values:
        total += _VALUE_OVERHEAD_BYTES
        if isinstance(value, str | bytes):
            total += len(value)
    return total


def fetch_rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any], max_bytes: int) -> tuple[list[tuple], bool]:
    """Read rows of one statement until it ends or ``max_bytes`` is reached; the rows and whether the bytes cut them.

    The statement is reset after the last row read, so no read is held open
    between batches. A SQLite error is raised as it is.
    """
    rows: list[tuple] = []
    size = 0
    cursor = conn.execute(sql, params)
    try:
        for row in cursor:
            rows.append(row)
            size += row_bytes(row)
            if size >= max_bytes:
                return rows, True
    finally:
        cursor.close()
    return rows, False


def keyset_batches(
    conn: sqlite3.Connection,
    sql: str,
    *,
    batch_rows: int = BATCH_ROWS,
    max_bytes: int = BATCH_BYTES,
) -> Iterator[list[tuple]]:
    """Yield the rows of ``sql`` in keyset batches by a whole-number key (``id`` or ``rowid``).

    ``sql`` selects the key as its first column and takes two parameters: the
    last key read (``WHERE id > ?``) and the batch size (``ORDER BY id LIMIT
    ?``). A batch ends at ``batch_rows`` rows or ``max_bytes`` text bytes.
    """
    after = NO_ID
    while True:
        rows, cut_by_bytes = fetch_rows(conn, sql, (after, batch_rows), max_bytes)
        if rows:
            yield rows
        if len(rows) < batch_rows and not cut_by_bytes:
            return
        after = rows[-1][0]
