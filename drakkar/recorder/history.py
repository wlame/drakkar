"""Shared history: where sealed recorder files live, and the meta file beside each.

A worker writes its recorder file only while it is live. Sealing (see
``EventRecorder``) closes it with a rollback journal and moves it into
``<shared dir>/history/<cluster>/``, next to a small JSON meta file that
carries the file's statistics, written once by the owner. Readers never
open a sealed file to learn its statistics; they read the meta file.

The meta bytes are a contract shared by every implementation:
``tests/fixtures/history_meta.json`` pins them.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from datetime import datetime
from typing import Any

import aiosqlite

from drakkar.dbfiles import DB_FILE_MODE
from drakkar.timefmt import format_epoch_micro, format_rfc3339_micro

META_SCHEMA = 1
META_SUFFIX = '.meta.json'
HISTORY_FOLDER = 'history'
MERGED_FOLDER = 'merged'
# A meta file is a few hundred bytes; anything far larger is not one.
MAX_META_BYTES = 64 * 1024

# No symlinks in the shared folder: a planted link must not make a worker
# read or overwrite a file elsewhere. O_NONBLOCK keeps a FIFO from hanging.
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW

# Folder and archive-name prefix for an empty or missing cluster name.
DEFAULT_CLUSTER = 'default'
_UNSAFE_CLUSTER_CHARS = re.compile(r'[^a-zA-Z0-9_-]')


def sanitize_cluster(name: str | None) -> str:
    """Return a cluster name safe to embed in a folder or file name.

    Everything outside ``[a-zA-Z0-9_-]`` becomes ``_``; an empty or
    missing name becomes :data:`DEFAULT_CLUSTER`.
    """
    if not name:
        return DEFAULT_CLUSTER
    return _UNSAFE_CLUSTER_CHARS.sub('_', name)


def history_dir(shared_dir: str, cluster: str) -> str:
    """The history folder of ``cluster``: ``<shared>/history/<sanitized cluster>``."""
    return os.path.join(shared_dir, HISTORY_FOLDER, sanitize_cluster(cluster))


def merged_dir(shared_dir: str) -> str:
    """The folder of UI merge results: ``<shared>/merged``."""
    return os.path.join(shared_dir, MERGED_FOLDER)


def list_sealed(folder: str) -> list[str]:
    """The sealed recorder files in a history folder: plain ``*.db`` files, no dot names, no links.

    Sorted by name. Dot-prefixed names are work files (an archive pass's
    merge, a sealing copy still on its way in); a symlink is never
    something sealing put there. A missing folder lists as empty.
    """
    try:
        names = os.listdir(folder)
    except FileNotFoundError:
        return []
    paths: list[str] = []
    for name in names:
        if name.startswith('.') or not name.endswith('.db'):
            continue
        path = os.path.join(folder, name)
        try:
            if not stat.S_ISREG(os.lstat(path).st_mode):
                continue
        except FileNotFoundError:
            continue
        paths.append(path)
    return sorted(paths)


def meta_path(db_path: str) -> str:
    """The meta file of a sealed recorder file: ``<name>.db.meta.json``."""
    return db_path + META_SUFFIX


def serialize_json(obj: dict[str, Any]) -> bytes:
    """Contract bytes of a meta file or lock body: sorted keys, compact, raw UTF-8, no trailing newline."""
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


async def _scalar_rows(conn: aiosqlite.Connection, sql: str) -> list[tuple[Any, ...]]:
    async with conn.execute(sql) as cursor:
        return [tuple(row) for row in await cursor.fetchall()]


async def build_meta(
    conn: aiosqlite.Connection,
    *,
    worker: str,
    instance: str,
    cluster: str,
    sealed_at: datetime,
    size_bytes: int,
) -> dict[str, Any]:
    """The meta object of the recorder file behind ``conn``.

    ``tables`` counts every table of the file except SQLite's own
    (``sqlite_*``); ``event_types`` and the first/last event times come
    from the ``events`` table when it exists.
    """
    table_names = [
        row[0]
        for row in await _scalar_rows(
            conn,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND substr(name, 1, 7) != 'sqlite_' ORDER BY name",
        )
    ]
    tables: dict[str, int] = {}
    for name in table_names:
        # The name comes from sqlite_master, not from input; quoting keeps
        # any odd character inside the identifier.
        quoted = '"' + name.replace('"', '""') + '"'
        (count,) = (await _scalar_rows(conn, f'SELECT COUNT(*) FROM {quoted}'))[0]
        tables[name] = int(count)

    event_types: dict[str, int] = {}
    first_ts: str | None = None
    last_ts: str | None = None
    if 'events' in tables:
        for event, count in await _scalar_rows(conn, 'SELECT event, COUNT(*) FROM events GROUP BY event'):
            event_types[str(event)] = int(count)
        ((low, high),) = await _scalar_rows(conn, 'SELECT MIN(ts), MAX(ts) FROM events')
        if low is not None:
            first_ts = format_epoch_micro(float(low))
            last_ts = format_epoch_micro(float(high))

    return {
        'schema': META_SCHEMA,
        'worker': worker,
        'instance': instance,
        'cluster': cluster,
        'first_event_ts': first_ts,
        'last_event_ts': last_ts,
        'sealed_at': format_rfc3339_micro(sealed_at),
        'size_bytes': size_bytes,
        'tables': dict(sorted(tables.items())),
        'event_types': dict(sorted(event_types.items())),
    }


def write_meta(path: str, meta: dict[str, Any]) -> None:
    """Write ``meta`` to ``path`` whole: a temp file (mode 0600) renamed over it."""
    folder, name = os.path.split(path)
    temp = os.path.join(folder, f'.{name}.{secrets.token_hex(8)}.tmp')
    data = serialize_json(meta)
    fd = os.open(temp, _WRITE_FLAGS, DB_FILE_MODE)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view) :]
    finally:
        os.close(fd)
    try:
        os.replace(temp, path)
    except OSError:
        try:
            os.unlink(temp)
        except OSError:
            pass
        raise


def read_meta(path: str) -> dict[str, Any] | None:
    """The meta object at ``path``, or ``None`` when it is missing, unreadable or not a schema-1 meta."""
    try:
        fd = os.open(path, _READ_FLAGS)
    except OSError:
        return None
    try:
        raw = os.read(fd, MAX_META_BYTES + 1)
    except OSError:
        return None
    finally:
        os.close(fd)
    if len(raw) > MAX_META_BYTES:
        return None
    try:
        meta = json.loads(raw.decode('utf-8'))
    except ValueError:
        return None
    if not isinstance(meta, dict):
        return None
    schema = meta.get('schema')
    if not isinstance(schema, int) or isinstance(schema, bool) or schema != META_SCHEMA:
        return None
    return meta
