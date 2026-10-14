"""History files: the meta file next to a sealed recorder file, and the shared layout.

``tests/fixtures/history_meta.json`` pins the meta bytes, the archive lock
body and the epoch-to-text conversion for any implementation.
"""

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import pytest

from drakkar.recorder.history import (
    META_SCHEMA,
    build_meta,
    history_dir,
    merged_dir,
    meta_path,
    read_meta,
    sanitize_cluster,
    serialize_json,
    write_meta,
)
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE
from drakkar.timefmt import format_epoch_micro, format_epoch_ns

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'history_meta.json').read_text(encoding='utf-8'))

SEALED_AT = datetime(2026, 10, 14, 9, 0, 2, 410000, tzinfo=UTC)


@pytest.mark.parametrize('case', FIXTURE['epochs'], ids=lambda case: repr(case['ts']))
def test_format_epoch_micro_matches_the_fixture(case):
    assert format_epoch_micro(case['ts']) == case['text']


@pytest.mark.parametrize('case', FIXTURE['mtimes_ns'], ids=lambda case: str(case['ns']))
def test_format_epoch_ns_matches_the_fixture(case):
    assert format_epoch_ns(case['ns']) == case['text']


def test_serialize_json_produces_the_meta_bytes():
    assert serialize_json(FIXTURE['meta']) == FIXTURE['bytes'].encode('utf-8')
    assert serialize_json(FIXTURE['empty_meta']) == FIXTURE['empty_bytes'].encode('utf-8')


def test_serialize_json_produces_the_lock_bytes():
    assert serialize_json(FIXTURE['lock']) == FIXTURE['lock_bytes'].encode('utf-8')


async def _recorder_file(path: Path, events: list[tuple[str, float]]) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(path))
    for schema in (SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE):
        await conn.executescript(schema)
    for event, ts in events:
        await conn.execute('INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', (ts, '', event))
    await conn.execute(
        'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) VALUES (1, ?, ?, 0, ?)',
        ('w-1', 'search', ''),
    )
    await conn.commit()
    return conn


async def test_build_meta_counts_tables_and_event_types(tmp_path):
    conn = await _recorder_file(
        tmp_path / 'w-1.db',
        [('task_started', 1760342400.123456), ('task_completed', 1760342399.871), ('task_started', 1760342401.0)],
    )
    try:
        meta = await build_meta(
            conn, worker='w-1', instance='9c41e0b27fd34a1e', cluster='search', sealed_at=SEALED_AT, size_bytes=8192
        )
    finally:
        await conn.close()

    assert meta == {
        'schema': META_SCHEMA,
        'worker': 'w-1',
        'instance': '9c41e0b27fd34a1e',
        'cluster': 'search',
        'first_event_ts': '2025-10-13T07:59:59.871000Z',
        'last_event_ts': '2025-10-13T08:00:01.000000Z',
        'sealed_at': '2026-10-14T09:00:02.410000Z',
        'size_bytes': 8192,
        # sqlite_sequence (from AUTOINCREMENT) is SQLite's own table and is left out.
        'tables': {'events': 3, 'worker_config': 1, 'worker_state': 0},
        'event_types': {'task_completed': 1, 'task_started': 2},
    }


async def test_build_meta_of_a_file_without_events(tmp_path):
    conn = await _recorder_file(tmp_path / 'w-1.db', [])
    try:
        meta = await build_meta(conn, worker='w-1', instance='i', cluster='', sealed_at=SEALED_AT, size_bytes=4096)
    finally:
        await conn.close()

    assert (meta['first_event_ts'], meta['last_event_ts'], meta['event_types']) == (None, None, {})
    assert meta['tables']['events'] == 0


async def test_build_meta_of_a_file_without_an_events_table(tmp_path):
    conn = await aiosqlite.connect(str(tmp_path / 'odd.db'))
    await conn.execute('CREATE TABLE other (x INTEGER)')
    try:
        meta = await build_meta(conn, worker='w-1', instance='i', cluster='', sealed_at=SEALED_AT, size_bytes=1)
    finally:
        await conn.close()

    assert meta['tables'] == {'other': 0}
    assert (meta['first_event_ts'], meta['event_types']) == (None, {})


def test_write_meta_writes_the_bytes_owner_only_with_no_temp_left(tmp_path):
    db = tmp_path / 'w-1-2026-10-14__08_00_00.db'

    write_meta(meta_path(str(db)), FIXTURE['meta'])

    written = tmp_path / 'w-1-2026-10-14__08_00_00.db.meta.json'
    assert written.read_bytes() == FIXTURE['bytes'].encode('utf-8')
    assert stat.S_IMODE(written.stat().st_mode) == 0o600
    assert sorted(p.name for p in tmp_path.iterdir()) == ['w-1-2026-10-14__08_00_00.db.meta.json']


def test_read_meta_returns_a_schema_1_meta(tmp_path):
    path = tmp_path / 'a.db.meta.json'
    path.write_bytes(FIXTURE['bytes'].encode('utf-8'))

    assert read_meta(str(path)) == FIXTURE['meta']


@pytest.mark.parametrize(
    'content',
    [b'{not json', b'[1]', json.dumps(dict(FIXTURE['meta'], schema=2)).encode(), b'{"schema": "1"}'],
    ids=['not-json', 'not-object', 'other-schema', 'schema-not-int'],
)
def test_read_meta_ignores_what_it_cannot_use(tmp_path, content):
    path = tmp_path / 'a.db.meta.json'
    path.write_bytes(content)

    assert read_meta(str(path)) is None


def test_read_meta_of_a_missing_file_is_none(tmp_path):
    assert read_meta(str(tmp_path / 'absent.db.meta.json')) is None


def test_read_meta_never_follows_a_symlink(tmp_path):
    target = tmp_path / 'elsewhere.json'
    target.write_bytes(FIXTURE['bytes'].encode('utf-8'))
    link = tmp_path / 'a.db.meta.json'
    os.symlink(target, link)

    assert read_meta(str(link)) is None


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ('search', 'search'),
        ('search-fleet_1', 'search-fleet_1'),
        ('a/b c', 'a_b_c'),
        ('', 'default'),
        (None, 'default'),
    ],
)
def test_sanitize_cluster_maps_unsafe_characters(raw, expected):
    assert sanitize_cluster(raw) == expected


@pytest.mark.parametrize(
    ('cluster', 'expected'),
    [('search', 'history/search'), ('', 'history/default'), ('a/b c', 'history/a_b_c')],
)
def test_history_dir_uses_the_sanitized_cluster(tmp_path, cluster, expected):
    assert history_dir(str(tmp_path), cluster) == os.path.join(str(tmp_path), expected)


def test_merged_dir(tmp_path):
    assert merged_dir(str(tmp_path)) == os.path.join(str(tmp_path), 'merged')
