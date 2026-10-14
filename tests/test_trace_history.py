"""Trace searches: the worker's own files, or the sealed files of its cluster history folder.

Without ``history`` a trace reads only what this worker wrote and has not
sealed into the history folder yet (the live file and closed own files).
With ``history`` it reads only the sealed files of ``history/<own cluster>/``,
newest first, within a file and time budget, opened with ``immutable=1``.
"""

import os
import sqlite3
from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs

from drakkar.models import SourceMessage
from drakkar.recorder import EventRecorder
from drakkar.recorder.queries import trace_history
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG
from tests.conftest import make_ui_config

CLUSTER = 'search'
PARTITION = 3
OFFSET = 77


def _db_name(worker: str, start: float) -> str:
    return f'{worker}-{datetime.fromtimestamp(start, tz=UTC).strftime("%Y-%m-%d__%H_%M_%S")}.db'


def _sealed_file(folder, worker: str, start: float, events: list[tuple[float, str, int, int]]) -> str:
    """A sealed recorder file (rollback journal) holding ``(ts, event, partition, offset)`` rows."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / _db_name(worker, start)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) VALUES (1, ?, ?, ?, ?)',
        [worker, CLUSTER, start, ''],
    )
    db.executescript(SCHEMA_EVENTS)
    for ts, event, partition, offset in events:
        db.execute(
            'INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, ?, ?, ?)',
            [ts, '', event, partition, offset],
        )
    db.commit()
    db.close()
    return str(path)


def _search(folder, **budget) -> list[dict]:
    limits = {'max_files': 256, 'max_seconds': 10.0} | budget
    return trace_history(str(folder), PARTITION, OFFSET, **limits)


# --- trace_history ---


def test_trace_history_gathers_a_message_from_every_sealed_file_in_time_order(tmp_path):
    folder = tmp_path / 'history' / CLUSTER
    _sealed_file(folder, 'w-1', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    _sealed_file(folder, 'w-2', 1_760_003_600, [(1_760_003_610.0, 'task_completed', PARTITION, OFFSET)])
    _sealed_file(folder, 'w-2', 1_760_007_200, [(1_760_007_210.0, 'consumed', PARTITION, OFFSET + 1)])

    rows = _search(folder)

    assert [(row['worker_name'], row['event']) for row in rows] == [('w-1', 'consumed'), ('w-2', 'task_completed')]


def test_trace_history_reads_the_newest_files_first_within_its_file_budget(tmp_path):
    folder = tmp_path / 'history' / CLUSTER
    _sealed_file(folder, 'w-1', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    _sealed_file(folder, 'w-1', 1_760_003_600, [(1_760_003_610.0, 'task_completed', PARTITION, OFFSET)])

    with capture_logs() as logs:
        rows = _search(folder, max_files=1)

    assert [row['event'] for row in rows] == ['task_completed']
    (truncated,) = [entry for entry in logs if entry['event'] == 'cross_trace_scan_truncated']
    assert truncated['op'] == 'trace_history'


def test_trace_history_reads_sealed_files_without_taking_locks(tmp_path):
    folder = tmp_path / 'history' / CLUSTER
    path = _sealed_file(folder, 'w-1', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    holder = sqlite3.connect(path)
    holder.execute('BEGIN EXCLUSIVE')
    try:
        rows = _search(folder, max_seconds=2.0)
    finally:
        holder.rollback()
        holder.close()

    assert len(rows) == 1


@pytest.mark.parametrize(
    'name',
    ['.w-9-2025-10-09__08_00_00.db.0123456789abcdef.incoming', 'w-9-2025-10-09__08_00_00.db.unreadable', 'notes.txt'],
)
def test_trace_history_skips_what_is_not_a_sealed_file(tmp_path, name):
    folder = tmp_path / 'history' / CLUSTER
    _sealed_file(folder, 'w-1', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    (folder / name).write_bytes(b'not a database')

    assert len(_search(folder)) == 1


def test_trace_history_skips_a_file_it_cannot_read(tmp_path):
    folder = tmp_path / 'history' / CLUSTER
    _sealed_file(folder, 'w-1', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    (folder / _db_name('w-2', 1_760_003_600)).write_bytes(b'garbage' * 100)

    assert len(_search(folder)) == 1


@pytest.mark.parametrize('folder', ['', 'missing'])
def test_trace_history_without_a_folder_finds_nothing(tmp_path, folder):
    assert trace_history(str(tmp_path / folder) if folder else '', PARTITION, OFFSET, max_files=8, max_seconds=1) == []


# --- the trace of the worker's own files ---


def _message(offset: int) -> SourceMessage:
    return SourceMessage(topic='t', partition=PARTITION, offset=offset, value=b'{}', timestamp=1000)


@pytest.fixture
async def recorder(tmp_path):
    (tmp_path / 'storage').mkdir()
    config = make_ui_config(enabled=True, store_output=False, flush_interval_seconds=60)
    rec = EventRecorder(
        config,
        worker_name='w-1',
        cluster_name=CLUSTER,
        storage_dir=str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
    )
    await rec.start()
    yield rec
    await rec.stop()


async def test_the_own_trace_reads_the_live_file_and_closed_own_files(tmp_path, recorder):
    recorder.record_consumed(_message(OFFSET))
    closed = _sealed_file(
        tmp_path / 'storage', 'w-1', 1_760_000_000, [(1_700_000_000.0, 'task_completed', PARTITION, OFFSET)]
    )

    rows = await recorder.cross_trace(PARTITION, OFFSET)

    assert [(row['worker_name'], row['event']) for row in rows] == [('w-1', 'task_completed'), ('w-1', 'consumed')]
    assert os.path.exists(closed)


async def test_the_own_trace_never_reads_other_workers_or_the_history_folder(tmp_path, recorder):
    _sealed_file(tmp_path / 'storage', 'w-2', 1_760_000_000, [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)])
    _sealed_file(
        tmp_path / 'shared' / 'history' / CLUSTER,
        'w-1',
        1_760_000_000,
        [(1_760_000_010.0, 'consumed', PARTITION, OFFSET)],
    )

    assert await recorder.cross_trace(PARTITION, OFFSET) == []


async def test_the_label_trace_reads_only_the_live_file(tmp_path, recorder):
    peer = _sealed_file(tmp_path / 'storage', 'w-2', 1_760_000_000, [])
    db = sqlite3.connect(peer)
    db.execute(
        "INSERT INTO events (ts, dt, event, task_id, labels) VALUES (1.0, '', 'task_started', 't-9', '{\"k\": \"v\"}')"
    )
    db.commit()
    db.close()

    assert await recorder.cross_trace_by_label('k', 'v') == []
