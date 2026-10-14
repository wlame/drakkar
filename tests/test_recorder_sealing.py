"""The recorder seals its files into the shared history folder.

Rotation and a clean stop seal the file the recorder was writing; a file
that fails stays in a backlog retried on every rotation tick; files a
crashed process left behind are sealed in the background after start.
"""

import asyncio
import errno
import os
import sqlite3
import threading
import time
from itertools import count

import pytest
from structlog.testing import capture_logs

import drakkar.recorder.core as recorder_core
from drakkar.metrics import recorder_seal_failures, recorder_unsealed_files
from drakkar.models import SourceMessage
from drakkar.recorder import EventRecorder, sealing
from drakkar.recorder.helpers import make_db_path
from drakkar.recorder.history import read_meta
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE
from tests.conftest import make_ui_config, wait_for

WORKER = 'w-1'
INSTANCE = '1111111111111111'
OLD_ENOUGH_SECONDS = 900


@pytest.fixture(autouse=True)
def distinct_file_names(monkeypatch):
    """One name per rotation: the real names stamp the second, and a rotation
    inside the same second would reopen the file it just sealed."""
    seconds = count(1)
    monkeypatch.setattr(
        recorder_core,
        'make_db_path',
        lambda folder, worker: os.path.join(folder, f'{worker}-2026-10-14__08_00_{next(seconds):02d}.db'),
    )


@pytest.fixture
async def make_recorder(tmp_path):
    """Build recorders for ``tmp_path``; any a failed test left running is stopped.

    A recorder left open keeps non-daemon driver threads alive, which
    would hold the whole test run at interpreter exit.
    """
    made: list[EventRecorder] = []

    def build(*, worker: str = WORKER, cluster: str = 'search', storage: bool = True) -> EventRecorder:
        storage_dir = tmp_path / 'storage'
        storage_dir.mkdir(exist_ok=True)
        config = make_ui_config(enabled=True, store_output=False, flush_interval_seconds=60)
        recorder = EventRecorder(
            config,
            worker_name=worker,
            cluster_name=cluster,
            storage_dir=str(storage_dir) if storage else '',
            shared_dir=str(tmp_path / 'shared'),
            instance=INSTANCE,
        )
        made.append(recorder)
        return recorder

    yield build
    for recorder in made:
        if recorder._running:
            await recorder.stop()


def _message(offset: int) -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, value=b'{}', timestamp=1000)


def _history(tmp_path, cluster: str = 'search'):
    return tmp_path / 'shared' / 'history' / cluster


def _names(folder) -> list[str]:
    return sorted(p.name for p in folder.iterdir()) if folder.exists() else []


def _immutable_event_count(path) -> int:
    conn = sqlite3.connect(f'file:{path}?immutable=1', uri=True)
    try:
        return conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    finally:
        conn.close()


def _journal_mode(path) -> str:
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        return conn.execute('PRAGMA journal_mode').fetchone()[0]
    finally:
        conn.close()


def _leftover(storage_dir, name: str, events: int, *, age_seconds: float) -> str:
    """A recorder file a killed process left behind: WAL mode, rows still in the -wal."""
    source = storage_dir / f'.source-{name}'
    writer = sqlite3.connect(source)
    writer.execute('PRAGMA journal_mode=WAL')
    for schema in (SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE):
        writer.executescript(schema)
    for i in range(events):
        writer.execute('INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', (1760342400.0 + i, '', 'consumed'))
    writer.commit()
    path = storage_dir / name
    path.write_bytes(source.read_bytes())
    (storage_dir / f'{name}-wal').write_bytes((storage_dir / f'.source-{name}-wal').read_bytes())
    writer.close()
    source.unlink()
    stamp = time.time() - age_seconds
    for file in (path, storage_dir / f'{name}-wal'):
        os.utime(file, (stamp, stamp))
    return str(path)


# --- rotation ---


async def test_rotation_seals_the_old_file_into_the_cluster_history_folder(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    old_name = os.path.basename(rec.db_path)
    rec.record_consumed(_message(0))
    rec.record_consumed(_message(1))

    await rec._rotate()
    await rec._seal_pending()

    history = _history(tmp_path)
    assert _names(history) == [old_name, f'{old_name}.meta.json']
    sealed = history / old_name
    assert _journal_mode(sealed) == 'delete'
    assert _immutable_event_count(sealed) == 2
    meta = read_meta(f'{sealed}.meta.json')
    assert meta is not None
    assert (meta['worker'], meta['instance'], meta['cluster']) == (WORKER, INSTANCE, 'search')
    assert meta['tables']['events'] == 2
    assert meta['size_bytes'] == sealed.stat().st_size
    assert not (tmp_path / 'storage' / old_name).exists()
    await rec.stop()


async def test_rotation_keeps_the_rows_of_a_flush_already_writing_the_old_file(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    old_name = os.path.basename(rec.db_path)
    real_write_chunk = rec._write_chunk
    release = asyncio.Event()

    async def slow_write_chunk(db, query, columns, batch):
        await release.wait()
        return await real_write_chunk(db, query, columns, batch)

    real_create_schema = rec._create_schema
    late_flushes: list[asyncio.Task] = []

    async def create_schema_then_flush(db):
        await real_create_schema(db)
        if db is not rec._db and not late_flushes:
            # A flush that starts after rotation's own flush and captures
            # the old writer before the swap.
            rec._write_chunk = slow_write_chunk
            rec.record_consumed(_message(9))
            late_flushes.append(asyncio.create_task(rec._flush()))
            await asyncio.sleep(0)

    rec._create_schema = create_schema_then_flush
    rotation = asyncio.create_task(rec._rotate())
    await asyncio.sleep(0.2)
    release.set()
    await rotation
    # A flush on a connection the seal already closed never returns.
    await asyncio.wait_for(late_flushes[0], timeout=5)

    assert _immutable_event_count(tmp_path / 'storage' / old_name) == 1
    await rec.stop()


async def test_no_live_link_is_ever_created(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    after_start = _names(tmp_path / 'storage')
    await rec._rotate()
    after_rotation = _names(tmp_path / 'storage')
    await rec.stop()

    for names in (after_start, after_rotation, _names(tmp_path / 'storage')):
        assert not [name for name in names if name.endswith('-live.db')]


async def test_a_hung_shared_folder_never_holds_up_rotation(tmp_path, monkeypatch, make_recorder):
    unblock = threading.Event()

    def hung_rename(source, destination):
        unblock.wait(10)
        os.rename(source, destination)

    monkeypatch.setattr(sealing, '_rename', hung_rename)
    rec = make_recorder()
    await rec.start()
    first = rec.db_path
    await asyncio.wait_for(rec._rotate(), timeout=2)
    rec._start_sealing()
    second = rec.db_path

    # The move of the first file is stuck; the next rotation still runs.
    await asyncio.wait_for(rec._rotate(), timeout=2)

    assert _journal_mode(first) == 'delete'
    assert _journal_mode(second) == 'delete'
    unblock.set()
    await wait_for(lambda: (_history(tmp_path) / os.path.basename(first)).exists())
    await wait_for(lambda: rec._sealing_task is not None and rec._sealing_task.done())
    await rec._seal_pending()
    assert (_history(tmp_path) / os.path.basename(second)).exists()


# --- clean stop ---


async def test_a_clean_stop_seals_the_live_file(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    name = os.path.basename(rec.db_path)
    rec.record_consumed(_message(0))

    await rec.stop()

    assert _names(tmp_path / 'storage') == []
    assert _names(_history(tmp_path)) == [name, f'{name}.meta.json']
    assert _immutable_event_count(_history(tmp_path) / name) == 1


async def test_a_stop_seals_the_live_file_while_a_page_reads_it(tmp_path, make_recorder):
    """A databases-page load or a trace that overlaps the stop must not leave
    the live file unsealed: the seal waits for the short-lived reader."""
    rec = make_recorder()
    await rec.start()
    name = os.path.basename(rec.db_path)
    reader = sqlite3.connect(f'file:{rec.db_path}?mode=ro', uri=True, check_same_thread=False)
    reader.execute('SELECT COUNT(*) FROM events').fetchone()
    closer = threading.Timer(0.3, reader.close)
    closer.start()

    await rec.stop()

    closer.join()
    assert _names(_history(tmp_path)) == [name, f'{name}.meta.json']


async def test_a_stop_with_no_time_left_seals_the_file_but_leaves_it_in_storage(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    path = rec.db_path

    with capture_logs() as logs:
        await rec.stop(timeout=0)

    assert _names(tmp_path / 'storage') == [os.path.basename(path)]
    assert _journal_mode(path) == 'delete'
    assert _names(_history(tmp_path)) == []
    (failure,) = [entry for entry in logs if entry['event'] == 'recorder_seal_failed']
    assert failure['stage'] == 'publish'


async def test_a_slow_publish_does_not_hold_up_the_stop(tmp_path, monkeypatch, make_recorder):
    unblock = threading.Event()

    def stuck_rename(source, destination):
        unblock.wait(10)
        raise OSError(errno.EIO, 'Input/output error')

    monkeypatch.setattr(sealing, '_rename', stuck_rename)
    rec = make_recorder()
    await rec.start()
    path = rec.db_path

    started = time.monotonic()
    await rec.stop(timeout=0.2)
    elapsed = time.monotonic() - started

    unblock.set()
    assert elapsed < 2
    assert os.path.exists(path)


# --- backlog ---


async def test_a_seal_failure_keeps_the_file_in_the_backlog_and_the_next_tick_retries_it(
    tmp_path, monkeypatch, make_recorder
):
    monkeypatch.setattr(sealing, 'SEAL_RETRY_DELAYS', (0.01,))
    rec = make_recorder()
    await rec.start()
    old_path = rec.db_path
    rec.record_consumed(_message(0))
    await rec._flush()
    # Another connection inside a read transaction: the journal mode cannot change.
    other = sqlite3.connect(old_path)
    other.execute('BEGIN')
    other.execute('SELECT COUNT(*) FROM events').fetchone()
    failures_before = recorder_seal_failures._value.get()

    with capture_logs() as logs:
        await rec._rotate()

    assert os.path.exists(old_path)
    assert recorder_seal_failures._value.get() == failures_before + 1
    assert recorder_unsealed_files._value.get() == 1
    (failure,) = [entry for entry in logs if entry['event'] == 'recorder_seal_failed']
    assert failure['stage'] == 'seal'

    other.close()
    await rec._seal_pending()

    name = os.path.basename(old_path)
    assert not os.path.exists(old_path)
    assert _immutable_event_count(_history(tmp_path) / name) == 1
    assert recorder_unsealed_files._value.get() == 0
    await rec.stop()


async def test_a_publish_failure_is_retried_from_the_backlog(tmp_path, monkeypatch, make_recorder):
    def denied(source, destination):
        raise PermissionError(errno.EACCES, 'Permission denied')

    rec = make_recorder()
    await rec.start()
    old_path = rec.db_path
    monkeypatch.setattr(sealing, '_rename', denied)
    await rec._rotate()

    with capture_logs() as logs:
        await rec._seal_pending()

    assert os.path.exists(old_path)
    (failure,) = [entry for entry in logs if entry['event'] == 'recorder_seal_failed']
    assert failure['stage'] == 'publish'

    monkeypatch.setattr(sealing, '_rename', os.rename)
    await rec._seal_pending()

    assert not os.path.exists(old_path)
    assert (_history(tmp_path) / os.path.basename(old_path)).exists()
    await rec.stop()


async def test_a_backlog_file_that_disappeared_is_dropped_without_a_new_database(tmp_path, make_recorder):
    rec = make_recorder()
    await rec.start()
    gone = str(tmp_path / 'storage' / f'{WORKER}-2026-10-14__07_00_00.db')
    rec._backlog.append(gone)

    await rec._seal_pending()

    assert not os.path.exists(gone)
    assert rec._backlog == []
    await rec.stop()


async def test_the_cross_filesystem_path_leaves_no_incoming_file(tmp_path, monkeypatch, make_recorder):
    def cross_device(source, destination):
        raise OSError(errno.EXDEV, 'Invalid cross-device link')

    monkeypatch.setattr(sealing, '_rename', cross_device)
    rec = make_recorder()
    await rec.start()
    old_name = os.path.basename(rec.db_path)
    rec.record_consumed(_message(0))

    await rec._rotate()
    await rec._seal_pending()

    assert _names(_history(tmp_path)) == [old_name, f'{old_name}.meta.json']
    assert _immutable_event_count(_history(tmp_path) / old_name) == 1
    assert not (tmp_path / 'storage' / old_name).exists()
    await rec.stop()


# --- leftovers ---


async def test_leftover_sealing_replays_a_crashed_wal_in_the_background(tmp_path, make_recorder):
    storage = tmp_path / 'storage'
    storage.mkdir()
    name = f'{WORKER}-2026-10-13__07_00_00.db'
    _leftover(storage, name, 5, age_seconds=OLD_ENOUGH_SECONDS)
    rec = make_recorder()
    await rec.start()

    with capture_logs() as logs:
        rec.start_leftover_sealing()
        await wait_for(lambda: (_history(tmp_path) / name).exists())
        await wait_for(lambda: any(entry['event'] == 'recorder_leftover_sealed' for entry in logs))

    assert _immutable_event_count(_history(tmp_path) / name) == 5
    assert not (storage / name).exists()
    assert not (storage / f'{name}-wal').exists()
    await rec.stop()


async def test_a_leftover_younger_than_the_floor_is_left_alone(tmp_path, make_recorder):
    storage = tmp_path / 'storage'
    storage.mkdir()
    name = f'{WORKER}-2026-10-13__07_00_00.db'
    _leftover(storage, name, 1, age_seconds=10)
    rec = make_recorder()
    await rec.start()

    await rec._seal_pending()

    assert (storage / name).exists()
    assert _names(_history(tmp_path)) == []
    await rec.stop()


async def test_the_leftover_task_looks_again_once_the_floor_has_passed(tmp_path, make_recorder):
    storage = tmp_path / 'storage'
    storage.mkdir()
    name = f'{WORKER}-2026-10-13__07_00_00.db'
    path = _leftover(storage, name, 1, age_seconds=10)
    rec = make_recorder()
    rec._leftover_floor_seconds = 0.3
    await rec.start()

    rec.start_leftover_sealing()
    await wait_for(lambda: (_history(tmp_path) / name).exists())

    assert not os.path.exists(path)
    await rec.stop()


async def test_another_workers_files_are_never_sealed(tmp_path, make_recorder):
    storage = tmp_path / 'storage'
    storage.mkdir()
    other = f'{WORKER}-b-2026-10-13__07_00_00.db'
    _leftover(storage, other, 1, age_seconds=OLD_ENOUGH_SECONDS)
    rec = make_recorder()
    await rec.start()

    await rec._seal_pending()

    assert (storage / other).exists()
    assert _journal_mode(storage / other) == 'wal'
    await rec.stop()


async def test_the_live_file_is_never_a_leftover_however_the_folder_is_spelled(tmp_path, monkeypatch):
    # The real name builder: it goes through pathlib, which drops the './'.
    monkeypatch.setattr(recorder_core, 'make_db_path', make_db_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'storage').mkdir()
    config = make_ui_config(enabled=True, store_output=False, flush_interval_seconds=60)
    rec = EventRecorder(config, worker_name=WORKER, cluster_name='search', storage_dir='./storage', instance=INSTANCE)
    await rec.start()
    try:
        stamp = time.time() - OLD_ENOUGH_SECONDS
        for name in os.listdir('storage'):
            os.utime(os.path.join('storage', name), (stamp, stamp))

        with capture_logs() as logs:
            await rec._seal_pending()

        assert rec._backlog == []
        assert not [entry for entry in logs if entry['event'] == 'recorder_seal_failed']
    finally:
        await rec.stop()


# --- memory-only ---


async def test_memory_only_creates_nothing(tmp_path, monkeypatch, make_recorder):
    monkeypatch.chdir(tmp_path)
    rec = make_recorder(storage=False)
    before = _names(tmp_path)
    await rec.start()
    rec.record_consumed(_message(0))
    await rec._rotate()
    await rec._seal_pending()
    await rec.stop()

    assert _names(tmp_path) == before
