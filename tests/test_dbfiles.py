"""Permissions of the SQLite files the recorder and the cache engine write.

The stores persist message-derived data (task args, subprocess output,
handler results) into a ``db_dir`` that defaults to the world-readable,
world-writable ``/tmp``. These tests pin the invariant that the DB file
**and both WAL sidecars** end up owner-only, in every path that creates
one: recorder start, recorder rotation, and cache-engine start.

The sidecars are the part that regresses silently. SQLite copies the main
database's permission bits onto ``-wal`` / ``-shm`` when it creates them,
so the whole guarantee rests on securing the main file *before* the driver
opens it. Chmod-ing afterwards — which is what the code used to do —
leaves both sidecars at 0644 while the main file reads 0600, so an
assertion on the main file alone would pass against the bug.

The Go backend mirrors this in internal/dbfile; both fleets share a
``db_dir``, so the two must agree.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from drakkar.cache import CacheEngine
from drakkar.config import CacheConfig, UIConfig
from drakkar.dbfiles import DB_DIR_MODE, DB_FILE_MODE, secure_db_file
from drakkar.recorder import EventRecorder
from tests.conftest import make_ui_config

WORKER_NAME = 'test-worker'


def mode_of(path: str | Path) -> int:
    """Permission bits of ``path``, or -1 when it does not exist."""
    try:
        return stat.S_IMODE(os.stat(path).st_mode)
    except OSError:
        return -1


def assert_db_files_owner_only(db_path: str) -> None:
    """Assert the DB and any existing WAL sidecars are 0600.

    A sidecar that does not exist is fine — SQLite deletes them on a clean
    close. What must never happen is a sidecar that exists and is readable
    by anyone else.
    """
    assert mode_of(db_path) == DB_FILE_MODE, f'{db_path} is {oct(mode_of(db_path))}'
    for suffix in ('-wal', '-shm'):
        sidecar = db_path + suffix
        actual = mode_of(sidecar)
        if actual == -1:
            continue
        assert actual == DB_FILE_MODE, f'{sidecar} is {oct(actual)}, expected {oct(DB_FILE_MODE)}'


def make_recorder_config(tmp_path: Path, **overrides) -> UIConfig:
    defaults: dict = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': True,
        'store_config': False,
        'store_state': False,
    }
    defaults.update(overrides)
    return make_ui_config(**defaults)


# --- secure_db_file unit behaviour ---


def test_secure_db_file_creates_missing_file_owner_only(tmp_path):
    db_path = str(tmp_path / 'new.db')

    secure_db_file(db_path)

    assert mode_of(db_path) == DB_FILE_MODE
    # An empty file is a valid empty SQLite database, so pre-creating it
    # must not put bytes in the way of the driver.
    assert os.path.getsize(db_path) == 0


def test_secure_db_file_tightens_existing_file_and_sidecars(tmp_path):
    """A crash leaves sidecars behind; re-opening must tighten them too."""
    db_path = str(tmp_path / 'existing.db')
    for path in (db_path, db_path + '-wal', db_path + '-shm'):
        Path(path).write_bytes(b'')
        os.chmod(path, 0o644)

    secure_db_file(db_path)

    assert_db_files_owner_only(db_path)


def test_secure_db_file_does_not_follow_a_planted_symlink(tmp_path):
    """A predictable filename in a world-writable db_dir invites a symlink swap.

    ``O_EXCL`` refuses to create through the link, so the victim keeps its
    own mode and never becomes the worker's database.
    """
    victim = tmp_path / 'victim'
    victim.write_text('secret')
    os.chmod(victim, 0o644)
    db_path = str(tmp_path / 'planted.db')
    os.symlink(victim, db_path)

    secure_db_file(db_path)

    assert mode_of(victim) == 0o644, 'chmod followed the symlink to the victim'
    assert victim.read_text() == 'secret'


def test_secure_db_file_survives_unwritable_directory(tmp_path):
    """Best-effort: permissions failures must never abort store startup."""
    locked = tmp_path / 'locked'
    locked.mkdir(mode=0o500)
    try:
        secure_db_file(str(locked / 'nope.db'))  # must not raise
    finally:
        os.chmod(locked, 0o700)


# --- recorder ---


async def test_recorder_db_and_wal_sidecars_are_owner_only(tmp_path):
    """Start + a real write, so the -wal/-shm sidecars actually exist."""
    rec = EventRecorder(make_recorder_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_committed(partition=0, offset=1)
        await rec.flush()
        assert mode_of(rec.db_path + '-wal') != -1, 'expected a WAL sidecar after a write'
        assert_db_files_owner_only(rec.db_path)
    finally:
        await rec.stop()


async def test_recorder_rotation_keeps_the_new_db_owner_only(tmp_path, monkeypatch):
    """Rotation opens a brand-new file and used to skip securing it entirely.

    ``make_db_path`` stamps to the second, so a test that rotates inside
    one second would reuse the same filename and silently assert nothing.
    Patch in a counter to guarantee a genuinely new path.
    """
    import drakkar.recorder.core as recorder_core

    real_make_db_path = recorder_core.make_db_path
    counter = iter(range(1, 100))
    monkeypatch.setattr(
        recorder_core,
        'make_db_path',
        lambda db_dir, worker_name: real_make_db_path(db_dir, f'{worker_name}-r{next(counter)}'),
    )

    rec = EventRecorder(make_recorder_config(tmp_path, retention_hours=24), worker_name=WORKER_NAME)
    await rec.start()
    try:
        original_path = rec.db_path
        await rec._rotate()
        assert rec.db_path != original_path, 'rotation should have opened a new file'

        rec.record_committed(partition=0, offset=2)
        await rec.flush()
        assert mode_of(rec.db_path + '-wal') != -1, 'expected a WAL sidecar after a write'
        assert_db_files_owner_only(rec.db_path)
    finally:
        await rec.stop()


# --- cache engine ---


async def test_cache_db_and_wal_sidecars_are_owner_only(tmp_path):
    db_dir = tmp_path / 'cachedir'
    engine = CacheEngine(
        config=CacheConfig(enabled=True, db_dir=str(db_dir)),
        ui_config=make_recorder_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert_db_files_owner_only(engine._db_path)
    finally:
        await engine.stop()


async def test_cache_creates_its_db_dir_owner_only(tmp_path):
    """A db_dir Drakkar creates itself must not be group/world traversable."""
    db_dir = tmp_path / 'created-by-drakkar'
    engine = CacheEngine(
        config=CacheConfig(enabled=True, db_dir=str(db_dir)),
        ui_config=make_recorder_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert mode_of(db_dir) == DB_DIR_MODE
    finally:
        await engine.stop()


@pytest.mark.parametrize('pre_existing_mode', [0o755, 0o777])
async def test_cache_leaves_an_operator_owned_db_dir_alone(tmp_path, pre_existing_mode):
    """``makedirs`` mode applies only to directories we create.

    An operator who points db_dir at an existing shared directory keeps
    whatever mode they chose — we tighten the files inside, not somebody
    else's directory. The world-writable warning covers that case instead.
    """
    db_dir = tmp_path / 'operator-owned'
    db_dir.mkdir(mode=pre_existing_mode)
    os.chmod(db_dir, pre_existing_mode)
    engine = CacheEngine(
        config=CacheConfig(enabled=True, db_dir=str(db_dir)),
        ui_config=make_recorder_config(tmp_path),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    await engine.start()
    try:
        assert mode_of(db_dir) == pre_existing_mode
        assert_db_files_owner_only(engine._db_path)
    finally:
        await engine.stop()
