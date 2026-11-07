"""The own folder of a worker: its path, its owner lock, its start sweep, and the worker name rule."""

import fcntl
import os
import threading
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from drakkar import ownerlock
from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig, own_storage_dir
from drakkar.config.identity import (
    check_cluster_name,
    check_worker_name,
    generated_worker_name,
    is_valid_worker_name,
)
from drakkar.ownerlock import (
    OWNER_LOCK_NAME,
    FolderInUseError,
    open_own_folder,
    owner_lock_held,
    remove_download_temps,
    take_own_lock,
    try_sibling_lock,
)
from tests.conftest import kafka_connections
from tests.test_app import SimpleHandler


def _config(tmp_path: Path, **sections) -> DrakkarConfig:
    values = {
        'connections': kafka_connections(),
        'storage': {'dir': str(tmp_path / 'storage')},
        'sources': {'kafka': {'enabled': True, 'topic': 'in'}},
        'sinks': {'kafka': {'results': {'topic': 'out'}}},
        'executor': {'binary_path': '/bin/echo'},
        'metrics': {'enabled': False},
        'ui': {'enabled': False, 'release': {'enabled': False}},
    }
    values.update(sections)
    return DrakkarConfig(**values)


# --- the path ---


@pytest.mark.parametrize(
    ('storage_dir', 'cluster', 'worker', 'expected'),
    [
        ('/data', 'search', 'w-1', '/data/search/w-1'),
        ('/data', '', 'w-1', '/data/default/w-1'),
        ('/data', 'search.eu', 'w-1', '/data/search_eu/w-1'),
        ('./data/', 'search', 'w-1', 'data/search/w-1'),
        ('', 'search', 'w-1', ''),
    ],
    ids=['cluster', 'empty-cluster-is-default', 'cluster-sanitized', 'normalized', 'memory-only'],
)
def test_own_storage_dir_joins_the_sanitized_cluster_and_the_worker(storage_dir, cluster, worker, expected):
    assert own_storage_dir(storage_dir, cluster, worker) == expected


def test_own_storage_dir_leaves_the_shared_folder_at_the_top_level(tmp_path):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path, cluster={'name': 'search'}), worker_id='w-1')

    assert app.own_storage_dir == str(tmp_path / 'storage' / 'search' / 'w-1')
    assert app._config.resolved_shared_dir == str(tmp_path / 'storage')


# --- the owner lock ---


@pytest.fixture
def short_lock_wait(monkeypatch):
    """A refused start gives up after 0.1 s instead of the real wait, so a refusal test stays fast."""
    monkeypatch.setattr(ownerlock, 'OWN_LOCK_WAIT_SECONDS', 0.1)
    monkeypatch.setattr(ownerlock, 'OWN_LOCK_RETRY_SECONDS', 0.01)


def test_take_own_lock_waits_out_a_short_shared_probe(tmp_path, monkeypatch):
    take_own_lock(str(tmp_path)).release()
    probe_holds = threading.Event()
    refused_once = threading.Event()
    real_try_lock = ownerlock._try_lock
    tries: list[bool] = []

    def counting_try_lock(folder, *, create):
        lock = real_try_lock(folder, create=create)
        tries.append(lock is not None)
        if lock is None:
            refused_once.set()
        return lock

    monkeypatch.setattr(ownerlock, '_try_lock', counting_try_lock)

    def probe() -> None:
        # What the Databases view does to a sibling folder, held until the start was refused once.
        fd = os.open(tmp_path / OWNER_LOCK_NAME, os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            probe_holds.set()
            refused_once.wait(5)
        finally:
            os.close(fd)

    thread = threading.Thread(target=probe)
    thread.start()
    assert probe_holds.wait(5)
    lock = take_own_lock(str(tmp_path))
    thread.join()
    try:
        assert lock.held
        assert tries[0] is False
        assert tries[-1] is True
        assert len(tries) >= 2
    finally:
        lock.release()


def test_take_own_lock_makes_the_folder_again_when_an_orphan_pass_renamed_it_away(tmp_path, monkeypatch):
    folder = tmp_path / 'own'
    folder.mkdir()
    real_try_lock = ownerlock._try_lock
    calls: list[int] = []

    def held_then_renamed(path, *, create):
        calls.append(1)
        if len(calls) == 1:
            # The orphan pass holds the lock, then moves the folder to its staging name.
            folder.rename(tmp_path / '.orphan-own-0123')
            return None
        return real_try_lock(path, create=create)

    monkeypatch.setattr(ownerlock, '_try_lock', held_then_renamed)
    lock = take_own_lock(str(folder))
    try:
        assert lock.held
        assert (folder / OWNER_LOCK_NAME).is_file()
        assert (tmp_path / '.orphan-own-0123').is_dir()
    finally:
        lock.release()


def test_take_own_lock_second_holder_fails_with_the_folder_in_use_message(tmp_path, short_lock_wait):
    first = take_own_lock(str(tmp_path))
    try:
        # Two open file descriptions are two flock holders, also in one process.
        with pytest.raises(FolderInUseError) as refused:
            take_own_lock(str(tmp_path))
    finally:
        first.release()

    assert str(refused.value) == (
        f'storage.dir: the folder {tmp_path} is in use by another process; '
        'give each worker a unique worker name (worker.name or WORKER_ID)'
    )


def test_take_own_lock_after_release_succeeds_and_release_twice_is_safe(tmp_path):
    first = take_own_lock(str(tmp_path))
    first.release()
    first.release()

    second = take_own_lock(str(tmp_path))
    try:
        assert second.held
        assert second.path == str(tmp_path / OWNER_LOCK_NAME)
        assert second.folder == str(tmp_path)
    finally:
        second.release()
    assert not second.held


def test_try_sibling_lock_held_folder_returns_none(tmp_path):
    owner = take_own_lock(str(tmp_path))
    try:
        assert try_sibling_lock(str(tmp_path)) is None
    finally:
        owner.release()


def test_try_sibling_lock_free_folder_returns_a_held_lock(tmp_path, short_lock_wait):
    take_own_lock(str(tmp_path)).release()

    lock = try_sibling_lock(str(tmp_path))
    try:
        assert lock is not None
        with pytest.raises(FolderInUseError):
            take_own_lock(str(tmp_path))
    finally:
        assert lock is not None
        lock.release()


def test_try_sibling_lock_folder_without_lock_file_returns_none_and_creates_nothing(tmp_path):
    assert try_sibling_lock(str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_owner_lock_held_held_folder_returns_true(tmp_path):
    owner = take_own_lock(str(tmp_path))
    try:
        assert owner_lock_held(str(tmp_path)) is True
    finally:
        owner.release()


def test_owner_lock_held_free_folder_returns_false_and_leaves_it_free(tmp_path):
    take_own_lock(str(tmp_path)).release()

    assert owner_lock_held(str(tmp_path)) is False
    # The probe freed its shared lock: an owner can take the folder at once.
    take_own_lock(str(tmp_path)).release()


def test_owner_lock_held_folder_without_lock_file_returns_none_and_creates_nothing(tmp_path):
    assert owner_lock_held(str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_owner_lock_held_flock_error_returns_none_and_closes_the_file(tmp_path, monkeypatch):
    take_own_lock(str(tmp_path)).release()
    closed: list[int] = []
    real_close = os.close

    def failing_flock(fd: int, operation: int) -> None:
        raise OSError('flock not supported')

    def recording_close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    monkeypatch.setattr(ownerlock.fcntl, 'flock', failing_flock)
    monkeypatch.setattr(ownerlock.os, 'close', recording_close)

    assert owner_lock_held(str(tmp_path)) is None
    assert len(closed) == 1


def test_take_own_lock_flock_error_closes_the_file_and_raises(tmp_path, monkeypatch):
    opened: list[int] = []
    real_open = os.open

    def tracking_open(*args, **kwargs):
        fd = real_open(*args, **kwargs)
        opened.append(fd)
        return fd

    def failing_flock(fd, operation):
        raise OSError('no locks on this filesystem')

    monkeypatch.setattr(ownerlock.os, 'open', tracking_open)
    monkeypatch.setattr(ownerlock.fcntl, 'flock', failing_flock)

    with pytest.raises(OSError, match='no locks'):
        take_own_lock(str(tmp_path))

    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_take_own_lock_symlink_in_place_of_the_lock_file_is_refused(tmp_path):
    target = tmp_path / 'elsewhere'
    target.write_text('')
    (tmp_path / OWNER_LOCK_NAME).symlink_to(target)

    with pytest.raises(OSError):
        take_own_lock(str(tmp_path))


# --- the start sweep ---


def test_remove_download_temps_deletes_only_this_workers_download_temps(tmp_path):
    own = [
        '.w-1-2026-11-08__10_00_00.db.0123456789abcdef.download',
        '.w-1-2026-11-08__10_00_00.db.0123456789abcdef.download-journal',
        '.w-1-memory-2026-11-08__10_00_00.db.fedcba9876543210.download',
        '.w-1-memory-2026-11-08__10_00_00.db.fedcba9876543210.download-wal',
        '.w-1-memory-2026-11-08__10_00_00.db.fedcba9876543210.download-shm',
    ]
    kept = [
        OWNER_LOCK_NAME,
        'w-1-2026-11-08__10_00_00.db',
        'w-1-cache.db',
        'w-1.watchdog',
        '.w-10-2026-11-08__10_00_00.db.0123456789abcdef.download',
        '.w-1-2026-11-08__10_00_00.db.0123456789abcdef.download.other',
    ]
    for name in own + kept:
        (tmp_path / name).write_text('')

    removed = remove_download_temps(str(tmp_path), 'w-1')

    assert removed == len(own)
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted(kept)


def test_open_own_folder_sweep_failure_releases_the_lock(tmp_path, monkeypatch):
    def failing_sweep(folder, worker):
        raise PermissionError('read-only folder')

    monkeypatch.setattr(ownerlock, 'remove_download_temps', failing_sweep)
    own = tmp_path / 'storage' / 'default' / 'w-1'

    with pytest.raises(PermissionError):
        open_own_folder(str(tmp_path / 'storage'), str(own), 'w-1')

    take_own_lock(str(own)).release()


# --- the lifecycle ---


async def test_prepare_storage_second_worker_with_the_same_name_is_refused(tmp_path, short_lock_wait):
    config = _config(tmp_path, cluster={'name': 'search'})
    first = DrakkarApp(handler=SimpleHandler(), config=config, worker_id='w-1')
    second = DrakkarApp(handler=SimpleHandler(), config=config.model_copy(deep=True), worker_id='w-1')
    await first._lifecycle._prepare_storage()
    try:
        with pytest.raises(FolderInUseError, match='is in use by another process'):
            await second._lifecycle._prepare_storage()
    finally:
        await first._lifecycle._release_owner_lock()

    await second._lifecycle._prepare_storage()
    await second._lifecycle._release_owner_lock()


async def test_prepare_storage_deletes_own_download_temps_left_by_an_earlier_process(tmp_path):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='w-1')
    own = Path(app.own_storage_dir)
    own.mkdir(parents=True)
    temp = own / '.w-1-2026-11-08__10_00_00.db.0123456789abcdef.download'
    temp.write_text('')
    recorder_file = own / 'w-1-2026-11-08__10_00_00.db'
    recorder_file.write_text('')

    await app._lifecycle._prepare_storage()
    try:
        assert not temp.exists()
        assert recorder_file.exists()
    finally:
        await app._lifecycle._release_owner_lock()


async def test_setup_watchdog_writes_into_the_own_folder(tmp_path):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='w-1')

    await app._lifecycle._setup_watchdog()

    assert app._lifecycle._watchdog is not None
    assert app._lifecycle._watchdog.path == Path(app.own_storage_dir) / 'w-1.watchdog'


async def test_shutdown_releases_the_owner_lock(tmp_path):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='w-1')
    await app._lifecycle._prepare_storage()

    await app._lifecycle._shutdown()

    assert app._lifecycle._owner_lock is None
    take_own_lock(app.own_storage_dir).release()


def test_owner_lock_survives_while_held_by_another_open_file(tmp_path, short_lock_wait):
    """A lock held through any open file of the folder's lock refuses the owner lock."""
    take_own_lock(str(tmp_path)).release()
    fd = os.open(tmp_path / OWNER_LOCK_NAME, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(FolderInUseError):
            take_own_lock(str(tmp_path))
    finally:
        os.close(fd)


# --- the worker name ---


@pytest.mark.parametrize('name', ['w', 'worker-1', 'pod_7f9-abc', 'A' * 128], ids=['short', 'dash', 'mixed', 'max'])
def test_is_valid_worker_name_safe_names_are_accepted(name):
    assert is_valid_worker_name(name)
    check_worker_name(name)


@pytest.mark.parametrize(
    ('name', 'suggestion'),
    [
        ('host.example.com', 'host_example_com'),
        ('a/b', 'a_b'),
        ('w 1', 'w_1'),
        ('ünïcode', '_n_code'),
        ('A' * 129, 'A' * 128),
    ],
    ids=['dots', 'slash', 'space', 'unicode', 'too-long'],
)
def test_check_worker_name_unsafe_name_raises_with_the_safe_form(name, suggestion):
    with pytest.raises(ValueError) as refused:
        check_worker_name(name)

    assert str(refused.value) == (
        f"worker.name: '{name}' must be 1 to 128 letters, digits, '_' or '-'; use '{suggestion}'"
    )


@pytest.mark.parametrize(
    ('host', 'expected'),
    [('pod-7f9', 'pod-7f9'), ('host.example.com', 'host_example_com'), ('h' * 200, 'h' * 128)],
    ids=['pod-name', 'fqdn', 'long'],
)
def test_generated_worker_name_host_name_made_safe(monkeypatch, host, expected):
    monkeypatch.setattr('socket.gethostname', lambda: host)

    assert generated_worker_name() == expected


@pytest.mark.parametrize(
    'host',
    ['', 'localhost', 'LOCALHOST', 'localhost.localdomain', 'localhost6.localdomain6'],
    ids=['empty', 'localhost', 'upper', 'localdomain', 'localhost6'],
)
def test_generated_worker_name_unspecific_host_gives_a_random_name(monkeypatch, host):
    monkeypatch.setattr('socket.gethostname', lambda: host)

    name = generated_worker_name()

    assert name.startswith('drakkar-')
    assert len(name) == len('drakkar-') + 8
    assert is_valid_worker_name(name)


def test_app_worker_name_from_the_environment_is_checked_and_names_the_variable(tmp_path, monkeypatch):
    monkeypatch.setenv('WORKER_ID', 'node.example')

    with pytest.raises(ValueError) as refused:
        DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path))

    assert str(refused.value) == (
        "the variable WORKER_ID: 'node.example' must be 1 to 128 letters, digits, '_' or '-'; use 'node_example'"
    )


def test_app_worker_name_from_the_argument_is_checked_and_names_the_argument(tmp_path):
    with pytest.raises(ValueError, match=r"^the worker_id argument: 'a/b'"):
        DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='a/b')


def test_app_worker_name_from_the_config_is_checked(tmp_path, monkeypatch):
    monkeypatch.delenv('WORKER_ID', raising=False)

    with pytest.raises(ValueError, match=r"^worker\.name: 'a\.b'"):
        DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path, worker={'name': 'a.b'}))


# --- the cluster name ---


@pytest.mark.parametrize('name', ['history', 'merged', 'tasks', 'fleet', 'workers', 'HISTORY', 'Fleet'])
def test_check_cluster_name_reserved_folder_name_is_refused(name):
    with pytest.raises(ValueError) as refused:
        check_cluster_name(name)

    assert str(refused.value) == (
        f"cluster.name: '{name}' gives the folder name '{name}', which is reserved "
        '(fleet, history, merged, tasks, workers); choose another cluster name'
    )


@pytest.mark.parametrize('name', ['', 'default', 'search', 'history-hot', 'my.tasks'])
def test_check_cluster_name_other_names_are_accepted(name):
    check_cluster_name(name)


def test_cluster_config_reserved_name_from_the_variable_fails_the_load(tmp_path, monkeypatch):
    monkeypatch.setenv('MY_CLUSTER', 'merged')

    with pytest.raises(ValueError, match=r"cluster\.name: 'merged' gives the folder name 'merged'"):
        _config(tmp_path, cluster={'name_env': 'MY_CLUSTER'})


# --- folders another user could change ---


@pytest.mark.parametrize('level', ['top', 'cluster', 'own'])
async def test_prepare_storage_world_writable_level_without_sticky_bit_warns(tmp_path, level):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='w-1')
    own = Path(app.own_storage_dir)
    own.mkdir(parents=True)
    folder = {'top': own.parent.parent, 'cluster': own.parent, 'own': own}[level]
    folder.chmod(0o777)

    with capture_logs() as logs:
        await app._lifecycle._prepare_storage()
    await app._lifecycle._release_owner_lock()

    (unsafe,) = [entry for entry in logs if entry['event'] == 'storage_folder_unsafe']
    assert unsafe['log_level'] == 'warning'
    assert (unsafe['folder'], unsafe['reason']) == (str(folder), 'world_writable')


async def test_prepare_storage_sticky_world_writable_top_folder_is_silent(tmp_path):
    app = DrakkarApp(handler=SimpleHandler(), config=_config(tmp_path), worker_id='w-1')
    top = tmp_path / 'storage'
    top.mkdir()
    top.chmod(0o1777)

    with capture_logs() as logs:
        await app._lifecycle._prepare_storage()
    await app._lifecycle._release_owner_lock()

    assert not [entry for entry in logs if entry['event'] == 'storage_folder_unsafe']


def test_unsafe_folder_levels_folder_of_another_owner_is_named(tmp_path, monkeypatch):
    own = tmp_path / 'storage' / 'default' / 'w-1'
    own.mkdir(parents=True, mode=0o700)
    monkeypatch.setattr(ownerlock.os, 'geteuid', lambda: os.getuid() + 1)

    levels = ownerlock.unsafe_folder_levels(str(tmp_path / 'storage'), str(own))

    assert levels == [
        (str(tmp_path / 'storage'), 'other_owner'),
        (str(own.parent), 'other_owner'),
        (str(own), 'other_owner'),
    ]


def test_unsafe_folder_levels_missing_level_is_skipped(tmp_path):
    assert ownerlock.unsafe_folder_levels(str(tmp_path / 'gone'), str(tmp_path / 'gone' / 'c' / 'w')) == []


async def test_start_cache_writes_the_cache_file_into_the_own_folder(tmp_path):
    app = DrakkarApp(
        handler=SimpleHandler(),
        config=_config(tmp_path, cluster={'name': 'search'}, cache={'enabled': True}),
        worker_id='w-1',
    )

    await app._lifecycle._start_cache()
    try:
        own = tmp_path / 'storage' / 'search' / 'w-1'
        assert app._cache_engine._resolve_db_dir() == str(own)
        assert (own / 'w-1-cache.db').is_file()
        assert not (tmp_path / 'storage' / 'w-1-cache.db').exists()
    finally:
        await app._cache_engine.stop()
