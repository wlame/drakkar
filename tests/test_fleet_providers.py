"""Built-in registry providers and the provider test kit.

Every provider runs the kit checks it can pass: the shape and age checks for
all of them, the publish, withdraw and prune checks for the providers that
store cards. The filesystem provider then gets the tests of its own layout:
file names, modes, ages from file times, pruning and unreadable files.
"""

import asyncio
import json
import os
import stat
import threading
from pathlib import Path

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.fleet import testkit
from drakkar.fleet.card import serialize_card
from drakkar.fleet.provider import PeerCard, RegistryContext
from drakkar.fleet.providers import (
    BUILTIN_PROVIDERS,
    FilesystemRegistry,
    NoneRegistry,
    StaticRegistry,
    prepare_registry_folder,
)
from tests.fleet_fakes import MemoryRegistry, MemoryStore

PRUNE_AFTER = 600.0


def _card(worker: str, instance: str = 'a' * 16, **extra) -> dict:
    return {'schema': 1, 'worker': worker, 'cluster': '', 'url': 'http://10.0.0.5:8080/', 'instance': instance, **extra}


def _context(worker: str = 'w-1', instance: str = 'a' * 16) -> RegistryContext:
    return RegistryContext(worker=worker, instance=instance, cluster='', logger=structlog.get_logger(), options={})


def _shift_card_times(folder: Path, seconds: float) -> None:
    """Make every file in ``folder`` older by ``seconds``, as if time had passed on the file server."""
    for path in folder.iterdir():
        info = path.stat(follow_symlinks=False)
        os.utime(path, (info.st_atime - seconds, info.st_mtime - seconds), follow_symlinks=False)


@pytest.fixture
def registry_dir(tmp_path) -> Path:
    return tmp_path / 'fleet'


@pytest.fixture
def make_filesystem(registry_dir):
    def make() -> FilesystemRegistry:
        return FilesystemRegistry(directory=str(registry_dir), prune_after_seconds=PRUNE_AFTER)

    return make


# --- The kit against every provider ---


async def test_kit_shape_and_age_checks_pass_for_none():
    await testkit.check_list_shape(NoneRegistry(), _context())
    await testkit.check_age_is_none_or_non_negative(NoneRegistry(), _context())


async def test_kit_shape_and_age_checks_pass_for_static():
    peers = [{'name': 'w-2', 'cluster': 'c', 'url': 'http://10.0.0.6:8080/'}]

    await testkit.check_list_shape(StaticRegistry(peers), _context())
    await testkit.check_age_is_none_or_non_negative(StaticRegistry(peers), _context())


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS, ids=lambda check: check.__name__)
async def test_kit_publishing_checks_pass_for_filesystem(check, make_filesystem, registry_dir):
    await check(make_filesystem, lambda seconds: _shift_card_times(registry_dir / 'workers', seconds), PRUNE_AFTER)


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS, ids=lambda check: check.__name__)
async def test_kit_publishing_checks_pass_for_an_in_memory_provider(check):
    store = MemoryStore()
    await check(lambda: MemoryRegistry(store, prune_after_seconds=PRUNE_AFTER), store.advance, PRUNE_AFTER)


async def test_kit_reports_a_provider_that_does_not_list_its_own_card():
    """A kit check fails loudly; it does not pass a provider that ignores publish."""
    with pytest.raises(AssertionError, match='own card'):
        await testkit.check_list_includes_own_card(NoneRegistry, lambda _seconds: None, PRUNE_AFTER)


# --- none and static ---


async def test_none_registry_lists_nothing_and_ignores_writes():
    registry = NoneRegistry()
    await registry.start(_context())
    await registry.publish(_card('w-1'))
    await registry.withdraw('w-1', 'a' * 16)

    assert await registry.list() == []
    await registry.stop()


async def test_static_registry_lists_the_configured_peers_with_unknown_age():
    registry = StaticRegistry(
        [
            {'name': 'w-2', 'cluster': 'search', 'url': 'http://10.0.0.6:8080/'},
            {'name': 'w-3', 'cluster': '', 'url': 'http://10.0.0.7:8080/'},
        ]
    )
    await registry.start(_context())
    await registry.publish(_card('w-1'))

    listed = await registry.list()

    assert listed == [
        PeerCard(
            card={'schema': 1, 'worker': 'w-2', 'cluster': 'search', 'url': 'http://10.0.0.6:8080/'}, age_seconds=None
        ),
        PeerCard(card={'schema': 1, 'worker': 'w-3', 'cluster': '', 'url': 'http://10.0.0.7:8080/'}, age_seconds=None),
    ]


async def test_static_registry_hands_out_copies():
    registry = StaticRegistry([{'name': 'w-2', 'cluster': '', 'url': 'http://10.0.0.6:8080/'}])
    (first,) = await registry.list()
    first.card['worker'] = 'changed'

    (second,) = await registry.list()

    assert second.card['worker'] == 'w-2'


def test_builtin_provider_table_names():
    assert sorted(BUILTIN_PROVIDERS) == ['filesystem', 'none', 'static']


def test_builtin_factories_build_their_providers(tmp_path):
    assert isinstance(BUILTIN_PROVIDERS['none']({}), NoneRegistry)
    assert isinstance(BUILTIN_PROVIDERS['static']({'peers': []}), StaticRegistry)
    filesystem = BUILTIN_PROVIDERS['filesystem']({'dir': str(tmp_path), 'prune_after_seconds': 60})
    assert isinstance(filesystem, FilesystemRegistry)


def test_filesystem_factory_needs_a_folder():
    with pytest.raises(ValueError, match='folder'):
        BUILTIN_PROVIDERS['filesystem']({'dir': '', 'prune_after_seconds': 60})


# --- filesystem: layout and modes ---


async def test_filesystem_publish_writes_the_card_bytes_under_the_sanitized_name(make_filesystem, registry_dir):
    registry = make_filesystem()
    await registry.start(_context('search.worker/3'))
    card = _card('search.worker/3')

    await registry.publish(card)

    assert (registry_dir / 'workers' / 'search_worker_3.json').read_bytes() == serialize_card(card)
    await registry.stop()


async def test_filesystem_publish_leaves_no_temp_file(make_filesystem, registry_dir):
    registry = make_filesystem()
    await registry.start(_context())

    await registry.publish(_card('w-1'))
    await registry.publish(_card('w-1', state='running'))

    assert sorted(path.name for path in (registry_dir / 'workers').iterdir()) == ['w-1.json']
    await registry.stop()


async def test_filesystem_creates_an_owner_only_folder_and_card(make_filesystem, registry_dir):
    registry = make_filesystem()
    await registry.start(_context())

    await registry.publish(_card('w-1'))

    assert stat.S_IMODE((registry_dir / 'workers').stat().st_mode) == 0o700
    assert stat.S_IMODE((registry_dir / 'workers' / 'w-1.json').stat().st_mode) == 0o600
    await registry.stop()


async def test_filesystem_start_does_not_create_the_folder(make_filesystem, registry_dir):
    """Folder trouble shows up as publish errors the service retries, never as a startup failure."""
    registry = make_filesystem()

    await registry.start(_context())

    assert not registry_dir.exists()
    await registry.stop()


async def test_filesystem_publish_raises_when_the_folder_cannot_be_written(tmp_path):
    blocker = tmp_path / 'not-a-folder'
    blocker.write_text('x')
    registry = FilesystemRegistry(directory=str(blocker), prune_after_seconds=PRUNE_AFTER)
    await registry.start(_context())

    with pytest.raises(OSError):
        await registry.publish(_card('w-1'))
    await registry.stop()


async def test_filesystem_list_of_a_missing_folder_is_empty(make_filesystem):
    registry = make_filesystem()
    await registry.start(_context())

    assert await registry.list() == []
    await registry.stop()


# --- filesystem: ages on the file server clock ---


async def test_filesystem_age_is_unknown_before_the_first_publish(make_filesystem):
    peer = make_filesystem()
    await peer.start(_context('w-2'))
    await peer.publish(_card('w-2'))
    reader = make_filesystem()
    await reader.start(_context('w-1'))

    (listed,) = await reader.list()

    assert listed.card['worker'] == 'w-2'
    assert listed.age_seconds is None
    await peer.stop()
    await reader.stop()


async def test_filesystem_age_counts_from_the_card_file_time(make_filesystem, registry_dir):
    reader = make_filesystem()
    await reader.start(_context('w-1'))
    await reader.publish(_card('w-1'))
    peer_card = registry_dir / 'workers' / 'w-2.json'
    peer_card.write_bytes(serialize_card(_card('w-2')))
    own_mtime = (registry_dir / 'workers' / 'w-1.json').stat().st_mtime
    os.utime(peer_card, (own_mtime - 42, own_mtime - 42))

    ages = {entry.card['worker']: entry.age_seconds for entry in await reader.list()}

    assert ages['w-1'] == pytest.approx(0.0, abs=0.5)
    assert ages['w-2'] == pytest.approx(42.0, abs=0.5)
    await reader.stop()


async def test_filesystem_age_grows_with_the_monotonic_clock_between_publishes(registry_dir):
    clock = [500.0]
    registry = FilesystemRegistry(
        directory=str(registry_dir), prune_after_seconds=PRUNE_AFTER, now_monotonic=lambda: clock[0]
    )
    await registry.start(_context())
    await registry.publish(_card('w-1'))

    clock[0] += 25
    (listed,) = await registry.list()

    assert listed.age_seconds == pytest.approx(25.0, abs=0.01)
    await registry.stop()


async def test_filesystem_age_never_goes_negative(make_filesystem, registry_dir):
    """A peer card written after this worker's sample (or a coarse server clock) reads as age 0."""
    reader = make_filesystem()
    await reader.start(_context('w-1'))
    await reader.publish(_card('w-1'))
    peer_card = registry_dir / 'workers' / 'w-2.json'
    peer_card.write_bytes(serialize_card(_card('w-2')))
    own_mtime = (registry_dir / 'workers' / 'w-1.json').stat().st_mtime
    os.utime(peer_card, (own_mtime + 30, own_mtime + 30))

    ages = {entry.card['worker']: entry.age_seconds for entry in await reader.list()}

    assert ages['w-2'] == 0.0
    await reader.stop()


# --- filesystem: pruning ---


async def test_filesystem_lists_an_offline_card_until_it_is_old_enough_to_prune(make_filesystem, registry_dir):
    reader = make_filesystem()
    await reader.start(_context('w-1'))
    await reader.publish(_card('w-1'))
    peer_card = registry_dir / 'workers' / 'w-2.json'
    peer_card.write_bytes(serialize_card(_card('w-2')))
    own_mtime = (registry_dir / 'workers' / 'w-1.json').stat().st_mtime

    os.utime(peer_card, (own_mtime - 120, own_mtime - 120))
    listed_while_offline = [entry.card['worker'] for entry in await reader.list()]
    os.utime(peer_card, (own_mtime - PRUNE_AFTER - 1, own_mtime - PRUNE_AFTER - 1))
    listed_after_prune = [entry.card['worker'] for entry in await reader.list()]

    assert listed_while_offline == ['w-1', 'w-2']
    assert listed_after_prune == ['w-1']
    assert not peer_card.exists()
    await reader.stop()


async def test_filesystem_never_prunes_while_ages_are_unknown(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    old_card = folder / 'w-2.json'
    old_card.write_bytes(serialize_card(_card('w-2')))
    os.utime(old_card, (1, 1))
    reader = make_filesystem()
    await reader.start(_context('w-1'))

    listed = await reader.list()

    assert [entry.card['worker'] for entry in listed] == ['w-2']
    assert old_card.exists()
    await reader.stop()


async def test_filesystem_prunes_a_stale_temp_file_and_keeps_a_young_one(make_filesystem, registry_dir):
    reader = make_filesystem()
    await reader.start(_context('w-1'))
    await reader.publish(_card('w-1'))
    folder = registry_dir / 'workers'
    own_mtime = (folder / 'w-1.json').stat().st_mtime
    stale = folder / '.w-9.json.0123456789abcdef.tmp'
    young = folder / '.w-8.json.fedcba9876543210.tmp'
    for path, age in ((stale, PRUNE_AFTER + 1), (young, 5)):
        path.write_bytes(b'{')
        os.utime(path, (own_mtime - age, own_mtime - age))

    await reader.list()

    assert not stale.exists()
    assert young.exists()
    await reader.stop()


# --- filesystem: unreadable files ---


async def test_filesystem_skips_unreadable_cards_and_lists_the_rest(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    (folder / 'w-2.json').write_bytes(serialize_card(_card('w-2')))
    (folder / 'truncated.json').write_bytes(b'{"schema":1,"wor')
    (folder / 'array.json').write_bytes(b'[1]')
    (folder / 'huge.json').write_bytes(b'{"pad":"' + b'x' * 5000 + b'"}')
    (folder / 'notes.txt').write_bytes(b'not a card')
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    with capture_logs() as first:
        listed = await registry.list()
    with capture_logs() as second:
        await registry.list()

    assert [entry.card['worker'] for entry in listed] == ['w-2']
    rejects = sorted((entry['file'], entry['reason']) for entry in first if entry['event'] == 'fleet_card_invalid')
    assert rejects == [('array.json', 'not_object'), ('huge.json', 'too_large'), ('truncated.json', 'not_json')]
    assert [entry for entry in second if entry['event'] == 'fleet_card_invalid'] == []
    await registry.stop()


async def test_filesystem_lists_a_card_of_another_schema_for_the_service_to_judge(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    (folder / 'w-2.json').write_bytes(json.dumps({'schema': 2, 'worker': 'w-2'}).encode())
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    (listed,) = await registry.list()

    assert listed.card == {'schema': 2, 'worker': 'w-2'}
    await registry.stop()


async def test_filesystem_skips_a_symlinked_card(make_filesystem, registry_dir, tmp_path):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    target = tmp_path / 'elsewhere.json'
    target.write_bytes(serialize_card(_card('w-2')))
    (folder / 'w-2.json').symlink_to(target)
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    assert await registry.list() == []
    await registry.stop()


async def test_filesystem_withdraw_of_a_missing_card_is_a_no_op(make_filesystem):
    registry = make_filesystem()
    await registry.start(_context())

    await registry.withdraw('w-1', 'a' * 16)
    await registry.stop()


# --- filesystem: its own thread ---


async def test_filesystem_a_stuck_call_times_out_and_later_calls_still_run(make_filesystem, monkeypatch):
    """A hung file call blocks only the provider's own thread, never the event loop or the shared pool."""
    registry = make_filesystem()
    await registry.start(_context())
    release = threading.Event()
    original = registry._list_sync

    def stuck_list():
        release.wait(5)
        return original()

    monkeypatch.setattr(registry, '_list_sync', stuck_list)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(registry.list(), timeout=0.05)
    monkeypatch.setattr(registry, '_list_sync', original)
    release.set()

    assert await asyncio.wait_for(registry.list(), timeout=2) == []
    await registry.stop()


async def test_filesystem_thread_is_a_daemon_so_a_hung_call_cannot_block_process_exit(make_filesystem):
    registry = make_filesystem()
    await registry.start(_context())
    await registry.list()

    assert registry._runner.thread.daemon
    await registry.stop()
    registry._runner.thread.join(timeout=2)
    assert not registry._runner.thread.is_alive()


# --- filesystem: error paths ---


async def test_filesystem_refuses_calls_before_start(make_filesystem):
    with pytest.raises(RuntimeError, match='before start'):
        await make_filesystem().list()


async def test_filesystem_stop_without_start_is_a_no_op(make_filesystem):
    await make_filesystem().stop()


async def test_filesystem_a_failed_rename_leaves_no_temp_file(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    (folder / 'w-1.json').mkdir(parents=True)
    registry = make_filesystem()
    await registry.start(_context())

    with pytest.raises(OSError):
        await registry.publish(_card('w-1'))

    assert sorted(path.name for path in folder.iterdir()) == ['w-1.json']
    await registry.stop()


async def test_filesystem_skips_a_folder_named_like_a_card(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    (folder / 'odd.json').mkdir(parents=True)
    (folder / 'w-2.json').write_bytes(serialize_card(_card('w-2')))
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    assert [entry.card['worker'] for entry in await registry.list()] == ['w-2']
    await registry.stop()


async def test_filesystem_keeps_temp_files_while_ages_are_unknown(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    stale = folder / '.w-9.json.0123456789abcdef.tmp'
    stale.write_bytes(b'{')
    os.utime(stale, (1, 1))
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    await registry.list()

    assert stale.exists()
    await registry.stop()


async def test_filesystem_skips_a_queued_call_whose_caller_gave_up(make_filesystem, monkeypatch):
    """A publish or list that timed out while waiting behind a stuck call never runs late."""
    registry = make_filesystem()
    await registry.start(_context())
    release = threading.Event()
    ran = []

    def stuck():
        release.wait(5)
        return [], []

    def counted():
        ran.append('list')
        return [], []

    monkeypatch.setattr(registry, '_list_sync', stuck)
    first = asyncio.ensure_future(registry.list())
    await asyncio.sleep(0.01)
    monkeypatch.setattr(registry, '_list_sync', counted)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(registry.list(), timeout=0.05)
    release.set()
    await first

    assert await asyncio.wait_for(registry.list(), timeout=2) == []
    assert ran == ['list']
    await registry.stop()


# --- review fixes: bounded queue, unreadable files, symlinked folder, write probe ---

needs_non_root = pytest.mark.skipif(os.geteuid() == 0, reason='root ignores file permissions')


async def test_filesystem_holds_at_most_one_call_while_a_call_hangs(make_filesystem, monkeypatch):
    """Timed-out calls behind a hung one never pile up in memory: there is one slot."""
    registry = make_filesystem()
    await registry.start(_context())
    release = threading.Event()
    original = registry._list_sync

    def stuck():
        release.wait(5)
        return original()

    monkeypatch.setattr(registry, '_list_sync', stuck)
    first = asyncio.ensure_future(registry.list())
    await asyncio.sleep(0.01)
    for _ in range(50):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(registry.list(), timeout=0.005)

    assert registry._runner._jobs.qsize() <= 1
    release.set()
    await first
    await registry.stop()


@needs_non_root
async def test_filesystem_logs_an_unreadable_card_once(make_filesystem, registry_dir):
    folder = registry_dir / 'workers'
    folder.mkdir(parents=True)
    locked = folder / 'w-2.json'
    locked.write_bytes(serialize_card(_card('w-2')))
    locked.chmod(0)
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    try:
        with capture_logs() as logs:
            await registry.list()
            await registry.list()
    finally:
        locked.chmod(0o600)
        await registry.stop()

    rejects = [(entry['file'], entry['reason']) for entry in logs if entry['event'] == 'fleet_card_invalid']
    assert rejects == [('w-2.json', 'unreadable')]


async def test_filesystem_logs_a_symlink_or_folder_named_like_a_card(make_filesystem, registry_dir, tmp_path):
    folder = registry_dir / 'workers'
    (folder / 'odd.json').mkdir(parents=True)
    target = tmp_path / 'elsewhere.json'
    target.write_bytes(serialize_card(_card('w-3')))
    (folder / 'w-3.json').symlink_to(target)
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    with capture_logs() as logs:
        assert await registry.list() == []
    await registry.stop()

    rejects = sorted((entry['file'], entry['reason']) for entry in logs if entry['event'] == 'fleet_card_invalid')
    assert rejects == [('odd.json', 'not_a_file'), ('w-3.json', 'not_a_file')]


async def test_filesystem_refuses_a_symlinked_workers_folder(make_filesystem, registry_dir, tmp_path):
    """Listing (and pruning) never follows a workers link into another folder."""
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.mkdir()
    old_file = elsewhere / 'precious.json'
    old_file.write_text('{}')
    os.utime(old_file, (1, 1))
    registry_dir.mkdir()
    (registry_dir / 'workers').symlink_to(elsewhere)
    registry = make_filesystem()
    await registry.start(_context('w-1'))

    with pytest.raises(OSError, match='not a folder'):
        await registry.publish(_card('w-1'))
    with pytest.raises(OSError, match='not a folder'):
        await registry.list()
    await registry.stop()

    assert old_file.exists()
    assert sorted(path.name for path in elsewhere.iterdir()) == ['precious.json']


@needs_non_root
def test_prepare_registry_folder_fails_on_a_folder_this_process_cannot_write(tmp_path):
    workers = tmp_path / 'fleet' / 'workers'
    workers.mkdir(parents=True)
    workers.chmod(0o500)
    try:
        with pytest.raises(OSError):
            prepare_registry_folder(str(tmp_path / 'fleet'))
    finally:
        workers.chmod(0o700)


def test_prepare_registry_folder_leaves_no_probe_file(tmp_path):
    prepare_registry_folder(str(tmp_path / 'fleet'))

    assert list((tmp_path / 'fleet' / 'workers').iterdir()) == []
