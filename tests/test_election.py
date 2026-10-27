"""The election: the provider kit, the filesystem provider and the lease refresh interval.

The kit runs over the built-in filesystem provider (leases age by setting the
lock files' mtime back) and over an in-memory provider on a fake clock.
"""

import asyncio
import dataclasses
import json
import os
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config.resilience import ResilienceConfig
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import election as election_mod
from drakkar.recorder.election import (
    BUILTIN_ELECTIONS,
    ElectionContext,
    FilesystemElection,
    lease_loss_reason,
    lease_refresh_seconds,
)
from drakkar.recorder.history import task_lock_dir
from drakkar.recorder.task_lock import LOCK_SUFFIX, STALE_INFIX
from drakkar.schedule import SCOPE_INVALID, task_scope
from tests import election_testkit as electiontest
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection

CLUSTER = 'search-fleet'
TASK = 'history.archive'
SCOPE = task_scope(CLUSTER, TASK)
LOCK_FILE = TASK + LOCK_SUFFIX
LEASE_SECONDS = 600.0


async def _no_sleep(seconds: float) -> None:
    return None


def _calls(provider: str, op: str, outcome: str) -> float:
    labels = {'feature': 'election', 'provider': provider, 'op': op, 'outcome': outcome}
    return REGISTRY.get_sample_value('drakkar_provider_calls_total', labels) or 0.0


def _caller(provider: str, **overrides) -> ResilientCaller:
    return ResilientCaller(
        ResilienceConfig(**overrides),
        feature='election',
        provider=provider,
        category='periodic',
        sleep=_no_sleep,
    )


# --- the kit, over both providers ---


class _FolderElection(FilesystemElection):
    """The filesystem provider started on a test folder: the kit's context carries no shared folder."""

    def __init__(self, shared_dir: Path) -> None:
        super().__init__()
        self._shared_dir = str(shared_dir)

    async def start(self, ctx: ElectionContext) -> None:
        await super().start(dataclasses.replace(ctx, shared_dir=self._shared_dir))


def _age_locks(shared_dir: Path, seconds: float) -> None:
    """Make every lock file under ``shared_dir`` look ``seconds`` older on the file server's clock."""
    for lock in shared_dir.glob(f'tasks/*/*{LOCK_SUFFIX}'):
        info = lock.stat()
        os.utime(lock, (info.st_atime, info.st_mtime - seconds))


@pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS, ids=lambda check: check.__name__)
async def test_the_filesystem_election_passes_the_kit(check, tmp_path):
    await check(lambda: _FolderElection(tmp_path), lambda seconds: _age_locks(tmp_path, seconds), LEASE_SECONDS)


@pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS, ids=lambda check: check.__name__)
async def test_the_memory_election_passes_the_kit(check):
    store = ElectionStore()
    await check(lambda: MemoryElection(store), store.advance, LEASE_SECONDS)


async def test_the_kit_takes_a_coroutine_advance():
    store = ElectionStore()

    async def advance(seconds: float) -> None:
        store.advance(seconds)

    await electiontest.check_refresh_keeps_the_lease(lambda: MemoryElection(store), advance, LEASE_SECONDS)


class _EveryoneHolds(MemoryElection):
    """A broken provider: every acquire succeeds, whoever holds the scope."""

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self._store.leases[scope] = (token, self._store.now + lease_seconds, lease_seconds)
        return True


class _ForgetfulElection(MemoryElection):
    """A broken provider: an acquire is refused while anyone holds the scope, its own holder included."""

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        if self._store.holder(scope) is not None:
            return False
        return await super().acquire(scope, token, lease_seconds)


async def test_the_kit_fails_a_provider_whose_acquire_locks_its_own_holder_out():
    store = ElectionStore()

    with pytest.raises(AssertionError, match='the holding token could not acquire its scope again'):
        await electiontest.check_acquire_is_idempotent_for_the_holder(
            lambda: _ForgetfulElection(store), store.advance, LEASE_SECONDS
        )


async def test_the_kit_fails_a_provider_that_lets_two_holders_in():
    store = ElectionStore()

    with pytest.raises(AssertionError, match='a second token acquired a held scope'):
        await electiontest.check_one_holder_at_a_time(lambda: _EveryoneHolds(store), store.advance, LEASE_SECONDS)


class _AnswersAfterStop(MemoryElection):
    """A broken provider: its fencing check and its start still work after stop."""

    async def is_held(self, scope: str, token: str) -> bool:
        return self._store.holder(scope) == token

    async def start(self, ctx: ElectionContext) -> None:
        self.started_with = ctx


async def test_the_kit_fails_a_provider_that_answers_calls_after_stop():
    store = ElectionStore()

    with pytest.raises(AssertionError, match='a call after stop did not fail: is_held, start'):
        await electiontest.check_calls_after_stop_are_refused(
            lambda: _AnswersAfterStop(store), store.advance, LEASE_SECONDS
        )


# --- the filesystem provider ---


async def _started_filesystem(tmp_path: Path, worker: str = 'w-1') -> FilesystemElection:
    provider = FilesystemElection()
    await provider.start(electiontest.make_context(worker, shared_dir=str(tmp_path)))
    return provider


async def test_the_filesystem_election_writes_the_lock_body_in_the_task_lock_file(tmp_path):
    provider = await _started_filesystem(tmp_path, worker='search-worker-3')

    assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS) is True

    path = Path(task_lock_dir(str(tmp_path), CLUSTER)) / LOCK_FILE
    body = json.loads(path.read_bytes())
    assert (body['worker'], body['instance'], body['token'], body['provider']) == (
        'search-worker-3',
        electiontest.INSTANCE_A,
        electiontest.TOKEN_A,
        'filesystem',
    )
    await provider.release(SCOPE, electiontest.TOKEN_A)
    assert not path.exists()


async def test_filesystem_election_puts_the_lock_in_the_tasks_folder(tmp_path):
    provider = await _started_filesystem(tmp_path)
    scope = task_scope('main cluster', 'history.archive')

    assert await provider.acquire(scope, electiontest.TOKEN_A, LEASE_SECONDS) is True

    folder = tmp_path / 'tasks' / 'main_cluster'
    assert sorted(p.name for p in folder.iterdir()) == ['history.archive.lock']
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    assert not (tmp_path / 'history').exists()
    await provider.release(scope, electiontest.TOKEN_A)
    await provider.stop()


@pytest.mark.parametrize('call', ['acquire', 'refresh', 'is_held', 'release'])
async def test_filesystem_election_refuses_a_scope_without_a_task(tmp_path, call):
    provider = await _started_filesystem(tmp_path)
    args = ('main', electiontest.TOKEN_A, LEASE_SECONDS) if call == 'acquire' else ('main', electiontest.TOKEN_A)

    with pytest.raises(ValueError) as raised:
        await getattr(provider, call)(*args)

    assert str(raised.value) == SCOPE_INVALID.format(scope='main')
    assert list(tmp_path.iterdir()) == []
    await provider.stop()


async def test_filesystem_takeover_logs_periodic_lock_taken_over_with_scope(tmp_path):
    holder = await _started_filesystem(tmp_path, worker='w-dead')
    taker = FilesystemElection()
    await taker.start(electiontest.make_context('w-2', electiontest.INSTANCE_B, shared_dir=str(tmp_path)))
    assert await holder.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS)
    _age_locks(tmp_path, LEASE_SECONDS + 60)

    with capture_logs() as logs:
        assert await taker.acquire(SCOPE, electiontest.TOKEN_B, LEASE_SECONDS) is True

    (event,) = [entry for entry in logs if entry['event'] == 'periodic_lock_taken_over']
    assert (event['category'], event['provider'], event['scope']) == ('periodic', 'filesystem', SCOPE)
    assert (event['previous_worker'], event['previous_instance']) == ('w-dead', electiontest.INSTANCE_A)
    assert event['age_seconds'] > LEASE_SECONDS
    folder = Path(task_lock_dir(str(tmp_path), CLUSTER))
    assert not [p for p in folder.iterdir() if STALE_INFIX in p.name]
    await taker.release(SCOPE, electiontest.TOKEN_B)
    await holder.stop()
    await taker.stop()


async def test_two_tasks_of_one_cluster_hold_independent_locks(tmp_path):
    first = await _started_filesystem(tmp_path, worker='w-1')
    second = FilesystemElection()
    await second.start(electiontest.make_context('w-2', electiontest.INSTANCE_B, shared_dir=str(tmp_path)))
    refresh_rates = task_scope(CLUSTER, 'refresh_rates')

    assert await first.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS) is True
    assert await second.acquire(refresh_rates, electiontest.TOKEN_B, LEASE_SECONDS) is True
    assert await second.acquire(SCOPE, electiontest.TOKEN_B, LEASE_SECONDS) is False

    folder = Path(task_lock_dir(str(tmp_path), CLUSTER))
    assert sorted(p.name for p in folder.iterdir()) == ['history.archive.lock', 'refresh_rates.lock']
    await first.release(SCOPE, electiontest.TOKEN_A)
    assert await second.is_held(refresh_rates, electiontest.TOKEN_B) is True
    await second.release(refresh_rates, electiontest.TOKEN_B)
    await first.stop()
    await second.stop()


async def test_the_filesystem_election_acquires_again_for_the_holding_token(tmp_path):
    # An acquire whose answer was lost (a timed-out attempt whose thread still
    # created the file) is retried with the same token.
    provider = await _started_filesystem(tmp_path)
    assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS)

    assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS) is True
    await provider.release(SCOPE, electiontest.TOKEN_A)


async def test_the_filesystem_election_takes_over_after_its_own_stale_age(tmp_path):
    holder = await _started_filesystem(tmp_path)
    taker = FilesystemElection(stale_seconds=10.0)
    await taker.start(electiontest.make_context('w-2', electiontest.INSTANCE_B, shared_dir=str(tmp_path)))
    assert await holder.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS)
    _age_locks(tmp_path, 60)

    assert await taker.acquire(SCOPE, electiontest.TOKEN_B, LEASE_SECONDS) is True
    await taker.release(SCOPE, electiontest.TOKEN_B)


async def test_the_filesystem_election_refuses_calls_before_start():
    with pytest.raises(RuntimeError, match='not started'):
        await FilesystemElection().is_held(SCOPE, electiontest.TOKEN_A)


async def test_the_filesystem_election_stop_releases_nothing(tmp_path):
    provider = await _started_filesystem(tmp_path)
    assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS)

    await provider.stop()

    assert (Path(task_lock_dir(str(tmp_path), CLUSTER)) / LOCK_FILE).exists()


def test_the_builtin_filesystem_factory_makes_a_filesystem_election():
    assert isinstance(BUILTIN_ELECTIONS['filesystem']({}), FilesystemElection)


async def test_the_filesystem_election_runs_its_file_calls_on_its_own_thread(tmp_path, monkeypatch):
    provider = await _started_filesystem(tmp_path, worker='search worker/3')
    seen: list[str] = []
    real_acquire = election_mod._acquire_in_folder

    def recording_acquire(lock, folder) -> bool:
        seen.append(threading.current_thread().name)
        return real_acquire(lock, folder)

    monkeypatch.setattr(election_mod, '_acquire_in_folder', recording_acquire)
    try:
        assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS)
        await provider.release(SCOPE, electiontest.TOKEN_A)
    finally:
        await provider.stop()

    assert seen == ['drakkar-election-files-search_worker_3']


async def test_a_hung_lock_file_call_holds_only_the_elections_own_thread(tmp_path, monkeypatch):
    # Every other to_thread user of the worker (sinks, the HTTP source, handler
    # code) shares the default executor; one thread there is the worst case.
    one_thread = ThreadPoolExecutor(max_workers=1)
    asyncio.get_running_loop().set_default_executor(one_thread)
    provider = await _started_filesystem(tmp_path)
    mount_answers = threading.Event()
    real_open = os.open

    def hanging_open(path, flags, *args, **kwargs):
        if os.path.basename(os.fspath(path)) == LOCK_FILE:
            mount_answers.wait(WAIT_FOR_TIMEOUT_SECONDS)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', hanging_open)
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS), timeout=0.1)

        assert await asyncio.wait_for(asyncio.to_thread(lambda: 'answered'), timeout=1.0) == 'answered'
    finally:
        mount_answers.set()
        await provider.stop()
        one_thread.shutdown(wait=True)


async def test_the_filesystem_election_keeps_one_thread_across_starts_and_ends_it_at_stop(tmp_path):
    # A stop before any start has nothing to end.
    await FilesystemElection().stop()

    def file_threads() -> set[threading.Thread]:
        return {thread for thread in threading.enumerate() if thread.name == 'drakkar-election-files-w-1'}

    before = file_threads()
    provider = await _started_filesystem(tmp_path)
    # A start retried after a lost answer must not leave a second thread behind.
    await provider.start(electiontest.make_context('w-1', shared_dir=str(tmp_path)))
    (thread,) = file_threads() - before

    await provider.stop()

    await wait_for(lambda: not thread.is_alive())


async def test_the_filesystem_election_refuses_calls_after_stop(tmp_path):
    provider = await _started_filesystem(tmp_path)
    await provider.stop()

    with pytest.raises(RuntimeError, match='stopped'):
        await provider.is_held(SCOPE, electiontest.TOKEN_A)


@pytest.mark.parametrize('started', [True, False], ids=['started', 'never-started'])
async def test_a_filesystem_election_start_after_stop_fails_and_starts_no_thread(tmp_path, started):
    provider = await _started_filesystem(tmp_path) if started else FilesystemElection()
    await provider.stop()
    threads_before = set(threading.enumerate())

    with pytest.raises(RuntimeError, match=r'^provider stopped$'):
        await provider.start(electiontest.make_context('w-1', shared_dir=str(tmp_path)))

    new_threads = set(threading.enumerate()) - threads_before
    assert not [thread for thread in new_threads if thread.name.startswith('drakkar-election-files-')]


async def test_a_lock_read_error_on_refresh_reads_as_unconfirmed(tmp_path, monkeypatch):
    provider = await _started_filesystem(tmp_path)
    caller = _caller('filesystem-io-error', retries=0)
    token = '0123456789abcdef'
    assert await provider.acquire(SCOPE, token, LEASE_SECONDS)
    real_open = os.open
    lock_path = os.path.join(task_lock_dir(str(tmp_path), CLUSTER), LOCK_FILE)

    def open_with_io_error(path, flags, *args, **kwargs):
        if os.fspath(path) == lock_path:
            raise OSError(5, 'Input/output error', lock_path)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', open_with_io_error)
    try:
        result = await caller.call('refresh', lambda: provider.refresh(SCOPE, token))
    finally:
        monkeypatch.undo()
        await provider.stop()

    assert lease_loss_reason(result) == 'unconfirmed'
    # Unconfirmed means it may still be held: the lock file stays for its holder to release.
    assert Path(lock_path).exists()


# --- the refresh interval ---


@pytest.mark.parametrize(
    ('lease_seconds', 'expected'),
    [(7200, 60.0), (180, 60.0), (90, 30.0), (30, 10.0), (10, 10 / 3)],
)
def test_lease_refresh_seconds_follows_short_leases(lease_seconds, expected):
    assert lease_refresh_seconds(lease_seconds) == pytest.approx(expected)
