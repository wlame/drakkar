"""The archive election: the provider kit, the filesystem provider, the archive thread's lease, the lookup.

The kit runs over the built-in filesystem provider (leases age by setting the
lock files' mtime back) and over an in-memory provider on a fake clock. The
lease tests drive the synchronous lease from a worker thread, the way the
archive pass does, against the test's own event loop.
"""

import asyncio
import contextlib
import dataclasses
import json
import os
import re
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from prometheus_client import REGISTRY

from drakkar.config.resilience import ResilienceConfig
from drakkar.providers import lookup
from drakkar.providers.lookup import UnknownProviderError, resolve_factory
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import election as election_mod
from drakkar.recorder import electiontest
from drakkar.recorder.archive_lock import LOCK_NAME
from drakkar.recorder.election import (
    BUILTIN_ELECTIONS,
    ELECTION_ENTRY_POINT_GROUP,
    ElectionContext,
    ElectionLease,
    FilesystemElection,
)
from drakkar.recorder.history import history_dir
from tests.conftest import wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection

SCOPE = 'search-fleet'
LEASE_SECONDS = 600.0
REFRESH_THREAD = 'drakkar-election-refresh'


async def _no_sleep(seconds: float) -> None:
    return None


def _refresh_threads() -> set[threading.Thread]:
    """The live refresh threads; tests compare against the ones that ran before they started."""
    return {thread for thread in threading.enumerate() if thread.name == REFRESH_THREAD}


def _calls(provider: str, op: str, outcome: str) -> float:
    labels = {'feature': 'election', 'provider': provider, 'op': op, 'outcome': outcome}
    return REGISTRY.get_sample_value('drakkar_provider_calls_total', labels) or 0.0


def _caller(provider: str, **overrides) -> ResilientCaller:
    return ResilientCaller(
        ResilienceConfig(**overrides),
        feature='election',
        provider=provider,
        category='recorder',
        sleep=_no_sleep,
    )


def _lease(provider, caller: ResilientCaller, *, refresh_seconds: float = 60.0, **kwargs) -> ElectionLease:
    return ElectionLease(
        provider,
        caller,
        asyncio.get_running_loop(),
        scope=SCOPE,
        lease_seconds=LEASE_SECONDS,
        provider_name='memory',
        refresh_seconds=refresh_seconds,
        **kwargs,
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
    for lock in shared_dir.glob(f'history/*/{LOCK_NAME}'):
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


async def test_the_kit_fails_a_provider_that_lets_two_holders_in():
    store = ElectionStore()

    with pytest.raises(AssertionError, match='a second token acquired a held scope'):
        await electiontest.check_one_holder_at_a_time(lambda: _EveryoneHolds(store), store.advance, LEASE_SECONDS)


# --- the filesystem provider ---


async def _started_filesystem(tmp_path: Path, worker: str = 'w-1') -> FilesystemElection:
    provider = FilesystemElection()
    await provider.start(electiontest.make_context(worker, shared_dir=str(tmp_path)))
    return provider


async def test_the_filesystem_election_writes_the_lock_body_in_the_scope_folder(tmp_path):
    provider = await _started_filesystem(tmp_path, worker='search-worker-3')

    assert await provider.acquire(SCOPE, electiontest.TOKEN_A, LEASE_SECONDS) is True

    path = Path(history_dir(str(tmp_path), SCOPE)) / LOCK_NAME
    body = json.loads(path.read_bytes())
    assert (body['worker'], body['instance'], body['token'], body['provider']) == (
        'search-worker-3',
        electiontest.INSTANCE_A,
        electiontest.TOKEN_A,
        'filesystem',
    )
    await provider.release(SCOPE, electiontest.TOKEN_A)
    assert not path.exists()


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

    assert (Path(history_dir(str(tmp_path), SCOPE)) / LOCK_NAME).exists()


def test_the_builtin_filesystem_factory_makes_a_filesystem_election():
    assert isinstance(BUILTIN_ELECTIONS['filesystem']({}), FilesystemElection)


# --- the lease ---


async def test_a_lease_token_is_16_hex_unless_given():
    caller = _caller('memory')

    made = _lease(MemoryElection(ElectionStore()), caller)
    given = _lease(MemoryElection(ElectionStore()), caller, token='0123456789abcdef')

    assert re.fullmatch(r'[0-9a-f]{16}', made.token)
    assert given.token == '0123456789abcdef'
    assert made.provider_name == 'memory'


async def test_the_lease_holds_and_releases_through_the_provider():
    store = ElectionStore()
    lease = _lease(MemoryElection(store), _caller('memory'))

    assert await asyncio.to_thread(lease.acquire) is True
    try:
        assert store.holder(SCOPE) == lease.token
        assert await asyncio.to_thread(lease.is_held) is True
    finally:
        await asyncio.to_thread(lease.release)

    assert store.holder(SCOPE) is None


async def test_a_lease_refused_by_the_provider_starts_no_refresh_thread():
    store = ElectionStore()
    store.leases[SCOPE] = ('f' * 16, store.now + LEASE_SECONDS, LEASE_SECONDS)
    lease = _lease(MemoryElection(store), _caller('memory'))
    before = _refresh_threads()

    assert await asyncio.to_thread(lease.acquire) is False

    assert _refresh_threads() - before == set()


async def test_the_lease_refresh_thread_ends_on_release():
    store = ElectionStore()
    lease = _lease(MemoryElection(store), _caller('memory'), refresh_seconds=60.0)
    before = _refresh_threads()
    assert await asyncio.to_thread(lease.acquire)
    try:
        (refresher,) = _refresh_threads() - before
    finally:
        await asyncio.to_thread(lease.release)

    assert refresher not in _refresh_threads()
    assert not refresher.is_alive()


async def test_the_lease_refresh_thread_ends_when_refresh_answers_false():
    store = ElectionStore()
    provider = MemoryElection(store)
    lease = _lease(provider, _caller('memory'), refresh_seconds=0.01)
    before = _refresh_threads()
    assert await asyncio.to_thread(lease.acquire)
    try:
        (refresher,) = _refresh_threads() - before
        # Another holder took the scope over: the next refresh answers False.
        store.leases[SCOPE] = ('f' * 16, store.now + LEASE_SECONDS, LEASE_SECONDS)
        await wait_for(lambda: not refresher.is_alive())

        assert provider.calls['refresh'] >= 1
        assert await asyncio.to_thread(lease.is_held) is False
        assert provider.calls['is_held'] == 0
    finally:
        await asyncio.to_thread(lease.release)


async def test_the_lease_refresh_keeps_the_lease_alive():
    store = ElectionStore()
    provider = MemoryElection(store)
    lease = _lease(provider, _caller('memory'), refresh_seconds=0.01)
    assert await asyncio.to_thread(lease.acquire)
    try:
        store.advance(LEASE_SECONDS - 1)
        await wait_for(lambda: provider.calls['refresh'] >= 1 and store.leases[SCOPE][1] > store.now + 1)

        assert await asyncio.to_thread(lease.is_held) is True
    finally:
        await asyncio.to_thread(lease.release)


class _UnreachableElection(MemoryElection):
    """A provider whose service cannot be reached: every acquire raises a connection error."""

    def __init__(self) -> None:
        super().__init__(ElectionStore())
        self.attempts = 0

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.attempts += 1
        raise ConnectionError('election service unreachable')


async def test_a_lease_call_that_fails_reads_as_not_held():
    provider = _UnreachableElection()
    caller = _caller('unreachable')
    lease = _lease(provider, caller)
    errors_before = _calls('unreachable', 'acquire', 'error')
    threads_before = _refresh_threads()

    assert await asyncio.to_thread(lease.acquire) is False

    assert provider.attempts == 1 + ResilienceConfig().retries
    assert _calls('unreachable', 'acquire', 'error') == errors_before + 1
    assert _refresh_threads() - threads_before == set()


async def test_a_lease_call_on_the_loop_thread_is_refused():
    provider = MemoryElection(ElectionStore())
    lease = _lease(provider, _caller('memory'))

    with pytest.raises(RuntimeError, match='never on its event loop'):
        lease.acquire()

    assert provider.calls['acquire'] == 0


async def test_a_lease_call_on_a_closed_loop_reads_as_not_held():
    loop = asyncio.new_event_loop()
    loop.close()
    lease = ElectionLease(
        MemoryElection(ElectionStore()),
        _caller('memory'),
        loop,
        scope=SCOPE,
        lease_seconds=LEASE_SECONDS,
        provider_name='memory',
    )

    assert lease.acquire() is False
    assert lease.is_held() is False
    lease.release()


@contextlib.contextmanager
def _loop_in_thread() -> Iterator[asyncio.AbstractEventLoop]:
    """An event loop running in its own thread, so the test's thread can call the lease directly."""
    loop = asyncio.new_event_loop()
    runner = threading.Thread(target=loop.run_forever, name='test-election-loop')
    runner.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        runner.join()
        loop.close()


def _single_attempt_lease(provider, loop: asyncio.AbstractEventLoop, timeout_seconds: float) -> ElectionLease:
    caller = ResilientCaller(
        ResilienceConfig(timeout_seconds=timeout_seconds, retries=0),
        feature='election',
        provider='memory',
        category='recorder',
    )
    return ElectionLease(provider, caller, loop, scope=SCOPE, lease_seconds=LEASE_SECONDS, provider_name='memory')


def test_a_lease_call_the_loop_does_not_answer_in_time_reads_as_not_held(monkeypatch):
    monkeypatch.setattr(election_mod, 'BRIDGE_SLACK_SECONDS', 0.0)
    with _loop_in_thread() as loop:
        lease = _single_attempt_lease(MemoryElection(ElectionStore()), loop, timeout_seconds=0.01)
        # The loop is busy with something else for longer than the call may
        # take. The provider would answer True, so False means the thread
        # stopped waiting.
        loop.call_soon_threadsafe(time.sleep, 0.2)
        before = _refresh_threads()

        assert lease.acquire() is False
        assert _refresh_threads() - before == set()


class _HangingElection(MemoryElection):
    """A provider whose acquire never answers; ``entered`` is set once a call is waiting."""

    def __init__(self) -> None:
        super().__init__(ElectionStore())
        self.entered = threading.Event()

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.entered.set()
        await asyncio.Event().wait()
        return True


def test_a_lease_call_cancelled_on_the_loop_reads_as_not_held():
    # A worker that shuts its loop down cancels every task, the lease's call among them.
    provider = _HangingElection()
    with _loop_in_thread() as loop:
        lease = _single_attempt_lease(provider, loop, timeout_seconds=30.0)

        def cancel_every_task() -> None:
            for task in asyncio.all_tasks(loop):
                task.cancel()

        def cancel_once_waiting() -> None:
            if provider.entered.wait(5):
                loop.call_soon_threadsafe(cancel_every_task)

        canceller = threading.Thread(target=cancel_once_waiting)
        canceller.start()
        try:
            assert lease.acquire() is False
        finally:
            canceller.join()
    assert provider.entered.is_set()


# --- the lookup ---


@pytest.fixture
def no_election_plugins(monkeypatch):
    monkeypatch.setattr(lookup, 'entry_points', lambda *, group: [])


def test_resolve_factory_finds_the_builtin_election(no_election_plugins):
    factory = resolve_factory(
        ELECTION_ENTRY_POINT_GROUP, BUILTIN_ELECTIONS, 'filesystem', config_path='history.archive.election.provider'
    )

    assert factory is BUILTIN_ELECTIONS['filesystem']


def test_resolve_factory_names_the_election_path(no_election_plugins):
    with pytest.raises(UnknownProviderError) as raised:
        resolve_factory(
            ELECTION_ENTRY_POINT_GROUP, BUILTIN_ELECTIONS, 'nope', config_path='history.archive.election.provider'
        )

    assert str(raised.value) == "history.archive.election.provider 'nope' is not known; known providers: filesystem"


def test_the_election_entry_point_group_name():
    assert ELECTION_ENTRY_POINT_GROUP == 'drakkar.election_providers'
