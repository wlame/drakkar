"""The archive election: the provider kit, the filesystem provider and the archive thread's lease.

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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from prometheus_client import REGISTRY

from drakkar.config.resilience import ResilienceConfig
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import election as election_mod
from drakkar.recorder import electiontest
from drakkar.recorder.archive_lock import LOCK_NAME
from drakkar.recorder.election import (
    BUILTIN_ELECTIONS,
    ElectionContext,
    ElectionLease,
    FilesystemElection,
)
from drakkar.recorder.history import history_dir
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, wait_for
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
        if os.path.basename(os.fspath(path)) == LOCK_NAME:
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


async def test_a_lock_read_error_on_refresh_reads_as_unconfirmed(tmp_path, monkeypatch):
    provider = await _started_filesystem(tmp_path)
    lease = _lease(provider, _caller('filesystem-io-error', retries=0), refresh_seconds=0.01)
    before = _refresh_threads()
    assert await asyncio.to_thread(lease.acquire)
    (refresher,) = _refresh_threads() - before
    real_open = os.open
    lock_path = os.path.join(history_dir(str(tmp_path), SCOPE), LOCK_NAME)

    def open_with_io_error(path, flags, *args, **kwargs):
        if os.fspath(path) == lock_path:
            raise OSError(5, 'Input/output error', lock_path)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', open_with_io_error)
    try:
        await wait_for(lambda: not refresher.is_alive())

        assert lease.loss_reason == 'unconfirmed'
    finally:
        monkeypatch.undo()
        await asyncio.to_thread(lease.release)
        await provider.stop()
    # Unconfirmed means it may still be held: the release deleted the lock.
    assert not Path(lock_path).exists()


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
    provider = MemoryElection(store)
    lease = _lease(provider, _caller('memory'))
    before = _refresh_threads()

    assert await asyncio.to_thread(lease.acquire) is False

    assert _refresh_threads() - before == set()
    # A clear "no" takes nothing, so there is nothing to give back.
    assert provider.calls['release'] == 0
    assert store.holder(SCOPE) == 'f' * 16


async def test_releasing_a_refused_lease_sends_no_release():
    store = ElectionStore()
    store.leases[SCOPE] = ('f' * 16, store.now + LEASE_SECONDS, LEASE_SECONDS)
    provider = MemoryElection(store)
    lease = _lease(provider, _caller('memory'))
    assert await asyncio.to_thread(lease.acquire) is False

    await asyncio.to_thread(lease.release)

    assert provider.calls['release'] == 0
    assert store.holder(SCOPE) == 'f' * 16


async def test_releasing_a_lease_taken_over_by_another_holder_sends_no_release():
    store = ElectionStore()
    provider = MemoryElection(store)
    lease = _lease(provider, _caller('memory'))
    assert await asyncio.to_thread(lease.acquire)
    store.leases[SCOPE] = ('f' * 16, store.now + LEASE_SECONDS, LEASE_SECONDS)
    assert await asyncio.to_thread(lease.is_held) is False
    assert lease.loss_reason == 'taken_over'

    await asyncio.to_thread(lease.release)

    assert provider.calls['release'] == 0
    assert store.holder(SCOPE) == 'f' * 16


async def test_a_lease_serves_one_pass():
    lease = _lease(MemoryElection(ElectionStore()), _caller('memory'))
    assert await asyncio.to_thread(lease.acquire)
    try:
        with pytest.raises(RuntimeError, match='serves one pass'):
            await asyncio.to_thread(lease.acquire)
    finally:
        await asyncio.to_thread(lease.release)

    with pytest.raises(RuntimeError, match='serves one pass'):
        await asyncio.to_thread(lease.acquire)


class _LostAnswerElection(MemoryElection):
    """Every acquire takes the lease and then fails, like an attempt whose answer was lost on a slow mount."""

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        await super().acquire(scope, token, lease_seconds)
        raise ConnectionError('the answer was lost')


async def test_an_acquire_with_an_unclear_outcome_releases_what_it_may_have_taken():
    store = ElectionStore()
    provider = _LostAnswerElection(store)
    lease = _lease(provider, _caller('lost-answer'))

    assert await asyncio.to_thread(lease.acquire) is False

    assert provider.calls['release'] == 1
    assert store.holder(SCOPE) is None


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
        assert lease.loss_reason == 'taken_over'
    finally:
        await asyncio.to_thread(lease.release)


class _RejectingRefreshElection(MemoryElection):
    """A provider whose refresh fails with an error that is not retried."""

    async def refresh(self, scope: str, token: str) -> bool:
        self.calls['refresh'] += 1
        raise ValueError('refresh rejected')


async def test_a_lease_whose_refresh_fails_is_lost_as_unconfirmed():
    provider = _RejectingRefreshElection(ElectionStore())
    lease = _lease(provider, _caller('rejecting-refresh'), refresh_seconds=0.01)
    before = _refresh_threads()
    assert await asyncio.to_thread(lease.acquire)
    try:
        (refresher,) = _refresh_threads() - before
        await wait_for(lambda: not refresher.is_alive())

        assert await asyncio.to_thread(lease.is_held) is False
        assert provider.calls['is_held'] == 0
        assert lease.loss_reason == 'unconfirmed'
    finally:
        await asyncio.to_thread(lease.release)
    # An unconfirmed lease may still be held, so it is given back.
    assert provider.calls['release'] == 1


async def test_a_lease_the_provider_no_longer_confirms_is_lost_as_taken_over():
    store = ElectionStore()
    lease = _lease(MemoryElection(store), _caller('memory'))
    assert await asyncio.to_thread(lease.acquire)
    try:
        assert lease.loss_reason is None
        store.leases[SCOPE] = ('f' * 16, store.now + LEASE_SECONDS, LEASE_SECONDS)

        assert await asyncio.to_thread(lease.is_held) is False
        assert lease.loss_reason == 'taken_over'
    finally:
        await asyncio.to_thread(lease.release)


class _UncheckableElection(MemoryElection):
    """A provider whose fencing check fails with an error that is not retried."""

    async def is_held(self, scope: str, token: str) -> bool:
        raise ValueError('no answer')


async def test_a_lease_whose_check_fails_is_lost_as_unconfirmed():
    lease = _lease(_UncheckableElection(ElectionStore()), _caller('uncheckable'))
    assert await asyncio.to_thread(lease.acquire)
    try:
        assert await asyncio.to_thread(lease.is_held) is False
        assert lease.loss_reason == 'unconfirmed'
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
    # An unclear outcome: the lease gives back whatever the provider may have taken.
    assert provider.calls['release'] == 1


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
        # Like asyncio.run: cancel and await what is left, so no abandoned call is destroyed unrun.
        asyncio.run_coroutine_threadsafe(_cancel_the_other_tasks(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        runner.join()
        loop.close()


async def _cancel_the_other_tasks() -> None:
    others = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in others:
        task.cancel()
    await asyncio.gather(*others, return_exceptions=True)


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
