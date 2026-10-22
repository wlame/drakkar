"""The election: which worker of a cluster runs a cluster task.

Every worker of a cluster runs the task; an election provider lets one of
them work at a time. A run takes a lease on its *scope*, ``<cluster>/<task>``
(the sanitized cluster name, a slash and the task name, see
:func:`drakkar.schedule.task_scope`), under a 16-hex *token* of its own. It
refreshes the lease while it works, may check it before a step that must not
run twice (fencing; the archive pass checks it right before each publish),
and releases it at the end. A holder that dies stops refreshing, and once
``lease_seconds`` pass another worker may take the scope over. Every
provider keeps its leases in one family keyed by the scope; a provider may
treat the scope as an opaque string.

The provider is asynchronous and runs on the worker's event loop; the archive
pass (the system task ``history.archive``) is synchronous and runs in a
thread. :class:`ElectionLease` joins the
two: the thread calls it, and it runs every provider call on the loop through
a :class:`~drakkar.providers.resilience.ResilientCaller`, waiting at most the
call's bound. A hung provider therefore holds up the archive thread for a
bounded time and never the loop. :class:`ThreadFence` gives a thread the same
bounded check of a cluster task's slot lease
(:class:`~drakkar.cluster_tasks.SlotLease`).

A third-party provider is a class with the six async methods of
:class:`ElectionProvider` plus a factory ``(options) -> provider`` that its
package exposes as an entry point of the ``drakkar.election_providers``
group. ``drakkar.recorder.electiontest`` holds the checks a provider should
pass.
"""

import asyncio
import concurrent.futures
import math
import os
import secrets
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from drakkar.dbfiles import DB_DIR_MODE
from drakkar.fleet.card import sanitize_worker_name
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import CallResult, ResilientCaller
from drakkar.providers.thread import PROVIDER_STOPPED, ProviderThread
from drakkar.recorder.election_consul import consul_election_factory
from drakkar.recorder.election_postgres import postgres_election_factory
from drakkar.recorder.election_redis import redis_election_factory
from drakkar.recorder.history import task_lock_dir
from drakkar.recorder.task_lock import FILESYSTEM_PROVIDER, TaskLock
from drakkar.schedule import parse_task_scope

if TYPE_CHECKING:
    # Only for the annotations: loading drakkar.cluster_tasks imports this module.
    from drakkar.cluster_tasks import SlotLease

# Public and stable: plugin packages name this group in their metadata.
ELECTION_ENTRY_POINT_GROUP = 'drakkar.election_providers'

# The feature label of the election's provider calls, metrics and log lines.
ELECTION_FEATURE = 'election'

REFRESH_THREAD_NAME = 'drakkar-election-refresh'
# The filesystem provider's file-call thread: this prefix plus the sanitized worker name.
ELECTION_THREAD_PREFIX = 'drakkar-election-files-'

# Why a lease is no longer held (``ElectionLease.loss_reason``): the provider
# answered that another token holds the scope, or no call could confirm it.
LOSS_TAKEN_OVER = 'taken_over'
LOSS_UNCONFIRMED = 'unconfirmed'

# How much longer than a call's own bound the archive thread waits for it, so
# the caller's own timeout, not the thread's wait, normally ends a slow call.
BRIDGE_SLACK_SECONDS = 1.0
# The longest gap between two refreshes of a lease, whatever its length.
MAX_REFRESH_SECONDS = 60.0
# A lease is refreshed this many times in its length, so one missed refresh does not lose it.
REFRESHES_PER_LEASE = 3


@dataclass(frozen=True)
class ElectionContext:
    """What a provider learns about the worker that runs it, at ``start``.

    ``shared_dir`` is the worker's shared folder (``shared.dir``); the
    ``filesystem`` provider keeps its locks in its ``tasks`` folder, others
    ignore it. ``options`` is the provider's own settings mapping (for a
    plugin, ``periodic.election.plugin_options``). ``connections``
    resolves the worker's named shared connections; it is ``None`` when the
    election runs outside a worker, as in a test.
    """

    worker: str
    instance: str
    cluster: str
    shared_dir: str
    logger: Any
    options: Mapping[str, Any]
    connections: Connections | None = None


class ElectionProvider(Protocol):
    """Leases on scopes, one holder per scope. Every method may raise; the caller counts and logs the failure.

    A provider is single-use. After ``stop`` every other method must raise,
    ``start`` included: the worker's stop may abandon an archive pass that
    is still in its window, and that pass is fenced at its next check only
    because the provider refuses the call.
    """

    async def start(self, ctx: ElectionContext) -> None:
        """Prepare the provider, before any other method.

        A failed ``start`` is retried, so it must leave nothing behind. A
        ``start`` after ``stop`` raises: the provider is not started again.
        """
        ...

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        """Take ``scope`` for ``token`` for ``lease_seconds``; False while another token holds a live lease on it.

        An acquire by the token that already holds the scope answers True,
        so a retried acquire whose first answer was lost does not lock the
        holder out.
        """
        ...

    async def refresh(self, scope: str, token: str) -> bool:
        """Extend the lease of ``token`` on ``scope``; False when ``token`` no longer holds it."""
        ...

    async def is_held(self, scope: str, token: str) -> bool:
        """True while ``token`` holds ``scope`` (the fencing check before each publish)."""
        ...

    async def release(self, scope: str, token: str) -> None:
        """Free ``scope``, but only while ``token`` holds it."""
        ...

    async def stop(self) -> None:
        """Release what ``start`` took. Called at most once, last; leases are released by their holders.

        Every call after it must raise (see the class docstring).
        """
        ...


ElectionFactory = Callable[[Mapping[str, Any]], ElectionProvider]


class FilesystemElection:
    """The ``filesystem`` provider: the lock file ``tasks/<cluster>/<task>.lock`` of a scope in the shared folder.

    One :class:`TaskLock` per scope, replaced when a call names another
    token. A scope that is not ``<cluster>/<task>`` raises ``ValueError``
    before any file is touched. Its takeover rule (rename aside, stale age
    on the file server's
    clock) decides when a lease ran out.

    Every file call runs on the provider's own :class:`ProviderThread`, one at
    a time, never on the shared ``asyncio.to_thread`` pool: a call stuck on a
    hung mount holds only that thread, and the next call times out while it
    waits for it. The event loop and every other ``to_thread`` user (sinks,
    sources, handler code) keep their threads.

    ``stale_seconds`` replaces the ``lease_seconds`` of :meth:`acquire` as the
    age past which a lock is taken over.
    """

    def __init__(self, stale_seconds: float | None = None) -> None:
        self._stale_seconds = stale_seconds
        self._ctx: ElectionContext | None = None
        self._locks: dict[str, TaskLock] = {}
        self._runner: ProviderThread | None = None
        self._stopped = False

    async def start(self, ctx: ElectionContext) -> None:
        """Remember the worker and its shared folder, and start the provider thread; no file is opened.

        A retried ``start`` keeps the thread of the first one. A ``start``
        after :meth:`stop` raises ``RuntimeError``: the provider is single-use.
        """
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        self._ctx = ctx
        if self._runner is None:
            self._runner = ProviderThread(name=f'{ELECTION_THREAD_PREFIX}{sanitize_worker_name(ctx.worker)}')

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        """Create the lock file, taking over a stale one; True again for the token that holds it."""
        stale_seconds = lease_seconds if self._stale_seconds is None else self._stale_seconds
        lock = self._new_lock(scope, token, stale_seconds)
        self._locks[scope] = lock
        folder = self._folder(scope)
        return await self._run(lambda: _acquire_in_folder(lock, folder))

    async def refresh(self, scope: str, token: str) -> bool:
        """Move the lock file's mtime to now while it holds ``token``."""
        return await self._run(self._lock(scope, token).refresh)

    async def is_held(self, scope: str, token: str) -> bool:
        """True while the lock file holds ``token``."""
        return await self._run(self._lock(scope, token).is_ours)

    async def release(self, scope: str, token: str) -> None:
        """Delete the lock file while it holds ``token``."""
        await self._run(self._lock(scope, token).release)

    async def stop(self) -> None:
        """Let the provider thread exit; no lease is released, each archive pass releases its own.

        A call after the stop raises at once, so a pass that outlived the
        stop is fenced at its next check instead of waiting for a thread
        that is gone. A ``start`` after it raises too.
        """
        self._stopped = True
        if self._runner is not None:
            self._runner.stop()

    async def _run(self, call: Callable[[], Any]) -> Any:
        """Run one blocking file call on the provider thread."""
        self._context()
        if self._stopped or self._runner is None:
            raise RuntimeError('the filesystem election is stopped')
        return await self._runner.run(call)

    def _context(self) -> ElectionContext:
        if self._ctx is None:
            raise RuntimeError('the filesystem election is not started')
        return self._ctx

    def _folder(self, scope: str) -> str:
        cluster, _ = parse_task_scope(scope)
        return task_lock_dir(self._context().shared_dir, cluster)

    def _new_lock(self, scope: str, token: str, stale_seconds: float) -> TaskLock:
        ctx = self._context()
        _, name = parse_task_scope(scope)
        return TaskLock(
            self._folder(scope),
            name,
            worker=ctx.worker,
            instance=ctx.instance,
            stale_seconds=stale_seconds,
            scope=scope,
            token=token,
            auto_refresh=False,
        )

    def _lock(self, scope: str, token: str) -> TaskLock:
        """The scope's lock for ``token``; a handle made here never takes anything over, only reads and deletes."""
        lock = self._locks.get(scope)
        if lock is None or lock.token != token:
            lock = self._new_lock(scope, token, math.inf)
            self._locks[scope] = lock
        return lock


def _acquire_in_folder(lock: TaskLock, folder: str) -> bool:
    """Take ``lock``, creating its task lock folder first when it is missing."""
    os.makedirs(folder, mode=DB_DIR_MODE, exist_ok=True)
    # A timed-out attempt's thread may still have created the file with this
    # token; the retry must not then lock its own holder out.
    return lock.acquire() or lock.is_ours()


BUILTIN_ELECTIONS: dict[str, ElectionFactory] = {
    FILESYSTEM_PROVIDER: lambda options: FilesystemElection(),
    'postgres': postgres_election_factory,
    'redis': redis_election_factory,
    'consul': consul_election_factory,
}


def lease_refresh_seconds(lease_seconds: float) -> float:
    """Seconds between two refreshes of a lease: a third of its length, at most :data:`MAX_REFRESH_SECONDS`."""
    return min(MAX_REFRESH_SECONDS, lease_seconds / REFRESHES_PER_LEASE)


class ElectionLease:
    """The archive thread's synchronous handle on one lease of one scope; one lease serves one pass.

    Every method blocks the calling thread and must never be called on the
    event loop's own thread: it runs the provider call on ``loop`` through
    ``caller`` and waits at most the caller's operation bound plus
    :data:`BRIDGE_SLACK_SECONDS`. A call that does not finish in that time,
    or does not succeed (``ok`` False), reads as not held.

    While held, a daemon thread refreshes the lease every ``refresh_seconds``
    (default: :func:`lease_refresh_seconds` of the lease length).
    It ends on :meth:`release`, and on a refresh that does not answer True;
    from then on :meth:`is_held` answers False without a provider call.
    :attr:`loss_reason` says why a lease reads as not held.
    """

    def __init__(
        self,
        provider: ElectionProvider,
        caller: ResilientCaller,
        loop: asyncio.AbstractEventLoop,
        *,
        scope: str,
        lease_seconds: float,
        provider_name: str,
        refresh_seconds: float | None = None,
        token: str | None = None,
    ) -> None:
        self.token = token if token is not None else secrets.token_hex(8)
        self.provider_name = provider_name
        self.scope = scope
        self.lease_seconds = lease_seconds
        self._provider = provider
        self._caller = caller
        self._loop = loop
        self._refresh_seconds = lease_refresh_seconds(lease_seconds) if refresh_seconds is None else refresh_seconds
        self._wait_seconds = caller.operation_bound_seconds + BRIDGE_SLACK_SECONDS
        self._acquired_once = False
        # True once the provider answered True to the acquire.
        self._held = False
        self._stop_refreshing = threading.Event()
        self._lost = threading.Event()
        self._loss_reason: str | None = None
        self._refresher: threading.Thread | None = None

    @property
    def loss_reason(self) -> str | None:
        """Why the last check read as not held: :data:`LOSS_TAKEN_OVER` or :data:`LOSS_UNCONFIRMED`; ``None`` before."""
        return self._loss_reason

    def acquire(self) -> bool:
        """Take the lease; on success start the refresh thread. A second call raises ``RuntimeError``.

        When the outcome is unclear (the call failed or did not answer in
        time), the provider may still have taken the lease, so the lease is
        released at once rather than left without a refresh until it expires.
        """
        if self._acquired_once:
            raise RuntimeError('an election lease serves one pass; make a new one for the next pass')
        self._acquired_once = True
        result = self._run('acquire', lambda: self._provider.acquire(self.scope, self.token, self.lease_seconds))
        if result is None or not result.ok:
            self._run('release', lambda: self._provider.release(self.scope, self.token))
            return False
        if result.value is not True:
            return False
        self._held = True
        self._refresher = threading.Thread(target=self._refresh_loop, name=REFRESH_THREAD_NAME, daemon=True)
        self._refresher.start()
        return True

    def is_held(self) -> bool:
        """True while the provider confirms the lease is still this pass's (the fencing check)."""
        if self._lost.is_set():
            return False
        reason = lease_loss_reason(self._run('is_held', lambda: self._provider.is_held(self.scope, self.token)))
        if reason is None:
            return True
        self._loss_reason = reason
        return False

    def release(self) -> None:
        """Stop the refresh thread, then release the lease at the provider while it may still be this pass's.

        No release is sent when the provider answered clearly that the lease
        is not this pass's: the acquire was refused, or a refresh or a check
        found another holder (:data:`LOSS_TAKEN_OVER`). An unclear acquire
        has already released at once. A lease lost as
        :data:`LOSS_UNCONFIRMED` may still be held, so it is released.
        """
        self._stop_refreshing.set()
        if self._refresher is not None:
            # A refresh in progress finishes within its own wait.
            self._refresher.join(self._wait_seconds)
            self._refresher = None
        if not self._held or self._loss_reason == LOSS_TAKEN_OVER:
            return
        self._run('release', lambda: self._provider.release(self.scope, self.token))

    def _refresh_loop(self) -> None:
        # Event.wait returns True once release has been called.
        while not self._stop_refreshing.wait(self._refresh_seconds):
            reason = lease_loss_reason(self._run('refresh', lambda: self._provider.refresh(self.scope, self.token)))
            if reason is not None:
                # The reason is written before the event, so a reader that sees the event sees it too.
                self._loss_reason = reason
                self._lost.set()
                return

    def _run(self, op: str, make_call: Callable[[], Awaitable[Any]]) -> CallResult | None:
        """Run one operation on the loop; ``None`` when the loop is closed or does not answer in time."""
        if _running_loop() is self._loop:
            # Waiting here would stop the loop from ever running the call.
            raise RuntimeError('an election lease is called from a worker thread, never on its event loop')
        operation = self._caller.call(op, make_call)
        try:
            future = asyncio.run_coroutine_threadsafe(operation, self._loop)
        except RuntimeError:
            # The loop is closed (the worker is shutting down).
            operation.close()
            return None
        try:
            return future.result(timeout=self._wait_seconds)
        except TimeoutError:
            future.cancel()
            return None
        except concurrent.futures.CancelledError:
            return None


class ThreadFence:
    """The archive thread's blocking check of one slot lease: is the slot still this worker's?

    A cluster task's lease (:class:`~drakkar.cluster_tasks.SlotLease`) lives
    on the event loop; a body that runs on a thread of its own (the archive
    pass) checks it through this before each step that must not run twice.
    :meth:`is_held` runs the lease's check on ``loop`` and waits at most the
    caller's operation bound plus :data:`BRIDGE_SLACK_SECONDS`; a check that
    does not answer in that time reads as not held, with
    :data:`LOSS_UNCONFIRMED`.
    """

    def __init__(
        self,
        lease: SlotLease,
        caller: ResilientCaller,
        loop: asyncio.AbstractEventLoop,
        *,
        provider_name: str,
    ) -> None:
        self.provider_name = provider_name
        self._lease = lease
        self._loop = loop
        self._wait_seconds = caller.operation_bound_seconds + BRIDGE_SLACK_SECONDS
        # Set when a check got no answer; the lease's own reason covers every other loss.
        self._no_answer_reason: str | None = None

    @property
    def token(self) -> str:
        """The token of the slot's lease."""
        return self._lease.token

    @property
    def loss_reason(self) -> str | None:
        """Why the last check read as not held: :data:`LOSS_TAKEN_OVER` or :data:`LOSS_UNCONFIRMED`; ``None`` before."""
        if self._no_answer_reason is not None:
            return self._no_answer_reason
        return self._lease.loss_reason

    def is_held(self) -> bool:
        """True while the provider confirms the slot's lease (blocking; never call it on ``loop``'s thread)."""
        if _running_loop() is self._loop:
            # Waiting here would stop the loop from ever running the check.
            raise RuntimeError('a thread fence is called from a worker thread, never on its event loop')
        check = self._lease.is_held()
        try:
            future = asyncio.run_coroutine_threadsafe(check, self._loop)
        except RuntimeError:
            # The loop is closed (the worker is shutting down).
            check.close()
            return self._no_answer()
        try:
            return future.result(timeout=self._wait_seconds)
        except (TimeoutError, concurrent.futures.CancelledError):
            future.cancel()
            return self._no_answer()

    def _no_answer(self) -> bool:
        self._no_answer_reason = LOSS_UNCONFIRMED
        return False


def lease_loss_reason(result: CallResult | None) -> str | None:
    """``None`` when the provider confirmed the lease, else why it reads as not held."""
    if result is None or not result.ok:
        return LOSS_UNCONFIRMED
    return None if result.value is True else LOSS_TAKEN_OVER


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None
