"""The budget of detached workers: ``max_detached`` lease slots in the election store.

A worker must hold one slot to be detached from its consumer group. A lease
with a token is a mutex, so N of them make a counting semaphore, and a
crashed worker's slot frees itself when its lease ends. The slots live in
the store of the worker's one election provider (:class:`ClusterElection`),
under the scopes ``<sanitized cluster>/detach.<group key>_<i>``, so the
providers need no new operation and the budget is per cluster and consumer
group.

Every call goes through a :class:`ResilientCaller` of its own (feature
``detach``), never the election's caller: a store failure seen by a detach
never opens the breaker of the cluster tasks, and an open breaker of the
cluster tasks never refuses a detach.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from collections.abc import Awaitable, Callable, Sequence
from enum import StrEnum
from functools import partial
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.periodic import SYSTEM_CLOCK, Clock
from drakkar.recorder.election import LOSS_TAKEN_OVER as ELECTION_LOSS_TAKEN_OVER
from drakkar.recorder.election import lease_loss_reason, lease_refresh_seconds
from drakkar.schedule import task_scope

if TYPE_CHECKING:
    from drakkar.cluster_tasks import ClusterElection
    from drakkar.providers.resilience import ResilientCaller

logger = structlog.get_logger()

# The feature label of the detach caller in the provider metrics and log lines.
DETACH_FEATURE = 'detach'
# The bound of each store call of a detach: the start, each acquire, each holder read, the release.
SLOT_ACQUIRE_SECONDS = 5.0
# The hex characters of the SHA-256 of the consumer group id kept in the group key.
_GROUP_KEY_HEX_CHARS = 12

# Why a held slot is lost (the reason ``on_lost`` receives): the store answered
# that another token holds it, or no refresh confirmed it for a whole lease.
LOSS_TAKEN_OVER = ELECTION_LOSS_TAKEN_OVER
LOSS_EXPIRED = 'expired'


class SlotAcquire(StrEnum):
    """What one try to take a slot came to."""

    WON = 'won'
    # Every slot answered that another token holds it.
    BUDGET_FULL = 'budget_full'
    # No slot was won, and at least one call failed or made no attempt.
    STORE_UNAVAILABLE = 'store_unavailable'


def group_key(consumer_group: str) -> str:
    """The short key of a consumer group in a slot scope: ``g`` plus 12 hex characters of its SHA-256.

    A group id may hold characters a scope cannot, so the scope carries this
    key instead of the id.
    """
    return 'g' + hashlib.sha256(consumer_group.encode('utf-8')).hexdigest()[:_GROUP_KEY_HEX_CHARS]


def slot_scope(cluster: str, consumer_group: str, index: int) -> str:
    """The election scope of slot ``index`` of ``consumer_group`` in ``cluster``."""
    return task_scope(cluster, f'detach.{group_key(consumer_group)}_{index}')


class _StopRequest:
    """Set when :meth:`DetachSlot.release` or :meth:`DetachSlot.stop_refreshing` ends a refresher on purpose."""

    def __init__(self) -> None:
        self.requested = False


async def _log_error(event: str, **fields: Any) -> None:
    """Log an error line of the refresher; a log call that fails must not keep the loss from its report."""
    try:
        await logger.aexception(event, category='kafka', **fields)
    except Exception:
        # The log is the only place this error could go; the loss report matters more.
        return


class DetachSlot:
    """The slot this worker holds while it is detached: at most one, taken from ``scopes`` in order.

    :meth:`acquire` takes the first free slot with a fresh token.
    :meth:`start_refreshing` keeps it on the event loop. A refresh that
    fails or does not answer is tried again at the next refresh time; the
    slot is reported lost (``expired``) when ``lease_seconds`` have passed
    since the last confirmed acquire or refresh, and at once (``taken_over``)
    when the store answers that another token holds it. :meth:`release`
    gives it back.
    """

    def __init__(
        self,
        election: ClusterElection,
        caller: ResilientCaller,
        *,
        scopes: Sequence[str],
        lease_seconds: int,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self._election = election
        self._caller = caller
        self._scopes = tuple(scopes)
        self._lease_seconds = lease_seconds
        self._clock = clock
        # The index of the slot held, its token, and the clock time of the last confirmed acquire or refresh.
        self.index: int | None = None
        self._token: str | None = None
        self._confirmed_at = 0.0
        self._loss_reason: str | None = None
        self._refresher: asyncio.Task[None] | None = None
        self._refresh_stop: _StopRequest | None = None
        # Releases started for an acquire that was cancelled; kept so they are not collected while they run.
        self._background: set[asyncio.Task[None]] = set()

    @property
    def seconds_left(self) -> float:
        """Seconds until the lease ends, counted from the last confirmed acquire or refresh; 0 with no slot or one taken over."""
        if self.index is None or self._loss_reason == LOSS_TAKEN_OVER:
            return 0.0
        return max(self._confirmed_at + self._lease_seconds - self._clock.time(), 0.0)

    async def acquire(self) -> SlotAcquire:
        """Start the provider if needed, then take the first free slot, each call within :data:`SLOT_ACQUIRE_SECONDS`.

        An acquire that failed after an attempt may still have taken the
        slot at the store, so it is released at once rather than left to
        expire. A cancelled acquire is released the same way, in the
        background, because the store may have taken it before the answer.
        """
        if self.index is not None:
            raise RuntimeError('this worker already holds a detach slot')
        started = await self._election.ensure_started(SLOT_ACQUIRE_SECONDS, caller=self._caller)
        if not started.ok:
            return SlotAcquire.STORE_UNAVAILABLE
        provider = self._election.provider
        store_failed = False
        for index, scope in enumerate(self._scopes):
            token = secrets.token_hex(8)
            asked_at = self._clock.time()
            try:
                result = await self._caller.call(
                    'acquire',
                    partial(provider.acquire, scope, token, self._lease_seconds),
                    deadline_seconds=SLOT_ACQUIRE_SECONDS,
                )
            except asyncio.CancelledError:
                self._release_in_background(scope, token)
                raise
            if result.ok and result.value is True:
                # No await from here to the return: a cancellation cannot leave a won slot unrecorded.
                self.index = index
                self._token = token
                self._confirmed_at = asked_at
                self._loss_reason = None
                return SlotAcquire.WON
            if result.ok:
                continue
            store_failed = True
            # A spent deadline or an open breaker made no call: there is nothing to give back.
            if result.attempts > 0:
                await self._release_scope(scope, token)
        return SlotAcquire.STORE_UNAVAILABLE if store_failed else SlotAcquire.BUDGET_FULL

    async def held_count(self) -> int:
        """How many of the slots are held now, this worker's included; a read that fails counts as held.

        Each holder read runs within :data:`SLOT_ACQUIRE_SECONDS`, all at once.
        Counting an unknown slot as held can only refuse a detach, never allow
        one too many.
        """
        provider = self._election.provider
        results = await asyncio.gather(
            *(
                self._caller.call(
                    'holder',
                    partial(provider.holder, scope),
                    deadline_seconds=SLOT_ACQUIRE_SECONDS,
                )
                for scope in self._scopes
            )
        )
        return sum(1 for result in results if not result.ok or result.value is not None)

    def start_refreshing(self, on_lost: Callable[[str], Awaitable[None]]) -> None:
        """Refresh the held slot on the event loop until :meth:`release` or :meth:`stop_refreshing`, or until it is lost.

        ``on_lost`` is called once, with :data:`LOSS_TAKEN_OVER` or
        :data:`LOSS_EXPIRED`; it may call :meth:`release`.
        """
        if self.index is None or self._token is None:
            raise RuntimeError('there is no detach slot to refresh')
        self.stop_refreshing()
        scope = self._scopes[self.index]
        # One flag per refresher: an old refresher that ends late cannot read a new one's flag.
        stop = _StopRequest()
        self._refresh_stop = stop
        self._refresher = asyncio.create_task(
            self._refresh_loop(scope, self._token, stop, on_lost), name=f'drakkar-detach-refresh-{scope}'
        )

    def stop_refreshing(self) -> None:
        """Stop the refresh task at once, releasing nothing and reporting no loss."""
        self._end_refresher()

    async def release(self, *, deadline_seconds: float = SLOT_ACQUIRE_SECONDS) -> None:
        """Stop refreshing, then give the slot back; no release is sent once the store answered ``taken_over``.

        A slot lost as ``expired`` may still be held at the store, so it is released.
        """
        refresher = self._end_refresher()
        # A refresher runs only while a slot is held, so with no slot there is nothing to wait for.
        if self.index is None or self._token is None:
            return
        scope, token = self._scopes[self.index], self._token
        if refresher is not None:
            try:
                # asyncio.wait, not await: a cancellation of this release must still reach its caller.
                await asyncio.wait({refresher})
            except asyncio.CancelledError:
                self._forget_slot()
                if self._loss_reason != LOSS_TAKEN_OVER:
                    self._release_in_background(scope, token)
                raise
        self._forget_slot()
        if self._loss_reason == LOSS_TAKEN_OVER:
            return
        await self._release_scope(scope, token, deadline_seconds=deadline_seconds)

    def _forget_slot(self) -> None:
        self.index = None
        self._token = None

    def _end_refresher(self) -> asyncio.Task[None] | None:
        """Mark the refresher's stop as requested, cancel it and return its task (``None`` when none runs)."""
        refresher, self._refresher = self._refresher, None
        stop, self._refresh_stop = self._refresh_stop, None
        if stop is not None:
            stop.requested = True
        if refresher is not None:
            refresher.cancel()
        return refresher

    async def _release_scope(self, scope: str, token: str, *, deadline_seconds: float = SLOT_ACQUIRE_SECONDS) -> None:
        """Release ``scope`` for ``token``; when this is cancelled, the release goes on in the background.

        The slot is no longer recorded here when this runs, so a release that a
        cancellation stopped would leave the store holding it with no owner
        until the lease ends.
        """
        try:
            await self._send_release(scope, token, deadline_seconds=deadline_seconds)
        except asyncio.CancelledError:
            self._release_in_background(scope, token)
            raise

    async def _send_release(self, scope: str, token: str, *, deadline_seconds: float = SLOT_ACQUIRE_SECONDS) -> None:
        provider = self._election.provider
        await self._caller.call(
            'release',
            lambda: provider.release(scope, token),
            deadline_seconds=deadline_seconds,
        )

    def _release_in_background(self, scope: str, token: str) -> None:
        """Release a slot whose release or acquire was cancelled, without waiting for it."""
        task = asyncio.create_task(self._send_release(scope, token), name=f'drakkar-detach-release-{scope}')
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _refresh_loop(
        self,
        scope: str,
        token: str,
        stop: _StopRequest,
        on_lost: Callable[[str], Awaitable[None]],
    ) -> None:
        try:
            reason = await self._refresh_until_lost(scope, token)
        except asyncio.CancelledError:
            if not stop.requested:
                # Cancelled by something other than release() or stop_refreshing():
                # nothing refreshes the slot any more, so the loss is reported.
                await self._report_loss(scope, LOSS_EXPIRED, on_lost)
            raise
        except Exception as exc:
            # Nothing awaits this task, so its error is logged here or never; the
            # loss is still reported, so the worker does not stay detached unrefreshed.
            await _log_error('detach_lease_refresh_failed', scope=scope, exc_info=exc)
            reason = LOSS_EXPIRED
        await self._report_loss(scope, reason, on_lost)

    async def _report_loss(self, scope: str, reason: str, on_lost: Callable[[str], Awaitable[None]]) -> None:
        """Record the loss and call ``on_lost`` once; an error of the callback is logged."""
        self._loss_reason = reason
        # Forget this task first: on_lost may call release(), which must not wait for its own task.
        if self._refresher is asyncio.current_task():
            self._refresher = None
            self._refresh_stop = None
        try:
            await on_lost(reason)
        except Exception as exc:
            await _log_error('detach_lease_loss_report_failed', scope=scope, reason=reason, exc_info=exc)

    async def _refresh_until_lost(self, scope: str, token: str) -> str:
        """Refresh until the slot is lost; the reason, ``taken_over`` or ``expired``."""
        refresh_seconds = lease_refresh_seconds(self._lease_seconds)
        provider = self._election.provider
        while True:
            lease_end = self._confirmed_at + self._lease_seconds
            # After a failed refresh the next try comes at the next refresh time or
            # the lease end, whichever is first; an open breaker cannot make it spin.
            wait = min(refresh_seconds, lease_end - self._clock.time())
            if wait > 0:
                await self._clock.sleep(wait)
            asked_at = self._clock.time()
            if asked_at >= lease_end:
                return LOSS_EXPIRED
            result = await self._caller.call(
                'refresh',
                lambda: provider.refresh(scope, token),
                deadline_seconds=lease_end - asked_at,
            )
            reason = lease_loss_reason(result)
            if reason is None:
                self._confirmed_at = asked_at
                continue
            if reason == LOSS_TAKEN_OVER:
                return LOSS_TAKEN_OVER
            await logger.awarning(
                'detach_lease_unconfirmed',
                category='kafka',
                scope=scope,
                seconds_left=self.seconds_left,
            )
