"""Cluster periodic tasks: each slot of a task runs on one worker of the cluster.

Every worker runs one loop per enabled cluster task, and every worker
computes the same slots (:mod:`drakkar.schedule`). At a slot ``S`` the loop
takes a lease on the task's scope ``<cluster>/<task>`` from the election
provider. The worker that gets it runs the task, holds the lease for the
whole run and at least until ``S + MIN_HOLD_SECONDS``, and then releases it.
The others skip the slot.

A worker may take the lease only before ``S + ACQUIRE_WINDOW_SECONDS`` by
its own clock, and the winner holds it until at least ``S +
MIN_HOLD_SECONDS`` by its own clock. So a worker whose clock is behind by
less than the difference of the two (10 seconds) finds the slot still held
and cannot run it a second time.

The worker owns one :class:`ClusterElection`: one provider and one resilient
caller for every cluster task, so the breaker and the rate-limited failure
log span the tasks. The provider starts at the first slot that needs it.

Each worker publishes the hash of its cluster task definitions in its
registry card (``features.periodic.cluster_hash``); :class:`ClusterHashChecker`
compares it with the online peers of the worker's cluster after each
registry refresh.
"""

from __future__ import annotations

import asyncio
import re
import secrets
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.config.ui import PERSISTENCE_SQLITE
from drakkar.metrics import periodic_cluster_mismatch_peers, periodic_cluster_slots
from drakkar.periodic import SYSTEM_CLOCK, Clock, OnErrorPolicy, run_task_once, sleep_until
from drakkar.providers.resilience import (
    OUTCOME_NOT_STARTED,
    OUTCOME_OK,
    CallResult,
    RateLimitedLog,
    ResilientCaller,
)
from drakkar.recorder.election import (
    ELECTION_FEATURE,
    LOSS_TAKEN_OVER,
    ElectionContext,
    ElectionProvider,
    lease_loss_reason,
    lease_refresh_seconds,
)
from drakkar.schedule import (
    ACQUIRE_WINDOW_SECONDS,
    MIN_HOLD_SECONDS,
    next_cron_slot,
    next_interval_slot,
    task_scope,
)

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig
    from drakkar.fleet.service import PeerView
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# What became of one slot on this worker (the ``outcome`` label and log field).
SLOT_WON = 'won'
SLOT_HELD_ELSEWHERE = 'held_elsewhere'
SLOT_LATE = 'late'
SLOT_ELECTION_FAILED = 'election_failed'

# A peer's definition hash counts only in this exact form; anything else is unknown.
_CLUSTER_HASH = re.compile(r'[0-9a-f]{16}')

# The answer of ensure_started once the provider runs: no call was made.
_ALREADY_STARTED = CallResult(ok=True, value=None, outcome=OUTCOME_OK, attempts=0)
# The answer of ensure_started after stop: a provider is single-use and is not started again.
_STOPPED = CallResult(ok=False, value=None, outcome=OUTCOME_NOT_STARTED, attempts=0)


def archive_task_runs(config: DrakkarConfig) -> bool:
    """Whether this worker runs the system cluster task ``history.archive``.

    It needs the recorder (``ui.enabled``), archiving on, a recorder file
    (effective persistence ``sqlite``), and a storage folder whose sealed
    files reach the shared history folder.
    """
    return (
        config.ui.enabled
        and config.history.archive.enabled
        and config.resolved_persistence == PERSISTENCE_SQLITE
        and bool(config.storage.dir)
    )


class ClusterElection:
    """The worker's one election provider and its caller, shared by every cluster task.

    The provider starts lazily: :meth:`ensure_started` starts it once, and
    concurrent first slots wait for that one start instead of starting it
    again. :meth:`stop` stops it once, with one attempt.
    """

    def __init__(
        self,
        provider: ElectionProvider,
        caller: ResilientCaller,
        *,
        provider_name: str,
        context: ElectionContext,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.provider = provider
        self.caller = caller
        self.provider_name = provider_name
        self._context = context
        self._clock = clock
        self._start_lock = asyncio.Lock()
        self._started = False
        self._stopped = False

    async def ensure_started(self, deadline_seconds: float) -> CallResult:
        """Start the provider unless it has started; the result of the start, or an ok one with no call.

        ``deadline_seconds`` bounds the start from the moment of this call:
        a slot that waited for another slot's start gets only the time left.
        After :meth:`stop` the provider is not started again (outcome
        ``not_started``, nothing counted).
        """
        entered = self._clock.time()
        async with self._start_lock:
            if self._started:
                return _ALREADY_STARTED
            if self._stopped:
                return _STOPPED
            time_left = deadline_seconds - (self._clock.time() - entered)
            result = await self.caller.call(
                'start', lambda: self.provider.start(self._context), deadline_seconds=time_left
            )
            if not result.ok:
                return result
            # Set before the log await: a cancellation there must not leave a
            # started provider that stop() would never stop.
            self._started = True
            await logger.ainfo(
                'provider_started',
                category='periodic',
                feature=ELECTION_FEATURE,
                provider=self.provider_name,
            )
            return result

    async def stop(self, deadline_seconds: float | None) -> None:
        """Stop the provider once, with one attempt inside ``deadline_seconds``; a provider never started is counted as a skipped stop."""
        if self._stopped:
            return
        self._stopped = True
        if not self._started:
            self.caller.skip('stop', OUTCOME_NOT_STARTED)
            return
        await self.caller.call('stop', self.provider.stop, attempts=1, deadline_seconds=deadline_seconds)


class SlotLease:
    """One slot's lease on one scope, under a token of its own; one lease serves one slot.

    Every call goes through the election's caller. While the run goes on, a
    task on the event loop refreshes the lease every
    :func:`~drakkar.recorder.election.lease_refresh_seconds` of its length;
    the first refresh that does not answer True marks the lease lost
    (:attr:`loss_reason`), calls ``on_lost`` once and stops refreshing. The
    run is not cancelled: a coroutine cannot be stopped safely in the middle.

    ``taken_over`` is final: once a refresh or a check found another holder,
    the lease is lost for good, nothing refreshes it, no later failure turns
    the reason into ``unconfirmed``, and no release is sent.
    """

    def __init__(
        self,
        election: ClusterElection,
        *,
        scope: str,
        lease_seconds: int,
        clock: Clock = SYSTEM_CLOCK,
        token: str | None = None,
    ) -> None:
        self.token = token if token is not None else secrets.token_hex(8)
        self.scope = scope
        self.lease_seconds = lease_seconds
        self._election = election
        self._clock = clock
        # True once the provider answered True to the acquire.
        self._held = False
        # True once the lease is lost for good (a refresh did not answer True,
        # or a check found another holder); is_held then answers False without a call.
        self._lost = False
        self._loss_reason: str | None = None
        self._refresher: asyncio.Task[None] | None = None
        # True while the refresher is in a refresh call or its loss report: a
        # release waits for it then, so the answer and the log line are not lost.
        self._refresher_busy = False
        # Set when the refresher is asked to end; it ends after the call in progress.
        self._refresh_stop_requested = False

    @property
    def election(self) -> ClusterElection:
        """The election service the lease's calls go through."""
        return self._election

    @property
    def loss_reason(self) -> str | None:
        """Why the lease reads as not held: ``taken_over`` or ``unconfirmed``; ``None`` before."""
        return self._loss_reason

    async def acquire(self, deadline_seconds: float, *, release_by: float | None = None) -> tuple[str, str | None]:
        """Take the lease within ``deadline_seconds``; the slot outcome and, for ``election_failed``, an error text.

        ``won`` when the provider answered True, ``held_elsewhere`` when it
        answered False. When the call failed after an attempt, the provider
        may still have taken the lease, so it is released at once rather
        than left to expire; the error text is ``acquire <caller outcome>``.
        That release must end by the clock time ``release_by`` when one is
        given (the slot loop gives the end of the slot's minimum hold, so
        the release cannot make the loop miss its next slot); with no time
        left it makes no call and is counted as ``timeout``.
        """
        provider = self._election.provider
        caller = self._election.caller
        result = await caller.call(
            'acquire',
            lambda: provider.acquire(self.scope, self.token, self.lease_seconds),
            deadline_seconds=deadline_seconds,
        )
        if not result.ok:
            # A spent deadline or an open breaker made no call: there is nothing to give back.
            if result.attempts > 0:
                release_deadline = None if release_by is None else max(release_by - self._clock.time(), 0.0)
                await caller.call(
                    'release',
                    lambda: provider.release(self.scope, self.token),
                    deadline_seconds=release_deadline,
                )
            return SLOT_ELECTION_FAILED, f'acquire {result.outcome}'
        if result.value is not True:
            return SLOT_HELD_ELSEWHERE, None
        self._held = True
        return SLOT_WON, None

    def start_refreshing(self, on_lost: Callable[[str], Awaitable[None]]) -> None:
        """Refresh the lease on the event loop until :meth:`release` or :meth:`stop_refreshing`, or until it is lost."""
        self._refresher = asyncio.create_task(self._refresh_loop(on_lost), name=f'drakkar-lease-refresh-{self.scope}')

    async def is_held(self) -> bool:
        """True while the provider confirms the lease is still this slot's (the fencing check)."""
        if self._lost:
            return False
        caller = self._election.caller
        result = await caller.call('is_held', lambda: self._election.provider.is_held(self.scope, self.token))
        reason = lease_loss_reason(result)
        if reason is None:
            return True
        self._record_loss(reason)
        if reason == LOSS_TAKEN_OVER:
            # Another token holds the scope: refreshing it again is pointless.
            self._lost = True
            self._end_refresher()
        return False

    async def release(self) -> None:
        """Stop refreshing, then release the lease at the provider while it may still be this slot's.

        No release is sent when the provider answered clearly that the lease
        is not this slot's: the acquire was refused, or a refresh or a check
        found another holder (``taken_over``). A lease lost as
        ``unconfirmed`` may still be held, so it is released.
        """
        refresher = self._end_refresher()
        if refresher is not None:
            # asyncio.wait, not await: a cancellation of this release must still reach its caller.
            await asyncio.wait({refresher})
        if not self._held or self._loss_reason == LOSS_TAKEN_OVER:
            return
        self._held = False
        await self._election.caller.call('release', lambda: self._election.provider.release(self.scope, self.token))

    def stop_refreshing(self) -> None:
        """Stop the refresh task at once, releasing nothing (shutdown: the lease ends by itself)."""
        if self._refresher is not None:
            self._refresher.cancel()
            self._refresher = None

    def _end_refresher(self) -> asyncio.Task[None] | None:
        """Ask the refresher to end and return its task: cancelled while it sleeps, else after its call and report."""
        refresher, self._refresher = self._refresher, None
        if refresher is not None:
            self._refresh_stop_requested = True
            if not self._refresher_busy:
                refresher.cancel()
        return refresher

    def _record_loss(self, reason: str) -> None:
        """Keep why the lease is lost; a confirmed ``taken_over`` is never replaced."""
        self._loss_reason = LOSS_TAKEN_OVER if LOSS_TAKEN_OVER in (self._loss_reason, reason) else reason

    async def _refresh_loop(self, on_lost: Callable[[str], Awaitable[None]]) -> None:
        try:
            await self._refresh_until_lost(on_lost)
        except Exception as exc:
            # Nothing awaits this task's result, so its error is logged here or never.
            await logger.aexception(
                'periodic_lease_refresh_failed', category='periodic', scope=self.scope, exc_info=exc
            )

    async def _refresh_until_lost(self, on_lost: Callable[[str], Awaitable[None]]) -> None:
        refresh_seconds = lease_refresh_seconds(self.lease_seconds)
        caller = self._election.caller
        provider = self._election.provider
        while not self._refresh_stop_requested:
            self._refresher_busy = False
            await self._clock.sleep(refresh_seconds)
            self._refresher_busy = True
            reason = lease_loss_reason(await caller.call('refresh', lambda: provider.refresh(self.scope, self.token)))
            if reason is not None:
                # Written before the callback, so the run sees the loss as soon as it is reported.
                self._record_loss(reason)
                self._lost = True
                await on_lost(self._loss_reason or reason)
                return


@dataclass(frozen=True)
class SlotContext:
    """What a cluster task's run gets: its slot (epoch second) and the slot's lease."""

    slot: int
    lease: SlotLease


@dataclass(frozen=True)
class ClusterTask:
    """One cluster task as its loop runs it.

    ``run`` is the body of one won slot. ``on_skip`` (optional) is called
    with the outcome of every slot this worker does not win; it must not
    block. ``system`` marks a task of the framework itself in the recorder.
    """

    name: str
    run: Callable[[SlotContext], Awaitable[None]]
    interval: int | None
    cron: str | None
    lease_seconds: int
    on_error: OnErrorPolicy
    system: bool = False
    on_skip: Callable[[str], None] | None = None

    def next_slot(self, now: float) -> int:
        """The first slot of this task strictly after ``now`` truncated to the second."""
        if self.interval is not None:
            return next_interval_slot(now, self.interval)
        return next_cron_slot(now, self.cron or '')


async def run_cluster_task(
    task: ClusterTask,
    election: ClusterElection,
    *,
    cluster: str,
    recorder: EventRecorder | None = None,
    clock: Clock = SYSTEM_CLOCK,
) -> None:
    """Run one cluster task's slot loop until it is cancelled, or until a failed run with ``on_error='stop'``.

    At each slot: sleep until it (a sleep that wakes early sleeps again),
    skip it as ``late`` once the acquire window has passed, start the
    provider if it has not started, take the lease, and run the task when
    this worker won it. Slots that pass during a run are skipped; a slot
    never runs twice on this worker, even when the clock steps back.

    Cancelling the loop while it holds a lease stops the refresh and
    releases nothing: the lease ends by itself, because an early release
    could let a worker whose clock is behind run the slot again.
    """
    scope = task_scope(cluster, task.name)
    schedule: dict[str, int | str] = (
        {'interval_seconds': task.interval} if task.interval is not None else {'cron': task.cron or ''}
    )
    log = logger.bind(periodic_task=task.name, **schedule)
    await log.ainfo('periodic_task_started', category='periodic', scope='cluster')
    # At most one warning line of this task's skipped slots per minute of its clock.
    warnings = RateLimitedLog(clock.time)
    last_slot = 0
    while True:
        # max() keeps a clock that stepped back from returning the slot just run.
        slot = task.next_slot(max(clock.time(), last_slot))
        await sleep_until(clock, slot)
        last_slot = slot
        now = clock.time()
        if now >= slot + ACQUIRE_WINDOW_SECONDS:
            await _skip_slot(task, slot, SLOT_LATE, warnings, late_seconds=round(now - slot, 3))
            continue
        lease = SlotLease(election, scope=scope, lease_seconds=task.lease_seconds, clock=clock)
        outcome, error = await _contest_slot(election, lease, slot, clock)
        if outcome != SLOT_WON:
            await _skip_slot(task, slot, outcome, warnings, error=error)
            continue
        periodic_cluster_slots.labels(name=task.name, outcome=SLOT_WON).inc()
        if await _run_won_slot(task, lease, slot, recorder=recorder, clock=clock, log=log):
            return


async def _contest_slot(election: ClusterElection, lease: SlotLease, slot: int, clock: Clock) -> tuple[str, str | None]:
    """Start the provider if needed and take the lease, both before the acquire window ends.

    A release after an unclear acquire must end by ``slot + MIN_HOLD_SECONDS``:
    after that the lease is no longer this slot's to give back, and the next
    slot may already be due.
    """
    window_end = slot + ACQUIRE_WINDOW_SECONDS
    started = await election.ensure_started(window_end - clock.time())
    if not started.ok:
        return SLOT_ELECTION_FAILED, f'start {started.outcome}'
    return await lease.acquire(window_end - clock.time(), release_by=slot + MIN_HOLD_SECONDS)


async def _skip_slot(
    task: ClusterTask,
    slot: int,
    outcome: str,
    warnings: RateLimitedLog,
    *,
    error: str | None = None,
    late_seconds: float | None = None,
) -> None:
    """Count and log a slot this worker does not run, and tell the task's ``on_skip`` hook."""
    periodic_cluster_slots.labels(name=task.name, outcome=outcome).inc()
    if task.on_skip is not None:
        try:
            task.on_skip(outcome)
        except Exception as exc:
            # A failing hook must not end the task's loop; the slot is skipped either way.
            await logger.aexception(
                'periodic_skip_hook_failed',
                category='periodic',
                name=task.name,
                slot=slot,
                outcome=outcome,
                exc_info=exc,
            )
    fields: dict[str, Any] = {'category': 'periodic', 'name': task.name, 'slot': slot, 'outcome': outcome}
    if outcome == SLOT_HELD_ELSEWHERE:
        # The normal case on every worker but one: no warning, no rate limit.
        logger.debug('periodic_slot_skipped', **fields)
        return
    if error is not None:
        fields['error'] = error
    if late_seconds is not None:
        fields['late_seconds'] = late_seconds
    await warnings.emit(task.name, logger.awarning, 'periodic_slot_skipped', **fields)


async def _run_won_slot(
    task: ClusterTask,
    lease: SlotLease,
    slot: int,
    *,
    recorder: EventRecorder | None,
    clock: Clock,
    log: Any,
) -> bool:
    """Run a won slot while the lease is refreshed, hold it until ``slot + MIN_HOLD_SECONDS``, release it.

    True when the task's loop must end (a failed run with ``on_error='stop'``).
    """

    async def report_lost(reason: str) -> None:
        await logger.awarning('periodic_lease_lost', category='periodic', name=task.name, slot=slot, reason=reason)

    lease.start_refreshing(report_lost)
    try:
        must_stop = await run_task_once(
            task.name,
            lambda: task.run(SlotContext(slot=slot, lease=lease)),
            recorder=recorder,
            system=task.system,
            on_error=task.on_error,
            log=log,
            slot=slot,
        )
        await sleep_until(clock, slot + MIN_HOLD_SECONDS)
    except BaseException:
        # Cancelled (shutdown) or broken: stop refreshing and release nothing;
        # the lease ends by itself, so no worker can run this slot again early.
        lease.stop_refreshing()
        raise
    await lease.release()
    return must_stop


def card_cluster_hash(card: Mapping[str, Any]) -> str | None:
    """The cluster task hash a registry card publishes, or ``None`` when it publishes none in the contract form.

    The hash is ``features.periodic.cluster_hash``: 16 lowercase hex
    characters. A card without ``features``, a ``features`` or ``periodic``
    that is not an object, or a hash of another form gives ``None``
    (unknown), never an error.
    """
    features = card.get('features')
    if not isinstance(features, Mapping):
        return None
    periodic = features.get('periodic')
    if not isinstance(periodic, Mapping):
        return None
    cluster_hash = periodic.get('cluster_hash')
    if not isinstance(cluster_hash, str) or _CLUSTER_HASH.fullmatch(cluster_hash) is None:
        return None
    return cluster_hash


class ClusterHashChecker:
    """Compares this worker's cluster task hash with the online peers of its cluster.

    :meth:`check` runs after each registry refresh. A peer counts when the
    registry calls it online, its card names exactly this worker's cluster,
    and its card carries a hash (:func:`card_cluster_hash`); a peer whose
    hash is unknown is not a mismatch. Each check sets
    ``drakkar_periodic_cluster_mismatch_peers`` to the count of peers whose
    hash differs and logs ``periodic_cluster_mismatch`` once per peer and
    peer hash while the peer stays in the registry listing. The record of
    logged pairs keeps only the peers of the current listing, so a peer
    that leaves the listing and comes back is logged again.
    """

    def __init__(self, own_hash: str, own_cluster: str, logger: Any = None) -> None:
        self._own_hash = own_hash
        self._own_cluster = own_cluster
        self._logger = logger if logger is not None else structlog.get_logger()
        # (peer worker name, peer hash) pairs already logged.
        self._reported: set[tuple[str, str]] = set()

    async def check(self, views: Sequence[PeerView]) -> None:
        """Count and report the peers of this worker's cluster that run other cluster task definitions."""
        mismatched: list[tuple[str, str]] = []
        for view in views:
            if view.online is not True or view.card.get('cluster') != self._own_cluster:
                continue
            peer_hash = card_cluster_hash(view.card)
            if peer_hash is not None and peer_hash != self._own_hash:
                mismatched.append((view.card['worker'], peer_hash))
        periodic_cluster_mismatch_peers.set(len(mismatched))
        # Kept to the peers still listed, so the set stays as small as the cluster.
        listed = {view.card.get('worker') for view in views}
        self._reported = {pair for pair in self._reported if pair[0] in listed}
        for peer, peer_hash in mismatched:
            if (peer, peer_hash) in self._reported:
                continue
            self._reported.add((peer, peer_hash))
            await self._logger.awarning(
                'periodic_cluster_mismatch',
                category='periodic',
                peer=peer,
                own_hash=self._own_hash,
                peer_hash=peer_hash,
            )
