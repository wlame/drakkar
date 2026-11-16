"""Detach a worker from its Kafka consumer group for a bounded time, and attach it again.

The controller is the state machine of spec states ``attached``, ``detaching``,
``detached`` and ``attaching``. A detach takes one of ``max_detached`` lease
slots in the autodiscover store (:mod:`drakkar.detach_slots`), checks that
enough workers of the group stay attached (:mod:`drakkar.detach_group`), and
leaves the group through the revoke path of the Kafka source. An attach joins
the group again and then gives the slot back.

Rules that hold on every path:

- One transition runs at a time, under one lock. Each transition runs in a
  task that the controller owns. A caller awaits it through a shield, so a
  caller that is cancelled cannot stop a transition half way: a cancelled
  attach still finishes, and a cancelled detach that is still in progress is
  undone.
- Every exit of a detach that is not a clean ``detached`` ends with the
  source joined again (unless the worker is stopping) and the slot released.
- A join that fails, returns False without a stop, or passes its bound is
  tried again every :data:`JOIN_RETRY_SECONDS` until it succeeds or the worker
  stops. Until then the state stays ``attaching`` and the slot stays held and
  refreshed: the worker really is outside the group.
- While ``detached``, two things bring a forgotten worker back: the deadline
  timer and the lease refresher of the slot. An error of either attaches the
  worker at once.
- The safety rules (deadline, lease lost, too few workers) never wait behind a
  slow detach: a detach in progress is told to stop, and undoes itself.

Every method runs on the app's main event loop; the operator routes reach
:meth:`DetachController.request` through ``dispatch_to_loop``. :attr:`state`
is one immutable object, safe to read from any thread.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.detach_group import GroupView, group_view, mismatched_peers
from drakkar.detach_slots import DETACH_FEATURE, DetachSlot, SlotAcquire, slot_scope
from drakkar.metrics import attach_total, detach_requests, detached
from drakkar.periodic import SYSTEM_CLOCK, Clock
from drakkar.providers.resilience import ResilientCaller
from drakkar.sources.kafka import LEAVE_MARGIN_SECONDS, KafkaSource, LeaveOutcome
from drakkar.timefmt import format_rfc3339_micro

if TYPE_CHECKING:
    from collections.abc import Coroutine, Sequence

    from drakkar.app import DrakkarApp
    from drakkar.config import KafkaDetachConfig
    from drakkar.fleet.service import PeerView

logger = structlog.get_logger()

# The states of the Kafka source of this worker.
STATE_ATTACHED = 'attached'
STATE_DETACHING = 'detaching'
STATE_DETACHED = 'detached'
STATE_ATTACHING = 'attaching'

# Refusal reasons of a detach (and of an attach: not_running, stopping, held_by_operator).
REFUSED_NOT_RUNNING = 'not_running'
REFUSED_STOPPING = 'stopping'
REFUSED_DISABLED = 'disabled'
REFUSED_INSIDE_PIPELINE_HOOK = 'inside_pipeline_hook'
REFUSED_ALREADY_DETACHED = 'already_detached'
REFUSED_COOLDOWN = 'cooldown'
REFUSED_TOO_FEW_WORKERS = 'too_few_workers'
REFUSED_BUDGET_FULL = 'budget_full'
REFUSED_STORE_UNAVAILABLE = 'store_unavailable'
REFUSED_LEAVE_FAILED = 'leave_failed'
REFUSED_HELD_BY_OPERATOR = 'held_by_operator'

# The causes of an attach (the ``cause`` of ``worker_attached``).
CAUSE_HANDLER = 'handler'
CAUSE_OPERATOR = 'operator'
CAUSE_DEADLINE = 'deadline'
CAUSE_LEASE_LOST = 'lease_lost'
CAUSE_TOO_FEW_WORKERS = 'too_few_workers'

# The outcomes of ``last_request`` that are not a refusal.
OUTCOME_DETACHED = 'detached'
OUTCOME_ATTACHED = 'attached'

# The actions of an operator request.
ACTION_DETACH = 'detach'
ACTION_ATTACH = 'attach'

MIN_DURATION_SECONDS = 10.0
REASON_MAX_CHARS = 200
# The pause between two tries of a join that failed.
JOIN_RETRY_SECONDS = 5.0

# What a detach in progress answers when a safety rule or a cancelled caller stops it.
_ABORT_REASONS = {
    CAUSE_LEASE_LOST: REFUSED_STORE_UNAVAILABLE,
    CAUSE_TOO_FEW_WORKERS: REFUSED_TOO_FEW_WORKERS,
    CAUSE_DEADLINE: REFUSED_LEAVE_FAILED,
}

# A slot acquire that was not won, as the refusal it gives.
_ACQUIRE_REFUSALS = {
    SlotAcquire.BUDGET_FULL: REFUSED_BUDGET_FULL,
    SlotAcquire.STORE_UNAVAILABLE: REFUSED_STORE_UNAVAILABLE,
}


class DetachOrigin(StrEnum):
    """Who asked for a detach."""

    HANDLER = 'handler'
    OPERATOR = 'operator'


@dataclass(frozen=True)
class DetachState:
    """The detach state of this worker: the state, who detached it, why, since when and until when."""

    state: str
    origin: DetachOrigin | None
    reason: str
    since: datetime | None
    until: datetime | None


@dataclass(frozen=True)
class DetachResult:
    """The answer of a detach: whether the worker is detached now, the refusal reason, and the deadline."""

    detached: bool
    reason: str
    until: datetime | None


@dataclass(frozen=True)
class AttachResult:
    """The answer of an attach: whether the worker is subscribed now, and the refusal reason."""

    attached: bool
    reason: str


ATTACHED_STATE = DetachState(state=STATE_ATTACHED, origin=None, reason='', since=None, until=None)


def check_duration(duration: float | None, config: KafkaDetachConfig) -> float:
    """The detach duration in seconds: ``default_duration`` for ``None``, else ``duration`` when it is in range.

    Raises:
        ValueError: ``duration`` is below :data:`MIN_DURATION_SECONDS` or above ``max_duration``.
    """
    if duration is None:
        return float(config.default_duration)
    seconds = float(duration)
    if not MIN_DURATION_SECONDS <= seconds <= config.max_duration:
        raise ValueError(
            f'duration must be from {MIN_DURATION_SECONDS:g} to {config.max_duration:g} seconds, got {duration}'
        )
    return seconds


def _utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, UTC)


def _epoch_ms(moment: datetime | None) -> int | None:
    return None if moment is None else int(moment.timestamp() * 1000)


class _Transition:
    """One detach request in progress; :meth:`abort` tells it to stop and undo itself."""

    def __init__(self) -> None:
        self.stopped: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    def abort(self, reason: str) -> None:
        """Ask the detach to stop and answer ``reason``; the first reason wins."""
        if not self.stopped.done():
            self.stopped.set_result(reason)

    @property
    def abort_reason(self) -> str | None:
        return self.stopped.result() if self.stopped.done() else None


class _TimerStop:
    """Set when the controller itself ends the deadline timer."""

    def __init__(self) -> None:
        self.requested = False


class DetachController:
    """The detach state machine of one worker; built for every app, as the consume-pause controller is."""

    def __init__(self, app: DrakkarApp, *, clock: Clock = SYSTEM_CLOCK) -> None:
        self._app = app
        self._clock = clock
        self._state = ATTACHED_STATE
        self._lock = asyncio.Lock()
        # The slot held from a won acquire until its release.
        self._slot: DetachSlot | None = None
        self._caller: ResilientCaller | None = None
        # The detach that holds the lock now; a safety rule aborts it.
        self._current: _Transition | None = None
        self._timer: asyncio.Task[None] | None = None
        self._timer_stop: _TimerStop | None = None
        # Clock times: when the worker became detached, and when it last attached (for the cooldown).
        self._detached_at: float | None = None
        self._last_attach_at: float | None = None
        self._last_request: dict[str, Any] | None = None
        self._stopped = False
        # Every task this controller started; a task with no reference could be collected while it runs.
        self._tasks: set[asyncio.Task[Any]] = set()
        # (peer, max_detached, min_attached) already logged as a mismatch.
        self._mismatch_reported: set[tuple[str, int, int]] = set()

    # --- reading -----------------------------------------------------------

    @property
    def state(self) -> DetachState:
        """The detach state now, one immutable object."""
        return self._state

    @property
    def is_detached(self) -> bool:
        """Whether the worker is outside its group or on its way: ``detaching``, ``detached`` or ``attaching``."""
        return self._state.state != STATE_ATTACHED

    def wire_state(self) -> dict[str, Any]:
        """The body of ``GET /api/v1/debug/detach``."""
        config = self._app.config
        detach = config.sources.kafka.detach
        state = self._state
        return {
            'enabled': config.sources.kafka.enabled and detach.enabled,
            'ui_enabled': config.ui.detach_enabled,
            'state': state.state,
            'origin': None if state.origin is None else str(state.origin),
            'reason': state.reason,
            'since_ms': _epoch_ms(state.since),
            'until_ms': _epoch_ms(state.until),
            'max_duration_seconds': detach.max_duration,
            'default_duration_seconds': detach.default_duration,
            'last_request': self._last_request,
        }

    # --- requests ----------------------------------------------------------

    async def detach(
        self,
        *,
        origin: DetachOrigin,
        duration: float | None,
        reason: str,
        in_pipeline_hook: bool = False,
    ) -> DetachResult:
        """Leave the consumer group for ``duration`` seconds, or answer why not.

        Returns when the revoke drain has ended and no partition is left on
        this worker, or with a refusal (spec order: ``not_running``,
        ``stopping``, ``disabled``, ``inside_pipeline_hook``,
        ``already_detached``, ``cooldown``, ``too_few_workers``,
        ``budget_full``, ``store_unavailable``, ``leave_failed``).

        Raises:
            ValueError: ``duration`` is out of range (:func:`check_duration`).
        """
        seconds = check_duration(duration, self._detach_config())
        reason = reason[:REASON_MAX_CHARS]
        asked_at = self._clock.time()
        refusal = self._early_detach_refusal(in_pipeline_hook=in_pipeline_hook)
        if refusal is not None:
            result = DetachResult(detached=False, reason=refusal, until=None)
            await self._account_detach(origin, result, asked_at)
            return result
        transition = _Transition()
        task = self._spawn(
            self._detach_job(transition, origin=origin, seconds=seconds, reason=reason, asked_at=asked_at),
            name='drakkar-detach',
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # The caller cannot learn the outcome any more: a detach still in progress is undone.
            transition.abort(REFUSED_LEAVE_FAILED)
            raise

    async def attach(self, *, cause: str, reason: str = '') -> AttachResult:
        """Join the consumer group again; ``cause`` is ``handler`` or ``operator``.

        Returns when the join has seen one poll and the slot is released. A
        handler attach does not end an operator detach (``held_by_operator``).
        A cancelled caller does not stop the attach.
        """
        asked_at = self._clock.time()
        source = self._source()
        refusal = None
        if source is not None and not source.is_ready:
            refusal = REFUSED_NOT_RUNNING
        elif self._is_stopping():
            refusal = REFUSED_STOPPING
        if refusal is not None:
            result = AttachResult(attached=False, reason=refusal)
            self._note_request(cause, ACTION_ATTACH, refusal, asked_at)
            return result
        task = self._spawn(
            self._attach_job(cause=cause, reason=reason[:REASON_MAX_CHARS], asked_at=asked_at),
            name='drakkar-attach',
        )
        return await asyncio.shield(task)

    def request(self, *, action: str, duration: float | None = None, reason: str = '') -> None:
        """Start an operator detach or attach on the main loop and return at once.

        The outcome shows in ``last_request`` of :meth:`wire_state` when it is known.

        Raises:
            ValueError: an unknown ``action``, or a ``duration`` out of range.
        """
        if action == ACTION_DETACH:
            check_duration(duration, self._detach_config())
            coro: Coroutine[Any, Any, Any] = self.detach(origin=DetachOrigin.OPERATOR, duration=duration, reason=reason)
        elif action == ACTION_ATTACH:
            coro = self.attach(cause=CAUSE_OPERATOR, reason=reason)
        else:
            raise ValueError(f'action must be {ACTION_DETACH} or {ACTION_ATTACH}, got {action!r}')
        self._spawn(self._guarded(coro, event='detach_request_failed'), name=f'drakkar-{action}-request')

    # --- registry and stop ---------------------------------------------------

    async def on_registry_refresh(self, views: list[PeerView]) -> None:
        """After each registry refresh: log the peers with other limits, and attach when too few peers stay attached."""
        if not self._feature_on():
            return
        config = self._detach_config()
        await self._log_mismatches(views, config)
        if self._current is None and self._state.state != STATE_DETACHED:
            return
        if self._group_of(views).attached_peers < config.min_attached:
            self._safety_attach(CAUSE_TOO_FEW_WORKERS)

    async def release_at_stop(self, time_left: float) -> None:
        """At a stop, after the sources stopped: end the timer and the transitions, then one release of a held slot."""
        loop = asyncio.get_running_loop()
        ends_at = loop.time() + time_left
        self._stopped = True
        self._cancel_timer()
        current = asyncio.current_task()
        pending = [task for task in self._tasks if task is not current and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=max(ends_at - loop.time(), 0.0))
        slot, self._slot = self._slot, None
        if slot is None or slot.index is None:
            return
        await slot.release(deadline_seconds=max(ends_at - loop.time(), 0.0))

    # --- the detach transition -------------------------------------------------

    async def _detach_job(
        self, transition: _Transition, *, origin: DetachOrigin, seconds: float, reason: str, asked_at: float
    ) -> DetachResult:
        async with self._lock:
            self._current = transition
            try:
                result = await self._detach_locked(transition, origin=origin, seconds=seconds, reason=reason)
            except Exception:
                await logger.aexception('detach_transition_failed', category='kafka', origin=str(origin))
                result = DetachResult(detached=False, reason=REFUSED_LEAVE_FAILED, until=None)
            finally:
                self._current = None
        await self._account_detach(origin, result, asked_at)
        return result

    async def _detach_locked(
        self, transition: _Transition, *, origin: DetachOrigin, seconds: float, reason: str
    ) -> DetachResult:
        if transition.abort_reason is not None:
            return self._refused(transition.abort_reason)
        if self._is_stopping():
            return self._refused(REFUSED_STOPPING)
        config = self._detach_config()
        state = self._state
        if state.state == STATE_DETACHED:
            return self._detach_again(state, origin=origin, seconds=seconds, reason=reason)
        if state.state != STATE_ATTACHED:
            # Only a stop leaves another state behind a transition.
            return self._refused(REFUSED_STOPPING)
        now = self._clock.time()
        if (
            origin == DetachOrigin.HANDLER
            and self._last_attach_at is not None
            and now - self._last_attach_at < config.cooldown
        ):
            return self._refused(REFUSED_COOLDOWN)
        if self._group_now().attached_peers < config.min_attached:
            return self._refused(REFUSED_TOO_FEW_WORKERS)
        slot = self._new_slot()
        if slot is None:
            return self._refused(REFUSED_STORE_UNAVAILABLE)
        acquired = await slot.acquire()
        if acquired != SlotAcquire.WON:
            return self._refused(_ACQUIRE_REFUSALS[acquired])
        self._slot = slot
        try:
            return await self._leave_with_slot(transition, slot, origin=origin, seconds=seconds, reason=reason)
        except asyncio.CancelledError:
            # Cancelled only by the stop, which releases the slot.
            raise
        except Exception:
            await logger.aexception('detach_transition_failed', category='kafka', origin=str(origin))
            await self._undo_leave(slot)
            return self._refused(REFUSED_LEAVE_FAILED)

    def _detach_again(self, state: DetachState, *, origin: DetachOrigin, seconds: float, reason: str) -> DetachResult:
        """A detach of a detached worker: a handler leaves an operator detach alone, else the deadline is replaced."""
        if origin == DetachOrigin.HANDLER and state.origin == DetachOrigin.OPERATOR:
            return DetachResult(detached=True, reason=REFUSED_ALREADY_DETACHED, until=state.until)
        until = self._clock.time() + min(seconds, self._detach_config().max_duration)
        self._state = DetachState(
            state=STATE_DETACHED, origin=origin, reason=reason, since=state.since, until=_utc(until)
        )
        self._start_timer(until)
        return DetachResult(detached=True, reason='', until=_utc(until))

    async def _leave_with_slot(
        self, transition: _Transition, slot: DetachSlot, *, origin: DetachOrigin, seconds: float, reason: str
    ) -> DetachResult:
        """With a slot won: the exact size check, then the leave, then the publish."""
        config = self._detach_config()
        held = await slot.held_count()
        if transition.abort_reason is not None:
            await self._release_slot(slot)
            return self._refused(transition.abort_reason)
        if self._group_now().size - held < config.min_attached:
            await self._release_slot(slot)
            return self._refused(REFUSED_TOO_FEW_WORKERS)
        if self._is_stopping():
            await self._release_slot(slot)
            return self._refused(REFUSED_STOPPING)
        source = self._source()
        assert source is not None
        # The lease may be shorter than the leave, so the slot is refreshed from now on.
        slot.start_refreshing(self._on_slot_lost)
        self._state = DetachState(
            state=STATE_DETACHING, origin=origin, reason=reason, since=_utc(self._clock.time()), until=None
        )
        leave = asyncio.create_task(source.leave_group(), name='drakkar-detach-leave')
        await asyncio.wait({leave, transition.stopped}, return_when=asyncio.FIRST_COMPLETED)
        if not leave.done():
            leave.cancel()
            await asyncio.wait({leave})
            await self._undo_leave(slot)
            return self._refused(transition.abort_reason or REFUSED_LEAVE_FAILED)
        outcome = leave.result()
        if outcome == LeaveOutcome.STOPPING:
            # The stop drains the partitions; nothing subscribes again.
            await self._release_slot(slot)
            return self._refused(REFUSED_STOPPING)
        if outcome == LeaveOutcome.TIMED_OUT:
            await self._undo_leave(slot)
            return self._refused(REFUSED_LEAVE_FAILED)
        if transition.abort_reason is not None:
            await self._undo_leave(slot)
            return self._refused(transition.abort_reason)
        return await self._publish_detached(slot, origin=origin, seconds=seconds, reason=reason)

    async def _publish_detached(
        self, slot: DetachSlot, *, origin: DetachOrigin, seconds: float, reason: str
    ) -> DetachResult:
        """Enter ``detached``: the timer, the gauge, the card, the recorder row and the log line."""
        now = self._clock.time()
        until = now + seconds
        self._detached_at = now
        self._state = DetachState(
            state=STATE_DETACHED, origin=origin, reason=reason, since=_utc(now), until=_utc(until)
        )
        self._start_timer(until)
        detached.set(1)
        self._publish_card()
        slot_index = slot.index if slot.index is not None else -1
        recorder = self._app.recorder
        if recorder is not None:
            recorder.record_worker_detached(origin=str(origin), reason=reason, until=until, slot=slot_index)
        await logger.awarning(
            'worker_detached',
            category='kafka',
            origin=str(origin),
            reason=reason,
            until=format_rfc3339_micro(_utc(until)),
            slot=slot_index,
        )
        return DetachResult(detached=True, reason='', until=_utc(until))

    async def _undo_leave(self, slot: DetachSlot) -> None:
        """Bring a detach that did not end in ``detached`` back: join again (unless stopping), release the slot."""
        state = self._state
        self._state = DetachState(
            state=STATE_ATTACHING, origin=state.origin, reason=state.reason, since=state.since, until=None
        )
        source = self._source()
        joined = source is None or source.is_group_member or await self._join_until_member(source)
        await self._release_slot(slot)
        if joined:
            self._state = ATTACHED_STATE

    # --- the attach transition -------------------------------------------------

    async def _attach_job(self, *, cause: str, reason: str, asked_at: float | None) -> AttachResult:
        """Attach under the lock; ``asked_at`` is set for a handler or operator request, which ``last_request`` shows."""
        async with self._lock:
            try:
                result = await self._attach_locked(cause=cause, reason=reason)
            except Exception:
                await logger.aexception('attach_transition_failed', category='kafka', cause=cause)
                result = AttachResult(attached=False, reason=REFUSED_STOPPING)
        if asked_at is not None:
            outcome = OUTCOME_ATTACHED if result.attached else result.reason
            self._note_request(cause, ACTION_ATTACH, outcome, asked_at)
        return result

    async def _attach_locked(self, *, cause: str, reason: str) -> AttachResult:
        state = self._state
        if state.state == STATE_ATTACHED:
            return AttachResult(attached=True, reason='')
        if state.state != STATE_DETACHED or self._is_stopping():
            return AttachResult(attached=False, reason=REFUSED_STOPPING)
        if cause == CAUSE_HANDLER and state.origin == DetachOrigin.OPERATOR:
            return AttachResult(attached=False, reason=REFUSED_HELD_BY_OPERATOR)
        self._cancel_timer()
        self._state = DetachState(
            state=STATE_ATTACHING, origin=state.origin, reason=state.reason, since=state.since, until=state.until
        )
        source = self._source()
        joined = source is None or await self._join_until_member(source)
        slot = self._slot
        if slot is not None:
            await self._release_slot(slot)
        if not joined:
            return AttachResult(attached=False, reason=REFUSED_STOPPING)
        now = self._clock.time()
        detached_seconds = round(now - (self._detached_at if self._detached_at is not None else now), 3)
        self._last_attach_at = now
        self._detached_at = None
        self._state = ATTACHED_STATE
        detached.set(0)
        attach_total.labels(cause=cause).inc()
        self._publish_card()
        recorder = self._app.recorder
        if recorder is not None:
            recorder.record_worker_attached(cause=cause, detached_seconds=detached_seconds)
        await logger.awarning(
            'worker_attached', category='kafka', cause=cause, detached_seconds=detached_seconds, reason=reason
        )
        return AttachResult(attached=True, reason='')

    async def _join_until_member(self, source: KafkaSource) -> bool:
        """Join until the source is a member; False when the worker stops first.

        Each try is bounded by ``executor.drain_timeout`` plus the leave
        margin. A try that fails, answers False without a stop, or passes its
        bound is followed by another one :data:`JOIN_RETRY_SECONDS` later.
        """
        bound = self._app.config.executor.drain_timeout + LEAVE_MARGIN_SECONDS
        while True:
            if self._is_stopping():
                return False
            try:
                joined = await self._join_once(source, bound)
            except Exception as exc:
                await logger.awarning('detach_join_failed', category='kafka', error=str(exc) or type(exc).__name__)
            else:
                if joined:
                    return True
                if self._is_stopping():
                    return False
                await logger.awarning('detach_join_failed', category='kafka', error='the join did not complete')
            await self._clock.sleep(JOIN_RETRY_SECONDS)

    async def _join_once(self, source: KafkaSource, bound: float) -> bool:
        """One join, bounded by ``bound`` seconds of the clock; False when it passes its bound."""
        join = asyncio.create_task(source.join_group(), name='drakkar-detach-join')
        limit = asyncio.create_task(self._clock.sleep(bound), name='drakkar-detach-join-bound')
        try:
            await asyncio.wait({join, limit}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            limit.cancel()
            if not join.done():
                # A cancelled join leaves the source not known as a member; the next try joins again.
                join.cancel()
                await asyncio.wait({join})
        if join.cancelled():
            return False
        return join.result()

    # --- safety rules ------------------------------------------------------------

    async def _on_slot_lost(self, reason: str) -> None:
        """Called on the refresher's task when the slot is lost; the attach runs on a task of its own."""
        self._safety_attach(CAUSE_LEASE_LOST)

    def _safety_attach(self, cause: str) -> None:
        """End a detach for a safety rule without waiting: abort one in progress, or start an attach."""
        if self._is_stopping():
            return
        if self._current is not None:
            self._current.abort(_ABORT_REASONS[cause])
            return
        if self._state.state == STATE_DETACHED:
            self._spawn(
                self._guarded(
                    self._attach_job(cause=cause, reason='', asked_at=None), event='attach_transition_failed'
                ),
                name=f'drakkar-attach-{cause}',
            )

    def _start_timer(self, until: float) -> None:
        self._cancel_timer()
        stop = _TimerStop()
        self._timer_stop = stop
        self._timer = self._spawn(self._run_timer(until, stop), name='drakkar-detach-deadline')

    def _cancel_timer(self) -> None:
        timer, self._timer = self._timer, None
        stop, self._timer_stop = self._timer_stop, None
        if stop is not None:
            stop.requested = True
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()

    async def _run_timer(self, until: float, stop: _TimerStop) -> None:
        """Attach at the deadline; a timer that fails or is cancelled by anything else attaches at once."""
        try:
            await self._wait_for_deadline(until)
        except asyncio.CancelledError:
            if not stop.requested:
                self._safety_attach(CAUSE_DEADLINE)
            raise
        except Exception:
            await logger.aexception('detach_deadline_timer_failed', category='kafka')
        if stop.requested:
            return
        self._timer = None
        self._timer_stop = None
        self._safety_attach(CAUSE_DEADLINE)

    async def _wait_for_deadline(self, until: float) -> None:
        while (left := until - self._clock.time()) > 0:
            await self._clock.sleep(left)

    async def _log_mismatches(self, views: Sequence[PeerView], config: KafkaDetachConfig) -> None:
        found = mismatched_peers(
            views,
            cluster=self._app._cluster_name,
            consumer_group=self._app.config.resolved_consumer_group,
            max_detached=config.max_detached,
            min_attached=config.min_attached,
        )
        listed = {view.card.get('worker') for view in views if isinstance(view.card, dict)}
        # Kept to the peers still listed, so a peer that leaves and comes back is logged again.
        self._mismatch_reported = {item for item in self._mismatch_reported if item[0] in listed}
        for item in found:
            if item in self._mismatch_reported:
                continue
            self._mismatch_reported.add(item)
            peer, peer_max, peer_min = item
            await logger.awarning(
                'detach_config_mismatch',
                category='kafka',
                peer=peer,
                peer_max_detached=peer_max,
                peer_min_attached=peer_min,
                max_detached=config.max_detached,
                min_attached=config.min_attached,
            )

    # --- helpers -----------------------------------------------------------------

    def _detach_config(self) -> KafkaDetachConfig:
        return self._app.config.sources.kafka.detach

    def _feature_on(self) -> bool:
        kafka = self._app.config.sources.kafka
        return kafka.enabled and kafka.detach.enabled and self._source() is not None

    def _source(self) -> KafkaSource | None:
        return self._app.kafka_source

    def _is_stopping(self) -> bool:
        """Whether the worker stops: a stop began, or the source will not poll any more."""
        if self._stopped or self._app._stopping:
            return True
        source = self._source()
        return source is not None and source._poll_waits_end()

    def _early_detach_refusal(self, *, in_pipeline_hook: bool) -> str | None:
        """The refusals that need no lock and no store call, in spec order."""
        source = self._source()
        if source is not None and not source.is_ready:
            return REFUSED_NOT_RUNNING
        if self._is_stopping():
            return REFUSED_STOPPING
        if not self._feature_on():
            return REFUSED_DISABLED
        if in_pipeline_hook:
            return REFUSED_INSIDE_PIPELINE_HOOK
        return None

    def _refused(self, reason: str) -> DetachResult:
        return DetachResult(detached=False, reason=reason, until=None)

    def _group_now(self) -> GroupView:
        """The group as the last registry refresh shows it; with no registry, this worker alone."""
        fleet = self._app.fleet
        return self._group_of(fleet.peer_views() if fleet is not None else [])

    def _group_of(self, views: Sequence[PeerView]) -> GroupView:
        return group_view(
            views, cluster=self._app._cluster_name, consumer_group=self._app.config.resolved_consumer_group
        )

    def _new_slot(self) -> DetachSlot | None:
        """A slot of this worker's group, or ``None`` when the worker keeps no election."""
        election = self._app.cluster_election
        if election is None:
            return None
        config = self._app.config
        detach = config.sources.kafka.detach
        if self._caller is None:
            autodiscover = config.autodiscover
            self._caller = ResilientCaller(
                autodiscover.resilience,
                feature=DETACH_FEATURE,
                provider=autodiscover.provider,
                category='kafka',
            )
        group = config.resolved_consumer_group
        return DetachSlot(
            election,
            self._caller,
            scopes=[slot_scope(self._app._cluster_name, group, index) for index in range(detach.max_detached)],
            lease_seconds=detach.lease,
            clock=self._clock,
        )

    async def _release_slot(self, slot: DetachSlot) -> None:
        """Give ``slot`` back; a failure is logged, and the lease then ends by itself."""
        if self._slot is slot:
            self._slot = None
        try:
            await slot.release()
        except Exception:
            await logger.aexception('detach_slot_release_failed', category='kafka')

    def _publish_card(self) -> None:
        fleet = self._app.fleet
        if fleet is not None:
            fleet.publish_soon()

    async def _account_detach(self, origin: DetachOrigin, result: DetachResult, asked_at: float) -> None:
        """Count, log and note a detach request once its outcome is known."""
        outcome = OUTCOME_DETACHED if result.detached and not result.reason else result.reason
        detach_requests.labels(origin=str(origin), outcome=outcome).inc()
        self._note_request(str(origin), ACTION_DETACH, outcome, asked_at)
        if outcome != OUTCOME_DETACHED:
            await logger.ainfo('detach_refused', category='kafka', origin=str(origin), reason=outcome)

    def _note_request(self, origin: str, action: str, outcome: str, asked_at: float) -> None:
        self._last_request = {
            'origin': origin,
            'action': action,
            'outcome': outcome,
            'at_ms': int(asked_at * 1000),
        }

    def _spawn(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _guarded(self, coro: Coroutine[Any, Any, Any], *, event: str) -> None:
        """Await ``coro`` on a task nobody awaits, logging its error."""
        try:
            await coro
        except Exception:
            await logger.aexception(event, category='kafka')


class HandlerDetachFront:
    """The handler's way to the app: detach and attach with the origin ``handler``, and the stop request.

    The app puts one on its handler when the worker starts; before that the
    handler holds a stub that answers ``not_running``.
    """

    def __init__(self, app: DrakkarApp) -> None:
        self._app = app

    async def detach(self, *, duration: float | None, reason: str, in_pipeline_hook: bool) -> DetachResult:
        """Ask the controller for a handler detach."""
        return await self._app.detach.detach(
            origin=DetachOrigin.HANDLER,
            duration=duration,
            reason=reason,
            in_pipeline_hook=in_pipeline_hook,
        )

    async def attach(self, *, reason: str) -> AttachResult:
        """Ask the controller for a handler attach."""
        return await self._app.detach.attach(cause=CAUSE_HANDLER, reason=reason)

    @property
    def state(self) -> DetachState:
        """The controller's detach state now."""
        return self._app.detach.state

    def request_stop(self, *, reason: str, exit_code: int) -> None:
        """Ask the app for a graceful stop."""
        self._app.request_stop(reason=reason, exit_code=exit_code)
