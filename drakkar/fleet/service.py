"""The registry service: publishes this worker's card and keeps a snapshot of its peers.

Two background loops, both started by :meth:`FleetRegistry.start`:

- publish: the card every ``heartbeat_seconds`` and at once on every state
  change (``starting``, ``running``, ``draining``). Each tick first starts
  the provider if it has not started yet: a ``start`` that fails is tried
  again at the next tick, and the card goes out in the same tick as the
  ``start`` that succeeds.
- refresh: ``list()`` every ``refresh_seconds`` into an immutable snapshot.
  The first refresh waits for the first publish attempt after the provider
  started, because a file folder learns the server clock from its own write.

Every provider call goes through a :class:`ResilientCaller`: a time limit
per attempt, bounded retries, a circuit breaker, and at most one failure
line per operation per minute. A failed call keeps the last snapshot and
counts an error. Each loop iteration runs under a guard: an error in card
building, snapshot assembly or the features supplier is counted and logged,
and the loop goes on. Nothing in the message pipeline awaits this service:
it serves the workers list and the cache exchange, and both read the
snapshot.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

from drakkar.config import FleetRegistryConfig
from drakkar.fleet.card import WorkerIdentity, build_card, validate_card
from drakkar.fleet.provider import PeerCardList, RegistryContext, RegistryProvider
from drakkar.metrics import (
    fleet_duplicate_worker_name,
    fleet_peers,
    fleet_registry_duration,
    fleet_registry_errors,
    fleet_registry_snapshot_age,
)
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import (
    OUTCOME_NOT_STARTED,
    CallResult,
    RateLimitedLog,
    ResilientCaller,
    describe_error,
)

# The feature label of this service's provider calls, metrics and log lines.
REGISTRY_FEATURE = 'registry'

# The operation whose error counter a failed loop iteration adds to.
LOOP_OPS = {'publish': 'publish', 'refresh': 'list'}

# The ``drakkar_fleet_peers`` state of each liveness verdict.
PEER_STATE_BY_VERDICT = {True: 'online', False: 'offline', None: 'unknown'}


def _liveness(online: bool | None, age_seconds: float | None, offline_after_seconds: float) -> bool | None:
    """The provider's verdict when it gives one, else the age rule, else ``None`` (unknown)."""
    if online is not None:
        return online
    if age_seconds is None:
        return None
    return age_seconds <= offline_after_seconds


@dataclass(frozen=True)
class SnapshotEntry:
    """One peer card as listed: its age and the provider's verdict at list time, and the monotonic time of the list."""

    card: dict[str, Any]
    age_seconds: float | None
    listed_at: float
    online: bool | None = None


@dataclass(frozen=True)
class FleetSnapshot:
    """The peers of the last successful refresh. ``refreshed_at`` is ``None`` before the first one."""

    peers: tuple[SnapshotEntry, ...] = ()
    refreshed_at: float | None = None


@dataclass(frozen=True)
class PeerView:
    """A peer as the workers list shows it now.

    ``age_seconds`` is the listed age plus the time since the list, so a slow
    refresh never makes a card look younger. ``online`` is the provider's
    verdict when it gives one, else that age against
    ``offline_after_seconds``, else ``None``. ``last_seen_ts`` is ``None``
    when the provider knows no age.
    """

    card: dict[str, Any]
    age_seconds: float | None
    online: bool | None
    last_seen_ts: float | None


class FleetRegistry:
    """Runs one registry provider for this worker."""

    def __init__(
        self,
        provider: RegistryProvider,
        *,
        provider_name: str,
        config: FleetRegistryConfig,
        identity: WorkerIdentity,
        provider_options: Mapping[str, Any] | None = None,
        features: Callable[[], dict[str, Any]] | None = None,
        logger: Any = None,
        now_monotonic: Callable[[], float] = time.monotonic,
        now_epoch: Callable[[], float] = time.time,
        caller: ResilientCaller | None = None,
        connections: Connections | None = None,
    ) -> None:
        self._provider = provider
        # The card's ``features``, read at every publish: the subsystems
        # that fill it (the cache engine) may start after the registry.
        self._features = features
        self._provider_name = provider_name
        self._config = config
        self._identity = identity
        self._provider_options = dict(provider_options or {})
        self._logger = logger or structlog.get_logger()
        self._now_monotonic = now_monotonic
        self._now_epoch = now_epoch
        self._caller = (
            caller
            if caller is not None
            else ResilientCaller(
                config.resilience,
                feature=REGISTRY_FEATURE,
                provider=provider_name,
                logger=self._logger,
                now_monotonic=now_monotonic,
            )
        )
        self._connections = connections
        self._created_at = now_monotonic()
        self._state = 'starting'
        # Set by set_state to publish at once instead of at the next heartbeat.
        self._state_changed = asyncio.Event()
        # Set once a provider ``start`` succeeded. Until then nothing is
        # published, listed or withdrawn, and the provider is never stopped.
        self._provider_started = asyncio.Event()
        # Set by the first publish tick that ran with the provider started.
        self._first_publish_done = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._snapshot = FleetSnapshot()
        self._iteration_failure_log = RateLimitedLog(now_monotonic)
        self._reported_card_workers: set[str] = set()
        self._duplicate_instances: set[str] = set()
        self._started = False
        self._stopped = False

    @property
    def identity(self) -> WorkerIdentity:
        return self._identity

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @property
    def state(self) -> str:
        return self._state

    async def start(self) -> None:
        """Start the loops in the background and return at once."""
        if self._started:
            return
        self._started = True
        await self._logger.ainfo(
            'fleet_registry_started',
            category='fleet',
            provider=self._provider_name,
            worker=self._identity.worker,
            instance=self._identity.instance,
            cluster=self._identity.cluster,
            url=self._identity.url,
            heartbeat_seconds=self._config.heartbeat_seconds,
            refresh_seconds=self._config.refresh_seconds,
        )
        # The first publish tick gets the state current now, even if the
        # worker moves on before that tick runs. Once the provider has
        # started, every state reaches the card; a state that passes while
        # the provider still fails to start is never published.
        self._task = asyncio.create_task(self._run(first_state=self._state), name='fleet-registry')

    def set_state(self, state: str) -> None:
        """Change the published state; the publish loop sends it without waiting for the heartbeat."""
        self._state = state
        self._state_changed.set()

    def snapshot(self) -> FleetSnapshot:
        """The last snapshot. Safe from any thread: it is one immutable object."""
        return self._snapshot

    def peer_views(self) -> list[PeerView]:
        """The peers with their age and liveness as of now. Safe from any thread."""
        snapshot = self._snapshot
        now_monotonic = self._now_monotonic()
        now_epoch = self._now_epoch()
        views: list[PeerView] = []
        for entry in snapshot.peers:
            age = None if entry.age_seconds is None else entry.age_seconds + (now_monotonic - entry.listed_at)
            views.append(
                PeerView(
                    card=entry.card,
                    age_seconds=age,
                    online=_liveness(entry.online, age, self._config.offline_after_seconds),
                    last_seen_ts=None if age is None else now_epoch - age,
                )
            )
        return views

    async def stop(self, deadline_seconds: float | None = None) -> None:
        """Stop the loops, withdraw this worker's card, stop the provider. Safe to call twice.

        The withdraw and the provider's ``stop`` get one attempt each, both
        inside ``deadline_seconds`` from this call; with ``None`` each has the
        per-call timeout. While the breaker is open, or when the provider
        never started, both are skipped at once.
        """
        if self._stopped or not self._started:
            return
        self._stopped = True
        deadline_at = None if deadline_seconds is None else self._now_monotonic() + deadline_seconds
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                # A loop that died must not cost the withdraw below.
                await self._logger.aerror(
                    'fleet_registry_loop_failed',
                    category='fleet',
                    provider=self._provider_name,
                    error=describe_error(exc),
                )
        if not self._provider_started.is_set():
            self._caller.skip('withdraw', OUTCOME_NOT_STARTED)
            self._caller.skip('stop', OUTCOME_NOT_STARTED)
            return
        await self._call_provider(
            'withdraw',
            lambda: self._provider.withdraw(self._identity.worker, self._identity.instance),
            attempts=1,
            deadline_seconds=self._time_left(deadline_at),
        )
        await self._call_provider(
            'stop', self._provider.stop, attempts=1, deadline_seconds=self._time_left(deadline_at)
        )

    def _time_left(self, deadline_at: float | None) -> float | None:
        return None if deadline_at is None else deadline_at - self._now_monotonic()

    # --- loops ---

    async def _run(self, first_state: str) -> None:
        async with asyncio.TaskGroup() as loops:
            loops.create_task(self._publish_loop(first_state))
            loops.create_task(self._refresh_loop())

    async def _publish_loop(self, first_state: str) -> None:
        """Publish ``first_state`` at once, then the current state after every heartbeat or change.

        Once the provider has started, a change made while a publish runs, or
        before the first one, leaves the event set, so the loop publishes
        again at once and no state is skipped.
        """
        state = first_state
        while True:
            await self._publish_tick(state)
            if self._provider_started.is_set():
                self._first_publish_done.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._state_changed.wait(), timeout=self._config.heartbeat_seconds)
            # Cleared before reading the state: a change after this point
            # sets the event again and wakes the next wait at once.
            self._state_changed.clear()
            state = self._state

    async def _refresh_loop(self) -> None:
        # Set only by a publish tick that ran with the provider started, so
        # nothing is listed before the provider starts.
        await self._first_publish_done.wait()
        while True:
            await self._refresh_tick()
            await asyncio.sleep(self._config.refresh_seconds)

    async def _publish_tick(self, state: str) -> None:
        """One publish iteration: start the provider if it has not started, then publish ``state``."""
        try:
            if await self._ensure_started():
                await self._publish_once(state)
            else:
                self._caller.skip('publish', OUTCOME_NOT_STARTED)
                # No refresh runs before the start: the workers list is as old as the service.
                fleet_registry_snapshot_age.set(self._now_monotonic() - self._created_at)
        except Exception as exc:
            await self._report_iteration_failure('publish', exc)

    async def _refresh_tick(self) -> None:
        """One refresh iteration; an error is counted and logged, and the loop goes on."""
        try:
            await self._refresh_once()
        except Exception as exc:
            await self._report_iteration_failure('refresh', exc)

    async def _ensure_started(self) -> bool:
        """Start the provider unless it has started; True when it is started."""
        if self._provider_started.is_set():
            return True
        ctx = RegistryContext(
            worker=self._identity.worker,
            instance=self._identity.instance,
            cluster=self._identity.cluster,
            logger=self._logger,
            options=self._provider_options,
            connections=self._connections,
        )
        result = await self._call_provider('start', lambda: self._provider.start(ctx))
        if not result.ok:
            return False
        # Set before the log await: a cancellation there must not leave a
        # started provider that stop() would never withdraw from or stop.
        self._provider_started.set()
        await self._logger.ainfo(
            'provider_started',
            category='fleet',
            feature=REGISTRY_FEATURE,
            provider=self._provider_name,
        )
        return True

    async def _publish_once(self, state: str) -> None:
        card = build_card(
            self._identity,
            state=state,
            heartbeat_at=datetime.fromtimestamp(self._now_epoch(), UTC),
            features=self._features() if self._features is not None else None,
        )
        await self._call_provider('publish', lambda: self._provider.publish(card))

    async def _refresh_once(self) -> None:
        result = await self._call_provider('list', self._list_checked)
        now = self._now_monotonic()
        if not result.ok:
            last_good = self._snapshot.refreshed_at
            fleet_registry_snapshot_age.set(now - (self._created_at if last_good is None else last_good))
            return
        peers = await self._peers_from(result.value, now)
        self._snapshot = FleetSnapshot(peers=tuple(peers), refreshed_at=now)
        fleet_registry_snapshot_age.set(0)
        self._set_peer_gauge(peers)

    async def _list_checked(self) -> PeerCardList:
        """``provider.list()``, refusing a result that is not a list so it counts as a failed call."""
        listed = await self._provider.list()
        if not isinstance(listed, list):
            raise TypeError(f'list() returned {type(listed).__name__}, not a list of PeerCard')
        return listed

    async def _peers_from(self, listed: PeerCardList, listed_at: float) -> list[SnapshotEntry]:
        """Valid peer cards of a listing; invalid ones are logged, this worker's own name is checked for twins."""
        peers: list[SnapshotEntry] = []
        for listed_card in listed:
            # A plugin may hand over something that is not a PeerCard; its
            # card then reads as None and is rejected like any invalid card.
            card = getattr(listed_card, 'card', None)
            age_seconds = getattr(listed_card, 'age_seconds', None)
            online = getattr(listed_card, 'online', None)
            # Anything but a bool is no verdict: the workers list shows true, false or nothing.
            online = online if isinstance(online, bool) else None
            if not isinstance(card, dict):
                await self._report_invalid_card(card, 'not_object')
                continue
            reason = validate_card(card)
            if reason:
                await self._report_invalid_card(card, reason)
                continue
            if card['worker'] == self._identity.worker:
                await self._check_duplicate(card, _liveness(online, age_seconds, self._config.offline_after_seconds))
                continue
            peers.append(SnapshotEntry(card=card, age_seconds=age_seconds, listed_at=listed_at, online=online))
        return peers

    def _set_peer_gauge(self, peers: list[SnapshotEntry]) -> None:
        counts = dict.fromkeys(PEER_STATE_BY_VERDICT.values(), 0)
        for entry in peers:
            verdict = _liveness(entry.online, entry.age_seconds, self._config.offline_after_seconds)
            counts[PEER_STATE_BY_VERDICT[verdict]] += 1
        for state, count in counts.items():
            fleet_peers.labels(state=state).set(count)

    # --- calls, failures, reports ---

    async def _call_provider(
        self,
        op: str,
        make_call: Callable[[], Awaitable[Any]],
        *,
        attempts: int | None = None,
        deadline_seconds: float | None = None,
    ) -> CallResult:
        """One provider operation through the caller, also counted in the registry's own metrics.

        An operation that made no attempt (open breaker, spent deadline) has no
        duration and is no error here: no call failed. The caller's
        ``drakkar_provider_calls_total`` counts it by its outcome.
        """
        started = time.perf_counter()
        result = await self._caller.call(op, make_call, attempts=attempts, deadline_seconds=deadline_seconds)
        if result.attempts == 0:
            return result
        fleet_registry_duration.labels(op=op).observe(time.perf_counter() - started)
        if not result.ok:
            fleet_registry_errors.labels(op=op).inc()
        return result

    async def _report_iteration_failure(self, loop: str, exc: Exception) -> None:
        """Count a loop iteration that raised; log it at most once a minute per loop."""
        fleet_registry_errors.labels(op=LOOP_OPS[loop]).inc()
        await self._iteration_failure_log.emit(
            loop,
            self._logger.aerror,
            'fleet_registry_iteration_failed',
            category='fleet',
            loop=loop,
            error=describe_error(exc),
            exc_info=exc,
        )

    async def _report_invalid_card(self, card: Any, reason: str) -> None:
        """Log a skipped card once per worker name (``''`` when the card names none)."""
        worker = card.get('worker') if isinstance(card, dict) else None
        worker = worker if isinstance(worker, str) else ''
        if worker in self._reported_card_workers:
            return
        self._reported_card_workers.add(worker)
        if reason == 'schema_unsupported':
            await self._logger.awarning(
                'fleet_card_schema_unsupported',
                category='fleet',
                worker=worker,
                schema=card.get('schema') if isinstance(card, dict) else None,
                hint='a worker of a newer or older release published this card; it is not listed',
            )
            return
        await self._logger.awarning('fleet_card_invalid', category='fleet', worker=worker, reason=reason)

    async def _check_duplicate(self, card: dict[str, Any], is_live: bool | None) -> None:
        """Warn once per foreign instance that publishes this worker's name while it is live."""
        other_instance = card.get('instance', '')
        if other_instance == self._identity.instance:
            return
        if not is_live:
            return
        if other_instance in self._duplicate_instances:
            return
        self._duplicate_instances.add(other_instance)
        fleet_duplicate_worker_name.inc()
        await self._logger.awarning(
            'fleet_duplicate_worker_name',
            category='fleet',
            worker=self._identity.worker,
            instance=self._identity.instance,
            other_instance=other_instance,
            other_url=card['url'],
            hint='two live processes publish one worker name; give each worker a unique name',
        )
