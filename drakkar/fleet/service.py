"""The registry service: publishes this worker's card and keeps a snapshot of its peers.

Two background loops, both started by :meth:`FleetRegistry.start`:

- publish: the card every ``heartbeat_seconds`` and at once on every state
  change (``starting``, ``running``, ``draining``);
- refresh: ``list()`` every ``refresh_seconds`` into an immutable snapshot.
  The first refresh waits for the first publish attempt, because a file
  folder learns the server clock from its own write.

Every provider call runs under ``timeout_seconds``. A failed call keeps the
last snapshot, counts an error, and logs at most once a minute per
operation. Nothing in the message pipeline awaits this service: it serves
the workers list and the cache exchange, and both read the snapshot.
"""

import asyncio
import contextlib
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from drakkar.config import FleetRegistryConfig
from drakkar.fleet.card import WorkerIdentity, build_card, validate_card
from drakkar.fleet.provider import PeerCardList, RegistryContext, RegistryProvider
from drakkar.fleet.providers import NoneRegistry
from drakkar.metrics import (
    fleet_duplicate_worker_name,
    fleet_peers,
    fleet_registry_duration,
    fleet_registry_errors,
    fleet_registry_snapshot_age,
)

# A failing provider logs one line per operation per this many seconds; the
# line carries how many failures it stood for.
FAILURE_LOG_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class SnapshotEntry:
    """One peer card as listed: its age at list time and the monotonic time of the list."""

    card: dict[str, Any]
    age_seconds: float | None
    listed_at: float


@dataclass(frozen=True)
class FleetSnapshot:
    """The peers of the last successful refresh. ``refreshed_at`` is ``None`` before the first one."""

    peers: tuple[SnapshotEntry, ...] = ()
    refreshed_at: float | None = None
    # worker name -> cluster name, for the cache exchange's per-peer lookup.
    clusters: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PeerView:
    """A peer as the workers list shows it now.

    ``age_seconds`` is the listed age plus the time since the list, so a slow
    refresh never makes a card look younger. ``online`` and ``last_seen_ts``
    are ``None`` when the provider knows no age.
    """

    card: dict[str, Any]
    age_seconds: float | None
    online: bool | None
    last_seen_ts: float | None


def _describe(exc: BaseException) -> str:
    """A log-friendly error text; a timeout's own text is empty, a task group's names only itself."""
    if isinstance(exc, BaseExceptionGroup):
        return '; '.join(_describe(inner) for inner in exc.exceptions)
    if isinstance(exc, TimeoutError):
        return 'timed out'
    return str(exc) or type(exc).__name__


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
        logger: Any = None,
        now_monotonic: Callable[[], float] = time.monotonic,
        now_epoch: Callable[[], float] = time.time,
    ) -> None:
        self._provider = provider
        self._provider_name = provider_name
        self._config = config
        self._identity = identity
        self._provider_options = dict(provider_options or {})
        self._logger = logger or structlog.get_logger()
        self._now_monotonic = now_monotonic
        self._now_epoch = now_epoch
        self._created_at = now_monotonic()
        self._state = 'starting'
        # Set by set_state to publish at once instead of at the next heartbeat.
        self._state_changed = asyncio.Event()
        self._first_publish_done = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._snapshot = FleetSnapshot()
        self._last_failure_log: dict[str, float] = {}
        self._suppressed_failures: dict[str, int] = {}
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
        # The first publish sends the state current now, even if the worker
        # moves on before that publish runs: every state reaches the card.
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
            if entry.age_seconds is None:
                views.append(PeerView(card=entry.card, age_seconds=None, online=None, last_seen_ts=None))
                continue
            age = entry.age_seconds + (now_monotonic - entry.listed_at)
            views.append(
                PeerView(
                    card=entry.card,
                    age_seconds=age,
                    online=age <= self._config.offline_after_seconds,
                    last_seen_ts=now_epoch - age,
                )
            )
        return views

    def peer_cluster(self, worker: str) -> str | None:
        """The cluster of peer ``worker`` from the last snapshot, or ``None`` when it is not listed."""
        return self._snapshot.clusters.get(worker)

    async def stop(self) -> None:
        """Stop the loops, withdraw this worker's card, stop the provider. Safe to call twice."""
        if self._stopped or not self._started:
            return
        self._stopped = True
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
                    error=_describe(exc),
                )
        await self._call('withdraw', lambda: self._provider.withdraw(self._identity.worker, self._identity.instance))
        try:
            await asyncio.wait_for(self._provider.stop(), timeout=self._config.timeout_seconds)
        except Exception as exc:
            await self._logger.awarning(
                'fleet_registry_stop_failed',
                category='fleet',
                provider=self._provider_name,
                error=_describe(exc),
            )

    # --- loops ---

    async def _run(self, first_state: str) -> None:
        await self._start_provider()
        async with asyncio.TaskGroup() as loops:
            loops.create_task(self._publish_loop(first_state))
            loops.create_task(self._refresh_loop())

    async def _start_provider(self) -> None:
        """Start the provider; one that cannot start is replaced by ``none`` so the worker runs on alone."""
        ctx = RegistryContext(
            worker=self._identity.worker,
            instance=self._identity.instance,
            cluster=self._identity.cluster,
            logger=self._logger,
            options=self._provider_options,
        )
        try:
            await asyncio.wait_for(self._provider.start(ctx), timeout=self._config.timeout_seconds)
        except Exception as exc:
            await self._logger.aerror(
                'fleet_registry_start_failed',
                category='fleet',
                provider=self._provider_name,
                error=_describe(exc),
                hint='the worker runs without peers; fix the provider and restart',
            )
            self._provider = NoneRegistry()

    async def _publish_loop(self, first_state: str) -> None:
        """Publish ``first_state`` at once, then the current state after every heartbeat or change.

        A change made while a publish runs, or before the first one, leaves
        the event set, so the loop publishes again at once and no state is
        skipped.
        """
        state = first_state
        while True:
            await self._publish_once(state)
            self._first_publish_done.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._state_changed.wait(), timeout=self._config.heartbeat_seconds)
            # Cleared before reading the state: a change after this point
            # sets the event again and wakes the next wait at once.
            self._state_changed.clear()
            state = self._state

    async def _refresh_loop(self) -> None:
        await self._first_publish_done.wait()
        while True:
            await self._refresh_once()
            await asyncio.sleep(self._config.refresh_seconds)

    async def _publish_once(self, state: str) -> None:
        card = build_card(
            self._identity,
            state=state,
            heartbeat_at=datetime.fromtimestamp(self._now_epoch(), UTC),
        )
        await self._call('publish', lambda: self._provider.publish(card))

    async def _refresh_once(self) -> None:
        succeeded, listed = await self._call('list', self._list_checked)
        now = self._now_monotonic()
        if not succeeded:
            last_good = self._snapshot.refreshed_at
            fleet_registry_snapshot_age.set(now - (self._created_at if last_good is None else last_good))
            return
        peers = await self._peers_from(listed, now)
        self._snapshot = FleetSnapshot(
            peers=tuple(peers),
            refreshed_at=now,
            clusters={entry.card['worker']: entry.card['cluster'] for entry in peers},
        )
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
            if not isinstance(card, dict):
                await self._report_invalid_card(card, 'not_object')
                continue
            reason = validate_card(card)
            if reason:
                await self._report_invalid_card(card, reason)
                continue
            if card['worker'] == self._identity.worker:
                await self._check_duplicate(card, age_seconds)
                continue
            peers.append(SnapshotEntry(card=card, age_seconds=age_seconds, listed_at=listed_at))
        return peers

    def _set_peer_gauge(self, peers: list[SnapshotEntry]) -> None:
        counts = {'online': 0, 'offline': 0, 'unknown': 0}
        for entry in peers:
            if entry.age_seconds is None:
                counts['unknown'] += 1
            elif entry.age_seconds <= self._config.offline_after_seconds:
                counts['online'] += 1
            else:
                counts['offline'] += 1
        for state, count in counts.items():
            fleet_peers.labels(state=state).set(count)

    # --- calls, failures, reports ---

    async def _call(self, op: str, make_call: Callable[[], Coroutine[Any, Any, Any]]) -> tuple[bool, Any]:
        """Run one provider call under the timeout: ``(True, result)`` or ``(False, None)`` after counting it."""
        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(make_call(), timeout=self._config.timeout_seconds)
        except Exception as exc:
            fleet_registry_duration.labels(op=op).observe(time.perf_counter() - started)
            fleet_registry_errors.labels(op=op).inc()
            await self._log_failure(op, exc)
            return False, None
        fleet_registry_duration.labels(op=op).observe(time.perf_counter() - started)
        return True, result

    async def _log_failure(self, op: str, exc: Exception) -> None:
        now = self._now_monotonic()
        last_logged = self._last_failure_log.get(op)
        if last_logged is not None and now - last_logged < FAILURE_LOG_INTERVAL_SECONDS:
            self._suppressed_failures[op] = self._suppressed_failures.get(op, 0) + 1
            return
        self._last_failure_log[op] = now
        await self._logger.awarning(
            f'fleet_registry_{op}_failed',
            category='fleet',
            provider=self._provider_name,
            error=_describe(exc),
            timeout_seconds=self._config.timeout_seconds,
            suppressed=self._suppressed_failures.pop(op, 0),
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

    async def _check_duplicate(self, card: dict[str, Any], age_seconds: float | None) -> None:
        """Warn once per foreign instance that publishes this worker's name while it is live."""
        other_instance = card.get('instance', '')
        if other_instance == self._identity.instance:
            return
        if age_seconds is None or age_seconds > self._config.offline_after_seconds:
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
