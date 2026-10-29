"""How the worker starts, feeds and stops the registry service and the election of its cluster tasks."""

import asyncio
import json
import re
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar import cluster_tasks as cluster_tasks_module
from drakkar.app import DrakkarApp
from drakkar.cluster_tasks import ClusterElection, ClusterHashChecker, archive_task_runs
from drakkar.config import DrakkarConfig
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.fleet.providers import BUILTIN_PROVIDERS, FilesystemRegistry
from drakkar.lifecycle import UnknownProviderError, election_provider_options, registry_provider_options
from drakkar.models import SourceMessage
from drakkar.periodic import periodic
from drakkar.providers.connections import Connections
from drakkar.recorder import core as core_mod
from drakkar.recorder.election import BUILTIN_ELECTIONS, ElectionContext, FilesystemElection
from drakkar.recorder.election_postgres import PostgresElection
from drakkar.schedule import ARCHIVE_TASK_NAME, MIN_HOLD_SECONDS, ClusterTaskDefinition, cluster_tasks_hash
from drakkar.sources import kafka as kafka_source_module
from tests.archive_helpers import (
    CLUSTER,
    SteppingClock,
    due_start,
    election_calls,
    history_folder,
    idle_archive_task,
    make_sealed_db,
)
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection, MemoryRegistry, MemoryStore
from tests.test_cluster_tasks import NOW, SLOT, RecordingLogger, VirtualClock
from tests.test_lifecycle_sources import StubSource
from tests.test_sources_validation import KafkaOnlyHandler


def _config(tmp_path, **overrides) -> DrakkarConfig:
    values = {
        'storage': {'dir': str(tmp_path / 'storage')},
        'ui': {'enabled': True, 'port': 8123, 'release': {'enabled': False}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': 1},
        'sinks': {'kafka': {'output_results': {'topic': 'output-results'}}},
        'sources': {'kafka': {'enabled': True, 'startup_align_enabled': False}},
    }
    values.update(overrides)
    return DrakkarConfig(**values)


def _app(tmp_path, handler: object | None = None, **overrides) -> DrakkarApp:
    return DrakkarApp(
        handler=handler if handler is not None else KafkaOnlyHandler(),
        config=_config(tmp_path, **overrides),
        worker_id='w-1',
    )


def _install_registry_factory(monkeypatch, factory) -> None:
    """Make ``factory`` the factory of the ``filesystem`` registry provider for the test."""
    monkeypatch.setitem(BUILTIN_PROVIDERS, 'filesystem', factory)


def _install_election_factory(monkeypatch, factory) -> None:
    """Make ``factory`` the factory of the ``filesystem`` election provider for the test."""
    monkeypatch.setitem(BUILTIN_ELECTIONS, 'filesystem', factory)


class _FixedPeersRegistry:
    """A registry fake: lists a fixed set of peers with unknown ages and ignores every write."""

    def __init__(self, peers: list[dict[str, str]]) -> None:
        self._cards = [
            {'schema': 1, 'worker': peer['name'], 'cluster': peer.get('cluster', ''), 'url': peer['url']}
            for peer in peers
        ]

    async def start(self, ctx: RegistryContext) -> None:
        return None

    async def publish(self, card: dict) -> None:
        return None

    async def list(self) -> PeerCardList:
        return [PeerCard(card=dict(card), age_seconds=None) for card in self._cards]

    async def withdraw(self, worker: str, instance: str) -> None:
        return None

    async def stop(self) -> None:
        return None


# --- which provider runs ---


async def test_the_default_registry_is_the_filesystem_provider_under_storage_dir(tmp_path):
    app = _app(tmp_path)

    await app._lifecycle._start_fleet()
    card_path = tmp_path / 'storage' / 'fleet' / 'workers' / 'w-1.json'
    await wait_for(card_path.exists)
    await app._fleet.stop()

    assert app.fleet is not None
    assert app.fleet.provider_name == 'filesystem'


async def test_the_card_carries_the_worker_identity(tmp_path):
    app = _app(
        tmp_path,
        cluster_name='search',
        ui={'enabled': True, 'port': 8123, 'release': {'enabled': False}, 'public_url': 'http://w-1.internal:8123/'},
    )

    await app._lifecycle._start_fleet()
    card_path = tmp_path / 'storage' / 'fleet' / 'workers' / 'w-1.json'
    await wait_for(card_path.exists)
    card = json.loads(card_path.read_bytes())
    await app._fleet.stop()

    assert card['worker'] == 'w-1'
    assert card['cluster'] == 'search'
    assert card['url'] == 'http://w-1.internal:8123/'
    assert card['backend'] == 'python'
    assert card['state'] == 'starting'
    assert re.fullmatch(r'[0-9a-f]{16}', card['instance'])


def test_the_ui_url_without_public_url_is_the_outbound_address(tmp_path, monkeypatch):
    import drakkar.app as app_module

    monkeypatch.setattr(app_module, 'detect_worker_ip', lambda: '10.1.2.3')
    app = _app(tmp_path)

    assert app.ui_url == 'http://10.1.2.3:8123/'


def test_every_process_gets_a_new_instance(tmp_path):
    assert _app(tmp_path).worker_identity.instance != _app(tmp_path).worker_identity.instance


async def test_a_shared_dir_holds_the_card_folder(tmp_path):
    app = _app(tmp_path, shared={'dir': str(tmp_path / 'shared')})

    await app._lifecycle._start_fleet()
    await wait_for((tmp_path / 'shared' / 'fleet' / 'workers' / 'w-1.json').exists)
    await app._fleet.stop()


async def test_the_registry_factory_gets_the_provider_options_and_the_worker_context(tmp_path, monkeypatch):
    received = {}

    class _Recording(_FixedPeersRegistry):
        async def start(self, ctx: RegistryContext) -> None:
            received['ctx'] = ctx

    def factory(options):
        received['options'] = dict(options)
        return _Recording([])

    _install_registry_factory(monkeypatch, factory)
    app = _app(tmp_path)

    await app._lifecycle._start_fleet()
    await wait_for(lambda: 'ctx' in received)
    await app._fleet.stop()

    assert received['options'] == registry_provider_options(app._config)
    assert received['ctx'].options == received['options']
    assert received['ctx'].worker == 'w-1'


def test_registry_provider_options_for_postgres_names_the_connection_and_prune(tmp_path):
    config = _config(
        tmp_path,
        connections={'postgres': {'main': {'dsn': 'postgresql://user:secret@db/x'}}},
        fleet={'registry': {'provider': 'postgres'}},
    )

    assert registry_provider_options(config) == {'connection': 'main', 'prune_after_seconds': 86_400}


def test_registry_provider_options_for_redis_names_the_connection_and_prune(tmp_path):
    config = _config(
        tmp_path,
        connections={'redis': {'main': {'url': 'redis://user:secret@cache:6379/0'}}},
        fleet={'registry': {'provider': 'redis'}},
    )

    assert registry_provider_options(config) == {'connection': 'main', 'prune_after_seconds': 86_400}


def test_registry_provider_options_for_consul_names_the_connection_and_the_offline_limit(tmp_path):
    config = _config(
        tmp_path,
        connections={'consul': {'agents': {}}},
        fleet={'registry': {'provider': 'consul', 'connection': 'agents', 'offline_after_seconds': 90}},
    )

    assert registry_provider_options(config) == {'connection': 'agents', 'offline_after_seconds': 90}


def test_election_provider_options_for_redis_names_the_connection(tmp_path):
    config = _config(
        tmp_path,
        connections={'redis': {'locks': {'url': 'redis://user:secret@cache:6379/0'}}},
        periodic={'election': {'provider': 'redis', 'connection': 'locks'}},
    )

    assert election_provider_options(config) == {'connection': 'locks'}


def test_election_provider_options_for_consul_names_the_connection(tmp_path):
    config = _config(
        tmp_path,
        connections={'consul': {'locks': {}}},
        periodic={'election': {'provider': 'consul', 'connection': 'locks'}},
    )

    assert election_provider_options(config) == {'connection': 'locks'}


def test_the_cluster_election_passes_the_connection_to_the_postgres_factory(tmp_path):
    app = _app(
        tmp_path,
        connections={'postgres': {'reports_db': {'dsn': 'postgresql://user:secret@db/x'}}},
        periodic={'election': {'provider': 'postgres', 'connection': 'reports_db'}},
    )

    election = app._lifecycle._build_cluster_election([ARCHIVE_TASK_NAME])

    assert election is not None
    assert isinstance(election.provider, PostgresElection)
    assert election.provider._connection == 'reports_db'
    assert election._context.options == {'connection': 'reports_db'}


def test_the_cluster_election_gives_the_filesystem_factory_no_options(tmp_path):
    app = _app(tmp_path)

    election = app._lifecycle._build_cluster_election([ARCHIVE_TASK_NAME])

    assert election is not None
    assert isinstance(election.provider, FilesystemElection)
    assert election._context.options == {}


def test_the_cluster_election_carries_the_worker_context_and_a_periodic_caller(tmp_path):
    app = _app(
        tmp_path,
        shared={'dir': str(tmp_path / 'shared')},
        cluster_name=CLUSTER,
        periodic={'election': {'resilience': {'timeout_seconds': 1.5, 'retries': 0}}},
    )
    app._lifecycle._create_connections()

    election = app._lifecycle._build_cluster_election([ARCHIVE_TASK_NAME])

    assert isinstance(election, ClusterElection)
    ctx = election._context
    assert (ctx.worker, ctx.instance, ctx.cluster) == ('w-1', app._instance, CLUSTER)
    assert ctx.shared_dir == str(tmp_path / 'shared')
    assert ctx.connections is app._connections
    assert (election.provider_name, election.caller.operation_bound_seconds) == ('filesystem', 1.5)
    assert election.caller._category == 'periodic'


def test_no_cluster_task_builds_no_election(tmp_path, monkeypatch):
    app = _app(tmp_path, periodic={'election': {'provider': 'no-such-election'}})

    assert app._lifecycle._build_cluster_election([]) is None


def test_the_filesystem_election_without_a_shared_folder_fails_startup_naming_the_first_task(tmp_path, monkeypatch):
    app = _app(tmp_path)
    monkeypatch.setattr(type(app._config), 'resolved_shared_dir', property(lambda self: ''))

    with pytest.raises(ValueError) as raised:
        app._lifecycle._build_cluster_election(['refresh_rates', 'zz_last'])

    assert str(raised.value) == (
        "periodic task 'refresh_rates' has scope cluster, but the filesystem election needs a shared folder "
        '(shared.dir or storage.dir)'
    )


@pytest.mark.parametrize(
    'overrides',
    [
        {'ui': {'enabled': False, 'release': {'enabled': False}}},
        {'storage': {'dir': ''}},
        {'history': {'archive': {'enabled': False}}},
    ],
    ids=['ui-off', 'memory-only', 'archiving-off'],
)
def test_election_is_built_only_when_the_archive_runs(tmp_path, monkeypatch, overrides):
    app = _app(tmp_path, periodic={'election': {'provider': 'no-such-election'}}, **overrides)

    app._lifecycle._build_periodic_roster()

    assert not archive_task_runs(app._config)
    assert app._cluster_election is None


@pytest.mark.parametrize('name', ['etcd', 'static', 'none'])
async def test_a_registry_provider_outside_the_four_built_in_ones_fails_startup(tmp_path, name):
    app = _app(tmp_path, fleet={'registry': {'provider': name}})

    with pytest.raises(UnknownProviderError) as raised:
        await app._lifecycle._start_fleet()

    assert str(raised.value) == (
        f"fleet.registry.provider '{name}' is not known; known providers: consul, filesystem, postgres, redis"
    )


async def test_unknown_election_provider_fails_startup_when_the_archive_runs(tmp_path, monkeypatch):
    app = _app(tmp_path, periodic={'election': {'provider': 'etcd'}})

    assert archive_task_runs(app._config)
    with pytest.raises(UnknownProviderError, match=re.escape("periodic.election.provider 'etcd' is not known")):
        app._lifecycle._build_periodic_roster()

    assert app._cluster_election is None


# --- the registry switched off ---


async def test_the_ui_off_turns_the_registry_off_with_one_warning(tmp_path):
    app = _app(tmp_path, ui={'enabled': False, 'release': {'enabled': False}})

    with capture_logs() as logs:
        await app._lifecycle._start_fleet()

    assert app.fleet is None
    assert [entry['event'] for entry in logs] == ['fleet_registry_disabled_no_ui']


async def test_no_folder_turns_the_filesystem_registry_off_with_one_warning(tmp_path):
    app = _app(tmp_path, storage={'dir': ''})

    with capture_logs() as logs:
        await app._lifecycle._start_fleet()

    assert app.fleet is None
    assert [entry['event'] for entry in logs] == ['fleet_registry_disabled_no_dir']


# --- the card folder ---


async def test_a_derived_folder_that_cannot_be_created_only_fails_publishes(tmp_path):
    """The derived folder is best effort: the worker runs, the registry counts errors and retries."""
    blocker = tmp_path / 'blocker'
    blocker.write_text('a file where the folder should be')
    app = _app(tmp_path, shared={'dir': str(blocker)})

    await app._lifecycle._start_fleet()
    await app._fleet.stop()

    assert isinstance(app.fleet._provider, FilesystemRegistry)


# --- states and order in the worker's life ---


class _RecordingRegistry(_FixedPeersRegistry):
    """A provider that records the state of every published card."""

    def __init__(self) -> None:
        super().__init__([])
        self.states: list[str] = []
        self.withdrawn = False

    async def publish(self, card: dict) -> None:
        self.states.append(card['state'])

    async def withdraw(self, worker: str, instance: str) -> None:
        self.withdrawn = True


def _runnable_app(
    tmp_path, monkeypatch, provider_factory, handler: object | None = None
) -> tuple[DrakkarApp, StubSource, list[str]]:
    """A worker with stub sources, no real UI server or recorder, and a registry from ``provider_factory``."""
    app = _app(tmp_path, handler=handler)
    source = StubSource('kafka')
    app.sources = [source]
    order: list[str] = []
    _install_registry_factory(monkeypatch, provider_factory)
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock())
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)

    ui_server = MagicMock()
    ui_server.stop = AsyncMock(side_effect=lambda: order.append('ui_server.stop'))
    recorder = MagicMock()
    recorder.stop = AsyncMock(side_effect=lambda **_kwargs: order.append('recorder.stop'))
    # A plain method on the real recorder. The stand-in task's first slot is an hour away at most,
    # and a won slot would do nothing, so no archive work runs here.
    recorder.archive_cluster_task = MagicMock(return_value=idle_archive_task())

    async def start_ui_and_recorder() -> None:
        order.append('ui_and_recorder')
        app._ui_server = ui_server
        app._recorder = recorder

    monkeypatch.setattr(app._lifecycle, '_start_ui_and_recorder', start_ui_and_recorder)
    return app, source, order


async def test_states_follow_the_worker_life_and_the_card_goes_before_the_ui_server(tmp_path, monkeypatch):
    provider = _RecordingRegistry()
    app, _source, order = _runnable_app(tmp_path, monkeypatch, lambda _options: provider)

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    await wait_for(lambda: provider.states[-1:] == ['running'])
    original_stop = app.fleet.stop

    async def recorded_stop(deadline_seconds: float | None = None) -> None:
        order.append('fleet.stop')
        await original_stop(deadline_seconds=deadline_seconds)

    app._fleet.stop = recorded_stop
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert provider.states[0] == 'starting'
    assert provider.states[-1] == 'draining'
    assert 'running' in provider.states
    assert provider.withdrawn
    assert app.lifecycle_state == 'draining'
    assert order.index('ui_and_recorder') == 0
    assert order.index('recorder.stop') < order.index('fleet.stop') < order.index('ui_server.stop')


@pytest.mark.parametrize(('ui_enabled', 'reserve'), [(True, 0.7), (False, 0.0)], ids=['registry', 'no-registry'])
async def test_the_recorder_stop_keeps_one_registry_call_only_while_a_registry_runs(
    tmp_path, monkeypatch, ui_enabled, reserve
):
    app, _source, _order = _runnable_app(tmp_path, monkeypatch, lambda _options: _RecordingRegistry())
    # The registry needs the UI server, so a worker with the UI off runs none.
    app._config.ui.enabled = ui_enabled
    app._config.fleet.registry.resilience.timeout_seconds = 0.7

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    recorder = app._recorder
    assert isinstance(recorder, MagicMock)
    assert recorder.stop.call_args.kwargs['reserve_seconds'] == reserve


async def test_the_registry_gets_the_shared_connections_and_they_close_after_it_inside_the_drain_time(
    tmp_path, monkeypatch
):
    received: dict = {}

    class _ContextRecording(_RecordingRegistry):
        async def start(self, ctx: RegistryContext) -> None:
            received['ctx'] = ctx

    app, _source, order = _runnable_app(tmp_path, monkeypatch, lambda _options: _ContextRecording())

    async def recorded_close_all(_connections) -> None:
        order.append('connections.close_all')

    monkeypatch.setattr(Connections, 'close_all', recorded_close_all)
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    await wait_for(lambda: 'ctx' in received)
    original_stop = app.fleet.stop

    async def recorded_stop(deadline_seconds: float | None = None) -> None:
        order.append('fleet.stop')
        received['deadline_seconds'] = deadline_seconds
        await original_stop(deadline_seconds=deadline_seconds)

    app._fleet.stop = recorded_stop
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert isinstance(app._connections, Connections)
    assert received['ctx'].connections is app._connections
    drain_timeout = app._config.executor.drain_timeout_seconds
    assert 0 < received['deadline_seconds'] <= drain_timeout
    assert order.index('fleet.stop') < order.index('connections.close_all') < order.index('ui_server.stop')


async def test_a_connection_close_that_outlasts_the_drain_time_is_cut_off_and_logged(tmp_path, monkeypatch):
    app, _source, order = _runnable_app(tmp_path, monkeypatch, lambda _options: _RecordingRegistry())
    app._config.executor.drain_timeout_seconds = 0.2

    async def hanging_close_all(_connections) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(Connections, 'close_all', hanging_close_all)
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)

    with capture_logs() as logs:
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    (timed_out,) = [entry for entry in logs if entry['event'] == 'connections_close_timed_out']
    assert 0 <= timed_out['timeout_seconds'] <= 0.2
    assert 'ui_server.stop' in order


async def test_a_connection_close_that_raises_is_logged_and_the_closers_after_it_still_run(tmp_path, monkeypatch):
    app, _source, order = _runnable_app(tmp_path, monkeypatch, lambda _options: _RecordingRegistry())

    async def broken_close_all(_connections) -> None:
        raise RuntimeError('closer table is corrupt')

    monkeypatch.setattr(Connections, 'close_all', broken_close_all)
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)

    with capture_logs() as logs:
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    (failed,) = [entry for entry in logs if entry['event'] == 'connections_close_failed']
    assert (failed['category'], failed['error']) == ('fleet', 'closer table is corrupt')
    assert 'ui_server.stop' in order
    app._sink_manager.close_all.assert_awaited_once()


async def test_the_cache_engine_reads_the_registrys_peer_views(tmp_path, monkeypatch):
    provider = _FixedPeersRegistry([{'name': 'w-2', 'cluster': 'search', 'url': 'http://10.0.0.2:8080/'}])
    app, _source, _order = _runnable_app(tmp_path, monkeypatch, lambda _options: provider)
    app._config.cache.enabled = True

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    await wait_for(lambda: app.fleet.snapshot().refreshed_at is not None)
    views = app.cache_engine._peer_views()
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert [(view.card['worker'], view.card['cluster']) for view in views] == [('w-2', 'search')]


async def test_the_card_carries_the_cache_marks_once_the_cache_engine_runs(tmp_path, monkeypatch):
    store = MemoryStore()
    app, _source, _order = _runnable_app(tmp_path, monkeypatch, lambda _options: MemoryRegistry(store))
    app._config.cache.enabled = True

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    # The first card can go out before the cache engine starts; a later
    # publish (the running state) carries the features.
    await wait_for(lambda: store.cards.get('w-1', ({},))[0].get('features', {}).get('cache') == {})
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)


class _BlockedRegistry:
    """Every call hangs forever, like a registry on a dead network mount."""

    async def _hang(self) -> None:
        await asyncio.Event().wait()

    async def start(self, ctx: RegistryContext) -> None:
        await self._hang()

    async def publish(self, card: dict) -> None:
        await self._hang()

    async def list(self) -> PeerCardList:
        await self._hang()
        return []

    async def withdraw(self, worker: str, instance: str) -> None:
        await self._hang()

    async def stop(self) -> None:
        await self._hang()


async def test_a_blocked_registry_never_holds_up_the_pipeline(tmp_path, monkeypatch):
    """Core isolation: the sources start and run, and shutdown ends, while every registry call hangs."""
    app, source, _order = _runnable_app(tmp_path, monkeypatch, lambda _options: _BlockedRegistry())
    app._config.fleet.registry.resilience.timeout_seconds = 0.05

    run = asyncio.create_task(app._lifecycle._async_run())
    # Readiness comes before the lifecycle calls ``run``; wait for both, or
    # the signal can land in between on a loaded machine.
    await asyncio.wait_for(_wait_until(lambda: app.is_ready and source.has_run), timeout=WAIT_FOR_TIMEOUT_SECONDS)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert source.calls[:3] == ['bind', 'start', 'run']
    assert 'stop' in source.calls


class _BlockedElection:
    """Every call hangs forever, like an election service that stopped answering; ``calls`` names each call."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.entered = asyncio.Event()

    async def _hang(self, op: str) -> None:
        self.calls.append(op)
        self.entered.set()
        await asyncio.Event().wait()

    async def start(self, ctx: ElectionContext) -> None:
        await self._hang('start')

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        await self._hang('acquire')
        return False

    async def refresh(self, scope: str, token: str) -> bool:
        await self._hang('refresh')
        return False

    async def is_held(self, scope: str, token: str) -> bool:
        await self._hang('is_held')
        return False

    async def release(self, scope: str, token: str) -> None:
        await self._hang('release')

    async def stop(self) -> None:
        await self._hang('stop')


class _FakeConsumer:
    """The Kafka consumer of one worker: partition 0 on subscribe, the messages a test delivers, recorded commits."""

    def __init__(self, *, connection, source, on_assign, on_revoke) -> None:
        self._on_assign = on_assign
        self._waiting: list[SourceMessage] = []
        self.committed: dict[int, int] = {}

    def deliver(self, messages: list[SourceMessage]) -> None:
        self._waiting.extend(messages)

    async def subscribe(self) -> None:
        self._on_assign([0])

    async def poll_batch(self, max_messages: int | None = None, timeout: float = 1.0) -> list[SourceMessage]:
        if not self._waiting:
            # A real poll waits for the broker; an empty one must not spin the loop.
            await asyncio.sleep(0.01)
            return []
        batch, self._waiting = self._waiting, []
        return batch

    async def commit(self, offsets: dict[int, int]) -> None:
        self.committed.update(offsets)

    async def pause(self, partition_ids: list[int]) -> None:
        pass

    async def resume(self, partition_ids: list[int]) -> None:
        pass

    async def close(self) -> None:
        pass


def _pipeline_app(tmp_path, monkeypatch, **overrides) -> tuple[DrakkarApp, list[_FakeConsumer]]:
    """A worker with its real recorder and Kafka source, on a fake consumer; no UI server, sinks or subprocesses."""
    values = {
        'shared': {'dir': str(tmp_path / 'shared')},
        'cluster_name': CLUSTER,
    }
    values.update(overrides)
    app = _app(tmp_path, **values)
    consumers: list[_FakeConsumer] = []

    def make_consumer(**kwargs) -> _FakeConsumer:
        consumer = _FakeConsumer(**kwargs)
        consumers.append(consumer)
        return consumer

    monkeypatch.setattr(kafka_source_module, 'KafkaConsumer', make_consumer)
    monkeypatch.setattr('drakkar.uiserver.server.UIServer', MagicMock(return_value=AsyncMock()))
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock())
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)
    return app, consumers


MESSAGE_COUNT = 20
# The time limit of the hanging election ``start``. The commit budget below
# is shorter, so a pipeline that waited for the start to time out would miss it.
ELECTION_START_LIMIT_SECONDS = 3.0
# Messages that need no task commit in milliseconds; this is far above that.
COMMIT_BUDGET_SECONDS = 1.5
# A whole hour: the first archive slot (every 3600 s by default) of a clock one second before it.
ARCHIVE_SLOT = 1_800_000_000


def _first_archive_slot_clock() -> SteppingClock:
    """A slot loop clock that reaches the first archive slot at once and parks after its hold."""
    return SteppingClock(ARCHIVE_SLOT - 1, limit=ARCHIVE_SLOT + MIN_HOLD_SECONDS)


async def test_a_blocked_election_never_holds_up_the_pipeline(tmp_path, monkeypatch):
    """Core isolation: messages are processed and committed, and shutdown ends, while every election call hangs."""
    election = _BlockedElection()
    _install_election_factory(monkeypatch, lambda _options: election)
    resilience = {'timeout_seconds': ELECTION_START_LIMIT_SECONDS, 'retries': 0}
    app, consumers = _pipeline_app(
        tmp_path,
        monkeypatch,
        periodic={'election': {'provider': 'filesystem', 'resilience': resilience}},
    )
    due_source = make_sealed_db(history_folder(tmp_path), 'w-2', due_start(time.time()))
    timeouts_before = election_calls('filesystem', 'start', 'timeout')
    app._lifecycle._clock = _first_archive_slot_clock()

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    await wait_for(election.entered.is_set)
    (consumer,) = consumers
    consumer.deliver(
        [
            SourceMessage(topic='input', partition=0, offset=offset, value=b'{"x": 1}', timestamp=1_000)
            for offset in range(MESSAGE_COUNT)
        ]
    )
    await wait_for(lambda: consumer.committed.get(0) == MESSAGE_COUNT, timeout=COMMIT_BUDGET_SECONDS)
    # The commits landed while the election start still hung.
    assert election_calls('filesystem', 'start', 'timeout') == timeouts_before
    await wait_for(lambda: election_calls('filesystem', 'start', 'timeout') >= timeouts_before + 1)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert election.calls == ['start']
    # No lease, no archive: the due window's source is still in the history folder.
    assert Path(due_source).exists()


async def test_a_pass_still_running_at_stop_leaves_the_registry_its_withdraw(tmp_path, monkeypatch):
    """The wait for an archive pass keeps one registry call's time limit, so the card withdraw after it gets time."""
    provider = _RecordingRegistry()
    _install_registry_factory(monkeypatch, lambda _options: provider)
    # The reserve (0.5 s) is shorter than the drain budget (1 s), which is
    # shorter than the stuck window.
    registry = {'provider': 'filesystem', 'resilience': {'timeout_seconds': 0.5}}
    app, _consumers = _pipeline_app(tmp_path, monkeypatch, fleet={'registry': registry})
    in_window = threading.Event()
    window_may_end = threading.Event()

    def stuck_pass(shared_dir, **kwargs) -> None:
        in_window.set()
        # A window that does not end when the stop asks, like a merge on a slow mount.
        window_may_end.wait(WAIT_FOR_TIMEOUT_SECONDS)

    monkeypatch.setattr(core_mod, 'run_archive_pass', stuck_pass)
    app._lifecycle._clock = _first_archive_slot_clock()
    run = asyncio.create_task(app._lifecycle._async_run())
    try:
        await wait_for(lambda: app.is_ready)
        await wait_for(in_window.is_set)
        with capture_logs() as logs:
            app._lifecycle._handle_signal()
            await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)
    finally:
        window_may_end.set()

    (abandoned,) = [entry for entry in logs if entry['event'] == 'recorder_archive_pass_abandoned']
    # The wait stopped when about the reserve was left of the drain budget.
    assert abandoned['waited_seconds'] <= 0.55
    assert provider.withdrawn


class _OrderRecordingElection(MemoryElection):
    """A memory election that writes each call into a shared ``order``."""

    def __init__(self, order: list[str]) -> None:
        super().__init__(ElectionStore())
        self.order = order

    async def start(self, ctx: ElectionContext) -> None:
        self.order.append('election.start')
        await super().start(ctx)

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.order.append('election.acquire')
        return await super().acquire(scope, token, lease_seconds)

    async def is_held(self, scope: str, token: str) -> bool:
        self.order.append('election.is_held')
        return await super().is_held(scope, token)

    async def release(self, scope: str, token: str) -> None:
        self.order.append('election.release')
        await super().release(scope, token)

    async def stop(self) -> None:
        self.order.append('election.stop')
        await super().stop()


class _OrderRecordingRegistry(_RecordingRegistry):
    """A registry provider that writes its withdraw into a shared ``order``."""

    def __init__(self, order: list[str]) -> None:
        super().__init__()
        self.order = order

    async def withdraw(self, worker: str, instance: str) -> None:
        self.order.append('registry.withdraw')
        await super().withdraw(worker, instance)


async def test_shutdown_waits_for_the_pass_then_stops_the_election_then_the_registry(tmp_path, monkeypatch):
    order: list[str] = []
    election = _OrderRecordingElection(order)
    _install_registry_factory(monkeypatch, lambda _options: _OrderRecordingRegistry(order))
    _install_election_factory(monkeypatch, lambda _options: election)
    app, _consumers = _pipeline_app(
        tmp_path,
        monkeypatch,
        executor={'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': WAIT_FOR_TIMEOUT_SECONDS},
        fleet={'registry': {'resilience': {'timeout_seconds': 0.5}}},
    )
    in_window = threading.Event()

    def pass_in_a_window(shared_dir, *, fence, stop, **kwargs) -> None:
        in_window.set()
        stop.wait(WAIT_FOR_TIMEOUT_SECONDS)
        # The rest of the window, after the recorder's stop asked the pass to end.
        time.sleep(0.2)
        fence.is_held()
        order.append('pass.end')

    monkeypatch.setattr(core_mod, 'run_archive_pass', pass_in_a_window)
    app._lifecycle._clock = _first_archive_slot_clock()
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    await wait_for(in_window.is_set)

    with capture_logs() as logs:
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    # A cancelled slot is not a failed run.
    assert 'periodic_task_failed' not in [entry['event'] for entry in logs]
    # The slot loop was cancelled while it held the lease: nothing is released.
    assert order == [
        'election.start',
        'election.acquire',
        'election.is_held',
        'pass.end',
        'election.stop',
        'registry.withdraw',
    ]


async def _wait_until(condition) -> None:
    while not condition():
        await asyncio.sleep(0.01)


# --- the user cluster tasks and their definition hash ---


class _ClusterTaskHandler(KafkaOnlyHandler):
    """A handler with one cluster task."""

    @periodic(60, scope='cluster')
    async def rebuild_index(self) -> None:
        return None


class _CardRecording(_FixedPeersRegistry):
    """A provider that keeps every published card and notes the withdraw."""

    def __init__(self) -> None:
        super().__init__([])
        self.cards: list[dict] = []
        self.withdrawn = False

    async def publish(self, card: dict) -> None:
        self.cards.append(card)

    async def withdraw(self, worker: str, instance: str) -> None:
        self.withdrawn = True


def _parked_clock() -> VirtualClock:
    """A slot-loop clock whose sleepers never wake: no slot comes during the test."""
    clock = VirtualClock(NOW)
    clock.limit = NOW
    return clock


async def _run_until_ready_then_stop(app: DrakkarApp, until_ready=None) -> None:
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    if until_ready is not None:
        await wait_for(until_ready)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)


async def test_first_published_card_carries_the_cluster_hash(tmp_path, monkeypatch):
    provider = _CardRecording()
    app, _source, _order = _runnable_app(
        tmp_path, monkeypatch, lambda _options: provider, handler=_ClusterTaskHandler()
    )
    app._lifecycle._clock = _parked_clock()

    await _run_until_ready_then_stop(app, lambda: provider.cards)

    expected = cluster_tasks_hash(
        [
            ClusterTaskDefinition(name='rebuild_index', interval=60, cron=None, lease_seconds=60),
            ClusterTaskDefinition(name=ARCHIVE_TASK_NAME, interval=3600, cron=None, lease_seconds=60),
        ]
    )
    assert app.cluster_tasks_hash == expected
    assert provider.cards[0]['state'] == 'starting'
    assert provider.cards[0]['features'] == {'periodic': {'cluster_hash': expected}}
    checker = getattr(app._fleet._on_refresh, '__self__', None)
    assert isinstance(checker, ClusterHashChecker)


async def test_roster_runs_before_the_registry_and_any_sink_connects(tmp_path, monkeypatch):
    app, _source, order = _runnable_app(
        tmp_path, monkeypatch, lambda _options: _CardRecording(), handler=_ClusterTaskHandler()
    )
    app._lifecycle._clock = _parked_clock()
    lifecycle = app._lifecycle
    build_roster, create_connections, start_fleet = (
        lifecycle._build_periodic_roster,
        lifecycle._create_connections,
        lifecycle._start_fleet,
    )

    def recorded_roster() -> None:
        order.append('roster')
        build_roster()

    def recorded_connections() -> None:
        order.append('connections')
        create_connections()

    async def recorded_fleet() -> None:
        order.append('fleet')
        await start_fleet()

    monkeypatch.setattr(lifecycle, '_build_periodic_roster', recorded_roster)
    monkeypatch.setattr(lifecycle, '_create_connections', recorded_connections)
    monkeypatch.setattr(lifecycle, '_start_fleet', recorded_fleet)
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock(side_effect=lambda: order.append('sinks')))

    await _run_until_ready_then_stop(app)

    assert order[:5] == ['connections', 'roster', 'ui_and_recorder', 'fleet', 'sinks']


@pytest.mark.parametrize(
    ('handler', 'ui_enabled', 'logged'),
    [
        (_ClusterTaskHandler, False, True),
        (KafkaOnlyHandler, False, False),
        (_ClusterTaskHandler, True, False),
    ],
    ids=['registry-off', 'no-cluster-task', 'registry-on'],
)
async def test_check_off_logged_once_when_the_registry_is_off_and_cluster_tasks_exist(
    tmp_path, handler, ui_enabled, logged
):
    overrides = {} if handler is _ClusterTaskHandler else {'history': {'archive': {'enabled': False}}}
    app = _app(tmp_path, handler=handler(), ui={'enabled': ui_enabled, 'release': {'enabled': False}}, **overrides)
    app._lifecycle._build_periodic_roster()

    with capture_logs() as logs:
        await app._lifecycle._start_fleet()
    if app._fleet is not None:
        await app._fleet.stop()

    lines = [entry for entry in logs if entry['event'] == 'periodic_cluster_check_off']
    expected = [
        {
            'event': 'periodic_cluster_check_off',
            'log_level': 'info',
            'category': 'periodic',
            # The archive needs the UI server too, so only the handler's own task counts.
            'cluster_tasks': 1,
            'reason': 'the worker registry is off',
        }
    ]
    assert lines == (expected if logged else [])


class _PipelineGuardHandler(KafkaOnlyHandler):
    @periodic(30, scope='cluster')
    async def pipeline_guard(self) -> None:
        return None


class _DownElection(MemoryElection):
    """An election whose service is down: every start raises."""

    async def start(self, ctx: ElectionContext) -> None:
        raise ConnectionError('election service unreachable')


async def test_startup_with_failing_election_start_still_runs_the_pipeline(tmp_path, monkeypatch):
    logs = RecordingLogger()
    monkeypatch.setattr(cluster_tasks_module, 'logger', logs)
    registry = _CardRecording()
    app, source, _order = _runnable_app(
        tmp_path, monkeypatch, lambda _options: registry, handler=_PipelineGuardHandler()
    )
    _install_election_factory(monkeypatch, lambda _options: _DownElection(ElectionStore()))
    app._config.periodic.election.resilience.retries = 0
    clock = VirtualClock(NOW)
    # Two slots of the task, SLOT and SLOT + 30, both within one minute.
    clock.limit = SLOT + 45
    app._lifecycle._clock = clock
    failed_before = REGISTRY.get_sample_value(
        'drakkar_periodic_cluster_slots_total', {'name': 'pipeline_guard', 'outcome': 'election_failed'}
    )

    await _run_until_ready_then_stop(app, clock.parked.is_set)

    assert source.has_run
    failed = REGISTRY.get_sample_value(
        'drakkar_periodic_cluster_slots_total', {'name': 'pipeline_guard', 'outcome': 'election_failed'}
    )
    assert failed - (failed_before or 0.0) == 2
    warnings = [line for line in logs.named('periodic_slot_skipped') if line['name'] == 'pipeline_guard']
    assert [(line['slot'], line['error'], line['suppressed']) for line in warnings] == [(SLOT, 'start error', 0)]


async def test_an_election_stop_that_raises_is_logged_and_the_card_is_still_withdrawn(tmp_path, monkeypatch):
    provider = _CardRecording()
    app, _source, order = _runnable_app(tmp_path, monkeypatch, lambda _options: provider, handler=_ClusterTaskHandler())
    app._lifecycle._clock = _parked_clock()
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    assert app._cluster_election is not None
    app._cluster_election.stop = AsyncMock(side_effect=RuntimeError('election stop broke'))

    with capture_logs() as logs:
        app._lifecycle._handle_signal()
        await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    (failure,) = [entry for entry in logs if entry['event'] == 'periodic_election_stop_failed']
    assert (failure['category'], failure['error']) == ('periodic', 'election stop broke')
    assert provider.withdrawn
    assert 'ui_server.stop' in order
