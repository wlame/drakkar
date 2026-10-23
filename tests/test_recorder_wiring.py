"""How the worker gives the recorder its folders, runs its archive task under the worker's election, and when it seals."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from drakkar.metrics import provider_calls
from drakkar.providers import lookup
from drakkar.providers.connections import Connections
from drakkar.recorder import core as core_mod
from drakkar.recorder.election import ELECTION_ENTRY_POINT_GROUP, ThreadFence
from tests.archive_helpers import idle_archive_task, run_won_slot
from tests.conftest import wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection
from tests.test_lifecycle_sources import StubSource
from tests.test_sources_validation import KafkaOnlyHandler

DRAIN_TIMEOUT_SECONDS = 4


def _app(tmp_path, **overrides) -> DrakkarApp:
    values = {
        'storage': {'dir': str(tmp_path / 'storage')},
        'shared': {'dir': str(tmp_path / 'shared')},
        'cluster_name': 'search',
        'ui': {'enabled': True, 'port': 8123, 'release': {'enabled': False}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'fleet': {'registry': {'provider': 'none'}},
        'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': DRAIN_TIMEOUT_SECONDS},
        'sinks': {'kafka': {'output_results': {'topic': 'output-results'}}},
        'sources': {'kafka': {'enabled': True, 'startup_align_enabled': False}},
    }
    values.update(overrides)
    return DrakkarApp(handler=KafkaOnlyHandler(), config=DrakkarConfig(**values), worker_id='w-1')


async def test_the_recorder_seals_into_the_shared_history_folder_with_the_process_instance(tmp_path, monkeypatch):
    app = _app(tmp_path)
    (tmp_path / 'storage').mkdir()
    monkeypatch.setattr('drakkar.uiserver.server.UIServer', MagicMock(return_value=AsyncMock()))

    await app._lifecycle._start_ui_and_recorder()
    try:
        assert app._recorder is not None
        assert app._recorder.history_dir == str(tmp_path / 'shared' / 'history' / 'search')
        assert app._recorder._instance == app._instance
    finally:
        await app._recorder.stop()


async def test_the_archive_task_runs_under_the_configured_election_with_its_options_resilience_and_connections(
    tmp_path, monkeypatch
):
    store = ElectionStore()
    election = MemoryElection(store)
    received: dict = {}

    def factory(options):
        received['options'] = dict(options)
        return election

    def discover(group: str) -> dict:
        return {'example-election': factory} if group == ELECTION_ENTRY_POINT_GROUP else {}

    monkeypatch.setattr(lookup, 'discover_plugin_factories', discover)
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: received.update(kwargs))
    monkeypatch.setattr('drakkar.uiserver.server.UIServer', MagicMock(return_value=AsyncMock()))
    election_config = {
        'provider': 'example-election',
        'plugin_options': {'address': 'consul:8500'},
        'resilience': {'timeout_seconds': 1.5, 'retries': 0},
    }
    app = _app(tmp_path, periodic={'election': election_config})
    (tmp_path / 'storage').mkdir()
    started_ok = provider_calls.labels(feature='election', provider='example-election', op='start', outcome='ok')
    started_before = started_ok._value.get()

    app._lifecycle._create_connections()
    app._lifecycle._build_periodic_roster()
    await app._lifecycle._start_ui_and_recorder()
    cluster_election = app._cluster_election
    assert app._recorder is not None
    assert cluster_election is not None
    archive_task = app._recorder.archive_cluster_task()
    assert archive_task is not None
    try:
        token = await run_won_slot(archive_task, cluster_election, cluster='search')
        assert cluster_election.caller.operation_bound_seconds == 1.5
    finally:
        await app._recorder.stop()
        await cluster_election.stop(None)

    assert received['options'] == {'address': 'consul:8500'}
    ctx = election.started_with
    assert ctx is not None
    assert (ctx.worker, ctx.instance, ctx.cluster) == ('w-1', app._instance, 'search')
    assert ctx.shared_dir == str(tmp_path / 'shared')
    assert ctx.options == {'address': 'consul:8500'}
    assert ctx.connections is app._connections
    fence = received['fence']
    assert isinstance(fence, ThreadFence)
    assert (fence.provider_name, fence.token) == ('example-election', token)
    assert store.leases == {}
    assert started_ok._value.get() == started_before + 1
    assert election.calls['stop'] == 1


def _runnable_app(tmp_path, monkeypatch) -> tuple[DrakkarApp, MagicMock, list[str]]:
    """A worker with a stub source and a stand-in recorder that logs what the lifecycle asks of it."""
    app = _app(tmp_path)
    source = StubSource('kafka')
    app.sources = [source]
    order: list[str] = []
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock())
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)

    recorder = MagicMock()
    recorder.stop = AsyncMock()
    # A plain method on the real recorder. The stand-in task's first slot is an hour away at most,
    # and a won slot would do nothing, so no archive work runs here.
    recorder.archive_cluster_task = MagicMock(return_value=idle_archive_task())
    recorder.start_leftover_sealing = MagicMock(side_effect=lambda: order.append('leftover_sealing'))
    real_start_sources = app._lifecycle._start_sources

    async def start_sources() -> None:
        await real_start_sources()
        order.append('sources_started')

    async def start_ui_and_recorder() -> None:
        app._recorder = recorder

    monkeypatch.setattr(app._lifecycle, '_start_sources', start_sources)
    monkeypatch.setattr(app._lifecycle, '_start_ui_and_recorder', start_ui_and_recorder)
    return app, recorder, order


async def test_leftover_sealing_starts_once_the_sources_run(tmp_path, monkeypatch):
    app, _recorder, order = _runnable_app(tmp_path, monkeypatch)

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert order == ['sources_started', 'leftover_sealing']


async def test_the_shared_connections_exist_before_the_recorder_starts(tmp_path, monkeypatch):
    app, _recorder, _order = _runnable_app(tmp_path, monkeypatch)
    start_stand_in = app._lifecycle._start_ui_and_recorder
    seen: dict = {}

    async def start_ui_and_recorder() -> None:
        seen['connections'] = app._connections
        await start_stand_in()

    monkeypatch.setattr(app._lifecycle, '_start_ui_and_recorder', start_ui_and_recorder)
    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert isinstance(seen['connections'], Connections)
    assert seen['connections'] is app._connections


async def test_the_recorder_stops_with_the_shutdown_time_that_is_left(tmp_path, monkeypatch):
    app, recorder, _order = _runnable_app(tmp_path, monkeypatch)

    run = asyncio.create_task(app._lifecycle._async_run())
    await wait_for(lambda: app.is_ready)
    signalled = time.monotonic()
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    (call,) = recorder.stop.await_args_list
    timeout = call.kwargs['timeout']
    assert 0 < timeout <= DRAIN_TIMEOUT_SECONDS
    assert timeout >= DRAIN_TIMEOUT_SECONDS - (time.monotonic() - signalled) - 0.1


async def test_lifecycle_creates_storage_dir_and_starts_no_leftover_sealing_with_none(tmp_path, monkeypatch):
    app = _app(
        tmp_path, ui={'enabled': True, 'port': 8123, 'release': {'enabled': False}, 'recorder': {'persistence': 'none'}}
    )
    monkeypatch.setattr('drakkar.uiserver.server.UIServer', MagicMock(return_value=AsyncMock()))

    await app._lifecycle._prepare_storage()
    await app._lifecycle._start_ui_and_recorder()
    try:
        assert app._recorder is not None
        assert app._recorder.persistence == 'none'
        app._recorder.start_leftover_sealing()
        assert app._recorder._leftover_task is None
        assert (tmp_path / 'storage').is_dir()
        assert list((tmp_path / 'storage').iterdir()) == []
        assert app._recorder.history_dir == str(tmp_path / 'shared' / 'history' / 'search')
    finally:
        await app._recorder.stop()


async def test_warn_if_archives_unbounded_not_called_when_the_archive_does_not_run(tmp_path, monkeypatch):
    warned = MagicMock()
    monkeypatch.setattr('drakkar.lifecycle.warn_if_archives_unbounded', warned)
    monkeypatch.setattr('drakkar.uiserver.server.UIServer', MagicMock(return_value=AsyncMock()))
    none_app = _app(
        tmp_path,
        ui={'enabled': True, 'port': 8123, 'release': {'enabled': False}, 'recorder': {'persistence': 'none'}},
    )
    await none_app._lifecycle._start_ui_and_recorder()
    await none_app._recorder.stop()
    warned.assert_not_called()

    file_app = _app(tmp_path)
    (tmp_path / 'storage').mkdir(exist_ok=True)
    await file_app._lifecycle._start_ui_and_recorder()
    await file_app._recorder.stop()
    warned.assert_called_once()
