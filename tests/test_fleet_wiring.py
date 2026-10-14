"""How the worker starts, feeds and stops the registry service."""

import asyncio
import json
import re
import stat
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from drakkar.fleet import plugins
from drakkar.fleet.plugins import UnknownRegistryProviderError
from drakkar.fleet.provider import PeerCardList, RegistryContext
from drakkar.fleet.providers import FilesystemRegistry, FleetRegistryFolderError, StaticRegistry
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, wait_for
from tests.fleet_fakes import MemoryRegistry, MemoryStore
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


def _app(tmp_path, **overrides) -> DrakkarApp:
    return DrakkarApp(handler=KafkaOnlyHandler(), config=_config(tmp_path, **overrides), worker_id='w-1')


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


async def test_the_static_provider_gets_the_configured_peers(tmp_path):
    peers = [{'name': 'w-2', 'cluster': 'search', 'url': 'http://10.0.0.2:8080/'}]
    app = _app(tmp_path, fleet={'registry': {'provider': 'static', 'static': {'peers': peers}}})

    await app._lifecycle._start_fleet()
    await wait_for(lambda: app.fleet.snapshot().refreshed_at is not None)
    await app._fleet.stop()

    assert isinstance(app.fleet._provider, StaticRegistry)
    assert [(view.card['worker'], view.card['cluster']) for view in app.fleet.peer_views()] == [('w-2', 'search')]


async def test_a_plugin_provider_gets_the_plugin_options(tmp_path, monkeypatch):
    received = {}

    class _Plugin(StaticRegistry):
        async def start(self, ctx: RegistryContext) -> None:
            received['ctx'] = ctx

    def factory(options):
        received['options'] = dict(options)
        return _Plugin([])

    monkeypatch.setattr(plugins, 'discover_plugin_factories', lambda: {'consul': factory})
    app = _app(tmp_path, fleet={'registry': {'provider': 'consul', 'plugin_options': {'address': 'consul:8500'}}})

    await app._lifecycle._start_fleet()
    await wait_for(lambda: 'ctx' in received)
    await app._fleet.stop()

    assert received['options'] == {'address': 'consul:8500'}
    assert received['ctx'].options == {'address': 'consul:8500'}
    assert received['ctx'].worker == 'w-1'


async def test_an_unknown_provider_fails_startup(tmp_path, monkeypatch):
    monkeypatch.setattr(plugins, 'discover_plugin_factories', dict)
    app = _app(tmp_path, fleet={'registry': {'provider': 'etcd'}})

    with pytest.raises(UnknownRegistryProviderError, match="'etcd'"):
        await app._lifecycle._start_fleet()


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


async def test_provider_none_runs_no_registry_and_logs_nothing(tmp_path):
    app = _app(tmp_path, fleet={'registry': {'provider': 'none'}})

    with capture_logs() as logs:
        await app._lifecycle._start_fleet()

    assert app.fleet is None
    assert logs == []


async def test_the_ui_off_with_provider_none_logs_nothing(tmp_path):
    app = _app(tmp_path, ui={'enabled': False, 'release': {'enabled': False}}, fleet={'registry': {'provider': 'none'}})

    with capture_logs() as logs:
        await app._lifecycle._start_fleet()

    assert logs == []


# --- the card folder ---


async def test_an_explicit_folder_that_cannot_be_created_fails_startup(tmp_path):
    blocker = tmp_path / 'blocker'
    blocker.write_text('a file where the folder should be')
    app = _app(tmp_path, fleet={'registry': {'filesystem': {'dir': str(blocker / 'fleet')}}})

    with pytest.raises(FleetRegistryFolderError, match=re.escape('fleet.registry.filesystem.dir')):
        await app._lifecycle._start_fleet()


async def test_an_explicit_folder_is_created_owner_only(tmp_path):
    folder = tmp_path / 'registry'
    app = _app(tmp_path, fleet={'registry': {'filesystem': {'dir': str(folder)}}})

    await app._lifecycle._start_fleet()
    await app._fleet.stop()

    assert stat.S_IMODE((folder / 'workers').stat().st_mode) == 0o700


async def test_a_derived_folder_that_cannot_be_created_only_fails_publishes(tmp_path):
    """The derived folder is best effort: the worker runs, the registry counts errors and retries."""
    blocker = tmp_path / 'blocker'
    blocker.write_text('a file where the folder should be')
    app = _app(tmp_path, shared={'dir': str(blocker)})

    await app._lifecycle._start_fleet()
    await app._fleet.stop()

    assert isinstance(app.fleet._provider, FilesystemRegistry)


# --- states and order in the worker's life ---


class _RecordingRegistry(StaticRegistry):
    """A provider that records the state of every published card."""

    def __init__(self) -> None:
        super().__init__([])
        self.states: list[str] = []
        self.withdrawn = False

    async def publish(self, card: dict) -> None:
        self.states.append(card['state'])

    async def withdraw(self, worker: str, instance: str) -> None:
        self.withdrawn = True


def _runnable_app(tmp_path, monkeypatch, provider_factory) -> tuple[DrakkarApp, StubSource, list[str]]:
    """A worker with stub sources, no real UI server or recorder, and a registry from ``provider_factory``."""
    app = _app(tmp_path)
    source = StubSource('kafka')
    app.sources = [source]
    order: list[str] = []
    monkeypatch.setattr(plugins, 'discover_plugin_factories', lambda: {'filesystem': provider_factory})
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock())
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)

    ui_server = MagicMock()
    ui_server.stop = AsyncMock(side_effect=lambda: order.append('ui_server.stop'))
    recorder = MagicMock()
    recorder.stop = AsyncMock(side_effect=lambda **_kwargs: order.append('recorder.stop'))

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

    async def recorded_stop() -> None:
        order.append('fleet.stop')
        await original_stop()

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


async def test_the_cache_engine_reads_the_registrys_peer_views(tmp_path, monkeypatch):
    provider = StaticRegistry([{'name': 'w-2', 'cluster': 'search', 'url': 'http://10.0.0.2:8080/'}])
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
    await wait_for(lambda: store.cards.get('w-1', ({},))[0].get('features') == {'cache': {}})
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
    app._config.fleet.registry.timeout_seconds = 0.05

    run = asyncio.create_task(app._lifecycle._async_run())
    await asyncio.wait_for(_wait_until(lambda: app.is_ready), timeout=WAIT_FOR_TIMEOUT_SECONDS)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert source.calls[:3] == ['bind', 'start', 'run']
    assert 'stop' in source.calls


async def _wait_until(condition) -> None:
    while not condition():
        await asyncio.sleep(0.01)
