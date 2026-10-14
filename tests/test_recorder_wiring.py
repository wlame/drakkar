"""How the worker gives the recorder its folders and when it seals."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from tests.conftest import wait_for
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
