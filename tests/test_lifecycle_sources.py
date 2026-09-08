"""Lifecycle orchestration of input sources, with stub sources."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from tests.test_sources_validation import BothHandler, KafkaOnlyHandler


class StubSource:
    """A ``Source`` that records the lifecycle calls it receives."""

    def __init__(self, name: str, *, start_error: Exception | None = None, drain_error: BaseException | None = None):
        self.name = name
        self.calls: list[str] = []
        self._ready = False
        self._stopped = asyncio.Event()
        self.start_error = start_error
        self.drain_error = drain_error
        self.ctx: Any = None
        self.drain_deadline: float | None = None

    def bind(self, ctx) -> None:
        self.calls.append('bind')
        self.ctx = ctx

    async def start(self) -> None:
        self.calls.append('start')
        if self.start_error:
            raise self.start_error
        self._ready = True

    async def run(self) -> None:
        self.calls.append('run')
        await self._stopped.wait()

    @property
    def is_ready(self) -> bool:
        return self._ready

    def signal_stop(self) -> None:
        self.calls.append('signal_stop')
        self._ready = False
        self._stopped.set()

    async def drain(self, deadline: float) -> bool:
        self.calls.append('drain')
        self.drain_deadline = deadline
        if self.drain_error:
            raise self.drain_error
        return True

    async def stop(self, deadline: float) -> None:
        self.calls.append('stop')

    def snapshot(self) -> dict[str, Any]:
        return {}


class FailingSource(StubSource):
    """A source whose ``run`` raises once the worker is up."""

    def __init__(self, name: str, error: Exception):
        super().__init__(name)
        self.error = error

    async def run(self) -> None:
        self.calls.append('run')
        raise self.error


def _app(monkeypatch, *sources, handler=None, **config_overrides) -> DrakkarApp:
    """A worker whose disk, sink and executor layers are stubbed out."""
    config_overrides.setdefault('sources', {'kafka': {'enabled': True, 'startup_align_enabled': False}})
    config = DrakkarConfig(
        # An empty ``db_dir`` keeps the watchdog (and every other on-disk
        # artifact) out of the test; ``test_watchdog.py`` covers that path.
        ui={'enabled': False, 'release': {'enabled': False}, 'recorder': {'db_dir': ''}},
        metrics={'enabled': False},
        runtime_health={'enabled': False},
        executor={'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': 1},
        sinks={'kafka': {'output_results': {'topic': 'output-results'}}},
        **config_overrides,
    )

    app = DrakkarApp(handler=handler or KafkaOnlyHandler(), config=config, worker_id='w')
    app.sources = list(sources)
    # Keep the sink layer hermetic.
    monkeypatch.setattr(app._sink_manager, 'connect_all', AsyncMock())
    monkeypatch.setattr(app._sink_manager, 'close_all', AsyncMock())
    monkeypatch.setattr(app, '_build_dlq', lambda: setattr(app, '_dlq_sink', None))
    monkeypatch.setattr(app._lifecycle, '_build_executor_pool', AsyncMock())
    app._executor_pool = MagicMock(active_count=0, max_executors=1)
    return app


async def _wait_ready(app: DrakkarApp) -> None:
    for _ in range(200):
        if app.is_ready:
            return
        await asyncio.sleep(0.01)


async def test_sources_started_in_order_and_worker_ready(monkeypatch):
    a, b = StubSource('a'), StubSource('b')
    app = _app(monkeypatch, a, b)
    run = asyncio.create_task(app._lifecycle._async_run())
    await _wait_ready(app)
    assert app.is_ready is True
    assert a.calls[:3] == ['bind', 'start', 'run']
    assert b.calls[:3] == ['bind', 'start', 'run']

    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    # Two ``signal_stop`` calls on purpose: the signal handler makes every
    # ``run`` loop exit, and ``_shutdown`` signals again because it also runs
    # for a start failure, where no signal ever arrived. ``signal_stop`` is
    # idempotent by contract.
    assert a.calls[3:] == ['signal_stop', 'signal_stop', 'drain', 'stop']
    assert b.calls[3:] == ['signal_stop', 'signal_stop', 'drain', 'stop']


async def test_sources_bound_with_worker_context(monkeypatch):
    a = StubSource('a')
    app = _app(monkeypatch, a)
    run = asyncio.create_task(app._lifecycle._async_run())
    await _wait_ready(app)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert a.ctx.worker_id == 'w'
    assert a.ctx.handler is app._handler
    assert a.ctx.sink_manager is app._sink_manager
    assert a.ctx.dlq_sink is None
    assert callable(a.ctx.on_collect)
    assert a.ctx.is_worker_ready() is False


async def test_start_failure_runs_teardown_and_reraises(monkeypatch):
    a = StubSource('a', start_error=RuntimeError('bind failed'))
    b = StubSource('b')
    app = _app(monkeypatch, a, b)

    with pytest.raises(RuntimeError, match='bind failed'):
        await app._lifecycle._async_run()

    assert 'start' not in b.calls
    assert 'stop' in a.calls
    app._sink_manager.close_all.assert_awaited()


async def test_run_failure_surfaces_after_teardown(monkeypatch):
    a = FailingSource('a', RuntimeError('poll loop died'))
    b = StubSource('b')
    app = _app(monkeypatch, a, b)

    with pytest.raises(RuntimeError, match='poll loop died'):
        await app._lifecycle._async_run()

    assert 'stop' in b.calls
    app._sink_manager.close_all.assert_awaited()


async def test_not_ready_while_any_source_not_ready(monkeypatch):
    a, b = StubSource('a'), StubSource('b')
    app = _app(monkeypatch, a, b)
    a._ready = True
    assert app.is_ready is False
    b._ready = True
    assert app.is_ready is True
    app._stopping = True
    assert app.is_ready is False


async def test_sources_share_one_drain_deadline(monkeypatch):
    a, b = StubSource('a'), StubSource('b')
    app = _app(monkeypatch, a, b)
    run = asyncio.create_task(app._lifecycle._async_run())
    await _wait_ready(app)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert a.drain_deadline is not None
    assert a.drain_deadline == b.drain_deadline


async def test_one_failing_drain_does_not_stop_the_others(monkeypatch):
    # ``_drain_sources`` gathers with return_exceptions, so a source that
    # blows up its own drain is logged and the rest still finish theirs.
    a = StubSource('a', drain_error=RuntimeError('drain blew up'))
    b = StubSource('b')
    app = _app(monkeypatch, a, b)
    run = asyncio.create_task(app._lifecycle._async_run())
    await _wait_ready(app)
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert b.calls.count('drain') == 1
    assert a.calls[-1] == 'stop'
    assert b.calls[-1] == 'stop'
    app._sink_manager.close_all.assert_awaited()


async def test_cancellation_during_drain_still_runs_the_whole_teardown(monkeypatch):
    # The case the try/finally exists for: an orchestrator whose grace
    # period expires cancels the shutdown mid-drain. Every subsystem must
    # still close and the watchdog must still be marked clean, or the next
    # startup reads the empty body as a SIGKILL.
    a, b = StubSource('a'), StubSource('b')
    app = _app(monkeypatch, a, b)
    app._lifecycle._bind_sources()
    watchdog = MagicMock()
    app._lifecycle._watchdog = watchdog

    async def _cancelled(sources, deadline):
        raise asyncio.CancelledError

    monkeypatch.setattr(app._lifecycle, '_drain_sources', _cancelled)

    with pytest.raises(asyncio.CancelledError):
        await app._lifecycle._shutdown()

    assert a.calls[-1] == 'stop'
    assert b.calls[-1] == 'stop'
    watchdog.mark_clean.assert_called_once()
    app._sink_manager.close_all.assert_awaited()


async def test_shutdown_before_binding_leaves_sources_alone(monkeypatch):
    # A startup failure before ``_bind_sources`` gets here too; an unbound
    # source has acquired nothing, and driving it would bury the real
    # startup error under teardown noise.
    a = StubSource('a')
    app = _app(monkeypatch, a)

    await app._lifecycle._shutdown()

    assert a.calls == ['signal_stop']


async def test_disabled_source_with_bad_config_logs_warning(monkeypatch):
    app = _app(
        monkeypatch,
        StubSource('a'),
        sources={
            'kafka': {'enabled': True, 'startup_align_enabled': False},
            'http': {'enabled': False, 'path': 'bad'},
        },
    )
    with capture_logs() as captured:
        await app._lifecycle._warn_ignored_source_config()

    warnings = [entry for entry in captured if entry['event'] == 'source_config_ignored']
    assert len(warnings) == 1
    assert warnings[0]['source'] == 'http'
    assert any('sources.http.path' in error for error in warnings[0]['errors'])


async def test_enabled_source_config_never_warns(monkeypatch):
    app = _app(monkeypatch, StubSource('a'))
    with capture_logs() as captured:
        await app._lifecycle._warn_ignored_source_config()

    assert [entry for entry in captured if entry['event'] == 'source_config_ignored'] == []


async def test_worker_info_reports_the_consumer_group_only_with_a_kafka_source(monkeypatch):
    """An HTTP-only worker joins no group, so the label must be empty."""
    from drakkar.metrics import worker_info

    monkeypatch.setattr('drakkar.lifecycle.start_metrics_server', lambda config: None)

    kafka_app = _app(
        monkeypatch,
        StubSource('a'),
        sources={'kafka': {'enabled': True, 'consumer_group': 'search-workers', 'startup_align_enabled': False}},
    )
    await kafka_app._lifecycle._start_observability()
    assert worker_info._value['consumer_group'] == 'search-workers'

    http_app = _app(monkeypatch, StubSource('a'), handler=BothHandler(), sources={'http': {'enabled': True}})
    await http_app._lifecycle._start_observability()
    assert worker_info._value['consumer_group'] == ''


async def test_kafka_source_reads_the_config_on_startup_returned(monkeypatch):
    """``on_startup`` runs after ``DrakkarApp.__init__`` built the sources.

    A source that kept the config it was constructed with would subscribe
    to the old topic while the logs, metrics and recorder report the new
    one, so every runtime read goes through the bound context instead.
    """
    from drakkar.sources.registry import build_sources

    built: list[dict] = []

    def _fake_consumer(**kwargs):
        built.append(kwargs)
        return MagicMock(subscribe=AsyncMock())

    monkeypatch.setattr('drakkar.sources.kafka.KafkaConsumer', _fake_consumer)

    class ReplacingHandler(KafkaOnlyHandler):
        async def on_startup(self, config):
            replacement = config.model_copy(deep=True)
            replacement.sources.kafka.topic = 'topic-from-on-startup'
            return replacement

    app = _app(monkeypatch, handler=ReplacingHandler())
    app.sources = build_sources(app)

    await app._lifecycle._run_on_startup()
    app._lifecycle._bind_sources()
    source = app.kafka_source
    assert source is not None
    await source.start()

    assert built[0]['source'].topic == 'topic-from-on-startup'
