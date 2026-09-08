"""Tests for the HTTP input source as the lifecycle drives it.

Two layers of behaviour:

1. **Startup ordering** — the HTTP source starts AFTER sinks
   ``connect_all`` so it never serves a request the underlying pipeline
   can't fulfil.
2. **Shutdown ordering** — the request gate closes BEFORE the drain phase
   begins, so new requests get an immediate 503 while in-flight requests
   continue draining, and the uvicorn thread is joined only after.

``tests/test_sources_http.py`` covers ``HttpSource`` in isolation; this
file is about how the lifecycle orders it against everything else.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import ExecutorTask


class _Input(BaseModel):
    a: int = 0


class _Output(BaseModel):
    b: int = 0


class _HttpReq(BaseModel):
    pattern: str = ''


class _HttpResp(BaseModel):
    matches: int = 0


class _WebHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Handler with all four Generic slots populated for HTTP use.

    Overrides both HTTP hooks — construction-time validation rejects an
    ``sources.http``-enabled handler that leaves them at the raising Base
    defaults.
    """

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['noop'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]

    async def arrange_http_request(self, req, pending):
        return []

    async def on_http_request_complete(self, group):
        return _HttpResp()


class _PlainHandler(BaseDrakkarHandler):
    """Handler with no HTTP types — used to exercise ``sources.http.enabled=False``."""

    async def arrange(self, messages, pending):
        return []


def _http_block(enabled: bool) -> dict:
    return {
        'enabled': enabled,
        'host': '127.0.0.1',
        'port': 0,  # ephemeral — never actually bound in these tests
        'path': '/process',
        'clients': [{'name': 'anonymous', 'token': '', 'rpm': 4}],
    }


def _build_config(*, http_enabled: bool, kafka_enabled: bool = True) -> DrakkarConfig:
    """Construct a minimal config with the HTTP source toggle pre-set."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092'),
        sources={
            'kafka': {'enabled': kafka_enabled, 'topic': 'test-in', 'startup_align_enabled': False},
            'http': _http_block(http_enabled),
        },
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_executors=2,
            task_timeout_seconds=10,
            window_size=5,
        ),
        sinks=SinksConfig(
            kafka={'results': KafkaSinkConfig(topic='test-out')},
        ),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui={'enabled': False, 'release': {'enabled': False}, 'recorder': {'db_dir': ''}},
        runtime_health={'enabled': False},
    )


class _FakeWebApp:
    """Just enough of ``WebApp`` for the lifecycle to drive it."""

    def __init__(self, call_log: list[str] | None = None):
        self._log = call_log if call_log is not None else []
        self.shutdown_event = MagicMock()
        self.shutdown_event.set = lambda: self._log.append('webapp.shutdown_event.set')
        self.stopped_with: float | None = None
        self.inflight_count = 0

    def start_in_thread(self) -> None:
        self._log.append('webapp.start_in_thread')

    def wait_until_ready(self, timeout: float) -> None:
        return None

    def stop(self, drain_timeout: float) -> None:
        self._log.append('webapp.stop')
        self.stopped_with = drain_timeout


# ---------------------------------------------------------------------------
# Startup ordering
# ---------------------------------------------------------------------------


async def test_http_source_starts_after_sinks_connect_all(monkeypatch):
    """Spy on the call sequence — sinks.connect_all comes before webapp.start_in_thread."""
    config = _build_config(http_enabled=True, kafka_enabled=False)
    app = DrakkarApp(handler=_WebHandler(), config=config)

    call_log: list[str] = []
    fake_webapp = _FakeWebApp(call_log)
    monkeypatch.setattr('drakkar.sources.http.WebApp', lambda app, cfg: fake_webapp)

    fake_sink_manager = MagicMock()
    fake_sink_manager.connect_all = AsyncMock(side_effect=lambda: call_log.append('connect_all'))
    fake_sink_manager.close_all = AsyncMock()
    fake_sink_manager.attach_runtime = MagicMock()
    fake_sink_manager.sinks = {}
    app._sink_manager = fake_sink_manager
    monkeypatch.setattr(app, '_build_sinks', lambda: None)
    monkeypatch.setattr(app, '_build_dlq', lambda: None)

    run = asyncio.create_task(app._lifecycle._async_run())
    for _ in range(200):
        if app.is_ready:
            break
        await asyncio.sleep(0.01)
    assert app.is_ready is True
    app._lifecycle._handle_signal()
    await asyncio.wait_for(run, timeout=5)

    assert call_log[:2] == ['connect_all', 'webapp.start_in_thread']
    assert app.http_source is not None


def test_http_source_absent_when_disabled():
    """sources.http.enabled=False → no HTTP source is built at all."""
    config = _build_config(http_enabled=False)
    app = DrakkarApp(handler=_PlainHandler(), config=config)

    assert app.http_source is None
    assert app._webapp is None


# ---------------------------------------------------------------------------
# Shutdown ordering
# ---------------------------------------------------------------------------


@pytest.fixture
def shutdown_app(monkeypatch) -> DrakkarApp:
    """A DrakkarApp with sinks/DLQ/consumer mocked — ready for ``_shutdown``."""
    config = _build_config(http_enabled=True)
    app = DrakkarApp(handler=_WebHandler(), config=config)

    assert app.kafka_source is not None
    app.kafka_source.consumer = AsyncMock()
    fake_sink_manager = MagicMock()
    fake_sink_manager.close_all = AsyncMock()
    fake_sink_manager.sinks = {}
    app._sink_manager = fake_sink_manager
    app._dlq_sink = AsyncMock()
    app._executor_pool = MagicMock(active_count=0, max_executors=2)
    # ``_shutdown`` drives bound sources; startup normally does this.
    app._lifecycle._bind_sources()

    # Tight drain timeout so the test does not hang.
    app._config.executor.drain_timeout_seconds = 0.05
    return app


def _stage_processor(app: DrakkarApp, partition_id: int, call_log: list[str]) -> MagicMock:
    """Put a no-op fake processor on the Kafka source's live partition map."""
    processor = MagicMock()
    processor.signal_stop = lambda pid=partition_id: call_log.append(f'processor.signal_stop[{pid}]')
    processor.partition_id = partition_id
    processor.is_dead = False
    processor.offset_tracker = MagicMock()
    processor.offset_tracker.pending_count = 0
    processor.offset_tracker.has_pending = MagicMock(return_value=False)
    processor.offset_tracker.committable = MagicMock(return_value=None)
    processor.queue_size = 0
    processor.inflight_count = 0
    processor.drain = AsyncMock()
    processor.stop = AsyncMock(side_effect=lambda **kwargs: call_log.append('processor.stop'))
    app._processors[partition_id] = processor
    return processor


async def test_shutdown_closes_the_request_gate_before_draining(shutdown_app):
    """The HTTP request gate closes before any processor is told to stop.

    ``is_ready`` drives the webapp's per-request gate, so flipping it at
    the very top of ``_shutdown`` is what makes a request arriving during
    the drain get an immediate 503 rather than queue behind a pipeline
    that no longer accepts work.
    """
    call_log: list[str] = []
    gate_when_signalled: list[bool] = []
    shutdown_app.http_source.webapp = _FakeWebApp(call_log)

    for partition_id in (0, 1):
        processor = _stage_processor(shutdown_app, partition_id, call_log)
        processor.signal_stop = lambda pid=partition_id: (
            gate_when_signalled.append(shutdown_app.is_ready),
            call_log.append(f'processor.signal_stop[{pid}]'),
        )

    await shutdown_app._lifecycle._shutdown()

    assert gate_when_signalled == [False, False]
    assert shutdown_app._stopping is True
    # The webapp's own gate flips in the same phase, before any drain work.
    assert call_log.index('webapp.shutdown_event.set') < call_log.index('processor.stop')


async def test_shutdown_stops_webapp_with_the_remaining_drain_budget(shutdown_app):
    """``webapp.stop`` receives what is left of the shared drain deadline.

    The stop joins the uvicorn thread, waiting up to that budget for
    in-flight HTTP requests; below the floor it still gets a moment to
    finish requests that are already settled.
    """
    fake_webapp = _FakeWebApp()
    shutdown_app.http_source.webapp = fake_webapp
    _stage_processor(shutdown_app, 0, [])

    await shutdown_app._lifecycle._shutdown()

    assert fake_webapp.stopped_with is not None
    assert 0 < fake_webapp.stopped_with <= 0.5


async def test_shutdown_handles_missing_webapp_gracefully(shutdown_app):
    """``_shutdown`` with no bound webapp runs the rest of teardown unaffected."""
    shutdown_app.http_source.webapp = None
    call_log: list[str] = []
    _stage_processor(shutdown_app, 0, call_log)

    # No exception even though there is no webapp to stop.
    await shutdown_app._lifecycle._shutdown()

    assert 'processor.signal_stop[0]' in call_log


async def test_shutdown_stops_webapp_after_processor_drain(shutdown_app):
    """``_shutdown`` orders the processor drain BEFORE ``webapp.stop``.

    In-flight HTTP requests that are mid-execute are waiting on the
    executor pool. Draining first lets them finish naturally (returning
    200 to clients) before the uvicorn thread is pulled down; whatever is
    still alive at the end of the drain is cancelled by the join.
    """
    call_log: list[str] = []
    fake_webapp = _FakeWebApp(call_log)
    shutdown_app.http_source.webapp = fake_webapp

    processor = _stage_processor(shutdown_app, 0, call_log)

    async def _record_drain():
        call_log.append('processor.drain')

    processor.drain = _record_drain
    processor.offset_tracker.has_pending = MagicMock(return_value=True)

    await shutdown_app._lifecycle._shutdown()

    webapp_calls = [entry for entry in call_log if entry.startswith('webapp.')]
    assert webapp_calls[0] == 'webapp.shutdown_event.set'
    assert webapp_calls[-1] == 'webapp.stop'
    assert call_log.index('processor.drain') < call_log.index('webapp.stop')


# ---------------------------------------------------------------------------
# Construction-time fail-fast
# ---------------------------------------------------------------------------


def test_app_construction_fails_fast_when_http_enabled_without_hooks():
    """sources.http.enabled + a handler without the HTTP hooks → immediate error.

    The pairing is rejected at construction, not at the first request.
    Before this check the misconfiguration was only discovered when the
    webapp thread failed to start — or worse, at the first POST.
    """
    from drakkar.webapp import ConfigurationError

    with pytest.raises(ConfigurationError) as exc_info:
        DrakkarApp(handler=_PlainHandler(), config=_build_config(http_enabled=True))
    assert 'sources.http.enabled=true' in str(exc_info.value)


def test_app_construction_succeeds_when_http_enabled_with_full_handler():
    app = DrakkarApp(handler=_WebHandler(), config=_build_config(http_enabled=True))
    assert app is not None


def test_app_construction_skips_http_validation_when_disabled():
    """A plain handler stays valid as long as the HTTP source is off."""
    app = DrakkarApp(handler=_PlainHandler(), config=_build_config(http_enabled=False))
    assert app is not None
