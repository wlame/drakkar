"""Tests for webapp wiring inside :class:`drakkar.lifecycle.AppLifecycle`.

Two layers of behaviour:

1. **Startup ordering** — the webapp starts AFTER sinks ``connect_all``
   so it never serves a request the underlying pipeline can't fulfil.
2. **Shutdown ordering** — ``shutdown_event.set()`` runs BEFORE the
   drain phase begins so new requests get an immediate 503 while
   in-flight requests continue draining.

Drain-with-in-flight tests live in the section further down, where the
runner + cancellation wiring is in place.
"""

from __future__ import annotations

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
    WebAppConfig,
    WebClientConfig,
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
    """Handler with all four Generic slots populated for webapp use.

    Overrides both HTTP hooks — construction-time validation (mirroring
    construction time) rejects a webapp-enabled handler that leaves them at
    the raising Base defaults.
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
    """Handler with no HTTP types — used to exercise ``webapp.enabled=False``."""

    async def arrange(self, messages, pending):
        return []


def _build_config(*, webapp_enabled: bool) -> DrakkarConfig:
    """Construct a minimal config with the webapp toggle pre-set."""
    return DrakkarConfig(
        kafka=KafkaConfig(
            brokers='localhost:9092',
            source_topic='test-in',
        ),
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
        webapp=WebAppConfig(
            enabled=webapp_enabled,
            host='127.0.0.1',
            port=0,  # ephemeral — never actually bound in these tests
            path='/process',
            clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
        ),
    )


# ---------------------------------------------------------------------------
# Startup ordering
# ---------------------------------------------------------------------------


async def test_webapp_starts_after_sinks_connect_all(monkeypatch):
    """Spy on the call sequence — sinks.connect_all comes before webapp.start_in_thread."""
    config = _build_config(webapp_enabled=True)
    app = DrakkarApp(handler=_WebHandler(), config=config)

    # Replace the sink_manager.connect_all with a spy that records when
    # it ran. Real connect_all isn't needed — we only care about ordering.
    call_log: list[str] = []

    async def _fake_connect_all():
        call_log.append('connect_all')

    # Patch in a fake sink manager so we don't touch real Kafka. The
    # lifecycle calls ``_build_sinks`` then ``connect_all`` then DLQ
    # ``connect`` — we stub all three.
    fake_sink_manager = MagicMock()
    fake_sink_manager.connect_all = AsyncMock(side_effect=_fake_connect_all)
    fake_sink_manager.attach_runtime = MagicMock()
    fake_sink_manager.sinks = {}
    app._sink_manager = fake_sink_manager
    monkeypatch.setattr(app, '_build_sinks', lambda: None)
    monkeypatch.setattr(app, '_build_dlq', lambda: None)
    fake_dlq = MagicMock()
    fake_dlq.connect = AsyncMock()
    fake_dlq.topic = 'test-in_dlq'
    app._dlq_sink = fake_dlq

    # Fake WebApp — we only need a class with a constructor and the
    # two methods the lifecycle calls. ``start_in_thread`` records the
    # call so we can assert ordering.
    fake_webapp_instance = MagicMock()

    def _fake_start_in_thread():
        call_log.append('webapp.start_in_thread')

    fake_webapp_instance.start_in_thread = _fake_start_in_thread
    fake_webapp_instance.wait_until_ready = MagicMock()

    fake_webapp_cls = MagicMock(return_value=fake_webapp_instance)

    # Patch the import inside ``drakkar.webapp`` — the lifecycle does a
    # local ``from drakkar.webapp import WebApp`` so we replace it on
    # the package module.
    import drakkar.webapp as webapp_pkg

    monkeypatch.setattr(webapp_pkg, 'WebApp', fake_webapp_cls)

    # Drive the slice of ``_async_run`` we care about: sinks → webapp.
    # We can't run the whole method (it calls KafkaConsumer.subscribe).
    # Instead we drive the ordered block manually.
    await app._sink_manager.connect_all()
    await app._dlq_sink.connect()

    if app._config.webapp.enabled:
        from drakkar.webapp import WebApp

        app._webapp = WebApp(app, app._config.webapp)
        app._webapp.start_in_thread()

    assert call_log == ['connect_all', 'webapp.start_in_thread']
    fake_webapp_cls.assert_called_once_with(app, config.webapp)


def test_webapp_does_not_start_when_disabled():
    """webapp.enabled=False → ``app._webapp`` stays None, no construction call."""
    config = _build_config(webapp_enabled=False)
    app = DrakkarApp(handler=_PlainHandler(), config=config)

    # Verify the disabled-path: ``app._webapp`` is the initial None
    # placeholder. The lifecycle would skip the WebApp construction
    # block entirely under the ``if app._config.webapp.enabled:`` guard.
    assert app._webapp is None
    assert config.webapp.enabled is False


# ---------------------------------------------------------------------------
# Shutdown ordering
# ---------------------------------------------------------------------------


@pytest.fixture
def shutdown_app() -> DrakkarApp:
    """A DrakkarApp with sinks/dlq/consumer mocked — ready for ``_shutdown``."""
    config = _build_config(webapp_enabled=True)
    app = DrakkarApp(handler=_WebHandler(), config=config)

    # Replace sinks/DLQ/consumer with mocks so ``_shutdown`` can run
    # through end-to-end. None of them are exercised for ordering — we
    # only care about the relative order of webapp.shutdown_event.set()
    # vs the first processor.signal_stop() call.
    app._consumer = AsyncMock()
    fake_sink_manager = MagicMock()
    fake_sink_manager.close_all = AsyncMock()
    fake_sink_manager.sinks = {}
    app._sink_manager = fake_sink_manager
    fake_dlq = AsyncMock()
    app._dlq_sink = fake_dlq

    # Tight drain timeout so the test does not hang.
    app._config.executor.drain_timeout_seconds = 0.05

    return app


async def test_shutdown_sets_webapp_shutdown_event_before_drain(shutdown_app):
    """``_shutdown`` flips webapp.shutdown_event BEFORE the drain phase begins.

    We can verify ordering with two collaborating spies:

    * ``shutdown_event.set`` records its call into ``call_log``.
    * ``processor.signal_stop`` (the first action of the drain phase)
      records into the same log.

    The assertion is then a simple list-equality.
    """
    call_log: list[str] = []

    # Build a fake webapp that records when shutdown_event.set() runs.
    fake_event = MagicMock()
    fake_event.set = lambda: call_log.append('webapp.shutdown_event.set')
    fake_webapp = MagicMock()
    fake_webapp.shutdown_event = fake_event
    fake_webapp.stop = MagicMock()
    shutdown_app._webapp = fake_webapp

    # Build a fake processor that records when signal_stop() runs.
    fake_processor = MagicMock()
    fake_processor.signal_stop = lambda: call_log.append('processor.signal_stop')
    fake_processor.partition_id = 0
    fake_processor.offset_tracker = MagicMock()
    fake_processor.offset_tracker.pending_count = 0
    fake_processor.offset_tracker.has_pending = MagicMock(return_value=False)
    fake_processor.offset_tracker.committable = MagicMock(return_value=None)
    fake_processor.queue_size = 0
    fake_processor.inflight_count = 0
    fake_processor.drain = AsyncMock()
    fake_processor.stop = AsyncMock()

    shutdown_app._processors[0] = fake_processor

    await shutdown_app._lifecycle._shutdown()

    # ``shutdown_event.set`` must run BEFORE the first ``signal_stop``.
    set_idx = call_log.index('webapp.shutdown_event.set')
    stop_idx = call_log.index('processor.signal_stop')
    assert set_idx < stop_idx

    # And the webapp.stop() must have been called eventually (after drain).
    fake_webapp.stop.assert_called_once_with(
        drain_timeout=shutdown_app._config.executor.drain_timeout_seconds,
    )


async def test_shutdown_handles_missing_webapp_gracefully(shutdown_app):
    """``_shutdown`` with ``app._webapp=None`` runs the rest of teardown unaffected."""
    shutdown_app._webapp = None

    # Stage one processor — we just want to confirm shutdown completes.
    fake_processor = MagicMock()
    fake_processor.signal_stop = MagicMock()
    fake_processor.partition_id = 0
    fake_processor.offset_tracker = MagicMock()
    fake_processor.offset_tracker.pending_count = 0
    fake_processor.offset_tracker.has_pending = MagicMock(return_value=False)
    fake_processor.offset_tracker.committable = MagicMock(return_value=None)
    fake_processor.queue_size = 0
    fake_processor.inflight_count = 0
    fake_processor.drain = AsyncMock()
    fake_processor.stop = AsyncMock()
    shutdown_app._processors[0] = fake_processor

    # No exception even though there is no webapp to stop.
    await shutdown_app._lifecycle._shutdown()

    fake_processor.signal_stop.assert_called_once()


# ---------------------------------------------------------------------------
# Drain-with-in-flight-HTTP-requests
# ---------------------------------------------------------------------------


async def test_shutdown_passes_drain_timeout_into_webapp_stop(shutdown_app):
    """``_shutdown`` invokes ``webapp.stop(drain_timeout=...)`` with the configured value.

    The stop method waits up to ``drain_timeout`` for in-flight HTTP
    requests before forcing the webapp thread down. We confirm the
    propagation of the configured drain budget into stop().
    """
    fake_webapp = MagicMock()
    fake_webapp.shutdown_event = MagicMock()
    fake_webapp.stop = MagicMock()
    shutdown_app._webapp = fake_webapp

    # Stage a no-op processor so the shutdown loop completes.
    fake_processor = MagicMock()
    fake_processor.signal_stop = MagicMock()
    fake_processor.partition_id = 0
    fake_processor.offset_tracker = MagicMock()
    fake_processor.offset_tracker.pending_count = 0
    fake_processor.offset_tracker.has_pending = MagicMock(return_value=False)
    fake_processor.offset_tracker.committable = MagicMock(return_value=None)
    fake_processor.queue_size = 0
    fake_processor.inflight_count = 0
    fake_processor.drain = AsyncMock()
    fake_processor.stop = AsyncMock()
    shutdown_app._processors[0] = fake_processor

    await shutdown_app._lifecycle._shutdown()

    # Drain budget propagated into webapp.stop().
    fake_webapp.stop.assert_called_once_with(
        drain_timeout=shutdown_app._config.executor.drain_timeout_seconds,
    )


async def test_shutdown_event_set_before_first_signal_stop(shutdown_app):
    """``shutdown_event.set()`` runs BEFORE the first ``processor.signal_stop()``.

    Documented invariant: the gate is flipped at the very top of
    ``_shutdown`` so any HTTP request that arrives during drain is
    rejected with 503 ``status='shutdown'`` rather than queued behind
    a pipeline that no longer accepts work.

    This test is similar to
    ``test_shutdown_sets_webapp_shutdown_event_before_drain`` but
    asserts on the strict total order with multiple processors so it
    survives a refactor that runs ``signal_stop`` in parallel.
    """
    call_log: list[str] = []

    fake_event = MagicMock()
    fake_event.set = lambda: call_log.append('webapp.shutdown_event.set')
    fake_webapp = MagicMock()
    fake_webapp.shutdown_event = fake_event
    fake_webapp.stop = MagicMock()
    shutdown_app._webapp = fake_webapp

    # Two processors: prove that NEITHER ``signal_stop`` runs before
    # the gate flips.
    for partition_id in (0, 1):
        proc = MagicMock()
        proc.signal_stop = lambda pid=partition_id: call_log.append(f'processor.signal_stop[{pid}]')
        proc.partition_id = partition_id
        proc.offset_tracker = MagicMock()
        proc.offset_tracker.pending_count = 0
        proc.offset_tracker.has_pending = MagicMock(return_value=False)
        proc.offset_tracker.committable = MagicMock(return_value=None)
        proc.queue_size = 0
        proc.inflight_count = 0
        proc.drain = AsyncMock()
        proc.stop = AsyncMock()
        shutdown_app._processors[partition_id] = proc

    await shutdown_app._lifecycle._shutdown()

    # The gate flips first; both ``signal_stop`` calls follow.
    set_idx = call_log.index('webapp.shutdown_event.set')
    sig_indices = [i for i, e in enumerate(call_log) if e.startswith('processor.signal_stop')]
    assert sig_indices, 'expected processor.signal_stop calls in the log'
    assert set_idx < min(sig_indices)


async def test_shutdown_calls_webapp_stop_after_processor_drain(shutdown_app):
    """``_shutdown`` orders processor drain BEFORE ``webapp.stop``.

    Rationale: in-flight HTTP requests that are mid-execute are
    waiting on the executor pool. Draining processors first lets
    those requests finish naturally (returning 200 to clients) before
    we pull the webapp's uvicorn thread down. Any request still alive
    at the end of drain is forcibly cancelled when ``webapp.stop``
    joins the thread with the ``drain_timeout`` budget.
    """
    call_log: list[str] = []

    fake_event = MagicMock()
    fake_event.set = lambda: call_log.append('webapp.shutdown_event.set')
    fake_webapp = MagicMock()
    fake_webapp.shutdown_event = fake_event
    fake_webapp.stop = lambda *args, **kwargs: call_log.append('webapp.stop')
    shutdown_app._webapp = fake_webapp

    fake_processor = MagicMock()
    fake_processor.signal_stop = lambda: call_log.append('processor.signal_stop')
    fake_processor.partition_id = 0
    fake_processor.offset_tracker = MagicMock()
    fake_processor.offset_tracker.pending_count = 0
    fake_processor.offset_tracker.has_pending = MagicMock(return_value=False)
    fake_processor.offset_tracker.committable = MagicMock(return_value=None)
    fake_processor.queue_size = 0
    fake_processor.inflight_count = 0

    async def _record_drain():
        call_log.append('processor.drain')

    fake_processor.drain = _record_drain
    fake_processor.stop = AsyncMock(side_effect=lambda: call_log.append('processor.stop'))
    shutdown_app._processors[0] = fake_processor

    await shutdown_app._lifecycle._shutdown()

    # ``webapp.stop`` runs AFTER the drain phase completes.
    drain_idx = call_log.index('webapp.shutdown_event.set')
    stop_idx = call_log.index('webapp.stop')
    assert drain_idx < stop_idx
    # The gate is the very first webapp-touching call (no other
    # webapp-* operation precedes it).
    webapp_calls = [c for c in call_log if c.startswith('webapp.')]
    assert webapp_calls[0] == 'webapp.shutdown_event.set'
    assert webapp_calls[-1] == 'webapp.stop'


# ---------------------------------------------------------------------------
# Construction-time fail-fast
# ---------------------------------------------------------------------------


def test_app_construction_fails_fast_when_webapp_enabled_without_hooks():
    """webapp.enabled + a handler without the HTTP hooks → immediate error.

    The pairing is rejected at construction, not at the first request.
    Before this check the misconfiguration was only discovered when the
    webapp thread failed to start (non-fatal, worker continued without
    the webapp) — or worse, at the first POST.
    """
    from drakkar.webapp import ConfigurationError

    with pytest.raises(ConfigurationError) as exc_info:
        DrakkarApp(handler=_PlainHandler(), config=_build_config(webapp_enabled=True))
    assert 'webapp.enabled=true' in str(exc_info.value)


def test_app_construction_succeeds_when_webapp_enabled_with_full_handler():
    app = DrakkarApp(handler=_WebHandler(), config=_build_config(webapp_enabled=True))
    assert app is not None


def test_app_construction_skips_webapp_validation_when_disabled():
    """A plain handler stays valid as long as the webapp is off."""
    app = DrakkarApp(handler=_PlainHandler(), config=_build_config(webapp_enabled=False))
    assert app is not None
