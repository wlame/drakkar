"""HttpSource: the webapp server as an input source."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.config import DrakkarConfig
from drakkar.metrics import drain_timeout_hit
from drakkar.sources.base import SourceContext
from drakkar.sources.http import HttpSource


class _FakeWebApp:
    def __init__(self, *, bind_ok: bool = True, stop_error: Exception | None = None):
        self.bind_ok = bind_ok
        self.stop_error = stop_error
        self.started = False
        self.stopped_with = None
        self.shutdown_event = MagicMock()
        self.inflight_count = 0

    def start_in_thread(self):
        self.started = True

    def wait_until_ready(self, timeout: float):
        if not self.bind_ok:
            raise TimeoutError('bind failed')

    def stop(self, drain_timeout: float):
        self.stopped_with = drain_timeout
        if self.stop_error is not None:
            raise self.stop_error


def _config(**http) -> DrakkarConfig:
    return DrakkarConfig(
        ui={'release': {'enabled': False}},
        executor={'binary_path': '/usr/bin/echo'},
        sources={'http': {'enabled': True, **http}},
    )


def _context(config: DrakkarConfig) -> SourceContext:
    return SourceContext(
        config=config,
        handler=MagicMock(),
        executor_pool=MagicMock(active_count=0),
        sink_manager=MagicMock(),
        dlq_sink=None,
        recorder=None,
        throughput=None,
        worker_id='w',
        cluster_name='',
        on_collect=AsyncMock(),
        is_worker_ready=lambda: True,
    )


def _source(monkeypatch, bind_ok=True, config=None) -> tuple[HttpSource, _FakeWebApp]:
    fake = _FakeWebApp(bind_ok=bind_ok)
    monkeypatch.setattr('drakkar.sources.http.WebApp', lambda app, cfg: fake)
    source = HttpSource(MagicMock())
    source.bind(_context(config if config is not None else _config()))
    return source, fake


async def test_start_binds_and_is_ready(monkeypatch):
    source, fake = _source(monkeypatch)
    assert source.is_ready is False
    await source.start()
    assert fake.started is True
    assert source.is_ready is True


async def test_bind_failure_raises_and_leaves_no_webapp(monkeypatch):
    source, _ = _source(monkeypatch, bind_ok=False)
    with pytest.raises(TimeoutError):
        await source.start()
    assert source.webapp is None
    assert source.is_ready is False


async def test_signal_stop_sets_gate_and_run_returns(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    import asyncio

    task = asyncio.create_task(source.run())
    await asyncio.sleep(0)
    assert not task.done()
    source.signal_stop()
    await asyncio.wait_for(task, timeout=1)
    fake.shutdown_event.set.assert_called_once()
    assert source.is_ready is False


async def test_stop_passes_remaining_budget(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    source.signal_stop()
    assert await source.drain(deadline=time.monotonic() + 1) is True
    await source.stop(deadline=time.monotonic() + 3)
    assert 2.5 <= fake.stopped_with <= 3


async def test_snapshot_reports_inflight(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    fake.inflight_count = 2
    assert source.snapshot() == {'inflight_requests': 2}


async def test_start_reads_the_port_from_the_bound_context(monkeypatch):
    """The context is built after ``on_startup``, so a config the hook
    replaced is the one the webapp server is given."""
    seen: list = []
    monkeypatch.setattr('drakkar.sources.http.WebApp', lambda app, cfg: seen.append(cfg) or _FakeWebApp())
    source = HttpSource(MagicMock())
    source.bind(_context(_config(port=8099)))

    await source.start()

    assert seen[0].port == 8099


async def test_bind_failure_cleanup_error_does_not_mask_the_bind_error(monkeypatch):
    fake = _FakeWebApp(bind_ok=False, stop_error=RuntimeError('cleanup exploded'))
    monkeypatch.setattr('drakkar.sources.http.WebApp', lambda app, cfg: fake)
    source = HttpSource(MagicMock())
    source.bind(_context(_config()))

    with capture_logs() as cap, pytest.raises(TimeoutError, match='bind failed'):
        await source.start()

    assert any(entry['event'] == 'webapp_cleanup_failed' for entry in cap)


async def test_drain_returns_true_once_inflight_reaches_zero(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    fake.inflight_count = 2
    source.signal_stop()

    async def _finish_requests():
        await asyncio.sleep(0.1)
        fake.inflight_count = 0

    finisher = asyncio.create_task(_finish_requests())
    assert await source.drain(deadline=time.monotonic() + 5) is True
    await finisher


async def test_drain_returns_false_and_warns_when_requests_outlive_the_deadline(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    fake.inflight_count = 1
    source.signal_stop()

    before = drain_timeout_hit._value.get()
    with capture_logs() as cap:
        assert await source.drain(deadline=time.monotonic() + 0.1) is False

    timeout = next(entry for entry in cap if entry['event'] == 'webapp_drain_timeout')
    assert timeout['inflight_requests'] == 1
    assert timeout['worker_id'] == 'w'
    assert drain_timeout_hit._value.get() - before == 1


async def test_drain_that_finishes_in_time_does_not_tick_the_timeout_counter(monkeypatch):
    source, fake = _source(monkeypatch)
    await source.start()
    fake.inflight_count = 0
    source.signal_stop()

    before = drain_timeout_hit._value.get()
    assert await source.drain(deadline=time.monotonic() + 1) is True
    assert drain_timeout_hit._value.get() == before
