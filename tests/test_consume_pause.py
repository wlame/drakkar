"""Tests for the timed consume pause (drakkar/consume_pause.py).

Covers the controller's contract — bounded pause with auto-resume, manual
resume at any time, replacement of an active pause — and the two
coordination rules with the rest of the pause machinery:

1. Backpressure's low-watermark resume must not fire while a debug pause
   is active (exercised through the REAL ``_poll_loop`` body, one
   iteration).
2. Partitions assigned during a debug pause arrive paused (exercised
   through the real ``_on_assign``).

Stall-paused partitions are never touched in either direction.

Isolation: real ``DrakkarApp`` + ``AsyncMock`` consumer, same pattern as
the backpressure tests in test_app.py. No Kafka, no sockets.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    UIConsumePauseConfig,
)
from drakkar.consume_pause import ConsumerNotReadyError
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import ExecutorTask


class _Handler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f't-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]


@pytest.fixture
def pause_config() -> DrakkarConfig:
    cfg = DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )
    cfg.ui.consume_pause = UIConsumePauseConfig(enabled=True, durations_seconds=[15, 60])
    return cfg


@pytest.fixture
async def app(pause_config):
    from drakkar.executor import ExecutorPool

    app = DrakkarApp(handler=_Handler(), config=pause_config)
    app._executor_pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    app._consumer = AsyncMock()
    yield app
    await app.consume_pause.resume()
    for proc in list(app.processors.values()):
        await proc.stop()


# --- controller basics --------------------------------------------------------


async def test_pause_pauses_assigned_partitions_and_reports_state(app):
    app._lifecycle._on_assign([0, 3])
    state = await app.consume_pause.pause(60)

    app._consumer.pause.assert_awaited_once_with([0, 3])
    assert state['active'] is True
    assert state['enabled'] is True
    assert state['durations_seconds'] == [15, 60]
    assert state['requested_seconds'] == 60
    assert state['resume_at_ms'] >= int(time.time() * 1000)


async def test_pause_without_consumer_raises_not_ready(app):
    app._consumer = None
    with pytest.raises(ConsumerNotReadyError):
        await app.consume_pause.pause(15)


async def test_pause_excludes_stall_paused_partitions(app):
    app._lifecycle._on_assign([0, 1])
    app._stalled_partitions.add(1)
    await app.consume_pause.pause(60)
    app._consumer.pause.assert_awaited_once_with([0])


async def test_second_pause_replaces_the_deadline(app):
    app._lifecycle._on_assign([0])
    first = await app.consume_pause.pause(15)
    second = await app.consume_pause.pause(3600)
    assert second['resume_at_ms'] > first['resume_at_ms']
    assert second['requested_seconds'] == 3600
    assert app.consume_pause.active


async def test_manual_resume_resumes_partitions_and_is_idempotent(app):
    app._lifecycle._on_assign([0, 2])
    await app.consume_pause.pause(3600)

    state = await app.consume_pause.resume()
    assert state['active'] is False
    assert state['resume_at_ms'] is None
    app._consumer.resume.assert_awaited_once_with([0, 2])

    # Second resume: no extra consumer calls, same inactive state.
    state = await app.consume_pause.resume()
    assert state['active'] is False
    app._consumer.resume.assert_awaited_once()


async def test_auto_resume_fires_at_the_deadline(app):
    app._lifecycle._on_assign([0])
    await app.consume_pause.pause(1)
    # Shrink the wait: cancel the real timer and drive the deadline directly.
    # (pause() scheduled a 1s sleep — too slow for a unit test.)
    app.consume_pause._cancel_timer()
    await app.consume_pause._auto_resume(0)

    assert not app.consume_pause.active
    app._consumer.resume.assert_awaited_once_with([0])


async def test_resume_defers_to_active_backpressure(app):
    """When backpressure still holds the partitions, a debug resume must not
    restart fetching — the backpressure loop resumes once queues drain."""
    app._lifecycle._on_assign([0])
    await app.consume_pause.pause(3600)
    app._paused = True  # backpressure engaged while debug pause was active

    state = await app.consume_pause.resume()
    assert state['active'] is False
    app._consumer.resume.assert_not_awaited()


# --- coordination with the real lifecycle code --------------------------------


async def _run_one_poll_iteration(app):
    """Run the REAL _poll_loop for exactly one iteration: the scripted
    poll_batch flips _running off so the while-loop exits after one pass."""

    async def _poll_batch(*args, **kwargs):
        app._running = False
        return []

    app._consumer.poll_batch = AsyncMock(side_effect=_poll_batch)
    app._running = True
    await app._lifecycle._poll_loop()


async def test_poll_loop_backpressure_resume_blocked_while_debug_paused(app):
    app._lifecycle._on_assign([0])
    await app.consume_pause.pause(3600)
    app._paused = True  # backpressure holds; queues are empty (below low watermark)

    await _run_one_poll_iteration(app)

    app._consumer.resume.assert_not_awaited()
    assert app._paused is True


async def test_poll_loop_backpressure_resume_works_when_not_debug_paused(app):
    app._lifecycle._on_assign([0])
    app._paused = True  # backpressure holds; queues empty; no debug pause

    await _run_one_poll_iteration(app)

    app._consumer.resume.assert_awaited_once_with([0])
    assert app._paused is False


async def test_partitions_assigned_during_debug_pause_arrive_paused(app):
    app._lifecycle._on_assign([0])
    await app.consume_pause.pause(3600)
    assert not app._paused  # backpressure NOT active — only the debug pause

    app._lifecycle._on_assign([5, 7])
    # The lifecycle pauses new partitions via a background task.
    for _ in range(50):
        if any(call.args[0] == [5, 7] for call in app._consumer.pause.call_args_list):
            break
        await asyncio.sleep(0.01)
    assert any(call.args[0] == [5, 7] for call in app._consumer.pause.call_args_list)


# --- config validation ---------------------------------------------------------


def test_consume_pause_config_defaults_disabled_with_presets():
    cfg = UIConsumePauseConfig()
    assert cfg.enabled is False
    assert cfg.durations_seconds == [15, 60, 300, 900]


@pytest.mark.parametrize('durations', [[], [0], [3601], list(range(1, 12))])
def test_consume_pause_config_rejects_bad_duration_presets(durations):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        UIConsumePauseConfig(durations_seconds=durations)
