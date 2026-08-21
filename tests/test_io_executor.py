"""Tests for the io.max_threads default-executor sizing (drakkar/lifecycle.py)."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

from drakkar.config import DrakkarConfig
from drakkar.lifecycle import AppLifecycle


def make_lifecycle(max_threads: int) -> AppLifecycle:
    config = DrakkarConfig()
    config.io.max_threads = max_threads
    lifecycle = AppLifecycle.__new__(AppLifecycle)
    lifecycle._app = SimpleNamespace(_config=config)
    return lifecycle


async def test_configured_value_installs_a_named_default_executor():
    lifecycle = make_lifecycle(max_threads=64)

    lifecycle._wire_io_executor()

    # to_thread runs on the replacement pool: the thread-name prefix is the
    # observable proof that set_default_executor took effect.
    name = await asyncio.to_thread(lambda: threading.current_thread().name)
    assert name.startswith('drakkar-io')
    # asyncio.run's own shutdown_default_executor tears the pool down after
    # this test's loop closes — no lifecycle-side cleanup exists to assert.


async def test_zero_keeps_pythons_own_executor():
    lifecycle = make_lifecycle(max_threads=0)
    loop = asyncio.get_running_loop()
    before = loop._default_executor

    lifecycle._wire_io_executor()

    assert loop._default_executor is before
    name = await asyncio.to_thread(lambda: threading.current_thread().name)
    assert not name.startswith('drakkar-io')
