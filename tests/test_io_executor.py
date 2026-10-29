"""Tests for the thread pool sizes: ``threads.io`` (the default executor) and the ``threads`` bounds."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, ThreadsConfig
from drakkar.lifecycle import AppLifecycle
from drakkar.offload import resolve_max_threads
from tests.conftest import kafka_connections


def make_lifecycle(max_threads: int | None) -> AppLifecycle:
    config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    config.threads.io = max_threads
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


async def test_null_keeps_pythons_own_executor():
    lifecycle = make_lifecycle(max_threads=None)
    loop = asyncio.get_running_loop()
    before = loop._default_executor

    lifecycle._wire_io_executor()

    assert loop._default_executor is before
    name = await asyncio.to_thread(lambda: threading.current_thread().name)
    assert not name.startswith('drakkar-io')


@pytest.mark.parametrize(
    ('section', 'message'),
    [
        ({'io': 0}, 'threads.io must be null or from 1 to 512'),
        ({'io': 513}, 'threads.io must be null or from 1 to 512'),
        ({'offload': 0}, 'threads.offload must be null or from 1 to 32'),
        ({'offload': 33}, 'threads.offload must be null or from 1 to 32'),
    ],
    ids=['io-zero', 'io-above', 'offload-zero', 'offload-above'],
)
def test_threads_null_derives_and_zero_is_refused(section, message):
    threads = ThreadsConfig()
    assert (threads.io, threads.offload) == (None, None)
    # Null derives: Python's own default executor stays, and the offload pool scales with the executor pool.
    assert resolve_max_threads(threads.offload, executor_pool_max=13) == 4
    assert ThreadsConfig(io=1, offload=32).model_dump() == {'io': 1, 'offload': 32}

    with pytest.raises(ValidationError, match=message):
        ThreadsConfig(**section)
