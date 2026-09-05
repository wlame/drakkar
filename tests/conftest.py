"""Shared test fixtures for Drakkar tests."""

import asyncio
from pathlib import Path

import pytest
import yaml

from drakkar import partition as partition_module
from drakkar.config import UIConfig, UIRecorderConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)

_RECORDER_FIELDS = frozenset(UIRecorderConfig.model_fields)


def make_ui_config(**kwargs) -> UIConfig:
    """Build a ``UIConfig`` from flat kwargs, routing recorder-tier fields.

    Test convenience so the many call sites that mix server settings with
    persistence settings (``make_ui_config(port=8080, db_dir='/tmp')``)
    stay one-liners after the ``debug.*``→``ui.*`` merge. ``recorder=`` /
    ``release=`` kwargs still pass through for tests that want the nested
    form explicitly.
    """
    recorder_kwargs = {key: kwargs.pop(key) for key in list(kwargs) if key in _RECORDER_FIELDS}
    if recorder_kwargs:
        assert 'recorder' not in kwargs, 'mix of flat recorder kwargs and recorder= is ambiguous'
        kwargs['recorder'] = UIRecorderConfig(**recorder_kwargs)
    return UIConfig(**kwargs)


# Default ceiling for wait_for. It is a safety net that turns a stuck test
# into a readable failure — not an assertion about how fast the code is. The
# suite runs one worker per core and many of these conditions wait on a real
# subprocess, so under that contention a few seconds is not a generous budget:
# five seconds produced a flaky failure roughly one run in three. Nothing pays
# this cost when the condition holds, and pytest-timeout still bounds a true hang
# at 60 s.
WAIT_FOR_TIMEOUT_SECONDS = 20.0


async def wait_for(condition, timeout=WAIT_FOR_TIMEOUT_SECONDS, interval=0.05):
    """Poll until condition() returns True or timeout expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError(f'Condition not met within {timeout}s')


# The partition loop blocks this long on an empty queue before waking to
# retry a commit. In production a second is right — it costs nothing and
# bounds how long a quiet partition holds progress. In a test it is dead
# waiting: every test that stops a running processor pays it once, which was
# most of the suite's tests over one second.
TEST_IDLE_POLL_TIMEOUT = 0.02


@pytest.fixture(autouse=True)
def fast_idle_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the partition loop's idle wake-up for every test.

    Only the wake-up cadence changes: the loop does the same work, and a
    test that needs the production value can set it back.
    """
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', TEST_IDLE_POLL_TIMEOUT)


@pytest.fixture
def source_message() -> SourceMessage:
    return SourceMessage(
        topic='test-topic',
        partition=0,
        offset=42,
        key=b'key-1',
        value=b'{"data": "hello"}',
        timestamp=1700000000,
    )


@pytest.fixture
def executor_task() -> ExecutorTask:
    return ExecutorTask(
        task_id='task-001',
        args=['--input', 'test.txt'],
        metadata={'source': 'test'},
        source_offsets=[42],
    )


@pytest.fixture
def executor_result(executor_task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(
        exit_code=0,
        stdout='result line 1\nresult line 2\n',
        stderr='',
        duration_seconds=1.5,
        task=executor_task,
    )


@pytest.fixture
def executor_error(executor_task: ExecutorTask) -> ExecutorError:
    return ExecutorError(
        task=executor_task,
        exit_code=1,
        stderr='error: file not found',
    )


@pytest.fixture
def minimal_config_dict() -> dict:
    return {
        'executor': {
            'binary_path': '/usr/bin/echo',
        },
    }


@pytest.fixture
def full_config_dict() -> dict:
    return {
        'kafka': {
            'brokers': 'kafka1:9092,kafka2:9092',
            'source_topic': 'input-events',
            'consumer_group': 'drakkar-workers',
            'max_poll_records': 200,
            'max_poll_interval_ms': 600_000,
            'session_timeout_ms': 30_000,
            'heartbeat_interval_ms': 5_000,
        },
        'executor': {
            'binary_path': '/usr/local/bin/processor',
            'max_executors': 40,
            'task_timeout_seconds': 300,
            'window_size': 100,
            'max_retries': 5,
            'drain_timeout_seconds': 10,
            'backpressure_high_multiplier': 16,
            'backpressure_low_multiplier': 2,
        },
        'sinks': {
            'kafka': {'results': {'topic': 'output-results'}},
            'postgres': {'main': {'dsn': 'postgresql://user:pass@db:5432/app', 'pool_min': 5, 'pool_max': 20}},
        },
        'metrics': {
            'enabled': True,
            'port': 9091,
        },
        'logging': {
            'level': 'DEBUG',
            'format': 'console',
        },
    }


@pytest.fixture
def config_yaml_file(full_config_dict: dict, tmp_path: Path) -> Path:
    config_path = tmp_path / 'drakkar.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(full_config_dict, f)
    return config_path


@pytest.fixture
def minimal_config_yaml_file(minimal_config_dict: dict, tmp_path: Path) -> Path:
    config_path = tmp_path / 'drakkar.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(minimal_config_dict, f)
    return config_path
