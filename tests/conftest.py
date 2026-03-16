"""Shared test fixtures for Drakkar tests."""

import asyncio
from pathlib import Path

import pytest
import yaml

from drakkar.models import (
    CollectResult,
    DBRow,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)


async def wait_for(condition, timeout=5.0, interval=0.05):
    """Poll until condition() returns True or timeout expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError(f'Condition not met within {timeout}s')


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
        task_id='task-001',
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
def pending_context(executor_task: ExecutorTask) -> PendingContext:
    return PendingContext(
        pending_tasks=[executor_task],
        pending_task_ids={executor_task.task_id},
    )


@pytest.fixture
def collect_result() -> CollectResult:
    return CollectResult(
        output_messages=[
            OutputMessage(key=b'out-key', value=b'{"result": "ok"}'),
        ],
        db_rows=[
            DBRow(table='results', data={'id': 1, 'status': 'done'}),
        ],
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
            'target_topic': 'output-results',
            'consumer_group': 'drakkar-workers',
            'max_poll_records': 200,
            'max_poll_interval_ms': 600_000,
            'session_timeout_ms': 30_000,
            'heartbeat_interval_ms': 5_000,
        },
        'executor': {
            'binary_path': '/usr/local/bin/processor',
            'max_workers': 40,
            'task_timeout_seconds': 300,
            'window_size': 100,
        },
        'postgres': {
            'dsn': 'postgresql://user:pass@db:5432/app',
            'pool_min': 5,
            'pool_max': 20,
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
