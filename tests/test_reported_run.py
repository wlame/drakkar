"""Tests for the optional ``task_run_seconds`` handler hook (drakkar/reported_run.py)."""

import math
from typing import Any
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar import reported_run
from drakkar.executor import ExecutorPool, ExecutorTaskError
from drakkar.handler import BaseDrakkarHandler, check_task_run_seconds_hook, task_run_seconds_hook
from drakkar.metrics import handler_hook_errors
from drakkar.models import ConfigurationError, ExecutorError, ExecutorResult, ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.providers.resilience import RateLimitedLog
from drakkar.request_ids import RequestIdResolver
from drakkar.webapp.runner import WebappRunner
from tests.conftest import wait_for


def hook_errors() -> float:
    return handler_hook_errors.labels(hook='task_run_seconds')._value.get()


def make_result(duration: float = 1.0, pid: int | None = 42, stdout: str = '') -> ExecutorResult:
    return ExecutorResult(
        exit_code=0,
        stdout=stdout,
        stderr='',
        duration_seconds=duration,
        task=ExecutorTask(task_id='t1', source_offsets=[0]),
        pid=pid,
    )


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> Clock:
    """A fresh process-wide rate limiter on a clock the test moves."""
    clock = Clock()
    monkeypatch.setattr(reported_run, '_failure_log', RateLimitedLog(clock))
    return clock


class LastLineHandler(BaseDrakkarHandler):
    """Reads ``elapsed=<seconds>`` from the last stdout line, as the docs show."""

    def task_run_seconds(self, result: ExecutorResult) -> float | None:
        last_line = result.stdout.rstrip().rpartition('\n')[2]
        if not last_line.startswith('elapsed='):
            return None
        return float(last_line.removeprefix('elapsed='))


# --- detection ------------------------------------------------------------


def test_a_handler_without_the_override_has_no_hook():
    assert task_run_seconds_hook(BaseDrakkarHandler()) is None
    assert BaseDrakkarHandler().task_run_seconds(make_result()) is None


def test_an_override_is_found_as_a_bound_method():
    handler = LastLineHandler()
    hook = task_run_seconds_hook(handler)
    assert hook is not None
    assert hook(make_result(stdout='work\nelapsed=0.25\n')) == 0.25


def test_an_async_override_is_refused_at_startup():
    class AsyncHandler(BaseDrakkarHandler):
        async def task_run_seconds(self, result):  # type: ignore[override]
            return 1.0

    with pytest.raises(ConfigurationError, match=r'AsyncHandler\.task_run_seconds must be a plain method'):
        check_task_run_seconds_hook(AsyncHandler())


def test_a_sync_override_and_the_default_pass_the_startup_check():
    check_task_run_seconds_hook(LastLineHandler())
    check_task_run_seconds_hook(BaseDrakkarHandler())


# --- reported_run_seconds -------------------------------------------------


def test_without_an_override_nothing_is_called():
    assert reported_run.reported_run_seconds(None, make_result()) is None


def test_a_result_without_a_process_is_never_passed_to_the_hook():
    hook = MagicMock(return_value=0.5)
    assert reported_run.reported_run_seconds(hook, make_result(pid=None)) is None
    hook.assert_not_called()


@pytest.mark.parametrize('value', [0, 0.5, 1, 1.05])
def test_a_valid_value_is_returned_as_a_float(value, clock):
    before = hook_errors()
    reported = reported_run.reported_run_seconds(lambda result: value, make_result(duration=1.0))
    assert reported == float(value)
    assert isinstance(reported, float)
    assert hook_errors() == before


def test_none_uses_the_framework_measure_silently(clock):
    before = hook_errors()
    with capture_logs() as logs:
        assert reported_run.reported_run_seconds(lambda result: None, make_result()) is None
    assert hook_errors() == before
    assert logs == []


def raise_value_error(result):
    raise ValueError('no elapsed line')


@pytest.mark.parametrize(
    ('hook', 'error'),
    [
        (raise_value_error, 'ValueError: no elapsed line'),
        (lambda result: math.nan, 'value out of range: nan'),
        (lambda result: math.inf, 'value out of range: inf'),
        (lambda result: -0.001, 'value out of range: -0.001'),
        (lambda result: 1.06, 'value out of range: 1.06'),
        (lambda result: True, 'value out of range: True'),
        (lambda result: '0.5', "value out of range: '0.5'"),
    ],
)
def test_a_bad_answer_falls_back_with_one_count_and_one_log_line(hook, error, clock):
    before = hook_errors()
    with capture_logs() as logs:
        assert reported_run.reported_run_seconds(hook, make_result(duration=1.0)) is None

    assert hook_errors() == before + 1
    assert logs == [
        {
            'event': 'task_run_seconds_hook_failed',
            'log_level': 'warning',
            'category': 'handler',
            'task_id': 't1',
            'error': error,
            'suppressed': 0,
        }
    ]


def test_failure_lines_are_limited_to_one_per_minute(clock):
    with capture_logs() as logs:
        for _ in range(3):
            reported_run.reported_run_seconds(raise_value_error, make_result())
        clock.now += 61
        reported_run.reported_run_seconds(raise_value_error, make_result())

    assert [log['suppressed'] for log in logs] == [0, 2]


# --- the partition path ---------------------------------------------------


class RunningHandler(LastLineHandler):
    async def arrange(self, messages: list[SourceMessage], pending: Any) -> list[ExecutorTask]:
        return [
            ExecutorTask(task_id=f'task-{msg.offset}', args=msg.value.decode().split('|'), source_offsets=[msg.offset])
            for msg in messages
        ]


def message(offset: int, args: str) -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, value=args.encode(), timestamp=1)


async def test_the_partition_asks_the_hook_for_a_failed_task():
    recorder = MagicMock()
    pool = ExecutorPool(binary_path='/bin/sh', max_executors=2, task_timeout_seconds=10)
    processor = PartitionProcessor(
        partition_id=0, handler=RunningHandler(), executor_pool=pool, window_size=10, recorder=recorder
    )

    processor.enqueue(message(0, '-c|echo elapsed=0; exit 3'))
    processor.start()
    await wait_for(lambda: recorder.record_task_failed.called)
    await processor.stop()

    call = recorder.record_task_failed.call_args
    assert call.kwargs['reported_run_seconds'] == 0.0
    assert call.kwargs['result'].run_started_at is not None


async def test_the_partition_passes_a_valid_reported_value():
    recorder = MagicMock()
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10)
    processor = PartitionProcessor(
        partition_id=0, handler=RunningHandler(), executor_pool=pool, window_size=10, recorder=recorder
    )

    processor.enqueue(message(0, 'elapsed=0'))
    processor.start()
    await wait_for(lambda: recorder.record_task_completed.called)
    await processor.stop()

    assert recorder.record_task_completed.call_args.kwargs['reported_run_seconds'] == 0.0


# --- the webapp path ------------------------------------------------------


async def test_the_webapp_runner_records_the_reported_run_time():
    task = ExecutorTask(task_id='w1', source_offsets=[1])
    ok = ExecutorResult(exit_code=0, stdout='elapsed=0.01\n', stderr='', duration_seconds=0.05, task=task, pid=7)
    failed_result = ExecutorResult(
        exit_code=2, stdout='elapsed=0.02', stderr='', duration_seconds=0.05, task=task, pid=8
    )
    pool = MagicMock()
    outcomes = iter([ok, ExecutorTaskError(error=ExecutorError(task=task, exit_code=2), result=failed_result)])

    async def execute(task, recorder, partition_id):
        outcome = next(outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    pool.execute = execute
    recorder = MagicMock()
    app = MagicMock()
    app._handler = LastLineHandler()
    # A real resolver: a MagicMock attribute would answer a mock as the request id.
    app._request_ids = RequestIdResolver(app._handler)
    runner = WebappRunner(app, MagicMock())

    for _ in range(2):
        await runner._execute_task(
            pool, task, recorder, records_outcome=True, run_seconds_hook=runner._task_run_seconds
        )

    assert recorder.record_task_completed.call_args.kwargs['reported_run_seconds'] == 0.01
    failed_call = recorder.record_task_failed.call_args
    assert failed_call.kwargs['reported_run_seconds'] == 0.02
    assert failed_call.kwargs['result'] is failed_result
