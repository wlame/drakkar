"""``run_in_thread_to_end`` and ``run_in_daemon_thread_to_end``: a cancel of the caller waits for the thread to end."""

import asyncio
import contextvars
import threading

import pytest

from drakkar.concurrency import run_in_daemon_thread_to_end, run_in_thread_to_end

RUNNERS = pytest.mark.parametrize('run', [run_in_thread_to_end, run_in_daemon_thread_to_end])

_REQUEST = contextvars.ContextVar('request', default='')


@RUNNERS
async def test_run_to_end_answers_the_result(run):
    assert await run(lambda a, b: a + b, 2, 3) == 5


@RUNNERS
async def test_a_failing_call_raises_its_error(run):
    def work() -> None:
        raise OSError('disk full')

    with pytest.raises(OSError, match='disk full'):
        await run(work)


@RUNNERS
async def test_the_call_sees_the_context_of_its_caller(run):
    _REQUEST.set('r-1')

    assert await run(_REQUEST.get) == 'r-1'


async def test_the_daemon_runner_calls_on_a_daemon_thread():
    def thread_kind() -> tuple[bool, str]:
        current = threading.current_thread()
        return current.daemon, current.name

    assert await run_in_daemon_thread_to_end(thread_kind) == (True, 'drakkar-daemon-call')


@RUNNERS
async def test_a_cancel_waits_for_the_thread_and_hands_its_result_back(run):
    entered = threading.Event()
    gate = threading.Event()
    ended: list[str] = []
    handed: list[str] = []

    def work() -> str:
        entered.set()
        gate.wait(5)
        ended.append('done')
        return 'opened'

    task = asyncio.ensure_future(run(work, on_cancelled_result=handed.append))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0.05)
    # Cancelled, but still waiting for the thread.
    assert not task.done()
    task.cancel()  # a second cancel is waited out too
    gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert ended == ['done']
    assert handed == ['opened']


@RUNNERS
async def test_a_cancel_of_a_failing_thread_raises_the_cancel(run):
    entered = threading.Event()
    gate = threading.Event()

    def work() -> None:
        entered.set()
        gate.wait(5)
        raise OSError('disk full')

    task = asyncio.ensure_future(run(work, on_cancelled_result=lambda _r: None))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
