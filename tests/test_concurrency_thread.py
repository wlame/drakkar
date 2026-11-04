"""``run_in_thread_to_end``: a cancel of the caller waits for the thread to end."""

import asyncio
import threading

import pytest

from drakkar.concurrency import run_in_thread_to_end


async def test_run_in_thread_to_end_answers_the_result():
    assert await run_in_thread_to_end(lambda a, b: a + b, 2, 3) == 5


async def test_a_cancel_waits_for_the_thread_and_hands_its_result_back():
    entered = threading.Event()
    gate = threading.Event()
    ended: list[str] = []
    handed: list[str] = []

    def work() -> str:
        entered.set()
        gate.wait(5)
        ended.append('done')
        return 'opened'

    task = asyncio.ensure_future(run_in_thread_to_end(work, on_cancelled_result=handed.append))
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


async def test_a_cancel_of_a_failing_thread_raises_the_cancel():
    entered = threading.Event()
    gate = threading.Event()

    def work() -> None:
        entered.set()
        gate.wait(5)
        raise OSError('disk full')

    task = asyncio.ensure_future(run_in_thread_to_end(work, on_cancelled_result=lambda _r: None))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
