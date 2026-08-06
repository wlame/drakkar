"""DebugRunner integration for user-defined probe details."""

import asyncio

import pytest
from pydantic import BaseModel

from drakkar import probe
from drakkar.probe import probe_field
from drakkar.uiserver.runner import DebugRunner
from drakkar.uiserver.runner_models import ProbeInput
from tests.test_debug_runner import _HappyPathHandler, _make_config, _make_executor_pool


class TaskRow(BaseModel):
    task_id: str
    verdict: str


class RunDetails(BaseModel):
    chosen_strategy: str | None = probe_field(section='Arrange', view='string', default=None)
    stage_counts: dict[str, int] = probe_field(section='Arrange', view='keyvalue', default_factory=dict)
    task_rows: list[TaskRow] = probe_field(section='Tasks', view='table', default_factory=list)


class _DetailsHandler(_HappyPathHandler):
    """Happy-path handler that fills probe details from three different stages."""

    probe_details_model = RunDetails

    async def arrange(self, messages, pending):
        probe.set(chosen_strategy='single-window')
        probe.update('stage_counts', arrange=1)
        return await super().arrange(messages, pending)

    async def on_task_complete(self, result):
        probe.append('task_rows', {'task_id': result.task.task_id, 'verdict': 'ok'})
        return await super().on_task_complete(result)


def _probe_input() -> ProbeInput:
    return ProbeInput(value='{"hello": "world"}', key='k', partition=0, offset=1, topic='in')


async def test_report_carries_user_details_with_data_and_layout():
    handler = _DetailsHandler(task_count=2)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    report = await runner.run(_probe_input())

    details = report.user_details
    assert details is not None
    assert details.model == 'RunDetails'
    assert details.data['chosen_strategy'] == 'single-window'
    assert details.data['stage_counts'] == {'arrange': 1}
    assert [r['task_id'] for r in details.data['task_rows']] == ['t-0', 't-1']
    assert [s.title for s in details.layout.sections] == ['Arrange', 'Tasks']


async def test_writes_are_stage_attributed():
    handler = _DetailsHandler(task_count=1)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    report = await runner.run(_probe_input())

    stages = [w.origin_stage for w in report.user_details.writes]
    assert stages[0] == 'arrange'
    assert stages[1] == 'arrange'
    assert stages[2].startswith('task_complete:')


async def test_no_registered_model_yields_null_user_details():
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    report = await runner.run(_probe_input())

    assert report.user_details is None


async def test_invalid_write_lands_in_probe_errors_and_probe_completes():
    class _BadWriteHandler(_DetailsHandler):
        async def arrange(self, messages, pending):
            probe.set(no_such_field='x')
            return await super().arrange(messages, pending)

    handler = _BadWriteHandler(task_count=1)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    report = await runner.run(_probe_input())

    assert report.truncated is False
    assert any(e.exception_class == 'ProbeDetailsError' and 'no_such_field' in e.message for e in report.errors)


async def test_contextvar_is_cleared_after_normal_run_with_hook_error():
    """A hook exception is caught per-stage (see runner.py's per-hook try/except), so this
    only proves the contextvar resets on the ordinary completion path — NOT on an exception
    reaching ``_run_locked``'s ``finally``. See
    ``test_contextvar_is_cleared_when_probe_is_cancelled_mid_run`` for that path; the only
    exception able to reach the ``finally`` unswallowed is ``asyncio.CancelledError``.
    """

    class _RaisingHandler(_DetailsHandler):
        async def arrange(self, messages, pending):
            probe.set(chosen_strategy='x')
            raise RuntimeError('arrange exploded')

    handler = _RaisingHandler(task_count=1)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    await runner.run(_probe_input())

    assert probe._active_state.get() is None
    probe.set(chosen_strategy='leak-check')  # must be a silent no-op now


async def test_contextvar_is_cleared_when_probe_is_cancelled_mid_run():
    """Genuine reset-on-exception coverage: cancel the run while it is inside ``arrange``,
    after a write has bound the details state, and confirm the ``finally`` in ``_run_locked``
    still resets ``_active_state`` even though ``CancelledError`` unwinds through every
    per-stage ``except Exception`` handler untouched (it is a ``BaseException``, not an
    ``Exception``).

    The observation must happen INSIDE the cancelled task, not from this test coroutine:
    ``asyncio.create_task`` snapshots the current context, so ``_active_state.set()``/
    ``.reset()`` inside that task mutate only the task's own copy. Checking
    ``probe._active_state.get()`` from the test's own context after ``await task`` would
    read a context that was never touched either way and would pass vacuously — the same
    flaw this test replaces.
    """
    entered = asyncio.Event()
    observed_after_cancel: list[object] = []

    class _CancelHandler(_DetailsHandler):
        async def arrange(self, messages, pending):
            probe.set(chosen_strategy='x')
            entered.set()
            await asyncio.Event().wait()  # never set — blocks until the task is cancelled

    handler = _CancelHandler(task_count=1)
    runner = DebugRunner(handler=handler, executor_pool=_make_executor_pool(), app_config=_make_config())

    async def _run_and_observe() -> None:
        try:
            await runner.run(_probe_input())
        finally:
            # Runs in this task's own context, after _run_locked's finally
            # (deeper in the same unwind) has already reset the contextvar.
            observed_after_cancel.append(probe._active_state.get())
            probe.set(chosen_strategy='leak-check')  # must be a silent no-op post-reset
            observed_after_cancel.append(probe._active_state.get())

    task = asyncio.create_task(_run_and_observe())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert observed_after_cancel == [None, None]
