"""Tests for the shared task-outcome policy.

`drakkar/taskflow.py` holds the rules that the production pipeline
(`PartitionProcessor`) and the debug probe (`DebugRunner`) must agree on:
what an ``on_error`` return means, when the retry budget is spent, how a
replacement is linked to its parent, and what a raised ``on_task_complete``
turns into. The probe's whole purpose is to answer "what would production
do", so a divergence here is a lie to the operator — these tests pin the
rules in one place.
"""

from __future__ import annotations

import pytest

from drakkar.hookctx import current_hook_context
from drakkar.models import ErrorAction, ExecutorError, ExecutorTask
from drakkar.taskflow import (
    TaskOutcome,
    call_on_error,
    decide_after_on_error,
    link_replacements,
    synthesize_internal_failure,
)


def make_task(task_id='t1', **kwargs) -> ExecutorTask:
    kwargs.setdefault('source_offsets', [1])
    return ExecutorTask(task_id=task_id, **kwargs)


class TestDecideAfterOnError:
    def test_a_list_return_replaces_the_task(self):
        replacements = [make_task('r1'), make_task('r2')]

        decision = decide_after_on_error(replacements, retry_count=0, max_retries=3)

        assert decision.outcome is TaskOutcome.REPLACE
        assert list(decision.replacements) == replacements
        assert decision.retries_exhausted is False

    def test_an_empty_list_still_replaces_rather_than_failing(self):
        """A handler that returns [] said "drop this task", not "fail it"."""
        decision = decide_after_on_error([], retry_count=0, max_retries=3)

        assert decision.outcome is TaskOutcome.REPLACE
        assert decision.replacements == ()

    @pytest.mark.parametrize('action', [ErrorAction.RETRY, 'retry'])
    def test_retry_within_budget_retries(self, action):
        """A raw string is as valid as the enum member — ErrorAction is a StrEnum."""
        decision = decide_after_on_error(action, retry_count=0, max_retries=3)

        assert decision.outcome is TaskOutcome.RETRY

    def test_last_retry_of_the_budget_is_still_allowed(self):
        decision = decide_after_on_error(ErrorAction.RETRY, retry_count=2, max_retries=3)

        assert decision.outcome is TaskOutcome.RETRY

    def test_retry_past_the_budget_fails_and_says_so(self):
        decision = decide_after_on_error(ErrorAction.RETRY, retry_count=3, max_retries=3)

        assert decision.outcome is TaskOutcome.FAIL
        assert decision.retries_exhausted is True, 'the caller logs max_retries_exceeded off this flag'

    def test_zero_budget_never_retries(self):
        decision = decide_after_on_error(ErrorAction.RETRY, retry_count=0, max_retries=0)

        assert decision.outcome is TaskOutcome.FAIL
        assert decision.retries_exhausted is True

    @pytest.mark.parametrize('action', [ErrorAction.SKIP, 'skip', None, 'nonsense', 42])
    def test_anything_that_is_not_retry_or_a_list_is_a_terminal_failure(self, action):
        decision = decide_after_on_error(action, retry_count=0, max_retries=3)

        assert decision.outcome is TaskOutcome.FAIL
        assert decision.retries_exhausted is False, 'only a spent RETRY budget is "exhausted"'


class TestLinkReplacements:
    def test_an_unset_parent_is_linked_to_the_failing_task(self):
        parent = make_task('parent')
        child = make_task('child')

        link_replacements(parent, [child])

        assert child.parent_task_id == 'parent'

    def test_an_explicit_parent_is_left_alone(self):
        parent = make_task('parent')
        child = make_task('child', parent_task_id='someone-else')

        link_replacements(parent, [child])

        assert child.parent_task_id == 'someone-else', 'the handler set this on purpose'

    def test_returns_the_same_tasks_for_chaining(self):
        parent = make_task('parent')
        children = [make_task('a'), make_task('b')]

        assert link_replacements(parent, children) == children


class TestSynthesizeInternalFailure:
    def test_builds_the_result_and_error_pair_a_raised_hook_stands_in_for(self):
        task = make_task()
        exc = ValueError('handler blew up')

        result, error = synthesize_internal_failure(task, exc)

        assert result.exit_code == -1
        assert result.stderr == 'handler blew up'
        assert result.duration_seconds == 0
        assert result.task is task
        assert error.kind == 'internal'
        assert error.exception == 'handler blew up'
        assert error.task is task


class TestCallOnError:
    async def test_returns_the_handler_action(self):
        task = make_task()
        error = ExecutorError(task=task, exception='boom')

        class Handler:
            async def on_error(self, task, error):
                return ErrorAction.RETRY

        call = await call_on_error(Handler(), task, error, partition=0, window_id=7)

        assert call.action == ErrorAction.RETRY
        assert call.exception is None
        assert call.failed is False
        assert call.duration_seconds >= 0

    async def test_captures_a_raising_hook_instead_of_propagating(self):
        """A broken on_error must never escape — it sits inside an ``except``
        clause, and an escape would leave the offset pending forever."""
        task = make_task()
        error = ExecutorError(task=task, exception='boom')

        class Handler:
            async def on_error(self, task, error):
                raise RuntimeError('hook is broken')

        call = await call_on_error(Handler(), task, error, partition=0, window_id=7)

        assert call.failed is True
        assert isinstance(call.exception, RuntimeError)
        assert call.action is None

    async def test_binds_the_hook_context_for_the_call(self):
        task = make_task('bound-task')
        error = ExecutorError(task=task, exception='boom')
        seen = {}

        class Handler:
            async def on_error(self, task, error):
                ctx = current_hook_context()
                seen['hook'] = ctx.hook
                seen['partition'] = ctx.partition
                seen['task_id'] = ctx.task_id
                return ErrorAction.SKIP

        await call_on_error(Handler(), task, error, partition=3, window_id=9)

        assert seen == {'hook': 'on_error', 'partition': 3, 'task_id': 'bound-task'}

    async def test_context_is_released_even_when_the_hook_raises(self):
        task = make_task()
        error = ExecutorError(task=task, exception='boom')

        class Handler:
            async def on_error(self, task, error):
                raise RuntimeError('boom')

        await call_on_error(Handler(), task, error, partition=0, window_id=1)

        assert current_hook_context() is None, 'a leaked context misattributes every later record'
