"""Tests for the ambient hook context."""

import asyncio

import pytest

from drakkar.hookctx import (
    bind_hook_context,
    clear_hook_context,
    current_hook_context,
)


def test_current_hook_context_outside_any_hook_returns_none():
    assert current_hook_context() is None


def test_bind_hook_context_exposes_all_anchor_fields():
    token = bind_hook_context(
        hook='arrange',
        partition=3,
        window_id=17,
        offsets=(100, 101, 102),
    )
    try:
        ctx = current_hook_context()
        assert ctx is not None
        assert ctx.hook == 'arrange'
        assert ctx.partition == 3
        assert ctx.window_id == 17
        assert ctx.offsets == (100, 101, 102)
        assert ctx.offset is None
        assert ctx.task_id is None
    finally:
        clear_hook_context(token)


def test_bind_hook_context_starts_budget_counters_at_zero():
    token = bind_hook_context(hook='arrange', partition=0)
    try:
        ctx = current_hook_context()
        assert ctx is not None
        assert ctx.drops == 0
        assert ctx.accepted_bytes == 0
    finally:
        clear_hook_context(token)


def test_clear_hook_context_restores_none_outside_hook():
    token = bind_hook_context(hook='arrange', partition=0)
    clear_hook_context(token)

    assert current_hook_context() is None


def test_nested_bind_shadows_outer_context_and_clear_restores_it():
    # Mirrors the real nesting in partition.py: a window-wide hook context
    # with a task-anchored one bound inside it.
    outer = bind_hook_context(hook='on_window_complete', partition=2, window_id=9, offsets=(50, 51))
    try:
        inner = bind_hook_context(hook='on_task_complete', partition=2, window_id=9, task_id='task-abc')
        try:
            ctx = current_hook_context()
            assert ctx is not None
            assert ctx.hook == 'on_task_complete'
            assert ctx.task_id == 'task-abc'
            # The inner context does not inherit the outer window's offsets.
            assert ctx.offsets == ()
        finally:
            clear_hook_context(inner)

        restored = current_hook_context()
        assert restored is not None
        assert restored.hook == 'on_window_complete'
        assert restored.task_id is None
        assert restored.offsets == (50, 51)
    finally:
        clear_hook_context(outer)


def test_nested_bind_does_not_inherit_outer_budget_counters():
    outer = bind_hook_context(hook='arrange', partition=1)
    try:
        outer_ctx = current_hook_context()
        assert outer_ctx is not None
        outer_ctx.drops = 4
        outer_ctx.accepted_bytes = 8192

        inner = bind_hook_context(hook='on_task_complete', partition=1, task_id='t1')
        try:
            inner_ctx = current_hook_context()
            assert inner_ctx is not None
            assert inner_ctx.drops == 0
            assert inner_ctx.accepted_bytes == 0
        finally:
            clear_hook_context(inner)

        # The outer invocation's running totals survive the nested bind.
        after = current_hook_context()
        assert after is not None
        assert after.drops == 4
        assert after.accepted_bytes == 8192
    finally:
        clear_hook_context(outer)


def test_clear_hook_context_in_finally_restores_context_after_raise():
    # partition.py lets handler exceptions propagate out of several hooks;
    # a context leaked on that path would misattribute every later record.
    token = bind_hook_context(hook='arrange', partition=5)
    with pytest.raises(ValueError, match='handler exploded'):
        try:
            raise ValueError('handler exploded')
        finally:
            clear_hook_context(token)

    assert current_hook_context() is None


async def test_bind_hook_context_does_not_leak_between_concurrent_tasks():
    # Every partition processor runs as its own asyncio task, so contexts
    # bound in one must be invisible to the others.
    observed: dict[str, int | None] = {}
    started = asyncio.Event()

    async def worker(name: str, partition: int) -> None:
        token = bind_hook_context(hook='arrange', partition=partition)
        try:
            if name == 'a':
                started.set()
            else:
                await started.wait()
            await asyncio.sleep(0)
            ctx = current_hook_context()
            observed[name] = ctx.partition if ctx else None
        finally:
            clear_hook_context(token)

    await asyncio.gather(worker('a', 11), worker('b', 22))

    assert observed == {'a': 11, 'b': 22}
    assert current_hook_context() is None
