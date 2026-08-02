"""Ambient per-invocation context for framework-invoked user hooks.

Every user hook the framework calls — ``arrange``, ``on_task_complete``,
``on_error``, ``on_message_complete``, ``on_window_complete``, and the two
webapp hooks — runs with a :class:`HookContext` bound to a
:class:`~contextvars.ContextVar`. The context names the pipeline entity the
hook is working on (partition, window, message offset, task id) so
handler-facing APIs can resolve those coordinates without the user passing
them.

Why a dedicated ContextVar rather than structlog's
--------------------------------------------------
``drakkar/partition.py`` already calls ``structlog.contextvars.bind_contextvars``
around each of these hooks with almost the same keys, and reading that store
would need no new plumbing at all. We deliberately do not:

1. **It is user-perturbable.** A handler that calls ``clear_contextvars()``
   or rebinds ``partition`` for its own logging would silently corrupt the
   anchors of everything built on top. Logging context is the user's to
   shape; functional wiring is not.
2. **It couples observability data to logging configuration.** Anything
   reading structlog's store inherits that module's processor setup and
   its behaviour under reconfiguration.

The two mechanisms stay side by side, bound in the same ``try``/``finally``
blocks, and are expected to carry the same values.

Scope and lifetime
------------------
One context per **hook invocation**, not per window. A window that fans out
to 500 tasks produces one ``arrange`` context and 500 ``on_task_complete``
contexts, each with its own budget counters, each discarded when the hook
returns. That is what makes the counters self-cleaning: there is no
window-end callback to reset them because nothing outlives the call.

Binding is always explicit — every field is passed at every call site rather
than inherited from an enclosing context. The values are all in scope at
each site, and explicitness rules out a whole class of bug where a stale
``task_id`` or ``offset`` leaks from one hook into the next.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass

__all__ = [
    'HookContext',
    'bind_hook_context',
    'clear_hook_context',
    'current_hook_context',
]


@dataclass(slots=True)
class HookContext:
    """Pipeline coordinates plus resource budgets for one hook invocation.

    The anchor fields describe *what* the hook is working on and are never
    mutated after binding. ``drops`` and ``accepted_bytes`` are running
    totals that consumers update in place; they are the only mutable state,
    which is why this dataclass is not frozen.

    Attributes:
        partition: Kafka partition the hook is running for. The webapp path
            binds its own synthetic partition id.
        hook: Name of the hook being invoked, e.g. ``'arrange'``. Matches
            the ``hook`` key structlog binds at the same site.
        window_id: Per-partition monotonic window counter, or ``None`` for
            hooks that run outside a window. Unique only within one
            (partition, worker run) — never treat it as a global id.
        offsets: Source-message offsets covered by this invocation. Set for
            window-wide hooks; empty for hooks anchored to a single message
            or task.
        offset: Source-message offset when the hook is anchored to exactly
            one message, else ``None``.
        task_id: Task id when the hook is anchored to exactly one task,
            else ``None``.
        drops: Number of diagnostic records rejected during this invocation.
            Counts rejections only — never accepted records — so a rejected
            record can never influence whether the next one is admitted.
            Read on the logging path to suppress repeated warnings.
        accepted_bytes: Total encoded size of records accepted during this
            invocation, checked against the per-invocation byte budget.
    """

    partition: int
    hook: str
    window_id: int | None = None
    offsets: tuple[int, ...] = ()
    offset: int | None = None
    task_id: str | None = None
    drops: int = 0
    accepted_bytes: int = 0


# ``default=None`` means "no hook is running" — the state seen on the main
# loop, in periodic tasks, and anywhere outside a framework-invoked hook.
# Consumers must treat None as a hard signal rather than substituting
# defaults, since a wrong anchor is worse than a missing one.
_hook_context: contextvars.ContextVar[HookContext | None] = contextvars.ContextVar(
    'drakkar_hook_context',
    default=None,
)


def bind_hook_context(
    *,
    hook: str,
    partition: int,
    window_id: int | None = None,
    offsets: tuple[int, ...] = (),
    offset: int | None = None,
    task_id: str | None = None,
) -> contextvars.Token[HookContext | None]:
    """Bind a fresh context for one hook invocation and return its reset token.

    Always pair with :func:`clear_hook_context` in a ``finally`` block so
    the context is restored even when the hook raises — several call sites
    in ``partition.py`` let handler exceptions propagate to ``on_error``,
    and a leaked context would misattribute every later record.

    Budget counters always start at zero: each invocation gets its own
    allowance, and nothing is inherited from an enclosing context.
    """
    return _hook_context.set(
        HookContext(
            partition=partition,
            hook=hook,
            window_id=window_id,
            offsets=offsets,
            offset=offset,
            task_id=task_id,
        )
    )


def clear_hook_context(token: contextvars.Token[HookContext | None]) -> None:
    """Restore the context that was bound before ``token`` was issued."""
    _hook_context.reset(token)


def current_hook_context() -> HookContext | None:
    """Return the context for the running hook, or ``None`` outside one."""
    return _hook_context.get()
