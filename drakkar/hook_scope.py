"""Mark the handler calls that run as part of a Kafka partition's work, and the Message Probe runs.

A detach waits for the revoke drain, and the drain waits for the in-flight
work of every partition. A ``detach()`` awaited inside that work would wait
for itself, so the handler front refuses it. A plain flag in a context
variable cannot tell the cases apart: a task that a hook starts inherits the
hook's context, both when the hook awaits it (``asyncio.gather``,
``asyncio.wait_for``), which must be refused, and when the task outlives the
hook, which is allowed. So the context variable holds a scope object, and the
framework marks that object as ended when the hook returns. A task that still
runs after that sees an ended scope.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from types import TracebackType


class PipelineHookScope:
    """One handler call that is part of a partition's work; ``ended`` is set when the call returns.

    ``parent`` is the scope that was current when this one began, so a scope
    inside another one counts as open while the outer one is open.
    """

    __slots__ = ('_token', 'ended', 'parent')

    def __init__(self) -> None:
        self.parent: PipelineHookScope | None = None
        self.ended = False
        self._token: Token[PipelineHookScope | None] | None = None

    def __enter__(self) -> PipelineHookScope:
        self.parent = _current_scope.get()
        self._token = _current_scope.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.end()

    def end(self) -> None:
        """Mark the scope as ended and put back the scope that was current before it."""
        self.ended = True
        if self._token is not None:
            _current_scope.reset(self._token)
            self._token = None

    @property
    def is_open(self) -> bool:
        """Whether this scope, or a scope it is inside, has not ended."""
        scope: PipelineHookScope | None = self
        while scope is not None:
            if not scope.ended:
                return True
            scope = scope.parent
        return False


_current_scope: ContextVar[PipelineHookScope | None] = ContextVar('drakkar_pipeline_hook', default=None)


def pipeline_hook() -> PipelineHookScope:
    """A context manager that marks the handler call inside it as part of a partition's work.

    Use it as ``with pipeline_hook(): await handler.arrange(...)``. The scope
    adds no ``await``: entering and leaving it set a context variable.
    """
    return PipelineHookScope()


def inside_pipeline_hook() -> bool:
    """Whether the current code runs inside a handler call that is part of a partition's work."""
    scope = _current_scope.get()
    return scope is not None and scope.is_open


# True while a Message Probe run calls handler code. A probe must not change
# the worker, so the handler's detach, attach and request_stop have no effect
# there. Tasks and threads that the probe starts copy the context, so they see
# the flag too. The pipeline never sets it.
_probe_run: ContextVar[bool] = ContextVar('drakkar_message_probe_run', default=False)


@contextmanager
def message_probe_run() -> Iterator[None]:
    """Mark the code inside as part of a Message Probe run."""
    token = _probe_run.set(True)
    try:
        yield
    finally:
        _probe_run.reset(token)


def inside_message_probe() -> bool:
    """Whether the current code runs as part of a Message Probe run."""
    return _probe_run.get()
