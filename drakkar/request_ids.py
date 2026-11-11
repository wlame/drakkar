"""The request id of each message: asked of the handler, checked, and bound into the log context.

Every Kafka message and every HTTP request gets a ``request_id``: the answer
of the handler hook ``request_id()``, by default a generated id. Every log
line written while the message is processed carries it; a line that covers
several messages (an arrange window, a task built from several messages)
carries the list of their ids. Tasks carry it too, as ``task.request_id`` and
as the label ``request_id``.
"""

import contextvars
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import structlog
from structlog.contextvars import bind_contextvars, reset_contextvars

from drakkar.models import ExecutorTask, SourceMessage
from drakkar.providers.resilience import RateLimitedLog
from drakkar.utils import make_request_id, validate_request_id

if TYPE_CHECKING:
    from drakkar.handler import BaseDrakkarHandler

logger = structlog.get_logger()

REQUEST_ID_FIELD = 'request_id'
# The most ids a line that covers several messages carries.
MAX_FIELD_VALUES = 100
# The text that joins the ids of a task built from several messages.
JOIN_SEPARATOR = ','

type BindTokens = Mapping[str, contextvars.Token[Any]]

# structlog's merge_contextvars leaves out a context variable whose value is
# Ellipsis (that is how unbind_contextvars works), so binding Ellipsis hides
# an outer scope's value without a separate unbind path.
_ABSENT = ...


def request_id_field(values: Iterable[str | None]) -> str | list[str] | None:
    """The log field of a scope: one id as text, several as a list, none as None.

    Missing ids are left out and repeated ones kept once, in first-seen
    order; a list holds at most ``MAX_FIELD_VALUES`` ids.
    """
    seen: dict[str, None] = {}
    for value in values:
        if value is not None and value not in seen:
            seen[value] = None
            if len(seen) == MAX_FIELD_VALUES:
                break
    if not seen:
        return None
    unique = list(seen)
    return unique[0] if len(unique) == 1 else unique


def joined_request_ids(values: Iterable[str | None]) -> str | None:
    """The ids of a task's source messages as one text, joined with ','; None when there is none."""
    field = request_id_field(values)
    if field is None or isinstance(field, str):
        return field
    return JOIN_SEPARATOR.join(field)


class RequestIdResolver:
    """Asks the handler for each message's request id and binds ids into the log context."""

    def __init__(self, handler: 'BaseDrakkarHandler') -> None:
        self._handler = handler
        # A handler that keeps the default hook gets a generated id, which
        # needs no check.
        # getattr: a handler-like object (a test double) may not define it on its class.
        self._has_hook = getattr(type(handler), 'request_id', None) is not _default_hook()
        self._hook_failures = RateLimitedLog(time.monotonic)

    def resolve(self, msg: SourceMessage) -> str:
        """The request id of ``msg``, from the handler hook; a generated id when the hook fails."""
        if not self._has_hook:
            return make_request_id('req')
        try:
            answer = self._handler.request_id(msg)
            if not isinstance(answer, str):
                raise TypeError(f'request_id returned {type(answer).__name__}, want str')
            validate_request_id(answer)
        except Exception as exc:
            self._log_hook_failure(error=f'{type(exc).__name__}: {exc}')
            return make_request_id('req')
        return answer

    def assign(self, msg: SourceMessage) -> str:
        """Resolve the request id of ``msg`` and store it on the message."""
        msg.request_id = self.resolve(msg)
        return msg.request_id

    def _log_hook_failure(self, error: str) -> None:
        suppressed = self._hook_failures.admit('hook')
        if suppressed is not None:
            logger.warning(
                'request_id_hook_failed',
                category='handler',
                error=error,
                suppressed=suppressed,
            )

    @staticmethod
    def stamp_task(task: ExecutorTask, values: Iterable[str | None]) -> None:
        """Give ``task`` the ids of its source messages: ``task.request_id`` and the label.

        A request id or label that the handler already set is kept.
        """
        joined = joined_request_ids(values)
        if joined is None:
            return
        if task.request_id is None:
            task.request_id = joined
        task.labels.setdefault(REQUEST_ID_FIELD, joined)

    @staticmethod
    def bind(values: Iterable[str | None]) -> BindTokens:
        """Bind the field of these ids into the log context; return the tokens to reset it.

        A scope with no id hides the field of an outer scope, so a task line
        never carries the list of the window it came from.
        """
        field = request_id_field(values)
        return bind_contextvars(**{REQUEST_ID_FIELD: _ABSENT if field is None else field})

    @contextmanager
    def scope(self, values: Iterable[str | None]) -> Iterator[None]:
        """Bind the field of these ids for the ``with`` block, then put the outer value back."""
        tokens = self.bind(values)
        try:
            yield
        finally:
            reset_request_id(tokens)


def reset_request_id(tokens: BindTokens) -> None:
    """Put back the log context that :meth:`RequestIdResolver.bind` replaced."""
    reset_contextvars(**tokens)


def _default_hook() -> Any:
    """The handler base class's ``request_id``, imported late (handler imports models)."""
    from drakkar.handler import BaseDrakkarHandler

    return BaseDrakkarHandler.request_id
