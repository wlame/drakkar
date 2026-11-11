"""Runtime side of the message identity: the handler hook, and binding the log field.

The pure rules (source parsing, extraction, the field of a scope) are in
:mod:`drakkar.message_identity`; this module calls the handler and touches
structlog's context.
"""

import contextvars
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import structlog
from structlog.contextvars import bind_contextvars, reset_contextvars

from drakkar.message_identity import (
    IdentitySource,
    extract_identity,
    identity_field,
    identity_label,
)
from drakkar.models import SourceMessage
from drakkar.providers.resilience import RateLimitedLog

if TYPE_CHECKING:
    from drakkar.config import MessageIdentityConfig
    from drakkar.handler import BaseDrakkarHandler

logger = structlog.get_logger()

type BindTokens = Mapping[str, contextvars.Token[Any]]

# structlog's merge_contextvars leaves out a context variable whose value is
# Ellipsis (that is how unbind_contextvars works), so binding Ellipsis hides
# an outer scope's value without a separate unbind path.
_ABSENT = ...


class MessageIdentityResolver:
    """Reads the identity of each message and binds it into the log context of a scope."""

    def __init__(self, config: 'MessageIdentityConfig', handler: 'BaseDrakkarHandler') -> None:
        self.name = config.name
        self.max_values = config.max_values
        self._source: IdentitySource = config.identity_source()
        self._handler = handler
        # A handler that keeps the default hook costs no call per message.
        self._has_hook = type(handler).message_identity is not _default_hook()
        self._hook_failures = RateLimitedLog(time.monotonic)

    def of_message(self, msg: SourceMessage) -> str | None:
        """The identity of a Kafka message."""
        configured = extract_identity(self._source, key=msg.key, value=msg.value, headers=msg.headers)
        return self._ask_hook(msg, configured)

    def of_http_request(self, msg: SourceMessage, headers: Mapping[str, str], raw_body: bytes) -> str | None:
        """The identity of an HTTP request: its headers and raw body; a request has no key."""
        header_list = [(name, value.encode('utf-8')) for name, value in headers.items()]
        configured = extract_identity(self._source, key=None, value=raw_body, headers=header_list)
        return self._ask_hook(msg, configured)

    def _ask_hook(self, msg: SourceMessage, configured: str | None) -> str | None:
        """Let the handler's ``message_identity`` change the configured value."""
        if not self._has_hook:
            return configured
        try:
            answer = self._handler.message_identity(msg, configured)
        except Exception as exc:
            self._log_hook_failure(error=f'{type(exc).__name__}: {exc}')
            return None
        if answer is None or isinstance(answer, str):
            return answer or None
        self._log_hook_failure(error=f'message_identity returned {type(answer).__name__}, want str or None')
        return None

    def _log_hook_failure(self, error: str) -> None:
        suppressed = self._hook_failures.admit('hook')
        if suppressed is not None:
            logger.warning(
                'message_identity_hook_failed',
                category='handler',
                error=error,
                suppressed=suppressed,
            )

    def label(self, values: Iterable[str | None]) -> str | None:
        """The task label of these values."""
        return identity_label(values, self.max_values)

    def bind(self, values: Iterable[str | None]) -> BindTokens:
        """Bind the field of these values into the log context; return the tokens to reset it.

        A scope with no value hides the field of an outer scope, so a task
        line never carries the list of the window it came from.
        """
        field = identity_field(values, self.max_values)
        return bind_contextvars(**{self.name: _ABSENT if field is None else field})

    @contextmanager
    def scope(self, values: Iterable[str | None]) -> Iterator[None]:
        """Bind the field of these values for the ``with`` block, then put the outer value back."""
        tokens = self.bind(values)
        try:
            yield
        finally:
            reset_identity(tokens)


def reset_identity(tokens: BindTokens) -> None:
    """Put back the log context that :meth:`MessageIdentityResolver.bind` replaced."""
    reset_contextvars(**tokens)


def _default_hook() -> Any:
    """The handler base class's ``message_identity``, imported late (handler imports models)."""
    from drakkar.handler import BaseDrakkarHandler

    return BaseDrakkarHandler.message_identity
