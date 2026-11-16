"""Fail fast when the handler cannot serve an enabled source."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from drakkar.config import SourcesConfig
from drakkar.models import ConfigurationError


def validate_handler_for_sources(handler: Any, sources: SourcesConfig) -> None:
    """Raise ``ConfigurationError`` unless the handler implements the hooks every enabled source calls.

    The checks of the streaming arrange hook run first, whatever sources are
    enabled: they are faults of the handler class alone.
    """
    from drakkar.handler import BaseDrakkarHandler, uses_arrange_stream

    validate_stream_hooks(handler)
    if sources.kafka.enabled:
        handler_cls = type(handler)
        overrides_arrange = (
            getattr(handler_cls, 'arrange', BaseDrakkarHandler.arrange) is not BaseDrakkarHandler.arrange
        )
        if not overrides_arrange and not uses_arrange_stream(handler):
            raise ConfigurationError(
                f'sources.kafka.enabled=true but {handler_cls.__name__} does not override arrange() or '
                'arrange_stream() — implement one of them or disable sources.kafka'
            )
    if sources.http.enabled:
        from drakkar.webapp.validation import validate_webapp_handler

        validate_webapp_handler(handler)


def validate_stream_hooks(handler: Any) -> None:
    """Raise ``ConfigurationError`` when the handler mixes the stream hooks with hooks that do not fit them."""
    from drakkar.handler import BaseDrakkarHandler, overridden_completion_hooks, uses_arrange_stream

    handler_cls = type(handler)
    name = handler_cls.__name__
    is_stream = uses_arrange_stream(handler)
    hooks = overridden_completion_hooks(handler)
    group_labels = getattr(handler_cls, 'group_labels', ())

    if is_stream:
        if getattr(handler_cls, 'arrange', BaseDrakkarHandler.arrange) is not BaseDrakkarHandler.arrange:
            raise ConfigurationError(f'{name} overrides both arrange() and arrange_stream(); a handler has one of them')
        if hooks['window_complete']:
            raise ConfigurationError(
                f'{name} overrides arrange_stream() and on_window_complete(); a stream handler has no windows, '
                'use on_group_complete() or on_message_complete()'
            )
        if not _is_valid_label_names(group_labels):
            raise ConfigurationError(f'{name}.group_labels must be a sequence of unique, non-empty label names')
        return
    if hooks['group_complete']:
        raise ConfigurationError(
            f'{name} overrides on_group_complete() without arrange_stream(); groups exist only for a stream handler'
        )
    if _is_set(group_labels):
        raise ConfigurationError(
            f'{name} sets group_labels without arrange_stream(); groups exist only for a stream handler'
        )


def _is_set(group_labels: object) -> bool:
    """Whether ``group_labels`` holds anything other than an empty tuple or list."""
    return not (isinstance(group_labels, tuple | list) and len(group_labels) == 0)


def _is_valid_label_names(group_labels: object) -> bool:
    """Whether ``group_labels`` is a sequence (not a text) of unique, non-empty strings."""
    if isinstance(group_labels, str | bytes | bytearray) or not isinstance(group_labels, Sequence):
        return False
    names = list(group_labels)
    if not all(isinstance(label, str) and label for label in names):
        return False
    return len(set(names)) == len(names)
