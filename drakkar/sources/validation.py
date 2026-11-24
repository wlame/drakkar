"""Fail fast when the handler cannot serve an enabled source."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any

from drakkar.config import SourcesConfig
from drakkar.models import ConfigurationError

# Hooks that only a handler with ``split`` can use, in the order the checks report them.
_SPLIT_ONLY_HOOKS = ('part_data', 'part_key', 'aggregate', 'on_job_timeout')


def validate_handler_for_sources(
    handler: Any, sources: SourcesConfig, *, split_enabled: bool = False, split_http_enabled: bool = False
) -> None:
    """Raise ``ConfigurationError`` unless the handler implements the hooks every enabled source calls.

    The checks of the streaming arrange hook and of the split hooks run first,
    whatever sources are enabled: they are faults of the handler class alone,
    except that the split checks also read ``split.enabled`` and
    ``split.http.enabled``. With both on, the HTTP source stores split jobs
    and needs none of the hooks or models of a synchronous request.
    """
    from drakkar.handler import overrides_arrange, uses_arrange_stream

    validate_stream_hooks(handler)
    validate_split_hooks(handler, split_enabled=split_enabled, split_http_enabled=split_http_enabled)
    if sources.kafka.enabled and not overrides_arrange(handler) and not uses_arrange_stream(handler):
        raise ConfigurationError(
            f'sources.kafka.enabled=true but {type(handler).__name__} does not override arrange() or '
            'arrange_stream() — implement one of them or disable sources.kafka'
        )
    if sources.http.enabled and not (split_enabled and split_http_enabled):
        from drakkar.webapp.validation import validate_webapp_handler

        validate_webapp_handler(handler)


def validate_stream_hooks(handler: Any) -> None:
    """Raise ``ConfigurationError`` when the handler mixes the stream hooks with hooks that do not fit them."""
    from drakkar.handler import overridden_completion_hooks, overrides_arrange, uses_arrange_stream

    handler_cls = type(handler)
    name = handler_cls.__name__
    is_stream = uses_arrange_stream(handler)
    hooks = overridden_completion_hooks(handler)
    group_labels = getattr(handler_cls, 'group_labels', ())

    if is_stream:
        if overrides_arrange(handler):
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


def validate_split_hooks(handler: Any, *, split_enabled: bool, split_http_enabled: bool = False) -> None:
    """Raise ``ConfigurationError`` when the split hooks of the handler do not fit each other.

    With ``split.enabled`` false nothing is checked: no split hook is ever
    called, so a handler whose methods or attributes have these names for
    its own use starts as it did before the feature existed.
    """
    from drakkar.handler import _handler_splits, _overrides_hook

    if not split_enabled:
        return
    handler_cls = type(handler)
    name = handler_cls.__name__
    if _handler_splits(handler):
        if not inspect.isasyncgenfunction(handler_cls.split):
            raise ConfigurationError(f'{name}.split must be an async generator function')
        if not split_http_enabled and _overrides_hook(handler, 'http_job_result'):
            raise ConfigurationError(
                f'{name} overrides http_job_result() without split.http.enabled; it is called only for HTTP split jobs'
            )
        return
    for hook in _SPLIT_ONLY_HOOKS:
        if _overrides_hook(handler, hook):
            raise ConfigurationError(
                f'{name} overrides {hook}() without split(); it is called only for a handler that splits'
            )
    if getattr(handler_cls, 'part_data_model', None) is not None:
        raise ConfigurationError(
            f'{name} sets part_data_model without split(); it is read only for a handler that splits'
        )
    raise ConfigurationError(
        f'split.enabled=true but {name} does not override split() — implement it or set split.enabled to false'
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
