"""Fail fast when the handler cannot serve an enabled source."""

from __future__ import annotations

from typing import Any

from drakkar.config import SourcesConfig
from drakkar.models import ConfigurationError


def validate_handler_for_sources(handler: Any, sources: SourcesConfig) -> None:
    """Raise ``ConfigurationError`` unless the handler implements the hooks every enabled source calls."""
    from drakkar.handler import BaseDrakkarHandler

    if sources.kafka.enabled:
        handler_cls = type(handler)
        if getattr(handler_cls, 'arrange', BaseDrakkarHandler.arrange) is BaseDrakkarHandler.arrange:
            raise ConfigurationError(
                f'sources.kafka.enabled=true but {handler_cls.__name__} does not override arrange() — '
                'implement it or disable sources.kafka'
            )
    if sources.http.enabled:
        from drakkar.webapp.validation import validate_webapp_handler

        validate_webapp_handler(handler)
