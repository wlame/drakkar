"""Which sources exist, keyed by their ``sources.*`` config section, in start order."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from drakkar.handler import _handler_splits
from drakkar.sources.base import Source
from drakkar.sources.http import HttpSource
from drakkar.sources.kafka import KafkaSource
from drakkar.sources.parts import PartsSource

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp


# A factory passes only what a source needs to exist. Config is not part of
# that: sources run after ``on_startup`` may have replaced it, so they read
# it from the context the lifecycle binds.
def _kafka(app: DrakkarApp) -> Source:
    return KafkaSource(consume_pause=app.consume_pause)


def _http(app: DrakkarApp) -> Source:
    return HttpSource(app)


# Insertion order is start order. Adding a source is one row here plus a
# config block under ``sources:``.
SOURCE_FACTORIES: dict[str, Callable[[DrakkarApp], Source]] = {'kafka': _kafka, 'http': _http}

KAFKA_SOURCE_NAME = 'kafka'


def reads_parts(app: DrakkarApp) -> bool:
    """Whether the worker reads the parts topic: ``split.enabled`` is true and the handler splits."""
    return app._config.split.enabled and _handler_splits(app._handler)


def build_sources(app: DrakkarApp) -> list[Source]:
    """Instantiate the enabled sources in table order.

    The parts source has no config block of its own: it follows the Kafka
    source when the worker splits messages, and starts right after it.
    """
    sources: list[Source] = []
    for name in app._config.sources.enabled_names:
        sources.append(SOURCE_FACTORIES[name](app))
        if name == KAFKA_SOURCE_NAME and reads_parts(app):
            sources.append(PartsSource(consume_pause=app.consume_pause))
    return sources
