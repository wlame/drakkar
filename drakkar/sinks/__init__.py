"""Pluggable sink system for Drakkar framework.

Sinks are output destinations that receive processed data from the pipeline.
Each sink type (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem) implements
the BaseSink interface. The SinkManager orchestrates routing, delivery,
and error handling across all configured sinks.
"""

from drakkar.sinks.base import BaseSink
from drakkar.sinks.manager import AmbiguousSinkError, SinkManager, SinkNotConfiguredError

__all__ = [
    'AmbiguousSinkError',
    'BaseSink',
    'SinkManager',
    'SinkNotConfiguredError',
]
