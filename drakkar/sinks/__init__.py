"""Pluggable sink system for Drakkar framework.

Sinks are output destinations that receive processed data from the pipeline.
Each sink type (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem) implements
the BaseSink interface. The SinkManager orchestrates routing, delivery,
and error handling across all configured sinks.

Plugin authors register additional sink types via the
``[project.entry-points."drakkar.sinks"]`` stanza in their ``pyproject.toml``;
:class:`SinkRegistry` discovers those entries at framework startup. See
``docs/sinks.md`` for the full plugin contract.
"""

from drakkar.sinks.base import BaseSink
from drakkar.sinks.filesystem import FileSink
from drakkar.sinks.http import HttpSink
from drakkar.sinks.kafka import KafkaSink
from drakkar.sinks.manager import AmbiguousSinkError, SinkManager, SinkNotConfiguredError
from drakkar.sinks.mongo import MongoSink
from drakkar.sinks.postgres import PostgresSink
from drakkar.sinks.redis import RedisSink
from drakkar.sinks.registry import SinkRegistry

# Pre-register the built-in sink classes under their canonical type
# names so the registry is the single source of truth for "what sink
# types does this Drakkar process know about?". Plugin discovery via
# ``SinkRegistry.discover()`` runs later (driven by SinkManager) and
# lands custom types alongside these. Names match the
# ``DrakkarConfig.sinks.<field>`` keys consumed by
# ``DrakkarApp._build_sinks``.
SinkRegistry.register('kafka', KafkaSink)
SinkRegistry.register('postgres', PostgresSink)
SinkRegistry.register('mongo', MongoSink)
SinkRegistry.register('http', HttpSink)
SinkRegistry.register('redis', RedisSink)
SinkRegistry.register('filesystem', FileSink)

__all__ = [
    'AmbiguousSinkError',
    'BaseSink',
    'SinkManager',
    'SinkNotConfiguredError',
    'SinkRegistry',
]
