"""Drakkar webapp pipeline — synchronous-HTTP entry point over the handler pipeline.

Re-exports the public-facing webapp types from the submodules so test code
and downstream wrappers can import them from one place. ``WebApp`` itself
is reachable here for advanced integration but intentionally absent from
``drakkar.__all__`` — it's a framework-internal lifecycle owner.
"""

from drakkar.webapp.models import (
    CacheStats,
    SinkDeliverySummary,
    SinkResult,
    StageTiming,
    TaskReport,
    TaskSummary,
    WebReport,
    WebRequestContext,
)
from drakkar.webapp.server import ConfigurationError, WebApp

__all__ = [
    'CacheStats',
    'ConfigurationError',
    'SinkDeliverySummary',
    'SinkResult',
    'StageTiming',
    'TaskReport',
    'TaskSummary',
    'WebApp',
    'WebReport',
    'WebRequestContext',
]
