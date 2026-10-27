"""Sink system for Drakkar framework.

Sinks are output destinations that receive processed data from the pipeline.
Each sink type (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem) implements
the BaseSink interface. The SinkManager orchestrates routing, delivery,
and error handling across all configured sinks.

**This module imports nothing at module scope, deliberately.** The pure
template helpers next door (``pgsql``, ``mql``, ``http_encoding``) are
imported by ``drakkar.config`` to validate operator-authored statements at
config load. Importing a submodule executes its package's ``__init__``
first, so any top-level import here that reaches ``drakkar.config`` — and
every sink class does, directly or through ``BaseSink`` — would make config
load fail with a partially-initialized-module ImportError. The re-exports
below therefore resolve through :pep:`562` module ``__getattr__``.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drakkar.sinks.base import BaseSink
    from drakkar.sinks.manager import AmbiguousSinkError, SinkManager, SinkNotConfiguredError

# Public name → module that defines it. Data rather than a chain of ``if``s
# in ``__getattr__``, so adding a re-export is a row instead of a branch.
_REEXPORTS: dict[str, str] = {
    'AmbiguousSinkError': 'drakkar.sinks.manager',
    'BaseSink': 'drakkar.sinks.base',
    'SinkManager': 'drakkar.sinks.manager',
    'SinkNotConfiguredError': 'drakkar.sinks.manager',
}

__all__ = [
    'AmbiguousSinkError',
    'BaseSink',
    'SinkManager',
    'SinkNotConfiguredError',
]


def __getattr__(name: str) -> Any:
    """Resolve a re-exported name on first access (PEP 562).

    ``from drakkar.sinks import SinkManager`` reaches this hook, so the
    public surface is unchanged for callers — only the moment of import
    moved.
    """
    module_name = _REEXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    from importlib import import_module

    return getattr(import_module(module_name), name)


def __dir__() -> list[str]:
    """Keep ``dir(drakkar.sinks)`` honest about the lazy names."""
    return sorted(__all__)
