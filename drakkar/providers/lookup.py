"""Provider lookup: a feature's built-in table plus the plugins installed for it.

Each feature has its own entry-point group (the registry
``drakkar.registry_providers``, the archive election
``drakkar.election_providers``). A package adds a provider by declaring an
entry point in that group whose target is a factory ``(options) -> provider``::

    [project.entry-points."drakkar.registry_providers"]
    consul = "drakkar_consul:make_registry"

A plugin wins over a built-in of the same name, so a package can replace a
built-in with its own variant. A plugin that fails to load is logged and
skipped, exactly like a broken sink plugin; the worker then fails at startup
only if the configured name is the one that is missing.
"""

from collections.abc import Mapping
from importlib.metadata import entry_points
from typing import Any

import structlog

logger = structlog.get_logger()


class UnknownProviderError(ValueError):
    """A configured provider name is neither a built-in nor an installed plugin."""


def discover_plugin_factories(group: str) -> dict[str, Any]:
    """Load every installed provider plugin of ``group``; broken ones are logged and left out.

    The log events keep the names the registry lookup has always used; the
    ``group`` field tells the features apart.
    """
    try:
        installed = entry_points(group=group)
    except Exception as exc:
        # A damaged install must not stop the worker: the built-ins still work.
        logger.warning(
            'fleet_registry_entry_point_scan_failed',
            category='fleet',
            group=group,
            error=str(exc),
        )
        return {}

    factories: dict[str, Any] = {}
    for entry_point in installed:
        try:
            factory = entry_point.load()
        except Exception as exc:
            logger.warning(
                'fleet_registry_entry_point_load_failed',
                category='fleet',
                group=group,
                name=entry_point.name,
                value=getattr(entry_point, 'value', '<unknown>'),
                error=str(exc),
            )
            continue
        if not callable(factory):
            logger.warning(
                'fleet_registry_entry_point_invalid',
                category='fleet',
                group=group,
                name=entry_point.name,
                value=getattr(entry_point, 'value', '<unknown>'),
                error='the entry point must load a factory (options) -> provider',
            )
            continue
        factories[entry_point.name] = factory
    return factories


def resolve_factory[F](group: str, builtins: Mapping[str, F], name: str, *, config_path: str) -> F:
    """Return the factory of provider ``name``: a plugin of ``group``, else the built-in.

    ``config_path`` is the config key that named the provider, for the error.

    Raises:
        UnknownProviderError: no built-in or plugin has that name; the
            message names the config key and lists the names that exist.
    """
    factories: dict[str, F] = {**builtins, **discover_plugin_factories(group)}
    factory = factories.get(name)
    if factory is None:
        raise UnknownProviderError(
            f"{config_path} '{name}' is not known; known providers: {', '.join(sorted(factories))}"
        )
    return factory
