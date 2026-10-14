"""Registry provider lookup: the built-in table plus installed plugins.

A package adds a provider by declaring an entry point in the
``drakkar.registry_providers`` group whose target is a factory
``(options) -> provider``::

    [project.entry-points."drakkar.registry_providers"]
    consul = "drakkar_consul:make_registry"

and the operator selects it with ``fleet.registry.provider: consul``. A
plugin wins over a built-in of the same name, so a package can replace
``filesystem`` with its own variant. A plugin that fails to load is logged
and skipped, exactly like a broken sink plugin; the worker then fails at
startup only if the configured name is the one that is missing.
"""

from importlib.metadata import entry_points

import structlog

from drakkar.fleet.provider import RegistryFactory
from drakkar.fleet.providers import BUILTIN_PROVIDERS

logger = structlog.get_logger()

# Public and stable: plugin packages name this group in their metadata.
ENTRY_POINT_GROUP = 'drakkar.registry_providers'


class UnknownRegistryProviderError(ValueError):
    """``fleet.registry.provider`` names neither a built-in nor an installed plugin."""


def discover_plugin_factories() -> dict[str, RegistryFactory]:
    """Load every installed provider plugin; broken ones are logged and left out."""
    try:
        installed = entry_points(group=ENTRY_POINT_GROUP)
    except Exception as exc:
        # A damaged install must not stop the worker: the built-ins still work.
        logger.warning(
            'fleet_registry_entry_point_scan_failed',
            category='fleet',
            group=ENTRY_POINT_GROUP,
            error=str(exc),
        )
        return {}

    factories: dict[str, RegistryFactory] = {}
    for entry_point in installed:
        try:
            factory = entry_point.load()
        except Exception as exc:
            logger.warning(
                'fleet_registry_entry_point_load_failed',
                category='fleet',
                group=ENTRY_POINT_GROUP,
                name=entry_point.name,
                value=getattr(entry_point, 'value', '<unknown>'),
                error=str(exc),
            )
            continue
        if not callable(factory):
            logger.warning(
                'fleet_registry_entry_point_invalid',
                category='fleet',
                group=ENTRY_POINT_GROUP,
                name=entry_point.name,
                value=getattr(entry_point, 'value', '<unknown>'),
                error='the entry point must load a factory (options) -> provider',
            )
            continue
        factories[entry_point.name] = factory
    return factories


def resolve_provider_factory(name: str) -> RegistryFactory:
    """Return the factory of provider ``name``.

    Raises:
        UnknownRegistryProviderError: no built-in or plugin has that name;
            the message lists the names that exist.
    """
    factories = {**BUILTIN_PROVIDERS, **discover_plugin_factories()}
    factory = factories.get(name)
    if factory is None:
        raise UnknownRegistryProviderError(
            f"fleet.registry.provider '{name}' is not known; known providers: {', '.join(sorted(factories))}"
        )
    return factory
