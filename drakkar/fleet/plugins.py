"""Registry provider lookup: the built-in table plus installed plugins.

A package adds a provider by declaring an entry point in the
``drakkar.registry_providers`` group whose target is a factory
``(options) -> provider``::

    [project.entry-points."drakkar.registry_providers"]
    consul = "drakkar_consul:make_registry"

and the operator selects it with ``fleet.registry.provider: consul``. The
rules (a plugin wins over a built-in of the same name, a broken plugin is
logged and skipped) are those of :mod:`drakkar.providers.lookup`, which every
provider-backed feature shares.
"""

from drakkar.fleet.provider import RegistryFactory
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.providers import lookup
from drakkar.providers.lookup import UnknownProviderError

# Public and stable: plugin packages name this group in their metadata.
ENTRY_POINT_GROUP = 'drakkar.registry_providers'

_CONFIG_PATH = 'fleet.registry.provider'


class UnknownRegistryProviderError(UnknownProviderError):
    """``fleet.registry.provider`` names neither a built-in nor an installed plugin."""


def discover_plugin_factories() -> dict[str, RegistryFactory]:
    """Load every installed registry provider plugin; broken ones are logged and left out."""
    return lookup.discover_plugin_factories(ENTRY_POINT_GROUP)


def resolve_provider_factory(name: str) -> RegistryFactory:
    """Return the factory of provider ``name``.

    Raises:
        UnknownRegistryProviderError: no built-in or plugin has that name;
            the message lists the names that exist.
    """
    try:
        return lookup.resolve_factory(ENTRY_POINT_GROUP, BUILTIN_PROVIDERS, name, config_path=_CONFIG_PATH)
    except UnknownProviderError as exc:
        raise UnknownRegistryProviderError(str(exc)) from None
