"""Where each config value came from: the model default, the YAML document or a ``DK_`` variable.

The loader records the leaf paths of the YAML document and of the environment
overrides, keyed by YAML key, before the two are merged. A config path is given
by a source when one of its recorded paths equals it, lies under it (a ``DK_``
variable that sets one key of a map such as ``executor.env``) or holds it (a
list written as one value).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

ORIGIN_DEFAULT = 'default'
ORIGIN_YAML = 'yaml'
ORIGIN_ENV = 'env'
ORIGIN_ON_STARTUP = 'on_startup'

Origin = Literal['default', 'yaml', 'env', 'on_startup']

ConfigPath = tuple[str, ...]


def leaf_paths(tree: Any, prefix: ConfigPath = ()) -> frozenset[ConfigPath]:
    """The paths of the leaves of a raw config tree; a list is a leaf, an empty map gives no path.

    An empty map gives nothing because it sets no value: ``connections.kafka.main: {}``
    declares a name and leaves every key of it at its default.
    """
    if not isinstance(tree, Mapping):
        return frozenset({prefix}) if prefix else frozenset()
    found: set[ConfigPath] = set()
    for key, value in tree.items():
        found |= leaf_paths(value, (*prefix, str(key)))
    return frozenset(found)


def _given(path: ConfigPath, given_paths: Iterable[ConfigPath]) -> bool:
    """True when a recorded path equals ``path``, lies under it, or holds it."""
    for given in given_paths:
        shorter = min(len(given), len(path))
        if given[:shorter] == path[:shorter]:
            return True
    return False


@dataclass(frozen=True)
class ConfigOrigins:
    """The leaf paths that the YAML document and the ``DK_`` variables gave, by YAML key."""

    yaml_paths: frozenset[ConfigPath] = frozenset()
    env_paths: frozenset[ConfigPath] = frozenset()

    @classmethod
    def from_sources(cls, yaml_data: Any, env_overrides: Any) -> ConfigOrigins:
        """Record the leaves of the YAML document and of the environment overrides."""
        return cls(yaml_paths=leaf_paths(yaml_data), env_paths=leaf_paths(env_overrides))

    def origin_of(self, path: ConfigPath) -> Origin:
        """``env`` over ``yaml`` over ``default`` for one config path."""
        if _given(path, self.env_paths):
            return ORIGIN_ENV
        if _given(path, self.yaml_paths):
            return ORIGIN_YAML
        return ORIGIN_DEFAULT
