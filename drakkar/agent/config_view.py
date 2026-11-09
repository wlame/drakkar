"""The answer of the agent config route: the effective config, one entry per configured key.

Each entry joins a config metadata row with the live value, its origin and the
advice of the registry. Values and defaults are written as text: a string as it
is, any other value as its compact JSON text (``4``, ``true``, ``null``, ``[]``).
Values with a unit are already their canonical text in the JSON dump of the
config.

Nothing secret leaves here. A secret key is ``***`` when it is set and ``''``
when it is empty. The secrets that live inside a value (an ``executor.env`` or
``client_config`` map, the client tokens of the HTTP source) are masked inside
it, and every other text loses the credentials of any URL in it. In the ``app``
section a key whose name holds a secret-like word is masked too.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from drakkar.agent.registry import Registry, dumps
from drakkar.config.origins import ORIGIN_DEFAULT, ORIGIN_ON_STARTUP, ORIGIN_YAML, ConfigOrigins, leaf_paths
from drakkar.recorder.helpers import sanitize_env_value
from drakkar.utils import redact_url

SECRET_TEXT = '***'
APP_SECTION = 'app'
# A key of the app section whose name holds one of these words, in any letter case, is masked.
APP_SECRET_WORDS = ('password', 'secret', 'token', 'key', 'dsn')


@dataclass(frozen=True)
class ConfigLeaf:
    """One leaf of the config metadata: a YAML path with ``*`` for map names."""

    path: str
    description: str
    type: str
    default: Any
    secret: bool


@dataclass(frozen=True)
class AppConfigView:
    """What the config route knows of the ``app`` section.

    ``leaves`` describe a handler's app config model (empty without one);
    ``values`` are its live values, or the raw section without a model;
    ``env_paths`` are the paths that the handler's own environment prefix gave;
    ``meanings`` are the descriptions of the handler's notes, by full path.
    """

    leaves: Sequence[ConfigLeaf] = ()
    values: Mapping[str, Any] = field(default_factory=dict)
    yaml_section: Mapping[str, Any] = field(default_factory=dict)
    env_paths: frozenset[tuple[str, ...]] = frozenset()
    meanings: Mapping[str, str] = field(default_factory=dict)


def value_text(value: Any) -> str:
    """A config value as text: a string as it is, anything else as compact JSON."""
    return value if isinstance(value, str) else dumps(value)


def _redact_texts(value: Any) -> Any:
    """Remove URL credentials from every text inside ``value``."""
    if isinstance(value, str):
        return redact_url(value)
    if isinstance(value, list):
        return [_redact_texts(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _redact_texts(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float)):
        return value
    # A YAML value of another type (a date in the open app section) is written as its text.
    return str(value)


def _mask_named_values(values: Any) -> Any:
    """Mask a ``{name: text}`` map by each name: a secret-like name hides its value, a URL loses its credentials."""
    if not isinstance(values, dict):
        return values
    return {
        name: sanitize_env_value(name, value) if isinstance(value, str) else value for name, value in values.items()
    }


def _mask_http_clients(clients: Any) -> Any:
    """Mask the ``token`` of each client of the HTTP source."""
    if not isinstance(clients, list):
        return clients
    masked = []
    for client in clients:
        if isinstance(client, dict) and 'token' in client:
            client = {**client, 'token': SECRET_TEXT if client['token'] else ''}
        masked.append(client)
    return masked


# Keys whose secret lives inside the value, so the metadata's secret flag
# cannot describe it. Keyed by the metadata path, map names written ``*``.
_INNER_MASKERS: dict[str, Callable[[Any], Any]] = {
    'sources.http.clients': _mask_http_clients,
    'executor.env': _mask_named_values,
    'connections.kafka.*.client_config': _mask_named_values,
    'dlq.client_config': _mask_named_values,
    'sinks.kafka.*.client_config': _mask_named_values,
}


def masked_text(path: str, value: Any, *, secret: bool) -> str:
    """The text an answer may carry for ``value`` at the metadata ``path``."""
    if secret:
        return SECRET_TEXT if value else ''
    masker = _INNER_MASKERS.get(path)
    if masker is not None:
        value = masker(value)
    return value_text(_redact_texts(value))


def expand_path(node: Any, segments: Sequence[str]) -> list[tuple[list[str], Any]]:
    """Every concrete path and value that ``segments`` names in ``node``; ``*`` takes every map name."""
    if not segments:
        return [([], node)]
    head, rest = segments[0], segments[1:]
    if not isinstance(node, dict):
        return []
    if head == '*':
        names = list(node)
    elif head in node:
        names = [head]
    else:
        return []
    return [([name, *tail], value) for name in names for tail, value in expand_path(node[name], rest)]


def _lookup(node: Any, segments: Sequence[str]) -> tuple[bool, Any]:
    for segment in segments:
        if not isinstance(node, dict) or segment not in node:
            return False, None
        node = node[segment]
    return True, node


def advice_by_path(registry: Registry) -> dict[str, str]:
    """The advice of the registry's config entries, by entry key."""
    return {entry['key']: entry['advice'] for entry in registry.parts['config'] if 'advice' in entry}


def advice_of(advice: Mapping[str, str], path: str) -> str | None:
    """The advice for a metadata path: its own entry, else the longest ``.*`` block entry that covers it."""
    if path in advice:
        return advice[path]
    blocks = [key for key in advice if key.endswith('.*') and path.startswith(key[:-1])]
    return advice[max(blocks, key=len)] if blocks else None


@dataclass(frozen=True)
class ConfigSources:
    """Where the values of the framework sections came from.

    ``origins`` is ``None`` for a config built in code; then a value that differs
    from its default counts as given in the file. ``before_on_startup`` is the
    config dump taken before the handler's ``on_startup`` ran, ``None`` before.
    """

    origins: ConfigOrigins | None
    before_on_startup: Mapping[str, Any] | None


def _framework_origin(segments: list[str], value: Any, default: Any, sources: ConfigSources) -> str:
    if sources.before_on_startup is not None:
        found, before = _lookup(sources.before_on_startup, segments)
        if not found or before != value:
            return ORIGIN_ON_STARTUP
    if sources.origins is not None:
        return sources.origins.origin_of(tuple(segments))
    return ORIGIN_DEFAULT if value == default else ORIGIN_YAML


def _entry(
    *,
    path: str,
    value: str,
    default: str,
    origin: str,
    leaf: ConfigLeaf,
    meaning: str,
    advice: str | None,
    secret: bool,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        'path': path,
        'value': value,
        'default': default,
        'origin': origin,
        'type': leaf.type,
        'secret': secret,
        'meaning': meaning,
    }
    if advice is not None:
        entry['advice'] = advice
    return entry


def framework_entries(
    *,
    leaves: Sequence[ConfigLeaf],
    config_dump: Mapping[str, Any],
    sources: ConfigSources,
    parts: Sequence[str],
    registry: Registry,
    full: bool,
) -> list[dict[str, Any]]:
    """The entries of the framework sections in ``parts``, one per configured key."""
    advice_table = advice_by_path(registry) if full else {}
    entries = []
    for leaf in leaves:
        segments = leaf.path.split('.')
        if segments[0] not in parts:
            continue
        advice = advice_of(advice_table, leaf.path) if full else None
        default = masked_text(leaf.path, leaf.default, secret=leaf.secret)
        for concrete, value in expand_path(config_dump, segments):
            entries.append(
                _entry(
                    path='.'.join(concrete),
                    value=masked_text(leaf.path, value, secret=leaf.secret),
                    default=default,
                    origin=_framework_origin(concrete, value, leaf.default, sources),
                    leaf=leaf,
                    meaning=leaf.description,
                    advice=advice,
                    secret=leaf.secret,
                )
            )
    return entries


def is_secret_app_path(segments: Sequence[str]) -> bool:
    """True when a key name of an app path holds a secret-like word, in any letter case."""
    return any(word in segment.lower() for segment in segments for word in APP_SECRET_WORDS)


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, int):
        return 'integer'
    if isinstance(value, float):
        return 'number'
    if isinstance(value, list):
        return 'array'
    if isinstance(value, dict):
        return 'object'
    if value is None:
        return 'null'
    return 'string'


def _app_origin(segments: tuple[str, ...], view: AppConfigView) -> str:
    inner = segments[1:]
    origins = ConfigOrigins(yaml_paths=leaf_paths(view.yaml_section), env_paths=view.env_paths)
    return origins.origin_of(inner)


def app_entries(view: AppConfigView) -> list[dict[str, Any]]:
    """The entries of the ``app`` section: the keys of the handler's model, else the keys of the raw section."""
    if view.leaves:
        rows = [
            (leaf, concrete, value)
            for leaf in view.leaves
            for concrete, value in expand_path({APP_SECTION: view.values}, leaf.path.split('.'))
        ]
    else:
        rows = [
            (
                ConfigLeaf(path='.'.join(path), description='', type=_json_type(value), default=None, secret=False),
                list(path),
                value,
            )
            for path, value in _app_leaves(view.values)
        ]
    entries = []
    for leaf, concrete, value in rows:
        secret = leaf.secret or is_secret_app_path(concrete[1:])
        path = '.'.join(concrete)
        entries.append(
            _entry(
                path=path,
                value=masked_text(leaf.path, value, secret=secret),
                default=masked_text(leaf.path, leaf.default, secret=secret) if view.leaves else '',
                origin=_app_origin(tuple(concrete), view),
                leaf=leaf,
                meaning=view.meanings.get(path, ''),
                advice=None,
                secret=secret,
            )
        )
    return entries


def _app_leaves(values: Mapping[str, Any]) -> list[tuple[tuple[str, ...], Any]]:
    """The leaves of the raw app section under ``app``; a list and an empty map are leaves."""
    found: list[tuple[tuple[str, ...], Any]] = []

    def walk(node: Any, prefix: tuple[str, ...]) -> None:
        if isinstance(node, dict) and node:
            for key, item in node.items():
                walk(item, (*prefix, str(key)))
            return
        found.append((prefix, node))

    for key, item in values.items():
        walk(item, (APP_SECTION, str(key)))
    return found


def config_answer(
    *,
    leaves: Sequence[ConfigLeaf],
    config_dump: Mapping[str, Any],
    sources: ConfigSources,
    app: AppConfigView,
    parts: Sequence[str],
    registry: Registry,
    full: bool,
) -> dict[str, Any]:
    """``{"entries": [...]}`` for the asked parts, sorted by path in byte order."""
    entries = framework_entries(
        leaves=leaves,
        config_dump=config_dump,
        sources=sources,
        parts=parts,
        registry=registry,
        full=full,
    )
    if APP_SECTION in parts:
        entries.extend(app_entries(app))
    entries.sort(key=lambda entry: entry['path'].encode('utf-8'))
    return {'entries': entries}


def entries_by_part(entries: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    """The entries grouped by their root section."""
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for entry in entries:
        grouped.setdefault(entry['path'].split('.', 1)[0], []).append(entry)
    return grouped
