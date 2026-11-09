"""The agent knowledge registry: load, check and render it.

The registry is seven YAML files and one JSON Schema in ``drakkar/agentdocs/``.
Each YAML file holds a list of entries. The files are a contract: other
implementations read the same bytes, compute the same ``registry_version`` and
render the same JSON and Markdown (pinned by ``tests/fixtures/agent_contract.json``).

Canonical bytes: for each file in ``CANONICAL_FILE_ORDER``, the ASCII file
name, one LF, the decimal byte length of the file, one LF, then the raw file
bytes; the blocks are concatenated with nothing between them. The
``registry_version`` is the first 16 hex characters of the SHA-256 of these bytes.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from importlib import resources
from typing import Any

import yaml
from yaml.constructor import ConstructorError

from drakkar.agent.params import MANUAL_PARTS

# Each registry part and the file that holds it, in manual order.
PART_FILES: tuple[tuple[str, str], ...] = (
    ('concepts', 'concepts.yaml'),
    ('config', 'config.yaml'),
    ('metrics', 'metrics.yaml'),
    ('events', 'events.yaml'),
    ('recipes', 'recipes.yaml'),
    ('routes', 'routes.yaml'),
    ('rules', 'rules.yaml'),
)
SCHEMA_FILE = 'schema.json'
CANONICAL_FILE_ORDER: tuple[str, ...] = (SCHEMA_FILE, *(name for _, name in PART_FILES))
REGISTRY_VERSION_LENGTH = 16

# The kind of a reference and the part that holds its entries.
REFERENCE_KINDS: dict[str, str] = {
    'concept': 'concepts',
    'config': 'config',
    'metric': 'metrics',
    'event': 'events',
    'recipe': 'recipes',
    'route': 'routes',
    'rule': 'rules',
}
APP_REFERENCE_PREFIX = 'app:'

# Every field after ``key`` and ``summary``, in output order, with its Markdown label and style.
# One order serves every kind: no two kinds order their shared fields differently.
FIELD_STYLES: tuple[tuple[str, str, str], ...] = (
    ('body', '', 'body'),
    ('principles', 'Principles', 'lines'),
    ('type', 'Type', 'text'),
    ('default', 'Default', 'json'),
    ('advice', 'Advice', 'text'),
    ('meaning', 'Meaning', 'text'),
    ('healthy', 'Healthy', 'text'),
    ('promql', 'PromQL', 'promql'),
    ('when', 'When', 'text'),
    ('fields', 'Fields', 'fields'),
    ('action', 'Action', 'text'),
    ('question', 'Question', 'text'),
    ('steps', 'Steps', 'steps'),
    ('causes', 'Causes', 'causes'),
    ('use', 'Use', 'text'),
    ('cost', 'Cost', 'text'),
    ('severity', 'Severity', 'text'),
    ('title', 'Title', 'text'),
    ('explain', 'Explain', 'code'),
    ('suggestion', 'Suggestion', 'text'),
    ('inputs', 'Inputs', 'code_lines'),
    ('related', 'Related', 'refs'),
)
EMPTY_PART_MARKDOWN = 'No entries.'
PROMQL_PLACEHOLDERS = ('worker', 'cluster')

CONFIG_METADATA_RESOURCE = ('uiserver', 'config-metadata.json')


class RegistryError(ValueError):
    """The registry files break the schema, repeat a key or hold a reference that does not resolve."""

    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = list(problems)
        super().__init__('; '.join(self.problems))


class TopicNotFoundError(LookupError):
    """A ``topic`` reference that is malformed or names no entry."""


@dataclass(frozen=True)
class Registry:
    """The checked registry: entries per part, the config paths and the version."""

    parts: dict[str, tuple[dict[str, Any], ...]]
    config_paths: frozenset[str]
    version: str
    _index: dict[str, tuple[str, dict[str, Any]]] = field(repr=False, default_factory=dict)
    _config_metadata: dict[str, dict[str, Any]] = field(repr=False, default_factory=dict)

    def find(self, ref: str) -> tuple[str, dict[str, Any]] | None:
        """Return the part and entry a reference names, or ``None``."""
        return self._index.get(ref)

    def resolves(self, ref: str) -> bool:
        """Whether a reference resolves: an entry, or for ``config:`` a config metadata path."""
        kind, _, key = ref.partition(':')
        if kind == 'config':
            return config_key_matches(key, self.config_paths)
        return ref in self._index

    def config_metadata_entry(self, path: str) -> dict[str, Any] | None:
        """An entry built from the config metadata for an exact path, or ``None``.

        It holds ``key`` (the path), ``summary`` (the metadata description),
        ``type`` and ``default`` (the canonical value of the metadata). A topic
        or a reference to a config path with no advice entry answers it.
        """
        return self._config_metadata.get(path)


# --- canonical bytes ------------------------------------------------------


def canonical_bytes(files: Mapping[str, bytes]) -> bytes:
    """The canonical bytes of the registry files (see the module docstring)."""
    blocks = []
    for name in CANONICAL_FILE_ORDER:
        data = files[name]
        blocks.append(name.encode('ascii') + b'\n' + str(len(data)).encode('ascii') + b'\n' + data)
    return b''.join(blocks)


def registry_version(files: Mapping[str, bytes]) -> str:
    """The first 16 hex characters of the SHA-256 of the canonical bytes."""
    return hashlib.sha256(canonical_bytes(files)).hexdigest()[:REGISTRY_VERSION_LENGTH]


# --- schema checks --------------------------------------------------------

# Keywords that only annotate a schema; the checker reads none of them.
_ANNOTATION_KEYWORDS = frozenset({'$schema', 'title', 'description', '$defs'})
_JSON_TYPES: dict[str, Callable[[Any], bool]] = {
    'object': lambda value: isinstance(value, dict) and all(isinstance(name, str) for name in value),
    'array': lambda value: isinstance(value, list),
    'string': lambda value: isinstance(value, str),
    'integer': lambda value: isinstance(value, int) and not isinstance(value, bool),
    'boolean': lambda value: isinstance(value, bool),
}


@cache
def _compile_pattern(pattern: str) -> re.Pattern[str]:
    """Compile a schema pattern with ECMA semantics for ``$``: the end of the text only.

    Python's ``$`` also matches before a final line break, so a pattern that ends
    in an unescaped ``$`` is compiled with ``\\Z`` in its place. A pattern that
    ends in an escaped backslash before ``$`` is not recognised; the registry
    schema has none.
    """
    if pattern.endswith('$') and not pattern.endswith('\\$'):
        pattern = pattern[:-1] + '\\Z'
    return re.compile(pattern)


def schema_problems(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str = '$') -> list[str]:
    """Check ``instance`` against a schema of the subset the registry schema uses.

    The subset: ``$ref`` (to ``#/$defs/<name>``), ``type``, ``enum``, ``properties``,
    ``required``, ``additionalProperties`` (false or a schema), ``minProperties``,
    ``items``, ``minItems``, ``minLength``, ``maxLength`` (in code points) and
    ``pattern``. Any other keyword is refused, so the schema cannot grow past
    what every implementation checks.
    """
    problems: list[str] = []
    for keyword in schema:
        if keyword not in _CHECKS and keyword not in _ANNOTATION_KEYWORDS:
            raise ValueError(f"schema keyword '{keyword}' is not supported")
    if '$ref' in schema:
        name = schema['$ref'].removeprefix('#/$defs/')
        return schema_problems(instance=instance, schema=root['$defs'][name], root=root, path=path)
    expected_type = schema.get('type')
    if expected_type is not None and not _JSON_TYPES[expected_type](instance):
        return [f'{path}: must be of type {expected_type}']
    for keyword, check in _CHECKS.items():
        if keyword in schema and keyword not in ('$ref', 'type'):
            problems.extend(check(instance, schema, root, path))
    return problems


def _check_enum(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    if instance in schema['enum'] and isinstance(instance, str):
        return []
    return [f'{path}: must be one of {", ".join(schema["enum"])}']


def _check_properties(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    problems = []
    for name, sub in schema['properties'].items():
        if name in instance:
            problems.extend(schema_problems(instance=instance[name], schema=sub, root=root, path=f'{path}.{name}'))
    return problems


def _check_required(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    return [f'{path}: missing field {name}' for name in schema['required'] if name not in instance]


def _check_additional(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    known = schema.get('properties', {})
    extra = schema['additionalProperties']
    problems = []
    for name in instance:
        if name in known:
            continue
        if extra is False:
            problems.append(f'{path}: unknown field {name}')
        else:
            problems.extend(schema_problems(instance=instance[name], schema=extra, root=root, path=f'{path}.{name}'))
    return problems


def _check_min_properties(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    return [] if len(instance) >= schema['minProperties'] else [f'{path}: has too few fields']


def _check_items(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    problems = []
    for index, item in enumerate(instance):
        problems.extend(schema_problems(instance=item, schema=schema['items'], root=root, path=f'{path}[{index}]'))
    return problems


def _check_min_items(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    return [] if len(instance) >= schema['minItems'] else [f'{path}: needs at least {schema["minItems"]} items']


def _check_min_length(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    return [] if len(instance) >= schema['minLength'] else [f'{path}: is too short']


def _check_max_length(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    limit = schema['maxLength']
    return [] if len(instance) <= limit else [f'{path}: is longer than {limit} characters']


def _check_pattern(instance: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[str]:
    if _compile_pattern(schema['pattern']).search(instance):
        return []
    return [f'{path}: does not match {schema["pattern"]!r}']


_CHECKS: dict[str, Callable[[Any, Mapping[str, Any], Mapping[str, Any], str], list[str]]] = {
    '$ref': lambda *_: [],
    'type': lambda *_: [],
    'enum': _check_enum,
    'properties': _check_properties,
    'required': _check_required,
    'additionalProperties': _check_additional,
    'minProperties': _check_min_properties,
    'items': _check_items,
    'minItems': _check_min_items,
    'minLength': _check_min_length,
    'maxLength': _check_max_length,
    'pattern': _check_pattern,
}


# --- references -----------------------------------------------------------


def config_key_matches(key: str, config_paths: frozenset[str] | set[str]) -> bool:
    """Whether a config key names a metadata path.

    The key equals a path (map names written ``*``), or it ends in ``.*`` and
    covers a block: some path starts with the key minus ``*``.
    """
    if key in config_paths:
        return True
    if not key.endswith('.*'):
        return False
    prefix = key[:-1]
    return any(path.startswith(prefix) for path in config_paths)


def entry_references(entry: Mapping[str, Any]) -> list[str]:
    """The references an entry holds: ``related``, then ``explain`` of a rule."""
    refs = list(entry.get('related', ()))
    if 'explain' in entry:
        refs.append(entry['explain'])
    return refs


def reference_of(part: str, key: str) -> str:
    """The reference that names the entry ``key`` of ``part``."""
    kind = next(kind for kind, kind_part in REFERENCE_KINDS.items() if kind_part == part)
    return f'{kind}:{key}'


# --- loading --------------------------------------------------------------


class _StrictLoader(yaml.SafeLoader):
    """A safe YAML loader that refuses a mapping with the same key twice.

    A plain safe load keeps the last value, while a strict reader refuses
    the file; refusing here makes every reader fail the same way.
    """

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        """Build a mapping, raising on a repeated key."""
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise ConstructorError(None, None, f'repeated key {key!r}', key_node.start_mark)
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _parse_entries(name: str, data: bytes) -> Any:
    """Parse one YAML file; an empty file is an empty list."""
    try:
        parsed = yaml.load(data.decode('utf-8'), Loader=_StrictLoader)  # _StrictLoader derives from SafeLoader
    except (UnicodeDecodeError, yaml.YAMLError) as error:
        raise RegistryError([f'{name}: cannot be read: {error}']) from error
    return [] if parsed is None else parsed


def _normalize(entry: dict[str, Any]) -> dict[str, Any]:
    """Sort the ``fields`` map of an event by field name, so every reader sees one order."""
    if isinstance(entry.get('fields'), dict):
        entry['fields'] = dict(sorted(entry['fields'].items()))
    return entry


def _metadata_entries(config_metadata: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        row['path']: {
            'key': row['path'],
            'summary': row['description'],
            'type': row['type'],
            'default': row['default'],
        }
        for row in config_metadata
    }


def build_registry(files: Mapping[str, bytes], config_metadata: Sequence[Mapping[str, Any]]) -> Registry:
    """Parse and check the registry files; raise ``RegistryError`` with every problem found.

    ``config_metadata`` is the list of leaf entries of the config metadata, each with
    ``path`` (map names written ``*``), ``description``, ``type`` and ``default``.
    """
    schema = json.loads(files[SCHEMA_FILE].decode('utf-8'))
    document = {part: _parse_entries(name, files[name]) for part, name in PART_FILES}
    problems = schema_problems(instance=document, schema=schema, root=schema)
    if problems:
        raise RegistryError(problems)

    parts = {part: tuple(_normalize(dict(entry)) for entry in document[part]) for part, _ in PART_FILES}
    index: dict[str, tuple[str, dict[str, Any]]] = {}
    for part, entries in parts.items():
        for entry in entries:
            ref = reference_of(part, entry['key'])
            if ref in index:
                problems.append(f'{part}: key {entry["key"]} is repeated')
            index[ref] = (part, entry)

    registry = Registry(
        parts=parts,
        config_paths=frozenset(row['path'] for row in config_metadata),
        version=registry_version(files),
        _index=index,
        _config_metadata=_metadata_entries(config_metadata),
    )
    for entry in parts['config']:
        if not config_key_matches(entry['key'], registry.config_paths):
            problems.append(f'config: key {entry["key"]} names no config path')
    for part, entries in parts.items():
        for entry in entries:
            for ref in entry_references(entry):
                if not registry.resolves(ref):
                    problems.append(f'{part}: {entry["key"]} references {ref}, which does not resolve')
    if problems:
        raise RegistryError(problems)
    return registry


def read_registry_files() -> dict[str, bytes]:
    """The raw bytes of the registry files shipped in the package."""
    folder = resources.files('drakkar').joinpath('agentdocs')
    return {name: folder.joinpath(name).read_bytes() for name in CANONICAL_FILE_ORDER}


def read_config_metadata() -> list[dict[str, Any]]:
    """Every leaf entry of the shipped config metadata, map names written ``*``."""
    metadata = json.loads(resources.files('drakkar').joinpath(*CONFIG_METADATA_RESOURCE).read_text(encoding='utf-8'))
    return [entry for group in metadata['groups'] for entry in group['entries']]


@cache
def load_registry() -> Registry:
    """The shipped registry, checked once per process."""
    return build_registry(files=read_registry_files(), config_metadata=read_config_metadata())


# --- selection ------------------------------------------------------------

# One entry to render and whether it is rendered with every field.
Selected = tuple[dict[str, Any], bool]


def _app_index(app_entries: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {entry['key']: dict(entry) for entry in app_entries}


def _find_any(registry: Registry, ref: str, app: Mapping[str, dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
    """The part and entry a reference names: a handler note, a registry entry, or a config metadata path."""
    if ref.startswith(APP_REFERENCE_PREFIX):
        entry = app.get(ref)
        return None if entry is None else ('app', entry)
    found = registry.find(ref)
    if found is not None:
        return found
    kind, _, key = ref.partition(':')
    if kind == 'config':
        entry = registry.config_metadata_entry(key)
        if entry is not None:
            return ('config', entry)
    return None


def select_manual(
    registry: Registry,
    *,
    parts: Sequence[str] = MANUAL_PARTS,
    detail: str = 'brief',
    topic: str | None = None,
    app_entries: Sequence[Mapping[str, Any]] = (),
) -> list[tuple[str, list[Selected]]]:
    """Choose the entries of a manual answer, per part, in manual order.

    Without ``topic``: every entry of each asked part, in file order, at ``detail``.
    With ``topic`` (it overrides ``parts`` and ``detail``): the topic entry with every
    field and the entries it references, one level deep, with ``key`` and ``summary``;
    only parts that hold one of them are listed. A config path that has no advice
    entry is answered from the config metadata (``config_metadata_entry``); such
    entries follow the file entries of the part, sorted by path. A reference that
    names nothing (a ``.*`` block with no entry) is left out. Raises
    ``TopicNotFoundError`` when the topic names nothing.
    """
    app = _app_index(app_entries)
    all_entries: dict[str, tuple[dict[str, Any], ...]] = {**registry.parts, 'app': tuple(app.values())}
    full = detail == 'full'
    if topic is None:
        return [(part, [(entry, full) for entry in all_entries[part]]) for part in MANUAL_PARTS if part in parts]

    found = _find_any(registry, topic, app)
    if found is None:
        raise TopicNotFoundError(topic)
    chosen: dict[int, bool] = {id(found[1]): True}
    extra: dict[str, dict[str, dict[str, Any]]] = {part: {} for part in MANUAL_PARTS}
    for part, entry in [found] + [hit for ref in entry_references(found[1]) if (hit := _find_any(registry, ref, app))]:
        chosen.setdefault(id(entry), False)
        if not any(entry is listed for listed in all_entries[part]):
            extra[part][entry['key']] = entry
    selection = []
    for part in MANUAL_PARTS:
        picked = [(entry, chosen[id(entry)]) for entry in all_entries[part] if id(entry) in chosen]
        picked += [(extra[part][key], chosen[id(extra[part][key])]) for key in sorted(extra[part])]
        if picked:
            selection.append((part, picked))
    return selection


def fill_promql(query: str, filters: Mapping[str, str]) -> str:
    """Replace ``{{worker}}`` and ``{{cluster}}`` in a PromQL text with the label filters given."""
    for name in PROMQL_PLACEHOLDERS:
        if name in filters:
            query = query.replace('{{' + name + '}}', filters[name])
    return query


def _full_entry(entry: Mapping[str, Any], promql_filters: Mapping[str, str] | None) -> dict[str, Any]:
    rendered: dict[str, Any] = {'key': entry['key'], 'summary': entry['summary']}
    for name, _, _ in FIELD_STYLES:
        if name not in entry:
            continue
        value = entry[name]
        if name == 'promql' and promql_filters is not None:
            value = [{'title': row['title'], 'query': fill_promql(row['query'], promql_filters)} for row in value]
        rendered[name] = value
    return rendered


def selection_json(
    selection: Sequence[tuple[str, Sequence[Selected]]],
    promql_filters: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """The JSON answer of the manual: ``{"parts": {part: [entry, ...]}}``."""
    return {
        'parts': {
            part: [
                _full_entry(entry, promql_filters) if full else {'key': entry['key'], 'summary': entry['summary']}
                for entry, full in entries
            ]
            for part, entries in selection
        }
    }


# --- Markdown -------------------------------------------------------------


def _md_field(style: str, label: str, value: Any) -> str:
    if style == 'body':
        return value
    if style == 'text':
        return f'**{label}:** {value}'
    if style == 'code':
        return f'**{label}:** `{value}`'
    if style == 'json':
        return f'**{label}:** `{dumps(value)}`'
    if style == 'refs':
        return f'**{label}:** ' + ', '.join(f'`{ref}`' for ref in value)
    if style == 'lines':
        return '\n'.join([f'**{label}:**', *(f'- {line}' for line in value)])
    if style == 'code_lines':
        return '\n'.join([f'**{label}:**', *(f'- `{line}`' for line in value)])
    if style == 'fields':
        return '\n'.join([f'**{label}:**', *(f'- `{name}`: {text}' for name, text in value.items())])
    if style == 'promql':
        return '\n'.join([f'**{label}:**', *(f'- {row["title"]}: `{row["query"]}`' for row in value)])
    if style == 'steps':
        lines = [f'{number}. {step["do"]} Why: {step["why"]}' for number, step in enumerate(value, start=1)]
        return '\n'.join([f'**{label}:**', *lines])
    if style == 'causes':
        lines = [f'- {row["cause"]} Check: {row["check"]} Fix: {row["fix"]}' for row in value]
        return '\n'.join([f'**{label}:**', *lines])
    raise ValueError(f'unknown field style {style}')


def selection_markdown(
    selection: Sequence[tuple[str, Sequence[Selected]]],
    promql_filters: Mapping[str, str] | None = None,
) -> str:
    """The Markdown answer of the manual.

    Blocks joined by one empty line, ending with one LF: ``# <part>`` per part
    (``No entries.`` under an empty one), then per entry ``## <key>``, the summary,
    and with every field one block per present field in ``FIELD_STYLES`` order.
    """
    blocks: list[str] = []
    for part, entries in selection:
        blocks.append(f'# {part}')
        if not entries:
            blocks.append(EMPTY_PART_MARKDOWN)
        for entry, full in entries:
            blocks.append(f'## {entry["key"]}')
            blocks.append(entry['summary'])
            if not full:
                continue
            rendered = _full_entry(entry, promql_filters)
            blocks.extend(
                _md_field(style, label, rendered[name]) for name, label, style in FIELD_STYLES if name in rendered
            )
    return '\n\n'.join(blocks) + '\n'


def dumps(value: Any) -> str:
    """The JSON text of every agent answer: compact, UTF-8 characters as they are, no HTML escaping.

    Keys keep the order of the value (the contract order, then the registry schema
    order of entry fields). The text has no trailing line break.
    """
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def approx_tokens(text: str) -> int:
    """The token estimate of an answer: the UTF-8 byte size divided by 4, rounded up."""
    return math.ceil(len(text.encode('utf-8')) / 4)
