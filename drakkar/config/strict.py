"""Strict config keys: the walker that finds every unknown key, and the env map names.

The walker runs on the raw merged tree (YAML plus ``DK_*`` environment) before any
field check, so one message names every unknown key at once. The rules:

- a model field is found by its YAML key;
- a map of models is keyed by operator names: the name is not checked, the level
  below it is;
- a list of models is checked element by element, written ``[i]``;
- a free map (``dict[str, str]``, ``dict[str, Any]``) and a union with no model
  branch are open;
- a mapping at the position of a list of models is walked as one element, with no
  index in the path: a list field such as ``when`` also takes the single-element form;
- any other value of another shape than its field is not walked: the field check
  reports it.

The message bytes are a contract (the ``unknown_keys`` section of
``tests/fixtures/config_contract.json``).
"""

import json
import types
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from drakkar.config.base import yaml_text

UNKNOWN_KEYS_HEADER = (
    'config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; '
    'see docs/config-reference.md):'
)

# How far a written key may be from a valid sibling to be suggested.
_SUGGESTION_MAX_DISTANCE = 2
_SHORT_KEY_LENGTH = 5
_SHORT_KEY_MAX_DISTANCE = 1


@dataclass(frozen=True)
class UnknownKey:
    """One key that no model declares, and the valid sibling it most resembles."""

    path: str
    suggestion: str | None


def yaml_key(name: str, info: FieldInfo) -> str:
    """The key an operator writes for a field: its alias when it has one, else its name.

    A field cannot always carry the YAML key as its name (``schema`` would
    shadow a ``BaseModel`` attribute), so it takes an alias.
    """
    return info.alias or name


def _is_model(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _holds_model(annotation: Any) -> bool:
    """Whether a model can be reached through this annotation."""
    shape = _shape(annotation)
    return _is_model(shape) or any(_holds_model(arg) for arg in get_args(shape))


def _shape(annotation: Any) -> Any:
    """The annotation the walker follows: no ``Annotated``, and a union reduced to its branch that holds a model.

    A union with no such branch keeps its union type, which the walker treats as open.
    """
    origin = get_origin(annotation)
    if origin is Annotated:
        return _shape(get_args(annotation)[0])
    if origin is Union or origin is types.UnionType:
        for branch in get_args(annotation):
            if _holds_model(branch):
                return _shape(branch)
    return annotation


def _levenshtein(left: str, right: str) -> int:
    """The edit distance between two texts, by characters."""
    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(
                min(
                    previous[right_index] + 1,
                    current[right_index - 1] + 1,
                    previous[right_index - 1] + (left_char != right_char),
                )
            )
        previous = current
    return previous[-1]


def _suggestion(written: str, siblings: Sequence[str]) -> str | None:
    """The sibling with the smallest distance within the limit; a tie goes to the key that sorts first."""
    limit = _SHORT_KEY_MAX_DISTANCE if len(written) < _SHORT_KEY_LENGTH else _SUGGESTION_MAX_DISTANCE
    closest = min(sorted(siblings), key=lambda sibling: _levenshtein(written, sibling), default=None)
    if closest is None or _levenshtein(written, closest) > limit:
        return None
    return closest


def _join(path: str, key: str) -> str:
    return f'{path}.{key}' if path else key


def _walk_model(model: type[BaseModel], tree: Mapping[Any, Any], path: str, found: list[UnknownKey]) -> None:
    fields = {yaml_key(name, info): info for name, info in model.model_fields.items()}
    for key, value in tree.items():
        info = fields.get(key) if isinstance(key, str) else None
        if info is None:
            text = yaml_text(key)
            found.append(UnknownKey(path=_join(path, text), suggestion=_suggestion(text, list(fields))))
        else:
            _walk_value(info.annotation, value, _join(path, key), found)


def _walk_value(annotation: Any, value: Any, path: str, found: list[UnknownKey]) -> None:
    shape = _shape(annotation)
    if _is_model(shape):
        if isinstance(value, Mapping):
            _walk_model(shape, value, path, found)
        return
    origin = get_origin(shape)
    args = get_args(shape)
    if origin is dict and isinstance(value, Mapping):
        for name, item in value.items():
            _walk_value(args[1], item, f'{path}.{yaml_text(name)}', found)
    elif origin is list and isinstance(value, list):
        for index, item in enumerate(value):
            _walk_value(args[0], item, f'{path}[{index}]', found)
    elif origin is list and isinstance(value, Mapping):
        # A mapping at a list position: the single-element form of a list of models.
        _walk_value(args[0], value, path, found)


def find_unknown_keys(tree: Mapping[str, Any], model: type[BaseModel]) -> list[UnknownKey]:
    """Every key of ``tree`` that ``model`` does not declare, in sorted path order.

    A key with children is reported alone: its children have no model to be checked against.
    """
    found: list[UnknownKey] = []
    _walk_model(model, tree, '', found)
    return sorted(found, key=lambda unknown: unknown.path)


def unknown_keys_message(found: Sequence[UnknownKey]) -> str:
    """The load error: the header, then one line per unknown key, indented by two spaces."""
    lines = [f"  {key.path} (did you mean '{key.suggestion}'?)" if key.suggestion else f'  {key.path}' for key in found]
    return '\n'.join([UNKNOWN_KEYS_HEADER, *lines])


def known_leaf_paths(model: type[BaseModel]) -> list[str]:
    """The leaf paths the walker accepts: ``*`` stands for an operator name, ``[]`` for a list element."""
    paths: list[str] = []
    _collect_model(model, '', paths)
    return paths


def _collect_model(model: type[BaseModel], path: str, paths: list[str]) -> None:
    for name, info in model.model_fields.items():
        _collect_value(info.annotation, _join(path, yaml_key(name, info)), paths)


def _collect_value(annotation: Any, path: str, paths: list[str]) -> None:
    shape = _shape(annotation)
    if _is_model(shape):
        _collect_model(shape, path, paths)
        return
    origin = get_origin(shape)
    args = get_args(shape)
    if origin is dict and _holds_model(args[1]):
        _collect_value(args[1], f'{path}.*', paths)
    elif origin is list and _holds_model(args[0]):
        _collect_value(args[0], f'{path}[]', paths)
    else:
        paths.append(path)


def resolve_env_map_names(
    env_tree: Mapping[str, Any],
    yaml_tree: Mapping[str, Any],
    model: type[BaseModel],
) -> dict[str, Any]:
    """Give each env segment at a map-name position the name the YAML tree already uses.

    An env variable name cannot spell every operator name (``main-db``), and its
    segments are lower-cased. A segment takes the existing name whose lower-case
    form with ``-`` replaced by ``_`` equals it, so
    ``DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX`` reaches ``sinks.postgres.main-db``.
    With no such name the segment stays as it is, and declares a name of its own.

    Raises:
        ValueError: when a segment matches two names.
    """
    return _resolve_model(model, env_tree, yaml_tree, '')


def _resolve_model(
    model: type[BaseModel],
    env_tree: Mapping[str, Any],
    yaml_tree: object,
    path: str,
) -> dict[str, Any]:
    yaml_values = yaml_tree if isinstance(yaml_tree, Mapping) else {}
    fields = {yaml_key(name, info): info for name, info in model.model_fields.items()}
    resolved: dict[str, Any] = {}
    for key, value in env_tree.items():
        info = fields.get(key)
        resolved[key] = (
            value if info is None else _resolve_value(info.annotation, value, yaml_values.get(key), _join(path, key))
        )
    return resolved


def _resolve_value(annotation: Any, env_value: Any, yaml_value: object, path: str) -> Any:
    shape = _shape(annotation)
    if _is_model(shape):
        return _resolve_model(shape, env_value, yaml_value, path) if isinstance(env_value, Mapping) else env_value
    origin = get_origin(shape)
    args = get_args(shape)
    if origin is dict and _holds_model(args[1]) and isinstance(env_value, Mapping):
        return _resolve_map_names(args[1], env_value, yaml_value, path)
    if origin is list and _holds_model(args[0]) and isinstance(env_value, list):
        yaml_items = yaml_value if isinstance(yaml_value, list) else []
        return [
            _resolve_value(
                args[0],
                item,
                yaml_items[index] if index < len(yaml_items) else None,
                f'{path}[{index}]',
            )
            for index, item in enumerate(env_value)
        ]
    return env_value


def _resolve_map_names(
    value_annotation: Any,
    env_map: Mapping[str, Any],
    yaml_value: object,
    path: str,
) -> dict[str, Any]:
    yaml_map = yaml_value if isinstance(yaml_value, Mapping) else {}
    resolved: dict[str, Any] = {}
    # Two segments can reach one name (``main_db`` and a literal ``main-db``), and
    # both must land. Sorted, so a key that both set has one result.
    for segment, item in sorted(env_map.items()):
        name = _existing_name(segment, yaml_map, path)
        value = _resolve_value(value_annotation, item, yaml_map.get(name), f'{path}.{name}')
        resolved[name] = deep_merge(resolved[name], value) if name in resolved else value
    return resolved


def _existing_name(segment: str, yaml_map: Mapping[Any, Any], path: str) -> str:
    matches = sorted(name for name in yaml_map if isinstance(name, str) and name.lower().replace('-', '_') == segment)
    if len(matches) > 1:
        raise ValueError(
            f"the environment name '{segment}' under {path} matches more than one configured name: {', '.join(matches)}"
        )
    return matches[0] if matches else segment


def decode_env_json(merged: Mapping[str, Any], env_tree: Mapping[str, Any], model: type[BaseModel]) -> dict[str, Any]:
    """Decode each ``DK_`` value that is a JSON text for a list or map field.

    ``DK_RECORDER__CAPTURE__ENV_VARS='["A","B"]'`` gives the list. The decoded value replaces
    the whole YAML value, so this runs on the tree after the merge, which would
    otherwise overlay a list by index. ``env_tree`` holds the resolved env values.
    A text that is not a JSON list or object stays text, and the field check reports it.
    """
    return _decode_model(model, merged, env_tree)


def _decode_model(model: type[BaseModel], merged: Mapping[str, Any], env_tree: Mapping[str, Any]) -> dict[str, Any]:
    fields = {yaml_key(name, info): info for name, info in model.model_fields.items()}
    decoded = dict(merged)
    for key, env_value in env_tree.items():
        info = fields.get(key)
        if info is not None:
            decoded[key] = _decode_value(info.annotation, merged[key], env_value)
    return decoded


def _decode_value(annotation: Any, merged: Any, env_value: Any) -> Any:
    shape = _shape(annotation)
    if isinstance(env_value, str):
        return _decoded_json(env_value) if _takes_json(shape) else merged
    if _is_model(shape):
        return _decode_model(shape, merged, env_value) if isinstance(env_value, Mapping) else merged
    origin = get_origin(shape)
    args = get_args(shape)
    if origin is dict and isinstance(env_value, Mapping):
        return {**merged, **{name: _decode_value(args[1], merged[name], item) for name, item in env_value.items()}}
    if origin is list and isinstance(env_value, list):
        items = list(merged)
        for index, item in enumerate(env_value):
            items[index] = _decode_value(args[0], items[index], item)
        return items
    return merged


def _takes_json(shape: Any) -> bool:
    """Whether a field holds a list or a map, in any branch of a union."""
    origin = get_origin(shape)
    if origin is Union or origin is types.UnionType:
        return any(_takes_json(_shape(branch)) for branch in get_args(shape))
    return (origin or shape) in (dict, list, set, frozenset, tuple)


def _decoded_json(text: str) -> Any:
    try:
        decoded = json.loads(text)
    except ValueError:
        return text
    return decoded if isinstance(decoded, dict | list) else text


def deep_merge(base: Any, override: Any) -> Any:
    """Deep-merge override on top of base. Override wins for leaf values.

    When both sides are lists, merge element-by-element by index: the
    override's i-th element overrides base's i-th element (recursively
    if both are dicts), and any extra base elements past the override's
    length are preserved. This supports the env-var override pattern
    where ``DK_SOURCES__HTTP__CLIENTS__0__RPM=10`` should change only the
    first client's rpm without dropping the rest of the clients defined
    in YAML.
    """
    if isinstance(base, dict) and isinstance(override, dict):
        result_dict: dict[Any, Any] = dict(base)
        for key, val in override.items():
            if key in result_dict:
                result_dict[key] = deep_merge(result_dict[key], val)
            else:
                result_dict[key] = val
        return result_dict
    if isinstance(base, list) and isinstance(override, list):
        merged_list: list[Any] = []
        for i in range(max(len(base), len(override))):
            if i < len(base) and i < len(override):
                merged_list.append(deep_merge(base[i], override[i]))
            elif i < len(override):
                merged_list.append(override[i])
            else:
                merged_list.append(base[i])
        return merged_list
    # Leaf or type mismatch: override wins.
    return override
