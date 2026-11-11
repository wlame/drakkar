"""The message identity: one value per incoming message, written into every log line.

``logging.message_identity`` names a log field and the place of its value in a
message: the key, a header, or a field of the JSON value. Every log line
written while the message is processed carries the field; a line that covers
several messages (an arrange window, a task built from several messages)
carries the list of their values. Each task also gets the value as a label, so
the recorder and the UI search by label find the message.

The rules here are pinned byte for byte by ``tests/fixtures/message_identity.json``.
"""

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

MAX_IDENTITY_CHARS = 256

NAME_PATTERN = re.compile(r'[a-z_][a-z0-9_]{0,63}')

# Fields the framework writes into log lines itself; a message identity with
# one of these names would replace or shadow them.
RESERVED_NAMES = frozenset(
    {
        'category',
        'consumer_group',
        'drakkar',
        'error',
        'event',
        'exc_info',
        'exception',
        'hook',
        'level',
        'message',
        'module',
        'offset',
        'offsets',
        'partition',
        'request_id',
        'service_name',
        'service_version',
        'sink_name',
        'sink_type',
        'stack',
        'task_id',
        'timestamp',
        'window_id',
        'worker_id',
    }
)

SOURCE_KEY = 'key'
SOURCE_HEADER_PREFIX = 'header:'
SOURCE_VALUE_PREFIX = 'value:'

type SourceKind = Literal['key', 'header', 'value']
type Headers = Sequence[tuple[str, bytes | None]]


@dataclass(frozen=True, slots=True)
class IdentitySource:
    """Where the identity of a message is read: the key, a header or a JSON value path."""

    kind: SourceKind
    header: str = ''
    path: tuple[str, ...] = ()


def name_error(name: str) -> str | None:
    """Return the load message for an unusable identity name, or None."""
    if not NAME_PATTERN.fullmatch(name):
        return (
            f"logging.message_identity.name '{name}' must be a lower-case identifier "
            'of at most 64 characters ([a-z_][a-z0-9_]*)'
        )
    if name in RESERVED_NAMES:
        return f"logging.message_identity.name '{name}' is a field the framework already writes; choose another name"
    return None


def parse_source(text: str) -> IdentitySource:
    """Parse ``logging.message_identity.source``; raise ValueError with the load message."""
    if text == SOURCE_KEY:
        return IdentitySource(kind='key')
    if text.startswith(SOURCE_HEADER_PREFIX) and len(text) > len(SOURCE_HEADER_PREFIX):
        return IdentitySource(kind='header', header=text[len(SOURCE_HEADER_PREFIX) :])
    if text.startswith(SOURCE_VALUE_PREFIX):
        segments = tuple(text[len(SOURCE_VALUE_PREFIX) :].split('.'))
        if all(segments):
            return IdentitySource(kind='value', path=segments)
    raise ValueError(f"logging.message_identity.source must be key, header:<name> or value:<path>, got '{text}'")


def _utf8_text(raw: bytes | None) -> str | None:
    """Decode bytes as UTF-8; None for no bytes or bytes that are not UTF-8."""
    if raw is None:
        return None
    try:
        return raw.decode('utf-8')
    except UnicodeDecodeError:
        return None


def _header_value(headers: Headers, name: str) -> str | None:
    """Return the first header named ``name`` (ASCII case ignored) as text."""
    wanted = name.lower()
    for header_name, header_value in headers:
        if header_name.lower() == wanted:
            return _utf8_text(header_value)
    return None


class _NumberText(str):
    """A JSON number kept as the text the message wrote (``1.0`` stays ``1.0``, ``1e3`` stays ``1e3``)."""

    __slots__ = ()


def _json_text(value: object) -> str | None:
    """The identity text of a JSON scalar, or None for null, an object or a list."""
    if isinstance(value, str):
        return str(value)
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return None


def _reject_constant(name: str) -> object:
    """json.loads hook that refuses NaN and Infinity, which are not JSON."""
    raise ValueError(f'not JSON: {name}')


def _value_at_path(value: bytes, path: tuple[str, ...]) -> str | None:
    """Follow ``path`` through the JSON document ``value``, which must be UTF-8."""
    try:
        node: object = json.loads(
            value.decode('utf-8'),
            parse_int=_NumberText,
            parse_float=_NumberText,
            parse_constant=_reject_constant,
        )
    except (ValueError, RecursionError):
        # UnicodeDecodeError is a ValueError too. A value nested deeper than
        # the interpreter's recursion limit cannot be read: no identity, and
        # never an error that would end the partition loop.
        return None
    for segment in path:
        if isinstance(node, dict):
            if segment not in node:
                return None
            node = node[segment]
        elif isinstance(node, list) and segment.isascii() and segment.isdigit():
            index = int(segment)
            if index >= len(node):
                return None
            node = node[index]
        else:
            return None
    return _json_text(node)


def extract_identity(source: IdentitySource, *, key: bytes | None, value: bytes, headers: Headers) -> str | None:
    """Read the identity of one message; None when the message has none.

    The text is cut to ``MAX_IDENTITY_CHARS`` characters, and an empty text
    is no identity.
    """
    if source.kind == 'key':
        text = _utf8_text(key)
    elif source.kind == 'header':
        text = _header_value(headers, source.header)
    else:
        text = _value_at_path(value, source.path)
    if not text:
        return None
    return text[:MAX_IDENTITY_CHARS]


def identity_field(values: Iterable[str | None], max_values: int) -> str | list[str] | None:
    """The log field of a scope: one value as text, several as a list, none as None.

    Missing values are left out and repeated ones kept once, in first-seen
    order; a list holds at most ``max_values`` values.
    """
    seen: dict[str, None] = {}
    for value in values:
        if value is not None and value not in seen:
            seen[value] = None
            if len(seen) == max_values:
                break
    if not seen:
        return None
    unique = list(seen)
    return unique[0] if len(unique) == 1 else unique


def identity_label(values: Iterable[str | None], max_values: int) -> str | None:
    """The task label of a task's source messages: the field values joined with ','."""
    field = identity_field(values, max_values)
    if field is None or isinstance(field, str):
        return field
    return ','.join(field)
