"""HTTP sink request-body encodings.

The sink's ``encoding`` setting selects one of three body formats. This
module owns the bytes: given the payload model it returns both the
request body and the Content-Type that describes it, so the two can never
disagree.

The emitted body is contractual: a receiver may byte-compare it, so
``tests/fixtures/http_encoding_vectors.json`` pins it.

Compact JSON is written by hand here rather than delegated to
``json.dumps`` because JSON encoders disagree on defaults that are all
visible in the bytes: whether non-ASCII is escaped, whether ``<``, ``>``
and ``&`` are HTML-escaped, and whether U+2028/U+2029 are escaped.
Writing one explicit escape set removes the question.
"""

import json
import secrets
from typing import Any, cast
from urllib.parse import quote_plus

from pydantic import BaseModel

JSON_CONTENT_TYPE = 'application/json'
FORM_CONTENT_TYPE = 'application/x-www-form-urlencoded'
MULTIPART_CONTENT_TYPE = 'multipart/form-data; boundary={boundary}'

# 30 random bytes render as 60 hex characters — comfortably inside the
# RFC 2046 boundary length limit of 70.
_BOUNDARY_RANDOM_BYTES = 30

# Characters below U+0020 that JSON gives a short escape.
_SHORT_ESCAPES = {
    0x08: '\\b',
    0x09: '\\t',
    0x0A: '\\n',
    0x0C: '\\f',
    0x0D: '\\r',
}


class HttpEncodingError(Exception):
    """A payload could not be encoded in the configured body format."""


class _RawNumber(str):
    """A JSON number carried as its original literal text.

    ``json.loads`` would turn numbers into ``int``/``float``, and
    re-rendering them re-formats them — ``42`` can come back as ``42.0``,
    and a high-precision decimal can lose digits. Keeping the literal emits
    exactly the bytes that were in the marshalled JSON.

    Subclassing ``str`` means ``isinstance(value, str)`` is also true, so
    every check for this type must come first.
    """

    __slots__ = ()


def _escape_json_string(value: str) -> str:
    """Render a string as a quoted JSON string using the agreed escape set."""
    out = ['"']
    for char in value:
        code = ord(char)
        if char == '"':
            out.append('\\"')
        elif char == '\\':
            out.append('\\\\')
        elif code in _SHORT_ESCAPES:
            out.append(_SHORT_ESCAPES[code])
        elif code < 0x20:
            out.append(f'\\u{code:04x}')
        else:
            out.append(char)
    out.append('"')
    return ''.join(out)


def _compact_json(value: Any) -> str:
    """Render a decoded JSON value as compact JSON with sorted object keys."""
    if isinstance(value, _RawNumber):
        return str(value)
    if isinstance(value, str):
        return _escape_json_string(value)
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if value is None:
        return 'null'
    if isinstance(value, list):
        return '[' + ','.join(_compact_json(item) for item in value) + ']'
    if isinstance(value, dict):
        pairs = (f'{_escape_json_string(cast(str, key))}:{_compact_json(item)}' for key, item in sorted(value.items()))
        return '{' + ','.join(pairs) + '}'
    raise HttpEncodingError(f'cannot encode value of type {type(value).__name__}')


def _render_value(value: Any) -> str:
    """Render one top-level field value per the agreed rules.

    Strings pass through verbatim; every other JSON type renders as its
    compact JSON text.
    """
    if isinstance(value, _RawNumber):
        return str(value)
    if isinstance(value, str):
        return value
    return _compact_json(value)


def _extract_fields(data: BaseModel) -> list[tuple[str, str]]:
    """Flatten a model into sorted (name, rendered value) pairs."""
    decoded = json.loads(
        data.model_dump_json(),
        parse_int=_RawNumber,
        parse_float=_RawNumber,
    )
    if not isinstance(decoded, dict):
        raise HttpEncodingError(f'form encodings require a JSON object at the top level, got {type(decoded).__name__}')
    return [(name, _render_value(decoded[name])) for name in sorted(decoded)]


def _encode_form(data: BaseModel) -> bytes:
    """Encode as application/x-www-form-urlencoded."""
    pairs = (f'{quote_plus(name)}={quote_plus(value)}' for name, value in _extract_fields(data))
    return '&'.join(pairs).encode()


def generate_boundary() -> str:
    """Return a fresh multipart boundary (60 lowercase hex characters)."""
    return secrets.token_hex(_BOUNDARY_RANDOM_BYTES)


def _escape_field_name(name: str) -> str:
    """Escape a field name for a Content-Disposition header.

    Backslash and double quote are escaped, nothing else — the escaping
    RFC 7578 receivers expect.
    """
    return name.replace('\\', '\\\\').replace('"', '\\"')


def _encode_multipart(data: BaseModel, boundary: str) -> bytes:
    """Encode as multipart/form-data with field parts only.

    No filename parameter and no per-part Content-Type: this carries form
    fields, not files. The framing (CRLF everywhere, closing ``--`` on the
    final delimiter) follows RFC 2046 exactly.
    """
    chunks: list[bytes] = []
    for index, (name, value) in enumerate(_extract_fields(data)):
        if '\r' in name or '\n' in name:
            raise HttpEncodingError(f'multipart field name {name!r} contains a line break, which cannot be encoded')
        # Every part after the first is PREFIXED with CRLF, rather than each
        # part being trailed by one. That shape makes the zero-field body come
        # out as CRLF + close-delimiter with no special case, which is what
        # RFC 2046's close-delimiter grammar asks for.
        chunks.append((f'--{boundary}\r\n' if index == 0 else f'\r\n--{boundary}\r\n').encode())
        chunks.append(f'Content-Disposition: form-data; name="{_escape_field_name(name)}"\r\n\r\n'.encode())
        chunks.append(value.encode())
    chunks.append(f'\r\n--{boundary}--\r\n'.encode())
    return b''.join(chunks)


def encode_body(
    data: BaseModel,
    encoding: str,
    *,
    boundary: str | None = None,
) -> tuple[bytes, str]:
    """Encode a payload model into a request body and its Content-Type.

    ``boundary`` is only meaningful for multipart and exists so tests can
    pin the generated value; production passes ``None``.
    """
    if encoding == 'json':
        return data.model_dump_json().encode(), JSON_CONTENT_TYPE
    if encoding == 'form':
        return _encode_form(data), FORM_CONTENT_TYPE
    if encoding == 'multipart':
        resolved = boundary or generate_boundary()
        return _encode_multipart(data, resolved), MULTIPART_CONTENT_TYPE.format(boundary=resolved)
    raise HttpEncodingError(f"unknown HTTP sink encoding {encoding!r}; expected 'json', 'form', or 'multipart'")
