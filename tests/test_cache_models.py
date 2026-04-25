"""Tests for the foundational cache models in drakkar/cache.py.

Covers the building-block primitives used by the cache engine and the
handler-facing API:

- `CacheScope` — string-valued Enum for scope discriminator
- `CacheEntry` — dataclass wrapping a cached value with all DB-relevant metadata
- `encode_value` / `decode_value` — JSON-only serialization helpers with Pydantic revival support

These helpers run on the hot path (every `set` and `get`), so tests assert
both the happy path (primitives, lists, dicts, Pydantic models) and the
deliberate choice to stay JSON-only (no binary codecs).
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from drakkar.cache import CacheEntry, CacheScope, decode_value, encode_value

# --- CacheScope --------------------------------------------------------------


def test_cache_scope_members_and_values():
    """The enum exposes exactly three scopes with the string values the SQL
    CHECK constraint references."""
    assert CacheScope.LOCAL.value == 'local'
    assert CacheScope.CLUSTER.value == 'cluster'
    assert CacheScope.GLOBAL.value == 'global'
    # exactly three members — no hidden INTERNAL/DEBUG scopes
    assert {s.value for s in CacheScope} == {'local', 'cluster', 'global'}


def test_cache_scope_is_str_enum():
    """CacheScope values must behave like strings for SQL parameter binding
    and JSON round-tripping without extra coercion."""
    # StrEnum members double as str instances
    assert isinstance(CacheScope.LOCAL, str)
    assert CacheScope.LOCAL == 'local'


# --- CacheEntry --------------------------------------------------------------


def test_cache_entry_requires_all_fields():
    """Every field is required — the engine always populates all of them
    when constructing an entry (no silent defaults for timestamps)."""
    entry = CacheEntry(
        key='k1',
        scope=CacheScope.LOCAL,
        value='"hello"',
        size_bytes=7,
        created_at_ms=100,
        updated_at_ms=100,
        expires_at_ms=None,
        origin_worker_id='worker-a',
    )
    assert entry.key == 'k1'
    assert entry.scope == CacheScope.LOCAL
    assert entry.value == '"hello"'
    assert entry.size_bytes == 7
    assert entry.created_at_ms == 100
    assert entry.updated_at_ms == 100
    assert entry.expires_at_ms is None
    assert entry.origin_worker_id == 'worker-a'


def test_cache_entry_expires_at_ms_is_optional():
    """`expires_at_ms=None` is the "never expires" sentinel — matches the
    nullable column in the SQL schema."""
    entry = CacheEntry(
        key='k',
        scope=CacheScope.GLOBAL,
        value='null',
        size_bytes=4,
        created_at_ms=1,
        updated_at_ms=1,
        expires_at_ms=None,
        origin_worker_id='w1',
    )
    assert entry.expires_at_ms is None


def test_cache_entry_size_bytes_matches_serialized_length():
    """`size_bytes` is the byte length of the serialized JSON — the caller
    populates it at construction time, so here we just assert the field is
    preserved exactly as given."""
    value_json = json.dumps({'a': 1, 'b': 'two'})
    size = len(value_json.encode('utf-8'))
    entry = CacheEntry(
        key='k',
        scope=CacheScope.LOCAL,
        value=value_json,
        size_bytes=size,
        created_at_ms=10,
        updated_at_ms=10,
        expires_at_ms=None,
        origin_worker_id='w1',
    )
    assert entry.size_bytes == size


# --- encode_value -----------------------------------------------------------------


@pytest.mark.parametrize(
    'value',
    [
        'hello',
        42,
        3.14,
        True,
        False,
        None,
    ],
)
def testencode_value_primitive_roundtrips_via_json(value):
    """Primitives encode to their `json.dumps` form, and the reported byte
    count matches the UTF-8 length of the string."""
    encoded, size = encode_value(value)
    assert encoded == json.dumps(value)
    assert size == len(encoded.encode('utf-8'))


def testencode_value_list_and_dict():
    """Containers encode via plain json.dumps — no Pydantic detour for
    primitive-only structures."""
    data = {'items': [1, 2, 3], 'name': 'test'}
    encoded, size = encode_value(data)
    assert json.loads(encoded) == data
    assert size == len(encoded.encode('utf-8'))


def testencode_value_pydantic_model_uses_model_dump_json():
    """Pydantic models go through `model_dump_json()` so we get the model's
    declared field types (e.g. Enum values, datetime iso) — not whatever
    `__dict__` happens to contain."""

    class User(BaseModel):
        id: int
        name: str

    user = User(id=7, name='alice')
    encoded, size = encode_value(user)
    # reparse to assert shape, not exact formatting (model_dump_json might
    # produce a slightly different key order than we'd write by hand)
    parsed = json.loads(encoded)
    assert parsed == {'id': 7, 'name': 'alice'}
    assert size == len(encoded.encode('utf-8'))


def testencode_value_size_uses_utf8_byte_count_not_codepoint_count():
    """Multi-byte UTF-8 characters must be counted by byte length — this is
    what SQLite will store and what Prometheus `bytes_in_memory` must reflect.
    """
    value = 'café'  # 'é' = 2 bytes in UTF-8, so 5 bytes not 4 codepoints
    encoded, size = encode_value(value)
    # JSON adds surrounding quotes → 'café' becomes '"café"' (7 bytes)
    assert size == len(encoded.encode('utf-8'))
    assert size > len(value)  # bytes > codepoints


# --- decode_value -----------------------------------------------------------------


@pytest.mark.parametrize(
    'value',
    [
        'hello',
        42,
        3.14,
        True,
        None,
        [1, 2, 3],
        {'a': 1, 'b': 'two'},
    ],
)
def testdecode_value_without_as_type_returns_plain_json(value):
    """Without `as_type`, decode returns the raw parsed JSON — no magic
    revival into domain objects."""
    encoded = json.dumps(value)
    assert decode_value(encoded) == value


def testdecode_value_dict_stays_a_plain_dict_without_as_type():
    """A dict-shaped JSON input must stay a plain dict — we explicitly
    do NOT try to guess whether it's a Pydantic model."""
    encoded = json.dumps({'id': 7, 'name': 'alice'})
    decoded = decode_value(encoded)
    assert isinstance(decoded, dict)
    assert not isinstance(decoded, BaseModel)
    assert decoded == {'id': 7, 'name': 'alice'}


def testdecode_value_with_as_type_revives_pydantic_model():
    """Pass `as_type=MyModel` to get a typed instance back via
    `model_validate` — the standard Pydantic entry point for external data."""

    class User(BaseModel):
        id: int
        name: str

    encoded = json.dumps({'id': 7, 'name': 'alice'})
    user = decode_value(encoded, as_type=User)
    assert isinstance(user, User)
    assert user.id == 7
    assert user.name == 'alice'


def testdecode_value_with_as_type_roundtripsencode_valued_pydantic():
    """End-to-end: encode a Pydantic model, decode with its type, equality."""

    class Item(BaseModel):
        sku: str
        qty: int

    original = Item(sku='abc', qty=3)
    encoded, _ = encode_value(original)
    decoded = decode_value(encoded, as_type=Item)
    assert decoded == original


def testdecode_value_empty_string_raises():
    """An empty string is not valid JSON — surface the error rather than
    silently returning None, which could mask a data-corruption bug."""
    with pytest.raises(json.JSONDecodeError):
        decode_value('')
