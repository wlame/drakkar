"""Unit tests for the HTTP sink body encoders."""

import pytest
from pydantic import BaseModel, RootModel

from drakkar.http_encoding import (
    FORM_CONTENT_TYPE,
    JSON_CONTENT_TYPE,
    HttpEncodingError,
    encode_body,
)


class Flat(BaseModel):
    zeta: str
    alpha: str
    mid: int


class Mixed(BaseModel):
    name: str
    count: int
    ratio: float
    ok: bool
    missing: str | None
    tags: list[str]
    meta: dict[str, int]


def test_json_encoding_matches_model_dump_json():
    model = Flat(zeta='z', alpha='a', mid=7)

    body, content_type = encode_body(model, 'json')

    assert body == model.model_dump_json().encode()
    assert content_type == JSON_CONTENT_TYPE


def test_form_encoding_sorts_field_names_lexicographically():
    model = Flat(zeta='z', alpha='a', mid=7)

    body, content_type = encode_body(model, 'form')

    assert body == b'alpha=a&mid=7&zeta=z'
    assert content_type == FORM_CONTENT_TYPE


def test_form_encoding_renders_each_json_type():
    model = Mixed(
        name='n',
        count=42,
        ratio=1.5,
        ok=True,
        missing=None,
        tags=['x', 'y'],
        meta={'k': 1},
    )

    body, _ = encode_body(model, 'form')

    assert body == (
        b'count=42&meta=%7B%22k%22%3A1%7D&missing=null&name=n&ok=true&ratio=1.5&tags=%5B%22x%22%2C%22y%22%5D'
    )


def test_form_encoding_preserves_large_integer_literals():
    class Big(BaseModel):
        n: int

    body, _ = encode_body(Big(n=9007199254740993), 'form')

    assert body == b'n=9007199254740993'


def test_form_encoding_escapes_reserved_characters():
    class Tricky(BaseModel):
        field: str

    body, _ = encode_body(Tricky(field='a&b=c d+e'), 'form')

    assert body == b'field=a%26b%3Dc+d%2Be'


def test_form_encoding_emits_non_ascii_literally_inside_inline_json():
    class Nested(BaseModel):
        meta: dict[str, str]

    body, _ = encode_body(Nested(meta={'k': 'café<&>'}), 'form')

    # The inline JSON keeps UTF-8 and does not HTML-escape; only the
    # urlencoding layer percent-encodes it.
    assert body == b'meta=%7B%22k%22%3A%22caf%C3%A9%3C%26%3E%22%7D'


def test_form_encoding_of_empty_model_is_empty_body():
    class Empty(BaseModel):
        pass

    body, content_type = encode_body(Empty(), 'form')

    assert body == b''
    assert content_type == FORM_CONTENT_TYPE


def test_form_encoding_rejects_non_object_top_level():
    class Listy(RootModel[list[int]]):
        pass

    with pytest.raises(HttpEncodingError) as excinfo:
        encode_body(Listy([1, 2]), 'form')

    assert 'object' in str(excinfo.value)


def test_unknown_encoding_raises():
    with pytest.raises(HttpEncodingError) as excinfo:
        encode_body(Flat(zeta='z', alpha='a', mid=1), 'xml')

    assert 'xml' in str(excinfo.value)


def test_multipart_encoding_frames_sorted_fields():
    model = Flat(zeta='z', alpha='a', mid=7)

    body, content_type = encode_body(model, 'multipart', boundary='BOUND')

    assert content_type == 'multipart/form-data; boundary=BOUND'
    assert body == (
        b'--BOUND\r\n'
        b'Content-Disposition: form-data; name="alpha"\r\n\r\n'
        b'a\r\n'
        b'--BOUND\r\n'
        b'Content-Disposition: form-data; name="mid"\r\n\r\n'
        b'7\r\n'
        b'--BOUND\r\n'
        b'Content-Disposition: form-data; name="zeta"\r\n\r\n'
        b'z\r\n'
        b'--BOUND--\r\n'
    )


def test_multipart_encoding_of_empty_model_is_closing_delimiter_only():
    class Empty(BaseModel):
        pass

    body, _ = encode_body(Empty(), 'multipart', boundary='BOUND')

    assert body == b'--BOUND--\r\n'


def test_multipart_encoding_escapes_quotes_in_field_names():
    class Raw(RootModel[dict[str, str]]):
        pass

    body, _ = encode_body(Raw({'a"b\\c': 'v'}), 'multipart', boundary='BOUND')

    assert b'name="a\\"b\\\\c"' in body


def test_multipart_rejects_field_name_with_line_break():
    class Raw(RootModel[dict[str, str]]):
        pass

    with pytest.raises(HttpEncodingError) as excinfo:
        encode_body(Raw({'a\nb': 'v'}), 'multipart', boundary='BOUND')

    assert 'line break' in str(excinfo.value)


def test_multipart_generates_a_unique_boundary_when_none_supplied():
    model = Flat(zeta='z', alpha='a', mid=1)

    _, first = encode_body(model, 'multipart')
    _, second = encode_body(model, 'multipart')

    assert first != second
    assert first.startswith('multipart/form-data; boundary=')
    assert len(first.rsplit('=', 1)[1]) == 60
