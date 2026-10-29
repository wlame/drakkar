"""Durations and sizes with units: the fixture cases, the annotated types, the metadata."""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel, Field, ValidationError

from drakkar.config.units import (
    ByteSize,
    Duration,
    WholeDays,
    WholeHours,
    WholeSeconds,
    format_duration,
    format_size,
    parse_duration,
    parse_size,
)
from drakkar.configmeta import walk_model_fields

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'config_units.json').read_text(encoding='utf-8'))
MESSAGES: dict[str, str] = FIXTURE['messages']


def _value_text(value: Any) -> str:
    """The input as the messages quote it."""
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _expected(message_key: str, value_text: str) -> str:
    return MESSAGES[message_key].replace('{v}', value_text)


@pytest.mark.parametrize('text, milliseconds', FIXTURE['durations'])
def test_parse_duration_accepts_the_fixture_case(text, milliseconds):
    assert parse_duration(text) == milliseconds


@pytest.mark.parametrize('value, message_key', FIXTURE['durations_refused'])
def test_parse_duration_refuses_the_fixture_case(value, message_key):
    with pytest.raises(ValueError) as excinfo:
        parse_duration(value)

    assert str(excinfo.value) == _expected(message_key, _value_text(value))


@pytest.mark.parametrize('scalar, message_key', FIXTURE['durations_refused_yaml'])
def test_parse_duration_refuses_the_yaml_scalar(scalar, message_key):
    with pytest.raises(ValueError) as excinfo:
        parse_duration(yaml.safe_load(scalar))

    assert str(excinfo.value) == _expected(message_key, scalar)


@pytest.mark.parametrize('text, size', FIXTURE['sizes'])
def test_parse_size_accepts_the_fixture_case(text, size):
    assert parse_size(text) == size


@pytest.mark.parametrize('value, message_key', FIXTURE['sizes_refused'])
def test_parse_size_refuses_the_fixture_case(value, message_key):
    with pytest.raises(ValueError) as excinfo:
        parse_size(value)

    assert str(excinfo.value) == _expected(message_key, _value_text(value))


@pytest.mark.parametrize('scalar, message_key', FIXTURE['sizes_refused_yaml'])
def test_parse_size_refuses_the_yaml_scalar(scalar, message_key):
    with pytest.raises(ValueError) as excinfo:
        parse_size(yaml.safe_load(scalar))

    assert str(excinfo.value) == _expected(message_key, scalar)


@pytest.mark.parametrize('milliseconds, text', FIXTURE['canonical_durations'])
def test_format_duration_gives_the_canonical_text(milliseconds, text):
    assert format_duration(milliseconds) == text
    assert parse_duration(text) == milliseconds


@pytest.mark.parametrize('size, text', FIXTURE['canonical_sizes'])
def test_format_size_gives_the_canonical_text(size, text):
    assert format_size(size) == text
    assert parse_size(text) == size


@pytest.mark.parametrize('formatter', [format_duration, format_size])
def test_format_refuses_a_negative_value(formatter):
    with pytest.raises(ValueError, match='negative'):
        formatter(-1)


@pytest.mark.parametrize('parser', [parse_duration, parse_size])
def test_a_value_that_is_no_text_or_number_is_refused(parser):
    with pytest.raises(ValueError, match="got 'null'"):
        parser(None)
    with pytest.raises(ValueError, match=r"got '\['1s'\]'"):
        parser(['1s'])


@pytest.mark.parametrize('parser', [parse_duration, parse_size])
def test_a_very_long_number_is_refused_with_the_contract_message(parser):
    text = '1' * 5000 + 's'

    with pytest.raises(ValueError, match=r'^is not a'):
        parser(text)


class _Units(BaseModel):
    """One field of each unit type, in the positions the config uses."""

    timeout: Duration = 5.0
    optional_timeout: Duration | None = None
    interval: WholeSeconds = 30
    rotation: WholeHours = 2
    retention: WholeDays = 30
    max_size: ByteSize = 1024
    durations: list[WholeSeconds] = Field(default_factory=lambda: [60, 300])


def _whole_type_cases() -> list[tuple[str, Any, int]]:
    return [
        (field, *case)
        for field, section in (('interval', 'whole_seconds'), ('rotation', 'whole_hours'), ('retention', 'whole_days'))
        for case in FIXTURE[section]['accepted']
    ]


def _whole_type_refusals() -> list[tuple[str, Any, str]]:
    return [
        (field, *case)
        for field, section in (('interval', 'whole_seconds'), ('rotation', 'whole_hours'), ('retention', 'whole_days'))
        for case in FIXTURE[section]['refused']
    ]


@pytest.mark.parametrize('field, value, whole', _whole_type_cases())
def test_a_whole_number_type_holds_the_whole_number(field, value, whole):
    loaded = getattr(_Units(**{field: value}), field)

    assert loaded == whole
    assert type(loaded) is int


@pytest.mark.parametrize('field, value, message_key', _whole_type_refusals())
def test_a_whole_number_type_refuses_with_the_message_under_the_field_path(field, value, message_key):
    with pytest.raises(ValidationError) as excinfo:
        _Units(**{field: value})

    assert field in str(excinfo.value)
    assert _expected(message_key, _value_text(value)) in str(excinfo.value)


def test_a_duration_holds_float_seconds():
    assert _Units(timeout='1.5s').timeout == 1.5
    assert _Units(timeout='500ms').timeout == 0.5
    assert _Units(timeout=0).timeout == 0.0
    assert type(_Units(timeout='2m').timeout) is float


def test_a_byte_size_holds_whole_bytes():
    assert _Units(max_size='64KiB').max_size == 65536
    assert _Units(max_size=0).max_size == 0


def test_an_optional_duration_takes_null_and_text():
    assert _Units(optional_timeout=None).optional_timeout is None
    assert _Units(optional_timeout='1m').optional_timeout == 60.0


def test_an_optional_duration_refuses_with_one_clean_message():
    with pytest.raises(ValidationError) as excinfo:
        _Units(optional_timeout=30)

    assert excinfo.value.error_count() == 1
    assert _expected('duration_needs_unit', '30') in str(excinfo.value)


@pytest.mark.parametrize('mode', ['python', 'json'])
def test_a_dump_writes_the_canonical_text_in_both_modes(mode):
    loaded = _Units(
        timeout='90s',
        optional_timeout='1500ms',
        interval='2m',
        rotation='48h',
        retention='30d',
        max_size='256MiB',
        durations=['1h'],
    )

    assert loaded.model_dump(mode=mode) == {
        'timeout': '1m30s',
        'optional_timeout': '1s500ms',
        'interval': '2m',
        'rotation': '2d',
        'retention': '30d',
        'max_size': '256MiB',
        'durations': ['1h'],
    }


def test_a_dump_loads_back_to_the_same_values():
    loaded = _Units(timeout='1.001s', optional_timeout='1h', interval='0', max_size='1500B')

    assert _Units(**loaded.model_dump()) == loaded


def test_a_dump_keeps_null_as_null():
    assert _Units().model_dump()['optional_timeout'] is None


def test_the_metadata_names_the_unit_types_and_writes_canonical_defaults():
    entries = {entry.path: entry for entry in walk_model_fields(_Units, path_prefix=['section'], group='section')}

    assert {path.removeprefix('section.'): entry.type for path, entry in entries.items()} == {
        'timeout': 'duration',
        'optional_timeout': 'duration',
        'interval': 'duration',
        'rotation': 'duration',
        'retention': 'duration',
        'max_size': 'size',
        'durations': 'array',
    }
    assert entries['section.timeout'].default == '5s'
    assert entries['section.optional_timeout'].default is None
    assert entries['section.interval'].default == '30s'
    assert entries['section.rotation'].default == '2h'
    assert entries['section.retention'].default == '30d'
    assert entries['section.max_size'].default == '1KiB'
    assert entries['section.durations'].default == ['1m', '5m']


def test_a_metadata_default_equals_the_dump_of_an_untouched_model():
    dumped = _Units().model_dump(mode='python')

    for entry in walk_model_fields(_Units, path_prefix=[], group='section'):
        assert entry.default == dumped[entry.path]
