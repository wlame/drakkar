"""Durations and sizes written with units in config values.

Config values carry their unit in the text (``30s``, ``2h45m``, ``64KiB``), while
the models hold plain seconds and bytes, so consumers read a number. The parsers
do exact arithmetic on the digits, so ``1.1s`` is 1100 milliseconds. The accepted
texts, the refusals and their messages, and the canonical forms are a contract
pinned by ``tests/fixtures/config_units.json``.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from fractions import Fraction
from typing import Annotated, Any

from pydantic import BeforeValidator, PlainSerializer

ZERO_TEXT = '0'

_NUMBER = r'[0-9]+(?:\.[0-9]+)?'
_PLAIN_NUMBER_RE = re.compile(rf'[+-]?{_NUMBER}')
# Keeps the digits below the length limit of Python's text-to-int conversion;
# any number this long is out of range or not whole anyway.
_MAX_TEXT_LENGTH = 4000

_MS_PER_SECOND = 1000
_MS_PER_HOUR = 3_600_000
_MS_PER_DAY = 86_400_000
_DURATION_UNITS: tuple[tuple[str, int], ...] = (
    ('d', _MS_PER_DAY),
    ('h', _MS_PER_HOUR),
    ('m', 60_000),
    ('s', _MS_PER_SECOND),
    ('ms', 1),
)
_DURATION_RE = re.compile(''.join(f'(?:(?P<{unit}>{_NUMBER}){unit})?' for unit, _ in _DURATION_UNITS))
# 292 years of 365 days, exclusive.
_DURATION_LIMIT_MS = 292 * 365 * _MS_PER_DAY

_BINARY_UNITS: tuple[tuple[str, int], ...] = (('TiB', 1024**4), ('GiB', 1024**3), ('MiB', 1024**2), ('KiB', 1024))
_DECIMAL_UNITS: tuple[tuple[str, int], ...] = (('TB', 1000**4), ('GB', 1000**3), ('MB', 1000**2), ('KB', 1000))
_SIZE_UNITS: dict[str, int] = {**dict(_BINARY_UNITS), **dict(_DECIMAL_UNITS), 'B': 1}
_SIZE_RE = re.compile(rf'(?P<number>{_NUMBER})(?P<unit>{"|".join(_SIZE_UNITS)})')
_SIZE_LIMIT = 2**63 - 1


@dataclass(frozen=True)
class _Refusals:
    """The two refusal texts of one kind of value; ``{v}`` is the value as written."""

    needs_unit: str
    invalid: str


_DURATION_REFUSALS = _Refusals(
    needs_unit="needs a unit, for example 30s; got '{v}'",
    invalid="is not a duration, got '{v}'; write for example 30s or 2h45m",
)
_SIZE_REFUSALS = _Refusals(
    needs_unit="needs a unit, for example 64KiB; got '{v}'",
    invalid="is not a size, got '{v}'; write for example 64KiB or 256MB",
)


def _value_text(value: object) -> str:
    """A raw value as a message quotes it, with YAML's spelling of the words."""
    if isinstance(value, bool):
        return str(value).lower()
    return 'null' if value is None else str(value)


def _refused(template: str, value: object) -> ValueError:
    return ValueError(template.replace('{v}', _value_text(value)))


def _checked_text(value: object, refusals: _Refusals) -> str:
    """The text to parse: a whole ``0`` of either type is ``'0'``, anything else must be text with a unit."""
    if isinstance(value, bool) or value is None:
        raise _refused(refusals.invalid, value)
    if isinstance(value, int | float):
        if isinstance(value, int) and value == 0:
            return ZERO_TEXT
        raise _refused(refusals.needs_unit, value)
    if not isinstance(value, str) or len(value) > _MAX_TEXT_LENGTH:
        raise _refused(refusals.invalid, value)
    if value != ZERO_TEXT and _PLAIN_NUMBER_RE.fullmatch(value):
        raise _refused(refusals.needs_unit, value)
    return value


def _scaled(number: str, factor: int) -> Fraction:
    """``number`` times ``factor``, exactly."""
    whole, _, fraction = number.partition('.')
    return Fraction(int(whole + fraction) * factor, 10 ** len(fraction))


def parse_duration(text: str | int | float) -> int:
    """The milliseconds of a duration text.

    Raises:
        ValueError: with the contract message; the caller adds the config path.
    """
    checked = _checked_text(text, _DURATION_REFUSALS)
    if checked == ZERO_TEXT:
        return 0
    match = _DURATION_RE.fullmatch(checked)
    if match is None or not checked:
        raise _refused(_DURATION_REFUSALS.invalid, text)
    # The whole-milliseconds rule applies to the total, not to each term.
    total = sum(
        (_scaled(number, factor) for unit, factor in _DURATION_UNITS if (number := match.group(unit)) is not None),
        start=Fraction(0),
    )
    if total.denominator != 1 or total >= _DURATION_LIMIT_MS:
        raise _refused(_DURATION_REFUSALS.invalid, text)
    return int(total)


def parse_size(text: str | int | float) -> int:
    """The bytes of a size text.

    Raises:
        ValueError: with the contract message; the caller adds the config path.
    """
    checked = _checked_text(text, _SIZE_REFUSALS)
    if checked == ZERO_TEXT:
        return 0
    match = _SIZE_RE.fullmatch(checked)
    if match is None:
        raise _refused(_SIZE_REFUSALS.invalid, text)
    size = _scaled(match['number'], _SIZE_UNITS[match['unit']])
    if size.denominator != 1 or size > _SIZE_LIMIT:
        raise _refused(_SIZE_REFUSALS.invalid, text)
    return int(size)


def format_duration(milliseconds: int) -> str:
    """The canonical text of a duration: descending terms, no zero term (``2h45m``, ``500ms``, ``0``)."""
    if milliseconds < 0:
        raise ValueError(f'a duration cannot be negative, got {milliseconds} ms')
    terms: list[str] = []
    remaining = milliseconds
    for unit, factor in _DURATION_UNITS:
        count, remaining = divmod(remaining, factor)
        if count:
            terms.append(f'{count}{unit}')
    return ''.join(terms) or ZERO_TEXT


def format_size(size: int) -> str:
    """The canonical text of a size: the unit that divides it and gives the smallest whole number.

    ``256000000`` is ``256MB`` and ``268435456`` is ``256MiB``. On a tie the binary unit wins.
    """
    if size < 0:
        raise ValueError(f'a size cannot be negative, got {size} bytes')
    if size == 0:
        return ZERO_TEXT
    # min() keeps the first of equal counts, and the binary units come first.
    unit, factor = min(
        ((unit, factor) for unit, factor in _SIZE_UNITS.items() if size % factor == 0),
        key=lambda candidate: size // candidate[1],
    )
    return f'{size // factor}{unit}'


@dataclass(frozen=True)
class UnitKind:
    """Marks an annotated config type for the config metadata.

    ``type_name`` is the metadata's name of the type; ``to_text`` writes a value
    the model holds as its canonical text.
    """

    type_name: str
    to_text: Callable[[Any], str]


DURATION_TYPE = 'duration'
SIZE_TYPE = 'size'


def _seconds_from_text(value: Any) -> float:
    return parse_duration(value) / _MS_PER_SECOND


def _text_from_seconds(seconds: float) -> str:
    return format_duration(round(seconds * _MS_PER_SECOND))


def _whole_duration_type(unit_name: str, unit_ms: int) -> Any:
    """An annotated ``int`` holding a whole number of ``unit_name``, written and dumped as a duration."""

    def from_text(value: Any) -> int:
        milliseconds = parse_duration(value)
        if milliseconds % unit_ms:
            raise _refused(f"must be a whole number of {unit_name}, got '{{v}}'", value)
        return milliseconds // unit_ms

    def to_text(count: int) -> str:
        return format_duration(count * unit_ms)

    return Annotated[
        int,
        BeforeValidator(from_text),
        PlainSerializer(to_text, return_type=str),
        UnitKind(type_name=DURATION_TYPE, to_text=to_text),
    ]


Duration = Annotated[
    float,
    BeforeValidator(_seconds_from_text),
    PlainSerializer(_text_from_seconds, return_type=str),
    UnitKind(type_name=DURATION_TYPE, to_text=_text_from_seconds),
]
"""A duration held as float seconds."""

WholeSeconds = _whole_duration_type('seconds', _MS_PER_SECOND)
"""A duration held as a whole number of seconds."""

WholeHours = _whole_duration_type('hours', _MS_PER_HOUR)
"""A duration held as a whole number of hours."""

WholeDays = _whole_duration_type('days', _MS_PER_DAY)
"""A duration held as a whole number of days."""

ByteSize = Annotated[
    int,
    BeforeValidator(parse_size),
    PlainSerializer(format_size, return_type=str),
    UnitKind(type_name=SIZE_TYPE, to_text=format_size),
]
"""A size held as whole bytes."""
