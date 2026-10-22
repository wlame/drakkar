"""The accepted cron subset: a syntax check of our own and the next fire time from the library.

A cron expression has five fields separated by exactly one space: minute,
hour, day of month, month and day of week, all in UTC. Each field is a comma
list; each element is ``*``, ``*/s``, ``v``, ``a-b`` with ``a < b``, or
``a-b/s``. ``v``, ``a`` and ``b`` are decimal numbers without sign or leading
zero, or (month and day of week only) a three-letter English name in any
letter case; ``s`` is from 1 to the field's maximum minus its minimum. The day
of month and the day of week must not both differ from ``*``.

The subset is defined independently of any cron library. Inside it, cron
libraries agree on every fire time; outside it they read the same text in
different ways, so a fleet whose workers are built on different libraries
would split. The check therefore refuses everything else (seconds and year
fields, ``@`` macros, ``L``, ``W``, ``#``, ``?``, wrap ranges, ``a-a``,
``n/s``, empty elements, full names), and its messages are ours, never a
library's. ``tests/fixtures/periodic_contract.json`` pins the verdicts, the
messages and the fire times.

Load order: :func:`check_cron_syntax`, then one :func:`next_cron_fire` call
from the current time, which refuses an expression that never fires (such as
``0 0 30 2 *``). :func:`validate_cron` does both.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from cronsim import CronSim, CronSimError

CRON_FIELD_NAMES: tuple[str, ...] = ('minute', 'hour', 'day of month', 'month', 'day of week')

CRON_FIELDS_MESSAGE = "cron '{expr}' must have 5 fields separated by single spaces"
CRON_DAY_RULE_MESSAGE = "cron '{expr}' restricts both the day of month and the day of week; make one of them *"
CRON_ELEMENT_MESSAGE = "cron '{expr}': {field} element '{item}' is not one of *, */s, v, a-b, a-b/s"
CRON_VALUE_MESSAGE = "cron '{expr}': {field} value '{token}' is not one of {allowed}"
CRON_RANGE_MESSAGE = "cron '{expr}': {field} range '{item}' must go from a lower to a higher value"
CRON_STEP_MESSAGE = "cron '{expr}': {field} step '{step}' must be from 1 to {max_step}"
CRON_NEVER_MESSAGE = "cron '{expr}' never fires"

_DAY_OF_MONTH_INDEX = 2
_DAY_OF_WEEK_INDEX = 4
# The longest number any field can need; a longer digit run is out of range
# without converting it (``int()`` refuses strings of thousands of digits).
_MAX_NUMBER_DIGITS = 4

_MONTH_NAMES = ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec')
_WEEKDAY_NAMES = ('sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat')

_NUMBER = r'(?:0|[1-9][0-9]*)'
_VALUE = rf'(?:{_NUMBER}|[A-Za-z]{{3}})'
# One list element: ``*``, ``*/s``, ``v``, ``a-b`` or ``a-b/s``. Always used
# with ``fullmatch``: ``match`` with ``$`` would accept a trailing newline.
_ELEMENT = re.compile(
    rf'\*(?:/(?P<star_step>{_NUMBER}))?|(?P<single>{_VALUE})|(?P<start>{_VALUE})-(?P<end>{_VALUE})(?:/(?P<step>{_NUMBER}))?'
)


@dataclass(frozen=True)
class _Field:
    """The bounds of one cron field, its names (numbered from ``lowest``) and its message texts."""

    name: str
    lowest: int
    highest: int
    names: tuple[str, ...]
    allowed: str

    @property
    def max_step(self) -> int:
        """The largest step the field accepts: its maximum minus its minimum."""
        return self.highest - self.lowest


_FIELDS = (
    _Field('minute', 0, 59, (), '0-59'),
    _Field('hour', 0, 23, (), '0-23'),
    _Field('day of month', 1, 31, (), '1-31'),
    _Field('month', 1, 12, _MONTH_NAMES, '1-12 or JAN-DEC'),
    _Field('day of week', 0, 6, _WEEKDAY_NAMES, '0-6 or SUN-SAT'),
)


class CronNeverFiresError(ValueError):
    """The cron library found no fire time after the given moment."""


def _to_number(token: str, field: _Field) -> int | None:
    """Return the number a value token stands for, or ``None`` when it is unknown or out of range."""
    if token.isdigit():
        if len(token) > _MAX_NUMBER_DIGITS:
            return None
        number = int(token)
    elif token.lower() in field.names:
        number = field.names.index(token.lower()) + field.lowest
    else:
        return None
    return number if field.lowest <= number <= field.highest else None


def _step_is_valid(step: str, field: _Field) -> bool:
    """Whether a step number is from 1 to the field's maximum step."""
    return len(step) <= _MAX_NUMBER_DIGITS and 1 <= int(step) <= field.max_step


def _check_element(expr: str, item: str, field: _Field) -> str | None:
    """Check one list element: shape, values, range order, then step. Return the message or ``None``."""
    match = _ELEMENT.fullmatch(item)
    if match is None:
        return CRON_ELEMENT_MESSAGE.format(expr=expr, field=field.name, item=item)
    numbers: list[int] = []
    for token in (match['single'], match['start'], match['end']):
        if token is None:
            continue
        number = _to_number(token, field)
        if number is None:
            return CRON_VALUE_MESSAGE.format(expr=expr, field=field.name, token=token, allowed=field.allowed)
        numbers.append(number)
    if match['start'] is not None and numbers[0] >= numbers[1]:
        return CRON_RANGE_MESSAGE.format(expr=expr, field=field.name, item=item)
    step = match['star_step'] or match['step']
    if step is not None and not _step_is_valid(step, field):
        return CRON_STEP_MESSAGE.format(expr=expr, field=field.name, step=step, max_step=field.max_step)
    return None


def check_cron_syntax(expr: str) -> str | None:
    """Check ``expr`` against the accepted subset; return the first refusal message, or ``None``.

    Order: the field count (an empty field included), the day rule, then the
    fields left to right and the elements of each field left to right.
    """
    fields = expr.split(' ')
    if len(fields) != len(_FIELDS) or '' in fields:
        return CRON_FIELDS_MESSAGE.format(expr=expr)
    if fields[_DAY_OF_MONTH_INDEX] != '*' and fields[_DAY_OF_WEEK_INDEX] != '*':
        return CRON_DAY_RULE_MESSAGE.format(expr=expr)
    for text, field in zip(fields, _FIELDS, strict=True):
        for item in text.split(','):
            message = _check_element(expr, item, field)
            if message is not None:
                return message
    return None


def next_cron_fire(expr: str, after_epoch: int) -> int:
    """The first fire time of ``expr`` strictly after ``after_epoch`` (UTC epoch seconds).

    Raises :class:`CronNeverFiresError` when the library refuses the
    expression or finds no fire time (it gives up after 50 years).
    """
    start = datetime.fromtimestamp(after_epoch, UTC)
    try:
        fire = next(CronSim(expr, start))
    except (CronSimError, StopIteration, OverflowError, ValueError) as error:
        raise CronNeverFiresError(CRON_NEVER_MESSAGE.format(expr=expr)) from error
    return int(fire.timestamp())


def validate_cron(expr: str, *, now_epoch: int) -> str | None:
    """Check the syntax, then ask the library for one fire time after ``now_epoch``.

    Returns the first refusal message, or ``None`` when ``expr`` is accepted.
    """
    message = check_cron_syntax(expr)
    if message is not None:
        return message
    try:
        next_cron_fire(expr, now_epoch)
    except CronNeverFiresError as error:
        return str(error)
    return None
