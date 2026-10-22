"""The cron subset check and the fire-time lookup, beyond the shared fixture cases."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from drakkar.cronexpr import (
    CRON_ELEMENT_MESSAGE,
    CRON_FIELD_NAMES,
    CRON_FIELDS_MESSAGE,
    CRON_NEVER_MESSAGE,
    CronNeverFiresError,
    check_cron_syntax,
    next_cron_fire,
    validate_cron,
)

MIDNIGHT = int(datetime(2026, 10, 31, tzinfo=UTC).timestamp())


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        ('0 0 * * *\n', CRON_ELEMENT_MESSAGE.format(expr='0 0 * * *\n', field='day of week', item='*\n')),
        ('0 0 * * *\t', CRON_ELEMENT_MESSAGE.format(expr='0 0 * * *\t', field='day of week', item='*\t')),
        ('0\t0 * * *', CRON_FIELDS_MESSAGE.format(expr='0\t0 * * *')),
        ('0 0 * * * ', CRON_FIELDS_MESSAGE.format(expr='0 0 * * * ')),
        ('5\n 0 * * *', CRON_ELEMENT_MESSAGE.format(expr='5\n 0 * * *', field='minute', item='5\n')),
    ],
)
def test_check_cron_refuses_trailing_newline_and_tab(expr, expected):
    assert check_cron_syntax(expr) == expected


def test_check_cron_refuses_a_digit_run_too_long_to_convert():
    expr = f'{"9" * 5000} * * * *'

    assert check_cron_syntax(expr) == f"cron '{expr}': minute value '{'9' * 5000}' is not one of 0-59"


def test_check_cron_refuses_an_empty_list_element():
    assert check_cron_syntax('1,,2 * * * *') == CRON_ELEMENT_MESSAGE.format(
        expr='1,,2 * * * *', field='minute', item=''
    )


def test_check_cron_reports_the_first_bad_element_of_the_first_bad_field():
    assert check_cron_syntax('1,99,x 25 * * *') == "cron '1,99,x 25 * * *': minute value '99' is not one of 0-59"


def test_cron_field_names_are_in_field_order():
    assert CRON_FIELD_NAMES == ('minute', 'hour', 'day of month', 'month', 'day of week')


def test_next_cron_fire_is_strictly_after_an_exact_match():
    assert next_cron_fire('0 0 * * *', MIDNIGHT) == MIDNIGHT + 86_400


def test_validate_cron_reports_never_fires_for_february_30():
    assert check_cron_syntax('0 0 30 2 *') is None
    assert validate_cron('0 0 30 2 *', now_epoch=MIDNIGHT) == CRON_NEVER_MESSAGE.format(expr='0 0 30 2 *')


def test_next_cron_fire_raises_when_the_library_finds_no_time():
    with pytest.raises(CronNeverFiresError, match='never fires'):
        next_cron_fire('0 0 31 4 *', MIDNIGHT)


def test_next_cron_fire_raises_when_the_library_gives_up_searching(monkeypatch):
    # The library stops after 50 years without a match; no accepted expression
    # reaches that, so a stand-in iterator that is already exhausted takes its place.
    monkeypatch.setattr('drakkar.cronexpr.CronSim', lambda expr, start: iter(()))

    with pytest.raises(CronNeverFiresError, match='never fires'):
        next_cron_fire('* * * * *', MIDNIGHT)


def test_validate_cron_accepts_an_expression_that_fires():
    assert validate_cron('*/10 * * * *', now_epoch=MIDNIGHT) is None
