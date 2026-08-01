"""Unit tests for pure Postgres SQL construction.

No database, no mocks, no asyncpg — every function under test is a string
transformation, which is the whole point of keeping them in a module that
imports nothing from ``drakkar``.
"""

import pytest

from drakkar.pgsql import MAX_INSERT_PARAMS, quote_ident, render_insert


def test_quote_ident_wraps_valid_name():
    assert quote_ident('valid_name') == '"valid_name"'


@pytest.mark.parametrize(
    'bad',
    [
        'col; DROP TABLE x',
        'has space',
        '1leading_digit',
        '',
        'quote"inside',
    ],
)
def test_quote_ident_rejects_suspicious(bad):
    with pytest.raises(ValueError, match='Invalid SQL identifier'):
        quote_ident(bad)


def test_render_insert_single_row():
    sql = render_insert('"results"', ['"id"', '"status"'], 1)
    assert sql == 'INSERT INTO "results" ("id", "status") VALUES ($1, $2)'


def test_render_insert_numbers_params_continuously_across_rows():
    sql = render_insert('"results"', ['"id"', '"status"'], 3)
    assert sql == ('INSERT INTO "results" ("id", "status") VALUES ($1, $2), ($3, $4), ($5, $6)')


def test_render_insert_single_column():
    sql = render_insert('"seen"', ['"key"'], 2)
    assert sql == 'INSERT INTO "seen" ("key") VALUES ($1), ($2)'


def test_max_insert_params_matches_wire_protocol_limit():
    assert MAX_INSERT_PARAMS == 65535
