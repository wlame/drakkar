"""Unit tests for pure Postgres SQL construction.

No database, no mocks, no asyncpg — every function under test is a string
transformation, which is the whole point of keeping them in a module that
imports nothing from ``drakkar``.
"""

import pytest

from drakkar.pgsql import (
    MAX_INSERT_PARAMS,
    quote_ident,
    render_insert,
    render_update,
    render_upsert,
)


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


def test_render_update_sets_then_filters():
    sql = render_update('"jobs"', ['"status"', '"finished_at"'], ['"id"'], [])
    assert sql == 'UPDATE "jobs" SET "status" = $1, "finished_at" = $2 WHERE "id" = $3'


def test_render_update_renders_is_null_without_consuming_a_param():
    """A None predicate value must become IS NULL — `= NULL` is never true."""
    sql = render_update('"jobs"', ['"status"'], ['"id"'], ['"claimed_by"'])
    assert sql == 'UPDATE "jobs" SET "status" = $1 WHERE "id" = $2 AND "claimed_by" IS NULL'


def test_render_update_all_null_predicate():
    sql = render_update('"jobs"', ['"status"'], [], ['"claimed_by"'])
    assert sql == 'UPDATE "jobs" SET "status" = $1 WHERE "claimed_by" IS NULL'


def test_render_update_appends_null_predicates_after_equalities():
    """Clause order is deterministic regardless of the caller's mapping order."""
    sql = render_update('"jobs"', ['"status"'], ['"id"', '"owner"'], ['"a"', '"b"'])
    assert sql == ('UPDATE "jobs" SET "status" = $1 WHERE "id" = $2 AND "owner" = $3 AND "a" IS NULL AND "b" IS NULL')


def test_render_upsert_do_update():
    sql = render_upsert('"totals"', ['"day"', '"hits"'], 2, ['"day"'], ['"hits"'])
    assert sql == (
        'INSERT INTO "totals" ("day", "hits") VALUES ($1, $2), ($3, $4) '
        'ON CONFLICT ("day") DO UPDATE SET "hits" = EXCLUDED."hits"'
    )


def test_render_upsert_multi_column_conflict_and_update():
    sql = render_upsert('"m"', ['"a"', '"b"', '"c"'], 1, ['"a"', '"b"'], ['"c"'])
    assert sql == (
        'INSERT INTO "m" ("a", "b", "c") VALUES ($1, $2, $3) ON CONFLICT ("a", "b") DO UPDATE SET "c" = EXCLUDED."c"'
    )


def test_render_upsert_empty_update_columns_becomes_do_nothing():
    """Every data column is a conflict column — DO UPDATE SET would be invalid."""
    sql = render_upsert('"seen"', ['"key"'], 1, ['"key"'], [])
    assert sql == 'INSERT INTO "seen" ("key") VALUES ($1) ON CONFLICT ("key") DO NOTHING'
