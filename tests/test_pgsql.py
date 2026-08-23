"""Unit tests for pure Postgres SQL construction.

No database, no mocks, no asyncpg — every function under test is a string
transformation, which is the whole point of keeping them in a module that
imports nothing from ``drakkar``.
"""

import json
from pathlib import Path

import pytest

from drakkar.pgsql import (
    MAX_INSERT_PARAMS,
    compile_named_statement,
    quote_ident,
    render_insert,
    render_update,
    render_upsert,
)

# Shared with drakkar-go: the same file is mirrored there and drives the same
# assertions, so a divergence between the two tokenizers fails a test rather
# than reaching an operator's SQL.
_CORPUS = json.loads((Path(__file__).parent / 'fixtures' / 'pg_statements.json').read_text())


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


def test_max_insert_params_matches_the_driver_limit():
    """asyncpg refuses a prepared statement with more than 32767 arguments
    (``InterfaceError``), well below the Postgres wire protocol's own 65535.
    The cap has to be the driver's, not the protocol's, or the sink builds
    statements the driver will not send — and the sink's per-row fallback
    then quietly turns every oversized batch into hundreds of single-row
    inserts. This is a deliberate divergence from the Go backend, whose pgx
    v5 driver does allow the full 65535.
    """
    assert MAX_INSERT_PARAMS == 32767


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


@pytest.mark.parametrize('case', _CORPUS['ok'], ids=lambda c: c['case'])
def test_compile_named_statement_corpus(case):
    """The corpus is shared with drakkar-go — both backends must agree."""
    sql, params = compile_named_statement(case['sql'])
    assert sql == case['expected_sql']
    assert params == case['params']


@pytest.mark.parametrize('case', _CORPUS['errors'], ids=lambda c: c['case'])
def test_compile_named_statement_corpus_errors(case):
    with pytest.raises(ValueError, match=case['error']):
        compile_named_statement(case['sql'])


def test_compile_named_statement_param_order_is_first_appearance():
    _, params = compile_named_statement('UPDATE t SET z = :zed, a = :ay WHERE k = :zed')
    assert params == ['zed', 'ay']


def test_compile_named_statement_lone_dollar_is_copied_verbatim():
    """A `$` that opens no dollar-quote tag is ordinary text, not an error."""
    sql, params = compile_named_statement('UPDATE t SET a = :v WHERE cost > 5$')
    assert sql == 'UPDATE t SET a = $1 WHERE cost > 5$'
    assert params == ['v']
