"""Unit tests for drakkar.mql — MQL template compilation and binding.

Pure functions, so no database, no mocks, and no sink. The vectors that must
stay identical to the Go backend live in tests/fixtures/mongo_statements.json
and are driven separately; these tests cover the rules themselves.
"""

import json
from pathlib import Path

import pytest

from drakkar.mql import compile_template, substitute


def _compiled(document):
    return compile_template(document)


# --- parameter collection ---------------------------------------------------


def test_compile_collects_distinct_names_in_first_appearance_order():
    compiled = _compiled({'b': ':second', 'a': ':first', 'c': ':second'})

    assert compiled.params == ('second', 'first')


def test_compile_of_a_template_without_placeholders_binds_nothing():
    compiled = _compiled({'status': 'done', 'attempts': 1})

    assert compiled.params == ()
    assert substitute(compiled, {}) == {'status': 'done', 'attempts': 1}


# --- rule 1: whole-value substitution, type preserved -----------------------


@pytest.mark.parametrize(
    'value',
    [
        'a string',
        5,
        0,
        -3,
        1.5,
        True,
        False,
        None,
        {'nested': 'document'},
        ['a', 'list'],
    ],
)
def test_substitute_preserves_the_bound_value_type(value):
    """An MQL comparison against "5" does not match a numeric field 5.

    Type preservation is the rule most likely to be broken by a naive
    string-templating implementation, and it fails silently: the query is
    well-formed and simply matches nothing.
    """
    compiled = _compiled({'count': ':count'})

    assert substitute(compiled, {'count': value}) == {'count': value}


def test_substitute_fills_every_position_a_repeated_name_appears_in():
    compiled = _compiled({'filter': {'id': ':id'}, 'update': {'$set': {'ref': ':id'}}})

    assert substitute(compiled, {'id': 42}) == {'filter': {'id': 42}, 'update': {'$set': {'ref': 42}}}


# --- rule 2: whole values only ----------------------------------------------


def test_substitute_leaves_a_placeholder_inside_a_longer_string_alone():
    """No interpolation, so a parameter can never splice into an expression."""
    compiled = _compiled({'note': 'job-:id'})

    assert compiled.params == ()
    assert substitute(compiled, {}) == {'note': 'job-:id'}


def test_a_colon_that_does_not_start_the_value_is_an_ordinary_string():
    compiled = _compiled({'at': '12:30'})

    assert compiled.params == ()
    assert substitute(compiled, {}) == {'at': '12:30'}


# --- rule 3: keys are never substitutable -----------------------------------


def test_a_placeholder_shaped_key_is_rejected():
    """Keys stay literal, so an operator writing one meant something else.

    Never substituting keys is what stops a parameter introducing $where or
    $out. Rejecting the shape turns a silent surprise — a query against a
    field literally named ':field' — into a startup error.
    """
    with pytest.raises(ValueError, match='key'):
        _compiled({':field': 1})


def test_a_placeholder_shaped_key_is_rejected_at_depth():
    with pytest.raises(ValueError, match='key'):
        _compiled({'$set': {'nested': {':field': 1}}})


# --- rule 4: the "::name" escape --------------------------------------------


def test_double_colon_escapes_a_literal_leading_colon():
    compiled = _compiled({'label': '::status'})

    assert compiled.params == ()
    assert substitute(compiled, {}) == {'label': ':status'}


def test_an_escaped_value_is_not_a_placeholder_even_with_a_bound_name():
    compiled = _compiled({'a': '::id', 'b': ':id'})

    assert compiled.params == ('id',)
    assert substitute(compiled, {'id': 7}) == {'a': ':id', 'b': 7}


# --- nesting ----------------------------------------------------------------


def test_substitute_reaches_into_nested_documents_and_arrays():
    compiled = _compiled(
        {
            '$set': {'status': ':status', 'tags': ['fixed', ':tag', {'deep': ':deep'}]},
            '$inc': {'attempts': 1},
        }
    )

    assert compiled.params == ('status', 'tag', 'deep')
    assert substitute(compiled, {'status': 'done', 'tag': 'x', 'deep': 9}) == {
        '$set': {'status': 'done', 'tags': ['fixed', 'x', {'deep': 9}]},
        '$inc': {'attempts': 1},
    }


def test_a_pipeline_form_update_is_a_list_at_the_top_level():
    """MongoDB's own mechanism for computed updates, so it must compile."""
    compiled = _compiled([{'$set': {'total': ':total'}}, {'$unset': 'stale'}])

    assert compiled.params == ('total',)
    assert substitute(compiled, {'total': 12}) == [{'$set': {'total': 12}}, {'$unset': 'stale'}]


# --- the compiled template is reusable --------------------------------------


def test_substitute_never_mutates_the_compiled_template():
    """Compiled once at connect(), used for every delivery afterwards."""
    compiled = _compiled({'$set': {'at': ':now', 'tags': [':tag']}})

    first = substitute(compiled, {'now': 1, 'tag': 'a'})
    second = substitute(compiled, {'now': 2, 'tag': 'b'})

    assert first == {'$set': {'at': 1, 'tags': ['a']}}
    assert second == {'$set': {'at': 2, 'tags': ['b']}}


def test_substitute_returns_a_document_the_caller_may_mutate():
    compiled = _compiled({'$set': {'at': ':now'}})

    first = substitute(compiled, {'now': 1})
    first['$set']['at'] = 'clobbered'

    assert substitute(compiled, {'now': 2}) == {'$set': {'at': 2}}


# --- malformed placeholders -------------------------------------------------


@pytest.mark.parametrize('value', [':', ':1abc', ':-', ': name'])
def test_a_malformed_placeholder_is_rejected(value):
    """A value starting with a colon is either a parameter or an escape.

    Anything else is a typo, and the message points at the '::' escape for
    the case where a literal leading colon was meant.
    """
    with pytest.raises(ValueError, match='placeholder'):
        _compiled({'field': value})


def test_a_malformed_placeholder_is_rejected_inside_a_list():
    with pytest.raises(ValueError, match='placeholder'):
        _compiled({'$set': {'tags': ['ok', ':']}})


# --- server-side JavaScript -------------------------------------------------


@pytest.mark.parametrize('operator', ['$where', '$function'])
def test_server_side_javascript_operators_are_rejected(operator):
    """Both execute JavaScript on the server; neither belongs in a sink write."""
    with pytest.raises(ValueError, match=operator.replace('$', r'\$')):
        _compiled({operator: 'this.qty < 10'})


@pytest.mark.parametrize('operator', ['$where', '$function'])
def test_server_side_javascript_is_rejected_at_any_depth(operator):
    with pytest.raises(ValueError, match=r'\$'):
        _compiled({'$and': [{'ok': 1}, {'nested': {operator: 'code'}}]})


def test_server_side_javascript_is_rejected_inside_a_pipeline_stage():
    """A pipeline stage is a document like any other, and gets walked too."""
    with pytest.raises(ValueError, match=r'\$function'):
        _compiled([{'$set': {'x': {'$function': {'body': 'f'}}}}])


# --- binding errors ---------------------------------------------------------


def test_substitute_rejects_a_missing_parameter():
    compiled = _compiled({'id': ':id', 'at': ':now'})

    with pytest.raises(ValueError, match='now'):
        substitute(compiled, {'id': 1})


def test_substitute_rejects_an_extra_parameter():
    """A silently ignored key is almost always a typo in the payload model."""
    compiled = _compiled({'id': ':id'})

    with pytest.raises(ValueError, match='typo'):
        substitute(compiled, {'id': 1, 'nope': 2})


def test_binding_errors_name_every_offending_key_sorted():
    compiled = _compiled({'a': ':alpha', 'b': ':beta'})

    with pytest.raises(ValueError) as excinfo:
        substitute(compiled, {})

    assert "'alpha', 'beta'" in str(excinfo.value)


def test_a_non_string_key_is_rejected():
    """BSON keys are strings, so a YAML mapping keyed by an integer could
    never reach Mongo — failing at config load beats a driver error later."""
    with pytest.raises(ValueError, match='keys must be strings'):
        _compiled({1: 'x'})


# --- golden vectors, shared with drakkar-go ---------------------------------
#
# tests/fixtures/mongo_statements.json is mirrored verbatim into the Go repo.
# The cases run through THIS module's own compile and substitute functions —
# not a test-local reimplementation — so what is pinned is the substitution
# that ships. A divergence between the two backends fails here instead of
# reaching an operator's database.

_MONGO_STATEMENT_CORPUS = json.loads((Path(__file__).parent / 'fixtures' / 'mongo_statements.json').read_text())


@pytest.mark.parametrize('case', _MONGO_STATEMENT_CORPUS['ok'], ids=lambda c: c['case'])
def test_mongo_statement_corpus_binds(case):
    """Both backends must produce this document from this template and params."""
    compiled = compile_template(case['template'])

    assert list(compiled.params) == case['names']
    assert substitute(compiled, case['params']) == case['document']


@pytest.mark.parametrize('case', _MONGO_STATEMENT_CORPUS['errors'], ids=lambda c: c['case'])
def test_mongo_statement_corpus_rejects(case):
    """Both backends must refuse these, compiling or binding."""
    with pytest.raises(ValueError) as excinfo:
        compiled = compile_template(case['template'])
        # A vector carrying params is a BIND-time rejection: compiling it
        # has to succeed first, or the vector would pass for the wrong
        # reason.
        substitute(compiled, case.get('params', {}))

    # Case-insensitive: the corpus pins WHICH failure a vector produces, not
    # its prose, because Go requires lowercase error strings and Python
    # capitalises. The same allowance the shared Postgres corpus makes.
    assert case['error'].lower() in str(excinfo.value).lower()


def test_mongo_statement_corpus_error_vectors_fail_where_they_claim():
    """A bind-time vector must COMPILE cleanly, or it proves nothing.

    Without this, a typo in a params-carrying vector's template would make
    it pass at compile time and never exercise binding at all — the vacuous
    vector the Postgres and Redis corpora both shipped before mutation
    testing found them.
    """
    for case in _MONGO_STATEMENT_CORPUS['errors']:
        if 'params' not in case:
            continue
        compile_template(case['template'])
