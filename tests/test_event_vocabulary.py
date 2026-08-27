"""The recorder's event-type vocabulary is one enum, pinned by a fixture.

Event names are a wire contract: they are written into ``events.event``
and the UI matches on them, so a rename breaks consumers rather than
refactoring them. Before this, each name was a string literal at its
``record_*`` site and at every consumer comparison, so a typo was invisible
to the type checker and to the tests.

``tests/fixtures/event_vocabulary.json`` is the reviewed list the enum is
pinned against — regenerate it with ``just gen-event-vocabulary``.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from drakkar.recorder.schema import EventType

FIXTURE = Path(__file__).parent / 'fixtures' / 'event_vocabulary.json'


def test_vocabulary_matches_the_shared_fixture() -> None:
    """The enum is the source of truth; the fixture is its vendored form."""
    fixture = json.loads(FIXTURE.read_text())
    assert fixture['events'] == sorted(member.value for member in EventType)


def test_docs_table_documents_every_stored_event() -> None:
    """The observability page's event table is part of the vocabulary contract.

    ``throughput`` is deliberately absent: it is broadcast on ``/ws`` and
    never written to ``events.event``, so it has no row in a table that
    describes that column.
    """
    import re

    doc = (REPO_ROOT / 'docs' / 'observability.md').read_text()
    start = doc.index('**Event types:**')
    table = doc[start : doc.index('For HTTP-origin rows', start)]
    documented = set(re.findall(r'^\| `([a-z_]+)` \|', table, re.M))

    fixture = json.loads(FIXTURE.read_text())
    stored = {name for name, is_stored in fixture['stored_in_events'].items() if is_stored}
    assert documented == stored, (
        f'undocumented: {sorted(stored - documented)}; documented but not stored: {sorted(documented - stored)}'
    )


def test_ws_only_events_are_never_buffered() -> None:
    """A WS-only name must not reach the events table."""
    fixture = json.loads(FIXTURE.read_text())
    ws_only = {name for name, is_stored in fixture['stored_in_events'].items() if not is_stored}
    assert ws_only == {EventType.THROUGHPUT.value}


def test_every_member_value_equals_its_lowercased_name() -> None:
    """Keeps the enum mechanically checkable against the wire strings."""
    for member in EventType:
        assert member.value == member.name.lower()


VOCABULARY = frozenset(member.value for member in EventType)

# Every module that writes an event row or compares one. Scanned as a list
# so adding a recorder module to the codebase means adding a row here, not
# discovering months later that its literals were never checked.
SCANNED_MODULES = (
    'drakkar/recorder/writer.py',
    'drakkar/recorder/core.py',
    'drakkar/recorder/queries.py',
    'drakkar/recorder/fanout.py',
    'drakkar/uiserver/routes_debug.py',
    'drakkar/uiserver/routes_live.py',
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _event_dict_literals(tree: ast.AST) -> list[str]:
    """Every ``{'event': '<literal>'}`` in ``tree`` — an unenumerated write."""
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values, strict=True):
            is_event_key = isinstance(key, ast.Constant) and key.value == 'event'
            if is_event_key and isinstance(value, ast.Constant):
                offenders.append(f'line {value.lineno}: {value.value!r}')
    return offenders


def _event_name_comparisons(tree: ast.AST) -> list[str]:
    """Every ``... == '<event name>'`` / ``... in ('a', 'b')`` against a literal."""
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        for comparator in node.comparators:
            candidates: list[ast.expr] = [comparator]
            if isinstance(comparator, ast.Tuple | ast.List | ast.Set):
                candidates = list(comparator.elts)
            for candidate in candidates:
                if isinstance(candidate, ast.Constant) and candidate.value in VOCABULARY:
                    offenders.append(f'line {candidate.lineno}: {candidate.value!r}')
    return offenders


@pytest.mark.parametrize('module', SCANNED_MODULES)
def test_module_names_events_through_the_enum(module: str) -> None:
    """Recorder event names must never appear as bare literals.

    An AST scan rather than a call-through test: it catches a literal in a
    branch no test happens to exercise, which is exactly the case the
    string-literal vocabulary made easy to get wrong.
    """
    tree = ast.parse((REPO_ROOT / module).read_text())
    offenders = _event_dict_literals(tree) + _event_name_comparisons(tree)
    assert not offenders, f'bare event-name literals in {module}: ' + ', '.join(offenders)


def test_the_scan_would_actually_catch_a_literal() -> None:
    """Guards the guard — a scan that matches nothing proves nothing."""
    tree = ast.parse("row = {'event': 'task_started'}\nif row['event'] == 'task_failed':\n    pass\n")
    assert _event_dict_literals(tree)
    assert _event_name_comparisons(tree)


@pytest.mark.parametrize(
    'member',
    [EventType.TASK_STARTED, EventType.TASK_COMPLETED, EventType.WEBAPP_REQUEST_RECEIVED],
)
def test_members_compare_and_serialize_as_plain_strings(member: EventType) -> None:
    """``StrEnum`` keeps rows, SQL binding and JSON output byte-identical."""
    assert member == member.value
    assert json.dumps({'event': member}) == json.dumps({'event': member.value})
