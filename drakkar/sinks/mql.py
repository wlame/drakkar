"""Pure MQL template handling for the MongoDB sink.

Every function here transforms plain documents: no pymongo, no I/O, and no
imports from the ``drakkar`` package.

That last rule is load-bearing rather than stylistic, for the same reason
it is in ``pgsql.py``: ``drakkar.config`` needs these helpers to validate
operator-authored statements at config load, so an import back into the
package here would close a cycle. See the note in
``drakkar/sinks/__init__.py``.

Keeping the module dependency-free also means the whole substitution
mechanism is unit-testable with no database and no mocks.

MQL is data rather than a string, so the escape hatch is a template
DOCUMENT with ``":name"`` placeholders rather than a query language to
parse. Four rules define the whole surface, and they exist to make an
operator-authored template safe to fill with message content:

1. A string value **exactly equal** to ``":name"`` is replaced by the bound
   parameter, with its type preserved.
2. Substitution applies to whole values only — never part of a longer
   string — so a parameter can never splice into a larger expression.
3. Keys are never substitutable, so a parameter cannot introduce an
   operator position such as ``$where`` or ``$out``.
4. ``"::name"`` escapes a literal string beginning with a colon, mirroring
   the ``::`` cast rule in the Postgres tokenizer.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# Same rule as the Postgres tokenizer's ``_PARAM_NAME_RE``, so the two
# escape hatches agree on what a parameter name is.
_PARAM_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

# Operators that execute JavaScript on the server. Neither has a legitimate
# use in a sink write, and both are rejected at config load rather than at
# delivery so an operator learns at startup.
_JAVASCRIPT_OPERATORS = frozenset({'$where', '$function'})

# One step of a path into a document: a mapping key or a list index.
_Step = str | int
_Path = tuple[_Step, ...]


@dataclass(frozen=True)
class CompiledTemplate:
    """An operator-authored MQL document, ready to bind parameters into.

    ``document`` is the template with every ``"::"`` escape already
    resolved, so binding is pure assignment. ``plan`` is where each
    parameter goes — compiling the paths once at ``connect()`` means a
    delivery is a copy plus N assignments rather than another walk.
    """

    document: object
    params: tuple[str, ...]
    """Distinct parameter names, SORTED. The caller supplies exactly one
    value per name, however often it appears.

    Sorted rather than in first-appearance order because the Go backend
    decodes a template into a map with no insertion order to recover, so
    sorting is the only rule both backends can honour unconditionally —
    the same reasoning that sorts Postgres columns and Redis mapping
    arguments. The order has no effect on binding, which is keyed by name;
    it exists so the two backends can be compared."""
    plan: tuple[tuple[_Path, str], ...]
    """(path, parameter name) for every position to fill."""


def compile_template(document: object) -> CompiledTemplate:
    """Walk a template document once, collecting its parameters.

    Rejects, at config-load time rather than at delivery: a malformed
    placeholder, a key shaped like a placeholder, and ``$where`` or
    ``$function`` at any depth — including inside aggregation-pipeline
    stages, which are documents like any other.

    Aggregation-pipeline updates (a list rather than a mapping) compile
    normally. They are MongoDB's own mechanism for computed updates, and
    ``$out``/``$merge`` are not reachable from one.
    """
    plan: list[tuple[_Path, str]] = []
    order: list[str] = []
    seen: set[str] = set()
    resolved = _walk(document, (), plan, order, seen)
    return CompiledTemplate(document=resolved, params=tuple(sorted(order)), plan=tuple(plan))


def substitute(template: CompiledTemplate, params: Mapping[str, object]) -> object:
    """Bind ``params`` into a compiled template, returning a fresh document.

    The template is never mutated, and the returned document shares no
    mutable structure with it, so one compiled statement serves every
    delivery.

    A missing OR extra key is an error. Extra keys are rejected because a
    silently ignored one is almost always a typo in the payload model — the
    same reason the Postgres statement path rejects them.
    """
    wanted = set(template.params)
    supplied = set(params)
    if missing := sorted(wanted - supplied):
        raise ValueError(f'missing parameters: {_quoted(missing)}')
    if extra := sorted(supplied - wanted):
        raise ValueError(f'unexpected parameters: {_quoted(extra)} — likely a typo, no placeholder wants them')

    document = _copy(template.document)
    for path, name in template.plan:
        _assign(document, path, params[name])
    return document


def _quoted(names: list[str]) -> str:
    """Render names for an error message, sorted and quoted."""
    return ', '.join(repr(name) for name in names)


def _walk(
    node: object,
    path: _Path,
    plan: list[tuple[_Path, str]],
    order: list[str],
    seen: set[str],
) -> object:
    """Recursively resolve escapes and record where parameters go."""
    if isinstance(node, Mapping):
        resolved: dict[str, object] = {}
        for key, value in node.items():
            name = _check_key(key, path)
            resolved[name] = _walk(value, (*path, name), plan, order, seen)
        return resolved
    # str is a Sequence, so it must be excluded explicitly or every string
    # would be walked character by character.
    if isinstance(node, list | tuple):
        return [_walk(item, (*path, i), plan, order, seen) for i, item in enumerate(node)]
    if isinstance(node, str):
        return _resolve_string(node, path, plan, order, seen)
    return node


def _check_key(key: object, path: _Path) -> str:
    """Reject a JavaScript operator, or a key shaped like a placeholder.

    A non-string key is rejected too: BSON document keys are strings, so a
    YAML mapping keyed by an integer could not be sent to Mongo at all, and
    failing at config load beats a driver error at first delivery.
    """
    if not isinstance(key, str):
        raise ValueError(f'{key!r} at {_render_path(path)} is not a valid document key — keys must be strings')
    if key in _JAVASCRIPT_OPERATORS:
        raise ValueError(
            f'{key!r} at {_render_path((*path, key))} executes JavaScript on the server '
            'and is not allowed in a statement template'
        )
    if key.startswith(':'):
        raise ValueError(
            f'{key!r} at {_render_path(path)} is a key, and keys are never parameters — '
            'a placeholder may only stand in for a whole value'
        )
    return key


def _resolve_string(
    value: str,
    path: _Path,
    plan: list[tuple[_Path, str]],
    order: list[str],
    seen: set[str],
) -> str:
    """Classify a string value: escape, placeholder, or ordinary text."""
    if not value.startswith(':'):
        return value
    if value.startswith('::'):
        # Rule 4: the escape yields a literal leading colon.
        return value[1:]
    name = value[1:]
    if not _PARAM_NAME_RE.match(name):
        raise ValueError(
            f'{value!r} at {_render_path(path)} is not a valid placeholder — '
            f"write ':name', or '::{name}' for a literal leading colon"
        )
    if name not in seen:
        seen.add(name)
        order.append(name)
    plan.append((path, name))
    return value


def _render_path(path: _Path) -> str:
    """Render a document path for an error message: ``update.$set.status``."""
    return '.'.join(str(step) for step in path) or '<root>'


def _copy(node: object) -> object:
    """Deep-copy the mutable parts of a document.

    Hand-written rather than ``copy.deepcopy`` because a template holds only
    documents, lists, and scalars — every scalar is immutable and can be
    shared, which makes this both faster and free of deepcopy's memo
    bookkeeping.
    """
    if isinstance(node, Mapping):
        return {key: _copy(value) for key, value in node.items()}
    if isinstance(node, list):
        return [_copy(item) for item in node]
    return node


def _assign(document: Any, path: _Path, value: object) -> None:
    """Write one bound value at its recorded path."""
    target = document
    for step in path[:-1]:
        target = target[step]
    target[path[-1]] = value
