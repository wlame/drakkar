"""Pure SQL construction for the PostgreSQL sink.

Every function here is a string transformation: no asyncpg, no I/O, and no
imports from the ``drakkar`` package.

That last rule is load-bearing rather than stylistic. ``drakkar.config``
needs these helpers to validate operator-authored statements at config
load, and importing anything under ``drakkar/sinks/`` executes
``drakkar/sinks/__init__.py``, which imports every sink, each of which
imports ``drakkar.config`` — so housing them under ``sinks/`` makes config
load fail with a partially-initialized-module ImportError. Living at the
top level alongside ``timefmt``/``dbfiles``/``merge`` avoids the cycle.

Keeping the module dependency-free also means every rendering rule is
unit-testable with no database and no mocks.
"""

import re

_IDENT_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Caps the positional parameters in one multi-row INSERT — the Postgres
# wire protocol limits a statement to 65535 bind parameters, so oversized
# groups are chunked into multiple statements. Matches the Go backend's
# ``maxInsertParams``.
MAX_INSERT_PARAMS = 65535


def quote_ident(name: str) -> str:
    """Quote a SQL identifier to prevent injection.

    Only allows simple alphanumeric+underscore identifiers.
    Raises ValueError for anything suspicious.
    """
    if not _IDENT_RE.match(name):
        raise ValueError(f'Invalid SQL identifier: {name!r}')
    return f'"{name}"'


def render_insert(quoted_table: str, quoted_columns: list[str], row_count: int) -> str:
    """Render one INSERT covering ``row_count`` rows of the same shape.

    Parameters are numbered continuously across rows — row 2 of a two-column
    insert binds ``$3, $4`` — because every row's values are flattened into
    one argument list at execution.

    Identifiers must already be quoted; this function never quotes, so a
    caller cannot accidentally bypass :func:`quote_ident` by passing a raw
    name and having it silently accepted.
    """
    col_names = ', '.join(quoted_columns)
    width = len(quoted_columns)
    tuples = ['(' + ', '.join(f'${row * width + col + 1}' for col in range(width)) + ')' for row in range(row_count)]
    return f'INSERT INTO {quoted_table} ({col_names}) VALUES {", ".join(tuples)}'
