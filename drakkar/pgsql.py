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


def render_update(
    quoted_table: str,
    quoted_set_columns: list[str],
    quoted_where_columns: list[str],
    quoted_null_where_columns: list[str],
) -> str:
    """Render a single-row UPDATE.

    Parameters are numbered SET values first, then the ``quoted_where_columns``
    values, so the caller flattens its arguments in that order.

    ``quoted_null_where_columns`` render ``IS NULL`` and consume no parameter:
    ``= NULL`` is never true in SQL's three-valued logic, so it would match no
    row and report success. They are appended after the equality predicates,
    which keeps the emitted clause order deterministic regardless of the
    caller's mapping order.
    """
    assignments = [f'{col} = ${i}' for i, col in enumerate(quoted_set_columns, start=1)]
    predicates = [f'{col} = ${i}' for i, col in enumerate(quoted_where_columns, start=len(quoted_set_columns) + 1)]
    predicates.extend(f'{col} IS NULL' for col in quoted_null_where_columns)
    return f'UPDATE {quoted_table} SET {", ".join(assignments)} WHERE {" AND ".join(predicates)}'


def render_upsert(
    quoted_table: str,
    quoted_columns: list[str],
    row_count: int,
    quoted_conflict: list[str],
    quoted_update_columns: list[str],
) -> str:
    """Render a multi-row INSERT with an ON CONFLICT tail.

    An empty ``quoted_update_columns`` means every inserted column belongs to
    the conflict target, so there is nothing left to overwrite; that renders
    ``DO NOTHING`` rather than a ``DO UPDATE SET`` with no assignments, which
    is a syntax error.
    """
    statement = render_insert(quoted_table, quoted_columns, row_count)
    target = ', '.join(quoted_conflict)
    if not quoted_update_columns:
        return f'{statement} ON CONFLICT ({target}) DO NOTHING'
    assignments = ', '.join(f'{col} = EXCLUDED.{col}' for col in quoted_update_columns)
    return f'{statement} ON CONFLICT ({target}) DO UPDATE SET {assignments}'


_PARAM_NAME_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_DOLLAR_TAG_RE = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)?\$')

# ASCII digits only. ``str.isdigit()`` is also True for characters like '٣'
# (ARABIC-INDIC THREE), which Postgres would never accept as a placeholder
# index — treating one as a positional parameter would reject valid SQL.
_ASCII_DIGITS = '0123456789'


def _scan_quoted(sql: str, start: int, quote: str) -> int:
    """Return the index just past a quoted run beginning at ``start``.

    Handles the doubled-quote escape (``''`` / ``""``), which is how both a
    literal and a quoted identifier embed their own delimiter.
    """
    i = start + 1
    n = len(sql)
    while i < n:
        if sql[i] == quote:
            if i + 1 < n and sql[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    raise ValueError(f'Unterminated {quote}-quoted run in statement SQL')


def _scan_block_comment(sql: str, start: int) -> int:
    """Return the index just past a ``/* ... */`` comment.

    Postgres nests block comments, unlike C, so this counts depth rather than
    stopping at the first ``*/``.
    """
    depth = 0
    i = start
    n = len(sql)
    while i < n:
        if sql.startswith('/*', i):
            depth += 1
            i += 2
        elif sql.startswith('*/', i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    raise ValueError('Unterminated block comment in statement SQL')


def _scan_dollar_quoted(sql: str, start: int) -> int | None:
    """Return the index past a dollar-quoted string, or None if ``start`` opens none."""
    match = _DOLLAR_TAG_RE.match(sql, start)
    if match is None:
        return None
    tag = match.group(0)
    end = sql.find(tag, match.end())
    if end == -1:
        raise ValueError('Unterminated dollar-quoted string in statement SQL')
    return end + len(tag)


def compile_named_statement(sql: str) -> tuple[str, list[str]]:
    """Rewrite ``:name`` placeholders in operator-authored SQL to positional ``$n``.

    Returns ``(rewritten_sql, param_names)``. ``param_names`` is in
    first-appearance order with duplicates collapsed, so ``:id`` used twice
    binds one value and is named once — the caller supplies exactly one value
    per returned name.

    Compiled once per statement at config validation and again at sink
    ``connect()``; never on the delivery path.

    The scan copies verbatim, and never interprets ``:``, inside single-quoted
    literals, double-quoted identifiers, dollar-quoted strings, ``--`` line
    comments, and nested ``/* */`` block comments. In code regions:

    - ``::`` is the cast operator, so ``::text`` is never read as a parameter.
    - ``:`` followed by an identifier is a named parameter.
    - ``:`` followed by anything else is copied verbatim — ``arr[1:3]`` slices
      and ``:=`` are legal SQL.
    - ``$`` followed by an ASCII digit raises: it is an author-written
      positional placeholder the framework cannot bind a value to. Unambiguous,
      because a dollar-quote tag never starts with a digit.
    """
    out: list[str] = []
    order: list[str] = []
    index: dict[str, int] = {}
    i = 0
    n = len(sql)

    while i < n:
        if sql.startswith('--', i):
            end = sql.find('\n', i)
            end = n if end == -1 else end
            out.append(sql[i:end])
            i = end
            continue

        if sql.startswith('/*', i):
            end = _scan_block_comment(sql, i)
            out.append(sql[i:end])
            i = end
            continue

        if sql[i] in ("'", '"'):
            end = _scan_quoted(sql, i, sql[i])
            out.append(sql[i:end])
            i = end
            continue

        if sql[i] == '$':
            if i + 1 < n and sql[i + 1] in _ASCII_DIGITS:
                raise ValueError(
                    'Statement SQL must use :name placeholders, not positional '
                    f'{sql[i : i + 2]!r} — the framework cannot bind it'
                )
            end = _scan_dollar_quoted(sql, i)
            if end is None:
                out.append('$')
                i += 1
            else:
                out.append(sql[i:end])
                i = end
            continue

        if sql.startswith('::', i):
            out.append('::')
            i += 2
            continue

        if sql[i] == ':':
            match = _PARAM_NAME_RE.match(sql, i + 1)
            if match is None:
                out.append(':')
                i += 1
                continue
            name = match.group(0)
            if name not in index:
                index[name] = len(order) + 1
                order.append(name)
            out.append(f'${index[name]}')
            i = match.end()
            continue

        out.append(sql[i])
        i += 1

    return ''.join(out), order
