"""``drakkar-schema`` — print the DDL of the tables a feature keeps in a shared database.

A worker creates its tables itself when ``manage_schema`` is true. When it is
false, an operator runs this script first, with the rights the worker does not
have::

    drakkar-schema postgres --schema=<name>
    python -m drakkar.schema postgres --schema=<name>
    drakkar-schema postgres --schema=<name> --recorder [--table-prefix=<p>] [--retention-hours=<n>]

``--recorder`` also prints the DDL of the recorder events table.

The output is plain SQL on stdout, so it can be piped to ``psql``. Exit codes
are 0 ok and 2 usage error; a usage error writes nothing to stdout.
"""

import argparse
import re
import sys
from collections.abc import Sequence
from typing import IO, NoReturn

from drakkar.config.connections import SCHEMA_RE
from drakkar.config.recorder import TABLE_PREFIX_RE
from drakkar.providers.postgres import (
    POSTGRES_PROVIDER,
    RETENTION_HOURS_MAX,
    RETENTION_HOURS_MIN,
    recorder_table_name,
    render_recorder_schema_sql,
    render_schema_sql,
)

EXIT_OK = 0
EXIT_USAGE = 2
HELP_FLAGS = ('-h', '--help')
DEFAULT_RETENTION_HOURS = 6
# Digits only: a sign, a space or a fraction is a usage error, the same on every implementation.
RETENTION_HOURS_RE = r'[0-9]+'

USAGE_TEXT = """drakkar-schema — print the DDL of the tables a feature keeps in a shared database.

Usage:
  drakkar-schema postgres --schema=<name>   print the DDL for that schema (also: python -m drakkar.schema)
  drakkar-schema postgres --schema=<name> --recorder [--table-prefix=<p>] [--retention-hours=<n>]   also print the recorder events table

The schema name is lower-case letters, digits and underscores, and starts with a letter or an underscore.

Exit codes:
  0  the DDL was printed
  2  usage error; the reason and this text go to stderr, nothing goes to stdout
"""


class _UsageError(Exception):
    """Raised for malformed arguments so ``main`` can return exit code 2 and keep stdout empty."""


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that raises instead of calling ``sys.exit``, so the handles stay under control."""

    def error(self, message: str) -> NoReturn:
        raise _UsageError(message)


def _build_parser() -> _Parser:
    """Return the parser: one feature name, the ``--schema`` flag and the recorder flags."""
    parser = _Parser(prog='drakkar-schema', add_help=False, allow_abbrev=False)
    parser.add_argument('feature', choices=[POSTGRES_PROVIDER])
    parser.add_argument('--schema', required=True)
    parser.add_argument('--recorder', action='store_true')
    # None marks a flag that was not given, so a recorder flag without --recorder is caught.
    parser.add_argument('--table-prefix', default=None)
    parser.add_argument('--retention-hours', default=None)
    return parser


def _check_recorder_flags(opts: argparse.Namespace) -> None:
    """Check the recorder flags; fill in their defaults when ``--recorder`` is given."""
    if not opts.recorder:
        if opts.table_prefix is not None or opts.retention_hours is not None:
            raise _UsageError('--table-prefix and --retention-hours need --recorder')
        return
    prefix = opts.table_prefix or ''
    if prefix and not re.fullmatch(TABLE_PREFIX_RE, prefix):
        raise _UsageError(f'--table-prefix must be empty or match {TABLE_PREFIX_RE}, got {prefix!r}')
    opts.table_prefix = prefix
    if opts.retention_hours is None:
        opts.retention_hours = DEFAULT_RETENTION_HOURS
        return
    text = opts.retention_hours
    hours = _whole_number(text)
    if hours is None or not RETENTION_HOURS_MIN <= hours <= RETENTION_HOURS_MAX:
        raise _UsageError(
            f'--retention-hours must be a whole number from {RETENTION_HOURS_MIN} to {RETENTION_HOURS_MAX}, got {text!r}'
        )
    opts.retention_hours = hours


def _whole_number(text: str) -> int | None:
    """Return the value of a string of ASCII digits, or ``None`` for anything else."""
    if not re.fullmatch(RETENTION_HOURS_RE, text):
        return None
    try:
        return int(text)
    except ValueError:  # more digits than int() converts; far outside the range anyway
        return None


def _parse(argv: Sequence[str]) -> argparse.Namespace:
    """Parse ``argv`` and check the schema name, the table prefix and the retention against their rules."""
    opts = _build_parser().parse_args(argv)
    if not re.fullmatch(SCHEMA_RE, opts.schema):
        raise _UsageError(f'--schema must match {SCHEMA_RE}, got {opts.schema!r}')
    _check_recorder_flags(opts)
    return opts


def main(argv: Sequence[str] | None = None, stdout: IO[str] = sys.stdout, stderr: IO[str] = sys.stderr) -> int:
    """Print the DDL for ``postgres --schema=<name>`` and return the exit code.

    With ``--recorder`` one empty line and the recorder events table's DDL follow.
    ``argv`` defaults to ``sys.argv[1:]``. Any usage error writes the reason and the
    usage text to ``stderr``, writes nothing to ``stdout`` and returns 2.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    if any(flag in args for flag in HELP_FLAGS):
        stdout.write(USAGE_TEXT)
        return EXIT_OK
    try:
        opts = _parse(args)
    except _UsageError as exc:
        stderr.write(f'drakkar-schema: {exc}\n\n{USAGE_TEXT}')
        return EXIT_USAGE
    output = render_schema_sql(opts.schema)
    if opts.recorder:
        table = recorder_table_name(opts.table_prefix)
        output += '\n' + render_recorder_schema_sql(opts.schema, table, opts.retention_hours)
    stdout.write(output)
    return EXIT_OK


if __name__ == '__main__':  # pragma: no cover -- exercised via subprocess in tests
    sys.exit(main())
