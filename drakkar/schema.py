"""``drakkar-schema`` — print the DDL of the tables a feature keeps in a shared database.

A worker creates its tables itself when ``manage_schema`` is true. When it is
false, an operator runs this script first, with the rights the worker does not
have::

    drakkar-schema postgres --schema=<name>
    python -m drakkar.schema postgres --schema=<name>

The output is plain SQL on stdout, so it can be piped to ``psql``. Exit codes
are 0 ok and 2 usage error; a usage error writes nothing to stdout.
"""

import argparse
import re
import sys
from collections.abc import Sequence
from typing import IO, NoReturn

from drakkar.config.connections import SCHEMA_RE
from drakkar.providers.postgres import POSTGRES_PROVIDER, render_schema_sql

EXIT_OK = 0
EXIT_USAGE = 2
HELP_FLAGS = ('-h', '--help')

USAGE_TEXT = """drakkar-schema — print the DDL of the tables a feature keeps in a shared database.

Usage:
  drakkar-schema postgres --schema=<name>   print the DDL for that schema (also: python -m drakkar.schema)

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
    """Return the parser: one feature name and the ``--schema`` flag."""
    parser = _Parser(prog='drakkar-schema', add_help=False, allow_abbrev=False)
    parser.add_argument('feature', choices=[POSTGRES_PROVIDER])
    parser.add_argument('--schema', required=True)
    return parser


def _parse(argv: Sequence[str]) -> argparse.Namespace:
    """Parse ``argv`` and check the schema name against the config's schema rule."""
    opts = _build_parser().parse_args(argv)
    if not re.fullmatch(SCHEMA_RE, opts.schema):
        raise _UsageError(f'--schema must match {SCHEMA_RE}, got {opts.schema!r}')
    return opts


def main(argv: Sequence[str] | None = None, stdout: IO[str] = sys.stdout, stderr: IO[str] = sys.stderr) -> int:
    """Print the DDL for ``postgres --schema=<name>`` and return the exit code.

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
    stdout.write(render_schema_sql(opts.schema))
    return EXIT_OK


if __name__ == '__main__':  # pragma: no cover -- exercised via subprocess in tests
    sys.exit(main())
