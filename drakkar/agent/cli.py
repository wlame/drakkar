"""``drakkar-agent`` — print the agent manual, the effective config or the agent reference page.

It needs no running worker::

    drakkar-agent manual [--parts=<p,...>] [--detail=brief|full] [--format=json|markdown] [--topic=<kind>:<key>]
    drakkar-agent config --config=<path> [--parts=<p,...>] [--detail=brief|full]
    drakkar-agent reference
    python -m drakkar.agent ...

The outputs are the texts of the agent routes: compact JSON plus one line
break, or Markdown. Every flag is written ``--name=value``. Exit codes are 0
ok and 2 for a usage error, an unknown topic or a config that does not load;
then nothing goes to stdout.
"""

from __future__ import annotations

import io
import sys
from collections.abc import Sequence
from typing import IO

import structlog

from drakkar.agent.config_view import AppConfigView, ConfigLeaf, ConfigSources, config_answer
from drakkar.agent.params import (
    CONFIG_PARTS,
    MANUAL_PARTS,
    NO_SUCH_TOPIC_MESSAGE,
    ParameterError,
    parse_detail,
    parse_format,
    parse_parts,
)
from drakkar.agent.reference import reference_markdown
from drakkar.agent.registry import (
    Registry,
    TopicNotFoundError,
    dumps,
    load_registry,
    read_config_metadata,
    select_manual,
    selection_json,
    selection_markdown,
)
from drakkar.config import load_config_with_origins
from drakkar.configmeta import dump_by_yaml_key

PROGRAM = 'drakkar-agent'
EXIT_OK = 0
EXIT_USAGE = 2
HELP_FLAGS = ('-h', '--help')
FLAG_PREFIX = '--'

# The flags each command takes; a flag of another command is a usage error.
COMMAND_FLAGS: dict[str, frozenset[str]] = {
    'manual': frozenset({'parts', 'detail', 'format', 'topic'}),
    'config': frozenset({'config', 'parts', 'detail'}),
    'reference': frozenset(),
}
# The handler notes are not known without a running worker.
OFFLINE_MANUAL_PARTS = tuple(part for part in MANUAL_PARTS if part != 'app')

USAGE_TEXT = """drakkar-agent — print the agent manual, the effective config or the agent reference page.

Usage:
  drakkar-agent manual [--parts=<p,...>] [--detail=brief|full] [--format=json|markdown] [--topic=<kind>:<key>]
      the framework manual, as GET /api/v1/agent/manual answers it, without the handler notes
  drakkar-agent config --config=<path> [--parts=<p,...>] [--detail=brief|full]
      the effective config of that file and the DK_ variables, as GET /api/v1/agent/config answers it
  drakkar-agent reference
      the Markdown page of every registry entry

Every flag is written --name=value. PromQL queries keep their {{worker}} and {{cluster}} placeholders.

Exit codes:
  0  the output was printed
  2  usage error, unknown topic or a config that does not load; the reason goes to stderr, nothing goes to stdout
"""


class _UsageError(Exception):
    """Malformed arguments: the reason and the usage text go to stderr."""


class _OutputError(Exception):
    """Arguments that parse but name nothing, or a config that does not load: only the reason goes to stderr."""


def _parse_flags(command: str, args: Sequence[str]) -> dict[str, str]:
    """The ``--name=value`` flags of ``command``; a raw form, an unknown flag or a repeat is a usage error."""
    flags: dict[str, str] = {}
    for arg in args:
        name, has_value, value = arg.removeprefix(FLAG_PREFIX).partition('=')
        if not arg.startswith(FLAG_PREFIX) or not has_value or name not in COMMAND_FLAGS[command]:
            raise _UsageError(f"unknown argument '{arg}'")
        if name in flags:
            raise _UsageError(f'--{name} is given twice')
        flags[name] = value
    return flags


def _manual(flags: dict[str, str], registry: Registry) -> str:
    parts = parse_parts(flags.get('parts'), OFFLINE_MANUAL_PARTS)
    detail = parse_detail(flags.get('detail'))
    markdown = parse_format(flags.get('format')) == 'markdown'
    topic = flags.get('topic')
    try:
        selection = select_manual(registry, parts=parts, detail=detail, topic=topic)
    except TopicNotFoundError:
        raise _OutputError(NO_SUCH_TOPIC_MESSAGE.format(ref=topic)) from None
    if markdown:
        return selection_markdown(selection)
    return dumps(selection_json(selection)) + '\n'


def _config_leaves() -> tuple[ConfigLeaf, ...]:
    return tuple(
        ConfigLeaf(
            path=row['path'],
            description=row['description'],
            type=row['type'],
            default=row['default'],
            secret=row['secret'],
        )
        for row in read_config_metadata()
    )


def _config(flags: dict[str, str], registry: Registry) -> str:
    if 'config' not in flags:
        raise _UsageError('config needs --config=<path>')
    parts = parse_parts(flags.get('parts'), CONFIG_PARTS)
    full = parse_detail(flags.get('detail')) == 'full'
    try:
        config, origins = load_config_with_origins(flags['config'])
    except Exception as exc:  # every load failure is the operator's to fix; its text says what failed
        raise _OutputError(str(exc)) from exc
    answer = config_answer(
        leaves=_config_leaves(),
        config_dump=dump_by_yaml_key(config, mode='json'),
        sources=ConfigSources(origins=origins, before_on_startup=None),
        app=AppConfigView(values=config.app, yaml_section=config.app),
        parts=parts,
        registry=registry,
        full=full,
    )
    return dumps(answer) + '\n'


def _output(command: str, flags: dict[str, str], registry: Registry) -> str:
    if command == 'manual':
        return _manual(flags, registry)
    if command == 'config':
        return _config(flags, registry)
    return reference_markdown(registry)


def main(
    argv: Sequence[str] | None = None,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
    registry: Registry | None = None,
) -> int:
    """Print the output of one command and return the exit code.

    ``argv`` defaults to ``sys.argv[1:]``, ``stdout`` and ``stderr`` to the
    streams of ``sys`` at the call, ``registry`` to the shipped registry.
    ``-h`` or ``--help`` anywhere prints the usage text to stdout. Any error writes
    to stderr only and returns 2.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    if any(flag in args for flag in HELP_FLAGS):
        stdout.write(USAGE_TEXT)
        return EXIT_OK
    try:
        if not args or args[0] not in COMMAND_FLAGS:
            raise _UsageError(f"unknown command '{args[0]}'" if args else 'a command is needed')
        command = args[0]
        flags = _parse_flags(command, args[1:])
        text = _output(command, flags, registry if registry is not None else load_registry())
    except _UsageError as exc:
        stderr.write(f'{PROGRAM}: {exc}\n\n{USAGE_TEXT}')
        return EXIT_USAGE
    except (ParameterError, _OutputError) as exc:
        stderr.write(f'{PROGRAM}: {exc}\n')
        return EXIT_USAGE
    stdout.write(text)
    return EXIT_OK


def use_utf8_lf(stream: IO[str]) -> None:
    """Make a text stream write UTF-8 with LF line breaks, whatever the locale says."""
    if isinstance(stream, io.TextIOWrapper):
        stream.reconfigure(encoding='utf-8', newline='\n')


def run() -> int:
    """The console entry: UTF-8 bytes with LF on both streams, and log lines of the config load on stderr.

    So stdout holds only the output, with the same bytes in every locale.
    """
    use_utf8_lf(sys.stdout)
    use_utf8_lf(sys.stderr)
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
    return main()
