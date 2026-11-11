"""Structured logging setup for Drakkar framework.

Three formats: ``json`` (flat structlog keys), ``ecs`` (Elastic Common Schema)
and ``console`` (human-readable).
"""

import json
import logging
import math
import sys
import traceback
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from types import TracebackType
from typing import Any, NamedTuple, TextIO

import structlog

from drakkar.config import LoggingConfig

_log_file_handle: TextIO | None = None

ECS_VERSION = '8.11.0'
ECS_OWN_FIELDS_KEY = 'drakkar'

# Keys of the event dict that become ECS fields; every other key goes under
# ``drakkar``. A framework field such as ``error`` (a string) or ``source``
# would clash with an ECS object of the same name, and Elasticsearch rejects
# a document whose field type differs from the mapping.
_ECS_CONSUMED_KEYS = frozenset(
    {'timestamp', 'level', 'event', 'module', 'service_name', 'service_version', 'worker_id', 'exc_info'},
)

type ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]


class EcsError(NamedTuple):
    """The ECS ``error`` fields of one exception."""

    type: str
    message: str
    stack_trace: str


def _sorted_value(value: Any) -> Any:
    """Return ``value`` with the keys of every mapping inside it sorted.

    A NaN or infinite float becomes its ``str()`` text: JSON has no such
    numbers, and ``json.dumps`` would write the invalid token ``NaN``.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _sorted_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, list | tuple):
        return [_sorted_value(item) for item in value]
    return value


def build_ecs_record(fields: Mapping[str, Any], error: EcsError | None = None) -> dict[str, Any]:
    """Map one log event to an ECS document with a fixed key order.

    ``fields`` is the event dict as the renderer sees it: ``timestamp``,
    ``level``, ``event``, ``module`` and the identity keys, plus the
    framework's own fields, which are placed sorted under ``drakkar``.
    """
    record: dict[str, Any] = {
        '@timestamp': fields.get('timestamp', ''),
        'log.level': fields.get('level', ''),
        'message': str(fields.get('event', '')),
        'ecs.version': ECS_VERSION,
    }
    if 'module' in fields:
        record['log'] = {'logger': fields['module']}

    service: dict[str, Any] = {'name': fields.get('service_name', 'drakkar')}
    if fields.get('service_version'):
        service['version'] = fields['service_version']
    if fields.get('worker_id'):
        service['node'] = {'name': fields['worker_id']}
    record['service'] = service

    if error is not None:
        record['error'] = {'type': error.type, 'message': error.message, 'stack_trace': error.stack_trace}

    own = {key: value for key, value in fields.items() if key not in _ECS_CONSUMED_KEYS}
    if own:
        record[ECS_OWN_FIELDS_KEY] = _sorted_value(own)
    return record


def render_ecs_line(fields: Mapping[str, Any], error: EcsError | None = None) -> str:
    """Render one log event as one compact ECS JSON line (no newline)."""
    return json.dumps(
        build_ecs_record(fields, error),
        ensure_ascii=False,
        separators=(',', ':'),
        default=str,
    )


def _resolve_exc_info(exc_info: object) -> ExcInfo | None:
    """Turn structlog's ``exc_info`` value (True, an exception or a tuple) into a tuple."""
    if isinstance(exc_info, BaseException):
        return type(exc_info), exc_info, exc_info.__traceback__
    if isinstance(exc_info, tuple) and len(exc_info) == 3 and exc_info[1] is not None:
        return exc_info  # type: ignore[return-value]
    if exc_info is True:
        current = sys.exc_info()
        if current[1] is not None:
            return current  # type: ignore[return-value]
    return None


def _ecs_error_of(exc_info: object) -> EcsError | None:
    """Build the ECS ``error`` fields from structlog's ``exc_info`` value."""
    resolved = _resolve_exc_info(exc_info)
    if resolved is None:
        return None
    exc_type, exc, tb = resolved
    return EcsError(
        type=f'{exc_type.__module__}.{exc_type.__qualname__}'
        if exc_type.__module__ != 'builtins'
        else exc_type.__qualname__,
        message=str(exc),
        stack_trace=''.join(traceback.format_exception(exc_type, exc, tb)),
    )


class IdentityFields:
    """structlog processor: add the worker identity to every log line.

    The identity is held here, not in ``structlog.contextvars``: a context
    variable reaches only code that runs in the context that bound it, while
    librdkafka callbacks run on threads of their own, and the work they hand
    to the event loop inherits that empty context. A value given at the call
    site wins.
    """

    def __init__(self, fields: Mapping[str, str]) -> None:
        self._fields = dict(fields)

    def __call__(self, _logger: object, _method_name: str, event_dict: MutableMapping[str, Any]) -> Any:
        for key, value in self._fields.items():
            event_dict.setdefault(key, value)
        return event_dict


class DeferredLog:
    """Log lines recorded before logging is set up, to be written after it.

    The app's constructor runs checks that warn, but logging is configured
    only when the worker starts: configuring it in the constructor would
    change global logging state for every program that only builds an app,
    tests included. A line written before the setup would come out in
    structlog's default console format, which a JSON log pipeline cannot read.
    """

    def __init__(self) -> None:
        self._lines: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        """Record an info line."""
        self._lines.append(('info', event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        """Record a warning line."""
        self._lines.append(('warning', event, fields))

    def drain(self) -> list[tuple[str, str, dict[str, Any]]]:
        """Return the recorded lines as (method, event, fields), oldest first, and forget them."""
        lines, self._lines = self._lines, []
        return lines


def identity_fields(worker_id: str = '', consumer_group: str = '', version: str = '') -> dict[str, str]:
    """The identity fields of every log line; empty values are left out."""
    fields = {'service_name': 'drakkar'}
    if worker_id:
        fields['worker_id'] = worker_id
    if consumer_group:
        fields['consumer_group'] = consumer_group
    if version:
        fields['service_version'] = version
    return fields


def ecs_renderer(_logger: object, _method_name: str, event_dict: MutableMapping[str, Any]) -> str:
    """structlog processor: render the event dict as one ECS JSON line."""
    error = _ecs_error_of(event_dict.get('exc_info'))
    return render_ecs_line(event_dict, error)


def _resolve_output(
    output: str,
    worker_id: str = '',
    cluster_name: str = '',
) -> TextIO:
    """Resolve the log output destination from config.

    "stderr" and "stdout" map to sys streams. Anything else is treated
    as a file path with template variable substitution.
    """
    global _log_file_handle

    if output == 'stderr':
        return sys.stderr
    if output == 'stdout':
        return sys.stdout

    path = output.format(worker_id=worker_id, cluster_name=cluster_name)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    _log_file_handle = open(path, 'a', encoding='utf-8')  # noqa: SIM115
    return _log_file_handle


def close_logging() -> None:
    """Close the log file handle if one was opened. No-op for stderr/stdout."""
    global _log_file_handle
    if _log_file_handle is not None:
        _log_file_handle.close()
        _log_file_handle = None


def setup_logging(
    config: LoggingConfig,
    worker_id: str = '',
    consumer_group: str = '',
    version: str = '',
    cluster_name: str = '',
) -> None:
    """Configure structlog with JSON, ECS or console output.

    ``json`` writes the flat structlog keys (``timestamp``, ``level``,
    ``event``); ``ecs`` writes the Elastic Common Schema with the framework's
    own fields under ``drakkar``. The identity fields appear on every log
    line, from every thread and task.

    Calling this again closes the log file of the previous call.

    The ``config.output`` field controls where logs are written:
    ``"stderr"`` (default), ``"stdout"``, or a file path with optional
    template variables ``{worker_id}`` and ``{cluster_name}``.
    """
    close_logging()
    log_output = _resolve_output(config.output, worker_id=worker_id, cluster_name=cluster_name)

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        IdentityFields(identity_fields(worker_id=worker_id, consumer_group=consumer_group, version=version)),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt='iso', key='timestamp'),
        structlog.processors.StackInfoRenderer(),
    ]
    # The ECS renderer reads exc_info itself to fill error.type, error.message
    # and error.stack_trace; format_exc_info would turn it into one text first.
    if config.format != 'ecs':
        processors.append(structlog.processors.format_exc_info)
    processors += [
        structlog.processors.CallsiteParameterAdder(
            [structlog.processors.CallsiteParameter.MODULE],
        ),
        structlog.processors.UnicodeDecoder(),
    ]

    if config.format == 'json':
        processors.append(structlog.processors.JSONRenderer())
    elif config.format == 'ecs':
        processors.append(ecs_renderer)
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.level.upper(), logging.INFO),
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=log_output),
        cache_logger_on_first_use=True,
    )
