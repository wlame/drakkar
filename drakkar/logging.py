"""Structured logging setup for Drakkar framework — ECS-compatible JSON output."""

import logging
import sys
from pathlib import Path
from typing import IO

import structlog

from drakkar.config import LoggingConfig


def _resolve_output(
    output: str,
    worker_id: str = '',
    cluster_name: str = '',
) -> IO:
    """Resolve the log output destination from config.

    "stderr" and "stdout" map to sys streams. Anything else is treated
    as a file path with template variable substitution.
    """
    if output == 'stderr':
        return sys.stderr
    if output == 'stdout':
        return sys.stdout

    path = output.format(worker_id=worker_id, cluster_name=cluster_name)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return open(path, 'a', encoding='utf-8')


def setup_logging(
    config: LoggingConfig,
    worker_id: str = '',
    consumer_group: str = '',
    version: str = '',
    cluster_name: str = '',
) -> None:
    """Configure structlog with JSON or console output.

    JSON mode produces ECS-compatible logs for Elastic ingestion.
    Global context fields are bound once and appear on every log line.

    The ``config.output`` field controls where logs are written:
    ``"stderr"`` (default), ``"stdout"``, or a file path with optional
    template variables ``{worker_id}`` and ``{cluster_name}``.
    """
    log_output = _resolve_output(config.output, worker_id=worker_id, cluster_name=cluster_name)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt='iso', key='timestamp'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.CallsiteParameterAdder(
            [structlog.processors.CallsiteParameter.MODULE],
        ),
    ]

    if config.format == 'json':
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.level.upper(), logging.INFO),
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=log_output),
        cache_logger_on_first_use=True,
    )

    ctx: dict[str, str] = {
        'service_name': 'drakkar',
    }
    if worker_id:
        ctx['worker_id'] = worker_id
    if consumer_group:
        ctx['consumer_group'] = consumer_group
    if version:
        ctx['service_version'] = version
    structlog.contextvars.bind_contextvars(**ctx)
