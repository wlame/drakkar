"""Structured logging setup for Drakkar framework — ECS-compatible JSON output."""

import logging
import sys

import structlog

from drakkar.config import LoggingConfig


def setup_logging(
    config: LoggingConfig,
    worker_id: str = "",
    consumer_group: str = "",
    version: str = "",
) -> None:
    """Configure structlog with JSON or console output.

    JSON mode produces ECS-compatible logs for Elastic ingestion.
    Global context fields are bound once and appear on every log line.
    """
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", key="timestamp"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.CallsiteParameterAdder(
            [structlog.processors.CallsiteParameter.MODULE],
        ),
    ]

    if config.format == "json":
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
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
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
