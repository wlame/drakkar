"""Tests for Drakkar structured logging setup."""

import json

import structlog

from drakkar.config import LoggingConfig
from drakkar.logging import setup_logging


def test_setup_logging_json(capsys: object):
    config = LoggingConfig(level="INFO", format="json")
    setup_logging(config)
    logger = structlog.get_logger()
    logger.info("test message", key="value")
    # structlog writes to stderr
    import sys
    # just verify no exception was raised and logger is configured


def test_setup_logging_console():
    config = LoggingConfig(level="DEBUG", format="console")
    setup_logging(config)
    logger = structlog.get_logger()
    logger.debug("debug test")


def test_setup_logging_with_worker_id():
    config = LoggingConfig(level="INFO", format="json")
    setup_logging(config, worker_id="worker-1")
    ctx = structlog.contextvars.get_contextvars()
    assert ctx.get("worker_id") == "worker-1"


def test_setup_logging_level_filtering():
    config = LoggingConfig(level="WARNING", format="json")
    setup_logging(config)
    logger = structlog.get_logger()
    # should not raise even for filtered levels
    logger.info("this should be filtered")
    logger.warning("this should pass")
