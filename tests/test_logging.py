"""Tests for Drakkar structured logging setup."""

import io
import json

import structlog

from drakkar.config import LoggingConfig
from drakkar.logging import setup_logging


def test_setup_logging_json():
    config = LoggingConfig(level='INFO', format='json')
    setup_logging(config)
    logger = structlog.get_logger()
    logger.info('test message', key='value')


def test_setup_logging_console():
    config = LoggingConfig(level='DEBUG', format='console')
    setup_logging(config)
    logger = structlog.get_logger()
    logger.debug('debug test')


def test_setup_logging_binds_worker_id():
    config = LoggingConfig(level='INFO', format='json')
    setup_logging(config, worker_id='worker-1')
    ctx = structlog.contextvars.get_contextvars()
    assert ctx.get('worker_id') == 'worker-1'


def test_setup_logging_binds_service_name():
    config = LoggingConfig(level='INFO', format='json')
    setup_logging(config)
    ctx = structlog.contextvars.get_contextvars()
    assert ctx['service_name'] == 'drakkar'


def test_setup_logging_binds_consumer_group_and_version():
    config = LoggingConfig(level='INFO', format='json')
    setup_logging(config, worker_id='w1', consumer_group='my-group', version='0.1.0')
    ctx = structlog.contextvars.get_contextvars()
    assert ctx['consumer_group'] == 'my-group'
    assert ctx['service_version'] == '0.1.0'
    assert ctx['worker_id'] == 'w1'


def test_setup_logging_level_filtering():
    config = LoggingConfig(level='WARNING', format='json')
    setup_logging(config)
    logger = structlog.get_logger()
    logger.info('this should be filtered')
    logger.warning('this should pass')


def test_setup_logging_json_output_has_ecs_fields():
    """JSON output includes service_name, worker_id, category, timestamp."""
    import sys

    buf = io.StringIO()
    config = LoggingConfig(level='INFO', format='json')
    setup_logging(config, worker_id='test-w')

    # temporarily swap logger factory to capture output
    structlog.configure(
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    try:
        logger = structlog.get_logger()
        logger.info('check fields', category='test')

        line = buf.getvalue().strip()
        data = json.loads(line)
        assert 'timestamp' in data
        assert data['service_name'] == 'drakkar'
        assert data['worker_id'] == 'test-w'
        assert data['category'] == 'test'
        assert data['level'] == 'info'
    finally:
        # restore stderr logger for other tests
        structlog.configure(
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )
