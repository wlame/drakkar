"""Tests for Drakkar structured logging setup."""

import io
import json
import sys

import structlog

from drakkar.config import LoggingConfig
from drakkar.logging import _resolve_output, close_logging, setup_logging


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


# --- Output destination tests ---


def test_resolve_output_stderr():
    assert _resolve_output('stderr') is sys.stderr


def test_resolve_output_stdout():
    assert _resolve_output('stdout') is sys.stdout


def test_resolve_output_file(tmp_path):
    log_file = str(tmp_path / 'app.log')
    f = _resolve_output(log_file)
    try:
        assert f.name == log_file
        assert f.mode == 'a'
        f.write('test\n')
        f.flush()
        assert (tmp_path / 'app.log').read_text() == 'test\n'
    finally:
        f.close()


def test_resolve_output_file_with_template_vars(tmp_path):
    pattern = str(tmp_path / '{worker_id}-{cluster_name}.log')
    f = _resolve_output(pattern, worker_id='w1', cluster_name='prod')
    try:
        expected = tmp_path / 'w1-prod.log'
        assert f.name == str(expected)
    finally:
        f.close()


def test_resolve_output_creates_parent_dirs(tmp_path):
    log_file = str(tmp_path / 'logs' / 'nested' / 'app.log')
    f = _resolve_output(log_file)
    try:
        assert (tmp_path / 'logs' / 'nested').is_dir()
    finally:
        f.close()


def test_setup_logging_with_stdout():
    config = LoggingConfig(level='INFO', format='json', output='stdout')
    setup_logging(config, worker_id='w-stdout')
    ctx = structlog.contextvars.get_contextvars()
    assert ctx['worker_id'] == 'w-stdout'
    # restore default
    structlog.configure(
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def test_setup_logging_with_file_writes_json(tmp_path):
    log_file = str(tmp_path / 'drakkar.log')
    config = LoggingConfig(level='INFO', format='json', output=log_file)
    setup_logging(config, worker_id='w-file')

    structlog.configure(cache_logger_on_first_use=False)
    try:
        logger = structlog.get_logger()
        logger.info('file_test', category='test')

        content = (tmp_path / 'drakkar.log').read_text().strip()
        data = json.loads(content)
        assert data['event'] == 'file_test'
        assert data['worker_id'] == 'w-file'
    finally:
        structlog.configure(
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )


def test_output_default_is_stderr():
    config = LoggingConfig()
    assert config.output == 'stderr'


# --- close_logging() tests ---


def test_close_logging_closes_file_handle(tmp_path):
    """close_logging() closes the file handle opened by _resolve_output."""
    import drakkar.logging as logging_mod

    log_file = str(tmp_path / 'closeable.log')
    f = _resolve_output(log_file)
    assert not f.closed
    assert logging_mod._log_file_handle is f

    close_logging()

    assert f.closed
    assert logging_mod._log_file_handle is None


def test_close_logging_noop_for_stderr():
    """close_logging() is a no-op when output is stderr (no file handle)."""
    import drakkar.logging as logging_mod

    _resolve_output('stderr')
    assert logging_mod._log_file_handle is None

    close_logging()  # should not raise
    assert logging_mod._log_file_handle is None


def test_close_logging_noop_for_stdout():
    """close_logging() is a no-op when output is stdout (no file handle)."""
    import drakkar.logging as logging_mod

    _resolve_output('stdout')
    assert logging_mod._log_file_handle is None

    close_logging()  # should not raise
    assert logging_mod._log_file_handle is None
