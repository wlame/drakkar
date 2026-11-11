"""Tests for Drakkar structured logging setup."""

import io
import json
import sys
import threading
from pathlib import Path

import pytest
import structlog
from pydantic import ValidationError

from drakkar.config import LoggingConfig
from drakkar.ecs import DEFAULT_ECS_NAMESPACE, ECS_RESERVED_NAMES, ECS_VERSION
from drakkar.logging import (
    DeferredLog,
    EcsError,
    EcsRenderer,
    EcsSettings,
    IdentityFields,
    _resolve_output,
    close_logging,
    render_ecs_line,
    setup_logging,
)


def _capture_json_line(emit, **identity) -> dict:
    """Configure json logging with ``identity``, run ``emit(logger)`` and return the parsed line."""
    buf = io.StringIO()
    structlog.contextvars.clear_contextvars()
    setup_logging(LoggingConfig(level='INFO', format='json'), **identity)
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=buf), cache_logger_on_first_use=False)
    try:
        emit(structlog.get_logger())
        return json.loads(buf.getvalue().strip())
    finally:
        structlog.configure(
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )


def test_setup_logging_adds_the_identity_to_every_line():
    data = _capture_json_line(lambda log: log.info('x'), worker_id='w1', consumer_group='my-group', version='0.1.0')
    assert data['service_name'] == 'drakkar'
    assert data['worker_id'] == 'w1'
    assert data['consumer_group'] == 'my-group'
    assert data['service_version'] == '0.1.0'


def test_setup_logging_leaves_out_empty_identity_fields():
    data = _capture_json_line(lambda log: log.info('x'))
    assert data['service_name'] == 'drakkar'
    assert not {'worker_id', 'consumer_group', 'service_version'} & set(data)


def test_setup_logging_adds_the_identity_on_a_thread_with_an_empty_context():
    # A librdkafka callback runs on a thread of its own, whose context holds
    # nothing that the worker bound; the identity must still be on its lines.
    def emit(log):
        thread = threading.Thread(target=lambda: log.info('from_thread'))
        thread.start()
        thread.join()

    data = _capture_json_line(emit, worker_id='w-thread')
    assert data['event'] == 'from_thread'
    assert data['worker_id'] == 'w-thread'


def test_identity_fields_keep_a_value_given_at_the_call_site():
    processor = IdentityFields({'worker_id': 'self', 'service_name': 'drakkar'})
    assert processor(None, 'info', {'worker_id': 'given'}) == {'worker_id': 'given', 'service_name': 'drakkar'}


def test_deferred_log_drains_its_lines_in_order_once():
    log = DeferredLog()
    log.warning('first', a=1)
    log.info('second')
    assert log.drain() == [('warning', 'first', {'a': 1}), ('info', 'second', {})]
    assert log.drain() == []


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
    assert structlog.get_config()['logger_factory']._file is sys.stdout
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


# --- ECS format ---

ECS_FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'log_ecs.json').read_text(encoding='utf-8'))


@pytest.mark.parametrize('case', ECS_FIXTURE['cases'], ids=lambda case: case['name'])
def test_render_ecs_line_matches_fixture(case):
    error = EcsError(**case['error']) if case['error'] else None
    assert render_ecs_line(case['fields'], error, EcsSettings(**case['settings'])) == case['line']


def test_ecs_fixture_pins_the_version_and_namespace():
    assert ECS_FIXTURE['ecs_version'] == ECS_VERSION
    assert ECS_FIXTURE['default_namespace'] == DEFAULT_ECS_NAMESPACE
    assert ECS_FIXTURE['ecs_reserved_names'] == sorted(ECS_RESERVED_NAMES)


@pytest.mark.parametrize('name', ECS_FIXTURE['ecs_reserved_names'])
def test_ecs_namespace_refuses_every_reserved_name(name):
    with pytest.raises(ValidationError, match='is an ECS field name'):
        LoggingConfig(ecs_namespace=name)


def test_setup_logging_ecs_uses_the_configured_namespace_and_root_copy():
    buf = io.StringIO()
    structlog.contextvars.clear_contextvars()
    setup_logging(LoggingConfig(format='ecs', ecs_namespace='acme', ecs_request_id_at_root=True))
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=buf), cache_logger_on_first_use=False)
    try:
        structlog.get_logger().info('x', request_id='r-1', partition=2)
        data = json.loads(buf.getvalue().strip())
    finally:
        structlog.configure(
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )
    assert data['request_id'] == 'r-1'
    assert data['acme'] == {'partition': 2, 'request_id': 'r-1'}
    assert 'drakkar' not in data


def _capture_ecs_line(worker_id: str, emit) -> dict:
    """Configure ECS logging, run ``emit(logger)`` and return the parsed line."""
    buf = io.StringIO()
    structlog.contextvars.clear_contextvars()
    setup_logging(LoggingConfig(level='INFO', format='ecs'), worker_id=worker_id, version='9.9.9')
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=buf), cache_logger_on_first_use=False)
    try:
        emit(structlog.get_logger())
        return json.loads(buf.getvalue().strip())
    finally:
        structlog.configure(
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )


def test_setup_logging_ecs_writes_ecs_fields():
    data = _capture_ecs_line('w-ecs', lambda log: log.info('sink_failed', category='sink', error='refused'))
    assert list(data)[:4] == ['@timestamp', 'log.level', 'message', 'ecs.version']
    assert data['message'] == 'sink_failed'
    assert data['log.level'] == 'info'
    assert data['log']['logger'] == 'test_logging'
    assert data['service'] == {'name': 'drakkar', 'version': '9.9.9', 'node': {'name': 'w-ecs'}}
    assert data['drakkar'] == {'category': 'sink', 'error': 'refused'}
    assert 'error' not in data


def test_setup_logging_ecs_maps_an_exception_to_error_fields():
    def emit(log):
        try:
            raise ValueError('bad input')
        except ValueError:
            log.exception('hook_failed', hook='arrange')

    data = _capture_ecs_line('w-ecs', emit)
    assert data['log.level'] == 'error'
    assert data['error']['type'] == 'ValueError'
    assert data['error']['message'] == 'bad input'
    assert data['error']['stack_trace'].startswith('Traceback (most recent call last):')
    assert data['drakkar'] == {'hook': 'arrange'}


@pytest.mark.parametrize(
    ('exc_info', 'expected_type'),
    [
        (KeyError('k'), 'KeyError'),
        ((OSError, OSError('disk'), None), 'OSError'),
        (json.JSONDecodeError('m', 'd', 0), 'json.decoder.JSONDecodeError'),
    ],
)
def test_ecs_renderer_accepts_exception_forms(exc_info, expected_type):
    line = json.loads(EcsRenderer()(None, 'error', {'event': 'x', 'level': 'error', 'exc_info': exc_info}))
    assert line['error']['type'] == expected_type


@pytest.mark.parametrize('exc_info', [True, False, None, (None, None, None)])
def test_ecs_renderer_without_an_exception_has_no_error(exc_info):
    line = json.loads(EcsRenderer()(None, 'info', {'event': 'x', 'level': 'info', 'exc_info': exc_info}))
    assert 'error' not in line
    assert 'drakkar' not in line


def test_logging_format_refuses_an_unknown_value():
    with pytest.raises(ValidationError):
        LoggingConfig(format='xml')


@pytest.mark.parametrize(('value', 'text'), [(float('nan'), 'nan'), (float('inf'), 'inf'), (float('-inf'), '-inf')])
def test_render_ecs_line_writes_a_non_finite_float_as_text(value, text):
    line = render_ecs_line({'event': 'x', 'level': 'info', 'ratio': value, 'nested': {'v': [value]}})
    assert json.loads(line)['drakkar'] == {'nested': {'v': [text]}, 'ratio': text}
