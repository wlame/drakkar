"""Tests for :mod:`drakkar.loopserver` — structured loop-error reporting."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
import structlog
import uvicorn
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close, CloseCode

from drakkar.loopserver import LoopLoggingServer, log_loop_exception


def _keepalive_timeout_error() -> ConnectionClosedError:
    """The exact error the report saw: the server gave up on a silent client."""
    return ConnectionClosedError(Close(CloseCode.INTERNAL_ERROR, 'keepalive ping timeout'), None)


@pytest.mark.parametrize(
    'exc',
    [_keepalive_timeout_error(), ConnectionResetError('peer reset'), BrokenPipeError('broken pipe')],
)
def test_log_loop_exception_client_disconnect_logs_info_without_traceback(exc):
    loop = asyncio.new_event_loop()
    try:
        with structlog.testing.capture_logs() as captured:
            log_loop_exception(
                loop,
                {'message': 'ConnectionClosedError exception in shielded future', 'exception': exc},
                server_name='ui',
            )
    finally:
        loop.close()

    assert len(captured) == 1
    entry = captured[0]
    assert entry['event'] == 'server_client_disconnected'
    assert entry['log_level'] == 'info'
    assert entry['server'] == 'ui'
    assert entry['error_type'] == type(exc).__name__
    assert 'exc_info' not in entry


def test_log_loop_exception_unexpected_error_logs_error_with_traceback():
    loop = asyncio.new_event_loop()
    exc = ValueError('bad state')
    try:
        with structlog.testing.capture_logs() as captured:
            log_loop_exception(
                loop, {'message': 'Task exception was never retrieved', 'exception': exc}, server_name='webapp'
            )
    finally:
        loop.close()

    assert len(captured) == 1
    entry = captured[0]
    assert entry['event'] == 'server_loop_unhandled_exception'
    assert entry['log_level'] == 'error'
    assert entry['server'] == 'webapp'
    assert entry['detail'] == 'Task exception was never retrieved'
    assert entry['error_type'] == 'ValueError'
    assert entry['exc_info'] is exc


def test_log_loop_exception_context_without_exception_still_logs_error():
    loop = asyncio.new_event_loop()
    try:
        with structlog.testing.capture_logs() as captured:
            log_loop_exception(loop, {}, server_name='ui')
    finally:
        loop.close()

    assert captured[0]['event'] == 'server_loop_unhandled_exception'
    assert captured[0]['error_type'] is None
    assert captured[0]['detail'] == 'Unhandled exception in event loop'


async def test_loop_logging_server_serve_routes_loop_errors_to_structlog(capsys):
    """Once serve() runs, an error reported to the loop — here the same call
    asyncio.shield() makes for an unretrieved inner exception — becomes a
    structured event and nothing reaches stderr."""
    server = LoopLoggingServer(uvicorn.Config(app=None), server_name='ui')
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()

    async def _fake_uvicorn_serve(self, sockets=None):
        loop.call_exception_handler(
            {'message': 'ConnectionClosedError exception in shielded future', 'exception': _keepalive_timeout_error()}
        )

    try:
        with (
            patch.object(uvicorn.Server, 'serve', _fake_uvicorn_serve),
            structlog.testing.capture_logs() as captured,
        ):
            await server.serve()
    finally:
        loop.set_exception_handler(previous_handler)

    assert [e['event'] for e in captured] == ['server_client_disconnected']
    assert capsys.readouterr().err == ''
