"""Shared sink test double for tests that drive a real ``DrakkarApp``.

Three copies of this helper existed, in ``test_app``, ``test_config_modes``
and ``test_shutdown_metrics``, and two more modules imported the first. Each
copy hand-listed the sink methods that must stay synchronous, because a bare
``AsyncMock()`` makes every attribute async and a coroutine object is always
truthy. Any predicate the manager calls therefore reads as True.

One copy missed ``batch_idempotent``, so ``SinkManager._deliver_to_sink``
never took its non-idempotent single-shot branch in those tests — it always
fell through to the idempotent retry loop instead. That is the failure mode a
hand-maintained list invites, so the list is gone: :func:`make_sink_mock`
specs the double against the real sink object, and ``create_autospec`` decides
per method whether it is sync or async by reading the class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import create_autospec

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp
    from drakkar.sinks.base import BaseSink


def make_sink_mock(sink: BaseSink):
    """Return a test double for ``sink`` with its sync/async split preserved.

    ``create_autospec`` reads the real class, so ``deliver`` is awaitable
    while ``batch_idempotent``, ``should_skip_delivery`` and the circuit
    breaker hooks stay synchronous. Adding a method to ``BaseSink`` needs no
    change here.

    The circuit-breaker state is then pinned to plain values. Autospec models
    a property as a mock whose spec is the property's *type*, so an equality
    check against ``'closed'`` would fail; tests that exercise the breaker
    override these afterwards.
    """
    mock = create_autospec(sink, instance=True)
    mock.sink_type = sink.sink_type
    mock.name = sink.name
    mock._name = sink.name
    # Defaults describing a healthy, closed, non-probing circuit.
    mock.should_skip_delivery.return_value = False
    mock.circuit_state = 'closed'
    mock.probe_inflight = False
    mock.is_connected = False
    # False keeps deliveries on the single-shot path. The idempotent path
    # retries transient errors, which would turn one expected ``deliver``
    # call into several and hide which branch a test is really exercising.
    mock.batch_idempotent.return_value = False
    return mock


def setup_app_sinks(app: DrakkarApp) -> None:
    """Build the app's sinks, then swap each one for a mock.

    Replaces the entry in ``_sinks`` and the matching entry in ``_by_type``,
    so lookups by key and by type both return the same double.
    """
    app._build_sinks()
    for key, sink in app._sink_manager._sinks.items():
        mock_sink = make_sink_mock(sink)
        app._sink_manager._sinks[key] = mock_sink
        for i, s in enumerate(app._sink_manager._by_type[sink.sink_type]):
            if s.name == sink.name:
                app._sink_manager._by_type[sink.sink_type][i] = mock_sink
