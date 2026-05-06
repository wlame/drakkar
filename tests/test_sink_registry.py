"""Tests for ``drakkar.sinks.registry.SinkRegistry``.

The registry is process-wide class-level state, so every test snapshots
it on entry and restores it on exit via the ``isolated_registry`` fixture.
That fixture is autouse to keep test order irrelevant — without it a
later test could observe registrations from an earlier one and produce a
false positive.

The tests cover four behaviours required by Phase 4 Task 5:

* register / get round-trip and validation guarantees;
* ``discover()`` driven by a mocked ``importlib.metadata.entry_points``;
* ``SinkManager`` resolving a registry-installed sink type by name; and
* ``get`` returning ``None`` for unknown names instead of raising.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from drakkar.sinks import SinkRegistry
from drakkar.sinks.base import BaseSink
from drakkar.sinks.manager import SinkManager
from drakkar.sinks.registry import ENTRY_POINT_GROUP


class _Payload(BaseModel):
    """Minimal payload model — content is irrelevant; the registry tests
    never call ``deliver``.
    """

    value: int = 0


class _CustomSink(BaseSink[_Payload]):
    """Concrete ``BaseSink`` subclass used as a stand-in for a plugin sink.

    The three abstract methods exist as no-ops so the class is concrete
    (and therefore ``register``-able). No connection or delivery happens
    in these tests.
    """

    sink_type = 'custom'

    async def connect(self) -> None:
        return None

    async def deliver(self, payloads: list[_Payload]) -> None:  # pragma: no cover - never called
        return None

    async def close(self) -> None:
        return None


class _AnotherSink(_CustomSink):
    """Second concrete subclass, used to verify re-registration semantics
    and ``all_names`` ordering.
    """

    sink_type = 'another'


@pytest.fixture(autouse=True)
def isolated_registry() -> Any:
    """Snapshot and restore the class-level registry state between tests.

    We do NOT use ``SinkRegistry._clear`` here unconditionally — clearing
    would also wipe the built-in registrations performed at
    ``drakkar.sinks`` import time, which other test modules running in
    the same process implicitly depend on. Snapshot-and-restore preserves
    whatever state the test inherited and rolls back any mutations the
    test made.
    """
    snapshot = dict(SinkRegistry._registered)
    discovered = SinkRegistry._discovered
    try:
        yield
    finally:
        SinkRegistry._registered = snapshot
        SinkRegistry._discovered = discovered


def test_register_and_get_round_trip() -> None:
    SinkRegistry.register('custom', _CustomSink)
    assert SinkRegistry.get('custom') is _CustomSink


def test_get_unknown_returns_none_does_not_raise() -> None:
    # Using a deliberately unlikely name to avoid colliding with a
    # built-in registration the snapshot fixture preserves.
    assert SinkRegistry.get('definitely_not_a_real_sink_type') is None


def test_register_rejects_non_basesink_class() -> None:
    class NotASink:
        pass

    with pytest.raises(TypeError, match='subclass of BaseSink'):
        SinkRegistry.register('bad', NotASink)  # type: ignore[arg-type]


def test_register_rejects_non_class() -> None:
    with pytest.raises(TypeError, match='subclass of BaseSink'):
        SinkRegistry.register('bad', 'not_a_class')  # type: ignore[arg-type]


@pytest.mark.parametrize('bad_name', ['', '   ', '\t', '\n'])
def test_register_rejects_empty_or_whitespace_name(bad_name: str) -> None:
    with pytest.raises(TypeError, match='non-empty string'):
        SinkRegistry.register(bad_name, _CustomSink)


def test_register_rejects_non_string_name() -> None:
    with pytest.raises(TypeError, match='non-empty string'):
        SinkRegistry.register(123, _CustomSink)  # type: ignore[arg-type]


def test_register_overwrites_existing_name() -> None:
    """Re-registering a name swaps the class — documented as the
    plugin-author override mechanism.
    """
    SinkRegistry.register('shared', _CustomSink)
    assert SinkRegistry.get('shared') is _CustomSink

    SinkRegistry.register('shared', _AnotherSink)
    assert SinkRegistry.get('shared') is _AnotherSink


def test_all_names_returns_sorted_list() -> None:
    SinkRegistry._clear()
    SinkRegistry.register('zeta', _CustomSink)
    SinkRegistry.register('alpha', _CustomSink)
    SinkRegistry.register('mid', _CustomSink)

    assert SinkRegistry.all_names() == ['alpha', 'mid', 'zeta']


def test_clear_resets_registry() -> None:
    SinkRegistry.register('x', _CustomSink)
    SinkRegistry._clear()
    assert SinkRegistry.get('x') is None
    assert SinkRegistry.all_names() == []


class _FakeEntryPoint:
    """Stand-in for ``importlib.metadata.EntryPoint`` for tests.

    Mimics the two attributes the registry consults (``name``, ``value``)
    and the ``load`` method. Avoids importing the real EntryPoint, which
    would otherwise require a fully formed entry-points dist on disk.
    """

    def __init__(self, name: str, target: type[BaseSink[Any]] | Exception) -> None:
        self.name = name
        self.value = f'<fake entry point {name}>'
        self._target = target

    def load(self) -> type[BaseSink[Any]]:
        if isinstance(self._target, Exception):
            raise self._target
        return self._target


def test_discover_loads_entry_points_into_registry() -> None:
    SinkRegistry._clear()
    fake_eps = [_FakeEntryPoint('plugin_sink', _CustomSink)]

    def fake_entry_points(*, group: str) -> list[_FakeEntryPoint]:
        assert group == ENTRY_POINT_GROUP
        return fake_eps

    with patch('drakkar.sinks.registry.entry_points', side_effect=fake_entry_points):
        SinkRegistry.discover()

    assert SinkRegistry.get('plugin_sink') is _CustomSink


def test_discover_is_idempotent() -> None:
    """Calling ``discover()`` twice does not double-walk the entry points
    nor duplicate the registration — the second call short-circuits.
    """
    SinkRegistry._clear()
    fake_eps = [_FakeEntryPoint('plugin_sink', _CustomSink)]

    call_count = 0

    def fake_entry_points(*, group: str) -> list[_FakeEntryPoint]:
        nonlocal call_count
        call_count += 1
        return fake_eps

    with patch('drakkar.sinks.registry.entry_points', side_effect=fake_entry_points):
        SinkRegistry.discover()
        SinkRegistry.discover()

    assert call_count == 1
    assert SinkRegistry.get('plugin_sink') is _CustomSink


def test_discover_skips_broken_entry_point_without_crash() -> None:
    """A single ``ep.load()`` raising must not abort the rest of discovery.
    Operators see a structured warning; the worker keeps starting.
    """
    SinkRegistry._clear()
    fake_eps = [
        _FakeEntryPoint('broken', ImportError('missing dep')),
        _FakeEntryPoint('working', _CustomSink),
    ]

    with patch('drakkar.sinks.registry.entry_points', return_value=fake_eps):
        SinkRegistry.discover()

    # Broken plugin skipped, working plugin registered.
    assert SinkRegistry.get('broken') is None
    assert SinkRegistry.get('working') is _CustomSink


def test_discover_skips_entry_point_pointing_at_non_basesink() -> None:
    """An entry point that loads cleanly but yields something that isn't
    a ``BaseSink`` subclass must be skipped, not crash.
    """

    class NotASink:
        pass

    SinkRegistry._clear()
    fake_eps = [
        _FakeEntryPoint('bad', NotASink),  # type: ignore[arg-type]
        _FakeEntryPoint('good', _CustomSink),
    ]

    with patch('drakkar.sinks.registry.entry_points', return_value=fake_eps):
        SinkRegistry.discover()

    assert SinkRegistry.get('bad') is None
    assert SinkRegistry.get('good') is _CustomSink


def test_discover_handles_metadata_failure() -> None:
    """If ``importlib.metadata.entry_points`` itself raises (corrupted
    install, partial uninstall) discovery must mark itself complete and
    return — not propagate the failure to startup.
    """
    SinkRegistry._clear()

    with patch('drakkar.sinks.registry.entry_points', side_effect=RuntimeError('broken metadata')):
        # Must not raise — the worker would refuse to start otherwise.
        SinkRegistry.discover()

    # And it should consider itself "done" so a retry is not attempted on
    # every subsequent SinkManager construction.
    assert SinkRegistry._discovered is True


def test_sink_manager_resolves_registered_type_by_name() -> None:
    """End-to-end: register a custom sink class, instantiate it, hand it
    to SinkManager, and confirm the manager can route to it.

    This is the configuration-driven path operators take: their config
    references a sink type by name, the registry returns the class, the
    framework instantiates it and registers the instance with the
    manager.

    ``entry_points`` is patched so the SinkManager constructor doesn't
    inadvertently load whatever ``drakkar.sinks`` plugins happen to be
    installed in the test environment — any such side effect would
    make this test environment-dependent.
    """
    SinkRegistry.register('custom', _CustomSink)
    # Force the registry's discover() pass to a no-op for SinkManager
    # construction so the test cannot pick up a real installed plugin.
    SinkRegistry._discovered = False
    with patch('drakkar.sinks.registry.entry_points', return_value=[]):
        mgr = SinkManager()

    # The manager exposes the registry lookup so DrakkarApp can resolve
    # config-named sink types without reaching into the registry directly.
    cls = mgr.resolve_sink_class('custom')
    assert cls is _CustomSink

    # Instantiate via the resolved class and verify the manager routes
    # by (sink_type, sink_name) afterwards — this is the same path
    # ``DrakkarApp._build_sinks`` will follow once it consults the
    # registry.
    sink = cls('instance-1')
    mgr.register(sink)

    resolved = mgr.resolve_sink('custom', 'instance-1')
    assert resolved is sink


def test_sink_manager_resolve_sink_class_returns_none_for_unknown() -> None:
    SinkRegistry._discovered = False
    with patch('drakkar.sinks.registry.entry_points', return_value=[]):
        mgr = SinkManager()
    assert mgr.resolve_sink_class('definitely_not_a_real_sink_type') is None


def test_builtin_sinks_are_pre_registered() -> None:
    """Importing ``drakkar.sinks`` registers the built-in types under
    their canonical names. This guards against accidental removal of
    those registrations.
    """
    for name in ('kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'):
        cls = SinkRegistry.get(name)
        assert cls is not None, f'built-in {name!r} sink missing from registry'
        assert issubclass(cls, BaseSink)
