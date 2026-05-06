"""Sink registry — plugin discovery for third-party sink types.

The :class:`SinkRegistry` is a process-wide name → ``BaseSink`` subclass
table. Two populations land in it:

1.  The built-in sinks (Kafka, Postgres, Mongo, Http, Redis, Filesystem)
    register themselves under their canonical ``sink_type`` strings the
    first time the registry is consulted.
2.  Third-party packages declare new sink types via the
    ``[project.entry-points."drakkar.sinks"]`` stanza in their
    ``pyproject.toml``. ``SinkRegistry.discover()`` loads those entry
    points at framework startup (driven from
    :class:`drakkar.sinks.manager.SinkManager`).

Once registered, a sink class can be looked up by name via
``SinkRegistry.get(name)``. The :class:`drakkar.sinks.manager.SinkManager`
calls ``discover()`` once at construction so configuration-driven sink
resolution always sees the latest plugin set.

Why a registry?
    The previous design hardcoded the type→class mapping inside
    ``DrakkarApp._build_sinks``. Adding a sink meant editing the framework.
    The registry keeps that wiring open: a plugin author writes their
    ``BaseSink`` subclass, declares one entry-point line, and the
    framework picks it up automatically without any monkey-patching.
"""

from __future__ import annotations

import inspect
from importlib.metadata import entry_points
from typing import ClassVar

import structlog

from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()

# The entry-point group third-party packages target. Stable, public —
# treat changes here as a breaking API change for plugin authors.
ENTRY_POINT_GROUP = 'drakkar.sinks'


class SinkRegistry:
    """Process-wide registry of sink type names to ``BaseSink`` subclasses.

    All state is class-level (not instance-level): a single registry
    serves the whole process, mirroring the way the importlib metadata
    entry-point table is itself global. Tests reset state between
    cases via :meth:`_clear`.

    Thread safety: the registry is constructed and consulted on the
    asyncio main loop thread. Discovery happens once at startup; reads
    after that are pure dict lookups. We do NOT add a lock because there
    is no concurrent writer in the supported execution model — if you
    register a sink from worker code racing the main loop, that's a
    misuse and would surface as an obvious data race regardless of any
    lock here.
    """

    # Class-level mapping: sink_type name → BaseSink subclass.
    # Populated by ``register`` (manual) and ``discover`` (entry points).
    # ``ClassVar`` because the dict is intentionally shared across the
    # process — instance-level state would defeat the registry's
    # singleton purpose.
    _registered: ClassVar[dict[str, type[BaseSink]]] = {}

    # Tracks whether ``discover()`` has run at least once. Subsequent
    # calls become a no-op for the entry-point scan portion (registrations
    # are still idempotent). Avoids re-walking the metadata table on every
    # ``SinkManager`` construction in tests.
    _discovered: bool = False

    @classmethod
    def register(cls, name: str, sink_cls: type[BaseSink]) -> None:
        """Register ``sink_cls`` under ``name``.

        Args:
            name: The type name used in ``DrakkarConfig.sinks`` to refer
                to this sink. Must be a non-empty, non-whitespace string.
            sink_cls: A concrete subclass of :class:`BaseSink`. Abstract
                base classes are rejected because instantiating one
                would fail at delivery time.

        Raises:
            TypeError: ``name`` is not a string, ``name`` is empty/blank,
                or ``sink_cls`` is not a ``BaseSink`` subclass.

        Re-registering the same name is allowed and silently overwrites —
        this is the documented mechanism for plugins that want to provide
        a drop-in replacement for a built-in sink (e.g., a custom
        ``KafkaSink`` with extra metrics).
        """
        if not isinstance(name, str) or not name.strip():
            raise TypeError(f'sink registry name must be a non-empty string, got {name!r}')

        # ``inspect.isclass`` first so ``issubclass`` does not raise
        # TypeError on non-class inputs (e.g., an instance, ``None``, a
        # module). The error message is identical either way — the caller
        # only cares that what they passed is not a usable sink class.
        if not inspect.isclass(sink_cls) or not issubclass(sink_cls, BaseSink):
            raise TypeError(f'sink class must be a subclass of BaseSink, got {sink_cls!r}')

        cls._registered[name] = sink_cls

    @classmethod
    def discover(cls) -> None:
        """Load and register every entry point in the ``drakkar.sinks`` group.

        Idempotent: calling more than once is safe. The entry-point scan
        is short-circuited on subsequent calls so this is cheap to call
        from every ``SinkManager`` constructor.

        Each entry point's ``load()`` is wrapped in ``try/except``: a
        single broken plugin (missing dependency, import-time error)
        logs a structured warning and is skipped, instead of crashing
        the whole worker. Operators can spot the warning in their log
        aggregator and remove or fix the offending plugin.
        """
        if cls._discovered:
            return

        # ``entry_points(group=...)`` returns an EntryPoints view. In
        # Python 3.10+ the keyword form is the canonical API; older
        # tuple-based behavior was removed in 3.13.
        try:
            eps = entry_points(group=ENTRY_POINT_GROUP)
        except Exception as exc:
            # If the metadata table itself is unreadable (corrupted
            # install, partial uninstall) we can't discover anything,
            # but we MUST NOT crash startup over it — log and move on.
            logger.warning(
                'sink_registry_entry_point_scan_failed',
                category='sink',
                group=ENTRY_POINT_GROUP,
                error=str(exc),
            )
            cls._discovered = True
            return

        for ep in eps:
            try:
                sink_cls = ep.load()
            except Exception as exc:
                logger.warning(
                    'sink_registry_entry_point_load_failed',
                    category='sink',
                    group=ENTRY_POINT_GROUP,
                    name=ep.name,
                    value=getattr(ep, 'value', '<unknown>'),
                    error=str(exc),
                )
                continue

            try:
                cls.register(ep.name, sink_cls)
            except TypeError as exc:
                # The entry point loaded fine but pointed at something
                # that isn't a ``BaseSink`` subclass. Log + skip so the
                # operator can spot the misconfiguration without losing
                # the rest of the plugin set.
                logger.warning(
                    'sink_registry_entry_point_invalid',
                    category='sink',
                    group=ENTRY_POINT_GROUP,
                    name=ep.name,
                    value=getattr(ep, 'value', '<unknown>'),
                    error=str(exc),
                )

        cls._discovered = True

    @classmethod
    def get(cls, name: str) -> type[BaseSink] | None:
        """Return the registered sink class for ``name`` or ``None`` if absent.

        Never raises — returning ``None`` keeps caller logic simple
        (``cls or fallback`` style) and avoids forcing every lookup site
        to wrap in ``try/except KeyError``.
        """
        return cls._registered.get(name)

    @classmethod
    def all_names(cls) -> list[str]:
        """Return all registered type names in sorted order.

        Sorting is stable across calls so log output and debug-UI dumps
        remain deterministic regardless of registration order.
        """
        return sorted(cls._registered)

    @classmethod
    def _clear(cls) -> None:
        """Reset the registry to empty — test-only helper.

        Production code never calls this; the registry is meant to be
        populated once and read many times. Tests use it to keep cases
        independent (one test's registration must not bleed into the
        next).
        """
        cls._registered.clear()
        cls._discovered = False
