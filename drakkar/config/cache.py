"""Cross-worker cache configuration, including peer sync."""

import structlog
from pydantic import BaseModel, Field, model_validator

# Module-scope logger for config-time warnings (field/model validators).
# These fire once per process at config load, so the sync structlog API
# is fine — no coroutine context to await in.
logger = structlog.get_logger()


class CachePeerSyncConfig(BaseModel):
    """Peer-sync settings for the handler cache.

    Controls the periodic loop that pulls entries from sibling workers' cache
    DBs. Disable by setting ``enabled=false`` — flush/cleanup continue to run,
    but no cross-worker propagation happens.

    ``peer_resolution_cache_seconds`` is intentionally NOT exposed as a knob
    in v1 (hardcoded 300s). Operators rarely change cluster_name at runtime,
    so exposing it would add config surface without real benefit. YAGNI —
    expose later if someone actually needs it.
    """

    enabled: bool = True
    interval_seconds: float = Field(default=30.0, gt=0)
    batch_size: int = Field(default=500, ge=1)
    timeout_seconds: float = Field(default=5.0, gt=0)
    # ``cycle_deadline_seconds`` is a hard wall-clock cap on one peer-sync
    # cycle. Without this bound, a single slow peer (NFS lag, disk
    # contention, unresponsive remote) could keep ``_sync_once`` in flight
    # indefinitely and starve the periodic task. ``None`` => default to
    # ``interval_seconds * 0.9`` so the deadline is always strictly less
    # than the gap between invocations. Minimum floor of 0.1s prevents
    # operators from accidentally setting a value so tight that normal
    # syncs can never complete.
    cycle_deadline_seconds: float | None = Field(
        default=None,
        ge=0.1,
        description=(
            'Hard wall-clock cap on one peer-sync cycle. When None, defaults '
            'to interval_seconds * 0.9. Prevents a single slow peer from '
            'starving the periodic task.'
        ),
    )

    @model_validator(mode='after')
    def _validate_deadline_vs_interval(self) -> 'CachePeerSyncConfig':
        """Reject explicit ``cycle_deadline_seconds >= interval_seconds``.

        The deadline's whole point is to cap a single cycle short of the next
        tick so the periodic task never overlaps itself. If the operator
        configures a deadline that's greater than or equal to the interval,
        the cap can't fire before the next invocation schedules — in that
        case the operator probably misread the fields or intended a much
        larger interval. Fail loudly at config load so the misconfiguration
        surfaces before any data flows.
        """
        if self.cycle_deadline_seconds is not None and self.cycle_deadline_seconds >= self.interval_seconds:
            raise ValueError(
                f'cache.peer_sync.cycle_deadline_seconds ({self.cycle_deadline_seconds}) must be '
                f'strictly less than cache.peer_sync.interval_seconds ({self.interval_seconds}) — '
                'otherwise the deadline can never fire before the next cycle schedules.'
            )
        return self


class CacheConfig(BaseModel):
    """Handler-accessible key/value cache, memory-backed with write-behind SQLite.

    When ``enabled=true``, every handler gains a ``self.cache`` attribute for
    sync ``set``/``peek``/``delete``/``__contains__`` and async ``get`` with
    DB fallback. Entries are periodically flushed to ``<worker>-cache.db``
    under ``db_dir`` (falls back to ``debug.db_dir`` when empty) and
    optionally pulled from sibling workers via the peer-sync loop.

    ``max_memory_entries`` defaults to ``10_000`` to prevent unbounded
    growth under write-heavy workloads; the in-memory dict uses LRU
    eviction and falls through to the DB on miss — the DB is the source
    of truth, so eviction never loses data. Explicit ``None`` disables
    the cap (unbounded cache); the engine emits a warning at startup so
    that choice is visible in logs.

    Gating rules (warn-and-continue, not fail-at-startup):
    - ``enabled=true`` but no ``db_dir`` anywhere → warning + effective-disable
    - ``peer_sync.enabled=true`` but ``debug.store_config=false`` → peer sync
      silently disabled (autodiscovery needs ``store_config``)
    """

    enabled: bool = False
    # empty → engine init falls back to debug.db_dir. Kept empty in config layer
    # so the config is pure data — resolution happens when the engine spins up.
    db_dir: str = ''
    flush_interval_seconds: float = Field(default=3.0, gt=0)
    cleanup_interval_seconds: float = Field(default=60.0, gt=0)
    # Cap for in-memory LRU entries. Default 10_000 prevents unbounded growth
    # under write-heavy workloads. Set to None for explicitly unbounded cache;
    # the engine will warn at startup so operators see the intentional choice.
    max_memory_entries: int | None = Field(
        default=10_000,
        ge=1,
        description=(
            'Cap for in-memory LRU entries. Default 10_000 prevents unbounded '
            'growth under write-heavy workloads. Set to None for explicitly '
            'unbounded cache.'
        ),
    )
    peer_sync: CachePeerSyncConfig = Field(default_factory=CachePeerSyncConfig)

    @model_validator(mode='after')
    def _warn_if_unbounded(self) -> 'CacheConfig':
        """Emit a startup warning when ``max_memory_entries`` is explicitly unbounded.

        Fires once at config load rather than inside ``CacheEngine.start()`` —
        the engine's ``start`` can be called multiple times in tests and on
        rotation, and the worker-id context the engine had is irrelevant for
        a choice that lives in the config itself. We only warn when the cache
        is actually enabled (otherwise the setting has no effect).
        """
        if self.enabled and self.max_memory_entries is None:
            logger.warning(
                'cache_max_memory_entries_unbounded',
                category='cache',
                reason='cache.max_memory_entries=None configured — memory is unbounded, monitor RSS under load',
            )
        return self
