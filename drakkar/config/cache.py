"""Handler cache configuration (exchange with peers lives in ``fleet.cache_exchange``)."""

import structlog
from pydantic import BaseModel, Field, model_validator

# Module-scope logger for config-time warnings (field/model validators).
# These fire once per process at config load, so the sync structlog API
# is fine — no coroutine context to await in.
logger = structlog.get_logger()


class CacheConfig(BaseModel):
    """Handler-accessible key/value cache, memory-backed with write-behind SQLite.

    When ``enabled=true``, every handler gains a ``self.cache`` attribute for
    sync ``set``/``peek``/``delete``/``__contains__`` and async ``get`` with
    DB fallback. Entries are periodically flushed to this worker's cache file
    in ``storage.dir``; ``fleet.cache_exchange`` decides how ``cluster`` and
    ``global`` rows reach the other workers.

    ``max_memory_entries`` defaults to ``10_000`` to prevent unbounded
    growth under write-heavy workloads; the in-memory dict uses LRU
    eviction and falls through to the DB on miss — the DB is the source
    of truth, so eviction never loses data. Explicit ``None`` disables
    the cap (unbounded cache); the engine emits a warning at startup so
    that choice is visible in logs.

    With ``storage.dir: ""`` there is no cache file: the engine warns at
    startup and disables itself.
    """

    enabled: bool = Field(
        default=False,
        description=(
            'Give every handler a self.cache key/value store, memory-backed with '
            'write-behind SQLite. Off, the handler cache API is absent.'
        ),
    )
    flush_interval_seconds: float = Field(
        default=3.0,
        gt=0,
        description='Seconds between write-behind flushes of dirty entries to SQLite.',
    )
    cleanup_interval_seconds: float = Field(
        default=60.0,
        gt=0,
        description='Seconds between cleanup passes deleting expired rows and refreshing DB-size gauges.',
    )
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
