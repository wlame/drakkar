"""Handler cache configuration, and how the cache exchanges rows with the peers of a worker."""

import structlog
from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel

# Module-scope logger for config-time warnings (field/model validators).
# These fire once per process at config load, so the sync structlog API
# is fine — no coroutine context to await in.
logger = structlog.get_logger()


class CacheExchangeConfig(ConfigModel):
    """How this worker pulls ``cluster`` and ``global`` cache rows from its peers."""

    enabled: bool = Field(
        default=True,
        description=(
            'Pull cluster and global cache rows from peers; the peers come from autodiscover, so with '
            'autodiscover off this does nothing. Off, flush and cleanup still run but nothing propagates '
            'across workers.'
        ),
    )
    interval_seconds: float = Field(default=30.0, gt=0, description='Seconds between exchange cycles.')
    page_size: int = Field(default=500, ge=1, le=1000, description='Maximum rows in one page pulled from a peer.')
    timeout_seconds: float = Field(default=5.0, gt=0, description='Time limit for one request to a peer.')
    # ``None`` means 0.9 x interval_seconds, so one cycle always ends before
    # the next one is scheduled. The 0.1 s floor stops a value so small that
    # no cycle could ever finish.
    cycle_deadline_seconds: float | None = Field(
        default=None,
        ge=0.1,
        description='Hard time limit for one exchange cycle. Empty means 0.9 x interval_seconds.',
    )
    max_concurrent_peers: int = Field(default=4, ge=1, description='Peers pulled from in parallel.')

    @model_validator(mode='after')
    def _validate_deadline_vs_interval(self) -> 'CacheExchangeConfig':
        """An explicit deadline must fire before the next cycle is scheduled."""
        if self.cycle_deadline_seconds is not None and self.cycle_deadline_seconds >= self.interval_seconds:
            raise ValueError(
                f'cache.exchange.cycle_deadline_seconds ({self.cycle_deadline_seconds:g}) must be '
                f'strictly less than cache.exchange.interval_seconds ({self.interval_seconds:g}) — '
                'otherwise the deadline can never fire before the next cycle schedules.'
            )
        return self


class CacheConfig(ConfigModel):
    """Handler-accessible key/value cache, memory-backed with write-behind SQLite.

    When ``enabled=true``, every handler gains a ``self.cache`` attribute for
    sync ``set``/``peek``/``delete``/``__contains__`` and async ``get`` with
    DB fallback. Entries are periodically flushed to this worker's cache file
    in ``storage.dir``; ``exchange`` decides how ``cluster`` and ``global``
    rows reach the other workers.

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
    exchange: CacheExchangeConfig = Field(
        default_factory=CacheExchangeConfig,
        description='How this worker pulls cluster and global cache rows from its peers.',
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
