"""Handler cache configuration, and how the cache exchanges rows with the peers of a worker."""

import structlog
from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.units import Duration, seconds_text

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
    interval: Duration = Field(default=30.0, description='Time between exchange cycles.')
    page_size: int = Field(default=500, ge=1, le=1000, description='Maximum rows in one page pulled from a peer.')
    timeout: Duration = Field(default=5.0, description='Time limit for one request to a peer.')
    # ``None`` means 0.9 x interval, so one cycle always ends before the next
    # one is scheduled. The 100ms floor stops a value so small that no cycle
    # could ever finish.
    cycle_deadline: Duration | None = Field(
        default=None,
        description='Hard time limit for one exchange cycle, at least 100ms. Null means 0.9 x interval.',
    )
    max_concurrent_peers: int = Field(default=4, ge=1, description='Peers pulled from in parallel.')

    @model_validator(mode='after')
    def _validate_deadline_vs_interval(self) -> 'CacheExchangeConfig':
        """An explicit deadline must fire before the next cycle is scheduled."""
        if self.cycle_deadline is not None and self.cycle_deadline >= self.interval:
            raise ValueError(
                f'cache.exchange.cycle_deadline ({seconds_text(self.cycle_deadline)}) must be '
                f'strictly less than cache.exchange.interval ({seconds_text(self.interval)}) — '
                'otherwise the deadline can never fire before the next cycle schedules.'
            )
        return self


class CacheConfig(ConfigModel):
    """Handler-accessible key/value cache, memory-backed with write-behind SQLite.

    When ``enabled=true``, every handler gains a ``self.cache`` attribute for
    sync ``set``/``peek``/``delete``/``__contains__`` and async ``get`` with
    DB fallback. Entries are periodically flushed to this worker's cache file
    in its own folder ``<storage.dir>/<cluster>/<worker>/``; ``exchange`` decides how ``cluster`` and ``global``
    rows reach the other workers.

    ``max_memory_entries`` defaults to ``10_000`` to prevent unbounded
    growth under write-heavy workloads; the in-memory dict uses LRU
    eviction and falls through to the DB on miss — the DB is the source
    of truth, so eviction never loses data. ``0`` removes the cap
    (unbounded cache); the config load logs a warning so that choice is
    visible in logs.

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
    flush_interval: Duration = Field(
        default=3.0,
        description='Time between write-behind flushes of dirty entries to SQLite.',
    )
    cleanup_interval: Duration = Field(
        default=60.0,
        description='Time between cleanup passes deleting expired rows and refreshing DB-size gauges.',
    )
    max_memory_entries: int = Field(
        default=10_000,
        ge=0,
        description=(
            'Cap for in-memory LRU entries. Default 10_000 prevents unbounded '
            'growth under write-heavy workloads. 0 = no cap, with a startup warning.'
        ),
    )
    exchange: CacheExchangeConfig = Field(
        default_factory=CacheExchangeConfig,
        description='How this worker pulls cluster and global cache rows from its peers.',
    )

    @model_validator(mode='after')
    def _warn_if_unbounded(self) -> 'CacheConfig':
        """Emit a startup warning when ``max_memory_entries`` is 0, which removes the cap.

        Fires once at config load rather than inside ``CacheEngine.start()`` —
        the engine's ``start`` can be called multiple times in tests and on
        rotation, and the worker-id context the engine had is irrelevant for
        a choice that lives in the config itself. We only warn when the cache
        is actually enabled (otherwise the setting has no effect).
        """
        if self.enabled and self.max_memory_entries == 0:
            logger.warning(
                'cache_max_memory_entries_unbounded',
                category='cache',
                reason='cache.max_memory_entries=0 configured — memory is unbounded, monitor RSS under load',
            )
        return self
