"""Runtime configuration: executor, metrics, throughput, health, IO, offload, logging.

Everything that tunes how a worker runs, as opposed to where its data
comes from (``kafka``) or goes (``sinks``).
"""

from pydantic import BaseModel, Field


class ExecutorConfig(BaseModel):
    """Subprocess executor pool settings.

    ``binary_path`` is optional here — if omitted, each ``ExecutorTask``
    must provide its own ``binary_path`` in ``arrange()``, otherwise the
    task will fail with a clear error.
    """

    binary_path: str | None = Field(default=None, min_length=1)
    env: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Environment variables passed to all executor subprocesses. '
            'Merged on top of the (filtered) parent process environment. '
            'Per-task env vars from ExecutorTask.env override these on conflict.'
        ),
    )
    env_inherit_parent: bool = Field(
        default=True,
        description=(
            'When True, the parent process env is passed to subprocesses '
            '(with deny patterns applied — see env_inherit_deny). Set False '
            'to run subprocesses with ONLY ExecutorConfig.env + '
            'ExecutorTask.env — fully isolated from parent env.'
        ),
    )
    env_inherit_deny: list[str] = Field(
        default_factory=lambda: [
            'DK_*',  # framework internals (SINKS__, KAFKA__, DEBUG__, ...)
            '*PASSWORD*',
            '*PASSWD*',
            '*SECRET*',
            '*TOKEN*',
            '*_KEY',
            '*_DSN',
            '*CREDENTIAL*',
            '*SALT*',
        ],
        description=(
            'Case-insensitive fnmatch patterns against parent env var names. '
            'Matching vars are NOT inherited by subprocesses, even when '
            'env_inherit_parent is True. Deliberately narrower than the '
            'recorder redaction list: a pattern here WITHHOLDS the variable '
            'from your binary, so names with common non-secret uses '
            '(AUTH_SERVICE_URL, CERT_PATH, PRIVATE_SUBNET) are excluded. '
            'Set to [] to fully trust the parent environment.'
        ),
    )
    max_executors: int = Field(default=4, ge=1)
    task_timeout_seconds: int = Field(default=120, ge=1)
    max_stdout_bytes: int = Field(
        default=0,
        ge=0,
        description=(
            'Maximum bytes of stdout retained per task. 0 (the default) = '
            'unlimited. When a process writes more, the retained prefix is '
            'cut at a UTF-8 character boundary, the rest is read and '
            'discarded (the process is never blocked on a full pipe), and '
            'ExecutorResult.stdout_truncated is set. Useful when '
            'subprocesses can emit very large output.'
        ),
    )
    max_stderr_bytes: int = Field(
        default=0,
        ge=0,
        description=(
            'Maximum bytes of stderr retained per task. 0 (the default) = '
            'unlimited. Same semantics as max_stdout_bytes, applied to the '
            'stderr stream; sets ExecutorResult.stderr_truncated on cut.'
        ),
    )
    window_size: int = Field(default=100, ge=1)
    max_retries: int = Field(default=3, ge=0)
    drain_timeout_seconds: int = Field(
        default=30,
        ge=1,
        description=(
            'Maximum seconds to wait for in-flight executor tasks to finish '
            'during graceful shutdown or partition revocation. Set lower to '
            'speed up shutdown; set at least as high as task_timeout_seconds '
            'if you rely on clean final commits for every in-flight task. '
            'When drain times out, offsets for in-flight tasks are NOT '
            'committed (those messages will replay on restart — at-least-once).'
        ),
    )
    backpressure_high_multiplier: int = Field(default=32, ge=1)
    backpressure_low_multiplier: int = Field(default=4, ge=1)


class MetricsConfig(BaseModel):
    """Prometheus metrics settings."""

    enabled: bool = True
    port: int = Field(default=9090, ge=1, le=65535)
    task_label_histograms: list[str] = Field(
        default_factory=list,
        description=(
            'Task label keys whose numeric values are observed into the '
            'drakkar_task_label_value histogram at task completion, one time '
            'series per key (e.g. a file-size or line-count label). Values '
            'that do not parse as finite numbers are skipped.'
        ),
    )


class ThroughputConfig(BaseModel):
    """Opt-in task cost tracking: per-task speed and windowed throughput.

    The operator names a numeric task label (set in ``arrange()``) whose
    value correlates with the task's computational hardness — file size, a
    computed score, any unit. The framework then derives per-task ``speed``
    (cost / duration) and sliding-window ``throughput`` aggregates (1, 5,
    30, 60, 300 s), surfaced in task events, the recent-tasks API, a
    per-second WebSocket frame, Prometheus, and worker_state snapshots.

    Purely observational: nothing schedules or prioritizes on cost.
    """

    cost_label: str = Field(
        default='',
        description=(
            'Task label key whose numeric value is the task cost — a '
            'number correlating with computational hardness (bytes, a '
            'computed score, any unit). Empty (the default) disables the '
            'whole feature. Values that do not parse as finite numbers '
            'leave the task uncounted.'
        ),
    )
    min_cost: float = Field(
        default=0.0,
        ge=0,
        description=(
            'Smallest cost worth counting. Tasks below it carry no speed '
            'and enter no aggregate — useful when fixed overhead dominates '
            'small tasks and their speeds would mislead.'
        ),
    )


class RuntimeHealthConfig(BaseModel):
    """Runtime health monitor: event-loop lag tracking and stall introspection.

    A heartbeat task measures how late the runtime wakes it (event-loop
    lag on this backend); a sampler thread captures stack traces of the
    code blocking the loop whenever the heartbeat goes silent for longer
    than ``stall_seconds``. Current state and lag history surface on the
    debug UI's Runtime tab, as Prometheus metrics, and as flight-recorder
    events (``runtime_health`` transitions/samples, ``runtime_stall``
    with captured stacks).

    The healthy-path cost per tick is one clock read, one comparison and
    one ring-buffer write — introspection (stack capture, task census)
    only runs during a stall or on an explicit debug-UI request.
    """

    enabled: bool = True
    tick_seconds: float = Field(
        default=0.25,
        gt=0.01,
        description=(
            'Heartbeat interval. Lag is measured as how late each tick '
            'fires; the sampler thread also checks heartbeat age at this '
            'interval. Smaller values narrow the attribution blind spot '
            'for short blocks at slightly more (still negligible) wakeups.'
        ),
    )
    warn_lag_seconds: float = Field(
        default=0.1,
        gt=0,
        description=(
            "Lag above this marks the runtime 'degraded'. Recovery to "
            "'healthy' needs several consecutive clean ticks (hysteresis), "
            'so a flapping loop does not spam state transitions.'
        ),
    )
    stall_seconds: float = Field(
        default=1.0,
        gt=0,
        description=(
            "Heartbeat age above this marks the runtime 'stalled' and starts "
            'stack sampling: the sampler thread captures what the runtime '
            'thread is executing until the heartbeat resumes. Each stall '
            'becomes one runtime_stall recorder event with the stacks.'
        ),
    )
    max_stall_stacks: int = Field(
        default=10,
        ge=1,
        description=(
            'Maximum distinct stack traces captured per stall. Repeated '
            'samples of the same location collapse into one entry with a '
            'count; further distinct stacks past the cap are dropped.'
        ),
    )
    sample_interval_seconds: float = Field(
        default=10.0,
        gt=0,
        description=(
            'Interval between runtime_health sample events written to the '
            'flight recorder for cross-restart history. The fine-grained '
            'lag sparkline comes from an in-memory ring buffer instead and '
            'costs no database writes.'
        ),
    )
    history_window_seconds: int = Field(
        default=900,
        ge=60,
        description=(
            'Length of the in-memory lag history ring buffer (one max/avg '
            'aggregate per second) served to the debug UI sparkline.'
        ),
    )
    episode_max_seconds: float = Field(
        default=300.0,
        ge=10,
        description=(
            'Maximum length of one lag episode. An episode spans the time '
            'the runtime is degraded or stalled; on recovery (or at this '
            'cap, for incidents that outlive it) the monitor writes one '
            'runtime_lag_episode event with aggregated stacks and a '
            'verdict — blocked, cpu_bound, starved, or inconclusive.'
        ),
    )
    probe_interval_seconds: float = Field(
        default=0.0,
        ge=0,
        description=(
            'Opt-in flight-recorder profiler: when above 0, the sampler '
            'thread records a runtime_probe event with the runtime '
            "thread's stack every interval, regardless of health state. "
            'Useful for tuning production workloads and post-incident '
            'analysis; 0 (the default) disables it because it writes '
            'events for as long as the worker runs.'
        ),
    )


class IOConfig(BaseModel):
    """Blocking-I/O thread pool: asyncio's default ``to_thread`` executor.

    Every ``asyncio.to_thread(...)`` / ``loop.run_in_executor(None, ...)``
    call — handler filesystem reads, plus the framework's own background
    file work (archive passes, database-stats scans) — shares ONE
    process-wide pool: asyncio's default executor. Python sizes it
    ``min(32, cpu_count + 4)``, so a many-core host is capped at 32
    concurrent blocking operations no matter what; under slow storage each
    blocked call holds a thread for its full wall time and later calls
    queue invisibly behind it.

    This section makes that pool's size a first-class knob. Distinct from
    ``offload.max_threads``, which sizes the separate dedicated pool
    behind ``handler.offload()`` (CPU-bound work) — see the Threads docs
    page for the full map.
    """

    max_threads: int = Field(
        default=0,
        ge=0,
        le=512,
        description=(
            "Worker threads in asyncio's default to_thread executor. 0 "
            "(the default) keeps Python's own sizing, min(32, cpu_count "
            '+ 4). Set an explicit value when handler I/O concurrency is '
            'capped by the pool — e.g. many-core hosts doing wide '
            'blocking filesystem fan-out. Blocking I/O releases the GIL, '
            'so large values are legitimate here (unlike '
            'offload.max_threads); the trade-off is pressure on the '
            'storage behind the calls.'
        ),
    )


class OffloadConfig(BaseModel):
    """Thread pool for CPU-bound work handlers move off the event loop.

    Backs ``BaseDrakkarHandler.offload()``: a handler hook that needs to
    run a heavy synchronous computation (deeply nested loops deriving task
    parameters in ``arrange()``, result crunching in ``on_message_complete``)
    awaits ``self.offload(fn, ...)`` and the function runs on this pool
    instead of the event loop. The pool exists to keep the loop responsive,
    not to add CPU throughput — under the GIL, pure-Python work is
    serialized regardless of thread count.
    """

    max_threads: int = Field(
        default=0,
        ge=0,
        le=32,
        description=(
            'Worker threads in the shared offload pool. 0 (the default) '
            'sizes the pool automatically from the executor pool: '
            'ceil(executor.max_executors / 4), with a minimum of 2 — e.g. pool '
            '8 -> 2 threads, 9 -> 3, 13 -> 4. Calls beyond this many '
            'concurrent offloaded computations queue (FIFO) and show up '
            'in the drakkar_offload_queued gauge. More threads do not '
            'speed up pure-Python work (GIL); set an explicit value only '
            'when several partitions routinely offload at once and '
            'queueing delay matters, or when the offloaded code releases '
            'the GIL (numpy, compiled extensions).'
        ),
    )


class LoggingConfig(BaseModel):
    """Structured logging settings."""

    level: str = 'INFO'
    format: str = Field(default='json', pattern='^(json|console)$')
    output: str = Field(
        default='stderr',
        description=(
            'Log output destination. "stderr" (default) or "stdout" for standard streams, '
            'or a file path for file output. File paths support template variables: '
            '{worker_id}, {cluster_name}. Example: "/var/log/drakkar/{worker_id}.log"'
        ),
    )
