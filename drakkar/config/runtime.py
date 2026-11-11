"""Runtime configuration: executor, thread pools, metrics, health, logging.

Everything that tunes how a worker runs, as opposed to where its data
comes from (``sources``) or goes (``sinks``).
"""

from typing import Literal

from pydantic import Field, field_validator, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.units import ByteSize, Duration, WholeSeconds
from drakkar.ecs import ecs_namespace_error

EXECUTOR_ON_FAILURE_CHOICES = ('skip', 'dlq')


class ExecutorConfig(ConfigModel):
    """Subprocess executor pool settings.

    ``binary_path`` is optional here — if omitted, each ``ExecutorTask``
    must provide its own ``binary_path`` in ``arrange()``, otherwise the
    task will fail with a clear error.
    """

    binary_path: str | None = Field(
        default=None,
        min_length=1,
        description=(
            'Default binary path for all tasks. None means each ExecutorTask returned by '
            'arrange() must carry its own binary_path, otherwise the task fails with a clear error.'
        ),
    )
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
            'DK_*',  # the worker's own config overrides
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
    max_executors: int = Field(
        default=4,
        ge=1,
        description='Maximum concurrent subprocesses. Tasks beyond this limit wait in a queue.',
    )
    task_timeout: WholeSeconds = Field(
        default=120,
        description=(
            'Wall-clock time limit per subprocess, a whole number of seconds. A process exceeding it is '
            'killed and treated as a failure.'
        ),
    )
    max_stdout: ByteSize = Field(
        default=0,
        description=(
            'Most stdout kept per task. 0 (the default) = no limit. When a '
            'process writes more, the kept prefix is cut at a UTF-8 '
            'character boundary, the rest is read and discarded (the '
            'process is never blocked on a full pipe), and '
            'ExecutorResult.stdout_truncated is set. Useful when '
            'subprocesses can emit very large output.'
        ),
    )
    max_stderr: ByteSize = Field(
        default=0,
        description=(
            'Most stderr kept per task. 0 (the default) = no limit. Same '
            'semantics as max_stdout, applied to the stderr stream; sets '
            'ExecutorResult.stderr_truncated on cut.'
        ),
    )
    window_size: int = Field(
        default=100,
        ge=1,
        description=(
            'Maximum messages collected per arrange() window. Larger windows allow more '
            'batching in arrange(); smaller windows reduce latency.'
        ),
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description=(
            'Maximum retry attempts per failed task (0 = no retries). A task runs at most max_retries + 1 times.'
        ),
    )
    on_failure: Literal['skip', 'dlq'] = Field(
        default='skip',
        description=(
            "What the default on_error returns when a task fails for good: 'skip' drops the "
            "task; 'dlq' writes its source message to the DLQ and ends the message."
        ),
    )
    drain_timeout: WholeSeconds = Field(
        default=30,
        description=(
            'Longest wait for in-flight executor tasks to finish during '
            'graceful shutdown or partition revocation, a whole number of '
            'seconds. Set lower to speed up shutdown; set at least as high '
            'as task_timeout if you rely on clean final commits for every '
            'in-flight task. '
            'When drain times out, the still-running tasks are cancelled — '
            'their subprocess process group is killed so the executor slot '
            'is freed at once — and their offsets are NOT committed (those '
            'messages will replay on restart — at-least-once).'
        ),
    )
    max_pending_tasks: int = Field(
        default=2000,
        ge=1,
        description=(
            'New windows wait while this many tasks are created and not settled (waiting for an '
            'executor slot, running, or in on_task_complete and its delivery). Must be at least '
            'max_executors. A soft limit: a window reserves its estimated tasks when it is taken, '
            'so the limit is passed only by its fan-out beyond the estimate: normally one message, '
            'more when the tasks per message jump.'
        ),
    )
    max_pending_bytes: ByteSize = Field(
        default=1024**3,
        description=(
            'New windows wait while task stdin plus kept task output reach this size, at least 16MiB. '
            'Counts the stdin of pending tasks and the stdout and stderr of their results while '
            'on_task_complete, on_error and the sink delivery run, and while results are kept for '
            'on_message_complete and on_window_complete, so large outputs slow admission. '
            'A soft limit, like max_pending_tasks. '
            'Size it to about a quarter of the container memory.'
        ),
    )

    @field_validator('on_failure', mode='before')
    @classmethod
    def _check_on_failure(cls, value: object) -> object:
        """Refuse any other value with the pinned message, which names the key by its path."""
        if value not in EXECUTOR_ON_FAILURE_CHOICES:
            raise ValueError(f"executor.on_failure must be skip or dlq, got '{value}'")
        return value

    @model_validator(mode='after')
    def _check_pending_tasks(self) -> 'ExecutorConfig':
        """A task limit below the slot count would leave slots idle while windows wait."""
        if self.max_pending_tasks < self.max_executors:
            raise ValueError(
                f'executor.max_pending_tasks ({self.max_pending_tasks}) must be >= '
                f'executor.max_executors ({self.max_executors})'
            )
        return self


class MetricsConfig(ConfigModel):
    """Prometheus metrics settings."""

    enabled: bool = Field(
        default=True,
        description='Serve Prometheus metrics over HTTP.',
    )
    port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description='Port for the Prometheus metrics endpoint.',
    )
    task_label_histograms: list[str] = Field(
        default_factory=list,
        description=(
            'Task label keys whose numeric values are observed into the '
            'drakkar_task_label_value histogram at task completion, one time '
            'series per key (e.g. a file-size or line-count label). Values '
            'that do not parse as finite numbers are skipped.'
        ),
    )


class RuntimeHealthConfig(ConfigModel):
    """Runtime health monitor: event-loop lag tracking and stall introspection.

    A heartbeat task measures how late the runtime wakes it (event-loop
    lag on this backend); a sampler thread captures stack traces of the
    code blocking the loop whenever the heartbeat goes silent for longer
    than ``stall``. Current state and lag history surface on the
    debug UI's Runtime tab, as Prometheus metrics, and as flight-recorder
    events (``runtime_health`` transitions/samples, ``runtime_stall``
    with captured stacks).

    The healthy-path cost per tick is one clock read, one comparison and
    one ring-buffer write — introspection (stack capture, task census)
    only runs during a stall or on an explicit debug-UI request.
    """

    enabled: bool = Field(
        default=True,
        description=(
            'Run the runtime health monitor: heartbeat lag tracking, stall stack sampling, '
            "and the data behind the debug UI's Runtime tab."
        ),
    )
    tick: Duration = Field(
        default=0.25,
        description=(
            'Heartbeat interval. Lag is measured as how late each tick '
            'fires; the sampler thread also checks heartbeat age at this '
            'interval. Smaller values narrow the attribution blind spot '
            'for short blocks at slightly more (still negligible) wakeups.'
        ),
    )
    warn_lag: Duration = Field(
        default=0.1,
        description=(
            "Lag above this marks the runtime 'degraded'. Recovery to "
            "'healthy' needs several consecutive clean ticks (hysteresis), "
            'so a flapping loop does not spam state transitions.'
        ),
    )
    stall: Duration = Field(
        default=1.0,
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
    sample_interval: Duration = Field(
        default=10.0,
        description=(
            'Interval between runtime_health sample events written to the '
            'flight recorder for cross-restart history. The fine-grained '
            'lag sparkline comes from an in-memory ring buffer instead and '
            'costs no database writes.'
        ),
    )
    history_window: WholeSeconds = Field(
        default=900,
        description=(
            'Length of the in-memory lag history ring buffer (one max/avg '
            'aggregate per second) served to the debug UI sparkline, a '
            'whole number of seconds and at least 1m.'
        ),
    )
    max_episode: Duration = Field(
        default=300.0,
        description=(
            'Maximum length of one lag episode, at least 10s. An episode spans the time '
            'the runtime is degraded or stalled; on recovery (or at this '
            'cap, for incidents that outlive it) the monitor writes one '
            'runtime_lag_episode event with aggregated stacks and a '
            'verdict — blocked, cpu_bound, starved, or inconclusive.'
        ),
    )
    stack_probe_enabled: bool = Field(
        default=False,
        description=(
            'Opt-in flight-recorder profiler: the sampler thread records a '
            "runtime_probe event with the runtime thread's stack every "
            'stack_probe_interval, regardless of health state. Useful for '
            'tuning production workloads and post-incident analysis; off by '
            'default because it writes events for as long as the worker runs.'
        ),
    )
    stack_probe_interval: Duration = Field(
        default=1.0,
        description='Time between runtime_probe events while stack_probe_enabled is on.',
    )


IO_THREADS_MAX = 512
OFFLOAD_THREADS_MAX = 32
IO_THREADS_MESSAGE = f'threads.io must be null or from 1 to {IO_THREADS_MAX}'
OFFLOAD_THREADS_MESSAGE = f'threads.offload must be null or from 1 to {OFFLOAD_THREADS_MAX}'


class ThreadsConfig(ConfigModel):
    """The two thread pools of the worker: blocking I/O and offloaded CPU work.

    ``io`` is asyncio's default ``to_thread`` executor. Every
    ``asyncio.to_thread(...)`` / ``loop.run_in_executor(None, ...)`` call —
    handler filesystem reads, plus the framework's own background file work
    (archive passes, database-stats scans) — shares this one process-wide
    pool. Python sizes it ``min(32, cpu_count + 4)``, so a many-core host is
    capped at 32 concurrent blocking operations; under slow storage each
    blocked call holds a thread for its full wall time and later calls queue
    behind it.

    ``offload`` backs ``BaseDrakkarHandler.offload()``: a handler hook that
    needs to run a heavy synchronous computation (deeply nested loops
    deriving task parameters in ``arrange()``, result crunching in
    ``on_message_complete``) awaits ``self.offload(fn, ...)`` and the
    function runs on this pool instead of the event loop. The pool exists to
    keep the loop responsive, not to add CPU throughput — under the GIL,
    pure-Python work is serialized regardless of thread count. See the
    Threads docs page for the full map.
    """

    io: int | None = Field(
        default=None,
        description=(
            "Worker threads in asyncio's default to_thread executor, from 1 to "
            f"{IO_THREADS_MAX}. Null (the default) keeps Python's own sizing, "
            'min(32, cpu_count + 4). Set an explicit value when handler I/O '
            'concurrency is capped by the pool — e.g. many-core hosts doing '
            'wide blocking filesystem fan-out. Blocking I/O releases the GIL, '
            'so large values are legitimate here (unlike threads.offload); '
            'the trade-off is pressure on the storage behind the calls.'
        ),
    )
    offload: int | None = Field(
        default=None,
        description=(
            f'Worker threads in the shared offload pool, from 1 to {OFFLOAD_THREADS_MAX}. '
            'Null (the default) sizes the pool from the executor pool: '
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

    @field_validator('io')
    @classmethod
    def _check_io(cls, value: int | None) -> int | None:
        """Refuse a size outside 1 to 512 with the pinned message; null derives it."""
        if value is not None and not 1 <= value <= IO_THREADS_MAX:
            raise ValueError(IO_THREADS_MESSAGE)
        return value

    @field_validator('offload')
    @classmethod
    def _check_offload(cls, value: int | None) -> int | None:
        """Refuse a size outside 1 to 32 with the pinned message; null derives it."""
        if value is not None and not 1 <= value <= OFFLOAD_THREADS_MAX:
            raise ValueError(OFFLOAD_THREADS_MESSAGE)
        return value


class LoggingConfig(ConfigModel):
    """Structured logging settings."""

    level: str = Field(
        default='INFO',
        description='Log level: DEBUG, INFO, WARNING, ERROR, or CRITICAL.',
    )
    format: str = Field(
        default='json',
        pattern='^(json|ecs|console)$',
        description=(
            "Output format. 'json' produces machine-readable structured logs; "
            "'ecs' produces JSON in the Elastic Common Schema, with the framework's own fields under 'drakkar'; "
            "'console' produces colorized human-readable output for local development."
        ),
    )
    output: str = Field(
        default='stderr',
        description=(
            'Log output destination. "stderr" (default) or "stdout" for standard streams, '
            'or a file path for file output. File paths support template variables: '
            '{worker_id}, {cluster_name}. Example: "/var/log/drakkar/{worker_id}.log"'
        ),
    )
    min_task_duration: Duration = Field(
        default=0.5,
        description=(
            'Shortest task duration that emits a slow_task_completed / slow_task_failed log line. 0 logs all tasks.'
        ),
    )
    ecs_namespace: str = Field(
        default='drakkar',
        description=(
            "With format 'ecs': the object that holds every field that is not an ECS field, for "
            'example your product name. A lower-case identifier of at most 64 characters, and not '
            'an ECS field set (host, error, event, ...).'
        ),
    )
    ecs_request_id_at_root: bool = Field(
        default=False,
        description=(
            "With format 'ecs': also write request_id at the root of each line, next to the copy "
            'in the ecs_namespace object.'
        ),
    )

    @field_validator('ecs_namespace')
    @classmethod
    def _check_ecs_namespace(cls, value: str) -> str:
        error = ecs_namespace_error(value)
        if error is not None:
            raise ValueError(error)
        return value
