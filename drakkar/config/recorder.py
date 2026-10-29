"""The flight recorder: what it captures, where it keeps it, and how long.

``recorder.enabled`` is durable recording, in the store ``recorder.provider``
names. The memory store is separate: it exists to serve the UI, so it follows
``ui.enabled``. With the recorder off and the UI on, the UI shows the memory
window; with the UI off and the recorder on, rows go straight to the durable
store. The block of a provider loads and does nothing when another provider,
or none, is in use.
"""

import re
import time
from typing import Literal

from pydantic import Field, field_validator, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.resilience import ResilienceConfig
from drakkar.config.units import ByteSize, Duration, WholeDays, WholeHours, WholeSeconds, format_duration
from drakkar.cronexpr import validate_cron
from drakkar.periodic import (
    PREFIX_ARCHIVE,
    RULE_ARCHIVE_INTERVAL,
    RULE_ARCHIVE_LEASE,
    RULE_AT_MOST_ONE,
    RULE_SWEEP_AFTER,
)
from drakkar.schedule import (
    DEFAULT_LEASE_SECONDS,
    MAX_LEASE_SECONDS,
    MIN_CLUSTER_INTERVAL_SECONDS,
    MIN_LEASE_SECONDS,
)

Persistence = Literal['none', 'sqlite', 'postgres']
PERSISTENCE_NONE: Persistence = 'none'
PERSISTENCE_SQLITE: Persistence = 'sqlite'
PERSISTENCE_POSTGRES: Persistence = 'postgres'

MIB = 1024 * 1024
HOURS_PER_DAY = 24
SECONDS_PER_HOUR = 3600

# The recorder table prefix: empty, or a lower-case letter and up to 23 more
# lower-case letters, digits or underscores. Matched with ``re.fullmatch``
# (a ``$`` anchor would accept a trailing newline).
TABLE_PREFIX_RE = r'^[a-z][a-z0-9_]{0,23}$'
TABLE_PREFIX_MESSAGE = (
    "recorder.postgres.table_prefix '{value}' must be empty, or a lower-case letter followed by "
    'at most 23 lower-case letters, digits or underscores'
)

# Archives are the only recorder artifact nothing else reclaims, so the
# default has to be a horizon rather than "forever": a worker left alone
# on its defaults must not be able to fill the disk it runs on. 30 days
# keeps a month of history queryable.
DEFAULT_ARCHIVE_RETENTION_DAYS = 30

# The archive pass runs once an hour unless recorder.archive gives an interval or a cron.
DEFAULT_ARCHIVE_INTERVAL_SECONDS = 3600
DEFAULT_SWEEP_AFTER_SECONDS = 7200
MIN_SWEEP_AFTER_SECONDS = 60


def duration_of_hours(hours: int) -> str:
    """A whole number of hours as a message prints it: the canonical duration text."""
    return format_duration(hours * SECONDS_PER_HOUR * 1000)


class RecorderMemoryConfig(ConfigModel):
    """Bounds of the in-memory store, which holds the recent events for the UI."""

    window: WholeSeconds = Field(
        default=120,
        ge=10,
        le=86400,
        description='Keep recorder events in memory for this long.',
    )
    rows_per_executor: int = Field(
        default=3000,
        ge=100,
        le=1_000_000,
        description=(
            'Row cap per executor slot: the store holds at most this many rows times executor.max_executors. '
            'The cap does not scale with the window; a longer window needs a larger value '
            '(rows per executor per window is about 4 x window / average task seconds).'
        ),
    )
    max_size: ByteSize = Field(
        default=256 * MIB,
        ge=16 * MIB,
        le=512 * MIB,
        description=(
            'Budget for the data in the memory store (pages in use), from 16MiB to 512MiB. It bounds the data, '
            'not the memory the store holds; free pages and one write stage can exceed it. At most 512MiB '
            'so the store stays under the 1 GiB limit of the SQLite in-memory database.'
        ),
    )


class RecorderCaptureConfig(ConfigModel):
    """What the recorder keeps, whichever store takes it."""

    events: bool = Field(
        default=True,
        description=(
            'Write processing events. Off, no event row goes anywhere, runtime health events included: '
            'the live WebSocket view still streams, but History, Trace, and task queries return nothing.'
        ),
    )
    config: bool = Field(
        default=True,
        description=(
            'Write the worker config snapshot to the worker_config table, so every recorder '
            'file names the worker and cluster that wrote it (history, merges, archives).'
        ),
    )
    output: bool = Field(
        default=True,
        description='Keep subprocess stdout/stderr content in task events. Off drops output content from all of them.',
    )
    stdin: bool = Field(
        default=False,
        description=(
            "Keep each task's stdin content (capped at max_stdin) in the task_started event metadata, so the "
            'debug UI can show exactly what a task consumed. Off by default: on a high-fan-out workload stdin '
            'is the largest payload the recorder would write. Failed tasks always keep their stdin (capped) '
            'on the task_failed event, regardless of this flag.'
        ),
    )
    max_stdin: ByteSize = Field(
        default=65536,
        ge=0,
        description=(
            'Size cap for kept stdin content (stdin, and the always-on failed-task capture). 0 = no limit. '
            'A cut is flagged as stdin_truncated in the event metadata.'
        ),
    )
    min_event_duration: Duration = Field(
        default=0,
        ge=0,
        description='Shortest task duration whose task event is written at all. 0 writes everything.',
    )
    min_output_duration: Duration = Field(
        default=0.5,
        ge=0,
        description=(
            'Shortest task duration whose args/stdout/stderr go into the written event. '
            'Faster tasks keep their row without output data.'
        ),
    )
    env_vars: list[str] = Field(
        default_factory=list,
        description=(
            'Environment variable names captured into the worker_config table — for recording '
            'deployment metadata like GIT_SHA or K8S_POD_NAME.'
        ),
    )


class RecorderAnnotationsConfig(ConfigModel):
    """Diagnostic records a handler attaches to a window, message, or task from inside a hook.

    They are ordinary rows in the events table. The two size caps guard different
    resources and are deliberately not one setting: ``max_size`` refuses a single
    unreasonable payload, while ``max_size_per_call`` bounds what one hook invocation
    can add in total — without it, a handler annotating every message of a wide
    window can flood the events table with low-value rows.
    """

    enabled: bool = Field(
        default=True,
        description='Accept handler annotations (self.annotate(...)) and write them to the events table.',
    )
    max_size: ByteSize = Field(
        default=16_384,
        ge=0,
        description='Size cap for one annotation payload; larger payloads are dropped whole. 0 = no limit.',
    )
    max_size_per_call: ByteSize = Field(
        default=262_144,
        ge=0,
        description='Total annotation size one hook invocation may write; further payloads are dropped. 0 = no limit.',
    )
    # Log lines usually ship to a metered aggregator, so an uncapped copy can
    # cost more than the row it replaced.
    max_log_size: ByteSize = Field(
        default=2048,
        ge=0,
        description='Cap on the payload copy in the warning log when an annotation is dropped. 0 = no limit.',
    )


class RecorderSqliteConfig(ConfigModel):
    """The recorder files of provider ``sqlite``."""

    rotation_interval: WholeHours = Field(
        default=1,
        ge=1,
        description='How often to roll over to a new SQLite file, a whole number of hours.',
    )
    # On each failure the batch goes back to the front of the file queue so the
    # next tick retries it; after this many attempts it is discarded, so a
    # persistent DB outage cannot hold the queue for ever.
    max_flush_retries: int = Field(
        default=3,
        ge=1,
        description=(
            'Consecutive failed flush attempts tolerated per batch before it is dropped '
            '(counted in drakkar_recorder_flush_batches_dropped_total).'
        ),
    )
    state_snapshots: bool = Field(
        default=True,
        description=(
            'Periodically snapshot worker counters (uptime, partitions, pool utilization, '
            'queue depth) to the worker_state table.'
        ),
    )
    state_interval: WholeSeconds = Field(
        default=10,
        ge=1,
        description=(
            'Time between worker_state snapshots and between resource samples, a whole number of seconds; '
            'also the heartbeat cadence worker liveness is judged by. Without provider sqlite, resource '
            'samples come every 10s.'
        ),
    )


class RecorderArchiveConfig(ConfigModel):
    """Archives built from sealed recorder files; used only with provider ``sqlite``."""

    enabled: bool = Field(
        default=True,
        description=(
            'Fold sealed recorder files into one compressed archive per window, then delete the files. '
            'The archive covers the whole cluster with autodiscover on and storage.shared_dir set, else '
            "only this worker's files. Off, nothing removes sealed files."
        ),
    )
    window: WholeHours = Field(
        default=24,
        ge=1,
        description='Width of one archive window, a whole number of hours; must be >= recorder.sqlite.rotation_interval.',
    )
    due_after: WholeHours = Field(
        default=48,
        ge=1,
        description=(
            'A window is archived this long after it ends, so late files of that window are '
            'still in it; a whole number of hours, >= recorder.sqlite.rotation_interval.'
        ),
    )
    retention: WholeDays = Field(
        default=DEFAULT_ARCHIVE_RETENTION_DAYS,
        ge=0,
        description=(
            f'How long archives are kept before deletion, a whole number of days. Defaults to '
            f'{DEFAULT_ARCHIVE_RETENTION_DAYS} days so a worker cannot fill its disk unattended. 0 = keep '
            'archives for ever (a startup warning names the choice); any other value must cover due_after plus '
            'one window.'
        ),
    )
    merged_retention: WholeDays = Field(
        default=32,
        ge=0,
        description='The archive pass deletes merge results from the UI older than this. 0 = never.',
    )
    interval: WholeSeconds | None = Field(
        default=None,
        description=(
            'Time between archive slots, aligned to the epoch, a whole number of seconds and at least '
            f'{MIN_CLUSTER_INTERVAL_SECONDS}s. Give at most one of interval and cron; with neither, a '
            'slot every 1h. At each slot one worker of the cluster runs an archive pass, or each worker '
            'runs one over its own files.'
        ),
    )
    cron: str | None = Field(
        default=None,
        description=(
            'Five-field UTC cron expression for the archive slots, instead of interval. Give at most '
            'one of interval and cron.'
        ),
    )
    lease: WholeSeconds = Field(
        default=DEFAULT_LEASE_SECONDS,
        description=(
            f'Lease of the archive slot, a whole number of seconds from {MIN_LEASE_SECONDS} to '
            f'{MAX_LEASE_SECONDS}. The holder refreshes it while its pass runs; a lease that nobody '
            "refreshed for this long is taken over. Not used by a pass over a worker's own files."
        ),
    )
    sweep_after: WholeSeconds = Field(
        default=DEFAULT_SWEEP_AFTER_SECONDS,
        description=(
            'An archive work file older than this belongs to a dead archiver and is removed. '
            f'At least {MIN_SWEEP_AFTER_SECONDS}s.'
        ),
    )

    @property
    def effective_interval(self) -> int | None:
        """The interval of the archive slots: ``None`` with a cron, else ``interval``, else the default."""
        if self.cron is not None:
            return None
        return self.interval if self.interval is not None else DEFAULT_ARCHIVE_INTERVAL_SECONDS

    @model_validator(mode='after')
    def _validate_schedule(self) -> 'RecorderArchiveConfig':
        """Check the schedule, the lease and the sweep age; defined first, so it runs before the retention check."""
        message = _archive_rule(self, now_epoch=int(time.time()))
        if message is not None:
            raise ValueError(PREFIX_ARCHIVE + message)
        return self

    @model_validator(mode='after')
    def _validate_retention_vs_due(self) -> 'RecorderArchiveConfig':
        """Retention must outlive the delay before a window is archived.

        A window is archived ``due_after`` after it ends, so the archive of a
        just-due window already describes data that old plus the window itself.
        A shorter retention would delete that archive in the pass that created it.
        """
        if self.retention and self.retention * HOURS_PER_DAY < self.due_after + self.window:
            default_note = (
                f' The value is the {DEFAULT_ARCHIVE_RETENTION_DAYS}-day default, so raise it (or set 0 to keep '
                f'archives for ever) to go with a window this wide.'
                if self.retention == DEFAULT_ARCHIVE_RETENTION_DAYS
                else ''
            )
            raise ValueError(
                f'recorder.archive.retention ({duration_of_hours(self.retention * HOURS_PER_DAY)}) must be 0 '
                f'(keep archives for ever) or cover the due delay plus one window: it must be >= '
                f'recorder.archive.due_after ({duration_of_hours(self.due_after)}) + '
                f'recorder.archive.window ({duration_of_hours(self.window)}).{default_note}'
            )
        return self


def _archive_rule(archive: RecorderArchiveConfig, *, now_epoch: int) -> str | None:
    """The first schedule rule ``archive`` breaks, without prefix, or ``None``.

    Order: at most one schedule, the cron check, the interval, the lease
    bounds, the sweep age. A given ``0`` and a given empty cron count as given.
    """
    if archive.interval is not None and archive.cron is not None:
        return RULE_AT_MOST_ONE
    if archive.cron is not None:
        message = validate_cron(archive.cron, now_epoch=now_epoch)
        if message is not None:
            return message
    if archive.interval is not None and archive.interval < MIN_CLUSTER_INTERVAL_SECONDS:
        return RULE_ARCHIVE_INTERVAL
    if not MIN_LEASE_SECONDS <= archive.lease <= MAX_LEASE_SECONDS:
        return RULE_ARCHIVE_LEASE
    if archive.sweep_after < MIN_SWEEP_AFTER_SECONDS:
        return RULE_SWEEP_AFTER
    return None


class RecorderPostgresConfig(ConfigModel):
    """The table of provider ``postgres``, which holds the history of all workers.

    The DSN and the schema come from ``connections.postgres.<connection>``.
    """

    connection: str = Field(
        default='main',
        description='Name of the entry under connections.postgres that holds the DSN and the schema.',
    )
    table_prefix: str = Field(
        default='',
        description=(
            'Empty for the table <schema>.recorder_events, or a lower-case letter followed by at most 23 '
            'lower-case letters, digits or underscores for <schema>.<prefix>_recorder_events.'
        ),
    )
    initial_retention: WholeHours = Field(
        default=24,
        ge=1,
        le=168,
        description=(
            'How long the table keeps history, a whole number of hours from 1h to 168h. Read only when this '
            'worker creates the table; after that the value stored with the table rules. At most 168h: one '
            'partition per hour, and a trace search visits every partition.'
        ),
    )
    store_completed_output: bool = Field(
        default=False,
        description='Send the stdout and stderr columns to Postgres. Off, both columns are written as NULL.',
    )
    max_output: ByteSize = Field(
        default=65536,
        ge=0,
        description='Longest stdout or stderr value written to the table. 0 = no limit.',
    )
    max_size: ByteSize = Field(
        default=0,
        ge=0,
        description='Largest total size of the table. 0 = no limit.',
    )
    batch_rows: int = Field(
        default=5000,
        ge=100,
        le=50000,
        description='Most rows in one COPY.',
    )
    max_queue: ByteSize = Field(
        default=128 * MIB,
        ge=16 * MIB,
        le=1024 * MIB,
        description=(
            'Most text waiting for Postgres, from 16MiB to 1GiB, counted in characters of text. '
            'When full, the oldest rows are dropped and counted.'
        ),
    )
    resilience: ResilienceConfig = Field(
        default_factory=ResilienceConfig,
        description='Timeout, retries, backoff and breaker of the recorder calls to Postgres.',
    )

    @field_validator('table_prefix')
    @classmethod
    def _check_table_prefix(cls, value: str) -> str:
        """Refuse a prefix that is not empty and not a short lower-case identifier."""
        if value and not re.fullmatch(TABLE_PREFIX_RE, value):
            raise ValueError(TABLE_PREFIX_MESSAGE.format(value=value))
        return value


class RecorderSearchConfig(ConfigModel):
    """Budget of one history search of the Trace page (``trace?history=1``)."""

    max_files: int = Field(
        default=256,
        ge=1,
        description='Most sealed history files one history search opens, newest first. Used only with provider sqlite.',
    )
    timeout: Duration = Field(
        default=10.0,
        gt=0,
        description=(
            'Time limit for one history search. With provider sqlite the search stops and returns what it '
            'found. With provider postgres the limit is capped at 1h, and a search past it answers 503 and '
            'returns no rows.'
        ),
    )


class RecorderConfig(ConfigModel):
    """The flight recorder, with or without the UI; see the module docstring."""

    enabled: bool = Field(
        default=True,
        description=(
            'Durable recording in the store provider names. Off, nothing is written to a file or a table; '
            'with the UI on, the recorder still holds the recent events in memory for it.'
        ),
    )
    provider: Literal['sqlite', 'postgres'] = Field(
        default='sqlite',
        description=(
            'Where durable recording goes: sqlite writes recorder files into storage.dir, which must be set; '
            'postgres writes a table named by the postgres block, and storage.dir may be empty.'
        ),
    )
    flush_interval: WholeSeconds = Field(
        default=5,
        ge=1,
        description=(
            'Time between recorder flushes, a whole number of seconds: the tick of the memory stage and of '
            'the file or Postgres stage.'
        ),
    )
    max_buffer_rows: int = Field(
        default=50_000,
        ge=1000,
        description=(
            'Rows the intake holds, and the file or Postgres queue too. When full, the oldest are evicted and '
            'counted in drakkar_recorder_dropped_events_total.'
        ),
    )
    memory: RecorderMemoryConfig = Field(default_factory=RecorderMemoryConfig)
    capture: RecorderCaptureConfig = Field(default_factory=RecorderCaptureConfig)
    annotations: RecorderAnnotationsConfig = Field(default_factory=RecorderAnnotationsConfig)
    sqlite: RecorderSqliteConfig = Field(default_factory=RecorderSqliteConfig)
    archive: RecorderArchiveConfig = Field(default_factory=RecorderArchiveConfig)
    postgres: RecorderPostgresConfig = Field(default_factory=RecorderPostgresConfig)
    search: RecorderSearchConfig = Field(default_factory=RecorderSearchConfig)
