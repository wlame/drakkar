"""Where a worker keeps its files: own storage, the shared folder, and history.

``storage.dir`` holds this worker's own live files; only this worker opens
them. ``shared.dir`` is the folder every worker of a fleet sees: registry
cards, sealed history, archives and merge results. ``history.*`` decides
how sealed recorder files become daily archives, when the archive slots come
(the ``history.archive`` cluster task), and how long archives and merge
results are kept.
"""

import time

from pydantic import BaseModel, Field, model_validator

from drakkar.cronexpr import validate_cron
from drakkar.periodic import (
    PREFIX_ARCHIVE,
    RULE_ARCHIVE_INTERVAL,
    RULE_AT_MOST_ONE,
    RULE_LEASE_BOUNDS,
    RULE_SWEEP_AFTER,
)
from drakkar.schedule import (
    DEFAULT_LEASE_SECONDS,
    MAX_LEASE_SECONDS,
    MIN_CLUSTER_INTERVAL_SECONDS,
    MIN_LEASE_SECONDS,
)

# Archives are the only recorder artifact nothing else reclaims, so the
# default has to be a horizon rather than "forever": a worker left alone
# on its defaults must not be able to fill the disk it runs on. 30 days
# keeps a month of history queryable.
DEFAULT_ARCHIVE_RETENTION_DAYS = 30

# The archive pass runs once an hour unless history.archive gives an interval or a cron.
DEFAULT_ARCHIVE_INTERVAL_SECONDS = 3600
DEFAULT_SWEEP_AFTER_SECONDS = 7200
MIN_SWEEP_AFTER_SECONDS = 60


class StorageConfig(BaseModel):
    """This worker's own files: the live recorder file, its backlog, the cache file, the crash marker."""

    dir: str = Field(
        default='/tmp/drakkar',
        description=(
            "Folder for this worker's own files (live recorder file, cache file, crash marker). "
            'Only this worker opens them. The worker creates it with mode 0700. Empty runs '
            'memory-only: no own files, no history, no cache file.'
        ),
    )


class SharedConfig(BaseModel):
    """The folder every worker of one fleet sees."""

    dir: str = Field(
        default='',
        description=(
            'Folder that every worker of the fleet sees (for example a ReadWriteMany volume): '
            'registry cards, sealed history, daily archives and merge results. Empty means the '
            'same folder as storage.dir, which is right for workers on one host.'
        ),
    )


class HistoryArchiveConfig(BaseModel):
    """Daily per-cluster archives built from sealed recorder files."""

    enabled: bool = Field(
        default=True,
        description=(
            'Fold sealed recorder files into one compressed archive per cluster and window, then '
            'delete the files. Off, nothing removes sealed files. Used only with ui.recorder.persistence sqlite; '
            'with none the history.archive keys load and are ignored.'
        ),
    )
    window_hours: int = Field(
        default=24,
        ge=1,
        description='Width of one archive window in hours; must be >= ui.recorder.rotation_interval_hours.',
    )
    due_after_hours: int = Field(
        default=48,
        ge=1,
        description=(
            'A window is archived this many hours after it ends, so late files of that window are '
            'still in it; must be >= ui.recorder.rotation_interval_hours.'
        ),
    )
    retention_days: int = Field(
        default=DEFAULT_ARCHIVE_RETENTION_DAYS,
        ge=0,
        description=(
            f'How long archives are kept before deletion. Defaults to {DEFAULT_ARCHIVE_RETENTION_DAYS} '
            f'days so a worker cannot fill its disk unattended. 0 = keep archives forever (a startup '
            f'warning names the choice); any other value must cover the due delay plus one window '
            f'(retention_days * 24 >= due_after_hours + window_hours).'
        ),
    )
    interval: int | None = Field(
        default=None,
        description=(
            'Seconds between archive slots, aligned to the epoch, a whole number of at least '
            f'{MIN_CLUSTER_INTERVAL_SECONDS}. Give at most one of interval and cron; with neither, a '
            f'slot every {DEFAULT_ARCHIVE_INTERVAL_SECONDS} seconds. At each slot one worker of the '
            'cluster runs an archive pass.'
        ),
    )
    cron: str | None = Field(
        default=None,
        description=(
            'Five-field UTC cron expression for the archive slots, instead of interval. Give at most '
            'one of interval and cron.'
        ),
    )
    lease_seconds: int = Field(
        default=DEFAULT_LEASE_SECONDS,
        description=(
            'Lease of the archive slot in seconds, a whole number from '
            f'{MIN_LEASE_SECONDS} to {MAX_LEASE_SECONDS}. The holder refreshes it while its pass runs; '
            'a lease that nobody refreshed for this long is taken over.'
        ),
    )
    sweep_after_seconds: int = Field(
        default=DEFAULT_SWEEP_AFTER_SECONDS,
        description=(
            'An archive work file older than this many seconds belongs to a dead archiver and is '
            f'removed. At least {MIN_SWEEP_AFTER_SECONDS}.'
        ),
    )

    @property
    def effective_interval(self) -> int | None:
        """The interval of the archive slots: ``None`` with a cron, else ``interval``, else the default."""
        if self.cron is not None:
            return None
        return self.interval if self.interval is not None else DEFAULT_ARCHIVE_INTERVAL_SECONDS

    @model_validator(mode='after')
    def _validate_schedule(self) -> 'HistoryArchiveConfig':
        """Check the schedule, the lease and the sweep age; defined first, so it runs before the retention check."""
        message = _archive_rule(self, now_epoch=int(time.time()))
        if message is not None:
            raise ValueError(PREFIX_ARCHIVE + message)
        return self

    @model_validator(mode='after')
    def _validate_retention_vs_due(self) -> 'HistoryArchiveConfig':
        """Retention must outlive the delay before a window is archived.

        A window is archived ``due_after_hours`` after it ends, so the
        archive of a just-due window already describes data that old plus
        the window itself. A shorter retention would delete that archive in
        the pass that created it.
        """
        if self.retention_days and self.retention_days * 24 < self.due_after_hours + self.window_hours:
            default_note = (
                f' The value is the {DEFAULT_ARCHIVE_RETENTION_DAYS}-day default, so raise it (or set 0 to keep '
                f'archives forever) to go with a window this wide.'
                if self.retention_days == DEFAULT_ARCHIVE_RETENTION_DAYS
                else ''
            )
            raise ValueError(
                f'history.archive.retention_days ({self.retention_days}) must be 0 (keep archives forever) '
                f'or cover the due delay plus one window: retention_days * 24 must be >= '
                f'history.archive.due_after_hours ({self.due_after_hours}) + '
                f'history.archive.window_hours ({self.window_hours}).{default_note}'
            )
        return self


def _archive_rule(archive: HistoryArchiveConfig, *, now_epoch: int) -> str | None:
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
    if not MIN_LEASE_SECONDS <= archive.lease_seconds <= MAX_LEASE_SECONDS:
        return RULE_LEASE_BOUNDS
    if archive.sweep_after_seconds < MIN_SWEEP_AFTER_SECONDS:
        return RULE_SWEEP_AFTER
    return None


class HistoryConfig(BaseModel):
    """What happens to recorder history after a worker is done with a file."""

    archive: HistoryArchiveConfig = Field(default_factory=HistoryArchiveConfig)
    merged_retention_days: int = Field(
        default=32,
        ge=0,
        description='Merge results from the UI are deleted after this many days. 0 = never delete.',
    )
