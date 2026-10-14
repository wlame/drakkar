"""Where a worker keeps its files: own storage, the shared folder, and history.

``storage.dir`` holds this worker's own live files; only this worker opens
them. ``shared.dir`` is the folder every worker of a fleet sees: registry
cards, sealed history, archives and merge results. ``history.*`` decides
how sealed recorder files become daily archives and how long archives and
merge results are kept.
"""

from pydantic import BaseModel, Field, model_validator

# Archives are the only recorder artifact nothing else reclaims, so the
# default has to be a horizon rather than "forever": a worker left alone
# on its defaults must not be able to fill the disk it runs on. 30 days
# keeps a month of history queryable.
DEFAULT_ARCHIVE_RETENTION_DAYS = 30


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
            'delete the files. Off, nothing removes sealed files.'
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
    lock_stale_seconds: int = Field(
        default=7200,
        ge=1,
        description=(
            'An archive lock or archive work file older than this many seconds belongs to a dead '
            'archiver: the lock is taken over and the work file is removed.'
        ),
    )

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


class HistoryConfig(BaseModel):
    """What happens to recorder history after a worker is done with a file."""

    archive: HistoryArchiveConfig = Field(default_factory=HistoryArchiveConfig)
    merged_retention_days: int = Field(
        default=32,
        ge=0,
        description='Merge results from the UI are deleted after this many days. 0 = never delete.',
    )
