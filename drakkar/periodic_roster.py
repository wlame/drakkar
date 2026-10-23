"""The periodic tasks a worker runs: the handler's declared tasks after the config overrides.

The roster is built once at startup, after ``on_startup`` and before any
connection opens, the registry starts or a sink connects. It discovers the
handler's ``@periodic`` methods, applies ``periodic.tasks.<name>``, checks
the merged tasks again, and splits them into worker tasks and cluster tasks.
It also holds the definitions of every enabled cluster task (``history.archive``
included when the worker runs it) and their hash, which the worker publishes
in its registry card from the first publish.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from drakkar.cluster_tasks import archive_task_runs
from drakkar.periodic import (
    PREFIX_OVERRIDE,
    RULE_UNKNOWN_TASK,
    PeriodicMeta,
    check_periodic_rules,
    discover_periodic_tasks,
)
from drakkar.schedule import ARCHIVE_TASK_NAME, DEFAULT_LEASE_SECONDS, ClusterTaskDefinition, cluster_tasks_hash

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig
    from drakkar.config.periodic import PeriodicTaskOverride


class PeriodicConfigError(ValueError):
    """A ``periodic.tasks`` override names no declared task, or makes a declared task break a periodic rule."""


@dataclass(frozen=True)
class RosterTask:
    """One enabled periodic task: its name, its bound method and its settings after the overrides.

    A cluster task also has its ``definition`` (``None`` for a worker task):
    its schedule with whole numbers as ``int`` and its lease filled in (the
    default lease when neither the decorator nor an override gives one).
    Its ``meta`` carries the same interval and lease.
    """

    name: str
    method: Callable[[], Awaitable[Any]]
    meta: PeriodicMeta
    definition: ClusterTaskDefinition | None = None


@dataclass(frozen=True)
class PeriodicRoster:
    """The worker's enabled periodic tasks and the hash of its cluster task definitions.

    ``definitions`` holds the user cluster tasks and, when ``runs_archive``,
    the system task ``history.archive``. ``runs_archive`` is the one place
    that decides whether this worker runs ``history.archive``; the worker
    starts its loop only then.
    """

    worker_tasks: tuple[RosterTask, ...]
    cluster_tasks: tuple[RosterTask, ...]
    runs_archive: bool
    definitions: tuple[ClusterTaskDefinition, ...]
    cluster_hash: str

    @property
    def cluster_task_count(self) -> int:
        """How many cluster tasks this worker runs, ``history.archive`` included."""
        return len(self.definitions)

    @property
    def cluster_task_names(self) -> list[str]:
        """The names of every cluster task this worker runs: the user tasks, then ``history.archive``."""
        return [definition.name for definition in self.definitions]


def archive_definition(config: DrakkarConfig) -> ClusterTaskDefinition:
    """The definition of the system cluster task ``history.archive`` under ``config``."""
    archive = config.history.archive
    return ClusterTaskDefinition(
        name=ARCHIVE_TASK_NAME,
        interval=archive.effective_interval,
        cron=archive.cron,
        lease_seconds=archive.lease_seconds,
    )


def resolve_periodic_roster(handler: object, config: DrakkarConfig, *, now_epoch: int) -> PeriodicRoster:
    """Discover the handler's periodic tasks, apply the overrides and build the roster.

    Overrides are checked in sorted name order, so the first message is the
    same for any key order. For each one: the handler must declare the task
    (``rule_unknown_task``), and the merged task must pass the decorator
    rules again (one cron lookup from ``now_epoch``). An override with
    ``enabled: false`` is checked too, and then leaves the task out.

    Raises:
        PeriodicConfigError: the first override that breaks a rule, with the
            ``periodic.tasks.<name>: `` prefix.
    """
    declared = {name: (method, meta) for name, method, meta in discover_periodic_tasks(handler)}
    overrides = config.periodic.tasks
    merged: dict[str, PeriodicMeta] = {name: meta for name, (_method, meta) in declared.items()}
    for name in sorted(overrides):
        if name not in declared:
            raise PeriodicConfigError(PREFIX_OVERRIDE.format(name=name) + RULE_UNKNOWN_TASK)
        meta = _apply_override(merged[name], overrides[name])
        rule = check_periodic_rules(
            name=name,
            interval=meta.interval,
            cron=meta.cron,
            scope=meta.scope,
            lease_seconds=meta.lease_seconds,
            now_epoch=now_epoch,
        )
        if rule is not None:
            raise PeriodicConfigError(PREFIX_OVERRIDE.format(name=name) + rule)
        merged[name] = meta

    enabled = [name for name in sorted(declared) if name not in overrides or overrides[name].enabled]
    worker_tasks = tuple(
        RosterTask(name=name, method=declared[name][0], meta=merged[name])
        for name in enabled
        if merged[name].scope == 'worker'
    )
    user_definitions = tuple(
        _cluster_definition(name, merged[name]) for name in enabled if merged[name].scope == 'cluster'
    )
    cluster_tasks = tuple(
        _cluster_task(declared[definition.name][0], merged[definition.name], definition)
        for definition in user_definitions
    )
    runs_archive = archive_task_runs(config)
    definitions = user_definitions + ((archive_definition(config),) if runs_archive else ())
    return PeriodicRoster(
        worker_tasks=worker_tasks,
        cluster_tasks=cluster_tasks,
        runs_archive=runs_archive,
        definitions=definitions,
        cluster_hash=cluster_tasks_hash(definitions),
    )


def _apply_override(meta: PeriodicMeta, override: PeriodicTaskOverride) -> PeriodicMeta:
    """``meta`` with the keys ``override`` gives: an interval or a cron replaces the whole schedule."""
    changes: dict[str, Any] = {}
    if override.interval is not None:
        changes.update(interval=override.interval, cron=None)
    elif override.cron is not None:
        changes.update(interval=None, cron=override.cron)
    if override.lease_seconds is not None:
        changes['lease_seconds'] = override.lease_seconds
    return dataclasses.replace(meta, **changes)


def _cluster_task(
    method: Callable[[], Awaitable[Any]], meta: PeriodicMeta, definition: ClusterTaskDefinition
) -> RosterTask:
    """A cluster task of the roster; its meta takes the interval and the lease of its definition."""
    normalised = dataclasses.replace(meta, interval=definition.interval, lease_seconds=definition.lease_seconds)
    return RosterTask(name=definition.name, method=method, meta=normalised, definition=definition)


def _cluster_definition(name: str, meta: PeriodicMeta) -> ClusterTaskDefinition:
    """The definition of a checked user cluster task, as the hash sees it.

    The one conversion of the roster: the rules already accepted the values,
    so an interval and a lease are whole numbers (``30.0`` from a decorator,
    ``600`` read as a float from YAML), and the definition hash needs JSON
    integers. A missing lease is the default lease.
    """
    lease = meta.lease_seconds if meta.lease_seconds is not None else DEFAULT_LEASE_SECONDS
    return ClusterTaskDefinition(
        name=name,
        interval=int(meta.interval) if meta.interval is not None else None,
        cron=meta.cron,
        lease_seconds=int(lease),
    )
