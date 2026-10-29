"""The periodic tasks a worker runs: the tasks the handler declares with ``@periodic``.

The roster is built once at startup, after ``on_startup`` and before any
connection opens, the registry starts or a sink connects. It discovers the
handler's ``@periodic`` methods and splits them into worker tasks and cluster tasks.
It also holds the definitions of every enabled cluster task (``history.archive``
included when the worker runs it as a cluster task) and their hash, which the
worker publishes in its registry card from the first publish.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from drakkar.cluster_tasks import ARCHIVE_SCOPE_CLUSTER, ArchiveScope, archive_scope
from drakkar.periodic import PeriodicMeta, discover_periodic_tasks
from drakkar.schedule import ARCHIVE_TASK_NAME, DEFAULT_LEASE_SECONDS, ClusterTaskDefinition, cluster_tasks_hash

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig


@dataclass(frozen=True)
class RosterTask:
    """One periodic task: its name, its bound method and its settings.

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

    ``definitions`` holds the user cluster tasks and, when ``archive_scope``
    is ``cluster``, the system task ``history.archive``. ``archive_scope`` is
    the one place that decides how this worker runs ``history.archive``: as a
    cluster task, over its own files with no election and no hash entry
    (``worker``), or not at all (``None``).
    """

    worker_tasks: tuple[RosterTask, ...]
    cluster_tasks: tuple[RosterTask, ...]
    archive_scope: ArchiveScope | None
    definitions: tuple[ClusterTaskDefinition, ...]
    cluster_hash: str

    @property
    def cluster_task_count(self) -> int:
        """How many cluster tasks this worker runs, ``history.archive`` included when it is one."""
        return len(self.definitions)

    @property
    def cluster_task_names(self) -> list[str]:
        """The names of every cluster task this worker runs: the user tasks, then ``history.archive`` when it is one."""
        return [definition.name for definition in self.definitions]


def archive_definition(config: DrakkarConfig) -> ClusterTaskDefinition:
    """The definition of the system cluster task ``history.archive`` under ``config``."""
    archive = config.recorder.archive
    return ClusterTaskDefinition(
        name=ARCHIVE_TASK_NAME,
        interval=archive.effective_interval,
        cron=archive.cron,
        lease_seconds=archive.lease,
    )


def resolve_periodic_roster(handler: object, config: DrakkarConfig) -> PeriodicRoster:
    """Discover the handler's periodic tasks and build the roster.

    The decorator already checked every declared task, so nothing is refused here.
    """
    declared = {name: (method, meta) for name, method, meta in discover_periodic_tasks(handler)}
    names = sorted(declared)
    worker_tasks = tuple(
        RosterTask(name=name, method=declared[name][0], meta=declared[name][1])
        for name in names
        if declared[name][1].scope == 'worker'
    )
    user_definitions = tuple(
        _cluster_definition(name, declared[name][1]) for name in names if declared[name][1].scope == 'cluster'
    )
    cluster_tasks = tuple(
        _cluster_task(declared[definition.name][0], declared[definition.name][1], definition)
        for definition in user_definitions
    )
    scope = archive_scope(config)
    definitions = user_definitions + ((archive_definition(config),) if scope == ARCHIVE_SCOPE_CLUSTER else ())
    return PeriodicRoster(
        worker_tasks=worker_tasks,
        cluster_tasks=cluster_tasks,
        archive_scope=scope,
        definitions=definitions,
        cluster_hash=cluster_tasks_hash(definitions),
    )


def _cluster_task(
    method: Callable[[], Awaitable[Any]], meta: PeriodicMeta, definition: ClusterTaskDefinition
) -> RosterTask:
    """A cluster task of the roster; its meta takes the interval and the lease of its definition."""
    normalised = dataclasses.replace(meta, interval=definition.interval, lease_seconds=definition.lease_seconds)
    return RosterTask(name=definition.name, method=method, meta=normalised, definition=definition)


def _cluster_definition(name: str, meta: PeriodicMeta) -> ClusterTaskDefinition:
    """The definition of a checked user cluster task, as the hash sees it.

    The one conversion of the roster: the rules already accepted the values,
    so an interval and a lease are whole numbers (``30.0`` from a decorator),
    and the definition hash needs JSON integers. A missing lease is the default lease.
    """
    lease = meta.lease_seconds if meta.lease_seconds is not None else DEFAULT_LEASE_SECONDS
    return ClusterTaskDefinition(
        name=name,
        interval=int(meta.interval) if meta.interval is not None else None,
        cron=meta.cron,
        lease_seconds=int(lease),
    )
