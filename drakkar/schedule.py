"""Slots of cluster periodic tasks, their election scopes and the definition hash.

Every worker of a cluster computes the same slots for the same moment: an
interval slot is a whole multiple of the interval since 1970-01-01T00:00:00Z,
a cron slot is the next fire time of the expression in UTC. Both are epoch
seconds, computed from the wall clock truncated to the whole second.

A slot runs on one worker of the cluster: the one that takes the lease on the
scope ``<sanitized cluster>/<task name>`` from the election provider. The
definition hash lets workers find a peer of their cluster that runs other
cluster task definitions.

This module depends on the standard library and :mod:`drakkar.cronexpr`
only, so every layer (config, recorder, election providers) can import it.
``tests/fixtures/periodic_contract.json`` pins the slots, scopes and hashes.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass

from drakkar.cronexpr import next_cron_fire

# Seconds after the slot during which a worker may still take the lease.
ACQUIRE_WINDOW_SECONDS = 10
# Seconds the winner holds the lease at least, also for a run that ends at once.
MIN_HOLD_SECONDS = 20
DEFAULT_LEASE_SECONDS = 60
MIN_LEASE_SECONDS = 40
MAX_LEASE_SECONDS = 86_400
MIN_CLUSTER_INTERVAL_SECONDS = 30

ARCHIVE_TASK_NAME = 'history.archive'
# Tasks the framework declares itself; their names carry a dot, which a user task name cannot.
SYSTEM_TASK_NAMES: tuple[str, ...] = ('cache.flush', 'cache.cleanup', 'cache.sync', ARCHIVE_TASK_NAME)

TASK_NAME_PATTERN = r'[a-z_][a-z0-9_]{0,63}'
# A scope is matched as a whole string (``fullmatch``): ``$`` would accept a trailing newline.
TASK_SCOPE_PATTERN = rf'[A-Za-z0-9_-]+/{TASK_NAME_PATTERN}(\.{TASK_NAME_PATTERN})?'
_TASK_SCOPE = re.compile(TASK_SCOPE_PATTERN)

SCOPE_INVALID = "election scope '{scope}' is not <cluster>/<task>"

# Folder and scope prefix for an empty or missing cluster name.
DEFAULT_CLUSTER = 'default'
_UNSAFE_CLUSTER_CHARS = re.compile(r'[^a-zA-Z0-9_-]')


def sanitize_cluster(name: str | None) -> str:
    """Return a cluster name safe to embed in a folder or file name.

    Everything outside ``[a-zA-Z0-9_-]`` becomes ``_``; an empty or
    missing name becomes :data:`DEFAULT_CLUSTER`.
    """
    if not name:
        return DEFAULT_CLUSTER
    return _UNSAFE_CLUSTER_CHARS.sub('_', name)


def next_interval_slot(now: float, interval: int) -> int:
    """The first slot of ``interval`` seconds strictly after ``now``, aligned to the epoch."""
    second = math.floor(now)
    return (second // interval + 1) * interval


def next_cron_slot(now: float, expr: str) -> int:
    """The first fire time of ``expr`` strictly after ``now`` truncated to the second.

    Raises :class:`drakkar.cronexpr.CronNeverFiresError` when there is none.
    """
    return next_cron_fire(expr, math.floor(now))


def task_scope(cluster: str, name: str) -> str:
    """The election scope of task ``name`` in ``cluster``: ``<sanitized cluster>/<name>``."""
    return f'{sanitize_cluster(cluster)}/{name}'


def parse_task_scope(scope: str) -> tuple[str, str]:
    """Split a scope into its sanitized cluster and its task name.

    Raises ``ValueError`` with :data:`SCOPE_INVALID` when ``scope`` is not
    ``<cluster>/<task>``: the cluster part is sanitized, the task part a task
    name (a system name may carry one dot).
    """
    if _TASK_SCOPE.fullmatch(scope) is None:
        raise ValueError(SCOPE_INVALID.format(scope=scope))
    cluster, name = scope.split('/', 1)
    return cluster, name


@dataclass(frozen=True)
class ClusterTaskDefinition:
    """What a worker runs as one cluster task: its name, one schedule and its lease."""

    name: str
    interval: int | None
    cron: str | None
    lease_seconds: int

    def __post_init__(self) -> None:
        """Refuse a definition with no schedule, two schedules or a non-integer number."""
        if (self.interval is None) == (self.cron is None):
            raise ValueError(f"cluster task '{self.name}' needs exactly one of interval and cron")
        for key, value in (('interval', self.interval), ('lease_seconds', self.lease_seconds)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise TypeError(f"cluster task '{self.name}': {key} must be an int, got {value!r}")


def _definition_object(definition: ClusterTaskDefinition) -> dict[str, int | str]:
    """The JSON object of one definition: its name, its one schedule key and its lease."""
    schedule: dict[str, int | str] = (
        {'interval': definition.interval} if definition.interval is not None else {'cron': definition.cron or ''}
    )
    return {'name': definition.name, **schedule, 'lease_seconds': definition.lease_seconds}


def canonical_definitions(defs: Iterable[ClusterTaskDefinition]) -> bytes:
    """The canonical bytes of a set of definitions: a JSON array sorted by name.

    Serialized like the registry card: sorted keys, no spaces, raw UTF-8.
    The cron string goes in as written; the accepted subset allows exactly
    one space between fields, so it needs no normalizing.
    """
    objects = [_definition_object(d) for d in sorted(defs, key=lambda d: d.name)]
    return json.dumps(objects, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def cluster_tasks_hash(defs: Iterable[ClusterTaskDefinition]) -> str:
    """The first 16 hex characters of the SHA-256 of :func:`canonical_definitions`."""
    return hashlib.sha256(canonical_definitions(defs)).hexdigest()[:16]
