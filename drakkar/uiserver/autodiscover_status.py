"""The answers of ``/api/v1/debug/autodiscover`` and ``/api/v1/debug/autodiscover/leases``.

Pure functions over values the routes read: the registry snapshot (memory
only, no store call), the roster of cluster tasks and the results of the
lease holder reads. No DSN, URL with credentials or token is put in an
answer: the registry header names the connection entry only, and the store
keys are folder-relative paths, schema-qualified rows or key texts.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from drakkar.cluster_tasks import SlotRecord
from drakkar.config.autodiscover import FILESYSTEM_PROVIDER
from drakkar.cronexpr import CronNeverFiresError
from drakkar.fleet.service import PeerView, RegistryStatus
from drakkar.providers.resilience import OUTCOME_SKIPPED_OPEN, CallResult
from drakkar.recorder.lease import LeaseHolder
from drakkar.schedule import ClusterTaskDefinition, next_task_slot, task_scope
from drakkar.timefmt import format_rfc3339_micro

# The most card rows one answer holds; ``truncated`` tells when more were listed.
MAX_CARD_ROWS = 1000

AUTODISCOVER_OFF_ERROR = 'autodiscover is off'

# The ``status`` of a lease row.
LEASE_HELD = 'held'
LEASE_FREE = 'free'
LEASE_NOT_STARTED = 'not_started'
LEASE_BREAKER_OPEN = 'breaker_open'
LEASE_FAILED = 'failed'
LEASE_BUSY = 'busy'

# Card fields sent as the card holds them, in this order.
_CARD_FIELDS = ('worker', 'instance', 'cluster', 'state', 'backend', 'version', 'url', 'started_at', 'heartbeat_at')


def _epoch_text(epoch: float | None) -> str | None:
    return None if epoch is None else format_rfc3339_micro(datetime.fromtimestamp(epoch, UTC))


def display_key(key: str | None, shared_dir: str) -> str | None:
    """A store key as the UI shows it: a path inside the shared folder becomes relative to it.

    The UI sends no absolute host path. The filesystem providers build their
    paths from the same ``storage.shared_dir`` text, some through
    ``os.path.normpath``, so both sides are normalized before the prefix test;
    that is pure text work, and nothing touches the file system, which may be
    a network mount (this runs on the API server's event loop). A key of
    another provider (a row, a Redis or Consul key) is kept as it is.
    """
    if key is None or not shared_dir:
        return key
    prefix = os.path.join(os.path.normpath(shared_dir), '')
    normalized = os.path.normpath(key)
    return normalized[len(prefix) :] if normalized.startswith(prefix) else key


def registry_header(status: RegistryStatus, *, connection: str | None, offline_after_seconds: float) -> dict[str, Any]:
    """The ``registry`` object: the provider, the connection entry's name and the service state."""
    return {
        'provider': status.provider,
        'connection': connection,
        'started': status.started,
        'refreshed_at': _epoch_text(status.refreshed_at),
        'last_publish_at': _epoch_text(status.last_publish_at),
        'breaker_open': status.breaker_open,
        'offline_after_seconds': offline_after_seconds,
    }


def registry_connection(provider: str, connection: str) -> str | None:
    """The connection entry the registry uses; ``None`` for ``filesystem``, which has none."""
    return None if provider == FILESYSTEM_PROVIDER else connection


def card_rows(
    views: Sequence[PeerView],
    *,
    own_cluster: str,
    card_key: Callable[[str], str | None],
    limit: int = MAX_CARD_ROWS,
) -> tuple[list[dict[str, Any]], bool]:
    """The ``workers`` rows: the own cluster first, then by cluster and name; at most ``limit``, and whether more were listed."""
    ordered = sorted(
        views,
        key=lambda view: (
            view.card.get('cluster') != own_cluster,
            str(view.card.get('cluster', '')),
            str(view.card.get('worker', '')),
        ),
    )
    rows: list[dict[str, Any]] = []
    for view in ordered[:limit]:
        card = view.card
        row: dict[str, Any] = {field: card.get(field) for field in _CARD_FIELDS}
        features = card.get('features')
        row.update(
            age_seconds=view.age_seconds,
            online=view.online,
            key=card_key(str(card.get('worker', ''))),
            features=features if isinstance(features, Mapping) else {},
        )
        rows.append(row)
    return rows, len(ordered) > limit


@dataclass(frozen=True)
class LeaseSkeleton:
    """What a lease row shows without a store call: the task, its scope, its lease, its next and last slots."""

    task: str
    scope: str
    system: bool
    lease_seconds: int
    next_slot_ts: int | None
    last_slot: SlotRecord | None


def lease_skeletons(
    definitions: Sequence[ClusterTaskDefinition],
    *,
    cluster: str,
    now: float,
    last_slots: Mapping[str, SlotRecord],
) -> list[LeaseSkeleton]:
    """One skeleton per cluster task, sorted by name."""
    skeletons = []
    for definition in sorted(definitions, key=lambda item: item.name):
        try:
            next_slot: int | None = next_task_slot(now, interval=definition.interval, cron=definition.cron)
        except CronNeverFiresError:
            # A cron expression with no fire time left; the load refuses one, so this is a guard only.
            next_slot = None
        skeletons.append(
            LeaseSkeleton(
                task=definition.name,
                scope=task_scope(cluster, definition.name),
                # A framework task's name carries a dot, which a user task name cannot.
                system='.' in definition.name,
                lease_seconds=definition.lease_seconds,
                next_slot_ts=next_slot,
                last_slot=last_slots.get(definition.name),
            )
        )
    return skeletons


def _holder_object(holder: LeaseHolder, lease_seconds: int) -> dict[str, Any]:
    """The ``holder`` object; the time left is the store's, else the lease length minus the age."""
    expires_in = holder.expires_in_seconds
    if expires_in is None and holder.age_seconds is not None:
        expires_in = lease_seconds - holder.age_seconds
    return {
        'worker': holder.worker,
        'instance': holder.instance,
        'acquired_at': holder.acquired_at,
        'age_seconds': holder.age_seconds,
        'age_approximate': holder.age_approximate,
        'expires_in_seconds': expires_in,
    }


def lease_status(started: bool, result: CallResult | BaseException | None) -> tuple[str, LeaseHolder | None]:
    """The status of one row and the holder, from the holder read (``None`` when no read was made or none answered)."""
    if not started:
        return LEASE_NOT_STARTED, None
    if not isinstance(result, CallResult):
        return LEASE_FAILED, None
    if not result.ok:
        return (LEASE_BREAKER_OPEN if result.outcome == OUTCOME_SKIPPED_OPEN else LEASE_FAILED), None
    if result.value is None:
        return LEASE_FREE, None
    return LEASE_HELD, result.value


def lease_row(skeleton: LeaseSkeleton, *, status: str, holder: LeaseHolder | None, key: str | None) -> dict[str, Any]:
    """One ``leases`` row."""
    last = skeleton.last_slot
    return {
        'task': skeleton.task,
        'scope': skeleton.scope,
        'system': skeleton.system,
        'lease_seconds': skeleton.lease_seconds,
        'next_slot_ts': skeleton.next_slot_ts,
        'last_slot': (
            None if last is None else {'slot': last.slot, 'outcome': last.outcome, 'at': _epoch_text(last.at)}
        ),
        'status': status,
        'holder': None if holder is None else _holder_object(holder, skeleton.lease_seconds),
        'key': key,
    }
