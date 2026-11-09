"""The live state of ``GET /api/v1/agent/state``, read from the readers the worker already has.

Nothing here measures anything new. The parts the main loop owns (the pending
budget, the partition loops, the sink stats and the recorder memory store) are
read in one coroutine on the main loop through the bounded dispatch of the API
server; a main loop that does not answer in time gives those parts ``null``.
The other parts are read where the request runs, from values that are safe to
read from any thread: the runtime health monitor (it answers during a stall),
the recorder's status and the registry's last peer snapshot.

Payload text (the stderr tail and the args of a failed task) is read only when
``agent.include_payloads`` is true, and only into ``recent_failures``.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from drakkar.agent.metrics_view import json_number
from drakkar.agent.params import NO_RECORDER_STORE_NOTE
from drakkar.agent.registry import dumps
from drakkar.agent.rules import DLQ_MESSAGES_METRIC, DROPPED_EVENTS_METRIC, DROPPED_POSTGRES_ROWS_METRIC
from drakkar.cluster_tasks import card_cluster_hash
from drakkar.config.recorder import PERSISTENCE_NONE
from drakkar.metrics import (
    consumer_lag,
    recorder_dropped_events,
    recorder_postgres_dropped_rows,
    runtime_stalls,
    sink_dlq_messages,
)
from drakkar.recorder.queries import read_memory
from drakkar.uiserver.databases_status import recorder_block
from drakkar.uiserver.server import dispatch_bounded

if TYPE_CHECKING:
    from prometheus_client.metrics import MetricWrapperBase

    from drakkar.app import DrakkarApp
    from drakkar.config import DrakkarConfig

STATE_ENDPOINT = '/api/v1/agent/state'
MAX_RECENT_FAILURES = 20
STDERR_TAIL_BYTES = 2048
HEALTH_OFF = 'off'
TASK_FAILED_EVENT = 'task_failed'

PAUSED_BY_BACKPRESSURE = 'backpressure'
PAUSED_BY_DEBUG = 'debug'
PAUSED_BY_DEATH = 'death'

# The parts read on the main loop; the others are read where the request runs.
# A loop part is ``None`` when the main loop did not answer in time.
LOOP_PARTS = frozenset({'budget', 'partitions', 'sinks', 'recent_failures'})

# The bound of the compact JSON text of the ``args`` of one failure.
ARGS_JSON_BYTES = 2048

# The columns of a failed task; the payload columns are read only with
# ``agent.include_payloads``. ``error_kind`` and ``offset`` (the smallest
# source offset) come out of the metadata in SQL, so the stored task input in
# the metadata is never read. Only the last bytes of stderr are read.
_FAILURE_COLUMNS = (
    'id, ts, "partition", task_id, exit_code, duration, '
    "CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.kind') END AS error_kind, "
    'CASE WHEN json_valid(metadata) THEN '
    '(SELECT MIN(value) FROM json_each(metadata, \'$.source_offsets\')) END AS "offset"'
)
_PAYLOAD_COLUMNS = f', substr(CAST(stderr AS BLOB), -{STDERR_TAIL_BYTES}) AS stderr, args'


# --- health -------------------------------------------------------------


def health_part(monitor: Any, *, uptime_seconds: float, stalls_total: float) -> dict[str, Any]:
    """The ``health`` part: the monitor's brief view, or state ``off`` with no monitor."""
    if monitor is None:
        view: Mapping[str, Any] = {
            'state': HEALTH_OFF,
            'heartbeat_age_ms': None,
            'current_lag_ms': None,
            'window_peak_lag_ms': None,
            'open_episode': None,
        }
    else:
        view = monitor.brief_view()
    return {
        'state': view['state'],
        'heartbeat_age_ms': _number_or_none(view['heartbeat_age_ms']),
        'current_lag_ms': _number_or_none(view['current_lag_ms']),
        'window_peak_lag_ms': _number_or_none(view['window_peak_lag_ms']),
        'stalls_total': json_number(stalls_total),
        'open_episode': view['open_episode'],
        'uptime_seconds': json_number(round(uptime_seconds, 3)),
    }


def _number_or_none(value: Any) -> float | int | None:
    return None if value is None else json_number(value)


# --- budget -------------------------------------------------------------


def budget_part(budget: Any, pool: Any, *, backpressure_active: bool, config: DrakkarConfig) -> dict[str, Any]:
    """The ``budget`` part; before the start (no budget or pool yet) the counts are 0 and the limits the config's."""
    executor = config.executor
    return {
        'tasks': budget.tasks if budget is not None else 0,
        'max_tasks': budget.max_tasks if budget is not None else executor.max_pending_tasks,
        'bytes': budget.bytes if budget is not None else 0,
        'max_bytes': budget.max_bytes if budget is not None else executor.max_pending_bytes,
        'waiting_loops': budget.waiting_loops if budget is not None else 0,
        'backpressure_active': backpressure_active,
        'pool_active': pool.active_count if pool is not None else 0,
        'pool_max': pool.max_executors if pool is not None else executor.max_executors,
        'pool_waiting': pool.waiting_count if pool is not None else 0,
    }


# --- partitions ---------------------------------------------------------


def gauge_values_by_partition(gauge: MetricWrapperBase) -> dict[int, int]:
    """The current values of a gauge labelled by ``partition``, as whole numbers."""
    values: dict[int, int] = {}
    for family in gauge.collect():
        for sample in family.samples:
            values[int(sample.labels['partition'])] = int(sample.value)
    return values


def partitions_part(source: Any, *, debug_paused: bool, lags: Mapping[int, int]) -> list[dict[str, Any]]:
    """The ``partitions`` part: one row per assigned partition, by number; empty without a Kafka source.

    ``paused_by`` names backpressure (the whole consumer is paused), an
    operator's debug pause, and a dead loop. A partition paused by a delivery
    stall is ``paused`` with no named reason.
    """
    if source is None:
        return []
    rows: list[dict[str, Any]] = []
    for number, processor in sorted(source.processors.items()):
        paused_by: list[str] = []
        if source.paused:
            paused_by.append(PAUSED_BY_BACKPRESSURE)
        if debug_paused:
            paused_by.append(PAUSED_BY_DEBUG)
        if number in source.dead_partitions_paused:
            paused_by.append(PAUSED_BY_DEATH)
        tracker = processor.offset_tracker
        rows.append(
            {
                'partition': number,
                'queued': processor.queue_size,
                'pending_offsets': tracker.pending_count,
                'committed': tracker.last_committed,
                'lag': lags.get(number),
                'paused': bool(paused_by) or number in source.stalled_partitions,
                'paused_by': sorted(paused_by),
                'dead': processor.is_dead,
            }
        )
    return rows


# --- sinks --------------------------------------------------------------


def sinks_part(manager: Any, *, now: float) -> list[dict[str, Any]]:
    """The ``sinks`` part, by type and name.

    ``breaker_open`` is true while the breaker is not closed (open, or half open
    and waiting for its probe). The totals count delivery calls.
    ``last_delivery_seconds`` is the age of the last successful delivery.
    """
    stats = manager.get_all_stats()
    rows: list[dict[str, Any]] = []
    for (sink_type, name), sink in sorted(manager.sinks.items()):
        sink_stats = stats.get((sink_type, name))
        last = sink_stats.last_delivery_ts if sink_stats is not None else None
        rows.append(
            {
                'type': sink_type,
                'name': name,
                'breaker_open': sink.circuit_state != 'closed',
                'delivered_total': sink_stats.delivered_count if sink_stats is not None else 0,
                'failed_total': sink_stats.error_count if sink_stats is not None else 0,
                'last_delivery_seconds': json_number(round(max(0.0, now - last), 3)) if last is not None else None,
            }
        )
    return rows


# --- recorder and autodiscover -----------------------------------------


def recorder_part(config: DrakkarConfig, recorder: Any, *, now: float) -> dict[str, Any]:
    """The ``recorder`` block of ``/api/v1/debug/databases``; provider ``none`` when the worker has no recorder."""
    if recorder is None:
        return {
            'enabled': config.recorder.enabled,
            'provider': PERSISTENCE_NONE,
            'memory': None,
            'sqlite': None,
            'postgres': None,
        }
    return recorder_block(config, recorder, now=now)


def autodiscover_part(config: DrakkarConfig, fleet: Any, *, own_cluster: str, own_hash: str) -> dict[str, Any]:
    """The ``autodiscover`` part, from the registry's last peer snapshot.

    The counts and the match are ``None`` with autodiscover off and before the
    registry started. ``cluster_tasks_match`` is false when an online peer of
    this worker's cluster publishes another valid cluster task hash.
    """
    autodiscover = config.autodiscover
    answer: dict[str, Any] = {
        'enabled': autodiscover.enabled,
        'provider': autodiscover.provider if autodiscover.enabled else None,
        'peers_online': None,
        'peers_offline': None,
        'cluster_tasks_match': None,
    }
    if not autodiscover.enabled or fleet is None:
        return answer
    views = fleet.peer_views()
    answer['peers_online'] = sum(1 for view in views if view.online is True)
    answer['peers_offline'] = sum(1 for view in views if view.online is False)
    answer['cluster_tasks_match'] = not any(
        view.online is True
        and view.card.get('cluster') == own_cluster
        and card_cluster_hash(view.card) not in (None, own_hash)
        for view in views
    )
    return answer


# --- recent failures ----------------------------------------------------


def recent_failures_query(*, include_payloads: bool) -> tuple[str, tuple[Any, ...]]:
    """The SELECT of the newest failed tasks (``ts`` descending, then id) and its parameters."""
    columns = _FAILURE_COLUMNS + (_PAYLOAD_COLUMNS if include_payloads else '')
    query = f'SELECT {columns} FROM events WHERE event = ? ORDER BY ts DESC, id DESC LIMIT ?'
    return query, (TASK_FAILED_EVENT, MAX_RECENT_FAILURES)


def stderr_tail(value: str | bytes | None) -> str:
    """The last ``STDERR_TAIL_BYTES`` bytes of ``value``, cut at a UTF-8 character boundary.

    ``value`` is the text, or the UTF-8 bytes the query read (already the last
    bytes of the text); ``None`` is an empty text.
    """
    if value is None:
        return ''
    data = value.encode('utf-8') if isinstance(value, str) else bytes(value)
    tail = data[-STDERR_TAIL_BYTES:]
    # Drop the continuation bytes of a character whose first byte was cut off.
    start = 0
    while start < len(tail) and tail[start] & 0xC0 == 0x80:
        start += 1
    return tail[start:].decode('utf-8', errors='replace')


def bounded_args(raw: Any) -> tuple[list[str], bool]:
    """The stored args (a JSON list) as texts, kept while their compact JSON stays within ``ARGS_JSON_BYTES``.

    Returns the kept texts and whether items were left out. A text item stays as
    it is; any other item is written as its compact JSON text. A value that is
    not a JSON list gives no args.
    """
    if not isinstance(raw, str):
        return [], False
    try:
        value = json.loads(raw)
    except ValueError:
        return [], False
    if not isinstance(value, list):
        return [], False
    kept: list[str] = []
    for item in value:
        text = item if isinstance(item, str) else dumps(item)
        if len(dumps([*kept, text]).encode('utf-8')) > ARGS_JSON_BYTES:
            break
        kept.append(text)
    return kept, len(kept) < len(value)


def failure_row(row: Mapping[str, Any], *, include_payloads: bool) -> dict[str, Any]:
    """One ``recent_failures`` entry from one row of :func:`recent_failures_query`."""
    kind = row['error_kind']
    offset = row['offset']
    entry: dict[str, Any] = {
        'task_id': row['task_id'],
        'ts': row['ts'],
        'partition': row['partition'],
        'offset': offset if isinstance(offset, int) and not isinstance(offset, bool) else None,
        'exit_code': row['exit_code'],
        'duration': row['duration'],
        'error_kind': kind if isinstance(kind, str) else None,
    }
    if include_payloads:
        entry['stderr_tail'] = stderr_tail(row['stderr'])
        entry['args'], entry['args_truncated'] = bounded_args(row['args'])
    return entry


async def read_recent_failures(recorder: Any, *, include_payloads: bool) -> dict[str, Any]:
    """The ``recent_failures`` part from the recorder memory store; runs on the main loop.

    With no open memory store the answer carries the note. A read that gives
    no answer (the store closed under it, or the read limit) is an empty list.
    """
    store = recorder.memory_store if recorder is not None else None
    if store is None or not store.is_open:
        return {'failures': [], 'note': NO_RECORDER_STORE_NOTE}
    await recorder.flush_memory()
    query, params = recent_failures_query(include_payloads=include_payloads)
    result = await read_memory(store, query, params, endpoint=STATE_ENDPOINT)
    if result is None:
        return {'failures': []}
    columns, rows = result
    return {
        'failures': [
            failure_row(dict(zip(columns, row, strict=True)), include_payloads=include_payloads) for row in rows
        ]
    }


# --- the snapshot -------------------------------------------------------


def counter_total(metric: MetricWrapperBase) -> float:
    """The sum of a counter over every label set (the ``_total`` samples)."""
    return sum(
        sample.value for family in metric.collect() for sample in family.samples if sample.name.endswith('_total')
    )


def rule_metric_values() -> dict[str, float]:
    """The values of the metrics the diagnose rules read, by exposed name."""
    return {
        DROPPED_EVENTS_METRIC: counter_total(recorder_dropped_events),
        DROPPED_POSTGRES_ROWS_METRIC: counter_total(recorder_postgres_dropped_rows),
        DLQ_MESSAGES_METRIC: counter_total(sink_dlq_messages),
    }


async def _loop_parts(app: DrakkarApp, parts: frozenset[str], include_payloads: bool) -> dict[str, Any]:
    """The parts the main loop owns, all read in one coroutine on the main loop."""
    answer: dict[str, Any] = {}
    if 'budget' in parts:
        answer['budget'] = budget_part(
            app._pending_budget,
            app._executor_pool,
            backpressure_active=app._paused,
            config=app._config,
        )
    if 'partitions' in parts:
        answer['partitions'] = partitions_part(
            app.kafka_source,
            debug_paused=app.consume_pause.active,
            lags=gauge_values_by_partition(consumer_lag),
        )
    if 'sinks' in parts:
        answer['sinks'] = sinks_part(app.sink_manager, now=time.time())
    if 'recent_failures' in parts:
        answer['recent_failures'] = await read_recent_failures(app._recorder, include_payloads=include_payloads)
    return answer


async def read_state(app: DrakkarApp, parts: Sequence[str], *, include_payloads: bool) -> dict[str, Any]:
    """The state answer: the asked parts in the contract order, from one read of each source."""
    asked = frozenset(parts)
    loop_parts: dict[str, Any] = {}
    if asked & LOOP_PARTS:
        loop_parts = await dispatch_bounded(_loop_parts(app, asked & LOOP_PARTS, include_payloads), app.main_loop) or {}
    config = app._config
    answer: dict[str, Any] = {}
    for part in parts:
        if part == 'health':
            answer[part] = health_part(
                app._runtime_health,
                uptime_seconds=time.monotonic() - app._start_time,
                stalls_total=counter_total(runtime_stalls),
            )
        elif part == 'recorder':
            answer[part] = recorder_part(config, app._recorder, now=time.time())
        elif part == 'autodiscover':
            answer[part] = autodiscover_part(
                config,
                app.fleet,
                own_cluster=app.worker_identity.cluster,
                own_hash=app.cluster_tasks_hash,
            )
        else:
            answer[part] = loop_parts.get(part)
    return answer
