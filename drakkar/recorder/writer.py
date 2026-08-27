"""Write side of the flight recorder: pipeline facts to event rows.

Every ``record_*`` method here turns one thing that happened — a message
consumed, a task started, a sink delivery, a webapp request timing out —
into the one row shape the ``events`` table and the ``/ws`` stream pin
(see :mod:`drakkar.recorder.schema`). Adding an event type means adding
one method here and nothing else.

:class:`EventWriter` deliberately cannot reach a database. It owns the
live fan-out and the in-memory counters, and hands finished rows to an
abstract :meth:`EventWriter._record`; the persistence half — buffer,
flush, rotation, archive — is :class:`drakkar.recorder.core.EventRecorder`,
which subclasses this and implements that one method. A test can subclass
it in five lines and assert on the rows without opening a file.
"""

from __future__ import annotations

import time
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel

from drakkar.config import UIConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)
from drakkar.recorder.fanout import WSFanout, WSSubscriber
from drakkar.recorder.helpers import encode_json_str, format_dt, sanitize_env_value
from drakkar.recorder.schema import EventType
from drakkar.timefmt import format_rfc3339_micro

if TYPE_CHECKING:
    from drakkar.webapp.models import WebRequestContext

logger = structlog.get_logger()


def _line_count(text: str) -> int:
    """Logical lines in a stream capture: a trailing unterminated line counts."""
    if not text:
        return 0
    return text.count('\n') + (0 if text.endswith('\n') else 1)


def _byte_len(text: str) -> int:
    """Length of ``text`` in UTF-8 bytes, without allocating when it is ASCII.

    Sizing stdin and stdout runs on the loop once per task, and at the
    throughput the pool targets a stray ``.encode()`` there is hundreds of
    megabytes a second of pure copying. ``str.isascii()`` is a flag check on
    CPython's compact-unicode representation, and for ASCII the byte length
    is the character count. Non-ASCII text still pays one encode.
    """
    return len(text) if text.isascii() else len(text.encode())


def _capped_stdin(stdin: str, max_bytes: int) -> tuple[str, bool]:
    """Cap stored stdin at ``max_bytes`` (0 = unlimited), UTF-8 safe.

    The cut happens on the encoded bytes, then decodes with errors ignored so
    a multi-byte character split by the cap is dropped rather than mangled.
    Both early exits — no cap, and text that already fits — answer without
    encoding at all.
    """
    if max_bytes <= 0 or _byte_len(stdin) <= max_bytes:
        return stdin, False
    return stdin.encode()[:max_bytes].decode(errors='ignore'), True


def _failed_metadata(task: ExecutorTask, error: ExecutorError, stdin_max_bytes: int) -> dict:
    """Metadata for a task_failed event: the exception, plus the task's stdin.

    stdin is stored on EVERY failure (capped), independent of the opt-in
    ``store_stdin`` flag — a failure without its input is half a fingerprint,
    and failures are rare enough that the write cost does not matter.
    """
    metadata: dict = {'exception': error.exception}
    if task.stdin:
        stored_stdin, truncated = _capped_stdin(task.stdin, stdin_max_bytes)
        metadata['stdin'] = stored_stdin
        if truncated:
            metadata['stdin_truncated'] = True
    return metadata


class EventWriter:
    """Builds recorder event rows and hands them to a sink.

    Subclasses supply the sink by implementing :meth:`_record`. This class
    owns only what row building needs: the UI config, the live fan-out, and
    the in-memory counters the ``worker_state`` snapshot reports.
    """

    def __init__(self, config: UIConfig) -> None:
        self._config = config
        # The persistence tier (ui.recorder.*) is read on hot paths — bind
        # it once so those reads don't repeat the attribute hop.
        self._store = config.recorder
        # Live-stream fan-out (subscribers, per-client queues, deferred
        # start events) — see :mod:`drakkar.recorder.fanout`.
        self._fanout = WSFanout(config.ws_min_duration_ms)
        # In-memory counters (used for worker_state regardless of store_events)
        self._counters = {
            'consumed': 0,
            'completed': 0,
            'failed': 0,
            'produced': 0,
            'committed': 0,
        }

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        """Hand one finished row to whatever this writer records into.

        Implemented by :class:`drakkar.recorder.core.EventRecorder`, which
        buffers the row for the flush loop and fans it out to the live
        stream.
        """
        raise NotImplementedError

    @property
    def counters(self) -> dict[str, int]:
        return dict(self._counters)

    @property
    def fanout(self) -> WSFanout:
        """The live-stream fan-out (subscribers + deferred start events)."""
        return self._fanout

    def subscribe(self, event_types: Collection[str] | None = None) -> WSSubscriber:
        """Subscribe to the live event stream — see :meth:`WSFanout.subscribe`."""
        return self._fanout.subscribe(event_types)

    def unsubscribe(self, sub: WSSubscriber) -> None:
        """Unsubscribe from the live event stream."""
        self._fanout.unsubscribe(sub)

    def broadcast_throughput(self, windows: dict) -> None:
        """Broadcast one ``throughput`` WS frame (contract v1.16).

        Broadcast-only — never buffered to the DB, so the pinned
        events-table row shape is untouched. ``windows`` is the
        three-window aggregate from the throughput tracker.
        """
        ts = time.time()
        self._fanout.broadcast(
            {
                'ts': ts,
                'dt': format_dt(ts),
                'event': EventType.THROUGHPUT,
                'metadata': encode_json_str({'windows': windows}),
            }
        )

    def record_consumed(self, msg: SourceMessage) -> None:
        self._counters['consumed'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': EventType.CONSUMED,
                'partition': msg.partition,
                'offset': msg.offset,
            }
        )

    def record_arranged(
        self,
        partition: int,
        messages: list[SourceMessage],
        tasks: list[ExecutorTask],
        duration: float = 0.0,
        message_labels: list[str] | None = None,
        window_id: int | None = None,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': EventType.ARRANGED,
                'partition': partition,
                'duration': round(duration, 4),
                'message_count': len(messages),
                'task_count': len(tasks),
                'message_labels': message_labels or [],
                'metadata': encode_json_str(
                    {
                        'offsets': [m.offset for m in messages],
                        'task_ids': [t.task_id for t in tasks],
                        'task_count': len(tasks),
                        'message_count': len(messages),
                        'message_labels': message_labels or [],
                        # Lets the UI group an ``arranged`` row with the
                        # window-scoped annotations emitted from the same
                        # ``arrange()`` call. Per-partition monotonic and
                        # reset on restart — never a global identifier.
                        'window_id': window_id,
                    }
                ),
            }
        )

    def record_annotation(
        self,
        *,
        kind: str,
        partition: int,
        metadata_json: str,
        offset: int | None = None,
        task_id: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Append one handler-emitted annotation to the event log.

        The envelope is already encoded by :class:`~drakkar.annotations.Annotator`,
        which owns the size budgets — by the time a payload reaches here it has
        been measured and accepted, so this method does no validation of its own.

        Anchoring follows the annotation's scope: ``offset`` set for a message,
        ``task_id`` set for a task, neither set for a window (those rows are
        matched through the ``offsets`` list inside ``metadata_json``).
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.ANNOTATION,
                'partition': partition,
                'offset': offset,
                'task_id': task_id,
                'metadata': metadata_json,
                'labels': encode_json_str(labels) if labels else None,
                'kind': kind,
            }
        )

    def record_task_started(
        self,
        task: ExecutorTask,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        slot: int = 0,
        precomputed: bool = False,
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
        queue_wait_ms: float | None = None,
    ) -> None:
        stdin_lines = 0
        stdin_size = 0
        if task.stdin:
            stdin_size = _byte_len(task.stdin)
            stdin_lines = _line_count(task.stdin)
        metadata: dict = {
            'source_offsets': task.source_offsets,
            'slot': slot,
        }
        if queue_wait_ms is not None:
            # How long the task waited for a pool slot before any work began —
            # the live UI shows it in the timeline hover so wall time splits
            # into waiting vs running.
            metadata['queue_wait_ms'] = queue_wait_ms
        if task.stdin and self._store.store_stdin:
            # Opt-in stdin capture (ui.recorder.store_stdin): the exact input
            # the task consumed, capped at stdin_max_bytes. Rides the existing
            # metadata JSON, so no events-table schema change is involved.
            stored_stdin, stdin_truncated = _capped_stdin(task.stdin, self._store.stdin_max_bytes)
            metadata['stdin'] = stored_stdin
            if stdin_truncated:
                metadata['stdin_truncated'] = True
        elif stdin_size:
            # Content not stored — record the size in metadata anyway. The
            # entry-level stdin_size below is WS-only (dropped at DB insert),
            # and the task detail page needs to say "N bytes consumed, content
            # not stored" instead of showing nothing.
            metadata['stdin_bytes'] = stdin_size
        if task.env:
            # Sanitize per-task env values before storing in the recorder DB.
            # The raw task.env stays untouched on the task object (subprocess
            # launch still needs real values); only the RECORDED copy is
            # redacted so the debug UI never exposes secrets.
            metadata['env'] = {k: sanitize_env_value(k, v) for k, v in task.env.items()}
        if precomputed:
            # Neutral marker: a result was supplied by the handler and no
            # subprocess ran. The framework does not classify the reason
            # (cache, lookup, deterministic shortcut, ...).
            metadata['precomputed'] = True
        # Origin / client_name / request_id default to whatever the
        # ``ExecutorTask`` carries — auto-tagged at ``arrange_*`` time on
        # both the Kafka path (``origin='kafka'`` default) and the
        # webapp path (``WebappRunner`` stamps each task before
        # submission). Explicit kwargs override only when callers need to
        # record an event for a task that doesn't carry those fields
        # (e.g., a synthetic test row).
        resolved_origin = origin if origin is not None else getattr(task, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task, 'request_id', None)
        entry = {
            'ts': time.time(),
            'event': EventType.TASK_STARTED,
            'partition': partition,
            'task_id': task.task_id,
            'args': encode_json_str(task.args),
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'slot': slot,
            'stdin_lines': stdin_lines,
            'stdin_size': stdin_size,
            'metadata': encode_json_str(metadata),
            'labels': encode_json_str(task.labels) if task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        ws_threshold_ms = self._config.ws_min_duration_ms
        if ws_threshold_ms > 0:
            # Defer WS broadcast: only send task_started to live UI if
            # the task is still running after ws_min_duration_ms.
            self._record(entry, skip_ws=True)
            self._fanout.defer(task.task_id, entry, ws_threshold_ms / 1000.0)
        else:
            self._record(entry)

    def record_task_completed(
        self,
        result: ExecutorResult,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        precomputed: bool = False,
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
        cost: float | None = None,
        speed: float | None = None,
    ) -> None:
        self._counters['completed'] += 1
        duration_ms = result.duration_seconds * 1000

        # If the task_started was deferred and the task finished before
        # the threshold, neither start nor completion goes to WS.
        # If the threshold already fired (start was sent), send completion too.
        # Removing the entry is enough to suppress the pending start event —
        # the shared sweep only broadcasts what is still in the map, so there
        # is no per-task timer to cancel.
        skip_ws = self._fanout.take_deferred(result.task.task_id) is not None

        skip_db = self._store.event_min_duration_ms > 0 and duration_ms < self._store.event_min_duration_ms
        include_output = duration_ms >= self._store.output_min_duration_ms

        # See ``record_task_started`` for the resolution rules: explicit
        # kwargs take precedence, otherwise we read from the task that
        # produced the result (auto-tagged by the upstream pipeline).
        task_obj = result.task
        resolved_origin = origin if origin is not None else getattr(task_obj, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task_obj, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task_obj, 'request_id', None)
        entry: dict = {
            'ts': time.time(),
            'event': EventType.TASK_COMPLETED,
            'partition': partition,
            'task_id': result.task.task_id,
            'exit_code': result.exit_code,
            'duration': result.duration_seconds,
            # Sized off the DECODED string, after each invalid byte has
            # become U+FFFD — so the number stays defined for non-UTF-8
            # output rather than depending on how it was decoded.
            'stdout_size': _byte_len(result.stdout),
            # WS-frame-only field, like task_started's stdin_lines/stdin_size:
            # not in the pinned events-table column list, so the DB insert
            # drops it and the row shape is unchanged.
            'stdout_lines': _line_count(result.stdout),
            'pid': result.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'labels': encode_json_str(result.task.labels) if result.task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        completed_meta: dict = {}
        if precomputed:
            # Mirrored on the completion event so downstream queries /
            # dashboards can filter precomputed outcomes without joining
            # to task_started.
            completed_meta['precomputed'] = True
        if result.spawn_seconds is not None:
            # Parent-side share of the duration (see ExecutorResult.spawn_seconds);
            # the live UI shows it in the timeline hover detail.
            completed_meta['spawn_ms'] = round(result.spawn_seconds * 1000, 1)
        if cost is not None and speed is not None:
            # Contract v1.16: present only for throughput-counted tasks —
            # the caller applied the counting rules; excluded tasks carry
            # no keys at all, never zeros.
            completed_meta['cost'] = cost
            completed_meta['speed'] = round(speed, 3)
        if completed_meta:
            entry['metadata'] = encode_json_str(completed_meta)
        if include_output:
            entry['args'] = encode_json_str(result.task.args)
        if include_output and self._store.store_output:
            entry['stdout'] = result.stdout
            entry['stderr'] = result.stderr
        self._record(entry, skip_ws=skip_ws, skip_db=skip_db)

        if duration_ms >= self._config.log_min_duration_ms:
            logger.info(
                'slow_task_completed',
                category='recorder',
                task_id=result.task.task_id,
                duration=result.duration_seconds,
                partition=partition,
            )

    def record_task_failed(
        self,
        task: ExecutorTask,
        error: ExecutorError,
        partition: int,
        pool_active: int = 0,
        pool_waiting: int = 0,
        duration_seconds: float | None = None,
        origin: str | None = None,
        client_name: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self._counters['failed'] += 1

        # Failed tasks ALWAYS go to WS regardless of ws_min_duration_ms.
        # If the task_started was deferred, send it now before the failure event
        # so the live UI sees the full start→fail sequence.
        start_event = self._fanout.take_deferred(task.task_id)
        if start_event is not None:
            self._fanout.broadcast(start_event)

        if duration_seconds is not None:
            duration_ms = duration_seconds * 1000
            skip_db = self._store.event_min_duration_ms > 0 and duration_ms < self._store.event_min_duration_ms
            include_output = duration_ms >= self._store.output_min_duration_ms
            should_log = duration_ms >= self._config.log_min_duration_ms
        else:
            skip_db = False
            include_output = True
            should_log = True

        # See ``record_task_started`` for the resolution rules.
        resolved_origin = origin if origin is not None else getattr(task, 'origin', 'kafka')
        resolved_client_name = client_name if client_name is not None else getattr(task, 'client_name', None)
        resolved_request_id = request_id if request_id is not None else getattr(task, 'request_id', None)
        entry: dict = {
            'ts': time.time(),
            'event': EventType.TASK_FAILED,
            'partition': partition,
            'task_id': task.task_id,
            'exit_code': error.exit_code,
            'pid': error.pid,
            'pool_active': pool_active,
            'pool_waiting': pool_waiting,
            'metadata': encode_json_str(_failed_metadata(task, error, self._store.stdin_max_bytes)),
            'labels': encode_json_str(task.labels) if task.labels else None,
            'origin': resolved_origin,
            'client_name': resolved_client_name,
            'request_id': resolved_request_id,
        }
        if duration_seconds is not None:
            entry['duration'] = duration_seconds
        if include_output:
            entry['args'] = encode_json_str(task.args)
        if include_output and self._store.store_output:
            entry['stderr'] = error.stderr
        self._record(entry, skip_ws=False, skip_db=skip_db)

        if should_log:
            logger.info(
                'slow_task_failed',
                category='recorder',
                task_id=task.task_id,
                duration=duration_seconds,
                partition=partition,
            )

    def record_task_complete(
        self,
        task_id: str,
        partition: int,
        duration: float,
        output_message_count: int,
    ) -> None:
        """Record that on_task_complete() finished for one successful task.

        Event name is ``task_complete`` (the hook name without ``on_``) —
        distinct from ``task_completed`` which marks subprocess exit. The
        two sit next to each other in the pipeline: subprocess ends first
        (task_completed), then the handler's post-processing and sink
        routing run, and this event marks the end of that stage.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.TASK_COMPLETE,
                'task_id': task_id,
                'partition': partition,
                'duration': round(duration, 4),
                'metadata': encode_json_str(
                    {
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_message_complete(
        self,
        partition: int,
        offset: int,
        duration: float,
        task_count: int,
        succeeded: int,
        failed: int,
        replaced: int,
        output_message_count: int,
    ) -> None:
        """Record that on_message_complete() finished for one source message.

        Fires once per source message, after every task derived from it
        has reached a terminal state. The event corresponds 1:1 with a
        handler ``on_message_complete`` call.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.MESSAGE_COMPLETE,
                'partition': partition,
                'offset': offset,
                'duration': round(duration, 4),
                'metadata': encode_json_str(
                    {
                        'task_count': task_count,
                        'succeeded': succeeded,
                        'failed': failed,
                        'replaced': replaced,
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_window_complete(
        self,
        partition: int,
        window_id: int,
        duration: float,
        task_count: int,
        output_message_count: int,
    ) -> None:
        """Record that on_window_complete() finished for one arrange() window."""
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WINDOW_COMPLETE,
                'partition': partition,
                'duration': round(duration, 4),
                'metadata': encode_json_str(
                    {
                        'window_id': window_id,
                        'task_count': task_count,
                        'output_message_count': output_message_count,
                    }
                ),
            }
        )

    def record_runtime_health(
        self,
        kind: str,
        state: str,
        lag_ms: float,
        unit_count: int,
    ) -> None:
        """Record one runtime-health transition or periodic sample.

        ``kind`` is ``'transition'`` (state changed) or ``'sample'`` (the
        low-frequency history point). ``unit_count`` is paired with a
        ``unit_label`` naming what it counts (asyncio tasks here), so a
        consumer renders it without assuming the unit.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.RUNTIME_HEALTH,
                'metadata': encode_json_str(
                    {
                        'kind': kind,
                        'state': state,
                        'lag_ms': lag_ms,
                        'unit_count': unit_count,
                    }
                ),
            }
        )

    def record_runtime_stall(
        self,
        duration_ms: float,
        stacks: list[dict],
        dropped_stacks: int,
        unit_count: int,
    ) -> None:
        """Record one runtime stall with the stacks captured while it lasted.

        ``stacks`` entries are ``{stack, location, count}`` — distinct
        blocking sites the sampler thread saw, with how often each was
        sampled. ``dropped_stacks`` counts distinct sites past the
        ``runtime_health.max_stall_stacks`` cap.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.RUNTIME_STALL,
                'duration': round(duration_ms / 1000, 4),
                'metadata': encode_json_str(
                    {
                        'duration_ms': duration_ms,
                        'stacks': stacks,
                        'dropped_stacks': dropped_stacks,
                        'unit_count': unit_count,
                    }
                ),
            }
        )

    def record_runtime_lag_episode(
        self,
        *,
        duration_ms: float,
        peak_lag_ms: float,
        lag_sum_ms: float,
        verdict: str,
        stall_count: int,
        sample_count: int,
        stacks: list[dict],
        dropped_stacks: int,
        unit_count: int,
        cpu_ms: float | None = None,
        cpu_ratio: float | None = None,
        evidence: dict | None = None,
    ) -> None:
        """One closed lag episode (contract v1.15).

        An episode spans the whole time the runtime was degraded or
        stalled; where ``runtime_stall`` answers "what blocked the loop
        just now", this row answers "what was the loop doing across the
        entire bad window, and was it blocked, busy, or starved of CPU".
        ``evidence`` carries optional corroborating host-pressure readings
        (``cpu_throttled_ms``, ``psi_cpu_some_avg10``, ``load1``), merged
        flat into the metadata.
        """
        meta: dict[str, Any] = {
            'duration_ms': duration_ms,
            'peak_lag_ms': peak_lag_ms,
            'lag_sum_ms': lag_sum_ms,
            'verdict': verdict,
            'stall_count': stall_count,
            'sample_count': sample_count,
            'stacks': stacks,
            'dropped_stacks': dropped_stacks,
            'unit_count': unit_count,
        }
        if cpu_ms is not None:
            meta['cpu_ms'] = cpu_ms
        if cpu_ratio is not None:
            meta['cpu_ratio'] = cpu_ratio
        if evidence:
            meta.update(evidence)
        self._record(
            {
                'ts': time.time(),
                'event': EventType.RUNTIME_LAG_EPISODE,
                'duration': round(duration_ms / 1000, 4),
                'metadata': encode_json_str(meta),
            }
        )

    def record_runtime_probe(self, *, lag_ms: float, unit_count: int, stacks: list[dict]) -> None:
        """One opt-in runtime stack probe (contract v1.15).

        Enabled by ``runtime_health.probe_interval_seconds > 0``: a
        flight-recorder profiler sample of what the runtime thread was
        executing, taken regardless of health state.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.RUNTIME_PROBE,
                'metadata': encode_json_str(
                    {
                        'lag_ms': lag_ms,
                        'unit_count': unit_count,
                        'stacks': stacks,
                    }
                ),
            }
        )

    def record_produced(
        self,
        payload: BaseModel,
        source_partition: int,
        source_offset: int | None = None,
    ) -> None:
        self._counters['produced'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': EventType.PRODUCED,
                'partition': source_partition,
                'offset': source_offset,
                'output_topic': getattr(payload, 'sink', ''),
            }
        )

    def record_sink_delivery(
        self,
        sink_type: str,
        sink_name: str,
        payload_count: int,
        duration: float,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': EventType.SINK_DELIVERED,
                'metadata': encode_json_str(
                    {
                        'sink_type': sink_type,
                        'sink_name': sink_name,
                        'payload_count': payload_count,
                        'duration': round(duration, 4),
                    }
                ),
            }
        )

    def record_sink_error(
        self,
        sink_type: str,
        sink_name: str,
        error: str,
        attempt: int,
    ) -> None:
        self._record(
            {
                'ts': time.time(),
                'event': EventType.SINK_ERROR,
                'metadata': encode_json_str(
                    {
                        'sink_type': sink_type,
                        'sink_name': sink_name,
                        'error': error,
                        'attempt': attempt,
                    }
                ),
            }
        )

    def record_committed(self, partition: int, offset: int) -> None:
        self._counters['committed'] += 1
        self._record(
            {
                'ts': time.time(),
                'event': EventType.COMMITTED,
                'partition': partition,
                'offset': offset,
            }
        )

    def record_assigned(self, partitions: list[int]) -> None:
        for p in partitions:
            self._record(
                {
                    'ts': time.time(),
                    'event': EventType.ASSIGNED,
                    'partition': p,
                }
            )

    def record_revoked(self, partitions: list[int]) -> None:
        for p in partitions:
            self._record(
                {
                    'ts': time.time(),
                    'event': EventType.REVOKED,
                    'partition': p,
                }
            )

    def record_partition_stalled(self, partition: int) -> None:
        """Record that a partition was paused because an offset stalled
        (delivery + DLQ unconfirmed under ``dlq.on_send_failure=stall``)."""
        self._record(
            {
                'ts': time.time(),
                'event': EventType.PARTITION_STALLED,
                'partition': partition,
            }
        )

    def record_offload(
        self,
        *,
        hook: str,
        partition: int | None,
        function: str,
        duration: float,
        queued: float,
        status: str,
        error: str = '',
        window_id: int | None = None,
        offsets: tuple[int, ...] = (),
        offset: int | None = None,
        task_id: str | None = None,
    ) -> None:
        """Record one ``BaseDrakkarHandler.offload()`` call.

        Anchoring mirrors annotations: ``offset`` for a message-anchored
        hook, ``task_id`` for a task-anchored one, neither for window-wide
        hooks (``arrange`` / ``on_window_complete``) whose rows are matched
        through the ``offsets`` list inside ``metadata``. ``partition`` is
        ``None`` when offload() ran outside a framework-invoked hook
        (``on_ready``, a ``@periodic`` method) — the row then lives outside
        any message trace, like ``periodic_run`` events do.

        ``queued`` is the seconds the call waited for a free offload-pool
        thread; sustained non-zero values are the signal that
        ``offload.max_threads`` is undersized for the deployment.
        """
        metadata: dict[str, Any] = {
            'hook': hook,
            'function': function,
            'queued': round(queued, 4),
            'status': status,
            'window_id': window_id,
            # Window-scoped rows only — same trace-matching contract as
            # window-scoped annotations (see record_annotation).
            'offsets': list(offsets) if offset is None and task_id is None else [],
        }
        if error:
            metadata['error'] = error[:500]
        self._record(
            {
                'ts': time.time(),
                'event': EventType.OFFLOAD,
                'partition': partition,
                'offset': offset,
                'task_id': task_id,
                'duration': round(duration, 4),
                'metadata': encode_json_str(metadata),
                # Not a DB column; carried for live WS subscribers so the
                # UI can label the row without parsing metadata.
                'hook': hook,
            }
        )

    def record_periodic_run(
        self,
        name: str,
        duration: float,
        status: str,
        error: str = '',
        system: bool = False,
    ) -> None:
        """Record a periodic task execution (success or failure).

        The ``system`` flag distinguishes framework-internal periodic loops
        (``cache.flush``, ``cache.sync``, ``cache.cleanup``) from user-defined
        ``@periodic`` handler methods. It is omitted from the metadata JSON
        when False so existing event rows remain byte-identical to those written
        before the flag was introduced — avoids a metadata schema diff.
        """
        metadata: dict[str, str | bool] = {'status': status}
        if error:
            metadata['error'] = error
        if system:
            metadata['system'] = True
        self._record(
            {
                'ts': time.time(),
                'event': EventType.PERIODIC_RUN,
                'task_id': name,
                'duration': duration,
                'exit_code': 0 if status == 'ok' else 1,
                'metadata': encode_json_str(metadata),
            }
        )

    # --- Webapp request lifecycle events ---
    #
    # These helpers record HTTP-origin request lifecycle events to the
    # ``events`` table alongside the per-task rows already produced by
    # ``record_task_*``. They populate the new ``origin`` / ``client_name``
    # / ``request_id`` columns so the debug UI can filter, group, and
    # trace HTTP-originated work without re-deriving the relationship
    # from labels. Every helper is sync and safe to call from either T1
    # (the runner) or T2 (the dependency layer) — appending to the
    # in-memory buffer doesn't touch the DB connection (the flush loop
    # owns that), so the recorder is loop-agnostic at the recording site.

    def record_webapp_request_received(self, ctx: WebRequestContext) -> None:
        """Record entry into the runner for one HTTP request.

        Fires once per request that passed the auth + rate-limit + body
        validation gates and is about to dispatch to T1. Captures the
        client identity, request id, and start timestamp so operators
        can pinpoint the request in the recorder/debug UI before any
        task fan-out.
        """
        body_bytes = self._compute_body_bytes(ctx)
        metadata: dict[str, Any] = {
            'started_at': format_rfc3339_micro(ctx.started_at) if ctx.started_at is not None else None,
        }
        if body_bytes is not None:
            metadata['body_bytes'] = body_bytes
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_RECEIVED,
                'partition': -1,  # synthetic partition for HTTP origin
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'metadata': encode_json_str(metadata),
            }
        )

    def record_webapp_request_completed(
        self,
        ctx: WebRequestContext,
        status: str,
        duration_ms: float,
    ) -> None:
        """Record successful completion of one HTTP request (status='ok').

        ``status`` is the same label used on ``drakkar_webapp_requests_total``
        — typically ``'ok'`` here, but kept as a parameter so the same
        helper can be reused if the route handler ever needs to record
        a non-ok terminal state without a dedicated helper.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_COMPLETED,
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'duration': duration_ms / 1000.0,
                'metadata': encode_json_str(
                    {
                        'status': status,
                        'duration_ms': duration_ms,
                    }
                ),
            }
        )

    def record_webapp_request_timeout(
        self,
        ctx: WebRequestContext,
        duration_ms: float,
    ) -> None:
        """Record a 504 timeout outcome for one HTTP request.

        Fires from T2 (the route handler) when ``asyncio.wait_for`` trips
        its budget. The matching ``record_webapp_request_dropped_after_timeout``
        is fired from T1 if the runner reaches its post-execute gate
        AFTER the timeout — together they let operators distinguish
        "timed out before any work" from "timed out after the executor
        already ran".
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_TIMEOUT,
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
                'duration': duration_ms / 1000.0,
                'metadata': encode_json_str({'duration_ms': duration_ms}),
            }
        )

    def record_webapp_request_rate_limited(
        self,
        client: str,
        rpm_limit: int,
        requests_in_window: int,
    ) -> None:
        """Record a 429 rate-limit outcome for one HTTP request.

        ``client`` is the matched client name (the rate-limit dep only
        runs after auth has resolved a real client, so a configured
        client name is always available). No ``request_id`` because the
        request is rejected before the runner allocates one.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_RATE_LIMITED,
                'partition': -1,
                'origin': 'http',
                'client_name': client,
                'metadata': encode_json_str(
                    {
                        'rpm_limit': rpm_limit,
                        'requests_in_window': requests_in_window,
                    }
                ),
            }
        )

    def record_webapp_request_auth_failed(self, token_prefix: str) -> None:
        """Record a 401 auth-failure for one HTTP request.

        ``token_prefix`` is the redacted (first-4-chars + ``...``) token
        produced by :func:`drakkar.webapp.utils.redact_token` — never
        the full token. ``client_name`` is left ``NULL`` because no
        client was matched; the debug UI can filter by ``event =
        'webapp_request_auth_failed'`` instead.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_AUTH_FAILED,
                'partition': -1,
                'origin': 'http',
                # No matched client; leave client_name NULL.
                'metadata': encode_json_str({'token_prefix': token_prefix}),
            }
        )

    def record_webapp_request_dropped_after_timeout(
        self,
        ctx: WebRequestContext,
    ) -> None:
        """Record a request dropped on T1 after T2 had already 504'd.

        Fires from the runner's post-execute / pre-on_http_request_complete
        cancellation gates. The user already received a 504 client-side;
        this row marks the wasted work that the framework managed to
        skip thanks to the cooperative cancellation flag.
        """
        self._record(
            {
                'ts': time.time(),
                'event': EventType.WEBAPP_REQUEST_DROPPED_AFTER_TIMEOUT,
                'partition': -1,
                'origin': 'http',
                'client_name': ctx.client_name,
                'request_id': ctx.request_id,
            }
        )

    @staticmethod
    def _compute_body_bytes(ctx: WebRequestContext) -> int | None:
        """Return the body size in bytes for the recorder metadata, or ``None``.

        Pydantic-modelled requests have a ``model_dump_json`` method we
        use to recover the on-the-wire body size; raw bytes are
        ``len()``ed directly. For the rare test fixture that hands in a
        plain object (no model, no bytes) we fall back to ``None`` rather
        than coerce — better to omit the field than misreport.
        """
        request = ctx.request
        if isinstance(request, bytes | bytearray):
            return len(request)
        # Avoid importing Pydantic at module load — the recorder runs in
        # processes that don't always carry the webapp dependencies.
        # Local import keeps the cost paid only when we actually inspect
        # a webapp request (a small fraction of recorder writes).
        try:
            from pydantic import BaseModel
        except ImportError:  # pragma: no cover — pydantic is a hard dep
            return None
        if isinstance(request, BaseModel):
            try:
                return len(request.model_dump_json().encode('utf-8'))
            except Exception:
                return None
        return None

    # --- Query methods (for debug UI, reads current DB) ---
