"""Isolated debug harness for the Message Probe tab.

This module implements the **no-footprint** probe: a user pastes a single
Kafka-like message and we replay it through the live handler's
``arrange`` → ``executor`` → ``on_task_complete`` → ``on_message_complete``
→ ``on_window_complete`` sequence, capturing every detail at every stage,
WITHOUT touching any production-bound side effect:

- No sink writes (SinkManager is never instantiated/called)
- No offset commits (no PartitionProcessor state is touched)
- No event-recorder rows (``ExecutorPool.execute`` is called with
  ``recorder=None`` — all ``recorder.record_*`` calls are guarded by
  ``if recorder:`` in the executor)
- No cache writes (live Cache is wrapped in ``DebugCacheProxy`` that
  no-ops every mutation and optionally forwards reads)
- No peer sync (cache proxy never enters the live Cache's ``_dirty`` map)

The architecture splits responsibility across three pieces:

1. **DebugCacheProxy** — impersonates the live ``Cache`` / ``NoOpCache``
   with the same method surface. Every call is logged to
   ``self.calls`` for the final report. Writes are always suppressed;
   reads either forward to the live cache (when the UI checkbox
   ``use_cache=True`` is on) or immediately return a miss.

2. **DebugSinkCollector** — replaces the ``_on_collect`` callback that
   ``PartitionProcessor`` normally uses to hand ``CollectResult``
   instances to ``SinkManager``. In debug mode we capture every
   ``(stage, CollectResult)`` pair and later flatten each sink field
   into a single ``PlannedSinkRecord`` list for the UI.

3. **DebugRunner** (added in Task 2) — orchestrates the run, swaps the
   handler's ``cache`` attribute for the duration of the probe, and
   produces the final ``DebugReport``.

This file (Task 1) defines the two helpers above plus all pydantic
models the report is built from. ``DebugRunner`` itself lands in Task 2.
"""

from __future__ import annotations

import contextvars
import time
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

from drakkar.cache import CacheScope
from drakkar.models import CollectResult

if TYPE_CHECKING:
    from drakkar.cache import Cache, NoOpCache


# ---- probe-stage contextvar ------------------------------------------------
#
# The runner (Task 2) sets this before each hook call ('arrange',
# 'task_complete:<id>', etc.) so the DebugCacheProxy can tag every cache
# call with the hook that made it. Reset to 'unknown' after each hook.
#
# Using a ContextVar (rather than a plain attribute on the proxy) keeps
# the proxy thread/task-agnostic: concurrent probes on the same handler
# would each see their own stage tag. We still serialize probes with a
# lock in Task 2 because swapping ``handler.cache`` is inherently
# process-wide, but the contextvar is the right choice for the stage
# tag itself.

_probe_stage: contextvars.ContextVar[str] = contextvars.ContextVar(
    'drakkar_probe_stage',
    default='unknown',
)


# ---- pydantic models for the report ----------------------------------------


class ProbeInput(BaseModel):
    """Body of the incoming probe request, and the echo shown in section A of the UI.

    ``value`` is received as a UTF-8 string — the runner encodes it to
    bytes when constructing the synthetic ``SourceMessage``. We keep it
    as a string on the wire because the /debug tab posts JSON, and
    base64-encoding the payload just to flip back immediately is noise.
    """

    value: str = Field(
        description='Raw message value as text. Encoded to UTF-8 bytes before the synthetic SourceMessage is built.'
    )
    key: str | None = Field(
        default=None,
        description='Optional Kafka message key. Encoded to UTF-8 bytes when set.',
    )
    partition: int = Field(default=0, description='Synthetic partition id for the probe.')
    offset: int = Field(default=0, description='Synthetic offset for the probe.')
    topic: str = Field(
        default='',
        description='Synthetic topic name. The endpoint substitutes the configured source topic when this is empty.',
    )
    timestamp: int | None = Field(
        default=None,
        description='Kafka-style milliseconds timestamp. Defaults to time.time() * 1000 in the runner.',
    )
    use_cache: bool = Field(
        default=False,
        description='When true, cache reads forward to the live cache; writes are ALWAYS suppressed.',
    )


class ProbeCacheCall(BaseModel):
    """One call made to the DebugCacheProxy during the probe.

    ``outcome`` distinguishes ``hit`` / ``miss`` (read paths) from
    ``suppressed`` (write paths that would have touched the real cache).
    ``value_preview`` is a short human-readable slice of the value —
    truncated to 120 chars with newlines collapsed so long payloads
    fit on one UI row without bloating the report JSON.
    """

    op: Literal['get', 'set', 'peek', 'delete', 'contains'] = Field(description='Which proxy method was called.')
    key: str = Field(description='Cache key involved.')
    scope: str = Field(description='CacheScope name for the call (LOCAL / CLUSTER / GLOBAL).')
    outcome: Literal['hit', 'miss', 'suppressed'] = Field(
        description='hit / miss for reads; suppressed for writes (set/delete), since writes never reach the live cache.'
    )
    value_preview: str | None = Field(
        default=None,
        description='First ~120 chars of the value being set/returned. Newlines collapsed to spaces.',
    )
    origin_stage: str = Field(
        description='The _probe_stage value at the time of the call (e.g. "arrange", "task_complete:t-abc").'
    )
    ms_since_start: float = Field(description='Elapsed milliseconds since the runner started this probe.')


class ProbeStageResult(BaseModel):
    """Generic container for the outcome of a single hook stage.

    Used for ``arrange``, ``on_message_complete``, ``on_window_complete``.
    ``error`` carries a one-line summary of any exception; the full
    traceback goes into the top-level ``errors`` list on ``DebugReport``
    so the UI can render all failures in one panel.
    """

    duration_seconds: float | None = Field(
        default=None, description='Wall-clock duration of the hook invocation, in seconds.'
    )
    collect_result: CollectResult | None = Field(
        default=None, description='CollectResult returned by the hook, if any.'
    )
    error: str | None = Field(
        default=None, description='One-line summary of any raised exception. Full traceback goes in DebugReport.errors.'
    )


class ProbeTaskEntry(BaseModel):
    """One row of the Tasks table in section C of the Message Probe UI.

    ``status`` values:
      - ``done``      — subprocess (or precomputed fast path) succeeded.
      - ``failed``    — the task terminally failed after on_error.
      - ``replaced``  — on_error returned a replacement list, original
                        task is no longer the terminal outcome.
    """

    task_id: str = Field(description='Unique task identifier (ExecutorTask.task_id).')
    parent_task_id: str | None = Field(
        default=None, description='Optional parent task id (set when on_error returns replacements).'
    )
    labels: dict[str, str] = Field(default_factory=dict, description='User-defined labels carried on the ExecutorTask.')
    source_offsets: list[int] = Field(default_factory=list, description='Source Kafka offsets that produced this task.')
    precomputed: bool = Field(
        default=False, description='True if the task was served via ExecutorTask.precomputed (no subprocess ran).'
    )
    status: Literal['done', 'failed', 'replaced'] = Field(
        description='Terminal status of this task within the probe run.'
    )
    exit_code: int | None = Field(
        default=None, description='Process exit code, or None if the task never produced one.'
    )
    duration_seconds: float | None = Field(
        default=None, description='Wall-clock duration of the subprocess / precomputed fast path.'
    )
    stdin: str = Field(default='', description='Raw stdin text written to the subprocess (from ExecutorTask.stdin).')
    stdout: str = Field(default='', description='Captured stdout. May be very large (multi-megabyte).')
    stderr: str = Field(default='', description='Captured stderr. May be very large.')
    subprocess_exception: str | None = Field(
        default=None,
        description='Exception message when the subprocess failed to launch or timed out.',
    )
    on_task_complete_duration: float | None = Field(
        default=None,
        description='Wall-clock duration of the on_task_complete hook for this task.',
    )
    on_task_complete_result: CollectResult | None = Field(
        default=None,
        description='CollectResult returned by on_task_complete for this task, if any.',
    )
    on_task_complete_error: str | None = Field(
        default=None,
        description='One-line summary if on_task_complete raised; traceback goes in DebugReport.errors.',
    )
    retry_of: str | None = Field(
        default=None, description='task_id of the task this row retries (set when on_error returned RETRY).'
    )
    replacement_for: str | None = Field(
        default=None, description='task_id of the task this row replaces (set for items in on_error replacement lists).'
    )


class ProbeError(BaseModel):
    """One exception captured during the probe, aggregated in the Errors panel (section I)."""

    stage: str = Field(
        description='Stage identifier where the exception was raised (e.g. "arrange", "on_task_complete:t-abc").'
    )
    exception_class: str = Field(description='Fully qualified exception class name.')
    message: str = Field(description='str(exception).')
    traceback: str = Field(description='traceback.format_exc() output captured at the point of failure.')
    occurred_at_ms: float = Field(description='Milliseconds since the probe started.')


class PlannedSinkRecord(BaseModel):
    """One record that WOULD have been written to a sink if this were a real run.

    The runner never actually writes to a sink during a probe. This record
    is built from the ``CollectResult`` payload field for the corresponding
    sink type. ``extras`` carries type-specific metadata (e.g. Kafka key,
    Postgres sink instance name, etc.) without bloating the core schema.
    """

    sink_type: Literal['kafka', 'postgres', 'mongo', 'http', 'redis', 'files'] = Field(
        description='Which CollectResult field the payload came from.'
    )
    destination: str = Field(
        description='Type-specific destination — topic for kafka, table for postgres, collection for mongo, sink instance name for http, key for redis, path for files.'
    )
    origin_stage: str = Field(
        description='Which stage produced this sink record (e.g. "task_complete:t-abc", "message_complete", "window_complete").'
    )
    payload: Any = Field(description='The serialized payload data (from BaseModel.model_dump()).')
    extras: dict[str, Any] = Field(
        default_factory=dict, description='Type-specific metadata: kafka key, sink instance name, ttl, etc.'
    )


class DebugReport(BaseModel):
    """Full report returned by the probe endpoint.

    ``truncated=True`` signals that the runner did not complete every
    stage — either the wall-clock timeout fired or an upstream hook
    raised before the downstream stages could run. The UI renders a
    warning banner when this is set.
    """

    input: ProbeInput = Field(description='Echo of the probe request, for the Input & Deserialization card.')
    deserialize_error: ProbeError | None = Field(
        default=None,
        description='Captured exception from handler.deserialize_message, if any.',
    )
    parsed_payload: Any | None = Field(
        default=None,
        description='Parsed message payload. Typically a dict (Pydantic model dump) or None if no input_model is set.',
    )
    message_label: str | None = Field(
        default=None, description='Output of handler.message_label for the synthetic SourceMessage.'
    )
    arrange: ProbeStageResult = Field(description='Outcome of the arrange() hook.')
    tasks: list[ProbeTaskEntry] = Field(
        default_factory=list, description='One entry per ExecutorTask run through the probe, in order.'
    )
    on_message_complete: ProbeStageResult | None = Field(
        default=None, description='Outcome of the on_message_complete hook, if it ran.'
    )
    on_window_complete: ProbeStageResult | None = Field(
        default=None, description='Outcome of the on_window_complete hook, if it ran.'
    )
    planned_sink_payloads: list[PlannedSinkRecord] = Field(
        default_factory=list,
        description='Flattened list of every payload that would have been written to a sink.',
    )
    cache_calls: list[ProbeCacheCall] = Field(
        default_factory=list, description='Full log of every cache call made during the probe.'
    )
    cache_summary: dict[str, int] = Field(
        default_factory=dict,
        description='Counts keyed by calls / hits / misses / writes_suppressed. Derived from cache_calls.',
    )
    timing: dict[str, float] = Field(
        default_factory=dict,
        description='Per-stage wall-clock durations in seconds: total_wallclock, arrange, on_message_complete, on_window_complete.',
    )
    errors: list[ProbeError] = Field(
        default_factory=list, description='Every exception captured during the probe, with full traceback.'
    )
    truncated: bool = Field(
        default=False, description='True when the probe did not reach all stages (timeout or upstream failure).'
    )


# ---- helpers ---------------------------------------------------------------


def _make_value_preview(value: Any) -> str | None:
    """Return a short, single-line preview of ``value`` for the cache-call log.

    Truncates to ~120 chars and collapses newlines to spaces so the UI
    can render the cell on one line. Returns ``None`` for ``None``
    inputs so the UI cell stays empty (rather than displaying the
    literal string ``"None"``).
    """
    if value is None:
        return None
    # repr() handles bytes, numbers, dicts, Pydantic models; str() would
    # silently lose bytes and re-raise on exotic types.
    try:
        text = repr(value)
    except Exception as exc:  # pragma: no cover — truly unserialisable reprs
        text = f'<unrepr: {exc.__class__.__name__}>'
    text = text.replace('\n', ' ').replace('\r', ' ')
    if len(text) > 120:
        text = text[:117] + '...'
    return text


# ---- cache proxy -----------------------------------------------------------


class DebugCacheProxy:
    """Read-forwarding, write-suppressing wrapper around a live Cache / NoOpCache.

    Matches the surface of ``drakkar.cache.Cache`` so handler code that
    calls ``self.cache.get(...)`` / ``self.cache.set(...)`` works
    unchanged when the runner swaps the handler's ``cache`` attribute
    for a proxy instance.

    Behaviour:
      - ``get`` / ``peek`` / ``__contains__`` forward to the real cache
        iff ``use_cache=True``; otherwise immediately return a miss
        (respecting the real return types of each method).
      - ``set`` / ``delete`` are ALWAYS no-ops — writes never touch the
        live cache. A ``ProbeCacheCall`` with ``outcome='suppressed'`` is
        still logged so the operator can see what the handler WANTED to
        write.
      - Every call appends one ``ProbeCacheCall`` to ``self.calls`` with
        ``origin_stage`` pulled from the ``_probe_stage`` contextvar.

    Note on scope kwarg:
      The live ``Cache.set`` takes ``scope=CacheScope.LOCAL`` but the
      other sync methods (``peek`` / ``delete`` / ``__contains__``) do
      not. ``Cache.get`` only takes ``as_type``. The proxy mirrors those
      exact signatures so any handler code that works against the real
      Cache works against the proxy. We derive the ``scope`` field of
      ``ProbeCacheCall`` from the value actually passed to ``set``;
      other ops report the scope as ``'-'`` since the underlying Cache
      does not track it per-call.
    """

    def __init__(
        self,
        real: Cache | NoOpCache,
        *,
        use_cache: bool,
        start_time: float,
    ) -> None:
        """Wire up the proxy.

        Args:
            real: the live ``Cache`` or ``NoOpCache`` that the handler
                had before the probe swapped it out. Reads fall through
                to this when ``use_cache`` is True.
            use_cache: UI checkbox — forward reads to the live cache
                when True; otherwise always return a miss.
            start_time: ``time.monotonic()`` timestamp when the probe
                started. Used to compute ``ms_since_start`` on every
                logged call.
        """
        self._real = real
        self._use_cache = use_cache
        self._start_time = start_time
        self.calls: list[ProbeCacheCall] = []

    # -- internals -----------------------------------------------------------

    def _ms_since_start(self) -> float:
        """Return elapsed milliseconds since the probe began, as a float."""
        return (time.monotonic() - self._start_time) * 1000.0

    def _origin_stage(self) -> str:
        """Snapshot the current probe stage from the contextvar."""
        return _probe_stage.get()

    def _log_call(
        self,
        *,
        op: Literal['get', 'set', 'peek', 'delete', 'contains'],
        key: str,
        scope: str,
        outcome: Literal['hit', 'miss', 'suppressed'],
        value_preview: str | None,
    ) -> None:
        """Append one ProbeCacheCall to the call log."""
        self.calls.append(
            ProbeCacheCall(
                op=op,
                key=key,
                scope=scope,
                outcome=outcome,
                value_preview=value_preview,
                origin_stage=self._origin_stage(),
                ms_since_start=self._ms_since_start(),
            )
        )

    # -- read-side API (signatures match Cache) ------------------------------

    async def get(self, key: str, *, as_type: type[BaseModel] | None = None) -> Any | None:
        """Async get — forwards to the real cache iff ``use_cache=True``, else miss."""
        if self._use_cache:
            value = await self._real.get(key, as_type=as_type)
            outcome: Literal['hit', 'miss'] = 'hit' if value is not None else 'miss'
            self._log_call(
                op='get',
                key=key,
                scope='-',
                outcome=outcome,
                value_preview=_make_value_preview(value) if value is not None else None,
            )
            return value
        # use_cache=False → always report a miss, never touch the real cache.
        self._log_call(op='get', key=key, scope='-', outcome='miss', value_preview=None)
        return None

    def peek(self, key: str) -> Any | None:
        """Sync peek — forwards to the real cache iff ``use_cache=True``, else miss.

        Mirrors ``Cache.peek``: memory-only lookup, never hits the DB.
        """
        if self._use_cache:
            value = self._real.peek(key)
            outcome: Literal['hit', 'miss'] = 'hit' if value is not None else 'miss'
            self._log_call(
                op='peek',
                key=key,
                scope='-',
                outcome=outcome,
                value_preview=_make_value_preview(value) if value is not None else None,
            )
            return value
        self._log_call(op='peek', key=key, scope='-', outcome='miss', value_preview=None)
        return None

    def __contains__(self, key: str) -> bool:
        """Membership test — forwards to real iff ``use_cache=True``, else False."""
        if self._use_cache:
            present = key in self._real
            outcome: Literal['hit', 'miss'] = 'hit' if present else 'miss'
            self._log_call(op='contains', key=key, scope='-', outcome=outcome, value_preview=None)
            return present
        self._log_call(op='contains', key=key, scope='-', outcome='miss', value_preview=None)
        return False

    # -- write-side API (always suppressed) ----------------------------------

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = CacheScope.LOCAL,
    ) -> None:
        """Suppressed write. Never touches the real cache. Still logs the call."""
        self._log_call(
            op='set',
            key=key,
            scope=scope.name,
            outcome='suppressed',
            value_preview=_make_value_preview(value),
        )
        # Deliberate no-op. ``ttl`` is accepted for signature parity.
        _ = ttl

    def delete(self, key: str) -> bool:
        """Suppressed delete. Returns False unconditionally — probe never mutates state.

        Mirrors ``NoOpCache.delete`` which also returns False. The log
        entry records the user intent even though the live cache is
        untouched.
        """
        self._log_call(op='delete', key=key, scope='-', outcome='suppressed', value_preview=None)
        return False


# ---- sink collector --------------------------------------------------------


class DebugSinkCollector:
    """Capture every ``CollectResult`` the handler returns during the probe.

    Acts as a drop-in for the ``_on_collect`` callback that
    ``PartitionProcessor`` normally feeds into ``SinkManager``. In debug
    mode we never actually call ``SinkManager``; instead we just record
    every ``(stage, CollectResult)`` pair.

    ``flatten()`` turns the captured entries into a single list of
    ``PlannedSinkRecord`` items — one per payload inside every
    ``CollectResult``. Sorted by stage first, then by sink field in the
    canonical CollectResult order (kafka → postgres → mongo → http →
    redis → files) for stable UI ordering.
    """

    def __init__(self) -> None:
        self.entries: list[tuple[str, CollectResult]] = []

    async def __call__(self, collect_result: CollectResult, partition_id: int) -> None:
        """Capture one CollectResult. Signature matches ``_on_collect`` in PartitionProcessor.

        ``partition_id`` is ignored in debug mode — the probe operates on
        a single synthetic partition and the ID is known at the runner
        level. Kept in the signature so the same callable can be dropped
        in anywhere the real ``_on_collect`` is expected.
        """
        _ = partition_id
        stage = _probe_stage.get()
        self.entries.append((stage, collect_result))

    def flatten(self) -> list[PlannedSinkRecord]:
        """Expand captured CollectResults into one PlannedSinkRecord per payload.

        Walks every field on every captured ``CollectResult`` and emits
        one ``PlannedSinkRecord`` per payload, attaching:

          - ``sink_type`` — the CollectResult field name
          - ``destination`` — payload-specific (topic/table/collection/
             sink-name/key/path) so the UI can display something
             meaningful without having to look up the sink config
          - ``origin_stage`` — the stage tag captured when the
             CollectResult was collected
          - ``payload`` — the serialized ``data`` BaseModel from the
             payload (``model_dump()``)
          - ``extras`` — type-specific metadata like Kafka key, HTTP
             sink instance name, Redis TTL, etc.
        """
        records: list[PlannedSinkRecord] = []
        for stage, cr in self.entries:
            # Kafka payloads — destination is the configured sink instance
            # name since the real topic lives in config; the UI can map
            # it back using the kafka_source_topic global.
            for kp in cr.kafka:
                records.append(
                    PlannedSinkRecord(
                        sink_type='kafka',
                        destination=kp.sink or '(default)',
                        origin_stage=stage,
                        payload=kp.data.model_dump(mode='json'),
                        extras={
                            'sink_instance': kp.sink,
                            # bytes → UTF-8 decode (errors='replace') so
                            # the JSON roundtrip does not lose binary keys.
                            'key': kp.key.decode('utf-8', errors='replace') if kp.key is not None else None,
                        },
                    )
                )
            for pp in cr.postgres:
                records.append(
                    PlannedSinkRecord(
                        sink_type='postgres',
                        destination=pp.table,
                        origin_stage=stage,
                        payload=pp.data.model_dump(mode='json'),
                        extras={'sink_instance': pp.sink},
                    )
                )
            for mp in cr.mongo:
                records.append(
                    PlannedSinkRecord(
                        sink_type='mongo',
                        destination=mp.collection,
                        origin_stage=stage,
                        payload=mp.data.model_dump(mode='json'),
                        extras={'sink_instance': mp.sink},
                    )
                )
            for hp in cr.http:
                records.append(
                    PlannedSinkRecord(
                        sink_type='http',
                        destination=hp.sink or '(default)',
                        origin_stage=stage,
                        payload=hp.data.model_dump(mode='json'),
                        extras={'sink_instance': hp.sink},
                    )
                )
            for rp in cr.redis:
                records.append(
                    PlannedSinkRecord(
                        sink_type='redis',
                        destination=rp.key,
                        origin_stage=stage,
                        payload=rp.data.model_dump(mode='json'),
                        extras={'sink_instance': rp.sink, 'ttl': rp.ttl},
                    )
                )
            for fp in cr.files:
                records.append(
                    PlannedSinkRecord(
                        sink_type='files',
                        destination=fp.path,
                        origin_stage=stage,
                        payload=fp.data.model_dump(mode='json'),
                        extras={'sink_instance': fp.sink},
                    )
                )
        return records
