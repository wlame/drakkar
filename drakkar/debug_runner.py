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

3. **DebugRunner** — orchestrates the run, swaps the handler's ``cache``
   attribute for the duration of the probe, and produces the final
   ``DebugReport``. Keeps an incremental ``_partial`` dict so the
   endpoint can return a partial ``DebugReport(truncated=True)`` when a
   wall-clock timeout fires mid-run.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
import traceback
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, Field

from drakkar.cache import CacheScope
from drakkar.executor import ExecutorTaskError
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    PendingContext,
    SourceMessage,
)

if TYPE_CHECKING:
    from drakkar.cache import Cache, NoOpCache
    from drakkar.config import DrakkarConfig
    from drakkar.executor import ExecutorPool
    from drakkar.handler import BaseDrakkarHandler


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


# ---- on_error-raised sentinel ----------------------------------------------
#
# ``_invoke_on_error`` returns ``_ON_ERROR_RAISED`` when on_error itself
# raised — distinct from any valid action value (ErrorAction.* enum
# member, list of tasks). Using a singleton instance of a dedicated
# sentinel class keeps ty's type narrowing happy: the runtime check
# ``action is _ON_ERROR_RAISED`` narrows ``action`` out of the
# ``_OnErrorRaisedSentinel`` branch, leaving the union of the real
# action types (``str | list[ExecutorTask]``) for the following code.


class _OnErrorRaisedSentinel:
    """Marker type for the on_error-raised return path. See ``_ON_ERROR_RAISED``."""


_ON_ERROR_RAISED = _OnErrorRaisedSentinel()


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


# ---- runner ----------------------------------------------------------------


class DebugRunner:
    """Replay a single message through the handler pipeline with zero side effects.

    The runner reproduces ``PartitionProcessor`` behaviour (arrange →
    execute → on_task_complete → on_message_complete → on_window_complete)
    inline, but substitutes production-bound collaborators with debug
    shims:

      - ``handler.cache`` is swapped for a ``DebugCacheProxy`` for the
        duration of the probe (restored in a ``finally`` even if a hook
        raises).
      - ``ExecutorPool.execute`` is called with ``recorder=None``, which
        the pool already supports — every ``recorder.record_*`` call is
        gated on ``if recorder:``.
      - Every ``CollectResult`` a hook returns is fed to a
        ``DebugSinkCollector`` instead of the real ``SinkManager``.

    Concurrency:
      ``handler.cache`` is a process-wide attribute. Two concurrent probes
      would race each other's swap/restore cycle. We serialize probes
      with an ``asyncio.Lock`` held by the runner — a second concurrent
      probe simply waits.

    Partial reports:
      ``_partial`` is mutated incrementally as each stage completes. If
      the endpoint's wall-clock timeout fires, it calls
      ``latest_partial_report()`` to build a ``DebugReport(truncated=True)``
      from whatever state has accumulated so far.
    """

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        executor_pool: ExecutorPool,
        app_config: DrakkarConfig,
    ) -> None:
        """Hold refs to the live app components.

        Args:
            handler: the user's ``BaseDrakkarHandler`` instance. The runner
                swaps ``handler.cache`` for the probe's duration.
            executor_pool: the live subprocess pool — reused so the probe
                exercises the real binary and honours the real
                task_timeout.
            app_config: the active ``DrakkarConfig``. The probe uses
                ``app_config.executor.max_retries`` (task 4) and the
                configured source topic for empty ``ProbeInput.topic``
                requests (task 5 via the endpoint wiring).
        """
        self._handler = handler
        self._executor_pool = executor_pool
        self._app_config = app_config
        # Serializes concurrent probes. The handler.cache swap is
        # process-wide, so two overlapping probes would clobber each
        # other's restore step without this lock.
        self._probe_lock = asyncio.Lock()
        # Incremental report state mutated stage-by-stage so the endpoint
        # can build a truncated DebugReport if wall-clock timeout fires
        # mid-run. Reset at the top of every run().
        self._partial: dict[str, Any] = {}
        # Scratch slot used by ``_execute_and_record_task`` to communicate
        # the ExecutorTaskError it caught (or None on success) back to
        # its caller in ``_process_task`` / ``_run_retry_loop``. Returning
        # a tuple would leak an implementation detail across all of the
        # task-execution helpers; this attribute keeps the public return
        # as just ``ProbeTaskEntry``.
        self._last_exec_error: ExecutorTaskError | None = None

    # -- incremental partial-report machinery --------------------------------

    def _update_partial(self, key: str, value: Any) -> None:
        """Store one piece of partial report state.

        Thin wrapper around dict assignment. Kept as a helper so future
        partial-report hooks (metrics, logs, etc.) have a single entry
        point to patch.
        """
        self._partial[key] = value

    def latest_partial_report(self) -> DebugReport:
        """Snapshot current ``_partial`` into a ``DebugReport(truncated=True)``.

        Called by the endpoint on wall-clock timeout. Missing sections
        fall back to safe defaults (empty lists, zero durations) so the
        UI can render whatever made it through.
        """
        # ``input`` MUST be present — run() sets it before anything else.
        # If it is absent (latest_partial_report called before run()
        # started), synthesise an empty ProbeInput so we still produce
        # a valid DebugReport.
        probe_input = self._partial.get('input') or ProbeInput(value='')
        arrange_result = self._partial.get('arrange') or ProbeStageResult()
        cache_calls = list(self._partial.get('cache_calls', []))
        return DebugReport(
            input=probe_input,
            deserialize_error=self._partial.get('deserialize_error'),
            parsed_payload=self._partial.get('parsed_payload'),
            message_label=self._partial.get('message_label'),
            arrange=arrange_result,
            tasks=list(self._partial.get('tasks', [])),
            on_message_complete=self._partial.get('on_message_complete'),
            on_window_complete=self._partial.get('on_window_complete'),
            planned_sink_payloads=list(self._partial.get('planned_sink_payloads', [])),
            cache_calls=cache_calls,
            cache_summary=_summarize_cache_calls(cache_calls),
            timing=dict(self._partial.get('timing', {})),
            errors=list(self._partial.get('errors', [])),
            truncated=True,
        )

    # -- top-level entrypoint ------------------------------------------------

    async def run(self, probe_input: ProbeInput) -> DebugReport:
        """Execute the full probe and return a ``DebugReport(truncated=False)``.

        Runs under ``self._probe_lock`` so overlapping probes serialize.
        Swaps ``handler.cache`` with ``DebugCacheProxy`` inside a
        ``try/finally`` that unconditionally restores the original cache
        — even if the handler raises or the task is cancelled by the
        endpoint's wall-clock timeout.
        """
        async with self._probe_lock:
            return await self._run_locked(probe_input)

    async def _run_locked(self, probe_input: ProbeInput) -> DebugReport:
        """Body of run() executed under the probe lock.

        Split into a helper so the lock boundary is visible in ``run``
        and the implementation below can concentrate on the state
        machine without an extra indentation level.
        """
        self._partial = {}
        self._update_partial('input', probe_input)

        start_monotonic = time.monotonic()
        msg = _build_source_message(probe_input)

        sink_collector = DebugSinkCollector()
        cache_proxy = DebugCacheProxy(
            real=self._handler.cache,
            use_cache=probe_input.use_cache,
            start_time=start_monotonic,
        )
        # Snapshot the call log list onto _partial so latest_partial_report
        # can see cache calls captured before the run completes.
        self._update_partial('cache_calls', cache_proxy.calls)

        original_cache = self._handler.cache
        # Use setattr to dodge the static type check — DebugCacheProxy
        # duck-types the Cache surface (verified by tests) but isn't in
        # the Cache | NoOpCache union ty sees on the handler attribute.
        # Keeping the swap out of the type system is intentional: the
        # proxy should never leak beyond the probe.
        setattr(self._handler, 'cache', cache_proxy)  # noqa: B010
        try:
            await self._run_stages(
                msg=msg,
                sink_collector=sink_collector,
                start_monotonic=start_monotonic,
            )
        finally:
            # Restore cache even if a hook raised or the task was
            # cancelled (wall-clock timeout path).
            setattr(self._handler, 'cache', original_cache)  # noqa: B010

        # Finalize planned sink records + timing + cache summary now that
        # the run is done. All other fields were filled in by _run_stages.
        self._update_partial('planned_sink_payloads', sink_collector.flatten())
        timing = dict(self._partial.get('timing', {}))
        timing['total_wallclock'] = time.monotonic() - start_monotonic
        self._update_partial('timing', timing)

        return DebugReport(
            input=probe_input,
            deserialize_error=self._partial.get('deserialize_error'),
            parsed_payload=self._partial.get('parsed_payload'),
            message_label=self._partial.get('message_label'),
            arrange=self._partial.get('arrange') or ProbeStageResult(),
            tasks=list(self._partial.get('tasks', [])),
            on_message_complete=self._partial.get('on_message_complete'),
            on_window_complete=self._partial.get('on_window_complete'),
            planned_sink_payloads=list(self._partial.get('planned_sink_payloads', [])),
            cache_calls=list(cache_proxy.calls),
            cache_summary=_summarize_cache_calls(cache_proxy.calls),
            timing=timing,
            errors=list(self._partial.get('errors', [])),
            truncated=False,
        )

    # -- stage sequencing ----------------------------------------------------

    async def _run_stages(
        self,
        *,
        msg: SourceMessage,
        sink_collector: DebugSinkCollector,
        start_monotonic: float,
    ) -> None:
        """Run the arrange → per-task → window sequence with graceful error capture.

        Extracted into its own coroutine so the ``finally`` block in
        ``_run_locked`` only has to worry about cache restoration —
        stage logic lives here. Task 3 added try/except wrappers around
        every hook so exceptions never crash the probe — they land in
        ``self._partial['errors']`` as ``ProbeError`` entries and the
        runner short-circuits downstream stages based on which hook
        failed (see ``_record_error`` and the per-stage logic below).

        Short-circuit rules:
          - deserialize error → skip all downstream stages
          - message_label error → non-fatal, keep going
          - arrange error → skip tasks + hooks
          - on_task_complete error → record on the task entry, keep processing other tasks
          - on_message_complete error → still run on_window_complete
          - on_window_complete error → capture and return
        """
        # Guarantee 'errors' list exists on _partial. latest_partial_report()
        # reads it, and every error-capture path appends to it.
        self._partial.setdefault('errors', [])

        # --- deserialize -----------------------------------------------------
        # Deserialize is the first stage. If it fails, we cannot usefully
        # build tasks or call hooks that depend on a parsed payload.
        deserialize_ok = self._run_deserialize(msg=msg, start_monotonic=start_monotonic)
        if not deserialize_ok:
            return

        # --- message_label ---------------------------------------------------
        # Label is a cosmetic string used for UI rows / logs. Failing here
        # must NOT skip downstream stages; we just leave message_label as
        # None and move on.
        self._run_message_label(msg=msg, start_monotonic=start_monotonic)

        # --- arrange ---------------------------------------------------------
        arrange_start = time.monotonic()
        tasks = await self._run_arrange(msg=msg, start_monotonic=start_monotonic, arrange_start=arrange_start)
        if tasks is None:
            # Arrange failed — skip tasks + both hook stages. The error is
            # already captured on _partial['errors'] and _partial['arrange'].
            return

        # --- per-task execution ---------------------------------------------
        # Maintain a live list on _partial so latest_partial_report() sees
        # progress as tasks finish.
        task_entries: list[ProbeTaskEntry] = []
        self._update_partial('tasks', task_entries)
        terminal_results: list[ExecutorResult] = []

        for task in tasks:
            # _process_task is the on_error-aware orchestrator: it handles
            # RETRY (up to max_retries), replacement lists, SKIP, and
            # on_error itself raising — appending one ProbeTaskEntry per
            # execute attempt (including retries and replacements).
            await self._process_task(
                task=task,
                msg=msg,
                sink_collector=sink_collector,
                terminal_results=terminal_results,
                task_entries=task_entries,
                start_monotonic=start_monotonic,
            )

        # --- on_message_complete --------------------------------------------
        # on_message_complete is non-blocking for on_window_complete: even
        # if it raises, we still invoke on_window_complete so the operator
        # can see its independent behaviour.
        await self._run_on_message_complete(
            msg=msg,
            tasks=tasks,
            terminal_results=terminal_results,
            sink_collector=sink_collector,
            arrange_start=arrange_start,
            start_monotonic=start_monotonic,
        )

        # --- on_window_complete ---------------------------------------------
        await self._run_on_window_complete(
            msg=msg,
            terminal_results=terminal_results,
            sink_collector=sink_collector,
            start_monotonic=start_monotonic,
        )

        # Expose the final sink-collector flattening on _partial so a
        # partial-report snapshot taken during on_window_complete
        # captures everything up to that point.
        self._update_partial('planned_sink_payloads', sink_collector.flatten())
        _ = start_monotonic  # parameter reserved for wall-clock tracking (task 5)

    # -- per-stage helpers ---------------------------------------------------
    #
    # Each helper runs one hook stage with error capture. They return a
    # value (or a flag) so the caller can decide whether to short-circuit
    # the pipeline. Keeping each stage in its own method keeps _run_stages
    # readable and mirrors the stage-by-stage rules documented in the plan.

    def _run_deserialize(self, *, msg: SourceMessage, start_monotonic: float) -> bool:
        """Run handler.deserialize_message with error capture. Returns True on success.

        Returning False signals the caller to stop — the parsed payload
        is missing and every downstream stage would be meaningless.
        """
        token = _probe_stage.set('deserialize')
        try:
            try:
                self._handler.deserialize_message(msg)
            except Exception as exc:
                probe_error = self._build_probe_error(
                    stage='deserialize',
                    exc=exc,
                    start_monotonic=start_monotonic,
                )
                self._record_error(probe_error)
                # Also surface as a dedicated field — the UI renders
                # deserialize errors in section A with special emphasis.
                self._update_partial('deserialize_error', probe_error)
                return False
        finally:
            _probe_stage.reset(token)
        # deserialize_message mutates msg.payload in place. Serialize
        # pydantic models via model_dump so the JSON roundtrip used by
        # the endpoint preserves structure.
        self._update_partial('parsed_payload', _serialize_payload(msg.payload))
        return True

    def _run_message_label(self, *, msg: SourceMessage, start_monotonic: float) -> None:
        """Run handler.message_label with error capture. Non-fatal on error."""
        token = _probe_stage.set('message_label')
        try:
            try:
                label = self._handler.message_label(msg)
            except Exception as exc:
                self._record_error(
                    self._build_probe_error(
                        stage='message_label',
                        exc=exc,
                        start_monotonic=start_monotonic,
                    )
                )
                # Leave message_label as None; downstream stages carry on.
                return
        finally:
            _probe_stage.reset(token)
        self._update_partial('message_label', label)

    async def _run_arrange(
        self,
        *,
        msg: SourceMessage,
        start_monotonic: float,
        arrange_start: float,
    ) -> list[ExecutorTask] | None:
        """Run handler.arrange with error capture. Returns None to signal fatal failure.

        Populates ``_partial['arrange']`` with a ``ProbeStageResult`` in
        both success and failure cases — on failure the ``error`` field
        carries a one-line summary and the full traceback is on
        ``_partial['errors']``.
        """
        token = _probe_stage.set('arrange')
        try:
            try:
                tasks = await self._handler.arrange([msg], PendingContext())
            except Exception as exc:
                arrange_duration = time.monotonic() - arrange_start
                probe_error = self._build_probe_error(
                    stage='arrange',
                    exc=exc,
                    start_monotonic=start_monotonic,
                )
                self._record_error(probe_error)
                self._update_partial(
                    'arrange',
                    ProbeStageResult(
                        duration_seconds=arrange_duration,
                        error=_one_line_summary(exc),
                    ),
                )
                timing = dict(self._partial.get('timing', {}))
                timing['arrange'] = arrange_duration
                self._update_partial('timing', timing)
                return None
        finally:
            _probe_stage.reset(token)

        arrange_duration = time.monotonic() - arrange_start
        self._update_partial(
            'arrange',
            ProbeStageResult(duration_seconds=arrange_duration),
        )
        timing = dict(self._partial.get('timing', {}))
        timing['arrange'] = arrange_duration
        self._update_partial('timing', timing)
        return tasks

    async def _run_on_message_complete(
        self,
        *,
        msg: SourceMessage,
        tasks: list[ExecutorTask],
        terminal_results: list[ExecutorResult],
        sink_collector: DebugSinkCollector,
        arrange_start: float,
        start_monotonic: float,
    ) -> None:
        """Run handler.on_message_complete with error capture.

        Even on failure, the runner continues to on_window_complete —
        the two hooks are independent (per plan rules), so a broken
        on_message_complete should not mask on_window_complete's
        behaviour from the operator.
        """
        mc_start = time.monotonic()
        token = _probe_stage.set('message_complete')
        try:
            try:
                group = MessageGroup(
                    source_message=msg,
                    tasks=list(tasks),
                    results=list(terminal_results),
                    errors=[],
                    started_at=arrange_start,
                    finished_at=time.monotonic(),
                )
                mc_result = await self._handler.on_message_complete(group)
            except Exception as exc:
                mc_duration = time.monotonic() - mc_start
                self._record_error(
                    self._build_probe_error(
                        stage='on_message_complete',
                        exc=exc,
                        start_monotonic=start_monotonic,
                    )
                )
                # Keep collect_result=None so the UI can distinguish "hook
                # raised" from "hook returned None by design".
                self._update_partial(
                    'on_message_complete',
                    ProbeStageResult(
                        duration_seconds=mc_duration,
                        collect_result=None,
                        error=_one_line_summary(exc),
                    ),
                )
                timing = dict(self._partial.get('timing', {}))
                timing['on_message_complete'] = mc_duration
                self._update_partial('timing', timing)
                return
        finally:
            _probe_stage.reset(token)

        mc_duration = time.monotonic() - mc_start
        # If the hook returned a CollectResult, route it through the sink
        # collector — same behaviour as PartitionProcessor's _on_collect
        # callback, minus the real SinkManager write.
        if mc_result is not None:
            token = _probe_stage.set('message_complete')
            try:
                await sink_collector(mc_result, msg.partition)
            finally:
                _probe_stage.reset(token)
        self._update_partial(
            'on_message_complete',
            ProbeStageResult(duration_seconds=mc_duration, collect_result=mc_result),
        )
        timing = dict(self._partial.get('timing', {}))
        timing['on_message_complete'] = mc_duration
        self._update_partial('timing', timing)

    async def _run_on_window_complete(
        self,
        *,
        msg: SourceMessage,
        terminal_results: list[ExecutorResult],
        sink_collector: DebugSinkCollector,
        start_monotonic: float,
    ) -> None:
        """Run handler.on_window_complete with error capture. Always the last stage."""
        wc_start = time.monotonic()
        token = _probe_stage.set('window_complete')
        try:
            try:
                wc_result = await self._handler.on_window_complete(
                    list(terminal_results),
                    [msg],
                )
            except Exception as exc:
                wc_duration = time.monotonic() - wc_start
                self._record_error(
                    self._build_probe_error(
                        stage='window_complete',
                        exc=exc,
                        start_monotonic=start_monotonic,
                    )
                )
                self._update_partial(
                    'on_window_complete',
                    ProbeStageResult(
                        duration_seconds=wc_duration,
                        collect_result=None,
                        error=_one_line_summary(exc),
                    ),
                )
                timing = dict(self._partial.get('timing', {}))
                timing['on_window_complete'] = wc_duration
                self._update_partial('timing', timing)
                return
        finally:
            _probe_stage.reset(token)

        wc_duration = time.monotonic() - wc_start
        if wc_result is not None:
            token = _probe_stage.set('window_complete')
            try:
                await sink_collector(wc_result, msg.partition)
            finally:
                _probe_stage.reset(token)
        self._update_partial(
            'on_window_complete',
            ProbeStageResult(duration_seconds=wc_duration, collect_result=wc_result),
        )
        timing = dict(self._partial.get('timing', {}))
        timing['on_window_complete'] = wc_duration
        self._update_partial('timing', timing)

    async def _process_task(
        self,
        *,
        task: ExecutorTask,
        msg: SourceMessage,
        sink_collector: DebugSinkCollector,
        terminal_results: list[ExecutorResult],
        task_entries: list[ProbeTaskEntry],
        start_monotonic: float,
        retry_of: str | None = None,
        replacement_for: str | None = None,
    ) -> None:
        """Run ONE task end-to-end with the full on_error / retry / replace cycle.

        Appends one ``ProbeTaskEntry`` per executor attempt to
        ``task_entries`` (so retries and replacements also land in the
        Tasks section of the UI). The live list is kept on ``_partial``
        so ``latest_partial_report`` sees in-flight progress.

        Flow:
          1. Execute the task via ``_execute_and_record_task``.
          2. On success (no ``ExecutorTaskError``) → append the entry,
             on_task_complete already fed into the sink collector.
          3. On failure:
             a. Call ``handler.on_error(task, error)`` with stage
                ``on_error:<task_id>``. If it raises, record a ProbeError
                and mark the entry ``status='failed'``.
             b. If the action is ``ErrorAction.RETRY``:
                  - re-execute the SAME task while ``retry_count < max_retries``.
                    Each attempt produces its own ``ProbeTaskEntry`` with
                    ``retry_of=<parent>`` set. Previous entries keep
                    ``status='failed'`` — the retry entry reflects the
                    retry's terminal outcome (done/failed).
                  - Once retries are exhausted (handler still wants RETRY
                    but budget is gone), the last failed entry is kept
                    as ``status='failed'``.
             c. If the action is a ``list[ExecutorTask]`` (replacements):
                  - mark the original entry ``status='replaced'`` and
                    recurse for each replacement with
                    ``replacement_for=<parent_task_id>``.
             d. If the action is ``ErrorAction.SKIP`` (or any other value):
                  - the failed entry keeps ``status='failed'``; we stop.

        Retries and replacement cascades each run their OWN on_error
        cycle, so a retry that fails again goes through on_error too (up
        to the per-task retry budget). Replacement tasks that themselves
        fail can trigger a fresh on_error; we don't cap recursion — that
        mirrors production semantics. A broken handler that infinitely
        returns replacements would hang the probe the same way it would
        hang the real worker.
        """
        entry = await self._execute_and_record_task(
            task=task,
            msg=msg,
            sink_collector=sink_collector,
            terminal_results=terminal_results,
            start_monotonic=start_monotonic,
            retry_of=retry_of,
            replacement_for=replacement_for,
        )
        task_entries.append(entry)

        # Success path → nothing more to do for this task.
        exec_error = self._last_exec_error
        self._last_exec_error = None  # reset so the next call starts clean
        if exec_error is None:
            return

        # -- on_error path ----------------------------------------------------
        await self._handle_task_failure(
            task=task,
            failed_entry=entry,
            exec_error=exec_error,
            msg=msg,
            sink_collector=sink_collector,
            terminal_results=terminal_results,
            task_entries=task_entries,
            start_monotonic=start_monotonic,
        )

    async def _handle_task_failure(
        self,
        *,
        task: ExecutorTask,
        failed_entry: ProbeTaskEntry,
        exec_error: ExecutorTaskError,
        msg: SourceMessage,
        sink_collector: DebugSinkCollector,
        terminal_results: list[ExecutorResult],
        task_entries: list[ProbeTaskEntry],
        start_monotonic: float,
    ) -> None:
        """Run on_error for ``failed_entry`` and react to its returned action.

        Branches out into three production-mirroring paths (RETRY /
        replacement list / SKIP-or-other) plus the on_error-itself-raises
        path. See ``_process_task`` docstring for the full state machine.
        """
        action = await self._invoke_on_error(
            task=task,
            exec_error=exec_error,
            start_monotonic=start_monotonic,
        )
        if action is _ON_ERROR_RAISED:
            # on_error itself raised — ProbeError already recorded.
            # failed_entry keeps its default status='failed'.
            return

        # Replacement list: original → 'replaced', recurse for each new task.
        if isinstance(action, list):
            failed_entry.status = 'replaced'
            # ty can narrow ``action`` to ``list`` via isinstance, but not
            # the element type. The on_error contract (see
            # BaseDrakkarHandler.on_error) guarantees list members are
            # ExecutorTask instances, so the cast is safe.
            replacements = cast('list[ExecutorTask]', action)
            for replacement in replacements:
                await self._process_task(
                    task=replacement,
                    msg=msg,
                    sink_collector=sink_collector,
                    terminal_results=terminal_results,
                    task_entries=task_entries,
                    start_monotonic=start_monotonic,
                    replacement_for=task.task_id,
                )
            return

        # RETRY path: re-execute up to max_retries more attempts, each as a
        # fresh ProbeTaskEntry with retry_of=<original task_id>. The loop
        # stops on the first success OR when max_retries is reached OR when
        # on_error stops returning RETRY.
        if action == ErrorAction.RETRY:
            await self._run_retry_loop(
                task=task,
                msg=msg,
                sink_collector=sink_collector,
                terminal_results=terminal_results,
                task_entries=task_entries,
                start_monotonic=start_monotonic,
            )
            return

        # SKIP or any unrecognized action → leave the failed entry as-is.
        # This matches production's "else: window.results.append(e.result)"
        # branch in PartitionProcessor._execute_and_track.

    async def _run_retry_loop(
        self,
        *,
        task: ExecutorTask,
        msg: SourceMessage,
        sink_collector: DebugSinkCollector,
        terminal_results: list[ExecutorResult],
        task_entries: list[ProbeTaskEntry],
        start_monotonic: float,
    ) -> None:
        """Re-execute ``task`` up to ``max_retries`` times, recording each attempt.

        Called after the FIRST attempt has already failed and on_error
        returned RETRY. ``max_retries`` is the total retry budget — a
        config value of 3 means up to 3 retry attempts after the initial
        failure (so 4 total executor invocations in the worst case, same
        as production).

        Each retry attempt:
          - Appends its own ``ProbeTaskEntry`` (with ``retry_of`` set).
          - On success → stops the loop.
          - On failure → calls on_error; if still RETRY and budget
            remains, continues. If on_error returns a list or something
            else, the loop exits and that branch is handled via
            ``_handle_task_failure`` on the retry entry.
        """
        max_retries = self._app_config.executor.max_retries
        retry_count = 0
        while retry_count < max_retries:
            retry_count += 1
            retry_entry = await self._execute_and_record_task(
                task=task,
                msg=msg,
                sink_collector=sink_collector,
                terminal_results=terminal_results,
                start_monotonic=start_monotonic,
                retry_of=task.task_id,
                replacement_for=None,
            )
            task_entries.append(retry_entry)
            exec_error = self._last_exec_error
            self._last_exec_error = None
            if exec_error is None:
                # Retry succeeded — done.
                return
            # Retry failed. Ask the handler what to do next.
            action = await self._invoke_on_error(
                task=task,
                exec_error=exec_error,
                start_monotonic=start_monotonic,
            )
            if action is _ON_ERROR_RAISED:
                return
            if isinstance(action, list):
                # Replacement path off a retry — retry_entry becomes
                # 'replaced' and we recurse on the replacements. See the
                # matching cast in ``_handle_task_failure`` for why the
                # cast is needed and safe.
                retry_entry.status = 'replaced'
                replacements = cast('list[ExecutorTask]', action)
                for replacement in replacements:
                    await self._process_task(
                        task=replacement,
                        msg=msg,
                        sink_collector=sink_collector,
                        terminal_results=terminal_results,
                        task_entries=task_entries,
                        start_monotonic=start_monotonic,
                        replacement_for=task.task_id,
                    )
                return
            if action != ErrorAction.RETRY:
                # SKIP or unknown → stop, retry_entry stays 'failed'.
                return
            # RETRY again and budget allows → next loop iteration.

    async def _invoke_on_error(
        self,
        *,
        task: ExecutorTask,
        exec_error: ExecutorTaskError,
        start_monotonic: float,
    ) -> str | list[ExecutorTask] | _OnErrorRaisedSentinel:
        """Call ``handler.on_error`` with error capture. Returns the raw action or a sentinel.

        Returns ``_ON_ERROR_RAISED`` when on_error itself raises (the
        ProbeError has already been appended). Otherwise returns the
        action verbatim so the caller can pattern-match on RETRY / list /
        SKIP / other.

        The return type uses ``str`` (not ``ErrorAction``) because
        ``ErrorAction`` is a ``StrEnum`` — user handlers are free to
        return the raw string ``'retry'`` / ``'skip'`` instead of the
        enum member. The caller uses ``action == ErrorAction.RETRY`` to
        treat both cases identically (StrEnum equality matches the
        underlying str).
        """
        stage = f'on_error:{task.task_id}'
        token = _probe_stage.set(stage)
        try:
            try:
                return await self._handler.on_error(task, exec_error.error)
            except Exception as exc:
                self._record_error(
                    self._build_probe_error(
                        stage=stage,
                        exc=exc,
                        start_monotonic=start_monotonic,
                    )
                )
                return _ON_ERROR_RAISED
        finally:
            _probe_stage.reset(token)

    async def _execute_and_record_task(
        self,
        *,
        task: ExecutorTask,
        msg: SourceMessage,
        sink_collector: DebugSinkCollector,
        terminal_results: list[ExecutorResult],
        start_monotonic: float,
        retry_of: str | None,
        replacement_for: str | None,
    ) -> ProbeTaskEntry:
        """Execute one attempt and return its ``ProbeTaskEntry``.

        Sets ``self._last_exec_error`` to the caught ``ExecutorTaskError``
        on failure, or ``None`` on success — a poor-man's second return
        value that keeps the call sites tidy (Python doesn't let us
        return a union cleanly without forcing every caller through the
        same unpacking dance).

        On success: runs ``on_task_complete``, feeds any returned
        ``CollectResult`` into the sink collector, returns a
        ``status='done'`` entry with stdout/stderr/etc attached.

        On failure: records a ``ProbeError`` via ``_record_error`` for
        ANY ``on_task_complete`` exception (still runs it even on success
        path), returns a ``status='failed'`` entry built from the
        ``ExecutorError`` attached to the exception.
        """
        # -- executor execute ------------------------------------------------
        token = _probe_stage.set(f'executor:{task.task_id}')
        try:
            try:
                exec_result = await self._executor_pool.execute(
                    task,
                    recorder=None,
                    partition_id=msg.partition,
                )
            except ExecutorTaskError as exc:
                self._last_exec_error = exc
                # Subprocess-level failure — build a 'failed' entry from
                # the ExecutorError payload attached to the exception.
                # on_error / retry / replace logic happens in the caller.
                return _failed_task_entry(
                    task=task,
                    error=exc,
                    retry_of=retry_of,
                    replacement_for=replacement_for,
                )
        finally:
            _probe_stage.reset(token)

        self._last_exec_error = None
        terminal_results.append(exec_result)

        # -- on_task_complete for this task --------------------------------
        tc_start = time.monotonic()
        tc_result: CollectResult | None = None
        tc_error_summary: str | None = None
        token = _probe_stage.set(f'task_complete:{task.task_id}')
        try:
            try:
                tc_result = await self._handler.on_task_complete(exec_result)
            except Exception as exc:
                tc_error_summary = _one_line_summary(exc)
                self._record_error(
                    self._build_probe_error(
                        stage=f'task_complete:{task.task_id}',
                        exc=exc,
                        start_monotonic=start_monotonic,
                    )
                )
        finally:
            _probe_stage.reset(token)
        tc_duration = time.monotonic() - tc_start

        # Only feed the sink collector on success — a raised
        # on_task_complete produced no result to forward.
        if tc_result is not None:
            token = _probe_stage.set(f'task_complete:{task.task_id}')
            try:
                await sink_collector(tc_result, msg.partition)
            finally:
                _probe_stage.reset(token)

        return ProbeTaskEntry(
            task_id=task.task_id,
            parent_task_id=task.parent_task_id,
            labels=dict(task.labels),
            source_offsets=list(task.source_offsets),
            precomputed=task.precomputed is not None,
            status='done',
            exit_code=exec_result.exit_code,
            duration_seconds=exec_result.duration_seconds,
            stdin=task.stdin or '',
            stdout=exec_result.stdout,
            stderr=exec_result.stderr,
            on_task_complete_duration=tc_duration,
            on_task_complete_result=tc_result,
            on_task_complete_error=tc_error_summary,
            retry_of=retry_of,
            replacement_for=replacement_for,
        )

    # -- error-capture helpers ----------------------------------------------

    def _build_probe_error(
        self,
        *,
        stage: str,
        exc: BaseException,
        start_monotonic: float,
    ) -> ProbeError:
        """Wrap an exception in a ``ProbeError`` with a captured traceback.

        ``traceback.format_exc()`` reads the CURRENT exception context,
        so this must only be called from inside an ``except`` block — the
        helper assumes its caller just caught ``exc`` and the frame is
        still active.
        """
        return ProbeError(
            stage=stage,
            exception_class=type(exc).__name__,
            message=str(exc),
            traceback=traceback.format_exc(),
            occurred_at_ms=(time.monotonic() - start_monotonic) * 1000.0,
        )

    def _record_error(self, error: ProbeError) -> None:
        """Append a ``ProbeError`` to the running ``_partial['errors']`` list.

        Kept as a single choke-point so future code (metrics, structured
        logging of captured errors) has one place to patch. The list is
        initialised at the top of ``_run_stages``.
        """
        errors: list[ProbeError] = self._partial.setdefault('errors', [])
        errors.append(error)


# ---- small helpers used by DebugRunner -------------------------------------


def _build_source_message(probe_input: ProbeInput) -> SourceMessage:
    """Build a synthetic ``SourceMessage`` from the probe input.

    - Encodes ``value`` and ``key`` as UTF-8 bytes (the on-wire Kafka shape).
    - Defaults ``timestamp`` to the current wall-clock time in ms when
      the input did not supply one (Kafka-style epoch ms).
    """
    timestamp = probe_input.timestamp
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    return SourceMessage(
        topic=probe_input.topic,
        partition=probe_input.partition,
        offset=probe_input.offset,
        key=probe_input.key.encode('utf-8') if probe_input.key is not None else None,
        value=probe_input.value.encode('utf-8'),
        timestamp=timestamp,
    )


def _serialize_payload(payload: Any) -> Any:
    """Serialize ``msg.payload`` into a JSON-safe shape for the report.

    ``deserialize_message`` typically sets ``msg.payload`` to a Pydantic
    BaseModel instance. Pydantic models round-trip cleanly through
    ``model_dump(mode='json')``; everything else (None, dict, primitive)
    passes through unchanged so user handlers that store plain dicts
    still work.
    """
    if payload is None:
        return None
    if isinstance(payload, BaseModel):
        return payload.model_dump(mode='json')
    return payload


def _summarize_cache_calls(calls: list[ProbeCacheCall]) -> dict[str, int]:
    """Compute the ``cache_summary`` counts used in the Cache calls header.

    Keys: ``calls`` (total), ``hits`` (read+hit), ``misses`` (read+miss),
    ``writes_suppressed`` (set/delete calls).
    """
    hits = sum(1 for c in calls if c.outcome == 'hit')
    misses = sum(1 for c in calls if c.outcome == 'miss')
    writes_suppressed = sum(1 for c in calls if c.outcome == 'suppressed')
    return {
        'calls': len(calls),
        'hits': hits,
        'misses': misses,
        'writes_suppressed': writes_suppressed,
    }


def _one_line_summary(exc: BaseException) -> str:
    """Short one-line summary of an exception for the stage's ``error`` field.

    The full traceback is always captured separately on the top-level
    ``DebugReport.errors`` list; this short form is what the UI shows
    inline on the corresponding stage card (arrange, message_complete,
    etc.) so the operator sees at-a-glance what went wrong.
    """
    message = str(exc) or '<no message>'
    # Collapse any embedded newlines so the cell stays on one line.
    message = message.replace('\n', ' ').replace('\r', ' ')
    return f'{type(exc).__name__}: {message}'


def _failed_task_entry(
    *,
    task: ExecutorTask,
    error: ExecutorTaskError,
    retry_of: str | None = None,
    replacement_for: str | None = None,
) -> ProbeTaskEntry:
    """Build a ``ProbeTaskEntry`` for a subprocess-level task failure.

    Used when ``ExecutorPool.execute`` raises ``ExecutorTaskError`` —
    the task never produced a successful ``ExecutorResult``, so we
    synthesise an entry from the ``ExecutorError`` attached to the
    exception. The entry starts with ``status='failed'``; callers may
    later flip it to ``'replaced'`` if on_error returns a replacement
    list (see ``DebugRunner._handle_task_failure``).

    ``retry_of`` / ``replacement_for`` thread through on_error retries
    and replacement cascades so the UI can show the lineage of each
    entry (see Tasks section C in the probe tab).
    """
    err = error.error
    result = error.result
    # Prefer the exception text when it's set (timeouts, launch failures);
    # otherwise fall back to stderr for non-zero-exit-code failures.
    subprocess_exception = err.exception or (err.stderr or None)
    return ProbeTaskEntry(
        task_id=task.task_id,
        parent_task_id=task.parent_task_id,
        labels=dict(task.labels),
        source_offsets=list(task.source_offsets),
        precomputed=task.precomputed is not None,
        status='failed',
        exit_code=err.exit_code,
        duration_seconds=result.duration_seconds,
        stdin=task.stdin or '',
        stdout=result.stdout,
        stderr=result.stderr,
        subprocess_exception=subprocess_exception,
        retry_of=retry_of,
        replacement_for=replacement_for,
    )
