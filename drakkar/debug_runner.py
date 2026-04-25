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
import time
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from drakkar.cache import CacheScope
from drakkar.debug_runner_helpers import (
    _build_source_message,
    _failed_task_entry,
    _make_value_preview,
    _one_line_summary,
    _serialize_payload,
    _summarize_cache_calls,
)
from drakkar.debug_runner_models import (
    DebugReport,
    PlannedSinkRecord,
    ProbeCacheCall,
    ProbeError,
    ProbeInput,
    ProbeStageResult,
    ProbeTaskEntry,
    _probe_stage,
    _stage,
)
from drakkar.executor import ExecutorTaskError
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorError,
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


class DebugCacheProxy:
    """Read-forwarding, write-suppressing wrapper around a live Cache / NoOpCache.

    Matches the surface of ``drakkar.cache.Cache`` so handler code that
    calls ``self.cache.get(...)`` / ``self.cache.set(...)`` works
    unchanged when the runner swaps the handler's ``cache`` attribute
    for a proxy instance.

    Concurrent-traffic isolation
    ----------------------------
    The probe swaps ``handler.cache`` on the SHARED handler instance —
    the same one the live worker uses for production messages. Without
    a guard, any production task that calls ``self.cache.X`` while the
    probe is in flight would (a) land in the probe's call log as
    pollution and (b) have its writes silently dropped because the
    proxy suppresses writes by design.

    To prevent both, every entry point checks the ``_probe_stage``
    contextvar. asyncio gives each task its own contextvar context, so
    only the task that the probe is running in (which sets the stage
    via ``with _stage('arrange')`` etc.) sees a non-default value.
    Concurrent production tasks see the default ``'unknown'`` and the
    proxy delegates straight to ``self._real`` with NO logging — fully
    transparent. The probe report therefore contains only the probe's
    own pipeline calls, and production cache writes survive.

    Behaviour (probe-stage caller — ``_probe_stage.get() != 'unknown'``):
      - ``get`` / ``peek`` / ``__contains__`` forward to the real cache
        iff ``use_cache=True``; otherwise immediately return a miss
        (respecting the real return types of each method).
      - ``set`` / ``delete`` are ALWAYS no-ops — writes never touch the
        live cache. A ``ProbeCacheCall`` with ``outcome='suppressed'`` is
        still logged so the operator can see what the handler WANTED to
        write.
      - Every call appends one ``ProbeCacheCall`` to ``self.calls`` with
        ``origin_stage`` pulled from the ``_probe_stage`` contextvar.

    Behaviour (production-task caller — ``_probe_stage.get() == 'unknown'``):
      - Every method delegates directly to ``self._real`` with no log
        entry. The proxy is invisible from the production task's
        perspective — same return values, same side effects, same
        latency profile.

    Note on scope kwarg:
      The live ``Cache.set`` takes ``scope=CacheScope.LOCAL`` but the
      other sync methods (``peek`` / ``delete`` / ``__contains__``) do
      not. ``Cache.get`` only takes ``as_type``. The proxy mirrors those
      exact signatures so any handler code that works against the real
      Cache works against the proxy. We derive the ``scope`` field of
      ``ProbeCacheCall`` from the value actually passed to ``set``;
      other ops leave ``scope=None`` since the underlying Cache does not
      track it per-call (the UI renders None as a blank cell).
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
                to this when ``use_cache`` is True. Production-task
                calls (callers outside any ``_stage(...)`` block) ALWAYS
                forward to this regardless of ``use_cache``.
            use_cache: UI checkbox — forward reads to the live cache
                when True; otherwise always return a miss. Only consulted
                for probe-stage callers; production-task callers go
                straight through to ``real``.
            start_time: ``time.monotonic()`` timestamp when the probe
                started. Used to compute ``ms_since_start`` on every
                logged call.
        """
        self._real = real
        self._use_cache = use_cache
        self._start_time = start_time
        self.calls: list[ProbeCacheCall] = []

    # -- internals -----------------------------------------------------------

    @staticmethod
    def _is_probe_active() -> bool:
        """Return True iff the calling asyncio task is inside a probe stage.

        asyncio tasks each carry their own copy of the contextvar context.
        The probe sets ``_probe_stage`` to a non-default value (e.g.
        ``'arrange'``, ``'task_complete:<task_id>'``) inside the
        ``with _stage(...)`` blocks that wrap every handler invocation in
        :class:`DebugRunner`. Production tasks (the poll loop, partition
        processors, ``@periodic`` background loops) never enter those
        blocks, so they see the default ``'unknown'`` and we know the
        call is NOT part of the current probe.

        When False, callers must be passed transparently to ``self._real``
        — logging or suppressing them would pollute the probe report and
        silently drop production cache writes.
        """
        return _probe_stage.get() != 'unknown'

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
        scope: str | None,
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
        # Production-task caller: pass through with no log. See class docstring.
        if not self._is_probe_active():
            return await self._real.get(key, as_type=as_type)
        if self._use_cache:
            value = await self._real.get(key, as_type=as_type)
            outcome: Literal['hit', 'miss'] = 'hit' if value is not None else 'miss'
            self._log_call(
                op='get',
                key=key,
                scope=None,
                outcome=outcome,
                value_preview=_make_value_preview(value) if value is not None else None,
            )
            return value
        # use_cache=False → always report a miss, never touch the real cache.
        self._log_call(op='get', key=key, scope=None, outcome='miss', value_preview=None)
        return None

    def peek(self, key: str) -> Any | None:
        """Sync peek — forwards to the real cache iff ``use_cache=True``, else miss.

        Mirrors ``Cache.peek``: memory-only lookup, never hits the DB.
        """
        # Production-task caller: pass through with no log. See class docstring.
        if not self._is_probe_active():
            return self._real.peek(key)
        if self._use_cache:
            value = self._real.peek(key)
            outcome: Literal['hit', 'miss'] = 'hit' if value is not None else 'miss'
            self._log_call(
                op='peek',
                key=key,
                scope=None,
                outcome=outcome,
                value_preview=_make_value_preview(value) if value is not None else None,
            )
            return value
        self._log_call(op='peek', key=key, scope=None, outcome='miss', value_preview=None)
        return None

    def __contains__(self, key: str) -> bool:
        """Membership test — forwards to real iff ``use_cache=True``, else False."""
        # Production-task caller: pass through with no log. See class docstring.
        if not self._is_probe_active():
            return key in self._real
        if self._use_cache:
            present = key in self._real
            outcome: Literal['hit', 'miss'] = 'hit' if present else 'miss'
            self._log_call(op='contains', key=key, scope=None, outcome=outcome, value_preview=None)
            return present
        self._log_call(op='contains', key=key, scope=None, outcome='miss', value_preview=None)
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
        """Suppressed write FOR THE PROBE; passes through for production callers.

        Production tasks running concurrently with a probe must not have
        their writes silently dropped — this would violate the README's
        "no cache writes — zero footprint on the live system" guarantee.
        See class docstring for the contextvar gating.
        """
        # Production-task caller: pass straight through to the real cache.
        if not self._is_probe_active():
            self._real.set(key, value, ttl=ttl, scope=scope)
            return
        self._log_call(
            op='set',
            key=key,
            scope=scope.name,
            outcome='suppressed',
            value_preview=_make_value_preview(value),
        )
        # Deliberate no-op for the probe path. ``ttl`` is accepted for
        # signature parity.
        _ = ttl

    def delete(self, key: str) -> bool:
        """Suppressed delete FOR THE PROBE; passes through for production callers.

        Probe-call semantics mirror the real ``Cache.delete`` contract:
        returns True when the key currently exists in memory, False
        otherwise. We DO NOT actually delete; we just peek at the live
        cache (only when ``use_cache=True``) to give handler code a
        truthful "was it there" signal, which some branches read. With
        ``use_cache=False`` the proxy refuses to look at the live cache
        at all and returns False, matching ``NoOpCache.delete``.

        Production-task callers pass straight through and DO mutate the
        real cache — same reasoning as ``set``.
        """
        # Production-task caller: pass straight through.
        if not self._is_probe_active():
            return self._real.delete(key)
        present = False
        if self._use_cache:
            # ``peek`` is memory-only and never touches the DB; safe to
            # call without risking side effects. Value semantics: the
            # real Cache's ``delete`` returns True iff the memory entry
            # existed, not the DB row, so peek is the right check.
            present = self._real.peek(key) is not None
        self._log_call(op='delete', key=key, scope=None, outcome='suppressed', value_preview=None)
        return present


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

    ``kafka_sink_topics`` maps Kafka sink instance names to their
    configured topic (from ``app.config.sinks.kafka[name].topic``).
    When set, the flattener uses the topic as the ``destination``
    field — the UI then renders a correct Kafka-UI deep-link for that
    topic. When the map is empty, the destination falls back to the
    sink instance name (old behaviour), so tests that don't wire a
    config mapping still work.
    """

    def __init__(self, *, kafka_sink_topics: dict[str, str] | None = None) -> None:
        self.entries: list[tuple[str, CollectResult]] = []
        self._kafka_sink_topics = kafka_sink_topics or {}

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
            # Kafka payloads — destination is the real topic when we can
            # resolve the sink instance through the runner's config
            # mapping. Falling back to the sink instance name preserves
            # behaviour for tests that don't wire a mapping. Putting the
            # real topic in ``destination`` fixes the UI Kafka-UI deep-
            # link (previously it was using the sink instance NAME as
            # the topic, producing broken links).
            for kp in cr.kafka:
                sink_name = kp.sink or ''
                topic = self._kafka_sink_topics.get(sink_name)
                destination = topic or sink_name or '(default)'
                records.append(
                    PlannedSinkRecord(
                        sink_type='kafka',
                        destination=destination,
                        origin_stage=stage,
                        payload=kp.data.model_dump(mode='json'),
                        extras={
                            'sink_instance': kp.sink,
                            'topic': topic,
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


@dataclass
class RunState:
    """Per-run state for a single probe invocation.

    One instance lives for the lifetime of ONE ``DebugRunner.run()`` call
    and is passed through every stage helper. Keeping state per-run (as
    opposed to on the runner instance) prevents cross-probe contamination
    when a second probe's wall-clock timeout fires while it is still
    waiting on the probe lock — the partial-snapshot code can only look
    at state that belongs to the probe that actually ran.

    The fields mirror the ``DebugReport`` schema one-to-one; see
    ``to_report`` for the conversion.
    """

    probe_input: ProbeInput
    sink_collector: DebugSinkCollector
    # ``cache_proxy`` is created lazily inside ``_run_locked`` so the
    # proxy captures the real ``handler.cache`` AFTER the probe lock is
    # acquired. If we captured it at ``_make_run_state`` time (before the
    # lock), a probe queued behind an in-flight probe would see the
    # predecessor's ``DebugCacheProxy`` as its ``real`` backend — a
    # second probe's reads would chain through the first probe's proxy
    # and (with ``use_cache=False`` on the outer probe) get false misses
    # for keys that actually live in the real cache. Creating the proxy
    # post-lock eliminates that chain entirely. ``None`` only persists
    # for probes whose wall-clock timeout fires BEFORE they acquire the
    # lock — in that case no cache calls were ever made, so an empty
    # call log is the correct report shape.
    cache_proxy: DebugCacheProxy | None
    use_cache: bool
    start_monotonic: float

    # Stage outputs — populated by the respective stage helpers. Kept
    # as plain attributes so ``to_report`` is a straightforward pass-
    # through rather than a dict walk.
    deserialize_error: ProbeError | None = None
    parsed_payload: Any = None
    message_label: str | None = None
    arrange: ProbeStageResult = field(default_factory=ProbeStageResult)
    tasks: list[ProbeTaskEntry] = field(default_factory=list)
    on_message_complete: ProbeStageResult | None = None
    on_window_complete: ProbeStageResult | None = None
    timing: dict[str, float] = field(default_factory=dict)
    errors: list[ProbeError] = field(default_factory=list)

    def to_report(self, *, truncated: bool) -> DebugReport:
        """Snapshot this state into a ``DebugReport`` suitable for JSON.

        ``truncated=True`` marks the report as partial — the endpoint
        sets this when the wall-clock timeout fires and cancels the run.
        Sink payloads are re-flattened from the collector on every call
        so a snapshot taken mid-run includes every CollectResult the
        handler produced up to the cancellation point.

        ``cache_proxy`` is ``None`` only when the probe's wall-clock
        timeout fired BEFORE the lock was acquired — the proxy is
        created inside ``_run_locked``, so a queued-then-cancelled probe
        never gets one. An empty call log is the correct shape in that
        case; the handler never ran.
        """
        cache_calls = list(self.cache_proxy.calls) if self.cache_proxy is not None else []
        return DebugReport(
            input=self.probe_input,
            deserialize_error=self.deserialize_error,
            parsed_payload=self.parsed_payload,
            message_label=self.message_label,
            arrange=self.arrange,
            tasks=list(self.tasks),
            on_message_complete=self.on_message_complete,
            on_window_complete=self.on_window_complete,
            planned_sink_payloads=self.sink_collector.flatten(),
            cache_calls=cache_calls,
            cache_summary=_summarize_cache_calls(cache_calls),
            timing=dict(self.timing),
            errors=list(self.errors),
            truncated=truncated,
        )


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

      The probe lock does NOT block the live worker's traffic: the poll
      loop, partition processors, and ``@periodic`` background tasks all
      keep running on the same handler instance while the probe holds
      the swapped cache. ``DebugCacheProxy`` distinguishes probe-task
      callers from production-task callers via the ``_probe_stage``
      contextvar (each asyncio task has its own context). Probe calls
      get the log-and-suppress semantics; concurrent production calls
      pass straight through to the real cache with no log entry. That's
      what keeps the probe report clean of pollution AND keeps the
      "no cache writes — zero footprint on the live system" guarantee
      true even when production traffic is in flight.

    Partial reports:
      Per-run state lives on a ``RunState`` dataclass created fresh at
      the top of ``_run_locked``. ``start_probe`` attaches that state
      onto the returned ``asyncio.Task`` — ``partial_report_for(task)``
      reads it back and returns a truncated ``DebugReport``, even if
      the task was cancelled before it acquired the probe lock. This
      task-scoped attachment is what prevents cross-probe contamination
      between an earlier cancelled probe and a subsequent one.
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

    # -- incremental partial-report machinery --------------------------------

    def start_probe(self, probe_input: ProbeInput) -> asyncio.Task[DebugReport]:
        """Create a run task whose partial state is scoped to that specific task.

        The endpoint calls this (instead of ``run`` directly) when it
        needs to distinguish "this probe's partial state" from "some
        earlier probe's partial state". The returned task carries a
        ``RunState`` object via ``partial_report_for`` — even if the
        task is cancelled before it acquires the probe lock, the
        endpoint will get back a valid empty ``DebugReport(truncated=
        True)`` rather than a stale blob from a previous probe.
        """
        state = self._make_run_state(probe_input)
        task: asyncio.Task[DebugReport] = asyncio.create_task(self._run_with_state(state))
        # Attach the state to the task. Using setattr keeps the runner
        # stateless across concurrent requests — each task's state is
        # self-contained.
        setattr(task, '_drakkar_probe_state', state)  # noqa: B010
        return task

    @staticmethod
    def partial_report_for(task: asyncio.Task[DebugReport]) -> DebugReport:
        """Build a truncated ``DebugReport`` from the state attached to ``task``.

        Companion of ``start_probe``. The state may be partially filled
        (probe was running when cancelled) or completely empty (probe
        was still queued on the lock when the endpoint timed out). In
        both cases we produce a valid DebugReport — the UI renders
        ``truncated=True`` as a warning banner and shows whatever made
        it through.
        """
        state: RunState | None = getattr(task, '_drakkar_probe_state', None)
        if state is None:  # pragma: no cover — only hit if caller uses a foreign task
            return DebugReport(input=ProbeInput(value=''), arrange=ProbeStageResult(), truncated=True)
        return state.to_report(truncated=True)

    def _make_run_state(self, probe_input: ProbeInput) -> RunState:
        """Build a fresh ``RunState`` for one probe invocation.

        Resolves Kafka sink instance names to their configured topics
        (from ``app_config.sinks.kafka[name].topic``). The sink
        collector uses this mapping so the UI's Kafka-UI deep-link
        points at the real topic instead of the sink instance name.

        The ``DebugCacheProxy`` is intentionally NOT created here — see
        the note on ``RunState.cache_proxy``. It is constructed inside
        ``_run_locked`` once the probe lock has been acquired so the
        proxy's ``real`` backend is the live cache, never a prior
        probe's proxy.
        """
        start_monotonic = time.monotonic()
        kafka_sink_topics = {name: cfg.topic for name, cfg in self._app_config.sinks.kafka.items()}
        return RunState(
            probe_input=probe_input,
            sink_collector=DebugSinkCollector(kafka_sink_topics=kafka_sink_topics),
            cache_proxy=None,
            use_cache=probe_input.use_cache,
            start_monotonic=start_monotonic,
        )

    # -- top-level entrypoint ------------------------------------------------

    async def run(self, probe_input: ProbeInput) -> DebugReport:
        """Execute the full probe and return a ``DebugReport(truncated=False)``.

        Runs under ``self._probe_lock`` so overlapping probes serialize.
        Swaps ``handler.cache`` with ``DebugCacheProxy`` inside a
        ``try/finally`` that unconditionally restores the original cache
        — even if the handler raises or the task is cancelled by the
        endpoint's wall-clock timeout.

        Unit tests typically call this directly for its convenience —
        the endpoint uses ``start_probe`` + ``wait_for`` +
        ``partial_report_for`` instead so it can return a correctly-
        scoped partial on timeout.
        """
        state = self._make_run_state(probe_input)
        return await self._run_with_state(state)

    async def _run_with_state(self, state: RunState) -> DebugReport:
        """Acquire the probe lock and execute the full run against ``state``.

        Shared body of ``run`` and ``start_probe``. Keeps the state
        object as a plain parameter so no mutable attribute lives on
        the runner for the endpoint path.
        """
        async with self._probe_lock:
            return await self._run_locked(state)

    async def _run_locked(self, state: RunState) -> DebugReport:
        """Body of run() executed under the probe lock.

        Swaps ``handler.cache`` with the run's ``DebugCacheProxy`` for
        the duration of the probe, restores it in a ``finally`` even
        if the handler raised or the task was cancelled by the
        endpoint's wall-clock timeout.

        The ``DebugCacheProxy`` is constructed HERE — after acquiring
        the lock — rather than in ``_make_run_state``. That way its
        ``real`` backend is the handler's live cache, never a prior
        probe's proxy. Constructing the proxy pre-lock would make
        concurrent probes chain through each other (probe B's reads
        forwarding through probe A's proxy), breaking the documented
        ``use_cache=True`` contract for the second probe.

        Concurrent production traffic safety:
          The cache swap below is process-wide — the live worker's
          asyncio tasks hit the same proxy. ``DebugCacheProxy`` checks
          the ``_probe_stage`` contextvar on every entry point and
          delegates production-task calls straight through to the real
          cache. The probe report therefore contains only the probe's
          own pipeline calls, and production cache writes survive. See
          ``DebugCacheProxy`` class docstring for the full contract.
        """
        msg = _build_source_message(state.probe_input)
        original_cache = self._handler.cache
        # Build the proxy now that we know the true live cache. This
        # also means any earlier-queued probe whose cache_proxy slot is
        # still ``None`` gets its proxy wired to the REAL cache (not to
        # whatever proxy was in flight while it was waiting).
        state.cache_proxy = DebugCacheProxy(
            real=original_cache,
            use_cache=state.use_cache,
            start_time=state.start_monotonic,
        )
        # Use setattr to dodge the static type check — DebugCacheProxy
        # duck-types the Cache surface (verified by tests) but isn't in
        # the Cache | NoOpCache union ty sees on the handler attribute.
        # Keeping the swap out of the type system is intentional: the
        # proxy should never leak beyond the probe.
        setattr(self._handler, 'cache', state.cache_proxy)  # noqa: B010
        try:
            await self._run_stages(state=state, msg=msg)
        finally:
            # Restore cache even if a hook raised or the task was
            # cancelled (wall-clock timeout path).
            setattr(self._handler, 'cache', original_cache)  # noqa: B010

        state.timing['total_wallclock'] = time.monotonic() - state.start_monotonic
        return state.to_report(truncated=False)

    # -- stage sequencing ----------------------------------------------------

    async def _run_stages(self, *, state: RunState, msg: SourceMessage) -> None:
        """Run the arrange → per-task → window sequence with graceful error capture.

        Extracted into its own coroutine so the ``finally`` block in
        ``_run_locked`` only has to worry about cache restoration —
        stage logic lives here. Task 3 added try/except wrappers around
        every hook so exceptions never crash the probe — they land in
        ``state.errors`` as ``ProbeError`` entries and the runner
        short-circuits downstream stages based on which hook failed
        (see ``_record_error`` and the per-stage logic below).

        Short-circuit rules:
          - deserialize error → skip all downstream stages
          - message_label error → non-fatal, keep going
          - arrange error → skip tasks + hooks
          - on_task_complete error → record on the task entry, keep processing other tasks
          - on_message_complete error → still run on_window_complete
          - on_window_complete error → capture and return
        """
        # --- deserialize -----------------------------------------------------
        # Deserialize is the first stage. If it fails, we cannot usefully
        # build tasks or call hooks that depend on a parsed payload.
        deserialize_ok = self._run_deserialize(state=state, msg=msg)
        if not deserialize_ok:
            return

        # --- message_label ---------------------------------------------------
        # Label is a cosmetic string used for UI rows / logs. Failing here
        # must NOT skip downstream stages; we just leave message_label as
        # None and move on.
        self._run_message_label(state=state, msg=msg)

        # --- arrange ---------------------------------------------------------
        arrange_start = time.monotonic()
        tasks = await self._run_arrange(state=state, msg=msg, arrange_start=arrange_start)
        if tasks is None:
            # Arrange failed — skip tasks + both hook stages. The error is
            # already captured on state.errors and state.arrange.
            return

        # --- per-task execution ---------------------------------------------
        # ``all_scheduled_tasks`` starts with the arrange output and grows
        # as on_error replacements are added (mirrors production's
        # ``window.tasks`` / ``tracker.tasks`` behaviour). Passed to
        # on_message_complete below so the handler sees the full lineage.
        all_scheduled_tasks: list[ExecutorTask] = list(tasks)
        # Production separates three lists with distinct semantics — the
        # probe mirrors all three:
        #
        # 1. ``successful_terminal_results`` (mirrors production's
        #    ``tracker.results``): ONE ExecutorResult per successful task.
        #    Fed into ``MessageGroup.results`` — so handlers that read
        #    ``group.results`` see the same "success-only" shape as
        #    production. See drakkar/partition.py:525-526.
        # 2. ``terminal_errors`` (mirrors production's ``tracker.errors``):
        #    ONE ExecutorError per terminally-failed task (SKIP /
        #    retries-exhausted / on_task_complete raised). Fed into
        #    ``MessageGroup.errors``. Replaced tasks do NOT contribute
        #    — only their successors. See drakkar/partition.py:527-528.
        # 3. ``all_terminal_results`` (mirrors production's
        #    ``window.results``): ONE ExecutorResult per terminal task
        #    outcome — successes AND failures (failures carry either the
        #    ExecutorError's ``.result`` or a synthesized failure
        #    ExecutorResult). Fed into ``on_window_complete(results, ...)``.
        #    See drakkar/partition.py:406, 473, 479.
        successful_terminal_results: list[ExecutorResult] = []
        terminal_errors: list[ExecutorError] = []
        all_terminal_results: list[ExecutorResult] = []

        for task in tasks:
            # _process_task is the on_error-aware orchestrator: it handles
            # RETRY (up to max_retries), replacement lists, SKIP, and
            # on_error itself raising — appending one ProbeTaskEntry per
            # execute attempt (including retries and replacements).
            await self._process_task(
                state=state,
                task=task,
                msg=msg,
                successful_terminal_results=successful_terminal_results,
                terminal_errors=terminal_errors,
                all_terminal_results=all_terminal_results,
                all_scheduled_tasks=all_scheduled_tasks,
            )

        # --- on_message_complete --------------------------------------------
        # on_message_complete is non-blocking for on_window_complete: even
        # if it raises, we still invoke on_window_complete so the operator
        # can see its independent behaviour.
        await self._run_on_message_complete(
            state=state,
            msg=msg,
            all_scheduled_tasks=all_scheduled_tasks,
            successful_terminal_results=successful_terminal_results,
            terminal_errors=terminal_errors,
            arrange_start=arrange_start,
        )

        # --- on_window_complete ---------------------------------------------
        await self._run_on_window_complete(
            state=state,
            msg=msg,
            all_terminal_results=all_terminal_results,
        )

    # -- per-stage helpers ---------------------------------------------------
    #
    # Each helper runs one hook stage with error capture. They return a
    # value (or a flag) so the caller can decide whether to short-circuit
    # the pipeline. Keeping each stage in its own method keeps _run_stages
    # readable and mirrors the stage-by-stage rules documented in the plan.

    def _run_deserialize(self, *, state: RunState, msg: SourceMessage) -> bool:
        """Run handler.deserialize_message with error capture. Returns True on success.

        Returning False signals the caller to stop — the parsed payload
        is missing and every downstream stage would be meaningless.
        """
        with _stage('deserialize'):
            try:
                self._handler.deserialize_message(msg)
            except Exception as exc:
                probe_error = self._build_probe_error(
                    state=state,
                    stage='deserialize',
                    exc=exc,
                )
                self._record_error(state=state, error=probe_error)
                # Also surface as a dedicated field — the UI renders
                # deserialize errors in section A with special emphasis.
                state.deserialize_error = probe_error
                return False
        # deserialize_message mutates msg.payload in place. Serialize
        # pydantic models via model_dump so the JSON roundtrip used by
        # the endpoint preserves structure.
        state.parsed_payload = _serialize_payload(msg.payload)
        return True

    def _run_message_label(self, *, state: RunState, msg: SourceMessage) -> None:
        """Run handler.message_label with error capture. Non-fatal on error."""
        with _stage('message_label'):
            try:
                label = self._handler.message_label(msg)
            except Exception as exc:
                self._record_error(
                    state=state,
                    error=self._build_probe_error(state=state, stage='message_label', exc=exc),
                )
                # Leave message_label as None; downstream stages carry on.
                return
        state.message_label = label

    async def _run_arrange(
        self,
        *,
        state: RunState,
        msg: SourceMessage,
        arrange_start: float,
    ) -> list[ExecutorTask] | None:
        """Run handler.arrange with error capture. Returns None to signal fatal failure.

        Populates ``state.arrange`` with a ``ProbeStageResult`` in both
        success and failure cases — on failure the ``error`` field
        carries a one-line summary and the full traceback is on
        ``state.errors``.
        """
        with _stage('arrange'):
            try:
                tasks = await self._handler.arrange([msg], PendingContext())
            except Exception as exc:
                arrange_duration = time.monotonic() - arrange_start
                probe_error = self._build_probe_error(state=state, stage='arrange', exc=exc)
                self._record_error(state=state, error=probe_error)
                state.arrange = ProbeStageResult(
                    duration_seconds=arrange_duration,
                    error=_one_line_summary(exc),
                )
                state.timing['arrange'] = arrange_duration
                return None

        arrange_duration = time.monotonic() - arrange_start
        state.arrange = ProbeStageResult(duration_seconds=arrange_duration)
        state.timing['arrange'] = arrange_duration
        return tasks

    async def _run_on_message_complete(
        self,
        *,
        state: RunState,
        msg: SourceMessage,
        all_scheduled_tasks: list[ExecutorTask],
        successful_terminal_results: list[ExecutorResult],
        terminal_errors: list[ExecutorError],
        arrange_start: float,
    ) -> None:
        """Run handler.on_message_complete with error capture.

        Even on failure, the runner continues to on_window_complete —
        the two hooks are independent (per plan rules), so a broken
        on_message_complete should not mask on_window_complete's
        behaviour from the operator.

        ``all_scheduled_tasks`` includes arrange output AND any
        on_error replacements — production's ``MessageGroup.tasks`` has
        the same shape. ``successful_terminal_results`` carries ONLY
        successful ExecutorResults (mirrors production's
        ``tracker.results``). ``terminal_errors`` carries terminally-
        failed tasks' ExecutorErrors (mirrors production's
        ``tracker.errors``).
        """
        mc_start = time.monotonic()
        with _stage('message_complete'):
            try:
                group = MessageGroup(
                    source_message=msg,
                    tasks=list(all_scheduled_tasks),
                    results=list(successful_terminal_results),
                    errors=list(terminal_errors),
                    started_at=arrange_start,
                    finished_at=time.monotonic(),
                )
                mc_result = await self._handler.on_message_complete(group)
            except Exception as exc:
                mc_duration = time.monotonic() - mc_start
                self._record_error(
                    state=state,
                    error=self._build_probe_error(state=state, stage='on_message_complete', exc=exc),
                )
                # Keep collect_result=None so the UI can distinguish "hook
                # raised" from "hook returned None by design".
                state.on_message_complete = ProbeStageResult(
                    duration_seconds=mc_duration,
                    collect_result=None,
                    error=_one_line_summary(exc),
                )
                state.timing['on_message_complete'] = mc_duration
                return

        mc_duration = time.monotonic() - mc_start
        # If the hook returned a CollectResult, route it through the sink
        # collector — same behaviour as PartitionProcessor's _on_collect
        # callback, minus the real SinkManager write.
        if mc_result is not None:
            with _stage('message_complete'):
                await state.sink_collector(mc_result, msg.partition)
        state.on_message_complete = ProbeStageResult(duration_seconds=mc_duration, collect_result=mc_result)
        state.timing['on_message_complete'] = mc_duration

    async def _run_on_window_complete(
        self,
        *,
        state: RunState,
        msg: SourceMessage,
        all_terminal_results: list[ExecutorResult],
    ) -> None:
        """Run handler.on_window_complete with error capture. Always the last stage.

        ``all_terminal_results`` mirrors production's ``window.results``
        — one ExecutorResult per terminal task outcome, successes AND
        failures alike (failures carry either the ExecutorTaskError's
        ``.result`` or a synthesized ``exit_code=-1`` failure). See
        drakkar/partition.py:406, 473, 479.
        """
        wc_start = time.monotonic()
        with _stage('window_complete'):
            try:
                wc_result = await self._handler.on_window_complete(
                    list(all_terminal_results),
                    [msg],
                )
            except Exception as exc:
                wc_duration = time.monotonic() - wc_start
                self._record_error(
                    state=state,
                    error=self._build_probe_error(state=state, stage='on_window_complete', exc=exc),
                )
                state.on_window_complete = ProbeStageResult(
                    duration_seconds=wc_duration,
                    collect_result=None,
                    error=_one_line_summary(exc),
                )
                state.timing['on_window_complete'] = wc_duration
                return

        wc_duration = time.monotonic() - wc_start
        if wc_result is not None:
            with _stage('window_complete'):
                await state.sink_collector(wc_result, msg.partition)
        state.on_window_complete = ProbeStageResult(duration_seconds=wc_duration, collect_result=wc_result)
        state.timing['on_window_complete'] = wc_duration

    async def _process_task(
        self,
        *,
        state: RunState,
        task: ExecutorTask,
        msg: SourceMessage,
        successful_terminal_results: list[ExecutorResult],
        terminal_errors: list[ExecutorError],
        all_terminal_results: list[ExecutorResult],
        all_scheduled_tasks: list[ExecutorTask],
        retry_of: str | None = None,
        replacement_for: str | None = None,
    ) -> None:
        """Run ONE task end-to-end with the full on_error / retry / replace cycle.

        Appends one ``ProbeTaskEntry`` per executor attempt to
        ``state.tasks`` (so retries and replacements also land in the
        Tasks section of the UI).

        Production keeps three terminal-outcome lists with distinct
        semantics — the probe mirrors all three:
          - ``successful_terminal_results`` (production: ``tracker.results``)
            — success-only, feeds ``MessageGroup.results``.
          - ``terminal_errors`` (production: ``tracker.errors``) — one
            per terminally-failed task, feeds ``MessageGroup.errors``.
          - ``all_terminal_results`` (production: ``window.results``) —
            both successes AND failures, feeds ``on_window_complete``.

        Flow:
          1. Execute the task via ``_execute_and_record_task``.
          2. On success → append the entry, on_task_complete already
             fed into the sink collector. No on_error invoked.
          3. On failure (SKIP or retries-exhausted):
             a. Call ``handler.on_error(task, error)``. If it raises,
                mark the entry ``status='failed'`` and append to
                ``terminal_errors`` AND ``all_terminal_results``.
             b. If the action is RETRY → ``_run_retry_loop``.
             c. If the action is a ``list[ExecutorTask]`` → mark original
                ``status='replaced'``, recurse for each replacement. Also
                apply production's parent_task_id auto-link: if the
                replacement did not set one, point it back at the
                original (see PartitionProcessor:437).
             d. If the action is SKIP or anything else → failed entry
                stays ``status='failed'`` and the ExecutorError goes to
                ``terminal_errors`` + the ExecutorResult goes to
                ``all_terminal_results`` (mirrors production's
                ``window.results.append(e.result)`` + ``tracker.errors.
                append(task_error)`` pair).

        Retries and replacement cascades each run their OWN on_error
        cycle. A broken handler that infinitely returns replacements
        would hang the probe the same way it hangs the real worker.
        """
        entry, exec_error = await self._execute_and_record_task(
            state=state,
            task=task,
            msg=msg,
            successful_terminal_results=successful_terminal_results,
            terminal_errors=terminal_errors,
            all_terminal_results=all_terminal_results,
            retry_of=retry_of,
            replacement_for=replacement_for,
        )
        state.tasks.append(entry)

        # Success path → nothing more to do for this task.
        if exec_error is None:
            return

        # -- on_error path ----------------------------------------------------
        await self._handle_task_failure(
            state=state,
            task=task,
            failed_entry=entry,
            exec_error=exec_error,
            msg=msg,
            successful_terminal_results=successful_terminal_results,
            terminal_errors=terminal_errors,
            all_terminal_results=all_terminal_results,
            all_scheduled_tasks=all_scheduled_tasks,
        )

    async def _handle_task_failure(
        self,
        *,
        state: RunState,
        task: ExecutorTask,
        failed_entry: ProbeTaskEntry,
        exec_error: ExecutorTaskError,
        msg: SourceMessage,
        successful_terminal_results: list[ExecutorResult],
        terminal_errors: list[ExecutorError],
        all_terminal_results: list[ExecutorResult],
        all_scheduled_tasks: list[ExecutorTask],
    ) -> None:
        """Run on_error for ``failed_entry`` and react to its returned action.

        Branches out into three production-mirroring paths (RETRY /
        replacement list / SKIP-or-other) plus the on_error-itself-raises
        path. See ``_process_task`` docstring for the full state machine.

        Terminal failures append to ``all_terminal_results`` (production's
        ``window.results``) AND ``terminal_errors`` (production's
        ``tracker.errors``). The success-only list ``successful_terminal_
        results`` is NOT touched here — only ``_execute_and_record_task``
        touches it on the happy path.
        """
        action = await self._invoke_on_error(state=state, task=task, exec_error=exec_error)
        if action is None:
            # on_error itself raised — ProbeError already recorded.
            # Terminal failure: feed the ExecutorResult into the
            # window-level list (mirrors ``window.results``) AND the
            # ExecutorError into the tracker-level error list (mirrors
            # ``tracker.errors``). Does NOT touch the success-only list.
            all_terminal_results.append(exec_error.result)
            terminal_errors.append(exec_error.error)
            return

        # Replacement list: original → 'replaced', recurse for each new task.
        # on_error's contract (see BaseDrakkarHandler.on_error) guarantees
        # list members are ExecutorTask instances.
        if isinstance(action, list):
            failed_entry.status = 'replaced'
            for replacement in action:
                # Auto-link replacement to the parent, matching production
                # (see drakkar/partition.py:437). User handlers rarely set
                # this explicitly; keeping the auto-link preserves the
                # replacement lineage in the probe report.
                if replacement.parent_task_id is None:
                    replacement.parent_task_id = task.task_id
                all_scheduled_tasks.append(replacement)
                await self._process_task(
                    state=state,
                    task=replacement,
                    msg=msg,
                    successful_terminal_results=successful_terminal_results,
                    terminal_errors=terminal_errors,
                    all_terminal_results=all_terminal_results,
                    all_scheduled_tasks=all_scheduled_tasks,
                    replacement_for=task.task_id,
                )
            return

        # RETRY path: re-execute up to max_retries more attempts, each as a
        # fresh ProbeTaskEntry with retry_of=<original task_id>. The loop
        # stops on the first success OR when max_retries is reached OR when
        # on_error stops returning RETRY.
        if action == ErrorAction.RETRY:
            await self._run_retry_loop(
                state=state,
                task=task,
                msg=msg,
                successful_terminal_results=successful_terminal_results,
                terminal_errors=terminal_errors,
                all_terminal_results=all_terminal_results,
                all_scheduled_tasks=all_scheduled_tasks,
                first_exec_error=exec_error,
            )
            return

        # SKIP or any unrecognized action → leave the failed entry as-is.
        # This matches production's "else: window.results.append(e.result)"
        # branch in PartitionProcessor._execute_and_track. Also append to
        # terminal_errors so MessageGroup.errors mirrors production.
        all_terminal_results.append(exec_error.result)
        terminal_errors.append(exec_error.error)

    async def _run_retry_loop(
        self,
        *,
        state: RunState,
        task: ExecutorTask,
        msg: SourceMessage,
        successful_terminal_results: list[ExecutorResult],
        terminal_errors: list[ExecutorError],
        all_terminal_results: list[ExecutorResult],
        all_scheduled_tasks: list[ExecutorTask],
        first_exec_error: ExecutorTaskError,
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
            else, the loop exits and that branch is handled inline so
            the retry entry gets the correct terminal status.

        ``first_exec_error`` carries the originally-failed attempt's
        error so that if we end up exhausting retries (budget gone but
        handler still wants RETRY), we can append the final failure to
        the terminal lists.

        Success-only ``successful_terminal_results`` is touched by
        ``_execute_and_record_task`` on the happy path; terminal-failure
        branches here append to ``all_terminal_results`` (production's
        ``window.results``) + ``terminal_errors`` (production's
        ``tracker.errors``) to keep the three lists in the production
        shape.
        """
        max_retries = self._app_config.executor.max_retries
        retry_count = 0
        last_exec_error: ExecutorTaskError | None = first_exec_error
        while retry_count < max_retries:
            retry_count += 1
            retry_entry, exec_error = await self._execute_and_record_task(
                state=state,
                task=task,
                msg=msg,
                successful_terminal_results=successful_terminal_results,
                terminal_errors=terminal_errors,
                all_terminal_results=all_terminal_results,
                retry_of=task.task_id,
                replacement_for=None,
            )
            state.tasks.append(retry_entry)
            if exec_error is None:
                # Retry succeeded — done. on_task_complete already fed
                # its CollectResult into the sink collector via
                # ``_execute_and_record_task``.
                return
            last_exec_error = exec_error
            # Retry failed. Ask the handler what to do next.
            action = await self._invoke_on_error(state=state, task=task, exec_error=exec_error)
            if action is None:
                # on_error itself raised — ProbeError already recorded.
                all_terminal_results.append(exec_error.result)
                terminal_errors.append(exec_error.error)
                return
            if isinstance(action, list):
                # Replacement path off a retry — retry_entry becomes
                # 'replaced' and we recurse on the replacements. Auto-
                # link parent_task_id as in ``_handle_task_failure``.
                retry_entry.status = 'replaced'
                for replacement in action:
                    if replacement.parent_task_id is None:
                        replacement.parent_task_id = task.task_id
                    all_scheduled_tasks.append(replacement)
                    await self._process_task(
                        state=state,
                        task=replacement,
                        msg=msg,
                        successful_terminal_results=successful_terminal_results,
                        terminal_errors=terminal_errors,
                        all_terminal_results=all_terminal_results,
                        all_scheduled_tasks=all_scheduled_tasks,
                        replacement_for=task.task_id,
                    )
                return
            if action != ErrorAction.RETRY:
                # SKIP or unknown → stop, retry_entry stays 'failed'.
                all_terminal_results.append(exec_error.result)
                terminal_errors.append(exec_error.error)
                return
            # RETRY again and budget allows → next loop iteration.
        # Loop exited because retry_count hit max_retries — retries
        # exhausted. Append the last attempt's failure (mirrors
        # production's "max_retries_exceeded" branch).
        if last_exec_error is not None:
            all_terminal_results.append(last_exec_error.result)
            terminal_errors.append(last_exec_error.error)

    async def _invoke_on_error(
        self,
        *,
        state: RunState,
        task: ExecutorTask,
        exec_error: ExecutorTaskError,
    ) -> str | list[ExecutorTask] | None:
        """Call ``handler.on_error`` with error capture.

        Returns ``None`` when on_error itself raises (the ProbeError has
        already been appended). Otherwise returns the action verbatim so
        the caller can pattern-match on RETRY / list / SKIP / other.

        The return type uses ``str`` (not ``ErrorAction``) because
        ``ErrorAction`` is a ``StrEnum`` — user handlers are free to
        return the raw string ``'retry'`` / ``'skip'`` instead of the
        enum member. The caller uses ``action == ErrorAction.RETRY`` to
        treat both cases identically (StrEnum equality matches the
        underlying str).
        """
        stage = f'on_error:{task.task_id}'
        with _stage(stage):
            try:
                return await self._handler.on_error(task, exec_error.error)
            except Exception as exc:
                self._record_error(
                    state=state,
                    error=self._build_probe_error(state=state, stage=stage, exc=exc),
                )
                return None

    async def _execute_and_record_task(
        self,
        *,
        state: RunState,
        task: ExecutorTask,
        msg: SourceMessage,
        successful_terminal_results: list[ExecutorResult],
        terminal_errors: list[ExecutorError],
        all_terminal_results: list[ExecutorResult],
        retry_of: str | None,
        replacement_for: str | None,
    ) -> tuple[ProbeTaskEntry, ExecutorTaskError | None]:
        """Execute one attempt and return its ``ProbeTaskEntry`` plus optional exec error.

        Returns a tuple ``(entry, exec_error)``. ``exec_error`` is the
        caught ``ExecutorTaskError`` on subprocess-level failure, or
        ``None`` on success. Using a tuple return (rather than a scratch
        slot on ``RunState``) keeps the failure signal co-located with
        the entry and removes the need for callers to read-then-clear a
        shared attribute.

        Production keeps three terminal-outcome lists (see
        ``_process_task`` docstring) with distinct semantics. This helper
        touches them as follows:

          - ``successful_terminal_results`` (production: ``tracker.results``)
            — appended ONLY when the executor task succeeded AND
            ``on_task_complete`` did not raise. Mirrors
            ``partition.py:525-526`` (tracker.results.append(task_result)
            runs only on the success branch where ``task_result`` is set
            at partition.py:407).
          - ``all_terminal_results`` (production: ``window.results``) —
            appended for BOTH success (real result) AND the
            ``on_task_complete`` raising path (synthesized
            ``exit_code=-1`` result). Mirrors ``partition.py:406`` +
            ``partition.py:479-487``.
          - ``terminal_errors`` (production: ``tracker.errors``) —
            appended ONLY when ``on_task_complete`` raised, carrying
            the synthesized ExecutorError. Mirrors the unexpected-
            exception branch at ``partition.py:488-495`` + ``partition.
            py:527-528``.

        On executor failure (``ExecutorTaskError``): returns a
        ``status='failed'`` entry built from the ExecutorError attached
        to the exception, paired with the caught error. The caller
        (``_process_task`` / ``_run_retry_loop``) handles on_error and
        is responsible for appending to the right terminal list(s) based
        on the on_error action.
        """
        # -- executor execute ------------------------------------------------
        with _stage(f'executor:{task.task_id}'):
            try:
                exec_result = await self._executor_pool.execute(
                    task,
                    recorder=None,
                    partition_id=msg.partition,
                )
            except ExecutorTaskError as exc:
                # Subprocess-level failure — build a 'failed' entry from
                # the ExecutorError payload attached to the exception.
                # on_error / retry / replace logic happens in the caller.
                failed_entry = _failed_task_entry(
                    task=task,
                    error=exc,
                    retry_of=retry_of,
                    replacement_for=replacement_for,
                )
                return failed_entry, exc

        # -- on_task_complete for this task --------------------------------
        tc_start = time.monotonic()
        tc_result: CollectResult | None = None
        tc_error_summary: str | None = None
        tc_exception: Exception | None = None
        with _stage(f'task_complete:{task.task_id}'):
            try:
                tc_result = await self._handler.on_task_complete(exec_result)
            except Exception as exc:
                tc_error_summary = _one_line_summary(exc)
                tc_exception = exc
                self._record_error(
                    state=state,
                    error=self._build_probe_error(
                        state=state,
                        stage=f'task_complete:{task.task_id}',
                        exc=exc,
                    ),
                )
        tc_duration = time.monotonic() - tc_start

        # Feed the terminal lists AFTER on_task_complete ran, mirroring
        # production ordering.
        if tc_exception is None:
            # Full success path — production: partition.py:406-407.
            # Both the window-level list AND the tracker-level success
            # list get the real result.
            successful_terminal_results.append(exec_result)
            all_terminal_results.append(exec_result)
        else:
            # on_task_complete raised — production's catch-all at
            # partition.py:476-495 synthesizes a failure ExecutorResult
            # for window.results + an ExecutorError for tracker.errors.
            # The success-only list is NOT touched here (in production,
            # ``task_result`` stays None on this path, so tracker.results
            # never sees an append at partition.py:525-526).
            synthesized_result = ExecutorResult(
                exit_code=-1,
                stdout='',
                stderr=str(tc_exception),
                duration_seconds=0,
                task=task,
            )
            synthesized_error = ExecutorError(
                task=task,
                exception=str(tc_exception),
                stderr=str(tc_exception),
            )
            all_terminal_results.append(synthesized_result)
            terminal_errors.append(synthesized_error)

        # Only feed the sink collector on success — a raised
        # on_task_complete produced no result to forward.
        if tc_result is not None:
            with _stage(f'task_complete:{task.task_id}'):
                await state.sink_collector(tc_result, msg.partition)

        entry = ProbeTaskEntry(
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
        return entry, None

    # -- error-capture helpers ----------------------------------------------

    def _build_probe_error(
        self,
        *,
        state: RunState,
        stage: str,
        exc: BaseException,
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
            occurred_at_ms=(time.monotonic() - state.start_monotonic) * 1000.0,
        )

    def _record_error(self, *, state: RunState, error: ProbeError) -> None:
        """Append a ``ProbeError`` to the per-run errors list.

        Kept as a single choke-point so future code (metrics, structured
        logging of captured errors) has one place to patch.
        """
        state.errors.append(error)
