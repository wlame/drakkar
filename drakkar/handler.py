"""User hook protocol and base handler for Drakkar framework.

Users extend BaseDrakkarHandler and override hooks to define their
pipeline logic: arrange() creates executor tasks, on_task_complete()
processes each result into sink payloads, and on_delivery_error()
handles sink failures.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, TypeVar, get_args

import structlog
from pydantic import BaseModel, ValidationError

# Imported at runtime (not under ``TYPE_CHECKING``) so the ``cache: CacheLike``
# annotations on the classes below are resolvable by tools that evaluate
# annotations eagerly. ``protocol`` is side-effect-free and dependency-light;
# importing it here does not pull in ``drakkar.cache.engine`` or any other
# heavy module, so there is no circular-import risk.
from drakkar.annotations import AnnotatorLike
from drakkar.cache.protocol import CacheLike
from drakkar.offload import OffloaderLike
from drakkar.timeline_events import NoOpTimelineEventEmitter, TimelineEventEmitter, TimelineMatch

if TYPE_CHECKING:
    from datetime import datetime

    from drakkar.config import DrakkarConfig
    from drakkar.uipages import Page

from drakkar.metrics import message_parse_failures
from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    InputT,
    MessageGroup,
    OutputT,
    PendingContext,
    SourceMessage,
)
from drakkar.utils import make_request_id

logger = structlog.get_logger()

# PEP 696 default TypeVars (Python 3.13+). These let the handler stay 2-param
# for non-webapp users (``BaseDrakkarHandler[InputT, OutputT]``) while extending
# to 4 params for webapp users (``BaseDrakkarHandler[InputT, OutputT, HttpReqT,
# HttpRespT]``). When a subclass omits the HTTP slots, ``get_args`` materialises
# them to ``None`` (verified empirically with CPython 3.13's PEP 696 impl), so
# ``_extract_type_args`` can simply read off the four positions and return
# ``None`` for unspecified slots.
#
# No ``bound=`` constraint: webapp users opt in by declaring concrete Pydantic
# models in those slots; the framework only requires non-None at startup when
# ``webapp.enabled=True``. Users who never enable the webapp keep the
# defaults and never see the HTTP hooks.
HttpRequestT = TypeVar('HttpRequestT', default=None)
HttpResponseT = TypeVar('HttpResponseT', default=None)


class DrakkarHandler(Protocol[InputT, OutputT, HttpRequestT, HttpResponseT]):
    """Protocol defining the hooks a user must implement.

    Parameterised over four type variables — ``InputT`` / ``OutputT`` for the
    Kafka path and ``HttpRequestT`` / ``HttpResponseT`` for the optional webapp
    path. The HTTP slots default to ``None`` via PEP 696 so 2-param users don't
    need to know the HTTP slots exist (see :class:`BaseDrakkarHandler`).
    """

    input_model: type[BaseModel] | None
    output_model: type[BaseModel] | None
    # One details model per handler; registering it is the opt-in for the
    # probe's User-defined tab.
    probe_details_model: type[BaseModel] | None
    # One application-config model per handler; registering it is the
    # opt-in for the framework-loaded ``self.app_config`` instance
    # (drakkar.yaml ``app:`` section + the handler's own env prefix).
    app_config_model: ClassVar[type[BaseModel] | None]
    app_env_prefix: ClassVar[str]
    # Declared UI pages: a deployment's custom dashboard pages, each a
    # list of widgets reading from a built-in source. Registering these
    # is the opt-in for the extra nav entries; validated at startup via
    # drakkar.uipages.build_pages.
    ui_pages: list[Page] | None
    # Handler-facing cache. Always non-None by the time user hooks are called:
    # either a real Cache (when cache.enabled=true) or a NoOpCache stub
    # (disabled path). Typed structurally as :class:`CacheLike` so test doubles
    # and alternate backends satisfy the contract without subclassing the
    # concrete classes — see :mod:`drakkar.cache.protocol`.
    cache: CacheLike

    def message_label(self, msg: SourceMessage) -> str: ...
    def task_priority(self, task: ExecutorTask) -> Any: ...
    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig: ...
    async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None: ...
    async def arrange(self, messages: list[SourceMessage], pending: PendingContext) -> list[ExecutorTask]: ...
    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None: ...
    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None: ...
    async def on_window_complete(
        self, results: list[ExecutorResult], source_messages: list[SourceMessage]
    ) -> CollectResult | None: ...
    async def on_error(self, task: ExecutorTask, error: ExecutorError) -> ErrorAction | list[ExecutorTask]: ...
    async def on_delivery_error(self, error: DeliveryError) -> DeliveryAction: ...
    async def on_assign(self, partitions: list[int]) -> None: ...
    async def on_revoke(self, partitions: list[int]) -> None: ...

    # Webapp hooks. Only invoked when ``webapp.enabled=True`` AND the handler
    # subclass declares concrete types in the HttpRequestT/HttpResponseT slots.
    async def arrange_http_request(self, req: HttpRequestT, pending: PendingContext) -> list[ExecutorTask]: ...
    async def on_http_request_complete(self, group: MessageGroup) -> HttpResponseT: ...
    def http_request_id(self, req: HttpRequestT, headers: Mapping[str, str]) -> str: ...
    def http_request_label(self, req: HttpRequestT, request_id: str) -> str: ...


def _extract_type_args(
    cls: type,
) -> tuple[type | None, type | None, type | None, type | None]:
    """Extract the four generic slots from ``BaseDrakkarHandler[...]`` bases.

    Returns ``(InputT, OutputT, HttpRequestT, HttpResponseT)`` — ``None`` for
    any slot that the subclass left at the default. PEP 696 materialises
    omitted defaults to ``None`` in ``get_args(orig_base)``, so we just read
    off the positions and substitute ``None`` whenever the runtime value is
    not a concrete ``type``. (The 2-param historical form is preserved by
    accepting ``len(args) == 2`` and returning ``None`` for the two HTTP
    slots — covers older subclasses that predate the webapp release.)
    """
    for base in getattr(cls, '__orig_bases__', ()):
        args = get_args(base)
        if len(args) == 4:
            input_t, output_t, http_req_t, http_resp_t = args
            return (
                input_t if isinstance(input_t, type) else None,
                output_t if isinstance(output_t, type) else None,
                http_req_t if isinstance(http_req_t, type) else None,
                http_resp_t if isinstance(http_resp_t, type) else None,
            )
        if len(args) == 2:
            input_t, output_t = args
            return (
                input_t if isinstance(input_t, type) else None,
                output_t if isinstance(output_t, type) else None,
                None,
                None,
            )
    return None, None, None, None


class BaseDrakkarHandler(Generic[InputT, OutputT, HttpRequestT, HttpResponseT]):
    """Base handler with no-op defaults for optional hooks.

    Users extend this class and must override ``arrange()``.
    All other hooks have sensible defaults.

    Hooks (all three output hooks are independent; use any combination):
        arrange(messages, pending) -> list[ExecutorTask]
            Required. Groups source messages into subprocess tasks.

        on_task_complete(result) -> CollectResult | None
            Called per successful task. Return a CollectResult with
            sink payloads for THIS task's result::

                return CollectResult(
                    kafka=[KafkaPayload(data=my_output, key=b"abc")],
                    postgres=[PostgresPayload(table="results", data=my_output)],
                )

            Use for 1-in → N-out fanout (one result → multiple sink
            messages with full detail).

        on_message_complete(group) -> CollectResult | None
            Called once per source message, after ALL tasks derived from
            that message have reached a terminal state. Receives a
            ``MessageGroup`` summarising the whole fan-out for that
            message. Use for N-in → 1-out aggregation (many tasks →
            single summary row).

            Offsets are committed *after* this hook fires — any sink
            emissions here are guaranteed delivered-or-failed before
            Kafka advances the consumer offset for this message.

        on_window_complete(results, source_messages) -> CollectResult | None
            Called after all tasks in an arrange() window complete.
            Coarser granularity than on_message_complete: useful for
            batch-level metrics or summaries that span many messages.

        on_error(task, error) -> ErrorAction | list[ExecutorTask]
            Handle executor failures. Return RETRY, SKIP, or new tasks.

        on_delivery_error(error) -> DeliveryAction
            Handle sink delivery failures. Return DLQ (default), RETRY, or SKIP.

    Generic usage (optional)::

        class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
            ...

    When type params are provided, the framework auto-deserializes
    incoming message bytes into ``InputT`` and sets ``msg.payload``.
    """

    input_model: type[BaseModel] | None = None
    output_model: type[BaseModel] | None = None
    # One details model per handler; registering it is the opt-in for the
    # probe's User-defined tab.
    probe_details_model: type[BaseModel] | None = None
    # Declared UI pages; see the Protocol attribute above.
    ui_pages: list[Page] | None = None

    # Application config declaration — see the :attr:`app_config` property
    # for the full story. ``app_config_model`` opts the handler into the
    # framework-loaded app config; ``app_env_prefix`` names the env-var
    # namespace the application owns (never ``DK_*``, which belongs to the
    # framework). ClassVar: these describe the handler CLASS, like
    # ``probe_details_model`` above, and are never set per-instance.
    app_config_model: ClassVar[type[BaseModel] | None] = None
    app_env_prefix: ClassVar[str] = 'APP_'

    # Backing field for :attr:`app_config`. A class-level ``None`` (rather
    # than an ``__init__`` assignment) keeps the property working on
    # subclasses that override ``__init__`` without calling
    # ``super().__init__()`` — the same reasoning as ``cache`` below. The
    # framework sets the loaded instance per-instance at startup.
    _app_config: BaseModel | None = None

    # Handler-facing cache attribute. The framework reassigns this to either
    # a real ``Cache`` (when ``config.cache.enabled=true``) or leaves the
    # class-level NoOpCache default in place. Kept as a class attribute (not
    # assigned in ``__init__``) so subclasses that override ``__init__``
    # without calling ``super().__init__()`` still see a working cache stub —
    # otherwise their instances would be missing the attribute and user code
    # calling ``self.cache.set(...)`` would AttributeError.
    #
    # Sharing one class-level stub across all handler instances is safe because
    # ``NoOpCache`` is completely stateless — every method either returns a
    # constant or silently discards its arguments. When the framework wires
    # in a real ``Cache`` at startup it does so per-instance (instance-level
    # attribute shadows the class-level default), so each handler still gets
    # its own cache when enabled.
    #
    # Assigned at module-import time below the class body (see after class
    # definition) because the ``NoOpCache`` import is deferred — module-level
    # import would trigger a circular import between ``handler.py`` and
    # ``cache.py`` at load time.
    #
    # Typed as :class:`CacheLike` (a structural Protocol) rather than the
    # concrete ``Cache | NoOpCache`` Union so tests can substitute a hand-rolled
    # fake without subclassing — and so future alternate cache backends
    # (Redis-backed, distributed, etc.) satisfy the contract structurally.
    cache: CacheLike

    # The four resolved generic slots, populated by ``__init_subclass__``.
    # Webapp users read ``http_request_model`` / ``http_response_model`` from
    # the *class*, mirroring how ``input_model`` / ``output_model`` work for
    # the Kafka path. ``None`` means "this slot was not specified" (PEP 696
    # default); the webapp bootstrap fail-fasts when these are None
    # and ``webapp.enabled=True``.
    http_request_model: type[BaseModel] | None = None
    http_response_model: type[BaseModel] | None = None

    # Backs :meth:`annotate`. Same lifecycle as ``cache`` above: a stateless
    # no-op stub lives on the class so ``self.annotate(...)`` is callable in
    # unit tests and before startup finishes, and the framework replaces it
    # per-instance with a real ``Annotator`` when the recorder is running and
    # ``ui.recorder.annotations_enabled`` is set. Private because users call
    # the method, never the object.
    _annotator: AnnotatorLike

    # Backs :meth:`timeline_event`. Same lifecycle as ``_annotator`` above: a
    # stateless no-op stub lives on the class so ``self.timeline_event(...)``
    # is callable in unit tests and before startup finishes, and the
    # framework replaces it per-instance with a real ``TimelineEventEmitter``
    # when the annotator is wired and ``ui.timeline.events`` declares at
    # least one type. Private because users call the method, never the
    # object.
    _timeline_events: TimelineEventEmitter | NoOpTimelineEventEmitter

    # Backs :meth:`offload`. Same lifecycle again: the class-level
    # ``InlineOffloader`` stub keeps ``await self.offload(...)`` working in
    # unit tests (it still runs the function on a thread, via
    # ``asyncio.to_thread``, just without the shared pool, metrics, or
    # recorder events), and the framework replaces it per-instance with the
    # worker's ``OffloadPool`` at startup.
    _offloader: OffloaderLike

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        input_t, output_t, http_req_t, http_resp_t = _extract_type_args(cls)
        if input_t and issubclass(input_t, BaseModel):
            cls.input_model = input_t
        if output_t and issubclass(output_t, BaseModel):
            cls.output_model = output_t
        if http_req_t and issubclass(http_req_t, BaseModel):
            cls.http_request_model = http_req_t
        if http_resp_t and issubclass(http_resp_t, BaseModel):
            cls.http_response_model = http_resp_t

    @property
    def app_config(self) -> BaseModel | None:
        """The validated user-defined application config, or ``None``.

        Declare a Pydantic model on the handler class and the framework
        loads it at startup from the same ``drakkar.yaml`` the framework
        config comes from — the reserved top-level ``app:`` section — with
        env-var overrides under the handler's own prefix and the
        framework's exact precedence (model defaults → YAML → env with
        ``__`` nesting)::

            class AppConfig(BaseModel):
                priority_threshold: int = 10
                scoring_url: str = 'http://localhost:9000/score'
                api_key: SecretStr = SecretStr('')

            class MyHandler(BaseDrakkarHandler[In, Out]):
                app_config_model = AppConfig
                app_env_prefix = 'MYAPP_'

                async def arrange(self, messages, pending):
                    threshold = self.app_config.priority_threshold
                    ...

        With that declaration, ``app: {priority_threshold: 20}`` in YAML or
        ``MYAPP_PRIORITY_THRESHOLD=20`` in the environment both reach
        ``self.app_config`` — validated fail-fast at startup, available
        before ``on_startup`` runs, and rendered as one more group in the
        debug UI's config reference (secrets masked). ``None`` when the
        handler declares no ``app_config_model``. See docs/app-config.md.
        """
        return self._app_config

    def message_label(self, msg: SourceMessage) -> str:
        """Return a short label for a message, used in logs and UI.

        Override to include application-specific fields like request_id.
        Default: partition:offset
        """
        return f'{msg.partition}:{msg.offset}'

    def task_priority(self, task: ExecutorTask) -> Any:
        """Return a sortable priority key for ordering tasks waiting on the executor pool.

        When the executor pool is saturated, queued tasks wake up in
        ascending priority order — smaller keys first. The default is
        ``min(task.source_offsets)``, so older Kafka messages drain
        before newer ones. That keeps ``MessageTracker`` /
        ``OffsetTracker`` state in front of the watermark small: the
        slowest task in a fan-out no longer anchors the whole message
        in memory while later messages pile up behind it.

        Override to inject business priority. The return value can be
        any heapq-comparable object — int, tuple, or any class with
        ``__lt__``. Two common patterns:

        - **Partition-aware ordering** — keep partition fairness while
          still preferring older messages within each partition::

              def task_priority(self, task):
                  return (task.source_offsets[0] // 1000, task.source_offsets[0])

        - **Business priority field** — read a metadata field that
          ``arrange()`` stamped on the task::

              def task_priority(self, task):
                  return (task.metadata.get('tier', 0), min(task.source_offsets))

        Equal-priority tasks tiebreak FIFO via the gate's internal
        sequence counter, so within a priority band behaviour matches
        the pre-priority semaphore.

        Errors from this method are logged + counted in
        ``drakkar_executor_priority_fn_errors_total`` and the framework
        falls back to ``min(task.source_offsets)`` so a buggy override
        never stalls a task.
        """
        return min(task.source_offsets) if task.source_offsets else 0

    def deserialize_message(self, msg: SourceMessage) -> SourceMessage:
        """Deserialize msg.value into msg.payload using input_model.

        Called by the framework before arrange(). If no input_model
        is set, returns the message unchanged.

        On parse failure ``msg.payload`` stays ``None`` and
        ``msg.parse_error`` is set; the framework then applies the
        ``kafka.on_parse_error`` policy (``skip`` / ``dlq`` / ``raise``).
        Failures are logged with partition/offset context and counted in
        ``drakkar_message_parse_failures_total``.
        """
        if self.input_model is not None:
            try:
                msg.payload = self.input_model.model_validate_json(msg.value)
                msg.parse_error = None
            except (ValidationError, TypeError, ValueError) as e:
                # ValidationError subclasses ValueError, but is listed
                # explicitly for clarity; TypeError covers a None value.
                msg.payload = None
                msg.parse_error = str(e)
                message_parse_failures.labels(partition=str(msg.partition)).inc()
                logger.warning(
                    'message_parse_failed',
                    category='handler',
                    partition=msg.partition,
                    offset=msg.offset,
                    model=self.input_model.__name__,
                    error=str(e)[:500],
                    value_snippet=msg.value[:200].decode('utf-8', errors='replace') if msg.value else '',
                )
        return msg

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
        """Called before any components are initialized.

        Return a (possibly modified) config to adjust settings at runtime.
        """
        return config

    async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None:
        """Called after all components are initialized, before the main loop.

        Use this to initialize state from DB, run migrations, load
        lookup tables, etc.
        """
        pass

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        """Group source messages into executor tasks.

        Must be implemented by the user. Receives a window of messages
        and the currently pending (in-flight) tasks for deduplication.
        """
        raise NotImplementedError('arrange() must be implemented by the user')

    async def on_task_complete(
        self,
        result: ExecutorResult,
    ) -> CollectResult | None:
        """Process a single executor result into sink payloads.

        Called after each task completes successfully. Return a
        CollectResult with payloads for configured sinks, or None
        to skip per-task delivery (e.g. when you aggregate in
        on_message_complete instead).
        """
        return None

    async def on_message_complete(
        self,
        group: MessageGroup,
    ) -> CollectResult | None:
        """Aggregate outcome for a single source message's task fan-out.

        Fires once per source message AFTER every task derived from that
        message has reached a terminal state (success, SKIP, retries
        exhausted, or replaced by a subsequent replacement-chain that
        itself terminated). Receives a ``MessageGroup`` containing:

          - ``source_message`` — the original SourceMessage
          - ``tasks`` — full task history (includes replaced tasks)
          - ``results`` — list[ExecutorResult] for terminal successes
          - ``errors`` — list[ExecutorError] for terminal failures that
            the on_error hook chose to stop on (SKIP or retries exhausted).
            Does NOT include errors whose on_error returned a replacement
            list — those are not terminal failures of the group.
          - ``started_at`` / ``finished_at`` — wall-clock timing

        Return a CollectResult to emit aggregate sink payloads, or None.

        Independent of on_task_complete — both can fire for the same
        tasks (e.g. per-task detail via on_task_complete, per-message
        rollup via on_message_complete). Offsets are committed
        immediately after this hook returns.
        """
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        """Called after all tasks in an arrange() window have completed.

        Coarser than on_message_complete — useful for batch-level
        summaries across messages in the same arrange() call. By the
        time this fires, each message's offset may already have been
        committed (see on_message_complete).
        """
        return None

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> ErrorAction | list[ExecutorTask]:
        """Handle an executor task failure.

        Return ErrorAction.RETRY to retry, ErrorAction.SKIP to drop,
        or a list of new ExecutorTasks to spawn replacement work.
        Default: SKIP.
        """
        return ErrorAction.SKIP

    async def on_delivery_error(
        self,
        error: DeliveryError,
    ) -> DeliveryAction:
        """Handle a sink delivery failure.

        Called when a sink's deliver() raises an exception. The error
        contains the sink name/type, error message, and the payloads
        that failed to deliver.

        Return:
            DeliveryAction.DLQ (default) — write to dead letter queue
            DeliveryAction.RETRY — retry delivery (up to max_retries)
            DeliveryAction.SKIP — drop the payloads, continue
        """
        return DeliveryAction.DLQ

    async def on_assign(self, partitions: list[int]) -> None:
        """Called when new partitions are assigned to this worker."""
        pass

    async def on_revoke(self, partitions: list[int]) -> None:
        """Called when partitions are revoked from this worker."""
        pass

    def annotate(
        self,
        target: SourceMessage | ExecutorTask | None,
        kind: str,
        data: Mapping[str, Any] | None = None,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Attach structured diagnostic data to a pipeline entity.

        Use this for the reasoning behind a decision — the inputs a hook
        considered, the flag that explains why a task was built the way it
        was, the alternative that was rejected. It is information not worth
        writing to a sink and not worth logging on every run, but exactly
        what someone needs when they open one message in the debug UI and
        ask why it produced what it did.

        The annotation is stored in the flight recorder and appears on the
        trace of the entity it is attached to. Recorder rotation and
        retention expire it like any other event, so nothing accumulates
        indefinitely.

        Scope comes from ``target``::

            # this one message
            self.annotate(msg, 'input_selection', {'candidates': paths, 'chosen': p})

            # this one task
            self.annotate(task, 'arg_derivation', {'template': tpl, 'flags': flags})

            # the whole arrange() window
            self.annotate(None, 'window_summary', {'deduplicated': 12})

        Args:
            target: A ``SourceMessage`` for message scope, an
                ``ExecutorTask`` for task scope, or ``None`` for the whole
                hook invocation's window.
            kind: Short name identifying what this annotation describes.
                Shown in the UI and useful for filtering; pick a stable
                value per call site rather than an interpolated string.
            data: JSON-serializable payload. Anything the encoder cannot
                represent natively degrades to its ``str()``.
            labels: Optional string labels stored on the row, indexed and
                searchable the same way ``ExecutorTask.labels`` are.

        Best-effort by design — this never raises and never affects
        processing. An oversize payload is DROPPED WHOLE rather than
        truncated, because a truncated structured document still parses and
        misleads whoever reads it. Drops are counted in
        ``drakkar_recorder_annotations_dropped_total`` and logged at warning
        level; see ``docs/annotations.md`` for the budgets and how to tune
        them.

        Calling this outside a framework-invoked hook has no pipeline entity
        to attach to, so the record is dropped and counted under
        ``reason="no_context"``.
        """
        self._annotator.emit(target, kind, data, labels=labels)

    def timeline_event(
        self,
        type_name: str,
        text: str = '',
        *,
        ts: datetime | None = None,
        end_ts: datetime | None = None,
        values: Mapping[str, Any] | None = None,
        match: TimelineMatch | None = None,
    ) -> None:
        """Emit one instance of a custom timeline event declared in ``ui.timeline.events``.

        Use this to mark a domain event on the live Debug UI timeline as it
        happens — a deploy, an incident window, a flag worth pinning — so an
        operator watching the timeline sees it lined up against the tasks
        running at the time::

            self.timeline_event('deploy', text='v2.1', values={'sha': 'ab12f'})

        ``type_name`` must match a type declared under ``ui.timeline.events``
        (see :class:`~drakkar.config.TimelineEventType` for ``kind``,
        ``action``, and the rest of a type's look and click behavior). It
        rides the same recorder storage as :meth:`annotate`.

        Best-effort by design — this never raises and never affects
        processing. An unknown ``type_name`` or a malformed emission (wrong
        shape for the type's ``kind``, an ill-formed ``match``) is dropped
        and counted in ``drakkar_recorder_annotations_dropped_total``; see
        :mod:`drakkar.timeline_events` for the exact drop rules.

        Args:
            type_name: Name of a type declared in ``ui.timeline.events``.
            text: Instance text substituted into the type's label/link
                templates.
            ts: Event start time. Defaults to now.
            end_ts: Event end time; required for ``kind=range`` types,
                rejected for every other kind.
            values: Extra instance data available to link templates.
            match: Which tasks this event correlates with, for
                ``action=highlight``/``action=filter`` types. Auto-filled
                from the current window when omitted.
        """
        self._timeline_events.emit(type_name, text, ts=ts, end_ts=end_ts, values=values, match=match)

    async def offload[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run a CPU-bound synchronous function off the event loop.

        Every hook runs on the worker's single event loop, so a hook that
        spends seconds in pure-Python computation — deeply nested loops
        deriving task parameters in ``arrange()``, result crunching in
        ``on_message_complete()`` — freezes the whole worker: Kafka
        polling, executor completions, sink flushes, the debug UI. Wrap
        that computation in a plain function and await it here instead::

            async def arrange(self, messages, pending):
                plan = await self.offload(self._build_plan, messages)
                return [ExecutorTask(...) for item in plan]

            def _build_plan(self, messages):     # plain sync function
                ...heavy nested loops...

        The function runs on a small shared thread pool
        (``offload.max_threads`` in config, default 2). This does NOT make
        the computation faster — under the GIL pure-Python work is
        serialized regardless of the thread it runs on — but the event
        loop stays responsive while it runs, which is the difference
        between a multi-second worker stall (see the runtime health
        monitor's ``runtime_stall`` events) and millisecond jitter.

        Rules inside the offloaded function:

        - It is synchronous: no ``await``, no coroutines, no loop-bound
          framework objects.
        - ``self.cache.peek`` / ``set`` / ``delete`` / ``in`` are safe —
          the cache guards its memory state with an internal lock. The
          async ``self.cache.get`` (DB fallback) is loop-only: ``await``
          it for the keys you need *before* offloading, which warms them
          into memory, then ``peek`` inside the function.
        - ``self.annotate(...)`` works and anchors to the same hook
          invocation — the hook's context is copied into the thread.

        Exceptions raised by ``fn`` propagate to the awaiting hook
        unchanged. Cancellation (partition revoked, worker shutdown)
        cancels a queued call outright; an already-running function
        cannot be interrupted — the thread finishes and the result is
        discarded.

        Callable in unit tests without a running app: the default
        offloader runs ``fn`` via ``asyncio.to_thread`` with identical
        semantics, minus pool/metrics/recorder.
        """
        return await self._offloader.run(fn, *args, **kwargs)

    # ------------------------------------------------------------------
    # Webapp hooks (optional — only invoked when webapp.enabled=True).
    #
    # Users opt in by declaring concrete types in the HttpRequestT /
    # HttpResponseT slots:
    #
    #     class MyHandler(BaseDrakkarHandler[KIn, KOut, HReq, HResp]):
    #         async def arrange_http_request(self, req, pending): ...
    #         async def on_http_request_complete(self, group): ...
    #
    # When ``webapp.enabled=True`` but a user hasn't overridden
    # ``arrange_http_request`` / ``on_http_request_complete``, the framework
    # raises ``NotImplementedError`` from the default below at request time.
    # The error message names the missing override so operators can quickly
    # find the problem in their handler subclass.
    # ------------------------------------------------------------------

    async def arrange_http_request(
        self,
        req: HttpRequestT,
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        """Translate an HTTP request into executor tasks.

        Mirrors :meth:`arrange` for the webapp path. The framework parses the
        incoming POST body as ``HttpRequestT`` (the third generic slot), then
        calls this hook on the main event loop. Each returned task is
        auto-tagged with ``origin='http'``, ``client_name``, and
        ``request_id`` by the framework before submission.

        Required when ``webapp.enabled=True``. The default raises
        ``NotImplementedError`` to fail fast at the first request — webapp
        users will see the missing-override message immediately.
        """
        raise NotImplementedError('override arrange_http_request when webapp.enabled=True')

    async def on_http_request_complete(
        self,
        group: MessageGroup,
    ) -> HttpResponseT:
        """Build the user-facing response from completed task results.

        Called once per HTTP request after ``arrange_http_request`` tasks all
        reach a terminal state. Mirrors :meth:`on_message_complete` but is
        REQUIRED for the webapp path and returns the user's
        ``HttpResponseT`` model instead of an optional ``CollectResult``.
        The framework wraps the returned model into the JSON response body
        under the ``"result"`` key (full response shape documented in
        ``docs/webapp.md``).

        Required when ``webapp.enabled=True``. The default raises
        ``NotImplementedError``.
        """
        raise NotImplementedError('override on_http_request_complete when webapp.enabled=True')

    def http_request_id(
        self,
        req: HttpRequestT,
        headers: Mapping[str, str],
    ) -> str:
        """Return the request_id used in logs, recorder rows, and the response.

        Override to use an upstream-provided correlation header (common
        examples: ``X-Request-ID``, ``X-Correlation-ID``) so a single
        request_id flows from caller → worker → downstream services.
        Whatever you return must satisfy
        :func:`drakkar.utils.validate_request_id` (≤64 chars, ASCII, no
        whitespace) — the framework calls ``validate_request_id`` on the
        return value before using it.

        Default: a fresh framework-generated id from
        :func:`drakkar.utils.make_request_id` with prefix ``'req'``. The
        default always passes ``validate_request_id`` (no need for callers
        to validate the framework-generated id).
        """
        return make_request_id('req')

    def http_request_label(
        self,
        req: HttpRequestT,
        request_id: str,
    ) -> str:
        """Return a short human-readable label for an HTTP request in logs/UI.

        Mirrors :meth:`message_label` for the webapp path. The framework
        passes the resolved ``request_id`` so the default override can be a
        no-op pass-through (``return request_id``) without needing to
        rediscover the id from ``req``. Override to embed business fields
        (e.g. ``f"{req.tenant}/{request_id}"``).
        """
        return request_id


# Attach the class-level default stubs after class definition. We do this
# outside the class body because ``NoOpCache`` is imported lazily (to avoid the
# circular ``handler.py`` ↔ ``cache.py`` import at module load time). The
# assignment runs once at import; every ``BaseDrakkarHandler`` subclass instance
# reads the shared stubs through the class attributes unless the framework
# replaces them with real objects at runtime.
def _install_default_cache() -> None:
    """Attach the stateless default stubs as class attributes.

    Called once at module import. All four stubs are stateless so sharing
    one instance across all handler classes is safe. ``InlineOffloader``
    still executes offloaded functions (on ``asyncio.to_thread``) — the
    stub-ness is only the missing pool/metrics/recorder wiring.
    """
    from drakkar.annotations import NoOpAnnotator
    from drakkar.cache import NoOpCache
    from drakkar.offload import InlineOffloader

    BaseDrakkarHandler.cache = NoOpCache()
    BaseDrakkarHandler._timeline_events = NoOpTimelineEventEmitter()
    BaseDrakkarHandler._annotator = NoOpAnnotator()
    BaseDrakkarHandler._offloader = InlineOffloader()


_install_default_cache()
