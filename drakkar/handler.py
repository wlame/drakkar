"""User hook protocol and base handler for Drakkar framework.

Users extend BaseDrakkarHandler and override hooks to define their
pipeline logic: arrange() creates executor tasks, on_task_complete()
processes each result into sink payloads, and on_delivery_error()
handles sink failures.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, get_args

from pydantic import BaseModel

if TYPE_CHECKING:
    from drakkar.cache import Cache, NoOpCache
    from drakkar.config import DrakkarConfig

from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    InputT,
    MessageGroup,
    OutputT,
    PendingContext,
    SourceMessage,
)


class DrakkarHandler(Protocol):
    """Protocol defining the hooks a user must implement."""

    input_model: type[BaseModel] | None
    output_model: type[BaseModel] | None
    # Handler-facing cache. Always non-None by the time user hooks are called:
    # either a real Cache (when cache.enabled=true) or a NoOpCache stub
    # (disabled path). See BaseDrakkarHandler.cache for the default stub.
    cache: Cache | NoOpCache

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
    async def on_error(self, task: ExecutorTask, error: ExecutorError) -> str | list[ExecutorTask]: ...
    async def on_delivery_error(self, error: DeliveryError) -> DeliveryAction: ...
    async def on_assign(self, partitions: list[int]) -> None: ...
    async def on_revoke(self, partitions: list[int]) -> None: ...


def _extract_type_args(cls: type) -> tuple[type | None, type | None]:
    """Extract InputT and OutputT from BaseDrakkarHandler[InputT, OutputT]."""
    for base in getattr(cls, '__orig_bases__', ()):
        args = get_args(base)
        if len(args) == 2:
            input_t, output_t = args
            if isinstance(input_t, type) and isinstance(output_t, type):
                return input_t, output_t
    return None, None


class BaseDrakkarHandler(Generic[InputT, OutputT]):
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
    cache: Cache | NoOpCache

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        input_t, output_t = _extract_type_args(cls)
        if input_t and issubclass(input_t, BaseModel):
            cls.input_model = input_t
        if output_t and issubclass(output_t, BaseModel):
            cls.output_model = output_t

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
        """
        if self.input_model is not None:
            try:
                msg.payload = self.input_model.model_validate_json(msg.value)
            except Exception:
                msg.payload = None
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
    ) -> str | list[ExecutorTask]:
        """Handle an executor task failure.

        Return ErrorAction.RETRY to retry, ErrorAction.SKIP to drop,
        or a list of new ExecutorTasks to spawn replacement work.
        Default: SKIP.
        """
        return DeliveryAction.SKIP

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


# Attach the class-level default cache stub after class definition. We do this
# outside the class body because ``NoOpCache`` is imported lazily (to avoid the
# circular ``handler.py`` ↔ ``cache.py`` import at module load time). The
# assignment runs once at import; every ``BaseDrakkarHandler`` subclass instance
# reads the shared stub through the class attribute unless the framework
# replaces it with a real ``Cache`` at runtime.
def _install_default_cache() -> None:
    """Attach the stateless NoOpCache stub as a class attribute.

    Called once at module import. ``NoOpCache`` is stateless so sharing one
    instance across all handler classes is safe.
    """
    from drakkar.cache import NoOpCache

    BaseDrakkarHandler.cache = NoOpCache()


_install_default_cache()
