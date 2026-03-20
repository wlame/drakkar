"""User hook protocol and base handler for Drakkar framework.

Users extend BaseDrakkarHandler and override hooks to define their
pipeline logic: arrange() creates executor tasks, collect() processes
results into sink payloads, and on_delivery_error() handles sink failures.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, get_args

from pydantic import BaseModel

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig

from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    InputT,
    OutputT,
    PendingContext,
    SourceMessage,
)


class DrakkarHandler(Protocol):
    """Protocol defining the hooks a user must implement."""

    input_model: type[BaseModel] | None
    output_model: type[BaseModel] | None

    def message_label(self, msg: SourceMessage) -> str: ...
    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig: ...
    async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None: ...
    async def arrange(
        self, messages: list[SourceMessage], pending: PendingContext
    ) -> list[ExecutorTask]: ...
    async def collect(self, result: ExecutorResult) -> CollectResult | None: ...
    async def on_window_complete(
        self, results: list[ExecutorResult], source_messages: list[SourceMessage]
    ) -> CollectResult | None: ...
    async def on_error(
        self, task: ExecutorTask, error: ExecutorError
    ) -> str | list[ExecutorTask]: ...
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

    Hooks:
        arrange(messages, pending) -> list[ExecutorTask]
            Required. Groups source messages into subprocess tasks.

        collect(result) -> CollectResult | None
            Process each executor result. Return a CollectResult with
            sink payloads to deliver::

                return CollectResult(
                    kafka=[KafkaPayload(data=my_output, key=b"abc")],
                    postgres=[PostgresPayload(table="results", data=my_output)],
                )

        on_window_complete(results, source_messages) -> CollectResult | None
            Called after all tasks in a window complete. Use for
            aggregation across the full window.

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

    async def collect(
        self,
        result: ExecutorResult,
    ) -> CollectResult | None:
        """Process a single executor result into sink payloads.

        Called after each task completes successfully. Return a
        CollectResult with payloads for configured sinks, or None
        to skip delivery for this result.
        """
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        """Called after all tasks in a window have completed.

        Use for aggregation, summary metrics, or batch-level outputs.
        Return a CollectResult or None.
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
