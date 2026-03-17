"""User hook protocol and base handler for Drakkar framework."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, get_args

from pydantic import BaseModel

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig

from drakkar.models import (
    CollectResult,
    InputT,
    OutputT,
    PendingContext,
    SourceMessage,
    VikingError,
    VikingResult,
    VikingTask,
)


class DrakkarHandler(Protocol):
    """Protocol defining the hooks a user must implement."""

    input_model: type[BaseModel] | None
    output_model: type[BaseModel] | None

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig: ...
    async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None: ...
    async def arrange(
        self, messages: list[SourceMessage], pending: PendingContext
    ) -> list[VikingTask]: ...
    async def collect(self, result: VikingResult) -> CollectResult | None: ...
    async def on_window_complete(
        self, results: list[VikingResult], source_messages: list[SourceMessage]
    ) -> CollectResult | None: ...
    async def on_error(self, task: VikingTask, error: VikingError) -> str | list[VikingTask]: ...
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

    Users extend this class and must override `arrange`.
    All other hooks have sensible defaults.

    Generic usage (optional):
        class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
            ...

    When type params are provided, the framework auto-deserializes
    incoming message bytes into `InputT` and sets `msg.payload`.
    Use `OutputMessage.from_model(output_instance)` to serialize output.

    Non-generic usage (backward compatible):
        class MyHandler(BaseDrakkarHandler):
            ...

    In this case, `msg.payload` is None and `msg.value` has raw bytes.
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
        return config

    async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None:
        """Called after all components are initialized, before the main loop.

        Use this to initialize state from DB, run migrations, load
        lookup tables, etc. The db_pool is an asyncpg.Pool (or None
        if Postgres is not configured).
        """
        pass

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[VikingTask]:
        raise NotImplementedError('arrange() must be implemented by the user')

    async def collect(
        self,
        result: VikingResult,
    ) -> CollectResult | None:
        return None

    async def on_window_complete(
        self,
        results: list[VikingResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        return None

    async def on_error(
        self,
        task: VikingTask,
        error: VikingError,
    ) -> str | list[VikingTask]:
        from drakkar.models import ErrorAction

        return ErrorAction.SKIP

    async def on_assign(self, partitions: list[int]) -> None:
        pass

    async def on_revoke(self, partitions: list[int]) -> None:
        pass
