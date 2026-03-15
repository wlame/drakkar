"""User hook protocol and base handler for Drakkar framework."""

from typing import Protocol

from drakkar.models import (
    CollectResult,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    PendingContext,
    SourceMessage,
)


class DrakkarHandler(Protocol):
    """Protocol defining the hooks a user must implement."""

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        """Transform a window of messages into executor tasks.

        Receives pending context so the user can deduplicate against
        tasks still running from previous windows on this partition.
        """
        ...

    async def collect(
        self,
        result: ExecutorResult,
    ) -> CollectResult | None:
        """Process a single executor result. Called per task completion."""
        ...

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        """Called when all tasks from one arrange() window complete."""
        ...

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        """Handle executor failure.

        Return ErrorAction.RETRY, ErrorAction.SKIP, or a list of new
        ExecutorTasks to schedule as replacements.
        """
        ...

    async def on_assign(self, partitions: list[int]) -> None:
        """Called when partitions are assigned to this worker."""
        ...

    async def on_revoke(self, partitions: list[int]) -> None:
        """Called when partitions are revoked from this worker."""
        ...


class BaseDrakkarHandler:
    """Base handler with no-op defaults for optional hooks.

    Users extend this class and must override `arrange`.
    All other hooks have sensible defaults.
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        raise NotImplementedError("arrange() must be implemented by the user")

    async def collect(
        self,
        result: ExecutorResult,
    ) -> CollectResult | None:
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        return None

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        from drakkar.models import ErrorAction

        return ErrorAction.SKIP

    async def on_assign(self, partitions: list[int]) -> None:
        pass

    async def on_revoke(self, partitions: list[int]) -> None:
        pass
