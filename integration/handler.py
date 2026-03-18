"""Integration test handler: runs `rg` multiple times against project source.

Demonstrates the typed handler pattern with Pydantic models for
input (SearchRequest) and output (SearchResult) messages.
"""

import asyncio
import random

from pydantic import BaseModel, Field

from drakkar import (
    BaseDrakkarHandler,
    CollectResult,
    DBRow,
    DrakkarConfig,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
    make_task_id,
)


# --- Typed message models ---


class SearchRequest(BaseModel):
    """Input message schema: what to search for."""

    request_id: str
    pattern: str
    file_path: str
    repeat: int = 1


class SearchResult(BaseModel):
    """Output message schema: search results."""

    request_id: str
    pattern: str
    file_path: str
    repeat: int
    match_count: int
    duration_seconds: float
    matches: list[str] = Field(default_factory=list)


# --- Handler ---


class RipgrepHandler(BaseDrakkarHandler[SearchRequest, SearchResult]):
    """Searches source files using ripgrep based on incoming Kafka messages.

    Uses typed handler: msg.payload is a SearchRequest instance,
    auto-deserialized from Kafka message bytes by the framework.
    """

    def message_label(self, msg: SourceMessage) -> str:
        """Show request_id + pattern for debugging."""
        if msg.payload:
            return f'{msg.partition}:{msg.offset} [{msg.payload.request_id[:8]}] {msg.payload.pattern}'
        return f'{msg.partition}:{msg.offset}'

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo(
            "handler_startup",
            category="handler",
            input_model=self.input_model.__name__ if self.input_model else None,
            output_model=self.output_model.__name__ if self.output_model else None,
            binary=config.executor.binary_path,
            max_workers=config.executor.max_workers,
        )
        return config

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # simulate IO-bound preparation (e.g., DB lookup, cache check)
        await asyncio.sleep(random.uniform(0.005, 0.03))

        tasks = []
        for msg in messages:
            req: SearchRequest = msg.payload
            if req is None:
                continue

            task_id = make_task_id("rg")
            if task_id in pending.pending_task_ids:
                continue

            tasks.append(ExecutorTask(
                task_id=task_id,
                args=[str(req.repeat), req.pattern, req.file_path],
                metadata={
                    "request_id": req.request_id,
                    "pattern": req.pattern,
                    "file_path": req.file_path,
                    "repeat": req.repeat,
                },
                source_offsets=[msg.offset],
            ))
        return tasks

    async def collect(self, result: ExecutorResult) -> CollectResult | None:
        # simulate post-processing (e.g., parsing, enrichment)
        await asyncio.sleep(random.uniform(0.002, 0.01))

        matches = [
            line for line in result.stdout.strip().split("\n") if line
        ]

        # build typed output model
        output = SearchResult(
            request_id=result.task.metadata["request_id"],
            pattern=result.task.metadata["pattern"],
            file_path=result.task.metadata["file_path"],
            repeat=result.task.metadata["repeat"],
            match_count=len(matches),
            duration_seconds=result.duration_seconds,
            matches=matches[:50],
        )

        return CollectResult(
            output_messages=[
                OutputMessage.from_model(
                    output,
                    key=result.task.metadata["request_id"].encode(),
                ),
            ],
            db_rows=[
                DBRow(
                    table="search_results",
                    data={
                        "request_id": output.request_id,
                        "pattern": output.pattern,
                        "file_path": output.file_path,
                        "match_count": output.match_count,
                        "duration_seconds": output.duration_seconds,
                    },
                ),
            ],
        )

    async def on_error(self, task: ExecutorTask, error: ExecutorError):
        # rg exits with 1 when no matches found — not a real error
        return ErrorAction.SKIP

    async def on_assign(self, partitions: list[int]) -> None:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo("handler_partitions_assigned", category="handler", partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo("handler_partitions_revoked", category="handler", partitions=partitions)
