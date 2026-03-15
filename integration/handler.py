"""Integration test handler: runs `rg` multiple times against project source."""

import asyncio
import json
import random

from drakkar import (
    BaseDrakkarHandler,
    CollectResult,
    DBRow,
    DrakkarConfig,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    make_task_id,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)


class RipgrepHandler(BaseDrakkarHandler):
    """Searches source files using ripgrep based on incoming Kafka messages.

    Input message:
    {
        "request_id": "uuid",
        "pattern": "regex pattern",
        "file_path": "/project/drakkar/app.py",
        "repeat": 5
    }

    The executor runs a shell script that calls `rg` `repeat` times,
    simulating CPU-bound work. 3% of messages have repeat=200 to
    simulate long-running tasks.
    """

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo(
            "handler_startup",
            category="handler",
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
            payload = json.loads(msg.value)
            request_id = payload.get("request_id", make_task_id("req"))
            pattern = payload["pattern"]
            file_path = payload["file_path"]
            repeat = payload.get("repeat", 1)

            task_id = make_task_id("rg")

            if task_id in pending.pending_task_ids:
                continue

            tasks.append(ExecutorTask(
                task_id=task_id,
                args=[str(repeat), pattern, file_path],
                metadata={
                    "request_id": request_id,
                    "pattern": pattern,
                    "file_path": file_path,
                    "repeat": repeat,
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

        output = {
            "request_id": result.task.metadata["request_id"],
            "pattern": result.task.metadata["pattern"],
            "file_path": result.task.metadata["file_path"],
            "repeat": result.task.metadata["repeat"],
            "match_count": len(matches),
            "duration_seconds": result.duration_seconds,
            "matches": matches[:50],
        }

        return CollectResult(
            output_messages=[
                OutputMessage(
                    key=result.task.metadata["request_id"].encode(),
                    value=json.dumps(output).encode(),
                ),
            ],
            db_rows=[
                DBRow(
                    table="search_results",
                    data={
                        "request_id": result.task.metadata["request_id"],
                        "pattern": result.task.metadata["pattern"],
                        "file_path": result.task.metadata["file_path"],
                        "match_count": len(matches),
                        "duration_seconds": result.duration_seconds,
                    },
                ),
            ],
        )

    async def on_window_complete(self, results, source_messages):
        return None

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
