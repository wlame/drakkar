"""Integration test handler: uses `rg` to search text files for patterns."""

import json
import uuid

from drakkar import (
    BaseDrakkarHandler,
    CollectResult,
    DBRow,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)


class RipgrepHandler(BaseDrakkarHandler):
    """Searches text files using ripgrep based on incoming Kafka messages.

    Expected input message format:
    {
        "request_id": "uuid",
        "pattern": "regex pattern",
        "file_path": "/data/sample.txt"
    }

    Output message format:
    {
        "request_id": "uuid",
        "pattern": "regex pattern",
        "file_path": "/data/sample.txt",
        "match_count": 5,
        "matches": ["line1", "line2", ...]
    }
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        tasks = []
        for msg in messages:
            payload = json.loads(msg.value)
            request_id = payload.get("request_id", str(uuid.uuid4()))
            pattern = payload["pattern"]
            file_path = payload["file_path"]

            task_id = f"rg-{request_id}"

            if task_id in pending.pending_task_ids:
                continue

            tasks.append(ExecutorTask(
                task_id=task_id,
                args=[
                    "--no-filename",
                    "--no-line-number",
                    pattern,
                    file_path,
                ],
                metadata={
                    "request_id": request_id,
                    "pattern": pattern,
                    "file_path": file_path,
                },
                source_offsets=[msg.offset],
            ))
        return tasks

    async def collect(self, result: ExecutorResult) -> CollectResult | None:
        matches = [
            line for line in result.stdout.strip().split("\n") if line
        ]

        output = {
            "request_id": result.task.metadata["request_id"],
            "pattern": result.task.metadata["pattern"],
            "file_path": result.task.metadata["file_path"],
            "match_count": len(matches),
            "matches": matches[:100],
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

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        # rg exits with 1 when no matches found — treat as success with 0 matches
        if error.exit_code == 1:
            return ErrorAction.SKIP
        return ErrorAction.SKIP

    async def on_assign(self, partitions: list[int]) -> None:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo("partitions_assigned_to_handler", partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        import structlog
        logger = structlog.get_logger()
        await logger.ainfo("partitions_revoked_from_handler", partitions=partitions)
