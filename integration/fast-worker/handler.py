"""Fast symbol-count handler — lightweight pipeline for character counting.

Reads the same search-requests topic as the main workers but with a
different consumer group.  For each message, runs count-symbols.sh
which counts total characters in the file_path.  Results go to a
dedicated Kafka topic (symbol-counts).
"""

import json

import structlog
from models import CountRequest, CountResult

import drakkar as dk

logger = structlog.get_logger()


class SymbolCountHandler(dk.BaseDrakkarHandler[CountRequest, CountResult]):
    """Count characters in files, output to Kafka."""

    def message_label(self, msg: dk.SourceMessage) -> str:
        if msg.payload:
            return f'{msg.partition}:{msg.offset} [{msg.payload.request_id[:8]}] {msg.payload.file_path}'
        return f'{msg.partition}:{msg.offset}'

    async def arrange(
        self,
        messages: list[dk.SourceMessage],
        pending: dk.PendingContext,
    ) -> list[dk.ExecutorTask]:
        tasks = []
        for msg in messages:
            req: CountRequest = msg.payload
            if req is None:
                continue

            task_id = dk.make_task_id('cnt')
            if task_id in pending.pending_task_ids:
                continue

            tasks.append(
                dk.ExecutorTask(
                    task_id=task_id,
                    args=[],
                    stdin=json.dumps({'file_path': req.file_path}),
                    metadata={
                        'request_id': req.request_id,
                        'file_path': req.file_path,
                    },
                    source_offsets=[msg.offset],
                )
            )
        return tasks

    async def collect(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        meta = result.task.metadata
        stdout = result.stdout.strip()
        try:
            parsed = json.loads(stdout)
            symbol_count = int(parsed.get('symbol_count', 0))
        except (json.JSONDecodeError, ValueError):
            symbol_count = 0

        output = CountResult(
            request_id=meta['request_id'],
            file_path=meta['file_path'],
            symbol_count=symbol_count,
        )

        return dk.CollectResult(
            kafka=[dk.KafkaPayload(data=output, key=meta['request_id'].encode())],
        )

    async def on_error(self, task: dk.ExecutorTask, error: dk.ExecutorError) -> str:
        await logger.awarning(
            'count_failed',
            category='handler',
            request_id=task.metadata.get('request_id', '?'),
            task_id=task.task_id,
            exit_code=error.exit_code,
        )
        return dk.ErrorAction.SKIP
