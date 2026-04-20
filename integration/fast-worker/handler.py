"""Fast symbol-count handler — lightweight pipeline for character counting.

Reads the same search-requests topic as the main workers but with a
different consumer group. For each message, runs count-symbols.sh
which counts total characters in the file_path.

Demonstrates two complementary output paths:
  - ``collect()`` emits one per-message result to the ``per_message_counts``
    Kafka sink (one message in → one message out, same granularity).
  - ``on_window_complete()`` emits one aggregate row per arrange/collect
    window to the ``window_summaries`` Kafka sink — statistics over all
    CountResults in that window. Lower rate, but richer per event.
"""

import json
import os

import structlog
from models import CountRequest, CountResult, WindowSummary

import drakkar as dk

logger = structlog.get_logger()


class SymbolCountHandler(dk.BaseDrakkarHandler[CountRequest, CountResult]):
    """Count characters in files, output per-message and per-window stats."""

    def __init__(self) -> None:
        super().__init__()
        # Counter used as window_id for the summary records — monotonic
        # within this worker instance so downstream consumers can order
        # windows from a single worker. Across workers, use (worker_id,
        # window_id) as the identity.
        self._window_counter = 0

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
                    args=[req.file_path],
                    stdin=json.dumps({'file_path': req.file_path}),
                    metadata={
                        'request_id': req.request_id,
                        'file_path': req.file_path,
                    },
                    labels={
                        'request_id': req.request_id,
                    },
                    source_offsets=[msg.offset],
                )
            )
        return tasks

    @staticmethod
    def _parse_symbol_count(stdout: str) -> int:
        """Extract the numeric symbol_count from the subprocess stdout.

        count-symbols.sh writes a one-line JSON document on stdout. If
        parsing fails (garbage output, non-JSON), treat the count as 0
        rather than crashing the task — the exit code would already have
        signalled failure if the subprocess itself errored.
        """
        try:
            return int(json.loads(stdout.strip()).get('symbol_count', 0))
        except (json.JSONDecodeError, ValueError):
            return 0

    async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        meta = result.task.metadata
        output = CountResult(
            request_id=meta['request_id'],
            file_path=meta['file_path'],
            symbol_count=self._parse_symbol_count(result.stdout),
        )

        # Per-message result → the "per_message_counts" Kafka sink instance.
        # The `sink=` field is the USER-CHOSEN instance name from the YAML
        # config — it is not a Drakkar keyword. With multiple Kafka sinks
        # configured, every KafkaPayload must specify which one to target.
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=output,
                    key=meta['request_id'].encode(),
                    sink='per_message_counts',
                ),
            ],
        )

    async def on_window_complete(
        self,
        results: list[dk.ExecutorResult],
        source_messages: list[dk.SourceMessage],
    ) -> dk.CollectResult | None:
        """Aggregate every per-message count in the finished window.

        Fires once per arrange() window (after every task in that window
        has either succeeded or been SKIP'd/DLQ'd). The return value
        flows through the same sink manager as collect()'s return value,
        so it can target any configured sink instance.
        """
        # Only consider successful tasks — failed ones contribute no count.
        counts = [self._parse_symbol_count(r.stdout) for r in results if r.exit_code == 0]
        if not counts:
            return None

        self._window_counter += 1
        # request_id is per-message; we use source_messages to reach it
        # because results[] for a window includes retries and replacements.
        req_ids = [m.payload.request_id for m in source_messages if m.payload is not None]
        summary = WindowSummary(
            worker_id=os.environ.get('WORKER_ID', 'unknown'),
            window_id=self._window_counter,
            file_count=len(counts),
            total_chars=sum(counts),
            avg_chars=round(sum(counts) / len(counts), 2),
            max_chars=max(counts),
            min_chars=min(counts),
            first_request_id=req_ids[0] if req_ids else '',
            last_request_id=req_ids[-1] if req_ids else '',
        )

        await logger.ainfo(
            'window_summary_emitted',
            category='handler',
            window_id=self._window_counter,
            file_count=summary.file_count,
            total_chars=summary.total_chars,
            avg_chars=summary.avg_chars,
        )

        # Route to the "window_summaries" Kafka sink (a user-chosen
        # instance name — see drakkar.yaml). Distinct from the per-message
        # "per_message_counts" sink used by collect().
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=summary,
                    key=f'{summary.worker_id}:{summary.window_id}'.encode(),
                    sink='window_summaries',
                ),
            ],
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
