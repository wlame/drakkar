"""Integration test handler: runs `rg` against project source with all sinks.

Demonstrates:
- Typed handler with Pydantic models
- Business logic in collect() routing to different sinks based on result
- Failure simulation via --fail flag
- on_error / on_delivery_error hooks
- Async structured logging from hooks
- Custom Prometheus metrics in collect()
"""

import asyncio
import random

import structlog
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

from drakkar import (
    BaseDrakkarHandler,
    CollectResult,
    DeliveryAction,
    DeliveryError,
    DrakkarConfig,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MongoPayload,
    PendingContext,
    PostgresPayload,
    RedisPayload,
    SourceMessage,
    make_task_id,
)

logger = structlog.get_logger()


# --- Custom Prometheus metrics (user-defined, not framework metrics) ---

search_match_count = Histogram(
    'app_search_match_count',
    'Number of matches per search request',
    buckets=(0, 1, 5, 10, 50, 100, 500),
)

search_errors_total = Counter(
    'app_search_errors_total',
    'Total search executor failures',
    ['error_type'],
)

delivery_retries_total = Counter(
    'app_delivery_retries_total',
    'Total sink delivery retries',
    ['sink_type'],
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


class SearchSummary(BaseModel):
    """Compact summary for Redis cache and MongoDB archive."""

    request_id: str
    pattern: str
    match_count: int
    duration_seconds: float


class SearchNotification(BaseModel):
    """Notification payload for HTTP webhook (high match counts only)."""

    request_id: str
    pattern: str
    match_count: int
    message: str


# --- Handler ---

# Fail rate for simulated executor failures (passed as --fail=X to CLI)
FAIL_RATE = '0.05'


class RipgrepHandler(BaseDrakkarHandler[SearchRequest, SearchResult]):
    """Searches source files using ripgrep, routes results to multiple sinks.

    Sink routing logic in collect():
    - Kafka (always): full search result to results topic
    - Postgres (always): row with key metrics
    - MongoDB (always): full document archive
    - Redis (always): cached summary with 1h TTL
    - HTTP (conditional): webhook notification if match_count > 20
    - Filesystem (conditional): JSONL log if match_count > 50
    """

    def message_label(self, msg: SourceMessage) -> str:
        if msg.payload:
            return (
                f'{msg.partition}:{msg.offset} [{msg.payload.request_id[:8]}] {msg.payload.pattern}'
            )
        return f'{msg.partition}:{msg.offset}'

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
        await logger.ainfo(
            'handler_startup',
            category='handler',
            input_model=self.input_model.__name__ if self.input_model else None,
            output_model=self.output_model.__name__ if self.output_model else None,
            binary=config.executor.binary_path,
            max_workers=config.executor.max_workers,
            fail_rate=FAIL_RATE,
        )
        return config

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        await asyncio.sleep(random.uniform(0.05, 0.5))

        tasks = []
        for msg in messages:
            req: SearchRequest = msg.payload
            if req is None:
                continue

            task_id = make_task_id('rg')
            if task_id in pending.pending_task_ids:
                continue

            tasks.append(
                ExecutorTask(
                    task_id=task_id,
                    args=[str(req.repeat), req.pattern, req.file_path, f'--fail={FAIL_RATE}'],
                    metadata={
                        'request_id': req.request_id,
                        'pattern': req.pattern,
                        'file_path': req.file_path,
                        'repeat': req.repeat,
                    },
                    source_offsets=[msg.offset],
                )
            )
        return tasks

    async def collect(self, result: ExecutorResult) -> CollectResult | None:
        await asyncio.sleep(random.uniform(0.001, 0.005))

        matches = [line for line in result.stdout.strip().split('\n') if line]
        meta = result.task.metadata

        output = SearchResult(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            file_path=meta['file_path'],
            repeat=meta['repeat'],
            match_count=len(matches),
            duration_seconds=result.duration_seconds,
            matches=matches[:50],
        )

        summary = SearchSummary(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            match_count=len(matches),
            duration_seconds=result.duration_seconds,
        )

        # --- Custom Prometheus metrics ---
        search_match_count.observe(len(matches))

        # --- Async structured logging ---
        await logger.ainfo(
            'search_completed',
            category='handler',
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            match_count=len(matches),
            duration=round(result.duration_seconds, 3),
        )

        # --- Build sink payloads with business logic ---
        sinks = CollectResult(
            # always: full result to Kafka output topic
            kafka=[KafkaPayload(data=output, key=meta['request_id'].encode())],
            # always: metrics row to Postgres
            postgres=[PostgresPayload(table='search_results', data=summary)],
            # always: full document to MongoDB
            mongo=[MongoPayload(collection='search_archive', data=output)],
            # always: cached summary in Redis with 1h TTL
            redis=[RedisPayload(key=f'search:{meta["request_id"]}', data=summary, ttl=3600)],
        )

        # conditional: HTTP webhook for high-match results
        if len(matches) > 20:
            notification = SearchNotification(
                request_id=meta['request_id'],
                pattern=meta['pattern'],
                match_count=len(matches),
                message=f"High match count: {len(matches)} matches for pattern '{meta['pattern']}'",
            )
            sinks.http.append(HttpPayload(data=notification))
            await logger.ainfo(
                'webhook_triggered',
                category='handler',
                request_id=meta['request_id'],
                match_count=len(matches),
            )

        # conditional: JSONL file log for very high-match results
        if len(matches) > 50:
            sinks.files.append(FilePayload(path='/tmp/high-match-results.jsonl', data=output))

        return sinks

    async def on_error(self, task: ExecutorTask, error: ExecutorError) -> str:
        error_type = 'timeout' if error.exception and 'Timeout' in error.exception else 'exit_code'
        search_errors_total.labels(error_type=error_type).inc()

        await logger.awarning(
            'search_failed',
            category='handler',
            request_id=task.metadata.get('request_id', '?'),
            task_id=task.task_id,
            exit_code=error.exit_code,
            error_type=error_type,
        )

        # retry simulated failures, skip everything else
        if error.exit_code == 1 and error.stderr and 'SIMULATED FAILURE' in error.stderr:
            await logger.ainfo(
                'retrying_simulated_failure', category='handler', task_id=task.task_id
            )
            return ErrorAction.RETRY
        return ErrorAction.SKIP

    async def on_delivery_error(self, error: DeliveryError) -> DeliveryAction:
        delivery_retries_total.labels(sink_type=error.sink_type).inc()

        await logger.awarning(
            'delivery_failed',
            category='handler',
            sink_name=error.sink_name,
            sink_type=error.sink_type,
            error=error.error,
            payload_count=len(error.payloads),
        )

        # retry HTTP/Redis failures (transient), DLQ for everything else
        if error.sink_type in ('http', 'redis'):
            return DeliveryAction.RETRY
        return DeliveryAction.DLQ

    async def on_assign(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_assigned', category='handler', partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_revoked', category='handler', partitions=partitions)
