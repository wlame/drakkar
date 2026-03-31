"""Ripgrep search handler — demonstrates all Drakkar framework features.

Shows how to:
- Use typed handler with Pydantic input/output models
- Route results to different sinks based on business logic in collect()
- Add custom Prometheus metrics
- Use async structured logging in hooks
- Handle executor failures with on_error() and retries
- Handle sink delivery failures with on_delivery_error() and DLQ
- Simulate random executor failures via --fail flag
- Use @periodic for recurring background tasks (stats, health checks)
"""

import asyncio
import os
import random

import structlog
from metrics import (
    delivery_retries_total,
    periodic_stats_runs_total,
    search_errors_total,
    search_match_count,
)
from models import SearchNotification, SearchRequest, SearchResult, SearchSummary

import drakkar as dk

logger = structlog.get_logger()

# Fail rate for simulated executor failures (passed as --fail=X to CLI)
FAIL_RATE = '0.05'


class RipgrepHandler(dk.BaseDrakkarHandler[SearchRequest, SearchResult]):
    """Searches source files using ripgrep, routes results to multiple sinks.

    Sink routing logic in collect():
    - Kafka (always): full search result to results topic
    - Postgres (always): row with key metrics
    - MongoDB (always): full document archive
    - Redis (always): cached summary with 1h TTL
    - HTTP (conditional): webhook notification if match_count > 20
    - Filesystem (conditional): JSONL log if match_count > 50
    """

    def message_label(self, msg: dk.SourceMessage) -> str:
        if msg.payload:
            return (
                f'{msg.partition}:{msg.offset} [{msg.payload.request_id[:8]}] {msg.payload.pattern}'
            )
        return f'{msg.partition}:{msg.offset}'

    async def on_startup(self, config: dk.DrakkarConfig) -> dk.DrakkarConfig:
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

    async def on_ready(self, config: dk.DrakkarConfig, db_pool: object) -> None:
        self.total_collected = 0

    # -- Periodic tasks -------------------------------------------------

    @dk.periodic(seconds=10)
    async def log_stats(self):
        """Log pipeline stats every 10 seconds. Demonstrates a recurring
        background task that accesses handler state set during processing."""
        periodic_stats_runs_total.inc()
        await logger.ainfo(
            'periodic_stats',
            category='periodic',
            total_collected=self.total_collected,
        )

    @dk.periodic(seconds=30, on_error='stop')
    async def health_check(self):
        """Verify /tmp/search-corpus exists (executor needs it).
        Demonstrates on_error='stop' — if the corpus disappears, this
        task logs an error and stops rather than spamming every 30s."""
        if not os.path.isdir('/tmp/search-corpus'):
            raise RuntimeError("Search corpus directory missing: /tmp/search-corpus")
        await logger.ainfo('health_check_ok', category='periodic')

    # -------------------------------------------------------------------

    async def arrange(
        self,
        messages: list[dk.SourceMessage],
        pending: dk.PendingContext,
    ) -> list[dk.ExecutorTask]:
        # simulate slow IO-bound preparation (e.g. DB lookup, HTTP call)
        await asyncio.sleep(random.uniform(0.05, 0.5))

        tasks = []
        for msg in messages:
            req: SearchRequest = msg.payload
            if req is None:
                continue

            task_id = dk.make_task_id('rg')
            if task_id in pending.pending_task_ids:
                continue

            tasks.append(
                dk.ExecutorTask(
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

    async def collect(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        self.total_collected += 1
        # simulate post-processing (e.g. parsing, enrichment)
        await asyncio.sleep(random.uniform(0.001, 0.005))

        matches = [line for line in result.stdout.strip().split('\n') if line]
        meta = result.task.metadata

        # build typed output models
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

        # custom Prometheus metric
        search_match_count.observe(len(matches))

        # async structured logging
        await logger.ainfo(
            'search_completed',
            category='handler',
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            match_count=len(matches),
            duration=round(result.duration_seconds, 3),
        )

        # build sink payloads with business logic
        sinks = dk.CollectResult(
            # always: full result to Kafka output topic
            kafka=[dk.KafkaPayload(data=output, key=meta['request_id'].encode())],
            # always: metrics row to Postgres
            postgres=[dk.PostgresPayload(table='search_results', data=summary)],
            # always: full document to MongoDB
            mongo=[dk.MongoPayload(collection='search_archive', data=output)],
            # always: cached summary in Redis with 1h TTL
            redis=[dk.RedisPayload(key=f'search:{meta["request_id"]}', data=summary, ttl=3600)],
        )

        # conditional: HTTP webhook for high-match results
        if len(matches) > 20:
            notification = SearchNotification(
                request_id=meta['request_id'],
                pattern=meta['pattern'],
                match_count=len(matches),
                message=f"High match count: {len(matches)} matches for '{meta['pattern']}'",
            )
            sinks.http.append(dk.HttpPayload(data=notification))
            await logger.ainfo(
                'webhook_triggered',
                category='handler',
                request_id=meta['request_id'],
                match_count=len(matches),
            )

        # conditional: JSONL file log for very high-match results
        if len(matches) > 50:
            sinks.files.append(dk.FilePayload(path='/tmp/high-match-results.jsonl', data=output))

        return sinks

    async def on_error(self, task: dk.ExecutorTask, error: dk.ExecutorError) -> str:
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
            return dk.ErrorAction.RETRY
        return dk.ErrorAction.SKIP

    async def on_delivery_error(self, error: dk.DeliveryError) -> dk.DeliveryAction:
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
            return dk.DeliveryAction.RETRY
        return dk.DeliveryAction.DLQ

    async def on_assign(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_assigned', category='handler', partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_revoked', category='handler', partitions=partitions)
