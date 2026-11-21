"""Reads the part results of one job back from the results log, on demand."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import structlog
from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.aio import AIOConsumer
from pydantic import BaseModel, ValidationError

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.kafka_security import merge_client_config
from drakkar.split.models import PartResult, SplitResultsExpired, SplitResultsUnavailable
from drakkar.split.records import parse_log_record, part_result_from_body
from drakkar.split.table import BITS_PER_BYTE, MAX_PART_INDEX, JobState

logger = structlog.get_logger()

BATCH_SIZE = 500
POLL_TIMEOUT_SECONDS = 1.0
WATERMARK_TIMEOUT_SECONDS = 5.0
NO_PROGRESS_LIMIT_SECONDS = 30.0
READER_MAX_WORKERS = 2


def results_consumer_factory(connection: KafkaConnectionConfig, group: str) -> Callable[[], Any]:
    """A factory of the short-lived consumer that reads the results log.

    The consumer only uses ``assign``: it never subscribes and never commits.
    """
    config = merge_client_config(
        {
            'bootstrap.servers': connection.brokers,
            'group.id': group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'error',
        },
        connection.security,
        connection.client_config,
    )

    def build() -> AIOConsumer:
        return AIOConsumer(config, max_workers=READER_MAX_WORKERS)

    return build


def _mark(bits: bytearray, index: int) -> bool:
    """Set the bit of ``index``; False when it was set already."""
    byte, bit = divmod(index, BITS_PER_BYTE)
    if byte >= len(bits):
        bits.extend(bytes(byte + 1 - len(bits)))
    if bits[byte] >> bit & 1:
        return False
    bits[byte] |= 1 << bit
    return True


def _is_job_record(message: Any, job: JobState, group: str) -> dict[str, Any] | None:
    """The body of a ``result`` record of the job's current attempt, else None."""
    entry = parse_log_record(message.headers(), message.value())
    if entry is None or entry.kind != 'result':
        return None
    ref = entry.ref
    same_job = (ref.group, ref.partition, ref.offset, ref.attempt_id) == (
        group,
        job.partition,
        job.offset,
        job.attempt_id,
    )
    return entry.body if same_job and message.key() == f'{job.partition}:{job.offset}'.encode() else None


def _part_result(body: dict[str, Any], job: JobState) -> PartResult | None:
    """The part result of a ``result`` body; None for a body of the wrong shape or an index out of range."""
    try:
        result = part_result_from_body(body)
    except (KeyError, ValidationError):
        return None
    limit = job.parts if job.parts is not None else MAX_PART_INDEX + 1
    return result if 0 <= result.index < limit else None


def _with_typed_data(result: PartResult, part_data_model: type[BaseModel] | None) -> PartResult:
    """Validate ``data`` with the model; data that does not validate stays a dict."""
    if part_data_model is None or not isinstance(result.data, dict):
        return result
    try:
        return result.model_copy(update={'data': part_data_model.model_validate(result.data)})
    except ValidationError:
        return result


async def _check_not_expired(consumer: Any, topic: str, partition: int, job: JobState) -> None:
    low, _high = await consumer.get_watermark_offsets(
        TopicPartition(topic, partition), timeout=WATERMARK_TIMEOUT_SECONDS
    )
    if low > job.first_open_offset:
        raise SplitResultsExpired(
            f'the results log of partition {partition} starts at offset {low}, '
            f'after the first record of the job at offset {job.first_open_offset}'
        )


async def read_results(
    *,
    consumer_factory: Callable[[], Any],
    topic: str,
    partition: int,
    job: JobState,
    group: str,
    part_data_model: type[BaseModel] | None,
) -> AsyncIterator[PartResult]:
    """Yield the part results of ``job`` in log order.

    Reads partition ``partition`` of ``topic`` from the job's first ``open`` record to its last counted
    result. Only records of the job's current attempt count, and a part is yielded once. ``group`` is the
    consumer group of the source, which every record of the job carries.

    Raises:
        SplitResultsExpired: the log no longer holds the start of the job.
        SplitResultsUnavailable: the client failed, or the read made no progress for too long.
    """
    last = job.last_result_offset
    if last is None:
        return
    consumer = consumer_factory()
    try:
        async for result in _read(consumer, topic, partition, job, group, last, part_data_model):
            yield result
    finally:
        try:
            await consumer.close()
        except KafkaException as exc:
            logger.warning('split_results_reader_close_failed', category='split', error=str(exc))


async def _read(
    consumer: Any,
    topic: str,
    partition: int,
    job: JobState,
    group: str,
    last: int,
    part_data_model: type[BaseModel] | None,
) -> AsyncIterator[PartResult]:
    loop = asyncio.get_running_loop()
    seen = bytearray()
    try:
        await _check_not_expired(consumer, topic, partition, job)
        await consumer.assign([TopicPartition(topic, partition, job.first_open_offset)])
        last_progress = loop.time()
        done = False
        while not done:
            messages = await consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
            if messages:
                last_progress = loop.time()
            elif loop.time() - last_progress >= NO_PROGRESS_LIMIT_SECONDS:
                raise SplitResultsUnavailable(
                    f'no record of partition {partition} for {NO_PROGRESS_LIMIT_SECONDS:.0f} seconds'
                )
            for message in messages:
                if message.error():
                    raise SplitResultsUnavailable(f'reading the results log failed: {message.error()}')
                if message.offset() > last:
                    done = True
                    break
                body = _is_job_record(message, job, group)
                result = _part_result(body, job) if body is not None else None
                if result is not None and _mark(seen, result.index):
                    yield _with_typed_data(result, part_data_model)
                if message.offset() == last:
                    done = True
                    break
            await asyncio.sleep(0)
    except KafkaException as exc:
        raise SplitResultsUnavailable(f'reading the results log failed: {exc}') from exc
