"""Ad-hoc Kafka reads with zero consumer-group side effects.

Backs the UI server's ``/api/debug/kafka/*`` endpoints: fetch one message
by (alias, partition, offset), or stream a time window of messages. The
logic lives here rather than in the routes module so it can be exercised
without FastAPI and reused by future surfaces (CLI, MCP).

No consumer-group side effects — the contract
---------------------------------------------
Every read builds a short-lived :class:`AIOConsumer` that only ever calls
``assign()`` — never ``subscribe()``, never ``commit()`` — with
``enable.auto.commit=false``. ``assign()`` does not join a consumer group,
and with zero commits the broker stores no offsets, so these reads are
invisible to the pipeline's consumer group and to any other group. The
``group.id`` below is required by librdkafka to construct a consumer but
never reaches the broker's group coordinator.

Topic aliases — the security boundary
-------------------------------------
Callers never name a raw Kafka topic. They name an *alias*, and the alias
table is built from the worker's own config (`build_alias_table`):

- ``source``          — the pipeline input topic (``kafka.source_topic``)
- ``dlq``             — the dead-letter topic (``dlq.topic`` or its
                        ``{source_topic}_dlq`` default)
- ``<sink name>``     — each configured Kafka sink instance, under the
                        operator-chosen instance name

Only these topics are reachable, and responses echo the alias — the raw
topic name never appears in the API. Broker addresses and credentials
resolve per alias with the same inheritance rule the pipeline uses
(``resolve_client``): a sink/DLQ with empty ``brokers`` reads from the
consumer's cluster with the consumer's credentials.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Literal

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.aio import AIOConsumer
from pydantic import BaseModel

from drakkar.config import DrakkarConfig
from drakkar.kafka_security import KafkaSecurityConfig, merge_client_config, resolve_client

logger = structlog.get_logger()

# Required by librdkafka to build a consumer, but assign()-only usage never
# joins a group or commits, so this id never reaches the group coordinator.
# Fixed and documented so broker-side ACLs can allow it explicitly if a
# cluster restricts group names.
READ_GROUP_ID = 'drakkar-ui-read'

# Wall-clock caps, in the spirit of consumer.py's LAG_QUERY_TIMEOUT_SECONDS:
# librdkafka blocks indefinitely without a timeout, and an untimed call
# parks one of the consumer's executor threads forever.
METADATA_TIMEOUT_SECONDS = 5.0
FETCH_DEADLINE_SECONDS = 10.0
POLL_TIMEOUT_SECONDS = 1.0

# End-to-end cap on ONE stream request. The per-poll timeout bounds each
# librdkafka call, but not the loop around them: a partition whose window
# never closes — the messages between the start offset and the high
# watermark were compacted or aged out, so the offsets simply do not
# arrive — leaves ``active`` non-empty and ``remaining`` positive forever,
# and the request spins holding a consumer, a connection and a task.
# Generous, because a legitimate wide window over a slow cluster is a real
# use; the client gets whatever was emitted before the deadline.
STREAM_DEADLINE_SECONDS = 120.0

# Hard cap on messages one stream request may emit. Protects the worker
# (and the reader's sanity) from an unbounded window over a large topic;
# a client that needs more traverses in windows using the last message's
# timestamp/offset as the next start.
STREAM_LIMIT_MAX = 10_000

# One ad-hoc consumer serves one request sequentially — it never needs the
# pipeline consumer's 8-thread pool (CONSUMER_MAX_WORKERS).
_READER_MAX_WORKERS = 2


# ---- alias table ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AliasTarget:
    """One readable topic, resolved from config: where it lives and how to connect."""

    alias: str
    kind: Literal['source', 'dlq', 'sink']
    topic: str
    brokers: str
    security: KafkaSecurityConfig
    client_config: dict[str, str]


def build_alias_table(config: DrakkarConfig) -> dict[str, AliasTarget]:
    """Map every readable alias to its resolved topic + client settings.

    Reserved aliases ``source`` and ``dlq`` always win; a Kafka sink
    instance that shadows one of them is skipped with a warning so the
    reserved meaning stays stable (the sink itself is unaffected —
    only its read alias is unavailable).
    """
    kafka = config.kafka
    table: dict[str, AliasTarget] = {
        'source': AliasTarget(
            alias='source',
            kind='source',
            topic=kafka.source_topic,
            brokers=kafka.brokers,
            security=kafka.security,
            client_config=dict(kafka.client_config),
        )
    }

    dlq_client = resolve_client(
        config.dlq.brokers,
        config.dlq.security,
        config.dlq.client_config,
        fallback_brokers=kafka.brokers,
        fallback_security=kafka.security,
        fallback_client_config=kafka.client_config,
    )
    table['dlq'] = AliasTarget(
        alias='dlq',
        kind='dlq',
        topic=config.dlq.topic or f'{kafka.source_topic}_dlq',
        brokers=dlq_client.brokers,
        security=dlq_client.security,
        client_config=dict(dlq_client.client_config),
    )

    for name, sink in config.sinks.kafka.items():
        if name in table:
            logger.warning(
                'kafka_read_alias_shadowed',
                category='kafka',
                alias=name,
                reason="sink instance name collides with a reserved read alias ('source'/'dlq'); "
                'the sink still delivers, but it is not readable under this alias',
            )
            continue
        sink_client = resolve_client(
            sink.brokers,
            sink.security,
            sink.client_config,
            fallback_brokers=kafka.brokers,
            fallback_security=kafka.security,
            fallback_client_config=kafka.client_config,
        )
        table[name] = AliasTarget(
            alias=name,
            kind='sink',
            topic=sink.topic,
            brokers=sink_client.brokers,
            security=sink_client.security,
            client_config=dict(sink_client.client_config),
        )
    return table


# ---- wire models ------------------------------------------------------------


class KafkaReadHeader(BaseModel):
    """One Kafka record header. Values are bytes on the wire, so the same
    utf-8-or-base64 encoding scheme as the payload applies."""

    key: str
    value: str | None
    value_encoding: Literal['utf-8', 'base64'] | None


class KafkaReadMessage(BaseModel):
    """One Kafka record with its metadata, as served to API clients.

    ``alias`` echoes the configured read alias — the raw topic name is
    deliberately absent from the wire shape (the alias table is the
    security boundary; see the module docstring).
    """

    alias: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: str | None
    key_encoding: Literal['utf-8', 'base64'] | None
    payload: str
    payload_encoding: Literal['utf-8', 'base64']
    payload_size_bytes: int
    headers: list[KafkaReadHeader]


def _encode_bytes(raw: bytes | None) -> tuple[str | None, Literal['utf-8', 'base64'] | None]:
    """Render record bytes as text: utf-8 when it decodes, base64 otherwise.

    JSON cannot carry raw bytes, and Kafka payloads/keys/header values are
    all bytes. Returning the encoding alongside the text lets clients
    round-trip binary values without guessing.
    """
    if raw is None:
        return None, None
    try:
        return raw.decode('utf-8'), 'utf-8'
    except UnicodeDecodeError:
        return base64.b64encode(raw).decode('ascii'), 'base64'


def _build_message(alias: str, msg: Message) -> KafkaReadMessage:
    """Convert one confluent-kafka Message into the wire model."""
    payload_raw: bytes = msg.value() or b''
    payload, payload_encoding = _encode_bytes(payload_raw)
    key, key_encoding = _encode_bytes(msg.key())
    # msg.timestamp() -> (type, value); TIMESTAMP_NOT_AVAILABLE (type 0)
    # means the broker/producer stamped nothing usable.
    ts_type, ts_value = msg.timestamp()
    headers = []
    for h_key, h_value in msg.headers() or []:
        # The stubs allow str header values; normalize to bytes so one
        # encoding path serves both.
        raw_value = h_value.encode('utf-8') if isinstance(h_value, str) else h_value
        value, value_encoding = _encode_bytes(raw_value)
        headers.append(KafkaReadHeader(key=h_key, value=value, value_encoding=value_encoding))
    return KafkaReadMessage(
        alias=alias,
        # partition()/offset() are typed Optional in the stubs but a
        # delivered record always carries both; coalesce for the checker.
        partition=msg.partition() or 0,
        offset=msg.offset() or 0,
        timestamp_ms=ts_value if ts_type != 0 and ts_value > 0 else None,
        key=key,
        key_encoding=key_encoding,
        # payload_raw is bytes (b'' when the record has a null value), so
        # _encode_bytes never returns None for it.
        payload=payload or '',
        payload_encoding=payload_encoding or 'utf-8',
        payload_size_bytes=len(payload_raw),
        headers=headers,
    )


# ---- errors -----------------------------------------------------------------


class KafkaReadNotFound(Exception):
    """The requested coordinates do not resolve to a message (HTTP 404)."""


class KafkaReadUnavailable(Exception):
    """The brokers could not answer within the deadline (HTTP 502)."""


# ---- readers ----------------------------------------------------------------


def _reader_conf(target: AliasTarget) -> dict:
    """librdkafka properties for one ad-hoc reader.

    Framework keys first, then the alias's security block, then its raw
    ``client_config`` escape hatch — the same precedence the pipeline
    consumer uses (``merge_client_config``).
    """
    return merge_client_config(
        {
            'bootstrap.servers': target.brokers,
            'group.id': READ_GROUP_ID,
            'enable.auto.commit': False,
            # Reads are explicit-coordinate only; if an assign() offset is
            # somehow out of range the reader must fail loudly, not fall
            # back to an implicit position.
            'auto.offset.reset': 'error',
        },
        target.security,
        target.client_config,
    )


def _watermarks_or_raise(exc: KafkaException) -> KafkaReadUnavailable | KafkaReadNotFound:
    """Classify a metadata/watermark failure into the HTTP-facing errors."""
    err = exc.args[0] if exc.args else None
    code = err.code() if isinstance(err, KafkaError) else None
    if code in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_PARTITION, KafkaError._UNKNOWN_TOPIC):
        return KafkaReadNotFound('Unknown topic or partition for this alias')
    return KafkaReadUnavailable(f'Kafka metadata query failed: {exc}')


async def fetch_message(target: AliasTarget, partition: int, offset: int) -> KafkaReadMessage:
    """Fetch exactly one message by (partition, offset) from the alias's topic.

    Raises:
        KafkaReadNotFound: partition unknown, offset outside the current
            watermarks, or the record at that offset was compacted/deleted.
        KafkaReadUnavailable: brokers unreachable or the fetch deadline
            (``FETCH_DEADLINE_SECONDS``) elapsed.
    """
    consumer = AIOConsumer(_reader_conf(target), max_workers=_READER_MAX_WORKERS)
    try:
        tp = TopicPartition(target.topic, partition)
        try:
            low, high = await consumer.get_watermark_offsets(tp, timeout=METADATA_TIMEOUT_SECONDS)
        except KafkaException as exc:
            raise _watermarks_or_raise(exc) from exc
        if not (low <= offset < high):
            raise KafkaReadNotFound(f"Offset {offset} is outside the partition's current range [{low}, {high})")

        await consumer.assign([TopicPartition(target.topic, partition, offset)])
        deadline = asyncio.get_running_loop().time() + FETCH_DEADLINE_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            msgs = await consumer.consume(num_messages=1, timeout=POLL_TIMEOUT_SECONDS)
            for msg in msgs:
                if msg.error():
                    raise KafkaReadUnavailable(f'Kafka read failed: {msg.error()}')
                if msg.offset() < offset:
                    continue  # should not happen with an exact assign; skip defensively
                if msg.offset() > offset:
                    # The slot exists inside the watermarks but the record is
                    # gone — log compaction or retention removed it.
                    raise KafkaReadNotFound(
                        f'Offset {offset} was compacted or deleted; the next surviving offset is {msg.offset()}'
                    )
                return _build_message(target.alias, msg)
        raise KafkaReadUnavailable(f'Timed out after {FETCH_DEADLINE_SECONDS:.0f}s waiting for the fetch')
    finally:
        await consumer.close()


async def stream_messages(
    target: AliasTarget,
    *,
    from_ts_ms: int,
    to_ts_ms: int | None = None,
    limit: int | None = None,
    partition: int | None = None,
) -> AsyncGenerator[KafkaReadMessage]:
    """Yield messages with ``timestamp >= from_ts_ms``, oldest-first per partition.

    Reads every partition of the alias's topic (or just ``partition`` when
    given), starting each at the first offset whose timestamp is at or past
    ``from_ts_ms`` (broker-side ``offsets_for_times``). The stream ends when
    every partition reaches the high watermark snapshotted at request time,
    when a partition's messages pass ``to_ts_ms``, or after ``limit``
    messages (capped at ``STREAM_LIMIT_MAX``) — whichever comes first.

    Ordering follows Kafka's contract: monotonic within a partition, best
    effort across partitions (records interleave in fetch order).

    Raises:
        KafkaReadNotFound: unknown topic/partition for this alias.
        KafkaReadUnavailable: brokers unreachable while resolving offsets.
    """
    remaining = min(limit or STREAM_LIMIT_MAX, STREAM_LIMIT_MAX)
    consumer = AIOConsumer(_reader_conf(target), max_workers=_READER_MAX_WORKERS)
    try:
        try:
            metadata = await consumer.list_topics(topic=target.topic, timeout=METADATA_TIMEOUT_SECONDS)
        except KafkaException as exc:
            raise _watermarks_or_raise(exc) from exc
        topic_meta = metadata.topics.get(target.topic)
        if topic_meta is None or topic_meta.error is not None:
            raise KafkaReadNotFound('Unknown topic for this alias')
        partition_ids = sorted(topic_meta.partitions)
        if partition is not None:
            if partition not in topic_meta.partitions:
                raise KafkaReadNotFound(f'Partition {partition} does not exist for this alias')
            partition_ids = [partition]

        # Broker-side timestamp -> offset resolution, then a high-watermark
        # snapshot per partition. The snapshot is the finish line: messages
        # produced after the request started are not part of this window.
        try:
            starts = await consumer.offsets_for_times(
                [TopicPartition(target.topic, pid, from_ts_ms) for pid in partition_ids],
                timeout=METADATA_TIMEOUT_SECONDS,
            )
            highs: dict[int, int] = {}
            for pid in partition_ids:
                _low, high = await consumer.get_watermark_offsets(
                    TopicPartition(target.topic, pid), timeout=METADATA_TIMEOUT_SECONDS
                )
                highs[pid] = high
        except KafkaException as exc:
            raise _watermarks_or_raise(exc) from exc

        # offsets_for_times returns offset -1 (OFFSET_END) when every record
        # in the partition is older than from_ts_ms — nothing to read there.
        assignments = [tp for tp in starts if tp.offset >= 0 and tp.offset < highs[tp.partition]]
        if not assignments or remaining <= 0:
            return
        await consumer.assign(assignments)

        active = {tp.partition for tp in assignments}
        loop = asyncio.get_running_loop()
        stream_deadline = loop.time() + STREAM_DEADLINE_SECONDS
        while active and remaining > 0:
            if loop.time() >= stream_deadline:
                logger.warning(
                    'kafka_read_stream_deadline',
                    category='debug',
                    topic=target.topic,
                    partitions=sorted(active),
                    remaining=remaining,
                    deadline_seconds=STREAM_DEADLINE_SECONDS,
                    hint='window did not close in time (compacted or aged-out offsets?); returning a partial stream',
                )
                return
            msgs = await consumer.consume(num_messages=min(remaining, 500), timeout=POLL_TIMEOUT_SECONDS)
            for msg in msgs:
                if msg.error():
                    raise KafkaReadUnavailable(f'Kafka read failed: {msg.error()}')
                pid = msg.partition()
                if pid not in active:
                    continue
                # Past the snapshot: this partition's window is done. The
                # message itself is newer than the request and not emitted.
                if msg.offset() >= highs[pid]:
                    active.discard(pid)
                    continue
                built = _build_message(target.alias, msg)
                if to_ts_ms is not None and built.timestamp_ms is not None and built.timestamp_ms > to_ts_ms:
                    active.discard(pid)
                    continue
                yield built
                remaining -= 1
                if msg.offset() + 1 >= highs[pid]:
                    active.discard(pid)
                if remaining <= 0:
                    break
    finally:
        await consumer.close()
