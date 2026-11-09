"""DLQ replay tool — reads DLQ records and republishes the original messages to a target topic.

A DLQ record is the failed source message itself: its key, its value and its
own headers, followed by ``dk-`` headers that say where and why it failed
(``drakkar/sinks/dlq_record.py``). The tool reads the records from the DLQ
topic the worker writes and republishes each one as an ordinary message: the
same key, the same value, and the source headers without any ``dk-`` header.
Intended as a reference operator tool after the cause of the failures has been
fixed and the dead-lettered messages should be driven through the pipeline
again.

A record without a ``dk-record-version`` header was written before the DLQ
record existed (a JSON document of payloads); it is never replayed and is
counted as ``skipped_old_format`` in the summary line.

Usage:
    uv run python scripts/replay_dlq.py \\
        --dlq-config=config.yaml \\
        --target-topic=search-requests \\
        --limit=1000 \\
        --stage=sink \\
        --filter='customer_id:123' \\
        --dry-run

Args:
    --dlq-config: path to the YAML config the worker used. The DLQ topic
        comes from ``dlq.topic`` (auto-derived ``{sources.kafka.topic}_dlq`` when
        empty) and the brokers from the connection ``dlq.connection`` names,
        identical to the runtime framework behavior.
    --target-topic: Kafka topic the records are republished to. Required
        unless --dry-run.
    --target-brokers: override the brokers used for producing. Defaults to
        the brokers of ``dlq.connection`` (same as the reader).
    --dry-run: iterate and log but never produce — useful for estimating
        volume and confirming filters before committing.
    --limit: stop after N records have been READ (not published — a
        filter-rejected record still counts toward the limit, matching the
        semantics of ``head -n N``).
    --filter: substring match on each record value, read as UTF-8 text
        (bytes that are not UTF-8 become U+FFFD). Empty disables it.
    --stage: replay only the records of one failing stage (``parse``,
        ``execute`` or ``sink``), read from the ``dk-stage`` header.

Exit codes:
    0 — drained successfully or --limit reached without errors.
    1 — a publish failed; stderr names the DLQ partition and offset of the
        failed record and of the last one published, so the operator can resume.
    2 — unrecoverable setup error (missing config, can't reach brokers, etc.).
    130 — Ctrl+C after flushing the producer (standard SIGINT convention).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import signal
import sys
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import structlog
from confluent_kafka import Producer

from drakkar.config import load_config
from drakkar.kafka_security import KafkaSecurityConfig, merge_client_config
from drakkar.sinks.dlq import DEFAULT_FLUSH_TIMEOUT_SECONDS, DLQEntry, read_dlq_entries
from drakkar.sinks.dlq_record import Header
from drakkar.sinks.kafka_delivery import (
    DeliveryReport,
    delivery_callback,
    delivery_failure,
    produce_and_flush,
    settle_budget,
)

logger = structlog.get_logger()

PROGRESS_EVERY = 1000
DLQ_HEADER_PREFIX = 'dk-'
STAGE_HEADER = 'dk-stage'
RECORD_VERSION_HEADER = 'dk-record-version'
STAGES = ('parse', 'execute', 'sink')
REPLAY_THREAD_NAME_PREFIX = 'replay'


@dataclass
class ReplayStats:
    """Counters maintained during a replay run, reported at the end.

    ``published`` tracks the number of messages ACTUALLY produced to the
    target topic. ``would_publish`` tracks the same count for ``--dry-run``
    so operators can estimate volume without the summary line making it
    look like real work happened. ``skipped_old_format`` counts records
    without a ``dk-record-version`` header (written before the DLQ record
    existed), which are never replayed. ``last_successful`` is the DLQ
    ``(partition, offset)`` of the last record published.
    """

    read: int = 0
    published: int = 0
    would_publish: int = 0
    filtered_out: int = 0
    skipped_old_format: int = 0
    last_successful: tuple[int, int] | None = None
    errors: list[str] = field(default_factory=list)


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse CLI surface.

    Kept in its own function so tests can introspect / reuse it without
    invoking the full ``main`` coroutine.
    """
    parser = argparse.ArgumentParser(
        prog='replay_dlq',
        description='Replay the original messages of a Drakkar DLQ topic to a target Kafka topic.',
    )
    parser.add_argument(
        '--dlq-config',
        required=True,
        help='Path to the YAML config the worker used (DLQ topic + brokers are read from it).',
    )
    parser.add_argument(
        '--target-topic',
        default=None,
        help='Kafka topic to republish to. Required unless --dry-run.',
    )
    parser.add_argument(
        '--target-brokers',
        default=None,
        help='Override brokers for the producer. Defaults to the brokers of dlq.connection.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Read and log, do not publish.',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Stop after reading N records (filter-rejected records count toward the limit).',
    )
    parser.add_argument(
        '--filter',
        default='',
        help='Substring match on each record value, read as UTF-8 text (empty disables filtering).',
    )
    parser.add_argument(
        '--stage',
        choices=STAGES,
        default=None,
        help='Replay only the records of this failing stage (the dk-stage header).',
    )
    return parser


def source_headers(entry: DLQEntry) -> list[Header]:
    """The record's own source headers: every header whose name does not start with ``dk-``, in order."""
    return [(name, value) for name, value in entry.headers if not name.startswith(DLQ_HEADER_PREFIX)]


def _is_dlq_record(entry: DLQEntry) -> bool:
    """True when the entry is a DLQ record: it carries a ``dk-record-version`` header.

    An entry without one was written before the DLQ record existed (a JSON
    document of payloads, not a source message), so republishing it would
    put that document into the target topic as a message.
    """
    return any(name == RECORD_VERSION_HEADER for name, _value in entry.headers)


def _stage_of(entry: DLQEntry) -> str | None:
    """The ``dk-stage`` header value of a record, or None when it has none."""
    for name, value in entry.headers:
        if name == STAGE_HEADER and value is not None:
            return value.decode('utf-8', errors='replace')
    return None


def _entry_matches_filter(entry: DLQEntry, pattern: str) -> bool:
    """Return True if the record value, read as UTF-8 text, contains ``pattern``.

    Empty ``pattern`` matches every record (the default). The check is a plain
    substring test — no regex, no JSON-path syntax — to keep the script
    dependency-free and predictable. A value that is not UTF-8 is still read:
    its bad bytes become U+FFFD and the rest of the text can match.
    """
    if not pattern:
        return True
    return pattern in entry.value.decode('utf-8', errors='replace')


def _entry_selected(entry: DLQEntry, *, pattern: str, stage: str | None) -> bool:
    """True when the record passes the stage filter and the text filter."""
    if stage is not None and _stage_of(entry) != stage:
        return False
    return _entry_matches_filter(entry, pattern)


@dataclass(frozen=True)
class DlqCoordinates:
    """Where the DLQ lives and how to reach it: the topic and the settings of its connection."""

    topic: str
    brokers: str
    security: KafkaSecurityConfig
    client_config: Mapping[str, str]


def _resolve_dlq_coordinates(config_path: str) -> DlqCoordinates:
    """Resolve the DLQ topic and its connection from a Drakkar config file.

    Applies the same fallback rules the runtime framework uses:
      - topic: ``dlq.topic`` when set, else ``{sources.kafka.topic}_dlq``
      - brokers and security: those of the connection ``dlq.connection`` names
      - client config: that connection's, then ``dlq.client_config`` on top
    """
    cfg = load_config(config_path)
    connection = cfg.dlq_connection
    if connection is None:
        raise ValueError(
            'cannot derive DLQ topic: dlq.enabled is off, or dlq.topic is empty and the Kafka source is disabled'
        )
    if not connection.brokers:
        raise ValueError(f'cannot derive DLQ brokers: connections.kafka.{cfg.dlq.connection}.brokers is empty')
    return DlqCoordinates(
        topic=cfg.resolved_dlq_topic,
        brokers=connection.brokers,
        security=connection.security,
        client_config=connection.client_config_with(cfg.dlq.client_config),
    )


class ReplayPublisher:
    """Writes records to the target topic with a sync producer on a thread of its own.

    The asyncio producer of the Kafka client cannot send headers, so the
    replay uses the plain ``Producer``. Its ``produce`` and ``flush`` block,
    so they run on one thread of the publisher's own, never on the event
    loop; one worker also keeps the writes in order. Each record is flushed
    and judged by its own delivery report.
    """

    def __init__(self, producer: Producer, *, flush_timeout_seconds: float) -> None:
        self._producer = producer
        self._flush_timeout_seconds = flush_timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=REPLAY_THREAD_NAME_PREFIX)

    async def publish(self, topic: str, entry: DLQEntry) -> None:
        """Republish one record's key, value and source headers; raise when the broker does not confirm it."""
        loop = asyncio.get_running_loop()
        report: asyncio.Future[DeliveryReport] = loop.create_future()
        deadline = time.monotonic() + self._flush_timeout_seconds
        # The producer-wide count of queued messages that the flush returns
        # says nothing about this record; its own delivery report decides.
        await loop.run_in_executor(
            self._executor,
            functools.partial(
                produce_and_flush,
                producer=self._producer,
                topic=topic,
                key=entry.key,
                value=entry.value,
                headers=source_headers(entry),
                on_delivery=delivery_callback(loop, report),
                flush_timeout_seconds=self._flush_timeout_seconds,
            ),
        )
        _settled, pending = await asyncio.wait([report], timeout=settle_budget(deadline))
        if pending:
            raise RuntimeError(f'Kafka delivery not confirmed within {self._flush_timeout_seconds}s.')
        failure = delivery_failure(*report.result())
        if failure is not None:
            raise RuntimeError(f'Kafka delivery error: {failure}')

    async def close(self) -> None:
        """Flush what is still queued on the publisher's thread, then shut the thread down."""
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._executor, self._producer.flush, self._flush_timeout_seconds
            )
        finally:
            self._executor.shutdown(wait=True)


def _coordinates(partition: int, offset: int) -> str:
    """The DLQ position of a record as the report lines print it."""
    return f'dlq_partition={partition} dlq_offset={offset}'


async def run(
    *,
    dlq_topic: str,
    dlq_brokers: str,
    target_topic: str | None,
    target_brokers: str,
    limit: int | None,
    filter_pattern: str,
    stage: str | None,
    dry_run: bool,
    stats: ReplayStats | None = None,
    producer: Producer | None = None,
    flush_timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    dlq_security: KafkaSecurityConfig | None = None,
    dlq_client_config: Mapping[str, str] | None = None,
    target_uses_dlq_connection: bool = True,
) -> ReplayStats:
    """Drive the actual read-and-republish loop.

    Kept separate from ``main`` so tests can feed a pre-built async
    iterator and a fake producer without going through argparse / config
    loading. Tests inject ``producer``; in the real CLI path it is built
    here from ``target_brokers``.

    ``dlq_security`` and ``dlq_client_config`` are the settings of the DLQ's
    connection. They go to the reader, and to the producer too while
    ``target_uses_dlq_connection`` is true (the target is the DLQ's own
    cluster): credentials of one cluster are not sent to another.
    """
    stats = stats or ReplayStats()
    stop_requested = False

    def _handle_sigint() -> None:
        nonlocal stop_requested
        stop_requested = True
        print('Received SIGINT — finishing in-flight produces and exiting.', file=sys.stderr)

    # Register a signal handler on the running loop so Ctrl+C is graceful:
    # we let the current iteration finish, then break out.
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _handle_sigint)
    except (NotImplementedError, ValueError):
        # Not all environments support signal handlers on the loop (e.g. on
        # Windows or under some test runners). The KeyboardInterrupt path in
        # ``main`` still catches the raw signal.
        pass

    publisher: ReplayPublisher | None = None
    publish_topic = ''
    if not dry_run:
        if not target_topic:
            raise ValueError('--target-topic is required unless --dry-run is set.')
        publish_topic = target_topic
        if producer is None:
            producer = Producer(
                merge_client_config(
                    {'bootstrap.servers': target_brokers},
                    (dlq_security if target_uses_dlq_connection else None) or KafkaSecurityConfig(),
                    dlq_client_config if target_uses_dlq_connection else None,
                )
            )
        publisher = ReplayPublisher(producer, flush_timeout_seconds=flush_timeout_seconds)

    try:
        async for entry in read_dlq_entries(
            topic=dlq_topic,
            brokers=dlq_brokers,
            limit=limit,
            security=dlq_security,
            client_config=dlq_client_config,
        ):
            if stop_requested:
                break
            stats.read += 1
            if not _is_dlq_record(entry):
                stats.skipped_old_format += 1
                if stats.read % PROGRESS_EVERY == 0:
                    _emit_progress(stats)
                continue
            if not _entry_selected(entry, pattern=filter_pattern, stage=stage):
                stats.filtered_out += 1
                if stats.read % PROGRESS_EVERY == 0:
                    _emit_progress(stats)
                continue

            if publisher is None:
                # Dry-run: track the would-be count separately so the summary
                # line shows "this many messages would have been published"
                # without lying about ``published`` (actually written).
                stats.would_publish += 1
                stats.last_successful = (entry.partition, entry.offset)
            else:
                try:
                    await publisher.publish(publish_topic, entry)
                except Exception as exc:
                    err = f'publish failed at {_coordinates(entry.partition, entry.offset)}: {exc}'
                    stats.errors.append(err)
                    print(err, file=sys.stderr)
                    if stats.last_successful is not None:
                        print(
                            f'last successful {_coordinates(*stats.last_successful)}; resume by skipping past it.',
                            file=sys.stderr,
                        )
                    raise
                stats.published += 1
                stats.last_successful = (entry.partition, entry.offset)

            # Progress emission ONLY on the success path — don't print a
            # confusing "replayed N/M" line immediately before the exception
            # propagates out of the loop.
            if stats.read % PROGRESS_EVERY == 0:
                _emit_progress(stats)
    finally:
        if publisher is not None:
            try:
                await publisher.close()
            except Exception as e:
                print(f'warning: producer close failed: {e}', file=sys.stderr)

    return stats


def _emit_progress(stats: ReplayStats) -> None:
    """Print a one-line progress update to stderr.

    Format: ``replayed N/M`` (or ``would-replay N/M`` in dry-run) where N
    is the count delivered/to-be-delivered and M is total read so far.
    Matches the ``tqdm``-ish conventions operators expect while staying
    dependency-free.
    """
    # In dry-run mode nothing gets published, but ``would_publish`` is
    # incremented so operators still see progress. Otherwise fall back to
    # the real ``published`` counter (zero is fine for early-iteration
    # progress before any record has been delivered).
    if stats.would_publish and not stats.published:
        print(f'would-replay {stats.would_publish}/{stats.read}', file=sys.stderr)
    else:
        print(f'replayed {stats.published}/{stats.read}', file=sys.stderr)


def _emit_summary(stats: ReplayStats) -> None:
    """Print the final run summary to stderr.

    Kept separate from progress emission so the shape is unambiguous when
    the script is piped into another tool.
    """
    print(
        f'summary: read={stats.read} published={stats.published} '
        f'would_publish={stats.would_publish} '
        f'filtered_out={stats.filtered_out} '
        f'skipped_old_format={stats.skipped_old_format} '
        f'errors={len(stats.errors)}',
        file=sys.stderr,
    )


async def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns the process exit code; the caller is responsible for passing it
    to ``sys.exit`` when executed as ``__main__``.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        dlq = _resolve_dlq_coordinates(args.dlq_config)
    except (FileNotFoundError, ValueError) as exc:
        print(f'config error: {exc}', file=sys.stderr)
        return 2

    target_brokers = args.target_brokers or dlq.brokers

    if not args.dry_run and not args.target_topic:
        print('--target-topic is required unless --dry-run is set.', file=sys.stderr)
        return 2

    print(
        f'reading dlq topic={dlq.topic!r} brokers={dlq.brokers!r} '
        f'target={args.target_topic!r} dry_run={args.dry_run} '
        f'limit={args.limit} stage={args.stage!r} filter={args.filter!r}',
        file=sys.stderr,
    )

    stats = ReplayStats()
    try:
        await run(
            dlq_topic=dlq.topic,
            dlq_brokers=dlq.brokers,
            dlq_security=dlq.security,
            dlq_client_config=dlq.client_config,
            target_uses_dlq_connection=target_brokers == dlq.brokers,
            target_topic=args.target_topic,
            target_brokers=target_brokers,
            limit=args.limit,
            filter_pattern=args.filter,
            stage=args.stage,
            dry_run=args.dry_run,
            stats=stats,
        )
    except KeyboardInterrupt:
        _emit_summary(stats)
        return 130
    except Exception as exc:
        print(f'aborted: {exc}', file=sys.stderr)
        _emit_summary(stats)
        return 1

    _emit_summary(stats)
    return 0


if __name__ == '__main__':  # pragma: no cover - trivial dispatch
    raise SystemExit(asyncio.run(main()))
