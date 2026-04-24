"""DLQ replay tool — reads dead-lettered messages and republishes to a target topic.

Reads entries from the configured DLQ Kafka topic (the same topic the worker
writes via ``DLQSink``), then republishes each entry's preserved
``original_payloads`` to a target Kafka topic. Intended as a reference /
operator tool after a bug in the downstream sink has been fixed and a batch
of previously dead-lettered messages should be re-driven through the pipeline.

Usage:
    uv run python scripts/replay_dlq.py \\
        --dlq-config=config.yaml \\
        --target-topic=search-results \\
        --limit=1000 \\
        --filter='customer_id:123' \\
        --dry-run

Args:
    --dlq-config: path to the YAML config the worker used. The DLQ topic +
        brokers are taken from ``dlq.topic`` / ``dlq.brokers`` (with fallback
        to ``kafka.brokers`` and auto-derived topic ``{source_topic}_dlq``,
        identical to the runtime framework behavior).
    --target-topic: Kafka topic the entries are republished to. When omitted,
        the script uses the sink_name embedded in the DLQ entry as a best-effort
        hint; operators are strongly encouraged to pass it explicitly.
    --target-brokers: override the brokers used for producing. Defaults to
        ``dlq.brokers`` → ``kafka.brokers`` (same as the reader).
    --dry-run: iterate and log but never call ``producer.produce`` — useful
        for estimating volume and confirming filters before committing.
    --limit: stop after N records have been READ (not published — a
        filter-rejected entry still counts toward the limit, matching the
        semantics of ``head -n N``).
    --filter: simple substring match applied to each payload string. An entry
        whose ``original_payloads`` contain the substring (anywhere) is
        published; non-matching entries are skipped. Empty string disables
        filtering.

Exit codes:
    0 — drained successfully or --limit reached without errors.
    1 — producer.produce raised after having already published some records;
        stderr carries the last successful offset so the operator can resume
        with an updated config / filter.
    2 — unrecoverable setup error (missing config, can't reach brokers, etc.).
    130 — Ctrl+C after flushing the producer (standard SIGINT convention).

Notes:
    - This script uses ``confluent_kafka.aio.AIOProducer`` (matching the rest
      of the project). The plan originally referenced ``aiokafka`` — the
      codebase standardized on ``confluent_kafka`` so we use it here too.
    - The DLQ entry format is defined by ``DLQMessage.serialize()`` in
      ``drakkar/sinks/dlq.py``. ``original_payloads`` is a JSON list of strings,
      each string a ``model_dump_json()`` of the original failed payload —
      we republish those strings verbatim as the target-topic message value.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from dataclasses import dataclass, field

import structlog
from confluent_kafka.aio import AIOProducer

from drakkar.config import load_config
from drakkar.sinks.dlq import read_dlq_entries

logger = structlog.get_logger()

PROGRESS_EVERY = 1000


@dataclass
class ReplayStats:
    """Counters maintained during a replay run, reported at the end.

    ``published`` tracks the number of messages ACTUALLY produced to the
    target topic. ``would_publish`` tracks the same count for ``--dry-run``
    so operators can estimate volume without the summary line making it
    look like real work happened.
    """

    read: int = 0
    published: int = 0
    would_publish: int = 0
    filtered_out: int = 0
    skipped_invalid: int = 0
    last_successful_offset: int | None = None
    errors: list[str] = field(default_factory=list)


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse CLI surface.

    Kept in its own function so tests can introspect / reuse it without
    invoking the full ``main`` coroutine.
    """
    parser = argparse.ArgumentParser(
        prog='replay_dlq',
        description='Replay messages from a Drakkar DLQ topic to a target Kafka topic.',
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
        help='Override brokers for the producer. Defaults to dlq.brokers or kafka.brokers.',
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
        help='Stop after reading N entries (filter-rejected entries count toward the limit).',
    )
    parser.add_argument(
        '--filter',
        default='',
        help='Substring match on each payload string (empty disables filtering).',
    )
    return parser


def _entry_matches_filter(entry: dict, pattern: str) -> bool:
    """Return True if any string payload in ``entry['original_payloads']`` contains ``pattern``.

    Empty ``pattern`` matches every entry (the default). The check is a plain
    ``in`` substring test — no regex, no JSON-path syntax — to keep the
    script dependency-free and predictable. Operators who want structural
    filtering can fork the script and substitute ``jq`` / ``jmespath``.
    """
    if not pattern:
        return True
    for raw in entry.get('original_payloads', []) or []:
        if isinstance(raw, str) and pattern in raw:
            return True
        if isinstance(raw, bytes) and pattern.encode() in raw:
            return True
    return False


def _extract_payload_values(entry: dict) -> list[bytes]:
    """Pull each preserved payload out of a DLQ entry as bytes for produce().

    ``original_payloads`` holds JSON-encoded strings (produced by
    ``BaseModel.model_dump_json()``); each one is an independent payload
    that was meant for the sink. We emit one Kafka message per payload so a
    multi-payload DLQ entry (e.g. a batched sink that failed atomically)
    is unpacked to individual target-topic records.
    """
    values: list[bytes] = []
    for raw in entry.get('original_payloads', []) or []:
        if isinstance(raw, str):
            values.append(raw.encode())
        elif isinstance(raw, bytes):
            values.append(raw)
        else:
            # Defensive — older DLQ payloads may have non-string fallbacks
            # (see the ``except Exception`` branch in ``DLQMessage.serialize``).
            values.append(json.dumps(raw).encode())
    return values


def _resolve_dlq_coordinates(config_path: str) -> tuple[str, str]:
    """Resolve (topic, brokers) for the DLQ reader from a Drakkar config file.

    Applies the same fallback rules the runtime framework uses:
      - topic: ``dlq.topic`` when set, else ``{kafka.source_topic}_dlq``
      - brokers: ``dlq.brokers`` when set, else ``kafka.brokers``
    """
    cfg = load_config(config_path)
    topic = cfg.dlq.topic or f'{cfg.kafka.source_topic}_dlq'
    brokers = cfg.dlq.brokers or cfg.kafka.brokers
    if not topic:
        raise ValueError('Cannot derive DLQ topic: dlq.topic is empty and kafka.source_topic is unset.')
    if not brokers:
        raise ValueError('Cannot derive DLQ brokers: dlq.brokers and kafka.brokers are both empty.')
    return topic, brokers


async def _maybe_publish(
    producer: AIOProducer | None,
    target_topic: str,
    values: list[bytes],
) -> None:
    """Publish each value to ``target_topic`` and await its delivery future.

    ``producer`` is ``None`` only when running under ``--dry-run`` — the
    caller still invokes this helper so the iteration shape (and stats)
    matches between modes, but we short-circuit before touching the network.
    """
    if producer is None:
        return
    futures = []
    for value in values:
        fut = await producer.produce(topic=target_topic, value=value)
        futures.append(fut)
    # ``flush()`` nudges the internal queue, matching ``KafkaSink.deliver``'s
    # pattern. A small batch + flush gives us prompt feedback on broker errors.
    remaining = await producer.flush()
    if remaining and remaining > 0:
        raise RuntimeError(f'Kafka flush incomplete: {remaining} message(s) still queued.')
    for i, fut in enumerate(futures):
        result = await fut
        if result is None:
            raise RuntimeError(f'Kafka delivery future resolved to None for message {i}.')
        if hasattr(result, 'error') and result.error() is not None:
            raise RuntimeError(f'Kafka delivery error for message {i}: {result.error()}')


async def run(
    *,
    dlq_topic: str,
    dlq_brokers: str,
    target_topic: str | None,
    target_brokers: str,
    limit: int | None,
    filter_pattern: str,
    dry_run: bool,
    stats: ReplayStats | None = None,
    producer: AIOProducer | None = None,
) -> ReplayStats:
    """Drive the actual read-and-republish loop.

    Kept separate from ``main`` so tests can feed a pre-built async
    iterator and a mock producer without going through argparse / config
    loading. The ``producer`` parameter is injected by tests; in the real
    CLI path ``main`` constructs one and passes it in.
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

    owns_producer = False
    if producer is None and not dry_run:
        if not target_topic:
            raise ValueError('--target-topic is required unless --dry-run is set.')
        producer = AIOProducer({'bootstrap.servers': target_brokers})
        owns_producer = True

    try:
        async for entry in read_dlq_entries(topic=dlq_topic, brokers=dlq_brokers, limit=limit):
            if stop_requested:
                break
            stats.read += 1
            if not _entry_matches_filter(entry, filter_pattern):
                stats.filtered_out += 1
                if stats.read % PROGRESS_EVERY == 0:
                    _emit_progress(stats)
                continue

            values = _extract_payload_values(entry)
            if not values:
                stats.skipped_invalid += 1
                if stats.read % PROGRESS_EVERY == 0:
                    _emit_progress(stats)
                continue

            try:
                if not dry_run:
                    await _maybe_publish(producer, target_topic or '', values)
                    stats.published += len(values)
                    stats.last_successful_offset = entry.get('_kafka_offset')
                else:
                    # Dry-run: track the would-be count separately so the
                    # summary line can show operators "this many messages
                    # would have been published" without lying about the
                    # ``published`` counter (which means "actually written to
                    # the target topic").
                    stats.would_publish += len(values)
                    stats.last_successful_offset = entry.get('_kafka_offset')
            except Exception as exc:
                err = f'publish failed at dlq_offset={entry.get("_kafka_offset")!r}: {exc}'
                stats.errors.append(err)
                print(err, file=sys.stderr)
                if stats.last_successful_offset is not None:
                    print(
                        f'last successful dlq_offset={stats.last_successful_offset}; resume by skipping past it.',
                        file=sys.stderr,
                    )
                raise

            # Progress emission ONLY on the success path — don't print a
            # confusing "replayed N/M" line immediately before the exception
            # propagates out of the loop.
            if stats.read % PROGRESS_EVERY == 0:
                _emit_progress(stats)
    finally:
        if owns_producer and producer is not None:
            try:
                await producer.close()
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
    # progress before any batches have been delivered).
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
        f'filtered_out={stats.filtered_out} skipped_invalid={stats.skipped_invalid} '
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
        dlq_topic, dlq_brokers = _resolve_dlq_coordinates(args.dlq_config)
    except (FileNotFoundError, ValueError) as exc:
        print(f'config error: {exc}', file=sys.stderr)
        return 2

    target_brokers = args.target_brokers or dlq_brokers

    if not args.dry_run and not args.target_topic:
        print('--target-topic is required unless --dry-run is set.', file=sys.stderr)
        return 2

    print(
        f'reading dlq topic={dlq_topic!r} brokers={dlq_brokers!r} '
        f'target={args.target_topic!r} dry_run={args.dry_run} '
        f'limit={args.limit} filter={args.filter!r}',
        file=sys.stderr,
    )

    stats = ReplayStats()
    try:
        await run(
            dlq_topic=dlq_topic,
            dlq_brokers=dlq_brokers,
            target_topic=args.target_topic,
            target_brokers=target_brokers,
            limit=args.limit,
            filter_pattern=args.filter,
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
