"""Checks of scenario-split.sh over what the Kafka console tools print.

Standard library only: the scenario runs it with the host's python3.

Subcommands:

  records      Parse the output of kafka-console-consumer.sh (fields split
               by control characters) and print the records with one key as
               JSON lines. Proves that the read saw every record that the
               topic had when the read began (the offsets of
               kafka-get-offsets.sh), so a tool that failed or stopped early
               is never read as "no record".
  summaries    Check the summary records of one request.
  items        Check the item records of one request.
  partial-worker
               A worker that wrote some, not all, item records of a part.
  results      Print the records of one kind from the results log.
  commit       Find, in the output of the offsets formatter over
               __consumer_offsets, the first commit of one group and
               partition past an offset.
  offsets-partition
               The partition of __consumer_offsets that holds a group.
  iso-ms       Epoch milliseconds of an ISO timestamp of a log line.

Exit codes: 0 the rule holds, 1 the rule is broken (the reason on stderr),
3 the read did not cover the topic (read again), 4 nothing found yet.
"""

import argparse
import collections
import datetime
import json
import re
import sys

EXIT_BROKEN = 1
EXIT_PARTIAL_READ = 3
EXIT_NOT_FOUND = 4
FIELD_SEPARATOR = '\x1f'
RECORD_SEPARATOR = '\x1e'
HEADER_SEPARATOR = '\x1d'
RECORD_FIELDS = 6
COMMIT_LINE = re.compile(
    r'^\[(?P<group>[^,\]]*),(?P<topic>[^,\]]*),(?P<partition>\d+)\]::'
    r'OffsetAndMetadata\(offset=(?P<offset>\d+),.*commitTimestamp=(?P<ts>\d+)'
)


def broken(reason: str) -> None:
    """End with the reason of a broken rule."""
    print(reason, file=sys.stderr)
    sys.exit(EXIT_BROKEN)


def parse_offsets(text: str) -> dict[int, int]:
    """Partition to offset, from the lines `<topic>:<partition>:<offset>` of kafka-get-offsets.sh."""
    offsets: dict[int, int] = {}
    for line in text.split():
        parts = line.rsplit(':', 2)
        if len(parts) != 3:
            broken(f'cannot read the offset line {line!r} of kafka-get-offsets.sh')
        offsets[int(parts[1])] = int(parts[2])
    return offsets


def parse_record(raw: str) -> dict:
    """One record of the console consumer: timestamp, partition, offset, headers, key, value."""
    fields = raw.split(FIELD_SEPARATOR)
    if len(fields) != RECORD_FIELDS:
        broken(f'a record of the console consumer has {len(fields)} fields, expected {RECORD_FIELDS}: {raw[:200]!r}')
    stamp, partition, offset, headers, key, value = fields
    # The tool writes <name>:<value>; a header name has no colon.
    pairs = [] if headers == 'NO_HEADERS' else [h.split(':', 1) for h in headers.split(HEADER_SEPARATOR)]
    return {
        'ts': int(stamp.split(':', 1)[1]),
        'partition': int(partition.split(':', 1)[1]),
        'offset': int(offset.split(':', 1)[1]),
        'headers': pairs,
        'key': key,
        'value': value,
    }


def missing_records(records: list[dict], earliest: dict[int, int], ends: dict[int, int]) -> list[str]:
    """The partitions whose offsets from earliest to end were not all read."""
    seen: dict[int, set[int]] = collections.defaultdict(set)
    for record in records:
        seen[record['partition']].add(record['offset'])
    short = []
    for partition, end in sorted(ends.items()):
        wanted = set(range(earliest.get(partition, 0), end))
        lacking = wanted - seen[partition]
        if lacking:
            short.append(f'partition {partition}: {len(lacking)} of {len(wanted)} records not read')
    return short


def command_records(args: argparse.Namespace) -> None:
    """Print the records with the key; exit 3 when the read missed records the topic had."""
    raw = sys.stdin.read()
    records = [parse_record(piece.lstrip('\n')) for piece in raw.split(RECORD_SEPARATOR) if piece.strip()]
    short = missing_records(records, parse_offsets(args.earliest), parse_offsets(args.ends))
    if short:
        print(f'the read of the topic is short: {"; ".join(short)}', file=sys.stderr)
        sys.exit(EXIT_PARTIAL_READ)
    for record in records:
        if args.key is None or record['key'] == args.key:
            print(json.dumps(record))


def read_json_lines() -> list[dict]:
    """The JSON records on stdin, in their order."""
    return [json.loads(line) for line in sys.stdin if line.strip()]


def summary_line(rows: list[tuple[dict, dict]]) -> str:
    """One text for the summaries that passed."""
    return f'{len(rows)} summary(ies): ' + '; '.join(
        f'{row["job"]} by {row["worker_id"]}, {row["received_parts"]}/{row["parts"]} parts, {row["items"]} items'
        for _, row in rows
    )


def check_two_summaries(rows: list[tuple[dict, dict]], disrupted: set[str], takers: set[str]) -> None:
    """Two summaries: two workers each ended the job, the first one was disrupted, the second took the job over."""
    (_, first), (_, second) = rows
    if first['worker_id'] == second['worker_id']:
        broken(
            f'two summaries by the same worker {first["worker_id"]}: the job ended twice on one worker: {[row for _, row in rows]}'
        )
    if disrupted and first['worker_id'] not in disrupted:
        broken(
            f'two summaries, and the first one is by {first["worker_id"]}, '
            f'not by a worker this check stopped or moved ({" ".join(sorted(disrupted))})'
        )
    if second['worker_id'] not in takers:
        broken(
            f'two summaries, and the second one is by {second["worker_id"]}, '
            f'which did not log split_job_resumed or split_attempt_started for the job'
        )


def command_summaries(args: argparse.Namespace) -> None:
    """Check the count, the workers and the numbers of the summaries of one request."""
    rows = sorted(((record, json.loads(record['value'])) for record in read_json_lines()), key=lambda r: r[0]['ts'])
    if not 1 <= len(rows) <= args.max:
        broken(f'{len(rows)} summaries, expected 1 to {args.max}: {[row for _, row in rows]}')
    for _, row in rows:
        got = (row['parts'], row['received_parts'], row['failed_parts'], row['items'])
        if got != (args.parts, args.parts, 0, args.items):
            broken(f'summary {row} is not {args.parts} parts, {args.parts} received, 0 failed, {args.items} items')
    if len(rows) == 2:
        check_two_summaries(rows, set(args.disrupted.split()), set(args.takers.split()))
    print(summary_line(rows))


def victim_partial_parts(rows: list[dict], victim: str, part_size: int) -> list[int]:
    """The parts of which the victim wrote some but not all item records."""
    by_part: dict[int, set[str]] = collections.defaultdict(set)
    for row in rows:
        if row['worker_id'] == victim:
            by_part[int(row['item'].rsplit('-', 1)[1]) // part_size].add(row['item'])
    return sorted(part for part, items in by_part.items() if len(items) < part_size)


def command_items(args: argparse.Namespace) -> None:
    """Check that every item of the request has a record, and how many records there are."""
    rows = [json.loads(record['value']) for record in read_json_lines()]
    names = {row['item'] for row in rows}
    lacking = sorted(set(f'item-{n}' for n in range(args.count)) - names)
    if lacking:
        broken(f'{len(rows)} item records, and no record of {len(lacking)} items: {lacking[:10]}')
    if args.exact and len(rows) != args.count:
        broken(f'{len(rows)} item records, expected exactly {args.count}: items ran more than once without a fault')
    counts = collections.Counter(row['worker_id'] for row in rows)
    text = f'{len(rows)} item records, per worker: ' + ' '.join(f'{w}={n}' for w, n in sorted(counts.items()))
    if args.victim:
        partial = victim_partial_parts(rows, args.victim, args.part_size)
        if args.require_partial and not partial:
            broken(
                f'{text}; {args.victim} finished every part it started, so the kill hit no part of this request in progress'
            )
        text += f'; parts {args.victim} left unfinished: {partial}'
    print(text)


def command_partial_worker(args: argparse.Namespace) -> None:
    """Print a worker other than the given one that wrote some but not all item records of a part; exit 4 when none."""
    rows = [json.loads(record['value']) for record in read_json_lines()]
    for worker in sorted({row['worker_id'] for row in rows} - {args.skip}):
        partial = victim_partial_parts(rows, worker, args.part_size)
        if partial:
            print(worker, ','.join(str(part) for part in partial))
            return
    sys.exit(EXIT_NOT_FOUND)


def command_results(args: argparse.Namespace) -> None:
    """Print `<ts> <attempt> <attempt id>` of each record of one kind in the results log."""
    for record in read_json_lines():
        headers = dict(record['headers'])
        if headers.get('dk-split-record') == args.kind:
            print(record['ts'], headers.get('dk-split-attempt'), headers.get('dk-split-attempt-id'))


def command_commit(args: argparse.Namespace) -> None:
    """Print `<commit ms> <offset>` of the first commit past the offset, and the last one at or below it."""
    below = None
    for line in sys.stdin:
        match = COMMIT_LINE.match(line.strip())
        if not match or match['group'] != args.group or match['topic'] != args.topic:
            continue
        if int(match['partition']) != args.partition:
            continue
        offset, stamp = int(match['offset']), int(match['ts'])
        if offset > args.after:
            print(f'{stamp} {offset} {below[0] if below else "-"} {below[1] if below else "-"}')
            return
        below = (stamp, offset)
    sys.exit(EXIT_NOT_FOUND)


def java_string_hash(text: str) -> int:
    """String.hashCode of the JVM, over UTF-16 code units, as a signed 32-bit integer."""
    value = 0
    data = text.encode('utf-16-be')
    for index in range(0, len(data), 2):
        value = (31 * value + int.from_bytes(data[index : index + 2], 'big')) & 0xFFFFFFFF
    return value - (1 << 32) if value >= 1 << 31 else value


def command_offsets_partition(args: argparse.Namespace) -> None:
    """The partition of __consumer_offsets for a group: the broker's abs(hashCode) mod the partition count.

    The broker's abs maps the smallest 32-bit integer to 0.
    """
    value = java_string_hash(args.group)
    print(0 if value == -(1 << 31) else abs(value) % args.count)


def command_iso_ms(args: argparse.Namespace) -> None:
    """Epoch milliseconds of `2026-11-21T12:34:12.145279Z`."""
    stamp = datetime.datetime.fromisoformat(args.stamp.replace('Z', '+00:00'))
    print(int(stamp.timestamp() * 1000))


def build_parser() -> argparse.ArgumentParser:
    """The subcommands and their arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    records = commands.add_parser('records')
    records.add_argument('--key')
    records.add_argument('--earliest', required=True)
    records.add_argument('--ends', required=True)
    records.set_defaults(run=command_records)
    summaries = commands.add_parser('summaries')
    summaries.add_argument('--parts', type=int, required=True)
    summaries.add_argument('--items', type=int, required=True)
    summaries.add_argument('--max', type=int, default=1)
    summaries.add_argument('--disrupted', default='')
    summaries.add_argument('--takers', default='')
    summaries.set_defaults(run=command_summaries)
    items = commands.add_parser('items')
    items.add_argument('--count', type=int, required=True)
    items.add_argument('--exact', action='store_true')
    items.add_argument('--victim', default='')
    items.add_argument('--part-size', type=int, default=5)
    items.add_argument('--require-partial', action='store_true')
    items.set_defaults(run=command_items)
    partial_worker = commands.add_parser('partial-worker')
    partial_worker.add_argument('--skip', required=True)
    partial_worker.add_argument('--part-size', type=int, default=5)
    partial_worker.set_defaults(run=command_partial_worker)
    results = commands.add_parser('results')
    results.add_argument('--kind', required=True)
    results.set_defaults(run=command_results)
    commit = commands.add_parser('commit')
    commit.add_argument('--group', required=True)
    commit.add_argument('--topic', required=True)
    commit.add_argument('--partition', type=int, required=True)
    commit.add_argument('--after', type=int, required=True)
    commit.set_defaults(run=command_commit)
    offsets_partition = commands.add_parser('offsets-partition')
    offsets_partition.add_argument('--group', required=True)
    offsets_partition.add_argument('--count', type=int, required=True)
    offsets_partition.set_defaults(run=command_offsets_partition)
    iso_ms = commands.add_parser('iso-ms')
    iso_ms.add_argument('stamp')
    iso_ms.set_defaults(run=command_iso_ms)
    return parser


def main() -> None:
    """Run one subcommand."""
    args = build_parser().parse_args()
    args.run(args)


if __name__ == '__main__':
    main()
