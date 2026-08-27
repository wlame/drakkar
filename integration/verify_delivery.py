"""Check the harness's delivery guarantee against what the sinks actually hold.

The docker harness proves the workers boot. It did not prove the thing the
framework is FOR: that every message a producer sent reaches its sinks, that
nothing arrives which was never sent, and that payload order survives a
rebalance. Those were verified by an operator looking at a dashboard, which
means a regression that loses or reorders records passed CI.

This script closes that. It reconstructs the produced request-id set from the
message count (``producer.py`` numbers them deterministically), reads the
sinks, and fails with a non-zero exit when any of the checks below break.

    uv run python integration/verify_delivery.py --total-messages=200

**Run it after the pipeline has drained.** Consumer lag above zero means work
is still in flight, and a request that has not been processed yet is
indistinguishable here from one that was lost. ``--wait-seconds`` polls until
the row count stops moving before it starts checking.

What is checked, and which real defect each one catches:

* ``no_loss`` — every produced request has a summary row. This is
  at-least-once delivery: a missing row means the pipeline committed an
  offset past a message whose sinks never confirmed.
* ``no_phantom`` — no summary row outside the produced set. Catches a
  corrupted key path and a database left dirty by an earlier run.
* ``update_not_reordered`` — the handler appends ``UPSERT`` then
  ``UPDATE notified=true`` for every request over the notify threshold, and
  ``notified`` is deliberately absent from the upsert's ``update_columns``.
  So an ``UPDATE`` that ran BEFORE its ``UPSERT`` matches zero rows and
  leaves ``notified`` false forever. A high-match request with
  ``notified = false`` is therefore the exact on-disk signature of the
  Postgres sink reordering a payload past its predecessor.
* ``task_rows_present`` — a request whose summary says tasks succeeded must
  have the per-task rows those tasks wrote. Catches a summary committing
  while the detail writes behind it were dropped.
* ``duplication_bounded`` — at-least-once permits duplicates, so this is
  informational until the factor passes ``--max-duplication``, where it stops
  looking like redelivery and starts looking like a replay loop.

Everything above :func:`run_checks` is pure: no asyncpg, no I/O, no clock. The
unit suite exercises the checks against synthetic rows in
``tests/test_integration_harness.py``, so the logic that decides pass/fail is
covered without a container.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field

import asyncpg

# Kept byte-identical in integration/infra/producer.py, which numbers the
# requests it sends. A unit test pins the two together.
REQUEST_ID_FORMAT = 'req-{:06d}'

# The handler notifies (and appends the UPDATE) above this many matches:
# `if aggregate.total_matches > 20:` in integration/worker/handler.py. A unit
# test pins this constant against that line, because a threshold that drifted
# would silently turn `update_not_reordered` into a check of nothing.
NOTIFY_THRESHOLD = 20

DEFAULT_DSN = 'postgresql://drakkar:drakkar@localhost:5432/drakkar'

# How many offending ids an error names. Enough to debug with, few enough
# that a total failure does not print thousands of lines into a CI log.
SAMPLE_SIZE = 10


@dataclass(frozen=True)
class SummaryRow:
    """One ``request_summaries`` row — the per-request rollup."""

    request_id: str
    total_matches: int
    succeeded_tasks: int
    notified: bool


@dataclass(frozen=True)
class Violation:
    """One failed check, named so a CI log says what broke without a rerun."""

    check: str
    detail: str
    sample: tuple[str, ...] = ()

    def render(self) -> str:
        line = f'FAIL {self.check}: {self.detail}'
        if self.sample:
            line += f'\n     sample: {", ".join(self.sample)}'
        return line


@dataclass
class Report:
    """The outcome of one verification run."""

    produced: int
    summaries: int
    result_rows: int
    duplication: float
    violations: list[Violation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


def expected_request_ids(total_messages: int) -> set[str]:
    """The ids ``producer.py`` sends for a run of ``total_messages``.

    The producer numbers requests 1..N and the flood phase re-sends that
    same range, so the DISTINCT id set is exactly this regardless of how
    many times each was delivered.
    """
    return {REQUEST_ID_FORMAT.format(n) for n in range(1, total_messages + 1)}


def _sample(ids: set[str]) -> tuple[str, ...]:
    return tuple(sorted(ids)[:SAMPLE_SIZE])


def check_no_loss(expected: set[str], rows: list[SummaryRow]) -> Violation | None:
    """Every produced request must have reached the sink."""
    missing = expected - {row.request_id for row in rows}
    if not missing:
        return None
    return Violation(
        check='no_loss',
        detail=(
            f'{len(missing)} of {len(expected)} produced requests have no request_summaries row. '
            f'Offsets were committed past messages whose sinks never confirmed, or the pipeline '
            f'had not drained when this ran.'
        ),
        sample=_sample(missing),
    )


def check_no_phantom(expected: set[str], rows: list[SummaryRow]) -> Violation | None:
    """Nothing may arrive that was never sent."""
    phantom = {row.request_id for row in rows} - expected
    if not phantom:
        return None
    return Violation(
        check='no_phantom',
        detail=(
            f'{len(phantom)} request_summaries rows are not in the produced id set. '
            f'Either the key path corrupted an id, or the database still holds rows from an '
            f'earlier run — tear the harness down with `just integration-down` before verifying.'
        ),
        sample=_sample(phantom),
    )


def check_update_not_reordered(rows: list[SummaryRow], threshold: int = NOTIFY_THRESHOLD) -> Violation | None:
    """A payload must never execute before the payload appended above it.

    ``notified`` is set by an UPDATE the handler appends immediately after
    the UPSERT that creates the row, and the UPSERT never writes the column
    itself. An UPDATE that ran first would match no row and lose the write
    permanently, so any high-match row still reading false is a reordering.
    """
    stale = {row.request_id for row in rows if row.total_matches > threshold and not row.notified}
    if not stale:
        return None
    return Violation(
        check='update_not_reordered',
        detail=(
            f'{len(stale)} requests over the notify threshold ({threshold} matches) have '
            f'notified=false. The UPDATE that sets it is appended after the UPSERT that creates '
            f'the row, so it can only be false if the sink executed the two out of payload order.'
        ),
        sample=_sample(stale),
    )


def check_task_rows_present(rows: list[SummaryRow], result_ids: set[str]) -> Violation | None:
    """A rollup claiming successful tasks must have those tasks' detail rows."""
    orphaned = {row.request_id for row in rows if row.succeeded_tasks > 0} - result_ids
    if not orphaned:
        return None
    return Violation(
        check='task_rows_present',
        detail=(
            f'{len(orphaned)} requests report succeeded tasks but wrote no search_results row. '
            f'The per-request rollup was delivered while the per-task writes behind it were not.'
        ),
        sample=_sample(orphaned),
    )


def duplication_factor(result_rows: int, distinct_result_ids: int) -> float:
    """Average ``search_results`` rows per request that produced any.

    At-least-once makes this >= 1 by design — the flood phase redelivers
    every request, so roughly double the per-request task count is the
    expected steady state, not a defect.
    """
    if distinct_result_ids == 0:
        return 0.0
    return result_rows / distinct_result_ids


def check_duplication_bounded(duplication: float, max_duplication: float) -> Violation | None:
    """Redelivery is expected; an unbounded replay loop is not."""
    if duplication <= max_duplication:
        return None
    return Violation(
        check='duplication_bounded',
        detail=(
            f'{duplication:.1f} search_results rows per request exceeds the {max_duplication:.1f} cap. '
            f'At-least-once permits duplicates, but this many means the same messages are being '
            f'replayed repeatedly — look for offsets that never commit.'
        ),
    )


def run_checks(
    *,
    total_messages: int,
    summaries: list[SummaryRow],
    result_ids: set[str],
    result_rows: int,
    max_duplication: float,
) -> Report:
    """Apply every check and collect the failures."""
    expected = expected_request_ids(total_messages)
    duplication = duplication_factor(result_rows, len(result_ids))
    candidates = (
        check_no_loss(expected, summaries),
        check_no_phantom(expected, summaries),
        check_update_not_reordered(summaries),
        check_task_rows_present(summaries, result_ids),
        check_duplication_bounded(duplication, max_duplication),
    )
    return Report(
        produced=len(expected),
        summaries=len(summaries),
        result_rows=result_rows,
        duplication=duplication,
        violations=[v for v in candidates if v is not None],
    )


# --- database access ------------------------------------------------------


async def _fetch(conn: asyncpg.Connection) -> tuple[list[SummaryRow], set[str], int]:
    """Read both tables the checks reason about."""
    summary_records = await conn.fetch(
        'SELECT request_id, total_matches, succeeded_tasks, notified FROM request_summaries'
    )
    summaries = [
        SummaryRow(
            request_id=record['request_id'],
            total_matches=record['total_matches'],
            succeeded_tasks=record['succeeded_tasks'],
            notified=record['notified'],
        )
        for record in summary_records
    ]
    result_ids = {record['request_id'] for record in await conn.fetch('SELECT DISTINCT request_id FROM search_results')}
    result_rows = await conn.fetchval('SELECT COUNT(*) FROM search_results')
    return summaries, result_ids, result_rows


async def _wait_until_drained(conn: asyncpg.Connection, *, wait_seconds: int, poll_seconds: int = 5) -> None:
    """Block until the summary row count stops moving, or the budget runs out.

    A request still in flight looks exactly like a lost one, so checking
    while the pipeline is draining reports failures that are not real. Two
    consecutive identical counts is the settle signal.
    """
    if wait_seconds <= 0:
        return
    deadline = asyncio.get_running_loop().time() + wait_seconds
    previous = -1
    while asyncio.get_running_loop().time() < deadline:
        current = await conn.fetchval('SELECT COUNT(*) FROM request_summaries')
        if current == previous:
            print(f'settled at {current} request_summaries rows', flush=True)
            return
        print(f'draining: {current} request_summaries rows', flush=True)
        previous = current
        await asyncio.sleep(poll_seconds)
    print(f'WARNING: still moving after {wait_seconds}s — verifying anyway', flush=True)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--total-messages',
        type=int,
        required=True,
        help='TOTAL_MESSAGES the producer ran with; the produced id set is rebuilt from it',
    )
    parser.add_argument('--dsn', default=DEFAULT_DSN, help=f'archive_results_db DSN (default: {DEFAULT_DSN})')
    parser.add_argument(
        '--wait-seconds',
        type=int,
        default=300,
        help='how long to wait for the row count to settle before checking (0 = check now)',
    )
    parser.add_argument(
        '--max-duplication',
        type=float,
        default=10.0,
        help='fail when search_results rows per request exceed this (replay-loop guard)',
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    conn = await asyncpg.connect(args.dsn)
    try:
        await _wait_until_drained(conn, wait_seconds=args.wait_seconds)
        summaries, result_ids, result_rows = await _fetch(conn)
    finally:
        await conn.close()

    report = run_checks(
        total_messages=args.total_messages,
        summaries=summaries,
        result_ids=result_ids,
        result_rows=result_rows,
        max_duplication=args.max_duplication,
    )

    print('')
    print(f'produced requests   : {report.produced}')
    print(f'summary rows        : {report.summaries}')
    print(f'search_results rows : {report.result_rows}')
    print(f'duplication factor  : {report.duplication:.2f} (>= 1.0 is expected under at-least-once)')
    print('')
    for violation in report.violations:
        print(violation.render(), flush=True)
    if report.ok:
        print('OK: every produced request landed, in order, with its task rows.', flush=True)
        return 0
    print(f'{len(report.violations)} check(s) failed.', flush=True)
    return 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
