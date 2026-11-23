"""Status records of HTTP split jobs and the in-memory map every worker builds from them.

The module is pure: it reads no clock and does no I/O; callers pass ``now``.
A job's result is kept as its compact JSON text, so reading the status of a
job never parses or re-encodes a result that can be large.
"""

from __future__ import annotations

import heapq
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, cast

from drakkar.sinks.dlq_record import cut_error
from drakkar.split.records import ANY_PARTITION, Header, WireRecord, _header_map, _object, encode_value
from drakkar.timefmt import format_rfc3339_micro

STATUS_VERSION = '1'
H_STATUS_VERSION = 'dk-status-version'
State = Literal['accepted', 'running', 'done', 'failed']
STATE_RANK: dict[str, int] = {'accepted': 0, 'running': 1, 'done': 2, 'failed': 2}
FAILED_OUTCOMES: tuple[str, ...] = (
    'dlq',
    'timeout',
    'split_failed',
    'attempts_spent',
    'orphaned',
    'parse',
    'response_failed',
)
ENDED_STATES = frozenset({'done', 'failed'})

_JOB_PATTERN = re.compile(r'([0-9]{1,19}):([0-9]{1,19})')
_TIME_PATTERN = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z')


@dataclass(frozen=True)
class PartCounts:
    """Part counters of a job; ``total`` is None until the split has finished."""

    total: int | None
    received: int
    failed: int


@dataclass(frozen=True)
class StatusRecord:
    """One status of a job: ``result_json`` is the compact JSON text of the result."""

    request_id: str
    state: State
    partition: int
    offset: int
    client: str
    updated_at: float
    parts: PartCounts | None = None
    result_json: str | None = None
    duration_seconds: float | None = None
    outcome: str | None = None
    error: str | None = None

    @property
    def job(self) -> str:
        """The source message of the job as ``<partition>:<offset>``."""
        return f'{self.partition}:{self.offset}'

    @property
    def is_ended(self) -> bool:
        """True for ``done`` and ``failed``."""
        return self.state in ENDED_STATES


def _time_text(epoch_seconds: float) -> str:
    return format_rfc3339_micro(datetime.fromtimestamp(epoch_seconds, UTC))


def _parts_body(parts: PartCounts) -> dict[str, int | None]:
    return {'total': parts.total, 'received': parts.received, 'failed': parts.failed}


def status_record(record: StatusRecord) -> WireRecord:
    """The status-topic record for ``record``; the partition is left to the key partitioner."""
    body: dict[str, Any] = {
        'state': record.state,
        'job': record.job,
        'client': record.client,
        'updated_at': _time_text(record.updated_at),
    }
    if record.parts is not None:
        body['parts'] = _parts_body(record.parts)
    if record.state == 'done':
        body['result'] = json.loads(record.result_json or 'null')
        if record.duration_seconds is not None:
            body['duration_seconds'] = record.duration_seconds
    if record.state == 'failed':
        body['outcome'] = record.outcome
        body['error'] = cut_error(record.error or '')
    return WireRecord(
        key=record.request_id.encode('utf-8'),
        value=encode_value(body),
        headers=[(H_STATUS_VERSION, STATUS_VERSION.encode())],
        partition=ANY_PARTITION,
    )


def _count(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _parse_parts(raw: object) -> PartCounts | Literal[False] | None:
    """The counters of a ``parts`` value; None when absent, False when malformed."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        return False
    total = raw.get('total')
    received = _count(raw.get('received'))
    failed = _count(raw.get('failed'))
    if received is None or failed is None or (total is not None and _count(total) is None):
        return False
    return PartCounts(total=total, received=received, failed=failed)


def _parse_time(text: object) -> float | None:
    if not isinstance(text, str) or not _TIME_PATTERN.fullmatch(text):
        return None
    try:
        return datetime.fromisoformat(text[:-1] + '+00:00').timestamp()
    except ValueError:
        return None


def parse_status_record(
    key: bytes | None, headers: Sequence[Header] | None, value: bytes | None
) -> StatusRecord | None:
    """Read a status-topic record; ``None`` for anything that is not a valid record of version 1."""
    if _header_map(headers).get(H_STATUS_VERSION) != STATUS_VERSION.encode() or not key:
        return None
    try:
        request_id = key.decode('utf-8')
    except UnicodeDecodeError:
        return None
    body = _object(value)
    if body is None:
        return None
    job = body.get('job')
    state = body.get('state')
    updated_at = _parse_time(body.get('updated_at'))
    match = _JOB_PATTERN.fullmatch(job) if isinstance(job, str) else None
    client = body.get('client', '')
    parts = _parse_parts(body.get('parts'))
    if match is None or state not in STATE_RANK or updated_at is None or parts is False:
        return None
    if not isinstance(client, str):
        return None
    fields = _end_fields(cast(str, state), body)
    if fields is None:
        return None
    return StatusRecord(
        request_id=request_id,
        state=cast(State, state),
        partition=int(match.group(1)),
        offset=int(match.group(2)),
        client=client,
        updated_at=updated_at,
        parts=parts,
        **fields,
    )


def _end_fields(state: str, body: Mapping[str, Any]) -> dict[str, Any] | None:
    """The state-specific fields of a body, or None when they are invalid."""
    fields: dict[str, Any] = {}
    if state == 'done':
        fields['result_json'] = json.dumps(
            body.get('result'), sort_keys=True, separators=(',', ':'), ensure_ascii=False
        )
        duration = body.get('duration_seconds')
        if duration is not None:
            if isinstance(duration, bool) or not isinstance(duration, int | float) or not math.isfinite(duration):
                return None
            fields['duration_seconds'] = duration
    elif state == 'failed':
        error = body.get('error', '')
        if body.get('outcome') not in FAILED_OUTCOMES or not isinstance(error, str):
            return None
        fields['outcome'] = body['outcome']
        fields['error'] = error
    return fields


class StatusMap:
    """The best status record of each request id, with expiry of ended entries.

    A new record replaces the stored one by the rules of the status topic:
    for another job the lower source offset wins, for the same job the
    higher or equal state rank wins. The outcome does not depend on the order
    in which records arrive.
    """

    def __init__(self, result_retention: float) -> None:
        """``result_retention`` is how long an ended entry stays after its ``updated_at``, in seconds."""
        self._retention = result_retention
        self._entries: dict[str, tuple[StatusRecord, int]] = {}
        self._heap: list[tuple[float, str, int]] = []
        self._generation = 0
        self._counts = dict.fromkeys(STATE_RANK, 0)

    def __len__(self) -> int:
        """The number of stored entries."""
        return len(self._entries)

    def get(self, request_id: str) -> StatusRecord | None:
        """The stored record of ``request_id``, if any."""
        entry = self._entries.get(request_id)
        return entry[0] if entry else None

    def counts(self) -> dict[str, int]:
        """The number of entries in each state."""
        return dict(self._counts)

    def put(self, record: StatusRecord, *, now: float) -> bool:
        """Offer ``record``; True when it is now the stored record of its id."""
        if record.is_ended and record.updated_at + self._retention <= now:
            return False
        entry = self._entries.get(record.request_id)
        if entry is not None:
            stored = entry[0]
            if not self._wins(record, stored):
                return False
            if record.job == stored.job and not record.client:
                record = replace(record, client=stored.client)
            self._counts[stored.state] -= 1
        self._generation += 1
        self._entries[record.request_id] = (record, self._generation)
        self._counts[record.state] += 1
        if record.is_ended:
            heapq.heappush(self._heap, (record.updated_at + self._retention, record.request_id, self._generation))
        return True

    @staticmethod
    def _wins(new: StatusRecord, stored: StatusRecord) -> bool:
        if new.job != stored.job:
            return (new.offset, new.partition) < (stored.offset, stored.partition)
        return STATE_RANK[new.state] >= STATE_RANK[stored.state]

    def expire(self, now: float, *, limit: int) -> int:
        """Remove ended entries past their retention, looking at no more than ``limit`` heap items."""
        removed = 0
        looked = 0
        while self._heap and looked < limit and self._heap[0][0] <= now:
            _, request_id, generation = heapq.heappop(self._heap)
            looked += 1
            entry = self._entries.get(request_id)
            if entry is None or entry[1] != generation:
                continue
            del self._entries[request_id]
            self._counts[entry[0].state] -= 1
            removed += 1
        return removed


def answer_body(record: StatusRecord) -> bytes:
    """The body of a status answer; the stored result text is spliced in as it is."""
    body: dict[str, Any] = {
        'request_id': record.request_id,
        'status': record.state,
        'updated_at': _time_text(record.updated_at),
    }
    if record.state == 'running' and record.parts is None:
        body['parts'] = _parts_body(PartCounts(total=None, received=0, failed=0))
    elif record.state != 'accepted' and record.parts is not None:
        body['parts'] = _parts_body(record.parts)
    if record.state == 'done' and record.duration_seconds is not None:
        body['duration_seconds'] = record.duration_seconds
    if record.state == 'failed':
        body['outcome'] = record.outcome
        body['error'] = record.error or ''
    text = encode_value(body)
    if record.state != 'done':
        return text
    return text[:-1] + b',"result":' + (record.result_json or 'null').encode('utf-8') + b'}'
