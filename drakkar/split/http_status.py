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
from datetime import datetime
from typing import Any, Literal, cast

from drakkar.sinks.dlq_record import cut_error
from drakkar.split.records import ANY_PARTITION, Header, WireRecord, encode_value, header_map, json_object
from drakkar.timefmt import format_epoch_micro

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
    'duplicate',
)
ENDED_STATES = frozenset({'done', 'failed'})

_JOB_PATTERN = re.compile(r'([0-9]{1,19}):([0-9]{1,19})')
LONE_SURROGATE = re.compile('[\ud800-\udfff]')
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
    return format_epoch_micro(epoch_seconds)


def _parts_body(parts: PartCounts) -> dict[str, int | None]:
    return {'total': parts.total, 'received': parts.received, 'failed': parts.failed}


def status_record(record: StatusRecord) -> WireRecord:
    """The status-topic record for ``record``; the partition is left to the key partitioner.

    Raises:
        ValueError: a ``failed`` record has no outcome of ``FAILED_OUTCOMES``; every reader would refuse it.
    """
    if record.state == 'failed' and record.outcome not in FAILED_OUTCOMES:
        raise ValueError(f'a failed status needs an outcome of {", ".join(FAILED_OUTCOMES)}, got {record.outcome!r}')
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
    if header_map(headers).get(H_STATUS_VERSION) != STATUS_VERSION.encode() or not key:
        return None
    try:
        request_id = key.decode('utf-8')
    except UnicodeDecodeError:
        return None
    body = json_object(value)
    if body is None:
        return None
    job = body.get('job')
    state = body.get('state')
    updated_at = _parse_time(body.get('updated_at'))
    match = _JOB_PATTERN.fullmatch(job) if isinstance(job, str) else None
    client = body.get('client', '')
    parts = _parse_parts(body.get('parts'))
    if match is None or not isinstance(state, str) or state not in STATE_RANK or updated_at is None or parts is False:
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
        result_json = _result_text(body.get('result'))
        if result_json is None:
            return None
        fields['result_json'] = result_json
        duration = body.get('duration_seconds')
        if duration is not None:
            if not _is_finite_number(duration):
                return None
            fields['duration_seconds'] = duration
    elif state == 'failed':
        error = body.get('error', '')
        if body.get('outcome') not in FAILED_OUTCOMES or not isinstance(error, str):
            return None
        fields['outcome'] = body['outcome']
        # A record from outside may carry any text: the map keeps no more than a record of a worker writes.
        fields['error'] = cut_error(error)
    return fields


def _result_text(result: object) -> str | None:
    """The canonical JSON text of a result; None for a number beyond the float range.

    A lone surrogate, which JSON text can carry as an escape but UTF-8 cannot, becomes U+FFFD.
    """
    try:
        text = json.dumps(result, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    except ValueError:
        return None
    if not LONE_SURROGATE.search(text):
        return text
    # A key can change, so the keys are sorted again after the replacement.
    repaired = json.loads(LONE_SURROGATE.sub('\ufffd', text))
    return json.dumps(repaired, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _is_finite_number(value: object) -> bool:
    """True for an int or a float that is finite as a float; an int too large for a float is not."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


class StatusMap:
    """The best status record of each request id, with expiry of ended entries.

    A new record replaces the stored one by the rules of the status topic:
    for another job the lower source offset wins, for the same job the
    higher or equal state rank wins. The outcome does not depend on the order
    in which records arrive, with one exception: ``done`` and ``failed`` of
    the same job have the same rank, so the one that arrives last wins.

    An end-state record older than the retention is not stored. It removes
    the stored entry of its own job, and the map marks the job as ended, so
    that a lower-rank record of that job read after it is not stored again.
    Without the mark, a restore that reads ``done`` before a late
    ``accepted`` would show the job as not ended for ever. A mark lives
    through at least one full round of :meth:`forget_old_marks`; within one
    partition of the topic, a lower-rank record follows the end state of its
    job by at most a few seconds of writes, so a restore has read every such
    record before its mark goes. A record of another job of the same id (a
    new job of a reused id) is not affected by the mark.
    """

    def __init__(self, result_retention: float) -> None:
        """``result_retention`` is how long an ended entry stays after its ``updated_at``, in seconds."""
        self._retention = result_retention
        self._entries: dict[str, tuple[StatusRecord, int]] = {}
        self._heap: list[tuple[float, str, int]] = []
        self._generation = 0
        self._counts = dict.fromkeys(STATE_RANK, 0)
        # The jobs whose end state expired, by request id: marks of this round and of the round before. An id
        # can have several such jobs (a reused id, a dropped duplicate), so each id keeps a set.
        self._marks: dict[str, set[str]] = {}
        self._old_marks: dict[str, set[str]] = {}

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
            self._end_expired(record)
            return False
        if not record.is_ended and self._is_marked(record):
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

    def _is_marked(self, record: StatusRecord) -> bool:
        """Whether the end state of the job of ``record`` expired in this round of marks or the one before."""
        return record.job in self._marks.get(record.request_id, ()) or record.job in self._old_marks.get(
            record.request_id, ()
        )

    def _end_expired(self, record: StatusRecord) -> None:
        """Mark the job of an expired end-state record as ended, and remove the entry of that job it beats."""
        self._marks.setdefault(record.request_id, set()).add(record.job)
        entry = self._entries.get(record.request_id)
        if entry is None:
            return
        stored = entry[0]
        if stored.job != record.job:
            return
        if stored.is_ended and stored.updated_at > record.updated_at:
            return
        del self._entries[record.request_id]
        self._counts[stored.state] -= 1

    def forget_old_marks(self) -> None:
        """Start a new round of marks: the marks of the round before this one are forgotten."""
        self._old_marks = self._marks
        self._marks = {}

    @staticmethod
    def _wins(new: StatusRecord, stored: StatusRecord) -> bool:
        if new.job != stored.job:
            return (new.offset, new.partition) < (stored.offset, stored.partition)
        return STATE_RANK[new.state] >= STATE_RANK[stored.state]

    def has_due(self, now: float) -> bool:
        """Whether a heap item is due at ``now``: a call of :meth:`expire` has work left."""
        return bool(self._heap) and self._heap[0][0] <= now

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
