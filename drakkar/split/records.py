"""Wire formats of the split feature: records of the results log and of the parts topic.

The headers, their order and the value encoding are a contract pinned by
``tests/fixtures/split_contract.json``. Header values are bytes; numbers are
ASCII decimal text.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast, get_args

from pydantic import BaseModel, ValidationError

from drakkar.split.models import PartResult, SplitPart

SPLIT_VERSION = '1'
PARTS_PARTITION_BASE = 1_000_000
DK_HEADER_PREFIX = 'dk-'
ORIGIN_KAFKA = 'kafka'

H_VERSION = 'dk-split-version'
H_RECORD = 'dk-split-record'
H_GROUP = 'dk-split-group'
H_TOPIC = 'dk-split-topic'
H_PARTITION = 'dk-split-partition'
H_OFFSET = 'dk-split-offset'
H_ATTEMPT = 'dk-split-attempt'
H_ATTEMPT_ID = 'dk-split-attempt-id'
H_INDEX = 'dk-split-index'
H_ORIGIN = 'dk-split-origin'
H_REQUEST_ID = 'dk-request-id'

LOG_HEADER_ORDER: tuple[str, ...] = (
    H_VERSION,
    H_RECORD,
    H_GROUP,
    H_PARTITION,
    H_OFFSET,
    H_ATTEMPT,
    H_ATTEMPT_ID,
)
PART_HEADER_ORDER: tuple[str, ...] = (
    H_VERSION,
    H_GROUP,
    H_TOPIC,
    H_PARTITION,
    H_OFFSET,
    H_ATTEMPT,
    H_ATTEMPT_ID,
    H_INDEX,
    H_ORIGIN,
    H_REQUEST_ID,
)

RecordKind = Literal['open', 'split-done', 'split-cancelled', 'result', 'closed']
ClosedOutcome = Literal['aggregated', 'dlq', 'timeout', 'split_failed', 'attempts_spent', 'orphaned']
RECORD_KINDS: frozenset[str] = frozenset(get_args(RecordKind))

Header = tuple[str, bytes | None]
ATTEMPT_ID_BYTES = 8
INT64_MAX = 2**63 - 1
MAX_DECIMAL_DIGITS = len(str(INT64_MAX))


@dataclass(frozen=True)
class JobRef:
    """Names one attempt of one job."""

    group: str
    partition: int
    offset: int
    attempt: int
    attempt_id: str


@dataclass(frozen=True)
class WireRecord:
    """A record ready to be produced."""

    key: bytes | None
    value: bytes | None
    headers: list[Header]
    partition: int


@dataclass(frozen=True)
class LogEntry:
    """A valid record of the results log, read back."""

    ref: JobRef
    kind: RecordKind
    body: dict[str, Any]


def job_id(partition: int, offset: int, attempt: int) -> str:
    """A job as log lines show it: ``<partition>:<offset>#<attempt>``."""
    return f'{partition}:{offset}#{attempt}'


def ref_id(ref: JobRef) -> str:
    """The attempt ``ref`` as log lines show it."""
    return job_id(ref.partition, ref.offset, ref.attempt)


def new_attempt_id() -> str:
    """A fresh attempt id: 16 lower-case hex characters."""
    return secrets.token_hex(ATTEMPT_ID_BYTES)


def encode_value(body: Mapping[str, Any]) -> bytes:
    """The contract bytes of a JSON value: sorted keys, compact, raw UTF-8."""
    return json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def _text(value: str | int) -> bytes:
    return str(value).encode('utf-8')


def log_record(ref: JobRef, kind: RecordKind, body: Mapping[str, Any]) -> WireRecord:
    """The results-log record of ``kind`` for the job attempt ``ref``."""
    values = {
        H_VERSION: SPLIT_VERSION,
        H_RECORD: kind,
        H_GROUP: ref.group,
        H_PARTITION: ref.partition,
        H_OFFSET: ref.offset,
        H_ATTEMPT: ref.attempt,
        H_ATTEMPT_ID: ref.attempt_id,
    }
    return WireRecord(
        key=f'{ref.partition}:{ref.offset}'.encode(),
        value=encode_value(body),
        headers=[(name, _text(values[name])) for name in LOG_HEADER_ORDER],
        partition=ref.partition,
    )


def open_body(*, worker: str, instance: str, request_id: str | None) -> dict[str, Any]:
    """The value of an ``open`` record."""
    return {'worker': worker, 'instance': instance, 'request_id': request_id, 'origin': ORIGIN_KAFKA}


def _data_json(data: Any) -> Any:
    """The JSON-ready form of a part's data: a model through pydantic, a dict as it is."""
    if isinstance(data, BaseModel):
        return data.model_dump(mode='json')
    return data


def result_body(result: PartResult) -> dict[str, Any]:
    """The value of a ``result`` record: the part result with ``part`` in place of ``index``."""
    body = result.model_dump(mode='json', exclude={'data', 'index'})
    body['part'] = result.index
    body['data'] = _data_json(result.data)
    return body


def part_result_from_body(body: Mapping[str, Any]) -> PartResult:
    """Rebuild a part result from the value of a ``result`` record; ``data`` stays a dict or None.

    The part number and every counter must be a JSON integer that is not
    negative. A boolean, a text of digits or a negative number is refused:
    a lenient read would count ``true`` as part 1, and a negative counter
    would take results of other parts off the sums of the job.

    Raises:
        KeyError: the body has no ``part``.
        ValueError: a number of the body is not a count, or the body does not
            fit the model (pydantic's ``ValidationError`` is a ``ValueError``).
    """
    fields = {name: value for name, value in body.items() if name != 'part'}
    fields['index'] = body['part']
    _require_counts(fields, ('index', *_RESULT_COUNTERS))
    deliveries = fields.get('deliveries')
    if isinstance(deliveries, Mapping):
        for counts in deliveries.values():
            if isinstance(counts, Mapping):
                _require_counts(counts, _SINK_COUNTERS)
    return PartResult.model_validate(fields)


_RESULT_COUNTERS = ('tasks_succeeded', 'tasks_failed', 'tasks_replaced')
_SINK_COUNTERS = ('delivered', 'skipped', 'failed')


def _require_counts(fields: Mapping[str, Any], names: Sequence[str]) -> None:
    """Raise ``ValueError`` when a present field of ``names`` is not a non-negative integer (a boolean is none)."""
    for name in names:
        value = fields.get(name, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f'{name} is not a count: {value!r}')


def closed_body(*, outcome: ClosedOutcome, received: int, parts: int | None, duration_seconds: float) -> dict[str, Any]:
    """The value of a ``closed`` record."""
    return {'outcome': outcome, 'received': received, 'parts': parts, 'duration_seconds': duration_seconds}


def _header_map(headers: Sequence[Header] | None) -> dict[str, bytes | None]:
    """The first value of each header name."""
    found: dict[str, bytes | None] = {}
    for name, value in headers or ():
        found.setdefault(name, value)
    return found


def _decimal(raw: bytes | None) -> int | None:
    """A non-negative decimal that fits a signed 64-bit integer, else None."""
    if raw is None or not raw.isascii() or not raw.isdigit() or len(raw) > MAX_DECIMAL_DIGITS:
        return None
    number = int(raw)
    return number if number <= INT64_MAX else None


def _utf8(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    try:
        return raw.decode('utf-8')
    except UnicodeDecodeError:
        return None


def _reject_constant(name: str) -> None:
    raise ValueError(f'{name} is not JSON')


def _object(value: bytes | None) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        # Decode as UTF-8 first: json.loads on bytes would also accept UTF-16 and UTF-32.
        body = json.loads(value.decode('utf-8'), parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        # UnicodeDecodeError is a ValueError.
        return None
    return body if isinstance(body, dict) else None


def parse_log_record(headers: Sequence[Header] | None, value: bytes | None) -> LogEntry | None:
    """Read a results-log record; ``None`` for anything that is not a valid record of version 1."""
    found = _header_map(headers)
    if found.get(H_VERSION) != SPLIT_VERSION.encode():
        return None
    kind = _utf8(found.get(H_RECORD))
    group = _utf8(found.get(H_GROUP))
    attempt_id = _utf8(found.get(H_ATTEMPT_ID))
    partition = _decimal(found.get(H_PARTITION))
    offset = _decimal(found.get(H_OFFSET))
    attempt = _decimal(found.get(H_ATTEMPT))
    body = _object(value)
    if kind not in RECORD_KINDS or not group or not attempt_id or body is None:
        return None
    if partition is None or offset is None or attempt is None:
        return None
    ref = JobRef(group=group, partition=partition, offset=offset, attempt=attempt, attempt_id=attempt_id)
    return LogEntry(ref=ref, kind=cast(RecordKind, kind), body=body)


def part_value(item: object) -> bytes:
    """The value of a part record: a model as its JSON, ``bytes`` as they are."""
    if isinstance(item, bytes):
        return item
    if isinstance(item, BaseModel):
        return item.model_dump_json().encode('utf-8')
    raise TypeError(f'a part must be a pydantic model or bytes, got {type(item).__name__}')


def part_partition(source_offset: int, index: int, partition_count: int) -> int:
    """The parts-topic partition of part ``index``: parts spread evenly from ``offset mod N``.

    Raises:
        ValueError: ``partition_count`` is below 1; a topic has at least one partition.
    """
    if partition_count < 1:
        raise ValueError(f'the parts topic needs at least 1 partition, got {partition_count}')
    return (source_offset % partition_count + index) % partition_count


def part_record(
    ref: JobRef,
    *,
    source_topic: str,
    index: int,
    request_id: str | None,
    key: bytes | None,
    value: bytes,
    source_headers: Sequence[Header],
    partition_count: int,
) -> WireRecord:
    """The parts-topic record of part ``index``: source headers without ``dk-`` ones, then the split headers."""
    values: dict[str, str | int | None] = {
        H_VERSION: SPLIT_VERSION,
        H_GROUP: ref.group,
        H_TOPIC: source_topic,
        H_PARTITION: ref.partition,
        H_OFFSET: ref.offset,
        H_ATTEMPT: ref.attempt,
        H_ATTEMPT_ID: ref.attempt_id,
        H_INDEX: index,
        H_ORIGIN: ORIGIN_KAFKA,
        H_REQUEST_ID: request_id,
    }
    kept = [(name, raw) for name, raw in source_headers if not name.startswith(DK_HEADER_PREFIX)]
    added = [(name, _text(text)) for name in PART_HEADER_ORDER if (text := values[name]) is not None]
    return WireRecord(
        key=key,
        value=value,
        headers=kept + added,
        partition=part_partition(ref.offset, index, partition_count),
    )


def parse_part_headers(headers: Sequence[Header] | None) -> SplitPart | None:
    """Read the split headers of a parts-topic message; ``None`` when they are missing or invalid."""
    found = _header_map(headers)
    if found.get(H_VERSION) != SPLIT_VERSION.encode():
        return None
    group = _utf8(found.get(H_GROUP))
    topic = _utf8(found.get(H_TOPIC))
    attempt_id = _utf8(found.get(H_ATTEMPT_ID))
    origin = _utf8(found.get(H_ORIGIN))
    numbers = [_decimal(found.get(name)) for name in (H_PARTITION, H_OFFSET, H_ATTEMPT, H_INDEX)]
    if not group or not topic or not attempt_id or None in numbers:
        return None
    fields = {
        'group': group,
        'source_topic': topic,
        'source_partition': numbers[0],
        'source_offset': numbers[1],
        'attempt': numbers[2],
        'attempt_id': attempt_id,
        'index': numbers[3],
        'origin': origin,
    }
    try:
        return SplitPart.model_validate(fields)
    except ValidationError:
        return None
