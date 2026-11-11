"""The DLQ record: the original source message plus ``dk-`` headers.

One record is written per failed source message. Its key and value are the
source message's key and value, byte for byte, so a record can be replayed
into the source topic as it is. The failure facts travel in headers:

1. the source message's own headers, in their order, minus every header
   whose name starts with ``dk-`` — a replayed message that fails again gets
   fresh facts, never two sets;
2. the ``dk-`` headers in ``DLQ_HEADER_ORDER``, each UTF-8 text, and only
   those whose value is present.

The order is fixed so the header bytes of a record are reproducible by any
implementation; ``tests/fixtures/dlq_contract.json`` pins it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from drakkar.timefmt import format_rfc3339_micro
from drakkar.utils import redact_url

DLQ_RECORD_VERSION = '1'
DLQ_ERROR_CUT_BYTES = 2000
DK_HEADER_PREFIX = 'dk-'

DLQ_HEADER_ORDER: tuple[str, ...] = (
    'dk-record-version',
    'dk-stage',
    'dk-error',
    'dk-sink',
    'dk-task-id',
    'dk-attempts',
    'dk-delivered-sinks',
    'dk-source-topic',
    'dk-source-partition',
    'dk-source-offset',
    'dk-source-timestamp',
    'dk-request-id',
    'dk-worker',
    'dk-instance',
    'dk-failed-at',
)

DLQStage = Literal['parse', 'execute', 'sink']

# A Kafka header may carry no value at all (null), which is not the same as
# an empty value; the record keeps the distinction when it copies headers.
Header = tuple[str, bytes | None]


class DLQRecord(BaseModel):
    """One failed source message, as the DLQ sink writes it.

    ``worker``, ``instance`` and ``failed_at`` are left empty by whoever
    builds the record: the DLQ sink fills them when it sends, because only
    the sink knows the worker it runs in and the moment of the write.
    """

    model_config = ConfigDict(frozen=True)

    key: bytes | None = Field(description='The source message key, unchanged; None for an HTTP request.')
    value: bytes = Field(description='The source message value (or the raw HTTP request body), unchanged.')
    source_headers: list[Header] = Field(
        default_factory=list,
        description='The source message headers in their order; dk- headers among them are dropped on write.',
    )
    stage: DLQStage = Field(description='Where the message failed: parse, execute or sink.')
    error: str = Field(description='The error text; redacted and cut when written.')
    sink: str | None = Field(default=None, description='<sink_type>/<sink_name> of the failed sink; sink stage only.')
    task_id: str | None = Field(default=None, description="The failed task's id; execute stage only.")
    attempts: int | None = Field(default=None, description='Attempts made; None for the parse stage.')
    delivered_sinks: list[str] = Field(
        default_factory=list,
        description='<sink_type>/<sink_name> of every sink that delivered a result of this message.',
    )
    source_topic: str | None = Field(default=None, description='The source topic; None for an HTTP request.')
    source_partition: int | None = Field(default=None, description='The source partition; None for an HTTP request.')
    source_offset: int | None = Field(default=None, description='The source offset; None for an HTTP request.')
    source_timestamp_ms: int | None = Field(
        default=None,
        description='The Kafka timestamp of the message in ms; for an HTTP request the time it was received.',
    )
    request_id: str | None = Field(default=None, description='The request id of the source message or HTTP request.')
    worker: str | None = Field(default=None, description='The worker name; set by the DLQ sink.')
    instance: str | None = Field(default=None, description='The worker instance id; set by the DLQ sink.')
    failed_at: datetime | None = Field(default=None, description='When the record was written; set by the DLQ sink.')


def cut_error(text: str) -> str:
    """Redact URL credentials in ``text``, then cut it to 2000 bytes at a UTF-8 character boundary.

    Redaction runs first so a cut can never leave half a password behind.
    A lone surrogate (text decoded with ``surrogateescape``, as subprocess
    output can be) has no UTF-8 form and becomes ``?``.
    """
    encoded = redact_url(text).encode('utf-8', errors='replace')
    # A cut inside a multi-byte character leaves an incomplete tail, which
    # 'ignore' drops, so the result ends at the last whole character.
    return encoded[:DLQ_ERROR_CUT_BYTES].decode('utf-8', errors='ignore')


def _decimal(number: int | None) -> str | None:
    return None if number is None else str(number)


def _dk_values(record: DLQRecord) -> dict[str, str | None]:
    """Every dk- header value of ``record``, None where the value is absent."""
    return {
        'dk-record-version': DLQ_RECORD_VERSION,
        'dk-stage': record.stage,
        'dk-error': cut_error(record.error),
        'dk-sink': record.sink,
        'dk-task-id': record.task_id,
        'dk-attempts': _decimal(record.attempts),
        'dk-delivered-sinks': ','.join(sorted(record.delivered_sinks)) or None,
        'dk-source-topic': record.source_topic,
        'dk-source-partition': _decimal(record.source_partition),
        'dk-source-offset': _decimal(record.source_offset),
        'dk-source-timestamp': _decimal(record.source_timestamp_ms),
        'dk-request-id': record.request_id,
        'dk-worker': record.worker,
        'dk-instance': record.instance,
        'dk-failed-at': None if record.failed_at is None else format_rfc3339_micro(record.failed_at),
    }


def render_headers(record: DLQRecord) -> list[Header]:
    """The record's Kafka headers: kept source headers first, then the present dk- headers in order."""
    kept = [(name, value) for name, value in record.source_headers if not name.startswith(DK_HEADER_PREFIX)]
    values = _dk_values(record)
    facts = [(name, text.encode('utf-8')) for name in DLQ_HEADER_ORDER if (text := values[name]) is not None]
    return kept + facts
