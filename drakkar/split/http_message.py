"""The message an HTTP split job request stores in the source topic, and how a worker reads it back.

The headers are a contract (``tests/fixtures/split_contract.json`` pins the
part headers that follow from them): a worker of another release reads the
same records. Reading fails closed: a malformed switch means no sink writes.
"""

from __future__ import annotations

from collections.abc import Sequence

from drakkar.models import HttpJob, SourceMessage
from drakkar.split.records import ANY_PARTITION, H_REQUEST_ID, Header, WireRecord, header_map, utf8_text
from drakkar.utils import validate_request_id

H_ORIGIN_HTTP = 'dk-origin'
H_HTTP_CLIENT = 'dk-http-client'
H_DELIVER_SINKS = 'dk-deliver-sinks'
ORIGIN_HTTP_VALUE = b'http'
TRUE_TEXT = b'true'
FALSE_TEXT = b'false'


def source_message_record(*, request_id: str, client: str, deliver_sinks: bool, body: bytes) -> WireRecord:
    """The record a split job request writes: the raw body, keyed by the request id, on any partition."""
    return WireRecord(
        key=request_id.encode('utf-8'),
        value=body,
        headers=[
            (H_ORIGIN_HTTP, ORIGIN_HTTP_VALUE),
            (H_REQUEST_ID, request_id.encode('utf-8')),
            (H_HTTP_CLIENT, client.encode('utf-8')),
            (H_DELIVER_SINKS, TRUE_TEXT if deliver_sinks else FALSE_TEXT),
        ],
        partition=ANY_PARTITION,
    )


def is_http_origin(headers: Sequence[Header] | None) -> bool:
    """True when the first ``dk-origin`` header has the value ``http``; runs for every source message, so no dict."""
    for name, value in headers or ():
        if name == H_ORIGIN_HTTP:
            return value == ORIGIN_HTTP_VALUE
    return False


def read_http_job(headers: Sequence[Header] | None) -> HttpJob | None:
    """Read the job facts of an HTTP job message; None when the headers do not make one.

    A message needs ``dk-origin: http`` and a ``dk-request-id`` that passes
    ``validate_request_id``. A missing or non-UTF-8 client reads as an empty
    text; a switch other than exactly ``true`` reads as no sink writes.
    """
    found = header_map(headers)
    if found.get(H_ORIGIN_HTTP) != ORIGIN_HTTP_VALUE:
        return None
    request_id = utf8_text(found.get(H_REQUEST_ID))
    if not request_id:
        return None
    try:
        validate_request_id(request_id)
    except ValueError:
        return None
    return HttpJob(
        request_id=request_id,
        client=utf8_text(found.get(H_HTTP_CLIENT)) or '',
        deliver_sinks=found.get(H_DELIVER_SINKS) == TRUE_TEXT,
    )


def delivers_to_sinks(message: SourceMessage) -> bool:
    """False when the message belongs to a job whose request asked for no sink writes."""
    if message.split is not None:
        return message.split.deliver_sinks
    if message.http_job is not None:
        return message.http_job.deliver_sinks
    return True
