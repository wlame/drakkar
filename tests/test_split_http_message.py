"""The message of an HTTP split job request and how a worker reads it back."""

import pytest

from drakkar.models import HttpJob, SourceMessage, SplitPart
from drakkar.split.http_message import (
    delivers_to_sinks,
    is_http_origin,
    read_http_job,
    source_message_record,
)
from drakkar.split.records import ANY_PARTITION


def test_source_message_record_headers_in_order():
    record = source_message_record(request_id='order-123', client='tenant-a', deliver_sinks=False, body=b'{"a":1}')

    assert record.headers == [
        ('dk-origin', b'http'),
        ('dk-request-id', b'order-123'),
        ('dk-http-client', b'tenant-a'),
        ('dk-deliver-sinks', b'false'),
    ]
    assert record.key == b'order-123'
    assert record.value == b'{"a":1}'
    assert record.partition == ANY_PARTITION == -1


def _headers(**changes):
    base = {
        'dk-origin': b'http',
        'dk-request-id': b'order-123',
        'dk-http-client': b'tenant-a',
        'dk-deliver-sinks': b'true',
    }
    base.update(changes)
    return [(name, value) for name, value in base.items() if value is not None]


@pytest.mark.parametrize(
    ('headers', 'expected'),
    [
        (_headers(), HttpJob(request_id='order-123', client='tenant-a', deliver_sinks=True)),
        (_headers(**{'dk-origin': None}), None),
        (_headers(**{'dk-origin': b'kafka'}), None),
        (_headers(**{'dk-request-id': b'bad id'}), None),
        (_headers(**{'dk-request-id': None}), None),
        (_headers(**{'dk-http-client': None}), HttpJob(request_id='order-123', client='', deliver_sinks=True)),
        (_headers(**{'dk-http-client': b'\xff'}), HttpJob(request_id='order-123', client='', deliver_sinks=True)),
        (
            _headers(**{'dk-deliver-sinks': b'TRUE'}),
            HttpJob(request_id='order-123', client='tenant-a', deliver_sinks=False),
        ),
        (
            _headers(**{'dk-deliver-sinks': None}),
            HttpJob(request_id='order-123', client='tenant-a', deliver_sinks=False),
        ),
    ],
)
def test_read_http_job(headers, expected):
    assert read_http_job(headers) == expected


def test_is_http_origin_only_for_the_http_value():
    assert is_http_origin([('dk-origin', b'http')])
    assert not is_http_origin([('dk-origin', b'kafka')])
    assert not is_http_origin([])
    assert not is_http_origin(None)
    # The first header of the name decides, as it does for every header read.
    assert is_http_origin([('other', b'x'), ('dk-origin', b'http'), ('dk-origin', b'kafka')])
    assert not is_http_origin([('dk-origin', b'kafka'), ('dk-origin', b'http')])


def _message(**fields):
    return SourceMessage(topic='t', partition=0, offset=1, value=b'', timestamp=0, **fields)


def test_delivers_to_sinks():
    part = {
        'group': 'g',
        'source_topic': 't',
        'source_partition': 0,
        'source_offset': 1,
        'attempt': 1,
        'attempt_id': 'a',
        'index': 0,
    }

    assert delivers_to_sinks(_message())
    assert not delivers_to_sinks(_message(split=SplitPart(**part, origin='http', deliver_sinks=False)))
    assert delivers_to_sinks(_message(split=SplitPart(**part)))
    assert not delivers_to_sinks(_message(http_job=HttpJob(request_id='r', client='', deliver_sinks=False)))
    assert delivers_to_sinks(_message(http_job=HttpJob(request_id='r', client='', deliver_sinks=True)))
