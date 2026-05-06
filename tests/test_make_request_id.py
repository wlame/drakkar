"""Tests for ``drakkar.utils.make_request_id`` and ``validate_request_id``."""

import re

import pytest

from drakkar.utils import make_request_id, validate_request_id

# Format shape: <prefix>_<YYYYMMDDTHHMMSS>_<4+digit counter>
# The counter is monotone module-level, so it grows beyond 4 digits over time;
# the regex tolerates this with ``\d{4,}``.
REQUEST_ID_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9]*_\d{8}T\d{6}_\d{4,}$')


def test_make_request_id_format_shape_default_prefix():
    """Default-prefix output matches ``req_<UTC-timestamp>_<counter>``."""
    rid = make_request_id()
    assert REQUEST_ID_PATTERN.match(rid), f'unexpected shape: {rid!r}'
    assert rid.startswith('req_')


def test_make_request_id_default_prefix_is_req():
    """Calling with no args yields a ``req``-prefixed ID."""
    rid = make_request_id()
    prefix, _ts, _seq = rid.split('_')
    assert prefix == 'req'


def test_make_request_id_custom_prefix_is_honored():
    """A custom prefix shows up at the start of the ID."""
    rid = make_request_id('http')
    assert REQUEST_ID_PATTERN.match(rid), f'unexpected shape: {rid!r}'
    assert rid.startswith('http_')


def test_make_request_id_monotone_counter_no_collisions_under_burst():
    """Many consecutive calls produce strictly different IDs.

    Even within the same UTC second the monotone counter ensures uniqueness.
    """
    ids = [make_request_id() for _ in range(5000)]
    assert len(set(ids)) == 5000


def test_make_request_id_counter_increments_strictly():
    """Two consecutive calls have strictly increasing counter values."""
    rid_a = make_request_id()
    rid_b = make_request_id()
    seq_a = int(rid_a.rsplit('_', 1)[1])
    seq_b = int(rid_b.rsplit('_', 1)[1])
    assert seq_b > seq_a


def test_validate_request_id_accepts_make_request_id_output():
    """Anything ``make_request_id`` produces must pass ``validate_request_id``."""
    for _ in range(50):
        validate_request_id(make_request_id())


@pytest.mark.parametrize(
    'rid',
    [
        'req_20260506T184231_0042',
        'a',
        'a' * 64,  # exactly at the 64-char limit
        'req-with-dashes',
        'req.with.dots',
        'req_with_under_scores',
    ],
)
def test_validate_request_id_accepts_valid_ids(rid: str):
    """Reasonable ASCII, whitespace-free, ``<=64``-char IDs are accepted."""
    validate_request_id(rid)


def test_validate_request_id_rejects_too_long():
    """A 65-char input is rejected and the message names the offender."""
    too_long = 'x' * 65
    with pytest.raises(ValueError) as excinfo:
        validate_request_id(too_long)
    assert too_long in str(excinfo.value)
    assert 'too long' in str(excinfo.value)


def test_validate_request_id_rejects_non_ascii():
    """A non-ASCII input is rejected and the message names the offender."""
    bad = 'req_café_001'
    with pytest.raises(ValueError) as excinfo:
        validate_request_id(bad)
    assert bad in str(excinfo.value)
    assert 'ASCII' in str(excinfo.value)


@pytest.mark.parametrize(
    'bad',
    [
        'req with space',
        'req\twith\ttab',
        'req\nwith\nnewline',
        'req\rwith\rcr',
    ],
)
def test_validate_request_id_rejects_whitespace(bad: str):
    """Whitespace of any kind is rejected and the message names the offender."""
    with pytest.raises(ValueError) as excinfo:
        validate_request_id(bad)
    assert bad in str(excinfo.value) or repr(bad) in str(excinfo.value)
    assert 'whitespace' in str(excinfo.value)
