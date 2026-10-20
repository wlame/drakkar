"""Unit tests for the host-and-port rule that the provider URL checks share.

The expected value of each row is the verdict that ``is_portable_url('redis://'
+ text + '/0')`` gave before the rule moved out of the Redis module, so the
table pins that moving it changed no verdict.
"""

import pytest

from drakkar.providers.redis import is_portable_url
from drakkar.providers.urlcheck import PRINTABLE_ASCII_RE, is_ipv6_address, is_portable_host_and_port

# (host and port text, accepted). Nothing here holds '@', '/', '#' or '?': those
# split the URL before the host and the port are judged.
HOST_AND_PORT_CASES = [
    ('h', True),
    ('h:1', True),
    ('h:65535', True),
    ('h:00001', True),
    ('', True),
    (':8500', True),
    # An empty port after ':' is accepted, as it was in the Redis URL check.
    ('h:', True),
    (':', True),
    ('h.example-1_a', True),
    ('[::1]', True),
    ('[::1]:8500', True),
    ('[::1]:', True),
    ('[::ffff:1.2.3.4]:80', True),
    ('h:0', False),
    ('h:65536', False),
    ('h:99999', False),
    ('h:008500', False),
    ('h:000000', False),
    ('h:+1', False),
    ('h:-1', False),
    ('h:1_0', False),
    ('h:abc', False),
    ('h:1:2', False),
    ('h:\u0661', False),
    ('h h', False),
    ('h:1 ', False),
    ('h$x', False),
    ('h\u00e9', False),
    ('[::1', False),
    ('[::1]x', False),
    ('[]', False),
    ('[:::]', False),
    ('[::g]', False),
    ('[::1]:0', False),
    ('[::1]:65536', False),
    ('[fe80::1%25eth0]', False),
    ('[fe80::1%25eth0]:8500', False),
    ('[127.0.0.1]', False),
    ('[127.0.0.1]:8500', False),
]


@pytest.mark.parametrize(('text', 'accepted'), HOST_AND_PORT_CASES, ids=[repr(text) for text, _ in HOST_AND_PORT_CASES])
def test_is_portable_host_and_port_gives_the_verdict_of_the_redis_url_check(text, accepted):
    assert is_portable_host_and_port(text) is accepted


@pytest.mark.parametrize(('text', 'accepted'), HOST_AND_PORT_CASES, ids=[repr(text) for text, _ in HOST_AND_PORT_CASES])
def test_is_portable_url_judges_the_host_and_port_with_the_shared_rule(text, accepted):
    assert is_portable_url('redis://' + text + '/0') is accepted


@pytest.mark.parametrize(
    ('text', 'accepted'),
    [
        ('::1', True),
        ('::', True),
        ('1::', True),
        ('fe80::1', True),
        ('2001:db8::8a2e:370:7334', True),
        ('1:2:3:4:5:6:7:8', True),
        ('::ffff:1.2.3.4', True),
        ('127.0.0.1', False),
        ('fe80::1%25eth0', False),
        ('fe80::1%eth0', False),
        ('', False),
        (':::', False),
        ('1::2::3', False),
        ('::g', False),
        ('abc', False),
        ('1:2:3:4:5:6:7:8:9', False),
    ],
)
def test_is_ipv6_address_accepts_only_an_ipv6_address_without_a_zone(text, accepted):
    assert is_ipv6_address(text) is accepted


@pytest.mark.parametrize(
    ('text', 'accepted'),
    [
        ('', True),
        ('redis://h:6379/0', True),
        ('!~', True),
        ('a b', False),
        ('a\n', False),
        ('tok\u00e9', False),
        ('\x7f', False),
    ],
)
def test_printable_ascii_re_matches_only_the_characters_from_bang_to_tilde(text, accepted):
    assert (PRINTABLE_ASCII_RE.fullmatch(text) is not None) is accepted
