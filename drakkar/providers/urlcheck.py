"""The host-and-port rule that every provider URL or address check shares.

URL parsers disagree at the edges of a host and a port: one reads a port of
six digits that another refuses, one decodes a zone index in an IPv6 address
that another keeps, one takes an IPv4 address in brackets that another
refuses. A provider that checks the address of a connection by hand, before
its client library parses it, runs this rule, so one address means the same
to every implementation. The rule lives apart from any provider so that all of
them judge the host and the port alike.

Every character class below is ASCII and spelled out, never ``\\w`` or ``\\d``,
which match more than ASCII in Python.
"""

import ipaddress
import re

# 0x21 to 0x7E: no space, tab, line break, other control character, DEL or non-ASCII character.
PRINTABLE_ASCII_RE = re.compile(r'[!-~]*')
# A host name, or nothing; the caller decides whether an empty host is allowed.
HOST_NAME_RE = re.compile(r'[A-Za-z0-9._-]*')
# The characters of an IPv6 address in brackets. No '%': a zone index is decoded differently by each parser.
IPV6_LITERAL_RE = re.compile(r'[0-9A-Fa-f:.]+')
# The port has a fixed number of digits at most (leading zeros count), so a number is never longer
# than every client reads alike: int() refuses a text of more than 4300 digits.
PORT_RE = re.compile(r'[0-9]{1,5}')
MAX_PORT = 65535


def is_portable_host_and_port(host_and_port: str) -> bool:
    """Tell whether ``host_and_port`` is a host name or a bracketed IPv6 address, then an optional port.

    The host is empty, a host name of letters, digits, ``.``, ``_`` and ``-``,
    or an IPv6 address in brackets with no zone. A port, when one is given, is
    at most five digits and 1 to 65535; a ``:`` with nothing after it is the
    same as no port.
    """
    if host_and_port.startswith('['):
        address, bracket, after = host_and_port[1:].partition(']')
        if not bracket or not is_ipv6_address(address) or after[:1] not in ('', ':'):
            return False
        port = after[1:]
    else:
        host, _, port = host_and_port.partition(':')
        if HOST_NAME_RE.fullmatch(host) is None:
            return False
    return not port or (PORT_RE.fullmatch(port) is not None and 1 <= int(port) <= MAX_PORT)


def is_ipv6_address(text: str) -> bool:
    """Tell whether ``text`` is an IPv6 address; an IPv4 address in brackets is not one."""
    if IPV6_LITERAL_RE.fullmatch(text) is None:
        return False
    try:
        ipaddress.IPv6Address(text)
    except ValueError:
        return False
    return True
