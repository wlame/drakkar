"""Canonical timestamp string format shared by both Drakkar backends.

Every framework-controlled datetime that is rendered to text — webapp
report fields, recorder JSON metadata, ad-hoc datetimes inside recorder
payload columns — uses ONE format, so a string written by a Python
worker is byte-identical to the same instant written by a Go worker
(``drakkar.FormatRFC3339Micro`` there; see docs/local-databases.md):

    2026-07-05T12:34:56.123456Z

UTC, RFC 3339, fixed six-digit microseconds, ``Z`` suffix. Microseconds
are Python's maximum real precision, and the fixed width keeps output
deterministic — ``isoformat()`` omits the fraction for whole seconds and
Go's ``RFC3339Nano`` trims trailing zeros, so neither is byte-stable.

The recorder's ``dt``/``created_at_dt`` SQLite columns keep their own
display format (``YYYY-MM-DD HH:MM:SS.mmm``, already identical on both
backends) — this module governs datetimes embedded in JSON.
"""

from __future__ import annotations

from datetime import UTC, datetime


def format_rfc3339_micro(dt: datetime) -> str:
    """Render ``dt`` in the canonical cross-backend timestamp format.

    Naive datetimes are interpreted as UTC (the framework only produces
    aware UTC datetimes; the fallback keeps user-supplied values safe);
    aware ones are converted.
    """
    aware = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return aware.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
