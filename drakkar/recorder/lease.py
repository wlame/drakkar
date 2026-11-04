"""Who holds a lease, as an election provider's store reports it.

The election providers answer :meth:`holder` with a :class:`LeaseHolder` for
the Databases view of the UI. The read is read-only: it takes, refreshes and
frees nothing, and it needs no token. Every provider keeps a lock body (the
sorted compact JSON with ``acquired_at``, ``instance``, ``provider``,
``token`` and ``worker``) or a row with the same facts; the token never
leaves the provider.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LeaseHolder:
    """The holder of one scope.

    ``worker``, ``instance`` and ``acquired_at`` (text, as the store keeps
    it) are ``None`` when the store holds the lease with a body that names
    no holder. ``age_seconds`` is the time since the last refresh, on the
    store's clock, when the store knows it. ``age_approximate`` is true when
    the age compares the store's time with this host's clock instead (a
    read that may not write to the store cannot sample the store's clock).
    ``expires_in_seconds`` is the time left before the store ends the lease
    by itself, when the store knows it.
    """

    worker: str | None
    instance: str | None
    acquired_at: str | None
    age_seconds: float | None = None
    expires_in_seconds: float | None = None
    age_approximate: bool = False


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def holder_from_body(
    body: bytes | str | Mapping[str, Any] | None,
    *,
    age_seconds: float | None = None,
    expires_in_seconds: float | None = None,
    age_approximate: bool = False,
) -> LeaseHolder:
    """The holder named by a lock body; a body that is not a JSON object names no holder.

    Only text values are taken: a field of another type reads as ``None``.
    """
    fields: Mapping[str, Any] = {}
    if isinstance(body, Mapping):
        fields = body
    elif body is not None:
        try:
            parsed = json.loads(body)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            fields = parsed
    return LeaseHolder(
        worker=_text(fields.get('worker')),
        instance=_text(fields.get('instance')),
        acquired_at=_text(fields.get('acquired_at')),
        age_seconds=age_seconds,
        expires_in_seconds=expires_in_seconds,
        age_approximate=age_approximate,
    )
