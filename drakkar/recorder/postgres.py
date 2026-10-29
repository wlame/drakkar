"""Recorder rows on their way to the Postgres events table: settings, records and the P queue.

Stage M hands every row it wrote to the memory store to the P queue as an
``(id, row tuple)`` pair. Stage P takes chunks from the queue and turns each
pair into one record in the column order of
:data:`~drakkar.providers.postgres.COPY_COLUMNS`. Building records is CPU
work, so it runs where stage P runs, never on the main loop; stage M only
appends pairs.
"""

from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import dataclass

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.config.resilience import ResilienceConfig
from drakkar.providers.postgres import recorder_table_name
from drakkar.recorder.schema import EVENT_COLUMNS, EventType

# What a NUL character becomes: Postgres ``text`` refuses U+0000, and
# subprocess output can hold it.
REPLACEMENT_CHARACTER = '�'
NUL_CHARACTER = '\x00'

# The metadata key that holds the trace offsets, by event type. Any other
# event has no trace offsets.
TRACE_OFFSETS_KEYS: dict[str, str] = {
    'task_started': 'source_offsets',
    'annotation': 'offsets',
    'offload': 'offsets',
}

# The signed 64-bit range of a ``bigint`` element.
BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1

# Positions in a row tuple (``EVENT_COLUMNS`` order, the id first).
_EVENT_INDEX = EVENT_COLUMNS.index('event')
_METADATA_INDEX = EVENT_COLUMNS.index('metadata')
_STDOUT_INDEX = EVENT_COLUMNS.index('stdout')
_STDERR_INDEX = EVENT_COLUMNS.index('stderr')
_OUTPUT_INDEXES = frozenset((_STDOUT_INDEX, _STDERR_INDEX))

# What joins the head and the tail of a cut value; ``removed`` is the number
# of UTF-8 bytes left out.
OUTPUT_CUT_MARKER = '\n...[cut {removed} bytes]...\n'
# The UTF-8 codec of the cut. ``surrogatepass`` lets a value with a lone
# surrogate be measured, so the record builder never raises; an uncut value
# keeps it and fails its COPY as a value the client cannot encode.
_UTF8 = 'utf-8'
_SURROGATE_PASS = 'surrogatepass'

QueuedPair = tuple[int, tuple]


@dataclass(frozen=True)
class RecorderPostgresSettings:
    """Everything the recorder needs to write to and search its Postgres table.

    ``connection_name`` is the entry under ``connections.postgres`` and
    ``connection`` its settings; ``queue_max_bytes`` is
    ``recorder.postgres.max_queue``. ``max_output`` (bytes of one stdout or
    stderr value) and ``max_size`` (bytes of the whole table) are 0 for no
    limit.
    """

    connection_name: str
    connection: PostgresConnectionConfig
    table_prefix: str
    retention_hours: int
    store_completed_output: bool
    max_output: int
    max_size: int
    batch_rows: int
    queue_max_bytes: int
    resilience: ResilienceConfig

    @property
    def schema(self) -> str:
        """The schema that holds the table, from the connection entry."""
        return self.connection.schema_name

    @property
    def table(self) -> str:
        """The table name without its schema."""
        return recorder_table_name(self.table_prefix)

    @property
    def qualified_table(self) -> str:
        """The table name with its schema, as log lines show it."""
        return f'{self.schema}.{self.table}'


def replace_nul(text: str) -> str:
    """Return ``text`` with every NUL character replaced by U+FFFD."""
    return text.replace(NUL_CHARACTER, REPLACEMENT_CHARACTER)


def _refuse_constant(name: str) -> None:
    """Refuse ``NaN``, ``Infinity`` and ``-Infinity``: they are not JSON, and not every parser reads them."""
    raise ValueError(f'not a JSON value: {name}')


def _is_bigint(value: object) -> bool:
    """True for a plain ``int`` (never a ``bool``) in the signed 64-bit range."""
    return type(value) is int and BIGINT_MIN <= value <= BIGINT_MAX


def trace_offsets_from_metadata(event: str, metadata: str | None) -> list[int] | None:
    """Return the trace offsets of one row, or ``None``.

    ``task_started`` reads the metadata key ``source_offsets``,
    ``annotation`` and ``offload`` the key ``offsets``. The value counts only
    when the metadata is a JSON object, the key is there and its value is a
    non-empty array of JSON integers in the signed 64-bit range. Anything
    else, a parse error included, gives ``None``; it never fails a row.
    """
    key = TRACE_OFFSETS_KEYS.get(event)
    if key is None or metadata is None:
        return None
    try:
        parsed = json.loads(metadata, parse_constant=_refuse_constant)
    except (ValueError, TypeError, RecursionError):
        return None
    if not isinstance(parsed, dict):
        return None
    offsets = parsed.get(key)
    if not isinstance(offsets, list) or not offsets or not all(_is_bigint(item) for item in offsets):
        return None
    return offsets


def _plain_text(value: str) -> str:
    """Return ``value`` as a plain ``str`` with every NUL replaced.

    A ``str`` subclass (the event type is a ``StrEnum``) becomes its plain
    text, so a record holds only ``int``, ``float``, ``str`` and ``None``.
    """
    return str.replace(value, NUL_CHARACTER, REPLACEMENT_CHARACTER)


def cut_output(text: str, max_output: int) -> str:
    """Return ``text`` cut to at most ``max_output`` UTF-8 bytes of its own, or as it is.

    A value of more bytes keeps a head of at most ``max_output // 2`` bytes
    and a tail of at most the rest of the limit, each ending or starting at
    a character boundary, joined by :data:`OUTPUT_CUT_MARKER` with the number
    of bytes removed. ``0`` is no limit. The marker is not counted in the
    limit.
    """
    # Every character takes at least one byte and at most four, so the
    # length alone settles most values without encoding them.
    if max_output == 0 or len(text) * 4 <= max_output:
        return text
    encoded = text.encode(_UTF8, _SURROGATE_PASS)
    if len(encoded) <= max_output:
        return text
    head_budget = max_output // 2
    tail_budget = max_output - head_budget
    # ``ignore`` drops only the partial character at the cut edge: the bytes
    # in between are whole characters.
    head = encoded[:head_budget].decode(_UTF8, 'ignore')
    tail = encoded[len(encoded) - tail_budget :].decode(_UTF8, 'ignore')
    removed = len(encoded) - len(head.encode(_UTF8, _SURROGATE_PASS)) - len(tail.encode(_UTF8, _SURROGATE_PASS))
    return head + OUTPUT_CUT_MARKER.format(removed=removed) + tail


def _output_value(value: str | None, *, keep: bool, max_output: int) -> str | None:
    """The ``stdout`` or ``stderr`` value written: ``None`` unless kept, else plain text cut to ``max_output``."""
    if not keep or value is None:
        return None
    return cut_output(_plain_text(value), max_output)


def build_record(
    row: tuple, *, cluster: str, worker: str, instance: str, store_completed_output: bool, max_output: int
) -> tuple:
    """Return the 24 values of one row in ``COPY_COLUMNS`` order.

    The writer identity comes first, then the 20 event columns as they are
    (every text value as plain text with its NULs replaced), then the trace
    offsets. Without ``store_completed_output`` the ``stdout`` and ``stderr``
    values are ``None``, except the ``stderr`` of a ``task_failed`` row. A
    written ``stdout`` or ``stderr`` is cut by :func:`cut_output` to
    ``max_output``. No other column is ever cut: ``stdout_size`` and
    ``metadata`` keep their values.
    """
    # The two output columns are set below, so their texts are replaced and measured once.
    columns = [
        None if index in _OUTPUT_INDEXES else (_plain_text(value) if isinstance(value, str) else value)
        for index, value in enumerate(row)
    ]
    # A failed task keeps its stderr even without ``store_completed_output``:
    # it is the one text that says why the task failed.
    keeps_stderr = store_completed_output or row[_EVENT_INDEX] == EventType.TASK_FAILED
    columns[_STDOUT_INDEX] = _output_value(row[_STDOUT_INDEX], keep=store_completed_output, max_output=max_output)
    columns[_STDERR_INDEX] = _output_value(row[_STDERR_INDEX], keep=keeps_stderr, max_output=max_output)
    trace_offsets = trace_offsets_from_metadata(row[_EVENT_INDEX], row[_METADATA_INDEX])
    return (_plain_text(cluster), _plain_text(worker), _plain_text(instance), *columns, trace_offsets)


def row_text_bytes(row: tuple) -> int:
    """The size of one row as the queue bounds count it: the sum of ``len()`` of its text values."""
    return sum(len(value) for value in row if isinstance(value, str))


class PostgresQueue:
    """The P queue: ``(id, row tuple)`` pairs bounded by count and by text size.

    Stage M appends on the main loop and stage P takes chunks elsewhere, so
    every method takes the queue's lock. An append that passes either bound
    drops the oldest pairs until both hold; stage M never waits.
    """

    def __init__(self, max_rows: int, max_bytes: int) -> None:
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        # Each entry keeps its size, so a drop or a take never measures a row again.
        self._entries: deque[tuple[QueuedPair, int]] = deque()
        self._bytes = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def bytes(self) -> int:
        """The text size of the pairs in the queue."""
        with self._lock:
            return self._bytes

    def append(self, pairs: list[QueuedPair]) -> int:
        """Append ``pairs`` in order; return how many pairs were dropped to keep both bounds.

        A pair larger than the byte bound on its own is dropped alone and
        never queued, so it cannot push out the pairs already waiting. Then
        the oldest pairs go until both bounds hold.
        """
        with self._lock:
            dropped = 0
            for pair in pairs:
                size = row_text_bytes(pair[1])
                if size > self._max_bytes:
                    dropped += 1
                    continue
                self._entries.append((pair, size))
                self._bytes += size
            while self._entries and (len(self._entries) > self._max_rows or self._bytes > self._max_bytes):
                _, size = self._entries.popleft()
                self._bytes -= size
                dropped += 1
            return dropped

    def take_chunk(self, max_rows: int, max_bytes: int) -> list[QueuedPair]:
        """Pop the oldest pairs: at most ``max_rows`` and ``max_bytes`` of text, and one pair at least when any wait."""
        with self._lock:
            chunk: list[QueuedPair] = []
            chunk_bytes = 0
            while self._entries and len(chunk) < max_rows:
                pair, size = self._entries[0]
                if chunk and chunk_bytes + size > max_bytes:
                    break
                self._entries.popleft()
                self._bytes -= size
                chunk.append(pair)
                chunk_bytes += size
            return chunk

    def past_half(self) -> bool:
        """True when the queue holds at least half of either bound."""
        with self._lock:
            return 2 * len(self._entries) >= self._max_rows or 2 * self._bytes >= self._max_bytes


@dataclass
class PendingChunk:
    """A chunk taken from the P queue and not yet written or dropped.

    A retry sends exactly ``records`` again. ``final_failures`` counts the
    final COPY errors of this chunk, and ``last_sqlstate`` is the code of the
    last one.
    """

    pairs: list[QueuedPair]
    records: list[tuple]
    final_failures: int = 0
    last_sqlstate: str | None = None
