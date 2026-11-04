"""Slice downloads: the request, its checks, the source plan, the file names and the slice tables.

A slice is a SQLite file with the recorder events of one time range,
filtered, built by the worker that holds the data and streamed to the
browser (``GET /api/v1/debug/slice``). A worker-scope slice has the schema of
a recorder file (``events``, ``worker_config``, ``worker_state``); a
cluster-scope slice has the schema of a merge result (``workers``,
``events`` with ``worker_id``, ``worker_states``). Both add the ``slice`` and
``slice_sources`` tables that describe the file. Readers that do not know
these two tables ignore them.

This module holds what has no I/O: the query parameters and their checks
in a fixed order, the sources a worker can read for a scope, the plan of
sources per recorder provider (the rule against double rows), the limits,
the attachment name and the DDL. ``drakkar.slice_build`` reads the sources
and writes the file. ``tests/fixtures/slice_contract.json`` pins the
messages, the names, the filters JSON and the DDL.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from drakkar.config.recorder import PERSISTENCE_POSTGRES, PERSISTENCE_SQLITE
from drakkar.recorder.schema import EventType
from drakkar.schedule import sanitize_cluster
from drakkar.sqlitebatch import BATCH_BYTES, BATCH_ROWS

# --- Limits (constants, not config keys) ---

# The widest time range: the cap of the Postgres table; sealed files older
# than that are archives already.
SLICE_MAX_RANGE_SECONDS = 604800
SLICE_MAX_ROWS = 1_000_000
SLICE_DEFAULT_MAX_ROWS = 200_000
# The slice file on disk, checked after each batch.
SLICE_MAX_FILE_BYTES = 1024 * 1024 * 1024
# The slice database of a build with no folder of its own, built in RAM.
SLICE_MAX_RAM_BYTES = 64 * 1024 * 1024
# The whole build, checked between batches and by a progress handler.
SLICE_TIME_LIMIT_SECONDS = 60.0
# Rows per read and per insert, at most.
SLICE_BATCH_ROWS = BATCH_ROWS
# Text bytes per read batch, at most: a batch ends at whichever of the two
# limits comes first, so one batch holds at most this much (plus one row) in
# memory. The file and RAM size limits are checked after each batch, so a
# file ends at most one batch past its limit.
SLICE_CHUNK_BYTES = BATCH_BYTES
# Rows of the first Postgres fetch; later fetches are sized from the bytes
# per row seen so far, so each stays near SLICE_CHUNK_BYTES.
SLICE_PG_FIRST_FETCH_ROWS = 100
# The longest wait for the recorder flush before a build.
SLICE_FLUSH_WAIT_SECONDS = 5.0
# The longest wait for one run of the Postgres write stage before a build that
# reads the table, so fewer rows of this worker sit only in its queue.
SLICE_POSTGRES_FLUSH_SECONDS = 3.0
# Bytes of finished slices that are still being sent (each an unlinked temp
# file or a buffer in RAM), at most, per worker; a new request past it
# answers 429.
SLICE_OPEN_MAX_BYTES = 2 * 1024 * 1024 * 1024
# A download whose client takes no chunk for this long is ended, so a stalled
# client cannot hold its file for ever.
SLICE_SEND_IDLE_SECONDS = 120.0
# The ``Retry-After`` of a 429 answer.
SLICE_RETRY_AFTER_SECONDS = 5

# --- Values of the request ---

SCOPE_WORKER = 'worker'
SCOPE_CLUSTER = 'cluster'
SOURCE_MEMORY = 'memory'
SOURCE_FILES = 'files'
SOURCE_HISTORY = 'history'
SOURCE_POSTGRES = 'postgres'
COMPRESS_GZIP = 'gzip'
MAX_PARTITION = 2**31 - 1
MIN_OFFSET = -(2**63)
MAX_OFFSET = 2**63 - 1
MAX_TASK_ID_CHARS = 256
MAX_EVENTS = 40
MAX_LABEL_KEY_CHARS = 128
MAX_LABEL_VALUE_CHARS = 1024
# The epoch seconds a bound may take: from 1970 to 9999-12-31 23:59:59 UTC,
# the last second a file name can spell.
MIN_EPOCH = 0
MAX_EPOCH = 253402300799

# The names of the query parameters, as the filters JSON spells them too.
PARAM_FROM = 'from'
PARAM_TO = 'to'

Scope = Literal['worker', 'cluster']
Truncation = Literal['rows', 'bytes', 'time', 'files']
TRUNCATED_ROWS: Truncation = 'rows'
TRUNCATED_BYTES: Truncation = 'bytes'
TRUNCATED_TIME: Truncation = 'time'
TRUNCATED_FILES: Truncation = 'files'

# The kinds of a ``slice_sources`` row.
SOURCE_KIND_MEMORY = 'memory'
SOURCE_KIND_LIVE = 'live'
SOURCE_KIND_BACKLOG = 'backlog'
SOURCE_KIND_SEALED = 'sealed'
SOURCE_KIND_POSTGRES = 'postgres'

# Notes of the ``slice`` table.
NOTE_FLUSH_TIMED_OUT = 'flush_timed_out'
NOTE_FLUSH_FAILED = 'flush_failed'
NOTE_SOURCE_UNREADABLE = 'source_unreadable'
# Rows of this worker were still waiting for the Postgres write stage after
# its flush: a row that also left the memory store meanwhile is in neither
# source, so it can be missing.
NOTE_POSTGRES_QUEUE_WAITING = 'postgres_queue_waiting'

SLICE_SCHEMA_VERSION = 1

# --- Messages (pinned in tests/fixtures/slice_contract.json), in check order ---

MSG_FROM_TO = 'from and to must be finite numbers of epoch seconds'
MSG_TO_AFTER_FROM = 'to must be greater than from'
MSG_RANGE = 'the time range is at most 168h'
MSG_SCOPE = 'scope must be worker or cluster'
MSG_SOURCE = "source '{source}' is not available on this worker; available: {available}"
MSG_PARTITION = 'partition must be a whole number from 0 to 2147483647'
MSG_OFFSETS_NEED_PARTITION = 'offset_from and offset_to need partition'
MSG_OFFSETS_WHOLE = 'offset_from and offset_to must be whole numbers from -9223372036854775808 to 9223372036854775807'
MSG_OFFSETS_ORDER = 'offset_from must not be greater than offset_to'
MSG_TASK_ID = 'task_id must be 1 to 256 characters'
MSG_EVENTS_COUNT = 'events must name at most 40 event types'
MSG_EVENT = "event '{event}' is not a recorder event type"
MSG_LABEL_PAIR = 'give label_key and label_value together'
MSG_LABEL_KEY = 'label_key must be 1 to 128 characters without " or \\'
MSG_LABEL_VALUE = 'label_value must be 1 to 1024 characters'
MSG_OUTPUT = 'output must be 0 or 1'
MSG_MAX_ROWS = 'max_rows must be a whole number from 1 to 1000000'
MSG_COMPRESS = 'compress must be gzip'
# The text after "available: " when the worker has no source for the scope.
NO_SOURCE_TEXT = 'none'

# The other answers.
CLUSTER_UNAVAILABLE_ERROR = 'scope cluster needs the shared history or the Postgres table'
BUSY_ERROR = 'a slice is already being built on this worker'
OPEN_BYTES_ERROR = 'slices still being sent hold 2 GiB on this worker; wait for one to end'
POSTGRES_FAILED_ERROR = 'postgres read failed'
FAILED_ERROR_PREFIX = 'Slice failed: '
HEADER_ROWS = 'X-Drakkar-Slice-Rows'
HEADER_TRUNCATED = 'X-Drakkar-Slice-Truncated'

# Number shapes, ASCII digits only: a decimal (an exponent allowed) for the
# epoch seconds, whole numbers for the rest.
_NUMBER_RE = re.compile(r'-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?')
_WHOLE_RE = re.compile(r'[0-9]+')
_SIGNED_RE = re.compile(r'-?[0-9]+')
_EVENT_TYPES = frozenset(event.value for event in EventType)
_OUTPUT_VALUES = {'0': False, '1': True}

# --- The slice tables (pinned in the fixture) ---

SLICE_TABLE_DDL = (
    'CREATE TABLE slice (\n'
    '    id          INTEGER PRIMARY KEY CHECK (id = 1),\n'
    '    schema      INTEGER NOT NULL,\n'
    '    created_at  TEXT    NOT NULL,\n'
    '    scope       TEXT    NOT NULL,\n'
    '    worker      TEXT,\n'
    '    cluster     TEXT,\n'
    '    from_ts     REAL    NOT NULL,\n'
    '    to_ts       REAL    NOT NULL,\n'
    '    filters     TEXT    NOT NULL,\n'
    '    rows        INTEGER NOT NULL,\n'
    '    truncated   TEXT,\n'
    '    notes       TEXT    NOT NULL\n'
    ')'
)
SLICE_SOURCES_TABLE_DDL = (
    'CREATE TABLE slice_sources (\n'
    '    kind    TEXT    NOT NULL,\n'
    '    name    TEXT,\n'
    '    worker  TEXT,\n'
    '    rows    INTEGER NOT NULL\n'
    ')'
)

# The UTC time format of the bounds in an attachment name. It differs from
# the recorder file name (``%Y-%m-%d__%H_%M_%S``), so a slice name never
# matches the rule of a worker's own files.
FILE_NAME_TIME_FORMAT = '%Y-%m-%d_%H-%M-%S'


class SliceRequestError(ValueError):
    """A query parameter breaks a rule; the message is the 400 answer."""


class SliceRequest(BaseModel):
    """The checked filters of one slice request.

    Built from the query parameters by :func:`parse_slice_request`, never
    from a body. ``sources`` are the sources asked for (every available
    one when the request names none), sorted. ``events`` keeps the asked
    order without repeats.
    """

    model_config = ConfigDict(frozen=True)

    from_ts: float
    to_ts: float
    scope: Scope = SCOPE_WORKER
    sources: tuple[str, ...] = ()
    partition: int | None = None
    offset_from: int | None = None
    offset_to: int | None = None
    task_id: str | None = None
    events: tuple[str, ...] = ()
    label_key: str | None = None
    label_value: str | None = None
    output: bool = True
    max_rows: int = SLICE_DEFAULT_MAX_ROWS
    compress: str | None = None

    def filters_json(self) -> str:
        """The filters of the request as sorted compact JSON, the ``filters`` column of the ``slice`` table.

        Always ``max_rows``, ``output`` and ``sources``; every other filter
        only when given. ``events`` is sorted here.
        """
        filters: dict[str, object] = {
            'max_rows': self.max_rows,
            'output': self.output,
            'sources': list(self.sources),
        }
        optional = {
            'partition': self.partition,
            'offset_from': self.offset_from,
            'offset_to': self.offset_to,
            'task_id': self.task_id,
            'label_key': self.label_key,
            'label_value': self.label_value,
        }
        filters.update({key: value for key, value in optional.items() if value is not None})
        if self.events:
            filters['events'] = sorted(self.events)
        return json.dumps(filters, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


@dataclass(frozen=True)
class SliceSources:
    """The sources one worker can read, per scope."""

    worker: tuple[str, ...]
    cluster: tuple[str, ...]

    def for_scope(self, scope: str) -> tuple[str, ...]:
        """The sources of ``scope``."""
        return self.cluster if scope == SCOPE_CLUSTER else self.worker


def available_sources(*, provider: str, has_memory: bool, has_history: bool, has_postgres: bool) -> SliceSources:
    """The sources of each scope for a worker of recorder ``provider``.

    Worker scope: ``memory`` with an open memory store; ``files`` with
    ``sqlite``, and ``history`` when it has a shared history folder;
    ``postgres`` with ``postgres``. Cluster scope: the shared history
    (``sqlite``) or the Postgres table (``postgres``); nothing with the
    recorder off.
    """
    worker: list[str] = []
    cluster: list[str] = []
    if has_memory:
        worker.append(SOURCE_MEMORY)
    if provider == PERSISTENCE_SQLITE:
        worker.append(SOURCE_FILES)
        if has_history:
            worker.append(SOURCE_HISTORY)
            cluster.append(SOURCE_HISTORY)
    if provider == PERSISTENCE_POSTGRES and has_postgres:
        worker.append(SOURCE_POSTGRES)
        cluster.append(SOURCE_POSTGRES)
    return SliceSources(worker=tuple(sorted(worker)), cluster=tuple(sorted(cluster)))


def _epoch(params: Mapping[str, str], name: str) -> float:
    text = params.get(name)
    if text is None or not _NUMBER_RE.fullmatch(text):
        raise SliceRequestError(MSG_FROM_TO)
    value = float(text)
    if not math.isfinite(value) or not MIN_EPOCH <= value <= MAX_EPOCH:
        raise SliceRequestError(MSG_FROM_TO)
    return value


def _whole(text: str | None, low: int, high: int, message: str) -> int | None:
    """``text`` as a whole number from ``low`` to ``high``; ``None`` when absent."""
    if text is None:
        return None
    pattern = _SIGNED_RE if low < 0 else _WHOLE_RE
    if not pattern.fullmatch(text) or not low <= int(text) <= high:
        raise SliceRequestError(message)
    return int(text)


def _sources(text: str | None, available: tuple[str, ...], *, check: bool) -> tuple[str, ...]:
    if text is None:
        return available
    asked = text.split(',')
    if check:
        for source in asked:
            if source not in available:
                listed = ', '.join(available) if available else NO_SOURCE_TEXT
                raise SliceRequestError(MSG_SOURCE.format(source=source, available=listed))
    return tuple(sorted(set(asked)))


def _offsets(params: Mapping[str, str], partition: int | None) -> tuple[int | None, int | None]:
    offset_from, offset_to = params.get('offset_from'), params.get('offset_to')
    if (offset_from is not None or offset_to is not None) and partition is None:
        raise SliceRequestError(MSG_OFFSETS_NEED_PARTITION)
    low = _whole(offset_from, MIN_OFFSET, MAX_OFFSET, MSG_OFFSETS_WHOLE)
    high = _whole(offset_to, MIN_OFFSET, MAX_OFFSET, MSG_OFFSETS_WHOLE)
    if low is not None and high is not None and low > high:
        raise SliceRequestError(MSG_OFFSETS_ORDER)
    return low, high


def _events(text: str | None) -> tuple[str, ...]:
    if text is None:
        return ()
    asked = text.split(',')
    if len(asked) > MAX_EVENTS:
        raise SliceRequestError(MSG_EVENTS_COUNT)
    for event in asked:
        if event not in _EVENT_TYPES:
            raise SliceRequestError(MSG_EVENT.format(event=event))
    return tuple(dict.fromkeys(asked))


def _label(params: Mapping[str, str]) -> tuple[str | None, str | None]:
    key, value = params.get('label_key'), params.get('label_value')
    if (key is None) != (value is None):
        raise SliceRequestError(MSG_LABEL_PAIR)
    if key is None or value is None:
        return None, None
    if not 1 <= len(key) <= MAX_LABEL_KEY_CHARS or '"' in key or '\\' in key:
        raise SliceRequestError(MSG_LABEL_KEY)
    if not 1 <= len(value) <= MAX_LABEL_VALUE_CHARS:
        raise SliceRequestError(MSG_LABEL_VALUE)
    return key, value


def parse_slice_request(params: Mapping[str, str], sources: SliceSources) -> SliceRequest:
    """Check the query parameters in the pinned order; the first rule broken raises :class:`SliceRequestError`.

    ``sources`` are what this worker can read. A cluster-scope request on
    a worker with no cluster source skips the source check: the caller
    answers 409 for it after every other check. Unknown parameters (the
    ``token`` of a download link) are ignored.
    """
    from_ts = _epoch(params, PARAM_FROM)
    to_ts = _epoch(params, PARAM_TO)
    if to_ts <= from_ts:
        raise SliceRequestError(MSG_TO_AFTER_FROM)
    if to_ts - from_ts > SLICE_MAX_RANGE_SECONDS:
        raise SliceRequestError(MSG_RANGE)
    scope = params.get('scope', SCOPE_WORKER)
    if scope not in (SCOPE_WORKER, SCOPE_CLUSTER):
        raise SliceRequestError(MSG_SCOPE)
    available = sources.for_scope(scope)
    chosen = _sources(params.get('sources'), available, check=scope == SCOPE_WORKER or bool(available))
    partition = _whole(params.get('partition'), 0, MAX_PARTITION, MSG_PARTITION)
    offset_from, offset_to = _offsets(params, partition)
    task_id = params.get('task_id')
    if task_id is not None and not 1 <= len(task_id) <= MAX_TASK_ID_CHARS:
        raise SliceRequestError(MSG_TASK_ID)
    events = _events(params.get('events'))
    label_key, label_value = _label(params)
    output_text = params.get('output', '1')
    if output_text not in _OUTPUT_VALUES:
        raise SliceRequestError(MSG_OUTPUT)
    max_rows = _whole(params.get('max_rows'), 1, SLICE_MAX_ROWS, MSG_MAX_ROWS)
    compress = params.get('compress')
    if compress is not None and compress != COMPRESS_GZIP:
        raise SliceRequestError(MSG_COMPRESS)
    return SliceRequest(
        from_ts=from_ts,
        to_ts=to_ts,
        scope=SCOPE_CLUSTER if scope == SCOPE_CLUSTER else SCOPE_WORKER,
        sources=chosen,
        partition=partition,
        offset_from=offset_from,
        offset_to=offset_to,
        task_id=task_id,
        events=events,
        label_key=label_key,
        label_value=label_value,
        output=_OUTPUT_VALUES[output_text],
        max_rows=SLICE_DEFAULT_MAX_ROWS if max_rows is None else max_rows,
        compress=compress,
    )


# --- The plan of sources (the rule against double rows) ---


@dataclass(frozen=True)
class SlicePlan:
    """What one build reads.

    ``flush``: ask the recorder for one flush (memory store and file) first.
    ``flush_postgres``: a flush of the memory store, then one bounded run of
    the Postgres write stage. ``flush_memory``: a flush of the memory store
    only, so the events still waiting in the recorder are in the store.
    ``memory``: the memory store, read first. ``sealed``: own sealed files
    (worker scope) or every sealed file of the cluster folder (cluster
    scope). ``backlog`` and ``live``: the own unsealed files. ``postgres``:
    the table, read last. ``dedupe_memory``: a row of a later source that the
    memory store gave already is left out (the same process and id).
    """

    flush: bool = False
    flush_postgres: bool = False
    flush_memory: bool = False
    sealed: bool = False
    backlog: bool = False
    live: bool = False
    postgres: bool = False
    memory: bool = False
    dedupe_memory: bool = False


def plan_slice(request: SliceRequest, provider: str) -> SlicePlan:
    """The sources a build reads for ``request`` on a worker of recorder ``provider``.

    Each row is written once. ``sqlite``, worker scope: flush, then the files
    only; after the flush every memory row is in the live file too, so
    ``memory`` adds nothing when ``files`` is asked for. ``memory`` without
    ``files`` reads the memory store first (after a flush of the recorder
    into it), then the own sealed files, leaving out the rows the store gave.
    ``postgres``: one bounded run of the write stage, then the memory store,
    then the table, leaving out the rows the store gave. Recorder off: the
    memory store. Cluster scope: the sealed files of the cluster folder
    (``sqlite``) or the table rows of the cluster (``postgres``).
    """
    asked = set(request.sources)
    if request.scope == SCOPE_CLUSTER:
        postgres = SOURCE_POSTGRES in asked
        return SlicePlan(flush_postgres=postgres, sealed=SOURCE_HISTORY in asked, postgres=postgres)
    files = SOURCE_FILES in asked
    memory = SOURCE_MEMORY in asked and not files
    if provider == PERSISTENCE_SQLITE:
        history = SOURCE_HISTORY in asked
        return SlicePlan(
            flush=files,
            flush_memory=memory,
            sealed=history,
            backlog=files,
            live=files,
            memory=memory,
            dedupe_memory=memory and history,
        )
    postgres = SOURCE_POSTGRES in asked
    return SlicePlan(
        flush_postgres=postgres,
        flush_memory=memory and not postgres,
        postgres=postgres,
        memory=memory,
        dedupe_memory=memory and postgres,
    )


# --- The attachment name ---


def _name_bound(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime(FILE_NAME_TIME_FORMAT)


def slice_file_name(
    *,
    scope: str,
    worker: str,
    cluster: str,
    from_ts: float,
    to_ts: float,
    truncated: str | None,
    compress: str | None,
) -> str:
    """The attachment name of a slice.

    ``<worker>-slice-<from>__<to>.db`` for a worker-scope slice,
    ``<cluster>-cluster-slice-<from>__<to>.db`` for a cluster-scope slice
    (the sanitized cluster, ``default`` when it has none), ``-partial`` before
    ``.db`` when a limit cut it, ``.gz`` after it with ``compress=gzip``. The
    worker name is already in its sanitized form (a load rule).
    """
    owner = f'{sanitize_cluster(cluster)}-cluster' if scope == SCOPE_CLUSTER else worker
    partial = '-partial' if truncated is not None else ''
    suffix = '.db.gz' if compress == COMPRESS_GZIP else '.db'
    return f'{owner}-slice-{_name_bound(from_ts)}__{_name_bound(to_ts)}{partial}{suffix}'
