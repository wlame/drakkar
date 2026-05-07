"""Flight recorder — public façade.

The implementation lives in :mod:`drakkar.recorder.core`. This module
re-exports the public API so call sites using ``from drakkar.recorder import ...``
continue working without changes.
"""

from __future__ import annotations

# Re-exported so ``patch('drakkar.recorder.socket.socket', ...)`` in tests
# keeps working — patching socket.socket via any reference to the module
# replaces the class globally, which is what the test needs.
import socket  # noqa: F401

from drakkar.recorder.core import EventRecorder, logger  # noqa: F401  (re-exported for tests)
from drakkar.recorder.helpers import (
    _HAS_ORJSON,  # noqa: F401  (re-exported for tests)
    _SECRET_ENV_PATTERNS,  # noqa: F401  (re-exported for tests)
    detect_worker_ip,  # noqa: F401  (re-exported for tests)
    encode_json,  # noqa: F401  (re-exported for tests)
    encode_json_str,  # noqa: F401  (re-exported for tests)
    format_dt,  # noqa: F401  (re-exported for tests)
    list_db_files,  # noqa: F401  (re-exported for tests)
    live_link_path,  # noqa: F401  (re-exported for tests)
    make_db_path,  # noqa: F401  (re-exported for tests)
    open_reader,  # noqa: F401  (re-exported for tests)
    sanitize_env_value,  # noqa: F401  (re-exported for tests)
)
from drakkar.recorder.schema import (
    _LABEL_TRACE_QUERY,  # noqa: F401  (re-exported for tests)
    _TRACE_QUERY,  # noqa: F401  (re-exported for tests)
    SCHEMA_EVENTS,  # noqa: F401  (re-exported for tests)
    SCHEMA_WORKER_CONFIG,  # noqa: F401  (re-exported for tests)
    SCHEMA_WORKER_STATE,  # noqa: F401  (re-exported for tests)
    WEBAPP_REQUIRED_EVENT_COLUMNS,  # noqa: F401  (re-exported for tests)
    RecorderSchemaError,
)

__all__ = ['EventRecorder', 'RecorderSchemaError']
