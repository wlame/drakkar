"""Filesystem sink — appends JSON lines to files.

Each FilePayload's data field is serialized via model_dump_json() and
appended as a newline-terminated line (JSONL format) to the file at
payload.path. Creates the file if it doesn't exist. Raises an error
if the parent directory is missing or not writable.
"""

import os
import time
from pathlib import Path

import structlog

from drakkar.config import FileSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import FilePayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class FileSink(BaseSink[FilePayload]):
    """Appends JSONL lines to files on the local filesystem.

    Each FilePayload specifies its own output path. The framework
    serializes payload.data via model_dump_json(), appends a newline,
    and writes it to the file (creating the file if needed).

    Raises FileNotFoundError if the parent directory doesn't exist,
    and PermissionError if the directory isn't writable.
    """

    sink_type = 'filesystem'

    # ``FileSink.deliver`` opens each target file with ``open(path, 'a')``
    # — APPEND mode. A retried batch after a partially-succeeded write
    # would duplicate records in the file (the previous lines are already
    # on disk, the retry appends them again). Append-only JSONL without a
    # dedup key is therefore NOT idempotent. We keep ``idempotent=False``
    # so a transient IO error bubbles to ``on_delivery_error`` rather
    # than silently doubling on retry. A user who implements a custom
    # write-replace file sink (e.g., atomic-rename overwrite) may
    # subclass and set ``idempotent = True``.
    idempotent = False

    def __init__(self, name: str, config: FileSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._base_path = config.base_path

    async def connect(self) -> None:
        """Validate base_path if configured, otherwise no-op."""
        if self._config.base_path:
            base = Path(self._config.base_path)
            if not base.is_dir():
                raise FileNotFoundError(f'FileSink base_path does not exist: {base}')
        await logger.ainfo(
            'file_sink_connected',
            category='sink',
            sink_name=self._name,
            base_path=self._config.base_path or '(none)',
        )

    async def deliver(self, payloads: list[FilePayload]) -> None:
        """Append each payload as a JSONL line to its target file.

        Creates the file if it doesn't exist. Raises FileNotFoundError
        if the parent directory is missing, PermissionError if not writable.
        """
        if not payloads:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            for payload in payloads:
                if self._base_path:
                    resolved = os.path.realpath(os.path.join(self._base_path, payload.path))
                    base_real = os.path.realpath(self._base_path)
                    if not resolved.startswith(base_real + os.sep) and resolved != base_real:
                        raise ValueError(
                            f'Path traversal detected: {payload.path!r} resolves outside base_path {self._base_path!r}'
                        )
                    path = Path(resolved)
                else:
                    path = Path(payload.path)
                if not path.parent.is_dir():
                    raise FileNotFoundError(f'Parent directory does not exist: {path.parent}')
                line = payload.data.model_dump_json() + '\n'
                with open(path, 'a') as f:
                    f.write(line)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    async def close(self) -> None:
        """No-op — filesystem doesn't hold persistent connections."""
        pass
