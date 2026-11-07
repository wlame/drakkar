"""Models for the symbol-count fast pipeline."""

from pydantic import BaseModel, model_validator


class CountRequest(BaseModel):
    """Input: the same Kafka message as the search pipeline.

    The producer sends the search pipeline's shape, a list of 1 to 2
    ``file_paths``; the harness scenarios send one ``file_path``. Both are
    read, and the handler counts each path in its own task. A message with
    neither fails the parse.
    """

    request_id: str
    file_path: str | None = None
    file_paths: list[str] = []
    repeat: int = 1

    @model_validator(mode='after')
    def _needs_a_path(self) -> 'CountRequest':
        if not self.paths():
            raise ValueError('a count request needs file_path or file_paths')
        return self

    def paths(self) -> list[str]:
        """The paths to count, in message order, without repeats."""
        given = ([self.file_path] if self.file_path else []) + self.file_paths
        return list(dict.fromkeys(given))


class CountResult(BaseModel):
    """Per-message output: request_id, path, and total symbol count."""

    request_id: str
    file_path: str
    symbol_count: int


class WindowSummary(BaseModel):
    """Per-window aggregate: emitted by on_window_complete().

    One of these is produced for every completed arrange/collect window,
    containing aggregate statistics over all the per-message CountResult
    values in that window.
    """

    worker_id: str
    window_id: int
    file_count: int
    total_chars: int
    avg_chars: float
    max_chars: int
    min_chars: int
    # First/last request id in the window — useful for correlating the
    # summary back to individual per-message results.
    first_request_id: str
    last_request_id: str
