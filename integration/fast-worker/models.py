"""Models for the symbol-count fast pipeline."""

from pydantic import BaseModel


class CountRequest(BaseModel):
    """Input: same Kafka message as the search pipeline."""

    request_id: str
    pattern: str
    file_path: str
    repeat: int = 1


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
