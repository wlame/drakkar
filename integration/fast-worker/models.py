"""Models for the symbol-count fast pipeline."""

from pydantic import BaseModel


class CountRequest(BaseModel):
    """Input: same Kafka message as the search pipeline."""

    request_id: str
    pattern: str
    file_path: str
    repeat: int = 1


class CountResult(BaseModel):
    """Output: request_id, path, and total symbol count."""

    request_id: str
    file_path: str
    symbol_count: int
