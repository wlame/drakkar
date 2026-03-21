"""Pydantic models for the ripgrep search pipeline.

Defines the input message schema (what comes from Kafka) and all output
schemas used by different sinks. Each output model represents data
shaped for a specific purpose — the handler decides which sinks receive which.
"""

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    """Input message schema: what to search for.

    Consumed from the Kafka source topic. The framework auto-deserializes
    this from message bytes when used as a type parameter on the handler.
    """

    request_id: str
    pattern: str
    file_path: str
    repeat: int = 1


class SearchResult(BaseModel):
    """Full search result — sent to Kafka output topic and MongoDB archive."""

    request_id: str
    pattern: str
    file_path: str
    repeat: int
    match_count: int
    duration_seconds: float
    matches: list[str] = Field(default_factory=list)


class SearchSummary(BaseModel):
    """Compact summary — sent to Postgres (metrics row) and Redis (cache)."""

    request_id: str
    pattern: str
    match_count: int
    duration_seconds: float


class SearchNotification(BaseModel):
    """Webhook notification — sent to HTTP sink for high match counts only."""

    request_id: str
    pattern: str
    match_count: int
    message: str
