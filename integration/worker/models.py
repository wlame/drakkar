"""Pydantic models for the ripgrep search pipeline.

Defines the input message schema (what comes from Kafka) and all output
schemas used by different sinks. Each output model represents data
shaped for a specific purpose — the handler decides which sinks receive which.

FAN-OUT DEMO: one SearchRequest lists 1-3 patterns and 1-2 file_paths.
arrange() expands this into len(patterns) x len(file_paths) executor tasks.
on_task_complete may emit per-task detail to Kafka. on_message_complete
fires ONCE after all the per-request tasks complete, receiving a
MessageGroup — and emits the aggregated summary (single row per request).
"""

from pydantic import BaseModel, Field


class RankRequest(BaseModel):
    """Webapp input model — synchronous HTTP rank request.

    Sent by the ``load_generator`` service via POST /process. Carries a
    framework-validated ``request_id`` (string id the generator stamps onto
    each request) and a single ``score`` integer the handler turns into
    one or two ripgrep tasks. Kept intentionally tiny so the integration
    scenario focuses on the HTTP plumbing rather than payload shape.
    """

    request_id: str
    score: int = Field(ge=0)


class RankResponse(BaseModel):
    """Webapp output model — what the handler returns to the HTTP client.

    The framework wraps this model under ``"result"`` in the JSON envelope
    (see ``docs/webapp.md``). ``client_hint`` mirrors any non-default
    behaviour selected by ``arrange_http_request`` so the HTTP caller can
    correlate the priority class assigned to its request.
    """

    request_id: str
    result: int
    client_hint: str = ''
    succeeded_tasks: int = 0
    failed_tasks: int = 0


class SearchRequest(BaseModel):
    """Input message schema — a single request to search.

    One request carries MULTIPLE patterns and MULTIPLE files; the handler
    expands it into a Cartesian product of subprocess tasks and aggregates
    the outcomes back into a single per-request summary.
    """

    request_id: str
    # 1-3 patterns, 1-2 file paths — the handler produces one executor task
    # per (pattern, file_path) pair. Minimum len=1 enforced; upper bound is
    # a demo convention (not a framework limit).
    patterns: list[str] = Field(min_length=1, max_length=3)
    file_paths: list[str] = Field(min_length=1, max_length=2)
    repeat: int = 1


class SearchResult(BaseModel):
    """Per-task result — one row per (pattern, file_path) subprocess task.

    Emitted by on_task_complete. Destined for the Kafka output topic and
    the MongoDB archive: fine-grained, indexable per-task detail.
    """

    request_id: str
    pattern: str
    file_path: str
    repeat: int
    match_count: int
    duration_seconds: float
    matches: list[str] = Field(default_factory=list)


class SearchSummary(BaseModel):
    """Per-task compact summary — sent to Postgres + Redis from on_task_complete."""

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


class SearchAggregate(BaseModel):
    """Per-REQUEST aggregate — emitted exactly once by on_message_complete.

    Rolls up every (pattern, file_path) task outcome for this request into
    one record. Scalar-only fields so the same model works cleanly for
    both Kafka and Postgres sinks. Downstream consumers who care about
    request-level outcomes don't need to stitch together N per-task rows.
    """

    request_id: str
    # Source message identity for traceability
    partition: int
    offset: int
    # Fan-out shape
    total_tasks: int
    succeeded_tasks: int
    failed_tasks: int
    replaced_tasks: int
    # Roll-up statistics over successful subprocess outcomes
    total_matches: int
    max_matches: int
    duration_seconds: float


# ---------------------------------------------------------------------
# Models for the write-operation demo in on_message_complete.
#
# Each one exists because a sink operation needs a specific SHAPE: an
# upsert needs the row, an update needs the columns to set AND a
# predicate, a named statement needs its bound parameters.
# ---------------------------------------------------------------------


class RequestSummary(BaseModel):
    """The upserted row in `request_summaries`, keyed on request_id.

    At-least-once delivery means a redelivered request would duplicate this
    row under a plain INSERT; the upsert converges instead.
    """

    request_id: str
    total_matches: int
    succeeded_tasks: int
    failed_tasks: int
    duration_seconds: float
    # Only ever set by the INSERT half of the upsert — see the payload's
    # update_columns, which deliberately omits it so a redelivery cannot
    # un-send a webhook.
    notified: bool = False


class RequestNotified(BaseModel):
    """The SET half of the UPDATE that records a webhook was sent."""

    notified: bool = True


class RequestKey(BaseModel):
    """The WHERE half of that UPDATE.

    An update's predicate is required and may never be empty — an empty
    one would rewrite every row in the table.
    """

    request_id: str


class PatternStatsParams(BaseModel):
    """Bound parameters for the `bump_pattern_stats` named statement.

    `:matches` appears twice in the SQL and binds one value.
    """

    pattern: str
    matches: int
