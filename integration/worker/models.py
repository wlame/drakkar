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

from typing import Any

from pydantic import BaseModel, Field, SecretStr

from drakkar import probe_field
from drakkar.probe import Column, Detail, Element, Link


class AppConfig(BaseModel):
    """User-defined application config — the app-config feature demo.

    Loaded by the framework from drakkar.yaml's reserved ``app:`` section
    plus ``RGAPP_*`` env overrides (see the handler's ``app_config_model``
    / ``app_env_prefix`` class attributes and docs/app-config.md), and
    read as ``self.app_config`` in hooks. Rendered as the ``Application``
    group in the Debug UI config reference, with the SecretStr masked.
    """

    priority_match_threshold: int = Field(
        default=20,
        description='Requests whose total match count exceeds this are treated as priority notifications.',
    )
    scoring_service_url: str = Field(
        default='http://localhost:9000/score',
        description='Endpoint a real deployment would send match scores to (demo value, never called).',
    )
    scoring_api_key: SecretStr = Field(
        default=SecretStr(''),
        description='Credential for the scoring endpoint; masked in the Debug UI config reference.',
    )


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


# ---------------------------------------------------------------------
# Probe user-details example (see docs/probe-user-details.md). These
# row models and the details model below only fill in during a Message
# Probe replay in the debug UI — probe.set/append/update are no-ops in
# production, so wiring this into the handler costs nothing on the hot
# path.
# ---------------------------------------------------------------------


class CacheLookupRow(BaseModel):
    """One (pattern, file_path) cache decision made during arrange()."""

    cache_key: str
    tier: str  # 'memory' | 'sqlite' | 'miss'
    decision: str  # 'precomputed' | 'subprocess'
    fan_in: int
    outcome: str  # 'hit' | 'miss' — same decision as `decision`, in badge-friendly words


class MatchAnalysisRow(BaseModel):
    """Per-task match stats recorded in on_task_complete()."""

    pattern: str
    file: str
    matches: int
    distinct_lines: int
    longest_line: int
    duration_ms: float
    source: str  # 'cache' | 'subprocess'
    bytes_scanned: int  # len(subprocess stdout), UTF-8 encoded
    # Detail-panel-only fields (not shown as table columns): synthetic but
    # plausible-looking diagnostic junk, to exercise the popup's keyvalue
    # and table elements without inventing a second real data source.
    scan_meta: dict[str, Any]
    sample_lines: list[dict[str, Any]]


class PatternRankRow(BaseModel):
    """One pattern's share of a request's total matches."""

    rank: int
    pattern: str
    matches: int
    share_pct: float


class SinkDecisionRow(BaseModel):
    """One conditional-sink decision made in on_message_complete()."""

    sink: str
    destination: str
    fired: str  # 'yes' | 'no'
    reason: str


class RipgrepProbeDetails(BaseModel):
    """User-defined Message Probe tab for `RipgrepHandler`.

    See docs/probe-user-details.md for the feature itself, and
    drakkar/probe.py for the exact `probe_field()` / `probe.set()` /
    `probe.append()` / `probe.update()` semantics.
    """

    # Arrange: how this window's messages collapsed into tasks, and
    # what the two-tier cache decided for each (pattern, file_path) pair.
    window_shape: str | None = probe_field(
        section='Arrange',
        view='string',
        default=None,
        hint='How this arrange() window collapsed messages into distinct (pattern, file) pairs.',
    )
    stage_counters: dict[str, int] = probe_field(section='Arrange', view='keyvalue', default_factory=dict)
    cache_lookups: list[CacheLookupRow] = probe_field(
        section='Arrange',
        view='table',
        default_factory=list,
        # A subset of CacheLookupRow's fields — 'decision' is dropped since
        # 'outcome' already carries the same information in badge-friendly form.
        columns={
            'cache_key': Column(link_template='{file_browser}/cache/{value}', hint='Cache key {value}'),
            'tier': Column(),
            'outcome': Column(badge_colors={'hit': 'green', 'miss': 'yellow', '*': 'gray'}),
            'fan_in': Column(),
        },
    )

    # Results: per-task match shape, and each request's per-pattern breakdown.
    match_analysis: list[MatchAnalysisRow] = probe_field(
        section='Results',
        view='table',
        default_factory=list,
        columns={
            'pattern': Column(),
            'file': Column(link_template='{code_review}/blob/main/{value}'),
            'matches': Column(renderer='matchBar', hint='{value} matches found'),
            'duration_ms': Column(format='duration_ms'),
            'bytes_scanned': Column(format='bytes'),
            'source': Column(),
        },
        detail=Detail(
            title='Match analysis: {row.pattern} in {row.file}',
            elements=[
                Element(field='source', view='string', label='Result source'),
                Element(field='scan_meta', view='keyvalue', label='Scan metadata'),
                Element(field='sample_lines', view='table', label='Sample lines'),
                Element(
                    view='links',
                    links=[
                        Link(label='Code review', template='{code_review}/blob/main/{row.file}'),
                        Link(label='Build farm job', template='{build_farm}/jobs/{row.pattern}'),
                    ],
                ),
            ],
        ),
    )
    pattern_ranking: list[PatternRankRow] = probe_field(section='Results', view='table', default_factory=list)
    # A per-request verdict, at a glance — filled in on_message_complete()
    # from the same total_matches the pattern ranking above rolls up.
    scan_verdict: str = probe_field(
        section='Results',
        view='badge',
        badge_colors={'clean': 'green', 'matched': 'blue', 'noisy': 'yellow', '*': 'gray'},
        default='',
    )
    # Custom-renderer scalar: a small JSON payload rendered by patternChip()
    # in custom-renderers.js rather than any built-in view.
    top_pattern_chip: dict = probe_field(
        section='Results',
        view='custom',
        renderer='patternChip',
        default_factory=dict,
    )

    # Routing: which conditional sinks fired for this request, and why.
    sink_decisions: list[SinkDecisionRow] = probe_field(section='Routing', view='table', default_factory=list)
    thresholds: dict = probe_field(section='Routing', view='dict', default_factory=dict)
