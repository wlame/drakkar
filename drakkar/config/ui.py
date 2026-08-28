"""UI configuration: the server, the flight recorder, and the release bundle.

Also holds the timeline tuning models (color rules, label roles, history
depth) and the consume-pause presets, which are UI-driven controls.
"""

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Archives are the only recorder artifact nothing else reclaims, so the
# default has to be a horizon rather than "forever": a worker left alone
# on its defaults must not be able to fill the disk it runs on. 30 days
# keeps a month of history queryable while covering every archive window
# up to 360 hours — wider windows have to name their own retention.
DEFAULT_ARCHIVE_RETENTION_DAYS = 30


class UIRecorderConfig(BaseModel):
    """Flight-recorder persistence settings — the UI's data store.

    Set ``db_dir: ""`` to run without any SQLite files on disk.

    Granular persistence flags (all require ``db_dir`` to be set):
    - ``store_events``: write processing events to the ``events`` table.
    - ``store_config``: write worker config to ``worker_config`` (enables autodiscovery).
    - ``store_state``: periodically dump counters to ``worker_state``.

    Any combination is valid — e.g. ``store_config=true`` with everything
    else ``false`` gives autodiscovery without event or state logging.
    """

    @model_validator(mode='after')
    def _validate_archive_window_vs_rotation(self) -> 'UIRecorderConfig':
        """The archive pass must see every rotated file before it ages out.

        If ``archive_window_hours`` were shorter than ``rotation_interval_hours``,
        a file could rotate out and be gone before the next archive pass ever
        looked at it.
        """
        if self.archive_window_hours < self.rotation_interval_hours:
            raise ValueError(
                f'ui.recorder.archive_window_hours ({self.archive_window_hours}) must be >= '
                f'ui.recorder.rotation_interval_hours ({self.rotation_interval_hours})'
            )
        return self

    @model_validator(mode='after')
    def _validate_archive_retention_vs_window(self) -> 'UIRecorderConfig':
        """Retention must outlive the window an archive is born into.

        A window is archived only once it ended a full window ago, so an
        archive of a just-due window already describes data one window old.
        Retention shorter than two windows would delete such an archive in
        the very pass that created it.

        This does not (and cannot) stop a backlog window from being
        archived and expired in one pass: a window whose data predates the
        retention horizon is past due for both. That is retention working
        as asked.
        """
        if self.archive_retention_days and self.archive_retention_days * 24 < 2 * self.archive_window_hours:
            default_note = (
                f' The value is the {DEFAULT_ARCHIVE_RETENTION_DAYS}-day default, so raise it (or set 0 to keep '
                f'archives forever) to go with a window this wide.'
                if self.archive_retention_days == DEFAULT_ARCHIVE_RETENTION_DAYS
                else ''
            )
            raise ValueError(
                f'ui.recorder.archive_retention_days ({self.archive_retention_days}) must cover at least two '
                f'archive windows, or be 0 to keep archives forever: archive_retention_days * 24 must be >= '
                f'2 * ui.recorder.archive_window_hours ({self.archive_window_hours}).{default_note}'
            )
        return self

    db_dir: str = Field(
        default='/tmp',
        description=(
            'Directory for the recorder SQLite files. Empty runs memory-only: no files, no '
            'history — the live WebSocket view keeps working. Use a shared filesystem for '
            'cross-worker autodiscovery and merge.'
        ),
    )
    store_events: bool = Field(
        default=True,
        description=(
            'Write processing events to the events table. Off, the live WebSocket view still '
            'streams, but History, Trace, and task queries return nothing.'
        ),
    )
    store_config: bool = Field(
        default=True,
        description=(
            'Write the worker config snapshot to the worker_config table — what makes the '
            'worker discoverable by peers sharing the same db_dir.'
        ),
    )
    store_state: bool = Field(
        default=True,
        description=(
            'Periodically snapshot worker counters (uptime, partitions, pool utilization, '
            'queue depth) to the worker_state table.'
        ),
    )
    state_sync_interval_seconds: int = Field(
        default=10,
        ge=1,
        description=(
            'Seconds between worker_state snapshots — also the heartbeat cadence worker liveness is judged by.'
        ),
    )
    rotation_interval_hours: int = Field(default=1, ge=1, description='How often to roll over to a new SQLite file.')
    archive_enabled: bool = Field(
        default=True,
        description='Archive rotated-out DB files instead of deleting them outright.',
    )
    archive_window_hours: int = Field(
        default=24,
        ge=1,
        description='How much recent history the archive pass keeps queryable; must be >= rotation_interval_hours.',
    )
    archive_retention_days: int = Field(
        default=DEFAULT_ARCHIVE_RETENTION_DAYS,
        ge=0,
        description=(
            f'How long archived files are kept before deletion. Defaults to '
            f'{DEFAULT_ARCHIVE_RETENTION_DAYS} days so a worker cannot fill its disk unattended. '
            f'0 = keep archives forever (a startup warning names the choice); any other value must '
            f'cover at least two archive windows (archive_retention_days * 24 >= 2 * archive_window_hours).'
        ),
    )
    dbstats_warm_interval_seconds: int = Field(
        default=60,
        ge=5,
        description=(
            'How often the background warmer sweeps db_dir, computing '
            'statistics for database files the .dbstats cache does not '
            'know yet and purging entries for deleted files. Cheap when '
            'everything is already cached — one directory listing plus '
            'one small SELECT.'
        ),
    )
    dbstats_inline_scan_limit: int = Field(
        default=4,
        ge=0,
        description=(
            'How many cold (uncached) database files one /api/v1/debug/databases '
            'request may fully scan inline. Files beyond the cap return '
            'immediately with stats_pending=true and fill in as the warmer '
            'catches up. 0 = requests never scan; matters only on a cold '
            'cache (first boot over a pre-existing directory).'
        ),
    )
    store_output: bool = Field(
        default=True,
        description='Store subprocess stdout/stderr content in task events. Off drops output content from all of them.',
    )
    store_stdin: bool = Field(
        default=False,
        description=(
            "Store each task's stdin content (capped at stdin_max_bytes) in the "
            'task_started event metadata, so the debug UI can show exactly what a '
            'task consumed. Off by default: on a high-fan-out workload stdin is '
            'the largest payload the recorder would write. Failed tasks always '
            'store their stdin (capped) on the task_failed event, regardless of '
            'this flag.'
        ),
    )
    stdin_max_bytes: int = Field(
        default=65536,
        ge=0,
        description=(
            'Byte cap for stored stdin content (store_stdin, and the always-on '
            'failed-task capture). 0 = unlimited. Truncation is flagged as '
            'stdin_truncated in the event metadata.'
        ),
    )
    flush_interval_seconds: int = Field(
        default=5,
        ge=1,
        description='Seconds between recorder buffer flushes to SQLite.',
    )
    max_buffer: int = Field(
        default=50_000,
        ge=1000,
        description=(
            'In-memory event buffer capacity. When full, the oldest events are evicted and '
            'counted in drakkar_recorder_dropped_events_total.'
        ),
    )
    # Maximum consecutive ``OperationalError`` failures tolerated on a single
    # batch before the recorder gives up and drops it. On each failure the
    # batch is re-queued at the front of the buffer so the next flush tick
    # retries it; after this many attempts the batch is discarded and the
    # ``drakkar_recorder_flush_batches_dropped_total`` counter ticks. Default
    # 3 matches the cache engine's retry budget and keeps a persistent DB
    # outage from leaking the buffer indefinitely.
    max_flush_retries: int = Field(
        default=3,
        ge=1,
        description=(
            'Consecutive failed flush attempts tolerated per batch before it is dropped '
            '(counted in drakkar_recorder_flush_batches_dropped_total).'
        ),
    )
    event_min_duration_ms: int = Field(
        default=0,
        ge=0,
        description='Minimum task duration (ms) to persist a task event at all. 0 persists everything.',
    )
    output_min_duration_ms: int = Field(
        default=500,
        ge=0,
        description=(
            'Minimum task duration (ms) to include args/stdout/stderr in the persisted event. '
            'Faster tasks keep their row without output data.'
        ),
    )
    # Handler annotations — diagnostic records a handler attaches to a window,
    # message, or task from inside a hook (see drakkar.annotations). They are
    # stored as ordinary rows in the events table, alongside everything else
    # recorder rotation carries into the next file.
    #
    # ``0`` disables each byte cap. The two caps guard different resources and
    # are deliberately not one setting: annotation_max_bytes rejects a single
    # unreasonable payload, while annotation_max_bytes_per_call bounds what one
    # hook invocation can add to the DB in total — without the latter, a handler
    # annotating every message of a wide window can flood the events table
    # with low-value rows.
    annotations_enabled: bool = Field(
        default=True,
        description='Accept handler annotations (self.annotate(...)) and write them to the events table.',
    )
    annotation_max_bytes: int = Field(
        default=16_384,
        ge=0,
        description='Byte cap for one annotation payload; larger payloads are dropped whole. 0 disables the cap.',
    )
    annotation_max_bytes_per_call: int = Field(
        default=262_144,
        ge=0,
        description=(
            'Total annotation bytes one hook invocation may write; further payloads past it '
            'are dropped. 0 disables the cap.'
        ),
    )
    # Cap on the payload copy written to the warning log when a record is
    # dropped. Higher than the row itself is pointless; lower is fine. Log
    # lines usually ship to a metered aggregator, so an uncapped copy can cost
    # more than the row it replaced.
    annotation_log_max_bytes: int = Field(
        default=2048,
        ge=0,
        description=(
            'Cap on the payload copy included in the warning log when an annotation is dropped. 0 disables the cap.'
        ),
    )


class UIReleaseConfig(BaseModel):
    """Decoupled drakkar-ui bundle fetching settings.

    The UI ships as its own versioned bundle (the separate drakkar-ui repo,
    published to GitHub Releases) so every backend on a host serves the same
    UI and looks identical. When ``enabled``, the worker resolves that bundle
    through :mod:`drakkar.uihost` (cache → fetch) and serves it.

    Default-ON with an update check: on startup the worker resolves the
    latest release, or serves the shared cache. A fetch failure is never
    fatal — release tags are immutable, so a bundle downloaded once serves
    from the cache on every later start, offline ones included. There is no
    HTML fallback baked into the package: with nothing cached the worker
    runs API-only and page requests answer 503 naming how to supply a
    bundle.
    """

    enabled: bool = Field(
        default=True,
        description=(
            'Resolve and serve the drakkar-ui bundle. Off means no UI at all '
            '(no fetch, no cache read): the JSON API, the health probes and '
            'the event WebSocket keep working, and page requests answer 503.'
        ),
    )
    repo: str = Field(
        default='wlame/drakkar-ui',
        description=(
            'The "owner/name" GitHub repo that publishes UI bundles. '
            'Empty disables fetching — only an already-cached bundle is '
            'served, which is how an air-gapped deployment pins itself to a '
            'bundle staged with the drakkar-ui CLI.'
        ),
    )
    pinned_version: str = Field(
        default='',
        description=(
            'Known-good UI release tag this backend is built against '
            '(e.g. "v1.2.0"); the contract is API-major compatible. Empty '
            'means "no pinned version".'
        ),
    )
    cache_dir: str = Field(
        default='',
        description=(
            'Bundle cache root override. Empty uses the per-user cache dir '
            '($XDG_CACHE_HOME/drakkar/ui, falling back to ~/.cache/drakkar/ui '
            '— the conventional per-user cache location, so co-located '
            'workers share one cache).'
        ),
    )
    check_update: bool = Field(
        default=True,
        description=(
            'Resolve the latest release tag on startup instead of only the '
            'pinned version (the "check for a new version" toggle). Already-'
            'cached versions are never re-downloaded — release tags are '
            'immutable.'
        ),
    )

    @field_validator('repo')
    @classmethod
    def _validate_repo(cls, v: str) -> str:
        """A non-empty repo must look like a GitHub ``owner/name`` slug."""
        if v and '/' not in v:
            raise ValueError(f'ui.release.repo must be "owner/name", got {v!r}')
        return v


class UIProbeDetailsConfig(BaseModel):
    """Caps for the Message Probe's user-defined details writes.

    Both limits guard a single probe run against a handler that writes
    unbounded diagnostics. The defaults are generous headroom for typical
    handler logic; raise them when a probe legitimately produces more
    (e.g. one table row per record across many large inputs).
    """

    max_writes: int = Field(
        default=10_000,
        ge=1,
        description=(
            'Maximum probe.set/append/update calls recorded per probe run. '
            'The first write past the cap records one ProbeError; further '
            'writes are dropped silently.'
        ),
    )
    max_total_bytes: int = Field(
        default=5_000_000,
        ge=1,
        description=(
            'Maximum total serialized size (bytes) of all probe-details '
            'writes per probe run. Past it, writes are dropped like the '
            'max_writes cap.'
        ),
    )


# --- Timeline tuning: color rules, label roles, history depth -----------
#
# Rule conditions and colors are validated against fixed vocabularies rather
# than left as free-form strings, so a typo in a YAML rule (an unknown op, a
# misspelled field) fails config load instead of silently never matching at
# render time.
TIMELINE_COLOR_NAMES = frozenset({'green', 'red', 'yellow', 'blue', 'gray', 'lightgray', 'purple', 'orange'})
TIMELINE_STRING_FIELDS = frozenset({'status', 'origin', 'client_name'})
TIMELINE_NUMERIC_FIELDS = frozenset(
    {'exit_code', 'duration', 'stdout_size', 'stdout_lines', 'stdin_size', 'stdin_lines', 'partition'}
)
TIMELINE_OPS = frozenset({'eq', 'ne', 'contains', 'prefix', 'gt', 'ge', 'lt', 'le', 'exists', 'missing'})
_TIMELINE_NO_VALUE_OPS = frozenset({'exists', 'missing'})
_TIMELINE_STRING_OPS = frozenset({'contains', 'prefix'})
_TIMELINE_NUMERIC_OPS = frozenset({'gt', 'ge', 'lt', 'le'})
_TIMELINE_HEX_RE = re.compile(r'#[0-9a-fA-F]{6}')


class TimelineRuleCondition(BaseModel):
    """One condition of a timeline color rule: a label or task field compared with an operator."""

    model_config = ConfigDict(extra='forbid')

    label: str = ''
    field: str = ''
    op: str
    value: str | int | float | None = None

    @model_validator(mode='after')
    def _validate_condition(self) -> 'TimelineRuleCondition':
        if bool(self.label) == bool(self.field):
            raise ValueError('timeline condition must set exactly one of label/field')
        if self.op not in TIMELINE_OPS:
            raise ValueError(f"timeline condition op '{self.op}' is not one of {sorted(TIMELINE_OPS)}")
        if self.op in _TIMELINE_NO_VALUE_OPS and self.value is not None:
            raise ValueError(f"timeline condition op '{self.op}' takes no value")
        if self.op not in _TIMELINE_NO_VALUE_OPS and self.value is None:
            raise ValueError(f"timeline condition op '{self.op}' requires a value")
        if self.field:
            if self.field not in TIMELINE_STRING_FIELDS | TIMELINE_NUMERIC_FIELDS:
                raise ValueError(f"timeline condition field '{self.field}' is not a known task field")
            if self.op in _TIMELINE_STRING_OPS and self.field in TIMELINE_NUMERIC_FIELDS:
                raise ValueError(f"op '{self.op}' cannot apply to numeric field '{self.field}'")
            if self.op in _TIMELINE_NUMERIC_OPS and self.field in TIMELINE_STRING_FIELDS:
                raise ValueError(f"op '{self.op}' cannot apply to string field '{self.field}'")
        return self


class TimelineColorRule(BaseModel):
    """A first-match-wins bar-coloring rule: all conditions in `when` must hold."""

    model_config = ConfigDict(extra='forbid')

    name: str = ''
    when: list[TimelineRuleCondition] = Field(min_length=1)
    color: str

    @field_validator('when', mode='before')
    @classmethod
    def _wrap_single_condition(cls, value: object) -> object:
        return [value] if isinstance(value, dict) else value

    @field_validator('color')
    @classmethod
    def _validate_color(cls, value: str) -> str:
        if value in TIMELINE_COLOR_NAMES or _TIMELINE_HEX_RE.fullmatch(value):
            return value
        raise ValueError(f"timeline color '{value}' must be one of {sorted(TIMELINE_COLOR_NAMES)} or '#rrggbb'")


class TimelineLabels(BaseModel):
    """Which task label the UI uses for each special timeline role; empty = role unbound."""

    model_config = ConfigDict(extra='forbid')

    tag: str = Field(
        default='',
        description="Label whose value draws as a short chip at the task bar's right edge (truncated to 16 chars).",
    )
    caption: str = Field(
        default='',
        description=(
            "Label whose value draws at the task bar's left edge, in the space the tag "
            'leaves over (truncated to 32 chars).'
        ),
    )
    highlight: str = Field(
        default='',
        description=(
            'Numeric label bound to the toolbar threshold input: tasks at or above the '
            'typed value are emphasized, the rest dimmed.'
        ),
    )
    filter: str = Field(
        default='',
        description=(
            'String label bound to the toolbar text input: tasks whose value contains the '
            'typed text are emphasized, the rest dimmed.'
        ),
    )
    marker: str = Field(
        default='',
        description=(
            'Label that draws one vertical pin above the strip per distinct value — e.g. a batch or request id.'
        ),
    )


_TIMELINE_EVENT_NAME_RE = re.compile(r'^[a-z_][a-z0-9_]*$')


class TimelineEventType(BaseModel):
    """One declared custom timeline event type: its identity, look, and click behavior."""

    model_config = ConfigDict(extra='forbid')

    name: str = Field(description='Type identifier handlers emit by; lower_snake_case, unique in the list.')
    kind: Literal['marker', 'range', 'flag'] = Field(
        description='Visualization family: a vertical marker line, a background time range, or a flag pin.'
    )
    color: str = Field(description="Timeline color name or '#rrggbb', same vocabulary as color_rules.")
    line: Literal['solid', 'bold', 'dotted'] | None = Field(
        default=None,
        description="Marker line style; only valid for kind=marker, where it defaults to 'solid'.",
    )
    label: str = Field(
        default='',
        description='Chip template drawn at the marker top / flag pin; {text} substitutes the instance text.',
    )
    enabled: bool = Field(
        default=True,
        description='False makes emission of this type a no-op: nothing recorded, nothing sent to the UI.',
    )
    show: bool = Field(default=True, description='Initial visibility state of the per-type UI toggle.')
    link: str = Field(
        default='',
        description=(
            'URL template opened on click when action=link; resolves {ts_ms}, {end_ts_ms}, {text}, '
            'instance value keys, and ui.link_bases names.'
        ),
    )
    action: Literal['none', 'link', 'highlight', 'filter'] = Field(
        default='none',
        description='Click behavior: nothing, open the link template, or emphasize/dim matching tasks.',
    )

    @model_validator(mode='after')
    def _validate_event_type(self) -> 'TimelineEventType':
        if not _TIMELINE_EVENT_NAME_RE.match(self.name):
            raise ValueError(f"timeline event name '{self.name}' must match {_TIMELINE_EVENT_NAME_RE.pattern}")
        if self.color not in TIMELINE_COLOR_NAMES and not _TIMELINE_HEX_RE.fullmatch(self.color):
            raise ValueError(
                f"timeline event color '{self.color}' must be one of {sorted(TIMELINE_COLOR_NAMES)} or '#rrggbb'"
            )
        if self.kind != 'marker' and self.line is not None:
            raise ValueError(f"timeline event '{self.name}': line style applies only to kind=marker")
        if self.kind == 'marker' and self.line is None:
            self.line = 'solid'
        if self.action == 'link' and not self.link:
            raise ValueError(f"timeline event '{self.name}': action=link requires a link template")
        if self.action != 'link' and self.link:
            raise ValueError(f"timeline event '{self.name}': link is only used with action=link")
        return self


class UITimelineConfig(BaseModel):
    """Timeline history depth, bar color rules, and special label roles."""

    model_config = ConfigDict(extra='forbid')

    history_factor: int = Field(
        default=100,
        ge=1,
        description='Timeline keeps the newest history_factor x executor.max_executors tasks.',
    )
    max_age_minutes: int = Field(
        default=60,
        ge=1,
        le=1440,
        description='Oldest task age the timeline shows, in minutes.',
    )
    color_rules: list[TimelineColorRule] = Field(
        default_factory=list,
        max_length=50,
        description='First-match-wins rules coloring timeline task bars from labels and task fields.',
    )
    labels: TimelineLabels = Field(
        default_factory=TimelineLabels,
        description='Task label keys the UI uses for the tag, caption, highlight, filter, and marker roles.',
    )
    events: list[TimelineEventType] = Field(
        default_factory=list,
        max_length=50,
        description='Declared custom timeline event types drawn as markers, ranges, or flag pins on the live timeline.',
    )

    @field_validator('events')
    @classmethod
    def _reject_duplicate_event_names(cls, value: list[TimelineEventType]) -> list[TimelineEventType]:
        seen: set[str] = set()
        for event_type in value:
            if event_type.name in seen:
                raise ValueError(f"duplicate timeline event name '{event_type.name}'")
            seen.add(event_type.name)
        return value


class UIConsumePauseConfig(BaseModel):
    """Timed debug pause of the pipeline consumer, driven from the Live page.

    Opt-in (``enabled: false`` by default) because it directly affects the
    pipeline's work: while paused the worker fetches nothing from Kafka and
    consumer lag grows. The pause never leaves the consumer group — it uses
    partition pause/resume (the same primitive backpressure uses), the poll
    loop keeps running, and heartbeats continue — so no rebalance is ever
    triggered, and every pause is bounded by an explicit duration with an
    auto-resume timer. See ``ConsumePauseController``.
    """

    enabled: bool = Field(
        default=False,
        description=(
            'Serve the consume-pause API (``/api/v1/debug/consume-pause``) and '
            'show the pause control on the Live page. Off by default: pausing '
            'stops message intake for the chosen duration, which is a '
            'production-affecting act — enable it deliberately, for '
            'debug-friendly deployments.'
        ),
    )
    durations_seconds: list[int] = Field(
        default_factory=lambda: [15, 60, 300, 900],
        description=(
            'Preset pause durations (seconds) offered as one-click buttons on '
            'the Live page. The API itself accepts any duration between 1 and '
            '3600 seconds regardless of the presets.'
        ),
    )

    @field_validator('durations_seconds')
    @classmethod
    def _validate_durations(cls, v: list[int]) -> list[int]:
        """Reject preset lists the UI cannot sensibly render.

        Each preset must sit in the same [1, 3600] range the API enforces —
        a preset button that the API would reject with 422 is a config
        mistake better caught at startup.
        """
        if not v:
            raise ValueError('durations_seconds must not be empty')
        if len(v) > 10:
            raise ValueError('durations_seconds supports at most 10 presets')
        for d in v:
            if not (1 <= d <= 3600):
                raise ValueError(f'durations_seconds entries must be in [1, 3600], got {d}')
        return v


class UIConfig(BaseModel):
    """The operator web UI: HTTP server, presentation, and sub-sections.

    One first-class ``ui.*`` section covers the whole surface:

    - top-level keys — the UI server itself (bind address, auth) and
      presentation settings the pages/SPA consume;
    - ``ui.recorder.*`` — the flight-recorder store that feeds the UI
      (:class:`UIRecorderConfig`);
    - ``ui.release.*`` — drakkar-ui bundle fetching
      (:class:`UIReleaseConfig`);
    - ``ui.probe_details.*`` — write caps for the Message Probe's
      user-defined details (:class:`UIProbeDetailsConfig`);
    - ``ui.timeline.*`` — timeline history depth, bar color rules, and
      label roles (:class:`UITimelineConfig`).

    Set ``enabled: false`` to disable the whole UI feature (server,
    recorder persistence, and bundle serving).

    Every field is overridable through ``DK_UI__*`` environment variables.
    """

    enabled: bool = Field(
        default=True,
        description=(
            'Enable the whole UI feature: the server, flight-recorder persistence, and '
            'bundle serving. Off skips all of it.'
        ),
    )
    host: str = Field(
        default='127.0.0.1',
        description='Bind address for the UI server. Use 0.0.0.0 to expose on all interfaces.',
    )
    port: int = Field(default=8080, ge=1, le=65535, description='Port for the UI server.')
    auth_token: str = Field(
        default='',
        description=(
            'Bearer token for the UI. **Empty (the default) disables auth** '
            'entirely — every endpoint (including database download, merge, and '
            'message-probe) is reachable without credentials and the WebSocket '
            'live-event stream skips both token and Origin checks. This is a '
            'deliberate opt-in design: no endpoint stops a worker, replays '
            'Kafka messages, mutates sinks, or commits offsets, and Drakkar is '
            'intended for deployment inside a private contour (VPC / internal '
            'cluster / operator-only ingress). Most endpoints are read-only, '
            'but the probe and merge routes are not — close those with '
            '``probe_enabled`` / ``merge_enabled``, which act independently of '
            'this token. A startup warning fires whenever the UI is enabled '
            'without a token so the unauthenticated posture is visible in logs, '
            'naming whichever side-effecting endpoint is still enabled. '
            'When set to a non-empty value, protected HTTP endpoints require '
            'an ``Authorization: Bearer <token>`` header or ``?token=<token>`` '
            'query parameter; WebSocket connections without a valid token are '
            'closed with code 4401, and the Origin header is validated against '
            '``allowed_ws_origins`` (or the request Host header if that list is '
            'empty). Comparison uses ``secrets.compare_digest`` to avoid timing '
            'side-channels. Trailing/leading whitespace is stripped on load to '
            'avoid silent mismatches when YAML accidentally quotes spaces.'
        ),
        json_schema_extra={'drakkar_secret': True},
    )
    allowed_ws_origins: list[str] = Field(
        default_factory=list,
        description=(
            'Explicit allowlist of WebSocket origins. Empty list with non-empty '
            'auth_token defaults to same-origin only; empty list with empty '
            'auth_token = no origin check (dev workflow preserved).'
        ),
    )
    probe_enabled: bool = Field(
        default=True,
        description=(
            'Serve ``POST /api/v1/debug/probe``. This is the one UI endpoint that '
            'runs caller-supplied bytes through the live handler and the real '
            'executor subprocess pool, so it is neither read-only nor free: it '
            'competes with production traffic for executor slots. Set to false '
            'to serve 403 instead — independently of ``auth_token``, so a '
            'deployment that cannot set a token can still close the endpoint, '
            'and a deployment that has one can still close it as defence in '
            'depth. Probes never write sinks, recorder rows, cache entries, or '
            'offsets, so switching it off costs no pipeline behaviour.'
        ),
    )
    merge_enabled: bool = Field(
        default=True,
        description=(
            'Serve ``POST /api/v1/debug/merge``. This is the one UI endpoint that '
            'writes to disk: each call creates a new ``merged-<ts>.db`` in '
            '``ui.recorder.db_dir`` and nothing reclaims it, so repeated calls '
            'grow unbounded. Set to false to serve 403 instead — independently '
            'of ``auth_token``, per the reasoning on ``probe_enabled``.'
        ),
    )
    kafka_read_enabled: bool = Field(
        default=True,
        description=(
            'Serve ``GET /api/v1/debug/kafka/*`` — ad-hoc reads of the configured '
            'topics (source, dlq, and each Kafka sink by instance name; never '
            'an arbitrary topic). Reads use assign()-only consumers that join '
            'no consumer group and commit no offsets, so they are invisible to '
            'the pipeline. Set to false to serve 403 instead — independently '
            'of ``auth_token``, per the reasoning on ``probe_enabled``. When '
            'the resolved Kafka security of any readable topic is not '
            'PLAINTEXT while ``auth_token`` is empty, startup logs a warning '
            'naming the exposed aliases (the API stays available — gate it '
            'with ``auth_token`` or close it here).'
        ),
    )

    @field_validator('auth_token', mode='before')
    @classmethod
    def _strip_auth_token(cls, v: object) -> object:
        """Strip leading/trailing whitespace from ``auth_token`` on load.

        Operators sometimes write ``auth_token: " secret "`` in YAML (quoted
        to preserve a trailing space, by accident). With the raw value kept,
        ``secrets.compare_digest`` would require clients to send the literal
        space-padded string — a footgun. We strip once here so the stored
        value is the canonical token; the startup security gate and the
        ``_token_matches`` helper both see the same post-strip value.
        """
        if isinstance(v, str):
            return v.strip()
        return v

    public_url: str = Field(
        default='',
        description=(
            "Externally reachable URL of this worker's UI, used for "
            'cross-worker links in the workers list. Empty derives '
            'http://<ip>:<port> from the bind address.'
        ),
    )
    workers_offline_after_seconds: int = Field(
        default=30,
        ge=1,
        description=(
            'A discovered worker whose newest heartbeat is older than this '
            'many seconds is reported offline in the workers list. Size it '
            'to at least 2-3x the largest '
            '``ui.recorder.state_sync_interval_seconds`` in the fleet so a '
            'healthy worker never flaps offline between heartbeats.'
        ),
    )
    expose_env_vars: list[str] = Field(
        default_factory=list,
        description=(
            'Environment variable names captured into the worker_config snapshot — for '
            'recording deployment metadata like GIT_SHA or K8S_POD_NAME.'
        ),
    )
    max_rows: int = Field(
        default=5000,
        ge=100,
        description='Maximum rows returned to the web UI in list views.',
    )
    log_min_duration_ms: int = Field(
        default=500,
        ge=0,
        description=(
            'Minimum task duration (ms) to emit a slow_task_completed / slow_task_failed log line. 0 logs all tasks.'
        ),
    )
    ws_min_duration_ms: int = Field(
        default=500,
        ge=0,
        description=(
            'Minimum task duration (ms) to broadcast to the live UI over WebSocket. Failed '
            'tasks always appear. 0 shows all tasks.'
        ),
    )
    prometheus_url: str = Field(
        default='',
        description='Base URL of the Prometheus server used for UI links. Empty shows no Prometheus links.',
    )
    prometheus_rate_interval: str = Field(
        default='5m',
        description="Rate interval used in PromQL rate() expressions in UI links (e.g. '1m', '5m', '15m').",
    )
    prometheus_worker_label: str = Field(
        default='',
        description=(
            'PromQL label filter for worker-scoped queries, with {worker_id}, {cluster_name}, '
            '{metrics_port}, and {debug_port} template variables. Empty defaults to '
            'instance="{hostname}:{metrics_port}".'
        ),
    )
    prometheus_cluster_label: str = Field(
        default='',
        description=(
            'PromQL label filter for cluster-wide queries, same template variables as '
            'prometheus_worker_label. Empty hides cluster-wide links.'
        ),
    )
    custom_links: list[dict[str, str]] = Field(
        default_factory=list,
        description=(
            'Custom links shown in the dashboard navigation. Each entry is a dict with name '
            'and url keys; url supports the {worker_id}, {cluster_name}, {metrics_port}, and '
            '{debug_port} template variables.'
        ),
    )
    link_bases: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Named URL bases for probe-details link templates, e.g. '
            "``{jira: 'https://jira.internal.example.com'}``. A template such as "
            '``{jira}/browse/{value}`` resolves ``{jira}`` from this map, so code '
            'declares the link shape once and each environment supplies its own '
            'hosts. A base referenced by a registered layout but missing here '
            'logs one startup warning and the UI renders plain text for it.'
        ),
    )
    custom_renderers_path: str = Field(
        default='',
        description=(
            'Path to a deployment-provided JS module of custom cell '
            'renderers, served as-is at ``GET /api/v1/ui/renderers.js``. '
            'Empty (the default) disables the feature — the route then '
            '404s and identity reports ``custom_renderers: false``. When '
            'set, the file must exist at startup or the worker fails to '
            'boot; its content is trusted the same as any other backend '
            'config (it runs same-origin in the operator UI).'
        ),
    )
    consume_pause: UIConsumePauseConfig = Field(default_factory=UIConsumePauseConfig)
    recorder: UIRecorderConfig = Field(default_factory=UIRecorderConfig)
    release: UIReleaseConfig = Field(default_factory=UIReleaseConfig)
    probe_details: UIProbeDetailsConfig = Field(default_factory=UIProbeDetailsConfig)
    timeline: UITimelineConfig = Field(default_factory=UITimelineConfig)

    @field_validator('link_bases')
    @classmethod
    def _validate_link_bases(cls, value: dict[str, str]) -> dict[str, str]:
        for name, base in value.items():
            if not re.fullmatch(r'[a-z][a-z0-9_]*', name):
                raise ValueError(f"link_bases name '{name}' must be a lower-case identifier ([a-z][a-z0-9_]*)")
            if not base.startswith(('http://', 'https://')):
                raise ValueError(f"link_bases['{name}'] must start with http:// or https://")
        return {name: base.rstrip('/') for name, base in value.items()}
