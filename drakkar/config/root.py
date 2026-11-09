"""The root ``DrakkarConfig`` settings model and the YAML/env loader.

``load_config`` layers three sources, lowest precedence first: model
defaults, the YAML document, then ``DK_*`` environment overrides.
"""

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from drakkar.config.agent import AgentConfig
from drakkar.config.api import APIConfig
from drakkar.config.autodiscover import CONNECTION_PROVIDERS, FILESYSTEM_PROVIDER, AutodiscoverConfig
from drakkar.config.cache import CacheConfig
from drakkar.config.connections import ConnectionsConfig, KafkaConnectionConfig
from drakkar.config.identity import ClusterConfig, WorkerConfig
from drakkar.config.origins import ConfigOrigins
from drakkar.config.recorder import (
    PERSISTENCE_NONE,
    PERSISTENCE_POSTGRES,
    PERSISTENCE_SQLITE,
    Persistence,
    RecorderConfig,
    duration_of_hours,
)
from drakkar.config.resilience import validate_resilience
from drakkar.config.runtime import (
    ExecutorConfig,
    LoggingConfig,
    MetricsConfig,
    RuntimeHealthConfig,
    ThreadsConfig,
)
from drakkar.config.sinks import DLQConfig, SinksConfig
from drakkar.config.sources import SourcesConfig
from drakkar.config.storage import StorageConfig
from drakkar.config.strict import (
    decode_env_json,
    deep_merge,
    find_unknown_keys,
    resolve_env_map_names,
    unknown_keys_message,
)
from drakkar.config.ui import UIConfig
from drakkar.config.unit_bounds import check_unit_bounds
from drakkar.config.webapp import WebAppConfig
from drakkar.schedule import sanitize_cluster

SQLITE_WITHOUT_DIR_MESSAGE = (
    'recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, '
    'or set recorder.enabled to false'
)

FILESYSTEM_WITHOUT_SHARED_DIR_MESSAGE = 'autodiscover.provider filesystem needs storage.shared_dir'

UI_WITHOUT_API_MESSAGE = 'ui.enabled needs api.enabled; set ui.enabled to false, or set api.enabled to true'

AUTODISCOVER_WITHOUT_API_MESSAGE = 'autodiscover.enabled needs api.enabled: peers reach a worker through its API'

AGENT_WITHOUT_API_MESSAGE = 'agent.enabled needs api.enabled: the agent routes are served by the API server'

# The loader passes the whole tree, the ``DK_*`` values already in it with their
# map names resolved. No variable name can start with NUL, so with this prefix the
# settings env source finds nothing and cannot add a value a second time under its
# literal name.
_NO_ENV_VARIABLE_PREFIX = '\0'

# The sink types whose instances name a connection; each is a field of ``ConnectionsConfig`` too.
SINK_CONNECTION_KINDS = ('kafka', 'mongo', 'postgres', 'redis')


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker.

    Combines the named service connections, the input sources, executor pool
    settings, sink definitions, and operational configs (metrics, logging, debug).
    """

    # A config error must not echo the input it rejected: pydantic prints a
    # cut of the offending section, which can hold a password or token, and
    # the error escapes ``DrakkarApp(...)`` to the operator's terminal and logs.
    # This hides the input only in ``str()`` and ``repr()``: ``errors()`` and
    # ``json()`` still carry it, so a structured error log must not use them.
    model_config = SettingsConfigDict(
        env_prefix='DK_',
        env_nested_delimiter='__',
        hide_input_in_errors=True,
    )

    @model_validator(mode='before')
    @classmethod
    def _reject_unknown_keys(cls, values: object) -> object:
        """Refuse every key the models do not declare, ahead of the field checks.

        The raw values here are YAML merged with the ``DK_*`` environment on
        every construction path. A section passed as a built model is not walked.
        """
        if isinstance(values, Mapping):
            found = find_unknown_keys(values, cls)
            if found:
                raise ValueError(unknown_keys_message(found))
        return values

    @model_validator(mode='before')
    @classmethod
    def _reject_dk_app_env_overrides(cls, values: object) -> object:
        """Refuse ``DK_APP__*`` env vars — the ``app.*`` section is not framework-owned.

        The ``app:`` section belongs to the handler-declared application
        config model (see docs/app-config.md), which binds its own env
        prefix (``BaseDrakkarHandler.app_env_prefix``, default ``APP_``).
        Letting ``DK_APP__*`` silently merge into the pass-through dict
        would create a second, framework-flavored override path for the
        same values. Sorted var order keeps the message deterministic.
        """
        app_env = sorted(name for name in os.environ if name == 'DK_APP' or name.startswith('DK_APP__'))
        if app_env:
            raise ValueError(
                'DK_APP__* environment overrides are not supported: the app.* section is '
                "passed through to the handler's own config model, which binds its own env "
                'prefix (app_env_prefix, default APP_); rename: ' + ', '.join(app_env)
            )
        return values

    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)
    connections: ConnectionsConfig = Field(default_factory=ConnectionsConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    threads: ThreadsConfig = Field(default_factory=ThreadsConfig)
    sinks: SinksConfig = Field(default_factory=SinksConfig)
    dlq: DLQConfig = Field(default_factory=DLQConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    recorder: RecorderConfig = Field(default_factory=RecorderConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    autodiscover: AutodiscoverConfig = Field(default_factory=AutodiscoverConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    runtime_health: RuntimeHealthConfig = Field(default_factory=RuntimeHealthConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)
    app: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            'Reserved section for user-defined application config (see docs/app-config.md); '
            'the framework passes it through unvalidated to the handler-declared model'
        ),
    )

    @model_validator(mode='after')
    def _validate_unit_bounds(self) -> 'DrakkarConfig':
        """Every value with a unit is inside its range; runs first, as a check of a single key."""
        check_unit_bounds(self)
        return self

    @model_validator(mode='after')
    def _require_one_source_enabled(self) -> 'DrakkarConfig':
        """At least one of ``sources.kafka``/``sources.http`` must be enabled.

        Checked here rather than on ``SourcesConfig`` itself, so a bare
        ``SourcesConfig()`` (both sources off) stays constructible on its
        own — the requirement is about a runnable worker, not about the
        section in isolation.
        """
        if not self.sources.enabled_names:
            raise ValueError('no input source enabled: set sources.kafka.enabled or sources.http.enabled')
        return self

    @model_validator(mode='after')
    def _validate_what_needs_the_api(self) -> 'DrakkarConfig':
        """The UI, ``autodiscover`` and the agent routes all need the API server."""
        self.check_what_needs_the_api()
        return self

    @model_validator(mode='after')
    def _validate_recorder_provider(self) -> 'DrakkarConfig':
        """What the enabled recorder provider needs from the rest of the config."""
        self.check_recorder_provider()
        return self

    @model_validator(mode='after')
    def _validate_archive_vs_rotation(self) -> 'DrakkarConfig':
        """Archive windows and the due delay must be at least one rotation long.

        A window shorter than the rotation interval would be named for files
        that rotate out of it later; a due delay shorter than one rotation
        could archive a window while a file of it is still being written.
        """
        rotation = self.recorder.sqlite.rotation_interval
        archive = self.recorder.archive
        rotation_text = f'recorder.sqlite.rotation_interval ({duration_of_hours(rotation)})'
        if archive.window < rotation:
            raise ValueError(
                f'recorder.archive.window ({duration_of_hours(archive.window)}) must be >= {rotation_text}'
            )
        if archive.due_after < rotation:
            raise ValueError(
                f'recorder.archive.due_after ({duration_of_hours(archive.due_after)}) must be >= {rotation_text}'
            )
        return self

    @model_validator(mode='after')
    def _validate_recorder_resilience(self) -> 'DrakkarConfig':
        """Check the rules of the recorder's resilience block that span fields, naming it by its YAML path."""
        validate_resilience('recorder.postgres.resilience', self.recorder.postgres.resilience)
        return self

    @model_validator(mode='after')
    def _validate_autodiscover(self) -> 'DrakkarConfig':
        """What an enabled ``autodiscover`` needs from the rest of the config."""
        self.check_autodiscover()
        return self

    @model_validator(mode='after')
    def _validate_kafka_source_connection(self) -> 'DrakkarConfig':
        """An enabled Kafka source needs its connection declared."""
        source = self.sources.kafka
        if source.enabled:
            _require_declared_connection('sources.kafka.connection', 'kafka', source.connection, self.connections)
        return self

    @model_validator(mode='after')
    def _validate_sink_connections(self) -> 'DrakkarConfig':
        """Every configured sink of a connection kind needs its connection declared; the first by sorted name fails."""
        for kind in SINK_CONNECTION_KINDS:
            section: dict[str, Any] = getattr(self.sinks, kind)
            for name in sorted(section):
                _require_declared_connection(
                    f'sinks.{kind}.{name}.connection', kind, section[name].connection, self.connections
                )
        return self

    @model_validator(mode='after')
    def _validate_dlq_connection(self) -> 'DrakkarConfig':
        """A DLQ that has a destination needs its connection declared."""
        if self.dlq_enabled:
            _require_declared_connection('dlq.connection', self.dlq.provider, self.dlq.connection, self.connections)
        return self

    @model_validator(mode='after')
    def _validate_sink_dependencies(self) -> 'DrakkarConfig':
        """Every ``depends_on`` reference names a configured sink, none names its own sink, and none forms a cycle."""
        self.sinks.check_dependencies()
        return self

    @property
    def resolved_persistence(self) -> Persistence:
        """The durable store of the recorder: ``recorder.provider``, or ``none`` with the recorder off."""
        return self.recorder.provider if self.recorder.enabled else PERSISTENCE_NONE

    @property
    def resolved_shared_dir(self) -> str:
        """The folder every worker of the cluster sees: ``storage.shared_dir``, else ``storage.dir``."""
        return self.storage.shared_dir or self.storage.dir

    @property
    def resolved_registry_dir(self) -> str:
        """Card folder of the filesystem registry: ``<storage.shared_dir>/fleet``.

        ``normpath`` makes the derived folder the same clean path on every
        backend (``./data/`` gives ``data/fleet``).
        """
        return os.path.normpath(os.path.join(self.storage.shared_dir, 'fleet'))

    @property
    def memory_max_rows(self) -> int:
        """The row cap of the recorder memory store: one share per executor slot."""
        return self.recorder.memory.rows_per_executor * self.executor.max_executors

    @property
    def dlq_enabled(self) -> bool:
        """Whether a DLQ producer is built: with ``dlq.enabled``, when the Kafka source or ``dlq.topic`` gives it a topic."""
        return bool(self.resolved_dlq_topic)

    @property
    def dlq_unconfigured(self) -> bool:
        """The DLQ is on but has no destination: no ``dlq.topic`` and no Kafka source.

        A record then has nowhere to go and is a counted drop. A DLQ switched off
        with ``dlq.enabled`` is not this case: it follows ``dlq.on_send_failure``.
        """
        return self.dlq.enabled and not self.resolved_dlq_topic

    @property
    def kafka_source_connection(self) -> KafkaConnectionConfig | None:
        """The connection of the Kafka source; ``None`` when the source is off."""
        source = self.sources.kafka
        if not source.enabled:
            return None
        return self.declared_connection('sources.kafka.connection', 'kafka', source.connection)

    @property
    def dlq_connection(self) -> KafkaConnectionConfig | None:
        """The connection of the DLQ; ``None`` when no DLQ producer is built."""
        if not self.dlq_enabled:
            return None
        return self.declared_connection('dlq.connection', self.dlq.provider, self.dlq.connection)

    def check_what_needs_the_api(self) -> None:
        """Refuse a UI, an ``autodiscover`` or an agent surface with no API server to serve them.

        The load calls this, and so does the startup after ``on_startup``, which
        may have changed the keys: both give the same messages.

        Raises:
            ValueError: ``ui``, ``autodiscover`` or ``agent`` is on while ``api`` is off.
        """
        if self.api.enabled:
            return
        if self.ui.enabled:
            raise ValueError(UI_WITHOUT_API_MESSAGE)
        if self.autodiscover.enabled:
            raise ValueError(AUTODISCOVER_WITHOUT_API_MESSAGE)
        if self.agent.enabled:
            raise ValueError(AGENT_WITHOUT_API_MESSAGE)

    def check_recorder_provider(self) -> None:
        """An enabled ``sqlite`` recorder needs a ``storage.dir``; an enabled ``postgres`` one its connection.

        With the recorder off, the block of its provider loads and does nothing.
        The load calls this, and so does the startup after ``on_startup``, which
        may have changed the keys: both give the same messages.

        Raises:
            ValueError: the provider lacks its folder or its connection.
        """
        if self.resolved_persistence == PERSISTENCE_SQLITE and not self.storage.dir:
            raise ValueError(SQLITE_WITHOUT_DIR_MESSAGE)
        if self.resolved_persistence == PERSISTENCE_POSTGRES:
            _require_declared_connection(
                'recorder.postgres.connection',
                PERSISTENCE_POSTGRES,
                self.recorder.postgres.connection,
                self.connections,
            )

    def check_autodiscover(self) -> None:
        """Refuse an ``autodiscover`` section that cannot run.

        The load calls this, and so does the startup after ``on_startup``, which
        may have changed the keys: both give the same messages.

        Raises:
            ValueError: the provider name is not valid, or the enabled provider
                lacks its shared folder or its connection.
        """
        autodiscover = self.autodiscover
        autodiscover.check_provider()
        if not autodiscover.enabled:
            return
        if autodiscover.provider == FILESYSTEM_PROVIDER and not self.storage.shared_dir:
            raise ValueError(FILESYSTEM_WITHOUT_SHARED_DIR_MESSAGE)
        if autodiscover.provider in CONNECTION_PROVIDERS:
            _require_declared_connection(
                'autodiscover.connection', autodiscover.provider, autodiscover.connection, self.connections
            )

    def declared_connection(self, path: str, kind: str, name: str) -> Any:
        """The connection ``name`` of the service ``kind``, read at run time.

        Config load checks every connection the file names. A hook can name one
        later (a sink it adds, a ``dlq.topic`` it sets), so a read at run time
        fails with the same message as the load: ``<path> '<name>' is not
        defined under connections.<kind>``.
        """
        return _require_declared_connection(path, kind, name, self.connections)

    @property
    def resolved_consumer_group(self) -> str:
        """The consumer group this worker joins; ``''`` when the Kafka source is off.

        A worker with no Kafka source joins no group, so reporting the
        ``sources.kafka.consumer_group`` default would put it in every
        group-scoped log filter and dashboard query it has nothing to do
        with. Read by the ``worker_info`` metric and by logging setup.
        """
        return self.sources.kafka.consumer_group if self.sources.kafka.enabled else ''

    @property
    def resolved_dlq_topic(self) -> str:
        """The DLQ topic the worker will use; ``''`` when the DLQ is off."""
        if not self.dlq.enabled:
            return ''
        if self.dlq.topic:
            return self.dlq.topic
        if self.sources.kafka.enabled:
            return f'{self.sources.kafka.topic}_dlq'
        return ''

    def config_summary(self, worker_id: str = '', cluster_name: str = '') -> str:
        """One-line human-readable config summary for startup logging and debug UI.

        Format:
        [worker/cluster] sources=[kafka:topic/group/100poll http:8090] exec=4w/100win retries=3/120s api=8080 ui=on agent=off storage=/tmp/drakkar recorder=sqlite autodiscover=consul cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO

        ``sources=[...]`` lists the enabled input sources in fixed order
        (kafka, then http). ``api`` is the port of the API server, or ``off``;
        ``ui`` says whether its UI routes are served (the ``ui.release``
        bundle-fetch settings are left out: they never affect pipeline
        behavior). ``agent`` says whether the agent routes are served.
        ``storage`` is the own-files folder (``mem`` with none), and
        ``shared`` appears only when the shared folder differs from it.
        ``recorder`` is the durable store, or ``off``; ``autodiscover`` the
        provider, or ``off``. The cache token reads ``cache.exchange`` for
        ``s=`` and shows ``/max=<n>`` only for a bounded memory cache. ``dlq``
        follows ``dlq.enabled``: ``off``, the configured topic, ``on`` for the
        topic derived from the Kafka source, or ``unconfigured`` when the DLQ
        is on and has no topic to write to. Durations print in seconds. The
        exact bytes are contractual.
        """
        identity = worker_id or '?'
        if cluster_name:
            identity = f'{identity}/{cluster_name}'

        source_tokens: list[str] = []
        if self.sources.kafka.enabled:
            k = self.sources.kafka
            source_tokens.append(f'kafka:{k.topic}/{k.consumer_group}/{k.max_poll_records}poll')
        if self.sources.http.enabled:
            source_tokens.append(f'http:{self.sources.http.port}')

        ex = self.executor
        exec_part = f'{ex.max_executors}w/{ex.window_size}win'
        retries_part = f'{ex.max_retries}/{ex.task_timeout}s'

        api_part = str(self.api.port) if self.api.enabled else 'off'
        ui_part = 'on' if self.ui.enabled else 'off'
        agent_part = 'on' if self.agent.enabled else 'off'

        storage_part = self.storage.dir or 'mem'
        shared_dir = self.storage.shared_dir
        shared_token = f' shared={shared_dir}' if shared_dir and shared_dir != self.storage.dir else ''

        recorder_part = 'off' if self.resolved_persistence == PERSISTENCE_NONE else self.resolved_persistence
        autodiscover_part = self.autodiscover.provider if self.autodiscover.enabled else 'off'
        metrics_part = str(self.metrics.port) if self.metrics.enabled else 'off'

        sink_parts: list[str] = []
        abbrevs = {
            'kafka': 'kf',
            'postgres': 'pg',
            'mongo': 'mg',
            'http': 'http',
            'redis': 'rd',
            'filesystem': 'fs',
        }
        for sink_type, names in self.sinks.summary().items():
            abbr = abbrevs.get(sink_type, sink_type)
            sink_parts.append(f'{abbr}:{",".join(names)}')
        sinks_str = ' '.join(sink_parts) if sink_parts else 'none'

        return (
            f'[{identity}]'
            f' sources=[{" ".join(source_tokens)}]'
            f' exec={exec_part}'
            f' retries={retries_part}'
            f' api={api_part}'
            f' ui={ui_part}'
            f' agent={agent_part}'
            f' storage={storage_part}'
            f'{shared_token}'
            f' recorder={recorder_part}'
            f' autodiscover={autodiscover_part}'
            f' cache={self._cache_summary()}'
            f' metrics={metrics_part}'
            f' dlq={self._dlq_summary()}'
            f' sinks=[{sinks_str}]'
            f' log={self.logging.level}'
        )

    def _cache_summary(self) -> str:
        """The cache token: ``off``, or ``on:f=<flush>/s=<exchange interval|off>/c=<cleanup>[/max=<n>]``.

        The ``:g`` format prints whole seconds without a fraction (``3s``) and
        keeps a fraction where there is one (``12.5s``).
        """
        cache = self.cache
        if not cache.enabled:
            return 'off'
        exchange = cache.exchange
        tokens = [
            f'f={cache.flush_interval:g}s',
            f's={exchange.interval:g}s' if exchange.enabled else 's=off',
            f'c={cache.cleanup_interval:g}s',
        ]
        if cache.max_memory_entries > 0:
            tokens.append(f'max={cache.max_memory_entries}')
        return 'on:' + '/'.join(tokens)

    def _dlq_summary(self) -> str:
        """The DLQ token; see :meth:`config_summary`."""
        if not self.dlq.enabled:
            return 'off'
        if self.dlq_unconfigured:
            return 'unconfigured'
        return self.dlq.topic or 'on'


def own_storage_dir(storage_dir: str, cluster: str, worker: str) -> str:
    """The folder of this worker's own files: ``<storage_dir>/<sanitized cluster>/<worker>``.

    An empty cluster gives the folder ``default``, as in the shared folder's
    ``history/`` and ``tasks/``. The worker name passes the worker name rule
    (``config.identity.check_worker_name``), so it is already safe as a
    folder name. An empty ``storage_dir`` (no own files) gives ``''``.
    ``normpath`` makes ``./data/`` and ``data`` give the same path.
    """
    if not storage_dir:
        return ''
    return os.path.normpath(os.path.join(storage_dir, sanitize_cluster(cluster), worker))


def _require_declared_connection(path: str, kind: str, name: str, connections: ConnectionsConfig) -> Any:
    """Return the entry ``name`` under the kind ``kind``; fail when ``connections`` does not declare it.

    ``path`` is the config path of the ``connection`` key, for the message. ``kind``
    is a field name of ``ConnectionsConfig``.
    """
    declared: dict[str, Any] = getattr(connections, kind)
    if name not in declared:
        raise ValueError(f"{path} '{name}' is not defined under connections.{kind}")
    return declared[name]


class _ConfigYamlLoader(yaml.SafeLoader):
    """The safe loader, except that an integer too long for ``int()`` stays text.

    Python refuses to convert a very long decimal text, and the error would name no
    config key. As text, the value reaches its field check.
    """

    def construct_yaml_int(self, node: yaml.ScalarNode) -> int | str:
        try:
            return super().construct_yaml_int(node)
        except ValueError:
            return node.value


_ConfigYamlLoader.add_constructor('tag:yaml.org,2002:int', _ConfigYamlLoader.construct_yaml_int)


def load_yaml_document(stream: Any) -> Any:
    """Parse one config document from an open file or a text."""
    return yaml.load(stream, Loader=_ConfigYamlLoader)


def load_config(config_path: str | Path | None = None) -> DrakkarConfig:
    """Load configuration from YAML file and environment variables.

    YAML file path is resolved in order:
    1. Explicit config_path argument
    2. DK_CONFIG environment variable
    3. Falls back to env-only config

    Environment variables override YAML values. Use DK_ prefix
    with __ for nesting (e.g., DK_EXECUTOR__MAX_EXECUTORS).
    """
    config, _ = load_config_with_origins(config_path)
    return config


def load_config_with_origins(config_path: str | Path | None = None) -> tuple[DrakkarConfig, ConfigOrigins]:
    """Load the configuration like :func:`load_config`, and record which keys YAML and ``DK_`` gave."""
    if config_path is None:
        config_path = os.environ.get('DK_CONFIG')

    yaml_data: Any = {}
    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')

        with open(path) as f:
            yaml_data = load_yaml_document(f) or {}

    env_overrides = resolve_env_map_names(_parse_env_overrides('DK_', '__'), yaml_data, DrakkarConfig)
    merged = deep_merge(yaml_data, env_overrides)
    # Top-level result is always a dict (the env-var prefix is fixed
    # and never numeric); the assert satisfies static typing without a
    # runtime branch.
    assert isinstance(merged, dict)
    merged = _apply_list_field_defaults(decode_env_json(merged, env_overrides, DrakkarConfig))
    # The before-validator cannot do this for the loader: the settings constructor
    # takes keys such as ``_env_file`` as its own arguments, so they never reach it.
    found = find_unknown_keys(merged, DrakkarConfig)
    if found:
        raise ValueError(unknown_keys_message(found))
    config = DrakkarConfig(_env_prefix=_NO_ENV_VARIABLE_PREFIX, **merged)
    return config, ConfigOrigins.from_sources(yaml_data=yaml_data, env_overrides=env_overrides)


def _apply_list_field_defaults(merged: dict) -> dict:
    """Ensure list-of-objects env-var overrides do not erase default entries.

    When env-vars target individual list elements (e.g.
    ``DK_SOURCES__HTTP__CLIENTS__0__RPM=10``), the parser produces a partial
    list like ``[{'rpm': '10'}]`` with no other fields. If the YAML did not
    supply ``sources.http.clients`` at all, Pydantic would now see this
    partial list as the entire value and reject it for missing required
    fields (``name``). To preserve the documented behaviour — env-vars
    override individual fields without forcing operators to repeat the
    defaults — we deep-merge the default ``WebAppConfig`` clients list
    under the partial override before construction.

    This is intentionally narrow (only ``sources.http.clients`` for now).
    If another list-of-objects field needs the same treatment later, add it
    here with a small helper rather than introducing a generic mechanism.
    """
    sources = merged.get('sources')
    if not isinstance(sources, dict):
        return merged
    http = sources.get('http')
    if not isinstance(http, dict):
        return merged
    clients_override = http.get('clients')
    if not isinstance(clients_override, list):
        return merged
    # Build the default clients list from the WebAppConfig default factory
    # and overlay the env-var override on top. We do this by dumping a
    # fresh WebAppConfig() to dict form so we are guaranteed to track any
    # future changes to the default list.
    default_clients = [c.model_dump() for c in WebAppConfig().clients]
    new_http = dict(http)
    new_http['clients'] = deep_merge(default_clients, clients_override)
    new_sources = dict(sources)
    new_sources['http'] = new_http
    new_merged = dict(merged)
    new_merged['sources'] = new_sources
    return new_merged


def _parse_env_overrides(prefix: str, delimiter: str, *, skip_config_key: bool = True) -> dict:
    """Extract env vars with prefix, split by delimiter into nested dict.

    Numeric path segments are detected and the surrounding dict is
    converted to a list (e.g. ``DK_SOURCES__HTTP__CLIENTS__0__RPM=10``
    becomes ``{'sources': {'http': {'clients': [{'rpm': '10'}]}}}``). This
    lets list-of-objects config fields (like ``sources.http.clients``) be
    overridden by env vars in the same nested-delimiter style as scalar
    fields.

    ``skip_config_key`` drops ``<prefix>CONFIG`` — the framework's own
    config-file-path convention (``DK_CONFIG``). The app-config loader
    (:mod:`drakkar.appconfig`) passes ``False``: a user prefix has no such
    convention, and silently dropping e.g. ``MYAPP_CONFIG`` would lose a
    legitimate override of a user field named ``config``.
    """
    result: dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        # skip the config file path env var itself
        if skip_config_key and key == f'{prefix}CONFIG':
            continue
        parts = key[len(prefix) :].lower().split(delimiter)
        d = result
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = value
    # Convert numeric-keyed nested dicts to lists. The top-level result
    # is always a dict (top-level prefix segments are never numeric), so
    # the cast is safe.
    converted_result = _numeric_dicts_to_lists(result)
    assert isinstance(converted_result, dict)
    return converted_result


def _numeric_dicts_to_lists(node: Any) -> Any:
    """Recursively convert dicts with all-numeric string keys to lists.

    A dict like ``{'0': {...}, '2': {...}}`` represents a sparse list with
    indices 0 and 2. We materialise it as ``[{...}, {}, {...}]`` (filling
    gaps with empty dicts) so Pydantic can validate the surrounding model
    and so ``deep_merge`` can later overlay it onto a YAML-supplied list
    by index.
    """
    if isinstance(node, dict):
        # Recurse into values first so nested numeric-keyed dicts are
        # converted before we decide whether to convert the parent.
        converted: dict[str, Any] = {k: _numeric_dicts_to_lists(v) for k, v in node.items()}
        if converted and all(isinstance(k, str) and k.isdigit() for k in converted):
            max_index = max(int(k) for k in converted)
            result_list: list[Any] = [{} for _ in range(max_index + 1)]
            for k, v in converted.items():
                result_list[int(k)] = v
            return result_list
        return converted
    return node
