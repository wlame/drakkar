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

from drakkar.config.cache import CacheConfig
from drakkar.config.connections import ConnectionsConfig, KafkaConnectionConfig
from drakkar.config.fleet import CONNECTION_PROVIDERS, FleetConfig
from drakkar.config.periodic import PeriodicConfig
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
    IOConfig,
    LoggingConfig,
    MetricsConfig,
    OffloadConfig,
    RuntimeHealthConfig,
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
from drakkar.config.webapp import WebAppConfig

SQLITE_WITHOUT_DIR_MESSAGE = (
    'recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, '
    'or set recorder.enabled to false'
)

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

    worker_name_env: str = Field(
        default='WORKER_ID',
        description='Environment variable that holds the worker name for logs, metrics, and UI',
    )
    cluster_name: str = Field(
        default='',
        description='Logical cluster name for grouping workers in the debug UI',
    )
    cluster_name_env: str = Field(
        default='',
        description='Environment variable that holds the cluster name (overrides cluster_name if set)',
    )
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    sinks: SinksConfig = Field(default_factory=SinksConfig)
    dlq: DLQConfig = Field(default_factory=DLQConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    runtime_health: RuntimeHealthConfig = Field(default_factory=RuntimeHealthConfig)
    io: IOConfig = Field(default_factory=IOConfig)
    offload: OffloadConfig = Field(default_factory=OffloadConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    recorder: RecorderConfig = Field(default_factory=RecorderConfig)
    periodic: PeriodicConfig = Field(default_factory=PeriodicConfig)
    fleet: FleetConfig = Field(default_factory=FleetConfig)
    connections: ConnectionsConfig = Field(default_factory=ConnectionsConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    app: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            'Reserved section for user-defined application config (see docs/app-config.md); '
            'the framework passes it through unvalidated to the handler-declared model'
        ),
    )

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
    def _validate_recorder_provider(self) -> 'DrakkarConfig':
        """An enabled ``sqlite`` recorder needs a ``storage.dir``; an enabled ``postgres`` one its connection.

        With the recorder off, the block of its provider loads and does nothing.
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
    def _validate_registry_connection(self) -> 'DrakkarConfig':
        """A registry provider that uses a connection needs that connection declared."""
        registry = self.fleet.registry
        _require_provider_connection(
            'fleet.registry.connection', registry.provider, registry.connection, self.connections
        )
        return self

    @model_validator(mode='after')
    def _validate_election_connection(self) -> 'DrakkarConfig':
        """An election provider that uses a connection needs that connection declared."""
        election = self.periodic.election
        _require_provider_connection(
            'periodic.election.connection', election.provider, election.connection, self.connections
        )
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
        """Card folder of the filesystem registry: ``<shared folder>/fleet``; ``''`` when no folder resolves."""
        shared_dir = self.resolved_shared_dir
        # normpath so the derived folder is the same clean path on every
        # backend ('./data/' -> 'data/fleet').
        return os.path.normpath(os.path.join(shared_dir, 'fleet')) if shared_dir else ''

    @property
    def fleet_summary(self) -> str:
        """The registry the worker will run: the provider name, or ``off``.

        ``off`` when the registry cannot run: it needs the UI server (peers
        reach a worker through it), and the filesystem provider needs a folder.
        """
        provider = self.fleet.registry.provider
        if not self.ui.enabled or (provider == 'filesystem' and not self.resolved_registry_dir):
            return 'off'
        return provider

    @property
    def election_summary(self) -> str:
        """The summary token of the periodic election provider, with its leading space.

        Empty for ``filesystem``, the default provider.
        """
        provider = self.periodic.election.provider
        return '' if provider == 'filesystem' else f' election={provider}'

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
        [worker/cluster] sources=[kafka:topic/group/100poll http:8090] exec=4w/100win retries=3/120s ui=on:8080 storage=/tmp/drakkar fleet=filesystem election=consul cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO

        ``sources=[...]`` lists the enabled input sources in fixed order
        (kafka, then http). The ``ui`` token reports the UI server state;
        the ``ui.release`` bundle-fetch settings are deliberately excluded
        (they never affect pipeline behavior). ``storage`` is the own-files
        folder (``mem`` for memory-only), ``shared`` appears only when the
        shared folder differs from it, and ``fleet`` names the registry the
        worker runs (see :attr:`fleet_summary`). ``election`` follows it
        only when the periodic election provider is not ``filesystem`` (see
        :attr:`election_summary`). The exact bytes are contractual.
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
        retries_part = f'{ex.max_retries}/{ex.task_timeout_seconds}s'

        ui_part = f'on:{self.ui.port}' if self.ui.enabled else 'off'

        # Cache summary: 'off' when disabled; otherwise 'on:f=Ns/s=Ns|off/c=Ns[/max=N]'.
        # :g format trims trailing zeros on integer-valued floats (3.0 → '3'), keeping
        # the common case compact while still rendering fractional intervals readably.
        if not self.cache.enabled:
            cache_part = 'off'
        else:
            flush = f'{self.cache.flush_interval_seconds:g}s'
            exchange = self.fleet.cache_exchange
            sync = f'{exchange.interval_seconds:g}s' if exchange.enabled else 'off'
            cleanup = f'{self.cache.cleanup_interval_seconds:g}s'
            cache_tokens = [f'f={flush}', f's={sync}', f'c={cleanup}']
            if self.cache.max_memory_entries is not None:
                cache_tokens.append(f'max={self.cache.max_memory_entries}')
            cache_part = 'on:' + '/'.join(cache_tokens)

        storage_part = self.storage.dir or 'mem'
        shared_dir = self.storage.shared_dir
        shared_token = f' shared={shared_dir}' if shared_dir and shared_dir != self.storage.dir else ''

        mode = self.resolved_persistence
        if mode == PERSISTENCE_POSTGRES:
            recorder_token = f' recorder={PERSISTENCE_POSTGRES}'
        elif mode == PERSISTENCE_NONE and self.storage.dir:
            recorder_token = f' recorder={PERSISTENCE_NONE}'
        else:
            recorder_token = ''

        metrics_part = str(self.metrics.port) if self.metrics.enabled else 'off'

        if not self.dlq_enabled:
            dlq_part = 'off'
        elif self.dlq.topic:
            dlq_part = self.dlq.topic
        else:
            dlq_part = 'on'

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
            f' ui={ui_part}'
            f' storage={storage_part}'
            f'{shared_token}'
            f'{recorder_token}'
            f' fleet={self.fleet_summary}'
            f'{self.election_summary}'
            f' cache={cache_part}'
            f' metrics={metrics_part}'
            f' dlq={dlq_part}'
            f' sinks=[{sinks_str}]'
            f' log={self.logging.level}'
        )


def _require_declared_connection(path: str, kind: str, name: str, connections: ConnectionsConfig) -> Any:
    """Return the entry ``name`` under the kind ``kind``; fail when ``connections`` does not declare it.

    ``path`` is the config path of the ``connection`` key, for the message. ``kind``
    is a field name of ``ConnectionsConfig``.
    """
    declared: dict[str, Any] = getattr(connections, kind)
    if name not in declared:
        raise ValueError(f"{path} '{name}' is not defined under connections.{kind}")
    return declared[name]


def _require_provider_connection(path: str, provider: str, name: str, connections: ConnectionsConfig) -> None:
    """Fail when ``provider`` uses a named connection and ``connections`` does not declare ``name``.

    The check goes by the provider name alone: a name in ``CONNECTION_PROVIDERS``
    (``consul``, ``redis``, ``postgres``) is checked; any other name is not.
    """
    if provider in CONNECTION_PROVIDERS:
        _require_declared_connection(path, provider, name, connections)


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
    return DrakkarConfig(_env_prefix=_NO_ENV_VARIABLE_PREFIX, **merged)


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
