"""The root ``DrakkarConfig`` settings model and the YAML/env loader.

``load_config`` layers three sources, lowest precedence first: model
defaults, the YAML document, then ``DK_*`` environment overrides.
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from drakkar.config.cache import CacheConfig
from drakkar.config.kafka import KafkaConfig
from drakkar.config.runtime import (
    ExecutorConfig,
    IOConfig,
    LoggingConfig,
    MetricsConfig,
    OffloadConfig,
    RuntimeHealthConfig,
    ThroughputConfig,
)
from drakkar.config.sinks import DLQConfig, SinksConfig
from drakkar.config.ui import UIConfig
from drakkar.config.webapp import WebAppConfig


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker.

    Combines Kafka source settings, executor pool settings,
    sink definitions, and operational configs (metrics, logging, debug).
    """

    model_config = SettingsConfigDict(
        env_prefix='DK_',
        env_nested_delimiter='__',
    )

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
        app_env = sorted(name for name in os.environ if name.startswith('DK_APP__'))
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
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    sinks: SinksConfig = Field(default_factory=SinksConfig)
    dlq: DLQConfig = Field(default_factory=DLQConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    throughput: ThroughputConfig = Field(default_factory=ThroughputConfig)
    runtime_health: RuntimeHealthConfig = Field(default_factory=RuntimeHealthConfig)
    io: IOConfig = Field(default_factory=IOConfig)
    offload: OffloadConfig = Field(default_factory=OffloadConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    webapp: WebAppConfig = Field(default_factory=WebAppConfig)
    app: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            'Reserved section for user-defined application config (see docs/app-config.md); '
            'the framework passes it through unvalidated to the handler-declared model'
        ),
    )

    def config_summary(self, worker_id: str = '', cluster_name: str = '') -> str:
        """One-line human-readable config summary for startup logging and debug UI.

        Format (Option C — structured-but-readable):
        [worker/cluster] topic=... group=... exec=4w/100win/100poll retries=3/120s ui=on:8080 webapp=on:8090 cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO

        The ``ui`` token reports the UI server state; the ``ui.release``
        bundle-fetch settings are deliberately excluded (they never affect
        pipeline behavior). The exact bytes are contractual.

        The ``webapp`` token reports the synchronous-ingress server
        (``webapp.host``/``port`` bind; port shown, host omitted like the
        other tokens).
        """
        identity = worker_id or '?'
        if cluster_name:
            identity = f'{identity}/{cluster_name}'

        ex = self.executor
        exec_part = f'{ex.max_executors}w/{ex.window_size}win/{self.kafka.max_poll_records}poll'
        retries_part = f'{ex.max_retries}/{ex.task_timeout_seconds}s'

        ui_part = f'on:{self.ui.port}' if self.ui.enabled else 'off'
        webapp_part = f'on:{self.webapp.port}' if self.webapp.enabled else 'off'

        # Cache summary: 'off' when disabled; otherwise 'on:f=Ns/s=Ns|off/c=Ns[/max=N]'.
        # :g format trims trailing zeros on integer-valued floats (3.0 → '3'), keeping
        # the common case compact while still rendering fractional intervals readably.
        if not self.cache.enabled:
            cache_part = 'off'
        else:
            flush = f'{self.cache.flush_interval_seconds:g}s'
            sync = f'{self.cache.peer_sync.interval_seconds:g}s' if self.cache.peer_sync.enabled else 'off'
            cleanup = f'{self.cache.cleanup_interval_seconds:g}s'
            cache_tokens = [f'f={flush}', f's={sync}', f'c={cleanup}']
            if self.cache.max_memory_entries is not None:
                cache_tokens.append(f'max={self.cache.max_memory_entries}')
            cache_part = 'on:' + '/'.join(cache_tokens)

        metrics_part = str(self.metrics.port) if self.metrics.enabled else 'off'

        dlq_topic = self.dlq.topic or f'{self.kafka.source_topic}_dlq'
        dlq_part = dlq_topic if self.dlq.topic else 'on'

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
            f' topic={self.kafka.source_topic}'
            f' group={self.kafka.consumer_group}'
            f' exec={exec_part}'
            f' retries={retries_part}'
            f' ui={ui_part}'
            f' webapp={webapp_part}'
            f' cache={cache_part}'
            f' metrics={metrics_part}'
            f' dlq={dlq_part}'
            f' sinks=[{sinks_str}]'
            f' log={self.logging.level}'
        )


def load_config(config_path: str | Path | None = None) -> DrakkarConfig:
    """Load configuration from YAML file and environment variables.

    YAML file path is resolved in order:
    1. Explicit config_path argument
    2. DK_CONFIG environment variable
    3. Falls back to env-only config

    Environment variables override YAML values. Use DK_ prefix
    with __ for nesting (e.g., DK_KAFKA__BROKERS).
    """
    if config_path is None:
        config_path = os.environ.get('DK_CONFIG')

    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')

        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

        # pydantic-settings ignores env vars for nested models when init
        # kwargs are passed. Fix: extract DK_* env vars, parse them
        # into nested structure, and deep-merge on top of YAML.
        env_overrides = _parse_env_overrides('DK_', '__')
        merged = _deep_merge(yaml_data, env_overrides)
        # Top-level result is always a dict (the env-var prefix is fixed
        # and never numeric); the assert satisfies static typing without a
        # runtime branch.
        assert isinstance(merged, dict)
        merged = _apply_list_field_defaults(merged)
        return DrakkarConfig(**merged)

    env_overrides = _parse_env_overrides('DK_', '__')
    env_overrides = _apply_list_field_defaults(env_overrides)
    return DrakkarConfig(**env_overrides)


def _apply_list_field_defaults(merged: dict) -> dict:
    """Ensure list-of-objects env-var overrides do not erase default entries.

    When env-vars target individual list elements (e.g.
    ``DK_WEBAPP__CLIENTS__0__RPM=10``), the parser produces a partial list
    like ``[{'rpm': '10'}]`` with no other fields. If the YAML did not
    supply ``webapp.clients`` at all, Pydantic would now see this partial
    list as the entire value and reject it for missing required fields
    (``name``). To preserve the documented behaviour — env-vars override
    individual fields without forcing operators to repeat the defaults —
    we deep-merge the default ``WebAppConfig`` clients list under the
    partial override before construction.

    This is intentionally narrow (only ``webapp.clients`` for now). If
    another list-of-objects field needs the same treatment later, add it
    here with a small helper rather than introducing a generic mechanism.
    """
    webapp = merged.get('webapp')
    if not isinstance(webapp, dict):
        return merged
    clients_override = webapp.get('clients')
    if not isinstance(clients_override, list):
        return merged
    # Build the default clients list from the WebAppConfig default factory
    # and overlay the env-var override on top. We do this by dumping a
    # fresh WebAppConfig() to dict form so we are guaranteed to track any
    # future changes to the default list.
    default_clients = [c.model_dump() for c in WebAppConfig().clients]
    merged_clients = _deep_merge(default_clients, clients_override)
    new_webapp = dict(webapp)
    new_webapp['clients'] = merged_clients
    new_merged = dict(merged)
    new_merged['webapp'] = new_webapp
    return new_merged


def _parse_env_overrides(prefix: str, delimiter: str, *, skip_config_key: bool = True) -> dict:
    """Extract env vars with prefix, split by delimiter into nested dict.

    Numeric path segments are detected and the surrounding dict is
    converted to a list (e.g. ``DK_WEBAPP__CLIENTS__0__RPM=10`` becomes
    ``{'webapp': {'clients': [{'rpm': '10'}]}}``). This lets list-of-objects
    config fields (like ``webapp.clients``) be overridden by env vars in
    the same nested-delimiter style as scalar fields.

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
    and so ``_deep_merge`` can later overlay it onto a YAML-supplied list
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


def _deep_merge(base: Any, override: Any) -> Any:
    """Deep-merge override on top of base. Override wins for leaf values.

    When both sides are lists, merge element-by-element by index: the
    override's i-th element overrides base's i-th element (recursively
    if both are dicts), and any extra base elements past the override's
    length are preserved. This supports the env-var override pattern
    where ``DK_WEBAPP__CLIENTS__0__RPM=10`` should change only the first
    client's rpm without dropping the rest of the clients defined in YAML.
    """
    if isinstance(base, dict) and isinstance(override, dict):
        result_dict: dict[Any, Any] = dict(base)
        for key, val in override.items():
            if key in result_dict:
                result_dict[key] = _deep_merge(result_dict[key], val)
            else:
                result_dict[key] = val
        return result_dict
    if isinstance(base, list) and isinstance(override, list):
        merged_list: list[Any] = []
        for i in range(max(len(base), len(override))):
            if i < len(base) and i < len(override):
                merged_list.append(_deep_merge(base[i], override[i]))
            elif i < len(override):
                merged_list.append(override[i])
            else:
                merged_list.append(base[i])
        return merged_list
    # Leaf or type mismatch: override wins.
    return override
