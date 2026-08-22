"""Loader for user-defined application config (see docs/app-config.md).

End users declare their business-logic settings as an ordinary Pydantic
model and receive a validated instance from the SAME ``drakkar.yaml`` the
framework reads — the reserved top-level ``app:`` section — plus their own
environment-variable prefix. Precedence mirrors the framework config
exactly: model defaults → YAML section → env vars with ``__`` nesting.

The usual entry point is declarative — a handler sets
``app_config_model`` / ``app_env_prefix`` class attributes and reads
``self.app_config`` (see :class:`drakkar.handler.BaseDrakkarHandler`).
:func:`load_app_config` is the standalone helper behind that wiring,
public so scripts and tests can load the same config without a running
app.
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

# Deliberately reuses the framework loader's own env-parsing and merge
# machinery so the app section cannot drift from the framework's override
# semantics (same ``__`` nesting, same deep-merge rules, same numeric-
# segment list handling).
from drakkar.config import _deep_merge, _parse_env_overrides


def load_app_config[ModelT: BaseModel](
    model: type[ModelT],
    config_path: str | Path | None = None,
    *,
    section: str = 'app',
    env_prefix: str = 'APP_',
    yaml_data: dict[str, Any] | None = None,
) -> ModelT:
    """Load and validate a user-defined application config model.

    Precedence (lowest to highest): ``model`` field defaults → the YAML
    ``section`` → environment variables starting with ``env_prefix``,
    nested with ``__`` (e.g. ``MYAPP_SCORING__URL`` sets
    ``scoring.url``). Type coercion is pydantic validation, exactly like
    the framework config — env strings such as ``"20"`` or ``"true"``
    coerce into ``int`` / ``bool`` fields.

    Args:
        model: The user's Pydantic model class.
        config_path: YAML file to read. Resolution mirrors
            :func:`drakkar.config.load_config` — explicit argument, then
            the ``DK_CONFIG`` env var, then env-only (defaults + env
            overrides, no file). Ignored when ``yaml_data`` is given.
        section: Top-level YAML key holding the app config
            (default ``'app'``, the framework's reserved section).
        env_prefix: The application's own env-var prefix. Must not start
            with ``DK_`` — that namespace belongs to the framework.
        yaml_data: Already-loaded YAML dict (the whole file, not the
            section), letting the app wiring skip a second file read.

    Returns:
        A validated instance of ``model``.

    Raises:
        ValueError: On a ``DK_``-colliding prefix, a non-mapping section,
            or validation failure — the message names the section and the
            model so a startup failure is actionable.
        FileNotFoundError: When an explicitly given config path is missing.
    """
    if env_prefix.startswith('DK_'):
        raise ValueError(
            f"env_prefix {env_prefix!r} collides with the framework's DK_ namespace "
            '(all DK_* variables are parsed as framework config overrides); '
            "pick an application-specific prefix like 'MYAPP_'"
        )

    if yaml_data is None:
        if config_path is None:
            config_path = os.environ.get('DK_CONFIG')
        if config_path is not None:
            path = Path(config_path)
            if not path.exists():
                raise FileNotFoundError(f'Config file not found: {path}')
            with open(path) as f:
                yaml_data = yaml.safe_load(f) or {}
        else:
            yaml_data = {}

    # A missing or empty section is fine — the model's defaults plus env
    # overrides still produce a valid instance (or a clear error listing
    # the required fields).
    section_data = yaml_data.get(section) or {}
    if not isinstance(section_data, dict):
        raise ValueError(
            f'Config section {section!r} must be a mapping for model {model.__name__}, '
            f'got {type(section_data).__name__}'
        )

    env_overrides = _parse_env_overrides(env_prefix, '__', skip_config_key=False)
    merged = _deep_merge(section_data, env_overrides)

    try:
        return model.model_validate(merged)
    except ValidationError as e:
        raise ValueError(f'Invalid config in section {section!r} for app config model {model.__name__}: {e}') from e
