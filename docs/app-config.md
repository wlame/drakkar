# App Config (your own settings in drakkar.yaml)

Business logic needs configuration too — thresholds, service URLs, feature
switches, credentials for the systems *your* hooks call. Without framework
support that config ends up in a second file with its own loader, its own
env-var story, and its own validation bugs. This feature removes the
second file: you declare your settings as an ordinary Pydantic model, and
the framework delivers a validated instance from the **same
`drakkar.yaml`** the worker already reads — the reserved top-level `app:`
section — with your **own env-var prefix** and the framework's exact
precedence:

**model defaults → YAML `app:` section → env vars (with `__` nesting)**

Validation is fail-fast at startup: a typo'd value stops the worker with a
clear error instead of surfacing mid-pipeline. The loaded instance is
available as `self.app_config` in every hook — including `on_startup` —
and appears in the Debug UI's config reference as one more group, with
secrets masked.

## Declaring the model

Two class attributes opt a handler in:

```python
from pydantic import BaseModel, Field, SecretStr

from drakkar import BaseDrakkarHandler


class AppConfig(BaseModel):
    priority_threshold: int = Field(
        default=10,
        description='Tasks scoring above this are prioritized.',
    )
    scoring_url: str = 'http://localhost:9000/score'
    api_key: SecretStr = SecretStr('')


class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    app_config_model = AppConfig   # opt-in: the framework loads it at startup
    app_env_prefix = 'MYAPP_'      # your env namespace (default: 'APP_')

    async def arrange(self, messages, pending):
        if self.app_config.priority_threshold > 0:
            ...
```

`self.app_config` is the validated `AppConfig` instance — typed, with
defaults applied. It is `None` only when the handler declares no
`app_config_model`.

`Field(description=...)` is worth writing: descriptions, types, and
defaults all surface in the Debug UI's config reference exactly like the
framework's own fields.

## The `app:` section

```yaml
# drakkar.yaml — framework sections and your section, one file
kafka:
  source_topic: input-events

app:
  priority_threshold: 20
  scoring_url: "http://scoring-service:9000/score"
```

The framework passes the section through **unvalidated** — its shape is
entirely yours; only your model judges it. Nested models nest as YAML
mappings, exactly like framework sections.

If the YAML carries a non-empty `app:` section but the handler declares no
`app_config_model`, the worker logs an `app_config_ignored` warning at
startup — a typo'd setup is loud, not silent.

## Env-var overrides

Your prefix, the framework's syntax — `__` between nesting levels:

```bash
MYAPP_PRIORITY_THRESHOLD=20            # app.priority_threshold
MYAPP_SCORING_URL=http://scoring:9000  # app.scoring_url
MYAPP_API_KEY=s3cret                   # app.api_key
MYAPP_SCORING__TIMEOUT_SECONDS=15      # nested: app.scoring.timeout_seconds
```

Values are strings in the environment; your model's types coerce them
(`"20"` → `int`, `"true"` → `bool`), like the framework config.

**`DK_APP__*` is rejected.** The `DK_` namespace belongs to the framework,
and the `app:` section does not: setting any `DK_APP__*` variable fails
startup with a message naming the offending variables and pointing at your
handler's own prefix. Likewise, `app_env_prefix` itself must not start
with `DK_` — the loader refuses it.

## Secrets

Two conventions mark a field as secret; the Debug UI masks either:

- **`SecretStr`** — the Pydantic type. Also protects against accidental
  `repr()`/log leaks in your own code, so prefer it for new fields.
- **`json_schema_extra={'drakkar_secret': True}`** on a plain field — the
  marker convention the framework's own config uses (for fields like DSNs
  where the code needs the raw string).

```python
class AppConfig(BaseModel):
    api_key: SecretStr = SecretStr('')
    webhook_url: str = Field(default='', json_schema_extra={'drakkar_secret': True})
```

In the config reference, a non-empty secret value renders as `••••••`
(same mask as framework secrets); an empty one stays visibly empty so
"never configured" is distinguishable from "configured and hidden".

## The config reference group

`GET /api/v1/config-reference` — the Configs tab in the Debug UI — gains
one more group, `Application`, built at runtime from your model: one entry
per field (nested models walked recursively) with path (`app.scoring.url`),
env name (`MYAPP_SCORING__URL`), type, description, default, live value,
and an `is_default` flag, secrets masked. The group is simply absent when
no model is declared.

## Standalone loading

The same loader is public, for scripts and tools that need the app config
without a running worker:

```python
from drakkar import load_app_config

app_config = load_app_config(AppConfig, '/etc/drakkar/drakkar.yaml', env_prefix='MYAPP_')
```

Path resolution mirrors `load_config`: explicit argument → the
`DK_CONFIG` env var → env-only (defaults + your env vars, no file). A
`section=` argument selects a different top-level key than `app`, and
`yaml_data=` accepts an already-loaded dict instead of a file.

## Notes

- Precedence and merge semantics are identical to the framework config —
  see [Configuration Loading](configuration.md#configuration-loading).
- The `app:` section never appears in the one-line config summary the
  worker logs at startup.
- The reserved `app:` section is part of the config contract, with its own
  model binding, so a mixed fleet shares one YAML layout.
