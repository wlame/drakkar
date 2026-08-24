"""Config-reference JSON API: ``GET /api/v1/config-reference``.

Joins the static field catalogue from :mod:`drakkar.configmeta` with the
live worker's actual :class:`~drakkar.config.DrakkarConfig` values, so the
Configs debug-UI tab can render "every field Drakkar accepts, and what this
worker is actually running" in one call.

Dynamic-instance fields (metadata paths carrying a literal ``*`` segment,
e.g. ``sinks.kafka.*.topic``) are expanded against the live config: one
entry per actually-configured instance, PLUS the unexpanded ``*`` template
entry itself (``value=None``). The template entry is kept even when zero
instances exist — dropping it would make an entirely-unconfigured sink type
(no ``sinks.postgres:`` block at all) invisible in a tab whose whole point
is discoverability of every field Drakkar accepts, not just the ones a
given deployment happens to use.

Secret masking happens here, not in :mod:`drakkar.configmeta` (which never
sees a live value): any field with ``secret=True`` whose live value is
non-empty is replaced by :data:`SECRET_MASK`. ``webapp.clients`` is a
special case — see its docstring caveat in ``drakkar.configmeta._walk`` —
because it is one un-decomposed ``array`` leaf whose elements embed a
per-client ``token`` that IS secret at the ``WebClientConfig`` model level
but has no per-element path in the metadata tree to hang a ``secret`` flag
on. This module masks inside the list by hand instead of trusting the
top-level flag.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from drakkar.configmeta import ConfigFieldMeta, _to_jsonable, build_config_metadata, walk_model_fields
from drakkar.recorder.helpers import sanitize_env_value

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

# Backend-chosen mask, deliberately distinct from pydantic's own SecretStr
# placeholder ('**********'): every secret field the endpoint touches ends
# up showing this exact string, regardless of whether the underlying config
# field is a plain ``str`` (dsn/uri/url/headers/auth_token — the raw secret
# would otherwise reach this module untouched) or a SecretStr (sasl/ssl
# passwords — pydantic already masks those in model_dump(mode='json'), but
# with its own placeholder, so this normalizes both cases to one contract).
SECRET_MASK = '•' * 6  # '••••••'


class ConfigReferenceEntry(BaseModel):
    """One config field, joined against the live worker's actual value."""

    path: str
    env: str | None
    description: str
    full_description: str
    type: str
    value: Any
    """The live value at this path, secret-masked. ``None`` for an
    unexpanded ``*`` template entry (no single value exists)."""
    default: Any
    """The field's default, secret-masked defensively (see module docstring)."""
    is_default: bool
    """``value == default`` computed BEFORE masking. Always ``True`` for an
    unexpanded ``*`` template entry — there is no live value to compare."""
    secret: bool


class ConfigReferenceGroup(BaseModel):
    key: str
    title: str
    doc_anchor: str
    entries: list[ConfigReferenceEntry]


class ConfigReferenceResponse(BaseModel):
    groups: list[ConfigReferenceGroup]


def _is_secret_worthy(value: Any) -> bool:
    """True when ``value`` is non-empty and thus worth masking.

    Every secret field in the config tree is a string or a dict (headers) —
    see the module docstring — so falsy (``''``, ``{}``, ``None``) reliably
    means "not actually set" and is left visible rather than masked into a
    bullet string that would otherwise hide "this operator never configured
    a credential" behind "this operator configured a credential".
    """
    return bool(value)


def _mask(value: Any, secret: bool) -> Any:
    return SECRET_MASK if secret and _is_secret_worthy(value) else value


def _expand_path(node: Any, segments: list[str]) -> list[tuple[list[str], Any]]:
    """Walk ``segments`` (literal names and/or ``*`` wildcards) against ``node``.

    A literal segment descends one level and yields nothing if the key is
    absent (defensive — a validated ``DrakkarConfig`` dump always has every
    field, so this only guards against a metadata/live-config mismatch). A
    ``*`` segment fans out over every key of the dict found there, in
    sorted order for a deterministic response. Multiple ``*`` segments in
    one path (e.g. ``sinks.mongo.*.statements.*.collection``, one level per
    dynamic map) compose naturally through the recursion.

    Returns one ``(concrete_segments, leaf_value)`` pair per match — exactly
    one for a path with no ``*``, zero or more for a templated one.
    """
    if not segments:
        return [([], node)]
    head, rest = segments[0], segments[1:]
    if head == '*':
        if not isinstance(node, dict):
            return []
        matches: list[tuple[list[str], Any]] = []
        for key in sorted(node):
            for tail_segments, value in _expand_path(node[key], rest):
                matches.append(([key, *tail_segments], value))
        return matches
    if not isinstance(node, dict) or head not in node:
        return []
    return [([head, *tail_segments], value) for tail_segments, value in _expand_path(node[head], rest)]


def _mask_webapp_clients(clients: Any) -> Any:
    """Deep-copy ``webapp.clients`` and mask each element's non-empty ``token``.

    See the module docstring: the metadata tree has no per-element path for
    ``WebClientConfig.token``, so the top-level ``secret`` flag (always
    False for this array leaf) cannot be trusted to cover it.
    """
    if not isinstance(clients, list):
        return clients
    masked = copy.deepcopy(clients)
    for client in masked:
        if isinstance(client, dict) and _is_secret_worthy(client.get('token')):
            client['token'] = SECRET_MASK
    return masked


def _mask_env_dict(values: Any) -> Any:
    """Mask a ``{name: value}`` config map the way the recorder masks env.

    ``executor.env`` is the documented way to hand credentials to the
    handler binary, and ``*.client_config`` is librdkafka passthrough where
    only four keys are reserved — so ``sasl.password`` and
    ``ssl.key.password`` can and do appear. Both are plain dict leaves, so
    the metadata's per-field ``secret`` flag cannot describe them: the
    secret is one key inside the value, not the value itself.

    Delegating to :func:`sanitize_env_value` is the point. The recorder
    already sanitizes the SAME ``executor.env`` by key name before storing
    it (``recorder/core.py``), so the two surfaces used to disagree about
    what counts as a secret. Now one function decides for both: redact
    fully on a secret-looking key name, otherwise strip credentials out of
    URL-shaped values.
    """
    if not isinstance(values, dict):
        return values
    return {
        name: sanitize_env_value(name, value) if isinstance(value, str) else value for name, value in values.items()
    }


# Fields whose secret lives BELOW the leaf, so the metadata's ``secret``
# flag cannot express it and a dedicated masker has to. Keyed by the
# metadata path, wildcards included — matched against ``field_meta.path``,
# which is the un-expanded template.
_DEEP_MASKERS: dict[str, Callable[[Any], Any]] = {
    'webapp.clients': _mask_webapp_clients,
    'executor.env': _mask_env_dict,
    'kafka.client_config': _mask_env_dict,
    'dlq.client_config': _mask_env_dict,
    'sinks.kafka.*.client_config': _mask_env_dict,
}


def _build_entries(field_meta: ConfigFieldMeta, config_dump: dict[str, Any]) -> list[ConfigReferenceEntry]:
    """Join one metadata field against the live config, returning 1+ response entries."""
    segments = field_meta.path.split('.')
    is_dynamic = '*' in segments
    deep_masker = _DEEP_MASKERS.get(field_meta.path)
    if deep_masker is not None:
        # Defense-in-depth: these defaults are empty today, so this is a
        # no-op in practice — but route the default through the same masker
        # as the live value so a future non-empty default cannot leak.
        masked_default = deep_masker(field_meta.default)
    else:
        masked_default = _mask(field_meta.default, field_meta.secret)

    entries: list[ConfigReferenceEntry] = []

    if is_dynamic:
        # The template entry itself, always emitted — see module docstring
        # for why zero configured instances must not hide the key.
        entries.append(
            ConfigReferenceEntry(
                path=field_meta.path,
                env=None,
                description=field_meta.description,
                full_description=field_meta.full_description,
                type=field_meta.type,
                value=None,
                default=masked_default,
                is_default=True,
                secret=field_meta.secret,
            )
        )

    matches = _expand_path(config_dump, segments)
    for concrete_segments, raw_value in matches:
        is_default = raw_value == field_meta.default
        value = deep_masker(raw_value) if deep_masker is not None else _mask(raw_value, field_meta.secret)
        entries.append(
            ConfigReferenceEntry(
                path='.'.join(concrete_segments),
                env=None if is_dynamic else field_meta.env,
                description=field_meta.description,
                full_description=field_meta.full_description,
                type=field_meta.type,
                value=value,
                default=masked_default,
                is_default=is_default,
                secret=field_meta.secret,
            )
        )

    return entries


def build_config_reference(config_dump: dict[str, Any]) -> ConfigReferenceResponse:
    """Join the static metadata catalogue against one live config dump.

    ``config_dump`` is ``DrakkarConfig.model_dump(mode='json')`` — dumped
    once by the caller so every field in this pass sees a consistent
    snapshot rather than each entry re-walking a live (mutable) config
    object.
    """
    groups = []
    for group_meta in build_config_metadata().groups:
        entries: list[ConfigReferenceEntry] = []
        for field_meta in group_meta.entries:
            entries.extend(_build_entries(field_meta, config_dump))
        groups.append(
            ConfigReferenceGroup(
                key=group_meta.key,
                title=group_meta.title,
                doc_anchor=group_meta.doc_anchor,
                entries=entries,
            )
        )
    return ConfigReferenceResponse(groups=groups)


def build_app_config_group(instance: BaseModel, env_prefix: str) -> ConfigReferenceGroup:
    """Build the runtime ``app`` group from a handler-declared app config.

    The static artifact (drakkar/configmeta.py) cannot describe the user's
    model — it only exists at runtime on the live handler — so this group
    is built per-request from the SAME walk machinery the framework fields
    use: descriptions, types, defaults, and nesting all come out exactly
    like the vendored entries. Entry paths carry the literal ``app.``
    prefix (matching how framework entries carry their full YAML path);
    env names use the handler's own prefix with the ``app`` segment
    dropped (``app.scoring.url`` -> ``MYAPP_SCORING__URL``). Secrets mask
    for both ``SecretStr`` annotations and the framework's
    ``json_schema_extra={'drakkar_secret': True}`` marker — the walk flags
    either, and masking itself reuses :data:`SECRET_MASK`.
    """
    field_metas = walk_model_fields(
        type(instance),
        path_prefix=['app'],
        group='app',
        env_prefix=env_prefix,
        env_skip_segments=1,
    )
    # A python-mode dump keeps SecretStr instances, which ``_to_jsonable``
    # unwraps to their real value — the same convention the metadata
    # defaults use — so ``is_default`` compares real value to real default
    # BEFORE ``_build_entries`` masks both for the response. (A json-mode
    # dump would pre-mask live SecretStr values with pydantic's own
    # placeholder and break that comparison.)
    live_dump = {'app': _to_jsonable(instance.model_dump(mode='python'))}
    entries: list[ConfigReferenceEntry] = []
    for field_meta in field_metas:
        entries.extend(_build_entries(field_meta, live_dump))
    return ConfigReferenceGroup(key='app', title='Application', doc_anchor='app-config', entries=entries)


def create_config_reference_router(deps: UIDeps) -> APIRouter:
    """Build the router owning ``GET /api/v1/config-reference`` (v1-only, no legacy alias)."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])

    @router.get('/api/v1/config-reference')
    async def api_config_reference() -> ConfigReferenceResponse:
        """Every config field Drakkar accepts, joined with this worker's live values.

        Static fields carry their live value straight from the running
        ``DrakkarConfig``. Dynamic per-instance fields (``sinks.*.*.<field>``
        and the nested ``sinks.mongo.*.statements.*.<field>``) are expanded
        into one entry per actually-configured instance, in addition to the
        unexpanded ``*`` template entry (``value=null``) that documents the
        key exists even with zero instances configured — see the module
        docstring for the discoverability rationale.

        Every ``secret``-flagged field with a non-empty live value is
        replaced by a fixed six-bullet mask before it leaves the process;
        ``webapp.clients`` is masked element-by-element (its ``token``
        field) since it has no per-element metadata path of its own.
        """
        config_dump = deps.drakkar_app._config.model_dump(mode='json')
        response = build_config_reference(config_dump)
        # Contract v1.17: one extra runtime group for the handler-declared
        # application config (docs/app-config.md). getattr-guarded because
        # tests mount this router over a MagicMock app without a handler;
        # a handler without a declared model has app_config None and the
        # group is simply absent.
        handler = getattr(deps.drakkar_app, 'handler', None)
        app_config = getattr(handler, 'app_config', None)
        if isinstance(app_config, BaseModel):
            env_prefix = getattr(handler, 'app_env_prefix', 'APP_')
            response.groups.append(build_app_config_group(app_config, env_prefix))
        return response

    return router
