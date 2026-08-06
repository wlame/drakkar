"""Tests for the canonical config-metadata generator (drakkar/configmeta.py).

Guards three things: the committed artifact never drifts from the live
model tree, every doc_anchor actually exists on the docs page it targets,
and every leaf field DrakkarConfig accepts is represented exactly once.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import get_args, get_origin

from pydantic import BaseModel

from drakkar.config import DrakkarConfig, KafkaConfig
from drakkar.configmeta import (
    ARTIFACT_PATH,
    ConfigMetadata,
    _first_sentence,
    _is_basemodel,
    _json_type,
    _mkdocs_anchor,
    _to_jsonable,
    build_config_metadata,
)
from drakkar.models import MongoOp

REPO_ROOT = Path(__file__).parent.parent
DOCS_PAGE = REPO_ROOT / 'docs' / 'config-reference.md'


# --- 1. Regenerate-and-compare: guards drift between model tree and artifact ---


def test_artifact_matches_freshly_built_metadata():
    """The committed JSON must be byte-identical to a fresh build.

    If this fails, a config model changed without regenerating the
    artifact. Fix with: `python -m drakkar.configmeta` (from the repo
    root), then commit the updated drakkar/uiserver/config-metadata.json.
    """
    committed = ARTIFACT_PATH.read_text()
    fresh = build_config_metadata().model_dump_json(indent=2) + '\n'
    assert committed == fresh, (
        'drakkar/uiserver/config-metadata.json is stale relative to the '
        'live DrakkarConfig model tree. Regenerate with: '
        '`python -m drakkar.configmeta`, then commit the result.'
    )


def test_artifact_is_valid_json_matching_the_schema():
    raw = json.loads(ARTIFACT_PATH.read_text())
    # Round-trips through the pydantic model without error.
    parsed = ConfigMetadata.model_validate(raw)
    assert len(parsed.groups) == 10


# --- 2. Anchor parity: every doc_anchor must exist on the real docs page ---


def _doc_heading_anchors() -> set[str]:
    """Compute the mkdocs anchor for every `##` heading in config-reference.md."""
    headings = []
    for line in DOCS_PAGE.read_text().splitlines():
        m = re.match(r'^##\s+(.*)$', line)
        if m:
            headings.append(m.group(1).strip())
    assert headings, 'expected to find at least one ## heading in config-reference.md'
    return {_mkdocs_anchor(h) for h in headings}


def test_mkdocs_anchor_matches_known_examples():
    """Pin the slug algorithm against anchors verified on the live site.

    Verified by fetching https://wlame.github.io/drakkar/config-reference/
    and reading the rendered heading `id=` attributes directly, rather than
    assumed from the algorithm description alone.
    """
    assert _mkdocs_anchor('Worker identity') == 'worker-identity'
    assert _mkdocs_anchor('Kafka source (`kafka:`)') == 'kafka-source-kafka'
    assert _mkdocs_anchor('Dead letter queue (`dlq:`)') == 'dead-letter-queue-dlq'
    assert _mkdocs_anchor('UI / Flight Recorder (`ui:`)') == 'ui-flight-recorder-ui'
    assert _mkdocs_anchor('Circuit breaker (shared default)') == 'circuit-breaker-shared-default'


def test_every_group_doc_anchor_exists_on_the_docs_page():
    real_anchors = _doc_heading_anchors()
    metadata = build_config_metadata()
    assert metadata.groups, 'expected at least one config group'
    for group in metadata.groups:
        assert group.doc_anchor in real_anchors, (
            f'group {group.key!r} points at doc_anchor {group.doc_anchor!r}, '
            f'which is not a ## heading anchor in docs/config-reference.md'
        )


def test_group_anchors_cover_every_real_config_section():
    """The 10 config-bearing ## headings (excludes the trailing cheatsheet section)."""
    real_anchors = _doc_heading_anchors()
    # The cheatsheet section documents env-var syntax, not a config group.
    real_anchors.discard('environment-variable-override-cheatsheet')
    metadata = build_config_metadata()
    mapped_anchors = {group.doc_anchor for group in metadata.groups}
    assert mapped_anchors == real_anchors


# --- 3. Coverage: every leaf field in DrakkarConfig appears exactly once ---


def _independent_leaf_paths(model_cls: type[BaseModel], prefix: str = '') -> set[str]:
    """Recompute the set of leaf paths straight from pydantic model_fields.

    Deliberately re-implemented here (not imported from drakkar.configmeta)
    so a bug in the production walker's traversal logic can't hide itself
    from its own test. Follows the same two traversal rules the module
    documents: a nested single-model field recurses in place, and a
    dict[str, SomeModel] field recurses through SomeModel with a literal
    `*` segment. list[SomeModel] fields (only webapp.clients) are NOT
    decomposed — they're one leaf, matching the production walker.
    """
    paths: set[str] = set()
    for name, info in model_cls.model_fields.items():
        annotation = info.annotation
        full_path = f'{prefix}.{name}' if prefix else name

        if _is_basemodel(annotation):
            paths |= _independent_leaf_paths(annotation, full_path)
            continue

        if get_origin(annotation) is dict:
            args = get_args(annotation)
            if len(args) == 2 and _is_basemodel(args[1]):
                paths |= _independent_leaf_paths(args[1], f'{full_path}.*')
                continue

        paths.add(full_path)
    return paths


def test_every_leaf_field_appears_exactly_once():
    expected_paths = _independent_leaf_paths(DrakkarConfig)
    metadata = build_config_metadata()

    actual_paths = [entry.path for group in metadata.groups for entry in group.entries]
    actual_path_set = set(actual_paths)

    assert len(actual_paths) == len(actual_path_set), 'a path appears more than once in the metadata'
    assert actual_path_set == expected_paths


# --- 4. Secrets: the known-sensitive minimum set is flagged ---


def _secret_paths() -> set[str]:
    metadata = build_config_metadata()
    return {entry.path for group in metadata.groups for entry in group.entries if entry.secret}


def test_known_sensitive_fields_are_flagged_secret():
    secret_paths = _secret_paths()
    expected_minimum = {
        'ui.auth_token',
        'kafka.security.sasl_password',
        'kafka.security.ssl_key_password',
        'dlq.security.sasl_password',
        'dlq.security.ssl_key_password',
        'sinks.kafka.*.security.sasl_password',
        'sinks.kafka.*.security.ssl_key_password',
        'sinks.postgres.*.dsn',
        'sinks.mongo.*.uri',
        'sinks.redis.*.url',
        'sinks.http.*.headers',
    }
    assert expected_minimum <= secret_paths

    # webapp.clients is `list[WebClientConfig]`, which the walker reports as
    # one array-typed leaf rather than decomposing per-element (see
    # _independent_leaf_paths' docstring) — so `webapp.clients.token` never
    # appears as its own metadata path. Confirm the underlying field is
    # still marked secret at the model level, so a future decomposition
    # would inherit the flag correctly.
    from drakkar.config import WebClientConfig

    assert WebClientConfig.model_fields['token'].json_schema_extra == {'drakkar_secret': True}


def test_non_secret_fields_are_not_flagged():
    secret_paths = _secret_paths()
    assert 'kafka.brokers' not in secret_paths
    assert 'sinks.http.*.url' not in secret_paths
    assert 'ui.public_url' not in secret_paths
    assert 'ui.prometheus_url' not in secret_paths


# --- 5. Env correctness ---


def test_env_var_names_for_known_scalar_paths():
    metadata = build_config_metadata()
    by_path = {entry.path: entry for group in metadata.groups for entry in group.entries}

    assert by_path['kafka.brokers'].env == 'DK_KAFKA__BROKERS'
    assert by_path['ui.recorder.db_dir'].env == 'DK_UI__RECORDER__DB_DIR'
    assert by_path['cluster_name'].env == 'DK_CLUSTER_NAME'


def test_env_is_none_for_dynamic_instance_paths():
    """No single fixed env var reaches a `*` (dynamic-instance) path.

    See the _ENV_NONE_FOR_DYNAMIC_PATHS note in drakkar/configmeta.py: env
    overrides for map keys are matched by literal (post-lowercase) string,
    with no hyphen/underscore reconciliation, so a hyphenated instance name
    like `main-db` is not reachable via `DK_..._MAIN_DB_...` at all —
    reporting a templated env name here would overstate what the loader
    actually guarantees.
    """
    metadata = build_config_metadata()
    dynamic_entries = [entry for group in metadata.groups for entry in group.entries if '*' in entry.path.split('.')]
    assert dynamic_entries, 'expected at least one dynamic-instance field'
    assert all(entry.env is None for entry in dynamic_entries)


def test_env_override_does_not_reach_hyphenated_instance_name(tmp_path, monkeypatch):
    """Empirical proof behind the env=None-for-`*`-paths decision above."""
    from drakkar.config import load_config

    config_file = tmp_path / 'drakkar.yaml'
    config_file.write_text('sinks:\n  postgres:\n    main-db:\n      dsn: "postgresql://orig"\n')
    monkeypatch.setenv('DK_SINKS__POSTGRES__MAIN_DB__DSN', 'postgresql://overridden')

    cfg = load_config(str(config_file))

    # The original hyphenated instance is untouched by the env override...
    assert cfg.sinks.postgres['main-db'].dsn == 'postgresql://orig'
    # ...and a second, separate instance was created instead of overriding it.
    assert cfg.sinks.postgres['main_db'].dsn == 'postgresql://overridden'


# --- 6. First-sentence extraction ---


def test_first_sentence_extraction_splits_at_first_terminator():
    text = 'First sentence here. Second sentence with more detail follows.'
    assert _first_sentence(text) == 'First sentence here.'


def test_first_sentence_extraction_empty_string_fallback():
    assert _first_sentence('') == ''


def test_first_sentence_extraction_single_sentence_unchanged():
    assert _first_sentence('Only one sentence.') == 'Only one sentence.'


def test_description_and_full_description_split_correctly_on_a_real_field():
    metadata = build_config_metadata()
    by_path = {entry.path: entry for group in metadata.groups for entry in group.entries}
    entry = by_path['sinks.postgres.*.statements']
    assert entry.description == 'Operator-authored SQL keyed by name.'
    assert entry.description != entry.full_description
    assert entry.full_description.startswith('Operator-authored SQL keyed by name.')
    assert len(entry.full_description) > len(entry.description)


def test_first_sentence_does_not_truncate_at_an_abbreviation():
    """Regression: `ui.release.pinned_version`'s description is ONE sentence
    containing '(e.g. "v1.2.0")' — a naive split-on-period would cut it
    right after 'e.g.', losing everything past the abbreviation. The `;`
    also does not end a sentence; only the final '.' after 'compatible' does.
    """
    metadata = build_config_metadata()
    by_path = {entry.path: entry for group in metadata.groups for entry in group.entries}
    entry = by_path['ui.release.pinned_version']
    assert entry.description == (
        'Known-good UI release tag this backend is built against (e.g. "v1.2.0"); the contract is API-major compatible.'
    )
    # The full description continues past the first sentence ('Empty means
    # "no pinned version".'), so description is a strict prefix of it.
    assert entry.full_description.startswith(entry.description)
    assert entry.description != entry.full_description


def test_no_generated_description_is_truncated_at_an_abbreviation_or_unbalanced():
    """Tree-wide sweep: no `description` in the committed metadata should end
    mid-abbreviation (a truncation bug would leave a dangling '(e.g.' or
    similar) or with unbalanced parentheses (a symptom of cutting inside a
    parenthetical)."""
    abbreviation_endings = ('e.g.', 'i.e.', 'etc.', 'vs.')
    metadata = build_config_metadata()
    for group in metadata.groups:
        for entry in group.entries:
            description = entry.description
            if not description:
                continue
            lowered = description.rstrip().lower()
            assert not any(lowered.endswith(a) for a in abbreviation_endings), (
                f'{entry.path}: description truncated at an abbreviation: {description!r}'
            )
            assert description.count('(') == description.count(')'), (
                f'{entry.path}: description has unbalanced parentheses: {description!r}'
            )


# --- misc: type mapping sanity, since configmeta.py's _json_type has no dedicated test above ---


def test_type_field_uses_simple_json_type_names():
    metadata = build_config_metadata()
    by_path = {entry.path: entry for group in metadata.groups for entry in group.entries}

    assert by_path['kafka.brokers'].type == 'string'
    assert by_path['kafka.max_poll_records'].type == 'integer'
    assert by_path['sinks.circuit_breaker.cooldown_seconds'].type == 'number'
    assert by_path['metrics.enabled'].type == 'boolean'
    assert by_path['ui.expose_env_vars'].type == 'array'
    assert by_path['executor.env'].type == 'object'
    assert by_path['sinks.mongo.*.statements.*.op'].type == 'string'


def test_regenerating_via_main_is_idempotent(tmp_path, monkeypatch):
    """`python -m drakkar.configmeta` writes exactly the committed content."""
    import drakkar.configmeta as configmeta_module

    out_path = tmp_path / 'config-metadata.json'
    # monkeypatch restores the real ARTIFACT_PATH automatically on teardown.
    monkeypatch.setattr(configmeta_module, 'ARTIFACT_PATH', out_path)
    configmeta_module._write_artifact()

    assert out_path.read_text() == ARTIFACT_PATH.read_text()


def test_module_entrypoint_regenerates_the_artifact_in_a_subprocess():
    """Exercises `python -m drakkar.configmeta` (the `__main__` block) end to end.

    Run out-of-process rather than via runpy: re-importing the module under
    `run_name='__main__'` while `drakkar.configmeta` is already in
    `sys.modules` produces a second, colliding `ConfigFieldMeta` class whose
    postponed (`from __future__ import annotations`) forward refs resolve
    against the wrong module globals — a real interaction, not a test
    artifact to work around. A subprocess sidesteps it entirely. The
    `__main__` block itself is two lines calling already-covered code
    (`_write_artifact`) plus a `print`, so it's marked `pragma: no cover`
    in the module rather than chasing that combination here too.
    """
    result = subprocess.run(
        [sys.executable, '-m', 'drakkar.configmeta'],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert 'Wrote' in result.stdout
    assert str(ARTIFACT_PATH) in result.stdout
    # Idempotent: regenerating writes back the same content already committed.
    assert ARTIFACT_PATH.read_text() == build_config_metadata().model_dump_json(indent=2) + '\n'


# --- direct unit coverage of the small pure helpers ---


def test_to_jsonable_unwraps_enum_to_its_value():
    assert _to_jsonable(MongoOp.UPDATE_ONE) == 'update_one'


def test_to_jsonable_passes_through_plain_scalars():
    assert _to_jsonable('x') == 'x'
    assert _to_jsonable(3) == 3
    assert _to_jsonable(None) is None


def test_json_type_multi_branch_union_prefers_array_when_no_object_present():
    assert _json_type(list[int] | set[int]) == 'array'


def test_json_type_multi_branch_union_falls_back_to_first_branch_kind():
    assert _json_type(int | float) == 'integer'


def test_json_type_unrecognized_annotation_falls_back_to_string():
    """Defensive catch-all: an annotation that's neither a type nor a
    recognized typing construct (e.g. a bare TypeVar-like sentinel) still
    yields a valid, if conservative, JSON type name instead of raising."""
    assert _json_type(object()) == 'string'


def test_json_type_basemodel_leaf_is_object():
    """Never hit through build_config_metadata (nested models always
    recurse rather than becoming a leaf), but _json_type must still be
    correct if ever called on one directly."""
    assert _json_type(KafkaConfig) == 'object'
