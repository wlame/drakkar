"""The shipped Kubernetes reference manifests must load through the real config.

deploy/ has no other consumer — no import, no CI job, no lint rule parses it —
so a config section that moves (as debug.* -> ui.* did) leaves these manifests
silently broken until an operator applies them and every pod crash-loops.
"""

from pathlib import Path

import pytest
import yaml

from drakkar.config import DrakkarConfig, UIRecorderConfig

DEPLOY_DIR = Path(__file__).parent.parent / 'deploy'


def _worker_configs() -> list[tuple[Path, dict]]:
    """Every embedded drakkar.yaml in the shipped manifests, with its source path."""
    found: list[tuple[Path, dict]] = []
    for path in sorted(DEPLOY_DIR.rglob('*.yaml')):
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict):
                continue
            data = doc.get('data')
            if isinstance(data, dict) and 'drakkar.yaml' in data:
                found.append((path, yaml.safe_load(data['drakkar.yaml'])))
    return found


def _sibling_writable_volume_mount_paths(config_path: Path) -> set[str]:
    """Every non-read-only container volumeMount path from Deployment
    manifests alongside a ConfigMap, so a recorder db_dir can be checked
    against the volume actually mounted for it rather than trusted blindly.
    """
    mount_paths: set[str] = set()
    for sibling in config_path.parent.glob('*.yaml'):
        for doc in yaml.safe_load_all(sibling.read_text()):
            if not isinstance(doc, dict) or doc.get('kind') != 'Deployment':
                continue
            pod_spec = doc['spec']['template']['spec']
            for container in pod_spec.get('containers', []):
                for volume_mount in container.get('volumeMounts', []):
                    if not volume_mount.get('readOnly', False):
                        mount_paths.add(volume_mount['mountPath'])
    return mount_paths


def test_deploy_dir_ships_at_least_one_worker_config():
    """Guards the guard: a rename that empties the glob must fail, not pass vacuously."""
    assert _worker_configs(), f'no embedded drakkar.yaml found under {DEPLOY_DIR}'


@pytest.mark.parametrize('path,values', _worker_configs(), ids=lambda v: getattr(v, 'name', ''))
def test_shipped_manifest_loads_through_real_config(path, values):
    """A shipped manifest that DrakkarConfig rejects would crash-loop every pod."""
    DrakkarConfig(**values)


@pytest.mark.parametrize('path,values', _worker_configs(), ids=lambda v: getattr(v, 'name', ''))
def test_shipped_manifest_recorder_db_dir_matches_a_writable_volume(path, values):
    """The effective ``ui.recorder.db_dir`` must resolve to a real writable
    volumeMount declared by the paired Deployment, not to pydantic's
    built-in default.

    ``DrakkarConfig(**values)`` not raising is a much weaker guarantee than
    it looks: every nested section keeps pydantic's default
    ``extra='ignore'``, so regressing ``ui.recorder.db_dir`` back to a flat
    ``ui.db_dir`` -- exactly the shape the ``debug.*`` -> ``ui.*`` move
    repaired -- is accepted silently, and the recorder falls back to the
    class default (``/tmp``) with no crash and no CI signal, landing
    recorder/cache data on the wrong volume.
    """
    config = DrakkarConfig(**values)
    db_dir = config.ui.recorder.db_dir
    default_db_dir = UIRecorderConfig.model_fields['db_dir'].default
    assert db_dir != default_db_dir, (
        f'{path}: ui.recorder.db_dir resolved to the unset pydantic default '
        f'{default_db_dir!r} -- check for a flattened or misplaced key'
    )
    writable_mount_paths = _sibling_writable_volume_mount_paths(path)
    assert writable_mount_paths, f'{path}: no writable volumeMount found in a paired Deployment manifest'
    assert db_dir in writable_mount_paths, (
        f'{path}: ui.recorder.db_dir={db_dir!r} is not backed by any writable volumeMount '
        f'declared in a paired Deployment manifest ({sorted(writable_mount_paths)})'
    )
