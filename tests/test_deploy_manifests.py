"""The shipped Kubernetes reference manifests must load through the real config.

deploy/ has no other consumer — no import, no CI job, no lint rule parses it —
so a config section that moves (as debug.* -> ui.* did) leaves these manifests
silently broken until an operator applies them and every pod crash-loops.
"""

from pathlib import Path

import pytest
import yaml

from drakkar.config import DrakkarConfig

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


def test_deploy_dir_ships_at_least_one_worker_config():
    """Guards the guard: a rename that empties the glob must fail, not pass vacuously."""
    assert _worker_configs(), f'no embedded drakkar.yaml found under {DEPLOY_DIR}'


@pytest.mark.parametrize('path,values', _worker_configs(), ids=lambda v: getattr(v, 'name', ''))
def test_shipped_manifest_loads_through_real_config(path, values):
    """A shipped manifest that DrakkarConfig rejects would crash-loop every pod."""
    DrakkarConfig(**values)
