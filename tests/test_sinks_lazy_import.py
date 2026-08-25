"""``drakkar/sinks/__init__.py`` must stay free of module-scope imports.

``drakkar.config`` validates operator-authored SQL/MQL statements at load
time, so it imports the pure template helpers that live under
``drakkar/sinks/`` (``pgsql``, ``mql``, ``http_encoding``). Importing a
submodule executes its package's ``__init__`` first — so a top-level import
there that reaches ``drakkar.config``, as every sink class does through
``BaseSink``, would make config load fail with a partially-initialized
module. The package therefore resolves its re-exports through PEP 562
``__getattr__`` and registers built-in sinks from an import-path table.

The AST case pins the rule itself rather than a symptom, so it keeps
holding no matter which module happens to be imported first in a given
process.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import drakkar.sinks

SINKS_INIT = Path(drakkar.sinks.__file__)


def _module_scope_imports(path: Path) -> list[str]:
    """Return every module imported at module scope (``if TYPE_CHECKING`` excluded)."""
    tree = ast.parse(path.read_text())
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def test_sinks_package_has_no_module_scope_drakkar_imports() -> None:
    offenders = [name for name in _module_scope_imports(SINKS_INIT) if name.startswith('drakkar')]
    assert not offenders, f'drakkar/sinks/__init__.py imports {offenders} at module scope'


def test_template_helpers_import_nothing_from_drakkar() -> None:
    """The helpers themselves are the other half of the rule."""
    for module in ('pgsql.py', 'mql.py', 'http_encoding.py'):
        path = SINKS_INIT.parent / module
        offenders = [name for name in _module_scope_imports(path) if name.startswith('drakkar')]
        assert not offenders, f'{module} imports {offenders}'


def test_config_imports_the_helpers_from_their_sinks_home() -> None:
    """A fresh interpreter must be able to load the config module."""
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            'import drakkar.config as c; import drakkar.sinks.pgsql, drakkar.sinks.mql; print(c.DrakkarConfig().kafka.brokers)',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_reexports_resolve_through_module_getattr() -> None:
    from drakkar.sinks import AmbiguousSinkError, SinkManager, SinkNotConfiguredError

    assert SinkManager.__name__ == 'SinkManager'
    assert issubclass(AmbiguousSinkError, Exception)
    assert issubclass(SinkNotConfiguredError, Exception)
    assert sorted(dir(drakkar.sinks)) == sorted(drakkar.sinks.__all__)

    with pytest.raises(AttributeError, match='no attribute'):
        _ = drakkar.sinks.NotAThing  # type: ignore[attr-defined]
