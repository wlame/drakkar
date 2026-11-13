"""The lower layers of the package never import the layers above them.

The rules are data: each row names an area of the package and the areas it must
not import at run time. An import under ``TYPE_CHECKING`` does not count,
because it creates no dependency when the module loads.
"""

import ast
from pathlib import Path

import pytest

import drakkar

PACKAGE_ROOT = Path(drakkar.__file__).parent

# The areas that wire the worker together or serve it. Nothing below may reach up to them.
ORCHESTRATION = frozenset({'app', 'lifecycle', 'partition', 'uiserver', 'webapp'})

FORBIDDEN_IMPORTS: dict[str, frozenset[str]] = {
    'models': frozenset({'*'}),
    'offsets': frozenset({'*'}),
    'config': ORCHESTRATION | {'recorder', 'sources', 'executor', 'cache'},
    'sinks': ORCHESTRATION | {'recorder', 'sources', 'cache'},
    'providers': ORCHESTRATION | {'recorder', 'sources', 'sinks', 'cache', 'fleet'},
    'fleet': ORCHESTRATION | {'recorder', 'sources', 'sinks', 'cache'},
    'cache': ORCHESTRATION | {'recorder', 'sources', 'sinks'},
    'executor': ORCHESTRATION | {'recorder', 'sources', 'sinks', 'cache'},
    'budget': ORCHESTRATION | {'recorder', 'sources', 'sinks', 'cache'},
}


def area_of(path: Path) -> str:
    """The top-level area a module belongs to: its sub-package, or its own name."""
    parts = path.relative_to(PACKAGE_ROOT).parts
    return parts[0] if len(parts) > 1 else path.stem


def is_type_checking_block(node: ast.AST) -> bool:
    return isinstance(node, ast.If) and 'TYPE_CHECKING' in ast.dump(node.test)


def runtime_imports(tree: ast.AST) -> set[str]:
    """The ``drakkar`` areas a module imports when it runs, imports inside functions included."""
    found: set[str] = set()

    def visit(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if is_type_checking_block(child):
                continue
            if isinstance(child, ast.ImportFrom) and child.module and child.module.startswith('drakkar.'):
                found.add(child.module.split('.')[1])
            elif isinstance(child, ast.ImportFrom) and child.module == 'drakkar':
                found.update(alias.name for alias in child.names)
            elif isinstance(child, ast.Import):
                found.update(a.name.split('.')[1] for a in child.names if a.name.startswith('drakkar.'))
            visit(child)

    visit(tree)
    return found


def imports_by_area() -> dict[str, dict[str, set[str]]]:
    """For each area, the areas it imports and the files that do so."""
    result: dict[str, dict[str, set[str]]] = {}
    for path in sorted(PACKAGE_ROOT.rglob('*.py')):
        source_area = area_of(path)
        for target in runtime_imports(ast.parse(path.read_text(encoding='utf-8'))):
            if target != source_area:
                result.setdefault(source_area, {}).setdefault(target, set()).add(path.name)
    return result


IMPORTS = imports_by_area()


@pytest.mark.parametrize('area', sorted(FORBIDDEN_IMPORTS))
def test_area_does_not_import_the_layers_above_it(area):
    forbidden = FORBIDDEN_IMPORTS[area]
    imported = IMPORTS.get(area, {})
    broken = imported if '*' in forbidden else {name: files for name, files in imported.items() if name in forbidden}

    assert not broken, f'{area} imports {sorted(broken)} in {sorted(set().union(*broken.values()))}'


def test_every_rule_names_an_area_that_exists():
    areas = {area_of(path) for path in PACKAGE_ROOT.rglob('*.py')}

    assert set(FORBIDDEN_IMPORTS) <= areas
    assert set().union(*FORBIDDEN_IMPORTS.values()) - {'*'} <= areas
