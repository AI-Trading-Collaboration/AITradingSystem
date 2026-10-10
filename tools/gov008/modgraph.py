"""GOV-008 P1 shared helpers: tracked-module index and static import graph.

Read-only analysis helpers. They never import project code; everything is derived from git-tracked
source text with the ``ast`` module, so results are reproducible from a commit.
"""

from __future__ import annotations

import ast
import subprocess
from collections.abc import Iterable
from pathlib import Path

PKG = "ai_trading_system"
MACHINERY_PREFIX = f"{PKG}.platform.architecture"
# Scripts that belong to the development/publication machinery (not to research or daily runs).
MACHINERY_SCRIPT_PREFIXES = (
    "architecture_",
    "run_validation_tier",
    "validation_readiness",
    "pytest_runtime_profile",
    "build_validation_parent",
    "refresh_partial_duration",
    "validate_architecture_dependencies",
    "governance_task_portfolio",
)


def git_tracked(repo: Path, *pathspecs: str) -> list[str]:
    out = subprocess.check_output(["git", "-C", str(repo), "ls-files", *pathspecs], text=True)
    return sorted(line for line in out.split("\n") if line)


def count_lines(path: Path) -> int:
    with path.open(encoding="utf-8", errors="ignore") as handle:
        return sum(1 for _ in handle)


def src_modules(repo: Path) -> dict[str, Path]:
    """Dotted module name -> path for every tracked ``src/ai_trading_system`` Python file."""
    result: dict[str, Path] = {}
    for rel in git_tracked(repo, "src"):
        if not rel.endswith(".py"):
            continue
        parts = rel[len("src/") : -len(".py")].split("/")
        if parts[-1] == "__init__":
            parts = parts[:-1]
        result[".".join(parts)] = repo / rel
    return result


def is_machinery_script(stem: str) -> bool:
    return stem.startswith(MACHINERY_SCRIPT_PREFIXES)


def _resolve_from(module: str, is_pkg: bool, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ""
    base = module.split(".") if is_pkg else module.split(".")[:-1]
    keep = len(base) - (node.level - 1)
    prefix = ".".join(base[:keep])
    return prefix + ("." + node.module if node.module else "")


def raw_imports(path: Path, module: str = "", is_pkg: bool = False) -> set[str]:
    """Absolute dotted names imported anywhere in the file (any nesting depth)."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except SyntaxError:
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            target = _resolve_from(module, is_pkg, node)
            if target:
                names.add(target)
                names.update(f"{target}.{alias.name}" for alias in node.names)
    return names


def build_src_graph(modules: dict[str, Path]) -> dict[str, set[str]]:
    """Module -> project modules it imports, including every ancestor package (``__init__``)."""
    graph: dict[str, set[str]] = {}
    for name, path in modules.items():
        is_pkg = path.name == "__init__.py"
        deps: set[str] = set()
        for dotted in raw_imports(path, name, is_pkg):
            if not dotted.startswith(PKG):
                continue
            parts = dotted.split(".")
            for i in range(1, len(parts) + 1):
                candidate = ".".join(parts[:i])
                if candidate in modules:
                    deps.add(candidate)
        deps.discard(name)
        graph[name] = deps
    return graph


def closure(graph: dict[str, set[str]], seeds: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    stack = [seed for seed in seeds if seed in graph]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(graph[current])
    return seen


# Validation scheduling and trigger provenance belong to the publication/validation machinery even
# though they live outside platform/architecture.
EXTRA_MACHINERY_MODULES = frozenset(
    {f"{PKG}.platform.validation_scheduling", f"{PKG}.platform.validation_trigger_provenance"}
)


def is_machinery_module(name: str) -> bool:
    return (
        name == MACHINERY_PREFIX
        or name.startswith(MACHINERY_PREFIX + ".")
        or name in EXTRA_MACHINERY_MODULES
    )
