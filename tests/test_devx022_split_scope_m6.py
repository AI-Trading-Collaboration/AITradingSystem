"""DEVX-022 M6: three more files are scheduled per node, and the review that allows it stays true.

The Full's wall was set by single-worker ``loadfile`` chains of the named data-quality candidate
tests (DEVX-022 17.14). Spreading their nodes over the workers is safe only while the nodes share
nothing: no pytest fixture, no module or class setup, no mutated module state, no process-wide
switch, and every real write goes through ``tmp_path`` or a uuid-named path. These tests pin that
review, so a later change to one of these files fails here instead of becoming a flaky Full.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ai_trading_system.platform.validation_scheduling import load_scheduling_manifest

ROOT = Path(__file__).resolve().parents[1]
M6_FILES = (
    "tests/test_composer_prospective_capture_contract.py",
    "tests/test_named_data_quality_candidate.py",
    "tests/test_named_simple_baseline_preview_candidate.py",
)
# Local helper module the files build their fixtures from by hand (plain functions, no state).
SUPPORT_FILES = ("tests/named_data_quality_support.py",)
SETUP_NAMES = {
    "setup_module",
    "teardown_module",
    "setup_class",
    "teardown_class",
    "setup_method",
    "teardown_method",
}
PROCESS_WIDE_CALLS = {("os", "chdir"), ("os", "putenv"), ("importlib", "reload")}


def _tree(relative: str) -> ast.Module:
    return ast.parse((ROOT / relative).read_text(encoding="utf-8"), filename=relative)


def _is_fixture(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return isinstance(target, ast.Attribute) and target.attr == "fixture"


def test_the_manifest_splits_the_three_files_and_stays_sorted_and_unique() -> None:
    manifest = load_scheduling_manifest(ROOT)
    assert manifest is not None and manifest.version >= 7
    for relative in M6_FILES:
        assert manifest.is_split_file(relative), relative
    files = list(manifest.split_scope_files)
    assert files == sorted(set(files))  # the runtime profile evidence demands sorted, unique


def test_a_split_file_holds_no_real_full_chain_node_and_no_exclusive_group_member() -> None:
    manifest = load_scheduling_manifest(ROOT)
    assert manifest is not None
    heavy_files = {key.split("::")[0] for key in manifest.real_full_chain_functions}
    grouped = {key.split("::")[0] for _, members in manifest.exclusive_groups for key in members}
    for relative in M6_FILES:
        assert relative not in heavy_files and relative not in grouped, relative


@pytest.mark.parametrize("relative", [*M6_FILES, *SUPPORT_FILES])
def test_the_files_define_no_fixture_and_no_module_or_class_setup(relative: str) -> None:
    tree = _tree(relative)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            assert not any(_is_fixture(d) for d in node.decorator_list), (relative, node.name)
            assert node.name not in SETUP_NAMES, (relative, node.name)
    assert not (ROOT / "tests" / "conftest.py").exists()  # no hidden session fixtures either


@pytest.mark.parametrize("relative", [*M6_FILES, *SUPPORT_FILES])
def test_the_files_do_not_switch_process_wide_state(relative: str) -> None:
    tree = _tree(relative)
    for node in ast.walk(tree):
        assert not isinstance(node, ast.Global), (relative, node.lineno)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            owner = node.func.value
            if isinstance(owner, ast.Name):
                assert (owner.id, node.func.attr) not in PROCESS_WIDE_CALLS, (relative, node.lineno)


MUTATORS = {
    "append",
    "extend",
    "insert",
    "add",
    "update",
    "setdefault",
    "pop",
    "popitem",
    "clear",
    "remove",
}


def _module_containers(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for statement in tree.body:
        targets: list[ast.expr] = []
        value: ast.expr | None = None
        if isinstance(statement, ast.Assign):
            targets, value = list(statement.targets), statement.value
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            targets, value = [statement.target], statement.value
        mutable = isinstance(value, ast.List | ast.Dict | ast.Set) or (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id in {"list", "dict", "set", "defaultdict", "OrderedDict"}
        )
        if mutable:
            names.update(t.id for t in targets if isinstance(t, ast.Name))
    return names


@pytest.mark.parametrize("relative", [*M6_FILES, *SUPPORT_FILES])
def test_the_files_never_mutate_a_module_level_container(relative: str) -> None:
    tree = _tree(relative)
    containers = _module_containers(
        tree
    )  # read-only constants such as EXPECTED_DEPENDENCIES are fine
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            owner = node.func.value
            if isinstance(owner, ast.Name) and owner.id in containers:
                assert node.func.attr not in MUTATORS, (relative, owner.id, node.lineno)
        if isinstance(node, ast.Assign | ast.AugAssign | ast.Delete):
            raw = node.targets if isinstance(node, ast.Assign | ast.Delete) else [node.target]
            for target in raw:
                if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
                    assert target.value.id not in containers, (
                        relative,
                        target.value.id,
                        node.lineno,
                    )
