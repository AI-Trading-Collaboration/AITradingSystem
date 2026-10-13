from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

import pytest

from ai_trading_system import paths
from ai_trading_system.paths import (
    CODE_ROOT,
    STATE_ROOT_ENV,
    cli_path,
    contained_root,
    glob_root,
    is_code_root,
    join_root,
    relative_to_root,
    rglob_root,
    state_args,
    state_dir,
    state_root,
)

SRC_ROOT = CODE_ROOT / "src" / "ai_trading_system"
STATE_DIRS = ("data", "outputs", "artifacts")
ROOT_NAMES = {
    "PROJECT_ROOT",
    "project_root",
    "REPO_ROOT",
    "repo_root",
    "ROOT",
    "root",
    "resolved_root",
    "resolved_project_root",
    "_PROJECT_ROOT",
    "_REPO_ROOT",
}


@pytest.fixture
def separate_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv(STATE_ROOT_ENV, str(state))
    return state.resolve()


def test_unset_state_root_is_the_code_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    assert state_root() == CODE_ROOT
    assert state_dir(CODE_ROOT, "outputs", "reports") == CODE_ROOT / "outputs" / "reports"
    assert join_root(CODE_ROOT, "data/raw/x.csv") == CODE_ROOT / "data/raw/x.csv"
    assert relative_to_root(CODE_ROOT / "outputs" / "x.md", CODE_ROOT) == Path("outputs/x.md")
    assert contained_root(CODE_ROOT, "outputs/x.json") == CODE_ROOT
    assert state_args(["--output-path", "outputs/x.md"]) == ["--output-path", "outputs/x.md"]


def test_relative_state_root_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STATE_ROOT_ENV, "relative/state")

    with pytest.raises(ValueError, match="absolute"):
        state_root()


def test_code_root_state_paths_move_to_the_state_root(separate_state: Path) -> None:
    assert state_root() == separate_state
    assert state_dir(CODE_ROOT, "outputs", "reports") == separate_state / "outputs" / "reports"
    assert state_dir(CODE_ROOT, "data/raw") == separate_state / "data/raw"
    assert join_root(CODE_ROOT, "artifacts/x.json") == separate_state / "artifacts/x.json"
    assert join_root(CODE_ROOT, Path("data") / "raw") == separate_state / "data" / "raw"
    assert contained_root(CODE_ROOT, "outputs/data_quality/x.json") == separate_state


def test_code_paths_stay_under_the_code_root(separate_state: Path) -> None:
    assert join_root(CODE_ROOT, "config/universe.yaml") == CODE_ROOT / "config/universe.yaml"
    assert join_root(CODE_ROOT, "docs/x.md") == CODE_ROOT / "docs/x.md"
    assert contained_root(CODE_ROOT, "src/ai_trading_system/paths.py") == CODE_ROOT
    absolute = separate_state / "elsewhere.csv"
    assert join_root(CODE_ROOT, absolute) == absolute


def test_other_roots_keep_everything_under_themselves(separate_state: Path, tmp_path: Path) -> None:
    other = tmp_path / "fake_project"
    assert not is_code_root(other)
    assert state_dir(other, "outputs", "x") == other / "outputs" / "x"
    assert join_root(other, "data/raw/x.csv") == other / "data/raw/x.csv"
    assert contained_root(other, "outputs/x.json") == other
    assert relative_to_root(other / "outputs" / "x", other) == Path("outputs/x")
    posix = PurePosixPath("mirror")
    assert state_dir(posix, "outputs") == PurePosixPath("mirror/outputs")


def test_relative_to_root_reads_state_paths_from_the_state_root(separate_state: Path) -> None:
    report = separate_state / "outputs" / "reports" / "x.md"
    assert relative_to_root(report, CODE_ROOT) == Path("outputs/reports/x.md")
    config = CODE_ROOT / "config" / "universe.yaml"
    assert relative_to_root(config, CODE_ROOT) == Path("config/universe.yaml")
    with pytest.raises(ValueError):
        relative_to_root(separate_state.parent / "other.txt", CODE_ROOT)


@pytest.fixture
def fake_code_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    code = (tmp_path / "code").resolve()
    code.mkdir()
    monkeypatch.setattr(paths, "CODE_ROOT", code)
    paths._resolves_to_code_root.cache_clear()
    yield code
    paths._resolves_to_code_root.cache_clear()


def test_glob_reads_state_dirs_from_the_state_root(
    separate_state: Path, fake_code_root: Path
) -> None:
    code = fake_code_root
    for rel in ("config/a.toml", "outputs/reports/old.json", "docs/reports/doc.json"):
        (code / rel).parent.mkdir(parents=True, exist_ok=True)
        (code / rel).write_text("{}", encoding="utf-8")
    report = separate_state / "outputs" / "reports" / "x.json"
    report.parent.mkdir(parents=True)
    report.write_text("{}", encoding="utf-8")

    assert list(glob_root(code, "outputs/reports/*.json")) == [report]
    assert list(glob_root(code, "config/*.toml")) == [code / "config" / "a.toml"]
    assert sorted(glob_root(code, "*/reports/*.json")) == sorted(
        [code / "docs" / "reports" / "doc.json", report]
    )
    assert sorted(rglob_root(code, "*.json")) == sorted(
        [code / "docs" / "reports" / "doc.json", report]
    )


def test_state_args_make_relative_state_paths_absolute(separate_state: Path) -> None:
    converted = state_args(
        [
            "pit-snapshots",
            "--raw-input-dir",
            "data/raw/daily_input_capture/2026-10-09",
            "--output-path=outputs/reports/x.md",
            "--config",
            "config/universe.yaml",
            "--as-of",
            "2026-10-09",
            "outputs",
        ]
    )

    assert converted == [
        "pit-snapshots",
        "--raw-input-dir",
        str(separate_state / "data/raw/daily_input_capture/2026-10-09"),
        f"--output-path={separate_state / 'outputs/reports/x.md'}",
        "--config",
        "config/universe.yaml",
        "--as-of",
        "2026-10-09",
        "outputs",
    ]


def test_cli_path_maps_relative_state_paths(separate_state: Path) -> None:
    assert cli_path("outputs") == separate_state / "outputs"
    assert (
        cli_path("data/raw/download_manifest.csv")
        == separate_state / "data/raw/download_manifest.csv"
    )
    assert cli_path("config") == Path("config")
    assert cli_path("/abs/outputs") == Path("/abs/outputs")


def test_cli_path_is_plain_without_a_separate_state_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    assert cli_path("outputs") == Path("outputs")


def test_is_code_root_accepts_an_unresolved_spelling() -> None:
    assert is_code_root(CODE_ROOT / "src" / "..")
    assert is_code_root(str(CODE_ROOT))
    assert not is_code_root(CODE_ROOT / "src")


def _root_like(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id in ROOT_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in ROOT_NAMES
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "resolve"
    ):
        return _root_like(node.func.value)
    return False


def _state_literal(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.split("/")[0] in STATE_DIRS
    )


def _hand_built_state_paths(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            if _state_literal(node.right):
                found.append(f"{node.lineno}: <path> / {node.right.value!r}")
            elif _root_like(node.left) and not isinstance(node.right, ast.Constant):
                found.append(f"{node.lineno}: <root> / <expression>")
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and len(node.args) == 1
        ):
            if node.func.attr == "relative_to" and _root_like(node.args[0]):
                found.append(f"{node.lineno}: .relative_to(<root>)")
            elif node.func.attr in ("glob", "rglob") and _root_like(node.func.value):
                found.append(f"{node.lineno}: <root>.{node.func.attr}(...)")
    return found


# Zone C files still to convert (DATA-023 step 2). The list only shrinks.
PENDING_ZONE_C: frozenset[str] = frozenset()


def test_no_state_path_is_built_from_a_root_outside_the_paths_module() -> None:
    offenders = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if path == Path(paths.__file__).resolve():
            continue
        found = _hand_built_state_paths(path)
        if found:
            offenders[path.relative_to(CODE_ROOT).as_posix()] = found

    unexpected = {rel: found for rel, found in offenders.items() if rel not in PENDING_ZONE_C}
    assert unexpected == {}, (
        "Build state paths with ai_trading_system.paths (state_dir, join_root, "
        f"relative_to_root, glob_root, rglob_root): {unexpected}"
    )
    assert set(PENDING_ZONE_C) <= set(offenders), (
        "Remove converted files from PENDING_ZONE_C: "
        f"{sorted(set(PENDING_ZONE_C) - set(offenders))}"
    )
