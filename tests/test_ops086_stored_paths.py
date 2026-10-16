"""OPS-086-08: a path recorded inside a stored report is a record, not an instruction.

The 2026-10-16 trial run read 15 files and listed one directory of the development checkout
because two readers followed absolute paths that old summaries and a parameter-search manifest
had stored. A state must be readable without the checkout that produced it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system import daily_task_dashboard as dashboard
from ai_trading_system.fundamentals import sec_pit_shadow_observe as observe
from ai_trading_system.paths import (
    CODE_ROOT,
    STATE_ROOT_ENV,
    join_root,
    state_root,
    stored_state_path,
)


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A state root separate from the code root."""
    root = tmp_path / "state"
    root.mkdir()
    monkeypatch.setenv(STATE_ROOT_ENV, str(root))
    return state_root()


# --- the helper ----------------------------------------------------------------------------------


def test_a_relative_path_means_what_join_root_means(state: Path) -> None:
    assert stored_state_path("outputs/x/y.csv") == join_root(CODE_ROOT, "outputs/x/y.csv")
    assert stored_state_path("outputs/x/y.csv") == state / "outputs" / "x" / "y.csv"
    assert stored_state_path("config/z.yaml") == CODE_ROOT / "config" / "z.yaml"


def test_an_absolute_path_under_a_trusted_root_is_returned_unchanged(
    state: Path, tmp_path: Path
) -> None:
    extra = tmp_path / "search"
    for trusted in (
        state / "outputs" / "a.csv",
        state,
        CODE_ROOT / "config" / "b.yaml",
        extra / "decision_snapshots",
    ):
        found = stored_state_path(str(trusted), extra)
        assert isinstance(found, Path)
        assert found == trusted, trusted


def test_a_root_is_not_a_prefix_match_on_characters(state: Path, tmp_path: Path) -> None:
    neighbour = tmp_path / "state_old" / "outputs" / "x.csv"

    assert stored_state_path(str(neighbour)) == state / "outputs" / "x.csv"


@pytest.mark.parametrize(
    "recorded",
    [
        r"D:\Work\AITradingSystem\outputs\sec_pit_evaluation\x.csv",
        "D:/Work/AITradingSystem/outputs/sec_pit_evaluation/x.csv",
        r"d:\work\aitradingsystem\Outputs\sec_pit_evaluation\x.csv",
        r"\\server\share\AITradingSystem\outputs\sec_pit_evaluation\x.csv",
        "/home/jack/AITradingSystem/outputs/sec_pit_evaluation/x.csv",
    ],
)
def test_a_path_recorded_elsewhere_is_rebased_below_the_state_root(
    state: Path, recorded: str
) -> None:
    found = stored_state_path(recorded)

    assert found == state / "outputs" / "sec_pit_evaluation" / "x.csv"
    assert isinstance(found, Path)


@pytest.mark.parametrize("anchor", ["data", "outputs", "artifacts"])
def test_each_state_directory_is_an_anchor(state: Path, anchor: str) -> None:
    found = stored_state_path(rf"D:\Dev\AITradingSystem\{anchor}\sub\file.json")

    assert found == state / anchor / "sub" / "file.json"


def test_the_first_state_directory_is_the_anchor(state: Path) -> None:
    found = stored_state_path(r"D:\Dev\AITradingSystem\outputs\sec\data\x.csv")

    assert found == state / "outputs" / "sec" / "data" / "x.csv"


def test_a_path_with_no_state_directory_is_not_resolved(state: Path) -> None:
    assert stored_state_path(r"D:\Work\other\x.csv") is None
    assert stored_state_path("/opt/tools/y.csv") is None
    assert stored_state_path("") is None
    assert stored_state_path("   ") is None


def test_without_a_separate_state_root_the_code_root_is_the_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    found = stored_state_path(r"D:\Dev\AITradingSystem\outputs\y.csv")

    assert found == CODE_ROOT / "outputs" / "y.csv"


# --- sec_pit_shadow_observe._artifact_path ---------------------------------------------------


def _summary(**artifacts: str) -> dict[str, object]:
    return {"output_artifacts": dict(artifacts)}


def test_a_summary_that_names_a_development_checkout_reads_the_state_copy(
    state: Path, tmp_path: Path
) -> None:
    home = state / "outputs" / "sec_pit_evaluation"
    default = home / "sec_pit_feature_effectiveness_2026-06-01.csv"

    for recorded in (
        r"D:\Work\AITradingSystem\outputs\sec_pit_evaluation\sec_pit_feature_effectiveness_2026-05-26.csv",
        "/home/jack/AITradingSystem/outputs/sec_pit_evaluation/sec_pit_feature_effectiveness_2026-05-26.csv",
    ):
        found = observe._artifact_path(_summary(effect_csv=recorded), "effect_csv", default, home)

        assert found == home / "sec_pit_feature_effectiveness_2026-05-26.csv"


def test_a_summary_path_inside_its_own_directory_is_kept(state: Path, tmp_path: Path) -> None:
    home = tmp_path / "elsewhere" / "sec_pit_evaluation"
    named = home / "sec_pit_signal_attribution_2023-01-05.csv"
    default = home / "default.csv"

    found = observe._artifact_path(_summary(a=str(named)), "a", default, home)

    assert found == named


def test_a_summary_path_that_cannot_be_placed_falls_back_to_the_default(
    state: Path,
) -> None:
    home = state / "outputs" / "sec_pit_evaluation"
    default = home / "default.csv"

    assert observe._artifact_path(_summary(a=r"D:\Work\x\y.csv"), "a", default, home) == default
    assert observe._artifact_path(_summary(), "a", default, home) == default
    assert observe._artifact_path(_summary(a=""), "a", default, home) == default
    assert observe._artifact_path({}, "a", default, home) == default


def test_a_relative_summary_path_still_resolves_under_the_state_root(state: Path) -> None:
    home = state / "outputs" / "sec_pit_evaluation"

    found = observe._artifact_path(
        _summary(a="outputs/sec_pit_evaluation/z.csv"), "a", home / "default.csv", home
    )

    assert found == state / "outputs" / "sec_pit_evaluation" / "z.csv"


# --- daily_task_dashboard._production_gate_cap_observations ----------------------------------


def _snapshot(directory: Path, day: str, caps: dict[str, float]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    gates = [{"gate_id": gate, "max_position": cap} for gate, cap in sorted(caps.items())]
    (directory / f"decision_snapshot_{day}.json").write_text(
        json.dumps({"positions": {"position_gates": gates}}), encoding="utf-8"
    )


def _manifest(recorded: object) -> dict[str, object]:
    return {
        "decision_snapshot_path": str(recorded),
        "search_window": {"start": "2026-05-01", "end": "2026-05-14"},
    }


def test_the_dashboard_reads_the_state_snapshots_not_the_recorded_checkout(
    state: Path, tmp_path: Path
) -> None:
    snapshots = state / "data" / "processed" / "decision_snapshots"
    _snapshot(snapshots, "2026-05-04", {"valuation": 0.4, "risk_budget": 1.0})
    _snapshot(snapshots, "2026-05-05", {"valuation": 0.5})
    dev = tmp_path / "dev" / "AITradingSystem" / "data" / "processed" / "decision_snapshots"
    _snapshot(dev, "2026-05-04", {"valuation": 0.9})
    project = tmp_path / "code"
    project.mkdir()

    observed = dashboard._production_gate_cap_observations(_manifest(dev), project)

    assert observed["valuation"] == {"min": 0.4, "max": 0.5, "count": 2, "snapshot_count": 2}
    assert observed["risk_budget"]["max"] == 1.0


def test_the_dashboard_reads_the_state_snapshots_for_a_windows_recorded_path(
    state: Path, tmp_path: Path
) -> None:
    _snapshot(state / "data" / "processed" / "decision_snapshots", "2026-05-04", {"valuation": 0.4})
    project = tmp_path / "code"
    project.mkdir()
    recorded = r"D:\Work\AITradingSystem\data\processed\decision_snapshots"

    observed = dashboard._production_gate_cap_observations(_manifest(recorded), project)

    assert observed["valuation"]["min"] == 0.4


def test_without_state_snapshots_the_recorded_checkout_is_not_a_fallback(
    state: Path, tmp_path: Path
) -> None:
    dev = tmp_path / "dev" / "AITradingSystem" / "data" / "processed" / "decision_snapshots"
    _snapshot(dev, "2026-05-04", {"valuation": 0.9})
    project = tmp_path / "code"
    project.mkdir()

    assert dashboard._production_gate_cap_observations(_manifest(dev), project) == {}


def test_a_snapshot_directory_inside_the_project_root_is_kept(state: Path, tmp_path: Path) -> None:
    project = tmp_path / "code"
    inside = project / "decision_snapshots"
    _snapshot(inside, "2026-05-04", {"valuation": 0.3})

    observed = dashboard._production_gate_cap_observations(_manifest(inside), project)

    assert observed["valuation"]["min"] == 0.3


def test_a_manifest_without_a_snapshot_path_has_no_observations(
    state: Path, tmp_path: Path
) -> None:
    assert dashboard._production_gate_cap_observations({}, tmp_path) == {}
    assert (
        dashboard._production_gate_cap_observations({"decision_snapshot_path": ""}, tmp_path) == {}
    )


# --- records ------------------------------------------------------------------------------------


def test_the_flow_and_requirement_documents_describe_the_rule() -> None:
    flow = (CODE_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    requirement = (
        CODE_ROOT / "docs" / "requirements" / "OPS-086_Daily_Release_Bundle.md"
    ).read_text(encoding="utf-8")

    assert "stored_state_path" in flow and "OPS-086-08" in flow
    assert "步骤 6c（OPS-086-08" in requirement and "stored_state_path" in requirement
