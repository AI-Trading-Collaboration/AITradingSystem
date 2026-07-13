from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_forward_confirmation_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DynamicV3BacktestSimulationError,
    validate_forward_confirmation_plan_artifact,
)


def test_forward_confirmation_plan_inherits_only_source_targets_and_criteria(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    plan = fixture["confirmation_plan"]
    targets = plan["confirmation_targets"]["targets"]
    bridge_targets = fixture["bridge"]["forward_confirmation_targets"]["targets"]
    bridge_by_id = {row["target"]: row for row in bridge_targets}

    assert {row["target_id"] for row in targets} == {"limited_adjustment_vs_no_trade"}
    assert "consensus_target_risk" not in {row["target_id"] for row in targets}
    target = targets[0]
    source = bridge_by_id[target["target_id"]]
    assert target["required_forward_events"] == source["required_forward_events"]
    assert target["windows"] == source["windows"]
    assert target["success_criteria"] == source["success_criteria"]
    assert target["source_bridge_target"] == source
    assert target["source_proposal_ids"]
    assert plan["manifest"]["status"] == "AVAILABLE"
    assert plan["manifest"]["auto_policy_apply"] is False
    assert (
        plan["confirmation_plan_dir"] / "forward_confirmation_plan_input_snapshot.json"
    ).exists()
    assert "Dynamic Rescue Forward Confirmation Plan" in plan["reader_brief_section"]

    validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=plan["confirmation_plan_id"],
        output_dir=fixture["confirmation_plan_dir"],
    )
    assert validation["status"] == "PASS"


def test_forward_confirmation_plan_empty_proposals_do_not_fabricate_targets() -> None:
    policy = sim._load_forward_confirmation_plan_policy(
        sim.DEFAULT_FORWARD_CONFIRMATION_PLAN_POLICY_PATH
    )
    bridge_targets = {
        "targets": [
            {
                "target": "limited_adjustment_vs_no_trade",
                "priority": "HIGH",
                "tracking_status": "TRACKING_REQUIRED",
                "reason": "source",
                "required_forward_events": 17,
                "windows": [5, 20],
                "success_criteria": {"avg_relative_return_min": 0.01},
            }
        ]
    }
    targets = sim._confirmation_targets(
        {"proposals": []}, bridge_targets, policy=policy
    )
    assert targets["targets"] == []
    assert targets["evidence_status"] == "INSUFFICIENT_DATA"
    assert targets["unmatched_bridge_target_ids"] == ["limited_adjustment_vs_no_trade"]


def test_forward_confirmation_plan_rejects_naive_cutoff_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_plan(fixture, tmp_path / "new", datetime(2026, 7, 31, 14))
    assert not (tmp_path / "new").exists()


def test_forward_confirmation_plan_rejects_invalid_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    source = fixture["bridge"]["bridge_dir"] / "sim_forward_bridge_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="source validation"):
        _run_plan(
            fixture, tmp_path / "new", datetime(2026, 7, 31, 14, tzinfo=UTC)
        )
    assert not (tmp_path / "new").exists()


def test_forward_confirmation_plan_rejects_invalid_policy_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    policy_path = tmp_path / "invalid.yaml"
    policy_path.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="metadata/rules"):
        _run_plan(
            fixture,
            tmp_path / "new",
            datetime(2026, 7, 31, 14, tzinfo=UTC),
            policy_path=policy_path,
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize(
    "artifact_name",
    [
        "confirmation_plan_manifest.json",
        "confirmation_targets.json",
        "trigger_conditions.json",
        "failure_conditions.json",
        "forward_confirmation_plan_input_snapshot.json",
        "forward_confirmation_plan_report.md",
        "reader_brief_section.md",
    ],
)
def test_forward_confirmation_plan_validator_rejects_output_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    plan = fixture["confirmation_plan"]
    path = plan["confirmation_plan_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=plan["confirmation_plan_id"],
        output_dir=fixture["confirmation_plan_dir"],
    )
    assert validation["status"] == "FAIL"


def test_forward_confirmation_plan_validator_rejects_live_source_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    plan = fixture["confirmation_plan"]
    source = fixture["proposal_review"]["proposal_review_dir"] / "reader_brief_section.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=plan["confirmation_plan_id"],
        output_dir=fixture["confirmation_plan_dir"],
    )
    assert validation["status"] == "FAIL"


def test_forward_confirmation_plan_validator_rejects_live_policy_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        sim.DEFAULT_FORWARD_CONFIRMATION_PLAN_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    plan = _run_plan(
        fixture,
        tmp_path / "plan",
        datetime(2026, 7, 31, 14, tzinfo=UTC),
        policy_path=policy_path,
    )
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8"
    )
    validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=plan["confirmation_plan_id"], output_dir=tmp_path / "plan"
    )
    assert validation["status"] == "FAIL"


def _run_plan(
    fixture: dict[str, Any],
    output_dir: Path,
    generated_at: datetime,
    *,
    policy_path: Path = sim.DEFAULT_FORWARD_CONFIRMATION_PLAN_POLICY_PATH,
) -> dict[str, Any]:
    return sim.run_forward_confirmation_plan(
        proposal_review_id=fixture["proposal_review"]["proposal_review_id"],
        bridge_id=fixture["bridge"]["bridge_id"],
        proposal_review_dir=fixture["proposal_review_dir"],
        bridge_dir=fixture["bridge_dir"],
        output_dir=output_dir,
        policy_path=policy_path,
        generated_at=generated_at,
    )
