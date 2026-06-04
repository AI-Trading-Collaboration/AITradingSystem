from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    SAFETY_FIELDS,
    DynamicAllocationError,
    build_dynamic_allocation_decision_record,
    build_dynamic_allocation_report,
    build_dynamic_allocation_validation_report,
    load_dynamic_allocation_policy_config,
    write_dynamic_allocation_report,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_allocation_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_allocation_policy_config()

    assert policy.safety.model_dump(mode="json") == SAFETY_FIELDS
    assert policy.market_regime.regime_id == "ai_after_chatgpt"
    assert policy.regime_weight_targets["neutral"].weights["CASH"] == pytest.approx(0.10)

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["safety"]["official_target_weights_mutated"] = True
    unsafe_path = tmp_path / "unsafe_dynamic_allocation.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicAllocationError):
        load_dynamic_allocation_policy_config(unsafe_path)

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["regime_weight_targets"]["risk_on"]["weights"]["QQQ"] = 0.90
    invalid_path = tmp_path / "invalid_dynamic_weights.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicAllocationError):
        load_dynamic_allocation_policy_config(invalid_path)


def test_dynamic_allocation_decision_is_candidate_only_and_constrained() -> None:
    policy = load_dynamic_allocation_policy_config()
    decision = build_dynamic_allocation_decision_record(
        policy=policy,
        decision_date=date(2026, 6, 3),
        input_scores=policy.sample_score_profiles["event_risk_high"],
        previous_weights=policy.base_weights,
        previous_scores=policy.sample_score_profiles["neutral"],
        source_trend_report="test_trend_report.json",
        data_quality_status="PASS_WITH_WARNINGS",
    )

    weights = decision["candidate_target_weights"]
    assert decision["regime_state"]["selected_regime"] == "event_risk_high"
    assert decision["production_effect"] == "none"
    assert decision["official_target_weights_mutated"] is False
    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["QQQ"] <= policy.exposure_constraints.asset_caps["QQQ"]
    assert weights["SMH"] + weights["SOXX"] <= policy.exposure_constraints.semiconductor_sleeve_max
    assert weights["CASH"] <= policy.exposure_constraints.cash_max
    assert decision["rebalance_decision"]["decision"] in {"rebalance_candidate", "hold"}

    serialized = json.dumps(decision, ensure_ascii=False)
    assert "data/etf_portfolio/target_weights.csv" not in serialized
    assert "broker_order" not in serialized
    assert "production_weight_update" not in serialized


def test_dynamic_allocation_cli_validate_decide_and_reader_brief(tmp_path: Path) -> None:
    runner = CliRunner()
    validate_result = runner.invoke(
        etf_app,
        [
            "dynamic-allocation",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert validate_result.exit_code == 0, validate_result.output

    decide_result = runner.invoke(
        etf_app,
        [
            "dynamic-allocation",
            "decide",
            "--date",
            "2026-06-03",
            "--score-profile",
            "semiconductor_leadership",
            "--no-latest-trend-report",
            "--decision-output-dir",
            str(tmp_path / "decisions"),
            "--report-output-dir",
            str(tmp_path / "reports"),
            "--registry-output-dir",
            str(tmp_path / "registry"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert decide_result.exit_code == 0, decide_result.output
    assert "selected_regime=semiconductor_leadership" in decide_result.output
    assert "official_target_weights_mutated=false" in decide_result.output

    validation = build_dynamic_allocation_validation_report()
    assert validation["status"] == "PASS"

    policy = load_dynamic_allocation_policy_config()
    decision = build_dynamic_allocation_decision_record(
        policy=policy,
        decision_date=date(2026, 6, 3),
        input_scores=policy.sample_score_profiles["neutral"],
        previous_weights=policy.base_weights,
    )
    report = build_dynamic_allocation_report(policy=policy, decision_records=[decision])
    report_paths = write_dynamic_allocation_report(report, output_dir=tmp_path / "reports2")
    summary = reader_brief._etf_dynamic_allocation_summary(
        {"reports": [_report_record("etf_dynamic_allocation_report", report_paths["json"])]}
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["selected_regime"] == report["summary"]["selected_regime"]
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["official_target_weights_mutated"] is False

    missing = reader_brief._etf_dynamic_allocation_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
