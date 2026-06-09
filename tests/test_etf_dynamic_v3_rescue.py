from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DYNAMIC_V3_RESCUE_SAFETY,
    DynamicV3RescueError,
    _sample_dynamic_v3_review_package,
    apply_drawdown_guardrail_overlay,
    apply_emergency_risk_off_exception,
    apply_soft_constraint_penalty_and_smoothing,
    build_constraint_drawdown_root_cause,
    build_dynamic_v3_candidate_templates,
    build_dynamic_v3_rescue_evaluation_report,
    build_dynamic_v3_rescue_validation_report,
    evaluate_emergency_risk_off,
    load_dynamic_v3_rescue_policy_config,
    normalize_pre_constraint_targets,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_v3_rescue_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_v3_rescue_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_V3_RESCUE_SAFETY
    assert policy.market_regime.regime_id == "ai_after_chatgpt"
    assert policy.base_candidate == "dynamic_regime_overlay_v0_4_lower_turnover"
    assert policy.constraint_targets.max_constraint_hit_delta_vs_v0_4 < 0
    assert len(policy.candidate_templates) == 4

    raw = yaml.safe_load(DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["production_effect"] = "write_production_weights"
    unsafe_path = tmp_path / "unsafe_dynamic_v3.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicV3RescueError):
        load_dynamic_v3_rescue_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["candidate_templates"][0]["production_config_mutation_allowed"] = True
    invalid_path = tmp_path / "invalid_dynamic_v3.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicV3RescueError):
        load_dynamic_v3_rescue_policy_config(invalid_path)


def test_dynamic_v3_root_cause_loader_and_templates_preserve_evidence() -> None:
    policy = load_dynamic_v3_rescue_policy_config()
    package = _sample_dynamic_v3_review_package()

    root = build_constraint_drawdown_root_cause(
        v04_review_package=package,
        policy=policy,
        v04_review_package_path="sample_v2_package.json",
    )
    templates = build_dynamic_v3_candidate_templates(root_cause=root, policy=policy)

    assert root["constraint_hit_delta"] == 143
    assert root["drawdown_root_cause"]
    assert root["source_links"]["dynamic_rescue_report"]
    assert root["safety"] == DYNAMIC_V3_RESCUE_SAFETY
    assert {template["policy_id"] for template in templates} == {
        "dynamic_regime_overlay_v0_3a_constraint_smooth",
        "dynamic_regime_overlay_v0_3b_drawdown_guarded",
        "dynamic_regime_overlay_v0_3c_constraint_smooth_guarded",
        "dynamic_regime_overlay_v0_3d_emergency_only_guarded",
    }
    assert all(template["candidate_only"] is True for template in templates)
    assert all(template["production_config_mutation_allowed"] is False for template in templates)

    broken = dict(package)
    broken.pop("constraint_hit_decomposition")
    with pytest.raises(DynamicV3RescueError):
        build_constraint_drawdown_root_cause(v04_review_package=broken, policy=policy)


def test_dynamic_v3_normalization_penalty_smoothing_guardrail_and_emergency() -> None:
    policy = load_dynamic_v3_rescue_policy_config()
    raw = {"SPY": 0.05, "QQQ": 0.56, "SMH": 0.20, "SOXX": 0.10, "CASH": 0.09}

    normalized = normalize_pre_constraint_targets(raw, policy)
    weights = normalized["normalized_target_weights"]
    reasons = normalized["normalization_reason_codes"]

    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["QQQ"] <= policy.normalization_policy.qqq_max_target
    assert weights["SMH"] + weights["SOXX"] <= policy.normalization_policy.semiconductor_max_target
    assert "REDUCED_SEMICONDUCTOR_TARGET" in reasons
    assert "REDUCED_QQQ_TARGET" in reasons

    cash_normalized = normalize_pre_constraint_targets(
        {"SPY": 0.05, "QQQ": 0.20, "SMH": 0.05, "SOXX": 0.05, "CASH": 0.65},
        policy,
    )
    assert cash_normalized["normalized_target_weights"]["CASH"] <= (
        policy.normalization_policy.cash_max_target
    )
    assert "REDUCED_CASH_TARGET" in cash_normalized["normalization_reason_codes"]

    previous = {"SPY": 0.42, "QQQ": 0.34, "SMH": 0.11, "SOXX": 0.03, "CASH": 0.10}
    smoothed = apply_soft_constraint_penalty_and_smoothing(
        weights,
        previous,
        policy,
    )
    assert smoothed["constraint_proximity_scores"]["QQQ_max_proximity"] <= 1.0
    assert "ALLOCATION_SMOOTHED" in smoothed["smoothing_reason_codes"]
    for symbol, value in smoothed["smoothed_target_weights"].items():
        assert abs(value - previous[symbol]) <= policy.smoothing_policy.max_single_rebalance_delta

    weak_guardrail = apply_drawdown_guardrail_overlay(
        smoothed["smoothed_target_weights"],
        {"portfolio_drawdown": -0.09},
        policy,
    )
    assert weak_guardrail["guardrail_triggered"] is False
    assert (
        "GUARDRAIL_NOT_TRIGGERED_INSUFFICIENT_CONFIRMATION"
        in weak_guardrail["guardrail_reason_codes"]
    )

    strong_guardrail = apply_drawdown_guardrail_overlay(
        smoothed["smoothed_target_weights"],
        {
            "portfolio_drawdown": -0.09,
            "QQQ_drawdown": -0.11,
            "SMH_drawdown": -0.14,
            "volatility_spike_confirmed": True,
        },
        policy,
    )
    assert strong_guardrail["guardrail_triggered"] is True
    assert strong_guardrail["after_weights"]["CASH"] > weak_guardrail["after_weights"]["CASH"]
    assert "SEMICONDUCTOR_RISK_REDUCED" in strong_guardrail["guardrail_reason_codes"]

    insufficient_emergency = evaluate_emergency_risk_off(
        {"TrendScore": 30.0, "RiskRegimeScore": 82.0},
        policy,
    )
    assert insufficient_emergency["emergency_risk_off_triggered"] is False

    emergency = apply_emergency_risk_off_exception(
        strong_guardrail["after_weights"],
        {
            "TrendScore": 30.0,
            "RiskRegimeScore": 82.0,
            "volatility_risk_high": True,
            "QQQ_SPY_relative_weakness": True,
        },
        policy,
    )
    assert emergency["emergency_risk_off_triggered"] is True
    assert emergency["bypass_smoothing"] is True
    assert "EMERGENCY_SMOOTHING_BYPASS" in emergency["emergency_reason_codes"]


def test_dynamic_v3_report_cli_and_reader_brief(tmp_path: Path) -> None:
    policy = load_dynamic_v3_rescue_policy_config()
    package = _sample_dynamic_v3_review_package()
    report = build_dynamic_v3_rescue_evaluation_report(
        v04_review_package=package,
        policy=policy,
        v04_review_package_path="sample_v2_package.json",
    )

    best = report["best_candidate"]
    assert report["status"] == "v0_3_rescue_success_candidate_found"
    assert best["policy_id"] == "dynamic_regime_overlay_v0_3c_constraint_smooth_guarded"
    assert best["constraint_fixed"] is True
    assert best["drawdown_fixed"] is True
    assert report["shadow_enrollment_allowed"] is False
    assert report["owner_approval_executed"] is False

    package_path = tmp_path / "dynamic_v2_review_package.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "run",
            "--no-latest-v2-review",
            "--v2-review-package",
            str(package_path),
            "--output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=v0_3_rescue_success_candidate_found" in result.output
    assert "shadow_enrollment_allowed=false" in result.output

    report_path = next((tmp_path / "reports").glob("*.json"))
    summary = reader_brief._etf_dynamic_v3_rescue_summary(
        {"reports": [_report_record("etf_dynamic_v3_rescue_evaluation_report", report_path)]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["best_candidate"] == "dynamic_regime_overlay_v0_3c_constraint_smooth_guarded"
    assert summary["constraint_status"] == "improved"
    assert summary["drawdown_status"] == "improved"
    assert summary["shadow_enrollment_allowed"] is False

    missing = reader_brief._etf_dynamic_v3_rescue_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_dynamic_v3_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_v3_rescue_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["no_auto_approval"] is True
    assert validation["no_auto_enrollment"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert "automatic_enrollment_allowed=false" in result.output


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": payload.get("status", "PASS"),
        "exists": True,
        "age_days": 0,
    }
