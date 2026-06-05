from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_v2_review import (
    DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    DYNAMIC_V2_REVIEW_SAFETY,
    DynamicV2ReviewError,
    _sample_dynamic_v2_review_inputs,
    build_dynamic_v2_review_package,
    build_dynamic_v2_review_validation_report,
    build_v04_candidate_evidence,
    load_dynamic_v2_review_policy_config,
    write_dynamic_v2_review_package,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_v2_review_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_v2_review_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_V2_REVIEW_SAFETY
    assert policy.market_regime.regime_id == "ai_after_chatgpt"
    assert policy.review_package_policy.default_candidate_policy_id.endswith("lower_turnover")
    assert policy.blocking_conditions.block_shadow_enrollment is True
    assert policy.comparison_targets

    raw = yaml.safe_load(DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["production_effect"] = "write_production_weights"
    unsafe_path = tmp_path / "unsafe_dynamic_v2_review.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicV2ReviewError):
        load_dynamic_v2_review_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["comparison_targets"] = []
    invalid_path = tmp_path / "invalid_dynamic_v2_review.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicV2ReviewError):
        load_dynamic_v2_review_policy_config(invalid_path)


def test_dynamic_v2_review_package_recognizes_improvements_and_blocks_shadow() -> None:
    policy = load_dynamic_v2_review_policy_config()
    sample = _sample_dynamic_v2_review_inputs(policy)
    package = build_dynamic_v2_review_package(
        rescue_report=sample["rescue_report"],
        candidate_robustness_report=sample["candidate_robustness_report"],
        shadow_package=sample["shadow_package"],
        policy=policy,
    )

    evidence = package["candidate_evidence"]
    gate = package["shadow_review_eligibility_gate"]
    constraints = package["constraint_hit_decomposition"]
    drawdown = package["drawdown_preservation_failure_review"]

    assert evidence["candidate_id"] == "v0_4_lower_turnover"
    assert evidence["false_risk_off_reduction"] == 217
    assert evidence["turnover_after"] == pytest.approx(1.15)
    assert evidence["constraint_hit_delta"] == 143
    assert evidence["drawdown_preservation"] == pytest.approx(-0.0595)
    assert "FALSE_RISK_OFF_REDUCED" in gate["positive_reason_codes"]
    assert "TURNOVER_REDUCED" in gate["positive_reason_codes"]
    assert "CONSTRAINT_HIT_WORSENED" in gate["blocking_reason_codes"]
    assert "DRAWDOWN_PRESERVATION_FAILED" in gate["blocking_reason_codes"]
    assert gate["status"] == "not_shadow_ready"
    assert gate["review_status"] == "review_candidate"
    assert constraints["blocker"] is True
    assert drawdown["drawdown_failure_status"] == "FAILED"
    assert package["shadow_enrollment_allowed"] is False
    assert package["automatic_enrollment_allowed"] is False
    assert package["owner_approval_executed"] is False


def test_dynamic_v2_evidence_loader_blocks_missing_rescue_report() -> None:
    policy = load_dynamic_v2_review_policy_config()
    sample = _sample_dynamic_v2_review_inputs(policy)

    with pytest.raises(DynamicV2ReviewError):
        build_v04_candidate_evidence(
            rescue_report={},
            candidate_robustness_report=sample["candidate_robustness_report"],
            policy=policy,
        )


def test_dynamic_v2_review_writer_cli_and_reader_brief(tmp_path: Path) -> None:
    policy = load_dynamic_v2_review_policy_config()
    sample = _sample_dynamic_v2_review_inputs(policy)
    rescue_path = tmp_path / "dynamic_rescue.json"
    robustness_path = tmp_path / "dynamic_robustness_v04.json"
    rescue_path.write_text(json.dumps(sample["rescue_report"]), encoding="utf-8")
    robustness_path.write_text(
        json.dumps(sample["candidate_robustness_report"]),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v2-review",
            "package",
            "--dynamic-rescue-report",
            str(rescue_path),
            "--candidate-robustness-report",
            str(robustness_path),
            "--dynamic-shadow-package-dir",
            str(tmp_path / "missing_shadow"),
            "--output-dir",
            str(tmp_path / "packages"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=not_shadow_ready" in result.output
    assert "shadow_enrollment_allowed=false" in result.output

    package_path = next((tmp_path / "packages").glob("*.json"))
    package = json.loads(package_path.read_text(encoding="utf-8"))
    assert "optional_dynamic_shadow_package_missing" in package["warnings"]

    paths = write_dynamic_v2_review_package(package, output_dir=tmp_path / "reader_packages")
    summary = reader_brief._etf_dynamic_v2_review_summary(
        {"reports": [_report_record("etf_dynamic_v2_review_package", paths["json"])]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["candidate"] == "v0_4_lower_turnover"
    assert "CONSTRAINT_HIT_WORSENED" in summary["blockers"]
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["shadow_enrollment_allowed"] is False

    missing = reader_brief._etf_dynamic_v2_review_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_dynamic_v2_review_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_v2_review_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["no_auto_approval"] is True
    assert validation["no_auto_enrollment"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v2-review",
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
