from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_formal_research_method_contract_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_promotion_thresholds as thresholds


def test_promotion_gate_threshold_policy_validates() -> None:
    validation = thresholds.validate_promotion_gate_threshold_policy()
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}

    assert validation["status"] == "PASS"
    assert failed == set()


def test_promotion_gate_threshold_report_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path)
    contract = fixture["formal_research_method_contract"]
    result = thresholds.build_promotion_gate_threshold_calibration_report(
        contract_id=contract["contract_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        output_dir=tmp_path / "threshold_calibration",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    report = result["report"]
    validation = result["validation"]
    payload = thresholds.promotion_gate_threshold_calibration_report_payload(
        calibration_id=result["calibration_id"],
        output_dir=tmp_path / "threshold_calibration",
    )

    assert report["status"] == "PASS"
    assert (
        report["current_threshold_interpretation"]
        == "FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS"
    )
    assert report["stress_required"] == "STRONG"
    assert report["confirmation_target_minimum"] == 3
    assert {row["threshold_family"] for row in report["threshold_rows"]} == set(
        thresholds.REQUIRED_THRESHOLD_FAMILIES
    )
    assert all(row["passed"] is True for row in report["threshold_rows"])
    assert validation["status"] == "PASS"
    assert payload["promotion_gate_threshold_calibration_report"]["status"] == "PASS"
    assert "promotion_threshold_calibration_id" in result["reader_brief_section"]
    assert_research_safe(report)


def test_promotion_gate_threshold_cli_report_and_validate(tmp_path: Path) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path)
    contract = fixture["formal_research_method_contract"]
    output_dir = tmp_path / "threshold_calibration"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "promotion-gate-threshold-calibration",
            "report",
            "--contract-id",
            contract["contract_id"],
            "--contract-dir",
            str(tmp_path / "formal_research_method_contract"),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "current_threshold_interpretation=FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS" in (
        result.output
    )
    calibration_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("calibration_id=")
    )

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "promotion-gate-threshold-calibration",
            "validate",
            "--calibration-id",
            calibration_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output

    policy_validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "promotion-gate-threshold-calibration",
            "validate",
        ],
    )
    assert policy_validation.exit_code == 0
    assert "status=PASS" in policy_validation.output
