from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    DYNAMIC_RESCUE_SAFETY,
    DynamicRescueError,
    build_dynamic_rescue_validation_report,
    build_dynamic_rescue_validation_sample_report,
    load_dynamic_failure_diagnostics_policy_config,
    write_dynamic_failure_dataset,
    write_dynamic_rescue_report,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_rescue_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_failure_diagnostics_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_RESCUE_SAFETY
    assert policy.market_regime.regime_id == "unified_primary_2021"
    assert len(policy.rescue_policy_templates) == 4

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["safety"]["production_effect"] = "write_production_weights"
    unsafe_path = tmp_path / "unsafe_dynamic_failure_diagnostics.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicRescueError):
        load_dynamic_failure_diagnostics_policy_config(unsafe_path)

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["rescue_policy_templates"][0]["constraints"]["production_config_mutation_allowed"] = True
    unsafe_template_path = tmp_path / "unsafe_dynamic_rescue_template.yaml"
    unsafe_template_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicRescueError):
        load_dynamic_failure_diagnostics_policy_config(unsafe_template_path)


def test_dynamic_rescue_report_builds_dataset_attribution_and_candidates() -> None:
    report = _sample_dynamic_rescue_report()
    dataset = report["failure_dataset"]

    assert dataset["row_count"] > 0
    row = dataset["rows"][0]
    assert {
        "date",
        "candidate_id",
        "trend_signal_config_id",
        "dynamic_policy_id",
        "regime_state",
        "input_scores",
        "previous_weights",
        "target_weights",
        "actual_rebalanced_weights",
        "constraint_hits",
        "turnover",
        "forward_return_dynamic",
        "forward_return_static",
        "forward_return_baseline",
        "forward_return_QQQ",
        "forward_return_SPY",
        "forward_return_SMH",
        "forward_drawdown_dynamic",
        "false_risk_off",
        "false_risk_on",
        "underperformance_vs_static",
        "underperformance_vs_baseline",
        "event_risk_flag",
        "data_quality_status",
        "evaluation_only",
        "safety",
    }.issubset(row)
    assert row["evaluation_only"] is True
    assert report["layer1_signal_failure_attribution"]["trend_signal_config_id"] is not None
    assert "false_risk_off_count" in report["false_signal_attribution"]
    assert "cash_drag" in report["layer2_allocation_underperformance_attribution"]
    assert "turnover_by_source" in report["turnover_constraint_breakdown"]
    assert len(report["rescue_policy_templates"]) == 4
    assert len(report["rescue_candidate_comparison"]) == 4
    assert report["shadow_enrollment_allowed"] is False
    assert report["automatic_enrollment_allowed"] is False
    assert report["owner_approval_executed"] is False


def test_dynamic_rescue_writer_cli_latest_and_reader_brief(tmp_path: Path) -> None:
    report = _sample_dynamic_rescue_report()
    dataset_paths = write_dynamic_failure_dataset(
        report["failure_dataset"],
        output_dir=tmp_path / "datasets",
    )
    report_paths = write_dynamic_rescue_report(report, output_dir=tmp_path / "reports")

    assert dataset_paths["json"].exists()
    assert report_paths["markdown"].exists()
    assert "Dynamic Strategy Rescue" in report_paths["markdown"].read_text(encoding="utf-8")

    summary = reader_brief._etf_dynamic_rescue_summary(
        {"reports": [_report_record("etf_dynamic_rescue_evaluation_report", report_paths["json"])]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["shadow_enrollment_allowed"] is False
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["best_rescue_candidate"] != "MISSING"

    missing = reader_brief._etf_dynamic_rescue_summary({"reports": []})
    assert missing["availability"] == "MISSING"

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-rescue",
            "report",
            "--latest",
            "--report-output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "shadow_enrollment_allowed=false" in result.output


def test_dynamic_rescue_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_rescue_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["no_auto_approval"] is True
    assert validation["no_auto_enrollment"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-rescue",
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


def _sample_dynamic_rescue_report() -> dict[str, object]:
    return build_dynamic_rescue_validation_sample_report()


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
