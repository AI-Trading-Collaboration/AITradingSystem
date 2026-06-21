from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    run_data_foundation_acceptance,
    run_data_source_qualification_remediation,
    run_data_source_remediation_execution,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

REQUIRED_RESULT_FIELDS = {
    "before_status",
    "after_status",
    "blocked_reason",
    "fix_applied",
    "remaining_gap",
    "repairable_without_relaxing_gate",
    "promotion_grade_candidate_after_fix",
    "allowed_uses",
}


def test_data_source_remediation_execution_contract(tmp_path: Path) -> None:
    acceptance_root = tmp_path / "acceptance"
    qualification_root = tmp_path / "qualification"
    acceptance_rerun_root = tmp_path / "acceptance_rerun"
    execution_root = tmp_path / "execution"

    run_data_foundation_acceptance(output_root=acceptance_root)
    run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        output_root=qualification_root,
    )
    result = run_data_source_remediation_execution(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        qualification_matrix_path=qualification_root / "data_source_qualification_matrix.json",
        remediation_plan_path=qualification_root / "data_foundation_remediation_plan.json",
        updated_acceptance_summary_path=(
            qualification_root / "data_foundation_acceptance_summary_updated.json"
        ),
        acceptance_output_root=acceptance_rerun_root,
        output_root=execution_root,
    )

    assert result["report_type"] == "data_source_remediation_execution_report"
    assert result["status"] == "REMEDIATION_EXECUTED_WITH_REMAINING_SOURCE_BLOCKERS"
    _assert_safety_boundary(result)
    summary = result["summary"]
    assert summary["matrix_status"] == "BLOCKED_UNTIL_QUALIFIED"
    assert summary["P0_remaining_count"] == 9
    assert summary["P0_resolved_count"] == 0
    assert summary["lookahead_violation_count"] == 0
    assert summary["acceptance_rerun_status"] == "BLOCKED_UNTIL_QUALIFIED_DATA"

    item_results_path = execution_root / "data_source_remediation_item_results.json"
    updated_matrix_path = execution_root / "data_source_qualification_matrix_updated.json"
    report_path = execution_root / "data_source_remediation_execution_report.json"
    assert item_results_path.exists()
    assert item_results_path.with_suffix(".md").exists()
    assert updated_matrix_path.exists()
    assert updated_matrix_path.with_suffix(".md").exists()
    assert report_path.exists()
    assert report_path.with_suffix(".md").exists()
    assert acceptance_rerun_root.joinpath("data_foundation_acceptance_report.json").exists()

    item_payload = _read_json(item_results_path)
    updated_matrix = _read_json(updated_matrix_path)
    _assert_safety_boundary(item_payload)
    _assert_safety_boundary(updated_matrix)

    item_results = item_payload["remediation_item_results"]
    assert len(item_results) == 10
    assert REQUIRED_RESULT_FIELDS <= set(item_results[0])
    assert item_results[0]["priority"] == "P0"
    assert item_results[0]["after_status"] == "BLOCKED_UNTIL_QUALIFIED"
    assert all(item["promotion_gate_allowed"] is False for item in item_results)
    assert all(item["strategy_input_allowed"] is False for item in item_results)
    assert all(item["promotion_evidence_allowed"] is False for item in item_results)
    assert all(item["source_input_allowed"] is False for item in item_results)
    assert all(item["gate_relaxation_allowed"] is False for item in item_results)
    assert not any(item["promotion_grade_candidate_after_fix"] for item in item_results)

    current_view = next(
        item for item in item_results if item["before_status"] == "CURRENT_VIEW_ONLY"
    )
    assert current_view["after_status"] == "CURRENT_VIEW_ONLY"
    assert current_view["allowed_uses"] == ["diagnostic", "research_label"]
    assert "current_view_only_source_isolated" in current_view["fix_applied"]
    assert "current_view_only_as_of_snapshot_missing" in current_view["remaining_gap"]

    research_label = next(
        item for item in item_results if item["before_status"] == "RESEARCH_LABEL_ONLY"
    )
    assert research_label["after_status"] == "RESEARCH_LABEL_ONLY"
    assert research_label["allowed_uses"] == ["analysis", "casebook", "stratified_reporting"]
    assert "research_label_only_usage_policy_applied" in research_label["fix_applied"]

    matrix_summary = updated_matrix["summary"]
    assert matrix_summary["promotion_grade_ready_count"] == 2
    assert matrix_summary["diagnostic_only_count"] == 11
    assert matrix_summary["blocked_until_qualified_count"] == 3
    assert matrix_summary["current_view_only_count"] == 1
    assert matrix_summary["research_label_only_count"] == 2
    assert matrix_summary["P0_remaining_count"] == 9
    assert matrix_summary["P0_resolved_count"] == 0


def test_data_source_remediation_execution_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    acceptance_root = tmp_path / "acceptance"
    qualification_root = tmp_path / "qualification"
    acceptance_rerun_root = tmp_path / "acceptance_rerun"
    execution_root = tmp_path / "execution"

    acceptance_result = runner.invoke(
        app,
        ["data", "foundation-acceptance", "run", "--output-root", str(acceptance_root)],
    )
    assert acceptance_result.exit_code == 0, acceptance_result.output
    remediation_result = runner.invoke(
        app,
        [
            "data",
            "source-qualification",
            "remediate",
            "--acceptance-report",
            str(acceptance_root / "data_foundation_acceptance_report.json"),
            "--output-root",
            str(qualification_root),
        ],
    )
    assert remediation_result.exit_code == 0, remediation_result.output

    result = runner.invoke(
        app,
        [
            "data",
            "source-qualification",
            "execute-remediation",
            "--acceptance-report",
            str(acceptance_root / "data_foundation_acceptance_report.json"),
            "--qualification-matrix",
            str(qualification_root / "data_source_qualification_matrix.json"),
            "--remediation-plan",
            str(qualification_root / "data_foundation_remediation_plan.json"),
            "--updated-acceptance-summary",
            str(qualification_root / "data_foundation_acceptance_summary_updated.json"),
            "--acceptance-output-root",
            str(acceptance_rerun_root),
            "--output-root",
            str(execution_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Data source remediation execution" in result.output
    assert "report：REMEDIATION_EXECUTED_WITH_REMAINING_SOURCE_BLOCKERS" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert "P0_remaining_count=9" in result.output
    assert (execution_root / "data_source_qualification_matrix_updated.json").exists()


def test_data_source_remediation_execution_registry_catalog_schema_and_tiers() -> None:
    test_path = "tests/test_data_source_remediation_execution.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    entry = report_ids["data_source_remediation_execution"]
    assert entry["command"] == "aits data source-qualification execute-remediation"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "data_source_remediation_execution_report.json/md" in catalog
    assert "P0_remaining_count" in catalog
    assert "strategy input" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "aits data source-qualification execute-remediation" in system_flow
    assert "TRADING-736 VALIDATING" in system_flow

    schema = PROJECT_ROOT / "docs" / "schema" / "data_source_remediation_execution.schema.json"
    assert schema.exists()


def _assert_safety_boundary(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
