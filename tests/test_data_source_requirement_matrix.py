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
    run_data_source_requirement_matrix,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

EXPECTED_REQUIREMENT_CATEGORIES = {
    "SOURCE_MANIFEST_REQUIRED",
    "AVAILABLE_TIME_REQUIRED",
    "AS_OF_SNAPSHOT_REQUIRED",
    "CORPORATE_ACTION_POLICY_REQUIRED",
    "VENDOR_CURRENT_VIEW_ONLY",
    "RESEARCH_LABEL_ONLY_BY_DESIGN",
    "TRUE_PIT_LIMITATION",
    "MANUAL_REVIEW_REQUIRED",
}
REQUIRED_REQUIREMENT_FIELDS = {
    "component",
    "current_status",
    "missing_proof",
    "required_raw_source",
    "required_timestamp_fields",
    "required_source_manifest",
    "required_as_of_snapshot",
    "required_corporate_action_revision_policy",
    "can_fix_with_existing_data",
    "requires_new_data_source",
    "can_remain_diagnostic_only",
    "promotion_grade_blocker",
}


def test_data_source_requirement_matrix_contract(tmp_path: Path) -> None:
    acceptance_root = tmp_path / "acceptance"
    qualification_root = tmp_path / "qualification"
    execution_root = tmp_path / "execution"
    requirement_root = tmp_path / "requirements"

    _build_trading_736_inputs(
        acceptance_root=acceptance_root,
        qualification_root=qualification_root,
        execution_root=execution_root,
    )
    result = run_data_source_requirement_matrix(
        remediation_execution_report_path=(
            execution_root / "data_source_remediation_execution_report.json"
        ),
        remediation_item_results_path=execution_root / "data_source_remediation_item_results.json",
        qualification_matrix_updated_path=(
            execution_root / "data_source_qualification_matrix_updated.json"
        ),
        output_root=requirement_root,
    )

    assert result["report_type"] == "data_source_requirement_matrix"
    assert result["status"] == "REQUIREMENTS_READY_WITH_SOURCE_BLOCKERS"
    _assert_safety_boundary(result)
    summary = result["summary"]
    assert summary["source_requirement_count"] == 9
    assert summary["P0_remaining_count"] == 9
    assert summary["P0_requirement_count"] == 9
    assert summary["can_fix_with_existing_data_count"] == 0
    assert summary["requires_new_data_source_count"] == 8
    assert summary["can_remain_diagnostic_only_count"] == 7
    assert summary["promotion_grade_blocker_count"] == 9
    assert summary["status_upgrade_attempted"] is False

    output_path = requirement_root / "data_source_requirement_matrix.json"
    assert output_path.exists()
    assert output_path.with_suffix(".md").exists()
    persisted = _read_json(output_path)
    _assert_safety_boundary(persisted)

    requirements = persisted["source_requirements"]
    assert len(requirements) == 9
    assert all(REQUIRED_REQUIREMENT_FIELDS <= set(item) for item in requirements)
    assert all(item["promotion_grade_blocker"] is True for item in requirements)
    assert all(item["status_upgrade_attempted"] is False for item in requirements)
    assert all(item["promotion_gate_allowed"] is False for item in requirements)
    assert all(item["strategy_input_allowed"] is False for item in requirements)
    assert all(item["promotion_evidence_allowed"] is False for item in requirements)
    assert not any(item["can_fix_with_existing_data"] for item in requirements)

    category_counts = persisted["source_requirement_category_counts"]
    assert set(persisted["source_requirement_categories"]) == EXPECTED_REQUIREMENT_CATEGORIES
    assert set(category_counts) == EXPECTED_REQUIREMENT_CATEGORIES
    assert all(category_counts[category] >= 1 for category in EXPECTED_REQUIREMENT_CATEGORIES)

    current_view = next(
        item for item in requirements if item["current_status"] == "CURRENT_VIEW_ONLY"
    )
    assert "VENDOR_CURRENT_VIEW_ONLY" in current_view["requirement_categories"]
    assert current_view["requires_new_data_source"] is True
    assert "as_of_snapshot" in current_view["missing_proof"]

    research_label = next(
        item for item in requirements if item["current_status"] == "RESEARCH_LABEL_ONLY"
    )
    assert "RESEARCH_LABEL_ONLY_BY_DESIGN" in research_label["requirement_categories"]
    assert research_label["can_remain_diagnostic_only"] is True
    assert research_label["allowed_uses_until_qualified"] == [
        "analysis",
        "casebook",
        "stratified_reporting",
    ]

    forward_limitation = next(
        item for item in requirements if item["component"] == "forward_evidence_archive"
    )
    assert "TRUE_PIT_LIMITATION" in forward_limitation["requirement_categories"]
    assert forward_limitation["requires_new_data_source"] is False


def test_data_source_requirement_matrix_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    acceptance_root = tmp_path / "acceptance"
    qualification_root = tmp_path / "qualification"
    execution_root = tmp_path / "execution"
    requirement_root = tmp_path / "requirements"

    _build_trading_736_inputs_with_cli(
        runner=runner,
        acceptance_root=acceptance_root,
        qualification_root=qualification_root,
        execution_root=execution_root,
    )
    result = runner.invoke(
        app,
        [
            "data",
            "source-qualification",
            "requirements",
            "--remediation-execution-report",
            str(execution_root / "data_source_remediation_execution_report.json"),
            "--remediation-item-results",
            str(execution_root / "data_source_remediation_item_results.json"),
            "--qualification-matrix-updated",
            str(execution_root / "data_source_qualification_matrix_updated.json"),
            "--output-root",
            str(requirement_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Data source requirement matrix" in result.output
    assert "source_requirement_count=9" in result.output
    assert "status_upgrade_attempted=False" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert (requirement_root / "data_source_requirement_matrix.json").exists()


def test_data_source_requirement_registry_catalog_schema_and_tiers() -> None:
    test_path = "tests/test_data_source_requirement_matrix.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    entry = report_ids["data_source_requirement_matrix"]
    assert entry["command"] == "aits data source-qualification requirements"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "data_source_requirement_matrix.json/md" in catalog
    assert "SOURCE_MANIFEST_REQUIRED" in catalog
    assert "不尝试升级数据源状态" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "aits data source-qualification requirements" in system_flow
    assert "TRADING-737 VALIDATING" in system_flow

    schema = PROJECT_ROOT / "docs" / "schema" / "data_source_requirement_matrix.schema.json"
    assert schema.exists()


def _build_trading_736_inputs(
    *,
    acceptance_root: Path,
    qualification_root: Path,
    execution_root: Path,
) -> None:
    run_data_foundation_acceptance(output_root=acceptance_root)
    run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        output_root=qualification_root,
    )
    run_data_source_remediation_execution(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        qualification_matrix_path=qualification_root / "data_source_qualification_matrix.json",
        remediation_plan_path=qualification_root / "data_foundation_remediation_plan.json",
        updated_acceptance_summary_path=(
            qualification_root / "data_foundation_acceptance_summary_updated.json"
        ),
        acceptance_output_root=acceptance_root / "rerun",
        output_root=execution_root,
    )


def _build_trading_736_inputs_with_cli(
    *,
    runner: CliRunner,
    acceptance_root: Path,
    qualification_root: Path,
    execution_root: Path,
) -> None:
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
    execution_result = runner.invoke(
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
            str(acceptance_root / "rerun"),
            "--output-root",
            str(execution_root),
        ],
    )
    assert execution_result.exit_code == 0, execution_result.output


def _assert_safety_boundary(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
