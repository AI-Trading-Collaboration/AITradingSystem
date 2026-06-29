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
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

EXPECTED_MODULE_COMPONENTS = {
    "pit_feature_store",
    "asset_master",
    "tradable_universe",
    "cost_liquidity_model",
    "regime_event_cluster_labels",
    "run_registry",
    "execution_cache",
    "forward_evidence_archive",
    "research_case_library",
}
EXPECTED_MATRIX_CATEGORIES = {
    "PROMOTION_GRADE_READY",
    "DIAGNOSTIC_ONLY",
    "BLOCKED_UNTIL_QUALIFIED",
    "RESEARCH_LABEL_ONLY",
    "CURRENT_VIEW_ONLY",
    "UNKNOWN_REQUIRES_MANUAL_REVIEW",
}
EXPECTED_P0_SOURCE_FAMILIES = {
    "price / adjusted price / corporate actions",
    "SEC / fundamental PIT availability",
    "asset master / ticker / tradability",
    "event labels as-known-before vs post-hoc",
    "cost / spread / liquidity assumptions",
}
REQUIRED_REMEDIATION_FIELDS = {
    "component",
    "current_status",
    "blocked_reason",
    "missing_contract",
    "missing_source_manifest",
    "missing_available_time",
    "current_view_only_risk",
    "lineage_gap",
    "PIT_risk",
    "repairable_without_relaxing_gate",
    "required_fix",
    "expected_promotion_grade_gain_if_fixed",
}


def test_source_qualification_remediation_contract(tmp_path: Path) -> None:
    acceptance_root = tmp_path / "acceptance"
    remediation_root = tmp_path / "qualification"
    acceptance = run_data_foundation_acceptance(output_root=acceptance_root)
    result = run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        output_root=remediation_root,
    )

    assert result["report_type"] == "data_source_qualification_remediation"
    assert result["status"] == "REMEDIATION_PLAN_READY"
    _assert_safety_boundary(result)
    assert result["summary"]["matrix_status"] == "BLOCKED_UNTIL_QUALIFIED"
    assert result["summary"]["lookahead_violation_count"] == 0
    assert result["summary"]["module_count"] == 9
    assert result["summary"]["remediation_item_count"] >= 10

    matrix_path = remediation_root / "data_source_qualification_matrix.json"
    plan_path = remediation_root / "data_foundation_remediation_plan.json"
    updated_path = remediation_root / "data_foundation_acceptance_summary_updated.json"
    assert matrix_path.exists()
    assert matrix_path.with_suffix(".md").exists()
    assert plan_path.exists()
    assert plan_path.with_suffix(".md").exists()
    assert updated_path.exists()
    assert updated_path.with_suffix(".md").exists()

    matrix = _read_json(matrix_path)
    plan = _read_json(plan_path)
    updated = _read_json(updated_path)
    _assert_safety_boundary(matrix)
    _assert_safety_boundary(plan)
    _assert_safety_boundary(updated)

    assert matrix["source_acceptance_status"] == acceptance["status"]
    assert set(matrix["source_qualification_matrix"]) == EXPECTED_MATRIX_CATEGORIES
    assert matrix["source_qualification_matrix"]["BLOCKED_UNTIL_QUALIFIED"] >= 1
    assert matrix["source_qualification_matrix"]["DIAGNOSTIC_ONLY"] >= 1
    assert matrix["source_qualification_matrix"]["CURRENT_VIEW_ONLY"] >= 1
    assert matrix["source_qualification_matrix"]["RESEARCH_LABEL_ONLY"] >= 1

    modules = matrix["module_level_qualification"]
    assert {item["component"] for item in modules} == EXPECTED_MODULE_COMPONENTS
    assert all(item["promotion_gate_allowed"] is False for item in modules)
    assert all(item["broker_action"] == "none" for item in modules)
    module_categories = {item["qualification_category"] for item in modules}
    assert {"PROMOTION_GRADE_READY", "DIAGNOSTIC_ONLY", "BLOCKED_UNTIL_QUALIFIED"} <= (
        module_categories
    )

    source_acceptance_summary = updated["source_acceptance_summary"]
    acceptance_summary = acceptance["summary"]
    source_count_fields = (
        "promotion_grade_ready_count",
        "diagnostic_only_count",
        "blocked_until_qualified_count",
    )
    for field in source_count_fields:
        assert source_acceptance_summary[field] == acceptance_summary[field]
    assert source_acceptance_summary["promotion_grade_ready_count"] == 0
    assert (
        source_acceptance_summary["diagnostic_only_count"]
        + source_acceptance_summary["blocked_until_qualified_count"]
        >= 5
    )
    assert updated["summary"]["source_acceptance_status"] == "BLOCKED_UNTIL_QUALIFIED_DATA"


def test_source_qualification_remediation_rows_and_p0_priorities(tmp_path: Path) -> None:
    acceptance_root = tmp_path / "acceptance"
    remediation_root = tmp_path / "qualification"
    run_data_foundation_acceptance(output_root=acceptance_root)
    run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        output_root=remediation_root,
    )

    plan = _read_json(remediation_root / "data_foundation_remediation_plan.json")
    items = plan["remediation_items"]
    statuses = {item["current_status"] for item in items}

    assert {"BLOCKED_UNTIL_QUALIFIED", "DIAGNOSTIC_ONLY", "CURRENT_VIEW_ONLY"} <= statuses
    assert "RESEARCH_LABEL_ONLY" in statuses
    assert all(REQUIRED_REMEDIATION_FIELDS <= set(item) for item in items)
    assert all(item["gate_relaxation_allowed"] is False for item in items)
    assert all(item["promotion_gate_allowed"] is False for item in items)
    assert all(item["paper_shadow_change_allowed"] is False for item in items)
    assert all(item["production_weight_change_allowed"] is False for item in items)
    assert all(item["broker_action"] == "none" for item in items)
    assert all(item["lookahead_violation_count"] == 0 for item in items)
    assert any(item["missing_source_manifest"] for item in items)
    assert any(item["missing_available_time"] for item in items)
    assert any(item["current_view_only_risk"] for item in items)
    assert any(item["PIT_risk"] == "HIGH" for item in items)

    p0_families = {item["source_family"] for item in plan["P0_remediation_priorities"]}
    assert EXPECTED_P0_SOURCE_FAMILIES <= p0_families
    assert plan["summary"]["no_gate_relaxation"] is True
    assert plan["summary"]["P0_item_count"] >= 8
    assert plan["summary"]["expected_promotion_grade_gain_if_fixed"] == sum(
        int(item["expected_promotion_grade_gain_if_fixed"]) for item in items
    )
    assert plan["blocked_until_qualified_items"] == [
        item for item in items if item["current_status"] == "BLOCKED_UNTIL_QUALIFIED"
    ]
    assert plan["diagnostic_only_items"] == [
        item for item in items if item["current_status"] == "DIAGNOSTIC_ONLY"
    ]
    assert plan["current_view_only_items"] == [
        item for item in items if item["current_status"] == "CURRENT_VIEW_ONLY"
    ]


def test_source_qualification_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    acceptance_root = tmp_path / "acceptance"
    remediation_root = tmp_path / "qualification"
    acceptance_result = runner.invoke(
        app,
        ["data", "foundation-acceptance", "run", "--output-root", str(acceptance_root)],
    )
    assert acceptance_result.exit_code == 0, acceptance_result.output

    result = runner.invoke(
        app,
        [
            "data",
            "source-qualification",
            "remediate",
            "--acceptance-report",
            str(acceptance_root / "data_foundation_acceptance_report.json"),
            "--output-root",
            str(remediation_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Data source qualification remediation" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert "lookahead_violation_count=0" in result.output
    assert (remediation_root / "data_source_qualification_matrix.json").exists()
    assert (remediation_root / "data_foundation_remediation_plan.json").exists()


def test_source_qualification_registry_catalog_schemas_and_tiers() -> None:
    test_path = "tests/test_data_source_qualification_remediation.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    entry = report_ids["data_source_qualification_remediation"]
    assert entry["command"] == "aits data source-qualification remediate"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "data_source_qualification_matrix.json/md" in catalog
    assert "CURRENT_VIEW_ONLY" in catalog
    assert "不能用于 paper-shadow" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "aits data source-qualification remediate" in system_flow
    assert "TRADING-735 VALIDATING" in system_flow

    matrix_schema = (
        PROJECT_ROOT / "docs" / "schema" / "data_source_qualification_matrix.schema.json"
    )
    plan_schema = PROJECT_ROOT / "docs" / "schema" / "data_foundation_remediation_plan.schema.json"
    assert matrix_schema.exists()
    assert plan_schema.exists()


def _assert_safety_boundary(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
