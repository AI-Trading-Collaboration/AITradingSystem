from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import run_data_foundation_acceptance
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS


def test_data_foundation_acceptance_report_contract(tmp_path: Path) -> None:
    payload = run_data_foundation_acceptance(output_root=tmp_path)
    summary = payload["summary"]

    assert payload["report_type"] == "data_foundation_acceptance_report"
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["requested_tickers"] == [
        "SPY",
        "QQQ",
        "SMH",
        "MSFT",
        "GOOGL",
        "NVDA",
        "AMD",
        "TSM",
        "CASH",
    ]
    assert summary["production_effect"] == "none"
    assert summary["broker_action"] == "none"
    assert summary["promotion_gate_allowed"] is False
    assert summary["paper_shadow_change_allowed"] is False
    assert summary["production_weight_change_allowed"] is False
    assert summary["lookahead_violation_count"] == 0
    assert summary["promotion_grade_ready_count"] == 0
    assert payload["source_qualification_summary"] == summary["source_qualification_summary"]
    assert payload["promotion_grade_ready_count"] == summary["promotion_grade_ready_count"]
    assert payload["diagnostic_only_count"] == summary["diagnostic_only_count"]
    assert payload["blocked_until_qualified_count"] == summary["blocked_until_qualified_count"]
    assert payload["lookahead_violation_count"] == summary["lookahead_violation_count"]
    assert summary["diagnostic_only_count"] + summary["blocked_until_qualified_count"] >= 5
    assert payload["fail_closed_probe"]["status"] == "FAIL_CLOSED"
    assert payload["pit_checks"]["available_time_on_or_before_decision_time"] is True
    assert payload["pit_checks"]["config_hash_present"] is True
    assert payload["pit_checks"]["input_hash_present"] is True
    assert payload["pit_checks"]["current_view_only_feature_count"] >= 1
    assert payload["cost_checks"]["turnover_higher_cost_not_lower"] is True
    assert payload["label_checks"]["as_of_label_and_post_hoc_label_distinguished"] is True
    assert payload["execution_checks"]["cache_hit_miss_verified"] is True
    assert payload["forward_checks"]["future_outcomes_appended_only"] is True
    assert (
        payload["forward_checks"]["historical_decision_fields_immutable_after_outcome_update"]
        is True
    )
    assert payload["case_library_checks"]["oracle_case_promotion_gate_allowed"] is False
    assert payload["case_library_checks"]["case_reuse_in_strategy_pair_diagnostics"] is True

    families = {item["feature_family"] for item in payload["feature_risk_classification"]}
    assert {"SEC", "fundamental", "valuation", "trend", "price"} <= families

    report_path = tmp_path / "data_foundation_acceptance_report.json"
    assert report_path.exists()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["summary"]["production_effect"] == "none"

    run_registry = tmp_path / "component_artifacts" / "research_runs" / "run_registry.jsonl"
    run_records = [
        json.loads(line)
        for line in run_registry.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(run_records) == 3
    for record in run_records:
        assert record["dataset_version"]
        assert record["feature_snapshot_id"] == payload["snapshot_id"]
        assert record["asset_universe_version"]
        assert record["cost_model_version"]
        assert record["label_version"]
        assert record["config_hash"]
        assert record["code_version"]
        assert record["artifact_paths"]
        assert record["promotion_gate_allowed"] is False
        assert record["paper_shadow_change_allowed"] is False
        assert record["production_weight_change_allowed"] is False
        assert record["broker_action"] == "none"
        assert record["production_effect"] == "none"


def test_data_foundation_acceptance_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "data",
            "foundation-acceptance",
            "run",
            "--output-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Data foundation acceptance report" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert (tmp_path / "data_foundation_acceptance_report.json").exists()


def test_data_foundation_acceptance_registry_catalog_and_tiers() -> None:
    test_path = "tests/test_data_foundation_acceptance.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    entry = report_ids["data_foundation_acceptance"]
    assert entry["command"] == "aits data foundation-acceptance run"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "data_foundation_acceptance_report.json/md" in catalog
    assert "BLOCKED_UNTIL_QUALIFIED_DATA" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "aits data foundation-acceptance run" in system_flow
    assert "TRADING-734 VALIDATING" in system_flow
