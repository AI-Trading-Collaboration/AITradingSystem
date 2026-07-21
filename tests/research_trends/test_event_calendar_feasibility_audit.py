from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.event_calendar_feasibility_audit import (
    DATA_QUALITY_STATUS,
    REQUIRED_EVENT_FAMILIES,
    REQUIRED_USE_CASES,
    SAFETY_FIELDS,
    STATUS,
    build_event_calendar_gating_use_case_matrix,
    build_event_calendar_source_inventory,
    run_event_calendar_data_feasibility_audit,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_event_calendar_feasibility_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "event-calendar-data-feasibility-audit" in result.output


def test_event_calendar_feasibility_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/event_calendar_feasibility_policy.yaml")
    )

    assert policy["policy_id"] == "event_calendar_feasibility_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == "TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT"
    assert policy["market_regime"] == "unified_primary_2021"
    assert policy["data_quality"]["status"] == DATA_QUALITY_STATUS
    assert policy["source_manifest_requirements"]["required"] is True
    assert {
        source["event_family"] for source in policy["event_sources"].values()
    } == REQUIRED_EVENT_FAMILIES
    assert set(policy["gating_use_cases"]) == REQUIRED_USE_CASES
    assert all(
        use_case["owner_review_required"] is True
        for use_case in policy["gating_use_cases"].values()
    )

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_event_calendar_source_inventory_and_use_cases_are_source_audit_only() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/event_calendar_feasibility_policy.yaml")
    )
    source_rows = build_event_calendar_source_inventory(policy)
    use_case_rows = build_event_calendar_gating_use_case_matrix(
        policy=policy,
        source_rows=source_rows,
    )

    assert len(source_rows) == 8
    assert {row["event_family"] for row in source_rows} == REQUIRED_EVENT_FAMILIES
    assert all(row["source_status"] == "SOURCE_AUDIT_REQUIRED" for row in source_rows)
    assert all(row["event_rows_downloaded"] is False for row in source_rows)
    assert all(row["generator_poc_ready"] is False for row in source_rows)
    assert all(row["promotion_allowed"] is False for row in source_rows)
    assert all(row["broker_action"] == "none" for row in source_rows)

    assert len(use_case_rows) == 4
    assert {row["use_case_id"] for row in use_case_rows} == REQUIRED_USE_CASES
    assert all(row["gating_generator_ready"] is False for row in use_case_rows)
    assert all(row["owner_review_required"] is True for row in use_case_rows)


def test_event_calendar_feasibility_cli_writes_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-calendar-data-feasibility-audit",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "event_calendar_data_feasibility_summary.json",
        "event_calendar_source_inventory.json",
        "event_calendar_source_inventory.csv",
        "event_calendar_known_at_requirement_matrix.json",
        "event_calendar_known_at_requirement_matrix.csv",
        "event_calendar_gating_use_case_matrix.json",
        "event_calendar_gating_use_case_matrix.csv",
        "event_calendar_manual_review_trigger_contract.json",
        "event_calendar_validation_route.json",
        "event_calendar_validation_route.csv",
        "event_calendar_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "event_calendar_data_feasibility_audit.md").exists()

    summary_payload = json.loads(
        (output_dir / "event_calendar_data_feasibility_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["actual_requested_date_range"] == "static_feasibility_audit"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["event_source_count"] == 8
    assert summary["source_audit_required_count"] == 8
    assert summary["pit_ready_source_count"] == 0
    assert summary["source_blocked_count"] == 4
    assert summary["known_at_requirement_row_count"] == 8
    assert summary["gating_use_case_count"] == 4
    assert summary["validation_route_row_count"] == 12
    assert summary["generator_poc_ready_now"] is False
    assert summary["event_rows_downloaded"] is False
    assert summary["event_calendar_cache_written"] is False
    assert summary["gating_signal_generated"] is False
    assert summary["event_outcome_prediction_allowed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    inventory = pd.read_csv(output_dir / "event_calendar_source_inventory.csv")
    assert set(inventory["event_family"]) == REQUIRED_EVENT_FAMILIES
    assert inventory["event_rows_downloaded"].eq(False).all()
    assert inventory["runtime_gating_ready"].eq(False).all()

    use_cases = pd.read_csv(output_dir / "event_calendar_gating_use_case_matrix.csv")
    assert set(use_cases["use_case_id"]) == REQUIRED_USE_CASES
    assert use_cases["gating_generator_ready"].eq(False).all()

    safety = json.loads(
        (output_dir / "event_calendar_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_read_market_cache"] is True
    assert safety["does_not_download_external_event_rows"] is True
    assert safety["does_not_generate_event_gating_signal"] is True
    assert safety["does_not_predict_event_outcome"] is True
    assert safety["dynamic_promotion_status"] == "BLOCKED"


def test_event_calendar_feasibility_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_event_calendar_data_feasibility_audit(
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_event_calendar_feasibility_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-calendar-data-feasibility-audit",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_event_calendar_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "event_calendar_data_feasibility_audit"
    )

    assert entry["command"] == "aits research trends event-calendar-data-feasibility-audit"
    assert entry["artifact_role"] == "event_calendar_feasibility_audit"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["static_feasibility_audit"] is True
    assert entry["event_source_count"] == 8
    assert entry["source_audit_required_count"] == 8
    assert entry["pit_ready_source_count"] == 0
    assert entry["gating_use_case_count"] == 4
    assert entry["event_rows_downloaded"] is False
    assert entry["gating_signal_generated"] is False
    assert entry["event_outcome_prediction_allowed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "event_calendar_data_feasibility_audit" in catalog
    assert STATUS in catalog
    assert "不是 event calendar ingestion" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2318" in system_flow
    assert "event-calendar-data-feasibility-audit" in system_flow
    assert "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT" in system_flow
    assert "event_rows_downloaded=false" in system_flow
