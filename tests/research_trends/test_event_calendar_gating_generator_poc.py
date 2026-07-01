from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.event_calendar_feasibility_audit import (
    run_event_calendar_data_feasibility_audit,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    DATA_QUALITY_STATUS,
    REQUIRED_USE_CASES,
    SAFETY_FIELDS,
    STATUS,
    EventCalendarGatingGeneratorPocError,
    build_event_gating_source_blocker_report,
    build_event_gating_use_case_readiness_matrix,
    load_trading_2318_feasibility_artifacts,
    run_event_calendar_gating_generator_poc,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_event_gating_generator_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "event-calendar-gating-generator-poc" in result.output


def test_event_gating_generator_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/event_calendar_gating_generator_policy.yaml")
    )

    assert policy["policy_id"] == "event_calendar_gating_generator_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_source_blocked"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["source_dependency"]["required_task_id"] == (
        "TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT"
    )
    assert policy["source_dependency"]["allow_source_blocked_package"] is True
    assert set(policy["required_use_cases"]) == REQUIRED_USE_CASES
    assert policy["inactive_signal_spec"]["spec_status"] == (
        "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
    )

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_event_gating_generator_builders_mark_source_blocked(tmp_path: Path) -> None:
    feasibility_dir = _write_trading_2318_source(tmp_path)
    source = load_trading_2318_feasibility_artifacts(feasibility_dir)
    policy = safe_load_yaml_path(
        Path("config/research/event_calendar_gating_generator_policy.yaml")
    )

    blockers = build_event_gating_source_blocker_report(source=source)
    readiness = build_event_gating_use_case_readiness_matrix(
        policy=policy,
        source=source,
    )

    assert len(blockers) == 8
    assert all(row["blocker_status"] == "SOURCE_BLOCKED_NO_GENERATOR" for row in blockers)
    assert all(row["event_rows_downloaded"] is False for row in blockers)
    assert len(readiness) == 4
    assert {row["use_case_id"] for row in readiness} == REQUIRED_USE_CASES
    assert all(row["readiness_status"] == "SOURCE_BLOCKED_NO_GENERATOR" for row in readiness)
    assert all(row["generator_ready"] is False for row in readiness)


def test_event_gating_generator_cli_writes_source_blocked_outputs(
    tmp_path: Path,
) -> None:
    feasibility_dir = _write_trading_2318_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-calendar-gating-generator-poc",
            "--feasibility-dir",
            str(feasibility_dir),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "event_calendar_gating_generator_summary.json",
        "event_gating_signal_spec.json",
        "event_gating_use_case_readiness_matrix.json",
        "event_gating_use_case_readiness_matrix.csv",
        "event_gating_source_blocker_report.json",
        "event_gating_source_blocker_report.csv",
        "event_gating_manual_review_trigger_contract.json",
        "event_gating_generator_validation_summary.json",
        "event_gating_generator_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert not (output_dir / "event_gating_signal_series.csv").exists()
    assert (docs_root / "event_calendar_gating_generator_poc.md").exists()

    summary_payload = json.loads(
        (output_dir / "event_calendar_gating_generator_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["source_status"] == "EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY"
    assert summary["source_pit_ready_source_count"] == 0
    assert summary["source_blocker_count"] == 8
    assert summary["use_case_readiness_count"] == 4
    assert summary["blocked_use_case_count"] == 4
    assert summary["validation_status"] == "PASS_SOURCE_BLOCKED_EXPECTED"
    assert summary["generator_poc_cli_implemented"] is True
    assert summary["executable_generator_ready"] is False
    assert summary["signal_spec_status"] == "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
    assert summary["event_rows_consumed"] is False
    assert summary["gating_signal_generated"] is False
    assert summary["event_gating_signal_series_generated"] is False
    assert summary["event_outcome_prediction_allowed"] is False
    assert summary["trading_direction_prediction_allowed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    signal_spec = json.loads(
        (output_dir / "event_gating_signal_spec.json").read_text(encoding="utf-8")
    )
    assert signal_spec["spec_status"] == "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
    assert signal_spec["executable_signal_ready"] is False
    assert signal_spec["event_gating_signal_series_generated"] is False
    assert "event_outcome_prediction" in signal_spec["blocked_actions"]
    assert "trading_direction_prediction" in signal_spec["blocked_actions"]

    readiness = pd.read_csv(output_dir / "event_gating_use_case_readiness_matrix.csv")
    assert set(readiness["use_case_id"]) == REQUIRED_USE_CASES
    assert readiness["readiness_status"].eq("SOURCE_BLOCKED_NO_GENERATOR").all()
    assert readiness["generator_ready"].eq(False).all()

    safety = json.loads(
        (output_dir / "event_gating_generator_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_download_external_event_rows"] is True
    assert safety["does_not_generate_event_gating_signal"] is True
    assert safety["does_not_predict_event_outcome"] is True
    assert safety["does_not_predict_trading_direction"] is True


def test_event_gating_generator_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_event_calendar_gating_generator_poc(
        feasibility_dir=_write_trading_2318_source(tmp_path),
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_event_gating_generator_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-calendar-gating-generator-poc",
            "--feasibility-dir",
            str(_write_trading_2318_source(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_event_gating_generator_fails_closed_if_source_unblocked(
    tmp_path: Path,
) -> None:
    feasibility_dir = _write_trading_2318_source(tmp_path)
    summary_path = feasibility_dir / "event_calendar_data_feasibility_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["summary"]["pit_ready_source_count"] = 1
    payload["pit_ready_source_count"] = 1
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(EventCalendarGatingGeneratorPocError, match="PIT-ready"):
        run_event_calendar_gating_generator_poc(
            feasibility_dir=feasibility_dir,
            output_dir=tmp_path / "out",
            docs_root=tmp_path / "docs",
        )


def test_event_gating_generator_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "event_calendar_gating_generator_poc"
    )

    assert entry["command"] == "aits research trends event-calendar-gating-generator-poc"
    assert entry["artifact_role"] == "event_calendar_gating_generator_poc_source_blocked"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["source_pit_ready_source_count"] == 0
    assert entry["source_blocker_count"] == 8
    assert entry["use_case_readiness_count"] == 4
    assert entry["blocked_use_case_count"] == 4
    assert entry["source_blocked_no_generation"] is True
    assert entry["executable_generator_ready"] is False
    assert entry["event_rows_consumed"] is False
    assert entry["gating_signal_generated"] is False
    assert entry["event_gating_signal_series_generated"] is False
    assert entry["event_outcome_prediction_allowed"] is False
    assert entry["trading_direction_prediction_allowed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "event_calendar_gating_generator_poc" in catalog
    assert STATUS in catalog
    assert "不是 executable event gating generator" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2319" in system_flow
    assert "event-calendar-gating-generator-poc" in system_flow
    assert "SOURCE_BLOCKED_NO_GENERATOR" in system_flow
    assert "event_gating_signal_series_generated=false" in system_flow


def _write_trading_2318_source(tmp_path: Path) -> Path:
    feasibility_dir = tmp_path / "trading_2318"
    run_event_calendar_data_feasibility_audit(
        output_dir=feasibility_dir,
        docs_root=tmp_path / "docs2318",
    )
    return feasibility_dir
