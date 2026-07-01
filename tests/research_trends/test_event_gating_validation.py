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
    run_event_calendar_gating_generator_poc,
)
from ai_trading_system.event_gating_validation import (
    DATA_QUALITY_STATUS,
    OBJECTIVE_STATUS,
    REQUIRED_OBJECTIVES,
    SAFETY_FIELDS,
    STATUS,
    EventGatingValidationError,
    build_event_gating_validation_data_requirement_matrix,
    build_event_gating_validation_readiness_matrix,
    load_trading_2319_gating_generator_artifacts,
    run_event_gating_validation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_event_gating_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "event-gating-validation" in result.output


def test_event_gating_validation_policy_is_governed() -> None:
    policy = safe_load_yaml_path(Path("config/research/event_gating_validation_policy.yaml"))

    assert policy["policy_id"] == "event_gating_validation_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_source_blocked"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == "TRADING-2320_EVENT_GATING_VALIDATION"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["source_dependency"]["required_task_id"] == (
        "TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC"
    )
    assert policy["source_dependency"]["allow_source_blocked_package"] is True
    assert set(policy["validation_objectives"]) == REQUIRED_OBJECTIVES
    assert policy["metric_contract"]["contract_status"] == (
        "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"
    )

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_event_gating_validation_builders_mark_source_blocked(tmp_path: Path) -> None:
    generator_dir = _write_trading_2319_source(tmp_path)
    source = load_trading_2319_gating_generator_artifacts(generator_dir)
    policy = safe_load_yaml_path(Path("config/research/event_gating_validation_policy.yaml"))

    readiness = build_event_gating_validation_readiness_matrix(
        policy=policy,
        source=source,
    )
    requirements = build_event_gating_validation_data_requirement_matrix(
        policy=policy,
        source=source,
    )

    assert len(readiness) == 3
    assert {row["validation_objective"] for row in readiness} == REQUIRED_OBJECTIVES
    assert all(row["readiness_status"] == OBJECTIVE_STATUS for row in readiness)
    assert all(row["validation_ready"] is False for row in readiness)
    assert all(row["effect_claim_allowed"] is False for row in readiness)
    assert len(requirements) == 15
    assert all(
        row["requirement_status"] == "MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED"
        for row in requirements
    )


def test_event_gating_validation_cli_writes_source_blocked_outputs(
    tmp_path: Path,
) -> None:
    generator_dir = _write_trading_2319_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-gating-validation",
            "--generator-dir",
            str(generator_dir),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "event_gating_validation_summary.json",
        "event_gating_validation_metric_contract.json",
        "event_gating_validation_readiness_matrix.json",
        "event_gating_validation_readiness_matrix.csv",
        "event_gating_validation_blocker_report.json",
        "event_gating_validation_blocker_report.csv",
        "event_gating_validation_data_requirement_matrix.json",
        "event_gating_validation_data_requirement_matrix.csv",
        "event_gating_validation_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert not (output_dir / "event_gating_validation_result.csv").exists()
    assert (docs_root / "event_gating_validation.md").exists()

    summary_payload = json.loads(
        (output_dir / "event_gating_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["source_status"] == (
        "EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL"
    )
    assert summary["source_signal_spec_status"] == "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
    assert summary["validation_objective_count"] == 3
    assert summary["blocked_objective_count"] == 3
    assert summary["data_requirement_count"] == 15
    assert summary["blocker_count"] == 18
    assert summary["validation_status"] == "PASS_SOURCE_BLOCKED_EXPECTED"
    assert summary["event_gating_validation_cli_implemented"] is True
    assert summary["executable_validation_ready"] is False
    assert summary["event_rows_consumed"] is False
    assert summary["gating_signal_consumed"] is False
    assert summary["event_gating_signal_series_consumed"] is False
    assert summary["market_data_consumed"] is False
    assert summary["event_gating_validation_executed"] is False
    assert summary["validation_result_generated"] is False
    assert summary["event_outcome_prediction_allowed"] is False
    assert summary["trading_direction_prediction_allowed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    metric_contract = json.loads(
        (output_dir / "event_gating_validation_metric_contract.json").read_text(
            encoding="utf-8"
        )
    )
    assert metric_contract["status"] == "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"
    assert metric_contract["metric_result_generated"] is False
    assert metric_contract["effect_claim_generated"] is False
    assert metric_contract["executable_validation_ready"] is False
    assert "validation_effect_claim" in metric_contract["blocked_actions"]

    readiness = pd.read_csv(output_dir / "event_gating_validation_readiness_matrix.csv")
    assert set(readiness["validation_objective"]) == REQUIRED_OBJECTIVES
    assert readiness["readiness_status"].eq(OBJECTIVE_STATUS).all()
    assert readiness["validation_ready"].eq(False).all()

    safety = json.loads(
        (output_dir / "event_gating_validation_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_read_market_cache"] is True
    assert safety["does_not_read_event_rows"] is True
    assert safety["does_not_execute_event_gating_validation"] is True
    assert safety["does_not_generate_effect_claim"] is True


def test_event_gating_validation_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_event_gating_validation(
        generator_dir=_write_trading_2319_source(tmp_path),
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_event_gating_validation_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "event-gating-validation",
            "--generator-dir",
            str(_write_trading_2319_source(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "actual_path_validation",
        ],
    )

    assert result.exit_code != 0


def test_event_gating_validation_fails_closed_if_source_has_signal(
    tmp_path: Path,
) -> None:
    generator_dir = _write_trading_2319_source(tmp_path)
    summary_path = generator_dir / "event_calendar_gating_generator_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["gating_signal_generated"] = True
    payload["summary"]["gating_signal_generated"] = True
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(EventGatingValidationError, match="gating signal"):
        run_event_gating_validation(
            generator_dir=generator_dir,
            output_dir=tmp_path / "out",
            docs_root=tmp_path / "docs",
        )


def test_event_gating_validation_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report for report in registry["reports"] if report["report_id"] == "event_gating_validation"
    )

    assert entry["command"] == "aits research trends event-gating-validation"
    assert entry["artifact_role"] == "event_gating_validation_source_blocked"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["source_signal_spec_status"] == "SOURCE_BLOCKED_INACTIVE_SPEC_ONLY"
    assert entry["validation_objective_count"] == 3
    assert entry["blocked_objective_count"] == 3
    assert entry["data_requirement_count"] == 15
    assert entry["blocker_count"] == 18
    assert entry["source_blocked_no_validation"] is True
    assert entry["executable_validation_ready"] is False
    assert entry["event_rows_consumed"] is False
    assert entry["gating_signal_consumed"] is False
    assert entry["event_gating_signal_series_consumed"] is False
    assert entry["market_data_consumed"] is False
    assert entry["event_gating_validation_executed"] is False
    assert entry["validation_result_generated"] is False
    assert entry["event_outcome_prediction_allowed"] is False
    assert entry["trading_direction_prediction_allowed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "event_gating_validation" in catalog
    assert STATUS in catalog
    assert "不是 event gating 有效性验证" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2320" in system_flow
    assert "event-gating-validation" in system_flow
    assert OBJECTIVE_STATUS in system_flow
    assert "event_gating_validation_executed=false" in system_flow


def _write_trading_2319_source(tmp_path: Path) -> Path:
    feasibility_dir = tmp_path / "trading_2318"
    generator_dir = tmp_path / "trading_2319"
    run_event_calendar_data_feasibility_audit(
        output_dir=feasibility_dir,
        docs_root=tmp_path / "docs2318",
    )
    run_event_calendar_gating_generator_poc(
        feasibility_dir=feasibility_dir,
        output_dir=generator_dir,
        docs_root=tmp_path / "docs2319",
    )
    return generator_dir
