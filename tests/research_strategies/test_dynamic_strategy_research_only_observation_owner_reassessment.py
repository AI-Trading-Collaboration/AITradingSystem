from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_observation_owner_reassessment import (
    DEFAULT_REASSESSMENT_CONCLUSION,
    FINAL_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_research_only_observation_owner_reassessment,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2373_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_READY"
)
SOURCE_2373_ROUTE = (
    "TRADING-2374_Dynamic_Strategy_Research_Only_Observation_"
    "Owner_Reassessment_Checkpoint"
)
SOURCE_2373_REPORT_MODE = "RESEARCH_ONLY_MANUAL_DRY_RUN"
OWNER_OPTION_NAMES = [
    "Continue research-only observation",
    "Return to candidate optimization",
    "Compare robustness top vs ranking top deeper",
    "Improve data and PIT coverage first",
    "Stop observation line",
]


def test_dynamic_strategy_research_only_observation_owner_reassessment_builder(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "reassessment"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_observation_owner_reassessment(
        source_report_dry_run_path=source_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_reassessment"] is True
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["owner_reassessment_checkpoint_ready"] is True
    assert payload["owner_reassessment_conclusion"] == DEFAULT_REASSESSMENT_CONCLUSION
    assert payload["research_only_observation_line_closed_for_reassessment"] is True
    assert payload["continue_linear_observation_tasks"] is False
    assert payload["next_task_auto_generated"] is False
    assert payload["trading_2375_auto_created"] is False
    assert payload["recommended_owner_options"] == OWNER_OPTION_NAMES
    assert len(payload["required_owner_questions"]) == 8
    assert payload["recommended_next_research_task"] == FINAL_ROUTE
    assert payload["next_route"] == FINAL_ROUTE
    assert payload["final_route"] == FINAL_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REASSESSMENT_ONLY_NO_FRESH_MARKET_DATA"
    )

    for key in (
        "paper_shadow_enabled",
        "paper_shadow_approved",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "event_append_enabled",
        "event_append_approved",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_approved",
        "outcome_binding_attempted",
        "production_enabled",
        "production_approved",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    checkpoint = payload["owner_reassessment_checkpoint"]
    assert checkpoint["owner_reassessment_checkpoint_ready"] is True
    assert checkpoint["default_conclusion"] == DEFAULT_REASSESSMENT_CONCLUSION
    assert checkpoint["continue_linear_observation_tasks"] is False
    assert checkpoint["trading_2375_auto_created"] is False
    assert checkpoint["final_route"] == FINAL_ROUTE

    options = payload["owner_reassessment_options"]
    assert [option["name"] for option in options] == OWNER_OPTION_NAMES

    evidence = payload["no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["owner_reassessment_checkpoint_only"] is True
    assert evidence["trading_2375_auto_created"] is False
    assert evidence["event_append_attempted"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert evidence["daily_report_generated"] is False

    for key in (
        "json_path",
        "owner_reassessment_checkpoint_json",
        "no_side_effect_evidence_json",
        "markdown_path",
        "checkpoint_markdown",
        "options_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_observation_owner_reassessment_cli(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "reassessment_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-observation-owner-reassessment",
            "--source-report-dry-run",
            str(source_path),
            "--as-of",
            "2026-07-05",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_reassessment_result.json").exists()
    assert (output_root / "owner_reassessment_checkpoint.json").exists()
    assert (output_root / "no_side_effect_evidence.json").exists()

    payload = json.loads(
        (output_root / "owner_reassessment_result.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["owner_reassessment_checkpoint_ready"] is True
    assert payload["continue_linear_observation_tasks"] is False
    assert payload["final_route"] == FINAL_ROUTE


def test_dynamic_strategy_research_only_observation_owner_reassessment_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_research_only_observation_owner_reassessment"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-observation-owner-reassessment"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "owner_reassessment_result.json" in item for item in entry["artifact_globs"]
    )
    assert any(
        "owner_reassessment_checkpoint.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_research_only_observation_owner_reassessment" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-observation-owner-reassessment"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2374_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
        "OWNER_REASSESSMENT_CHECKPOINT"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _write_source_artifact(tmp_path: Path) -> Path:
    source_path = tmp_path / "source" / "observation_report_dry_run_result.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        json.dumps(_source_report_dry_run(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return source_path


def _source_report_dry_run() -> dict[str, object]:
    return {
        "task_id": "TRADING-2373",
        "status": SOURCE_2373_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2373_ROUTE,
        "report_mode": SOURCE_2373_REPORT_MODE,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "observation_record_example_ready": True,
        "observation_report_dry_run_ready": True,
        "no_side_effect_evidence_ready": True,
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "production_effect": "none",
        "broker_action": "none",
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
    }
