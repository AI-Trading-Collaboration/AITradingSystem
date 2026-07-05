from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_observation_log_schema_plan import (
    NEXT_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_research_only_observation_log_schema_plan,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    OWNER_DECISION,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2371_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
    "OWNER_REVIEW_DECISION_READY"
)
SOURCE_2371_ROUTE = (
    "TRADING-2372_Dynamic_Strategy_Research_Only_Observation_"
    "Log_Schema_And_Report_Plan"
)


def test_dynamic_strategy_research_only_observation_log_schema_plan_builder(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "schema_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_observation_log_schema_plan(
        source_owner_review_decision_path=source_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_schema_plan"] is True
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["owner_decision_from_2371"] == OWNER_DECISION
    assert payload["observation_log_schema_ready"] is True
    assert payload["observation_report_plan_ready"] is True
    assert payload["schema_only"] is True
    assert payload["report_plan_only"] is True
    assert payload["periodic_daily_report_generated"] is False
    assert payload["event_log_written"] is False
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["next_route"] == NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_SCHEMA_PLAN_ONLY_NO_FRESH_MARKET_DATA"
    )

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    schema = payload["observation_log_schema"]
    sections = {item["section"]: item for item in schema["field_sections"]}
    assert set(sections) == {
        "identity",
        "candidate_context",
        "signal_context",
        "portfolio_preview",
        "cost_turnover",
        "comparison",
        "review",
        "guardrails",
    }
    assert "observation_id" in sections["identity"]["required_fields"]
    assert "owner_decision_from_2371" in sections["candidate_context"][
        "required_fields"
    ]
    assert "valid_until_window_state" in sections["signal_context"][
        "required_fields"
    ]
    assert "daily_report_generated" in sections["guardrails"]["required_fields"]
    assert schema["write_policy"]["event_append_allowed"] is False
    assert schema["write_policy"]["daily_report_allowed"] is False

    report_plan = payload["observation_report_plan"]
    assert report_plan["report_mode"] == "RESEARCH_ONLY_MANUAL_REPORT_PLAN"
    assert report_plan["sections"] == [
        "Executive summary",
        "Candidate under observation",
        "Signal / valid-until status",
        "Portfolio preview",
        "Static baseline comparison",
        "Ranking top vs robustness top comparison",
        "Cost / turnover / cooldown status",
        "Review flags",
        "Guardrail summary",
        "Explicit non-goals",
    ]
    assert report_plan["report_generation_policy"]["daily_report_generated"] is False

    evidence = payload["no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["schema_only"] is True
    assert evidence["report_plan_only"] is True
    assert evidence["event_append_attempted"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert evidence["daily_report_generated"] is False

    for key in (
        "json_path",
        "observation_log_schema_json",
        "observation_report_plan_json",
        "markdown_path",
        "observation_log_schema_markdown",
        "observation_report_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_observation_log_schema_plan_cli(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "schema_plan_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-observation-log-schema-plan",
            "--source-owner-review-decision",
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
    assert (output_root / "log_schema_plan_result.json").exists()
    assert (output_root / "observation_log_schema.json").exists()
    assert (output_root / "observation_report_plan.json").exists()

    payload = json.loads(
        (output_root / "log_schema_plan_result.json").read_text(encoding="utf-8")
    )
    assert payload["observation_log_schema_ready"] is True
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_observation_log_schema_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_research_only_observation_log_schema_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-observation-log-schema-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("log_schema_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("observation_log_schema.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_research_only_observation_log_schema_plan" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-observation-log-schema-plan"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2372_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
        "LOG_SCHEMA_AND_REPORT_PLAN"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _write_source_artifact(tmp_path: Path) -> Path:
    source_path = tmp_path / "source" / "owner_review_decision_result.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        json.dumps(_source_owner_decision(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return source_path


def _source_owner_decision() -> dict[str, object]:
    return {
        "task_id": "TRADING-2371",
        "status": SOURCE_2371_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2371_ROUTE,
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "research_only_observation_continue_allowed": True,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
    }
