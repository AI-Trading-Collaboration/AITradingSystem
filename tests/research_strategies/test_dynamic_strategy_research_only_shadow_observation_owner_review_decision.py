from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    NEXT_ROUTE,
    OWNER_DECISION,
    READY_STATUS,
    run_dynamic_strategy_research_only_shadow_observation_owner_review_decision,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2370_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
    "NO_SIDE_EFFECT_VALIDATION_READY"
)
SOURCE_2370_ROUTE = (
    "TRADING-2371_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Owner_Review_Decision"
)


def test_dynamic_strategy_research_only_shadow_observation_owner_review_builder(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_decision"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_shadow_observation_owner_review_decision(
        source_replay_validation_path=source_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == OWNER_DECISION
    assert payload["research_only_observation_continue_allowed"] is True
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["observation_decision_from_2370"] == "OWNER_REVIEW_REQUIRED"
    assert payload["owner_review_required_from_2370"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["next_route"] == NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
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
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    record = payload["owner_review_decision_record"]
    assert record["owner_review_decision_recorded"] is True
    assert record["owner_decision"] == OWNER_DECISION
    assert record["research_only_observation_continue_allowed"] is True
    assert "paper_shadow" in record["non_approved_paths"]
    assert "broker" in record["non_approved_paths"]

    evidence = payload["no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["event_append_approved"] is False
    assert evidence["outcome_binding_approved"] is False
    assert evidence["paper_shadow_approved"] is False
    assert evidence["paper_trade_created"] is False
    assert evidence["shadow_position_created"] is False
    assert evidence["daily_report_generated"] is False
    assert evidence["broker_action_attempted"] is False

    for key in (
        "json_path",
        "owner_review_decision_record_json",
        "no_side_effect_evidence_json",
        "markdown_path",
        "owner_review_decision_record_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_shadow_observation_owner_review_cli(
    tmp_path: Path,
) -> None:
    source_path = _write_source_artifact(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-shadow-observation-owner-review-decision",
            "--source-replay-validation",
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
    assert (output_root / "owner_review_decision_result.json").exists()
    assert (output_root / "owner_review_decision_record.json").exists()
    assert (output_root / "no_side_effect_evidence.json").exists()

    payload = json.loads(
        (output_root / "owner_review_decision_result.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["owner_decision"] == OWNER_DECISION
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_shadow_observation_owner_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[
        "dynamic_strategy_research_only_shadow_observation_owner_review_decision"
    ]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-shadow-observation-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "owner_review_decision_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any(
        "owner_review_decision_record.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_research_only_shadow_observation_owner_review_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-shadow-observation-owner-review-decision"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2371_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
        "OWNER_REVIEW_DECISION"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert "TRADING-2371" in Path(
        "docs/requirements/TRADING-2371_to_2374_Research_Only_Observation_Closure_Pack.md"
    ).read_text(encoding="utf-8")


def _write_source_artifact(tmp_path: Path) -> Path:
    source_path = tmp_path / "source" / "replay_validation_result.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        json.dumps(_source_replay_validation(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return source_path


def _source_replay_validation() -> dict[str, object]:
    return {
        "task_id": "TRADING-2370",
        "status": SOURCE_2370_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2370_ROUTE,
        "stable_semantic_replay_passed": True,
        "no_side_effect_evidence_ready": True,
        "observation_decision": "OWNER_REVIEW_REQUIRED",
        "owner_review_required": True,
        "research_only_shadow_observation_allowed": True,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "execution_cadence": "valid_until_window",
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
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
