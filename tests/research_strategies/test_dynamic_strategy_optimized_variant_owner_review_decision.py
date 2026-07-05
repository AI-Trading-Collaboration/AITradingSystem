from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    BEST_VARIANT_EXPECTED,
    DATA_QUALITY_GATE_REASON,
    NEXT_ROUTE,
    OWNER_DECISION,
    READY_STATUS,
    run_dynamic_strategy_optimized_variant_owner_review_decision,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2376_READY = "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY"
SOURCE_2378_READY = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY"
)
SOURCE_2379_READY = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
)
SOURCE_2379_ROUTE = (
    "TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_"
    "Observation_Decision"
)
BASE_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"


def test_dynamic_strategy_optimized_variant_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_optimized_variant_owner_review_decision(
        source_variant_retest_path=source_paths["variant_retest"],
        source_variant_decision_update_path=source_paths["variant_decision_update"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        source_optimization_plan_path=source_paths["optimization_plan"],
        source_variant_evaluation_plan_path=source_paths["variant_evaluation_plan"],
        source_targeted_retest_path=source_paths["targeted_retest"],
        source_targeted_decision_update_path=source_paths[
            "targeted_decision_update"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["base_candidate"] == BASE_CANDIDATE
    assert payload["ranking_top_reference"] == RANKING_TOP
    assert payload["best_variant_from_2379"] == BEST_VARIANT_EXPECTED
    assert payload["best_variant_decision_from_2379"] == "CONTINUE_OPTIMIZATION"
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == OWNER_DECISION
    assert payload["research_only_observation_approved"] is False
    assert payload["continue_optimization_allowed"] is True
    assert payload["optimization_plateau_review_required"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["observation_rejection_rationale_ready"] is True
    assert payload["observation_rejection_reasons"]

    rejection = payload["observation_rejection_rationale"]
    assert rejection["research_only_observation_approved"] is False
    assert rejection["paper_shadow_approved"] is False
    assert rejection["continue_optimization_allowed"] is True
    assert rejection["optimization_plateau_review_required"] is True
    assert {
        "BEST_VARIANT_DECISION_REMAINS_CONTINUE_OPTIMIZATION",
        "RESEARCH_ONLY_OBSERVATION_ACCEPTANCE_CRITERIA_NOT_MET",
        "TIME_OR_REGIME_SLICE_ROBUSTNESS_REQUIRES_MORE_EVIDENCE",
        "RETURN_GAP_REPAIR_NOT_SUFFICIENT_FOR_OBSERVATION_APPROVAL",
    } <= set(rejection["observation_rejection_reasons"])

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "owner_review_decision_json",
        "observation_rejection_rationale_json",
        "markdown_path",
        "observation_rejection_rationale_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_optimized_variant_owner_review_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-optimized-variant-owner-review-decision",
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-variant-decision-update",
            str(source_paths["variant_decision_update"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-optimization-plan",
            str(source_paths["optimization_plan"]),
            "--source-variant-evaluation-plan",
            str(source_paths["variant_evaluation_plan"]),
            "--source-targeted-retest",
            str(source_paths["targeted_retest"]),
            "--source-targeted-decision-update",
            str(source_paths["targeted_decision_update"]),
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
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "observation_rejection_rationale.json").exists()
    assert (
        docs_root / "dynamic_strategy_optimized_variant_owner_review_decision.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_observation_rejection_rationale.md").exists()
    assert (docs_root / "dynamic_strategy_2381_route.md").exists()


def test_dynamic_strategy_optimized_variant_owner_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_optimized_variant_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-optimized-variant-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "observation_rejection_rationale.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_optimized_variant_owner_review_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-optimized-variant-owner-review-decision" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2380_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_"
        "OBSERVATION_DECISION"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_"
        "Observation_Decision.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "variant_retest": _variant_retest(),
        "variant_decision_update": _variant_decision_update(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "optimization_plan": _optimization_plan(),
        "variant_evaluation_plan": _variant_evaluation_plan(),
        "targeted_retest": _targeted_retest(),
        "targeted_decision_update": _targeted_decision_update(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = source_root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "as_of": "2026-07-05",
        "base_candidate": BASE_CANDIDATE,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_after_retest": BEST_VARIANT_EXPECTED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "primary_execution_cadence": "valid_until_window",
        "recommended_next_research_task": SOURCE_2379_ROUTE,
        "decision_update": {
            "best_variant_after_retest": BEST_VARIANT_EXPECTED,
            "best_variant_decision": "CONTINUE_OPTIMIZATION",
            "best_variant_metrics": {
                "return_gap_reduction_vs_base": 0.00807,
                "time_slice_pass_rate": 0.0,
                "regime_slice_pass_rate": 0.0,
                "survives_realistic_cost": True,
                "survives_conservative_cost": True,
                "survives_harsh_cost": False,
            },
        },
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _variant_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "schema_version": "dynamic_strategy_optimized_variant_decision_update.v1",
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": BEST_VARIANT_EXPECTED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "recommended_next_research_task": SOURCE_2379_ROUTE,
        "ranking": [
            {
                "variant_id": BEST_VARIANT_EXPECTED,
                "decision": "CONTINUE_OPTIMIZATION",
                "rank": 1,
            }
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimization_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2378_READY,
        "primary_candidate": BASE_CANDIDATE,
        "ranking_top_reference": RANKING_TOP,
        "primary_execution_cadence": "valid_until_window",
        "recommended_next_research_task": (
            "TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest"
        ),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _variant_evaluation_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2378_READY,
        "schema_version": "dynamic_strategy_variant_evaluation_plan.v1",
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "primary_candidate": BASE_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "primary_execution_cadence": "valid_until_window",
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "schema_version": "dynamic_strategy_targeted_retest_decision_update.v1",
        "production_effect": "none",
        "broker_action": "none",
    }
