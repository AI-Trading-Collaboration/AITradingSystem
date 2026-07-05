from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DATA_QUALITY_GATE_REASON,
    NEXT_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_candidate_pool_expansion_plan,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2379_READY = "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
SOURCE_2383_READY = "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY"
SOURCE_2384_READY = (
    "DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2379_ROUTE = (
    "TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_2383_ROUTE = (
    "TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_2384_ROUTE = (
    "TRADING-2385_Dynamic_Strategy_Candidate_Pool_Expansion_And_Signal_Family_"
    "Diversification_Plan"
)
SOURCE_2384_OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED"
)
NEXT_DIRECTION_2384 = "OPTION_C_EXPAND_CANDIDATE_POOL_AND_SIGNAL_FAMILIES"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
LOWER_TURNOVER = "dynamic_regime_overlay_v0_4_lower_turnover"
COOLDOWN_BALANCED = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
GUARDED_TURNOVER = "equal_risk_growth_tilt_guarded_turnover_v1"
PRIMARY_CADENCE = "valid_until_window"


def test_dynamic_strategy_candidate_pool_expansion_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "candidate_pool"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_candidate_pool_expansion_plan(
        source_owner_review_path=source_paths["owner_review"],
        source_next_direction_path=source_paths["next_direction"],
        source_guarded_variant_retest_path=source_paths["guarded_variant_retest"],
        source_guarded_variant_ranking_path=source_paths["guarded_variant_ranking"],
        source_guarded_decision_update_path=source_paths[
            "guarded_decision_update"
        ],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_candidate_pool_expansion_plan"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["owner_decision_from_2384"] == SOURCE_2384_OWNER_DECISION
    assert payload["candidate_pool_expansion_recommended"] is True
    assert payload["signal_family_diversification_recommended"] is True
    assert payload["candidate_budget_ready"] is True
    assert payload["anti_overfit_guardrails_ready"] is True
    assert payload["retest_plan_2386_ready"] is True
    assert payload["primary_execution_cadence"] == PRIMARY_CADENCE
    assert payload["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    reference_candidates = set(payload["reference_candidates"])
    assert RANKING_TOP in reference_candidates
    assert LOWER_TURNOVER in reference_candidates
    assert COOLDOWN_BALANCED in reference_candidates
    assert GUARDED_TURNOVER in reference_candidates
    assert payload["signal_families"] == [
        "regime_transition_family",
        "trend_confirmation_family",
        "volatility_aware_family",
        "signal_age_valid_until_family",
        "turnover_budget_family",
        "risk_cap_interaction_family",
    ]
    assert len(payload["new_candidates_for_2386"]) == 12
    budget = payload["candidate_budget_guardrails"]
    assert budget["budget_check"]["within_total_budget"] is True
    assert budget["post_hoc_metric_cherry_picking_forbidden"] is True
    assert (
        payload["retest_plan_2386"]["required_candidates"]["reference"]
        == payload["reference_candidates"]
    )

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
        "candidate_pool_expansion_plan_json",
        "signal_family_diversification_plan_json",
        "candidate_budget_guardrails_json",
        "retest_plan_2386_json",
        "markdown_path",
        "signal_family_diversification_markdown",
        "candidate_budget_guardrails_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_candidate_pool_expansion_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "candidate_pool_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-candidate-pool-expansion-plan",
            "--source-owner-review",
            str(source_paths["owner_review"]),
            "--source-next-direction",
            str(source_paths["next_direction"]),
            "--source-guarded-variant-retest",
            str(source_paths["guarded_variant_retest"]),
            "--source-guarded-variant-ranking",
            str(source_paths["guarded_variant_ranking"]),
            "--source-guarded-decision-update",
            str(source_paths["guarded_decision_update"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--as-of",
            "2026-07-06",
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
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "candidate_pool_expansion_plan.json").exists()
    assert (output_root / "signal_family_diversification_plan.json").exists()
    assert (output_root / "candidate_budget_guardrails.json").exists()
    assert (output_root / "retest_plan_2386.json").exists()
    assert (docs_root / "dynamic_strategy_candidate_pool_expansion_plan.md").exists()
    assert (
        docs_root / "dynamic_strategy_signal_family_diversification_plan.md"
    ).exists()
    assert (
        docs_root / "dynamic_strategy_candidate_budget_and_anti_overfit_guardrails.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_2386_route.md").exists()


def test_dynamic_strategy_candidate_pool_expansion_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_candidate_pool_expansion_plan"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-candidate-pool-expansion-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "candidate_pool_expansion_plan.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("retest_plan_2386.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_candidate_pool_expansion_plan" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-candidate-pool-expansion-plan" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2385_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_"
        "DIVERSIFICATION_PLAN"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2385_Dynamic_Strategy_Candidate_Pool_Expansion_And_"
        "Signal_Family_Diversification_Plan.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "owner_review": _owner_review(),
        "next_direction": _next_direction(),
        "guarded_variant_retest": _guarded_variant_retest(),
        "guarded_variant_ranking": _guarded_variant_ranking(),
        "guarded_decision_update": _guarded_decision_update(),
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update(),
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


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2384_READY,
        "as_of": "2026-07-06",
        "owner_decision": SOURCE_2384_OWNER_DECISION,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "continue_local_optimization_allowed": False,
        "research_only_observation_approved": False,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2384_ROUTE,
        "best_variant_from_2379": COOLDOWN_BALANCED,
        "best_guarded_variant_from_2383": GUARDED_TURNOVER,
        "lower_turnover_line": {
            "base_candidate": LOWER_TURNOVER,
            "best_variant": COOLDOWN_BALANCED,
            "decision": "CONTINUE_OPTIMIZATION",
        },
        "ranking_top_guarded_line": {
            "base_candidate": RANKING_TOP,
            "best_variant": GUARDED_TURNOVER,
            "decision": "CONTINUE_OPTIMIZATION",
        },
        **_safety_fields(),
    }


def _next_direction() -> dict[str, object]:
    return {
        "status": SOURCE_2384_READY,
        "next_research_direction_decision": {
            "next_direction": NEXT_DIRECTION_2384,
            "recommended_next_research_task": SOURCE_2384_ROUTE,
            "candidate_pool_expansion_recommended": True,
            "signal_family_diversification_recommended": True,
        },
        **_safety_fields(),
    }


def _guarded_variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "as_of": "2026-07-05",
        "ranking_top_candidate": RANKING_TOP,
        "best_guarded_variant": GUARDED_TURNOVER,
        "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2383_ROUTE,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        **_safety_fields(),
    }


def _guarded_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "best_guarded_variant": GUARDED_TURNOVER,
        "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
        **_safety_fields(),
    }


def _guarded_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "decision_update": {
            "best_guarded_variant": GUARDED_TURNOVER,
            "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
            "recommended_next_research_task": SOURCE_2383_ROUTE,
        },
        **_safety_fields(),
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "as_of": "2026-07-05",
        "base_candidate": LOWER_TURNOVER,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_after_retest": COOLDOWN_BALANCED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2379_ROUTE,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        **_safety_fields(),
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": COOLDOWN_BALANCED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        **_safety_fields(),
    }


def _candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "candidate_ranking": [
            {
                "rank": 1,
                "candidate_id": RANKING_TOP,
                "primary_execution_cadence": PRIMARY_CADENCE,
            }
        ],
        **_safety_fields(),
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "sensitivity_matrix": [
            {"candidate_id": RANKING_TOP, "primary_execution_cadence": PRIMARY_CADENCE}
        ],
        **_safety_fields(),
    }


def _sensitivity_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": {
            "decision_update_ready": True,
            "combined_stress_results": [
                {
                    "candidate_id": RANKING_TOP,
                    "primary_execution_cadence": PRIMARY_CADENCE,
                }
            ],
        },
        **_safety_fields(),
    }


def _safety_fields() -> dict[str, object]:
    return {
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }
