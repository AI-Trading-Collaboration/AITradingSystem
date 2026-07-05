from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DATA_QUALITY_GATE_REASON,
    GUARDED_VARIANT_IDS,
    NEXT_ROUTE,
    PRIMARY_EXECUTION_CADENCE,
    RANKING_TOP_CANDIDATE,
    READY_STATUS,
    run_dynamic_strategy_ranking_top_guarded_turnover_retest_plan,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2379_READY = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
)
SOURCE_2380_READY = (
    "DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2381_READY = (
    "DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY"
)
SOURCE_2381_NEXT_DIRECTION = "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT"
SOURCE_2381_ROUTE = (
    "TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan"
)
LOWER_TURNOVER = "dynamic_regime_overlay_v0_4_lower_turnover"
BEST_LOWER_TURNOVER_VARIANT = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"


def test_dynamic_strategy_ranking_top_guarded_turnover_retest_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "guarded_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_ranking_top_guarded_turnover_retest_plan(
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        source_owner_review_path=source_paths["owner_review"],
        source_observation_rejection_path=source_paths["observation_rejection"],
        source_plateau_decision_path=source_paths["plateau_decision"],
        source_next_direction_path=source_paths["next_direction"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_retest_plan"] is True
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_generated"] is False
    assert payload["next_direction_from_2381"] == SOURCE_2381_NEXT_DIRECTION
    assert payload["ranking_top_candidate"] == RANKING_TOP_CANDIDATE
    assert payload["guardrail_reference_candidates"] == [
        LOWER_TURNOVER,
        BEST_LOWER_TURNOVER_VARIANT,
    ]
    assert payload["primary_execution_cadence"] == PRIMARY_EXECUTION_CADENCE

    assert payload["retest_plan_ready"] is True
    assert payload["ranking_top_fragility_diagnosis_ready"] is True
    assert payload["guarded_variant_plan_ready"] is True
    assert payload["variant_evaluation_plan_ready"] is True
    assert payload["planned_variants"] == list(GUARDED_VARIANT_IDS)
    assert RANKING_TOP_CANDIDATE in payload["all_variant_ids_for_2383"]
    assert set(GUARDED_VARIANT_IDS) <= set(payload["all_variant_ids_for_2383"])
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    fragility = payload["ranking_top_fragility_diagnosis"]
    assert fragility["ranking_top_fragility_diagnosis_ready"] is True
    assert {
        "turnover_risk",
        "drawdown_risk",
        "cooldown_fragility",
        "cost_fragility",
        "stale_signal_risk",
    } <= set(fragility["diagnosis"])

    variant_plan = payload["guarded_variant_plan"]
    assert variant_plan["guarded_variant_plan_ready"] is True
    assert variant_plan["planned_variants"] == list(GUARDED_VARIANT_IDS)
    assert {
        "valid_until_window",
        "cooldown_balancing",
        "risk_cap_preservation",
        "no_stale_signal_execution",
    } <= {row["guardrail"] for row in variant_plan["transferable_guardrails"]}

    evaluation_plan = payload["variant_evaluation_plan"]
    assert evaluation_plan["variant_evaluation_plan_ready"] is True
    assert evaluation_plan["primary_execution_cadence"] == PRIMARY_EXECUTION_CADENCE
    assert evaluation_plan["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert "valid_until_window" in evaluation_plan["comparison_cadences"]
    assert "survives_realistic_cost" in evaluation_plan["acceptance_criteria"]["must"]
    assert "require_paper_shadow" in evaluation_plan["acceptance_criteria"]["must_not"]
    assert {
        "use_monthly_rebalance_as_primary",
        "allow_stale_signal_carry_forward",
        "ignore_transaction_costs",
        "accept_variant_that_requires_scheduler_or_paper_shadow",
    } <= set(payload["forbidden_optimization_paths"])

    for key in (
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "retest_plan_result_json",
        "ranking_top_fragility_diagnosis_json",
        "guarded_variant_plan_json",
        "variant_evaluation_plan_json",
        "markdown_path",
        "ranking_top_fragility_diagnosis_markdown",
        "guarded_variant_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()

    evaluation_payload = json.loads(
        Path(artifact_paths["variant_evaluation_plan_json"]).read_text(
            encoding="utf-8"
        )
    )
    assert evaluation_payload["recommended_next_research_task"] == NEXT_ROUTE
    assert evaluation_payload["monthly_rebalance"][
        "allowed_for_primary_decision"
    ] is False


def test_dynamic_strategy_ranking_top_guarded_turnover_retest_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "guarded_plan_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-ranking-top-guarded-turnover-retest-plan",
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-owner-review",
            str(source_paths["owner_review"]),
            "--source-observation-rejection",
            str(source_paths["observation_rejection"]),
            "--source-plateau-decision",
            str(source_paths["plateau_decision"]),
            "--source-next-direction",
            str(source_paths["next_direction"]),
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
    assert (output_root / "retest_plan_result.json").exists()
    assert (output_root / "ranking_top_fragility_diagnosis.json").exists()
    assert (output_root / "guarded_variant_plan.json").exists()
    assert (output_root / "variant_evaluation_plan.json").exists()
    assert (
        docs_root / "dynamic_strategy_ranking_top_guarded_turnover_retest_plan.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_ranking_top_fragility_diagnosis.md").exists()
    assert (docs_root / "dynamic_strategy_guarded_ranking_top_variant_plan.md").exists()
    assert (docs_root / "dynamic_strategy_2383_route.md").exists()


def test_dynamic_strategy_ranking_top_guarded_turnover_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_ranking_top_guarded_turnover_retest_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-ranking-top-guarded-turnover-retest-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("retest_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("guarded_variant_plan.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_ranking_top_guarded_turnover_retest_plan" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-ranking-top-guarded-turnover-retest-plan" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2382_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": {"status": SOURCE_2366_READY},
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "owner_review": _owner_review(),
        "observation_rejection": _observation_rejection(),
        "plateau_decision": _plateau_decision(),
        "next_direction": _next_direction(),
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


def _candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "candidate_ranking": [
            {
                "candidate_id": RANKING_TOP_CANDIDATE,
                "cooldown_vs_event_driven_gap": -0.001081,
                "cost_adjusted_return": 0.214462,
                "decision": "OWNER_REVIEW_REQUIRED",
                "false_risk_off_count": 15,
                "max_drawdown": -0.183495,
                "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                "rank": 1,
                "rebalance_count": 65,
                "relies_on_high_turnover": True,
                "stale_signal_count": 0,
                "survives_cooldown_constraints": True,
                "survives_cost_adjustment": True,
                "turnover": 1.964574,
                "valid_until_vs_monthly_gap": 0.003743,
            },
            {
                "candidate_id": LOWER_TURNOVER,
                "cost_adjusted_return": 0.195375,
                "decision": "CONTINUE_RESEARCH",
                "max_drawdown": -0.122632,
                "rank": 2,
                "rebalance_count": 19,
                "turnover": 2.04,
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "robustness_ranking": [
            {
                "candidate_id": LOWER_TURNOVER,
                "realistic_cost_adjusted_return": 0.194762,
                "robust_rank": 1,
            },
            {
                "candidate_id": RANKING_TOP_CANDIDATE,
                "realistic_cost_adjusted_return": 0.213859,
                "robust_rank": 2,
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "base_candidate": LOWER_TURNOVER,
        "ranking_top_reference": RANKING_TOP_CANDIDATE,
        "best_variant_after_retest": BEST_LOWER_TURNOVER_VARIANT,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "decision_update": {
            "best_variant_after_retest": BEST_LOWER_TURNOVER_VARIANT,
            "best_variant_decision": "CONTINUE_OPTIMIZATION",
            "best_variant_metrics": {
                "annualized_return": 0.202832,
                "conservative_cost_passed": True,
                "realistic_cost_passed": True,
                "regime_slice_pass_rate": 0.0,
                "return_gap_reduction_vs_base": 0.00807,
                "time_slice_pass_rate": 0.0,
                "turnover_profile_preserved": True,
                "variant_vs_ranking_top_gap": -0.011027,
            },
        },
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": BEST_LOWER_TURNOVER_VARIANT,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "ranking": [
            {
                "decision": "CONTINUE_OPTIMIZATION",
                "rank": 1,
                "variant_id": BEST_LOWER_TURNOVER_VARIANT,
            }
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "research_only_observation_approved": False,
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _observation_rejection() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "research_only_observation_approved": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _plateau_decision() -> dict[str, object]:
    return {
        "status": SOURCE_2381_READY,
        "as_of": "2026-07-06",
        "best_variant_from_2379": BEST_LOWER_TURNOVER_VARIANT,
        "next_direction_decision": SOURCE_2381_NEXT_DIRECTION,
        "recommended_next_research_task": SOURCE_2381_ROUTE,
        "observation_approved_from_2380": False,
        "optimization_plateau_review_ready": True,
        "optimization_plateau_detected": "LOWER_TURNOVER_LOCAL_PLATEAU_DETECTED",
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "source_findings": {
            "ranking_top_still_has_return_advantage": True,
        },
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _next_direction() -> dict[str, object]:
    return {
        "status": SOURCE_2381_READY,
        "next_candidate_direction": {
            "next_direction_decision": SOURCE_2381_NEXT_DIRECTION,
            "recommended_next_research_task": SOURCE_2381_ROUTE,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _safety_fields() -> dict[str, bool]:
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
    }
