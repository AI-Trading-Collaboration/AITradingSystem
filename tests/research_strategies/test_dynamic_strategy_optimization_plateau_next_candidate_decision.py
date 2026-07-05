from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    BEST_VARIANT_EXPECTED,
    DATA_QUALITY_GATE_REASON,
    NEXT_DIRECTION_DECISION,
    NEXT_ROUTE,
    OPTIMIZATION_PLATEAU_DETECTED,
    READY_STATUS,
    run_dynamic_strategy_optimization_plateau_next_candidate_decision,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2376_READY = "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY"
SOURCE_2379_READY = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
)
SOURCE_2380_READY = (
    "DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2380_OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED"
)
SOURCE_2380_ROUTE = (
    "TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_"
    "Decision"
)
BASE_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"


def test_dynamic_strategy_optimization_plateau_next_candidate_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plateau"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_optimization_plateau_next_candidate_decision(
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_targeted_retest_path=source_paths["targeted_retest"],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        source_owner_review_path=source_paths["owner_review"],
        source_observation_rejection_path=source_paths["observation_rejection"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_plateau_decision"] is True
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["base_candidate"] == BASE_CANDIDATE
    assert payload["ranking_top_reference"] == RANKING_TOP
    assert payload["best_variant_from_2379"] == BEST_VARIANT_EXPECTED
    assert payload["best_variant_decision_from_2379"] == "CONTINUE_OPTIMIZATION"
    assert payload["observation_approved_from_2380"] is False
    assert payload["primary_execution_cadence"] == "valid_until_window"

    assert payload["optimization_plateau_review_ready"] is True
    assert payload["optimization_plateau_detected"] == OPTIMIZATION_PLATEAU_DETECTED
    plateau = payload["optimization_plateau_review"]
    assert plateau["plateau_scope"] == "lower_turnover_local_optimization_line"
    assert plateau["lower_turnover_line_has_value"] is True
    assert plateau["lower_turnover_line_not_accepted_for_observation"] is True
    assert plateau["ranking_top_still_has_return_advantage"] is True
    assert {
        "TIME_SLICE_ROBUSTNESS_NOT_READY",
        "REGIME_SLICE_ROBUSTNESS_NOT_READY",
        "RETURN_GAP_VS_RANKING_TOP_REMAINS",
        "OBSERVATION_ACCEPTANCE_CRITERIA_NOT_MET",
    } <= set(plateau["primary_blockers"])

    assert payload["next_direction_decision"] == NEXT_DIRECTION_DECISION
    assert payload["recommended_default_direction"] == NEXT_DIRECTION_DECISION
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    direction = payload["next_candidate_direction"]
    assert direction["recommended_next_research_task"] == NEXT_ROUTE
    assert {
        row["option"] for row in direction["decision_options"]
    } == {
        "OPTION_A_CONTINUE_LOWER_TURNOVER_OPTIMIZATION",
        "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT",
        "OPTION_C_EXPAND_CANDIDATE_POOL",
        "OPTION_D_PAUSE_AND_IMPROVE_DATA_PIT_COVERAGE",
        "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW",
    }
    selected = [
        row for row in direction["decision_options"] if row["decision"] == "SELECT"
    ]
    assert [row["option"] for row in selected] == [NEXT_DIRECTION_DECISION]

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
        "optimization_plateau_decision_json",
        "next_candidate_direction_json",
        "markdown_path",
        "plateau_review_markdown",
        "next_candidate_direction_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()

    direction_payload = json.loads(
        Path(artifact_paths["next_candidate_direction_json"]).read_text(
            encoding="utf-8"
        )
    )
    assert direction_payload["status"] == READY_STATUS
    assert (
        direction_payload["next_candidate_direction"]["next_direction_decision"]
        == NEXT_DIRECTION_DECISION
    )
    assert direction_payload["production_effect"] == "none"
    assert direction_payload["broker_action"] == "none"


def test_dynamic_strategy_optimization_plateau_next_candidate_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plateau_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-optimization-plateau-next-candidate-decision",
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-targeted-retest",
            str(source_paths["targeted_retest"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-owner-review",
            str(source_paths["owner_review"]),
            "--source-observation-rejection",
            str(source_paths["observation_rejection"]),
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
    assert (output_root / "optimization_plateau_decision.json").exists()
    assert (output_root / "next_candidate_direction.json").exists()
    assert (
        docs_root / "dynamic_strategy_optimization_plateau_next_candidate_decision.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_plateau_review.md").exists()
    assert (docs_root / "dynamic_strategy_next_candidate_direction.md").exists()
    assert (docs_root / "dynamic_strategy_2382_route.md").exists()


def test_dynamic_strategy_optimization_plateau_next_candidate_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_optimization_plateau_next_candidate_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-optimization-plateau-next-candidate-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "optimization_plateau_decision.json" in item
        for item in entry["artifact_globs"]
    )
    assert any(
        "next_candidate_direction.json" in item for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_optimization_plateau_next_candidate_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-optimization-plateau-next-candidate-decision" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2381_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_"
        "DECISION"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_"
        "Decision.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": {"status": SOURCE_2366_READY},
        "targeted_retest": _targeted_retest(),
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "owner_review": _owner_review(),
        "observation_rejection": _observation_rejection(),
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
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": [
            {
                "candidate_id": RANKING_TOP,
                "cost_adjusted_return": 0.214462,
                "decision": "OWNER_REVIEW_REQUIRED",
                "rank": 1,
            },
            {
                "candidate_id": BASE_CANDIDATE,
                "cost_adjusted_return": 0.195375,
                "decision": "CONTINUE_RESEARCH",
                "rank": 2,
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": [
            {
                "candidate_id": BASE_CANDIDATE,
                "realistic_cost_adjusted_return": 0.194762,
                "robust_rank": 1,
            },
            {
                "candidate_id": RANKING_TOP,
                "realistic_cost_adjusted_return": 0.213859,
                "robust_rank": 2,
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _targeted_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "primary_candidate": BASE_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "primary_execution_cadence": "valid_until_window",
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "base_candidate": BASE_CANDIDATE,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_after_retest": BEST_VARIANT_EXPECTED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "primary_execution_cadence": "valid_until_window",
        "decision_update": {
            "best_variant_after_retest": BEST_VARIANT_EXPECTED,
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
        "best_variant_after_retest": BEST_VARIANT_EXPECTED,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "ranking": [
            {
                "decision": "CONTINUE_OPTIMIZATION",
                "rank": 1,
                "variant_id": BEST_VARIANT_EXPECTED,
            }
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "as_of": "2026-07-05",
        "base_candidate": BASE_CANDIDATE,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_from_2379": BEST_VARIANT_EXPECTED,
        "best_variant_decision_from_2379": "CONTINUE_OPTIMIZATION",
        "owner_decision": SOURCE_2380_OWNER_DECISION,
        "recommended_next_research_task": SOURCE_2380_ROUTE,
        "research_only_observation_approved": False,
        "optimization_plateau_review_required": True,
        "primary_execution_cadence": "valid_until_window",
        "production_effect": "none",
        "broker_action": "none",
        **_safety_fields(),
    }


def _observation_rejection() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "research_only_observation_approved": False,
        "optimization_plateau_review_required": True,
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
