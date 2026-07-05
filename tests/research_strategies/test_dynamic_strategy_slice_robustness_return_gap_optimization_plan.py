from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DATA_QUALITY_GATE_REASON,
    FORBIDDEN_OPTIMIZATION_PATHS,
    NEXT_ROUTE,
    PLANNED_VARIANTS,
    PRIMARY_CANDIDATE_ID,
    RANKING_TOP_FALLBACK,
    READY_STATUS,
    run_dynamic_strategy_slice_robustness_return_gap_optimization_plan,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2375_READY = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY"
)
SOURCE_2376_READY = "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY"
SOURCE_2377_READY = (
    "DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_"
    "OPTIMIZATION_DECISION_READY"
)
SOURCE_2377_ROUTE = (
    "TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_"
    "Optimization_Plan"
)


def test_dynamic_strategy_slice_robustness_return_gap_optimization_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "optimization_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_slice_robustness_return_gap_optimization_plan(
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_optimization_review_path=source_paths["optimization_review"],
        source_optimization_decision_update_path=source_paths[
            "optimization_decision_update"
        ],
        source_targeted_retest_path=source_paths["targeted_retest"],
        source_targeted_decision_update_path=source_paths[
            "targeted_decision_update"
        ],
        source_owner_review_decision_path=source_paths["owner_review_decision"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_optimization_plan"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["primary_candidate"] == PRIMARY_CANDIDATE_ID
    assert payload["ranking_top_reference"] == RANKING_TOP_FALLBACK
    assert payload["decision_from_2377"] == "KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION"
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["optimization_plan_ready"] is True
    assert payload["time_slice_gap_diagnosis_ready"] is True
    assert payload["regime_slice_gap_diagnosis_ready"] is True
    assert payload["return_gap_repair_plan_ready"] is True
    assert payload["variant_plan_ready"] is True
    assert payload["variant_evaluation_plan_ready"] is True
    assert payload["planned_variants"] == list(PLANNED_VARIANTS)
    assert set(FORBIDDEN_OPTIMIZATION_PATHS) <= set(
        payload["forbidden_optimization_paths"]
    )
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    assert payload["time_slice_gap_diagnosis"]
    assert payload["regime_slice_gap_diagnosis"]
    assert payload["return_gap_repair_plan"]["return_gap_components"]
    assert payload["variant_evaluation_plan"]["required_2379_tests"]

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
        "time_regime_slice_gap_diagnosis_json",
        "return_gap_repair_variant_plan_json",
        "variant_evaluation_plan_json",
        "markdown_path",
        "slice_diagnosis_markdown",
        "variant_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_slice_robustness_return_gap_optimization_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "optimization_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-slice-robustness-return-gap-optimization-plan",
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-optimization-review",
            str(source_paths["optimization_review"]),
            "--source-optimization-decision-update",
            str(source_paths["optimization_decision_update"]),
            "--source-targeted-retest",
            str(source_paths["targeted_retest"]),
            "--source-targeted-decision-update",
            str(source_paths["targeted_decision_update"]),
            "--source-owner-review-decision",
            str(source_paths["owner_review_decision"]),
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
    assert (output_root / "optimization_plan_result.json").exists()
    assert (output_root / "time_regime_slice_gap_diagnosis.json").exists()
    assert (output_root / "return_gap_repair_variant_plan.json").exists()
    assert (output_root / "variant_evaluation_plan.json").exists()
    assert (
        docs_root / "dynamic_strategy_slice_robustness_return_gap_optimization_plan.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_2379_route.md").exists()


def test_dynamic_strategy_slice_robustness_return_gap_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_slice_robustness_return_gap_optimization_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-slice-robustness-return-gap-optimization-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("optimization_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("variant_evaluation_plan.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_slice_robustness_return_gap_optimization_plan" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-slice-robustness-return-gap-optimization-plan" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2378_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_"
        "OPTIMIZATION_PLAN"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_"
        "Optimization_Plan.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "event_retest": _event_retest(),
        "candidate_ranking": _candidate_ranking_document(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update_document(),
        "optimization_review": _optimization_review(),
        "optimization_decision_update": _optimization_decision_update_document(),
        "targeted_retest": _targeted_retest(),
        "targeted_decision_update": _targeted_decision_update_document(),
        "owner_review_decision": _owner_review_decision(),
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


def _event_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _candidate_ranking_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": _robustness_rows(),
        "decision_update": _sensitivity_decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": _sensitivity_decision_update_payload(),
    }


def _optimization_review() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "primary_execution_cadence": "valid_until_window",
        "ranking_top_from_2365": RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
        "candidate_decision_update": _optimization_decision_payload(),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimization_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "candidate_decision_update": _optimization_decision_payload(),
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "as_of": "2026-07-05",
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "ranking_top_from_2365": RANKING_TOP_FALLBACK,
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "primary_execution_cadence": "valid_until_window",
        "time_slice_retest_result": _time_slice_rows(),
        "regime_slice_retest_result": _regime_slice_rows(),
        "decision_update": _targeted_decision_update_payload(),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "decision_update": _targeted_decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review_decision() -> dict[str, object]:
    return {
        "status": SOURCE_2377_READY,
        "as_of": "2026-07-05",
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "ranking_top_from_2365": RANKING_TOP_FALLBACK,
        "decision_from_2376": "CONTINUE_OPTIMIZATION",
        "owner_decision": "KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION",
        "research_only_observation_approved": False,
        "continue_optimization_approved": True,
        "primary_execution_cadence": "valid_until_window",
        "recommended_next_research_task": SOURCE_2377_ROUTE,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_rows() -> list[dict[str, object]]:
    return [
        {"candidate_id": RANKING_TOP_FALLBACK, "rank": 1},
        {"candidate_id": PRIMARY_CANDIDATE_ID, "rank": 2},
    ]


def _robustness_rows() -> list[dict[str, object]]:
    return [
        {"candidate_id": PRIMARY_CANDIDATE_ID, "robust_rank": 1},
        {"candidate_id": RANKING_TOP_FALLBACK, "robust_rank": 2},
    ]


def _time_slice_rows() -> list[dict[str, object]]:
    return [
        _slice_row("early_period", False, 0.011049, -0.069704, 0.72),
        _slice_row("middle_period", True, 0.017353, 0.029479, 0.42),
        _slice_row("recent_period", False, -0.025276, -0.028777, 0.90),
        _slice_row("post_2023_ai_cycle", True, 0.000970, -0.002215, 1.32),
        _slice_row("high_volatility_periods", False, -0.074194, -0.076763, 0.78),
        _slice_row("drawdown_recovery_periods", False, -0.032893, -0.007148, 0.42),
    ]


def _regime_slice_rows() -> list[dict[str, object]]:
    return [
        _slice_row("risk_on", False, 0.021829, -0.043513, 1.02),
        _slice_row("risk_off", False, -0.040424, 0.031362, 1.02),
        _slice_row("high_volatility", False, -0.020305, -0.038624, 1.62),
        _slice_row("low_volatility", False, 0.027161, 0.005354, 0.42),
        _slice_row("trend_confirmed", False, 0.038923, -0.062361, 0.78),
        _slice_row("trend_uncertain", False, -0.071370, 0.051926, 0.24),
        _slice_row("recovery", False, -0.032893, -0.007148, 0.42),
    ]


def _slice_row(
    scenario_id: str,
    passed: bool,
    gap_static: float,
    gap_ranking: float,
    turnover: float,
) -> dict[str, object]:
    return {
        "candidate_id": PRIMARY_CANDIDATE_ID,
        "scenario_id": scenario_id,
        "slice_passed": passed,
        "fragility_reason": "test fragility reason",
        "relative_metrics": {
            "dynamic_vs_static_gap": gap_static,
            "dynamic_vs_ranking_top_gap": gap_ranking,
        },
        "execution_metrics": {"turnover": turnover},
    }


def _sensitivity_decision_update_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "top_candidate_after_sensitivity": PRIMARY_CANDIDATE_ID,
        "robustness_ranking": _robustness_rows(),
    }


def _optimization_decision_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "ranking_top_from_2365": RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
    }


def _targeted_decision_update_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "dynamic_vs_ranking_top_gap": -0.019097,
        "time_slice_pass_rate": 0.428571,
        "regime_slice_pass_rate": 0.0,
        "ablation_support_rate": 1.0,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
