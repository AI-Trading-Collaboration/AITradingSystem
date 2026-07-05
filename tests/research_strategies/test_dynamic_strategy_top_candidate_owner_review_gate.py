from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    DECISION_OWNER_REVIEW,
    NEXT_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_top_candidate_owner_review_gate,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ROBUSTNESS_TOP = "dynamic_regime_overlay_v0_4_lower_turnover"
SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2365_NEXT_ROUTE = (
    "TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis"
)
SOURCE_2366_NEXT_ROUTE = (
    "TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate"
)


def test_dynamic_strategy_top_candidate_owner_review_gate_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_gate"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_top_candidate_owner_review_gate(
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_cadence_matrix_path=source_paths["cadence_matrix"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_matrix_path=source_paths["sensitivity_matrix"],
        source_decision_update_path=source_paths["decision_update"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_review_gate"] is True
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["ranking_top_decision_from_2365"] == DECISION_OWNER_REVIEW
    assert payload["robustness_top_from_2366"] == ROBUSTNESS_TOP
    assert payload["ranking_robustness_divergence_detected"] is True
    assert payload["owner_review_required"] is True
    assert payload["candidate_review_comparison_ready"] is True
    assert payload["shadow_research_gate_decision_ready"] is True
    assert payload["recommended_gate_candidate"] == ROBUSTNESS_TOP
    assert payload["recommended_gate_decision"] == DECISION_OWNER_REVIEW
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["research_only_shadow_observation_allowed"] is True
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_attempted"] is False
    assert payload["paper_trade_created"] is False
    assert payload["shadow_position_created"] is False
    assert payload["event_append_enabled"] is False
    assert payload["event_append_attempted"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["outcome_binding_attempted"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["broker_action"] == "none"
    assert payload["daily_report_generated"] is False

    comparison = {
        item["candidate_id"]: item for item in payload["candidate_review_comparison"]
    }
    assert {RANKING_TOP, ROBUSTNESS_TOP, "static_baseline"} <= set(comparison)
    assert comparison[RANKING_TOP]["ranking_rank_from_2365"] == 1.0
    assert comparison[ROBUSTNESS_TOP]["robustness_rank"] == 1.0
    assert comparison[ROBUSTNESS_TOP]["turnover_acceptable_after_2366"] is False
    assert comparison["static_baseline"]["decision_after_2366"] == (
        "STATIC_BASELINE_REFERENCE"
    )
    for field in (
        "total_return",
        "cost_adjusted_return",
        "dynamic_vs_static_gap",
        "max_drawdown",
        "turnover",
        "transaction_cost_drag",
        "slippage_drag",
        "cooldown_block_count",
        "cooldown_fragility",
        "survives_realistic_cost",
        "survives_conservative_cost",
        "constraint_hit_count",
        "robustness_rank",
        "ranking_rank_from_2365",
        "decision_from_2365",
        "decision_after_2366",
    ):
        assert field in comparison[ROBUSTNESS_TOP]

    gate = payload["shadow_research_gate_decision"]
    assert gate["recommended_gate_candidate"] == ROBUSTNESS_TOP
    assert gate["owner_review_required"] is True
    assert gate["research_only_shadow_observation_allowed"] is True
    assert gate["paper_shadow_enabled"] is False
    assert gate["broker_action_enabled"] is False
    assert gate["next_route"] == NEXT_ROUTE

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "candidate_owner_review_comparison_json",
        "shadow_research_gate_decision_json",
        "markdown_path",
        "candidate_owner_review_comparison_markdown",
        "shadow_research_gate_decision_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_top_candidate_owner_review_gate_cli(tmp_path: Path) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_gate_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-top-candidate-owner-review-gate",
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-cadence-matrix",
            str(source_paths["cadence_matrix"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-matrix",
            str(source_paths["sensitivity_matrix"]),
            "--source-decision-update",
            str(source_paths["decision_update"]),
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
    assert (output_root / "owner_review_gate_result.json").exists()
    assert (output_root / "candidate_owner_review_comparison.json").exists()
    assert (output_root / "shadow_research_gate_decision.json").exists()


def test_dynamic_strategy_top_candidate_owner_review_gate_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_top_candidate_owner_review_gate"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-top-candidate-owner-review-gate"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "owner_review_gate_result.json" in item for item in entry["artifact_globs"]
    )
    assert any(
        "shadow_research_gate_decision.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_top_candidate_owner_review_gate" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-top-candidate-owner-review-gate" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2367_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    ranking = _candidate_ranking()
    cadence_matrix = _cadence_matrix()
    sensitivity_matrix = _sensitivity_matrix()
    decision_update = _decision_update()
    event_retest = {
        "status": SOURCE_2365_READY,
        "as_of": "2026-07-05",
        "primary_execution_cadence": "valid_until_window",
        "next_route": SOURCE_2365_NEXT_ROUTE,
        "candidate_ranking": ranking,
        "cadence_comparison_matrix": cadence_matrix,
        "data_quality": {"status": "PASS_WITH_WARNINGS", "error_count": 0},
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
    }
    candidate_ranking = {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": ranking,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    cadence = {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "cadence_comparison_matrix": cadence_matrix,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    sensitivity_result = {
        "status": SOURCE_2366_READY,
        "as_of": "2026-07-05",
        "primary_execution_cadence": "valid_until_window",
        "next_route": SOURCE_2366_NEXT_ROUTE,
        "summary": {
            "candidate_count": 3,
            "top_candidate_from_2365": RANKING_TOP,
            "top_candidate_after_sensitivity": ROBUSTNESS_TOP,
            "top_candidate_decision_after_sensitivity": DECISION_OWNER_REVIEW,
        },
        "summary_findings": {
            "ranking_changed_after_sensitivity": "YES",
            "top_candidate_turnover_acceptable": "NO",
        },
        "robustness_ranking": decision_update["robustness_ranking"],
        "decision_update": decision_update,
        "sensitivity_matrix": sensitivity_matrix,
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    sensitivity = {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "sensitivity_matrix": sensitivity_matrix,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    decision = {
        "status": SOURCE_2366_READY,
        "decision_update": decision_update,
        "summary_findings": sensitivity_result["summary_findings"],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    paths = {
        "event_retest": source_root / "event_driven_retest_result.json",
        "candidate_ranking": source_root / "candidate_ranking.json",
        "cadence_matrix": source_root / "cadence_comparison_matrix.json",
        "sensitivity_result": source_root / "sensitivity_result.json",
        "sensitivity_matrix": source_root / "sensitivity_matrix.json",
        "decision_update": source_root / "decision_update.json",
    }
    documents = {
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "cadence_matrix": cadence,
        "sensitivity_result": sensitivity_result,
        "sensitivity_matrix": sensitivity,
        "decision_update": decision,
    }
    for key, path in paths.items():
        path.write_text(json.dumps(documents[key], ensure_ascii=False, indent=2))
    return paths


def _candidate_ranking() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": RANKING_TOP,
            "rank": 1,
            "decision": DECISION_OWNER_REVIEW,
            "cost_adjusted_return": 0.214462,
            "dynamic_vs_static_gap": 0.021905,
            "max_drawdown": -0.183495,
            "turnover": 1.964574,
            "rebalance_count": 65,
            "cooldown_block_count": 0,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "false_risk_off_count": 15,
            "missed_upside_count": 174,
        },
        {
            "candidate_id": ROBUSTNESS_TOP,
            "rank": 2,
            "decision": DECISION_OWNER_REVIEW,
            "cost_adjusted_return": 0.195171,
            "dynamic_vs_static_gap": 0.002614,
            "max_drawdown": -0.12271,
            "turnover": 2.04,
            "rebalance_count": 19,
            "cooldown_block_count": 0,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "false_risk_off_count": 4,
            "missed_upside_count": 100,
        },
        {
            "candidate_id": "limited_adjustment",
            "rank": 3,
            "decision": "REJECT_FOR_NOW",
            "cost_adjusted_return": 0.186255,
            "dynamic_vs_static_gap": -0.006302,
            "max_drawdown": -0.116361,
            "turnover": 2.4,
            "rebalance_count": 17,
            "cooldown_block_count": 0,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "false_risk_off_count": 3,
            "missed_upside_count": 90,
        },
    ]


def _cadence_matrix() -> list[dict[str, object]]:
    return [
        {
            "scenario_id": "static_baseline",
            "strategy_id": "qqq_60_sgov_40",
            "candidate_id": None,
            "total_return": 0.87298,
            "cost_adjusted_return": 0.192557,
            "max_drawdown": -0.140068,
            "volatility": 0.121479,
            "turnover": 0.0,
            "rebalance_count": 0,
            "average_holding_days": 0.0,
            "cooldown_block_count": 0,
            "constraint_hit_count": 0,
        },
        _cadence_row(RANKING_TOP, 0.994955, 0.213859, 0.021302, -0.183642, 1.964574),
        _cadence_row(ROBUSTNESS_TOP, 0.88535, 0.194762, 0.002205, -0.122866, 2.04),
        _cadence_row("limited_adjustment", 0.837946, 0.186255, -0.006302, -0.116361, 2.4),
    ]


def _cadence_row(
    candidate_id: str,
    total_return: float,
    cost_adjusted_return: float,
    gap: float,
    max_drawdown: float,
    turnover: float,
) -> dict[str, object]:
    return {
        "scenario_id": "valid_until_window",
        "candidate_id": candidate_id,
        "strategy_id": candidate_id,
        "total_return": total_return,
        "cost_adjusted_return": cost_adjusted_return,
        "dynamic_vs_static_annualized_return_gap": gap,
        "dynamic_vs_static_drawdown_gap": max_drawdown - (-0.140068),
        "max_drawdown": max_drawdown,
        "volatility": 0.126591,
        "turnover": turnover,
        "rebalance_count": 20,
        "average_holding_days": 20.0,
        "cooldown_block_count": 0,
        "constraint_hit_count": 0,
        "false_risk_off_count": 1,
        "missed_upside_count": 10,
    }


def _sensitivity_matrix() -> list[dict[str, object]]:
    return [
        _sensitivity_row(
            RANKING_TOP, "combined_realistic", 0.213859, 0.021302, -0.183642, 1.964574
        ),
        _sensitivity_row(
            ROBUSTNESS_TOP, "combined_realistic", 0.194762, 0.002205, -0.122866, 2.04
        ),
        _sensitivity_row(
            "limited_adjustment",
            "combined_realistic",
            0.186255,
            -0.006302,
            -0.116361,
            2.4,
        ),
        _sensitivity_row(
            RANKING_TOP, "combined_harsh", 0.212521, 0.019964, -0.183968, 1.964574
        ),
        _sensitivity_row(
            ROBUSTNESS_TOP, "combined_harsh", 0.1934, 0.000843, -0.123384, 2.04
        ),
        _sensitivity_row(
            "limited_adjustment", "combined_harsh", 0.18466, -0.007897, -0.116711, 2.4
        ),
    ]


def _sensitivity_row(
    candidate_id: str,
    scenario_id: str,
    cost_adjusted_return: float,
    gap: float,
    max_drawdown: float,
    turnover: float,
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "candidate_id": candidate_id,
        "strategy_id": candidate_id,
        "is_static_baseline": False,
        "execution_cadence": "valid_until_window",
        "cost_metrics": {
            "cost_adjusted_return": cost_adjusted_return,
            "cost_adjusted_dynamic_vs_static_gap": gap,
            "transaction_cost_drag": 0.001,
            "slippage_drag": 0.001,
        },
        "performance_metrics": {
            "total_return": cost_adjusted_return * 4,
            "max_drawdown": max_drawdown,
            "volatility": 0.126591,
        },
        "turnover_metrics": {
            "turnover": turnover,
            "rebalance_count": 20,
            "average_holding_days": 20.0,
        },
        "cooldown_metrics": {"cooldown_block_count": 0},
        "robustness_metrics": {
            "constraint_hit_count": 0,
            "stale_signal_execution_count": 0,
        },
    }


def _decision_update() -> dict[str, object]:
    return {
        "schema_version": "dynamic_strategy_sensitivity_decision_update.v1",
        "source_top_candidate": RANKING_TOP,
        "source_top_candidate_decision": DECISION_OWNER_REVIEW,
        "top_candidate_after_sensitivity": ROBUSTNESS_TOP,
        "top_candidate_decision_after_sensitivity": DECISION_OWNER_REVIEW,
        "ranking_changed_after_sensitivity": True,
        "recommended_next_research_task": SOURCE_2366_NEXT_ROUTE,
        "robustness_ranking": [
            {
                "candidate_id": ROBUSTNESS_TOP,
                "robust_rank": 1,
                "source_rank_2365": 2,
                "decision_update": DECISION_OWNER_REVIEW,
                "decision_update_reason": "turnover still needs owner review",
                "realistic_cost_adjusted_return": 0.194762,
                "realistic_dynamic_vs_static_gap": 0.002205,
                "conservative_dynamic_vs_static_gap": 0.001524,
                "harsh_dynamic_vs_static_gap": 0.000843,
                "realistic_max_drawdown": -0.122866,
                "realistic_turnover": 2.04,
                "survives_realistic_cost": True,
                "survives_conservative_cost": True,
                "survives_harsh_cost": True,
                "turnover_acceptable": False,
                "cooldown_fragility": "NOT_SEVERE",
                "fragility_reason": "turnover 超出 sensitivity cap",
            },
            {
                "candidate_id": RANKING_TOP,
                "robust_rank": 2,
                "source_rank_2365": 1,
                "decision_update": DECISION_OWNER_REVIEW,
                "decision_update_reason": "drawdown and turnover need owner review",
                "realistic_cost_adjusted_return": 0.213859,
                "realistic_dynamic_vs_static_gap": 0.021302,
                "conservative_dynamic_vs_static_gap": 0.020633,
                "harsh_dynamic_vs_static_gap": 0.019964,
                "realistic_max_drawdown": -0.183642,
                "realistic_turnover": 1.964574,
                "survives_realistic_cost": True,
                "survives_conservative_cost": True,
                "survives_harsh_cost": True,
                "turnover_acceptable": False,
                "cooldown_fragility": "NOT_SEVERE",
                "fragility_reason": "turnover 超出 sensitivity cap",
            },
            {
                "candidate_id": "limited_adjustment",
                "robust_rank": 3,
                "source_rank_2365": 3,
                "decision_update": "REJECT_FOR_NOW",
                "decision_update_reason": "underperforms static after cost",
                "realistic_cost_adjusted_return": 0.186255,
                "realistic_dynamic_vs_static_gap": -0.006302,
                "conservative_dynamic_vs_static_gap": -0.0071,
                "harsh_dynamic_vs_static_gap": -0.007897,
                "realistic_max_drawdown": -0.116361,
                "realistic_turnover": 2.4,
                "survives_realistic_cost": False,
                "survives_conservative_cost": False,
                "survives_harsh_cost": False,
                "turnover_acceptable": False,
                "cooldown_fragility": "NOT_SEVERE",
                "fragility_reason": "realistic cost 后不再优于 static",
            },
        ],
    }
