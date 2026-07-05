from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DATA_QUALITY_GATE_REASON as SOURCE_2385_DATA_QUALITY_GATE_REASON,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DATA_QUALITY_GATE_REASON,
    NEXT_ROUTE,
    READY_STATUS,
    RECOMMENDED_POLICY_ACTION,
    REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
    run_dynamic_strategy_observation_gate_threshold_calibration_review,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2384_READY = (
    "DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2385_READY = (
    "DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_"
    "PLAN_READY"
)
SOURCE_2386_READY = (
    "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY"
)
SOURCE_2385_ROUTE = (
    "TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening"
)
SOURCE_2386_ROUTE = (
    "TRADING-2387_Dynamic_Strategy_Expanded_Candidate_Owner_Review_And_Next_"
    "Research_Decision"
)
SOURCE_2384_OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED"
)
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
GUARDED_TURNOVER = "equal_risk_growth_tilt_guarded_turnover_v1"
COOLDOWN_BALANCED = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
TURNOVER_BUDGETED = "dynamic_turnover_budgeted_growth_tilt_v1"
VALID_UNTIL_STRICT = "dynamic_valid_until_expiry_strict_v1"
PRIMARY_CADENCE = "valid_until_window"


def test_dynamic_strategy_observation_gate_threshold_calibration_review_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "gate_review"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_observation_gate_threshold_calibration_review(
        source_expanded_candidate_retest_path=source_paths["expanded_candidate_retest"],
        source_expanded_candidate_ranking_path=source_paths[
            "expanded_candidate_ranking"
        ],
        source_signal_family_screening_path=source_paths["signal_family_screening"],
        source_decision_update_path=source_paths["decision_update"],
        source_candidate_pool_plan_path=source_paths["candidate_pool_plan"],
        source_owner_review_path=source_paths["owner_review"],
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
    assert payload["source_ready_for_gate_calibration_review"] is True
    assert payload["source_validation_errors"] == []
    assert payload["current_best_candidate"] == RANKING_TOP
    assert payload["current_best_candidate_decision"] == "CONTINUE_OPTIMIZATION"
    assert payload["observation_ready_candidate_found_in_2386"] is False
    assert payload["gate_calibration_review_ready"] is True
    assert payload["reference_candidate_policy_review_ready"] is True
    assert payload["time_slice_threshold_review_ready"] is True
    assert payload["regime_slice_threshold_review_ready"] is True
    assert payload["drawdown_materiality_review_ready"] is True
    assert payload["research_only_vs_paper_shadow_gate_review_ready"] is True
    assert payload["candidate_reclassification_preview_ready"] is True
    assert payload["recommended_policy_action"] == RECOMMENDED_POLICY_ACTION
    assert (
        payload["reference_candidate_policy_recommendation"]
        == REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert payload["research_only_gate_may_be_too_strict"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["expanded_candidate_retest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["policy_update_applied"] is False
    assert payload["rules_mutated"] is False
    assert payload["observation_approved"] is False

    best_preview = payload["candidate_reclassification_preview"][RANKING_TOP]
    assert best_preview["current_decision"] == "CONTINUE_OPTIMIZATION"
    assert best_preview["preview_decision_under_calibrated_gate"] == (
        "OWNER_REVIEW_REQUIRED"
    )
    assert best_preview["auto_accept_allowed"] is False
    assert best_preview["owner_review_allowed"] is True

    turnover_preview = payload["candidate_reclassification_preview"][TURNOVER_BUDGETED]
    valid_until_preview = payload["candidate_reclassification_preview"][
        VALID_UNTIL_STRICT
    ]
    assert turnover_preview["preview_decision_under_calibrated_gate"] == (
        "CONTINUE_OPTIMIZATION"
    )
    assert turnover_preview["component_attribution_needed"] is True
    assert valid_until_preview["preview_decision_under_calibrated_gate"] == (
        "CONTINUE_OPTIMIZATION"
    )
    assert valid_until_preview["component_attribution_needed"] is True

    drawdown_review = payload["drawdown_materiality_review"]
    assert drawdown_review["return_per_drawdown_penalty"] == 0.48887
    assert drawdown_review["drawdown_gap_materiality_tier"] == (
        "owner_review_required"
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

    for key in (
        "json_path",
        "gate_policy_review_json",
        "candidate_reclassification_preview_json",
        "recommended_gate_policy_update_json",
        "markdown_path",
        "gate_policy_review_markdown",
        "candidate_reclassification_preview_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_observation_gate_threshold_calibration_review_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "gate_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-observation-gate-threshold-calibration-review",
            "--source-expanded-candidate-retest",
            str(source_paths["expanded_candidate_retest"]),
            "--source-expanded-candidate-ranking",
            str(source_paths["expanded_candidate_ranking"]),
            "--source-signal-family-screening",
            str(source_paths["signal_family_screening"]),
            "--source-decision-update",
            str(source_paths["decision_update"]),
            "--source-candidate-pool-plan",
            str(source_paths["candidate_pool_plan"]),
            "--source-owner-review",
            str(source_paths["owner_review"]),
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
    assert (output_root / "gate_calibration_review_result.json").exists()
    assert (output_root / "gate_policy_review.json").exists()
    assert (output_root / "candidate_reclassification_preview.json").exists()
    assert (output_root / "recommended_gate_policy_update.json").exists()


def test_dynamic_strategy_observation_gate_threshold_calibration_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_observation_gate_threshold_calibration_review"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-observation-gate-threshold-calibration-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "gate_calibration_review_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any(
        "recommended_gate_policy_update.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_observation_gate_threshold_calibration_review" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-observation-gate-threshold-calibration-review" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2387_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "expanded_candidate_retest": _expanded_candidate_retest(),
        "expanded_candidate_ranking": _expanded_candidate_ranking(),
        "signal_family_screening": _signal_family_screening(),
        "decision_update": _decision_update(),
        "candidate_pool_plan": _candidate_pool_plan(),
        "owner_review": _owner_review(),
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _expanded_candidate_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2386_READY,
        "recommended_next_research_task": SOURCE_2386_ROUTE,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
        "reference_candidate_count": 5,
        "new_candidates_tested_count": 12,
        "signal_families_tested_count": 6,
        "best_candidate_after_expanded_screening": RANKING_TOP,
        "best_candidate_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "observation_ready_candidate_found": False,
        "expanded_candidate_ranking": _ranking_rows(),
        "signal_family_screening": _family_rows(),
        "decision_update": _decision_update()["decision_update"],
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _expanded_candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2386_READY,
        "expanded_candidate_ranking": _ranking_rows(),
        "production_effect": "none",
        "broker_action": "none",
    }


def _signal_family_screening() -> dict[str, object]:
    return {
        "status": SOURCE_2386_READY,
        "signal_family_screening": _family_rows(),
        "production_effect": "none",
        "broker_action": "none",
    }


def _decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2386_READY,
        "decision_update": {
            "schema_version": "dynamic_strategy_expanded_candidate_decision_update.v1",
            "decision_update_ready": True,
            "best_candidate_after_expanded_screening": RANKING_TOP,
            "best_candidate_decision": "CONTINUE_OPTIMIZATION",
            "best_signal_family": "reference_ranking_top",
            "candidate_ready_for_research_only_observation": False,
            "owner_review_required": False,
            "recommended_next_research_task": SOURCE_2386_ROUTE,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "scheduler_enabled": False,
            "top_candidate_ranking_row": _ranking_rows()[0],
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_pool_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2385_READY,
        "recommended_next_research_task": SOURCE_2385_ROUTE,
        "data_quality_gate_reason": SOURCE_2385_DATA_QUALITY_GATE_REASON,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "reference_candidates": [
            "static_baseline",
            RANKING_TOP,
            "dynamic_regime_overlay_v0_4_lower_turnover",
            COOLDOWN_BALANCED,
            GUARDED_TURNOVER,
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2384_READY,
        "owner_decision": SOURCE_2384_OWNER_DECISION,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "candidate_ranking": [
            {"candidate_id": RANKING_TOP, "rank": 1},
            {"candidate_id": COOLDOWN_BALANCED, "rank": 2},
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "sensitivity_matrix": [
            {"candidate_id": RANKING_TOP, "scenario_id": "base"},
            {"candidate_id": COOLDOWN_BALANCED, "scenario_id": "base"},
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": {"decision_update_ready": True},
        "production_effect": "none",
        "broker_action": "none",
    }


def _ranking_rows() -> list[dict[str, object]]:
    return [
        _ranking_row(
            RANKING_TOP,
            rank=1,
            candidate_type="reference_candidate",
            signal_family="reference_ranking_top",
            static_gap=0.021302,
            lower_gap=0.019097,
            guarded_gap=0.000682,
            time_slice=0.0,
            regime_slice=0.0,
            return_retained=1.0,
            drawdown_gap=0.043574,
            drawdown_ok=False,
        ),
        _ranking_row(
            GUARDED_TURNOVER,
            rank=2,
            candidate_type="reference_candidate",
            signal_family="reference_guarded_ranking_top",
            static_gap=0.02062,
            lower_gap=0.018415,
            guarded_gap=0.0,
            time_slice=0.0,
            regime_slice=0.0,
            return_retained=0.967984,
            drawdown_gap=0.036251,
            drawdown_ok=False,
        ),
        _ranking_row(
            TURNOVER_BUDGETED,
            rank=3,
            candidate_type="new_candidate",
            signal_family="turnover_budget_family",
            static_gap=0.006941,
            lower_gap=0.004736,
            guarded_gap=-0.013679,
            time_slice=0.428571,
            regime_slice=0.0,
            return_retained=0.325838,
            drawdown_gap=-0.000389,
            drawdown_ok=True,
        ),
        _ranking_row(
            VALID_UNTIL_STRICT,
            rank=4,
            candidate_type="new_candidate",
            signal_family="signal_age_valid_until_family",
            static_gap=0.007195,
            lower_gap=0.00499,
            guarded_gap=-0.013425,
            time_slice=0.428571,
            regime_slice=0.0,
            return_retained=0.337762,
            drawdown_gap=-0.005479,
            drawdown_ok=True,
        ),
        _ranking_row(
            COOLDOWN_BALANCED,
            rank=5,
            candidate_type="reference_candidate",
            signal_family="reference_lower_turnover",
            static_gap=0.002205,
            lower_gap=0.0,
            guarded_gap=-0.018415,
            time_slice=0.428571,
            regime_slice=0.0,
            return_retained=0.103,
            drawdown_gap=-0.006,
            drawdown_ok=True,
        ),
    ]


def _ranking_row(
    candidate_id: str,
    *,
    rank: int,
    candidate_type: str,
    signal_family: str,
    static_gap: float,
    lower_gap: float,
    guarded_gap: float,
    time_slice: float,
    regime_slice: float,
    return_retained: float,
    drawdown_gap: float,
    drawdown_ok: bool,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "rank": rank,
        "candidate_type": candidate_type,
        "signal_family": signal_family,
        "decision": "CONTINUE_OPTIMIZATION",
        "dynamic_vs_static_gap": static_gap,
        "cost_adjusted_dynamic_vs_static_gap": static_gap,
        "candidate_vs_lower_turnover_gap": lower_gap,
        "candidate_vs_guarded_ranking_top_gap": guarded_gap,
        "candidate_vs_ranking_top_gap": 0.0 if candidate_id == RANKING_TOP else -0.01,
        "time_slice_pass_rate": time_slice,
        "regime_slice_pass_rate": regime_slice,
        "return_advantage_retained": return_retained,
        "turnover_budget_passed": True,
        "realistic_cost_passed": True,
        "conservative_cost_passed": True,
        "harsh_cost_passed": True,
        "drawdown_gap_vs_static": drawdown_gap,
        "valid_until_window_preserved": True,
        "no_stale_signal_carry_forward": True,
        "decision_reasons": [
            f"realistic_gap={static_gap}",
            f"time_slice_pass_rate={time_slice}",
            f"regime_slice_pass_rate={regime_slice}",
            f"drawdown_not_materially_worse={drawdown_ok}",
            "slice_stability_not_yet_enough",
        ],
    }


def _family_rows() -> list[dict[str, object]]:
    return [
        {
            "signal_family": "signal_age_valid_until_family",
            "family_best_candidate": VALID_UNTIL_STRICT,
            "family_best_candidate_decision": "CONTINUE_OPTIMIZATION",
            "family_average_score": 0.523172,
            "family_candidate_count": 2,
            "family_time_slice_pass_rate": 0.214285,
            "family_regime_slice_pass_rate": 0.0,
            "family_failure_reason": "regime_slice_stability_failure",
            "owner_review_candidate_count": 0,
        },
        {
            "signal_family": "turnover_budget_family",
            "family_best_candidate": TURNOVER_BUDGETED,
            "family_best_candidate_decision": "CONTINUE_OPTIMIZATION",
            "family_average_score": 0.383216,
            "family_candidate_count": 2,
            "family_time_slice_pass_rate": 0.214285,
            "family_regime_slice_pass_rate": 0.0,
            "family_failure_reason": "regime_slice_stability_failure",
            "owner_review_candidate_count": 0,
        },
    ]
