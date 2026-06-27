from __future__ import annotations

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.defensive_overlay_gate import (
    defensive_overlay_candidate_can_enter_broker_universe,
    evaluate_defensive_overlay_candidate,
    load_defensive_overlay_config,
)


def test_defensive_overlay_gate_does_not_enable_dynamic_promotion() -> None:
    config = load_defensive_overlay_config()
    evaluation = evaluate_defensive_overlay_candidate(
        _candidate_row(),
        _overlay_metrics(),
        config,
        evidence_source="actual_path",
    )

    assert evaluation["overlay_classification"] == (
        "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE"
    )
    assert evaluation["overlay_gate_passed"] is False
    assert evaluation["promotion_allowed"] is False
    assert evaluation["paper_shadow_allowed"] is False
    assert evaluation["production_allowed"] is False
    assert evaluation["broker_action"] == "none"
    assert "walk_forward_split_evidence_pending" in evaluation["overlay_blockers"]


def test_defensive_overlay_candidate_cannot_enter_broker_universe() -> None:
    config = load_defensive_overlay_config()
    evaluation = evaluate_defensive_overlay_candidate(
        _candidate_row(),
        _overlay_metrics(),
        config,
        evidence_source="actual_path",
    )

    assert defensive_overlay_candidate_can_enter_broker_universe(evaluation) is False
    assert evaluation["broker_universe_eligible"] is False


def test_target_path_metrics_cannot_pass_overlay_gate() -> None:
    config = load_defensive_overlay_config()
    evaluation = evaluate_defensive_overlay_candidate(
        _candidate_row(),
        _overlay_metrics(),
        config,
        evidence_source="target_path",
    )

    assert evaluation["overlay_gate_passed"] is False
    assert evaluation["overlay_classification"] == "TARGET_PATH_REJECTED"
    assert "target_path_metrics_not_allowed_for_overlay_gate" in evaluation["overlay_blockers"]


def test_tqqq_overlay_candidate_requires_research_only_status() -> None:
    config = load_defensive_overlay_config()
    row = _candidate_row(
        candidate_id="expanded_state_highest_return_under_max_dd_cap",
        stress_risk_too_high=True,
    )
    metrics = _overlay_metrics(tqqq_max_weight=0.30, qqq_equivalent_exposure=1.06)

    evaluation = evaluate_defensive_overlay_candidate(
        row,
        metrics,
        config,
        evidence_source="actual_path",
    )

    assert evaluation["overlay_classification"] == "TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY"
    assert evaluation["allowed_use"] == "tqqq_research_diagnostic_only"
    assert evaluation["overlay_gate_passed"] is False
    assert evaluation["production_allowed"] is False


def test_full_allocation_failure_can_only_reclassify_to_overlay_watch() -> None:
    config = load_defensive_overlay_config()
    row = _candidate_row(verdict="STATIC_FRONTIER_DOMINATES")
    evaluation = evaluate_defensive_overlay_candidate(
        row,
        _overlay_metrics(),
        config,
        evidence_source="actual_path",
    )

    assert evaluation["overlay_classification"] == (
        "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE"
    )
    assert evaluation["full_allocation_promotion_allowed"] is False
    assert evaluation["watchlist_allowed"] is True


def test_defensive_overlay_cli_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["research", "strategies", "defensive-overlay", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "full-pack" in result.output


def _candidate_row(
    *,
    candidate_id: str = "limited_adjustment",
    verdict: str = "STATIC_FRONTIER_DOMINATES",
    stress_risk_too_high: bool = False,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "strategy_id": candidate_id,
        "candidate_family": "limited_adjustment_baseline",
        "verdict": verdict,
        "same_risk_baseline": "simplex_qqq0600_sgov0400_tqqq0000",
        "walk_forward_failed": True,
        "stress_risk_too_high": stress_risk_too_high,
        "net_of_cost_failed": True,
    }


def _overlay_metrics(
    *,
    tqqq_max_weight: float = 0.0,
    qqq_equivalent_exposure: float = 0.60,
) -> dict[str, object]:
    return {
        "actual_position_path_available": True,
        "drawdown_improvement": 0.025,
        "annual_return_edge": -0.005,
        "net_annual_return_edge": -0.005,
        "calmar_edge": 0.20,
        "sharpe_edge": 0.03,
        "tqqq_max_weight": tqqq_max_weight,
        "qqq_equivalent_exposure": qqq_equivalent_exposure,
    }
