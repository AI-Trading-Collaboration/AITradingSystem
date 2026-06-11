from __future__ import annotations

from pathlib import Path

from manual_portfolio_guardrail_helpers import (
    consensus_candidate_weights,
    write_manual_snapshot_artifact,
    write_shadow_shortlist,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_execution_guardrails_check,
    run_portfolio_exposure_validation,
    run_position_drift_analysis,
    validate_execution_guardrails_artifact,
)


def test_execution_guardrails_cap_oversized_deltas(tmp_path: Path) -> None:
    drift, exposure = _drift_and_exposure(tmp_path, consensus_candidate_weights())

    guardrail = run_execution_guardrails_check(
        drift_id=drift["drift_id"],
        exposure_id=exposure["exposure_id"],
        drift_dir=tmp_path / "position_drift",
        exposure_dir=tmp_path / "portfolio_exposure",
        output_dir=tmp_path / "execution_guardrails",
    )
    validation = validate_execution_guardrails_artifact(
        guardrail_id=guardrail["guardrail_id"],
        output_dir=tmp_path / "execution_guardrails",
    )

    assert guardrail["guardrail_summary"]["capped_count"] > 0
    assert guardrail["guardrail_summary"]["total_capped_adjustment"] <= 0.10
    assert guardrail["guardrail_summary"]["recommended_action"] == (
        "paper_adjustment_review_only"
    )
    assert validation["status"] == "PASS"


def test_execution_guardrails_block_risk_increase_when_required(tmp_path: Path) -> None:
    weights = [
        {"QQQ": 0.10, "SMH": 0.0, "SPY": 0.70, "SOXX": 0.0, "TLT": 0.0, "CASH": 0.20},
        {"QQQ": 0.0, "SMH": 0.0, "SPY": 0.0, "SOXX": 0.0, "TLT": 0.80, "CASH": 0.20},
    ]
    drift, exposure = _drift_and_exposure(tmp_path, weights)

    guardrail = run_execution_guardrails_check(
        drift_id=drift["drift_id"],
        exposure_id=exposure["exposure_id"],
        drift_dir=tmp_path / "position_drift",
        exposure_dir=tmp_path / "portfolio_exposure",
        output_dir=tmp_path / "execution_guardrails",
    )
    blocked = [
        row for row in guardrail["proposed_adjustment_checks"] if row["blocked"] is True
    ]

    assert drift["consensus_drift_summary"]["candidate_agreement_status"] == (
        "HIGH_DISAGREEMENT"
    )
    assert any(row["symbol"] == "SPY" for row in blocked)
    assert guardrail["guardrail_summary"]["blocked_count"] >= 1
    assert guardrail["manifest"]["order_ticket_generation_allowed"] is False


def _drift_and_exposure(
    tmp_path: Path,
    candidate_weights: list[dict[str, float]],
) -> tuple[dict[str, object], dict[str, object]]:
    snapshot = write_manual_snapshot_artifact(tmp_path)
    exposure = run_portfolio_exposure_validation(
        snapshot_id=snapshot["snapshot_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        output_dir=tmp_path / "portfolio_exposure",
    )
    shadow = write_shadow_shortlist(tmp_path, candidate_weights)
    drift = run_position_drift_analysis(
        snapshot_id=snapshot["snapshot_id"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_drift",
    )
    return drift, exposure
