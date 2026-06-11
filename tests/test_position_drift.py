from __future__ import annotations

from pathlib import Path

from manual_portfolio_guardrail_helpers import (
    consensus_candidate_weights,
    high_disagreement_candidate_weights,
    write_manual_snapshot_artifact,
    write_shadow_shortlist,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_position_drift_analysis,
    validate_position_drift_artifact,
)


def test_position_drift_calculates_consensus_delta(tmp_path: Path) -> None:
    snapshot = write_manual_snapshot_artifact(tmp_path)
    shadow = write_shadow_shortlist(tmp_path, consensus_candidate_weights())

    drift = run_position_drift_analysis(
        snapshot_id=snapshot["snapshot_id"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_drift",
    )
    validation = validate_position_drift_artifact(
        drift_id=drift["drift_id"],
        output_dir=tmp_path / "position_drift",
    )

    summary = drift["consensus_drift_summary"]
    assert summary["candidate_agreement_status"] == "CONSENSUS"
    assert summary["drift_status"] == "HIGH"
    assert summary["consensus_deltas"]["SPY"] > 0.24
    assert drift["drift_action_candidates"]["recommended_action"] == (
        "guardrail_check_required"
    )
    assert validation["status"] == "PASS"


def test_position_drift_candidate_disagreement_blocks_adjustment(tmp_path: Path) -> None:
    snapshot = write_manual_snapshot_artifact(tmp_path)
    shadow = write_shadow_shortlist(tmp_path, high_disagreement_candidate_weights())

    drift = run_position_drift_analysis(
        snapshot_id=snapshot["snapshot_id"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "position_drift",
    )

    assert drift["consensus_drift_summary"]["candidate_agreement_status"] == (
        "HIGH_DISAGREEMENT"
    )
    assert drift["drift_action_candidates"]["recommended_action"] == "manual_review"
    assert drift["manifest"]["broker_action_allowed"] is False
