from __future__ import annotations

from dynamic_v3_system_target_helpers import run_experiment_triage_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_experiment_triage_promotes_strong_variant_and_hard_rejects_bad_drawdown(
    tmp_path,
) -> None:
    fixture = run_experiment_triage_fixture(tmp_path)
    triage = fixture["triage"]
    scorecard = {row["variant_id"]: row for row in triage["variant_scorecard"]}
    summary = triage["triage_summary"]

    promoted = scorecard["sideways_choppy_hold_previous"]
    rejected = scorecard["cash_buffer_15"]

    assert promoted["triage_decision"] == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
    assert promoted["hard_reject_flags"] == []
    assert rejected["triage_decision"] == "REJECT"
    assert "max_drawdown_materially_worse" in rejected["hard_reject_flags"]
    assert summary["promote_count"] >= 1
    assert summary["reject_count"] >= 1
    assert triage["promotion_candidates"]
    assert triage["manifest"]["broker_action_allowed"] is False
    assert triage["manifest"]["production_effect"] == "none"

    validation = system_target.validate_experiment_triage_artifact(
        triage_id=triage["triage_id"],
        output_dir=tmp_path / "experiment_triage",
    )
    assert validation["status"] == "PASS"
