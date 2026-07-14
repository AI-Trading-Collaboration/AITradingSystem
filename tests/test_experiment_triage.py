from __future__ import annotations

from dynamic_v3_system_target_helpers import run_experiment_triage_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_experiment_triage_keeps_best_supported_variant_and_defers_no_effect_variants(
    tmp_path,
) -> None:
    fixture = run_experiment_triage_fixture(tmp_path)
    triage = fixture["triage"]
    scorecard = {row["variant_id"]: row for row in triage["variant_scorecard"]}
    summary = triage["triage_summary"]

    best = triage["variant_scorecard"][0]
    rejected = next(
        row
        for row in scorecard.values()
        if row["triage_decision"] == "REJECT" and row["hard_reject_flags"]
    )

    deferred = next(
        row for row in scorecard.values() if "transform_effect" in row["missing_score_components"]
    )

    assert best["triage_decision"] == "KEEP_FOR_MORE_TESTING"
    assert best["hard_reject_flags"] == []
    assert best["observed_metrics"]["transform_effective_rebalance_count"] > 0
    assert rejected["triage_decision"] == "REJECT"
    assert rejected["hard_reject_flags"]
    assert deferred["triage_decision"] == "DEFER"
    assert deferred["observed_metrics"]["transform_effective_rebalance_count"] == 0
    assert summary["promote_count"] == 0
    assert summary["keep_testing_count"] >= 1
    assert summary["reject_count"] >= 1
    assert summary["defer_count"] >= 1
    assert triage["promotion_candidates"] == []
    assert triage["manifest"]["broker_action_allowed"] is False
    assert triage["manifest"]["production_effect"] == "none"

    validation = system_target.validate_experiment_triage_artifact(
        triage_id=triage["triage_id"],
        output_dir=tmp_path / "experiment_triage",
    )
    assert validation["status"] == "PASS"
