from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_alternative_method_review_proposes_risk_capped_and_regime_gated(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    backfill_id = fixture["backfill"]["backfill_id"]
    consistency = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "paper_shadow_regime_review",
        stability_dir=tmp_path / "paper_shadow_stability",
        output_dir=tmp_path / "limited_consistency",
        generated_at=datetime(2026, 1, 7, 7, tzinfo=UTC),
    )
    instability = system_target.run_limited_instability_diagnosis(
        backfill_id=backfill_id,
        consistency_id=consistency["consistency_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        consistency_dir=tmp_path / "limited_consistency",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        output_dir=tmp_path / "limited_instability",
        generated_at=datetime(2026, 1, 7, 10, tzinfo=UTC),
    )
    risk = system_target.run_limited_risk_attribution(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_risk_attribution",
        generated_at=datetime(2026, 1, 7, 11, tzinfo=UTC),
    )

    review = system_target.run_alternative_method_review(
        backfill_id=backfill_id,
        risk_attribution_id=risk["risk_attribution_id"],
        instability_id=instability["instability_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_attribution_dir=tmp_path / "limited_risk_attribution",
        instability_dir=tmp_path / "limited_instability",
        output_dir=tmp_path / "alternative_method_review",
        generated_at=datetime(2026, 1, 7, 12, tzinfo=UTC),
    )

    candidates = review["alternative_method_candidates"]["candidates"]
    methods = {row["method"] for row in candidates}
    scorecard = review["alternative_method_scorecard"]

    assert "risk_capped_limited_adjustment" in methods
    assert "regime_gated_limited_adjustment" in methods
    assert all(row["auto_apply"] is False for row in candidates)
    assert scorecard["recommended_alternative"] in {
        "risk_capped_limited_adjustment",
        "regime_gated_limited_adjustment",
    }
    assert any(
        row["recommendation"] == "IMPLEMENT_AS_RESEARCH_CANDIDATE"
        for row in scorecard["methods"]
    )
    conceptual = [
        row for row in scorecard["methods"] if row["implementation_status"] == "NOT_IMPLEMENTED"
    ]
    assert conceptual
    assert all(row["metrics"]["total_return"] is None for row in conceptual)
    assert all(row["return_expectation"] == "UNKNOWN" for row in conceptual)
    assert review["manifest"]["input_snapshot_schema"] == (
        "alternative_method_review_input_snapshot.v2"
    )

    validation = system_target.validate_alternative_method_review_artifact(
        alt_review_id=review["alt_review_id"],
        output_dir=tmp_path / "alternative_method_review",
    )
    assert validation["status"] == "PASS"
