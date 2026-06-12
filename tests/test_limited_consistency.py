from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_limited_consistency_integrates_rolling_regime_and_stability(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)

    consistency = system_target.run_limited_consistency_check(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "paper_shadow_regime_review",
        stability_dir=tmp_path / "paper_shadow_stability",
        output_dir=tmp_path / "limited_consistency",
        generated_at=datetime(2024, 3, 1, 7, tzinfo=UTC),
    )

    rolling = consistency["rolling_consistency_summary"]
    regime = consistency["regime_consistency_summary"]
    stability = consistency["stability_consistency_summary"]

    assert consistency["manifest"]["status"] == "PASS"
    assert rolling["target_method"] == "limited_adjustment"
    assert rolling["rolling_consistency_status"] in {
        "STABLE",
        "MIXED",
        "UNSTABLE",
        "INSUFFICIENT_DATA",
    }
    assert regime["target_method"] == "limited_adjustment"
    assert regime["regime_consistency_status"] in {
        "BROADLY_CONSISTENT",
        "REGIME_DEPENDENT",
        "WEAK_IN_PRESSURE",
        "INSUFFICIENT_DATA",
    }
    assert {row["regime"] for row in regime["regimes"]} >= {
        "ai_trend",
        "tech_drawdown",
        "semiconductor_pullback",
    }
    assert stability["target_method"] == "limited_adjustment"
    assert stability["turnover_status"] in {"LOW", "MODERATE", "HIGH", "INSUFFICIENT_DATA"}
    assert stability["broker_action_allowed"] is False

    validation = system_target.validate_limited_consistency_artifact(
        consistency_id=consistency["consistency_id"],
        output_dir=tmp_path / "limited_consistency",
    )
    assert validation["status"] == "PASS"
