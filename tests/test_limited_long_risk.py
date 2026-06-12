from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_limited_long_risk_reports_tradeoff_and_exposure_path(tmp_path) -> None:
    fixture = run_backfill_fixture(tmp_path)

    risk = system_target.run_limited_long_risk_review(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_long_risk",
        generated_at=datetime(2024, 3, 1, 6, tzinfo=UTC),
    )

    long_window = risk["long_window_risk_return"]
    metrics = long_window["metrics"]
    exposure = risk["exposure_path_analysis"]
    comparisons = risk["limited_vs_baseline_breakdown"]["comparisons"]

    assert risk["manifest"]["status"] == "PASS"
    assert long_window["target_method"] == "limited_adjustment"
    assert long_window["risk_return_status"] in {
        "RETURN_IMPROVES_RISK_IMPROVES",
        "RETURN_IMPROVES_RISK_WORSENS",
        "RETURN_WORSE_RISK_IMPROVES",
        "RETURN_WORSE_RISK_WORSE",
        "INSUFFICIENT_DATA",
    }
    assert {"total_return", "max_drawdown", "turnover"}.issubset(metrics)
    assert {row["baseline"] for row in comparisons} == {
        "static_baseline",
        "no_trade_baseline",
    }
    assert exposure["target_method"] == "limited_adjustment"
    assert exposure["risk_exposure_interpretation"] in {
        "higher_risk_exposure",
        "similar_risk_exposure",
        "lower_risk_exposure",
        "mixed",
    }
    assert long_window["not_official_target_weights"] is True
    assert long_window["broker_action_allowed"] is False

    validation = system_target.validate_limited_long_risk_artifact(
        risk_review_id=risk["risk_review_id"],
        output_dir=tmp_path / "limited_long_risk",
    )
    assert validation["status"] == "PASS"
