from __future__ import annotations

from dynamic_v3_system_target_helpers import run_regime_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_regime_review_keeps_defensive_status_auditable(tmp_path) -> None:
    fixture = run_regime_review_fixture(tmp_path)
    regime = fixture["regime"]
    summary = regime["regime_method_summary"]
    metrics = regime["method_regime_metrics"]

    assert regime["manifest"]["status"] == "PASS"
    assert regime["manifest"]["market_regime"] == "ai_after_chatgpt"
    assert summary["defensive_limited_adjustment_status"] in {
        "PASS",
        "MIXED",
        "FAIL",
        "INSUFFICIENT_DATA",
    }
    assert {row["regime"] for row in summary["regimes"]} == {
        "ai_trend",
        "tech_drawdown",
        "semiconductor_pullback",
        "risk_off",
        "sideways_choppy",
        "strong_recovery",
    }
    assert any(row["target_method"] == "defensive_limited_adjustment" for row in metrics)
    assert all(row["broker_action_allowed"] is False for row in metrics)

    validation = system_target.validate_paper_shadow_regime_review_artifact(
        regime_review_id=regime["regime_review_id"],
        output_dir=tmp_path / "paper_shadow_regime_review",
    )
    assert validation["status"] == "PASS"
