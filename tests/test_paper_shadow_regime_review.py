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
    assert (
        regime["manifest"]["input_snapshot_schema"]
        == "paper_shadow_regime_review_input_snapshot.v2"
    )
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


def test_paper_shadow_regime_review_validation_rejects_label_tamper(tmp_path) -> None:
    fixture = run_regime_review_fixture(tmp_path)
    regime = fixture["regime"]
    labels_path = regime["regime_review_dir"] / "regime_date_labels.jsonl"
    labels_path.write_text(
        labels_path.read_text(encoding="utf-8").replace("ai_trend", "unreviewed_regime", 1),
        encoding="utf-8",
    )

    validation = system_target.validate_paper_shadow_regime_review_artifact(
        regime_review_id=regime["regime_review_id"],
        output_dir=tmp_path / "paper_shadow_regime_review",
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
