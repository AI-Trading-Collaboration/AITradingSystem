from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_regime_validation_reports_sideways_and_recovery(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)

    result = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
    )

    sideways = result["sideways_validation_summary"]
    recovery = result["recovery_lag_validation_summary"]
    assert sideways["regime"] == "sideways_choppy"
    assert recovery["regime"] == "strong_recovery"
    assert {row["method"] for row in sideways["methods"]} == set(
        system_target.SMOOTHED_METHOD_TO_VARIANT
    )
    primary_sideways = next(
        row
        for row in sideways["methods"]
        if row["method"] == "smooth_weights_3d_limited_adjustment"
    )
    primary_recovery = next(
        row
        for row in recovery["methods"]
        if row["method"] == "smooth_weights_3d_limited_adjustment"
    )
    assert primary_sideways["sideways_status"] in {
        "IMPROVED",
        "WORSE",
        "MIXED",
        "INSUFFICIENT_DATA",
    }
    assert primary_recovery["lag_status"] in {"LOW", "MEDIUM", "HIGH", "INSUFFICIENT_DATA"}
    assert primary_recovery["missed_upside"] >= 0

    validation = system_target.validate_smoothed_regime_validation_artifact(
        regime_validation_id=result["regime_validation_id"],
        output_dir=tmp_path / "smoothed_regime_validation",
    )
    assert validation["status"] == "PASS"
