from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_comparison_outputs_metrics_regime_rolling_stability_and_lag(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    smoothed = system_target.run_smoothed_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "smoothed_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )
    risk_capped = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_capped_backfill",
        paper_shadow_backfill_dir=tmp_path / "risk_source_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )

    result = system_target.run_smoothed_comparison(
        smoothed_backfill_id=smoothed["smoothed_backfill_id"],
        baseline_backfill_id=smoothed["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_id=risk_capped["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_comparison",
    )

    comparisons = result["smoothed_vs_limited_metrics"]["comparisons"]
    assert any(
        row["method_a"] == "smooth_weights_3d_limited_adjustment"
        and row["method_b"] == "limited_adjustment"
        for row in comparisons
    )
    assert any(
        row["method_a"] == "smooth_weights_5d_limited_adjustment"
        and row["method_b"] == "risk_capped_limited_adjustment"
        for row in comparisons
    )
    assert result["smoothed_regime_comparison"]["regimes"]
    assert result["smoothed_rolling_comparison"]["methods"]
    assert result["smoothed_stability_comparison"]["methods"]
    assert result["smoothing_lag_cost_analysis"]["methods"]

    validation = system_target.validate_smoothed_comparison_artifact(
        comparison_id=result["comparison_id"],
        output_dir=tmp_path / "smoothed_comparison",
    )
    assert validation["status"] == "PASS"
