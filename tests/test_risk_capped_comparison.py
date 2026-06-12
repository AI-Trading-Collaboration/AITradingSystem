from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_risk_capped_comparison_outputs_metrics_regime_rolling_and_stability(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    backfill = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_capped_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )

    result = system_target.run_risk_capped_comparison(
        risk_capped_backfill_id=backfill["risk_capped_backfill_id"],
        baseline_backfill_id=backfill["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "risk_capped_comparison",
    )

    metrics = result["risk_capped_vs_limited_metrics"]
    assert metrics["comparison"]["method_a"] == "risk_capped_limited_adjustment"
    assert metrics["comparison"]["method_b"] == "limited_adjustment"
    assert "avg_semiconductor_weight_delta" in metrics["metrics"]
    assert result["risk_capped_regime_comparison"]["regimes"]
    assert result["risk_capped_rolling_comparison"]["rolling_windows_total"] > 0
    assert result["risk_capped_stability_comparison"]["stability_conclusion"] in {
        "IMPROVED",
        "WORSE",
        "MIXED",
        "INSUFFICIENT_DATA",
    }

    validation = system_target.validate_risk_capped_comparison_artifact(
        comparison_id=result["comparison_id"],
        output_dir=tmp_path / "risk_capped_comparison",
    )
    assert validation["status"] == "PASS"
