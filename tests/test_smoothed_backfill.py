from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_backfill_generates_state_history_and_summary(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)

    result = system_target.run_smoothed_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "smoothed_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )

    summary = result["smoothed_backfill_summary"]
    methods = {row["method"] for row in summary["methods"]}
    assert "smooth_weights_3d_limited_adjustment" in methods
    assert "smooth_weights_5d_limited_adjustment" in methods
    assert result["smoothed_method_states"]
    assert result["smoothed_trade_ledger"]
    assert summary["data_quality"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["broker_action_taken"] is False

    validation = system_target.validate_smoothed_backfill_artifact(
        backfill_id=result["smoothed_backfill_id"],
        output_dir=tmp_path / "smoothed_backfill",
    )
    assert validation["status"] == "PASS"
