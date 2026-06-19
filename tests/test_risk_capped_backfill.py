from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_risk_capped_backfill_generates_state_history_and_summary(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)

    result = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_capped_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )

    summary = result["risk_capped_backfill_summary"]
    assert result["risk_capped_method_states"]
    assert result["risk_capped_trade_ledger"]
    assert summary["schema_version"] == 1
    assert summary["report_type"] == "etf_dynamic_v3_risk_capped_backfill_summary"
    assert summary["status"] == "PASS"
    assert summary["method"] == "risk_capped_limited_adjustment"
    assert summary["cap_event_count"] >= 0
    assert summary["data_quality"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["broker_action_taken"] is False

    validation = system_target.validate_risk_capped_backfill_artifact(
        backfill_id=result["risk_capped_backfill_id"],
        output_dir=tmp_path / "risk_capped_backfill",
    )
    assert validation["status"] == "PASS"
