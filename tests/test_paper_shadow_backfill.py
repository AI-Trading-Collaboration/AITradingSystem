from __future__ import annotations

from dynamic_v3_system_target_helpers import BACKFILL_START, run_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_backfill_runs_with_quality_gate_and_safety(tmp_path) -> None:
    fixture = run_backfill_fixture(tmp_path)
    backfill = fixture["backfill"]
    manifest = backfill["manifest"]
    data_quality = backfill["backfill_data_quality"]

    config_validation = system_target.validate_paper_shadow_backfill_config(fixture["config_path"])
    assert config_validation["status"] == "PASS"
    assert config_validation["not_pit_safe"] is True
    assert config_validation["broker_action_allowed"] is False

    assert manifest["market_regime"] == "ai_after_chatgpt"
    assert manifest["requested_start_date"] == BACKFILL_START.isoformat()
    assert manifest["date_start"] == BACKFILL_START.isoformat()
    assert manifest["mode"] == "BACKTEST_SIMULATION"
    assert manifest["not_pit_safe"] is True
    assert manifest["not_official_target_weights"] is True
    assert manifest["broker_action_taken"] is False
    assert manifest["order_ticket_generated"] is False
    assert data_quality["data_quality"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert backfill["backfill_rebalance_calendar"]["rebalance_count"] > 40
    assert len(backfill["backfill_method_states"]) > 1000
    assert all(row["broker_action_taken"] is False for row in backfill["backfill_trade_ledger"])

    artifact_validation = system_target.validate_paper_shadow_backfill_artifact(
        backfill_id=backfill["backfill_id"],
        output_dir=tmp_path / "paper_shadow_backfill",
    )
    assert artifact_validation["status"] == "PASS"
