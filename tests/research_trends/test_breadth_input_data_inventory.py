from __future__ import annotations

from ai_trading_system.breadth_participation_feasibility_audit import (
    BIAS_RISKS,
    PIT_STATUSES,
    build_breadth_input_data_inventory,
)


def test_breadth_input_data_inventory_rows_are_generated() -> None:
    rows = build_breadth_input_data_inventory(target_etfs=["QQQ", "SPY", "SMH"])

    assert rows
    assert any(row["input_category"] == "historical_etf_constituents" for row in rows)
    assert any(row["input_category"] == "alternative_proxy_inputs" for row in rows)


def test_breadth_input_data_inventory_pit_status_is_legal() -> None:
    rows = build_breadth_input_data_inventory(target_etfs=["QQQ", "SPY", "SMH"])

    assert {row["pit_status"] for row in rows} <= PIT_STATUSES


def test_breadth_input_data_inventory_bias_risk_is_legal() -> None:
    rows = build_breadth_input_data_inventory(target_etfs=["QQQ", "SPY", "SMH"])

    assert {row["bias_risk"] for row in rows} <= BIAS_RISKS


def test_breadth_input_data_inventory_required_fields_exist() -> None:
    rows = build_breadth_input_data_inventory(target_etfs=["QQQ", "SPY", "SMH"])

    for row in rows:
        assert "manual_action_required" in row
        assert "recommended_usage" in row
        assert row["promotion_allowed"] is False
        assert row["paper_shadow_allowed"] is False
        assert row["production_allowed"] is False
        assert row["broker_action"] == "none"
