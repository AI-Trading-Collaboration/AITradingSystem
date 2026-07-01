from __future__ import annotations

from ai_trading_system.breadth_participation_feasibility_audit import (
    build_current_constituents_proxy_risk_matrix,
)


def test_breadth_proxy_risk_matrix_is_generated() -> None:
    rows = build_current_constituents_proxy_risk_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert {row["target_etf"] for row in rows} == {"QQQ", "SPY", "SMH"}


def test_breadth_proxy_risk_matrix_blocks_promotion() -> None:
    rows = build_current_constituents_proxy_risk_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["acceptable_for_promotion"] is False for row in rows)
    assert all(row["promotion_allowed"] is False for row in rows)


def test_breadth_proxy_risk_matrix_records_bias_fields() -> None:
    rows = build_current_constituents_proxy_risk_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["lookahead_bias_risk"] for row in rows)
    assert all(row["survivorship_bias_risk"] for row in rows)


def test_breadth_proxy_risk_matrix_is_diagnostics_only() -> None:
    rows = build_current_constituents_proxy_risk_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["acceptable_for_candidate_generator_poc"] is True for row in rows)
    assert all(row["acceptable_for_actual_path_validation"] is False for row in rows)
    assert all("Diagnostics-only" in row["notes"] for row in rows)
