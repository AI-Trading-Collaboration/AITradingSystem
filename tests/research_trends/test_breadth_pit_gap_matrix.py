from __future__ import annotations

from ai_trading_system.breadth_participation_feasibility_audit import (
    build_historical_constituent_pit_gap_matrix,
)


def test_breadth_pit_gap_matrix_marks_strict_pit_blocked() -> None:
    rows = build_historical_constituent_pit_gap_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert rows
    assert all(row["historical_constituents_available"] is False for row in rows)
    assert all(row["strict_pit_breadth_blocked"] is True for row in rows)


def test_breadth_pit_gap_matrix_marks_pit_approximation_blocked() -> None:
    rows = build_historical_constituent_pit_gap_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["pit_approximation_possible"] is False for row in rows)


def test_breadth_pit_gap_matrix_marks_current_proxy_only() -> None:
    rows = build_historical_constituent_pit_gap_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["current_constituents_proxy_only"] is True for row in rows)
    assert {
        row["recommendation"] for row in rows
    } == {"CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY"}


def test_breadth_pit_gap_matrix_records_survivorship_and_lookahead_risk() -> None:
    rows = build_historical_constituent_pit_gap_matrix(target_etfs=["QQQ", "SPY", "SMH"])

    assert all(row["survivorship_bias_risk"] == "HIGH_SURVIVORSHIP_BIAS" for row in rows)
    assert all(row["lookahead_bias_risk"] == "HIGH_LOOKAHEAD_BIAS" for row in rows)
