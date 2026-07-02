from __future__ import annotations

import pandas as pd
from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_baseline_exposure_schedule,
    build_dynamic_target_risk_cap_trigger_alignment_matrix,
)


def test_risk_cap_alignment_respects_next_session_policy() -> None:
    schedule = build_dynamic_target_baseline_exposure_schedule(
        wrapper_rows=[dry_run_wrapper_row(row_date="2023-01-06")]
    )
    price_matrix = pd.DataFrame(
        {"QQQ": [100.0, 101.0]},
        index=pd.to_datetime(["2023-01-06", "2023-01-09"]),
    )

    rows = build_dynamic_target_risk_cap_trigger_alignment_matrix(
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        target_assets=["QQQ"],
        schedule_rows=schedule,
        price_matrix=price_matrix,
        date_trigger_map={
            "2023-01-09": {
                "risk_cap_triggered": True,
                "risk_cap_intensity": "high",
                "risk_cap_score": 0.9,
                "scope_active": True,
                "signal_direction": "portfolio_level_risk_cap",
            }
        },
        trigger_source_hash="hash",
        risk_cap_trigger_source_available=True,
    )

    assert rows[0]["timestamp_alignment_status"] == "NEXT_SESSION_ALIGNED_WITH_PIT_CAVEAT"
    assert rows[0]["simulation_eligible"] is True
    assert rows[0]["risk_cap_triggered"] is True


def test_risk_cap_alignment_marks_missing_market_data_ineligible() -> None:
    schedule = build_dynamic_target_baseline_exposure_schedule(
        wrapper_rows=[dry_run_wrapper_row(row_date="2023-01-06")]
    )
    price_matrix = pd.DataFrame(
        {"QQQ": [100.0, None]},
        index=pd.to_datetime(["2023-01-06", "2023-01-09"]),
    )

    rows = build_dynamic_target_risk_cap_trigger_alignment_matrix(
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        target_assets=["QQQ"],
        schedule_rows=schedule,
        price_matrix=price_matrix,
        date_trigger_map={},
        trigger_source_hash="hash",
        risk_cap_trigger_source_available=True,
    )

    assert rows[0]["simulation_eligible"] is False
    assert "missing_market_data" in rows[0]["ineligible_reason"]


def test_risk_cap_alignment_marks_missing_dynamic_target_ineligible() -> None:
    price_matrix = pd.DataFrame(
        {"QQQ": [100.0, 101.0]},
        index=pd.to_datetime(["2023-01-06", "2023-01-09"]),
    )

    rows = build_dynamic_target_risk_cap_trigger_alignment_matrix(
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        target_assets=["QQQ"],
        schedule_rows=[],
        price_matrix=price_matrix,
        date_trigger_map={},
        trigger_source_hash="hash",
        risk_cap_trigger_source_available=True,
    )

    assert rows[0]["simulation_eligible"] is False
    assert "missing_dynamic_target" in rows[0]["ineligible_reason"]
