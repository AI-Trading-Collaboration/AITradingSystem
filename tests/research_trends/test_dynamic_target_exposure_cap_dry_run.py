from __future__ import annotations

import pandas as pd

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_exposure_cap_dry_run_rows,
)


def _policy() -> dict:
    return {
        "cap_policy": {
            "default_max_allowed_exposure": 1.0,
            "max_allowed_exposure_by_intensity": {"high": 0.5, "medium": 0.7},
        },
        "cooldown_policy": {
            "default_cooldown_days": 1,
            "cooldown_days_by_intensity": {"high": 1, "medium": 1},
        },
    }


def _schedule(exposure: float, risk: float, day: str = "2023-01-09") -> dict:
    return {
        "date": day,
        "target_asset": "QQQ",
        "baseline_id": "baseline",
        "target_exposure": exposure,
        "risk_asset_exposure": risk,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _prices() -> pd.DataFrame:
    return pd.DataFrame(
        {"QQQ": [100.0, 110.0, 99.0]},
        index=pd.to_datetime(["2023-01-06", "2023-01-09", "2023-01-10"]),
    )


def test_dynamic_dry_run_caps_binding_exposure() -> None:
    rows = build_dynamic_target_exposure_cap_dry_run_rows(
        policy=_policy(),
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        schedule_rows=[_schedule(0.8, 0.8)],
        price_matrix=_prices(),
        alignment_rows=[
            {"date": "2023-01-09", "target_asset": "QQQ", "simulation_eligible": True}
        ],
        date_trigger_map={
            "2023-01-09": {
                "risk_cap_triggered": True,
                "risk_cap_intensity": "high",
                "risk_cap_score": 0.9,
            }
        },
        target_assets=["QQQ"],
        data_quality_status="PASS",
    )

    assert rows[0]["simulated_cap_binding_active"] is True
    assert rows[0]["simulated_final_exposure_after_cap"] <= 0.5
    assert rows[0]["risk_cap_incremental_binding"] is True
    assert rows[0]["simulated_turnover_proxy"] == 0.3
    assert rows[0]["dynamic_no_cap_return_contribution_proxy"] == 0.08
    assert rows[0]["dynamic_capped_return_contribution_proxy"] == 0.05


def test_dynamic_dry_run_does_not_cap_without_trigger_or_cooldown() -> None:
    rows = build_dynamic_target_exposure_cap_dry_run_rows(
        policy=_policy(),
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        schedule_rows=[_schedule(0.8, 0.8)],
        price_matrix=_prices(),
        alignment_rows=[
            {"date": "2023-01-09", "target_asset": "QQQ", "simulation_eligible": True}
        ],
        date_trigger_map={},
        target_assets=["QQQ"],
        data_quality_status="PASS",
    )

    assert rows[0]["simulated_cap_binding_active"] is False
    assert rows[0]["simulated_final_exposure_after_cap"] == 0.8


def test_dynamic_dry_run_marks_already_derisked_as_redundant() -> None:
    rows = build_dynamic_target_exposure_cap_dry_run_rows(
        policy=_policy(),
        simulation_dates=[pd.Timestamp("2023-01-09").date()],
        schedule_rows=[_schedule(0.4, 0.4)],
        price_matrix=_prices(),
        alignment_rows=[
            {"date": "2023-01-09", "target_asset": "QQQ", "simulation_eligible": True}
        ],
        date_trigger_map={
            "2023-01-09": {
                "risk_cap_triggered": True,
                "risk_cap_intensity": "high",
                "risk_cap_score": 0.9,
            }
        },
        target_assets=["QQQ"],
        data_quality_status="PASS",
    )

    assert rows[0]["dynamic_strategy_already_de_risked"] is True
    assert rows[0]["risk_cap_incremental_binding"] is False
