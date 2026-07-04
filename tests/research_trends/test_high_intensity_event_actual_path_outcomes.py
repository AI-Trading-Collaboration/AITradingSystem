from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_event_actual_path_outcome_matrix,
    build_high_intensity_outcome_classification_policy,
)


def _price_matrix() -> pd.DataFrame:
    idx = pd.bdate_range("2023-01-02", periods=30)
    return pd.DataFrame({"QQQ": [100, 102, 101, 105, 106, 110, *range(111, 135)]}, index=idx)


def _event() -> dict[str, object]:
    return {
        "event_id": "evt1",
        "event_cluster_id": "cl1",
        "event_date": "2023-01-02",
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
    }


def _pending(horizon: str, due_date: str) -> dict[str, object]:
    return {
        "pending_outcome_id": f"pending_{horizon}",
        "event_id": "evt1",
        "event_cluster_id": "cl1",
        "event_date": "2023-01-02",
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "horizon": horizon,
        "outcome_due_date": due_date,
    }


def test_event_actual_path_outcomes_compute_forward_metrics() -> None:
    rows = build_high_intensity_event_actual_path_outcome_matrix(
        event_log=[_event()],
        pending_registry=[
            _pending("1d", "2023-01-03"),
            _pending("5d", "2023-01-09"),
            _pending("10d", "2023-01-16"),
            _pending("20d", "2023-01-30"),
        ],
        outcome_schedule=[],
        price_matrix=_price_matrix(),
        outcome_as_of_date=date(2023, 2, 28),
        classification_policy=build_high_intensity_outcome_classification_policy(),
    )

    one_day = next(row for row in rows if row["horizon"] == "1d")
    five_day = next(row for row in rows if row["horizon"] == "5d")
    assert one_day["forward_return"] == 0.02
    assert five_day["forward_return"] == 0.1
    assert five_day["forward_min_return"] == 0.0
    assert five_day["forward_max_drawdown"] == 0.0
    assert five_day["realized_volatility"] > 0
    assert {row["horizon"] for row in rows} == {"1d", "5d", "10d", "20d"}
    assert all(row["outcome_binding_status"] == "OUTCOME_BOUND" for row in rows)


def test_event_actual_path_outcomes_mark_not_due_and_missing_data() -> None:
    rows = build_high_intensity_event_actual_path_outcome_matrix(
        event_log=[_event()],
        pending_registry=[
            _pending("20d", "2023-01-30"),
            {**_pending("1d", "2023-01-03"), "target_asset": "MISSING"},
        ],
        outcome_schedule=[],
        price_matrix=_price_matrix(),
        outcome_as_of_date=date(2023, 1, 10),
        classification_policy=build_high_intensity_outcome_classification_policy(),
    )

    assert rows[0]["outcome_binding_status"] == "OUTCOME_NOT_DUE"
    assert rows[0]["forward_return"] is None
    assert rows[1]["outcome_binding_status"] == "OUTCOME_BLOCKED_MARKET_DATA"
