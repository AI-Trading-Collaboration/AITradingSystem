from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_event_actual_path_outcome_matrix,
    build_high_intensity_outcome_classification_policy,
)


def test_outcome_classification_detects_stress_and_downside_capture() -> None:
    matrix = pd.DataFrame(
        {"QQQ": [100, 96, 94, 93, 92, 91, 90]},
        index=pd.bdate_range("2023-01-02", periods=7),
    )
    rows = build_high_intensity_event_actual_path_outcome_matrix(
        event_log=[],
        pending_registry=[
            {
                "pending_outcome_id": "p5",
                "event_id": "evt1",
                "event_cluster_id": "cl1",
                "event_date": "2023-01-02",
                "target_asset": "QQQ",
                "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
                "horizon": "5d",
                "outcome_due_date": "2023-01-09",
            }
        ],
        outcome_schedule=[],
        price_matrix=matrix,
        outcome_as_of_date=date(2023, 1, 20),
        classification_policy=build_high_intensity_outcome_classification_policy(),
    )

    assert rows[0]["stress_detected"] is True
    assert rows[0]["downside_capture_candidate"] is True
    assert rows[0]["false_warning_candidate"] is False


def test_outcome_classification_detects_rebound_false_warning_and_missed_upside() -> None:
    matrix = pd.DataFrame(
        {"QQQ": [100, 102, 104, 105, 106, 108, 110]},
        index=pd.bdate_range("2023-01-02", periods=7),
    )
    rows = build_high_intensity_event_actual_path_outcome_matrix(
        event_log=[],
        pending_registry=[
            {
                "pending_outcome_id": "p5",
                "event_id": "evt1",
                "event_cluster_id": "cl1",
                "event_date": "2023-01-02",
                "target_asset": "QQQ",
                "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
                "horizon": "5d",
                "outcome_due_date": "2023-01-09",
            }
        ],
        outcome_schedule=[],
        price_matrix=matrix,
        outcome_as_of_date=date(2023, 1, 20),
        classification_policy=build_high_intensity_outcome_classification_policy(),
    )

    assert rows[0]["rebound_detected"] is True
    assert rows[0]["false_warning_candidate"] is True
    assert rows[0]["missed_upside_candidate"] is True
    assert rows[0]["downside_capture_candidate"] is False
