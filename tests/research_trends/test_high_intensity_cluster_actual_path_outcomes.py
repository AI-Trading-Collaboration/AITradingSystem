from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_cluster_actual_path_outcome_matrix,
    build_high_intensity_cluster_weighting_policy,
    build_high_intensity_outcome_classification_policy,
)


def test_cluster_actual_path_outcomes_use_cluster_start_once() -> None:
    matrix = pd.DataFrame(
        {"QQQ": [100 + idx for idx in range(40)]},
        index=pd.bdate_range("2023-01-02", periods=40),
    )
    cluster = {
        "event_cluster_id": "cl1",
        "primary_event_id": "evt1",
        "cluster_start_date": "2023-01-02",
        "cluster_end_date": "2023-01-04",
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "cluster_active_days": 3,
        "trigger_day_count": 3,
    }

    rows = build_high_intensity_cluster_actual_path_outcome_matrix(
        cluster_registry=[cluster],
        price_matrix=matrix,
        outcome_as_of_date=date(2023, 3, 1),
        classification_policy=build_high_intensity_outcome_classification_policy(),
    )

    assert len(rows) == 4
    assert rows[0]["primary_event_id"] == "evt1"
    assert rows[0]["cluster_start_date"] == "2023-01-02"
    assert rows[0]["cluster_outcome_id"].startswith("hicapo_")
    assert rows[0]["cluster_forward_return"] == 0.01
    assert build_high_intensity_cluster_weighting_policy()["primary_analysis_level"] == "cluster"
