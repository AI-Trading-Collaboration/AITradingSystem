from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_downside_capture_classification_report,
    build_high_intensity_false_warning_classification_report,
    build_high_intensity_manual_review_usefulness_proxy_report,
    build_high_intensity_missed_upside_classification_report,
)


def test_classification_reports_use_cluster_level_rates() -> None:
    event_rows = [
        {
            "event_id": "evt1",
            "target_asset": "QQQ",
            "horizon": "5d",
            "outcome_binding_status": "OUTCOME_BOUND",
            "false_warning_candidate": True,
            "missed_upside_candidate": True,
            "downside_capture_candidate": False,
            "manual_review_would_have_helped_candidate": True,
        }
    ]
    cluster_rows = [
        {
            "event_cluster_id": "cl1",
            "target_asset": "QQQ",
            "horizon": "5d",
            "cluster_outcome_binding_status": "OUTCOME_BOUND",
            "cluster_false_warning_candidate": True,
            "cluster_missed_upside_candidate": True,
            "cluster_downside_capture_candidate": False,
            "cluster_manual_review_would_have_helped_candidate": True,
            "cluster_stress_detected": False,
            "cluster_forward_return": 0.06,
            "cluster_forward_max_drawdown": 0.0,
        }
    ]

    false_warning = build_high_intensity_false_warning_classification_report(
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
    )
    missed_upside = build_high_intensity_missed_upside_classification_report(
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
    )
    downside = build_high_intensity_downside_capture_classification_report(
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
    )
    manual = build_high_intensity_manual_review_usefulness_proxy_report(
        event_log=[{"event_id": "evt1"}],
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
    )

    assert false_warning["false_warning_cluster_rate"] == 1.0
    assert missed_upside["missed_upside_cluster_rate"] == 1.0
    assert downside["downside_capture_cluster_rate"] == 0.0
    assert manual["manual_review_usefulness_proxy"] == 1.0
