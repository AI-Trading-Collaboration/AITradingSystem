from __future__ import annotations

from copy import deepcopy

from high_intensity_runtime_dry_run_fixtures import sample_detection_rows

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    build_high_intensity_runtime_cluster_update_dry_run_result,
    build_high_intensity_runtime_event_append_dry_run_result,
)


def test_cluster_update_creates_new_cluster() -> None:
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    cluster_rows = build_high_intensity_runtime_cluster_update_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[],
    )

    assert cluster_rows[0]["cluster_update_action"] == "CREATE_NEW_CLUSTER"
    assert cluster_rows[0]["monthly_cluster_count_after_update"] == 1
    assert cluster_rows[0]["consecutive_trigger_days_after_update"] == 1


def test_cluster_update_extends_open_cluster() -> None:
    detection = deepcopy(sample_detection_rows())
    detection[0]["date"] = "2026-07-01"
    prior_cluster = {
        "event_cluster_id": "hicl_open",
        "cluster_status": "OPEN",
        "cluster_start_date": "2026-06-30",
        "cluster_end_date": "2026-06-30",
        "monthly_bucket": "2026-06",
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "primary_event_id": "hievt_open",
        "trigger_day_count": 1,
        "consecutive_trigger_days": 1,
    }
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=detection,
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[prior_cluster],
    )

    cluster_rows = build_high_intensity_runtime_cluster_update_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[prior_cluster],
    )

    assert cluster_rows[0]["cluster_update_action"] == "EXTEND_OPEN_CLUSTER"
    assert cluster_rows[0]["trigger_day_count_after_update"] == 2
    assert cluster_rows[0]["consecutive_trigger_days_after_update"] == 2


def test_non_consecutive_trigger_creates_new_cluster() -> None:
    detection = deepcopy(sample_detection_rows())
    detection[0]["date"] = "2026-07-10"
    prior_cluster = {
        "event_cluster_id": "hicl_open",
        "cluster_status": "OPEN",
        "cluster_start_date": "2026-06-30",
        "cluster_end_date": "2026-06-30",
        "monthly_bucket": "2026-06",
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "primary_event_id": "hievt_open",
    }
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=detection,
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[prior_cluster],
    )

    cluster_rows = build_high_intensity_runtime_cluster_update_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[prior_cluster],
    )

    assert append_rows[0]["append_status"] == "WOULD_APPEND_NEW_EVENT"
    assert cluster_rows[0]["cluster_update_action"] == "CREATE_NEW_CLUSTER"
