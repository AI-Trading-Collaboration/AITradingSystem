from __future__ import annotations

from high_intensity_runtime_dry_run_fixtures import sample_detection_rows

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    build_high_intensity_runtime_event_append_dry_run_result,
    build_high_intensity_runtime_pending_outcome_update_dry_run_result,
)


def test_pending_outcome_update_creates_all_horizons_for_new_event() -> None:
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    pending_rows = build_high_intensity_runtime_pending_outcome_update_dry_run_result(
        append_rows=append_rows,
    )

    assert [row["horizon"] for row in pending_rows] == ["1d", "5d", "10d", "20d"]
    assert all(row["outcome_status_on_create"] == "OUTCOME_PENDING" for row in pending_rows)
    assert all(row["outcome_binding_allowed_in_2343"] is False for row in pending_rows)
    assert all("forward_return" not in row for row in pending_rows)


def test_pending_outcome_id_is_deterministic() -> None:
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    first = build_high_intensity_runtime_pending_outcome_update_dry_run_result(
        append_rows=append_rows,
    )
    second = build_high_intensity_runtime_pending_outcome_update_dry_run_result(
        append_rows=append_rows,
    )

    assert [row["pending_outcome_id"] for row in first] == [
        row["pending_outcome_id"] for row in second
    ]
