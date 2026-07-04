from __future__ import annotations

from high_intensity_event_logger_fixtures import sample_selected_rule
from test_high_intensity_observe_event_log import _trigger_day

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    build_high_intensity_observe_event_log,
)


def test_event_clustering_marks_new_events_and_continuations() -> None:
    clusters, _event_log, trigger_rows = build_high_intensity_observe_event_log(
        trigger_day_rows=[
            _trigger_day("2023-01-03"),
            _trigger_day("2023-01-04"),
            _trigger_day("2023-01-07"),
            _trigger_day("2023-01-11"),
        ],
        selected_rule=sample_selected_rule(),
    )

    assert len(clusters) == 2
    assert clusters[0]["trigger_day_count"] == 3
    assert clusters[0]["cluster_active_days"] == 5
    assert clusters[0]["consecutive_trigger_days"] == 2
    first_cluster_rows = [
        row for row in trigger_rows if row["event_cluster_id"] == clusters[0]["event_cluster_id"]
    ]
    assert sum(row["is_new_event"] is True for row in first_cluster_rows) == 1
    assert sum(row["is_existing_cluster_continuation"] is True for row in first_cluster_rows) == 2
