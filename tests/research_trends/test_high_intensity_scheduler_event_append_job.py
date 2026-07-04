from __future__ import annotations

from copy import deepcopy

from high_intensity_scheduler_dry_run_fixtures import sample_detection_rows

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_event_append_job_dry_run_result,
)


def test_scheduler_event_append_uses_append_only_mode() -> None:
    rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    assert rows[0]["append_status"] == "WOULD_APPEND_NEW_EVENT"
    assert rows[0]["append_mode"] == "append_only"
    assert rows[0]["original_event_log_mutation_allowed"] is False


def test_scheduler_event_append_deduplicates_existing_trigger_day() -> None:
    rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[
            {
                "date": "2026-06-30",
                "target_asset": "QQQ",
                "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            }
        ],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    assert rows[0]["append_status"] == "NO_APPEND_DUPLICATE"
    assert rows[0]["would_append_event"] is False


def test_scheduler_historical_replay_can_have_zero_would_append_events() -> None:
    rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[
            {
                "date": "2026-06-30",
                "target_asset": "QQQ",
                "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            }
        ],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    assert sum(1 for row in rows if row["would_append_event"]) == 0


def test_scheduler_event_append_extends_open_cluster() -> None:
    detection = deepcopy(sample_detection_rows())
    detection[0]["date"] = "2026-07-01"

    rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=detection,
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[
            {
                "event_cluster_id": "hicl_open",
                "cluster_status": "OPEN",
                "cluster_start_date": "2026-06-30",
                "cluster_end_date": "2026-06-30",
                "target_asset": "QQQ",
                "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
                "primary_event_id": "hievt_open",
                "trigger_day_count": 1,
                "consecutive_trigger_days": 1,
            }
        ],
    )

    assert rows[0]["append_status"] == "WOULD_EXTEND_EXISTING_CLUSTER"
    assert rows[0]["would_append_event"] is True
    assert rows[0]["event_cluster_id"] == "hicl_open"


def test_scheduler_event_append_does_not_mutate_prior_event_log() -> None:
    prior_events = [
        {
            "event_date": "2026-06-29",
            "target_asset": "QQQ",
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "event_id": "existing",
        }
    ]
    before = deepcopy(prior_events)

    build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=prior_events,
        prior_cluster_rows=[],
    )

    assert prior_events == before
