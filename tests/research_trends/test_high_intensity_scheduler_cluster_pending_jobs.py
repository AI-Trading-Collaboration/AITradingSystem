from __future__ import annotations

from copy import deepcopy

from high_intensity_scheduler_dry_run_fixtures import sample_detection_rows

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_cluster_update_job_dry_run_result,
    build_high_intensity_scheduler_event_append_job_dry_run_result,
    build_high_intensity_scheduler_monthly_concentration_job_dry_run_result,
    build_high_intensity_scheduler_pending_outcome_job_dry_run_result,
)


def test_scheduler_cluster_update_creates_new_cluster_fixture() -> None:
    append_rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    cluster_rows = build_high_intensity_scheduler_cluster_update_job_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[],
    )

    assert cluster_rows[0]["cluster_update_action"] == "CREATE_NEW_CLUSTER"
    assert cluster_rows[0]["cluster_active_days_after_update"] == 1


def test_scheduler_cluster_update_extends_existing_cluster_fixture() -> None:
    detection = deepcopy(sample_detection_rows())
    detection[0]["date"] = "2026-07-01"
    prior_cluster = {
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
    append_rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=detection,
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[prior_cluster],
    )

    cluster_rows = build_high_intensity_scheduler_cluster_update_job_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[prior_cluster],
    )

    assert cluster_rows[0]["cluster_update_action"] == "EXTEND_OPEN_CLUSTER"
    assert cluster_rows[0]["trigger_day_count_after_update"] == 2


def test_scheduler_pending_outcome_creates_all_required_horizons() -> None:
    append_rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=sample_detection_rows(),
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )

    pending_rows = build_high_intensity_scheduler_pending_outcome_job_dry_run_result(
        append_rows=append_rows
    )

    assert {row["horizon"] for row in pending_rows} == {"1d", "5d", "10d", "20d"}
    assert all(row["outcome_status_on_create"] == "OUTCOME_PENDING" for row in pending_rows)
    assert all(row["outcome_binding_allowed_in_2345"] is False for row in pending_rows)
    assert all("actual_path_outcome" not in row for row in pending_rows)


def test_scheduler_monthly_concentration_warning_is_preserved() -> None:
    detection_rows = sample_detection_rows()
    append_rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=detection_rows,
        prior_trigger_day_rows=[],
        prior_event_rows=[],
        prior_cluster_rows=[],
    )
    cluster_rows = build_high_intensity_scheduler_cluster_update_job_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=[],
    )

    result = build_high_intensity_scheduler_monthly_concentration_job_dry_run_result(
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_update_rows=cluster_rows,
        prior_event_rows=[],
        prior_cluster_rows=[],
        inputs={
            "runtime_plan": {
                "monthly_concentration_monitoring_contract": {
                    "guardrails": {
                        "max_monthly_event_count": 3,
                        "max_monthly_cluster_count": 3,
                    },
                    "inherited_warning": "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL",
                }
            },
            "continue_decision": {"monthly_plan": {}},
        },
    )

    assert result["monthly_monitoring_status"] == "PASS_WITH_WARNINGS"
    assert result["inherited_warning"] == "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    assert "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL" in result["monitoring_warnings"]
