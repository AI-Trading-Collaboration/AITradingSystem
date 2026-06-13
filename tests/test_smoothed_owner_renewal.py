from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_smoothed_owner_renewal_options_and_reader_brief(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    renewal = fixture["renewal"]

    options = renewal["owner_renewal_options"]
    assert options["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert options["previous_owner_decision"] == "continue_observation"
    assert options["current_recheck_decision"] == "WAIT_FOR_MORE_FORWARD_DATA"
    assert options["recommended_owner_action"] == "continue_observation"
    assert options["forward_progress"] == "0/10"
    assert options["sideways_progress"] == "0/5"
    assert options["recovery_lag_status"] == "INSUFFICIENT_EVENTS"
    assert options["auto_switch"] is False
    assert options["not_official_target_weights"] is True
    assert options["broker_action_allowed"] is False
    assert options["production_effect"] == "none"

    by_decision = {row["decision"]: row for row in options["owner_options"]}
    assert by_decision["continue_observation"]["recommended"] is True
    assert by_decision["promote_to_primary_research_candidate"]["recommended"] is False
    assert "Dynamic Rescue Smoothed Owner Renewal" in renewal["reader_brief_section"]
    assert "no broker / no production" in renewal["owner_renewal_checklist"]

    validation = system_target.validate_smoothed_owner_renewal_artifact(
        renewal_id=renewal["renewal_id"],
        output_dir=tmp_path / "smoothed_owner_renewal",
    )
    assert validation["status"] == "PASS"

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_forward_progress",
                "latest_artifact_path": str(
                    fixture["progress"]["progress_dir"]
                    / "smoothed_forward_progress_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_weekly_dashboard",
                "latest_artifact_path": str(
                    fixture["dashboard"]["dashboard_dir"]
                    / "smoothed_weekly_dashboard_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_event_monitor",
                "latest_artifact_path": str(
                    fixture["monitor"]["monitor_dir"] / "smoothed_event_monitor_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_switch_readiness",
                "latest_artifact_path": str(
                    fixture["recheck"]["recheck_dir"]
                    / "smoothed_switch_readiness_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_owner_renewal",
                "latest_artifact_path": str(
                    fixture["renewal"]["renewal_dir"] / "smoothed_owner_renewal_manifest.json"
                ),
            },
        ]
    }
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)
    assert summary["smoothed_forward_progress_forward_events"] == "0/10"
    assert summary["smoothed_weekly_ready_for_switch_recheck"] is False
    assert summary["smoothed_event_recovery_lag_status"] == "INSUFFICIENT_EVENTS"
    assert summary["smoothed_switch_recheck_decision"] == "WAIT_FOR_MORE_FORWARD_DATA"
    assert summary["smoothed_switch_can_execute_switch"] is False
    assert summary["smoothed_owner_renewal_recommended_action"] == "continue_observation"
    assert summary["production_effect"] == "none"
