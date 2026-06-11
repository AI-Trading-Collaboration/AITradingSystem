from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_weekly_defensive_evidence_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_weekly_defensive_evidence_artifact,
)


def test_weekly_defensive_evidence_reports_research_only_sample_progress(tmp_path):
    fixture = run_weekly_defensive_evidence_fixture(tmp_path)
    weekly = fixture["weekly_defensive"]
    summary = weekly["weekly_defensive_summary"]

    assert summary["new_simulation_pressure_samples"] > 0
    assert summary["total_forward_pressure_samples"] == 0
    assert summary["defensive_rule_status"] == "RESEARCH_ONLY"
    assert summary["weekly_recommendation"] == "continue_tracking"
    assert summary["policy_change_allowed"] is False
    assert "Dynamic Rescue Weekly Defensive Evidence" in weekly["reader_brief_section"]

    validation = validate_weekly_defensive_evidence_artifact(
        weekly_defensive_id=weekly["weekly_defensive_id"],
        output_dir=fixture["weekly_defensive_evidence_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
