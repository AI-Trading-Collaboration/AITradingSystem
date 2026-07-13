from __future__ import annotations

from pathlib import Path

from dynamic_v3_defensive_evidence_helpers import run_weekly_defensive_evidence_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_forward_pressure import (
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


def test_weekly_defensive_validation_rejects_ledger_and_reader_drift(tmp_path):
    fixture = run_weekly_defensive_evidence_fixture(tmp_path)
    weekly = fixture["weekly_defensive"]
    ledger = fixture["pressure_sample_ledger"]
    with Path(ledger["ledger_dir"], "pressure_sample_ledger_report.md").open(
        "a", encoding="utf-8"
    ) as handle:
        handle.write("\nunauthorized drift\n")
    with Path(weekly["weekly_defensive_dir"], "reader_brief_section.md").open(
        "a", encoding="utf-8"
    ) as handle:
        handle.write("\nunauthorized drift\n")

    validation = validate_weekly_defensive_evidence_artifact(
        weekly_defensive_id=weekly["weekly_defensive_id"],
        output_dir=fixture["weekly_defensive_evidence_dir"],
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
