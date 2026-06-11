from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_research_note_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_defensive_research_note_artifact,
)


def test_defensive_research_note_keeps_hypothesis_research_only(tmp_path):
    fixture = run_research_note_fixture(tmp_path)
    note = fixture["defensive_research_note"]
    summary = note["defensive_hypothesis_summary"]

    assert summary["current_status"] == "RESEARCH_ONLY"
    assert summary["simulation_support"] == "PARTIAL"
    assert summary["forward_support"] == "NONE"
    assert summary["can_support_rule_approval"] is False
    assert summary["policy_change_allowed"] is False
    assert "Dynamic Rescue Defensive Hypothesis Review" in note["reader_brief_section"]

    validation = validate_defensive_research_note_artifact(
        note_id=note["note_id"],
        output_dir=fixture["defensive_research_note_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
