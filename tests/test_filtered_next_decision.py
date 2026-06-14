from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_next_decision_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_next_decision_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_filtered_next_decision_fixture(tmp_path)
    next_decision = fixture["filtered_next_decision"]
    validation = readiness.validate_filtered_next_decision_artifact(
        decision_id=next_decision["decision_id"],
        output_dir=tmp_path / "filtered_next_decision",
    )
    assert validation["status"] == "PASS"
    decision = next_decision["filtered_next_decision"]
    assert decision["decision"] in {
        "FORMALIZE_RESEARCH_METHOD",
        "CONTINUE_TESTING",
        "DEFER_FOR_FORWARD_CONFIRMATION",
    }
    assert decision["broker_action_allowed"] is False
    assert next_decision["next_task_plan"]["next_tasks"]
    assert_research_safe(next_decision["manifest"])
    assert "Filtered Next Decision" in next_decision["reader_brief_section"]
