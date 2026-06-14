from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_next_research_direction_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_next_research_direction_generates_manual_task_plan(tmp_path) -> None:
    fixture = run_next_research_direction_fixture(tmp_path)
    direction = fixture["next_direction"]

    assert direction["manifest"]["status"] == "PASS"
    assert direction["next_research_direction_decision"]["decision"]
    assert direction["next_task_plan"]["tasks"]
    assert direction["manifest"]["broker_action_allowed"] is False
    assert "Next Research Direction" in direction["reader_brief_section"]

    validation = weight_search.validate_next_research_direction_artifact(
        direction_id=direction["direction_id"],
        output_dir=tmp_path / "next_research_direction",
    )
    assert validation["status"] == "PASS"
