from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_next_formal_or_search_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_next_formal_or_search_plan_records_manual_next_step(tmp_path) -> None:
    fixture = run_next_formal_or_search_plan_fixture(tmp_path)
    next_plan = fixture["next_plan"]
    decision = next_plan["next_plan_decision"]

    assert next_plan["manifest"]["status"] == "PASS"
    assert decision["decision"] in {
        "FORMAL_METHOD_PLAN",
        "KEEP_TESTING_PLAN",
        "CONTINUE_SEARCH_PLAN",
        "NO_CANDIDATE_PLAN",
    }
    assert decision["broker_action_allowed"] is False
    assert "owner_review_required: true" in next_plan["owner_next_action_checklist"].lower()

    validation = weight_search.validate_next_formal_or_search_plan_artifact(
        plan_id=next_plan["plan_id"],
        output_dir=tmp_path / "next_formal_or_search_plan",
    )
    assert validation["status"] == "PASS"
