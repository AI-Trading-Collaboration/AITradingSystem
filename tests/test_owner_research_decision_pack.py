from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_owner_research_decision_pack_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_owner_research_decision_pack_lists_manual_options(tmp_path) -> None:
    fixture = run_owner_research_decision_pack_fixture(tmp_path)
    owner_pack = fixture["owner_pack"]

    assert owner_pack["manifest"]["status"] == "PASS"
    assert owner_pack["owner_decision_options"]["recommended_decision"] in {
        "continue_search",
        "implement_top_candidate",
        "defer_for_forward_data",
        "reject_all_candidates",
        "run_expanded_search",
    }
    assert owner_pack["manifest"]["broker_action_allowed"] is False
    assert owner_pack["manifest"]["production_effect"] == "none"

    validation = weight_search.validate_owner_research_decision_pack_artifact(
        owner_pack_id=owner_pack["owner_pack_id"],
        output_dir=tmp_path / "owner_research_decision_pack",
    )
    assert validation["status"] == "PASS"
