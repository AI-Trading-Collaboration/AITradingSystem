from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_adaptive_branch_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_weight_adaptive_branch_emits_next_search_decision(tmp_path) -> None:
    fixture = run_weight_adaptive_branch_fixture(tmp_path)
    branch = fixture["branch"]

    assert branch["manifest"]["status"] == "PASS"
    assert branch["branch_decision"]["branch_decision"] in {
        "BLOCKED_DATA_QUALITY_FAIL",
        "RUN_PROMOTION_GATE",
        "RUN_EXPANDED_SEARCH",
    }
    assert branch["branch_decision"]["next_command"]
    assert (
        branch["branch_decision"]["search_space_id"] == fixture["search_space"]["search_space_id"]
    )
    assert branch["manifest"]["production_effect"] == "none"

    validation = weight_search.validate_weight_adaptive_branch_artifact(
        branch_id=branch["branch_id"],
        output_dir=tmp_path / "weight_adaptive_branch",
    )
    assert validation["status"] == "PASS"
