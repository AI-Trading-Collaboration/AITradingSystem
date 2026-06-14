from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_formalization_readiness_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_formalization_readiness_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_filtered_formalization_readiness_fixture(tmp_path)
    formalization = fixture["filtered_formalization_readiness"]
    validation = readiness.validate_filtered_formalization_readiness_artifact(
        readiness_id=formalization["readiness_id"],
        output_dir=tmp_path / "filtered_formalization_readiness",
    )
    assert validation["status"] == "PASS"
    decision = formalization["formalization_readiness_decision"]
    assert decision["decision"] in {
        "READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION",
        "CONTINUE_TESTING",
    }
    assert decision["can_implement_research_only_method"] is True
    assert decision["can_write_official_target_weights"] is False
    assert formalization["formalization_blockers"]["blockers"]
    assert_research_safe(formalization["manifest"])
