from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_method_promotion_gate_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_weight_method_promotion_gate_stays_research_only(tmp_path) -> None:
    fixture = run_weight_method_promotion_gate_fixture(tmp_path)
    gate = fixture["promotion_gate"]
    decisions = gate["promotion_gate_decision"]["decisions"]

    assert gate["manifest"]["status"] == "PASS"
    assert decisions
    assert gate["manifest"]["promoted_candidate_count"] <= 3
    assert gate["manifest"]["auto_apply"] is False
    assert gate["manifest"]["production_effect"] == "none"

    validation = weight_search.validate_weight_method_promotion_gate_artifact(
        promotion_gate_id=gate["promotion_gate_id"],
        output_dir=tmp_path / "weight_method_promotion_gate",
    )
    assert validation["status"] == "PASS"
