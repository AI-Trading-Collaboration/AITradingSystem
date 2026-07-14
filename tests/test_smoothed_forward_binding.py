from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as smoothed_promotion,
)


@smoothed_promotion._with_validation_session
def test_smoothed_forward_binding_connects_targets_to_weekly_progress(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    binding = fixture["binding"]

    targets_payload = binding["bound_confirmation_targets"]
    assert targets_payload["source_confirmation_id"] == fixture["confirmation"]["confirmation_id"]
    assert targets_payload["candidate_method"] is None
    assert targets_payload["binding_status"] == "NOT_REGISTERED"
    assert targets_payload["targets"] == []

    requirements = binding["forward_progress_requirements"]
    assert requirements["requirements"] == []
    assert requirements["rule_review_ready_when"] == []
    assert "Dynamic Rescue Smoothed Forward Binding" in binding["reader_brief_section"]

    validation = system_target.validate_smoothed_forward_binding_artifact(
        binding_id=binding["binding_id"],
        output_dir=tmp_path / "smoothed_forward_binding",
    )
    assert validation["status"] == "PASS"
