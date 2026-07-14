from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_confirmation_registers_targets_and_safety_boundary(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    regime = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
    )

    result = system_target.register_smoothed_confirmation_targets(
        review_id=fixture["review"]["review_id"],
        regime_validation_id=regime["regime_validation_id"],
        review_dir=tmp_path / "smoothed_review",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )

    targets = result["smoothed_confirmation_targets"]
    assert targets["schema_version"] == 2
    assert targets["report_type"] == "etf_dynamic_v3_smoothed_confirmation_targets"
    assert targets["status"] == "INSUFFICIENT_EVIDENCE"
    assert targets["candidate_method"] is None
    assert targets["targets"] == []
    assert targets["auto_apply"] is False
    assert targets["broker_action_allowed"] is False
    assert targets["production_effect"] == "none"

    validation = system_target.validate_smoothed_confirmation_artifact(
        confirmation_id=result["confirmation_id"],
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )
    assert validation["status"] == "PASS"
