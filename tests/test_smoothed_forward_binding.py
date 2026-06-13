from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_forward_binding_connects_targets_to_weekly_progress(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    binding = fixture["binding"]

    targets_payload = binding["bound_confirmation_targets"]
    targets = {row["target_id"]: row for row in targets_payload["targets"]}
    assert targets_payload["source_confirmation_id"] == fixture["confirmation"]["confirmation_id"]
    assert targets["smooth_3d_vs_limited"]["status"] == "IN_PROGRESS"
    assert (
        targets["smooth_3d_vs_limited"]["required_forward_events"]
        == system_target.SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS
    )
    assert (
        targets["smooth_3d_sideways_choppy_improvement"]["required_sideways_events"]
        == system_target.SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS
    )
    assert targets["smooth_3d_recovery_lag_watch"]["status"] == "WATCH_ONLY"
    assert all(row["bound_to_weekly_progress"] is True for row in targets.values())
    assert all(row["broker_action_allowed"] is False for row in targets.values())

    requirements = binding["forward_progress_requirements"]
    assert "required_forward_events_met" in requirements["rule_review_ready_when"]
    assert "no_high_lag_failure" in requirements["rule_review_ready_when"]
    assert "Dynamic Rescue Smoothed Forward Binding" in binding["reader_brief_section"]

    validation = system_target.validate_smoothed_forward_binding_artifact(
        binding_id=binding["binding_id"],
        output_dir=tmp_path / "smoothed_forward_binding",
    )
    assert validation["status"] == "PASS"
