from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    TARGET_AS_OF,
    build_model_target_fixture,
    run_smoothed_forward_ops_chain_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_forward_weekly_run_handles_no_due_windows(tmp_path) -> None:
    target = build_model_target_fixture(tmp_path)
    ops = run_smoothed_forward_ops_chain_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "weekly_market_cache")

    weekly = system_target.run_smoothed_forward_weekly_run(
        week_ending=TARGET_AS_OF,
        target_id=target["target_id"],
        binding_id=ops["binding"]["binding_id"],
        switch_plan_id=ops["switch_plan"]["switch_plan_id"],
        owner_promotion_id=ops["recorded_owner_promotion"]["decision_id"],
        model_target_dir=tmp_path / "model_target",
        emission_dir=tmp_path / "smoothed_daily_emission",
        due_dir=tmp_path / "smoothed_outcome_due",
        update_dir=tmp_path / "smoothed_outcome_update",
        classification_dir=tmp_path / "smoothed_forward_classification",
        binding_dir=tmp_path / "smoothed_forward_binding",
        progress_dir=tmp_path / "smoothed_forward_progress_weekly",
        dashboard_dir=tmp_path / "smoothed_weekly_dashboard_weekly",
        monitor_dir=tmp_path / "smoothed_event_monitor_weekly",
        switch_plan_dir=tmp_path / "paper_shadow_primary_switch",
        recheck_dir=tmp_path / "smoothed_switch_readiness_weekly",
        owner_promotion_dir=tmp_path / "smoothed_owner_promotion",
        renewal_dir=tmp_path / "smoothed_owner_renewal_weekly",
        output_dir=tmp_path / "smoothed_forward_weekly_run",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 4, tzinfo=UTC),
    )

    summary = weekly["weekly_run_summary"]
    steps = weekly["weekly_run_steps"]["steps"]

    assert summary["emitted_events"] == 1
    assert summary["due_windows"] == 0
    assert summary["updated_windows"] == 0
    assert summary["available_forward_events"] == 0
    assert summary["available_sideways_events"] == 0
    assert summary["available_recovery_events"] == 0
    assert summary["can_execute_switch"] is False
    assert summary["weekly_recommendation"] == "continue_observation"
    assert summary["broker_action_allowed"] is False
    assert summary["production_effect"] == "none"
    assert {row["step"] for row in steps} >= {
        "daily_emission",
        "outcome_due_scan",
        "outcome_update",
        "forward_classification",
        "progress_update",
        "weekly_dashboard",
    }
    assert all(row["status"] == "PASS" for row in steps)
    assert "Dynamic Rescue Smoothed Forward Weekly Run" in weekly["reader_brief_section"]

    check = system_target.validate_smoothed_forward_weekly_run_artifact(
        weekly_run_id=weekly["weekly_run_id"],
        output_dir=tmp_path / "smoothed_forward_weekly_run",
    )
    assert check["status"] == "PASS"
