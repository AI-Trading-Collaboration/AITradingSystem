from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    TARGET_AS_OF,
    build_model_target_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _due_fixture(tmp_path):
    target = build_model_target_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "market_cache")
    system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        target_id=target["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    due = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )
    return due, prices_path


def test_smoothed_outcome_update_only_updates_ready_windows(tmp_path) -> None:
    due, prices_path = _due_fixture(tmp_path)

    update = system_target.run_smoothed_outcome_update(
        due_id=due["due_id"],
        due_dir=tmp_path / "smoothed_outcome_due",
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_update",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 8, 1, tzinfo=UTC),
    )

    summary = update["smoothed_outcome_delta_summary"]
    updated = update["updated_smoothed_outcomes"]
    skipped = update["skipped_smoothed_outcomes"]

    assert summary["updated_count"] == 1
    assert summary["skipped_count"] == 3
    assert summary["available_forward_events_after_update"] == 1
    assert updated[0]["window_days"] == 1
    assert updated[0]["outcome_status"] == "AVAILABLE"
    assert updated[0]["future_data_used"] is False
    assert "smooth_3d_vs_limited" in updated[0]["relative_metrics"]
    assert {row["skip_reason"] for row in skipped} == {"NOT_DUE"}
    assert update["manifest"]["broker_action_allowed"] is False
    assert update["manifest"]["production_effect"] == "none"

    check = system_target.validate_smoothed_outcome_update_artifact(
        update_id=update["update_id"],
        output_dir=tmp_path / "smoothed_outcome_update",
    )
    assert check["status"] == "PASS"
