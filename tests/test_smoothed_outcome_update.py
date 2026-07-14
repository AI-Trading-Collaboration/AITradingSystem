from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import EVALUATION_AS_OF, TARGET_AS_OF

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _due_fixture(tmp_path):
    system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        output_dir=tmp_path / "smoothed_daily_emission",
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    due = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )
    return due


def test_smoothed_outcome_update_only_updates_ready_windows(tmp_path) -> None:
    due = _due_fixture(tmp_path)

    update = system_target.run_smoothed_outcome_update(
        due_id=due["due_id"],
        due_dir=tmp_path / "smoothed_outcome_due",
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_update",
        generated_at=datetime(2026, 1, 8, 1, tzinfo=UTC),
    )

    summary = update["smoothed_outcome_delta_summary"]
    updated = update["updated_smoothed_outcomes"]
    skipped = update["skipped_smoothed_outcomes"]

    assert summary["updated_count"] == 0
    assert summary["skipped_count"] == 0
    assert summary["available_forward_events_after_update"] == 0
    assert summary["avg_candidate_relative_return_vs_baseline"] is None
    assert updated == []
    assert skipped == []
    assert update["manifest"]["candidate_method"] is None
    assert update["manifest"]["broker_action_allowed"] is False
    assert update["manifest"]["production_effect"] == "none"

    check = system_target.validate_smoothed_outcome_update_artifact(
        update_id=update["update_id"],
        output_dir=tmp_path / "smoothed_outcome_update",
    )
    assert check["status"] == "PASS"
