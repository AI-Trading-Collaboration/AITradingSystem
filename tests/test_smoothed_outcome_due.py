from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import EVALUATION_AS_OF, TARGET_AS_OF

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _emission_fixture(tmp_path):
    emission = system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        output_dir=tmp_path / "smoothed_daily_emission",
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    return emission


def test_smoothed_outcome_due_keeps_candidate_less_inventory_empty(tmp_path) -> None:
    _emission_fixture(tmp_path)

    due = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )

    summary = due["due_summary"]
    assert summary["events_scanned"] == 0
    assert summary["total_windows_scanned"] == 0
    assert summary["due_windows"] == 0
    assert summary["not_due_windows"] == 0
    assert summary["update_ready_count"] == 0
    assert summary["blocked_future_as_of"] == 0
    assert due["due_windows"] == []
    assert due["manifest"]["binding_status"] == "NOT_REGISTERED"
    assert due["manifest"]["emission_ids"] == []

    check = system_target.validate_smoothed_outcome_due_artifact(
        due_id=due["due_id"],
        output_dir=tmp_path / "smoothed_outcome_due",
    )
    assert check["status"] == "PASS"
