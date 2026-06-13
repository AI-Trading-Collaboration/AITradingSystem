from __future__ import annotations

from datetime import UTC, date, datetime

from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    TARGET_AS_OF,
    build_model_target_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _emission_fixture(tmp_path):
    target = build_model_target_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "market_cache")
    emission = system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        target_id=target["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    return emission, prices_path


def test_smoothed_outcome_due_scans_due_not_due_and_missing_windows(tmp_path) -> None:
    _, prices_path = _emission_fixture(tmp_path)

    due = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )

    summary = due["due_summary"]
    rows = {row["window_days"]: row for row in due["due_windows"]}

    assert summary["events_scanned"] == 1
    assert summary["total_windows_scanned"] == 4
    assert summary["due_windows"] == 1
    assert summary["not_due_windows"] == 3
    assert summary["update_ready_count"] == 1
    assert summary["blocked_future_as_of"] == 0
    assert rows[1]["due_status"] == "DUE"
    assert rows[1]["can_update"] is True
    assert rows[20]["due_status"] == "NOT_DUE"
    assert rows[20]["can_update"] is False

    missing_scan = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF.replace(day=20),
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due_missing",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    assert missing_scan["due_summary"]["price_missing_windows"] == 2
    assert all(
        row["can_update"] is False
        for row in missing_scan["due_windows"]
        if row["due_status"] == "PRICE_MISSING"
    )

    future_scan = system_target.scan_smoothed_outcome_due(
        as_of=TARGET_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due_future",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 4, tzinfo=UTC),
    )
    assert future_scan["due_summary"]["due_windows"] == 0
    assert all(row["can_update"] is False for row in future_scan["due_windows"])

    check = system_target.validate_smoothed_outcome_due_artifact(
        due_id=due["due_id"],
        output_dir=tmp_path / "smoothed_outcome_due",
    )
    assert check["status"] == "PASS"


def test_smoothed_outcome_due_ignores_inactive_emission_events(tmp_path) -> None:
    target = build_model_target_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "market_cache")
    emission = system_target.run_smoothed_daily_emission(
        as_of=date(2026, 1, 20),
        target_id=target["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
    )
    assert emission["manifest"]["emitted_event_count"] == 0
    assert emission["smoothed_forward_events"][0]["event_status"] == "INSUFFICIENT_DATA"

    due = system_target.scan_smoothed_outcome_due(
        as_of=date(2026, 1, 20),
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )

    assert due["due_summary"]["events_scanned"] == 0
    assert due["due_summary"]["total_windows_scanned"] == 0
    assert due["due_windows"] == []
