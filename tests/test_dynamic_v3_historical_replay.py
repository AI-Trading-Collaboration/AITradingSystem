from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    write_owner_reviews,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    REPLAY_VARIANTS,
    run_historical_replay,
    validate_historical_replay_artifact,
)


def test_dynamic_v3_historical_replay_skips_unsafe_and_requires_warning_opt_in(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="warning",
        as_of="2026-06-04",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="unsafe",
        as_of="2026-06-05",
        target_weights=None,
    )
    write_owner_reviews(paths["owner_review_dir"], ["safe", "unsafe"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))

    default_replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=False,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert default_replay["manifest"]["replay_event_count"] == 1
    assert default_replay["manifest"]["skipped_count"] == 2
    event = default_replay["events"][0]
    assert event["daily_advisory_id"] == "safe"
    assert {row["variant"] for row in event["variants"]} == set(REPLAY_VARIANTS)
    assert all(row["broker_action_taken"] is False for row in event["variants"])
    assert event["outcome_mode"] == "HISTORICAL_REPLAY"
    assert event["broker_action_taken"] is False
    limited = next(row for row in event["variants"] if row["variant"] == "limited_adjustment")
    assert limited["turnover"] > 0

    validation = validate_historical_replay_artifact(
        replay_id=default_replay["replay_id"],
        output_dir=paths["historical_replay_dir"],
    )
    assert validation["status"] == "PASS"

    warning_replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 11, tzinfo=UTC),
    )
    assert warning_replay["manifest"]["replay_event_count"] == 2
    assert {row["daily_advisory_id"] for row in warning_replay["events"]} == {"safe", "warning"}
    assert all(row["pit_safety_status"] != "PIT_UNSAFE" for row in warning_replay["events"])
