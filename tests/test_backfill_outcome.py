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
    run_backfill_outcome,
    run_historical_replay,
    validate_backfill_outcome_artifact,
)


def test_backfill_outcome_distinguishes_available_pending_and_insufficient_data(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="available",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="missing-price",
        as_of="2026-06-04",
        current_weights={"QQQ": 0.50, "XYZ": 0.20, "SOXX": 0.10, "CASH": 0.20},
        target_weights={"QQQ": 0.45, "XYZ": 0.30, "SOXX": 0.10, "CASH": 0.15},
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="pending",
        as_of="2026-07-30",
        target_weights=target,
    )
    write_owner_reviews(paths["owner_review_dir"], ["available", "missing-price", "pending"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 7, 31))
    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    backfill = run_backfill_outcome(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    statuses = {row["outcome_status"] for row in backfill["outcome_rows"]}

    assert {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"} <= statuses
    assert backfill["manifest"]["replay_event_count"] == replay["manifest"]["replay_event_count"]
    assert backfill["manifest"]["data_quality_status"] == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    assert backfill["manifest"]["available_count"] > 0
    assert backfill["manifest"]["pending_count"] > 0
    assert backfill["manifest"]["insufficient_data_count"] > 0
    assert backfill["variant_performance_summary"]["best_variant"] != ""
    assert all(row["outcome_mode"] == "HISTORICAL_REPLAY" for row in backfill["outcome_rows"])

    validation = validate_backfill_outcome_artifact(
        backfill_id=backfill["backfill_id"],
        output_dir=paths["backfill_dir"],
    )
    assert validation["status"] == "PASS"
