from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    write_owner_reviews,
    write_paper_action,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    validate_replay_inventory_artifact,
)


def test_replay_inventory_classifies_pit_safety_and_blocks_unsafe(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    safe_target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=safe_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="warning",
        as_of="2026-06-04",
        target_weights=safe_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="unsafe",
        as_of="2026-06-05",
        target_weights=None,
    )
    write_owner_reviews(paths["owner_review_dir"], ["safe", "unsafe"])
    write_paper_action(
        paths["paper_portfolio_dir"],
        daily_advisory_id="safe",
        after_weights=safe_target,
    )

    result = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))
    rows = {row["daily_advisory_id"]: row for row in result["rows"]}

    assert rows["safe"]["pit_safety_status"] == "PIT_SAFE"
    assert rows["warning"]["pit_safety_status"] == "PIT_WARNING"
    assert rows["unsafe"]["pit_safety_status"] == "PIT_UNSAFE"
    assert rows["unsafe"]["replay_eligibility"] == "INELIGIBLE"
    assert result["manifest"]["pit_safe_count"] == 1
    assert result["manifest"]["pit_warning_count"] == 1
    assert result["manifest"]["pit_unsafe_count"] == 1
    assert result["pit_safety_audit"]["pit_unsafe_allowed_in_default_replay"] is False

    validation = validate_replay_inventory_artifact(
        inventory_id=result["inventory_id"],
        output_dir=paths["inventory_dir"],
    )
    assert validation["status"] == "PASS"
