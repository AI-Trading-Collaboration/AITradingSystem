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
    run_historical_paper_sim,
    run_historical_replay,
    validate_historical_paper_sim_artifact,
)


def test_historical_paper_sim_rebuilds_state_and_ledger_without_broker_action(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    first_target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    second_target = {"QQQ": 0.40, "SMH": 0.35, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="first",
        as_of="2026-06-03",
        target_weights=first_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="second",
        as_of="2026-06-10",
        target_weights=second_target,
    )
    write_owner_reviews(paths["owner_review_dir"], ["first", "second"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 30))
    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    sim = run_historical_paper_sim(
        replay_id=replay["replay_id"],
        variant="limited_adjustment",
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    assert sim["performance_summary"]["simulation_status"] == "AVAILABLE"
    assert sim["performance_summary"]["variant"] == "limited_adjustment"
    assert sim["state_history"]
    assert sim["trade_ledger"]
    assert sim["performance_summary"]["turnover"] > 0
    assert sim["manifest"]["broker_action_taken"] is False
    assert all(row["broker_action_taken"] is False for row in sim["trade_ledger"])

    validation = validate_historical_paper_sim_artifact(
        sim_id=sim["sim_id"],
        output_dir=paths["paper_sim_dir"],
    )
    assert validation["status"] == "PASS"
