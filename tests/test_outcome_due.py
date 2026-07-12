from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_market_cache,
    write_validated_daily_advisory,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    init_paper_portfolio,
    track_advisory_outcome,
)


def test_outcome_due_scan_classifies_due_not_due_and_price_missing(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    due = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    missing_advisory = write_validated_daily_advisory(
        tmp_path / "missing_source",
        as_of=date(2026, 6, 8),
        generated_at=datetime(2026, 6, 8, 10, 1, tzinfo=UTC),
    )
    missing = track_advisory_outcome(
        daily_advisory_id=missing_advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=missing_advisory["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, 1, tzinfo=UTC),
    )
    event_path = missing["outcome_dir"] / "advisory_event.json"
    event = json.loads(event_path.read_text(encoding="utf-8"))
    event["target_weights"] = {"FAKE": 0.50, "CASH": 0.50}
    event_path.write_text(json.dumps(event, sort_keys=True), encoding="utf-8")
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache", start="2026-06-08")
    prices = pd.read_csv(prices_path)
    prices["symbol"] = prices["ticker"]
    prices.to_csv(prices_path, index=False)

    result = accumulation.run_outcome_due_scan(
        as_of=date(2026, 6, 15),
        output_dir=tmp_path / "outcome_due",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        prices_path=prices_path,
        rates_path=rates_path,
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )

    by_outcome = {}
    for row in result["due_window_inventory"]:
        by_outcome.setdefault(row["outcome_id"], []).append(row)
    due_statuses = {row["window_days"]: row["due_status"] for row in by_outcome[due["outcome_id"]]}
    missing_statuses = {
        row["window_days"]: row["due_status"] for row in by_outcome[missing["outcome_id"]]
    }
    assert due_statuses[1] == "DUE"
    assert due_statuses[5] == "DUE"
    assert due_statuses[10] == "NOT_DUE"
    assert missing_statuses[1] == "PRICE_MISSING"
    assert result["pending_window_summary"]["update_ready_count"] == 2
    assert {row["outcome_id"] for row in result["update_ready_list"]} == {due["outcome_id"]}
    assert (
        accumulation.validate_outcome_due_artifact(
            due_id=result["due_id"],
            output_dir=tmp_path / "outcome_due",
        )["status"]
        == "PASS"
    )
