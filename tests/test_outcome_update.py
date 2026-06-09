from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dynamic_v3_outcome_loop_helpers import (
    build_ready_outcome_update_fixture,
    run_safe_update_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_outcome_update_updates_ready_window_and_audits_skips(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    result = run_safe_update_fixture(tmp_path, monkeypatch)["update"]
    delta = result["outcome_status_delta"]

    assert result["manifest"]["updated_count"] == 1
    assert result["updated_windows"][0]["old_status"] == "PENDING"
    assert result["updated_windows"][0]["new_status"] == "AVAILABLE"
    assert result["updated_windows"][0]["future_data_used_in_decision"] is False
    assert {row["skip_reason"] for row in result["skipped_windows"]} >= {"NOT_DUE"}
    assert delta["before"]["forward_available"] == 0
    assert delta["after"]["forward_available"] == 1
    assert delta["before"]["forward_pending"] == 4
    assert delta["after"]["forward_pending"] == 3
    assert (
        accumulation.validate_outcome_update_artifact(
            update_id=result["outcome_update_id"],
            output_dir=tmp_path / "outcome_update",
        )["status"]
        == "PASS"
    )


def test_outcome_update_treats_cash_as_zero_return_without_price_row(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    prices_path = fixture["update_prices_path"]
    prices = pd.read_csv(prices_path)
    prices = prices[prices["ticker"] != "CASH"]
    prices.to_csv(prices_path, index=False)

    result = accumulation.run_outcome_update(
        update_review_id=fixture["update_review"]["update_review_id"],
        output_dir=tmp_path / "outcome_update",
        review_dir=tmp_path / "outcome_update_review",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=fixture["update_rates_path"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    assert result["manifest"]["updated_count"] == 1
    assert result["updated_windows"][0]["new_status"] == "AVAILABLE"
